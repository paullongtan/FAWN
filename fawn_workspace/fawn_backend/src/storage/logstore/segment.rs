/*
TODO:
- replace linear scan of footer with binary search
*/

use std::{
    collections::HashMap, fs::{File, OpenOptions}, io::{self, Write}, os::unix::fs::FileExt, path::{Path, PathBuf}
};
use memmap2::Mmap;

use super::record::{self, Record, RecordFlags};

/// Small metadata carrier shared by writer & reader.
#[derive(Clone, Debug)]
pub struct SegmentInfo {
    pub id:       u64,
    pub log_path: PathBuf,
    pub ftr_path: PathBuf,
}

impl SegmentInfo {
    /// Construct paths like `<dir>/<id>.log`  `<dir>/<id>.ftr`.
    pub fn new<P: AsRef<Path>>(dir: P, id: u64) -> Self {
        // format id as 16-digit hex
        let log_path = dir.as_ref().join(format!("{id:016X}.log"));
        let ftr_path = dir.as_ref().join(format!("{id:016X}.ftr"));
        Self { id, log_path, ftr_path }
    }
}

pub struct SegmentWriter {
    pub meta:       SegmentInfo,
    log_fd:         File,
    footer_buf:     Vec<(u32 /*hash32*/, u32 /*offset32*/ )>, // unsorted until flushed
    in_mem_idx:     HashMap<u32, u32>, // hash32 → offset
    offset:         u32, // current write offset in the log file
    max_size:       u32,
}

impl SegmentWriter {
    /// Create a fresh `.log` file, empty footer buffer.
    pub fn create<P: AsRef<Path>>(dir: P, id: u64, max_size: u32) -> io::Result<Self> {
        let meta = SegmentInfo::new(dir, id);
        let log_fd = OpenOptions::new()
            .create(true)
            .read(true) // allow reading back
            .append(true) // allow appending
            .open(&meta.log_path)?;
        let offset = log_fd.metadata()?.len() as u32; // 0 for new file
        Ok(Self {
            meta,
            log_fd,
            footer_buf: Vec::new(),
            in_mem_idx: HashMap::new(),
            offset,
            max_size,
        })
    }

    /// Re-open an existing segment (.log) for appending
    pub fn open<P: AsRef<Path>>(dir: P, id: u64, max_size: u32) -> io::Result<Self> {
        let meta = SegmentInfo::new(dir, id);
        let log_fd = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&meta.log_path)?;
        let offset = log_fd.metadata()?.len() as u32; // current write offset

        // load footer if it exists
        let mut footer_buf = Vec::new();
        let mut in_mem_idx = HashMap::new();
        if meta.ftr_path.exists() {
            let ftr_fd = File::open(&meta.ftr_path)?;
            let mmap = unsafe { Mmap::map(&ftr_fd)? };
            if mmap.len() % 8 != 0 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Footer is corrupted"));
            }
            for slot in mmap.chunks_exact(8) {
                let hash = u32::from_le_bytes(slot[0..4].try_into().unwrap());
                let offset = u32::from_le_bytes(slot[4..8].try_into().unwrap());
                footer_buf.push((hash, offset));
                in_mem_idx.insert(hash, offset);
            }
        }

        Ok(Self {
            meta,
            log_fd,
            footer_buf,
            in_mem_idx,
            offset,
            max_size,
        })
    }

    pub fn append_put(&mut self, hash: u32, key: &[u8], val: &[u8]) -> io::Result<()> {
        self.append_generic(hash, key, val, RecordFlags::Put)
    }

    pub fn append_delete(&mut self, hash: u32, key: &[u8]) -> io::Result<()> {
        self.append_generic(hash, key, &[], RecordFlags::Delete)
    }

    fn append_generic(&mut self, hash: u32, key: &[u8], val: &[u8], flags: RecordFlags) -> io::Result<()> {
        let rec = Record { flags, key, value: val, hash32: hash };

        // rollover guard (caller should roll segment if needed)
        if self.offset + rec.encoded_len() as u32 > self.max_size {
            return Err(io::Error::new(io::ErrorKind::Other, "Segment size limit exceeded"));
        }

        // encode into temp buf
        let mut buf = Vec::with_capacity(rec.encoded_len());
        rec.encode(&mut buf)?;

        // write & update book-keeping
        self.log_fd.write_all(&buf)?;
        self.footer_buf.push((hash, self.offset));
        self.in_mem_idx.insert(hash, self.offset);
        self.offset += buf.len() as u32;
        Ok(())
    }

    /// Fast in-memory lookup while the segment is still open.
    /// it looks up the hash32 in the in-memory index,
    /// then reads the record at the offset, decodes it, and checks the key.
    /// Returns `Some(Some(value))` for a put, `Some(None)` for a delete (tombstone), or `None` if not found.
    pub fn lookup_in_mem(
        &self,
        hash: u32,
        key: &[u8],
    ) -> io::Result<Option<Option<Vec<u8>>>> {

        // check if we have this hash in the in-memory index
        let offset = if let Some(&off) = self.in_mem_idx.get(&hash) {
            off
        } else {
            return Ok(None); // not found
        };

        // read header length
        let mut len_buf = [0u8; 4];
        self.log_fd.read_exact_at(&mut len_buf, offset as u64)?;

        // read the full record
        let rec_len = u32::from_le_bytes(len_buf) as usize + 4; // +4 for the length itself
        let mut rec_buf = vec![0u8; rec_len];
        self.log_fd.read_exact_at(&mut rec_buf, offset as u64)?;
        
        // store the latest offset for each hash32
        // if two different keys have the same hash32, the latest one overwrites the previous one
        // however, this is not a problem because the latest record for a key determines its current state
        // and all the records will be written to disk and indexed in the footer anyway
        let rec = Record::decode(&mut &rec_buf[..])?;
        if rec.key == key {
            return Ok(Some(match rec.flags {
                RecordFlags::Put => Some(rec.value.to_vec()),
                RecordFlags::Delete => None, // tombstone
            }));
        }
        Ok(None) // key mismatch
    }

    /// Flush the footer buffer to the `.ftr` file.
    /// sort by hash32. No-op if empty.
    pub fn flush_footer(&mut self) -> io::Result<()> {
        if self.footer_buf.is_empty() {
            return Ok(()); // nothing to flush
        }

        // sort by hash32
        self.footer_buf.sort_by_key(|&(hash, _)| hash);

        // open footer file
        let mut ftr_fd = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true) // overwrite existing
            .open(&self.meta.ftr_path)?;

        // write each entry
        for (hash, offset) in &self.footer_buf {
            ftr_fd.write_all(&hash.to_le_bytes())?;
            ftr_fd.write_all(&offset.to_le_bytes())?;
        }

        ftr_fd.sync_all()?; // ensure it's on disk
        self.log_fd.sync_all()?; // ensure log is also synced
        self.footer_buf.clear(); // clear the buffer after flushing
        Ok(())
    }

    /// Finish the segment, return a read-only handle (mmap footer).
    pub fn seal(mut self) -> io::Result<SegmentReader> {
        self.flush_footer()?;
        drop(self.log_fd.sync_all()); // ensure log is synced before mmap

        // reopen read-only
        let log_ro = File::open(&self.meta.log_path)?;
        SegmentReader::open_with_fd(self.meta, log_ro)
    }

    pub fn bytes_written(&self) -> u32 {
        self.offset
    }

    // For appending records with key bytes
    pub fn append_put_with_key(&mut self, key: &[u8], val: &[u8]) -> io::Result<()> {
        self.append_generic_with_key(key, val, RecordFlags::Put)
    }

    pub fn append_delete_with_key(&mut self, key: &[u8]) -> io::Result<()> {
        self.append_generic_with_key(key, &[], RecordFlags::Delete)
    }

    pub fn append_generic_with_key(&mut self, key: &[u8], val: &[u8], flags: RecordFlags) -> io::Result<()> {
        let hash = record::hash32(key);
        let rec = Record { flags, key, value: val, hash32: hash };

        // rollover guard (caller should roll segment if needed)
        if self.offset + rec.encoded_len() as u32 > self.max_size {
            return Err(io::Error::new(io::ErrorKind::Other, "Segment size limit exceeded"));
        }

        // encode into temp buf
        let mut buf = Vec::with_capacity(rec.encoded_len());
        rec.encode(&mut buf)?;

        // write & update book-keeping
        self.log_fd.write_all(&buf)?;
        self.footer_buf.push((hash, self.offset));
        self.in_mem_idx.insert(hash, self.offset);
        self.offset += buf.len() as u32;
        Ok(())
    }
}

pub struct SegmentReader {
    pub meta:   SegmentInfo,
    log_fd:     File,
    footer_mm:  Mmap,
}

impl SegmentReader {
    /// Open existing `<id>.log/<id>.ftr` pair.
    pub fn open(meta: SegmentInfo) -> io::Result<Self> {
        let log_fd = File::open(&meta.log_path)?;
        Self::open_with_fd(meta, log_fd)
    }

    pub fn open_with_fd(meta: SegmentInfo, log_fd: File) -> io::Result<Self> {
        // Try to open the footer file, map NotFound to InvalidData
        let ftr_fd = File::open(&meta.ftr_path).map_err(|e| {
            if e.kind() == io::ErrorKind::NotFound {
                io::Error::new(io::ErrorKind::InvalidData, "Footer is missing")
            } else {
                e
            }
        })?;

        let footer_mm = unsafe { Mmap::map(&ftr_fd)? }; // memory-map the footer file
        let reader = Self { meta, log_fd, footer_mm };
        
        if reader.footer_valid() {
            println!("SegmentReader: footer is valid");
            Ok(reader)
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidData, "Footer is corrupted"))
        }
    }

    pub fn log_len(&self) -> std::io::Result<u64> {
        self.log_fd.metadata().map(|m| m.len())
    }

    /// check if the footer is corrupted (assuming the footer is already memory-mapped):
    /// - must be a multiple of 8 bytes (hash32 + offset32)
    /// - all offsets must be within the log file length
    fn footer_valid(&self) -> bool {
        // footer must be a multiple of 8 bytes (hash32 + offset32)
        if self.footer_mm.len() % 8 != 0 {
            return false;
        }

        // check that all offsets are within the log file length
        let log_len = self.log_fd.metadata().map(|m| m.len()).unwrap_or(0);
        for slot in self.footer_mm.chunks_exact(8) {
            let off = u32::from_le_bytes(slot[4..8].try_into().unwrap()) as u64;
            if off >= log_len {
                return false; // offset past EOR -> corrupted footer
            }
        }
        true // footer is valid
    }

    /// Linear scan footer to find the hash32. (replace with binary search later)
    /// Binary-search footer → single `pread`.
    pub fn lookup(&self, hash: u32, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let slots = self.footer_mm.len() / 8; // each entry is 8 bytes (hash32 + offset32)

        // scan the footer in reverse order (newest to oldest since the latest record for a key determines its current state)
        for i in (0..slots).rev() {
            let slot = &self.footer_mm[i * 8..(i + 1) * 8];

            // read the hash32 and offset32 from the slot
            let h = u32::from_le_bytes(slot[0..4].try_into().unwrap());
            if h != hash {
                continue; // not a match
            }
            let offset = u32::from_le_bytes(slot[4..8].try_into().unwrap());

            // read the record at this offset
            let mut len_buf = [0u8; 4];
            self.log_fd.read_exact_at(&mut len_buf, offset as u64)?;
            let rec_len = u32::from_le_bytes(len_buf) as usize + 4; // +4 for the length itself
            let mut rec_buf = vec![0u8; rec_len];
            self.log_fd.read_exact_at(&mut rec_buf, offset as u64)?;

            // decode the record and check key match (it handles collision by checking all possible offsets for the hash)
            let rec = Record::decode(&mut &rec_buf[..])?;
            if rec.key == key {
                return Ok(match rec.flags {
                    RecordFlags::Put => Some(rec.value.to_vec()),
                    RecordFlags::Delete => None, // tombstone
                });
            }
        }
        Ok(None) // not found
    }

    /// Iterate over (hash32, offset32) pairs stored in the footer.
    pub fn footer_pairs(&self) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.footer_mm
            .chunks_exact(8)
            .map(|slot| {
                let h = u32::from_le_bytes(slot[0..4].try_into().unwrap());
                let o = u32::from_le_bytes(slot[4..8].try_into().unwrap());
                (h, o)
            })
    }

    /// Read the raw bytes of one record at `off`.
    pub fn read_record_bytes(&self, off: u32) -> io::Result<Vec<u8>> {
        let mut len_buf = [0u8; 4];
        self.log_fd.read_exact_at(&mut len_buf, off as u64)?;
        let rec_len = u32::from_le_bytes(len_buf) as usize + 4;
        let mut buf = vec![0u8; rec_len];
        self.log_fd.read_exact_at(&mut buf, off as u64)?;
        Ok(buf)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn writer_reader_cycle() {
        let dir = TempDir::new().unwrap();
        println!("Temp dir: {:?}", dir);
        let mut w = SegmentWriter::create(dir.path(), 0, 4 * 1024 * 1024).unwrap();

        // Append some records
        w.append_put_with_key(b"alpha", b"one").unwrap();
        w.append_put_with_key(b"beta",  b"two").unwrap();
        w.append_delete_with_key(b"alpha").unwrap();

        // check in-memory lookup
        assert_eq!(
            w.lookup_in_mem(record::hash32(b"beta"), b"beta").unwrap(),
            Some(Some(b"two".to_vec())) // found a put
        );

        assert_eq!(
            w.lookup_in_mem(record::hash32(b"alpha"), b"alpha").unwrap(),
            Some(None) // found a delete (tombstone)
        );

        println!("passed in-mem lookup");

        // Seal the segment
        let rdr = w.seal().unwrap();

        // reader sees tombstone & live
        assert!(
            rdr.lookup(record::hash32(b"alpha"), b"alpha")
            .unwrap()
            .is_none()
        );

        assert_eq!(
            rdr.lookup(record::hash32(b"beta"), b"beta").unwrap(),
            Some(b"two".to_vec()) // found a put
        );
    }
}