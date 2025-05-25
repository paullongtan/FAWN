/*
TODO:
- replace linear scan of footer with binary search
*/

use std::{
    collections::HashMap, fs::{File, OpenOptions}, io::{self, Write}, os::unix::fs::FileExt, path::{Path, PathBuf}
};
use memmap2::Mmap;

use crate::record::{self, Record, RecordFlags};

/// Small metadata carrier shared by writer & reader.
#[derive(Clone, Debug)]
pub(crate) struct SegmentInfo {
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

pub(crate) struct SegmentWriter {
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

    pub fn append_put(&mut self, key: &[u8], val: &[u8]) -> io::Result<()> {
        self.append_generic(key, val, RecordFlags::Put)
    }

    pub fn append_delete(&mut self, key: &[u8]) -> io::Result<()> {
        self.append_generic(key, &[], RecordFlags::Delete)
    }

    fn append_generic(&mut self, key: &[u8], val: &[u8], flags: RecordFlags) -> io::Result<()> {
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
            *off
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
        
        // decode the record and check key match (since we only store the hash32 of the key)
        // doesn't try to find the next offset if a collision happens
        // LIMIT: it only return the latest offset if a collision happens
        // however, footer stores all (hash32, offset) pairs, so we can still find all records later.
        let rec = Record::decode(&mut rec_buf[..])?;
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
    pub fn seal(self) -> io::Result<SegmentReader> {
        self.flush_footer()?;
        drop(self.log_fd.sync_all()); // ensure log is synced before mmap

        // reopen read-only
        let log_ro = File::open(&self.meta.log_path)?;
        SegmentReader::open_with_fd(sefl.meta, log_ro)
    }

    pub fn bytes_written(&self) -> u32 {
        self.offset
    }
}

pub(crate) struct SegmentReader {
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
        let ftr_fd = File::open(&meta.ftr_path)?;
        let footer_mm = unsafe { Mmap::map(&ftr_fd)? };
        Ok(Self { meta, log_fd, footer_mm })
    }

    /// Linear scan footer to find the hash32. (replace with binary search later)
    /// Binary-search footer → single `pread`.
    pub fn lookup(&self, hash: u32, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let slots = self.footer_mm.len() / 8; // each entry is 8 bytes (hash32 + offset32)
        for i in 0..slots {
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
            let rec = Record::decode(&mut rec_buf[..])?;
            if rec.key == key {
                return Ok(match rec.flags {
                    RecordFlags::Put => Some(rec.value.to_vec()),
                    RecordFlags::Delete => None, // tombstone
                });
            }
        }
        Ok(None) // not found
    }
}



/*
let mut w = SegmentWriter::create("./data", 0, 4*1024*1024)?;
w.append_put(b"foo", b"bar")?;
assert_eq!(
    w.lookup_in_mem(record::hash32(b"foo"), b"foo")?,
    Some(Some(b"bar".to_vec()))
);
let r = w.seal()?;
assert_eq!(
    r.lookup(record::hash32(b"foo"), b"foo")?,
    Some(b"bar".to_vec())
);
*/