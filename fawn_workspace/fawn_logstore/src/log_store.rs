/*
TODO: 
- reuse the latest segment if it is not full, instead of always rolling.
- Compaction work not implemented yet. (placeholder only)
*/

use crate::segment::{SegmentInfo, SegmentWriter, SegmentReader};
use std::{
    fs, 
    io::{self, Write}, 
    path::PathBuf, 
    sync::Mutex, 
    time::{Duration, Instant}, 
    os::unix::fs::FileExt
};

const FLUSH_EVERY: Duration = Duration::from_secs(5); // flush every 5 seconds
const MAX_SEG_SIZE: u32 = 4 * 1024 * 1024; // 4 MiB max segment size

pub struct LogStructuredStore {
    dir:           PathBuf,
    active:        Mutex<SegmentWriter>,
    sealed:        Mutex<Vec<SegmentReader>>, // sorted by id descending (newest first)
    max_seg_size:  u32,
    last_flush:    Mutex<Instant>,
}

/// Rebuild the footer for a segment by reading the log file and writing the footer file.
fn rebuild_footer(meta: &crate::segment::SegmentInfo) -> io::Result<()> {
    let fd = fs::File::open(&meta.log_path)?;
    let mut off = 0u32; // offset in the log file
    let mut footer = Vec::<(u32, u32)>::new(); // (hash32, offset)
    let log_len = fd.metadata()?.len(); // total length of the log file

    // sequentially read the log file to rebuild entries for the footer
    while (off as u64) < log_len {
        // Read the record length (4 bytes) at the current offset
        let mut len_buf = [0u8; 4];
        if let Err(_) = fd.read_exact_at(&mut len_buf, off as u64) {
            break; // incomplete length field at the end
        }

        // compute the full record length (including the 4 bytes for rec_len itself)
        let rec_len = u32::from_le_bytes(len_buf) + 4;

        // check if the record length is valid
        if rec_len == 4 || (off as u64) + (rec_len as u64) > log_len {
            break; // incomplete or corrupt record at the end
        }

        // read the fixed header after rec_len (key_len + flags + reserved + hash32) (8 bytes)
        let mut hdr = [0u8; 8];
        if let Err(_) = fd.read_exact_at(&mut hdr, off as u64 + 4) {
            break; // incomplete header at the end
        }

        // retrieve the hash32 from the header
        let hash = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
        footer.push((hash, off));

        // move to the next record
        off += rec_len; 
    }

    // sort by hash32
    footer.sort_by_key(|e| e.0);

    // wrtie the footer to the segment's footer file (overwrite existing)
    let mut ftr = fs::File::create(&meta.ftr_path)?;
    for (h, o) in footer {
        ftr.write_all(&h.to_le_bytes())?;
        ftr.write_all(&o.to_le_bytes())?;
    }
    ftr.sync_all()
}

impl LogStructuredStore {
    /// Open (or create) a store in `dir`.
    pub fn open<P: AsRef<std::path::Path>>(dir: P) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // discover *.log, sotring them in a vector by id descending (newest first)
        let mut ids: Vec<u64> = fs::read_dir(&dir)?
            .filter_map(|e| {
                let name = e.ok()?.file_name();
                let name = name.to_str()?;
                if let Some(id_str) = name.strip_suffix(".log") {
                    u64::from_str_radix(id_str, 16).ok()
                } else {
                    None
                }
            })
            .collect();
        ids.sort_unstable_by(|a, b| b.cmp(a)); // descending order

        let mut sealed_readers = Vec::new(); 
        let mut active_writer: Option<SegmentWriter> = None;
        
        // open existing segments
        for (idx, &id) in ids.iter().enumerate() {
            let meta = SegmentInfo::new(&dir, id);

            // try to open the segment reader
            let reader = match SegmentReader::open(meta.clone()) {
                Ok(r) => r, 
                Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                    eprintln!("logstore: rebuilding footer for segment {:016X}", id);
                    rebuild_footer(&meta)?;
                    SegmentReader::open(meta.clone())?
                }
                Err(e) => return Err(e), // real I/O error
            };

            // decide whether to reuse the latest segment as the active writer
            if idx == 0 {
                let log_len = reader.log_len()? as u32;
                if log_len < MAX_SEG_SIZE {
                    // reuse this segment as the active writer
                    active_writer = Some(SegmentWriter::open(&dir, id, MAX_SEG_SIZE)?);
                    continue; // skip adding this reader
                }
            }

            // add the reader to the sealed list
            sealed_readers.push(reader);
        }

        // create a new active segment writer if no reusable segment was found
        let writer = active_writer.unwrap_or_else(|| {
            let next_id = ids.first().map_or(0, |&id| id + 1);
            SegmentWriter::create(&dir, next_id, MAX_SEG_SIZE).unwrap()          
        });

        Ok(Self {
            dir,
            active: Mutex::new(writer),
            sealed: Mutex::new(sealed_readers),
            max_seg_size: MAX_SEG_SIZE,
            last_flush: Mutex::new(Instant::now()),
        })
    }

    pub fn put(&self, key: Vec<u8>, val: Vec<u8>) -> io::Result<()> {
        let len_needed = crate::record::FIXED_HDR + key.len() + val.len() + 4; // 4 for CRC32
        let mut writer = self.active.lock().unwrap();

        // check if we need to roll the segment
        if writer.bytes_written() + len_needed as u32 > self.max_seg_size {
            self.roll_segment(&mut *writer)?;
        }

        // append the record
        writer.append_put(&key, &val)?;

        self.check_periodic_flush(&mut *writer)
    }

    pub fn delete(&self, key: &[u8]) -> io::Result<()> {
        let len_needed = crate::record::FIXED_HDR + key.len() + 4; // 4 for CRC32
        let mut writer = self.active.lock().unwrap();

        // check if we need to roll the segment
        if writer.bytes_written() + len_needed as u32 > self.max_seg_size {
            self.roll_segment(&mut *writer)?;
        }

        // append the delete record
        writer.append_delete(key)?;

        self.check_periodic_flush(&mut *writer)
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let hash = crate::record::hash32(key);

        // search in the active segment first
        if let Some(value) = self.active.lock().unwrap().lookup_in_mem(hash, key)? {
            return Ok(value);
        }

        // then search in the sealed segments from newest to oldest
        for reader in self.sealed.lock().unwrap().iter() {
            if let Some(value) = reader.lookup(hash, key)? {
                return Ok(Some(value));
            }
        }

        // not found
        Ok(None)
    }

    // Flush the active segment writer's footer to disk.
    pub fn flush(&self) -> io::Result<()> {
        self.active.lock().unwrap().flush_footer()
    }

    // Compact the log store by merging segments.
    // This is a placeholder; actual compaction logic is not implemented yet.
    pub fn compact(&self) -> io::Result<()> { Ok(()) }

    fn roll_segment(&self, old: &mut SegmentWriter) -> io::Result<()> {
        // create the replacement before invalidating the old one
        let next_id = old.meta.id + 1;
        let new_writer = SegmentWriter::create(&self.dir, next_id, self.max_seg_size)?;

        // move the current writer out of the mutex slot
        let old_writer = std::mem::replace(old, new_writer);

        // seal the moved-out writer, push its reader
        let reader = old_writer.seal()?;
        self.sealed.lock().unwrap().insert(0, reader); // insert at the front (newest first)

        Ok(())
    }

    fn check_periodic_flush(&self, w: &mut SegmentWriter) -> io::Result<()> {
        let mut last_flush = self.last_flush.lock().unwrap();
        if last_flush.elapsed() >= FLUSH_EVERY {
            w.flush_footer()?;
            *last_flush = Instant::now();
        }
        Ok(())
    }

    /// Iterate over all records in the store within the given hash range.
    /// The range is inclusive of `lo` and exclusive of `hi`. [lo, hi)
    /// This function is for migration
    /// It returns an iterator over `(key, value)` pair that matches the range.
    pub fn iter_range(
        &self,
        lo: u32,
        hi: u32,
    ) -> io::Result<impl Iterator<Item = (Vec<u8>, Vec<u8>)>> {
        // flush active footer so view is consistent
        // wrapped to limit the lifetime of the lock
        {
            let mut w = self.active.lock().unwrap();
            w.flush_footer()?;
        }

        // collect matching pairs into a Vec while we hold the lock
        let mut items = Vec::<(Vec<u8>, Vec<u8>)>::new();
        let sealed = self.sealed.lock().unwrap();

        for seg in sealed.iter() {
            for (h, off) in seg.footer_pairs() {
                if !(lo < h && h <= hi) { continue; }

                let buf = seg.read_record_bytes(off)?;
                let rec = crate::record::Record::decode(&mut &buf[..])?;

                if rec.flags == crate::record::RecordFlags::Put {
                    items.push((rec.key.to_vec(), rec.value.to_vec()));
                }
            }
        }

        // return an iterator that owns the Vec (lock already released)
        Ok(items.into_iter())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_log_store_basic() -> io::Result<()> {
        let temp_dir = TempDir::new()?;
        let store = LogStructuredStore::open(temp_dir.path())?;

        // put some records
        store.put(b"key1".to_vec(), b"value1".to_vec())?;
        store.put(b"key2".to_vec(), b"value2".to_vec())?;
        store.put(b"key3".to_vec(), b"value3".to_vec())?;

        // get records
        assert_eq!(store.get(b"key1")?, Some(b"value1".to_vec()));
        assert_eq!(store.get(b"key2")?, Some(b"value2".to_vec()));
        assert_eq!(store.get(b"key3")?, Some(b"value3".to_vec()));
        assert_eq!(store.get(b"key4")?, None); // not found

        // delete a record
        store.delete(b"key2")?;
        assert_eq!(store.get(b"key2")?, None); // should be deleted

        Ok(())
    }

    #[test]
    fn store_basic_put_get_roll() {
        let dir = TempDir::new().unwrap();
        let store = LogStructuredStore::open(dir.path()).unwrap();

        // tiny max size to force roll
        store.put(b"K1".to_vec(), vec![0; 1024]).unwrap(); // 1 KiB
        store.put(b"K2".to_vec(), vec![0; 1024]).unwrap();

        // retrieve
        assert!(store.get(b"K1").unwrap().is_some());

        // flush & drop (graceful shutdown)
        store.flush().unwrap();
        drop(store);

        // reopen -> data still there
        let reopened = LogStructuredStore::open(dir.path()).unwrap();
        assert!(reopened.get(b"K2").unwrap().is_some());
        reopened.delete(b"K2").unwrap();
        assert!(reopened.get(b"K2").unwrap().is_none());
    }

    #[test]
    fn rebuild_footer_after_crash() {
        use std::fs;

        let dir = TempDir::new().unwrap();
        let mut w = SegmentWriter::create(dir.path(), 0, 4 * 1024 * 1024).unwrap();

        // Append some records
        w.append_put(b"alpha", b"one").unwrap();
        w.append_put(b"beta",  b"two").unwrap();
        w.append_delete(b"alpha").unwrap();

        // seal the writer to flush footer into the .ftr file
        let rdr = w.seal().unwrap();

        // Simulate crash: delete the .ftr file
        fs::remove_file(&rdr.meta.ftr_path.clone()).unwrap();

        // check if the footer is missing now
        assert!(!rdr.meta.ftr_path.exists(), "Footer file should be deleted");

        // Rebuild the footer from the log file
        crate::log_store::rebuild_footer(&rdr.meta).unwrap();

        // footer should now exist
        assert!(rdr.meta.ftr_path.exists(), "Footer file should be rebuilt");

        // Reopen the segment and verify lookups
        let rdr2 = SegmentReader::open(rdr.meta.clone()).unwrap();
        assert!(rdr2.lookup(crate::record::hash32(b"alpha"), b"alpha").unwrap().is_none());
        assert_eq!(rdr2.lookup(crate::record::hash32(b"beta"), b"beta").unwrap(), Some(b"two".to_vec()));
    }

    #[test]
    fn test_reuse_segment_as_writer() {
        // Create a temp directory and open the store
        let dir = TempDir::new().unwrap();
        let store = LogStructuredStore::open(dir.path()).unwrap();

        // Fill less than MAX_SEG_SIZE so the segment is not full
        let small_val = vec![1u8; 1024]; // 1 KiB
        store.put(b"reuse1".to_vec(), small_val.clone()).unwrap();

        // Ensure the record is retrievable
        assert_eq!(store.get(b"reuse1").unwrap(), Some(small_val.clone()));

        // Drop the store to simulate shutdown (footer hashn't been flushed)
        drop(store);

        // Reopen the store, which should reuse the existing segment as writer
        let store2 = LogStructuredStore::open(dir.path()).unwrap();

        // Put another record, which should go into the same segment
        store2.put(b"reuse2".to_vec(), small_val.clone()).unwrap();

        // Both records should be retrievable
        assert_eq!(store2.get(b"reuse1").unwrap(), Some(small_val.clone()));
        assert_eq!(store2.get(b"reuse2").unwrap(), Some(small_val.clone()));

        // Ensure only one .log file exists (no roll)
        let log_count = std::fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| e.as_ref().unwrap().file_name().to_str().unwrap().ends_with(".log"))
            .count();
        assert_eq!(log_count, 1, "Should only be one segment log file");
    }
}