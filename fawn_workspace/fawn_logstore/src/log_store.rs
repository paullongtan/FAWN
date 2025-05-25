/*
TODO: 
- Compaction work not implemented yet. (placeholder only)
*/

use crate::segment::{SegmentInfo, SegmentWriter, SegmentReader};
use std::{
    fs, 
    io, 
    path::PathBuf, 
    sync::Mutex, 
    time::{Duration, Instant}
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

        // open existing segments
        let mut sealed_readers = Vec::new(); 
        for &id in &ids {
            let meta = SegmentInfo::new(&dir, id);
            let reader = SegmentReader::open(meta)?;
            sealed_readers.push(reader);
        }

        // create a new active segment writer
        let next_id = ids.first().map_or(0, |&id| id + 1);
        let writer = SegmentWriter::create(&dir, next_id, MAX_SEG_SIZE)?;

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

        // flush & drop
        store.flush().unwrap();
        drop(store);

        // reopen -> data still there
        let reopened = LogStructuredStore::open(dir.path()).unwrap();
        assert!(reopened.get(b"K2").unwrap().is_some());
        reopened.delete(b"K2").unwrap();
        assert!(reopened.get(b"K2").unwrap().is_none());
    }
}