/*
TODO: 
- Compaction work not implemented yet. (placeholder only)
*/

use crate::segment::{SegmentWriter, SegmentReader};
use std::{
    fs, 
    io, 
    path::{Path, PathBuf}, 
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
            let meta = segment::SegmentInfo::new(&dir, id);
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

    fn roll_segment(self, old: &mut SegmentWriter) -> io::Result<()> {
        // seal old writer -> reader
        let reader = old.seal()?;
        self.sealed.lock().unwrap().insert(0, reader); // insert at the front (newest first)

        // create a new writer
        let next_id = old.meta.id + 1;
        *old = SegmentWriter::create(&self.dir, next_id, self.max_seg_size)?;
        
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
        
        // seal the active writer first for a consistent view
        let mut writer = self.active.lock().unwrap();
        writer.flush_footer()?;
        drop(writer); // release the lock

        let sealed = self.sealed.lock().unwrap().clone();
        Ok(sealed.into_iter().flat_map(move |seg| {
            // naive full-scan iterator; optimize later
            (0..seg.footer_mm.len() / 8).filter_map(move |i| {
                let slot = &seg.footer_mm[i * 8..(i + 1) * 8];
                let hash = u32::from_le_bytes(slot[0..4].try_into().unwrap());
                if hash < lo || hash >= hi {
                    return None; // out of range
                }
                let offset = u32::from_le_bytes(slot[4..8].try_into().unwrap());
                
                // read the record at this offset
                let mut len_buf = [0u8; 4];
                seg.log_fd.read_exact_at(&mut len_buf, offset as u64).ok()?;
                let rec_len = u32::from_le_bytes(len_buf) as usize + 4; // +4 for the length itself
                let mut rec_buf = vec![0u8; rec_len];
                seg.log_fd.read_exact_at(&mut rec_buf, offset as u64).ok()?;

                // decode the record
                match crate::record::Record::decode(&mut rec_buf[..]) {
                    Ok(rec) => Some((rec.key.to_vec(), rec.value.to_vec())),
                    Err(_) => None, // skip corrupted records
                }
            })
        }))
    }
}