/*
TODO: 
- Compaction work not implemented yet. (placeholder only)
*/

use super::segment::{SegmentInfo, SegmentWriter, SegmentReader};
use super::pointer::RecordPtr;
use super::record::{self, Record, RecordFlags};
use std::result;
use std::{
    fs, io::{self, Write}, os::unix::fs::FileExt, path::PathBuf, sync::{Mutex, RwLock}, time::{Duration, Instant}
};
use std::collections::HashSet;
use log;

const FLUSH_EVERY: Duration = Duration::from_secs(5); // flush every 5 seconds
const MAX_SEG_SIZE: u32 = 4 * 1024 * 1024; // 4 MiB max segment size
const EMPTY_KEY: &[u8] = b""; // empty key for raw hash operations

pub struct LogStructuredStore {
    dir:           PathBuf,
    active:        Mutex<SegmentWriter>, // use Mutex to ensure only one thread writes at a time
    sealed:        RwLock<Vec<SegmentReader>>, // sorted by id descending (newest first) (use RwLock to allow concurrent reads) (write lock for rolling)
    max_seg_size:  u32,
    last_flush:    Mutex<Instant>, // last time we flushed the active segment's footer  
}

/// Rebuild the footer for a segment by reading the log file and writing the footer file.
fn rebuild_footer(meta: &super::segment::SegmentInfo) -> io::Result<()> {
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

/// Convert a 32-bit hash to a 4-byte array in little-endian order.
#[inline]
fn key_bytes(id: u32) -> [u8; 4] { id.to_le_bytes() }

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
            sealed: RwLock::new(sealed_readers),
            max_seg_size: MAX_SEG_SIZE,
            last_flush: Mutex::new(Instant::now()),
        })
    }

    /// Let user directly put a record using a hashed key using their own hash function.
    pub fn put(&self, hashed_key: u32, value: Vec<u8>) -> io::Result<RecordPtr> {
        let key = EMPTY_KEY; // we don't use the key here, just the hash
        let len_needed = super::record::FIXED_HDR + key.len() + value.len() + 4; // 4 for CRC32
        let mut writer = self.active.lock().unwrap();

        // check if we need to roll the segment (no record crossing allowed)
        if writer.bytes_written() + len_needed as u32 > self.max_seg_size {
            self.roll_segment(&mut *writer)?;
        }

        // append the record
        writer.append_put(hashed_key, key, &value)?;

        self.check_periodic_flush(&mut *writer)?;

        // return the pointer to the tail of the record
        Ok(RecordPtr {
            seg_id: writer.meta.id,
            offset: writer.get_current_offset(), 
        })        
    }


    pub fn delete(&self, hashed_key: u32) -> io::Result<RecordPtr> {
        let key = EMPTY_KEY; // we don't use the key here, just the hash
        let len_needed = super::record::FIXED_HDR + key.len() + 4; // 4 for CRC32
        let mut writer = self.active.lock().unwrap();

        // check if we need to roll the segment
        if writer.bytes_written() + len_needed as u32 > self.max_seg_size {
            self.roll_segment(&mut *writer)?;
        }

        let offset_before = writer.get_current_offset();
        
        // append the delete record
        writer.append_delete(hashed_key, key)?;

        self.check_periodic_flush(&mut *writer)?;

        // return the pointer to the head of the record
        Ok(RecordPtr {
            seg_id: writer.meta.id,
            offset: offset_before, 
        })        
    }


    pub fn get(&self, hashed_key: u32) -> io::Result<Option<Vec<u8>>> {
        let key = EMPTY_KEY; // we don't use the key here, just the hash

        // search in the active segment first
        if let Some(value) = self.active.lock().unwrap().lookup_in_mem(hashed_key, key)? {
            return Ok(value);
        }

        // then search in the sealed segments from newest to oldest
        for reader in self.sealed.read().unwrap().iter() {
            if let Some(value) = reader.lookup(hashed_key, key)? {
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
        // use write lock to ensure no concurrent reads while sealing
        let reader = old_writer.seal()?;
        self.sealed.write().unwrap().insert(0, reader); // insert at the front (newest first)

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

    pub fn iter_range(
        &self,
        lo: u32,
        hi: u32,
    ) -> io::Result<impl Iterator<Item = (u32, Vec<u8>)>> {
        // flush footer and clone meta of active segment
        let meta = {
            let mut w = self.active.lock().unwrap();
            w.flush_footer()?;
            w.meta.clone()
        };

        // open a read-only view of the active segment
        let active_seg = super::segment::SegmentReader::open(meta)?;
        // collect matches from active + sealed
        let mut seen: HashSet<u32> = HashSet::new(); // only keep the first occurrence of each hash
        let mut items = Vec::<(u32, Vec<u8>)>::new(); // [hash32, value(in bytes)]

        // helper closure for ring range check
        // if (lo < hi): range is (lo, hi]
        // if (lo > hi): range is (lo, MAX] U [0, hi]
        let in_range = |h: u32| -> bool {
            if lo < hi {
                lo < h && h <= hi
            } else if lo > hi {
                h > lo || h <= hi
            } else {
                true // if lo == hi, include all (includes everything)
            }
        };

        // scan the active segment first
        for (h, off) in active_seg.footer_pairs() {
            if !in_range(h) { continue; }
            if seen.contains(&h) { continue; } // skip if already seen (currently, we consider no hash collision)

            let buf = active_seg.read_record_bytes(off)?;
            let rec = super::record::Record::decode(&mut &buf[..])?;
            if rec.flags == super::record::RecordFlags::Put {
                seen.insert(rec.hash32); // mark as seen
                items.push((rec.hash32, rec.value.to_vec()));
            } else if rec.flags == super::record::RecordFlags::Delete {
                seen.insert(rec.hash32); // mark as seen to avoid including old PUT record
            }
        }

        // scan the sealed segments
        for seg in self.sealed.read().unwrap().iter() {
            for (h, off) in seg.footer_pairs() {
                if !in_range(h) { continue; }
                if seen.contains(&h) { continue; } // skip if already seen (currently, we consider no hash collision)

                let buf = seg.read_record_bytes(off)?;
                let rec = super::record::Record::decode(&mut &buf[..])?;
                if rec.flags == super::record::RecordFlags::Put {
                    items.push((rec.hash32, rec.value.to_vec()));
                } else if rec.flags == super::record::RecordFlags::Delete {
                    seen.insert(rec.hash32); // mark as seen to avoid including old PUT record
                }
            }
        }

        Ok(items.into_iter())
    }

    pub fn truncate_after_ptr(&self, ptr: RecordPtr) -> io::Result<()> {
        // remove sealed segments with id > ptr.seg_id
        {
            let mut sealed = self.sealed.write().unwrap();
            for seg in sealed.iter() {
                if seg.meta.id > ptr.seg_id {
                    let _ = fs::remove_file(&seg.meta.log_path);
                    let _ = fs::remove_file(&seg.meta.ftr_path);
                }
            }
            sealed.retain(|seg| seg.meta.id <= ptr.seg_id);
        }

        // remove active segment if needed
        {
            let mut active = self.active.lock().unwrap();
            if active.meta.id > ptr.seg_id {
                let _ = fs::remove_file(&active.meta.log_path);
                let _ = fs::remove_file(&active.meta.ftr_path);
                // TODO: set active to None
            }
        }

        // truncate the segment at ptr.seg_id to ptr.offset
        let seg_path = self.dir.join(format!("{:016X}.log", ptr.seg_id));
        let file = fs::OpenOptions::new().write(true).open(&seg_path)?;
        file.set_len(ptr.offset as u64)?;

        // rebuild the footer for the truncated segment
        let meta = SegmentInfo::new(&self.dir, ptr.seg_id);
        rebuild_footer(&meta)?;

        // update sealed list: remove all segments with id >= ptr.seg_id, and refresh the reader for ptr.seg_id
        {
            let mut sealed = self.sealed.write().unwrap();
            sealed.retain(|seg| seg.meta.id < ptr.seg_id);
        }

        // reopen the segment at ptr.seg_id as active writer
        {
            let mut active = self.active.lock().unwrap();
            *active = SegmentWriter::open(&self.dir, ptr.seg_id, self.max_seg_size)?;
        }

        Ok(())
    }

    fn scan_between_ptr_with_filter<F>(
        &self, 
        start_ptr: RecordPtr, 
        end_ptr: RecordPtr,
        filter: F, 
    ) -> io::Result<Vec<(RecordPtr, RecordFlags, u32, Vec<u8>)>>
    where 
        F: Fn(u32) -> bool, 
    {
        let mut results = Vec::new();
        log::info!("Starting scan between ptrs: start={:?}, end={:?}", start_ptr, end_ptr);

        // sealed segments: reverse order (oldest to newest)
        let sealed = self.sealed.read().unwrap();
        log::info!("Got sealed segments lock, found {} segments", sealed.len());

        // scan sealed segments from oldest to newest
        for (i, seg) in sealed.iter().rev().enumerate() {
            if seg.meta.id < start_ptr.seg_id {
                log::info!("Skipping segment {} as it's before start_ptr", seg.meta.id);
                continue;
            }
            log::info!("Scanning sealed segment {} ({}/{})", seg.meta.id, i+1, sealed.len());
            scan_segment_between_ptr(seg, start_ptr, end_ptr, &mut results, &filter)?;
        }

        // scan active segment
        // flush footer and clone meta of active segment
        log::info!("Preparing to scan active segment");
        let meta = {
            let mut w = self.active.lock().unwrap();
            log::info!("Got active segment lock");

            // If the active segment is empty, there's nothing to scan.
            if w.bytes_written() == 0 {
                log::info!("Active segment is empty, skipping scan.");
                log::info!("Scan complete, found {} records", results.len());
                return Ok(results);
            }

            w.flush_footer()?;
            log::info!("Flushed footer of active segment");
            w.meta.clone()
        };
        log::info!("Opening active segment reader");
        let active_reader = super::segment::SegmentReader::open(meta)?;
        log::info!("Scanning active segment");
        scan_segment_between_ptr(&active_reader, start_ptr, end_ptr, &mut results, &filter)?;
        
        log::info!("Scan complete, found {} records", results.len());
        Ok(results)
    }

    pub fn scan_between_ptr_in_range(
        &self, 
        start_ptr: RecordPtr, 
        end_ptr: RecordPtr,
        range: Option<(u32, u32)>
    ) -> io::Result<Vec<(RecordPtr, RecordFlags, u32, Vec<u8>)>> {
        // if range is None, we scan all records after start_ptr
        let filter: Box<dyn Fn(u32) -> bool> = match range {
            Some((lo, hi)) => Box::new(move |h| {
                if lo < hi {
                    lo < h && h <= hi
                } else if lo > hi {
                    h > lo || h <= hi
                } else {
                    false
                }
            }),
            None => Box::new(|_| true),
        };
        self.scan_between_ptr_with_filter(start_ptr, end_ptr, filter)
    }

    fn scan_after_ptr_with_filter<F>(
        &self, 
        start_ptr: RecordPtr, 
        filter: F, 
    ) -> io::Result<Vec<(RecordPtr, RecordFlags, u32, Vec<u8>)>>
    where 
        F: Fn(u32) -> bool, 
    {
        let mut results = Vec::new();

        // sealed segments: reverse order (oldest to newest)
        let sealed = self.sealed.read().unwrap();

        // scan sealed segments from oldest to newest
        for seg in sealed.iter().rev() {
            if seg.meta.id < start_ptr.seg_id {
                continue;
            }
            scan_segment_after_ptr(seg, start_ptr, &mut results, &filter)?;
        }

        // scan active segment
        // flush footer and clone meta of active segment
        let meta = {
            let mut w = self.active.lock().unwrap();
            w.flush_footer()?;
            w.meta.clone()
        };
        let active_reader = super::segment::SegmentReader::open(meta)?;
        scan_segment_after_ptr(&active_reader, start_ptr, &mut results, &filter)?;
        
        Ok(results)
    }

    pub fn scan_after_ptr_in_range(
        &self, 
        start_ptr: RecordPtr, 
        range: Option<(u32, u32)>
    ) -> io::Result<Vec<(RecordPtr, RecordFlags, u32, Vec<u8>)>> {
        // if range is None, we scan all records after start_ptr
        let filter: Box<dyn Fn(u32) -> bool> = match range {
            Some((lo, hi)) => Box::new(move |h| {
                if lo < hi {
                    lo < h && h <= hi
                } else if lo > hi {
                    h > lo || h <= hi
                } else {
                    false
                }
            }),
            None => Box::new(|_| true),
        };
        self.scan_after_ptr_with_filter(start_ptr, filter)
    }

    // Private API: for appending records with key bytes
    fn _put_with_key(&self, key: Vec<u8>, val: Vec<u8>) -> io::Result<()> {
        let len_needed = super::record::FIXED_HDR + key.len() + val.len() + 4; // 4 for CRC32
        let mut writer = self.active.lock().unwrap();

        // check if we need to roll the segment
        if writer.bytes_written() + len_needed as u32 > self.max_seg_size {
            self.roll_segment(&mut *writer)?;
        }

        // append the record
        writer.append_put_with_key(&key, &val)?;

        self.check_periodic_flush(&mut *writer)
    }

    fn _delete_with_key(&self, key: &[u8]) -> io::Result<()> {
        let len_needed = super::record::FIXED_HDR + key.len() + 4; // 4 for CRC32
        let mut writer = self.active.lock().unwrap();

        // check if we need to roll the segment
        if writer.bytes_written() + len_needed as u32 > self.max_seg_size {
            self.roll_segment(&mut *writer)?;
        }

        // append the delete record
        writer.append_delete_with_key(key)?;

        self.check_periodic_flush(&mut *writer)
    }

    fn _get_with_key(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let hash = super::record::hash32(key);

        // search in the active segment first
        if let Some(value) = self.active.lock().unwrap().lookup_in_mem(hash, key)? {
            return Ok(value);
        }

        // then search in the sealed segments from newest to oldest
        for reader in self.sealed.read().unwrap().iter() {
            if let Some(value) = reader.lookup(hash, key)? {
                return Ok(Some(value));
            }
        }

        // not found
        Ok(None)
    }
}


fn scan_segment_after_ptr<F>(
    seg: &SegmentReader,
    start_ptr: RecordPtr,
    results: &mut Vec<(RecordPtr, RecordFlags, u32, Vec<u8>)>,
    in_range: F,
) -> io::Result<()>
where
    F: Fn(u32) -> bool,
{
    let seg_id = seg.meta.id;
    for (_hash, offset) in seg.footer_pairs() {
        if seg_id == start_ptr.seg_id && offset < start_ptr.offset {
            continue;
        }

        let buf = seg.read_record_bytes(offset)?;
        let rec = super::record::Record::decode(&mut &buf[..])?;

        if !in_range(rec.hash32) {
            continue;
        }

        results.push((
            RecordPtr { seg_id, offset },
            rec.flags,
            rec.hash32,
            rec.value.to_vec(),
        ));
    }
    Ok(())
}

fn scan_segment_between_ptr(
    seg: &SegmentReader,
    start_ptr: RecordPtr,
    end_ptr: RecordPtr,
    results: &mut Vec<(RecordPtr, RecordFlags, u32, Vec<u8>)>,
    filter: &impl Fn(u32) -> bool,
) -> io::Result<()> {
    log::info!("Starting scan of segment {} between ptrs: start={:?}, end={:?}", seg.meta.id, start_ptr, end_ptr);
    
    // get all records in this segment
    let mut offset: u32 = 0;
    while offset < seg.log_len()? as u32 {
        let ptr = RecordPtr {
            seg_id: seg.meta.id,
            offset,
        };
        
        // stop if we've reached the end pointer
        if ptr >= end_ptr {
            log::info!("Reached end pointer in segment {}", seg.meta.id);
            break;
        }
        
        // skip if before start pointer
        if ptr < start_ptr {
            log::info!("Skipping record at offset {} in segment {} as it's before start_ptr", offset, seg.meta.id);
            offset += 1;
            continue;
        }
        
        // read the record
        let buf = seg.read_record_bytes(offset)?;
        let rec = super::record::Record::decode(&mut &buf[..])?;
        
        // apply the filter
        if filter(rec.hash32) {
            results.push((ptr, rec.flags, rec.hash32, rec.value.to_vec()));
        }
        
        offset += buf.len() as u32;
    }
    
    log::info!("Finished scanning segment {}, found {} records", seg.meta.id, results.len());
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use fawn_common::util::get_key_id;

    #[test]
    fn test_log_store_basic() {
        let temp_dir = TempDir::new().unwrap();
        let store = LogStructuredStore::open(temp_dir.path()).unwrap();

        // put some records using hashed key ids
        let key1 = "key1";
        let key2 = "key2";
        let key3 = "key3";
        let key4 = "key4";
        let id1 = get_key_id(key1);
        let id2 = get_key_id(key2);
        let id3 = get_key_id(key3);
        let id4 = get_key_id(key4);

        store.put(id1, b"value1".to_vec());
        store.put(id2, b"value2".to_vec());
        store.put(id3, b"value3".to_vec());

        // get records using hashed key ids
        assert_eq!(store.get(id1).unwrap(), Some(b"value1".to_vec()));
        assert_eq!(store.get(id2).unwrap(), Some(b"value2".to_vec()));
        assert_eq!(store.get(id3).unwrap(), Some(b"value3".to_vec()));
        assert_eq!(store.get(id4).unwrap(), None); // not found

        // delete a record using hashed key id
        store.delete(id2);
        assert_eq!(store.get(id2).unwrap(), None); // should be deleted

        // put another record to key2
        store.put(id2, b"value2_updated".to_vec());
        assert_eq!(store.get(id2).unwrap(), Some(b"value2_updated".to_vec()));
    }

    #[test]
    fn store_basic_put_get_roll() {
        let dir = TempDir::new().unwrap();
        let store = LogStructuredStore::open(dir.path()).unwrap();

        let key1 = "key1";
        let key2 = "key2";
        let id1 = get_key_id(key1);
        let id2 = get_key_id(key2);

        // tiny max size to force roll
        store.put(id1, vec![0; 1024]).unwrap(); // 1 KiB
        store.put(id2, vec![0; 1024]).unwrap();

        // retrieve
        assert!(store.get(id1).unwrap().is_some());

        // flush & drop (graceful shutdown)
        store.flush().unwrap();
        drop(store);

        // reopen -> data still there
        let reopened = LogStructuredStore::open(dir.path()).unwrap();
        assert!(reopened.get(id2).unwrap().is_some());
        reopened.delete(id2).unwrap();
        assert!(reopened.get(id2).unwrap().is_none());
    }


    #[test]
    fn rebuild_footer_after_crash() {
        use std::fs;

        let dir = TempDir::new().unwrap();
        let mut w = SegmentWriter::create(dir.path(), 0, 4 * 1024 * 1024).unwrap();

        let key1 = "alpha";
        let key2 = "beta";
        let id1 = get_key_id(key1);
        let id2 = get_key_id(key2);

        // Append some records
        w.append_put(id1, &[], b"one").unwrap();
        w.append_put(id2, &[],  b"two").unwrap();
        w.append_delete(id1, &[]).unwrap();

        // seal the writer to flush footer into the .ftr file
        let rdr = w.seal().unwrap();

        // Simulate crash: delete the .ftr file
        fs::remove_file(&rdr.meta.ftr_path.clone()).unwrap();

        // check if the footer is missing now
        assert!(!rdr.meta.ftr_path.exists(), "Footer file should be deleted");

        // Rebuild the footer from the log file
        rebuild_footer(&rdr.meta).unwrap();

        // footer should now exist
        assert!(rdr.meta.ftr_path.exists(), "Footer file should be rebuilt");

        // Reopen the segment and verify lookups
        let rdr2 = SegmentReader::open(rdr.meta.clone()).unwrap();
        assert!(rdr2.lookup(id1, &[]).unwrap().is_none());
        assert_eq!(rdr2.lookup(id2, &[]).unwrap(), Some(b"two".to_vec()));
    }

    #[test]
    fn test_reuse_segment_as_writer() {
        // Create a temp directory and open the store
        let dir = TempDir::new().unwrap();
        let store = LogStructuredStore::open(dir.path()).unwrap();

        let key1 = "reuse1";
        let key2 = "reuse2";
        let id1 = get_key_id(key1);
        let id2 = get_key_id(key2);

        // Fill less than MAX_SEG_SIZE so the segment is not full
        let small_val = vec![1u8; 1024]; // 1 KiB
        store.put(id1, small_val.clone()).unwrap();

        // Ensure the record is retrievable
        assert_eq!(store.get(id1).unwrap(), Some(small_val.clone()));

        // Drop the store to simulate shutdown (footer hashn't been flushed)
        drop(store);

        // Reopen the store, which should reuse the existing segment as writer
        let store2 = LogStructuredStore::open(dir.path()).unwrap();

        // Put another record, which should go into the same segment
        store2.put(id2, small_val.clone()).unwrap();

        // Both records should be retrievable
        assert_eq!(store2.get(id1).unwrap(), Some(small_val.clone()));
        assert_eq!(store2.get(id2).unwrap(), Some(small_val.clone()));

        // Ensure only one .log file exists (no roll)
        let log_count = std::fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| e.as_ref().unwrap().file_name().to_str().unwrap().ends_with(".log"))
            .count();
        assert_eq!(log_count, 1, "Should only be one segment log file");
    }

    #[test]
    fn test_concurrent_write_and_read() {
        use std::sync::Arc; // Arc for shared ownership
        use std::thread;
        use std::time::Duration;

        let dir = TempDir::new().unwrap();
        let store = Arc::new(LogStructuredStore::open(dir.path()).unwrap());

        // Spawn writer threads
        let writers: Vec<_> = (0..4).map(|i| {
            let store = store.clone();
            thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("key{}_{}", i, j);
                    let id = get_key_id(&key);
                    let val = vec![i as u8, j as u8];
                    store.put(id, val).unwrap();
                }
            })
        }).collect();

        // Spawn reader threads
        let readers: Vec<_> = (0..4).map(|i| {
            let store = store.clone();
            thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("key{}_{}", i, j);
                    let id = get_key_id(&key);
                    // Try to read, may be None if not written yet
                    let _ = store.get(id);
                    // Optionally sleep to increase interleaving
                    thread::sleep(Duration::from_millis(1));
                }
            })
        }).collect();

        // Wait for all threads to finish
        for w in writers { w.join().unwrap(); }
        for r in readers { r.join().unwrap(); }

        // Verify some data
        for i in 0..4 {
            for j in 0..100 {
                let key = format!("key{}_{}", i, j);
                let id = get_key_id(&key);
                let val = vec![i as u8, j as u8];
                assert_eq!(store.get(id).unwrap(), Some(val));
            }
        }
    }

    #[test]
    fn test_iter_range() {
        let temp_dir = TempDir::new().unwrap();
        let store = LogStructuredStore::open(temp_dir.path()).unwrap();

        // Insert records
        let keys = vec!["apple", "banana", "cherry", "date"];
        let values: Vec<&[u8]> = vec![b"red".as_ref(), b"yellow".as_ref(), b"red".as_ref(), b"brown".as_ref()];
        let mut ids = Vec::new();
        for (k, v) in keys.iter().zip(values.iter()) {
            let id = get_key_id(k);
            ids.push(id);
            store.put(id, v.to_vec()).unwrap();
        }

        // assert that we can retrieve the values
        for (k, v) in keys.iter().zip(values.iter()) {
            let id = get_key_id(k);
            assert_eq!(store.get(id).unwrap(), Some(v.to_vec()));
        }

        // Sort ids to pick a range
        let mut sorted_ids = ids.clone();
        sorted_ids.sort_unstable();

        // Pick a range that includes "banana" and "cherry"
        let lo = sorted_ids[0]; // apple
        let hi = sorted_ids[2]; // cherry

        println!("Iterating range: ({}, {}]", lo, hi);

        // collect results in (lo, hi] range
        let results: Vec<_> = store.iter_range(lo, hi).unwrap().collect();
        println!("Found {} records in range", results.len());
        assert_eq!(results.len(), 2); // should find banana and cherry

        // // Find which keys are in the range
        // let expected: Vec<_> = keys.iter()
        //     .zip(values.iter())
        //     .filter(|(k, _)| {
        //         let id = get_key_id(k);
        //         id > lo && id <= hi
        //     })
        //     .map(|(k, v)| (EMPTY_KEY.to_vec().to_vec(), v.to_vec()))
        //     .collect();

        // assert_eq!(results, expected);
    }

    #[test]
    fn test_iter_range_ring() {
        let temp_dir = TempDir::new().unwrap();
        let store = LogStructuredStore::open(temp_dir.path()).unwrap();

        // insert records with known hash ids
        let keys = vec!["a", "b", "c", "d"];    
        let values = vec![b"va", b"vb", b"vc", b"vd"];

        // Insert records with known hash ids
        let mut ids = Vec::new();
        for (k, v) in keys.iter().zip(values.iter()) {
            let id = get_key_id(k);
            ids.push(id);
            store.put(id, v.to_vec()).unwrap();
        }

        println!("Inserted records with IDs: {:?}", ids);

        // Sort ids to pick a range
        let mut sorted_ids = ids.clone();
        sorted_ids.sort_unstable();
        println!("Sorted IDs: {:?}", sorted_ids);

        // linear range: (sorted_ids[0], sorted_ids[2]], should include sorted_ids[1] and sorted_ids[2] 
        let results: Vec<_> = store.iter_range(sorted_ids[0], sorted_ids[2]).unwrap().collect();
        println!("Results in linear range: {:?}", results);
        assert_eq!(results.len(), 2); // should find sorted_ids[1] and sorted_ids[2] 
        assert!(results.iter().any(|(id, _)| *id == sorted_ids[1])); // sorted_ids[1] should be found
        assert!(results.iter().any(|(id, _)| *id == sorted_ids[2])); // sorted_ids[2] should be found
    
        // ring range: (sorted_ids[2], sorted_ids[0]], should include sorted_ids[3], sorted_ids[0], sorted_ids[1]
        let results: Vec<_> = store.iter_range(sorted_ids[2], sorted_ids[1]).unwrap().collect();
        println!("Results in ring range: {:?}", results);
        assert_eq!(results.len(), 3); // should find sorted_ids[3], sorted_ids[0], sorted_ids[1]
        assert!(results.iter().any(|(id, _)| *id == sorted_ids[3])); // sorted_ids[3] should be found
        assert!(results.iter().any(|(id, _)| *id == sorted_ids[0])); // sorted_ids[0] should be found
        assert!(results.iter().any(|(id, _)| *id == sorted_ids[1])); // sorted_ids[1] should be found

        // empty range: (sorted_ids[0], sorted_ids[0]], should be empty
        let results: Vec<_> = store.iter_range(sorted_ids[0], sorted_ids[0]).unwrap().collect();
        println!("Results in empty range: {:?}", results);
        assert!(results.is_empty(), "Empty range should return no results");
    }

    #[test]
    fn test_scan_after_ptr_in_range() {
        let temp_dir = TempDir::new().unwrap();
        let store = LogStructuredStore::open(temp_dir.path()).unwrap();

        // insert records with known keys, values, and ids
        let keys = vec!["a", "b", "c", "d"];    
        let values = vec![b"va", b"vb", b"vc", b"vd"];

        let mut ptrs = Vec::new(); // (id, ptr)

        for (k, v) in keys.iter().zip(values.iter()) {
            let id = get_key_id(k);
            let ptr = store.put(id, v.to_vec()).unwrap();
            ptrs.push((k, id, ptr));
        }

        println!("Inserted records with pointers: {:?}", ptrs);

        // sort pointers for predictable order
        ptrs.sort_by_key(|(_k, _id, ptr)| *ptr);

        println!("Sorted pointers: {:?}", ptrs);

        // use the second pointer as the start point
        let start_ptr = ptrs[1].2;

        let results = store.scan_after_ptr_in_range(start_ptr, None).unwrap();
        println!("Scan results after pointer: {:?}", results);

        let found_ids: Vec<u32> = results.iter()
            .map(|(_, _, id, _)| *id)
            .collect();
        let expected_ids: Vec<u32> = vec![ptrs[2].1, ptrs[3].1];

        assert_eq!(found_ids, expected_ids, "Should find records after the start pointer");
    }
}