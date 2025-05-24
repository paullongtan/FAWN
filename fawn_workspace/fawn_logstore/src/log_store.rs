use crate::segment::{SegmentWriter, SegmentReader};
use std::{io, path::PathBuf, sync::Mutex, time::{Duration, Instant}};

pub struct LogStructuredStore {
    dir:           PathBuf,
    active:        Mutex<SegmentWriter>,
    sealed:        Mutex<Vec<SegmentReader>>,
    max_seg_size:  u32,
    last_flush:    Mutex<Instant>,
}

impl LogStructuredStore {
    /// Open (or create) a store in `dir`.
    pub fn open<P: AsRef<std::path::Path>>(dir: P) -> io::Result<Self> {
        todo!()
    }

    // operation
    pub fn put(&self, key: Vec<u8>, val: Vec<u8>) -> io::Result<()> { todo!() }
    pub fn delete(&self, key: &[u8]) -> io::Result<()> { todo!() }
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> { todo!() }

    // maintenance
    pub fn flush(&self) -> io::Result<()> { todo!() }
    pub fn compact(&self) -> io::Result<()> { todo!() }
    fn roll_segment(&mut self) -> io::Result<()> { todo!() }

    pub fn iter_range(
        &self,
        lo: u32,
        hi: u32,
    ) -> io::Result<impl Iterator<Item = (Vec<u8>, Vec<u8>)>> {
        Ok(std::iter::empty())
    }
}