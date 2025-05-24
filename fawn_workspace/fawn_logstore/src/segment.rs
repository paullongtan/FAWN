//! SegmentInfo, SegmentWriter, SegmentReader – kept together for simplicity.

use std::{
    collections::HashMap,
    fs::File,
    io,
    path::{Path, PathBuf},
};
use memmap2::Mmap;

use crate::record::{self, Record};

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
        todo!()
    }
}

pub(crate) struct SegmentWriter {
    pub meta:       SegmentInfo,
    log_fd:         File,
    footer_buf:     Vec<(u32 /*hash32*/, u32 /*offset32*/ )>,
    in_mem_idx:     HashMap<u32, u32>,          // hash32 → offset
    offset:         u32,
    max_size:       u32,
}

impl SegmentWriter {
    /// Create a fresh `.log` file, empty footer buffer.
    pub fn create<P: AsRef<Path>>(dir: P, id: u64, max_size: u32) -> io::Result<Self> {
        todo!()
    }

    pub fn append_put(&mut self, key: &[u8], val: &[u8]) -> io::Result<()> {
        todo!()
    }

    pub fn append_delete(&mut self, key: &[u8]) -> io::Result<()> {
        todo!()
    }

    /// Return `Some(Some(value))` = live record,
    ///        `Some(None)`       = tombstone,
    ///        `None`             = hash not present.
    pub fn lookup_in_mem(
        &self,
        hash: u32,
        key: &[u8],
    ) -> io::Result<Option<Option<Vec<u8>>>> {
        todo!()
    }

    pub fn flush_footer(&mut self) -> io::Result<()> {
        todo!()
    }

    /// Finish the segment, return a read-only handle.
    pub fn seal(self) -> io::Result<SegmentReader> {
        todo!()
    }

    pub fn bytes_written(&self) -> u32 {
        todo!()
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
        todo!()
    }

    /// Binary-search footer → single `pread`.
    pub fn lookup(&self, hash: u32, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        todo!()
    }
}