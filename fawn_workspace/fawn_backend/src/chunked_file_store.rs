use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
    sync::Arc,
};

use anyhow::Result;
use fawn_logstore::LogStructuredStore;
use serde::{Deserialize, Serialize};

const CHUNK_SIZE: usize = 256 * 1024; // 256 KiB "record" size

#[derive(Serialize, Deserialize)]
struct Manifest {
    chunk_cnt: u32, 
    file_size: u64,
    sha256: [u8; 32], // SHA-256 hash of the file
}

/// High-level adapter that turns a big file into many KV records
/// (`<file-id>:<chunk-id>`, ...) plus one manifest record (`<file-id>:meta`).
pub struct ChunkedFileStore {
    store: Arc<LogStructuredStore>, 
}

impl ChunkedFileStore {
    pub fn new(store: Arc<LogStructuredStore>) -> Self {
        Self { store }
    }

    /// Upload a file from disk
    pub fn put_file(&self, file_id: &str, path: &Path) -> Result<()> {
        todo!()
    }

    /// Re-assemble the file into output path
    pub fn get_file(&self, file_id: &str, output_path: &Path) -> Result<()> {
        todo!()
    }

    /// Delete a file by its ID
    /// Tombstone manifest first; chunks are dropped during compaction.
    pub fn delete_file(&self, file_id: &str) -> Result<()> {
        todo!()
    }
}