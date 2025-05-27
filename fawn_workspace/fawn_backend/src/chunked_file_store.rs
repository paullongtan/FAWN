use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
    sync::Arc,
};

use sha2::Digest;
use anyhow::{anyhow, Result};
use fawn_logstore::log_store::LogStructuredStore;
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
        let mut file = File::open(path)?;
        let mut buffer = vec![0u8; CHUNK_SIZE];
        let mut hasher = sha2::Sha256::new();
        let mut total = 0_u64;
        let mut idx = 0_u32;

        loop {
            let bytes_read = file.read(&mut buffer)?;
            if bytes_read == 0 {
                break; // EOF
            }
            hasher.update(&buffer[..bytes_read]);
            total += bytes_read as u64;

            let chunk_key = format!("{file_id}:{:08X}", idx);
            self.store.put(chunk_key.into_bytes(), buffer[..bytes_read].to_vec())?;
            idx += 1;
        }

        // wrtie manifest last so orphand chunks are harmless on crash
        let manifest = Manifest {
            chunk_cnt: idx,
            file_size: total,
            sha256: hasher.finalize().into(),
        };
        let meta_key = format!("{file_id}:meta");
        self.store.put(meta_key.into_bytes(), serde_json::to_vec(&manifest)?)?;
        
        Ok(())
    }

    /// Re-assemble the file into output path
    pub fn get_file(&self, file_id: &str, output_path: &Path) -> Result<()> {
        // fetch manifest first
        let meta_key = format!("{file_id}:meta");
        let meta_val = self
            .store
            .get(meta_key.as_bytes())?
            .ok_or_else(|| anyhow!("file not found"))?;
        let manifest: Manifest = serde_json::from_slice(&meta_val)?;

        let mut out = File::create(output_path)?;
        let mut hasher = sha2::Sha256::new();

        // fetch each chunk and write to output
        for idx in 0..manifest.chunk_cnt {
            let chunk_key = format!("{file_id}:{:08X}", idx);
            let chunk = self
                .store
                .get(chunk_key.as_bytes())?
                .ok_or_else(|| anyhow!("chunk {idx} not found"))?;
            hasher.update(&chunk);
            out.write_all(&chunk)?;
        }

        // verify the SHA-256 hash
        if hasher.finalize() != manifest.sha256.into() {
            return Err(anyhow::anyhow!("SHA-256 mismatch — data corrupted {file_id}"));
        }

        Ok(())
    }

    /// Delete a file by its ID
    /// Tombstone manifest first; chunks are dropped during compaction.
    pub fn delete_file(&self, file_id: &str) -> Result<()> {
        let meta_key = format!("{file_id}:meta");
        self.store.delete(meta_key.as_bytes())?;
        Ok(())
    }
}