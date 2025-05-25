use tempfile::{TempDir, NamedTempFile};
use std::io::{Write, Read};
use std::sync::Arc;
use sha2::{Digest, Sha256};

use fawn_logstore::log_store::LogStructuredStore;
use fawn_backend::chunked_file_store::ChunkedFileStore;


#[test]
fn chunked_put_fetch_delete() {
    // create 800 KiB random file
    let mut src = NamedTempFile::new().unwrap();
    let data = vec![7u8; 800 * 1024];
    src.write_all(&data).unwrap();

    // backend store
    let dir = TempDir::new().unwrap();
    let kv = Arc::new(LogStructuredStore::open(dir.path()).unwrap());
    let fs = ChunkedFileStore::new(kv.clone());

    fs.put_file("big1", src.path()).unwrap();

    // fetch to new file
    let out = NamedTempFile::new().unwrap();
    fs.get_file("big1", out.path()).unwrap();
    let mut round = Vec::new();
    std::fs::File::open(out.path()).unwrap().read_to_end(&mut round).unwrap();
    assert_eq!(Sha256::digest(&round)[..], Sha256::digest(&data)[..]);

    // delete and ensure manifest gone
    fs.delete_file("big1").unwrap();
    assert!(kv.get(b"big1:meta").unwrap().is_none());
}