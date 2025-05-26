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

#[test]
#[ignore = "reason: this test is for manual demo purposes"]
fn demo_put_file() {
    use std::fs::File;
    use std::io::Write;
    let max_segment_size: usize = 4 * 1024 * 1024; // 4 MiB max segment size

    // Clean up previous directory if it exists
    let dir = "./demo_put_file";
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();

    // Create a large test file to force multiple segments
    let test_file_path = format!("{}/large.dat", dir);
    let mut test_file = File::create(&test_file_path).unwrap();
    let data = vec![b'A'; max_segment_size * 2]; // 8 MiB of data
    test_file.write_all(&data).unwrap();

    // Create backend store
    let kv = Arc::new(LogStructuredStore::open(dir).unwrap());
    let fs = ChunkedFileStore::new(kv.clone());

    // Put and get the file
    let output_file_path = format!("{}/output.dat", dir);
    fs.put_file("large", std::path::Path::new(&test_file_path)).unwrap();
    fs.get_file("large", std::path::Path::new(&output_file_path)).unwrap();

    // Read back and print output
    let mut output_file = File::open(&output_file_path).unwrap();
    let mut output_data = Vec::new();
    output_file.read_to_end(&mut output_data).unwrap();
    assert_eq!(output_data, data, "Output data does not match input data");

    // Print where to find the .log and .ftr files
    let paths = std::fs::read_dir(dir).unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map(|ext| ext == "log" || ext == "ftr").unwrap_or(false))
        .collect::<Vec<_>>();
    println!("You can inspect the segment files at:");
    for path in paths {
        println!("  {}", path.display());
    }
}
