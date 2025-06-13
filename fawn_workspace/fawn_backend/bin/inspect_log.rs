use std::path::Path;
use fawn_backend::storage::logstore::LogStructuredStore;
use fawn_common::util::get_key_id;

pub fn inspect_log_dir<P: AsRef<Path>>(dir: P, key: &str) {
    // Open the log store
    let store = LogStructuredStore::open(dir).expect("Failed to open log store");

    // Compute the key hash (or use your own hash function)
    let key_id = get_key_id(key);

    // Retrieve the value
    match store.get(key_id) {
        Ok(Some(value)) => {
            println!("Found value for key '{}': {:?}", key, value);
        }
        Ok(None) => {
            println!("Key '{}' not found in log store.", key);
        }
        Err(e) => {
            println!("Error reading from log store: {}", e);
        }
    }
}

pub fn list_active_keys<P: AsRef<Path>>(dir: P) {
    let store = LogStructuredStore::open(dir).expect("Failed to open log store");
    // iter_range support full ring by having lo == hi
    let results = store.iter_range(0, 0).expect("Failed to iterate range");
    println!("Active keys in storage:");
    for (key_id, value) in results {
        println!("Key ID: {}", key_id);
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() == 3 && args[1] == "--get" {
        // Usage: inspect_log --get <log_dir> <key>
        let log_dir = &args[2];
        let key = &args[3];
        inspect_log_dir(log_dir, key);
    } else if args.len() == 3 && args[1] == "--list" {
        // Usage: inspect_log --list <log_dir>
        let log_dir = &args[2];
        list_active_keys(log_dir);
    } else {
        eprintln!("Usage:");
        eprintln!("  {} --get <log_dir> <key>   # Retrieve value for a key", args[0]);
        eprintln!("  {} --list <log_dir>        # List all active keys", args[0]);
        std::process::exit(1);
    }
}