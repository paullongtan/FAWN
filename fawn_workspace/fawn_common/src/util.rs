use std::hash::{Hash, Hasher};
use crate::types::NodeInfo;
use xxhash_rust::xxh3::xxh3_64;

// Generate a node id from ip and port.
// The node id is a 32-bit integer that is a hash of the ip and port.
pub fn get_node_id(ip: &str, port: u32) -> u32 {
    let ip_hash = xxh3_64(ip.as_bytes()) as u32;
    let port_hash = xxh3_64(port.to_string().as_bytes()) as u32;
    ip_hash ^ port_hash
}

// Generate a key id from user key.
// The key id is a 32-bit integer that is a hash of the user key.
pub fn get_key_id(user_key: &str) -> u32 {
    xxh3_64(user_key.as_bytes()) as u32
}

