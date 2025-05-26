use std::hash::{Hash, Hasher};
use crate::types::NodeInfo;

// Generate a node id from ip and port.
// The node id is a 32-bit integer that is a hash of the ip and port.
pub fn get_node_id(ip: &str, port: u32) -> u32 {
    let mut ip_hasher = std::collections::hash_map::DefaultHasher::new();
    let mut port_hasher = std::collections::hash_map::DefaultHasher::new();
    ip.hash(&mut ip_hasher);
    port.hash(&mut port_hasher);
    let ip_hash = ip_hasher.finish() as u32;
    let port_hash = port_hasher.finish() as u32;

    let node_id = (ip_hash ^ port_hash) & ((1u64 << 32) - 1) as u32;
    return node_id;
}

// Generate a key id from user key.
// The key id is a 32-bit integer that is a hash of the user key.
pub fn get_key_id(user_key: &str) -> u32 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    user_key.hash(&mut hasher);
    let key_id = hasher.finish() as u32;
    return key_id;
}

