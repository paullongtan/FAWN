use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    pub ip: String,
    pub port: u32,
    pub id: u32,
}

impl NodeInfo {
    pub fn new(ip: String, port: u32, id: u32) -> NodeInfo {
        NodeInfo { ip, port, id }
    }
}

#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: String,
    pub value: Vec<u8>,
}
impl KeyValue {
    pub fn new(key: String, value: Vec<u8>) -> KeyValue {
        KeyValue { key, value }
    }
}