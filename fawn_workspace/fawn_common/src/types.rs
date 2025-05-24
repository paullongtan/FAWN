use serde::{Deserialize, Serialize};

/// Represents information about a node in the distributed system
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    /// IP address of the node
    pub ip: String,
    /// Port number the node listens on
    pub port: u32,
    /// Unique identifier for the node
    pub id: u32,
}

impl NodeInfo {
    /// Creates a new NodeInfo instance
    pub fn new(ip: String, port: u32, id: u32) -> NodeInfo {
        NodeInfo { ip, port, id }
    }
}

impl From<crate::fawn_frontend_api::NodeInfo> for NodeInfo {
    fn from(proto: crate::fawn_frontend_api::NodeInfo) -> Self {
        NodeInfo {
            ip: proto.ip,
            port: proto.port,
            id: proto.id,
        }
    }
}

impl From<NodeInfo> for crate::fawn_frontend_api::NodeInfo {
    fn from(domain: NodeInfo) -> Self {
        crate::fawn_frontend_api::NodeInfo {
            ip: domain.ip,
            port: domain.port,
            id: domain.id,
        }
    }
}

/// Represents a key-value pair in the system
#[derive(Debug, Clone)]
pub struct KeyValue {
    /// The key string
    pub key: String,
    /// The value as a byte vector
    pub value: Vec<u8>,
}

impl KeyValue {
    /// Creates a new KeyValue instance
    pub fn new(key: String, value: Vec<u8>) -> KeyValue {
        KeyValue { key, value }
    }
}