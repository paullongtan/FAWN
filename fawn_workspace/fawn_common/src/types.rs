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
        let clean_ip = ip.trim_start_matches("http://").trim_start_matches("https://");
        NodeInfo { ip: clean_ip.to_string(), port, id }
    }

    /// Returns the address in gRPC format (http://IP:PORT)
    pub fn get_http_addr(&self) -> String {
        format!("http://{}:{}", self.ip, self.port)
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

impl From<crate::fawn_backend_api::NodeInfo> for NodeInfo {
    fn from(proto: crate::fawn_backend_api::NodeInfo) -> Self {
        NodeInfo {
            ip: proto.ip,
            port: proto.port,
            id: proto.id,
        }
    }
}

impl From<NodeInfo> for crate::fawn_backend_api::NodeInfo {
    fn from(domain: NodeInfo) -> Self {
        crate::fawn_backend_api::NodeInfo {
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