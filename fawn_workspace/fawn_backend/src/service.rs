// use fawn_common::fawn_backend_api::NodeInfo;
use std::sync::Arc;

use fawn_common::types::NodeInfo;
use fawn_common::err::{FawnError, FawnResult};
use fawn_common::util::get_node_id;

use crate::storage::logstore::LogStructuredStore;

#[derive(Clone)]
pub struct BackendService {
    storage: Arc<LogStructuredStore>,
    predecessor: Option<NodeInfo>,
    prev_predecessor: Option<NodeInfo>,
    successor: Option<NodeInfo>,
    self_info: NodeInfo,
}

impl BackendService {
    pub fn new(storage: Arc<LogStructuredStore>, address: &str) -> FawnResult<Self> {
        let clean_addr = address.trim_start_matches("http://").trim_start_matches("https://");
        let back = clean_addr.parse::<std::net::SocketAddr>()
                .map_err(|e| FawnError::SystemError(format!("Invalid address format: {}", e)))?;
        let id = get_node_id(&back.ip().to_string(), back.port() as u32);

        Ok(Self {
            storage,
            predecessor: None,
            prev_predecessor: None,
            successor: None,
            self_info: NodeInfo::new(back.ip().to_string(), back.port() as u32, id),
        })
    }

    pub fn handle_ping(&self) -> FawnResult<NodeInfo> {
        Ok(self.self_info.clone())
    }

    pub fn handle_get_value(&self, key_id: u32) -> FawnResult<Vec<u8>> {
        match self.storage.get_hashed(key_id) {
            Ok(Some(value)) => {
                Ok(value)
            }
            Ok(None) => {
                Ok(vec![])
            }
            Err(e) => {
                Err(Box::new(FawnError::SystemError(format!("Failed to get value due to storage error: {}", e))))
            }
        }
    }

    pub fn handle_store_value(&self, key_id: u32, value: Vec<u8>) -> FawnResult<bool> {
        match self.storage.put_hashed(key_id, value) {
            Ok(()) => {
                Ok(true)
            }
            Err(e) => {
                Err(Box::new(FawnError::SystemError(format!("Failed to store value due to storage error: {}", e))))
            }
        }
    }

    pub fn handle_update_successor(&mut self, successor: &NodeInfo) -> FawnResult<bool> {
        self.successor = Some(successor.clone());
        Ok(true)
    }

    pub fn handle_prepare_for_split(&mut self, predecessor: &NodeInfo) -> FawnResult<bool> {
        self.prev_predecessor = self.predecessor.clone();
        self.predecessor = Some(predecessor.clone());
        Ok(true)
    }

    pub fn handle_migrate_data(&self, predecessor: &NodeInfo) -> FawnResult<bool> {
        let low_id = self.prev_predecessor.as_ref().map(|info| info.id).unwrap_or(0);
        let high_id = self.self_info.id;
        let migrate_data_iterator = self.storage.iter_range(low_id, high_id)?;
        for (key_id, value) in migrate_data_iterator {
            let key_id = get_key_id(key);
            let value = value.unwrap_or_default();
            self.storage.put_hashed(key_id, value)?;
        }
        Ok(true)
    }
}