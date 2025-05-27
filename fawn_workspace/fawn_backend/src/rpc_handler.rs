use fawn_common::fawn_backend_api::{fawn_backend_service_client::FawnBackendServiceClient, StoreRequest};
use std::sync::Arc;

use fawn_common::types::NodeInfo;
use fawn_common::err::{FawnError, FawnResult};
use fawn_common::util::get_node_id;

use crate::storage::logstore::LogStructuredStore;
use crate::server::BackendSystemState;

#[derive(Clone)]
pub struct BackendHandler {
    storage: Arc<LogStructuredStore>,
    state: Arc<BackendSystemState>,
}

impl BackendHandler {
    pub fn new(storage: LogStructuredStore, state: Arc<BackendSystemState>) -> FawnResult<Self> {
        Ok(Self {
            storage: Arc::new(storage),
            state,
        })
    }

    pub fn handle_ping(&self) -> FawnResult<NodeInfo> {
        Ok(self.state.self_info.clone())
    }

    pub fn handle_get_value(&self, key_id: u32) -> FawnResult<Vec<u8>> {
        match self.storage.get(key_id) {
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
        match self.storage.put(key_id, value) {
            Ok(()) => {
                Ok(true)
            }
            Err(e) => {
                Err(Box::new(FawnError::SystemError(format!("Failed to store value due to storage error: {}", e))))
            }
        }
    }

    pub async fn handle_update_successor(&mut self, successor: &NodeInfo) -> FawnResult<bool> {
        let mut successor_lock = self.state.successor.write().await;
        *successor_lock = successor.clone();
        Ok(true)
    }

    pub async fn handle_prepare_for_split(&mut self, predecessor: &NodeInfo) -> FawnResult<bool> {
        let mut prev_pred_lock = self.state.prev_predecessor.write().await;
        let mut pred_lock = self.state.predecessor.write().await;
        *prev_pred_lock = pred_lock.clone();
        *pred_lock = predecessor.clone();
        Ok(true)
    }

    pub async fn handle_migrate_data(&self, request_node: &NodeInfo) -> FawnResult<bool> {
        let prev_predecessor = self.state.prev_predecessor.read().await;
        let predecessor = self.state.predecessor.read().await;

        // check if the request node is the predecessor
        if request_node.id != predecessor.id {
            return Err(Box::new(FawnError::InvalidRequest("Request node is not the predecessor".to_string())));
        }

        let low_id = prev_predecessor.id;
        let high_id = predecessor.id;
        // get all key-value pairs in the range (low_id, high_id]
        let migrate_data_iterator = self.storage.iter_range(low_id, high_id)?;
        let mut client = FawnBackendServiceClient::connect(request_node.get_http_addr()).await.map_err(|e| FawnError::RpcError(format!("Failed to connect to predecessor: {}", e)))?;
        
        // send the key-value pairs to the new predecessor
        for (key_id, value) in migrate_data_iterator {
            let request = fawn_common::fawn_backend_api::StoreRequest {
                key_id,
                value,
            };
            let response = client.store_value(request).await.map_err(|e| FawnError::RpcError(format!("Failed to send data to predecessor: {}", e)))?;
            if !response.get_ref().success {
                return Err(Box::new(FawnError::SystemError("Failed to send data to predecessor".to_string())));
            }
        }
        Ok(true)
    }
}