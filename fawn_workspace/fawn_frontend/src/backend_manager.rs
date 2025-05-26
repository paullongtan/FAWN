// src/backend_manager.rs
use std::sync::Arc;
use tokio::sync::RwLock;
use fawn_common::types::NodeInfo;

#[derive(Debug)]
pub struct BackendManager {
    // Active backend nodes in the ring
    active_backends: Arc<RwLock<Vec<NodeInfo>>>
}

impl BackendManager {
    pub fn new(initial_backends: Vec<NodeInfo>) -> Self {
        Self {
            active_backends: Arc::new(RwLock::new(initial_backends))
        }
    }

    pub async fn get_active_backends(&self) -> Vec<NodeInfo> {
        self.active_backends.read().await.clone()
    }

    pub async fn add_backend(&self, backend: NodeInfo) {
        let mut backends = self.active_backends.write().await;
        backends.push(backend);
        // Sort backends by ID to maintain ring order
        backends.sort_by_key(|b| b.id);
    }

    pub async fn remove_backend(&self, backend_id: u32) {
        let mut backends = self.active_backends.write().await;
        backends.retain(|b| b.id != backend_id);
    }

    pub async fn find_successor(&self, key_id: u32) -> Option<NodeInfo> {
        let backends = self.active_backends.read().await;

        if backends.is_empty() {
            return None;
        }

        // if key_id is greater than all node ids, wrap around to first node
        if key_id > backends.last().unwrap().id {
            return backends.first().cloned();
        }
        
        // Find the first backend with ID greater than or equal to the key_id
        backends.iter()
            .find(|b| b.id >= key_id)
            .cloned()
        }

    pub async fn find_predecessor(&self, key_id: u32) -> Option<NodeInfo> {
        let backends = self.active_backends.read().await;

        if backends.is_empty() {
            return None;
        }

        // if key_id is smaller than all node ids, wrap around to last node
        if key_id <= backends.first().unwrap().id {
            return backends.last().cloned();
        }

        // Find the backend with the greatest ID less than the key_id
        backends.iter()
            .rev()
            .find(|b| b.id < key_id)
            .cloned()
    }
}