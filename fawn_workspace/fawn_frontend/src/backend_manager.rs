// src/backend_manager.rs
use std::sync::Arc;
use tokio::sync::RwLock;
use fawn_common::types::NodeInfo;

#[derive(Debug)]
pub struct BackendManager {
    // Active backend nodes in the ring
    active_backends: Arc<RwLock<Vec<NodeInfo>>>,
    replication_count: u32,
}

impl BackendManager {
    pub fn new(initial_backends: Vec<NodeInfo>, replication_count: u32) -> Self {
        Self {
            active_backends: Arc::new(RwLock::new(initial_backends)),
            replication_count,
        }
    }

    pub async fn get_active_backends(&self) -> Vec<NodeInfo> {
        self.active_backends.read().await.clone()
    }

    pub async fn add_backend(&self, backend: NodeInfo) {
        let mut backends = self.active_backends.write().await;
        // Use binary search to find the insertion point
        let pos = backends.binary_search_by_key(&backend.id, |b| b.id)
            .unwrap_or_else(|p| p);
        backends.insert(pos, backend);
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

        // Use binary search to find the first backend with ID >= key_id
        match backends.binary_search_by_key(&key_id, |b| b.id) {
            Ok(pos) => Some(backends[pos].clone()), // Exact match
            Err(pos) => {
                if pos == backends.len() {
                    // Wrap around to the first node
                    backends.first().cloned()
                } else {
                    Some(backends[pos].clone())
                }
            }
        }
    }

    pub async fn find_predecessor(&self, key_id: u32) -> Option<NodeInfo> {
        let backends = self.active_backends.read().await;

        if backends.is_empty() {
            return None;
        }

        // Use binary search to find the backend with max ID that is < key_id
        match backends.binary_search_by_key(&key_id, |b| b.id) {
            Ok(pos) => {
                // For exact match, we want the previous node
                if pos == 0 {
                    backends.last().cloned() // Wrap around to last node
                } else {
                    Some(backends[pos - 1].clone())
                }
            }
            Err(pos) => {
                if pos == 0 {
                    // Wrap around to the last node
                    backends.last().cloned()
                } else {
                    Some(backends[pos - 1].clone())
                }
            }
        }
    }

    pub async fn get_chain_members(&self, key_id: u32) -> Vec<NodeInfo> {
        let backends = self.active_backends.read().await;
        let max_members = std::cmp::min(self.replication_count as usize, backends.len());
        
        if backends.is_empty() {
            return Vec::new();
        }

        let mut chain_members = Vec::with_capacity(max_members);
        
        // Find the starting position using binary search
        let start_pos = match backends.binary_search_by_key(&key_id, |b| b.id) {
            Ok(pos) => pos,
            Err(pos) => {
                if pos == backends.len() {
                    0 // Wrap around to start
                } else {
                    pos
                }
            }
        };

        // Collect successors by iterating through the array
        let mut current_pos = start_pos;
        while chain_members.len() < max_members {
            chain_members.push(backends[current_pos].clone());
            current_pos = (current_pos + 1) % backends.len();
        }
        
        chain_members
    }

    pub async fn num_active_backends(&self) -> u32 {
        self.active_backends.read().await.len() as u32
    }
}