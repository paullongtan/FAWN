// src/backend_manager.rs
use std::{cmp, sync::Arc};
use tokio::sync::RwLock;
use fawn_common::types::NodeInfo;

#[derive(Debug)]
pub struct BackendManager {
    // Active backend nodes in the ring
    active_backends: Arc<RwLock<Vec<NodeInfo>>>,
    // max number of chain members to return
    replication_count: usize,
}

impl BackendManager {
    pub fn new(initial_backends: Vec<NodeInfo>, replication_count: usize) -> Self {
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

        // Binary search for the first backend with ID greater than or equal to the key_id
        match backends.binary_search_by_key(&key_id, |b| b.id) {
            Ok(pos) => Some(backends[pos].clone()), // Exact match
            Err(pos) => {
                if pos >= backends.len() {
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

        let mut pos = match backends.binary_search_by_key(&key_id, |b| b.id) {
            Ok(pos) => pos,
            Err(pos) => {
                if pos >= backends.len() {
                    backends.len() - 1
                } else {
                    pos
                }
            }
        };
        // Ensure chain size doesn't exceed available backends
        let chain_size = std::cmp::min(self.replication_count, backends.len());
        // Move back by chain size to get the predecessor
        pos = (pos + backends.len() - chain_size) % backends.len();

        if pos >= backends.len() {
            // Wrap around to the last node
            backends.last().cloned()
        } else {
            Some(backends[pos].clone())
        }
    }

    pub async fn get_chain_members(&self, key_id: u32) -> Vec<NodeInfo> {
        let backends = self.active_backends.read().await;

        if backends.is_empty() {
            return vec![];
        }

        let size = cmp::min(backends.len(), self.replication_count);
        let mut chain_members = Vec::with_capacity(size);

        let mut index = match backends.binary_search_by_key(&key_id, |b| b.id) {
            Ok(pos) => pos,
            Err(pos) => {
                if pos >= backends.len() {
                    0
                } else {
                    pos
                }
            }
        };

        let mut cnt = 0;
        while cnt < size {
            chain_members.push(backends[index].clone());
            index = (index + 1) % backends.len();
            cnt += 1;
        }
        
        chain_members
    }

    pub async fn get_updated_chain(
        &self,
        new_node: NodeInfo,
    ) -> Vec<(Vec<NodeInfo>, Option<NodeInfo>, NodeInfo)> {
        let backends = self.active_backends.read().await;
        if backends.is_empty() {
            // The first node forms a chain by itself. There is no predecessor and no data source.
            return vec![(vec![new_node.clone()], None, new_node)];
        }

        let pos = match backends.binary_search_by_key(&new_node.id, |b| b.id) {
            Ok(_) => return vec![], // Node already exists
            Err(pos) => pos,
        };

        let mut new_backends = backends.clone();
        new_backends.insert(pos, new_node.clone());

        let new_len = new_backends.len();
        let chain_size = cmp::min(self.replication_count, new_len);
        let mut result = Vec::new();
        let mut affected_heads = std::collections::HashSet::new();

        for i in 0..chain_size {
            let head_pos_new = (pos + new_len - i) % new_len;
            let head_node_new = &new_backends[head_pos_new];

            if affected_heads.insert(head_node_new.id) {
                let mut new_chain = Vec::with_capacity(chain_size);
                for j in 0..chain_size {
                    new_chain.push(new_backends[(head_pos_new + j) % new_len].clone());
                }

                let old_head_node = if head_node_new.id == new_node.id {
                    // This is the chain for which the new node is the head.
                    // The old chain was headed by the new node's successor.
                    &backends[pos % backends.len()]
                } else {
                    // This is a chain that the new node is joining.
                    // The head is an existing node.
                    head_node_new
                };

                let old_head_pos = backends
                    .binary_search_by_key(&old_head_node.id, |b| b.id)
                    .unwrap_or_else(|p| p % backends.len());

                let original_chain_size = cmp::min(self.replication_count, backends.len());
                let old_tail_pos = (old_head_pos + original_chain_size - 1) % backends.len();
                let old_tail = backends[old_tail_pos].clone();

                let predecessor_of_head =
                    new_backends[(head_pos_new + new_len - 1) % new_len].clone();

                result.push((new_chain, Some(old_tail), predecessor_of_head));
            }
        }
        result
    }

    pub async fn num_active_backends(&self) -> u32 {
        self.active_backends.read().await.len() as u32
    }
}