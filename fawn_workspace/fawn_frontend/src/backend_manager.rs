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
    ) -> Vec<(Vec<NodeInfo>, NodeInfo, NodeInfo)> {
        let backends = self.active_backends.read().await;
        if backends.is_empty() {
            // If the ring is empty, the new node forms a chain by itself.
            // There's no old tail to migrate data from, so no updates are returned.
            return vec![];
        }

        let pos = match backends.binary_search_by_key(&new_node.id, |b| b.id) {
            Ok(_) => return vec![], // Node already exists, no changes needed.
            Err(pos) => pos,
        };

        // Create a new representation of the ring with the new node included.
        let mut new_backends = backends.clone();
        new_backends.insert(pos, new_node.clone());

        let new_len = new_backends.len();
        let chain_size = cmp::min(self.replication_count, new_len);
        let original_chain_size = cmp::min(self.replication_count, backends.len());

        let mut result = Vec::with_capacity(chain_size);

        // The introduction of a new node affects `chain_size` chains.
        // We iterate through them to determine their new members and the old tail for data migration.
        for i in 0..chain_size {
            // The heads of the affected chains are the new node and its `chain_size - 1` predecessors.
            let head_pos_new = (pos + new_len - i) % new_len;

            // Construct the new chain.
            let mut chain = Vec::with_capacity(chain_size);
            for j in 0..chain_size {
                let idx = (head_pos_new + j) % new_len;
                chain.push(new_backends[idx].clone());
            }

            // For each new chain, we identify the tail of its corresponding old chain.
            // This 'old tail' holds the data that needs to be migrated to new members.
            let old_tail = if i == 0 {
                // This case handles the chain for which `new_node` is the new head.
                // Its key range was previously managed by the chain starting at `new_node`'s successor.
                // The head of that old chain is `backends[pos]`. We get data from its tail.
                let old_head_pos = pos % backends.len();
                let old_tail_pos = (old_head_pos + original_chain_size - 1) % backends.len();
                backends[old_tail_pos].clone()
            } else {
                // These are existing chains that `new_node` is joining. The head remains the same.
                // We find the head in the old `backends` list to identify the old chain's tail.
                let head_node = &chain[0];
                let old_head_pos = backends
                    .binary_search_by_key(&head_node.id, |b| b.id)
                    .expect("Chain head must have existed in the old backend list");
                let old_tail_pos = (old_head_pos + original_chain_size - 1) % backends.len();
                backends[old_tail_pos].clone()
            };

            let predecessor_pos = (head_pos_new + new_len - 1) % new_len;
            let predecessor = new_backends[predecessor_pos].clone();

            result.push((chain, old_tail, predecessor));
        }

        result
    }
}