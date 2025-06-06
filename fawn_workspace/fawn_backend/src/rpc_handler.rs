use fawn_common::fawn_backend_api::{fawn_backend_service_client::FawnBackendServiceClient, StoreRequest};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::BTreeMap;

use fawn_common::types::NodeInfo;
use fawn_common::err::{FawnError, FawnResult};
use fawn_common::util::get_node_id;

use crate::meta::Meta;
use crate::storage::logstore::{LogStructuredStore, RecordPtr};
use crate::server::BackendSystemState;

type PendingOp = (u32 /*key*/, Vec<u8> /*val*/, u32 /*remaining_pass*/);
type PendingBuf = BTreeMap<u64 /*timestamp*/, PendingOp>;

#[derive(Clone)]
pub struct BackendHandler {
    storage: Arc<LogStructuredStore>,
    state: Arc<BackendSystemState>,

    clock: Arc<AtomicU64>, // Logical clock for operation sequencing
    last_acked: Arc<AtomicU64>, // Last acknowledged timestamp
    pending_ops: Arc<tokio::sync::RwLock<PendingBuf>>, // Pending operations to be processed

    meta_path: PathBuf, // Path to the metadata file
}

// TODO: read from on-disk metadata to initiate its clock and pending_ops
impl BackendHandler {
    pub fn new(storage: LogStructuredStore, state: Arc<BackendSystemState>, meta: Meta, meta_path: PathBuf) -> FawnResult<Self> {

        // TODO: rebuild pending buffer; every record with timestamp > last_acked_ts
        // let pending = store.scan_since_ts(meta.last_acked_ts)?;

        Ok(Self {
            storage: Arc::new(storage),
            state,
            clock: Arc::new(AtomicU64::new(meta.last_ts)),
            last_acked: Arc::new(AtomicU64::new(meta.last_acked_ts)),
            pending_ops: Arc::new(tokio::sync::RwLock::new(BTreeMap::new())), // TODO: rebuild from disk
            meta_path,
        })
    }

    pub fn handle_ping(&self) -> FawnResult<NodeInfo> {
        Ok(self.state.self_info.clone())
    }

    // TODO: check whether we need to forward to the tail
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

    pub async fn handle_store_value(&self, key_id: u32, value: Vec<u8>, pass_remaining: u32, timestamp_in: u64) -> FawnResult<bool> {
        // head attached its logical clock to the operation
        let timestamp = if timestamp_in == 0 { // only head receives timestamp 0
            // head increments its logical clock
            // and attaches its current timestamp to the operation
            self.bump_clock()
        } else {
            // followers make sure local clock >= incoming timestamp
            self.update_clock(timestamp_in);
            timestamp_in
        };

        // local append (store on local disk)
        self.storage
            .put(key_id, value.clone())
            .map_err(|e| FawnError::SystemError(format!("Failed to store value due to storage error: {}", e)))?;

        // stop forwarding if no pass_remaining is 0 (tail)
        if pass_remaining == 0 {
            // TODO: tail should acknowledge the operation
            self.storage.flush()?; // TODO: check whether we need to flush the storage
            return Ok(true);
        }

        // log the operation in the pending buffer
        {
            let mut pending_ops = self.pending_ops.write().await;
            pending_ops.insert(timestamp, (key_id, value.clone(), pass_remaining));
        }

        // forward the store request to the successor
        match self.forward_once(timestamp, key_id, value, pass_remaining - 1).await? {
            true => { self.remove_pending(timestamp).await; Ok(true) }, 
            false => Err(Box::new(FawnError::RpcError("transport failure".into()))), // let the caller replay the operation
        }
    }

    async fn forward_once(&self, timestamp: u64, key_id: u32, value: Vec<u8>, pass_remaining_next: u32) -> FawnResult<bool> {
        let successor = self.state.successor.read().await.clone();
        let mut client = FawnBackendServiceClient::connect(successor.get_http_addr()).await
            .map_err(|e| FawnError::RpcError(format!("Failed to connect to successor: {}", e)))?;

        let request = StoreRequest {
            key_id,
            value,
            timestamp,
            pass_count: pass_remaining_next,
        }; 

        // TODO: check whether to separate transport errors from application errors
        match client.store_value(request).await {
            Ok(_) => Ok(true), // delivered & ACKed
            Err(_) => Ok(false), // map every error as a transport failure, caller should retry
        }
    }

    async fn remove_pending(&self, timestamp: u64) {
        self.pending_ops.write().await.remove(&timestamp);
        // acknowledge the operation
        self.ack(timestamp);
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
                timestamp: 0, // TODO: attached the timestamp from record
                pass_count: 0, // no further forwarding needed
            };
            let response = client.store_value(request).await.map_err(|e| FawnError::RpcError(format!("Failed to send data to predecessor: {}", e)))?;
            if !response.get_ref().success {
                return Err(Box::new(FawnError::SystemError("Failed to send data to predecessor".to_string())));
            }
        }
        Ok(true)
    }

    // TODO: update the timestamp on disk
    fn update_clock(&self, timestamp: u64) {
        let mut curr = self.clock.load(Ordering::Relaxed);
        while timestamp > curr {
            match self.clock.compare_exchange(curr, timestamp, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => {
                    self.persist_meta(); // persist the updated clock
                    break; // successfully updated the clock
                }
                Err(v) => curr = v, // lost race; retry if still smaller
            }
        }
    }

    pub async fn replay_pending_ops(&self) -> FawnResult<()> {
        loop {
            // take a ordered snapshot of pending operations
            let snapshot: Vec<(u64, PendingOp)> = {
                let buf = self.pending_ops.read().await;
                if buf.is_empty() { return Ok(()); }
                buf.iter().map(|(ts, op)| (*ts, op.clone())).collect()
            };

            // replay each operation in the snapshot
            for (timestamp, (key_id, value, pass_remaining)) in snapshot {
                match self.forward_once(timestamp, key_id, value, pass_remaining - 1).await? {
                    true => { self.remove_pending(timestamp).await; } // drop entry if successfully forwarded
                    false => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; // wait before retrying
                        continue; // retry the operation
                    }
                }
            }
        }
    }

    pub fn get_current_timestamp(&self) -> u64 {
        self.clock.load(Ordering::Relaxed)
    }

    fn bump_clock(&self) -> u64 {
        let ts = self.clock.fetch_add(1, Ordering::Relaxed) + 1; // increment and return the new value
        self.persist_meta();
        ts
    }

    // TODO: advance pointer
    fn ack(&self, timestamp: u64) {
        // update the last acked timestamp
        self.last_acked.store(timestamp, Ordering::Relaxed);
        self.persist_meta();
    }

    fn persist_meta(&self) -> std::io::Result<()>  {
        let m = Meta {
            last_ts: self.get_current_timestamp(),
            last_acked_ts: self.last_acked.load(Ordering::Relaxed),
        }; 
        m.store(&self.meta_path)
    }
}