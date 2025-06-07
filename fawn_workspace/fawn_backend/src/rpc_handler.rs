use fawn_common::fawn_backend_api::{fawn_backend_service_client::FawnBackendServiceClient, StoreRequest, ValueEntry};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use fawn_common::types::NodeInfo;
use fawn_common::err::{FawnError, FawnResult};
use fawn_common::util::get_node_id;

use crate::meta::Meta;
use crate::storage::logstore::{LogStructuredStore, RecordPtr, RecordFlags};
use crate::server::BackendSystemState;

// type PendingOp = (u32 /*key*/, Vec<u8> /*val*/, u32 /*remaining_pass*/);

#[derive(Clone)]
pub struct BackendHandler {
    storage: Arc<LogStructuredStore>,
    state: Arc<BackendSystemState>,
    last_acked_ptr: Arc<Mutex<RecordPtr>>, // pointer to last acked record
    last_sent_ptr: Arc<Mutex<RecordPtr>>, // pointer to last sent record (for pre-copy)
    meta_path: PathBuf, // Path to the metadata file
}

impl BackendHandler {
    pub fn new(storage: LogStructuredStore, state: Arc<BackendSystemState>, meta: Meta, meta_path: PathBuf) -> FawnResult<Self> {
        Ok(Self {
            storage: Arc::new(storage),
            state,
            last_acked_ptr: Arc::new(Mutex::new(meta.last_acked_ptr)),
            last_sent_ptr: Arc::new(Mutex::new(meta.last_sent_ptr)),
            meta_path,
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

    pub async fn handle_store_value(&self, key_id: u32, value: Vec<u8>, pass_remaining: u32) -> FawnResult<bool> {
        // local append (store on local disk)
        let ptr_after_record = self.storage
            .put(key_id, value.clone())
            .map_err(|e| FawnError::SystemError(format!("Failed to store value due to storage error: {}", e)))?;

        // stop forwarding if no pass_remaining is 0 (tail)
        if pass_remaining == 0 {
            self.advance_ptr_to_dest(&self.last_acked_ptr, ptr_after_record)?; // advance ack pointer
            return Ok(true);
        }

        // forward the store request to the successor
        match self.forward_once( key_id, value, pass_remaining - 1).await? {
            // successor has acked the operation
            true => { 
                self.advance_ptr_to_dest(&self.last_acked_ptr, ptr_after_record)?; // advance ack pointer
                Ok(true) // delivered & ACKed
            }, 
            false => Err(Box::new(FawnError::RpcError("transport failure".into()))), // let the caller replay the operation
        }
    }

    async fn forward_once(&self, key_id: u32, value: Vec<u8>, pass_remaining_next: u32) -> FawnResult<bool> {
        let successor = self.state.successor.read().await.clone();
        let mut client = FawnBackendServiceClient::connect(successor.get_http_addr()).await
            .map_err(|e| FawnError::RpcError(format!("Failed to connect to successor: {}", e)))?;

        let request = StoreRequest {
            key_id,
            value,
            pass_count: pass_remaining_next,
        }; 

        // TODO: check whether to separate transport errors from application errors
        match client.store_value(request).await {
            Ok(_) => Ok(true), // delivered & ACKed
            Err(_) => Ok(false), // map every error as a transport failure, caller should retry
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

    /// Collect every record with key \in (lo, hi] and ptr > last_sent_ptr, 
    /// convert to ValueEntry, advance last_sent_ptr, and return the vector containing them.
    pub async fn build_migrate_slice(&self, range: (u32, u32)) -> FawnResult<Vec<ValueEntry>> {
        let floor = self.last_sent_ptr.lock().unwrap().clone();

        // Vec<(ptr, flag, key, val)>
        let records = self.storage
            .scan_after_ptr_in_range(floor, Some(range))
            .map_err(|e| FawnError::SystemError(format!("Failed to scan for migration: {}", e)))?;

        // convert to proto entries
        let entries: Vec<ValueEntry> = records.iter()
            .filter_map(|(_ptr, flag, k, v)| {
                if *flag != RecordFlags::Put {
                    return None; // only migrate Put records
                }
            Some(ValueEntry {
                key_id: *k, 
                value: v.clone(),
            })
        }).collect();

        // advance the last_sent_ptr to the last record pointer
        if let Some((ptr, ..)) = records.last() {
            self.advance_ptr_to_dest(&self.last_sent_ptr, *ptr)?;
        }

        Ok(entries)
    }

    pub async fn send_flush(&self, dest: &NodeInfo) -> FawnResult<()> {
        let start_ptr = self.last_sent_ptr.lock().unwrap().clone();
        let records = self.storage
            .scan_after_ptr_in_range(start_ptr, None)
            .map_err(|e| FawnError::SystemError(format!("Failed to scan for flush: {}", e)))?;

        if records.is_empty() {
            return Ok(()); // nothing to flush
        }

        let mut client = FawnBackendServiceClient::connect(dest.get_http_addr()).await
            .map_err(|e| FawnError::RpcError(format!("Failed to connect to destination: {}", e)))?;

        // convert Vec -> stream that advances last_sent_ptr after each record send
        let last_sent_ptr = Arc::clone(&self.last_sent_ptr);
        let mut last_ptr_sent = None;
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            for (ptr, flag, k, v) in records {
                if flag != RecordFlags::Put {
                    continue; // only flush Put records
                }
                let entry = ValueEntry {
                    key_id: k,
                    value: v.clone(),
                };
                if tx.send(entry).await.is_err() {
                    eprintln!("Failed to send entry during flush");
                    break; // stop if the receiver is gone
                }

                // advance last_sent_ptr to the current record ptr
                let mut guard = last_sent_ptr.lock().unwrap();
                if *guard < ptr {
                    *guard = ptr;
                    last_ptr_sent = Some(ptr);
                }
            }
        });

        // wait for flush_data to complete
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        client.flush_data(stream).await
            .map_err(|e| FawnError::RpcError(format!("Failed to flush data to destination: {}", e)))?;

        // persist the last_sent_ptr if it was advanced
        if let Some(ptr) = last_ptr_sent {
            self.advance_ptr_to_dest(&self.last_sent_ptr, ptr)?;
        }

        Ok(())
    }

    // should be called once the backend node starts up
    pub async fn replay_unacked_ops(&self) -> FawnResult<()> {
        let mut ops = self.storage
            .scan_after_ptr_in_range(self.last_acked_ptr.lock().unwrap().clone(), None)?;

        // replay each Put record after the last acked pointer
        for (ptr_after_record, flag, key_id, value) in ops.drain(..) {
            if flag != RecordFlags::Put {
                continue; // only replay Put for now
            }

            // keep retrying until successfully forwarded
            loop {
                match self.forward_once(key_id, value.clone(), 0).await {
                    Ok(true) => {
                        self.advance_ptr_to_dest(&self.last_acked_ptr, ptr_after_record)?; // advance ack pointer
                        break;
                    }
                    Ok(false) | Err(_) => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        continue; // retry the operation
                    }
                }
            }
        }

        Ok(())
    }

    /// Advances the given pointer (curr_ptr) to dest_ptr if dest_ptr is greater.
    /// Persists the change if advanced.
    fn advance_ptr_to_dest(&self, curr_ptr: &Arc<Mutex<RecordPtr>>, dest_ptr: RecordPtr) -> FawnResult<()> {
        let mut ptr_guard = curr_ptr.lock().unwrap();
        if *ptr_guard < dest_ptr {
            *ptr_guard = dest_ptr;
            self.persist_meta()?; // persist after advancing
        }
        Ok(())
    }


    fn persist_meta(&self) -> std::io::Result<()>  {
        let m = Meta {
            last_acked_ptr: self.last_acked_ptr.lock().unwrap().clone(),
            last_sent_ptr: self.last_sent_ptr.lock().unwrap().clone(),
        }; 
        m.store(&self.meta_path)
    }
}