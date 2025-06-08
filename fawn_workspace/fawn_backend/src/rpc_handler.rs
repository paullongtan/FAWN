use fawn_common::fawn_backend_api::{fawn_backend_service_client::FawnBackendServiceClient, StoreRequest, ValueEntry};
use std::path::PathBuf;
use std::ptr;
use std::sync::{Arc, Mutex};

use fawn_common::types::NodeInfo;
use fawn_common::err::{FawnError, FawnResult};
use fawn_common::util::get_node_id;

use crate::meta::Meta;
use crate::stage::Stage;
use crate::storage::logstore::{LogStructuredStore, RecordPtr, RecordFlags};
use crate::server::BackendSystemState;

#[derive(Clone)]
pub struct StorageContext {
    pub storage: Arc<LogStructuredStore>,
    pub last_acked_ptr: Arc<Mutex<RecordPtr>>, // pointer to last acked record
    pub last_sent_ptr: Arc<Mutex<RecordPtr>>, // pointer to last sent record (for pre-copy)
    pub meta_path: PathBuf, // Path to the metadata file
}

pub enum StorageRole {
    Primary,    // for operations on primary storage
    Temporary,  // for operations on temporary storage
}

#[derive(Clone)]
pub struct BackendHandler {
    stage: Arc<tokio::sync::RwLock<Stage>>,
    stage_meta_path: PathBuf, 

    primary: StorageContext, // for true store operations
    temp: StorageContext, // for temp store operations 

    state: Arc<BackendSystemState>,
}

impl BackendHandler {
    pub fn new(
        primary_storage: LogStructuredStore,
        primary_meta_path: PathBuf,
        temp_storage: LogStructuredStore,
        temp_meta_path: PathBuf,
        stage_meta_path: PathBuf,
        state: Arc<BackendSystemState>,
    ) -> FawnResult<Self> {
        // load meta for primary and temp
        let primary_meta = Meta::load(&primary_meta_path)
            .map_err(|e| FawnError::SystemError(format!("Failed to load primary metadata: {}", e)))?;
        let temp_meta = Meta::load(&temp_meta_path)
            .map_err(|e| FawnError::SystemError(format!("Failed to load temp metadata: {}", e)))?;

        // load stage (default to Normal if not set)
        let stage = Stage::load(&stage_meta_path).unwrap_or(Stage::Normal);

        Ok(Self {
            stage: Arc::new(tokio::sync::RwLock::new(stage)),
            stage_meta_path: stage_meta_path,
            primary: StorageContext {
                storage: Arc::new(primary_storage),
                last_acked_ptr: Arc::new(Mutex::new(primary_meta.last_acked_ptr)),
                last_sent_ptr: Arc::new(Mutex::new(primary_meta.last_sent_ptr)),
                meta_path: primary_meta_path,
            },
            temp: StorageContext {
                storage: Arc::new(temp_storage),
                last_acked_ptr: Arc::new(Mutex::new(temp_meta.last_acked_ptr)),
                last_sent_ptr: Arc::new(Mutex::new(temp_meta.last_sent_ptr)),
                meta_path: temp_meta_path,
            },
            state,
        })
    }

    pub fn handle_ping(&self) -> FawnResult<NodeInfo> {
        Ok(self.state.self_info.clone())
    }

    pub fn handle_get_value(&self, key_id: u32) -> FawnResult<Vec<u8>> {
        let ctx = self.get_context_by_role(StorageRole::Primary);
        match ctx.storage.get(key_id) {
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

    // state check to determine whether to tmp or 
    pub async fn handle_store_value(&self, key_id: u32, value: Vec<u8>, pass_remaining: u32) -> FawnResult<bool> {
        // get the current stage and context based on it
        let stage = *self.stage.read().await;
        let ctx = self.get_context_by_stage(stage);

        // local append (store on local disk)
        let ptr_after_record = ctx.storage
            .put(key_id, value.clone())
            .map_err(|e| FawnError::SystemError(format!("Failed to store value due to storage error: {}", e)))?;

        // stop forwarding if no pass_remaining is 0 (tail)
        if pass_remaining == 0 {
            self.advance_ptr_to_dest(&ctx.last_acked_ptr, ptr_after_record)?; // advance ack pointer
            ctx.persist_meta()?;
            return Ok(true);
        }

        // forward the store request to the successor
        match self.forward_once( key_id, value, pass_remaining - 1).await? {
            // successor has acked the operation
            true => { 
                self.advance_ptr_to_dest(&ctx.last_acked_ptr, ptr_after_record)?; // advance ack pointer
                ctx.persist_meta()?;
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
    /// we will only migrate data from true store.
    // TODO: only collect data < last_acked_ptr
    pub async fn handle_migrate_data(&self, range: (u32, u32)) -> FawnResult<Vec<ValueEntry>> {
        let ctx = self.get_context_by_role(StorageRole::Primary);
        let start_ptr = ctx.last_sent_ptr.lock().unwrap().clone();

        // Vec<(ptr, flag, key, val)>
        let records = ctx.storage
            .scan_after_ptr_in_range(start_ptr, Some(range))
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
        if let Some((ptr_after_last_retrieved_record, ..)) = records.last() {
            self.advance_ptr_to_dest(&ctx.last_sent_ptr, *ptr_after_last_retrieved_record)?;
            ctx.persist_meta()?;
        }

        Ok(entries)
    }

    /// send every records between last_sent_ptr and last_acked_ptr to the destination node.
    /// dest should decide whether to filter out records based on its own range.
    /// we will only flush data from true store.
    pub async fn send_flush(&self, dest: &NodeInfo) -> FawnResult<()> {
        let ctx = self.get_context_by_role(StorageRole::Primary);
        let start_ptr = ctx.last_sent_ptr.lock().unwrap().clone();
        let end_ptr = ctx.last_acked_ptr.lock().unwrap().clone();
        let records = ctx.storage
            .scan_between_ptr_in_range(start_ptr, end_ptr, None)
            .map_err(|e| FawnError::SystemError(format!("Failed to scan for flush: {}", e)))?;

        if records.is_empty() {
            return Ok(()); // nothing to flush
        }

        let mut client = FawnBackendServiceClient::connect(dest.get_http_addr()).await
            .map_err(|e| FawnError::RpcError(format!("Failed to connect to destination: {}", e)))?;

        // convert Vec -> stream that advances last_sent_ptr after each record send
        let last_sent_ptr = Arc::clone(&ctx.last_sent_ptr);
        let mut last_ptr_sent = None;
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        // TODO: streaming logic
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
            self.advance_ptr_to_dest(&ctx.last_sent_ptr, ptr)?;
            ctx.persist_meta()?;
        }

        Ok(())
    }

    /// helper function for flush_data to put record directly into true_store
    pub fn store_into_primary(&self, key_id: u32, value: Vec<u8>) -> FawnResult<bool> {
        let ctx = self.get_context_by_role(StorageRole::Primary);
        let ptr_after_record = ctx.storage
            .put(key_id, value.clone())
            .map_err(|e| FawnError::SystemError(format!("Failed to store value due to storage error: {}", e)))?;

        self.advance_ptr_to_dest(&ctx.last_acked_ptr, ptr_after_record)?; 
        ctx.persist_meta()?;
        
        return Ok(true); // always return true for direct put
    }

    // should be called once the backend node starts up
    pub async fn replay_unacked_ops(&self) -> FawnResult<()> {
        let ctx = self.get_context_by_role(StorageRole::Primary);
        let mut ops = ctx.storage
            .scan_after_ptr_in_range(ctx.last_acked_ptr.lock().unwrap().clone(), None)?;

        // replay each Put record after the last acked pointer
        for (ptr_after_record, flag, key_id, value) in ops.drain(..) {
            if flag != RecordFlags::Put {
                continue; // only replay Put for now
            }

            // keep retrying until successfully forwarded
            loop {
                match self.forward_once(key_id, value.clone(), 0).await {
                    Ok(true) => {
                        self.advance_ptr_to_dest(&ctx.last_acked_ptr, ptr_after_record)?; // advance ack pointer
                        ctx.persist_meta()?; // persist after replaying
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
        }
        Ok(())
    }

    /// Returns the appropriate storage context based on the current stage.
    fn get_context_by_stage(&self, stage: Stage) -> &StorageContext {
        match stage {
            Stage::TempMember => &self.temp,
            _ => &self.primary, // Normal or PreCopy
        }
    }

    /// Returns the appropriate storage context based on a role you want to use.
    fn get_context_by_role(&self, role: StorageRole) -> &StorageContext {
        match role {
            StorageRole::Temporary => &self.temp,
            StorageRole::Primary => &self.primary, // Normal or PreCopy
        }
    }

    /// Switches the backend's stage and persists it to disk.
    pub async fn switch_stage(&self, new_stage: Stage) -> FawnResult<()> {
        {
            let mut stage_guard = self.stage.write().await;
            *stage_guard = new_stage;
        }
        // Persist the new stage to disk using the Stage::store method
        new_stage
            .store(&self.stage_meta_path)
            .map_err(|e| FawnError::SystemError(format!("Failed to persist stage: {}", e)))?;
        Ok(())
    }
}

impl StorageContext {
    fn persist_meta(&self) -> std::io::Result<()>  {
        let m = Meta {
            last_acked_ptr: self.last_acked_ptr.lock().unwrap().clone(),
            last_sent_ptr: self.last_sent_ptr.lock().unwrap().clone(),
        }; 
        m.store(&self.meta_path)
    }
}