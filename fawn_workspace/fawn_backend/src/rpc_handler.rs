use fawn_common::fawn_backend_api::{fawn_backend_service_client::FawnBackendServiceClient, StoreRequest, ValueEntry};
use std::path::PathBuf;
use std::ptr;
use std::sync::{Arc, Mutex};
use futures_util::StreamExt; // for .next()
use log::{info, error, warn};

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

impl StorageContext {
    /// Increments the last_acked_ptr to the given pointer and persists the metadata
    fn increment_ack_ptr(&self, ptr_after_record: RecordPtr) -> FawnResult<()> {
        {
            let mut ptr_guard = self.last_acked_ptr.lock().unwrap();
            if *ptr_guard < ptr_after_record {
                *ptr_guard = ptr_after_record;
            }
        }
        self.persist_meta()?;
        Ok(())
    }

    /// Increments the last_sent_ptr to the given pointer and persists the metadata
    fn increment_sent_ptr(&self, ptr_after_record: RecordPtr) -> FawnResult<()> {
        {
            let mut ptr_guard = self.last_sent_ptr.lock().unwrap();
            if *ptr_guard < ptr_after_record {
                *ptr_guard = ptr_after_record;
            }
        }
        self.persist_meta()?;
        Ok(())
    }

    /// Resets the last_sent_ptr to zero and persists the metadata
    fn reset_sent_ptr(&self) -> FawnResult<()> {
        {
            let mut ptr_guard = self.last_sent_ptr.lock().unwrap();
            *ptr_guard = RecordPtr::zero();
        }
        self.persist_meta()?;
        Ok(())
    }
    
}

pub enum StorageRole {
    Primary,    // for operations on primary storage
    Temporary,  // for operations on temporary storage
}

#[derive(Clone)]
pub struct BackendHandler {
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
        state: Arc<BackendSystemState>,
    ) -> FawnResult<Self> {
        info!("Initializing BackendHandler with primary storage at {:?} and temp storage at {:?}", 
            primary_meta_path, temp_meta_path);
        
        // load meta for primary and temp
        let primary_meta = Meta::load(&primary_meta_path)
            .map_err(|e| FawnError::SystemError(format!("Failed to load primary metadata: {}", e)))?;
        let temp_meta = Meta::load(&temp_meta_path)
            .map_err(|e| FawnError::SystemError(format!("Failed to load temp metadata: {}", e)))?;

        info!("Successfully loaded metadata for both primary and temp storage");
        
        Ok(Self {
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

    pub fn handle_get_value(&self, key_id: u32) -> FawnResult<Vec<u8>> {
        info!("Handling get_value request for key_id: {}", key_id);

        // Temp storage may have fresher data, so check it first.
        let temp_ctx = self.get_context_by_role(StorageRole::Temporary);
        if let Ok(Some(value)) = temp_ctx.storage.get(key_id) {
            info!("Successfully retrieved value for key_id: {} from temporary storage", key_id);
            return Ok(value);
        }

        // If not in temp storage, check primary storage.
        let ctx = self.get_context_by_role(StorageRole::Primary);
        match ctx.storage.get(key_id) {
            Ok(Some(value)) => {
                info!("Successfully retrieved value for key_id: {} from primary storage", key_id);
                Ok(value)
            }
            Ok(None) => {
                info!("No value found for key_id: {} in either storage", key_id);
                Ok(vec![])
            }
            Err(e) => {
                error!("Failed to get value for key_id {}: {}", key_id, e);
                Err(Box::new(FawnError::SystemError(format!("Failed to get value due to storage error: {}", e))))
            }
        }
    }

    pub async fn handle_store_value(&self, key_id: u32, value: Vec<u8>, pass_remaining: u32) -> FawnResult<bool> {
        info!("Handling store_value request for key_id: {}, pass_remaining: {}", key_id, pass_remaining);
        
        // get the current stage and context based on it
        let stage = *self.state.storage_stage.read().await;
        let ctx = self.get_context_by_stage(stage);

        // local append (store on local disk)
        let ptr_after_record = match ctx.storage.put(key_id, value.clone()) {
            Ok(ptr) => {
                info!("Successfully stored value locally for key_id: {}", key_id);
                ptr
            }
            Err(e) => {
                error!("Failed to store value locally for key_id {}: {}", key_id, e);
                return Err(Box::new(FawnError::SystemError(format!("Failed to store value due to storage error: {}", e))));
            }
        };

        // If not the tail, forward the request to the successor.
        if pass_remaining > 0 {
            let successor = self.state.successor.read().await.clone();
            info!("Forwarding store request to successor {} for key_id: {}", successor.id, key_id);
            
            let mut client = match FawnBackendServiceClient::connect(successor.get_http_addr()).await {
                Ok(client) => client,
                Err(e) => {
                    error!("Failed to connect to successor for key_id {}: {}", key_id, e);
                    return Err(Box::new(FawnError::RpcError(format!("Failed to connect to successor: {}", e))));
                }
            };

            let request = StoreRequest {
                key_id,
                value,
                pass_count: pass_remaining - 1,
            }; 
    
            if let Err(e) = client.store_value(request).await {
                error!("Failed to forward store to successor for key_id {}: {}", key_id, e);
                return Err(Box::new(FawnError::RpcError(format!("Failed to forward store to successor: {}", e))));
            }
            info!("Successfully forwarded store request to successor for key_id: {}", key_id);
        }

        // This node is the tail OR forwarding to successor was successful.
        // In either case, we can now acknowledge the local write.
        if let Err(e) = ctx.increment_ack_ptr(ptr_after_record) {
            error!("Failed to increment ack pointer for key_id {}: {}", key_id, e);
            return Err(e);
        }
        info!("Successfully acknowledged local write for key_id: {}", key_id);

        Ok(true)
    }

    /// Collect every record with key \in (lo, hi] and ptr > last_sent_ptr, 
    /// convert to ValueEntry, advance last_sent_ptr, and return the vector containing them.
    /// we will only migrate data from true store.
    pub async fn handle_migrate_data(&self, range: (u32, u32)) -> FawnResult<Vec<ValueEntry>> {
        info!("Handling migrate_data request for range: {:?}", range);
        let ctx = self.get_context_by_role(StorageRole::Primary);
        // This is a bootstrap operation. Always scan from the beginning for correctness.
        let start_ptr = RecordPtr::zero();
        let end_ptr = ctx.last_acked_ptr.lock().unwrap().clone();

        // Vec<(ptr, flag, key, val)>
        let records = match ctx.storage.scan_between_ptr_in_range(start_ptr, end_ptr, Some(range)) {
            Ok(records) => {
                info!("Successfully scanned records for migration in range {:?}", range);
                records
            }
            Err(e) => {
                error!("Failed to scan for migration in range {:?}: {}", range, e);
                return Err(Box::new(FawnError::SystemError(format!("Failed to scan for migration: {}", e))));
            }
        };

        // Get the pointer of the last record before consuming the vector
        let ptr_after_last_retrieved_record = records.last().map(|(ptr, ..)| *ptr);

        // convert to proto entries
        let entries: Vec<ValueEntry> = records.into_iter()
            .filter_map(|(_ptr, flag, k, v)| {
                if flag == RecordFlags::Put {
                    Some(ValueEntry { key_id: k, value: v })
                } else {
                    None
                }
            }).collect();

        // Do not advance last_sent_ptr for this stateless operation.
        info!("Found {} entries to migrate in range {:?}", entries.len(), range);

        Ok(entries)
    }

    pub fn handle_store_migrated_data(&self, key_id: u32, value: Vec<u8>) -> FawnResult<()> {
        info!("Handling store_migrated_data request for key_id: {}", key_id);
        let ctx = self.get_context_by_role(StorageRole::Primary);
        
        let ptr_after_record = match ctx.storage.put(key_id, value) {
            Ok(ptr) => {
                info!("Successfully stored migrated value for key_id: {}", key_id);
                ptr
            }
            Err(e) => {
                error!("Failed to store migrated value for key_id {}: {}", key_id, e);
                return Err(Box::new(FawnError::SystemError(format!("Failed to store value due to storage error: {}", e))));
            }
        };

        if let Err(e) = ctx.increment_ack_ptr(ptr_after_record) {
            error!("Failed to increment ack pointer for migrated data key_id {}: {}", key_id, e);
            return Err(e);
        }
        info!("Successfully acknowledged migrated data for key_id: {}", key_id);
        
        Ok(())
    }

    pub async fn handle_update_chain_member(
        &mut self,
        chain_members: Vec<NodeInfo>,
        ) -> FawnResult<bool> {
        info!("Handling update_chain_member request with {} members", chain_members.len());
        let self_id = self.state.self_info.id;
        
        // Find my position in the chain
        let my_position = chain_members.iter().position(|node| node.id == self_id);
        
        if let Some(pos) = my_position {
            // Update my successor if I'm not the last node in the chain
            if pos < chain_members.len() - 1 {
                let new_successor = &chain_members[pos + 1];
                let mut successor_lock = self.state.successor.write().await;
                *successor_lock = new_successor.clone();
                info!("Updated successor to: {}", new_successor.id);
                
                // Forward the update to my successor (pass down the chain)
                let mut client = match FawnBackendServiceClient::connect(new_successor.get_http_addr()).await {
                    Ok(client) => client,
                    Err(e) => {
                        error!("Failed to connect to successor for chain update: {}", e);
                        return Err(Box::new(FawnError::RpcError(format!("Failed to connect to successor: {}", e))));
                    }
                };

                let chain_info = fawn_common::fawn_backend_api::ChainMemberInfo {
                    chain_members: chain_members.into_iter().map(|node| node.into()).collect(),
                };

                if let Err(e) = client.update_chain_member(chain_info).await {
                    error!("Failed to forward update chain member: {}", e);
                    return Err(Box::new(FawnError::RpcError(format!("Failed to forward update chain member: {}", e))));
                }
                info!("Successfully forwarded chain update to successor");
            } else {
                // I am the tail node - don't call flush, just log
                info!("I am the tail node, chain update complete");
            }
        } else {
            error!("Node not found in chain members list");
            return Err(Box::new(FawnError::SystemError(
                "Node not found in chain members list".to_string()
            )));
        }
        // when we reach the tail
        Ok(true)
    }

    pub async fn handle_flush_data<S>(&self, mut stream: S) -> FawnResult<()>
    where
        S: futures_core::Stream<Item = Result<ValueEntry, tonic::Status>> + Unpin,
    {
        info!("Handling flush_data request");
        let mut count = 0;
        
        while let Some(entry) = stream.next().await {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    error!("Stream error during flush: {}", e);
                    return Err(Box::new(FawnError::SystemError(format!("Stream error: {}", e))));
                }
            };
            
            let key_id = entry.key_id;
            let value = entry.value;
            let ctx = self.get_context_by_role(StorageRole::Primary);
            
            let ptr_after_record = match ctx.storage.put(key_id, value.clone()) {
                Ok(ptr) => ptr,
                Err(e) => {
                    error!("Failed to store value during flush for key_id {}: {}", key_id, e);
                    return Err(Box::new(FawnError::SystemError(format!("Failed to store value due to storage error: {}", e))));
                }
            };

            if let Err(e) = ctx.increment_ack_ptr(ptr_after_record) {
                error!("Failed to increment ack pointer during flush for key_id {}: {}", key_id, e);
                return Err(e);
            }
            count += 1;
        }
        
        info!("Successfully flushed {} entries", count);
        Ok(())
    }

    /// send every records between last_sent_ptr and last_acked_ptr in primary storage to the destination node.
    /// dest should decide whether to filter out records based on its own range.
    pub async fn handle_trigger_flush(&self, dest: &NodeInfo, start_id: u32, end_id: u32) -> FawnResult<()> {
        info!("Handling trigger_flush request to destination node {}", dest.id);
        let ctx = self.get_context_by_role(StorageRole::Primary);
        // This is a sync operation after a join. To ensure correctness without complex state,
        // we re-scan all data. The receiving end's put is idempotent.
        let start_ptr = RecordPtr::zero();
        let end_ptr = ctx.last_acked_ptr.lock().unwrap().clone();

        // Vec<(ptr, flag, key, val)>
        let records = match ctx.storage.scan_between_ptr_in_range(start_ptr, end_ptr, Some((start_id, end_id))) {
            Ok(records) => records,
            Err(e) => {
                error!("Failed to scan for flush: {}", e);
                return Err(Box::new(FawnError::SystemError(format!("Failed to scan for flush: {}", e))));
            }
        };

        if records.is_empty() {
            info!("No records to flush");
            return Ok(());
        }

        let stream = tokio_stream::iter(records.into_iter().filter_map(move |(_ptr, flag, k, v)| {
            if flag != RecordFlags::Put {
                return None;
            }
            Some(ValueEntry { key_id: k, value: v.clone() })
        }));

        let mut client = match FawnBackendServiceClient::connect(dest.get_http_addr()).await {
            Ok(client) => client,
            Err(e) => {
                error!("Failed to connect to destination for flush: {}", e);
                return Err(Box::new(FawnError::RpcError(format!("Failed to connect to destination: {}", e))));
            }
        };

        if let Err(e) = client.flush_data(stream).await {
            error!("Failed to flush data to destination: {}", e);
            return Err(Box::new(FawnError::RpcError(format!("Failed to flush data to destination: {}", e))));
        }

        // The stateless nature of this flush means we no longer manage last_sent_ptr here.
        info!("Successfully flushed data to destination node {}", dest.id);
        Ok(())
    }

    /// Flush every records between last_sent_ptr and last_acked_ptr in the temporary storage into primary storage.
    pub async fn handle_trigger_merge(&self) -> FawnResult<()> {
        info!("Handling trigger_merge request");
        // retrieve records from temporary
        let ctx_temp = self.get_context_by_role(StorageRole::Temporary);
        let records = match ctx_temp.storage.scan_between_ptr_in_range(
            ctx_temp.last_sent_ptr.lock().unwrap().clone(),
            ctx_temp.last_acked_ptr.lock().unwrap().clone(),
            None
        ) {
            Ok(records) => records,
            Err(e) => {
                error!("Failed to scan for flush from temp: {}", e);
                return Err(Box::new(FawnError::SystemError(format!("Failed to scan for flush from temp: {}", e))));
            }
        };
            
        if records.is_empty() {
            info!("No records to merge");
        } else {
            let ptr_after_last_flushed_record = records.last().map(|(ptr, ..)| *ptr);

            // store each record into primary
            let ctx = self.get_context_by_role(StorageRole::Primary);
            let mut count = 0;

            for (_ptr, flag, key_id, value) in records {
                if flag != RecordFlags::Put {
                    continue; // only flush Put records
                }

                let ptr_after_record = match ctx.storage.put(key_id, value.clone()) {
                    Ok(ptr) => ptr,
                    Err(e) => {
                        error!("Failed to store value during merge for key_id {}: {}", key_id, e);
                        return Err(Box::new(FawnError::SystemError(format!("Failed to store value due to storage error: {}", e))));
                    }
                };

                if let Err(e) = ctx.increment_ack_ptr(ptr_after_record) {
                    error!("Failed to increment ack pointer during merge for key_id {}: {}", key_id, e);
                    return Err(e);
                }
                count += 1;
            }

            // advance the last_sent_ptr after all records are flushed
            if let Some(ptr) = ptr_after_last_flushed_record {
                if let Err(e) = ctx_temp.increment_sent_ptr(ptr) {
                    error!("Failed to increment sent pointer after merge: {}", e);
                    return Err(e);
                }
            }
            info!("Successfully merged {} records from temporary to primary storage", count);
        }

        // set stage to Normal
        self.state.switch_stage(Stage::Normal).await
            .map_err(|e| FawnError::SystemError(format!("Failed to switch stage to Normal: {}", e)))?;

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