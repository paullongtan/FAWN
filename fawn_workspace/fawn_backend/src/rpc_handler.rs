use fawn_common::fawn_backend_api::{fawn_backend_service_client::FawnBackendServiceClient, StoreRequest};
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
        let ptr = self.storage
            .put(key_id, value.clone())
            .map_err(|e| FawnError::SystemError(format!("Failed to store value due to storage error: {}", e)))?;

        // stop forwarding if no pass_remaining is 0 (tail)
        if pass_remaining == 0 {
            self.ack(ptr)?; // acknowledge the operation
            return Ok(true);
        }

        // forward the store request to the successor
        match self.forward_once( key_id, value, pass_remaining - 1).await? {
            // successor has acked the operation
            true => { 
                self.ack(ptr)?; // acknowledge the operation
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
    /* 
    - I just handle the update membership part
    */
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
                pass_count: 0, // no further forwarding needed
            };
            let response = client.store_value(request).await.map_err(|e| FawnError::RpcError(format!("Failed to send data to predecessor: {}", e)))?;
            if !response.get_ref().success {
                return Err(Box::new(FawnError::SystemError("Failed to send data to predecessor".to_string())));
            }
        }
        Ok(true)
    }
    
    pub async fn handle_update_chain_member(
        &mut self,
        predecessor: Option<NodeInfo>,
        new_node: NodeInfo,
        successor: Option<NodeInfo>,
        old_tail: Option<NodeInfo>,
    ) -> FawnResult<bool> {
        let self_id = self.state.self_info.id;

        // Check if I am the predecessor of the new node
        if let Some(pred) = &predecessor {
            if pred.id == self_id {
                let mut successor_lock = self.state.successor.write().await;
                *successor_lock = new_node.clone();
                println!("Updated successor to new node: {}", new_node.id);
            }
        }

        // Check if I am the successor of the new node
        if let Some(succ) = &successor {
            if succ.id == self_id {
                let mut predecessor_lock = self.state.predecessor.write().await;
                *predecessor_lock = new_node.clone();
                println!("Updated predecessor to new node: {}", new_node.id);
            }
        }

        // Check if I am the new node
        if new_node.id == self_id {
            if let Some(pred) = &predecessor {
                let mut predecessor_lock = self.state.predecessor.write().await;
                *predecessor_lock = pred.clone();
                println!("New node: Updated my predecessor");
            }
            if let Some(succ) = &successor {
                let mut successor_lock = self.state.successor.write().await;
                *successor_lock = succ.clone();
                println!("New node: Updated my successor");
            }
        }

        // 06/08
        // Check if I am the old tail - flush data to new node -- need to flush and then send acks backwards back to the head?

        // need to get the data from a certain range: from last sent --> last acked?
        // during migrate data we now what range of values to send to the new node, but new data will be coming in, need to flush this data
        // we will know this new range of values via a pointer
        
        /*
        To Do: pull and merge changes --> get updated protobuf and backend handler info
        - add switch state after data migration --> switch to temporary stage
        - call flush data on dest=new_node which will know to flush from temp-->normal storage
        trigger flush will be done from the frontend, but i still need to flush data from temporary to normal in this function

        - i do not need to handle the trigger part

        - update the successor and predecessors, actually may jsut need to update the successor, no more predecessor
        - check for the stopping sign when pass, --> if you are teh last member of the chain, if you are , just stop


        for join_ring:
        - similar to what i ahve rn
        - request join, --> list of migrate info
        - migrate on all of the list of nodes provided
        - send the finalize ring join

        - sends ok

        - merge from pauls branch
        - then me and kevin will merge into some integration branch
         */

        if let Some(old_tail_node) = &old_tail {
            if old_tail_node.id == self_id {
                println!("I am the old tail, flushing data to new node: {}", new_node.id);
                // FLUSH....
                //C nnect to the new node and call flush
                let mut client = FawnBackendServiceClient::connect(new_node.get_http_addr()).await
                    .map_err(|e| FawnError::RpcError(format!("Failed to connect to new node for flush: {}", e)))?;
        
                // Call the flush service function
                // client.flush(/* appropriate stream parameter */).await
                //     .map_err(|e| FawnError::RpcError(format!("Failed to flush data to new node: {}", e)))?;
        
                println!("Successfully flushed data to new node: {}", new_node.id);                
                // Don't continue propagation from old tail
                return Ok(true);
            }
        }

        // Forward the update chain member call to successor if needed
        let current_successor = self.state.successor.read().await.clone();
        if current_successor.id != self_id {
            let mut client = FawnBackendServiceClient::connect(current_successor.get_http_addr()).await
                .map_err(|e| FawnError::RpcError(format!("Failed to connect to successor: {}", e)))?;

            let chain_info = fawn_common::fawn_backend_api::ChainMemberInfo {
                predecessor: predecessor.as_ref().map(|p| p.clone().into()),
                new_node: Some(new_node.into()),
                successor: successor.as_ref().map(|s| s.clone().into()),
                old_tail: old_tail.as_ref().map(|t| t.clone().into()),
            };

            client.update_chain_member(chain_info).await
                .map_err(|e| FawnError::RpcError(format!("Failed to forward update chain member: {}", e)))?;
        }

        Ok(true)
    }

    // should be called once the backend node starts up
    pub async fn replay_unacked_ops(&self) -> FawnResult<()> {
        let mut ops = self.storage
            .scan_after_ptr_in_range(self.last_acked_ptr.lock().unwrap().clone(), 0, u32::MAX)?;

        // replay each Put record after the last acked pointer
        for (ptr, flag, key_id, value) in ops.drain(..) {
            if flag != RecordFlags::Put {
                continue; // only replay Put for now
            }

            // keep retrying until successfully forwarded
            loop {
                match self.forward_once(key_id, value.clone(), 0).await {
                    Ok(true) => {
                        self.ack(ptr)?; // ack and break out of retry loop
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

    // advance the last_acked_ptr to the given record pointer
    fn ack(&self, ptr: RecordPtr) -> FawnResult<()> {
        let mut current = self.last_acked_ptr.lock().unwrap();
        if *current < ptr {
            *current = ptr;
            self.persist_meta()?;
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