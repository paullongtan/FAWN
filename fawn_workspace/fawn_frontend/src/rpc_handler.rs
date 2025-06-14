use std::sync::Arc;
use crate::backend_manager::BackendManager;
use crate::server::FrontendSystemState;
use crate::types::MigrateInfo;
use log::{info, error, warn};

use fawn_common::types::NodeInfo;
use fawn_common::err::{FawnError, FawnResult};
use fawn_common::util::get_key_id;
use fawn_common::fawn_backend_api::fawn_backend_service_client::FawnBackendServiceClient;
use fawn_common::fawn_frontend_api::fawn_frontend_service_client::FawnFrontendServiceClient;
use fawn_common::fawn_backend_api::{
    ChainMemberInfo,
    TriggerFlushRequest,
    TriggerMergeRequest,
};
use tonic::Request;
use fawn_common::fawn_frontend_api::NotifyBackendJoinRequest;
use tonic::Status;

pub struct FrontendHandler {
    state: Arc<FrontendSystemState>,
}

impl FrontendHandler {
    pub fn new(state: Arc<FrontendSystemState>) -> Self {
        info!("Initializing FrontendHandler");
        Self { state }
    }

    pub async fn handle_request_join_ring(
        &self,
        new_node: NodeInfo,
    ) -> FawnResult<Vec<MigrateInfo>> {
        info!("Handling request to join ring from node: {}", new_node.get_http_addr());
        // get affected chains
        let updated_chain_infos = self.state.backend_manager.get_updated_chain(new_node).await;

        // caluculate data range to migrate for each chain
        // range: (predecessor, head node]
        let mut migrate_list = Vec::new();
        for (chain, data_src_opt, predecessor) in updated_chain_infos {
            if let Some(data_src) = data_src_opt {
                let head_node = chain.first().unwrap();
                let start_id = predecessor.id;
                let end_id = head_node.id;
                info!("Calculated migration range for chain: start_id={}, end_id={}, data_src={}", 
                    start_id, end_id, data_src.get_http_addr());
                migrate_list.push(MigrateInfo {
                    src_info: data_src.clone(),
                    start_id,
                    end_id,
                });
            }
        }
        Ok(migrate_list)
    }
    
    pub async fn handle_finalize_join_ring(
        &self,
        new_node: NodeInfo,
    ) -> Result<(), Status> {
        info!("Finalizing join ring for node: {}", new_node.get_http_addr());
        // for each of the chain that the new node is in, call update chain member on the head node
        let updated_chain_infos = self.state.backend_manager.get_updated_chain(new_node.clone()).await;
        for (chain, old_tail_opt, predecessor) in updated_chain_infos {
            let head_node = chain.first().unwrap();
            let addr = head_node.get_http_addr();
            info!("Updating chain members for head node: {}", addr);
            let mut head_client = FawnBackendServiceClient::connect(addr.clone())
                .await
                .map_err(|e| {
                    error!("Failed to connect to backend {}: {}", addr, e);
                    Status::internal(format!("Failed to connect to backend: {}", e))
                })?;
            let request = Request::new(ChainMemberInfo {
                chain_members: chain.clone().into_iter().map(|n| n.into()).collect(),
            });
            head_client
                .update_chain_member(request)
                .await
                .map_err(|e| {
                    error!("Failed to update chain member for {}: {}", addr, e);
                    Status::internal(format!("Failed to update chain member: {}", e))
                })?;

            if let Some(old_tail) = old_tail_opt {
                let old_tail_addr = old_tail.get_http_addr();
                info!("Triggering flush on old tail node: {}", old_tail_addr);
                let mut old_tail_client = FawnBackendServiceClient::connect(old_tail_addr.clone())
                    .await
                    .map_err(|e| {
                        error!("Failed to connect to old tail node {}: {}", old_tail_addr, e);
                        Status::internal(format!("Failed to connect to backend: {}", e))
                    })?;
                
                let flush_request = Request::new(TriggerFlushRequest {
                    new_node: Some(new_node.clone().into()),
                    start_id: predecessor.id,
                    end_id: head_node.id,
                });
                old_tail_client
                    .trigger_flush(flush_request)
                    .await
                    .map_err(|e| {
                        error!("Failed to trigger flush on {}: {}", old_tail_addr, e);
                        Status::internal(format!("Failed to trigger flush: {}", e))
                    })?;
            }
        }
        // call trigger merge on the new node
        let new_node_addr = new_node.get_http_addr();
        info!("Triggering merge on new node: {}", new_node_addr);
        let mut new_node_client = FawnBackendServiceClient::connect(new_node_addr.clone())
            .await
            .map_err(|e| {
                error!("Failed to connect to new node {}: {}", new_node_addr, e);
                Status::internal(format!("Failed to connect to backend: {}", e))
            })?;
        let merge_request = Request::new(TriggerMergeRequest {});
        new_node_client
            .trigger_merge(merge_request)
            .await
            .map_err(|e| {
                error!("Failed to trigger merge on {}: {}", new_node_addr, e);
                Status::internal(format!("Failed to trigger merge: {}", e))
            })?;

        // add the new node to the backend list
        info!("Adding new node to backend list: {}", new_node_addr);
        self.state.backend_manager.add_backend(new_node.clone()).await;

        // notify other frontends of the new node's node info to update their backend list
        for i in 0..self.state.fronts.len() {
            if i != self.state.this {
                let addr = self.state.fronts[i].get_http_addr();
                info!("Notifying frontend {} about new node", addr);
                let mut client = FawnFrontendServiceClient::connect(addr.clone()).await.map_err(|e| {
                    error!("Failed to connect to frontend {}: {}", addr, e);
                    Status::internal(format!("Failed to connect to frontend: {}", e))
                })?;
                let request = Request::new(NotifyBackendJoinRequest {
                    backend_info: Some(new_node.clone().into())
                });
                client.notify_backend_join(request).await.map_err(|e| {
                    error!("Failed to notify frontend {}: {}", addr, e);
                    Status::internal(format!("Failed to notify other frontends: {}", e))
                })?;
            }
        }
    
        info!("Backend {} joined the ring successfully", new_node.get_http_addr());
    
        Ok(())
    }
    
    pub async fn handle_get_value(
        &self,
        user_key: String,
    ) -> Result<Vec<u8>, Status> {
        let key_id = fawn_common::util::get_key_id(&user_key);
        info!("Handling get request for key: {} (id: {})", user_key, key_id);
        let chain_members = self.state.backend_manager.get_chain_members(key_id).await;
        let tail_node = chain_members.last().unwrap();
        
        let addr = tail_node.get_http_addr();
        info!("Getting value from tail node: {}", addr);
        let mut client = FawnBackendServiceClient::connect(addr.clone()).await
            .map_err(|e| {
                error!("Failed to connect to tail node {}: {}", addr, e);
                Status::unavailable(format!("Failed to connect to successor: {}", e))
            })?;
        
        let request = fawn_common::fawn_backend_api::GetRequest {
            key_id,
        };
        
        let response = client.get_value(request).await
            .map_err(|e| {
                error!("Failed to get value from {}: {}", addr, e);
                Status::internal(format!("Failed to get value: {}", e))
            })?;
        
        info!("Successfully retrieved value for key: {}", user_key);
        Ok(response.into_inner().value)
    }
    
    pub async fn handle_put_value(
        &self,
        user_key: String,
        value: Vec<u8>,
    ) -> Result<(), Status> {
        let key_id = fawn_common::util::get_key_id(&user_key);
        info!("Handling put request for key: {} (id: {})", user_key, key_id);
        let chain_members = self.state.backend_manager.get_chain_members(key_id).await;
        let head_node = chain_members.first().unwrap();
        
        let addr = head_node.get_http_addr();
        info!("Storing value in head node: {}", addr);
        let mut client = FawnBackendServiceClient::connect(addr.clone()).await
            .map_err(|e| {
                error!("Failed to connect to head node {}: {}", addr, e);
                Status::unavailable(format!("Failed to connect to successor: {}", e))
            })?;
        
        let pass_count = (chain_members.len() - 1) as u32;
        let request = fawn_common::fawn_backend_api::StoreRequest {
            key_id,
            value,
            pass_count,
        };
        
        client.store_value(request).await
            .map_err(|e| {
                error!("Failed to store value in {}: {}", addr, e);
                Status::internal(format!("Failed to store value: {}", e))
            })?;
        
        info!("Successfully stored value for key: {}", user_key);
        Ok(())
    }
    
    pub async fn handle_notify_backend_join(
        &self,
        backend_info: NodeInfo,
    ) {
        info!("Received notification about new backend: {}", backend_info.get_http_addr());
        self.state.backend_manager.add_backend(backend_info).await;
        info!("Added new backend to the list");
        // print the backend list
        info!("Backend list: {:?}", self.state.backend_manager.get_active_backends().await);
    }
    
    pub async fn handle_notify_backend_leave(
        &self,
        backend_info: NodeInfo,
    ) {
        info!("Received notification about backend leaving: {}", backend_info.get_http_addr());
        self.state.backend_manager.remove_backend(backend_info.id).await;
        info!("Removed backend from the list");
    }
}

