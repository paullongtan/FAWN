use std::sync::Arc;
use crate::backend_manager::BackendManager;
use crate::server::FrontendSystemState;
use crate::types::MigrateInfo;

use fawn_common::types::NodeInfo;
use fawn_common::err::{FawnError, FawnResult};
use fawn_common::util::get_key_id;
use fawn_common::fawn_backend_api::fawn_backend_service_client::FawnBackendServiceClient;
use fawn_common::fawn_frontend_api::fawn_frontend_service_client::FawnFrontendServiceClient;
use fawn_common::fawn_backend_api::{
    ChainMemberInfo,
    TriggerFlushRequest,
};
use tonic::Request;
use fawn_common::fawn_frontend_api::NotifyBackendJoinRequest;
use tonic::Status;

pub struct FrontendHandler {
    backend_manager: Arc<BackendManager>,
    state: Arc<FrontendSystemState>,
}

impl FrontendHandler {
    pub fn new(backend_manager: Arc<BackendManager>, state: Arc<FrontendSystemState>) -> Self {
        Self { backend_manager, state }
    }

    pub async fn handle_request_join_ring(
        &self,
        new_node: NodeInfo,
    ) -> FawnResult<Vec<MigrateInfo>> {
        // get affected chains
        let updated_chain_infos = self.backend_manager.get_updated_chain(new_node).await;

        // caluculate data range to migrate for each chain
        // range: (predecessor, head node]
        let mut migrate_list = Vec::new();
        for (chain, data_src, predecessor) in updated_chain_infos {
            let head_node = chain.first().unwrap();
            let start_id = predecessor.id;
            let end_id = head_node.id;
            migrate_list.push(MigrateInfo {
                src_info: data_src.clone(),
                start_id,
                end_id,
            });
        }
        Ok(migrate_list)
    }
    
    pub async fn handle_finalize_join_ring(
        &self,
        new_node: NodeInfo,
    ) -> Result<(), Status> {
        // for each of the chain that the new node is in, call update chain member on the head node
        let updated_chain_infos = self.backend_manager.get_updated_chain(new_node.clone()).await;
        for (chain, old_tail, _) in updated_chain_infos {
            let head_node = chain.first().unwrap();
            let addr = head_node.get_http_addr();
            let mut head_client = FawnBackendServiceClient::connect(addr)
                .await
                .map_err(|e| Status::internal(format!("Failed to connect to backend: {}", e)))?;
            let request = Request::new(ChainMemberInfo {
                chain_members: chain.clone().into_iter().map(|n| n.into()).collect(),
            });
            head_client
                .update_chain_member(request)
                .await
                .map_err(|e| {
                    Status::internal(format!("Failed to update chain member: {}", e))
                })?;

            let old_tail_addr = old_tail.get_http_addr();
            let mut old_tail_client = FawnBackendServiceClient::connect(old_tail_addr)
                .await
                .map_err(|e| Status::internal(format!("Failed to connect to backend: {}", e)))?;
            let flush_request = Request::new(TriggerFlushRequest {
                new_node: Some(new_node.clone().into()),
            });
            old_tail_client
                .trigger_flush(flush_request)
                .await
                .map_err(|e| Status::internal(format!("Failed to trigger flush: {}", e)))?;
        }

        // add the new node to the backend list
        self.backend_manager.add_backend(new_node.clone()).await;

        // notify other frontends of the new node's node info to update their backend list
        for i in 0..self.state.fronts.len() {
            if i != self.state.this {
                let addr = self.state.fronts[i].get_http_addr();
                let mut client = FawnFrontendServiceClient::connect(addr).await.map_err(|e| Status::internal(format!("Failed to connect to frontend: {}", e)))?;
                let request = Request::new(NotifyBackendJoinRequest {
                    backend_info: Some(new_node.clone().into())
                });
                client.notify_backend_join(request).await.map_err(|e| Status::internal(format!("Failed to notify other frontends: {}", e)))?;
            }
        }
    
        println!("Backend {} joined the ring successfully", new_node.get_http_addr());
    
        Ok(())
    }
    
    pub async fn handle_get_value(
        &self,
        user_key: String,
    ) -> Result<Vec<u8>, Status> {
        let key_id = fawn_common::util::get_key_id(&user_key);
        let chain_members = self.backend_manager.get_chain_members(key_id).await;
        let tail_node = chain_members.last().unwrap();
        
        let addr = tail_node.get_http_addr();
        let mut client = FawnBackendServiceClient::connect(addr).await
            .map_err(|e| Status::unavailable(format!("Failed to connect to successor: {}", e)))?;
        
        let request = fawn_common::fawn_backend_api::GetRequest {
            key_id,
        };
        
        let response = client.get_value(request).await
            .map_err(|e| Status::internal(format!("Failed to get value: {}", e)))?;
        
        Ok(response.into_inner().value)
    }
    
    pub async fn handle_put_value(
        &self,
        user_key: String,
        value: Vec<u8>,
    ) -> Result<(), Status> {
        let key_id = fawn_common::util::get_key_id(&user_key);
        let chain_members = self.backend_manager.get_chain_members(key_id).await;
        let head_node = chain_members.first().unwrap();
        
        let addr = head_node.get_http_addr();
        let mut client = FawnBackendServiceClient::connect(addr).await
            .map_err(|e| Status::unavailable(format!("Failed to connect to successor: {}", e)))?;
        
        let pass_count = (chain_members.len() - 1) as u32;
        let request = fawn_common::fawn_backend_api::StoreRequest {
            key_id,
            value,
            pass_count,
        };
        
        client.store_value(request).await
            .map_err(|e| Status::internal(format!("Failed to store value: {}", e)))?;
        
        Ok(())
    }
    
    pub async fn handle_notify_backend_join(
        &self,
        backend_info: NodeInfo,
    ) {
        self.backend_manager.add_backend(backend_info).await;
    }
    
    pub async fn handle_notify_backend_leave(
        &self,
        backend_info: NodeInfo,
    ) {
        self.backend_manager.remove_backend(backend_info.id).await;
    }
}

