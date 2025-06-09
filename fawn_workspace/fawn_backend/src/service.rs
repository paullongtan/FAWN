use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Response, Status};
use crate::rpc_handler::BackendHandler;
use fawn_common::fawn_backend_api::fawn_backend_service_server::FawnBackendService;
use fawn_common::fawn_backend_api::{
    ChainMemberInfo, GetRequest, GetResponse, MigrateDataRequest, PingRequest, PingResponse, StoreRequest, StoreResponse, UpdateChainMemberResponse
};
use fawn_common::err::FawnError;

pub struct BackendService {
    handler: Arc<Mutex<BackendHandler>>,
}

impl BackendService {
    pub fn new(handler: BackendHandler) -> Self {
        Self { handler: Arc::new(Mutex::new(handler)) }
    }
}

#[async_trait]
impl FawnBackendService for BackendService {
    async fn ping(
        &self,
        request: tonic::Request<PingRequest>,
    ) -> std::result::Result<tonic::Response<PingResponse>, tonic::Status>{
        let self_info = self.handler.lock().await.handle_ping().map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PingResponse {
            node_info: Some(self_info.into()),
        }))
    }

    async fn get_value(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> std::result::Result<tonic::Response<GetResponse>, tonic::Status>{
        let key_id = request.into_inner().key_id;
        let value = self.handler.lock().await.handle_get_value(key_id).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(GetResponse {
            value,
            success: true,
        }))
    }

    async fn store_value(
        &self,
        request: tonic::Request<StoreRequest>,
    ) -> std::result::Result<tonic::Response<StoreResponse>, tonic::Status>{
        let inner = request.into_inner();
        let key_id = inner.key_id;
        let value = inner.value;
        let pass_remaining = inner.pass_count;
        let success = self.handler.lock().await
                            .handle_store_value(key_id, value, pass_remaining).await
                            .map_err(|e| Status::internal(e.to_string()))?  ;
        Ok(Response::new(StoreResponse {}))
    }
    // handle the index of up to what point has data been sent to new node
    // refactor the response type 
    
    async fn migrate_data(
        &self,
        request: tonic::Request<MigrateDataRequest>,
    ) -> std::result::Result<
        tonic::Response<MigrateDataResponse>,
        tonic::Status,
    >{
        let request_node_info = request.into_inner().new_predecessor_info.ok_or_else(|| Status::invalid_argument("new_predecessor_info is required"))?;
        let request_node_info = fawn_common::types::NodeInfo::from(request_node_info);
        let service = self.handler.lock().await;
        let result = service.handle_migrate_data(&request_node_info).await;
        match result {
            Ok(success) => {
                Ok(Response::new(MigrateDataResponse {
                    success,
                }))
            }
            Err(e) => {
                if let Some(fawn_err) = e.downcast_ref::<FawnError>() {
                    match fawn_err {
                        FawnError::InvalidRequest(msg) => {
                            Err(Status::invalid_argument(msg))
                        }
                        FawnError::SystemError(msg) => {
                            Err(Status::internal(msg))
                        }
                        _ => {
                            Err(Status::internal(e.to_string()))
                        }
                    }
                } else {
                    Err(Status::internal(e.to_string()))
                }
            }
        }
    }
    async fn update_chain_member(
        &self,
        request: tonic::Request<ChainMemberInfo>,
    ) -> std::result::Result<tonic::Response<UpdateChainMemberResponse>, tonic::Status> {
        let chain_info = request.into_inner();
        
        let predecessor = chain_info.predecessor.map(|p| fawn_common::types::NodeInfo::from(p));
        let new_node = chain_info.new_node.ok_or_else(|| Status::invalid_argument("new_node is required"))?;
        let new_node = fawn_common::types::NodeInfo::from(new_node);
        let successor = chain_info.successor.map(|s| fawn_common::types::NodeInfo::from(s));
        let old_tail = chain_info.old_tail.map(|t| fawn_common::types::NodeInfo::from(t));

        let mut handler = self.handler.lock().await;
        let result = handler.handle_update_chain_member(predecessor, new_node, successor, old_tail).await;
        
        match result {
            Ok(_) => Ok(Response::new(UpdateChainMemberResponse {})),
            Err(e) => {
                if let Some(fawn_err) = e.downcast_ref::<FawnError>() {
                    match fawn_err {
                        FawnError::InvalidRequest(msg) => Err(Status::invalid_argument(msg)),
                        FawnError::SystemError(msg) => Err(Status::internal(msg)),
                        _ => Err(Status::internal(e.to_string())),
                    }
                } else {
                    Err(Status::internal(e.to_string()))
                }
            }
        }
    }
}