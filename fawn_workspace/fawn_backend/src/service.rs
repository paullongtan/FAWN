use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Response, Status};
use crate::rpc_handler::BackendHandler;
use fawn_common::fawn_backend_api::fawn_backend_service_server::FawnBackendService;
use fawn_common::fawn_backend_api::{
    GetRequest, GetResponse, 
    MigrateDataRequest, PingRequest, 
    PingResponse, StoreRequest, StoreResponse,
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
}