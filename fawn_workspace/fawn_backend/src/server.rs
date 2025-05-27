use async_trait::async_trait;
use tonic::{transport::Server, Request, Response, Status};
use crate::service::BackendService;
use fawn_common::fawn_backend_api::fawn_backend_service_server::FawnBackendServiceServer;

use fawn_common::fawn_backend_api::{
    PingRequest, 
    PingResponse, 
    GetRequest, 
    GetResponse, 
    StoreRequest, 
    StoreResponse, 
    UpdateSuccessorRequest, 
    UpdateSuccessorResponse, 
    PrepareForSplitRequest, 
    PrepareForSplitResponse, 
    MigrateDataRequest,
    MigrateDataResponse};

pub struct BackendServer {
    service: BackendService,
}

impl BackendServer {
    pub fn new(service: BackendService) -> Self {
        Self { service }
    }
}

#[async_trait]
impl FawnBackendServiceServer for BackendServer {
    async fn ping(
        &self,
        request: tonic::Request<PingRequest>,
    ) -> std::result::Result<tonic::Response<PingResponse>, tonic::Status>{
        let self_info = self.service.handle_ping().map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PingResponse {
            node_info: Some(self_info.into()),
        }))
    }

    async fn get_value(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> std::result::Result<tonic::Response<GetResponse>, tonic::Status>{
        let key_id = request.key_id;
        let value = self.service.handle_get_value(key_id).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(GetResponse {
            value,
            success: true,
        }))
    }

    async fn store_value(
        &self,
        request: tonic::Request<StoreRequest>,
    ) -> std::result::Result<tonic::Response<StoreResponse>, tonic::Status>{
        let key_id = request.key_id;
        let value = request.value;
        let success = self.service.handle_store_value(key_id, value).map_err(|e| Status::internal(e.to_string()))?  ;
        Ok(Response::new(StoreResponse {
            success,
        }))
    }

    async fn update_successor(
        &self,
        request: tonic::Request<UpdateSuccessorRequest>,
    ) -> std::result::Result<
        tonic::Response<UpdateSuccessorResponse>,
        tonic::Status,
    >{
        let successor_info = request.successor_info.ok_or_else(|| Status::invalid_argument("successor_info is required"))?;
        let successor_info = fawn_common::types::NodeInfo::from(successor_info);
        let success = self.service.handle_update_successor(&successor_info).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(UpdateSuccessorResponse {
            success,
        }))
    }
    async fn prepare_for_split(
        &self,
        request: tonic::Request<PrepareForSplitRequest>,
    ) -> std::result::Result<
        tonic::Response<PrepareForSplitResponse>,
        tonic::Status,
    >{
        let new_predecessor_info = request.new_predecessor_info.ok_or_else(|| Status::invalid_argument("new_predecessor_info is required"))?;
        let new_predecessor_info = fawn_common::types::NodeInfo::from(new_predecessor_info);
        let success = self.service.handle_prepare_for_split(&new_predecessor_info).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PrepareForSplitResponse {
            success,
        }))
    }
    async fn migrate_data(
        &self,
        request: tonic::Request<super::MigrateDataRequest>,
    ) -> std::result::Result<
        tonic::Response<super::MigrateDataResponse>,
        tonic::Status,
    >{
        let new_predecessor_info = request.new_predecessor_info.ok_or_else(|| Status::invalid_argument("new_predecessor_info is required"))?;
        let new_predecessor_info = fawn_common::types::NodeInfo::from(new_predecessor_info);
        let success = self.service.handle_migrate_data(&new_predecessor_info).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(MigrateDataResponse {
            success,
        }))
    }
}