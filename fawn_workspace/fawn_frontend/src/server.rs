use async_trait::async_trait;
use fawn_common::{config::FrontConfig, fawn_frontend_api::fawn_frontend_service_server::FawnFrontendService};

use fawn_common::fawn_frontend_api::{
    RequestJoinRingRequest,
    RequestJoinRingResponse,
    FinalizeJoinRingRequest,
    FinalizeJoinRingResponse,
    GetRequest,
    GetResponse,
    PutRequest,
    PutResponse,
};
use tonic::{transport::Server, Request, Response, Status};
use crate::rpc_handler::{handle_request_join_ring, handle_finalize_join_ring, handle_get_value, handle_put_value};

pub struct FawnFrontend {
    pub fronts: Vec<String>,
    pub active_backs: Vec<String>,
    pub this: usize,
}

#[async_trait]
impl FawnFrontendService for FawnFrontend {
    async fn request_join_ring(
        &self,
        request: tonic::Request<RequestJoinRingRequest>,
    ) -> Result<Response<RequestJoinRingResponse>, Status> {
        let inner = request.into_inner();
        let node_info = inner.node_info.ok_or_else(|| 
            Status::invalid_argument("node_info is required"))?;
        let request_node = fawn_common::types::NodeInfo::from(node_info);
        let (successor, predecessor) = handle_request_join_ring(request_node)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RequestJoinRingResponse {
            successor_info: Some(successor.into()),
            predecessor_info: Some(predecessor.into()),
        }))
    }

    async fn finalize_join_ring(
        &self,
        request: tonic::Request<FinalizeJoinRingRequest>,
    ) -> Result<Response<FinalizeJoinRingResponse>, Status> {
        let inner = request.into_inner();
        let node_info = inner.node_info.ok_or_else(|| 
            Status::invalid_argument("node_info is required"))?;
        let request_node = fawn_common::types::NodeInfo::from(node_info);
        let migrate_success = inner.migrate_success;
        let join_ring_success = handle_finalize_join_ring(request_node, migrate_success)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(FinalizeJoinRingResponse {
            join_ring_success,
        }))
    }

    async fn get_value(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> std::result::Result<tonic::Response<GetResponse>, tonic::Status> {
        let inner = request.into_inner();
        let user_key = inner.user_key;
        let (value, success) = handle_get_value(user_key).await.map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(GetResponse {
            value,
            success,
        }))
    }

    async fn put_value(
        &self,
        request: tonic::Request<PutRequest>,
    ) -> std::result::Result<tonic::Response<PutResponse>, tonic::Status> {
        let inner = request.into_inner();
        let user_key = inner.user_key;
        let value = inner.value;
        let success = handle_put_value(user_key, value).await.map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PutResponse {
            success,
        }))
    }
}