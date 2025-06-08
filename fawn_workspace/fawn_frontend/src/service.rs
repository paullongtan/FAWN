use async_trait::async_trait;
use std::sync::{Arc, atomic::Ordering};
use tonic::{Response, Status};
use crate::rpc_handler::FrontendHandler;
use crate::server::FrontendSystemState;
use fawn_common::fawn_frontend_api::fawn_frontend_service_server::FawnFrontendService;
use fawn_common::fawn_frontend_api::fawn_frontend_service_client::FawnFrontendServiceClient;
use fawn_common::fawn_frontend_api::{
    RequestJoinRingRequest,
    RequestJoinRingResponse,
    FinalizeJoinRingRequest,
    FinalizeJoinRingResponse,
    GetRequest,
    GetResponse,
    PutRequest,
    PutResponse,
    NotifyBackendJoinRequest,
    NotifyBackendJoinResponse,
    NotifyBackendLeaveRequest,
    NotifyBackendLeaveResponse,
};
use fawn_common::util;

pub struct FrontendService {
    handler: Arc<FrontendHandler>,
    state: Arc<FrontendSystemState>,
}

impl FrontendService {
    pub fn new(handler: FrontendHandler, state: Arc<FrontendSystemState>) -> Self {
        Self {
            handler: Arc::new(handler),
            state,
        }
    }
}

#[async_trait]
impl FawnFrontendService for FrontendService {
    async fn request_join_ring(
        &self,
        request: tonic::Request<RequestJoinRingRequest>,
    ) -> Result<Response<RequestJoinRingResponse>, Status> {
        // for backend nodes to join the ring, they need to send a join ring request to the frontend
        // the frontend will find the backend successor and predecessor of the requesting backend
        // the frontend will notify the successor and predecessor of the new backend
        // the successor and predeccessor information will be returned to the requesting backend
        // the requesting backend will then migrate responsible data from its successor backend
        // the requesting backend will then send a request to the frontend to finalize the join

        // todo!("Update to support chain replication joins");
        // all frontend will receive this from each joining backend
        // the frontend will update the active_backends list
        // the frontend will also check if the new backend need to join its chain
        // if the new backend is within its chains, the frontend will notify the new backend of the migrate info on its chain

        if !self.state.ready.load(Ordering::Relaxed) {
            return Err(Status::unavailable("System is not ready yet"));
        }
        let inner = request.into_inner();
        let node_info = inner.node_info.ok_or_else(|| 
            Status::invalid_argument("node_info is required"))?;
        let request_node = fawn_common::types::NodeInfo::from(node_info);
        let predecessor = self.state.backend_manager.find_predecessor(request_node.id).await;
        let successor = self.state.backend_manager.find_successor(request_node.id).await;

        let migrate_info = self.handler.handle_request_join_ring(request_node)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let migrate_info = migrate_info.into_iter().map(|info| info.into()).collect();
        Ok(Response::new(RequestJoinRingResponse {
            predecessor: predecessor.map(|n| n.into()),
            successor: successor.map(|n| n.into()),
            migrate_info,
        }))
    }

    async fn finalize_join_ring(
        &self,
        request: tonic::Request<FinalizeJoinRingRequest>,
    ) -> Result<Response<FinalizeJoinRingResponse>, Status> {
        // for backend nodes to finalize the join ring, they need to send a finalize join ring request to the frontend
        // the frontend will update its backend list
        // the frontend will then notify other frontends of the new node's node info to update their backend list
        // the frontend will then return a response to the requesting backend

        if !self.state.ready.load(Ordering::Relaxed) {
            return Err(Status::unavailable("System is not ready yet"));
        }
        let inner = request.into_inner();
        let node_info = inner.node_info.ok_or_else(|| 
            Status::invalid_argument("node_info is required"))?;
        let request_node = fawn_common::types::NodeInfo::from(node_info);

        self.handler.handle_finalize_join_ring(request_node).await?;
        
        Ok(Response::new(FinalizeJoinRingResponse {}))
    }

    async fn get_value(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> std::result::Result<tonic::Response<GetResponse>, tonic::Status> {

        if !self.state.ready.load(Ordering::Relaxed) {
            return Err(Status::unavailable("System is not ready yet"));
        }
        let inner = request.into_inner();
        let user_key = inner.user_key;
        let key_id = util::get_key_id(&user_key);

        // check if we are responsible frontend for the key
        match self.state.get_responsible_frontend(key_id).await {
            Ok(responsible_frontend) => {
                // if we are not responsible frontend for the key, forward the request to the responsible frontend
                if responsible_frontend.id != self.state.fronts[self.state.this].id {
                    let addr = responsible_frontend.get_http_addr();
                    let mut client = FawnFrontendServiceClient::connect(addr).await.map_err(|e| Status::internal(e.to_string()))?;
                    let request = tonic::Request::new(GetRequest {user_key: user_key});
                    let response = client.get_value(request).await.map_err(|e| Status::internal(e.to_string()))?;
                    return Ok(response);
                }
            }
            Err(e) => {
                return Err(Status::internal(e.to_string()));
            }
        }
        let value = self.handler.handle_get_value(user_key).await?;
        Ok(Response::new(GetResponse {
            value
        }))
    }

    async fn put_value(
        &self,
        request: tonic::Request<PutRequest>,
    ) -> std::result::Result<tonic::Response<PutResponse>, tonic::Status> {        

        if !self.state.ready.load(Ordering::Relaxed) {
            return Err(Status::unavailable("System is not ready yet"));
        }
        let inner = request.into_inner();
        let user_key = inner.user_key;
        let value = inner.value;
        let key_id = util::get_key_id(&user_key);

        // check if we are responsible frontend for the key
        match self.state.get_responsible_frontend(key_id).await {
            Ok(responsible_frontend) => {
                // if we are not responsible frontend for the key, forward the request to the responsible frontend
                if responsible_frontend.id != self.state.fronts[self.state.this].id {
                    let addr = responsible_frontend.get_http_addr();
                    let mut client = FawnFrontendServiceClient::connect(addr).await.map_err(|e| Status::internal(e.to_string()))?;
                    let request = tonic::Request::new(PutRequest {user_key: user_key, value: value});
                    let response = client.put_value(request).await.map_err(|e| Status::internal(e.to_string()))?;
                    return Ok(response);
                }
            }
            Err(e) => {
                return Err(Status::internal(e.to_string()));
            }
        }

        self.handler.handle_put_value(user_key, value).await?;
        Ok(Response::new(PutResponse {}))
    }

    async fn notify_backend_join(
        &self,
        request: tonic::Request<NotifyBackendJoinRequest>,
    ) -> std::result::Result<
        tonic::Response<NotifyBackendJoinResponse>,
        tonic::Status,
    >{
        if !self.state.ready.load(Ordering::Relaxed) {
            return Err(Status::unavailable("System is not ready yet"));
        }
        let inner = request.into_inner();
        let backend_info = inner.backend_info.ok_or_else(|| 
            Status::invalid_argument("backend_info is required"))?;
        let backend_info = fawn_common::types::NodeInfo::from(backend_info);
        self.handler.handle_notify_backend_join(backend_info).await;
        Ok(Response::new(NotifyBackendJoinResponse {}))
        
    }
    async fn notify_backend_leave(
        &self,
        request: tonic::Request<NotifyBackendLeaveRequest>,
    ) -> std::result::Result<
        tonic::Response<NotifyBackendLeaveResponse>,
        tonic::Status,
    >{
        if !self.state.ready.load(Ordering::Relaxed) {
            return Err(Status::unavailable("System is not ready yet"));
        }
        let inner = request.into_inner();
        let backend_info = inner.backend_info.ok_or_else(|| 
            Status::invalid_argument("backend_info is required"))?;
        let backend_info = fawn_common::types::NodeInfo::from(backend_info);
        self.handler.handle_notify_backend_leave(backend_info).await;
        Ok(Response::new(NotifyBackendLeaveResponse {}))
    }
    
}