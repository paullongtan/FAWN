use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use async_trait::async_trait;
use fawn_common::{fawn_frontend_api::{fawn_frontend_service_client::FawnFrontendServiceClient, fawn_frontend_service_server::FawnFrontendServiceServer}, types::{KeyValue, NodeInfo}, util};
use fawn_common::util::get_node_id;
use fawn_common::err::{FawnError, FawnResult};
use fawn_common::fawn_frontend_api::fawn_frontend_service_server::FawnFrontendService;
use crate::backend_manager::BackendManager;

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
use tokio::time::{timeout, Duration};
use tonic::{transport::Server, Request, Response, Status};
use crate::rpc_handler::{handle_request_join_ring, handle_finalize_join_ring, handle_get_value, handle_put_value, handle_notify_backend_join, handle_notify_backend_leave};

#[derive(Debug)]
struct SystemState {
    ready: AtomicBool,
    backend_manager: Arc<BackendManager>,
    fronts: Vec<NodeInfo>,
    this: usize,
}

impl SystemState {
    pub fn new(front_addresses: Vec<String>, this: usize) -> FawnResult<Self> {
        let mut fronts: Vec<NodeInfo> = Vec::new();
        for front_address in front_addresses {
            // Remove any existing protocol prefix for parsing
            let clean_addr = front_address.trim_start_matches("http://").trim_start_matches("https://");
            let front = clean_addr.parse::<std::net::SocketAddr>()
                .map_err(|e| FawnError::SystemError(format!("Invalid address format: {}", e)))?;
            let id = get_node_id(&front.ip().to_string(), front.port() as u32);
            fronts.push(NodeInfo::new(front.ip().to_string(), front.port() as u32, id));
        }

        // sort fronts by id to maintain ring order
        fronts.sort_by_key(|b| b.id);

        Ok(Self {
            ready: AtomicBool::new(false),
            backend_manager: Arc::new(BackendManager::new(vec![])),
            fronts,
            this,
        })
    }

    pub async fn get_responsible_frontend(&self, key_id: u32) -> FawnResult<NodeInfo> {
        // each frontend is responsible for a range of backend ids
        // the range is (frontend's predecessor's id, frontend's id]
        // first find the backend successor for the key_id
        // then find the frontend successor for the backend's id
        let backend_successor = self.backend_manager.find_successor(key_id).await.ok_or_else(|| 
            FawnError::NoBackendAvailable("Failed to find backend successor".to_string()))?;
        let frontend_successor = self.find_frontend_successor(backend_successor.id);
        
        Ok(frontend_successor)
    }

    pub fn get_frontend_for_backend_join(&self, backend_id: u32) -> NodeInfo {
        // Find the frontend successor for the backend's id
        self.find_frontend_successor(backend_id)
    }

    fn find_frontend_successor(&self, key_id: u32) -> NodeInfo {

        // if key_id is greater than all node ids, wrap around to first node
        if key_id > self.fronts.last().unwrap().id {
            return self.fronts.first().unwrap().clone();
        }

        self.fronts.iter()
            .find(|b| b.id >= key_id)
            .cloned()
            .unwrap()
    }

    fn find_frontend_predecessor(&self, key_id: u32) -> NodeInfo {

        // if key_id is smaller than all node ids, wrap around to last node
        if key_id <= self.fronts.first().unwrap().id {
            return self.fronts.last().unwrap().clone();
        }
        
        self.fronts.iter()
            .rev()
            .find(|b| b.id < key_id)
            .cloned()
            .unwrap()
    }
}

#[derive(Debug)]
pub struct FrontendNode {
    state: Arc<SystemState>,
}

impl FrontendNode {
    pub fn new(fronts: Vec<String>, this: usize) -> FawnResult<Self> {
        let state = SystemState::new(fronts, this)?;
        let state = Arc::new(state);
        
        Ok(Self {
            state: state.clone(),
        })
    }

    pub async fn start(&mut self) -> FawnResult<()> {
        println!("Starting frontend node");
        let node_info = &self.state.fronts[self.state.this];
        // For server binding, we need just the IP:PORT format
        let addr = format!("{}:{}", node_info.ip, node_info.port)
            .parse()
            .map_err(|e: std::net::AddrParseError| FawnError::SystemError(e.to_string()))?;

        let service = FawnFrontendServiceServer::new(FawnFrontend::new(self.state.clone()));

        // Create shutdown signal
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);
        
        // Handle system signals
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            if let Ok(()) = tokio::signal::ctrl_c().await {
                println!("Received shutdown signal");
                shutdown_tx_clone.send(()).ok();
            }
        });

        let server = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_shutdown(addr, async move {
                shutdown_rx.recv().await.ok();
                println!("Shutting down server...");
            });

        // Start server in a separate task
        let server_handle = tokio::spawn(async move {
            println!("Server starting on {}", addr);
            server.await
        });

        // Wait for server to be bound
        let mut is_bound = false;
        for _ in 0..300 {
            // Try for up to 30 seconds
            match tokio::net::TcpStream::connect(&addr).await {
                Ok(_) => {
                    is_bound = true;
                    break;
                }
                Err(_) => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
        }

        if !is_bound {
            return Err(Box::new(FawnError::SystemError("Server failed to bind".to_string())));
        }

        println!("Server is bound and ready");

        // Ping all other frontends, wait until all frontends are ready
        let all_frontends_ready = self.check_frontend_ready().await?;
        if !all_frontends_ready {
            return Err(Box::new(FawnError::SystemError("Frontends are not ready".to_string())));
        }

        println!("All frontends are ready to serve requests");
        // set SystemState ready to true to start serving requests
        self.set_ready(true);

        // Wait for server to complete
        server_handle.await.map_err(|e| FawnError::SystemError(e.to_string()))??;
        
        Ok(())
    }

    async fn check_frontend_ready(&self) -> FawnResult<bool> {
        let mut all_frontends_ready = false;
        while !all_frontends_ready {
            all_frontends_ready = true;
            for i in 0..self.state.fronts.len() {
                if i != self.state.this {
                    // For client connections, we need the full URL with protocol
                    let addr = self.state.fronts[i].get_http_addr();
                    let connect_result = timeout(
                        Duration::from_millis(300), 
                        FawnFrontendServiceClient::connect(addr)
                    ).await;
                    match connect_result {
                        Ok(Ok(_)) => {
                            continue;
                        }
                        Ok(Err(_)) | Err(_) => {
                            all_frontends_ready = false;
                            break;
                        }
                    }
                }
            }
            if !all_frontends_ready {
                println!("Frontends are not ready, waiting for 1 second");
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
        Ok(all_frontends_ready)
    }


    fn set_ready(&self, ready: bool) {
        self.state.ready.store(ready, Ordering::Relaxed);
    }

    // Add other system management methods
}

#[derive(Clone, Debug)]
struct FawnFrontend {
    state: Arc<SystemState>,
}

impl FawnFrontend {
    pub fn new(state: Arc<SystemState>) -> Self {
        Self {
            state,
        }
    }
}

#[async_trait]
impl FawnFrontendService for FawnFrontend {
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

        if !self.state.ready.load(Ordering::Relaxed) {
            return Err(Status::unavailable("System is not ready yet"));
        }
        let inner = request.into_inner();
        let node_info = inner.node_info.ok_or_else(|| 
            Status::invalid_argument("node_info is required"))?;
        let request_node = fawn_common::types::NodeInfo::from(node_info);

        // check if we are responsible frontend for the backend joining the ring
        let responsible_frontend = self.state.get_frontend_for_backend_join(request_node.id);

        // if we are not responsible frontend for the backend joining the ring, forward the request to the responsible frontend
        if responsible_frontend.id != self.state.fronts[self.state.this].id {
            let addr = responsible_frontend.get_http_addr();
            let mut client = FawnFrontendServiceClient::connect(addr).await.map_err(|e| Status::internal(e.to_string()))?;
            let request = tonic::Request::new(RequestJoinRingRequest {node_info: Some(request_node.clone().into())});
            let response = client.request_join_ring(request).await.map_err(|e| Status::internal(e.to_string()))?;
            return Ok(response);
        }

        let (successor, predecessor) = handle_request_join_ring(self.state.backend_manager.clone(), request_node)
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
        let migrate_success = inner.migrate_success;

        // check if we are responsible frontend for the backend joining the ring
        let responsible_frontend = self.state.get_frontend_for_backend_join(request_node.id);

        // if we are not responsible frontend for the backend joining the ring, forward the request to the responsible frontend
        if responsible_frontend.id != self.state.fronts[self.state.this].id {
            let addr = responsible_frontend.get_http_addr();
            let mut client = FawnFrontendServiceClient::connect(addr).await.map_err(|e| Status::internal(e.to_string()))?;
            let request = tonic::Request::new(FinalizeJoinRingRequest {node_info: Some(request_node.clone().into()), migrate_success: migrate_success});
            let response = client.finalize_join_ring(request).await.map_err(|e| Status::internal(e.to_string()))?;
            return Ok(response);
        }

        let join_ring_success = handle_finalize_join_ring(
            self.state.backend_manager.clone(), 
            request_node.clone(), 
            migrate_success,
            &self.state.fronts,
            self.state.this
        )
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
        let (value, success) = handle_get_value(self.state.backend_manager.clone(), user_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(GetResponse {
            value,
            success,
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

        let success = handle_put_value(self.state.backend_manager.clone(), user_key, value)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PutResponse {
            success,
        }))
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
        let success = handle_notify_backend_join(self.state.backend_manager.clone(), backend_info)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(NotifyBackendJoinResponse {
            success,
        }))
        
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
        let success = handle_notify_backend_leave(self.state.backend_manager.clone(), backend_info)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(NotifyBackendLeaveResponse {
            success,
        }))
    }
    
}