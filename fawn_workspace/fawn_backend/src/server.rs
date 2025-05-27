use crate::storage::logstore::LogStructuredStore;
use std::sync::Arc;
use crate::service::BackendService;
use crate::rpc_handler::BackendHandler;
use fawn_common::types::NodeInfo;
use fawn_common::util::get_node_id;
use fawn_common::fawn_backend_api::fawn_backend_service_server::FawnBackendServiceServer;
use fawn_common::fawn_backend_api::fawn_backend_service_client::FawnBackendServiceClient;
use fawn_common::fawn_frontend_api::fawn_frontend_service_client::FawnFrontendServiceClient;
use fawn_common::err::{FawnError, FawnResult};
use fawn_common::fawn_frontend_api::{
    RequestJoinRingRequest,
    FinalizeJoinRingRequest};

use fawn_common::fawn_backend_api::{
    MigrateDataRequest};

#[derive(Clone)]
pub struct BackendSystemState {
    pub successor: Arc<tokio::sync::RwLock<NodeInfo>>,
    pub predecessor: Arc<tokio::sync::RwLock<NodeInfo>>,
    pub prev_predecessor: Arc<tokio::sync::RwLock<NodeInfo>>,
    pub self_info: NodeInfo,
}

impl BackendSystemState {
    pub fn new(self_address: String) -> FawnResult<Self> {
        let clean_addr = self_address.trim_start_matches("http://").trim_start_matches("https://");
        let self_info = clean_addr.parse::<std::net::SocketAddr>()
            .map_err(|e| FawnError::SystemError(format!("Invalid address format: {}", e)))?;
        let self_info = NodeInfo::new(self_info.ip().to_string(), self_info.port() as u32, get_node_id(&self_info.ip().to_string(), self_info.port() as u32));
       
        Ok(Self {
            successor: Arc::new(tokio::sync::RwLock::new(self_info.clone())),
            predecessor: Arc::new(tokio::sync::RwLock::new(self_info.clone())),
            prev_predecessor: Arc::new(tokio::sync::RwLock::new(self_info.clone())),
            self_info,
        })
    }
}

pub struct BackendServer {
    fronts: Vec<NodeInfo>,
    state: Arc<BackendSystemState>,
}

impl BackendServer {
    pub fn new(front_addresses: Vec<String>, self_address: String) -> FawnResult<Self> {
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
        
        let state = BackendSystemState::new(self_address)?;
        Ok(Self { fronts, state: Arc::new(state) })
    }

    pub async fn start(&mut self, log_store: LogStructuredStore) -> FawnResult<()> {

        // spawn a task to start the server
        // wait for the server to be ready
        // join the ring by sending a request to the frontend (request_join_ring)
        // request data migration from the successor
        // wait for the data migration to complete
        // finalize the join ring by sending a request to the frontend (finalize_join_ring)
        // start serving requests

        let node_info = self.state.self_info.clone();
        let handler = BackendHandler::new(log_store, self.state.clone())?;
        let service = FawnBackendServiceServer::new(BackendService::new(handler));

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

        let addr = format!("{}:{}", node_info.ip, node_info.port)
            .parse()
            .map_err(|e: std::net::AddrParseError| FawnError::SystemError(e.to_string()))?;

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

        // join the ring
        self.join_ring().await?;

        println!("Backend is ready to serve requests");

        // Wait for server to complete
        server_handle.await.map_err(|e| FawnError::SystemError(e.to_string()))??;        

        Ok(())
    }

    async fn join_ring(&self) -> FawnResult<(bool)> {
        println!("Joining ring");
        let node_info = self.state.self_info.clone();
        let front_index = node_info.id % self.fronts.len() as u32;
        let front_addr = self.fronts[front_index as usize].get_http_addr();
        let mut front_client = FawnFrontendServiceClient::connect(front_addr).await.map_err(|e| FawnError::RpcError(format!("Failed to connect to frontend: {}", e)))?;
        let request = RequestJoinRingRequest {
            node_info: Some(node_info.clone().into()),
        };
        let response = front_client.request_join_ring(request).await.map_err(|e| FawnError::RpcError(format!("Failed to join ring: {}", e)))?;
        let response = response.into_inner();
        let successor_info = response.successor_info;
        let predecessor_info = response.predecessor_info;
        match predecessor_info {
            Some(predecessor_info) => {
                // update the predecessor in the state
                let predecessor_info = NodeInfo::from(predecessor_info);
                *self.state.predecessor.write().await = predecessor_info.clone();
            }
            None => {}
        }

        // request data migration from the successor
        match successor_info {
            Some(successor_info) => {
                // update the successor in the state
                let successor_info = NodeInfo::from(successor_info);
                *self.state.successor.write().await = successor_info.clone();

                // request data migration from the successor
                println!("Requesting data migration from successor: {:?}", successor_info.get_http_addr());
                let mut successor_client = FawnBackendServiceClient::connect(successor_info.get_http_addr()).await.map_err(|e| FawnError::RpcError(format!("Failed to connect to successor: {}", e)))?;
                let request = MigrateDataRequest {
                    new_predecessor_info: Some(node_info.clone().into()),
                };
                let response = successor_client.migrate_data(request).await.map_err(|e| FawnError::RpcError(format!("Failed to migrate data from successor: {}", e)))?;
                let response = response.into_inner();
                let migrate_success = response.success;
                if !migrate_success {
                    println!("Data migration failed");
                    return Err(Box::new(FawnError::SystemError("Data migration failed".to_string())));
                }
            }
            None => {
                println!("No successor, first node in ring");
            }
        }

        // finalize the join ring
        println!("Finalizing join ring");
        let request = FinalizeJoinRingRequest {
            node_info: Some(node_info.clone().into()),
            migrate_success: true,
        };
        let response = front_client.finalize_join_ring(request).await.map_err(|e| FawnError::RpcError(format!("Failed to finalize join ring: {}", e)))?;
        let response = response.into_inner();
        let join_ring_success = response.join_ring_success;
        if !join_ring_success {
            println!("Finalize join ring failed");
            return Err(Box::new(FawnError::SystemError("Finalize join ring failed".to_string())));
        }

        Ok(join_ring_success)
    }
    
}