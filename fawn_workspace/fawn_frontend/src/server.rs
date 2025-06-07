use std::sync::{Arc, atomic::{AtomicBool, Ordering}, RwLock};
use fawn_common::{fawn_frontend_api::{fawn_frontend_service_client::FawnFrontendServiceClient, fawn_frontend_service_server::FawnFrontendServiceServer}, types::NodeInfo};
use fawn_common::util::get_node_id;
use fawn_common::err::{FawnError, FawnResult};

use crate::service::FrontendService;
use crate::rpc_handler::FrontendHandler;
use crate::backend_manager::BackendManager;

use tokio::time::{timeout, Duration};
use tonic::{transport::Server, Request, Response, Status};

#[derive(Debug)]
pub struct FrontendSystemState {
    pub ready: AtomicBool,
    pub backend_manager: Arc<BackendManager>,
    pub fronts: Vec<NodeInfo>,
    pub chain_members: Arc<RwLock<Vec<NodeInfo>>>,
    pub this: usize,
}

impl FrontendSystemState {
    pub fn new(front_addresses: Vec<String>, this: usize, replication_count: u32) -> FawnResult<Self> {
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
            backend_manager: Arc::new(BackendManager::new(vec![], replication_count)),
            fronts,
            chain_members: Arc::new(RwLock::new(vec![])), // no backend at system start
            this,
        })
    }

    pub async fn get_responsible_frontend(&self, key_id: u32) -> FawnResult<NodeInfo> {
        // each frontend is responsible for a range of chains with head backend id in (frontend's predecessor's id, frontend's id]
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
pub struct FrontendServer {
    state: Arc<FrontendSystemState>,
}

impl FrontendServer {
    pub fn new(fronts: Vec<String>, this: usize, replication_count: u32) -> FawnResult<Self> {
        let state = FrontendSystemState::new(fronts, this, replication_count)?;
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

        let backend_manager = BackendManager::new(vec![], 3);
        let handler = FrontendHandler::new(Arc::new(backend_manager), self.state.clone());
        let service = FawnFrontendServiceServer::new(FrontendService::new(handler, self.state.clone()));

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

}

