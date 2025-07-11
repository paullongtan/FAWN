use crate::storage::logstore::LogStructuredStore;
use crate::meta::Meta;
use std::path::PathBuf;
use std::sync::Arc;
use crate::service::BackendService;
use crate::rpc_handler::BackendHandler;
use crate::stage::Stage;
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

use log::info;

#[derive(Clone)]
pub struct BackendSystemState {
    pub successor: Arc<tokio::sync::RwLock<NodeInfo>>,
    pub self_info: NodeInfo,
    pub storage_stage: Arc<tokio::sync::RwLock<Stage>>,
    stage_meta_path: PathBuf,
}

impl BackendSystemState {
    pub fn new(self_address: String, stage_meta_path: PathBuf) -> FawnResult<Self> {
        let clean_addr = self_address.trim_start_matches("http://").trim_start_matches("https://");
        let self_info = clean_addr.parse::<std::net::SocketAddr>()
            .map_err(|e| FawnError::SystemError(format!("Invalid address format: {}", e)))?;
        let self_info = NodeInfo::new(self_info.ip().to_string(), self_info.port() as u32, get_node_id(&self_info.ip().to_string(), self_info.port() as u32));
       
        Ok(Self {
            successor: Arc::new(tokio::sync::RwLock::new(self_info.clone())),
            self_info,
            storage_stage: Arc::new(tokio::sync::RwLock::new(Stage::Normal)),
            stage_meta_path,
        })
    }

    /// Switches the backend's stage and persists it to disk.
    pub async fn switch_stage(&self, new_stage: Stage) -> FawnResult<()> {
        // Persist the new stage to disk using the Stage::store method
        new_stage
            .store(&self.stage_meta_path)
            .map_err(|e| FawnError::SystemError(format!("Failed to persist stage: {}", e)))?;

        {
            let mut stage_guard = self.storage_stage.write().await;
            *stage_guard = new_stage;
        }
        
        Ok(())
    }
}

pub struct BackendServer {
    fronts: Vec<NodeInfo>,
    state: Arc<BackendSystemState>,
    handler: BackendHandler,
}

impl BackendServer {
    pub fn new(front_addresses: Vec<String>, 
        self_address: String, 
        stage_meta_path: PathBuf,
        primary_storage: LogStructuredStore, 
        primary_meta_path: PathBuf, 
        temp_storage: LogStructuredStore,
        temp_meta_path: PathBuf,
    ) -> FawnResult<Self> {
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
        
        let state = Arc::new(BackendSystemState::new(self_address, stage_meta_path)?);
        let handler = BackendHandler::new(
            primary_storage,
            primary_meta_path,
            temp_storage,
            temp_meta_path,
            state.clone()
        )?;
        Ok(Self { fronts, state, handler })
    }

    pub async fn start(&mut self) -> FawnResult<()> {

        // spawn a task to start the server
        // wait for the server to be ready
        // join the ring by sending a request to the frontend (request_join_ring)
        // request data migration from the successor
        // wait for the data migration to complete
        // finalize the join ring by sending a request to the frontend (finalize_join_ring)
        // start serving requests

        let node_info = self.state.self_info.clone();
        let service = FawnBackendServiceServer::new(BackendService::new(self.handler.clone()));

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

    async fn join_ring(&self) -> FawnResult<bool> {
        info!("Joining ring");
        let node_info = self.state.self_info.clone();
        let front_index = node_info.id % self.fronts.len() as u32;
        let front_addr = self.fronts[front_index as usize].get_http_addr();
        let mut front_client = FawnFrontendServiceClient::connect(front_addr).await.map_err(|e| FawnError::RpcError(format!("Failed to connect to frontend: {}", e)))?;
        let request = RequestJoinRingRequest {
            node_info: Some(node_info.clone().into()),
        };
        let response = front_client.request_join_ring(request).await.map_err(|e| FawnError::RpcError(format!("Failed to join ring: {}", e)))?;
        let response = response.into_inner();

        let successor = response.successor;
        if let Some(successor) = successor {
            let mut successor_lock = self.state.successor.write().await;
            *successor_lock = NodeInfo::from(successor);
        }

        for migrate_info in response.migrate_info {
            let src_info = NodeInfo::from(migrate_info.src_info.unwrap());
            let start_id = migrate_info.start_id;
            let end_id = migrate_info.end_id;
            
            info!("Requesting data migration from {} for range [{}, {}]", 
                     src_info.get_http_addr(), start_id, end_id);
            
            // Connect to the source node and request migration
            let mut src_client = FawnBackendServiceClient::connect(src_info.get_http_addr())
                .await.map_err(|e| FawnError::RpcError(format!("Failed to connect to source {}: {}", src_info.get_http_addr(), e)))?;
            
            let migrate_request = MigrateDataRequest {
                dest_info: Some(node_info.clone().into()),
                start_id,
                end_id,
            };
            
            // Receive the streamed data --> put into normal storage
            let mut response_stream = src_client.migrate_data(migrate_request).await
                .map_err(|e| FawnError::RpcError(format!("Failed to start data migration from {}: {}", src_info.get_http_addr(), e)))?
                .into_inner();
            
            // Process each value from the stream
            while let Some(value_entry) = response_stream.message().await
                .map_err(|e| FawnError::RpcError(format!("Failed to receive migration data: {}", e)))? {
                self.handler.handle_store_migrated_data(value_entry.key_id, value_entry.value.clone())
                    .map_err(|e| FawnError::SystemError(format!("Failed to store migrated data: {}", e)))?;
                println!("Received migrated data: key_id={}", value_entry.key_id);
            }
            
            info!("Completed data migration from {}", src_info.get_http_addr());
        }
        
        // change the stage to temp member
        info!("Switching stage from Normal to TempMember");
        self.state.switch_stage(Stage::TempMember).await
            .map_err(|e| FawnError::SystemError(format!("Failed to switch stage to TempMember: {}", e)))?;

        info!("Finalizing join ring");
        let request = FinalizeJoinRingRequest {
            node_info: Some(node_info.clone().into()),
        };

        // finalize_join_ring: will handle the update_chain_membership, flush data from data src, and trigger merge
        let response = front_client.finalize_join_ring(request).await
            .map_err(|e| FawnError::RpcError(format!("Failed to finalize join ring: {}", e)))?;
        let response = response.into_inner();
        
        info!("Successfully joined the ring");

        Ok(true)
    }
    
}