use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Response, Status};
use tokio_stream::wrappers::ReceiverStream;
use crate::rpc_handler::BackendHandler;
use fawn_common::fawn_backend_api::fawn_backend_service_server::FawnBackendService;
use fawn_common::fawn_backend_api::{
    FlushDataResponse, GetRequest, GetResponse, MigrateDataRequest, PingRequest, PingResponse, StoreRequest, StoreResponse
};
use fawn_common::err::FawnError;
use fawn_common::fawn_backend_api::ValueEntry;

pub struct BackendService {
    // TODO: remove Mutex
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

    // the joiner itself is the RPC client
    async fn migrate_data(
        &self,
        request: tonic::Request<MigrateDataRequest>,
    ) -> std::result::Result<
        tonic::Response<Self::MigrateDataStream>,
        tonic::Status,
    >{
        let msg = request.into_inner();
        let dest_node_info = msg.dest_info.ok_or_else(|| Status::invalid_argument("dest_info is required"))?;
        let dest_node_info = fawn_common::types::NodeInfo::from(dest_node_info);
        
        let start_id = msg.start_id;
        let end_id = msg.end_id;

        // build slice from storage
        let entries = {
            let handler = self.handler.lock().await;
            handler.handle_migrate_data((start_id, end_id))
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        }; 

        let stream = tokio_stream::iter(entries.into_iter().map(Ok));

        Ok(Response::new(stream))
    }

    // should be moved into 
    async fn flush_data(
        &self, 
        request: tonic::Request<tonic::Streaming<ValueEntry>>,
    ) -> std::result::Result<
        tonic::Response<FlushDataResponse>,
        tonic::Status,
    >{
        let mut stream = request.into_inner();
        let handler = self.handler.lock().await;

        handler.handle_flush_data(stream).await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(FlushDataResponse {}))
    }
}