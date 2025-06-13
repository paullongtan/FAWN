use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Response, Status};
use tokio_stream::wrappers::ReceiverStream;
use crate::rpc_handler::BackendHandler;
use fawn_common::fawn_backend_api::fawn_backend_service_server::FawnBackendService;
use fawn_common::fawn_backend_api::{
    FlushDataResponse, GetRequest, GetResponse, 
    MigrateDataRequest, PingRequest, PingResponse, 
    StoreRequest, StoreResponse, 
    TriggerMergeRequest, TriggerMergeResponse, 
    TriggerFlushRequest, TriggerFlushResponse, 
    ChainMemberInfo, UpdateChainMemberResponse,
};
use fawn_common::err::FawnError;
use fawn_common::fawn_backend_api::ValueEntry;

pub struct BackendService {
    handler: Arc<BackendHandler>,
}

impl BackendService {
    pub fn new(handler: BackendHandler) -> Self {
        Self { handler: Arc::new(handler) }
    }
}

#[async_trait]
impl FawnBackendService for BackendService {
    async fn ping(
        &self,
        request: tonic::Request<PingRequest>,
    ) -> std::result::Result<tonic::Response<PingResponse>, tonic::Status>{
        let _self_info = self.handler.handle_ping().map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PingResponse {}))
    }

    async fn get_value(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> std::result::Result<tonic::Response<GetResponse>, tonic::Status>{
        let key_id = request.into_inner().key_id;
        let value = self.handler.handle_get_value(key_id).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(GetResponse {
            value,
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
        let _success = self.handler
                            .handle_store_value(key_id, value, pass_remaining).await
                            .map_err(|e| Status::internal(e.to_string()))?  ;
        Ok(Response::new(StoreResponse {}))
    }

    /// Define the type of the stream returned by the migrate_data method
    type MigrateDataStream = tokio_stream::Iter<
        std::iter::Map<
            std::vec::IntoIter<ValueEntry>,
            fn(ValueEntry) -> Result<ValueEntry, tonic::Status>
        >
    >;

    // the joiner itself is the RPC client
    async fn migrate_data(
        &self,
        request: tonic::Request<MigrateDataRequest>,
    ) -> std::result::Result<
        tonic::Response<Self::MigrateDataStream>,
        tonic::Status,
    >{
        let msg = request.into_inner();

        let start_id = msg.start_id;
        let end_id = msg.end_id;

        // build slice from storage
        let entries = {
            self.handler.handle_migrate_data((start_id, end_id))
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        }; 

        let stream = tokio_stream::iter(
            entries.into_iter().map(Result::<ValueEntry, tonic::Status>::Ok as fn(ValueEntry) -> Result<ValueEntry, tonic::Status>)
        );

        Ok(Response::new(stream))
    }

    async fn update_chain_member(
        &self,
        request: tonic::Request<ChainMemberInfo>,
    ) -> std::result::Result<tonic::Response<UpdateChainMemberResponse>, tonic::Status> {
        let msg = request.into_inner();
        
        // Convert protobuf NodeInfo to internal NodeInfo
        let chain_members: Vec<fawn_common::types::NodeInfo> = msg.chain_members
            .into_iter()
            .map(|node| node.into())
            .collect();
        
        let mut handler = self.handler.as_ref().clone(); // Get a mutable reference
        handler.handle_update_chain_member(chain_members).await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        Ok(Response::new(UpdateChainMemberResponse {}))
    }

    // should be moved into 
    async fn flush_data(
        &self, 
        request: tonic::Request<tonic::Streaming<ValueEntry>>,
    ) -> std::result::Result<
        tonic::Response<FlushDataResponse>,
        tonic::Status,
    >{
        let stream = request.into_inner();

        self.handler.handle_flush_data(stream).await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(FlushDataResponse {}))
    }

    /// Trigger a merge operation on the backend (flush every records in temp storage into the primary storage)
    async fn trigger_merge(
        &self, 
        request: tonic::Request<TriggerMergeRequest>, 
    ) -> std::result::Result<
        tonic::Response<TriggerMergeResponse>, 
        tonic::Status, 
    >{
        let _ = request.into_inner(); // currently unused, but could be used for future extensions

        self.handler.handle_trigger_merge().await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(TriggerMergeResponse {}))
    }

    async fn trigger_flush(
        &self, 
        request: tonic::Request<TriggerFlushRequest>, 
    ) -> std::result::Result<
        tonic::Response<TriggerFlushResponse>, 
        tonic::Status,
    >{
        let msg = request.into_inner();
        let new_node_info = msg.new_node
            .map(fawn_common::types::NodeInfo::from)
            .ok_or_else(|| Status::invalid_argument("new_node is required"))?;

        self.handler.handle_trigger_flush(&new_node_info).await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(TriggerFlushResponse {}))
    }
}