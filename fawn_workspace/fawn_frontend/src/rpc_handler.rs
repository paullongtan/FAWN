use std::sync::Arc;
use fawn_common::types::{NodeInfo, KeyValue};
use fawn_common::err::{FawnError, FawnResult};
use fawn_common::util::get_key_id;
use fawn_common::fawn_backend_api::fawn_backend_service_client::FawnBackendServiceClient;
use crate::backend_manager::BackendManager;
use fawn_common::fawn_backend_api::{
    UpdateSuccessorRequest, 
    PrepareForSplitRequest};
use fawn_common::fawn_frontend_api::fawn_frontend_service_client::FawnFrontendServiceClient;
use tonic::Request;
use fawn_common::fawn_frontend_api::NotifyBackendJoinRequest;

pub async fn handle_request_join_ring(
    backend_manager: Arc<BackendManager>,
    new_node: NodeInfo,
) -> FawnResult<(NodeInfo, NodeInfo)> {
    let successor = backend_manager.find_successor(new_node.id).await
        .ok_or_else(|| Box::new(FawnError::NoBackendAvailable("no backend available".to_string())))?;
    let predecessor = backend_manager.find_predecessor(new_node.id).await
        .ok_or_else(|| Box::new(FawnError::NoBackendAvailable("no backend available".to_string())))?;

    // Notify the successor and predecessor of the new node
    let successor_addr = successor.get_http_addr();
    let predecessor_addr = predecessor.get_http_addr();
    let mut successor_client = FawnBackendServiceClient::connect(successor_addr).await.map_err(|e| FawnError::RpcError(format!("Failed to connect to successor: {}", e)))?;
    let mut predecessor_client = FawnBackendServiceClient::connect(predecessor_addr).await.map_err(|e| FawnError::RpcError(format!("Failed to connect to predecessor: {}", e)))?;

    // Prepare the successor of the new node for the split and update the successor's predecessor to the new node
    successor_client.prepare_for_split(PrepareForSplitRequest {
        new_predecessor_info: Some(new_node.clone().into()),
    }).await?;

    // Update the predecessor of the new node
    predecessor_client.update_successor(UpdateSuccessorRequest {
        successor_info: Some(new_node.clone().into()),
    }).await?;
    
    Ok((successor, predecessor))
}

pub async fn handle_finalize_join_ring(
    backend_manager: Arc<BackendManager>,
    new_node: NodeInfo,
    migrate_success: bool,
    fronts: &[NodeInfo],
    this: usize,
) -> FawnResult<bool> {

    // add the new node to the active backend list if the migration was successful
    if migrate_success {
        backend_manager.add_backend(new_node.clone()).await;
    }

    // notify other frontends of the new node's node info to update their backend list
    for i in 0..fronts.len() {
        if i != this {
            let addr = fronts[i].get_http_addr();
            let mut client = FawnFrontendServiceClient::connect(addr).await.map_err(|e| FawnError::RpcError(format!("Failed to connect to frontend: {}", e)))?;
            let request = Request::new(NotifyBackendJoinRequest {
                backend_info: Some(new_node.clone().into())
            });
            let response = client.notify_backend_join(request).await.map_err(|e| FawnError::RpcError(format!("Failed to notify other frontends: {}", e)))?;
            if !response.get_ref().success {
                return Err(Box::new(FawnError::SystemError("Failed to notify other frontends".to_string())));
            }
        }
    }

    Ok(true)
}

pub async fn handle_get_value(
    backend_manager: Arc<BackendManager>,
    user_key: String,
) -> FawnResult<(Vec<u8>, bool)> {
    let key_id = fawn_common::util::get_key_id(&user_key);
    let successor = backend_manager.find_successor(key_id).await
        .ok_or_else(|| Box::new(FawnError::NoBackendAvailable("no backend available".to_string())))?;
    
    // Create a gRPC client to the successor backend
    let addr = successor.get_http_addr();
    let mut client = FawnBackendServiceClient::connect(addr).await.map_err(|e| FawnError::RpcError(format!("Failed to connect to successor: {}", e)))?;
    
    // Make the gRPC call to get the value
    let request = fawn_common::fawn_backend_api::GetRequest {
        key_id,
    };
    
    let response = client.get_value(request).await.map_err(|e| FawnError::RpcError(format!("Failed to get value: {}", e)))?;
    let response = response.into_inner();
    
    Ok((response.value, response.success))
}

pub async fn handle_put_value(
    backend_manager: Arc<BackendManager>,
    user_key: String,
    value: Vec<u8>,
) -> FawnResult<bool> {
    let key_id = fawn_common::util::get_key_id(&user_key);
    let successor = backend_manager.find_successor(key_id).await
        .ok_or_else(|| Box::new(FawnError::NoBackendAvailable("no backend available".to_string())))?;
    
    // Create a gRPC client to the successor backend
    let addr = successor.get_http_addr();
    let mut client = FawnBackendServiceClient::connect(addr).await.map_err(|e| FawnError::RpcError(format!("Failed to connect to successor: {}", e)))?;
    
    // Make the gRPC call to store the value
    let request = fawn_common::fawn_backend_api::StoreRequest {
        key_id,
        value,
    };
    
    let response = client.store_value(request).await.map_err(|e| FawnError::RpcError(format!("Failed to store value: {}", e)))?;
    let response = response.into_inner();
    
    Ok(response.success)
}

pub async fn handle_notify_backend_join(
    backend_manager: Arc<BackendManager>,
    backend_info: NodeInfo,
) -> FawnResult<bool> {
    backend_manager.add_backend(backend_info).await;
    Ok(true)
}

pub async fn handle_notify_backend_leave(
    backend_manager: Arc<BackendManager>,
    backend_info: NodeInfo,
) -> FawnResult<bool> {
    backend_manager.remove_backend(backend_info.id).await;
    Ok(true)
}