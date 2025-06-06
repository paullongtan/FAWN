use std::sync::Arc;
use fawn_common::types::NodeInfo;
use fawn_common::err::{FawnError, FawnResult};
use fawn_common::util::get_key_id;
use fawn_common::fawn_backend_api::fawn_backend_service_client::FawnBackendServiceClient;
use crate::backend_manager::BackendManager;
use fawn_common::fawn_frontend_api::fawn_frontend_service_client::FawnFrontendServiceClient;
use tonic::Request;
use fawn_common::fawn_frontend_api::NotifyBackendJoinRequest;
use tonic::Status;

pub async fn handle_request_join_ring(
    backend_manager: Arc<BackendManager>,
    new_node: NodeInfo,
) -> FawnResult<(NodeInfo, NodeInfo)> {
    let successor = backend_manager.find_successor(new_node.id).await;
    let predecessor = backend_manager.find_predecessor(new_node.id).await;

    if successor.is_none() || predecessor.is_none() {
        return Ok((new_node.clone(), new_node.clone()));
    }

    let successor = successor.unwrap();
    let predecessor = predecessor.unwrap();

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

    println!("Backend {} joined the ring successfully", new_node.get_http_addr());

    Ok(true)
}

pub async fn handle_get_value(
    backend_manager: Arc<BackendManager>,
    user_key: String,
) -> Result<Vec<u8>, Status> {
    let key_id = fawn_common::util::get_key_id(&user_key);
    let chain_members = backend_manager.get_chain_members(key_id).await;
    let tail_node = chain_members.last().unwrap();
    
    let addr = tail_node.get_http_addr();
    let mut client = FawnBackendServiceClient::connect(addr).await
        .map_err(|e| Status::unavailable(format!("Failed to connect to successor: {}", e)))?;
    
    let request = fawn_common::fawn_backend_api::GetRequest {
        key_id,
    };
    
    let response = client.get_value(request).await
        .map_err(|e| Status::internal(format!("Failed to get value: {}", e)))?;
    
    Ok(response.into_inner().value)
}

pub async fn handle_put_value(
    backend_manager: Arc<BackendManager>,
    user_key: String,
    value: Vec<u8>,
) -> Result<(), Status> {
    let key_id = fawn_common::util::get_key_id(&user_key);
    let chain_members = backend_manager.get_chain_members(key_id).await;
    let head_node = chain_members.first().unwrap();
    
    let addr = head_node.get_http_addr();
    let mut client = FawnBackendServiceClient::connect(addr).await
        .map_err(|e| Status::unavailable(format!("Failed to connect to successor: {}", e)))?;
    
    let pass_count = (chain_members.len() - 1) as u32;
    let request = fawn_common::fawn_backend_api::StoreRequest {
        key_id,
        value,
        pass_count,
    };
    
    client.store_value(request).await
        .map_err(|e| Status::internal(format!("Failed to store value: {}", e)))?;
    
    Ok(())
}

pub async fn handle_notify_backend_join(
    backend_manager: Arc<BackendManager>,
    backend_info: NodeInfo,
) {
    backend_manager.add_backend(backend_info).await;
}

pub async fn handle_notify_backend_leave(
    backend_manager: Arc<BackendManager>,
    backend_info: NodeInfo,
) {
    backend_manager.remove_backend(backend_info.id).await;
}