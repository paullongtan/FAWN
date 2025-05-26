use std::sync::Arc;
use fawn_common::types::{NodeInfo, KeyValue};
use fawn_common::err::{FawnError, FawnResult};
use fawn_common::util::get_key_id;
use fawn_common::fawn_backend_api::fawn_backend_service_client::FawnBackendServiceClient;
use crate::backend_manager::BackendManager;

pub async fn handle_request_join_ring(
    backend_manager: Arc<BackendManager>,
    new_node: NodeInfo,
) -> FawnResult<(NodeInfo, NodeInfo)> {
    let backends = backend_manager.get_active_backends().await;
    // Implementation logic here
    todo!()
}

pub async fn handle_finalize_join_ring(
    backend_manager: Arc<BackendManager>,
    new_node: NodeInfo,
    migrate_success: bool,
) -> FawnResult<bool> {
    if migrate_success {
        backend_manager.add_backend(new_node).await;
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
    // Implementation logic here
    todo!()
}

pub async fn handle_put_value(
    backend_manager: Arc<BackendManager>,
    user_key: String,
    value: Vec<u8>,
) -> FawnResult<bool> {
    let key_id = fawn_common::util::get_key_id(&user_key);
    let successor = backend_manager.find_successor(key_id).await
        .ok_or_else(|| Box::new(FawnError::NoBackendAvailable("no backend available".to_string())))?;
    // Implementation logic here
    todo!()
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