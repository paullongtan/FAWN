use fawn_common::{types::{NodeInfo, KeyValue}, err::{FawnError, FawnResult}};

pub async fn handle_request_join_ring(
    new_node: NodeInfo,
) -> FawnResult<(NodeInfo, NodeInfo)> {
    todo!()
}

pub async fn handle_finalize_join_ring(
    new_node: NodeInfo, migrate_success: bool,
) -> FawnResult<bool> {
    todo!()
}

pub async fn handle_get_value(
    user_key: String,
) -> FawnResult<(Vec<u8>, bool)> {
    todo!()
}

pub async fn handle_put_value(
    user_key: String,
    value: Vec<u8>,
) -> FawnResult<bool> {
    todo!()
}