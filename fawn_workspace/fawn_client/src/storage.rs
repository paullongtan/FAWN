use async_trait::async_trait;
use std::path::Path;
use fawn_common::err::FawnResult;

#[async_trait]
pub trait Storage: Send + Sync {
    async fn put(&self, key: &str, path: &Path) -> FawnResult<()>;
    async fn get(&self, key: &str) -> FawnResult<Vec<u8>>;
    async fn delete(&self, key: &str) -> FawnResult<()>;
}

