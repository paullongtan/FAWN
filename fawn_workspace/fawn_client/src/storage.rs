use async_trait::async_trait;

#[async_trait]
pub trait Storage: Send + Sync {
    async fn put(&self, key: &str, value: &str) -> Result<(), Box<dyn std::error::Error>>;
    async fn get(&self, key: &str) -> Result<Option<String>, Box<dyn std::error::Error>>;
    async fn delete(&self, key: &str) -> Result<(), Box<dyn std::error::Error>>;
}
