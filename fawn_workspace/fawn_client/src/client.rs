use crate::storage::Storage;
use async_trait::async_trait;

pub struct FawnClient {
    base_url: String,
}

impl FawnClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.to_string(),
        }
    }
}

#[async_trait]
impl Storage for FawnClient {
    async fn put(&self, key: &str, value: &str) -> Result<(), Box<dyn std::error::Error>> {
        println!("[PUT] key: '{}', value: '{}'", key, value);
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<String>, Box<dyn std::error::Error>> {
        println!("[GET] key: '{}'", key);
        Ok(Some("mocked_value".to_string()))
    }

    async fn delete(&self, key: &str) -> Result<(), Box<dyn std::error::Error>> {
        println!("[DELETE] key: '{}'", key);
        Ok(())
    }
}
