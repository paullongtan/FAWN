use crate::client::FawnClient;
use crate::storage::Storage;

pub async fn new_client(addr: &str) -> Result<Box<dyn Storage>, Box<dyn std::error::Error>> {
    Ok(Box::new(FawnClient::new(addr)))
}
