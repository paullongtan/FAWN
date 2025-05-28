use crate::storage::Storage;
use async_trait::async_trait;
use fawn_common::err::{FawnError, FawnResult};
use tokio::fs::File;
use tokio::io::{AsyncReadExt};
use sha2::{Sha256, Digest};
use std::path::Path;
use std::string;
use fawn_common::fawn_frontend_api::{GetRequest, PutRequest, RequestJoinRingRequest, fawn_frontend_service_client::FawnFrontendServiceClient};
use serde::{Serialize, Deserialize};
use tonic::Request;

#[derive(Serialize, Deserialize)]
struct FileManifest {   // need this for file reconstruction
    chunk_count: u32,   // number of chunks file will be splitting to
    file_size: u64,     // original file size in bytes
    sha256: [u8; 32],   // hash of the file content
}

const CHUNK_SIZE: usize = 1 * 1024 * 1024; // 1 MiB
pub struct FawnClient {
    base_url: String,
    frontends: Vec<String>
}

impl FawnClient {
    pub fn new(base_url: &str, frontends_addresses: &[String]) -> Self {
    Self {
        base_url: base_url.to_string(),
        frontends: frontends_addresses.to_vec(),
    }
}
    async fn put_bytes(&self, key: &str, value: Vec<u8>) -> FawnResult<()> {
        let mut frontend = FawnFrontendServiceClient::connect(self.base_url.clone()).await?;
        // Assuming you're calling a gRPC `put_value` method on the frontend
        let body = PutRequest {
            user_key: key.to_string(),
            value, // bytes
        };
        let request = Request::new(body);
        let response = frontend.put_value(request).await?;
        // let bool = response.into_inner()
        Ok(())
    }
}
// Implementation of put_file as an inherent method, not part of the trait
#[async_trait]
impl Storage for  FawnClient {
    /*
    key: is a string -- is the key 
    path: this is a file of bytes (what our value will be)
     */
    async fn put(&self, key: &str, path: &Path) -> FawnResult<()> {
        let mut file = File::open(path).await?;
        let mut buffer = vec![0; CHUNK_SIZE];
        let mut hasher = Sha256::new();

        let mut chunk_index = 0;    // tracks the index of the current chunk
        let mut total_bytes = 0;    // tracks the running total of how many bytes are read

        loop { // Reading the file in chunks
            let bytes_read = file.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }

            hasher.update(&buffer[..bytes_read]);

            // Creates a unique key: "{myfile.txt}.chunk{0}"
            let chunk_key = format!("{}.chunk{}", key, chunk_index);
            // truncates part of the buffer that was read
            let chunk_data = &buffer[..bytes_read];

            // This is where data is sent to frontend!!
            self.put_bytes(&chunk_key, chunk_data.to_vec()).await?;

            total_bytes += bytes_read as u64;
            chunk_index += 1;
        }
        // Finalizes the SHA-256 hash after the full file has been read.
        let sha256 = hasher.finalize();

        // Creates a FileManifest object containing metadata needed for future file retrieval and validation.
        let manifest = FileManifest {
            chunk_count: chunk_index,
            file_size: total_bytes,
            sha256: sha256.into(),
        };

        println!("[PUT] key: '{}', file_size: {}, number_chunks: {}", key, total_bytes, chunk_index);
        // Creates a unique key to store the manifest file: "{myfile.txt}.manifest.json"
        let manifest_key = format!("{}.manifest.json", key);
        let manifest_bytes = serde_json::to_vec(&manifest)?;

        self.put_bytes(&manifest_key, manifest_bytes).await?;

        Ok(())
    }

    async fn get(&self, key: &str) -> FawnResult<()> {
        println!("[GET] key: '{}'", key);
        // Ok(Some("mocked_value".to_string()))
        Ok(())
    }

    async fn delete(&self, key: &str) -> FawnResult<()> {
        println!("[DELETE] key: '{}'", key);
        Ok(())
    }
}
