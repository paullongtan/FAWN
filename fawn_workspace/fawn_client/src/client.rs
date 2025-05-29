use crate::storage::Storage;
use async_trait::async_trait;
use fawn_common::err::{FawnError, FawnResult};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
        let connection = format!("http://{}", self.frontends[0].clone());
        let mut frontend = FawnFrontendServiceClient::connect(connection).await?;
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
    async fn get_bytes(&self, key: &str) -> FawnResult<Vec<u8>> {
        let connection = format!("http://{}", self.frontends[0].clone());
        let mut frontend = FawnFrontendServiceClient::connect(connection).await?;
        let body = GetRequest {
            user_key: key.to_string(),
        };
        let request = Request::new(body);
        let response = frontend.get_value(request).await?;
        Ok(response.into_inner().value) // Assuming the response has a `value` field
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

    async fn get(&self, key: &str) -> FawnResult<Vec<u8>> {
        println!("[GET] key: '{}'", key);
        
        // Retrieve manifest
        let manifest_key = format!("{}.manifest.json", key);
        let manifest_bytes = self.get_bytes(&manifest_key).await?;
        if manifest_bytes.is_empty() {
            return Err(Box::new(FawnError::KeyNotFound(key.to_string())));
        }
        let manifest: FileManifest = serde_json::from_slice(&manifest_bytes)?;
        
        println!("[GET] Retrieved manifest: {} chunks, {} bytes", manifest.chunk_count, manifest.file_size);
        
        // Reconstruct the file
        let mut file_data = Vec::with_capacity(manifest.file_size as usize);
        let mut hasher = Sha256::new();
        
        for chunk_index in 0..manifest.chunk_count {
            let chunk_key = format!("{}.chunk{}", key, chunk_index);
            let chunk_data = self.get_bytes(&chunk_key).await?;
            
            hasher.update(&chunk_data);
            file_data.extend_from_slice(&chunk_data);
        }
        
        // Validate hash
        let computed_hash = hasher.finalize();
        if computed_hash.as_slice() != manifest.sha256 {
            return Err(Box::new(FawnError::SystemError("File hash mismatch during reconstruction".to_string())));
        }
        
        // Validate size
        if file_data.len() != manifest.file_size as usize {
            return Err(Box::new(FawnError::SystemError("File size mismatch during reconstruction".to_string())));
        }
        
        // Create output directory if it doesn't exist
        let output_dir = std::path::Path::new("test/get");
        tokio::fs::create_dir_all(output_dir).await?;
        
        // Write to file in retrieved_data/
        let output_path = output_dir.join(format!("{}.bin", key));
        let mut output_file = File::create(&output_path).await?;
        output_file.write_all(&file_data).await?;
        output_file.flush().await?; // <-- This is critical
        println!("[GET] File reconstructed and saved as: {:?}", output_path);

        // Return the file data
        Ok(file_data)
    }



    async fn delete(&self, key: &str) -> FawnResult<()> {
        println!("[DELETE] key: '{}'", key);
        Ok(())
    }
}
