use fawn_backend::meta::Meta;
use server::BackendServer;
use storage::logstore::LogStructuredStore;
use fawn_common::err::{FawnError, FawnResult};
use fawn_common::config::BackConfig;

mod storage;
mod server;
mod rpc_handler;
mod service;
mod meta;

fn main() -> FawnResult<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <config_file>", args[0]);
        std::process::exit(1);
    }
    let config = read_config(&args[1]);

    // initialize primary storage and its meta_path
    let primary_storage = LogStructuredStore::open(&config.storage_dir)
        .map_err(|e| FawnError::SystemError(format!("Failed to open storage: {}", e)))?;
    let primary_meta_path = Path::new(&config.storage_dir).join("meta.bin");

    // initialize temporary storage
    let temp_storage_dir = Path::new(&config.storage_dir).join("temp");
    let temp_storage = LogStructuredStore::open(&temp_storage_dir)
        .map_err(|e| FawnError::SystemError(format!("Failed to open temporary storage: {}", e)))?;
    let temp_meta_path = temp_storage_dir.join("meta.bin");

    // initialize stage meta with default Normal
    let stage_meta_path = Path::new(&config.storage_dir).join("stage.meta");
    // persist if the file doesn't exist
    if !stage_meta_path.exists() {
        Stage::Normal.store(&stage_meta_path).map_err(|e| FawnError::SystemError(format!("Failed to store stage meta: {}", e)))?;
    }

    let mut server = BackendServer::new(
        config.fronts, 
        config.address, 
    ).map_err(|e| FawnError::SystemError(format!("Failed to create backend server: {}", e)))?;

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        server.start(
            primary_storage, 
            primary_meta_path, 
            temp_storage, 
            temp_meta_path,
            stage_meta_path
        ).await?;
        Ok(())
    })
}

fn read_config(file_path: &str) -> BackConfig {
    let config = BackConfig::from_file(file_path).unwrap();
    config
}