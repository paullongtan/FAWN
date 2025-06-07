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

    // initialize a metadata file
    let meta_path = Path::new(&config.storage_dir).join("meta.bin");
    let meta = Meta::load(&meta_path)
        .map_err(|e| FawnError::SystemError(format!("Failed to load metadata: {}", e)))?;

    // TODO: should storage knows where the last fully-acked timestamp is?
    let storage = LogStructuredStore::open(&config.storage_dir).map_err(|e| FawnError::SystemError(format!("Failed to open storage: {}", e)))?;

    let mut server = BackendServer::new(
        config.fronts, 
        config.address, 
    ).map_err(|e| FawnError::SystemError(format!("Failed to create backend server: {}", e)))?;

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        server.start(storage, meta, meta_path).await?;
        Ok(())
    })
}

fn read_config(file_path: &str) -> BackConfig {
    let config = BackConfig::from_file(file_path).unwrap();
    config
}