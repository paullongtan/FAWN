use tonic::{transport::Server, Request, Response, Status};
use fawn_common::config::FrontConfig;
use fawn_common::err::{FawnError, FawnResult};
use crate::server::FrontendServer;
use tokio::time::{timeout, Duration};
use env_logger::Env;

mod rpc_handler;
mod service;
mod server;
mod backend_manager;
mod types;

fn main() {
    // Initialize the logger
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <config_file>", args[0]);
        std::process::exit(1);
    }
    let config = read_config(&args[1]);
    let mut frontend = FrontendServer::new(config.fronts, config.this, 3).unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        frontend.start().await.unwrap();
    });
}

fn read_config(file_path: &str) -> FrontConfig {
    let config = FrontConfig::from_file(file_path).unwrap();
    config
}