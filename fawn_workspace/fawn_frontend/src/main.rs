use tonic::{transport::Server, Request, Response, Status};
use fawn_common::config::FrontConfig;
use fawn_common::err::{FawnError, FawnResult};
use fawn_common::fawn_frontend_api::{fawn_frontend_service_server::FawnFrontendServiceServer, fawn_frontend_service_client::FawnFrontendServiceClient};
use crate::node::FrontendNode;
use tokio::time::{timeout, Duration};

mod rpc_handler;
mod service;
mod server;
mod backend_manager;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <config_file>", args[0]);
        std::process::exit(1);
    }
    let config = read_config(&args[1]);
    let mut frontend = FrontendNode::new(config.fronts, config.this).unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        frontend.start().await.unwrap();
    });
}

fn read_config(file_path: &str) -> FrontConfig {
    let config = FrontConfig::from_file(file_path).unwrap();
    config
}