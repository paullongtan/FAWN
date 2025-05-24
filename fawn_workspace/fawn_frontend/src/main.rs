use tonic::{transport::Server, Request, Response, Status};
use fawn_common::{config::FrontConfig, fawn_frontend_api::fawn_frontend_service_server::FawnFrontendService};
mod server;
mod rpc_handler;
mod backend_client;


fn main() {
    println!("Hello, world!");
}
