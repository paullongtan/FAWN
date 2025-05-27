use std::{error::Error, fmt::Display};

#[derive(Debug, Clone)]
pub enum FawnError {
    // Network errors
    ConnectionError(String),
    TimeoutError(String),
    
    // RPC errors
    RpcError(String),

    // System errors
    NoBackendAvailable(String),
    KeyNotFound(String),
    SystemError(String),
    InvalidRequest(String),
    Unknown(String),
}

// Implement Display for user-friendly messages
impl Display for FawnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let x = match self {
            FawnError::ConnectionError(x) => format!("connection error: {}", x),
            FawnError::TimeoutError(x) => format!("timeout error: {}", x),
            FawnError::KeyNotFound(x) => format!("key \"{}\" not found", x),
            FawnError::RpcError(x) => format!("rpc error: {}", x),
            FawnError::InvalidRequest(x) => format!("invalid request: {}", x),
            FawnError::SystemError(x) => format!("system error: {}", x),
            FawnError::Unknown(x) => format!("unknown error: {}", x),
            FawnError::NoBackendAvailable(x) => format!("no backend available: {}", x),
        };
        write!(f, "{}", x)
    }
}

// Implement Error trait
impl std::error::Error for FawnError {}

// Add these implementations
unsafe impl Send for FawnError {}
unsafe impl Sync for FawnError {}

// Implement From for common error types
impl From<tonic::Status> for FawnError {
    fn from(v: tonic::Status) -> Self {
        FawnError::RpcError(format!("{:?}", v))
    }
}

impl From<tonic::transport::Error> for FawnError {
    fn from(v: tonic::transport::Error) -> Self {
        FawnError::RpcError(format!("{:?}", v))
    }
}

/// A [Result] type which either returns `T` or a [boxed error](https://doc.rust-lang.org/rust-by-example/error/multiple_error_types/boxing_errors.html)
pub type FawnResult<T> = Result<T, Box<(dyn Error + Send + Sync)>>;

impl From<Box<dyn Error>> for FawnError {
    fn from(x: Box<dyn Error>) -> Self {
        FawnError::Unknown(x.to_string())
    }
}