pub mod storage;
pub mod service;

// Re-export commonly used types
pub use storage::{
    LogStore,
    Record,
    Segment,
    Compaction,
};

pub use service::BackendService;
