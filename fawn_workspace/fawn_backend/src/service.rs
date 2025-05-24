use crate::chunked_file_store::ChunkedFileStore;

#[derive(Clone)]
pub struct BackendSvc {
    store: Arc<LogStructuredStore>,
    file_store: ChunkedFileStore,
}

impl BackendSvc {
    pub fn new(data_dir: &Path) -> anyhow::Result<Self> {
        let store = Arc::new(LogStructuredStore::open(data_dir)?);
        Ok(Self {
            file_store: ChunkedFileStore::new(store.clone()),
            store,
        })
    }
}