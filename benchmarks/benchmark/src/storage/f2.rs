use crate::storage::Storage;
use faster_wrapper::F2Store;
use minibytes::Bytes;
use std::path::Path;
use std::sync::Arc;

pub struct F2Storage {
    store: Arc<F2Store>,
}

impl F2Storage {
    #[allow(dead_code)]
    pub fn open(path: &Path) -> Arc<Self> {
        std::fs::create_dir_all(path).unwrap();

        let store = F2Store::new(path).expect("Failed to create F2 store");

        Arc::new(Self {
            store: Arc::new(store),
        })
    }
}

impl Storage for F2Storage {
    fn insert(&self, k: Bytes, v: Bytes) {
        self.store.insert(k, v).unwrap_or_else(|e| {
            eprintln!("F2 insert failed: {}", e);
        });
    }

    fn get(&self, k: &[u8]) -> Option<Bytes> {
        self.store.get(k).ok()
    }

    fn get_lt(&self, _k: &[u8], _iterations: usize) -> Vec<Bytes> {
        // F2 doesn't have native support for range queries
        // This is a known limitation for this type of storage engine
        Vec::new()
    }

    fn exists(&self, k: &[u8]) -> bool {
        self.store.exists(k)
    }

    fn name(&self) -> &'static str {
        "f2"
    }
}
