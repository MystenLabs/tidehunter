use crate::storage::Storage;
use faster_wrapper::FasterStore;
use minibytes::Bytes;
use std::path::Path;
use std::sync::Arc;

pub struct FasterNativeStorage {
    store: Arc<FasterStore>,
}

impl FasterNativeStorage {
    #[allow(dead_code)]
    pub fn open(path: &Path) -> Arc<Self> {
        std::fs::create_dir_all(path).unwrap();

        // Use the optimized configuration for large datasets (1TB data, 256GB RAM)
        let store = FasterStore::new_for_large_dataset(path)
            .expect("Failed to create FASTER store");

        Arc::new(Self {
            store: Arc::new(store),
        })
    }
}

impl Storage for FasterNativeStorage {
    fn insert(&self, k: Bytes, v: Bytes) {
        self.store.insert(k, v).unwrap_or_else(|e| {
            eprintln!("FASTER insert failed: {}", e);
        });
    }

    fn get(&self, k: &[u8]) -> Option<Bytes> {
        self.store.get(k).ok()
    }

    fn get_lt(&self, _k: &[u8], _iterations: usize) -> Vec<Bytes> {
        // FASTER doesn't have native support for range queries
        // This is a known limitation
        Vec::new()
    }

    fn exists(&self, k: &[u8]) -> bool {
        self.store.exists(k)
    }

    fn name(&self) -> &'static str {
        "faster_native"
    }
}
