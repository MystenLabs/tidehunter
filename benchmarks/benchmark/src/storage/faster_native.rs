use crate::storage::Storage;
use faster_wrapper::{Config, FasterStore};
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

        // FASTER config optimized for 1TB dataset with 256GB RAM
        // Note: Using smaller sizes would require careful page alignment
        let config = Config {
            storage_path: path.to_string_lossy().to_string(),
            initial_log_size: 1 << 36, // 64GB initial size
            max_log_size: 1 << 40,     // 1TB max size
            page_size: 1 << 21,        // 2MB pages
            segment_size: 1 << 32,     // 4GB segments
            hash_table_size: 1 << 24,  // 16M buckets for better distribution
            enable_read_cache: true,
            read_cache_size: 1 << 36,  // 64GB read cache (25% of RAM)
            log_mutable_fraction: 0.9, // 90% mutable region for write-heavy workload
        };

        let store = FasterStore::new(config).expect("Failed to create FASTER store");

        Arc::new(Self {
            store: Arc::new(store),
        })
    }
}

impl Storage for FasterNativeStorage {
    fn insert(&self, k: Bytes, v: Bytes) {
        let key_vec = k.as_ref().to_vec();
        let val_vec = v.as_ref().to_vec();
        self.store.insert(key_vec, val_vec).unwrap_or_else(|e| {
            eprintln!("FASTER insert failed: {}", e);
        });
    }

    fn get(&self, k: &[u8]) -> Option<Bytes> {
        let key_vec = k.to_vec();
        self.store
            .get::<Vec<u8>, Vec<u8>>(key_vec)
            .ok()
            .flatten()
            .map(|v| Bytes::copy_from_slice(&v))
    }

    fn get_lt(&self, _k: &[u8], _iterations: usize) -> Vec<Bytes> {
        // FASTER doesn't have native support for range queries
        // This is a known limitation
        Vec::new()
    }

    fn exists(&self, k: &[u8]) -> bool {
        let key_vec = k.to_vec();
        self.store.get::<Vec<u8>, Vec<u8>>(key_vec).ok().is_some()
    }

    fn name(&self) -> &'static str {
        "faster_native"
    }
}
