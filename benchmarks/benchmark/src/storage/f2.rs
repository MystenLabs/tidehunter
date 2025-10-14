use crate::storage::Storage;
use faster_wrapper::{F2Config, F2Store};
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

        // Create optimized configuration for large datasets (1TB data, 256GB RAM)
        // F2 uses a two-tier architecture: hot store for frequently accessed data,
        // cold store for less active data
        let config = F2Config {
            storage_path: path.to_string_lossy().to_string(),
            hot_store_size: 1 << 37, // 128GB hot store (50% of RAM for active data)
            cold_store_size: 1 << 40, // 1TB cold store (full dataset capacity)
            index_size: 1 << 24,     // 16M buckets for better hash distribution
            read_cache_size: 1 << 36, // 64GB read cache (25% of RAM)
            hot_threshold: 0.8,      // Move to cold when hot store is 80% full
            cold_threshold: 0.1,     // Promote to hot at 10% access frequency
            segment_size: 1 << 32,   // 4GB segments for better I/O efficiency
        };

        let store = F2Store::new(config).expect("Failed to create F2 store");

        Arc::new(Self {
            store: Arc::new(store),
        })
    }
}

impl Storage for F2Storage {
    fn insert(&self, k: Bytes, v: Bytes) {
        let key_vec = k.as_ref().to_vec();
        let val_vec = v.as_ref().to_vec();
        self.store.insert(key_vec, val_vec).unwrap_or_else(|e| {
            eprintln!("F2 insert failed: {}", e);
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
        // F2 doesn't have native support for range queries
        // This is a known limitation for this type of storage engine
        Vec::new()
    }

    fn exists(&self, k: &[u8]) -> bool {
        let key_vec = k.to_vec();
        self.store.get::<Vec<u8>, Vec<u8>>(key_vec).ok().is_some()
    }

    fn name(&self) -> &'static str {
        "f2"
    }
}
