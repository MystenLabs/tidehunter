use crate::storage::Storage;
use minibytes::Bytes;
use prometheus::Registry;
use std::path::Path;
use std::sync::Arc;
use tidehunter::config::Config;
use tidehunter::db::Db;
use tidehunter::key_shape::{KeyShape, KeySpace};
use tidehunter::metrics::Metrics;

pub struct TidehunterStorage {
    pub db: Arc<Db>,
    ks: KeySpace,
}

impl Storage for Arc<TidehunterStorage> {
    fn insert(&self, k: Bytes, v: Bytes) {
        self.db.insert(self.ks, k, v).unwrap()
    }

    fn get(&self, k: &[u8]) -> Option<Bytes> {
        self.db.get(self.ks, k).unwrap()
    }

    fn get_lt(&self, k: &[u8], iterations: usize) -> Vec<Bytes> {
        let mut iterator = self.db.iterator(self.ks);
        iterator.set_upper_bound(k.to_vec());
        iterator.reverse();
        let mut result = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            if let Some(next) = iterator.next() {
                result.push(next.expect("Db error").1);
            } else {
                break;
            }
        }
        result
    }

    fn exists(&self, k: &[u8]) -> bool {
        self.db.exists(self.ks, k).unwrap()
    }

    fn name(&self) -> &'static str {
        "tidehunter"
    }
}

impl TidehunterStorage {
    pub fn open(
        registry: &Registry,
        config: Config,
        path: &Path,
        (key_shape, ks): (KeyShape, KeySpace),
    ) -> Arc<Self> {
        let config = Arc::new(config);
        let metrics = Metrics::from_config_registry(registry, &config);
        let db = Db::open(path, key_shape, config, metrics).unwrap();
        let this = Self { db, ks };
        Arc::new(this)
    }
}
