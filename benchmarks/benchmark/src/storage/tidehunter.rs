use crate::storage::Storage;
use minibytes::Bytes;
use prometheus::Registry;
use std::path::Path;
use std::sync::Arc;
use tidehunter::config::Config;
use tidehunter::db::Db;
use tidehunter::key_shape::{KeyShape, KeySpace, KeyType};
use tidehunter::metrics::Metrics;

pub struct TidehunterStorage {
    pub db: Arc<Db>,
    ks: KeySpace,
    pub metrics: Arc<Metrics>,
}

impl Storage for Arc<TidehunterStorage> {
    fn insert(&self, k: Bytes, v: Bytes) {
        self.db.insert(self.ks, k, v).unwrap()
    }

    fn get(&self, k: &[u8]) -> Option<Bytes> {
        self.db.get(self.ks, k).unwrap()
    }
}

impl TidehunterStorage {
    pub fn open(config: Config, path: &Path) -> Arc<Self> {
        let config = Arc::new(config);
        let registry = Registry::new();
        let metrics = Metrics::new_in(&registry);
        crate::prometheus::start_prometheus_server("127.0.0.1:9092".parse().unwrap(), &registry);
        let (key_shape, ks) = KeyShape::new_single(32, 1024, KeyType::uniform(32));
        let db = Db::open(path, key_shape, config, metrics.clone()).unwrap();
        let this = Self { db, ks, metrics };
        Arc::new(this)
    }
}
