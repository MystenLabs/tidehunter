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
    pub metrics: Arc<Metrics>,
}

impl Storage for Arc<TidehunterStorage> {
    fn insert(&self, k: Bytes, v: Bytes) {
        self.db.insert(self.ks, k, v).unwrap()
    }

    fn get(&self, k: &[u8]) -> Option<Bytes> {
        self.db.get(self.ks, k).unwrap()
    }

    fn get_lt(&self, k: &[u8]) -> Option<Bytes> {
        let mut iterator = self.db.iterator(self.ks);
        iterator.set_upper_bound(k.to_vec());
        iterator.reverse();
        let next = iterator.next()?;
        Some(next.expect("Db error").1)
    }

    fn name() -> &'static str {
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
        let metrics = Metrics::new_in(registry);
        let db = Db::open(path, key_shape, config, metrics.clone()).unwrap();
        let this = Self { db, ks, metrics };
        Arc::new(this)
    }
}
