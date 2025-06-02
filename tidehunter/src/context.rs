use crate::config::Config;
use crate::key_shape::{KeySpace, KeySpaceDesc};
use crate::metrics::Metrics;
use std::sync::Arc;

#[derive(Clone)]
pub struct KsContext {
    pub config: Arc<Config>,
    pub ks_config: KeySpaceDesc,
    pub metrics: Arc<Metrics>,
}

impl KsContext {
    pub fn name(&self) -> &str {
        self.ks_config.name()
    }

    pub fn id(&self) -> KeySpace {
        self.ks_config.id()
    }

    pub fn max_dirty_keys(&self) -> usize {
        self.ks_config
            .max_dirty_keys()
            .unwrap_or(self.config.max_dirty_keys)
    }

    pub fn excess_dirty_keys(&self, dirty_keys_count: usize) -> bool {
        dirty_keys_count > self.max_dirty_keys()
    }
}
