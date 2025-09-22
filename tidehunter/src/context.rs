use crate::config::Config;
use crate::key_shape::{KeySpace, KeySpaceDesc};
use crate::metrics::Metrics;
use prometheus::IntGauge;
use std::sync::Arc;

#[derive(Clone)]
pub struct KsContext {
    pub config: Arc<Config>,
    pub ks_config: KeySpaceDesc,
    pub metrics: Arc<Metrics>,
    pub loaded_key_bytes: IntGauge,
}

impl KsContext {
    pub fn new(config: Arc<Config>, ks_config: KeySpaceDesc, metrics: Arc<Metrics>) -> Self {
        let loaded_key_bytes = metrics
            .loaded_key_bytes
            .with_label_values(&[ks_config.name()]);
        Self {
            config,
            ks_config,
            metrics,
            loaded_key_bytes,
        }
    }

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

    /// Returns fixed key size or None if variable keys are configured for this key space.
    pub fn index_key_size(&self) -> Option<usize> {
        self.ks_config.index_key_size()
    }
}
