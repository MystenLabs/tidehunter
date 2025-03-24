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
}
