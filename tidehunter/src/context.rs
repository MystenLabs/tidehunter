use crate::config::Config;
use crate::key_shape::{KeySpace, KeySpaceDesc};
use crate::metrics::{Metrics, TimerExt};
use prometheus::{Histogram, IntCounter, IntGauge};
use std::sync::Arc;
use strum::{AsRefStr, EnumCount, EnumIter, FromRepr, IntoEnumIterator};

#[derive(Clone)]
pub struct KsContext {
    pub config: Arc<Config>,
    pub ks_config: KeySpaceDesc,
    pub metrics: Arc<Metrics>,
    pub loaded_key_bytes: IntGauge,
    // Operation metrics indexed by DbOpKind
    db_op_metrics: Vec<Histogram>,
    // WAL written bytes metrics indexed by WalWriteKind
    wal_written_metrics: Vec<IntCounter>,
}

#[derive(Clone, Copy, Debug, EnumIter, EnumCount, AsRefStr, FromRepr)]
#[repr(usize)]
#[strum(serialize_all = "snake_case")]
pub enum DbOpKind {
    Insert,
    Remove,
    Get,
    Exists,
    NextEntry,
    NextCell,
    UpdateFlushedIndex,
}

#[derive(Clone, Copy, Debug, EnumIter, EnumCount, AsRefStr, FromRepr)]
#[repr(usize)]
#[strum(serialize_all = "snake_case")]
pub enum WalWriteKind {
    Record,
    Tombstone,
    Index,
}

impl KsContext {
    pub fn new(config: Arc<Config>, ks_config: KeySpaceDesc, metrics: Arc<Metrics>) -> Self {
        let ks_name = ks_config.name();
        let loaded_key_bytes = metrics.loaded_key_bytes.with_label_values(&[ks_name]);

        let db_op_metrics = DbOpKind::iter()
            .map(|op| metrics.db_op_mcs.with_label_values(&[op.as_ref(), ks_name]))
            .collect();

        let wal_written_metrics = WalWriteKind::iter()
            .map(|kind| {
                metrics
                    .wal_written_bytes_type
                    .with_label_values(&[kind.as_ref(), ks_name])
            })
            .collect();

        Self {
            config,
            ks_config,
            metrics,
            loaded_key_bytes,
            db_op_metrics,
            wal_written_metrics,
        }
    }

    pub fn db_op_timer(&self, op: DbOpKind) -> impl Drop {
        self.db_op_metrics[op as usize].clone().mcs_timer()
    }

    pub fn inc_wal_written(&self, kind: WalWriteKind, bytes: u64) {
        self.wal_written_metrics[kind as usize].inc_by(bytes);
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
