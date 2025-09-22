use crate::config::Config;
use crate::key_shape::{KeySpace, KeySpaceDesc};
use crate::metrics::{Metrics, TimerExt};
use prometheus::{Histogram, IntCounter, IntGauge};
use std::sync::Arc;
use strum::{EnumCount, EnumIter, IntoEnumIterator};

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

#[derive(Clone, Copy, Debug, EnumIter, EnumCount)]
#[repr(usize)]
pub enum DbOpKind {
    Insert = 0,
    Remove = 1,
    Get = 2,
    Exists = 3,
    NextEntry = 4,
    NextCell = 5,
    UpdateFlushedIndex = 6,
}

impl DbOpKind {
    fn as_str(&self) -> &'static str {
        match self {
            DbOpKind::Insert => "insert",
            DbOpKind::Remove => "remove",
            DbOpKind::Get => "get",
            DbOpKind::Exists => "exists",
            DbOpKind::NextEntry => "next_entry",
            DbOpKind::NextCell => "next_cell",
            DbOpKind::UpdateFlushedIndex => "update_flushed_index",
        }
    }
}

#[derive(Clone, Copy, Debug, EnumIter, EnumCount)]
#[repr(usize)]
pub enum WalWriteKind {
    Record = 0,
    Tombstone = 1,
    Index = 2,
}

impl WalWriteKind {
    fn as_str(&self) -> &'static str {
        match self {
            WalWriteKind::Record => "record",
            WalWriteKind::Tombstone => "tombstone",
            WalWriteKind::Index => "index",
        }
    }
}

impl KsContext {
    pub fn new(config: Arc<Config>, ks_config: KeySpaceDesc, metrics: Arc<Metrics>) -> Self {
        let ks_name = ks_config.name();
        let loaded_key_bytes = metrics.loaded_key_bytes.with_label_values(&[ks_name]);

        let mut db_op_metrics = Vec::with_capacity(DbOpKind::COUNT);
        for op in DbOpKind::iter() {
            db_op_metrics.push(metrics.db_op_mcs.with_label_values(&[op.as_str(), ks_name]));
        }

        let mut wal_written_metrics = Vec::with_capacity(WalWriteKind::COUNT);
        for kind in WalWriteKind::iter() {
            wal_written_metrics.push(
                metrics
                    .wal_written_bytes_type
                    .with_label_values(&[kind.as_str(), ks_name]),
            );
        }

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
