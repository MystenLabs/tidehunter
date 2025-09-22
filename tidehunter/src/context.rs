use crate::config::Config;
use crate::key_shape::{KeySpace, KeySpaceDesc};
use crate::large_table::GetResult;
use crate::metrics::{Metrics, TimerExt};
use crate::wal::WalPosition;
use prometheus::{Histogram, IntCounter, IntGauge};
use std::array;
use std::ops::Deref;
use std::sync::Arc;
use strum::{AsRefStr, EnumCount, EnumIter, FromRepr};

#[derive(Clone)]
pub struct KsContext {
    inner: Arc<KsContextInner>,
}

pub struct KsContextInner {
    pub config: Arc<Config>,
    pub ks_config: KeySpaceDesc,
    pub metrics: Arc<Metrics>,
    pub loaded_key_bytes: IntGauge,
    // Operation metrics indexed by DbOpKind
    db_op_metrics: [Histogram; DbOpKind::COUNT],
    // WAL written bytes metrics indexed by WalWriteKind
    wal_written_metrics: [IntCounter; WalWriteKind::COUNT],
    // Lookup result metrics indexed by [LookupResult][LookupSource]
    lookup_result_metrics: [[IntCounter; LookupSource::COUNT]; LookupResult::COUNT], // 2 for Found/NotFound
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

#[derive(Clone, Copy, Debug, EnumCount, AsRefStr, FromRepr)]
#[repr(usize)]
#[strum(serialize_all = "snake_case")]
pub enum LookupResult {
    Found,
    NotFound,
}

#[derive(Clone, Copy, Debug, EnumIter, EnumCount, AsRefStr, FromRepr)]
#[repr(usize)]
#[strum(serialize_all = "snake_case")]
pub enum LookupSource {
    Lru,
    Bloom,
    Cache,
    Lookup,
    Prefix,
}

impl KsContext {
    pub fn new(config: Arc<Config>, ks_config: KeySpaceDesc, metrics: Arc<Metrics>) -> Self {
        let ks_name = ks_config.name();
        let loaded_key_bytes = metrics.loaded_key_bytes.with_label_values(&[ks_name]);

        let db_op_metrics = array::from_fn(|i| {
            let op = DbOpKind::from_repr(i).expect("Invalid DbOpKind index");
            metrics.db_op_mcs.with_label_values(&[op.as_ref(), ks_name])
        });

        let wal_written_metrics = array::from_fn(|i| {
            let kind = WalWriteKind::from_repr(i).expect("Invalid WalWriteKind index");
            metrics
                .wal_written_bytes_type
                .with_label_values(&[kind.as_ref(), ks_name])
        });

        // Initialize lookup result metrics as a 2D array [result][source]
        let lookup_result_metrics = array::from_fn(|result_idx| {
            let result = LookupResult::from_repr(result_idx).expect("Invalid LookupResult index");
            array::from_fn(|source_idx| {
                let source =
                    LookupSource::from_repr(source_idx).expect("Invalid LookupSource index");
                metrics.lookup_result.with_label_values(&[
                    ks_name,
                    result.as_ref(),
                    source.as_ref(),
                ])
            })
        });

        let inner = KsContextInner {
            config,
            ks_config,
            metrics,
            loaded_key_bytes,
            db_op_metrics,
            wal_written_metrics,
            lookup_result_metrics,
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn db_op_timer(&self, op: DbOpKind) -> impl Drop {
        self.db_op_metrics[op as usize].clone().mcs_timer()
    }

    pub fn inc_wal_written(&self, kind: WalWriteKind, bytes: u64) {
        self.wal_written_metrics[kind as usize].inc_by(bytes);
    }

    pub fn inc_lookup_result(&self, result: LookupResult, source: LookupSource) {
        self.lookup_result_metrics[result as usize][source as usize].inc();
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

    pub fn report_lookup_result(&self, v: Option<WalPosition>, source: LookupSource) -> GetResult {
        let result = if v.is_some() {
            LookupResult::Found
        } else {
            LookupResult::NotFound
        };
        self.inc_lookup_result(result, source);
        match v {
            None => GetResult::NotFound,
            Some(w) => GetResult::WalPosition(w),
        }
    }
}

impl Deref for KsContext {
    type Target = KsContextInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
