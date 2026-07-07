use crate::batch::{PendingOp, RelocatedWriteBatch, WriteBatch};
use crate::cell::CellId;
use crate::checkpoint::DbCheckpoint;
use crate::compressed_batch::{
    BatchCodec, CompressedBatch, decompress_wal_entry, find_record, find_record_by,
};
use crate::config::Config;
use crate::context::{DbOpKind, KsContext, KsContextVec, ReadType, WalEntryKind};
use crate::control::{ControlRegion, ControlRegionStore, RelocateFiles};
use crate::crc::IntoBytesFixed;
use crate::flusher::IndexFlusher;
use crate::index::index_format::{IndexFormat, IndexIterCaches};
use crate::index::index_table::IndexTable;
use crate::index::levels::IndexLevels;
use crate::iterators::IteratorResult;
use crate::iterators::db_iterator::{DbIterator, IterationSource};
use crate::key_shape::{KeyShape, KeySpace, KeySpaceDesc, KeySpaces, KeyType};
use crate::large_table::{GetResult, LargeTable, Loader, PendingBatchOp};
use crate::lock::DbLock;
use crate::metrics::{Metrics, TimerExt};
use crate::relocation::updates::RelocationUpdates;
use crate::relocation::{RelocationCommand, RelocationDriver, RelocationStrategy, Relocator};
use crate::state_snapshot;
use crate::wal::layout::WalKind;
use crate::wal::position::{LastProcessed, WalFileId, WalPosition};
use crate::wal::tracker::WalGuard;
use crate::wal::{PreparedWalWrite, Wal, WalError, WalRandomRead, WalWriter};
use bytes::{Buf, BufMut, BytesMut};
use minibytes::Bytes;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::io;
use std::io::Write;
use std::iter;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, Weak, mpsc};
use std::thread;
use std::time::{Duration, Instant};

pub struct Db {
    pub(crate) large_table: LargeTable,
    pub(crate) wal: Arc<Wal>,
    pub(crate) indexes: Arc<Wal>,
    pub(crate) wal_writer: WalWriter,
    pub(crate) index_writer: WalWriter,
    pub(crate) control_region_store: Mutex<ControlRegionStore>,
    pub(crate) config: Arc<Config>,
    metrics: Arc<Metrics>,
    pub(crate) key_shape: KeyShape,
    /// Canonical name → handle map for the keyspaces the caller declared;
    /// resolved via [`Db::ks`] / [`Db::try_ks`] / [`Db::single_ks`].
    keyspaces: KeySpaces,
    relocator: Relocator,
    commit_pool: Option<rayon::ThreadPool>,
    /// One handle per pending-promotion shard, for waking threads after batch commits.
    /// Index `i` owns mutex shards where `mutex_idx % len == i`.
    pending_promotion_threads: Box<[OnceLock<thread::Thread>]>,
    _lock: Mutex<Option<DbLock>>,
}

pub type DbResult<T> = Result<T, DbError>;

pub const MAX_KEY_LEN: usize = u16::MAX as usize;
pub const CONTROL_REGION_FILE: &str = "cr";

impl Db {
    pub fn open(
        path: &Path,
        key_shape: KeyShape,
        config: Arc<Config>,
        metrics: Arc<Metrics>,
    ) -> DbResult<Arc<Self>> {
        config.validate();
        let path = path.canonicalize()?;
        let lock = DbLock::acquire(&path, config.open_lock_retry_timeout)?;
        let (key_shape, keyspaces, registry_dirty) = Self::reconcile_key_shape(&path, key_shape)?;
        Self::maybe_create_config_file(&path, &config)?;
        let wal = Wal::open(&path, config.wal_layout(WalKind::Replay), metrics.clone())?;
        let indexes = Wal::open(&path, config.wal_layout(WalKind::Index), metrics.clone())?;
        let (control_region_store, control_region) =
            Self::read_or_create_control_region(path.join(CONTROL_REGION_FILE), &key_shape)?;
        if registry_dirty {
            // Persisted only after the control region verified the canonical
            // shape, so a bad declaration never overwrites the registry.
            // No WAL write can happen before `open` returns, so the registry
            // is always durable before the first write that uses a new id.
            Self::write_shape_file(&path, &key_shape)?;
        }

        // Create channels for flusher threads first
        let (flusher_senders, flusher_receivers) = (0..config.num_flusher_threads)
            .map(|_| mpsc::channel())
            .unzip();
        let (relocator_sender, relocator_receiver) = mpsc::channel();

        let flusher = IndexFlusher::new(flusher_senders, metrics.clone());
        let relocator = Relocator(relocator_sender);
        let large_table = LargeTable::from_unloaded(
            &key_shape,
            control_region.snapshot(),
            config.clone(),
            flusher,
            metrics.clone(),
            indexes.as_ref(),
        );
        let contexts = KsContextVec::new(&key_shape, config.clone(), metrics.clone());
        let wal_iterator = wal.wal_iterator_for_writer(control_region.last_position())?;
        let wal_writer = crate::wal_replay::replay_wal(
            &contexts,
            &large_table,
            &key_shape,
            wal_iterator,
            &metrics,
        )?;
        let last_index_position = control_region.last_index_wal_position();
        let index_writer = indexes.writer_after(last_index_position)?;
        let control_region_store = Mutex::new(control_region_store);

        let commit_pool = if config.commit_pool_size > 0 {
            Some(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(config.commit_pool_size)
                    .thread_name(|idx| format!("commit-pool-{}", idx))
                    .build()
                    .expect("Failed to create commit thread pool"),
            )
        } else {
            None
        };

        let num_promotion_threads = config.num_pending_promotion_threads.max(1);
        let pending_promotion_threads = (0..num_promotion_threads)
            .map(|_| OnceLock::new())
            .collect::<Box<[_]>>();

        let this = Self {
            large_table,
            wal_writer,
            index_writer,
            wal,
            indexes,
            control_region_store,
            config,
            metrics: metrics.clone(),
            key_shape,
            keyspaces,
            relocator,
            commit_pool,
            pending_promotion_threads,
            _lock: Mutex::new(Some(lock)),
        };
        this.report_memory_estimates();
        let this = Arc::new(this);

        // Now start the flusher threads with the weak reference
        let weak_db = Arc::downgrade(&this);
        let _handles = IndexFlusher::start_threads(flusher_receivers, weak_db, metrics.clone());

        // Start the relocator with the weak reference
        let _handle = RelocationDriver::start(
            Arc::downgrade(&this),
            path,
            relocator_receiver,
            metrics.clone(),
        );

        // Start the event-driven pending-promotion threads (one per shard) and the flat-promotion
        // polling thread.
        let num_shards = this.pending_promotion_threads.len();
        let _pending_handles: Vec<_> = (0..num_shards)
            .map(|shard_idx| {
                Self::start_pending_promotion_job(Arc::downgrade(&this), shard_idx, num_shards)
            })
            .collect();
        let _flat_handle = Self::start_flat_promotion_job(Arc::downgrade(&this));

        // todo: store handles and wait for them on Db drop

        Ok(this)
    }

    /// Deletes the database directory at `path` if no other process holds the lock.
    /// Returns `ErrorKind::AlreadyExists` if the database is currently open.
    pub fn drop_db(path: &Path) -> io::Result<()> {
        let _lock = DbLock::acquire(path, Duration::ZERO)?;
        std::fs::remove_dir_all(path)
    }

    /// Path of the canonical keyspace registry: the shape in canonical id
    /// order, written during `Db::open` — before any WAL write can happen —
    /// at creation and whenever a keyspace is added or removed. The entry
    /// order in this file defines the keyspace ids used in WAL bytes and
    /// the control region.
    pub(crate) fn shape_v2_file_path(path: &Path) -> PathBuf {
        path.join("shape_v2.yaml")
    }

    /// Path of the legacy shape file. Written once at creation by older
    /// versions and never updated, so it may lack keyspaces added later.
    /// Read-only fallback for databases that predate `shape_v2.yaml`.
    fn shape_file_path(path: &Path) -> PathBuf {
        path.join("shape.yaml")
    }

    /// Establishes the canonical keyspace order for this database and the set
    /// of keyspaces exposed to the caller.
    ///
    /// The canonical order is the order keyspaces were first created in,
    /// recorded in the `shape_v2.yaml` registry. The declared `KeyShape` may
    /// list the registry's keyspaces — matched by name — in any order; new
    /// keyspaces are appended. A keyspace in the registry but absent from the
    /// declared shape is **retained** as a live keyspace internally (data
    /// preserved, never destroyed) but is simply not exposed — re-declaring
    /// its name later reconnects to the same id with its data intact (this
    /// mirrors RocksDB, which never drops a column family implicitly).
    ///
    /// Returns the canonical-ordered shape (the full set of keyspaces, used
    /// for everything internal: WAL bytes, index, control region), the
    /// [`KeySpaces`] map exposing the declared keyspaces by name (returned by
    /// `Db::open`), and whether the registry needs to be (re)written. The
    /// caller persists the registry only after the control region verified
    /// the canonical shape, so a bad declaration never overwrites it; no WAL
    /// write can happen before `open` returns, so the registry is always
    /// durable before the first write that uses a new id.
    ///
    /// When the registry does not exist, the declared order itself is
    /// canonical: databases written before the registry existed allowed
    /// appending keyspaces only, so their declared order always matches the
    /// ids in the WAL. Reordering keyspaces of such a database requires
    /// opening it once with the original shape first (which creates the
    /// registry).
    fn reconcile_key_shape(
        path: &Path,
        declared: KeyShape,
    ) -> DbResult<(KeyShape, KeySpaces, bool)> {
        let registry_path = Self::shape_v2_file_path(path);
        // Only the name order is taken from the stored shape; runtime
        // configuration (compactors, bloom filters, etc.) comes from the
        // declared shape for exposed keyspaces and from the registry for
        // retained (undeclared) ones.
        let registry = if registry_path.exists() {
            Some(Self::load_shape_from_file(&registry_path)?)
        } else {
            None
        };
        let (key_shape, keyspaces, changed) = declared.reorder_to_canonical(registry.as_ref());
        let registry_dirty = registry.is_none() || changed;
        Ok((key_shape, keyspaces, registry_dirty))
    }

    /// Atomically writes the canonical shape to `shape_v2.yaml`
    /// (write to a temp file, fsync, rename).
    fn write_shape_file(path: &Path, key_shape: &KeyShape) -> DbResult<()> {
        let shape_file_path = Self::shape_v2_file_path(path);
        let yaml = key_shape.to_yaml().map_err(|e| {
            DbError::Io(io::Error::other(format!(
                "Failed to serialize key shape: {e}"
            )))
        })?;
        let temp_file = shape_file_path.with_extension("yaml.tmp");
        {
            let mut file = std::fs::File::create(&temp_file)?;
            file.write_all(yaml.as_bytes())?;
            file.sync_all()?;
        }
        std::fs::rename(&temp_file, &shape_file_path)?;
        Ok(())
    }

    /// Load key shape from database directory.
    /// Prefers the canonical `shape_v2.yaml`, falling back to the legacy
    /// `shape.yaml` for databases that predate it.
    #[doc(hidden)] // Used by tools/tideconsole for loading database schema
    pub fn load_key_shape(path: &Path) -> DbResult<KeyShape> {
        let v2_path = Self::shape_v2_file_path(path);
        if v2_path.exists() {
            return Self::load_shape_from_file(&v2_path);
        }
        let legacy_path = Self::shape_file_path(path);
        if legacy_path.exists() {
            return Self::load_shape_from_file(&legacy_path);
        }
        Err(DbError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "Key shape file not found at {} or {}",
                v2_path.display(),
                legacy_path.display()
            ),
        )))
    }

    fn load_shape_from_file(shape_file_path: &Path) -> DbResult<KeyShape> {
        let yaml = std::fs::read_to_string(shape_file_path)?;
        KeyShape::from_yaml(&yaml).map_err(|e| {
            DbError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to deserialize key shape: {e}"),
            ))
        })
    }

    pub fn config_file_path(path: &Path) -> PathBuf {
        path.join("config.yaml")
    }

    /// Create config file if it doesn't exist
    fn maybe_create_config_file(path: &Path, config: &Config) -> DbResult<()> {
        let config_file_path = Self::config_file_path(path);
        if !config_file_path.exists() {
            let yaml = serde_yaml::to_string(config).map_err(|e| {
                DbError::Io(io::Error::other(format!("Failed to serialize config: {e}")))
            })?;
            std::fs::write(&config_file_path, yaml)?;
        }
        Ok(())
    }

    /// Load config from database directory
    #[doc(hidden)] // Used by tools/tideconsole for loading database config
    pub fn load_config(path: &Path) -> DbResult<Config> {
        let config_file_path = Self::config_file_path(path);
        if !config_file_path.exists() {
            return Err(DbError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Config file not found at {}", config_file_path.display()),
            )));
        }

        let yaml = std::fs::read_to_string(&config_file_path)?;
        serde_yaml::from_str(&yaml).map_err(|e| {
            DbError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to deserialize config: {e}"),
            ))
        })
    }

    pub fn start_periodic_snapshot(self: &Arc<Self>) {
        // todo account number of bytes read during wal replay
        let position = self.wal_writer.position();
        let weak = Arc::downgrade(self);
        thread::Builder::new()
            .name("snapshot".to_string())
            .spawn(move || Self::periodic_snapshot_thread(weak, position))
            .unwrap();
    }

    fn periodic_snapshot_thread(weak: Weak<Db>, mut position: u64) -> Option<()> {
        let start = Instant::now();
        let mut last_snapshot = Duration::ZERO;
        const SNAPSHOT_EVERY_SECS: u64 = 3600;
        loop {
            // Check if database is still alive periodically (every second) to allow faster shutdown
            for _ in 0..60 {
                if weak.strong_count() == 0 {
                    return None;
                }
                thread::sleep(Duration::from_secs(1));
            }

            let db = weak.upgrade()?;
            db.large_table.report_entries_state();
            // todo when we get to wal position wrapping around this will need to be fixed
            let current_wal_position = db.wal_writer.position();
            let written = current_wal_position.checked_sub(position).unwrap();
            let now = start.elapsed();
            let timed_snapshot =
                if now.saturating_sub(last_snapshot).as_secs() >= SNAPSHOT_EVERY_SECS {
                    last_snapshot = now;
                    true
                } else {
                    false
                };
            if timed_snapshot || written > db.config.snapshot_written_bytes() {
                // todo taint storage instance on failure?
                let snapshot_position = db
                    .rebuild_control_region()
                    .expect("Failed to rebuild control region");
                // snapshot_position is now a u64 offset
                position = snapshot_position;
            }
        }
    }

    fn read_or_create_control_region(
        path: PathBuf,
        key_shape: &KeyShape,
    ) -> Result<(ControlRegionStore, ControlRegion), DbError> {
        let control_region = ControlRegion::read_or_create(&path, key_shape);
        let control_region_store = ControlRegionStore::new(path, &control_region);
        Ok((control_region_store, control_region))
    }

    pub fn insert(&self, ks: KeySpace, k: impl Into<Bytes>, v: impl Into<Bytes>) -> DbResult<()> {
        let context = self.ks_context(ks);
        let _timer = context.db_op_timer(DbOpKind::Insert);
        let k = k.into();
        let v = v.into();
        context.ks_config.check_key(&k);
        let w = PreparedWalWrite::new(&WalEntry::Record(context.id(), k.clone(), v.clone(), false));
        context.inc_wal_written(WalEntryKind::Record, w.len() as u64);
        let guard = self.wal_writer.write(&w)?;
        self.metrics
            .wal_written_bytes
            .set(guard.wal_position().offset() as i64);
        let full_key = k.clone();
        let reduced_key = context.ks_config.reduced_key_bytes(k);
        self.large_table
            .insert(context, reduced_key, full_key, guard, &v, self)?;
        Ok(())
    }

    pub fn remove(&self, ks: KeySpace, k: impl Into<Bytes>) -> DbResult<()> {
        let context = self.ks_context(ks);
        let _timer = context.db_op_timer(DbOpKind::Remove);
        let k = k.into();
        context.ks_config.check_key(&k);
        let w = PreparedWalWrite::new(&WalEntry::Remove(context.id(), k.clone()));
        context.inc_wal_written(WalEntryKind::Tombstone, w.len() as u64);
        let guard = self.wal_writer.write(&w)?;
        let reduced_key = context.ks_config.reduced_key_bytes(k);
        self.large_table.remove(context, reduced_key, guard, self)
    }

    pub fn get(&self, ks: KeySpace, k: &[u8]) -> DbResult<Option<Bytes>> {
        let context = self.ks_context(ks);
        let _timer = context.db_op_timer(DbOpKind::Get);
        let reduced_key = context.ks_config.reduce_key(k);
        match self.large_table.get(context, reduced_key.as_ref(), self)? {
            GetResult::Value(full_key, value) => {
                if context.ks_config.need_check_index_key() && full_key.as_ref() != k {
                    return Ok(None);
                }
                Ok(Some(value))
            }
            GetResult::WalPosition(w) => {
                let value = self.read_record_check_key(context, k, w)?;
                let Some(value) = value else {
                    return Ok(None);
                };
                self.large_table.update_lru(
                    context,
                    reduced_key.to_vec().into(),
                    Bytes::from(k.to_vec()),
                    value.clone(),
                );
                Ok(Some(value))
            }
            GetResult::NotFound => Ok(None),
        }
    }

    /// Opens a checkpoint: a stable, point-in-time read view of the database as
    /// of the current WAL frontier.
    ///
    /// Acquiring the checkpoint takes a [`WalTrackerLatch`], which (a) blocks
    /// until every WAL position written before this call is recorded in the
    /// in-memory index, and (b) pins the externally observed `last_processed`
    /// at that frontier for as long as the returned [`DbCheckpoint`] is held.
    /// Writes that land after this call sit at or above the frontier and stay
    /// in the index overlay instead of being promoted into flat, so
    /// [`DbCheckpoint::get`] can filter them out and keep returning the value
    /// as of the checkpoint even as the live database advances.
    ///
    /// The checkpoint pins the index frontier but does not itself hold WAL
    /// files from being reclaimed, so it is intended for short-lived reads.
    ///
    /// Returned as an `Arc` so it can back a [`DbCheckpoint::iterator`] (which
    /// needs `Arc<Self>` to keep the latch alive for the iterator's lifetime).
    pub fn checkpoint(self: &Arc<Self>) -> Arc<DbCheckpoint> {
        Arc::new(DbCheckpoint::new(self.clone(), self.wal_writer.latch()))
    }

    pub fn exists(&self, ks: KeySpace, k: &[u8]) -> DbResult<bool> {
        let context = self.ks_context(ks);
        let _timer = context.db_op_timer(DbOpKind::Exists);
        let reduced_key = context.ks_config.reduce_key(k);
        match self.large_table.get(context, reduced_key.as_ref(), self)? {
            GetResult::Value(full_key, _) => {
                Ok(!context.ks_config.need_check_index_key() || full_key.as_ref() == k)
            }
            GetResult::WalPosition(w) => {
                if context.ks_config.need_check_index_key() {
                    // With key reduction an index hit may be a different key that
                    // reduces to the same index key, so read the record to compare.
                    Ok(self.read_record_check_key(context, k, w)?.is_some())
                } else {
                    Ok(true)
                }
            }
            GetResult::NotFound => Ok(false),
        }
    }

    /// Drops all cells in the specified key range for the given key space.
    ///
    /// This method removes all data from cells that contain keys in the range
    /// `[from_inclusive, to_inclusive]`.
    ///
    /// This method requires that cell range provided covers full range of cells,
    /// it is not possible to drop part of the cell with this method.
    ///
    /// # Arguments
    ///
    /// * `ks` - The key space to operate on
    /// * `from_inclusive` - The first key in the range (must be the first key in its cell)
    /// * `to_inclusive` - The last key in the range (must be the last key in its cell)
    ///
    /// # Panics
    ///
    /// This method panics if:
    /// - `from_inclusive` is not the first key in its cell
    /// - `to_inclusive` is not the last key in its cell
    /// - The key space does not support fixed-length keys
    ///
    /// # Behavior by Key Type
    ///
    /// - **Uniform key type** (Array-based storage): Entries are cleared but remain in the fixed-size array
    /// - **PrefixedUniform key type** (Tree-based storage): Entries are completely removed from the large table
    pub fn drop_cells_in_range(
        &self,
        ks: KeySpace,
        from_inclusive: &[u8],
        to_inclusive: &[u8],
    ) -> DbResult<()> {
        let ksd = self.key_shape.ks(ks);

        // Convert keys from user format to index format
        let from_reduced = ksd.reduce_key(from_inclusive);
        let to_reduced = ksd.reduce_key(to_inclusive);

        // Validate keys and get cell range
        let (from_cell, to_cell) = ksd.map_key_range_to_cell_range(&from_reduced, &to_reduced);

        let entry = WalEntry::DropCells(ks, from_cell.clone(), to_cell.clone());
        let w = PreparedWalWrite::new(&entry);
        self.wal_writer.write(&w)?;

        let context = self.ks_context(ks);
        self.large_table
            .drop_cells_in_range(context, &from_cell, &to_cell);

        Ok(())
    }

    pub fn write_batch(self: &Arc<Db>) -> WriteBatch {
        WriteBatch::new(Arc::clone(self))
    }

    pub(crate) fn do_write_batch(&self, batch: WriteBatch) -> DbResult<()> {
        let WriteBatch {
            transaction,
            mut writes,
            tag,
            mut pending_ops,
            ..
        } = batch;
        if writes.is_empty() {
            return Ok(());
        }
        let _timer = self
            .metrics
            .db_op_mcs
            .with_label_values(&["write_batch", &tag])
            .mcs_timer();
        let _write_timer = self
            .metrics
            .write_batch_times
            .with_label_values(&[&tag, "write"])
            .mcs_timer();

        // Either write one compressed frame whose position is shared by every
        // entry in the batch, or write per-record frames. Choice is a config
        // knob; default is the per-record path.
        let (guards, positions) = if let Some(codec) = self.config.batch_codec {
            // Position sharing means the in-memory index can have at most
            // one op per (ks, reduced_key) — `IndexTable::checked_insert`
            // requires strictly increasing positions per key, so two ops
            // on the same key in one batch would panic. Collapse to
            // last-wins both in the WAL frame and in the pending ops.
            dedup_last_wins(&mut writes, &mut pending_ops);
            self.write_compressed_batch_into_wal(&writes, codec)?
        } else {
            self.write_batch_into_wal(&writes)?
        };

        // Group pending_ops by (ks_id, mutex_idx) so each mutex is acquired only once.
        let mut grouped: Vec<(
            usize, /*ks_id*/
            usize, /*mutex_idx*/
            CellId,
            PendingBatchOp,
            WalPosition,
        )> = pending_ops
            .iter()
            .zip(positions.iter())
            .map(|(op, &pos)| {
                let ks = op.ks();
                let cell_id = op.cell_id().clone();
                let context = self.ks_context(ks);
                let mutex_idx = context.ks_config.mutex_for_cell(&cell_id);
                let batch_op = match op {
                    PendingOp::Insert {
                        reduced_key,
                        lru_update,
                        ..
                    } => PendingBatchOp::Insert {
                        reduced_key: reduced_key.clone(),
                        lru_update: lru_update.clone(),
                    },
                    PendingOp::Remove { reduced_key, .. } => PendingBatchOp::Remove {
                        reduced_key: reduced_key.clone(),
                    },
                };
                (ks.as_usize(), mutex_idx, cell_id, batch_op, pos)
            })
            .collect();
        // Stable sort preserves WAL-position order within the same mutex group.
        grouped.sort_by_key(|(ks_id, mutex_idx, _, _, _)| (*ks_id, *mutex_idx));

        // Compute group boundaries: contiguous ranges with the same (ks_id, mutex_idx).
        let mut group_ranges: Vec<(usize, usize)> = Vec::new();
        {
            let mut i = 0;
            while i < grouped.len() {
                let (ks_id_0, mutex_idx_0, _, _, _) = grouped[i];
                let j = i + grouped[i..].partition_point(|(ks_id, mutex_idx, _, _, _)| {
                    *ks_id == ks_id_0 && *mutex_idx == mutex_idx_0
                });
                group_ranges.push((i, j));
                i = j;
            }
        }

        // Apply all pending operations to the large table with known WAL positions.
        // Each group shares one mutex shard and is processed under a single lock.
        let apply_group = |start: usize, end: usize| {
            let group = &grouped[start..end];
            let ks = KeySpace(group[0].0 as u8);
            let mutex_idx = group[0].1;
            let context = self.ks_context(ks);
            let ops: Vec<(CellId, &PendingBatchOp, WalPosition)> = group
                .iter()
                .map(|(_, _, cell, op, pos)| (cell.clone(), op, *pos))
                .collect();
            self.large_table
                .apply_pending_batch(context, mutex_idx, &ops, &transaction);
        };

        if let Some(pool) = &self.commit_pool {
            use rayon::prelude::*;
            pool.install(|| {
                group_ranges
                    .par_iter()
                    .with_min_len(1)
                    .for_each(|&(start, end)| {
                        apply_group(start, end);
                    });
            });
        } else {
            for (start, end) in group_ranges {
                apply_group(start, end);
            }
        }

        let mut num_inserts = 0;
        let mut num_deletes = 0;
        for write in writes.iter() {
            match write {
                WalEntry::Record(..) => num_inserts += 1,
                WalEntry::Remove(..) => num_deletes += 1,
                _ => unreachable!("WriteBatch only produces Record/Remove"),
            }
        }
        if self.config.batch_codec.is_some() {
            // Compressed-batch path: one frame covers many keyspaces, so we
            // credit it under `CompressedBatch` against the first-touched
            // keyspace. No per-record decode is required.
            if let (Some(first), Some(pos)) = (writes.first(), positions.first()) {
                let context = self.ks_context(batch_entry_ks(first));
                context.inc_wal_written(WalEntryKind::CompressedBatch, pos.frame_len() as u64);
            }
        } else {
            // Default path: per-record framing. Credit each entry's WAL bytes
            // to its own keyspace under the appropriate `WalEntryKind`.
            for (write, pos) in writes.iter().zip(positions.iter()) {
                let context = self.ks_context(batch_entry_ks(write));
                context.inc_wal_written(batch_entry_wal_kind(write), pos.frame_len() as u64);
            }
        }

        drop(_write_timer);
        self.metrics
            .write_batch_operations
            .with_label_values(&[&tag, "put"])
            .inc_by(num_inserts as u64);
        self.metrics
            .write_batch_operations
            .with_label_values(&[&tag, "delete"])
            .inc_by(num_deletes as u64);
        let _commit_timer = self
            .metrics
            .write_batch_times
            .with_label_values(&[&tag, "commit"])
            .mcs_timer();

        // keep all guards active until transaction is committed
        transaction.commit();
        let num_shards = self.pending_promotion_threads.len();
        // Wake only the promotion shard(s) that own the mutexes touched by this batch.
        let mut shards_to_wake = vec![false; num_shards];
        for (_, mutex_idx, _, _, _) in &grouped {
            shards_to_wake[mutex_idx % num_shards] = true;
        }
        for (shard_idx, should_wake) in shards_to_wake.iter().enumerate() {
            if *should_wake && let Some(t) = self.pending_promotion_threads[shard_idx].get() {
                t.unpark();
            }
        }

        self.metrics.wal_written_bytes.set(
            guards
                .last()
                .expect("Guards can't be empty")
                .wal_position()
                .offset() as i64,
        );
        // Keep batch WAL guards alive until pending writes are committed.
        drop(guards);

        Ok(())
    }

    pub(crate) fn write_relocated_batch(&self, batch: RelocatedWriteBatch) -> DbResult<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let RelocatedWriteBatch {
            prepared_writes,
            keys,
            ks,
            cell_id,
            last_processed,
            size_bytes,
        } = batch;
        let context = self.ks_context(ks);

        self.metrics
            .relocation_written_bytes
            .inc_by(size_bytes as u64);
        let positions = self.write_relocated_batch_into_wal(&prepared_writes)?;
        let mut updates = RelocationUpdates::new(last_processed);
        for (pos, key) in positions.into_iter().zip(keys.into_iter()) {
            let reduced_key = context.ks_config.reduced_key_bytes(key);
            updates.add(reduced_key, pos);
        }
        // todo this do not hold guards before flushing the index - need to confirm if this is ok
        self.large_table
            .sync_flush_for_relocation(context, &cell_id, self, Some(updates), None)?;
        Ok(())
    }

    /// Per-record batch write (default path): one `BatchStart(N)` marker
    /// followed by N `Record`/`Remove` frames. Returns all guards (N+1, with
    /// the marker first) and the N positions that should be installed into
    /// the large-table index — one per entry.
    fn write_batch_into_wal(
        &self,
        entries: &[WalEntry],
    ) -> DbResult<(Vec<WalGuard>, Vec<WalPosition>)> {
        let batch_start_entry = PreparedWalWrite::new(&WalEntry::BatchStart(entries.len() as u32));
        let prepared: Vec<PreparedWalWrite> = entries.iter().map(PreparedWalWrite::new).collect();
        let guards = self
            .wal_writer
            .multi_write(iter::once(&batch_start_entry).chain(prepared.iter()))?;
        let positions = guards[1..].iter().map(|g| *g.wal_position()).collect();
        Ok((guards, positions))
    }

    /// Serialise and compress all inner entries into a single
    /// `WalEntry::CompressedBatch` frame. Returns the single WAL guard and a
    /// `positions` vector with one entry per input — every entry shares the
    /// frame's `WalPosition`.
    fn write_compressed_batch_into_wal(
        &self,
        entries: &[WalEntry],
        codec: BatchCodec,
    ) -> DbResult<(Vec<WalGuard>, Vec<WalPosition>)> {
        let compressed = CompressedBatch::encode(entries, codec);
        let entry = WalEntry::CompressedBatch(
            compressed.codec,
            compressed.uncompressed_len,
            compressed.body,
        );
        let prepared = PreparedWalWrite::new(&entry);
        let guard = self.wal_writer.write(&prepared)?;
        let position = *guard.wal_position();
        let positions = vec![position; entries.len()];
        Ok((vec![guard], positions))
    }

    fn write_relocated_batch_into_wal(
        &self,
        prepared_writes: &[PreparedWalWrite],
    ) -> DbResult<Vec<WalPosition>> {
        // todo make sure its ok to drop guard right away
        prepared_writes
            .iter()
            .map(|write| {
                self.wal_writer
                    .write(write)
                    .map_err(DbError::from)
                    .map(|g| *g.wal_position())
            })
            .collect()
    }

    /// Ordered iterator over DB in the specified range
    pub fn iterator(self: &Arc<Self>, ks: KeySpace) -> DbIterator {
        DbIterator::new(IterationSource::Db(self.clone()), ks)
    }

    /// Returns true if this storage is empty.
    ///
    /// (warn) Right now it returns true if storage was never inserted true,
    /// but may return false if entry was inserted and then deleted.
    pub fn is_empty(&self) -> bool {
        self.large_table.is_empty()
    }

    /// Canonical handle for the key space named `name`, as declared in the
    /// `KeyShape` passed to [`Db::open`]. Panics if no such key space was
    /// declared. Use the returned handle in all database operations.
    pub fn ks(&self, name: &str) -> KeySpace {
        self.keyspaces.ks(name)
    }

    /// Canonical handle for `name`, or `None` if it was not declared.
    pub fn try_ks(&self, name: &str) -> Option<KeySpace> {
        self.keyspaces.try_ks(name)
    }

    /// Canonical handle for the sole declared key space. Panics unless exactly
    /// one was declared; convenience for single-keyspace databases (those
    /// built with [`KeyShape::new_single`]).
    pub fn single_ks(&self) -> KeySpace {
        self.keyspaces.single()
    }

    /// Names of all declared key spaces, in arbitrary order.
    pub fn ks_names(&self) -> impl Iterator<Item = &str> {
        self.keyspaces.names()
    }

    pub fn ks_name(&self, ks: KeySpace) -> &str {
        self.key_shape.ks(ks).name()
    }

    pub(crate) fn ks_desc(&self, ks: KeySpace) -> &KeySpaceDesc {
        self.key_shape.ks(ks)
    }

    pub(crate) fn ks_context(&self, ks: KeySpace) -> &KsContext {
        self.large_table.ks_context(ks)
    }

    /// Returns the next entry in the database, after the specified previous key.
    /// Iterator must specify the cell to inspect and the (Optional) previous key.
    ///
    /// If the prev_key is set to None, the first key in the cell is returned.
    ///
    /// When iterating the entire DB, the iterator starts with cell=0 and prev_key=None.
    ///
    /// The returned values:
    /// (1) the key fetched by the iterator
    /// (2) the value fetched by the iterator
    ///
    /// This function allows concurrent modification of the database.
    /// The function will return the key-value pair next after the deleted key.
    pub(crate) fn next_entry(
        &self,
        ks: KeySpace,
        cell: CellId,
        prev_key: Option<Bytes>,
        end_cell_exclusive: &Option<CellId>,
        reverse: bool,
        cache: &mut IndexIterCaches,
    ) -> DbResult<Option<IteratorResult<Bytes>>> {
        let context = self.ks_context(ks);
        let _timer = context.db_op_timer(DbOpKind::NextEntry);
        let Some(result) = self.large_table.next_entry(
            context,
            cell,
            prev_key,
            self,
            end_cell_exclusive,
            reverse,
            cache,
            None,
        )?
        else {
            return Ok(None);
        };
        let (key, value) = match result.value {
            GetResult::Value(ref full_key, ref v) => (full_key.clone(), v.clone()),
            GetResult::WalPosition(w) => {
                let Some((k, v)) =
                    self.read_record_for_indexed_key(context, w, result.key.as_ref())?
                else {
                    return Ok(None);
                };
                self.large_table
                    .update_lru(context, result.key.clone(), k.clone(), v.clone());
                (k, v)
            }
            GetResult::NotFound => unreachable!(),
        };
        Ok(Some(result.with_key_value(key, value)))
    }

    /// Returns the next cell in the large table
    pub(crate) fn next_cell(
        &self,
        ks: &KeySpaceDesc, // todo pass context instead of KeySpaceDesc here
        cell: &CellId,
        reverse: bool,
    ) -> Option<CellId> {
        let context = self.ks_context(ks.id());
        let _timer = context.db_op_timer(DbOpKind::NextCell);
        self.large_table.next_cell(context, cell, reverse)
    }

    pub(crate) fn update_flushed_index(
        &self,
        ks: KeySpace,
        cell: CellId,
        original_index: Arc<IndexTable>,
        new_levels: IndexLevels,
    ) {
        let context = self.ks_context(ks);
        let _timer = context.db_op_timer(DbOpKind::UpdateFlushedIndex);
        self.large_table
            .update_flushed_index(context, &cell, original_index, new_levels)
    }

    pub(crate) fn update_relocated_index(
        &self,
        ks: KeySpace,
        cell: CellId,
        new_levels: IndexLevels,
    ) {
        let context = self.ks_context(ks);
        self.large_table
            .update_relocated_index(context, &cell, new_levels);
    }

    pub(crate) fn read_record_check_key(
        &self,
        context: &KsContext,
        k: &[u8],
        position: WalPosition,
    ) -> DbResult<Option<Bytes>> {
        if !context.ks_config.need_check_index_key() && self.config.batch_codec.is_none() {
            // Optimization only possible if an index key always matches a record key
            // AND the position is guaranteed to point at a single `Record` frame
            // (not a `CompressedBatch` whose payload length could accidentally
            // match `record_len(k, &[])`).
            if position.payload_len() == WalEntry::record_len(k, &[]) {
                // We can see that wal position only holds the key and the value is empty,
                // we don't need disk read from the wal.
                // See test_empty_value_read_optimization
                // todo this optimization can be enabled for iterators as well
                return Ok(Some(Bytes::new()));
            }
        }
        let Some(entry) = self.read_report_entry(&self.wal, position)? else {
            return Ok(None);
        };
        match entry {
            WalEntry::Record(_, wal_key, v, _) => {
                if wal_key.as_ref() != k {
                    Ok(None)
                } else {
                    Ok(Some(v))
                }
            }
            entry @ WalEntry::CompressedBatch(..) => {
                let _timer = context.read_decompress_mcs.clone().mcs_timer();
                let decompressed = decompress_wal_entry(entry).expect("matched above");
                let result = find_record(&decompressed, context.ks_config.id(), k);
                context.read_decompress_count.inc();
                Ok(result)
            }
            other => panic!("Unexpected wal entry where expected record/batch: {other:?}"),
        }
    }

    /// Read the record matching `indexed_key` at `position`. `indexed_key` is
    /// what the large-table index stores — for keyspaces without reduction it
    /// equals the full key; for reducing keyspaces it is the result of
    /// `ks_config.reduce_key(full_key)`.
    ///
    /// Returns `Some((full_key, value))` if a matching record is found, or
    /// `None` if the position is unreachable (GC'd) or the inner batch no
    /// longer contains a record for this indexed key (e.g. it was a tombstone).
    pub(crate) fn read_record_for_indexed_key(
        &self,
        context: &KsContext,
        position: WalPosition,
        indexed_key: &[u8],
    ) -> DbResult<Option<(Bytes, Bytes)>> {
        let Some(entry) = self.read_report_entry(&self.wal, position)? else {
            return Ok(None);
        };
        match entry {
            WalEntry::Record(KeySpace(_), k, v, _relocated) => Ok(Some((k, v))),
            entry @ WalEntry::CompressedBatch(..) => {
                let _timer = context.read_decompress_mcs.clone().mcs_timer();
                let decompressed = decompress_wal_entry(entry).expect("matched above");
                let result = find_record_by(&decompressed, context.ks_config.id(), |full_key| {
                    context.ks_config.reduce_key(full_key).as_ref() == indexed_key
                });
                context.read_decompress_count.inc();
                Ok(result)
            }
            other => panic!("Unexpected wal entry where expected record/batch: {other:?}"),
        }
    }

    fn report_read(&self, entry: &WalEntry, read_type: ReadType) {
        let (kind, ks) = match entry {
            WalEntry::Record(ks, ..) => (WalEntryKind::Record, *ks),
            WalEntry::Index(ks, _) => (WalEntryKind::Index, *ks),
            WalEntry::Remove(ks, _) => (WalEntryKind::Tombstone, *ks),
            WalEntry::BatchStart(_) => return,
            WalEntry::DropCells(_, _, _) => return, // No metrics for DropCells
            // A compressed batch may serve many keys across multiple keyspaces;
            // per-keyspace attribution would require decompressing the body,
            // which would defeat the metric being lightweight. Skip for the sketch.
            WalEntry::CompressedBatch(_, _, _) => return,
        };
        let context = self.large_table.ks_context(ks);
        context.inc_read(kind, read_type);
        context.inc_read_bytes(kind, read_type, entry.len() as u64);
    }

    pub fn rebuild_control_region(&self) -> DbResult<u64> {
        let current_wal_position = self.wal_writer.position();
        let threshold_position =
            current_wal_position.saturating_sub(self.config.snapshot_unload_threshold);
        self.rebuild_control_region_from(threshold_position)
    }

    pub fn force_rebuild_control_region(&self) -> DbResult<u64> {
        // Wait for the async WAL tracker to catch up so all recent writes are included in the snapshot.
        self.wal_writer.wal_tracker_barrier();
        self.rebuild_control_region_from(self.wal_writer.position())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn is_all_clean(&self) -> bool {
        self.large_table.is_all_clean()
    }

    pub(crate) fn rebuild_control_region_from(&self, threshold_position: u64) -> DbResult<u64> {
        // Capture index writer position before pass 1 so files created by the
        // pass-1 flushes (and pass-2 force-relocations) are excluded from the
        // relocation candidate set built from the accumulator.
        let pre_snapshot_index_pos = self.index_writer.position();
        let layout = self.indexes.layout().clone();

        // Pass 1: queue async flushes for dirty entries past threshold and accumulate
        // per-file live bytes from current on-disk levels — no IO under cell locks.
        let alive_bytes = self
            .large_table
            .flush_and_accumulate(self, &layout, threshold_position);
        // Wait for pass 1 flushes to complete before computing relocation candidates
        // and starting pass 2.
        self.large_table.flusher.barrier();

        let relocate_files = RelocateFiles::from_accumulator(
            alive_bytes,
            layout,
            self.config.index_min_occupancy_pct,
            pre_snapshot_index_pos,
        );

        // Pass 2: queue force-relocations for entries in low-occupancy files,
        // wait for them, then build the snapshot.
        let mut crs = self.control_region_store.lock();
        let _timer = self
            .metrics
            .rebuild_control_region_time_mcs
            .clone()
            .mcs_timer();
        let _snapshot_timer = self.metrics.snapshot_lock_time_mcs.clone().mcs_timer();
        let snapshot = self
            .large_table
            .relocate_and_snapshot(self, &relocate_files);

        // Sparse GC: an open index file is reclaimable iff it has no live
        // position in `snapshot.data` AND sits strictly below both safety
        // bounds:
        //
        //   - `pre_snapshot_file`: a file written during this rebuild may
        //     hold a freshly-flushed blob whose updated `IndexLevels` post-
        //     dates `snapshot.data` — the new valpos lives only in in-memory
        //     state, so deleting that file would orphan it.
        //   - `max_live_file`: on restart the writer rewinds to
        //     `next_after(max(live_valpos))` and the mapper's
        //     INITIAL_MAPS_BUFFER lookahead would immediately panic trying
        //     to mmap a deleted file in `[F_max_live, F_writer]`.
        let layout = self.indexes.layout();
        let pre_snapshot_file = layout.locate_file(pre_snapshot_index_pos);
        let mut live_files: HashSet<WalFileId> = HashSet::new();
        let mut max_live_file: Option<WalFileId> = None;
        for pos in snapshot.data.iter_valid_val_positions() {
            let file_id = layout.locate_file(pos.offset());
            live_files.insert(file_id);
            max_live_file = Some(max_live_file.map_or(file_id, |m| m.max(file_id)));
        }
        let to_delete: Vec<WalFileId> = match max_live_file {
            Some(max_live_file) => {
                let cutoff = pre_snapshot_file.min(max_live_file);
                self.indexes
                    .file_ids()
                    .into_iter()
                    .filter(|id| *id < cutoff && !live_files.contains(id))
                    .collect()
            }
            // Empty snapshot — restart starts the writer at offset 0, so we
            // must keep every file the writer might traverse.
            None => Vec::new(),
        };
        self.indexes.fsync()?;
        self.wal.fsync()?;
        // Persist the control region BEFORE unlinking files. A crash between
        // these steps must leave the on-disk CR consistent with the files
        // still present: extra (un-deleted) files are tolerated by the next
        // snapshot; a CR that references a file we already deleted is not.
        crs.store(
            snapshot.data,
            snapshot.replay_from,
            &self.key_shape,
            &self.metrics,
        );
        self.index_writer.delete_files(to_delete)?;
        Ok(snapshot.replay_from)
    }

    fn write_index(&self, ks: KeySpace, index: &IndexTable) -> DbResult<WalPosition> {
        let context = self.ks_context(ks);
        self.metrics
            .flush_count
            .with_label_values(&[context.name()])
            .inc();
        self.metrics
            .flushed_keys
            .with_label_values(&[context.name()])
            .inc_by(index.len() as u64);
        let index = context
            .ks_config
            .index_format()
            .serialize_index(index, &context.ks_config);
        self.metrics
            .flushed_bytes
            .with_label_values(&[context.name()])
            .inc_by(index.len() as u64);
        let w = PreparedWalWrite::new(&WalEntry::Index(ks, index));
        context.inc_wal_written(WalEntryKind::Index, w.len() as u64);
        Ok(*self.index_writer.write(&w)?.wal_position())
    }

    fn read_entry(wal: &Wal, position: WalPosition) -> DbResult<(ReadType, Option<WalEntry>)> {
        let (read_type, entry) = wal.read(position)?;
        Ok((read_type, entry.map(WalEntry::from_bytes)))
    }

    fn read_report_entry(&self, wal: &Wal, position: WalPosition) -> DbResult<Option<WalEntry>> {
        let (read_type, entry) = Self::read_entry(wal, position)?;
        if let Some(ref entry) = entry {
            self.report_read(entry, read_type);
        }
        Ok(entry)
    }

    fn read_index(ks: &KeySpaceDesc, entry: WalEntry) -> DbResult<IndexTable> {
        if let WalEntry::Index(_, bytes) = entry {
            let entry = ks.index_format().deserialize_index(ks, bytes);
            Ok(entry)
        } else {
            panic!("Unexpected wal entry where expected index");
        }
    }

    fn report_memory_estimates(&self) {
        for ks in self.key_shape.iter_ks() {
            match ks.key_type() {
                KeyType::Uniform(config) => {
                    let num_cells = config.num_cells(ks);
                    // todo - provide better way to see memory usage for tables with var key length
                    let cache_estimate = (ks.index_key_size().unwrap_or(64)
                        + size_of::<WalPosition>())
                        * num_cells
                        * self.config.max_dirty_keys;
                    self.metrics
                        .memory_estimate
                        .with_label_values(&[ks.name(), "index_cache"])
                        .set(cache_estimate as i64);
                    if let Some(bloom_filter) = ks.bloom_filter() {
                        // Bits needed for a classic bloom filter at false-positive rate p
                        // holding n items: m = -n * ln(p) / (ln 2)^2. The on-disk layout
                        // is now fastbloom's block bloom, which sizes similarly; this
                        // estimate is for a Prometheus metric so the approximation is fine.
                        let n = bloom_filter.count as f64;
                        let p = bloom_filter.rate as f64;
                        let bits = (-n * p.ln() / (std::f64::consts::LN_2 * std::f64::consts::LN_2))
                            .ceil() as usize;
                        let bloom_size = bits / 8;
                        let bloom_estimate = bloom_size * num_cells;
                        self.metrics
                            .memory_estimate
                            .with_label_values(&[ks.name(), "bloom"])
                            .set(bloom_estimate as i64);
                    }
                }
                KeyType::PrefixedUniform(_) => {
                    // todo report actual values for memory usage since we can't have estimates here
                }
            }
        }
        let maps_estimate = (self.config.max_maps as u64) * self.config.frag_size;
        self.metrics
            .memory_estimate
            .with_label_values(&["_", "maps"])
            .set(maps_estimate as i64);
    }

    /// Starts an event-driven thread for one pending-promotion shard.
    /// The thread parks itself and is unparked by `do_write_batch` when a batch touches any mutex
    /// in this shard, and by `Drop` when the Db is torn down.
    fn start_pending_promotion_job(
        db: Weak<Db>,
        shard_idx: usize,
        num_shards: usize,
    ) -> thread::JoinHandle<()> {
        thread::Builder::new()
            .name(format!("pending-promotion-{shard_idx}"))
            .spawn(move || {
                let Some(initial) = db.upgrade() else { return };
                // OnceLock::set can only fail if set twice; this thread runs once so it always succeeds.
                initial.pending_promotion_threads[shard_idx]
                    .set(thread::current())
                    .ok();
                drop(initial);
                loop {
                    thread::park();
                    let Some(db) = db.upgrade() else { break };
                    let _timer = db
                        .metrics
                        .pending_promotion_job_time_mcs
                        .clone()
                        .mcs_timer();
                    if let Err(e) =
                        db.large_table
                            .promote_dirty_pending(db.as_ref(), shard_idx, num_shards)
                    {
                        eprintln!("Error in pending promotion job: {:?}", e);
                    }
                }
            })
            .unwrap()
    }

    /// Starts a background thread that compacts BTreeMap index entries into flat arrays.
    /// Runs on a 10-second polling cadence, independently of the pending-promotion thread.
    fn start_flat_promotion_job(db: Weak<Db>) -> thread::JoinHandle<()> {
        thread::Builder::new()
            .name("flat-promotion".to_string())
            .spawn(move || {
                loop {
                    thread::sleep(Duration::from_secs(10));
                    let Some(db) = db.upgrade() else { break };
                    let _timer = db.metrics.flat_promotion_job_time_mcs.clone().mcs_timer();
                    if let Err(e) = db.large_table.promote_flat_job(db.as_ref()) {
                        eprintln!("Error in flat promotion job: {:?}", e);
                    }
                }
            })
            .unwrap()
    }

    /// Create a snapshot of the current db state
    pub fn create_state_snapshot(&self, snapshot_path: PathBuf) -> DbResult<()> {
        let guard = self.control_region_store.lock();
        let control_region_path = guard.path().clone();
        drop(guard);
        let wal_position = self.wal_writer.position();
        state_snapshot::create(&wal_position, &control_region_path, snapshot_path)
    }

    /// Restore the database from a snapshot
    pub fn restore_state_snapshot(
        snapshot_path: PathBuf,
        database_path: PathBuf,
        key_shape: KeyShape,
        config: Arc<Config>,
        metrics: Arc<Metrics>,
    ) -> DbResult<Arc<Self>> {
        state_snapshot::load(snapshot_path, database_path, key_shape, config, metrics)
    }

    pub fn start_relocation(&self) -> Result<(), mpsc::SendError<RelocationCommand>> {
        self.start_relocation_with_strategy(self.config.relocation_strategy)
    }

    pub fn start_relocation_with_strategy(
        &self,
        strategy: RelocationStrategy,
    ) -> Result<(), mpsc::SendError<RelocationCommand>> {
        self.relocator.0.send(RelocationCommand::Start(strategy))
    }

    #[cfg(test)]
    pub fn start_blocking_relocation(&self) {
        self.start_blocking_relocation_with_strategy(self.config.relocation_strategy)
    }

    pub fn start_blocking_relocation_with_strategy(&self, strategy: RelocationStrategy) {
        let (sender, receiver) = mpsc::channel();
        self.relocator
            .0
            .send(RelocationCommand::StartBlocking(strategy, sender))
            .unwrap();
        receiver.recv().unwrap();
    }

    /// Wait for all background threads to finish by polling until no strong references remain.
    ///
    /// The caller is responsible to make sure there is no more alive Arc references to the Db held by the user.
    ///
    /// This method panics after certain period to avoid hanging forever.
    pub fn wait_for_background_threads_to_finish(self: Arc<Self>) {
        use std::thread;
        use std::time::Duration;
        #[cfg(test)]
        const POLL_LIMIT: usize = 10_000;
        #[cfg(not(test))]
        const POLL_LIMIT: usize = 100_000;

        // Take the lock out before releasing the Arc. This ensures the registry removal
        // happens exactly once, here in the calling thread, after all background threads
        // have finished — not racing with DbLock::drop in a background thread.
        let lock = self._lock.lock().take();

        let weak_db = Arc::downgrade(&self);
        drop(self);

        let mut poll_count = 0usize;
        loop {
            if weak_db.upgrade().is_none() {
                break;
            }
            poll_count += 1;
            if poll_count > POLL_LIMIT {
                panic!(
                    "Database shutdown timeout: background threads not terminating after {poll_count} polls"
                );
            }
            thread::sleep(Duration::from_millis(10));
        }

        // Drop the lock now that all background threads are done.
        // DbLock::drop removes from the registry and releases the fcntl lock.
        drop(lock);
    }

    /// Test utility accessor methods
    #[cfg(feature = "test-utils")]
    pub fn test_get_metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    #[cfg(feature = "test-utils")]
    pub fn test_get_key_shape(&self) -> &KeyShape {
        &self.key_shape
    }

    #[cfg(feature = "test-utils")]
    pub fn test_get_large_table(&self) -> &LargeTable {
        &self.large_table
    }

    #[cfg(feature = "test-utils")]
    pub fn test_get_replay_from(&self) -> u64 {
        let path = self.control_region_store.lock().path().clone();
        let cr = ControlRegion::read_or_create(&path, &self.key_shape);
        cr.last_position()
    }

    /// Drains every cell's BTreeMap into its flat buffer, bypassing
    /// `PROMOTE_THRESHOLD`. Lets concurrent tests reliably hit the
    /// `insert → promote → remove → FlushLoaded` window that triggered
    /// the `clean_self` stale-record bug.
    #[cfg(feature = "test-utils")]
    pub fn test_promote_flat_force(&self) {
        self.large_table.test_promote_flat_force(self)
    }
}

/// Inner keyspace for `WriteBatch`-produced entries. Only `Record`/`Remove`
/// can appear in a batch, so this is total over that domain.
fn batch_entry_ks(e: &WalEntry) -> KeySpace {
    match e {
        WalEntry::Record(ks, ..) | WalEntry::Remove(ks, _) => *ks,
        _ => unreachable!("WriteBatch only produces Record/Remove"),
    }
}

/// Metric kind for a `WriteBatch`-produced entry.
fn batch_entry_wal_kind(e: &WalEntry) -> WalEntryKind {
    match e {
        WalEntry::Record(..) => WalEntryKind::Record,
        WalEntry::Remove(..) => WalEntryKind::Tombstone,
        _ => unreachable!("WriteBatch only produces Record/Remove"),
    }
}

/// Collapse `(writes, pending_ops)` to last-wins per `(ks, reduced_key)`.
/// `writes[i]` and `pending_ops[i]` describe the same logical operation, so
/// they're filtered in lockstep. Required when all entries in the batch share
/// one `WalPosition`: the in-memory index cannot hold two ops on the same key
/// at the same position.
fn dedup_last_wins(writes: &mut Vec<WalEntry>, pending_ops: &mut Vec<PendingOp>) {
    debug_assert_eq!(writes.len(), pending_ops.len());
    if writes.len() < 2 {
        return;
    }
    let mut last_idx: HashMap<(KeySpace, Bytes), usize> = HashMap::with_capacity(pending_ops.len());
    for (i, op) in pending_ops.iter().enumerate() {
        last_idx.insert((op.ks(), op.reduced_key().clone()), i);
    }
    if last_idx.len() == pending_ops.len() {
        return; // no duplicates
    }
    let keep: HashSet<usize> = last_idx.into_values().collect();
    let mut i = 0;
    writes.retain(|_| {
        let k = keep.contains(&i);
        i += 1;
        k
    });
    let mut j = 0;
    pending_ops.retain(|_| {
        let k = keep.contains(&j);
        j += 1;
        k
    });
}

impl Drop for Db {
    fn drop(&mut self) {
        for slot in self.pending_promotion_threads.iter() {
            if let Some(t) = slot.get() {
                t.unpark();
            }
        }
        // Note: we intentionally do NOT send Cancel to the relocation driver here.
        // When this drop runs, the Relocator (Sender) field will be dropped as part
        // of automatic field destruction, disconnecting the channel. The driver's
        // `receiver.recv()` will return Err and the run loop exits cleanly.
        //
        // Sending Cancel and blocking on the ack would deadlock if the relocation
        // driver held the last Arc<Db>: Db::drop would fire inside the driver thread,
        // which is also the channel reader — so nobody would process the Cancel.
    }
}

impl Loader for Wal {
    type Error = DbError;

    fn load(&self, ks: &KeySpaceDesc, position: WalPosition) -> DbResult<IndexTable> {
        let (_, entry) = Db::read_entry(self, position)?;
        Db::read_index(
            ks,
            entry.unwrap_or_else(|| {
                panic!(
                    "Trying to read file from position {} but the file was deleted",
                    position.offset()
                )
            }),
        )
    }

    fn index_reader(&self, position: WalPosition) -> Result<WalRandomRead, Self::Error> {
        Ok(self.random_reader_at(position, WalEntry::INDEX_PREFIX_SIZE)?)
    }

    fn flush_supported(&self) -> bool {
        false
    }

    fn flush(&self, _ks: KeySpace, _data: &IndexTable) -> DbResult<WalPosition> {
        unimplemented!()
    }

    fn last_processed_wal_position(&self) -> LastProcessed {
        unimplemented!()
    }

    fn current_wal_position(&self) -> u64 {
        unimplemented!()
    }

    fn min_wal_position(&self) -> u64 {
        self.min_wal_position()
    }
}

impl Loader for Db {
    type Error = DbError;

    fn load(&self, ks: &KeySpaceDesc, position: WalPosition) -> DbResult<IndexTable> {
        let entry = self.read_report_entry(&self.indexes, position)?;
        Self::read_index(
            ks,
            entry.unwrap_or_else(|| {
                panic!(
                    "Trying to read file from position {} but the file was deleted",
                    position.offset()
                )
            }),
        )
    }

    fn index_reader(&self, position: WalPosition) -> Result<WalRandomRead, Self::Error> {
        Ok(self
            .indexes
            .random_reader_at(position, WalEntry::INDEX_PREFIX_SIZE)?)
    }

    fn flush_supported(&self) -> bool {
        true
    }

    fn flush(&self, ks: KeySpace, data: &IndexTable) -> DbResult<WalPosition> {
        self.write_index(ks, data)
    }

    fn last_processed_wal_position(&self) -> LastProcessed {
        self.wal_writer.last_processed()
    }

    fn current_wal_position(&self) -> u64 {
        self.wal_writer.position()
    }

    fn min_wal_position(&self) -> u64 {
        self.wal.min_wal_position()
    }
}

#[doc(hidden)] // Used by tools/tideconsole for WAL inspection
#[derive(Debug)]
pub enum WalEntry {
    Record(
        KeySpace,
        Bytes,
        Bytes,
        bool, /* true for relocated record */
    ),
    Index(KeySpace, Bytes),
    Remove(KeySpace, Bytes),
    BatchStart(u32),
    DropCells(KeySpace, CellId, CellId),
    /// A compressed `WriteBatch`: one WAL frame whose payload, when decompressed,
    /// is a sequence of `Record`/`Remove` inner entries (see `compressed_batch.rs`).
    /// All large-table entries produced by a batch share the same `WalPosition`
    /// — the position of this frame. Point lookups linear-scan the inner entries
    /// by key. Tombstones are never point-read.
    CompressedBatch(
        BatchCodec,
        u32,   /* uncompressed_len */
        Bytes, /* body */
    ),
}

#[derive(Debug)]
pub enum DbError {
    Io(io::Error),
    CrCorrupted,
    WalError(WalError),
    CorruptedIndexEntry(bincode::Error),
}

impl WalEntry {
    const WAL_ENTRY_RECORD: u8 = 1;
    const WAL_ENTRY_INDEX: u8 = 2;
    const WAL_ENTRY_REMOVE: u8 = 3;
    const WAL_ENTRY_BATCH_START: u8 = 4;
    const WAL_ENTRY_RELOCATED_RECORD: u8 = 5;
    const WAL_ENTRY_DROP_CELLS: u8 = 6;
    const WAL_ENTRY_COMPRESSED_BATCH: u8 = 7;
    pub const INDEX_PREFIX_SIZE: usize = 2;

    pub fn from_bytes(bytes: Bytes) -> Self {
        let mut b = &bytes[..];
        let entry_type = b.get_u8();
        match entry_type {
            WalEntry::WAL_ENTRY_RECORD | WalEntry::WAL_ENTRY_RELOCATED_RECORD => {
                let ks = KeySpace(b.get_u8());
                let key_len = b.get_u16() as usize;
                let k = bytes.slice(4..4 + key_len);
                let v = bytes.slice(4 + key_len..);
                let relocated = entry_type == WalEntry::WAL_ENTRY_RELOCATED_RECORD;
                WalEntry::Record(ks, k, v, relocated)
            }
            WalEntry::WAL_ENTRY_INDEX => {
                let ks = KeySpace(b.get_u8());
                WalEntry::Index(ks, bytes.slice(2..))
            }
            WalEntry::WAL_ENTRY_REMOVE => {
                let ks = KeySpace(b.get_u8());
                WalEntry::Remove(ks, bytes.slice(2..))
            }
            WalEntry::WAL_ENTRY_BATCH_START => WalEntry::BatchStart(b.get_u32()),
            WalEntry::WAL_ENTRY_DROP_CELLS => {
                let ks = KeySpace(b.get_u8());
                let from_cell = CellId::from_bytes(&mut b);
                let to_cell = CellId::from_bytes(&mut b);
                WalEntry::DropCells(ks, from_cell, to_cell)
            }
            WalEntry::WAL_ENTRY_COMPRESSED_BATCH => {
                let codec = BatchCodec::from_u8(b.get_u8());
                let uncompressed_len = b.get_u32();
                let body = bytes.slice(1 + 1 + 4..);
                WalEntry::CompressedBatch(codec, uncompressed_len, body)
            }
            _ => panic!("Unknown wal entry type {entry_type}"),
        }
    }

    fn record_len(k: &[u8], v: &[u8]) -> usize {
        1 + 1 + 2 + k.len() + v.len()
    }
}

impl IntoBytesFixed for WalEntry {
    fn len(&self) -> usize {
        match self {
            WalEntry::Record(KeySpace(_), k, v, _relocated) => Self::record_len(k, v),
            WalEntry::Index(KeySpace(_), index) => 1 + 1 + index.len(),
            WalEntry::Remove(KeySpace(_), k) => 1 + 1 + k.len(),
            WalEntry::BatchStart(_) => 1 + 4,
            WalEntry::DropCells(KeySpace(_), from_cell, to_cell) => {
                1 + 1 + from_cell.len() + to_cell.len()
            }
            WalEntry::CompressedBatch(_, _, body) => 1 + 1 + 4 + body.len(),
        }
    }

    fn write_into_bytes(&self, buf: &mut BytesMut) {
        // todo avoid copy here
        match self {
            WalEntry::Record(ks, k, v, relocated) => {
                if *relocated {
                    buf.put_u8(Self::WAL_ENTRY_RELOCATED_RECORD);
                } else {
                    buf.put_u8(Self::WAL_ENTRY_RECORD);
                }
                buf.put_u8(ks.0);
                // todo use key len from ks instead
                buf.put_u16(k.len() as u16);
                buf.put_slice(k);
                buf.put_slice(v);
            }
            WalEntry::Index(ks, bytes) => {
                buf.put_u8(Self::WAL_ENTRY_INDEX);
                buf.put_u8(ks.0);
                buf.put_slice(bytes);
            }
            WalEntry::Remove(ks, k) => {
                buf.put_u8(Self::WAL_ENTRY_REMOVE);
                buf.put_u8(ks.0);
                buf.put_slice(k)
            }
            WalEntry::BatchStart(size) => {
                buf.put_u8(Self::WAL_ENTRY_BATCH_START);
                buf.put_u32(*size);
            }
            WalEntry::DropCells(ks, from_cell, to_cell) => {
                buf.put_u8(Self::WAL_ENTRY_DROP_CELLS);
                buf.put_u8(ks.0);
                from_cell.write_into_bytes(buf);
                to_cell.write_into_bytes(buf);
            }
            WalEntry::CompressedBatch(codec, uncompressed_len, body) => {
                buf.put_u8(Self::WAL_ENTRY_COMPRESSED_BATCH);
                buf.put_u8(*codec as u8);
                buf.put_u32(*uncompressed_len);
                buf.put_slice(body);
            }
        }
    }
}

impl From<io::Error> for DbError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<WalError> for DbError {
    fn from(value: WalError) -> Self {
        Self::WalError(value)
    }
}

impl From<bincode::Error> for DbError {
    fn from(value: bincode::Error) -> Self {
        Self::CorruptedIndexEntry(value)
    }
}

#[cfg(test)]
#[path = "db_tests/generated.rs"]
mod tests;

#[cfg(test)]
mod multi_flusher_tests {
    use super::*;
    use crate::key_shape::{KeyShapeBuilder, KeyType};
    use tempdir::TempDir;

    #[test]
    fn test_multi_flusher_threads() {
        let dir = TempDir::new("test_multi_flusher").unwrap();

        // Test with different numbers of flusher threads
        for num_threads in [1, 2, 4, 8] {
            let mut config = Config::small();
            config.num_flusher_threads = num_threads;

            let mut builder = KeyShapeBuilder::new();
            builder.add_key_space("test", 8, 16, KeyType::uniform(16));
            let key_shape = builder.build();

            let db = Db::open(dir.path(), key_shape, Arc::new(config), Metrics::new()).unwrap();

            // Perform some operations to ensure the DB works
            for i in 0..100 {
                let key = format!("key{:05}", i);
                let value = format!("value{}", i);
                db.insert(KeySpace(0), key, value).unwrap();
            }

            // Wait for all flusher threads to finish
            db.large_table.flusher.barrier();

            // Verify we can read back the data
            for i in 0..100 {
                let key = format!("key{:05}", i);
                let value = db.get(KeySpace(0), key.as_bytes()).unwrap();
                assert!(value.is_some());
                assert_eq!(value.unwrap().as_ref(), format!("value{}", i).as_bytes());
            }
        }
    }
}
