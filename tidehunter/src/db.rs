use crate::batch::{Update, WriteBatch};
use crate::cell::CellId;
use crate::config::Config;
use crate::context::{DbOpKind, KsContext, WalWriteKind};
use crate::control::{ControlRegion, ControlRegionStore};
use crate::crc::IntoBytesFixed;
use crate::flusher::IndexFlusher;
use crate::index::index_format::IndexFormat;
use crate::index::index_table::IndexTable;
use crate::iterators::db_iterator::DbIterator;
use crate::iterators::IteratorResult;
use crate::key_shape::{KeyShape, KeySpace, KeySpaceDesc, KeyType};
use crate::large_table::{GetResult, LargeTable, Loader};
use crate::metrics::{Metrics, TimerExt};
use crate::relocation::{RelocationCommand, RelocationDriver, RelocationWatermarks, Relocator};
use crate::state_snapshot;
use crate::wal::{
    PreparedWalWrite, Wal, WalError, WalIterator, WalKind, WalPosition, WalRandomRead, WalWriter,
};
use bloom::needed_bits;
use bytes::{Buf, BufMut, BytesMut};
use minibytes::Bytes;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Weak};
use std::time::{Duration, Instant};
use std::{io, thread};

pub struct Db {
    pub(crate) large_table: LargeTable,
    pub(crate) wal: Arc<Wal>,
    pub(crate) indexes: Arc<Wal>,
    pub(crate) wal_writer: WalWriter,
    pub(crate) index_writer: WalWriter,
    pub(crate) control_region_store: Mutex<ControlRegionStore>,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
    pub(crate) key_shape: KeyShape,
    relocator: Relocator,
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
        let path = path.canonicalize()?;
        Self::maybe_create_shape_file(&path, &key_shape)?;
        Self::maybe_create_config_file(&path, &config)?;
        let (control_region_store, control_region) =
            Self::read_or_create_control_region(path.join(CONTROL_REGION_FILE), &key_shape)?;
        let relocation_watermarks = RelocationWatermarks::load(&path)?;
        let wal = Wal::open(&path, config.wal_layout(WalKind::Replay), metrics.clone())?;
        let indexes = Wal::open(&path, config.wal_layout(WalKind::Index), metrics.clone())?;

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
        let wal_iterator = wal.wal_iterator(control_region.last_position())?;
        let wal_writer =
            Self::replay_wal(&key_shape, &large_table, wal_iterator, &indexes, &metrics)?;
        let last_index_position = control_region.last_index_wal_position();
        let index_writer = indexes.writer_after(last_index_position)?;
        let control_region_store = Mutex::new(control_region_store);
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
            relocator,
        };
        this.report_memory_estimates();
        let this = Arc::new(this);

        // Now start the flusher threads with the weak reference
        let weak_db = Arc::downgrade(&this);
        let _handles = IndexFlusher::start_threads(flusher_receivers, weak_db, metrics.clone());

        // Start the relocator with the weak reference
        let _handle = RelocationDriver::start(
            Arc::downgrade(&this),
            relocation_watermarks,
            relocator_receiver,
            metrics,
        );

        // todo: store handles and wait for them on Db drop

        Ok(this)
    }

    pub fn shape_file_path(path: &Path) -> PathBuf {
        path.join("shape.yaml")
    }

    /// Create shape file if it doesn't exist
    fn maybe_create_shape_file(path: &Path, key_shape: &KeyShape) -> DbResult<()> {
        let shape_file_path = Self::shape_file_path(path);
        if !shape_file_path.exists() {
            let yaml = key_shape.to_yaml().map_err(|e| {
                DbError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to serialize key shape: {}", e),
                ))
            })?;
            std::fs::write(&shape_file_path, yaml)?;
        }
        Ok(())
    }

    /// Load key shape from database directory
    #[doc(hidden)] // Used by tools/wal_inspector for loading database schema
    pub fn load_key_shape(path: &Path) -> DbResult<KeyShape> {
        let shape_file_path = Self::shape_file_path(path);
        if !shape_file_path.exists() {
            return Err(DbError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Key shape file not found at {}", shape_file_path.display()),
            )));
        }

        let yaml = std::fs::read_to_string(&shape_file_path)?;
        KeyShape::from_yaml(&yaml).map_err(|e| {
            DbError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to deserialize key shape: {}", e),
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
                DbError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to serialize config: {}", e),
                ))
            })?;
            std::fs::write(&config_file_path, yaml)?;
        }
        Ok(())
    }

    /// Load config from database directory
    #[doc(hidden)] // Used by tools/wal_inspector for loading database config
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
                format!("Failed to deserialize config: {}", e),
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
                    .rebuild_control_region_from(current_wal_position)
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
        let w = PreparedWalWrite::new(&WalEntry::Record(context.id(), k.clone(), v.clone()));
        context.inc_wal_written(WalWriteKind::Record, w.len() as u64);
        let guard = self.wal_writer.write(&w)?;
        self.metrics
            .wal_written_bytes
            .set(guard.wal_position().offset() as i64);
        let reduced_key = context.ks_config.reduced_key_bytes(k);
        self.large_table
            .insert(&context.ks_config, reduced_key, guard, &v, self)?;
        Ok(())
    }

    pub fn remove(&self, ks: KeySpace, k: impl Into<Bytes>) -> DbResult<()> {
        let context = self.ks_context(ks);
        let _timer = context.db_op_timer(DbOpKind::Remove);
        let k = k.into();
        context.ks_config.check_key(&k);
        let w = PreparedWalWrite::new(&WalEntry::Remove(context.id(), k.clone()));
        context.inc_wal_written(WalWriteKind::Tombstone, w.len() as u64);
        let guard = self.wal_writer.write(&w)?;
        let reduced_key = context.ks_config.reduced_key_bytes(k);
        self.large_table
            .remove(&context.ks_config, reduced_key, guard, self)
    }

    pub fn get(&self, ks: KeySpace, k: &[u8]) -> DbResult<Option<Bytes>> {
        let context = self.ks_context(ks);
        let _timer = context.db_op_timer(DbOpKind::Get);
        let reduced_key = context.ks_config.reduce_key(k);
        match self
            .large_table
            .get(context, reduced_key.as_ref(), self, false)?
        {
            GetResult::Value(value) => {
                // todo check collision ?
                Ok(Some(value))
            }
            GetResult::WalPosition(w) => {
                let value = self.read_record_check_key(k, w)?;
                let Some(value) = value else {
                    return Ok(None);
                };
                self.large_table.update_lru(
                    &context.ks_config,
                    reduced_key.to_vec().into(),
                    value.clone(),
                );
                Ok(Some(value))
            }
            GetResult::NotFound => Ok(None),
        }
    }

    pub fn exists(&self, ks: KeySpace, k: &[u8]) -> DbResult<bool> {
        let context = self.ks_context(ks);
        let _timer = context.db_op_timer(DbOpKind::Exists);
        // todo check collision ?
        let reduced_key = context.ks_config.reduce_key(k);
        Ok(self
            .large_table
            .get(context, reduced_key.as_ref(), self, false)?
            .is_found())
    }

    pub fn write_batch(&self, batch: WriteBatch) -> DbResult<()> {
        let WriteBatch {
            updates,
            prepared_writes,
            tag,
        } = batch;
        if updates.is_empty() {
            return Ok(());
        }
        let _timer = self
            .metrics
            .db_op_mcs
            .with_label_values(&["write_batch", &tag])
            .mcs_timer();
        let mut last_position = WalPosition::INVALID;
        let mut update_writes = Vec::with_capacity(updates.len());
        let _write_timer = self
            .metrics
            .write_batch_times
            .with_label_values(&[&tag, "write"])
            .mcs_timer();

        let batch_start_entry = PreparedWalWrite::new(&WalEntry::BatchStart(updates.len() as u32));
        let guards = self
            .wal_writer
            .multi_write(std::iter::once(&batch_start_entry).chain(&prepared_writes))?;

        let mut num_inserts = 0;
        let mut num_deletes = 0;
        // Create iterator and skip the first guard (batch start)
        let mut guards_iter = guards.into_iter();
        guards_iter.next(); // Skip batch start guard

        for (idx, (update, guard)) in updates.iter().zip(guards_iter).enumerate() {
            let ks = update.ks();
            let context = self.ks_context(ks);
            let wal_kind = match update {
                Update::Record(..) => {
                    num_inserts += 1;
                    WalWriteKind::Record
                }
                Update::Remove(..) => {
                    num_deletes += 1;
                    WalWriteKind::Tombstone
                }
            };
            context.inc_wal_written(wal_kind, prepared_writes[idx].len() as u64);
            update_writes.push((update, guard));
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
        let _update_timer = self
            .metrics
            .write_batch_times
            .with_label_values(&[&tag, "update"])
            .mcs_timer();

        for (update, guard) in update_writes {
            let ks = self.key_shape.ks(update.ks());
            let reduced_key = update.reduced_key(ks);
            last_position = *guard.wal_position();
            match update {
                Update::Record(_, _, value) => {
                    self.large_table
                        .insert(ks, reduced_key, guard, value, self)?
                }
                Update::Remove(..) => self.large_table.remove(ks, reduced_key, guard, self)?,
            }
        }
        if last_position != WalPosition::INVALID {
            self.metrics
                .wal_written_bytes
                .set(last_position.offset() as i64);
        }

        Ok(())
    }

    /// Ordered iterator over DB in the specified range
    pub fn iterator(self: &Arc<Self>, ks: KeySpace) -> DbIterator {
        DbIterator::new(self.clone(), ks)
    }

    /// Returns true if this storage is empty.
    ///
    /// (warn) Right now it returns true if storage was never inserted true,
    /// but may return false if entry was inserted and then deleted.
    pub fn is_empty(&self) -> bool {
        self.large_table.is_empty()
    }

    pub fn ks_name(&self, ks: KeySpace) -> &str {
        self.key_shape.ks(ks).name()
    }

    pub(crate) fn ks(&self, ks: KeySpace) -> &KeySpaceDesc {
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
    ) -> DbResult<Option<IteratorResult<Bytes>>> {
        let context = self.ks_context(ks);
        let _timer = context.db_op_timer(DbOpKind::NextEntry);
        let Some(result) = self.large_table.next_entry(
            &context.ks_config,
            cell,
            prev_key,
            self,
            end_cell_exclusive,
            reverse,
        )?
        else {
            return Ok(None);
        };
        let Some(record) = self.read_record(result.value)? else {
            return Ok(None);
        };
        let (key, value) = record;
        Ok(Some(result.with_key_value(key, value)))
    }

    /// Returns the next cell in the large table
    pub(crate) fn next_cell(
        &self,
        ks: &KeySpaceDesc,
        cell: &CellId,
        reverse: bool,
    ) -> Option<CellId> {
        let context = self.ks_context(ks.id());
        let _timer = context.db_op_timer(DbOpKind::NextCell);
        self.large_table.next_cell(ks, cell, reverse)
    }

    pub(crate) fn update_flushed_index(
        &self,
        ks: KeySpace,
        cell: CellId,
        original_index: Arc<IndexTable>,
        position: WalPosition,
    ) {
        let context = self.ks_context(ks);
        let _timer = context.db_op_timer(DbOpKind::UpdateFlushedIndex);
        self.large_table
            .update_flushed_index(&context.ks_config, &cell, original_index, position)
    }

    fn read_record_check_key(&self, k: &[u8], position: WalPosition) -> DbResult<Option<Bytes>> {
        let Some(record) = self.read_record(position)? else {
            return Ok(None);
        };
        let (wal_key, v) = record;
        if wal_key.as_ref() != k {
            Ok(None)
        } else {
            Ok(Some(v))
        }
    }

    fn read_record(&self, position: WalPosition) -> DbResult<Option<(Bytes, Bytes)>> {
        let entry = self.read_report_entry(&self.wal, position)?;
        let Some(entry) = entry else {
            return Ok(None);
        };
        if let WalEntry::Record(KeySpace(_), k, v) = entry {
            Ok(Some((k, v)))
        } else {
            panic!("Unexpected wal entry where expected record");
        }
    }

    fn report_read(&self, entry: &WalEntry, mapped: bool) {
        let (kind, ks) = match entry {
            WalEntry::Record(ks, _, _) => ("record", self.key_shape.ks(*ks).name()),
            WalEntry::Index(ks, _) => ("index", self.key_shape.ks(*ks).name()),
            WalEntry::Remove(ks, _) => ("tombstone", self.key_shape.ks(*ks).name()),
            WalEntry::BatchStart(_) => return,
        };
        let mapped = if mapped { "mapped" } else { "unmapped" };
        self.metrics
            .read
            .with_label_values(&[ks, kind, mapped])
            .inc();
        self.metrics
            .read_bytes
            .with_label_values(&[ks, kind, mapped])
            .inc_by(entry.len() as u64);
    }

    fn replay_wal(
        key_shape: &KeyShape,
        large_table: &LargeTable,
        mut wal_iterator: WalIterator,
        indexes: &Wal,
        metrics: &Metrics,
    ) -> DbResult<WalWriter> {
        let mut batch = VecDeque::new();
        let mut batch_remaining = 0;
        loop {
            let (position, entry) = if batch_remaining == 0 && !batch.is_empty() {
                batch.pop_front().expect("invariant checked")
            } else {
                let entry = wal_iterator.next();
                if matches!(entry, Err(WalError::Crc(_))) {
                    break Ok(wal_iterator.into_writer());
                }
                let (position, raw_entry) = entry?;
                let entry = WalEntry::from_bytes(raw_entry);
                (position, entry)
            };
            if batch_remaining > 0 {
                assert!(
                    matches!(entry, WalEntry::Record(..) | WalEntry::Remove(..)),
                    "encountered entry {entry:?} at position {position:?} during replay, while expected record or tombstone"
                );
                batch_remaining -= 1;
                batch.push_back((position, entry));
                continue;
            }
            match entry {
                WalEntry::Record(ks, k, v) => {
                    metrics.replayed_wal_records.inc();
                    let ks = key_shape.ks(ks);
                    let reduced_key = ks.reduced_key_bytes(k);
                    let guard = crate::wal_tracker::WalGuard::replay_guard(position);
                    large_table.insert(ks, reduced_key, guard, &v, indexes)?;
                }
                WalEntry::Index(_ks, _bytes) => {
                    // todo - handle this by updating large table to Loaded()
                }
                WalEntry::Remove(ks, k) => {
                    metrics.replayed_wal_records.inc();
                    let ks = key_shape.ks(ks);
                    let reduced_key = ks.reduced_key_bytes(k);
                    let guard = crate::wal_tracker::WalGuard::replay_guard(position);
                    large_table.remove(ks, reduced_key, guard, indexes)?;
                }
                WalEntry::BatchStart(size) => {
                    batch = VecDeque::with_capacity(size as usize);
                    batch_remaining = size;
                }
            }
        }
    }

    pub fn rebuild_control_region(&self) -> DbResult<u64> {
        self.rebuild_control_region_from(self.wal_writer.position())
    }

    pub fn force_rebuild_control_region(&self) -> DbResult<u64> {
        // Force flush by setting snapshot_unload_threshold to 0
        self.rebuild_control_region_from_with_threshold(self.wal_writer.position(), Some(0))
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn is_all_clean(&self) -> bool {
        self.large_table.is_all_clean()
    }

    fn rebuild_control_region_from(&self, current_wal_position: u64) -> DbResult<u64> {
        self.rebuild_control_region_from_with_threshold(current_wal_position, None)
    }

    fn rebuild_control_region_from_with_threshold(
        &self,
        current_wal_position: u64,
        snapshot_unload_threshold_override: Option<u64>,
    ) -> DbResult<u64> {
        let mut crs = self.control_region_store.lock();
        let _timer = self
            .metrics
            .rebuild_control_region_time_mcs
            .clone()
            .mcs_timer();
        let _snapshot_timer = self.metrics.snapshot_lock_time_mcs.clone().mcs_timer();
        let snapshot = self.large_table.snapshot(
            current_wal_position,
            self,
            crs.p90_index_position(),
            snapshot_unload_threshold_override,
        )?;
        // todo remove this sleep.
        // We need a way to ensure that in-flight index reads do not read files we are removing
        thread::sleep(Duration::from_millis(100));
        let min_index_position = snapshot.data.iter_valid_val_positions().min();
        if let Some(min_index_position) = min_index_position {
            self.index_writer.gc(min_index_position.offset())?;
        }
        self.indexes.fsync()?;
        self.wal.fsync()?;
        crs.store(snapshot.data, snapshot.replay_from, &self.metrics);
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
        context.inc_wal_written(WalWriteKind::Index, w.len() as u64);
        Ok(*self.index_writer.write(&w)?.wal_position())
    }

    fn read_entry(wal: &Wal, position: WalPosition) -> DbResult<(bool, Option<WalEntry>)> {
        let (mapped, entry) = wal.read_unmapped(position)?;
        Ok((mapped, entry.map(WalEntry::from_bytes)))
    }

    fn read_report_entry(&self, wal: &Wal, position: WalPosition) -> DbResult<Option<WalEntry>> {
        let (mapped, entry) = Self::read_entry(wal, position)?;
        if let Some(ref entry) = entry {
            self.report_read(entry, mapped);
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
                        let bloom_size = needed_bits(bloom_filter.rate, bloom_filter.count) / 8;
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
        self.relocator.0.send(RelocationCommand::Start)
    }

    #[cfg(test)]
    pub fn start_blocking_relocation(&self) {
        let (sender, receiver) = mpsc::channel();
        self.relocator
            .0
            .send(RelocationCommand::StartBlocking(sender))
            .unwrap();
        receiver.recv().unwrap();
    }

    /// Wait for all background threads to finish by polling until no strong references remain.
    /// This ensures clean shutdown before database restart in tests.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn wait_for_background_threads_to_finish(self: Arc<Self>) {
        use std::thread;
        use std::time::Duration;

        // Convert Arc to Weak to track when all references are dropped
        let weak_db = Arc::downgrade(&self);

        // Drop our strong reference
        drop(self);

        // Wait for all background threads to finish
        let mut poll_count = 0;
        loop {
            if weak_db.upgrade().is_none() {
                // All references dropped, safe to proceed
                break;
            }
            poll_count += 1;
            if poll_count > 10000 {
                // Increased timeout
                panic!(
                    "Database shutdown timeout: background threads not terminating after {} polls",
                    poll_count
                );
            }
            thread::sleep(Duration::from_millis(10)); // Longer sleep
        }
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
}

impl Drop for Db {
    fn drop(&mut self) {
        let (sender, recv) = mpsc::channel();
        self.relocator
            .0
            .send(RelocationCommand::Cancel(sender))
            .expect("Failed to send a cancellation request to the relocator");
        recv.recv()
            .expect("Failed to await for graceful relocator shutdown");
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

    fn last_processed_wal_position(&self) -> u64 {
        // Wal doesn't have access to WalWriter, so return 0
        // This is only used during flush which Wal doesn't support
        0
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

    fn last_processed_wal_position(&self) -> u64 {
        self.wal_writer.last_processed()
    }

    fn min_wal_position(&self) -> u64 {
        self.wal.min_wal_position()
    }
}

#[doc(hidden)] // Used by tools/wal_inspector for WAL inspection
#[derive(Debug)]
pub enum WalEntry {
    Record(KeySpace, Bytes, Bytes),
    Index(KeySpace, Bytes),
    Remove(KeySpace, Bytes),
    BatchStart(u32),
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
    pub const INDEX_PREFIX_SIZE: usize = 2;

    pub fn from_bytes(bytes: Bytes) -> Self {
        let mut b = &bytes[..];
        let entry_type = b.get_u8();
        match entry_type {
            WalEntry::WAL_ENTRY_RECORD => {
                let ks = KeySpace(b.get_u8());
                let key_len = b.get_u16() as usize;
                let k = bytes.slice(4..4 + key_len);
                let v = bytes.slice(4 + key_len..);
                WalEntry::Record(ks, k, v)
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
            _ => panic!("Unknown wal entry type {entry_type}"),
        }
    }
}

impl IntoBytesFixed for WalEntry {
    fn len(&self) -> usize {
        match self {
            WalEntry::Record(KeySpace(_), k, v) => 1 + 1 + 2 + k.len() + v.len(),
            WalEntry::Index(KeySpace(_), index) => 1 + 1 + index.len(),
            WalEntry::Remove(KeySpace(_), k) => 1 + 1 + k.len(),
            WalEntry::BatchStart(_) => 1 + 4,
        }
    }

    fn write_into_bytes(&self, buf: &mut BytesMut) {
        // todo avoid copy here
        match self {
            WalEntry::Record(ks, k, v) => {
                buf.put_u8(Self::WAL_ENTRY_RECORD);
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

            drop(db);
        }
    }
}
