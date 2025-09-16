use crate::cell::{CellId, CellIdBytesContainer};
use crate::config::Config;
use crate::context::KsContext;
use crate::flusher::{FlushKind, FlusherCommand, IndexFlusher, IndexFlusherThread};
use crate::index::index_format::Direction;
use crate::index::index_format::IndexFormat;
use crate::index::index_table::IndexTable;
use crate::index::utils::{take_next_entry, NextEntryResult};
use crate::iterators::IteratorResult;
use crate::key_shape::{KeyShape, KeySpace, KeySpaceDesc, KeyType};
use crate::metrics::Metrics;
use crate::primitives::arc_cow::ArcCow;
use crate::primitives::range_from_excluding;
use crate::primitives::sharded_mutex::ShardedMutex;
use crate::runtime;
use crate::wal::{WalPosition, WalRandomRead};
use bloom::{BloomFilter, ASMS};
use lru::LruCache;
use minibytes::Bytes;
use parking_lot::MutexGuard;
use rand::rngs::ThreadRng;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use std::{cmp, mem};

pub struct LargeTable {
    table: Vec<KsTable>,
    config: Arc<Config>,
    pub(crate) flusher: IndexFlusher,
    metrics: Arc<Metrics>,
    pub(crate) fp: LargeTableFailPoints,
}

pub struct LargeTableEntry {
    cell: CellId,
    pub(crate) data: ArcCow<IndexTable>,
    state: LargeTableEntryState,
    context: KsContext,
    bloom_filter: Option<BloomFilter>,
    unload_jitter: usize,
    last_processed: u64,
    /// Tracks the WAL last_processed position captured when an async flush was initiated.
    /// - `None`: No flush is pending
    /// - `Some(pos)`: Async flush is in progress, will update `last_processed` to `pos` when complete
    pending_last_processed: Option<u64>,
}

enum LargeTableEntryState {
    Empty,
    Unloaded(WalPosition),
    Loaded(WalPosition),
    DirtyUnloaded(WalPosition),
    DirtyLoaded(WalPosition),
}

struct KsTable {
    ks: KeySpaceDesc,
    rows: ShardedMutex<Row>,
}

struct Row {
    value_lru: Option<LruCache<Bytes, Bytes>>,
    context: KsContext,
    entries: Entries,
}

enum Entries {
    Array(usize /*num_mutexes*/, Box<[LargeTableEntry]>),
    Tree(BTreeMap<CellIdBytesContainer, LargeTableEntry>),
}

pub(crate) type RowContainer<T> = BTreeMap<CellId, T>;

/// Snapshot data for a single entry containing both WAL position and last processed position
#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
#[doc(hidden)] // Used by tools/wal_inspector for control region inspection
pub struct SnapshotEntryData {
    pub position: WalPosition,
    pub last_processed: u64,
}

impl SnapshotEntryData {
    /// Creates an empty/invalid snapshot entry data
    pub fn empty() -> Self {
        Self {
            position: WalPosition::INVALID,
            last_processed: 0,
        }
    }
}

/// ks -> row -> cell
#[derive(Serialize, Deserialize, Debug)]
#[doc(hidden)] // Used by tools/wal_inspector for control region inspection
pub struct LargeTableContainer<T>(pub Vec<Vec<RowContainer<T>>>);

pub(crate) struct LargeTableSnapshot {
    pub data: LargeTableContainer<SnapshotEntryData>,
    pub replay_from: u64,
}

impl LargeTable {
    pub(crate) fn from_unloaded<L: Loader>(
        key_shape: &KeyShape,
        snapshot: &LargeTableContainer<SnapshotEntryData>,
        config: Arc<Config>,
        flusher: IndexFlusher,
        metrics: Arc<Metrics>,
        loader: &L,
    ) -> Self {
        assert_eq!(
            snapshot.0.len(),
            key_shape.num_ks(),
            "Snapshot has different number of key spaces"
        );
        let mut rng = ThreadRng::default();
        let table = key_shape
            .iter_ks()
            .zip(snapshot.0.iter())
            .map(|(ks, ks_snapshot)| {
                if ks_snapshot.len() != ks.num_mutexes() {
                    panic!(
                        "Invalid snapshot for ks {}: {} rows, expected {} rows",
                        ks.id().as_usize(),
                        ks_snapshot.len(),
                        ks.num_mutexes()
                    );
                }
                let mut bloom_filter_restore_time = 0;
                let rows = ks_snapshot.iter().map(|row_snapshot| {
                    let context = KsContext {
                        ks_config: ks.clone(),
                        config: config.clone(),
                        metrics: metrics.clone(),
                    };
                    let entries = row_snapshot.iter().map(|(cell, entry_data)| {
                        let bloom_filter = context.ks_config.bloom_filter().map(|opts| {
                            let mut filter = BloomFilter::with_rate(opts.rate, opts.count);
                            if entry_data.position.is_valid() {
                                let now = Instant::now();
                                let data =
                                    loader.load(&context.ks_config, entry_data.position).expect(
                                        "Failed to load an index entry to reconstruct bloom filter",
                                    );
                                for key in data.keys() {
                                    filter.insert(key);
                                }
                                bloom_filter_restore_time += now.elapsed().as_micros();
                            }
                            filter
                        });
                        let unload_jitter = config.gen_dirty_keys_jitter(&mut rng);
                        LargeTableEntry::from_snapshot_data(
                            context.clone(),
                            cell.clone(),
                            entry_data,
                            unload_jitter,
                            bloom_filter,
                        )
                    });
                    let entries = match ks.key_type() {
                        KeyType::Uniform(_) => Entries::Array(ks.num_mutexes(), entries.collect()),
                        KeyType::PrefixedUniform(_) => Entries::Tree(
                            entries
                                .map(|e| (e.cell.assume_bytes_id().clone(), e))
                                .collect(),
                        ),
                    };
                    let value_lru = ks.value_cache_size().map(LruCache::new);
                    Row {
                        entries,
                        context,
                        value_lru,
                    }
                });
                let rows = ShardedMutex::from_iterator(rows);
                if bloom_filter_restore_time > 0 {
                    metrics
                        .bloom_filter_restore_time_mcs
                        .with_label_values(&[ks.name()])
                        .inc_by(bloom_filter_restore_time as u64);
                }
                KsTable {
                    ks: ks.clone(),
                    rows,
                }
            })
            .collect();
        Self {
            table,
            config,
            flusher,
            metrics,
            fp: Default::default(),
        }
    }

    pub fn insert<L: Loader>(
        &self,
        ks: &KeySpaceDesc,
        k: Bytes,
        guard: crate::wal_tracker::WalGuard,
        value: &Bytes,
        loader: &L,
    ) -> Result<(), L::Error> {
        self.fp.fp_insert_before_lock();
        let v = *guard.wal_position();
        let (mut row, cell) = self.row(ks, &k);
        let entry = row.entry_mut(&cell);
        if self.skip_stale_update(entry, &k, v) {
            self.metrics
                .skip_stale_update
                .with_label_values(&[ks.name(), "insert"])
                .inc();
            return Ok(());
        }
        entry.insert(k.clone(), v);
        let index_size = entry.data.len();
        if loader.flush_supported() && self.too_many_dirty(entry) {
            // Drop the guard before flushing to ensure last_processed is updated
            drop(guard);
            entry.unload_if_ks_enabled(&self.flusher, loader)?;
        }
        if let Some(value_lru) = &mut row.value_lru {
            let delta: i64 = (k.len() + value.len()) as i64;
            let previous = value_lru.push(k, value.clone());
            self.update_lru_metric(ks, previous, delta);
        }
        self.metrics
            .max_index_size
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                if index_size > old {
                    Some(index_size)
                } else {
                    None
                }
            })
            .ok();

        let max_index_size = self.metrics.max_index_size.load(Ordering::Relaxed);
        self.metrics
            .max_index_size_metric
            .set(max_index_size as i64);
        self.metrics.index_size.observe(index_size as f64);
        Ok(())
    }

    pub fn remove<L: Loader>(
        &self,
        ks: &KeySpaceDesc,
        k: Bytes,
        guard: crate::wal_tracker::WalGuard,
        _loader: &L,
    ) -> Result<(), L::Error> {
        self.fp.fp_remove_before_lock();
        let v = *guard.wal_position();
        let (mut row, cell) = self.row(ks, &k);
        let entry = row.entry_mut(&cell);
        if self.skip_stale_update(entry, &k, v) {
            self.metrics
                .skip_stale_update
                .with_label_values(&[ks.name(), "remove"])
                .inc();
            return Ok(());
        }
        entry.remove(k.clone(), v);
        if let Some(value_lru) = &mut row.value_lru {
            let previous = value_lru.pop(&k);
            self.update_lru_metric(ks, previous.map(|v| (k, v)), 0);
        }
        Ok(())
    }

    pub fn update_lru(&self, ks: &KeySpaceDesc, key: Bytes, value: Bytes) {
        if ks.value_cache_size().is_none() {
            return;
        }
        let (mut row, _cell) = self.row(ks, &key);
        let Some(value_lru) = &mut row.value_lru else {
            unreachable!()
        };
        let delta: i64 = (key.len() + value.len()) as i64;
        let previous = value_lru.push(key, value);
        self.update_lru_metric(ks, previous, delta);
    }

    fn update_lru_metric(
        &self,
        ks: &KeySpaceDesc,
        previous: Option<(Bytes, Bytes)>,
        mut delta: i64,
    ) {
        if let Some((p_key, p_value)) = previous {
            delta -= (p_key.len() + p_value.len()) as i64;
        }
        self.metrics
            .value_cache_size
            .with_label_values(&[ks.name()])
            .add(delta);
    }

    pub fn get<L: Loader>(
        &self,
        ks: &KeySpaceDesc,
        k: &[u8],
        loader: &L,
        skip_lru_cache: bool,
    ) -> Result<GetResult, L::Error> {
        let (mut row, cell) = self.row(ks, k);
        if let (Some(value_lru), false) = (&mut row.value_lru, skip_lru_cache) {
            if let Some(value) = value_lru.get(k) {
                self.metrics
                    .lookup_result
                    .with_label_values(&[ks.name(), "found", "lru"])
                    .inc();
                return Ok(GetResult::Value(value.clone()));
            }
        }
        let entry = row.try_entry_mut(&cell);
        let Some(entry) = entry else {
            return Ok(self.report_lookup_result(ks, None, "prefix"));
        };
        if entry.bloom_filter_not_found(k) {
            return Ok(self.report_lookup_result(ks, None, "bloom"));
        }
        let index_position = match entry.state {
            LargeTableEntryState::Empty => return Ok(self.report_lookup_result(ks, None, "cache")),
            LargeTableEntryState::Loaded(_) => {
                return Ok(self.report_lookup_result(ks, entry.get(k), "cache"))
            }
            LargeTableEntryState::DirtyLoaded(_) => {
                return Ok(self.report_lookup_result(
                    ks,
                    entry.get(k).and_then(WalPosition::valid),
                    "cache",
                ))
            }
            LargeTableEntryState::DirtyUnloaded(position) => {
                // optimization: in dirty unloaded state we might not need to load entry
                if let Some(found) = entry.get(k) {
                    return Ok(self.report_lookup_result(ks, found.valid(), "cache"));
                }
                position
            }
            LargeTableEntryState::Unloaded(position) => position,
        };
        // drop row to avoid holding mutex during IO
        drop(row);
        let now = Instant::now();
        let index_reader = loader.index_reader(index_position)?;
        // todo - consider only doing block_in_place for the syscall random reader
        // TODO: handle entries that may be removed by relocation but are still referenced in the index
        let result = runtime::block_in_place(|| {
            ks.index_format()
                .lookup_unloaded(ks, &index_reader, k, &self.metrics)
        });
        self.metrics
            .lookup_mcs
            .with_label_values(&[index_reader.kind_str(), ks.name()])
            .observe(now.elapsed().as_micros() as f64);
        Ok(self.report_lookup_result(ks, result, "lookup"))
    }

    /// Checks if the update is potentially stale and the operation needs to be canceled.
    /// This can happen if there are multiple concurrent updates for the same key.
    /// Returns true if the update is outdated and should be skipped
    fn skip_stale_update(
        &self,
        entry: &LargeTableEntry,
        key: &Bytes,
        wal_position: WalPosition,
    ) -> bool {
        // check the existing loaded WAL position first, if present
        if let Some(last_position) = entry.data.get_update_position(key) {
            last_position > wal_position
        } else {
            false
        }
    }

    fn report_lookup_result(
        &self,
        ks: &KeySpaceDesc,
        v: Option<WalPosition>,
        source: &str,
    ) -> GetResult {
        let found = if v.is_some() { "found" } else { "not_found" };
        self.metrics
            .lookup_result
            .with_label_values(&[ks.name(), found, source])
            .inc();
        match v {
            None => GetResult::NotFound,
            Some(w) => GetResult::WalPosition(w),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.table
            .iter()
            .all(|m| m.rows.mutexes().iter().all(|m| m.lock().entries.is_empty()))
    }

    fn too_many_dirty(&self, entry: &mut LargeTableEntry) -> bool {
        if entry.state.is_dirty() {
            // todo - we no longer have a counter for number of dirty keys for DirtyLoaded state.
            // This means in DirtyLoaded state inserting single dirty key can trigger flush,
            // if many clean keys are present.
            // Ideally instead we should just unload DirtyLoaded->DirtyUnloaded instead.
            let dirty_count = entry.data.len();
            entry
                .context
                .excess_dirty_keys(dirty_count.saturating_sub(entry.unload_jitter))
        } else {
            false
        }
    }

    fn row(&self, ks: &KeySpaceDesc, k: &[u8]) -> (MutexGuard<'_, Row>, CellId) {
        let cell = ks.cell_id(k);
        let mutex = ks.mutex_for_cell(&cell);
        let row = self.row_by_mutex(ks, mutex);
        (row, cell)
    }

    fn row_by_mutex(&self, ks: &KeySpaceDesc, mutex: usize) -> MutexGuard<'_, Row> {
        let ks_table = self.ks_table(ks);
        ks_table.lock(
            mutex,
            &self
                .metrics
                .large_table_contention
                .with_label_values(&[ks.name()]),
        )
    }

    fn ks_table(&self, ks: &KeySpaceDesc) -> &ShardedMutex<Row> {
        &self
            .table
            .get(ks.id().as_usize())
            .expect("Table not found for ks")
            .rows
    }

    /// Provides a snapshot of this large table along with replay position in the wal for the snapshot.
    pub(crate) fn snapshot<L: Loader>(
        &self,
        tail_position: u64,
        loader: &L,
        // All entries below this wal positions will be relocated
        force_relocate_below: Option<WalPosition>,
        snapshot_unload_threshold_override: Option<u64>,
    ) -> Result<LargeTableSnapshot, L::Error> {
        assert!(loader.flush_supported());
        // Capture the WAL's last_processed position at snapshot time
        let wal_last_processed = loader.last_processed_wal_position();
        // See ks_snapshot documentation for details
        // on how snapshot replay position is determined.
        let mut replay_from: Option<u64> = None;
        let mut max_position: Option<WalPosition> = None;
        let mut data = Vec::with_capacity(self.table.len());
        for ks_table in self.table.iter() {
            let (ks_data, ks_replay_from, ks_max_position) = self.ks_snapshot(
                ks_table,
                tail_position,
                loader,
                force_relocate_below,
                snapshot_unload_threshold_override,
            )?;
            data.push(ks_data);
            if let Some(ks_replay_from) = ks_replay_from {
                replay_from = Some(cmp::min(replay_from.unwrap_or(u64::MAX), ks_replay_from));
            }
            if let Some(ks_max_position) = ks_max_position {
                max_position = Some(if let Some(max_position) = max_position {
                    cmp::max(ks_max_position, max_position)
                } else {
                    ks_max_position
                });
            }
        }

        let replay_from = if let Some(replay_from) = replay_from {
            // Case 1: Some entries are dirty - use minimum last_processed
            replay_from
        } else if let Some(max_pos) = max_position {
            // Case 2: All clean - use highest flushed position
            max_pos.offset()
        } else {
            // Case 3: Empty database - use WAL's last_processed position
            wal_last_processed
        };

        let data = LargeTableContainer(data);
        Ok(LargeTableSnapshot { data, replay_from })
    }

    /// Takes snapshot of a given key space.
    /// Returns (Snapshot, ReplayFrom, MaximumValidEntryWalPosition).
    ///
    /// Replay from is calculated as the lowest wal position across all dirty entries
    /// Replay from is None if all entries in ks are clean entries.
    ///
    /// Maximum valid entry wal position is a maximum wal position of all entries persisted to disk
    /// This position is None if no entries are persisted to disk (all entries are Empty).
    ///
    /// The combined snapshot replay position across all key spaces is determined as following:
    ///
    /// * If at least one replay_from for ks table is not None, the replay_from for entire snapshot
    ///   is a minimum across all key spaces where replay_from is not None.
    ///   Reasoning here is that if some key space has some dirty entry,
    ///   the entire snapshot needs to be replayed from the position of that dirty entry.
    ///
    /// * If all the ReplayFrom are None, the replay position for snapshot
    ///   is a maximum of all non-None MaximumValidEntryWalPosition across all key spaces.
    ///   The Reasoning is that if all entries in all key spaces are clean, it is safe to replay
    ///   wal from the highest written index entry.
    ///   As more entries could be added to the wal while snapshot is created,
    ///   we cannot simply take the current wal writer position here as a snapshot replay position.
    ///
    /// * If all ReplayFrom are None and all MaximumValidEntryWalPosition are None,
    ///   then the database is empty at the time of a snapshot, and the replay_position
    ///   for snapshot is set to WalPosition::INVALID to indicate wal needs to be replayed
    ///   from the beginning.
    #[allow(clippy::type_complexity)] // todo fix
    fn ks_snapshot<L: Loader>(
        &self,
        ks_table: &KsTable,
        tail_position: u64,
        loader: &L,
        force_relocate_below: Option<WalPosition>,
        snapshot_unload_threshold_override: Option<u64>,
    ) -> Result<
        (
            Vec<RowContainer<SnapshotEntryData>>,
            Option<u64>,
            Option<WalPosition>,
        ),
        L::Error,
    > {
        let mut replay_from: Option<u64> = None;
        let mut max_wal_position: Option<WalPosition> = None;
        let mut ks_data = Vec::with_capacity(ks_table.rows.mutexes().len());
        for mutex in ks_table.rows.mutexes() {
            let mut row = mutex.lock();
            let mut row_data = RowContainer::new();
            for entry in row.entries.iter_mut() {
                entry.maybe_flush_for_snapshot(
                    loader,
                    &self.config,
                    tail_position,
                    force_relocate_below,
                    snapshot_unload_threshold_override,
                )?;
                let position = entry.state.wal_position();
                let snapshot_data = SnapshotEntryData {
                    position,
                    last_processed: entry.last_processed,
                };
                row_data.insert(entry.cell.clone(), snapshot_data);
                if let Some(valid_position) = position.valid() {
                    max_wal_position = if let Some(max_wal_position) = max_wal_position {
                        Some(cmp::max(max_wal_position, valid_position))
                    } else {
                        Some(valid_position)
                    };
                }
                // Do not use last_processed from empty entries
                if !entry.state.is_empty() {
                    replay_from = Some(cmp::min(
                        replay_from.unwrap_or(u64::MAX),
                        entry.last_processed,
                    ));
                }
            }
            ks_data.push(row_data);
        }
        let metric = self
            .metrics
            .index_distance_from_tail
            .with_label_values(&[ks_table.ks.name()]);
        match replay_from {
            None => metric.set(0),
            Some(0) => metric.set(-1), // 0 indicates replay from beginning
            Some(position) => {
                // This can go below 0 because tail_position is not synchronized
                // and index position can be higher than the tail_position in rare cases
                metric.set(tail_position.saturating_sub(position) as i64)
            }
        }
        Ok((ks_data, replay_from, max_wal_position))
    }

    /// Takes a next entry in the large table.
    ///
    /// See Db::next_entry for documentation.
    pub(crate) fn next_entry<L: Loader>(
        &self,
        ks: &KeySpaceDesc,
        mut cell: CellId,
        mut prev_key: Option<Bytes>,
        loader: &L,
        end_cell_exclusive: &Option<CellId>,
        reverse: bool,
    ) -> Result<Option<IteratorResult<WalPosition>>, L::Error> {
        let ks_table = self.ks_table(ks);
        loop {
            let next_in_cell = {
                let row = ks.mutex_for_cell(&cell);
                let mut row = ks_table.lock(
                    row,
                    &self
                        .metrics
                        .large_table_contention
                        .with_label_values(&[ks.name()]),
                );
                Self::next_in_cell(loader, &mut row, &cell, prev_key, reverse)?
                // drop row mutex as required by Self::next_cell called below
            };
            if let Some((key, value)) = next_in_cell {
                return Ok(Some(IteratorResult {
                    cell: Some(cell),
                    key,
                    value,
                }));
            } else {
                prev_key = None;
                let Some(next_cell) = self.next_cell(ks, &cell, reverse) else {
                    return Ok(None);
                };
                if let Some(end_cell_exclusive) = end_cell_exclusive {
                    if reverse {
                        if &next_cell <= end_cell_exclusive {
                            return Ok(None);
                        }
                    } else if &next_cell >= end_cell_exclusive {
                        return Ok(None);
                    }
                }
                cell = next_cell;
            }
        }
    }

    fn next_in_cell<L: Loader>(
        loader: &L,
        row: &mut Row,
        cell: &CellId,
        prev_key: Option<Bytes>,
        reverse: bool,
    ) -> Result<Option<(Bytes, WalPosition)>, L::Error> {
        let Some(entry) = row.try_entry_mut(cell) else {
            return Ok(None);
        };

        if !entry.context.ks_config.unloaded_iterator_enabled() {
            entry.maybe_load(loader)?;
        }

        match &entry.state {
            // If already loaded, just use what's in memory
            LargeTableEntryState::Loaded(_) | LargeTableEntryState::DirtyLoaded(_) => {
                let mut prev_key = prev_key;
                // Skip entries marked as invalid (deleted)
                while let Some((key, pos)) = entry.next_entry(prev_key.take(), reverse) {
                    if pos.is_valid() {
                        return Ok(Some((key, pos)));
                    }
                    // Get next entry after the invalid one
                    prev_key = Some(key);
                }

                Ok(None)
            }

            // For empty state, we have nothing to return
            LargeTableEntryState::Empty => Ok(None),

            // For unloaded states, read from disk without loading everything
            LargeTableEntryState::Unloaded(position) => {
                // Get reader for on-disk index
                let index_reader = loader.index_reader(*position)?;
                let format = entry.context.ks_config.index_format();

                // Use the format to find the next entry from on-disk index
                let result = runtime::block_in_place(|| {
                    let direction = Direction::from_bool(reverse);

                    format.next_entry_unloaded(
                        &entry.context.ks_config,
                        &index_reader,
                        prev_key.as_deref(),
                        direction,
                        &entry.context.metrics,
                    )
                });

                Ok(result)
            }

            // For dirty unloaded, combine in-memory and on-disk entries
            LargeTableEntryState::DirtyUnloaded(position) => {
                // Check in-memory part first (dirty keys)
                let in_memory_next = entry.next_entry(prev_key.clone(), reverse);

                // Get reader for on-disk index
                let index_reader = loader.index_reader(*position)?;
                let format = entry.context.ks_config.index_format();

                // Use the format to find the next entry from on-disk index
                let on_disk_next = runtime::block_in_place(|| {
                    let direction = Direction::from_bool(reverse);

                    format.next_entry_unloaded(
                        &entry.context.ks_config,
                        &index_reader,
                        prev_key.as_deref(),
                        direction,
                        &entry.context.metrics,
                    )
                });

                // Use the utility function to determine the next entry
                let direction = Direction::from_bool(reverse);
                let result = match take_next_entry(in_memory_next, on_disk_next, direction) {
                    NextEntryResult::NotFound => None,
                    NextEntryResult::Found(key, val) => Some((key, val)),
                    NextEntryResult::SkipDeleted(skip_key) => {
                        // todo remove recursion
                        return Self::next_in_cell(loader, row, cell, Some(skip_key), reverse);
                    }
                };

                Ok(result)
            }
        }
    }

    /// See Db::next_cell for documentation
    /// This function acquires row mutexes, should not be called by code that might hold row mutex
    pub fn next_cell(&self, ks: &KeySpaceDesc, cell: &CellId, reverse: bool) -> Option<CellId> {
        match (ks.key_type(), cell) {
            (KeyType::Uniform(config), CellId::Integer(cell)) => {
                config.next_cell(ks, *cell, reverse)
            }
            (KeyType::PrefixedUniform(_), CellId::Bytes(bytes)) => {
                let mut mutex = ks.mutex_for_cell(cell);
                loop {
                    let row = self.row_by_mutex(ks, mutex);
                    if let Some(next) = row.entries.next_tree_cell(bytes, reverse) {
                        return Some(CellId::Bytes(next.clone()));
                    };
                    // todo we can optimize this by keeping hints to a next non-empty mutex in the row
                    let next_mutex = ks.next_mutex(mutex, reverse)?;
                    mutex = next_mutex;
                }
            }
            (KeyType::Uniform(_), CellId::Bytes(_)) => {
                panic!("next_cell with uniform key type but bytes cell id")
            }
            (KeyType::PrefixedUniform(_), CellId::Integer(_)) => {
                panic!("next_cell with prefix key type but integer cell id")
            }
        }
    }

    pub fn report_entries_state(&self) {
        let mut states: HashMap<_, i64> = HashMap::new();
        for ks_table in &self.table {
            for mutex in ks_table.rows.mutexes() {
                let lock = mutex.lock();
                for entry in lock.entries.iter() {
                    *states
                        .entry((entry.context.name().to_string(), entry.state.name()))
                        .or_default() += 1;
                }
            }
        }
        for ((ks, state), value) in states {
            self.metrics
                .entry_state
                .with_label_values(&[&ks, state])
                .set(value);
        }
    }

    pub fn update_flushed_index(
        &self,
        ks: &KeySpaceDesc,
        cell: &CellId,
        original_index: Arc<IndexTable>,
        position: WalPosition,
    ) {
        let row = ks.mutex_for_cell(cell);
        let ks_table = self.ks_table(ks);
        let mut row = ks_table.lock(
            row,
            &self
                .metrics
                .large_table_contention
                .with_label_values(&[ks.name()]),
        );
        let entry = row.entry_mut(cell);
        entry.update_flushed_index(original_index, position);
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn each_entry(&self, f: impl Fn(&mut LargeTableEntry)) {
        for ks_table in &self.table {
            for mutex in ks_table.rows.mutexes() {
                let mut lock = mutex.lock();
                for entry in lock.entries.iter_mut() {
                    f(entry);
                }
            }
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn is_all_clean(&self) -> bool {
        for ks_table in &self.table {
            for mutex in ks_table.rows.mutexes() {
                let mut lock = mutex.lock();
                for entry in lock.entries.iter_mut() {
                    if entry.state.is_dirty() {
                        return false;
                    }
                }
            }
        }
        true
    }

    pub(crate) fn bloom_key(key: &[u8], pos: WalPosition) -> Vec<u8> {
        let mut result = Vec::with_capacity(key.len() + 8);
        result.extend_from_slice(key);
        result.extend_from_slice(&pos.offset().to_le_bytes());
        result
    }

    pub(crate) fn build_index_bloom_filters<L: Loader>(
        &self,
        loader: &L,
    ) -> Result<HashMap<KeySpace, BloomFilter>, L::Error> {
        let now = Instant::now();
        let mut filters = HashMap::new();
        for ks_table in self.table.iter() {
            let ksd = &ks_table.ks;
            let Some(bloom_params) = ksd.relocation_bloom_filter() else {
                continue;
            };
            let mut bloom = BloomFilter::with_rate(bloom_params.rate, bloom_params.count);
            for mutex in ks_table.rows.mutexes() {
                let row = mutex.lock();
                for entry in row.entries.iter() {
                    for (key, pos) in entry.data.iter() {
                        bloom.insert(&Self::bloom_key(key, pos));
                    }
                    if let LargeTableEntryState::Unloaded(position)
                    | LargeTableEntryState::DirtyUnloaded(position) = &entry.state
                    {
                        if position.is_valid() {
                            let index_table = loader.load(ksd, *position)?;
                            for (key, pos) in index_table.iter() {
                                if entry.data.get(key).is_none() {
                                    bloom.insert(&Self::bloom_key(key, pos));
                                }
                            }
                        }
                    }
                }
            }
            filters.insert(ksd.id(), bloom);
        }
        if !filters.is_empty() {
            self.metrics
                .relocation_bloom_filter_build_time_mcs
                .inc_by(now.elapsed().as_micros() as u64);
        }
        Ok(filters)
    }
}

impl Row {
    pub fn entry_mut(&mut self, id: &CellId) -> &mut LargeTableEntry {
        self.entries.entry_mut(id, &self.context)
    }

    pub fn try_entry_mut(&mut self, id: &CellId) -> Option<&mut LargeTableEntry> {
        self.entries.try_entry_mut(id, &self.context)
    }
}

pub trait Loader {
    type Error: std::fmt::Debug;

    fn load(&self, ks: &KeySpaceDesc, position: WalPosition) -> Result<IndexTable, Self::Error>;

    fn index_reader(&self, position: WalPosition) -> Result<WalRandomRead, Self::Error>;

    fn flush_supported(&self) -> bool;

    fn flush(&self, ks: KeySpace, data: &IndexTable) -> Result<WalPosition, Self::Error>;

    /// Returns the last WAL position that has been fully processed and committed.
    /// This position represents the highest WAL offset where all operations up to and
    /// including that offset have been successfully processed and their guards dropped.
    fn last_processed_wal_position(&self) -> u64;

    fn min_wal_position(&self) -> u64;
}

impl LargeTableEntry {
    pub fn new_unloaded(
        context: KsContext,
        cell: CellId,
        position: WalPosition,
        unload_jitter: usize,
        bloom_filter: Option<BloomFilter>,
    ) -> Self {
        Self::new_with_state(
            context,
            cell,
            LargeTableEntryState::Unloaded(position),
            unload_jitter,
            bloom_filter,
        )
    }

    pub fn new_empty(context: KsContext, cell: CellId, unload_jitter: usize) -> Self {
        let bloom_filter = context
            .ks_config
            .bloom_filter()
            .map(|params| BloomFilter::with_rate(params.rate, params.count));
        Self::new_with_state(
            context,
            cell,
            LargeTableEntryState::Empty,
            unload_jitter,
            bloom_filter,
        )
    }

    fn new_with_state(
        context: KsContext,
        cell: CellId,
        state: LargeTableEntryState,
        unload_jitter: usize,
        bloom_filter: Option<BloomFilter>,
    ) -> Self {
        Self {
            context,
            cell,
            state,
            data: Default::default(),
            bloom_filter,
            unload_jitter,
            pending_last_processed: None,
            last_processed: 0,
        }
    }

    pub fn from_snapshot_data(
        context: KsContext,
        cell: CellId,
        entry_data: &SnapshotEntryData,
        unload_jitter: usize,
        bloom_filter: Option<BloomFilter>,
    ) -> Self {
        let mut entry = if entry_data.position == WalPosition::INVALID {
            LargeTableEntry::new_empty(context, cell, unload_jitter)
        } else {
            LargeTableEntry::new_unloaded(
                context,
                cell,
                entry_data.position,
                unload_jitter,
                bloom_filter,
            )
        };
        entry.last_processed = entry_data.last_processed;
        entry
    }

    pub fn insert(&mut self, k: Bytes, v: WalPosition) {
        self.state.mark_dirty();
        self.insert_bloom_filter(&k);
        self.data.make_mut().insert(k, v);
        self.report_loaded_keys_count();
    }

    pub fn remove(&mut self, k: Bytes, v: WalPosition) {
        self.state.mark_dirty();
        self.data.make_mut().remove(k, v);
        self.report_loaded_keys_count();
    }

    fn report_loaded_keys_count(&self) {
        // Report the total number of keys * key size as the metric
        let num_keys = self.data.len() as i64;
        let key_size = self.context.index_key_size().unwrap_or(64) as i64;
        self.context
            .metrics
            .loaded_key_bytes
            .with_label_values(&[self.context.name()])
            .set(num_keys * key_size);
    }

    fn insert_bloom_filter(&mut self, key: &[u8]) {
        let Some(bloom_filter) = &mut self.bloom_filter else {
            return;
        };
        // todo - rebuild bloom filter from time to time?
        bloom_filter.insert(&key);
    }

    /// Returns true if key is definitely not in the cell
    fn bloom_filter_not_found(&self, key: &[u8]) -> bool {
        if let Some(bloom_filter) = &self.bloom_filter {
            !bloom_filter.contains(&key)
        } else {
            false
        }
    }

    pub fn get(&self, k: &[u8]) -> Option<WalPosition> {
        if matches!(&self.state, LargeTableEntryState::Unloaded(_)) {
            panic!("Can't get in unloaded state");
        }
        self.data.get(k)
    }

    /// See IndexTable::next_entry for documentation.
    pub fn next_entry(
        &self,
        prev_key: Option<Bytes>,
        reverse: bool,
    ) -> Option<(Bytes, WalPosition)> {
        if matches!(&self.state, LargeTableEntryState::Unloaded(_)) {
            panic!("Can't next_entry in unloaded state");
        }
        self.data.next_entry(prev_key, reverse)
    }

    pub fn maybe_load<L: Loader>(&mut self, loader: &L) -> Result<(), L::Error> {
        let Some((state, position)) = self.state.as_unloaded_state() else {
            return Ok(());
        };
        let mut data = loader.load(&self.context.ks_config, position)?;
        let is_dirty = match state {
            UnloadedState::Dirty => {
                data.merge_dirty_no_clean(&self.data);
                true
            }
            UnloadedState::Clean => {
                assert!(self.data.is_empty());
                false
            }
        };
        self.data = ArcCow::new_owned(data);
        self.report_loaded_keys_count();
        if is_dirty {
            self.state = LargeTableEntryState::DirtyLoaded(position);
        } else {
            self.state = LargeTableEntryState::Loaded(position);
        }
        Ok(())
    }

    pub fn maybe_flush_for_snapshot<L: Loader>(
        &mut self,
        loader: &L,
        config: &Config,
        tail_position: u64,
        force_relocate_below: Option<WalPosition>,
        snapshot_unload_threshold_override: Option<u64>,
    ) -> Result<(), L::Error> {
        if self.pending_last_processed.is_some() {
            // todo metric / log?
            return Ok(());
        }
        let forced_relocation = if let (Some(index_wal_position), Some(force_relocate_below)) =
            (self.state.wal_position().valid(), force_relocate_below)
        {
            index_wal_position < force_relocate_below
        } else {
            false
        };
        if forced_relocation {
            self.context
                .metrics
                .snapshot_forced_relocation
                .with_label_values(&[self.context.name()])
                .inc();
        }
        if !forced_relocation && !self.state.is_dirty() {
            self.last_processed = loader.last_processed_wal_position();
            return Ok(());
        }
        let position = self.last_processed;
        // position can actually be great then tail_position due to concurrency
        let distance = tail_position.saturating_sub(position);
        // Use override if provided, otherwise use config value
        let threshold =
            snapshot_unload_threshold_override.unwrap_or(config.snapshot_unload_threshold);
        // todo this needs to be fixed for ks with self.context.ks_config.unloading_disabled()
        if (forced_relocation || distance >= threshold)
            && !self.context.ks_config.unloading_disabled()
        {
            self.context
                .metrics
                .snapshot_force_unload
                .with_label_values(&[self.context.name()])
                .inc();
            self.sync_flush(loader, forced_relocation)?;
        }
        Ok(())
    }

    /// Performs a synchronous flush regardless of the config setting.
    /// If forced_relocation is set, the flush happens even for clean state
    /// Caller is responsible for checking to things:
    /// * ks_config.unloading_disabled is false
    /// * pending_last_processed is None
    fn sync_flush<L: Loader>(
        &mut self,
        loader: &L,
        forced_relocation: bool,
    ) -> Result<(), L::Error> {
        let flush_kind = match self.flush_kind() {
            Some(kind) => kind,
            None => {
                if !forced_relocation {
                    return Ok(()); // Not in a flushable state and relocation was not requested
                }
                self.maybe_load(loader)?;
                FlushKind::FlushLoaded(self.data.clone_shared())
            }
        };

        let last_processed = loader.last_processed_wal_position();
        // Perform a synchronous flush
        let Some((_original_index, position)) = IndexFlusherThread::handle_command(
            loader,
            &FlusherCommand::new(self.context.id(), self.cell.clone(), flush_kind),
            &self.context,
        ) else {
            unreachable!(
                "IndexFlusherThread::handle_command should not return None for non-test command"
            )
        };
        self.last_processed = last_processed;
        self.clear_after_flush(position, last_processed);
        Ok(())
    }

    fn unload_if_ks_enabled<L: Loader>(
        &mut self,
        flusher: &IndexFlusher,
        loader: &L,
    ) -> Result<(), L::Error> {
        if self.context.ks_config.unloading_disabled() {
            return Ok(());
        }
        if self.pending_last_processed.is_none() {
            if self.context.config.sync_flush {
                self.sync_flush(loader, false)?;
            } else {
                // Perform async flush - store the captured value for later
                let flush_kind = self
                    .flush_kind()
                    .expect("unload_if_ks_enabled is called in clean state");
                self.pending_last_processed = Some(loader.last_processed_wal_position());
                flusher.request_flush(self.context.id(), self.cell.clone(), flush_kind);
            }
        }
        Ok(())
    }

    fn clear_after_flush(&mut self, position: WalPosition, last_processed: u64) {
        // Retain only entries with offset > last_processed
        self.data.make_mut().retain_above_position(last_processed);
        self.report_loaded_keys_count();

        // If all entries were removed, update state to Unloaded
        if self.data.is_empty() {
            self.state = LargeTableEntryState::Unloaded(position);
            self.context
                .metrics
                .flush_update
                .with_label_values(&["clear"])
                .inc();
        } else {
            // Some entries remain, keep state as DirtyUnloaded
            self.state = LargeTableEntryState::DirtyUnloaded(position);
            self.context
                .metrics
                .flush_update
                .with_label_values(&["partial"])
                .inc();
        }
    }

    pub fn update_flushed_index(&mut self, original_index: Arc<IndexTable>, position: WalPosition) {
        let pending_last_processed = self
            .pending_last_processed
            .take()
            .expect("update_flushed_index called while pending_last_processed is not set");

        // For unmerge, we always use the actual last_processed value (not u64::MAX)
        // to ensure we only remove entries that were actually committed
        match self.state {
            LargeTableEntryState::DirtyUnloaded(_) => {}
            LargeTableEntryState::DirtyLoaded(_) => {}
            LargeTableEntryState::Empty => panic!("update_merged_index in Empty state"),
            LargeTableEntryState::Unloaded(_) => panic!("update_merged_index in Unloaded state"),
            LargeTableEntryState::Loaded(_) => panic!("update_merged_index in Loaded state"),
        }
        // Now that flush is complete, update last_processed
        self.last_processed = pending_last_processed;
        if self.data.same_shared(&original_index) {
            self.clear_after_flush(position, pending_last_processed);
        } else {
            self.data
                .make_mut()
                .unmerge_flushed(&original_index, pending_last_processed);
            self.report_loaded_keys_count();
            self.state = LargeTableEntryState::DirtyUnloaded(position);
            self.context
                .metrics
                .flush_update
                .with_label_values(&["unmerge"])
                .inc();
        }
    }

    #[allow(dead_code)]
    fn unload_dirty_loaded<L: Loader>(&mut self, loader: &L) -> Result<(), L::Error> {
        let mut data = Default::default();
        mem::swap(&mut data, &mut self.data);
        let mut data = data.into_owned();
        data.clean_self();
        // todo - if this line returns error, self.data will be in the inconsistent state
        let position = loader.flush(self.context.id(), &data)?;
        self.state = LargeTableEntryState::Unloaded(position);
        self.data = Default::default();
        self.report_loaded_keys_count();
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.state, LargeTableEntryState::Empty)
    }

    pub fn flush_kind(&mut self) -> Option<FlushKind> {
        match self.state {
            LargeTableEntryState::Empty => None,
            LargeTableEntryState::Unloaded(_) => None,
            LargeTableEntryState::Loaded(_) => None,
            LargeTableEntryState::DirtyUnloaded(position) => {
                Some(FlushKind::MergeUnloaded(position, self.data.clone_shared()))
            }
            LargeTableEntryState::DirtyLoaded(_) => {
                Some(FlushKind::FlushLoaded(self.data.clone_shared()))
            }
        }
    }
}

impl LargeTableEntryState {
    pub fn mark_dirty(&mut self) {
        match self {
            LargeTableEntryState::Empty => {
                *self = LargeTableEntryState::DirtyLoaded(WalPosition::INVALID)
            }
            LargeTableEntryState::Loaded(position) => {
                *self = LargeTableEntryState::DirtyLoaded(*position)
            }
            LargeTableEntryState::DirtyLoaded(_) => {}
            LargeTableEntryState::Unloaded(pos) => {
                *self = LargeTableEntryState::DirtyUnloaded(*pos)
            }
            LargeTableEntryState::DirtyUnloaded(_) => {}
        }
    }

    pub fn as_unloaded_state(&mut self) -> Option<(UnloadedState, WalPosition)> {
        match self {
            LargeTableEntryState::Empty => None,
            LargeTableEntryState::Unloaded(pos) => Some((UnloadedState::Clean, *pos)),
            LargeTableEntryState::Loaded(_) => None,
            LargeTableEntryState::DirtyUnloaded(pos) => Some((UnloadedState::Dirty, *pos)),
            LargeTableEntryState::DirtyLoaded(_) => None,
        }
    }

    /// Wal position of the persisted index entry.
    pub fn wal_position(&self) -> WalPosition {
        match self {
            LargeTableEntryState::Empty => WalPosition::INVALID,
            LargeTableEntryState::Unloaded(w) => *w,
            LargeTableEntryState::Loaded(w) => *w,
            LargeTableEntryState::DirtyUnloaded(w) => *w,
            LargeTableEntryState::DirtyLoaded(w) => *w,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, LargeTableEntryState::Empty)
    }

    /// Returns whether wal needs to be replayed from the position returned by wal_position()
    /// to restore large table entry.
    ///
    /// This is the same as as_dirty_state().is_some()
    pub fn is_dirty(&self) -> bool {
        match self {
            LargeTableEntryState::Empty => false,
            LargeTableEntryState::Unloaded(_) => false,
            LargeTableEntryState::Loaded(_) => false,
            LargeTableEntryState::DirtyUnloaded(_) => true,
            LargeTableEntryState::DirtyLoaded(_) => true,
        }
    }

    #[allow(dead_code)]
    pub fn name(&self) -> &'static str {
        match self {
            LargeTableEntryState::Empty => "empty",
            LargeTableEntryState::Unloaded(_) => "unloaded",
            LargeTableEntryState::Loaded(_) => "loaded",
            LargeTableEntryState::DirtyUnloaded(_) => "dirty_unloaded",
            LargeTableEntryState::DirtyLoaded(_) => "dirty_loaded",
        }
    }
}

pub enum GetResult {
    Value(Bytes),
    WalPosition(WalPosition),
    NotFound,
}

impl GetResult {
    pub fn is_found(&self) -> bool {
        match self {
            GetResult::Value(_) => true,
            GetResult::WalPosition(_) => true,
            GetResult::NotFound => false,
        }
    }
}

enum UnloadedState {
    Dirty,
    Clean,
}

impl<T: Copy> LargeTableContainer<T> {
    /// Creates a new container with the given shape and filled with copy of passed value
    pub fn new_from_key_shape(key_shape: &KeyShape, value: T) -> Self {
        Self(
            key_shape
                .iter_ks()
                .map(|ks| {
                    (0..ks.num_mutexes())
                        .map(|row| Self::new_row(ks, row, value))
                        .collect()
                })
                .collect(),
        )
    }

    fn new_row(ks: &KeySpaceDesc, row: usize, value: T) -> RowContainer<T> {
        match ks.key_type() {
            KeyType::Uniform(config) => {
                // todo - create empty row and fill integer cells on-demand?
                (0..config.cells_per_mutex())
                    .map(|offset| (CellId::Integer(ks.cell_by_location(row, offset)), value))
                    .collect()
            }
            KeyType::PrefixedUniform(_) => RowContainer::new(),
        }
    }

    pub fn iter_cells(&self) -> impl Iterator<Item = &T> {
        self.0
            .iter()
            .flat_map(|ks| ks.iter().flat_map(|row| row.values()))
    }
}

impl LargeTableContainer<SnapshotEntryData> {
    pub fn iter_valid_val_positions(&self) -> impl Iterator<Item = WalPosition> + '_ {
        self.iter_cells().filter_map(|entry| entry.position.valid())
    }

    /// Returns a given percentile across valid wal positions in the container.
    ///
    /// In other words, for a given pct,
    /// returns WalPosition so that pct% of wal positions in the container
    /// are above the returned WalPosition.
    ///
    /// Returns None if the container is empty or does not contain any valid wal positions.
    pub fn pct_wal_position(&self, pct: usize) -> Option<WalPosition> {
        // Collect all valid WAL positions
        let mut positions: Vec<WalPosition> = self.iter_valid_val_positions().collect();

        if positions.is_empty() {
            return None;
        }

        // Sort positions in descending order (higher positions first)
        positions.sort_by(|a, b| b.cmp(a));

        // Calculate the index for the percentile
        // pct% of positions should be above the returned position
        let index = (positions.len() * pct) / 100;

        // Clamp index to valid range
        let index = index.min(positions.len() - 1);

        Some(positions[index])
    }
}

impl Default for LargeTableEntryState {
    fn default() -> Self {
        Self::Empty
    }
}

impl Entries {
    pub fn entry_mut(&mut self, cell_id: &CellId, context: &KsContext) -> &mut LargeTableEntry {
        match self {
            Entries::Array(num_mutexes, arr) => {
                let CellId::Integer(cell) = cell_id else {
                    panic!("Invalid cell id for array entry list: {cell_id:?}");
                };
                let offset = *cell / *num_mutexes;
                arr.get_mut(offset).unwrap()
            }
            Entries::Tree(tree) => {
                let CellId::Bytes(cell) = cell_id else {
                    panic!("Invalid cell id for tree entry list: {cell_id:?}");
                };
                // todo this clones key on every get query - need a fix
                tree.entry(cell.clone()).or_insert_with(|| {
                    let unload_jitter = context
                        .config
                        .gen_dirty_keys_jitter(&mut ThreadRng::default());
                    // todo unify places where LargeTableEntry is created
                    LargeTableEntry::new_empty(context.clone(), cell_id.clone(), unload_jitter)
                })
                // This is ideally what it should look like(but does not work w/ current borrow checker):
                // if let Some(entry) = tree.get_mut(cell) {
                //     entry
                // } else {
                //     let Entry::Vacant(va) =  tree.entry(cell.clone()) else {
                //         unreachable!()
                //     };
                //     let unload_jitter = context.config.gen_dirty_keys_jitter(&mut ThreadRng::default());
                //     let new_entry = LargeTableEntry::new_empty(context.clone(), cell_id.clone(), unload_jitter);
                //     va.insert(new_entry)
                // }
            }
        }
    }

    pub fn try_entry_mut(
        &mut self,
        cell_id: &CellId,
        context: &KsContext,
    ) -> Option<&mut LargeTableEntry> {
        match self {
            Entries::Array(_, _) => Some(self.entry_mut(cell_id, context)),
            Entries::Tree(tree) => {
                let CellId::Bytes(cell) = cell_id else {
                    panic!("Invalid cell id for tree entry list: {cell_id:?}");
                };
                tree.get_mut(cell)
            }
        }
    }

    fn next_tree_cell(
        &self,
        cell: &CellIdBytesContainer,
        reverse: bool,
    ) -> Option<&CellIdBytesContainer> {
        let Entries::Tree(tree) = self else {
            panic!("next_cell_id can only be called on tree entries");
        };
        range_from_excluding::next_key_in_tree(tree, cell, reverse)
    }

    pub fn is_empty(&self) -> bool {
        self.iter().all(LargeTableEntry::is_empty)
    }

    pub fn iter_mut(&mut self) -> Box<dyn Iterator<Item = &mut LargeTableEntry> + '_> {
        match self {
            Entries::Array(_, arr) => {
                let iter = arr.iter_mut();
                Box::new(iter)
            }
            Entries::Tree(tree) => {
                let iter = tree.iter_mut().map(|(_k, v)| v);
                Box::new(iter)
            }
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = &LargeTableEntry> + '_> {
        match self {
            Entries::Array(_, arr) => Box::new(arr.iter()),
            Entries::Tree(tree) => Box::new(tree.values()),
        }
    }
}

#[cfg(not(test))]
#[derive(Default)]
pub(crate) struct LargeTableFailPoints;

#[cfg(test)]
#[derive(Default)]
pub(crate) struct LargeTableFailPoints(pub(crate) parking_lot::RwLock<LargeTableFailPointsInner>);

#[cfg(test)]
#[derive(Default)]
pub(crate) struct LargeTableFailPointsInner {
    pub fp_insert_before_lock: crate::failpoints::FailPoint,
    pub fp_remove_before_lock: crate::failpoints::FailPoint,
}

#[cfg(not(test))]
impl LargeTableFailPoints {
    pub fn fp_insert_before_lock(&self) {}
    pub fn fp_remove_before_lock(&self) {}
}

#[cfg(test)]
impl LargeTableFailPoints {
    pub fn fp_insert_before_lock(&self) {
        self.0.read().fp_insert_before_lock.fp();
    }

    pub fn fp_remove_before_lock(&self) {
        self.0.read().fp_insert_before_lock.fp();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key_shape::{KeyShapeBuilder, KeySpaceConfig};
    use crate::wal::{Wal, WalKind};
    use std::io;

    #[test]
    fn test_ks_allocation() {
        let config = Config::small();
        let mut ks = KeyShapeBuilder::new();
        let a = ks.add_key_space("a", 0, 1, KeyType::uniform(1));
        let b = ks.add_key_space("b", 0, 1, KeyType::uniform(1));
        ks.add_key_space("c", 0, 1, KeyType::uniform(1));
        let ks = ks.build();
        let tmp_dir = tempdir::TempDir::new("test_ks_allocation").unwrap();
        let wal = Wal::open(
            tmp_dir.path(),
            config.wal_layout(WalKind::Replay),
            Metrics::new(),
        )
        .unwrap();
        let l = LargeTable::from_unloaded(
            &ks,
            &LargeTableContainer::new_from_key_shape(&ks, SnapshotEntryData::empty()),
            Arc::new(config),
            IndexFlusher::new_unstarted_for_test(),
            Metrics::new(),
            wal.as_ref(),
        );
        let (mut row, cell) = l.row(ks.ks(a), &[]);
        assert_eq!(row.entry_mut(&cell).context.name(), "a");
        let (mut row, cell) = l.row(ks.ks(b), &[5, 2, 3, 4]);
        assert_eq!(row.entry_mut(&cell).context.name(), "b");
    }

    #[test]
    fn test_bloom_size() {
        let f = BloomFilter::with_rate(0.01, 8000);
        println!("hashes: {}, bytes: {}", f.num_hashes(), f.num_bits() / 8);
    }

    #[test]
    fn test_next_in_cell() {
        // Create a test setup with different entry states
        let metrics = Metrics::new();

        // Create key space with unloaded_iterator enabled
        let config = KeySpaceConfig::default().with_unloaded_iterator(true);
        let (shape, ks_id) = KeyShape::new_single_config(8, 1, KeyType::uniform(1), config);

        let ks = shape.ks(ks_id);
        let context = KsContext {
            ks_config: ks.clone(),
            config: Arc::new(Config::small()),
            metrics: metrics.clone(),
        };

        // A cell ID to use in our tests
        let cell_id = CellId::Integer(0);

        // Create mock loader implementation
        struct MockLoader {
            disk_index: IndexTable,
            serialized_data: Bytes,
        }

        impl Loader for MockLoader {
            type Error = io::Error;

            fn load(
                &self,
                _ks: &KeySpaceDesc,
                _position: WalPosition,
            ) -> Result<IndexTable, Self::Error> {
                Ok(self.disk_index.clone())
            }

            fn index_reader(&self, _position: WalPosition) -> Result<WalRandomRead, Self::Error> {
                Ok(WalRandomRead::Mapped(self.serialized_data.clone()))
            }

            fn flush_supported(&self) -> bool {
                true
            }

            fn flush(&self, _ks: KeySpace, _data: &IndexTable) -> Result<WalPosition, Self::Error> {
                Ok(WalPosition::test_value(100))
            }

            fn last_processed_wal_position(&self) -> u64 {
                // Return a test value for mock
                0
            }

            fn min_wal_position(&self) -> u64 {
                0
            }
        }

        // Create test data for our disk index
        let mut disk_index = IndexTable::default();
        for i in 1..6 {
            let key = (i as u64 * 10).to_be_bytes().to_vec();
            disk_index.insert(Bytes::from(key), WalPosition::test_value(i));
        }
        // We now have keys [10, 20, 30, 40, 50] in the on-disk index

        // Serialize the disk index for our mock reader
        let format = ks.index_format();
        let serialized_data = format.clean_serialize_index(&mut disk_index, ks);

        let loader = MockLoader {
            disk_index: disk_index.clone(),
            serialized_data,
        };

        // TEST CASE 1: Empty state
        {
            // Create a row with an Empty entry
            let mut row = Row {
                value_lru: None,
                context: context.clone(),
                entries: Entries::Array(
                    1,
                    Box::new([LargeTableEntry::new_empty(
                        context.clone(),
                        cell_id.clone(),
                        0,
                    )]),
                ),
            };

            // Test next_in_cell with empty state
            let result =
                LargeTable::next_in_cell(&loader, &mut row, &cell_id, None, false).unwrap();
            assert!(result.is_none(), "Empty state should return None");
        }

        // TEST CASE 2: Loaded state
        {
            // Create a row with a Loaded entry
            let mut entry = LargeTableEntry::new_empty(context.clone(), cell_id.clone(), 0);
            entry.data = ArcCow::new_owned(disk_index.clone());
            entry.state = LargeTableEntryState::Loaded(WalPosition::test_value(42));

            let mut row = Row {
                value_lru: None,
                context: context.clone(),
                entries: Entries::Array(1, Box::new([entry])),
            };

            // Test forward iteration with loaded state
            let result =
                LargeTable::next_in_cell(&loader, &mut row, &cell_id, None, false).unwrap();
            assert!(result.is_some(), "Loaded state should return first entry");
            let (key, pos) = result.unwrap();
            assert_eq!(key.as_ref(), 10_u64.to_be_bytes());
            assert_eq!(pos, WalPosition::test_value(1));

            // Test forward iteration from a key
            let result = LargeTable::next_in_cell(
                &loader,
                &mut row,
                &cell_id,
                Some(Bytes::from(20_u64.to_be_bytes().to_vec())),
                false,
            )
            .unwrap();
            assert!(result.is_some());
            let (key, pos) = result.unwrap();
            assert_eq!(key.as_ref(), 30_u64.to_be_bytes());
            assert_eq!(pos, WalPosition::test_value(3));

            // Test backward iteration
            let result = LargeTable::next_in_cell(&loader, &mut row, &cell_id, None, true).unwrap();
            assert!(result.is_some());
            let (key, pos) = result.unwrap();
            assert_eq!(key.as_ref(), 50_u64.to_be_bytes());
            assert_eq!(pos, WalPosition::test_value(5));
        }

        // TEST CASE 3: Unloaded state
        {
            // Create a row with an Unloaded entry
            let entry = LargeTableEntry::new_unloaded(
                context.clone(),
                cell_id.clone(),
                WalPosition::test_value(42),
                0,
                None,
            );

            let mut row = Row {
                value_lru: None,
                context: context.clone(),
                entries: Entries::Array(1, Box::new([entry])),
            };

            // Test forward iteration with unloaded state
            let result =
                LargeTable::next_in_cell(&loader, &mut row, &cell_id, None, false).unwrap();
            assert!(
                result.is_some(),
                "Unloaded state should return first entry from disk"
            );
            let (key, pos) = result.unwrap();
            assert_eq!(key.as_ref(), 10_u64.to_be_bytes());
            assert_eq!(pos, WalPosition::test_value(1));

            // Test forward iteration from a key
            let result = LargeTable::next_in_cell(
                &loader,
                &mut row,
                &cell_id,
                Some(Bytes::from(20_u64.to_be_bytes().to_vec())),
                false,
            )
            .unwrap();
            assert!(result.is_some());
            let (key, pos) = result.unwrap();
            assert_eq!(key.as_ref(), 30_u64.to_be_bytes());
            assert_eq!(pos, WalPosition::test_value(3));
        }

        // TEST CASE 4: DirtyLoaded state
        {
            // Create a row with a DirtyLoaded entry
            let mut entry = LargeTableEntry::new_empty(context.clone(), cell_id.clone(), 0);
            entry.data = ArcCow::new_owned(disk_index.clone());

            // Add a new key and mark one as deleted
            entry.data.make_mut().insert(
                Bytes::from(15_u64.to_be_bytes().to_vec()),
                WalPosition::test_value(15),
            );
            entry.data.make_mut().remove(
                Bytes::from(40_u64.to_be_bytes().to_vec()),
                WalPosition::test_value(18),
            ); // Deleted key

            entry.state = LargeTableEntryState::DirtyLoaded(WalPosition::test_value(42));

            let mut row = Row {
                value_lru: None,
                context: context.clone(),
                entries: Entries::Array(1, Box::new([entry])),
            };

            // Test iteration with deleted key
            let result = LargeTable::next_in_cell(
                &loader,
                &mut row,
                &cell_id,
                Some(Bytes::from(30_u64.to_be_bytes().to_vec())),
                false,
            )
            .unwrap();
            assert!(result.is_some());
            let (key, pos) = result.unwrap();
            assert_eq!(key.as_ref(), 50_u64.to_be_bytes()); // Should skip over the deleted 40
            assert_eq!(pos, WalPosition::test_value(5));

            // Test finding the new key
            let result = LargeTable::next_in_cell(
                &loader,
                &mut row,
                &cell_id,
                Some(Bytes::from(10_u64.to_be_bytes().to_vec())),
                false,
            )
            .unwrap();
            assert!(result.is_some());
            let (key, pos) = result.unwrap();
            assert_eq!(key.as_ref(), 15_u64.to_be_bytes()); // Should find our new key
            assert_eq!(pos, WalPosition::test_value(15));
        }

        // TEST CASE 5: DirtyUnloaded state
        {
            // Create a row with a DirtyUnloaded entry
            let mut entry = LargeTableEntry::new_unloaded(
                context.clone(),
                cell_id.clone(),
                WalPosition::test_value(42),
                0,
                None,
            );

            // Add a new key (in-memory but marked dirty)
            entry.data.make_mut().insert(
                Bytes::from(15_u64.to_be_bytes().to_vec()),
                WalPosition::test_value(15),
            );
            // Add a deleted key (in-memory but marked as deleted)
            entry.data.make_mut().remove(
                Bytes::from(40_u64.to_be_bytes().to_vec()),
                WalPosition::test_value(16),
            );

            entry.state = LargeTableEntryState::DirtyUnloaded(WalPosition::test_value(42));

            let mut row = Row {
                value_lru: None,
                context: context.clone(),
                entries: Entries::Array(1, Box::new([entry])),
            };

            // Test finding the in-memory added key
            let result = LargeTable::next_in_cell(
                &loader,
                &mut row,
                &cell_id,
                Some(Bytes::from(10_u64.to_be_bytes().to_vec())),
                false,
            )
            .unwrap();
            assert!(result.is_some());
            let (key, pos) = result.unwrap();
            assert_eq!(key.as_ref(), 15_u64.to_be_bytes());
            assert_eq!(pos, WalPosition::test_value(15));

            // Test skipping the in-memory deleted key and finding the next on-disk key
            let result = LargeTable::next_in_cell(
                &loader,
                &mut row,
                &cell_id,
                Some(Bytes::from(30_u64.to_be_bytes().to_vec())),
                false,
            )
            .unwrap();
            assert!(result.is_some());
            let (key, pos) = result.unwrap();
            assert_eq!(key.as_ref(), 50_u64.to_be_bytes()); // Should skip over the deleted 40
            assert_eq!(pos, WalPosition::test_value(5));
        }
    }

    #[test]
    fn test_pct_wal_position() {
        // Create a container with various WAL positions
        let mut entries = RowContainer::new();
        for i in 0..5 {
            let position_value = (i + 1) * 100;
            entries.insert(
                CellId::Integer(i),
                SnapshotEntryData {
                    position: WalPosition::test_value(position_value as u64),
                    last_processed: position_value as u64,
                },
            );
        }
        let container = LargeTableContainer(vec![vec![entries]]);

        // Test various percentiles
        // 0% - all positions are above this, should return the highest position
        assert_eq!(
            container.pct_wal_position(0),
            Some(WalPosition::test_value(500))
        );

        // 20% - 20% of positions (1 out of 5) should be above this
        // Positions sorted: [500, 400, 300, 200, 100]
        // Index = 5 * 20 / 100 = 1, so position at index 1 is 400
        assert_eq!(
            container.pct_wal_position(20),
            Some(WalPosition::test_value(400))
        );

        // 50% - 50% of positions should be above this (median)
        // Index = 5 * 50 / 100 = 2, so position at index 2 is 300
        assert_eq!(
            container.pct_wal_position(50),
            Some(WalPosition::test_value(300))
        );

        // 80% - 80% of positions should be above this
        // Index = 5 * 80 / 100 = 4, so position at index 4 is 100
        assert_eq!(
            container.pct_wal_position(80),
            Some(WalPosition::test_value(100))
        );

        // 100% - all positions should be above this, but clamped to last valid index
        // Index = 5 * 100 / 100 = 5, clamped to 4, so position is 100
        assert_eq!(
            container.pct_wal_position(100),
            Some(WalPosition::test_value(100))
        );

        // Test with empty container
        let empty_container = LargeTableContainer::<SnapshotEntryData>(vec![]);
        assert_eq!(empty_container.pct_wal_position(50), None);

        // Test with container having only invalid positions
        let invalid_container = LargeTableContainer(vec![vec![vec![(
            CellId::Integer(0),
            SnapshotEntryData {
                position: WalPosition::INVALID,
                last_processed: 0,
            },
        )]
        .into_iter()
        .collect()]]);
        assert_eq!(invalid_container.pct_wal_position(50), None);
    }
}
