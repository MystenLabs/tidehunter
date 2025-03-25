use crate::cell::{CellId, CellIdBytesContainer};
use crate::config::Config;
use crate::context::KsContext;
use crate::flusher::{FlushKind, IndexFlusher};
use crate::index::index_format::IndexFormat;
use crate::index::index_table::IndexTable;
use crate::index::INDEX_FORMAT;
use crate::key_shape::{KeyShape, KeySpace, KeySpaceDesc, KeyType};
use crate::metrics::Metrics;
use crate::primitives::arc_cow::ArcCow;
use crate::primitives::sharded_mutex::ShardedMutex;
use crate::wal::{WalPosition, WalRandomRead};
use bloom::{BloomFilter, ASMS};
use lru::LruCache;
use minibytes::Bytes;
use parking_lot::MutexGuard;
use rand::rngs::ThreadRng;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use std::{cmp, mem};

pub struct LargeTable {
    table: Vec<KsTable>,
    config: Arc<Config>,
    flusher: IndexFlusher,
    metrics: Arc<Metrics>,
}

pub struct LargeTableEntry {
    cell: CellId,
    data: ArcCow<IndexTable>,
    last_added_position: Option<WalPosition>,
    state: LargeTableEntryState,
    context: KsContext,
    bloom_filter: Option<BloomFilter>,
    unload_jitter: usize,
    flush_pending: bool,
}

enum LargeTableEntryState {
    Empty,
    Unloaded(WalPosition),
    Loaded(WalPosition),
    DirtyUnloaded(WalPosition, HashSet<Bytes>),
    DirtyLoaded(WalPosition, HashSet<Bytes>),
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
    #[allow(dead_code)]
    Tree(BTreeMap<CellIdBytesContainer, LargeTableEntry>),
}

pub(crate) type RowContainer<T> = BTreeMap<CellId, T>;

/// ks -> row -> cell
#[derive(Serialize, Deserialize)]
pub(crate) struct LargeTableContainer<T>(pub Vec<Vec<RowContainer<T>>>);

pub(crate) struct LargeTableSnapshot {
    pub data: LargeTableContainer<WalPosition>,
    pub last_added_position: WalPosition,
}

impl LargeTable {
    pub fn from_unloaded(
        key_shape: &KeyShape,
        snapshot: &LargeTableContainer<WalPosition>,
        config: Arc<Config>,
        flusher: IndexFlusher,
        metrics: Arc<Metrics>,
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
                let rows = ks_snapshot.iter().map(|row_snapshot| {
                    let context = KsContext {
                        ks_config: ks.clone(),
                        config: config.clone(),
                        metrics: metrics.clone(),
                    };
                    let entries = row_snapshot.iter().map(|(cell, position)| {
                        let unload_jitter = config.gen_dirty_keys_jitter(&mut rng);
                        LargeTableEntry::from_snapshot_position(
                            context.clone(),
                            cell.clone(),
                            position,
                            unload_jitter,
                        )
                    });
                    let entries = match ks.key_type() {
                        KeyType::Uniform => Entries::Array(ks.num_mutexes(), entries.collect()),
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
        }
    }

    pub fn insert<L: Loader>(
        &self,
        ks: &KeySpaceDesc,
        k: Bytes,
        v: WalPosition,
        value: &Bytes,
        loader: &L,
    ) -> Result<(), L::Error> {
        let (mut row, cell) = self.row(ks, &k);
        if let Some(value_lru) = &mut row.value_lru {
            let delta: i64 = (k.len() + value.len()) as i64;
            let previous = value_lru.push(k.clone(), value.clone());
            self.update_lru_metric(ks, previous, delta);
        }
        let entry = row.entry_mut(&cell);
        entry.insert(k, v);
        let index_size = entry.data.len();
        if loader.flush_supported() && self.too_many_dirty(entry) {
            entry.unload_if_ks_enabled(&self.flusher);
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
        v: WalPosition,
        _loader: &L,
    ) -> Result<(), L::Error> {
        let (mut row, cell) = self.row(ks, &k);
        if let Some(value_lru) = &mut row.value_lru {
            let previous = value_lru.pop(&k);
            self.update_lru_metric(ks, previous.map(|v| (k.clone(), v)), 0);
        }
        let entry = row.entry_mut(&cell);
        entry.remove(k, v);
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
            .with_label_values(&[&ks.name()])
            .add(delta);
    }

    pub fn get<L: Loader>(
        &self,
        ks: &KeySpaceDesc,
        k: &[u8],
        loader: &L,
    ) -> Result<GetResult, L::Error> {
        let (mut row, cell) = self.row(ks, k);
        if let Some(value_lru) = &mut row.value_lru {
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
            LargeTableEntryState::DirtyLoaded(_, _) => {
                return Ok(self.report_lookup_result(ks, entry.get(k), "cache"))
            }
            LargeTableEntryState::DirtyUnloaded(position, _) => {
                // optimization: in dirty unloaded state we might not need to load entry
                if let Some(found) = entry.get(k) {
                    return Ok(self.report_lookup_result(ks, found.valid(), "cache"));
                }
                position
            }
            LargeTableEntryState::Unloaded(position) => position,
        };
        // todo move tokio dep under a feature
        let now = Instant::now();
        let index_reader = loader.index_reader(index_position)?;
        // todo - consider only doing block_in_place for the syscall random reader
        let result =
            tokio::task::block_in_place(|| INDEX_FORMAT.lookup_unloaded(ks, &index_reader, k));
        self.metrics
            .lookup_mcs
            .with_label_values(&[index_reader.kind_str(), entry.context.name()])
            .observe(now.elapsed().as_micros() as f64);
        Ok(self.report_lookup_result(ks, result, "lookup"))
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
        if let Some(dk) = entry.state.dirty_keys() {
            self.config
                .excess_dirty_keys(dk.len().saturating_sub(entry.unload_jitter))
        } else {
            false
        }
    }

    fn row(&self, ks: &KeySpaceDesc, k: &[u8]) -> (MutexGuard<'_, Row>, CellId) {
        let ks_table = self.ks_table(ks);
        let (mutex, cell) = ks.location_for_key(k);
        let row = ks_table.lock(
            mutex,
            &self
                .metrics
                .large_table_contention
                .with_label_values(&[ks.name()]),
        );
        (row, cell)
    }

    fn ks_table(&self, ks: &KeySpaceDesc) -> &ShardedMutex<Row> {
        &self
            .table
            .get(ks.id().as_usize())
            .expect("Table not found for ks")
            .rows
    }

    /// Provides a snapshot of this large table along with replay position in the wal for the snapshot.
    pub fn snapshot<L: Loader>(
        &self,
        tail_position: u64,
        loader: &L,
    ) -> Result<LargeTableSnapshot, L::Error> {
        assert!(loader.flush_supported());
        // See ks_snapshot documentation for details
        // on how snapshot replay position is determined.
        let mut replay_from = None;
        let mut max_position: Option<WalPosition> = None;
        let mut data = Vec::with_capacity(self.table.len());
        for ks_table in self.table.iter() {
            let (ks_data, ks_replay_from, ks_max_position) =
                self.ks_snapshot(ks_table, tail_position, loader)?;
            data.push(ks_data);
            if let Some(ks_replay_from) = ks_replay_from {
                replay_from = Some(cmp::min(
                    replay_from.unwrap_or(WalPosition::MAX),
                    ks_replay_from,
                ));
            }
            if let Some(ks_max_position) = ks_max_position {
                max_position = Some(if let Some(max_position) = max_position {
                    cmp::max(ks_max_position, max_position)
                } else {
                    ks_max_position
                });
            }
        }

        let replay_from =
            replay_from.unwrap_or_else(|| max_position.unwrap_or(WalPosition::INVALID));

        let data = LargeTableContainer(data);
        Ok(LargeTableSnapshot {
            data,
            last_added_position: replay_from,
        })
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
    fn ks_snapshot<L: Loader>(
        &self,
        ks_table: &KsTable,
        tail_position: u64,
        loader: &L,
    ) -> Result<
        (
            Vec<RowContainer<WalPosition>>,
            Option<WalPosition>,
            Option<WalPosition>,
        ),
        L::Error,
    > {
        let mut replay_from = None;
        let mut max_wal_position: Option<WalPosition> = None;
        let mut ks_data = Vec::with_capacity(ks_table.rows.mutexes().len());
        for mutex in ks_table.rows.mutexes() {
            let mut row = mutex.lock();
            let mut row_data = RowContainer::new();
            for (key, entry) in row.entries.iter_mut() {
                entry.maybe_unload_for_snapshot(loader, &self.config, tail_position)?;
                let position = entry.state.wal_position();
                row_data.insert(key, position);
                if let Some(valid_position) = position.valid() {
                    max_wal_position = if let Some(max_wal_position) = max_wal_position {
                        Some(cmp::max(max_wal_position, valid_position))
                    } else {
                        Some(valid_position)
                    };
                }
                if entry.state.is_dirty() {
                    replay_from = Some(cmp::min(replay_from.unwrap_or(WalPosition::MAX), position));
                }
            }
            ks_data.push(row_data);
        }
        let metric = self
            .metrics
            .index_distance_from_tail
            .with_label_values(&[&ks_table.ks.name()]);
        match replay_from {
            None => metric.set(0),
            Some(WalPosition::INVALID) => metric.set(-1),
            Some(position) => {
                // This can go below 0 because tail_position is not synchronized
                // and index position can be higher than the tail_position in rare cases
                metric.set(tail_position.saturating_sub(position.as_u64()) as i64)
            }
        }
        Ok((ks_data, replay_from, max_wal_position))
    }

    /// Takes a next entry in the large table.
    ///
    /// See Db::next_entry for documentation.
    pub fn next_entry<L: Loader>(
        &self,
        ks: &KeySpaceDesc,
        mut cell: CellId,
        mut next_key: Option<Bytes>,
        loader: &L,
        end_cell_exclusive: &Option<CellId>,
        reverse: bool,
    ) -> Result<
        Option<(
            Option<CellId>, /*next cell*/
            Option<Bytes>,  /*next key*/
            Bytes,          /*fetched key*/
            WalPosition,    /*fetched value*/
        )>,
        L::Error,
    > {
        let ks_table = self.ks_table(ks);
        loop {
            let row = ks.mutex_for_cell(&cell);
            let mut row = ks_table.lock(
                row,
                &self
                    .metrics
                    .large_table_contention
                    .with_label_values(&[ks.name()]),
            );
            if let Some((key, value, next_key)) =
                self.next_in_cell(loader, &mut row, &cell, next_key, reverse)?
            {
                let next_cell = if next_key.is_none() {
                    ks.next_cell(cell, reverse)
                } else {
                    Some(cell)
                };
                return Ok(Some((next_cell, next_key, key, value)));
            } else {
                next_key = None;
                let Some(next_cell) = ks.next_cell(cell, reverse) else {
                    return Ok(None);
                };
                if let Some(end_cell_exclusive) = end_cell_exclusive {
                    if reverse {
                        if &next_cell <= end_cell_exclusive {
                            return Ok(None);
                        }
                    } else {
                        if &next_cell >= end_cell_exclusive {
                            return Ok(None);
                        }
                    }
                }
                cell = next_cell;
            }
        }
    }

    fn next_in_cell<L: Loader>(
        &self,
        loader: &L,
        row: &mut Row,
        cell: &CellId,
        next_key: Option<Bytes>,
        reverse: bool,
    ) -> Result<Option<(Bytes, WalPosition, Option<Bytes>)>, L::Error> {
        let Some(entry) = row.try_entry_mut(cell) else {
            return Ok(None);
        };
        // todo read from disk instead of loading
        entry.maybe_load(loader)?;
        Ok(entry.next_entry(next_key, reverse))
    }

    /// See Db::last_in_range for documentation.
    pub fn last_in_range<L: Loader>(
        &self,
        ks: &KeySpaceDesc,
        cell: &CellId,
        from_included: &Bytes,
        to_included: &Bytes,
        loader: &L,
    ) -> Result<Option<(Bytes, WalPosition)>, L::Error> {
        // todo duplicate code with next_entry(...)
        let row = ks.mutex_for_cell(&cell);
        let ks_table = self.ks_table(ks);
        let mut row = ks_table.lock(
            row,
            &self
                .metrics
                .large_table_contention
                .with_label_values(&[ks.name()]),
        );
        // todo use try_entry_mut
        let entry = row.entry_mut(cell);
        // todo read from disk instead of loading
        entry.maybe_load(loader)?;
        // todo make sure can't have dirty markers in index in this state
        Ok(entry.data.last_in_range(from_included, to_included))
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
        let row = ks.mutex_for_cell(&cell);
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
    pub(crate) fn is_all_clean(&self) -> bool {
        for ks_table in &self.table {
            for mutex in ks_table.rows.mutexes() {
                let mut lock = mutex.lock();
                for (_, entry) in lock.entries.iter_mut() {
                    if entry.state.as_dirty_state().is_some() {
                        return false;
                    }
                }
            }
        }
        true
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
    type Error;

    fn load(&self, ks: &KeySpaceDesc, position: WalPosition) -> Result<IndexTable, Self::Error>;

    fn index_reader(&self, position: WalPosition) -> Result<WalRandomRead, Self::Error>;

    fn flush_supported(&self) -> bool;

    fn flush(&self, ks: KeySpace, data: &IndexTable) -> Result<WalPosition, Self::Error>;
}

impl LargeTableEntry {
    pub fn new_unloaded(
        context: KsContext,
        cell: CellId,
        position: WalPosition,
        unload_jitter: usize,
    ) -> Self {
        Self::new_with_state(
            context,
            cell,
            LargeTableEntryState::Unloaded(position),
            unload_jitter,
        )
    }

    pub fn new_empty(context: KsContext, cell: CellId, unload_jitter: usize) -> Self {
        Self::new_with_state(context, cell, LargeTableEntryState::Empty, unload_jitter)
    }

    fn new_with_state(
        context: KsContext,
        cell: CellId,
        state: LargeTableEntryState,
        unload_jitter: usize,
    ) -> Self {
        let bloom_filter = context
            .ks_config
            .bloom_filter()
            .map(|params| BloomFilter::with_rate(params.rate, params.count));
        Self {
            context,
            cell,
            state,
            data: Default::default(),
            last_added_position: Default::default(),
            bloom_filter,
            unload_jitter,
            flush_pending: false,
        }
    }

    pub fn from_snapshot_position(
        context: KsContext,
        cell: CellId,
        position: &WalPosition,
        unload_jitter: usize,
    ) -> Self {
        if position == &WalPosition::INVALID {
            LargeTableEntry::new_empty(context, cell, unload_jitter)
        } else {
            LargeTableEntry::new_unloaded(context, cell, *position, unload_jitter)
        }
    }

    pub fn insert(&mut self, k: Bytes, v: WalPosition) {
        let dirty_state = self.state.mark_dirty();
        dirty_state.into_dirty_keys().insert(k.clone());
        self.insert_bloom_filter(&k);
        let previous = self.data.make_mut().insert(k, v);
        self.report_loaded_keys_change(previous, Some(v));
        self.last_added_position = Some(v);
    }

    pub fn remove(&mut self, k: Bytes, v: WalPosition) {
        let dirty_state = self.state.mark_dirty();
        let (previous, new) = match dirty_state {
            DirtyState::Loaded(dirty_keys) => {
                let previous = self.data.make_mut().remove(&k);
                dirty_keys.insert(k);
                (previous, None)
            }
            DirtyState::Unloaded(dirty_keys) => {
                // We could just use dirty_keys and not use WalPosition::INVALID as a marker.
                // In that case, however, we would need to clone and pass dirty_keys to a snapshot.
                let previous = self.data.make_mut().insert(k.clone(), WalPosition::INVALID);
                dirty_keys.insert(k);
                (previous, Some(WalPosition::INVALID))
            }
        };
        self.report_loaded_keys_change(previous, new);
        self.last_added_position = Some(v);
    }

    fn report_loaded_keys_delta(&self, delta: i64) {
        self.context
            .metrics
            .loaded_keys
            .with_label_values(&[self.context.name()])
            .add(delta);
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

    fn report_loaded_keys_change(&self, old: Option<WalPosition>, new: Option<WalPosition>) {
        let delta = match (old, new) {
            (None, None) => return,
            (Some(_), Some(_)) => return,
            (Some(_), None) => -1,
            (None, Some(_)) => 1,
        };
        self.report_loaded_keys_delta(delta);
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
        next_key: Option<Bytes>,
        reverse: bool,
    ) -> Option<(Bytes, WalPosition, Option<Bytes>)> {
        if matches!(&self.state, LargeTableEntryState::Unloaded(_)) {
            panic!("Can't next_entry in unloaded state");
        }
        self.data.next_entry(next_key, reverse)
    }

    pub fn maybe_load<L: Loader>(&mut self, loader: &L) -> Result<(), L::Error> {
        let Some((state, position)) = self.state.as_unloaded_state() else {
            return Ok(());
        };
        let mut data = loader.load(&self.context.ks_config, position)?;
        let dirty_keys = match state {
            UnloadedState::Dirty(dirty_keys) => {
                data.merge_dirty(&self.data);
                Some(mem::take(dirty_keys))
            }
            UnloadedState::Clean => None,
        };
        self.report_loaded_keys_delta(data.len() as i64 - self.data.len() as i64);
        self.data = ArcCow::new_owned(data);
        if let Some(dirty_keys) = dirty_keys {
            self.state = LargeTableEntryState::DirtyLoaded(position, dirty_keys);
        } else {
            self.state = LargeTableEntryState::Loaded(position);
        }
        Ok(())
    }

    pub fn maybe_unload_for_snapshot<L: Loader>(
        &mut self,
        loader: &L,
        config: &Config,
        tail_position: u64,
    ) -> Result<(), L::Error> {
        if !self.state.is_dirty() {
            return Ok(());
        }
        if self.flush_pending {
            // todo metric / log?
            return Ok(());
        }
        let position = self.state.wal_position();
        // position can actually be great then tail_position due to concurrency
        let distance = tail_position.saturating_sub(position.as_u64());
        if distance >= config.snapshot_unload_threshold {
            self.context
                .metrics
                .snapshot_force_unload
                .with_label_values(&[self.context.name()])
                .inc();
            // todo - we don't need to unload here, only persist the entry,
            // Just reusing unload logic for now.
            self.unload(loader, config, true)?;
        }
        Ok(())
    }

    pub fn unload_if_ks_enabled(&mut self, flusher: &IndexFlusher) {
        if self.context.ks_config.unloading_disabled() {
            return;
        }
        if !self.flush_pending {
            self.flush_pending = true;
            let flush_kind = self
                .flush_kind()
                .expect("unload_if_ks_enabled is called in clean state");
            println!("Flushing {:?}", self.cell);
            flusher.request_flush(self.context.id(), self.cell.clone(), flush_kind);
        }
    }

    pub fn update_flushed_index(&mut self, original_index: Arc<IndexTable>, position: WalPosition) {
        println!("update_flushed_index {:?}", self.cell);
        assert!(
            self.flush_pending,
            "update_merged_index called while flush_pending is not set"
        );
        match self.state {
            LargeTableEntryState::DirtyUnloaded(_, _) => {}
            LargeTableEntryState::DirtyLoaded(_, _) => {}
            LargeTableEntryState::Empty => panic!("update_merged_index in Empty state"),
            LargeTableEntryState::Unloaded(_) => panic!("update_merged_index in Unloaded state"),
            LargeTableEntryState::Loaded(_) => panic!("update_merged_index in Loaded state"),
        }
        self.flush_pending = false;
        if self.data.same_shared(&original_index) {
            self.report_loaded_keys_delta(-(self.data.len() as i64));
            self.data = Default::default();
            self.state = LargeTableEntryState::Unloaded(position);
            self.context
                .metrics
                .flush_update
                .with_label_values(&["clear"])
                .inc();
        } else {
            let delta = self.data.make_mut().unmerge_flushed(&original_index);
            self.report_loaded_keys_delta(delta);
            // todo remove dirty_keys from DirtyUnloaded
            let dirty_keys = self.data.data.keys().cloned().collect();
            self.state = LargeTableEntryState::DirtyUnloaded(position, dirty_keys);
            self.context
                .metrics
                .flush_update
                .with_label_values(&["unmerge"])
                .inc();
        }
    }

    fn unload<L: Loader>(
        &mut self,
        loader: &L,
        config: &Config,
        force_clean: bool,
    ) -> Result<(), L::Error> {
        match &self.state {
            LargeTableEntryState::Empty => {}
            LargeTableEntryState::Unloaded(_) => {}
            LargeTableEntryState::Loaded(pos) => {
                self.context
                    .metrics
                    .unload
                    .with_label_values(&["clean"])
                    .inc();
                self.state = LargeTableEntryState::Unloaded(*pos);
                self.report_loaded_keys_delta(-(self.data.len() as i64));
                self.data = Default::default();
            }
            LargeTableEntryState::DirtyUnloaded(_pos, _dirty_keys) => {
                // load, merge, flush and unload -> Unloaded(..)
                self.context
                    .metrics
                    .unload
                    .with_label_values(&["merge_flush"])
                    .inc();
                self.maybe_load(loader)?;
                assert!(matches!(
                    self.state,
                    LargeTableEntryState::DirtyLoaded(_, _)
                ));
                self.unload_dirty_loaded(loader)?;
            }
            LargeTableEntryState::DirtyLoaded(position, dirty_keys) => {
                // todo - this position can be invalid
                if force_clean || config.excess_dirty_keys(dirty_keys.len()) {
                    self.context
                        .metrics
                        .unload
                        .with_label_values(&["flush"])
                        .inc();
                    // either (a) flush and unload -> Unloaded(..)
                    // small code duplicate between here and unload_dirty_unloaded
                    self.unload_dirty_loaded(loader)?;
                } else {
                    self.context
                        .metrics
                        .unload
                        .with_label_values(&["unmerge"])
                        .inc();
                    // or (b) unmerge and unload -> DirtyUnloaded(..)
                    /*todo - avoid cloning dirty_keys, especially twice*/
                    let delta = self.data.make_mut().make_dirty(dirty_keys.clone());
                    self.report_loaded_keys_delta(delta);
                    self.state = LargeTableEntryState::DirtyUnloaded(*position, dirty_keys.clone());
                }
            }
        }
        // if let LargeTableEntryState::Loaded(position) = self.state {
        //     self.state = LargeTableEntryState::Unloaded(position);
        //     self.data.clear();
        // } else if let LargeTableEntryState::Dirty(_) = self.state {
        //     let position = loader.unload(&self.data)?;
        //     // todo trigger re-index to cap memory during restart?
        //     self.state = LargeTableEntryState::Unloaded(position);
        //     self.data.clear();
        // }
        Ok(())
    }

    fn unload_dirty_loaded<L: Loader>(&mut self, loader: &L) -> Result<(), L::Error> {
        self.run_compactor();
        let position = loader.flush(self.context.id(), &self.data)?;
        self.state = LargeTableEntryState::Unloaded(position);
        self.report_loaded_keys_delta(-(self.data.len() as i64));
        self.data = Default::default();
        Ok(())
    }

    fn run_compactor(&mut self) {
        if let Some(compactor) = self.context.ks_config.compactor() {
            let index = self.data.make_mut();
            let pre_compact_len = index.len();
            compactor(&mut index.data);
            let compacted = pre_compact_len.saturating_sub(index.len());
            self.context
                .metrics
                .compacted_keys
                .with_label_values(&[self.context.name()])
                .inc_by(compacted as u64);
            self.report_loaded_keys_delta(-(compacted as i64));
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.state, LargeTableEntryState::Empty)
    }

    pub fn flush_kind(&mut self) -> Option<FlushKind> {
        match self.state {
            LargeTableEntryState::Empty => None,
            LargeTableEntryState::Unloaded(_) => None,
            LargeTableEntryState::Loaded(_) => None,
            LargeTableEntryState::DirtyUnloaded(position, _) => {
                Some(FlushKind::MergeUnloaded(position, self.data.clone_shared()))
            }
            LargeTableEntryState::DirtyLoaded(_, _) => {
                Some(FlushKind::FlushLoaded(self.data.clone_shared()))
            }
        }
    }
}

impl LargeTableEntryState {
    pub fn mark_dirty(&mut self) -> DirtyState {
        match self {
            LargeTableEntryState::Empty => {
                *self = LargeTableEntryState::DirtyLoaded(WalPosition::INVALID, Default::default())
            }
            LargeTableEntryState::Loaded(position) => {
                *self = LargeTableEntryState::DirtyLoaded(*position, Default::default())
            }
            LargeTableEntryState::DirtyLoaded(_, _) => {}
            LargeTableEntryState::Unloaded(pos) => {
                *self = LargeTableEntryState::DirtyUnloaded(*pos, HashSet::default())
            }
            LargeTableEntryState::DirtyUnloaded(_, _) => {}
        }
        self.as_dirty_state()
            .expect("mark_dirty sets state to one of dirty states")
    }

    pub fn as_dirty_state(&mut self) -> Option<DirtyState> {
        match self {
            LargeTableEntryState::Empty => None,
            LargeTableEntryState::Unloaded(_) => None,
            LargeTableEntryState::Loaded(_) => None,
            LargeTableEntryState::DirtyUnloaded(_, dirty_keys) => {
                Some(DirtyState::Unloaded(dirty_keys))
            }
            LargeTableEntryState::DirtyLoaded(_, dirty_keys) => {
                Some(DirtyState::Loaded(dirty_keys))
            }
        }
    }

    pub fn as_unloaded_state(&mut self) -> Option<(UnloadedState, WalPosition)> {
        match self {
            LargeTableEntryState::Empty => None,
            LargeTableEntryState::Unloaded(pos) => Some((UnloadedState::Clean, *pos)),
            LargeTableEntryState::Loaded(_) => None,
            LargeTableEntryState::DirtyUnloaded(pos, dirty_keys) => {
                Some((UnloadedState::Dirty(dirty_keys), *pos))
            }
            LargeTableEntryState::DirtyLoaded(_, _) => None,
        }
    }

    pub fn dirty_keys(&mut self) -> Option<&mut HashSet<Bytes>> {
        Some(self.as_dirty_state()?.into_dirty_keys())
    }

    /// Wal position of the persisted index entry.
    pub fn wal_position(&self) -> WalPosition {
        match self {
            LargeTableEntryState::Empty => WalPosition::INVALID,
            LargeTableEntryState::Unloaded(w) => *w,
            LargeTableEntryState::Loaded(w) => *w,
            LargeTableEntryState::DirtyUnloaded(w, _) => *w,
            LargeTableEntryState::DirtyLoaded(w, _) => *w,
        }
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
            LargeTableEntryState::DirtyUnloaded(_, _) => true,
            LargeTableEntryState::DirtyLoaded(_, _) => true,
        }
    }

    #[allow(dead_code)]
    pub fn name(&self) -> &'static str {
        match self {
            LargeTableEntryState::Empty => "empty",
            LargeTableEntryState::Unloaded(_) => "unloaded",
            LargeTableEntryState::Loaded(_) => "loaded",
            LargeTableEntryState::DirtyUnloaded(_, _) => "dirty_unloaded",
            LargeTableEntryState::DirtyLoaded(_, _) => "dirty_loaded",
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

impl<'a> DirtyState<'a> {
    pub fn into_dirty_keys(self) -> &'a mut HashSet<Bytes> {
        match self {
            DirtyState::Loaded(dirty_keys) => dirty_keys,
            DirtyState::Unloaded(dirty_keys) => dirty_keys,
        }
    }
}

enum DirtyState<'a> {
    Loaded(&'a mut HashSet<Bytes>),
    Unloaded(&'a mut HashSet<Bytes>),
}

enum UnloadedState<'a> {
    Dirty(&'a mut HashSet<Bytes>),
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
            KeyType::Uniform => {
                // todo - create empty row and fill integer cells on-demand?
                (0..ks.cells_per_mutex())
                    .map(|offset| (CellId::Integer(ks.cell_by_location(row, offset)), value))
                    .collect()
            }
            KeyType::PrefixedUniform(_) => RowContainer::new(),
        }
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

    pub fn is_empty(&self) -> bool {
        self.iter().all(LargeTableEntry::is_empty)
    }

    pub fn iter_mut(&mut self) -> Box<dyn Iterator<Item = (CellId, &mut LargeTableEntry)> + '_> {
        match self {
            Entries::Array(_, arr) => {
                let iter = arr
                    .iter_mut()
                    .enumerate()
                    .map(|(i, e)| (CellId::Integer(i), e));
                Box::new(iter)
            }
            Entries::Tree(tree) => {
                let iter = tree.iter_mut().map(|(k, v)| (CellId::Bytes(k.clone()), v));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key_shape::KeyShapeBuilder;

    #[test]
    fn test_ks_allocation() {
        let config = Config::small();
        let mut ks = KeyShapeBuilder::new();
        let a = ks.add_key_space("a", 0, 1, 1);
        let b = ks.add_key_space("b", 0, 1, 1);
        ks.add_key_space("c", 0, 1, 1);
        let ks = ks.build();
        let l = LargeTable::from_unloaded(
            &ks,
            &LargeTableContainer::new_from_key_shape(&ks, WalPosition::INVALID),
            Arc::new(config),
            IndexFlusher::new_unstarted_for_test(),
            Metrics::new(),
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
}
