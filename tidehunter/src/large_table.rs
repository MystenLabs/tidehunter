use crate::cell::{CellId, CellIdBytesContainer};
use crate::config::Config;
use crate::container::LargeTableContainer;
use crate::context::{KsContext, LookupResult, LookupSource};
use crate::control::RelocateFiles;
use crate::flusher::{FlushKind, IndexFlushCommand, IndexFlusher, IndexFlusherThread};
use crate::index::index_format::{Direction, IndexFormat, IndexIterCaches};
use crate::index::index_table::IndexTable;
use crate::index::levels::{INLINE_LEVELS, IndexLevels};
use crate::index::pending_table::{CommittedChange, PendingTable, Transaction};
use crate::index::utils::{NextEntryResult, merge_levels_next_entry};
use crate::iterators::IteratorResult;
use crate::key_shape::{KeyShape, KeySpace, KeySpaceDesc, KeyType};
use crate::metrics::Metrics;
use crate::primitives::arc_cow::ArcCow;
use crate::primitives::range_from_excluding::next_key_in_tree;
use crate::primitives::sharded_mutex::ShardedMutex;
use crate::relocation::updates::RelocationUpdates;
use crate::replay_buffer::{CellReplayBuffer, ReplayBuffer};
use crate::runtime;
use crate::wal::WalRandomRead;
use crate::wal::layout::WalLayout;
use crate::wal::position::{LastProcessed, WalFileId, WalPosition};
use crate::wal::tracker::WalGuard;
use fastbloom::BloomFilter;
use lru::LruCache;
use minibytes::Bytes;
use parking_lot::{MutexGuard, RwLock};
use rand::rngs::ThreadRng;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use std::{cmp, thread};

pub struct LargeTable {
    table: Vec<KsTable>,
    pub(crate) flusher: IndexFlusher,
    metrics: Arc<Metrics>,
    pub(crate) fp: LargeTableFailPoints,
    /// Tracks the (ks, state) label pairs reported in the previous call to
    /// [`report_entries_state`]. Used to zero out stale combinations that no
    /// longer have any entries, without resetting the entire gauge vec.
    reported_entry_state_keys: parking_lot::Mutex<HashSet<(String, &'static str)>>,
}

pub struct LargeTableEntry {
    cell: CellId,
    pub(crate) data: ArcCow<IndexTable>,
    pending_data: PendingTable,
    state: LargeTableEntryState,
    /// On-disk index-blob positions for this cell.
    ///
    /// Invariant: empty iff `state == Empty`. Held as an `IndexLevels` so
    /// state-transition code is level-generic.
    levels: IndexLevels,
    context: KsContext,
    bloom_filter: Option<BloomFilter>,
    unload_jitter: usize,
    last_processed: LastProcessed,
    /// Tracks the WAL last_processed position captured when an async flush was initiated.
    /// - `None`: No flush is pending
    /// - `Some(pos)`: Async flush is in progress, will update `last_processed` to `pos` when complete
    pending_last_processed: Option<LastProcessed>,
    // (full_key, value). full_key is the original (non-reduced) WAL key; for
    // non-key-reduction keyspaces it equals the index key.
    value_lru: Option<LruCache<Bytes, (Bytes, Bytes)>>,
    /// Last value added to the shared `loaded_key_bytes` gauge.
    /// Used to compute the delta on the next report so multiple cells
    /// can safely share one gauge via `.add()` instead of `.set()`.
    last_reported_key_bytes: i64,
    /// Last value added to the shared `flat_index_bytes` gauge.
    last_reported_flat_bytes: i64,
    /// Last value added to the shared `dirty_keys` gauge.
    last_reported_dirty_count: i64,
}

/// In-memory status of a `LargeTableEntry`.
///
/// The on-disk position(s) for the cell are stored separately on
/// [`LargeTableEntry::levels`].
///
/// Invariant: `Empty` ⇔ `levels.is_empty()`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LargeTableEntryState {
    Empty,
    Unloaded,
    Loaded,
    DirtyUnloaded,
    DirtyLoaded,
}

struct KsTable {
    context: KsContext,
    rows: ShardedMutex<Row>,
    /// For PrefixedUniform keyspaces, tracks all cells to enable O(log n) iteration.
    /// For other keyspace types, this is empty.
    /// This might contain cell that was already deleted from the large table.
    /// Drop cells currently does not delete from this index.
    ///
    /// The write lock on this lock is acquire within scope of row lock.
    /// No new locks are acquired withing the scope of cell_index lock(read or write).
    cell_index: RwLock<BTreeSet<CellIdBytesContainer>>,
    /// One flag per mutex shard. Set by the commit path when a batch is applied to that shard;
    /// cleared and consumed by the pending-promotion thread.
    pending_dirty: Box<[AtomicBool]>,
}

struct Row {
    context: KsContext,
    entries: Entries,
}

/// Captured under the row mutex by `LargeTable::prepare_walk` so the
/// merge loop in `execute_walk` can run without holding the lock. The
/// `Arc<IndexTable>` snapshot of the in-memory overlay is stable for
/// the duration of the walk: any concurrent writer triggers `ArcCow`
/// copy-on-write rather than mutating this `Arc`'s contents.
struct WalkPlan {
    data: Arc<IndexTable>,
    level_positions: SmallVec<[WalPosition; INLINE_LEVELS]>,
    readers: SmallVec<[WalRandomRead; INLINE_LEVELS]>,
    /// `true` iff the cell was sharded when this plan was built. Sharded
    /// cells have to bubble `SkipDeleted` out so `prepare_walk` can re-pick
    /// the shard for the advanced `prev_key`; unsharded cells can absorb
    /// the tombstone skip inside `execute_walk` (the plan stays valid).
    is_sharded: bool,
}

/// Outcome of one `execute_walk` step. `SkipDeleted` is only returned for
/// sharded cells, where advancing past the tombstone may cross a shard
/// boundary; the caller must re-run `prepare_walk` with the advanced key.
/// Unsharded cells resolve tombstones inline and never surface this variant.
enum WalkOutcome {
    Found(Bytes, WalPosition),
    Exhausted,
    SkipDeleted(Bytes),
}

enum Entries {
    Array(usize /*num_mutexes*/, Box<[LargeTableEntry]>),
    Tree(BTreeMap<CellIdBytesContainer, LargeTableEntry>),
}

/// Snapshot data for a single entry: the list of on-disk index blobs
/// (`levels`) plus the durable WAL frontier (`last_processed`).
///
/// `levels` is an `IndexLevels` — see `index::levels` for slot semantics
/// (L0 freshest, L1 cold, `WalPosition::INVALID` sentinels for empty
/// interior slots). Backward compatibility with the legacy
/// `{ position, last_processed }` on-disk format is handled by the
/// control-region reader, not by this struct.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[doc(hidden)] // Used by tools/tideconsole for control region inspection
pub struct SnapshotEntryData {
    pub levels: IndexLevels,
    pub last_processed: LastProcessed,
}

impl SnapshotEntryData {
    /// Creates an empty/invalid snapshot entry data
    pub fn empty() -> Self {
        Self {
            levels: IndexLevels::new(),
            last_processed: LastProcessed::none(),
        }
    }

    /// Returns the on-disk index blobs for this cell as an `IndexLevels`.
    pub fn levels(&self) -> &IndexLevels {
        &self.levels
    }

    /// Constructs from an `IndexLevels` + frontier. The flusher may emit
    /// sparse/multi-level lists (e.g. `[INVALID, L1]` after a promote) —
    /// they round-trip through the on-disk control region as-is.
    pub fn from_levels(levels: IndexLevels, last_processed: LastProcessed) -> Self {
        Self {
            levels,
            last_processed,
        }
    }
}

pub(crate) struct LargeTableSnapshot {
    pub data: LargeTableContainer<SnapshotEntryData>,
    pub replay_from: u64,
}

/// A single pending operation to apply within a batch, used by [`LargeTable::apply_pending_batch`].
pub(crate) enum PendingBatchOp {
    Insert {
        reduced_key: Bytes,
        lru_update: Option<(Bytes, Bytes)>,
    },
    Remove {
        reduced_key: Bytes,
    },
}

impl LargeTable {
    pub(crate) fn from_unloaded<L: Loader + Sync>(
        key_shape: &KeyShape,
        snapshot: &LargeTableContainer<SnapshotEntryData>,
        config: Arc<Config>,
        flusher: IndexFlusher,
        metrics: Arc<Metrics>,
        loader: &L,
    ) -> Self {
        assert_eq!(
            snapshot.data.len(),
            key_shape.num_ks(),
            "Snapshot has different number of key spaces"
        );
        let table = key_shape
            .iter_ks()
            .zip(&snapshot.data) // Each data[i] = BTreeMap for keyspace i
            .collect::<Vec<_>>()
            .into_par_iter()
            .map(
                |(ks, ks_cells): (_, &BTreeMap<CellId, SnapshotEntryData>)| {
                    let context = KsContext::new(config.clone(), ks.clone(), metrics.clone());
                    let bloom_filter_start = Instant::now();
                    let num_mutexes = ks.num_mutexes();

                    // Distribute cells to rows based on current mutex_for_cell()
                    let mut row_cells: Vec<Vec<(CellId, SnapshotEntryData)>> =
                        (0..num_mutexes).map(|_| Vec::new()).collect();

                    for (cell, entry_data) in ks_cells {
                        let mutex_idx = ks.mutex_for_cell(cell);
                        row_cells[mutex_idx].push((cell.clone(), entry_data.clone()));
                    }

                    // Create Row structures
                    let rows = row_cells.into_par_iter().map(|cells| {
                        let entries = cells.into_iter().map(|(cell, entry_data)| {
                            let bloom_filter = context.ks_config.bloom_filter().map(|opts| {
                                let mut filter = BloomFilter::with_false_pos(opts.rate as f64)
                                    .expected_items(opts.count as usize);
                                // Rebuild the cell-wide bloom by walking every on-disk
                                // level. With a single level this loads one blob; with
                                // multiple levels it unions keys across all blobs, which
                                // matches the read-path union semantics.
                                //
                                // TODO: a key that was flushed to L1 as a real entry and
                                // later tombstoned in L0 still shows up in L1's `keys()`
                                // (tombstones are filtered per-level, not across levels),
                                // so it ends up in the bloom even though it's logically
                                // deleted. Harmless false positive — read-path still
                                // returns None via the L0-wins rule — but wastes bloom
                                // capacity on dead keys. A future pass could merge levels
                                // (L0 tombstones shadow deeper entries) before feeding
                                // the bloom, trading init CPU for a tighter filter.
                                for position in entry_data.levels().iter() {
                                    let data = loader.load(&context.ks_config, position).expect(
                                        "Failed to load an index entry to reconstruct bloom filter",
                                    );
                                    for key in data.keys() {
                                        filter.insert(&key);
                                    }
                                }
                                filter
                            });
                            let unload_jitter =
                                config.gen_dirty_keys_jitter(&mut ThreadRng::default());
                            LargeTableEntry::from_snapshot_data(
                                context.clone(),
                                cell,
                                &entry_data,
                                unload_jitter,
                                bloom_filter,
                            )
                        });
                        let entries = match ks.key_type() {
                            KeyType::Uniform(_) => {
                                Entries::Array(ks.num_mutexes(), entries.collect())
                            }
                            KeyType::PrefixedUniform(_) => Entries::Tree(
                                entries
                                    .map(|e| (e.cell.assume_bytes_id().clone(), e))
                                    .collect(),
                            ),
                        };
                        Row {
                            entries,
                            context: context.clone(),
                        }
                    });

                    let rows = ShardedMutex::from_parallel_iterator(rows);
                    metrics
                        .large_table_init_mcs
                        .with_label_values(&[ks.name()])
                        .inc_by(bloom_filter_start.elapsed().as_micros() as u64);

                    // For PrefixedUniform keyspaces, build cell index for fast iteration
                    let cell_index = match ks.key_type() {
                        KeyType::PrefixedUniform(_) => {
                            let cells = ks_cells
                                .keys()
                                .map(|cell| cell.assume_bytes_id().clone())
                                .collect();
                            RwLock::new(cells)
                        }
                        KeyType::Uniform(_) => Default::default(),
                    };

                    let pending_dirty = (0..num_mutexes)
                        .map(|_| AtomicBool::new(false))
                        .collect::<Box<[_]>>();
                    KsTable {
                        context,
                        rows,
                        cell_index,
                        pending_dirty,
                    }
                },
            )
            .collect();
        Self {
            table,
            flusher,
            metrics,
            fp: Default::default(),
            reported_entry_state_keys: parking_lot::Mutex::new(HashSet::new()),
        }
    }

    pub(crate) fn ks_context(&self, ks: KeySpace) -> &KsContext {
        &self.table[ks.as_usize()].context
    }

    pub fn insert<L: Loader>(
        &self,
        context: &KsContext,
        reduced_key: Bytes,
        full_key: Bytes,
        guard: WalGuard,
        value: &Bytes,
        loader: &L,
    ) -> Result<(), L::Error> {
        self.fp.fp_insert_before_lock();
        let v = *guard.wal_position();
        let (mut row, cell) = self.row(context, &reduced_key);
        let entry = self.entry_mut(&mut row, &cell);

        entry.promote_pending_for(&reduced_key);

        if !entry.insert(reduced_key.clone(), v, full_key, Some(value)) {
            return Ok(());
        }

        let index_size = entry.data.len();
        if loader.flush_supported() && self.too_many_dirty(entry) {
            // Drop the guard before flushing to ensure last_processed is updated
            drop(guard);
            entry.unload_if_ks_enabled(&self.flusher, loader)?;
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
        context: &KsContext,
        k: Bytes,
        guard: WalGuard,
        _loader: &L,
    ) -> Result<(), L::Error> {
        self.fp.fp_remove_before_lock();
        let v = *guard.wal_position();
        let (mut row, cell) = self.row(context, &k);
        let entry = self.entry_mut(&mut row, &cell);

        entry.promote_pending_for(&k);

        if !entry.remove(k.clone(), v) {
            return Ok(());
        }
        Ok(())
    }

    /// Drain a [`ReplayBuffer`] accumulated by `Db::replay_wal` and apply
    /// each cell's writes in bulk.
    ///
    /// `cutoff` is the WAL offset above which buffered entries are silently
    /// discarded — used to drop a partial `BatchStart` group whose record
    /// count was never reached. The replay loop passes `u64::MAX` when no
    /// partial batch is in flight (the common case). Because per-cell
    /// entries are pushed in WAL order, the cutoff translates to a single
    /// `partition_point` + `truncate` inside each cell.
    ///
    /// The replay loop accumulates per-cell vectors in a local buffer (no
    /// locks at all). This method walks the buffer once per keyspace,
    /// groups cells by mutex shard, and acquires each row lock at most
    /// once — instead of the row-lock-per-record cost of the live path.
    ///
    /// Per-cell apply also skips the BTreeMap insert path entirely: entries
    /// land directly in the cell's flat buffer via
    /// `IndexTable::from_sorted_entries`.
    ///
    /// Live-write checks intentionally skipped (replay is sequential, no
    /// in-flight transactions, no concurrent writers):
    ///   - `promote_pending_for` / `pending_data` (always empty)
    ///   - `skip_stale_update` (WAL is monotonic; later-position wins
    ///     naturally via HashMap overwrite semantics)
    ///   - Value LRU (churns + evicts during replay, ends up holding a
    ///     random tail; not worth the cost)
    ///   - `too_many_dirty` / flush trigger (replay loader has
    ///     `flush_supported() = false`)
    ///   - `max_index_size` / `index_size` per-record metric updates
    pub(crate) fn apply_replay_buffer(&self, mut buffer: ReplayBuffer, cutoff: u64) {
        for ks_idx in 0..buffer.num_keyspaces() {
            let by_cell = buffer.take_ks(ks_idx);
            if by_cell.is_empty() {
                continue;
            }
            let ks_table = &self.table[ks_idx];
            // Group cells by mutex shard so we take each row lock exactly once.
            let mut by_shard: HashMap<usize, Vec<(CellId, CellReplayBuffer)>> = HashMap::new();
            for (cell, buf) in by_cell {
                let mutex_idx = ks_table.context.ks_config.mutex_for_cell(&cell);
                by_shard.entry(mutex_idx).or_default().push((cell, buf));
            }
            // Shards are independent — apply them in parallel. Each rayon
            // worker takes a single row lock and drains every cell on it.
            let by_shard_vec: Vec<(usize, Vec<(CellId, CellReplayBuffer)>)> =
                by_shard.into_iter().collect();
            by_shard_vec.into_par_iter().for_each(|(mutex_idx, cells)| {
                let mut row = ks_table
                    .rows
                    .lock(mutex_idx, &ks_table.context.large_table_contention);
                for (cell, buf) in cells {
                    let entry = self.entry_mut(&mut row, &cell);
                    entry.apply_replay_buffer(buf, cutoff);
                }
            });
        }
    }

    /// Apply a batch of pending operations that all share the same mutex shard,
    /// acquiring the mutex only once.
    pub(crate) fn apply_pending_batch(
        &self,
        context: &KsContext,
        mutex_idx: usize,
        ops: &[(CellId, &PendingBatchOp, WalPosition)],
        transaction: &Transaction,
    ) {
        let mut row = self.row_by_mutex(context, mutex_idx);
        for (cell_id, op, position) in ops {
            let entry = self.entry_mut(&mut row, cell_id);
            match op {
                PendingBatchOp::Insert {
                    reduced_key,
                    lru_update,
                } => {
                    entry.pending_data.insert(
                        reduced_key.clone(),
                        *position,
                        lru_update.clone(),
                        transaction,
                    );
                }
                PendingBatchOp::Remove { reduced_key } => {
                    entry
                        .pending_data
                        .remove(reduced_key.clone(), *position, transaction);
                }
            }
            context.pending_table_len.add(1);
        }
        self.ks_table(&context.ks_config).pending_dirty[mutex_idx].store(true, Ordering::Release);
    }

    pub fn update_lru(
        &self,
        context: &KsContext,
        reduced_key: Bytes,
        full_key: Bytes,
        value: Bytes,
    ) {
        if context.ks_config.value_cache_size().is_none() {
            return;
        }
        let (mut row, cell) = self.row(context, &reduced_key);
        let entry = self.entry_mut(&mut row, &cell);
        let Some(value_lru) = &mut entry.value_lru else {
            unreachable!()
        };
        let reduced_key = reduced_key.into_owned();
        let full_key = full_key.into_owned();
        let value = value.into_owned();
        let delta: i64 = (reduced_key.len() + full_key.len() + value.len()) as i64;
        let previous = value_lru.push(reduced_key, (full_key, value));
        LargeTableEntry::update_lru_metric(context, previous, delta);
    }

    pub fn get<L: Loader>(
        &self,
        context: &KsContext,
        k: &[u8],
        loader: &L,
    ) -> Result<GetResult, L::Error> {
        let (mut row, cell) = self.row(context, k);
        let entry = row.try_entry_mut(&cell);
        let Some(entry) = entry else {
            return Ok(context.report_lookup_result(None, LookupSource::Prefix));
        };

        // Important: promote_pending must be called before checking LRU cache
        // to ensure pending deletes are processed and LRU is updated correctly
        entry.promote_pending();

        if let Some(value_lru) = &mut entry.value_lru
            && let Some((full_key, value)) = value_lru.get(k)
        {
            context.inc_lookup_result(LookupResult::Found, LookupSource::Lru);
            return Ok(GetResult::Value(full_key.clone(), value.clone()));
        }

        if entry.bloom_filter_not_found(k) {
            return Ok(context.report_lookup_result(None, LookupSource::Bloom));
        }
        if entry.state == LargeTableEntryState::Empty {
            return Ok(context.report_lookup_result(None, LookupSource::Cache));
        }
        // Probe the in-memory overlay first. For Loaded/DirtyLoaded, `entry.data`
        // holds L0 + overlay and a hit wins (valid position = Found; INVALID =
        // tombstone shadowing deeper levels). For DirtyUnloaded it holds just
        // the overlay. Unloaded has empty data by invariant but `entry.get`
        // panics there, so skip it.
        if entry.state != LargeTableEntryState::Unloaded
            && let Some(found) = entry.get(k)
        {
            return Ok(context.report_lookup_result(found.valid(), LookupSource::Cache));
        }
        // Build the reader list while holding the mutex — otherwise a concurrent
        // relocation could delete the backing blob before we open it. The
        // key-aware form keeps a sharded cell to a single shard reader.
        let level_positions = entry.disk_levels_to_walk_for_key(k);
        if level_positions.is_empty() {
            // No on-disk levels left to consult (e.g., Loaded cell with just
            // `[L0]` — entry.data is authoritative and already missed).
            return Ok(context.report_lookup_result(None, LookupSource::Cache));
        }
        // Allocate index readers **before** dropping the mutex — this ensures
        // that we can read from the index even if a relocation is concurrently
        // deleting the underlying blobs. See also test_concurrent_index_reclaim.
        let mut index_readers: SmallVec<[_; INLINE_LEVELS]> = SmallVec::new();
        for pos in &level_positions {
            index_readers.push(loader.index_reader(*pos)?);
        }
        // drop row to avoid holding mutex during IO
        drop(row);

        Ok(self.lookup_disk_levels(context, index_readers, k))
    }

    /// Walk the on-disk index levels (readers already opened under the row
    /// lock) for `k`, returning the first hit. Shared by the value [`Self::get`]
    /// and checkpoint [`Self::get_checkpoint`] read paths — both consult disk
    /// identically once the in-memory overlay misses.
    ///
    /// `readers` must be ordered shallowest level first and be non-empty
    /// (callers short-circuit when there are no on-disk levels). The row lock
    /// must already be dropped.
    fn lookup_disk_levels(
        &self,
        context: &KsContext,
        readers: SmallVec<[WalRandomRead; INLINE_LEVELS]>,
        k: &[u8],
    ) -> GetResult {
        let ks = &context.ks_config;
        self.fp.fp_lookup_after_lock_drop();
        let now = Instant::now();
        // todo - consider only doing block_in_place for the syscall random reader
        // TODO: handle entries that may be removed by relocation but are still referenced in the index
        let (result, read_type, terminal_level) = runtime::block_in_place(|| {
            // `readers` is non-empty (guarded by callers' `level_positions.is_empty()`
            // check), and the loop reassigns `last_read_type` on every iteration
            // before the early-return branches can fire — so the init is just a seed.
            let mut last_read_type = readers[0].read_type();
            let mut last_level = 0usize;
            for (level, reader) in readers.iter().enumerate() {
                last_read_type = reader.read_type();
                last_level = level;
                match ks
                    .index_format()
                    .lookup_unloaded(ks, reader, k, &self.metrics)
                {
                    // Valid hit — return it.
                    Some(pos) if pos.is_valid() => return (Some(pos), last_read_type, level),
                    // On-disk tombstone shadows any key in deeper levels.
                    Some(_) => return (None, last_read_type, level),
                    // Miss on this level — continue to the next one.
                    None => {}
                }
            }
            (None, last_read_type, last_level)
        });
        context
            .lookup_mcs_histogram(read_type)
            .observe(now.elapsed().as_micros() as f64);
        context.report_lookup_result(result, LookupSource::for_level(terminal_level))
    }

    /// Checkpoint read: a point-in-time view of `k` as of the WAL frontier
    /// `last_processed`, captured by a
    /// [`crate::wal::tracker::WalTrackerLatch`].
    ///
    /// Differs from [`Self::get`] in how the in-memory overlay is read: it is
    /// probed with [`LargeTableEntry::get_at`], which ignores index positions
    /// at or above `last_processed` (writes that landed after the checkpoint
    /// frontier). On-disk levels are walked identically to `get` — every
    /// on-disk entry is below the latched frontier by the `promote_to_flat`
    /// invariant (see [`IndexTable::get_at`]), so no extra filtering is needed.
    ///
    /// The bloom filter and value LRU are both reused, soundly:
    ///   - The bloom only ever yields true negatives ("key not in this cell").
    ///     Its key set is a superset of every key with any index entry, so a
    ///     negative means the key is absent from the index entirely — and the
    ///     checkpoint reads that same index, so it would also miss.
    ///   - The LRU holds the *latest* value, which may postdate the frontier,
    ///     so it is consulted only when the key has not been written since the
    ///     checkpoint — i.e. its as-of-frontier position equals its latest
    ///     position (`entry.get(k) == as_of`). Then the cached value matches.
    pub fn get_checkpoint<L: Loader>(
        &self,
        context: &KsContext,
        k: &[u8],
        last_processed: LastProcessed,
        loader: &L,
    ) -> Result<GetResult, L::Error> {
        let (mut row, cell) = self.row(context, k);
        let entry = row.try_entry_mut(&cell);
        let Some(entry) = entry else {
            return Ok(context.report_lookup_result(None, LookupSource::Prefix));
        };

        // Apply pending committed ops to the overlay before reading; the offset
        // filter in `get_at` discards any that postdate the checkpoint. Mirrors
        // the ordering in `get`.
        entry.promote_pending();

        if entry.bloom_filter_not_found(k) {
            return Ok(context.report_lookup_result(None, LookupSource::Bloom));
        }
        if entry.state == LargeTableEntryState::Empty {
            return Ok(context.report_lookup_result(None, LookupSource::Cache));
        }
        if entry.state != LargeTableEntryState::Unloaded
            && let Some(found) = entry.get_at(k, last_processed)
        {
            let Some(as_of) = found.valid() else {
                // Deleted as of the frontier — shadows deeper levels.
                return Ok(context.report_lookup_result(None, LookupSource::Cache));
            };
            // Serve from the value LRU only when the key is unchanged since the
            // checkpoint (its as-of-frontier position is also its latest one);
            // otherwise the cache holds a newer value. The `is_some` gate comes
            // first so keyspaces without a value cache skip the extra
            // `entry.get(k)` index lookup entirely (a leading `let Some(..) =
            // &mut entry.value_lru` would instead hold a borrow across it).
            if entry.value_lru.is_some()
                && entry.get(k) == Some(as_of)
                && let Some(value_lru) = &mut entry.value_lru
                && let Some((full_key, value)) = value_lru.get(k)
            {
                context.inc_lookup_result(LookupResult::Found, LookupSource::Lru);
                return Ok(GetResult::Value(full_key.clone(), value.clone()));
            }
            return Ok(context.report_lookup_result(Some(as_of), LookupSource::Cache));
        }
        let level_positions = entry.disk_levels_to_walk_for_key(k);
        if level_positions.is_empty() {
            return Ok(context.report_lookup_result(None, LookupSource::Cache));
        }
        let mut index_readers: SmallVec<[_; INLINE_LEVELS]> = SmallVec::new();
        for pos in &level_positions {
            index_readers.push(loader.index_reader(*pos)?);
        }
        // drop row to avoid holding mutex during IO
        drop(row);

        Ok(self.lookup_disk_levels(context, index_readers, k))
    }

    fn entry_mut<'a>(&self, row: &'a mut Row, cell: &CellId) -> &'a mut LargeTableEntry {
        let ks_table = if row.context.ks_config.needs_large_table_cell_index() {
            Some(self.ks_table(&row.context.ks_config))
        } else {
            None
        };
        let (entry, created) = row.entry_mut(cell);
        if created && let Some(ks_table) = ks_table {
            ks_table
                .cell_index
                .write()
                .insert(cell.assume_bytes_id().clone());
        }
        entry
    }

    pub fn is_empty(&self) -> bool {
        self.table
            .iter()
            .all(|m| m.rows.mutexes().iter().all(|m| m.lock().entries.is_empty()))
    }

    /// Promotes pending entries for cells whose dirty flag was set by the commit path.
    /// Called by the event-driven pending-promotion thread immediately after batches commit.
    ///
    /// Only processes mutex shards assigned to this shard: `mutex_idx % num_shards == shard_idx`.
    /// Commits that arrive after the flag is swapped to false will set it again and be
    /// processed on the next wakeup, or caught by the flat-promotion fallback.
    pub(crate) fn promote_dirty_pending<L: Loader>(
        &self,
        loader: &L,
        shard_idx: usize,
        num_shards: usize,
    ) -> Result<(), L::Error> {
        for ks_table in &self.table {
            for (mutex_idx, flag) in ks_table.pending_dirty.iter().enumerate() {
                if mutex_idx % num_shards == shard_idx && flag.swap(false, Ordering::Acquire) {
                    let mut row = ks_table
                        .rows
                        .lock(mutex_idx, &ks_table.context.large_table_contention);
                    for entry in row.entries.iter_mut() {
                        entry.promote_pending_and_check_flush(loader, &self.flusher)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Compacts BTreeMap index entries into flat sorted arrays for all cells in all keyspaces.
    /// Also reports remaining pending entry counts per keyspace to metrics.
    /// Runs on a 10-second polling cadence, independently of the pending-promotion thread.
    pub(crate) fn promote_flat_job<L: Loader>(&self, loader: &L) -> Result<(), L::Error> {
        for ks_table in &self.table {
            let mut total_remaining = 0;
            for mutex in ks_table.rows.mutexes() {
                // Phase 1: under lock — promote any remaining pending entries and check flush.
                // promote_pending must run before flush to ensure pending entries are applied
                // before the flusher snapshots the index. The event-driven thread handles
                // the common case; this is a 1-second catch-all for anything it missed.
                //
                // Capture each entry's promote threshold under the lock: if a flush is in
                // flight (`pending_last_processed = Some(P)`) we must not promote past P,
                // because `clear_after_flush` will run with that P and assume all flat
                // entries are processed by it. Otherwise the current loader position is
                // fine — it only ever advances, so a later observer sees the invariant
                // "flat offset < observed last_processed" preserved.
                let snapshots: Vec<(CellId, Arc<IndexTable>, LastProcessed)> = {
                    let mut row = mutex.lock();
                    let loader_lp = loader.last_processed_wal_position();
                    let mut snapshots = Vec::with_capacity(row.entries.iter().count());
                    for entry in row.entries.iter_mut() {
                        let remaining =
                            entry.promote_pending_and_check_flush(loader, &self.flusher)?;
                        total_remaining += remaining;
                        let threshold = entry.pending_last_processed.unwrap_or(loader_lp);
                        let arc = entry.data.clone_shared();
                        snapshots.push((entry.cell.clone(), arc, threshold));
                    }
                    snapshots
                    // lock released here
                };

                // Phase 2: outside the lock — clone each snapshot and compact BTreeMap into flat.
                // If a writer modifies an entry concurrently it will clone on make_mut
                // (ArcCow semantics); the same_shared check below detects this and discards stale work.
                let promoted: Vec<(CellId, Arc<IndexTable>, IndexTable)> = snapshots
                    .iter()
                    .filter_map(|(cell, arc, threshold)| {
                        // Single decision point for this cell: cells that
                        // fail the gate skip the deep clone below, and cells
                        // that pass drain without re-evaluating the trigger.
                        if !arc.should_promote_to_flat(*threshold) {
                            return None;
                        }
                        let mut table = (**arc).clone();
                        table.promote_to_flat_unconditionally(*threshold);
                        Some((cell.clone(), arc.clone(), table))
                    })
                    .collect();

                // Phase 3: re-acquire lock — write back promoted table only if the entry was
                // not modified while we were outside the lock, then update metrics.
                if !promoted.is_empty() {
                    let mut row = mutex.lock();
                    for (cell, arc, promoted_table) in promoted {
                        let Some(entry) = row.try_entry_mut(&cell) else {
                            continue;
                        };
                        if entry.data.same_shared(&arc) {
                            entry.data = ArcCow::new_owned(promoted_table);
                        } else {
                            self.metrics.promote_flat_arc_miss.inc();
                        }
                        entry.report_loaded_keys_count();
                    }
                }
            }
            self.metrics
                .pending_promotion_job_remaining
                .with_label_values(&[ks_table.context.name()])
                .set(total_remaining as i64);
        }
        Ok(())
    }

    /// Test-only: drains every cell's BTreeMap into its flat buffer under the
    /// row lock, bypassing `PROMOTE_THRESHOLD`. Unlike `promote_flat_job`,
    /// this does not call `promote_pending_and_check_flush` or trigger
    /// flushes — it just performs the compaction step. Skips cells whose
    /// BTreeMap is empty to avoid spurious `make_mut` (Arc clone) that would
    /// break `same_shared` on the flusher's snapshot. Lets concurrent tests
    /// reliably open the `insert → promote → remove → FlushLoaded` window
    /// that triggered the `clean_self` stale-record bug without needing
    /// 128+ keys per cell.
    ///
    /// The promote threshold is captured the same way `promote_flat_job`
    /// does: `entry.pending_last_processed.unwrap_or(loader_lp)`. Passing
    /// `u64::MAX` instead would promote unprocessed entries too and falsify
    /// the test against the real production invariant.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn test_promote_flat_force<L: Loader>(&self, loader: &L) {
        for ks_table in &self.table {
            for mutex in ks_table.rows.mutexes() {
                let mut row = mutex.lock();
                let loader_lp = loader.last_processed_wal_position();
                for entry in row.entries.iter_mut() {
                    if entry.data.data_btree_is_empty() {
                        continue;
                    }
                    let threshold = entry.pending_last_processed.unwrap_or(loader_lp);
                    let table = entry.data.make_mut();
                    table.promote_to_flat_force(threshold);
                }
            }
        }
    }

    fn too_many_dirty(&self, entry: &mut LargeTableEntry) -> bool {
        if entry.state.is_dirty() {
            let dirty_count = entry.data.dirty_count();
            entry
                .context
                .excess_dirty_keys(dirty_count.saturating_sub(entry.unload_jitter))
        } else {
            false
        }
    }

    fn row(&self, context: &KsContext, k: &[u8]) -> (MutexGuard<'_, Row>, CellId) {
        let cell = context.ks_config.cell_id(k);
        let mutex = context.ks_config.mutex_for_cell(&cell);
        let row = self.row_by_mutex(context, mutex);
        (row, cell)
    }

    fn row_by_mutex(&self, context: &KsContext, mutex: usize) -> MutexGuard<'_, Row> {
        let ks_table = self.ks_rows(&context.ks_config);
        ks_table.lock(mutex, &context.large_table_contention)
    }

    /// Locks the row for a cell, waiting for any pending async flush to complete.
    ///
    /// The returned cell is guaranteed to have pending_last_processed = None.
    ///
    /// This is not suitable for frequent user-facing operations as it blocks current thread
    /// via sleep while it waits for pending flush to finish.
    fn lock_cell_waiting_for_flush(
        &self,
        context: &KsContext,
        cell: &CellId,
    ) -> MutexGuard<'_, Row> {
        let mutex = context.ks_config.mutex_for_cell(cell);
        let ks_table = self.ks_rows(&context.ks_config);

        loop {
            let mut row = ks_table.lock(mutex, &context.large_table_contention);

            if let Some(entry) = row.try_entry_mut(cell)
                && entry.pending_last_processed.is_some()
            {
                // Async flush is in progress, wait for it to complete
                drop(row);
                thread::sleep(Duration::from_millis(50));
                continue;
            }

            return row;
        }
    }

    /// Returns the **as-of-`last_processed`** index view for a cell: the on-disk
    /// levels merged under the in-memory overlay, with the overlay reduced to its
    /// below-`last_processed` positions. Returns `None` if the cell doesn't exist.
    ///
    /// Used by relocation, which must relocate the as-of-frontier value of every
    /// key (not the live latest) so a held checkpoint can still read it after GC
    /// reclaims the original WAL file.
    ///
    /// Correctness hinge: the overlay is filtered (`retain_processed`) on its
    /// *uncollapsed* positions, BEFORE the latest-per-key collapse that
    /// `maybe_load`/`merge_dirty` perform. A key with a below-frontier write
    /// shadowed by a post-frontier overwrite holds both positions in the overlay;
    /// collapsing first keeps only the latest (post-frontier) one, which the
    /// filter then drops — losing the as-of value entirely and stranding its WAL
    /// file for GC. We therefore build the view here without `maybe_load` (which
    /// would also destructively collapse the live cell's overlay). `retain_processed`
    /// touches only `data`; disk-derived levels are merged unfiltered (they are
    /// as-of-safe by the unprocessed-write retention invariant — see `crate::checkpoint`).
    pub fn get_index_for_cell<L: Loader>(
        &self,
        context: &KsContext,
        cell_id: &CellId,
        loader: &L,
        last_processed: LastProcessed,
    ) -> Result<Option<Arc<IndexTable>>, L::Error> {
        let mutex_index = context.ks_config.mutex_for_cell(cell_id);
        let mut row = self.row_by_mutex(context, mutex_index);

        let entry = match row.try_entry_mut(cell_id) {
            Some(entry) => entry,
            None => return Ok(None), // Cell doesn't exist
        };

        entry.promote_pending();

        // As-of overlay: clone the (uncollapsed) in-memory overlay and drop its
        // post-frontier positions before any merge collapses to latest-per-key.
        let mut overlay = IndexTable::clone(&entry.data);
        overlay.retain_processed(last_processed);

        // Base = the on-disk levels the overlay sits on. Load L1; for an unloaded
        // cell L0 is still on disk (not folded into `entry.data`), so merge it over
        // L1. For a loaded cell L0 already lives in `entry.data` (and thus `overlay`).
        let mut merged = match entry.levels().l1() {
            Some(l1_pos) => loader.load(&entry.context.ks_config, l1_pos)?,
            None => IndexTable::default(),
        };
        if entry.as_unloaded_state().is_some()
            && let Some(l0_pos) = entry.levels().l0()
        {
            let l0 = loader.load(&entry.context.ks_config, l0_pos)?;
            merged.merge_dirty_and_clean(&l0);
        }
        merged.merge_dirty_and_clean(&overlay);
        Ok(Some(Arc::new(merged)))
    }

    pub fn sync_flush_for_relocation<L: Loader>(
        &self,
        context: &KsContext,
        cell_id: &CellId,
        loader: &L,
        relocation_updates: Option<RelocationUpdates>,
        relocation_cutoff: Option<u64>,
    ) -> Result<(), L::Error> {
        let mut row = self.lock_cell_waiting_for_flush(context, cell_id);

        let entry = match row.try_entry_mut(cell_id) {
            Some(entry) => entry,
            None => return Ok(()), // Cell doesn't exist
        };

        entry.promote_pending();

        entry.sync_flush(loader, true, relocation_updates, relocation_cutoff)
    }

    fn ks_rows(&self, ks: &KeySpaceDesc) -> &ShardedMutex<Row> {
        &self
            .table
            .get(ks.id().as_usize())
            .expect("Table not found for ks")
            .rows
    }

    fn ks_table(&self, ks: &KeySpaceDesc) -> &KsTable {
        self.table
            .get(ks.id().as_usize())
            .expect("Table not found for ks")
    }

    /// Provides a snapshot of this large table along with replay position in the wal for the snapshot.
    pub(crate) fn snapshot<L: Loader>(&self, loader: &L) -> LargeTableSnapshot {
        // Capture the WAL's last_processed position before iterating entries.
        // Used as replay_from for the empty-database case; must be captured early so
        // any writes that arrive during iteration are not silently skipped on replay.
        let wal_last_processed = loader.last_processed_wal_position();
        let mut replay_from: Option<u64> = None;
        let mut data = Vec::with_capacity(self.table.len());
        for ks_table in self.table.iter() {
            let (ks_data, ks_replay_from) = self.ks_snapshot(ks_table, loader);
            data.push(ks_data);
            if let Some(ks_replay_from) = ks_replay_from {
                replay_from = Some(cmp::min(replay_from.unwrap_or(u64::MAX), ks_replay_from));
            }
        }

        // replay_from is None only when every keyspace has zero non-empty entries (empty DB).
        let replay_from = replay_from.unwrap_or_else(|| wal_last_processed.as_u64());

        let data = LargeTableContainer { data };
        LargeTableSnapshot { data, replay_from }
    }

    /// Takes snapshot of a given key space.
    /// Returns (Snapshot, ReplayFrom).
    ///
    /// ReplayFrom is the minimum `last_processed` across all non-empty entries, or None if
    /// the keyspace has no non-empty entries. See docs/snapshot_mechanism.md for details.
    fn ks_snapshot<L: Loader>(
        &self,
        ks_table: &KsTable,
        loader: &L,
    ) -> (BTreeMap<CellId, SnapshotEntryData>, Option<u64>) {
        let mut replay_from: Option<u64> = None;
        // Collect all cells from all rows into one BTreeMap
        let mut ks_data = BTreeMap::new();
        for mutex in ks_table.rows.mutexes() {
            let mut row = mutex.lock();
            for entry in row.entries.iter_mut() {
                // Important - read last_processed_wal_position before promote_pending.
                // See also comments in unload_if_ks_enabled.
                let loader_last_processed = loader.last_processed_wal_position();
                entry.promote_pending();
                // For clean entries, advance last_processed to the current WAL position so
                // that replay_from in the snapshot reflects the actual WAL frontier rather
                // than the stale position from when the entry was last written.
                if !entry.state.is_dirty() && !entry.state.is_empty() {
                    entry.last_processed = loader_last_processed;
                }
                // `entry.levels()` is the full on-disk level list for this
                // cell. Today it's always len 0 or 1; persisting it verbatim
                // means the snapshot shape will grow with no extra work once
                // the flusher emits L0 + L1.
                let snapshot_data =
                    SnapshotEntryData::from_levels(entry.levels().clone(), entry.last_processed);
                ks_data.insert(entry.cell.clone(), snapshot_data);
                // Do not use last_processed from empty entries
                if !entry.state.is_empty() {
                    replay_from = Some(cmp::min(
                        replay_from.unwrap_or(u64::MAX),
                        entry.last_processed.as_u64(),
                    ));
                }
            }
        }
        let metric = self
            .metrics
            .index_distance_from_tail
            .with_label_values(&[ks_table.context.name()]);
        match replay_from {
            None => metric.set(0),
            Some(0) => metric.set(-1), // 0 indicates replay from beginning
            Some(position) => {
                // This can go below 0 because tail_position is not synchronized
                // and index position can be higher than the tail_position in rare cases
                let tail_position = loader.current_wal_position();
                metric.set(tail_position.saturating_sub(position) as i64)
            }
        }
        (ks_data, replay_from)
    }

    /// Takes a next entry in the large table.
    ///
    /// See Db::next_entry for documentation.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn next_entry<L: Loader>(
        &self,
        context: &KsContext,
        mut cell: CellId,
        mut prev_key: Option<Bytes>,
        loader: &L,
        end_cell_exclusive: &Option<CellId>,
        reverse: bool,
        caches: &mut IndexIterCaches,
        last_processed: Option<LastProcessed>,
    ) -> Result<Option<IteratorResult<GetResult>>, L::Error> {
        let ks_table = self.ks_rows(&context.ks_config);
        loop {
            // Phase 1 (under row lock): advance the per-cell state machine and
            // capture an `Arc<IndexTable>` snapshot of the in-memory overlay
            // along with the open readers we need for the on-disk levels.
            // For sharded cells, the shard-pickers in `IndexLevels` use
            // `prev_key`/`reverse` plus the per-shard `[min_key, max_key]`
            // to pick the single shard that holds the next key in
            // direction — no retries needed.
            let mutex_idx = context.ks_config.mutex_for_cell(&cell);
            let plan = {
                let mut row = ks_table.lock(mutex_idx, &context.large_table_contention);
                Self::prepare_walk(
                    loader,
                    &mut row,
                    &cell,
                    prev_key.as_deref(),
                    reverse,
                    caches,
                )?
            };

            // Phase 2 (no lock): k-way merge across the snapshot and the
            // on-disk readers. Disk I/O happens here without holding the row
            // mutex, so other cells in the same shard aren't blocked.
            let walk_result = match plan {
                Some(plan) => Self::execute_walk(
                    &plan,
                    &context.ks_config,
                    &context.metrics,
                    prev_key.clone(),
                    reverse,
                    caches,
                    last_processed,
                ),
                None => WalkOutcome::Exhausted,
            };

            match walk_result {
                WalkOutcome::Found(key, val) => {
                    // Phase 3: re-acquire the row lock briefly to consult the
                    // per-entry LRU. Skipped entirely when no LRU is configured.
                    // If the entry was removed between phases, fall back to a
                    // WAL read. Checkpoint reads (`last_processed.is_some()`)
                    // also skip the LRU: it holds the latest value, which may
                    // postdate the checkpoint frontier.
                    let value = if last_processed.is_none()
                        && context.ks_config.value_cache_size().is_some()
                    {
                        let mut row = ks_table.lock(mutex_idx, &context.large_table_contention);
                        if let Some(entry) = row.try_entry_mut(&cell) {
                            Self::lru_or_wal(entry, &key, val)
                        } else {
                            GetResult::WalPosition(val)
                        }
                    } else {
                        GetResult::WalPosition(val)
                    };
                    return Ok(Some(IteratorResult {
                        cell: Some(cell),
                        key,
                        value,
                    }));
                }
                WalkOutcome::SkipDeleted(skip_key) => {
                    // Tombstone shadowed an on-disk hit. Advance past it and
                    // re-pick: for sharded cells the advance can cross a
                    // shard boundary, so we must re-enter `prepare_walk`.
                    prev_key = Some(skip_key);
                    continue;
                }
                WalkOutcome::Exhausted => {}
            }

            prev_key = None;
            // Moving to a new cell — the cached buffers are no longer valid.
            caches.clear();
            let Some(next_cell) = self.next_cell(context, &cell, reverse) else {
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

    /// Phase 1 of an iterator step: advance the entry's state machine and
    /// build a `WalkPlan` capturing everything Phase 2 needs (snapshot of
    /// the in-memory overlay, level positions, open readers). Caller must
    /// hold the row mutex; on `Ok(None)` there is nothing to walk in this
    /// cell.
    ///
    /// For sharded cells `disk_levels_to_walk_for_iter` picks the one
    /// shard that holds the next key in `prev_key`/`reverse` direction;
    /// no retry plumbing is needed.
    fn prepare_walk<L: Loader>(
        loader: &L,
        row: &mut Row,
        cell: &CellId,
        prev_key: Option<&[u8]>,
        reverse: bool,
        caches: &mut IndexIterCaches,
    ) -> Result<Option<WalkPlan>, L::Error> {
        let Some(entry) = row.try_entry_mut(cell) else {
            return Ok(None);
        };
        entry.promote_pending();
        if !entry.context.ks_config.unloaded_iterator_enabled() {
            entry.maybe_load(loader)?;
        }
        if entry.state == LargeTableEntryState::Empty {
            caches.clear();
            return Ok(None);
        }
        let level_positions = entry.disk_levels_to_walk_for_iter(prev_key, reverse);
        if level_positions.is_empty() && entry.state == LargeTableEntryState::Unloaded {
            // No on-disk blobs and (by invariant) nothing in memory.
            caches.clear();
            return Ok(None);
        }
        // Open readers before we drop the row mutex — same rationale as
        // `get()`: a concurrent relocation must not delete the backing blob
        // out from under us.
        let mut readers: SmallVec<[WalRandomRead; INLINE_LEVELS]> = SmallVec::new();
        for position in level_positions.iter() {
            readers.push(loader.index_reader(*position)?);
        }
        // `clone_shared` migrates the `ArcCow` from `Owned` to `Shared` and
        // hands us an `Arc<IndexTable>`. Subsequent writers go through
        // `make_mut` which copy-on-writes, so the snapshot is stable across
        // the lock-released walk in Phase 2.
        let data = entry.data.clone_shared();
        let is_sharded = entry.levels.is_sharded();
        Ok(Some(WalkPlan {
            data,
            level_positions,
            readers,
            is_sharded,
        }))
    }

    /// Phase 2 of an iterator step: k-way merge across the in-memory snapshot
    /// and the on-disk levels in `plan`. Runs without the row mutex held.
    /// Returns `None` when the cell yields no further entry in `direction`,
    /// otherwise the (key, position) of the next entry.
    fn execute_walk(
        plan: &WalkPlan,
        ks_config: &KeySpaceDesc,
        metrics: &Metrics,
        mut prev_key: Option<Bytes>,
        reverse: bool,
        caches: &mut IndexIterCaches,
        last_processed: Option<LastProcessed>,
    ) -> WalkOutcome {
        let direction = Direction::from_bool(reverse);
        runtime::block_in_place(|| {
            let format = ks_config.index_format();
            loop {
                // Checkpoint reads filter the in-memory overlay to positions
                // below the latched frontier; on-disk levels need no filtering
                // (every flat entry is already below it). See IndexTable::get_at.
                let in_memory_next = match last_processed {
                    Some(lp) => plan.data.next_entry_at(prev_key.clone(), reverse, lp),
                    None => plan.data.next_entry(prev_key.clone(), reverse),
                };
                let mut candidates: SmallVec<[Option<(Bytes, WalPosition)>; 3]> = SmallVec::new();
                candidates.push(in_memory_next);
                for (level_idx, (position, reader)) in plan
                    .level_positions
                    .iter()
                    .zip(plan.readers.iter())
                    .enumerate()
                {
                    let cache_slot = caches.slot_mut(level_idx);
                    let on_disk_next = format.next_entry_unloaded(
                        ks_config,
                        reader,
                        prev_key.as_deref(),
                        direction,
                        metrics,
                        *position,
                        cache_slot,
                    );
                    candidates.push(on_disk_next);
                }
                match merge_levels_next_entry(candidates, direction) {
                    NextEntryResult::NotFound => return WalkOutcome::Exhausted,
                    NextEntryResult::Found(key, val) => return WalkOutcome::Found(key, val),
                    NextEntryResult::SkipDeleted(skip_key) => {
                        // Sharded cells must re-pick the shard: the skip can
                        // cross a shard boundary, and the picked shard in
                        // this plan would then have no more keys.
                        if plan.is_sharded {
                            return WalkOutcome::SkipDeleted(skip_key);
                        }
                        // Unsharded: the plan stays valid past the tombstone,
                        // absorb the skip inline.
                        prev_key = Some(skip_key);
                    }
                }
            }
        })
    }

    /// Phase 3 of an iterator step: probe the per-entry value LRU for `key`
    /// to short-circuit a WAL read. Returns `Value(full_key, value)` on hit,
    /// `WalPosition(pos)` on miss (caller will read the WAL).
    fn lru_or_wal(entry: &mut LargeTableEntry, key: &[u8], pos: WalPosition) -> GetResult {
        if let Some(lru) = &mut entry.value_lru
            && let Some((full_key, value)) = lru.get(key)
        {
            GetResult::Value(full_key.clone(), value.clone())
        } else {
            GetResult::WalPosition(pos)
        }
    }

    /// Test-only convenience that drives prepare_walk/execute_walk against
    /// an already-locked `Row`. Production iteration releases the row
    /// mutex between phases (see `next_entry`); the unit tests below
    /// construct `Row` values directly and don't model that release.
    #[cfg(test)]
    fn next_in_cell<L: Loader>(
        loader: &L,
        row: &mut Row,
        cell: &CellId,
        mut prev_key: Option<Bytes>,
        reverse: bool,
        caches: &mut IndexIterCaches,
    ) -> Result<Option<(Bytes, GetResult)>, L::Error> {
        loop {
            let Some(plan) =
                Self::prepare_walk(loader, row, cell, prev_key.as_deref(), reverse, caches)?
            else {
                return Ok(None);
            };
            let walk_result = Self::execute_walk(
                &plan,
                &row.context.ks_config,
                &row.context.metrics,
                prev_key.clone(),
                reverse,
                caches,
                None,
            );
            match walk_result {
                WalkOutcome::Found(key, val) => {
                    let get_result = if let Some(entry) = row.try_entry_mut(cell) {
                        Self::lru_or_wal(entry, &key, val)
                    } else {
                        GetResult::WalPosition(val)
                    };
                    return Ok(Some((key, get_result)));
                }
                WalkOutcome::SkipDeleted(skip_key) => {
                    prev_key = Some(skip_key);
                }
                WalkOutcome::Exhausted => return Ok(None),
            }
        }
    }

    /// See Db::next_cell for documentation
    /// This function acquires row mutexes, should not be called by code that might hold row mutex
    pub fn next_cell(&self, context: &KsContext, cell: &CellId, reverse: bool) -> Option<CellId> {
        let ks = &context.ks_config;
        match (ks.key_type(), cell) {
            (KeyType::Uniform(config), CellId::Integer(cell)) => {
                config.next_cell(ks, *cell, reverse)
            }
            (KeyType::PrefixedUniform(_), CellId::Bytes(bytes)) => {
                let ks_table = self.ks_table(&context.ks_config);

                let next_cell = next_key_in_tree(&ks_table.cell_index.read(), bytes, reverse);

                next_cell.map(CellId::Bytes)
            }
            (KeyType::Uniform(_), CellId::Bytes(_)) => {
                panic!("next_cell with uniform key type but bytes cell id")
            }
            (KeyType::PrefixedUniform(_), CellId::Integer(_)) => {
                panic!("next_cell with prefix key type but integer cell id")
            }
        }
    }

    /// Drops all cells in the specified range for the given key space.
    ///
    /// This method removes all entries from cells between `from_cell` and `to_cell` (inclusive).
    /// For Array-based storage (Uniform key type), entries are cleared but remain in the array.
    /// For Tree-based storage (PrefixedUniform key type), entries are actually removed from the BTreeMap.
    ///
    /// The implementation waits for any pending async flush operations to complete before dropping cells.
    pub(crate) fn drop_cells_in_range(
        &self,
        context: &KsContext,
        from_cell: &CellId,
        to_cell: &CellId,
    ) {
        let mut current_cell = Some(from_cell.clone());

        while let Some(cell) = current_cell.as_ref() {
            let mut row = self.lock_cell_waiting_for_flush(context, cell);
            row.remove_entry(cell);

            if cell == to_cell {
                break;
            }

            current_cell = self.next_cell(context, cell, false);

            if let Some(ref next) = current_cell
                && next > to_cell
            {
                break;
            }
        }
    }

    pub fn report_entries_state(&self) {
        let mut states: HashMap<(String, &'static str), i64> = HashMap::new();
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
        let mut prev_keys = self.reported_entry_state_keys.lock();
        for (ks, state) in prev_keys.iter() {
            if !states.contains_key(&(ks.clone(), *state)) {
                self.metrics
                    .entry_state
                    .with_label_values(&[ks, state])
                    .set(0);
            }
        }
        for ((ks, state), value) in &states {
            self.metrics
                .entry_state
                .with_label_values(&[ks, state])
                .set(*value);
        }
        *prev_keys = states.into_keys().collect();
    }

    pub fn update_flushed_index(
        &self,
        context: &KsContext,
        cell: &CellId,
        original_index: Arc<IndexTable>,
        new_levels: IndexLevels,
    ) {
        self.with_promoted_entry(context, cell, |entry| {
            entry.update_flushed_index(original_index, new_levels);
        });
    }

    /// Called by the flusher after a `ForceRelocate` flush completes.
    /// Updates the entry's level state to reflect the relocated blob without
    /// requiring the original index.
    pub(crate) fn update_relocated_index(
        &self,
        context: &KsContext,
        cell: &CellId,
        new_levels: IndexLevels,
    ) {
        self.with_promoted_entry(context, cell, |entry| {
            entry.update_relocated_position(new_levels);
        });
    }

    /// Test-only view of an entry's state name (the state enum is private).
    #[cfg(test)]
    pub(crate) fn entry_state_for_test(&self, context: &KsContext, cell: &CellId) -> String {
        self.with_entry_for_test(context, cell, |entry| format!("{:?}", entry.state))
    }

    /// Test-only access to a cell's entry under its row lock, mirroring the
    /// flusher-completion callbacks: like them it runs `promote_pending`
    /// before handing the entry to `f`.
    #[cfg(test)]
    pub(crate) fn with_entry_for_test<R>(
        &self,
        context: &KsContext,
        cell: &CellId,
        f: impl FnOnce(&mut LargeTableEntry) -> R,
    ) -> R {
        self.with_promoted_entry(context, cell, f)
    }

    /// Locks the row containing `cell`, calls `promote_pending` on the entry,
    /// then hands it to `f`. Shared by flusher-completion callbacks.
    fn with_promoted_entry<R>(
        &self,
        context: &KsContext,
        cell: &CellId,
        f: impl FnOnce(&mut LargeTableEntry) -> R,
    ) -> R {
        let row = context.ks_config.mutex_for_cell(cell);
        let ks_table = self.ks_rows(&context.ks_config);
        let mut row = ks_table.lock(row, &context.large_table_contention);
        let entry = self.entry_mut(&mut row, cell);
        entry.promote_pending();
        f(entry)
    }

    /// Pass 1 of the two-pass snapshot flush: iterate all entries, queue async flushes
    /// for dirty entries whose `last_processed` is at or below `threshold_position`,
    /// and accumulate live-byte counts per index WAL file from current on-disk levels.
    ///
    /// The accumulator captures pre-flush levels — files written by the queued flushes
    /// land past the WAL position recorded by the caller before pass 1 began, so they
    /// are excluded by `RelocateFiles::from_accumulator`'s `exclude_from` filter.
    ///
    /// No IO is performed under cell locks.
    pub(crate) fn flush_and_accumulate<L: Loader>(
        &self,
        loader: &L,
        layout: &WalLayout,
        threshold_position: u64,
    ) -> HashMap<WalFileId, u64> {
        let mut accumulator: HashMap<WalFileId, u64> = HashMap::new();
        for ks_table in self.table.iter() {
            for mutex in ks_table.rows.mutexes() {
                let mut row = mutex.lock();
                for entry in row.entries.iter_mut() {
                    // Important - read last_processed_wal_position before promote_pending.
                    // See also comments in unload_if_ks_enabled.
                    let loader_last_processed = loader.last_processed_wal_position();
                    entry.promote_pending();
                    for pos in entry.levels.iter() {
                        let file_id = layout.locate_file(pos.offset());
                        *accumulator.entry(file_id).or_insert(0) += pos.frame_len() as u64;
                    }
                    entry.request_flush_if_behind(
                        &self.flusher,
                        loader_last_processed,
                        threshold_position,
                    );
                }
                // row lock released immediately — no IO under lock
            }
        }
        accumulator
    }

    /// Pass 2 of the two-pass snapshot flush: queue force-relocation flushes for
    /// entries with positions in low-occupancy files, wait for completion, then
    /// build the snapshot.
    pub(crate) fn relocate_and_snapshot<L: Loader>(
        &self,
        loader: &L,
        relocate_files: &RelocateFiles,
    ) -> LargeTableSnapshot {
        if !relocate_files.is_empty() {
            for ks_table in self.table.iter() {
                for mutex in ks_table.rows.mutexes() {
                    let mut row = mutex.lock();
                    for entry in row.entries.iter_mut() {
                        let loader_last_processed = loader.last_processed_wal_position();
                        entry.promote_pending();
                        entry.request_force_relocate(
                            &self.flusher,
                            loader_last_processed,
                            relocate_files,
                        );
                    }
                }
            }
            self.flusher.barrier();
        }
        self.snapshot(loader)
    }

    #[cfg(test)]
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

    #[cfg(test)]
    pub fn force_unload_clean(&self) {
        for ks_table in &self.table {
            for mutex in ks_table.rows.mutexes() {
                let mut row = mutex.lock();
                for entry in row.entries.iter_mut() {
                    if entry.state == LargeTableEntryState::Loaded {
                        entry.state = LargeTableEntryState::Unloaded;
                        entry.data = Default::default();
                    }
                }
            }
        }
    }
}

impl Row {
    /// Get mutable entry.
    /// You should normally use LargeTable:entry_mut rather than this function.
    pub fn entry_mut(&mut self, id: &CellId) -> (&mut LargeTableEntry, bool) {
        self.entries.entry_mut(id, &self.context)
    }

    pub fn try_entry_mut(&mut self, id: &CellId) -> Option<&mut LargeTableEntry> {
        self.entries.try_entry_mut(id, &self.context)
    }

    pub fn remove_entry(&mut self, id: &CellId) {
        self.entries.remove_entry(id, &self.context)
    }
}

pub trait Loader {
    type Error: std::fmt::Debug;

    fn load(&self, ks: &KeySpaceDesc, position: WalPosition) -> Result<IndexTable, Self::Error>;

    fn index_reader(&self, position: WalPosition) -> Result<WalRandomRead, Self::Error>;

    fn flush_supported(&self) -> bool;

    /// Write an on-disk index blob. Any `Removed` entries in `data` are
    /// persisted as tombstones. Callers writing the deepest level should call
    /// `IndexTable::clean_self` first to strip them.
    fn flush(&self, ks: KeySpace, data: &IndexTable) -> Result<WalPosition, Self::Error>;

    /// Returns the last WAL position that has been fully processed and committed.
    /// This position represents the highest WAL offset where all operations up to and
    /// including that offset have been successfully processed and their guards dropped.
    fn last_processed_wal_position(&self) -> LastProcessed;

    /// Returns the current WAL writer position (tail of the log).
    /// This is used for metrics and snapshot operations.
    fn current_wal_position(&self) -> u64;

    fn min_wal_position(&self) -> u64;
}

/// Splits an `lru_update` from the pending table into `(full_key, lru_value)`.
///
/// When `lru_update` is `None` (LRU not configured or deletion), the reduced key
/// is returned as `full_key` — it won't be stored in the LRU anyway.
fn split_lru_update(
    reduced_key: &Bytes,
    lru_update: Option<(Bytes, Bytes)>,
) -> (Bytes, Option<Bytes>) {
    match lru_update {
        Some((full_key, value)) => (full_key, Some(value)),
        None => (reduced_key.clone(), None),
    }
}

impl LargeTableEntry {
    pub fn new_unloaded(
        context: KsContext,
        cell: CellId,
        levels: IndexLevels,
        unload_jitter: usize,
        bloom_filter: Option<BloomFilter>,
    ) -> Self {
        Self::new_with_state(
            context,
            cell,
            LargeTableEntryState::Unloaded,
            levels,
            unload_jitter,
            bloom_filter,
        )
    }

    pub fn new_empty(context: KsContext, cell: CellId, unload_jitter: usize) -> Self {
        let bloom_filter = context.ks_config.bloom_filter().map(|params| {
            BloomFilter::with_false_pos(params.rate as f64).expected_items(params.count as usize)
        });
        Self::new_with_state(
            context,
            cell,
            LargeTableEntryState::Empty,
            IndexLevels::new(),
            unload_jitter,
            bloom_filter,
        )
    }

    fn new_with_state(
        context: KsContext,
        cell: CellId,
        state: LargeTableEntryState,
        levels: IndexLevels,
        unload_jitter: usize,
        bloom_filter: Option<BloomFilter>,
    ) -> Self {
        let value_lru = context.ks_config.value_cache_size().map(LruCache::new);
        Self {
            context,
            cell,
            state,
            levels,
            data: Default::default(),
            pending_data: Default::default(),
            bloom_filter,
            unload_jitter,
            pending_last_processed: None,
            last_processed: LastProcessed::none(),
            value_lru,
            last_reported_key_bytes: 0,
            last_reported_flat_bytes: 0,
            last_reported_dirty_count: 0,
        }
    }

    pub fn from_snapshot_data(
        context: KsContext,
        cell: CellId,
        entry_data: &SnapshotEntryData,
        unload_jitter: usize,
        bloom_filter: Option<BloomFilter>,
    ) -> Self {
        let levels = entry_data.levels();
        let mut entry = if levels.is_empty() {
            LargeTableEntry::new_empty(context, cell, unload_jitter)
        } else {
            LargeTableEntry::new_unloaded(
                context,
                cell,
                levels.clone(),
                unload_jitter,
                bloom_filter,
            )
        };
        entry.last_processed = entry_data.last_processed;
        entry
    }

    /// Checks if the update is potentially stale and the operation needs to be canceled.
    /// This can happen if there are multiple concurrent updates for the same key.
    /// Returns true if the update is outdated and should be skipped
    fn skip_stale_update(&self, key: &Bytes, wal_position: &WalPosition, op: &'static str) -> bool {
        // check the existing loaded WAL position first, if present
        let skip = if let Some(last_position) = self.data.get_update_position(key) {
            &last_position > wal_position
        } else {
            false
        };
        if skip {
            self.context
                .metrics
                .skip_stale_update
                .with_label_values(&[self.context.name(), op])
                .inc();
        }
        skip
    }

    /// Transitions the entry into a dirty state for a write at WAL position `v`.
    ///
    /// Also anchors `last_processed` at `v.offset()` when the entry was
    /// previously `Empty`: without this, a freshly-dirty entry carries
    /// `last_processed = 0` until its first flush and pins the snapshot's
    /// `replay_from` to 0 — violating monotonicity of `cr.last_position`.
    fn mark_dirty(&mut self, v: WalPosition) {
        if matches!(self.state, LargeTableEntryState::Empty) {
            self.last_processed = LastProcessed::new(v.offset());
        }
        self.state.mark_dirty();
    }

    /// Returns true if successful, false if update was skipped.
    /// The lru_value must be provided if LRU is configured.
    pub fn insert(
        &mut self,
        k: Bytes,
        v: WalPosition,
        full_key: Bytes,
        lru_value: Option<&Bytes>,
    ) -> bool {
        if self.skip_stale_update(&k, &v, "insert") {
            return false;
        }
        self.mark_dirty(v);
        self.insert_bloom_filter(&k);
        self.data.make_mut().insert(k.clone(), v);
        self.report_loaded_keys_count();

        if let Some(value_lru) = &mut self.value_lru {
            let lru_value = lru_value.expect("lru_value must be provided if LRU is configured");
            let delta: i64 = (k.len() + full_key.len() + lru_value.len()) as i64;
            let previous = value_lru.push(k, (full_key, lru_value.clone()));
            Self::update_lru_metric(&self.context, previous, delta);
        }
        true
    }

    /// Returns true if successful, false if update was skipped
    pub fn remove(&mut self, k: Bytes, v: WalPosition) -> bool {
        if self.skip_stale_update(&k, &v, "remove") {
            return false;
        }
        self.mark_dirty(v);

        // Remove from LRU cache if enabled
        if let Some(value_lru) = &mut self.value_lru {
            let previous = value_lru.pop(&k);
            Self::update_lru_metric(
                &self.context,
                previous.map(|(full_key, v)| (k.clone(), (full_key, v))),
                0,
            );
        }

        self.data.make_mut().remove(k, v);
        self.report_loaded_keys_count();
        true
    }

    /// Drain a [`CellReplayBuffer`] into this entry. See
    /// `LargeTable::apply_replay_buffer` for the broader rationale.
    ///
    /// `cutoff` is the WAL offset above which entries belong to an
    /// incomplete `BatchStart` group and must be dropped. Entries are
    /// pushed in WAL order, so the cutoff resolves to a single
    /// `partition_point` + `truncate`.
    ///
    /// During replay the cell is in `Empty` or `Unloaded` state (no
    /// `maybe_load` call) and `entry.data` is the default IndexTable, so we
    /// can replace it wholesale with a flat buffer built from the sorted
    /// entries. `mark_dirty` is fed the *first* WAL position observed for
    /// the cell so that `last_processed` matches what the per-record path
    /// would have anchored on the `Empty → Dirty` transition.
    fn apply_replay_buffer(&mut self, mut entries: CellReplayBuffer, cutoff: u64) {
        if entries.is_empty() {
            return;
        }
        // Discard entries from an incomplete `BatchStart` group. Entries
        // were pushed in WAL order so they're monotonic by offset; a
        // single binary search finds the cut.
        if cutoff != u64::MAX {
            let cut = entries.partition_point(|(_, iwp)| iwp.offset() < cutoff);
            entries.truncate(cut);
            if entries.is_empty() {
                return;
            }
        }
        debug_assert!(
            self.data.is_empty(),
            "apply_replay_buffer expects a fresh entry — replay never auto-loads L0",
        );
        // `entries` was pushed in WAL order, so `entries[0]` has the earliest
        // position for this cell — anchor `mark_dirty` on it. Only the offset
        // matters for `last_processed`; `len` is irrelevant.
        let first_offset = entries[0].1.offset();
        self.mark_dirty(WalPosition::new(first_offset, 0));
        // Sort by (key ASC, offset DESC) so that for runs of equal keys the
        // highest-position (latest) write comes first; `dedup_by` then keeps
        // exactly that entry per key.
        entries.sort_unstable_by(|a, b| {
            a.0.as_ref()
                .cmp(b.0.as_ref())
                .then_with(|| b.1.offset().cmp(&a.1.offset()))
        });
        entries.dedup_by(|a, b| a.0.as_ref() == b.0.as_ref());
        if let Some(bloom_filter) = &mut self.bloom_filter {
            // Match the live path: only inserts populate the bloom; tombstones
            // do not. A key whose last replay write is a `Removed` tombstone
            // and that has no value in any deeper level would otherwise show
            // up as a false-positive "maybe present" until the next bloom
            // rebuild.
            for (key, iwp) in entries.iter() {
                if !iwp.is_removed() {
                    bloom_filter.insert(&key.as_ref());
                }
            }
        }
        self.data = ArcCow::new_owned(IndexTable::from_sorted_entries(entries));
        self.report_loaded_keys_count();
    }

    fn promote_pending(&mut self) {
        let (committed, removed) = self.pending_data.take_committed();
        if removed > 0 {
            self.context.pending_table_len.add(-(removed as i64));
        }
        self.apply_committed_changes(committed);
    }

    /// Promotes pending entries for a single key.
    fn promote_pending_for(&mut self, key: &Bytes) {
        let (committed, removed) = self.pending_data.take_committed_for(key);
        if removed > 0 {
            self.context.pending_table_len.add(-(removed as i64));
        }
        self.apply_committed_changes(committed);
    }

    fn apply_committed_changes(&mut self, committed: Vec<CommittedChange>) {
        for committed_change in committed {
            // todo perf - report_loaded_keys_count and make_mut calls can be done once
            // Changes from a committed batch go over the same skip_stale_update logic as regular updates.
            if committed_change.is_modified {
                let (full_key, lru_value) =
                    split_lru_update(&committed_change.key, committed_change.lru_update);
                self.insert(
                    committed_change.key,
                    committed_change.value,
                    full_key,
                    lru_value.as_ref(),
                );
            } else {
                self.remove(committed_change.key, committed_change.value);
            }
        }
    }

    /// Promotes pending entries and checks if flush is needed due to too many dirty keys.
    /// Returns the number of remaining pending entries.
    fn promote_pending_and_check_flush<L: Loader>(
        &mut self,
        loader: &L,
        flusher: &IndexFlusher,
    ) -> Result<usize, L::Error> {
        self.promote_pending();
        let remaining_pending = self.pending_data.len();
        let should_flush = loader.flush_supported() && self.state.is_dirty() && {
            let dirty_count = self.data.dirty_count();
            self.context
                .excess_dirty_keys(dirty_count.saturating_sub(self.unload_jitter))
        };
        if should_flush {
            self.unload_if_ks_enabled(flusher, loader)?;
        }
        Ok(remaining_pending)
    }

    fn report_loaded_keys_count(&mut self) {
        let new_bytes = self.data.total_key_bytes() as i64;
        let delta = new_bytes - self.last_reported_key_bytes;
        self.last_reported_key_bytes = new_bytes;
        self.context.loaded_key_bytes.add(delta);

        let new_flat_bytes = self.data.flat_key_bytes() as i64;
        let flat_delta = new_flat_bytes - self.last_reported_flat_bytes;
        self.last_reported_flat_bytes = new_flat_bytes;
        self.context.flat_index_bytes.add(flat_delta);

        let new_dirty = self.data.dirty_count() as i64;
        let dirty_delta = new_dirty - self.last_reported_dirty_count;
        self.last_reported_dirty_count = new_dirty;
        self.context.dirty_keys.add(dirty_delta);
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

    fn update_lru_metric(
        context: &KsContext,
        previous: Option<(Bytes, (Bytes, Bytes))>,
        mut delta: i64,
    ) {
        if let Some((p_key, (p_full_key, p_value))) = previous {
            delta -= (p_key.len() + p_full_key.len() + p_value.len()) as i64;
        }
        context.value_cache_size.add(delta);
    }

    pub fn get(&self, k: &[u8]) -> Option<WalPosition> {
        if self.state == LargeTableEntryState::Unloaded {
            panic!("Can't get in unloaded state");
        }
        self.data.get(k)
    }

    /// Checkpoint variant of [`Self::get`]: only considers overlay/flat index
    /// positions processed relative to `last_processed`. Returns `None` when
    /// the key has only positions that postdate the checkpoint frontier, so the
    /// caller falls through to deeper on-disk levels. See [`IndexTable::get_at`].
    pub fn get_at(&self, k: &[u8], last_processed: LastProcessed) -> Option<WalPosition> {
        if self.state == LargeTableEntryState::Unloaded {
            panic!("Can't get in unloaded state");
        }
        self.data.get_at(k, last_processed)
    }

    /// On-disk level positions the read path still needs to consult after
    /// probing `self.data`. For Loaded/DirtyLoaded, `maybe_load` folded L0
    /// into `self.data`, so only L1+ remain; everything else walks all levels.
    ///
    /// When the cell is sharded, every shard blob is included. Point-lookup
    /// callers should use [`disk_levels_to_walk_for_key`] and iterator
    /// callers should use [`disk_levels_to_walk_for_iter`] so only the
    /// covering shard is opened.
    pub(crate) fn disk_levels_to_walk(&self) -> SmallVec<[WalPosition; INLINE_LEVELS]> {
        match self.state {
            LargeTableEntryState::Loaded | LargeTableEntryState::DirtyLoaded => {
                self.levels.iter_below_l0().collect()
            }
            _ => self.levels.iter().collect(),
        }
    }

    /// Point-lookup specialization of [`disk_levels_to_walk`]: for a sharded
    /// cell, returns at most one shard — the one whose `[min, max]` covers
    /// `key` — instead of every shard. When `key` lies outside every
    /// shard's content range the shard is omitted entirely, so the caller
    /// can short-circuit a wasted on-disk read. For unsharded cells the
    /// result is identical to [`disk_levels_to_walk`].
    pub(crate) fn disk_levels_to_walk_for_key(
        &self,
        key: &[u8],
    ) -> SmallVec<[WalPosition; INLINE_LEVELS]> {
        if !self.levels.is_sharded() {
            return self.disk_levels_to_walk();
        }
        let mut out: SmallVec<[WalPosition; INLINE_LEVELS]> = SmallVec::new();
        let below_l0_only = matches!(
            self.state,
            LargeTableEntryState::Loaded | LargeTableEntryState::DirtyLoaded
        );
        if !below_l0_only && let Some(l0) = self.levels.l0() {
            out.push(l0);
        }
        if let Some(shard) = self.levels.shard_for(key) {
            out.push(shard.position);
        }
        out
    }

    /// Iterator-step specialization of [`disk_levels_to_walk`]: for a
    /// sharded cell, opens **one** shard per step — the one whose
    /// content range contains the iterator's next key in
    /// `prev_key`/`reverse` direction. Returns an empty list when the
    /// cell's on-disk side is exhausted in that direction (no shard
    /// has a content key past `prev_key`). For unsharded cells the
    /// result matches [`disk_levels_to_walk`].
    pub(crate) fn disk_levels_to_walk_for_iter(
        &self,
        prev_key: Option<&[u8]>,
        reverse: bool,
    ) -> SmallVec<[WalPosition; INLINE_LEVELS]> {
        if !self.levels.is_sharded() {
            return self.disk_levels_to_walk();
        }
        let shard = if reverse {
            self.levels.shard_for_iter_reverse(prev_key)
        } else {
            self.levels.shard_for_iter_forward(prev_key)
        };

        let mut out: SmallVec<[WalPosition; INLINE_LEVELS]> = SmallVec::new();
        let below_l0_only = matches!(
            self.state,
            LargeTableEntryState::Loaded | LargeTableEntryState::DirtyLoaded
        );
        if !below_l0_only && let Some(l0) = self.levels.l0() {
            out.push(l0);
        }
        if let Some(s) = shard {
            out.push(s.position);
        }
        out
    }

    /// See IndexTable::next_entry for documentation.
    pub fn next_entry(
        &self,
        prev_key: Option<Bytes>,
        reverse: bool,
    ) -> Option<(Bytes, WalPosition)> {
        if self.state == LargeTableEntryState::Unloaded {
            panic!("Can't next_entry in unloaded state");
        }
        self.data.next_entry(prev_key, reverse)
    }

    pub fn maybe_load<L: Loader>(&mut self, loader: &L) -> Result<(), L::Error> {
        let Some(unloaded) = self.as_unloaded_state() else {
            return Ok(());
        };
        // Load only the L0 slot — never L1. Reading L1 into `self.data` here
        // would make the next flush treat L1 content as "L0 + overlay" and
        // rewrite L1 as the new L0. On the read path, lookups already walk
        // `levels` level-by-level, so L1 is reached via on-disk reads.
        let mut data = match self.levels.l0() {
            Some(pos) => loader.load(&self.context.ks_config, pos)?,
            None => IndexTable::default(),
        };
        let is_dirty = match unloaded {
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
        // Levels unchanged — maybe_load doesn't alter the on-disk state.
        if is_dirty {
            self.state = LargeTableEntryState::DirtyLoaded;
        } else {
            self.state = LargeTableEntryState::Loaded;
        }
        Ok(())
    }

    /// Pass 1 helper: queues a normal async flush if this entry is dirty and its
    /// `last_processed` is at or below `threshold_position`.
    /// No IO is performed — flushes are dispatched asynchronously.
    pub fn request_flush_if_behind(
        &mut self,
        flusher: &IndexFlusher,
        loader_last_processed: LastProcessed,
        threshold_position: u64,
    ) {
        if self.pending_last_processed.is_some() {
            return;
        }
        if !self.state.is_dirty() {
            return;
        }
        if self.last_processed.as_u64() > threshold_position {
            return;
        }
        self.pending_last_processed = Some(loader_last_processed);
        self.context
            .metrics
            .snapshot_force_unload
            .with_label_values(&[self.context.name()])
            .inc();
        let flush_kind = self
            .flush_kind(HashSet::new())
            .expect("entry must be dirty here (state checked above)");
        flusher.request_flush(self.context.id(), self.cell.clone(), flush_kind);
    }

    /// Pass 2 helper: queues a force-relocation flush if any populated level lives
    /// in a low-occupancy file. For dirty entries we issue a normal flush instead —
    /// the new flush will naturally land past the relocation cutoff.
    /// No IO is performed — flushes are dispatched asynchronously.
    pub fn request_force_relocate(
        &mut self,
        flusher: &IndexFlusher,
        loader_last_processed: LastProcessed,
        relocate_files: &RelocateFiles,
    ) {
        if self.pending_last_processed.is_some() {
            return;
        }
        // Check every populated level (not just the freshest): a `[L0, L1]` cell
        // may have an ancient L1 in a sparse file even when L0 is fresh.
        let relocate_positions: HashSet<WalPosition> = self
            .levels
            .iter()
            .filter(|p| relocate_files.contains(p))
            .collect();
        if relocate_positions.is_empty() {
            return;
        }
        self.context
            .metrics
            .snapshot_forced_relocation
            .with_label_values(&[self.context.name()])
            .inc();
        self.pending_last_processed = Some(loader_last_processed);
        self.context
            .metrics
            .snapshot_force_unload
            .with_label_values(&[self.context.name()])
            .inc();
        if self.state.is_dirty() {
            // Dirty entry: dispatch a normal Flush carrying `relocate_positions`
            // so the flusher's post-pass rewrites any blob the merge/promote
            // leaves in place (e.g. untouched re-sharding shards or a stale L1
            // under the L0-write branch).
            let kind = self
                .flush_kind(relocate_positions)
                .expect("entry must be dirty here (state checked above)");
            flusher.request_flush(self.context.id(), self.cell.clone(), kind);
        } else {
            // Clean entry: dispatch the levels plus the subset of positions
            // in low-occupancy files. See `FlushKind::ForceRelocate` for the
            // two semantics.
            flusher.request_flush(
                self.context.id(),
                self.cell.clone(),
                FlushKind::ForceRelocate {
                    levels: self.levels.clone(),
                    relocate_positions,
                },
            );
        }
    }

    /// Updates the entry's level state after a `ForceRelocate` flush completes.
    /// Unlike `update_flushed_index`, this handles clean entries (Unloaded/Loaded)
    /// since forced relocation re-flushes an already-clean entry to a new position.
    pub fn update_relocated_position(&mut self, new_levels: IndexLevels) {
        let last_processed = self.commit_pending_last_processed();
        // Force-relocate on an unsharded cell collapses all populated levels
        // into a single new blob, so `new_levels` is single-level. On a
        // sharded cell it rewrites a subset of shards in place, preserving
        // the sharded shape. Overwriting `self.levels` wholesale is
        // intentional in both cases: any pre-flush blob that was rewritten
        // has been replaced at a fresh position.
        debug_assert!(
            new_levels.is_single_level() || new_levels.is_sharded(),
            "force-relocate must produce either a single-level blob or a sharded set (got {:?})",
            new_levels,
        );
        match self.state {
            LargeTableEntryState::Unloaded => {
                self.levels = new_levels;
            }
            LargeTableEntryState::Loaded => {
                // Unload after relocation (snapshot context)
                self.levels = new_levels;
                self.state = LargeTableEntryState::Unloaded;
                self.data = Default::default();
                self.report_loaded_keys_count();
            }
            LargeTableEntryState::DirtyUnloaded => {
                // A write arrived while the relocation was in flight. The
                // overlay holds only those writes — none are covered by the
                // relocated blob — so it stays intact over the new levels.
                self.levels = new_levels;
            }
            LargeTableEntryState::DirtyLoaded => {
                // A write arrived while the relocation was in flight, on a
                // cell whose overlay covers the *old* L0 blob.
                //
                // Sharded relocation rewrites blobs in place with identical
                // content, and an unsharded cell with nothing below L0
                // collapses into a blob whose content the overlay already
                // covers — in both cases the loaded overlay remains a
                // superset of the L0 slot and the cell can stay loaded.
                let overlay_still_covers_l0 =
                    new_levels.is_sharded() || self.levels.iter_below_l0().next().is_none();
                self.levels = new_levels;
                if overlay_still_covers_l0 {
                    return;
                }
                // The relocated blob folded the levels below L0 into the L0
                // slot, so the overlay no longer covers it. Keeping the cell
                // loaded would hide the folded-in keys from reads (loaded
                // cells skip the L0 slot in `disk_levels_to_walk`) and drop
                // them at the next flush (which clones the overlay as the
                // cell's entire L0 content). Retain the racing writes and
                // demote, so reads and flushes consult the new blob again.
                //
                // The request frontier is the exact cutoff: the cell was
                // clean at request time and in-flight writes hold their WAL
                // guards until applied, so every overlay entry sits at or
                // above it (no key straddles the frontier, making the
                // per-key-latest `retain` equivalent to per-position here);
                // everything below it is covered by the new blob, and the
                // promote job never moves entries at or above a pending
                // frontier into flat.
                self.data.make_mut().retain_unprocessed(last_processed);
                self.report_loaded_keys_count();
                if self.data.is_empty() {
                    self.state = LargeTableEntryState::Unloaded;
                } else {
                    self.state = LargeTableEntryState::DirtyUnloaded;
                }
            }
            LargeTableEntryState::Empty => {
                panic!("update_relocated_position called in Empty state")
            }
        }
    }

    /// Performs a synchronous flush regardless of the config setting.
    /// If forced_relocation is set, the flush happens even for clean state.
    /// If unload is false, the in-memory index is kept after the flush (no eviction from cache).
    /// Caller is responsible for checking two things:
    /// * pending_last_processed is None
    fn sync_flush<L: Loader>(
        &mut self,
        loader: &L,
        forced_relocation: bool,
        relocation_updates: Option<RelocationUpdates>,
        relocation_cutoff: Option<u64>,
    ) -> Result<(), L::Error> {
        assert!(
            self.pending_last_processed.is_none(),
            "sync_flush is called while async flush is in progress"
        );
        let last_processed = loader.last_processed_wal_position();
        // Important: promote pending should be called after last_processed_wal_position read from loader
        // See also comments in unload_if_ks_enabled.
        self.promote_pending();
        let flush_kind = match self.flush_kind(HashSet::new()) {
            Some(kind) => kind,
            None => {
                if !forced_relocation {
                    return Ok(()); // Not in a flushable state and relocation was not requested
                }
                self.maybe_load(loader)?;
                FlushKind::Flush {
                    dirty: self.data.clone_shared(),
                    current_levels: self.levels.clone(),
                    loaded: true,
                    relocate_positions: HashSet::new(),
                }
            }
        };

        // Perform a synchronous flush
        let Some((_original_index, new_levels)) = IndexFlusherThread::handle_command(
            loader,
            &IndexFlushCommand::new(self.context.id(), self.cell.clone(), flush_kind),
            &self.context,
            relocation_updates,
            relocation_cutoff,
        ) else {
            return Ok(());
        };
        self.last_processed = last_processed;
        if self.context.ks_config.unloading_disabled() && self.state.is_loaded() {
            // An unloading-disabled cell is never evicted, so its reads stay on
            // the in-memory overlay and never fall through to the freshly
            // written blob. Relocation just moved this cell's as-of values to
            // new WAL positions and reclaimed the old frames, so re-point the
            // overlay: adopt the blob's flat (the as-of view, incl. relocated
            // copies) and keep only post-frontier writes in the BTree. Without
            // this the overlay would keep below-frontier positions whose frames
            // are now gone. `clear_after_flush` below then just fixes up state
            // (its loaded branch keeps `self.data` as-is).
            let blob = match new_levels.l0() {
                Some(pos) => loader.load(&self.context.ks_config, pos)?,
                None => IndexTable::default(),
            };
            self.data.make_mut().rebase_on_as_of(blob, last_processed);
        }
        self.clear_after_flush(new_levels, last_processed, true);
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
                self.sync_flush(loader, false, None, None)?;
            } else {
                // Perform async flush - store the captured value for later
                self.pending_last_processed = Some(loader.last_processed_wal_position());
                // Important - it is required to call promote_pending once
                // more after acquiring Loader::last_processed_wal_position.
                // This ensures that all batch writes committed
                // before last_processed_wal_position are propagated to the index.
                self.promote_pending();
                let flush_kind = self
                    .flush_kind(HashSet::new())
                    .expect("unload_if_ks_enabled is called in clean state");
                flusher.request_flush(self.context.id(), self.cell.clone(), flush_kind);
            }
        }
        Ok(())
    }

    /// Consumes the `pending_last_processed` captured when the async flush
    /// was requested and commits it to `self.last_processed`. Returns the
    /// value so callers can reuse it without re-reading `self`. Panics if
    /// no pending value is set — call only from paths that match a prior
    /// `pending_last_processed = Some(...)` assignment.
    fn commit_pending_last_processed(&mut self) -> LastProcessed {
        let pending = self
            .pending_last_processed
            .take()
            .expect("commit_pending_last_processed called while pending_last_processed is not set");
        self.last_processed = pending;
        pending
    }

    fn clear_after_flush(
        &mut self,
        new_levels: IndexLevels,
        last_processed: LastProcessed,
        unload: bool,
    ) {
        self.levels = new_levels;
        if !unload && self.state == LargeTableEntryState::DirtyUnloaded {
            // Keep dirty entries in memory; just update the on-disk index position.
            // Transitioning to DirtyLoaded would be wrong — self.data only has the dirty
            // overlay, not the full merged index that was written to disk.
            return;
        }
        if self.state.is_loaded() && (!unload || self.context.ks_config.unloading_disabled()) {
            if self.data.has_unprocessed(last_processed) {
                // Some entries remain (newer than last_processed), keep state as DirtyLoaded
                self.state = LargeTableEntryState::DirtyLoaded;
            } else {
                // All entries are processed — if nothing on-disk sits below L0,
                // strip tombstones from `self.data` (in Loaded state `self.data`
                // is authoritative for L0; a tombstone against nothing is dead
                // weight and is canonicalised away to keep the Loaded overlay
                // in clean form).
                //
                // With an L1 below, tombstones in `self.data` shadow L1 entries
                // on reads — dropping them here would let L1 entries resurface
                // (the flusher keeps tombstones in the new L0 blob under the
                // same invariant; see `flusher.rs` "deepest level" comment).
                if self.levels.iter_below_l0().next().is_none() {
                    self.data.make_mut().clean_self();
                }
                self.report_loaded_keys_count();
                self.state = LargeTableEntryState::Loaded;
            }
            return;
        }
        // Unloading enabled: retain only entries with offset >= last_processed
        self.data.make_mut().retain_unprocessed(last_processed);
        self.report_loaded_keys_count();

        // If all entries were removed, update state to Unloaded
        if self.data.is_empty() {
            self.state = LargeTableEntryState::Unloaded;
            self.context
                .metrics
                .flush_update
                .with_label_values(&["clear"])
                .inc();
        } else {
            // Some entries remain, keep state as DirtyUnloaded
            self.state = LargeTableEntryState::DirtyUnloaded;
            self.context
                .metrics
                .flush_update
                .with_label_values(&["partial"])
                .inc();
        }
    }

    pub fn update_flushed_index(
        &mut self,
        original_index: Arc<IndexTable>,
        new_levels: IndexLevels,
    ) {
        // For unmerge, we always use the actual last_processed value (not u64::MAX)
        // to ensure we only remove entries that were actually committed
        match self.state {
            LargeTableEntryState::DirtyUnloaded => {}
            LargeTableEntryState::DirtyLoaded => {}
            LargeTableEntryState::Empty => panic!("update_merged_index in Empty state"),
            LargeTableEntryState::Unloaded => panic!("update_merged_index in Unloaded state"),
            LargeTableEntryState::Loaded => panic!("update_merged_index in Loaded state"),
        }
        // Now that flush is complete, commit pending_last_processed.
        let pending_last_processed = self.commit_pending_last_processed();
        if self.data.same_shared(&original_index) {
            self.clear_after_flush(new_levels, pending_last_processed, true);
        } else {
            self.data
                .make_mut()
                .unmerge_flushed(&original_index, pending_last_processed);
            self.report_loaded_keys_count();
            self.levels = new_levels;
            self.state = LargeTableEntryState::DirtyUnloaded;
            self.context
                .metrics
                .flush_update
                .with_label_values(&["unmerge"])
                .inc();
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.state, LargeTableEntryState::Empty)
    }

    /// Clears this entry, setting its state to Empty and dropping all data.
    pub(crate) fn clear(&mut self) {
        self.state = LargeTableEntryState::Empty;
        self.levels = IndexLevels::new();
        self.data = Default::default();
        if let Some(ref mut bloom) = self.bloom_filter {
            bloom.clear();
        }
        // Drop cached values too. A cleared cell has no live entries, but the
        // read path consults the value LRU *before* the `Empty` state check, so
        // a retained entry would serve a stale value for a dropped key.
        if let Some(value_lru) = &mut self.value_lru {
            let freed: i64 = value_lru
                .iter()
                .map(|(k, (full_key, value))| (k.len() + full_key.len() + value.len()) as i64)
                .sum();
            value_lru.clear();
            self.context.value_cache_size.add(-freed);
        }
        self.last_processed = LastProcessed::none();
        self.report_loaded_keys_count();
    }

    pub fn flush_kind(&mut self, relocate_positions: HashSet<WalPosition>) -> Option<FlushKind> {
        match self.state {
            LargeTableEntryState::Empty => None,
            LargeTableEntryState::Unloaded => None,
            LargeTableEntryState::Loaded => None,
            LargeTableEntryState::DirtyUnloaded => Some(FlushKind::Flush {
                dirty: self.data.clone_shared(),
                current_levels: self.levels.clone(),
                loaded: false,
                relocate_positions,
            }),
            LargeTableEntryState::DirtyLoaded => Some(FlushKind::Flush {
                dirty: self.data.clone_shared(),
                current_levels: self.levels.clone(),
                loaded: true,
                relocate_positions,
            }),
        }
    }

    /// Returns true if the in-memory index is currently loaded (Loaded or DirtyLoaded).
    #[cfg(test)]
    pub fn is_index_loaded(&self) -> bool {
        matches!(
            self.state,
            LargeTableEntryState::Loaded | LargeTableEntryState::DirtyLoaded
        )
    }

    /// Returns the on-disk index blobs for this cell.
    pub fn levels(&self) -> &IndexLevels {
        &self.levels
    }

    /// If this entry is in an unloaded state, returns which flavor of
    /// unloaded it is. Returns `None` for loaded / empty states.
    fn as_unloaded_state(&self) -> Option<UnloadedState> {
        match self.state {
            LargeTableEntryState::Empty => None,
            LargeTableEntryState::Unloaded => Some(UnloadedState::Clean),
            LargeTableEntryState::Loaded => None,
            LargeTableEntryState::DirtyUnloaded => Some(UnloadedState::Dirty),
            LargeTableEntryState::DirtyLoaded => None,
        }
    }
}

impl LargeTableEntryState {
    /// Pure state-label transition for the dirty edge. Callers must go
    /// through `LargeTableEntry::mark_dirty`, which also initializes
    /// `last_processed` on the `Empty → DirtyLoaded` edge and owns the
    /// `levels` field.
    fn mark_dirty(&mut self) {
        match self {
            LargeTableEntryState::Empty => *self = LargeTableEntryState::DirtyLoaded,
            LargeTableEntryState::Loaded => *self = LargeTableEntryState::DirtyLoaded,
            LargeTableEntryState::DirtyLoaded => {}
            LargeTableEntryState::Unloaded => *self = LargeTableEntryState::DirtyUnloaded,
            LargeTableEntryState::DirtyUnloaded => {}
        }
    }

    /// Empty, Loaded and DirtyLoaded is considered 'loaded' state.
    /// No disk lookup is needed when accessing entry in this state.
    pub fn is_loaded(&self) -> bool {
        match self {
            LargeTableEntryState::Empty => true,
            LargeTableEntryState::Unloaded => false,
            LargeTableEntryState::Loaded => true,
            LargeTableEntryState::DirtyUnloaded => false,
            LargeTableEntryState::DirtyLoaded => true,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, LargeTableEntryState::Empty)
    }

    /// Returns whether wal needs to be replayed from this entry's
    /// `last_processed` to restore the large-table entry.
    pub fn is_dirty(&self) -> bool {
        match self {
            LargeTableEntryState::Empty => false,
            LargeTableEntryState::Unloaded => false,
            LargeTableEntryState::Loaded => false,
            LargeTableEntryState::DirtyUnloaded => true,
            LargeTableEntryState::DirtyLoaded => true,
        }
    }

    #[allow(dead_code)]
    pub fn name(&self) -> &'static str {
        match self {
            LargeTableEntryState::Empty => "empty",
            LargeTableEntryState::Unloaded => "unloaded",
            LargeTableEntryState::Loaded => "loaded",
            LargeTableEntryState::DirtyUnloaded => "dirty_unloaded",
            LargeTableEntryState::DirtyLoaded => "dirty_loaded",
        }
    }
}

#[derive(Debug)]
pub enum GetResult {
    /// LRU hit: `(full_key, value)`. `full_key` is the original (non-reduced) WAL
    /// key. For non-key-reduction keyspaces, `full_key == index_key`.
    Value(Bytes, Bytes),
    WalPosition(WalPosition),
    NotFound,
}

impl GetResult {
    pub fn is_found(&self) -> bool {
        match self {
            GetResult::Value(..) => true,
            GetResult::WalPosition(_) => true,
            GetResult::NotFound => false,
        }
    }

    #[cfg(test)]
    pub fn unwrap_wal_position(self) -> WalPosition {
        match self {
            GetResult::WalPosition(p) => p,
            other => panic!("expected WalPosition, got {other:?}"),
        }
    }
}

enum UnloadedState {
    Dirty,
    Clean,
}

impl Default for LargeTableEntryState {
    fn default() -> Self {
        Self::Empty
    }
}

impl Entries {
    /// Return cell for the given CellId. Create cell if it does not exist for byte-addressed cell spaces.
    /// Returns the cell and whether cell was just crated.
    pub fn entry_mut(
        &mut self,
        cell_id: &CellId,
        context: &KsContext,
    ) -> (&mut LargeTableEntry, bool) {
        match self {
            Entries::Array(num_mutexes, arr) => {
                let CellId::Integer(cell) = cell_id else {
                    panic!("Invalid cell id for array entry list: {cell_id:?}");
                };
                let offset = *cell / *num_mutexes;
                (arr.get_mut(offset).unwrap(), false)
            }
            Entries::Tree(tree) => {
                let CellId::Bytes(cell) = cell_id else {
                    panic!("Invalid cell id for tree entry list: {cell_id:?}");
                };
                match tree.entry(cell.clone()) {
                    Entry::Occupied(o) => (o.into_mut(), false),
                    Entry::Vacant(v) => {
                        let unload_jitter = context
                            .config
                            .gen_dirty_keys_jitter(&mut ThreadRng::default());
                        let new_entry = LargeTableEntry::new_empty(
                            context.clone(),
                            cell_id.clone(),
                            unload_jitter,
                        );
                        (v.insert(new_entry), true)
                    }
                }
            }
        }
    }

    pub fn try_entry_mut(
        &mut self,
        cell_id: &CellId,
        context: &KsContext,
    ) -> Option<&mut LargeTableEntry> {
        match self {
            Entries::Array(_, _) => {
                let (entry, _) = self.entry_mut(cell_id, context);
                Some(entry)
            }
            Entries::Tree(tree) => {
                let CellId::Bytes(cell) = cell_id else {
                    panic!("Invalid cell id for tree entry list: {cell_id:?}");
                };
                tree.get_mut(cell)
            }
        }
    }

    /// Removes an entry from the container.
    /// For Array-based entries, this clears the entry but does not remove it.
    /// For Tree-based entries, this actually removes the entry from the BTreeMap.
    ///
    /// Caller must ensure entry does not have pending async flush, otherwise this function panics.
    pub fn remove_entry(&mut self, cell_id: &CellId, context: &KsContext) {
        match self {
            Entries::Array(_, _) => {
                // For arrays, we can't remove entries, only clear them
                if let Some(entry) = self.try_entry_mut(cell_id, context) {
                    assert!(
                        entry.pending_last_processed.is_none(),
                        "remove_entry on cell while async flush is pending"
                    );
                    entry.clear();
                }
            }
            Entries::Tree(tree) => {
                let CellId::Bytes(cell) = cell_id else {
                    panic!("Invalid cell id for tree entry list: {cell_id:?}");
                };
                if let Some(mut entry) = tree.remove(cell) {
                    assert!(
                        entry.pending_last_processed.is_none(),
                        "remove_entry on cell while async flush is pending"
                    );
                    entry.clear();
                }
            }
        }
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
    pub fp_lookup_after_lock_drop: crate::failpoints::FailPoint,
    /// Hit by the flusher thread between the flush work and the visible
    /// completion (`update_flushed_index` / `update_relocated_index`).
    pub fp_flush_before_completion: crate::failpoints::FailPoint,
}

#[cfg(not(test))]
impl LargeTableFailPoints {
    pub fn fp_insert_before_lock(&self) {}
    pub fn fp_remove_before_lock(&self) {}

    pub fn fp_lookup_after_lock_drop(&self) {}

    pub fn fp_flush_before_completion(&self) {}
}

#[cfg(test)]
impl LargeTableFailPoints {
    pub fn fp_insert_before_lock(&self) {
        self.0.read().fp_insert_before_lock.fp();
    }

    pub fn fp_remove_before_lock(&self) {
        self.0.read().fp_remove_before_lock.fp();
    }
    pub fn fp_lookup_after_lock_drop(&self) {
        self.0.read().fp_lookup_after_lock_drop.fp();
    }

    pub fn fp_flush_before_completion(&self) {
        self.0.read().fp_flush_before_completion.fp();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::levels::IndexShard;
    use crate::key_shape::{KeyShapeBuilder, KeySpaceConfig};
    use crate::wal::Wal;
    use crate::wal::layout::WalKind;
    use smallvec::SmallVec;
    use std::io;

    #[test]
    fn test_ks_allocation() {
        let config = Config::small();
        let mut ks = KeyShapeBuilder::new();
        ks.add_key_space("a", 0, 1, KeyType::uniform(1));
        ks.add_key_space("b", 0, 1, KeyType::uniform(1));
        ks.add_key_space("c", 0, 1, KeyType::uniform(1));
        let ks = ks.build();
        let kss = crate::key_shape::KeySpaces::from_key_shape(&ks);
        let a = kss.ks("a");
        let b = kss.ks("b");
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
        let (mut row, cell) = l.row(l.ks_context(a), &[]);
        assert_eq!(row.entry_mut(&cell).0.context.name(), "a");
        let (mut row, cell) = l.row(l.ks_context(b), &[5, 2, 3, 4]);
        assert_eq!(row.entry_mut(&cell).0.context.name(), "b");
    }

    #[test]
    fn test_bloom_size() {
        let f = BloomFilter::with_false_pos(0.01).expected_items(8000);
        println!("hashes: {}, bytes: {}", f.num_hashes(), f.num_bits() / 8);
    }

    #[test]
    fn test_next_in_cell() {
        // Create a test setup with different entry states
        let metrics = Metrics::new();

        // Create key space with unloaded_iterator enabled
        let config = KeySpaceConfig::default().with_unloaded_iterator(true);
        let shape = KeyShape::new_single_config(8, 1, KeyType::uniform(1), config);
        let ks_id = KeySpace::first();

        let ks = shape.ks(ks_id);
        let context = KsContext::new(Arc::new(Config::small()), ks.clone(), metrics.clone());

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

            fn last_processed_wal_position(&self) -> LastProcessed {
                // Return a test value for mock
                LastProcessed::none()
            }

            fn current_wal_position(&self) -> u64 {
                // Return a test value for mock
                1000
            }

            fn min_wal_position(&self) -> u64 {
                0
            }
        }

        // Create test data for our disk index
        let mut disk_index = IndexTable::default();
        for i in 1..6u64 {
            let key = (i * 10).to_be_bytes().to_vec();
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
            let result = LargeTable::next_in_cell(
                &loader,
                &mut row,
                &cell_id,
                None,
                false,
                &mut IndexIterCaches::new(),
            )
            .unwrap();
            assert!(result.is_none(), "Empty state should return None");
        }

        // TEST CASE 2: Loaded state
        {
            // Create a row with a Loaded entry
            let mut entry = LargeTableEntry::new_empty(context.clone(), cell_id.clone(), 0);
            entry.data = ArcCow::new_owned(disk_index.clone());
            entry.levels = IndexLevels::single(WalPosition::test_value(42));
            entry.state = LargeTableEntryState::Loaded;

            let mut row = Row {
                context: context.clone(),
                entries: Entries::Array(1, Box::new([entry])),
            };

            // Test forward iteration with loaded state
            let result = LargeTable::next_in_cell(
                &loader,
                &mut row,
                &cell_id,
                None,
                false,
                &mut IndexIterCaches::new(),
            )
            .unwrap();
            assert!(result.is_some(), "Loaded state should return first entry");
            let (key, get_result) = result.unwrap();
            let pos = get_result.unwrap_wal_position();
            assert_eq!(key.as_ref(), 10_u64.to_be_bytes());
            assert_eq!(pos, WalPosition::test_value(1));

            // Test forward iteration from a key
            let result = LargeTable::next_in_cell(
                &loader,
                &mut row,
                &cell_id,
                Some(Bytes::from(20_u64.to_be_bytes().to_vec())),
                false,
                &mut IndexIterCaches::new(),
            )
            .unwrap();
            assert!(result.is_some());
            let (key, get_result) = result.unwrap();
            let pos = get_result.unwrap_wal_position();
            assert_eq!(key.as_ref(), 30_u64.to_be_bytes());
            assert_eq!(pos, WalPosition::test_value(3));

            // Test backward iteration
            let result = LargeTable::next_in_cell(
                &loader,
                &mut row,
                &cell_id,
                None,
                true,
                &mut IndexIterCaches::new(),
            )
            .unwrap();
            assert!(result.is_some());
            let (key, get_result) = result.unwrap();
            let pos = get_result.unwrap_wal_position();
            assert_eq!(key.as_ref(), 50_u64.to_be_bytes());
            assert_eq!(pos, WalPosition::test_value(5));
        }

        // TEST CASE 3: Unloaded state
        {
            // Create a row with an Unloaded entry
            let entry = LargeTableEntry::new_unloaded(
                context.clone(),
                cell_id.clone(),
                IndexLevels::single(WalPosition::test_value(42)),
                0,
                None,
            );

            let mut row = Row {
                context: context.clone(),
                entries: Entries::Array(1, Box::new([entry])),
            };

            // Test forward iteration with unloaded state
            let result = LargeTable::next_in_cell(
                &loader,
                &mut row,
                &cell_id,
                None,
                false,
                &mut IndexIterCaches::new(),
            )
            .unwrap();
            assert!(
                result.is_some(),
                "Unloaded state should return first entry from disk"
            );
            let (key, get_result) = result.unwrap();
            let pos = get_result.unwrap_wal_position();
            assert_eq!(key.as_ref(), 10_u64.to_be_bytes());
            assert_eq!(pos, WalPosition::test_value(1));

            // Test forward iteration from a key
            let result = LargeTable::next_in_cell(
                &loader,
                &mut row,
                &cell_id,
                Some(Bytes::from(20_u64.to_be_bytes().to_vec())),
                false,
                &mut IndexIterCaches::new(),
            )
            .unwrap();
            assert!(result.is_some());
            let (key, get_result) = result.unwrap();
            let pos = get_result.unwrap_wal_position();
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

            entry.levels = IndexLevels::single(WalPosition::test_value(42));
            entry.state = LargeTableEntryState::DirtyLoaded;

            let mut row = Row {
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
                &mut IndexIterCaches::new(),
            )
            .unwrap();
            assert!(result.is_some());
            let (key, get_result) = result.unwrap();
            let pos = get_result.unwrap_wal_position();
            assert_eq!(key.as_ref(), 50_u64.to_be_bytes()); // Should skip over the deleted 40
            assert_eq!(pos, WalPosition::test_value(5));

            // Test finding the new key
            let result = LargeTable::next_in_cell(
                &loader,
                &mut row,
                &cell_id,
                Some(Bytes::from(10_u64.to_be_bytes().to_vec())),
                false,
                &mut IndexIterCaches::new(),
            )
            .unwrap();
            assert!(result.is_some());
            let (key, get_result) = result.unwrap();
            let pos = get_result.unwrap_wal_position();
            assert_eq!(key.as_ref(), 15_u64.to_be_bytes()); // Should find our new key
            assert_eq!(pos, WalPosition::test_value(15));
        }

        // TEST CASE 5: DirtyUnloaded state
        {
            // Create a row with a DirtyUnloaded entry
            let mut entry = LargeTableEntry::new_unloaded(
                context.clone(),
                cell_id.clone(),
                IndexLevels::single(WalPosition::test_value(42)),
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

            entry.state = LargeTableEntryState::DirtyUnloaded;

            let mut row = Row {
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
                &mut IndexIterCaches::new(),
            )
            .unwrap();
            assert!(result.is_some());
            let (key, get_result) = result.unwrap();
            let pos = get_result.unwrap_wal_position();
            assert_eq!(key.as_ref(), 15_u64.to_be_bytes());
            assert_eq!(pos, WalPosition::test_value(15));

            // Test skipping the in-memory deleted key and finding the next on-disk key
            let result = LargeTable::next_in_cell(
                &loader,
                &mut row,
                &cell_id,
                Some(Bytes::from(30_u64.to_be_bytes().to_vec())),
                false,
                &mut IndexIterCaches::new(),
            )
            .unwrap();
            assert!(result.is_some());
            let (key, get_result) = result.unwrap();
            let pos = get_result.unwrap_wal_position();
            assert_eq!(key.as_ref(), 50_u64.to_be_bytes()); // Should skip over the deleted 40
            assert_eq!(pos, WalPosition::test_value(5));
        }
    }

    #[test]
    fn test_disk_levels_to_walk_for_key_sharded() {
        let metrics = Metrics::new();
        let config = KeySpaceConfig::default().with_unloaded_iterator(true);
        let shape = KeyShape::new_single_config(8, 1, KeyType::uniform(1), config);
        let ks_id = KeySpace::first();
        let ks = shape.ks(ks_id);
        let context = KsContext::new(Arc::new(Config::small()), ks.clone(), metrics.clone());
        let cell_id = CellId::Integer(0);

        // Three-shard btree. Ranges are tight `[min, max]` content extents:
        // A: [b"a", b"l"], B: [b"m", b"t"], C: [b"u", b"z"].
        let mut shards = BTreeMap::new();
        shards.insert(
            b"a".to_vec(),
            IndexShard::new(WalPosition::test_value(11), b"l".to_vec()),
        );
        shards.insert(
            b"m".to_vec(),
            IndexShard::new(WalPosition::test_value(22), b"t".to_vec()),
        );
        shards.insert(
            b"u".to_vec(),
            IndexShard::new(WalPosition::test_value(33), b"z".to_vec()),
        );

        // Unloaded + sharded (no L0): just `shard_for(key).position`.
        let entry = LargeTableEntry::new_unloaded(
            context.clone(),
            cell_id.clone(),
            IndexLevels::sharded(shards.clone()),
            0,
            None,
        );
        // Keys inside each shard's content range:
        assert_eq!(
            entry.disk_levels_to_walk_for_key(b"a").as_slice(),
            &[WalPosition::test_value(11)],
        );
        assert_eq!(
            entry.disk_levels_to_walk_for_key(b"p").as_slice(),
            &[WalPosition::test_value(22)],
        );
        assert_eq!(
            entry.disk_levels_to_walk_for_key(b"u").as_slice(),
            &[WalPosition::test_value(33)],
        );
        // Keys outside every shard's content range: no on-disk read needed.
        assert!(entry.disk_levels_to_walk_for_key(b"").is_empty()); // below A.min
        assert!(entry.disk_levels_to_walk_for_key(b"~").is_empty()); // above C.max

        // Unloaded + sharded with L0: L0 first, then the covering shard.
        let entry = LargeTableEntry::new_unloaded(
            context.clone(),
            cell_id.clone(),
            IndexLevels::sharded_with_l0(WalPosition::test_value(7), shards.clone()),
            0,
            None,
        );
        assert_eq!(
            entry.disk_levels_to_walk_for_key(b"a").as_slice(),
            &[WalPosition::test_value(7), WalPosition::test_value(11)],
        );
        assert_eq!(
            entry.disk_levels_to_walk_for_key(b"z").as_slice(),
            &[WalPosition::test_value(7), WalPosition::test_value(33)],
        );
        // Out-of-range key still consults L0 (the dirty overlay might hold it).
        assert_eq!(
            entry.disk_levels_to_walk_for_key(b"~").as_slice(),
            &[WalPosition::test_value(7)],
        );

        // Loaded + sharded with L0: L0 is already folded into `self.data`, so
        // the disk walk drops it and only the covering shard remains.
        let mut entry = LargeTableEntry::new_empty(context.clone(), cell_id.clone(), 0);
        entry.levels = IndexLevels::sharded_with_l0(WalPosition::test_value(7), shards.clone());
        entry.state = LargeTableEntryState::Loaded;
        assert_eq!(
            entry.disk_levels_to_walk_for_key(b"a").as_slice(),
            &[WalPosition::test_value(11)],
        );
        assert_eq!(
            entry.disk_levels_to_walk_for_key(b"q").as_slice(),
            &[WalPosition::test_value(22)],
        );

        // Unsharded: identical to `disk_levels_to_walk` regardless of `key`.
        let mut entry = LargeTableEntry::new_empty(context.clone(), cell_id.clone(), 0);
        entry.levels = IndexLevels::promoted(WalPosition::test_value(99));
        entry.state = LargeTableEntryState::Loaded;
        assert_eq!(
            entry.disk_levels_to_walk_for_key(b"anything"),
            entry.disk_levels_to_walk(),
        );
    }

    /// Counts `load` and `index_reader` calls per position so tests can
    /// assert the iterator opens shard readers on demand instead of all
    /// at once.
    struct CountingByPosLoader {
        blobs: HashMap<u64, (IndexTable, Bytes)>,
        opens_by_pos: parking_lot::Mutex<HashMap<u64, usize>>,
    }

    impl CountingByPosLoader {
        fn new(blobs: HashMap<u64, (IndexTable, Bytes)>) -> Self {
            Self {
                blobs,
                opens_by_pos: parking_lot::Mutex::new(HashMap::new()),
            }
        }

        fn opens_for(&self, pos: WalPosition) -> usize {
            *self.opens_by_pos.lock().get(&pos.offset()).unwrap_or(&0)
        }

        fn total_opens(&self) -> usize {
            self.opens_by_pos.lock().values().sum()
        }
    }

    impl Loader for CountingByPosLoader {
        type Error = io::Error;
        fn load(&self, _ks: &KeySpaceDesc, position: WalPosition) -> Result<IndexTable, io::Error> {
            Ok(self.blobs.get(&position.offset()).unwrap().0.clone())
        }
        fn index_reader(&self, position: WalPosition) -> Result<WalRandomRead, io::Error> {
            *self
                .opens_by_pos
                .lock()
                .entry(position.offset())
                .or_insert(0) += 1;
            Ok(WalRandomRead::Mapped(
                self.blobs.get(&position.offset()).unwrap().1.clone(),
            ))
        }
        fn flush_supported(&self) -> bool {
            true
        }
        fn flush(&self, _ks: KeySpace, _data: &IndexTable) -> Result<WalPosition, io::Error> {
            unreachable!("sharded iteration test never flushes")
        }
        fn last_processed_wal_position(&self) -> LastProcessed {
            LastProcessed::none()
        }
        fn current_wal_position(&self) -> u64 {
            1000
        }
        fn min_wal_position(&self) -> u64 {
            0
        }
    }

    /// Builds a two-shard cell. Shard A: keys 10, 20. Shard B: keys 30, 40, 50.
    /// Returns the loader, the row holding the entry, and the WAL positions of
    /// each shard so callers can assert per-shard open counts.
    fn make_two_shard_cell(
        context: KsContext,
        ks: &KeySpaceDesc,
        cell_id: CellId,
    ) -> (CountingByPosLoader, Row, WalPosition, WalPosition) {
        let mut shard_a_table = IndexTable::default();
        shard_a_table.insert(
            Bytes::from(10_u64.to_be_bytes().to_vec()),
            WalPosition::test_value(101),
        );
        shard_a_table.insert(
            Bytes::from(20_u64.to_be_bytes().to_vec()),
            WalPosition::test_value(102),
        );
        let mut shard_b_table = IndexTable::default();
        shard_b_table.insert(
            Bytes::from(30_u64.to_be_bytes().to_vec()),
            WalPosition::test_value(103),
        );
        shard_b_table.insert(
            Bytes::from(40_u64.to_be_bytes().to_vec()),
            WalPosition::test_value(104),
        );
        shard_b_table.insert(
            Bytes::from(50_u64.to_be_bytes().to_vec()),
            WalPosition::test_value(105),
        );

        let format = ks.index_format();
        let serialized_a = format.clean_serialize_index(&mut shard_a_table.clone(), ks);
        let serialized_b = format.clean_serialize_index(&mut shard_b_table.clone(), ks);

        let pos_a = WalPosition::test_value(201);
        let pos_b = WalPosition::test_value(202);

        let mut blobs = HashMap::new();
        blobs.insert(pos_a.offset(), (shard_a_table, serialized_a));
        blobs.insert(pos_b.offset(), (shard_b_table, serialized_b));
        let loader = CountingByPosLoader::new(blobs);

        // Shard A: [10, 20] at pos_a. Shard B: [30, 50] at pos_b.
        // Btree keys are real content mins (8-byte BE), values carry max_key.
        let mut shards = BTreeMap::new();
        shards.insert(
            10_u64.to_be_bytes().to_vec(),
            IndexShard::new(pos_a, 20_u64.to_be_bytes().to_vec()),
        );
        shards.insert(
            30_u64.to_be_bytes().to_vec(),
            IndexShard::new(pos_b, 50_u64.to_be_bytes().to_vec()),
        );

        let entry = LargeTableEntry::new_unloaded(
            context.clone(),
            cell_id.clone(),
            IndexLevels::sharded(shards),
            0,
            None,
        );
        let row = Row {
            context,
            entries: Entries::Array(1, Box::new([entry])),
        };
        (loader, row, pos_a, pos_b)
    }

    #[test]
    fn test_next_in_cell_sharded() {
        // Two-shard cell, forward iteration. The min/max metadata on each
        // shard lets `shard_for_iter_forward` pick the right shard in one
        // btree probe per step — no boundary retries.
        let metrics = Metrics::new();
        let config = KeySpaceConfig::default().with_unloaded_iterator(true);
        let shape = KeyShape::new_single_config(8, 1, KeyType::uniform(1), config);
        let ks_id = KeySpace::first();
        let ks = shape.ks(ks_id);
        let context = KsContext::new(Arc::new(Config::small()), ks.clone(), metrics.clone());
        let cell_id = CellId::Integer(0);

        let (loader, mut row, pos_a, pos_b) = make_two_shard_cell(context, ks, cell_id.clone());

        let expected: Vec<(u64, WalPosition)> = vec![
            (10, WalPosition::test_value(101)),
            (20, WalPosition::test_value(102)),
            (30, WalPosition::test_value(103)),
            (40, WalPosition::test_value(104)),
            (50, WalPosition::test_value(105)),
        ];
        let mut yielded: Vec<(u64, WalPosition)> = Vec::new();
        let mut prev: Option<Bytes> = None;
        let mut caches = IndexIterCaches::new();
        loop {
            let next = LargeTable::next_in_cell(
                &loader,
                &mut row,
                &cell_id,
                prev.clone(),
                false,
                &mut caches,
            )
            .unwrap();
            let Some((key, get_result)) = next else {
                break;
            };
            let mut buf = [0u8; 8];
            buf.copy_from_slice(key.as_ref());
            yielded.push((u64::from_be_bytes(buf), get_result.unwrap_wal_position()));
            prev = Some(key);
        }
        assert_eq!(
            yielded, expected,
            "sharded iteration must merge in key order"
        );

        // Open accounting: one shard open per emitted key. The terminating
        // call (prev = 50 = max of B) returns no plan at all because
        // `shard_for_iter_forward` reports no shard past prev_key, so
        // prepare_walk short-circuits before opening any reader.
        // A handles emits 10, 20 → 2 opens. B handles 30, 40, 50 → 3 opens.
        assert_eq!(loader.opens_for(pos_a), 2);
        assert_eq!(loader.opens_for(pos_b), 3);
        assert_eq!(loader.total_opens(), 5);
    }

    #[test]
    fn test_next_in_cell_sharded_reverse() {
        // Same cell, reverse iteration. `shard_for_iter_reverse` picks B
        // first and switches to A exactly when prev_key drops to B.min.
        let metrics = Metrics::new();
        let config = KeySpaceConfig::default().with_unloaded_iterator(true);
        let shape = KeyShape::new_single_config(8, 1, KeyType::uniform(1), config);
        let ks_id = KeySpace::first();
        let ks = shape.ks(ks_id);
        let context = KsContext::new(Arc::new(Config::small()), ks.clone(), metrics.clone());
        let cell_id = CellId::Integer(0);

        let (loader, mut row, pos_a, pos_b) = make_two_shard_cell(context, ks, cell_id.clone());

        let expected_keys: Vec<u64> = vec![50, 40, 30, 20, 10];
        let mut yielded: Vec<u64> = Vec::new();
        let mut prev: Option<Bytes> = None;
        let mut caches = IndexIterCaches::new();
        loop {
            let next = LargeTable::next_in_cell(
                &loader,
                &mut row,
                &cell_id,
                prev.clone(),
                true,
                &mut caches,
            )
            .unwrap();
            let Some((key, _)) = next else {
                break;
            };
            let mut buf = [0u8; 8];
            buf.copy_from_slice(key.as_ref());
            yielded.push(u64::from_be_bytes(buf));
            prev = Some(key);
        }
        assert_eq!(yielded, expected_keys);
        // Mirror of the forward case: B handles 50, 40, 30 → 3 opens, A
        // handles 20, 10 → 2 opens. The final call (prev = 10 = A.min)
        // short-circuits before any open.
        assert_eq!(loader.opens_for(pos_b), 3);
        assert_eq!(loader.opens_for(pos_a), 2);
        assert_eq!(loader.total_opens(), 5);
    }

    #[test]
    fn test_next_cell_prefixed_uniform() {
        // This test verifies that next_cell correctly handles PrefixedUniform key types
        // where cells are distributed across mutexes by hashing.
        // The fix ensures cells are returned in sorted order regardless of mutex distribution.

        let config = Arc::new(Config::small());
        let mut ks_builder = KeyShapeBuilder::new();

        // Create a PrefixedUniform keyspace with 3-byte prefix and 1024 mutexes
        ks_builder.add_key_space(
            "test",
            36,   // key_size in bytes
            1024, // mutexes (must be power of 2)
            KeyType::prefix_uniform(3, 0),
        );
        let shape = ks_builder.build();
        let ks_id = crate::key_shape::KeySpaces::from_key_shape(&shape).ks("test");
        let tmp_dir = tempdir::TempDir::new("test_next_cell_prefixed").unwrap();
        let wal = Wal::open(
            tmp_dir.path(),
            config.wal_layout(WalKind::Replay),
            Metrics::new(),
        )
        .unwrap();

        let table = LargeTable::from_unloaded(
            &shape,
            &LargeTableContainer::new_from_key_shape(&shape, SnapshotEntryData::empty()),
            config.clone(),
            IndexFlusher::new_unstarted_for_test(),
            Metrics::new(),
            wal.as_ref(),
        );

        let context = table.ks_context(ks_id);

        // Insert test cells with prefixes that will hash to different mutexes
        // Using prefixes: [0,0,1], [0,0,2], [0,0,5], [0,0,10], [0,0,255]
        let test_cells = vec![
            vec![0u8, 0, 1],
            vec![0, 0, 2],
            vec![0, 0, 5],
            vec![0, 0, 10],
            vec![0, 0, 255],
        ];

        // Insert a key into each cell to make them non-empty
        for cell_bytes in &test_cells {
            let (mut row, cell) = table.row(context, cell_bytes);
            let (entry, _) = row.entry_mut(&cell);

            // Create a key for this cell (prefix + random bytes)
            let mut key = cell_bytes.clone();
            key.extend_from_slice(&[0u8; 33]); // Pad to 36 bytes total

            entry
                .data
                .make_mut()
                .insert(Bytes::from(key), WalPosition::test_value(1));

            // Manually update cell_index since we're bypassing LargeTable::insert
            if let CellId::Bytes(bytes) = &cell {
                let ks_table = table.ks_table(&context.ks_config);
                ks_table.cell_index.write().insert(bytes.clone());
            }
        }

        // Test forward iteration: should visit cells in sorted order
        let mut current_cell = CellId::Bytes(SmallVec::from_slice(&[0u8, 0, 0]));
        let mut visited_cells = Vec::new();

        for _ in 0..10 {
            // Safety limit
            match table.next_cell(context, &current_cell, false) {
                Some(next_cell) => {
                    let bytes = next_cell.assume_bytes_id();
                    visited_cells.push(bytes.to_vec());
                    current_cell = next_cell;
                }
                None => break,
            }
        }

        // Verify forward iteration found all cells in sorted order
        assert_eq!(visited_cells.len(), 5, "Should find all 5 test cells");
        assert_eq!(visited_cells[0], vec![0u8, 0, 1]);
        assert_eq!(visited_cells[1], vec![0, 0, 2]);
        assert_eq!(visited_cells[2], vec![0, 0, 5]);
        assert_eq!(visited_cells[3], vec![0, 0, 10]);
        assert_eq!(visited_cells[4], vec![0, 0, 255]);

        // Test reverse iteration: should visit cells in reverse sorted order
        let mut current_cell = CellId::Bytes(SmallVec::from_slice(&[255u8, 255, 255]));
        let mut visited_cells_reverse = Vec::new();

        for _ in 0..10 {
            // Safety limit
            match table.next_cell(context, &current_cell, true) {
                Some(next_cell) => {
                    let bytes = next_cell.assume_bytes_id();
                    visited_cells_reverse.push(bytes.to_vec());
                    current_cell = next_cell;
                }
                None => break,
            }
        }

        // Verify reverse iteration found all cells in reverse sorted order
        assert_eq!(
            visited_cells_reverse.len(),
            5,
            "Should find all 5 test cells in reverse"
        );
        assert_eq!(visited_cells_reverse[0], vec![0u8, 0, 255]);
        assert_eq!(visited_cells_reverse[1], vec![0, 0, 10]);
        assert_eq!(visited_cells_reverse[2], vec![0, 0, 5]);
        assert_eq!(visited_cells_reverse[3], vec![0, 0, 2]);
        assert_eq!(visited_cells_reverse[4], vec![0, 0, 1]);

        // Verify reverse is the exact reverse of forward
        let forward_reversed: Vec<_> = visited_cells.into_iter().rev().collect();
        assert_eq!(
            forward_reversed, visited_cells_reverse,
            "Reverse iteration should be exact reverse of forward"
        );

        // Test iteration from a specific cell (not the start/end)
        let start_cell = CellId::Bytes(SmallVec::from_slice(&[0u8, 0, 2]));
        let next = table
            .next_cell(context, &start_cell, false)
            .expect("Should find next cell after [0,0,2]");
        assert_eq!(
            next.assume_bytes_id().as_slice(),
            &[0u8, 0, 5],
            "Next cell after [0,0,2] should be [0,0,5]"
        );

        let prev = table
            .next_cell(context, &start_cell, true)
            .expect("Should find previous cell before [0,0,2]");
        assert_eq!(
            prev.assume_bytes_id().as_slice(),
            &[0u8, 0, 1],
            "Previous cell before [0,0,2] should be [0,0,1]"
        );
    }

    #[test]
    fn test_report_entries_state() {
        let config = Arc::new(Config::small());
        let metrics = Arc::new(Metrics::new());
        let mut ks_builder = KeyShapeBuilder::new();
        // Single-cell KS: 1-byte key, 1 mutex
        ks_builder.add_key_space("test", 1, 1, KeyType::uniform(1));
        let shape = ks_builder.build();
        let ks_id = crate::key_shape::KeySpaces::from_key_shape(&shape).ks("test");
        let tmp_dir = tempdir::TempDir::new("test_report_entries_state").unwrap();
        let wal = Wal::open(
            tmp_dir.path(),
            config.wal_layout(WalKind::Replay),
            Metrics::new(),
        )
        .unwrap();
        let table = LargeTable::from_unloaded(
            &shape,
            &LargeTableContainer::new_from_key_shape(&shape, SnapshotEntryData::empty()),
            config.clone(),
            IndexFlusher::new_unstarted_for_test(),
            Arc::clone(&metrics),
            wal.as_ref(),
        );

        let entry_state = |state: &str| {
            metrics
                .entry_state
                .with_label_values(&["test", state])
                .get()
        };

        // Before any report: all label combinations are absent (metrics return 0 by default)
        table.report_entries_state();
        assert_eq!(entry_state("empty"), 1, "one cell should be in empty state");
        assert_eq!(
            metrics.entry_state.label_count(),
            1,
            "only one label after first report"
        );

        // Manually set the single entry to DirtyLoaded to simulate a write
        let context = table.ks_context(ks_id);
        let (mut row, cell) = table.row(context, &[0u8]);
        let (entry, _) = row.entry_mut(&cell);
        entry
            .data
            .make_mut()
            .insert(Bytes::from(vec![0u8]), WalPosition::test_value(1));
        entry.levels = IndexLevels::single(WalPosition::test_value(1));
        entry.state = LargeTableEntryState::DirtyLoaded;
        drop(row);

        table.report_entries_state();
        assert_eq!(entry_state("empty"), 0, "empty should be 0 after insert");
        assert_eq!(
            entry_state("dirty_loaded"),
            1,
            "one cell should be dirty_loaded"
        );
        assert_eq!(
            metrics.entry_state.label_count(),
            2,
            "empty + dirty_loaded after second report"
        );
    }
}
