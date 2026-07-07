// See docs/snapshot_mechanism.md for an overview of how the flusher integrates
// with the two-pass snapshot mechanism.
use crate::cell::CellId;
use crate::context::KsContext;
use crate::db::Db;
use crate::index::index_format::IndexFormat;
use crate::index::index_table::IndexTable;
use crate::index::levels::{IndexLevels, IndexShard};
use crate::key_shape::{KeySpace, KeySpaceDesc};
use crate::large_table::Loader;
use crate::metrics::Metrics;
use crate::relocation::updates::RelocationUpdates;
use crate::wal::position::WalPosition;
use minibytes::Bytes;
use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::mpsc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Instant;

pub struct IndexFlusher {
    senders: Vec<mpsc::Sender<FlusherCommand>>,
    metrics: Arc<Metrics>,
}

pub(crate) struct IndexFlusherThread {
    db: Weak<Db>,
    receiver: mpsc::Receiver<FlusherCommand>,
    metrics: Arc<Metrics>,
    thread_id: usize,
}

pub enum FlusherCommand {
    Command(IndexFlushCommand),
    Barrier(Arc<SendGuard>),
}

pub struct IndexFlushCommand {
    pub(crate) ks: KeySpace,
    pub(crate) cell: CellId,
    pub(crate) flush_kind: FlushKind,
}

impl IndexFlushCommand {
    pub fn new(ks: KeySpace, cell: CellId, flush_kind: FlushKind) -> Self {
        Self {
            ks,
            cell,
            flush_kind,
        }
    }
}

pub enum FlushKind {
    /// Flush dirty contents, producing a new on-disk level state.
    ///
    /// - `dirty` is the dirty/loaded in-memory index provided by the caller.
    /// - `current_levels` is the entry's on-disk level state at the time the
    ///   flush was scheduled. It tells the flusher which L0 (if any) to
    ///   load-and-merge and which L1 (if any) to consult on promote.
    /// - `loaded` distinguishes the two caller shapes:
    ///   - `loaded = true` (was `FlushLoaded`): `dirty` already reflects
    ///     L0 + overlay — no separate L0 load is needed.
    ///   - `loaded = false` (was `MergeUnloaded`): `dirty` is only the
    ///     overlay; the flusher must load L0 from `current_levels.l0()`
    ///     (if any) and merge.
    /// - `relocate_positions` is the subset of pre-flush positions flagged
    ///   by the snapshot pass for low-occupancy-file evacuation. Empty for
    ///   ordinary unload-driven flushes. When non-empty, the flusher runs
    ///   a post-pass after the normal merge/promote: any blob in the
    ///   resulting levels whose position is still in `relocate_positions`
    ///   (i.e., wasn't naturally rewritten by the merge/promote) is
    ///   reflushed at a fresh position. This is how dirty sharded cells
    ///   evacuate stale shards that re-sharding leaves untouched.
    Flush {
        dirty: Arc<IndexTable>,
        current_levels: IndexLevels,
        loaded: bool,
        relocate_positions: HashSet<WalPosition>,
    },
    /// Re-write a clean entry's on-disk blobs that live in low-occupancy
    /// WAL files so those files become GC-eligible.
    ///
    /// `relocate_positions` is the subset of positions in `levels` flagged
    /// by the snapshot pass.
    ///
    /// Unsharded `levels`: collapse all populated blobs into a single new
    /// L0 (L0 wins on overlap) regardless of `relocate_positions`. Trades
    /// a one-time WAF penalty for a reduced level count.
    ///
    /// Sharded `levels`: rewrite only the shards (and the L0 blob, if
    /// present) whose position is in `relocate_positions`. The shard btree
    /// shape is preserved — only touched positions change.
    ForceRelocate {
        levels: IndexLevels,
        relocate_positions: HashSet<WalPosition>,
    },
}

impl IndexFlusher {
    pub fn new(senders: Vec<mpsc::Sender<FlusherCommand>>, metrics: Arc<Metrics>) -> Self {
        assert!(!senders.is_empty(), "Must have at least one flusher thread");
        Self { senders, metrics }
    }

    /// Start flusher threads with the given receivers and database reference
    pub fn start_threads(
        receivers: Vec<mpsc::Receiver<FlusherCommand>>,
        db: Weak<Db>,
        metrics: Arc<Metrics>,
    ) -> Vec<JoinHandle<()>> {
        receivers
            .into_iter()
            .enumerate()
            .map(|(thread_id, receiver)| {
                let flusher_thread =
                    IndexFlusherThread::new(db.clone(), receiver, metrics.clone(), thread_id);

                thread::Builder::new()
                    .name(format!("flusher-{thread_id}"))
                    .spawn(move || flusher_thread.run())
                    .unwrap()
            })
            .collect()
    }

    pub fn request_flush(&self, ks: KeySpace, cell: CellId, flush_kind: FlushKind) {
        let thread_index = self.get_thread_for_cell(&cell);
        let command = IndexFlushCommand::new(ks, cell, flush_kind);
        self.metrics.flush_pending.add(1);
        self.senders[thread_index]
            .send(FlusherCommand::Command(command))
            .expect("Flusher has stopped unexpectedly")
    }

    fn get_thread_for_cell(&self, cell: &CellId) -> usize {
        cell.mutex_seed() % self.senders.len()
    }

    #[cfg(test)]
    pub fn new_unstarted_for_test() -> Self {
        let (sender, _receiver) = mpsc::channel();
        Self::new(vec![sender], Metrics::new())
    }

    /// Wait until all messages that are currently queued for all flusher threads are processed.
    pub fn barrier(&self) {
        let mutex = Arc::new(parking_lot::Mutex::new(()));
        let guard = Arc::new(SendGuard(mutex.lock_arc()));
        for sender in &self.senders {
            self.metrics.flush_pending.add(1);
            sender
                .send(FlusherCommand::Barrier(Arc::clone(&guard)))
                .expect("Flusher has stopped unexpectedly");
        }
        drop(guard);
        drop(mutex.lock());
    }
}

/// Returns `(min_key, max_key)` from a non-empty sorted slice of entries.
fn shard_bounds(entries: &[(Bytes, WalPosition)]) -> (Vec<u8>, Vec<u8>) {
    (
        entries
            .first()
            .expect("entries must be non-empty")
            .0
            .to_vec(),
        entries
            .last()
            .expect("entries must be non-empty")
            .0
            .to_vec(),
    )
}

impl IndexFlusherThread {
    pub fn new(
        db: Weak<Db>,
        receiver: mpsc::Receiver<FlusherCommand>,
        metrics: Arc<Metrics>,
        thread_id: usize,
    ) -> Self {
        Self {
            db,
            receiver,
            metrics,
            thread_id,
        }
    }

    pub fn run(self) {
        while let Ok(command) = self.receiver.recv() {
            match command {
                FlusherCommand::Barrier(_guard) => {
                    self.metrics.flush_pending.add(-1);
                    // Dropping _guard releases the mutex, allowing the barrier() caller to proceed
                }
                FlusherCommand::Command(command) => {
                    self.metrics.flush_pending.add(-1);
                    let now = Instant::now();
                    let Some(db) = self.db.upgrade() else {
                        return;
                    };

                    let ks_context = db.ks_context(command.ks);
                    let is_relocation =
                        matches!(command.flush_kind, FlushKind::ForceRelocate { .. });
                    if let Some((original_index, new_levels)) =
                        Self::handle_command(&*db, &command, ks_context, None, None)
                    {
                        // Failpoint between the flush work and its visible
                        // completion: lets tests pause here to interleave
                        // writes with an in-flight flush deterministically.
                        db.large_table.fp.fp_flush_before_completion();
                        if is_relocation {
                            db.update_relocated_index(command.ks, command.cell, new_levels);
                        } else {
                            db.update_flushed_index(
                                command.ks,
                                command.cell,
                                original_index,
                                new_levels,
                            );
                        }
                    }

                    self.metrics
                        .flush_time_mcs
                        .with_label_values(&[&self.thread_id.to_string()])
                        .inc_by(now.elapsed().as_micros() as u64);
                }
            }
        }
    }

    pub(crate) fn handle_command<L: Loader>(
        loader: &L,
        command: &IndexFlushCommand,
        ctx: &KsContext,
        relocation_updates: Option<RelocationUpdates>,
        relocation_cutoff: Option<u64>,
    ) -> Option<(Arc<IndexTable>, IndexLevels)> {
        // Sharded ForceRelocate short-circuits before the merge-build
        // path below, which would collapse the shard btree into one blob.
        if let FlushKind::ForceRelocate {
            levels,
            relocate_positions,
        } = &command.flush_kind
            && levels.is_sharded()
        {
            ctx.metrics
                .unload
                .with_label_values(&["force_relocate"])
                .inc();
            let new_levels = Self::relocate_in_levels(
                loader,
                ctx,
                command.ks,
                levels.clone(),
                relocate_positions,
            );
            return Some((Arc::new(IndexTable::default()), new_levels));
        }

        // Build the merged L0 candidate plus the cell's pre-flush levels.
        //
        // For `Flush`, `current_levels` describes the cell's pre-flush on-disk
        // state; we honour the sentinel (empty L0 slot after a prior promote)
        // so post-promote cells start a fresh L0 with just the overlay.
        //
        // Unsharded `ForceRelocate` collapses L0+L1 into one blob handed
        // downstream as `merged_l0` with empty `current_levels`, so the
        // non-promote branch writes it as a single level and strips
        // tombstones. Sharded ForceRelocate never reaches this match.
        let (original_index, mut merged_l0, current_levels) = match &command.flush_kind {
            FlushKind::Flush {
                dirty,
                current_levels,
                loaded,
                ..
            } => {
                let label = if *loaded { "flush" } else { "merge_flush" };
                ctx.metrics.unload.with_label_values(&[label]).inc();
                let merged = if *loaded {
                    // `dirty` already reflects L0+overlay. Clone so we can
                    // mutate (compact, strip tombstones, etc.) without
                    // touching the shared in-memory table. Tombstone handling
                    // is deferred to the per-level branch below: if this
                    // flush produces the deepest blob we will `clean_self`
                    // there; otherwise tombstones must survive to shadow L1.
                    // todo - no need to make copy if there is no compactor
                    IndexTable::clone(dirty)
                } else {
                    let mut base = match current_levels.l0() {
                        Some(pos) => loader
                            .load(&ctx.ks_config, pos)
                            .expect("Failed to load L0 index in flusher thread"),
                        None => IndexTable::default(),
                    };
                    base.merge_dirty_and_clean(dirty);
                    base
                };
                (dirty.clone(), merged, current_levels.clone())
            }
            FlushKind::ForceRelocate { levels, .. } => {
                // Start from L1, merge L0 on top so L0 wins on overlap.
                // Force-relocate runs on clean entries only — no dirty
                // overlay to merge.
                ctx.metrics
                    .unload
                    .with_label_values(&["force_relocate"])
                    .inc();
                let mut merged = Self::load_l1_or_default(loader, ctx, levels.l1());
                if let Some(l0_pos) = levels.l0() {
                    let l0 = loader
                        .load(&ctx.ks_config, l0_pos)
                        .expect("Failed to load L0 for forced relocation");
                    merged.merge_dirty_and_clean(&l0);
                }
                let arc_index = Arc::new(merged);
                (
                    arc_index.clone(),
                    IndexTable::clone(&arc_index),
                    IndexLevels::new(),
                )
            }
        };
        let existing_l1 = current_levels.l1();

        // Unprocessed entries may still be read by an in-flight checkpoint, so
        // don't merge them to disk; keep them in the in-memory overlay.
        //
        // Ordering: this frontier filter must run before
        // `RelocationUpdates::apply` below. `apply` lifts re-pointed
        // disk-derived entries (relocated copies at WAL-tail offsets, i.e.
        // >= the frontier) into `data`, where this offset predicate would
        // misread them as unprocessed in-flight writes and drop them.
        merged_l0.retain_processed(loader.last_processed_wal_position());

        // `RelocationUpdates::apply` uses compare-and-set semantics — it can
        // only rewrite positions for keys already in `merged_l0`. Keys that
        // live only in L1 (the common case for post-promote `[INVALID, L1]`
        // cells) would be silently dropped. Pre-load L1 into `merged_l0` so
        // apply sees the full key set. The subsequent promote branch still
        // re-loads L1 from disk; the duplication is acceptable on the
        // relocation path — correctness first, optimize later.
        if relocation_updates.is_some() {
            assert!(
                !current_levels.is_sharded(),
                "RelocationUpdates on sharded cells is not yet supported",
            );
            if existing_l1.is_some() {
                let mut combined = Self::load_l1_or_default(loader, ctx, existing_l1);
                // `merged_l0` is the fresher overlay (L0 + dirty) — it wins
                // on overlapping keys, so treat it as the "dirty" side.
                combined.merge_dirty_and_clean(&merged_l0);
                merged_l0 = combined;
            }
        }
        if let Some(relocation_updates) = relocation_updates {
            relocation_updates.apply(&mut merged_l0);
        }

        match relocation_cutoff {
            Some(cutoff) => {
                let length = merged_l0.len();
                merged_l0.retain_above_position(cutoff);
                if merged_l0.len() == length {
                    return None;
                }
            }
            // TODO: Used only if relocation doesn't call sync flush. Remove if no such implementation is needed anymore
            None => merged_l0.retain_above_position(loader.min_wal_position()),
        }

        // Decide: write as a new L0 on top of the existing L1, or promote by
        // merging L0 into L1. `ForceRelocate` keeps single-level semantics.
        //
        // The read path walks `entry.levels()` — `get()` does a miss-chain
        // across levels and `next_in_cell` does a k-way merge — so
        // producing `[L0, L1]` is safe.
        let is_relocation = matches!(&command.flush_kind, FlushKind::ForceRelocate { .. });
        let l0_threshold = ctx.l0_max_entries();
        let over_threshold = !is_relocation && merged_l0.len() > l0_threshold;

        let new_levels = if over_threshold {
            ctx.metrics
                .promote_total
                .with_label_values(&[ctx.name()])
                .inc();
            if current_levels.is_sharded() {
                // Re-sharding: spread merged_l0 across existing shards, rewriting
                // only those that received keys (each possibly split again
                // if it overflows the shard budget). Untouched shards keep
                // their existing WAL blobs.
                Self::write_sharded_promote(
                    loader,
                    ctx,
                    command.ks,
                    &merged_l0,
                    current_levels.shards(),
                )
            } else {
                // Promote: merge the freshly-built L0 with the existing L1 (if
                // any). `merge_dirty_and_clean` lets `merged_l0` win on overlap
                // by treating it as the "dirty" overlay.
                let mut new_l1 = Self::load_l1_or_default(loader, ctx, existing_l1);
                new_l1.merge_dirty_and_clean(&merged_l0);
                Self::run_compactor(ctx, &mut new_l1);
                // L1 is the deepest level — nothing below to shadow, so drop
                // tombstones (and the keys they shadow) before writing.
                new_l1.clean_self();
                Self::write_promoted_l1(loader, ctx, command.ks, new_l1)
            }
        } else {
            Self::run_compactor(ctx, &mut merged_l0);
            // If no deeper level exists, this L0 is the deepest — strip
            // tombstones. Otherwise (L1 or shards below) leave them in so
            // they shadow the level beneath.
            let deeper_level_exists = existing_l1.is_some() || current_levels.is_sharded();
            if !deeper_level_exists {
                merged_l0.clean_self();
            }
            let new_l0_pos = loader
                .flush(command.ks, &merged_l0)
                .expect("Failed to flush index");
            if !is_relocation {
                ctx.metrics
                    .l0_bytes_written
                    .with_label_values(&[ctx.name()])
                    .inc_by(new_l0_pos.frame_len() as u64);
            }
            if current_levels.is_sharded() {
                IndexLevels::sharded_with_l0(new_l0_pos, current_levels.shards().clone())
            } else {
                let mut levels = IndexLevels::single(new_l0_pos);
                if let Some(l1) = existing_l1 {
                    levels.set(1, l1);
                }
                levels
            }
        };

        // Post-pass: any blob in `new_levels` whose position is still in
        // `relocate_positions` wasn't naturally rewritten by the merge/promote
        // above (untouched re-sharding shards, or a stale L1 under the L0-write
        // branch). Reflush those at fresh positions.
        let new_levels = match &command.flush_kind {
            FlushKind::Flush {
                relocate_positions, ..
            } => Self::relocate_in_levels(loader, ctx, command.ks, new_levels, relocate_positions),
            FlushKind::ForceRelocate { .. } => new_levels,
        };

        Some((original_index, new_levels))
    }

    /// Load the L1 blob at `l1`, or return an empty `IndexTable` when `l1` is
    /// `None`. The `expect` here matches the other flusher `load` call sites —
    /// a failure indicates a corrupt or missing blob and isn't recoverable on
    /// this thread.
    fn load_l1_or_default<L: Loader>(
        loader: &L,
        ctx: &KsContext,
        l1: Option<WalPosition>,
    ) -> IndexTable {
        match l1 {
            Some(pos) => loader
                .load(&ctx.ks_config, pos)
                .expect("Failed to load L1 index in flusher thread"),
            None => IndexTable::default(),
        }
    }

    // todo - result of compactor is not applied to in-memory index for DirtyLoaded
    fn run_compactor(ctx: &KsContext, index: &mut IndexTable) {
        if let Some(compactor) = ctx.ks_config.compactor() {
            let pre_compact_len = index.len();
            index.compact_with(|iter| (compactor)(iter));
            let compacted = pre_compact_len.saturating_sub(index.len());
            ctx.metrics
                .compacted_keys
                .with_label_values(&[ctx.name()])
                .inc_by(compacted as u64);
        }
    }

    /// Writes the freshly-promoted L1 image. When auto-sharding is enabled
    /// and the serialized blob exceeds the shard-split threshold, splits
    /// in-memory and flushes one blob per shard. The split is a pure
    /// in-memory partition of `new_l1` — no further shards are loaded
    /// from disk.
    fn write_promoted_l1<L: Loader>(
        loader: &L,
        ctx: &KsContext,
        ks: KeySpace,
        new_l1: IndexTable,
    ) -> IndexLevels {
        if let Some(threshold) = ctx.config.index_auto_shard_threshold {
            let serialized = ctx
                .ks_config
                .index_format()
                .serialize_index(&new_l1, &ctx.ks_config);
            if serialized.len() > threshold {
                return Self::flush_split_l1(loader, ctx, ks, new_l1, threshold);
            }
        }
        let pos = loader.flush(ks, &new_l1).expect("Failed to flush index");
        ctx.metrics
            .l1_bytes_written
            .with_label_values(&[ctx.name()])
            .inc_by(pos.frame_len() as u64);
        IndexLevels::promoted(pos)
    }

    /// Median-by-index splits `new_l1` until every shard's serialized
    /// size is ≤ `size_limit`, then flushes each as its own WAL blob.
    /// A shard that's still oversize after collapsing to a single entry
    /// is written as-is — the WAL layer will panic if the frame exceeds
    /// the fragment, matching the design's "reshape the keyspace" guidance.
    fn flush_split_l1<L: Loader>(
        loader: &L,
        ctx: &KsContext,
        ks: KeySpace,
        new_l1: IndexTable,
        size_limit: usize,
    ) -> IndexLevels {
        let entries: Vec<(Bytes, WalPosition)> = new_l1.iter().collect();
        let mut shard_tables: Vec<(Vec<u8>, Vec<u8>, IndexTable)> = Vec::new();
        Self::split_recursive(&entries, &ctx.ks_config, size_limit, &mut shard_tables);
        debug_assert!(
            !shard_tables.is_empty(),
            "split_recursive returned no shards for a non-empty L1",
        );

        let mut shards: BTreeMap<Vec<u8>, IndexShard> = BTreeMap::new();
        let total_bytes = Self::flush_shard_tables(loader, ks, &shard_tables, &mut shards);
        ctx.metrics
            .l1_bytes_written
            .with_label_values(&[ctx.name()])
            .inc_by(total_bytes);
        ctx.metrics
            .l1_shards_total
            .with_label_values(&[ctx.name()])
            .inc_by(shards.len() as u64);
        ctx.metrics
            .l1_shard_split_total
            .with_label_values(&[ctx.name()])
            .inc();
        IndexLevels::sharded(shards)
    }

    /// Flushes each `(min_key, max_key, table)` triple as its own WAL blob
    /// and inserts the resulting `IndexShard` into `target` keyed by
    /// `min_key`. Returns the total `frame_len` written.
    fn flush_shard_tables<L: Loader>(
        loader: &L,
        ks: KeySpace,
        tables: &[(Vec<u8>, Vec<u8>, IndexTable)],
        target: &mut BTreeMap<Vec<u8>, IndexShard>,
    ) -> u64 {
        let mut total_bytes = 0u64;
        for (min_key, max_key, table) in tables {
            let pos = loader.flush(ks, table).expect("Failed to flush shard");
            total_bytes += pos.frame_len() as u64;
            target.insert(min_key.clone(), IndexShard::new(pos, max_key.clone()));
        }
        total_bytes
    }

    /// Re-sharding promote: partitions `merged_l0` by owning shard,
    /// rewrites touched shards (re-splitting on overflow), and preserves
    /// untouched shard positions.
    fn write_sharded_promote<L: Loader>(
        loader: &L,
        ctx: &KsContext,
        ks: KeySpace,
        merged_l0: &IndexTable,
        current_shards: &BTreeMap<Vec<u8>, IndexShard>,
    ) -> IndexLevels {
        // `iter_with_tombstones` collapses Removed→INVALID and discards the
        // original offset. Use the WAL tail as the placeholder so the merge
        // path's monotonic-offset assertion holds.
        let tombstone_placeholder = WalPosition::new(loader.current_wal_position().max(1), 1);
        // `usize::MAX` when auto-sharding is disabled: re-sharding still
        // rewrites touched shards but never splits any further.
        let split_threshold = ctx.config.index_auto_shard_threshold.unwrap_or(usize::MAX);
        let first_owner: &[u8] = current_shards
            .keys()
            .next()
            .map(Vec::as_slice)
            .expect("write_sharded_promote requires non-empty current_shards");

        // Owner = largest btree_key ≤ key. Keys below the first shard's
        // btree key fall to the first shard so they extend it leftward.
        // Partition keys borrow into `current_shards`, so the routing loop
        // does not allocate per dirty key.
        let mut partitions: BTreeMap<&[u8], IndexTable> = BTreeMap::new();
        for (key, pos) in merged_l0.iter_with_tombstones() {
            let owner: &[u8] = current_shards
                .range::<[u8], _>((Bound::Unbounded, Bound::Included(key.as_ref())))
                .next_back()
                .map(|(bk, _)| bk.as_slice())
                .unwrap_or(first_owner);
            let table = partitions.entry(owner).or_default();
            if pos.is_valid() {
                table.insert(key, pos);
            } else {
                table.remove(key, tombstone_placeholder);
            }
        }

        let mut new_shards: BTreeMap<Vec<u8>, IndexShard> = BTreeMap::new();
        let mut total_bytes: u64 = 0;
        let mut rewritten: u64 = 0;
        let mut split_events: u64 = 0;

        for (btree_key, shard) in current_shards.iter() {
            let Some(dirty_subset) = partitions.remove(btree_key.as_slice()) else {
                new_shards.insert(btree_key.clone(), shard.clone());
                continue;
            };
            rewritten += 1;
            let mut shard_table = loader
                .load(&ctx.ks_config, shard.position)
                .expect("Failed to load shard for sharded promote");
            shard_table.merge_dirty_and_clean(&dirty_subset);
            Self::run_compactor(ctx, &mut shard_table);
            // Shard is the deepest level for its key range; strip tombstones.
            shard_table.clean_self();

            if shard_table.is_empty() {
                // All entries in this shard's range were tombstoned out.
                continue;
            }

            let entries: Vec<(Bytes, WalPosition)> = shard_table.iter().collect();
            let serialized = ctx
                .ks_config
                .index_format()
                .serialize_index(&shard_table, &ctx.ks_config);
            if serialized.len() <= split_threshold {
                let (min_key, max_key) = shard_bounds(&entries);
                let single = [(min_key, max_key, shard_table)];
                total_bytes += Self::flush_shard_tables(loader, ks, &single, &mut new_shards);
            } else {
                split_events += 1;
                let mut sub_shards: Vec<(Vec<u8>, Vec<u8>, IndexTable)> = Vec::new();
                Self::split_recursive(&entries, &ctx.ks_config, split_threshold, &mut sub_shards);
                total_bytes += Self::flush_shard_tables(loader, ks, &sub_shards, &mut new_shards);
            }
        }
        debug_assert!(
            partitions.is_empty(),
            "every partition key must match a current shard btree key",
        );

        ctx.metrics
            .l1_bytes_written
            .with_label_values(&[ctx.name()])
            .inc_by(total_bytes);
        ctx.metrics
            .l1_shard_rewritten_total
            .with_label_values(&[ctx.name()])
            .inc_by(rewritten);
        ctx.metrics
            .l1_shards_total
            .with_label_values(&[ctx.name()])
            .inc_by(new_shards.len() as u64);
        ctx.metrics
            .l1_shard_split_total
            .with_label_values(&[ctx.name()])
            .inc_by(split_events);

        if new_shards.is_empty() {
            // Every shard got tombstoned out. Fall back to a single empty
            // L1 blob so the cell still has a valid post-promote shape.
            let empty = IndexTable::default();
            let pos = loader
                .flush(ks, &empty)
                .expect("Failed to flush empty post-collapse L1");
            return IndexLevels::promoted(pos);
        }

        IndexLevels::sharded(new_shards)
    }

    /// Walks `levels` and reflushes every blob whose position appears in
    /// `relocate_positions` at a fresh WAL position; preserves the rest in
    /// place. Returns new `IndexLevels` with the same shape (sharded vs
    /// unsharded, L0/L1 presence) as the input, only positions changed.
    ///
    /// Used by two call paths:
    /// - Sharded `ForceRelocate` (clean entry): the entire rewrite.
    /// - Flush post-pass: rewrite the blobs the normal merge/promote left
    ///   in place (e.g. untouched shards under re-sharding, or a stale L1 when
    ///   the dirty overlay was small enough for the L0-write branch).
    fn relocate_in_levels<L: Loader>(
        loader: &L,
        ctx: &KsContext,
        ks: KeySpace,
        levels: IndexLevels,
        relocate_positions: &HashSet<WalPosition>,
    ) -> IndexLevels {
        if relocate_positions.is_empty() {
            return levels;
        }

        let mut total_bytes: u64 = 0;
        let mut shards_rewritten: u64 = 0;

        let new_l0 = match levels.l0() {
            Some(pos) if relocate_positions.contains(&pos) => {
                let new_pos = Self::reflush_blob_at(loader, ctx, ks, pos, "L0");
                total_bytes += new_pos.frame_len() as u64;
                Some(new_pos)
            }
            other => other,
        };

        let new_levels = if levels.is_sharded() {
            let mut new_shards: BTreeMap<Vec<u8>, IndexShard> = BTreeMap::new();
            for (btree_key, shard) in levels.shards().iter() {
                let new_pos = if relocate_positions.contains(&shard.position) {
                    let p = Self::reflush_blob_at(loader, ctx, ks, shard.position, "shard");
                    total_bytes += p.frame_len() as u64;
                    shards_rewritten += 1;
                    p
                } else {
                    shard.position
                };
                new_shards.insert(
                    btree_key.clone(),
                    IndexShard::new(new_pos, shard.max_key.clone()),
                );
            }
            match new_l0 {
                Some(l0) => IndexLevels::sharded_with_l0(l0, new_shards),
                None => IndexLevels::sharded(new_shards),
            }
        } else {
            let new_l1 = match levels.l1() {
                Some(pos) if relocate_positions.contains(&pos) => {
                    let new_pos = Self::reflush_blob_at(loader, ctx, ks, pos, "L1");
                    total_bytes += new_pos.frame_len() as u64;
                    Some(new_pos)
                }
                other => other,
            };
            match (new_l0, new_l1) {
                (None, None) => IndexLevels::new(),
                (Some(l0), None) => IndexLevels::single(l0),
                (None, Some(l1)) => IndexLevels::promoted(l1),
                (Some(l0), Some(l1)) => {
                    let mut lvls = IndexLevels::single(l0);
                    lvls.set(1, l1);
                    lvls
                }
            }
        };

        if shards_rewritten > 0 {
            ctx.metrics
                .l1_shard_rewritten_total
                .with_label_values(&[ctx.name()])
                .inc_by(shards_rewritten);
            ctx.metrics
                .l1_shards_total
                .with_label_values(&[ctx.name()])
                .inc_by(shards_rewritten);
        }
        if total_bytes > 0 {
            ctx.metrics
                .l1_bytes_written
                .with_label_values(&[ctx.name()])
                .inc_by(total_bytes);
        }

        new_levels
    }

    /// Loads the blob at `pos` and reflushes it at a fresh WAL position.
    /// `label` names the blob ("shard", "L0") for `expect` messages.
    fn reflush_blob_at<L: Loader>(
        loader: &L,
        ctx: &KsContext,
        ks: KeySpace,
        pos: WalPosition,
        label: &str,
    ) -> WalPosition {
        let table = loader
            .load(&ctx.ks_config, pos)
            .unwrap_or_else(|_| panic!("Failed to load {label} for sharded force-relocate"));
        loader
            .flush(ks, &table)
            .unwrap_or_else(|_| panic!("Failed to flush {label} for sharded force-relocate"))
    }

    /// Recursively bisect `entries` by index. Appends each chunk that
    /// fits `size_limit` (or has a single entry, which is always
    /// accepted) to `out`.
    fn split_recursive(
        entries: &[(Bytes, WalPosition)],
        ks: &KeySpaceDesc,
        size_limit: usize,
        out: &mut Vec<(Vec<u8>, Vec<u8>, IndexTable)>,
    ) {
        if entries.is_empty() {
            return;
        }
        let table = Self::build_shard_table(entries);
        let serialized = ks.index_format().serialize_index(&table, ks);
        if serialized.len() <= size_limit || entries.len() == 1 {
            let (min_key, max_key) = shard_bounds(entries);
            out.push((min_key, max_key, table));
            return;
        }
        let mid = entries.len() / 2;
        Self::split_recursive(&entries[..mid], ks, size_limit, out);
        Self::split_recursive(&entries[mid..], ks, size_limit, out);
    }

    fn build_shard_table(entries: &[(Bytes, WalPosition)]) -> IndexTable {
        let mut table = IndexTable::default();
        for (k, v) in entries {
            table.insert(k.clone(), *v);
        }
        table
    }
}

pub struct SendGuard(#[allow(dead_code)] parking_lot::ArcMutexGuard<parking_lot::RawMutex, ()>);

// Rather than enable send_guard feature globally in parking_lot,
// using this just for flusher barrier
unsafe impl Send for SendGuard {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::index::index_format::IndexFormat;
    use crate::key_shape::{KeyShape, KeySpace, KeySpaceConfig, KeyType};
    use crate::wal::WalRandomRead;
    use crate::wal::position::{LastProcessed, WalPosition};
    use minibytes::Bytes;
    use parking_lot::Mutex;
    use std::collections::HashMap;
    use std::io;

    /// In-memory `Loader` that records each `flush` call's table length
    /// and stores the flushed table in `blobs` keyed by position offset,
    /// so a subsequent `load` returns the same content (matching the
    /// real WAL round-trip semantically, without serialization).
    struct RecordingLoader {
        flushes: Mutex<Vec<usize>>, // entry count of each flushed table, in order
        blobs: Mutex<HashMap<u64, IndexTable>>,
        next_offset: Mutex<u64>,
    }

    impl RecordingLoader {
        fn new() -> Self {
            Self {
                flushes: Mutex::new(Vec::new()),
                blobs: Mutex::new(HashMap::new()),
                next_offset: Mutex::new(1000),
            }
        }

        fn flush_count(&self) -> usize {
            self.flushes.lock().len()
        }

        fn flush_sizes(&self) -> Vec<usize> {
            self.flushes.lock().clone()
        }

        /// Pre-seeds the loader with a blob at the given position so
        /// re-sharding tests can simulate "this shard already exists on disk".
        fn seed(&self, pos: WalPosition, table: IndexTable) {
            self.blobs.lock().insert(pos.offset(), table);
        }
    }

    impl Loader for RecordingLoader {
        type Error = io::Error;

        fn load(
            &self,
            _ks: &crate::key_shape::KeySpaceDesc,
            position: WalPosition,
        ) -> Result<IndexTable, Self::Error> {
            Ok(self
                .blobs
                .lock()
                .get(&position.offset())
                .cloned()
                .unwrap_or_default())
        }

        fn index_reader(&self, _position: WalPosition) -> Result<WalRandomRead, Self::Error> {
            unreachable!("RecordingLoader does not serve index reads")
        }

        fn flush_supported(&self) -> bool {
            true
        }

        fn flush(&self, _ks: KeySpace, data: &IndexTable) -> Result<WalPosition, Self::Error> {
            self.flushes.lock().push(data.len());
            let mut off = self.next_offset.lock();
            let pos = WalPosition::new(*off, 64);
            *off += 4096;
            self.blobs.lock().insert(pos.offset(), data.clone());
            Ok(pos)
        }

        fn last_processed_wal_position(&self) -> LastProcessed {
            // The seeded/flushed entries represent already-processed data being
            // flushed, so report a frontier above every seeded offset (it is
            // strictly less than `current_wal_position`). This keeps the
            // flusher's `retain_processed` filter a no-op for these tests; with
            // `none()` (= 0) every entry would count as unprocessed and the
            // whole blob would be dropped.
            LastProcessed::new(u64::MAX / 2)
        }

        fn current_wal_position(&self) -> u64 {
            // Strictly greater than any seeded/flushed offset so the
            // tombstone-placeholder offset in write_sharded_promote
            // satisfies merge_dirty's "must be increasing" assertion.
            u64::MAX / 2
        }

        fn min_wal_position(&self) -> u64 {
            0
        }
    }

    fn make_ctx(auto_sharding: bool, frag_size: u64) -> KsContext {
        let mut config = Config::small();
        config.frag_size = frag_size;
        config.l0_max_entries = Some(64);
        if auto_sharding {
            config.with_index_auto_sharding();
        }
        let shape =
            KeyShape::new_single_config(8, 1, KeyType::uniform(8), KeySpaceConfig::default());
        let ks_id = KeySpace::first();
        let ks = shape.ks(ks_id).clone();
        KsContext::new(Arc::new(config), ks, Metrics::new())
    }

    fn build_table(n: usize) -> IndexTable {
        let mut t = IndexTable::default();
        for i in 0..n as u64 {
            let key = (i + 1).to_be_bytes().to_vec(); // 8-byte BE, all unique
            t.insert(Bytes::from(key), WalPosition::test_value(i + 1));
        }
        t
    }

    fn flush_command(ks: KeySpace, dirty: IndexTable, current: IndexLevels) -> IndexFlushCommand {
        IndexFlushCommand::new(
            ks,
            CellId::Integer(0),
            FlushKind::Flush {
                dirty: Arc::new(dirty),
                current_levels: current,
                loaded: true,
                relocate_positions: HashSet::new(),
            },
        )
    }

    /// Like `flush_command` but with the given positions flagged for
    /// post-flush relocation. Used by the dirty-sharded-relocate tests.
    fn flush_command_with_relocation(
        ks: KeySpace,
        dirty: IndexTable,
        current: IndexLevels,
        relocate_positions: impl IntoIterator<Item = WalPosition>,
    ) -> IndexFlushCommand {
        IndexFlushCommand::new(
            ks,
            CellId::Integer(0),
            FlushKind::Flush {
                dirty: Arc::new(dirty),
                current_levels: current,
                loaded: true,
                relocate_positions: relocate_positions.into_iter().collect(),
            },
        )
    }

    fn serialized_size(ks: &crate::key_shape::KeySpaceDesc, t: &IndexTable) -> usize {
        ks.index_format().serialize_index(t, ks).len()
    }

    #[test]
    fn auto_sharding_off_writes_single_l1_even_when_huge() {
        let ctx = make_ctx(false, 4096);
        let loader = RecordingLoader::new();
        // Far above l0_max_entries=64 and the synthetic 2 KiB threshold a
        // 4 KiB frag_size would imply — but auto_sharding is off, so the
        // flusher must keep today's single-blob shape.
        let dirty = build_table(1000);
        let cmd = flush_command(ctx.id(), dirty, IndexLevels::new());
        let (_orig, new_levels) =
            IndexFlusherThread::handle_command(&loader, &cmd, &ctx, None, None).unwrap();
        assert!(!new_levels.is_sharded());
        assert_eq!(new_levels.l0(), None);
        assert!(new_levels.l1().is_some(), "expected promoted [INVALID, L1]");
        assert_eq!(loader.flush_count(), 1);
    }

    #[test]
    fn auto_sharding_on_small_l1_stays_unsharded() {
        // L1 fits in one fragment → no split, identical to the
        // auto_sharding=false shape.
        let ctx = make_ctx(true, 1024 * 1024);
        let loader = RecordingLoader::new();
        let dirty = build_table(200);
        let cmd = flush_command(ctx.id(), dirty, IndexLevels::new());
        let (_orig, new_levels) =
            IndexFlusherThread::handle_command(&loader, &cmd, &ctx, None, None).unwrap();
        assert!(!new_levels.is_sharded());
        assert!(new_levels.l1().is_some());
        assert_eq!(loader.flush_count(), 1);
    }

    #[test]
    fn auto_sharding_on_large_l1_splits_into_multiple_shards() {
        // Tiny frag_size forces the split threshold below the serialized
        // size of the merged_l0, so the flusher must shard.
        let ctx = make_ctx(true, 4096);
        let loader = RecordingLoader::new();
        let dirty = build_table(1000);
        // Sanity: serialized size > the shard-split budget.
        assert!(
            serialized_size(&ctx.ks_config, &dirty)
                > ctx.config.index_auto_shard_threshold.unwrap()
        );

        let cmd = flush_command(ctx.id(), dirty, IndexLevels::new());
        let (_orig, new_levels) =
            IndexFlusherThread::handle_command(&loader, &cmd, &ctx, None, None).unwrap();
        assert!(new_levels.is_sharded(), "expected sharded IndexLevels");
        assert_eq!(
            new_levels.l1(),
            None,
            "L1 slot must be vacated when sharded"
        );
        assert!(new_levels.shards().len() > 1, "expected at least 2 shards");
        // Flush count matches the shard count exactly — one WAL blob per
        // shard, no extra writes.
        assert_eq!(loader.flush_count(), new_levels.shards().len());
        // Flushed entry counts add up to the original L1 size.
        assert_eq!(loader.flush_sizes().iter().sum::<usize>(), 1000);

        // Shards are disjoint and totally cover the input keyset.
        let shards: Vec<(&Vec<u8>, &IndexShard)> = new_levels.shards().iter().collect();
        let mut prev_max: Option<&[u8]> = None;
        for (min_key, shard) in &shards {
            assert!(min_key.as_slice() <= shard.max_key.as_slice());
            if let Some(prev) = prev_max {
                assert!(prev < min_key.as_slice(), "shards must be disjoint");
            }
            prev_max = Some(shard.max_key.as_slice());
        }
        assert_eq!(shards.first().unwrap().0, &1_u64.to_be_bytes().to_vec());
        assert_eq!(
            shards.last().unwrap().1.max_key,
            1000_u64.to_be_bytes().to_vec(),
        );

        // Re-run split_recursive directly to confirm every shard fits
        // the budget — the on-disk shape the WAL layer accepts.
        let threshold = ctx.config.index_auto_shard_threshold.unwrap();
        let entries: Vec<(Bytes, WalPosition)> = build_table(1000).iter().collect();
        let mut shard_tables: Vec<(Vec<u8>, Vec<u8>, IndexTable)> = Vec::new();
        IndexFlusherThread::split_recursive(&entries, &ctx.ks_config, threshold, &mut shard_tables);
        for (_, _, t) in &shard_tables {
            assert!(serialized_size(&ctx.ks_config, t) <= threshold);
        }
        assert_eq!(shard_tables.len(), new_levels.shards().len());
    }

    #[test]
    fn split_recursive_singleton_when_one_entry_still_too_big() {
        // Defensive: with size_limit=0 every shard is "too big", but
        // split_recursive bottoms out at len=1 instead of recursing forever.
        let ctx = make_ctx(true, 1024 * 1024);
        let entries: Vec<(Bytes, WalPosition)> = build_table(4).iter().collect();
        let mut out: Vec<(Vec<u8>, Vec<u8>, IndexTable)> = Vec::new();
        IndexFlusherThread::split_recursive(&entries, &ctx.ks_config, 0, &mut out);
        assert_eq!(out.len(), 4, "len=1 shards must be accepted defensively");
        for (min_key, max_key, _) in &out {
            assert_eq!(min_key, max_key, "single-entry shard has min==max");
        }
    }

    // ----- Re-sharding (incremental sharded promote) -----

    /// Seeds the loader with a sharded cell whose shards cover the given
    /// inclusive `(min, max)` u64 ranges.
    fn seed_sharded_cell(
        loader: &RecordingLoader,
        ranges: &[(u64, u64)],
    ) -> (IndexLevels, Vec<WalPosition>) {
        let mut shards: BTreeMap<Vec<u8>, IndexShard> = BTreeMap::new();
        let mut positions = Vec::new();
        let mut next_seed_offset: u64 = 500;
        for (min, max) in ranges {
            let mut table = IndexTable::default();
            for k in *min..=*max {
                table.insert(
                    Bytes::from(k.to_be_bytes().to_vec()),
                    WalPosition::test_value(k.max(1)),
                );
            }
            let pos = WalPosition::new(next_seed_offset, 64);
            next_seed_offset += 4096;
            loader.seed(pos, table);
            positions.push(pos);
            shards.insert(
                min.to_be_bytes().to_vec(),
                IndexShard::new(pos, max.to_be_bytes().to_vec()),
            );
        }
        (IndexLevels::sharded(shards), positions)
    }

    /// Builds an `IndexTable` from an iterator of `(key, Option<pos>)`.
    /// `None` means tombstone (remove with a placeholder offset). Used to
    /// construct the dirty L0 overlay for re-sharding tests.
    fn build_dirty(entries: &[(u64, Option<u64>)]) -> IndexTable {
        let mut t = IndexTable::default();
        for (k, p) in entries {
            let key = Bytes::from(k.to_be_bytes().to_vec());
            match p {
                Some(pos) => {
                    t.insert(key, WalPosition::test_value(*pos));
                }
                None => {
                    t.remove(key, WalPosition::test_value(u64::MAX / 4));
                }
            }
        }
        t
    }

    fn shard_key(k: u64) -> Vec<u8> {
        k.to_be_bytes().to_vec()
    }

    /// Reads the new shard at `min_key` from the loader's blob store and
    /// returns its sorted entries as `(key_u64, pos_u64)` for assertion.
    fn shard_entries(loader: &RecordingLoader, pos: WalPosition) -> Vec<u64> {
        let blob = loader.blobs.lock().get(&pos.offset()).cloned().unwrap();
        blob.iter()
            .map(|(k, _)| {
                let bytes: [u8; 8] = k.as_ref().try_into().unwrap();
                u64::from_be_bytes(bytes)
            })
            .collect()
    }

    #[test]
    fn case_b_untouched_shards_keep_their_position() {
        // Two shards. Dirty L0 hits only the first shard's range.
        let ctx = make_ctx(true, 1024 * 1024);
        let loader = RecordingLoader::new();
        let (current_levels, positions) = seed_sharded_cell(&loader, &[(1, 100), (200, 300)]);

        // 65 dirty overwrites in shard A's existing [1, 100] range; >
        // l0_max_entries=64 so the flusher promotes via re-sharding.
        let dirty_entries: Vec<(u64, Option<u64>)> =
            (1..=65u64).map(|k| (k, Some(k + 10_000))).collect();
        let dirty = build_dirty(&dirty_entries);

        let cmd = flush_command(ctx.id(), dirty, current_levels);
        let (_orig, new_levels) =
            IndexFlusherThread::handle_command(&loader, &cmd, &ctx, None, None).unwrap();

        assert!(new_levels.is_sharded(), "still sharded after re-sharding");
        assert_eq!(new_levels.shards().len(), 2, "two shards expected");

        // Shard B (200..=300) is untouched: its btree key and position
        // should still match what we seeded.
        let new_b = new_levels.shards().get(&shard_key(200)).unwrap();
        assert_eq!(new_b.position, positions[1]);
        assert_eq!(new_b.max_key, shard_key(300));

        // Shard A was rewritten; min/max bounds unchanged.
        let new_a = new_levels.shards().get(&shard_key(1)).unwrap();
        assert_ne!(new_a.position, positions[0]);
        assert_eq!(new_a.max_key, shard_key(100));

        // Exactly one shard rewritten → one flush.
        assert_eq!(loader.flush_count(), 1);
    }

    #[test]
    fn case_b_tombstones_drop_keys_from_shard() {
        let ctx = make_ctx(true, 1024 * 1024);
        let loader = RecordingLoader::new();
        let (current_levels, _) = seed_sharded_cell(&loader, &[(1, 5), (100, 200)]);

        // 65 dirty entries to trigger promote: 2 tombstones in shard A
        // (keys 2 and 4) plus 63 inserts in shard B (101..=163, already
        // present in B — overwritten with newer offsets).
        let mut dirty_entries: Vec<(u64, Option<u64>)> = vec![(2, None), (4, None)];
        for k in 101..=163u64 {
            dirty_entries.push((k, Some(k + 50_000)));
        }
        let dirty = build_dirty(&dirty_entries);

        let cmd = flush_command(ctx.id(), dirty, current_levels);
        let (_orig, new_levels) =
            IndexFlusherThread::handle_command(&loader, &cmd, &ctx, None, None).unwrap();
        assert!(new_levels.is_sharded());
        assert_eq!(new_levels.shards().len(), 2);

        let new_a = new_levels.shards().get(&shard_key(1)).unwrap();
        let a_keys = shard_entries(&loader, new_a.position);
        assert_eq!(a_keys, vec![1, 3, 5], "tombstones removed keys 2 and 4");
        assert_eq!(new_a.max_key, shard_key(5));

        let new_b = new_levels.shards().get(&shard_key(100)).unwrap();
        let b_keys = shard_entries(&loader, new_b.position);
        assert_eq!(b_keys.first(), Some(&100));
        assert_eq!(b_keys.last(), Some(&200));
    }

    #[test]
    fn case_b_gap_key_routed_to_predecessor_shard() {
        // Shards A (1..=50) and B (200..=300). Dirty key 100 lies in the
        // gap; the predecessor rule sends it to A, extending A.max_key.
        let ctx = make_ctx(true, 1024 * 1024);
        let loader = RecordingLoader::new();
        let (current_levels, _) = seed_sharded_cell(&loader, &[(1, 50), (200, 300)]);

        let mut dirty_entries: Vec<(u64, Option<u64>)> =
            (1..=65u64).map(|k| (k, Some(k + 10_000))).collect();
        // The single gap-key that should land in A.
        dirty_entries.push((100, Some(100_000)));
        let dirty = build_dirty(&dirty_entries);

        let cmd = flush_command(ctx.id(), dirty, current_levels);
        let (_orig, new_levels) =
            IndexFlusherThread::handle_command(&loader, &cmd, &ctx, None, None).unwrap();
        let new_a = new_levels.shards().get(&shard_key(1)).unwrap();
        assert_eq!(
            new_a.max_key,
            shard_key(100),
            "gap key 100 extends shard A's max_key",
        );
        assert!(
            shard_entries(&loader, new_a.position).contains(&100),
            "key 100 is present in shard A",
        );
        // Shard B is untouched.
        let new_b = new_levels.shards().get(&shard_key(200)).unwrap();
        assert_eq!(new_b.max_key, shard_key(300));
    }

    #[test]
    fn case_b_rewritten_shard_splits_when_oversized() {
        // Tiny frag_size forces a re-split when shard A grows past
        // frag_size/2 after the merge.
        let ctx = make_ctx(true, 4096);
        let loader = RecordingLoader::new();
        let (current_levels, _) = seed_sharded_cell(&loader, &[(1, 100), (10_000, 10_010)]);

        // 500 dirty inserts in shard A's extended range so the rewritten
        // shard A blob crosses the 2 KiB threshold and re-splits.
        let dirty_entries: Vec<(u64, Option<u64>)> =
            (1..=500u64).map(|k| (k, Some(k + 1_000))).collect();
        let dirty = build_dirty(&dirty_entries);

        let cmd = flush_command(ctx.id(), dirty, current_levels);
        let (_orig, new_levels) =
            IndexFlusherThread::handle_command(&loader, &cmd, &ctx, None, None).unwrap();
        assert!(new_levels.is_sharded());
        // Started with 2 shards; A got split into multiple, B is untouched.
        assert!(
            new_levels.shards().len() > 2,
            "shard A was re-split: total shards > 2, got {}",
            new_levels.shards().len(),
        );
        // B's btree entry survives unchanged.
        let new_b = new_levels.shards().get(&shard_key(10_000)).unwrap();
        assert_eq!(new_b.max_key, shard_key(10_010));
    }

    #[test]
    fn case_b_all_keys_tombstoned_collapses_to_empty_promoted() {
        // One shard with 3 keys; all tombstoned + a few padding inserts
        // in a separate range so we cross l0_max_entries. The shard's
        // entries are dropped; with no remaining shards, the cell
        // collapses to an empty `IndexLevels::promoted` blob.
        let ctx = make_ctx(true, 1024 * 1024);
        let loader = RecordingLoader::new();
        let (current_levels, _) = seed_sharded_cell(&loader, &[(1, 3)]);

        let mut dirty_entries: Vec<(u64, Option<u64>)> = vec![(1, None), (2, None), (3, None)];
        // Padding to cross l0_max_entries=64. These go to the same shard
        // by the predecessor rule (they're > 3, so above the only shard's
        // max_key — predecessor is the single shard). Then they're also
        // tombstoned so the shard ends up empty.
        for k in 100..=200u64 {
            dirty_entries.push((k, None));
        }
        let dirty = build_dirty(&dirty_entries);

        let cmd = flush_command(ctx.id(), dirty, current_levels);
        let (_orig, new_levels) =
            IndexFlusherThread::handle_command(&loader, &cmd, &ctx, None, None).unwrap();
        assert!(
            !new_levels.is_sharded(),
            "empty-shard cell collapses to unsharded",
        );
        assert!(
            new_levels.l1().is_some(),
            "fallback is IndexLevels::promoted"
        );
    }

    #[test]
    fn case_b_sharded_l0_path_preserves_shards() {
        // Sharded cell + a tiny dirty L0 that doesn't trigger promote.
        // Flusher writes the new L0 and keeps the shard btree intact.
        let ctx = make_ctx(true, 1024 * 1024);
        let loader = RecordingLoader::new();
        let (current_levels, positions) = seed_sharded_cell(&loader, &[(1, 50), (200, 300)]);

        // Tiny dirty: 10 entries, below l0_max_entries=64.
        let dirty_entries: Vec<(u64, Option<u64>)> =
            (1..=10u64).map(|k| (k, Some(k + 10_000))).collect();
        let dirty = build_dirty(&dirty_entries);

        let cmd = flush_command(ctx.id(), dirty, current_levels);
        let (_orig, new_levels) =
            IndexFlusherThread::handle_command(&loader, &cmd, &ctx, None, None).unwrap();

        assert!(new_levels.is_sharded(), "shards preserved on L0-only flush");
        assert!(new_levels.l0().is_some(), "new L0 written");
        let s = new_levels.shards();
        assert_eq!(s.len(), 2);
        // Both shard btree entries are unchanged.
        assert_eq!(s.get(&shard_key(1)).unwrap().position, positions[0]);
        assert_eq!(s.get(&shard_key(200)).unwrap().position, positions[1]);
        // Exactly one flush (the new L0).
        assert_eq!(loader.flush_count(), 1);
    }

    /// Builds a ForceRelocate command for the given levels with the supplied
    /// positions flagged for relocation.
    fn force_relocate_command(
        ks: KeySpace,
        levels: IndexLevels,
        relocate_positions: impl IntoIterator<Item = WalPosition>,
    ) -> IndexFlushCommand {
        IndexFlushCommand::new(
            ks,
            CellId::Integer(0),
            FlushKind::ForceRelocate {
                levels,
                relocate_positions: relocate_positions.into_iter().collect(),
            },
        )
    }

    #[test]
    fn sharded_force_relocate_rewrites_only_flagged_shards() {
        // Two shards seeded. Flag only shard A's position for relocation;
        // shard B must keep its WAL blob position, A must move.
        let ctx = make_ctx(true, 1024 * 1024);
        let loader = RecordingLoader::new();
        let (current_levels, positions) = seed_sharded_cell(&loader, &[(1, 5), (100, 200)]);
        let pos_a = positions[0];
        let pos_b = positions[1];

        let cmd = force_relocate_command(ctx.id(), current_levels, [pos_a]);
        let (_orig, new_levels) =
            IndexFlusherThread::handle_command(&loader, &cmd, &ctx, None, None).unwrap();

        assert!(new_levels.is_sharded(), "sharded shape preserved");
        assert_eq!(new_levels.shards().len(), 2);
        assert_eq!(new_levels.l0(), None);

        let new_a = new_levels.shards().get(&shard_key(1)).unwrap();
        let new_b = new_levels.shards().get(&shard_key(100)).unwrap();
        assert_ne!(new_a.position, pos_a, "shard A reflushed at fresh position");
        assert_eq!(new_a.max_key, shard_key(5), "shard A bounds unchanged");
        assert_eq!(new_b.position, pos_b, "shard B untouched");
        assert_eq!(new_b.max_key, shard_key(200));
        // Exactly one flush: shard A's rewrite. B is untouched, no L0.
        assert_eq!(loader.flush_count(), 1);

        // Content of the rewritten shard A blob equals the original.
        let a_keys = shard_entries(&loader, new_a.position);
        assert_eq!(a_keys, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn sharded_force_relocate_rewrites_all_flagged() {
        // Flag both shards: both move, ordering and bounds preserved.
        let ctx = make_ctx(true, 1024 * 1024);
        let loader = RecordingLoader::new();
        let (current_levels, positions) = seed_sharded_cell(&loader, &[(1, 5), (100, 110)]);

        let cmd = force_relocate_command(ctx.id(), current_levels, [positions[0], positions[1]]);
        let (_orig, new_levels) =
            IndexFlusherThread::handle_command(&loader, &cmd, &ctx, None, None).unwrap();

        assert!(new_levels.is_sharded());
        assert_eq!(new_levels.shards().len(), 2);
        let new_a = new_levels.shards().get(&shard_key(1)).unwrap();
        let new_b = new_levels.shards().get(&shard_key(100)).unwrap();
        assert_ne!(new_a.position, positions[0]);
        assert_ne!(new_b.position, positions[1]);
        assert_eq!(loader.flush_count(), 2);
    }

    #[test]
    fn sharded_force_relocate_rewrites_l0_when_flagged() {
        // Sharded cell carrying an L0. Flag only the L0 blob — shards stay
        // in place, L0 moves.
        let ctx = make_ctx(true, 1024 * 1024);
        let loader = RecordingLoader::new();
        let (sharded_levels, positions) = seed_sharded_cell(&loader, &[(1, 5), (100, 110)]);
        // Seed an L0 blob.
        let l0_pos = WalPosition::new(7_000, 64);
        let mut l0_table = IndexTable::default();
        l0_table.insert(
            Bytes::from(50u64.to_be_bytes().to_vec()),
            WalPosition::test_value(99),
        );
        loader.seed(l0_pos, l0_table);
        let levels = IndexLevels::sharded_with_l0(l0_pos, sharded_levels.shards().clone());

        let cmd = force_relocate_command(ctx.id(), levels, [l0_pos]);
        let (_orig, new_levels) =
            IndexFlusherThread::handle_command(&loader, &cmd, &ctx, None, None).unwrap();

        assert!(new_levels.is_sharded());
        // L0 moved.
        let new_l0 = new_levels.l0().expect("L0 still present");
        assert_ne!(new_l0, l0_pos, "L0 rewritten at fresh position");
        // Shards untouched.
        let new_a = new_levels.shards().get(&shard_key(1)).unwrap();
        let new_b = new_levels.shards().get(&shard_key(100)).unwrap();
        assert_eq!(new_a.position, positions[0]);
        assert_eq!(new_b.position, positions[1]);
        // Exactly one flush: the L0 rewrite.
        assert_eq!(loader.flush_count(), 1);
    }

    #[test]
    fn dirty_sharded_l0_write_relocates_flagged_shard() {
        // Small dirty overlay → L0-write branch (no promote). Existing shards
        // carry over in `new_levels`. The post-pass must rewrite the flagged
        // shard at a fresh position; the unflagged shard stays.
        let ctx = make_ctx(true, 1024 * 1024);
        let loader = RecordingLoader::new();
        let (current_levels, positions) = seed_sharded_cell(&loader, &[(1, 5), (100, 110)]);
        let pos_a = positions[0];
        let pos_b = positions[1];

        // 5 dirty entries — well below l0_max_entries=64, so no promote.
        let dirty: Vec<(u64, Option<u64>)> = (1..=5u64).map(|k| (k, Some(k + 1000))).collect();
        let cmd =
            flush_command_with_relocation(ctx.id(), build_dirty(&dirty), current_levels, [pos_a]);
        let (_orig, new_levels) =
            IndexFlusherThread::handle_command(&loader, &cmd, &ctx, None, None).unwrap();

        // Shape: sharded with fresh L0.
        assert!(new_levels.is_sharded());
        assert!(new_levels.l0().is_some(), "L0-write branch wrote a new L0");
        assert_eq!(new_levels.shards().len(), 2);

        let new_a = new_levels.shards().get(&shard_key(1)).unwrap();
        let new_b = new_levels.shards().get(&shard_key(100)).unwrap();
        assert_ne!(
            new_a.position, pos_a,
            "flagged shard A reflushed by post-pass"
        );
        assert_eq!(new_b.position, pos_b, "unflagged shard B preserved");
    }

    #[test]
    fn dirty_sharded_resharding_relocates_untouched_flagged_shard() {
        // Big dirty overlay → re-sharding rewrites the touched shard. The other
        // shard is untouched by re-sharding but flagged for relocation; the
        // post-pass must rewrite it.
        let ctx = make_ctx(true, 1024 * 1024);
        let loader = RecordingLoader::new();
        let (current_levels, positions) = seed_sharded_cell(&loader, &[(1, 100), (200, 300)]);
        let pos_b = positions[1];

        // 65 overwrites in shard A's range → re-sharding rewrites A. Flag shard B.
        let dirty: Vec<(u64, Option<u64>)> = (1..=65u64).map(|k| (k, Some(k + 10_000))).collect();
        let cmd =
            flush_command_with_relocation(ctx.id(), build_dirty(&dirty), current_levels, [pos_b]);
        let (_orig, new_levels) =
            IndexFlusherThread::handle_command(&loader, &cmd, &ctx, None, None).unwrap();

        assert!(new_levels.is_sharded());
        assert_eq!(new_levels.l0(), None, "promote vacated L0");
        let new_a = new_levels.shards().get(&shard_key(1)).unwrap();
        let new_b = new_levels.shards().get(&shard_key(200)).unwrap();
        // Both shards moved: A via re-sharding, B via the post-pass.
        assert_ne!(new_a.position, positions[0]);
        assert_ne!(new_b.position, pos_b);
        assert_eq!(new_b.max_key, shard_key(300), "B bounds preserved");
    }

    #[test]
    fn dirty_unsharded_l0_write_relocates_stale_l1() {
        // Unsharded post-promote cell ([INVALID, L1]) with a small dirty
        // overlay → L0-write branch produces [new_L0, old_L1]. The old L1
        // is flagged; post-pass must rewrite it.
        let ctx = make_ctx(false, 1024 * 1024);
        let loader = RecordingLoader::new();
        let l1_pos = WalPosition::new(7_000, 64);
        let mut l1_table = IndexTable::default();
        l1_table.insert(
            Bytes::from(50u64.to_be_bytes().to_vec()),
            WalPosition::test_value(99),
        );
        loader.seed(l1_pos, l1_table);
        let current_levels = IndexLevels::promoted(l1_pos);

        let dirty: Vec<(u64, Option<u64>)> = (1..=5u64).map(|k| (k, Some(k + 1000))).collect();
        let cmd =
            flush_command_with_relocation(ctx.id(), build_dirty(&dirty), current_levels, [l1_pos]);
        let (_orig, new_levels) =
            IndexFlusherThread::handle_command(&loader, &cmd, &ctx, None, None).unwrap();

        assert!(!new_levels.is_sharded());
        assert!(new_levels.l0().is_some(), "fresh L0 written");
        let new_l1 = new_levels.l1().expect("L1 carried over");
        assert_ne!(new_l1, l1_pos, "stale L1 reflushed by post-pass");
    }

    #[test]
    fn resharding_repeated_promotes_stay_consistent() {
        // Two consecutive re-sharding promotes on the same cell. Each promote
        // dirties a different shard; both shards must end up with the
        // expected content after the second pass, and the untouched shard
        // from the second promote must still point at the blob the first
        // promote wrote (not the original seed).
        let ctx = make_ctx(true, 1024 * 1024);
        let loader = RecordingLoader::new();
        let (current_levels, _) = seed_sharded_cell(&loader, &[(1, 100), (200, 300)]);

        // Promote 1: 65 overwrites in shard A's range.
        let dirty1: Vec<(u64, Option<u64>)> = (1..=65u64).map(|k| (k, Some(k + 10_000))).collect();
        let cmd1 = flush_command(ctx.id(), build_dirty(&dirty1), current_levels);
        let (_orig, levels_after_1) =
            IndexFlusherThread::handle_command(&loader, &cmd1, &ctx, None, None).unwrap();
        assert!(levels_after_1.is_sharded());
        let pos_b_after_1 = levels_after_1
            .shards()
            .get(&shard_key(200))
            .unwrap()
            .position;

        // Promote 2: 65 overwrites in shard B's range. Shard A is now
        // "untouched" by this promote — its btree entry should carry the
        // position written in promote 1, not the original seed.
        let dirty2: Vec<(u64, Option<u64>)> =
            (200..=264u64).map(|k| (k, Some(k + 20_000))).collect();
        let cmd2 = flush_command(ctx.id(), build_dirty(&dirty2), levels_after_1.clone());
        let (_orig, levels_after_2) =
            IndexFlusherThread::handle_command(&loader, &cmd2, &ctx, None, None).unwrap();
        assert!(levels_after_2.is_sharded());
        assert_eq!(levels_after_2.shards().len(), 2);

        // Shard A's position carries through unchanged from promote 1.
        let a_after_2 = levels_after_2.shards().get(&shard_key(1)).unwrap();
        assert_eq!(
            a_after_2.position,
            levels_after_1.shards().get(&shard_key(1)).unwrap().position,
            "shard A untouched in promote 2 keeps promote-1 position",
        );
        // Shard B got rewritten, so its position changed.
        let b_after_2 = levels_after_2.shards().get(&shard_key(200)).unwrap();
        assert_ne!(b_after_2.position, pos_b_after_1);
        // Content check: shard B's blob contains keys 200..=300 (all
        // original plus the overwrites — same set).
        let b_keys = shard_entries(&loader, b_after_2.position);
        assert_eq!(b_keys.first(), Some(&200));
        assert_eq!(b_keys.last(), Some(&300));
        assert_eq!(b_keys.len(), 101);
    }
}
