// See docs/snapshot_mechanism.md for an overview of how the flusher integrates
// with the two-pass snapshot mechanism.
use crate::cell::CellId;
use crate::context::KsContext;
use crate::db::Db;
use crate::index::index_table::IndexTable;
use crate::index::index_table::IndexWalPosition;
use crate::index::levels::IndexLevels;
use crate::key_shape::KeySpace;
use crate::large_table::Loader;
use crate::metrics::Metrics;
use crate::relocation::updates::RelocationUpdates;
use crate::wal::position::WalPosition;
use minibytes::Bytes;
use std::collections::{HashMap, HashSet};
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
    Flush {
        dirty: Arc<IndexTable>,
        current_levels: IndexLevels,
        loaded: bool,
    },
    /// Re-write a clean entry's on-disk blobs past `force_relocate_below`.
    ///
    /// The flusher collapses all populated levels into a single new blob
    /// (L0 wins on overlap). The output is always single-level; callers
    /// that care about level-specific rewriting must add a richer variant
    /// — today collapse-on-relocate trades a one-time WAF penalty (L1
    /// rewritten even when only L0 was below cutoff) for a simpler code
    /// path and a reduced level count afterwards.
    ForceRelocate(IndexLevels),
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
                    let is_relocation = matches!(command.flush_kind, FlushKind::ForceRelocate(_));
                    if let Some((original_index, new_levels, on_disk_keys, dropped_from_disk)) =
                        Self::handle_command(&*db, &command, ks_context, None, None)
                    {
                        if is_relocation {
                            db.update_relocated_index(command.ks, command.cell, new_levels);
                        } else {
                            db.update_flushed_index(
                                command.ks,
                                command.cell,
                                original_index,
                                new_levels,
                                on_disk_keys,
                                dropped_from_disk,
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

    #[allow(clippy::type_complexity)]
    pub(crate) fn handle_command<L: Loader>(
        loader: &L,
        command: &IndexFlushCommand,
        ctx: &KsContext,
        relocation_updates: Option<RelocationUpdates>,
        relocation_cutoff: Option<u64>,
    ) -> Option<(
        Arc<IndexTable>,
        IndexLevels,
        HashSet<Bytes>,
        std::collections::HashMap<Bytes, crate::index::index_table::IndexWalPosition>,
    )> {
        // Build the merged L0 candidate plus the existing L1 (if any).
        //
        // For `Flush`, `current_levels` describes the cell's pre-flush on-disk
        // state; we honour the sentinel (empty L0 slot after a prior promote)
        // so post-promote cells start a fresh L0 with just the overlay.
        //
        // `ForceRelocate` collapses L0 + L1 into a single blob so the output
        // is always single-level (`existing_l1 = None`). Downstream this means
        // the non-promote branch will `clean_self` (since no L1 sits below)
        // and emit `IndexLevels::single(new_pos)`.
        // `existing_disk_entries` is a snapshot of keys/positions present on
        // disk *before* this flush merges in `dirty`. We need it to detect
        // entries that the compactor or `clean_self` are about to drop from
        // disk that did not appear in `dirty` — those need to be folded back
        // into the in-memory entry on flush completion. Without this, a
        // version that was on disk under a previous flush can vanish from
        // both memory and disk when a higher version arrives via this flush
        // (the dirty-overlay-loss bug, second-order form). We capture both
        // existing L0 and L1 because compactor/clean_self can collapse
        // entries from either level.
        let mut existing_disk_entries: HashMap<Bytes, IndexWalPosition> = HashMap::new();
        let (original_index, mut merged_l0, existing_l1) = match &command.flush_kind {
            FlushKind::Flush {
                dirty,
                current_levels,
                loaded,
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
                    // Capture existing L0 keys+positions BEFORE merging dirty.
                    // We use these to detect compactor/clean_self drops below.
                    for (k, iwp) in base.iter_with_positions() {
                        existing_disk_entries.insert(k, iwp);
                    }
                    base.merge_dirty_and_clean(dirty);
                    base
                };
                // Also capture existing L1 entries — they participate in the
                // promote branch's merge and can be dropped by compactor /
                // clean_self the same way L0 entries can.
                if let Some(l1_pos) = current_levels.l1() {
                    let l1 = loader
                        .load(&ctx.ks_config, l1_pos)
                        .expect("Failed to load L1 index in flusher thread");
                    for (k, iwp) in l1.iter_with_positions() {
                        // L0 wins over L1 on overlap; don't overwrite a
                        // captured L0 position with a (necessarily older) L1
                        // value at the same key.
                        existing_disk_entries.entry(k).or_insert(iwp);
                    }
                }
                (dirty.clone(), merged, current_levels.l1())
            }
            FlushKind::ForceRelocate(levels) => {
                // Collapse L0 + L1 into a single blob. Start from L1 (the
                // deeper level) and merge L0 on top so L0 wins on overlap,
                // matching the read-path precedence. The result is handed
                // downstream as `merged_l0` with `existing_l1 = None`, so
                // the non-promote branch writes it as a single level and
                // strips tombstones (since nothing sits below it).
                //
                // Force-relocate runs on clean entries only (see
                // `request_async_snapshot_flush`), so there is no dirty
                // overlay to worry about here.
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
                (arc_index.clone(), IndexTable::clone(&arc_index), None)
            }
        };

        // `RelocationUpdates::apply` uses compare-and-set semantics — it can
        // only rewrite positions for keys already in `merged_l0`. Keys that
        // live only in L1 (the common case for post-promote `[INVALID, L1]`
        // cells) would be silently dropped. Pre-load L1 into `merged_l0` so
        // apply sees the full key set. The subsequent promote branch still
        // re-loads L1 from disk; the duplication is acceptable on the
        // relocation path — correctness first, optimize later.
        if relocation_updates.is_some() && existing_l1.is_some() {
            let mut combined = Self::load_l1_or_default(loader, ctx, existing_l1);
            // `merged_l0` is the fresher overlay (L0 + dirty) — it wins on
            // overlapping keys, so treat it as the "dirty" side.
            combined.merge_dirty_and_clean(&merged_l0);
            merged_l0 = combined;
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
        let is_relocation = matches!(&command.flush_kind, FlushKind::ForceRelocate(_));
        let l0_threshold = ctx.l0_max_entries();
        let over_threshold = !is_relocation && merged_l0.len() > l0_threshold;

        // `on_disk_keys` is the set of keys whose data actually survived
        // the compactor + clean_self stages and was written to the new
        // on-disk levels. Returned to `update_flushed_index` so the
        // in-memory `retain_unprocessed_or_not_on_disk` only drops flat
        // entries that are actually safe-on-disk. Without this,
        // entries dropped by compactor or clean_self can be lost from
        // both memory and disk in the same flush completion (the
        // dirty-overlay-loss bug, first-order form).
        let (new_levels, on_disk_keys): (IndexLevels, HashSet<Bytes>) = if over_threshold {
            // Promote: merge the freshly-built L0 with the existing L1 (if
            // any). `merge_dirty_and_clean` lets `merged_l0` win on overlap
            // by treating it as the "dirty" overlay.
            let mut new_l1 = Self::load_l1_or_default(loader, ctx, existing_l1);
            new_l1.merge_dirty_and_clean(&merged_l0);
            Self::run_compactor(ctx, &mut new_l1);
            // L1 is the deepest level — nothing below to shadow, so drop
            // tombstones (and the keys they shadow) before writing.
            new_l1.clean_self();
            let on_disk_keys = new_l1.key_set();
            let new_l1_pos = loader
                .flush(command.ks, &new_l1)
                .expect("Failed to flush index");
            ctx.metrics
                .promote_total
                .with_label_values(&[ctx.name()])
                .inc();
            ctx.metrics
                .l1_bytes_written
                .with_label_values(&[ctx.name()])
                .inc_by(new_l1_pos.frame_len() as u64);
            (IndexLevels::promoted(new_l1_pos), on_disk_keys)
        } else {
            Self::run_compactor(ctx, &mut merged_l0);
            // If no L1 exists below, this L0 is the deepest level — strip
            // tombstones. Otherwise leave them in so they shadow L1.
            if existing_l1.is_none() {
                merged_l0.clean_self();
            }
            let on_disk_keys = merged_l0.key_set();
            let new_l0_pos = loader
                .flush(command.ks, &merged_l0)
                .expect("Failed to flush index");
            if !is_relocation {
                ctx.metrics
                    .l0_bytes_written
                    .with_label_values(&[ctx.name()])
                    .inc_by(new_l0_pos.frame_len() as u64);
            }
            let mut levels = IndexLevels::single(new_l0_pos);
            if let Some(l1) = existing_l1 {
                levels.set(1, l1);
            }
            (levels, on_disk_keys)
        };

        // `dropped_from_disk`: keys that were on disk in `existing_l0` BEFORE
        // this flush but did NOT make it into the new on-disk levels. These
        // get folded back into the in-memory entry by update_flushed_index
        // so reads can still find them. Otherwise an old version that was
        // safely on disk vanishes when a higher version arrives via this
        // flush and the compactor + clean_self collapse the merge to that
        // higher version (dirty-overlay-loss bug, second-order form).
        let mut dropped_from_disk: HashMap<Bytes, IndexWalPosition> = HashMap::new();
        for (k, iwp) in existing_disk_entries {
            if !on_disk_keys.contains(&k) {
                dropped_from_disk.insert(k, iwp);
            }
        }
        Some((original_index, new_levels, on_disk_keys, dropped_from_disk))
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
}

pub struct SendGuard(#[allow(dead_code)] parking_lot::ArcMutexGuard<parking_lot::RawMutex, ()>);

// Rather than enable send_guard feature globally in parking_lot,
// using this just for flusher barrier
unsafe impl Send for SendGuard {}
