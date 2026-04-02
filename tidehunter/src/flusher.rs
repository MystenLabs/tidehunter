// See docs/snapshot_mechanism.md for an overview of how the flusher integrates
// with the two-pass snapshot mechanism.
use crate::cell::CellId;
use crate::context::KsContext;
use crate::db::Db;
use crate::index::index_table::IndexTable;
use crate::key_shape::KeySpace;
use crate::large_table::Loader;
use crate::metrics::Metrics;
use crate::relocation::updates::RelocationUpdates;
use crate::wal::position::WalPosition;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::Ordering;
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
    MergeUnloaded(WalPosition, Arc<IndexTable>),
    FlushLoaded(Arc<IndexTable>),
    ForceRelocate(WalPosition),
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
        let count = self
            .metrics
            .flush_pending_count
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        self.metrics.flush_pending.set(count as i64);
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
            let count = self
                .metrics
                .flush_pending_count
                .fetch_add(1, Ordering::Relaxed)
                + 1;
            self.metrics.flush_pending.set(count as i64);
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
                    let count = self
                        .metrics
                        .flush_pending_count
                        .fetch_sub(1, Ordering::Relaxed)
                        - 1;
                    self.metrics.flush_pending.set(count as i64);
                    // Dropping _guard releases the mutex, allowing the barrier() caller to proceed
                }
                FlusherCommand::Command(command) => {
                    let count = self
                        .metrics
                        .flush_pending_count
                        .fetch_sub(1, Ordering::Relaxed)
                        - 1;
                    self.metrics.flush_pending.set(count as i64);
                    let now = Instant::now();
                    let Some(db) = self.db.upgrade() else {
                        return;
                    };

                    let ks_context = db.ks_context(command.ks);
                    let is_relocation = matches!(command.flush_kind, FlushKind::ForceRelocate(_));
                    if let Some((original_index, position)) =
                        Self::handle_command(&*db, &command, ks_context, None, None)
                    {
                        if is_relocation {
                            db.update_relocated_index(command.ks, command.cell, position);
                        } else {
                            db.update_flushed_index(
                                command.ks,
                                command.cell,
                                original_index,
                                position,
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
    ) -> Option<(Arc<IndexTable>, WalPosition)> {
        let (original_index, mut merged_index) = match &command.flush_kind {
            FlushKind::MergeUnloaded(position, dirty_index) => {
                ctx.metrics.unload.with_label_values(&["merge_flush"]).inc();
                let mut disk_index = loader
                    .load(&ctx.ks_config, *position)
                    .expect("Failed to load index in flusher thread");
                disk_index.merge_dirty_and_clean(dirty_index);
                (dirty_index.clone(), disk_index)
            }
            FlushKind::FlushLoaded(index) => {
                ctx.metrics.unload.with_label_values(&["flush"]).inc();
                // todo - no need to make copy if there is no compactor
                let mut index_copy = IndexTable::clone(index);
                index_copy.clean_self();
                (index.clone(), index_copy)
            }
            FlushKind::ForceRelocate(position) => {
                ctx.metrics
                    .unload
                    .with_label_values(&["force_relocate"])
                    .inc();
                let disk_index = loader
                    .load(&ctx.ks_config, *position)
                    .expect("Failed to load index for forced relocation");
                let arc_index = Arc::new(disk_index);
                (arc_index.clone(), IndexTable::clone(&arc_index))
            }
        };

        if let Some(relocation_updates) = relocation_updates {
            relocation_updates.apply(&mut merged_index);
        }

        match relocation_cutoff {
            Some(cutoff) => {
                let length = merged_index.len();
                merged_index.retain_above_position(cutoff);
                if merged_index.len() == length {
                    return None;
                }
            }
            // TODO: Used only if relocation doesn't call sync flush. Remove if no such implementation is needed anymore
            None => merged_index.retain_above_position(loader.min_wal_position()),
        }
        Self::run_compactor(ctx, &mut merged_index);

        // Always flush everything to disk to avoid data loss
        // The filtering will happen during unmerge_flushed
        let position = loader
            .flush(command.ks, &merged_index)
            .expect("Failed to flush index");

        Some((original_index, position))
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
