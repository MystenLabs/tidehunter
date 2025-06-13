use crate::cell::CellId;
use crate::db::Db;
use crate::index::index_table::IndexTable;
use crate::key_shape::{KeySpace, KeySpaceDesc};
use crate::large_table::Loader;
use crate::metrics::Metrics;
use crate::wal::WalPosition;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Weak;
use std::thread;
use std::thread::JoinHandle;
use std::time::Instant;

pub struct IndexFlusher {
    senders: Vec<mpsc::Sender<FlusherCommand>>,
    metrics: Arc<Metrics>,
}

struct IndexFlusherThread {
    db: Weak<Db>,
    receiver: mpsc::Receiver<FlusherCommand>,
    metrics: Arc<Metrics>,
    thread_id: usize,
}

pub struct FlusherCommand {
    ks: KeySpace,
    cell: CellId,
    flush_kind: FlushKind,
}

pub enum FlushKind {
    MergeUnloaded(WalPosition, Arc<IndexTable>),
    FlushLoaded(Arc<IndexTable>),
    #[cfg(test)]
    Barrier(#[allow(dead_code)] SendGuard),
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
                    .name(format!("flusher-{}", thread_id))
                    .spawn(move || flusher_thread.run())
                    .unwrap()
            })
            .collect()
    }

    pub fn request_flush(&self, ks: KeySpace, cell: CellId, flush_kind: FlushKind) {
        let thread_index = self.get_thread_for_cell(&cell);
        let command = FlusherCommand {
            ks,
            cell,
            flush_kind,
        };
        self.metrics.flush_pending.add(1);
        self.senders[thread_index]
            .send(command)
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

    /// Wait until all messages that are currently queued for flusher are processed
    #[cfg(test)]
    pub fn barrier(&self) {
        use parking_lot::Mutex;
        let mutex = Arc::new(Mutex::new(()));

        // Send a barrier command to each thread
        for (thread_id, sender) in self.senders.iter().enumerate() {
            let lock = mutex.lock_arc();
            let command = FlusherCommand {
                ks: KeySpace::new_test(0),
                cell: CellId::Integer(thread_id), // Use thread_id to ensure it goes to the right thread
                flush_kind: FlushKind::Barrier(SendGuard(lock)),
            };
            self.metrics.flush_pending.add(1);
            sender.send(command).unwrap();
        }

        // Wait for all threads to process their barriers
        let _ = mutex.lock();
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
            self.metrics.flush_pending.add(-1);
            let now = Instant::now();
            let Some(db) = self.db.upgrade() else {
                return;
            };
            let (original_index, mut merged_index) = match command.flush_kind {
                FlushKind::MergeUnloaded(position, dirty_index) => {
                    self.metrics
                        .unload
                        .with_label_values(&["merge_flush"])
                        .inc();
                    let mut disk_index = db
                        .load_index(command.ks, position)
                        .expect("Failed to load index in flusher thread");
                    disk_index.merge_dirty(&dirty_index);
                    (dirty_index, disk_index)
                }
                FlushKind::FlushLoaded(index) => {
                    self.metrics.unload.with_label_values(&["flush"]).inc();
                    // todo - no need to make copy if there is no compactor
                    let index_copy = IndexTable::clone(&index);
                    (index, index_copy)
                }
                #[cfg(test)]
                FlushKind::Barrier(_) => continue,
            };
            let ks = db.ks(command.ks);
            self.run_compactor(ks, &mut merged_index);
            let position = db
                .flush(command.ks, &merged_index)
                .expect("Failed to flush index");
            db.update_flushed_index(command.ks, command.cell, original_index, position);
            self.metrics
                .flush_time_mcs
                .with_label_values(&[&self.thread_id.to_string()])
                .inc_by(now.elapsed().as_micros() as u64);
        }
    }

    // todo - code duplicate with LargeTable::run_compactor
    // todo - result of compactor is not applied to in-memory index for DirtyLoaded
    fn run_compactor(&self, ks: &KeySpaceDesc, index: &mut IndexTable) {
        if let Some(compactor) = ks.compactor() {
            let pre_compact_len = index.len();
            compactor(&mut index.data);
            let compacted = pre_compact_len.saturating_sub(index.len());
            self.metrics
                .compacted_keys
                .with_label_values(&[ks.name()])
                .inc_by(compacted as u64);
        }
    }
}

#[cfg(test)]
pub(crate) struct SendGuard(
    #[allow(dead_code)] parking_lot::ArcMutexGuard<parking_lot::RawMutex, ()>,
);

#[cfg(test)]
// Rather than enable send_guard feature globally in parking_lot,
// using this just for flusher barrier
unsafe impl Send for SendGuard {}
