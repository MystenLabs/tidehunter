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
    sender: mpsc::Sender<FlusherCommand>,
}

struct IndexFlusherThread {
    db: Weak<Db>,
    receiver: mpsc::Receiver<FlusherCommand>,
    metrics: Arc<Metrics>,
}

pub struct FlusherCommand {
    ks: KeySpace,
    cell: usize,
    flush_kind: FlushKind,
}

pub enum FlushKind {
    MergeUnloaded(WalPosition, Arc<IndexTable>),
    FlushLoaded(Arc<IndexTable>),
}

impl IndexFlusher {
    pub fn new(sender: mpsc::Sender<FlusherCommand>) -> Self {
        Self { sender }
    }

    pub fn start_thread(
        receiver: mpsc::Receiver<FlusherCommand>,
        db: Weak<Db>,
        metrics: Arc<Metrics>,
    ) -> JoinHandle<()> {
        let flusher_thread = IndexFlusherThread {
            db,
            receiver,
            metrics,
        };
        let jh = thread::Builder::new()
            .name("flusher".to_string())
            .spawn(move || flusher_thread.run())
            .unwrap();
        jh
    }

    pub fn request_flush(&self, ks: KeySpace, cell: usize, flush_kind: FlushKind) {
        let command = FlusherCommand {
            ks,
            cell,
            flush_kind,
        };
        self.sender
            .send(command)
            .expect("Flusher has stopped unexpectedly")
    }

    #[cfg(test)]
    pub fn new_unstarted_for_test() -> Self {
        let (sender, _receiver) = mpsc::channel();
        Self::new(sender)
    }
}

impl IndexFlusherThread {
    pub fn run(self) {
        // todo run compactor with flusher
        while let Ok(command) = self.receiver.recv() {
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
            };
            let ks = db.ks(command.ks);
            self.run_compactor(ks, &mut merged_index);
            let position = db
                .flush(command.ks, &merged_index)
                .expect("Failed to flush index");
            db.update_flushed_index(command.ks, command.cell, original_index, position);
            self.metrics
                .flush_time_mcs
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
