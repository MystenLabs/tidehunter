use crate::db::{Db, DbResult, WalEntry};
use crate::key_shape::KeySpaceDesc;
use crate::large_table::GetResult;
use crate::metrics::Metrics;
use crate::wal::WalError;
use crate::WalPosition;
use std::sync::{mpsc, Arc, Weak};
use std::thread::JoinHandle;
mod watermark;
use crate::index::index_format::IndexFormat;
pub use watermark::RelocationWatermarks;

pub(crate) struct Relocator(pub(crate) mpsc::Sender<RelocationCommand>);

pub enum RelocationCommand {
    Start,
    Cancel(mpsc::Sender<()>),
    #[cfg(test)]
    StartBlocking(mpsc::Sender<()>),
}

pub enum Decision {
    Keep,
    Remove,
    StopRelocation,
}

pub trait RelocationFilter: Fn(&[u8], &[u8]) -> Decision + Send + Sync + 'static {}
impl<F> RelocationFilter for F where F: Fn(&[u8], &[u8]) -> Decision + Send + Sync + 'static {}

pub(crate) struct RelocationDriver {
    db: Weak<Db>,
    receiver: mpsc::Receiver<RelocationCommand>,
    metrics: Arc<Metrics>,
    watermarks: RelocationWatermarks,
}

impl RelocationDriver {
    const NUM_ITERATIONS_IN_BATCH: usize = 1000;

    pub fn start(
        db: Weak<Db>,
        watermarks: RelocationWatermarks,
        receiver: mpsc::Receiver<RelocationCommand>,
        metrics: Arc<Metrics>,
    ) -> JoinHandle<()> {
        let driver = Self {
            db,
            receiver,
            metrics,
            watermarks,
        };
        std::thread::Builder::new()
            .name("relocator".to_string())
            .spawn(move || driver.run())
            .unwrap()
    }

    pub fn run(mut self) {
        while let Ok(command) = self.receiver.recv() {
            match command {
                RelocationCommand::Start => {
                    // TODO: better error handling and retries
                    self.relocation_run().expect("relocation error");
                }
                RelocationCommand::Cancel(callback) => {
                    callback.send(()).expect("failed to send ");
                }
                #[cfg(test)]
                RelocationCommand::StartBlocking(cb) => {
                    self.relocation_run().unwrap();
                    cb.send(()).unwrap()
                }
            }
        }
    }

    fn relocation_run(&mut self) -> DbResult<()> {
        let Some(db) = self.db.upgrade() else {
            return Ok(());
        };
        // TODO: handle potentially uninitialized positions at the end of the WAL
        let upper_limit = db.wal_writer.position();
        let mut wal_iterator = db
            .wal
            .wal_iterator(self.watermarks.get_relocation_start_position())?;

        for i in 0..usize::MAX {
            if i % Self::NUM_ITERATIONS_IN_BATCH == 0 && self.should_cancel_relocation() {
                break;
            }
            let entry = wal_iterator.next();
            if matches!(entry, Err(WalError::Crc(_))) {
                break;
            }
            let (position, raw_entry) = entry?;
            if position.offset() >= upper_limit {
                break;
            }
            self.watermarks.update(position);
            match WalEntry::from_bytes(raw_entry) {
                WalEntry::Record(ks, key, value) => {
                    let ksd = db.key_shape.ks(ks);
                    match self.should_keep_entry(&db, ksd, &key, &value, position)? {
                        Decision::StopRelocation => break,
                        Decision::Remove => {
                            // TODO: handle LRU entries
                            self.metrics
                                .relocation_removed
                                .with_label_values(&[ksd.name()])
                                .inc();
                            continue;
                        }
                        Decision::Keep => {
                            self.metrics
                                .relocation_kept
                                .with_label_values(&[ksd.name()])
                                .inc();
                            // TODO: handle potential races with concurrent writes to the same key
                            db.insert(ks, key, value)?
                        }
                    }
                }
                WalEntry::Index(ks, bytes) => {
                    let ksd = db.key_shape.ks(ks);
                    let index = ksd.index_format().deserialize_index(ksd, bytes);
                    if let Some((key, _)) = index.next_entry(None, false) {
                        if let Some(last_pos) = db.large_table.get_index_position(ksd, &key) {
                            if last_pos == position {
                                // For now, we stop the relocation process if we encounter an index entry
                                // that has not been overwritten yet.
                                // TODO: Add support for relocating index entries.
                                break;
                            }
                        }
                    }
                }
                WalEntry::Remove(..) | WalEntry::BatchStart(..) => {}
            }
        }
        self.watermarks.save()?;
        self.metrics
            .relocation_position
            .set(self.watermarks.get_progress_watermark() as i64);
        Ok(())
    }

    fn should_keep_entry(
        &self,
        db: &Arc<Db>,
        ks: &KeySpaceDesc,
        key: &[u8],
        value: &[u8],
        position: WalPosition,
    ) -> DbResult<Decision> {
        if let Some(filter) = ks.relocation_filter() {
            return Ok(filter(key, value));
        }
        let reduced_key = ks.reduce_key(key);
        let decision = match db.large_table.get(ks, &reduced_key, db.as_ref(), true)? {
            GetResult::NotFound => Decision::Remove,
            GetResult::Value(..) => unreachable!("getter was called with skip cache"),
            GetResult::WalPosition(last_pos) => {
                if last_pos.offset() > position.offset() {
                    Decision::Remove
                } else {
                    Decision::Keep
                }
            }
        };
        Ok(decision)
    }

    fn should_cancel_relocation(&self) -> bool {
        loop {
            match self.receiver.try_recv() {
                // consume and ignore all Start commands while relocation is in progress
                Ok(RelocationCommand::Start) => {}
                Ok(RelocationCommand::Cancel(cb)) => {
                    cb.send(())
                        .expect("Failed to send cancel relocation command");
                    return true;
                }
                Err(mpsc::TryRecvError::Empty) => return false,
                Err(mpsc::TryRecvError::Disconnected) => return true,
                #[cfg(test)]
                Ok(RelocationCommand::StartBlocking(cb)) => cb.send(()).unwrap(),
            }
        }
    }
}
