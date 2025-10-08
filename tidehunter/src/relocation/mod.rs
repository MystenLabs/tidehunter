use crate::batch::RelocatedWriteBatch;
use crate::db::{Db, DbResult, WalEntry};
use crate::index::index_table::IndexTable;
use crate::key_shape::KeySpace;
use crate::large_table::Loader;
use crate::metrics::Metrics;
use crate::relocation::watermark::RelocationWatermarks;
use crate::wal::WalError;
use crate::WalPosition;
pub use cell_reference::CellReference;
use minibytes::Bytes;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Weak};
use std::thread::JoinHandle;

mod cell_reference;
mod watermark;

#[cfg(test)]
mod relocation_tests;
pub mod updates;

pub(crate) struct Relocator(pub(crate) mpsc::Sender<RelocationCommand>);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RelocationStrategy {
    /// WAL-based sequential relocation
    #[default]
    WalBased,
    /// Index-based relocation that processes entire cells atomically
    IndexBased,
}

pub enum RelocationCommand {
    Start(RelocationStrategy),
    Cancel(mpsc::Sender<()>),
    #[cfg(test)]
    StartBlocking(RelocationStrategy, mpsc::Sender<()>),
}

pub enum Decision {
    Keep,
    Remove,
    StopRelocation,
}

pub trait RelocationFilter: Fn(&[u8], &[u8]) -> Decision + Send + Sync + 'static {}
impl<F> RelocationFilter for F where F: Fn(&[u8], &[u8]) -> Decision + Send + Sync + 'static {}

struct CellProcessingContext {
    batch: RelocatedWriteBatch,
    highest_wal_position: WalPosition,
    entries_removed: u64,
    entries_kept: u64,
}

impl CellProcessingContext {
    fn new(batch: RelocatedWriteBatch) -> Self {
        Self {
            batch,
            highest_wal_position: WalPosition::new(0, 0),
            entries_removed: 0,
            entries_kept: 0,
        }
    }

    fn add_entry_to_relocate(&mut self, key: Bytes, value: Bytes, position: WalPosition) {
        self.batch.write(key, value);
        self.entries_kept += 1;
        if position.offset() > self.highest_wal_position.offset() {
            self.highest_wal_position = position;
        }
    }

    fn mark_entry_removed(&mut self, position: WalPosition) {
        self.entries_removed += 1;
        if position.offset() > self.highest_wal_position.offset() {
            self.highest_wal_position = position;
        }
    }
}

pub(crate) struct RelocationDriver {
    db: Weak<Db>,
    path: PathBuf,
    receiver: mpsc::Receiver<RelocationCommand>,
    metrics: Arc<Metrics>,
}

impl RelocationDriver {
    const NUM_ITERATIONS_IN_BATCH: usize = 1000;
    const NUM_ITERATIONS_TILL_SAVE: usize = 100000;

    pub fn start(
        db: Weak<Db>,
        path: PathBuf,
        receiver: mpsc::Receiver<RelocationCommand>,
        metrics: Arc<Metrics>,
    ) -> JoinHandle<()> {
        let driver = Self {
            db,
            path,
            receiver,
            metrics,
        };
        std::thread::Builder::new()
            .name("relocator".to_string())
            .spawn(move || driver.run())
            .unwrap()
    }

    pub fn run(mut self) {
        while let Ok(command) = self.receiver.recv() {
            match command {
                RelocationCommand::Start(strategy) => {
                    // TODO: better error handling and retries
                    self.relocation_run(strategy).expect("relocation error");
                }
                RelocationCommand::Cancel(callback) => {
                    callback.send(()).expect("failed to send ");
                }
                #[cfg(test)]
                RelocationCommand::StartBlocking(strategy, cb) => {
                    self.relocation_run(strategy).unwrap();
                    cb.send(()).unwrap()
                }
            }
        }
    }

    fn save_progress(
        &mut self,
        db: &Db,
        watermarks: &RelocationWatermarks,
        watermark_only: bool,
    ) -> DbResult<()> {
        watermarks.save()?;
        if watermark_only {
            return Ok(());
        }

        let gc_watermark = std::cmp::min(
            watermarks.gc_watermark(),
            db.control_region_store.lock().last_position(),
        );

        db.wal_writer.gc(gc_watermark)?;
        Ok(())
    }

    fn relocation_run(&mut self, strategy: RelocationStrategy) -> DbResult<()> {
        let Some(db) = self.db.upgrade() else {
            return Ok(());
        };
        match strategy {
            RelocationStrategy::WalBased => self.wal_based_relocation(db),
            RelocationStrategy::IndexBased => self.index_based_relocation(db),
        }
    }

    fn index_based_relocation(&mut self, db: Arc<Db>) -> DbResult<()> {
        let mut watermarks = RelocationWatermarks::read_or_create(&self.path)?;
        // Capture the upper WAL limit to avoid race conditions
        // Only process entries written before this point. This is the last position that was written
        // and made its way into the large table
        let upper_limit = db.wal_writer.last_processed();

        // Get starting cell reference from saved progress
        let mut current_cell_ref = match &watermarks.data.next_to_process {
            Some(cr) => Some(cr.clone()), // Resume from saved position
            None => CellReference::first(&db, KeySpace::first()), // Starting from beginning
        };

        let mut cells_processed = 0;
        let mut highest_wal_position = 0u64;
        let mut current_ks_id = None;

        while let Some(cell_ref) = current_cell_ref.take() {
            // Check for cancellation periodically
            if cells_processed % Self::NUM_ITERATIONS_IN_BATCH == 0 {
                if self.should_cancel_relocation() {
                    break;
                }
                // Save progress periodically
                if cells_processed % Self::NUM_ITERATIONS_TILL_SAVE == 0 {
                    watermarks.set(Some(cell_ref.clone()), highest_wal_position, upper_limit);
                    self.save_progress(&db, &watermarks, true)?;
                    // Save watermark only
                }
            }

            // Update current keyspace metric when it changes
            let ks_id = cell_ref.keyspace.as_usize();
            if current_ks_id != Some(ks_id) {
                current_ks_id = Some(ks_id);
                self.metrics.relocation_current_keyspace.set(ks_id as i64);
            }

            // Process each cell
            let context = self.process_single_cell(&cell_ref, &db, upper_limit)?;

            // Track the highest WAL position seen
            if context.highest_wal_position.offset() > highest_wal_position {
                highest_wal_position = context.highest_wal_position.offset();
            }

            // Relocate entries if any were marked for keeping
            let keyspace_desc = &db.ks_context(cell_ref.keyspace).ks_config;
            if !context.batch.is_empty() {
                let successful = self.relocate_entries(context.batch, &db)?;
                // Track successful relocations with existing metrics (same as WAL-based)
                self.metrics
                    .relocation_kept
                    .with_label_values(&[keyspace_desc.name()])
                    .inc_by(successful);
            }

            // Track cells processed
            self.metrics
                .relocation_cells_processed
                .with_label_values(&[keyspace_desc.name()])
                .inc();

            cells_processed += 1;

            // Get next cell reference
            current_cell_ref = cell_ref.next(&db);
        }

        // Save final progress with upper_limit and highest WAL position
        watermarks.set(current_cell_ref.clone(), highest_wal_position, upper_limit);
        self.save_progress(&db, &watermarks, false)?;
        Ok(())
    }

    fn wal_based_relocation(&mut self, db: Arc<Db>) -> DbResult<()> {
        let upper_limit = db.wal_writer.last_processed();
        let mut wal_iterator = db.wal.wal_iterator(db.wal.min_wal_position())?;
        let mut target_position = None;
        // find target cut-off position
        loop {
            let entry = wal_iterator.next();
            if matches!(entry, Err(WalError::Crc(_))) {
                break;
            }
            let (position, raw_entry) = entry?;
            if position.offset() >= upper_limit {
                break;
            }
            if let WalEntry::Record(ks, key, value, _relocated) = WalEntry::from_bytes(raw_entry) {
                let ksd = db.key_shape.ks(ks);
                if let Some(filter) = ksd.relocation_filter() {
                    if let Decision::StopRelocation = filter(&key, &value) {
                        target_position = Some(position);
                        break;
                    }
                }
            }
        }
        let Some(target_position) = target_position else {
            return Ok(());
        };
        self.metrics
            .relocation_target_position
            .set(target_position.offset() as i64);
        // ensure the target position is big enough to cut
        if target_position.offset() < (db.wal.wal_file_size() + db.wal.min_wal_position()) {
            return Ok(());
        }
        for ks in db.key_shape.iter_ks() {
            // no need to relocate. All entries with position < target_position are stale
            if ks.relocation_filter().is_some() {
                continue;
            }
            let mut current_cell = CellReference::first(&db, ks.id());
            while let Some(cell) = current_cell.take() {
                current_cell = cell.next_in_ks(&db);
                let mut batch = RelocatedWriteBatch::new(
                    cell.keyspace,
                    cell.cell_id.clone(),
                    target_position.offset(),
                );
                let index = db
                    .large_table
                    .get_cell_index(db.ks_context(ks.id()), &cell.cell_id, db.as_ref())?
                    .unwrap_or(IndexTable::default().into());
                for (_reduced_key, position) in index.iter() {
                    if position.offset() < target_position.offset() {
                        if let Some((key, value)) = db.read_record(position)? {
                            batch.write(key, value);
                            self.metrics
                                .relocation_kept
                                .with_label_values(&[ks.name()])
                                .inc();
                        }
                    }
                }
                db.write_relocated_batch(batch)?;
            }
        }
        db.rebuild_control_region()?;
        db.wal_writer.gc(std::cmp::min(
            target_position.offset(),
            db.control_region_store.lock().last_position(),
        ))?;
        Ok(())
    }

    fn should_cancel_relocation(&self) -> bool {
        loop {
            match self.receiver.try_recv() {
                // consume and ignore all Start commands while relocation is in progress
                Ok(RelocationCommand::Start(_)) => {}
                Ok(RelocationCommand::Cancel(cb)) => {
                    cb.send(())
                        .expect("Failed to send cancel relocation command");
                    return true;
                }
                Err(mpsc::TryRecvError::Empty) => return false,
                Err(mpsc::TryRecvError::Disconnected) => return true,
                #[cfg(test)]
                Ok(RelocationCommand::StartBlocking(_, cb)) => cb.send(()).unwrap(),
            }
        }
    }

    /// Process a single cell: collect keys, read values from WAL, make decisions
    fn process_single_cell(
        &self,
        cell_ref: &CellReference,
        db: &Arc<Db>,
        upper_limit: u64,
    ) -> DbResult<CellProcessingContext> {
        let batch = RelocatedWriteBatch::new(
            cell_ref.keyspace,
            cell_ref.cell_id.clone(),
            db.last_processed_wal_position(),
        );
        let mut context = CellProcessingContext::new(batch);
        let mut removed_count = 0;

        // Phase A: Get shared reference to cell index
        let index = match db.large_table.get_cell_index(
            db.ks_context(cell_ref.keyspace),
            &cell_ref.cell_id,
            db.as_ref(),
        )? {
            Some(index) => index,
            None => {
                // Cell doesn't exist or is empty
                return Ok(context);
            }
        };

        // Phase B: Read values from WAL and make decisions (no lock held, efficient iteration)
        // TODO(#74): Optimization needed - add support for making relocation decisions without loading values
        // This would be beneficial in two scenarios:
        // 1. Applications without pruner callbacks - relocation just moves non-deleted entries
        // 2. Applications that can make decisions based on key only, without needing the value
        //
        // This strategy is most useful for applications that don't need values for decision making.
        // For applications like Sui that require values (similar to current behavior),
        // WAL-based relocation remains the better choice.
        //
        // Implementation could involve:
        // - Optional key-only decision callback in RelocationFilter trait
        // - Skip WAL value reads when key-only decisions are possible
        // - Fall back to current value-based approach when needed
        let keyspace_desc = &db.ks_context(cell_ref.keyspace).ks_config;
        for (key, position) in index.iter() {
            if position.offset() >= upper_limit {
                continue;
            }

            // Read the actual value from WAL
            let value = match db.read_record(position)? {
                Some((_, val)) => val,
                None => {
                    // Entry might have been deleted or corrupted, skip it
                    context.mark_entry_removed(position);
                    removed_count += 1;
                    continue;
                }
            };

            // Simplified decision logic for index-based relocation. Since we're iterating through
            // current index entries, we only need to check the relocation filter
            let decision = keyspace_desc
                .relocation_filter()
                .map_or(Decision::Keep, |filter| filter(key, &value));

            match decision {
                Decision::Keep => {
                    context.add_entry_to_relocate(key.clone(), value, position);
                }
                Decision::Remove => {
                    context.mark_entry_removed(position);
                    removed_count += 1;
                }
                Decision::StopRelocation => {
                    break;
                }
            }
        }

        // Track removed entries with existing metrics (same as WAL-based)
        if removed_count > 0 {
            self.metrics
                .relocation_removed
                .with_label_values(&[keyspace_desc.name()])
                .inc_by(removed_count);
        }

        Ok(context)
    }

    /// Relocate entries following the same pattern as WAL-based relocation
    fn relocate_entries(&self, batch: RelocatedWriteBatch, db: &Arc<Db>) -> DbResult<u64> {
        let successful_inserts = batch.len() as u64;

        db.write_relocated_batch(batch)?;

        Ok(successful_inserts)
    }
}
