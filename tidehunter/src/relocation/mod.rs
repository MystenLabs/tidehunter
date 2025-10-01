use crate::large_table::{GetResult, LargeTable};
use crate::metrics::Metrics;
use crate::wal::WalError;
use crate::WalPosition;
use crate::{
    db::{Db, DbResult, WalEntry},
    relocation::watermark::IndexWatermarkData,
};
use crate::{
    key_shape::{KeySpace, KeySpaceDesc},
    relocation::watermark::WalWatermarkData,
};
use bloom::{BloomFilter, ASMS};
use minibytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{mpsc, Arc, Weak};
use std::thread::JoinHandle;

mod cell_reference;
mod watermark;

#[cfg(test)]
mod relocation_tests;

pub use cell_reference::CellReference;
pub use watermark::RelocationWatermarks;
pub(crate) use watermark::WatermarkData;

pub(crate) struct Relocator(pub(crate) mpsc::Sender<RelocationCommand>);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelocationStrategy {
    /// WAL-based sequential relocation
    WalBased,
    /// Index-based relocation that processes entire cells atomically
    IndexBased,
}

impl Default for RelocationStrategy {
    fn default() -> Self {
        Self::WalBased // Default to existing behavior for backward compatibility
    }
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

#[derive(Debug)]
struct CellProcessingContext {
    entries_to_relocate: Vec<(Bytes, Bytes, WalPosition)>, // key, value, original position
    highest_wal_position: WalPosition,
    entries_removed: u64,
    entries_kept: u64,
}

impl CellProcessingContext {
    fn new() -> Self {
        Self {
            entries_to_relocate: Vec::new(),
            highest_wal_position: WalPosition::new(0, 0),
            entries_removed: 0,
            entries_kept: 0,
        }
    }

    fn add_entry_to_relocate(&mut self, key: Bytes, value: Bytes, position: WalPosition) {
        self.entries_to_relocate.push((key, value, position));
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
    receiver: mpsc::Receiver<RelocationCommand>,
    metrics: Arc<Metrics>,
    watermarks: RelocationWatermarks,
}

impl RelocationDriver {
    const NUM_ITERATIONS_IN_BATCH: usize = 1000;
    const NUM_ITERATIONS_TILL_SAVE: usize = 100000;

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

    fn save_progress(&mut self, db: &Db, watermark_only: bool) -> DbResult<()> {
        self.watermarks.save(&self.metrics)?;
        if watermark_only {
            return Ok(());
        }

        let gc_watermark = std::cmp::min(
            self.watermarks.gc_watermark(),
            db.control_region_store.lock().last_position(),
        );

        db.wal_writer.gc(gc_watermark)?;
        Ok(())
    }

    fn relocation_run(&mut self, strategy: RelocationStrategy) -> DbResult<()> {
        let Some(db) = self.db.upgrade() else {
            return Ok(());
        };

        // Validate that watermark strategy matches commanded strategy
        let watermark_strategy = match &self.watermarks.data {
            WatermarkData::WalBased { .. } => RelocationStrategy::WalBased,
            WatermarkData::IndexBased { .. } => RelocationStrategy::IndexBased,
        };

        if watermark_strategy != strategy {
            panic!(
                "Relocation strategy mismatch: loaded watermarks are {:?} but commanded strategy is {:?}. \
                Please delete the watermark file (rel) to start fresh with the new strategy.",
                watermark_strategy, strategy
            );
        }

        match strategy {
            RelocationStrategy::WalBased => {
                let WatermarkData::WalBased(watermark_data) = &self.watermarks.data else {
                    unreachable!("Strategy mismatch should be caught above");
                };
                let watermark_data = watermark_data.clone();
                self.wal_based_relocation(db, &watermark_data)
            }
            RelocationStrategy::IndexBased => {
                let WatermarkData::IndexBased(watermark_data) = &self.watermarks.data else {
                    unreachable!("Strategy mismatch should be caught above");
                };
                let watermark_data = watermark_data.clone();
                self.index_based_relocation(db, &watermark_data)
            }
        }
    }

    fn index_based_relocation(
        &mut self,
        db: Arc<Db>,
        watermark_data: &IndexWatermarkData,
    ) -> DbResult<()> {
        // Capture the upper WAL limit to avoid race conditions
        // Only process entries written before this point. This is the last position that was written
        // and made its way into the large table
        let upper_limit = db.wal_writer.last_processed();

        // Get starting cell reference from saved progress
        let mut current_cell_ref = match &watermark_data.next_to_process {
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
                    self.watermarks.data = WatermarkData::IndexBased(IndexWatermarkData {
                        next_to_process: Some(cell_ref.clone()),
                        highest_wal_position,
                        upper_limit,
                    });
                    self.save_progress(&db, true)?;
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
            if !context.entries_to_relocate.is_empty() {
                let successful =
                    self.relocate_entries(context.entries_to_relocate, cell_ref.keyspace, &db)?;
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
        self.watermarks.data = WatermarkData::IndexBased(IndexWatermarkData {
            next_to_process: current_cell_ref.clone(),
            highest_wal_position,
            upper_limit,
        });
        self.save_progress(&db, false)?;

        Ok(())
    }

    fn wal_based_relocation(
        &mut self,
        db: Arc<Db>,
        watermark_data: &WalWatermarkData,
    ) -> DbResult<()> {
        // TODO: handle potentially uninitialized positions at the end of the WAL
        let upper_limit = db.wal_writer.last_processed();

        // Get starting position from saved progress
        let start_position = watermark_data.progress;
        let mut wal_iterator = db.wal.wal_iterator(start_position)?;

        // Skip the first entry if we're resuming from a saved position
        if start_position > 0 {
            match wal_iterator.next() {
                Ok(_) => {}                 // Successfully skipped
                Err(WalError::Crc(_)) => {} // End of WAL, that's fine
                Err(e) => return Err(e.into()),
            }
        }

        let bloom_filters = db.large_table.build_index_bloom_filters(db.as_ref())?;

        for i in 0..usize::MAX {
            if i % Self::NUM_ITERATIONS_IN_BATCH == 0 {
                if self.should_cancel_relocation() {
                    break;
                }
                if i % Self::NUM_ITERATIONS_TILL_SAVE == 0 {
                    let WatermarkData::WalBased(WalWatermarkData { progress }) =
                        &self.watermarks.data
                    else {
                        unreachable!("Strategy validated in relocation_run()");
                    };
                    let has_wal_files_to_drop =
                        *progress > (db.wal.wal_file_size() + db.wal.min_wal_position());
                    self.save_progress(&db, !has_wal_files_to_drop)?;
                }
            }
            let entry = wal_iterator.next();
            if matches!(entry, Err(WalError::Crc(_))) {
                break;
            }
            let (position, raw_entry) = entry?;
            if position.offset() >= upper_limit {
                break;
            }
            self.watermarks.data = WatermarkData::WalBased(WalWatermarkData {
                progress: position.offset(),
            });
            match WalEntry::from_bytes(raw_entry) {
                WalEntry::Record(ks, key, value) => {
                    let ksd = db.key_shape.ks(ks);
                    match self.should_keep_entry(
                        &db,
                        &bloom_filters,
                        ksd,
                        &key,
                        &value,
                        position,
                    )? {
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
                            // TODO: handle potential races with concurrent writes for WAL replay
                            db.apply_insert(ks, key, value, Some(upper_limit))?
                        }
                    }
                }
                WalEntry::Index(..) => unreachable!("relocation must never process index entries"),
                WalEntry::Remove(..) | WalEntry::BatchStart(..) => {}
            }
        }
        self.save_progress(&db, false)?;
        Ok(())
    }

    fn should_keep_entry(
        &self,
        db: &Arc<Db>,
        bloom_filters: &HashMap<KeySpace, BloomFilter>,
        ks: &KeySpaceDesc,
        key: &[u8],
        value: &[u8],
        position: WalPosition,
    ) -> DbResult<Decision> {
        if let Some(filter) = ks.relocation_filter() {
            return Ok(filter(key, value));
        }
        let reduced_key = ks.reduce_key(key);

        if let Some(bloom) = bloom_filters.get(&ks.id()) {
            if !bloom.contains(&LargeTable::bloom_key(&reduced_key, position)) {
                return Ok(Decision::Remove);
            }
        }

        let context = db.ks_context(ks.id());
        let decision = match db
            .large_table
            .get(context, &reduced_key, db.as_ref(), true)?
        {
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
        let mut context = CellProcessingContext::new();
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
    fn relocate_entries(
        &self,
        entries: Vec<(Bytes, Bytes, WalPosition)>,
        keyspace: KeySpace,
        db: &Arc<Db>,
    ) -> DbResult<u64> {
        // Returns successful_inserts
        let mut successful_inserts = 0;

        for (key, value, _original_position) in entries {
            // TODO: handle potential races with concurrent writes to the same key
            // (same TODO as WAL-based relocation - consistency with existing approach)
            db.insert(keyspace, key.to_vec(), value.to_vec())?;
            successful_inserts += 1;
        }

        Ok(successful_inserts)
    }
}
