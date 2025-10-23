use crate::WalPosition;
use crate::batch::RelocatedWriteBatch;
use crate::db::{Db, DbResult, WalEntry};
use crate::index::index_table::IndexTable;
use crate::key_shape::KeySpace;
use crate::large_table::Loader;
use crate::metrics::Metrics;
use crate::relocation::watermark::RelocationWatermarks;
use crate::wal::WalError;
pub use cell_reference::CellReference;
use minibytes::Bytes;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{Arc, Weak, mpsc};
use std::thread::JoinHandle;

mod cell_reference;
mod watermark;

#[cfg(test)]
mod relocation_tests;
pub mod updates;

/// Computes a target WAL position based on a ratio of the total WAL range.
///
/// # Arguments
/// * `db` - The database instance
/// * `ratio` - A value between 0.0 and 1.0 representing the fraction of WAL to process.
///   0.0 = start of WAL, 0.5 = middle, 1.0 = end (last_processed)
///
/// # Returns
/// * `Some(position)` - The computed target position in bytes
/// * `None` - If there is no WAL data to process (last_processed <= min_position)
///
/// # Examples
/// ```ignore
/// // Relocate the first 30% of the WAL
/// let target = compute_target_position_from_ratio(&db, 0.3);
/// db.start_relocation_with_strategy(RelocationStrategy::IndexBased(target));
/// ```
pub fn compute_target_position_from_ratio(db: &Arc<Db>, ratio: f64) -> Option<u64> {
    // Clamp ratio to valid range [0.0, 1.0]
    let ratio = ratio.clamp(0.0, 1.0);

    // Get the WAL range
    let min_position = db.wal.min_wal_position();
    let last_processed = db.wal_writer.last_processed().as_u64();

    // Handle edge cases
    if last_processed <= min_position {
        return None; // WAL is empty or in invalid state
    }

    // Compute the total range
    let total_range = last_processed - min_position;

    // Calculate target position
    let offset = (total_range as f64 * ratio) as u64;
    let target = min_position + offset;

    // Ensure we don't exceed last_processed
    Some(target.min(last_processed))
}

pub(crate) struct Relocator(pub(crate) mpsc::Sender<RelocationCommand>);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelocationStrategy {
    /// WAL-based sequential relocation
    WalBased,
    /// Index-based relocation that processes entire cells atomically
    /// with an optional target position limit
    IndexBased(Option<u64>),
}

#[allow(clippy::derivable_impls)] // Can't derive Default with tuple variant
impl Default for RelocationStrategy {
    fn default() -> Self {
        RelocationStrategy::WalBased
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
            RelocationStrategy::IndexBased(target_position) => {
                self.index_based_relocation(db, target_position)
            }
        }
    }

    fn index_based_relocation(
        &mut self,
        db: Arc<Db>,
        target_position: Option<u64>,
    ) -> DbResult<()> {
        let mut watermarks = RelocationWatermarks::read_or_create(&self.path)?;
        // Capture the upper WAL limit to avoid race conditions
        // Only process entries written before this point. This is the last position that was written
        // and made its way into the large table
        let upper_limit = db.wal_writer.last_processed().as_u64();

        // Compute effective limit based on target_position
        let effective_limit =
            target_position.map_or(upper_limit, |t| std::cmp::min(t, upper_limit));

        // Restart from beginning if target_position changed or if previous run completed
        let should_restart = watermarks.data.next_to_process.is_none()
            || watermarks.data.target_position != target_position;

        // Get starting cell reference from saved progress or restart
        let mut current_cell_ref = if should_restart {
            CellReference::first(&db, KeySpace::first())
        } else {
            watermarks.data.next_to_process.clone()
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
                    watermarks.set(
                        Some(cell_ref.clone()),
                        highest_wal_position,
                        upper_limit,
                        target_position,
                    );
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
            let context = self.process_single_cell(&cell_ref, &db, effective_limit)?;

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
        watermarks.set(
            current_cell_ref.clone(),
            highest_wal_position,
            upper_limit,
            target_position,
        );
        self.save_progress(&db, &watermarks, false)?;
        Ok(())
    }

    fn wal_based_relocation(&mut self, db: Arc<Db>) -> DbResult<()> {
        let upper_limit = db.wal_writer.last_processed().as_u64();
        let min_wal_position = db.wal.min_wal_position();
        let mut wal_iterator = db.wal.wal_iterator(min_wal_position)?;

        // Calculate the maximum amount we can reclaim based on configured percentage
        let max_target_position = min_wal_position
            + (upper_limit.saturating_sub(min_wal_position)
                * db.config.relocation_max_reclaim_pct as u64
                / 100)
                .max(db.wal.wal_file_size());
        // find target cut-off position
        let mut target_position = None;
        loop {
            let entry = wal_iterator.next();
            if matches!(entry, Err(WalError::Crc(_))) {
                break;
            }
            let (position, raw_entry) = entry?;
            if position.offset() >= max_target_position.min(upper_limit) {
                target_position = Some(position);
                break;
            }
            if let WalEntry::Record(ks, key, value, _relocated) = WalEntry::from_bytes(raw_entry) {
                let ksd = db.key_shape.ks(ks);
                if let Some(filter) = ksd.relocation_filter()
                    && let Decision::StopRelocation = filter(&key, &value)
                {
                    target_position = Some(position);
                    break;
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
        if target_position.offset() < (db.wal.wal_file_size() + min_wal_position) {
            return Ok(());
        }
        let mut current_cell = CellReference::first(&db, KeySpace::first());
        while let Some(cell) = current_cell.take() {
            current_cell = cell.next(&db);
            let ks = db.key_shape.ks(cell.keyspace);
            // For keyspaces with relocation filter, flush and clear stale entries
            if ks.relocation_filter().is_some() {
                db.large_table.sync_flush_for_relocation(
                    db.ks_context(cell.keyspace),
                    &cell.cell_id,
                    db.as_ref(),
                    None,
                    Some(target_position.offset()),
                )?;
                continue;
            }
            // For keyspaces without relocation filter, relocate entries
            let mut batch = RelocatedWriteBatch::new(
                cell.keyspace,
                cell.cell_id.clone(),
                target_position.offset(),
            );
            let index = db
                .large_table
                .get_cell_index(db.ks_context(cell.keyspace), &cell.cell_id, db.as_ref())?
                .unwrap_or(IndexTable::default().into());
            for (_reduced_key, position) in index.iter() {
                if position.offset() < target_position.offset()
                    && let Some((key, value)) = db.read_record(position)?
                {
                    batch.write(key, value);
                    self.metrics
                        .relocation_kept
                        .with_label_values(&[ks.name()])
                        .inc();
                }
            }
            db.write_relocated_batch(batch)?;
        }
        db.rebuild_control_region_from(target_position.offset())?;
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
        effective_limit: u64,
    ) -> DbResult<CellProcessingContext> {
        let batch = RelocatedWriteBatch::new(
            cell_ref.keyspace,
            cell_ref.cell_id.clone(),
            db.last_processed_wal_position().as_u64(),
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
            if position.offset() >= effective_limit {
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
