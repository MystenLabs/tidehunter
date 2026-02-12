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
use std::time::{Duration, Instant};

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
    entries_skipped: u64,
    index_load_time: Duration,
    entry_read_time: Duration,
}

impl CellProcessingContext {
    fn new(batch: RelocatedWriteBatch) -> Self {
        Self {
            batch,
            highest_wal_position: WalPosition::new(0, 0),
            entries_removed: 0,
            entries_kept: 0,
            entries_skipped: 0,
            index_load_time: Duration::ZERO,
            entry_read_time: Duration::ZERO,
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

        let wm_gc = watermarks.gc_watermark();
        let cr_pos = db.control_region_store.lock().last_position();
        let gc_watermark = std::cmp::min(wm_gc, cr_pos);
        eprintln!(
            "[relocation] GC watermark components: watermarks_gc={}, control_region={}, result={}",
            wm_gc, cr_pos, gc_watermark
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

        eprintln!(
            "[relocation] Starting index-based relocation: upper_limit={}, effective_limit={}, target_position={:?}, should_restart={}, min_wal_position={}",
            upper_limit,
            effective_limit,
            target_position,
            should_restart,
            db.wal.min_wal_position()
        );
        let iteration_start = std::time::Instant::now();

        // Get starting cell reference from saved progress or restart
        let mut current_cell_ref = if should_restart {
            CellReference::first(&db, KeySpace::first())
        } else {
            watermarks.data.next_to_process.clone()
        };

        let mut cells_processed = 0u64;
        let mut highest_wal_position = 0u64;
        let mut current_ks_id = None;

        // Phase timing accumulators for periodic progress logging
        let mut phase_a_total = Duration::ZERO; // index loading
        let mut phase_b_total = Duration::ZERO; // entry iteration + WAL reads
        let mut phase_c_total = Duration::ZERO; // write_relocated_batch (WAL write + flush)
        let mut total_entries_kept = 0u64;
        let mut total_entries_removed = 0u64;
        let mut total_entries_skipped = 0u64;
        let mut batch_log_start = Instant::now();

        while let Some(cell_ref) = current_cell_ref.take() {
            // Check for cancellation periodically
            if cells_processed.is_multiple_of(Self::NUM_ITERATIONS_IN_BATCH as u64) {
                if self.should_cancel_relocation() {
                    break;
                }
                // Log progress every NUM_ITERATIONS_IN_BATCH cells
                if cells_processed > 0 {
                    let batch_elapsed = batch_log_start.elapsed();
                    eprintln!(
                        "[relocation] Progress: cells={}, entries_kept={}, entries_removed={}, entries_skipped={}, \
                         phase_a(index_load)={:.1}s, phase_b(entry_read)={:.1}s, phase_c(write+flush)={:.1}s, \
                         batch_wall={:.1}s, total_elapsed={:.0}s",
                        cells_processed,
                        total_entries_kept,
                        total_entries_removed,
                        total_entries_skipped,
                        phase_a_total.as_secs_f64(),
                        phase_b_total.as_secs_f64(),
                        phase_c_total.as_secs_f64(),
                        batch_elapsed.as_secs_f64(),
                        iteration_start.elapsed().as_secs_f64(),
                    );
                    batch_log_start = Instant::now();
                }

                // Save progress periodically
                if cells_processed.is_multiple_of(Self::NUM_ITERATIONS_TILL_SAVE as u64)
                    && cells_processed > 0
                {
                    watermarks.set(
                        Some(cell_ref.clone()),
                        highest_wal_position,
                        upper_limit,
                        target_position,
                    );
                    self.save_progress(&db, &watermarks, false)?;
                    // Save progress and run gc()
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

            // Accumulate per-cell timing
            phase_a_total += context.index_load_time;
            phase_b_total += context.entry_read_time;
            total_entries_kept += context.entries_kept;
            total_entries_removed += context.entries_removed;
            total_entries_skipped += context.entries_skipped;

            // Track the highest WAL position seen
            if context.highest_wal_position.offset() > highest_wal_position {
                highest_wal_position = context.highest_wal_position.offset();
            }

            // Relocate entries if any were marked for keeping
            let keyspace_desc = &db.ks_context(cell_ref.keyspace).ks_config;
            if !context.batch.is_empty() {
                let phase_c_start = Instant::now();
                let successful = self.relocate_entries(context.batch, &db)?;
                phase_c_total += phase_c_start.elapsed();
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
        let total_elapsed = iteration_start.elapsed();
        eprintln!(
            "[relocation] Completed index-based relocation: cells_processed={}, highest_wal_position={}, \
             upper_limit={}, effective_limit={}, elapsed_secs={}, \
             phase_a(index_load)={:.1}s, phase_b(entry_read)={:.1}s, phase_c(write+flush)={:.1}s, \
             entries_kept={}, entries_removed={}, entries_skipped={}",
            cells_processed,
            highest_wal_position,
            upper_limit,
            effective_limit,
            total_elapsed.as_secs(),
            phase_a_total.as_secs_f64(),
            phase_b_total.as_secs_f64(),
            phase_c_total.as_secs_f64(),
            total_entries_kept,
            total_entries_removed,
            total_entries_skipped,
        );
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
        let mut terminal_position = 0;
        loop {
            let entry = wal_iterator.next();
            if matches!(entry, Err(WalError::Crc(_))) {
                break;
            }
            let (position, raw_entry) = entry?;
            if position.offset() >= upper_limit {
                terminal_position = position.offset();
                break;
            }
            if let WalEntry::Record(ks, key, value, _relocated) = WalEntry::from_bytes(raw_entry) {
                let ksd = db.key_shape.ks(ks);
                if let Some(filter) = ksd.relocation_filter()
                    && let Decision::StopRelocation = filter(&key, &value)
                {
                    terminal_position = position.offset();
                    break;
                }
            }
        }
        let mut target_position = terminal_position.min(max_target_position);
        target_position -= target_position % db.wal.wal_file_size();
        self.metrics
            .relocation_target_position
            .set(target_position as i64);
        self.metrics
            .relocation_terminal_position
            .set(terminal_position as i64);
        // ensure the target position is big enough to cut
        if target_position < (db.wal.wal_file_size() + min_wal_position) {
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
                    Some(terminal_position),
                )?;
                continue;
            }
            // For keyspaces without relocation filter, relocate entries
            let mut batch =
                RelocatedWriteBatch::new(cell.keyspace, cell.cell_id.clone(), target_position);
            let index = db
                .large_table
                .get_index_for_cell(db.ks_context(cell.keyspace), &cell.cell_id, db.as_ref())?
                .unwrap_or(IndexTable::default().into());
            for (_reduced_key, position) in index.iter() {
                if position.offset() < target_position
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
        db.rebuild_control_region_from(target_position)?;
        db.wal_writer.gc(std::cmp::min(
            target_position,
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
        let phase_a_start = Instant::now();
        let index = match db.large_table.get_index_for_cell(
            db.ks_context(cell_ref.keyspace),
            &cell_ref.cell_id,
            db.as_ref(),
        )? {
            Some(index) => index,
            None => {
                context.index_load_time = phase_a_start.elapsed();
                // Cell doesn't exist or is empty
                return Ok(context);
            }
        };
        context.index_load_time = phase_a_start.elapsed();

        // Phase B: Collect entries, sort by WAL position for sequential I/O, then read values.
        // Sorting by position turns random mmap reads into sequential access, enabling OS readahead.
        // TODO(#74): Further optimization possible - add key-only decision callback to
        // RelocationFilter trait to skip WAL reads for entries decidable by key alone.
        let phase_b_start = Instant::now();
        let keyspace_desc = &db.ks_context(cell_ref.keyspace).ks_config;

        // Step 1: Collect eligible (key, position) pairs from the index
        let mut entries: Vec<(Bytes, WalPosition)> = Vec::new();
        for (key, position) in index.iter() {
            if position.offset() >= effective_limit {
                context.entries_skipped += 1;
                continue;
            }
            entries.push((key.clone(), position));
        }

        // Step 2: Sort by WAL position offset for sequential I/O
        entries.sort_unstable_by_key(|(_key, pos)| pos.offset());

        // Step 3: Read in position order, with or without filter
        if let Some(filter) = keyspace_desc.relocation_filter() {
            for (key, position) in &entries {
                let value = match db.read_record(*position)? {
                    Some((_, val)) => val,
                    None => {
                        context.mark_entry_removed(*position);
                        removed_count += 1;
                        continue;
                    }
                };

                match filter(key, &value) {
                    Decision::Keep => {
                        context.add_entry_to_relocate(key.clone(), value, *position);
                    }
                    Decision::Remove => {
                        context.mark_entry_removed(*position);
                        removed_count += 1;
                    }
                    Decision::StopRelocation => {
                        break;
                    }
                }
            }
        } else {
            // No filter: all entries are unconditionally kept
            for (key, position) in &entries {
                let value = match db.read_record(*position)? {
                    Some((_, val)) => val,
                    None => {
                        context.mark_entry_removed(*position);
                        removed_count += 1;
                        continue;
                    }
                };
                context.add_entry_to_relocate(key.clone(), value, *position);
            }
        }
        context.entry_read_time = phase_b_start.elapsed();

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
