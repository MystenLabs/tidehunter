use crate::WalPosition;
use crate::batch::RelocatedWriteBatch;
use crate::db::{Db, DbResult, WalEntry};
use crate::flusher::RelocationFlush;
use crate::key_shape::KeySpace;
use crate::metrics::Metrics;
use crate::relocation::watermark::RelocationWatermarks;
use crate::wal::WalError;
use crate::wal::position::LastProcessed;
pub use cell_reference::CellReference;
use minibytes::Bytes;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak, mpsc};
use std::thread::JoinHandle;

mod cell_reference;
pub(crate) mod watermark;

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

/// WAL range one relocation stripe covers: `stripe_bytes` rounded down to
/// whole WAL files, and at least one file.
fn stripe_range(stripe_bytes: u64, wal_file_size: u64) -> u64 {
    (stripe_bytes / wal_file_size).max(1) * wal_file_size
}

/// Raises `Db::relocation_running` for the duration of a WAL-based run, which
/// pauses the periodic snapshot thread's size-triggered snapshots.
struct PeriodicSnapshotPause<'a>(&'a AtomicBool);

impl<'a> PeriodicSnapshotPause<'a> {
    fn new(flag: &'a AtomicBool) -> Self {
        flag.store(true, Ordering::Release);
        Self(flag)
    }
}

impl Drop for PeriodicSnapshotPause<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
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
        // A saved watermark may point into a keyspace destroyed since it was
        // written; its cells no longer exist, so re-anchor to the next live
        // keyspace.
        if let Some(cell_ref) = &current_cell_ref
            && db.key_shape.ks(cell_ref.keyspace).destroyed()
        {
            let mut next_keyspace = cell_ref.keyspace;
            next_keyspace.increment();
            current_cell_ref = CellReference::first(&db, next_keyspace);
        }

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
            let context = self.process_single_cell(
                &cell_ref,
                &db,
                effective_limit,
                RelocationStrategy::IndexBased(target_position),
            )?;

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
        // todo: support compressed-batch frames in the terminal_position scan
        // below. Today it only matches `WalEntry::Record(..)`, so records
        // buried inside a `CompressedBatch` never see the relocation filter
        // (e.g. `Decision::StopRelocation` is silently skipped). Until that
        // path decompresses inner entries, refuse to run with the codec on.
        assert!(
            db.config.batch_codec.is_none(),
            "wal-based relocation does not support Config::batch_codec yet"
        );
        // Raised before the scan below, so a periodic snapshot already under
        // way when the run starts can finish while the scan reads the WAL.
        let _paused = PeriodicSnapshotPause::new(&db.relocation_running);
        let last_processed = db.wal_writer.last_processed();
        let upper_limit = last_processed.as_u64();
        let min_wal_position = db.wal.min_wal_position();
        let mut wal_iterator = db.wal.wal_iterator_for_scan(min_wal_position)?;

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
            if matches!(entry, Err(WalError::Crc(_) | WalError::EndOfWal)) {
                break;
            }
            let (position, raw_entry) = entry?;
            if position.offset() >= upper_limit {
                terminal_position = position.offset();
                break;
            }
            let entry = WalEntry::from_bytes(raw_entry);
            assert!(
                !matches!(entry, WalEntry::CompressedBatch(..)),
                "wal-based relocation reached a CompressedBatch frame at {} but does not yet \
                 understand its inner records (todo)",
                position.offset()
            );
            if let WalEntry::Record(ks, key, value, _relocated) = entry {
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
        let stripe_range = stripe_range(db.config.relocation_stripe_bytes, db.wal.wal_file_size());
        let mut stripe_start = min_wal_position;
        let mut cutoff = Some(terminal_position);
        while stripe_start < target_position {
            let stripe_end = stripe_start
                .saturating_add(stripe_range)
                .min(target_position);
            self.relocate_stripe(&db, stripe_end, cutoff.take(), last_processed)?;
            // Every entry below `stripe_end` is now re-pointed or dropped in
            // the index; persist that before the WAL below it is reclaimed.
            // The stripe's snapshot stands in for the periodic one, so it
            // force-unloads behind the live tail as that would. Cells written
            // since the run started are flushed too, and replay after a crash
            // starts no earlier than the last stripe's snapshot.
            let unload_threshold = db
                .wal_writer
                .position()
                .saturating_sub(db.config.snapshot_unload_threshold());
            db.rebuild_control_region_from(stripe_end.max(unload_threshold))?;
            let replay_from = db.control_region_store.lock().last_position();
            db.wal_writer.gc(stripe_end.min(replay_from))?;
            self.metrics.relocation_stripes.inc();
            db.large_table.fp.fp_relocation_after_stripe();
            stripe_start = stripe_end;
        }
        Ok(())
    }

    /// One pass over every cell for the stripe ending at `stripe_end`. Key
    /// spaces without a relocation filter get their live entries below
    /// `stripe_end` copied to the WAL tail, or to a side WAL when the key
    /// space promotes (see `Db::relocation_target_generation`), and their
    /// index re-pointed at the copies; entries below the previous stripe's
    /// end were moved by that stripe, and either kind of copy sits above
    /// every stripe end. Key spaces with a filter only drop the entries
    /// below `cutoff`, which the first stripe does for the whole run.
    fn relocate_stripe(
        &self,
        db: &Arc<Db>,
        stripe_end: u64,
        cutoff: Option<u64>,
        last_processed: LastProcessed,
    ) -> DbResult<()> {
        let mut current_cell = CellReference::first(db, KeySpace::first());
        while let Some(cell) = current_cell.take() {
            current_cell = cell.next(db);
            let ks = db.key_shape.ks(cell.keyspace);
            // For keyspaces with relocation filter, flush and clear stale entries
            if ks.relocation_filter().is_some() {
                if let Some(cutoff) = cutoff {
                    let dropped = db.large_table.sync_flush_for_relocation(
                        db.ks_context(cell.keyspace),
                        &cell.cell_id,
                        db.as_ref(),
                        RelocationFlush::Cutoff(cutoff),
                    )?;
                    db.retire_index_blobs(&dropped);
                }
                continue;
            }
            // For keyspaces without relocation filter, relocate entries.
            // Relocate the value as of the checkpoint frontier, not the live
            // latest: a post-frontier overwrite would otherwise strand the
            // as-of value (which GC reclaims) that a held checkpoint reads.
            // `get_index_for_cell` returns that as-of view directly — the overlay
            // is filtered to < `last_processed` before any latest-per-key collapse,
            // and the on-disk levels it merges under are already as-of-safe.
            let context = db.ks_context(cell.keyspace);
            let Some(mut index) = db.large_table.get_index_for_cell(
                context,
                &cell.cell_id,
                db.as_ref(),
                last_processed,
            )?
            else {
                continue;
            };
            let batch_limit = db.config.relocation_batch_max_bytes();
            // Where the key space's records are promoted to (0: nowhere), and
            // how many passes a record must have survived by then. A record
            // read here has survived this pass; one that is itself a
            // relocation copy survived an earlier pass as well.
            let promote_to =
                db.relocation_target_generation(cell.keyspace, RelocationStrategy::WalBased);
            let promote_after = ks.promote_after_passes();
            // The copies re-point a key only while its latest position is
            // still below `stripe_end`, i.e. the original this stripe read.
            let new_batch = |target_generation| {
                RelocatedWriteBatch::new(
                    cell.keyspace,
                    cell.cell_id.clone(),
                    stripe_end,
                    target_generation,
                )
            };
            let mut main_batch = new_batch(0);
            let mut promoted_batch = new_batch(promote_to);
            for (reduced_key, position) in index.iter(context) {
                if position.offset() < stripe_end
                    && let Some((key, value, recopied)) =
                        db.read_record_for_relocation(context, position, reduced_key.as_ref())?
                {
                    let promote = promote_to != 0 && (promote_after <= 1 || recopied);
                    let batch = if promote {
                        &mut promoted_batch
                    } else {
                        &mut main_batch
                    };
                    batch.write(key, value);
                    let ks_name = &[ks.name()];
                    self.metrics
                        .relocation_kept
                        .with_label_values(ks_name)
                        .inc();
                    if recopied {
                        self.metrics
                            .relocation_recopied
                            .with_label_values(ks_name)
                            .inc();
                    }
                    if promote {
                        self.metrics
                            .relocation_promoted
                            .with_label_values(ks_name)
                            .inc();
                    }
                    if batch.size_bytes() >= batch_limit {
                        let full = std::mem::replace(batch, new_batch(batch.target_generation));
                        db.write_relocated_batch(full)?;
                    }
                }
            }
            db.write_relocated_batch(main_batch)?;
            db.write_relocated_batch(promoted_batch)?;
        }
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
        strategy: RelocationStrategy,
    ) -> DbResult<CellProcessingContext> {
        // The CAS threshold must be the same frontier the as-of index view
        // below is built with. Any write newer than that view sits at an
        // offset >= `effective_limit`; a higher threshold (e.g. the live
        // frontier) would let the CAS re-point such a write to the relocated
        // copy of the older as-of value, losing the overwrite. (The WAL-based
        // path passes each stripe's end, which lies at or below the frontier
        // its view is built with.)
        let batch = RelocatedWriteBatch::new(
            cell_ref.keyspace,
            cell_ref.cell_id.clone(),
            effective_limit,
            db.relocation_target_generation(cell_ref.keyspace, strategy),
        );
        let mut context = CellProcessingContext::new(batch);
        let mut removed_count = 0;

        // Phase A: Get shared reference to cell index, as of the relocation
        // frontier (filters the overlay before any latest-per-key collapse).
        let mut index = match db.large_table.get_index_for_cell(
            db.ks_context(cell_ref.keyspace),
            &cell_ref.cell_id,
            db.as_ref(),
            LastProcessed::new(effective_limit),
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
        let ks_context = db.ks_context(cell_ref.keyspace);
        let keyspace_desc = &ks_context.ks_config;
        // Relocate the value as of the checkpoint frontier, not the live latest
        // (see the WAL-based path for why). `get_index_for_cell` already returns
        // the as-of view; the loop below skips any on-disk entry still at >= the
        // cutoff.
        for (reduced_key, position) in index.iter(ks_context) {
            if position.offset() >= effective_limit {
                continue;
            }

            // Read the record from WAL. The index holds the reduced key (a hash
            // or a key range for such keyspaces); the filter and the relocated
            // copy take the full key the record carries, as on the WAL-based path.
            let (key, value) =
                match db.read_record_for_indexed_key(ks_context, position, reduced_key.as_ref())? {
                    Some(record) => record,
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
                .map_or(Decision::Keep, |filter| filter(&key, &value));

            match decision {
                Decision::Keep => {
                    context.add_entry_to_relocate(key, value, position);
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

    /// Writes the copies of one cell of an index-based pass, following the
    /// same pattern as WAL-based relocation.
    fn relocate_entries(&self, batch: RelocatedWriteBatch, db: &Arc<Db>) -> DbResult<u64> {
        let successful_inserts = batch.len() as u64;

        db.write_relocated_batch(batch)?;

        Ok(successful_inserts)
    }
}
