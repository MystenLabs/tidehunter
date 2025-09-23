use crate::cell::CellId;
use crate::db::{Db, DbResult, WalEntry};
use crate::key_shape::{KeySpace, KeySpaceDesc};
use crate::large_table::{GetResult, LargeTable};
use crate::metrics::Metrics;
use crate::wal::WalError;
use crate::WalPosition;
use bloom::{BloomFilter, ASMS};
use minibytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{mpsc, Arc, Weak};
use std::thread::JoinHandle;

mod watermark;
pub use watermark::{CellBasedWatermark, RelocationWatermarks};

pub(crate) struct Relocator(pub(crate) mpsc::Sender<RelocationCommand>);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelocationStrategy {
    /// Original WAL-based sequential relocation
    WalBased,
    /// New cell-based relocation that processes entire cells atomically
    CellBased,
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

#[derive(Clone)]
pub struct CellReference {
    pub keyspace_desc: KeySpaceDesc,
    pub cell_id: CellId,
}

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
            highest_wal_position: WalPosition::INVALID,
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

/// Iterator for traversing all cells across all keyspaces in the database.
/// Supports all key types including uniform and prefixed uniform keys.
pub struct CellIterator<'a> {
    db: &'a Db,
    /// Current keyspace being iterated (0-based index)
    current_keyspace: usize,
    /// Current cell position within the keyspace (None = start of keyspace)
    current_cell: Option<CellId>,
}

impl<'a> CellIterator<'a> {
    pub fn new(db: &'a Db) -> Self {
        Self {
            db,
            current_keyspace: 0,
            current_cell: None,
        }
    }

    pub fn from_watermark(db: &'a Db, watermark: &CellBasedWatermark) -> Self {
        let mut iter = Self::new(db);
        iter.current_keyspace = watermark.keyspace_id as usize;
        iter.current_cell = watermark.cell_id.clone();
        iter
    }

    pub fn current_position(&self) -> CellBasedWatermark {
        CellBasedWatermark {
            keyspace_id: self.current_keyspace as u8,
            cell_id: self.current_cell.clone(),
            highest_wal_position: 0, // Will be updated during processing
            upper_limit: 0,          // Will be set during relocation
        }
    }

    pub fn next_cell(&mut self) -> Option<CellReference> {
        loop {
            if self.current_keyspace >= self.db.key_shape.num_ks() {
                return None;
            }

            let ks_desc = self.db.key_shape.ks(KeySpace(self.current_keyspace as u8));
            let context = self.db.ks_context(ks_desc.id());

            let next_cell = match &self.current_cell {
                None => Some(ks_desc.first_cell()),
                Some(cell) => self.db.large_table.next_cell(context, cell, false),
            };

            match next_cell {
                Some(cell) => {
                    self.current_cell = Some(cell.clone());
                    return Some(CellReference {
                        keyspace_desc: ks_desc.clone(),
                        cell_id: cell,
                    });
                }
                None => {
                    // Move to next keyspace
                    self.current_keyspace += 1;
                    self.current_cell = None;
                }
            }
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

        // For cell-based relocation, use the minimum of highest_wal_position seen and upper_limit
        // This ensures we never GC WAL segments beyond the safe boundary
        let gc_watermark = if self.watermarks.get_cell_progress().upper_limit > 0 {
            // Cell-based relocation is active
            let cell_watermark = self.watermarks.get_cell_progress();
            std::cmp::min(
                std::cmp::min(
                    cell_watermark.highest_wal_position,
                    cell_watermark.upper_limit,
                ),
                db.control_region_store.lock().last_position(),
            )
        } else {
            // WAL-based relocation
            std::cmp::min(
                self.watermarks.get_relocation_progress(),
                db.control_region_store.lock().last_position(),
            )
        };

        db.wal_writer.gc(gc_watermark)?;
        Ok(())
    }

    fn relocation_run(&mut self, strategy: RelocationStrategy) -> DbResult<()> {
        match strategy {
            RelocationStrategy::WalBased => self.wal_based_relocation(),
            RelocationStrategy::CellBased => self.cell_based_relocation(),
        }
    }

    fn cell_based_relocation(&mut self) -> DbResult<()> {
        let Some(db) = self.db.upgrade() else {
            return Ok(());
        };

        // Capture the upper WAL limit to avoid race conditions
        // Only process entries written before this point
        let upper_limit = db.wal_writer.position();

        // Create iterator starting from saved progress
        let mut cell_iter = if self.watermarks.get_cell_progress().keyspace_id == 0
            && self.watermarks.get_cell_progress().cell_id.is_none()
        {
            // Starting from beginning
            CellIterator::new(&db)
        } else {
            // Resume from saved position
            CellIterator::from_watermark(&db, self.watermarks.get_cell_progress())
        };

        let mut cells_processed = 0;
        let mut highest_wal_position = 0u64;

        // Build bloom filters for optimization (same as WAL-based relocation)
        let bloom_filters = db.large_table.build_index_bloom_filters(db.as_ref())?;

        let mut current_ks_id = None;

        while let Some(cell_ref) = cell_iter.next_cell() {
            // Check for cancellation periodically
            if cells_processed % Self::NUM_ITERATIONS_IN_BATCH == 0 {
                if self.should_cancel_relocation() {
                    break;
                }
                // Save progress periodically
                if cells_processed % Self::NUM_ITERATIONS_TILL_SAVE == 0 {
                    let mut current_pos = cell_iter.current_position();
                    current_pos.upper_limit = upper_limit;
                    current_pos.highest_wal_position = highest_wal_position;
                    self.watermarks.set_cell_progress(current_pos);
                    self.save_progress(&db, true)?; // Save watermark only
                }
            }

            // Update current keyspace metric when it changes
            let ks_id = cell_ref.keyspace_desc.id().as_usize();
            if current_ks_id != Some(ks_id) {
                current_ks_id = Some(ks_id);
                self.metrics.relocation_current_keyspace.set(ks_id as i64);
            }

            // Process each cell
            let context = self.process_single_cell(&cell_ref, &db, &bloom_filters)?;

            // Track the highest WAL position seen
            if context.highest_wal_position.offset() > highest_wal_position {
                highest_wal_position = context.highest_wal_position.offset();
            }

            // Relocate entries if any were marked for keeping
            if !context.entries_to_relocate.is_empty() {
                let successful = self.relocate_entries(
                    context.entries_to_relocate,
                    cell_ref.keyspace_desc.id(),
                    &db,
                )?;
                // Track successful relocations with existing metrics (same as WAL-based)
                self.metrics
                    .relocation_kept
                    .with_label_values(&[cell_ref.keyspace_desc.name()])
                    .inc_by(successful);
            }

            // Track cells processed
            self.metrics
                .relocation_cells_processed
                .with_label_values(&[cell_ref.keyspace_desc.name()])
                .inc();

            cells_processed += 1;
        }

        // Save final progress with upper_limit and highest WAL position
        let mut final_pos = cell_iter.current_position();
        final_pos.upper_limit = upper_limit;
        final_pos.highest_wal_position = highest_wal_position;
        self.watermarks.set_cell_progress(final_pos);
        self.save_progress(&db, false)?;

        Ok(())
    }

    fn wal_based_relocation(&mut self) -> DbResult<()> {
        let Some(db) = self.db.upgrade() else {
            return Ok(());
        };
        // TODO: handle potentially uninitialized positions at the end of the WAL
        let upper_limit = db.wal_writer.position();
        let start_position = self.watermarks.get_relocation_progress();
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
                    let has_wal_files_to_drop = self.watermarks.get_relocation_progress()
                        > (db.wal.wal_file_size() + db.wal.min_wal_position());
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
            self.watermarks.set_relocation_progress(position);
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
                            // TODO: handle potential races with concurrent writes to the same key
                            db.insert(ks, key, value)?
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
        bloom_filters: &HashMap<KeySpace, BloomFilter>,
    ) -> DbResult<CellProcessingContext> {
        let mut context = CellProcessingContext::new();
        let mut removed_count = 0;

        // Phase A: Get shared reference to cell index
        let index = match db.large_table.get_cell_index(
            db.ks_context(cell_ref.keyspace_desc.id()),
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
        for (key, position) in index.iter() {
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

            let decision = self.should_keep_entry(
                db,
                bloom_filters,
                &cell_ref.keyspace_desc,
                &key,
                &value,
                position,
            )?;

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
                .with_label_values(&[cell_ref.keyspace_desc.name()])
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
