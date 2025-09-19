use crate::cell::CellId;
use crate::db::{Db, DbResult, WalEntry};
use crate::key_shape::{KeySpace, KeySpaceDesc, KeyType};
use crate::large_table::{GetResult, LargeTable};
use crate::metrics::Metrics;
use crate::wal::WalError;
use crate::WalPosition;
use bloom::{BloomFilter, ASMS};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{mpsc, Arc, Weak};
use std::thread::JoinHandle;

mod watermark;
pub use watermark::{RelocationWatermarks, CellBasedWatermark};

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
    pub keyspace_id: KeySpace,
    pub keyspace_desc: KeySpaceDesc,
    // TODO: These fields will be used in Phase 2 for actual cell processing
    #[allow(dead_code)]
    pub row_index: usize,
    #[allow(dead_code)]
    pub cell_id: CellId,
}

pub struct CellIterator<'a> {
    db: &'a Db,
    current_keyspace: usize,
    current_row: usize,
    current_cell_index: usize,
    // TODO: Add support for PrefixedUniform iteration in Phase 2
    #[allow(dead_code)]
    current_tree_iter: Option<std::collections::btree_map::Keys<'a, crate::cell::CellIdBytesContainer, crate::large_table::LargeTableEntry>>,
    current_tree_position: Option<crate::cell::CellIdBytesContainer>,
}

impl<'a> CellIterator<'a> {
    pub fn new(db: &'a Db) -> Self {
        Self {
            db,
            current_keyspace: 0,
            current_row: 0,
            current_cell_index: 0,
            current_tree_iter: None,
            current_tree_position: None,
        }
    }

    pub fn from_watermark(db: &'a Db, watermark: &CellBasedWatermark) -> Self {
        let mut iter = Self::new(db);
        iter.current_keyspace = watermark.keyspace_id as usize;
        iter.current_row = watermark.row_index;
        iter.current_cell_index = watermark.cell_index;
        if let Some(ref cell_bytes) = watermark.cell_bytes {
            use smallvec::SmallVec;
            iter.current_tree_position = Some(SmallVec::from_slice(cell_bytes));
        }
        iter
    }

    pub fn current_position(&self) -> CellBasedWatermark {
        CellBasedWatermark {
            keyspace_id: self.current_keyspace as u8,
            row_index: self.current_row,
            cell_index: self.current_cell_index,
            cell_bytes: self.current_tree_position.as_ref().map(|bytes| bytes.to_vec()),
            highest_wal_position: 0, // Will be updated during processing
        }
    }

    pub fn next_cell(&mut self) -> Option<CellReference> {
        loop {
            if self.current_keyspace >= self.db.key_shape.num_ks() {
                return None;
            }

            let ks_desc = self.db.key_shape.ks(KeySpace(self.current_keyspace as u8));

            if self.current_row >= ks_desc.num_mutexes() {
                // Move to next keyspace
                self.current_keyspace += 1;
                self.current_row = 0;
                self.current_cell_index = 0;
                self.current_tree_iter = None;
                self.current_tree_position = None;
                continue;
            }

            match ks_desc.key_type() {
                KeyType::Uniform(config) => {
                    // For uniform keys, we iterate through array indices
                    let cells_per_mutex = config.cells_per_mutex();
                    if self.current_cell_index >= cells_per_mutex {
                        // Move to next row
                        self.current_row += 1;
                        self.current_cell_index = 0;
                        continue;
                    }

                    let cell_ref = CellReference {
                        keyspace_id: KeySpace(self.current_keyspace as u8),
                        keyspace_desc: ks_desc.clone(),
                        row_index: self.current_row,
                        cell_id: CellId::Integer(self.current_cell_index),
                    };

                    self.current_cell_index += 1;
                    return Some(cell_ref);
                }
                KeyType::PrefixedUniform(_) => {
                    // For prefixed uniform keys, we need to iterate through the BTreeMap
                    // This is more complex and would require accessing the actual row data
                    // For now, let's move to the next row
                    self.current_row += 1;
                    continue;
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
        let gc_watermark = std::cmp::min(
            self.watermarks.get_relocation_progress(),
            db.control_region_store.lock().last_position(),
        );
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

        // Create iterator starting from saved progress
        let mut cell_iter = if self.watermarks.get_cell_progress().keyspace_id == 0 &&
                               self.watermarks.get_cell_progress().row_index == 0 &&
                               self.watermarks.get_cell_progress().cell_index == 0 {
            // Starting from beginning
            CellIterator::new(&db)
        } else {
            // Resume from saved position
            CellIterator::from_watermark(&db, self.watermarks.get_cell_progress())
        };

        let mut cells_processed = 0;
        let save_interval = 100; // Save progress every 100 cells

        let mut current_ks_id = None;

        while let Some(cell_ref) = cell_iter.next_cell() {
            // Check for cancellation periodically
            if cells_processed % 10 == 0 {
                if self.should_cancel_relocation() {
                    break;
                }
            }

            // Update current keyspace metric when it changes
            let ks_id = cell_ref.keyspace_id.as_usize();
            if current_ks_id != Some(ks_id) {
                current_ks_id = Some(ks_id);
                self.metrics.relocation_current_keyspace.set(ks_id as i64);
            }

            // TODO: Process the cell entries (Phase 2)
            // For now, just update progress

            // Track cells processed
            self.metrics
                .relocation_cells_processed
                .with_label_values(&[cell_ref.keyspace_desc.name()])
                .inc();

            cells_processed += 1;

            // Save progress periodically
            if cells_processed % save_interval == 0 {
                let current_pos = cell_iter.current_position();
                self.watermarks.set_cell_progress(current_pos);
                self.save_progress(&db, true)?; // Save watermark only
            }
        }

        // Save final progress
        let final_pos = cell_iter.current_position();
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
}
