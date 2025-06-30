use crate::batch::WriteBatch;
use crate::cell::CellId;
use crate::config::Config;
use crate::control::{ControlRegion, ControlRegionStore};
use crate::crc::IntoBytesFixed;
use crate::flusher::IndexFlusher;
use crate::index::index_format::IndexFormat;
use crate::index::index_table::IndexTable;
use crate::iterators::db_iterator::DbIterator;
use crate::iterators::IteratorResult;
use crate::key_shape::{KeyShape, KeySpace, KeySpaceDesc, KeyType};
use crate::large_table::{GetResult, LargeTable, Loader};
use crate::metrics::{Metrics, TimerExt};
use crate::state_snapshot;
use crate::wal::{
    PreparedWalWrite, Wal, WalError, WalIterator, WalPosition, WalRandomRead, WalWriter,
};
use bloom::needed_bits;
use bytes::{Buf, BufMut, BytesMut};
use minibytes::Bytes;
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Weak};
use std::time::Duration;
use std::{io, thread};

pub struct Db {
    large_table: LargeTable,
    wal: Arc<Wal>,
    wal_writer: WalWriter,
    control_region_store: Mutex<ControlRegionStore>,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
    key_shape: KeyShape,
}

pub type DbResult<T> = Result<T, DbError>;

pub const MAX_KEY_LEN: usize = u16::MAX as usize;
pub const CONTROL_REGION_FILE: &str = "cr";

impl Db {
    pub fn open(
        path: &Path,
        key_shape: KeyShape,
        config: Arc<Config>,
        metrics: Arc<Metrics>,
    ) -> DbResult<Arc<Self>> {
        let path = path.canonicalize()?;
        let wal_path = Self::wal_path(&path);
        let (control_region_store, control_region) =
            Self::read_or_create_control_region(path.join(CONTROL_REGION_FILE), &key_shape)?;
        let wal = Wal::open(&wal_path, config.wal_layout(), metrics.clone())?;

        // Create channels for flusher threads first
        let (flusher_senders, flusher_receivers) = (0..config.num_flusher_threads)
            .map(|_| mpsc::channel())
            .unzip();

        let flusher = IndexFlusher::new(flusher_senders, metrics.clone());
        let large_table = LargeTable::from_unloaded(
            &key_shape,
            control_region.snapshot(),
            config.clone(),
            flusher,
            metrics.clone(),
            wal.as_ref(),
        );
        let wal_iterator = wal.wal_iterator(control_region.last_position())?;
        let wal_writer = Self::replay_wal(&key_shape, &large_table, wal_iterator, &metrics)?;
        let control_region_store = Mutex::new(control_region_store);
        let this = Self {
            large_table,
            wal_writer,
            wal,
            control_region_store,
            config,
            metrics: metrics.clone(),
            key_shape,
        };
        this.report_memory_estimates();
        let this = Arc::new(this);

        // Now start the flusher threads with the weak reference
        let weak_db = Arc::downgrade(&this);
        let _handles = IndexFlusher::start_threads(flusher_receivers, weak_db, metrics);

        // todo: store handles and wait for them on Db drop

        Ok(this)
    }

    pub fn wal_path(path: &Path) -> PathBuf {
        path.join("wal")
    }

    pub fn start_periodic_snapshot(self: &Arc<Self>) {
        // todo account number of bytes read during wal replay
        let position = self.wal_writer.position();
        let weak = Arc::downgrade(self);
        thread::Builder::new()
            .name("snapshot".to_string())
            .spawn(move || Self::periodic_snapshot_thread(weak, position))
            .unwrap();
    }

    fn periodic_snapshot_thread(weak: Weak<Db>, mut position: u64) -> Option<()> {
        loop {
            thread::sleep(Duration::from_secs(60));
            let db = weak.upgrade()?;
            db.large_table.report_entries_state();
            // todo when we get to wal position wrapping around this will need to be fixed
            let current_wal_position = db.wal_writer.position();
            let written = current_wal_position.checked_sub(position).unwrap();
            if written > db.config.snapshot_written_bytes() {
                // todo taint storage instance on failure?
                let snapshot_position = db
                    .rebuild_control_region_from(current_wal_position)
                    .expect("Failed to rebuild control region");
                // Treat WalPosition::INVALID as 0 for accounting purpose
                position = snapshot_position
                    .valid()
                    .map(|p| p.offset())
                    .unwrap_or_default();
            }
        }
    }

    fn read_or_create_control_region(
        path: PathBuf,
        key_shape: &KeyShape,
    ) -> Result<(ControlRegionStore, ControlRegion), DbError> {
        let control_region = ControlRegion::read_or_create(&path, key_shape);
        let control_region_store = ControlRegionStore::new(path);
        Ok((control_region_store, control_region))
    }

    pub fn insert(&self, ks: KeySpace, k: impl Into<Bytes>, v: impl Into<Bytes>) -> DbResult<()> {
        let ks = self.key_shape.ks(ks);
        let _timer = self
            .metrics
            .db_op_mcs
            .with_label_values(&["insert", ks.name()])
            .mcs_timer();
        let k = k.into();
        let v = v.into();
        ks.check_key(&k);
        let w = PreparedWalWrite::new(&WalEntry::Record(ks.id(), k.clone(), v.clone()));
        self.metrics
            .wal_written_bytes_type
            .with_label_values(&["record", ks.name()])
            .inc_by(w.len() as u64);
        let position = self.wal_writer.write(&w)?;
        self.metrics.wal_written_bytes.set(position.offset() as i64);
        let reduced_key = ks.reduced_key_bytes(k);
        self.large_table.insert(ks, reduced_key, position, &v, self);
        Ok(())
    }

    pub fn remove(&self, ks: KeySpace, k: impl Into<Bytes>) -> DbResult<()> {
        let ks = self.key_shape.ks(ks);
        let _timer = self
            .metrics
            .db_op_mcs
            .with_label_values(&["remove", ks.name()])
            .mcs_timer();
        let k = k.into();
        ks.check_key(&k);
        let w = PreparedWalWrite::new(&WalEntry::Remove(ks.id(), k.clone()));
        self.metrics
            .wal_written_bytes_type
            .with_label_values(&["tombstone", ks.name()])
            .inc_by(w.len() as u64);
        let position = self.wal_writer.write(&w)?;
        let reduced_key = ks.reduced_key_bytes(k);
        Ok(self.large_table.remove(ks, reduced_key, position, self)?)
    }

    pub fn get(&self, ks: KeySpace, k: &[u8]) -> DbResult<Option<Bytes>> {
        let ks = self.key_shape.ks(ks);
        let _timer = self
            .metrics
            .db_op_mcs
            .with_label_values(&["get", ks.name()])
            .mcs_timer();
        let reduced_key = ks.reduce_key(k);
        match self.large_table.get(ks, reduced_key.as_ref(), self)? {
            GetResult::Value(value) => {
                // todo check collision ?
                Ok(Some(value))
            }
            GetResult::WalPosition(w) => {
                let value = self.read_record_check_key(k, w)?;
                let Some(value) = value else {
                    return Ok(None);
                };
                self.large_table
                    .update_lru(ks, reduced_key.to_vec().into(), value.clone());
                Ok(Some(value))
            }
            GetResult::NotFound => Ok(None),
        }
    }

    pub fn exists(&self, ks: KeySpace, k: &[u8]) -> DbResult<bool> {
        let ks = self.key_shape.ks(ks);
        let _timer = self
            .metrics
            .db_op_mcs
            .with_label_values(&["exists", ks.name()])
            .mcs_timer();
        // todo check collision ?
        let reduced_key = ks.reduce_key(k);
        Ok(self
            .large_table
            .get(ks, reduced_key.as_ref(), self)?
            .is_found())
    }

    pub fn write_batch(&self, batch: WriteBatch) -> DbResult<()> {
        let WriteBatch {
            writes,
            deletes,
            tag,
        } = batch;
        let _timer = self
            .metrics
            .db_op_mcs
            .with_label_values(&["write_batch", &tag])
            .mcs_timer();
        // todo implement atomic durability
        let mut last_position = WalPosition::INVALID;
        let mut update_writes = Vec::with_capacity(writes.len());
        let _write_timer = self
            .metrics
            .write_batch_times
            .with_label_values(&[&tag, "write"])
            .mcs_timer();
        for w in writes {
            let ks = self.key_shape.ks(w.ks);
            self.metrics
                .wal_written_bytes_type
                .with_label_values(&["record", ks.name()])
                .inc_by(w.wal_write.len() as u64);
            let position = self.wal_writer.write(&w.wal_write)?;
            update_writes.push((w, position));
        }

        let mut update_deletes = Vec::with_capacity(deletes.len());
        for w in deletes {
            let ks = self.key_shape.ks(w.ks);
            self.metrics
                .wal_written_bytes_type
                .with_label_values(&["tombstone", ks.name()])
                .inc_by(w.wal_write.len() as u64);
            let position = self.wal_writer.write(&w.wal_write)?;
            update_deletes.push((w, position));
        }
        drop(_write_timer);
        self.metrics
            .write_batch_operations
            .with_label_values(&[&tag, "put"])
            .inc_by(update_writes.len() as u64);
        self.metrics
            .write_batch_operations
            .with_label_values(&[&tag, "delete"])
            .inc_by(update_deletes.len() as u64);
        let _update_timer = self
            .metrics
            .write_batch_times
            .with_label_values(&[&tag, "update"])
            .mcs_timer();

        for (w, position) in update_writes {
            let ks = self.key_shape.ks(w.ks);
            ks.check_key(&w.key);
            let reduced_key = ks.reduced_key_bytes(w.key);
            self.large_table
                .insert(ks, reduced_key, position, &w.value, self);
            last_position = position;
        }
        for (w, position) in update_deletes {
            let ks = self.key_shape.ks(w.ks);
            ks.check_key(&w.key);
            let reduced_key = ks.reduced_key_bytes(w.key);
            self.large_table.remove(ks, reduced_key, position, self)?;
            last_position = position;
        }
        if last_position != WalPosition::INVALID {
            self.metrics
                .wal_written_bytes
                .set(last_position.offset() as i64);
        }

        Ok(())
    }

    /// Ordered iterator over DB in the specified range
    pub fn iterator(self: &Arc<Self>, ks: KeySpace) -> DbIterator {
        DbIterator::new(self.clone(), ks)
    }

    /// Returns true if this storage is empty.
    ///
    /// (warn) Right now it returns true if storage was never inserted true,
    /// but may return false if entry was inserted and then deleted.
    pub fn is_empty(&self) -> bool {
        self.large_table.is_empty()
    }

    pub fn ks_name(&self, ks: KeySpace) -> &str {
        self.key_shape.ks(ks).name()
    }

    pub(crate) fn ks(&self, ks: KeySpace) -> &KeySpaceDesc {
        self.key_shape.ks(ks)
    }

    /// Returns the next entry in the database, after the specified previous key.
    /// Iterator must specify the cell to inspect and the (Optional) previous key.
    ///
    /// If the prev_key is set to None, the first key in the cell is returned.
    ///
    /// When iterating the entire DB, the iterator starts with cell=0 and prev_key=None.
    ///
    /// The returned values:
    /// (1) the key fetched by the iterator
    /// (2) the value fetched by the iterator
    ///
    /// This function allows concurrent modification of the database.
    /// The function will return the key-value pair next after the deleted key.
    pub(crate) fn next_entry(
        &self,
        ks: KeySpace,
        cell: CellId,
        prev_key: Option<Bytes>,
        end_cell_exclusive: &Option<CellId>,
        reverse: bool,
    ) -> DbResult<Option<IteratorResult<Bytes>>> {
        let ks = self.key_shape.ks(ks);
        let _timer = self
            .metrics
            .db_op_mcs
            .with_label_values(&["next_entry", ks.name()])
            .mcs_timer();
        let Some(result) =
            self.large_table
                .next_entry(ks, cell, prev_key, self, end_cell_exclusive, reverse)?
        else {
            return Ok(None);
        };
        let (key, value) = self.read_record(result.value)?;
        Ok(Some(result.with_key_value(key, value)))
    }

    /// Returns the next cell in the large table
    pub(crate) fn next_cell(
        &self,
        ks: &KeySpaceDesc,
        cell: &CellId,
        reverse: bool,
    ) -> Option<CellId> {
        let _timer = self
            .metrics
            .db_op_mcs
            .with_label_values(&["next_cell", ks.name()])
            .mcs_timer();
        self.large_table.next_cell(ks, cell, reverse)
    }

    pub(crate) fn update_flushed_index(
        &self,
        ks: KeySpace,
        cell: CellId,
        original_index: Arc<IndexTable>,
        position: WalPosition,
    ) {
        let ks = self.key_shape.ks(ks);
        let _timer = self
            .metrics
            .db_op_mcs
            .with_label_values(&["update_flushed_index", ks.name()])
            .mcs_timer();
        self.large_table
            .update_flushed_index(ks, &cell, original_index, position)
    }

    pub(crate) fn load_index(&self, ks: KeySpace, position: WalPosition) -> DbResult<IndexTable> {
        let ks = self.key_shape.ks(ks);
        let entry = self.read_report_entry(position)?;
        Self::read_index(ks, entry)
    }

    fn read_record_check_key(&self, k: &[u8], position: WalPosition) -> DbResult<Option<Bytes>> {
        let (wal_key, v) = self.read_record(position)?;
        if wal_key.as_ref() != k {
            Ok(None)
        } else {
            Ok(Some(v))
        }
    }

    fn read_record(&self, position: WalPosition) -> DbResult<(Bytes, Bytes)> {
        let entry = self.read_report_entry(position)?;
        if let WalEntry::Record(KeySpace(_), k, v) = entry {
            Ok((k, v))
        } else {
            panic!("Unexpected wal entry where expected record");
        }
    }

    fn report_read(&self, entry: &WalEntry, mapped: bool) {
        let (kind, ks) = match entry {
            WalEntry::Record(ks, _, _) => ("record", self.key_shape.ks(*ks).name()),
            WalEntry::Index(ks, _) => ("index", self.key_shape.ks(*ks).name()),
            WalEntry::Remove(ks, _) => ("tombstone", self.key_shape.ks(*ks).name()),
        };
        let mapped = if mapped { "mapped" } else { "unmapped" };
        self.metrics
            .read
            .with_label_values(&[ks, kind, mapped])
            .inc();
        self.metrics
            .read_bytes
            .with_label_values(&[ks, kind, mapped])
            .inc_by(entry.len() as u64);
    }

    fn replay_wal(
        key_shape: &KeyShape,
        large_table: &LargeTable,
        mut wal_iterator: WalIterator,
        metrics: &Metrics,
    ) -> DbResult<WalWriter> {
        loop {
            let entry = wal_iterator.next();
            if matches!(entry, Err(WalError::Crc(_))) {
                break Ok(wal_iterator.into_writer());
            }
            let (position, entry) = entry?;
            let entry = WalEntry::from_bytes(entry);
            match entry {
                WalEntry::Record(ks, k, v) => {
                    metrics.replayed_wal_records.inc();
                    let ks = key_shape.ks(ks);
                    let reduced_key = ks.reduced_key_bytes(k);
                    large_table.insert(ks, reduced_key, position, &v, wal_iterator.wal());
                }
                WalEntry::Index(_ks, _bytes) => {
                    // todo - handle this by updating large table to Loaded()
                }
                WalEntry::Remove(ks, k) => {
                    metrics.replayed_wal_records.inc();
                    let ks = key_shape.ks(ks);
                    let reduced_key = ks.reduced_key_bytes(k);
                    large_table.remove(ks, reduced_key, position, wal_iterator.wal())?;
                }
            }
        }
    }

    #[cfg(test)]
    fn rebuild_control_region(&self) -> DbResult<WalPosition> {
        self.rebuild_control_region_from(self.wal_writer.position())
    }

    fn rebuild_control_region_from(&self, current_wal_position: u64) -> DbResult<WalPosition> {
        let mut crs = self.control_region_store.lock();
        let _timer = self
            .metrics
            .rebuild_control_region_time_mcs
            .clone()
            .mcs_timer();
        let _snapshot_timer = self.metrics.snapshot_lock_time_mcs.clone().mcs_timer();
        let snapshot = self.large_table.snapshot(current_wal_position, self)?;
        self.wal.fsync()?;
        crs.store(snapshot.data, snapshot.last_added_position, &self.metrics);
        Ok(snapshot.last_added_position)
    }

    fn write_index(&self, ks: KeySpace, index: &IndexTable) -> DbResult<WalPosition> {
        let ksd = self.key_shape.ks(ks);
        self.metrics
            .flush_count
            .with_label_values(&[ksd.name()])
            .inc();
        self.metrics
            .flushed_keys
            .with_label_values(&[ksd.name()])
            .inc_by(index.len() as u64);
        let index = ksd.index_format().to_bytes(&index, ksd);
        self.metrics
            .flushed_bytes
            .with_label_values(&[ksd.name()])
            .inc_by(index.len() as u64);
        let w = PreparedWalWrite::new(&WalEntry::Index(ks, index));
        self.metrics
            .wal_written_bytes_type
            .with_label_values(&["index", ksd.name()])
            .inc_by(w.len() as u64);
        Ok(self.wal_writer.write(&w)?)
    }

    fn read_entry(wal: &Wal, position: WalPosition) -> DbResult<(bool, WalEntry)> {
        let (mapped, entry) = wal.read_unmapped(position)?;
        Ok((mapped, WalEntry::from_bytes(entry)))
    }

    fn read_report_entry(&self, position: WalPosition) -> DbResult<WalEntry> {
        let (mapped, entry) = Self::read_entry(&self.wal, position)?;
        self.report_read(&entry, mapped);
        Ok(entry)
    }

    fn read_index(ks: &KeySpaceDesc, entry: WalEntry) -> DbResult<IndexTable> {
        if let WalEntry::Index(_, bytes) = entry {
            let entry = ks.index_format().from_bytes(ks, bytes);
            Ok(entry)
        } else {
            panic!("Unexpected wal entry where expected record");
        }
    }

    fn report_memory_estimates(&self) {
        for ks in self.key_shape.iter_ks() {
            match ks.key_type() {
                KeyType::Uniform(config) => {
                    let num_cells = config.num_cells(ks);
                    let cache_estimate = (ks.index_key_size() + WalPosition::SIZE)
                        * num_cells
                        * self.config.max_dirty_keys;
                    self.metrics
                        .memory_estimate
                        .with_label_values(&[ks.name(), "index_cache"])
                        .set(cache_estimate as i64);
                    if let Some(bloom_filter) = ks.bloom_filter() {
                        let bloom_size = needed_bits(bloom_filter.rate, bloom_filter.count) / 8;
                        let bloom_estimate = bloom_size * num_cells;
                        self.metrics
                            .memory_estimate
                            .with_label_values(&[ks.name(), "bloom"])
                            .set(bloom_estimate as i64);
                    }
                }
                KeyType::PrefixedUniform(_) => {
                    // todo report actual values for memory usage since we can't have estimates here
                }
            }
        }
        let maps_estimate = (self.config.max_maps as u64) * self.config.frag_size;
        self.metrics
            .memory_estimate
            .with_label_values(&["_", "maps"])
            .set(maps_estimate as i64);
    }

    /// Create a snapshot of the current db state
    pub fn create_state_snapshot(&self, snapshot_path: PathBuf) -> DbResult<()> {
        let guard = self.control_region_store.lock();
        let control_region_path = guard.path().clone();
        drop(guard);
        let wal_position = self.wal_writer.position();
        state_snapshot::create(&wal_position, &control_region_path, snapshot_path)
    }

    /// Restore the database from a snapshot
    pub fn restore_state_snapshot(
        snapshot_path: PathBuf,
        database_path: PathBuf,
        key_shape: KeyShape,
        config: Arc<Config>,
        metrics: Arc<Metrics>,
    ) -> DbResult<Arc<Self>> {
        state_snapshot::load(snapshot_path, database_path, key_shape, config, metrics)
    }
}

impl Loader for Wal {
    type Error = DbError;

    fn load(&self, ks: &KeySpaceDesc, position: WalPosition) -> DbResult<IndexTable> {
        let (_, entry) = Db::read_entry(self, position)?;
        Db::read_index(ks, entry)
    }

    fn index_reader(&self, position: WalPosition) -> Result<WalRandomRead, Self::Error> {
        Ok(self.random_reader_at(position, WalEntry::INDEX_PREFIX_SIZE)?)
    }

    fn flush_supported(&self) -> bool {
        false
    }

    fn flush(&self, _ks: KeySpace, _data: &IndexTable) -> DbResult<WalPosition> {
        unimplemented!()
    }
}

impl Loader for Db {
    type Error = DbError;

    fn load(&self, ks: &KeySpaceDesc, position: WalPosition) -> DbResult<IndexTable> {
        let entry = self.read_report_entry(position)?;
        Self::read_index(ks, entry)
    }

    fn index_reader(&self, position: WalPosition) -> Result<WalRandomRead, Self::Error> {
        Ok(self
            .wal
            .random_reader_at(position, WalEntry::INDEX_PREFIX_SIZE)?)
    }

    fn flush_supported(&self) -> bool {
        true
    }

    fn flush(&self, ks: KeySpace, data: &IndexTable) -> DbResult<WalPosition> {
        self.write_index(ks, data)
    }
}

pub(crate) enum WalEntry {
    Record(KeySpace, Bytes, Bytes),
    Index(KeySpace, Bytes),
    Remove(KeySpace, Bytes),
}

#[derive(Debug)]
pub enum DbError {
    Io(io::Error),
    CrCorrupted,
    WalError(WalError),
    CorruptedIndexEntry(bincode::Error),
}

impl WalEntry {
    const WAL_ENTRY_RECORD: u8 = 1;
    const WAL_ENTRY_INDEX: u8 = 2;
    const WAL_ENTRY_REMOVE: u8 = 3;
    pub const INDEX_PREFIX_SIZE: usize = 2;

    pub fn from_bytes(bytes: Bytes) -> Self {
        let mut b = &bytes[..];
        let entry_type = b.get_u8();
        match entry_type {
            WalEntry::WAL_ENTRY_RECORD => {
                let ks = KeySpace(b.get_u8());
                let key_len = b.get_u16() as usize;
                let k = bytes.slice(4..4 + key_len);
                let v = bytes.slice(4 + key_len..);
                WalEntry::Record(ks, k, v)
            }
            WalEntry::WAL_ENTRY_INDEX => {
                let ks = KeySpace(b.get_u8());
                WalEntry::Index(ks, bytes.slice(2..))
            }
            WalEntry::WAL_ENTRY_REMOVE => {
                let ks = KeySpace(b.get_u8());
                WalEntry::Remove(ks, bytes.slice(2..))
            }
            _ => panic!("Unknown wal entry type {entry_type}"),
        }
    }
}

impl IntoBytesFixed for WalEntry {
    fn len(&self) -> usize {
        match self {
            WalEntry::Record(KeySpace(_), k, v) => 1 + 1 + 2 + k.len() + v.len(),
            WalEntry::Index(KeySpace(_), index) => 1 + 1 + index.len(),
            WalEntry::Remove(KeySpace(_), k) => 1 + 1 + k.len(),
        }
    }

    fn write_into_bytes(&self, buf: &mut BytesMut) {
        // todo avoid copy here
        match self {
            WalEntry::Record(ks, k, v) => {
                buf.put_u8(Self::WAL_ENTRY_RECORD);
                buf.put_u8(ks.0);
                // todo use key len from ks instead
                buf.put_u16(k.len() as u16);
                buf.put_slice(&k);
                buf.put_slice(&v);
            }
            WalEntry::Index(ks, bytes) => {
                buf.put_u8(Self::WAL_ENTRY_INDEX);
                buf.put_u8(ks.0);
                buf.put_slice(&bytes);
            }
            WalEntry::Remove(ks, k) => {
                buf.put_u8(Self::WAL_ENTRY_REMOVE);
                buf.put_u8(ks.0);
                buf.put_slice(&k)
            }
        }
    }
}

impl From<io::Error> for DbError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<WalError> for DbError {
    fn from(value: WalError) -> Self {
        Self::WalError(value)
    }
}

impl From<bincode::Error> for DbError {
    fn from(value: bincode::Error) -> Self {
        Self::CorruptedIndexEntry(value)
    }
}

#[cfg(test)]
#[path = "db_tests/generated.rs"]
mod tests;

#[cfg(test)]
mod multi_flusher_tests {
    use super::*;
    use crate::key_shape::{KeyShapeBuilder, KeyType};
    use tempdir::TempDir;

    #[test]
    fn test_multi_flusher_threads() {
        let dir = TempDir::new("test_multi_flusher").unwrap();

        // Test with different numbers of flusher threads
        for num_threads in [1, 2, 4, 8] {
            let mut config = Config::small();
            config.num_flusher_threads = num_threads;

            let mut builder = KeyShapeBuilder::new();
            builder.add_key_space("test", 8, 16, KeyType::uniform(16));
            let key_shape = builder.build();

            let db = Db::open(dir.path(), key_shape, Arc::new(config), Metrics::new()).unwrap();

            // Perform some operations to ensure the DB works
            for i in 0..100 {
                let key = format!("key{:05}", i);
                let value = format!("value{}", i);
                db.insert(KeySpace(0), key, value).unwrap();
            }

            // Wait for all flusher threads to finish
            db.large_table.flusher.barrier();

            // Verify we can read back the data
            for i in 0..100 {
                let key = format!("key{:05}", i);
                let value = db.get(KeySpace(0), key.as_bytes()).unwrap();
                assert!(value.is_some());
                assert_eq!(value.unwrap().as_ref(), format!("value{}", i).as_bytes());
            }

            drop(db);
        }
    }
}
