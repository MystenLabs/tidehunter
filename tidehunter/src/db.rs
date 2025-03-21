use crate::batch::WriteBatch;
use crate::cell::CellId;
use crate::config::Config;
use crate::control::ControlRegion;
use crate::crc::{CrcFrame, CrcReadError, IntoBytesFixed};
use crate::flusher::IndexFlusher;
use crate::index::index_format::IndexFormat;
use crate::index::index_table::IndexTable;
use crate::index::INDEX_FORMAT;
use crate::iterators::db_iterator::DbIterator;
use crate::key_shape::{KeyShape, KeySpace, KeySpaceDesc};
use crate::large_table::{GetResult, LargeTable, LargeTableContainer, Loader, Version};
use crate::metrics::{Metrics, TimerExt};
use crate::wal::{
    PreparedWalWrite, Wal, WalError, WalIterator, WalPosition, WalRandomRead, WalWriter,
};
use bloom::needed_bits;
use bytes::{Buf, BufMut, BytesMut};
use memmap2::{MmapMut, MmapOptions};
use minibytes::Bytes;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
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

impl Db {
    pub fn open(
        path: &Path,
        key_shape: KeyShape,
        config: Arc<Config>,
        metrics: Arc<Metrics>,
    ) -> DbResult<Arc<Self>> {
        let cr = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.join("cr"))?;
        let (control_region_store, control_region) =
            Self::read_or_create_control_region(&cr, &key_shape)?;
        let (flusher_sender, flusher_receiver) = mpsc::channel();
        let flusher = IndexFlusher::new(flusher_sender);
        let large_table = LargeTable::from_unloaded(
            &key_shape,
            control_region.snapshot(),
            config.clone(),
            flusher,
            metrics.clone(),
        );
        let wal = Wal::open(&Self::wal_path(path), config.wal_layout(), metrics.clone())?;
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
        // todo wait for jh on Db drop
        let _jh = IndexFlusher::start_thread(flusher_receiver, Arc::downgrade(&this), metrics);
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
            thread::sleep(Duration::from_secs(30));
            let db = weak.upgrade()?;
            db.large_table.report_entries_state();
            // todo when we get to wal position wrapping around this will need to be fixed
            let current_wal_position = db.wal_writer.wal_position();
            let written = current_wal_position.as_u64().checked_sub(position).unwrap();
            if written > db.config.snapshot_written_bytes() {
                // todo taint storage instance on failure?
                let snapshot_position = db
                    .rebuild_control_region_from(current_wal_position)
                    .expect("Failed to rebuild control region");
                // Treat WalPosition::INVALID as 0 for accounting purpose
                position = snapshot_position
                    .valid()
                    .map(|p| p.as_u64())
                    .unwrap_or_default();
            }
        }
    }

    fn read_or_create_control_region(
        cr: &File,
        key_shape: &KeyShape,
    ) -> Result<(ControlRegionStore, ControlRegion), DbError> {
        let file_len = cr.metadata()?.len() as usize;
        let cr_len = key_shape.cr_len();
        let mut cr_map = unsafe { MmapOptions::new().len(cr_len * 2).map_mut(cr)? };
        let (last_written_left, control_region) = if file_len != cr_len * 2 {
            cr.set_len((cr_len * 2) as u64)?;
            let skip_marker = CrcFrame::skip_marker();
            cr_map[0..skip_marker.len_with_header()].copy_from_slice(skip_marker.as_ref());
            cr_map.flush()?;
            (false, ControlRegion::new_empty(key_shape))
        } else {
            Self::read_control_region(&cr_map, key_shape)?
        };
        let control_region_store = ControlRegionStore {
            cr_map,
            last_written_left,
            last_version: control_region.version(),
        };
        Ok((control_region_store, control_region))
    }

    fn read_control_region(
        cr_map: &MmapMut,
        key_shape: &KeyShape,
    ) -> Result<(bool, ControlRegion), DbError> {
        let cr_len = key_shape.cr_len();
        assert_eq!(cr_map.len(), cr_len * 2);
        let cr1 = CrcFrame::read_from_slice(&cr_map, 0);
        let cr2 = CrcFrame::read_from_slice(&cr_map, cr_len);
        let (last_written_left, cr) = match (cr1, cr2) {
            (Ok(cr1), Err(_)) => (true, cr1),
            (Err(_), Ok(cr2)) => (false, cr2),
            (Ok(cr1), Ok(cr2)) => {
                let version1 = ControlRegion::version_from_bytes(cr1);
                let version2 = ControlRegion::version_from_bytes(cr2);
                if version1 > version2 {
                    (true, cr1)
                } else {
                    (false, cr2)
                }
            }
            // Cr region is valid but empty
            (Err(CrcReadError::SkipMarker), Err(_)) => {
                return Ok((false, ControlRegion::new_empty(key_shape)))
            }
            (Err(_), Err(_)) => return Err(DbError::CrCorrupted),
        };
        let control_region = ControlRegion::from_slice(&cr, key_shape);
        Ok((last_written_left, control_region))
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
            .with_label_values(&["record"])
            .inc_by(w.len() as u64);
        let position = self.wal_writer.write(&w)?;
        self.metrics.wal_written_bytes.set(position.as_u64() as i64);
        let reduced_key = ks.reduced_key_bytes(k);
        self.large_table
            .insert(ks, reduced_key, position, &v, self)?;
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
            .with_label_values(&["tombstone"])
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
        match self.large_table.get(ks, reduced_key, self)? {
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
        Ok(self.large_table.get(ks, reduced_key, self)?.is_found())
    }

    pub fn write_batch(&self, batch: WriteBatch) -> DbResult<()> {
        // todo implement atomic durability
        let WriteBatch { writes, deletes } = batch;
        let mut last_position = WalPosition::INVALID;
        for w in writes {
            self.metrics
                .wal_written_bytes_type
                .with_label_values(&["record"])
                .inc_by(w.wal_write.len() as u64);
            let position = self.wal_writer.write(&w.wal_write)?;
            let ks = self.key_shape.ks(w.ks);
            ks.check_key(&w.key);
            let reduced_key = ks.reduced_key_bytes(w.key);
            self.large_table
                .insert(ks, reduced_key, position, &w.value, self)?;
            last_position = position;
        }

        for w in deletes {
            self.metrics
                .wal_written_bytes_type
                .with_label_values(&["tombstone"])
                .inc_by(w.wal_write.len() as u64);
            let position = self.wal_writer.write(&w.wal_write)?;
            let ks = self.key_shape.ks(w.ks);
            ks.check_key(&w.key);
            let reduced_key = ks.reduced_key_bytes(w.key);
            self.large_table.remove(ks, reduced_key, position, self)?;
            last_position = position;
        }
        if last_position != WalPosition::INVALID {
            self.metrics
                .wal_written_bytes
                .set(last_position.as_u64() as i64);
        }

        Ok(())
    }

    /// Ordered iterator over DB in the specified range
    pub fn iterator(self: &Arc<Self>, ks: KeySpace) -> DbIterator {
        DbIterator::new(self.clone(), ks)
    }

    /// Returns last key-value pair in the given range, where both ends of the range are included
    ///
    /// Both start and end of the range should have the same first 4 bytes,
    /// otherwise this function panics.
    pub fn last_in_range(
        &self,
        ks: KeySpace,
        full_from_included: &Bytes,
        full_to_included: &Bytes,
    ) -> DbResult<Option<(Bytes, Bytes)>> {
        let ks = self.key_shape.ks(ks);
        let _timer = self
            .metrics
            .db_op_mcs
            .with_label_values(&["last_in_range", ks.name()])
            .mcs_timer();
        // todo use reduced_key instead of reduced_key_bytes
        let from_included = ks.reduced_key_bytes(full_from_included.clone());
        let to_included = ks.reduced_key_bytes(full_to_included.clone());
        let cell = self
            .key_shape
            .range_cell(ks.id(), &from_included, &to_included);
        let Some((_key, position)) =
            self.large_table
                .last_in_range(ks, &cell, &from_included, &to_included, self)?
        else {
            return Ok(None);
        };
        let (key, value) = self.read_record(position)?;
        if key <= full_to_included && key >= full_from_included {
            Ok(Some((key, value)))
        } else {
            Ok(None)
        }
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

    /// Returns the next entry in the database.
    /// Iterator must specify the cell to inspect and the (Optional) next key.
    ///
    /// If the next_key is set to None, the first key in the cell is returned.
    ///
    /// When iterating the entire DB, the iterator starts with cell=0 and next_key=None.
    ///
    /// The returned values:
    /// (1) Next cell to read, None if iterator has reached the end of the DB.
    /// (2) Next key to read.
    ///     This value should be passed as is to next call of next_entry.
    ///     None here **does not** mean iteration has ended.
    /// (3) the key fetched by the iterator
    /// (4) the value fetched by the iterator
    ///
    /// This function allows concurrent modification of the database.
    /// If next_key is deleted,
    /// the function will return the key-value pair next after the deleted key.
    ///
    /// As such, the returned key might not match the value passed in the next_key.
    pub(crate) fn next_entry(
        &self,
        ks: KeySpace,
        cell: CellId,
        next_key: Option<Bytes>,
        end_cell_exclusive: &Option<CellId>,
        reverse: bool,
    ) -> DbResult<
        Option<(
            Option<CellId>, /*next cell*/
            Option<Bytes>,  /*next key*/
            Bytes,          /*fetched key*/
            Bytes,          /*fetched value*/
        )>,
    > {
        let ks = self.key_shape.ks(ks);
        let _timer = self
            .metrics
            .db_op_mcs
            .with_label_values(&["next_entry", ks.name()])
            .mcs_timer();
        let Some((next_cell, next_key, _key, wal_position)) =
            self.large_table
                .next_entry(ks, cell, next_key, self, end_cell_exclusive, reverse)?
        else {
            return Ok(None);
        };
        let (key, value) = self.read_record(wal_position)?;
        Ok(Some((next_cell, next_key, key, value)))
    }

    pub(crate) fn update_flushed_index(
        &self,
        ks: KeySpace,
        cell: CellId,
        original_index: Arc<IndexTable>,
        position: WalPosition,
    ) {
        let ks = self.key_shape.ks(ks);
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
                    large_table.insert(ks, reduced_key, position, &v, wal_iterator.wal())?;
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
        self.rebuild_control_region_from(self.wal_writer.wal_position())
    }

    fn rebuild_control_region_from(
        &self,
        current_wal_position: WalPosition,
    ) -> DbResult<WalPosition> {
        let mut crs = self.control_region_store.lock();
        let _timer = self
            .metrics
            .rebuild_control_region_time_mcs
            .clone()
            .mcs_timer();
        let _snapshot_timer = self.metrics.snapshot_lock_time_mcs.clone().mcs_timer();
        let snapshot = self
            .large_table
            .snapshot(current_wal_position.as_u64(), self)?;
        // todo fsync wal first
        crs.store(snapshot.data, snapshot.last_added_position, &self.metrics)?;
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
        let index = INDEX_FORMAT.to_bytes(&index, ksd);
        self.metrics
            .flushed_bytes
            .with_label_values(&[ksd.name()])
            .inc_by(index.len() as u64);
        let w = PreparedWalWrite::new(&WalEntry::Index(ks, index));
        self.metrics
            .wal_written_bytes_type
            .with_label_values(&["index"])
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
            let entry = INDEX_FORMAT.from_bytes(ks, bytes);
            Ok(entry)
        } else {
            panic!("Unexpected wal entry where expected record");
        }
    }

    fn report_memory_estimates(&self) {
        for ks in self.key_shape.iter_ks() {
            let cache_estimate = (ks.reduced_key_size() + WalPosition::SIZE)
                * ks.num_cells()
                * self.config.max_dirty_keys;
            self.metrics
                .memory_estimate
                .with_label_values(&[ks.name(), "index_cache"])
                .set(cache_estimate as i64);
            if let Some(bloom_filter) = ks.bloom_filter() {
                let bloom_size = needed_bits(bloom_filter.rate, bloom_filter.count) / 8;
                let bloom_estimate = bloom_size * ks.num_cells();
                self.metrics
                    .memory_estimate
                    .with_label_values(&[ks.name(), "bloom"])
                    .set(bloom_estimate as i64);
            }
        }
        let maps_estimate = (self.config.max_maps as u64) * self.config.frag_size;
        self.metrics
            .memory_estimate
            .with_label_values(&["_", "maps"])
            .set(maps_estimate as i64);
    }
}

struct ControlRegionStore {
    cr_map: MmapMut,
    last_version: Version,
    last_written_left: bool,
}

impl ControlRegionStore {
    pub fn store(
        &mut self,
        snapshot: LargeTableContainer<WalPosition>,
        last_position: WalPosition,
        metrics: &Metrics,
    ) -> DbResult<()> {
        let control_region = ControlRegion::new(snapshot, self.increment_version(), last_position);
        let frame = CrcFrame::new(&control_region);
        assert_eq!(frame.len_with_header() * 2, self.cr_map.len());
        let write_to = if self.last_written_left {
            // write right
            &mut self.cr_map[frame.len_with_header()..]
        } else {
            // write left
            &mut self.cr_map[..frame.len_with_header()]
        };
        write_to.copy_from_slice(frame.as_ref());
        metrics.snapshot_written_bytes.inc_by(write_to.len() as u64);
        self.cr_map.flush()?;
        self.last_written_left = !self.last_written_left;
        Ok(())
    }

    fn increment_version(&mut self) -> Version {
        self.last_version.checked_increment();
        self.last_version
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
mod test {
    use super::*;
    use crate::key_shape::{KeyShapeBuilder, KeySpaceConfig};
    use rand::rngs::ThreadRng;
    use rand::Rng;

    #[test]
    fn db_test() {
        let dir = tempdir::TempDir::new("test-wal").unwrap();
        let config = Arc::new(Config::small());
        let (key_shape, ks) = KeyShape::new_single(4, 12, 12);
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap();
            db.insert(ks, vec![1, 2, 3, 4], vec![5, 6]).unwrap();
            db.insert(ks, vec![3, 4, 5, 6], vec![7]).unwrap();
            assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![7].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
        }
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                force_unload_config(&config),
                Metrics::new(),
            )
            .unwrap();
            assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![7].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
            db.rebuild_control_region().unwrap();
            assert!(
                db.large_table.is_all_clean(),
                "Some entries are not clean after snapshot"
            );
        }
        {
            let metrics = Metrics::new();
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                metrics.clone(),
            )
            .unwrap();
            // nothing replayed from wal since we just rebuilt the control region
            assert_eq!(metrics.replayed_wal_records.get(), 0);
            assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![7].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
            db.insert(ks, vec![3, 4, 5, 6], vec![8]).unwrap();
            assert_eq!(Some(vec![8].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
        }
        {
            let metrics = Metrics::new();
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                metrics.clone(),
            )
            .unwrap();
            assert_eq!(metrics.replayed_wal_records.get(), 1);
            assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![8].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
        }
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new().clone(),
            )
            .unwrap();
            db.insert(ks, vec![3, 4, 5, 6], vec![9]).unwrap();
            assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![9].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
        }
    }

    #[test]
    fn test_multi_thread_write() {
        let dir = tempdir::TempDir::new("test-batch").unwrap();
        let config = Config::small();
        let config = Arc::new(config);
        let (key_shape, ks) = KeyShape::new_single(8, 12, 12);
        let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
        let threads = 8u64;
        let mut jhs = Vec::with_capacity(threads as usize);
        let iterations = 256u64;
        for t in 0..threads {
            let db = db.clone();
            let jh = thread::spawn(move || {
                for i in 0..iterations {
                    let key = (t << 16) + i;
                    let value = (i << 16) + t;
                    db.insert(ks, key.to_be_bytes().to_vec(), value.to_be_bytes().to_vec())
                        .unwrap();
                }
            });
            jhs.push(jh);
        }
        for jh in jhs {
            jh.join().unwrap();
        }
        for t in 0..threads {
            for i in 0..iterations {
                let key = (t << 16) + i;
                let expected_value = (i << 16) + t;
                let expected_value = expected_value.to_be_bytes();
                let value = db.get(ks, &key.to_be_bytes()).unwrap();
                let value = value.unwrap();
                assert_eq!(&expected_value, value.as_ref());
            }
        }
    }

    #[test]
    fn test_batch() {
        let dir = tempdir::TempDir::new("test-batch").unwrap();
        let config = Arc::new(Config::small());
        let (key_shape, ks) = KeyShape::new_single(4, 12, 12);
        let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
        let mut batch = WriteBatch::new();
        batch.write(ks, vec![5, 6, 7, 8], vec![15]);
        batch.write(ks, vec![6, 7, 8, 9], vec![17]);
        db.write_batch(batch).unwrap();
        assert_eq!(Some(vec![15].into()), db.get(ks, &[5, 6, 7, 8]).unwrap());
        assert_eq!(Some(vec![17].into()), db.get(ks, &[6, 7, 8, 9]).unwrap());
    }

    #[test]
    fn test_remove() {
        let dir = tempdir::TempDir::new("test-remove").unwrap();
        let config = Arc::new(Config::small());
        let (key_shape, ks) = KeyShape::new_single(4, 12, 12);
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap();
            db.insert(ks, vec![1, 2, 3, 4], vec![5, 6]).unwrap();
            db.insert(ks, vec![3, 4, 5, 6], vec![7]).unwrap();
            assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![7].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
            db.remove(ks, vec![1, 2, 3, 4]).unwrap();
            assert_eq!(None, db.get(ks, &[1, 2, 3, 4]).unwrap());
            db.remove(ks, vec![1, 2, 3, 4]).unwrap();
            assert_eq!(None, db.get(ks, &[1, 2, 3, 4]).unwrap());
        }
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                force_unload_config(&config),
                Metrics::new(),
            )
            .unwrap();
            assert_eq!(None, db.get(ks, &[1, 2, 3, 4]).unwrap());
            db.insert(ks, vec![1, 2, 3, 4], vec![9, 10]).unwrap();
            assert_eq!(Some(vec![7].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
            assert_eq!(Some(vec![9, 10].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
            db.rebuild_control_region().unwrap();
            db.remove(ks, vec![1, 2, 3, 4]).unwrap();
        }
        {
            let metrics = Metrics::new();
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                metrics.clone(),
            )
            .unwrap();
            assert_eq!(metrics.replayed_wal_records.get(), 1);
            assert_eq!(None, db.get(ks, &[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![7].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
        }
    }

    #[test]
    fn test_iterator() {
        let dir = tempdir::TempDir::new("test-iterator").unwrap();
        let config = Arc::new(Config::small());
        let mut data = Vec::with_capacity(1024);
        let (key_shape, ks) = KeyShape::new_single(4, 4, 4);
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap();
            let mut it = db.iterator(ks);
            assert!(it.next().is_none());
            for v in 0..1024u32 {
                let v = v * 3;
                let k = ku32(v);
                let v = vu32(v);
                data.push((k.clone(), v.clone()));
                db.insert(ks, k, v).unwrap();
            }
            let it = db.iterator(ks);
            let s: DbResult<Vec<_>> = it.collect();
            let s = s.unwrap();
            assert_eq!(s, data);
        }
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap();
            let it = db.iterator(ks);
            let s: DbResult<Vec<_>> = it.collect();
            let s = s.unwrap();
            assert_eq!(s, data);

            let mut it = db.iterator(ks);
            it.set_lower_bound(ku32(6));
            assert_eq!((ku32(6), vu32(6)), it.next().unwrap().unwrap());

            let mut it = db.iterator(ks);
            it.set_lower_bound(ku32(7));
            assert_eq!((ku32(9), vu32(9)), it.next().unwrap().unwrap());

            let mut it = db.iterator(ks);
            it.set_lower_bound(ku32(1024 * 3));
            assert!(it.next().is_none());

            let mut it = db.iterator(ks);
            it.set_lower_bound(ku32(12));
            it.set_upper_bound(ku32(16));
            assert_eq!((ku32(12), vu32(12)), it.next().unwrap().unwrap());
            assert_eq!((ku32(15), vu32(15)), it.next().unwrap().unwrap());
            assert!(it.next().is_none());

            let mut it = db.iterator(ks);
            it.set_lower_bound(ku32(12));
            it.set_upper_bound(ku32(15));
            assert_eq!((ku32(12), vu32(12)), it.next().unwrap().unwrap());
            assert!(it.next().is_none());

            // Reverse iterator
            let mut it = db.iterator(ks);
            it.set_lower_bound(ku32(12));
            it.set_upper_bound(ku32(15));
            it.reverse();
            assert_eq!((ku32(12), vu32(12)), it.next().unwrap().unwrap());
            assert!(it.next().is_none());
        }
    }

    #[test]
    fn test_iterator_gen() {
        let sequential = Vec::from_iter(125u128..1125);
        let mut random = sequential.clone();
        ThreadRng::default().fill(&mut random[..]);
        random.sort();
        for reduced in [true, false] {
            let ks_config = if reduced {
                KeySpaceConfig::default()
            } else {
                KeySpaceConfig::default().with_key_reduction(0..16)
            };
            println!("Starting sequential test, reduced={reduced}");
            test_iterator_run(sequential.clone(), ks_config.clone());
            println!("Starting random test, reduced={reduced}");
            test_iterator_run(random.clone(), ks_config);
        }
    }

    fn test_iterator_run(data: Vec<u128>, ks_config: KeySpaceConfig) {
        let dir = tempdir::TempDir::new("test-iterator").unwrap();
        let config = Arc::new(Config::small());
        let (key_shape, ks) = KeyShape::new_single_config(16, 4, 4, ks_config);
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        for k in &data {
            db.insert(ks, ku128(*k), vu128(*k)).unwrap();
        }
        let mut rng = ThreadRng::default();
        for reverse in [true, false] {
            println!("Testing with reverse={reverse}");
            for _ in 0..128 {
                let from = rng.gen_range(0..data.len() - 1);
                let to = rng.gen_range(from + 1..data.len());
                test_iterator_slice(&db, ks, &data[from..to], reverse);
            }
        }
    }

    fn test_iterator_slice(db: &Arc<Db>, ks: KeySpace, slice: &[u128], reverse: bool) {
        let mut iterator = db.iterator(ks);
        iterator.set_lower_bound(ku128(slice[0]));
        iterator.set_upper_bound(ku128(slice[slice.len() - 1] + 1));
        if reverse {
            iterator.reverse();
        }
        let data: Vec<_> = iterator.collect::<DbResult<_>>().unwrap();
        assert_eq!(data.len(), slice.len());
        let slice_iter: Box<dyn Iterator<Item = &u128>> = if reverse {
            Box::new(slice.into_iter().rev())
        } else {
            Box::new(slice.into_iter())
        };
        for ((key, value), expected) in data.into_iter().zip(slice_iter) {
            assert_eq!(key, ku128(*expected));
            assert_eq!(value, vu128(*expected));
        }
    }

    #[test]
    fn test_ordered_iterator() {
        let dir = tempdir::TempDir::new("test-ordered-iterator").unwrap();
        let config = Arc::new(Config::small());
        let (key_shape, ks) = KeyShape::new_single(5, 12, 12);
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap();
            let mut it = db.iterator(ks);
            assert!(it.next().is_none());
            db.insert(ks, vec![1, 2, 3, 4, 6], vec![1]).unwrap();
            db.insert(ks, vec![1, 2, 3, 4, 5], vec![2]).unwrap();
            db.insert(ks, vec![1, 2, 3, 4, 10], vec![3]).unwrap();
            db.insert(ks, vec![3, 4, 5, 6, 11], vec![7]).unwrap();
            let mut it = db.iterator(ks);
            it.set_lower_bound(vec![1, 2, 3, 4, 0]);
            it.set_upper_bound(vec![1, 2, 3, 4, 10]);
            let v: DbResult<Vec<_>> = it.collect();
            let v = v.unwrap();
            assert_eq!(v.len(), 2);
            assert_eq!(
                v.get(0).unwrap(),
                &(vec![1, 2, 3, 4, 5].into(), vec![2].into())
            );
            assert_eq!(
                v.get(1).unwrap(),
                &(vec![1, 2, 3, 4, 6].into(), vec![1].into())
            );
        }
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap();
            let mut it = db.iterator(ks);
            it.set_lower_bound(vec![1, 2, 3, 4, 0]);
            it.set_upper_bound(vec![1, 2, 3, 4, 10]);
            let v: DbResult<Vec<_>> = it.collect();
            let v = v.unwrap();
            assert_eq!(v.len(), 2);
            assert_eq!(
                v.get(0).unwrap(),
                &(vec![1, 2, 3, 4, 5].into(), vec![2].into())
            );
            assert_eq!(
                v.get(1).unwrap(),
                &(vec![1, 2, 3, 4, 6].into(), vec![1].into())
            );
        }
    }

    #[test]
    fn test_empty() {
        let dir = tempdir::TempDir::new("test-empty").unwrap();
        let config = Arc::new(Config::small());
        let (key_shape, ks) = KeyShape::new_single(5, 12, 12);
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap();
            assert!(db.is_empty());
            db.insert(ks, vec![1, 2, 3, 4, 0], vec![1]).unwrap();
            assert!(!db.is_empty());
        }
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap();
            assert!(!db.is_empty());
        }
    }

    #[test]
    fn test_small_keys() {
        let dir = tempdir::TempDir::new("test-small-keys").unwrap();
        let config = Arc::new(Config::small());
        let mut ksb = KeyShapeBuilder::new();
        let ks0 = ksb.add_key_space("a", 0, 12, 12);
        let ks1 = ksb.add_key_space("b", 1, 12, 12);
        let ks2 = ksb.add_key_space("c", 2, 12, 12);
        let _ks3 = ksb.add_key_space("d", 3, 12, 12);
        let key_shape = ksb.build();
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap();
            db.insert(ks0, vec![], vec![1]).unwrap();
            db.insert(ks1, vec![1], vec![2]).unwrap();
            db.insert(ks2, vec![1, 2], vec![3]).unwrap();
            assert_eq!(db.get(ks0, &[]).unwrap(), Some(vec![1].into()));
            assert_eq!(db.get(ks1, &[1]).unwrap(), Some(vec![2].into()));
            assert_eq!(db.get(ks2, &[1, 2]).unwrap(), Some(vec![3].into()));
        }
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap();
            assert_eq!(db.get(ks0, &[]).unwrap(), Some(vec![1].into()));
            assert_eq!(db.get(ks1, &[1]).unwrap(), Some(vec![2].into()));
            assert_eq!(db.get(ks2, &[1, 2]).unwrap(), Some(vec![3].into()));
        }
    }

    #[test]
    fn test_last_in_range() {
        let dir = tempdir::TempDir::new("test-last-in-range").unwrap();
        let config = Arc::new(Config::small());
        let (key_shape, ks) = KeyShape::new_single(5, 12, 12);
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        db.insert(ks, vec![1, 2, 3, 4, 6], vec![1]).unwrap();
        db.insert(ks, vec![1, 2, 3, 4, 5], vec![2]).unwrap();
        db.insert(ks, vec![1, 2, 3, 4, 10], vec![3]).unwrap();
        assert_eq!(
            db.last_in_range(ks, &vec![1, 2, 3, 4, 5].into(), &vec![1, 2, 3, 4, 8].into())
                .unwrap(),
            Some((vec![1, 2, 3, 4, 6].into(), vec![1].into()))
        );
        assert_eq!(
            db.last_in_range(ks, &vec![1, 2, 3, 4, 5].into(), &vec![1, 2, 3, 4, 6].into())
                .unwrap(),
            Some((vec![1, 2, 3, 4, 6].into(), vec![1].into()))
        );
        assert_eq!(
            db.last_in_range(ks, &vec![1, 2, 3, 4, 5].into(), &vec![1, 2, 3, 4, 5].into())
                .unwrap(),
            Some((vec![1, 2, 3, 4, 5].into(), vec![2].into()))
        );
        assert_eq!(
            db.last_in_range(ks, &vec![1, 2, 3, 4, 4].into(), &vec![1, 2, 3, 4, 4].into())
                .unwrap(),
            None
        );
    }

    #[test]
    fn test_value_cache() {
        let dir = tempdir::TempDir::new("test-value-cache").unwrap();
        let config = Arc::new(Config::small());
        let mut ksb = KeyShapeBuilder::new();
        let ksc = KeySpaceConfig::new().with_value_cache_size(512);
        let ks = ksb.add_key_space_config("k", 8, 1, 1, ksc);
        let key_shape = ksb.build();
        let metrics = Metrics::new();
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();

        for i in 0..1024u64 {
            db.insert(ks, i.to_be_bytes().to_vec(), vec![]).unwrap();
        }
        for i in (0..1024u64).rev() {
            assert!(db.get(ks, &i.to_be_bytes()).unwrap().is_some());
        }

        let found_lru = metrics
            .lookup_result
            .with_label_values(&["k", "found", "lru"])
            .get();

        assert_eq!(found_lru, 512);
    }

    #[test]
    fn test_bloom_filter() {
        let dir = tempdir::TempDir::new("test-bloom-filter").unwrap();
        let config = Arc::new(Config::small());
        let mut ksb = KeyShapeBuilder::new();
        let ksc = KeySpaceConfig::new().with_bloom_filter(0.01, 2000);
        let ks = ksb.add_key_space_config("k", 8, 1, 1, ksc);
        let key_shape = ksb.build();
        let metrics = Metrics::new();
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();

        for i in 0..1000u64 {
            db.insert(ks, i.to_be_bytes().to_vec(), vec![]).unwrap();
        }

        for i in 0..1000u64 {
            assert!(db.exists(ks, &i.to_be_bytes()).unwrap());
        }

        for i in 1000..2000u64 {
            assert!(!db.exists(ks, &i.to_be_bytes()).unwrap());
        }
        let found = metrics
            .lookup_result
            .with_label_values(&["k", "found", "cache"])
            .get()
            + metrics
                .lookup_result
                .with_label_values(&["k", "found", "lookup"])
                .get();
        let not_found_bloom = metrics
            .lookup_result
            .with_label_values(&["k", "not_found", "bloom"])
            .get();

        assert_eq!(found, 1000);
        if not_found_bloom < 900 {
            panic!("Bloom filter efficiency less then 90%");
        }
    }

    #[test]
    fn test_dirty_unloading() {
        let dir = tempdir::TempDir::new("test-dirty-unloading").unwrap();
        let mut config = Config::small();
        config.max_dirty_keys = 2;
        let config = Arc::new(config);
        let (key_shape, ks) = KeyShape::new_single(5, 2, 1024);
        #[track_caller]
        fn check_all(db: &Db, ks: KeySpace, last: u8) {
            for i in 5u8..=last {
                assert_eq!(db.get(ks, &[1, 2, 3, 4, i]).unwrap(), Some(vec![i].into()));
            }
        }
        #[track_caller]
        fn check_metrics(
            metrics: &Metrics,
            unmerge: u64,
            flush: u64,
            merge_flush: u64,
            clean: u64,
        ) {
            assert_eq!(
                metrics
                    .unload
                    .get_metric_with_label_values(&["unmerge"])
                    .unwrap()
                    .get(),
                unmerge,
                "unmerge metric does not match"
            );
            assert_eq!(
                metrics
                    .unload
                    .get_metric_with_label_values(&["flush"])
                    .unwrap()
                    .get(),
                flush,
                "flush metric does not match"
            );
            assert_eq!(
                metrics
                    .unload
                    .get_metric_with_label_values(&["merge_flush"])
                    .unwrap()
                    .get(),
                merge_flush,
                "merge_flush metric does not match"
            );
            assert_eq!(
                metrics
                    .unload
                    .get_metric_with_label_values(&["clean"])
                    .unwrap()
                    .get(),
                clean,
                "clean metric does not match"
            );
        }
        let other_key = vec![129, 2, 3, 4, 5];
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap();
            {
                // todo rewrite
            }

            db.insert(ks, other_key.clone(), vec![5]).unwrap(); // fill one
            db.insert(ks, vec![1, 2, 3, 4, 5], vec![5]).unwrap();
            db.insert(ks, vec![1, 2, 3, 4, 6], vec![6]).unwrap();
            db.insert(ks, vec![1, 2, 3, 4, 7], vec![7]).unwrap();
            db.insert(ks, vec![1, 2, 3, 4, 8], vec![8]).unwrap();
            thread::sleep(Duration::from_millis(100)); // todo use barrier w/ flusher thread instead
            check_metrics(&db.metrics, 0, 1, 0, 0);
            db.insert(ks, vec![1, 2, 3, 4, 9], vec![9]).unwrap();
            db.insert(ks, vec![1, 2, 3, 4, 10], vec![10]).unwrap();
            check_all(&db, ks, 10);
            thread::sleep(Duration::from_millis(100)); // todo use barrier w/ flusher thread instead
            check_metrics(&db.metrics, 0, 1, 1, 0);
        }
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap();
            check_all(&db, ks, 10);
            check_metrics(&db.metrics, 0, 0, 0, 0);
        }
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap();
            db.insert(ks, vec![1, 2, 3, 4, 11], vec![11]).unwrap();
            db.insert(ks, vec![1, 2, 3, 4, 12], vec![12]).unwrap();
            thread::sleep(Duration::from_millis(100)); // todo use barrier w/ flusher thread instead
            check_metrics(&db.metrics, 0, 1, 0, 0);
            check_all(&db, ks, 12);
            db.insert(ks, vec![1, 2, 3, 4, 13], vec![13]).unwrap();
            db.get(ks, &other_key).unwrap().unwrap();
            check_metrics(&db.metrics, 0, 1, 0, 0);
        }
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                force_unload_config(&config),
                Metrics::new(),
            )
            .unwrap();
            check_all(&db, ks, 13);
            check_metrics(&db.metrics, 0, 0, 0, 0);
            db.rebuild_control_region().unwrap(); // this puts all entries into clean state
            assert!(
                db.large_table.is_all_clean(),
                "Some entries are not clean after snapshot"
            );
            db.get(ks, &other_key).unwrap().unwrap();
            check_metrics(&db.metrics, 0, 2, 0, 0);
        }
    }

    #[test]
    fn test_value_cache_update_remove() {
        let dir = tempdir::TempDir::new("test-value-cache-update-remove").unwrap();
        let config = Arc::new(Config::small());
        let mut ksb = KeyShapeBuilder::new();
        let ksc = KeySpaceConfig::new().with_value_cache_size(10);
        let ks = ksb.add_key_space_config("k", 1, 1, 1, ksc);
        let key_shape = ksb.build();
        let metrics = Metrics::new();
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();
        db.insert(ks, vec![1], vec![2]).unwrap();
        db.insert(ks, vec![2], vec![3]).unwrap();
        assert_eq!(&db.get(ks, &[1]).unwrap().unwrap(), &[2]);
        assert_eq!(&db.get(ks, &[2]).unwrap().unwrap(), &[3]);
        assert_eq!(
            2,
            metrics
                .lookup_result
                .with_label_values(&["k", "found", "lru"])
                .get()
        );
        db.insert(ks, vec![1], vec![4]).unwrap();
        assert_eq!(&db.get(ks, &[1]).unwrap().unwrap(), &[4]);
        db.remove(ks, vec![1]).unwrap();
        assert_eq!(db.get(ks, &[1]).unwrap(), None);
        assert_eq!(3, lru_lookups("k", &metrics));
    }

    // This test is disabled as it takes a long time, but it should be run if logic around
    // IndexTable::insert is changed.
    // todo we can also rewrite this test more efficiently if we introduce
    // random sleep between wal write and large_table insert during the test.
    #[ignore]
    #[test]
    // This test verifies that the last value written into the large table
    // cache matches the last value written to wal.
    // Because wal write and write into large table are not done under single mutex,
    // there can be race condition unless special measures are taken.
    fn test_concurrent_single_value_update() {
        let num_threads = 8;
        let mut threads = Vec::with_capacity(num_threads);
        for i in 0..num_threads {
            let jh = thread::spawn(move || {
                for _ in 0..256 {
                    test_concurrent_single_value_update_iteration(i)
                }
            });
            threads.push(jh);
        }
        for jh in threads {
            jh.join().unwrap();
        }
    }

    fn test_concurrent_single_value_update_iteration(i: usize) {
        let dir =
            tempdir::TempDir::new(&format!("test-concurrent-single-value-update-{i}")).unwrap();
        let config = Arc::new(Config::small());
        let (key_shape, ks) = KeyShape::new_single(4, 1, 1);
        let cached_value;
        let key = Bytes::from(15u32.to_be_bytes().to_vec());
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap();
            let num_threads = 16;
            let mut threads = Vec::with_capacity(num_threads);
            for _ in 0..num_threads {
                let db = db.clone();
                let jh = thread::spawn(move || {
                    let mut rng = ThreadRng::default();
                    for _ in 0..1024 {
                        let key = Bytes::from(15u32.to_be_bytes().to_vec());
                        let value: u32 = rng.gen();
                        db.insert(ks, key.clone(), value.to_be_bytes().to_vec())
                            .unwrap();
                    }
                });
                threads.push(jh);
            }
            for jh in threads {
                jh.join().unwrap();
            }
            cached_value = db.get(ks, &key).unwrap().unwrap();
        }
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap();
            let replay_value = db.get(ks, &key).unwrap().unwrap();
            assert_eq!(replay_value, cached_value);
        }
    }

    #[test]
    fn test_key_reduction() {
        let dir = tempdir::TempDir::new("test_key_reduction").unwrap();
        let config = Arc::new(Config::small());
        let ks_config = KeySpaceConfig::new().with_key_reduction(0..2);
        let (key_shape, ks) = KeyShape::new_single_config(4, 1, 1, ks_config);
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();

        db.insert(ks, vec![1, 2, 3, 4], vec![1]).unwrap();
        db.insert(ks, vec![1, 3, 3, 4], vec![2]).unwrap();
        db.insert(ks, vec![1, 5, 3, 4], vec![3]).unwrap();

        // Simple get tests
        assert_eq!(db.get(ks, &[1, 2, 3, 4]).unwrap().unwrap().as_ref(), &[1]);
        assert_eq!(db.get(ks, &[1, 3, 3, 4]).unwrap().unwrap().as_ref(), &[2]);
        assert_eq!(db.get(ks, &[1, 5, 3, 4]).unwrap().unwrap().as_ref(), &[3]);
        assert!(db.get(ks, &[1, 6, 3, 4]).unwrap().is_none());
        assert!(db.get(ks, &[1, 5, 4, 4]).unwrap().is_none());

        // Last in range tests
        let (k, v) = db
            .last_in_range(
                ks,
                &Bytes::from(vec![1, 3, 3, 4]),
                &Bytes::from(vec![1, 3, 3, 4]),
            )
            .unwrap()
            .unwrap();
        assert_eq!(k.as_ref(), &[1, 3, 3, 4]);
        assert_eq!(v.as_ref(), &[2]);

        let r = db
            .last_in_range(
                ks,
                &Bytes::from(vec![1, 3, 6, 7]),
                &Bytes::from(vec![1, 3, 6, 7]),
            )
            .unwrap();
        assert_eq!(None, r);

        // Iterator test (forward direction)
        let mut iterator = db.iterator(ks);
        iterator.set_lower_bound(vec![1, 3, 3, 4]);
        iterator.set_upper_bound(vec![1, 3, 3, 5]);
        let (k, v) = iterator.next().unwrap().unwrap();
        assert_eq!(k.as_ref(), &[1, 3, 3, 4]);
        assert_eq!(v.as_ref(), &[2]);
        assert!(iterator.next().is_none());

        let mut iterator = db.iterator(ks);
        iterator.set_lower_bound(vec![1, 3, 3, 5]);
        iterator.set_upper_bound(vec![1, 3, 3, 6]);
        assert!(iterator.next().is_none());

        let mut iterator = db.iterator(ks);
        iterator.set_lower_bound(vec![1, 3, 3, 3]);
        iterator.set_upper_bound(vec![1, 3, 3, 4]);
        assert!(iterator.next().is_none());

        // Iterator test (reverse direction)
        let mut iterator = db.iterator(ks);
        iterator.set_lower_bound(vec![1, 3, 3, 4]);
        iterator.set_upper_bound(vec![1, 3, 3, 5]);
        iterator.reverse();
        let (k, v) = iterator.next().unwrap().unwrap();
        assert_eq!(k.as_ref(), &[1, 3, 3, 4]);
        assert_eq!(v.as_ref(), &[2]);
        assert!(iterator.next().is_none());

        let mut iterator = db.iterator(ks);
        iterator.set_lower_bound(vec![1, 3, 3, 5]);
        iterator.set_upper_bound(vec![1, 3, 3, 6]);
        iterator.reverse();
        assert!(iterator.next().is_none());

        let mut iterator = db.iterator(ks);
        iterator.set_lower_bound(vec![1, 3, 3, 3]);
        iterator.set_upper_bound(vec![1, 3, 3, 4]);
        iterator.reverse();
        assert!(iterator.next().is_none());

        // Remove test
        db.remove(ks, vec![1, 3, 3, 4]).unwrap();
        assert_eq!(db.get(ks, &[1, 3, 3, 4]).unwrap(), None);
    }

    #[test]
    fn test_key_reduction_lru() {
        let dir = tempdir::TempDir::new("test_key_reduction_lru").unwrap();
        let config = Arc::new(Config::small());
        let ks_config = KeySpaceConfig::new()
            .with_key_reduction(0..2)
            .with_value_cache_size(2);
        let (key_shape, ks) = KeyShape::new_single_config(4, 1, 1, ks_config);
        let metrics = Metrics::new();
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();

        db.insert(ks, vec![1, 2, 3, 4], vec![1]).unwrap();
        assert_eq!(db.get(ks, &[1, 2, 3, 4]).unwrap().unwrap().as_ref(), &[1]);
        assert_eq!(1, lru_lookups("root", &metrics));

        db.insert(ks, vec![1, 3, 3, 4], vec![2]).unwrap();
        assert_eq!(db.get(ks, &[1, 3, 3, 4]).unwrap().unwrap().as_ref(), &[2]);
        assert_eq!(2, lru_lookups("root", &metrics));

        db.insert(ks, vec![1, 5, 3, 4], vec![3]).unwrap();
        assert_eq!(db.get(ks, &[1, 5, 3, 4]).unwrap().unwrap().as_ref(), &[3]);
        assert_eq!(3, lru_lookups("root", &metrics));

        // First key was evicted, so lru lookup metric does not increment
        assert_eq!(db.get(ks, &[1, 2, 3, 4]).unwrap().unwrap().as_ref(), &[1]);
        assert_eq!(3, lru_lookups("root", &metrics));
        // Since we just fetched this key, and it should be populated to lru,
        // the next lookup comes from the lru cache
        assert_eq!(db.get(ks, &[1, 2, 3, 4]).unwrap().unwrap().as_ref(), &[1]);
        assert_eq!(4, lru_lookups("root", &metrics));
    }

    fn lru_lookups(ks: &str, metrics: &Metrics) -> u64 {
        metrics
            .lookup_result
            .with_label_values(&[ks, "found", "lru"])
            .get()
    }

    fn force_unload_config(config: &Config) -> Arc<Config> {
        let mut config2 = Config::clone(config);
        config2.snapshot_unload_threshold = 0;
        Arc::new(config2)
    }

    fn ku32(k: u32) -> Bytes {
        k.to_be_bytes().to_vec().into()
    }

    fn vu32(v: u32) -> Bytes {
        v.to_le_bytes().to_vec().into()
    }

    fn ku128(k: u128) -> Bytes {
        k.to_be_bytes().to_vec().into()
    }

    fn vu128(v: u128) -> Bytes {
        v.to_le_bytes().to_vec().into()
    }
}
