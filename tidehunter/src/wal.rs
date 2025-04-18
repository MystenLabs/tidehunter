use crate::crc::{CrcFrame, CrcReadError, IntoBytesFixed};
use crate::file_reader::{align_size, set_direct_options, FileReader};
use crate::index::index_format::IndexFormat;
use crate::key_shape::KeySpaceDesc;
use crate::lookup::{FileRange, RandomRead};
use crate::metrics::{Metrics, TimerExt};
use crate::wal_syncer::WalSyncer;
use bytes::{Buf, BufMut};
use minibytes::Bytes;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;
use std::time::Instant;
use std::{io, mem, ptr, thread};

pub struct WalWriter {
    wal: Arc<Wal>,
    position_and_map: Mutex<(IncrementalWalPosition, Map)>,
    mapper: WalMapper,
}

pub struct Wal {
    file: File,
    layout: WalLayout,
    maps: RwLock<BTreeMap<u64, Map>>,
    wal_syncer: WalSyncer,
    metrics: Arc<Metrics>,
}

struct WalMapper {
    jh: Option<JoinHandle<()>>,
    receiver: Option<Mutex<mpsc::Receiver<Map>>>,
}

struct WalMapperThread {
    sender: mpsc::SyncSender<Map>,
    last_map: u64,
    file: File,
    layout: WalLayout,
}

pub struct WalIterator {
    wal: Arc<Wal>,
    map: Map,
    position: u64,
}

#[derive(Clone)]
// todo only pub between wal.rs and wal_syncer.rs
pub(crate) struct Map {
    id: u64,
    pub data: Bytes,
    writeable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct WalPosition(u64);

pub enum WalRandomRead<'a> {
    Mapped(Bytes),
    File(FileRange<'a>),
}

impl WalWriter {
    pub fn write(&self, w: &PreparedWalWrite) -> Result<WalPosition, WalError> {
        let len = w.frame.len_with_header() as u64;
        let len_aligned = self.wal.layout.align(len);
        let mut current_map_and_position = self.position_and_map.lock();
        let (pos, prev_block_end) = current_map_and_position.0.allocate_position(len_aligned);
        // todo duplicated code
        let (map_id, offset) = self.wal.layout.locate(pos);
        // todo - decide whether map is covered by mutex or we want concurrent writes
        if current_map_and_position.1.id != map_id {
            if pos != prev_block_end {
                let (prev_map, prev_offset) = self.wal.layout.locate(prev_block_end);
                assert_eq!(prev_map, current_map_and_position.1.id);
                let skip_marker = CrcFrame::skip_marker();
                assert!(
                    current_map_and_position.1.writeable,
                    "Attempt to write into read-only map"
                );
                let buf = write_buf_at(
                    &current_map_and_position.1.data,
                    prev_offset as usize,
                    skip_marker.as_ref().len(),
                );
                buf.copy_from_slice(skip_marker.as_ref());
            }
            let mut offloaded_map =
                self.wal
                    .recv_map(&self.mapper, map_id, &current_map_and_position.1);
            mem::swap(&mut offloaded_map, &mut current_map_and_position.1);
            self.wal
                .wal_syncer
                .send(offloaded_map, self.wal.layout.map_range(map_id).end);
        } else {
            // todo it is possible to have a race between map mutex and pos allocation so this check may fail
            // assert_eq!(pos, align(prev_block_end));
        }
        // safety: pos calculation logic guarantees non-overlapping writes
        // position only available after write here completes
        assert!(
            current_map_and_position.1.writeable,
            "Attempt to write into read-only map"
        );
        let data = current_map_and_position.1.data.clone();
        // Dropping lock to allow data copy to be done in parallel
        drop(current_map_and_position);
        let buf = write_buf_at(&data, offset as usize, len as usize);
        buf.copy_from_slice(w.frame.as_ref());
        // conversion to u32 is safe - pos is less than self.frag_size,
        // and self.frag_size is asserted less than u32::MAX
        Ok(WalPosition(pos))
    }

    /// Current un-initialized position,
    /// not to be used as WalPosition, only as a metric to see how many bytes were written
    pub fn position(&self) -> u64 {
        self.position_and_map.lock().0.position
    }

    /// Current un-initialized position,
    // todo need to re-think this and not return un-initialized position as WalPosition
    pub fn wal_position(&self) -> WalPosition {
        WalPosition(self.position_and_map.lock().0.position)
    }
}

#[derive(Clone)]
struct IncrementalWalPosition {
    position: u64,
    layout: WalLayout,
}

impl IncrementalWalPosition {
    /// Allocate new position according to layout
    ///
    /// Returns new position and then end of previous block
    pub fn allocate_position(&mut self, len_aligned: u64) -> (u64, u64) {
        assert!(len_aligned > 0);
        let position = self.layout.next_position(self.position, len_aligned);
        let result = (position, self.position);
        self.position = position + len_aligned;
        result
    }
}

#[derive(Clone)]
pub(crate) struct WalLayout {
    pub frag_size: u64,
    pub max_maps: usize,
    pub direct_io: bool,
}

impl WalLayout {
    fn assert_layout(&self) {
        assert!(self.frag_size <= u32::MAX as u64, "Frag size too large");
        assert_eq!(
            self.frag_size,
            self.align(self.frag_size),
            "Frag size not aligned"
        );
    }

    /// Allocate the next position.
    /// Block should not cross the map boundary defined by the self.frag_size
    fn next_position(&self, mut pos: u64, len_aligned: u64) -> u64 {
        assert!(
            len_aligned <= self.frag_size,
            "Entry({len_aligned}) is larger then frag_size({})",
            self.frag_size
        );
        let map_start = self.locate(pos).0;
        let map_end = self.locate(pos + len_aligned - 1).0;
        if map_start != map_end {
            pos = (map_start + 1) * self.frag_size;
        }
        pos
    }

    /// Return number of a mapping and offset inside the mapping for given position
    #[inline]
    fn locate(&self, pos: u64) -> (u64, u64) {
        (pos / self.frag_size, pos % self.frag_size)
    }

    /// Return range of a particular mapping
    fn map_range(&self, map: u64) -> Range<u64> {
        let start = self.frag_size * map;
        let end = self.frag_size * (map + 1);
        start..end
    }

    fn align(&self, v: u64) -> u64 {
        align_size(v, self.direct_io)
    }
}

impl Wal {
    pub(crate) fn open(
        p: &Path,
        layout: WalLayout,
        metrics: Arc<Metrics>,
    ) -> io::Result<Arc<Self>> {
        layout.assert_layout();
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true);
        set_direct_options(&mut options, layout.direct_io);
        let file = options.open(p)?;
        Self::resize(&layout, &file)?;
        Ok(Self::from_file(file, layout, metrics))
    }

    fn from_file(file: File, layout: WalLayout, metrics: Arc<Metrics>) -> Arc<Self> {
        let wal_syncer = WalSyncer::start(metrics.clone());
        let reader = Wal {
            file,
            layout,
            maps: Default::default(),
            wal_syncer,
            metrics,
        };
        Arc::new(reader)
    }

    // todo remove
    #[doc(hidden)]
    #[cfg(test)]
    pub fn read(&self, pos: WalPosition) -> Result<Bytes, WalError> {
        assert_ne!(
            pos,
            WalPosition::INVALID,
            "Trying to read invalid wal position"
        );
        let (map, offset) = self.layout.locate(pos.0);
        let map = self.map(map, false)?;
        // todo avoid clone, introduce Bytes::slice_in_place
        Ok(CrcFrame::read_from_bytes(&map.data, offset as usize)?)
    }

    /// Read the wal position without mapping.
    /// If mapping exists, it is still used for reading
    /// if mapping does not exist the read syscall is used instead.
    ///
    /// Returns (false, _) if read syscall was used
    /// Returns (true, _) if mapping was used
    pub fn read_unmapped(&self, pos: WalPosition) -> Result<(bool, Bytes), WalError> {
        assert_ne!(
            pos,
            WalPosition::INVALID,
            "Trying to read invalid wal position"
        );
        const INITIAL_READ_SIZE: usize = 1024; // todo probably need to increase even more
        let (map, offset) = self.layout.locate(pos.0);
        if let Some(map) = self.get_map(map) {
            // using CrcFrame::read_from_slice to avoid holding the larger byte array
            Ok((
                true,
                CrcFrame::read_from_slice(&map.data, offset as usize)?
                    .to_vec()
                    .into(),
            ))
        } else {
            let mut buf = self.file_reader().io_buffer_bytes(INITIAL_READ_SIZE);
            let read = self.file.read_at(&mut buf, pos.0)?;
            assert!(read > CrcFrame::CRC_HEADER_LENGTH); // todo this is not actually guaranteed
            let size = CrcFrame::read_size(&buf[..read]);
            let target_size = size + CrcFrame::CRC_HEADER_LENGTH;
            if target_size > read {
                if self.layout.direct_io && read != INITIAL_READ_SIZE {
                    panic!("read not aligned: {read}");
                }
                // todo more test coverage for those cases including when read != INITIAL_READ_SIZE
                if target_size > INITIAL_READ_SIZE {
                    let target_read_size = if self.layout.direct_io {
                        self.layout.align(target_size as u64) as usize
                    } else {
                        target_size
                    };
                    buf = self.file_reader().reallocate(buf, target_read_size);
                }
                self.file
                    .read_exact_at(&mut buf[read..], pos.0 + read as u64)?;
            }
            let mut bytes = Bytes::from(bytes::Bytes::from(buf));
            if self.layout.direct_io && bytes.len() > target_size {
                // Direct IO buffer can be larger then needed
                bytes = bytes.slice(..target_size);
            }
            Ok((false, CrcFrame::read_from_bytes(&bytes, 0)?))
        }
    }

    pub fn random_reader_at(
        &self,
        ks: &KeySpaceDesc,
        pos: WalPosition,
        inner_offset: usize,
    ) -> Result<WalRandomRead, WalError> {
        if ks.index_format().use_unbounded_reader() {
            self.random_reader_at_unbounded(pos, inner_offset)
        } else {
            self.random_reader_at_bounded(pos, inner_offset)
        }
    }

    pub fn random_reader_at_unbounded(
        &self,
        pos: WalPosition,
        inner_offset: usize,
    ) -> Result<WalRandomRead, WalError> {
        assert_ne!(
            pos,
            WalPosition::INVALID,
            "Trying to read invalid wal position"
        );
        let (map, offset) = self.layout.locate(pos.0);
        if let Some(map) = self.get_map(map) {
            let offset = offset as usize;
            let header_end = offset + CrcFrame::CRC_HEADER_LENGTH;
            let data = map.data.slice(header_end + inner_offset..);
            Ok(WalRandomRead::Mapped(data))
        } else {
            let header_end = pos.0 + CrcFrame::CRC_HEADER_LENGTH as u64;
            let frag_end = self.layout.map_range(map).end;
            let range = (header_end + inner_offset as u64)..frag_end;
            Ok(WalRandomRead::File(FileRange::new(
                self.file_reader(),
                range,
            )))
        }
    }

    pub fn random_reader_at_bounded(
        &self,
        pos: WalPosition,
        inner_offset: usize,
    ) -> Result<WalRandomRead, WalError> {
        assert_ne!(
            pos,
            WalPosition::INVALID,
            "Trying to read invalid wal position"
        );
        let (map, offset) = self.layout.locate(pos.0);
        if let Some(map) = self.get_map(map) {
            let offset = offset as usize;
            let header_end = offset + CrcFrame::CRC_HEADER_LENGTH;
            let size = CrcFrame::read_size(&map.data[offset..header_end]);
            let data = map.data.slice(header_end + inner_offset..header_end + size);
            Ok(WalRandomRead::Mapped(data))
        } else {
            let mut buf = [0; CrcFrame::CRC_HEADER_LENGTH];
            self.file.read_exact_at(&mut buf, pos.0)?;
            let size = CrcFrame::read_size(&buf);
            let header_end = pos.0 + CrcFrame::CRC_HEADER_LENGTH as u64;
            let range = (header_end + inner_offset as u64)..(header_end + size as u64);
            Ok(WalRandomRead::File(FileRange::new(
                self.file_reader(),
                range,
            )))
        }
    }

    fn get_map(&self, id: u64) -> Option<Map> {
        let maps = match self.maps.try_read() {
            Some(maps) => maps,
            None => {
                let now = Instant::now();
                let maps = self.maps.read();
                self.metrics
                    .wal_contention
                    .observe(now.elapsed().as_micros() as f64);
                maps
            }
        };
        maps.get(&id).cloned()
    }

    fn recv_map(&self, wal_mapper: &WalMapper, expect_id: u64, pin_map: &Map) -> Map {
        let map = wal_mapper.next_map();
        assert_eq!(
            map.id, expect_id,
            "Id from wal mapper does not match expected map id"
        );
        let mut maps = self.maps.write();
        let prev = maps.insert(map.id, map.clone());
        if prev.is_some() {
            panic!("Re-inserting mapping into wal is not allowed");
        }
        let pin_map_entry = maps.get_mut(&pin_map.id).expect("Pin map not found");
        assert!(
            ptr::eq(pin_map.data.as_ptr(), pin_map_entry.data.as_ptr()),
            "Pin map entry and located map do not match"
        );
        pin_map_entry.writeable = false;
        // Remove memory mapping and copy over data to a regular byte array
        // pin_map_entry.data = Bytes::copy_from_slice(&pin_map.data);
        // Preserve mem mapping
        pin_map_entry.data = pin_map.data.clone();
        if maps.len() > self.layout.max_maps {
            if let Some((_, popped_map)) = maps.pop_first() {
                drop(maps);
                // self.maps write lock is very expensive as it blocks any IO operation
                // dropping popped_map can result in syscall, therefore doing it after lock is released
                drop(popped_map);
            }
        }
        map
    }

    fn map(&self, id: u64, writeable: bool) -> io::Result<Map> {
        let mut maps = self.maps.write();
        let _timer = self.metrics.map_time_mcs.clone().mcs_timer();
        let map = match maps.entry(id) {
            Entry::Vacant(va) => {
                // todo - make sure WalMapper is not active when this code is called
                let range = self.layout.map_range(id);
                let data = unsafe {
                    let mut options = memmap2::MmapOptions::new();
                    options
                        .offset(range.start)
                        .len(self.layout.frag_size as usize);
                    if writeable {
                        options /*.populate()*/
                            .map_mut(&self.file)?
                            .into()
                    } else {
                        options.map(&self.file)?.into()
                    }
                };
                let map = Map {
                    id,
                    writeable,
                    data,
                };
                va.insert(map)
            }
            Entry::Occupied(oc) => {
                let map = oc.into_mut();
                if writeable && !map.writeable {
                    // this can be supported but not needed?
                    panic!("Requested writable mapping but it is already mapped as read-only");
                }
                map
            }
        };
        let map = map.clone();
        if maps.len() > self.layout.max_maps {
            maps.pop_first();
        }
        Ok(map)
    }

    /// Resize file to fit the specified map id
    fn extend_to_map(layout: &WalLayout, file: &File, id: u64) -> io::Result<()> {
        let range = layout.map_range(id);
        let len = file.metadata()?.len();
        if len < range.end {
            file.set_len(range.end)?;
        }
        Ok(())
    }

    /// Resize the file to fit the current layout
    fn resize(layout: &WalLayout, file: &File) -> io::Result<()> {
        let len = file.metadata()?.len();
        let r = len % layout.frag_size;
        if r != 0 {
            file.set_len(len + layout.frag_size - r)?;
        }
        Ok(())
    }

    /// Iterate wal from the position after given position
    /// If WalPosition::INVALID is specified, iterate from start
    pub fn wal_iterator(self: &Arc<Self>, position: WalPosition) -> Result<WalIterator, WalError> {
        let (skip_one, position) = if position == WalPosition::INVALID {
            (false, 0)
        } else {
            (true, position.0)
        };
        let (map_id, _) = self.layout.locate(position);
        Self::extend_to_map(&self.layout, &self.file, map_id)?;
        let map = self.map(map_id, true)?;
        let mut iterator = WalIterator {
            wal: self.clone(),
            position,
            map,
        };
        if skip_one {
            iterator.next()?;
        }
        Ok(iterator)
    }

    fn file_reader(&self) -> FileReader {
        FileReader::new(&self.file, self.layout.direct_io)
    }

    /// Ensure the file is written to disk (blocking call).
    pub fn fsync(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Returns the file descriptor of the wal file
    #[cfg(test)]
    pub(crate) fn file(&self) -> &File {
        &self.file
    }
}

impl WalMapper {
    pub fn start(last_map: u64, file: File, layout: WalLayout) -> Self {
        let (sender, receiver) = mpsc::sync_channel(2);
        let this = WalMapperThread {
            last_map,
            file,
            layout,
            sender,
        };
        let jh = thread::Builder::new()
            .name("wal-mapper".to_string())
            .spawn(move || this.run())
            .expect("failed to start wal-mapper thread");
        let receiver = Mutex::new(receiver);
        let receiver = Some(receiver);
        let jh = Some(jh);
        Self { jh, receiver }
    }

    pub fn next_map(&self) -> Map {
        self.receiver
            .as_ref()
            .expect("next_map is called after drop")
            .lock()
            .recv()
            .expect("Map thread stopped unexpectedly")
    }
}

impl Drop for WalMapper {
    fn drop(&mut self) {
        self.receiver.take();
        self.jh
            .take()
            .unwrap()
            .join()
            .expect("wal-mapper thread panic")
    }
}

impl WalMapperThread {
    pub fn run(mut self) {
        loop {
            let id = self.last_map + 1;
            Wal::extend_to_map(&self.layout, &self.file, id).expect("Failed to extend wal file");
            let range = self.layout.map_range(id);
            let data = unsafe {
                let mut options = memmap2::MmapOptions::new();
                options
                    .offset(range.start)
                    .len(self.layout.frag_size as usize);
                options
                    .populate()
                    .map_mut(&self.file)
                    .expect("Failed to mmap on wal file")
                    .into()
            };
            let map = Map {
                id,
                writeable: true,
                data,
            };
            self.last_map = id;
            // todo ideally figure out a way to not create a map when sender closes
            if self.sender.send(map).is_err() {
                return;
            }
        }
    }
}

impl WalIterator {
    pub fn next(&mut self) -> Result<(WalPosition, Bytes), WalError> {
        let frame = self.read_one();
        let frame = if matches!(frame, Err(WalError::Crc(CrcReadError::SkipMarker))) {
            // handle skip marker - jump to next frag
            let next_map = self.map.id + 1;
            self.position = self.wal.layout.map_range(next_map).start;
            self.read_one()?
        } else {
            frame?
        };
        let position = WalPosition(self.position);
        self.position += self
            .wal
            .layout
            .align((frame.len() + CrcFrame::CRC_HEADER_LENGTH) as u64);
        Ok((position, frame))
    }

    fn read_one(&mut self) -> Result<Bytes, WalError> {
        // todo duplicated code
        let (map_id, offset) = self.wal.layout.locate(self.position);
        if self.map.id != map_id {
            Wal::extend_to_map(&self.wal.layout, &self.wal.file, map_id)?;
            self.map = self.wal.map(map_id, true)?;
        }
        Ok(CrcFrame::read_from_bytes(&self.map.data, offset as usize)?)
    }

    pub fn into_writer(self) -> WalWriter {
        let position = IncrementalWalPosition {
            position: self.position,
            layout: self.wal.layout.clone(),
        };
        let mapper = WalMapper::start(
            self.map.id,
            self.wal.file.try_clone().unwrap(),
            self.wal.layout.clone(),
        );
        assert_eq!(self.wal.layout.locate(position.position).0, self.map.id);
        let position_and_map = (position, self.map);
        let position_and_map = Mutex::new(position_and_map);
        WalWriter {
            wal: self.wal,
            position_and_map,
            mapper,
        }
    }

    pub fn wal(&self) -> &Wal {
        &self.wal
    }
}

impl WalRandomRead<'_> {
    pub fn kind_str(&self) -> &'static str {
        match self {
            WalRandomRead::Mapped(_) => "mapped",
            WalRandomRead::File(_) => "syscall",
        }
    }
}

impl RandomRead for WalRandomRead<'_> {
    fn read(&self, range: Range<usize>) -> Bytes {
        match self {
            WalRandomRead::Mapped(bytes) => bytes.slice(range),
            WalRandomRead::File(fr) => fr.read(range),
        }
    }

    fn len(&self) -> usize {
        match self {
            WalRandomRead::Mapped(bytes) => bytes.len(),
            WalRandomRead::File(range) => range.len(),
        }
    }
}

fn write_buf_at(data: &Bytes, offset: usize, len: usize) -> &mut [u8] {
    unsafe {
        let ptr = data.as_ptr().add(offset) as *mut u8;
        std::slice::from_raw_parts_mut(ptr, len)
    }
}

pub struct PreparedWalWrite {
    frame: CrcFrame,
}

impl PreparedWalWrite {
    pub fn new(t: &impl IntoBytesFixed) -> Self {
        let frame = CrcFrame::new(t);
        Self { frame }
    }

    pub fn len(&self) -> usize {
        self.frame.len_with_header()
    }
}

impl WalPosition {
    pub const MAX: WalPosition = WalPosition(u64::MAX);
    pub const SIZE: usize = 8;
    pub const INVALID: WalPosition = Self::MAX;
    pub const LENGTH: usize = 8;
    #[cfg(test)]
    pub const TEST: WalPosition = WalPosition(3311);

    pub fn write_to_buf(&self, buf: &mut impl BufMut) {
        buf.put_u64(self.0);
    }

    pub fn read_from_buf(buf: &mut impl Buf) -> Self {
        Self(buf.get_u64())
    }

    pub fn from_slice(slice: &[u8]) -> Self {
        Self(u64::from_be_bytes(
            slice
                .try_into()
                .expect("Invalid slice length for WalPosition::from_slice"),
        ))
    }

    pub fn valid(self) -> Option<Self> {
        if self == Self::INVALID {
            None
        } else {
            Some(self)
        }
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn is_valid(self) -> bool {
        self != Self::INVALID
    }

    #[allow(dead_code)]
    pub fn test_value(v: u64) -> Self {
        Self(v)
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    Crc(CrcReadError),
}

impl From<CrcReadError> for WalError {
    fn from(value: CrcReadError) -> Self {
        Self::Crc(value)
    }
}

impl From<io::Error> for WalError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use std::collections::HashSet;

    #[test]
    fn test_wal() {
        let dir = tempdir::TempDir::new("test-wal").unwrap();
        let file = dir.path().join("wal");
        let layout = WalLayout {
            frag_size: 1024,
            max_maps: 2,
            direct_io: false,
        };
        // todo - add second test case when there is no space for skip marker after large
        let large = vec![1u8; 1024 - 8 - CrcFrame::CRC_HEADER_LENGTH * 3 - 9];
        {
            let wal = Wal::open(&file, layout.clone(), Metrics::new()).unwrap();
            let writer = wal
                .wal_iterator(WalPosition::INVALID)
                .unwrap()
                .into_writer();
            let pos = writer
                .write(&PreparedWalWrite::new(&vec![1, 2, 3]))
                .unwrap();
            let data = wal.read(pos).unwrap();
            assert_eq!(&[1, 2, 3], data.as_ref());
            let pos = writer.write(&PreparedWalWrite::new(&vec![])).unwrap();
            let data = wal.read(pos).unwrap();
            assert_eq!(&[] as &[u8], data.as_ref());
            drop(data);
            let pos = writer.write(&PreparedWalWrite::new(&large)).unwrap();
            let data = wal.read(pos).unwrap();
            assert_eq!(&large, data.as_ref());
        }
        {
            let wal = Wal::open(&file, layout.clone(), Metrics::new()).unwrap();
            let mut wal_iterator = wal.wal_iterator(WalPosition::INVALID).unwrap();
            assert_bytes(&[1, 2, 3], wal_iterator.next());
            assert_bytes(&[], wal_iterator.next());
            assert_bytes(&large, wal_iterator.next());
            wal_iterator.next().expect_err("Error expected");
            let writer = wal_iterator.into_writer();
            let pos = writer
                .write(&PreparedWalWrite::new(&vec![91, 92, 93]))
                .unwrap();
            assert_eq!(pos.0, 1024); // assert we skipped over to next frag
            let data = wal.read(pos).unwrap();
            assert_eq!(&[91, 92, 93], data.as_ref());
        }
        {
            let wal = Wal::open(&file, layout.clone(), Metrics::new()).unwrap();
            let mut wal_iterator = wal.wal_iterator(WalPosition::INVALID).unwrap();
            let p1 = assert_bytes(&[1, 2, 3], wal_iterator.next());
            let p2 = assert_bytes(&[], wal_iterator.next());
            let p3 = assert_bytes(&large, wal_iterator.next());
            let p4 = assert_bytes(&[91, 92, 93], wal_iterator.next());
            wal_iterator.next().expect_err("Error expected");
            let wal = Wal::open(&file, layout.clone(), Metrics::new()).unwrap();
            assert_eq!(&[1, 2, 3], wal.read(p1).unwrap().as_ref());
            assert_eq!(&[] as &[u8], wal.read(p2).unwrap().as_ref());
            assert_eq!(&large, wal.read(p3).unwrap().as_ref());
            assert_eq!(&[91, 92, 93], wal.read(p4).unwrap().as_ref());

            assert_eq!(&[1, 2, 3], wal.read_unmapped(p1).unwrap().1.as_ref());
            assert_eq!(&[] as &[u8], wal.read_unmapped(p2).unwrap().1.as_ref());
            assert_eq!(&large, wal.read_unmapped(p3).unwrap().1.as_ref());
            assert_eq!(&[91, 92, 93], wal.read_unmapped(p4).unwrap().1.as_ref());
        }
        // we wrote into two frags
        // assert_eq!(2048, fs::metadata(file).unwrap().len());
    }

    #[test]
    fn test_incremental_wal_position() {
        let layout = WalLayout {
            frag_size: 512,
            max_maps: 2,
            direct_io: false,
        };
        let mut position = IncrementalWalPosition {
            layout,
            position: 0,
        };
        assert_eq!((0, 0), position.allocate_position(16));
        assert_eq!((16, 16), position.allocate_position(8));
        assert_eq!((24, 24), position.allocate_position(8));
        assert_eq!((32, 32), position.allocate_position(104));
        assert_eq!((136, 136), position.allocate_position(128));
        assert_eq!((264, 264), position.allocate_position(240));
        // Leap over frag boundary
        assert_eq!((512, 504), position.allocate_position(16));
        assert_eq!((512 + 16, 512 + 16), position.allocate_position(32));
    }

    #[test]
    fn test_concurrent_wal_write() {
        let dir = tempdir::TempDir::new("test-wal").unwrap();
        let file = dir.path().join("wal");
        let layout = WalLayout {
            frag_size: 512,
            max_maps: 2,
            direct_io: false,
        };
        let wal = Wal::open(&file, layout.clone(), Metrics::new()).unwrap();
        let wal_writer = wal
            .wal_iterator(WalPosition::INVALID)
            .unwrap()
            .into_writer();
        let wal_writer = Arc::new(wal_writer);
        let threads = 8u64;
        let writes_per_thread = 256u64;
        let mut all_writes = HashSet::new();
        let mut jhs = Vec::with_capacity(threads as usize);
        for thread in 0..threads {
            all_writes.extend(
                (0..writes_per_thread)
                    .into_iter()
                    .map(|w| (thread << 16) + w),
            );
            let wal_writer = wal_writer.clone();
            let jh = thread::spawn(move || {
                for write in 0..writes_per_thread {
                    let value = (thread << 16) + write;
                    let write = PreparedWalWrite::new(&value);
                    wal_writer.write(&write).unwrap();
                }
            });
            jhs.push(jh);
        }
        for jh in jhs {
            jh.join().unwrap();
        }
        drop(wal_writer);
        drop(wal);
        let wal = Wal::open(&file, layout.clone(), Metrics::new()).unwrap();
        let mut iterator = wal.wal_iterator(WalPosition::INVALID).unwrap();
        while let Ok((_, value)) = iterator.next() {
            let value = u64::from_be_bytes(value[..].try_into().unwrap());
            if !all_writes.remove(&value) {
                panic!("Value {value} was in wal but was not written")
            }
        }
        assert!(
            all_writes.is_empty(),
            "Some writes not found in wal({})",
            all_writes.len()
        )
    }

    #[test]
    fn test_position() {
        let mut buf = BytesMut::new();
        WalPosition::TEST.write_to_buf(&mut buf);
        let bytes: bytes::Bytes = buf.into();
        let mut buf = bytes.as_ref();
        let position = WalPosition::read_from_buf(&mut buf);
        assert_eq!(position, WalPosition::TEST);
    }

    /// Test that the wal file is resized correctly when the file is corrupted in such a way that
    /// the file length is not a multiple of the frag size.
    #[test]
    fn test_wal_resize() {
        let dir = tempdir::TempDir::new("test_wal_resize").unwrap();
        let file_path = dir.path().join("wal");
        let frag_size = 512;
        let layout = WalLayout {
            frag_size,
            max_maps: 2,
            direct_io: false,
        };

        // Write an entry into the WAl
        let position = {
            let wal = Wal::open(&file_path, layout.clone(), Metrics::new()).unwrap();
            let writer = wal
                .wal_iterator(WalPosition::INVALID)
                .unwrap()
                .into_writer();
            writer
                .write(&PreparedWalWrite::new(&vec![1, 2, 3]))
                .unwrap()
        };

        // Corrupt the file length
        let file = OpenOptions::new().write(true).open(&file_path).unwrap();
        let len = file.metadata().unwrap().len();
        file.set_len(len - frag_size / 2).unwrap();
        assert_ne!(file.metadata().unwrap().len() % frag_size, 0);

        // Re-open the WAL and ensure it resizes correctly
        let wal = Wal::open(&file_path, layout, Metrics::new()).unwrap();
        let data = wal.read(position).unwrap();
        assert_eq!(&[1, 2, 3], data.as_ref());
        assert_eq!(file.metadata().unwrap().len() % frag_size, 0);
    }

    #[track_caller]
    fn assert_bytes(e: &[u8], v: Result<(WalPosition, Bytes), WalError>) -> WalPosition {
        let v = v.expect("Expected value, got nothing");
        assert_eq!(e, v.1.as_ref());
        v.0
    }

    impl IntoBytesFixed for u64 {
        fn len(&self) -> usize {
            8
        }

        fn write_into_bytes(&self, buf: &mut BytesMut) {
            buf.put_u64(*self)
        }
    }
}
