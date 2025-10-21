// Submodules
pub(crate) mod allocator;
pub(crate) mod files;
pub mod layout;
mod mapper;
pub mod position;
mod syncer;
pub(crate) mod tracker;

use crate::context::ReadType;
use crate::crc::{CrcFrame, CrcReadError, IntoBytesFixed};
use crate::file_reader::set_direct_options;
use crate::file_reader::FileReader;
use crate::lookup::{FileRange, RandomRead};
use crate::metrics::Metrics;
use arc_swap::ArcSwap;
use minibytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::wal::mapper::WalMaps;
use crate::wal_allocator::WalAllocator;
use files::WalFiles;
use layout::WalLayout;
use mapper::WalMapper;
use position::{LastProcessed, MapId, WalPosition};
use syncer::WalSyncer;
use tracker::{WalGuard, WalTracker};

pub struct WalWriter {
    wal: Arc<Wal>,
    allocator: WalAllocator,
    wal_tracker: WalTracker,
    pub(crate) fp: WalFailPoints,
}

pub struct Wal {
    files: Arc<ArcSwap<WalFiles>>,
    layout: WalLayout,
    maps: Arc<ArcSwap<WalMaps>>,
    metrics: Arc<Metrics>,
}

pub struct WalIterator {
    wal: Arc<Wal>,
    maps: WalMaps,
    map: Map,
    position: u64,
}

#[derive(Clone)]
// todo only pub between wal.rs and wal_syncer.rs
pub(crate) struct Map {
    id: MapId,
    pub data: Bytes,
    writeable: bool,
}

pub enum WalRandomRead {
    Mapped(Bytes),
    File(FileRange),
}

impl WalWriter {
    pub fn write(&self, w: &PreparedWalWrite) -> Result<WalGuard, WalError> {
        Ok(self
            .multi_write(std::iter::once(w))?
            .into_iter()
            .next()
            .unwrap())
    }

    pub fn multi_write<'a>(
        &self,
        writes: impl IntoIterator<Item = &'a PreparedWalWrite> + Clone,
    ) -> Result<Vec<WalGuard>, WalError> {
        let len_aligned = writes
            .clone()
            .into_iter()
            .map(|w| self.wal.layout.align(w.len() as u64))
            .sum();
        let allocation_result = self.allocator.allocate(len_aligned);
        if let Some(skip_marker_pos) = allocation_result.need_skip_marker() {
            self.write_skip_marker(skip_marker_pos);
        }

        let (map, mut offset) = self.get_writeable_map(allocation_result.allocated_position());

        // Calculate the end position after all writes
        let mut pos = allocation_result.allocated_position();
        let wal_batch = self.wal_tracker.allocated(allocation_result);

        let mut guards = vec![];
        for w in writes {
            let frame_size = w.len();
            let aligned_frame_size = self.wal.layout.align(frame_size as u64);
            self.fp.fp_multi_write_before_write_buf();
            let buf = write_buf_at(&map.data, offset, frame_size);
            buf.copy_from_slice(w.frame.as_ref());
            // conversion to u32 is safe - pos is less than self.frag_size,
            // and self.frag_size is asserted less than u32::MAX
            let wal_position = WalPosition::new(pos, frame_size as u32);
            guards.push(wal_batch.guard(wal_position));
            pos += aligned_frame_size;
            offset += aligned_frame_size as usize;
        }
        Ok(guards)
    }

    fn write_skip_marker(&self, position: u64) {
        let (map, offset) = self.get_writeable_map(position);
        let skip_marker = CrcFrame::skip_marker();
        let buf = write_buf_at(&map.data, offset, skip_marker.as_ref().len());
        buf.copy_from_slice(skip_marker.as_ref());
    }

    fn get_writeable_map(&self, position: u64) -> (Map, usize) {
        let (map, offset) = self.wal.layout.locate(position);
        const MAX_ATTEMPTS: usize = 10 * 1000;
        for _ in 0..MAX_ATTEMPTS {
            let Some(map) = self.wal.get_map(map) else {
                self.wal.metrics.wal_write_wait.inc();
                thread::sleep(Duration::from_millis(1));
                continue;
            };
            assert!(map.writeable, "Map is not writable");
            return (map, offset as usize);
        }
        panic!("Could not receive writable map {map:?}")
    }

    /// Current un-initialized position,
    /// not to be used as WalPosition, only as a metric to see how many bytes were written
    pub fn position(&self) -> u64 {
        self.allocator.position()
    }

    /// Returns the last processed position from the WalTracker
    pub fn last_processed(&self) -> LastProcessed {
        self.wal_tracker.last_processed()
    }

    /// Requests deletion of WAL files that have been fully processed by the relocation process up to the watermark position.
    ///
    /// Given watermark positions will be preserved.
    /// The actual file deletion is performed by the mapper thread.
    pub fn gc(&self, watermark: u64) -> io::Result<()> {
        // Send message to mapper thread to update minimum WAL position and remove old files
        self.wal_tracker.min_wal_position_updated(watermark);
        self.wal
            .metrics
            .gc_position
            .with_label_values(&[self.wal.layout.kind.name()])
            .set(watermark as i64);
        Ok(())
    }

    #[cfg(test)]
    /// Waits until wal_tracker processes all in-flight messages.
    pub fn wal_tracker_barrier(&self) {
        self.wal_tracker.barrier()
    }
}

impl Wal {
    #[doc(hidden)] // Used by tools/wal_inspector to open WAL files directly
    pub fn open(
        base_path: &Path,
        layout: WalLayout,
        metrics: Arc<Metrics>,
    ) -> io::Result<Arc<Self>> {
        layout.assert_layout();
        let files = WalFiles::new(base_path, &layout)?;
        let wal = Wal {
            files,
            layout,
            maps: Default::default(),
            metrics,
        };
        Ok(Arc::new(wal))
    }

    pub(crate) fn open_file(path: &Path, layout: &WalLayout) -> io::Result<File> {
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true);
        set_direct_options(&mut options, layout.direct_io);
        let file = options.open(path)?;
        Wal::resize(layout, &file)?;
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            unsafe {
                assert_eq!(
                    0,
                    libc::posix_fadvise(
                        file.as_raw_fd(),
                        0, /*offset*/
                        0, /*len*/
                        libc::POSIX_FADV_RANDOM,
                    ),
                    "fadvise failed"
                );
            }
        }
        Ok(file)
    }

    /// Read the wal position.
    /// If mapping exists, it is used for reading.
    /// If mapping does not exist, the read syscall is used instead.
    ///
    /// This method returns what type of read was used along with bytes read.
    pub fn read(&self, pos: WalPosition) -> Result<(ReadType, Option<Bytes>), WalError> {
        assert_ne!(
            pos,
            WalPosition::INVALID,
            "Trying to read invalid wal position"
        );
        let (map, offset) = self.layout.locate(pos.offset);
        if let Some(map) = self.get_map(map) {
            // using CrcFrame::read_from_slice to avoid holding the larger byte array
            Ok((
                ReadType::Mapped,
                Some(
                    CrcFrame::read_from_slice(&map.data, offset as usize)?
                        .to_vec()
                        .into(),
                ),
            ))
        } else {
            let buffer_size = if self.layout.direct_io {
                self.layout.align(pos.frame_len() as u64) as usize
            } else {
                pos.frame_len()
            };
            let mut buf = FileReader::io_buffer_bytes(buffer_size, self.layout.direct_io);
            let files = self.files.load();
            let Some(file) = files.get_checked(self.layout.locate_file(pos.offset)) else {
                return Ok((ReadType::Syscall, None));
            };
            file.read_exact_at(&mut buf, self.layout.offset_in_wal_file(pos.offset))?;
            let mut bytes = Bytes::from(bytes::Bytes::from(buf));
            if self.layout.direct_io && bytes.len() > pos.frame_len() {
                // Direct IO buffer can be larger then needed
                bytes = bytes.slice(..pos.frame_len());
            }
            Ok((
                ReadType::Syscall,
                Some(CrcFrame::read_from_bytes(&bytes, 0)?),
            ))
        }
    }

    pub fn random_reader_at(
        &self,
        pos: WalPosition,
        inner_offset: usize,
    ) -> Result<WalRandomRead, WalError> {
        assert_ne!(
            pos,
            WalPosition::INVALID,
            "Trying to read invalid wal position"
        );
        let (map, offset) = self.layout.locate(pos.offset);
        if let Some(map) = self.get_map(map) {
            let offset = offset as usize;
            let header_end = offset + CrcFrame::CRC_HEADER_LENGTH;
            let data = map.data.slice(
                header_end + inner_offset
                    ..header_end + pos.frame_len() - CrcFrame::CRC_HEADER_LENGTH,
            );
            Ok(WalRandomRead::Mapped(data))
        } else {
            let files = self.files.load();
            let file = files.get(self.layout.locate_file(pos.offset));
            let offset = self.layout.offset_in_wal_file(pos.offset);
            let header_end = offset + CrcFrame::CRC_HEADER_LENGTH as u64;
            let range = (header_end + inner_offset as u64)..(offset + pos.frame_len() as u64);
            Ok(WalRandomRead::File(FileRange::new(
                FileReader::new(file.clone(), self.layout.direct_io),
                range,
            )))
        }
    }

    fn get_map(&self, id: MapId) -> Option<Map> {
        self.maps.load().get(id).cloned()
    }

    /// Resize file to fit the specified map id
    fn extend_to_map_id(layout: &WalLayout, files: &WalFiles, map_id: MapId) -> io::Result<()> {
        let file = files.get(layout.file_for_map(map_id));
        let mut end = layout.offset_in_wal_file(layout.map_range(map_id).end);
        if end == 0 {
            // If the map range end equals wal_file_size, set the end explicitly instead of using 0
            end = layout.wal_file_size;
        }

        let len = file.metadata()?.len();
        if len < end {
            file.set_len(end)?;
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
    pub fn wal_iterator(self: &Arc<Self>, position: u64) -> Result<WalIterator, WalError> {
        WalIterator::new(self.clone(), position)
    }

    /// Returns wal writer positions after a given valid write position.
    /// If None is given as position, the returned writer writes from the beginning of the wal.
    pub fn writer_after(
        self: &Arc<Self>,
        position: Option<WalPosition>,
    ) -> Result<WalWriter, WalError> {
        let position = if let Some(position) = position {
            self.layout.next_after_wal_position(position)
        } else {
            0
        };
        let iterator = self.wal_iterator(position)?;
        Ok(iterator.into_writer(None))
    }

    /// Ensure the file is written to disk (blocking call).
    pub fn fsync(&self) -> io::Result<()> {
        self.files.load().current_file().sync_all()
    }

    /// Get the minimum WAL position based on the oldest WAL file
    pub fn min_wal_position(&self) -> u64 {
        self.files.load().min_file_id.0 * self.layout.wal_file_size
    }

    pub fn wal_file_size(&self) -> u64 {
        self.layout.wal_file_size
    }

    /// Returns the file descriptor of the wal file
    #[cfg(test)]
    pub(crate) fn file(&self) -> File {
        self.files.load().current_file().try_clone().unwrap()
    }
}

impl WalIterator {
    fn new(wal: Arc<Wal>, position: u64) -> Result<Self, WalError> {
        let mut maps = WalMaps::default();
        let (map_id, _) = wal.layout.locate(position);
        let files = wal.files.load();
        let map = Self::make_map(&wal.layout, map_id, &files, &mut maps)?
            .expect("First map must be available"); // todo check this is actually true
        Ok(Self {
            maps,
            wal,
            position,
            map,
        })
    }

    #[allow(clippy::should_implement_trait)] // todo better name
    pub fn next(&mut self) -> Result<(WalPosition, Bytes), WalError> {
        let frame = self.read_one();
        let frame = if matches!(frame, Err(WalError::Crc(CrcReadError::SkipMarker))) {
            // handle skip marker - jump to next frag
            let next_map = self.map.id.next_map();
            self.position = self.wal.layout.map_range(next_map).start;
            self.read_one()?
        } else {
            frame?
        };
        let position = WalPosition::new(
            self.position,
            (frame.len() + CrcFrame::CRC_HEADER_LENGTH) as u32,
        );
        self.position += self
            .wal
            .layout
            .align((frame.len() + CrcFrame::CRC_HEADER_LENGTH) as u64);
        Ok((position, frame))
    }

    fn read_one(&mut self) -> Result<Bytes, WalError> {
        let (map_id, offset) = self.wal.layout.locate(self.position);
        if self.map.id != map_id {
            let files = self.wal.files.load();
            let Some(map) = Self::make_map(&self.wal.layout, map_id, &files, &mut self.maps)?
            else {
                return Err(WalError::EndOfWal);
            };
            self.map = map;
        }
        Ok(CrcFrame::read_from_bytes(&self.map.data, offset as usize)?)
    }

    fn make_map(
        layout: &WalLayout,
        map_id: MapId,
        files: &WalFiles,
        maps: &mut WalMaps,
    ) -> Result<Option<Map>, WalError> {
        Wal::extend_to_map_id(layout, files, map_id)?;
        let Some(file) = files.get_checked(layout.file_for_map(map_id)) else {
            return Ok(None);
        };
        Ok(Some(maps.map(file, layout, map_id).clone()))
    }

    pub fn into_writer(self, position_override: Option<u64>) -> WalWriter {
        let position = position_override.unwrap_or(self.position);

        self.clear_fragment_from_position(position);

        let syncer = WalSyncer::start(self.wal.metrics.clone());
        self.wal.maps.store(Arc::new(self.maps));
        let mapper = WalMapper::start(
            self.wal.maps.clone(),
            self.wal.files.clone(),
            self.wal.layout.clone(),
            syncer,
            self.wal.metrics.clone(),
        );
        assert_eq!(self.wal.layout.locate(position).0, self.map.id);
        let wal_tracker = WalTracker::start(
            self.wal.layout.clone(),
            mapper,
            LastProcessed::new(position),
        );
        let allocator = WalAllocator::new(self.wal.layout.clone(), position);
        WalWriter {
            wal: self.wal,
            allocator,
            wal_tracker,
            #[allow(clippy::default_constructed_unit_structs)]
            fp: WalFailPoints::default(),
        }
    }

    /// Fills fragment with zeroes from given position to the end of the fragment.
    fn clear_fragment_from_position(&self, position: u64) {
        let (map_id, offset) = self.wal.layout.locate(position);
        assert_eq!(map_id, self.map.id, "position must be in current map");

        let map_range = self.wal.layout.map_range(map_id);
        let fragment_size = (map_range.end - map_range.start) as usize;
        let bytes_to_zero = fragment_size - offset as usize;

        if bytes_to_zero > 0 {
            let buf = write_buf_at(&self.map.data, offset as usize, bytes_to_zero);
            buf.fill(0);
        }
    }

    pub fn wal(&self) -> &Wal {
        &self.wal
    }
}

impl WalRandomRead {
    pub fn read_type(&self) -> ReadType {
        match self {
            WalRandomRead::Mapped(_) => ReadType::Mapped,
            WalRandomRead::File(_) => ReadType::Syscall,
        }
    }
}

impl RandomRead for WalRandomRead {
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

#[allow(clippy::mut_from_ref)] // todo look more into it?
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

#[allow(dead_code)]
#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    Crc(CrcReadError),
    EndOfWal,
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

// WalFailPoints definitions
#[cfg(not(test))]
#[derive(Default)]
pub(crate) struct WalFailPoints;

#[cfg(test)]
pub(crate) struct WalFailPoints(pub(crate) ArcSwap<WalFailPointsInner>);

#[cfg(test)]
#[derive(Default)]
pub(crate) struct WalFailPointsInner {
    pub fp_multi_write_before_write_buf: crate::failpoints::FailPoint,
}

#[cfg(test)]
impl Default for WalFailPoints {
    fn default() -> Self {
        Self(ArcSwap::from_pointee(WalFailPointsInner::default()))
    }
}

#[cfg(not(test))]
impl WalFailPoints {
    pub fn fp_multi_write_before_write_buf(&self) {}
}

#[cfg(test)]
impl WalFailPoints {
    pub fn fp_multi_write_before_write_buf(&self) {
        self.0.load().fp_multi_write_before_write_buf.fp();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::layout::WalKind;
    use bytes::{BufMut, BytesMut};
    use std::collections::HashSet;

    #[test]
    fn test_wal() {
        let dir = tempdir::TempDir::new("test-wal").unwrap();
        let layout = WalLayout {
            frag_size: 1024,
            max_maps: 3,
            direct_io: false,
            wal_file_size: 10 << 12,
            kind: WalKind::Replay,
        };
        // todo - add second test case when there is no space for skip marker after large
        let large = vec![1u8; 1024 - 8 - CrcFrame::CRC_HEADER_LENGTH * 3 - 9];
        {
            let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
            let writer = wal.wal_iterator(0).unwrap().into_writer(None);
            let pos = writer
                .write(&PreparedWalWrite::new(&vec![1, 2, 3]))
                .unwrap();
            let data = wal.read(*pos.wal_position()).unwrap();
            assert_eq!(&[1, 2, 3], data.1.as_ref().unwrap().as_ref());
            let pos = writer.write(&PreparedWalWrite::new(&vec![])).unwrap();
            let data = wal.read(*pos.wal_position()).unwrap();
            assert_eq!(&[] as &[u8], data.1.as_ref().unwrap().as_ref());
            drop(data);
            let pos = writer.write(&PreparedWalWrite::new(&large)).unwrap();
            let data = wal.read(*pos.wal_position()).unwrap();
            assert_eq!(&large, data.1.unwrap().as_ref());
        }
        {
            let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
            let mut wal_iterator = wal.wal_iterator(0).unwrap();
            assert_bytes(&[1, 2, 3], wal_iterator.next());
            assert_bytes(&[], wal_iterator.next());
            assert_bytes(&large, wal_iterator.next());
            wal_iterator.next().expect_err("Error expected");
            let writer = wal_iterator.into_writer(None);
            let pos = writer
                .write(&PreparedWalWrite::new(&vec![91, 92, 93]))
                .unwrap();
            assert_eq!(pos.wal_position().offset(), 1024); // assert we skipped over to next frag
            let data = wal.read(*pos.wal_position()).unwrap();
            assert_eq!(&[91, 92, 93], data.1.unwrap().as_ref());
        }
        {
            let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
            let mut wal_iterator = wal.wal_iterator(0).unwrap();
            let p1 = assert_bytes(&[1, 2, 3], wal_iterator.next());
            let p2 = assert_bytes(&[], wal_iterator.next());
            let p3 = assert_bytes(&large, wal_iterator.next());
            let p4 = assert_bytes(&[91, 92, 93], wal_iterator.next());
            wal_iterator.next().expect_err("Error expected");
            let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
            assert_eq!(&[1, 2, 3], wal.read(p1).unwrap().1.unwrap().as_ref());
            assert_eq!(&[] as &[u8], wal.read(p2).unwrap().1.unwrap().as_ref());
            assert_eq!(&large, wal.read(p3).unwrap().1.unwrap().as_ref());
            assert_eq!(&[91, 92, 93], wal.read(p4).unwrap().1.unwrap().as_ref());
        }
        // we wrote into two frags
        // assert_eq!(2048, fs::metadata(file).unwrap().len());
    }

    #[test]
    fn test_concurrent_wal_write() {
        println!("Phase 1");
        let dir = tempdir::TempDir::new("test-wal").unwrap();
        let layout = WalLayout {
            frag_size: 512,
            max_maps: 4,
            direct_io: false,
            wal_file_size: 10 << 12,
            kind: WalKind::Replay,
        };
        let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
        let wal_writer = wal.wal_iterator(0).unwrap().into_writer(None);
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
        println!("Phase 2");
        let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
        let mut iterator = wal.wal_iterator(0).unwrap();
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
    fn test_wal_tracker_integration() {
        let dir = tempdir::TempDir::new("test_wal_tracker").unwrap();
        let layout = WalLayout {
            frag_size: 1024,
            max_maps: 16,
            direct_io: false,
            wal_file_size: 10 << 12,
            kind: WalKind::Replay,
        };
        let wal = Wal::open(dir.path(), layout, Metrics::new()).unwrap();
        let wal_iterator = wal.wal_iterator(0).unwrap();
        let writer = wal_iterator.into_writer(None);

        // Write some data and get a guard
        let data = vec![1, 2, 3, 4, 5];
        let prepared_write = PreparedWalWrite::new(&data);
        let guard = writer.write(&prepared_write).unwrap();
        let guard_position = *guard.wal_position();

        // Wait a bit to let any immediate processing settle
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Get initial last_processed from the writer
        let initial_last_processed = writer.last_processed();

        // Initial last_processed should be less than or equal to guard position
        assert!(
            initial_last_processed.as_u64() <= guard_position.offset(),
            "initial last_processed ({}) should be <= guard position ({})",
            initial_last_processed.as_u64(),
            guard_position.offset()
        );

        // Drop the guard
        drop(guard);
        std::thread::sleep(std::time::Duration::from_millis(20));

        // Check that last_processed is greater than the guard position
        let final_last_processed = writer.last_processed();
        assert!(
            final_last_processed.as_u64() > guard_position.offset(),
            "final last_processed ({}) should be > guard position ({})",
            final_last_processed.as_u64(),
            guard_position.offset()
        );
    }

    #[test]
    fn test_wal_resize() {
        let dir = tempdir::TempDir::new("test_wal_resize").unwrap();
        let file_path = dir.path().join("wal_0000000000000000");
        let frag_size = 512;
        let layout = WalLayout {
            frag_size,
            max_maps: 3,
            direct_io: false,
            wal_file_size: 10 << 12,
            kind: WalKind::Replay,
        };

        // Write an entry into the WAL and extract just the position (not the guard)
        let position = {
            let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
            let writer = wal.wal_iterator(0).unwrap().into_writer(None);
            writer
                .write(&PreparedWalWrite::new(&vec![1, 2, 3]))
                .unwrap()
                .into_wal_position()
        };
        // Guard dropped immediately above, WalWriter drops here and joins background threads

        // Corrupt the file length
        let file = OpenOptions::new().write(true).open(&file_path).unwrap();
        let len = file.metadata().unwrap().len();
        file.set_len(len - frag_size / 2).unwrap();
        assert_ne!(file.metadata().unwrap().len() % frag_size, 0);

        // Re-open the WAL and ensure it resizes correctly
        let wal = Wal::open(dir.path(), layout, Metrics::new()).unwrap();
        let data = wal.read(position).unwrap();
        assert_eq!(&[1, 2, 3], data.1.unwrap().as_ref());
        assert_eq!(file.metadata().unwrap().len() % frag_size, 0);
    }

    #[test]
    fn test_multi_file_wal() {
        let dir = tempdir::TempDir::new("test-multi-file-wal").unwrap();
        let layout = WalLayout {
            frag_size: 1024,
            max_maps: 3,
            direct_io: false,
            wal_file_size: 8192,
            kind: WalKind::Replay,
        };
        let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
        let writer = wal.wal_iterator(0).unwrap().into_writer(None);
        for i in 0..100 {
            let mut data = vec![0; 256];
            data[0] = i as u8;
            writer.write(&PreparedWalWrite::new(&data)).unwrap();
        }

        // Check that multiple WAL files were created
        let wal_files = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|entry| {
                let path = entry.ok()?.path();
                let name = path.file_name()?.to_str()?;
                if name.starts_with("wal_") {
                    Some(name.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(wal_files.len(), 5);

        let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
        let mut wal_iterator = wal.wal_iterator(0).unwrap();

        for i in 0..100 {
            let (_, data) = wal_iterator.next().unwrap();
            assert!(data[0] == i as u8 && data.len() == 256);
        }
    }

    #[test]
    fn test_wal_random_reader_at() {
        use rand::{Rng, SeedableRng};

        let dir = tempdir::TempDir::new("test-wal-random-reader").unwrap();
        let layout = WalLayout {
            frag_size: 4096, // 4KB as requested
            max_maps: 16,
            direct_io: false,
            wal_file_size: 1024 << 12, // 4MB to handle 1000 writes
            kind: WalKind::Replay,
        };

        let wal = Arc::new(Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap());
        let writer = wal.wal_iterator(0).unwrap().into_writer(None);

        // Use a seeded RNG for reproducibility
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        // Store written data and their positions for verification
        let mut written_data: Vec<(WalPosition, Vec<u8>)> = Vec::new();

        // Write 1000 random-sized values
        for i in 0..1000 {
            let size = rng.gen_range(0..=1024);
            let mut data = vec![0u8; size];
            rng.fill(&mut data[..]);

            // Also add a marker at the beginning to help with debugging
            if size >= 4 {
                data[0..4].copy_from_slice(&(i as u32).to_le_bytes());
            }

            let prepared = PreparedWalWrite::new(&data);
            let guard = writer.write(&prepared).unwrap();
            let pos = *guard.wal_position();

            written_data.push((pos, data));
        }

        // Now read each position with random offsets
        for (i, (pos, original_data)) in written_data.iter().enumerate() {
            // Skip if the data is empty
            if original_data.is_empty() {
                continue;
            }

            // Generate a random offset within the data
            let max_offset = original_data.len();
            let random_offset = rng.gen_range(0..max_offset);

            // Read using random_reader_at
            let reader = wal.random_reader_at(*pos, random_offset).unwrap();

            // Extract the data from the reader using the RandomRead trait
            use crate::lookup::RandomRead;
            let len = reader.len();
            let read_data = reader.read(0..len).to_vec();

            // Verify the read data matches the original data from the offset
            let expected_data = &original_data[random_offset..];
            assert_eq!(
                read_data.as_slice(),
                expected_data,
                "Entry {}: Data mismatch at offset {}. Size was {}",
                i,
                random_offset,
                original_data.len()
            );
        }

        // Also test reading the full data (offset 0) for a subset of entries
        for (i, (pos, original_data)) in written_data.iter().enumerate() {
            if original_data.is_empty() {
                continue;
            }

            let reader = wal.random_reader_at(*pos, 0).unwrap();
            use crate::lookup::RandomRead;
            let read_data = reader.read(0..reader.len()).to_vec();

            assert_eq!(
                read_data.as_slice(),
                original_data.as_slice(),
                "Entry {} (full read): Data mismatch",
                i
            );
        }
    }

    #[test]
    fn test_writer_after() {
        let dir = tempdir::TempDir::new("test-writer-after").unwrap();
        let layout = WalLayout {
            frag_size: 4096,
            max_maps: 16,
            direct_io: false,
            wal_file_size: 1024 << 12, // 4MB
            kind: WalKind::Replay,
        };

        let pos1 = {
            let wal = Arc::new(Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap());
            let writer = wal.writer_after(None).unwrap();
            writer
                .write(&PreparedWalWrite::new(&vec![1, 2, 3]))
                .unwrap()
                .into_wal_position()
        };

        let pos2 = {
            let wal = Arc::new(Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap());
            let writer = wal.writer_after(Some(pos1)).unwrap();
            writer
                .write(&PreparedWalWrite::new(&vec![4, 5, 6]))
                .unwrap()
                .into_wal_position()
        };

        let wal = Arc::new(Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap());
        assert_eq!(&[1, 2, 3], wal.read(pos1).unwrap().1.unwrap().as_ref());
        assert_eq!(&[4, 5, 6], wal.read(pos2).unwrap().1.unwrap().as_ref());
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
