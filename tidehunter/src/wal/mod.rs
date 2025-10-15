// Submodules
pub mod layout;
pub mod position;
mod syncer;
pub(crate) mod tracker;

use crate::context::ReadType;
use crate::crc::{CrcFrame, CrcReadError, IntoBytesFixed};
use crate::file_reader::set_direct_options;
use crate::file_reader::FileReader;
use crate::lookup::{FileRange, RandomRead};
use crate::metrics::{Metrics, TimerExt};
use arc_swap::ArcSwap;
use minibytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;
use std::time::Instant;
use std::{io, mem, ptr, thread};

#[cfg(any(test, feature = "test-utils"))]
use layout::WalKind;
use layout::WalLayout;
use position::{WalFileId, WalPosition};
use syncer::WalSyncer;
use tracker::{WalGuard, WalTracker};

pub struct WalWriter {
    wal: Arc<Wal>,
    position_and_map: Mutex<(IncrementalWalPosition, Map)>,
    wal_tracker: WalTracker,
    mapper: WalMapper,
    wal_syncer: WalSyncer,
}

pub struct Wal {
    files: Arc<ArcSwap<WalFiles>>,
    layout: WalLayout,
    maps: RwLock<BTreeMap<u64, Map>>,
    metrics: Arc<Metrics>,
}

struct WalMapper {
    jh: Option<JoinHandle<()>>,
    receiver: Option<Mutex<mpsc::Receiver<Map>>>,
}

struct WalMapperThread {
    sender: mpsc::SyncSender<Map>,
    last_map: u64,
    files: Arc<ArcSwap<WalFiles>>,
    layout: WalLayout,
    metrics: Arc<Metrics>,
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

struct WalFiles {
    base_path: PathBuf,
    files: Vec<Arc<File>>,
    min_file_id: WalFileId,
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
        let mut current_map_and_position = self.position_and_map.lock();
        let (mut pos, prev_block_end) = current_map_and_position.0.allocate_position(len_aligned);
        // todo duplicated code
        let (map_id, mut offset) = self.wal.layout.locate(pos);
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
            self.wal_syncer
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

        // Calculate the end position after all writes
        let end_pos = pos + len_aligned;
        // IMPORTANT: Must call new_batch while holding the position mutex to ensure
        // WalTracker receives positions in order
        let wal_batch = self.wal_tracker.new_batch(end_pos);

        // Dropping lock to allow data copy to be done in parallel
        drop(current_map_and_position);

        let mut guards = vec![];
        for w in writes {
            let frame_size = w.len();
            let aligned_frame_size = self.wal.layout.align(frame_size as u64);
            let buf = write_buf_at(&data, offset as usize, frame_size);
            buf.copy_from_slice(w.frame.as_ref());
            // conversion to u32 is safe - pos is less than self.frag_size,
            // and self.frag_size is asserted less than u32::MAX
            let wal_position = WalPosition::new(pos, frame_size as u32);
            guards.push(wal_batch.guard(wal_position));
            pos += aligned_frame_size;
            offset += aligned_frame_size;
        }
        Ok(guards)
    }

    /// Current un-initialized position,
    /// not to be used as WalPosition, only as a metric to see how many bytes were written
    pub fn position(&self) -> u64 {
        self.position_and_map.lock().0.position
    }

    /// Returns the last processed position from the WalTracker
    pub fn last_processed(&self) -> u64 {
        self.wal_tracker.last_processed()
    }

    /// Deletes WAL files that have been fully processed by the relocation process up to the watermark position.
    ///
    /// Given watermark positions will be preserved.
    pub fn gc(&self, watermark: u64) -> io::Result<()> {
        let wal_files = self.wal.files.load();
        let mut new_min_file_id = None;
        for idx in 0..wal_files.files.len() {
            let file_id = WalFileId(wal_files.min_file_id.0 + idx as u64);
            if (file_id.0 + 1) * self.wal.layout.wal_file_size >= watermark {
                break;
            }
            let path = self.wal.layout.wal_file_name(&wal_files.base_path, file_id);
            if path.exists() {
                std::fs::remove_file(path)?;
            }
            new_min_file_id = Some(file_id);
        }
        if let Some(new_min_file_id) = new_min_file_id {
            let threshold =
                (new_min_file_id.0 + 1) * self.wal.layout.wal_file_size / self.wal.layout.frag_size;
            let mut maps = self.wal.maps.write();
            maps.retain(|map_id, _| *map_id >= threshold);
        }
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

impl WalFiles {
    fn new(base_path: &Path, layout: &WalLayout) -> io::Result<Arc<ArcSwap<Self>>> {
        let wal_files = Self::load(base_path, layout)?;
        Ok(Arc::new(ArcSwap::from(Arc::new(wal_files))))
    }

    fn load(base_path: &Path, layout: &WalLayout) -> io::Result<Self> {
        let mut files = vec![];
        for entry in std::fs::read_dir(base_path)? {
            let file_path = entry?.path();
            if file_path.is_file() {
                if let Some(file_name) = file_path.file_name().and_then(|name| name.to_str()) {
                    if let Some(id_str) = file_name.strip_prefix(layout.kind.name()) {
                        let Some(id_str) = id_str.strip_prefix("_") else {
                            panic!(
                                "invalid wal file name {:?}(failed to strip _ prefix)",
                                file_name
                            );
                        };
                        let id = u64::from_str_radix(id_str, 16)
                            .unwrap_or_else(|_| panic!("invalid wal file name {:?}", file_name));
                        let file = Wal::open_file(&file_path, layout)?;
                        files.push((id, file));
                    }
                }
            }
        }
        if files.is_empty() {
            let file = Wal::open_file(&layout.wal_file_name(base_path, WalFileId(0)), layout)?;
            files.push((0, file));
        }
        files.sort_by_key(|(id, _)| *id);
        let min_file_id = files[0].0;
        assert_eq!(
            files[files.len() - 1].0 - min_file_id + 1,
            files.len() as u64,
            "WAL file IDs must form a contiguous range",
        );
        Ok(Self {
            base_path: base_path.to_path_buf(),
            files: files.into_iter().map(|(_, file)| Arc::new(file)).collect(),
            min_file_id: WalFileId(min_file_id),
        })
    }

    fn current_file_id(&self) -> WalFileId {
        WalFileId(self.min_file_id.0 + self.files.len() as u64 - 1)
    }

    #[inline]
    fn current_file(&self) -> &Arc<File> {
        self.files.last().expect("unable to find current WAL file")
    }

    fn get_checked(&self, id: WalFileId) -> Option<&Arc<File>> {
        id.0.checked_sub(self.min_file_id.0)
            .and_then(|id| self.files.get(id as usize))
    }

    fn get(&self, id: WalFileId) -> &Arc<File> {
        self.get_checked(id)
            .unwrap_or_else(|| panic!("attempt to access non existing file {:?}", id))
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

    fn open_file(path: &Path, layout: &WalLayout) -> io::Result<File> {
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

    // todo remove
    #[doc(hidden)]
    #[cfg(test)]
    pub fn read(&self, pos: WalPosition) -> Result<Bytes, WalError> {
        assert_ne!(
            pos,
            WalPosition::INVALID,
            "Trying to read invalid wal position"
        );
        let (map, offset) = self.layout.locate(pos.offset);
        let map = self.map(map, false)?;
        // todo avoid clone, introduce Bytes::slice_in_place
        Ok(CrcFrame::read_from_bytes(&map.data, offset as usize)?)
    }

    /// Read the wal position without mapping.
    /// If mapping exists, it is still used for reading
    /// if mapping does not exist the read syscall is used instead.
    ///
    /// This method returns what type of read was used along with bytes read.
    pub fn read_unmapped(&self, pos: WalPosition) -> Result<(ReadType, Option<Bytes>), WalError> {
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
                        .offset(self.layout.offset_in_wal_file(range.start))
                        .len(self.layout.frag_size as usize);
                    let files = self.files.load();
                    let file = files.get(self.layout.locate_file(range.start));
                    if writeable {
                        options /*.populate()*/
                            .map_mut(file)?
                            .into()
                    } else {
                        options.map(file)?.into()
                    }
                };
                let map = Map {
                    id,
                    writeable,
                    data,
                };
                va.insert(map)
            }
            Entry::Occupied(oc) => oc.into_mut(),
        };
        let map = map.clone();
        if maps.len() > self.layout.max_maps {
            maps.pop_first();
        }
        Ok(map)
    }

    /// Resize file to fit the specified map id
    fn extend_to_map(layout: &WalLayout, files: &WalFiles, position: u64) -> io::Result<()> {
        let (map_id, _) = layout.locate(position);
        let file = files.get(layout.locate_file(position));
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
    /// If WalPosition::INVALID is specified, iterate from start
    pub fn wal_iterator(self: &Arc<Self>, position: u64) -> Result<WalIterator, WalError> {
        let (map_id, _) = self.layout.locate(position);
        Self::extend_to_map(&self.layout, &self.files.load(), position)?;
        let map = self.map(map_id, true)?;
        let iterator = WalIterator {
            wal: self.clone(),
            position,
            map,
        };
        Ok(iterator)
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
        Ok(iterator.into_writer())
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

impl WalMapper {
    pub fn start(
        last_map: u64,
        files: Arc<ArcSwap<WalFiles>>,
        layout: WalLayout,
        metrics: Arc<Metrics>,
    ) -> Self {
        let (sender, receiver) = mpsc::sync_channel(2);
        let this = WalMapperThread {
            last_map,
            files,
            layout,
            sender,
            metrics,
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
            let timer = Instant::now();
            let map_id = self.last_map + 1;
            let range = self.layout.map_range(map_id);
            let file_id = self.layout.locate_file(range.start);
            let mut files = self.files.load();
            // if min_file_id has already been removed by relocation, reload the list of active files
            if !self
                .layout
                .wal_file_name(&files.base_path, files.min_file_id)
                .exists()
            {
                let new_files = WalFiles::load(&files.base_path, &self.layout)
                    .expect("Failed to reload wal files directory");
                self.files.store(Arc::new(new_files));
                files = self.files.load();
            }
            if file_id > files.current_file_id() {
                assert_eq!(file_id, WalFileId(files.current_file_id().0 + 1));
                let mut new_files = files.files.clone();
                let new_file_path = self.layout.wal_file_name(&files.base_path, file_id);
                let new_file = Wal::open_file(&new_file_path, &self.layout)
                    .expect("Failed to create new wal file");
                new_files.push(Arc::new(new_file));

                let new_wal_files = WalFiles {
                    base_path: files.base_path.clone(),
                    files: new_files,
                    min_file_id: files.min_file_id,
                };
                self.files.store(Arc::new(new_wal_files));
                files = self.files.load();
            }
            Wal::extend_to_map(&self.layout, &files, range.start)
                .expect("Failed to extend wal file");
            let data = unsafe {
                let mut options = memmap2::MmapOptions::new();
                options
                    .offset(self.layout.offset_in_wal_file(range.start))
                    .len(self.layout.frag_size as usize);
                options
                    .populate()
                    .map_mut(files.get(file_id))
                    .expect("Failed to mmap on wal file")
                    .into()
            };
            let map = Map {
                id: map_id,
                writeable: true,
                data,
            };
            self.last_map = map_id;
            self.metrics
                .wal_mapper_time_mcs
                .inc_by(timer.elapsed().as_micros() as u64);
            // todo ideally figure out a way to not create a map when sender closes
            if self.sender.send(map).is_err() {
                return;
            }
        }
    }
}

impl WalIterator {
    #[allow(clippy::should_implement_trait)] // todo better name
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
        // todo duplicated code
        let (map_id, offset) = self.wal.layout.locate(self.position);
        if self.map.id != map_id {
            Wal::extend_to_map(&self.wal.layout, &self.wal.files.load(), self.position)?;
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
            Arc::clone(&self.wal.files),
            self.wal.layout.clone(),
            self.wal().metrics.clone(),
        );
        assert_eq!(self.wal.layout.locate(position.position).0, self.map.id);
        let wal_tracker = WalTracker::start(position.position);
        let position_and_map = (position, self.map);
        let position_and_map = Mutex::new(position_and_map);
        let wal_syncer = WalSyncer::start(self.wal.metrics.clone());
        WalWriter {
            wal: self.wal,
            position_and_map,
            wal_tracker,
            wal_syncer,
            mapper,
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

#[doc(hidden)] // Used by tools/wal_inspector for progress tracking
#[cfg(any(test, feature = "test-utils"))]
pub fn list_wal_files_with_sizes(base_path: &Path) -> io::Result<Vec<(PathBuf, u64)>> {
    let prefix = format!("{}_", WalKind::Replay.name());
    let mut files = vec![];

    for entry in std::fs::read_dir(base_path)? {
        let file_path = entry?.path();
        if file_path.is_file() {
            if let Some(file_name) = file_path.file_name().and_then(|name| name.to_str()) {
                if let Some(id_str) = file_name.strip_prefix(&prefix) {
                    if u64::from_str_radix(id_str, 16).is_ok() {
                        let metadata = std::fs::metadata(&file_path)?;
                        files.push((file_path, metadata.len()));
                    }
                }
            }
        }
    }

    // If no WAL files found, check for the default wal_0000000000000000 file
    if files.is_empty() {
        let default_wal_path = base_path.join(format!("{}{:016x}", prefix, 0));
        if default_wal_path.exists() {
            let metadata = std::fs::metadata(&default_wal_path)?;
            files.push((default_wal_path, metadata.len()));
        }
    }

    // Sort by file name (which corresponds to WAL file ID)
    files.sort_by(|(a, _), (b, _)| {
        let a_name = a.file_name().and_then(|n| n.to_str()).unwrap_or("");
        let b_name = b.file_name().and_then(|n| n.to_str()).unwrap_or("");
        a_name.cmp(b_name)
    });

    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use std::collections::HashSet;

    #[test]
    fn test_wal() {
        let dir = tempdir::TempDir::new("test-wal").unwrap();
        let layout = WalLayout {
            frag_size: 1024,
            max_maps: 2,
            direct_io: false,
            wal_file_size: 10 << 12,
            kind: WalKind::Replay,
        };
        // todo - add second test case when there is no space for skip marker after large
        let large = vec![1u8; 1024 - 8 - CrcFrame::CRC_HEADER_LENGTH * 3 - 9];
        {
            let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
            let writer = wal.wal_iterator(0).unwrap().into_writer();
            let pos = writer
                .write(&PreparedWalWrite::new(&vec![1, 2, 3]))
                .unwrap();
            let data = wal.read(*pos.wal_position()).unwrap();
            assert_eq!(&[1, 2, 3], data.as_ref());
            let pos = writer.write(&PreparedWalWrite::new(&vec![])).unwrap();
            let data = wal.read(*pos.wal_position()).unwrap();
            assert_eq!(&[] as &[u8], data.as_ref());
            drop(data);
            let pos = writer.write(&PreparedWalWrite::new(&large)).unwrap();
            let data = wal.read(*pos.wal_position()).unwrap();
            assert_eq!(&large, data.as_ref());
        }
        {
            let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
            let mut wal_iterator = wal.wal_iterator(0).unwrap();
            assert_bytes(&[1, 2, 3], wal_iterator.next());
            assert_bytes(&[], wal_iterator.next());
            assert_bytes(&large, wal_iterator.next());
            wal_iterator.next().expect_err("Error expected");
            let writer = wal_iterator.into_writer();
            let pos = writer
                .write(&PreparedWalWrite::new(&vec![91, 92, 93]))
                .unwrap();
            assert_eq!(pos.wal_position().offset(), 1024); // assert we skipped over to next frag
            let data = wal.read(*pos.wal_position()).unwrap();
            assert_eq!(&[91, 92, 93], data.as_ref());
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
            assert_eq!(&[1, 2, 3], wal.read(p1).unwrap().as_ref());
            assert_eq!(&[] as &[u8], wal.read(p2).unwrap().as_ref());
            assert_eq!(&large, wal.read(p3).unwrap().as_ref());
            assert_eq!(&[91, 92, 93], wal.read(p4).unwrap().as_ref());

            assert_eq!(
                &[1, 2, 3],
                wal.read_unmapped(p1).unwrap().1.unwrap().as_ref()
            );
            assert_eq!(
                &[] as &[u8],
                wal.read_unmapped(p2).unwrap().1.unwrap().as_ref()
            );
            assert_eq!(&large, wal.read_unmapped(p3).unwrap().1.unwrap().as_ref());
            assert_eq!(
                &[91, 92, 93],
                wal.read_unmapped(p4).unwrap().1.unwrap().as_ref()
            );
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
            wal_file_size: 10 << 12,
            kind: WalKind::Replay,
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
        let layout = WalLayout {
            frag_size: 512,
            max_maps: 2,
            direct_io: false,
            wal_file_size: 10 << 12,
            kind: WalKind::Replay,
        };
        let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
        let wal_writer = wal.wal_iterator(0).unwrap().into_writer();
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
        let writer = wal_iterator.into_writer();

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
            initial_last_processed <= guard_position.offset(),
            "initial last_processed ({}) should be <= guard position ({})",
            initial_last_processed,
            guard_position.offset()
        );

        // Drop the guard
        drop(guard);
        std::thread::sleep(std::time::Duration::from_millis(20));

        // Check that last_processed is greater than the guard position
        let final_last_processed = writer.last_processed();
        assert!(
            final_last_processed > guard_position.offset(),
            "final last_processed ({}) should be > guard position ({})",
            final_last_processed,
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
            max_maps: 2,
            direct_io: false,
            wal_file_size: 10 << 12,
            kind: WalKind::Replay,
        };

        // Write an entry into the WAl
        let position = {
            let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
            let writer = wal.wal_iterator(0).unwrap().into_writer();
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
        let wal = Wal::open(dir.path(), layout, Metrics::new()).unwrap();
        let data = wal.read(*position.wal_position()).unwrap();
        assert_eq!(&[1, 2, 3], data.as_ref());
        assert_eq!(file.metadata().unwrap().len() % frag_size, 0);
    }

    #[test]
    fn test_multi_file_wal() {
        let dir = tempdir::TempDir::new("test-multi-file-wal").unwrap();
        let layout = WalLayout {
            frag_size: 1024,
            max_maps: 2,
            direct_io: false,
            wal_file_size: 8192,
            kind: WalKind::Replay,
        };
        let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
        let writer = wal.wal_iterator(0).unwrap().into_writer();
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
        let writer = wal.wal_iterator(0).unwrap().into_writer();

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
            *writer
                .write(&PreparedWalWrite::new(&vec![1, 2, 3]))
                .unwrap()
                .wal_position()
        };

        let pos2 = {
            let wal = Arc::new(Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap());
            let writer = wal.writer_after(Some(pos1)).unwrap();
            *writer
                .write(&PreparedWalWrite::new(&vec![4, 5, 6]))
                .unwrap()
                .wal_position()
        };

        let wal = Arc::new(Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap());
        assert_eq!(&[1, 2, 3], wal.read(pos1).unwrap().as_ref());
        assert_eq!(&[4, 5, 6], wal.read(pos2).unwrap().as_ref());
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
