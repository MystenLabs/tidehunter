// Submodules
pub(crate) mod allocator;
pub(crate) mod files;
pub mod layout;
mod mapper;
pub mod position;
mod syncer;
pub(crate) mod tracker;
pub(crate) mod tracking_mmap;

use crate::context::ReadType;
use crate::crc::{CrcFrame, CrcReadError, IntoBytesFixed};
use crate::file_reader::FileReader;
use crate::file_reader::set_direct_options;
use crate::index_blob_cache::IndexBlobFill;
use crate::lookup::{FileRange, RandomRead};
use crate::metrics::{MetricIntGauge, Metrics};
use arc_swap::ArcSwap;
use minibytes::Bytes;
use std::collections::BTreeSet;
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
use mapper::{INITIAL_MAPS_BUFFER, WalMapper};
use position::{LastProcessed, MapId, WalPosition};
use syncer::WalSyncer;
use tracker::{SharedLastProcessed, WalGuard, WalTracker, WalTrackerLatch};

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
    skip_crc: bool,
    /// When false, the iterator does not populate `maps` (each fragment
    /// it mmaps is dropped as soon as the cursor advances past it) and
    /// `into_writer` panics — the writer needs the accumulated mmaps as
    /// its initial `WalMaps`. Set by `new_for_scan` vs `new_for_writer`.
    for_writer: bool,
}

#[derive(Clone)]
// todo only pub between wal.rs and wal_syncer.rs
pub(crate) struct Map {
    id: MapId,
    pub data: Bytes,
    writeable: bool,
    /// Whether the pages were pre-faulted at mmap time (`MAP_POPULATE`).
    /// False only for the read-only historical maps restored by
    /// `WalIterator::premap_live_fragments`; see `Wal::frame_source` for
    /// how reads treat them.
    populated: bool,
}

pub enum WalRandomRead {
    Mapped(Bytes),
    File(FileRange),
    /// A whole index blob resident in the index blob cache; reads are
    /// slices of it (see `index_blob_cache`).
    Cached(Bytes),
    /// A syscall-path index blob admitted to the index blob cache. Built
    /// under the row lock from the `File` reader; the first read, after the
    /// lock is released, fills the cache or joins an in-flight fill, and
    /// every read is then a slice of the cached blob.
    CachedFill(IndexBlobFill),
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
            // The in-memory flat index packs the entry kind into the top 2 bits of the
            // 4-byte WAL frame-length field, so frame sizes must fit in 30 bits (< 1 GiB).
            assert!(
                frame_size < (1 << 30),
                "WAL frame size {frame_size} exceeds 1 GiB limit"
            );
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
        let mut attempts: usize = 0;
        loop {
            if let Some(map) = self.wal.get_map(map) {
                assert!(map.writeable, "Map is not writable");
                return (map, offset as usize);
            }
            self.wal.metrics.wal_write_wait.inc();
            thread::sleep(Duration::from_millis(1));
            attempts += 1;
            // ~Once a second, sanity-check that the mapper thread is
            // still alive. If it died (panic, OOM, anything), no one
            // will ever create this map and the writer would otherwise
            // spin forever; surface that as a loud panic instead.
            if attempts.is_multiple_of(1000) && !self.wal_tracker.is_mapper_alive() {
                panic!(
                    "wal-mapper thread is no longer running; \
                     writer cannot make progress on map {map:?}"
                );
            }
            if attempts.is_multiple_of(10 * 1000) {
                println!(
                    "Still waiting for writable map {map:?} after {} seconds",
                    attempts / 1000
                );
            }
        }
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

    /// Acquires a latch that pins the externally observed `last_processed`
    /// position (see [`WalTrackerLatch`]). Blocks until every position below the
    /// latch's [`WalTrackerLatch::position`] (the received frontier captured
    /// when the latch was acquired) has been processed into the in-memory index.
    /// While the returned latch is held, [`Self::last_processed`] will not
    /// advance past that position.
    pub fn latch(&self) -> WalTrackerLatch {
        self.wal_tracker.latch()
    }

    /// Test hook: see [`WalTracker::latch_requests_sent`].
    #[cfg(any(test, feature = "test-utils"))]
    pub fn latch_requests_sent(&self) -> u64 {
        self.wal_tracker.latch_requests_sent()
    }

    /// Releases a latch acquired via [`Self::latch`] (see
    /// [`WalTracker::release_latch`]).
    pub fn release_latch(&self, latch: WalTrackerLatch) {
        self.wal_tracker.release_latch(latch)
    }

    /// Blocks until every WAL position allocated before this call is recorded
    /// in the in-memory index (see [`WalTracker::wait_processed`]). Pins
    /// nothing, unlike [`Self::latch`].
    pub fn wait_processed(&self) {
        self.wal_tracker.wait_processed()
    }

    /// Test hook: see [`WalTracker::wait_requests_sent`].
    #[cfg(any(test, feature = "test-utils"))]
    pub fn wait_requests_sent(&self) -> u64 {
        self.wal_tracker.wait_requests_sent()
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
            .with_label_values(&[self.wal.layout.label.as_str()])
            .set(watermark as i64);
        Ok(())
    }

    /// Requests deletion of a specific set of WAL files (gaps allowed).
    /// Read-only maps over those files are dropped; files that still have a
    /// writeable map are skipped and will be reconsidered on a later call
    /// once their maps have been evicted. Blocks until the mapper has
    /// removed the deletable entries from the file map; the actual
    /// `remove_file` syscalls run on a background unlink worker.
    pub fn delete_files(&self, files: Vec<position::WalFileId>) -> io::Result<()> {
        if files.is_empty() {
            return Ok(());
        }
        self.wal_tracker.delete_files(files);
        Ok(())
    }

    /// Waits until wal_tracker processes all in-flight messages.
    pub fn wal_tracker_barrier(&self) {
        self.wal_tracker.barrier()
    }
}

/// Where the bytes for a frame live right now: the mmap covering its
/// fragment (with the frame's offset within that map), or the WAL file for
/// a syscall read. Produced by `Wal::frame_source`; `None` there means the
/// file was garbage-collected.
enum FrameSource {
    Mapped(Map, u64),
    File(Arc<File>),
}

impl Wal {
    #[doc(hidden)] // Used by tools/tideconsole to open WAL files directly
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
    /// If a pre-faulted mapping exists, it is used for reading.
    /// Otherwise the read syscall is used instead (see `frame_source`).
    ///
    /// This method returns what type of read was used along with bytes read.
    pub fn read(&self, pos: WalPosition) -> Result<(ReadType, Option<Bytes>), WalError> {
        match self.frame_source(pos, true) {
            Some(FrameSource::Mapped(map, offset)) => {
                // using CrcFrame::read_from_slice to avoid holding the larger byte array
                Ok((
                    ReadType::Mapped,
                    Some(
                        CrcFrame::read_from_slice(&map.data, offset as usize)?
                            .to_vec()
                            .into(),
                    ),
                ))
            }
            Some(FrameSource::File(file)) => {
                let buffer_size = if self.layout.direct_io {
                    self.layout.align(pos.frame_len() as u64) as usize
                } else {
                    pos.frame_len()
                };
                let mut buf = FileReader::io_buffer_bytes(buffer_size, self.layout.direct_io);
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
            None => Ok((ReadType::Syscall, None)),
        }
    }

    /// Returns whether the frame at `pos` is still readable: its fragment
    /// is mapped, or its WAL file has not been garbage-collected. `false`
    /// corresponds exactly to the `Ok(None)` that [`Self::read`] returns
    /// for reclaimed positions (both go through [`Self::frame_source`]).
    /// Pure lookup — no I/O and no map creation.
    pub(crate) fn is_reachable(&self, pos: WalPosition) -> bool {
        self.frame_source(pos, false).is_some()
    }

    pub fn random_reader_at(
        &self,
        pos: WalPosition,
        inner_offset: usize,
    ) -> Result<WalRandomRead, WalError> {
        self.reader_at(pos, inner_offset, false)
    }

    /// Like [`Self::random_reader_at`], for a caller that will read the whole
    /// frame: a fragment whose pages were not pre-faulted is served through
    /// the file, so the frame comes in one `pread` instead of a fault per
    /// page (see `frame_source`).
    pub fn whole_frame_reader_at(
        &self,
        pos: WalPosition,
        inner_offset: usize,
    ) -> Result<WalRandomRead, WalError> {
        self.reader_at(pos, inner_offset, true)
    }

    fn reader_at(
        &self,
        pos: WalPosition,
        inner_offset: usize,
        whole_frame: bool,
    ) -> Result<WalRandomRead, WalError> {
        match self.frame_source(pos, whole_frame) {
            Some(FrameSource::Mapped(map, offset)) => {
                let offset = offset as usize;
                let header_end = offset + CrcFrame::CRC_HEADER_LENGTH;
                let data = map.data.slice(
                    header_end + inner_offset
                        ..header_end + pos.frame_len() - CrcFrame::CRC_HEADER_LENGTH,
                );
                Ok(WalRandomRead::Mapped(data))
            }
            Some(FrameSource::File(file)) => {
                let offset = self.layout.offset_in_wal_file(pos.offset);
                let header_end = offset + CrcFrame::CRC_HEADER_LENGTH as u64;
                let range = (header_end + inner_offset as u64)..(offset + pos.frame_len() as u64);
                Ok(WalRandomRead::File(FileRange::new(
                    FileReader::new(file, self.layout.direct_io),
                    range,
                )))
            }
            None => panic!(
                "attempt to access non existing file {:?}",
                self.layout.locate_file(pos.offset)
            ),
        }
    }

    /// Resolves where the bytes for the frame at `pos` come from: the mmap
    /// covering its fragment, the WAL file (syscall read), or nowhere
    /// because the file was garbage-collected. Single source of truth for
    /// position resolution — `read`, `random_reader_at` and `is_reachable`
    /// must agree on when a position is gone, so any new resolution
    /// dimension belongs here, not in a caller.
    ///
    /// `whole_frame` says the caller will copy the entire frame. A map whose
    /// pages were not pre-faulted (see `WalMaps::map_readonly`) is then
    /// resolved to the file instead: the WAL fd carries `POSIX_FADV_RANDOM`,
    /// so copying a cold frame through such a map would fault it in one page
    /// at a time, while a single `pread` fetches the whole range at once.
    /// Partial reads keep using the map; they touch a handful of pages. The
    /// file always exists while such a map does: premapping requires it at
    /// open and `delete_files` skips files that have a map.
    fn frame_source(&self, pos: WalPosition, whole_frame: bool) -> Option<FrameSource> {
        assert_ne!(
            pos,
            WalPosition::INVALID,
            "Trying to read invalid wal position"
        );
        let (map, offset) = self.layout.locate(pos.offset);
        if let Some(map) = self.get_map(map)
            && (map.populated || !whole_frame)
        {
            return Some(FrameSource::Mapped(map, offset));
        }
        self.files
            .load()
            .get_checked(self.layout.locate_file(pos.offset))
            .map(|file| FrameSource::File(file.clone()))
    }

    fn get_map(&self, id: MapId) -> Option<Map> {
        self.maps.load().get(id).cloned()
    }

    /// Resize file to fit the specified map id
    fn extend_to_map_id(layout: &WalLayout, file: &File, map_id: MapId) -> io::Result<()> {
        let end = Self::map_end_in_file(layout, map_id);
        let len = file.metadata()?.len();
        if len < end {
            file.set_len(end)?;
        }
        Ok(())
    }

    /// Whether `file` is long enough to hold all of fragment `map_id`.
    fn covers_map(layout: &WalLayout, file: &File, map_id: MapId) -> io::Result<bool> {
        Ok(file.metadata()?.len() >= Self::map_end_in_file(layout, map_id))
    }

    /// Byte offset within its WAL file at which fragment `map_id` ends.
    fn map_end_in_file(layout: &WalLayout, map_id: MapId) -> u64 {
        let end = layout.offset_in_wal_file(layout.map_range(map_id).end);
        // The last fragment of a file ends on the file boundary, which the
        // modulo maps to 0.
        if end == 0 { layout.wal_file_size } else { end }
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

    /// Iterate wal from the position after given position. The returned
    /// iterator does not retain mmapped fragments past the cursor and
    /// cannot be turned into a writer — use `wal_iterator_for_writer`
    /// for that.
    pub fn wal_iterator_for_scan(self: &Arc<Self>, position: u64) -> Result<WalIterator, WalError> {
        WalIterator::new_for_scan(self.clone(), position)
    }

    /// Like `wal_iterator_for_scan`, but retains every mmapped fragment in the
    /// iterator's `WalMaps` so they can be handed off to a `WalWriter`
    /// via `into_writer`.
    pub fn wal_iterator_for_writer(
        self: &Arc<Self>,
        position: u64,
    ) -> Result<WalIterator, WalError> {
        WalIterator::new_for_writer(self.clone(), position)
    }

    /// Returns wal writer positions after a given valid write position.
    /// If None is given as position, the returned writer writes from the beginning of the wal.
    pub fn writer_after(
        self: &Arc<Self>,
        position: Option<WalPosition>,
    ) -> Result<WalWriter, WalError> {
        self.writer_after_premapped(position, std::iter::empty())
    }

    /// Like [`Self::writer_after`], but first restores read-only mmaps over
    /// the most recent fragments below the writer that still hold `live`
    /// positions. See [`WalIterator::premap_live_fragments`] for what gets
    /// mapped and why.
    pub fn writer_after_premapped(
        self: &Arc<Self>,
        position: Option<WalPosition>,
        live: impl IntoIterator<Item = WalPosition>,
    ) -> Result<WalWriter, WalError> {
        let position = if let Some(position) = position {
            self.layout.next_after_wal_position(position)
        } else {
            0
        };
        let mut iterator = self.wal_iterator_for_writer(position)?;
        iterator.premap_live_fragments(live)?;
        Ok(iterator.into_writer(None))
    }

    /// Ensure the file is written to disk (blocking call).
    ///
    /// Covers only the newest file; frames in older files are msync'd by the
    /// background `WalSyncer` with no completion guarantee. Sufficient for
    /// WALs whose contents are re-derived by replay; use [`Self::fsync_all`]
    /// when persisted references into the whole WAL must survive a crash.
    pub fn fsync(&self) -> io::Result<()> {
        self.files.load().current_file().sync_all()
    }

    /// Ensure every live file of this WAL is written to disk (blocking call).
    ///
    /// Needed before persisting a control region that references positions
    /// anywhere in a side WAL: side WALs are not replayed on restart, so the
    /// control region is the only recovery path for those bytes.
    pub fn fsync_all(&self) -> io::Result<()> {
        for file in self.files.load().all_files() {
            file.sync_all()?;
        }
        Ok(())
    }

    /// Get the minimum WAL position based on the oldest WAL file present.
    /// For sparsely-GC'd WALs (index) this is the lowest still-live file id;
    /// it is not a guarantee that every byte below it has been reclaimed.
    pub fn min_wal_position(&self) -> u64 {
        self.files.load().min_file_id().0 * self.layout.wal_file_size
    }

    /// Snapshot of currently-open WAL file ids, ordered ascending.
    /// Used by the snapshot path to compute the set of unreferenced files
    /// to delete (sparse GC). The active writer file is included.
    pub(crate) fn file_ids(&self) -> Vec<position::WalFileId> {
        self.files.load().files.keys().copied().collect()
    }

    pub fn wal_file_size(&self) -> u64 {
        self.layout.wal_file_size
    }

    pub fn layout(&self) -> &WalLayout {
        &self.layout
    }

    /// Returns the file descriptor of the wal file
    #[cfg(test)]
    pub(crate) fn file(&self) -> File {
        self.files.load().current_file().try_clone().unwrap()
    }
}

impl WalIterator {
    /// Scan-mode iterator. Fragments are mmapped on demand for the
    /// cursor and dropped as soon as the cursor advances past them;
    /// `into_writer` panics on this iterator.
    fn new_for_scan(wal: Arc<Wal>, position: u64) -> Result<Self, WalError> {
        Self::new_impl(wal, position, false)
    }

    /// Writer-mode iterator. Every fragment the iterator opens is
    /// retained in `WalMaps` so that `into_writer` can hand them to
    /// the resulting `WalWriter`.
    fn new_for_writer(wal: Arc<Wal>, position: u64) -> Result<Self, WalError> {
        Self::new_impl(wal, position, true)
    }

    fn new_impl(wal: Arc<Wal>, position: u64, for_writer: bool) -> Result<Self, WalError> {
        let mut maps = WalMaps::default();
        let (map_id, _) = wal.layout.locate(position);
        let files = wal.files.load();
        let map = Self::make_map(
            &wal.layout,
            map_id,
            &files,
            &mut maps,
            wal.metrics.wal_mmap_bytes.clone(),
            for_writer,
        )?
        .expect("First map must be available"); // todo check this is actually true
        Ok(Self {
            maps,
            wal,
            position,
            map,
            skip_crc: false,
            for_writer,
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
            let Some(map) = Self::make_map(
                &self.wal.layout,
                map_id,
                &files,
                &mut self.maps,
                self.wal.metrics.wal_mmap_bytes.clone(),
                self.for_writer,
            )?
            else {
                return Err(WalError::EndOfWal);
            };
            self.map = map;
        }
        if self.skip_crc {
            Ok(CrcFrame::read_unchecked_from_bytes(
                &self.map.data,
                offset as usize,
            )?)
        } else {
            Ok(CrcFrame::read_from_bytes(&self.map.data, offset as usize)?)
        }
    }

    /// Skip CRC verification when reading frames.
    /// Useful for inspection tools that prioritise speed or need to read potentially
    /// corrupted WAL files.
    pub fn set_skip_crc(&mut self, skip_crc: bool) {
        self.skip_crc = skip_crc;
    }

    fn make_map(
        layout: &WalLayout,
        map_id: MapId,
        files: &WalFiles,
        maps: &mut WalMaps,
        wal_mmap_bytes: MetricIntGauge,
        for_writer: bool,
    ) -> Result<Option<Map>, WalError> {
        // Check if the file still exists before trying to extend it.
        // GC may have removed the file asynchronously (via the mapper thread)
        // between iterator steps.
        let Some(file) = files.get_checked(layout.file_for_map(map_id)) else {
            return Ok(None);
        };
        Wal::extend_to_map_id(layout, file, map_id)?;
        if for_writer {
            Ok(Some(maps.map(file, layout, map_id, wal_mmap_bytes).clone()))
        } else {
            Ok(Some(WalMaps::create_map(
                file,
                layout,
                map_id,
                wal_mmap_bytes,
            )))
        }
    }

    /// Restores the read window over historical fragments before this
    /// iterator is turned into a writer.
    ///
    /// The mapper only maps forward from the writer's fragment and evicts the
    /// lowest `MapId` first, so in steady state the mapped set is the
    /// writer's fragment, [`INITIAL_MAPS_BUFFER`] unwritten lookahead
    /// fragments, and the `max_maps - INITIAL_MAPS_BUFFER - 1` most recently
    /// finalized ones. A writer created on an existing WAL starts with only
    /// its own fragment plus whatever this iterator traversed to reach it:
    /// for the replay WAL that is every fragment replay read, for the index
    /// WAL (`Db::open` goes through `Wal::writer_after_premapped`) nothing,
    /// so without this every index blob written before a restart is served
    /// through the syscall read path until its cell happens to be flushed
    /// again. This maps the most recent fragments below the writer's that
    /// hold at least one position from `live`, as many as fit under
    /// `max_maps` once the writer's fragment, the fragments already
    /// retained, and the lookahead are accounted for.
    ///
    /// The maps are read-only and lazily faulted (see
    /// [`WalMaps::map_readonly`]); partial reads use them, whole-frame copies
    /// go through `pread` instead (see `Wal::frame_source`). Fragments whose
    /// file has been garbage-collected, or which their file no longer fully
    /// covers, are skipped. Eviction stays oldest-first, so these maps age
    /// out as the writer advances; sparse GC also drops them as soon as it
    /// deletes their file (see `WalMapperThread::delete_files`).
    ///
    /// Returns the number of fragments mapped.
    pub fn premap_live_fragments(
        &mut self,
        live: impl IntoIterator<Item = WalPosition>,
    ) -> Result<usize, WalError> {
        assert!(
            self.for_writer,
            "premap_live_fragments called on a scan-mode WalIterator"
        );
        let layout = &self.wal.layout;
        let budget = layout
            .max_maps
            .saturating_sub(INITIAL_MAPS_BUFFER + self.maps.len());
        if budget == 0 {
            return Ok(0);
        }
        let writer_map = self.map.id;
        let candidates: BTreeSet<MapId> = live
            .into_iter()
            .filter(|pos| pos.is_valid())
            .map(|pos| layout.locate(pos.offset()).0)
            .filter(|map_id| *map_id < writer_map && !self.maps.contains(*map_id))
            .collect();
        let files = self.wal.files.load();
        let mut mapped = 0;
        for map_id in candidates.into_iter().rev() {
            if mapped == budget {
                break;
            }
            // Sparse GC may have removed the file; positions in it are
            // unreadable regardless of mapping.
            let Some(file) = files.get_checked(layout.file_for_map(map_id)) else {
                continue;
            };
            // A live position in a fragment the file no longer covers is
            // corruption. Leave it on the syscall path, which fails loudly
            // on read, rather than growing the file with zeros and reporting
            // every key in the fragment as missing.
            if !Wal::covers_map(layout, file, map_id)? {
                continue;
            }
            self.maps.map_readonly(
                file,
                layout,
                map_id,
                self.wal.metrics.wal_mmap_bytes.clone(),
            );
            mapped += 1;
        }
        Ok(mapped)
    }

    pub fn into_writer(self, position_override: Option<u64>) -> WalWriter {
        self.into_writer_with_last_processed(position_override, SharedLastProcessed::default())
    }

    /// Like [`Self::into_writer`], but the tracker publishes its externally
    /// observed `last_processed` into `last_processed`, so the caller can
    /// hand that handle to readers built before the writer exists.
    pub fn into_writer_with_last_processed(
        self,
        position_override: Option<u64>,
        last_processed: SharedLastProcessed,
    ) -> WalWriter {
        assert!(
            self.for_writer,
            "into_writer called on a scan-mode WalIterator — construct via wal_iterator_for_writer"
        );
        let position = position_override.unwrap_or(self.position);
        if let Some(map) = self.maps.get(self.wal.layout.locate(position).0) {
            assert!(
                map.writeable,
                "writer start position {position} falls in a read-only premapped fragment"
            );
        }

        self.clear_fragment_from_position(position);

        let syncer = WalSyncer::start(self.wal.metrics.clone(), self.wal.layout.label.clone());
        self.wal.maps.store(Arc::new(self.maps));
        let mapper = WalMapper::start(
            self.wal.maps.clone(),
            self.wal.files.clone(),
            self.wal.layout.clone(),
            syncer,
            self.wal.metrics.clone(),
        );
        // The writer doesn't use `self.map` directly — it looks up the
        // correct map via `get_writeable_map` against `self.wal.maps`,
        // creating the file/mapping on demand via the mapper thread if
        // missing. So `position` being in a fragment we never loaded
        // (e.g. crash between `write_skip_marker` and `get_writeable_map`,
        // see `clear_fragment_from_position` doc) is fine — the writer
        // will materialise it on first write.
        let wal_tracker = WalTracker::start(
            self.wal.layout.clone(),
            mapper,
            LastProcessed::new(position),
            self.wal.metrics.clone(),
            last_processed,
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
    ///
    /// If `position` falls in a fragment we don't currently have mapped
    /// this is a no-op: typically replay consumed a skip marker that
    /// advanced `self.position` into the next fragment but `make_map`
    /// then returned `EndOfWal` (the file for that fragment doesn't
    /// exist yet). This can happen in production after a crash between
    /// `multi_write`'s `write_skip_marker` and `get_writeable_map`
    /// steps: the skip marker is on disk but the next fragment's file
    /// was never created. There's no data to zero in a fragment that
    /// doesn't exist; the writer that takes over from this iterator
    /// will create the file lazily via the mapper thread when its first
    /// write lands there.
    fn clear_fragment_from_position(&self, position: u64) {
        let (map_id, offset) = self.wal.layout.locate(position);
        if map_id != self.map.id {
            return;
        }

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
    /// How reads through this reader are served. For `CachedFill` the
    /// answer depends on whether this reader led the fill, so call it after
    /// the reads it is meant to label (see `IndexBlobFill::read_type`).
    pub fn read_type(&self) -> ReadType {
        match self {
            WalRandomRead::Mapped(_) => ReadType::Mapped,
            WalRandomRead::File(_) => ReadType::Syscall,
            WalRandomRead::Cached(_) => ReadType::Cached,
            WalRandomRead::CachedFill(fill) => fill.read_type(),
        }
    }
}

impl RandomRead for WalRandomRead {
    fn read(&self, range: Range<usize>) -> Bytes {
        match self {
            WalRandomRead::Mapped(bytes) | WalRandomRead::Cached(bytes) => bytes.slice(range),
            WalRandomRead::File(fr) => fr.read(range),
            WalRandomRead::CachedFill(fill) => fill.read(range),
        }
    }

    fn len(&self) -> usize {
        match self {
            WalRandomRead::Mapped(bytes) | WalRandomRead::Cached(bytes) => bytes.len(),
            WalRandomRead::File(range) => range.len(),
            WalRandomRead::CachedFill(fill) => fill.len(),
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
    use crate::wal::position::WalFileId;
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
            label: WalKind::Replay.name().to_string(),
        };
        // todo - add second test case when there is no space for skip marker after large
        let large = vec![1u8; 1024 - 8 - CrcFrame::CRC_HEADER_LENGTH * 3 - 9];
        {
            let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
            let writer = wal.wal_iterator_for_writer(0).unwrap().into_writer(None);
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
            let mut wal_iterator = wal.wal_iterator_for_writer(0).unwrap();
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
            let mut wal_iterator = wal.wal_iterator_for_scan(0).unwrap();
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
            label: WalKind::Replay.name().to_string(),
        };
        let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
        let wal_writer = wal.wal_iterator_for_writer(0).unwrap().into_writer(None);
        let wal_writer = Arc::new(wal_writer);
        let threads = 8u64;
        let writes_per_thread = 256u64;
        let mut all_writes = HashSet::new();
        let mut jhs = Vec::with_capacity(threads as usize);
        for thread in 0..threads {
            all_writes.extend((0..writes_per_thread).map(|w| (thread << 16) + w));
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
        let mut iterator = wal.wal_iterator_for_scan(0).unwrap();
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
            label: WalKind::Replay.name().to_string(),
        };
        let wal = Wal::open(dir.path(), layout, Metrics::new()).unwrap();
        let wal_iterator = wal.wal_iterator_for_writer(0).unwrap();
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
            label: WalKind::Replay.name().to_string(),
        };

        // Write an entry into the WAL and extract just the position (not the guard)
        let position = {
            let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
            let writer = wal.wal_iterator_for_writer(0).unwrap().into_writer(None);
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
            label: WalKind::Replay.name().to_string(),
        };
        let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
        let writer = wal.wal_iterator_for_writer(0).unwrap().into_writer(None);
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
        let mut wal_iterator = wal.wal_iterator_for_scan(0).unwrap();

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
            label: WalKind::Replay.name().to_string(),
        };

        let wal = Arc::new(Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap());
        let writer = wal.wal_iterator_for_writer(0).unwrap().into_writer(None);

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
            label: WalKind::Replay.name().to_string(),
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

    /// Fills `frags` fragments of a fresh WAL with 4 entries of `payload`
    /// bytes each and returns their positions. With `payload` 200 an entry
    /// occupies 208 bytes and the fourth leaves 192 bytes of slack in a
    /// 1024-byte fragment; with 248 the four entries fill it exactly, so the
    /// next write starts on the fragment boundary.
    fn fill_fragments(
        dir: &Path,
        layout: &WalLayout,
        frags: u64,
        payload: usize,
    ) -> Vec<WalPosition> {
        let wal = Wal::open(dir, layout.clone(), Metrics::new()).unwrap();
        let writer = wal.writer_after(None).unwrap();
        (0..frags * 4)
            .map(|i| {
                writer
                    .write(&PreparedWalWrite::new(&vec![i as u8; payload]))
                    .unwrap()
                    .into_wal_position()
            })
            .collect()
    }

    fn index_layout(max_maps: usize, wal_file_size: u64) -> WalLayout {
        WalLayout {
            frag_size: 1024,
            max_maps,
            direct_io: false,
            wal_file_size,
            kind: WalKind::Index,
            label: WalKind::Index.name().to_string(),
        }
    }

    /// Read type a partial (index lookup style) read of `pos` takes.
    fn lookup_type(wal: &Wal, pos: WalPosition) -> ReadType {
        wal.random_reader_at(pos, 0).unwrap().read_type()
    }

    /// Read type a whole-frame copy of `pos` takes.
    fn load_type(wal: &Wal, pos: WalPosition) -> ReadType {
        wal.read(pos).unwrap().0
    }

    /// Blocks until the mapper thread has processed every message the
    /// writer's tracker has produced so far (fragment finalizations included).
    fn mapper_barrier(writer: &WalWriter) {
        writer.wal_tracker_barrier();
        // `gc(0)` deletes nothing and round-trips through the mapper thread
        // behind everything already queued to it.
        writer.gc(0).unwrap();
    }

    #[test]
    fn test_writer_after_premapped() {
        let dir = tempdir::TempDir::new("test-writer-after-premapped").unwrap();
        // writer's fragment + 2 lookahead + room for 2 historical maps
        let layout = index_layout(5, 10 << 12);
        let positions = fill_fragments(dir.path(), &layout, 8, 200);
        let frag = |pos: &WalPosition| layout.locate(pos.offset()).0.as_u64();
        let in_frag = |f: u64| *positions.iter().find(|p| frag(p) == f).unwrap();
        let last = *positions.last().unwrap();
        assert_eq!(frag(&last), 7);

        let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
        // Live positions in fragments 0, 2, 5 and in the writer's own
        // fragment 7. Budget is 2, so the two most recent historical
        // fragments (5 and 2) get mapped; 0 does not, and 3 has no live data.
        let writer = wal
            .writer_after_premapped(Some(last), [in_frag(0), in_frag(2), in_frag(5), in_frag(7)])
            .unwrap();
        assert_eq!(ReadType::Mapped, lookup_type(&wal, in_frag(7)));
        assert_eq!(ReadType::Mapped, lookup_type(&wal, in_frag(5)));
        assert_eq!(ReadType::Mapped, lookup_type(&wal, in_frag(2)));
        assert_eq!(ReadType::Syscall, lookup_type(&wal, in_frag(0)));
        assert_eq!(ReadType::Syscall, lookup_type(&wal, in_frag(3)));
        // Whole-frame copies bypass the lazily faulted historical maps but
        // still use the writer's pre-faulted one.
        assert_eq!(ReadType::Mapped, load_type(&wal, in_frag(7)));
        assert_eq!(ReadType::Syscall, load_type(&wal, in_frag(5)));
        assert_eq!(ReadType::Syscall, load_type(&wal, in_frag(2)));
        // Every path returns the same bytes.
        for (i, p) in positions.iter().enumerate() {
            assert_eq!(
                &[i as u8; 200][..],
                wal.read(*p).unwrap().1.unwrap().as_ref()
            );
            let reader = wal.random_reader_at(*p, 0).unwrap();
            assert_eq!(&[i as u8; 200][..], reader.read(0..reader.len()).as_ref());
        }

        // The writer still lands in fragment 7 for the first write (there
        // is room for one more entry), so reads of it are mapped.
        let pos = writer
            .write(&PreparedWalWrite::new(&vec![91u8; 100]))
            .unwrap()
            .into_wal_position();
        assert_eq!(frag(&pos), 7);
        assert_eq!(ReadType::Mapped, load_type(&wal, pos));
        assert_eq!(&[91u8; 100][..], wal.read(pos).unwrap().1.unwrap().as_ref());

        // Crossing into fragment 8 finalizes 7; the mapper then maps 10 and,
        // with 6 maps over a budget of 5, evicts the oldest historical map
        // (fragment 2). Fragment 5 stays mapped.
        let pos = writer
            .write(&PreparedWalWrite::new(&vec![92u8; 200]))
            .unwrap()
            .into_wal_position();
        assert_eq!(frag(&pos), 8);
        mapper_barrier(&writer);
        assert_eq!(ReadType::Syscall, lookup_type(&wal, in_frag(2)));
        assert_eq!(ReadType::Mapped, lookup_type(&wal, in_frag(5)));
        assert_eq!(&[92u8; 200][..], wal.read(pos).unwrap().1.unwrap().as_ref());
    }

    #[test]
    fn test_writer_after_premapped_no_budget() {
        let dir = tempdir::TempDir::new("test-writer-after-premapped-nb").unwrap();
        // writer's fragment + 2 lookahead: nothing left for history
        let layout = index_layout(3, 10 << 12);
        let positions = fill_fragments(dir.path(), &layout, 3, 200);
        let last = *positions.last().unwrap();
        let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
        let _writer = wal
            .writer_after_premapped(Some(last), positions.iter().copied())
            .unwrap();
        assert_eq!(ReadType::Syscall, lookup_type(&wal, positions[0]));
        assert_eq!(ReadType::Syscall, lookup_type(&wal, positions[4]));
        assert_eq!(ReadType::Mapped, lookup_type(&wal, last));
    }

    #[test]
    fn test_writer_after_premapped_skips_deleted_files() {
        let dir = tempdir::TempDir::new("test-writer-after-premapped-gc").unwrap();
        // 4 fragments per file
        let layout = index_layout(5, 4096);
        let positions = fill_fragments(dir.path(), &layout, 12, 200);
        let frag = |pos: &WalPosition| layout.locate(pos.offset()).0.as_u64();
        let in_frag = |f: u64| *positions.iter().find(|p| frag(p) == f).unwrap();
        let last = *positions.last().unwrap();
        assert_eq!(frag(&last), 11);
        // Sparse GC removed the middle file (fragments 4..8).
        std::fs::remove_file(layout.wal_file_name(dir.path(), WalFileId(1))).unwrap();

        let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
        // Candidates are visited newest first: 9 is mapped, 6 sits in the
        // deleted file and is skipped without consuming budget, so 1 is
        // mapped as well.
        let writer = wal
            .writer_after_premapped(Some(last), [in_frag(1), in_frag(6), in_frag(9)])
            .unwrap();
        assert_eq!(ReadType::Mapped, lookup_type(&wal, in_frag(9)));
        assert_eq!(ReadType::Mapped, lookup_type(&wal, in_frag(1)));
        assert!(!wal.is_reachable(in_frag(6)));
        assert!(wal.read(in_frag(6)).unwrap().1.is_none());
        let pos = writer
            .write(&PreparedWalWrite::new(&vec![7u8; 100]))
            .unwrap()
            .into_wal_position();
        assert_eq!(&[7u8; 100][..], wal.read(pos).unwrap().1.unwrap().as_ref());
    }

    /// A writer whose restored position is exactly a fragment start
    /// finalizes the fragment below it on its first write. Here that
    /// fragment is a premapped read-only map, which must not be treated as
    /// a finalized writer fragment: no extra lookahead map, no eviction.
    #[test]
    fn test_writer_after_premapped_at_fragment_boundary() {
        let dir = tempdir::TempDir::new("test-writer-after-premapped-boundary").unwrap();
        let layout = index_layout(5, 10 << 12);
        let positions = fill_fragments(dir.path(), &layout, 4, 248);
        let frag = |pos: &WalPosition| layout.locate(pos.offset()).0.as_u64();
        let in_frag = |f: u64| *positions.iter().find(|p| frag(p) == f).unwrap();
        let last = *positions.last().unwrap();
        assert_eq!(frag(&last), 3);
        assert_eq!(layout.next_after_wal_position(last), 4 * 1024);

        let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
        let writer = wal
            .writer_after_premapped(Some(last), [in_frag(1), in_frag(3)])
            .unwrap();
        assert_eq!(ReadType::Mapped, lookup_type(&wal, in_frag(1)));
        assert_eq!(ReadType::Mapped, lookup_type(&wal, in_frag(3)));

        // The first write lands at the start of fragment 4 and finalizes 3.
        let pos = writer
            .write(&PreparedWalWrite::new(&vec![1u8; 200]))
            .unwrap()
            .into_wal_position();
        assert_eq!(pos.offset(), 4 * 1024);
        mapper_barrier(&writer);
        // Lookahead is still fragments 5 and 6, and neither historical map
        // was evicted to make room for a third.
        assert!(wal.get_map(MapId::new(6)).is_some());
        assert!(wal.get_map(MapId::new(7)).is_none());
        assert_eq!(ReadType::Mapped, lookup_type(&wal, in_frag(1)));
        assert_eq!(ReadType::Mapped, lookup_type(&wal, in_frag(3)));
        assert_eq!(&[1u8; 200][..], wal.read(pos).unwrap().1.unwrap().as_ref());
    }

    #[test]
    #[should_panic(expected = "read-only premapped fragment")]
    fn test_into_writer_rejects_start_in_premapped_fragment() {
        let dir = tempdir::TempDir::new("test-into-writer-premapped").unwrap();
        let layout = index_layout(5, 10 << 12);
        let positions = fill_fragments(dir.path(), &layout, 4, 200);
        let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
        let start = layout.next_after_wal_position(*positions.last().unwrap());
        let mut iterator = wal.wal_iterator_for_writer(start).unwrap();
        // positions[4] is in fragment 1.
        assert_eq!(1, iterator.premap_live_fragments([positions[4]]).unwrap());
        let _writer = iterator.into_writer(Some(positions[4].offset()));
    }

    /// A file truncated below a fragment the control region still
    /// references is corruption; premapping must not grow it back with
    /// zeros, which would turn loud read failures into silent misses.
    #[test]
    fn test_writer_after_premapped_skips_short_files() {
        let dir = tempdir::TempDir::new("test-writer-after-premapped-short").unwrap();
        // 4 fragments per file
        let layout = index_layout(5, 4096);
        let positions = fill_fragments(dir.path(), &layout, 12, 200);
        let frag = |pos: &WalPosition| layout.locate(pos.offset()).0.as_u64();
        let in_frag = |f: u64| *positions.iter().find(|p| frag(p) == f).unwrap();
        let last = *positions.last().unwrap();
        // Cut the middle file (fragments 4..8) down to fragments 4 and 5.
        let short_file = layout.wal_file_name(dir.path(), WalFileId(1));
        OpenOptions::new()
            .write(true)
            .open(&short_file)
            .unwrap()
            .set_len(2048)
            .unwrap();

        let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
        // Budget 2, newest first: 9 is mapped, 6 is skipped because its file
        // ends before it, so 5 is mapped as well.
        let _writer = wal
            .writer_after_premapped(Some(last), [in_frag(5), in_frag(6), in_frag(9)])
            .unwrap();
        assert_eq!(ReadType::Mapped, lookup_type(&wal, in_frag(9)));
        assert_eq!(ReadType::Mapped, lookup_type(&wal, in_frag(5)));
        assert_eq!(ReadType::Syscall, lookup_type(&wal, in_frag(6)));
        assert_eq!(2048, std::fs::metadata(&short_file).unwrap().len());
    }

    /// Sparse GC must not be held up by read-only premapped maps: deleting a
    /// file drops its historical maps instead of skipping the file, while a
    /// file with a writeable map is still skipped.
    #[test]
    fn test_delete_files_drops_premapped_maps() {
        let dir = tempdir::TempDir::new("test-delete-files-premapped").unwrap();
        // 4 fragments per file
        let layout = index_layout(5, 4096);
        let positions = fill_fragments(dir.path(), &layout, 12, 200);
        let frag = |pos: &WalPosition| layout.locate(pos.offset()).0.as_u64();
        let in_frag = |f: u64| *positions.iter().find(|p| frag(p) == f).unwrap();
        let last = *positions.last().unwrap();
        assert_eq!(frag(&last), 11);

        let wal = Wal::open(dir.path(), layout.clone(), Metrics::new()).unwrap();
        let writer = wal
            .writer_after_premapped(Some(last), [in_frag(1), in_frag(5)])
            .unwrap();
        assert_eq!(ReadType::Mapped, lookup_type(&wal, in_frag(1)));
        assert_eq!(ReadType::Mapped, lookup_type(&wal, in_frag(5)));

        // File 0 only has a premapped map: it goes, and the map with it.
        writer.delete_files(vec![WalFileId(0)]).unwrap();
        assert!(!wal.file_ids().contains(&WalFileId(0)));
        assert!(!wal.is_reachable(in_frag(1)));
        assert!(wal.read(in_frag(1)).unwrap().1.is_none());
        assert_eq!(ReadType::Mapped, lookup_type(&wal, in_frag(5)));

        // File 2 holds the writer's fragment 11 and stays.
        writer.delete_files(vec![WalFileId(2)]).unwrap();
        assert!(wal.file_ids().contains(&WalFileId(2)));
        assert_eq!(ReadType::Mapped, lookup_type(&wal, last));
        let pos = writer
            .write(&PreparedWalWrite::new(&vec![9u8; 100]))
            .unwrap()
            .into_wal_position();
        assert_eq!(&[9u8; 100][..], wal.read(pos).unwrap().1.unwrap().as_ref());
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
