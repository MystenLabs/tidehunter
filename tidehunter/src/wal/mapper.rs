use super::layout::WalLayout;
use super::position::{MapId, WalFileId};
use super::tracking_mmap::TrackingMMapMut;
use super::{Map, Wal, WalFiles};
use crate::metrics::{MetricIntCounter, MetricIntGauge, Metrics, TimerExt};
use crate::wal::syncer::WalSyncer;
use arc_swap::ArcSwap;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, mpsc};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub(crate) enum WalMapperMessage {
    MapFinalized(MapId),
    MinWalPositionUpdated(u64, mpsc::SyncSender<()>),
    DeleteFiles(Vec<WalFileId>, mpsc::SyncSender<()>),
}

pub(crate) struct WalMapper {
    jh: Option<JoinHandle<()>>,
    sender: Option<mpsc::SyncSender<WalMapperMessage>>,
}

struct WalMapperThread {
    receiver: mpsc::Receiver<WalMapperMessage>,
    maps_arc: Arc<ArcSwap<WalMaps>>,
    maps: WalMaps,
    files: Arc<ArcSwap<WalFiles>>,
    layout: WalLayout,
    syncer: WalSyncer,
    metrics: Arc<Metrics>,
    unlinker: UnlinkWorker,
}
/// Number of fragments past the writer's current one that the mapper keeps
/// mmapped ahead of time so the writer never waits for a map to appear.
pub(super) const INITIAL_MAPS_BUFFER: usize = 2;

/// Background worker that performs `remove_file` calls and closes the
/// underlying `File` off the mapper thread. Sparse GC can produce large
/// delete batches; running the syscalls (or `File::drop`, which closes the
/// fd) inline would block `MapFinalized` processing long enough for the
/// writer to time out in `get_writeable_map`.
struct UnlinkWorker {
    sender: Option<mpsc::Sender<UnlinkMessage>>,
    jh: Option<JoinHandle<()>>,
}

struct UnlinkMessage {
    path: PathBuf,
    file: Arc<File>,
}

impl UnlinkWorker {
    fn start(metrics: Arc<Metrics>, label: String) -> Self {
        let (sender, receiver) = mpsc::channel::<UnlinkMessage>();
        let time_mcs = metrics
            .wal_unlinker_time_mcs
            .with_label_values(&[label.as_str()]);
        let retry_count = metrics
            .wal_unlinker_close_retry_count
            .with_label_values(&[label.as_str()]);
        let jh = thread::Builder::new()
            .name(format!("{label}-unlinker"))
            .spawn(move || {
                for msg in receiver {
                    let _timer = time_mcs.clone().mcs_timer();
                    drop_arc_file(msg.file, &msg.path, &retry_count);
                    if msg.path.exists() {
                        std::fs::remove_file(&msg.path).expect("Failed to remove wal file");
                    }
                }
            })
            .expect("failed to start wal-unlinker thread");
        Self {
            sender: Some(sender),
            jh: Some(jh),
        }
    }

    fn unlink(&self, path: PathBuf, file: Arc<File>) {
        // The receiver is only dropped when the worker thread exits, which
        // only happens after this struct is dropped. `send` cannot fail in
        // normal operation.
        let _: Result<(), mpsc::SendError<UnlinkMessage>> = self
            .sender
            .as_ref()
            .expect("UnlinkWorker dropped")
            .send(UnlinkMessage { path, file });
    }
}

/// Spin on `Arc::try_unwrap` until we are the sole owner, then drop the
/// `File` here so the `close(2)` syscall runs on the unlink worker rather
/// than whatever thread happens to release the last reader-side clone.
fn drop_arc_file(mut file: Arc<File>, path: &std::path::Path, retry_count: &MetricIntCounter) {
    const RETRY_SLEEP: Duration = Duration::from_millis(10);
    const LOG_EVERY: Duration = Duration::from_secs(10);
    let start = Instant::now();
    let mut next_log_at = LOG_EVERY;
    loop {
        match Arc::try_unwrap(file) {
            Ok(f) => {
                drop(f);
                return;
            }
            Err(arc) => {
                retry_count.inc();
                file = arc;
            }
        }
        thread::sleep(RETRY_SLEEP);
        let elapsed = start.elapsed();
        if elapsed >= next_log_at {
            // In debug builds we treat a 10s wait as a bug — a lingering
            // `Arc<File>` clone means something is holding the file beyond
            // its expected lifetime.
            debug_assert!(
                false,
                "wal-unlinker stuck closing {} after {:?}: Arc::try_unwrap still failing",
                path.display(),
                elapsed,
            );
            eprintln!(
                "WARNING: wal-unlinker still waiting to close {} after {:?}",
                path.display(),
                elapsed,
            );
            next_log_at += LOG_EVERY;
        }
    }
}

impl Drop for UnlinkWorker {
    fn drop(&mut self) {
        // Closing the sender lets the worker drain its queue and exit.
        self.sender.take();
        if let Some(jh) = self.jh.take() {
            crate::thread_util::join_thread_with_timeout(jh, "wal-unlinker", 10);
        }
    }
}

#[derive(Clone, Default)]
pub struct WalMaps {
    // todo make it vec + min map id
    maps: BTreeMap<MapId, Map>,
}

impl WalMapper {
    pub fn start(
        maps_arc: Arc<ArcSwap<WalMaps>>,
        files: Arc<ArcSwap<WalFiles>>,
        layout: WalLayout,
        syncer: WalSyncer,
        metrics: Arc<Metrics>,
    ) -> Self {
        let maps = maps_arc.load();
        assert!(
            !maps.maps.is_empty(),
            "Should not pass empty maps to WalMapper::start"
        );
        let min_max_maps = INITIAL_MAPS_BUFFER + 1;
        if layout.max_maps < min_max_maps {
            panic!(
                "Provided layout with max_maps {}, minimum allowed {min_max_maps}",
                layout.max_maps
            );
        }
        let maps = WalMaps::clone(&maps);
        let (sender, receiver) = mpsc::sync_channel(5);
        let unlinker = UnlinkWorker::start(metrics.clone(), layout.label.clone());
        let mapper_thread_name = format!("{}-mapper", layout.label);
        let this = WalMapperThread {
            maps,
            maps_arc,
            files,
            layout,
            receiver,
            metrics,
            syncer,
            unlinker,
        };
        let jh = thread::Builder::new()
            .name(mapper_thread_name)
            .spawn(move || this.run())
            .expect("failed to start wal-mapper thread");
        let sender = Some(sender);
        let jh = Some(jh);
        Self { jh, sender }
    }

    /// Returns `true` while the mapper thread is still running. Flips
    /// to `false` once the thread exits — cleanly via channel close
    /// *or* via panic. The writer's spin loop polls this so a mapper
    /// crash surfaces as a panic on the writer thread instead of an
    /// indefinite wait. The `Drop` impl below `take()`s `jh` to join,
    /// so callers post-drop see `false`.
    pub(crate) fn is_alive(&self) -> bool {
        self.jh.as_ref().is_some_and(|h| !h.is_finished())
    }

    #[cfg(test)]
    pub fn new_unstarted() -> (Self, mpsc::Receiver<WalMapperMessage>) {
        let (sender, receiver) = mpsc::sync_channel(1024);
        (
            Self {
                jh: None,
                sender: Some(sender),
            },
            receiver,
        )
    }

    pub fn map_finalized(&self, map_id: MapId) {
        self.sender
            .as_ref()
            .expect("Wal mapper dropped")
            .send(WalMapperMessage::MapFinalized(map_id))
            .ok();
    }

    pub fn min_wal_position_updated(&self, watermark: u64) {
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender
            .as_ref()
            .expect("Wal mapper dropped")
            .send(WalMapperMessage::MinWalPositionUpdated(watermark, tx))
            .ok();
        // Block until the mapper thread finishes deleting files and updating
        // the file set. This prevents a race where a subsequent operation
        // (e.g. the next relocation run, or another `delete_files` call)
        // iterates or rewrites WAL files that are concurrently being deleted.
        rx.recv().ok();
    }

    /// Delete a specific set of WAL files. Non-writeable maps over those
    /// files are dropped along the way; files that still have a writeable
    /// map (the writer's current file and its lookahead) are skipped by the
    /// mapper thread and will be reconsidered by the next snapshot once
    /// their maps have been evicted. Blocks until the mapper has removed the
    /// deletable entries from the file map; the actual `remove_file`
    /// syscalls run on a background unlink worker, so subsequent
    /// `file_ids()` calls see the deletion immediately even though the
    /// dirents may not be gone yet.
    pub fn delete_files(&self, files: Vec<WalFileId>) {
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender
            .as_ref()
            .expect("Wal mapper dropped")
            .send(WalMapperMessage::DeleteFiles(files, tx))
            .ok();
        rx.recv().ok();
    }
}

impl Drop for WalMapper {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(jh) = self.jh.take() {
            crate::thread_util::join_thread_with_timeout(jh, "wal-mapper", 10);
        }
    }
}

impl WalMapperThread {
    pub fn run(mut self) {
        let mut map_id = *self
            .maps
            .maps
            .last_key_value()
            .expect("Can't start wal mapper with empty maps")
            .0;
        for _ in 0..INITIAL_MAPS_BUFFER {
            map_id = map_id.next_map();
            self.make_map(map_id);
        }
        while let Ok(message) = self.receiver.recv() {
            let _timer = self.metrics.wal_mapper_time_mcs.clone().mcs_timer();
            match message {
                WalMapperMessage::MapFinalized(map_to_sync_id) => {
                    let map_to_sync = self.maps.maps.get_mut(&map_to_sync_id);
                    let Some(map_to_sync) = map_to_sync else {
                        // It is possible (mostly in tests) that map is removed
                        // via min_wal_position_updated before it is finalized.
                        // In this case we simply drop the map since underlining file
                        // is already deleted.
                        continue;
                    };
                    if !map_to_sync.writeable {
                        // A read-only map restored by
                        // `WalIterator::premap_live_fragments` for the
                        // fragment right below a writer that started exactly
                        // on a fragment boundary. Nothing was written through
                        // it, so there is nothing to sync, and it did not
                        // come out of the lookahead budget, so there is no
                        // slot to refill.
                        continue;
                    }
                    map_to_sync.writeable = false;
                    self.syncer.send(
                        map_to_sync.clone(),
                        self.layout.map_range(map_to_sync_id).end,
                    );
                    map_id = map_id.next_map();
                    self.make_map(map_id);
                }
                WalMapperMessage::MinWalPositionUpdated(watermark, _cb) => {
                    self.min_wal_position_updated(watermark);
                    // dropping _cb to release receiver
                }
                WalMapperMessage::DeleteFiles(to_delete, _cb) => {
                    self.delete_files(&to_delete);
                    // dropping _cb to release receiver
                }
            }
        }
    }

    /// Delete files forming a prefix below `watermark`.
    fn min_wal_position_updated(&mut self, watermark: u64) {
        let wal_files = self.files.load();
        let mut to_delete = Vec::new();
        for &file_id in wal_files.files.keys() {
            if self.layout.wal_file_range(file_id).end > watermark {
                break;
            }
            to_delete.push(file_id);
        }
        drop(wal_files);
        if !to_delete.is_empty() {
            self.delete_files(&to_delete);
        }
    }

    /// Delete the specific set of files (gaps allowed).
    ///
    /// Non-writeable maps over those files — read-only maps restored by
    /// `WalIterator::premap_live_fragments` and finalized writer fragments
    /// still in the LRU — are dropped first. Nothing writes through them,
    /// in-flight readers hold their own `Map` clones (the mmap outlives the
    /// unlink), and new readers resolve the position to a missing file, so
    /// keeping a dead file around until such a map ages out of the LRU would
    /// only delay GC. Premapped fragments can sit in old, sparse files that
    /// are exactly the ones relocation empties, so this matters after a
    /// restart.
    ///
    /// Files that still have a writeable map — the writer's current file
    /// and its lookahead — are skipped. That is safe: the snapshot path
    /// recomputes the deletable set each time it runs, so such a file is
    /// picked up by a later snapshot once its maps have been evicted by
    /// `WalMaps::pop_first`.
    fn delete_files(&mut self, to_delete: &[WalFileId]) {
        let wal_files = self.files.load();
        let layout = &self.layout;
        let deleting: HashSet<WalFileId> = to_delete.iter().copied().collect();
        let maps_before = self.maps.maps.len();
        self.maps.maps.retain(|map_id, map| {
            map.writeable || !deleting.contains(&layout.file_for_map(*map_id))
        });
        if self.maps.maps.len() != maps_before {
            self.publish_maps();
        }
        let mapped_files: HashSet<WalFileId> = self
            .maps
            .maps
            .keys()
            .map(|map_id| layout.file_for_map(*map_id))
            .collect();
        let mut actual: Vec<WalFileId> = to_delete
            .iter()
            .copied()
            .filter(|id| !mapped_files.contains(id) && wal_files.files.contains_key(id))
            .collect();
        if actual.is_empty() {
            return;
        }
        actual.sort();
        actual.dedup();
        // Drop the entries from the file map first — subsequent `file_ids()`
        // and `get_checked()` see the deletion immediately. The actual
        // `remove_file` syscalls (and the `close(2)` from dropping `File`)
        // are dispatched to the unlink worker so a large batch doesn't keep
        // this thread out of `MapFinalized` for long. In-flight readers
        // retain their `Arc<File>` clones, and Unix unlink keeps the inode
        // alive until the last reference drops.
        let (new_files, removed_files) = wal_files.without_files(&actual);
        let to_unlink: Vec<(PathBuf, Arc<File>)> = removed_files
            .into_iter()
            .map(|(id, file)| (self.layout.wal_file_name(&wal_files.base_path, id), file))
            .collect();
        self.files.store(Arc::new(new_files));
        drop(wal_files);
        for (path, file) in to_unlink {
            self.unlinker.unlink(path, file);
        }
        // Every remaining map belongs to a file outside `actual`: the
        // non-writeable maps over deleted files were dropped above, and files
        // with writeable maps were filtered out.
    }

    fn make_map(&mut self, map_id: MapId) {
        let file_id = self.layout.file_for_map(map_id);
        let mut files = self.files.load();
        if file_id > files.current_file_id() {
            assert_eq!(file_id, WalFileId(files.current_file_id().0 + 1));
            let new_file_path = self.layout.wal_file_name(&files.base_path, file_id);
            let new_file = Wal::open_file(&new_file_path, &self.layout)
                .expect("Failed to create new wal file");
            let new_wal_files = files.with_file(file_id, Arc::new(new_file));
            self.files.store(Arc::new(new_wal_files));
            files = self.files.load();
        }
        let file = files.get(file_id);
        Wal::extend_to_map_id(&self.layout, file, map_id).expect("Failed to extend wal file");
        self.maps.map(
            file,
            &self.layout,
            map_id,
            self.metrics.wal_mmap_bytes.clone(),
        );

        self.publish_maps();
    }

    fn publish_maps(&self) {
        let new_maps = self.maps.clone();
        self.maps_arc.store(Arc::new(new_maps));
    }
}

impl WalMaps {
    pub fn get(&self, map_id: MapId) -> Option<&Map> {
        self.maps.get(&map_id)
    }

    pub fn len(&self) -> usize {
        self.maps.len()
    }

    pub fn contains(&self, map_id: MapId) -> bool {
        self.maps.contains_key(&map_id)
    }

    pub fn map(
        &mut self,
        file: &File,
        layout: &WalLayout,
        map_id: MapId,
        wal_mmap_bytes: MetricIntGauge,
    ) -> &Map {
        let map = Self::create_map(file, layout, map_id, wal_mmap_bytes);
        assert!(self.maps.insert(map_id, map).is_none());
        if self.maps.len() > layout.max_maps {
            self.maps.pop_first();
        }
        self.maps.get(&map_id).unwrap()
    }

    /// mmap an already-written fragment for reading only and insert it into
    /// the cache. Unlike [`Self::map`] the pages are not pre-faulted: the
    /// caller is restoring a read window over historical data, so paying
    /// for every page up front (possibly from disk) would only delay open,
    /// while partial reads (index lookups) touch a handful of pages per
    /// blob. Whole-frame copies bypass such maps and use `pread` instead,
    /// see `Wal::frame_source`. The map is marked non-writeable; the writer
    /// never allocates below its start position, so nothing can write
    /// through it.
    ///
    /// Never evicts. The caller sizes the set so that the writer's map plus
    /// [`INITIAL_MAPS_BUFFER`] lookahead still fit under `max_maps`.
    pub fn map_readonly(
        &mut self,
        file: &File,
        layout: &WalLayout,
        map_id: MapId,
        wal_mmap_bytes: MetricIntGauge,
    ) {
        assert!(
            self.maps.len() < layout.max_maps,
            "map_readonly would exceed max_maps {}",
            layout.max_maps
        );
        let map = Self::create_map_impl(file, layout, map_id, wal_mmap_bytes, false, false);
        assert!(self.maps.insert(map_id, map).is_none());
    }

    /// mmap a single WAL fragment without inserting it into the cache.
    /// Used by the scan-mode iterator that doesn't need the map handoff
    /// to a writer.
    pub fn create_map(
        file: &File,
        layout: &WalLayout,
        map_id: MapId,
        wal_mmap_bytes: MetricIntGauge,
    ) -> Map {
        Self::create_map_impl(file, layout, map_id, wal_mmap_bytes, true, true)
    }

    fn create_map_impl(
        file: &File,
        layout: &WalLayout,
        map_id: MapId,
        wal_mmap_bytes: MetricIntGauge,
        populate: bool,
        writeable: bool,
    ) -> Map {
        let range = layout.map_range(map_id);
        let data = unsafe {
            let mut options = memmap2::MmapOptions::new();
            options
                .offset(layout.offset_in_wal_file(range.start))
                .len(layout.frag_size as usize);
            if populate {
                options.populate();
            }
            let mmap = options.map_mut(file).expect("Failed to mmap on wal file");
            TrackingMMapMut::new(mmap, wal_mmap_bytes).into()
        };
        Map {
            id: map_id,
            writeable,
            populated: populate,
            data,
        }
    }
}
