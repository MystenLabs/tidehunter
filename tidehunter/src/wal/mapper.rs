use super::layout::WalLayout;
use super::position::{MapId, WalFileId};
use super::{Map, Wal, WalFiles};
use crate::metrics::Metrics;
use crate::wal::syncer::WalSyncer;
use arc_swap::ArcSwap;
use std::collections::BTreeMap;
use std::fs::File;
use std::sync::{mpsc, Arc};
use std::thread;
use std::thread::JoinHandle;
use std::time::Instant;

pub(crate) enum WalMapperMessage {
    MapFinalized(MapId),
    MinWalPositionUpdated(u64),
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
}
const INITIAL_MAPS_BUFFER: usize = 2;

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
        let this = WalMapperThread {
            maps,
            maps_arc,
            files,
            layout,
            receiver,
            metrics,
            syncer,
        };
        let jh = thread::Builder::new()
            .name("wal-mapper".to_string())
            .spawn(move || this.run())
            .expect("failed to start wal-mapper thread");
        let sender = Some(sender);
        let jh = Some(jh);
        Self { jh, sender }
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
        self.sender
            .as_ref()
            .expect("Wal mapper dropped")
            .send(WalMapperMessage::MinWalPositionUpdated(watermark))
            .ok();
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
            let timer = Instant::now();
            match message {
                WalMapperMessage::MapFinalized(map_to_sync_id) => {
                    let map_to_sync = self
                        .maps
                        .maps
                        .get_mut(&map_to_sync_id)
                        .expect("Map to finalize not found");
                    map_to_sync.writeable = false;
                    self.syncer.send(
                        map_to_sync.clone(),
                        self.layout.map_range(map_to_sync_id).end,
                    );
                    map_id = map_id.next_map();
                    self.make_map(map_id);
                }
                WalMapperMessage::MinWalPositionUpdated(watermark) => {
                    // Delete files up to the watermark
                    let wal_files = self.files.load();
                    let mut num_files_deleted = 0;
                    for idx in 0..wal_files.files.len() {
                        let file_id = WalFileId(wal_files.min_file_id.0 + idx as u64);
                        if self.layout.wal_file_range(file_id).end >= watermark {
                            break;
                        }
                        let path = self.layout.wal_file_name(&wal_files.base_path, file_id);
                        if path.exists() {
                            std::fs::remove_file(path).expect("Failed to remove wal file");
                        }
                        num_files_deleted += 1;
                    }
                    // Update the WalFiles structure and maps by removing deleted files
                    if num_files_deleted > 0 {
                        let new_files = wal_files.skip_first_n_files(num_files_deleted);
                        let new_min_file_id = new_files.min_file_id;
                        self.files.store(Arc::new(new_files));

                        // Remove all maps that belonged to deleted files
                        self.maps.maps.retain(|&map_id, _| {
                            self.layout.file_for_map(map_id) >= new_min_file_id
                        });

                        self.publish_maps();
                    }
                }
            }
            self.metrics
                .wal_mapper_time_mcs
                .inc_by(timer.elapsed().as_micros() as u64);
        }
    }

    fn make_map(&mut self, map_id: MapId) {
        let file_id = self.layout.file_for_map(map_id);
        let mut files = self.files.load();
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
        Wal::extend_to_map_id(&self.layout, &files, map_id).expect("Failed to extend wal file");
        self.maps.map(files.get(file_id), &self.layout, map_id);

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

    pub fn map(&mut self, file: &File, layout: &WalLayout, map_id: MapId) -> &Map {
        let range = layout.map_range(map_id);
        let data = unsafe {
            let mut options = memmap2::MmapOptions::new();
            options
                .offset(layout.offset_in_wal_file(range.start))
                .len(layout.frag_size as usize);
            options
                .populate()
                .map_mut(file)
                .expect("Failed to mmap on wal file")
                .into()
        };
        let map = Map {
            id: map_id,
            writeable: true,
            data,
        };
        assert!(self.maps.insert(map_id, map).is_none());
        if self.maps.len() > layout.max_maps {
            self.maps.pop_first();
        }
        self.maps.get(&map_id).unwrap()
    }
}
