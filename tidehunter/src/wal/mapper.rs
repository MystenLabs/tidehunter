use super::layout::WalLayout;
use super::position::WalFileId;
use super::{Map, Wal, WalFiles};
use crate::metrics::Metrics;
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use std::sync::{mpsc, Arc};
use std::thread;
use std::thread::JoinHandle;
use std::time::Instant;

pub(crate) struct WalMapper {
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
