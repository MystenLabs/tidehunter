use crate::metrics::Metrics;
use crate::wal::Map;
use memmap2::MmapMut;
use std::sync::{mpsc, Arc};
use std::thread;
use std::thread::JoinHandle;

pub struct WalSyncer {
    sender: Option<mpsc::Sender<SyncRequest>>,
    jh: Option<JoinHandle<()>>,
}

struct WalSyncerThread {
    receiver: mpsc::Receiver<SyncRequest>,
    metrics: Arc<Metrics>,
}

struct SyncRequest {
    map: Map,
    end_position: u64,
}

impl WalSyncer {
    pub fn start(metrics: Arc<Metrics>) -> Self {
        let (sender, receiver) = mpsc::channel();
        let syncer_thread = WalSyncerThread { receiver, metrics };
        let jh = thread::Builder::new()
            .name("wal-syncer".to_string())
            .spawn(move || syncer_thread.run())
            .unwrap();
        let sender = Some(sender);
        let jh = Some(jh);
        WalSyncer { sender, jh }
    }

    pub fn send(&self, map: Map, end_position: u64) {
        let request = SyncRequest { map, end_position };
        self.sender
            .as_ref()
            .unwrap()
            .send(request)
            .expect("Syncer thread stopped unexpectedly");
    }
}

impl WalSyncerThread {
    pub fn run(self) {
        while let Ok(request) = self.receiver.recv() {
            let map = request
                .map
                .data
                .downcast_ref::<MmapMut>()
                .expect("Failed to downcast to MmapMut");
            map.flush().expect("Wal sync failed");
            // todo we can also monitor here number of dangling maps to make sure it does not happen
            self.metrics
                .wal_synced_position
                .set(request.end_position as i64);
        }
    }
}

impl Drop for WalSyncer {
    fn drop(&mut self) {
        self.sender.take();
        self.jh.take().unwrap().join().unwrap();
    }
}
