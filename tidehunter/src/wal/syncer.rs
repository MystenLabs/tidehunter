use super::Map;
use super::tracking_mmap::TrackingMMapMut;
use crate::metrics::Metrics;
use std::sync::{Arc, mpsc};
use std::thread;
use std::thread::JoinHandle;

pub struct WalSyncer {
    sender: Option<mpsc::Sender<SyncRequest>>,
    jh: Option<JoinHandle<()>>,
}

struct WalSyncerThread {
    receiver: mpsc::Receiver<SyncRequest>,
    metrics: Arc<Metrics>,
    label: String,
}

struct SyncRequest {
    map: Map,
    end_position: u64,
}

impl WalSyncer {
    pub fn start(metrics: Arc<Metrics>, label: String) -> Self {
        let (sender, receiver) = mpsc::channel();
        let thread_name = format!("{label}-syncer");
        let syncer_thread = WalSyncerThread {
            receiver,
            metrics,
            label,
        };
        let jh = thread::Builder::new()
            .name(thread_name)
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
            // Most of the time maps here are received in order of map id and without gaps.
            // However, in some rare cases it is possible to have a 'gap' where some map ids are skipped
            // See WalMapperMessage::MapFinalized handling in wal mapper

            let map = request
                .map
                .data
                .downcast_ref::<TrackingMMapMut>()
                .expect("Failed to downcast to TrackingMMapMut");
            map.flush().expect("Wal sync failed");
            // todo we can also monitor here number of dangling maps to make sure it does not happen
            self.metrics
                .wal_synced_position
                .with_label_values(&[self.label.as_str()])
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
