use crate::storage::Storage;
use faster_rs::{FasterKv, FasterKvBuilder};
use minibytes::Bytes;
use std::cell::Cell;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

// Thread-local state for FASTER session and operation tracking
// Each thread manages its own session and tracks operations for periodic maintenance
thread_local! {
    static SESSION_STARTED: Cell<bool> = const { Cell::new(false) };
    static OPERATION_COUNT: Cell<u64> = const { Cell::new(0) };
}

// Global monotonic serial number for operation ordering
static SERIAL: AtomicU64 = AtomicU64::new(1);

// Periodic maintenance intervals matching FASTER C++ benchmark patterns
const REFRESH_INTERVAL: u64 = 64; // Advance epoch
const COMPLETE_PENDING_INTERVAL: u64 = 1600; // Flush pending operations

pub struct FasterStorage {
    store: Arc<FasterKv>,
}

impl FasterStorage {
    #[allow(dead_code)]
    pub fn open(path: &Path) -> Arc<Self> {
        std::fs::create_dir_all(path).unwrap();

        let table_size = 1 << 27; // 134M entries 
        let log_size = 137438953472; // 128GB

        let store = FasterKvBuilder::new(table_size, log_size)
            .with_disk(path.to_str().unwrap())
            .build()
            .expect("Failed to create FASTER store");

        Arc::new(Self {
            store: Arc::new(store),
        })
    }

    /// Ensure the current thread has started a FASTER session.
    /// Sessions are per-thread and started lazily on first operation.
    fn ensure_session_started(&self) {
        SESSION_STARTED.with(|started| {
            if !started.get() {
                self.store.start_session();
                started.set(true);
            }
        });
    }

    /// Perform periodic maintenance operations based on operation count.
    /// Matches the pattern from faster-rs benchmarks:
    /// - refresh() every 64 operations to advance epoch
    /// - complete_pending(false) every 1600 operations to flush pending ops
    fn do_periodic_maintenance(&self) {
        OPERATION_COUNT.with(|count| {
            let current = count.get();
            count.set(current + 1);

            if current % REFRESH_INTERVAL == 0 {
                self.store.refresh();

                if current % COMPLETE_PENDING_INTERVAL == 0 {
                    self.store.complete_pending(false);
                }
            }
        });
    }

    /// Trigger a FASTER checkpoint operation.
    /// This creates a consistent snapshot of both the index and hybrid log.
    /// Matches the C++ benchmark pattern of periodic checkpointing.
    pub fn checkpoint(&self) -> Result<(), String> {
        self.store
            .checkpoint()
            .map(|_| ())
            .map_err(|e| format!("Checkpoint failed: {:?}", e))
    }
}

impl Storage for FasterStorage {
    fn insert(&self, k: Bytes, v: Bytes) {
        self.ensure_session_started();

        let key_vec = k.as_ref().to_vec();
        let val_vec = v.as_ref().to_vec();
        let serial = SERIAL.fetch_add(1, Ordering::Relaxed);

        let status = self.store.upsert(&key_vec, &val_vec, serial);

        if status != 0 {
            eprintln!("FASTER upsert failed with status: {}", status);
        }

        self.do_periodic_maintenance();
    }

    fn get(&self, k: &[u8]) -> Option<Bytes> {
        self.ensure_session_started();

        let key_vec = k.to_vec();
        let serial = SERIAL.fetch_add(1, Ordering::Relaxed);

        // Read returns (status, Receiver<V>) for async result delivery
        let (status, receiver) = self.store.read::<Vec<u8>, Vec<u8>>(&key_vec, serial);

        // Force completion of pending reads to get synchronous result
        self.store.complete_pending(true);

        if status == 0 {
            // Try to receive value with timeout
            if let Ok(value) = receiver.recv_timeout(Duration::from_millis(100)) {
                self.do_periodic_maintenance();
                return Some(Bytes::copy_from_slice(&value));
            }
        }

        self.do_periodic_maintenance();
        None
    }

    fn get_lt(&self, _k: &[u8], _iterations: usize) -> Vec<Bytes> {
        // FASTER doesn't support range queries
        // This is a fundamental limitation of the log-structured design
        Vec::new()
    }

    fn exists(&self, k: &[u8]) -> bool {
        self.get(k).is_some()
    }

    fn name(&self) -> &'static str {
        "faster"
    }
}

// Note: We don't implement Drop to call stop_session() because:
// 1. Sessions are per-thread, not per-store instance
// 2. We don't have thread lifecycle hooks in the Storage trait
// 3. FASTER will clean up sessions automatically on process exit
