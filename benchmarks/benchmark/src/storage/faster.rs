use crate::storage::Storage;
use faster_rs::{FasterKv, FasterKvBuilder};
use minibytes::Bytes;
use std::cell::{Cell, RefCell};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Guard that ensures FASTER session is properly stopped when thread exits.
/// This is critical - FASTER requires explicit stop_session() calls before threads terminate,
/// otherwise subsequent threads may fail to start sessions (hitting session limits).
struct SessionGuard {
    store: Arc<FasterKv>,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        // Clean up FASTER session when thread exits
        self.store.stop_session();
    }
}

// Thread-local state for FASTER session and operation tracking
// Each thread manages its own session and tracks operations for periodic maintenance
thread_local! {
    static SESSION_GUARD: RefCell<Option<SessionGuard>> = const { RefCell::new(None) };
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
    /// The SessionGuard ensures stop_session() is called when the thread exits.
    fn ensure_session_started(&self) {
        SESSION_GUARD.with(|guard| {
            if guard.borrow().is_none() {
                self.store.start_session();
                // Store guard to ensure stop_session() is called on thread exit
                *guard.borrow_mut() = Some(SessionGuard {
                    store: self.store.clone(),
                });
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

// Note: Session cleanup is handled via SessionGuard's Drop implementation.
// When a thread exits, the thread_local SESSION_GUARD is dropped, which calls
// stop_session() to properly clean up the FASTER session for that thread.
// This is critical to prevent session limit errors when new threads are spawned.
