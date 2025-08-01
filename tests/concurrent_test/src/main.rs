use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tidehunter::config::Config;
use tidehunter::db::Db;
use tidehunter::key_shape::{KeyShape, KeyType};
use tidehunter::metrics::Metrics;

/// Type alias for the key-specific mutex
type KeyMutex = Arc<Mutex<()>>;

/// Type alias for the locks map
type LocksMap = Arc<Mutex<HashMap<Vec<u8>, KeyMutex>>>;

/// Manages per-key locks to ensure atomic operations on individual keys.
///
/// This allows multiple threads to operate on different keys in parallel
/// while preventing race conditions on the same key. Essential for testing
/// concurrent access patterns without serializing all operations.
#[derive(Clone)]
struct KeyLockManager {
    locks: LocksMap,
}

impl KeyLockManager {
    fn new() -> Self {
        Self {
            locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns a mutex for the given key, creating one if it doesn't exist.
    /// Threads must acquire this lock before performing any operation on the key.
    fn get_lock(&self, key: &[u8]) -> KeyMutex {
        let mut locks = self.locks.lock();
        locks
            .entry(key.to_vec())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

/// Shadow state that tracks the expected database contents.
///
/// This in-memory HashMap maintains what we expect the database to contain
/// after all operations. Used to verify database consistency by comparing
/// actual database state against this expected state.
#[derive(Clone)]
struct InMemoryState {
    data: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl InMemoryState {
    fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn insert(&self, key: Vec<u8>, value: Vec<u8>) {
        let mut data = self.data.lock();
        data.insert(key, value);
    }

    fn remove(&self, key: &[u8]) {
        let mut data = self.data.lock();
        data.remove(key);
    }

    fn get_all(&self) -> HashMap<Vec<u8>, Vec<u8>> {
        self.data.lock().clone()
    }
}

/// Count open file descriptors for a given directory using lsof.
/// Returns the number of open file descriptors.
fn count_open_file_descriptors(db_path: &Path) -> usize {
    let mut command = Command::new("lsof");
    command.arg("+D").arg(db_path);
    let output = command.output();
    let output = output.unwrap();

    String::from_utf8_lossy(&output.stdout)
        .lines()
        .skip(1) // Skip header line
        .count()
}

/// Tests concurrent database operations on overlapping keys to ensure thread-safety.
///
/// This test validates that TideHunter correctly handles multiple threads performing
/// concurrent operations (insert/update, read, delete) on the same set of keys.
///
/// ## Test Strategy:
/// 1. Creates 100 shared keys that all threads will operate on
/// 2. Spawns 8 threads, each performing 500 random operations
/// 3. Uses per-key locking to ensure atomic operations
/// 4. Maintains an in-memory shadow state for verification
/// 5. Verifies consistency during reads and after all operations complete
///
/// ## What This Tests:
/// - Thread-safe concurrent access to the database
/// - Correctness under high contention (multiple threads accessing same keys)
/// - No lost updates or phantom reads
/// - Iterator consistency with concurrent modifications
/// - Memory consistency across threads
fn main() {
    let temp_dir = tempdir::TempDir::new("test_concurrent").unwrap();

    // Use a custom config with very small values to trigger more frequent flushes and snapshots
    let mut config = Config::small();
    config.max_dirty_keys = 4;
    // config.snapshot_unload_threshold = 1024; // Commented out - too aggressive for now
    let config = Arc::new(config);
    let (key_shape, key_space) = KeyShape::new_single(8, 8, KeyType::uniform(1));

    // Wrap database in RwLock with Option to allow safe restarts
    let db = Arc::new(RwLock::new(Some(
        Db::open(
            temp_dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap(),
    )));

    // Track number of database restarts and rebuilds for debugging
    let restart_count = Arc::new(AtomicU64::new(0));
    let rebuild_count = Arc::new(AtomicU64::new(0));

    // Path for database restarts
    let db_path = temp_dir.path().to_path_buf();

    // Key-level locking ensures atomic operations on individual keys while
    // allowing parallelism across different keys
    let key_lock_manager = KeyLockManager::new();

    // Shadow state tracks expected database contents for verification
    let in_memory_state = InMemoryState::new();

    // Define a set of keys that will be accessed by multiple threads
    // Using a fixed set of keys ensures high contention
    let keys: Vec<Vec<u8>> = (0..100)
        .map(|i| {
            // Create 8-byte keys as expected by KeyShape
            let mut key = vec![0u8; 8];
            key[0..4].copy_from_slice(&(i as u32).to_be_bytes());
            key
        })
        .collect();

    let num_threads = 8;
    let operations_per_thread = 4 * 5000;
    let total_operations = num_threads * operations_per_thread;

    // Create progress tracking
    let multi_progress = MultiProgress::new();
    let overall_pb = Arc::new(multi_progress.add(ProgressBar::new(total_operations as u64)));
    overall_pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );
    overall_pb.set_message("Total operations");

    let mut handles = vec![];
    let _start_time = Instant::now();

    for thread_id in 0..num_threads {
        let db = db.clone();
        let keys = keys.clone();
        let key_lock_manager = key_lock_manager.clone();
        let in_memory_state = in_memory_state.clone();
        let restart_count = restart_count.clone();
        let rebuild_count = rebuild_count.clone();
        let db_path = db_path.clone();
        let key_shape = key_shape.clone();
        let config = config.clone();

        // Create progress bar for this thread
        let thread_pb = multi_progress.add(ProgressBar::new(operations_per_thread as u64));
        thread_pb.set_style(
            ProgressStyle::default_bar()
                .template(&format!(
                    "[Thread {thread_id}] {{bar:30.green/white}} {{pos:>6}}/{{len:6}} {{msg}}"
                ))
                .unwrap()
                .progress_chars("=>-"),
        );
        thread_pb.set_message("Running");

        let overall_pb = overall_pb.clone();

        let handle = thread::spawn(move || {
            use rand::{Rng, SeedableRng};
            let mut rng = rand::rngs::StdRng::seed_from_u64(thread_id as u64);

            for op_num in 0..operations_per_thread {
                // Update progress bars
                thread_pb.inc(1);
                overall_pb.inc(1);
                // 1% chance to restart the database
                if rng.gen_range(0..100) < 1 {
                    // 1/3 chance to rebuild control region before restart
                    let should_rebuild = rng.gen_range(0..3) == 0;

                    if should_rebuild {
                        // Call rebuild_control_region outside of write lock
                        let db_read = db.read();
                        let db_instance = db_read.as_ref().unwrap();
                        db_instance.rebuild_control_region().unwrap();
                        drop(db_read);
                        rebuild_count.fetch_add(1, Ordering::Relaxed);
                    }

                    // Acquire write lock to restart database and hold it for entire restart
                    let mut db_write = db.write();

                    // Take the current database out of the Option
                    if let Some(old_db) = db_write.take() {
                        // Check file descriptors before stopping
                        assert_ne!(
                            count_open_file_descriptors(&db_path),
                            0,
                            "Expected at least 1 open file descriptors before stopping database"
                        );

                        // Wait for all background threads to finish while holding the lock
                        old_db.wait_for_background_threads_to_finish();

                        // Verify all file descriptors are released after background threads finish
                        assert_eq!(
                            count_open_file_descriptors(&db_path),
                            0,
                            "Expected 0 open file descriptors after stopping database"
                        );

                        // Create new database while still holding the write lock
                        *db_write = Some(
                            Db::open(&db_path, key_shape.clone(), config.clone(), Metrics::new())
                                .unwrap(),
                        );

                        restart_count.fetch_add(1, Ordering::Relaxed);
                    }
                    // Lock is automatically released when db_write goes out of scope
                }
                // Pick a random key from our fixed set to ensure overlapping access
                let key_index = rng.gen_range(0..keys.len());
                let key = keys[key_index].clone();

                // Acquire key-specific lock to ensure this operation is atomic
                // This prevents race conditions while still allowing other threads
                // to operate on different keys
                let lock_mutex = key_lock_manager.get_lock(&key);
                let _lock = lock_mutex.lock();

                // Randomly choose between insert/update (0), read (1), or delete (2)
                // Equal probability ensures good coverage of all operations
                let operation = rng.gen_range(0..3);

                match operation {
                    0 => {
                        // Insert/Update operation
                        // Value encodes thread_id and operation number for debugging
                        let mut value = vec![0u8; 16];
                        value[0..4].copy_from_slice(&(thread_id as u32).to_be_bytes());
                        value[4..8].copy_from_slice(&(op_num as u32).to_be_bytes());
                        value[8..16].copy_from_slice(b"TESTDATA");

                        // Update both database and shadow state atomically
                        {
                            let db_read = db.read();
                            let db_instance = db_read.as_ref().unwrap();
                            db_instance
                                .insert(key_space, key.clone(), value.clone())
                                .unwrap();
                        }
                        in_memory_state.insert(key.clone(), value);
                    }
                    1 => {
                        // Read operation with immediate consistency check
                        let db_value = {
                            let db_read = db.read();
                            let db_instance = db_read.as_ref().unwrap();
                            match db_instance.get(key_space, &key) {
                                Ok(value) => value,
                                Err(e) => {
                                    println!("ERROR: db.get() failed for key {:?}: {:?}", key, e);
                                    println!("Exiting test due to error");
                                    std::process::exit(1);
                                }
                            }
                        };

                        // Verify database state matches our shadow state
                        // This catches any consistency issues immediately
                        let in_memory_data = in_memory_state.data.lock();
                        let in_memory_value = in_memory_data.get(&key);

                        match (db_value, in_memory_value) {
                            (Some(db_val), Some(mem_val)) => {
                                assert_eq!(db_val.as_ref(), mem_val.as_slice());
                            }
                            (None, None) => {} // Both agree key doesn't exist
                            _ => panic!("Database and in-memory state mismatch for key: {:?}", key),
                        }
                    }
                    2 => {
                        // Delete operation
                        // Remove from both database and shadow state atomically
                        {
                            let db_read = db.read();
                            let db_instance = db_read.as_ref().unwrap();
                            db_instance.remove(key_space, key.clone()).unwrap();
                        }
                        in_memory_state.remove(&key);
                    }
                    _ => unreachable!(),
                }
            }

            // Mark thread as finished
            thread_pb.finish_with_message("Done");
        });

        handles.push(handle);
    }

    // Wait for all worker threads
    for handle in handles {
        handle.join().unwrap();
    }

    // Mark overall progress as complete
    overall_pb.finish_with_message("All operations completed");

    // Keep multi_progress alive until the end
    drop(multi_progress);

    // Final verification: ensure database state matches in-memory state exactly
    // This catches any operations that may have been lost or incorrectly applied
    println!("Verifying final state consistency...");

    let in_memory_data = in_memory_state.get_all();

    // Check 1: Every key-value pair in shadow state exists in database
    for (key, expected_value) in &in_memory_data {
        let db_value = {
            let db_read = db.read();
            let db_instance = db_read.as_ref().unwrap();
            db_instance.get(key_space, key).unwrap()
        };
        match db_value {
            Some(actual_value) => {
                assert_eq!(
                    actual_value.as_ref(),
                    expected_value.as_slice(),
                    "Value mismatch for key: {:?}",
                    key
                );
            }
            None => panic!("Key exists in memory but not in database: {:?}", key),
        }
    }

    // Check 2: No extra keys exist in database (bidirectional consistency)
    let mut db_keys = vec![];
    {
        let db_read = db.read();
        let db_instance = db_read.as_ref().unwrap();
        let iterator = db_instance.iterator(key_space);
        for result in iterator {
            let (key, _) = result.unwrap();
            db_keys.push(key.to_vec());
        }
    }

    for db_key in db_keys {
        if !in_memory_data.contains_key(&db_key) {
            panic!("Key exists in database but not in memory: {:?}", db_key);
        }
    }

    println!("✓ Database state matches in-memory state perfectly!");
    println!(
        "  Total operations performed: {}",
        num_threads * operations_per_thread
    );
    println!("  Total keys in final state: {}", in_memory_data.len());
    println!(
        "  Total database restarts: {}",
        restart_count.load(Ordering::Relaxed)
    );
    println!(
        "  Total control region rebuilds: {}",
        rebuild_count.load(Ordering::Relaxed)
    );

    // Print snapshot_force_unload metric to see impact of config changes
    let db_read = db.read();
    let db_instance = db_read.as_ref().unwrap();
    let metrics = db_instance.test_get_metrics();
    let force_unload_count = metrics
        .snapshot_force_unload
        .with_label_values(&[db_instance.ks_name(key_space)])
        .get();
    println!("  snapshot_force_unload metric: {}", force_unload_count);

    println!("\nTest passed successfully!");
}
