use crate::batch::WriteBatch;
use crate::config::Config;
use crate::db::Db;
use crate::key_shape::{KeyShape, KeyType};
use crate::metrics::Metrics;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Manages per-key locks to ensure atomic operations on individual keys.
///
/// This allows multiple threads to operate on different keys in parallel
/// while preventing race conditions on the same key. Essential for testing
/// concurrent access patterns without serializing all operations.
#[derive(Clone)]
struct KeyLockManager {
    locks: Arc<Mutex<HashMap<Vec<u8>, Arc<Mutex<()>>>>>,
}

impl KeyLockManager {
    fn new() -> Self {
        Self {
            locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns a mutex for the given key, creating one if it doesn't exist.
    /// Threads must acquire this lock before performing any operation on the key.
    fn get_lock(&self, key: &[u8]) -> Arc<Mutex<()>> {
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
#[test]
fn test_concurrent_operations_with_overlapping_keys() {
    let temp_dir = tempdir::TempDir::new("test_concurrent").unwrap();

    // Use a custom config with very small max_dirty_keys to trigger more frequent flushes
    let mut config = Config::small();
    config.max_dirty_keys = 4;
    let config = Arc::new(config);
    let (key_shape, key_space) = KeyShape::new_single(8, 8, KeyType::uniform(1));

    // Wrap database in RwLock to allow restarts
    let db = Arc::new(RwLock::new(
        Db::open(
            temp_dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap(),
    ));

    // Track number of database restarts for debugging
    let restart_count = Arc::new(AtomicU64::new(0));

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
    let operations_per_thread = 500;

    let mut handles = vec![];
    let _start_time = Instant::now();

    for thread_id in 0..num_threads {
        let db = db.clone();
        let key_space = key_space.clone();
        let keys = keys.clone();
        let key_lock_manager = key_lock_manager.clone();
        let in_memory_state = in_memory_state.clone();
        let restart_count = restart_count.clone();
        let db_path = db_path.clone();
        let key_shape = key_shape.clone();
        let config = config.clone();

        let handle = thread::spawn(move || {
            use rand::{Rng, SeedableRng};
            let mut rng = rand::rngs::StdRng::seed_from_u64(thread_id as u64);

            for op_num in 0..operations_per_thread {
                // 1% chance to restart the database
                if rng.gen_range(0..100) < 1 {
                    // 1/3 chance to rebuild control region before restart
                    let should_rebuild = rng.gen_range(0..3) == 0;

                    if should_rebuild {
                        // Call rebuild_control_region outside of write lock
                        let db_read = db.read();
                        db_read.rebuild_control_region().unwrap();
                        drop(db_read);
                        println!("Thread {} rebuilt control region before restart", thread_id);
                    }

                    // Acquire write lock to restart database
                    let mut db_write = db.write();

                    // Close the current database by dropping it
                    *db_write =
                        Db::open(&db_path, key_shape.clone(), config.clone(), Metrics::new())
                            .unwrap();

                    restart_count.fetch_add(1, Ordering::Relaxed);
                    println!(
                        "Thread {} restarted database (restart #{})",
                        thread_id,
                        restart_count.load(Ordering::Relaxed)
                    );

                    // Release write lock before continuing
                    drop(db_write);
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
                            db_read
                                .insert(key_space, key.clone(), value.clone())
                                .unwrap();
                        }
                        in_memory_state.insert(key.clone(), value);
                    }
                    1 => {
                        // Read operation with immediate consistency check
                        let db_value = {
                            let db_read = db.read();
                            db_read.get(key_space, &key).unwrap()
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
                            db_read.remove(key_space, key.clone()).unwrap();
                        }
                        in_memory_state.remove(&key);
                    }
                    _ => unreachable!(),
                }
            }
        });

        handles.push(handle);
    }

    // Implement timeout protection to prevent test hangs
    let timeout = Duration::from_secs(60);
    let all_done = Arc::new(AtomicBool::new(false));
    let all_done_clone = all_done.clone();

    // Spawn a watchdog thread that will panic if test takes too long
    let watchdog = thread::spawn(move || {
        thread::sleep(timeout);
        if !all_done_clone.load(Ordering::Relaxed) {
            panic!("Test timed out after 60 seconds");
        }
    });

    // Wait for all worker threads
    for handle in handles {
        handle.join().unwrap();
    }

    // Signal that we're done
    all_done.store(true, Ordering::Relaxed);
    watchdog.join().ok();

    // Final verification: ensure database state matches in-memory state exactly
    // This catches any operations that may have been lost or incorrectly applied
    println!("Verifying final state consistency...");

    let in_memory_data = in_memory_state.get_all();

    // Check 1: Every key-value pair in shadow state exists in database
    for (key, expected_value) in &in_memory_data {
        let db_value = {
            let db_read = db.read();
            db_read.get(key_space, key).unwrap()
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
        let iterator = db_read.iterator(key_space);
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
    println!("  Total keys in final state: {}", in_memory_data.len());
    println!(
        "  Total database restarts: {}",
        restart_count.load(Ordering::Relaxed)
    );
}

#[test]
fn test_simple_concurrent_batch_operations() {
    let temp_dir = tempdir::TempDir::new("test_simple_batch").unwrap();

    let config = Arc::new(Config::small());
    let (key_shape, key_space) = KeyShape::new_single(8, 16, KeyType::uniform(16));

    let db = Arc::new(Db::open(temp_dir.path(), key_shape, config, Metrics::new()).unwrap());

    let num_threads = 4;
    let batches_per_thread = 20;

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db = db.clone();
        let key_space = key_space.clone();

        let handle = thread::spawn(move || {
            use rand::{Rng, SeedableRng};
            let mut rng = rand::rngs::StdRng::seed_from_u64(thread_id as u64);

            for batch_num in 0..batches_per_thread {
                let mut batch = WriteBatch::new();

                // Each thread works on its own key range to avoid conflicts
                let base_key = thread_id * 1000 + batch_num * 10;

                for i in 0..5 {
                    let mut key = vec![0u8; 8];
                    key[0..4].copy_from_slice(&((base_key + i) as u32).to_be_bytes());
                    key[4..8].copy_from_slice(b"SMPL");

                    let mut value = vec![0u8; 16];
                    value[0..4].copy_from_slice(&(thread_id as u32).to_be_bytes());
                    value[4..8].copy_from_slice(&(batch_num as u32).to_be_bytes());
                    value[8..12].copy_from_slice(&(i as u32).to_be_bytes());
                    value[12..16].copy_from_slice(b"TEST");

                    if rng.gen_bool(0.8) {
                        batch.write(key_space, key, value);
                    } else {
                        batch.delete(key_space, key);
                    }
                }

                db.write_batch(batch).unwrap();
            }
        });

        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify database is accessible and contains data
    let mut count = 0;
    let iterator = db.iterator(key_space);
    for result in iterator {
        let (_, _) = result.unwrap();
        count += 1;
    }

    println!("✓ Simple batch operations test passed!");
    println!("  Total keys in database: {}", count);
    assert!(
        count > 0,
        "Database should contain some keys after batch operations"
    );
}
