use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use parking_lot::{Mutex, RwLock};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Instant;
use tidehunter::config::Config;
use tidehunter::db::Db;
use tidehunter::key_shape::{KeyShape, KeyShapeBuilder, KeyType};
use tidehunter::metrics::Metrics;
use tidehunter::minibytes::Bytes;

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

/// Opens a database with the given configuration and starts periodic snapshots.
fn open_db_with_snapshots(
    db_path: &Path,
    key_shape: KeyShape,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
) -> Arc<Db> {
    let db = Db::open(db_path, key_shape, config, metrics).unwrap();
    db.start_periodic_snapshot();
    db
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
    config.l0_max_entries = Some(6);
    config.snapshot_unload_threshold = 1024;
    config.snapshot_written_bytes = 4 * 1024 * 1024; // 4 MB — trigger snapshots frequently
    let config = Arc::new(config);

    let mut key_shape_builder = KeyShapeBuilder::new();
    key_shape_builder.add_key_space("main", 1, 8, KeyType::uniform(1));
    key_shape_builder.add_key_space("secondary", 4, 8, KeyType::uniform(1));
    let key_shape = key_shape_builder.build();

    // Shared metrics across restarts so relocation counters accumulate
    let shared_metrics = Metrics::new();

    // Open once to obtain the canonical key space handles (stable across the
    // same-shape reopens below), then wrap the db in RwLock<Option<_>> to
    // allow safe restarts.
    let initial_db = open_db_with_snapshots(
        temp_dir.path(),
        key_shape.clone(),
        config.clone(),
        shared_metrics.clone(),
    );
    let key_space = initial_db.ks("main");
    let key_space2 = initial_db.ks("secondary");
    let db = Arc::new(RwLock::new(Some(initial_db)));

    // Track number of database restarts and rebuilds for debugging
    let restart_count = Arc::new(AtomicU64::new(0));
    let rebuild_count = Arc::new(AtomicU64::new(0));

    // Secondary key space state - tracks expected contents for correctness checking
    let secondary_state: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>> =
        Arc::new(Mutex::new(BTreeMap::new()));
    let next_secondary_key = Arc::new(AtomicU64::new(0));

    // Path for database restarts
    let db_path = temp_dir.path().to_path_buf();

    // Key-level locking ensures atomic operations on individual keys while
    // allowing parallelism across different keys
    let key_lock_manager = KeyLockManager::new();

    // Shadow state tracks expected database contents for verification
    let in_memory_state = InMemoryState::new();

    // Define a set of keys that will be accessed by multiple threads
    // Using a fixed set of keys ensures high contention
    let keys: Vec<Vec<u8>> = (0u8..25).map(|i| vec![i + b'a']).collect();

    let num_threads = 8;
    let operations_per_thread = 64 * 5000;
    let total_operations = num_threads * operations_per_thread;

    // Check if progress bars should be disabled
    let no_progress = std::env::var("NO_PROGRESS").is_ok();

    // Create progress tracking (completely hidden if NO_PROGRESS is set)
    let multi_progress = if no_progress {
        MultiProgress::with_draw_target(ProgressDrawTarget::hidden())
    } else {
        MultiProgress::new()
    };

    let overall_pb = Arc::new(multi_progress.add(ProgressBar::new(total_operations as u64)));
    if !no_progress {
        overall_pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
                .unwrap()
                .progress_chars("##-"),
        );
        overall_pb.set_message("Total operations");
    }

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
        let shared_metrics = shared_metrics.clone();
        let secondary_state = secondary_state.clone();
        let next_secondary_key = next_secondary_key.clone();

        // Create progress bar for this thread
        let thread_pb = multi_progress.add(ProgressBar::new(operations_per_thread as u64));
        if !no_progress {
            thread_pb.set_style(
                ProgressStyle::default_bar()
                    .template(&format!(
                        "[Thread {thread_id}] {{bar:30.green/white}} {{pos:>6}}/{{len:6}} {{msg}}"
                    ))
                    .unwrap()
                    .progress_chars("=>-"),
            );
            thread_pb.set_message("Running");
        }

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
                        // Only check file descriptors 0.2% of the time to reduce overhead
                        let should_check_fds = rng.gen_range(0..500) < 1;

                        if should_check_fds {
                            // Check file descriptors before stopping
                            let fd_count = count_open_file_descriptors(&db_path);
                            if fd_count == 0 {
                                eprintln!(
                                    "ERROR: Expected at least 1 open file descriptors before stopping database, but got 0"
                                );
                                std::process::exit(1);
                            }
                        }

                        // Wait for all background threads to finish while holding the lock
                        old_db.wait_for_background_threads_to_finish();

                        if should_check_fds {
                            // Verify all file descriptors are released after background threads finish
                            let fd_count = count_open_file_descriptors(&db_path);
                            if fd_count != 0 {
                                eprintln!(
                                    "ERROR: Expected 0 open file descriptors after stopping database, but got {}",
                                    fd_count
                                );
                                std::process::exit(1);
                            }
                        }

                        // Create new database while still holding the write lock.
                        // The key space handles are stable across same-shape reopens.
                        *db_write = Some(open_db_with_snapshots(
                            &db_path,
                            key_shape.clone(),
                            config.clone(),
                            shared_metrics.clone(),
                        ));

                        restart_count.fetch_add(1, Ordering::Relaxed);
                    }
                    // Lock is automatically released when db_write goes out of scope
                }
                // 0.1% chance to trigger explicit WAL-based relocation
                if rng.gen_range(0..1000u32) < 1 {
                    let db_read = db.read();
                    let db_instance = db_read.as_ref().unwrap();
                    db_instance.start_relocation().unwrap();
                }

                // 0.1% chance to force the flat-promotion pass with the threshold
                // bypassed. Normally promote_to_flat runs on a 10-second timer and
                // only fires when a cell's write buffer has more than 128 entries;
                // with only 25 test keys spread across cells neither condition is
                // met, so the insert → promote → remove → FlushLoaded window
                // essentially never opens. Forcing it here interleaves
                // promote_to_flat with writes from other threads and exposes the
                // `clean_self` stale-record bug where a Removed tombstone in
                // `data` shadows a Modified entry in `flat`.
                if rng.gen_range(0..100u32) < 1 {
                    let db_read = db.read();
                    let db_instance = db_read.as_ref().unwrap();
                    db_instance.test_promote_flat_force();
                }

                // 1% chance to operate on secondary key space
                if rng.gen_range(0..100) < 1 {
                    let mut state = secondary_state.lock();
                    let roll = rng.gen_range(0..100u32);
                    let db_read = db.read();
                    let db_instance = db_read.as_ref().unwrap();

                    if roll < 10 && state.len() > 2 {
                        // Overwrite existing value
                        let idx = rng.gen_range(state.len() / 2..state.len());
                        let key = state.keys().nth(idx).unwrap().clone();
                        let mut value = vec![0u8; 16];
                        value[0..4].copy_from_slice(&(thread_id as u32).to_be_bytes());
                        value[4..8].copy_from_slice(&(op_num as u32).to_be_bytes());
                        value[8..16].copy_from_slice(b"KS2OVRWR");
                        db_instance
                            .insert(key_space2, key.clone(), value.clone())
                            .unwrap();
                        state.insert(key, value);
                    } else if roll < 20 && state.len() > 2 {
                        // Remove existing value
                        let idx = rng.gen_range(state.len() / 2..state.len());
                        let key = state.keys().nth(idx).unwrap().clone();
                        db_instance.remove(key_space2, key.clone()).unwrap();
                        state.remove(&key);
                    } else {
                        // Insert new value
                        let next = next_secondary_key.fetch_add(1, Ordering::Relaxed);
                        let key = (next as u32).to_be_bytes().to_vec();
                        let mut value = vec![0u8; 16];
                        value[0..4].copy_from_slice(&(thread_id as u32).to_be_bytes());
                        value[4..8].copy_from_slice(&(op_num as u32).to_be_bytes());
                        value[8..16].copy_from_slice(b"KS2NEWKV");
                        db_instance
                            .insert(key_space2, key.clone(), value.clone())
                            .unwrap();
                        state.insert(key, value);
                    }
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
                            if rng.r#gen() {
                                db_instance
                                    .insert(key_space, key.clone(), value.clone())
                                    .unwrap();
                            } else {
                                // Some of the writes are done via batch
                                let mut batch = db_instance.write_batch();
                                batch.write(key_space, key.clone(), value.clone());
                                batch.commit().unwrap();
                            }
                        }
                        in_memory_state.insert(key.clone(), value);
                    }
                    1 => {
                        // Read operation with immediate consistency check
                        let db_read = db.read();
                        let db_instance = db_read.as_ref().unwrap();
                        let db_value = {
                            match db_instance.get(key_space, &key) {
                                Ok(value) => value,
                                Err(e) => {
                                    println!("ERROR: db.get() failed for key {key:?}: {e:?}");
                                    println!("Exiting test due to error");
                                    std::process::exit(1);
                                }
                            }
                        };

                        // Verify database state matches our shadow state
                        // This catches any consistency issues immediately
                        let in_memory_data = in_memory_state.data.lock();
                        let in_memory_value = in_memory_data.get(&key);
                        let key = Bytes::from(key);
                        match (db_value, in_memory_value) {
                            (Some(db_val), Some(mem_val)) => {
                                if db_val.as_ref() != mem_val.as_slice() {
                                    eprintln!(
                                        "ERROR: Value mismatch for key {:?}: database has {:?}, in-memory has {:?}",
                                        key,
                                        db_val.as_ref(),
                                        mem_val.as_slice()
                                    );
                                    std::process::exit(1);
                                }
                            }
                            (None, None) => {} // Both agree key doesn't exist
                            (Some(db_val), None) => {
                                eprintln!(
                                    "ERROR: Key {:?} exists in database with value {:?}, but not in in-memory state",
                                    key,
                                    db_val.as_ref()
                                );
                                std::process::exit(1);
                            }
                            (None, Some(mem_val)) => {
                                eprintln!(
                                    "ERROR: Key {:?} exists in in-memory state with value {:?}, but not in database",
                                    key,
                                    mem_val.as_slice()
                                );
                                std::process::exit(1);
                            }
                        }
                    }
                    2 => {
                        // Delete operation
                        // Remove from both database and shadow state atomically
                        {
                            let db_read = db.read();
                            let db_instance = db_read.as_ref().unwrap();
                            if rng.r#gen() {
                                db_instance.remove(key_space, key.clone()).unwrap();
                            } else {
                                // Some of the deletes are done via batch
                                let mut batch = db_instance.write_batch();
                                batch.delete(key_space, key.clone());
                                batch.commit().unwrap();
                            }
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
                if actual_value.as_ref() != expected_value.as_slice() {
                    eprintln!(
                        "ERROR: Final verification: Value mismatch for key {:?}: database has {:?}, in-memory has {:?}",
                        key,
                        actual_value.as_ref(),
                        expected_value.as_slice()
                    );
                    std::process::exit(1);
                }
            }
            None => {
                eprintln!(
                    "ERROR: Key {key:?} exists in in-memory state with value {expected_value:?}, but not in database"
                );
                std::process::exit(1);
            }
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

    for db_key in &db_keys {
        if !in_memory_data.contains_key(db_key) {
            let db_read = db.read();
            let db_instance = db_read.as_ref().unwrap();
            let db_value = db_instance.get(key_space, db_key).unwrap();
            eprintln!(
                "ERROR: Key {:?} exists in database with value {:?}, but not in in-memory state",
                db_key,
                db_value.map(|v| v.as_ref().to_vec())
            );
            std::process::exit(1);
        }
    }

    println!("✓ Database state matches in-memory state perfectly!");

    // Verify secondary key space
    println!("Verifying secondary key space consistency...");
    let secondary_data = secondary_state.lock().clone();
    for (key, expected_value) in &secondary_data {
        let db_value = {
            let db_read = db.read();
            let db_instance = db_read.as_ref().unwrap();
            db_instance.get(key_space2, key).unwrap()
        };
        match db_value {
            Some(actual_value) => {
                if actual_value.as_ref() != expected_value.as_slice() {
                    eprintln!(
                        "ERROR: Secondary KS: Value mismatch for key {:?}: database has {:?}, expected {:?}",
                        key,
                        actual_value.as_ref(),
                        expected_value.as_slice()
                    );
                    std::process::exit(1);
                }
            }
            None => {
                eprintln!(
                    "ERROR: Secondary KS: Key {key:?} exists in expected state but not in database"
                );
                std::process::exit(1);
            }
        }
    }
    // Check no extra keys in secondary ks
    let mut db_ks2_keys = vec![];
    {
        let db_read = db.read();
        let db_instance = db_read.as_ref().unwrap();
        let iterator = db_instance.iterator(key_space2);
        for result in iterator {
            let (key, _) = result.unwrap();
            db_ks2_keys.push(key.to_vec());
        }
    }
    for db_key in &db_ks2_keys {
        if !secondary_data.contains_key(db_key) {
            eprintln!(
                "ERROR: Secondary KS: Key {:?} exists in database but not in expected state",
                db_key
            );
            std::process::exit(1);
        }
    }
    println!("✓ Secondary key space state matches perfectly!");
    println!("  Keys in secondary key space: {}", secondary_data.len());

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

    // Print metrics
    let db_read = db.read();
    let db_instance = db_read.as_ref().unwrap();
    println!(
        "  Wal size: {}",
        human_readable_bytes(shared_metrics.wal_written_bytes.get() as u64)
    );
    let replay_from = db_instance.test_get_replay_from();
    println!(
        "  Replay from: {}({})",
        replay_from,
        human_readable_bytes(replay_from)
    );
    let force_unload = shared_metrics
        .snapshot_force_unload
        .with_label_values(&["main"])
        .get();
    println!("  snapshot_force_unload: {force_unload}");

    let forced_relocation = shared_metrics
        .snapshot_forced_relocation
        .with_label_values(&["main"])
        .get();
    println!("  snapshot_forced_relocation: {forced_relocation}");

    let relocation_cells = shared_metrics
        .relocation_cells_processed
        .with_label_values(&["main"])
        .get();
    println!("  relocation_cells_processed: {relocation_cells}");

    let relocation_kept = shared_metrics
        .relocation_kept
        .with_label_values(&["secondary"])
        .get();
    println!("  relocation_kept: {relocation_kept}");
    let relocation_removed = shared_metrics
        .relocation_removed
        .with_label_values(&["secondary"])
        .get();
    println!("  relocation_removed: {relocation_removed}");
    let wal_gc_position = shared_metrics.gc_position.with_label_values(&["wal"]).get();
    println!("  wal_gc_position: {wal_gc_position}");
    let index_gc_position = shared_metrics
        .gc_position
        .with_label_values(&["index"])
        .get();
    println!("  index_gc_position: {index_gc_position}");

    // Two-level LSM visibility: did the run actually exercise L1?
    // `index_0` / `index_1` break down the on-disk read path; a non-zero
    // `index_1` (for either Found or NotFound) means lookups fell through L0.
    for ks in ["main", "secondary"] {
        let l0_bytes = shared_metrics
            .l0_bytes_written
            .with_label_values(&[ks])
            .get();
        let l1_bytes = shared_metrics
            .l1_bytes_written
            .with_label_values(&[ks])
            .get();
        let promotes = shared_metrics.promote_total.with_label_values(&[ks]).get();
        let mut read_index_0 = 0u64;
        let mut read_index_1 = 0u64;
        for result in ["found", "not_found"] {
            read_index_0 += shared_metrics
                .lookup_result
                .with_label_values(&[ks, result, "index_0"])
                .get();
            read_index_1 += shared_metrics
                .lookup_result
                .with_label_values(&[ks, result, "index_1"])
                .get();
        }
        println!(
            "  lsm[{ks}]: l0_bytes={l0_bytes} l1_bytes={l1_bytes} promotes={promotes} \
             reads_index_0={read_index_0} reads_index_1={read_index_1}"
        );
    }
    drop(db_read);

    println!("\nTest passed successfully!");
}

fn human_readable_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}
