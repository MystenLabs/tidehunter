use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use tidehunter::metrics::Metrics;
use tidehunter::{PreparedWalWrite, Wal, WalKind, WalLayout, WalWriter};

/// Multi-threaded WAL benchmark for TideHunter
#[derive(Parser, Debug)]
#[command(name = "wal_benchmark", about = "Multi-threaded WAL write benchmark")]
struct Args {
    /// Number of threads (default: number of CPUs)
    #[arg(short = 't', long, default_value_t = num_cpus::get())]
    threads: usize,

    /// Size of each write in bytes
    #[arg(short = 's', long, default_value_t = 512)]
    write_size: usize,

    /// Duration to run the benchmark in seconds
    #[arg(short = 'd', long, default_value_t = 300)]
    duration_secs: u64,

    /// Database path (uses temporary directory if not specified)
    #[arg(short = 'p', long)]
    path: Option<String>,

    /// Number of WAL instances (each thread assigned to instance via round-robin)
    #[arg(short = 'w', long, default_value_t = 1)]
    wal_instances: usize,

    /// Enable direct I/O (bypasses OS page cache)
    #[arg(long, default_value_t = false)]
    direct_io: bool,
}

struct BenchmarkResult {
    writes_completed: usize,
    bytes_written: u64,
}

fn run_benchmark_thread(
    thread_id: usize,
    wal_writer: Arc<WalWriter>,
    write_size: usize,
    duration: Duration,
    total_writes: Arc<AtomicU64>,
    total_bytes: Arc<AtomicU64>,
) -> BenchmarkResult {
    let mut rng = rand::rngs::StdRng::from_entropy();
    let mut writes_completed = 0;
    let mut bytes_written = 0u64;

    let start = Instant::now();

    loop {
        // Check termination condition
        if start.elapsed() >= duration {
            break;
        }

        // Generate random data
        let mut data = vec![0u8; write_size];
        rng.fill(&mut data[..]);

        // Write to WAL
        let prepared_write = PreparedWalWrite::new(&data);
        match wal_writer.write(&prepared_write) {
            Ok(_guard) => {
                writes_completed += 1;
                bytes_written += write_size as u64;

                // Update global counters for progress bar
                total_writes.fetch_add(1, Ordering::Relaxed);
                total_bytes.fetch_add(write_size as u64, Ordering::Relaxed);
            }
            Err(e) => {
                eprintln!("Thread {thread_id}: Write failed: {e:?}");
                break;
            }
        }
    }

    BenchmarkResult {
        writes_completed,
        bytes_written,
    }
}

fn format_throughput(bytes: u64, duration: Duration) -> String {
    let seconds = duration.as_secs_f64();
    let bytes_per_sec = bytes as f64 / seconds;

    if bytes_per_sec >= 1_000_000_000.0 {
        format!("{:.2} GB/s", bytes_per_sec / 1_000_000_000.0)
    } else if bytes_per_sec >= 1_000_000.0 {
        format!("{:.2} MB/s", bytes_per_sec / 1_000_000.0)
    } else if bytes_per_sec >= 1_000.0 {
        format!("{:.2} KB/s", bytes_per_sec / 1_000.0)
    } else {
        format!("{bytes_per_sec:.2} B/s")
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.2} GB", bytes as f64 / 1_000_000_000.0)
    } else if bytes >= 1_000_000 {
        format!("{:.2} MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.2} KB", bytes as f64 / 1_000.0)
    } else {
        format!("{bytes} bytes")
    }
}

fn main() {
    let args = Args::parse();

    assert!(
        args.wal_instances >= 1,
        "Number of WAL instances must be at least 1"
    );

    println!("WAL Benchmark Configuration:");
    println!("  Threads: {}", args.threads);
    println!("  Write size: {} bytes", args.write_size);
    println!(
        "  Duration: {} seconds ({} minutes)",
        args.duration_secs,
        args.duration_secs / 60
    );
    println!("  WAL instances: {}", args.wal_instances);
    println!("  Direct I/O: {}", args.direct_io);
    println!();

    // Setup database directory
    let _temp_dir;
    let db_path = if let Some(ref path) = args.path {
        let path = std::path::PathBuf::from(path);
        // Create directory if it doesn't exist
        if !path.exists() {
            std::fs::create_dir_all(&path).expect("Failed to create database directory");
        }
        path
    } else {
        _temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        _temp_dir.path().to_path_buf()
    };

    println!("Database path: {db_path:?}");
    println!();

    // Setup WAL
    let wal_layout = WalLayout {
        frag_size: 1024 * 1024 * 1024,
        max_maps: 16,
        direct_io: args.direct_io,
        wal_file_size: 10 * 1024 * 1024 * 1024,
        kind: WalKind::Replay,
    };

    // Create multiple WAL instances in separate subdirectories
    let mut wal_writers = Vec::new();
    let metrics = Metrics::new();

    for i in 0..args.wal_instances {
        let wal_path = if args.wal_instances == 1 {
            db_path.clone()
        } else {
            let instance_path = db_path.join(format!("wal_{i}"));
            std::fs::create_dir_all(&instance_path)
                .expect("Failed to create WAL instance directory");
            instance_path
        };

        let wal =
            Wal::open(&wal_path, wal_layout.clone(), metrics.clone()).expect("Failed to open WAL");
        let wal_writer = Arc::new(
            wal.wal_iterator(0)
                .expect("Failed to create WAL iterator")
                .into_writer(None),
        );

        wal_writers.push(wal_writer);
    }

    // Create progress bar
    let progress_bar = ProgressBar::new(args.duration_secs);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len}s | {msg}")
            .unwrap()
            .progress_chars("#>-"),
    );

    // Atomic counters for progress tracking
    let total_writes = Arc::new(AtomicU64::new(0));
    let total_bytes = Arc::new(AtomicU64::new(0));

    // Spawn progress bar update thread
    let pb = progress_bar.clone();
    let tw = Arc::clone(&total_writes);
    let tb = Arc::clone(&total_bytes);
    let duration_secs = args.duration_secs;
    let progress_handle = thread::spawn(move || {
        let start = Instant::now();
        let mut prev_writes = 0u64;
        let mut prev_bytes = 0u64;
        let mut prev_time = start;

        loop {
            thread::sleep(Duration::from_millis(500));

            let now = Instant::now();
            let elapsed = now.duration_since(start).as_secs();
            let writes = tw.load(Ordering::Relaxed);
            let bytes = tb.load(Ordering::Relaxed);

            // Calculate instantaneous rates
            let time_delta = now.duration_since(prev_time).as_secs_f64();
            let writes_delta = writes.saturating_sub(prev_writes);
            let bytes_delta = bytes.saturating_sub(prev_bytes);

            let ops_per_sec = if time_delta > 0.0 {
                writes_delta as f64 / time_delta
            } else {
                0.0
            };

            prev_writes = writes;
            prev_bytes = bytes;
            prev_time = now;

            if elapsed >= duration_secs {
                pb.set_position(duration_secs);
                pb.finish_with_message(format!(
                    "{} ops ({} written) - Completed!",
                    writes,
                    format_bytes(bytes)
                ));
                break;
            }

            pb.set_position(elapsed);
            pb.set_message(format!(
                "{} ops ({}) | {:.0} ops/sec, {}",
                writes,
                format_bytes(bytes),
                ops_per_sec,
                format_throughput(bytes_delta, Duration::from_secs_f64(time_delta))
            ));
        }
    });

    let start_time = Instant::now();
    let duration = Duration::from_secs(args.duration_secs);

    // Spawn worker threads - each thread is assigned to a WAL instance via round-robin
    let handles: Vec<_> = (0..args.threads)
        .map(|thread_id| {
            let wal_instance_id = thread_id % args.wal_instances;
            let wal_writer = Arc::clone(&wal_writers[wal_instance_id]);
            let write_size = args.write_size;
            let tw = Arc::clone(&total_writes);
            let tb = Arc::clone(&total_bytes);

            thread::spawn(move || {
                run_benchmark_thread(thread_id, wal_writer, write_size, duration, tw, tb)
            })
        })
        .collect();

    // Wait for all threads to complete and collect results
    let mut results = Vec::new();
    for handle in handles {
        match handle.join() {
            Ok(result) => results.push(result),
            Err(e) => eprintln!("Thread panicked: {e:?}"),
        }
    }

    // Wait for progress bar thread to finish
    progress_handle.join().unwrap();

    let total_elapsed = start_time.elapsed();

    // Calculate aggregate statistics
    let total_writes_final: usize = results.iter().map(|r| r.writes_completed).sum();
    let total_bytes_final: u64 = results.iter().map(|r| r.bytes_written).sum();

    // Calculate overall QPS and throughput
    let overall_qps = total_writes_final as f64 / total_elapsed.as_secs_f64();
    let overall_throughput = format_throughput(total_bytes_final, total_elapsed);

    // Print results
    println!();
    println!("Benchmark Results:");
    println!("==================");
    println!();
    println!("Overall Statistics:");
    println!("  Total elapsed time: {total_elapsed:.2?}");
    println!("  Total writes: {total_writes_final}");
    println!(
        "  Total bytes written: {} ({})",
        format_bytes(total_bytes_final),
        total_bytes_final
    );
    println!("  Overall QPS: {overall_qps:.2} writes/sec");
    println!("  Overall throughput: {overall_throughput}");

    println!("  Metric wal_write_wait: {}", metrics.wal_write_wait.get());

    println!();
    println!("Benchmark completed successfully!");
}
