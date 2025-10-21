use clap::{Parser, Subcommand};
use prometheus::Registry;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use tidehunter::metrics::print_histogram_stats;

use minibytes::Bytes;
use std::time::{Duration, Instant};
use tidehunter::WalPosition;
use tidehunter::file_reader::{FileReader, set_direct_options};
use tidehunter::index::index_format::IndexFormat;
use tidehunter::index::index_table::IndexTable;
use tidehunter::index::lookup_header::LookupHeaderIndex;
use tidehunter::index::uniform_lookup::UniformLookupIndex;
use tidehunter::key_shape::KeySpace;
use tidehunter::key_shape::{KeyShape, KeyType};
use tidehunter::lookup::FileRange;
use tidehunter::metrics::Metrics;

/// Generates a file with serialized indices for benchmarking
pub(crate) fn generate_index_file<P: IndexFormat + Send + Sync + 'static + Clone>(
    output_path: &Path,
    n_indices: usize,
    entries_per_index: usize,
    index_format: P,
) -> std::io::Result<()> {
    println!("Generating index file with {n_indices} indices, {entries_per_index} entries each");

    // Create the main output file and write the number of indices
    let mut file = File::create(output_path)?;
    file.write_all(&n_indices.to_be_bytes())?;

    // Create KeyShape for the benchmark
    let (key_shape, ks) = KeyShape::new_single(32, 1, KeyType::uniform(1)); // Using 32-byte keys
    let ks_desc = key_shape.ks(ks);

    let start = Instant::now();

    // Create a tokio runtime for parallel processing
    let num_threads = num_cpus::get();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_threads)
        .build()
        .unwrap();

    let ks_desc = ks_desc.clone();

    // Generate indices in parallel and send through a channel
    runtime.block_on(async {
        // Create a channel for sending indices
        let (tx, rx) = tokio::sync::mpsc::channel::<(usize, Vec<u8>)>(num_threads);

        // Spawn a task to consume indices and write to the file
        let consumer = tokio::spawn(async move {
            let mut rx = rx;
            let mut indices_written = 0;

            while let Some((i, buffer)) = rx.recv().await {
                if i % 100 == 0 {
                    println!("  Merging index {i} into main file...");
                }

                // Write the serialized index to the main file
                file.write_all(&buffer)?;
                indices_written += 1;
            }

            assert_eq!(
                indices_written, n_indices,
                "Not all indices were written to the file"
            );
            file.flush()?;

            Ok::<_, std::io::Error>(())
        });

        // Spawn tasks to generate indices
        let mut producer_tasks = Vec::new();
        for i in 0..num_threads {
            let tx_clone = tx.clone();
            let index_format_clone = index_format.clone();
            let ks_desc_clone = ks_desc.clone();

            // Spawn a task for each thread
            let task = tokio::spawn(async move {
                println!("  Starting task {i}...");
                let batch_size = if i < num_threads - 1 {
                    n_indices / num_threads
                } else {
                    n_indices / num_threads + n_indices % num_threads
                };

                for j in 0..batch_size {
                    let index_idx = i * batch_size + j;
                    if index_idx % 1000 == 0 {
                        println!("  Generating index {index_idx}...");
                    }

                    // Create an IndexTable with m entries
                    let mut index = IndexTable::default();
                    let mut rng = StdRng::from_entropy();

                    // Fill with random entries
                    for _ in 0..entries_per_index {
                        // Create a random key (32 bytes)
                        let mut key = vec![0u8; 32];
                        rng.fill(&mut key[..]);

                        // Create a random WalPosition
                        let position = WalPosition::test_value(rng.r#gen());

                        // Insert into the index
                        index.insert(Bytes::from(key), position);
                    }

                    // Serialize the index
                    let bytes =
                        index_format_clone.clean_serialize_index(&mut index, &ks_desc_clone);

                    // Send the serialized index through the channel
                    let result = tx_clone
                        .send((index_idx, bytes.as_ref().to_vec()))
                        .await
                        .map_err(|e| {
                            std::io::Error::other(format!(
                                "Failed to send index through channel: {e}"
                            ))
                        });

                    if let Err(e) = result {
                        println!("Failed to send index through channel: {e}");
                        return Err(e);
                    }
                }
                Ok(())
            });

            producer_tasks.push(task);
        }

        // Drop the original sender so the channel will close when all producer tasks are done
        drop(tx);

        // Wait for all producer tasks to complete
        for task in futures::future::join_all(producer_tasks).await {
            match task {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(std::io::Error::other(e.to_string())),
            }?
        }

        // Wait for the consumer task to complete
        match consumer.await {
            Ok(result) => result,
            Err(e) => Err(std::io::Error::other(e.to_string())),
        }
    })?;

    println!("Index file generated successfully in {:?}", start.elapsed());
    Ok(())
}

struct IndexBenchmark {
    readers: Vec<FileRange>,
    key_shape: KeyShape,
    ks: KeySpace,
}

impl IndexBenchmark {
    fn load_from_file(file: Arc<File>, file_length: u64, direct_io: bool) -> std::io::Result<Self> {
        let reader = FileReader::new(file.clone(), direct_io);
        // get index count (first 8 bytes of file)
        let index_count = match reader.read_exact_at(0, 8) {
            Ok(buf) => u64::from_be_bytes(buf.as_ref().try_into().unwrap()),
            Err(_) => {
                panic!("Failed to read index count");
            }
        };

        assert!(
            (file_length - 8).is_multiple_of(index_count),
            "File size is not a multiple of index count"
        );
        let index_size = (file_length - 8) / index_count;

        let mut readers = Vec::new();

        for i in 0..index_count {
            let range = Range {
                start: 8 + i * index_size,
                end: 8 + (i + 1) * index_size,
            };
            readers.push(FileRange::new(
                FileReader::new(file.clone(), direct_io),
                range,
            ));
        }

        let (key_shape, ks) = KeyShape::new_single(32, 1, KeyType::uniform(1));

        Ok(Self {
            readers,
            key_shape,
            ks,
        })
    }

    fn run_benchmark_multithreaded<P: IndexFormat + Clone + Send + 'static>(
        &self,
        index_format: P,
        num_lookups: usize,
        batch_size: usize,
        num_threads: usize,
        metrics: &Metrics,
    ) -> (Duration, Vec<Vec<Duration>>) {
        println!(
            "Running multithreaded benchmark with {num_threads} threads, {num_lookups} lookups per thread in batches of {batch_size}"
        );

        // Use thread::scope to manage threads and collect results
        let ks_desc = self.key_shape.ks(self.ks);
        let start = Instant::now();

        let thread_durations = thread::scope(|s| {
            // Spawn worker threads
            let handles: Vec<_> = (0..num_threads)
                .map(|_| {
                    let index_format = index_format.clone();
                    let ks_desc = ks_desc.clone();

                    // Each thread gets access to all readers
                    let thread_readers = &self.readers[..];

                    s.spawn(move || {
                        let mut rng = rand::thread_rng();
                        let mut durations = Vec::with_capacity(num_lookups / batch_size);

                        for _ in 0..(num_lookups / batch_size) {
                            let start = Instant::now();

                            for _ in 0..batch_size {
                                // Choose a random index from all readers
                                let index_idx = rng.gen_range(0..thread_readers.len() - 1);
                                let reader = &thread_readers[index_idx];

                                // Create a random key to look up
                                let mut key = vec![0u8; 32];
                                rng.fill(&mut key[..]);

                                // Look up the key
                                index_format.lookup_unloaded(&ks_desc, reader, &key, metrics);
                            }

                            durations.push(start.elapsed());
                        }

                        durations
                    })
                })
                .collect();

            // Collect results from all threads
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<Vec<Duration>>>()
        });

        let total_time = start.elapsed();
        (total_time, thread_durations)
    }
}

fn analyze_multithreaded_results(
    name: &str,
    end_to_end_time: Duration,
    thread_durations: &[Vec<Duration>],
    batch_size: usize,
    num_threads: usize,
) {
    // Calculate overall statistics
    let total_lookups = thread_durations
        .iter()
        .map(|t| t.len() * batch_size)
        .sum::<usize>();
    let end_to_end_throughput = total_lookups as f64 / end_to_end_time.as_secs_f64();

    // Flatten all durations for overall latency stats
    let all_durations: Vec<Duration> = thread_durations.iter().flatten().cloned().collect();

    println!("{name} Multithreaded Results:");
    println!("  Threads: {num_threads}");
    println!("  End-to-end time: {end_to_end_time:.2?}");
    println!("  Total lookups: {total_lookups}");
    println!("  End-to-end throughput: {end_to_end_throughput:.2} lookups/sec");

    // Calculate per-thread statistics
    for (i, thread_dur) in thread_durations.iter().enumerate() {
        let thread_total_lookups = thread_dur.len() * batch_size;
        let thread_total_time: Duration = thread_dur.iter().sum();
        let thread_throughput = thread_total_lookups as f64 / thread_total_time.as_secs_f64();

        // Calculate per-thread latency stats
        let ns_per_lookup: Vec<f64> = thread_dur
            .iter()
            .map(|d| d.as_nanos() as f64 / batch_size as f64)
            .collect();

        let mean = ns_per_lookup.iter().sum::<f64>() / ns_per_lookup.len() as f64;
        let variance = ns_per_lookup
            .iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>()
            / ns_per_lookup.len() as f64;
        let std_dev = variance.sqrt();
        let min = ns_per_lookup.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max = ns_per_lookup
            .iter()
            .fold(f64::NEG_INFINITY, |a, &b| a.max(b));

        println!("  Thread {i} Results:");
        println!("    Lookups: {thread_total_lookups}");
        println!("    Throughput: {thread_throughput:.2} lookups/sec");
        println!("    Latency per lookup:");
        println!("      Mean: {mean:.2} ns");
        println!("      Std Dev: {std_dev:.2} ns");
        println!("      Min: {min:.2} ns");
        println!("      Max: {max:.2} ns");
    }

    // Calculate overall latency statistics
    let all_ns_per_lookup: Vec<f64> = all_durations
        .iter()
        .map(|d| d.as_nanos() as f64 / batch_size as f64)
        .collect();

    let all_mean = all_ns_per_lookup.iter().sum::<f64>() / all_ns_per_lookup.len() as f64;
    let all_variance = all_ns_per_lookup
        .iter()
        .map(|x| (x - all_mean).powi(2))
        .sum::<f64>()
        / all_ns_per_lookup.len() as f64;
    let all_std_dev = all_variance.sqrt();
    let all_min = all_ns_per_lookup
        .iter()
        .fold(f64::INFINITY, |a, &b| a.min(b));
    let all_max = all_ns_per_lookup
        .iter()
        .fold(f64::NEG_INFINITY, |a, &b| a.max(b));

    println!("  Overall Latency Statistics:");
    println!("    Mean: {all_mean:.2} ns");
    println!("    Std Dev: {all_std_dev:.2} ns");
    println!("    Min: {all_min:.2} ns");
    println!("    Max: {all_max:.2} ns");
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate benchmark files
    Generate {
        /// Number of indices to generate
        #[arg(long, default_value_t = 25_000)]
        num_indices: usize,
        /// Number of entries per index
        #[arg(long, default_value_t = 1_000_000)]
        entries_per_index: usize,
        /// Output file for header index
        #[arg(long, default_value = "data/bench-header-100GB-100K.dat")]
        header_file: String,
        /// Output file for uniform index
        #[arg(long, default_value = "data/bench-uniform-100GB-100K.dat")]
        uniform_file: String,
    },
    /// Run benchmarks
    Run {
        /// Number of lookups to perform
        #[arg(long, default_value_t = 1_000_000)]
        num_lookups: usize,
        /// Number of benchmark runs
        #[arg(long, default_value_t = 10)]
        num_runs: usize,
        /// Batch size for lookups
        #[arg(long, default_value_t = 1000)]
        batch_size: usize,
        /// Window size for uniform index
        #[arg(long, default_value_t = 800)]
        window_size: usize,
        /// Input file for header index
        #[arg(long, default_value = "data/bench-header-100GB-100K.dat")]
        header_file: String,
        /// Input file for uniform index
        #[arg(long, default_value = "data/bench-uniform-100GB-100K.dat")]
        uniform_file: String,
        /// Whether to use direct I/O
        #[arg(long, default_value_t = false)]
        direct_io: bool,
        /// Number of threads to use for benchmark
        #[arg(long, default_value_t = 1)]
        num_threads: usize,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Generate {
            num_indices,
            entries_per_index,
            header_file,
            uniform_file,
        } => {
            let header_path = Path::new(&header_file);
            let uniform_path = Path::new(&uniform_file);

            println!("Generating benchmark files...");
            println!("Generating LookupHeaderIndex file: {header_path:?}");
            generate_index_file(
                header_path,
                num_indices,
                entries_per_index,
                LookupHeaderIndex,
            )
            .expect("Failed to generate LookupHeaderIndex file");

            println!("Generating UniformLookupIndex file: {uniform_path:?}");
            generate_index_file(
                uniform_path,
                num_indices,
                entries_per_index,
                UniformLookupIndex::new(),
            )
            .expect("Failed to generate UniformLookupIndex file");

            println!("Benchmark files generated.");
        }
        Commands::Run {
            num_lookups,
            num_runs,
            batch_size,
            window_size,
            header_file,
            uniform_file,
            direct_io,
            num_threads,
        } => {
            let header_registry = Registry::new();
            let uniform_registry = Registry::new();
            let header_metrics = Metrics::new_in(&header_registry);
            let uniform_metrics = Metrics::new_in(&uniform_registry);

            let uniform_path = Path::new(&uniform_file);
            let mut options = OpenOptions::new();
            options.read(true);
            set_direct_options(&mut options, direct_io);

            let uniform_file = Arc::new(
                options
                    .open(uniform_path)
                    .expect("Failed to open UniformLookupIndex file"),
            );
            let uniform_file_length = std::fs::metadata(uniform_path)
                .expect("Failed to get file metadata")
                .len();
            let uniform_bench =
                IndexBenchmark::load_from_file(uniform_file, uniform_file_length, direct_io)
                    .expect("Failed to load UniformLookupIndex benchmark file");

            let header_path = Path::new(&header_file);
            let header_file = Arc::new(
                options
                    .open(header_path)
                    .expect("Failed to open HeaderLookupIndex file"),
            );
            let header_file_length = std::fs::metadata(header_path)
                .expect("Failed to get file metadata")
                .len();
            let header_bench =
                IndexBenchmark::load_from_file(header_file, header_file_length, direct_io)
                    .expect("Failed to load HeaderLookupIndex benchmark file");

            println!("Running multithreaded benchmark with {num_threads} threads");
            for run in 0..num_runs {
                println!("Run {}/{}", run + 1, num_runs);

                // Run HeaderLookupIndex benchmark
                let (header_total_time, header_thread_durations) = header_bench
                    .run_benchmark_multithreaded(
                        LookupHeaderIndex,
                        num_lookups,
                        batch_size,
                        num_threads,
                        &header_metrics,
                    );

                analyze_multithreaded_results(
                    "HeaderLookupIndex",
                    header_total_time,
                    &header_thread_durations,
                    batch_size,
                    num_threads,
                );

                // Run UniformLookupIndex benchmark
                let (uniform_total_time, uniform_thread_durations) = uniform_bench
                    .run_benchmark_multithreaded(
                        UniformLookupIndex::new_with_window_size(window_size),
                        num_lookups,
                        batch_size,
                        num_threads,
                        &uniform_metrics,
                    );

                analyze_multithreaded_results(
                    "UniformLookupIndex",
                    uniform_total_time,
                    &uniform_thread_durations,
                    batch_size,
                    num_threads,
                );
            }
            // }

            // Print HeaderLookupIndex stats
            println!(
                "HeaderLookupIndex: scan mcs {:?}",
                header_metrics.lookup_scan_mcs
            );
            println!(
                "HeaderLookupIndex: io mcs {:?}",
                header_metrics.lookup_io_mcs
            );
            println!(
                "HeaderLookupIndex: io bytes {:?}",
                header_metrics.lookup_io_bytes
            );

            // Print UniformLookupIndex stats
            print_histogram_stats(&uniform_metrics.lookup_iterations);
            println!(
                "UniformLookupIndex: scan mcs {:?}",
                uniform_metrics.lookup_scan_mcs
            );
            println!(
                "UniformLookupIndex: io mcs {:?}",
                uniform_metrics.lookup_io_mcs
            );
            println!(
                "UniformLookupIndex: io bytes {:?}",
                uniform_metrics.lookup_io_bytes
            );
        }
    }
}
