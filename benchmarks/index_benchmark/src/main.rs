use clap::{Parser, Subcommand};
use prometheus::Registry;
use rand::Rng;
use std::env;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::ops::Range;
use std::path::Path;
use tidehunter::metrics::print_histogram_stats;

use minibytes::Bytes;
use std::io::Read;
use std::time::{Duration, Instant};
use tidehunter::file_reader::{set_direct_options, FileReader};
use tidehunter::index::index_format::IndexFormat;
use tidehunter::index::index_table::IndexTable;
use tidehunter::index::lookup_header::LookupHeaderIndex;
use tidehunter::index::uniform_lookup::UniformLookupIndex;
use tidehunter::key_shape::KeyShape;
use tidehunter::key_shape::KeySpace;
use tidehunter::lookup::FileRange;
use tidehunter::metrics::Metrics;
use tidehunter::wal::WalPosition;

/// Generates a file with serialized indices for benchmarking
pub(crate) fn generate_index_file<P: IndexFormat + Send + Sync + 'static + Clone>(
    output_path: &Path,
    n_indices: usize,
    entries_per_index: usize,
    index_format: P,
) -> std::io::Result<()> {
    println!(
        "Generating index file with {} indices, {} entries each",
        n_indices, entries_per_index
    );

    // Create the main output file and write the number of indices
    let mut file = File::create(output_path)?;
    file.write_all(&n_indices.to_be_bytes())?;

    // Create KeyShape for the benchmark
    let (key_shape, ks) = KeyShape::new_single(32, 1, 1); // Using 32-byte keys
    let ks_desc = key_shape.ks(ks);

    let start = Instant::now();

    // Create a temporary directory for index chunks
    let project_root = env::current_dir()?;
    let temp_dir = tempfile::Builder::new()
        .prefix("tidehunter_index_gen_")
        .tempdir_in(project_root)?;

    // Create a tokio runtime for parallel processing
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .build()
        .unwrap();

    // No need to clone index_format since we own it
    // let index_format = index_format.clone();
    let ks_desc = ks_desc.clone();

    // Generate indices in parallel and write to temporary files
    // todo replace temporary files with a channel
    runtime.block_on(async {
        let mut tasks = Vec::new();

        for i in 0..n_indices {
            let index_format_clone = index_format.clone();
            let ks_desc_clone = ks_desc.clone();
            let temp_path = temp_dir.path().join(format!("index_{}.tmp", i));

            // Spawn a task for each index
            let task = tokio::spawn(async move {
                if i % 100 == 0 {
                    println!("  Generating index {}...", i);
                }

                // Create an IndexTable with m entries
                let mut index = IndexTable::default();
                let mut rng = rand::thread_rng();

                // Fill with random entries
                for _ in 0..entries_per_index {
                    // Create a random key (32 bytes)
                    let mut key = vec![0u8; 32];
                    rng.fill(&mut key[..]);

                    // Create a random WalPosition
                    let position = WalPosition::test_value(rng.gen());

                    // Insert into the index
                    index.insert(Bytes::from(key), position);
                }

                // Serialize the index
                let bytes = index_format_clone.to_bytes(&index, &ks_desc_clone);

                // Write the serialized index to a temporary file
                let mut temp_file = File::create(&temp_path)?;
                temp_file.write_all(&bytes)?;

                Ok::<_, std::io::Error>((i, temp_path))
            });

            tasks.push(task);
        }

        // Process completed tasks as they finish
        for task in futures::future::join_all(tasks).await {
            match task {
                Ok(Ok((i, temp_path))) => {
                    if i % 100 == 0 {
                        println!("  Merging index {} into main file...", i);
                    }

                    // Read the temporary file
                    let mut temp_file = File::open(&temp_path)?;
                    let mut buffer = Vec::new();
                    temp_file.read_to_end(&mut buffer)?;

                    // Append to the main file
                    file.write_all(&buffer)?;

                    Ok::<_, std::io::Error>(())
                }
                Ok(Err(e)) => Err(e),
                Err(e) => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )),
            }?
        }

        Ok::<_, std::io::Error>(())
    })?;

    file.flush()?;
    println!("Index file generated successfully in {:?}", start.elapsed());
    Ok(())
}

struct IndexBenchmark<'a> {
    index_count: u64,
    readers: Vec<FileRange<'a>>,
    key_shape: KeyShape,
    ks: KeySpace,
}

impl<'a> IndexBenchmark<'a> {
    fn load_from_file(file: &'a File, file_length: u64, direct_io: bool) -> std::io::Result<Self> {
        let reader = FileReader::new(file, direct_io);
        // get index count (first 8 bytes of file)
        let index_count;
        match reader.read_exact_at(0, 8) {
            Ok(buf) => {
                index_count = u64::from_be_bytes(buf.as_ref().try_into().unwrap());
            }
            Err(_) => {
                panic!("Failed to read index count");
            }
        };

        assert!(
            (file_length - 8) % index_count == 0,
            "File size is not a multiple of index count"
        );
        let index_size = (file_length - 8) / index_count;

        let mut readers = Vec::new();

        for i in 0..index_count as u64 {
            let range = Range {
                start: 8 + i * index_size,
                end: 8 + (i + 1) * index_size as u64,
            };
            readers.push(FileRange::new(FileReader::new(file, direct_io), range));
        }

        let (key_shape, ks) = KeyShape::new_single(32, 1, 1);

        Ok(Self {
            index_count,
            readers,
            key_shape,
            ks,
        })
    }

    fn run_benchmark<P: IndexFormat>(
        &self,
        index_format: &P,
        num_lookups: usize,
        batch_size: usize,
    ) -> Vec<Duration> {
        let ks_desc = self.key_shape.ks(self.ks);
        let mut rng = rand::thread_rng();
        let mut durations = Vec::with_capacity(num_lookups / batch_size);

        println!(
            "Running {} lookups in batches of {}",
            num_lookups, batch_size
        );

        for _ in 0..(num_lookups / batch_size) {
            let start = Instant::now();

            for _ in 0..batch_size {
                // Choose a random index
                // todo revert to self.index_count. this solution avoid alignment issues
                let index_idx = rng.gen_range(0..self.index_count - 1) as usize;
                let reader = &self.readers[index_idx];

                // Create a random key to look up
                let mut key = vec![0u8; 32];
                rng.fill(&mut key[..]);

                // Look up the key
                index_format.lookup_unloaded(ks_desc, reader, &key);
            }

            durations.push(start.elapsed());
        }

        durations
    }
}

fn analyze_results(name: &str, durations: &[Duration], batch_size: usize) {
    let total_lookups = durations.len() * batch_size;
    let total_time: Duration = durations.iter().sum();
    let throughput = total_lookups as f64 / total_time.as_secs_f64();

    // Calculate simple statistics without statrs dependency
    let ns_per_lookup: Vec<f64> = durations
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

    println!("{} Results:", name);
    println!("  Total: {} lookups in {:.2?}", total_lookups, total_time);
    println!("  Throughput: {:.2} lookups/sec", throughput);
    println!("  Latency per lookup:");
    println!("    Mean: {:.2} ns", mean);
    println!("    Std Dev: {:.2} ns", std_dev);
    println!("    Min: {:.2} ns", min);
    println!("    Max: {:.2} ns", max);
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
        #[arg(long, default_value_t = 500)]
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
            println!("Generating LookupHeaderIndex file: {:?}", header_path);
            generate_index_file(
                header_path,
                num_indices,
                entries_per_index,
                LookupHeaderIndex::new_with_default_metrics(),
            )
            .expect("Failed to generate LookupHeaderIndex file");

            println!("Generating UniformLookupIndex file: {:?}", uniform_path);
            generate_index_file(
                uniform_path,
                num_indices,
                entries_per_index,
                UniformLookupIndex::new_with_default_metrics(),
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
        } => {
            let header_registry = Registry::new();
            let uniform_registry = Registry::new();
            let header_metrics = Metrics::new_in(&header_registry);
            let uniform_metrics = Metrics::new_in(&uniform_registry);

            let uniform_path = Path::new(&uniform_file);
            let mut options = OpenOptions::new();
            options.read(true);
            set_direct_options(&mut options, direct_io);

            let uniform_file = options
                .open(uniform_path)
                .expect("Failed to open UniformLookupIndex file");
            let uniform_file_length = std::fs::metadata(uniform_path)
                .expect("Failed to get file metadata")
                .len();
            let uniform_bench =
                IndexBenchmark::load_from_file(&uniform_file, uniform_file_length, direct_io)
                    .expect("Failed to load UniformLookupIndex benchmark file");

            let header_path = Path::new(&header_file);
            let header_file = options
                .open(header_path)
                .expect("Failed to open HeaderLookupIndex file");
            let header_file_length = std::fs::metadata(header_path)
                .expect("Failed to get file metadata")
                .len();
            let header_bench =
                IndexBenchmark::load_from_file(&header_file, header_file_length, direct_io)
                    .expect("Failed to load HeaderLookupIndex benchmark file");

            let mut header_durations = Vec::with_capacity(num_runs * num_lookups / batch_size);
            let mut uniform_durations = Vec::with_capacity(num_runs * num_lookups / batch_size);
            for _ in 0..num_runs {
                let mut durations = header_bench.run_benchmark(
                    &LookupHeaderIndex::new(header_metrics.clone()),
                    num_lookups,
                    batch_size,
                );
                header_durations.append(&mut durations);
                analyze_results("HeaderLookupIndex", &header_durations, batch_size);

                durations = uniform_bench.run_benchmark(
                    &UniformLookupIndex::new_with_window_size(uniform_metrics.clone(), window_size),
                    num_lookups,
                    batch_size,
                );
                uniform_durations.append(&mut durations);
                analyze_results("UniformLookupIndex", &uniform_durations, batch_size);
            }

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
