use prometheus::Registry;
use rand::Rng;
use std::env;
use std::fs::File;
use std::io::{Seek, Write};
use std::ops::Range;
use std::path::Path;
use tidehunter::metrics::print_histogram_stats;

use crate::file_reader::FileReader;
use crate::index::index_format::IndexFormat;
use crate::index::index_table::IndexTable;
use crate::index::lookup_header::LookupHeaderIndex;
use crate::index::uniform_lookup::UniformLookupIndex;
use crate::key_shape::KeyShape;
use crate::lookup::FileRange;
use crate::metrics::Metrics;
use crate::wal::WalPosition;
use minibytes::Bytes;
use std::io::{BufReader, Read};
use std::time::{Duration, Instant};

use crate::key_shape::KeySpace;

const HEADER_INDEX_FILE: &str = "data/bench-header-100GB-100K.dat";
const UNIFORM_INDEX_FILE: &str = "data/bench-uniform-100GB-100K.dat";
const NUM_INDICES: usize = 25_000;
const ENTRIES_PER_INDEX: usize = 1_000_000;
const NUM_LOOKUPS: usize = 1_000_000;
const NUM_RUNS: usize = 10;
const USE_DIRECT_IO: bool = false;

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

pub fn generate_benchmark_files() {
    let n_indices = NUM_INDICES; // Number of indices
    let entries_per_index = ENTRIES_PER_INDEX; // Entries per index

    println!("Generating benchmark files...");

    let header_path = Path::new(HEADER_INDEX_FILE);
    println!("Generating LookupHeaderIndex file: {:?}", header_path);
    generate_index_file(header_path, n_indices, entries_per_index, LookupHeaderIndex)
        .expect("Failed to generate LookupHeaderIndex file");

    let uniform_path = Path::new(UNIFORM_INDEX_FILE);
    println!("Generating UniformLookupIndex file: {:?}", uniform_path);
    generate_index_file(
        uniform_path,
        n_indices,
        entries_per_index,
        UniformLookupIndex::new_with_default_metrics(),
    )
    .expect("Failed to generate UniformLookupIndex file");

    println!("Benchmark files generated.");
}

struct IndexBenchmark<'a> {
    index_count: u64,
    readers: Vec<FileRange<'a>>,
    key_shape: KeyShape,
    ks: KeySpace,
}

impl<'a> IndexBenchmark<'a> {
    fn load_from_file(file: &'a File, file_length: u64) -> std::io::Result<Self> {
        let mut reader = BufReader::new(file);
        // get index count (first 8 bytes of file)
        let mut size_buf = [0u8; 8];
        let index_count;
        match reader.read_exact(&mut size_buf) {
            Ok(_) => {
                index_count = u64::from_be_bytes(size_buf);
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
        let cursor = reader.stream_position().unwrap();
        for i in 0..index_count as u64 {
            let range = Range {
                start: cursor + i * index_size,
                end: cursor + (i + 1) * index_size as u64,
            };
            readers.push(FileRange::new(FileReader::new(file, USE_DIRECT_IO), range));
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
                let index_idx = rng.gen_range(0..self.index_count) as usize;
                let reader = &self.readers[index_idx];

                // Create a random key to look up
                let mut key = vec![0u8; 32];
                rng.fill(&mut key[..]);

                // Look up the key
                let _ = index_format.lookup_unloaded(ks_desc, reader, &key);
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

pub fn run_benchmarks() {
    let registry = Registry::new();
    let metrics = Metrics::new_in(&registry);

    let num_lookups = NUM_LOOKUPS;
    let batch_size = 1000;

    let uniform_path = Path::new(UNIFORM_INDEX_FILE);
    let uniform_file = File::open(uniform_path).expect("Failed to open UniformLookupIndex file");
    let uniform_file_length = std::fs::metadata(uniform_path)
        .expect("Failed to get file metadata")
        .len();
    let uniform_bench = IndexBenchmark::load_from_file(&uniform_file, uniform_file_length)
        .expect("Failed to load UniformLookupIndex benchmark file");

    let header_path = Path::new(HEADER_INDEX_FILE);
    let header_file = File::open(header_path).expect("Failed to open UniformLookupIndex file");
    let header_file_length = std::fs::metadata(header_path)
        .expect("Failed to get file metadata")
        .len();
    let header_bench = IndexBenchmark::load_from_file(&header_file, header_file_length)
        .expect("Failed to load HeaderLookupIndex benchmark file");

    let mut header_durations = Vec::with_capacity(NUM_RUNS * NUM_LOOKUPS / batch_size);
    let mut uniform_durations = Vec::with_capacity(NUM_RUNS * NUM_LOOKUPS / batch_size);
    for _ in 0..NUM_RUNS {
        let mut durations = header_bench.run_benchmark(&LookupHeaderIndex, num_lookups, batch_size);
        header_durations.append(&mut durations);
        analyze_results("HeaderLookupIndex", &header_durations, batch_size);

        durations = uniform_bench.run_benchmark(
            &UniformLookupIndex::new(metrics.clone()),
            num_lookups,
            batch_size,
        );
        uniform_durations.append(&mut durations);
        analyze_results("UniformLookupIndex", &uniform_durations, batch_size);
    }
    print_histogram_stats(&metrics.lookup_iterations);
}
