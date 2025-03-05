use rand::Rng;
use std::fs::File;
use std::io::{BufWriter, Seek, Write};
use std::ops::Range;
use std::path::Path;

use crate::index::index_format::IndexFormat;
use crate::index::index_table::IndexTable;
use crate::index::lookup_header::LookupHeaderIndex;
use crate::index::uniform_lookup::UniformLookupIndex;
use crate::key_shape::KeyShape;
use crate::lookup::FileRange;
use crate::wal::WalPosition;
use minibytes::Bytes;
use std::io::{BufReader, Read};
use std::time::{Duration, Instant};
// use time::macros::format_description;

use crate::key_shape::KeySpace;

pub const HEADER_INDEX_FILE: &str = "data/bench_header.dat";
pub const UNIFORM_INDEX_FILE: &str = "data/bench_uniform.dat";

/// Generates a file with serialized indices for benchmarking
pub(crate) fn generate_index_file<P: IndexFormat>(
    output_path: &Path,
    n_indices: usize,
    entries_per_index: usize,
    index_format: &P,
) -> std::io::Result<()> {
    println!(
        "Generating index file with {} indices, {} entries each",
        n_indices, entries_per_index
    );

    // Create file with BufferedWriter
    let file = File::create(output_path)?;
    let mut writer = BufWriter::new(file);

    // Create KeyShape for the benchmark
    let (key_shape, ks) = KeyShape::new_single(32, 1, 1); // Using 32-byte keys
    let ks_desc = key_shape.ks(ks);

    let mut rng = rand::thread_rng();

    writer.write_all(&n_indices.to_be_bytes())?;

    // Generate N indices
    for i in 0..n_indices {
        if i % 100 == 0 {
            println!("  Generated {} indices...", i);
        }

        // Create an IndexTable with m entries
        let mut index = IndexTable::default();

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
        let bytes = index_format.to_bytes(&index, ks_desc);

        // Write size and bytes to file

        writer.write_all(&bytes)?;
    }

    writer.flush()?;
    println!("Index file generated successfully");
    Ok(())
}

pub fn generate_benchmark_files() {
    let n_indices = 1000; // Number of indices
    let entries_per_index = 1000; // Entries per index

    println!("Generating benchmark files...");

    let header_path = Path::new(HEADER_INDEX_FILE);
    println!("Generating LookupHeaderIndex file: {:?}", header_path);
    generate_index_file(
        header_path,
        n_indices,
        entries_per_index,
        &LookupHeaderIndex,
    )
    .expect("Failed to generate LookupHeaderIndex file");

    let uniform_path = Path::new(UNIFORM_INDEX_FILE);
    println!("Generating UniformLookupIndex file: {:?}", uniform_path);
    generate_index_file(
        uniform_path,
        n_indices,
        entries_per_index,
        &UniformLookupIndex::new(),
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
            readers.push(FileRange::new(&file, range));
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

        for i in 0..(num_lookups / batch_size) {
            if i % 10 == 0 {
                println!("  Completed {} batches...", i);
            }

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
    let num_lookups = 100_000;
    let batch_size = 1000;

    // Benchmark HeaderLookupIndex
    println!("\nBenchmarking HeaderLookupIndex...");
    let header_path = Path::new(HEADER_INDEX_FILE);
    let header_file = File::open(header_path).expect("Failed to open UniformLookupIndex file");
    let header_file_length = std::fs::metadata(header_path)
        .expect("Failed to get file metadata")
        .len();
    let header_bench = IndexBenchmark::load_from_file(&header_file, header_file_length)
        .expect("Failed to load HeaderLookupIndex benchmark file");

    let header_durations = header_bench.run_benchmark(&LookupHeaderIndex, num_lookups, batch_size);

    // Benchmark UniformLookupIndex
    println!("\nBenchmarking UniformLookupIndex...");
    let uniform_path = Path::new(UNIFORM_INDEX_FILE);
    let uniform_file = File::open(uniform_path).expect("Failed to open UniformLookupIndex file");
    let uniform_file_length = std::fs::metadata(uniform_path)
        .expect("Failed to get file metadata")
        .len();
    let uniform_bench = IndexBenchmark::load_from_file(&uniform_file, uniform_file_length)
        .expect("Failed to load UniformLookupIndex benchmark file");

    let uniform_durations =
        uniform_bench.run_benchmark(&UniformLookupIndex::new(), num_lookups, batch_size);

    // Analyze and compare results
    println!("\n===== BENCHMARK RESULTS =====");
    analyze_results("HeaderLookupIndex", &header_durations, batch_size);
    analyze_results("UniformLookupIndex", &uniform_durations, batch_size);

    // Calculate speedup
    let header_total: Duration = header_durations.iter().sum();
    let uniform_total: Duration = uniform_durations.iter().sum();

    let speedup = header_total.as_secs_f64() / uniform_total.as_secs_f64();
    println!(
        "\nComparison: UniformLookupIndex is {:.2}x {} than HeaderLookupIndex",
        if speedup > 1.0 {
            speedup
        } else {
            1.0 / speedup
        },
        if speedup > 1.0 { "faster" } else { "slower" }
    );
}
