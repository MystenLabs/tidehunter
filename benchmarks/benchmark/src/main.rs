use crate::configs::METRICS_PORT;
use crate::metrics::BenchmarkMetrics;
use crate::storage::rocks::RocksStorage;
use crate::storage::Storage;
use ::prometheus::Registry;
use bytes::BufMut;
use clap::Parser;
use configs::{
    Backend, KeyLayout, ReadMode, StressArgs, StressClientParameters, StressTestConfigs,
};
use histogram::AtomicHistogram;
use parking_lot::RwLock;
use rand::rngs::{StdRng, ThreadRng};
use rand::{Rng, RngCore, SeedableRng};
use rand_distr::{Distribution, Zipf};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};
use std::{fs, thread};
use tidehunter::key_shape::{KeyShape, KeySpaceConfig, KeyType};

mod configs;
mod metrics;
#[allow(dead_code)]
mod prometheus;
mod storage;

macro_rules! report {
    ($report: expr, $($arg:tt)*) => {
        let line = format!($($arg)*);
        println!("{line}");
        $report.lines.push('\n');
        $report.lines.push_str(&line);
    };
}

pub fn main() {
    let start_time = SystemTime::now();
    let mut report = Report::default();

    let args: StressArgs = StressArgs::parse();
    let default_config = if let Some(parameters_path) = &args.parameters_path {
        println!("Loading default configs from {}", parameters_path);
        StressTestConfigs::from_yml(parameters_path).unwrap()
    } else {
        StressTestConfigs::default()
    };
    let config = configs::override_default_args(args, default_config);

    println!("DB parameters: {:#?}", &config.db_parameters);
    println!(
        "Stress client parameters: {:#?}",
        &config.stress_client_parameters
    );

    let temp_dir = if let Some(path) = &config.stress_client_parameters.path {
        tempdir::TempDir::new_in(path, "stress").unwrap()
    } else {
        tempdir::TempDir::new("stress").unwrap()
    };

    let path = if let Some(reuse) = &config.stress_client_parameters.reuse {
        reuse.parse().unwrap()
    } else {
        temp_dir.path().to_path_buf()
    };

    println!("Path to storage: {}", path.display());
    println!(
        "Using {:?} key layout",
        config.stress_client_parameters.key_layout
    );
    println!(
        "Using {:?} read mode",
        config.stress_client_parameters.read_mode
    );
    let print_report = config.stress_client_parameters.report;
    let registry = Registry::new();
    let benchmark_metrics = BenchmarkMetrics::new_in(&registry);
    prometheus::start_prometheus_server(
        format!("0.0.0.0:{METRICS_PORT}").parse().unwrap(),
        &registry,
    );
    let storage: Arc<dyn Storage> = match config.stress_client_parameters.backend {
        Backend::Tidehunter => {
            if config.db_parameters.direct_io {
                report!(report, "Using **direct IO**");
            }
            use crate::storage::tidehunter::TidehunterStorage;
            let mutexes = 4096 * 32;
            let (key_shape, ks) = match config.stress_client_parameters.key_layout {
                KeyLayout::Uniform => KeyShape::new_single_config(
                    32,
                    mutexes,
                    KeyType::uniform(1),
                    key_space_config(),
                ),
                KeyLayout::SequenceChoice => {
                    let key_type = KeyType::prefix_uniform(8, 2);
                    KeyShape::new_single_config(32, mutexes, key_type, key_space_config())
                }
                KeyLayout::ChoiceSequence => {
                    let key_type = KeyType::prefix_uniform(15, 5);
                    KeyShape::new_single_config(32, mutexes, key_type, key_space_config())
                }
            };
            let storage =
                TidehunterStorage::open(&registry, config.db_parameters, &path, (key_shape, ks));
            if !config.stress_client_parameters.no_snapshot {
                report!(report, "Periodic snapshot **enabled**");
                storage.db.start_periodic_snapshot();
            } else {
                report!(report, "Periodic snapshot **disabled**");
            }
            Arc::new(storage)
        }
        Backend::Rocksdb => {
            let storage = RocksStorage::open(&path);
            Arc::new(storage)
        }
    };
    let stress = Stress {
        storage,
        parameters: Arc::new(config.stress_client_parameters),
        benchmark_metrics,
    };
    println!("Starting write test");
    let write_sec;
    if stress.parameters.reuse.is_none() {
        let elapsed = stress.measure(
            stress.parameters.write_threads,
            StressThread::run_writes,
            &mut report,
        );
        let written = stress.parameters.writes * stress.parameters.write_threads;
        let written_bytes = written * stress.parameters.write_size;
        let msecs = elapsed.as_millis() as usize;
        write_sec = dec_div(written / msecs * 1000);
        report!(
            report,
            "Write test done in {elapsed:?}: {} writes/s, {}/sec",
            write_sec,
            byte_div(written_bytes / msecs * 1000)
        );
    } else {
        write_sec = "".to_string();
        println!("Skipping writes because reuse is specified");
    }
    {
        let storage_len = fs_extra::dir::get_size(&path).unwrap();
        report!(
            report,
            "Storage used {:.1} Gb",
            storage_len as f64 / 1024. / 1024. / 1024.
        );
    }
    println!(
        "Starting mixed read/write test ({}% reads, {}% writes)",
        stress.parameters.read_percentage,
        100 - stress.parameters.read_percentage
    );
    let manual_stop = if stress.parameters.background_writes > 0 {
        stress.background(
            stress.parameters.write_threads,
            StressThread::run_background_writes,
        )
    } else {
        Default::default()
    };
    let elapsed = stress.measure(
        stress.parameters.mixed_threads,
        StressThread::run_mixed_operations,
        &mut report,
    );
    manual_stop.store(true, Ordering::Relaxed);
    let total_ops = stress.parameters.operations * stress.parameters.mixed_threads;
    let total_bytes = total_ops * stress.parameters.write_size;
    let msecs = elapsed.as_millis() as usize;
    let ops_sec = dec_div(total_ops / msecs * 1000);
    report!(
        report,
        "Mixed test done in {elapsed:?}: {} ops/s, {}/sec",
        ops_sec,
        byte_div(total_bytes / msecs * 1000),
    );
    if print_report {
        println!("Writing report file");
        fs::write("report.txt", &report.lines).unwrap();
    }
    if !stress.parameters.tldr.is_empty() {
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open("tldr.txt")
            .unwrap();
        let start_time = start_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let end_time = SystemTime::now();
        let end_time = end_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        writeln!(
            file,
            "{: <15}|{: <15}|{: <24}|{: <8}|{: <8}",
            start_time, end_time, stress.parameters.tldr, write_sec, ops_sec
        )
        .unwrap();
    }
    if stress.parameters.preserve {
        temp_dir.into_path();
    }
}

fn key_space_config() -> KeySpaceConfig {
    use tidehunter::index::index_format::IndexFormatType;
    use tidehunter::index::uniform_lookup::UniformLookupIndex;
    KeySpaceConfig::new()
        .with_index_format(IndexFormatType::Uniform(
            UniformLookupIndex::new_with_window_size(744),
        ))
        .with_unloaded_iterator(true)
}

struct Stress {
    storage: Arc<dyn Storage>,
    parameters: Arc<StressClientParameters>,
    benchmark_metrics: Arc<BenchmarkMetrics>,
}

#[derive(Default)]
struct Report {
    lines: String,
}

impl Stress {
    pub fn background<F: FnOnce(StressThread) + Clone + Send + 'static>(
        &self,
        n: usize,
        f: F,
    ) -> Arc<AtomicBool> {
        let (_, manual_stop, _, _) = self.start_threads(n, f);
        manual_stop
    }

    pub fn measure<F: FnOnce(StressThread) + Clone + Send + 'static>(
        &self,
        n: usize,
        f: F,
        report: &mut Report,
    ) -> Duration {
        let (threads, _, latency, latency_errors) = self.start_threads(n, f);
        let start = Instant::now();
        for t in threads {
            t.join().unwrap();
        }
        let latency = latency.drain();
        let percentiles = latency
            .percentiles(&[50., 90., 99., 99.9, 99.99, 99.999])
            .unwrap()
            .unwrap();
        let p = move |i: usize| percentiles.get(i).unwrap().1.range();
        let latency_errors = latency_errors.load(Ordering::Relaxed);
        let latency_errors = if latency_errors > 0 {
            format!(", {latency_errors} out of bound")
        } else {
            "".to_string()
        };
        report!(report, "Latency(mcs): p50: {:?}, p90: {:?}, p99: {:?}, p99.9: {:?}, p99.99: {:?}, p99.999: {:?}{latency_errors}",
        p(0), p(1), p(2), p(3), p(4), p(5));
        start.elapsed()
    }

    fn start_threads<F: FnOnce(StressThread) + Clone + Send + 'static>(
        &self,
        n: usize,
        f: F,
    ) -> (
        Vec<JoinHandle<()>>,
        Arc<AtomicBool>,
        Arc<AtomicHistogram>,
        Arc<AtomicUsize>,
    ) {
        let mut threads = Vec::with_capacity(n);
        let start_lock = Arc::new(RwLock::new(()));
        let start_w = start_lock.write();
        let manual_stop = Arc::new(AtomicBool::new(false));
        let latency = AtomicHistogram::new(12, 26).unwrap();
        let latency = Arc::new(latency);
        let latency_errors = Arc::new(AtomicUsize::default());
        for index in 0..n {
            let thread = StressThread {
                db: self.storage.clone(),
                start_lock: start_lock.clone(),
                parameters: self.parameters.clone(),
                index: index as u64,
                manual_stop: manual_stop.clone(),

                latency: latency.clone(),
                latency_errors: latency_errors.clone(),
                benchmark_metrics: self.benchmark_metrics.clone(),
            };
            let f = f.clone();
            let thread = thread::spawn(move || f(thread));
            threads.push(thread);
        }
        drop(start_w);
        (threads, manual_stop, latency, latency_errors)
    }
}

fn dec_div(n: usize) -> String {
    const M: usize = 1_000_000;
    const K: usize = 1_000;
    if n > M {
        format!("{:.2}M", n as f64 / M as f64)
    } else if n > K {
        format!("{:.2}K", n as f64 / K as f64)
    } else {
        format!("{n}")
    }
}

fn byte_div(n: usize) -> String {
    const K: usize = 1_024;
    const M: usize = K * K;
    if n > M {
        format!("{}Mb", n / M)
    } else if n > K {
        format!("{}Kb", n / K)
    } else {
        format!("{n}")
    }
}

struct StressThread {
    db: Arc<dyn Storage>,
    start_lock: Arc<RwLock<()>>,
    parameters: Arc<StressClientParameters>,
    index: u64,
    manual_stop: Arc<AtomicBool>,

    latency: Arc<AtomicHistogram>,
    latency_errors: Arc<AtomicUsize>,
    benchmark_metrics: Arc<BenchmarkMetrics>,
}

impl StressThread {
    pub fn run_writes(self) {
        #[allow(clippy::let_underscore_lock)] // RWLock here acts as a barrier
        let _ = self.start_lock.read();
        for pos in 0..self.parameters.writes {
            let pos = self.global_pos(pos);
            let (key, value) = self.key_value(pos);
            let timer = Instant::now();
            self.db.insert(key.into(), value.into());
            let latency = timer.elapsed().as_micros();
            self.benchmark_metrics
                .bench_writes
                .with_label_values(&[self.db.name()])
                .observe(latency as f64);
            self.latency.increment(latency as u64).unwrap();
        }
    }

    pub fn run_background_writes(self) {
        let writes_per_thread = self.parameters.background_writes / self.parameters.write_threads;
        let delay = Duration::from_micros(1_000_000 / writes_per_thread as u64);
        let mut deadline = Instant::now();
        let mut pos = u32::MAX;
        while !self.manual_stop.load(Ordering::Relaxed) {
            deadline += delay;
            pos -= 1;
            let pos = self.global_pos(pos as usize);
            let (key, value) = self.key_value(pos);
            self.db.insert(key.into(), value.into());
            thread::sleep(
                deadline
                    .checked_duration_since(Instant::now())
                    .unwrap_or_default(),
            )
        }
    }

    pub fn run_mixed_operations(self) {
        #[allow(clippy::let_underscore_lock)] // RWLock here acts as a barrier
        let _ = self.start_lock.read();
        let mut thread_rng = ThreadRng::default();
        let read_percentage = self.parameters.read_percentage;

        // Start writing new keys just after the ones written in the initial phase.
        let mut local_write_pos_counter = self.parameters.writes;

        for _ in 0..self.parameters.operations {
            // Randomly decide whether to read or write based on percentage
            let do_read = thread_rng.gen_range(0..100) < read_percentage;

            if do_read {
                // Perform a read operation.
                // Read from the whole keyspace, which expands as writes are made. The highest
                // key position this thread can read is determined by the latest key it has written.
                let highest_local_pos = local_write_pos_counter.saturating_sub(1);
                let read_pos_upper_bound = self.global_pos(highest_local_pos) + 1;

                let pos = if self.parameters.zipf_exponent != 0.0 && read_pos_upper_bound > 1 {
                    let zipf =
                        Zipf::new(read_pos_upper_bound, self.parameters.zipf_exponent).unwrap();
                    let sample = zipf.sample(&mut thread_rng) as u64;

                    // The Zipf distribution generates number from 1 to N, where lower numbers are
                    // more likely. We want to read higher positions more often, so we subtract
                    // the sample from the upper bound.
                    read_pos_upper_bound - sample
                } else if read_pos_upper_bound > 0 {
                    thread_rng.gen_range(0..read_pos_upper_bound)
                } else {
                    0
                };

                let timer;
                match self.parameters.read_mode {
                    ReadMode::Get => {
                        let (key, value) = self.key_value(pos);
                        timer = Instant::now();
                        if let Some(found_value) = self.db.get(&key) {
                            assert_eq!(
                                &value[..],
                                &found_value[..],
                                "Found value does not match expected value"
                            );
                        }
                        // If the key is not found, we do nothing as it may not have been written yet.
                        // This can happen because we select pos between 0 and global_pos(highest_local_pos)
                        // This range includes global positions that are owned by other threads,
                        // who may not have used those positions yet.
                    }
                    ReadMode::Lt(iterations) => {
                        let mut key = vec![0u8; self.parameters.key_len];
                        thread_rng.fill(&mut key[..]);
                        timer = Instant::now();
                        let result = self.db.get_lt(&key, iterations);
                        let result = if result.len() == iterations {
                            "found"
                        } else if result.is_empty() {
                            "not_found"
                        } else {
                            "partial"
                        };
                        self.benchmark_metrics
                            .get_lt_result
                            .with_label_values(&[result])
                            .inc();
                    }
                    ReadMode::Exists => {
                        let (key, _) = self.key_value(pos);
                        timer = Instant::now();
                        let exists = self.db.exists(&key);
                        // For exists mode, we expect the key to exist if pos < self.parameters.writes
                        // since those were written in the initial write phase
                        if pos < self.global_pos(self.parameters.writes - 1) {
                            assert!(exists, "Key should exist but was not found");
                        }
                        // Keys beyond initial writes may or may not exist depending on
                        // whether they were written during the mixed phase. For more details,
                        // see comment above for get mode.
                    }
                }
                let latency = timer.elapsed().as_micros();
                self.benchmark_metrics
                    .bench_reads
                    .with_label_values(&[self.db.name()])
                    .observe(latency as f64);
                if self.latency.increment(latency as u64).is_err() {
                    self.latency_errors.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                // Perform a write operation to a new key beyond the initial dataset.
                let pos = self.global_pos(local_write_pos_counter);
                local_write_pos_counter += 1;

                let (key, value) = self.key_value(pos);
                let timer = Instant::now();
                self.db.insert(key.into(), value.into());
                let latency = timer.elapsed().as_micros();
                self.benchmark_metrics
                    .bench_writes
                    .with_label_values(&[self.db.name()])
                    .observe(latency as f64);
                if self.latency.increment(latency as u64).is_err() {
                    self.latency_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    #[allow(dead_code)]
    fn key(&self, pos: u64) -> Vec<u8> {
        let (key, _) = self.key_and_rng(pos);
        key
    }

    fn key_and_rng(&self, pos: u64) -> (Vec<u8>, StdRng) {
        let mut rng = Self::rng_at(pos);
        let mut key = vec![0u8; self.parameters.key_len];
        rng.fill_bytes(&mut key);
        match self.parameters.key_layout {
            KeyLayout::Uniform => {}
            KeyLayout::SequenceChoice => {
                // the first 16 bytes of a key are not random anymore
                // First 8 bytes are a sequentially growing value (like consensus round)
                key[..8].copy_from_slice(&u64::to_be_bytes(pos / 256));
                // Next 8 bytes are choice of value in range 0..255 (like consensus validator index)
                key[8..16].copy_from_slice(&u64::to_be_bytes(pos % 256));
            }
            KeyLayout::ChoiceSequence => {
                // Doing the same as above in different order
                key[..8].copy_from_slice(&u64::to_be_bytes(pos % 256));
                key[8..16].copy_from_slice(&u64::to_be_bytes(pos / 256));
            }
        }
        (key, rng)
    }

    fn key_value(&self, pos: u64) -> (Vec<u8>, Vec<u8>) {
        let (key, mut rng) = self.key_and_rng(pos);
        let mut value = vec![0u8; self.parameters.write_size];
        rng.fill_bytes(&mut value);
        (key, value)
    }

    /// Maps local index into continuous global space
    fn global_pos(&self, pos: usize) -> u64 {
        (pos * self.parameters.write_threads) as u64 + self.index
    }

    fn rng_at(pos: u64) -> StdRng {
        let mut seed = <StdRng as SeedableRng>::Seed::default();
        let mut writer = &mut seed[..];
        writer.put_u64(pos);
        StdRng::from_seed(seed)
    }
}
