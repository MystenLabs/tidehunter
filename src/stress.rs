use crate::config::Config;
use crate::db::Db;
use crate::key_shape::{KeyShape, KeySpace};
use crate::metrics::Metrics;
use bytes::BufMut;
use clap::Parser;
use histogram::AtomicHistogram;
use parking_lot::RwLock;
use prometheus::Registry;
use rand::rngs::{StdRng, ThreadRng};
use rand::{Rng, RngCore, SeedableRng};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use std::{fs, thread};

macro_rules! report {
    ($report: expr, $($arg:tt)*) => {
        let line = format!($($arg)*);
        println!("{line}");
        $report.lines.push('\n');
        $report.lines.push_str(&line);
    };
}

#[derive(Parser, Debug)]
struct StressArgs {
    #[arg(long, short = 't', help = "Number of write threads")]
    threads: usize,
    #[arg(long, short = 'b', help = "Length of the value")]
    write_size: usize,
    #[arg(long, short = 'k', help = "Length of the key")]
    key_len: usize,
    #[arg(long, short = 'w', help = "Blocks to write per thread")]
    writes: usize,
    #[arg(long, short = 'r', help = "Blocks to read per thread")]
    reads: usize,
    #[arg(
        long,
        short = 'u',
        help = "Background writes per second during read test"
    )]
    background_writes: usize,
    #[arg(
        long,
        short = 'n',
        help = "Disable periodic snapshot",
        default_value = "false"
    )]
    no_snapshot: bool,
    #[arg(long, short = 'p', help = "Path for storage temp dir")]
    path: Option<String>,
    #[arg(long, help = "Print report file", default_value = "false")]
    report: bool,
}

pub fn main() {
    let mut report = Report::default();
    let args = StressArgs::parse();
    let args = Arc::new(args);
    let dir = if let Some(path) = &args.path {
        tempdir::TempDir::new_in(path, "stress").unwrap()
    } else {
        tempdir::TempDir::new("stress").unwrap()
    };
    println!("Path to storage: {}", dir.path().display());
    let print_report = args.report;
    let mut config = Config::default();
    config.max_dirty_keys = 1024;
    let config = Arc::new(config);
    let registry = Registry::new();
    let metrics = Metrics::new_in(&registry);
    crate::prometheus::start_prometheus_server("127.0.0.1:9092".parse().unwrap(), &registry);
    let (key_shape, ks) = KeyShape::new_single(32, 1024, 32);
    let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
    if !args.no_snapshot {
        report!(report, "Periodic snapshot **enabled**");
        db.start_periodic_snapshot();
    } else {
        report!(report, "Periodic snapshot **disabled**");
    }
    let stress = Stress { db, ks, args };
    println!("Starting write test");
    let elapsed = stress.measure(StressThread::run_writes, &mut report);
    let written = stress.args.writes * stress.args.threads;
    let written_bytes = written * stress.args.write_size;
    let msecs = elapsed.as_millis() as usize;
    report!(
        report,
        "Write test done in {elapsed:?}: {} writes/s, {}/sec",
        dec_div(written / msecs * 1000),
        byte_div(written_bytes / msecs * 1000)
    );
    let wal = Db::wal_path(dir.path());
    let wal_len = fs::metadata(wal).unwrap().len();
    report!(
        report,
        "Wal size {:.1} Gb",
        wal_len as f64 / 1024. / 1024. / 1024.
    );
    println!("Starting read test");
    let manual_stop = if stress.args.background_writes > 0 {
        stress.background(StressThread::run_background_writes)
    } else {
        Default::default()
    };
    let elapsed = stress.measure(StressThread::run_reads, &mut report);
    manual_stop.store(true, Ordering::Relaxed);
    let read = stress.args.reads * stress.args.threads;
    let read_bytes = read * stress.args.write_size;
    let msecs = elapsed.as_millis() as usize;
    report!(
        report,
        "Read test done in {elapsed:?}: {} reads/s, {}/sec",
        dec_div(read / msecs * 1000),
        byte_div(read_bytes / msecs * 1000)
    );
    report!(
        report,
        "Max index size {} entries",
        metrics.max_index_size.load(Ordering::Relaxed)
    );
    if print_report {
        println!("Writing report file");
        fs::write("report.txt", &report.lines).unwrap();
    }
}

struct Stress {
    db: Arc<Db>,
    ks: KeySpace,
    args: Arc<StressArgs>,
}

#[derive(Default)]
struct Report {
    lines: String,
}

impl Stress {
    pub fn background<F: FnOnce(StressThread) + Clone + Send + 'static>(
        &self,
        f: F,
    ) -> Arc<AtomicBool> {
        let (_, manual_stop, _) = self.start_threads(f);
        manual_stop
    }

    pub fn measure<F: FnOnce(StressThread) + Clone + Send + 'static>(
        &self,
        f: F,
        report: &mut Report,
    ) -> Duration {
        let (threads, _, latency) = self.start_threads(f);
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
        report!(report, "Latency(mcs): p50: {:?}, p90: {:?}, p99: {:?}, p99.9: {:?}, p99.99: {:?}, p99.999: {:?}",
        p(0), p(1), p(2), p(3), p(4), p(5));
        start.elapsed()
    }

    fn start_threads<F: FnOnce(StressThread) + Clone + Send + 'static>(
        &self,
        f: F,
    ) -> (Vec<JoinHandle<()>>, Arc<AtomicBool>, Arc<AtomicHistogram>) {
        let mut threads = Vec::with_capacity(self.args.threads);
        let start_lock = Arc::new(RwLock::new(()));
        let start_w = start_lock.write();
        let manual_stop = Arc::new(AtomicBool::new(false));
        let latency = AtomicHistogram::new(12, 20).unwrap();
        let latency = Arc::new(latency);
        for index in 0..self.args.threads {
            let thread = StressThread {
                ks: self.ks,
                db: self.db.clone(),
                start_lock: start_lock.clone(),
                args: self.args.clone(),
                index: index as u64,
                manual_stop: manual_stop.clone(),

                latency: latency.clone(),
            };
            let f = f.clone();
            let thread = thread::spawn(move || f(thread));
            threads.push(thread);
        }
        drop(start_w);
        (threads, manual_stop, latency)
    }
}

fn dec_div(n: usize) -> String {
    const M: usize = 1_000_000;
    const K: usize = 1_000;
    if n > M {
        format!("{}M", n / M)
    } else if n > K {
        format!("{}K", n / K)
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
    db: Arc<Db>,
    ks: KeySpace,
    start_lock: Arc<RwLock<()>>,
    args: Arc<StressArgs>,
    index: u64,
    manual_stop: Arc<AtomicBool>,

    latency: Arc<AtomicHistogram>,
}

impl StressThread {
    pub fn run_writes(self) {
        let _ = self.start_lock.read();
        for pos in 0..self.args.writes {
            let (key, value) = self.key_value(pos);
            let timer = Instant::now();
            self.db.insert(self.ks, key, value).unwrap();
            self.latency
                .increment(timer.elapsed().as_micros() as u64)
                .unwrap();
        }
    }

    pub fn run_background_writes(self) {
        let writes_per_thread = self.args.background_writes / self.args.threads;
        let delay = Duration::from_micros(1_000_000 / writes_per_thread as u64);
        let mut deadline = Instant::now();
        let mut pos = usize::MAX;
        while !self.manual_stop.load(Ordering::Relaxed) {
            deadline = deadline + delay;
            pos -= 1;
            let (key, value) = self.key_value(pos);
            self.db.insert(self.ks, key, value).unwrap();
            thread::sleep(
                deadline
                    .checked_duration_since(Instant::now())
                    .unwrap_or_default(),
            )
        }
    }

    pub fn run_reads(self) {
        let _ = self.start_lock.read();
        let mut pos_rng = ThreadRng::default();
        for _ in 0..self.args.reads {
            let pos = pos_rng.gen_range(0..self.args.writes);
            let (key, value) = self.key_value(pos);
            let timer = Instant::now();
            let found_value = self
                .db
                .get(self.ks, &key)
                .unwrap()
                .expect("Expected value not found");
            self.latency
                .increment(timer.elapsed().as_micros() as u64)
                .unwrap();
            assert_eq!(
                &value[..],
                &found_value[..],
                "Found value does not match expected value"
            );
        }
    }

    fn key_value(&self, pos: usize) -> (Vec<u8>, Vec<u8>) {
        let mut value = vec![0u8; self.args.write_size];
        let mut key = vec![0u8; self.args.key_len];
        let mut rng = self.rng_at(pos as u64);
        rng.fill_bytes(&mut key);
        rng.fill_bytes(&mut value);
        (key, value)
    }

    fn rng_at(&self, pos: u64) -> StdRng {
        let mut seed = <StdRng as SeedableRng>::Seed::default();
        let mut writer = &mut seed[..];
        writer.put_u64(self.index);
        writer.put_u64(pos);
        StdRng::from_seed(seed)
    }
}
