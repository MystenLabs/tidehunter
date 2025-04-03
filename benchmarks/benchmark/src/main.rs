use crate::storage::Storage;
use bytes::BufMut;
use clap::Parser;
use histogram::AtomicHistogram;
use parking_lot::RwLock;
use rand::rngs::{StdRng, ThreadRng};
use rand::{Rng, RngCore, SeedableRng};
use std::fs::OpenOptions;
use std::io::Write;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};
use std::{fs, thread};
#[cfg(not(feature = "rocks"))]
use tidehunter::config::Config;
#[cfg(not(feature = "rocks"))]
use tidehunter::db::Db;
#[cfg(not(feature = "rocks"))]
use tidehunter::key_shape::{KeyShape, KeyType};

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
    #[arg(long, help = "Use direct IO", default_value = "false")]
    direct_io: bool,
    #[arg(long, short = 'p', help = "Path for storage temp dir")]
    path: Option<String>,
    #[arg(long, help = "Print report file", default_value = "false")]
    report: bool,
    #[arg(long, help = "Key layout", default_value = "u")]
    key_layout: KeyLayout,
    #[arg(long, help = "Print tldr report", default_value = "")]
    tldr: String,
}

#[derive(Debug, Clone)]
enum KeyLayout {
    Uniform,
    SequenceChoice,
    ChoiceSequence,
}

pub fn main() {
    let start_time = SystemTime::now();
    let mut report = Report::default();
    let args: StressArgs = StressArgs::parse();
    let args = Arc::new(args);
    let dir = if let Some(path) = &args.path {
        tempdir::TempDir::new_in(path, "stress").unwrap()
    } else {
        tempdir::TempDir::new("stress").unwrap()
    };
    println!("Path to storage: {}", dir.path().display());
    println!("Using {:?} key layout", args.key_layout);
    let print_report = args.report;
    #[cfg(not(feature = "rocks"))]
    let storage = {
        let mut config = Config::default();
        config.max_dirty_keys = 1024;
        config.direct_io = args.direct_io;
        config.frag_size = 1024 * 1024 * 1024;
        config.max_maps = 32;
        config.snapshot_unload_threshold = 128 * 1024 * 1024 * 1024;
        config.snapshot_written_bytes = 64 * 1024 * 1024 * 1024;
        config.unload_jitter_pct = 30;
        if args.direct_io {
            report!(report, "Using **direct IO**");
        }
        use crate::storage::tidehunter::TidehunterStorage;
        let (key_shape, ks) = match args.key_layout {
            KeyLayout::Uniform => KeyShape::new_single(32, 1024, KeyType::uniform(32)),
            KeyLayout::SequenceChoice => {
                let key_type = KeyType::prefix_uniform(7, 0);
                KeyShape::new_single(32, 1024, key_type)
            }
        };
        let storage = TidehunterStorage::open(config, dir.path(), (key_shape, ks));
        if !args.no_snapshot {
            report!(report, "Periodic snapshot **enabled**");
            storage.db.start_periodic_snapshot();
        } else {
            report!(report, "Periodic snapshot **disabled**");
        }
        storage
    };
    #[cfg(feature = "rocks")]
    let storage = {
        use crate::storage::rocks::RocksStorage;
        RocksStorage::open(dir.path())
    };
    let stress = Stress { storage, args };
    println!("Starting write test");
    let elapsed = stress.measure(StressThread::run_writes, &mut report);
    let written = stress.args.writes * stress.args.threads;
    let written_bytes = written * stress.args.write_size;
    let msecs = elapsed.as_millis() as usize;
    let write_sec = dec_div(written / msecs * 1000);
    report!(
        report,
        "Write test done in {elapsed:?}: {} writes/s, {}/sec",
        write_sec,
        byte_div(written_bytes / msecs * 1000)
    );
    #[cfg(not(feature = "rocks"))]
    {
        let wal = Db::wal_path(dir.path());
        let wal_len = fs::metadata(wal).unwrap().len();
        report!(
            report,
            "Wal size {:.1} Gb",
            wal_len as f64 / 1024. / 1024. / 1024.
        );
    }
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
    let read_sec = dec_div(read / msecs * 1000);
    report!(
        report,
        "Read test done in {elapsed:?}: {} reads/s, {}/sec",
        read_sec,
        byte_div(read_bytes / msecs * 1000)
    );
    #[cfg(not(feature = "rocks"))]
    report!(
        report,
        "Max index size {} entries",
        stress
            .storage
            .metrics
            .max_index_size
            .load(Ordering::Relaxed)
    );
    if print_report {
        println!("Writing report file");
        fs::write("report.txt", &report.lines).unwrap();
    }
    if !stress.args.tldr.is_empty() {
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open("tldr.txt")
            .unwrap();
        let start_time = start_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let end_time = SystemTime::now();
        let end_time = end_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        writeln!(
            file,
            "{: <12}|{: <12}|{: <24}|{: <8}|{: <8}",
            start_time, end_time, stress.args.tldr, write_sec, read_sec
        )
        .unwrap();
    }
}

struct Stress<T> {
    storage: T,
    args: Arc<StressArgs>,
}

#[derive(Default)]
struct Report {
    lines: String,
}

impl<T: Storage> Stress<T> {
    pub fn background<F: FnOnce(StressThread<T>) + Clone + Send + 'static>(
        &self,
        f: F,
    ) -> Arc<AtomicBool> {
        let (_, manual_stop, _, _) = self.start_threads(f);
        manual_stop
    }

    pub fn measure<F: FnOnce(StressThread<T>) + Clone + Send + 'static>(
        &self,
        f: F,
        report: &mut Report,
    ) -> Duration {
        let (threads, _, latency, latency_errors) = self.start_threads(f);
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

    fn start_threads<F: FnOnce(StressThread<T>) + Clone + Send + 'static>(
        &self,
        f: F,
    ) -> (
        Vec<JoinHandle<()>>,
        Arc<AtomicBool>,
        Arc<AtomicHistogram>,
        Arc<AtomicUsize>,
    ) {
        let mut threads = Vec::with_capacity(self.args.threads);
        let start_lock = Arc::new(RwLock::new(()));
        let start_w = start_lock.write();
        let manual_stop = Arc::new(AtomicBool::new(false));
        let latency = AtomicHistogram::new(12, 26).unwrap();
        let latency = Arc::new(latency);
        let latency_errors = Arc::new(AtomicUsize::default());
        for index in 0..self.args.threads {
            let thread = StressThread {
                db: self.storage.clone(),
                start_lock: start_lock.clone(),
                args: self.args.clone(),
                index: index as u64,
                manual_stop: manual_stop.clone(),

                latency: latency.clone(),
                latency_errors: latency_errors.clone(),
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

struct StressThread<T> {
    db: T,
    start_lock: Arc<RwLock<()>>,
    args: Arc<StressArgs>,
    index: u64,
    manual_stop: Arc<AtomicBool>,

    latency: Arc<AtomicHistogram>,
    latency_errors: Arc<AtomicUsize>,
}

impl<T: Storage> StressThread<T> {
    pub fn run_writes(self) {
        let _ = self.start_lock.read();
        for pos in 0..self.args.writes {
            let (key, value) = self.key_value(pos);
            let timer = Instant::now();
            self.db.insert(key.into(), value.into());
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
            self.db.insert(key.into(), value.into());
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
            let found_value = self.db.get(&key).expect("Expected value not found");
            if self
                .latency
                .increment(timer.elapsed().as_micros() as u64)
                .is_err()
            {
                self.latency_errors.fetch_add(1, Ordering::Relaxed);
            }
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
        match self.args.key_layout {
            KeyLayout::Uniform => {}
            KeyLayout::SequenceChoice => {
                let global_pos = self.global_pos(pos) as u64;
                // the first 16 bytes of a key are not random anymore
                // First 8 bytes are a sequentially growing value (like consensus round)
                key[..8].copy_from_slice(&u64::to_be_bytes(global_pos / 256));
                // Next 8 bytes are choice of value in range 0..255 (like consensus validator index)
                key[8..16].copy_from_slice(&u64::to_be_bytes(global_pos % 256));
            }
            KeyLayout::ChoiceSequence => {
                let global_pos = self.global_pos(pos) as u64;
                // Doing the same as above in different order
                key[..8].copy_from_slice(&u64::to_be_bytes(global_pos % 256));
                key[8..16].copy_from_slice(&u64::to_be_bytes(global_pos / 256));
            }
        }
        rng.fill_bytes(&mut value);
        (key, value)
    }

    /// Maps local index into continuous global space
    fn global_pos(&self, pos: usize) -> usize {
        pos * self.args.threads + self.index as usize
    }

    fn rng_at(&self, pos: u64) -> StdRng {
        let mut seed = <StdRng as SeedableRng>::Seed::default();
        let mut writer = &mut seed[..];
        writer.put_u64(self.index);
        writer.put_u64(pos);
        StdRng::from_seed(seed)
    }
}

impl FromStr for KeyLayout {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "u" {
            Ok(Self::Uniform)
        } else if s == "sc" {
            Ok(Self::SequenceChoice)
        } else if s == "cs" {
            Ok(Self::ChoiceSequence)
        } else {
            anyhow::bail!(
                "Only allowed choices for key_layout are 'u'(uniform) or 'sc'(sequence-choice) or 'cs'(choice-sequence)"
            );
        }
    }
}
