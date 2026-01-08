use std::io;
use std::path::Path;
use std::str::FromStr;

use clap::{Parser, arg};
use serde::{Deserialize, Serialize};
use tidehunter::RelocationStrategy;

/// Port for Prometheus metrics
pub const METRICS_PORT: u16 = 9092;

/// Benchmark-level relocation configuration that stores user intent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RelocationConfig {
    Wal,
    Index { ratio: Option<f64> },
}

/// Helper to parse RelocationConfig from string
/// Accepts "wal", "index", or "index:0.5" (with ratio)
fn parse_relocation_config(s: &str) -> Result<RelocationConfig, anyhow::Error> {
    if s == "wal" {
        Ok(RelocationConfig::Wal)
    } else if s == "index" {
        Ok(RelocationConfig::Index { ratio: None })
    } else if let Some(ratio_str) = s.strip_prefix("index:") {
        let ratio: f64 = ratio_str
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid ratio format: must be a number"))?;
        if !(0.0..=1.0).contains(&ratio) {
            anyhow::bail!("Ratio must be between 0.0 and 1.0, got {}", ratio);
        }
        Ok(RelocationConfig::Index { ratio: Some(ratio) })
    } else {
        anyhow::bail!(
            "Invalid relocation strategy: use 'wal', 'index', or 'index:<ratio>' (e.g., 'index:0.5')"
        );
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeyLayout {
    Uniform,
    SequenceChoice,
    ChoiceSequence,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReadMode {
    Get,
    Lt(usize),
    Exists,
}

impl FromStr for ReadMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "get" {
            Ok(Self::Get)
        } else if s == "lt" {
            Ok(Self::Lt(1))
        } else if let Some(stripped) = s.strip_prefix("lt:") {
            Ok(Self::Lt(
                stripped.parse().expect("Failed to parse read mode"),
            ))
        } else if s == "exists" {
            Ok(Self::Exists)
        } else {
            anyhow::bail!(
                "Only allowed choices for read_mode are 'get'(get), 'lt'(iterator less then), or 'exists'(exists check)"
            );
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Backend {
    Tidehunter,
    Rocksdb,
    Blobdb,
}

impl FromStr for Backend {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "thdb" {
            Ok(Self::Tidehunter)
        } else if s == "rocks" {
            Ok(Self::Rocksdb)
        } else if s == "blobdb" {
            Ok(Self::Blobdb)
        } else {
            anyhow::bail!(
                "Only allowed choices for backend are 'thdb'(Tidehunter), 'rocks'(RocksDB), or 'blobdb'(RocksDB BlobDB)"
            );
        }
    }
}
/// The benchmark parameters to configure the stress client
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StressClientParameters {
    /// Number of mixed read/write threads
    #[serde(default = "defaults::default_mixed_threads")]
    pub mixed_threads: usize,
    /// Number of write threads
    #[serde(default = "defaults::default_write_threads")]
    pub write_threads: usize,
    /// Length of the values
    #[serde(default = "defaults::default_write_size")]
    pub write_size: usize,
    /// Length of the keys
    #[serde(default = "defaults::default_key_len")]
    pub key_len: usize,
    /// The number of blocks to write per thread
    #[serde(default = "defaults::default_writes")]
    pub writes: usize,
    /// Duration of the mixed read/write phase in seconds
    #[serde(default = "defaults::default_mixed_duration_secs")]
    pub mixed_duration_secs: u64,
    /// Pause between benchmark phases in seconds (0 = no pause)
    #[serde(default = "defaults::default_pause_between_phases_secs")]
    pub pause_between_phases_secs: u64,
    /// Background writes per second during mixed test
    #[serde(default = "defaults::default_background_writes")]
    pub background_writes: usize,
    /// Whether to disable periodic snapshots
    #[serde(default = "defaults::default_no_snapshot")]
    pub no_snapshot: bool,
    /// Path of the storage temp dir. Will generate a temp file if not specified.
    pub path: Option<String>,
    /// Whether to print the report file
    #[serde(default = "defaults::default_report")]
    pub report: bool,
    /// The key layout
    #[serde(default = "defaults::default_key_layout")]
    pub key_layout: KeyLayout,
    /// Whether to print the tldr report"
    #[serde(default = "defaults::default_tldr")]
    pub tldr: String,
    /// Whether to preserve the generated directory
    #[serde(default = "defaults::default_preserve")]
    pub preserve: bool,
    /// Use pre-generated DB
    pub reuse: Option<String>,
    /// The read mode
    #[serde(default = "defaults::default_read_mode")]
    pub read_mode: ReadMode,
    /// The backend DB
    #[serde(default = "defaults::default_backend")]
    pub backend: Backend,
    /// Percentage of reads in the mixed read/write phase (0-100)
    #[serde(default = "defaults::default_read_percentage")]
    pub read_percentage: u8,
    /// The zipf exponent for reader position selection. 0 means uniform.
    #[serde(default = "defaults::default_zipf_exponent")]
    pub zipf_exponent: f64,
    /// Relocation configuration. None means disabled, Some(config) enables continuous relocation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relocation: Option<RelocationConfig>,
    /// Ratio of writes that overwrite existing keys (0.0 to 1.0, default 0.0)
    #[serde(default = "defaults::default_overwrite_ratio")]
    pub overwrite_ratio: f64,
}

impl Default for StressClientParameters {
    fn default() -> Self {
        Self {
            mixed_threads: defaults::default_mixed_threads(),
            write_threads: defaults::default_write_threads(),
            write_size: defaults::default_write_size(),
            key_len: defaults::default_key_len(),
            writes: defaults::default_writes(),
            mixed_duration_secs: defaults::default_mixed_duration_secs(),
            pause_between_phases_secs: defaults::default_pause_between_phases_secs(),
            background_writes: defaults::default_background_writes(),
            no_snapshot: defaults::default_no_snapshot(),
            path: None,
            report: defaults::default_report(),
            key_layout: defaults::default_key_layout(),
            tldr: defaults::default_tldr(),
            preserve: defaults::default_preserve(),
            reuse: None,
            read_mode: defaults::default_read_mode(),
            backend: defaults::default_backend(),
            read_percentage: defaults::default_read_percentage(),
            zipf_exponent: defaults::default_zipf_exponent(),
            relocation: None,
            overwrite_ratio: defaults::default_overwrite_ratio(),
        }
    }
}

/// Default values for the benchmark parameters
pub mod defaults {
    use super::{Backend, KeyLayout, ReadMode};

    pub fn default_mixed_threads() -> usize {
        1
    }

    pub fn default_write_threads() -> usize {
        1
    }

    pub fn default_write_size() -> usize {
        1024
    }

    pub fn default_key_len() -> usize {
        32
    }

    pub fn default_writes() -> usize {
        1_000_000
    }

    pub fn default_mixed_duration_secs() -> u64 {
        600
    }

    pub fn default_pause_between_phases_secs() -> u64 {
        600
    }

    pub fn default_background_writes() -> usize {
        0
    }

    pub fn default_no_snapshot() -> bool {
        false
    }

    pub fn default_report() -> bool {
        false
    }

    pub fn default_key_layout() -> KeyLayout {
        KeyLayout::Uniform
    }

    pub fn default_tldr() -> String {
        "".to_string()
    }

    pub fn default_preserve() -> bool {
        false
    }

    pub fn default_read_mode() -> ReadMode {
        ReadMode::Get
    }

    pub fn default_backend() -> Backend {
        Backend::Tidehunter
    }

    pub fn default_read_percentage() -> u8 {
        100
    }

    pub fn default_zipf_exponent() -> f64 {
        0.0
    }

    pub fn default_overwrite_ratio() -> f64 {
        0.0
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct StressTestConfigs {
    pub db_parameters: tidehunter::config::Config,
    pub stress_client_parameters: StressClientParameters,
}

impl StressTestConfigs {
    /// Load the configuration from a YAML file located at the provided path.
    pub fn from_yml<P: AsRef<Path>>(path: P) -> Result<Self, io::Error> {
        let path = path.as_ref();
        let error_message = format!("Unable to load config from {}", path.display());
        let reader = std::fs::File::open(path)
            .map_err(|_| io::Error::new(io::ErrorKind::NotFound, error_message.clone()))?;
        let config = serde_yaml::from_reader(reader)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, error_message))?;
        Ok(config)
    }
}

impl Default for StressTestConfigs {
    fn default() -> Self {
        // This overwrites tidehunter defaults with more reasonable values for benchmark
        let db_parameters = tidehunter::config::Config {
            // Allocate 100Gb space for map cache
            frag_size: 1024 * 1024 * 1024,
            max_maps: 100,
            // Default to 8 flusher threads
            num_flusher_threads: 8,
            ..Default::default()
        };
        let stress_client_parameters = StressClientParameters::default();
        Self {
            db_parameters,
            stress_client_parameters,
        }
    }
}

#[derive(Parser, Debug)]
pub struct StressArgs {
    // Allows to call the benchmark using parameters specified in a file. Even if the user specifies a file,
    // the command line arguments will override the values in the file. Defaults apply otherwise.
    #[arg(
        long,
        help = "Path to the default parameters file. Any value can be overridden by command line arguments"
    )]
    pub parameters_path: Option<String>,

    #[arg(long, help = "Number of mixed read/write threads")]
    mixed_threads: Option<usize>,
    #[arg(long, help = "Number of write threads")]
    write_threads: Option<usize>,
    #[arg(long, short = 'v', help = "Length of the value")]
    write_size: Option<usize>,
    #[arg(long, short = 'k', help = "Length of the key")]
    key_len: Option<usize>,
    #[arg(long, short = 'w', help = "Blocks to write per thread")]
    writes: Option<usize>,
    #[arg(long, help = "Duration of mixed phase in seconds")]
    mixed_duration_secs: Option<u64>,
    #[arg(long, help = "Pause between benchmark phases in seconds")]
    pause_between_phases_secs: Option<u64>,
    #[arg(long, short = 'u', help = "Background writes/s during mixed test")]
    background_writes: Option<usize>,
    #[arg(long, short = 'n', help = "Disable periodic snapshot")]
    no_snapshot: Option<bool>,
    #[arg(long, help = "Use direct IO")]
    direct_io: Option<bool>,
    #[arg(
        long,
        help = "Enable metrics across all backends (Tidehunter, RocksDB, BlobDB)"
    )]
    metrics_enabled: Option<bool>,
    #[arg(long, short = 'p', help = "Path for storage temp dir")]
    path: Option<String>,
    #[arg(long, help = "Print report file")]
    report: Option<bool>,
    #[arg(long, help = "Key layout")]
    key_layout: Option<KeyLayout>,
    #[arg(long, help = "Print tldr report")]
    tldr: Option<String>,
    #[arg(long, help = "Preserve generated directory")]
    preserve: Option<bool>,
    #[arg(long, help = "Use pre-generated DB")]
    reuse: Option<String>,
    #[arg(long, help = "Read mode")]
    read_mode: Option<ReadMode>,
    #[arg(long, short = 'b', help = "Backend")]
    backend: Option<Backend>,
    #[arg(long, help = "Percentage of reads in mixed phase (0-100)")]
    read_percentage: Option<u8>,
    #[arg(
        long,
        help = "The zipf exponent for reader position selection. 0 means uniform."
    )]
    zipf_exponent: Option<f64>,
    #[arg(
        long,
        help = "Relocation strategy (wal or index). Enables continuous relocation"
    )]
    relocation: Option<String>,
    #[arg(
        long,
        help = "Ratio of writes that overwrite existing keys (0.0 to 1.0)"
    )]
    overwrite_ratio: Option<f64>,
}

/// Override default arguments with the ones provided by the user
pub fn override_default_args(args: StressArgs, mut config: StressTestConfigs) -> StressTestConfigs {
    if let Some(mixed_threads) = args.mixed_threads {
        config.stress_client_parameters.mixed_threads = mixed_threads;
    }
    if let Some(write_threads) = args.write_threads {
        config.stress_client_parameters.write_threads = write_threads;
    }
    if let Some(write_size) = args.write_size {
        config.stress_client_parameters.write_size = write_size;
    }
    if let Some(key_len) = args.key_len {
        config.stress_client_parameters.key_len = key_len;
    }
    if let Some(writes) = args.writes {
        config.stress_client_parameters.writes = writes;
    }
    if let Some(mixed_duration_secs) = args.mixed_duration_secs {
        config.stress_client_parameters.mixed_duration_secs = mixed_duration_secs;
    }
    if let Some(pause_between_phases_secs) = args.pause_between_phases_secs {
        config.stress_client_parameters.pause_between_phases_secs = pause_between_phases_secs;
    }
    if let Some(background_writes) = args.background_writes {
        config.stress_client_parameters.background_writes = background_writes;
    }
    if let Some(no_snapshot) = args.no_snapshot {
        config.stress_client_parameters.no_snapshot = no_snapshot;
    }
    if let Some(direct_io) = args.direct_io {
        config.db_parameters.direct_io = direct_io;
    }
    if let Some(metrics_enabled) = args.metrics_enabled {
        config.db_parameters.metrics_enabled = metrics_enabled;
    }
    if let Some(path) = args.path {
        config.stress_client_parameters.path = Some(path);
    }
    if let Some(report) = args.report {
        config.stress_client_parameters.report = report;
    }
    if let Some(key_layout) = args.key_layout {
        config.stress_client_parameters.key_layout = key_layout;
    }
    if let Some(tldr) = args.tldr {
        config.stress_client_parameters.tldr = tldr;
    }
    if let Some(preserve) = args.preserve {
        config.stress_client_parameters.preserve = preserve;
    }
    if let Some(reuse) = args.reuse {
        config.stress_client_parameters.reuse = Some(reuse);
    }
    if let Some(read_mode) = args.read_mode {
        config.stress_client_parameters.read_mode = read_mode;
    }
    if let Some(backend) = args.backend {
        config.stress_client_parameters.backend = backend;
    }
    if let Some(read_percentage) = args.read_percentage {
        config.stress_client_parameters.read_percentage = read_percentage;
    }
    if let Some(zipf_exponent) = args.zipf_exponent {
        config.stress_client_parameters.zipf_exponent = zipf_exponent;
    }
    if let Some(relocation_str) = args.relocation {
        match parse_relocation_config(&relocation_str) {
            Ok(relocation_config) => {
                config.stress_client_parameters.relocation = Some(relocation_config.clone());
                // Set the base strategy in db_parameters for tidehunter
                // The actual target position will be computed dynamically in the benchmark
                config.db_parameters.relocation_strategy = match relocation_config {
                    RelocationConfig::Wal => RelocationStrategy::WalBased,
                    RelocationConfig::Index { .. } => RelocationStrategy::IndexBased(None),
                };
            }
            Err(e) => {
                eprintln!("Error parsing relocation config: {e}");
                std::process::exit(1);
            }
        }
    }
    if let Some(overwrite_ratio) = args.overwrite_ratio {
        if !(0.0..=1.0).contains(&overwrite_ratio) {
            eprintln!("Error: overwrite_ratio must be between 0.0 and 1.0");
            std::process::exit(1);
        }
        config.stress_client_parameters.overwrite_ratio = overwrite_ratio;
    }

    config
}
