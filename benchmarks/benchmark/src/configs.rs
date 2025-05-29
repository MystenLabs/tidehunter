use std::io;
use std::path::Path;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Port for Prometheus metrics
pub const METRICS_PORT: u16 = 9092;

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
}

impl FromStr for ReadMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "get" {
            Ok(Self::Get)
        } else if s == "lt" {
            Ok(Self::Lt(1))
        } else if s.starts_with("lt:") {
            Ok(Self::Lt(s[3..].parse().expect("Failed to parse read mode")))
        } else {
            anyhow::bail!(
                "Only allowed choices for read_mode are 'get'(get) or 'lt'(iterator less then)"
            );
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Backend {
    Tidehunter,
    Rocksdb,
}

impl FromStr for Backend {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "thdb" {
            Ok(Self::Tidehunter)
        } else if s == "rocks" {
            Ok(Self::Rocksdb)
        } else {
            anyhow::bail!(
                "Only allowed choices for backend are 'thdb'(Tidehunter) or 'rocks'(RocksDB)"
            );
        }
    }
}
/// The benchmark parameters to configure the stress client
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StressClientParameters {
    /// Number of read threads
    #[serde(default = "defaults::default_read_threads")]
    pub read_threads: usize,
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
    /// The number of blocks to read per thread
    #[serde(default = "defaults::default_reads")]
    pub reads: usize,
    /// Background writes per second during read test
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
}

impl Default for StressClientParameters {
    fn default() -> Self {
        Self {
            read_threads: defaults::default_read_threads(),
            write_threads: defaults::default_write_threads(),
            write_size: defaults::default_write_size(),
            key_len: defaults::default_key_len(),
            writes: defaults::default_writes(),
            reads: defaults::default_reads(),
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
        }
    }
}

/// Default values for the benchmark parameters
pub mod defaults {
    use super::{Backend, KeyLayout, ReadMode};

    pub fn default_read_threads() -> usize {
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

    pub fn default_reads() -> usize {
        1_000_000
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
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct StressTestConfigs {
    pub db_parameters: tidehunter::config::Config,
    pub stress_client_parameters: StressClientParameters,
}

impl StressTestConfigs {
    /// Load the configuration from a YAML file located at the provided path.
    pub fn from_yml<P: AsRef<Path>>(path: P) -> Result<Self, io::Error> {
        let path = path.as_ref();
        let error_message = format!("Unable to load config from {}", path.display());
        let reader = std::fs::File::open(path)?;
        let config = serde_yaml::from_reader(reader)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, error_message))?;
        Ok(config)
    }
}
