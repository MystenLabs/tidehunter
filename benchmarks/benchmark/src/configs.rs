use clap::Parser;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeyLayout {
    Uniform,
    SequenceChoice,
    ChoiceSequence,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReadMode {
    Get,
    Lt(usize),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Backend {
    Tidehunter,
    Rocksdb,
}

/// The benchmark parameters to configure the stress client
#[derive(Parser, Debug, Serialize, Deserialize)]
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
    /// Whether to use direct IO
    #[serde(default = "defaults::default_direct_io")]
    pub direct_io: bool,
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
    pub backend: Backend,
}

/// Default values for the benchmark parameters
pub mod defaults {
    use super::{KeyLayout, ReadMode};

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

    pub fn default_direct_io() -> bool {
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
}
