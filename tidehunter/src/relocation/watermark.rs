use super::CellReference;
use crate::metrics::Metrics;
use crate::WalPosition;
use bytes::Buf;
use serde::{Deserialize, Serialize};
use std::fs::{rename, File, OpenOptions};
use std::io::{self, Error, Read, Write};
use std::path::{Path, PathBuf};

pub const RELOCATION_FILE: &str = "rel";
pub const INDEX_RELOCATION_FILE: &str = "rel_index";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexBasedWatermark {
    pub cell_ref: Option<CellReference>, // Current cell position (None = start from beginning)
    pub highest_wal_position: u64,
    pub upper_limit: u64, // WAL position boundary for safe GC
}

pub struct RelocationWatermarks {
    path: PathBuf,
    /// Watermark that tracks internal relocation progress (for WAL-based strategy)
    relocation_progress: u64,
    /// Watermark that tracks index-based relocation progress
    index_progress: IndexBasedWatermark,
}

impl RelocationWatermarks {
    fn relocation_file_path(path: &Path) -> PathBuf {
        path.join(RELOCATION_FILE)
    }

    fn index_relocation_file_path(path: &Path) -> PathBuf {
        path.join(INDEX_RELOCATION_FILE)
    }

    pub fn load(path: &Path) -> Result<Self, Error> {
        let wal_progress = Self::load_wal_progress(path)?;
        let index_progress = Self::load_index_progress(path)?;

        Ok(Self {
            path: path.to_path_buf(),
            relocation_progress: wal_progress,
            index_progress,
        })
    }

    fn load_wal_progress(path: &Path) -> Result<u64, Error> {
        let mut file = match File::open(Self::relocation_file_path(path)) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Ok(0);
            }
            Err(e) => return Err(e),
        };
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let mut buf = &buf[..];
        Ok(buf.get_u64())
    }

    fn load_index_progress(path: &Path) -> Result<IndexBasedWatermark, Error> {
        let mut file = match File::open(Self::index_relocation_file_path(path)) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Ok(IndexBasedWatermark::default());
            }
            Err(e) => return Err(e),
        };
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        bincode::deserialize(&buffer).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to deserialize index watermark: {}", e),
            )
        })
    }

    pub fn save(&self, metrics: &Metrics) -> Result<(), io::Error> {
        self.save_wal_progress(metrics)?;
        self.save_index_progress(metrics)?;
        Ok(())
    }

    fn save_wal_progress(&self, metrics: &Metrics) -> Result<(), io::Error> {
        let target_path = Self::relocation_file_path(&self.path);
        let tmp_path = target_path.with_extension("tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(&self.relocation_progress.to_be_bytes())?;
        file.sync_all()?;
        drop(file);
        rename(&tmp_path, &target_path)?;
        metrics
            .relocation_position
            .set(self.relocation_progress as i64);
        Ok(())
    }

    fn save_index_progress(&self, _metrics: &Metrics) -> Result<(), io::Error> {
        let target_path = Self::index_relocation_file_path(&self.path);
        let tmp_path = target_path.with_extension("tmp");

        let serialized = bincode::serialize(&self.index_progress).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to serialize index watermark: {}", e),
            )
        })?;

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(&serialized)?;
        file.sync_all()?;
        drop(file);
        rename(&tmp_path, &target_path)?;
        Ok(())
    }

    pub fn set_relocation_progress(&mut self, position: WalPosition) {
        self.relocation_progress = position.offset();
    }

    pub fn get_relocation_progress(&self) -> u64 {
        self.relocation_progress
    }

    pub fn get_index_progress(&self) -> &IndexBasedWatermark {
        &self.index_progress
    }

    pub fn set_index_progress(&mut self, progress: IndexBasedWatermark) {
        self.index_progress = progress;
    }
}
