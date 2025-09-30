use super::{CellReference, RelocationStrategy};
use crate::metrics::Metrics;
use crate::WalPosition;
use serde::{Deserialize, Serialize};
use std::fs::{rename, File, OpenOptions};
use std::io::{self, Error, Read, Write};
use std::path::{Path, PathBuf};

pub const RELOCATION_FILE: &str = "rel";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexBasedWatermark {
    pub cell_ref: Option<CellReference>, // Current cell position (None = start from beginning)
    pub highest_wal_position: u64,
    pub upper_limit: u64, // WAL position boundary for safe GC
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum WatermarkData {
    WalBased(u64),
    IndexBased(IndexBasedWatermark),
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

    pub fn load(path: &Path) -> Result<Self, Error> {
        let rel_path = Self::relocation_file_path(path);

        let mut file = match File::open(&rel_path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // No existing watermark file, return defaults
                return Ok(Self {
                    path: path.to_path_buf(),
                    relocation_progress: 0,
                    index_progress: IndexBasedWatermark::default(),
                });
            }
            Err(e) => return Err(e),
        };

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let watermark_data = bincode::deserialize::<WatermarkData>(&buffer).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to deserialize watermark: {}", e),
            )
        })?;

        match watermark_data {
            WatermarkData::WalBased(progress) => Ok(Self {
                path: path.to_path_buf(),
                relocation_progress: progress,
                index_progress: IndexBasedWatermark::default(),
            }),
            WatermarkData::IndexBased(progress) => Ok(Self {
                path: path.to_path_buf(),
                relocation_progress: 0,
                index_progress: progress,
            }),
        }
    }

    pub fn save(&self, strategy: RelocationStrategy, metrics: &Metrics) -> Result<(), io::Error> {
        let target_path = Self::relocation_file_path(&self.path);
        let tmp_path = target_path.with_extension("tmp");

        // Create the watermark data based on strategy
        let watermark_data = match strategy {
            RelocationStrategy::WalBased => WatermarkData::WalBased(self.relocation_progress),
            RelocationStrategy::IndexBased => {
                WatermarkData::IndexBased(self.index_progress.clone())
            }
        };

        // Serialize using bincode
        let serialized = bincode::serialize(&watermark_data).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to serialize watermark: {}", e),
            )
        })?;

        // Write atomically
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(&serialized)?;
        file.sync_all()?;
        drop(file);
        rename(&tmp_path, &target_path)?;

        // Update metrics for WAL-based strategy
        if strategy == RelocationStrategy::WalBased {
            metrics
                .relocation_position
                .set(self.relocation_progress as i64);
        }

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
