use super::{CellReference, RelocationStrategy};
use crate::metrics::Metrics;
use serde::{Deserialize, Serialize};
use std::fs::{rename, File, OpenOptions};
use std::io::{self, Error, Read, Write};
use std::path::{Path, PathBuf};

pub const RELOCATION_FILE: &str = "rel";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum WatermarkData {
    WalBased {
        progress: u64,
    },
    IndexBased {
        cell_ref: Option<CellReference>,
        highest_wal_position: u64,
        upper_limit: u64,
    },
}

impl WatermarkData {
    pub(crate) fn new(strategy: RelocationStrategy) -> Self {
        match strategy {
            RelocationStrategy::WalBased => WatermarkData::WalBased { progress: 0 },
            RelocationStrategy::IndexBased => WatermarkData::IndexBased {
                cell_ref: None,
                highest_wal_position: 0,
                upper_limit: 0,
            },
        }
    }
}

pub struct RelocationWatermarks {
    path: PathBuf,
    pub(crate) data: WatermarkData,
}

impl RelocationWatermarks {
    fn relocation_file_path(path: &Path) -> PathBuf {
        path.join(RELOCATION_FILE)
    }

    pub fn load(path: &Path) -> Result<Option<Self>, Error> {
        let rel_path = Self::relocation_file_path(path);

        let mut file = match File::open(&rel_path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let data = bincode::deserialize::<WatermarkData>(&buffer).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to deserialize watermark: {}", e),
            )
        })?;

        Ok(Some(Self {
            path: path.to_path_buf(),
            data,
        }))
    }

    pub fn new(path: PathBuf, strategy: RelocationStrategy) -> Self {
        Self {
            path,
            data: WatermarkData::new(strategy),
        }
    }

    pub fn save(&self, metrics: &Metrics) -> Result<(), io::Error> {
        let target_path = Self::relocation_file_path(&self.path);
        let tmp_path = target_path.with_extension("tmp");

        // Serialize using bincode
        let serialized = bincode::serialize(&self.data).map_err(|e| {
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

        // Update metrics based on strategy
        match &self.data {
            WatermarkData::WalBased { progress } => {
                metrics.relocation_position.set(*progress as i64);
            }
            WatermarkData::IndexBased { .. } => {
                // TODO: Could add index-specific metrics here if needed
            }
        }

        Ok(())
    }

    pub fn gc_watermark(&self) -> u64 {
        match &self.data {
            WatermarkData::WalBased { progress } => *progress,
            WatermarkData::IndexBased {
                highest_wal_position,
                upper_limit,
                ..
            } => std::cmp::min(*highest_wal_position, *upper_limit),
        }
    }
}
