use crate::WalPosition;
use bytes::Buf;
use std::fs::{rename, File, OpenOptions};
use std::io::{self, Error, Read, Write};
use std::path::{Path, PathBuf};

pub const RELOCATION_FILE: &str = "rel";

pub struct RelocationWatermarks {
    path: PathBuf,
    /// Watermark that tracks internal relocation progress
    relocation_progress: u64,
}

impl RelocationWatermarks {
    fn relocation_file_path(path: &Path) -> PathBuf {
        path.join(RELOCATION_FILE)
    }

    pub fn load(path: &Path) -> Result<Self, Error> {
        let mut file = match File::open(Self::relocation_file_path(path)) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Ok(Self {
                    path: path.to_path_buf(),
                    relocation_progress: 0,
                });
            }
            Err(e) => return Err(e),
        };
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let mut buf = &buf[..];
        Ok(Self {
            path: path.to_path_buf(),
            relocation_progress: buf.get_u64(),
        })
    }

    pub fn save(&self) -> Result<(), io::Error> {
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
        Ok(())
    }

    pub fn set_relocation_progress(&mut self, position: WalPosition) {
        self.relocation_progress = position.offset();
    }

    pub fn get_relocation_progress(&self) -> u64 {
        self.relocation_progress
    }
}
