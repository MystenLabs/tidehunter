use crate::db::Db;
use crate::relocation::util::truncate_file;
use crate::wal::WalLayout;
use bytes::Buf;
use std::fs::{rename, File, OpenOptions};
use std::io::{self, Error, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub const RELOCATION_FILE: &str = "rel";

pub struct RelocationWatermarks {
    path: PathBuf,
    offset: Arc<AtomicU64>,
    relocation_progress: u64,
    // optional size of the WAL file, only used by the recovery process
    size: Option<u64>,
}

impl RelocationWatermarks {
    fn relocation_file_path(path: &Path) -> PathBuf {
        path.join(RELOCATION_FILE)
    }

    pub fn load(path: &Path, upper_pruning_limit: u64, layout: WalLayout) -> Result<Self, Error> {
        let wal_path = Db::wal_path(path);
        // check for potential recovery
        let tmp_watermarks_path = Self::relocation_file_path(path).with_extension("tmp");
        if tmp_watermarks_path.exists() {
            let mut buf = [0u8; 24];
            File::open(&tmp_watermarks_path)?.read_exact(&mut buf)?;
            let mut buf = &buf[16..];
            let previous_size = buf.get_u64();
            if previous_size > std::fs::metadata(&wal_path)?.len() {
                rename(&tmp_watermarks_path, Self::relocation_file_path(path))?;
            }
        }
        let mut file = match File::open(Self::relocation_file_path(path)) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Ok(Self {
                    path: path.to_path_buf(),
                    offset: Arc::new(AtomicU64::new(0)),
                    relocation_progress: 0,
                    size: None,
                });
            }
            Err(e) => return Err(e),
        };
        let mut buf = [0u8; 16];
        file.read_exact(&mut buf)?;
        let mut buf = &buf[..];
        let offset = buf.get_u64();
        let relocation_progress = buf.get_u64();
        let mut target_offset = std::cmp::min(relocation_progress, upper_pruning_limit);
        target_offset = (target_offset / layout.frag_size) * layout.frag_size;

        if offset == target_offset {
            return Ok(Self {
                path: path.to_path_buf(),
                offset: Arc::new(AtomicU64::new(offset)),
                relocation_progress,
                size: None,
            });
        }
        let watermarks = Self {
            path: path.to_path_buf(),
            offset: Arc::new(AtomicU64::new(target_offset)),
            relocation_progress,
            size: Some(std::fs::metadata(&wal_path)?.len()),
        };
        watermarks.save(move || truncate_file(&wal_path, target_offset - offset))?;
        Ok(watermarks)
    }

    pub fn save<F: Fn() -> Result<(), io::Error>>(&self, callback: F) -> Result<(), io::Error> {
        let target_path = Self::relocation_file_path(&self.path);
        let tmp_path = target_path.with_extension("tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(&self.offset.load(Ordering::Relaxed).to_be_bytes())?;
        file.write_all(&self.relocation_progress.to_be_bytes())?;
        file.write_all(&self.size.unwrap_or_default().to_be_bytes())?;
        file.sync_all()?;
        drop(file);
        callback()?;
        rename(&tmp_path, &target_path)?;
        Ok(())
    }

    pub fn set_relocation_progress(&mut self, offset: u64) {
        self.relocation_progress = offset;
    }

    #[cfg(test)]
    pub fn set_global_offset(&mut self, offset: u64) {
        self.offset.store(offset, Ordering::Relaxed);
    }

    pub fn get_relocation_progress(&self) -> u64 {
        self.relocation_progress
    }

    pub fn get_offset(&self) -> Arc<AtomicU64> {
        self.offset.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_save_load_relocation_watermarks() {
        let dir = tempdir::TempDir::new("test-watermark-recovery").unwrap();
        let wal_path = Db::wal_path(dir.path());
        let wal_layout = WalLayout {
            frag_size: 512,
            max_maps: 2,
            direct_io: false,
        };
        let mut watermarks =
            RelocationWatermarks::load(&dir.path(), u64::MAX, wal_layout.clone()).unwrap();
        assert_eq!(watermarks.relocation_progress, 0);
        assert_eq!(watermarks.offset.load(Ordering::Relaxed), 0);
        assert_eq!(watermarks.size, None);

        let block_size = 4096;
        let wal_size = 10 * block_size;
        std::fs::write(&wal_path, vec![1u8; wal_size as usize]).unwrap();
        watermarks.set_relocation_progress(block_size);
        watermarks.save(|| Ok(())).unwrap();

        // normal restart triggers truncation
        let mut watermarks =
            RelocationWatermarks::load(&dir.path(), u64::MAX, wal_layout.clone()).unwrap();
        assert_eq!(
            std::fs::metadata(&wal_path).unwrap().len(),
            wal_size - block_size
        );

        // verify recovery: crash after file is truncated
        watermarks.set_relocation_progress(2 * block_size);
        watermarks.set_global_offset(2 * block_size);
        let _ = watermarks.save(|| {
            truncate_file(&wal_path, block_size).unwrap();
            Err(io::Error::new(io::ErrorKind::Other, "failure"))
        });
        let mut watermarks =
            RelocationWatermarks::load(&dir.path(), u64::MAX, wal_layout.clone()).unwrap();
        assert_eq!(watermarks.relocation_progress, 2 * block_size);
        assert_eq!(watermarks.offset.load(Ordering::Relaxed), 2 * block_size);
        assert_eq!(
            std::fs::metadata(&wal_path).unwrap().len(),
            wal_size - 2 * block_size,
        );

        // verify recovery: crash before file is truncated
        watermarks.set_relocation_progress(3 * block_size);
        let _ = watermarks.save(|| Err(io::Error::new(io::ErrorKind::Other, "failure")));
        let watermarks = RelocationWatermarks::load(&dir.path(), u64::MAX, wal_layout).unwrap();
        assert_eq!(watermarks.relocation_progress, 2 * block_size);
        assert_eq!(watermarks.offset.load(Ordering::Relaxed), 2 * block_size);
    }
}
