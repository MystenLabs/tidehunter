use super::Wal;
#[cfg(any(test, feature = "test-utils"))]
use super::layout::WalKind;
use super::layout::WalLayout;
use super::position::WalFileId;
use arc_swap::ArcSwap;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) struct WalFiles {
    pub(crate) base_path: PathBuf,
    pub(crate) files: Vec<Arc<File>>,
    pub(crate) min_file_id: WalFileId,
}

impl WalFiles {
    pub(crate) fn new(base_path: &Path, layout: &WalLayout) -> io::Result<Arc<ArcSwap<Self>>> {
        let wal_files = Self::load(base_path, layout)?;
        Ok(Arc::new(ArcSwap::from(Arc::new(wal_files))))
    }

    pub(crate) fn load(base_path: &Path, layout: &WalLayout) -> io::Result<Self> {
        let mut files = vec![];
        for entry in std::fs::read_dir(base_path)? {
            let file_path = entry?.path();
            if file_path.is_file()
                && let Some(file_name) = file_path.file_name().and_then(|name| name.to_str())
                && let Some(id_str) = file_name.strip_prefix(layout.kind.name())
            {
                let Some(id_str) = id_str.strip_prefix("_") else {
                    panic!("invalid wal file name {file_name:?}(failed to strip _ prefix)");
                };
                let id = u64::from_str_radix(id_str, 16)
                    .unwrap_or_else(|_| panic!("invalid wal file name {file_name:?}"));
                let file = Wal::open_file(&file_path, layout)?;
                files.push((id, file));
            }
        }
        if files.is_empty() {
            let file = Wal::open_file(&layout.wal_file_name(base_path, WalFileId(0)), layout)?;
            files.push((0, file));
        }
        files.sort_by_key(|(id, _)| *id);
        let min_file_id = files[0].0;
        assert_eq!(
            files[files.len() - 1].0 - min_file_id + 1,
            files.len() as u64,
            "WAL file IDs must form a contiguous range",
        );
        Ok(Self {
            base_path: base_path.to_path_buf(),
            files: files.into_iter().map(|(_, file)| Arc::new(file)).collect(),
            min_file_id: WalFileId(min_file_id),
        })
    }

    pub(crate) fn current_file_id(&self) -> WalFileId {
        WalFileId(self.min_file_id.0 + self.files.len() as u64 - 1)
    }

    #[inline]
    pub(crate) fn current_file(&self) -> &Arc<File> {
        self.files.last().expect("unable to find current WAL file")
    }

    pub(crate) fn get_checked(&self, id: WalFileId) -> Option<&Arc<File>> {
        id.0.checked_sub(self.min_file_id.0)
            .and_then(|id| self.files.get(id as usize))
    }

    pub(crate) fn get(&self, id: WalFileId) -> &Arc<File> {
        self.get_checked(id)
            .unwrap_or_else(|| panic!("attempt to access non existing file {id:?}"))
    }

    /// Creates a new WalFiles with the first `num_files` removed.
    /// This is used after deleting old WAL files from disk.
    pub(crate) fn skip_first_n_files(&self, num_files: usize) -> Self {
        assert!(
            num_files <= self.files.len(),
            "Cannot skip {} files, only {} files exist",
            num_files,
            self.files.len()
        );
        Self {
            base_path: self.base_path.clone(),
            files: self.files[num_files..].to_vec(),
            min_file_id: WalFileId(self.min_file_id.0 + num_files as u64),
        }
    }
}

#[allow(dead_code)]
#[doc(hidden)] // Used by tools/wal_inspector for progress tracking
#[cfg(any(test, feature = "test-utils"))]
pub fn list_wal_files_with_sizes(base_path: &Path) -> io::Result<Vec<(PathBuf, u64)>> {
    let prefix = format!("{}_", WalKind::Replay.name());
    let mut files = vec![];

    for entry in std::fs::read_dir(base_path)? {
        let file_path = entry?.path();
        if file_path.is_file()
            && let Some(file_name) = file_path.file_name().and_then(|name| name.to_str())
            && let Some(id_str) = file_name.strip_prefix(&prefix)
            && u64::from_str_radix(id_str, 16).is_ok()
        {
            let metadata = std::fs::metadata(&file_path)?;
            files.push((file_path, metadata.len()));
        }
    }

    // If no WAL files found, check for the default wal_0000000000000000 file
    if files.is_empty() {
        let default_wal_path = base_path.join(format!("{}{:016x}", prefix, 0));
        if default_wal_path.exists() {
            let metadata = std::fs::metadata(&default_wal_path)?;
            files.push((default_wal_path, metadata.len()));
        }
    }

    // Sort by file name (which corresponds to WAL file ID)
    files.sort_by(|(a, _), (b, _)| {
        let a_name = a.file_name().and_then(|n| n.to_str()).unwrap_or("");
        let b_name = b.file_name().and_then(|n| n.to_str()).unwrap_or("");
        a_name.cmp(b_name)
    });

    Ok(files)
}
