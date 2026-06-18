use super::Wal;
#[cfg(any(test, feature = "test-utils"))]
use super::layout::WalKind;
use super::layout::WalLayout;
use super::position::WalFileId;
use arc_swap::ArcSwap;
use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Open WAL files keyed by `WalFileId`. The map may be sparse — the index WAL
/// is GC'd by file id, so middle file ids can be missing after a snapshot
/// reclaims dead files. The replay WAL keeps a contiguous prefix by virtue of
/// its watermark-based GC, but the data structure does not enforce that.
pub(crate) struct WalFiles {
    pub(crate) base_path: PathBuf,
    pub(crate) files: BTreeMap<WalFileId, Arc<File>>,
}

impl WalFiles {
    pub(crate) fn new(base_path: &Path, layout: &WalLayout) -> io::Result<Arc<ArcSwap<Self>>> {
        let wal_files = Self::load(base_path, layout)?;
        Ok(Arc::new(ArcSwap::from(Arc::new(wal_files))))
    }

    pub(crate) fn load(base_path: &Path, layout: &WalLayout) -> io::Result<Self> {
        let mut files: BTreeMap<WalFileId, Arc<File>> = BTreeMap::new();
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
                files.insert(WalFileId(id), Arc::new(file));
            }
        }
        if files.is_empty() {
            let file = Wal::open_file(&layout.wal_file_name(base_path, WalFileId(0)), layout)?;
            files.insert(WalFileId(0), Arc::new(file));
        }
        Ok(Self {
            base_path: base_path.to_path_buf(),
            files,
        })
    }

    pub(crate) fn min_file_id(&self) -> WalFileId {
        *self.files.keys().next().expect("WalFiles is never empty")
    }

    pub(crate) fn current_file_id(&self) -> WalFileId {
        *self
            .files
            .keys()
            .next_back()
            .expect("WalFiles is never empty")
    }

    #[inline]
    pub(crate) fn current_file(&self) -> &Arc<File> {
        self.files
            .values()
            .next_back()
            .expect("unable to find current WAL file")
    }

    /// All live files, oldest first.
    pub(crate) fn all_files(&self) -> impl Iterator<Item = &Arc<File>> {
        self.files.values()
    }

    pub(crate) fn get_checked(&self, id: WalFileId) -> Option<&Arc<File>> {
        self.files.get(&id)
    }

    pub(crate) fn get(&self, id: WalFileId) -> &Arc<File> {
        self.get_checked(id)
            .unwrap_or_else(|| panic!("attempt to access non existing file {id:?}"))
    }

    /// Returns a new `WalFiles` with the listed file ids removed, along with
    /// the `(WalFileId, Arc<File>)` pairs that were removed. Ids not present
    /// in the map contribute no entry to the returned vec.
    pub(crate) fn without_files(
        &self,
        to_remove: &[WalFileId],
    ) -> (Self, Vec<(WalFileId, Arc<File>)>) {
        let mut files = self.files.clone();
        let mut removed = Vec::with_capacity(to_remove.len());
        for id in to_remove {
            if let Some(file) = files.remove(id) {
                removed.push((*id, file));
            }
        }
        (
            Self {
                base_path: self.base_path.clone(),
                files,
            },
            removed,
        )
    }

    /// Returns a new `WalFiles` with `file` inserted at `id`. Panics if `id`
    /// is already present — the writer-side caller appends sequentially and
    /// must never collide with an existing entry.
    pub(crate) fn with_file(&self, id: WalFileId, file: Arc<File>) -> Self {
        let mut files = self.files.clone();
        assert!(
            files.insert(id, file).is_none(),
            "wal file {id:?} already in registry",
        );
        Self {
            base_path: self.base_path.clone(),
            files,
        }
    }
}

#[allow(dead_code)]
#[doc(hidden)] // Used by tools/tideconsole for listing WAL files
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
