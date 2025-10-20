use crate::file_reader::align_size;
use std::ops::Range;
use std::path::{Path, PathBuf};

use super::position::{MapId, WalFileId};

#[derive(Debug, Copy, Clone)]
pub enum WalKind {
    Replay,
    Index,
}

#[doc(hidden)] // Used by tools/wal_inspector for WAL configuration
#[derive(Clone)]
pub struct WalLayout {
    pub frag_size: u64,
    pub max_maps: usize,
    pub direct_io: bool,
    pub wal_file_size: u64,
    pub kind: WalKind,
}

impl WalLayout {
    pub(super) fn assert_layout(&self) {
        assert!(self.frag_size <= u32::MAX as u64, "Frag size too large");
        assert_eq!(
            self.frag_size,
            self.align(self.frag_size),
            "Frag size not aligned"
        );
        assert_eq!(
            self.wal_file_size % self.frag_size,
            0,
            "WAL file size must be a multiple of the frag size"
        );
    }

    /// Returns next position to write to after a previously allocated valid wal position.
    pub(super) fn next_after_wal_position(
        &self,
        wal_position: super::position::WalPosition,
    ) -> u64 {
        assert!(wal_position.is_valid());
        let len_aligned = self.align(wal_position.len as u64);
        assert!(
            len_aligned <= self.frag_size,
            "Entry({len_aligned}) is larger then frag_size({})",
            self.frag_size
        );
        wal_position.offset + len_aligned
    }

    /// Allocate the next position.
    /// Block should not cross the map boundary defined by the self.frag_size
    pub(super) fn next_position(&self, mut pos: u64, len_aligned: u64) -> u64 {
        assert!(
            len_aligned <= self.frag_size,
            "Entry({len_aligned}) is larger then frag_size({})",
            self.frag_size
        );
        let map_start = self.locate(pos).0;
        let map_end = self.locate(pos + len_aligned - 1).0;
        if map_start != map_end {
            debug_assert_eq!(map_start.next_map(), map_end);
            pos = self.first_in_frag(map_start.next_map());
        }
        pos
    }

    /// Return number of a mapping and offset inside the mapping for given position
    #[inline]
    pub(super) fn locate(&self, pos: u64) -> (MapId, u64) {
        (MapId::new(pos / self.frag_size), pos % self.frag_size)
    }

    /// Returns first position in fragment
    fn first_in_frag(&self, map: MapId) -> u64 {
        map.as_u64() * self.frag_size
    }

    /// Check if offset of given wal position is the first position in fragment.
    /// Return Some(frag) if this is the first wal position in frag, returns None otherwise.
    pub(super) fn is_first_in_frag(&self, pos: u64) -> Option<MapId> {
        let (frag, offset) = self.locate(pos);
        if offset == 0 {
            Some(frag)
        } else {
            None
        }
    }

    /// Return range of a particular mapping
    pub(super) fn map_range(&self, map: MapId) -> Range<u64> {
        let start = self.frag_size * map.as_u64();
        let end = self.frag_size * (map.as_u64() + 1);
        start..end
    }

    /// Return the position range for a particular WAL file
    pub fn wal_file_range(&self, file_id: WalFileId) -> Range<u64> {
        let start = self.wal_file_size * file_id.0;
        let end = self.wal_file_size * (file_id.0 + 1);
        start..end
    }

    #[doc(hidden)] // Used by tools/wal_inspector for control region inspection
    #[inline]
    pub fn locate_file(&self, offset: u64) -> WalFileId {
        WalFileId(offset / self.wal_file_size)
    }

    pub fn file_for_map(&self, map_id: MapId) -> WalFileId {
        self.locate_file(self.map_range(map_id).start)
    }

    #[inline]
    pub(super) fn offset_in_wal_file(&self, offset: u64) -> u64 {
        offset % self.wal_file_size
    }

    pub fn align(&self, v: u64) -> u64 {
        align_size(v, self.direct_io)
    }

    pub fn wal_file_name(&self, base_path: &Path, file_id: WalFileId) -> PathBuf {
        base_path.join(format!("{}_{:016x}", self.kind.name(), file_id.0))
    }

    #[cfg(test)]
    pub fn new_simple(frag_size: u64) -> Self {
        Self {
            frag_size,
            wal_file_size: frag_size,
            max_maps: 1,
            direct_io: false,
            kind: WalKind::Replay,
        }
    }
}

impl WalKind {
    pub fn name(&self) -> &'static str {
        match self {
            WalKind::Replay => "wal",
            WalKind::Index => "index",
        }
    }
}

#[test]
fn test_first_in_frag() {
    let layout = WalLayout {
        frag_size: 1024,
        max_maps: 1,
        direct_io: false,
        wal_file_size: 1024,
        kind: WalKind::Replay,
    };

    assert_eq!(0, layout.first_in_frag(MapId::new(0)));
    assert_eq!(1024, layout.first_in_frag(MapId::new(1)));
    assert_eq!(Some(MapId::new(1)), layout.is_first_in_frag(1024));
    assert_eq!(None, layout.is_first_in_frag(1025));
}

#[test]
fn test_wal_file_range() {
    let layout = WalLayout {
        frag_size: 1024,
        max_maps: 4,
        direct_io: false,
        wal_file_size: 8192,
        kind: WalKind::Replay,
    };

    // File 0 should contain positions 0..8192
    assert_eq!(0..8192, layout.wal_file_range(WalFileId(0)));

    // File 1 should contain positions 8192..16384
    assert_eq!(8192..16384, layout.wal_file_range(WalFileId(1)));

    // File 5 should contain positions 40960..49152
    assert_eq!(40960..49152, layout.wal_file_range(WalFileId(5)));

    // Verify that locate_file and wal_file_range are consistent
    for pos in [0, 100, 8191, 8192, 16383, 16384, 40960] {
        let file_id = layout.locate_file(pos);
        let range = layout.wal_file_range(file_id);
        assert!(
            range.contains(&pos),
            "Position {} should be in range {:?} for file {:?}",
            pos,
            range,
            file_id
        );
    }
}
