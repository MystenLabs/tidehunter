//! `IndexRef` — how a cell refers to one of its on-disk index blobs.
//!
//! An index blob lives at a `WalPosition` in the index WAL; the ref is the
//! blob's identity together with the one fact a reader needs that only the
//! writer knew: how many micro-cells the blob's lookup header has (see
//! `index::micro_cells`). Everything that identifies, opens or deserializes
//! an index blob goes through the ref: the per-cell level list
//! (`IndexLevels`, persisted in the control region), the `Loader` trait, and
//! the flusher. WAL-level concerns, such as garbage collection, relocation
//! and the index blob cache, work on the position inside it.
use crate::index::micro_cells::LEGACY_MICRO_CELLS;
use crate::wal::position::WalPosition;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct IndexRef {
    position: WalPosition,
    /// Micro-cells in the blob's lookup header. 0 for blobs of index formats
    /// without a header (`IndexFormatType::Uniform`) and for `INVALID`.
    micro_cells: u32,
}

impl IndexRef {
    /// Sentinel for an empty level slot; see `IndexLevels`.
    pub const INVALID: Self = Self {
        position: WalPosition::INVALID,
        micro_cells: 0,
    };

    pub fn new(position: WalPosition, micro_cells: u32) -> Self {
        Self {
            position,
            micro_cells,
        }
    }

    /// A ref to a blob written before refs recorded the micro-cell count,
    /// when every lookup header had `LEGACY_MICRO_CELLS` micro-cells. Used
    /// when migrating control regions that store levels as bare positions.
    /// An invalid position stays the `INVALID` sentinel.
    pub fn legacy(position: WalPosition) -> Self {
        if position.is_valid() {
            Self::new(position, LEGACY_MICRO_CELLS)
        } else {
            Self::INVALID
        }
    }

    /// Where the blob is in the index WAL.
    pub fn position(&self) -> WalPosition {
        self.position
    }

    /// Byte offset of the blob's frame in the index WAL.
    pub fn offset(&self) -> u64 {
        self.position.offset()
    }

    /// Unaligned length of the blob's WAL frame, header included; see
    /// `WalPosition::frame_len`.
    pub fn frame_len(&self) -> usize {
        self.position.frame_len()
    }

    /// Micro-cells in the blob's lookup header; what the lookup-header
    /// format needs to read the blob.
    pub fn micro_cells(&self) -> u32 {
        self.micro_cells
    }

    pub fn is_valid(&self) -> bool {
        self.position.is_valid()
    }

    pub fn valid(self) -> Option<Self> {
        if self.is_valid() { Some(self) } else { None }
    }

    /// A ref with one micro-cell, for tests whose blob is never read through
    /// the lookup-header format (mock loaders serving tables) or is small
    /// enough to have one. Tests that read a real blob build the ref from
    /// `SerializedIndex::micro_cells`.
    #[cfg(test)]
    pub fn test_value(v: u64) -> Self {
        Self::new(WalPosition::test_value(v), 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accessors_and_serde_roundtrip() {
        let position = WalPosition::new(12345, 678);
        let index_ref = IndexRef::new(position, 512);
        assert_eq!(index_ref.position(), position);
        assert_eq!(index_ref.offset(), 12345);
        assert_eq!(index_ref.frame_len(), 678);
        assert_eq!(index_ref.micro_cells(), 512);
        assert!(index_ref.is_valid());
        assert_eq!(index_ref.valid(), Some(index_ref));
        let back: IndexRef =
            bincode::deserialize(&bincode::serialize(&index_ref).unwrap()).unwrap();
        assert_eq!(back, index_ref);
        assert_eq!(bincode::serialize(&index_ref).unwrap().len(), 12 + 4);

        assert!(!IndexRef::INVALID.is_valid());
        assert_eq!(IndexRef::INVALID.valid(), None);
        assert_eq!(IndexRef::INVALID.micro_cells(), 0);
    }

    #[test]
    fn legacy_refs() {
        let position = WalPosition::new(99, 1000);
        let legacy = IndexRef::legacy(position);
        assert_eq!(legacy.position(), position);
        assert_eq!(legacy.micro_cells(), LEGACY_MICRO_CELLS);
        assert_eq!(IndexRef::legacy(WalPosition::INVALID), IndexRef::INVALID);
        assert_eq!(IndexRef::test_value(7).micro_cells(), 1);
    }
}
