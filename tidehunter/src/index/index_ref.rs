//! `IndexRef` — how a cell refers to one of its on-disk index blobs.
//!
//! An index blob lives at a `WalPosition` in the index WAL; the ref is the
//! blob's identity. Everything that identifies, opens or deserializes an
//! index blob goes through it: the per-cell level list (`IndexLevels`,
//! persisted in the control region), the `Loader` trait, and the flusher.
//! WAL-level concerns, such as garbage collection, relocation and the index
//! blob cache, work on the position inside it.
//!
//! On the wire the ref is its fields in order, so a control region that
//! stores a level as a bare position reads back as a ref with that position.
use crate::wal::position::WalPosition;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct IndexRef {
    position: WalPosition,
}

impl IndexRef {
    /// Sentinel for an empty level slot; see `IndexLevels`.
    pub const INVALID: Self = Self {
        position: WalPosition::INVALID,
    };

    pub fn new(position: WalPosition) -> Self {
        Self { position }
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

    pub fn is_valid(&self) -> bool {
        self.position.is_valid()
    }

    pub fn valid(self) -> Option<Self> {
        if self.is_valid() { Some(self) } else { None }
    }

    #[cfg(test)]
    pub fn test_value(v: u64) -> Self {
        Self::new(WalPosition::test_value(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A ref serializes exactly like the position it wraps, which is what
    /// keeps control regions written with bare positions readable.
    #[test]
    fn serializes_like_a_position() {
        for position in [
            WalPosition::new(0, 0),
            WalPosition::new(12345, 678),
            WalPosition::new(u64::MAX - 1, u32::MAX - 1),
            WalPosition::INVALID,
        ] {
            let index_ref = IndexRef::new(position);
            assert_eq!(
                bincode::serialize(&index_ref).unwrap(),
                bincode::serialize(&position).unwrap()
            );
            let back: IndexRef =
                bincode::deserialize(&bincode::serialize(&position).unwrap()).unwrap();
            assert_eq!(back, index_ref);
            assert_eq!(back.position(), position);
            assert_eq!(back.offset(), position.offset());
            assert_eq!(back.frame_len(), position.frame_len());
            assert_eq!(back.is_valid(), position.is_valid());
        }
        assert!(!IndexRef::INVALID.is_valid());
        assert_eq!(IndexRef::INVALID.valid(), None);
        assert_eq!(
            IndexRef::test_value(7).valid(),
            Some(IndexRef::test_value(7))
        );
    }
}
