use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};

pub trait HasOffset {
    fn offset(&self) -> u64;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct WalPosition {
    pub(super) offset: u64,
    pub(super) len: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct WalFileId(pub(super) u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct MapId(pub(super) u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct LastProcessed(pub(super) u64);

impl WalPosition {
    pub const MAX: WalPosition = WalPosition {
        offset: u64::MAX,
        len: u32::MAX,
    };
    pub const INVALID: WalPosition = Self::MAX;
    pub const LENGTH: usize = 12;
    #[cfg(test)]
    pub const TEST: WalPosition = WalPosition::new(3311, 12);

    // Creates new wal position.
    // This should only be called from wal.rs or from conversion IndexWalPosition<->WalPosition
    pub(crate) const fn new(offset: u64, len: u32) -> Self {
        Self { offset, len }
    }

    pub fn write_to_buf(&self, buf: &mut impl BufMut) {
        buf.put_u64(self.offset);
        buf.put_u32(self.len);
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(Self::LENGTH);
        self.write_to_buf(&mut buf);
        buf.into()
    }

    pub fn read_from_buf(buf: &mut impl Buf) -> Self {
        Self::new(buf.get_u64(), buf.get_u32())
    }

    pub fn from_slice(mut slice: &[u8]) -> Self {
        let r = Self::read_from_buf(&mut slice);
        assert_eq!(0, slice.len());
        r
    }

    /// Returns unaligned length of the wal frame.
    /// The length includes frame header but does not account for alignment
    pub fn frame_len(&self) -> usize {
        self.len as usize
    }

    /// Same as frame_len but returns u32
    pub fn frame_len_u32(&self) -> u32 {
        self.len
    }

    /// Returns length of the payload at given wal position, without crc frame header and alignment.
    /// This is the length of the buffer that was passed when WalWriter::write was
    /// called that created this wal position.
    pub fn payload_len(&self) -> usize {
        self.frame_len()
            .checked_sub(crate::crc::CrcFrame::CRC_HEADER_LENGTH)
            .expect("Frame length must be greater or equal to crc header length")
    }

    pub fn valid(self) -> Option<Self> {
        if self == Self::INVALID {
            None
        } else {
            Some(self)
        }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn is_valid(self) -> bool {
        self != Self::INVALID
    }

    #[allow(dead_code)]
    pub fn test_value(v: u64) -> Self {
        Self::new(v, 0)
    }

    /// Bit reserved at the top of `offset` as the generation flag.
    /// 0 means generation 0 (the normal WAL); 1 means non-G0.
    const GEN_FLAG_BIT: u8 = 63;

    /// Whether an encoded offset has the generation flag bit set — i.e. the
    /// position lives in a non-G0 side WAL, or is the INVALID sentinel
    /// (`u64::MAX`, used on disk for tombstones). Flagged offsets are not
    /// comparable to main-WAL offsets by raw magnitude; see
    /// `IndexWalPosition::newer_than` for the cross-generation recency rule.
    #[inline]
    pub(crate) fn offset_is_flagged(offset: u64) -> bool {
        offset >> Self::GEN_FLAG_BIT != 0
    }

    /// Logical recency of two same-key offsets across WAL generations: an
    /// unflagged (G0) offset postdates any coexisting flagged one — data only
    /// flows down into deeper generations — and within one class raw offset
    /// order is write order. See `IndexWalPosition::newer_than` for the full
    /// rationale.
    #[inline]
    pub(crate) fn offset_newer_than(a: u64, b: u64) -> bool {
        match (Self::offset_is_flagged(a), Self::offset_is_flagged(b)) {
            (false, true) => true,
            (true, false) => false,
            _ => a > b,
        }
    }

    /// Encode a generation-tagged offset.
    ///
    /// `generation == 0` returns `raw_offset` unchanged. For `generation >= 1`
    /// the high bit is set and the next `generation_bits` bits encode
    /// `generation - 1`. `raw_offset` must fit in the remaining bits.
    ///
    /// `generation_bits` is the value returned by `Config::generation_bits()`;
    /// it can be 0 when there is exactly one non-G0 generation.
    pub fn encode_offset(generation: u8, raw_offset: u64, generation_bits: u8) -> u64 {
        if generation == 0 {
            assert_eq!(
                raw_offset >> Self::GEN_FLAG_BIT,
                0,
                "G0 offset {raw_offset} overflows the encoded range"
            );
            return raw_offset;
        }
        let payload_bits = Self::GEN_FLAG_BIT - generation_bits;
        assert_eq!(
            raw_offset >> payload_bits,
            0,
            "non-G0 offset {raw_offset} overflows {payload_bits} payload bits"
        );
        let gen_idx = (generation - 1) as u64;
        if generation_bits > 0 {
            let max_gen = (1u64 << generation_bits) - 1;
            assert!(
                gen_idx <= max_gen,
                "generation {generation} does not fit in {generation_bits} bits"
            );
        } else {
            assert_eq!(
                gen_idx, 0,
                "generation {generation} requires generation_bits > 0"
            );
        }
        (1u64 << Self::GEN_FLAG_BIT) | (gen_idx << payload_bits) | raw_offset
    }

    /// Inverse of `encode_offset`: returns `(generation, raw_offset)`.
    ///
    /// Must not be called on `INVALID`.
    pub fn decode_offset(offset: u64, generation_bits: u8) -> (u8, u64) {
        if offset >> Self::GEN_FLAG_BIT == 0 {
            return (0, offset);
        }
        let payload_bits = Self::GEN_FLAG_BIT - generation_bits;
        let payload_mask = if payload_bits == 64 {
            u64::MAX
        } else {
            (1u64 << payload_bits) - 1
        };
        let gen_idx = if generation_bits == 0 {
            0
        } else {
            (offset >> payload_bits) & ((1u64 << generation_bits) - 1)
        };
        let raw = offset & payload_mask;
        ((gen_idx + 1) as u8, raw)
    }

    /// Build a `WalPosition` from a generation tag, in-generation offset and length.
    pub fn for_generation(generation: u8, raw_offset: u64, len: u32, generation_bits: u8) -> Self {
        Self::new(
            Self::encode_offset(generation, raw_offset, generation_bits),
            len,
        )
    }

    /// Returns the generation tag encoded in this position. `0` is the default
    /// (G0) WAL. Panics if called on `INVALID`.
    pub fn generation(&self, generation_bits: u8) -> u8 {
        assert!(self.is_valid(), "called generation() on INVALID");
        Self::decode_offset(self.offset, generation_bits).0
    }

    /// Returns the in-generation offset (i.e. the offset inside the per-gen WAL).
    /// Panics if called on `INVALID`.
    pub fn raw_offset(&self, generation_bits: u8) -> u64 {
        assert!(self.is_valid(), "called raw_offset() on INVALID");
        Self::decode_offset(self.offset, generation_bits).1
    }
}

impl HasOffset for WalPosition {
    fn offset(&self) -> u64 {
        self.offset
    }
}

impl MapId {
    pub(super) fn new(id: u64) -> Self {
        Self(id)
    }

    pub(super) fn as_u64(self) -> u64 {
        self.0
    }

    pub(super) fn prev_map(self) -> Option<Self> {
        self.0.checked_sub(1).map(MapId)
    }

    pub(super) fn next_map(self) -> Self {
        MapId(self.0 + 1)
    }
}

impl LastProcessed {
    pub(crate) fn new(value: u64) -> Self {
        Self(value)
    }

    pub(crate) fn none() -> Self {
        Self(0)
    }

    #[doc(hidden)] // Used by tools/tideconsole for control region inspection
    pub fn as_u64(self) -> u64 {
        self.0
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[doc(hidden)] // Used by benches/index_iter_benchmarks to drive as-of lookups
    pub fn from_u64_for_test(value: u64) -> Self {
        Self(value)
    }

    /// Returns whether the position is processed(included in the index).
    ///
    /// Panics if the caller passes an INVALID or non-G0 position. The
    /// `last_processed` watermark lives in the main WAL's address space, so
    /// the comparison is only meaningful when the high gen-flag bit is
    /// clear, and the INVALID sentinel (`u64::MAX`) has no place here. By
    /// the `IndexTable` invariants only entries in `data` reach this
    /// predicate, and `data` holds only genuine main-WAL-offset writes.
    pub(crate) fn is_processed<T: HasOffset>(self, pos: &T) -> bool {
        let off = pos.offset();
        assert_ne!(off, u64::MAX, "is_processed called on INVALID position");
        assert_eq!(
            off >> 63,
            0,
            "is_processed called on non-G0 position (offset 0x{off:x})"
        );
        // Offset is processed if it is below (but not equal) to last_processed position
        // last_processed points to a next allocated position
        off < self.0
    }

    #[cfg(test)]
    pub(crate) fn new_test(value: u64) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_position() {
        let mut buf = BytesMut::new();
        WalPosition::TEST.write_to_buf(&mut buf);
        let bytes: bytes::Bytes = buf.into();
        let mut buf = bytes.as_ref();
        let position = WalPosition::read_from_buf(&mut buf);
        assert_eq!(position, WalPosition::TEST);
    }

    #[test]
    fn test_generation_encoding_g0_passthrough() {
        // G0 must be a no-op for any generation_bits.
        for gen_bits in 0..=4u8 {
            let enc = WalPosition::encode_offset(0, 12345, gen_bits);
            assert_eq!(enc, 12345);
            assert_eq!(WalPosition::decode_offset(enc, gen_bits), (0, 12345));
        }
    }

    #[test]
    fn test_generation_encoding_single_non_g0() {
        // Two generations total -> only G1 exists in non-G0 space, no bits needed.
        let gen_bits = 0;
        let enc = WalPosition::encode_offset(1, 999, gen_bits);
        assert_eq!(WalPosition::decode_offset(enc, gen_bits), (1, 999));
        // Top bit must be set.
        assert_ne!(enc >> 63, 0);
    }

    #[test]
    fn test_generation_encoding_multiple_non_g0() {
        // 4 generations: G1..G3 need 2 bits.
        let gen_bits = 2;
        for g in 1u8..=3 {
            for raw in [0u64, 1, 1024, (1u64 << 50) - 1] {
                let enc = WalPosition::encode_offset(g, raw, gen_bits);
                let (decoded_gen, decoded_raw) = WalPosition::decode_offset(enc, gen_bits);
                assert_eq!((decoded_gen, decoded_raw), (g, raw));
            }
        }
    }

    #[test]
    fn test_generation_round_trip_via_wal_position() {
        let gen_bits = 1;
        let p = WalPosition::for_generation(2, 42, 16, gen_bits);
        assert_eq!(p.generation(gen_bits), 2);
        assert_eq!(p.raw_offset(gen_bits), 42);
        assert_eq!(p.frame_len(), 16);
    }

    #[test]
    fn test_offset_newer_than() {
        let flagged = |x: u64| (1u64 << 63) | x;
        // Same class: raw offset order, strict.
        assert!(WalPosition::offset_newer_than(2, 1));
        assert!(!WalPosition::offset_newer_than(1, 2));
        assert!(!WalPosition::offset_newer_than(1, 1));
        assert!(WalPosition::offset_newer_than(flagged(2), flagged(1)));
        // Cross class: a G0 offset postdates any flagged one regardless of
        // magnitude.
        assert!(WalPosition::offset_newer_than(1, flagged(50)));
        assert!(!WalPosition::offset_newer_than(flagged(50), 1));
        // The INVALID sentinel (u64::MAX) is flagged: a live G0 write
        // postdates a disk tombstone.
        assert!(WalPosition::offset_newer_than(1, u64::MAX));
        assert!(!WalPosition::offset_newer_than(u64::MAX, 1));
    }
}
