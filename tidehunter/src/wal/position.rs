use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct WalPosition {
    pub(super) offset: u64,
    pub(super) len: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct WalFileId(pub(super) u64);

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
}
