use crate::crc::IntoBytesFixed;
use crate::math::ending_u32;
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::cmp::Ordering;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CellId {
    Integer(usize),
    Bytes(CellIdBytesContainer),
}

pub type CellIdBytesContainer = SmallVec<[u8; 16]>;

impl CellId {
    const CELL_ID_INTEGER: u8 = 0;
    const CELL_ID_BYTES: u8 = 1;

    pub fn mutex_seed(&self) -> usize {
        match self {
            CellId::Integer(p) => *p,
            CellId::Bytes(bytes) => ending_u32(bytes) as usize,
        }
    }

    pub fn assume_bytes_id(&self) -> &CellIdBytesContainer {
        match self {
            CellId::Integer(_) => panic!("assume_bytes_id called on integer cell id"),
            CellId::Bytes(bytes) => bytes,
        }
    }

    /// Deserialize a CellId from bytes
    pub fn from_bytes(b: &mut impl Buf) -> Self {
        let cell_type = b.get_u8();
        match cell_type {
            Self::CELL_ID_INTEGER => {
                let val = b.get_u64() as usize;
                CellId::Integer(val)
            }
            Self::CELL_ID_BYTES => {
                let len = b.get_u16() as usize;
                let mut vec = SmallVec::new();
                vec.extend_from_slice(&b.chunk()[..len]);
                b.advance(len);
                CellId::Bytes(vec)
            }
            _ => panic!("Unknown cell id type {cell_type}"),
        }
    }
}

impl PartialOrd for CellId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CellId {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (CellId::Integer(this), CellId::Integer(other)) => this.cmp(other),
            (CellId::Bytes(this), CellId::Bytes(other)) => this.cmp(other),
            (CellId::Integer(_), CellId::Bytes(_)) => {
                panic!("Not comparable cell ids: left integer, right bytes")
            }
            (CellId::Bytes(_), CellId::Integer(_)) => {
                panic!("Not comparable cell ids: left bytes, right integer")
            }
        }
    }
}

impl IntoBytesFixed for CellId {
    fn len(&self) -> usize {
        match self {
            CellId::Integer(_) => 1 + 8,                 // type + u64
            CellId::Bytes(bytes) => 1 + 2 + bytes.len(), // type + len(u16) + data
        }
    }

    fn write_into_bytes(&self, buf: &mut BytesMut) {
        match self {
            CellId::Integer(val) => {
                buf.put_u8(Self::CELL_ID_INTEGER);
                buf.put_u64(*val as u64);
            }
            CellId::Bytes(bytes) => {
                buf.put_u8(Self::CELL_ID_BYTES);
                assert!(
                    bytes.len() <= u16::MAX as usize,
                    "Cell ID bytes length {} exceeds u16::MAX",
                    bytes.len()
                );
                buf.put_u16(bytes.len() as u16);
                buf.put_slice(bytes);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cell_id_serialization(cell: CellId, expected_size: usize) {
        let mut buf = BytesMut::new();
        cell.write_into_bytes(&mut buf);
        assert_eq!(buf.len(), expected_size);

        let mut reader = &buf[..];
        let decoded = CellId::from_bytes(&mut reader);
        assert_eq!(cell, decoded);
    }

    #[test]
    fn test_cell_id_integer_serialization() {
        test_cell_id_serialization(CellId::Integer(12345), 9); // 1 byte type + 8 bytes u64
    }

    #[test]
    fn test_cell_id_bytes_serialization() {
        let bytes = SmallVec::from_slice(&[0x12, 0x34, 0x56, 0x78]);
        test_cell_id_serialization(CellId::Bytes(bytes), 7); // 1 byte type + 2 bytes len + 4 bytes data
    }

    #[test]
    fn test_cell_id_empty_bytes_serialization() {
        test_cell_id_serialization(CellId::Bytes(SmallVec::new()), 3); // 1 byte type + 2 bytes len + 0 bytes data
    }

    #[test]
    fn test_cell_id_large_bytes_serialization() {
        let bytes = SmallVec::from_slice(&[0xAB; 256]);
        test_cell_id_serialization(CellId::Bytes(bytes), 259); // 1 byte type + 2 bytes len + 256 bytes data
    }

    #[test]
    #[should_panic(expected = "Cell ID bytes length 65536 exceeds u16::MAX")]
    fn test_cell_id_too_large_bytes_panic() {
        let bytes = SmallVec::from_vec(vec![0xAB; 65536]);
        let cell = CellId::Bytes(bytes);
        let mut buf = BytesMut::new();
        cell.write_into_bytes(&mut buf); // Should panic
    }
}
