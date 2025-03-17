use bytes::{Buf, BufMut, BytesMut};
use minibytes::Bytes;
use std::ops::Range;

pub struct CrcFrame {
    bytes: Bytes,
}

pub trait IntoBytesFixed {
    fn len(&self) -> usize;
    fn write_into_bytes(&self, buf: &mut BytesMut);
}

impl CrcFrame {
    pub const CRC_HEADER_LENGTH: usize = 8;

    pub fn new(t: &impl IntoBytesFixed) -> Self {
        let mut bytes = BytesMut::with_capacity(t.len() + 8);
        bytes.put_u64(0);
        t.write_into_bytes(&mut bytes);
        let len = (t.len() as u32).to_be_bytes();
        bytes[0..4].copy_from_slice(&len);
        let crc = Self::crc(&bytes[8..]);
        let crc = crc.to_be_bytes();
        bytes[4..8].copy_from_slice(&crc);
        let bytes: bytes::Bytes = bytes.into();
        let bytes = bytes.into();
        Self { bytes }
    }

    pub fn read_from_slice(b: &[u8], pos: usize) -> Result<&[u8], CrcReadError> {
        let len = Self::checked_read(&b, pos)?;
        Ok(&b[Self::data_range(pos, len)])
    }

    pub fn read_from_bytes(b: &Bytes, pos: usize) -> Result<Bytes, CrcReadError> {
        let len = Self::checked_read(&b, pos)?;
        let data = b.slice(Self::data_range(pos, len));
        Ok(data)
    }
    pub fn read_size(mut b: &[u8]) -> usize {
        assert!(b.len() >= Self::CRC_HEADER_LENGTH);
        b.get_u32() as usize
    }

    fn checked_read(b: &[u8], pos: usize) -> Result<usize, CrcReadError> {
        if b.len() < pos + Self::CRC_HEADER_LENGTH {
            return Err(CrcReadError::OutOfBoundsHeader);
        }
        let mut h = &b[pos..];
        let len = h.get_u32();
        let crc = h.get_u32();
        if len == u32::MAX && crc == u32::MAX {
            return Err(CrcReadError::SkipMarker);
        }
        let len = len as usize;
        // no overflow because len and pos are converted from u32
        if b.len() < pos + Self::CRC_HEADER_LENGTH + len {
            return Err(CrcReadError::OutOfBoundsBody(len));
        }

        let data = &b[Self::data_range(pos, len)];
        let actual_crc = Self::crc(&data);
        if actual_crc != crc {
            return Err(CrcReadError::CrcMismatch);
        }
        Ok(len)
    }

    fn data_range(pos: usize, len: usize) -> Range<usize> {
        pos + Self::CRC_HEADER_LENGTH..pos + Self::CRC_HEADER_LENGTH + len
    }

    fn crc(b: &[u8]) -> u32 {
        if b.len() == 0 {
            u32::MAX
        } else {
            crc32fast::hash(b)
        }
    }

    pub fn skip_marker() -> Self {
        Self {
            bytes: vec![0xff; 8].into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CrcReadError {
    OutOfBoundsHeader,
    OutOfBoundsBody(usize),
    CrcMismatch,
    SkipMarker,
}

impl CrcFrame {
    pub fn len_with_header(&self) -> usize {
        self.bytes.len()
    }
}

impl AsRef<[u8]> for CrcFrame {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::BufMut;

    #[test]
    pub fn crc_test() {
        let data = vec![1u8, 2, 3];
        let crc = CrcFrame::new(&data);
        assert_eq!(
            &[1, 2, 3],
            &CrcFrame::read_from_bytes(&crc.bytes, 0).unwrap().as_ref()
        );
        let mut bm = BytesMut::with_capacity(1024);
        bm.put_u64(u64::MAX);
        bm.put_slice(&crc.bytes);
        let bytes = bytes::Bytes::from(bm.clone()).into();
        assert_eq!(
            &[1, 2, 3],
            &CrcFrame::read_from_bytes(&bytes, 8).unwrap().as_ref()
        );
        let bytes = bytes.slice(..bytes.len() - 1);
        assert_eq!(
            CrcFrame::read_from_bytes(&bytes, 8),
            Err(CrcReadError::OutOfBoundsBody(3))
        );
        let pos = bm.len() - 1;
        bm[pos] = 15;
        let bytes = bytes::Bytes::from(bm).into();
        assert_eq!(
            CrcFrame::read_from_bytes(&bytes, 8),
            Err(CrcReadError::CrcMismatch)
        );
    }

    impl IntoBytesFixed for Vec<u8> {
        fn len(&self) -> usize {
            self.len()
        }

        fn write_into_bytes(&self, buf: &mut BytesMut) {
            buf.put_slice(&self);
        }
    }
}
