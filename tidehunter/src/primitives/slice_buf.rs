use bytes::Buf;
use minibytes::Bytes;

/// SliceBuf implements Buf on top of Bytes and allows to slice bytes from the buffer.
pub struct SliceBuf {
    data: Bytes,
    offset: usize,
}

impl SliceBuf {
    pub fn new(data: Bytes) -> Self {
        SliceBuf { data, offset: 0 }
    }

    /// Slices `n` bytes from the current position.
    /// Panics if fewer than `n` bytes remain.
    pub fn slice_n(&mut self, n: usize) -> Bytes {
        if self.remaining() < n {
            panic!(
                "Requested {} bytes, but only {} available",
                n,
                self.remaining()
            );
        }
        let slice = self.data.slice(self.offset..self.offset + n);
        self.offset += n;
        slice
    }
}

impl Buf for SliceBuf {
    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.offset)
    }

    fn chunk(&self) -> &[u8] {
        &self.data[self.offset..]
    }

    fn advance(&mut self, cnt: usize) {
        self.offset = self.offset.saturating_add(cnt).min(self.data.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slice_n_basic() {
        let mut buf = SliceBuf::new(Bytes::from("abcdef"));
        let chunk = buf.slice_n(3);
        assert_eq!(chunk, Bytes::from("abc"));
        assert_eq!(buf.remaining(), 3);
    }

    #[test]
    fn test_slice_n_exact() {
        let mut buf = SliceBuf::new(Bytes::from("12345"));
        let _ = buf.slice_n(5);
        assert_eq!(buf.remaining(), 0);
    }

    #[test]
    #[should_panic(expected = "Requested 10 bytes")]
    fn test_slice_n_panic_on_overread() {
        let mut buf = SliceBuf::new(Bytes::from("data"));
        let _ = buf.slice_n(10); // Should panic
    }

    #[test]
    fn test_chunk_and_advance() {
        let mut buf = SliceBuf::new(Bytes::from("xyz"));
        assert_eq!(buf.chunk(), b"xyz");
        buf.advance(1);
        assert_eq!(buf.chunk(), b"yz");
        assert_eq!(buf.remaining(), 2);
    }
}
