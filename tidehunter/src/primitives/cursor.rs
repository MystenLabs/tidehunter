use bytes::Buf;

/// Slice cursor implements Buf on top of slice and tracks the current offset.
pub struct SliceCursor<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> SliceCursor<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    /// Slices first len bytes from current position, return the slice and advance cursor's buffer.
    pub fn take_slice(&mut self, len: usize) -> &'a [u8] {
        let end_offset = self.offset + len;
        if end_offset > self.data.len() {
            panic!(
                "SliceCursor::take_slice offset {}, requested slice {}, data.len {}",
                self.offset,
                len,
                self.data.len()
            );
        }
        let slice = &self.data[self.offset..end_offset];
        self.offset += len;
        slice
    }
}

impl Buf for SliceCursor<'_> {
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
    fn test_slice_cursor_chunk_and_advance() {
        let v = vec![1u8, 2, 3, 4, 5];
        let mut cur = SliceCursor::new(&v);
        assert_eq!(cur.chunk(), &[1, 2, 3, 4, 5]);
        cur.advance(1);
        assert_eq!(cur.chunk(), &[2, 3, 4, 5]);
        assert_eq!(cur.remaining(), 4);
        assert_eq!(cur.offset(), 1);
        cur.advance(1);
        assert_eq!(cur.chunk(), &[3, 4, 5]);
        assert_eq!(cur.remaining(), 3);
        assert_eq!(cur.offset(), 2);
    }

    #[test]
    fn test_slice_cursor_take_slice() {
        let v = vec![1u8, 2, 3, 4, 5];
        let mut cur = SliceCursor::new(&v);
        assert_eq!(cur.chunk(), &[1, 2, 3, 4, 5]);

        assert_eq!(&[1], cur.take_slice(1));
        assert_eq!(cur.chunk(), &[2, 3, 4, 5]);
        assert_eq!(cur.remaining(), 4);
        assert_eq!(cur.offset(), 1);

        assert_eq!(&[2, 3], cur.take_slice(2));
        assert_eq!(cur.chunk(), &[4, 5]);
        assert_eq!(cur.remaining(), 2);
        assert_eq!(cur.offset(), 3);

        assert_eq!(&[4, 5], cur.take_slice(2));
        assert_eq!(cur.chunk(), &[]);
        assert_eq!(cur.remaining(), 0);
        assert_eq!(cur.offset(), 5);
    }
}
