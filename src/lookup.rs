use crate::file_reader::FileReader;
use minibytes::Bytes;
use std::ops::Range;

pub struct FileRange<'a> {
    reader: FileReader<'a>,
    range: Range<u64>,
}

pub trait RandomRead {
    fn read(&self, range: Range<usize>) -> Bytes;
    fn len(&self) -> usize;
}

impl RandomRead for Bytes {
    fn read(&self, range: Range<usize>) -> Bytes {
        self.slice(range.start..range.end)
    }

    fn len(&self) -> usize {
        AsRef::<[u8]>::as_ref(self).len()
    }
}

impl RandomRead for FileRange<'_> {
    fn read(&self, range: Range<usize>) -> Bytes {
        let read_range = self.checked_range(range);
        self.reader
            .read_exact_at(
                read_range.start,
                (read_range.end - read_range.start) as usize,
            )
            .expect("Failed to read file")
    }

    fn len(&self) -> usize {
        (self.range.end - self.range.start) as usize
    }
}

impl<'a> FileRange<'a> {
    pub fn new(reader: FileReader<'a>, range: Range<u64>) -> Self {
        Self { reader, range }
    }

    #[inline]
    fn checked_range(&self, range: Range<usize>) -> Range<u64> {
        let start = self.range.start.checked_add(range.start as u64).unwrap();
        let end = start.checked_add(range.len() as u64).unwrap();
        let mapped_range = start..end;
        if start >= self.range.end || end > self.range.end {
            panic!(
                "Trying to read range {range:?}, mapped to {mapped_range:?}, limits {:?} ",
                self.range
            );
        }
        mapped_range
    }
}
