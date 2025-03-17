use bytes::BytesMut;
use minibytes::Bytes;
use std::alloc::Layout;
use std::fs::{File, OpenOptions};
use std::io;
use std::ops::Range;
use std::os::unix::fs::FileExt;

pub struct FileReader<'a> {
    file: &'a File,
    direct_io: bool,
}

impl<'a> FileReader<'a> {
    pub fn new(file: &'a File, direct_io: bool) -> Self {
        Self { file, direct_io }
    }

    /// Returns new un-initialized buffer of a given size
    pub fn io_buffer(&self, size: usize) -> Vec<u8> {
        if self.direct_io {
            unsafe {
                const PAGE_SIZE: usize = 4 * 1024;
                let layout = Layout::from_size_align(size, PAGE_SIZE).unwrap();
                let mem = std::alloc::alloc(layout);
                Vec::from_raw_parts(mem, size, size)
            }
        } else {
            let mut v = Vec::with_capacity(size);
            unsafe { v.set_len(size) };
            v
        }
    }

    pub fn io_buffer_bytes(&self, size: usize) -> BytesMut {
        let buffer = self.io_buffer(size);
        BytesMut::from(bytes::Bytes::from(buffer))
    }

    /// returns new un-initialized buffer with requested size and copies over given buffer to the beginning
    pub fn reallocate(&self, buf: BytesMut, new_size: usize) -> BytesMut {
        assert!(new_size >= buf.len());
        // todo we can try to use mem realloc here since we might avoid memory copy in that case
        let mut new_buf = self.io_buffer_bytes(new_size);
        new_buf[..buf.len()].copy_from_slice(&buf);
        new_buf
    }

    pub fn read_exact_at(&self, pos: u64, len: usize) -> io::Result<Bytes> {
        if self.direct_io {
            let range = pos..(pos + len as u64);
            let (read_range, map_range) = align_range(range);
            let mut buffer = self.io_buffer((read_range.end - read_range.start) as usize);
            self.file.read_exact_at(&mut buffer, read_range.start)?;
            let buffer = Bytes::from(buffer);
            let buffer = buffer.slice(map_range);
            Ok(buffer)
        } else {
            let mut buffer = self.io_buffer(len);
            self.file.read_exact_at(&mut buffer, pos)?;
            Ok(Bytes::from(buffer))
        }
    }
}

/// Aligns given range with specified alignment (const 512 bytes right now).
/// This adjusts given arbitrary range to a range that can be used with direct io.
///
/// Returns two ranges:
///  - **Read range** - potentially bigger range aligned for direct IO.
///  - **Map range** - Range that maps region that has been read into the originally requested region.
fn align_range(range: Range<u64>) -> (Range<u64>, Range<usize>) {
    let range_len = range.end - range.start;
    let read_range =
        align_down(range.start, DIRECT_IO_ALIGNMENT)..align_up(range.end, DIRECT_IO_ALIGNMENT);
    let map_start = (range.start - read_range.start) as usize;
    let map_end = map_start + range_len as usize;
    let map_range = map_start..map_end;
    (read_range, map_range)
}

const DEFAULT_ALIGNMENT: u64 = 8;
const DIRECT_IO_ALIGNMENT: u64 = 512;

pub(crate) const fn align_size(v: u64, direct_io: bool) -> u64 {
    if direct_io {
        align_up(v, DIRECT_IO_ALIGNMENT)
    } else {
        align_up(v, DEFAULT_ALIGNMENT)
    }
}

const fn align_down(v: u64, align: u64) -> u64 {
    v / align * align
}

const fn align_up(v: u64, align: u64) -> u64 {
    align_down(v + align - 1, align)
}

pub(crate) fn set_direct_options(options: &mut OpenOptions, direct_io: bool) {
    if direct_io {
        set_o_direct(options)
    }
}

#[cfg(unix)]
fn set_o_direct(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(0x4000 /*O_DIRECT*/);
}

#[cfg(not(unix))]
fn set_o_direct(options: &mut OpenOptions) {
    unimplemented!("set_o_direct not implemented non-unix systems");
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;
    use std::fs::OpenOptions;
    use std::path::Path;

    #[test]
    fn test_align_range() {
        #[track_caller]
        fn assert_align(range: Range<u64>, expected_read: Range<u64>, expected_map: Range<usize>) {
            let (actual_read, actual_map) = align_range(range);
            assert_eq!(actual_read, expected_read, "Read range mismatch");
            assert_eq!(actual_map, expected_map, "Map range mismatch");
        }

        assert_align(0..10, 0..512, 0..10);
        assert_align(0..512, 0..512, 0..512);
        assert_align(5..512, 0..512, 5..512);
        assert_align(5..20, 0..512, 5..20);

        assert_align(1024..1034, 1024..(1024 + 512), 0..10);
        assert_align(1024..(1024 + 512), 1024..(1024 + 512), 0..512);
        assert_align(1034..(1024 + 512), 1024..(1024 + 512), 10..512);
        assert_align(1034..1044, 1024..(1024 + 512), 10..20);
    }

    #[test]
    fn test_align_value() {
        const A: u64 = 512;
        assert_eq!(align_down(0, A), 0);
        assert_eq!(align_down(1, A), 0);
        assert_eq!(align_down(10, A), 0);
        assert_eq!(align_down(511, A), 0);
        assert_eq!(align_down(512, A), 512);
        assert_eq!(align_down(513, A), 512);
        assert_eq!(align_down(1023, A), 512);
        assert_eq!(align_down(1024, A), 1024);
        assert_eq!(align_down(1025, A), 1024);

        assert_eq!(align_up(0, A), 0);
        assert_eq!(align_up(1, A), 512);
        assert_eq!(align_up(10, A), 512);
        assert_eq!(align_up(511, A), 512);
        assert_eq!(align_up(512, A), 512);
        assert_eq!(align_up(513, A), 1024);
        assert_eq!(align_up(1023, A), 1024);
        assert_eq!(align_up(1024, A), 1024);
        assert_eq!(align_up(1025, A), 1024 + 512);
    }

    #[test]
    fn test_file_reader() {
        let dir = tempdir::TempDir::new("test_file_reader").unwrap();
        let path = dir.path().join("file");
        let mut buf = BytesMut::zeroed(12 * 1024);
        let mut writer = &mut buf[..];
        for i in 0..3 * 1024 {
            writer.put_u32(i);
        }
        std::fs::write(&path, &buf).unwrap();
        test_file_reader_impl(&path, false);
        test_file_reader_impl(&path, true);
    }

    fn test_file_reader_impl(path: &Path, direct_io: bool) {
        println!("test_file_reader_impl direct_io: {direct_io}");
        let mut options = OpenOptions::new();
        options.read(true);
        set_direct_options(&mut options, direct_io);
        let file = options.open(path).unwrap();
        let reader = FileReader::new(&file, direct_io);
        #[track_caller]
        fn test_read(reader: &FileReader, v: u32) {
            let pos = (v as u64) * 4;
            let buf = reader.read_exact_at(pos, 4).unwrap();
            let mut sbuf = [0u8; 4];
            sbuf.copy_from_slice(&buf);
            assert_eq!(u32::from_be_bytes(sbuf), v);
        }

        test_read(&reader, 0);
        test_read(&reader, 1);
        test_read(&reader, 15);
        test_read(&reader, 1024);
        test_read(&reader, 1025);
        test_read(&reader, 2048);
        test_read(&reader, 2047);
        test_read(&reader, 2049);
        test_read(&reader, 3 * 1024 - 1);
    }
}
