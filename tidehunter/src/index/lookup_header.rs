use std::time::Instant;

use bytes::{Buf, BufMut};
use minibytes::Bytes;

use crate::math::rescale_u32;
use crate::metrics::Metrics;
use crate::wal::WalPosition;
use crate::{index::index_table::IndexTable, key_shape::KeySpaceDesc, lookup::RandomRead};

use super::index_format::{IndexFormat, HEADER_ELEMENTS, HEADER_ELEMENT_SIZE, HEADER_SIZE};
use super::{deserialize_index_entries, serialize_index_entries};
use crate::index::index_format::binary_search_entries;

#[derive(Clone)]
pub struct LookupHeaderIndex;

impl LookupHeaderIndex {
    fn key_micro_cell(ks: &KeySpaceDesc, key: &[u8]) -> usize {
        let prefix = ks.index_prefix_u32(key);
        let cell = ks.cell_id(key);
        let cell_prefix_range = ks.index_prefix_range(&cell);
        #[cfg(debug_assertions)]
        {
            let prefix = prefix as u64;
            if !cell_prefix_range.contains(&prefix) {
                panic!("prefix do not fall in prefix range. key {key:?}, cell {cell:?}, prefix {prefix:x}, range {cell_prefix_range:x?}");
            }
        }
        let cell_offset = prefix
            // cell_prefix_range.start is always u32 (but not cell_prefix_range.end)
            .checked_sub(cell_prefix_range.start as u32)
            .expect("Key prefix is out of cell prefix range");
        let cell_size = cell_prefix_range.end - cell_prefix_range.start;
        let micro_cell = rescale_u32(cell_offset, cell_size, HEADER_ELEMENTS as u32);
        micro_cell as usize
    }
}

impl IndexFormat for LookupHeaderIndex {
    fn to_bytes(&self, table: &IndexTable, ks: &KeySpaceDesc) -> Bytes {
        let element_size = Self::element_size(ks);
        let capacity = element_size * table.data.len() + HEADER_SIZE;

        // Use the common function to serialize entries
        let mut out = serialize_index_entries(table, ks, capacity, HEADER_SIZE);

        // Build header
        let mut header = IndexTableHeaderBuilder::new(ks);
        let mut current_offset = HEADER_SIZE;

        for (key, _) in table.data.iter() {
            header.add_key(key, current_offset);
            current_offset += element_size;
        }

        // Write header to the reserved space
        header.write_header(out.len(), &mut out[..HEADER_SIZE]);

        out.to_vec().into()
    }

    fn from_bytes(&self, ks: &KeySpaceDesc, b: Bytes) -> IndexTable {
        deserialize_index_entries(HEADER_SIZE, ks, b)
    }

    fn lookup_unloaded(
        &self,
        ks: &KeySpaceDesc,
        reader: &impl RandomRead,
        key: &[u8],
        metrics: &Metrics,
    ) -> Option<WalPosition> {
        let key_size = ks.reduced_key_size();
        assert_eq!(key.len(), key_size);
        let micro_cell = Self::key_micro_cell(ks, key);
        let now = Instant::now();
        let header_element =
            reader.read(micro_cell * HEADER_ELEMENT_SIZE..(micro_cell + 1) * HEADER_ELEMENT_SIZE);
        metrics
            .lookup_io_mcs
            .inc_by(now.elapsed().as_micros() as u64);
        metrics.lookup_io_bytes.inc_by(header_element.len() as u64);
        let mut header_element = &header_element[..];
        let from_offset = header_element.get_u32() as usize;
        let to_offset = header_element.get_u32() as usize;
        if from_offset == 0 && to_offset == 0 {
            return None;
        }
        let now = Instant::now();
        let buffer = reader.read(from_offset..to_offset);
        metrics
            .lookup_io_mcs
            .inc_by(now.elapsed().as_micros() as u64);
        metrics.lookup_io_bytes.inc_by(buffer.len() as u64);
        let element_size = Self::element_size(ks);

        // Use binary search instead of linear search
        binary_search_entries(&buffer, key, element_size, key_size, metrics)
    }

    fn use_unbounded_reader(&self) -> bool {
        true
    }
}
pub struct IndexTableHeaderBuilder<'a> {
    ks: &'a KeySpaceDesc,
    header: Vec<(u32, u32)>,
    last_micro_cell: Option<usize>,
}

impl<'a> IndexTableHeaderBuilder<'a> {
    pub fn new(ks: &'a KeySpaceDesc) -> Self {
        let header = (0..HEADER_ELEMENTS).map(|_| (0, 0)).collect();
        Self {
            ks,
            header,
            last_micro_cell: None,
        }
    }

    pub fn add_key(&mut self, key: &[u8], offset: usize) {
        let offset = Self::check_offset(offset);
        let micro_cell = LookupHeaderIndex::key_micro_cell(&self.ks, key);
        if let Some(last_micro_cell) = self.last_micro_cell {
            if last_micro_cell == micro_cell {
                return;
            }
            self.header[last_micro_cell].1 = offset;
        }
        self.last_micro_cell = Some(micro_cell);
        self.header[micro_cell].0 = offset;
    }

    pub fn write_header(self, end_offset: usize, mut buf: &mut [u8]) {
        let mut header = self.header;
        let end_offset = Self::check_offset(end_offset);
        if let Some(last_micro_cell) = self.last_micro_cell {
            header[last_micro_cell].1 = end_offset;
        }
        for (start, end) in header {
            buf.put_u32(start);
            buf.put_u32(end);
        }
    }

    fn check_offset(offset: usize) -> u32 {
        assert!(offset < u32::MAX as usize, "Index table is too large");
        offset as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::index_format::test::*;

    #[test]
    pub fn test_index_lookup() {
        let index = LookupHeaderIndex;
        test_index_lookup_inner(&index);
    }

    #[test]
    pub fn test_index_lookup_random() {
        let index = LookupHeaderIndex;
        test_index_lookup_random_inner(&index);
    }
}
