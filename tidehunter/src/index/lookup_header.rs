use std::time::Instant;

use bytes::{Buf, BufMut, BytesMut};
use minibytes::Bytes;

use crate::math::{next_bounded, rescale_u32};
use crate::metrics::Metrics;
use crate::wal::WalPosition;
use crate::{index::index_table::IndexTable, key_shape::KeySpaceDesc, lookup::RandomRead};

use super::index_format::{
    binary_search, Direction, IndexFormat, HEADER_ELEMENTS, HEADER_ELEMENT_SIZE, HEADER_SIZE,
};
use super::{deserialize_index_entries, index_element_size, serialize_index_entries};

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

    /// Reads a section of the index defined by the micro-cell
    fn read_micro_cell_section(
        &self,
        reader: &impl RandomRead,
        micro_cell: usize,
        metrics: &Metrics,
    ) -> Option<(Bytes, usize, usize)> {
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

        Some((buffer, from_offset, to_offset))
    }

    /// Process a single micro-cell to find the next entry
    fn process_micro_cell(
        &self,
        reader: &impl RandomRead,
        micro_cell: usize,
        prev: Option<&[u8]>,
        direction: Direction,
        key_size: usize,
        element_size: usize,
        metrics: &Metrics,
    ) -> Option<(Bytes, WalPosition)> {
        let (buffer, _, _) = self.read_micro_cell_section(reader, micro_cell, metrics)?;

        if buffer.is_empty() {
            return None;
        }

        let entry_count = buffer.len() / element_size;
        if entry_count == 0 {
            return None;
        }

        // If we have a previous key, find its position in the buffer
        let start_pos = if let Some(prev_key) = prev {
            // Use binary search function to find the position
            let (found_pos, insertion_point, _) =
                binary_search(&buffer, prev_key, element_size, key_size, Some(metrics));

            match direction {
                Direction::Forward => {
                    if let Some(pos) = found_pos {
                        // Move to the next position after the found key
                        pos + 1
                    } else {
                        // If key not found, use the insertion point
                        insertion_point
                    }
                }
                Direction::Backward => {
                    if let Some(pos) = found_pos {
                        pos.checked_sub(1)?
                    } else {
                        insertion_point.checked_sub(1)?
                    }
                }
            }
        } else {
            // No previous key, start from the beginning or end based on direction
            direction.first_in_range(0..entry_count)
        };

        // Check if there's a valid entry at the position
        if start_pos >= entry_count {
            return None;
        }

        // Extract the key and value at the position
        let entry_start = start_pos * element_size;
        let key = buffer.slice(entry_start..entry_start + key_size);
        let value_bytes = &buffer[entry_start + key_size..entry_start + element_size];
        let pos = WalPosition::from_slice(value_bytes);

        Some((key, pos))
    }
}

impl IndexFormat for LookupHeaderIndex {
    fn to_bytes(&self, table: &IndexTable, ks: &KeySpaceDesc) -> Bytes {
        let element_size = index_element_size(ks);
        let capacity = element_size * table.data.len() + HEADER_SIZE;
        let mut out = BytesMut::with_capacity(capacity);
        out.put_bytes(0, HEADER_SIZE);

        // Use the common function to serialize entries
        serialize_index_entries(table, ks, &mut out);

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
        deserialize_index_entries(ks, b.slice(HEADER_SIZE..))
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

        let (buffer, _, _) = self.read_micro_cell_section(reader, micro_cell, metrics)?;
        let element_size = index_element_size(ks);

        // Use binary search instead of linear search
        let (_, _, pos) = binary_search(&buffer, key, element_size, key_size, Some(metrics));
        pos
    }

    fn next_entry_unloaded(
        &self,
        ks: &KeySpaceDesc,
        reader: &impl RandomRead,
        prev: Option<&[u8]>,
        direction: Direction,
        metrics: &Metrics,
    ) -> Option<(Bytes, WalPosition)> {
        let key_size = ks.reduced_key_size();
        let element_size = index_element_size(ks);

        // If no previous key is provided, start from the first or last micro-cell
        // depending on the direction
        let start_micro_cell = if let Some(prev_key) = prev {
            assert_eq!(prev_key.len(), key_size);
            Self::key_micro_cell(ks, prev_key)
        } else {
            direction.first_in_range(0..HEADER_ELEMENTS)
        };

        let mut current_micro_cell = start_micro_cell;
        loop {
            if let result @ Some(_) = self.process_micro_cell(
                reader,
                current_micro_cell,
                prev,
                direction,
                key_size,
                element_size,
                metrics,
            ) {
                break result;
            }
            current_micro_cell = next_bounded(
                current_micro_cell,
                HEADER_ELEMENTS,
                direction == Direction::Backward,
            )?;
        }
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
    use crate::key_shape::{KeyShape, KeyType};
    use minibytes::Bytes;

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

    /// Tests for the next_entry_unloaded function
    #[test]
    fn test_next_entry_unloaded() {
        let index_format = LookupHeaderIndex;
        crate::index::index_format::test::test_next_entry_unloaded_inner(&index_format);
    }

    #[test]
    fn test_next_entry_unloaded_empty_index() {
        let index_format = LookupHeaderIndex;
        crate::index::index_format::test::test_next_entry_unloaded_empty_index_inner(&index_format);
    }

    #[test]
    fn test_next_entry_micro_cell_boundary() {
        let metrics = Metrics::new();
        // Create a key shape with a larger key space to ensure multiple micro cells
        let (shape, ks_id) = KeyShape::new_single(8, 1, KeyType::uniform(1));
        let ks = shape.ks(ks_id);

        // Create an index with entries that span multiple micro cells
        let mut table = IndexTable::default();

        // Adding keys that should span across micro cells
        // The exact distribution depends on key_micro_cell function behavior
        for i in 0..20 {
            // Create keys with increasing values spread across the key space
            let key_value = i as u64 * (u64::MAX / 20);
            let key = key_value.to_be_bytes().to_vec();
            table.insert(Bytes::from(key), WalPosition::test_value(i));
        }

        // Convert the table to bytes
        let index_format = LookupHeaderIndex;
        let serialized = index_format.to_bytes(&table, ks);

        // Use the MockRandomRead from the test module in index_format.rs
        let reader = MockRandomRead::new(serialized);

        // Test forward iteration across all entries to verify micro cell boundaries
        let mut current_key = None;
        let mut count = 0;

        loop {
            let result = index_format.next_entry_unloaded(
                ks,
                &reader,
                current_key.as_deref(),
                Direction::Forward,
                &metrics,
            );

            if result.is_none() {
                break;
            }

            let (key, _) = result.unwrap();
            current_key = Some(key);
            count += 1;
        }

        // Verify we can iterate through all entries
        assert_eq!(count, 20, "Should iterate through all 20 entries");

        // Test backward iteration
        let mut current_key = None;
        let mut count = 0;

        loop {
            let result = index_format.next_entry_unloaded(
                ks,
                &reader,
                current_key.as_deref(),
                Direction::Backward,
                &metrics,
            );

            if result.is_none() {
                break;
            }

            let (key, _) = result.unwrap();
            current_key = Some(key);
            count += 1;
        }

        // Verify we can iterate through all entries backwards
        assert_eq!(count, 20, "Should iterate through all 20 entries backward");
    }
}
