use bytes::BytesMut;
use minibytes::Bytes;
use std::ops::Range;
use std::time::Instant;

use super::index_format::{Direction, IndexFormat};
use super::{deserialize_index_entries, index_element_size, serialize_index_entries};
use crate::index::index_format::{binary_search, PREFIX_LENGTH};
use crate::key_shape::CELL_PREFIX_LENGTH;
use crate::metrics::Metrics;
use crate::wal::WalPosition;
use crate::{index::index_table::IndexTable, key_shape::KeySpaceDesc, lookup::RandomRead};

const DEFAULT_WINDOW_SIZE: usize = 800;
const NUM_WINDOW_SIZES: usize = 1;

#[derive(Clone)]
pub struct UniformLookupIndex {
    window_sizes: Vec<Vec<usize>>,
}

impl Default for UniformLookupIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl UniformLookupIndex {
    pub fn new() -> Self {
        let mut matrix = Vec::with_capacity(NUM_WINDOW_SIZES);
        for _ in 0..NUM_WINDOW_SIZES {
            let row = vec![DEFAULT_WINDOW_SIZE; NUM_WINDOW_SIZES];
            matrix.push(row);
        }

        Self {
            window_sizes: matrix,
        }
    }

    pub fn new_with_window_size(window_size: usize) -> Self {
        let mut matrix = Vec::with_capacity(NUM_WINDOW_SIZES);
        for _ in 0..NUM_WINDOW_SIZES {
            let row = vec![window_size; NUM_WINDOW_SIZES];
            matrix.push(row);
        }

        Self {
            window_sizes: matrix,
        }
    }

    #[allow(clippy::assertions_on_constants)] // todo use static assertion
    fn probable_key_offset_and_window_size(
        &self,
        ks: &KeySpaceDesc,
        cell_prefix_range: &Range<u64>,
        key: &[u8],
        file_length: usize,
    ) -> (usize, usize) {
        let long_prefix_range_start = cell_prefix_range
            .start
            .saturating_mul(1 << ((PREFIX_LENGTH - CELL_PREFIX_LENGTH) * 8));
        let long_prefix_range_end = cell_prefix_range
            .end
            .saturating_mul(1 << ((PREFIX_LENGTH - CELL_PREFIX_LENGTH) * 8));
        let cell_width = long_prefix_range_end.saturating_sub(long_prefix_range_start);

        assert!(PREFIX_LENGTH >= CELL_PREFIX_LENGTH);
        assert!(PREFIX_LENGTH <= 8); // we want the prefix to fit in u64
        let prefix = ks.index_prefix_u64(key);
        if long_prefix_range_start > prefix || prefix > long_prefix_range_end {
            panic!("Key prefix out of range: key prefix={prefix}, long_prefix_range={long_prefix_range_start}..{long_prefix_range_end}");
        }
        let prefix_pos = prefix.saturating_sub(long_prefix_range_start);
        // cannot cause overflow because prefix_pos is always smaller than cell_width
        let probable_offset =
            (((prefix_pos as u128) * (file_length as u128) / (cell_width as u128)) as usize)
                .clamp(0, file_length - 1);
        let half_window_size = self.window_sizes[0][0] / 2; // todo lookup by N and p

        (probable_offset, half_window_size)
    }

    fn lookup_window(
        half_window_size: usize,
        estimated_offset: usize,
        element_size: usize,
        file_length: usize,
    ) -> (usize, usize) {
        // make sure to align to element size
        let estimated_offset = estimated_offset / element_size * element_size;
        let from_offset = estimated_offset.saturating_sub(half_window_size * element_size);
        let to_offset = estimated_offset
            .saturating_add(half_window_size * element_size)
            .clamp(0, file_length);
        (from_offset, to_offset)
    }

    fn move_window_up(
        to_offset: usize,
        half_window_size: usize,
        element_size: usize,
        file_length: usize,
    ) -> (usize, usize) {
        let window_size = 2 * half_window_size * element_size;
        let new_from_offset = (to_offset.saturating_sub(element_size)).clamp(0, file_length);
        let new_to_offset = new_from_offset
            .saturating_add(window_size)
            .clamp(0, file_length);
        (new_from_offset, new_to_offset)
    }

    fn move_window_down(
        from_offset: usize,
        half_window_size: usize,
        element_size: usize,
        file_length: usize,
    ) -> (usize, usize) {
        let window_size = 2 * half_window_size * element_size;
        let new_to_offset = (from_offset + element_size).clamp(0, file_length);
        let new_from_offset = new_to_offset
            .saturating_sub(window_size)
            .clamp(0, file_length);
        (new_from_offset, new_to_offset)
    }

    fn next_entry_unloaded_no_prev(
        &self,
        ks: &KeySpaceDesc,
        reader: &impl RandomRead,
        direction: Direction,
        metrics: &Metrics,
    ) -> Option<(Bytes, WalPosition)> {
        let element_size = index_element_size(ks);
        let key_size = ks.index_key_size();
        let file_length = reader.len();

        // Read a window from the beginning or end of the file
        let window_size = self.window_sizes[0][0];
        let (from_offset, to_offset) = match direction {
            Direction::Forward => (0, (window_size * element_size).min(file_length)),
            Direction::Backward => {
                let from = file_length.saturating_sub(window_size * element_size);
                (from, file_length)
            }
        };

        if from_offset >= to_offset || from_offset >= file_length {
            return None;
        }

        let io_start = Instant::now();
        let buffer = reader.read(from_offset..to_offset);
        metrics
            .lookup_io_mcs
            .inc_by(io_start.elapsed().as_micros() as u64);
        metrics.lookup_io_bytes.inc_by(buffer.len() as u64);

        if buffer.is_empty() || buffer.len() < element_size {
            return None;
        }

        let entry_count = buffer.len() / element_size;
        if entry_count == 0 {
            return None;
        }

        // Return first or last entry based on direction
        let pos = direction.first_in_range(0..entry_count);

        let entry_start = pos * element_size;
        let key = buffer.slice(entry_start..entry_start + key_size);
        let value_bytes = &buffer[entry_start + key_size..entry_start + element_size];
        let pos = WalPosition::from_slice(value_bytes);

        Some((key, pos))
    }
}

impl IndexFormat for UniformLookupIndex {
    fn serialize_index(&self, table: &IndexTable, ks: &KeySpaceDesc) -> Bytes {
        let element_size = index_element_size(ks);
        let capacity = element_size * table.data.len();
        let mut out = BytesMut::with_capacity(capacity);
        serialize_index_entries(table, ks, &mut out);
        out.to_vec().into()
    }

    fn deserialize_index(&self, ks: &KeySpaceDesc, b: Bytes) -> IndexTable {
        deserialize_index_entries(ks, b)
    }

    fn lookup_unloaded(
        &self,
        ks: &KeySpaceDesc,
        reader: &impl RandomRead,
        key: &[u8],
        metrics: &Metrics,
    ) -> Option<WalPosition> {
        // todo simplify this function
        // compute cell and prefix
        let element_size = index_element_size(ks);
        let key_size = ks.index_key_size();
        assert_eq!(key.len(), key_size);
        let cell = ks.cell_id(key);
        let cell_prefix_range = ks.index_prefix_range(&cell);

        // compute probable offset
        let file_length = reader.len();
        let (probable_offset, half_window_size) =
            self.probable_key_offset_and_window_size(ks, &cell_prefix_range, key, file_length);

        // compute start and end of the window around probable offset
        let (mut from_offset, mut to_offset) =
            Self::lookup_window(half_window_size, probable_offset, element_size, file_length);

        let mut iterations = 0;
        loop {
            iterations += 1;
            if (to_offset - from_offset < element_size) || (from_offset >= file_length) {
                metrics.lookup_iterations.observe(iterations as f64);
                return None;
            }
            let io_start = Instant::now();
            let buffer = reader.read(from_offset..to_offset);
            metrics
                .lookup_io_mcs
                .inc_by(io_start.elapsed().as_micros() as u64);
            metrics.lookup_io_bytes.inc_by(buffer.len() as u64);

            let first_element_key = &buffer[..key_size];
            let last_element_key =
                &buffer[buffer.len() - element_size..buffer.len() - element_size + key_size];
            // first check if the buffer range contains the key
            if key < first_element_key && from_offset != 0 {
                // key is smaller than the first element in the buffer
                (from_offset, to_offset) = Self::move_window_down(
                    from_offset,
                    half_window_size,
                    element_size,
                    file_length,
                );
                continue;
            }
            if key > last_element_key && to_offset != file_length {
                // key is larger than the last element in the buffer
                (from_offset, to_offset) =
                    Self::move_window_up(to_offset, half_window_size, element_size, file_length);
                continue;
            }

            // if the key is in the index, it must be in this buffer, iterate over the buffer to find the key
            debug_assert_eq!(buffer.len() % element_size, 0);
            let count = buffer.len() / element_size;
            if count == 0 {
                metrics.lookup_iterations.observe(iterations as f64);
                return None; // no entries in this buffer window
            }

            // Use the extracted binary search function
            let (_, _, result) = binary_search(&buffer, key, element_size, key_size, Some(metrics));
            metrics.lookup_iterations.observe(iterations as f64);
            return result;
        }
    }

    fn next_entry_unloaded(
        &self,
        ks: &KeySpaceDesc,
        reader: &impl RandomRead,
        prev: Option<&[u8]>,
        direction: Direction,
        metrics: &Metrics,
    ) -> Option<(Bytes, WalPosition)> {
        let element_size = index_element_size(ks);
        let key_size = ks.index_key_size();
        let file_length = reader.len();

        // If there's no previous key, just start at the beginning/end
        let Some(prev_key) = prev else {
            return self.next_entry_unloaded_no_prev(ks, reader, direction, metrics);
        };

        assert_eq!(prev_key.len(), key_size);

        // Find the cell and compute probable offset
        let cell = ks.cell_id(prev_key);
        let cell_prefix_range = ks.index_prefix_range(&cell);
        let (probable_offset, half_window_size) =
            self.probable_key_offset_and_window_size(ks, &cell_prefix_range, prev_key, file_length);

        // Compute the initial window around the probable offset
        let (mut from_offset, mut to_offset) =
            Self::lookup_window(half_window_size, probable_offset, element_size, file_length);

        let mut iterations = 0;
        loop {
            iterations += 1;
            if (to_offset - from_offset < element_size) || (from_offset >= file_length) {
                metrics.lookup_iterations.observe(iterations as f64);
                return None;
            }

            let io_start = Instant::now();
            let buffer = reader.read(from_offset..to_offset);
            metrics
                .lookup_io_mcs
                .inc_by(io_start.elapsed().as_micros() as u64);
            metrics.lookup_io_bytes.inc_by(buffer.len() as u64);

            let first_element_key = &buffer[..key_size];
            let last_element_key =
                &buffer[buffer.len() - element_size..buffer.len() - element_size + key_size];

            // Check if the buffer range contains the key
            if prev_key < first_element_key && from_offset != 0 {
                // Key is smaller than the first element in the buffer, move window down
                (from_offset, to_offset) = Self::move_window_down(
                    from_offset,
                    half_window_size,
                    element_size,
                    file_length,
                );
                continue;
            }

            if prev_key > last_element_key && to_offset != file_length {
                // Key is larger than the last element in the buffer, move window up
                (from_offset, to_offset) =
                    Self::move_window_up(to_offset, half_window_size, element_size, file_length);
                continue;
            }

            // The key should be in this buffer (or its insertion point is)
            let entry_count = buffer.len() / element_size;
            if entry_count == 0 {
                metrics.lookup_iterations.observe(iterations as f64);
                return None;
            }

            // Use binary search to find the position
            let (found_pos, insertion_point, _) =
                binary_search(&buffer, prev_key, element_size, key_size, Some(metrics));

            let next_pos = match direction {
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
                        // Move to the previous position before the found key
                        if pos == 0 {
                            // If at the beginning of this buffer, we might need to check previous buffers
                            if from_offset == 0 {
                                return None; // Already at the beginning of the file
                            }

                            // Move window down and continue search
                            (from_offset, to_offset) = Self::move_window_down(
                                from_offset,
                                half_window_size,
                                element_size,
                                file_length,
                            );
                            continue;
                        }
                        pos - 1
                    } else {
                        // If key not found, use the position before insertion point
                        if insertion_point == 0 {
                            // If at the beginning of this buffer, we might need to check previous buffers
                            if from_offset == 0 {
                                return None; // Already at the beginning of the file
                            }

                            // Move window down and continue search
                            (from_offset, to_offset) = Self::move_window_down(
                                from_offset,
                                half_window_size,
                                element_size,
                                file_length,
                            );
                            continue;
                        }
                        insertion_point - 1
                    }
                }
            };

            // Check if there's a valid entry at the position
            if next_pos >= entry_count {
                // If we've reached the end of this buffer, try to move up
                if to_offset == file_length {
                    metrics.lookup_iterations.observe(iterations as f64);
                    return None; // Already at the end of the file
                }

                // Move window up and continue search
                (from_offset, to_offset) =
                    Self::move_window_up(to_offset, half_window_size, element_size, file_length);
                continue;
            }

            // Extract the key and value at the position
            let entry_start = next_pos * element_size;
            let key = buffer.slice(entry_start..entry_start + key_size);
            let value_bytes = &buffer[entry_start + key_size..entry_start + element_size];
            let pos = WalPosition::from_slice(value_bytes);

            metrics.lookup_iterations.observe(iterations as f64);
            return Some((key, pos));
        }
    }
}

#[cfg(test)]
mod test {
    use rand::Rng;

    use super::*;
    use crate::key_shape::KeyType;
    use crate::{file_reader::FileReader, index::index_format::test::*, key_shape::KeyShape};
    use std::collections::HashSet;
    #[test]
    pub fn test_index_lookup() {
        let pi = UniformLookupIndex::new();
        test_index_lookup_inner(&pi);
    }

    #[test]
    pub fn test_index_lookup_random() {
        let pi = UniformLookupIndex::new();
        test_index_lookup_random_inner(&pi);
    }

    #[test]
    fn test_key_at_window_edge() {
        let metrics = Metrics::new();
        let (shape, ks_id) = KeyShape::new_single(8, 1, KeyType::uniform(1));
        let ks = shape.ks(ks_id);

        // 2) Insert several entries in ascending order
        //    We'll ensure the "search key" is the first or last in the chunk
        let mut table = IndexTable::default();

        // We'll store keys [10, 20, 30, 40, 50], each is 8 bytes for simplicity
        // We do a trivial WalPosition for demonstration
        for &k in &[10_u64, 20, 30, 40, 50] {
            let key_bytes = k.to_be_bytes().to_vec();
            let walpos = WalPosition::test_value(k); // e.g. store the same number
            table.insert(Bytes::from(key_bytes), walpos);
        }

        // 3) Convert the table to bytes using SingleHopIndex
        let index_format = UniformLookupIndex::new();
        let serialized = index_format.serialize_index(&table, ks);

        // 4) We'll build our mock "window" that intentionally ends
        //    at the last entry being the search key.
        //    Let's say we want to read only the final two entries [40, 50].
        //    If the user wants to find key=40, it is the "first" in the chunk.
        //    If the user wants to find key=50, it is the "last" in the chunk.
        //
        // We'll figure out the offsets manually: each entry is 8 bytes of key + 8 bytes of WalPosition = 16 bytes total.
        // We have 5 entries => total size = 5 * 16 = 80. The layout is:
        // entry #0: offset 0..16   (key=10)
        // entry #1: offset 16..32 (key=20)
        // entry #2: offset 32..48 (key=30)
        // entry #3: offset 48..64 (key=40)
        // entry #4: offset 64..80 (key=50)

        // We'll define a custom partial chunk from offset=48..80 => that includes keys [40, 50].
        let mock_reader = MockRandomRead::new(serialized.clone());
        let from_offset = 48;
        let to_offset = 80;

        // 5) Manually read that "window" to confirm the first key is 40, last is 50
        let _ = mock_reader.read(from_offset..to_offset);
        // chunk now holds entries #3 (key=40) and #4 (key=50)

        // 6) Suppose we want to confirm that lookup_unloaded can find key=40 if it picks exactly this chunk
        //    We'll just call the normal function, though keep in mind that SingleHopIndex internally
        //    calculates offsets. In a real test, we might override the 'probable_offset' logic or do multiple steps
        //    until it arrives at from_offset=48..to_offset=80.
        //
        // For demonstration, we'll do it directly:
        let key_40 = 40_u64.to_be_bytes();
        let found = index_format.lookup_unloaded(ks, &mock_reader, &key_40, &metrics);
        assert_eq!(
            found,
            Some(WalPosition::test_value(40)),
            "Key=40 not found at chunk start"
        );

        // 7) Similarly, check key=50 (which is the last in the chunk)
        let key_50 = 50_u64.to_be_bytes();
        let found_50 = index_format.lookup_unloaded(ks, &mock_reader, &key_50, &metrics);
        assert_eq!(
            found_50,
            Some(WalPosition::test_value(50)),
            "Key=50 not found at chunk end"
        );

        // 8) Also confirm a missing key (key=45) isn't found in that partial chunk
        let key_45 = 45_u64.to_be_bytes();
        let missing = index_format.lookup_unloaded(ks, &mock_reader, &key_45, &metrics);
        assert_eq!(missing, None, "Key=45 should not be found");
    }

    #[test]
    fn test_probable_window_accuracy_with_random_keys() {
        let metrics = Metrics::new();
        let num_entries = 1_000_000;
        let test_lookups = num_entries / 10; // 10% lookups
        let (shape, ks_id) = KeyShape::new_single(8, 1, KeyType::uniform(1));
        let ks = shape.ks(ks_id);

        // 1) Generate random, distinct 64-bit keys
        //    We'll store them in a HashSet to ensure uniqueness.
        let mut rng = rand::thread_rng();
        let mut unique_keys = HashSet::with_capacity(num_entries);
        while unique_keys.len() < num_entries {
            let k = rng.gen::<u64>();
            unique_keys.insert(k);
        }

        // 2) Build an IndexTable from those random keys
        let mut index = IndexTable::default();
        for &k in &unique_keys {
            let key_bytes = k.to_be_bytes().to_vec();
            // We'll store WalPosition as the same integer for simplicity
            index.insert(Bytes::from(key_bytes), WalPosition::test_value(k));
        }

        // 3) Convert to bytes using SingleHopIndex
        let index_format = UniformLookupIndex::new();
        let data = index_format.serialize_index(&index, ks);

        // 4) Wrap it in our MockRandomRead
        let reader = MockRandomRead::new(data);

        // We'll now do random lookups for 10% of the keys
        // Build a vec of all keys, then sample from it
        let all_keys: Vec<u64> = unique_keys.into_iter().collect();
        let mut single_hop_success = 0;

        for _ in 0..test_lookups {
            // pick a random key from the set
            let key = all_keys[rng.gen_range(0..all_keys.len())];

            // reset the read call count
            reader.reset_call_count();

            // do the lookup
            let key_bytes = key.to_be_bytes();
            let found = index_format.lookup_unloaded(ks, &reader, &key_bytes, &metrics);

            // confirm correctness, though you can skip if you only care about stats
            assert_eq!(
                found,
                Some(WalPosition::test_value(key)),
                "Did not find expected key in index!"
            );

            // check if exactly one read call was needed
            if reader.call_count() == 1 {
                single_hop_success += 1;
            }
        }

        let success_rate = (single_hop_success as f64) / (test_lookups as f64) * 100.0;
        println!(
            "Single-hop success rate: {}/{} (~{:.2}%)",
            single_hop_success, test_lookups, success_rate
        );
        // Optionally: assert if you want a minimum threshold
        // assert!(success_rate > 50.0, "Single-hop success is too low!");
    }

    #[test]
    fn test_offset_calculation() {
        let cell_prefix_range = 0..100;
        let file_length = 1000; // test with file larger than range
        let pi = UniformLookupIndex::new();
        // key shape with one uniform cell
        let (key_shape, ks) = KeyShape::new_single(1, 1, KeyType::uniform(1));
        let ks = key_shape.ks(ks);

        let key = [0, 0, 0, 0, 0, 0, 0, 0];
        let (offset, _) =
            pi.probable_key_offset_and_window_size(ks, &cell_prefix_range, &key, file_length);
        assert_eq!(offset, 0);

        let key: [u8; 9] = [0, 0, 0, 0, 0, 0, 0, 0, 255];
        let (offset, _) =
            pi.probable_key_offset_and_window_size(ks, &cell_prefix_range, &key, file_length);
        assert_eq!(offset, 0);

        let key = k64(0);
        let (offset, _) =
            pi.probable_key_offset_and_window_size(ks, &cell_prefix_range, &key, file_length);
        assert_eq!(offset, 0);

        let key = k64((cell_prefix_range.end << 32) - 1);
        let (offset, _) =
            pi.probable_key_offset_and_window_size(ks, &cell_prefix_range, &key, file_length);
        assert_eq!(offset, file_length - 1);

        let key = k64((cell_prefix_range.end << 32) / 2);
        let (offset, _) =
            pi.probable_key_offset_and_window_size(ks, &cell_prefix_range, &key, file_length);
        assert_eq!(offset, file_length / 2);

        let file_length = 10; // test with file smaller than range

        let key = [0, 0, 0, 0, 0, 0, 0, 0];
        let (offset, _) =
            pi.probable_key_offset_and_window_size(ks, &cell_prefix_range, &key, file_length);
        assert_eq!(offset, 0);

        let key: [u8; 9] = [0, 0, 0, 0, 0, 0, 0, 0, 255];
        let (offset, _) =
            pi.probable_key_offset_and_window_size(ks, &cell_prefix_range, &key, file_length);
        assert_eq!(offset, 0);

        let key = k64(0);
        let (offset, _) =
            pi.probable_key_offset_and_window_size(ks, &cell_prefix_range, &key, file_length);
        assert_eq!(offset, 0);

        let key = k64((cell_prefix_range.end << 32) - 1);
        let (offset, _) =
            pi.probable_key_offset_and_window_size(ks, &cell_prefix_range, &key, file_length);
        assert_eq!(offset, file_length - 1);

        let key = k64((cell_prefix_range.end << 32) / 2);
        let (offset, _) =
            pi.probable_key_offset_and_window_size(ks, &cell_prefix_range, &key, file_length);
        assert_eq!(offset, file_length / 2);
    }

    #[test]
    fn test_singlehopindex_with_short_keys() {
        let metrics = Metrics::new();
        // 1) Build a KeyShape that expects e.g. 4‐byte keys
        let (shape, ks_id) = KeyShape::new_single(4, 16, KeyType::uniform(16));
        let ks = shape.ks(ks_id);

        // 2) Insert a few short keys into IndexTable
        //    e.g. 4‐byte keys, 1‐byte key, empty key...
        let mut index = IndexTable::default();
        let key1 = vec![1, 2, 3, 4];
        let key2 = vec![3, 4, 5, 6];

        index.insert(key1.clone().into(), WalPosition::test_value(1001));
        index.insert(key2.clone().into(), WalPosition::test_value(1002));

        // 3) Convert to bytes with SingleHopIndex
        let single_hop = UniformLookupIndex::new();
        let data = single_hop.serialize_index(&index, ks);

        // 4) Mock a simple in‐memory reader

        let reader = MockRandomRead::new(data);

        // 5) Make sure we can look up short_key1, short_key2, short_key3
        //    without panicking or indexing out of range
        let found1 = single_hop.lookup_unloaded(ks, &reader, &key1, &metrics);
        assert_eq!(found1, Some(WalPosition::test_value(1001)));

        let found2 = single_hop.lookup_unloaded(ks, &reader, &key2, &metrics);
        assert_eq!(found2, Some(WalPosition::test_value(1002)));

        // 6) Also ensure that a random short key that doesn't exist returns None
        let not_inserted = Bytes::from(vec![7, 8, 9, 10]);
        let found4 = single_hop.lookup_unloaded(ks, &reader, &not_inserted, &metrics);
        assert_eq!(found4, None, "Key {not_inserted:?} unexpectedly found");
    }

    #[test]
    fn test_persisted_index_with_filerange() {
        use std::fs::OpenOptions;
        use std::io::Write;
        let metrics = Metrics::new();

        // 1) Choose which PersistedIndex to test:
        let index_impl = UniformLookupIndex::new();

        // 2) Build a KeyShape, e.g. 8-byte keys
        let (shape, ks_id) = KeyShape::new_single(4, 16, KeyType::uniform(16));
        let ks = shape.ks(ks_id);

        // 3) Populate an IndexTable
        let mut table = IndexTable::default();
        let key1 = Bytes::from(vec![1, 2, 3, 4]);
        let key2 = Bytes::from(vec![3, 4, 5, 6]);
        table.insert(key1.clone(), WalPosition::test_value(100));
        table.insert(key2.clone(), WalPosition::test_value(200));

        // 4) Convert the table to bytes
        let bytes = index_impl.serialize_index(&table, ks);

        // 5) Write those bytes to a temp file
        let tmp_dir = tempdir::TempDir::new("test-wal").unwrap();
        let file_path = tmp_dir.path().join("index_data_filerange.bin");

        {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(&file_path)
                .expect("Failed to open file for writing");

            file.write_all(&bytes).expect("Failed to write index bytes");
            file.sync_all().unwrap();
        }

        // 6) Open the file again for reading, get its size, and build a FileRange
        let file = OpenOptions::new()
            .read(true)
            .open(&file_path)
            .expect("Failed to open file for reading");

        let file_len = file.metadata().unwrap().len();
        // Full file range is 0..file_len
        let file_range = crate::lookup::FileRange::new(FileReader::new(&file, false), 0..file_len);

        // 7) Use lookup_unloaded to find each key
        // let found1 = index_impl.lookup_unloaded(ks, &file_range, &key1);
        // assert_eq!(found1, Some(WalPosition::test_value(100)));

        let found2 = index_impl.lookup_unloaded(ks, &file_range, &key2, &metrics);
        assert_eq!(found2, Some(WalPosition::test_value(200)));

        // 8) Confirm non-existent key returns None
        let key3 = Bytes::from(vec![99, 99, 99, 99]);
        let found3 = index_impl.lookup_unloaded(ks, &file_range, &key3, &metrics);
        assert_eq!(found3, None);
    }

    #[test]
    fn single_entry_search() {
        let (shape, ks_id) = KeyShape::new_single(8, 4, KeyType::uniform(4));
        let ks = shape.ks(ks_id);
        let metrics = Metrics::new();

        // 1) Make an IndexTable with exactly 1 key-value
        let mut index = IndexTable::default();
        let key = Bytes::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let walpos = WalPosition::test_value(12345);
        index.insert(key.clone(), walpos);

        // 2) Write it to bytes
        let pi = UniformLookupIndex::new();
        let bytes = pi.serialize_index(&index, ks);
        assert!(!bytes.is_empty());

        // 3) Make sure we can find that exact key
        assert_eq!(Some(walpos), pi.lookup_unloaded(ks, &bytes, &key, &metrics));

        // 4) Try smaller key
        let smaller_key = Bytes::from(vec![0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(None, pi.lookup_unloaded(ks, &bytes, &smaller_key, &metrics));

        // 5) Try bigger key
        let bigger_key = Bytes::from(vec![255, 255, 255, 255, 255, 255, 255, 255]);
        assert_eq!(None, pi.lookup_unloaded(ks, &bytes, &bigger_key, &metrics));
    }

    #[test]
    fn test_persisted_index_roundtrip() {
        // 1) Choose an implementation: SingleHopIndex or MicroCellIndex
        //    Depending on which you want to test.
        let single_hop = UniformLookupIndex::new();

        // 2) Build a key shape (assuming 8-byte keys, but adapt as needed)
        let (shape, ks_id) = KeyShape::new_single(8, 4, KeyType::uniform(4));
        let ks = shape.ks(ks_id);

        // 3) Populate an IndexTable with some entries
        let mut original_index = IndexTable::default();

        // Insert a few sample entries:
        let key1 = Bytes::from(vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
        let key2 = Bytes::from(vec![0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0, 0, 0]);
        let key3 = Bytes::from(vec![0, 1, 2, 3, 4, 5, 6, 7]);
        original_index.insert(key1.clone(), WalPosition::test_value(100));
        original_index.insert(key2.clone(), WalPosition::test_value(200));
        original_index.insert(key3.clone(), WalPosition::test_value(300));

        // 4) Convert to bytes
        let serialized = single_hop.serialize_index(&original_index, ks);

        // 5) From those bytes, build a new IndexTable
        let roundtrip_index = single_hop.deserialize_index(ks, serialized);

        // 6) Confirm the new table has the same entries
        //    For example, check that each key still maps to the same WalPosition
        assert_eq!(
            roundtrip_index.get(&key1),
            Some(WalPosition::test_value(100)),
            "Key1 not found or mismatched in round-trip"
        );

        assert_eq!(
            roundtrip_index.get(&key2),
            Some(WalPosition::test_value(200)),
            "Key2 not found or mismatched in round-trip"
        );

        assert_eq!(
            roundtrip_index.get(&key3),
            Some(WalPosition::test_value(300)),
            "Key3 not found or mismatched in round-trip"
        );

        // Also iterate over roundtrip_index.data
        // and confirm it matches original_index.data exactly.
        assert_eq!(
            original_index.data.len(),
            roundtrip_index.data.len(),
            "IndexTable size mismatch"
        );

        for (k, pos) in &original_index.data {
            let rt_pos = roundtrip_index
                .get(k)
                .expect("Missing key in round-trip index");
            assert_eq!(pos, &rt_pos, "WalPosition mismatch for key={:?}", k);
        }

        // If no assertion failed, we've verified that
        // the persisted index correctly round-trips these entries.
    }

    #[test]
    fn test_next_entry_unloaded() {
        let index_format = UniformLookupIndex::new();
        crate::index::index_format::test::test_next_entry_unloaded_inner(&index_format);
    }

    #[test]
    fn test_next_entry_unloaded_empty_index() {
        let index_format = UniformLookupIndex::new();
        crate::index::index_format::test::test_next_entry_unloaded_empty_index_inner(&index_format);
    }

    #[test]
    fn test_next_entry_unloaded_no_prev() {
        let metrics = Metrics::new();
        let (shape, ks_id) = KeyShape::new_single(8, 1, KeyType::uniform(1));
        let ks = shape.ks(ks_id);

        // Create an index with sorted entries
        let mut table = IndexTable::default();
        for i in 1..6 {
            let key = (i as u64 * 10).to_be_bytes().to_vec();
            table.insert(Bytes::from(key), WalPosition::test_value(i));
        }

        // Convert the table to bytes
        let index_format = UniformLookupIndex::new();
        let serialized = index_format.serialize_index(&table, ks);
        let reader = MockRandomRead::new(serialized);

        // Test the newly extracted function directly
        // Test forward direction (should return first entry)
        let result =
            index_format.next_entry_unloaded_no_prev(ks, &reader, Direction::Forward, &metrics);
        assert!(result.is_some());
        let (key, pos) = result.unwrap();
        assert_eq!(key, 10_u64.to_be_bytes());
        assert_eq!(pos, WalPosition::test_value(1));

        // Test backward direction (should return last entry)
        let result =
            index_format.next_entry_unloaded_no_prev(ks, &reader, Direction::Backward, &metrics);
        assert!(result.is_some());
        let (key, pos) = result.unwrap();
        assert_eq!(key, 50_u64.to_be_bytes());
        assert_eq!(pos, WalPosition::test_value(5));
    }
}
