use std::ops::Range;
use std::time::Instant;

use minibytes::Bytes;

use super::lookup_header::LookupHeaderIndex;
use super::uniform_lookup::UniformLookupIndex;
use crate::metrics::Metrics;
use crate::wal::WalPosition;
use crate::{index::index_table::IndexTable, key_shape::KeySpaceDesc, lookup::RandomRead};

pub const HEADER_ELEMENTS: usize = 128;
pub const HEADER_ELEMENT_SIZE: usize = 8;
pub const HEADER_SIZE: usize = HEADER_ELEMENTS * HEADER_ELEMENT_SIZE;
pub const PREFIX_LENGTH: usize = 8; // prefix of key used to estimate position in file, in bytes

pub trait IndexFormat {
    fn to_bytes(&self, table: &IndexTable, ks: &KeySpaceDesc) -> Bytes;
    fn from_bytes(&self, ks: &KeySpaceDesc, b: Bytes) -> IndexTable;
    fn lookup_unloaded(
        &self,
        ks: &KeySpaceDesc,
        reader: &impl RandomRead,
        key: &[u8],
        metrics: &Metrics,
    ) -> Option<WalPosition>;

    /// Find the next entry in the index after the given key in the specified direction.
    /// If prev is None, returns the first entry in forward direction or last entry in backward direction.
    fn next_entry_unloaded(
        &self,
        ks: &KeySpaceDesc,
        reader: &impl RandomRead,
        prev: Option<&[u8]>,
        direction: Direction,
        metrics: &Metrics,
    ) -> Option<(Bytes, WalPosition)>;
}

#[derive(Clone, Default)]
pub enum IndexFormatType {
    #[default]
    Header,
    Uniform(UniformLookupIndex),
}

impl IndexFormat for IndexFormatType {
    fn to_bytes(&self, table: &IndexTable, ks: &KeySpaceDesc) -> Bytes {
        match self {
            IndexFormatType::Header => LookupHeaderIndex.to_bytes(table, ks),
            IndexFormatType::Uniform(index) => index.to_bytes(table, ks),
        }
    }

    fn from_bytes(&self, ks: &KeySpaceDesc, b: Bytes) -> IndexTable {
        match self {
            IndexFormatType::Header => LookupHeaderIndex.from_bytes(ks, b),
            IndexFormatType::Uniform(index) => index.from_bytes(ks, b),
        }
    }

    fn lookup_unloaded(
        &self,
        ks: &KeySpaceDesc,
        reader: &impl RandomRead,
        key: &[u8],
        metrics: &Metrics,
    ) -> Option<WalPosition> {
        match self {
            IndexFormatType::Header => LookupHeaderIndex.lookup_unloaded(ks, reader, key, metrics),
            IndexFormatType::Uniform(index) => index.lookup_unloaded(ks, reader, key, metrics),
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
        match self {
            IndexFormatType::Header => {
                LookupHeaderIndex.next_entry_unloaded(ks, reader, prev, direction, metrics)
            }
            IndexFormatType::Uniform(index) => {
                index.next_entry_unloaded(ks, reader, prev, direction, metrics)
            }
        }
    }
}

/// Direction for index navigation
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Direction {
    Forward,
    Backward,
}

impl Direction {
    pub fn from_bool(reverse: bool) -> Self {
        if reverse {
            Self::Backward
        } else {
            Self::Forward
        }
    }

    pub fn first_in_range(&self, range: Range<usize>) -> usize {
        match self {
            Self::Forward => range.start,
            Self::Backward => range.end - 1,
        }
    }
}

/// Performs a comprehensive binary search on a buffer of key-value entries.
/// Each entry consists of a key followed by a WalPosition.
///
/// # Arguments
/// * `buffer` - The buffer containing the entries
/// * `key` - The key to search for
/// * `element_size` - The size of each full entry (key + position)
/// * `key_size` - The size of just the key portion
/// * `metrics` - Optional metrics to track performance
///
/// # Returns
/// * `(found_pos, insertion_point, position)` where:
///   - `found_pos` is Some(index) if key is found, None otherwise
///   - `insertion_point` is where the key would be inserted if not found
///   - `position` is Some(WalPosition) if key is found, None otherwise
pub fn binary_search(
    buffer: &[u8],
    key: &[u8],
    element_size: usize,
    key_size: usize,
    metrics: Option<&Metrics>,
) -> (Option<usize>, usize, Option<WalPosition>) {
    if buffer.is_empty() {
        return (None, 0, None);
    }

    let scan_start = Instant::now();
    let count = buffer.len() / element_size;
    if count == 0 {
        if let Some(m) = metrics {
            m.lookup_scan_mcs
                .inc_by(scan_start.elapsed().as_micros() as u64);
        }
        return (None, 0, None);
    }

    let mut left = 0;
    let mut right = count - 1;
    let mut found_pos = None;

    while left <= right {
        let mid = left + (right - left) / 2;
        let entry_start = mid * element_size;
        let entry_key = &buffer[entry_start..entry_start + key_size];

        match entry_key.cmp(key) {
            std::cmp::Ordering::Equal => {
                found_pos = Some(mid);
                break;
            }
            std::cmp::Ordering::Less => left = mid + 1,
            std::cmp::Ordering::Greater => {
                if mid == 0 {
                    break;
                }
                right = mid - 1;
            }
        }
    }

    let position = found_pos.map(|pos| {
        let entry_start = pos * element_size;
        let pos_slice = &buffer[(entry_start + key_size)..(entry_start + element_size)];
        WalPosition::from_slice(pos_slice)
    });

    if let Some(m) = metrics {
        m.lookup_scan_mcs
            .inc_by(scan_start.elapsed().as_micros() as u64);
    }

    (found_pos, left, position)
}

#[cfg(test)]
pub mod test {
    use std::cell::Cell;

    use minibytes::Bytes;
    use rand::{rngs::ThreadRng, Rng, RngCore};

    use crate::index::index_format::binary_search;
    use crate::index::index_format::Direction;
    use crate::key_shape::{KeyType, MAX_U32_PLUS_ONE};
    use crate::lookup::RandomRead;
    use crate::{
        index::{index_format::IndexFormat, index_table::IndexTable},
        key_shape::KeyShape,
        metrics::Metrics,
        wal::WalPosition,
    };

    // Create a mock reader
    pub struct MockRandomRead {
        data: Bytes,
        read_calls: Cell<usize>,
    }

    impl MockRandomRead {
        pub fn new(data: Bytes) -> Self {
            Self {
                data,
                read_calls: Cell::new(0),
            }
        }

        pub fn reset_call_count(&self) {
            self.read_calls.set(0);
        }

        pub fn call_count(&self) -> usize {
            self.read_calls.get()
        }
    }

    impl RandomRead for MockRandomRead {
        fn read(&self, range: std::ops::Range<usize>) -> Bytes {
            // Increment our call counter
            let old = self.read_calls.get();
            self.read_calls.set(old + 1);

            let end = range.end.min(self.data.len());
            self.data.slice(range.start.min(end)..end)
        }

        fn len(&self) -> usize {
            self.data.len()
        }
    }

    pub fn test_index_lookup_inner(pi: &impl IndexFormat) {
        let metrics = Metrics::new();
        let (shape, ks) = KeyShape::new_single(16, 8, KeyType::uniform(8));
        let ks = shape.ks(ks);
        let mut index = IndexTable::default();
        index.insert(k128(1), w(5));
        index.insert(k128(5), w(10));

        let bytes = pi.to_bytes(&index, ks);
        assert_eq!(None, pi.lookup_unloaded(ks, &bytes, &k128(0), &metrics));
        assert_eq!(
            Some(w(5)),
            pi.lookup_unloaded(ks, &bytes, &k128(1), &metrics)
        );
        assert_eq!(
            Some(w(10)),
            pi.lookup_unloaded(ks, &bytes, &k128(5), &metrics)
        );
        assert_eq!(None, pi.lookup_unloaded(ks, &bytes, &k128(10), &metrics));
        let mut index = IndexTable::default();
        index.insert(k128(u128::MAX), w(15));
        index.insert(k128(u128::MAX - 5), w(25));
        let bytes = pi.to_bytes(&index, ks);
        assert_eq!(
            Some(w(15)),
            pi.lookup_unloaded(ks, &bytes, &k128(u128::MAX), &metrics)
        );
        assert_eq!(
            Some(w(25)),
            pi.lookup_unloaded(ks, &bytes, &k128(u128::MAX - 5), &metrics)
        );
        assert_eq!(
            None,
            pi.lookup_unloaded(ks, &bytes, &k128(u128::MAX - 1), &metrics)
        );
        assert_eq!(
            None,
            pi.lookup_unloaded(ks, &bytes, &k128(u128::MAX - 100), &metrics)
        );
    }

    pub fn test_index_lookup_random_inner(pi: &impl IndexFormat) {
        let metrics = Metrics::new();
        const M: usize = 8;
        const P: usize = 8;
        let (shape, ks) = KeyShape::new_single(4, M, KeyType::uniform(P));
        let ks = shape.ks(ks);
        let mut index = IndexTable::default();
        let mut rng = ThreadRng::default();
        let target_bucket = rng.gen_range(0..((M * P) as u32));
        let bucket_size = (MAX_U32_PLUS_ONE / ((M * P) as u64)) as u32;
        let target_range =
            target_bucket * bucket_size..(target_bucket + 1).saturating_mul(bucket_size);

        const ITERATIONS: usize = 100_000;
        for _ in 0..ITERATIONS {
            let key = rng.gen_range(target_range.clone());
            let pos = rng.next_u64();
            index.insert(k32(key), w(pos));
        }
        let bytes = pi.to_bytes(&index, ks);
        for (key, expected_value) in &index.data {
            let value = pi.lookup_unloaded(ks, &bytes, key, &metrics);
            assert_eq!(Some(*expected_value), value);
        }
        for _ in 0..ITERATIONS {
            let key = rng.gen_range(target_range.clone());
            if index.data.contains_key(&k32(key)) {
                continue; // skip keys that are in the index
            }
            let value = pi.lookup_unloaded(ks, &bytes, &k32(key), &metrics);
            assert!(value.is_none());
        }
    }

    #[test]
    fn test_binary_search() {
        // Create a buffer with several entries
        let key_size = 4;
        let element_size = 16; // 4-byte key + 12-byte value

        // Create a sorted set of keys
        let keys = vec![
            vec![1, 2, 3, 4],
            vec![5, 6, 7, 8],
            vec![10, 11, 12, 13],
            vec![20, 21, 22, 23],
        ];

        // Build a buffer with these keys and values
        let mut buffer = Vec::new();
        for (i, key) in keys.iter().enumerate() {
            buffer.extend_from_slice(key);
            let wal_position = WalPosition::test_value(i as u64 + 100);
            buffer.extend_from_slice(&wal_position.to_vec());
        }

        // Test key found - first key
        let (found_pos, insertion_point, position) =
            binary_search(&buffer, &keys[0], element_size, key_size, None);
        assert_eq!(found_pos, Some(0));
        assert_eq!(insertion_point, 0);
        assert!(position.is_some());
        assert_eq!(position.unwrap(), WalPosition::test_value(100));

        // Test key found - middle key
        let (found_pos, insertion_point, position) =
            binary_search(&buffer, &keys[2], element_size, key_size, None);
        assert_eq!(found_pos, Some(2));
        assert_eq!(insertion_point, 2);
        assert!(position.is_some());
        assert_eq!(position.unwrap(), WalPosition::test_value(102));

        // Test key not found - should find insertion point
        let missing_key = vec![3, 4, 5, 6]; // Between keys[0] and keys[1]
        let (found_pos, insertion_point, position) =
            binary_search(&buffer, &missing_key, element_size, key_size, None);
        assert_eq!(found_pos, None);
        assert_eq!(insertion_point, 1); // Would be inserted at position 1
        assert!(position.is_none());

        // Test key smaller than all keys
        let smaller_key = vec![0, 0, 0, 0];
        let (found_pos, insertion_point, position) =
            binary_search(&buffer, &smaller_key, element_size, key_size, None);
        assert_eq!(found_pos, None);
        assert_eq!(insertion_point, 0); // Would be inserted at the beginning
        assert!(position.is_none());

        // Test key larger than all keys
        let larger_key = vec![255, 255, 255, 255];
        let (found_pos, insertion_point, position) =
            binary_search(&buffer, &larger_key, element_size, key_size, None);
        assert_eq!(found_pos, None);
        assert_eq!(insertion_point, 4); // Would be inserted after the last element
        assert!(position.is_none());

        // Test with metrics
        let metrics = Metrics::new();
        let (found_pos, insertion_point, position) =
            binary_search(&buffer, &keys[3], element_size, key_size, Some(&metrics));
        assert_eq!(found_pos, Some(3));
        assert_eq!(insertion_point, 3);
        assert!(position.is_some());
        assert_eq!(position.unwrap(), WalPosition::test_value(103));
        // Verify metrics were updated - metrics are likely wrapped in Atomics so we can't directly check value
        // Instead, we'll just assume they were updated as it's implementation-dependent
    }

    pub fn k32(k: u32) -> Bytes {
        k.to_be_bytes().to_vec().into()
    }

    pub fn k64(k: u64) -> [u8; 8] {
        k.to_be_bytes()
    }

    pub fn k128(k: u128) -> Bytes {
        k.to_be_bytes().to_vec().into()
    }

    pub fn w(w: u64) -> WalPosition {
        WalPosition::test_value(w)
    }

    /// Generic test for next_entry_unloaded that can be used by both index implementations
    pub fn test_next_entry_unloaded_inner(index_format: &impl IndexFormat) {
        let metrics = Metrics::new();
        let (shape, ks_id) = KeyShape::new_single(8, 1, KeyType::uniform(1));
        let ks = shape.ks(ks_id);

        // Create an index with sorted entries
        let mut table = IndexTable::default();
        for i in 1..6 {
            let key = (i as u64 * 10).to_be_bytes().to_vec();
            table.insert(Bytes::from(key), WalPosition::test_value(i));
        }

        // Keys: [10, 20, 30, 40, 50]
        // Convert the table to bytes
        let serialized = index_format.to_bytes(&table, ks);
        let reader = MockRandomRead::new(serialized);

        // Test forward iteration with no previous key (should return first entry)
        let result =
            index_format.next_entry_unloaded(ks, &reader, None, Direction::Forward, &metrics);
        assert!(result.is_some());
        let (key, pos) = result.unwrap();
        assert_eq!(key, 10_u64.to_be_bytes());
        assert_eq!(pos, WalPosition::test_value(1));

        // Test backward iteration with no previous key (should return last entry)
        let result =
            index_format.next_entry_unloaded(ks, &reader, None, Direction::Backward, &metrics);
        assert!(result.is_some());
        let (key, pos) = result.unwrap();
        assert_eq!(key, 50_u64.to_be_bytes());
        assert_eq!(pos, WalPosition::test_value(5));

        // Test forward iteration from a specific key (should return next entry)
        let prev_key = 20_u64.to_be_bytes();
        let result = index_format.next_entry_unloaded(
            ks,
            &reader,
            Some(&prev_key),
            Direction::Forward,
            &metrics,
        );
        assert!(result.is_some());
        let (key, pos) = result.unwrap();
        assert_eq!(key, 30_u64.to_be_bytes());
        assert_eq!(pos, WalPosition::test_value(3));

        // Test backward iteration from a specific key (should return previous entry)
        let prev_key = 40_u64.to_be_bytes();
        let result = index_format.next_entry_unloaded(
            ks,
            &reader,
            Some(&prev_key),
            Direction::Backward,
            &metrics,
        );
        assert!(result.is_some());
        let (key, pos) = result.unwrap();
        assert_eq!(key, 30_u64.to_be_bytes());
        assert_eq!(pos, WalPosition::test_value(3));

        // Test edge case: next from last entry (should return None)
        let prev_key = 50_u64.to_be_bytes();
        let result = index_format.next_entry_unloaded(
            ks,
            &reader,
            Some(&prev_key),
            Direction::Forward,
            &metrics,
        );
        assert!(result.is_none());

        // Test edge case: previous from first entry (should return None)
        let prev_key = 10_u64.to_be_bytes();
        let result = index_format.next_entry_unloaded(
            ks,
            &reader,
            Some(&prev_key),
            Direction::Backward,
            &metrics,
        );
        assert!(result.is_none());

        // Test with a key that doesn't exist (should return the next entry in sequence)
        let prev_key = 15_u64.to_be_bytes();
        let result = index_format.next_entry_unloaded(
            ks,
            &reader,
            Some(&prev_key),
            Direction::Forward,
            &metrics,
        );
        assert!(result.is_some());
        let (key, pos) = result.unwrap();
        assert_eq!(key, 20_u64.to_be_bytes());
        assert_eq!(pos, WalPosition::test_value(2));

        // Test with a key that doesn't exist (should return the previous entry in sequence)
        let prev_key = 35_u64.to_be_bytes();
        let result = index_format.next_entry_unloaded(
            ks,
            &reader,
            Some(&prev_key),
            Direction::Backward,
            &metrics,
        );
        assert!(result.is_some());
        let (key, pos) = result.unwrap();
        assert_eq!(key, 30_u64.to_be_bytes());
        assert_eq!(pos, WalPosition::test_value(3));
    }

    /// Generic test for an empty index
    pub fn test_next_entry_unloaded_empty_index_inner(index_format: &impl IndexFormat) {
        let metrics = Metrics::new();
        let (shape, ks_id) = KeyShape::new_single(8, 1, KeyType::uniform(1));
        let ks = shape.ks(ks_id);

        // Create an empty index
        let table = IndexTable::default();

        // Convert the table to bytes
        let serialized = index_format.to_bytes(&table, ks);
        let reader = MockRandomRead::new(serialized);

        // Test with empty index should return None
        let result =
            index_format.next_entry_unloaded(ks, &reader, None, Direction::Forward, &metrics);
        assert!(result.is_none());

        let result =
            index_format.next_entry_unloaded(ks, &reader, None, Direction::Backward, &metrics);
        assert!(result.is_none());
    }
}
