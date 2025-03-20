use std::time::Instant;

use minibytes::Bytes;

use super::lookup_header::LookupHeaderIndex;
use super::uniform_lookup::UniformLookupIndex;
use crate::metrics::Metrics;
use crate::wal::WalPosition;
use crate::{index::index_table::IndexTable, key_shape::KeySpaceDesc, lookup::RandomRead};

pub const HEADER_ELEMENTS: usize = 1024;
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

    fn element_size(ks: &KeySpaceDesc) -> usize {
        ks.reduced_key_size() + WalPosition::LENGTH
    }

    fn use_unbounded_reader(&self) -> bool;
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

    fn use_unbounded_reader(&self) -> bool {
        match self {
            IndexFormatType::Header => LookupHeaderIndex.use_unbounded_reader(),
            IndexFormatType::Uniform(index) => index.use_unbounded_reader(),
        }
    }
}

/// Performs a binary search on a buffer of key-value entries to find a match for the given key.
/// Each entry consists of a key followed by a WalPosition.
///
/// # Arguments
/// * `buffer` - The buffer containing the entries
/// * `key` - The key to search for
/// * `element_size` - The size of each full entry (key + position)
/// * `key_size` - The size of just the key portion
/// * `metrics` - Metrics to track performance
///
/// # Returns
/// * `Some(WalPosition)` if key is found
/// * `None` if key is not found
pub fn binary_search_entries(
    buffer: &[u8],
    key: &[u8],
    element_size: usize,
    key_size: usize,
    metrics: &Metrics,
) -> Option<WalPosition> {
    if buffer.is_empty() {
        return None;
    }

    let scan_start = Instant::now();
    let count = buffer.len() / element_size;
    if count == 0 {
        return None;
    }

    let mut left = 0;
    let mut right = count; // one past the last valid index
    while left < right {
        let mid = (left + right) / 2;
        let entry_offset = mid * element_size;
        let k = &buffer[entry_offset..entry_offset + key_size];

        match k.cmp(key) {
            std::cmp::Ordering::Less => {
                left = mid + 1;
            }
            std::cmp::Ordering::Greater => {
                right = mid;
            }
            std::cmp::Ordering::Equal => {
                // parse wal position
                let mut pos_slice =
                    &buffer[(entry_offset + key_size)..(entry_offset + element_size)];
                let position = WalPosition::read_from_buf(&mut pos_slice);
                metrics
                    .lookup_scan_mcs
                    .inc_by(scan_start.elapsed().as_micros() as u64);
                return Some(position);
            }
        }
    }

    // Not found
    metrics
        .lookup_scan_mcs
        .inc_by(scan_start.elapsed().as_micros() as u64);
    None
}

#[cfg(test)]
pub mod test {
    use minibytes::Bytes;
    use rand::{rngs::ThreadRng, Rng, RngCore};

    use crate::key_shape::KeyType;
    use crate::metrics::Metrics;
    use crate::{
        index::index_format::IndexFormat, index::index_table::IndexTable, key_shape::KeyShape,
        wal::WalPosition,
    };

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
        let bucket_size = u32::MAX / ((M * P) as u32);
        let target_range = target_bucket * bucket_size..(target_bucket + 1) * bucket_size;
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
}
