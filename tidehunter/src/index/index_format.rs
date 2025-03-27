use std::sync::Arc;

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
        metrics: Arc<Metrics>,
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
        metrics: Arc<Metrics>,
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
        assert_eq!(
            None,
            pi.lookup_unloaded(ks, &bytes, &k128(0), metrics.clone())
        );
        assert_eq!(
            Some(w(5)),
            pi.lookup_unloaded(ks, &bytes, &k128(1), metrics.clone())
        );
        assert_eq!(
            Some(w(10)),
            pi.lookup_unloaded(ks, &bytes, &k128(5), metrics.clone())
        );
        assert_eq!(
            None,
            pi.lookup_unloaded(ks, &bytes, &k128(10), metrics.clone())
        );
        let mut index = IndexTable::default();
        index.insert(k128(u128::MAX), w(15));
        index.insert(k128(u128::MAX - 5), w(25));
        let bytes = pi.to_bytes(&index, ks);
        assert_eq!(
            Some(w(15)),
            pi.lookup_unloaded(ks, &bytes, &k128(u128::MAX), metrics.clone())
        );
        assert_eq!(
            Some(w(25)),
            pi.lookup_unloaded(ks, &bytes, &k128(u128::MAX - 5), metrics.clone())
        );
        assert_eq!(
            None,
            pi.lookup_unloaded(ks, &bytes, &k128(u128::MAX - 1), metrics.clone())
        );
        assert_eq!(
            None,
            pi.lookup_unloaded(ks, &bytes, &k128(u128::MAX - 100), metrics.clone())
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
            let value = pi.lookup_unloaded(ks, &bytes, key, metrics.clone());
            assert_eq!(Some(*expected_value), value);
        }
        for _ in 0..ITERATIONS {
            let key = rng.gen_range(target_range.clone());
            if index.data.contains_key(&k32(key)) {
                continue; // skip keys that are in the index
            }
            let value = pi.lookup_unloaded(ks, &bytes, &k32(key), metrics.clone());
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
