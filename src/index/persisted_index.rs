use minibytes::Bytes;

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
    ) -> Option<WalPosition>;

    fn element_size(ks: &KeySpaceDesc) -> usize {
        ks.reduced_key_size() + WalPosition::LENGTH
    }
}

#[cfg(test)]
pub mod test {
    use minibytes::Bytes;
    use rand::{rngs::ThreadRng, Rng, RngCore};

    use crate::{
        index::index_table::IndexTable, index::persisted_index::IndexFormat, key_shape::KeyShape,
        wal::WalPosition,
    };

    pub fn test_index_lookup_inner(pi: &impl IndexFormat) {
        let (shape, ks) = KeyShape::new_single(16, 8, 8);
        let ks = shape.ks(ks);
        let mut index = IndexTable::default();
        index.insert(k128(1), w(5));
        index.insert(k128(5), w(10));

        let bytes = pi.to_bytes(&index, ks);
        assert_eq!(None, pi.lookup_unloaded(ks, &bytes, &k128(0)));
        assert_eq!(Some(w(5)), pi.lookup_unloaded(ks, &bytes, &k128(1)));
        assert_eq!(Some(w(10)), pi.lookup_unloaded(ks, &bytes, &k128(5)));
        assert_eq!(None, pi.lookup_unloaded(ks, &bytes, &k128(10)));
        let mut index = IndexTable::default();
        index.insert(k128(u128::MAX), w(15));
        index.insert(k128(u128::MAX - 5), w(25));
        let bytes = pi.to_bytes(&index, ks);
        assert_eq!(
            Some(w(15)),
            pi.lookup_unloaded(ks, &bytes, &k128(u128::MAX))
        );
        assert_eq!(
            Some(w(25)),
            pi.lookup_unloaded(ks, &bytes, &k128(u128::MAX - 5))
        );
        assert_eq!(None, pi.lookup_unloaded(ks, &bytes, &k128(u128::MAX - 1)));
        assert_eq!(None, pi.lookup_unloaded(ks, &bytes, &k128(u128::MAX - 100)));
    }

    pub fn test_index_lookup_random_inner(pi: &impl IndexFormat) {
        const M: usize = 8;
        const P: usize = 8;
        let (shape, ks) = KeyShape::new_single(4, M, P);
        let ks = shape.ks(ks);
        let mut index = IndexTable::default();
        let mut rng = ThreadRng::default();
        let target_bucket = rng.gen_range(0..((M * P) as u32));
        let bucket_size = u32::MAX / ((M * P) as u32);
        let target_range = target_bucket * bucket_size..(target_bucket + 1) * bucket_size;
        const ITERATIONS: usize = 1000;
        for _ in 0..ITERATIONS {
            let key = rng.gen_range(target_range.clone());
            let pos = rng.next_u64();
            index.insert(k32(key), w(pos));
        }
        let bytes = pi.to_bytes(&index, ks);
        for (key, expected_value) in index.data {
            let value = pi.lookup_unloaded(ks, &bytes, &key);
            assert_eq!(Some(expected_value), value);
        }
        for _ in 0..ITERATIONS {
            let key = rng.gen_range(target_range.clone());
            let value = pi.lookup_unloaded(ks, &bytes, &k32(key));
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
