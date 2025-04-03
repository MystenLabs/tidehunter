use crate::cell::CellId;
use crate::db::MAX_KEY_LEN;
use crate::index::index_format::IndexFormatType;
use crate::math;
use crate::math::{downscale_u32, starting_u32};
use crate::wal::WalPosition;
use minibytes::Bytes;
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::ops::{Deref, Range, RangeInclusive};
use std::sync::Arc;

pub(crate) const CELL_PREFIX_LENGTH: usize = 4; // in bytes
pub(crate) const MAX_U32_PLUS_ONE: u64 = u32::MAX as u64 + 1;

#[derive(Clone)]
pub struct KeyShape {
    key_spaces: Vec<KeySpaceDesc>,
}

pub struct KeyShapeBuilder {
    key_spaces: Vec<KeySpaceDesc>,
}

#[derive(Clone, Copy)]
pub struct KeySpace(pub(crate) u8);

#[doc(hidden)]
#[derive(Clone)]
pub struct KeySpaceDesc {
    inner: Arc<KeySpaceDescInner>,
}

#[doc(hidden)]
pub struct KeySpaceDescInner {
    id: KeySpace,
    name: String,
    key_size: usize,
    mutexes: usize,
    key_type: KeyType,
    config: KeySpaceConfig,
}

#[derive(Default, Clone)]
pub struct KeySpaceConfig {
    key_offset: usize,
    compactor: Option<Arc<Compactor>>,
    disable_unload: bool,
    bloom_filter: Option<BloomFilterParams>,
    value_cache_size: usize,
    key_reduction: Option<Range<usize>>,
    index_format: IndexFormatType,
}

#[derive(Clone, Copy)]
pub enum KeyType {
    Uniform(UniformKeyConfig),
    PrefixedUniform(PrefixedUniformKeyConfig),
}

#[derive(Clone, Copy)]
pub struct UniformKeyConfig {
    cells_per_mutex: usize,
}

#[derive(Clone, Copy)]
pub struct PrefixedUniformKeyConfig {
    /// First prefix_len_bytes of a key considered a 'prefix'
    prefix_len_bytes: usize,
    /// The last cluster_bits of the prefix are set to zero to "cluster"
    /// multiple prefixes into the same cell
    #[allow(dead_code)]
    cluster_bits: usize,
    /// Mask that has all ones followed by cluster_bits zeroes at the end.
    /// E.g. for cluster_bits=2 the mask is 1111_1100
    reset_mask: u8,
}

#[derive(Default, Clone)]
pub(crate) struct BloomFilterParams {
    pub rate: f32,
    pub count: u32,
}

// todo - we want better compactor API that does not expose too much internal details
// todo - make mod wal private
pub type Compactor = Box<dyn Fn(&mut BTreeMap<Bytes, WalPosition>) + Sync + Send>;

#[allow(dead_code)]
impl KeyShapeBuilder {
    pub fn new() -> Self {
        Self { key_spaces: vec![] }
    }

    pub fn add_key_space(
        &mut self,
        name: impl Into<String>,
        key_size: usize,
        mutexes: usize,
        key_type: KeyType,
    ) -> KeySpace {
        self.add_key_space_config(name, key_size, mutexes, key_type, KeySpaceConfig::default())
    }

    pub fn add_key_space_config(
        &mut self,
        name: impl Into<String>,
        key_size: usize,
        mutexes: usize,
        key_type: KeyType,
        config: KeySpaceConfig,
    ) -> KeySpace {
        let name = name.into();
        assert!(mutexes > 0, "mutexes should be greater then 0");

        assert!(
            self.key_spaces.len() < (u8::MAX - 1) as usize,
            "Maximum {} key spaces allowed",
            u8::MAX
        );
        assert!(
            key_size <= MAX_KEY_LEN,
            "Specified key size exceeding max key length"
        );

        let ks = KeySpace(self.key_spaces.len() as u8);
        let key_space = KeySpaceDescInner {
            id: ks,
            name,
            key_size,
            mutexes,
            key_type,
            config,
        };
        let key_space = KeySpaceDesc {
            inner: Arc::new(key_space),
        };
        self.key_spaces.push(key_space);
        ks
    }

    pub fn build(self) -> KeyShape {
        self.self_check();
        KeyShape {
            key_spaces: self.key_spaces,
        }
    }

    fn self_check(&self) {
        for ks in &self.key_spaces {
            if ks.config.bloom_filter.is_some() && ks.config.compactor.is_some() {
                panic!("Tidehunter currently does not support key space with both compactor and bloom filter enabled");
            }
            ks.key_type
                .verify_key_size(ks.reduced_key_size() - ks.config.key_offset);
        }
    }
}

impl KeySpaceDesc {
    pub(crate) fn check_key(&self, k: &[u8]) {
        if k.len() != self.key_size {
            panic!(
                "Key space {} accepts keys size {}, given {}",
                self.name,
                self.key_size,
                k.len()
            );
        }
    }

    /* Nomenclature for the various conversion methods below:
     * **Location** is a tuple (mutex, offset) identifying the cell.
     * **Cell** is a single usize identifying the cell.
     * **Key** is a full key(u8 slice).
     * **Prefix** is u32 representing a 4-byte prefix of the key used to map key to its cell.
     */

    pub(crate) fn mutex_for_cell(&self, cell: &CellId) -> usize {
        cell.mutex_seed() % self.num_mutexes()
    }

    pub(crate) fn next_mutex(&self, mutex: usize, reverse: bool) -> Option<usize> {
        math::next_bounded(mutex, self.num_mutexes(), reverse)
    }

    // Reverse of locate_cell
    pub(crate) fn cell_by_location(&self, row: usize, offset: usize) -> usize {
        offset * self.num_mutexes() + row
    }

    pub fn num_mutexes(&self) -> usize {
        self.mutexes
    }

    pub(crate) fn first_cell(&self) -> CellId {
        self.key_type.first_cell()
    }

    pub(crate) fn key_reduction(&self) -> &Option<Range<usize>> {
        &self.config.key_reduction
    }

    pub(crate) fn key_type(&self) -> &KeyType {
        &self.key_type
    }

    pub(crate) fn reduced_key_size(&self) -> usize {
        if let Some(key_reduction) = &self.config.key_reduction {
            key_reduction.len()
        } else {
            self.key_size
        }
    }

    /// Returns u32 prefix
    pub(crate) fn index_prefix_u32(&self, k: &[u8]) -> u32 {
        starting_u32(self.index_prefix(k))
    }

    fn index_prefix<'a>(&self, k: &'a [u8]) -> &'a [u8] {
        self.key_type.index_prefix(&k[self.config.key_offset..])
    }

    pub(crate) fn cell_id(&self, k: &[u8]) -> CellId {
        let k = &k[self.config.key_offset..];
        match self.key_type {
            KeyType::Uniform(config) => {
                let starting_u32 = starting_u32(k);
                let num_cells = config.num_cells(self) as u32;
                let cell = downscale_u32(starting_u32, num_cells) as usize;
                CellId::Integer(cell)
            }
            KeyType::PrefixedUniform(config) => {
                let mut prefix = SmallVec::from(&k[..config.prefix_len_bytes]);
                let cluster_byte_ref = &mut prefix[config.prefix_len_bytes - 1];
                *cluster_byte_ref = config.cluster_bits(*cluster_byte_ref);
                CellId::Bytes(prefix)
            }
        }
    }

    pub(crate) fn index_prefix_range(&self, cell: &CellId) -> Range<u64> {
        match (self.key_type(), cell) {
            (KeyType::Uniform(config), CellId::Integer(cell)) => {
                let cell = *cell as u64;
                let num_cells = config.num_cells(self) as u64;
                // If you have only 1 cell, it has u32::MAX+1 elements,
                let cell_size = MAX_U32_PLUS_ONE / num_cells;
                cell * cell_size..((cell + 1) * cell_size)
            }
            (KeyType::PrefixedUniform(prefix_config), CellId::Bytes(cell)) => {
                // CellId can not be empty
                prefix_config.prefix_range(cell[cell.len() - 1])
            }
            (KeyType::Uniform(_), CellId::Bytes(_)) => {
                panic!("index_prefix_range called for uniform key type and bytes cell id")
            }
            (KeyType::PrefixedUniform(_), CellId::Integer(_)) => {
                panic!("index_prefix_range called for prefix key type and integer cell id")
            }
        }
    }

    /// Returns the cell containing the range.
    /// Right now, this only works if the entire range "fits" single cell.
    pub(crate) fn range_cell(&self, from_included: &[u8], to_included: &[u8]) -> CellId {
        let start_prefix = self.cell_id(&from_included);
        let end_prefix = self.cell_id(&to_included);
        if start_prefix == end_prefix {
            end_prefix
        } else {
            panic!("Can't have ordered iterator over key range that does not fit same large table cell");
        }
    }

    pub(crate) fn reduce_key<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        if let Some(key_reduction) = &self.config.key_reduction {
            &key[key_reduction.clone()]
        } else {
            key
        }
    }

    pub(crate) fn reduced_key_bytes(&self, key: Bytes) -> Bytes {
        if let Some(key_reduction) = &self.config.key_reduction {
            key.slice(key_reduction.clone())
        } else {
            key
        }
    }

    pub(crate) fn compactor(&self) -> Option<&Compactor> {
        self.config.compactor.as_ref().map(Arc::as_ref)
    }

    pub(crate) fn bloom_filter(&self) -> Option<&BloomFilterParams> {
        self.config.bloom_filter.as_ref()
    }

    pub(crate) fn value_cache_size(&self) -> Option<NonZeroUsize> {
        NonZeroUsize::new(self.config.value_cache_size)
    }

    pub(crate) fn unloading_disabled(&self) -> bool {
        self.config.disable_unload
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub fn id(&self) -> KeySpace {
        self.id
    }

    pub(crate) fn index_format(&self) -> &IndexFormatType {
        &self.config.index_format
    }
}

impl KeySpaceConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_key_offset(key_offset: usize) -> Self {
        Self {
            key_offset,
            ..Self::default()
        }
    }

    pub fn with_compactor(mut self, compactor: Compactor) -> Self {
        self.compactor = Some(Arc::new(compactor));
        self
    }

    pub fn disable_unload(mut self) -> Self {
        self.disable_unload = true;
        self
    }

    pub fn with_bloom_filter(mut self, rate: f32, count: u32) -> Self {
        self.bloom_filter = Some(BloomFilterParams { rate, count });
        self
    }

    pub fn with_value_cache_size(mut self, size: usize) -> Self {
        self.value_cache_size = size;
        self
    }

    pub fn with_key_reduction(mut self, key_reduction: Range<usize>) -> Self {
        self.key_reduction = Some(key_reduction);
        self
    }

    pub fn with_index_format(mut self, index_format: IndexFormatType) -> Self {
        self.index_format = index_format;
        self
    }
}

impl KeyShape {
    pub fn new_single(key_size: usize, mutexes: usize, key_type: KeyType) -> (Self, KeySpace) {
        Self::new_single_config(key_size, mutexes, key_type, Default::default())
    }

    pub fn new_single_config(
        key_size: usize,
        mutexes: usize,
        key_type: KeyType,
        config: KeySpaceConfig,
    ) -> (Self, KeySpace) {
        let key_space = KeySpaceDescInner {
            id: KeySpace(0),
            name: "root".into(),
            key_size,
            mutexes,
            key_type,
            config,
        };
        let key_space = KeySpaceDesc {
            inner: Arc::new(key_space),
        };
        let key_spaces = vec![key_space];
        let this = Self { key_spaces };
        (this, KeySpace(0))
    }

    pub(crate) fn iter_ks(&self) -> impl Iterator<Item = &KeySpaceDesc> + '_ {
        self.key_spaces.iter()
    }

    pub(crate) fn num_ks(&self) -> usize {
        self.key_spaces.len()
    }

    pub(crate) fn range_cell(
        &self,
        ks: KeySpace,
        from_included: &[u8],
        to_included: &[u8],
    ) -> CellId {
        self.ks(ks).range_cell(from_included, to_included)
    }

    #[doc(hidden)]
    pub fn ks(&self, ks: KeySpace) -> &KeySpaceDesc {
        let Some(key_space) = self.key_spaces.get(ks.0 as usize) else {
            panic!("Key space {} not found", ks.0)
        };
        key_space
    }
}

impl KeySpace {
    pub(crate) fn as_usize(&self) -> usize {
        self.0 as usize
    }

    #[cfg(test)]
    pub fn new_test(v: u8) -> Self {
        Self(v)
    }
}

impl Deref for KeySpaceDesc {
    type Target = KeySpaceDescInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl KeyType {
    pub fn uniform(cells_per_mutex: usize) -> Self {
        Self::Uniform(UniformKeyConfig::new(cells_per_mutex))
    }

    pub fn prefix_uniform(prefix_len_bytes: usize, cluster_bits: usize) -> Self {
        Self::PrefixedUniform(PrefixedUniformKeyConfig::new(
            prefix_len_bytes,
            cluster_bits,
        ))
    }

    fn verify_key_size(&self, key_size: usize) {
        match self {
            KeyType::Uniform(_) => {}
            KeyType::PrefixedUniform(config) => {
                assert!(
                    key_size > config.prefix_len_bytes,
                    "key_size({}) must be greater then prefix len({})",
                    key_size,
                    config.prefix_len_bytes
                );
            }
        }
    }

    fn first_cell(&self) -> CellId {
        match self {
            KeyType::Uniform(_) => CellId::Integer(0),
            KeyType::PrefixedUniform(config) => {
                let bytes = SmallVec::from_elem(0, config.prefix_len_bytes);
                CellId::Bytes(bytes)
            }
        }
    }

    fn index_prefix<'a>(&self, k: &'a [u8]) -> &'a [u8] {
        match self {
            KeyType::Uniform(_) => k,
            KeyType::PrefixedUniform(config) => &k[config.discard_prefix_bytes()..],
        }
    }
}

impl UniformKeyConfig {
    pub fn new(cells_per_mutex: usize) -> Self {
        assert!(
            cells_per_mutex > 0,
            "cells_per_mutex should be greater then 0"
        );
        Self { cells_per_mutex }
    }

    pub(crate) fn cells_per_mutex(&self) -> usize {
        self.cells_per_mutex
    }

    pub(crate) fn num_cells(&self, ksd: &KeySpaceDesc) -> usize {
        self.cells_per_mutex * ksd.num_mutexes()
    }

    pub(crate) fn next_cell(
        &self,
        ksd: &KeySpaceDesc,
        cell: usize,
        reverse: bool,
    ) -> Option<CellId> {
        let next = math::next_bounded(cell, self.num_cells(ksd), reverse);
        next.map(CellId::Integer)
    }
}

impl PrefixedUniformKeyConfig {
    pub fn new(prefix_len_bytes: usize, cluster_bits: usize) -> Self {
        assert!(
            prefix_len_bytes > 0,
            "prefix_len_bytes must be greater then zero, otherwise Uniform key type must be used"
        );
        let reset_mask = Self::make_reset_mask(cluster_bits);
        Self {
            prefix_len_bytes,
            cluster_bits,
            reset_mask,
        }
    }

    fn make_reset_mask(cluster_bits: usize) -> u8 {
        assert!(
            cluster_bits < 8,
            "cluster_bits must be less then 8, reduce prefix_len_bytes otherwise"
        );
        u8::MAX - ((1 << cluster_bits) - 1)
    }

    /// Returns number of first bytes that are the same across all keys in the cell.
    /// If configuration has no cluster bits, this is equal to the length of the prefix.
    /// If configuration has cluster bits, this is equal to the length of the prefix minus one.
    /// In the latter case, one is subtracted because
    /// the last byte of prefix can be different for the same cell.
    fn discard_prefix_bytes(&self) -> usize {
        if self.has_cluster_bits() {
            self.prefix_len_bytes - 1
        } else {
            self.prefix_len_bytes
        }
    }

    /// See explanation on prefix_range_u8 for details.
    #[inline(always)]
    fn cluster_bits(&self, v: u8) -> u8 {
        v & self.reset_mask
    }

    /// Returns whether config has cluster bits.
    #[inline(always)]
    fn has_cluster_bits(&self) -> bool {
        // reset_mask == u8::MAX equivalent to cluster_bits == 0
        self.reset_mask != u8::MAX
    }

    fn prefix_range(&self, last_byte: u8) -> Range<u64> {
        if self.has_cluster_bits() {
            let range_u8 = self.prefix_range_u8(last_byte);
            let start_inclusive = (*range_u8.start() as u64) << 24;
            let end_inclusive = (*range_u8.end() as u64) << 24;
            let end_exclusive = end_inclusive + 1;
            start_inclusive..end_exclusive
        } else {
            0..MAX_U32_PLUS_ONE
        }
    }

    /// Returns range of u8 values for which all keys sharing the same cluster byte will fall into.
    ///
    /// This function has invariant (I) that for any value x
    /// prefix_range_u8(x).contains(x)
    ///
    /// Combined this function and cluster_bits have the following invariant (II):
    /// For any two values x and y,
    /// cluster_bits(x) == cluster_bits(y) if and only if
    /// prefix_range_u8(x) == prefix_range_u8(y)
    ///
    /// In other words, all bytes falling within the same prefix_range_u8 share the same cluster bits.
    ///
    /// See prefix_range_u8 for examples.
    #[inline(always)]
    fn prefix_range_u8(&self, last_byte: u8) -> RangeInclusive<u8> {
        let min_byte = last_byte & self.reset_mask;
        // Mask that has all zeroes followed by cluster_bits ones at the end.
        // E.g., for cluster_bits=2 the compliment_mask is 0000_0011
        // This naturally turns out to be !self.reset_mask
        let compliment_mask = !self.reset_mask;
        let max_byte = min_byte + compliment_mask;
        min_byte..=max_byte
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell::CellIdBytesContainer;
    use hex_literal::hex;
    use std::collections::hash_map::Entry;
    use std::collections::HashMap;

    #[test]
    fn test_make_reset_mask() {
        let f = PrefixedUniformKeyConfig::make_reset_mask;
        assert_eq!(f(0), 0b1111_1111);
        assert_eq!(f(1), 0b1111_1110);
        assert_eq!(f(2), 0b1111_1100);
        assert_eq!(f(7), 0b1000_0000);
    }

    #[test]
    fn index_prefix_test() {
        let (key_shape, ks) = KeyShape::new_single(4, 2, KeyType::prefix_uniform(2, 0));
        let ks = key_shape.ks(ks);

        assert_eq!(c(&hex!("1234")), ks.cell_id(&hex!("12345678")));
        assert_eq!(&hex!("5678"), ks.index_prefix(&hex!("12345678")));
        let prefix = 0x56780000;
        assert_eq!(prefix, ks.index_prefix_u32(&hex!("12345678")));

        let cell_prefix_range = ks.index_prefix_range(&ks.cell_id(&hex!("12345678")));
        assert_eq!(cell_prefix_range.start, 0);
        assert_eq!(cell_prefix_range.end, 0x1_0000_0000);
    }

    #[test]
    fn test_prefix_range_for_prefix_key() {
        let config = PrefixedUniformKeyConfig::new(1, 0);
        assert_prefix_range_eq(0x0000_0000..0x1_0000_0000, config.prefix_range(111));
        let config = PrefixedUniformKeyConfig::new(1, 7);
        assert_prefix_range_eq(0x0000_0000..0x7f00_0001, config.prefix_range(0));
        assert_prefix_range_eq(0x0000_0000..0x7f00_0001, config.prefix_range(15));
        assert_prefix_range_eq(0x0000_0000..0x7f00_0001, config.prefix_range(0x7f));
        assert_prefix_range_eq(0x8000_0000..0xff00_0001, config.prefix_range(0x80));
        assert_prefix_range_eq(0x8000_0000..0xff00_0001, config.prefix_range(0xff));
    }

    #[track_caller]
    fn assert_prefix_range_eq(r1: Range<u64>, r2: Range<u64>) {
        if r1 != r2 {
            panic!(
                "{:08x}..{:08x} != {:08x}..{:08x}",
                r1.start, r1.end, r2.start, r2.end
            )
        }
    }

    #[test]
    fn test_prefix_range_u8() {
        for bits in 0..8 {
            let c = PrefixedUniformKeyConfig::new(1, bits);
            test_prefix_range_config(bits, &c);
        }
    }

    fn test_prefix_range_config(bits: usize, c: &PrefixedUniformKeyConfig) {
        // Assert invariants (I) and (II) for prefix_range_u8 (see docs)
        let mut ranges = HashMap::<u8, (usize, RangeInclusive<u8>)>::default();
        for v in 0..=u8::MAX {
            let range = c.prefix_range_u8(v);
            assert!(
                range.contains(&v),
                "prefix_range_u8 {range:?} for value {v} does not contain that value"
            );
            let cluster_id = c.cluster_bits(v);
            match ranges.entry(cluster_id) {
                Entry::Vacant(va) => {
                    va.insert((1, range));
                }
                Entry::Occupied(oc) => {
                    let (count, oc_range) = oc.into_mut();
                    assert_eq!(oc_range, &range);
                    *count += 1;
                }
            }
        }
        // Assert that configuration with n cluster bits splits space into 2^(8-n) chunks
        assert_eq!(2usize.pow((8 - bits) as u32), ranges.len(), "bits={bits}");
        // Assert that space split into equal 'chunks' of 2^n size
        let expected_count = 2usize.pow(bits as u32);
        for (_, (count, _)) in ranges {
            assert_eq!(expected_count, count, "bits={bits}");
        }
    }

    fn c(s: &[u8]) -> CellId {
        CellId::Bytes(CellIdBytesContainer::from(s))
    }
}
