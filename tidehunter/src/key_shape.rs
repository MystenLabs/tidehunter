use crate::cell::CellId;
use crate::control::ControlRegion;
use crate::crc::CrcFrame;
use crate::db::MAX_KEY_LEN;
use crate::math::{downscale_u32, starting_u32};
use crate::wal::WalPosition;
use minibytes::Bytes;
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::ops::{Deref, Range};
use std::sync::Arc;

pub(crate) const CELL_PREFIX_LENGTH: usize = 4; // in bytes

#[derive(Clone)]
pub struct KeyShape {
    key_spaces: Vec<KeySpaceDesc>,
}

#[allow(dead_code)]
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
    per_mutex: usize,
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
    key_type: KeyType,
}

#[derive(Clone, Copy)]
pub enum KeyType {
    Uniform,
    PrefixedUniform(PrefixedUniformKeyConfig),
}

#[derive(Clone, Copy)]
pub struct PrefixedUniformKeyConfig {
    /// First prefix_len_bytes of a key considered a 'prefix'
    prefix_len_bytes: usize,
    /// The last cluster_bits of the prefix are set to zero to "cluster"
    /// multiple prefixes into the same cell
    #[allow(dead_code)]
    cluster_bits: usize,
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
        per_mutex: usize,
    ) -> KeySpace {
        self.add_key_space_config(
            name,
            key_size,
            mutexes,
            per_mutex,
            KeySpaceConfig::default(),
        )
    }

    pub fn add_key_space_config(
        &mut self,
        name: impl Into<String>,
        key_size: usize,
        mutexes: usize,
        per_mutex: usize,
        config: KeySpaceConfig,
    ) -> KeySpace {
        let name = name.into();
        assert!(mutexes > 0, "mutexes should be greater then 0");
        assert!(per_mutex > 0, "per_mutex should be greater then 0");

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
            per_mutex,
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
            ks.config
                .key_type
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
    pub(crate) fn location_for_key(&self, k: &[u8]) -> (usize, CellId) {
        let cell = self.cell_id(k);
        let mutex = self.mutex_for_cell(&cell);
        (mutex, cell)
    }

    pub(crate) fn mutex_for_cell(&self, cell: &CellId) -> usize {
        cell.mutex_seed() % self.num_mutexes()
    }

    // Reverse of locate_cell
    pub(crate) fn cell_by_location(&self, row: usize, offset: usize) -> usize {
        offset * self.num_mutexes() + row
    }

    pub fn num_mutexes(&self) -> usize {
        self.mutexes
    }

    pub fn cells_per_mutex(&self) -> usize {
        self.per_mutex
    }

    pub fn next_cell(&self, cell: CellId, reverse: bool) -> Option<CellId> {
        let CellId::Integer(cell) = cell else {
            unimplemented!("next_cell for bytes")
        };
        let next = if reverse {
            cell.checked_sub(1)
        } else {
            if cell >= self.num_cells() - 1 {
                None
            } else {
                Some(cell + 1)
            }
        };
        next.map(CellId::Integer)
    }

    pub(crate) fn first_cell(&self) -> CellId {
        self.config.key_type.first_cell()
    }

    pub(crate) fn key_reduction(&self) -> &Option<Range<usize>> {
        &self.config.key_reduction
    }

    pub(crate) fn reduced_key_size(&self) -> usize {
        if let Some(key_reduction) = &self.config.key_reduction {
            key_reduction.len()
        } else {
            self.key_size
        }
    }

    pub(crate) fn num_cells(&self) -> usize {
        self.mutexes * self.per_mutex
    }

    // todo - rewrite to support prefix key in lookup
    pub(crate) fn cell_by_prefix(&self, prefix: u32) -> usize {
        let bucket = downscale_u32(prefix, self.num_cells() as u32) as usize;
        bucket
    }

    /// Returns u32 prefix
    pub(crate) fn index_prefix_u32(&self, k: &[u8]) -> u32 {
        starting_u32(self.index_prefix(k))
    }

    fn index_prefix<'a>(&self, k: &'a [u8]) -> &'a [u8] {
        self.config
            .key_type
            .index_prefix(&k[self.config.key_offset..])
    }

    pub(crate) fn cell_id(&self, k: &[u8]) -> CellId {
        let k = &k[self.config.key_offset..];
        match self.config.key_type {
            KeyType::Uniform => {
                let ending_u32 = starting_u32(k);
                let cell = downscale_u32(ending_u32, self.num_cells() as u32) as usize;
                CellId::Integer(cell)
            }
            KeyType::PrefixedUniform(config) => {
                let mut prefix = SmallVec::from(&k[..config.prefix_len_bytes]);
                prefix[config.prefix_len_bytes - 1] &= config.reset_mask;
                CellId::Bytes(prefix)
            }
        }
    }

    pub(crate) fn cell_prefix_range(&self, cell: usize) -> Range<u64> {
        let cell = cell as u64;
        let cell_size = self.cell_size();
        cell * cell_size..((cell + 1) * cell_size)
    }

    pub(crate) fn cell_size(&self) -> u64 {
        let cells = self.num_cells() as u64;
        // If you have only 1 cell, it has u32::MAX+1 elements,
        (u32::MAX as u64 + 1) / cells
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

    pub fn with_key_type(mut self, key_type: KeyType) -> Self {
        self.key_type = key_type;
        self
    }
}

impl KeyShape {
    pub fn new_single(key_size: usize, mutexes: usize, per_mutex: usize) -> (Self, KeySpace) {
        Self::new_single_config(key_size, mutexes, per_mutex, Default::default())
    }

    pub fn new_single_config(
        key_size: usize,
        mutexes: usize,
        per_mutex: usize,
        config: KeySpaceConfig,
    ) -> (Self, KeySpace) {
        let key_space = KeySpaceDescInner {
            id: KeySpace(0),
            name: "root".into(),
            key_size,
            mutexes,
            per_mutex,
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

    pub fn cr_len(&self) -> usize {
        ControlRegion::len_bytes_from_key_shape(self) + CrcFrame::CRC_HEADER_LENGTH
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
}

impl Deref for KeySpaceDesc {
    type Target = KeySpaceDescInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Default for KeyType {
    fn default() -> Self {
        KeyType::Uniform
    }
}

impl KeyType {
    fn verify_key_size(&self, key_size: usize) {
        match self {
            KeyType::Uniform => {}
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
            KeyType::Uniform => CellId::Integer(0),
            KeyType::PrefixedUniform(config) => {
                let bytes = SmallVec::from_elem(0, config.prefix_len_bytes);
                CellId::Bytes(bytes)
            }
        }
    }

    fn index_prefix<'a>(&self, k: &'a [u8]) -> &'a [u8] {
        match self {
            KeyType::Uniform => k,
            KeyType::PrefixedUniform(config) => &k[config.discard_prefix_bytes()..],
        }
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
        // reset_mask == u8::MAX equivalent to cluster_bits == 0
        if self.reset_mask == u8::MAX {
            self.prefix_len_bytes
        } else {
            self.prefix_len_bytes - 1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    //
    // #[test]
    // fn test_cell_by_location() {
    //     let ks = KeySpaceDescInner {
    //         id: KeySpace(0),
    //         name: "".to_string(),
    //         key_size: 0,
    //         mutexes: 128,
    //         per_mutex: 512,
    //         config: Default::default(),
    //     };
    //     let ks = KeySpaceDesc {
    //         inner: Arc::new(ks),
    //     };
    //     for cell in 0..1024usize {
    //         let (row, offset) = ks.location_for_cell(cell);
    //         let CellId::Integer(offset) = offset else {
    //             panic!("Unexpected cell id")
    //         };
    //         let evaluated_cell = ks.cell_by_location(row, offset);
    //         assert_eq!(evaluated_cell, cell);
    //     }
    // }

    #[test]
    fn test_make_reset_mask() {
        let f = PrefixedUniformKeyConfig::make_reset_mask;
        assert_eq!(f(0), 0b1111_1111);
        assert_eq!(f(1), 0b1111_1110);
        assert_eq!(f(2), 0b1111_1100);
        assert_eq!(f(7), 0b1000_0000);
    }
}
