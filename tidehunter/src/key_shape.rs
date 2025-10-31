use crate::cell::CellId;
use crate::db::MAX_KEY_LEN;
use crate::index::index_format::IndexFormatType;
use crate::index::index_table::IndexWalPosition;
use crate::math;
use crate::math::{downscale_u32, starting_u32, starting_u64};
use crate::relocation::RelocationFilter;
use crate::wal::position::WalPosition;
use blake2::Digest;
use minibytes::Bytes;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::ops::{Deref, Range, RangeInclusive};
use std::sync::Arc;

pub(crate) const CELL_PREFIX_LENGTH: usize = 4; // in bytes
pub(crate) const MAX_U32_PLUS_ONE: u64 = u32::MAX as u64 + 1;

#[derive(Clone, Serialize, Deserialize)]
pub struct KeyShape {
    key_spaces: Vec<KeySpaceDesc>,
}

pub struct KeyShapeBuilder {
    key_spaces: Vec<KeySpaceDesc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct KeySpace(pub(crate) u8);

#[doc(hidden)]
#[derive(Clone, Serialize, Deserialize)]
pub struct KeySpaceDesc {
    inner: Arc<KeySpaceDescInner>,
}

#[doc(hidden)]
#[derive(Serialize, Deserialize)]
pub struct KeySpaceDescInner {
    id: KeySpace,
    name: String,
    key_indexing: KeyIndexing,
    mutexes: usize,
    key_type: KeyType,
    config: KeySpaceConfig,
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct KeySpaceConfig {
    #[serde(skip)]
    compactor: Option<Arc<Compactor>>,
    disable_unload: bool,
    max_dirty_keys: Option<usize>,
    bloom_filter: Option<BloomFilterParams>,
    value_cache_size: usize,
    index_format: IndexFormatType,
    unloaded_iterator: bool,
    #[serde(skip)]
    relocation_filter: Option<Arc<Box<dyn RelocationFilter>>>,
}

/// This enum allows customizing the key used in the index.
/// By default, KeyIndexing::Fixed is used putting a key into index as it is.
///
/// With other options, when a user puts a key-value pair (K, V) in the database,
/// we write (K, V) into wal, but we use F(K) in the index, instead of K,
/// where F(K) depends on the type of key indexing.
///
/// This allows for various use cases such as
/// * Key reduction (reducing index size by using fewer bytes from the key in the index)
#[derive(Clone, Serialize, Deserialize)]
pub enum KeyIndexing {
    Fixed(usize),
    Reduction(usize, Range<usize>),
    VariableLength,
    Hash,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub enum KeyType {
    Uniform(UniformKeyConfig),
    PrefixedUniform(PrefixedUniformKeyConfig),
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct UniformKeyConfig {
    cells_per_mutex: usize,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
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

#[derive(Default, Clone, Serialize, Deserialize)]
pub(crate) struct BloomFilterParams {
    pub rate: f32,
    pub count: u32,
}

// todo - we want better compactor API that does not expose too much internal details
// todo - make mod wal private
pub type Compactor = Box<dyn Fn(&mut BTreeMap<Bytes, IndexWalPosition>) + Sync + Send>;

#[allow(dead_code)]
impl Default for KeyShapeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

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
        self.add_key_space_config_indexing(
            name,
            KeyIndexing::fixed(key_size),
            mutexes,
            key_type,
            config,
        )
    }

    pub fn add_key_space_config_indexing(
        &mut self,
        name: impl Into<String>,
        key_indexing: KeyIndexing,
        mutexes: usize,
        key_type: KeyType,
        config: KeySpaceConfig,
    ) -> KeySpace {
        let name = name.into();
        assert!(mutexes > 0, "mutexes should be greater then 0");
        // also see test_prefix_falls_in_range
        assert!(
            mutexes.is_power_of_two(),
            "mutexes should be power of 2, given {mutexes}"
        );

        assert!(
            self.key_spaces.len() < (u8::MAX - 1) as usize,
            "Maximum {} key spaces allowed",
            u8::MAX
        );

        let ks = KeySpace(self.key_spaces.len() as u8);
        let key_space = KeySpaceDescInner {
            id: ks,
            name,
            key_indexing,
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
                panic!(
                    "Tidehunter currently does not support key space with both compactor and bloom filter enabled"
                );
            }
            ks.key_type.verify_key_size(ks.index_key_size());
            if matches!(ks.key_indexing, KeyIndexing::VariableLength) {
                // todo this can be supported
                assert!(
                    !ks.config.unloaded_iterator,
                    "Unloaded iterator currently not supported for variable length key indexing"
                );
            }
        }
    }
}

impl KeySpaceDesc {
    pub(crate) fn check_key(&self, k: &[u8]) {
        self.key_indexing.check_key_size(k.len(), self.name());
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

    pub(crate) fn last_cell(&self) -> CellId {
        self.key_type.last_cell(self)
    }

    /// Returns true if finding key in the index does not mean the record key matches the index key.
    ///
    /// This returns false for KeyIndexing::Hash because we use cryptographically strong hash,
    /// and therefore it should be impossible to construct a key that collides with existing key.
    pub(crate) fn need_check_index_key(&self) -> bool {
        match self.key_indexing {
            KeyIndexing::Fixed(_) => false,
            KeyIndexing::Reduction(_, _) => true,
            KeyIndexing::Hash => false,
            KeyIndexing::VariableLength => false,
        }
    }

    pub(crate) fn assert_supports_iterator_bound(&self) {
        match self.key_indexing {
            KeyIndexing::Fixed(_) => (),
            KeyIndexing::Reduction(_, _) => (),
            KeyIndexing::Hash => panic!(
                "Key space {} does not support iterator bounds and reversal because it uses KeyIndexing::Hash",
                self.name()
            ),
            KeyIndexing::VariableLength => (),
        }
    }

    pub(crate) fn key_indexing(&self) -> &KeyIndexing {
        &self.key_indexing
    }

    pub(crate) fn key_type(&self) -> &KeyType {
        &self.key_type
    }

    /// Returns index key size if index key size is fixed, None for variable length keys
    pub(crate) fn index_key_size(&self) -> Option<usize> {
        self.key_indexing().index_key_size()
    }

    pub(crate) fn index_key_element_size(&self) -> Option<(usize, usize)> {
        self.index_key_size()
            .map(|key_size| (key_size, self.index_element_size().unwrap()))
    }

    /// Returns index key size if index key size is fixed.
    /// Panics for variable length keys with a message including the provided reason.
    pub(crate) fn require_index_key_size(&self, reason: &'static str) -> usize {
        let Some(key_size) = self.index_key_size() else {
            panic!(
                "Ks {} does not support variable length keys for {}",
                self.name(),
                reason
            )
        };
        key_size
    }

    /// Returns index element size if index key size is fixed, None for variable length keys
    pub(crate) fn index_element_size(&self) -> Option<usize> {
        self.index_key_size().map(|i| i + WalPosition::LENGTH)
    }

    /// Returns index element size if index key size is fixed.
    /// Panics for variable length keys.
    pub(crate) fn require_index_element_size(&self) -> usize {
        let Some(element_size) = self.index_element_size() else {
            panic!(
                "Ks {} uses uniform index that requires key length known ahead of time",
                self.name()
            )
        };
        element_size
    }

    /// Returns imprecise element size that can be used for buffer capacity calculation.
    /// This method uses known key size for key spaces with fixed key size.
    /// For key spaces with variable length keys,
    /// we use the best effort estimate of 64 bytes for index element size.
    pub(crate) fn index_element_size_for_capacity(&self) -> usize {
        self.index_element_size().unwrap_or(64)
    }

    /// Returns u32 prefix
    pub(crate) fn index_prefix_u32(&self, k: &[u8]) -> u32 {
        starting_u32(self.index_prefix(k))
    }

    /// Returns u64 prefix
    pub(crate) fn index_prefix_u64(&self, k: &[u8]) -> u64 {
        starting_u64(self.index_prefix(k))
    }

    fn index_prefix<'a>(&self, k: &'a [u8]) -> &'a [u8] {
        self.key_type.index_prefix(k)
    }

    pub(crate) fn cell_id(&self, k: &[u8]) -> CellId {
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

    pub(crate) fn reduce_key<'a>(&self, key: &'a [u8]) -> Cow<'a, [u8]> {
        self.key_indexing().reduce_key(key)
    }

    pub(crate) fn reduced_key_bytes(&self, key: Bytes) -> Bytes {
        self.key_indexing().reduced_key_bytes(key)
    }

    pub(crate) fn compactor(&self) -> Option<&Compactor> {
        self.config.compactor.as_ref().map(Arc::as_ref)
    }

    pub(crate) fn relocation_filter(&self) -> Option<&dyn RelocationFilter> {
        self.config
            .relocation_filter
            .as_ref()
            .map(|arc| arc.as_ref().as_ref())
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

    pub(crate) fn max_dirty_keys(&self) -> Option<usize> {
        self.config.max_dirty_keys
    }

    pub(crate) fn unloaded_iterator_enabled(&self) -> bool {
        self.config.unloaded_iterator
    }

    #[doc(hidden)] // Used by tools/wal_inspector to display keyspace names
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn id(&self) -> KeySpace {
        self.id
    }

    #[doc(hidden)] // Used by tools/wal_inspector for analyzing keyspace indices
    pub fn index_format(&self) -> &IndexFormatType {
        &self.config.index_format
    }

    /// Returns the first and last keys in a cell.
    ///
    /// This method is only supported for key spaces with fixed-length keys.
    /// Panics if called on a key space with variable-length keys.
    ///
    /// Returns a tuple of (first_key, last_key) where both keys have the key space's configured length.
    pub(crate) fn cell_range(&self, cell: &CellId) -> (Vec<u8>, Vec<u8>) {
        let key_len = self.require_index_key_size("cell_range");

        match self.key_type {
            KeyType::Uniform(_) => {
                let range = self.index_prefix_range(cell);
                let first_u32 = range.start as u32;
                let last_u32 = (range.end - 1) as u32;

                let mut first_key = vec![0u8; key_len];
                let mut last_key = vec![0xFFu8; key_len];

                first_key[0..CELL_PREFIX_LENGTH].copy_from_slice(&first_u32.to_be_bytes());
                last_key[0..CELL_PREFIX_LENGTH].copy_from_slice(&last_u32.to_be_bytes());

                (first_key, last_key)
            }
            KeyType::PrefixedUniform(config) => {
                let prefix = cell.assume_bytes_id();
                let prefix_len = config.prefix_len_bytes;

                let mut first_key = vec![0u8; key_len];
                let mut last_key = vec![0xFFu8; key_len];

                first_key[0..prefix_len].copy_from_slice(prefix);
                last_key[0..prefix_len].copy_from_slice(prefix);

                (first_key, last_key)
            }
        }
    }

    /// Maps a range of keys to a range of cell IDs.
    ///
    /// Verifies that `from_inclusive` is the first key in its cell and
    /// `to_inclusive` is the last key in its cell, panicking otherwise.
    ///
    /// This method is only supported for key spaces with fixed-length keys.
    /// Panics if called on a key space with variable-length keys.
    ///
    /// Returns a tuple of (from_cell_id, to_cell_id) representing the inclusive range.
    pub(crate) fn map_key_range_to_cell_range(
        &self,
        from_inclusive: &[u8],
        to_inclusive: &[u8],
    ) -> (CellId, CellId) {
        let from_cell = self.cell_id(from_inclusive);
        let to_cell = self.cell_id(to_inclusive);

        // Verify from_inclusive is the first key in from_cell
        let (first_key, _) = self.cell_range(&from_cell);
        assert_eq!(
            from_inclusive,
            first_key.as_slice(),
            "from_inclusive is not the first key in its cell"
        );

        // Verify to_inclusive is the last key in to_cell
        let (_, last_key) = self.cell_range(&to_cell);
        assert_eq!(
            to_inclusive,
            last_key.as_slice(),
            "to_inclusive is not the last key in its cell"
        );

        (from_cell, to_cell)
    }

    /// Returns whether this key space uses byte-addressed cells and needs large table cell index.
    /// assume_bytes_id can be used on cell ids for this key space if this returns true.
    pub(crate) fn needs_large_table_cell_index(&self) -> bool {
        matches!(self.key_type, KeyType::PrefixedUniform(_))
    }
}

impl KeySpaceConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_compactor(mut self, compactor: Compactor) -> Self {
        self.compactor = Some(Arc::new(compactor));
        self
    }

    pub fn with_relocation_filter(mut self, filter: impl RelocationFilter) -> Self {
        self.relocation_filter = Some(Arc::new(Box::new(filter)));
        self
    }

    pub fn disable_unload(mut self) -> Self {
        self.disable_unload = true;
        self
    }

    /// Overrides Config::max_dirty_keys for this key space
    // todo this override currently does not work correctly with unload_jitter
    pub fn with_max_dirty_keys(mut self, max_dirty_keys: usize) -> Self {
        self.max_dirty_keys = Some(max_dirty_keys);
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

    pub fn with_index_format(mut self, index_format: IndexFormatType) -> Self {
        self.index_format = index_format;
        self
    }

    pub fn with_unloaded_iterator(mut self, enabled: bool) -> Self {
        self.unloaded_iterator = enabled;
        self
    }
}

impl KeyShape {
    /// Serialize KeyShape to YAML string
    pub(crate) fn to_yaml(&self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(self)
    }

    /// Deserialize KeyShape from YAML string
    pub(crate) fn from_yaml(yaml: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    pub fn new_single(key_size: usize, mutexes: usize, key_type: KeyType) -> (Self, KeySpace) {
        Self::new_single_config(key_size, mutexes, key_type, Default::default())
    }

    pub fn new_single_config(
        key_size: usize,
        mutexes: usize,
        key_type: KeyType,
        config: KeySpaceConfig,
    ) -> (Self, KeySpace) {
        Self::new_single_config_indexing(KeyIndexing::fixed(key_size), mutexes, key_type, config)
    }

    pub fn new_single_config_indexing(
        key_indexing: KeyIndexing,
        mutexes: usize,
        key_type: KeyType,
        config: KeySpaceConfig,
    ) -> (Self, KeySpace) {
        assert!(
            mutexes.is_power_of_two(),
            "mutexes should be power of 2, given {mutexes}"
        );
        let key_space = KeySpaceDescInner {
            id: KeySpace(0),
            name: "root".into(),
            key_indexing,
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

    #[doc(hidden)] // Used by tools/wal_inspector for iterating over keyspaces
    pub fn iter_ks(&self) -> impl Iterator<Item = &KeySpaceDesc> + '_ {
        self.key_spaces.iter()
    }

    pub(crate) fn num_ks(&self) -> usize {
        self.key_spaces.len()
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
    pub(crate) fn first() -> Self {
        KeySpace(0)
    }

    pub(crate) fn as_usize(&self) -> usize {
        self.0 as usize
    }

    pub(crate) fn increment(&mut self) {
        self.0 = self.0.wrapping_add(1);
    }

    #[cfg(test)]
    pub fn new_test(v: u8) -> Self {
        Self(v)
    }

    #[doc(hidden)] // Used by tools/wal_inspector for keyspace handling
    #[cfg(feature = "test-utils")]
    pub fn new(v: u8) -> Self {
        Self(v)
    }

    #[doc(hidden)] // Used by tools/wal_inspector for keyspace handling
    #[cfg(feature = "test-utils")]
    pub fn as_u8(&self) -> u8 {
        self.0
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

    /// Creates a PrefixedUniform KeyType from the number of prefix bits.
    pub fn from_prefix_bits(prefix_bits: u64) -> Self {
        assert!(prefix_bits > 0, "prefix_bits must be greater than 0");

        let max_bits = (MAX_KEY_LEN as u64) * 8;
        assert!(
            prefix_bits <= max_bits,
            "prefix_bits ({prefix_bits}) exceeds maximum key length in bits ({max_bits})"
        );

        let prefix_len_bytes = prefix_bits.div_ceil(8) as usize;
        // Added this allow here to support rust 1.85
        // TODO: remove this allow once Sui upgrades to rust 1.90
        #[allow(unknown_lints, clippy::manual_is_multiple_of)]
        let cluster_bits = if prefix_bits % 8 == 0 {
            0
        } else {
            (8 - (prefix_bits % 8)) as usize
        };

        Self::PrefixedUniform(PrefixedUniformKeyConfig::new(
            prefix_len_bytes,
            cluster_bits,
        ))
    }

    fn verify_key_size(&self, key_size: Option<usize>) {
        match (self, key_size) {
            (KeyType::Uniform(_), _) => {}
            (KeyType::PrefixedUniform(config), Some(key_size)) => {
                assert!(
                    key_size > config.prefix_len_bytes,
                    "key_size({}) must be greater then prefix len({})",
                    key_size,
                    config.prefix_len_bytes
                );
            }
            (KeyType::PrefixedUniform(_config), None) => {}
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
    fn last_cell(&self, ksd: &KeySpaceDesc) -> CellId {
        match self {
            KeyType::Uniform(config) => CellId::Integer(config.num_cells(ksd) - 1),
            KeyType::PrefixedUniform(config) => {
                let bytes = SmallVec::from_elem(255, config.prefix_len_bytes);
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
        assert!(
            cells_per_mutex.is_power_of_two(),
            "cells_per_mutex should be power of two, given {cells_per_mutex}"
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
            let end_exclusive = end_inclusive + 0x1000000;
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

impl Debug for KeyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyType::Uniform(c) => write!(f, "uniform({})", c.cells_per_mutex),
            KeyType::PrefixedUniform(c) => {
                write!(f, "prefix({}, {})", c.prefix_len_bytes, c.cluster_bits)
            }
        }
    }
}

impl KeyIndexing {
    const HASH_SIZE: usize = 16;

    pub fn fixed(key_length: usize) -> Self {
        Self::check_configured_key_size(key_length);
        Self::Fixed(key_length)
    }

    pub fn key_reduction(key_length: usize, range: Range<usize>) -> Self {
        Self::check_configured_key_size(key_length);
        Self::Reduction(key_length, range)
    }

    pub fn variable_length() -> Self {
        Self::VariableLength
    }

    pub fn hash() -> Self {
        Self::Hash
    }

    fn check_configured_key_size(key_size: usize) {
        assert!(
            key_size <= MAX_KEY_LEN,
            "Specified key size exceeding max key length"
        );
    }

    pub(crate) fn index_key_size(&self) -> Option<usize> {
        match self {
            KeyIndexing::Fixed(key_size) => Some(*key_size),
            KeyIndexing::Reduction(_, range) => Some(range.len()),
            KeyIndexing::Hash => Some(Self::HASH_SIZE),
            KeyIndexing::VariableLength => None,
        }
    }

    pub(crate) fn check_key_size(&self, k: usize, name: &str) {
        let expected_key_size = match self {
            KeyIndexing::Fixed(key_size) => *key_size,
            KeyIndexing::Reduction(key_size, _) => *key_size,
            KeyIndexing::Hash | KeyIndexing::VariableLength => {
                if k > MAX_KEY_LEN {
                    panic!("Key space {name} accepts maximum keys size {MAX_KEY_LEN}, given {k}");
                }
                return;
            }
        };
        if expected_key_size != k {
            panic!("Key space {name} accepts keys size {expected_key_size}, given {k}");
        }
    }

    pub(crate) fn reduce_key<'a>(&self, key: &'a [u8]) -> Cow<'a, [u8]> {
        match self {
            KeyIndexing::Fixed(_) | KeyIndexing::VariableLength => Cow::Borrowed(key),
            KeyIndexing::Reduction(_, range) => Cow::Borrowed(&key[range.clone()]),
            KeyIndexing::Hash => Cow::Owned(Self::hash_key(key)),
        }
    }

    pub(crate) fn reduced_key_bytes(&self, key: Bytes) -> Bytes {
        match self {
            KeyIndexing::Fixed(_) | KeyIndexing::VariableLength => key,
            KeyIndexing::Reduction(_, range) => key.slice(range.clone()),
            KeyIndexing::Hash => Self::hash_key(&key).into(),
        }
    }

    fn hash_key(key: &[u8]) -> Vec<u8> {
        type Blake2b256 = blake2::Blake2b<typenum::U32>;
        let hash = Blake2b256::digest(key);
        hash[..Self::HASH_SIZE].to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell::CellIdBytesContainer;
    use hex_literal::hex;
    use rand::prelude::StdRng;
    use rand::{Rng, SeedableRng};
    use std::collections::HashMap;
    use std::collections::hash_map::Entry;

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
        assert_prefix_range_eq(0x0000_0000..0x8000_0000, config.prefix_range(0));
        assert_prefix_range_eq(0x0000_0000..0x8000_0000, config.prefix_range(15));
        assert_prefix_range_eq(0x0000_0000..0x8000_0000, config.prefix_range(0x7f));
        assert_prefix_range_eq(0x8000_0000..0x1_0000_0000, config.prefix_range(0x80));
        assert_prefix_range_eq(0x8000_0000..0x1_0000_0000, config.prefix_range(0xff));
    }

    #[test]
    fn test_prefix_falls_in_range() {
        // todo do we want to support num mutexes that are not power of 2?
        for mutexes in [1, 16, 256 /*, 12*/] {
            test_prefix_falls_in_range_impl(mutexes, KeyType::uniform(1));
            test_prefix_falls_in_range_impl(mutexes, KeyType::uniform(64));
            test_prefix_falls_in_range_impl(mutexes, KeyType::uniform(256));
            test_prefix_falls_in_range_impl(mutexes, KeyType::prefix_uniform(8, 4));
            test_prefix_falls_in_range_impl(mutexes, KeyType::prefix_uniform(15, 4));
        }
    }

    fn test_prefix_falls_in_range_impl(mutexes: usize, key_type: KeyType) {
        let (key_shape, ks) = KeyShape::new_single(32, mutexes, key_type.clone());
        let ks = key_shape.ks(ks);
        let mut rng = StdRng::from_seed(Default::default());
        for _ in 0..1024 {
            let mut key = vec![0u8; 32];
            rng.fill(&mut key[..]);
            test_prefix_falls_in_range_for_key(mutexes, &key_type, ks, &key);
        }
        // border values
        test_prefix_falls_in_range_for_key(mutexes, &key_type, ks, &[0x0u8; 32]);
        test_prefix_falls_in_range_for_key(mutexes, &key_type, ks, &[0xffu8; 32]);
    }

    #[track_caller]
    fn test_prefix_falls_in_range_for_key(
        mutexes: usize,
        key_type: &KeyType,
        ks: &KeySpaceDesc,
        key: &[u8],
    ) {
        let prefix = ks.index_prefix_u32(&key) as u64;
        let cell = ks.cell_id(&key);
        let range = ks.index_prefix_range(&cell);
        if !range.contains(&prefix) {
            let mut formatted_key = String::default();
            for chunk in key.chunks(8) {
                formatted_key.push_str(&hex::encode(chunk));
                formatted_key.push('_');
            }
            panic!(
                "Failed for key {formatted_key}, cell {cell:x?}, prefix {prefix:x}, range {range:x?}, key type {key_type:?}, mutexes {mutexes}"
            );
        }
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

    #[test]
    fn test_from_prefix_bits_8_bits() {
        // Test 8-bit prefix: first byte determines the cell
        let key_type = KeyType::from_prefix_bits(8);
        let (key_shape, ks) = KeyShape::new_single(32, 16, key_type);
        let ksd = key_shape.ks(ks);

        // Keys with same first byte map to same cell
        assert_eq!(ksd.cell_id(&[0xAA, 0x00, 0x00, 0x00]), c(&[0xAA]));
        assert_eq!(ksd.cell_id(&[0xAA, 0xFF, 0x11, 0x22]), c(&[0xAA]));

        // Keys with different first byte map to different cells
        assert_eq!(ksd.cell_id(&[0xBB, 0x00, 0x00, 0x00]), c(&[0xBB]));
    }

    #[test]
    fn test_from_prefix_bits_12_bits() {
        // Test 12-bit prefix: first byte + top 4 bits of second byte
        let key_type = KeyType::from_prefix_bits(12);
        let (key_shape, ks) = KeyShape::new_single(32, 16, key_type);
        let ksd = key_shape.ks(ks);

        // Keys with same top 12 bits map to same cell
        assert_eq!(
            ksd.cell_id(&[0xAB, 0b11000000, 0x00, 0x00]),
            c(&[0xAB, 0b11000000])
        );
        assert_eq!(
            ksd.cell_id(&[0xAB, 0b11000101, 0xFF, 0x11]),
            c(&[0xAB, 0b11000000])
        );

        // Keys with different top 12 bits map to different cells
        assert_eq!(
            ksd.cell_id(&[0xAB, 0b11010000, 0x00, 0x00]),
            c(&[0xAB, 0b11010000])
        );
    }

    #[test]
    fn test_from_prefix_bits_1_bit() {
        // Test 1-bit prefix: only the MSB of first byte matters
        let key_type = KeyType::from_prefix_bits(1);
        let (key_shape, ks) = KeyShape::new_single(32, 16, key_type);
        let ksd = key_shape.ks(ks);

        // Keys with MSB=0 map to same cell
        assert_eq!(
            ksd.cell_id(&[0b00000000, 0xFF, 0xFF, 0xFF]),
            c(&[0b00000000])
        );
        assert_eq!(
            ksd.cell_id(&[0b01111111, 0x00, 0x00, 0x00]),
            c(&[0b00000000])
        );

        // Keys with MSB=1 map to same cell (different from MSB=0)
        assert_eq!(
            ksd.cell_id(&[0b10000000, 0x00, 0x00, 0x00]),
            c(&[0b10000000])
        );
        assert_eq!(
            ksd.cell_id(&[0b11111111, 0xFF, 0xFF, 0xFF]),
            c(&[0b10000000])
        );
    }

    #[test]
    fn test_from_prefix_bits_9_bits() {
        // Test 9-bit prefix: first byte + top 1 bit of second byte
        let key_type = KeyType::from_prefix_bits(9);
        let (key_shape, ks) = KeyShape::new_single(32, 16, key_type);
        let ksd = key_shape.ks(ks);

        // Keys with same top 9 bits map to same cell
        assert_eq!(
            ksd.cell_id(&[0xAA, 0b10000000, 0x00, 0x00]),
            c(&[0xAA, 0b10000000])
        );
        assert_eq!(
            ksd.cell_id(&[0xAA, 0b11111111, 0x11, 0x22]),
            c(&[0xAA, 0b10000000])
        );

        // Keys with different top 9 bits map to different cells
        assert_eq!(
            ksd.cell_id(&[0xAA, 0b01111111, 0x00, 0x00]),
            c(&[0xAA, 0b00000000])
        );
    }

    #[test]
    fn test_from_prefix_bits_15_bits() {
        // Test 15-bit prefix: first byte + top 7 bits of second byte
        let key_type = KeyType::from_prefix_bits(15);
        let (key_shape, ks) = KeyShape::new_single(32, 16, key_type);
        let ksd = key_shape.ks(ks);

        // Keys with same top 15 bits map to same cell
        assert_eq!(
            ksd.cell_id(&[0xAB, 0b11111110, 0x00, 0x00]),
            c(&[0xAB, 0b11111110])
        );
        assert_eq!(
            ksd.cell_id(&[0xAB, 0b11111111, 0x11, 0x22]),
            c(&[0xAB, 0b11111110])
        );

        // Keys with different top 15 bits map to different cells
        assert_eq!(
            ksd.cell_id(&[0xAB, 0b11111100, 0x00, 0x00]),
            c(&[0xAB, 0b11111100])
        );
    }

    #[test]
    fn test_from_prefix_bits_16_bits() {
        // Test 16-bit prefix: first two bytes determine the cell
        let key_type = KeyType::from_prefix_bits(16);
        let (key_shape, ks) = KeyShape::new_single(32, 16, key_type);
        let ksd = key_shape.ks(ks);

        // Keys with same first two bytes map to same cell
        assert_eq!(ksd.cell_id(&[0x12, 0x34, 0x00, 0x00]), c(&[0x12, 0x34]));
        assert_eq!(ksd.cell_id(&[0x12, 0x34, 0xFF, 0xFF]), c(&[0x12, 0x34]));

        // Keys with different first two bytes map to different cells
        assert_eq!(ksd.cell_id(&[0x12, 0x35, 0x00, 0x00]), c(&[0x12, 0x35]));
    }

    #[test]
    #[should_panic(expected = "prefix_bits must be greater than 0")]
    fn test_from_prefix_bits_zero() {
        KeyType::from_prefix_bits(0);
    }

    #[test]
    #[should_panic(expected = "exceeds maximum key length")]
    fn test_from_prefix_bits_too_large() {
        let max_bits = (MAX_KEY_LEN as u64) * 8;
        KeyType::from_prefix_bits(max_bits + 1);
    }

    #[test]
    fn test_cell_range_uniform() {
        // Create a key shape with 4 cells (2 mutexes, 2 cells per mutex)
        let (key_shape, ks) = KeyShape::new_single(10, 2, KeyType::uniform(2));
        let ksd = key_shape.ks(ks);

        // Test cell 0: range 0x00000000..0x40000000
        let (first, last) = ksd.cell_range(&CellId::Integer(0));
        assert_eq!(first.as_slice(), &hex!("00000000 00000000 0000"));
        assert_eq!(last.as_slice(), &hex!("3fffffff ffffffff ffff"));

        // Test cell 1: range 0x40000000..0x80000000
        let (first, last) = ksd.cell_range(&CellId::Integer(1));
        assert_eq!(first.as_slice(), &hex!("40000000 00000000 0000"));
        assert_eq!(last.as_slice(), &hex!("7fffffff ffffffff ffff"));

        // Test cell 3 (last cell): range 0xC0000000..0x100000000
        let (first, last) = ksd.cell_range(&CellId::Integer(3));
        assert_eq!(first.as_slice(), &hex!("c0000000 00000000 0000"));
        assert_eq!(last.as_slice(), &hex!("ffffffff ffffffff ffff"));
    }

    #[test]
    fn test_cell_range_prefixed_uniform() {
        // Create a key shape with 2-byte prefix
        let (key_shape, ks) = KeyShape::new_single(10, 16, KeyType::from_prefix_bits(16));
        let ksd = key_shape.ks(ks);

        // Test cell [0x12, 0x34]
        let cell = c(&[0x12, 0x34]);
        let (first, last) = ksd.cell_range(&cell);
        assert_eq!(first.as_slice(), &hex!("1234 00000000 00000000"));
        assert_eq!(last.as_slice(), &hex!("1234 ffffffff ffffffff"));

        // Test cell [0xff, 0xff] (last cell)
        let cell = c(&[0xff, 0xff]);
        let (first, last) = ksd.cell_range(&cell);
        assert_eq!(first.as_slice(), &hex!("ffff 00000000 00000000"));
        assert_eq!(last.as_slice(), &hex!("ffff ffffffff ffffffff"));
    }

    #[test]
    fn test_map_key_range_to_cell_range_uniform() {
        // Create a key shape with 4 cells (2 mutexes, 2 cells per mutex)
        let (key_shape, ks) = KeyShape::new_single(32, 2, KeyType::uniform(2));
        let ksd = key_shape.ks(ks);

        // Each cell covers (u32::MAX + 1) / 4 = 0x40000000 u32 values
        // Cell 0: 0x00000000..0x40000000
        // Cell 1: 0x40000000..0x80000000
        // Cell 2: 0x80000000..0xC0000000
        // Cell 3: 0xC0000000..0x100000000

        // Test valid range from cell 0 to cell 1
        let mut from_key = vec![0u8; 32];
        from_key[0..4].copy_from_slice(&0x00000000u32.to_be_bytes());

        let mut to_key = vec![0xFFu8; 32];
        to_key[0..4].copy_from_slice(&0x7FFFFFFFu32.to_be_bytes());

        let (from_cell, to_cell) = ksd.map_key_range_to_cell_range(&from_key, &to_key);
        assert_eq!(from_cell, CellId::Integer(0));
        assert_eq!(to_cell, CellId::Integer(1));

        // Test single cell range
        let mut from_key2 = vec![0u8; 32];
        from_key2[0..4].copy_from_slice(&0x40000000u32.to_be_bytes());

        let mut to_key2 = vec![0xFFu8; 32];
        to_key2[0..4].copy_from_slice(&0x7FFFFFFFu32.to_be_bytes());

        let (from_cell2, to_cell2) = ksd.map_key_range_to_cell_range(&from_key2, &to_key2);
        assert_eq!(from_cell2, CellId::Integer(1));
        assert_eq!(to_cell2, CellId::Integer(1));
    }

    #[test]
    #[should_panic(expected = "from_inclusive is not the first key in its cell")]
    fn test_map_key_range_invalid_from_key_uniform() {
        let (key_shape, ks) = KeyShape::new_single(32, 2, KeyType::uniform(2));
        let ksd = key_shape.ks(ks);

        // First key with non-zero byte at position 5
        let mut from_key = vec![0u8; 32];
        from_key[5] = 1;

        let to_key = vec![0xFFu8; 32];
        ksd.map_key_range_to_cell_range(&from_key, &to_key);
    }

    #[test]
    #[should_panic(expected = "to_inclusive is not the last key in its cell")]
    fn test_map_key_range_invalid_to_key_uniform() {
        let (key_shape, ks) = KeyShape::new_single(32, 2, KeyType::uniform(2));
        let ksd = key_shape.ks(ks);

        let from_key = vec![0u8; 32];

        // Last key with non-FF byte at position 5
        let mut to_key = vec![0xFFu8; 32];
        to_key[5] = 0xFE;

        ksd.map_key_range_to_cell_range(&from_key, &to_key);
    }

    #[test]
    fn test_map_key_range_to_cell_range_prefixed_uniform() {
        // Create a key shape with 2-byte prefix
        let (key_shape, ks) = KeyShape::new_single(32, 16, KeyType::prefix_uniform(2, 0));
        let ksd = key_shape.ks(ks);

        // Test range from one cell to another
        let mut from_key = vec![0u8; 32];
        from_key[0] = 0x12;
        from_key[1] = 0x34;
        // Rest are zeros

        let mut to_key = vec![0xFFu8; 32];
        to_key[0] = 0x56;
        to_key[1] = 0x78;
        // Rest are 0xFF

        let (from_cell, to_cell) = ksd.map_key_range_to_cell_range(&from_key, &to_key);
        assert_eq!(from_cell, c(&[0x12, 0x34]));
        assert_eq!(to_cell, c(&[0x56, 0x78]));
    }

    #[test]
    #[should_panic(expected = "from_inclusive is not the first key in its cell")]
    fn test_map_key_range_invalid_from_key_prefixed() {
        let (key_shape, ks) = KeyShape::new_single(32, 16, KeyType::prefix_uniform(2, 0));
        let ksd = key_shape.ks(ks);

        // First key with non-zero byte after prefix
        let mut from_key = vec![0u8; 32];
        from_key[0] = 0x12;
        from_key[1] = 0x34;
        from_key[2] = 1; // Should be 0

        let to_key = vec![0xFFu8; 32];
        ksd.map_key_range_to_cell_range(&from_key, &to_key);
    }

    #[test]
    #[should_panic(expected = "to_inclusive is not the last key in its cell")]
    fn test_map_key_range_invalid_to_key_prefixed() {
        let (key_shape, ks) = KeyShape::new_single(32, 16, KeyType::prefix_uniform(2, 0));
        let ksd = key_shape.ks(ks);

        let from_key = vec![0u8; 32];

        // Last key with non-FF byte after prefix
        let mut to_key = vec![0xFFu8; 32];
        to_key[0] = 0x56;
        to_key[1] = 0x78;
        to_key[2] = 0xFE; // Should be 0xFF

        ksd.map_key_range_to_cell_range(&from_key, &to_key);
    }
}
