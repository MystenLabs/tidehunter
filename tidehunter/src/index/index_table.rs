use crate::key_shape::KeySpaceDesc;
use crate::primitives::cursor::SliceCursor;
use crate::primitives::slice_buf::SliceBuf;
use crate::primitives::var_int::{MAX_U16_VARINT, deserialize_u16_varint, serialize_u16_varint};
use crate::wal::position::{HasOffset, LastProcessed, WalPosition};
use bytes::{Buf, BufMut, BytesMut};
use minibytes::Bytes;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::collections::btree_map::Entry;
use std::ops::Bound;

#[derive(Default, Clone, Debug)]
#[doc(hidden)]
pub struct IndexTable {
    data: BTreeMap<Bytes, IndexWalPosition>,
    /// Sum of all key lengths currently in `data`. Maintained incrementally.
    key_bytes: usize,
    /// Sorted clean entries in compact binary format.
    ///
    /// Two formats depending on whether the key space uses fixed-length keys:
    /// - Variable-length (`key_size == None`): `[count: u32][offsets: u32 * count][key_len: u16][key][pos (12)]*`
    /// - Fixed-length (`key_size == Some(n)`): `[key (n bytes)][pos (12 bytes)]*` — identical to on-disk body
    ///
    /// All entries are considered clean (promoted from the BTreeMap).
    /// Entries in `data` take priority over entries in `flat` for the same key.
    flat: Bytes,
    /// Sum of all key lengths currently in `flat`. Maintained incrementally.
    flat_key_bytes: usize,
    /// Fixed key size for this key space, or `None` for variable-length keys.
    /// Determines which flat format is used.
    key_size: Option<usize>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct IndexWalPosition {
    offset: u64,
    len: u32,
    kind: IndexEntryKind,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum IndexEntryKind {
    Clean,
    Modified,
    Removed,
}

// Compile time check to ensure IndexWalPosition consumes the same amount of memory as WalPosition
const _: [u8; size_of::<WalPosition>()] = [0u8; size_of::<IndexWalPosition>()];

// ---------------------------------------------------------------------------
// Flat array helper functions
//
// Two in-memory formats, selected by `IndexTable::key_size`:
//
//   Variable-length (key_size == None):
//     [count: u32][offsets: u32 * count][key_len: u16][key][pos (12)]*
//     Offsets enable O(log n) binary search without scanning entries.
//
//   Fixed-length (key_size == Some(n)):
//     [key (n bytes)][pos (12 bytes)]*
//     No per-entry overhead; count = flat.len() / (n + 12).
//     Identical format to the on-disk body for fixed-length key spaces.
//
// The flat buffer is purely in-memory and never persisted.
// ---------------------------------------------------------------------------

// ---- Variable-length helpers (operate on the flat buffer directly) --------

fn var_flat_entry_count(flat: &[u8]) -> usize {
    if flat.len() < 4 {
        return 0;
    }
    u32::from_be_bytes(flat[0..4].try_into().unwrap()) as usize
}

fn var_flat_entries_section_start(count: usize) -> usize {
    4 + 4 * count
}

fn var_flat_read_entry_offset(flat: &[u8], idx: usize) -> usize {
    let pos = 4 + 4 * idx;
    u32::from_be_bytes(flat[pos..pos + 4].try_into().unwrap()) as usize
}

/// Parse a `(key_slice, WalPosition)` from the var-len entries section at `byte_offset`.
fn var_flat_parse_entry(entries_section: &[u8], byte_offset: usize) -> (&[u8], WalPosition) {
    let key_len = u16::from_be_bytes(
        entries_section[byte_offset..byte_offset + 2]
            .try_into()
            .unwrap(),
    ) as usize;
    let key = &entries_section[byte_offset + 2..byte_offset + 2 + key_len];
    let wal_start = byte_offset + 2 + key_len;
    let wal_offset = u64::from_be_bytes(
        entries_section[wal_start..wal_start + 8]
            .try_into()
            .unwrap(),
    );
    let wal_len = u32::from_be_bytes(
        entries_section[wal_start + 8..wal_start + 12]
            .try_into()
            .unwrap(),
    );
    (key, WalPosition::new(wal_offset, wal_len))
}

/// Build a flat buffer from a sorted list of `(key, position)` pairs.
///
/// Pass `key_size = Some(n)` for fixed-length key spaces; `None` for variable-length.
fn build_flat_bytes(entries: Vec<(Bytes, WalPosition)>, key_size: Option<usize>) -> Bytes {
    let n = entries.len();
    if n == 0 {
        return Bytes::default();
    }

    if let Some(key_size) = key_size {
        // Fixed-length format: [key...pos...]*
        let elem_size = key_size + WalPosition::LENGTH;
        let mut result = Vec::with_capacity(n * elem_size);
        for (key, pos) in &entries {
            debug_assert_eq!(key.len(), key_size, "key length mismatch in fixed flat");
            result.extend_from_slice(key);
            pos.write_to_buf(&mut result);
        }
        Bytes::from(result)
    } else {
        // Variable-length format: [count: u32][offsets: u32*n][key_len: u16][key][pos]*
        let mut data_section: Vec<u8> = Vec::new();
        let mut offsets: Vec<u32> = Vec::with_capacity(n);
        for (key, pos) in &entries {
            offsets.push(data_section.len() as u32);
            let key_len = key.len() as u16;
            data_section.extend_from_slice(&key_len.to_be_bytes());
            data_section.extend_from_slice(key);
            pos.write_to_buf(&mut data_section);
        }
        let header_size = 4 + 4 * n; // count + offsets
        let mut result = Vec::with_capacity(header_size + data_section.len());
        result.extend_from_slice(&(n as u32).to_be_bytes());
        for off in offsets {
            result.extend_from_slice(&off.to_be_bytes());
        }
        result.extend_from_slice(&data_section);
        Bytes::from(result)
    }
}

// ---------------------------------------------------------------------------
// Sequential flat iterator
// ---------------------------------------------------------------------------

enum FlatIter<'a> {
    Empty,
    VarLen {
        flat: &'a [u8],            // full flat buffer, starts with count field
        entries_section: &'a [u8],
        count: usize,
        index: usize,
    },
    Fixed {
        entries: &'a [u8], // full flat buffer: packed key+pos elements
        key_size: usize,
        count: usize,
        index: usize,
    },
}

impl<'a> FlatIter<'a> {
    fn new(flat: &'a [u8], key_size: Option<usize>) -> Self {
        if flat.is_empty() {
            return Self::Empty;
        }
        match key_size {
            None => {
                let count = var_flat_entry_count(flat);
                let section_start = var_flat_entries_section_start(count);
                let entries_section = if count > 0 { &flat[section_start..] } else { &[] };
                Self::VarLen { flat, entries_section, count, index: 0 }
            }
            Some(key_size) => {
                let elem_size = key_size + WalPosition::LENGTH;
                let count = if elem_size > 0 { flat.len() / elem_size } else { 0 };
                Self::Fixed { entries: flat, key_size, count, index: 0 }
            }
        }
    }
}

impl<'a> Iterator for FlatIter<'a> {
    type Item = (&'a [u8], WalPosition);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Empty => None,
            Self::VarLen { flat, entries_section, count, index } => {
                if *index >= *count {
                    return None;
                }
                let byte_offset = var_flat_read_entry_offset(flat, *index);
                let (key, pos) = var_flat_parse_entry(entries_section, byte_offset);
                *index += 1;
                Some((key, pos))
            }
            Self::Fixed { entries, key_size, count, index } => {
                if *index >= *count {
                    return None;
                }
                let elem_size = *key_size + WalPosition::LENGTH;
                let start = *index * elem_size;
                let key = &entries[start..start + *key_size];
                let pos =
                    WalPosition::from_slice(&entries[start + *key_size..start + elem_size]);
                *index += 1;
                Some((key, pos))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// IndexTable implementation
// ---------------------------------------------------------------------------

impl IndexTable {
    pub fn insert(&mut self, k: Bytes, v: WalPosition) -> Option<WalPosition> {
        self.checked_insert(k, IndexWalPosition::new_modified(v))
    }

    pub fn remove(&mut self, k: Bytes, v: WalPosition) -> Option<WalPosition> {
        self.checked_insert(k, IndexWalPosition::new_removed(v))
    }

    fn checked_insert(&mut self, k: Bytes, v: IndexWalPosition) -> Option<WalPosition> {
        let k = k.into_owned();
        // Only update index entry if new entry has higher wal position then the previous entry
        // See test_concurrent_single_value_update for details how this is tested
        // todo handle this comparison correctly when we have data relocation
        // todo might want a separate test with snapshot enabled
        match self.data.entry(k) {
            Entry::Vacant(va) => {
                self.key_bytes += va.key().len();
                va.insert(v);
                None
            }
            Entry::Occupied(mut oc) => {
                let previous = *oc.get();
                assert!(
                    previous.offset < v.offset,
                    "Index WAL position must be increasing"
                );
                oc.insert(v);
                Some(previous.into_wal_position())
            }
        }
    }

    /// Merges dirty IndexTable into a loaded IndexTable, producing **clean** index table
    pub fn merge_dirty_and_clean(&mut self, dirty: &Self) {
        // todo implement this efficiently taking into account both self and dirty are sorted
        for (k, v) in dirty.data.iter() {
            // Strict check for concurrent_test and unit tests
            #[cfg(any(debug_assertions, feature = "test_methods"))]
            if let Some(found) = self.data.get(k)
                && found.offset > v.offset
            {
                // This can't happen - this would mean we somehow have written an
                // older entry in a dirty set.
                panic!("found.offset {} > v.offset {}", found.offset, v.offset);
            }
            if v.is_removed() {
                if self.flat.is_empty() {
                    if self.data.remove(k).is_some() {
                        self.key_bytes -= k.len();
                    }
                } else {
                    // Keep tombstone in BTreeMap to shadow any existing flat entry for this key.
                    if self.data.insert(k.clone(), *v).is_none() {
                        self.key_bytes += k.len();
                    }
                }
            } else if self.data.insert(k.clone(), v.as_clean_modified()).is_none() {
                self.key_bytes += k.len();
            }
        }
    }

    /// Merges dirty IndexTable into a loaded IndexTable, preserving dirty states
    pub fn merge_dirty_no_clean(&mut self, dirty: &Self) {
        // todo implement this efficiently taking into account both self and dirty are sorted
        for (k, v) in dirty.data.iter() {
            if self.data.insert(k.clone(), *v).is_none() {
                self.key_bytes += k.len();
            }
        }
    }

    /// Remove flushed index entries that have offset <= last_processed
    pub fn unmerge_flushed(&mut self, original: &Self, last_processed: LastProcessed) {
        for (k, v) in original.data.iter() {
            if !last_processed.is_processed(v) {
                // Do not unmerge entries that might have in-flight operations
                continue;
            }
            let entry = self.data.entry(k.clone());
            match entry {
                Entry::Vacant(_) => {
                    #[cfg(any(debug_assertions, feature = "test_methods"))]
                    panic!("unmerge_flushed entry mismatch {k:?}")
                    // todo promote this to unconditional panic?
                }
                Entry::Occupied(oc) => {
                    if oc.get().into_wal_position() == v.into_wal_position() {
                        self.key_bytes -= k.len();
                        oc.remove();
                    }
                }
            }
        }
    }

    /// Count the number of dirty(modified or removed) wal positions in this index.
    /// This information is not cached and causes iteration over the entire index table.
    pub fn count_dirty(&self) -> usize {
        self.data.values().filter(|p| !p.is_clean()).count()
    }

    fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&Bytes, &mut IndexWalPosition) -> bool,
    {
        let mut removed_bytes = 0usize;
        self.data.retain(|k, v| {
            if f(k, v) {
                true
            } else {
                removed_bytes += k.len();
                false
            }
        });
        self.key_bytes -= removed_bytes;
    }

    /// Change loaded dirty IndexTable into unloaded dirty by retaining dirty keys and tombstones.
    /// Also clears the flat array since it contains clean data that is already on disk.
    pub fn retain_dirty(&mut self) {
        self.retain(|_, pos| !pos.is_clean());
        self.flat = Bytes::default();
        self.flat_key_bytes = 0;
    }

    pub fn get(&self, k: &[u8]) -> Option<WalPosition> {
        // BTreeMap takes priority: may have a more-recent Modified or Removed entry.
        if let Some(p) = self.data.get(k) {
            return Some(p.into_wal_position());
        }
        // Fall through to the flat array.
        self.flat_binary_search(k)
    }

    /// similar to `get`, but returns the position of the modification operation for both deletes and inserts
    pub fn get_update_position(&self, k: &[u8]) -> Option<WalPosition> {
        if let Some(p) = self.data.get(k) {
            return Some(p.into_update_position());
        }
        // For flat entries all positions are clean, so update position == value position.
        self.flat_binary_search(k)
    }

    /// If prev is None returns first entry.
    ///
    /// If prev is not None, returns entry after specified prev.
    ///
    /// Returns tuple of a key, value.
    ///
    /// This works even if prev is set to Some(k), but the value at k does not exist (for ex. was deleted).
    pub fn next_entry(&self, prev: Option<Bytes>, reverse: bool) -> Option<(Bytes, WalPosition)> {
        if reverse {
            self.next_entry_reverse(prev.as_deref())
        } else {
            self.next_entry_forward(prev.as_deref())
        }
    }

    fn next_entry_forward(&self, prev: Option<&[u8]>) -> Option<(Bytes, WalPosition)> {
        // Find first BTreeMap entry strictly after prev.
        // We include Removed entries so callers can see the tombstone (INVALID position)
        // and correctly shadow any matching on-disk entry (used by DirtyUnloaded iteration).
        let btree_cand: Option<(&Bytes, &IndexWalPosition)> = if let Some(prev) = prev {
            self.data
                .range::<[u8], _>((Bound::Excluded(prev), Bound::<&[u8]>::Unbounded))
                .next()
        } else {
            self.data.iter().next()
        };

        // Find the first flat entry strictly after prev, skipping entries that are
        // shadowed by a Removed tombstone in BTreeMap.
        let mut flat_cand = self.flat_next_forward(prev);
        while let Some((ref fk, _)) = flat_cand {
            if self
                .data
                .get(fk.as_ref())
                .map(|v| v.is_removed())
                .unwrap_or(false)
            {
                let fk_clone = fk.clone();
                flat_cand = self.flat_next_forward(Some(fk_clone.as_ref()));
            } else {
                break;
            }
        }

        // Take the candidate with the smaller key; BTreeMap wins on ties.
        match (btree_cand, flat_cand) {
            (None, None) => None,
            (Some((bk, bv)), None) => Some((bk.clone(), bv.into_wal_position())),
            (None, Some((fk, fv))) => Some((fk, fv)),
            (Some((bk, bv)), Some((fk, fv))) => match bk.as_ref().cmp(fk.as_ref()) {
                Ordering::Less | Ordering::Equal => Some((bk.clone(), bv.into_wal_position())),
                Ordering::Greater => Some((fk, fv)),
            },
        }
    }

    fn next_entry_reverse(&self, prev: Option<&[u8]>) -> Option<(Bytes, WalPosition)> {
        // Find last BTreeMap entry strictly before prev.
        // We include Removed entries so callers can see the tombstone (INVALID position)
        // and correctly shadow any matching on-disk entry (used by DirtyUnloaded iteration).
        let btree_cand: Option<(&Bytes, &IndexWalPosition)> = if let Some(prev) = prev {
            self.data
                .range::<[u8], _>((Bound::Unbounded, Bound::Excluded(prev)))
                .next_back()
        } else {
            self.data.iter().next_back()
        };

        // Find the last flat entry strictly before prev, skipping deleted entries.
        let mut flat_cand = self.flat_next_reverse(prev);
        while let Some((ref fk, _)) = flat_cand {
            if self
                .data
                .get(fk.as_ref())
                .map(|v| v.is_removed())
                .unwrap_or(false)
            {
                let fk_clone = fk.clone();
                flat_cand = self.flat_next_reverse(Some(fk_clone.as_ref()));
            } else {
                break;
            }
        }

        // Take the candidate with the larger key; BTreeMap wins on ties.
        match (btree_cand, flat_cand) {
            (None, None) => None,
            (Some((bk, bv)), None) => Some((bk.clone(), bv.into_wal_position())),
            (None, Some((fk, fv))) => Some((fk, fv)),
            (Some((bk, bv)), Some((fk, fv))) => match bk.as_ref().cmp(fk.as_ref()) {
                Ordering::Greater | Ordering::Equal => Some((bk.clone(), bv.into_wal_position())),
                Ordering::Less => Some((fk, fv)),
            },
        }
    }

    pub fn len(&self) -> usize {
        // Note: may slightly overcount when BTreeMap has entries that also exist in flat,
        // but the error window is small (only during period between promote_to_flat calls).
        self.flat_len() + self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty() && self.flat.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (Bytes, WalPosition)> + '_ {
        // Flat entries not overridden by BTreeMap, followed by valid BTreeMap entries.
        // Collect and sort to guarantee sorted output.
        let flat_entries = FlatIter::new(&self.flat, self.key_size)
            .filter(|(k, _)| !self.data.contains_key(*k))
            .map(|(k, pos)| (Bytes::from(k.to_vec()), pos));
        let btree_entries = self
            .data
            .iter()
            .filter_map(|(k, v)| v.into_wal_position().valid().map(|pos| (k.clone(), pos)));

        let mut all: Vec<(Bytes, WalPosition)> = flat_entries.chain(btree_entries).collect();
        all.sort_by(|(a, _), (b, _)| a.as_ref().cmp(b.as_ref()));
        all.into_iter()
    }

    pub fn keys(&self) -> impl Iterator<Item = Bytes> + '_ {
        // Flat keys not in BTreeMap, plus all BTreeMap keys.
        let flat_keys = FlatIter::new(&self.flat, self.key_size)
            .filter(|(k, _)| !self.data.contains_key(*k))
            .map(|(k, _)| Bytes::from(k.to_vec()));
        let btree_keys = self.data.keys().cloned();

        let mut all: Vec<Bytes> = flat_keys.chain(btree_keys).collect();
        all.sort();
        all.into_iter()
    }

    /// Writes key-value pairs from IndexTable to a BytesMut buffer.
    /// Returns the populated buffer.
    pub fn serialize_index_entries(&self, ks: &KeySpaceDesc, out: &mut BytesMut) {
        self.serialize_index_entries_with_visitor(ks, out, &mut ());
    }

    /// Same as serialize_index_entries, but a visitor can be passed.
    /// This visitor is called every time key-value pair is serialized.
    pub fn serialize_index_entries_with_visitor(
        &self,
        ks: &KeySpaceDesc,
        out: &mut BytesMut,
        visitor: &mut impl IndexSerializationVisitor,
    ) {
        if self.flat.is_empty() {
            // Fast path: only BTreeMap entries.
            for (key, value) in self.data.iter() {
                if value.is_removed() {
                    continue;
                }
                self.write_key_val(ks, out, visitor, key, value.ensure_clean_wal_position());
            }
            return;
        }

        // Merge-sort flat and BTreeMap entries; BTreeMap overrides flat for the same key.
        let flat = &self.flat[..];

        // Use owned Vec<u8> for flat keys to avoid borrow conflicts.
        let mut cur_flat: Option<(Vec<u8>, WalPosition)> =
            FlatIter::new(flat, self.key_size).next().map(|(k, p)| (k.to_vec(), p));
        let mut btree_it = self.data.iter();
        let mut cur_btree: Option<(&Bytes, &IndexWalPosition)> = btree_it.next();

        loop {
            let cmp = match (&cur_flat, cur_btree) {
                (None, None) => break,
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some((fk, _)), Some((bk, _))) => fk.as_slice().cmp(bk.as_ref()),
            };

            match cmp {
                Ordering::Less => {
                    // Flat key is smaller (or btree is empty): output flat entry.
                    let (fk, fp) = cur_flat.take().unwrap();
                    self.write_key_val(ks, out, visitor, fk.as_slice(), fp);
                    cur_flat = {
                        // Advance flat iterator: we need to find the entry after the one we just wrote.
                        // Re-scan from the beginning using the key we just wrote, since FlatIter doesn't have position.
                        // More efficient: track position manually.
                        // For now, use a fresh FlatIter and skip until > fk.
                        FlatIter::new(flat, self.key_size)
                            .find(|(k, _)| *k > fk.as_slice())
                            .map(|(k, p)| (k.to_vec(), p))
                    };
                }
                Ordering::Equal => {
                    // Same key: BTreeMap overrides flat.
                    cur_flat = {
                        let fk = cur_flat.take().unwrap().0;
                        FlatIter::new(flat, self.key_size)
                            .find(|(k, _)| *k > fk.as_slice())
                            .map(|(k, p)| (k.to_vec(), p))
                    };
                    let (bk, bv) = cur_btree.take().unwrap();
                    cur_btree = btree_it.next();
                    if !bv.is_removed() {
                        self.write_key_val(ks, out, visitor, bk, bv.ensure_clean_wal_position());
                    }
                }
                Ordering::Greater => {
                    // BTreeMap key is smaller (or flat is empty): output btree entry.
                    let (bk, bv) = cur_btree.take().unwrap();
                    cur_btree = btree_it.next();
                    if !bv.is_removed() {
                        self.write_key_val(ks, out, visitor, bk, bv.ensure_clean_wal_position());
                    }
                }
            }
        }
    }

    fn write_key_val(
        &self,
        ks: &KeySpaceDesc,
        out: &mut BytesMut,
        visitor: &mut impl IndexSerializationVisitor,
        key: &[u8],
        pos: WalPosition,
    ) {
        visitor.add_key(key, out.len());
        if let Some(expected_size) = ks.index_key_size() {
            if key.len() != expected_size {
                panic!(
                    "Index in ks {} contains key length {} (configured {})",
                    ks.name(),
                    key.len(),
                    expected_size
                );
            }
        } else {
            if key.len() > MAX_U16_VARINT as usize {
                panic!(
                    "Trying to insert {key:?} into ks {}, key length is {}, maximum allowed {}",
                    ks.name(),
                    key.len(),
                    MAX_U16_VARINT
                );
            }
            serialize_u16_varint(key.len() as u16, out);
        }
        out.put_slice(key);
        pos.write_to_buf(out);
    }

    /// Deserializes IndexTable from bytes
    /// - data_offset: Where actual data begins (after any headers)
    /// - ks: KeySpaceDesc to determine element sizes
    /// - b: Source bytes
    ///   Returns the deserialized IndexTable
    pub fn deserialize_index_entries(ks: &KeySpaceDesc, bytes: Bytes) -> Self {
        let mut bytes = SliceBuf::new(bytes);
        let mut data = BTreeMap::new();
        let mut key_bytes = 0usize;
        while bytes.has_remaining() {
            let key = if let Some(key_len) = ks.index_key_size() {
                bytes.slice_n(key_len)
            } else {
                let key_len = deserialize_u16_varint(&mut bytes);
                bytes.slice_n(key_len as usize)
            };
            let value = WalPosition::read_from_buf(&mut bytes);
            key_bytes += key.len();
            if data
                .insert(key, IndexWalPosition::new_clean(value))
                .is_some()
            {
                panic!("Duplicate keys detected in index");
            }
        }
        IndexTable {
            data,
            key_bytes,
            flat: Bytes::default(),
            flat_key_bytes: 0,
            key_size: ks.index_key_size(),
        }
    }

    /// Marks all elements in this index as clean and remove tombstones.
    /// When flat is non-empty, Removed tombstones are kept in BTreeMap so they continue
    /// to shadow flat entries until the next flush/unload.
    pub fn clean_self(&mut self) {
        let has_flat = !self.flat.is_empty();
        self.retain(|_, v| match v.kind {
            IndexEntryKind::Clean => true,
            IndexEntryKind::Modified => {
                *v = v.as_clean_modified();
                true
            }
            IndexEntryKind::Removed => {
                // Keep tombstone if flat might contain this key; it prevents the flat
                // entry from appearing alive after the BTreeMap tombstone is removed.
                has_flat
            }
        });
    }

    /// Retain only unprocessed entries
    pub fn retain_unprocessed(&mut self, last_processed: LastProcessed) {
        self.retain(|_, v| !last_processed.is_processed(v));
    }

    /// Returns if this index table has any unprocessed entries.
    pub fn has_unprocessed(&self, last_processed: LastProcessed) -> bool {
        self.data
            .iter()
            .any(|(_k, v)| !last_processed.is_processed(v))
    }

    /// Retain only entries with offset >= last_processed
    pub fn retain_above_position(&mut self, last_processed: u64) {
        // todo ideally should use LastProcessed::is_processed here
        self.retain(|_, v| v.offset >= last_processed);
    }

    /// Compact index by retaining keys identified by the compactor.
    /// The compactor receives a double-ended iterator over all keys and returns keys to retain.
    pub fn compact_with<F>(&mut self, compactor: F)
    where
        F: FnOnce(&mut dyn DoubleEndedIterator<Item = &Bytes>) -> HashSet<Bytes>,
    {
        // Collect all valid unique keys from both flat and BTreeMap in sorted order.
        let all_keys: Vec<Bytes> = {
            let flat_only = FlatIter::new(&self.flat, self.key_size)
                .filter(|(k, _)| !self.data.contains_key(*k))
                .map(|(k, _)| Bytes::from(k.to_vec()));
            let btree_valid = self
                .data
                .iter()
                .filter(|(_, v)| !v.is_removed())
                .map(|(k, _)| k.clone());
            let mut v: Vec<Bytes> = flat_only.chain(btree_valid).collect();
            v.sort();
            v
        };

        let mut iter = all_keys.iter();
        let to_retain = compactor(&mut iter);
        drop(iter);
        drop(all_keys);

        self.retain(|k, _| to_retain.contains(k));

        if !self.flat.is_empty() {
            let retained: Vec<(Bytes, WalPosition)> = FlatIter::new(&self.flat, self.key_size)
                .filter(|(k, _)| to_retain.contains(&Bytes::from(k.to_vec())))
                .map(|(k, p)| (Bytes::from(k.to_vec()), p))
                .collect();
            self.flat_key_bytes = retained.iter().map(|(k, _)| k.len()).sum();
            self.flat = build_flat_bytes(retained, self.key_size);
        }
    }

    /// Apply given update function to a value with a given key.
    /// If the key does not exist, this function completes without calling the update function
    pub fn apply_update(&mut self, key: &[u8], update: impl FnOnce(&mut IndexWalPosition)) {
        let value = self.data.get_mut(key);
        if let Some(value) = value {
            update(value)
        }
    }

    /// Total bytes occupied by keys in this table (data + flat). Always O(1).
    pub fn total_key_bytes(&self) -> usize {
        self.key_bytes + self.flat_key_bytes
    }

    /// Move all clean entries from the BTreeMap into the flat array, then remove them
    /// from BTreeMap. Entries from both old flat and clean BTreeMap entries are merged
    /// into a new sorted flat. Removed (tombstone) entries in BTreeMap remain to shadow
    /// corresponding flat entries on future reads.
    ///
    pub fn promote_to_flat(&mut self) {
        let has_clean = self.data.values().any(|v| v.is_clean());
        if !has_clean && self.flat.is_empty() {
            return;
        }

        // Collect old flat entries. FlatIter handles both format variants.
        let old_flat = std::mem::take(&mut self.flat);
        let flat_entries: Vec<(Bytes, WalPosition)> = FlatIter::new(&old_flat, self.key_size)
            .map(|(k, p)| (Bytes::from(k.to_vec()), p))
            .collect();

        // Collect clean BTreeMap entries sorted by key (BTreeMap is already sorted).
        let btree_clean: Vec<(&Bytes, WalPosition)> = self
            .data
            .iter()
            .filter(|(_, v)| v.is_clean())
            .map(|(k, v)| (k, v.into_wal_position()))
            .collect();

        // Merge sorted flat entries + sorted clean BTreeMap entries.
        let mut result: Vec<(Bytes, WalPosition)> = Vec::new();
        let mut fi = 0usize;
        let mut bi = 0usize;
        let flat_count = flat_entries.len();

        while fi < flat_count || bi < btree_clean.len() {
            match (flat_entries.get(fi), btree_clean.get(bi)) {
                (None, None) => break,
                (Some((fk, fv)), None) => {
                    // Only in flat: include unless there is a Removed tombstone in BTreeMap.
                    if !self.data.get(fk.as_ref()).map(|v| v.is_removed()).unwrap_or(false) {
                        result.push((fk.clone(), *fv));
                    }
                    fi += 1;
                }
                (None, Some((bk, bv))) => {
                    result.push(((*bk).clone(), *bv));
                    bi += 1;
                }
                (Some((fk, fv)), Some((bk, bv))) => match fk.as_ref().cmp(bk.as_ref()) {
                    Ordering::Less => {
                        if !self.data.get(fk.as_ref()).map(|v| v.is_removed()).unwrap_or(false) {
                            result.push((fk.clone(), *fv));
                        }
                        fi += 1;
                    }
                    Ordering::Equal => {
                        // BTreeMap clean entry overrides flat entry for the same key.
                        result.push(((*bk).clone(), *bv));
                        fi += 1;
                        bi += 1;
                    }
                    Ordering::Greater => {
                        result.push(((*bk).clone(), *bv));
                        bi += 1;
                    }
                },
            }
        }

        // Update key_bytes: subtract clean entries leaving data.
        let clean_key_bytes: usize = self.data.iter()
            .filter(|(_, v)| v.is_clean())
            .map(|(k, _)| k.len())
            .sum();
        self.key_bytes -= clean_key_bytes;

        self.flat_key_bytes = result.iter().map(|(k, _)| k.len()).sum();
        self.flat = build_flat_bytes(result, self.key_size);
        // Remove clean entries from BTreeMap; they are now stored in flat.
        self.data.retain(|_, v| !v.is_clean());
    }

    // ---------------------------------------------------------------------------
    // Private flat-array helpers
    // ---------------------------------------------------------------------------

    fn flat_len(&self) -> usize {
        if self.flat.is_empty() {
            return 0;
        }
        match self.key_size {
            None => var_flat_entry_count(&self.flat),
            Some(key_size) => self.flat.len() / (key_size + WalPosition::LENGTH),
        }
    }

    /// Binary search the flat array for an exact key match.
    fn flat_binary_search(&self, key: &[u8]) -> Option<WalPosition> {
        if self.flat.is_empty() {
            return None;
        }
        let flat = &self.flat[..];
        match self.key_size {
            None => {
                let count = var_flat_entry_count(flat);
                if count == 0 {
                    return None;
                }
                let entries_start = var_flat_entries_section_start(count);
                let entries = &flat[entries_start..];
                let mut lo = 0usize;
                let mut hi = count;
                while lo < hi {
                    let mid = (lo + hi) / 2;
                    let off = var_flat_read_entry_offset(flat, mid);
                    let (mid_key, mid_pos) = var_flat_parse_entry(entries, off);
                    match mid_key.cmp(key) {
                        Ordering::Equal => return Some(mid_pos),
                        Ordering::Less => lo = mid + 1,
                        Ordering::Greater => hi = mid,
                    }
                }
                None
            }
            Some(key_size) => {
                let elem_size = key_size + WalPosition::LENGTH;
                let count = flat.len() / elem_size;
                if count == 0 {
                    return None;
                }
                let mut lo = 0usize;
                let mut hi = count;
                while lo < hi {
                    let mid = (lo + hi) / 2;
                    let start = mid * elem_size;
                    let mid_key = &flat[start..start + key_size];
                    match mid_key.cmp(key) {
                        Ordering::Equal => {
                            return Some(WalPosition::from_slice(
                                &flat[start + key_size..start + elem_size],
                            ));
                        }
                        Ordering::Less => lo = mid + 1,
                        Ordering::Greater => hi = mid,
                    }
                }
                None
            }
        }
    }

    /// Return the first flat entry with key strictly greater than `prev`
    /// (or the first entry at all when `prev` is `None`).
    fn flat_next_forward(&self, prev: Option<&[u8]>) -> Option<(Bytes, WalPosition)> {
        if self.flat.is_empty() {
            return None;
        }
        let flat = &self.flat[..];
        match self.key_size {
            None => {
                let count = var_flat_entry_count(flat);
                if count == 0 {
                    return None;
                }
                let entries_start = var_flat_entries_section_start(count);
                let entries = &flat[entries_start..];
                let start_idx = if let Some(prev) = prev {
                    let mut lo = 0usize;
                    let mut hi = count;
                    while lo < hi {
                        let mid = (lo + hi) / 2;
                        let off = var_flat_read_entry_offset(flat, mid);
                        let (mid_key, _) = var_flat_parse_entry(entries, off);
                        if mid_key <= prev { lo = mid + 1; } else { hi = mid; }
                    }
                    lo
                } else {
                    0
                };
                if start_idx >= count {
                    return None;
                }
                let off = var_flat_read_entry_offset(flat, start_idx);
                let (key, pos) = var_flat_parse_entry(entries, off);
                // entries_start + off + 2 (key_len u16)
                let key_abs_start = entries_start + off + 2;
                let key_bytes = self.flat.slice(key_abs_start..key_abs_start + key.len());
                Some((key_bytes, pos))
            }
            Some(key_size) => {
                let elem_size = key_size + WalPosition::LENGTH;
                let count = flat.len() / elem_size;
                if count == 0 {
                    return None;
                }
                let start_idx = if let Some(prev) = prev {
                    let mut lo = 0usize;
                    let mut hi = count;
                    while lo < hi {
                        let mid = (lo + hi) / 2;
                        let mid_key = &flat[mid * elem_size..mid * elem_size + key_size];
                        if mid_key <= prev { lo = mid + 1; } else { hi = mid; }
                    }
                    lo
                } else {
                    0
                };
                if start_idx >= count {
                    return None;
                }
                let key_abs_start = start_idx * elem_size;
                let key_bytes = self.flat.slice(key_abs_start..key_abs_start + key_size);
                let pos = WalPosition::from_slice(
                    &flat[key_abs_start + key_size..key_abs_start + elem_size],
                );
                Some((key_bytes, pos))
            }
        }
    }

    /// Return the last flat entry with key strictly less than `prev`
    /// (or the last entry at all when `prev` is `None`).
    fn flat_next_reverse(&self, prev: Option<&[u8]>) -> Option<(Bytes, WalPosition)> {
        if self.flat.is_empty() {
            return None;
        }
        let flat = &self.flat[..];
        match self.key_size {
            None => {
                let count = var_flat_entry_count(flat);
                if count == 0 {
                    return None;
                }
                let entries_start = var_flat_entries_section_start(count);
                let entries = &flat[entries_start..];
                let end_idx = if let Some(prev) = prev {
                    let mut lo = 0usize;
                    let mut hi = count;
                    while lo < hi {
                        let mid = (lo + hi) / 2;
                        let off = var_flat_read_entry_offset(flat, mid);
                        let (mid_key, _) = var_flat_parse_entry(entries, off);
                        if mid_key < prev { lo = mid + 1; } else { hi = mid; }
                    }
                    if lo == 0 {
                        return None;
                    }
                    lo - 1
                } else {
                    count - 1
                };
                let off = var_flat_read_entry_offset(flat, end_idx);
                let (key, pos) = var_flat_parse_entry(entries, off);
                let key_abs_start = entries_start + off + 2;
                let key_bytes = self.flat.slice(key_abs_start..key_abs_start + key.len());
                Some((key_bytes, pos))
            }
            Some(key_size) => {
                let elem_size = key_size + WalPosition::LENGTH;
                let count = flat.len() / elem_size;
                if count == 0 {
                    return None;
                }
                let end_idx = if let Some(prev) = prev {
                    let mut lo = 0usize;
                    let mut hi = count;
                    while lo < hi {
                        let mid = (lo + hi) / 2;
                        let mid_key = &flat[mid * elem_size..mid * elem_size + key_size];
                        if mid_key < prev { lo = mid + 1; } else { hi = mid; }
                    }
                    if lo == 0 {
                        return None;
                    }
                    lo - 1
                } else {
                    count - 1
                };
                let key_abs_start = end_idx * elem_size;
                let key_bytes = self.flat.slice(key_abs_start..key_abs_start + key_size);
                let pos = WalPosition::from_slice(
                    &flat[key_abs_start + key_size..key_abs_start + elem_size],
                );
                Some((key_bytes, pos))
            }
        }
    }

    #[cfg(test)]
    pub fn into_data(self) -> BTreeMap<Bytes, WalPosition> {
        self.data
            .into_iter()
            .map(|(k, v)| (k, v.into_wal_position()))
            .collect()
    }
}

impl IndexWalPosition {
    fn new_clean(w: WalPosition) -> Self {
        debug_assert_ne!(w, WalPosition::INVALID);
        Self {
            offset: w.offset(),
            len: w.frame_len_u32(),
            kind: IndexEntryKind::Clean,
        }
    }

    fn new_modified(w: WalPosition) -> Self {
        debug_assert_ne!(w, WalPosition::INVALID);
        Self {
            offset: w.offset(),
            len: w.frame_len_u32(),
            kind: IndexEntryKind::Modified,
        }
    }

    fn new_removed(w: WalPosition) -> Self {
        debug_assert_ne!(w, WalPosition::INVALID);
        Self {
            offset: w.offset(),
            len: w.frame_len_u32(),
            kind: IndexEntryKind::Removed,
        }
    }

    /// If the index position is clean or modified, return the corresponding wal position.
    /// If the index wal position is removed, returns WalPosition::INVALID.
    fn into_wal_position(self) -> WalPosition {
        match self.kind {
            IndexEntryKind::Clean | IndexEntryKind::Modified => {
                WalPosition::new(self.offset, self.len)
            }
            IndexEntryKind::Removed => WalPosition::INVALID,
        }
    }

    pub fn replace_wal_position(&mut self, position: WalPosition) {
        self.offset = position.offset();
        self.len = position.frame_len_u32();
    }

    fn into_update_position(self) -> WalPosition {
        WalPosition::new(self.offset, self.len)
    }

    fn ensure_clean_wal_position(self) -> WalPosition {
        assert_eq!(self.kind, IndexEntryKind::Clean);
        debug_assert_ne!(self.offset, u64::MAX);
        WalPosition::new(self.offset, self.len)
    }

    pub fn is_removed(&self) -> bool {
        self.kind == IndexEntryKind::Removed
    }

    pub fn is_clean(&self) -> bool {
        self.kind == IndexEntryKind::Clean
    }

    /// Change modified entry into clean entry.
    pub fn as_clean_modified(mut self) -> Self {
        match self.kind {
            IndexEntryKind::Clean | IndexEntryKind::Modified => {}
            IndexEntryKind::Removed => {
                panic!("Can't call as_clean_modified on IndexEntryKind::Removed")
            }
        }
        self.kind = IndexEntryKind::Clean;
        self
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }
}

impl HasOffset for IndexWalPosition {
    fn offset(&self) -> u64 {
        self.offset
    }
}

pub trait IndexSerializationVisitor {
    fn add_key(&mut self, key: &[u8], offset: usize);
}

impl IndexSerializationVisitor for () {
    fn add_key(&mut self, _key: &[u8], _offset: usize) {}
}

/// An iterator for unloaded portion of an index for variable length keys.
pub struct VariableLenKeyIndexIterator<'a> {
    buf: SliceCursor<'a>,
}

impl<'a> VariableLenKeyIndexIterator<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            buf: SliceCursor::new(buf),
        }
    }
}

impl<'a> Iterator for VariableLenKeyIndexIterator<'a> {
    type Item = (usize, &'a [u8], WalPosition);

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            return None;
        }
        let offset = self.buf.offset();
        let len = deserialize_u16_varint(&mut self.buf);
        let key = self.buf.take_slice(len as usize);
        let wal_position = WalPosition::read_from_buf(&mut self.buf);
        Some((offset, key, wal_position))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key_shape::{KeyIndexing, KeyShape, KeyType};

    #[test]
    fn test_var_len_iterator() {
        let data = BTreeMap::from_iter([
            (vec![1, 2, 3].into(), iwp(22)),
            (vec![2, 5].into(), iwp(23)),
            (vec![].into(), iwp(25)),
        ]);
        let key_bytes: usize = data.keys().map(|k: &Bytes| k.len()).sum();
        let index = IndexTable {
            data,
            key_bytes,
            flat: Default::default(),
            flat_key_bytes: 0,
            key_size: None,
        };
        let (shape, ks) = KeyShape::new_single_config_indexing(
            KeyIndexing::variable_length(),
            1,
            KeyType::uniform(1),
            Default::default(),
        );
        let ks = shape.ks(ks);
        let mut buf = BytesMut::new();
        index.serialize_index_entries(ks, &mut buf);
        let mut iterator = VariableLenKeyIndexIterator::new(&buf);
        assert_eq!((0, u8ref(&[]), wp(25)), iterator.next().unwrap());
        assert_eq!((13, u8ref(&[1, 2, 3]), wp(22)), iterator.next().unwrap());
        assert_eq!((13 + 16, u8ref(&[2, 5]), wp(23)), iterator.next().unwrap());
    }

    fn iwp(n: u64) -> IndexWalPosition {
        IndexWalPosition::new_clean(wp(n))
    }
    fn wp(n: u64) -> WalPosition {
        WalPosition::new(n, 15)
    }

    fn u8ref(a: &[u8]) -> &[u8] {
        a
    }

    #[test]
    fn test_unmerge_flushed() {
        let mut index = IndexTable::default();
        index.insert(vec![1].into(), WalPosition::test_value(2));
        index.insert(vec![2].into(), WalPosition::test_value(3));
        index.insert(vec![6].into(), WalPosition::test_value(4));
        let mut index2 = index.clone();
        index2.insert(vec![1].into(), WalPosition::test_value(5));
        index2.insert(vec![3].into(), WalPosition::test_value(8));
        let len_before = index2.len();
        index2.unmerge_flushed(&index, LastProcessed::new_test(u64::MAX));
        let len_after = index2.len();
        assert_eq!(len_after as i64 - len_before as i64, -2);
        let data = index2.into_data().into_iter().collect::<Vec<_>>();
        assert_eq!(
            data,
            vec![
                (vec![1].into(), WalPosition::test_value(5)),
                (vec![3].into(), WalPosition::test_value(8))
            ]
        );
    }

    #[test]
    fn test_unmerge_flushed_with_position_filter() {
        let mut index = IndexTable::default();
        // Original index has entries at positions 2, 3, 4
        index.insert(vec![1].into(), WalPosition::test_value(2));
        index.insert(vec![2].into(), WalPosition::test_value(3));
        index.insert(vec![6].into(), WalPosition::test_value(4));

        let mut index2 = index.clone();
        // New entries at positions 5 and 8
        index2.insert(vec![1].into(), WalPosition::test_value(5));
        index2.insert(vec![3].into(), WalPosition::test_value(8));

        // With last_processed=4, only entries at positions 2 and 3 should be unmerged
        let len_before = index2.len();
        index2.unmerge_flushed(&index, LastProcessed::new_test(4));
        let len_after = index2.len();
        assert_eq!(len_after as i64 - len_before as i64, -1);
        let data = index2.into_data().into_iter().collect::<Vec<_>>();
        assert_eq!(
            data,
            vec![
                (vec![1].into(), WalPosition::test_value(5)),
                (vec![3].into(), WalPosition::test_value(8)),
                (vec![6].into(), WalPosition::test_value(4)) // This one wasn't unmerged because offset > 3
            ]
        );
    }

    #[test]
    fn test_retain_above_position() {
        let mut index = IndexTable::default();
        // Add entries at different positions
        index.insert(vec![1].into(), WalPosition::test_value(100));
        index.insert(vec![2].into(), WalPosition::test_value(200));
        index.insert(vec![3].into(), WalPosition::test_value(300));
        index.insert(vec![4].into(), WalPosition::test_value(400));

        // Retain only entries with offset > 250
        let len_before = index.len();
        index.retain_above_position(250);
        let len_after = index.len();
        assert_eq!(len_before - len_after, 2); // Removed 2 entries

        // Check remaining entries
        assert_eq!(index.len(), 2);
        assert!(index.get(&[1]).is_none()); // offset 100, removed
        assert!(index.get(&[2]).is_none()); // offset 200, removed
        assert_eq!(index.get(&[3]), Some(WalPosition::test_value(300))); // offset 300, kept
        assert_eq!(index.get(&[4]), Some(WalPosition::test_value(400))); // offset 400, kept
    }

    #[test]
    fn test_next_entry() {
        let mut table = IndexTable::default();
        table.insert(vec![1, 2, 3, 4].into(), WalPosition::test_value(1));
        table.insert(vec![1, 2, 3, 7].into(), WalPosition::test_value(2));
        table.insert(vec![1, 2, 4, 5].into(), WalPosition::test_value(3));

        // Reverse = false

        // existing element - next found exclusive
        let next = table.next_entry(Some(vec![1, 2, 3, 7].into()), false);
        let next = next.unwrap();
        assert_eq!(next.0, Bytes::from(vec![1, 2, 4, 5]));
        assert_eq!(next.1, WalPosition::test_value(3));

        // not existing element - next found exclusive
        let next = table.next_entry(Some(vec![1, 2, 3, 6].into()), false);
        let next = next.unwrap();
        assert_eq!(next.0, Bytes::from(vec![1, 2, 3, 7]));
        assert_eq!(next.1, WalPosition::test_value(2));

        // existing element - next not found exclusive
        let next = table.next_entry(Some(vec![1, 2, 4, 5].into()), false);
        assert!(next.is_none());

        // not existing element - next not found inclusive
        let next = table.next_entry(Some(vec![1, 2, 4, 6].into()), false);
        assert!(next.is_none());

        // Reverse = true

        // existing element - next found exclusive
        let next = table.next_entry(Some(vec![1, 2, 3, 7].into()), true);
        let next = next.unwrap();
        assert_eq!(next.0, Bytes::from(vec![1, 2, 3, 4]));
        assert_eq!(next.1, WalPosition::test_value(1));

        // not existing element - next found inclusive
        let next = table.next_entry(Some(vec![1, 2, 3, 8].into()), true);
        let next = next.unwrap();
        assert_eq!(next.0, Bytes::from(vec![1, 2, 3, 7]));
        assert_eq!(next.1, WalPosition::test_value(2));

        // existing element - next not found exclusive
        let next = table.next_entry(Some(vec![1, 2, 3, 4].into()), true);
        assert!(next.is_none());

        // not existing element - next not found inclusive
        let next = table.next_entry(Some(vec![1, 2, 3, 3].into()), true);
        assert!(next.is_none());
    }

    #[test]
    fn test_insert_while_iterating() {
        let mut table = IndexTable::default();
        table.insert(vec![1, 2, 3, 4].into(), WalPosition::test_value(1));
        table.insert(vec![1, 2, 3, 7].into(), WalPosition::test_value(2));

        let next = table.next_entry(Some(vec![1, 2, 3, 4].into()), false);
        let next = next.unwrap();
        assert_eq!(next.0, Bytes::from(vec![1, 2, 3, 7]));
        assert_eq!(next.1, WalPosition::test_value(2));

        table.insert(vec![1, 2, 3, 6].into(), WalPosition::test_value(3));
        let next = table.next_entry(Some(vec![1, 2, 3, 4].into()), false);
        let next = next.unwrap();
        assert_eq!(next.0, Bytes::from(vec![1, 2, 3, 6]));
        assert_eq!(next.1, WalPosition::test_value(3));
    }

    #[test]
    fn test_promote_to_flat_basic() {
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(1));
        table.insert(vec![2].into(), WalPosition::test_value(2));
        table.insert(vec![3].into(), WalPosition::test_value(3));
        // Clean entries (simulate loaded state).
        table
            .data
            .values_mut()
            .for_each(|v| *v = v.as_clean_modified());

        table.promote_to_flat();

        // BTreeMap should be empty after promotion.
        assert!(table.data.is_empty());
        assert_eq!(table.flat_len(), 3);

        // Lookups should still work.
        assert_eq!(table.get(&[1]), Some(WalPosition::test_value(1)));
        assert_eq!(table.get(&[2]), Some(WalPosition::test_value(2)));
        assert_eq!(table.get(&[3]), Some(WalPosition::test_value(3)));
        assert_eq!(table.get(&[4]), None);
    }

    #[test]
    fn test_promote_to_flat_with_remove() {
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(1));
        table.insert(vec![2].into(), WalPosition::test_value(2));
        table
            .data
            .values_mut()
            .for_each(|v| *v = v.as_clean_modified());
        // First promotion: flat gets [1, 2].
        table.promote_to_flat();

        // Now remove key [1] (goes to BTreeMap as Removed).
        table.remove(vec![1].into(), WalPosition::test_value(10));
        assert_eq!(table.get(&[1]), Some(WalPosition::INVALID)); // deleted

        // Add a new key.
        table.insert(vec![3].into(), WalPosition::test_value(5));
        table.data.values_mut().for_each(|v| {
            if v.is_modified() {
                *v = v.as_clean_modified();
            }
        });

        // Second promotion: flat should have [2, 3], key [1] not in flat.
        // The Removed tombstone for key [1] stays in BTreeMap for dirty tracking,
        // so get() returns INVALID (not None).
        table.promote_to_flat();
        assert_eq!(table.flat_len(), 2);
        assert_eq!(table.get(&[1]), Some(WalPosition::INVALID)); // deleted
        assert_eq!(table.get(&[2]), Some(WalPosition::test_value(2)));
        assert_eq!(table.get(&[3]), Some(WalPosition::test_value(5)));
    }

    #[test]
    fn test_next_entry_after_promote() {
        let mut table = IndexTable::default();
        table.insert(vec![1, 2, 3, 4].into(), WalPosition::test_value(1));
        table.insert(vec![1, 2, 3, 7].into(), WalPosition::test_value(2));
        table.insert(vec![1, 2, 4, 5].into(), WalPosition::test_value(3));
        table
            .data
            .values_mut()
            .for_each(|v| *v = v.as_clean_modified());
        table.promote_to_flat();

        // Forward traversal after promotion.
        let next = table.next_entry(Some(vec![1, 2, 3, 7].into()), false);
        let next = next.unwrap();
        assert_eq!(next.0, Bytes::from(vec![1, 2, 4, 5]));

        // Reverse traversal.
        let next = table.next_entry(Some(vec![1, 2, 3, 7].into()), true);
        let next = next.unwrap();
        assert_eq!(next.0, Bytes::from(vec![1, 2, 3, 4]));

        // First entry (no prev).
        let next = table.next_entry(None, false);
        assert_eq!(next.unwrap().0, Bytes::from(vec![1, 2, 3, 4]));

        // Last entry (no prev, reverse).
        let next = table.next_entry(None, true);
        assert_eq!(next.unwrap().0, Bytes::from(vec![1, 2, 4, 5]));
    }

    fn k4(v: u32) -> Bytes {
        Bytes::from(v.to_be_bytes().to_vec())
    }

    #[test]
    fn test_promote_to_flat_fixed_len_basic() {
        let mut table = IndexTable::default();
        table.insert(k4(10), WalPosition::test_value(1));
        table.insert(k4(20), WalPosition::test_value(2));
        table.insert(k4(30), WalPosition::test_value(3));
        table.data.values_mut().for_each(|v| *v = v.as_clean_modified());

        table.key_size = Some(4);
        table.promote_to_flat();

        assert!(table.data.is_empty());
        assert_eq!(table.flat_len(), 3);
        // Fixed-length format: 3*(4+12) = 48 bytes, no header
        assert_eq!(table.flat.len(), 3 * (4 + WalPosition::LENGTH));

        assert_eq!(table.get(&k4(10)), Some(WalPosition::test_value(1)));
        assert_eq!(table.get(&k4(20)), Some(WalPosition::test_value(2)));
        assert_eq!(table.get(&k4(30)), Some(WalPosition::test_value(3)));
        assert_eq!(table.get(&k4(15)), None);
    }

    #[test]
    fn test_promote_to_flat_fixed_len_next_entry() {
        let mut table = IndexTable::default();
        table.insert(k4(10), WalPosition::test_value(1));
        table.insert(k4(20), WalPosition::test_value(2));
        table.insert(k4(30), WalPosition::test_value(3));
        table.data.values_mut().for_each(|v| *v = v.as_clean_modified());
        table.key_size = Some(4);
        table.promote_to_flat();

        // Forward from k4(10) → k4(20)
        let next = table.next_entry(Some(k4(10)), false).unwrap();
        assert_eq!(next.0, k4(20));
        assert_eq!(next.1, WalPosition::test_value(2));

        // Reverse from k4(30) → k4(20)
        let next = table.next_entry(Some(k4(30)), true).unwrap();
        assert_eq!(next.0, k4(20));
        assert_eq!(next.1, WalPosition::test_value(2));

        // First (no prev, forward)
        assert_eq!(table.next_entry(None, false).unwrap().0, k4(10));
        // Last (no prev, reverse)
        assert_eq!(table.next_entry(None, true).unwrap().0, k4(30));

        // Past end → None
        assert!(table.next_entry(Some(k4(30)), false).is_none());
        // Before start → None
        assert!(table.next_entry(Some(k4(10)), true).is_none());
    }

    #[test]
    fn test_promote_to_flat_fixed_len_with_remove() {
        let mut table = IndexTable::default();
        table.insert(k4(10), WalPosition::test_value(1));
        table.insert(k4(20), WalPosition::test_value(2));
        table.data.values_mut().for_each(|v| *v = v.as_clean_modified());
        table.key_size = Some(4);
        table.promote_to_flat();
        assert_eq!(table.flat_len(), 2);

        // Remove k4(10); tombstone stays in BTreeMap.
        table.remove(k4(10), WalPosition::test_value(5));
        assert_eq!(table.get(&k4(10)), Some(WalPosition::INVALID));

        // Add k4(30) and promote again.
        table.insert(k4(30), WalPosition::test_value(6));
        table.data.values_mut().for_each(|v| {
            if v.is_modified() {
                *v = v.as_clean_modified();
            }
        });
        table.key_size = Some(4);
        table.promote_to_flat();

        // flat: [20, 30]; k4(10) shadowed by Removed tombstone.
        assert_eq!(table.flat_len(), 2);
        assert_eq!(table.get(&k4(10)), Some(WalPosition::INVALID));
        assert_eq!(table.get(&k4(20)), Some(WalPosition::test_value(2)));
        assert_eq!(table.get(&k4(30)), Some(WalPosition::test_value(6)));

        // next_entry returns the Removed tombstone for k4(10) (INVALID pos) so callers
        // can shadow the corresponding on-disk entry (DirtyUnloaded merge).
        let (key, pos) = table.next_entry(None, false).unwrap();
        assert_eq!(key, k4(10));
        assert_eq!(pos, WalPosition::INVALID);
        // Continuing past the tombstone yields k4(20).
        assert_eq!(table.next_entry(Some(k4(10)), false).unwrap().0, k4(20));
    }
}

// Helper for tests only.
#[cfg(test)]
impl IndexWalPosition {
    fn is_modified(&self) -> bool {
        self.kind == IndexEntryKind::Modified
    }
}
