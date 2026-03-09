mod flat;
use flat::{
    FlatIter, build_flat_bytes, flat_count, flat_entry_at, flat_lower_bound, flat_upper_bound,
};

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
    /// Write buffer: new entries land here before the next `promote_to_flat` call.
    /// After `promote_to_flat` this is always empty.
    data: BTreeMap<Bytes, IndexWalPosition>,
    /// Sum of all key lengths currently in `data`. Maintained incrementally.
    key_bytes: usize,
    /// Count of dirty (Modified or Removed) entries across both `data` and `flat`.
    /// Maintained incrementally for O(1) access via `dirty_count()`.
    dirty_count: usize,
    /// Primary storage for all entries in compact binary format.
    ///
    /// Two formats depending on whether the key space uses fixed-length keys:
    /// - Variable-length: `[count: u32][offsets: u32*count][key_len: u16][key][wal_offset: u64][encoded_len: u32]*`
    /// - Fixed-length:    `[key (n)][wal_offset: u64][encoded_len: u32]*`
    ///
    /// `encoded_len` packs the entry kind into the top 2 bits of the length field
    /// (bits 31-30: 0=Clean, 1=Modified, 2=Removed; bits 29-0: actual frame length).
    /// This is valid because WAL frame sizes are always < 1 GiB (2^30), as asserted
    /// in `Wal::multi_write`.
    ///
    /// Stores Clean, Modified, and Removed entries. Entries in `data` (the write
    /// buffer) take priority over entries in `flat` for the same key.
    flat: Bytes,
    /// Fixed key size for this key space, or `None` for variable-length keys.
    /// Determines which flat format is used.
    key_size: Option<usize>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct IndexWalPosition {
    pub(super) offset: u64,
    pub(super) len: u32,
    pub(super) kind: IndexEntryKind,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(super) enum IndexEntryKind {
    Clean,
    Modified,
    Removed,
}

impl IndexEntryKind {
    pub(super) fn to_u8(self) -> u8 {
        match self {
            IndexEntryKind::Clean => 0,
            IndexEntryKind::Modified => 1,
            IndexEntryKind::Removed => 2,
        }
    }

    pub(super) fn from_u8(b: u8) -> Self {
        match b {
            1 => IndexEntryKind::Modified,
            2 => IndexEntryKind::Removed,
            _ => IndexEntryKind::Clean,
        }
    }
}

// Compile time check to ensure IndexWalPosition consumes the same amount of memory as WalPosition
const _: [u8; size_of::<WalPosition>()] = [0u8; size_of::<IndexWalPosition>()];

/// Adjust `dirty_count` when an entry transitions between clean and dirty states.
/// Takes a direct reference to the counter to avoid borrow conflicts when `self.data` is held.
fn adjust_dirty_count(dirty_count: &mut usize, was_dirty: bool, is_dirty: bool) {
    match (was_dirty, is_dirty) {
        (true, false) => *dirty_count -= 1,
        (false, true) => *dirty_count += 1,
        _ => {}
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

        // Enforce the ordering assertion for keys that were promoted to flat (not in BTreeMap yet).
        // Must be done before self.data.entry() to avoid conflicting borrows.
        #[cfg(any(debug_assertions, feature = "test_methods"))]
        if !self.data.contains_key(&k)
            && let Some(existing) = self.flat_binary_search(&k)
        {
            assert!(
                existing.offset < v.offset,
                "Index WAL position must be increasing (flat)"
            );
        }

        match self.data.entry(k) {
            Entry::Vacant(va) => {
                self.key_bytes += va.key().len();
                adjust_dirty_count(&mut self.dirty_count, false, !v.is_clean());
                va.insert(v);
                None
            }
            Entry::Occupied(mut oc) => {
                let previous = *oc.get();
                assert!(
                    previous.offset < v.offset,
                    "Index WAL position must be increasing"
                );
                adjust_dirty_count(&mut self.dirty_count, !previous.is_clean(), !v.is_clean());
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
        // Also merge flat entries (present when promote_to_flat has been called on dirty).
        // BTreeMap entries already processed above take priority; skip those keys.
        for (k, v) in FlatIter::new(&dirty.flat, dirty.key_size) {
            if dirty.data.contains_key(k) {
                continue;
            }
            // Zero-copy: k is a subslice of dirty.flat, so slice_ref avoids a heap allocation.
            let key = dirty.flat.slice_to_bytes(k);
            if v.is_removed() {
                if self.flat.is_empty() {
                    if self.data.remove(&key).is_some() {
                        self.key_bytes -= key.len();
                    }
                } else {
                    let len = key.len();
                    if self.data.insert(key, v).is_none() {
                        self.key_bytes += len;
                    }
                }
            } else {
                let len = key.len();
                if self.data.insert(key, v.as_clean_modified()).is_none() {
                    self.key_bytes += len;
                }
            }
        }
        self.dirty_count = self.count_dirty_slow();
    }

    /// Merges dirty IndexTable into a loaded IndexTable, preserving dirty states
    pub fn merge_dirty_no_clean(&mut self, dirty: &Self) {
        // todo implement this efficiently taking into account both self and dirty are sorted
        for (k, v) in dirty.data.iter() {
            if self.data.insert(k.clone(), *v).is_none() {
                self.key_bytes += k.len();
            }
        }
        // Also merge flat entries (present when promote_to_flat has been called on dirty).
        // BTreeMap entries already processed above take priority; skip those keys.
        for (k, v) in FlatIter::new(&dirty.flat, dirty.key_size) {
            if dirty.data.contains_key(k) {
                continue;
            }
            // Zero-copy: k is a subslice of dirty.flat, so slice_ref avoids a heap allocation.
            let key = dirty.flat.slice_to_bytes(k);
            let len = key.len();
            if self.data.insert(key, v).is_none() {
                self.key_bytes += len;
            }
        }
        self.dirty_count = self.count_dirty_slow();
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
        // Also unmerge flat entries (present when promote_to_flat was called on original).
        // Rebuild self.flat, dropping entries whose key+position matches a processed original entry.
        if !original.flat.is_empty() {
            let orig_flat_bytes = original.flat.clone();
            let self_flat_bytes = std::mem::take(&mut self.flat);
            // Peekable iterator over original flat avoids a full Vec allocation.
            let mut orig_it = FlatIter::new(&orig_flat_bytes, original.key_size).peekable();
            let mut kept: Vec<(Bytes, IndexWalPosition)> = Vec::new();
            for (sk, s_iwp) in FlatIter::new(&self_flat_bytes, self.key_size) {
                // Advance orig_it past entries with keys smaller than sk.
                while orig_it.peek().map(|(ok, _)| *ok < sk).unwrap_or(false) {
                    orig_it.next();
                }
                // Check for an exact key match and consume the orig entry if found.
                let should_remove = if orig_it.peek().map(|(ok, _)| *ok == sk).unwrap_or(false) {
                    let (_, o_iwp) = orig_it.next().unwrap();
                    last_processed.is_processed(&o_iwp)
                        && s_iwp.into_wal_position() == o_iwp.into_wal_position()
                } else {
                    false
                };
                if !should_remove {
                    // Zero-copy: sk is a subslice of self_flat_bytes.
                    kept.push((self_flat_bytes.slice_to_bytes(sk), s_iwp));
                }
            }
            self.flat = build_flat_bytes(kept, self.key_size);
        }
        self.dirty_count = self.count_dirty_slow();
    }

    /// Count the number of dirty (Modified or Removed) entries. O(1) — uses cached count.
    pub fn dirty_count(&self) -> usize {
        self.dirty_count
    }

    /// Slow path: recount by iterating all entries. Used to recompute the cache after
    /// bulk mutations that touch both `flat` and `data` in ways too complex to track inline.
    fn count_dirty_slow(&self) -> usize {
        let flat_dirty = FlatIter::new(&self.flat, self.key_size)
            .filter(|(_, iwp)| !iwp.is_clean())
            .count();
        flat_dirty + self.data.values().filter(|p| !p.is_clean()).count()
    }

    fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&Bytes, &mut IndexWalPosition) -> bool,
    {
        let mut removed_bytes = 0usize;
        let mut dirty_delta: i64 = 0;
        self.data.retain(|k, v| {
            let was_dirty = !v.is_clean();
            if f(k, v) {
                let is_dirty = !v.is_clean();
                match (was_dirty, is_dirty) {
                    (true, false) => dirty_delta -= 1,
                    (false, true) => dirty_delta += 1,
                    _ => {}
                }
                true
            } else {
                removed_bytes += k.len();
                if was_dirty {
                    dirty_delta -= 1;
                }
                false
            }
        });
        self.key_bytes -= removed_bytes;
        if dirty_delta >= 0 {
            self.dirty_count += dirty_delta as usize;
        } else {
            self.dirty_count = self.dirty_count.saturating_sub((-dirty_delta) as usize);
        }
    }

    /// Reduces this index to its dirty overlay: keeps only Modified and Removed entries,
    /// dropping all Clean entries. Used when transitioning to DirtyUnloaded state.
    pub fn retain_dirty(&mut self) {
        // Rebuild flat keeping only Modified and Removed entries.
        let old_flat = std::mem::take(&mut self.flat);
        let kept: Vec<(Bytes, IndexWalPosition)> = FlatIter::new(&old_flat, self.key_size)
            .filter(|(_, iwp)| !iwp.is_clean())
            .map(|(k, iwp)| (Bytes::from(k.to_vec()), iwp))
            .collect();
        self.flat = build_flat_bytes(kept, self.key_size);
        // Drop Clean BTreeMap entries too.
        self.retain(|_, pos| !pos.is_clean());
    }

    pub fn get(&self, k: &[u8]) -> Option<WalPosition> {
        // BTreeMap (write buffer) takes priority: may have a more-recent entry.
        if let Some(p) = self.data.get(k) {
            return Some(p.into_wal_position());
        }
        // Fall through to the flat array.
        self.flat_binary_search(k)
            .map(|iwp| iwp.into_wal_position())
    }

    /// Similar to `get`, but returns the WAL position of the update operation for both
    /// inserts and deletes.
    pub fn get_update_position(&self, k: &[u8]) -> Option<WalPosition> {
        if let Some(p) = self.data.get(k) {
            return Some(p.into_update_position());
        }
        self.flat_binary_search(k)
            .map(|iwp| iwp.into_update_position())
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
        // Both sources are sorted and disjoint (flat excludes keys present in data).
        // Merge in O(N+M) rather than collecting and sorting.
        let mut flat_it = FlatIter::new(&self.flat, self.key_size)
            .filter(|(k, _)| !self.data.contains_key(*k))
            .filter_map(|(k, iwp)| {
                iwp.into_wal_position()
                    .valid()
                    .map(|pos| (Bytes::from(k.to_vec()), pos))
            })
            .peekable();
        let mut btree_it = self
            .data
            .iter()
            .filter_map(|(k, v)| v.into_wal_position().valid().map(|pos| (k.clone(), pos)))
            .peekable();

        let mut result = Vec::with_capacity(self.flat_len() + self.data.len());
        loop {
            let cmp = match (flat_it.peek(), btree_it.peek()) {
                (None, None) => break,
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some((fk, _)), Some((bk, _))) => fk.cmp(bk),
            };
            if cmp != Ordering::Greater {
                result.push(flat_it.next().unwrap());
            } else {
                result.push(btree_it.next().unwrap());
            }
        }
        result.extend(flat_it);
        result.extend(btree_it);
        result.into_iter()
    }

    pub fn keys(&self) -> impl Iterator<Item = Bytes> + '_ {
        // Both sources are sorted and disjoint (flat excludes keys present in data).
        // Merge in O(N+M) rather than collecting and sorting.
        let mut flat_it = FlatIter::new(&self.flat, self.key_size)
            .filter(|(k, iwp)| !self.data.contains_key(*k) && !iwp.is_removed())
            .map(|(k, _)| Bytes::from(k.to_vec()))
            .peekable();
        let mut btree_it = self
            .data
            .iter()
            .filter(|(_, v)| !v.is_removed())
            .map(|(k, _)| k.clone())
            .peekable();

        let mut result = Vec::with_capacity(self.flat_len() + self.data.len());
        loop {
            let cmp = match (flat_it.peek(), btree_it.peek()) {
                (None, None) => break,
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some(fk), Some(bk)) => fk.cmp(bk),
            };
            if cmp != Ordering::Greater {
                result.push(flat_it.next().unwrap());
            } else {
                result.push(btree_it.next().unwrap());
            }
        }
        result.extend(flat_it);
        result.extend(btree_it);
        result.into_iter()
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
                self.write_key_val(ks, out, visitor, key, value.into_update_position());
            }
            return;
        }

        // Merge-sort flat and BTreeMap entries; BTreeMap overrides flat for the same key.
        // Removed entries from either source are skipped (tombstones are not written on disk).
        let flat = &self.flat[..];

        // Keep a single stateful FlatIter alive for O(N) sequential traversal.
        let mut flat_iter = FlatIter::new(flat, self.key_size);
        let mut cur_flat: Option<(Vec<u8>, IndexWalPosition)> =
            flat_iter.next().map(|(k, iwp)| (k.to_vec(), iwp));
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
                    // Flat key is smaller (or btree is empty): output flat entry if not Removed.
                    let (fk, fiwp) = cur_flat.take().unwrap();
                    if !fiwp.is_removed() {
                        self.write_key_val(
                            ks,
                            out,
                            visitor,
                            fk.as_slice(),
                            fiwp.into_update_position(),
                        );
                    }
                    cur_flat = flat_iter.next().map(|(k, iwp)| (k.to_vec(), iwp));
                }
                Ordering::Equal => {
                    // Same key: BTreeMap overrides flat; advance both.
                    cur_flat = flat_iter.next().map(|(k, iwp)| (k.to_vec(), iwp));
                    let (bk, bv) = cur_btree.take().unwrap();
                    cur_btree = btree_it.next();
                    if !bv.is_removed() {
                        self.write_key_val(ks, out, visitor, bk, bv.into_update_position());
                    }
                }
                Ordering::Greater => {
                    // BTreeMap key is smaller (or flat is empty): output btree entry if not Removed.
                    let (bk, bv) = cur_btree.take().unwrap();
                    cur_btree = btree_it.next();
                    if !bv.is_removed() {
                        self.write_key_val(ks, out, visitor, bk, bv.into_update_position());
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
            key_size: ks.index_key_size(),
            dirty_count: 0,
        }
    }

    /// Marks all elements in this index as clean and removes tombstones.
    /// Rebuilds flat with Modified→Clean and Removed entries dropped.
    /// After this call both flat and BTreeMap contain only Clean entries.
    pub fn clean_self(&mut self) {
        // Rebuild flat: convert Modified→Clean, drop Removed.
        let old_flat = std::mem::take(&mut self.flat);
        let kept: Vec<(Bytes, IndexWalPosition)> = FlatIter::new(&old_flat, self.key_size)
            .filter(|(_, iwp)| !iwp.is_removed())
            .map(|(k, mut iwp)| {
                iwp.kind = IndexEntryKind::Clean;
                (Bytes::from(k.to_vec()), iwp)
            })
            .collect();
        self.flat = build_flat_bytes(kept, self.key_size);

        // Clean BTreeMap: Modified→Clean, drop Removed (no flat shadowing needed
        // because Removed entries in flat are already gone after the rebuild above).
        self.retain(|_, v| match v.kind {
            IndexEntryKind::Clean => true,
            IndexEntryKind::Modified => {
                *v = v.as_clean_modified();
                true
            }
            IndexEntryKind::Removed => false,
        });
        // The flat rebuild above removed flat dirty entries without updating dirty_count.
        // retain correctly handled BTreeMap changes, but dirty_count is still off by the
        // flat dirty entries that were dropped. Setting to 0 is correct since all remaining
        // entries are Clean after this call.
        self.dirty_count = 0;
    }

    /// Retain only unprocessed entries (those with WAL offset > last_processed).
    /// Flat is cleared entirely: promote_to_flat always runs before last_processed is
    /// captured, so all flat entries are guaranteed to be processed.
    pub fn retain_unprocessed(&mut self, last_processed: LastProcessed) {
        self.flat = Bytes::default();
        self.retain(|_, v| !last_processed.is_processed(v));
        // flat was cleared unconditionally; recount since retain only tracked BTreeMap changes.
        self.dirty_count = self.count_dirty_slow();
    }

    /// Returns true if this index table has any unprocessed entries.
    /// Only the BTreeMap (write buffer) needs to be checked: flat entries are always
    /// at or below last_processed when this is called (promote runs before capture).
    pub fn has_unprocessed(&self, last_processed: LastProcessed) -> bool {
        self.data
            .iter()
            .any(|(_k, v)| !last_processed.is_processed(v))
    }

    /// Retain only entries with offset >= last_processed. Applied to both flat and BTreeMap.
    pub fn retain_above_position(&mut self, last_processed: u64) {
        // Rebuild flat keeping entries with offset >= cutoff.
        let old_flat = std::mem::take(&mut self.flat);
        let kept: Vec<(Bytes, IndexWalPosition)> = FlatIter::new(&old_flat, self.key_size)
            .filter(|(_, iwp)| iwp.offset >= last_processed)
            .map(|(k, iwp)| (Bytes::from(k.to_vec()), iwp))
            .collect();
        self.flat = build_flat_bytes(kept, self.key_size);
        self.retain(|_, v| v.offset >= last_processed);
        // flat was rebuilt; recount since retain only tracked BTreeMap changes.
        self.dirty_count = self.count_dirty_slow();
    }

    /// Compact index by retaining keys identified by the compactor.
    /// The compactor receives a double-ended iterator over all keys and returns keys to retain.
    pub fn compact_with<F>(&mut self, compactor: F)
    where
        F: FnOnce(&mut dyn DoubleEndedIterator<Item = &Bytes>) -> HashSet<Bytes>,
    {
        // Collect all valid unique keys from both flat and BTreeMap in sorted order.
        // Both sources are sorted and disjoint — merge in O(N+M).
        let all_keys: Vec<Bytes> = {
            let mut flat_it = FlatIter::new(&self.flat, self.key_size)
                .filter(|(k, _)| !self.data.contains_key(*k))
                .filter(|(_, iwp)| !iwp.is_removed())
                .map(|(k, _)| Bytes::from(k.to_vec()))
                .peekable();
            let mut btree_it = self
                .data
                .iter()
                .filter(|(_, v)| !v.is_removed())
                .map(|(k, _)| k.clone())
                .peekable();
            let mut v: Vec<Bytes> = Vec::with_capacity(self.flat_len() + self.data.len());
            loop {
                let cmp = match (flat_it.peek(), btree_it.peek()) {
                    (None, None) => break,
                    (Some(_), None) => Ordering::Less,
                    (None, Some(_)) => Ordering::Greater,
                    (Some(fk), Some(bk)) => fk.cmp(bk),
                };
                if cmp != Ordering::Greater {
                    v.push(flat_it.next().unwrap());
                } else {
                    v.push(btree_it.next().unwrap());
                }
            }
            v.extend(flat_it);
            v.extend(btree_it);
            v
        };

        let mut iter = all_keys.iter();
        let to_retain = compactor(&mut iter);
        drop(iter);
        drop(all_keys);

        self.retain(|k, _| to_retain.contains(k));

        if !self.flat.is_empty() {
            let retained: Vec<(Bytes, IndexWalPosition)> = FlatIter::new(&self.flat, self.key_size)
                .filter(|(_, iwp)| !iwp.is_removed())
                .filter(|(k, _)| to_retain.contains(&Bytes::from(k.to_vec())))
                .map(|(k, iwp)| (Bytes::from(k.to_vec()), iwp))
                .collect();
            self.flat = build_flat_bytes(retained, self.key_size);
        }
        // flat was rebuilt; recount since retain only tracked BTreeMap changes.
        self.dirty_count = self.count_dirty_slow();
    }

    /// Apply given update function to a value with a given key.
    /// If the key does not exist, this function completes without calling the update function
    pub fn apply_update(&mut self, key: &[u8], update: impl FnOnce(&mut IndexWalPosition)) {
        if let Some(value) = self.data.get_mut(key) {
            let was_dirty = !value.is_clean();
            update(value);
            adjust_dirty_count(&mut self.dirty_count, was_dirty, !value.is_clean());
            return;
        }
        // Also check the flat buffer (present when promote_to_flat has been called).
        // Insert the updated entry into the BTreeMap so it shadows the stale flat entry.
        if let Some(mut iwp) = self.flat_binary_search(key) {
            let was_dirty = !iwp.is_clean();
            update(&mut iwp);
            adjust_dirty_count(&mut self.dirty_count, was_dirty, !iwp.is_clean());
            let key_bytes = Bytes::from(key.to_vec());
            self.key_bytes += key_bytes.len();
            self.data.insert(key_bytes, iwp);
        }
    }

    /// Total bytes occupied by keys in this table (BTreeMap keys + full flat buffer). Always O(1).
    pub fn total_key_bytes(&self) -> usize {
        self.key_bytes + self.flat.len()
    }

    /// Bytes occupied by the flat buffer. Always O(1).
    pub fn flat_key_bytes(&self) -> usize {
        self.flat.len()
    }

    /// Move ALL entries from the BTreeMap (write buffer) into the flat array, merging
    /// with any existing flat content. BTreeMap is empty after this call.
    /// BTreeMap entries override flat entries for the same key.
    #[cfg(not(test))]
    const PROMOTE_THRESHOLD: usize = 128;
    #[cfg(test)]
    const PROMOTE_THRESHOLD: usize = 0;

    /// Returns `true` if the flat buffer was updated, `false` if nothing changed.
    pub fn promote_to_flat(&mut self) -> bool {
        if self.data.len() <= Self::PROMOTE_THRESHOLD {
            return false;
        }

        // Collect old flat entries.
        let old_flat = std::mem::take(&mut self.flat);
        let flat_entries: Vec<(Bytes, IndexWalPosition)> = FlatIter::new(&old_flat, self.key_size)
            .map(|(k, iwp)| (Bytes::from(k.to_vec()), iwp))
            .collect();

        // Collect ALL BTreeMap entries sorted by key (BTreeMap is already sorted).
        let btree_entries: Vec<(&Bytes, IndexWalPosition)> =
            self.data.iter().map(|(k, v)| (k, *v)).collect();

        // Merge-sort: BTreeMap entries override flat entries for the same key.
        let mut result: Vec<(Bytes, IndexWalPosition)> = Vec::new();
        let mut fi = 0usize;
        let mut bi = 0usize;
        let flat_count = flat_entries.len();

        while fi < flat_count || bi < btree_entries.len() {
            match (flat_entries.get(fi), btree_entries.get(bi)) {
                (None, None) => break,
                (Some((fk, fiwp)), None) => {
                    result.push((fk.clone(), *fiwp));
                    fi += 1;
                }
                (None, Some((bk, biwp))) => {
                    result.push(((*bk).clone(), *biwp));
                    bi += 1;
                }
                (Some((fk, fiwp)), Some((bk, biwp))) => match fk.as_ref().cmp(bk.as_ref()) {
                    Ordering::Less => {
                        result.push((fk.clone(), *fiwp));
                        fi += 1;
                    }
                    Ordering::Equal => {
                        // BTreeMap entry overrides flat entry for the same key.
                        result.push(((*bk).clone(), *biwp));
                        fi += 1;
                        bi += 1;
                    }
                    Ordering::Greater => {
                        result.push(((*bk).clone(), *biwp));
                        bi += 1;
                    }
                },
            }
        }

        // All BTreeMap key bytes are now in flat; reset the BTreeMap.
        self.key_bytes = 0;
        // Recount dirty from the merged result (cheaper than an extra pass over flat after build).
        self.dirty_count = result.iter().filter(|(_, iwp)| !iwp.is_clean()).count();
        self.flat = build_flat_bytes(result, self.key_size);
        self.data.clear();
        true
    }

    // ---------------------------------------------------------------------------
    // Private helpers
    // ---------------------------------------------------------------------------

    // ---------------------------------------------------------------------------
    // Private flat-array helpers
    // ---------------------------------------------------------------------------

    fn flat_len(&self) -> usize {
        flat_count(&self.flat, self.key_size)
    }

    /// Binary search the flat array for an exact key match.
    fn flat_binary_search(&self, key: &[u8]) -> Option<IndexWalPosition> {
        if self.flat.is_empty() {
            return None;
        }
        let flat = &self.flat[..];
        let idx = flat_lower_bound(flat, self.key_size, key);
        if idx >= flat_count(flat, self.key_size) {
            return None;
        }
        let (found_key, iwp) = flat_entry_at(flat, self.key_size, idx);
        if found_key == key { Some(iwp) } else { None }
    }

    /// Return the first flat entry with key strictly greater than `prev`
    /// (or the first entry at all when `prev` is `None`).
    /// Returns `WalPosition` (INVALID for Removed entries).
    fn flat_next_forward(&self, prev: Option<&[u8]>) -> Option<(Bytes, WalPosition)> {
        if self.flat.is_empty() {
            return None;
        }
        let flat = &self.flat[..];
        let idx = match prev {
            Some(p) => flat_upper_bound(flat, self.key_size, p),
            None => 0,
        };
        if idx >= flat_count(flat, self.key_size) {
            return None;
        }
        let (key_slice, iwp) = flat_entry_at(flat, self.key_size, idx);
        let key_start = key_slice.as_ptr() as usize - flat.as_ptr() as usize;
        let key_bytes = self.flat.slice(key_start..key_start + key_slice.len());
        Some((key_bytes, iwp.into_wal_position()))
    }

    /// Return the last flat entry with key strictly less than `prev`
    /// (or the last entry at all when `prev` is `None`).
    /// Returns `WalPosition` (INVALID for Removed entries).
    fn flat_next_reverse(&self, prev: Option<&[u8]>) -> Option<(Bytes, WalPosition)> {
        if self.flat.is_empty() {
            return None;
        }
        let flat = &self.flat[..];
        let count = flat_count(flat, self.key_size);
        let idx = match prev {
            Some(p) => {
                let lb = flat_lower_bound(flat, self.key_size, p);
                if lb == 0 {
                    return None;
                }
                lb - 1
            }
            None => {
                if count == 0 {
                    return None;
                }
                count - 1
            }
        };
        let (key_slice, iwp) = flat_entry_at(flat, self.key_size, idx);
        let key_start = key_slice.as_ptr() as usize - flat.as_ptr() as usize;
        let key_bytes = self.flat.slice(key_start..key_start + key_slice.len());
        Some((key_bytes, iwp.into_wal_position()))
    }

    #[cfg(test)]
    pub fn into_data(self) -> BTreeMap<Bytes, WalPosition> {
        // Flat entries have lower priority; BTreeMap entries override them.
        let mut result: BTreeMap<Bytes, WalPosition> = FlatIter::new(&self.flat, self.key_size)
            .map(|(k, iwp)| (Bytes::from(k.to_vec()), iwp.into_wal_position()))
            .collect();
        for (k, v) in self.data {
            result.insert(k, v.into_wal_position());
        }
        result
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
    use super::flat::{decode_kind_from_len, encode_kind_in_len};
    use super::*;
    use crate::key_shape::{KeyIndexing, KeyShape, KeyType};

    #[test]
    fn test_encode_decode_kind_in_len() {
        for (kind, expected_bits) in [
            (IndexEntryKind::Clean, 0u32),
            (IndexEntryKind::Modified, 1u32),
            (IndexEntryKind::Removed, 2u32),
        ] {
            let len = 0x1234_5678u32; // arbitrary value that fits in 30 bits
            let encoded = encode_kind_in_len(len, kind);
            assert_eq!(encoded >> 30, expected_bits);
            let (decoded_len, decoded_kind) = decode_kind_from_len(encoded);
            assert_eq!(decoded_len, len);
            assert_eq!(decoded_kind, kind);
        }
    }

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
            key_size: None,
            dirty_count: 0,
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

        // Second promotion: flat has [1]=Removed, [2]=Clean, [3]=Clean — all 3 entries.
        table.promote_to_flat();
        assert_eq!(table.flat_len(), 3);
        assert_eq!(table.get(&[1]), Some(WalPosition::INVALID)); // Removed → INVALID
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
        table
            .data
            .values_mut()
            .for_each(|v| *v = v.as_clean_modified());

        table.key_size = Some(4);
        table.promote_to_flat();

        assert!(table.data.is_empty());
        assert_eq!(table.flat_len(), 3);
        // Fixed-length format: 3*(4+12) = 48 bytes — key + WalPosition (kind packed in len)
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
        table
            .data
            .values_mut()
            .for_each(|v| *v = v.as_clean_modified());
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
        table
            .data
            .values_mut()
            .for_each(|v| *v = v.as_clean_modified());
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

        // flat: [10]=Removed, [20]=Clean, [30]=Clean — Removed entry stays in flat.
        assert_eq!(table.flat_len(), 3);
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

    #[test]
    fn test_promote_to_flat_modified_entries() {
        // promote_to_flat should promote Modified (dirty) entries too, not just Clean.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(1));
        table.insert(vec![2].into(), WalPosition::test_value(2));
        table.insert(vec![3].into(), WalPosition::test_value(3));
        // All entries are Modified (not yet flushed).
        assert_eq!(table.dirty_count(), 3);

        table.promote_to_flat();

        // BTreeMap should be completely empty: all entries moved to flat.
        assert!(table.data.is_empty());
        assert_eq!(table.flat_len(), 3);
        // Dirty count should include the flat dirty entries.
        assert_eq!(table.dirty_count(), 3);

        // Lookups still work.
        assert_eq!(table.get(&[1]), Some(WalPosition::test_value(1)));
        assert_eq!(table.get(&[2]), Some(WalPosition::test_value(2)));
        assert_eq!(table.get(&[3]), Some(WalPosition::test_value(3)));
        assert_eq!(table.get(&[4]), None);

        // After clean_self, all dirty entries are cleaned.
        table.clean_self();
        assert_eq!(table.dirty_count(), 0);
        // Entries remain accessible.
        assert_eq!(table.get(&[1]), Some(WalPosition::test_value(1)));
    }

    #[test]
    fn test_promote_to_flat_mixed_clean_and_modified() {
        // promote_to_flat should handle a mix of Clean and Modified entries.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(1));
        table.insert(vec![2].into(), WalPosition::test_value(2));
        table.insert(vec![3].into(), WalPosition::test_value(3));
        // Make [1] and [3] clean; leave [2] modified.
        table
            .data
            .entry(vec![1].into())
            .and_modify(|v| *v = v.as_clean_modified());
        table
            .data
            .entry(vec![3].into())
            .and_modify(|v| *v = v.as_clean_modified());

        table.promote_to_flat();

        assert!(table.data.is_empty());
        assert_eq!(table.flat_len(), 3);
        // Only [2] is dirty in flat.
        assert_eq!(table.dirty_count(), 1);

        assert_eq!(table.get(&[1]), Some(WalPosition::test_value(1)));
        assert_eq!(table.get(&[2]), Some(WalPosition::test_value(2)));
        assert_eq!(table.get(&[3]), Some(WalPosition::test_value(3)));
    }

    #[test]
    fn test_promote_to_flat_modified_with_remove() {
        // Removed entries stay in BTreeMap to shadow flat; Modified entries go to flat.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(1));
        table.insert(vec![2].into(), WalPosition::test_value(2));
        // First promotion with Modified entries.
        table.promote_to_flat();
        assert_eq!(table.flat_len(), 2);
        assert_eq!(table.dirty_count(), 2);

        // Delete [1] (Removed goes to BTreeMap) and add [3].
        table.remove(vec![1].into(), WalPosition::test_value(10));
        table.insert(vec![3].into(), WalPosition::test_value(5));
        // dirty_count: 2 (flat Modified) + 1 (Removed) + 1 (new Modified) = 4
        assert_eq!(table.dirty_count(), 4);

        table.promote_to_flat();
        // flat: [1]=Removed, [2]=Modified, [3]=Modified — Removed entry stays in flat.
        assert_eq!(table.flat_len(), 3);
        assert_eq!(table.dirty_count(), 3);
        assert_eq!(table.get(&[1]), Some(WalPosition::INVALID)); // Removed → INVALID
        assert_eq!(table.get(&[2]), Some(WalPosition::test_value(2)));
        assert_eq!(table.get(&[3]), Some(WalPosition::test_value(5)));
    }

    // -----------------------------------------------------------------------
    // Tests for flat interaction with other methods
    // -----------------------------------------------------------------------

    #[test]
    fn test_into_data_after_promote() {
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(1));
        table.insert(vec![2].into(), WalPosition::test_value(2));
        table.promote_to_flat();
        // New entry added after promote stays in BTreeMap.
        table.insert(vec![3].into(), WalPosition::test_value(3));

        let data = table.into_data();
        assert_eq!(data.len(), 3);
        assert_eq!(data[&Bytes::from(vec![1])], WalPosition::test_value(1));
        assert_eq!(data[&Bytes::from(vec![2])], WalPosition::test_value(2));
        assert_eq!(data[&Bytes::from(vec![3])], WalPosition::test_value(3));
    }

    #[test]
    fn test_iter_after_promote() {
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(10));
        table.insert(vec![3].into(), WalPosition::test_value(30));
        table.promote_to_flat();
        // New entry between existing flat entries.
        table.insert(vec![2].into(), WalPosition::test_value(20));
        // Delete a flat entry via BTreeMap tombstone.
        table.remove(vec![3].into(), WalPosition::test_value(35));

        let items: Vec<_> = table.iter().collect();
        // [3] is Removed so excluded; [1] and [2] survive in order.
        assert_eq!(items.len(), 2);
        assert_eq!(
            items[0],
            (Bytes::from(vec![1]), WalPosition::test_value(10))
        );
        assert_eq!(
            items[1],
            (Bytes::from(vec![2]), WalPosition::test_value(20))
        );
    }

    #[test]
    fn test_keys_after_promote() {
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(10));
        table.insert(vec![3].into(), WalPosition::test_value(30));
        table.promote_to_flat();
        table.insert(vec![2].into(), WalPosition::test_value(20));
        table.remove(vec![3].into(), WalPosition::test_value(35));

        let keys: Vec<_> = table.keys().collect();
        assert_eq!(keys, vec![Bytes::from(vec![1]), Bytes::from(vec![2])]);
    }

    #[test]
    fn test_serialize_after_promote() {
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(10));
        table.insert(vec![3].into(), WalPosition::test_value(30));
        table
            .data
            .values_mut()
            .for_each(|v| *v = v.as_clean_modified());
        table.promote_to_flat();
        // New entry after promote (in BTreeMap).
        table.insert(vec![2].into(), WalPosition::test_value(20));

        let (shape, ks) = KeyShape::new_single_config_indexing(
            KeyIndexing::variable_length(),
            1,
            KeyType::uniform(1),
            Default::default(),
        );
        let ks = shape.ks(ks);
        let mut buf = BytesMut::new();
        table.serialize_index_entries(ks, &mut buf);
        let deserialized = IndexTable::deserialize_index_entries(ks, Bytes::from(buf.freeze()));
        assert_eq!(deserialized.get(&[1]), Some(WalPosition::test_value(10)));
        assert_eq!(deserialized.get(&[2]), Some(WalPosition::test_value(20)));
        assert_eq!(deserialized.get(&[3]), Some(WalPosition::test_value(30)));
        assert_eq!(deserialized.len(), 3);
    }

    #[test]
    fn test_next_entry_mixed_flat_and_btree() {
        let mut table = IndexTable::default();
        // [1], [3], [5] go to flat.
        table.insert(vec![1].into(), WalPosition::test_value(10));
        table.insert(vec![3].into(), WalPosition::test_value(30));
        table.insert(vec![5].into(), WalPosition::test_value(50));
        table
            .data
            .values_mut()
            .for_each(|v| *v = v.as_clean_modified());
        table.promote_to_flat();
        // [2] and [4] stay in BTreeMap.
        table.insert(vec![2].into(), WalPosition::test_value(20));
        table.insert(vec![4].into(), WalPosition::test_value(40));

        // Forward: all 5 keys in sorted order.
        let mut prev = None;
        let expected_fwd = vec![
            (Bytes::from(vec![1]), WalPosition::test_value(10)),
            (Bytes::from(vec![2]), WalPosition::test_value(20)),
            (Bytes::from(vec![3]), WalPosition::test_value(30)),
            (Bytes::from(vec![4]), WalPosition::test_value(40)),
            (Bytes::from(vec![5]), WalPosition::test_value(50)),
        ];
        for expected in &expected_fwd {
            let entry = table.next_entry(prev, false).unwrap();
            assert_eq!(entry.0, expected.0);
            assert_eq!(entry.1, expected.1);
            prev = Some(entry.0);
        }
        assert!(table.next_entry(prev, false).is_none());

        // Reverse: all 5 keys in reverse sorted order.
        let mut prev = None;
        for expected in expected_fwd.iter().rev() {
            let entry = table.next_entry(prev, true).unwrap();
            assert_eq!(entry.0, expected.0);
            assert_eq!(entry.1, expected.1);
            prev = Some(entry.0);
        }
        assert!(table.next_entry(prev, true).is_none());
    }

    #[test]
    fn test_retain_dirty_with_flat() {
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(10)); // Modified
        table.insert(vec![2].into(), WalPosition::test_value(20)); // will become Clean
        table.insert(vec![3].into(), WalPosition::test_value(30)); // Modified
        table
            .data
            .entry(vec![2].into())
            .and_modify(|v| *v = v.as_clean_modified());
        table.promote_to_flat();
        // flat: [1]=Modified, [2]=Clean, [3]=Modified

        table.retain_dirty();

        assert_eq!(table.flat_len(), 2);
        assert_eq!(table.get(&[1]), Some(WalPosition::test_value(10)));
        assert!(table.get(&[2]).is_none()); // Clean removed
        assert_eq!(table.get(&[3]), Some(WalPosition::test_value(30)));
        assert_eq!(table.dirty_count(), 2);
    }

    #[test]
    fn test_merge_dirty_and_clean_with_flat() {
        // self: loaded index with [1, 2].
        let mut self_table = IndexTable::default();
        self_table.insert(vec![1].into(), WalPosition::test_value(10));
        self_table.insert(vec![2].into(), WalPosition::test_value(20));
        self_table
            .data
            .values_mut()
            .for_each(|v| *v = v.as_clean_modified());

        // dirty: [2] updated, [3] new — promoted to flat.
        let mut dirty = IndexTable::default();
        dirty.insert(vec![2].into(), WalPosition::test_value(25));
        dirty.insert(vec![3].into(), WalPosition::test_value(35));
        dirty.promote_to_flat();

        self_table.merge_dirty_and_clean(&dirty);

        assert_eq!(self_table.get(&[1]), Some(WalPosition::test_value(10)));
        assert_eq!(self_table.get(&[2]), Some(WalPosition::test_value(25)));
        assert_eq!(self_table.get(&[3]), Some(WalPosition::test_value(35)));
    }

    #[test]
    fn test_merge_dirty_no_clean_with_flat() {
        let mut self_table = IndexTable::default();
        self_table.insert(vec![1].into(), WalPosition::test_value(10));
        self_table
            .data
            .values_mut()
            .for_each(|v| *v = v.as_clean_modified());

        let mut dirty = IndexTable::default();
        dirty.insert(vec![2].into(), WalPosition::test_value(20));
        dirty.insert(vec![3].into(), WalPosition::test_value(30));
        dirty.promote_to_flat();

        self_table.merge_dirty_no_clean(&dirty);

        assert_eq!(self_table.get(&[1]), Some(WalPosition::test_value(10)));
        assert_eq!(self_table.get(&[2]), Some(WalPosition::test_value(20)));
        assert_eq!(self_table.get(&[3]), Some(WalPosition::test_value(30)));
    }

    #[test]
    fn test_unmerge_flushed_with_flat() {
        // original: [1, 2, 3] promoted to flat.
        let mut original = IndexTable::default();
        original.insert(vec![1].into(), WalPosition::test_value(2));
        original.insert(vec![2].into(), WalPosition::test_value(3));
        original.insert(vec![3].into(), WalPosition::test_value(4));
        original.promote_to_flat();

        // current: clone of original, plus new entries [4, 5] in BTreeMap.
        let mut current = original.clone();
        current.insert(vec![4].into(), WalPosition::test_value(5));
        current.insert(vec![5].into(), WalPosition::test_value(6));

        let len_before = current.len();
        current.unmerge_flushed(&original, LastProcessed::new_test(u64::MAX));
        let len_after = current.len();

        // [1, 2, 3] removed; [4, 5] remain.
        assert_eq!(len_before as i64 - len_after as i64, 3);
        assert!(current.get(&[1]).is_none());
        assert!(current.get(&[2]).is_none());
        assert!(current.get(&[3]).is_none());
        assert_eq!(current.get(&[4]), Some(WalPosition::test_value(5)));
        assert_eq!(current.get(&[5]), Some(WalPosition::test_value(6)));
    }
}

// Helper for tests only.
#[cfg(test)]
impl IndexWalPosition {
    fn is_modified(&self) -> bool {
        self.kind == IndexEntryKind::Modified
    }
}
