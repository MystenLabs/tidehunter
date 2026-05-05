mod flat;
use flat::{
    FlatIter, append_flat_varlen, build_flat_bytes, flat_count, flat_entry_at, flat_lower_bound,
    flat_upper_bound, merge_into_flat,
};

use crate::index::index_format::Direction;
use crate::key_shape::KeySpaceDesc;
#[cfg(test)]
use crate::primitives::cursor::SliceCursor;
use crate::primitives::slice_buf::SliceBuf;
use crate::primitives::var_int::{MAX_U16_VARINT, deserialize_u16_varint, serialize_u16_varint};
use crate::wal::position::{HasOffset, LastProcessed, WalPosition};
use bytes::{Buf, BufMut, BytesMut};
use minibytes::Bytes;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
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
    ///
    /// Tombstones from `dirty` are preserved in the output: they replace any
    /// matching entry in `self` and are inserted standalone when no matching
    /// entry exists. This is required by the two-level LSM — a tombstone in
    /// the new L0 must survive to shadow a live entry for the same key in L1
    /// below. Callers producing the deepest level (no further level below)
    /// should call `clean_self()` after this to strip tombstones.
    pub fn merge_dirty_and_clean(&mut self, dirty: &Self) {
        self.merge_dirty(dirty, /* promote_to_clean */ true);
    }

    /// Merges dirty IndexTable into a loaded IndexTable, preserving dirty states
    pub fn merge_dirty_no_clean(&mut self, dirty: &Self) {
        self.merge_dirty(dirty, /* promote_to_clean */ false);
    }

    /// Merges `dirty` into `self`. When `promote_to_clean` is true, non-removed
    /// entries are demoted Modified→Clean via `as_clean_modified` (the flush path:
    /// these entries are about to become clean on disk). Tombstones are preserved
    /// as-is in both modes — callers producing the deepest level still need to
    /// call `clean_self()` afterwards to strip them.
    fn merge_dirty(&mut self, dirty: &Self, promote_to_clean: bool) {
        // todo implement this efficiently taking into account both self and dirty are sorted
        // self.flat is untouched here, so its dirty contribution to self.dirty_count is
        // unchanged — we only need to track the delta on self.data inserts.
        let mut dirty_delta: i64 = 0;
        for (k, v) in dirty.data.iter() {
            // Strict check for concurrent_test and unit tests
            #[cfg(any(debug_assertions, feature = "test_methods"))]
            if promote_to_clean
                && let Some(found) = self.data.get(k)
                && found.offset > v.offset
            {
                // This can't happen - this would mean we somehow have written an
                // older entry in a dirty set.
                panic!("found.offset {} > v.offset {}", found.offset, v.offset);
            }
            let incoming = if promote_to_clean && !v.is_removed() {
                v.as_clean_modified()
            } else {
                *v
            };
            let incoming_dirty = !incoming.is_clean();
            match self.data.insert(k.clone(), incoming) {
                Some(prev) if !prev.is_clean() => dirty_delta -= 1,
                Some(_) => {}
                None => self.key_bytes += k.len(),
            }
            if incoming_dirty {
                dirty_delta += 1;
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
            let incoming = if promote_to_clean && !v.is_removed() {
                v.as_clean_modified()
            } else {
                v
            };
            let len = key.len();
            let incoming_dirty = !incoming.is_clean();
            match self.data.insert(key, incoming) {
                Some(prev) if !prev.is_clean() => dirty_delta -= 1,
                Some(_) => {}
                None => self.key_bytes += len,
            }
            if incoming_dirty {
                dirty_delta += 1;
            }
        }
        if dirty_delta >= 0 {
            self.dirty_count += dirty_delta as usize;
        } else {
            self.dirty_count = self.dirty_count.saturating_sub((-dirty_delta) as usize);
        }
    }

    /// Remove flushed index entries that have offset <= last_processed
    pub fn unmerge_flushed(&mut self, original: &Self, last_processed: LastProcessed) {
        // Walk original.data; remove matching entries from self.data. If promote_flat_job
        // ran between snapshot capture and now, an entry present in original.data may now
        // live in self.flat — record those for the flat-rebuild pass below.
        let mut pending_flat: HashMap<Bytes, IndexWalPosition> = HashMap::new();
        for (k, v) in original.data.iter() {
            if !last_processed.is_processed(v) {
                // Do not unmerge entries that might have in-flight operations
                continue;
            }
            let removed_from_data = match self.data.entry(k.clone()) {
                Entry::Vacant(_) => false,
                Entry::Occupied(oc) => {
                    // Use into_update_position: into_wal_position collapses all Removed
                    // entries to INVALID, which would incorrectly match two different
                    // Removed tombstones at distinct WAL offsets.
                    if oc.get().into_update_position() == v.into_update_position() {
                        self.key_bytes -= k.len();
                        oc.remove();
                        true
                    } else {
                        false
                    }
                }
            };
            if !removed_from_data {
                // Either Vacant (likely promoted into self.flat) or Occupied with a newer
                // position (a stale flat entry at the snapshot's position may still exist).
                // Let the flat rebuild below drop it by key+position match.
                pending_flat.insert(k.clone(), *v);
            }
        }
        // Rebuild self.flat, dropping entries matched by either:
        //   - original.flat (flat→flat: original was promoted before snapshot)
        //   - original.data entries in `pending_flat` (data→flat: promote_flat_job raced us)
        if !original.flat.is_empty() || !pending_flat.is_empty() {
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
                // Check for an exact key match in original.flat and consume if found.
                // Use into_update_position so two distinct Removed entries (both INVALID
                // via into_wal_position) don't compare equal.
                let remove_by_flat = if orig_it.peek().map(|(ok, _)| *ok == sk).unwrap_or(false) {
                    let (_, o_iwp) = orig_it.next().unwrap();
                    last_processed.is_processed(&o_iwp)
                        && s_iwp.into_update_position() == o_iwp.into_update_position()
                } else {
                    false
                };
                let remove_by_data = !remove_by_flat
                    && pending_flat
                        .get(sk)
                        .map(|o_iwp| s_iwp.into_update_position() == o_iwp.into_update_position())
                        .unwrap_or(false);
                if !remove_by_flat && !remove_by_data {
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

    /// Rebuild the flat buffer by walking existing entries through `keep`. The
    /// closure returns `None` to drop the entry or `Some(new_iwp)` to keep it
    /// (optionally with a rewritten position — e.g. Modified→Clean).
    ///
    /// `dirty_count` is **not** adjusted here — callers are expected to either
    /// set it (`clean_self`: goes to 0) or recompute via `count_dirty_slow`
    /// (`retain_above_position`, `compact_with`) since the accurate delta
    /// depends on caller context.
    fn retain_flat<F>(&mut self, mut keep: F)
    where
        F: FnMut(&[u8], IndexWalPosition) -> Option<IndexWalPosition>,
    {
        if self.flat.is_empty() {
            return;
        }
        let old_flat = std::mem::take(&mut self.flat);
        let kept: Vec<(Bytes, IndexWalPosition)> = FlatIter::new(&old_flat, self.key_size)
            .filter_map(|(k, iwp)| {
                keep(k, iwp).map(|new_iwp| (old_flat.slice_to_bytes(k), new_iwp))
            })
            .collect();
        self.flat = build_flat_bytes(kept, self.key_size);
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
        self.next_entry_directional(prev.as_deref(), Direction::from_bool(reverse))
    }

    /// Walk to the next entry in the given direction. BTreeMap and flat are
    /// both sorted ascending, so Forward picks the smaller key and Backward
    /// the larger; BTreeMap wins on ties in either direction (freshest data).
    ///
    /// Removed tombstones in BTreeMap are surfaced (as INVALID positions) so
    /// callers can shadow a matching on-disk entry — but a tombstone that
    /// shadows a *flat* entry at the same key suppresses that flat candidate
    /// from this iterator (the tombstone in BTreeMap is the winning view).
    fn next_entry_directional(
        &self,
        prev: Option<&[u8]>,
        direction: Direction,
    ) -> Option<(Bytes, WalPosition)> {
        let btree_cand: Option<(&Bytes, &IndexWalPosition)> = match (direction, prev) {
            (Direction::Forward, Some(p)) => self
                .data
                .range::<[u8], _>((Bound::Excluded(p), Bound::<&[u8]>::Unbounded))
                .next(),
            (Direction::Forward, None) => self.data.iter().next(),
            (Direction::Backward, Some(p)) => self
                .data
                .range::<[u8], _>((Bound::Unbounded, Bound::Excluded(p)))
                .next_back(),
            (Direction::Backward, None) => self.data.iter().next_back(),
        };

        let flat_step = |from: Option<&[u8]>| match direction {
            Direction::Forward => self.flat_next_forward(from),
            Direction::Backward => self.flat_next_reverse(from),
        };

        let mut flat_cand = flat_step(prev);
        while let Some((ref fk, _)) = flat_cand {
            if self
                .data
                .get(fk.as_ref())
                .map(|v| v.is_removed())
                .unwrap_or(false)
            {
                let fk_clone = fk.clone();
                flat_cand = flat_step(Some(fk_clone.as_ref()));
            } else {
                break;
            }
        }

        match (btree_cand, flat_cand) {
            (None, None) => None,
            (Some((bk, bv)), None) => Some((bk.clone(), bv.into_wal_position())),
            (None, Some((fk, fv))) => Some((fk, fv)),
            (Some((bk, bv)), Some((fk, fv))) => {
                let cmp = bk.as_ref().cmp(fk.as_ref());
                // Forward: smaller wins; Backward: larger wins. BTreeMap wins ties.
                let btree_wins = match direction {
                    Direction::Forward => cmp != Ordering::Greater,
                    Direction::Backward => cmp != Ordering::Less,
                };
                if btree_wins {
                    Some((bk.clone(), bv.into_wal_position()))
                } else {
                    Some((fk, fv))
                }
            }
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

    #[cfg(any(test, feature = "test-utils"))]
    pub fn data_btree_is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (Bytes, WalPosition)> + '_ {
        self.iter_inner(false)
    }

    /// Same as `iter()` but yields tombstones as `(key, WalPosition::INVALID)`
    /// instead of skipping them. Used by serializers that want to persist
    /// tombstones on disk (two-level LSM L0 over L1).
    pub fn iter_with_tombstones(&self) -> impl Iterator<Item = (Bytes, WalPosition)> + '_ {
        self.iter_inner(true)
    }

    fn iter_inner(
        &self,
        include_tombstones: bool,
    ) -> impl Iterator<Item = (Bytes, WalPosition)> + '_ {
        // Both sources are sorted and disjoint (flat excludes keys present in data).
        // Merge in O(N+M) rather than collecting and sorting.
        let mut flat_it = FlatIter::new(&self.flat, self.key_size)
            .filter(|(k, _)| !self.data.contains_key(*k))
            .filter_map(move |(k, iwp)| {
                let pos = iwp.into_wal_position();
                if include_tombstones || pos.is_valid() {
                    Some((Bytes::from(k.to_vec()), pos))
                } else {
                    None
                }
            })
            .peekable();
        let mut btree_it = self
            .data
            .iter()
            .filter_map(move |(k, v)| {
                let pos = v.into_wal_position();
                if include_tombstones || pos.is_valid() {
                    Some((k.clone(), pos))
                } else {
                    None
                }
            })
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
    ///
    /// Removed entries are persisted with `WalPosition::INVALID` as the
    /// on-disk payload; `deserialize_index_entries` decodes them back into
    /// `Removed`. Callers that want to strip tombstones (e.g. writing the
    /// deepest LSM level) should call `clean_self()` on the table first.
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
                let pos = if value.is_removed() {
                    WalPosition::INVALID
                } else {
                    value.into_update_position()
                };
                self.write_key_val(ks, out, visitor, key, pos);
            }
            return;
        }

        // Merge-sort flat and BTreeMap entries; BTreeMap overrides flat for the same key.
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

            let (key, iwp, advance_flat, advance_btree) = match cmp {
                Ordering::Less => {
                    let (fk, fiwp) = cur_flat.take().unwrap();
                    (fk, fiwp, true, false)
                }
                Ordering::Equal => {
                    // Same key: BTreeMap overrides flat; advance both.
                    cur_flat = flat_iter.next().map(|(k, iwp)| (k.to_vec(), iwp));
                    let (bk, bv) = cur_btree.take().unwrap();
                    (bk.to_vec(), *bv, false, true)
                }
                Ordering::Greater => {
                    let (bk, bv) = cur_btree.take().unwrap();
                    (bk.to_vec(), *bv, false, true)
                }
            };

            let pos = if iwp.is_removed() {
                WalPosition::INVALID
            } else {
                iwp.into_update_position()
            };
            self.write_key_val(ks, out, visitor, &key, pos);

            if advance_flat {
                cur_flat = flat_iter.next().map(|(k, iwp)| (k.to_vec(), iwp));
            }
            if advance_btree {
                cur_btree = btree_it.next();
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

    // -------------------------------------------------------------------------
    // pub(super) helpers shared with lookup_header.rs
    //
    // The on-disk variable-length key section format written by
    // LookupHeaderIndex is identical to the in-memory flat buffer format used
    // by IndexTable.  These thin wrappers expose the flat-buffer primitives
    // (which return/accept IndexWalPosition) as a WalPosition-based API that
    // lookup_header.rs can use without touching IndexWalPosition internals.
    // -------------------------------------------------------------------------

    /// Returns the number of entries in a variable-length flat section.
    pub(super) fn flat_varlen_count(buffer: &[u8]) -> usize {
        flat_count(buffer, None)
    }

    /// Returns the key slice and WAL position for the entry at `idx`. O(1) via the offset table.
    pub(super) fn flat_varlen_entry_at(buffer: &[u8], idx: usize) -> (&[u8], WalPosition) {
        let (key, iwp) = flat_entry_at(buffer, None, idx);
        (key, iwp.into_update_position())
    }

    /// Lower bound: smallest index `i` where `buffer[i].key >= key`.
    pub(super) fn flat_varlen_lower_bound(buffer: &[u8], key: &[u8]) -> usize {
        flat_lower_bound(buffer, None, key)
    }

    /// Binary-search a variable-length flat section for `key`.
    /// Returns the WAL position if found, None otherwise.
    pub(super) fn flat_varlen_lookup(buffer: &[u8], key: &[u8]) -> Option<WalPosition> {
        let idx = flat_lower_bound(buffer, None, key);
        if idx >= flat_count(buffer, None) {
            return None;
        }
        let (found_key, iwp) = flat_entry_at(buffer, None, idx);
        (found_key == key).then(|| iwp.into_update_position())
    }

    /// Append `(key, WalPosition)` pairs as a variable-length flat section into `out`.
    /// `WalPosition::INVALID` entries are written as tombstones (when the caller
    /// wants to preserve them); otherwise all entries are stored as clean.
    /// Writes directly into the caller's buffer, avoiding a separate allocation.
    pub(super) fn append_flat_varlen_section(
        entries: Vec<(Bytes, WalPosition)>,
        out: &mut BytesMut,
    ) {
        let iwp_entries: Vec<(Bytes, IndexWalPosition)> = entries
            .into_iter()
            .map(|(k, pos)| (k, IndexWalPosition::from_disk(pos)))
            .collect();
        append_flat_varlen(&iwp_entries, out);
    }

    /// Constructs a flat-backed IndexTable by wrapping a pre-built varlen flat
    /// buffer. The caller is responsible for the bytes being well-formed
    /// (`[count: u32][offsets: u32 * count][entries]` — see
    /// `build_flat_bytes`/`append_flat_varlen`) and for the entries being
    /// sorted by key. `data` is empty, `dirty_count` is 0.
    #[doc(hidden)] // Used by lookup_header.rs for varlen index deserialization
    pub(crate) fn from_varlen_flat_bytes(flat: Bytes) -> Self {
        IndexTable {
            data: BTreeMap::new(),
            key_bytes: 0,
            flat,
            key_size: None,
            dirty_count: 0,
        }
    }

    /// Deserializes IndexTable from bytes.
    ///
    /// Loaded entries populate `flat` (not the BTreeMap `data`): on-disk blobs
    /// are already sorted and immutable, so the compact flat representation
    /// with its offset table fits the read-only nature of the deserialized
    /// table and avoids a per-entry BTreeMap allocation.
    ///
    /// Entries whose on-disk position is `WalPosition::INVALID` are decoded as
    /// Removed tombstones (see `serialize_index_entries`). The two-level LSM
    /// needs on-disk tombstones in L0 to shadow keys still present in L1.
    ///
    /// Fast path: for fixed-size keys with no tombstones, the on-disk format
    /// is byte-identical to the flat buffer format (clean entries have
    /// `kind=0`, so `encoded_len = len`). We validate sortedness and absence
    /// of tombstones in a single pass and, on success, hand the input buffer
    /// straight to `flat`.
    pub fn deserialize_index_entries(ks: &KeySpaceDesc, bytes: Bytes) -> Self {
        if let Some(key_size) = ks.index_key_size() {
            return Self::deserialize_fixed_size(key_size, bytes);
        }
        Self::deserialize_varlen(bytes)
    }

    fn deserialize_fixed_size(key_size: usize, bytes: Bytes) -> Self {
        let elem_size = key_size + WalPosition::LENGTH;
        assert_eq!(
            bytes.len() % elem_size,
            0,
            "fixed-size index blob length {} is not a multiple of element size {}",
            bytes.len(),
            elem_size,
        );

        // Single pass: validate sortedness + detect on-disk tombstones.
        // On-disk tombstones carry `offset = u64::MAX`; the flat encoding of a
        // tombstone uses `encoded_len = 0x8000_0000` (kind=Removed packed into
        // the top 2 bits over `len = 0`), so the two layouts differ and we
        // must fall back to the decode-and-rebuild path in that case.
        let mut has_tombstone = false;
        let mut prev_key: Option<&[u8]> = None;
        for chunk in bytes.chunks_exact(elem_size) {
            let key = &chunk[..key_size];
            if let Some(prev) = prev_key {
                match prev.cmp(key) {
                    Ordering::Less => {}
                    Ordering::Equal => panic!("Duplicate keys detected in index"),
                    Ordering::Greater => {
                        panic!("Out-of-order keys detected in index (flat requires sorted input)")
                    }
                }
            }
            prev_key = Some(key);
            let offset = u64::from_be_bytes(chunk[key_size..key_size + 8].try_into().unwrap());
            if offset == u64::MAX {
                has_tombstone = true;
            }
        }

        if !has_tombstone {
            return IndexTable {
                data: BTreeMap::new(),
                key_bytes: 0,
                flat: bytes,
                key_size: Some(key_size),
                dirty_count: 0,
            };
        }

        // Slow path: blob contains tombstones whose on-disk encoding
        // (`encoded_len = u32::MAX`) differs from the flat encoding
        // (`0x8000_0000`). Decode per-entry and rebuild via build_flat_bytes.
        let mut sb = SliceBuf::new(bytes);
        let mut entries: Vec<(Bytes, IndexWalPosition)> =
            Vec::with_capacity(sb.remaining() / elem_size);
        while sb.has_remaining() {
            let key = sb.slice_n(key_size);
            let value = WalPosition::read_from_buf(&mut sb);
            entries.push((key, IndexWalPosition::from_disk(value)));
        }
        let flat = build_flat_bytes(entries, Some(key_size));
        IndexTable {
            data: BTreeMap::new(),
            key_bytes: 0,
            flat,
            key_size: Some(key_size),
            dirty_count: 0,
        }
    }

    fn deserialize_varlen(bytes: Bytes) -> Self {
        let mut bytes = SliceBuf::new(bytes);
        let mut entries: Vec<(Bytes, IndexWalPosition)> = Vec::new();
        while bytes.has_remaining() {
            let key_len = deserialize_u16_varint(&mut bytes);
            let key = bytes.slice_n(key_len as usize);
            let value = WalPosition::read_from_buf(&mut bytes);
            let iwp = IndexWalPosition::from_disk(value);
            if let Some((prev_key, _)) = entries.last() {
                match prev_key.as_ref().cmp(key.as_ref()) {
                    Ordering::Less => {}
                    Ordering::Equal => panic!("Duplicate keys detected in index"),
                    Ordering::Greater => {
                        panic!("Out-of-order keys detected in index (flat requires sorted input)")
                    }
                }
            }
            entries.push((key, iwp));
        }
        let flat = build_flat_bytes(entries, None);
        IndexTable {
            data: BTreeMap::new(),
            key_bytes: 0,
            flat,
            key_size: None,
            dirty_count: 0,
        }
    }

    /// Marks all elements in this index as clean and removes tombstones.
    /// Rebuilds flat with Modified→Clean and Removed entries dropped.
    /// After this call both flat and BTreeMap contain only Clean entries.
    ///
    /// # LSM tombstone invariant — read before changing call sites
    ///
    /// On-disk index blobs may contain tombstones (`WalPosition::INVALID`
    /// entries). A tombstone exists solely to shadow a key in a **deeper**
    /// LSM level. Therefore:
    ///
    /// - The **deepest** level below a cell has nothing to shadow —
    ///   tombstones there are dead weight and MUST be stripped by calling
    ///   this function before the blob is written.
    /// - **Shallower** levels MUST preserve tombstones; stripping them would
    ///   resurrect the deeper key the tombstone was shadowing.
    ///
    /// Call sites that honor this rule (keep in sync if you add another):
    /// - `flusher.rs` promote branch: new L1 is deepest → `clean_self`.
    /// - `flusher.rs` non-promote branch: `clean_self` **only if**
    ///   `existing_l1.is_none()`.
    /// - `large_table.rs::clear_after_flush`: strips in-memory tombstones
    ///   only when no L1 sits below.
    ///
    /// The read path (`IndexTable::get` returning `Some(INVALID)` →
    /// `LargeTable::get` shadowing via `found.valid()`) relies on this
    /// invariant: a surviving tombstone in a shallower level is what makes
    /// a deletion observable across a flush boundary.
    pub fn clean_self(&mut self) {
        // A Removed tombstone in `data` shadows any flat entry for the same key;
        // both sides must be dropped together, otherwise the shadowed flat entry
        // survives as a stale record.
        //
        // Single pass over `data`: collect tombstone keys AND clean the map
        // (Modified→Clean, drop Removed). Collecting keys inside the retain
        // closure avoids a separate filter-and-collect walk.
        let mut tombstoned: HashSet<Bytes> = HashSet::new();
        self.retain(|k, v| match v.kind {
            IndexEntryKind::Clean => true,
            IndexEntryKind::Modified => {
                *v = v.as_clean_modified();
                true
            }
            IndexEntryKind::Removed => {
                tombstoned.insert(k.clone());
                false
            }
        });

        // Rebuild flat: convert Modified→Clean, drop Removed (in flat or shadowed by data).
        self.retain_flat(|k, mut iwp| {
            if iwp.is_removed() || tombstoned.contains(k) {
                None
            } else {
                iwp.kind = IndexEntryKind::Clean;
                Some(iwp)
            }
        });

        // All remaining entries are Clean after this call — bypass the slow
        // recount. `retain_flat` intentionally doesn't touch dirty_count; `retain`
        // above tracked BTreeMap changes but didn't see the flat dirty drops.
        self.dirty_count = 0;
    }

    /// Retain only unprocessed entries (those with WAL offset >= last_processed).
    /// Walks both the BTreeMap write buffer and the flat buffer; processed entries
    /// are dropped from either side.
    ///
    /// We cannot clear `self.flat` unconditionally: `promote_flat_job` can move
    /// still-pending writes from the BTreeMap into flat between the flush
    /// snapshot capture and this callback. Those entries' WAL offsets exceed
    /// `last_processed` and must survive — the new on-disk blob written by the
    /// flusher does not contain them (it was built from the snapshot, which
    /// observed them in the BTreeMap, but `clean_self` / compactor passes can
    /// drop them on the way to disk if the keys appear deleted at snapshot
    /// time). Dropping them here as well would leave a window where neither
    /// the on-disk index nor the in-memory overlay holds the durable WAL
    /// entry — observed in production as a dirty-overlay loss after a clean
    /// flush completion.
    pub fn retain_unprocessed(&mut self, last_processed: LastProcessed) {
        self.retain_flat(|_, iwp| {
            if last_processed.is_processed(&iwp) {
                None
            } else {
                Some(iwp)
            }
        });
        self.retain(|_, v| !last_processed.is_processed(v));
        // retain_flat doesn't track dirty_count; recompute.
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
        self.retain_flat(|_, iwp| (iwp.offset >= last_processed).then_some(iwp));
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

        self.retain_flat(|k, iwp| {
            if iwp.is_removed() || !to_retain.contains(k) {
                None
            } else {
                Some(iwp)
            }
        });
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
        self.promote_to_flat_inner(Self::PROMOTE_THRESHOLD)
    }

    /// Test-only variant that bypasses `PROMOTE_THRESHOLD`. Lets concurrent tests
    /// reliably open the `insert → promote → remove → FlushLoaded` window that
    /// triggers the `clean_self` stale-record bug, without needing 128+ keys per
    /// cell.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn promote_to_flat_force(&mut self) -> bool {
        self.promote_to_flat_inner(0)
    }

    fn promote_to_flat_inner(&mut self, threshold: usize) -> bool {
        if self.data.len() <= threshold {
            return false;
        }

        let (new_flat, dirty_count) = merge_into_flat(&self.flat, self.key_size, &self.data);

        self.flat = new_flat;
        self.dirty_count = dirty_count;
        self.key_bytes = 0;
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

    /// Constructs an IndexWalPosition from a WAL position read off disk.
    /// `WalPosition::INVALID` is the on-disk marker for a tombstone; any other
    /// position is treated as a clean entry.
    ///
    /// For a tombstone the concrete `len` is semantically meaningless (there
    /// is no WAL frame) — `into_wal_position()` collapses the `Removed` arm
    /// to `WalPosition::INVALID` regardless. We deliberately normalise the
    /// len to `0` so that `build_flat_bytes` can pack the kind into the top
    /// two bits of `encoded_len` without colliding with the real len bits
    /// (see `encode_kind_in_len` — it requires `len < 2^30`; the on-disk
    /// tombstone sentinel uses `len = u32::MAX`, which would violate that).
    fn from_disk(w: WalPosition) -> Self {
        if w == WalPosition::INVALID {
            Self {
                offset: w.offset(),
                len: 0,
                kind: IndexEntryKind::Removed,
            }
        } else {
            Self::new_clean(w)
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

/// An iterator for the old (pre-flat-buffer) varint-encoded variable-length key format.
/// Only retained for the unit test that validates `serialize_index_entries` round-trips.
#[cfg(test)]
pub struct VariableLenKeyIndexIterator<'a> {
    buf: SliceCursor<'a>,
}

#[cfg(test)]
impl<'a> VariableLenKeyIndexIterator<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            buf: SliceCursor::new(buf),
        }
    }
}

#[cfg(test)]
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
    fn test_retain_unprocessed_preserves_flat_unprocessed() {
        // Regression: `promote_flat_job` can move a still-pending write from
        // BTreeMap into flat between flush snapshot and `update_flushed_index`.
        // `retain_unprocessed` must walk flat (not clear it unconditionally),
        // otherwise the unprocessed entry is lost from in-memory state.
        let mut index = IndexTable::default();
        index.insert(vec![1].into(), WalPosition::test_value(100));
        index.insert(vec![2].into(), WalPosition::test_value(200));
        index.insert(vec![3].into(), WalPosition::test_value(500));

        // Simulate `promote_flat_job` moving BTreeMap → flat.
        index.promote_to_flat_force();
        assert!(index.data_btree_is_empty());
        assert_eq!(index.len(), 3);

        // last_processed = 300: positions 100, 200 are processed; 500 is not.
        index.retain_unprocessed(LastProcessed::new(300));

        assert!(index.get(&[1]).is_none());
        assert!(index.get(&[2]).is_none());
        assert_eq!(index.get(&[3]), Some(WalPosition::test_value(500)));
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
    fn test_clean_self_drops_flat_shadowed_by_tombstone() {
        // Regression: a Removed tombstone in `data` must also erase the
        // matching flat entry, otherwise clean_self leaks a stale record.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(1));
        table.promote_to_flat();
        // flat: [1]=Modified, data: empty
        assert_eq!(table.flat_len(), 1);

        table.remove(vec![1].into(), WalPosition::test_value(10));
        // flat: [1]=Modified, data: [1]=Removed (shadows flat)

        table.clean_self();

        assert!(table.get(&[1]).is_none());
        assert_eq!(table.flat_len(), 0);
        assert_eq!(table.dirty_count(), 0);
        assert_eq!(table.len(), 0);
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
    fn test_deserialize_preserves_tombstone() {
        // Regression: a tombstone round-tripped through serialize/deserialize
        // must decode back as a tombstone, not as a bogus live pointer.
        //
        // Pre-fix: `IndexWalPosition::from_disk(INVALID)` produced
        // `len = u32::MAX`, and `encode_kind_in_len(u32::MAX, Removed=2)`
        // collided with the kind bits (top 2 bits of the encoded len were
        // both already set by len), so decode yielded kind byte 3 — which
        // `IndexEntryKind::from_u8` maps to `Clean` via its fallback arm.
        // The tombstone then surfaced as a live `{u64::MAX, 0x3FFF_FFFF}`
        // hit on read.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(10));
        table.insert(vec![2].into(), WalPosition::test_value(20));
        // Tombstone on key [2].
        table.remove(vec![2].into(), WalPosition::test_value(25));

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
        // A tombstone must surface as INVALID via get().
        assert_eq!(
            deserialized.get(&[2]),
            Some(WalPosition::INVALID),
            "tombstone failed to round-trip through serialize/deserialize"
        );
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
    fn test_merge_dirty_and_clean_preserves_tombstones() {
        // Regression for two-level LSM: tombstones in `dirty` must survive
        // into the merged output so they shadow deeper-level entries in the
        // new L0. An earlier bug dropped tombstones when `self.flat.is_empty()`
        // — the common shape in any merge path where `self` is a freshly-built
        // overlay — causing phantom keys to resurrect from L1 under aggressive
        // promotion.

        // self: flat-empty shape (entries in `data`, flat empty).
        let mut self_table = IndexTable::default();
        self_table.insert(vec![1].into(), WalPosition::test_value(10));
        self_table.insert(vec![2].into(), WalPosition::test_value(20));
        self_table
            .data
            .values_mut()
            .for_each(|v| *v = v.as_clean_modified());
        assert!(self_table.flat.is_empty());

        // dirty overlay: tombstone for [2], tombstone for [9] (no matching
        // entry in self — still must survive to shadow any deeper level).
        let mut dirty = IndexTable::default();
        dirty.remove(vec![2].into(), WalPosition::test_value(25));
        dirty.remove(vec![9].into(), WalPosition::test_value(35));

        self_table.merge_dirty_and_clean(&dirty);

        // Live entry untouched.
        assert_eq!(self_table.get(&[1]), Some(WalPosition::test_value(10)));
        // Tombstone preserved in-place of the live entry — get() yields INVALID.
        assert_eq!(self_table.get(&[2]), Some(WalPosition::INVALID));
        assert!(
            self_table
                .data
                .get(&Bytes::from(vec![2]))
                .unwrap()
                .is_removed()
        );
        // Standalone tombstone preserved (would shadow a deeper-level entry).
        assert_eq!(self_table.get(&[9]), Some(WalPosition::INVALID));
        assert!(
            self_table
                .data
                .get(&Bytes::from(vec![9]))
                .unwrap()
                .is_removed()
        );

        // clean_self() then strips tombstones — the deepest-level contract.
        self_table.clean_self();
        assert!(!self_table.data.contains_key(&Bytes::from(vec![2])));
        assert!(!self_table.data.contains_key(&Bytes::from(vec![9])));
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

    #[test]
    fn test_unmerge_flushed_self_promoted_original_not() {
        // Reproduces the promote_flat_job race: original's entries live in .data,
        // but by the time unmerge_flushed runs they have been promoted into self.flat.
        let mut original = IndexTable::default();
        original.insert(vec![1].into(), WalPosition::test_value(2));
        original.insert(vec![2].into(), WalPosition::test_value(3));
        original.insert(vec![3].into(), WalPosition::test_value(4));
        // original is NOT promoted — its entries still live in .data.
        assert!(original.flat.is_empty());

        // current was cloned from original, then promoted (simulating promote_flat_job),
        // then accepted a new write after promotion.
        let mut current = original.clone();
        current.promote_to_flat();
        assert!(current.data.is_empty());
        assert!(!current.flat.is_empty());
        current.insert(vec![4].into(), WalPosition::test_value(5));

        let len_before = current.len();
        current.unmerge_flushed(&original, LastProcessed::new_test(u64::MAX));
        let len_after = current.len();

        // [1, 2, 3] removed from self.flat; [4] remains in self.data.
        assert_eq!(len_before as i64 - len_after as i64, 3);
        assert!(current.get(&[1]).is_none());
        assert!(current.get(&[2]).is_none());
        assert!(current.get(&[3]).is_none());
        assert_eq!(current.get(&[4]), Some(WalPosition::test_value(5)));
        // The flushed entries must not continue to count as dirty.
        assert_eq!(current.dirty_count(), 1);
    }

    #[test]
    fn test_unmerge_flushed_self_promoted_stale_flat_after_overwrite() {
        // original has K@pos1 in .data. self was promoted so K@pos1 is in self.flat,
        // then a newer write lands K@pos2 in self.data. unmerge_flushed must drop
        // the stale flat entry while leaving the newer data entry intact.
        let mut original = IndexTable::default();
        original.insert(vec![1].into(), WalPosition::test_value(2));
        original.insert(vec![2].into(), WalPosition::test_value(3));

        let mut current = original.clone();
        current.promote_to_flat();
        // Overwrite key [1] with a newer position in self.data.
        current.insert(vec![1].into(), WalPosition::test_value(10));

        current.unmerge_flushed(&original, LastProcessed::new_test(u64::MAX));

        // Key [1] still has its newer position; key [2] fully removed.
        assert_eq!(current.get(&[1]), Some(WalPosition::test_value(10)));
        assert!(current.get(&[2]).is_none());
        // No stale [1]@pos2 left behind in flat.
        assert_eq!(current.flat_binary_search(&[1]), None);
    }

    // -----------------------------------------------------------------------
    // merge_into_flat — direct tests for the two-pass merge writer
    // -----------------------------------------------------------------------

    use super::flat::merge_into_flat;

    /// Decode a flat buffer back to a Vec for assertion-friendly comparison.
    fn decode_flat(flat: &Bytes, key_size: Option<usize>) -> Vec<(Vec<u8>, IndexWalPosition)> {
        FlatIter::new(flat, key_size)
            .map(|(k, iwp)| (k.to_vec(), iwp))
            .collect()
    }

    fn iwp_modified(n: u64) -> IndexWalPosition {
        IndexWalPosition::new_modified(WalPosition::test_value(n))
    }
    fn iwp_clean(n: u64) -> IndexWalPosition {
        IndexWalPosition::new_clean(WalPosition::test_value(n))
    }
    fn iwp_removed(n: u64) -> IndexWalPosition {
        IndexWalPosition::new_removed(WalPosition::test_value(n))
    }

    #[test]
    fn test_merge_into_flat_var_len_btree_only() {
        let flat = Bytes::default();
        let btree: BTreeMap<Bytes, IndexWalPosition> = BTreeMap::from_iter([
            (vec![1].into(), iwp_modified(1)),
            (vec![2, 5].into(), iwp_clean(2)),
            (vec![9].into(), iwp_modified(3)),
        ]);

        let (new_flat, dirty) = merge_into_flat(&flat, None, &btree);
        assert_eq!(dirty, 2);
        let entries = decode_flat(&new_flat, None);
        assert_eq!(
            entries,
            vec![
                (vec![1], iwp_modified(1)),
                (vec![2, 5], iwp_clean(2)),
                (vec![9], iwp_modified(3)),
            ]
        );

        // Exact size: header (4 + 4*n) + per-entry (14 + key_len).
        let expected = 4 + 4 * 3 + (14 + 1) + (14 + 2) + (14 + 1);
        assert_eq!(new_flat.len(), expected);
    }

    #[test]
    fn test_merge_into_flat_var_len_flat_only() {
        // Build a flat buffer via promote_to_flat, then call merge_into_flat with empty btree.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(1));
        table.insert(vec![2].into(), WalPosition::test_value(2));
        table.insert(vec![3].into(), WalPosition::test_value(3));
        table.promote_to_flat();
        let flat = table.flat.clone();
        let btree: BTreeMap<Bytes, IndexWalPosition> = BTreeMap::new();

        let (new_flat, dirty) = merge_into_flat(&flat, None, &btree);
        // All three flat entries are Modified.
        assert_eq!(dirty, 3);
        // Output equals input bytes (same entries, same order, same encoding).
        assert_eq!(new_flat.as_ref(), flat.as_ref());
    }

    #[test]
    fn test_merge_into_flat_var_len_btree_overrides_flat() {
        // flat: [1]=Modified@1, [3]=Modified@3
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(1));
        table.insert(vec![3].into(), WalPosition::test_value(3));
        table.promote_to_flat();
        let flat = table.flat.clone();

        // btree: [1]=Modified@10 (overrides flat), [2]=Modified@2 (new)
        let btree: BTreeMap<Bytes, IndexWalPosition> = BTreeMap::from_iter([
            (vec![1].into(), iwp_modified(10)),
            (vec![2].into(), iwp_modified(2)),
        ]);

        let (new_flat, dirty) = merge_into_flat(&flat, None, &btree);
        // 3 emitted entries, all dirty (Modified).
        assert_eq!(dirty, 3);
        let entries = decode_flat(&new_flat, None);
        assert_eq!(
            entries,
            vec![
                (vec![1], iwp_modified(10)), // btree wins
                (vec![2], iwp_modified(2)),
                (vec![3], iwp_modified(3)),
            ]
        );

        // Exact size: 3 entries with 1-byte keys each.
        let expected = 4 + 4 * 3 + 3 * (14 + 1);
        assert_eq!(new_flat.len(), expected);
    }

    #[test]
    fn test_merge_into_flat_var_len_interleaved() {
        // flat: [1], [4], [7]   btree: [2], [5], [8]
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(1));
        table.insert(vec![4].into(), WalPosition::test_value(4));
        table.insert(vec![7].into(), WalPosition::test_value(7));
        table.promote_to_flat();
        let flat = table.flat.clone();

        let btree: BTreeMap<Bytes, IndexWalPosition> = BTreeMap::from_iter([
            (vec![2].into(), iwp_clean(2)),
            (vec![5].into(), iwp_clean(5)),
            (vec![8].into(), iwp_clean(8)),
        ]);

        let (new_flat, dirty) = merge_into_flat(&flat, None, &btree);
        // 3 flat dirty (Modified) + 0 btree dirty (Clean) = 3.
        assert_eq!(dirty, 3);
        let keys: Vec<Vec<u8>> = decode_flat(&new_flat, None)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(
            keys,
            vec![vec![1], vec![2], vec![4], vec![5], vec![7], vec![8]]
        );
    }

    #[test]
    fn test_merge_into_flat_var_len_btree_removed_overrides_flat_modified() {
        // flat: [1]=Modified, [2]=Modified. btree marks [1] as Removed.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(1));
        table.insert(vec![2].into(), WalPosition::test_value(2));
        table.promote_to_flat();
        let flat = table.flat.clone();
        let btree: BTreeMap<Bytes, IndexWalPosition> =
            BTreeMap::from_iter([(vec![1].into(), iwp_removed(10))]);

        let (new_flat, dirty) = merge_into_flat(&flat, None, &btree);
        // Modified [2] from flat + Removed [1] from btree = 2 dirty.
        assert_eq!(dirty, 2);
        let entries = decode_flat(&new_flat, None);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, vec![1]);
        assert_eq!(entries[0].1.kind, IndexEntryKind::Removed);
        assert_eq!(entries[0].1.offset, WalPosition::test_value(10).offset());
        assert_eq!(entries[1].0, vec![2]);
        assert_eq!(entries[1].1.kind, IndexEntryKind::Modified);
    }

    #[test]
    fn test_merge_into_flat_var_len_preserves_flat_tombstone() {
        // Promote a tombstone into flat (insert → promote → remove → promote).
        // The standalone Removed entry must survive a subsequent merge with
        // an empty btree — needed for L0/L1 LSM where a tombstone shadows a
        // live entry in the lower level.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(1));
        table.insert(vec![2].into(), WalPosition::test_value(2));
        table.promote_to_flat();
        table.remove(vec![1].into(), WalPosition::test_value(10));
        table.promote_to_flat();
        // flat now has [1]=Removed, [2]=Modified.
        let flat = table.flat.clone();
        // Sanity: flat really has the tombstone encoded.
        let pre = decode_flat(&flat, None);
        assert_eq!(pre.len(), 2);
        assert_eq!(pre[0].1.kind, IndexEntryKind::Removed);

        // Merge with empty btree — Removed must round-trip unchanged.
        let (new_flat, dirty) = merge_into_flat(&flat, None, &BTreeMap::new());
        assert_eq!(dirty, 2); // Modified + Removed both dirty.
        let entries = decode_flat(&new_flat, None);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, vec![1]);
        assert_eq!(entries[0].1.kind, IndexEntryKind::Removed);
        assert_eq!(entries[1].0, vec![2]);
        assert_eq!(entries[1].1.kind, IndexEntryKind::Modified);
        // Byte-identical: same entries through encode → decode → encode.
        assert_eq!(new_flat.as_ref(), flat.as_ref());
    }

    #[test]
    fn test_merge_into_flat_var_len_btree_modified_overrides_flat_removed() {
        // Re-insertion case: flat has [1]=Removed, btree has [1]=Modified.
        // btree wins, the resulting entry must be Modified, not Removed.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(1));
        table.promote_to_flat();
        table.remove(vec![1].into(), WalPosition::test_value(10));
        table.promote_to_flat();
        let flat = table.flat.clone();

        let btree: BTreeMap<Bytes, IndexWalPosition> =
            BTreeMap::from_iter([(vec![1].into(), iwp_modified(20))]);
        let (new_flat, dirty) = merge_into_flat(&flat, None, &btree);
        assert_eq!(dirty, 1);
        let entries = decode_flat(&new_flat, None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1.kind, IndexEntryKind::Modified);
        assert_eq!(entries[0].1.offset, WalPosition::test_value(20).offset());
    }

    #[test]
    fn test_merge_into_flat_fixed_len_preserves_flat_tombstone() {
        // Same as the var-len tombstone-preservation test, but for fixed-len keys.
        let mut table = IndexTable::default();
        table.insert(k4(10), WalPosition::test_value(1));
        table.insert(k4(20), WalPosition::test_value(2));
        table.key_size = Some(4);
        table.promote_to_flat();
        table.remove(k4(10), WalPosition::test_value(50));
        table.promote_to_flat();
        let flat = table.flat.clone();

        let (new_flat, dirty) = merge_into_flat(&flat, Some(4), &BTreeMap::new());
        assert_eq!(dirty, 2);
        let entries = decode_flat(&new_flat, Some(4));
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, k4(10).to_vec());
        assert_eq!(entries[0].1.kind, IndexEntryKind::Removed);
        assert_eq!(entries[1].0, k4(20).to_vec());
        assert_eq!(entries[1].1.kind, IndexEntryKind::Modified);
        // Byte-identical round-trip.
        assert_eq!(new_flat.as_ref(), flat.as_ref());
    }

    #[test]
    fn test_merge_into_flat_empty_inputs() {
        let flat = Bytes::default();
        let btree: BTreeMap<Bytes, IndexWalPosition> = BTreeMap::new();
        let (new_flat, dirty) = merge_into_flat(&flat, None, &btree);
        assert!(new_flat.is_empty());
        assert_eq!(dirty, 0);
    }

    #[test]
    fn test_merge_into_flat_fixed_len_btree_overrides_flat() {
        // flat with two fixed-len keys.
        let mut table = IndexTable::default();
        table.insert(k4(10), WalPosition::test_value(10));
        table.insert(k4(30), WalPosition::test_value(30));
        table
            .data
            .values_mut()
            .for_each(|v| *v = v.as_clean_modified());
        table.key_size = Some(4);
        table.promote_to_flat();
        let flat = table.flat.clone();

        // btree overrides k4(10), inserts k4(20).
        let btree: BTreeMap<Bytes, IndexWalPosition> =
            BTreeMap::from_iter([(k4(10), iwp_modified(100)), (k4(20), iwp_modified(20))]);

        let (new_flat, dirty) = merge_into_flat(&flat, Some(4), &btree);
        // 2 btree Modified + 0 flat dirty (clean) = 2.
        assert_eq!(dirty, 2);
        let entries = decode_flat(&new_flat, Some(4));
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, k4(10).to_vec());
        assert_eq!(entries[0].1, iwp_modified(100)); // btree wins
        assert_eq!(entries[1].0, k4(20).to_vec());
        assert_eq!(entries[1].1, iwp_modified(20));
        assert_eq!(entries[2].0, k4(30).to_vec());
        assert_eq!(entries[2].1, iwp_clean(30));

        // Exact size: 3 entries × (4 + WalPosition::LENGTH).
        assert_eq!(new_flat.len(), 3 * (4 + WalPosition::LENGTH));
    }

    #[test]
    fn test_merge_into_flat_fixed_len_btree_only() {
        let flat = Bytes::default();
        let btree: BTreeMap<Bytes, IndexWalPosition> = BTreeMap::from_iter([
            (k4(1), iwp_modified(1)),
            (k4(2), iwp_clean(2)),
            (k4(3), iwp_removed(3)),
        ]);

        let (new_flat, dirty) = merge_into_flat(&flat, Some(4), &btree);
        // Modified + Removed = 2 dirty (Clean is not dirty).
        assert_eq!(dirty, 2);
        let entries = decode_flat(&new_flat, Some(4));
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].1.kind, IndexEntryKind::Modified);
        assert_eq!(entries[1].1.kind, IndexEntryKind::Clean);
        assert_eq!(entries[2].1.kind, IndexEntryKind::Removed);
        assert_eq!(new_flat.len(), 3 * (4 + WalPosition::LENGTH));
    }

    #[test]
    fn test_merge_into_flat_fixed_len_flat_only() {
        let mut table = IndexTable::default();
        table.insert(k4(10), WalPosition::test_value(10));
        table.insert(k4(20), WalPosition::test_value(20));
        table.key_size = Some(4);
        table.promote_to_flat();
        let flat = table.flat.clone();

        let (new_flat, dirty) = merge_into_flat(&flat, Some(4), &BTreeMap::new());
        // Both flat entries are Modified.
        assert_eq!(dirty, 2);
        // Byte-identical: same entries, same order.
        assert_eq!(new_flat.as_ref(), flat.as_ref());
    }

    #[test]
    fn test_merge_into_flat_var_len_dirty_count_mixed() {
        // flat: [1]=Clean, [2]=Modified, [3]=Removed
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(1));
        table.insert(vec![2].into(), WalPosition::test_value(2));
        table.insert(vec![3].into(), WalPosition::test_value(3));
        // Demote [1] and [3] to clean/removed before promotion.
        table
            .data
            .entry(vec![1].into())
            .and_modify(|v| *v = v.as_clean_modified());
        table.data.insert(vec![3].into(), iwp_removed(3));
        table.promote_to_flat();
        // flat now: [1]=Clean, [2]=Modified, [3]=Removed.
        let flat = table.flat.clone();

        // btree: [4]=Clean (overrides nothing), [2]=Modified@20 (overrides [2])
        let btree: BTreeMap<Bytes, IndexWalPosition> = BTreeMap::from_iter([
            (vec![2].into(), iwp_modified(20)),
            (vec![4].into(), iwp_clean(4)),
        ]);

        let (new_flat, dirty) = merge_into_flat(&flat, None, &btree);
        // Emitted: [1]=Clean, [2]=Modified(btree-overrides), [3]=Removed, [4]=Clean.
        // Dirty: Modified + Removed = 2.
        assert_eq!(dirty, 2);
        let entries = decode_flat(&new_flat, None);
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].1.kind, IndexEntryKind::Clean);
        assert_eq!(entries[1].1.kind, IndexEntryKind::Modified);
        assert_eq!(entries[1].1.offset, WalPosition::test_value(20).offset());
        assert_eq!(entries[2].1.kind, IndexEntryKind::Removed);
        assert_eq!(entries[3].1.kind, IndexEntryKind::Clean);
    }

    #[test]
    fn test_merge_into_flat_var_len_size_with_varying_key_lengths() {
        // Verify exact buffer size for keys of different lengths.
        let flat = Bytes::default();
        let btree: BTreeMap<Bytes, IndexWalPosition> = BTreeMap::from_iter([
            (vec![1].into(), iwp_modified(1)),
            (vec![2, 3].into(), iwp_modified(2)),
            (vec![4, 5, 6].into(), iwp_modified(3)),
            (vec![7, 8, 9, 10, 11].into(), iwp_modified(4)),
        ]);

        let (new_flat, _) = merge_into_flat(&flat, None, &btree);
        // header: 4 + 4*4 = 20
        // entries: (14+1) + (14+2) + (14+3) + (14+5) = 15+16+17+19 = 67
        assert_eq!(new_flat.len(), 20 + 67);
    }

    impl IndexWalPosition {
        fn is_modified(&self) -> bool {
            self.kind == IndexEntryKind::Modified
        }
    }
}
