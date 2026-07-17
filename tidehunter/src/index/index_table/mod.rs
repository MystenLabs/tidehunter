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
use smallvec::{SmallVec, smallvec};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::btree_map::Entry;
use std::ops::Bound;

/// Positions recorded in the `data` overlay for a single key, kept ascending by
/// `IndexWalPosition` (so the last element is the latest). Inline capacity 1
/// keeps the common single-write-per-key case allocation-free; the rare key
/// rewritten again before promotion spills its extra positions to the heap.
pub(super) type DataPositions = SmallVec<[IndexWalPosition; 1]>;

/// The `data` write buffer: one entry per key, mapping the key to its ordered
/// positions. A `BTreeMap` (vs. a flat `BTreeSet<(key, position)>`) lets the
/// hot insert path reach a key with a single tree descent via `entry`, stores
/// each key once, and makes `data.len()` the unique-key count the rest of the
/// impl assumes.
pub(super) type DataOverlay = BTreeMap<Bytes, DataPositions>;

#[derive(Default, Clone, Debug)]
#[doc(hidden)]
pub struct IndexTable {
    /// Write buffer: new entries land here before the next `promote_to_flat` call.
    /// `promote_to_flat` drains only entries whose WAL offset is below the
    /// supplied `last_processed`; unprocessed (in-flight) entries remain here
    /// until a later promote observes a high enough `last_processed`.
    ///
    /// Stored as a `BTreeMap<key, positions>` so multiple positions for the
    /// same key may coexist (the `positions` vec, ascending by
    /// `IndexWalPosition`). Observable semantics treat the entry with the
    /// largest `IndexWalPosition` — the last vec element — as the live value
    /// for that key.
    data: DataOverlay,
    /// Sum of unique key lengths currently in `data` (each key counted once,
    /// regardless of how many positions exist for it). Maintained
    /// incrementally where cheap, recomputed via `recount_data_stats`
    /// after bulk operations.
    key_bytes: usize,
    /// Count of dirty (Modified or Removed) entries across both `data` and
    /// `flat`. For `data`, a key contributes at most 1 — determined by
    /// whether its *latest* position is dirty.
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
    ///
    /// **Invariant: flat never holds an in-flight entry**, which is what
    /// `has_unprocessed` and `retain_unprocessed` rely on to treat it as fully
    /// processed against any caller-supplied `last_processed`. Each of its
    /// writers maintains this independently:
    /// - `promote_to_flat` admits only entries with WAL `offset` strictly
    ///   below the promote's `last_processed` threshold (monotonically
    ///   non-decreasing across promotes).
    /// - `merge_dirty` → `fold_dirty_flat` folds a disk-loaded blob's flat
    ///   into this buffer — never into `data`. A blob carries no in-flight
    ///   entries: the flusher filters the merged table with `retain_processed`
    ///   before serializing, and the blob entries that can still sit at or
    ///   above a caller's frontier — relocated copies re-pointed to WAL-tail
    ///   offsets and `INVALID`-sentinel tombstones — are durable in the blob
    ///   itself, so treating them as processed is sound.
    /// - WAL replay (`from_sorted_entries`) builds flat from replayed
    ///   entries, all below the post-replay frontier any later caller can
    ///   observe.
    ///
    /// The complementary invariant — disk-derived entries (relocated copies,
    /// `INVALID`-sentinel tombstones) live only here, never in `data`, which
    /// holds only genuine in-memory WAL writes — is what makes
    /// `retain_processed`'s frontier predicate on `data` sound; see the
    /// `debug_assert` there.
    flat: Bytes,
    /// Fixed key size for this key space, or `None` for variable-length keys.
    /// Determines which flat format is used.
    key_size: Option<usize>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct IndexWalPosition {
    pub(super) offset: u64,
    pub(super) len: u32,
    pub(super) kind: IndexEntryKind,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, PartialOrd, Ord)]
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

/// Iterate one entry per key — the key and its latest (largest IWP) position.
/// Each key's `positions` vec is kept ascending, so the latest is its last
/// element. Keys are yielded in ascending order (`BTreeMap` iteration).
pub(super) fn data_latest_per_key(
    data: &DataOverlay,
) -> impl Iterator<Item = (&Bytes, &IndexWalPosition)> + '_ {
    data.iter()
        .map(|(k, positions)| (k, positions.last().expect("data positions are never empty")))
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

        // Enforce the ordering assertion for keys promoted to flat but not yet
        // in `data`. Done before `entry` so the immutable flat read doesn't
        // overlap the mutable `data` borrow the entry holds (the `contains_key`
        // probe is debug/test-only — release does a single `data` descent).
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
            Entry::Occupied(mut oc) => {
                // `positions` is non-empty and ascending by IWP, so the latest
                // is its last element and the new (strictly greater) position
                // appends in order.
                let prev = *oc.get().last().unwrap();
                assert!(
                    prev.offset < v.offset,
                    "Index WAL position must be increasing"
                );
                adjust_dirty_count(&mut self.dirty_count, !prev.is_clean(), !v.is_clean());
                oc.get_mut().push(v);
                Some(prev.into_wal_position())
            }
            Entry::Vacant(va) => {
                self.key_bytes += va.key().len();
                adjust_dirty_count(&mut self.dirty_count, false, !v.is_clean());
                va.insert(smallvec![v]);
                None
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
        // ---- data side: dirty's in-flight overlay folds into self.data ----
        // These are genuine WAL-offset writes — the only entries the
        // offset-based filters (`retain_processed`, `retain_unprocessed`,
        // `promote_to_flat`) ever act on. `_and_clean` demotes Modified→Clean
        // because they are about to become clean on disk.
        for (k, v) in dirty.data_iter_latest() {
            #[cfg(any(debug_assertions, feature = "test_methods"))]
            if promote_to_clean
                && let Some(found) = self.data_get_latest(k)
                && found.offset > v.offset
            {
                panic!("found.offset {} > v.offset {}", found.offset, v.offset);
            }
            let incoming = if promote_to_clean && !v.is_removed() {
                v.as_clean_modified()
            } else {
                *v
            };
            self.data_insert_sorted(k.clone(), incoming);
        }

        // ---- flat side: dirty's disk-derived `flat` folds into self.flat ----
        // Invariant: entries that originated on disk live only in `flat`, never
        // in the `data` overlay. The old code lifted them into `data`, where
        // the offset-based filters then misread them — most damagingly,
        // `retain_processed` saw a disk tombstone's INVALID sentinel offset
        // (u64::MAX) as an unprocessed post-frontier write and dropped it,
        // resurrecting the value the tombstone shadowed in a deeper level.
        // `flat`'s dirty-entry count: `fold_dirty_flat` tallies it while building
        // the new buffer, so the merged blob is not re-decoded here. When there
        // is no `dirty.flat` to fold, `self.flat` is unchanged and counted once.
        let flat_dirty = if !dirty.flat.is_empty() {
            self.fold_dirty_flat(dirty, promote_to_clean)
        } else {
            FlatIter::new(&self.flat, self.key_size)
                .filter(|(_, v)| !v.is_clean())
                .count()
        };

        // Data-side stats are a cheap scan: `data` holds only the in-flight
        // overlay (small; empty in the common freshly-loaded-base merge).
        let mut key_bytes = 0usize;
        let mut data_dirty = 0usize;
        for (k, v) in self.data_iter_latest() {
            key_bytes += k.len();
            if !v.is_clean() {
                data_dirty += 1;
            }
        }
        self.key_bytes = key_bytes;
        self.dirty_count = flat_dirty + data_dirty;
    }

    /// Merge `dirty.flat` (disk-derived, already-processed entries) into
    /// `self.flat`, upholding the invariant that disk entries never enter the
    /// `data` overlay.
    ///
    /// `dirty` is the fresher side, so on equal keys its flat entry wins over
    /// `self.flat` — except where `dirty.data` already carries the key, in
    /// which case the overlay (just merged into `self.data`) is freshest and
    /// `self.flat`'s entry is kept untouched as the below-overlay value
    /// (matching the prior behavior of skipping `dirty.flat` for overlay keys).
    ///
    /// For a key taken from `dirty.flat`, any now-stale `self.data` tuple is
    /// dropped so the flat value is the one observed. In every production
    /// caller `self` is a freshly-loaded merge base with empty `data`, so that
    /// cleanup is a no-op there; it only matters for synthetic callers that
    /// pre-seed `self.data` with values older than `dirty` (debug asserts
    /// enforce the "older" contract on both the dropped tuples and equal-key
    /// flat entries).
    ///
    /// Returns the number of dirty (non-clean) entries in the rebuilt `flat`,
    /// so the caller can finish its stats without re-decoding the blob.
    fn fold_dirty_flat(&mut self, dirty: &Self, promote_to_clean: bool) -> usize {
        if self.flat.is_empty() {
            // `self` may be a default table that has not adopted a key layout.
            self.key_size = dirty.key_size;
        }
        // Each side is decoded with its own `key_size` and re-encoded with the
        // output (`self`) layout. The two need not match: an in-memory overlay
        // promotes to a var-len `flat` (`key_size = None`) even in a fixed-size
        // keyspace, while a disk-loaded base is fixed. Keys are length-consistent
        // for a keyspace, so `build_flat_bytes` re-encodes them either way.
        let demote = |iwp: IndexWalPosition| {
            if promote_to_clean && !iwp.is_removed() {
                iwp.as_clean_modified()
            } else {
                iwp
            }
        };

        let self_flat = std::mem::take(&mut self.flat);
        let mut self_it = FlatIter::new(&self_flat, self.key_size).peekable();
        let mut dirty_it = FlatIter::new(&dirty.flat, dirty.key_size).peekable();
        // Upper bound on the merged entry count (O(1) flat_count each); sizing
        // up front avoids the reallocation growth of an unbounded push loop.
        let cap = flat_count(&self_flat, self.key_size) + flat_count(&dirty.flat, dirty.key_size);
        let mut merged: Vec<(Bytes, IndexWalPosition)> = Vec::with_capacity(cap);
        let mut keys_from_dirty: Vec<(Bytes, IndexWalPosition)> = Vec::new();

        loop {
            let ord = match (self_it.peek(), dirty_it.peek()) {
                (None, None) => break,
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some((sk, _)), Some((dk, _))) => sk.cmp(dk),
            };
            match ord {
                Ordering::Less => {
                    let (k, v) = self_it.next().unwrap();
                    merged.push((self_flat.slice_to_bytes(k), v));
                }
                Ordering::Greater => {
                    let (k, v) = dirty_it.next().unwrap();
                    if dirty.data_contains_key(k) {
                        continue; // overlay carries the fresher value
                    }
                    let key = dirty.flat.slice_to_bytes(k);
                    keys_from_dirty.push((key.clone(), v));
                    merged.push((key, demote(v)));
                }
                Ordering::Equal => {
                    let (sk, sv) = self_it.next().unwrap();
                    let (dk, dv) = dirty_it.next().unwrap();
                    if dirty.data_contains_key(dk) {
                        // Overlay is freshest; keep self.flat's entry beneath it.
                        merged.push((self_flat.slice_to_bytes(sk), sv));
                    } else {
                        // Caller contract: `dirty` is the fresher side. Offsets
                        // are comparable unless either side is a disk tombstone
                        // (`INVALID` sentinel offset, meaningless for ordering).
                        debug_assert!(
                            sv.offset == WalPosition::INVALID.offset()
                                || dv.offset == WalPosition::INVALID.offset()
                                || dv.offset >= sv.offset,
                            "fold_dirty_flat: dirty.flat entry at offset {} is older than \
                             self.flat entry at offset {} for the same key",
                            dv.offset,
                            sv.offset
                        );
                        let key = dirty.flat.slice_to_bytes(dk);
                        keys_from_dirty.push((key.clone(), dv));
                        merged.push((key, demote(dv)));
                    }
                }
            }
        }

        // Drop overlay positions shadowed by a value just taken from dirty.flat:
        // every position for that key is below the winner, so the whole key goes.
        for (key, winner) in keys_from_dirty {
            if let Some(stale) = self.data.remove(&key) {
                for e in &stale {
                    // Caller contract: any same-key `self.data` position is older
                    // than the dirty.flat value shadowing it — dropping a newer
                    // one here would lose a write. (A tombstone winner sits at the
                    // INVALID sentinel offset u64::MAX, trivially satisfying this.)
                    debug_assert!(
                        e.offset <= winner.offset,
                        "fold_dirty_flat: dropping self.data position at offset {} newer than \
                         the dirty.flat value at offset {} that shadows it",
                        e.offset,
                        winner.offset
                    );
                }
            }
        }

        // Tally the rebuilt flat's dirty entries here (cheap — over the decoded
        // tuples) so the caller need not re-decode `self.flat`.
        let flat_dirty = merged.iter().filter(|(_, v)| !v.is_clean()).count();
        self.flat = build_flat_bytes(merged, self.key_size);
        flat_dirty
    }

    /// Remove the snapshot entries the flush made durable, keeping only
    /// post-snapshot writes. `last_processed` gates the `data` overlay (keep
    /// unprocessed in-flight writes); `flat` unmerges on position match alone,
    /// since every flat entry is covered by the post-flush fall-through read.
    pub fn unmerge_flushed(&mut self, original: &Self, last_processed: LastProcessed) {
        // Walk original.data; remove matching entries from self.data. If promote_flat_job
        // ran between snapshot capture and now, an entry present in original.data may now
        // live in self.flat — record those for the flat-rebuild pass below.
        let mut pending_flat: HashMap<Bytes, IndexWalPosition> = HashMap::new();
        for (k, positions) in original.data.iter() {
            for v in positions {
                if !last_processed.is_processed(v) {
                    // Do not unmerge entries that might have in-flight operations
                    continue;
                }
                // Remove the position in self.data with matching key + update_position
                // (kind may differ, but offset+len must match). Use into_update_position:
                // into_wal_position collapses all Removed entries to INVALID, which would
                // incorrectly match two different Removed tombstones at distinct WAL offsets.
                let v_update_position = v.into_update_position();
                let removed_from_data = self
                    .data_remove_matching(k, |sv| sv.into_update_position() == v_update_position);
                if !removed_from_data {
                    // Either no matching position in self.data (likely promoted
                    // into self.flat) or only newer positions exist for the key.
                    // Let the flat rebuild below drop it by key+position match.
                    pending_flat.insert(k.clone(), *v);
                }
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
                // Drop self.flat entries that position-match original.flat: they
                // were flushed, and the post-flush read falls through to disk. No
                // frontier gate (flat never holds unflushed writes), so a disk
                // tombstone (offset u64::MAX) is fine here. into_update_position
                // so two distinct Removed entries don't compare equal.
                let remove_by_flat = if orig_it.peek().map(|(ok, _)| *ok == sk).unwrap_or(false) {
                    let (_, o_iwp) = orig_it.next().unwrap();
                    s_iwp.into_update_position() == o_iwp.into_update_position()
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
        let (kb, dc) = self.recount_stats();
        self.key_bytes = kb;
        self.dirty_count = dc;
    }

    /// Count the number of dirty (Modified or Removed) entries. O(1) — uses cached count.
    pub fn dirty_count(&self) -> usize {
        self.dirty_count
    }

    /// Retain in `data` only the keys whose *latest* IWP satisfies `f`. The
    /// decision is per-unique-key — when a key fails the predicate, every
    /// tuple for it is dropped, so a stale older tuple cannot survive to
    /// shadow the now-removed latest. This matches the BTreeMap-era contract
    /// where each key had a single entry. `key_bytes` and `dirty_count` are
    /// recomputed after.
    fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&Bytes, &IndexWalPosition) -> bool,
    {
        self.drop_data_keys(f);
        let (kb, dc) = self.recount_stats();
        self.key_bytes = kb;
        self.dirty_count = dc;
    }

    /// Drop from `data` every tuple of any key whose *latest* IWP fails `f`.
    /// The decision is per-unique-key, matching [`Self::retain`], but this
    /// helper does **not** refresh `key_bytes`/`dirty_count` — callers that
    /// already know the resulting flat dirty count can recount just the data
    /// side (see `promote_to_flat_inner`).
    fn drop_data_keys<F>(&mut self, mut f: F)
    where
        F: FnMut(&Bytes, &IndexWalPosition) -> bool,
    {
        // Keep a key (and thus all its positions) iff its latest IWP passes `f`.
        self.data
            .retain(|k, positions| f(k, positions.last().expect("data positions are never empty")));
    }

    /// Rebuild the flat buffer by walking existing entries through `keep`. The
    /// closure returns `None` to drop the entry or `Some(new_iwp)` to keep it
    /// (optionally with a rewritten position — e.g. Modified→Clean).
    ///
    /// `dirty_count` is **not** adjusted here — callers are expected to either
    /// set it (`clean_self`: goes to 0) or recompute via `recount_stats`
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
        // `data` (write buffer) takes priority over `flat` — its latest entry
        // for `k` shadows any flat entry for the same key.
        if let Some(p) = self.data_get_latest(k) {
            return Some(p.into_wal_position());
        }
        // Fall through to the flat array.
        self.flat_binary_search(k)
            .map(|iwp| iwp.into_wal_position())
    }

    /// Checkpoint read: like [`Self::get`], but only considers index positions
    /// whose WAL offset is processed relative to `last_processed` (offset <
    /// `last_processed`). Positions at or above it — writes that landed after
    /// the checkpoint frontier was captured — are ignored, so the returned
    /// position is the latest value for `k` as of `last_processed`.
    ///
    /// The data overlay is searched for the latest *processed* position; if the
    /// key only has unprocessed positions there (a write that postdates the
    /// checkpoint), we fall through to `flat`. Falling through is correct
    /// because, for any key, `data` entries always have a higher WAL offset
    /// than a `flat` entry for the same key (data is the newer write buffer;
    /// `promote_to_flat` only ever moves lower offsets down). So a processed
    /// data entry, when present, is the freshest as-of-frontier value and wins.
    ///
    /// `flat` is always safe to consult in full: by the unprocessed-write
    /// retention invariant (see the `crate::checkpoint` module docs) no user
    /// value newer than the frontier ever reaches flat. (Relocated copies in
    /// flat can sit at offsets `>= last_processed`, but they hold the
    /// as-of-frontier value, so reading them unfiltered is still correct.)
    pub fn get_at(&self, k: &[u8], last_processed: LastProcessed) -> Option<WalPosition> {
        if let Some(p) = self.data_get_latest_processed(k, last_processed) {
            return Some(p.into_wal_position());
        }
        self.flat_binary_search(k)
            .map(|iwp| iwp.into_wal_position())
    }

    /// Similar to `get`, but returns the WAL position of the update operation for both
    /// inserts and deletes.
    pub fn get_update_position(&self, k: &Bytes) -> Option<WalPosition> {
        if let Some(p) = self.data_get_latest(k) {
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

    /// Walk to the next entry in the given direction. `data` and `flat` are
    /// both sorted ascending, so Forward picks the smaller key and Backward
    /// the larger; `data` wins on ties in either direction (freshest data).
    ///
    /// Removed tombstones in `data` are surfaced (as INVALID positions) so
    /// callers can shadow a matching on-disk entry. A flat entry shadowed by a
    /// same-key `data` tombstone needs no separate pre-filter: at the step
    /// where that flat key becomes `flat_cand` the key also exists in `data`
    /// on the correct side of `prev`, so `data_cand` is `<=` it (Forward) /
    /// `>=` it (Backward) and wins the tie below. `data_next_*` surfaces a
    /// key's latest position — the very position `data_get_latest` would
    /// return — so the tie-break alone suppresses the flat entry and surfaces
    /// the tombstone, one shadowed key per scan step.
    fn next_entry_directional(
        &self,
        prev: Option<&[u8]>,
        direction: Direction,
    ) -> Option<(Bytes, WalPosition)> {
        let data_cand: Option<(&Bytes, &IndexWalPosition)> = match direction {
            Direction::Forward => self.data_next_forward(prev),
            Direction::Backward => self.data_next_reverse(prev),
        };

        let flat_cand = match direction {
            Direction::Forward => self.flat_next_forward(prev),
            Direction::Backward => self.flat_next_reverse(prev),
        };

        match (data_cand, flat_cand) {
            (None, None) => None,
            (Some((bk, bv)), None) => Some((bk.clone(), bv.into_wal_position())),
            (None, Some((fk, fv))) => Some((fk, fv)),
            (Some((bk, bv)), Some((fk, fv))) => {
                let cmp = bk.as_ref().cmp(fk.as_ref());
                // Forward: smaller wins; Backward: larger wins. `data` wins ties.
                let data_wins = match direction {
                    Direction::Forward => cmp != Ordering::Greater,
                    Direction::Backward => cmp != Ordering::Less,
                };
                if data_wins {
                    Some((bk.clone(), bv.into_wal_position()))
                } else {
                    Some((fk, fv))
                }
            }
        }
    }

    /// Checkpoint variant of [`Self::next_entry`]: walks the index as of the
    /// `last_processed` frontier. Per key, the data overlay contributes its
    /// latest *processed* position (offset < `last_processed`); keys whose
    /// positions all postdate the frontier are skipped, leaving any flat entry
    /// for those keys visible. Flat is consulted unfiltered — every flat entry
    /// is below the latched frontier by the `promote_to_flat` invariant (see
    /// [`Self::get_at`]).
    pub fn next_entry_at(
        &self,
        prev: Option<Bytes>,
        reverse: bool,
        last_processed: LastProcessed,
    ) -> Option<(Bytes, WalPosition)> {
        self.next_entry_directional_at(
            prev.as_deref(),
            Direction::from_bool(reverse),
            last_processed,
        )
    }

    /// As-of-`last_processed` counterpart of [`Self::next_entry_directional`].
    /// Structurally identical, except the data side uses latest-*processed*
    /// positions: candidates come from `data_next_{forward,reverse}_at`.
    ///
    /// A flat entry shadowed by a same-key *processed* tombstone needs no
    /// pre-filter for the same reason as the non-`at` walk: a key whose latest
    /// processed position is a tombstone still has a processed position, so
    /// `data_next_*_at` surfaces it as `data_cand` and the tie-break suppresses
    /// the flat entry. A key whose positions are *all* post-frontier is skipped
    /// by `data_next_*_at` and contributes nothing, leaving any flat entry for
    /// that key visible — which is exactly the desired as-of view.
    fn next_entry_directional_at(
        &self,
        prev: Option<&[u8]>,
        direction: Direction,
        last_processed: LastProcessed,
    ) -> Option<(Bytes, WalPosition)> {
        let data_cand: Option<(Bytes, IndexWalPosition)> = match direction {
            Direction::Forward => self.data_next_forward_at(prev, last_processed),
            Direction::Backward => self.data_next_reverse_at(prev, last_processed),
        };

        let flat_cand = match direction {
            Direction::Forward => self.flat_next_forward(prev),
            Direction::Backward => self.flat_next_reverse(prev),
        };

        match (data_cand, flat_cand) {
            (None, None) => None,
            (Some((bk, bv)), None) => Some((bk, bv.into_wal_position())),
            (None, Some((fk, fv))) => Some((fk, fv)),
            (Some((bk, bv)), Some((fk, fv))) => {
                let cmp = bk.as_ref().cmp(fk.as_ref());
                // Forward: smaller wins; Backward: larger wins. `data` wins ties.
                let data_wins = match direction {
                    Direction::Forward => cmp != Ordering::Greater,
                    Direction::Backward => cmp != Ordering::Less,
                };
                if data_wins {
                    Some((bk, bv.into_wal_position()))
                } else {
                    Some((fk, fv))
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        // Note: may overcount when a key is present in both flat and data. With
        // the `promote_to_flat` invariant this happens whenever a still-unprocessed
        // write shadows an existing flat entry for the same key — the
        // overlap persists until the data-side entry becomes processed and a
        // later promote folds it into flat (or until the entry is removed).
        self.flat_len() + self.data_unique_key_count()
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

    /// Merge `flat` and `data` into the table's logical view: one entry per
    /// unique key, with the `data` overlay shadowing `flat` on ties (matching
    /// [`Self::get`] priority). Keys are yielded in ascending order. Tombstones
    /// are included as-is — callers apply their own kind filter.
    ///
    /// Both inputs are strictly ascending and individually unique (`flat` by
    /// construction, `data` via latest-per-key), so this is an O(N+M) merge.
    /// Flat keys are zero-copy subslices of `self.flat` (`slice_to_bytes`); data
    /// keys are `Bytes` clones (owner refcount bump). No per-entry heap copy of
    /// the key bytes is made, and the equal-key case folds the shadowing in
    /// directly rather than via a separate `data_contains_key` membership probe.
    fn iter_combined(&self) -> impl Iterator<Item = (Bytes, IndexWalPosition)> + '_ {
        let flat = &self.flat;
        let mut flat_it = FlatIter::new(flat, self.key_size).peekable();
        let mut data_it = self.data_iter_latest().peekable();
        std::iter::from_fn(move || {
            // Resolve to an owned `Ordering` first so the peek borrows end
            // before we advance either iterator below.
            let cmp = match (flat_it.peek(), data_it.peek()) {
                (None, None) => return None,
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some((fk, _)), Some((bk, _))) => {
                    let bk_slice: &[u8] = bk.as_ref();
                    fk.cmp(&bk_slice)
                }
            };
            match cmp {
                Ordering::Less => {
                    let (k, iwp) = flat_it.next().unwrap();
                    Some((flat.slice_to_bytes(k), iwp))
                }
                Ordering::Greater => {
                    let (k, v) = data_it.next().unwrap();
                    Some((k.clone(), *v))
                }
                // Same key: data overlay wins; drop the shadowed flat entry.
                Ordering::Equal => {
                    flat_it.next();
                    let (k, v) = data_it.next().unwrap();
                    Some((k.clone(), *v))
                }
            }
        })
    }

    fn iter_inner(
        &self,
        include_tombstones: bool,
    ) -> impl Iterator<Item = (Bytes, WalPosition)> + '_ {
        self.iter_combined().filter_map(move |(k, iwp)| {
            let pos = iwp.into_wal_position();
            (include_tombstones || pos.is_valid()).then_some((k, pos))
        })
    }

    pub fn keys(&self) -> impl Iterator<Item = Bytes> + '_ {
        self.iter_combined()
            .filter(|(_, iwp)| !iwp.is_removed())
            .map(|(k, _)| k)
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
            // Fast path: only data entries (one per unique key, latest position).
            for (key, value) in self.data_iter_latest() {
                let pos = if value.is_removed() {
                    WalPosition::INVALID
                } else {
                    value.into_update_position()
                };
                self.write_key_val(ks, out, visitor, key, pos);
            }
            return;
        }

        // Merge-sort flat and data entries; data overrides flat for the same key.
        let flat = &self.flat[..];

        // Keep a single stateful FlatIter alive for O(N) sequential traversal.
        let mut flat_iter = FlatIter::new(flat, self.key_size);
        let mut cur_flat: Option<(Vec<u8>, IndexWalPosition)> =
            flat_iter.next().map(|(k, iwp)| (k.to_vec(), iwp));
        let mut data_it = self.data_iter_latest();
        let mut cur_data: Option<(&Bytes, &IndexWalPosition)> = data_it.next();

        loop {
            let cmp = match (&cur_flat, cur_data) {
                (None, None) => break,
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some((fk, _)), Some((bk, _))) => fk.as_slice().cmp(bk.as_ref()),
            };

            let (key, iwp, advance_flat, advance_data) = match cmp {
                Ordering::Less => {
                    let (fk, fiwp) = cur_flat.take().unwrap();
                    (fk, fiwp, true, false)
                }
                Ordering::Equal => {
                    // Same key: data overrides flat; advance both.
                    cur_flat = flat_iter.next().map(|(k, iwp)| (k.to_vec(), iwp));
                    let (bk, bv) = cur_data.take().unwrap();
                    (bk.to_vec(), *bv, false, true)
                }
                Ordering::Greater => {
                    let (bk, bv) = cur_data.take().unwrap();
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
            if advance_data {
                cur_data = data_it.next();
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

    /// Build a flat-backed IndexTable from a vector of entries that the caller
    /// has already sorted by key. Used by the replay accumulator path to skip
    /// the BTreeMap on the hot insert path — replay batches per-cell writes
    /// into a HashMap, then converts them in bulk via a single sort + flat
    /// build. `data` is empty; `key_size` is `None` (varlen flat), matching
    /// what `promote_to_flat` produces from a default IndexTable.
    pub(crate) fn from_sorted_entries(entries: Vec<(Bytes, IndexWalPosition)>) -> Self {
        let dirty_count = entries.iter().filter(|(_, iwp)| !iwp.is_clean()).count();
        let flat = build_flat_bytes(entries, None);
        IndexTable {
            data: BTreeMap::new(),
            key_bytes: 0,
            flat,
            key_size: None,
            dirty_count,
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
        // A Removed tombstone (the latest entry for a key) shadows any flat
        // entry for the same key; both sides must be dropped together,
        // otherwise the shadowed flat entry survives as a stale record.
        //
        // Walk `data` latest-per-key:
        //   - Latest Clean: keep as Clean.
        //   - Latest Modified: rewrite to Clean.
        //   - Latest Removed: drop the key entirely (tombstoned).
        // Older positions for any key (set duplicates) are also discarded — the
        // logical "view" only ever exposed the latest, and clean_self is the
        // resting point where we settle to one entry per key.
        let mut tombstoned: HashSet<Bytes> = HashSet::new();
        let mut new_data: DataOverlay = BTreeMap::new();
        for (k, v) in data_latest_per_key(&self.data) {
            match v.kind {
                IndexEntryKind::Clean => {
                    new_data.insert(k.clone(), smallvec![*v]);
                }
                IndexEntryKind::Modified => {
                    new_data.insert(k.clone(), smallvec![v.as_clean_modified()]);
                }
                IndexEntryKind::Removed => {
                    tombstoned.insert(k.clone());
                }
            }
        }
        self.data = new_data;

        // Rebuild flat: convert Modified→Clean, drop Removed (in flat or shadowed by data).
        self.retain_flat(|k, mut iwp| {
            if iwp.is_removed() || tombstoned.contains(k) {
                None
            } else {
                iwp.kind = IndexEntryKind::Clean;
                Some(iwp)
            }
        });

        // All remaining entries are Clean after this call.
        let (kb, dc) = self.recount_stats();
        self.key_bytes = kb;
        self.dirty_count = dc;
        debug_assert_eq!(self.dirty_count, 0);
    }

    /// Retain only unprocessed entries (those with WAL offset >= last_processed).
    ///
    /// By the `flat` field invariant (no in-flight entries, see its doc),
    /// `flat` is cleared unconditionally. The BTreeMap still holds entries on
    /// both sides of the boundary and is filtered.
    pub fn retain_unprocessed(&mut self, last_processed: LastProcessed) {
        self.flat = Bytes::default();
        self.retain(|_, v| !last_processed.is_processed(v));
    }

    /// Returns true if this index table has any unprocessed entries.
    ///
    /// Only the data set (write buffer) needs to be checked: by the `flat`
    /// field invariant (no in-flight entries, see its doc), flat contributes
    /// no unprocessed entries against any caller-supplied `last_processed`.
    pub fn has_unprocessed(&self, last_processed: LastProcessed) -> bool {
        self.data
            .values()
            .flatten()
            .any(|v| !last_processed.is_processed(v))
    }

    /// Retain only entries with offset >= last_processed. Applied to both flat and data.
    /// `retain_flat` runs first so `retain`'s internal recount sees the final flat.
    pub fn retain_above_position(&mut self, last_processed: u64) {
        self.retain_flat(|_, iwp| (iwp.offset >= last_processed).then_some(iwp));
        self.retain(|_, v| v.offset >= last_processed);
    }

    /// Compact index by retaining keys identified by the compactor.
    /// The compactor receives a double-ended iterator over all keys and returns keys to retain.
    ///
    /// # Tombstone preservation
    ///
    /// `Removed` entries (tombstones) are never shown to the user's compactor
    /// closure and are always retained in the output. A compacted index is
    /// sometimes the deepest level (over_threshold promote → new L1, deepest),
    /// in which case the caller follows up with `clean_self()` to strip
    /// tombstones; but it is just as often a shallower level written above an
    /// existing L1 (`flusher.rs:333-354`), where the tombstones are the only
    /// thing shadowing matching keys in the level below. Stripping them here
    /// would resurrect those deeper keys — see the regression test
    /// `test_compact_with_preserves_tombstones_shadowing_l1`.
    pub fn compact_with<F>(&mut self, compactor: F)
    where
        F: FnOnce(&mut dyn DoubleEndedIterator<Item = &Bytes>) -> HashSet<Bytes>,
    {
        // Collect all valid unique keys from both flat and data in sorted order.
        let all_keys: Vec<Bytes> = self
            .iter_combined()
            .filter(|(_, iwp)| !iwp.is_removed())
            .map(|(k, _)| k)
            .collect();

        let mut iter = all_keys.iter();
        let to_retain = compactor(&mut iter);
        drop(iter);
        drop(all_keys);

        // `retain_flat` first so the subsequent `retain` recounts against the
        // final flat. Keep tombstones regardless of `to_retain` so they can
        // shadow deeper levels (see method-level doc above). Per-key `retain`
        // semantics ensure that when a key's latest is non-tombstone and not
        // retained, every position for the key is dropped — preventing an
        // older Removed tuple from surviving as an orphan shadow.
        self.retain_flat(|k, iwp| {
            if iwp.is_removed() || to_retain.contains(k) {
                Some(iwp)
            } else {
                None
            }
        });
        self.retain(|k, v| v.is_removed() || to_retain.contains(k));
    }

    /// Drop every post-frontier overlay position (offset `>= last_processed`)
    /// from the BTree, reducing the table to its as-of-`last_processed` view.
    /// Used by the flusher so an on-disk blob only holds as-of-frontier data;
    /// the mirror of [`Self::retain_unprocessed`].
    ///
    /// This is the flush-side enforcement of the unprocessed-write retention
    /// invariant — see the `crate::checkpoint` module docs.
    ///
    /// The cutoff is the overlay's alone — flat is left untouched: a relocation
    /// merge parks an as-of value at a WAL-tail offset (`>= last_processed`) in
    /// flat that re-filtering would wrongly drop. It is per position, not per
    /// key — a key may carry both a below- and a post-frontier write.
    pub fn retain_processed(&mut self, last_processed: LastProcessed) {
        // The offset filter is only sound because `data` holds exclusively
        // genuine WAL-offset writes. Disk-derived entries (notably tombstones,
        // stored with the INVALID sentinel offset u64::MAX) live in `flat` and
        // are never lifted here — see `merge_dirty`. Were such an entry present
        // its sentinel offset would read as "post-frontier" and be dropped,
        // resurrecting the value it shadows.
        //
        // The assert below cannot catch every violation: `apply_update` lifts
        // re-pointed relocated copies into `data` at genuine WAL-tail offsets
        // (>= the frontier), which this filter would also wrongly drop. That is
        // an ordering requirement on the flush path — this filter must run
        // before `RelocationUpdates::apply`, never after (see `flusher.rs`).
        debug_assert!(
            self.data
                .values()
                .flatten()
                .all(|v| v.offset() != WalPosition::INVALID.offset()),
            "disk-origin (INVALID-offset) entry found in data overlay — \
             violates the invariant that disk entries live only in flat"
        );
        // Per position, not per key — a key may carry both a below- and a
        // post-frontier write; drop only the post-frontier ones, then drop any
        // key left with no positions.
        self.data.retain(|_, positions| {
            positions.retain(|v| last_processed.is_processed(v));
            !positions.is_empty()
        });
        let (kb, dc) = self.recount_stats();
        self.key_bytes = kb;
        self.dirty_count = dc;
    }

    /// Rebase a still-loaded overlay onto a freshly flushed as-of-`last_processed`
    /// blob: adopt `as_of`'s flat as the base, keep only the post-frontier overlay
    /// writes (offset `>= last_processed`) on top.
    ///
    /// Used **only** by the relocation flush of an unloading-disabled cell
    /// (`LargeTableEntry::sync_flush`) — the single path where a cell stays loaded
    /// while relocation moves its as-of values to new WAL positions and reclaims
    /// the old frames, so the loaded overlay must be re-pointed at the surviving
    /// copies. Every other flush either unloads the cell (reads then fall through
    /// to the relocated on-disk blob) or does no WAL GC, so none of them need this.
    ///
    /// `as_of` is disk-loaded (flat-only). Post-frontier tuples are re-inserted
    /// raw, bypassing `checked_insert`'s increasing-offset assertion — a relocated
    /// flat copy can transiently sit above a post-frontier write for the same key.
    pub fn rebase_on_as_of(&mut self, as_of: IndexTable, last_processed: LastProcessed) {
        let post_frontier: Vec<(Bytes, IndexWalPosition)> = self
            .data
            .iter()
            .flat_map(|(k, positions)| {
                positions
                    .iter()
                    .filter(|v| !last_processed.is_processed(*v))
                    .map(move |v| (k.clone(), *v))
            })
            .collect();
        *self = as_of;
        for (k, iwp) in post_frontier {
            // Raw sorted insert: a relocated flat copy can transiently sit above
            // a post-frontier write for the same key, so order is not guaranteed.
            self.data_insert_sorted(k, iwp);
        }
        let (kb, dc) = self.recount_stats();
        self.key_bytes = kb;
        self.dirty_count = dc;
    }

    /// Apply given update function to a value with a given key.
    /// If the key does not exist, this function completes without calling the update function.
    /// Operates on the latest position for the key.
    pub fn apply_update(&mut self, key: &[u8], update: impl FnOnce(&mut IndexWalPosition)) {
        if let Some(latest) = self.data_get_latest(key) {
            let was_dirty = !latest.is_clean();
            let mut new_iwp = latest;
            update(&mut new_iwp);
            adjust_dirty_count(&mut self.dirty_count, was_dirty, !new_iwp.is_clean());
            // Replace the latest position (the vec's last element) with the
            // updated one, keeping the vec ordered. The update re-points to a
            // fresh, strictly-greater WAL offset, so `new_iwp` always inserts as
            // a new element rather than colliding with a surviving older
            // position — assert that, since a silent collision would drop the
            // element while `dirty_count` was already adjusted for it.
            let positions = self
                .data
                .get_mut(key)
                .expect("data_get_latest found the key");
            positions.pop();
            match positions.binary_search(&new_iwp) {
                Err(pos) => positions.insert(pos, new_iwp),
                Ok(_) => debug_assert!(
                    false,
                    "apply_update: updated position collided with an existing position for the key"
                ),
            }
            debug_assert!(!positions.is_empty());
            return;
        }
        // Also check the flat buffer (present when promote_to_flat has been called).
        if let Some(original) = self.flat_binary_search(key) {
            let mut iwp = original;
            update(&mut iwp);
            // Only lift the entry into the data overlay when the update actually
            // re-pointed it. A no-op update must leave the entry in flat: lifting
            // it would copy a disk-derived position into `data` (e.g. a
            // tombstone's INVALID sentinel offset, which relocation's closure
            // never rewrites), violating the invariant that disk entries live
            // only in flat. Lifting an unchanged entry is a no-op for reads
            // anyway — the data copy would just duplicate the flat value.
            if iwp == original {
                return;
            }
            let was_dirty = !original.is_clean();
            adjust_dirty_count(&mut self.dirty_count, was_dirty, !iwp.is_clean());
            let key_bytes = Bytes::from(key.to_vec());
            self.key_bytes += key_bytes.len();
            // The key is absent from `data` (we reached the flat branch), so the
            // updated position shadows the stale flat entry on reads as a fresh
            // single-position entry.
            self.data.insert(key_bytes, smallvec![iwp]);
        }
    }

    /// Total bytes occupied by keys in this table (unique data keys + full flat buffer). Always O(1).
    pub fn total_key_bytes(&self) -> usize {
        self.key_bytes + self.flat.len()
    }

    /// Bytes occupied by the flat buffer. Always O(1).
    pub fn flat_key_bytes(&self) -> usize {
        self.flat.len()
    }

    /// Promote BTreeMap entries below `last_processed` into the flat array,
    /// merging with any existing flat content. Entries at or above
    /// `last_processed` are unprocessed (still in flight in the WAL) and remain
    /// in the BTreeMap. BTreeMap entries override flat entries for the same key.
    ///
    /// This is the only path that grows `flat`, so it establishes the invariant
    /// that **every flat entry has WAL offset < the `last_processed` snapshot
    /// observed at its most recent promote**. Callers downstream (`has_unprocessed`,
    /// `retain_unprocessed`, `clean_self` tombstone stripping) rely on this:
    /// because `last_processed` is monotonically non-decreasing, any later call
    /// observing a `LastProcessed` value L sees all flat entries as processed
    /// (offset < L).
    ///
    /// Pass `LastProcessed::new(u64::MAX)` to promote unconditionally (used by
    /// the test-only `promote_to_flat_force`).
    #[cfg(not(test))]
    const PROMOTE_THRESHOLD: usize = 128;
    #[cfg(test)]
    const PROMOTE_THRESHOLD: usize = 0;

    /// A cell with few unique keys never crosses `PROMOTE_THRESHOLD`, yet its
    /// per-key position lists still grow by one entry per write until a
    /// promote or flush drains them — and `unmerge_flushed` cost grows
    /// quadratically with that backlog (see
    /// `repro_unmerge_flushed_quadratic`). Promote whenever the positions a
    /// promote would drain cross this bound, so the overlay stays small
    /// regardless of key cardinality.
    const PROMOTE_POSITIONS_THRESHOLD: usize = 1024;

    /// Whether [`Self::promote_to_flat`] would drain this table. Lets
    /// `promote_flat_job` skip the deep clone of tables a promote would leave
    /// unchanged.
    pub fn should_promote_to_flat(&self, last_processed: LastProcessed) -> bool {
        self.should_promote(Self::PROMOTE_THRESHOLD, last_processed)
    }

    /// The promote trigger: unique-key count above `key_threshold`, or a
    /// drainable position backlog above `PROMOTE_POSITIONS_THRESHOLD`.
    ///
    /// Re-inserting the same key does not inflate the key trigger (one map
    /// entry per key); the backlog such a hot key builds up is what the
    /// position trigger catches. The backlog scan only runs under the key
    /// threshold, so it stays short.
    fn should_promote(&self, key_threshold: usize, last_processed: LastProcessed) -> bool {
        if self.data_unique_key_count() > key_threshold {
            return true;
        }
        self.drainable_position_count(last_processed) > Self::PROMOTE_POSITIONS_THRESHOLD
    }

    /// Positions a promote would drain from `data`: a key with an in-flight
    /// latest position contributes 0, and one with a processed latest
    /// contributes its whole (ascending, hence fully processed) list.
    ///
    /// Must stay in lockstep with the drain itself, which is all-or-nothing
    /// per key on the latest position in both of its halves: `merge_walk`'s
    /// data-side filter (`data_latest_per_key(..)` + `is_processed` in
    /// flat.rs) decides what enters flat, and `drop_data_keys` in
    /// `promote_to_flat_inner` drops keys by the same latest-position test.
    /// Counting anything else (e.g. per-position) would fire promotes that
    /// drain less than counted.
    fn drainable_position_count(&self, last_processed: LastProcessed) -> usize {
        self.data
            .values()
            .filter(|positions| {
                let latest = positions.last().expect("data positions are never empty");
                last_processed.is_processed(latest)
            })
            .map(|positions| positions.len())
            .sum()
    }

    /// Returns `true` if the flat buffer was updated, `false` if nothing changed.
    ///
    /// Folds only processed overlay positions (offset `< last_processed`) into
    /// flat; unprocessed ones stay in the BTree. This is the promote-side
    /// enforcement of the unprocessed-write retention invariant — see the
    /// `crate::checkpoint` module docs.
    pub fn promote_to_flat(&mut self, last_processed: LastProcessed) -> bool {
        self.promote_to_flat_inner(Self::PROMOTE_THRESHOLD, last_processed)
    }

    /// Test-only variant that bypasses `PROMOTE_THRESHOLD` but still respects
    /// the `last_processed` filter — the threshold check is the only
    /// difference from `promote_to_flat`. Lets concurrent tests reliably open
    /// the `insert → promote → remove → FlushLoaded` window without needing
    /// 128+ keys per cell, while exercising the same partition behavior
    /// `promote_flat_job` uses in production. Callers must capture
    /// `last_processed` the same way prod does (see `promote_flat_job`).
    #[cfg(any(test, feature = "test-utils"))]
    pub fn promote_to_flat_force(&mut self, last_processed: LastProcessed) -> bool {
        self.promote_to_flat_inner(0, last_processed)
    }

    fn promote_to_flat_inner(&mut self, threshold: usize, last_processed: LastProcessed) -> bool {
        // Cheap cap on entry (see `should_promote` for the two triggers). When
        // the key trigger fires but every key's latest entry happens to be
        // unprocessed, the merge below is a no-op (rare; not worth a separate
        // scan to detect).
        if !self.should_promote(threshold, last_processed) {
            return false;
        }
        self.promote_to_flat_unconditionally(last_processed);
        true
    }

    /// The drain half of a promote, without re-evaluating the trigger. For
    /// callers that already checked [`Self::should_promote_to_flat`] on the
    /// same content and `last_processed` (`promote_flat_job` gates before its
    /// deep clone); calling it when nothing is drainable is sound but
    /// rebuilds `flat` for no gain.
    pub fn promote_to_flat_unconditionally(&mut self, last_processed: LastProcessed) {
        let (new_flat, flat_dirty) =
            merge_into_flat(&self.flat, self.key_size, &self.data, last_processed);
        self.flat = new_flat;
        // Per-key: if the latest entry was promoted (its offset < last_processed),
        // discard ALL positions for that key from data. Otherwise (latest is
        // unprocessed), keep every position for that key untouched.
        //
        // `merge_into_flat` already returned `flat_dirty` for the new flat, so
        // recount only the data side rather than re-scanning the (now larger)
        // flat via the full `retain`/`recount_stats` path.
        self.drop_data_keys(|_, v| !last_processed.is_processed(v));
        let (data_key_bytes, data_dirty) = self.recount_data_stats();
        self.key_bytes = data_key_bytes;
        self.dirty_count = flat_dirty + data_dirty;
    }

    // ---------------------------------------------------------------------------
    // Private data-overlay helpers
    //
    // The `data` map keys each entry once and keeps its positions ascending by
    // IWP, so the latest position is the vec's last element and a key lookup is a
    // single `get`. Helpers that expose the "one logical entry per key" view the
    // rest of the impl assumes read that last element.
    // ---------------------------------------------------------------------------

    /// Latest `IndexWalPosition` recorded in `data` for `key`, or `None` if no
    /// position exists for that key. Single map descent; `Bytes: Borrow<[u8]>`
    /// means the borrowed-slice lookup needs no key allocation.
    fn data_get_latest(&self, key: &[u8]) -> Option<IndexWalPosition> {
        self.data
            .get(key)
            .map(|positions| *positions.last().expect("data positions are never empty"))
    }

    /// Insert `iwp` into `key`'s position list, preserving ascending IWP order
    /// and set semantics (an identical position is not duplicated). Creates the
    /// key's entry if absent. Used by merge/rebase paths where the incoming
    /// position is not guaranteed to be the largest for the key (the hot insert
    /// path appends directly in `checked_insert`).
    fn data_insert_sorted(&mut self, key: Bytes, iwp: IndexWalPosition) {
        let positions = self.data.entry(key).or_default();
        if let Err(pos) = positions.binary_search(&iwp) {
            positions.insert(pos, iwp);
        }
    }

    /// Remove the first position for `key` (lowest IWP) matching `pred`, dropping
    /// the key entirely if that empties its list. Returns whether a position was
    /// removed.
    fn data_remove_matching(
        &mut self,
        key: &[u8],
        pred: impl Fn(&IndexWalPosition) -> bool,
    ) -> bool {
        let Some(positions) = self.data.get_mut(key) else {
            return false;
        };
        let Some(idx) = positions.iter().position(pred) else {
            return false;
        };
        positions.remove(idx);
        if positions.is_empty() {
            self.data.remove(key);
        }
        true
    }

    /// Latest `IndexWalPosition` recorded in `data` for `key` that is processed
    /// relative to `last_processed` (offset < `last_processed`), or `None` if
    /// the key has no processed position there.
    ///
    /// Positions ascend by `(offset, ..)` and `LastProcessed::is_processed`
    /// depends only on `offset`, so scanning from the back yields the latest
    /// processed position first.
    fn data_get_latest_processed(
        &self,
        key: &[u8],
        last_processed: LastProcessed,
    ) -> Option<IndexWalPosition> {
        self.data.get(key).and_then(|positions| {
            positions
                .iter()
                .rev()
                .find(|v| last_processed.is_processed(*v))
                .copied()
        })
    }

    fn data_contains_key(&self, key: &[u8]) -> bool {
        self.data.contains_key(key)
    }

    /// Iterator over `data` yielding one entry per unique key (the latest).
    fn data_iter_latest(&self) -> impl Iterator<Item = (&Bytes, &IndexWalPosition)> + '_ {
        data_latest_per_key(&self.data)
    }

    /// Count of unique keys present in `data` (one map entry per key).
    fn data_unique_key_count(&self) -> usize {
        self.data.len()
    }

    /// Forward iteration on `data`: smallest key strictly greater than
    /// `prev` (or smallest key if `prev` is `None`), with that key's
    /// *latest* `IndexWalPosition` (its last, largest position).
    fn data_next_forward(&self, prev: Option<&[u8]>) -> Option<(&Bytes, &IndexWalPosition)> {
        let start = match prev {
            Some(p) => Bound::Excluded(p),
            None => Bound::Unbounded,
        };
        self.data
            .range::<[u8], _>((start, Bound::Unbounded))
            .next()
            .map(|(k, positions)| (k, positions.last().expect("data positions are never empty")))
    }

    /// Backward iteration on `data`: largest key strictly less than `prev`
    /// (or largest key if `prev` is `None`), with that key's *latest*
    /// `IndexWalPosition`.
    fn data_next_reverse(&self, prev: Option<&[u8]>) -> Option<(&Bytes, &IndexWalPosition)> {
        let end = match prev {
            Some(p) => Bound::Excluded(p),
            None => Bound::Unbounded,
        };
        self.data
            .range::<[u8], _>((Bound::Unbounded, end))
            .next_back()
            .map(|(k, positions)| (k, positions.last().expect("data positions are never empty")))
    }

    /// As-of-`last_processed` forward step over `data`: the smallest key
    /// strictly greater than `prev` that has at least one processed position,
    /// paired with that key's latest processed `IndexWalPosition`. Keys whose
    /// positions all postdate the frontier are skipped over.
    ///
    /// Positions ascend by `(offset, ..)`, so scanning a key's list from the
    /// back yields the latest processed position first.
    fn data_next_forward_at(
        &self,
        prev: Option<&[u8]>,
        last_processed: LastProcessed,
    ) -> Option<(Bytes, IndexWalPosition)> {
        let start = match prev {
            Some(p) => Bound::Excluded(p),
            None => Bound::Unbounded,
        };
        self.data
            .range::<[u8], _>((start, Bound::Unbounded))
            .find_map(|(key, positions)| {
                positions
                    .iter()
                    .rev()
                    .find(|v| last_processed.is_processed(*v))
                    .map(|v| (key.clone(), *v))
            })
    }

    /// As-of-`last_processed` backward counterpart of [`Self::data_next_forward_at`].
    fn data_next_reverse_at(
        &self,
        prev: Option<&[u8]>,
        last_processed: LastProcessed,
    ) -> Option<(Bytes, IndexWalPosition)> {
        let end = match prev {
            Some(p) => Bound::Excluded(p),
            None => Bound::Unbounded,
        };
        self.data
            .range::<[u8], _>((Bound::Unbounded, end))
            .rev()
            .find_map(|(key, positions)| {
                positions
                    .iter()
                    .rev()
                    .find(|v| last_processed.is_processed(*v))
                    .map(|v| (key.clone(), *v))
            })
    }

    /// Single-pass recount of `(data_key_bytes, dirty_count)`. Both counters
    /// are "unique-key" — a key contributes once regardless of how many
    /// positions it carries. Used after bulk mutations that touch both
    /// `flat` and `data` in ways too complex to track inline.
    fn recount_stats(&self) -> (usize, usize) {
        let flat_dirty = FlatIter::new(&self.flat, self.key_size)
            .filter(|(_, iwp)| !iwp.is_clean())
            .count();
        let (key_bytes, data_dirty) = self.recount_data_stats();
        (key_bytes, flat_dirty + data_dirty)
    }

    /// Single-pass recount of the data side only: `(data_key_bytes,
    /// data_dirty)`. The flat-side dirty count is excluded, so callers that
    /// already hold an accurate flat dirty count (e.g. the value
    /// `merge_into_flat` returns) can avoid a redundant full flat re-scan and
    /// combine it themselves: `dirty_count = flat_dirty + data_dirty`.
    fn recount_data_stats(&self) -> (usize, usize) {
        let mut key_bytes = 0usize;
        let mut data_dirty = 0usize;
        for (k, v) in self.data_iter_latest() {
            key_bytes += k.len();
            if !v.is_clean() {
                data_dirty += 1;
            }
        }
        (key_bytes, data_dirty)
    }

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

    /// Test helper: rewrite every `Modified` position in `data` to `Clean`.
    #[cfg(test)]
    fn demote_data_modified_to_clean(&mut self) {
        for positions in self.data.values_mut() {
            for v in positions.iter_mut() {
                if v.kind == IndexEntryKind::Modified {
                    *v = v.as_clean_modified();
                }
            }
        }
        let (kb, dc) = self.recount_stats();
        self.key_bytes = kb;
        self.dirty_count = dc;
    }

    #[cfg(test)]
    pub fn into_data(self) -> BTreeMap<Bytes, WalPosition> {
        // Flat entries have lower priority; data entries override them. Each
        // data key's latest position (its last) is the live value.
        let mut result: BTreeMap<Bytes, WalPosition> = FlatIter::new(&self.flat, self.key_size)
            .map(|(k, iwp)| (Bytes::from(k.to_vec()), iwp.into_wal_position()))
            .collect();
        for (k, positions) in self.data {
            let latest = positions.last().expect("data positions are never empty");
            result.insert(k, latest.into_wal_position());
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

    pub(crate) fn new_modified(w: WalPosition) -> Self {
        debug_assert_ne!(w, WalPosition::INVALID);
        Self {
            offset: w.offset(),
            len: w.frame_len_u32(),
            kind: IndexEntryKind::Modified,
        }
    }

    pub(crate) fn new_removed(w: WalPosition) -> Self {
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
    use crate::key_shape::{KeyIndexing, KeyShape, KeySpace, KeyType};

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
        let data: DataOverlay = data_overlay([
            (vec![1, 2, 3].into(), iwp(22)),
            (vec![2, 5].into(), iwp(23)),
            (vec![].into(), iwp(25)),
        ]);
        let key_bytes: usize = data.keys().map(|k| k.len()).sum();
        let index = IndexTable {
            data,
            key_bytes,
            flat: Default::default(),
            key_size: None,
            dirty_count: 0,
        };
        let shape = KeyShape::new_single_config_indexing(
            KeyIndexing::variable_length(),
            1,
            KeyType::uniform(1),
            Default::default(),
        );
        let ks = KeySpace::first();
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

    /// Build a `data` overlay from `(key, position)` tuples, grouping positions
    /// by key and keeping each key's list ascending — the in-memory shape the
    /// production write path maintains.
    fn data_overlay(entries: impl IntoIterator<Item = (Bytes, IndexWalPosition)>) -> DataOverlay {
        let mut map: DataOverlay = BTreeMap::new();
        for (k, v) in entries {
            map.entry(k).or_default().push(v);
        }
        for positions in map.values_mut() {
            positions.sort();
        }
        map
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

    /// Repro for the epoch-boundary stall on hot few-key cells (e.g. a
    /// ~5-key `disable_unload` watermarks cell): the overlay accumulates one
    /// position per write, and `unmerge_flushed` then removes the flushed
    /// snapshot's positions one at a time from the front of each key's list
    /// (`SmallVec::remove(0)` in `data_remove_matching`) — O(m²) total
    /// memmove for a per-key backlog of m.
    ///
    /// Prints timings for doubling backlogs; the time grows ~4x per doubling.
    /// Run with:
    /// `cargo test -p tidehunter repro_unmerge_flushed_quadratic -- --ignored --nocapture`
    /// Production value of `PROMOTE_THRESHOLD`, which cfg(test) rebinds to 0;
    /// the trigger tests pass it explicitly to exercise the production gate.
    const PROD_KEY_THRESHOLD: usize = 128;

    /// Insert one position per key at consecutive offsets, advancing `offset`.
    fn insert_round(table: &mut IndexTable, keys: &[Bytes], offset: &mut u64) {
        for key in keys {
            table.insert(key.clone(), WalPosition::test_value(*offset));
            *offset += 1;
        }
    }

    #[test]
    #[ignore = "manual timing repro of the quadratic unmerge backlog"]
    fn repro_unmerge_flushed_quadratic() {
        const KEYS: u32 = 5;
        let keys: Vec<Bytes> = (0..KEYS).map(|i| i.to_be_bytes().to_vec().into()).collect();
        for backlog_per_key in [25_000u64, 50_000, 100_000] {
            let mut live = IndexTable::default();
            let mut offset = 1u64;
            for _ in 0..backlog_per_key {
                insert_round(&mut live, &keys, &mut offset);
            }
            // The flusher snapshots the overlay here (`FlushKind::Flush { dirty }`).
            let flushed = live.clone();
            let frontier = LastProcessed::new_test(offset);
            // Writes that land while the flush is in flight force the
            // `unmerge_flushed` completion path over the whole backlog.
            insert_round(&mut live, &keys, &mut offset);
            let start = std::time::Instant::now();
            live.unmerge_flushed(&flushed, frontier);
            let elapsed = start.elapsed();
            // Only the in-flight write survives for each key.
            assert_eq!(live.len(), KEYS as usize);
            assert!(live.data.values().all(|p| p.len() == 1));
            println!(
                "backlog {backlog_per_key} positions x {KEYS} keys: unmerge_flushed took {elapsed:?}"
            );
        }
    }

    /// Regression test for the position-backlog promote trigger: a hot cell
    /// with a handful of unique keys (far below `PROD_KEY_THRESHOLD`) must
    /// still promote once its drainable positions cross
    /// `PROMOTE_POSITIONS_THRESHOLD`, or the backlog grows one entry per
    /// write until a flush — and `unmerge_flushed` is quadratic in it.
    #[test]
    fn test_should_promote_on_position_backlog() {
        let keys: Vec<Bytes> = (0..3u32).map(|i| i.to_be_bytes().to_vec().into()).collect();
        let mut table = IndexTable::default();
        let mut offset = 1u64;
        // One position per key: neither trigger fires under the production
        // key threshold.
        insert_round(&mut table, &keys, &mut offset);
        assert!(!table.should_promote(PROD_KEY_THRESHOLD, LastProcessed::new_test(offset)));

        // Grow the backlog past PROMOTE_POSITIONS_THRESHOLD total positions,
        // tallying inserts directly rather than via the function under test.
        while (offset - 1) as usize <= IndexTable::PROMOTE_POSITIONS_THRESHOLD {
            insert_round(&mut table, &keys, &mut offset);
        }
        let frontier = LastProcessed::new_test(offset);
        // Every inserted position is below the frontier, so the count must
        // match the ground-truth insert tally.
        assert_eq!(
            table.drainable_position_count(frontier),
            (offset - 1) as usize
        );
        // Nothing is drainable while every position is unprocessed.
        assert!(!table.should_promote(PROD_KEY_THRESHOLD, LastProcessed::new_test(1)));
        // Past the frontier, the position trigger fires despite 3 unique keys.
        assert!(table.should_promote(PROD_KEY_THRESHOLD, frontier));

        // The promote drains the backlog to one flat entry per key, and the
        // trigger disarms.
        assert!(table.promote_to_flat_inner(PROD_KEY_THRESHOLD, frontier));
        assert!(table.data.is_empty());
        assert_eq!(table.flat_len(), keys.len());
        assert!(!table.should_promote(PROD_KEY_THRESHOLD, frontier));
    }

    /// A key whose latest position is at/above the frontier is not drainable
    /// (`merge_into_flat` skips it whole), so it neither counts toward the
    /// position trigger nor loses positions when another key's backlog
    /// promotes.
    #[test]
    fn test_should_promote_skips_undrainable_keys() {
        let hot: Bytes = vec![1].into();
        let inflight: Bytes = vec![2].into();
        let mut table = IndexTable::default();
        let mut offset = 1u64;
        for _ in 0..=IndexTable::PROMOTE_POSITIONS_THRESHOLD {
            table.insert(hot.clone(), WalPosition::test_value(offset));
            offset += 1;
        }
        let frontier = LastProcessed::new_test(offset);
        table.insert(inflight.clone(), WalPosition::test_value(offset));
        // `inflight`'s only position sits at the frontier — undrainable; the
        // `hot` backlog alone trips the position trigger.
        assert_eq!(
            table.drainable_position_count(frontier),
            IndexTable::PROMOTE_POSITIONS_THRESHOLD + 1
        );
        assert!(table.should_promote(PROD_KEY_THRESHOLD, frontier));
        assert!(table.promote_to_flat_inner(PROD_KEY_THRESHOLD, frontier));
        // `hot` drained to a single flat entry; `inflight` stays in data.
        assert_eq!(table.flat_len(), 1);
        assert_eq!(table.data_unique_key_count(), 1);
        assert!(table.data.contains_key(&inflight));
    }

    #[test]
    fn test_promote_to_flat_retains_unprocessed() {
        // Invariant: `promote_to_flat` only drains entries strictly below
        // `last_processed`; unprocessed entries stay in the BTreeMap so that
        // `clear_after_flush` (which assumes flat is fully processed) does not
        // observe them in flat. Without this the previously-fixed dirty-overlay
        // loss can resurface.
        let mut index = IndexTable::default();
        index.insert(vec![1].into(), WalPosition::test_value(100));
        index.insert(vec![2].into(), WalPosition::test_value(200));
        index.insert(vec![3].into(), WalPosition::test_value(500));

        // last_processed = 300: 100 and 200 are processed and promote into
        // flat; 500 is unprocessed and stays in the BTreeMap.
        index.promote_to_flat(LastProcessed::new(300));

        assert_eq!(index.flat_len(), 2);
        assert_eq!(index.data.len(), 1);
        assert_eq!(index.data_get_latest(&[3]).map(|v| v.offset), Some(500));

        // Retaining unprocessed against the same threshold clears flat
        // (everything in it is processed) and keeps the BTreeMap entry.
        index.retain_unprocessed(LastProcessed::new(300));
        assert!(index.flat.is_empty());
        assert_eq!(index.get(&[1]), None);
        assert_eq!(index.get(&[2]), None);
        assert_eq!(index.get(&[3]), Some(WalPosition::test_value(500)));
    }

    #[test]
    fn test_promote_to_flat_equal_key_unprocessed_keeps_flat() {
        // Coverage for `merge_walk`'s Equal-key handling under the filter: an
        // unprocessed BTreeMap write that shadows an existing flat entry must
        // (1) be skipped from the merge (the unprocessed value cannot be
        // promoted) and (2) leave the existing flat entry in place. The
        // BTreeMap entry stays as the in-memory shadow and read priority.
        let mut index = IndexTable::default();
        index.insert(vec![7].into(), WalPosition::test_value(100));
        // First promote: K@100 lands in flat.
        index.promote_to_flat(LastProcessed::new(200));
        assert_eq!(index.flat_len(), 1);
        assert!(index.data.is_empty());

        // Overwrite K with an unprocessed position (offset 500 > threshold 300).
        index.insert(vec![7].into(), WalPosition::test_value(500));
        assert_eq!(index.data.len(), 1);

        // Second promote with threshold 300: K@500 is unprocessed, so the merge
        // skips it and the existing flat K@100 stays put. K@500 remains in the
        // BTreeMap.
        index.promote_to_flat(LastProcessed::new(300));
        assert_eq!(index.flat_len(), 1);
        assert_eq!(index.data.len(), 1);
        assert_eq!(index.data_get_latest(&[7]).map(|v| v.offset), Some(500));
        // Read returns the BTreeMap value (newer write shadows flat).
        assert_eq!(index.get(&[7]), Some(WalPosition::test_value(500)));

        // Once last_processed catches up past 500, a follow-up promote folds
        // K@500 into flat, overwriting the stale K@100 entry.
        index.promote_to_flat(LastProcessed::new(600));
        assert_eq!(index.flat_len(), 1);
        assert!(index.data.is_empty());
        assert_eq!(index.get(&[7]), Some(WalPosition::test_value(500)));
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
        table.demote_data_modified_to_clean();

        table.promote_to_flat(LastProcessed::new(u64::MAX));

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
        table.demote_data_modified_to_clean();
        // First promotion: flat gets [1, 2].
        table.promote_to_flat(LastProcessed::new(u64::MAX));

        // Now remove key [1] (goes to BTreeMap as Removed).
        table.remove(vec![1].into(), WalPosition::test_value(10));
        assert_eq!(table.get(&[1]), Some(WalPosition::INVALID)); // deleted

        // Add a new key.
        table.insert(vec![3].into(), WalPosition::test_value(5));
        table.demote_data_modified_to_clean();

        // Second promotion: flat has [1]=Removed, [2]=Clean, [3]=Clean — all 3 entries.
        table.promote_to_flat(LastProcessed::new(u64::MAX));
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
        table.demote_data_modified_to_clean();
        table.promote_to_flat(LastProcessed::new(u64::MAX));

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
        table.demote_data_modified_to_clean();

        table.key_size = Some(4);
        table.promote_to_flat(LastProcessed::new(u64::MAX));

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
        table.demote_data_modified_to_clean();
        table.key_size = Some(4);
        table.promote_to_flat(LastProcessed::new(u64::MAX));

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
        table.demote_data_modified_to_clean();
        table.key_size = Some(4);
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        assert_eq!(table.flat_len(), 2);

        // Remove k4(10); tombstone stays in BTreeMap.
        table.remove(k4(10), WalPosition::test_value(5));
        assert_eq!(table.get(&k4(10)), Some(WalPosition::INVALID));

        // Add k4(30) and promote again.
        table.insert(k4(30), WalPosition::test_value(6));
        table.demote_data_modified_to_clean();
        table.key_size = Some(4);
        table.promote_to_flat(LastProcessed::new(u64::MAX));

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
    fn test_next_entry_data_tombstone_shadows_live_flat() {
        // A live flat entry shadowed by a same-key `data` tombstone, before the
        // remove is re-promoted into flat. next_entry must surface the tombstone
        // (INVALID) and never the stale live flat position, in both directions.
        // This is the case the per-flat-candidate `data_get_latest` pre-filter
        // used to cover; the data-wins-ties merge covers it on its own.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(1));
        table.insert(vec![2].into(), WalPosition::test_value(2));
        table.insert(vec![3].into(), WalPosition::test_value(3));
        table.demote_data_modified_to_clean();
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        assert!(table.data.is_empty());
        assert_eq!(table.flat_len(), 3);

        // Tombstone [2] in `data`; flat still holds the live [2]@2 position.
        table.remove(vec![2].into(), WalPosition::test_value(10));
        assert_eq!(table.get(&[2]), Some(WalPosition::INVALID));

        // Forward: [1] live, then the [2] tombstone (INVALID, not [2]@2), then [3].
        let n = table.next_entry(Some(vec![1].into()), false).unwrap();
        assert_eq!(n, (Bytes::from(vec![2]), WalPosition::INVALID));
        let n = table.next_entry(Some(vec![2].into()), false).unwrap();
        assert_eq!(n, (Bytes::from(vec![3]), WalPosition::test_value(3)));

        // Reverse: [3] live, then the [2] tombstone, then [1].
        let n = table.next_entry(Some(vec![3].into()), true).unwrap();
        assert_eq!(n, (Bytes::from(vec![2]), WalPosition::INVALID));
        let n = table.next_entry(Some(vec![2].into()), true).unwrap();
        assert_eq!(n, (Bytes::from(vec![1]), WalPosition::test_value(1)));
    }

    #[test]
    fn test_next_entry_at_remove_processed_vs_post_frontier() {
        // As-of view of a live flat [2] with a `data` remove layered on top. The
        // remove only shadows the flat entry once it is processed (offset below
        // the frontier); a post-frontier remove contributes nothing and the live
        // flat entry stays visible.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(1));
        table.insert(vec![2].into(), WalPosition::test_value(2));
        table.insert(vec![3].into(), WalPosition::test_value(3));
        table.demote_data_modified_to_clean();
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        table.remove(vec![2].into(), WalPosition::test_value(100));

        // Frontier above the remove@100: tombstone is processed, shadows flat [2].
        let lp = LastProcessed::new_test(200);
        let n = table
            .next_entry_at(Some(vec![1].into()), false, lp)
            .unwrap();
        assert_eq!(n, (Bytes::from(vec![2]), WalPosition::INVALID));
        let n = table.next_entry_at(Some(vec![3].into()), true, lp).unwrap();
        assert_eq!(n, (Bytes::from(vec![2]), WalPosition::INVALID));

        // Frontier below the remove@100: tombstone is post-frontier and skipped,
        // so the live flat [2]@2 is the as-of view.
        let lp = LastProcessed::new_test(50);
        let n = table
            .next_entry_at(Some(vec![1].into()), false, lp)
            .unwrap();
        assert_eq!(n, (Bytes::from(vec![2]), WalPosition::test_value(2)));
        let n = table.next_entry_at(Some(vec![3].into()), true, lp).unwrap();
        assert_eq!(n, (Bytes::from(vec![2]), WalPosition::test_value(2)));
    }

    #[test]
    fn test_next_entry_consecutive_data_tombstones_over_flat() {
        // Several adjacent flat keys each shadowed by a same-key `data`
        // tombstone. The removed loop skipped all of them in a single call; the
        // tie-break merge surfaces exactly one tombstone (INVALID) per scan
        // step. Both must yield the same sequence: [1] live, [2]/[3]/[4]
        // INVALID, [5] live.
        let mut table = IndexTable::default();
        for k in 1u8..=5 {
            table.insert(vec![k].into(), WalPosition::test_value(k as u64));
        }
        table.demote_data_modified_to_clean();
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        assert!(table.data.is_empty());
        table.remove(vec![2].into(), WalPosition::test_value(10));
        table.remove(vec![3].into(), WalPosition::test_value(11));
        table.remove(vec![4].into(), WalPosition::test_value(12));

        let mut prev: Option<Bytes> = None;
        let mut seen = Vec::new();
        while let Some((k, pos)) = table.next_entry(prev.clone(), false) {
            seen.push((k.as_ref().to_vec(), pos));
            prev = Some(k);
        }
        assert_eq!(
            seen,
            vec![
                (vec![1], WalPosition::test_value(1)),
                (vec![2], WalPosition::INVALID),
                (vec![3], WalPosition::INVALID),
                (vec![4], WalPosition::INVALID),
                (vec![5], WalPosition::test_value(5)),
            ]
        );
    }

    #[test]
    fn test_next_entry_data_tombstone_with_other_live_data_key() {
        // The shadowing tombstone is NOT the nearest data candidate: a smaller
        // live data key wins the comparison first (non-tie ordering path), and
        // only at its own step does the tombstone tie with — and shadow — the
        // live flat entry. The stale flat [5]@5 must never surface.
        let mut table = IndexTable::default();
        table.insert(vec![5].into(), WalPosition::test_value(5));
        table.demote_data_modified_to_clean();
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        assert_eq!(table.flat_len(), 1);
        // Live data key [3] (< 5) and a tombstone shadowing flat [5].
        table.insert(vec![3].into(), WalPosition::test_value(20));
        table.remove(vec![5].into(), WalPosition::test_value(21));

        // Forward: live [3]@20 wins by ordering, then [5] tombstone (INVALID).
        let n = table.next_entry(None, false).unwrap();
        assert_eq!(n, (Bytes::from(vec![3]), WalPosition::test_value(20)));
        let n = table.next_entry(Some(vec![3].into()), false).unwrap();
        assert_eq!(n, (Bytes::from(vec![5]), WalPosition::INVALID));
        assert!(table.next_entry(Some(vec![5].into()), false).is_none());

        // Reverse: [5] tombstone first, then live [3]@20.
        let n = table.next_entry(None, true).unwrap();
        assert_eq!(n, (Bytes::from(vec![5]), WalPosition::INVALID));
        let n = table.next_entry(Some(vec![5].into()), true).unwrap();
        assert_eq!(n, (Bytes::from(vec![3]), WalPosition::test_value(20)));
    }

    #[test]
    fn test_next_entry_at_processed_live_over_flat_ignores_post_frontier_tombstone() {
        // Key [2] in `data` carries a processed live position (@10) and a later
        // post-frontier tombstone (@100), layered over a live flat [2]@2.
        // As-of the frontier between them, the latest *processed* position is
        // the live @10 — it shadows flat [2]@2 and the tombstone does not count.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(1));
        table.insert(vec![2].into(), WalPosition::test_value(2));
        table.insert(vec![3].into(), WalPosition::test_value(3));
        table.demote_data_modified_to_clean();
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        table.insert(vec![2].into(), WalPosition::test_value(10));
        table.remove(vec![2].into(), WalPosition::test_value(100));

        // Frontier=50: latest processed at [2] is the live @10 (not flat @2,
        // not the post-frontier tombstone @100).
        let lp = LastProcessed::new_test(50);
        let n = table
            .next_entry_at(Some(vec![1].into()), false, lp)
            .unwrap();
        assert_eq!(n, (Bytes::from(vec![2]), WalPosition::test_value(10)));
        let n = table.next_entry_at(Some(vec![3].into()), true, lp).unwrap();
        assert_eq!(n, (Bytes::from(vec![2]), WalPosition::test_value(10)));

        // Frontier=200: now the tombstone @100 is processed and is latest →
        // [2] is shadowed (INVALID).
        let lp = LastProcessed::new_test(200);
        let n = table
            .next_entry_at(Some(vec![1].into()), false, lp)
            .unwrap();
        assert_eq!(n, (Bytes::from(vec![2]), WalPosition::INVALID));
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

        table.promote_to_flat(LastProcessed::new(u64::MAX));

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
        for k in [&[1u8][..], &[3u8][..]] {
            let prev = table.data_get_latest(k).unwrap();
            table.data_remove_matching(k, |v| *v == prev);
            table.data_insert_sorted(Bytes::from(k.to_vec()), prev.as_clean_modified());
        }

        table.promote_to_flat(LastProcessed::new(u64::MAX));

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
        table.promote_to_flat(LastProcessed::new(u64::MAX));
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
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        assert_eq!(table.flat_len(), 2);
        assert_eq!(table.dirty_count(), 2);

        // Delete [1] (Removed goes to BTreeMap) and add [3].
        table.remove(vec![1].into(), WalPosition::test_value(10));
        table.insert(vec![3].into(), WalPosition::test_value(5));
        // dirty_count: 2 (flat Modified) + 1 (Removed) + 1 (new Modified) = 4
        assert_eq!(table.dirty_count(), 4);

        table.promote_to_flat(LastProcessed::new(u64::MAX));
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
        table.promote_to_flat(LastProcessed::new(u64::MAX));
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
        table.promote_to_flat(LastProcessed::new(u64::MAX));
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
        table.promote_to_flat(LastProcessed::new(u64::MAX));
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
        table.demote_data_modified_to_clean();
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        // New entry after promote (in BTreeMap).
        table.insert(vec![2].into(), WalPosition::test_value(20));

        let shape = KeyShape::new_single_config_indexing(
            KeyIndexing::variable_length(),
            1,
            KeyType::uniform(1),
            Default::default(),
        );
        let ks = KeySpace::first();
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

        let shape = KeyShape::new_single_config_indexing(
            KeyIndexing::variable_length(),
            1,
            KeyType::uniform(1),
            Default::default(),
        );
        let ks = KeySpace::first();
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
    fn test_merge_dirty_keeps_disk_entries_in_flat() {
        // Regression: a tombstone read back from disk carries the INVALID
        // sentinel offset (u64::MAX). `merge_dirty_and_clean` must fold such
        // disk-derived `flat` entries into the merged `flat`, never into the
        // `data` overlay — the offset-based filters only reason about genuine
        // WAL offsets, so a sentinel-offset entry in `data` is misread as an
        // unprocessed post-frontier write and dropped by retain_processed,
        // resurrecting the value the tombstone shadows in a deeper level.
        let shape = KeyShape::new_single_config_indexing(
            KeyIndexing::variable_length(),
            1,
            KeyType::uniform(1),
            Default::default(),
        );
        let ks = KeySpace::first();
        let ks = shape.ks(ks);

        // Build an on-disk blob holding a tombstone for [2], then round-trip it
        // so the tombstone decodes back as INVALID (offset u64::MAX) in `flat`.
        let mut on_disk = IndexTable::default();
        on_disk.insert(vec![1].into(), WalPosition::test_value(10));
        on_disk.remove(vec![2].into(), WalPosition::test_value(25));
        let mut buf = BytesMut::new();
        on_disk.serialize_index_entries(ks, &mut buf);
        let dirty = IndexTable::deserialize_index_entries(ks, Bytes::from(buf.freeze()));
        assert_eq!(dirty.get(&[2]), Some(WalPosition::INVALID));

        // Mirror the flusher's merge-flush path.
        let mut merged = IndexTable::default();
        merged.merge_dirty_and_clean(&dirty);

        // Invariant: no disk-derived (INVALID-offset) entry leaked into `data`.
        assert!(
            merged
                .data
                .values()
                .flatten()
                .all(|v| v.offset() != WalPosition::INVALID.offset()),
            "disk tombstone leaked into the data overlay"
        );
        assert!(
            !merged.data_contains_key(&[2]),
            "tombstone should live in flat"
        );

        // The offset filter must leave the disk tombstone untouched (it is in
        // flat), so the shadowed value stays deleted.
        merged.retain_processed(LastProcessed::new(1_000_000));
        assert_eq!(merged.get(&[1]), Some(WalPosition::test_value(10)));
        assert_eq!(
            merged.get(&[2]),
            Some(WalPosition::INVALID),
            "disk-origin tombstone was dropped (resurrection bug)"
        );
    }

    #[test]
    fn test_next_entry_mixed_flat_and_btree() {
        let mut table = IndexTable::default();
        // [1], [3], [5] go to flat.
        table.insert(vec![1].into(), WalPosition::test_value(10));
        table.insert(vec![3].into(), WalPosition::test_value(30));
        table.insert(vec![5].into(), WalPosition::test_value(50));
        table.demote_data_modified_to_clean();
        table.promote_to_flat(LastProcessed::new(u64::MAX));
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
        self_table.demote_data_modified_to_clean();

        // dirty: [2] updated, [3] new — promoted to flat.
        let mut dirty = IndexTable::default();
        dirty.insert(vec![2].into(), WalPosition::test_value(25));
        dirty.insert(vec![3].into(), WalPosition::test_value(35));
        dirty.promote_to_flat(LastProcessed::new(u64::MAX));

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
        self_table.demote_data_modified_to_clean();
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
        assert!(self_table.data_get_latest(&[2]).unwrap().is_removed());
        // Standalone tombstone preserved (would shadow a deeper-level entry).
        assert_eq!(self_table.get(&[9]), Some(WalPosition::INVALID));
        assert!(self_table.data_get_latest(&[9]).unwrap().is_removed());

        // clean_self() then strips tombstones — the deepest-level contract.
        self_table.clean_self();
        assert!(!self_table.data_contains_key(&[2]));
        assert!(!self_table.data_contains_key(&[9]));
    }

    #[test]
    fn test_merge_dirty_no_clean_with_flat() {
        let mut self_table = IndexTable::default();
        self_table.insert(vec![1].into(), WalPosition::test_value(10));
        self_table.demote_data_modified_to_clean();

        let mut dirty = IndexTable::default();
        dirty.insert(vec![2].into(), WalPosition::test_value(20));
        dirty.insert(vec![3].into(), WalPosition::test_value(30));
        dirty.promote_to_flat(LastProcessed::new(u64::MAX));

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
        original.promote_to_flat(LastProcessed::new(u64::MAX));

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
        current.promote_to_flat(LastProcessed::new(u64::MAX));
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
        current.promote_to_flat(LastProcessed::new(u64::MAX));
        // Overwrite key [1] with a newer position in self.data.
        current.insert(vec![1].into(), WalPosition::test_value(10));

        current.unmerge_flushed(&original, LastProcessed::new_test(u64::MAX));

        // Key [1] still has its newer position; key [2] fully removed.
        assert_eq!(current.get(&[1]), Some(WalPosition::test_value(10)));
        assert!(current.get(&[2]).is_none());
        // No stale [1]@pos2 left behind in flat.
        assert_eq!(current.flat_binary_search(&[1]), None);
    }

    #[test]
    fn test_unmerge_flushed_drops_disk_tombstone_from_flat() {
        // A disk tombstone in `flat` has offset u64::MAX, which the old frontier
        // gate read as "unprocessed" and kept. The position match drops it; the
        // post-flush read falls through to disk, so the drop is correct.
        let shape = KeyShape::new_single_config_indexing(
            KeyIndexing::variable_length(),
            1,
            KeyType::uniform(1),
            Default::default(),
        );
        let ks = KeySpace::first();
        let ks = shape.ks(ks);

        // Round-trip a blob (live [1], tombstone [2]) so [2] lands in `flat` as
        // INVALID (offset u64::MAX).
        let mut on_disk = IndexTable::default();
        on_disk.insert(vec![1].into(), WalPosition::test_value(10));
        on_disk.remove(vec![2].into(), WalPosition::test_value(25));
        let mut buf = BytesMut::new();
        on_disk.serialize_index_entries(ks, &mut buf);
        let original = IndexTable::deserialize_index_entries(ks, Bytes::from(buf.freeze()));
        assert_eq!(original.get(&[2]), Some(WalPosition::INVALID));
        assert!(original.data.is_empty());

        // Snapshot + a concurrent write to a different key, so the flat side is
        // reconciled against original.flat (not short-circuited as same-shared).
        let mut current = original.clone();
        current.insert(vec![3].into(), WalPosition::test_value(40));

        // Real frontier well below u64::MAX, as in production.
        current.unmerge_flushed(&original, LastProcessed::new_test(1000));

        // Both flat entries unmerged; only the post-snapshot write survives.
        assert_eq!(current.get(&[1]), None);
        assert_eq!(current.get(&[2]), None);
        assert_eq!(current.get(&[3]), Some(WalPosition::test_value(40)));
        assert!(current.flat_binary_search(&[2]).is_none());
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
        let btree: DataOverlay = data_overlay([
            (vec![1].into(), iwp_modified(1)),
            (vec![2, 5].into(), iwp_clean(2)),
            (vec![9].into(), iwp_modified(3)),
        ]);

        let (new_flat, dirty) = merge_into_flat(&flat, None, &btree, LastProcessed::new(u64::MAX));
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
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        let flat = table.flat.clone();
        let btree: DataOverlay = DataOverlay::new();

        let (new_flat, dirty) = merge_into_flat(&flat, None, &btree, LastProcessed::new(u64::MAX));
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
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        let flat = table.flat.clone();

        // btree: [1]=Modified@10 (overrides flat), [2]=Modified@2 (new)
        let btree: DataOverlay = data_overlay([
            (vec![1].into(), iwp_modified(10)),
            (vec![2].into(), iwp_modified(2)),
        ]);

        let (new_flat, dirty) = merge_into_flat(&flat, None, &btree, LastProcessed::new(u64::MAX));
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
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        let flat = table.flat.clone();

        let btree: DataOverlay = data_overlay([
            (vec![2].into(), iwp_clean(2)),
            (vec![5].into(), iwp_clean(5)),
            (vec![8].into(), iwp_clean(8)),
        ]);

        let (new_flat, dirty) = merge_into_flat(&flat, None, &btree, LastProcessed::new(u64::MAX));
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
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        let flat = table.flat.clone();
        let btree: DataOverlay = data_overlay([(vec![1].into(), iwp_removed(10))]);

        let (new_flat, dirty) = merge_into_flat(&flat, None, &btree, LastProcessed::new(u64::MAX));
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
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        table.remove(vec![1].into(), WalPosition::test_value(10));
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        // flat now has [1]=Removed, [2]=Modified.
        let flat = table.flat.clone();
        // Sanity: flat really has the tombstone encoded.
        let pre = decode_flat(&flat, None);
        assert_eq!(pre.len(), 2);
        assert_eq!(pre[0].1.kind, IndexEntryKind::Removed);

        // Merge with empty btree — Removed must round-trip unchanged.
        let (new_flat, dirty) = merge_into_flat(
            &flat,
            None,
            &DataOverlay::new(),
            LastProcessed::new(u64::MAX),
        );
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
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        table.remove(vec![1].into(), WalPosition::test_value(10));
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        let flat = table.flat.clone();

        let btree: DataOverlay = data_overlay([(vec![1].into(), iwp_modified(20))]);
        let (new_flat, dirty) = merge_into_flat(&flat, None, &btree, LastProcessed::new(u64::MAX));
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
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        table.remove(k4(10), WalPosition::test_value(50));
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        let flat = table.flat.clone();

        let (new_flat, dirty) = merge_into_flat(
            &flat,
            Some(4),
            &DataOverlay::new(),
            LastProcessed::new(u64::MAX),
        );
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
        let btree: DataOverlay = DataOverlay::new();
        let (new_flat, dirty) = merge_into_flat(&flat, None, &btree, LastProcessed::new(u64::MAX));
        assert!(new_flat.is_empty());
        assert_eq!(dirty, 0);
    }

    #[test]
    fn test_merge_into_flat_fixed_len_btree_overrides_flat() {
        // flat with two fixed-len keys.
        let mut table = IndexTable::default();
        table.insert(k4(10), WalPosition::test_value(10));
        table.insert(k4(30), WalPosition::test_value(30));
        table.demote_data_modified_to_clean();
        table.key_size = Some(4);
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        let flat = table.flat.clone();

        // btree overrides k4(10), inserts k4(20).
        let btree: DataOverlay =
            data_overlay([(k4(10), iwp_modified(100)), (k4(20), iwp_modified(20))]);

        let (new_flat, dirty) =
            merge_into_flat(&flat, Some(4), &btree, LastProcessed::new(u64::MAX));
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
        let btree: DataOverlay = data_overlay([
            (k4(1), iwp_modified(1)),
            (k4(2), iwp_clean(2)),
            (k4(3), iwp_removed(3)),
        ]);

        let (new_flat, dirty) =
            merge_into_flat(&flat, Some(4), &btree, LastProcessed::new(u64::MAX));
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
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        let flat = table.flat.clone();

        let (new_flat, dirty) = merge_into_flat(
            &flat,
            Some(4),
            &DataOverlay::new(),
            LastProcessed::new(u64::MAX),
        );
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
        // Demote [1] to Clean. Then add a Removed@3 position for [3] — it
        // coexists with the prior Modified@3 position in the key's list, and
        // since `Removed > Modified` under the derived IWP ordering, the
        // Removed entry wins as the latest for [3].
        let prev_1 = table.data_get_latest(&[1]).unwrap();
        table.data_remove_matching(&[1], |v| *v == prev_1);
        table.data_insert_sorted(Bytes::from(vec![1]), prev_1.as_clean_modified());
        table.data_insert_sorted(Bytes::from(vec![3]), iwp_removed(3));
        table.promote_to_flat(LastProcessed::new(u64::MAX));
        // flat now: [1]=Clean, [2]=Modified, [3]=Removed.
        let flat = table.flat.clone();

        // data set: [4]=Clean (overrides nothing), [2]=Modified@20 (overrides [2])
        let data: DataOverlay = data_overlay([
            (vec![2].into(), iwp_modified(20)),
            (vec![4].into(), iwp_clean(4)),
        ]);

        let (new_flat, dirty) = merge_into_flat(&flat, None, &data, LastProcessed::new(u64::MAX));
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
        let btree: DataOverlay = data_overlay([
            (vec![1].into(), iwp_modified(1)),
            (vec![2, 3].into(), iwp_modified(2)),
            (vec![4, 5, 6].into(), iwp_modified(3)),
            (vec![7, 8, 9, 10, 11].into(), iwp_modified(4)),
        ]);

        let (new_flat, _) = merge_into_flat(&flat, None, &btree, LastProcessed::new(u64::MAX));
        // header: 4 + 4*4 = 20
        // entries: (14+1) + (14+2) + (14+3) + (14+5) = 15+16+17+19 = 67
        assert_eq!(new_flat.len(), 20 + 67);
    }

    // -----------------------------------------------------------------------
    // Multi-position-per-key behaviour
    //
    // The `data` map holds several `IndexWalPosition` values per key (the key's
    // ordered position list). Observers must dedupe to the latest IWP per key
    // (the list's last element — the largest under derived `(offset, len, kind)`
    // ordering, where `Removed > Modified > Clean`). The tests below exercise
    // scenarios reachable when a key is written more than once before a
    // promote drains it.
    // -----------------------------------------------------------------------

    #[test]
    fn test_multi_position_get_iter_keys_len_dedup() {
        // Two writes to the same key without an intervening promote: every
        // observer must see exactly one logical entry (the latest position).
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(10));
        table.insert(vec![1].into(), WalPosition::test_value(20));

        assert_eq!(table.get(&[1]), Some(WalPosition::test_value(20)));
        assert_eq!(table.len(), 1);
        assert_eq!(table.dirty_count(), 1);
        assert_eq!(table.total_key_bytes(), 1);

        let items: Vec<_> = table.iter().collect();
        assert_eq!(
            items,
            vec![(Bytes::from(vec![1]), WalPosition::test_value(20))]
        );
        let keys: Vec<_> = table.keys().collect();
        assert_eq!(keys, vec![Bytes::from(vec![1])]);
    }

    #[test]
    fn test_multi_position_next_entry_skips_older_for_same_key() {
        // Forward/backward traversal must yield one tuple per key, not one
        // per stored position. Key [2] has two positions; both directions
        // must surface only the latest (@30) and then move on.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(10));
        table.insert(vec![2].into(), WalPosition::test_value(20));
        table.insert(vec![2].into(), WalPosition::test_value(30));
        table.insert(vec![3].into(), WalPosition::test_value(40));

        // Forward.
        let n = table.next_entry(Some(vec![1].into()), false).unwrap();
        assert_eq!(n, (Bytes::from(vec![2]), WalPosition::test_value(30)));
        let n = table.next_entry(Some(vec![2].into()), false).unwrap();
        assert_eq!(n, (Bytes::from(vec![3]), WalPosition::test_value(40)));

        // Reverse.
        let n = table.next_entry(Some(vec![3].into()), true).unwrap();
        assert_eq!(n, (Bytes::from(vec![2]), WalPosition::test_value(30)));
        let n = table.next_entry(Some(vec![2].into()), true).unwrap();
        assert_eq!(n, (Bytes::from(vec![1]), WalPosition::test_value(10)));
    }

    #[test]
    fn test_next_entry_at_returns_latest_processed_position() {
        // Key [2] carries a processed (@10) and a post-frontier (@100) position.
        // As-of frontier=50, both directions must surface the latest *processed*
        // position (@10), never the post-frontier one.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(5));
        table.insert(vec![2].into(), WalPosition::test_value(10));
        table.insert(vec![2].into(), WalPosition::test_value(100));
        table.insert(vec![3].into(), WalPosition::test_value(40));
        let lp = LastProcessed::new_test(50);

        let fwd = table
            .next_entry_at(Some(vec![1].into()), false, lp)
            .unwrap();
        assert_eq!(fwd, (Bytes::from(vec![2]), WalPosition::test_value(10)));
        let rev = table.next_entry_at(Some(vec![3].into()), true, lp).unwrap();
        assert_eq!(rev, (Bytes::from(vec![2]), WalPosition::test_value(10)));
        assert_eq!(table.get_at(&[2], lp), Some(WalPosition::test_value(10)));
    }

    #[test]
    fn test_next_entry_at_skips_key_with_only_post_frontier_positions() {
        // Key [2]'s positions are all post-frontier (@100, @200). As-of
        // frontier=50 it must be skipped entirely; iteration lands on the next
        // processed key in each direction.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(5));
        table.insert(vec![2].into(), WalPosition::test_value(100));
        table.insert(vec![2].into(), WalPosition::test_value(200));
        table.insert(vec![3].into(), WalPosition::test_value(40));
        let lp = LastProcessed::new_test(50);

        let fwd = table
            .next_entry_at(Some(vec![1].into()), false, lp)
            .unwrap();
        assert_eq!(fwd, (Bytes::from(vec![3]), WalPosition::test_value(40)));
        let rev = table.next_entry_at(Some(vec![3].into()), true, lp).unwrap();
        assert_eq!(rev, (Bytes::from(vec![1]), WalPosition::test_value(5)));
        assert_eq!(table.get_at(&[2], lp), None);
    }

    #[test]
    fn test_next_entry_at_forward_reverse_agree_on_within_group_selection() {
        // Key [2] has three positions @10/@20/@30. Forward selects the last
        // tuple below the frontier; reverse selects the first one descending.
        // Both must resolve to the same latest-processed position.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(5));
        table.insert(vec![2].into(), WalPosition::test_value(10));
        table.insert(vec![2].into(), WalPosition::test_value(20));
        table.insert(vec![2].into(), WalPosition::test_value(30));
        table.insert(vec![3].into(), WalPosition::test_value(40));

        // frontier=25: @10,@20 processed, @30 post-frontier → latest processed @20.
        let lp = LastProcessed::new_test(25);
        let fwd = table
            .next_entry_at(Some(vec![1].into()), false, lp)
            .unwrap();
        let rev = table.next_entry_at(Some(vec![3].into()), true, lp).unwrap();
        assert_eq!(fwd, (Bytes::from(vec![2]), WalPosition::test_value(20)));
        assert_eq!(rev, (Bytes::from(vec![2]), WalPosition::test_value(20)));

        // frontier=1000: all processed → latest processed @30.
        let lp = LastProcessed::new_test(1000);
        let fwd = table
            .next_entry_at(Some(vec![1].into()), false, lp)
            .unwrap();
        let rev = table.next_entry_at(Some(vec![3].into()), true, lp).unwrap();
        assert_eq!(fwd, (Bytes::from(vec![2]), WalPosition::test_value(30)));
        assert_eq!(rev, (Bytes::from(vec![2]), WalPosition::test_value(30)));
    }

    #[test]
    fn test_promote_to_flat_unprocessed_latest_keeps_all_positions() {
        // User contract: when the latest position for a key is unprocessed,
        // the whole key (every position) remains in `data`. The older
        // processed position must NOT be promoted standalone — that would
        // leave a stale flat entry shadowed by a newer in-memory write.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(10));
        table.insert(vec![1].into(), WalPosition::test_value(100));

        // last_processed = 50: @10 processed, @100 unprocessed.
        table.promote_to_flat(LastProcessed::new(50));

        assert_eq!(table.flat_len(), 0);
        // Both positions for [1] are retained (the whole key stays in `data`).
        assert_eq!(table.data.get([1].as_ref()).map(|p| p.len()), Some(2));
        assert_eq!(table.get(&[1]), Some(WalPosition::test_value(100)));
    }

    #[test]
    fn test_apply_update_on_multi_position_replaces_latest() {
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(10));
        table.insert(vec![1].into(), WalPosition::test_value(20));

        // apply_update sees the latest (@20), rewrites it to @25.
        table.apply_update(&[1], |iwp| {
            iwp.replace_wal_position(WalPosition::test_value(25));
        });

        assert_eq!(table.get(&[1]), Some(WalPosition::test_value(25)));
        // The older @10 still occupies a slot; the @20 position is gone and
        // replaced by @25.
        let offsets: Vec<u64> = table
            .data
            .get([1].as_ref())
            .unwrap()
            .iter()
            .map(|v| v.offset)
            .collect();
        assert_eq!(offsets, vec![10, 25]);
    }

    #[test]
    fn test_clean_self_modified_then_removed_drops_key() {
        // data = [(K, Modified@10), (K, Removed@20)]. Latest is Removed →
        // clean_self drops the entire key (both tuples), no orphan left.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(10));
        table.remove(vec![1].into(), WalPosition::test_value(20));

        table.clean_self();

        assert_eq!(table.get(&[1]), None);
        assert!(table.is_empty());
        assert_eq!(table.dirty_count(), 0);
    }

    #[test]
    fn test_clean_self_two_modified_collapses_to_single_clean() {
        // data = [(K, M@10), (K, M@20)]. clean_self collapses to exactly one
        // Clean tuple at @20 (the latest). The older @10 must be discarded.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(10));
        table.insert(vec![1].into(), WalPosition::test_value(20));

        table.clean_self();

        let entries: Vec<_> = table.data_iter_latest().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(table.data.get([1].as_ref()).map(|p| p.len()), Some(1));
        assert_eq!(entries[0].1.offset, 20);
        assert!(entries[0].1.is_clean());
        assert_eq!(table.get(&[1]), Some(WalPosition::test_value(20)));
        assert_eq!(table.dirty_count(), 0);
    }

    #[test]
    fn test_unmerge_flushed_keeps_newer_position_added_after_snapshot() {
        // original snapshot: [K@10]. After cloning, self adds K@20.
        // unmerge_flushed must drop the snapshotted @10 (it's been flushed)
        // and leave the post-snapshot @20 intact.
        let mut original = IndexTable::default();
        original.insert(vec![1].into(), WalPosition::test_value(10));

        let mut current = original.clone();
        current.insert(vec![1].into(), WalPosition::test_value(20));

        current.unmerge_flushed(&original, LastProcessed::new_test(u64::MAX));

        assert_eq!(current.get(&[1]), Some(WalPosition::test_value(20)));
        // One position remains for [1] (the post-snapshot @20).
        assert_eq!(current.data.get([1].as_ref()).map(|p| p.len()), Some(1));
    }

    #[test]
    fn test_merge_dirty_uses_latest_per_key_from_dirty() {
        // dirty.data has multi-position state for [1]; merge_dirty must only
        // project the latest (per the user's contract).
        let mut self_table = IndexTable::default();
        self_table.insert(vec![1].into(), WalPosition::test_value(5));
        self_table.demote_data_modified_to_clean();

        let mut dirty = IndexTable::default();
        dirty.insert(vec![1].into(), WalPosition::test_value(10));
        dirty.insert(vec![1].into(), WalPosition::test_value(20));

        self_table.merge_dirty_and_clean(&dirty);

        assert_eq!(self_table.get(&[1]), Some(WalPosition::test_value(20)));
    }

    #[test]
    fn test_serialize_round_trip_with_multi_position_data() {
        // Serialization must dedupe to latest-per-key. After a round trip
        // the deserialized table holds one entry per key.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(10));
        table.insert(vec![1].into(), WalPosition::test_value(20));
        table.insert(vec![2].into(), WalPosition::test_value(30));

        let shape = KeyShape::new_single_config_indexing(
            KeyIndexing::variable_length(),
            1,
            KeyType::uniform(1),
            Default::default(),
        );
        let ks = KeySpace::first();
        let ks = shape.ks(ks);
        let mut buf = BytesMut::new();
        table.serialize_index_entries(ks, &mut buf);
        let deserialized = IndexTable::deserialize_index_entries(ks, Bytes::from(buf.freeze()));

        assert_eq!(deserialized.get(&[1]), Some(WalPosition::test_value(20)));
        assert_eq!(deserialized.get(&[2]), Some(WalPosition::test_value(30)));
        assert_eq!(deserialized.len(), 2);
    }

    #[test]
    fn test_compact_with_drops_all_positions_for_not_retained_key() {
        // Regression: previously the per-tuple `retain` predicate kept every
        // `Removed` tuple unconditionally, so dropping a key whose multi-
        // position state interleaved a tombstone left an orphan tombstone
        // behind that would shadow deeper levels. Per-key `retain` drops
        // every tuple for the key together.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(10));
        table.remove(vec![1].into(), WalPosition::test_value(20));
        table.insert(vec![1].into(), WalPosition::test_value(30));
        table.insert(vec![2].into(), WalPosition::test_value(40));

        // Pre-compact: [1] has a live latest (Modified@30).
        assert_eq!(table.get(&[1]), Some(WalPosition::test_value(30)));

        // Compactor drops [1], keeps [2].
        table.compact_with(|iter| {
            let mut s = HashSet::new();
            for k in iter {
                if k.as_ref() == [2u8].as_ref() {
                    s.insert(k.clone());
                }
            }
            s
        });

        // [1] must be completely gone — not an INVALID tombstone shadowing L1.
        assert_eq!(table.get(&[1]), None);
        assert!(!table.data_contains_key(&[1]));
        // [2] retained.
        assert_eq!(table.get(&[2]), Some(WalPosition::test_value(40)));
    }

    #[test]
    fn test_compact_with_preserves_tombstone_when_latest_is_removed() {
        // The complement of the regression above: if the latest IS a
        // tombstone, the key (with every position) must be kept so the
        // tombstone can shadow deeper levels.
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(10));
        table.remove(vec![1].into(), WalPosition::test_value(20));
        // Latest for [1] is Removed.

        // Compactor wouldn't see [1] (filtered as removed). Pass empty retain set.
        table.compact_with(|_| HashSet::new());

        // Tombstone preserved (latest still Removed).
        assert_eq!(table.get(&[1]), Some(WalPosition::INVALID));
        assert!(table.data_get_latest(&[1]).unwrap().is_removed());
    }

    #[test]
    fn test_into_data_dedupes_multi_position() {
        let mut table = IndexTable::default();
        table.insert(vec![1].into(), WalPosition::test_value(10));
        table.insert(vec![1].into(), WalPosition::test_value(20));
        table.insert(vec![2].into(), WalPosition::test_value(30));

        let data = table.into_data();
        assert_eq!(data.len(), 2);
        assert_eq!(data[&Bytes::from(vec![1])], WalPosition::test_value(20));
        assert_eq!(data[&Bytes::from(vec![2])], WalPosition::test_value(30));
    }
}
