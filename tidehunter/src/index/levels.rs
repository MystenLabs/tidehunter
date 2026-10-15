//! `IndexLevels` — the per-cell list of on-disk index blobs.
//!
//! The two-level LSM design represents each cell's on-disk state as an
//! ordered list of level slots: L0 (freshest, small, rewritten often) at
//! index 0, L1 (cold, large, rewritten only on promote) at index 1, and —
//! schema-wise — further levels if ever needed.
//!
//! A slot can be "empty" even when a later slot is populated: immediately
//! after a promote, L0 is empty and L1 holds the merged blob. To represent
//! that unambiguously (without depending on size-based classification at
//! read-time) we store `IndexRef::INVALID` as a sentinel for empty
//! interior slots. Trailing INVALIDs do not occur — all constructors
//! produce canonical form and `set()` only accepts valid refs.
//!
//! When auto-sharding is enabled and a cell's L1 would exceed the WAL
//! fragment size, the L1 slot is "vacated" (set to INVALID) and the
//! `shards` btree owns L1 instead. Each entry maps a shard's
//! **content minimum** key to the shard's `IndexRef` plus its
//! **content maximum** key. The min/max pair lets the iterator pick the
//! one shard that holds a key past `prev_key` (forward) or before it
//! (reverse) without ever opening an empty shard.
use crate::index::index_ref::IndexRef;
use crate::wal::position::WalPosition;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::ops::Bound;

/// Per-shard descriptor stored as the value side of the shard btree.
///
/// `index_ref` identifies the shard's serialized index blob.
/// `max_key` is the largest content key inside that blob — paired with
/// the btree key (which is the shard's smallest content key) it gives
/// the iterator a closed `[min_key, max_key]` range without reading the
/// blob, so forward iteration can decide "does this shard hold anything
/// past prev_key?" in O(log N_shards).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexShard {
    pub index_ref: IndexRef,
    pub max_key: Vec<u8>,
}

impl IndexShard {
    pub fn new(index_ref: IndexRef, max_key: Vec<u8>) -> Self {
        Self { index_ref, max_key }
    }
}

/// Inline SmallVec capacity used by all per-level containers (the level list
/// itself, plus matching buffers of readers/caches built alongside it in
/// `large_table` and `index_format`). Sized to the two-level default policy
/// so the common case — L0 only, or L0+L1 — never allocates. Not a hard cap:
/// SmallVec spills to the heap if a cell ever carries more levels.
pub const INLINE_LEVELS: usize = 2;

/// Ordered list of refs to one cell's on-disk index blobs, freshest first.
///
/// Invariants:
/// - `refs[0]` is the L0 slot, `refs[1]` is L1, etc.
/// - An entry of `IndexRef::INVALID` means "this level slot exists in the
///   schema but currently holds no blob" (e.g., L0 right after a promote).
/// - No trailing INVALIDs: `[L0]` is the only form of "L0 only"; `[L0, INVALID]`
///   never occurs. Interior INVALIDs — typically `[INVALID, L1]` post-promote —
///   are valid and preserved.
/// - The empty list represents a cell that has never been flushed.
/// - Sharded form (`!shards.is_empty()`): `refs[1]` MUST be
///   `IndexRef::INVALID` — the btree owns the L1 data. `refs[0]`
///   (the L0 slot) still behaves normally.
/// - Sharded btree keys are each shard's **smallest content key** in
///   strictly increasing byte order (no sentinel). Each value's
///   `max_key` is the shard's **largest content key** and satisfies
///   `btree_key ≤ max_key < next_btree_key` (shards are disjoint).
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexLevels {
    refs: SmallVec<[IndexRef; INLINE_LEVELS]>,
    /// Per-range L1 sub-blobs when auto-sharding has split L1. Empty when
    /// the cell uses the legacy single-blob L1 (the common case). See the
    /// struct-level invariants for the contract this field carries.
    #[serde(default)]
    shards: BTreeMap<Vec<u8>, IndexShard>,
}

impl IndexLevels {
    /// Returns an empty level set (no on-disk blobs).
    pub fn new() -> Self {
        Self {
            refs: SmallVec::new(),
            shards: BTreeMap::new(),
        }
    }

    /// Returns a level set with a single L0 blob.
    pub fn single(index_ref: IndexRef) -> Self {
        debug_assert!(
            index_ref.is_valid(),
            "IndexLevels::single requires a valid ref",
        );
        let mut refs = SmallVec::new();
        refs.push(index_ref);
        Self {
            refs,
            shards: BTreeMap::new(),
        }
    }

    /// Returns a post-promote level set: L0 is empty (sentinel), L1 holds
    /// the promoted blob. Shape: `[INVALID, l1]`.
    pub fn promoted(l1: IndexRef) -> Self {
        debug_assert!(
            l1.is_valid(),
            "IndexLevels::promoted requires a valid L1 ref",
        );
        let mut refs = SmallVec::new();
        refs.push(IndexRef::INVALID);
        refs.push(l1);
        Self {
            refs,
            shards: BTreeMap::new(),
        }
    }

    /// Constructs from a possibly-invalid `WalPosition`. Used in the
    /// migration path from `SnapshotEntryData` where `WalPosition::INVALID`
    /// historically represented "no blob"; callers should prefer `new()` or
    /// `single()` for fresh code.
    pub fn from_legacy_position(position: WalPosition) -> Self {
        if position.is_valid() {
            Self::single(IndexRef::new(position))
        } else {
            Self::new()
        }
    }

    /// Rebuilds an `IndexLevels` from a raw `positions` smallvec deserialized
    /// from the V2 (pre-sharding) control-region format. The resulting cell
    /// is unsharded — `shards` starts empty.
    pub(crate) fn from_legacy_v2_positions(
        positions: SmallVec<[WalPosition; INLINE_LEVELS]>,
    ) -> Self {
        Self {
            refs: positions.into_iter().map(IndexRef::new).collect(),
            shards: BTreeMap::new(),
        }
    }

    /// Returns a sharded level set with no L0: L1 has been split into the
    /// per-range blobs in `shards`. Shape: `[INVALID, INVALID]` plus the
    /// btree owning L1.
    ///
    /// Panics (debug) on any invariant violation (see [`debug_assert_shards`]).
    pub fn sharded(shards: BTreeMap<Vec<u8>, IndexShard>) -> Self {
        Self::debug_assert_shards(&shards);
        let mut refs = SmallVec::new();
        refs.push(IndexRef::INVALID);
        refs.push(IndexRef::INVALID);
        Self { refs, shards }
    }

    /// Returns a sharded level set carrying an L0 alongside the shard btree.
    /// Shape: `[l0, INVALID]` plus the btree owning L1.
    pub fn sharded_with_l0(l0: IndexRef, shards: BTreeMap<Vec<u8>, IndexShard>) -> Self {
        debug_assert!(
            l0.is_valid(),
            "IndexLevels::sharded_with_l0 requires a valid L0 ref",
        );
        Self::debug_assert_shards(&shards);
        let mut refs = SmallVec::new();
        refs.push(l0);
        refs.push(IndexRef::INVALID);
        Self { refs, shards }
    }

    fn debug_assert_shards(shards: &BTreeMap<Vec<u8>, IndexShard>) {
        debug_assert!(
            !shards.is_empty(),
            "sharded IndexLevels requires at least one shard",
        );
        debug_assert!(
            shards.values().all(|s| s.index_ref.is_valid()),
            "shard refs must all be valid",
        );
        // min_key (= btree key) ≤ max_key for every shard, and shards are
        // disjoint: max_key(X) < min_key(X+1).
        let mut prev_max: Option<&[u8]> = None;
        for (min_key, shard) in shards.iter() {
            debug_assert!(
                min_key.as_slice() <= shard.max_key.as_slice(),
                "shard min_key must be ≤ max_key",
            );
            if let Some(prev) = prev_max {
                debug_assert!(
                    prev < min_key.as_slice(),
                    "shards must be disjoint: previous max_key < current min_key",
                );
            }
            prev_max = Some(shard.max_key.as_slice());
        }
    }

    /// True when no level holds a blob. Equivalent to `l0().is_none() && l1().is_none() && ...`
    /// for the populated slots, and considers shard membership too.
    pub fn is_empty(&self) -> bool {
        self.refs.iter().all(|r| !r.is_valid()) && self.shards.is_empty()
    }

    /// Number of schema slots currently tracked (including empty interior slots).
    /// Does not count shards — those live under the L1 slot when sharded.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.refs.len()
    }

    /// Returns the freshest populated level (lowest-index non-INVALID slot
    /// in `refs`). Skips empty interior slots: for `[INVALID, L1]`, this
    /// returns `Some(L1)`.
    ///
    /// For sharded cells, this returns the L0 slot if populated and `None`
    /// otherwise — "freshest" is not meaningful across N disjoint shards,
    /// so we deliberately do not pick one. Callers that need a per-key
    /// answer should use [`shard_for`] instead.
    #[cfg(test)]
    pub fn latest(&self) -> Option<IndexRef> {
        self.refs.iter().copied().find(|r| r.is_valid())
    }

    /// Iterates **populated** levels from freshest (L0) to oldest, skipping
    /// empty interior slots. This is the read-path order: first hit wins.
    /// When sharded, the shard blobs are appended in btree key order after
    /// the (always-INVALID) L1 slot is skipped.
    pub fn iter(&self) -> impl Iterator<Item = IndexRef> + '_ {
        self.refs
            .iter()
            .copied()
            .filter(|r| r.is_valid())
            .chain(self.shards.values().map(|s| s.index_ref))
    }

    /// Like [`iter`] but yields `(level_idx, index_ref)` pairs. The level index
    /// is the schema slot (0 = L0, 1 = L1, …), preserved across empty interior
    /// slots — so a post-promote `[INVALID, L1]` yields `(1, L1)`. Shard blobs
    /// are yielded at the L1 slot (level index 1) in btree key order.
    pub fn iter_with_level(&self) -> impl Iterator<Item = (usize, IndexRef)> + '_ {
        self.refs
            .iter()
            .copied()
            .enumerate()
            .filter(|(_, r)| r.is_valid())
            .chain(self.shards.values().map(|s| (1usize, s.index_ref)))
    }

    /// Iterates **populated** levels below L0 (slot index ≥ 1), skipping
    /// empty interior slots. Used on read paths where L0 content has already
    /// been folded into an in-memory view (e.g., after `maybe_load`) and the
    /// caller needs to consult remaining on-disk levels. When sharded, yields
    /// every shard blob — point-lookup callers that only need one shard should
    /// use [`shard_for`] instead.
    pub fn iter_below_l0(&self) -> impl Iterator<Item = IndexRef> + '_ {
        self.refs
            .iter()
            .copied()
            .skip(1)
            .filter(|r| r.is_valid())
            .chain(self.shards.values().map(|s| s.index_ref))
    }

    /// Returns the ref at a specific level slot, if populated.
    /// INVALID slot or out-of-bounds both return `None`.
    pub fn get(&self, level: usize) -> Option<IndexRef> {
        self.refs.get(level).copied().filter(|r| r.is_valid())
    }

    /// Convenience for the L0 slot.
    pub fn l0(&self) -> Option<IndexRef> {
        self.get(0)
    }

    /// Convenience for the L1 slot.
    pub fn l1(&self) -> Option<IndexRef> {
        self.get(1)
    }

    /// Replaces (or appends) the ref at `level`. `index_ref` must be valid.
    ///
    /// Panics if `level` skips past the end — callers must fill levels contiguously.
    pub fn set(&mut self, level: usize, index_ref: IndexRef) {
        debug_assert!(index_ref.is_valid());
        assert!(
            level <= self.refs.len(),
            "level {level} skips past end of levels (len = {})",
            self.refs.len(),
        );
        if level == self.refs.len() {
            self.refs.push(index_ref);
        } else {
            self.refs[level] = index_ref;
        }
    }

    /// True when at most one slot is populated (post-promote `[INVALID, L1]`
    /// counts as single-level since only L1 holds data). Used by call sites
    /// that need to assert the L0-only or L1-only invariant explicitly.
    /// A cell sharded into more than one blob is **not** single-level — the
    /// shard btree contributes multiple populated entries to `iter()`.
    pub fn is_single_level(&self) -> bool {
        self.iter().count() <= 1
    }

    /// True when the L1 slot has been split into per-range shards. The
    /// caller must respect the sharded-form invariants documented on
    /// [`IndexLevels`].
    pub fn is_sharded(&self) -> bool {
        !self.shards.is_empty()
    }

    /// Returns the shard btree. Use [`shard_for`] for the per-key lookup
    /// instead of walking the map directly.
    pub fn shards(&self) -> &BTreeMap<Vec<u8>, IndexShard> {
        &self.shards
    }

    /// Returns the shard whose `[min_key, max_key]` range contains `key`,
    /// or `None` when the cell is unsharded or `key` falls outside every
    /// shard's content range (below the first shard's min, above the last
    /// shard's max, or in a gap between shards). Point-lookup callers use
    /// this to skip on-disk reads for keys that can't be in any shard.
    pub fn shard_for(&self, key: &[u8]) -> Option<&IndexShard> {
        self.shard_entry_for(key).map(|(_, shard)| shard)
    }

    /// [`shard_for`] plus the shard's btree key, for callers that group
    /// work per shard.
    pub fn shard_entry_for(&self, key: &[u8]) -> Option<(&Vec<u8>, &IndexShard)> {
        let (min_key, shard) = self
            .shards
            .range::<[u8], _>((Bound::Unbounded, Bound::Included(key)))
            .next_back()?;
        (shard.max_key.as_slice() >= key).then_some((min_key, shard))
    }

    /// Forward-iteration shard picker: the shard holding the smallest
    /// content key strictly greater than `prev_key`, or `None` when the
    /// cell has no key past `prev_key`. Returns the first shard when
    /// `prev_key` is `None`.
    ///
    /// Algorithm: the predecessor of `prev_key` in the btree (if any)
    /// covers `prev_key`'s range; we accept it iff its `max_key`
    /// strictly exceeds `prev_key` — i.e. there's still content past
    /// the cursor inside that shard. Otherwise the next shard whose
    /// `min_key > prev_key` (a single btree query) is the answer; the
    /// shard's `min_key` is itself the smallest content key past
    /// `prev_key`.
    pub fn shard_for_iter_forward(&self, prev_key: Option<&[u8]>) -> Option<&IndexShard> {
        if self.shards.is_empty() {
            return None;
        }
        let Some(k) = prev_key else {
            return self.shards.values().next();
        };
        if let Some((_, pred)) = self
            .shards
            .range::<[u8], _>((Bound::Unbounded, Bound::Included(k)))
            .next_back()
            && pred.max_key.as_slice() > k
        {
            return Some(pred);
        }
        self.shards
            .range::<[u8], _>((Bound::Excluded(k), Bound::Unbounded))
            .next()
            .map(|(_, s)| s)
    }

    /// Reverse-iteration shard picker: the shard holding the largest
    /// content key strictly less than `prev_key`, or `None` when the
    /// cell has no key below `prev_key`. Returns the last shard when
    /// `prev_key` is `None`.
    ///
    /// Algorithm: pick the shard with the largest `min_key < prev_key`
    /// (strict). Because the btree key IS the shard's smallest content
    /// key, that `min_key` is itself a content key below `prev_key`, so
    /// the shard is guaranteed non-empty for the reverse merge.
    pub fn shard_for_iter_reverse(&self, prev_key: Option<&[u8]>) -> Option<&IndexShard> {
        if self.shards.is_empty() {
            return None;
        }
        let Some(k) = prev_key else {
            return self.shards.values().next_back();
        };
        self.shards
            .range::<[u8], _>((Bound::Unbounded, Bound::Excluded(k)))
            .next_back()
            .map(|(_, s)| s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::position::WalPosition;

    fn pos(v: u64) -> IndexRef {
        IndexRef::test_value(v)
    }

    #[test]
    fn empty_roundtrip() {
        let lvls = IndexLevels::new();
        assert!(lvls.is_empty());
        assert_eq!(lvls.len(), 0);
        assert_eq!(lvls.latest(), None);
        assert_eq!(lvls.l0(), None);
        assert_eq!(lvls.l1(), None);
        assert!(lvls.is_single_level());
    }

    #[test]
    fn single_roundtrip() {
        let lvls = IndexLevels::single(pos(42));
        assert!(!lvls.is_empty());
        assert_eq!(lvls.len(), 1);
        assert_eq!(lvls.latest(), Some(pos(42)));
        assert_eq!(lvls.l0(), Some(pos(42)));
        assert_eq!(lvls.l1(), None);
        assert!(lvls.is_single_level());
    }

    #[test]
    fn legacy_invalid_is_empty() {
        let lvls = IndexLevels::from_legacy_position(WalPosition::INVALID);
        assert!(lvls.is_empty());
    }

    #[test]
    fn two_levels() {
        let mut lvls = IndexLevels::single(pos(1));
        lvls.set(1, pos(2));
        assert_eq!(lvls.len(), 2);
        assert_eq!(lvls.latest(), Some(pos(1)));
        assert_eq!(lvls.l0(), Some(pos(1)));
        assert_eq!(lvls.l1(), Some(pos(2)));
        assert!(!lvls.is_single_level());
        let collected: Vec<_> = lvls.iter().collect();
        assert_eq!(collected, vec![pos(1), pos(2)]);
    }

    #[test]
    fn promoted_post_promote_state() {
        let lvls = IndexLevels::promoted(pos(99));
        assert!(!lvls.is_empty());
        assert_eq!(lvls.len(), 2);
        assert_eq!(lvls.l0(), None);
        assert_eq!(lvls.l1(), Some(pos(99)));
        assert_eq!(lvls.latest(), Some(pos(99)));
        assert!(lvls.is_single_level());
        let collected: Vec<_> = lvls.iter().collect();
        assert_eq!(collected, vec![pos(99)]);
    }

    #[test]
    fn set_replaces() {
        let mut lvls = IndexLevels::single(pos(1));
        lvls.set(0, pos(10));
        assert_eq!(lvls.latest(), Some(pos(10)));
        assert_eq!(lvls.len(), 1);
    }

    #[test]
    #[should_panic]
    fn set_past_end_panics() {
        let mut lvls = IndexLevels::new();
        lvls.set(1, pos(1));
    }

    /// Builds a two-shard btree.
    /// Shard A: `[min_a, max_a]` at `pos_a`. Shard B: `[min_b, max_b]` at `pos_b`.
    /// Caller must keep `max_a < min_b` to honor the disjointness invariant.
    fn two_shard_btree(
        min_a: &[u8],
        max_a: &[u8],
        pos_a: IndexRef,
        min_b: &[u8],
        max_b: &[u8],
        pos_b: IndexRef,
    ) -> BTreeMap<Vec<u8>, IndexShard> {
        let mut m = BTreeMap::new();
        m.insert(min_a.to_vec(), IndexShard::new(pos_a, max_a.to_vec()));
        m.insert(min_b.to_vec(), IndexShard::new(pos_b, max_b.to_vec()));
        m
    }

    #[test]
    fn sharded_no_l0() {
        // Shard A: [b"a", b"l"] at pos 10. Shard B: [b"m", b"z"] at pos 20.
        let shards = two_shard_btree(b"a", b"l", pos(10), b"m", b"z", pos(20));
        let lvls = IndexLevels::sharded(shards.clone());
        assert!(!lvls.is_empty());
        assert!(lvls.is_sharded());
        assert_eq!(lvls.l0(), None);
        assert_eq!(lvls.l1(), None, "L1 slot is vacated when sharded");
        // iter yields shard positions in btree order.
        let collected: Vec<_> = lvls.iter().collect();
        assert_eq!(collected, vec![pos(10), pos(20)]);
        let below: Vec<_> = lvls.iter_below_l0().collect();
        assert_eq!(below, vec![pos(10), pos(20)]);
        // shard_for: tight `[min, max]` check.
        assert_eq!(lvls.shard_for(b"a").map(|s| s.index_ref), Some(pos(10))); // min of A
        assert_eq!(lvls.shard_for(b"f").map(|s| s.index_ref), Some(pos(10))); // inside A
        assert_eq!(lvls.shard_for(b"l").map(|s| s.index_ref), Some(pos(10))); // max of A
        assert_eq!(lvls.shard_for(b"m").map(|s| s.index_ref), Some(pos(20))); // min of B
        assert_eq!(lvls.shard_for(b"z").map(|s| s.index_ref), Some(pos(20))); // max of B
        // Outside any shard's content range:
        assert_eq!(lvls.shard_for(b"").map(|s| s.index_ref), None); // below A.min
        assert_eq!(lvls.shard_for(b"~").map(|s| s.index_ref), None); // above B.max
        assert!(!lvls.is_single_level());
        assert_eq!(lvls.shards(), &shards);
    }

    #[test]
    fn sharded_with_l0_iterates_l0_first() {
        let shards = two_shard_btree(b"a", b"j", pos(10), b"k", b"z", pos(20));
        let lvls = IndexLevels::sharded_with_l0(pos(99), shards);
        assert!(!lvls.is_empty());
        assert!(lvls.is_sharded());
        assert_eq!(lvls.l0(), Some(pos(99)));
        assert_eq!(lvls.l1(), None);
        let collected: Vec<_> = lvls.iter().collect();
        assert_eq!(collected, vec![pos(99), pos(10), pos(20)]);
        let levels_pairs: Vec<_> = lvls.iter_with_level().collect();
        assert_eq!(levels_pairs, vec![(0, pos(99)), (1, pos(10)), (1, pos(20))]);
        let below: Vec<_> = lvls.iter_below_l0().collect();
        assert_eq!(below, vec![pos(10), pos(20)]);
    }

    #[test]
    fn shard_for_returns_none_when_not_sharded() {
        let lvls = IndexLevels::promoted(pos(1));
        assert!(!lvls.is_sharded());
        assert!(lvls.shard_for(b"").is_none());
        assert!(lvls.shard_for(b"anything").is_none());
    }

    #[test]
    fn shard_for_iter_forward_paths() {
        // Shard A: [b"a", b"j"]. Shard B: [b"m", b"z"].
        let shards = two_shard_btree(b"a", b"j", pos(10), b"m", b"z", pos(20));
        let lvls = IndexLevels::sharded(shards);

        // prev=None → first shard.
        assert_eq!(
            lvls.shard_for_iter_forward(None).map(|s| s.index_ref),
            Some(pos(10))
        );
        // prev below all content → first shard (its min > prev).
        assert_eq!(
            lvls.shard_for_iter_forward(Some(b"")).map(|s| s.index_ref),
            Some(pos(10))
        );
        // prev inside A, before A.max → A (predecessor with max > prev).
        assert_eq!(
            lvls.shard_for_iter_forward(Some(b"e")).map(|s| s.index_ref),
            Some(pos(10))
        );
        // prev == A.max → A has nothing past, skip to B.
        assert_eq!(
            lvls.shard_for_iter_forward(Some(b"j")).map(|s| s.index_ref),
            Some(pos(20))
        );
        // prev in the gap (above A.max, below B.min) → B.
        assert_eq!(
            lvls.shard_for_iter_forward(Some(b"k")).map(|s| s.index_ref),
            Some(pos(20))
        );
        // prev inside B → B.
        assert_eq!(
            lvls.shard_for_iter_forward(Some(b"q")).map(|s| s.index_ref),
            Some(pos(20))
        );
        // prev == B.max → cell exhausted.
        assert!(lvls.shard_for_iter_forward(Some(b"z")).is_none());
        // prev above everything → cell exhausted.
        assert!(lvls.shard_for_iter_forward(Some(b"~")).is_none());
    }

    #[test]
    fn shard_for_iter_reverse_paths() {
        let shards = two_shard_btree(b"a", b"j", pos(10), b"m", b"z", pos(20));
        let lvls = IndexLevels::sharded(shards);

        // prev=None → last shard.
        assert_eq!(
            lvls.shard_for_iter_reverse(None).map(|s| s.index_ref),
            Some(pos(20))
        );
        // prev above B.max → B.
        assert_eq!(
            lvls.shard_for_iter_reverse(Some(b"~")).map(|s| s.index_ref),
            Some(pos(20))
        );
        // prev inside B → B (its min < prev).
        assert_eq!(
            lvls.shard_for_iter_reverse(Some(b"q")).map(|s| s.index_ref),
            Some(pos(20))
        );
        // prev == B.min → no shard has min < B.min except A; pick A.
        assert_eq!(
            lvls.shard_for_iter_reverse(Some(b"m")).map(|s| s.index_ref),
            Some(pos(10))
        );
        // prev inside A → A.
        assert_eq!(
            lvls.shard_for_iter_reverse(Some(b"e")).map(|s| s.index_ref),
            Some(pos(10))
        );
        // prev == A.min → no shard has min < A.min → cell exhausted.
        assert!(lvls.shard_for_iter_reverse(Some(b"a")).is_none());
        // prev below A.min → cell exhausted.
        assert!(lvls.shard_for_iter_reverse(Some(b"")).is_none());
    }

    #[test]
    fn sharded_serde_roundtrip() {
        let shards = two_shard_btree(b"a", b"f", pos(7), b"split", b"z", pos(8));
        let lvls = IndexLevels::sharded_with_l0(pos(3), shards);
        let bytes = bincode::serialize(&lvls).unwrap();
        let back: IndexLevels = bincode::deserialize(&bytes).unwrap();
        assert_eq!(back, lvls);
    }

    #[test]
    fn empty_shard_btree_treated_as_unsharded_after_serde() {
        // Default empty shards round-trips fine and behaves like the legacy
        // (unsharded) form.
        let lvls = IndexLevels::single(pos(1));
        let bytes = bincode::serialize(&lvls).unwrap();
        let back: IndexLevels = bincode::deserialize(&bytes).unwrap();
        assert_eq!(back, lvls);
        assert!(!back.is_sharded());
    }

    #[test]
    fn sharded_latest_returns_l0_or_none() {
        let shards = two_shard_btree(b"a", b"j", pos(10), b"m", b"z", pos(20));
        let with_l0 = IndexLevels::sharded_with_l0(pos(99), shards.clone());
        assert_eq!(with_l0.latest(), Some(pos(99)));
        let no_l0 = IndexLevels::sharded(shards);
        assert_eq!(no_l0.latest(), None);
    }
}
