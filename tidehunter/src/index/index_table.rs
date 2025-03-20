use crate::wal::WalPosition;
use minibytes::Bytes;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet};
use std::ops::RangeInclusive;

#[derive(Default, Clone, Debug)]
#[doc(hidden)]
pub struct IndexTable {
    // todo instead of loading entire BTreeMap in memory we should be able
    // to load parts of it from disk
    pub(crate) data: BTreeMap<Bytes, WalPosition>,
}

impl IndexTable {
    pub fn insert(&mut self, k: Bytes, v: WalPosition) -> Option<WalPosition> {
        // Only update index entry if new entry has higher wal position then the previous entry
        // See test_concurrent_single_value_update for details how this is tested
        // todo handle this comparison correctly when we have data relocation
        // todo might want a separate test with snapshot enabled
        match self.data.entry(k) {
            Entry::Vacant(va) => {
                va.insert(v);
                None
            }
            Entry::Occupied(mut oc) => {
                let previous = *oc.get();
                if previous < v {
                    oc.insert(v);
                }
                Some(previous)
            }
        }
    }

    pub fn remove(&mut self, k: &[u8]) -> Option<WalPosition> {
        self.data.remove(k)
    }

    /// Merges dirty IndexTable into a loaded IndexTable
    pub fn merge_dirty(&mut self, dirty: &Self) {
        // todo implement this efficiently taking into account both self and dirty are sorted
        for (k, v) in dirty.data.iter() {
            if v == &WalPosition::INVALID {
                self.remove(k);
            } else {
                self.insert(k.clone(), *v);
            }
        }
    }

    /// Remove flushed index entries, returning number of entries changed
    pub fn unmerge_flushed(&mut self, original: &Self) -> i64 {
        let mut delta = 0i64;
        for (k, v) in original.data.iter() {
            let prev = self.data.get(k);
            // todo clarify None here
            if prev == Some(v) {
                self.data.remove(k);
                delta -= 1;
            }
        }
        delta
    }

    /// Change loaded dirty IndexTable into unloaded dirty by retaining dirty keys and tombstones
    /// Returns delta in number of entries
    pub fn make_dirty(&mut self, mut dirty_keys: HashSet<Bytes>) -> i64 {
        let original_data_len = self.data.len() as i64;
        // todo this method can be optimized if dirty_keys are made sorted
        // only retain keys that are dirty, removing all clean keys
        self.data.retain(|k, _| dirty_keys.remove(k));
        // remaining dirty_keys are not in this index, means they were deleted
        // turn them into tombstones
        for dirty_key in dirty_keys {
            self.insert(dirty_key, WalPosition::INVALID);
        }
        let data_len = self.data.len() as i64;
        data_len - original_data_len
    }

    pub fn get(&self, k: &[u8]) -> Option<WalPosition> {
        self.data.get(k).copied()
    }

    /// If next_entry is None returns first entry.
    ///
    /// If next_entry is not None, returns entry on or after specified next_entry.
    ///
    /// Returns tuple of a key, value and an optional next key if present.
    ///
    /// This works even if next is set to Some(k), but the value at k does not exist (for ex. was deleted).
    /// For this reason, the returned key might be different from the next key requested.
    pub fn next_entry(
        &self,
        next: Option<Bytes>,
        reverse: bool,
    ) -> Option<(Bytes, WalPosition, Option<Bytes>)> {
        fn take_next<'a>(
            mut iter: impl Iterator<Item = (&'a Bytes, &'a WalPosition)>,
        ) -> Option<(Bytes, WalPosition, Option<Bytes>)> {
            let (key, value) = iter.next()?;
            let next_key = iter.next().map(|(k, _v)| k.clone());
            Some((key.clone(), *value, next_key))
        }

        if let Some(next) = next {
            if reverse {
                let range = self.data.range(..=next);
                take_next(range.into_iter().rev())
            } else {
                let range = self.data.range(next..);
                take_next(range.into_iter())
            }
        } else {
            let iterator = self.data.iter();
            if reverse {
                take_next(iterator.rev())
            } else {
                take_next(iterator)
            }
        }
    }

    pub fn last_in_range(
        &self,
        from_included: &Bytes,
        to_included: &Bytes,
    ) -> Option<(Bytes, WalPosition)> {
        let range = self
            .data
            .range::<Bytes, RangeInclusive<&Bytes>>(from_included..=to_included);
        if let Some((bytes, position)) = range.last() {
            Some((bytes.clone(), *position))
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_unmerge_flushed() {
        let mut index = IndexTable::default();
        index.insert(vec![1].into(), WalPosition::test_value(2));
        index.insert(vec![2].into(), WalPosition::test_value(3));
        index.insert(vec![6].into(), WalPosition::test_value(4));
        let mut index2 = index.clone();
        index2.insert(vec![1].into(), WalPosition::test_value(5));
        index2.insert(vec![3].into(), WalPosition::test_value(8));
        assert_eq!(index2.unmerge_flushed(&index), -2);
        let data = index2.data.into_iter().collect::<Vec<_>>();
        assert_eq!(
            data,
            vec![
                (vec![1].into(), WalPosition::test_value(5)),
                (vec![3].into(), WalPosition::test_value(8))
            ]
        );
    }

    #[test]
    fn test_next_entry() {
        let mut table = IndexTable::default();
        table.insert(vec![1, 2, 3, 4].into(), WalPosition::test_value(1));
        table.insert(vec![1, 2, 3, 7].into(), WalPosition::test_value(2));
        table.insert(vec![1, 2, 4, 5].into(), WalPosition::test_value(3));

        // Reverse = false

        // existing element - next found
        let next = table.next_entry(Some(vec![1, 2, 3, 7].into()), false);
        let next = next.unwrap();
        assert_eq!(next.0, Bytes::from(vec![1, 2, 3, 7]));
        assert_eq!(next.1, WalPosition::test_value(2));
        assert_eq!(next.2, Some(vec![1, 2, 4, 5].into()));

        // not existing element - next found
        let next = table.next_entry(Some(vec![1, 2, 3, 6].into()), false);
        let next = next.unwrap();
        assert_eq!(next.0, Bytes::from(vec![1, 2, 3, 7]));
        assert_eq!(next.1, WalPosition::test_value(2));
        assert_eq!(next.2, Some(vec![1, 2, 4, 5].into()));

        // existing element - next not found
        let next = table.next_entry(Some(vec![1, 2, 4, 5].into()), false);
        let next = next.unwrap();
        assert_eq!(next.0, Bytes::from(vec![1, 2, 4, 5]));
        assert_eq!(next.1, WalPosition::test_value(3));
        assert_eq!(next.2, None);

        // not existing element - next not found
        let next = table.next_entry(Some(vec![1, 2, 4, 6].into()), false);
        assert!(next.is_none());

        // Reverse = true

        // existing element - next found
        let next = table.next_entry(Some(vec![1, 2, 3, 7].into()), true);
        let next = next.unwrap();
        assert_eq!(next.0, Bytes::from(vec![1, 2, 3, 7]));
        assert_eq!(next.1, WalPosition::test_value(2));
        assert_eq!(next.2, Some(vec![1, 2, 3, 4].into()));

        // not existing element - next found
        let next = table.next_entry(Some(vec![1, 2, 3, 8].into()), true);
        let next = next.unwrap();
        assert_eq!(next.0, Bytes::from(vec![1, 2, 3, 7]));
        assert_eq!(next.1, WalPosition::test_value(2));
        assert_eq!(next.2, Some(vec![1, 2, 3, 4].into()));

        // existing element - next not found
        let next = table.next_entry(Some(vec![1, 2, 3, 4].into()), true);
        let next = next.unwrap();
        assert_eq!(next.0, Bytes::from(vec![1, 2, 3, 4]));
        assert_eq!(next.1, WalPosition::test_value(1));
        assert_eq!(next.2, None);

        // not existing element - next not found
        let next = table.next_entry(Some(vec![1, 2, 3, 3].into()), true);
        assert!(next.is_none());
    }
}
