use crate::primitives::range_from_excluding::RangeFromExcluding;
use crate::wal::WalPosition;
use minibytes::Bytes;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet};

#[derive(Default, Clone, Debug)]
#[doc(hidden)]
pub struct IndexTable {
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

    /// If prev is None returns first entry.
    ///
    /// If prev is not None, returns entry after specified prev.
    ///
    /// Returns tuple of a key, value.
    ///
    /// This works even if prev is set to Some(k), but the value at k does not exist (for ex. was deleted).
    pub fn next_entry(&self, prev: Option<Bytes>, reverse: bool) -> Option<(Bytes, WalPosition)> {
        if let Some(prev) = prev {
            if reverse {
                let range = self.data.range(..prev);
                range
                    .into_iter()
                    .next_back()
                    .map(|(key, value)| (key.clone(), *value))
            } else {
                let range = RangeFromExcluding { from: &prev };
                let range = self.data.range(range);
                range
                    .into_iter()
                    .next()
                    .map(|(key, value)| (key.clone(), *value))
            }
        } else {
            let mut iterator = self.data.iter();
            if reverse {
                iterator
                    .next_back()
                    .map(|(key, value)| (key.clone(), *value))
            } else {
                iterator.next().map(|(key, value)| (key.clone(), *value))
            }
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
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
}
