use crate::key_shape::KeySpaceDesc;
use crate::primitives::cursor::SliceCursor;
use crate::primitives::range_from_excluding::RangeFromExcluding;
use crate::primitives::slice_buf::SliceBuf;
use crate::primitives::var_int::{deserialize_u16_varint, serialize_u16_varint, MAX_U16_VARINT};
use crate::wal::WalPosition;
use bytes::{Buf, BufMut, BytesMut};
use minibytes::Bytes;
use std::collections::btree_map::{Entry, Keys};
use std::collections::BTreeMap;

#[derive(Default, Clone, Debug)]
#[doc(hidden)]
pub struct IndexTable {
    data: BTreeMap<Bytes, IndexWalPosition>,
}

#[derive(Copy, Clone, Debug)]
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

impl IndexTable {
    pub fn insert(&mut self, k: Bytes, v: WalPosition) -> Option<WalPosition> {
        self.checked_insert(k, IndexWalPosition::new_modified(v))
    }

    pub fn remove(&mut self, k: Bytes, v: WalPosition) -> Option<WalPosition> {
        self.checked_insert(k, IndexWalPosition::new_removed(v))
    }

    fn checked_insert(&mut self, k: Bytes, v: IndexWalPosition) -> Option<WalPosition> {
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
            if v.is_removed() {
                self.data.remove(k);
            } else {
                self.data.insert(k.clone(), v.as_clean_modified());
            }
        }
    }

    /// Merges dirty IndexTable into a loaded IndexTable, preserving dirty states
    pub fn merge_dirty_no_clean(&mut self, dirty: &Self) {
        // todo implement this efficiently taking into account both self and dirty are sorted
        for (k, v) in dirty.data.iter() {
            self.data.insert(k.clone(), *v);
        }
    }

    /// Remove flushed index entries that have offset <= last_processed, returning number of entries changed
    pub fn unmerge_flushed(&mut self, original: &Self, last_processed: u64) -> i64 {
        let mut delta = 0i64;
        for (k, v) in original.data.iter() {
            // Only unmerge entries that were actually flushed (offset <= last_processed)
            if v.offset <= last_processed {
                let entry = self.data.entry(k.clone());
                match entry {
                    Entry::Vacant(_) => {
                        // todo clarify how this is possible
                    }
                    Entry::Occupied(oc) => {
                        if oc.get().into_wal_position() == v.into_wal_position() {
                            oc.remove();
                            delta -= 1;
                        }
                    }
                }
            }
        }
        delta
    }

    /// Count the number of dirty(modified or removed) wal positions in this index.
    /// This information is not cached and causes iteration over the entire index table.
    pub fn count_dirty(&self) -> usize {
        self.data.values().filter(|p| !p.is_clean()).count()
    }

    /// Change loaded dirty IndexTable into unloaded dirty by retaining dirty keys and tombstones
    /// Returns delta in number of entries
    pub fn retain_dirty(&mut self) -> i64 {
        let original_data_len = self.data.len() as i64;
        self.data.retain(|_, pos| !pos.is_clean());
        let data_len = self.data.len() as i64;
        data_len - original_data_len
    }

    pub fn get(&self, k: &[u8]) -> Option<WalPosition> {
        self.data.get(k).map(|p| p.into_wal_position())
    }

    /// similar to `get`, but returns the position of the modification operation for both deletes and inserts
    pub fn get_update_position(&self, k: &[u8]) -> Option<WalPosition> {
        self.data.get(k).map(|p| p.into_update_position())
    }

    /// If prev is None returns first entry.
    ///
    /// If prev is not None, returns entry after specified prev.
    ///
    /// Returns tuple of a key, value.
    ///
    /// This works even if prev is set to Some(k), but the value at k does not exist (for ex. was deleted).
    pub fn next_entry(&self, prev: Option<Bytes>, reverse: bool) -> Option<(Bytes, WalPosition)> {
        fn next<'a>(
            mut it: impl Iterator<Item = (&'a Bytes, &'a IndexWalPosition)> + 'a,
        ) -> Option<(Bytes, WalPosition)> {
            it.next()
                .map(|(key, value)| (key.clone(), value.into_wal_position()))
        }
        if let Some(prev) = prev {
            if reverse {
                next(self.data.range(..prev).rev())
            } else {
                let range = RangeFromExcluding { from: &prev };
                next(self.data.range(range))
            }
        } else {
            let iterator = self.data.iter();
            if reverse {
                next(iterator.rev())
            } else {
                next(iterator)
            }
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn keys(&self) -> Keys<Bytes, IndexWalPosition> {
        self.data.keys()
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
        // Write each key-value pair
        for (key, value) in self.data.iter() {
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
            value.ensure_clean_wal_position().write_to_buf(out);
        }
    }

    /// Deserializes IndexTable from bytes
    /// - data_offset: Where actual data begins (after any headers)
    /// - ks: KeySpaceDesc to determine element sizes
    /// - b: Source bytes
    ///   Returns the deserialized IndexTable
    pub fn deserialize_index_entries(ks: &KeySpaceDesc, bytes: Bytes) -> Self {
        let mut bytes = SliceBuf::new(bytes);
        let mut data = BTreeMap::new();
        while bytes.has_remaining() {
            let key = if let Some(key_len) = ks.index_key_size() {
                bytes.slice_n(key_len)
            } else {
                let key_len = deserialize_u16_varint(&mut bytes);
                bytes.slice_n(key_len as usize)
            };
            let value = WalPosition::read_from_buf(&mut bytes);
            if data
                .insert(key, IndexWalPosition::new_clean(value))
                .is_some()
            {
                panic!("Duplicate keys detected in index");
            }
        }
        IndexTable { data }
    }

    /// Marks all elements in this index as clean and remove tombstones
    pub fn clean_self(&mut self) {
        self.data.retain(|_k, v| match v.kind {
            IndexEntryKind::Clean => true,
            IndexEntryKind::Modified => {
                *v = v.as_clean_modified();
                true
            }
            IndexEntryKind::Removed => false,
        });
    }

    /// Retain only entries with offset > last_processed, returning number of entries removed
    pub fn retain_above_position(&mut self, last_processed: u64) -> i64 {
        let original_len = self.data.len();
        self.data.retain(|_k, v| v.offset > last_processed);
        let removed = original_len - self.data.len();
        -(removed as i64)
    }

    // todo compactor API should change so that we don't have to expose IndexWalPosition
    /// Accessor for self.data, only to be used for the compaction callback.
    pub fn data_for_compaction(&mut self) -> &mut BTreeMap<Bytes, IndexWalPosition> {
        &mut self.data
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
            len: w.len_u32(),
            kind: IndexEntryKind::Clean,
        }
    }

    fn new_modified(w: WalPosition) -> Self {
        debug_assert_ne!(w, WalPosition::INVALID);
        Self {
            offset: w.offset(),
            len: w.len_u32(),
            kind: IndexEntryKind::Modified,
        }
    }

    fn new_removed(w: WalPosition) -> Self {
        debug_assert_ne!(w, WalPosition::INVALID);
        Self {
            offset: w.offset(),
            len: w.len_u32(),
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
        let index = IndexTable { data };
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
        assert_eq!(index2.unmerge_flushed(&index, u64::MAX), -2);
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

        // With last_processed=3, only entries at positions 2 and 3 should be unmerged
        assert_eq!(index2.unmerge_flushed(&index, 3), -1);
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
        let delta = index.retain_above_position(250);
        assert_eq!(delta, -2); // Removed 2 entries

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
}
