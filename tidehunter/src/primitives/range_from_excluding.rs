use std::collections::{BTreeMap, Bound};
use std::ops::RangeBounds;

/// Range that excludes starting point
pub struct RangeFromExcluding<'a, T> {
    from: &'a T,
}

impl<'a, T> RangeBounds<T> for RangeFromExcluding<'a, T> {
    fn start_bound(&self) -> Bound<&T> {
        Bound::Excluded(self.from)
    }

    fn end_bound(&self) -> Bound<&T> {
        Bound::Unbounded
    }
}

/// Returns next element in the tree in the given direction
pub fn next_key_in_tree<'a, K: Ord, V>(
    tree: &'a BTreeMap<K, V>,
    key: &K,
    reverse: bool,
) -> Option<&'a K> {
    if reverse {
        tree.range::<K, _>(..key).next_back().map(|(k, _v)| k)
    } else {
        tree.range::<K, _>(RangeFromExcluding { from: key })
            .next()
            .map(|(k, _v)| k)
    }
}

#[test]
fn test_next_key_in_tree() {
    let tree: BTreeMap<usize, ()> = [(1, ()), (5, ()), (8, ())].into_iter().collect();

    // forward
    assert_eq!(Some(&1), next_key_in_tree(&tree, &0, false));
    assert_eq!(Some(&5), next_key_in_tree(&tree, &1, false));
    assert_eq!(None, next_key_in_tree(&tree, &8, false));

    // reverse
    assert_eq!(Some(&1), next_key_in_tree(&tree, &2, true));
    assert_eq!(Some(&1), next_key_in_tree(&tree, &5, true));
    assert_eq!(Some(&5), next_key_in_tree(&tree, &6, true));
    assert_eq!(None, next_key_in_tree(&tree, &1, true));
}
