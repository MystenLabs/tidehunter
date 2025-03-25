use std::collections::{BTreeMap, HashMap};

/// Allows inserting values and pop oldest inserted value.
///
/// This is almost like a binary heap, but inserting value again
/// pushes the value to the end of the queue.
#[allow(dead_code)]
#[derive(Default)]
pub struct Lru {
    pos: u64,
    map: HashMap<u64, u64>,
    rmap: BTreeMap<u64, u64>,
}

#[allow(dead_code)]
impl Lru {
    /// Insert entry in Lru at the last position.
    pub fn insert(&mut self, id: u64) {
        let prev_pos = self.map.insert(id, self.pos);
        if let Some(prev_pos) = prev_pos {
            self.rmap.remove(&prev_pos);
        }
        self.rmap.insert(self.pos, id);
        debug_assert_eq!(self.map.len(), self.rmap.len());
        self.pos += 1;
    }

    /// Remove entry returning its position if it was present in Lru.
    pub fn remove(&mut self, id: u64) -> Option<u64> {
        let prev_pos = self.map.remove(&id)?;
        let rmap_id = self.rmap.remove(&prev_pos);
        debug_assert_eq!(Some(id), rmap_id);
        Some(prev_pos)
    }

    /// Pops first entry from Lru.
    pub fn pop(&mut self) -> Option<u64> {
        self.pop_when(|_| true)
    }

    /// Pops first entry matching the predicate from Lru.
    pub fn pop_when<F: FnMut(u64) -> bool>(&mut self, mut f: F) -> Option<u64> {
        let mut to_remove = None;
        for (pos, id) in &self.rmap {
            if f(*id) {
                to_remove = Some((*pos, *id));
                break;
            }
        }
        if let Some((pos, id)) = to_remove {
            let rmap_id = self.rmap.remove(&pos);
            debug_assert_eq!(Some(id), rmap_id);
            let map_pos = self.map.remove(&id);
            debug_assert_eq!(Some(pos), map_pos);
            Some(id)
        } else {
            None
        }
    }
}

#[test]
fn test_lru() {
    let mut lru = Lru::default();
    lru.insert(1);
    lru.insert(2);
    lru.insert(3);
    lru.insert(1);
    assert_eq!(lru.pop(), Some(2));
    assert_eq!(lru.pop(), Some(3));
    assert_eq!(lru.pop(), Some(1));
    assert_eq!(lru.pop(), None);
}
