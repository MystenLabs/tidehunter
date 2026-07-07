use crate::cell::CellId;
use crate::checkpoint::DbCheckpoint;
use crate::db::{Db, DbResult};
use crate::index::index_format::IndexIterCaches;
use crate::iterators::IteratorResult;
use crate::key_shape::{KeySpace, KeySpaceDesc};
use minibytes::Bytes;
use std::sync::Arc;

/// Backing store a [`DbIterator`] walks: either a live [`Db`] or a
/// point-in-time [`DbCheckpoint`]. Holding the `Arc` keeps the source alive
/// for the iterator's lifetime — for a checkpoint that also pins its latch.
/// `ks`/`next_cell` are identical for both; only `next_entry` differs (the
/// checkpoint filters to its frontier), so the read-mode distinction does not
/// leak into the iterator logic.
pub(crate) enum IterationSource {
    Db(Arc<Db>),
    Checkpoint(Arc<DbCheckpoint>),
}

impl IterationSource {
    fn ks(&self, ks: KeySpace) -> &KeySpaceDesc {
        match self {
            IterationSource::Db(db) => db.ks_desc(ks),
            IterationSource::Checkpoint(cp) => cp.ks(ks),
        }
    }

    fn next_cell(&self, ks: &KeySpaceDesc, cell: &CellId, reverse: bool) -> Option<CellId> {
        match self {
            IterationSource::Db(db) => db.next_cell(ks, cell, reverse),
            IterationSource::Checkpoint(cp) => cp.next_cell(ks, cell, reverse),
        }
    }

    fn next_entry(
        &self,
        ks: KeySpace,
        cell: CellId,
        prev_key: Option<Bytes>,
        end_cell_exclusive: &Option<CellId>,
        reverse: bool,
        cache: &mut IndexIterCaches,
    ) -> DbResult<Option<IteratorResult<Bytes>>> {
        match self {
            IterationSource::Db(db) => {
                db.next_entry(ks, cell, prev_key, end_cell_exclusive, reverse, cache)
            }
            IterationSource::Checkpoint(cp) => {
                cp.next_entry(ks, cell, prev_key, end_cell_exclusive, reverse, cache)
            }
        }
    }
}

pub struct DbIterator {
    source: IterationSource,
    ks: KeySpace,
    cell: Option<CellId>,
    prev_key: Option<Bytes>,
    full_lower_bound: Option<Bytes>,
    full_upper_bound: Option<Bytes>,
    with_key_reduction: bool,
    end_cell_exclusive: Option<CellId>,
    reverse: bool,
    /// Per-level cached index buffers carried across successive `next()`
    /// calls to avoid redundant disk reads when iterating over unloaded
    /// cells. One cache slot per LSM level so k-way merge does not thrash.
    iter_cache: IndexIterCaches,
}

impl DbIterator {
    pub(crate) fn new(source: IterationSource, ks: KeySpace) -> Self {
        let ksd = source.ks(ks);
        let with_key_reduction = ksd.need_check_index_key();
        let cell = ksd.first_cell();
        Self {
            source,
            ks,
            cell: Some(cell),
            prev_key: None,
            full_lower_bound: None,
            full_upper_bound: None,
            end_cell_exclusive: None,
            reverse: false,
            with_key_reduction,
            iter_cache: IndexIterCaches::new(),
        }
    }

    /// Set lower bound(inclusive) for the iterator.
    /// Updating boundaries may reset the iterator.
    pub fn set_lower_bound(&mut self, lower_bound: impl Into<Bytes>) {
        self.iter_cache.clear();
        let full_lower_bound = lower_bound.into();
        let ks = self.source.ks(self.ks);
        ks.assert_supports_iterator_bound();
        let reduced_lower_bound = ks.reduced_key_bytes(full_lower_bound.clone());
        if self.reverse {
            self.end_cell_exclusive =
                self.source
                    .next_cell(ks, &ks.cell_id(&reduced_lower_bound), true);
            // When iterating in reverse with only a lower bound,
            // we need to start from the end if no upper bound has been set yet
            if self.full_upper_bound.is_none() {
                self.cell = Some(ks.last_cell());
                self.prev_key = None;
            }
        } else {
            let next_key = saturated_decrement_vec(&reduced_lower_bound);

            self.cell = Some(ks.cell_id(&next_key));
            self.prev_key = if is_nonzero(&next_key) || is_nonzero(&reduced_lower_bound) {
                Some(next_key)
            } else {
                None
            };
        }
        self.full_lower_bound = Some(full_lower_bound);
    }

    /// Set upper bound(exclusive) for the iterator.
    /// Updating boundaries may reset the iterator.
    pub fn set_upper_bound(&mut self, upper_bound: impl Into<Bytes>) {
        self.iter_cache.clear();
        let full_upper_bound = upper_bound.into();
        let ks = self.source.ks(self.ks);
        ks.assert_supports_iterator_bound();
        let reduced_upper_bound = ks.reduced_key_bytes(full_upper_bound.clone());
        if self.reverse {
            let next_key = if self.with_key_reduction {
                saturated_increment_vec(&reduced_upper_bound)
            } else {
                reduced_upper_bound
            };
            self.cell = Some(ks.cell_id(&next_key));
            self.prev_key = if !self.with_key_reduction || is_nonmax(&next_key) {
                Some(next_key)
            } else {
                None
            };
        } else {
            self.end_cell_exclusive =
                self.source
                    .next_cell(ks, &ks.cell_id(&reduced_upper_bound), false);
        }
        self.full_upper_bound = Some(full_upper_bound);
    }

    pub fn reverse(&mut self) {
        self.iter_cache.clear();
        let ks = self.source.ks(self.ks);
        ks.assert_supports_iterator_bound();
        self.reverse = !self.reverse;
        if self.full_upper_bound.is_none() {
            self.cell = Some(ks.last_cell()); // Initialize current cell as the last cell in ks
        }
        if let Some(lower_bound) = self.full_lower_bound.take() {
            self.set_lower_bound(lower_bound);
        }
        if let Some(upper_bound) = self.full_upper_bound.take() {
            self.set_upper_bound(upper_bound);
        }
    }

    fn try_next(&mut self) -> Result<Option<DbResult<(Bytes, Bytes)>>, IteratorAction> {
        let Some(cell) = self.cell.take() else {
            return Ok(None);
        };
        let prev_key = self.prev_key.take();
        if let Some(prev_key) = &prev_key {
            // todo - implement with key reduction to reduce calls to storage.next_entry
            // This can be be used as is with key reduction
            // because next_key is a reduced key
            if !self.with_key_reduction {
                self.check_bounds(prev_key)?;
            }
        }
        match self.source.next_entry(
            self.ks,
            cell,
            prev_key,
            &self.end_cell_exclusive,
            self.reverse,
            &mut self.iter_cache,
        ) {
            Ok(Some(result)) => {
                self.cell = result.cell;
                let ks = self.source.ks(self.ks);
                self.prev_key = Some(ks.reduced_key_bytes(result.key.clone()));
                self.check_bounds(&result.key)?;
                Ok(Some(Ok((result.key, result.value))))
            }
            Ok(None) => Ok(None),
            Err(err) => Ok(Some(Err(err))),
        }
    }

    fn check_bounds(&self, key: &Bytes) -> Result<(), IteratorAction> {
        if self.with_key_reduction {
            // Need to check both bounds if there is a key reduction,
            // since index can fetch a key outside actual requested bounds
            if self.is_out_of_bound(key, self.reverse) {
                return Err(IteratorAction::Stop);
            }
            if self.is_out_of_bound(key, !self.reverse) {
                Err(IteratorAction::Skip)
            } else {
                Ok(())
            }
        } else {
            // If no key reduction only the end bound needs to be checked
            if self.is_out_of_bound(key, self.reverse) {
                Err(IteratorAction::Stop)
            } else {
                Ok(())
            }
        }
    }

    fn is_out_of_bound(&self, key: &Bytes, reverse: bool) -> bool {
        if reverse {
            if let Some(lower_bound) = &self.full_lower_bound {
                key < lower_bound
            } else {
                false
            }
        } else if let Some(upper_bound) = &self.full_upper_bound {
            key >= upper_bound
        } else {
            false
        }
    }
}

enum IteratorAction {
    Skip,
    Stop,
}

impl Iterator for DbIterator {
    type Item = DbResult<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<DbResult<(Bytes, Bytes)>> {
        loop {
            match self.try_next() {
                Ok(r) => return r,
                Err(IteratorAction::Stop) => return None,
                Err(IteratorAction::Skip) => {}
            }
        }
    }
}

fn saturated_decrement_vec(original_bytes: &[u8]) -> Bytes {
    let mut bytes = original_bytes.to_vec();
    for v in bytes.iter_mut().rev() {
        let sub = v.checked_sub(1);
        if let Some(sub) = sub {
            *v = sub;
            return bytes.into();
        } else {
            *v = 255;
        }
    }
    original_bytes.to_vec().into()
}

fn saturated_increment_vec(original_bytes: &[u8]) -> Bytes {
    let mut bytes = original_bytes.to_vec();
    for v in bytes.iter_mut().rev() {
        let add = v.checked_add(1);
        if let Some(add) = add {
            *v = add;
            return bytes.into();
        } else {
            *v = 0;
        }
    }
    original_bytes.to_vec().into()
}

fn is_nonzero(bytes: &[u8]) -> bool {
    bytes.iter().any(|v| *v != 0)
}

fn is_nonmax(bytes: &[u8]) -> bool {
    bytes.iter().any(|v| *v != 255)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_saturated_decrement_vec() {
        assert_eq!(saturated_decrement_vec(&[1, 2, 3]).as_ref(), &[1, 2, 2]);
        assert_eq!(saturated_decrement_vec(&[1, 2, 0]).as_ref(), &[1, 1, 255]);
        assert_eq!(saturated_decrement_vec(&[1, 0, 0]).as_ref(), &[0, 255, 255]);
        assert_eq!(saturated_decrement_vec(&[0, 0, 0]).as_ref(), &[0, 0, 0]);
    }

    #[test]
    fn test_saturated_increment_vec() {
        assert_eq!(saturated_increment_vec(&[1, 2, 3]).as_ref(), &[1, 2, 4]);
        assert_eq!(saturated_increment_vec(&[1, 2, 255]).as_ref(), &[1, 3, 0]);
        assert_eq!(
            saturated_increment_vec(&[255, 255, 255]).as_ref(),
            &[255, 255, 255]
        );
    }
}
