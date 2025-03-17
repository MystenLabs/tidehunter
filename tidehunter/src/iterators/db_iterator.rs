use crate::db::{Db, DbResult};
use crate::key_shape::KeySpace;
use minibytes::Bytes;
use std::sync::Arc;

pub struct DbIterator {
    db: Arc<Db>,
    ks: KeySpace,
    next_cell: Option<usize>,
    next_key: Option<Bytes>,
    full_lower_bound: Option<Bytes>,
    full_upper_bound: Option<Bytes>,
    with_key_reduction: bool,
    end_cell_exclusive: Option<usize>,
    reverse: bool,
}

impl DbIterator {
    pub(crate) fn new(db: Arc<Db>, ks: KeySpace) -> Self {
        let with_key_reduction = db.ks(ks).key_reduction().is_some();
        Self {
            db,
            ks,
            next_cell: Some(0),
            next_key: None,
            full_lower_bound: None,
            full_upper_bound: None,
            end_cell_exclusive: None,
            reverse: false,
            with_key_reduction,
        }
    }

    /// Set lower bound(inclusive) for the iterator.
    /// Updating boundaries may reset the iterator.
    pub fn set_lower_bound(&mut self, lower_bound: impl Into<Bytes>) {
        let full_lower_bound = lower_bound.into();
        let ks = self.db.ks(self.ks);
        let reduced_lower_bound = ks.reduced_key_bytes(full_lower_bound.clone());
        if self.reverse {
            self.end_cell_exclusive = ks.next_cell(ks.cell_for_key(&reduced_lower_bound), true);
        } else {
            self.next_cell = Some(ks.cell_for_key(&reduced_lower_bound));
            self.next_key = Some(reduced_lower_bound);
        }
        self.full_lower_bound = Some(full_lower_bound);
    }

    /// Set upper bound(exclusive) for the iterator.
    /// Updating boundaries may reset the iterator.
    pub fn set_upper_bound(&mut self, upper_bound: impl Into<Bytes>) {
        let full_upper_bound = upper_bound.into();
        let ks = self.db.ks(self.ks);
        let reduced_upper_bound = ks.reduced_key_bytes(full_upper_bound.clone());
        if self.reverse {
            let next_key = if self.with_key_reduction {
                reduced_upper_bound
            } else {
                saturated_decrement_vec(&reduced_upper_bound)
            };
            self.next_cell = Some(ks.cell_for_key(&next_key));
            self.next_key = Some(next_key);
        } else {
            self.end_cell_exclusive = ks.next_cell(ks.cell_for_key(&reduced_upper_bound), false);
        }
        self.full_upper_bound = Some(full_upper_bound);
    }

    pub fn reverse(&mut self) {
        self.reverse = !self.reverse;
        if let Some(lower_bound) = self.full_lower_bound.take() {
            self.set_lower_bound(lower_bound);
        }
        if let Some(upper_bound) = self.full_upper_bound.take() {
            self.set_upper_bound(upper_bound);
        }
    }

    fn try_next(&mut self) -> Result<Option<DbResult<(Bytes, Bytes)>>, IteratorAction> {
        let Some(next_cell) = self.next_cell else {
            return Ok(None);
        };
        let next_key = self.next_key.take();
        if let Some(next_key) = &next_key {
            // todo - implement with key reduction to reduce calls to db.next_entry
            // This can be be used as is with key reduction
            // because next_key is a reduced key
            if !self.with_key_reduction {
                self.check_bounds(&next_key)?;
            }
        }
        match self.db.next_entry(
            self.ks,
            next_cell,
            next_key,
            self.end_cell_exclusive,
            self.reverse,
        ) {
            Ok(Some((next_cell, next_key, key, value))) => {
                self.next_cell = next_cell;
                self.next_key = next_key;
                self.check_bounds(&key)?;
                Ok(Some(Ok((key, value))))
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
        } else {
            if let Some(upper_bound) = &self.full_upper_bound {
                key >= upper_bound
            } else {
                false
            }
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
}
