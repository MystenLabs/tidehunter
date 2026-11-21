use crate::WalPosition;
use crate::index::index_table::IndexTable;
use minibytes::Bytes;

/// Holds wal positions that were relocated.
/// Updates are essentially compare-and-set commands that modify values in
/// the index if they are the same as they were when update was created.
pub struct RelocationUpdates {
    last_processed: u64,
    updates: Vec<RelocationUpdate>,
}

/// One compare-and-set: re-point `reduced_key` to `new_value` if its entry
/// still holds the position the copy was made from.
pub(crate) struct RelocationUpdate {
    reduced_key: Bytes,
    /// The exact position the copy was made from, for a copy of a record
    /// that already sits in a side WAL. `None` for a copy of a main-WAL
    /// record, which is identified by the relocation frontier instead.
    old_value: Option<WalPosition>,
    new_value: WalPosition,
}

impl RelocationUpdate {
    pub(crate) fn reduced_key(&self) -> &Bytes {
        &self.reduced_key
    }
}

impl RelocationUpdates {
    pub fn new(last_processed: u64) -> Self {
        Self {
            last_processed,
            updates: Default::default(),
        }
    }

    /// The copy of a main-WAL record below the relocation frontier.
    pub fn add(&mut self, reduced_key: Bytes, new_value: WalPosition) {
        self.updates.push(RelocationUpdate {
            reduced_key,
            old_value: None,
            new_value,
        });
    }

    /// The copy of the side-WAL record at `old_value`.
    pub fn add_exact(
        &mut self,
        reduced_key: Bytes,
        old_value: WalPosition,
        new_value: WalPosition,
    ) {
        self.updates.push(RelocationUpdate {
            reduced_key,
            old_value: Some(old_value),
            new_value,
        });
    }

    pub(crate) fn push(&mut self, update: RelocationUpdate) {
        self.updates.push(update);
    }

    /// Apply relocation updates to the index table. Returns whether any
    /// position was re-pointed: an update whose key is absent, or whose
    /// entry no longer holds the position the copy was made from, leaves
    /// the table untouched.
    ///
    /// Compare-and-set, in one of two forms:
    ///
    /// - The copy of a main-WAL record re-points an entry only if it still
    ///   holds a main-WAL (G0) offset below the relocation frontier.
    ///   `last_processed` is a G0 offset, always below the generation flag
    ///   bit, so the raw comparison also rejects every flagged position by
    ///   construction: an entry already re-pointed to a side WAL (by this
    ///   pass reading a stale snapshot, or by a previous pass) is never
    ///   demoted back. This is intended — the comparison doubles as the
    ///   generation check.
    /// - The copy of a side-WAL record (side-WAL reclamation) re-points an
    ///   entry only if it still holds exactly the position the copy was made
    ///   from. A key written since holds a G0 position, which never equals
    ///   a flagged one.
    pub fn apply(self, index: &mut IndexTable) -> bool {
        let mut changed = false;
        for update in self.updates {
            index.apply_update(&update.reduced_key, |v| {
                let holds_original = match update.old_value {
                    Some(old_value) => v.offset() == old_value.offset(),
                    None => v.offset() < self.last_processed,
                };
                if holds_original {
                    v.replace_wal_position(update.new_value);
                    changed = true;
                }
            });
        }
        changed
    }

    /// The updates, for callers that split them across the levels holding
    /// their keys.
    pub(crate) fn into_updates(self) -> impl Iterator<Item = RelocationUpdate> {
        self.updates.into_iter()
    }

    pub fn last_processed(&self) -> u64 {
        self.last_processed
    }

    pub fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }
}
