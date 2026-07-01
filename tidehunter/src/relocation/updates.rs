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

struct RelocationUpdate {
    reduced_key: Bytes,
    new_value: WalPosition,
}

impl RelocationUpdates {
    pub fn new(last_processed: u64) -> Self {
        Self {
            last_processed,
            updates: Default::default(),
        }
    }

    pub fn add(&mut self, reduced_key: Bytes, new_value: WalPosition) {
        self.updates.push(RelocationUpdate {
            reduced_key,
            new_value,
        });
    }

    /// Apply relocation updates to the index table. Returns whether any
    /// position was re-pointed: an update whose key is absent, or whose
    /// latest position already sits at or above the relocation frontier,
    /// leaves the table untouched.
    ///
    /// Compare-and-set: an update re-points an entry only if it still holds
    /// the pre-relocation position — a main-WAL (G0) offset below the
    /// relocation frontier. `last_processed` is a G0 offset, always below
    /// the generation flag bit, so the raw comparison also rejects every
    /// flagged position by construction: an entry already re-pointed to a
    /// side WAL (by this pass reading a stale snapshot, or by a previous
    /// pass) is never demoted back. This is intended — the comparison
    /// doubles as the generation check.
    pub fn apply(self, index: &mut IndexTable) -> bool {
        let mut changed = false;
        for update in self.updates {
            index.apply_update(&update.reduced_key, |v| {
                if v.offset() < self.last_processed {
                    v.replace_wal_position(update.new_value);
                    changed = true;
                }
            });
        }
        changed
    }

    /// The `(reduced_key, new_position)` pairs, for callers that split the
    /// updates across the levels holding their keys.
    pub fn into_entries(self) -> impl Iterator<Item = (Bytes, WalPosition)> {
        self.updates
            .into_iter()
            .map(|update| (update.reduced_key, update.new_value))
    }

    pub fn last_processed(&self) -> u64 {
        self.last_processed
    }

    pub fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }
}
