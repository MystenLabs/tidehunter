use crate::index::index_table::IndexTable;
use crate::WalPosition;
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

    /// Apply relocation updates to the index table.
    pub fn apply(self, index: &mut IndexTable) {
        for update in self.updates {
            index.apply_update(&update.reduced_key, |v| {
                if v.offset() < self.last_processed {
                    v.replace_wal_position(update.new_value)
                }
            });
        }
    }

    pub fn last_processed(&self) -> u64 {
        self.last_processed
    }

    pub fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }
}
