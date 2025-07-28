use std::sync::Arc;

use minibytes::Bytes;

use crate::cell::CellId;
use crate::{
    db::{Db, DbResult},
    iterators::IteratorResult,
    key_shape::KeySpace,
    WalPosition,
};

pub struct DefaultRelocator {
    db: Arc<Db>,
    ks: KeySpace,
    first_wal_position: WalPosition,
    threshold: u64,
    keys_to_relocate: Vec<(Bytes, WalPosition)>,
}

impl DefaultRelocator {
    pub fn new(db: Arc<Db>, ks: KeySpace, first_wal_position: WalPosition, threshold: u64) -> Self {
        DefaultRelocator {
            db,
            ks,
            first_wal_position,
            threshold,
            keys_to_relocate: Vec::with_capacity(threshold as usize),
        }
    }

    /// NOTE: If this function crashes halfway through, the keys that were relocated will be relocated again
    /// when the next relocation is triggered.
    pub fn relocate(&mut self) -> DbResult<()> {
        let ksd = self.db.ks(self.ks).clone();
        let mut next_cell = Some(ksd.first_cell());

        // Collect keys from all cells.
        while let Some(cell) = next_cell {
            // Collect keys within the current cell.
            self.collect_entries_within_cell(cell.clone())?;

            // Relocate the collected keys.
            for (key, wal_position) in self.keys_to_relocate.drain(..) {
                // Check with the application if the key should be relocated.
                // TODO

                let (_, value) = self.db.read_record(wal_position)?;
                // TODO: Before inserting this entry, we should check that there are no race conditions where
                // the application has overwritten the key in the meantime.
                self.db.insert(self.ks, key, value)?;
            }

            // Move to the next cell.
            next_cell = self.db.next_cell(&ksd, &cell, false);
        }

        // Truncate the WAL to remove relocated entries.
        // TODO

        Ok(())
    }

    fn collect_entries_within_cell(&mut self, cell: CellId) -> DbResult<()> {
        let mut prev_key = None;
        let end_cell_exclusive = Some(cell.clone()); // Only iterate within this cell.

        // TODO: this is naive, it acquires a lock over the row at every iteration.
        while let Some(result) = self.db.next_entry_position(
            self.ks,
            cell.clone(),
            prev_key,
            &end_cell_exclusive,
            false,
        )? {
            let IteratorResult {
                key,
                value: wal_position,
                ..
            } = result;

            // If the WAL position is below the threshold, we can relocate the entry.
            if wal_position.offset() <= self.first_wal_position.offset() + self.threshold {
                self.keys_to_relocate.push((key.clone(), wal_position));
            }

            prev_key = Some(key);
        }

        Ok(())
    }
}
