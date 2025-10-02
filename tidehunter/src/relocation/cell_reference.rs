use crate::cell::CellId;
use crate::db::Db;
use crate::key_shape::KeySpace;
use crate::WalPosition;
use minibytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellReference {
    pub keyspace: KeySpace,
    pub cell_id: CellId,
}

impl CellReference {
    /// Get the first cell reference for a given keyspace
    pub fn first(db: &Db, keyspace: KeySpace) -> Option<Self> {
        if keyspace.as_usize() >= db.key_shape.num_ks() {
            return None;
        }

        let context = db.ks_context(keyspace);
        let ks_desc = &context.ks_config;
        let first_cell = ks_desc.first_cell();

        Some(CellReference {
            keyspace,
            cell_id: first_cell,
        })
    }

    /// Get the next cell reference, handling keyspace boundaries
    pub fn next(&self, db: &Db) -> Option<Self> {
        if let Some(cell) = self.next_in_ks(db) {
            return Some(cell);
        }
        // No more cells in current keyspace, move to next keyspace
        let mut next_keyspace = self.keyspace;
        next_keyspace.increment();

        Self::first(db, next_keyspace)
    }

    pub fn next_in_ks(&self, db: &Db) -> Option<Self> {
        db.large_table
            .next_cell(db.ks_context(self.keyspace), &self.cell_id, false)
            .map(|next_cell| CellReference {
                keyspace: self.keyspace,
                cell_id: next_cell,
            })
    }
}

#[derive(Debug)]
pub(crate) struct CellProcessingContext {
    pub entries_to_relocate: Vec<(Bytes, Bytes, WalPosition)>, // key, value, original position
    pub highest_wal_position: WalPosition,
    pub entries_removed: u64,
    pub entries_kept: u64,
}

impl CellProcessingContext {
    pub fn new() -> Self {
        Self {
            entries_to_relocate: Vec::new(),
            highest_wal_position: WalPosition::new(0, 0),
            entries_removed: 0,
            entries_kept: 0,
        }
    }

    pub fn add_entry_to_relocate(&mut self, key: Bytes, value: Bytes, position: WalPosition) {
        self.entries_to_relocate.push((key, value, position));
        self.entries_kept += 1;
        if position.offset() > self.highest_wal_position.offset() {
            self.highest_wal_position = position;
        }
    }

    pub fn mark_entry_removed(&mut self, position: WalPosition) {
        self.entries_removed += 1;
        if position.offset() > self.highest_wal_position.offset() {
            self.highest_wal_position = position;
        }
    }
}
