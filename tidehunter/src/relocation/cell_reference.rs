use crate::cell::CellId;
use crate::db::Db;
use crate::key_shape::KeySpace;
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
