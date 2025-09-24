use crate::cell::CellId;
use crate::db::Db;
use crate::key_shape::{KeySpace, KeySpaceDesc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct CellReference {
    pub keyspace_desc: KeySpaceDesc,
    pub cell_id: CellId,
}

impl CellReference {
    /// Get the first cell reference for a given keyspace
    pub fn first(db: &Db, keyspace: KeySpace) -> Option<Self> {
        if keyspace.as_usize() >= db.key_shape.num_ks() {
            return None;
        }

        let context = db.ks_context(keyspace);
        let ks_desc = &(*context).ks_config;
        let first_cell = ks_desc.first_cell();

        Some(CellReference {
            keyspace_desc: ks_desc.clone(),
            cell_id: first_cell,
        })
    }

    /// Get the next cell reference, handling keyspace boundaries
    pub fn next(&self, db: &Db) -> Option<Self> {
        let context = db.ks_context(self.keyspace_desc.id());

        // Try to get next cell in current keyspace
        if let Some(next_cell) = db.large_table.next_cell(context, &self.cell_id, false) {
            return Some(CellReference {
                keyspace_desc: self.keyspace_desc.clone(),
                cell_id: next_cell,
            });
        }

        // No more cells in current keyspace, move to next keyspace
        let mut next_keyspace = self.keyspace_desc.id();
        next_keyspace.increment();

        Self::first(db, next_keyspace)
    }
}
