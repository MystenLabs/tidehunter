use crate::cell::CellId;
use crate::key_shape::{KeyShape, KeySpaceDesc, KeyType};
use crate::large_table::SnapshotEntryData;
use crate::wal::position::WalPosition;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Container for snapshot data: ks -> cell (mutex-independent)
/// Vec index corresponds to keyspace (same as keyspace_names Vec)
/// CellId is scoped within each keyspace
#[derive(Serialize, Deserialize, Debug)]
#[doc(hidden)] // Used by tools/tideconsole for control region inspection
pub struct LargeTableContainer<T> {
    // Vec[ks_idx] = BTreeMap of all cells in that keyspace
    pub data: Vec<BTreeMap<CellId, T>>,
}

impl<T: Clone> LargeTableContainer<T> {
    /// Creates a new container with the given shape and filled with clones of the passed value
    pub fn new_from_key_shape(key_shape: &KeyShape, value: T) -> Self {
        let data = key_shape
            .iter_ks()
            .map(|ks| Self::new_keyspace(ks, value.clone()))
            .collect();
        Self { data }
    }

    pub(crate) fn new_keyspace(ks: &KeySpaceDesc, value: T) -> BTreeMap<CellId, T> {
        match ks.key_type() {
            KeyType::Uniform(config) => {
                // Pre-populate all integer cells for uniform keyspaces
                let total_cells = ks.num_mutexes() * config.cells_per_mutex();
                (0..total_cells)
                    .map(|cell_idx| {
                        let row = cell_idx / config.cells_per_mutex();
                        let offset = cell_idx % config.cells_per_mutex();
                        (
                            CellId::Integer(ks.cell_by_location(row, offset)),
                            value.clone(),
                        )
                    })
                    .collect()
            }
            KeyType::PrefixedUniform(_) => BTreeMap::new(),
        }
    }
}

impl<T> LargeTableContainer<T> {
    pub fn iter_cells(&self) -> impl Iterator<Item = &T> {
        self.data.iter().flat_map(|ks_map| ks_map.values())
    }
}

impl LargeTableContainer<SnapshotEntryData> {
    /// Iterates every on-disk index-blob position across every cell, in
    /// keyspace-then-cell order. Level-generic: a multi-level cell yields
    /// one position per level.
    pub fn iter_valid_val_positions(&self) -> impl Iterator<Item = WalPosition> + '_ {
        self.iter_cells()
            .flat_map(|entry| entry.levels().iter().map(|index_ref| index_ref.position()))
    }
}
