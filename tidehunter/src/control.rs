use crate::WalPosition;
use crate::key_shape::KeyShape;
use crate::large_table::{LargeTableContainer, SnapshotEntryData};
use crate::metrics::Metrics;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize, Debug)]
#[doc(hidden)] // Used by tools/wal_inspector for control region inspection
pub struct ControlRegion {
    /// 0 when wal is empty or nothing has been processed
    last_position: u64,
    snapshot: LargeTableContainer<SnapshotEntryData>,
    /// Keyspace names in order - used to verify that keyspaces haven't been reordered
    /// or inserted in the middle. Only new keyspaces can be added at the end.
    #[serde(default)]
    keyspace_names: Vec<String>,
}

pub(crate) struct ControlRegionStore {
    path: PathBuf,
    last_position: u64,
    force_relocation_position: Option<WalPosition>,
}

impl ControlRegion {
    pub fn new_empty(key_shape: &KeyShape) -> Self {
        let snapshot =
            LargeTableContainer::new_from_key_shape(key_shape, SnapshotEntryData::empty());
        let keyspace_names = key_shape
            .iter_ks()
            .map(|ks| ks.name().to_string())
            .collect();
        Self {
            last_position: 0,
            snapshot,
            keyspace_names,
        }
    }

    pub fn new(
        snapshot: LargeTableContainer<SnapshotEntryData>,
        last_position: u64,
        key_shape: &KeyShape,
    ) -> Self {
        let keyspace_names = key_shape
            .iter_ks()
            .map(|ks| ks.name().to_string())
            .collect();
        Self {
            snapshot,
            last_position,
            keyspace_names,
        }
    }

    pub fn read_or_create(path: &Path, key_shape: &KeyShape) -> Self {
        match Self::read(path, key_shape) {
            Ok(control_region) => control_region,
            Err(err) if err.kind() == ErrorKind::NotFound => ControlRegion::new_empty(key_shape),
            Err(err) => {
                panic!("Failed to read control region file: {err:?}")
            }
        }
    }

    #[doc(hidden)] // Used by tools/wal_inspector for control region inspection
    pub fn read(path: &Path, key_shape: &KeyShape) -> io::Result<Self> {
        let bytes = fs::read(path)?;
        let mut control_region: ControlRegion =
            bincode::deserialize(&bytes).map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
        control_region.populate_keyspace_names_if_empty(key_shape);
        control_region.verify_shape(key_shape);
        control_region.extend_snapshot_if_needed(key_shape);
        Ok(control_region)
    }

    /// Populates keyspace names from key_shape if they're empty (for old control regions)
    fn populate_keyspace_names_if_empty(&mut self, key_shape: &KeyShape) {
        if self.keyspace_names.is_empty() {
            // This is an old control region without keyspace names
            // Populate names for existing keyspaces from the current key_shape
            self.keyspace_names = key_shape
                .iter_ks()
                .take(self.snapshot.0.len())
                .map(|ks| ks.name().to_string())
                .collect();
        }
    }

    /// Extends the snapshot to include new keyspaces if the key_shape has more keyspaces
    fn extend_snapshot_if_needed(&mut self, key_shape: &KeyShape) {
        let snapshot_len = self.snapshot.0.len();
        let num_ks = key_shape.num_ks();

        if snapshot_len < num_ks {
            // Add empty entries for new keyspaces
            for ks in key_shape.iter_ks().skip(snapshot_len) {
                let new_ks_snapshot =
                    LargeTableContainer::new_keyspace(ks, SnapshotEntryData::empty());
                self.snapshot.0.push(new_ks_snapshot);
                self.keyspace_names.push(ks.name().to_string());
            }
        }
    }

    fn verify_shape(&self, key_shape: &KeyShape) {
        let snapshot_len = self.snapshot.0.len();
        let num_ks = key_shape.num_ks();

        // We allow adding keyspaces at the end, but not removing or reordering
        if snapshot_len > num_ks {
            panic!(
                "Control region has {snapshot_len} key spaces, while configuration has {num_ks}. Removing key spaces is not supported"
            );
        }

        // Verify that existing keyspaces match by name in the same order
        // This ensures that keyspaces are only added at the end, not inserted in the middle
        for (idx, ks_desc) in key_shape.iter_ks().enumerate().take(snapshot_len) {
            let current_name = ks_desc.name();

            // Check if we have stored keyspace names (backward compatibility: might be empty for old control regions)
            if idx < self.keyspace_names.len() {
                let stored_name = &self.keyspace_names[idx];
                if current_name != stored_name {
                    panic!(
                        "Keyspace mismatch at position {idx}: control region has keyspace '{stored_name}', \
                         but configuration has '{current_name}'. \
                         Reordering or renaming keyspaces is not supported. \
                         Only adding new keyspaces at the end is allowed."
                    );
                }
            }
        }
    }

    pub fn snapshot(&self) -> &LargeTableContainer<SnapshotEntryData> {
        &self.snapshot
    }

    pub fn last_position(&self) -> u64 {
        self.last_position
    }

    pub fn last_index_wal_position(&self) -> Option<WalPosition> {
        self.snapshot.iter_valid_val_positions().max()
    }
}

const FORCE_RELOCATION_PCT: usize = 99;

impl ControlRegionStore {
    pub fn new(path: PathBuf, control_region: &ControlRegion) -> Self {
        Self {
            path,
            last_position: control_region.last_position,
            force_relocation_position: control_region
                .snapshot
                .pct_wal_position(FORCE_RELOCATION_PCT),
        }
    }

    pub fn store(
        &mut self,
        snapshot: LargeTableContainer<SnapshotEntryData>,
        last_position: u64,
        key_shape: &KeyShape,
        metrics: &Metrics,
    ) {
        let force_relocation_position = snapshot.pct_wal_position(FORCE_RELOCATION_PCT);
        let control_region = ControlRegion::new(snapshot, last_position, key_shape);
        let serialized =
            bincode::serialize(&control_region).expect("Failed to serialize control region");
        let temp_file = self.path.with_extension(".bak");
        fs::write(&temp_file, &serialized).expect("Failed to write control region file");
        metrics
            .snapshot_written_bytes
            .inc_by(serialized.len() as u64);
        fs::rename(&temp_file, &self.path).expect("Failed to rename control region file");
        self.last_position = last_position;
        self.force_relocation_position = force_relocation_position;
    }

    /// The path to the control region file
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn last_position(&self) -> u64 {
        self.last_position
    }

    pub fn force_relocation_position(&self) -> Option<WalPosition> {
        self.force_relocation_position
    }
}
