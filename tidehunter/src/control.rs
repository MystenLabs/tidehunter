use crate::key_shape::KeyShape;
use crate::large_table::{LargeTableContainer, SnapshotEntryData};
use crate::metrics::Metrics;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct ControlRegion {
    /// 0 when wal is empty or nothing has been processed
    last_position: u64,
    snapshot: LargeTableContainer<SnapshotEntryData>,
}

pub(crate) struct ControlRegionStore {
    path: PathBuf,
}

impl ControlRegion {
    pub fn new_empty(key_shape: &KeyShape) -> Self {
        let snapshot =
            LargeTableContainer::new_from_key_shape(key_shape, SnapshotEntryData::empty());
        Self {
            last_position: 0,
            snapshot,
        }
    }

    pub fn new(snapshot: LargeTableContainer<SnapshotEntryData>, last_position: u64) -> Self {
        Self {
            snapshot,
            last_position,
        }
    }

    pub fn read_or_create(path: &Path, key_shape: &KeyShape) -> Self {
        let bytes = fs::read(path);
        let control_region: ControlRegion = match bytes {
            Err(err) => {
                if err.kind() == ErrorKind::NotFound {
                    return ControlRegion::new_empty(key_shape);
                } else {
                    panic!("{1}: {:?}", err, "Failed to read control region file")
                }
            }
            Ok(bytes) => {
                bincode::deserialize(&bytes).expect("Failed to deserialize control region")
            }
        };
        control_region.verify_shape(key_shape);
        control_region
    }

    fn verify_shape(&self, key_shape: &KeyShape) {
        let snapshot_len = self.snapshot.0.len();
        let num_ks = key_shape.num_ks();
        if snapshot_len != num_ks {
            panic!("Control region has {snapshot_len} key spaces, while configuration has {num_ks}. Re-configuration is not currently supported");
        }
        // todo more verifications for the key shape
    }

    pub fn snapshot(&self) -> &LargeTableContainer<SnapshotEntryData> {
        &self.snapshot
    }

    pub fn last_position(&self) -> u64 {
        self.last_position
    }
}

impl ControlRegionStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
    pub fn store(
        &mut self,
        snapshot: LargeTableContainer<SnapshotEntryData>,
        last_position: u64,
        metrics: &Metrics,
    ) {
        let control_region = ControlRegion::new(snapshot, last_position);
        let serialized =
            bincode::serialize(&control_region).expect("Failed to serialize control region");
        let temp_file = self.path.with_extension(".bak");
        fs::write(&temp_file, &serialized).expect("Failed to write control region file");
        metrics
            .snapshot_written_bytes
            .inc_by(serialized.len() as u64);
        fs::rename(&temp_file, &self.path).expect("Failed to rename control region file");
    }

    /// The path to the control region file
    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}
