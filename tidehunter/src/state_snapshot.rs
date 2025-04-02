use std::{fs, path::PathBuf};

use crate::{
    db::{DbResult, CONTROL_REGION_FILE},
    WalPosition,
};

const WAL_POSITION_FILE: &str = "ptr";

/// `StateSnapshot` is used to create a snapshot of the database state
pub struct StateSnapshot {
    /// Path to the control region file in the source database
    source_control_region_path: PathBuf,
    /// Path to the control region file in the saved snapshot path
    saved_control_region_path: PathBuf,
    /// Path to the WAL position file in the saved snapshot path
    saved_wal_position_path: PathBuf,
}

impl StateSnapshot {
    /// Creates a new `StateSnapshot` instance.
    pub fn new(source_path: PathBuf, destination_path: PathBuf) -> Self {
        Self {
            source_control_region_path: source_path.join(CONTROL_REGION_FILE),
            saved_control_region_path: destination_path.join(CONTROL_REGION_FILE),
            saved_wal_position_path: destination_path.join(WAL_POSITION_FILE),
        }
    }

    /// Create a state snapshot by copying the control region and saving the WAL pointer.
    pub fn create(&self, wal_position: &WalPosition) -> DbResult<()> {
        // Save the control region
        fs::copy(
            &self.source_control_region_path,
            &self.saved_control_region_path,
        )?;

        // Save the WAL pointer
        let serialized_wal_position =
            bincode::serialize(wal_position).expect("Wal position should be serializable");
        fs::write(&self.saved_wal_position_path, &serialized_wal_position)?;
        Ok(())
    }

    /// Load the state snapshot from the saved files. It copies the control region
    /// back to the source path and loads the WAL pointer from the saved file. The 
    /// returned `WalPosition` can be used to truncate the WAL file.
    pub fn load(&self) -> DbResult<WalPosition> {
        // Copy back the control region
        fs::copy(
            &self.saved_control_region_path,
            &self.source_control_region_path,
        )?;

        // Load the WAL pointer from file
        let serialized_wal_position = fs::read(&self.saved_wal_position_path)?;
        let wal_position = bincode::deserialize(&serialized_wal_position)
            .expect("Wal position should be deserializable");

        Ok(wal_position)
    }
}
