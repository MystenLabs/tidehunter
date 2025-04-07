use crate::{
    db::{DbResult, CONTROL_REGION_FILE},
    wal::{Wal, WalError},
    WalPosition,
};
use std::{fs, path::PathBuf, sync::Arc};
use std::{os::unix::fs::FileExt, path::Path};

/// The name of the control region file in the saved snapshot path
const WAL_POSITION_FILE: &str = "ptr";

/// Return the path to the control region file in the specified directory.
fn control_region_path(dir: PathBuf) -> PathBuf {
    dir.join(CONTROL_REGION_FILE)
}

/// Return the path to the control region file in the specified directory.
fn wal_position_path(dir: PathBuf) -> PathBuf {
    dir.join(WAL_POSITION_FILE)
}

/// Create a state snapshot by copying the control region and saving the WAL pointer.
pub fn create(
    wal_position: &WalPosition,
    source_control_region_path: &Path,
    destination_path: PathBuf,
) -> DbResult<()> {
    // Save the control region
    let snapshot_control_region_path = control_region_path(destination_path.clone());
    fs::copy(source_control_region_path, &snapshot_control_region_path)?;

    // Save the WAL pointer
    fs::write(
        &wal_position_path(destination_path),
        &bincode::serialize(wal_position).expect("Wal position should be serializable"),
    )?;

    Ok(())
}

/// Load the state snapshot from the saved files. It copies the control region
/// back to the source path and loads the WAL pointer from the saved file. The
/// returned `WalPosition` can be used to truncate the WAL file.
pub fn load(
    wal: &Arc<Wal>,
    snapshot_path: PathBuf,
    database_path: PathBuf,
) -> DbResult<WalPosition> {
    // Copy back the control region
    let saved_control_region_path = control_region_path(snapshot_path.clone());
    let db_control_region_path = control_region_path(database_path);
    fs::copy(&saved_control_region_path, &db_control_region_path)?;

    // Load the WAL pointer from file
    let saved_wal_position_path = wal_position_path(snapshot_path);
    let serialized_wal_position = fs::read(&saved_wal_position_path)?;
    let last_wal_position = bincode::deserialize(&serialized_wal_position)
        .expect("Wal position should be deserializable");

    // Truncate the WAL file to the last position
    let mut wal_iterator = wal.wal_iterator_no_skip(last_wal_position)?;
    loop {
        match wal_iterator.next() {
            Ok((position, entry)) => {
                let length = entry.len();
                let empty = vec![0; length];
                println!("----> writing empty ({length}B) entry at {position:?}");
                wal.file().write_all_at(&empty, position.as_u64())?;
            }
            Err(WalError::Crc(_)) => {
                // CRC errors indicate the end of the WAL file
                break;
            }
            error => {
                error?;
            }
        }
    }

    Ok(last_wal_position)
}
