use crate::config::Config;
use crate::db::{Db, DbResult, CONTROL_REGION_FILE};
use crate::key_shape::KeyShape;
use crate::metrics::Metrics;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The name of the control region file in the saved snapshot path
const WAL_POSITION_FILE: &str = "ptr";

/// Return the path to the control region file in the specified directory.
fn control_region_path(dir: PathBuf) -> PathBuf {
    dir.join(CONTROL_REGION_FILE)
}

/// Return the path to the WAL pointer file in the specified directory.
fn wal_position_path(dir: PathBuf) -> PathBuf {
    dir.join(WAL_POSITION_FILE)
}

/// Create a state snapshot by copying the control region and saving the WAL pointer.
pub fn create(
    wal_position: &u64,
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
/// returned WAL position can be used to truncate the WAL file.
pub fn load(
    snapshot_path: PathBuf,
    database_path: PathBuf,
    key_shape: KeyShape,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
) -> DbResult<Arc<Db>> {
    // Copy back the control region
    let saved_control_region_path = control_region_path(snapshot_path.clone());
    let db_control_region_path = control_region_path(database_path.clone());
    let _ = fs::remove_file(&db_control_region_path); // Ignore error if file doesn't exist
    fs::copy(&saved_control_region_path, &db_control_region_path)?;

    // Load the WAL pointer from file
    let saved_wal_position_path = wal_position_path(snapshot_path);
    let serialized_wal_position = fs::read(&saved_wal_position_path)?;
    let last_wal_position = bincode::deserialize(&serialized_wal_position)
        .expect("Wal position should be deserializable");

    // Open the db and truncate the WAL file
    let db = Db::open(&database_path, key_shape, config, metrics)?;
    db.wal_writer().set_wal_position(last_wal_position);
    // Truncate the WAL file to the recorded position. The +1 is necessary because
    // the truncation length is inclusive, and we need to include the byte at the
    // last_wal_position.
    db.wal().file().set_len(last_wal_position + 1)?;

    Ok(db)
}
