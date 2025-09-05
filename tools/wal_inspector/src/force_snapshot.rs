use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
use tidehunter::config::Config;
use tidehunter::db::Db;
use tidehunter::test_utils::Metrics;

pub fn force_snapshot_command(db_path: PathBuf, verbose: bool) -> Result<()> {
    println!("WAL Inspector - Force Snapshot");
    println!("==============================");
    println!("DB path: {:?}", db_path);

    // Load the key shape from the database
    let key_shape = Db::load_key_shape(&db_path)
        .expect("Failed to load key shape from database - is this a valid TideHunter database?");

    if verbose {
        println!("Loaded key shape successfully");
    }

    // Create config and metrics for opening the database
    let config = Arc::new(Config::default());
    let metrics = Metrics::new();

    // Open the database
    if verbose {
        println!("Opening database...");
    }
    let db = Db::open(&db_path, key_shape, config, metrics)
        .map_err(|e| anyhow::anyhow!("Failed to open database: {:?}", e))?;

    // Check if there are any dirty entries
    let has_dirty_entries = !db.is_all_clean();
    if verbose {
        if has_dirty_entries {
            println!("Database has dirty entries that will be flushed");
        } else {
            println!("Database has no dirty entries");
        }
    }

    // Force rebuild control region with flush
    println!("Forcing snapshot with flush of all dirty entries...");
    let snapshot_position = db
        .force_rebuild_control_region()
        .map_err(|e| anyhow::anyhow!("Failed to force rebuild control region: {:?}", e))?;

    println!("✓ Snapshot completed successfully");
    println!("  Snapshot replay position: {:#x}", snapshot_position);

    // Verify all entries are clean
    if !db.is_all_clean() {
        println!("⚠️  Warning: Some entries are still dirty after forced snapshot");
    } else if verbose {
        println!("✓ All entries are now clean");
    }

    // Report statistics
    if has_dirty_entries && verbose {
        println!("  Dirty entries were flushed to disk");
    }

    // The database will be closed when it goes out of scope
    if verbose {
        println!("Closing database...");
    }

    Ok(())
}
