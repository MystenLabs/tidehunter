use crate::InspectorContext;
use anyhow::Result;
use std::sync::Arc;
use tidehunter::db::Db;
use tidehunter::test_utils::Metrics;

pub fn force_snapshot_command(context: &InspectorContext) -> Result<()> {
    println!("WAL Inspector - Force Snapshot");
    println!("==============================");

    // Create config and metrics for opening the database
    let config = Arc::new(context.config.clone());
    let metrics = Metrics::new();

    // Open the database
    if context.verbose {
        println!("Opening database...");
    }
    let db = Db::open(&context.db_path, context.key_shape.clone(), config, metrics)
        .map_err(|e| anyhow::anyhow!("Failed to open database: {:?}", e))?;

    // Check if there are any dirty entries
    let has_dirty_entries = !db.is_all_clean();
    if context.verbose {
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
    } else if context.verbose {
        println!("✓ All entries are now clean");
    }

    // Report statistics
    if has_dirty_entries && context.verbose {
        println!("  Dirty entries were flushed to disk");
    }

    // The database will be closed when it goes out of scope
    if context.verbose {
        println!("Closing database...");
    }

    Ok(())
}
