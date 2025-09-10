use anyhow::Result;
use std::path::PathBuf;
use tidehunter::control::ControlRegion;
use tidehunter::db::{Db, CONTROL_REGION_FILE};

pub fn control_region_command(db_path: PathBuf, num_positions: usize, verbose: bool) -> Result<()> {
    // Load key shape
    let key_shape = Db::load_key_shape(&db_path)
        .map_err(|e| anyhow::anyhow!("Failed to load key shape: {:?}", e))?;

    // Load control region (fail if it doesn't exist)
    let control_path = db_path.join(CONTROL_REGION_FILE);
    let control_region = ControlRegion::read(&control_path, &key_shape)
        .map_err(|e| anyhow::anyhow!("Failed to read control region: {}", e))?;

    // Gather statistics
    let snapshot = control_region.snapshot();
    let mut total_entries = 0;
    let mut ks_stats = Vec::new();

    // Collect all valid positions for sorting
    let mut all_positions = Vec::new();

    for (ks_idx, ks_data) in snapshot.0.iter().enumerate() {
        let mut ks_total = 0;

        for row in ks_data {
            for entry in row.values() {
                ks_total += 1;
                total_entries += 1;

                if let Some(pos) = entry.position.valid() {
                    all_positions.push(pos);
                }
            }
        }

        ks_stats.push((ks_idx, ks_total));
    }

    // Sort positions to find lowest
    all_positions.sort();

    // Print basic statistics
    println!("Control Region Statistics");
    println!("=========================");
    println!("Path: {:?}", control_path);
    println!(
        "Last processed position: {}",
        control_region.last_position()
    );
    if let Some(last_index_pos) = control_region.last_index_wal_position() {
        println!("Last index WAL position: {:?}", last_index_pos);
    }
    println!();

    println!("Entry Statistics:");
    println!("  Total entries: {}", total_entries);
    println!("  Valid WAL positions: {}", all_positions.len());
    println!();

    // Print keyspace breakdown if verbose
    if verbose && !ks_stats.is_empty() {
        println!("Keyspace Breakdown:");
        for (ks_idx, total) in ks_stats {
            println!("  Keyspace {}: {} entries", ks_idx, total);
        }
        println!();
    }

    // Print percentiles
    if !all_positions.is_empty() {
        println!("WAL Position Percentiles:");
        if let Some(p50) = snapshot.pct_wal_position(50) {
            println!("  P50: {:?}", p50);
        }
        if let Some(p90) = snapshot.pct_wal_position(90) {
            println!("  P90: {:?}", p90);
        }
        if let Some(p99) = snapshot.pct_wal_position(99) {
            println!("  P99: {:?}", p99);
        }
        println!();
    }

    // Print lowest N positions
    if !all_positions.is_empty() {
        let display_count = num_positions.min(all_positions.len());
        println!("Lowest {} WAL positions:", display_count);
        for (i, position) in all_positions.iter().take(display_count).enumerate() {
            println!("  {}: {:?}", i + 1, position);
        }
    } else {
        println!("No valid WAL positions found in control region");
    }

    Ok(())
}
