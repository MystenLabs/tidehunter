use anyhow::Result;
use std::path::PathBuf;
use tidehunter::control::ControlRegion;
use tidehunter::db::{Db, CONTROL_REGION_FILE};
use tidehunter::WalKind;

pub fn control_region_command(db_path: PathBuf, num_positions: usize, verbose: bool) -> Result<()> {
    // Load key shape
    let key_shape = Db::load_key_shape(&db_path)
        .map_err(|e| anyhow::anyhow!("Failed to load key shape: {:?}", e))?;

    // Load config to get WAL layout
    let config = Db::load_config(&db_path).unwrap_or_default();
    let wal_layout = config.wal_layout(WalKind::Index);

    // Load control region (fail if it doesn't exist)
    let control_path = db_path.join(CONTROL_REGION_FILE);
    let control_region = ControlRegion::read(&control_path, &key_shape)
        .map_err(|e| anyhow::anyhow!("Failed to read control region: {}", e))?;

    // Gather statistics
    let snapshot = control_region.snapshot();
    let mut total_entries = 0;
    let mut ks_stats = Vec::new();

    // Collect all valid positions with their keyspace for sorting
    let mut all_positions = Vec::new();

    for (ks_idx, ks_data) in snapshot.0.iter().enumerate() {
        let mut ks_total = 0;

        for row in ks_data {
            for entry in row.values() {
                ks_total += 1;
                total_entries += 1;

                if let Some(pos) = entry.position.valid() {
                    all_positions.push((pos, ks_idx));
                }
            }
        }

        ks_stats.push((ks_idx, ks_total));
    }

    // Sort positions to find lowest (by position, then by keyspace)
    all_positions.sort_by_key(|(pos, ks)| (*pos, *ks));

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

    // Print percentiles (need to extract just positions for percentile calculation)
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

    // Print lowest N positions with their keyspace name and WAL file
    if !all_positions.is_empty() {
        let display_count = num_positions.min(all_positions.len());
        println!("Lowest {} WAL positions:", display_count);
        for (i, (position, ks_idx)) in all_positions.iter().take(display_count).enumerate() {
            let ks = key_shape.ks(tidehunter::key_shape::KeySpace::new(*ks_idx as u8));
            let file_id = wal_layout.locate_file(position.offset());
            let file_name = wal_layout.wal_file_name(&db_path, file_id);
            let file_name = file_name
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown");
            println!(
                "  {}: offset {} in {} ({})",
                i + 1,
                position.offset(),
                file_name,
                ks.name()
            );
        }
    } else {
        println!("No valid WAL positions found in control region");
    }

    Ok(())
}
