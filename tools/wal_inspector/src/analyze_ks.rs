use crate::utils::format_bytes;
use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use tidehunter::control::ControlRegion;
use tidehunter::db::{Db, CONTROL_REGION_FILE};
use tidehunter::index::index_format::IndexFormat;
use tidehunter::key_shape::KeySpace;
use tidehunter::test_utils::{Metrics, Wal, WalEntry};
use tidehunter::WalKind;

pub fn analyze_ks_command(db_path: PathBuf, keyspace_name: String, verbose: bool) -> Result<()> {
    // Load key shape
    let key_shape = Db::load_key_shape(&db_path)
        .map_err(|e| anyhow::anyhow!("Failed to load key shape: {:?}", e))?;

    // Find keyspace by name
    let mut ks_desc = None;
    let mut ks = KeySpace::new(0);
    for desc in key_shape.iter_ks() {
        if desc.name() == keyspace_name {
            ks_desc = Some(desc);
            ks = desc.id();
            break;
        }
    }
    let ks_desc =
        ks_desc.ok_or_else(|| anyhow::anyhow!("Keyspace '{}' not found", keyspace_name))?;

    if verbose {
        println!("Analyzing keyspace: {}", ks_desc.name());
        println!();
    }

    // Load config to get WAL layout
    let config = Db::load_config(&db_path).unwrap_or_default();
    let wal_layout = config.wal_layout(WalKind::Index);

    // Load control region
    let control_path = db_path.join(CONTROL_REGION_FILE);
    let control_region = ControlRegion::read(&control_path, &key_shape)
        .map_err(|e| anyhow::anyhow!("Failed to read control region: {}", e))?;

    // Get snapshot and collect index positions for the target keyspace
    let snapshot = control_region.snapshot();
    let ks_idx = ks.as_u8() as usize;

    if ks_idx >= snapshot.0.len() {
        return Err(anyhow::anyhow!(
            "Keyspace index {} out of range (snapshot has {} keyspaces)",
            ks_idx,
            snapshot.0.len()
        ));
    }

    let ks_data = &snapshot.0[ks_idx];

    // Collect all valid index positions for this keyspace
    let mut index_positions = Vec::new();
    for row in ks_data {
        for entry in row.values() {
            if let Some(pos) = entry.position.valid() {
                index_positions.push(pos);
            }
        }
    }

    if verbose {
        println!(
            "Found {} index entries in control region",
            index_positions.len()
        );
        println!();
    }

    if index_positions.is_empty() {
        println!("No index entries found for keyspace: {}", keyspace_name);
        return Ok(());
    }

    // Open index WAL in read-only mode
    let metrics = Metrics::new();
    let index_wal = Wal::open(&db_path, wal_layout, metrics)
        .map_err(|e| anyhow::anyhow!("Failed to open index WAL: {:?}", e))?;

    // Process each index position
    let mut total_payload_len = 0u64;
    let mut total_keys = 0usize;
    let mut successfully_read = 0usize;
    let mut payload_sizes: Vec<usize> = Vec::new();

    // Create progress bar if not in verbose mode
    let progress_bar = if !verbose {
        let pb = ProgressBar::new(index_positions.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%)")
                .unwrap()
                .progress_chars("#>-"),
        );
        Some(pb)
    } else {
        None
    };

    for (idx, index_position) in index_positions.iter().enumerate() {
        if verbose {
            println!(
                "Reading index entry {}/{} at offset {}",
                idx + 1,
                index_positions.len(),
                index_position.offset()
            );
        } else if let Some(pb) = &progress_bar {
            pb.set_position((idx + 1) as u64);
        }

        // Read the entry from index WAL
        let entry_result = index_wal.read(*index_position);

        let entry_bytes = match entry_result {
            Ok((_read_type, Some(bytes))) => bytes,
            Ok((_read_type, None)) => {
                eprintln!("Warning: No data at index position {:?}", index_position);
                continue;
            }
            Err(e) => {
                eprintln!(
                    "Warning: Failed to read index at position {:?}: {:?}",
                    index_position, e
                );
                continue;
            }
        };

        let parsed_entry = WalEntry::from_bytes(entry_bytes);

        match parsed_entry {
            WalEntry::Index(entry_ks, index_bytes) => {
                if entry_ks != ks {
                    eprintln!(
                        "Warning: Expected keyspace {}, but found keyspace {}",
                        ks.as_u8(),
                        entry_ks.as_u8()
                    );
                    continue;
                }

                successfully_read += 1;

                // Deserialize the index to iterate through all positions
                let index_table = ks_desc
                    .index_format()
                    .deserialize_index(ks_desc, index_bytes);

                if verbose {
                    println!("  Index contains {} keys", index_table.len());
                }

                // Sum up payload_len for all positions in this index
                for (_key, wal_position) in index_table.iter() {
                    let payload_len = wal_position.payload_len();
                    total_payload_len += payload_len as u64;
                    total_keys += 1;
                    payload_sizes.push(payload_len);
                }
            }
            _ => {
                eprintln!(
                    "Warning: Expected Index entry at position {:?}, but found different entry type",
                    index_position
                );
            }
        }
    }

    // Finish progress bar
    if let Some(pb) = progress_bar {
        pb.finish_and_clear();
    }

    // Print results
    println!("\nAnalysis Results for Keyspace: {}", keyspace_name);
    println!("=====================================");
    println!("Index entries in control region: {}", index_positions.len());
    println!("Index entries successfully read: {}", successfully_read);
    println!("Total keys in indices: {}", total_keys);
    println!(
        "Total payload size: {} ({} bytes)",
        format_bytes(total_payload_len as usize),
        total_payload_len
    );

    if total_keys > 0 {
        let avg_payload = total_payload_len / total_keys as u64;
        println!("Average payload size per key: {} bytes", avg_payload);

        // Calculate and print size distribution
        println!("\nPayload Size Distribution:");
        payload_sizes.sort_unstable();

        let percentiles = [10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 99];
        for &percentile in &percentiles {
            let index = (payload_sizes.len() as f64 * percentile as f64 / 100.0) as usize;
            let index = index.min(payload_sizes.len() - 1);
            println!("  P{:02}: {} bytes", percentile, payload_sizes[index]);
        }

        println!("  Min: {} bytes", payload_sizes[0]);
        println!("  Max: {} bytes", payload_sizes[payload_sizes.len() - 1]);
    }

    Ok(())
}
