use crate::InspectorContext;
use crate::utils::{format_bytes, format_count};
use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::fs;
use std::io::ErrorKind;
use tidehunter::WalKind;
use tidehunter::key_shape::{KeyShape, KeySpace};
use tidehunter::test_utils::{Metrics, Wal, WalEntry, WalError, list_wal_files_with_sizes};

pub fn stat_command(context: &InspectorContext) -> Result<()> {
    println!("WAL Inspector - Statistics");
    println!("==========================");

    // Read control region if it exists
    print_control_region_info(context)?;

    let stats = collect_wal_statistics(context)?;
    print_statistics_report(&stats, &context.key_shape);

    Ok(())
}

#[derive(Debug, Default)]
struct WalStatistics {
    // Entry counts
    record_count: usize,
    remove_count: usize,
    index_count: usize,
    batch_start_count: usize,

    // Space usage in bytes
    record_space: usize,
    remove_space: usize,
    index_space: usize,
    batch_start_space: usize,

    // Additional statistics
    total_entries: usize,
    total_space: usize,

    // Key/value size statistics
    min_key_size: Option<usize>,
    max_key_size: Option<usize>,
    total_key_bytes: usize,

    min_value_size: Option<usize>,
    max_value_size: Option<usize>,
    total_value_bytes: usize,

    // Keyspace distribution
    keyspace_stats: HashMap<u8, KeyspaceStats>,
}

#[derive(Debug, Default)]
struct KeyspaceStats {
    record_count: usize,
    remove_count: usize,
    index_count: usize,
    total_key_bytes: usize,
    total_value_bytes: usize,
    total_space: usize,
}

fn collect_wal_statistics(context: &InspectorContext) -> Result<WalStatistics> {
    let metrics = Metrics::new();

    // First, get the list of WAL files and their sizes for progress tracking
    let wal_files = list_wal_files_with_sizes(&context.db_path)
        .map_err(|e| anyhow::anyhow!("Failed to list WAL files: {:?}", e))?;

    let total_wal_size: u64 = wal_files.iter().map(|(_, size)| *size).sum();

    if context.verbose {
        println!(
            "Found {} WAL files with total size: {}",
            wal_files.len(),
            format_bytes(total_wal_size as usize)
        );
        for (path, size) in &wal_files {
            println!(
                "  {}: {}",
                path.file_name().unwrap().to_string_lossy(),
                format_bytes(*size as usize)
            );
        }
        println!();
    }

    // Set up progress bar
    let progress_bar = if total_wal_size > 0 {
        let pb = ProgressBar::new(total_wal_size);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                .unwrap()
                .progress_chars("#>-"),
        );
        Some(pb)
    } else {
        None
    };

    // Open the WAL file with the correct configuration
    let wal = Wal::open(
        &context.db_path,
        context.config.wal_layout(WalKind::Replay),
        metrics,
    )
    .map_err(|e| anyhow::anyhow!("Failed to open WAL file: {:?}", e))?;

    // Create iterator starting from beginning
    let mut wal_iterator = wal
        .wal_iterator(0)
        .map_err(|e| anyhow::anyhow!("Failed to create WAL iterator: {:?}", e))?;

    let mut stats = WalStatistics::default();
    let mut entry_count = 0;
    let mut last_position = 0u64;
    let mut bytes_processed = 0u64;

    if !context.verbose {
        println!("Analyzing WAL entries...");
    }

    // Read all entries from WAL
    loop {
        let entry_result = wal_iterator.next();

        // Check for end of WAL (CRC error typically indicates end)
        if let Err(WalError::Crc(crc_err)) = &entry_result {
            if let Some(pb) = &progress_bar {
                pb.finish_and_clear();
            }
            println!("CRC error encountered: {crc_err:?}");
            println!("Last successful WAL position: {last_position:#x}");
            break;
        }

        let (position, raw_entry) = match entry_result {
            Ok(entry) => entry,
            Err(e) => {
                if let Some(pb) = &progress_bar {
                    pb.finish_and_clear();
                }
                if context.verbose {
                    println!("Error reading WAL entry: {e:?}");
                }
                break;
            }
        };

        // Update last successful position
        last_position = position.offset();

        // Update progress bar
        let new_bytes_processed = position.offset();
        if let Some(pb) = &progress_bar
            && new_bytes_processed > bytes_processed
        {
            pb.set_position(new_bytes_processed);
            bytes_processed = new_bytes_processed;
        }

        entry_count += 1;
        if context.verbose && progress_bar.is_none() && entry_count % 10000 == 0 {
            println!("  Processed {entry_count} entries...");
        }

        // Calculate the size of the raw entry
        let entry_size = raw_entry.len();
        stats.total_entries += 1;
        stats.total_space += entry_size;

        let entry = WalEntry::from_bytes(raw_entry);

        match entry {
            WalEntry::Record(ks, key, value, _relocated) => {
                stats.record_count += 1;
                stats.record_space += entry_size;

                // Update key statistics
                let key_size = key.len();
                stats.total_key_bytes += key_size;
                stats.min_key_size = Some(match stats.min_key_size {
                    Some(min) => min.min(key_size),
                    None => key_size,
                });
                stats.max_key_size = Some(match stats.max_key_size {
                    Some(max) => max.max(key_size),
                    None => key_size,
                });

                // Update value statistics
                let value_size = value.len();
                stats.total_value_bytes += value_size;
                stats.min_value_size = Some(match stats.min_value_size {
                    Some(min) => min.min(value_size),
                    None => value_size,
                });
                stats.max_value_size = Some(match stats.max_value_size {
                    Some(max) => max.max(value_size),
                    None => value_size,
                });

                // Update keyspace statistics
                let ks_stats = stats.keyspace_stats.entry(ks.as_u8()).or_default();
                ks_stats.record_count += 1;
                ks_stats.total_key_bytes += key_size;
                ks_stats.total_value_bytes += value_size;
                ks_stats.total_space += entry_size;
            }
            WalEntry::Remove(ks, key) => {
                stats.remove_count += 1;
                stats.remove_space += entry_size;

                // Update key statistics for removes
                let key_size = key.len();
                stats.total_key_bytes += key_size;
                stats.min_key_size = Some(match stats.min_key_size {
                    Some(min) => min.min(key_size),
                    None => key_size,
                });
                stats.max_key_size = Some(match stats.max_key_size {
                    Some(max) => max.max(key_size),
                    None => key_size,
                });

                // Update keyspace statistics
                let ks_stats = stats.keyspace_stats.entry(ks.as_u8()).or_default();
                ks_stats.remove_count += 1;
                ks_stats.total_key_bytes += key_size;
                ks_stats.total_space += entry_size;
            }
            WalEntry::Index(ks, _data) => {
                stats.index_count += 1;
                stats.index_space += entry_size;

                // Update keyspace statistics
                let ks_stats = stats.keyspace_stats.entry(ks.as_u8()).or_default();
                ks_stats.index_count += 1;
                ks_stats.total_space += entry_size;
            }
            WalEntry::BatchStart(_size) => {
                stats.batch_start_count += 1;
                stats.batch_start_space += entry_size;
            }
        }
    }

    // Finish progress bar
    if let Some(pb) = progress_bar {
        pb.finish_and_clear();
    }

    println!("Finished analyzing {} entries", stats.total_entries);

    Ok(stats)
}

fn print_statistics_report(stats: &WalStatistics, key_shape: &KeyShape) {
    println!("\n{}", "=".repeat(70));
    println!("WAL STATISTICS REPORT");
    println!("{}", "=".repeat(70));

    // Overall summary
    println!("\n📊 OVERALL SUMMARY");
    println!("  Total entries: {:>12}", format_count(stats.total_entries));
    println!("  Total size:    {:>12}", format_bytes(stats.total_space));

    // Entry type breakdown
    println!("\n📈 ENTRY TYPE BREAKDOWN");
    println!(
        "  {:20} {:>10} {:>12} {:>12} {:>8}",
        "Type", "Count", "Size", "Avg Size", "% Space"
    );
    println!("  {}", "-".repeat(62));

    if stats.record_count > 0 {
        let avg_size = stats.record_space / stats.record_count;
        let percent = (stats.record_space as f64 / stats.total_space as f64) * 100.0;
        println!(
            "  {:20} {:>10} {:>12} {:>12} {:>7.1}%",
            "Record",
            format_count(stats.record_count),
            format_bytes(stats.record_space),
            format_bytes(avg_size),
            percent
        );
    }

    if stats.remove_count > 0 {
        let avg_size = stats.remove_space / stats.remove_count;
        let percent = (stats.remove_space as f64 / stats.total_space as f64) * 100.0;
        println!(
            "  {:20} {:>10} {:>12} {:>12} {:>7.1}%",
            "Remove",
            format_count(stats.remove_count),
            format_bytes(stats.remove_space),
            format_bytes(avg_size),
            percent
        );
    }

    if stats.index_count > 0 {
        let avg_size = stats.index_space / stats.index_count;
        let percent = (stats.index_space as f64 / stats.total_space as f64) * 100.0;
        println!(
            "  {:20} {:>10} {:>12} {:>12} {:>7.1}%",
            "Index",
            format_count(stats.index_count),
            format_bytes(stats.index_space),
            format_bytes(avg_size),
            percent
        );
    }

    if stats.batch_start_count > 0 {
        let avg_size = stats.batch_start_space / stats.batch_start_count;
        let percent = (stats.batch_start_space as f64 / stats.total_space as f64) * 100.0;
        println!(
            "  {:20} {:>10} {:>12} {:>12} {:>7.1}%",
            "BatchStart",
            format_count(stats.batch_start_count),
            format_bytes(stats.batch_start_space),
            format_bytes(avg_size),
            percent
        );
    }

    // Key/Value statistics
    if stats.record_count > 0 || stats.remove_count > 0 {
        println!("\n🔑 KEY STATISTICS");
        if let (Some(min), Some(max)) = (stats.min_key_size, stats.max_key_size) {
            let total_key_entries = stats.record_count + stats.remove_count;
            let avg_key_size = if total_key_entries > 0 {
                stats.total_key_bytes / total_key_entries
            } else {
                0
            };

            println!("  Min key size:     {:>8}", format_bytes(min));
            println!("  Max key size:     {:>8}", format_bytes(max));
            println!("  Avg key size:     {:>8}", format_bytes(avg_key_size));
            println!(
                "  Total key bytes:  {:>8}",
                format_bytes(stats.total_key_bytes)
            );
        }

        if stats.record_count > 0 {
            println!("\n💾 VALUE STATISTICS");
            if let (Some(min), Some(max)) = (stats.min_value_size, stats.max_value_size) {
                let avg_value_size = stats.total_value_bytes / stats.record_count;

                println!("  Min value size:   {:>8}", format_bytes(min));
                println!("  Max value size:   {:>8}", format_bytes(max));
                println!("  Avg value size:   {:>8}", format_bytes(avg_value_size));
                println!(
                    "  Total value bytes:{:>8}",
                    format_bytes(stats.total_value_bytes)
                );
            }
        }
    }

    // Keyspace distribution
    if !stats.keyspace_stats.is_empty() {
        println!("\n🗂️  KEYSPACE DISTRIBUTION");
        println!(
            "  {:35} {:>10} {:>10} {:>10} {:>12} {:>8}",
            "Keyspace", "Records", "Removes", "Indexes", "Total Size", "% Space"
        );
        println!("  {}", "-".repeat(95));

        let mut keyspaces: Vec<_> = stats.keyspace_stats.iter().collect();
        keyspaces.sort_by_key(|(k, _)| *k);

        for (ks_id, ks_stats) in keyspaces {
            let percent = (ks_stats.total_space as f64 / stats.total_space as f64) * 100.0;

            // Get the keyspace name
            let ks = KeySpace::new(*ks_id);
            let ks_desc = key_shape.ks(ks);
            let ks_name = format!("{} ({})", ks_desc.name(), ks_id);

            println!(
                "  {:35} {:>10} {:>10} {:>10} {:>12} {:>7.1}%",
                ks_name,
                format_count(ks_stats.record_count),
                format_count(ks_stats.remove_count),
                format_count(ks_stats.index_count),
                format_bytes(ks_stats.total_space),
                percent
            );
        }
    }

    println!("\n{}", "=".repeat(70));
}

fn print_control_region_info(context: &InspectorContext) -> Result<()> {
    // Control region file is named "cr" in the database directory
    let control_region_path = context.db_path.join("cr");

    // Try to read the control region file
    let control_region_data = match fs::read(&control_region_path) {
        Ok(data) => data,
        Err(err) if err.kind() == ErrorKind::NotFound => {
            println!("\n⚠️  No control region found (database may be new or not yet flushed)");
            return Ok(());
        }
        Err(err) => {
            return Err(anyhow::anyhow!("Failed to read control region: {}", err));
        }
    };

    // Deserialize the control region to get the replay position
    // The control region structure has a field called last_position
    // We'll use bincode to deserialize just the u64 at the beginning
    if control_region_data.len() >= 8 {
        // The first 8 bytes should be the last_position (u64)
        let replay_position = u64::from_le_bytes([
            control_region_data[0],
            control_region_data[1],
            control_region_data[2],
            control_region_data[3],
            control_region_data[4],
            control_region_data[5],
            control_region_data[6],
            control_region_data[7],
        ]);

        println!("\n📍 CONTROL REGION INFO");
        println!(
            "  Replay position:     {}",
            format_bytes(replay_position as usize)
        );

        // Get total WAL file sizes
        let wal_files = list_wal_files_with_sizes(&context.db_path)
            .map_err(|e| anyhow::anyhow!("Failed to list WAL files: {:?}", e))?;
        let total_wal_size: u64 = wal_files.iter().map(|(_, size)| *size).sum();

        println!(
            "  Total WAL size:      {}",
            format_bytes(total_wal_size as usize)
        );

        // Calculate bytes to replay
        if total_wal_size >= replay_position {
            let bytes_to_replay = total_wal_size - replay_position;
            println!(
                "  Bytes to replay:     {}",
                format_bytes(bytes_to_replay as usize)
            );

            if total_wal_size > 0 {
                let replay_percent = (replay_position as f64 / total_wal_size as f64) * 100.0;
                println!("  Progress:            {replay_percent:.1}%");
            }
        }
    } else {
        println!("\n⚠️  Control region file exists but appears to be corrupted or empty");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tidehunter::batch::WriteBatch;
    use tidehunter::config::Config;
    use tidehunter::db::Db;
    use tidehunter::key_shape::{KeyShape, KeyType};

    #[test]
    fn test_stat_collection() -> Result<()> {
        // Create a temporary directory for the test
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("test_db");

        // Create a simple key shape
        let (key_shape, ks) = KeyShape::new_single(8, 16, KeyType::uniform(16));

        // Create and populate a database
        std::fs::create_dir_all(&db_path)?;
        let config = Arc::new(Config::default());
        let metrics = Metrics::new();
        let db = Db::open(&db_path, key_shape.clone(), config.clone(), metrics.clone())
            .map_err(|e| anyhow::anyhow!("Failed to create test database: {:?}", e))?;

        // Write some test records
        let mut batch = WriteBatch::new();
        for i in 0..10 {
            let key = format!("key{i:05}");
            let value = format!("value{i}");
            batch.write(ks, key.into_bytes(), value.into_bytes());
        }
        db.write_batch(batch)
            .map_err(|e| anyhow::anyhow!("Failed to write batch: {:?}", e))?;

        // Add some removes
        let mut batch = WriteBatch::new();
        for i in 2..5 {
            let key = format!("key{i:05}");
            batch.delete(ks, key.into_bytes());
        }
        db.write_batch(batch)
            .map_err(|e| anyhow::anyhow!("Failed to write batch: {:?}", e))?;

        // Note: We don't have a public flush method, but index entries
        // will be created during normal operation

        // Close the database
        drop(db);

        // Now collect statistics using InspectorContext
        let context = InspectorContext::load(db_path, false)?;
        let stats = collect_wal_statistics(&context)?;

        // Verify statistics
        assert_eq!(stats.record_count, 10, "Should have 10 record entries");
        assert_eq!(stats.remove_count, 3, "Should have 3 remove entries");
        assert!(stats.total_entries > 0, "Should have some entries");
        assert!(stats.total_space > 0, "Should have consumed some space");

        // Check that we have keyspace statistics
        assert!(
            !stats.keyspace_stats.is_empty(),
            "Should have keyspace statistics"
        );

        Ok(())
    }
}
