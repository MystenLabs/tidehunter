use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tidehunter::config::Config;
use tidehunter::db::Db;
use tidehunter::test_utils::{Metrics, Wal, WalEntry, WalError};

pub fn stat_command(db_path: PathBuf, verbose: bool) -> Result<()> {
    println!("WAL Inspector - Statistics");
    println!("==========================");
    println!("DB path: {:?}", db_path);

    // Derive WAL path from database path
    let wal_path = Db::wal_path(&db_path);
    println!("WAL file: {:?}", wal_path);
    println!();

    // Check if WAL file exists
    if !wal_path.exists() {
        return Err(anyhow::anyhow!(
            "WAL file not found at {:?}. Is this a valid TideHunter database directory?",
            wal_path
        ));
    }

    let stats = collect_wal_statistics(&db_path, verbose)?;
    print_statistics_report(&stats);

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

fn collect_wal_statistics(db_path: &Path, verbose: bool) -> Result<WalStatistics> {
    // Create config and metrics for WAL reading
    let config = Config::default();
    let metrics = Metrics::new();

    // Open the WAL file (Wal::open expects the database directory path)
    let wal = Wal::open(db_path, config.wal_layout(), metrics)
        .map_err(|e| anyhow::anyhow!("Failed to open WAL file: {:?}", e))?;

    // Create iterator starting from beginning
    let mut wal_iterator = wal
        .wal_iterator(0)
        .map_err(|e| anyhow::anyhow!("Failed to create WAL iterator: {:?}", e))?;

    let mut stats = WalStatistics::default();
    let mut entry_count = 0;

    println!("Analyzing WAL entries...");

    // Read all entries from WAL
    loop {
        let entry_result = wal_iterator.next();

        // Check for end of WAL (CRC error typically indicates end)
        if matches!(entry_result, Err(WalError::Crc(_))) {
            if verbose {
                println!("Reached end of WAL (CRC error)");
            }
            break;
        }

        let (_position, raw_entry) = match entry_result {
            Ok(entry) => entry,
            Err(e) => {
                if verbose {
                    println!("Error reading WAL entry: {:?}", e);
                }
                break;
            }
        };

        entry_count += 1;
        if verbose && entry_count % 10000 == 0 {
            println!("  Processed {} entries...", entry_count);
        }

        // Calculate the size of the raw entry
        let entry_size = raw_entry.len();
        stats.total_entries += 1;
        stats.total_space += entry_size;

        let entry = WalEntry::from_bytes(raw_entry);

        match entry {
            WalEntry::Record(ks, key, value) => {
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

    if verbose {
        println!("Finished analyzing {} entries", stats.total_entries);
    }

    Ok(stats)
}

fn print_statistics_report(stats: &WalStatistics) {
    println!("\n{}", "=".repeat(70));
    println!("WAL STATISTICS REPORT");
    println!("{}", "=".repeat(70));

    // Overall summary
    println!("\n📊 OVERALL SUMMARY");
    println!(
        "  Total entries: {:>12}",
        format_number(stats.total_entries)
    );
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
            format_number(stats.record_count),
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
            format_number(stats.remove_count),
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
            format_number(stats.index_count),
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
            format_number(stats.batch_start_count),
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
            "  {:10} {:>10} {:>10} {:>10} {:>12} {:>8}",
            "Keyspace", "Records", "Removes", "Indexes", "Total Size", "% Space"
        );
        println!("  {}", "-".repeat(70));

        let mut keyspaces: Vec<_> = stats.keyspace_stats.iter().collect();
        keyspaces.sort_by_key(|(k, _)| *k);

        for (ks_id, ks_stats) in keyspaces {
            let percent = (ks_stats.total_space as f64 / stats.total_space as f64) * 100.0;
            println!(
                "  {:10} {:>10} {:>10} {:>10} {:>12} {:>7.1}%",
                format!("KS {}", ks_id),
                format_number(ks_stats.record_count),
                format_number(ks_stats.remove_count),
                format_number(ks_stats.index_count),
                format_bytes(ks_stats.total_space),
                percent
            );
        }
    }

    println!("\n{}", "=".repeat(70));
}

fn format_bytes(bytes: usize) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.2} {}", size, UNITS[unit_index])
    }
}

fn format_number(num: usize) -> String {
    let s = num.to_string();
    let mut result = String::new();
    let mut count = 0;

    for c in s.chars().rev() {
        if count == 3 {
            result.push(',');
            count = 0;
        }
        result.push(c);
        count += 1;
    }

    result.chars().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tidehunter::batch::WriteBatch;
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
            let key = format!("key{:05}", i);
            let value = format!("value{}", i);
            batch.write(ks, key.into_bytes(), value.into_bytes());
        }
        db.write_batch(batch)
            .map_err(|e| anyhow::anyhow!("Failed to write batch: {:?}", e))?;

        // Add some removes
        let mut batch = WriteBatch::new();
        for i in 2..5 {
            let key = format!("key{:05}", i);
            batch.delete(ks, key.into_bytes());
        }
        db.write_batch(batch)
            .map_err(|e| anyhow::anyhow!("Failed to write batch: {:?}", e))?;

        // Note: We don't have a public flush method, but index entries
        // will be created during normal operation

        // Close the database
        drop(db);

        // Now collect statistics
        let stats = collect_wal_statistics(&db_path, false)?;

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

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(999), "999");
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(1234567), "1,234,567");
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
    }
}
