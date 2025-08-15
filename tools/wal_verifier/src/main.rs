use anyhow::Result;
use clap::Parser;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tidehunter::config::Config;
use tidehunter::db::Db;
use tidehunter::key_shape::{KeyShape, KeySpace, KeyType};
use tidehunter::minibytes::Bytes;
use tidehunter::test_utils::{Metrics, Wal, WalEntry, WalError};

#[derive(Parser, Debug)]
#[command(
    name = "wal_verifier",
    about = "Verifies that all keys in a database's WAL file are accessible from the database"
)]
struct Args {
    /// Path to the database directory containing the WAL file
    #[arg(short = 'd', long)]
    db_path: PathBuf,

    /// Verbose output
    #[arg(short, long)]
    verbose: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("WAL Verifier");
    println!("============");
    println!("DB path: {:?}", args.db_path);

    // Derive WAL path from database path
    let wal_path = Db::wal_path(&args.db_path);
    println!("WAL file: {:?}", wal_path);
    println!();

    // Check if WAL file exists
    if !wal_path.exists() {
        return Err(anyhow::anyhow!(
            "WAL file not found at {:?}. Is this a valid TideHunter database directory?",
            wal_path
        ));
    }

    // For now, we need to provide a KeyShape manually since it's not stored in WAL
    // This is a placeholder - in real usage, the user would need to provide the correct schema
    println!("WARNING: Using default KeyShape - this may not match your actual database schema!");
    println!("KeyShape information is not stored in WAL files.");
    let (key_shape, _ks) = KeyShape::new_single(32, 16, KeyType::uniform(16));

    let result = verify_wal(&args.db_path, key_shape, args.verbose)?;

    // Print summary
    println!("\n{}", "=".repeat(50));
    println!("Verification Summary:");
    println!("  Total keys in WAL: {}", result.total_keys);
    println!("  Successfully verified: {}", result.verified);
    println!("  Missing keys: {}", result.missing);
    println!("  Errors: {}", result.errors);

    if result.missing > 0 || result.errors > 0 {
        println!("\n❌ VERIFICATION FAILED");
        std::process::exit(1);
    } else {
        println!("\n✅ VERIFICATION SUCCESSFUL");
        println!(
            "All {} keys from WAL are accessible in the database",
            result.total_keys
        );
    }

    Ok(())
}

#[derive(Debug, Clone)]
pub struct VerificationResult {
    pub total_keys: usize,
    pub verified: usize,
    pub missing: usize,
    pub errors: usize,
}

/// Verifies that all keys in a database's WAL file are accessible from the database
pub fn verify_wal(
    db_path: &Path,
    key_shape: KeyShape,
    verbose: bool,
) -> Result<VerificationResult> {
    // Get the WAL path from the database path
    let wal_path = Db::wal_path(db_path);

    // Step 1: Read all keys from WAL file
    println!("Step 1: Reading keys from WAL file...");
    let keys = read_keys_from_wal(&wal_path, verbose)?;
    println!("Found {} keys in WAL file", keys.len());

    if verbose {
        println!("\nKeys found:");
        for (i, (ks, key)) in keys.iter().enumerate() {
            let key_str = String::from_utf8_lossy(key);
            println!("  [{}] KeySpace: {:?}, Key: {:?}", i + 1, ks, key_str);
        }
    }

    // Step 2: Open database from the WAL file
    println!("\nStep 2: Opening database from WAL file...");

    // Create default config and metrics
    let config = Arc::new(Config::default());
    let metrics = Metrics::new();

    // Open the database - it will replay the WAL automatically
    let db = Db::open(db_path, key_shape, config, metrics)
        .map_err(|e| anyhow::anyhow!("Failed to open database: {:?}", e))?;
    println!("Database opened successfully");

    // Step 3: Verify all keys are accessible
    println!("\nStep 3: Verifying all keys are accessible...");
    let mut verified = 0;
    let mut missing = 0;
    let mut errors = 0;

    for (ks, key) in &keys {
        match db.get(*ks, key) {
            Ok(Some(_value)) => {
                verified += 1;
                if verbose {
                    let key_str = String::from_utf8_lossy(key);
                    println!("  ✓ Key {:?} found", key_str);
                }
            }
            Ok(None) => {
                missing += 1;
                let key_str = String::from_utf8_lossy(key);
                println!("  ✗ Key {:?} NOT FOUND", key_str);
            }
            Err(e) => {
                errors += 1;
                let key_str = String::from_utf8_lossy(key);
                println!("  ✗ Error accessing key {:?}: {:?}", key_str, e);
            }
        }
    }

    Ok(VerificationResult {
        total_keys: keys.len(),
        verified,
        missing,
        errors,
    })
}

// Helper type to track keys - using string representation for simplicity
type KeyEntry = (u8, Vec<u8>); // (keyspace_id, key_bytes)

fn read_keys_from_wal(wal_path: &Path, verbose: bool) -> Result<Vec<(KeySpace, Bytes)>> {
    // Create config and metrics for WAL reading
    let config = Config::default();
    let metrics = Metrics::new();

    // Open the WAL file
    let wal = Wal::open(wal_path, config.wal_layout(), metrics)
        .map_err(|e| anyhow::anyhow!("Failed to open WAL file: {:?}", e))?;

    // Create iterator starting from beginning
    let mut wal_iterator = wal
        .wal_iterator(0)
        .map_err(|e| anyhow::anyhow!("Failed to create WAL iterator: {:?}", e))?;

    // Track unique keys (latest value wins) - use a simpler representation
    let mut keys_map: HashMap<KeyEntry, ()> = HashMap::new();
    let mut entry_count = 0;
    let mut record_count = 0;
    let mut remove_count = 0;
    let mut index_count = 0;
    let mut batch_count = 0;

    println!("Reading WAL entries...");

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
        let entry = WalEntry::from_bytes(raw_entry);

        match entry {
            WalEntry::Record(ks, key, _value) => {
                record_count += 1;
                // Store as simpler key entry
                let key_entry = (ks.as_u8(), key.to_vec());
                keys_map.insert(key_entry, ());
                if verbose && record_count % 1000 == 0 {
                    println!("  Processed {} records...", record_count);
                }
            }
            WalEntry::Remove(ks, key) => {
                remove_count += 1;
                // Remove the key from our map since it was deleted
                let key_entry = (ks.as_u8(), key.to_vec());
                keys_map.remove(&key_entry);
            }
            WalEntry::Index(_ks, _data) => {
                index_count += 1;
            }
            WalEntry::BatchStart(_size) => {
                batch_count += 1;
            }
        }
    }

    println!("\nWAL Statistics:");
    println!("  Total entries: {}", entry_count);
    println!("  Records: {}", record_count);
    println!("  Removes: {}", remove_count);
    println!("  Index entries: {}", index_count);
    println!("  Batch starts: {}", batch_count);
    println!("  Unique keys remaining: {}", keys_map.len());

    // Convert back to expected format
    let keys: Vec<(KeySpace, Bytes)> = keys_map
        .into_keys()
        .map(|(ks_id, key_vec)| (KeySpace::new(ks_id), Bytes::from(key_vec)))
        .collect();

    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tidehunter::batch::WriteBatch;

    #[test]
    fn test_verify_wal_with_simple_records() -> Result<()> {
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

        // Write some test records (8-byte keys to match KeyShape)
        let mut batch = WriteBatch::new();
        batch.write(ks, b"key00001".to_vec(), b"value1".to_vec());
        batch.write(ks, b"key00002".to_vec(), b"value2".to_vec());
        batch.write(ks, b"key00003".to_vec(), b"value3".to_vec());
        db.write_batch(batch)
            .map_err(|e| anyhow::anyhow!("Failed to write batch: {:?}", e))?;

        // Close the database
        drop(db);

        // Now verify the WAL
        let result = verify_wal(&db_path, key_shape, false)?;

        // Check results
        assert_eq!(result.total_keys, 3, "Should have 3 keys in WAL");
        assert_eq!(result.verified, 3, "All 3 keys should be verified");
        assert_eq!(result.missing, 0, "No keys should be missing");
        assert_eq!(result.errors, 0, "No errors should occur");

        Ok(())
    }

    #[test]
    fn test_verify_wal_with_deletes() -> Result<()> {
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

        // Write some test records (8-byte keys to match KeyShape)
        let mut batch = WriteBatch::new();
        batch.write(ks, b"key00001".to_vec(), b"value1".to_vec());
        batch.write(ks, b"key00002".to_vec(), b"value2".to_vec());
        batch.write(ks, b"key00003".to_vec(), b"value3".to_vec());
        db.write_batch(batch)
            .map_err(|e| anyhow::anyhow!("Failed to write batch: {:?}", e))?;

        // Delete one key (8-byte key to match KeyShape)
        let mut delete_batch = WriteBatch::new();
        delete_batch.delete(ks, b"key00002".to_vec());
        db.write_batch(delete_batch)
            .map_err(|e| anyhow::anyhow!("Failed to write delete batch: {:?}", e))?;

        // Close the database
        drop(db);

        // Now verify the WAL
        let result = verify_wal(&db_path, key_shape, false)?;

        // Check results - should only have 2 keys after deletion
        assert_eq!(
            result.total_keys, 2,
            "Should have 2 keys in WAL after deletion"
        );
        assert_eq!(result.verified, 2, "Both remaining keys should be verified");
        assert_eq!(result.missing, 0, "No keys should be missing");
        assert_eq!(result.errors, 0, "No errors should occur");

        Ok(())
    }
}
