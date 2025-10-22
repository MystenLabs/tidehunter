use crate::InspectorContext;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tidehunter::WalKind;
use tidehunter::db::Db;
use tidehunter::key_shape::KeySpace;
use tidehunter::minibytes::Bytes;
use tidehunter::test_utils::{Metrics, Wal, WalEntry, WalError};

pub fn verify_command(context: &InspectorContext) -> Result<()> {
    println!("WAL Inspector - Verify");
    println!("======================");

    let result = verify_wal(context)?;

    // Print summary
    println!("\n{}", "=".repeat(50));
    println!("Verification Summary:");
    println!("  Total keys in WAL: {}", result.total_keys);
    println!();
    println!("  Direct Access Verification:");
    println!("    Successfully verified: {}", result.verified);
    println!("    Missing keys: {}", result.missing);
    println!("    Errors: {}", result.errors);
    println!();
    println!("  Iterator Verification:");
    println!("    Successfully verified: {}", result.iterator_verified);
    println!("    Missing keys: {}", result.iterator_missing);
    println!("    Errors: {}", result.iterator_errors);

    let total_failures =
        result.missing + result.errors + result.iterator_missing + result.iterator_errors;

    if total_failures > 0 {
        println!("\n❌ VERIFICATION FAILED");
        if result.missing > 0 || result.errors > 0 {
            println!(
                "  Direct access failures: {}",
                result.missing + result.errors
            );
        }
        if result.iterator_missing > 0 || result.iterator_errors > 0 {
            println!(
                "  Iterator failures: {}",
                result.iterator_missing + result.iterator_errors
            );
        }
        std::process::exit(1);
    } else {
        println!("\n✅ VERIFICATION SUCCESSFUL");
        println!(
            "All {} keys from WAL are accessible through both direct access and iteration",
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
    pub iterator_verified: usize,
    pub iterator_missing: usize,
    pub iterator_errors: usize,
}

/// Verifies that all keys in a database's WAL file are accessible from the database
pub fn verify_wal(context: &InspectorContext) -> Result<VerificationResult> {
    // Step 1: Read all keys from WAL file
    println!("Step 1: Reading keys from WAL file...");
    let keys = read_keys_from_wal(context)?;
    println!("Found {} keys in WAL file", keys.len());

    if context.verbose {
        println!("\nKeys found:");
        for (i, (ks, key)) in keys.iter().enumerate() {
            println!(
                "  [{}] KeySpace: {:?}, Key: {}",
                i + 1,
                ks,
                hex::encode(key)
            );
        }
    }

    // Step 2: Open database with loaded configuration
    println!("\nStep 2: Opening database with loaded configuration...");

    // Create config and metrics
    let config = Arc::new(context.config.clone());
    let metrics = Metrics::new();

    // Open the database - it will replay the WAL automatically
    let db = Db::open(&context.db_path, context.key_shape.clone(), config, metrics)
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
                if context.verbose {
                    println!("  ✓ Key {} found", hex::encode(key));
                }
            }
            Ok(None) => {
                missing += 1;
                println!("  ✗ Key {} NOT FOUND", hex::encode(key));
            }
            Err(e) => {
                errors += 1;
                println!("  ✗ Error accessing key {}: {:?}", hex::encode(key), e);
            }
        }
    }

    // Step 4: Verify all keys are accessible through iteration
    println!("\nStep 4: Verifying all keys are accessible through iteration...");
    let (iterator_verified, iterator_missing, iterator_errors) =
        verify_keys_through_iteration(&db, &keys, context.verbose)?;

    Ok(VerificationResult {
        total_keys: keys.len(),
        verified,
        missing,
        errors,
        iterator_verified,
        iterator_missing,
        iterator_errors,
    })
}

/// Verify that all keys from WAL are accessible through database iteration
fn verify_keys_through_iteration(
    db: &std::sync::Arc<Db>,
    expected_keys: &[(KeySpace, Bytes)],
    verbose: bool,
) -> Result<(usize, usize, usize)> {
    use std::collections::HashMap;

    // Group expected keys by keyspace, and also store their expected values
    // We need to get the values from the database first since we only have keys from WAL
    let mut expected_by_ks: HashMap<u8, HashMap<Vec<u8>, Vec<u8>>> = HashMap::new();
    for (ks, key) in expected_keys {
        // Get the value from the database for comparison
        match db.get(*ks, key) {
            Ok(Some(value)) => {
                expected_by_ks
                    .entry(ks.as_u8())
                    .or_default()
                    .insert(key.to_vec(), value.to_vec());
            }
            Ok(None) => {
                // Key not found - this will be caught in the regular verification
                // Still add it with empty value so we can report it as missing
                expected_by_ks
                    .entry(ks.as_u8())
                    .or_default()
                    .insert(key.to_vec(), Vec::new());
            }
            Err(_) => {
                // Error getting value - will be reported in regular verification
                expected_by_ks
                    .entry(ks.as_u8())
                    .or_default()
                    .insert(key.to_vec(), Vec::new());
            }
        }
    }

    let mut verified = 0;
    let mut missing = 0;
    let mut errors = 0;

    // Iterate through each keyspace found in the WAL
    for (&ks_id, expected_keys_set) in &expected_by_ks {
        let ks = KeySpace::new(ks_id);

        if verbose {
            println!(
                "  Iterating keyspace {}: expecting {} keys",
                ks_id,
                expected_keys_set.len()
            );
        }

        // Collect all key-value pairs found through iteration for this keyspace
        let mut found_kv_pairs: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        let mut iterator = db.iterator(ks);
        loop {
            match iterator.next() {
                Some(Ok((key, value))) => {
                    found_kv_pairs.insert(key.to_vec(), value.to_vec());
                    if verbose && found_kv_pairs.len().is_multiple_of(100) {
                        println!(
                            "    Found {} key-value pairs so far in keyspace {}",
                            found_kv_pairs.len(),
                            ks_id
                        );
                    }
                }
                Some(Err(e)) => {
                    errors += 1;
                    println!("  ✗ Error during iteration in keyspace {ks_id}: {e:?}");
                    break;
                }
                None => break, // End of iteration
            }
        }

        if verbose {
            println!(
                "  Found {} key-value pairs through iteration in keyspace {}",
                found_kv_pairs.len(),
                ks_id
            );
        }

        // Check that all expected key-value pairs were found through iteration with correct values
        for (expected_key, expected_value) in expected_keys_set {
            if let Some(found_value) = found_kv_pairs.get(expected_key) {
                if expected_value.is_empty() {
                    // We couldn't get the expected value earlier (key was missing or error)
                    // Just count it as verified if we found it through iteration
                    verified += 1;
                    if verbose {
                        println!(
                            "    ✓ Key {} found through iteration (value not checked - key was missing in direct access)",
                            hex::encode(expected_key)
                        );
                    }
                } else if found_value == expected_value {
                    verified += 1;
                    if verbose {
                        println!(
                            "    ✓ Key {} found through iteration with correct value",
                            hex::encode(expected_key)
                        );
                    }
                } else {
                    errors += 1;
                    println!(
                        "    ✗ Key {} found through iteration but VALUE MISMATCH in keyspace {}",
                        hex::encode(expected_key),
                        ks_id
                    );
                    println!("      Expected value: {}", hex::encode(expected_value));
                    println!("      Found value: {}", hex::encode(found_value));
                }
            } else {
                missing += 1;
                println!(
                    "    ✗ Key {} NOT FOUND through iteration in keyspace {}",
                    hex::encode(expected_key),
                    ks_id
                );
            }
        }

        // Report any extra keys found through iteration (shouldn't happen)
        let expected_keys: std::collections::HashSet<_> =
            expected_keys_set.keys().cloned().collect();
        let found_keys: std::collections::HashSet<_> = found_kv_pairs.keys().cloned().collect();
        let extra_keys: Vec<_> = found_keys.difference(&expected_keys).collect();
        if !extra_keys.is_empty() {
            println!(
                "  ⚠ Found {} extra keys through iteration in keyspace {} (not in WAL):",
                extra_keys.len(),
                ks_id
            );
            for extra_key in extra_keys.iter().take(10) {
                // Show first 10
                println!("    + Extra key: {}", hex::encode(extra_key));
            }
            if extra_keys.len() > 10 {
                println!("    + ... and {} more", extra_keys.len() - 10);
            }
        }
    }

    Ok((verified, missing, errors))
}

// Helper type to track keys - using string representation for simplicity
type KeyEntry = (u8, Vec<u8>); // (keyspace_id, key_bytes)

fn read_keys_from_wal(context: &InspectorContext) -> Result<Vec<(KeySpace, Bytes)>> {
    // Create metrics for WAL reading
    let metrics = Metrics::new();

    // Open the WAL file with correct configuration
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
            if context.verbose {
                println!("Reached end of WAL (CRC error)");
            }
            break;
        }

        let (_position, raw_entry) = match entry_result {
            Ok(entry) => entry,
            Err(e) => {
                if context.verbose {
                    println!("Error reading WAL entry: {e:?}");
                }
                break;
            }
        };

        entry_count += 1;
        let entry = WalEntry::from_bytes(raw_entry);

        match entry {
            WalEntry::Record(ks, key, _value, _relocated) => {
                record_count += 1;
                // Store as simpler key entry
                let key_entry = (ks.as_u8(), key.to_vec());
                keys_map.insert(key_entry, ());
                if context.verbose && record_count % 1000 == 0 {
                    println!("  Processed {record_count} records...");
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
            WalEntry::DropCells(_ks, _from, _to) => {
                // DropCells is an administrative operation that drops ranges of cells
                println!(
                    "\nWARNING: DropCells WAL entry detected. This tool does not support DropCells entries, and verification results may be incorrect."
                );
            }
        }
    }

    println!("\nWAL Statistics:");
    println!("  Total entries: {entry_count}");
    println!("  Records: {record_count}");
    println!("  Removes: {remove_count}");
    println!("  Index entries: {index_count}");
    println!("  Batch starts: {batch_count}");
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
    use tidehunter::config::Config;
    use tidehunter::db::Db;
    use tidehunter::key_shape::{KeyShape, KeyType};

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

        // Now verify the WAL using InspectorContext
        let context = InspectorContext::load(db_path, false)?;
        let result = verify_wal(&context)?;

        // Check results
        assert_eq!(result.total_keys, 3, "Should have 3 keys in WAL");
        assert_eq!(result.verified, 3, "All 3 keys should be verified");
        assert_eq!(result.missing, 0, "No keys should be missing");
        assert_eq!(result.errors, 0, "No errors should occur");
        assert_eq!(
            result.iterator_verified, 3,
            "All 3 keys should be verified through iteration"
        );
        assert_eq!(
            result.iterator_missing, 0,
            "No keys should be missing through iteration"
        );
        assert_eq!(result.iterator_errors, 0, "No iterator errors should occur");

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

        // Now verify the WAL using InspectorContext
        let context = InspectorContext::load(db_path, false)?;
        let result = verify_wal(&context)?;

        // Check results - should only have 2 keys after deletion
        assert_eq!(
            result.total_keys, 2,
            "Should have 2 keys in WAL after deletion"
        );
        assert_eq!(result.verified, 2, "Both remaining keys should be verified");
        assert_eq!(result.missing, 0, "No keys should be missing");
        assert_eq!(result.errors, 0, "No errors should occur");
        assert_eq!(
            result.iterator_verified, 2,
            "Both remaining keys should be verified through iteration"
        );
        assert_eq!(
            result.iterator_missing, 0,
            "No keys should be missing through iteration"
        );
        assert_eq!(result.iterator_errors, 0, "No iterator errors should occur");

        Ok(())
    }
}
