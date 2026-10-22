use super::util::{format_bytes, format_count};
use super::{DbHandle, Entry, from_wal_entry};
use rhai::{Dynamic, Engine, EvalAltResult, FnPtr, Map, NativeCallContext};
use std::collections::BTreeMap;
use std::path::Path;
use tidehunter::WalKind;
use tidehunter::config::Config;
use tidehunter::control::ControlRegion;
use tidehunter::db::CONTROL_REGION_FILE;
use tidehunter::key_shape::KeySpace;
use tidehunter::test_utils::{
    IndexFormat, Metrics, Wal, WalEntry, WalError, list_wal_files_with_sizes,
};

// ---------------------------------------------------------------------------
// WAL walking helper (shared by walk_wal and wal_stats)
// ---------------------------------------------------------------------------

pub(crate) fn do_walk_wal<F>(
    db_path: &Path,
    config: &Config,
    start_pos: u64,
    skip_crc: bool,
    mut f: F,
) -> Result<(), Box<EvalAltResult>>
where
    F: FnMut(Entry) -> Result<(), Box<EvalAltResult>>,
{
    let metrics = Metrics::new();
    let wal = Wal::open(db_path, config.wal_layout(WalKind::Replay), metrics)
        .map_err(|e| -> Box<EvalAltResult> { format!("Failed to open WAL: {e:?}").into() })?;

    let mut iter = wal
        .wal_iterator_for_scan(start_pos)
        .map_err(|e| -> Box<EvalAltResult> {
            format!("Failed to create WAL iterator: {e:?}").into()
        })?;
    iter.set_skip_crc(skip_crc);

    loop {
        match iter.next() {
            Ok((position, raw_bytes)) => {
                let raw_size = raw_bytes.len();
                let offset = position.offset();
                let entry = from_wal_entry(WalEntry::from_bytes(raw_bytes), offset, raw_size);
                f(entry)?;
            }
            // CRC errors and EndOfWal both signal the end of written data
            Err(WalError::Crc(_)) | Err(WalError::EndOfWal) => break,
            Err(WalError::Io(e)) => {
                return Err(format!("WAL I/O error: {e}").into());
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

pub(crate) fn register(engine: &mut Engine) {
    // --- db.walk_wal(|entry| { ... }) ---
    engine.register_fn(
        "walk_wal",
        |native_ctx: NativeCallContext,
         h: &mut DbHandle,
         visitor: FnPtr|
         -> Result<Dynamic, Box<EvalAltResult>> {
            let (db_path, config) = {
                let state = h.0.lock();
                (state.db_path.clone(), state.config.clone())
            };
            do_walk_wal(&db_path, &config, 0, false, |entry| {
                let _: Dynamic = visitor.call_within_context::<Dynamic>(&native_ctx, (entry,))?;
                Ok(())
            })?;
            Ok(Dynamic::UNIT)
        },
    );

    // --- db.walk_wal(start_pos, |entry| { ... }) ---
    engine.register_fn(
        "walk_wal",
        |native_ctx: NativeCallContext,
         h: &mut DbHandle,
         start_pos: i64,
         visitor: FnPtr|
         -> Result<Dynamic, Box<EvalAltResult>> {
            let (db_path, config) = {
                let state = h.0.lock();
                (state.db_path.clone(), state.config.clone())
            };
            let start = u64::try_from(start_pos)
                .map_err(|_| -> Box<EvalAltResult> { "start_pos must be non-negative".into() })?;
            do_walk_wal(&db_path, &config, start, false, |entry| {
                let _: Dynamic = visitor.call_within_context::<Dynamic>(&native_ctx, (entry,))?;
                Ok(())
            })?;
            Ok(Dynamic::UNIT)
        },
    );

    // --- db.walk_wal_unchecked(|entry| { ... }) — skips CRC verification ---
    engine.register_fn(
        "walk_wal_unchecked",
        |native_ctx: NativeCallContext,
         h: &mut DbHandle,
         visitor: FnPtr|
         -> Result<Dynamic, Box<EvalAltResult>> {
            let (db_path, config) = {
                let state = h.0.lock();
                (state.db_path.clone(), state.config.clone())
            };
            do_walk_wal(&db_path, &config, 0, true, |entry| {
                let _: Dynamic = visitor.call_within_context::<Dynamic>(&native_ctx, (entry,))?;
                Ok(())
            })?;
            Ok(Dynamic::UNIT)
        },
    );

    // --- db.walk_wal_unchecked(start_pos, |entry| { ... }) ---
    engine.register_fn(
        "walk_wal_unchecked",
        |native_ctx: NativeCallContext,
         h: &mut DbHandle,
         start_pos: i64,
         visitor: FnPtr|
         -> Result<Dynamic, Box<EvalAltResult>> {
            let (db_path, config) = {
                let state = h.0.lock();
                (state.db_path.clone(), state.config.clone())
            };
            let start = u64::try_from(start_pos)
                .map_err(|_| -> Box<EvalAltResult> { "start_pos must be non-negative".into() })?;
            do_walk_wal(&db_path, &config, start, true, |entry| {
                let _: Dynamic = visitor.call_within_context::<Dynamic>(&native_ctx, (entry,))?;
                Ok(())
            })?;
            Ok(Dynamic::UNIT)
        },
    );

    // --- db.wal_stats() ---
    engine.register_fn(
        "wal_stats",
        |h: &mut DbHandle| -> Result<(), Box<EvalAltResult>> {
            let (db_path, config, key_shape, print) = {
                let state = h.0.lock();
                (
                    state.db_path.clone(),
                    state.config.clone(),
                    state.key_shape.clone(),
                    state.print_fn.clone(),
                )
            };

            // Counters indexed by type: [record, remove, index, batch_start, drop_cells]
            let mut counts = [0usize; 5];
            let mut type_bytes = [0usize; 5];
            let mut total_bytes = 0usize;
            // Key size stats (record + remove entries)
            let mut min_key = usize::MAX;
            let mut max_key = 0usize;
            let mut total_key_bytes = 0usize;
            let mut key_entry_count = 0usize;
            // Value size stats (record entries only)
            let mut min_val = usize::MAX;
            let mut max_val = 0usize;
            let mut total_val_bytes = 0usize;
            // Per-KS: (records, removes, bytes)
            let mut ks_counts: BTreeMap<u8, (usize, usize, usize)> = BTreeMap::new();

            do_walk_wal(&db_path, &config, 0, false, |e| {
                let raw = e.raw_size as usize;
                total_bytes += raw;
                match e.entry_type.as_str() {
                    "record" => {
                        counts[0] += 1;
                        type_bytes[0] += raw;
                        let kl = e.key.len();
                        let vl = e.value_len as usize;
                        min_key = min_key.min(kl);
                        max_key = max_key.max(kl);
                        total_key_bytes += kl;
                        key_entry_count += 1;
                        min_val = min_val.min(vl);
                        max_val = max_val.max(vl);
                        total_val_bytes += vl;
                        let s = ks_counts.entry(e.keyspace as u8).or_default();
                        s.0 += 1;
                        s.2 += raw;
                    }
                    "remove" => {
                        counts[1] += 1;
                        type_bytes[1] += raw;
                        let kl = e.key.len();
                        min_key = min_key.min(kl);
                        max_key = max_key.max(kl);
                        total_key_bytes += kl;
                        key_entry_count += 1;
                        let s = ks_counts.entry(e.keyspace as u8).or_default();
                        s.1 += 1;
                        s.2 += raw;
                    }
                    "index" => {
                        counts[2] += 1;
                        type_bytes[2] += raw;
                    }
                    "batch_start" => {
                        counts[3] += 1;
                        type_bytes[3] += raw;
                    }
                    "drop_cells" => {
                        counts[4] += 1;
                        type_bytes[4] += raw;
                    }
                    _ => {}
                }
                Ok(())
            })?;

            let total = counts.iter().sum::<usize>();
            print("WAL Statistics");
            print("==============");
            print(&format!("  Total entries : {}", format_count(total)));
            print(&format!("  Total bytes   : {}", format_bytes(total_bytes)));

            // Per-type breakdown table
            const TYPE_NAMES: [&str; 5] = ["Record", "Remove", "Index", "BatchStart", "DropCells"];
            print("");
            print(&format!(
                "  {:<14} {:>10} {:>12} {:>10} {:>8}",
                "Type", "Count", "Size", "Avg Size", "% Space"
            ));
            print(&format!("  {}", "-".repeat(58)));
            for (i, name) in TYPE_NAMES.iter().enumerate() {
                if counts[i] == 0 {
                    continue;
                }
                let avg = type_bytes[i] / counts[i];
                let pct = if total_bytes > 0 {
                    type_bytes[i] as f64 / total_bytes as f64 * 100.0
                } else {
                    0.0
                };
                print(&format!(
                    "  {:<14} {:>10} {:>12} {:>10} {:>7.1}%",
                    name,
                    format_count(counts[i]),
                    format_bytes(type_bytes[i]),
                    format_bytes(avg),
                    pct,
                ));
            }

            // Key size stats
            if key_entry_count > 0 && min_key != usize::MAX {
                let avg_key = total_key_bytes / key_entry_count;
                print("");
                print("Key Statistics:");
                print(&format!(
                    "  Min : {:>10}   Max : {:>10}   Avg : {:>10}",
                    format_bytes(min_key),
                    format_bytes(max_key),
                    format_bytes(avg_key),
                ));
            }

            // Value size stats
            if counts[0] > 0 && min_val != usize::MAX {
                let avg_val = total_val_bytes / counts[0];
                print("");
                print("Value Statistics:");
                print(&format!(
                    "  Min : {:>10}   Max : {:>10}   Avg : {:>10}",
                    format_bytes(min_val),
                    format_bytes(max_val),
                    format_bytes(avg_val),
                ));
            }

            // Per-keyspace breakdown
            if !ks_counts.is_empty() {
                print("");
                print("Per-keyspace:");
                print(&format!(
                    "  {:<35} {:>10} {:>10} {:>12}",
                    "Keyspace", "Records", "Removes", "Size"
                ));
                print(&format!("  {}", "-".repeat(72)));
                for (ks_id, (recs, rems, bytes)) in &ks_counts {
                    let name = key_shape.ks(KeySpace::new(*ks_id)).name().to_string();
                    print(&format!(
                        "  {:<35} {:>10} {:>10} {:>12}",
                        name,
                        format_count(*recs),
                        format_count(*rems),
                        format_bytes(*bytes),
                    ));
                }
            }
            Ok(())
        },
    );

    // --- db.list_wal_files() ---
    engine.register_fn(
        "list_wal_files",
        |h: &mut DbHandle| -> Result<rhai::Array, Box<EvalAltResult>> {
            let (db_path, config) = {
                let state = h.0.lock();
                (state.db_path.clone(), state.config.clone())
            };

            let wal_file_size = config.wal_layout(WalKind::Replay).wal_file_size;

            let files = list_wal_files_with_sizes(&db_path).map_err(|e| -> Box<EvalAltResult> {
                format!("Failed to list WAL files: {e:?}").into()
            })?;

            let result = files
                .into_iter()
                .map(|(path, size)| {
                    let file_name = path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("")
                        .to_string();

                    // File name: "wal_{id:016x}" — parse ID to derive start position.
                    let id = file_name
                        .strip_prefix("wal_")
                        .and_then(|s| u64::from_str_radix(s, 16).ok())
                        .unwrap_or(0);
                    let start_pos = (id * wal_file_size) as i64;

                    let created = std::fs::metadata(&path)
                        .ok()
                        .and_then(|m| m.created().ok().or_else(|| m.modified().ok()))
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(-1);

                    let mut map = rhai::Map::new();
                    map.insert("name".into(), Dynamic::from(file_name));
                    map.insert("start_pos".into(), Dynamic::from(start_pos));
                    map.insert("size".into(), Dynamic::from(size as i64));
                    map.insert("created".into(), Dynamic::from(created));
                    Dynamic::from(map)
                })
                .collect();

            Ok(result)
        },
    );

    // --- db.load_index(offset) ---
    //
    // Reads the index entry stored in the INDEX WAL at the given byte offset
    // (as returned by db.load_cr() cell.levels[i].offset) and returns the
    // key→data-WAL-position mapping stored in that on-disk index.
    //
    // Returns an array of maps:
    //   [ { "key": "<hex>", "wal_position": <i64> }, ... ]
    //
    // Useful for verifying whether a specific write is reflected in the on-disk index
    // without doing a full WAL replay.
    engine.register_fn(
        "load_index",
        |h: &mut DbHandle, offset: i64| -> Result<rhai::Array, Box<EvalAltResult>> {
            let (db_path, config, key_shape) = {
                let state = h.0.lock();
                (
                    state.db_path.clone(),
                    state.config.clone(),
                    state.key_shape.clone(),
                )
            };

            if offset < 0 {
                return Err(format!("Invalid index offset {offset}: negative offsets indicate no index has been written for this cell").into());
            }
            let offset = offset as u64;

            let metrics = Metrics::new();
            let index_wal =
                Wal::open(&db_path, config.wal_layout(WalKind::Index), metrics).map_err(
                    |e| -> Box<EvalAltResult> {
                        format!("Failed to open index WAL: {e:?}").into()
                    },
                )?;

            let mut iter = index_wal
                .wal_iterator_for_scan(offset)
                .map_err(|e| -> Box<EvalAltResult> {
                    format!("Failed to create index WAL iterator at offset {offset}: {e:?}").into()
                })?;

            let (position, raw_bytes) = iter.next().map_err(|e| -> Box<EvalAltResult> {
                format!("Failed to read index WAL entry at offset {offset}: {e:?}").into()
            })?;

            if position.offset() != offset {
                return Err(format!(
                    "Index WAL iterator landed at offset {} instead of requested {offset}; \
                     the offset may not be frame-aligned",
                    position.offset()
                )
                .into());
            }

            let entry = WalEntry::from_bytes(raw_bytes);
            let (ks_id, bytes) = match entry {
                WalEntry::Index(ks, bytes) => (ks, bytes),
                other => {
                    return Err(format!(
                        "Expected an Index entry at offset {offset} but found {:?}",
                        std::mem::discriminant(&other)
                    )
                    .into());
                }
            };

            let ks_desc = key_shape.ks(KeySpace::new(ks_id.as_u8()));
            // The blob's micro-cell count lives in its ref, which the control
            // region holds for every live blob.
            let control_region =
                ControlRegion::read_or_create(&db_path.join(CONTROL_REGION_FILE), &key_shape);
            let Some(index_ref) = control_region.index_ref_at(offset) else {
                return Err(format!(
                    "Index blob at offset {offset} is not referenced by the control region, \
                     so its micro-cell count is unknown"
                )
                .into());
            };
            let index_table =
                ks_desc
                    .index_format()
                    .deserialize_index(ks_desc, bytes, index_ref);

            let result: rhai::Array = index_table
                .iter()
                .map(|(key, wal_pos)| {
                    let mut m = Map::new();
                    m.insert("key".into(), Dynamic::from(hex::encode(&key)));
                    m.insert(
                        "wal_position".into(),
                        Dynamic::from(wal_pos.offset() as i64),
                    );
                    m.insert(
                        "payload_len".into(),
                        Dynamic::from(wal_pos.payload_len() as i64),
                    );
                    Dynamic::from(m)
                })
                .collect();

            Ok(result)
        },
    );
}
