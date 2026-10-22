use super::util::format_bytes;
use super::{DbHandle, resolve_ks};
use rhai::{Dynamic, Engine, EvalAltResult, Map};
use tidehunter::CellId;
use tidehunter::WalKind;
use tidehunter::control::ControlRegion;
use tidehunter::db::CONTROL_REGION_FILE;
use tidehunter::index::index_ref::IndexRef;
use tidehunter::key_shape::KeySpace;
use tidehunter::test_utils::{IndexFormat, Metrics, Wal, WalEntry, WalError};

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

pub(crate) fn register(engine: &mut Engine) {
    // --- db.force_snapshot() → i64 ---
    //
    // Opens the database (triggering WAL replay if needed), flushes all dirty
    // entries, and rebuilds the control region.  Returns the WAL position at
    // which the snapshot was taken.
    engine.register_fn(
        "force_snapshot",
        |h: &mut DbHandle| -> Result<i64, Box<EvalAltResult>> {
            let db = h.require_db()?;
            let pos = db
                .force_rebuild_control_region()
                .map_err(|e| -> Box<EvalAltResult> {
                    format!("force_snapshot failed: {e:?}").into()
                })?;
            Ok(pos as i64)
        },
    );

    // --- db.load_cr() → map ---
    //
    // Reads the control region file without opening the database (no WAL replay).
    // Returns a Rhai map with the full raw contents of the ControlRegion struct:
    //
    //   {
    //     "last_position": i64,        // WAL offset at which replay stopped
    //     "keyspaces": [               // one entry per keyspace, in keyspace-ID order
    //       {
    //         "name":        string,
    //         "id":          i64,
    //         "cells": [               // every cell in the snapshot (including empty ones)
    //           {
    //             "cell_id":        i64 | string,  // Integer → i64; Bytes → lowercase hex
    //             "levels": [                      // one entry per populated on-disk blob,
    //               {                              //   freshest (L0) first; empty if no index
    //                 "level":  i64,               // schema slot (0 = L0, 1 = L1, …); shard
    //                                              //   blobs are reported at L1 too
    //                 "offset": i64,               // index WAL offset
    //                 "len":    i64,               // index frame length
    //               },
    //               ...
    //             ],
    //             "shards": [                      // empty unless auto_sharding split L1
    //               {                              //   into per-range sub-blobs
    //                 "min_key": string,           // hex; shard's smallest content key
    //                 "max_key": string,           // hex; shard's largest content key
    //                 "offset":  i64,              // index WAL offset
    //                 "len":     i64,              // index frame length
    //               },
    //               ...
    //             ],
    //             "last_processed": i64,           // WAL offset up to which index is current
    //           },
    //           ...
    //         ]
    //       },
    //       ...
    //     ]
    //   }
    engine.register_fn(
        "load_cr",
        |h: &mut DbHandle| -> Result<Dynamic, Box<EvalAltResult>> {
            let (db_path, key_shape) = {
                let state = h.0.lock();
                (state.db_path.clone(), state.key_shape.clone())
            };

            let control_path = db_path.join(CONTROL_REGION_FILE);
            let cr = ControlRegion::read(&control_path, &key_shape).map_err(
                |e| -> Box<EvalAltResult> { format!("Failed to read control region: {e}").into() },
            )?;

            let snapshot = cr.snapshot();
            let mut ks_array: rhai::Array = Vec::new();

            for (ks_idx, ks_data) in snapshot.data.iter().enumerate() {
                let name = key_shape.ks(KeySpace::new(ks_idx as u8)).name().to_string();

                let cells: rhai::Array = ks_data
                    .iter()
                    .map(|(cell_id, entry)| {
                        let cell_id_val: Dynamic = match cell_id {
                            CellId::Integer(n) => Dynamic::from(*n as i64),
                            CellId::Bytes(b) => Dynamic::from(hex::encode(b.as_slice())),
                        };

                        let levels: rhai::Array = entry
                            .levels
                            .iter_with_level()
                            .map(|(level, pos)| {
                                let mut m = Map::new();
                                m.insert("level".into(), Dynamic::from(level as i64));
                                m.insert("offset".into(), Dynamic::from(pos.offset() as i64));
                                m.insert("len".into(), Dynamic::from(pos.frame_len() as i64));
                                Dynamic::from(m)
                            })
                            .collect();

                        let shards: rhai::Array = entry
                            .levels
                            .shards()
                            .iter()
                            .map(|(min_key, shard)| {
                                let mut m = Map::new();
                                m.insert("min_key".into(), Dynamic::from(hex::encode(min_key)));
                                m.insert(
                                    "max_key".into(),
                                    Dynamic::from(hex::encode(&shard.max_key)),
                                );
                                m.insert(
                                    "offset".into(),
                                    Dynamic::from(shard.index_ref.offset() as i64),
                                );
                                m.insert(
                                    "len".into(),
                                    Dynamic::from(shard.index_ref.frame_len() as i64),
                                );
                                Dynamic::from(m)
                            })
                            .collect();

                        let mut cell_map = Map::new();
                        cell_map.insert("cell_id".into(), cell_id_val);
                        cell_map.insert("levels".into(), Dynamic::from(levels));
                        cell_map.insert("shards".into(), Dynamic::from(shards));
                        cell_map.insert(
                            "last_processed".into(),
                            Dynamic::from(entry.last_processed.as_u64() as i64),
                        );
                        Dynamic::from(cell_map)
                    })
                    .collect();

                let mut ks_map = Map::new();
                ks_map.insert("name".into(), Dynamic::from(name));
                ks_map.insert("id".into(), Dynamic::from(ks_idx as i64));
                ks_map.insert("cells".into(), Dynamic::from(cells));
                ks_array.push(Dynamic::from(ks_map));
            }

            let mut map = Map::new();
            map.insert(
                "last_position".into(),
                Dynamic::from(cr.last_position() as i64),
            );
            map.insert("keyspaces".into(), Dynamic::from(ks_array));
            Ok(Dynamic::from(map))
        },
    );

    // --- db.cr_stats() / db.cr_stats(n) ---
    //
    // Prints a summary of the control region without opening the database:
    //   - Last WAL position
    //   - Valid cells count
    //   - Index WAL total file size vs. space used by live positions → usage %
    //   - WAL position percentiles (P50 / P90 / P99)
    //   - Lowest N cells by position (default n = 10)
    engine.register_fn(
        "cr_stats",
        |h: &mut DbHandle| -> Result<(), Box<EvalAltResult>> { cr_stats_impl(h, 10) },
    );
    engine.register_fn(
        "cr_stats",
        |h: &mut DbHandle, n: i64| -> Result<(), Box<EvalAltResult>> {
            cr_stats_impl(h, n.max(0) as usize)
        },
    );

    // --- db.analyze_ks(ks) → map ---
    //
    // Reads every index entry for the given keyspace from the on-disk index WAL
    // and returns a payload-size distribution map (no WAL replay required):
    //
    //   {
    //     "total_index_entries": i64,   // cells with a valid index position in CR
    //     "total_keys":          i64,   // total key count across all index entries
    //     "total_payload_bytes": i64,
    //     "avg_payload_bytes":   i64,
    //     "min_payload_bytes":   i64,
    //     "max_payload_bytes":   i64,
    //     "p10": i64, "p25": i64, "p50": i64, "p75": i64,
    //     "p90": i64, "p95": i64, "p99": i64,
    //   }
    engine.register_fn(
        "analyze_ks",
        |h: &mut DbHandle, ks: Dynamic| -> Result<rhai::Map, Box<EvalAltResult>> {
            let (db_path, key_shape, config) = {
                let state = h.0.lock();
                (
                    state.db_path.clone(),
                    state.key_shape.clone(),
                    state.config.clone(),
                )
            };

            // Resolve keyspace
            let ks_id = {
                let state = h.0.lock();
                resolve_ks(&state, ks)?.as_u8()
            };
            let ks_space = KeySpace::new(ks_id);
            let ks_desc = key_shape.ks(ks_space);

            // Load CR and collect valid index positions for this keyspace
            let control_path = db_path.join(CONTROL_REGION_FILE);
            let cr = ControlRegion::read(&control_path, &key_shape).map_err(
                |e| -> Box<EvalAltResult> { format!("Failed to read control region: {e}").into() },
            )?;
            let snapshot = cr.snapshot();
            let ks_idx = ks_id as usize;
            if ks_idx >= snapshot.data.len() {
                return Err(format!(
                    "Keyspace id {ks_id} out of range (snapshot has {} keyspaces)",
                    snapshot.data.len()
                )
                .into());
            }

            let mut valid_refs: Vec<IndexRef> = Vec::new();
            for entry in snapshot.data[ks_idx].values() {
                valid_refs.extend(entry.levels.iter());
            }
            let total_index_entries = valid_refs.len();

            // Open index WAL and read each index entry
            let metrics = Metrics::new();
            let index_wal = Wal::open(&db_path, config.wal_layout(WalKind::Index), metrics)
                .map_err(|e| -> Box<EvalAltResult> {
                    format!("Failed to open index WAL: {e:?}").into()
                })?;

            let mut payload_sizes: Vec<usize> = Vec::new();

            for index_ref in &valid_refs {
                let offset = index_ref.offset();
                let mut iter =
                    index_wal
                        .wal_iterator_for_scan(offset)
                        .map_err(|e| -> Box<EvalAltResult> {
                            format!("Failed to seek index WAL to {offset:#x}: {e:?}").into()
                        })?;

                match iter.next() {
                    Ok((_pos, raw_bytes)) => {
                        let entry = WalEntry::from_bytes(raw_bytes);
                        if let WalEntry::Index(_, index_bytes) = entry {
                            let index_table = ks_desc.index_format().deserialize_index(
                                ks_desc,
                                index_bytes,
                                *index_ref,
                            );
                            for (_, wal_pos) in index_table.iter() {
                                payload_sizes.push(wal_pos.payload_len());
                            }
                        }
                    }
                    Err(WalError::EndOfWal) | Err(WalError::Crc(_)) => {}
                    Err(WalError::Io(e)) => {
                        return Err(format!("I/O error reading index at {offset:#x}: {e}").into());
                    }
                }
            }

            // Compute distribution
            payload_sizes.sort_unstable();
            let total_keys = payload_sizes.len();
            let total_payload_bytes: usize = payload_sizes.iter().sum();
            let avg = if total_keys > 0 {
                total_payload_bytes / total_keys
            } else {
                0
            };
            let min = payload_sizes.first().copied().unwrap_or(0);
            let max = payload_sizes.last().copied().unwrap_or(0);

            let pct = |p: usize| -> i64 {
                if payload_sizes.is_empty() {
                    return 0;
                }
                let idx = (payload_sizes.len() * p / 100).min(payload_sizes.len() - 1);
                payload_sizes[idx] as i64
            };

            let mut m = Map::new();
            m.insert(
                "total_index_entries".into(),
                Dynamic::from(total_index_entries as i64),
            );
            m.insert("total_keys".into(), Dynamic::from(total_keys as i64));
            m.insert(
                "total_payload_bytes".into(),
                Dynamic::from(total_payload_bytes as i64),
            );
            m.insert("avg_payload_bytes".into(), Dynamic::from(avg as i64));
            m.insert("min_payload_bytes".into(), Dynamic::from(min as i64));
            m.insert("max_payload_bytes".into(), Dynamic::from(max as i64));
            m.insert("p10".into(), Dynamic::from(pct(10)));
            m.insert("p25".into(), Dynamic::from(pct(25)));
            m.insert("p50".into(), Dynamic::from(pct(50)));
            m.insert("p75".into(), Dynamic::from(pct(75)));
            m.insert("p90".into(), Dynamic::from(pct(90)));
            m.insert("p95".into(), Dynamic::from(pct(95)));
            m.insert("p99".into(), Dynamic::from(pct(99)));
            Ok(m)
        },
    );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn cr_stats_impl(h: &mut DbHandle, lowest_n: usize) -> Result<(), Box<EvalAltResult>> {
    let (db_path, key_shape, print) = {
        let state = h.0.lock();
        (
            state.db_path.clone(),
            state.key_shape.clone(),
            state.print_fn.clone(),
        )
    };

    let control_path = db_path.join(CONTROL_REGION_FILE);
    let cr = ControlRegion::read(&control_path, &key_shape).map_err(|e| -> Box<EvalAltResult> {
        format!("Failed to read control region: {e}").into()
    })?;
    let snapshot = cr.snapshot();

    // Collect all valid positions: (offset, frame_len, ks_idx, level_idx).
    // Each populated level (L0, L1, …) within a cell contributes one tuple,
    // so a cell with both L0 and L1 appears twice — disambiguated by level_idx.
    // Sharded L1 contributes one tuple per shard, all at level=1.
    let mut all_positions: Vec<(u64, usize, usize, usize)> = Vec::new();
    let mut total_position_size = 0u64;
    let mut sharded_cells = 0usize;
    let mut total_shards = 0usize;
    for (ks_idx, ks_data) in snapshot.data.iter().enumerate() {
        for entry in ks_data.values() {
            for (level, pos) in entry.levels.iter_with_level() {
                let off = pos.offset();
                let len = pos.frame_len();
                all_positions.push((off, len, ks_idx, level));
                total_position_size += len as u64;
            }
            if entry.levels.is_sharded() {
                sharded_cells += 1;
                total_shards += entry.levels.shards().len();
            }
        }
    }
    all_positions.sort_by_key(|(off, _, _, _)| *off);

    // Sum index WAL files on disk
    let index_prefix = format!("{}_", WalKind::Index.name());
    let mut total_index_wal_size = 0u64;
    if let Ok(entries) = std::fs::read_dir(&db_path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };
            let id_str = match name.strip_prefix(&index_prefix) {
                Some(s) => s,
                None => continue,
            };
            if u64::from_str_radix(id_str, 16).is_err() {
                continue;
            }
            if let Ok(meta) = std::fs::metadata(&path) {
                total_index_wal_size += meta.len();
            }
        }
    }

    print("Control Region Stats");
    print("====================");
    print(&format!("  Last WAL position : {:#x}", cr.last_position()));
    print(&format!("  Valid level blobs : {}", all_positions.len()));
    if sharded_cells > 0 {
        print(&format!(
            "  Sharded cells     : {sharded_cells} ({total_shards} shard blobs)"
        ));
    }

    print("");
    print("Index WAL Space:");
    print(&format!(
        "  Total files size  : {}",
        format_bytes(total_index_wal_size as usize)
    ));
    print(&format!(
        "  Used by positions : {}",
        format_bytes(total_position_size as usize)
    ));
    if total_index_wal_size > 0 {
        let pct = total_position_size as f64 / total_index_wal_size as f64 * 100.0;
        print(&format!("  Usage             : {pct:.1}%"));
    }

    // Position percentiles (computed from sorted positions)
    if !all_positions.is_empty() {
        let offsets: Vec<u64> = all_positions.iter().map(|(off, _, _, _)| *off).collect();
        let pct = |p: usize| -> u64 {
            let idx = (offsets.len() * p / 100).min(offsets.len() - 1);
            offsets[idx]
        };
        print("");
        print("WAL Position Percentiles:");
        print(&format!("  P50 : {:#x}", pct(50)));
        print(&format!("  P90 : {:#x}", pct(90)));
        print(&format!("  P99 : {:#x}", pct(99)));
    }

    // Lowest N positions
    if !all_positions.is_empty() && lowest_n > 0 {
        let n = lowest_n.min(all_positions.len());
        print("");
        print(&format!("Lowest {n} level blobs by WAL position:"));
        for (i, (offset, _len, ks_idx, level)) in all_positions.iter().take(n).enumerate() {
            let ks_name = key_shape
                .ks(KeySpace::new(*ks_idx as u8))
                .name()
                .to_string();
            print(&format!(
                "  {:2}. offset={offset:#010x}  ks={ks_name}  L{level}",
                i + 1
            ));
        }
    }

    Ok(())
}
