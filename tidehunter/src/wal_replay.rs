//! WAL replay driver.
//!
//! Drives [`WalIterator`] through every WAL frame and accumulates per-cell
//! writes into a [`ReplayBuffer`]. At end-of-stream the buffer is drained
//! into the [`LargeTable`] in bulk via `apply_replay_buffer`:
//!
//!   - one row lock per mutex shard instead of one per WAL record,
//!   - one per-cell sort + flat-buffer build instead of per-record BTreeMap
//!     inserts (see `replay_buffer` for the per-cell accumulator).
//!
//! ## BatchStart handling
//!
//! A `WalEntry::BatchStart(N)` declares the next `N` frames as a single
//! atomic unit — if any of them is missing (CRC fail or EOF before the
//! count is reached) the whole batch must be discarded. The straightforward
//! approach was to buffer those `N` entries into a `VecDeque` and only
//! dispatch once the batch completed; profiling showed the deque was a
//! ~26% bottleneck because its 88-byte slots get evicted from L1 between
//! the push pass and the drain pass.
//!
//! Instead we apply each in-batch record inline, the same as any other
//! WAL record, while remembering the BatchStart's WAL offset. If the batch
//! is interrupted, `apply_replay_buffer` receives that offset as a cutoff
//! and silently drops any per-cell entries with `iwp.offset() >= cutoff`.
//! Per-cell entries are pushed in WAL order — the per-cell buffer is
//! already sorted by offset — so the cutoff translates to a single
//! `partition_point` + `truncate` at apply time.
//!
//! Lives in its own module so `db.rs` doesn't carry the loop body.

use crate::compressed_batch::{InnerIter, decompress_wal_entry};
use crate::context::KsContextVec;
use crate::db::{DbResult, WalEntry};
use crate::key_shape::KeyShape;
use crate::large_table::LargeTable;
use crate::metrics::Metrics;
use crate::replay_buffer::ReplayBuffer;
use crate::wal::{WalError, WalIterator, WalWriter};

pub(crate) fn replay_wal(
    contexts: &KsContextVec,
    large_table: &LargeTable,
    key_shape: &KeyShape,
    mut wal_iterator: WalIterator,
    metrics: &Metrics,
) -> DbResult<WalWriter> {
    let mut buffer = ReplayBuffer::new(key_shape);
    // Number of records still expected inside the currently-open
    // BatchStart group. `0` means we're not inside a batch.
    let mut batch_remaining: u32 = 0;
    // WAL offset of the BatchStart frame. `Some(_)` only while a batch
    // is open; serves both as the `into_writer` rewind argument and as
    // the apply-time cutoff if replay terminates with a partial batch.
    let mut batch_start_position: Option<u64> = None;

    let writer = loop {
        let entry = wal_iterator.next();
        if matches!(entry, Err(WalError::Crc(_) | WalError::EndOfWal)) {
            break wal_iterator.into_writer(batch_start_position);
        }
        let (position, raw_entry) = entry?;
        let entry = WalEntry::from_bytes(raw_entry);

        // Inside a BatchStart group: apply inline and tick down the
        // counter. The `batch_start_position` carried forward lets us
        // drop these records at apply time if the batch never completes.
        if batch_remaining > 0 {
            match entry {
                WalEntry::Record(ks, k, _v, relocated) => {
                    metrics.replayed_wal_records.inc();
                    if !relocated {
                        let context = contexts.ks_context(ks);
                        let reduced_key = context.ks_config.reduced_key_bytes(k);
                        let cell = context.ks_config.cell_id(&reduced_key);
                        buffer.insert(ks, cell, reduced_key, position);
                    }
                }
                WalEntry::Remove(ks, k) => {
                    metrics.replayed_wal_records.inc();
                    let context = contexts.ks_context(ks);
                    let reduced_key = context.ks_config.reduced_key_bytes(k);
                    let cell = context.ks_config.cell_id(&reduced_key);
                    buffer.remove(ks, cell, reduced_key, position);
                }
                other => panic!(
                    "encountered entry {other:?} at position {position:?} during replay, while expected record or tombstone"
                ),
            }
            batch_remaining -= 1;
            if batch_remaining == 0 {
                batch_start_position = None;
            }
            continue;
        }

        match entry {
            WalEntry::Record(ks, k, _v, relocated) => {
                metrics.replayed_wal_records.inc();
                if relocated {
                    // Nothing needs to be done for the relocated record
                    continue;
                }
                let context = contexts.ks_context(ks);
                let reduced_key = context.ks_config.reduced_key_bytes(k);
                let cell = context.ks_config.cell_id(&reduced_key);
                buffer.insert(ks, cell, reduced_key, position);
            }
            WalEntry::Index(_ks, _bytes) => {
                unreachable!("Should not have index entries in wal");
            }
            WalEntry::Remove(ks, k) => {
                metrics.replayed_wal_records.inc();
                let context = contexts.ks_context(ks);
                let reduced_key = context.ks_config.reduced_key_bytes(k);
                let cell = context.ks_config.cell_id(&reduced_key);
                buffer.remove(ks, cell, reduced_key, position);
            }
            WalEntry::BatchStart(size) => {
                // `BatchStart(0)` is unreachable from the writer: only
                // `Db::write_batch_into_wal` (`db.rs:649`) constructs
                // a `BatchStart`, with `entries.len() as u32`, and
                // `Db::do_write_batch` (`db.rs:439-441`) short-circuits
                // empty batches before that call. CRC has already
                // validated this frame's bytes, so a 0 here is a writer
                // bug, not random corruption. Asserting loudly is
                // strictly better than skipping silently — a silent skip
                // would mask the bug and a forged 0 here would still be
                // catastrophic anyway (it would latch
                // `batch_start_position` forever and the apply-time
                // cutoff would drop every record past this frame).
                assert!(
                    size > 0,
                    "WalEntry::BatchStart(0) at {position:?} — writer invariant violated"
                );
                batch_start_position = Some(position.offset());
                batch_remaining = size;
            }
            WalEntry::DropCells(ks, from_cell, to_cell) => {
                metrics.replayed_wal_records.inc();
                let context = contexts.ks_context(ks);
                // Drop on both sides: the buffer (so pre-drop writes don't
                // resurrect at apply time) and `large_table` (in case the
                // cells exist as Unloaded entries from a CR snapshot loaded
                // at replay start).
                buffer.drop_cells_in_range(ks, &from_cell, &to_cell);
                large_table.drop_cells_in_range(context, &from_cell, &to_cell);
            }
            entry @ WalEntry::CompressedBatch(..) => {
                // A compressed batch is one durable WAL frame — either it
                // CRC-verifies (full batch present) or it does not. There is
                // no half-state, so we apply every inner entry
                // unconditionally; all of them share `position`.
                let decompressed = decompress_wal_entry(entry).expect("matched above");
                for inner in InnerIter::new(decompressed) {
                    metrics.replayed_wal_records.inc();
                    match inner {
                        WalEntry::Record(ks, key, _value, _relocated) => {
                            let context = contexts.ks_context(ks);
                            let reduced_key = context.ks_config.reduced_key_bytes(key);
                            let cell = context.ks_config.cell_id(&reduced_key);
                            buffer.insert(ks, cell, reduced_key, position);
                        }
                        WalEntry::Remove(ks, key) => {
                            let context = contexts.ks_context(ks);
                            let reduced_key = context.ks_config.reduced_key_bytes(key);
                            let cell = context.ks_config.cell_id(&reduced_key);
                            buffer.remove(ks, cell, reduced_key, position);
                        }
                        other => panic!(
                            "Unexpected inner entry in CompressedBatch at {position:?}: {other:?}"
                        ),
                    }
                }
            }
        }
    };
    // If replay ended mid-batch, anything we already buffered at or after
    // `batch_start_position` is from an incomplete batch and must be
    // discarded. `None` (= complete or no batch) maps to `u64::MAX` =
    // "keep everything".
    let cutoff = batch_start_position.unwrap_or(u64::MAX);
    large_table.apply_replay_buffer(buffer, cutoff);
    Ok(writer)
}
