// ---------------------------------------------------------------------------
// Flat binary buffer helpers for IndexTable
//
// Two in-memory formats, selected by `IndexTable::key_size`:
//
//   Variable-length (key_size == None):
//     [count: u32][offsets: u32 * count][key_len: u16][key][wal_offset: u64][encoded_len: u32]*
//     Offsets enable O(log n) binary search without scanning entries.
//
//   Fixed-length (key_size == Some(n)):
//     [key (n bytes)][wal_offset: u64][encoded_len: u32]*
//     No per-entry overhead; count = flat.len() / (n + 12).
//
// `encoded_len` packs the entry kind into the top 2 bits of the 4-byte length
// field.  WAL frame sizes are always < 1 GiB (2^30), so bits 30-31 are free:
//   bits 31-30 = kind  (0 = Clean, 1 = Modified, 2 = Removed)
//   bits 29-0  = actual WAL frame length
//
// The flat buffer is purely in-memory and never persisted.
// ---------------------------------------------------------------------------

use super::{DataOverlay, IndexEntryKind, IndexWalPosition, data_latest_per_key};
use crate::wal::position::{LastProcessed, WalPosition};
use bytes::BytesMut;
use minibytes::Bytes;
use std::cmp::Ordering;

// ---- Variable-length helpers (operate on the flat buffer directly) --------

fn var_flat_entry_count(flat: &[u8]) -> usize {
    if flat.len() < 4 {
        return 0;
    }
    u32::from_be_bytes(flat[0..4].try_into().unwrap()) as usize
}

fn var_flat_entries_section_start(count: usize) -> usize {
    4 + 4 * count
}

fn var_flat_read_entry_offset(flat: &[u8], idx: usize) -> usize {
    let pos = 4 + 4 * idx;
    u32::from_be_bytes(flat[pos..pos + 4].try_into().unwrap()) as usize
}

/// Encode `kind` into the top 2 bits of `len`. `len` must be < 2^30 (asserted in `Wal::multi_write`).
/// Tombstones read from disk carry `WalPosition::INVALID` whose len is `u32::MAX` —
/// `IndexWalPosition::from_disk` normalises that to `0` so the encoding here stays lossless.
#[inline]
pub(super) fn encode_kind_in_len(len: u32, kind: IndexEntryKind) -> u32 {
    assert!(
        len < (1 << 30),
        "encode_kind_in_len: len must be < 2^30 (got 0x{len:08x}) — kind bits collide",
    );
    len | ((kind.to_u8() as u32) << 30)
}

/// Extract `kind` from the top 2 bits and the actual `len` from the bottom 30 bits.
#[inline]
pub(super) fn decode_kind_from_len(encoded: u32) -> (u32, IndexEntryKind) {
    (
        encoded & 0x3FFF_FFFF,
        IndexEntryKind::from_u8((encoded >> 30) as u8),
    )
}

/// Parse a `(key_slice, IndexWalPosition)` from the var-len entries section at `byte_offset`.
/// Layout: [key_len: u16][key][wal_offset: u64][encoded_len: u32]
fn var_flat_parse_entry(entries_section: &[u8], byte_offset: usize) -> (&[u8], IndexWalPosition) {
    let key_len = u16::from_be_bytes(
        entries_section[byte_offset..byte_offset + 2]
            .try_into()
            .unwrap(),
    ) as usize;
    let key = &entries_section[byte_offset + 2..byte_offset + 2 + key_len];
    let wal_start = byte_offset + 2 + key_len;
    let wal_offset = u64::from_be_bytes(
        entries_section[wal_start..wal_start + 8]
            .try_into()
            .unwrap(),
    );
    let encoded = u32::from_be_bytes(
        entries_section[wal_start + 8..wal_start + 12]
            .try_into()
            .unwrap(),
    );
    let (wal_len, kind) = decode_kind_from_len(encoded);
    (
        key,
        IndexWalPosition {
            offset: wal_offset,
            len: wal_len,
            kind,
        },
    )
}

// ---------------------------------------------------------------------------
// Shared flat-buffer helpers (operate on raw `&[u8]`, format-agnostic)
// ---------------------------------------------------------------------------

/// Returns the number of entries encoded in `flat`.
pub(super) fn flat_count(flat: &[u8], key_size: Option<usize>) -> usize {
    if flat.is_empty() {
        return 0;
    }
    match key_size {
        None => var_flat_entry_count(flat),
        Some(ks) => flat.len() / (ks + WalPosition::LENGTH),
    }
}

/// Returns the key slice and `IndexWalPosition` for the entry at position `idx`.
/// The returned key slice borrows from `flat`.
/// Panics if `idx >= flat_count(flat, key_size)`.
pub(super) fn flat_entry_at(
    flat: &[u8],
    key_size: Option<usize>,
    idx: usize,
) -> (&[u8], IndexWalPosition) {
    match key_size {
        None => {
            let count = var_flat_entry_count(flat);
            let entries_start = var_flat_entries_section_start(count);
            let entries = &flat[entries_start..];
            let off = var_flat_read_entry_offset(flat, idx);
            var_flat_parse_entry(entries, off)
        }
        Some(ks) => {
            let elem_size = ks + WalPosition::LENGTH;
            let start = idx * elem_size;
            let key = &flat[start..start + ks];
            let wal_offset =
                u64::from_be_bytes(flat[start + ks..start + ks + 8].try_into().unwrap());
            let encoded =
                u32::from_be_bytes(flat[start + ks + 8..start + elem_size].try_into().unwrap());
            let (len, kind) = decode_kind_from_len(encoded);
            (
                key,
                IndexWalPosition {
                    offset: wal_offset,
                    len,
                    kind,
                },
            )
        }
    }
}

/// Lower bound: the smallest index `i` such that `flat[i].key >= target`.
/// Returns `flat_count(flat, key_size)` when all keys are less than `target`.
pub(super) fn flat_lower_bound(flat: &[u8], key_size: Option<usize>, target: &[u8]) -> usize {
    let count = flat_count(flat, key_size);
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let (mid_key, _) = flat_entry_at(flat, key_size, mid);
        if mid_key < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Upper bound: the smallest index `i` such that `flat[i].key > target`.
/// Returns `flat_count(flat, key_size)` when all keys are <= `target`.
pub(super) fn flat_upper_bound(flat: &[u8], key_size: Option<usize>, target: &[u8]) -> usize {
    let count = flat_count(flat, key_size);
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let (mid_key, _) = flat_entry_at(flat, key_size, mid);
        if mid_key <= target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Build a flat buffer from a sorted list of `(key, IndexWalPosition)` pairs.
///
/// Pass `key_size = Some(n)` for fixed-length key spaces; `None` for variable-length.
///
/// Fixed format:    `[key (n)][wal_offset: u64][encoded_len: u32]*`
/// Variable format: `[count: u32][offsets: u32*n][key_len: u16][key][wal_offset: u64][encoded_len: u32]*`
///
/// `encoded_len` packs the entry kind into the top 2 bits (see `encode_kind_in_len`).
pub(super) fn build_flat_bytes(
    entries: Vec<(Bytes, IndexWalPosition)>,
    key_size: Option<usize>,
) -> Bytes {
    let n = entries.len();
    if n == 0 {
        return Bytes::default();
    }

    if let Some(key_size) = key_size {
        // Fixed-length format: [key (key_size)][wal_offset (8)][encoded_len (4)]
        let elem_size = key_size + WalPosition::LENGTH;
        let mut result = Vec::with_capacity(n * elem_size);
        for (key, iwp) in &entries {
            debug_assert_eq!(key.len(), key_size, "key length mismatch in fixed flat");
            result.extend_from_slice(key);
            result.extend_from_slice(&iwp.offset.to_be_bytes());
            result.extend_from_slice(&encode_kind_in_len(iwp.len, iwp.kind).to_be_bytes());
        }
        Bytes::from(result)
    } else {
        // Variable-length format: [count: u32][offsets: u32*n][key_len: u16][key][wal_offset (8)][encoded_len (4)]*
        // Each entry contributes 14 + key.len() bytes (2 + key + 8 + 4) to the entries section;
        // the header is 4 (count) + 4*n (offset table). Pre-size the buffer so `append_flat_varlen`
        // writes into a single allocation.
        let total_key_bytes: usize = entries.iter().map(|(k, _)| k.len()).sum();
        let total_size = 4 + 4 * n + 14 * n + total_key_bytes;
        let mut out = BytesMut::with_capacity(total_size);
        append_flat_varlen(&entries, &mut out);
        debug_assert_eq!(out.len(), total_size, "varlen flat size mismatch");
        Bytes::from(out.freeze())
    }
}

/// Append variable-length entries directly into `out`, avoiding a separate allocation.
///
/// Format: `[count: u32][offsets: u32*n][key_len: u16][key][wal_offset: u64][encoded_len: u32]*`
///
/// Offsets are relative to the start of the entries section (immediately after the offset table),
/// which is the same convention used by `build_flat_bytes` and the read-side helpers.
pub(super) fn append_flat_varlen(entries: &[(Bytes, IndexWalPosition)], out: &mut BytesMut) {
    let n = entries.len();
    if n == 0 {
        return;
    }
    let header_start = out.len();
    let offsets_start = header_start + 4; // after count field
    let data_start = offsets_start + 4 * n; // after offset table

    // Reserve header: [count: u32][offsets: u32 * n] — placeholders filled in below.
    out.extend_from_slice(&(n as u32).to_be_bytes());
    for _ in 0..n {
        out.extend_from_slice(&0u32.to_be_bytes());
    }

    // Write data entries and back-fill each offset as we go.
    for (i, (key, iwp)) in entries.iter().enumerate() {
        let entry_offset: u32 = (out.len() - data_start)
            .try_into()
            .expect("flat buffer exceeds u32 offset range");
        out[offsets_start + 4 * i..offsets_start + 4 * i + 4]
            .copy_from_slice(&entry_offset.to_be_bytes());
        out.extend_from_slice(&(key.len() as u16).to_be_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(&iwp.offset.to_be_bytes());
        out.extend_from_slice(&encode_kind_in_len(iwp.len, iwp.kind).to_be_bytes());
    }

    // Back-fill the count field.
    out[header_start..header_start + 4].copy_from_slice(&(n as u32).to_be_bytes());
}

// ---------------------------------------------------------------------------
// Sequential flat iterator
// ---------------------------------------------------------------------------

pub(super) enum FlatIter<'a> {
    Empty,
    VarLen {
        flat: &'a [u8], // full flat buffer, starts with count field
        entries_section: &'a [u8],
        count: usize,
        index: usize,
    },
    Fixed {
        entries: &'a [u8], // full flat buffer: packed key+pos elements
        key_size: usize,
        count: usize,
        index: usize,
    },
}

impl<'a> FlatIter<'a> {
    pub(super) fn new(flat: &'a [u8], key_size: Option<usize>) -> Self {
        if flat.is_empty() {
            return Self::Empty;
        }
        match key_size {
            None => {
                let count = var_flat_entry_count(flat);
                let section_start = var_flat_entries_section_start(count);
                let entries_section = if count > 0 {
                    &flat[section_start..]
                } else {
                    &[]
                };
                Self::VarLen {
                    flat,
                    entries_section,
                    count,
                    index: 0,
                }
            }
            Some(key_size) => {
                let elem_size = key_size + WalPosition::LENGTH;
                let count = if elem_size > 0 {
                    flat.len() / elem_size
                } else {
                    0
                };
                Self::Fixed {
                    entries: flat,
                    key_size,
                    count,
                    index: 0,
                }
            }
        }
    }
}

impl<'a> Iterator for FlatIter<'a> {
    type Item = (&'a [u8], IndexWalPosition);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Empty => None,
            Self::VarLen {
                flat,
                entries_section,
                count,
                index,
            } => {
                if *index >= *count {
                    return None;
                }
                let byte_offset = var_flat_read_entry_offset(flat, *index);
                let (key, iwp) = var_flat_parse_entry(entries_section, byte_offset);
                *index += 1;
                Some((key, iwp))
            }
            Self::Fixed {
                entries,
                key_size,
                count,
                index,
            } => {
                if *index >= *count {
                    return None;
                }
                // Fixed layout per entry: [key (key_size)][wal_offset (8)][encoded_len (4)]
                let elem_size = *key_size + WalPosition::LENGTH;
                let start = *index * elem_size;
                let key = &entries[start..start + *key_size];
                let wal_offset = u64::from_be_bytes(
                    entries[start + *key_size..start + *key_size + 8]
                        .try_into()
                        .unwrap(),
                );
                let encoded = u32::from_be_bytes(
                    entries[start + *key_size + 8..start + elem_size]
                        .try_into()
                        .unwrap(),
                );
                let (len, kind) = decode_kind_from_len(encoded);
                *index += 1;
                Some((
                    key,
                    IndexWalPosition {
                        offset: wal_offset,
                        len,
                        kind,
                    },
                ))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Merge writer (flat + BTreeMap → new flat) used by `promote_to_flat`
// ---------------------------------------------------------------------------

/// Walk the merge of `flat` (decoded via `FlatIter`) with the sorted `data`
/// set, invoking `f` once per emitted entry in sorted-key order. Data
/// entries override flat entries when the keys are equal.
///
/// For each key, `data` may carry multiple positions — only the *latest*
/// (largest IWP) participates in the merge. If that latest is unprocessed
/// (offset >= last_processed), the entire key is skipped (and any matching
/// flat entry is kept as-is). Pass `LastProcessed::new(u64::MAX)` to merge
/// everything.
fn merge_walk(
    flat: &[u8],
    key_size: Option<usize>,
    data: &DataOverlay,
    last_processed: LastProcessed,
    mut f: impl FnMut(&[u8], IndexWalPosition),
) {
    let mut flat_iter = FlatIter::new(flat, key_size);
    let mut data_iter = data_latest_per_key(data).filter(|(_, v)| last_processed.is_processed(*v));
    let mut flat_cur = flat_iter.next();
    let mut data_cur = data_iter.next();

    loop {
        let order = match (&flat_cur, &data_cur) {
            (None, None) => break,
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some(fl), Some(bt)) => fl.0.cmp(bt.0.as_ref()),
        };
        match order {
            Ordering::Less => {
                let (fk, fiwp) = flat_cur.take().unwrap();
                f(fk, fiwp);
                flat_cur = flat_iter.next();
            }
            Ordering::Greater => {
                let (bk, biwp) = data_cur.take().unwrap();
                f(bk.as_ref(), *biwp);
                data_cur = data_iter.next();
            }
            Ordering::Equal => {
                // Data entry overrides flat entry on equal keys.
                flat_cur = flat_iter.next();
                let (bk, biwp) = data_cur.take().unwrap();
                f(bk.as_ref(), *biwp);
                data_cur = data_iter.next();
            }
        }
    }
}

/// Merge `flat` and `data` into a new flat buffer with a single allocation.
///
/// `last_processed` filters the data side: for each key, only the *latest*
/// position participates and only if its WAL offset is strictly below
/// `last_processed`. Keys whose latest position is unprocessed are skipped
/// entirely (the existing flat entry, if any, is kept). Pass
/// `LastProcessed::new(u64::MAX)` to merge everything.
///
/// Two passes over the merge:
///  1. Sizing: count emitted entries (and total key bytes for var-len) and
///     dirty entries to compute the exact output size.
///  2. Fill: write entries directly into a pre-sized `BytesMut`.
///
/// Returns the new flat buffer and the dirty-entry count.
pub(super) fn merge_into_flat(
    flat: &[u8],
    key_size: Option<usize>,
    data: &DataOverlay,
    last_processed: LastProcessed,
) -> (Bytes, usize) {
    let mut entry_count: usize = 0;
    let mut total_key_bytes: usize = 0;
    let mut dirty_count: usize = 0;
    merge_walk(flat, key_size, data, last_processed, |key, iwp| {
        entry_count += 1;
        total_key_bytes += key.len();
        if !iwp.is_clean() {
            dirty_count += 1;
        }
    });

    if entry_count == 0 {
        return (Bytes::default(), 0);
    }

    let new_flat = match key_size {
        Some(ks) => {
            // Fixed-len: [key (ks)][wal_offset: u64][encoded_len: u32]*
            let elem_size = ks + WalPosition::LENGTH;
            let total_size = entry_count * elem_size;
            let mut out = BytesMut::with_capacity(total_size);
            merge_walk(flat, key_size, data, last_processed, |key, iwp| {
                debug_assert_eq!(key.len(), ks, "key length mismatch in fixed flat");
                out.extend_from_slice(key);
                out.extend_from_slice(&iwp.offset.to_be_bytes());
                out.extend_from_slice(&encode_kind_in_len(iwp.len, iwp.kind).to_be_bytes());
            });
            debug_assert_eq!(out.len(), total_size, "fixed flat size mismatch");
            Bytes::from(out.freeze())
        }
        None => {
            // Var-len: [count: u32][offsets: u32 * n][key_len: u16][key][wal_offset: u64][encoded_len: u32]*
            // Per entry contributes 14 + key.len() bytes (2 + key + 8 + 4) to the entries section.
            let header_size = 4 + 4 * entry_count;
            let entries_size = entry_count * 14 + total_key_bytes;
            let total_size = header_size + entries_size;
            let mut out = BytesMut::with_capacity(total_size);
            out.extend_from_slice(&(entry_count as u32).to_be_bytes());
            let offsets_start = out.len();
            // Reserve the offset table; back-fill each slot as we write entries.
            out.resize(offsets_start + 4 * entry_count, 0);
            let data_start = out.len();
            let mut i: usize = 0;
            merge_walk(flat, key_size, data, last_processed, |key, iwp| {
                let entry_offset: u32 = (out.len() - data_start)
                    .try_into()
                    .expect("flat buffer exceeds u32 offset range");
                out[offsets_start + 4 * i..offsets_start + 4 * i + 4]
                    .copy_from_slice(&entry_offset.to_be_bytes());
                out.extend_from_slice(&(key.len() as u16).to_be_bytes());
                out.extend_from_slice(key);
                out.extend_from_slice(&iwp.offset.to_be_bytes());
                out.extend_from_slice(&encode_kind_in_len(iwp.len, iwp.kind).to_be_bytes());
                i += 1;
            });
            debug_assert_eq!(out.len(), total_size, "varlen flat size mismatch");
            Bytes::from(out.freeze())
        }
    };

    (new_flat, dirty_count)
}
