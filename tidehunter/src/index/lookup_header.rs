use std::time::Instant;

use std::collections::BTreeMap;

use bytes::{Buf, BufMut, BytesMut};
use minibytes::Bytes;

use crate::index::index_table::{IndexSerializationVisitor, IndexTable};
use crate::math::{next_bounded, rescale_u32};
use crate::metrics::{Metrics, TimerExt};
use crate::wal::position::WalPosition;
use crate::{key_shape::KeySpaceDesc, lookup::RandomRead};

use super::index_format::{
    Direction, HEADER_ELEMENT_SIZE, HEADER_ELEMENTS, HEADER_SIZE, IndexFormat, IndexIterCache,
    binary_search,
};

#[derive(Clone)]
pub struct LookupHeaderIndex;

impl LookupHeaderIndex {
    fn key_micro_cell(ks: &KeySpaceDesc, key: &[u8]) -> usize {
        let prefix = ks.index_prefix_u32(key);
        let cell = ks.cell_id(key);
        let cell_prefix_range = ks.index_prefix_range(&cell);
        #[cfg(debug_assertions)]
        {
            let prefix = prefix as u64;
            if !cell_prefix_range.contains(&prefix) {
                panic!(
                    "prefix do not fall in prefix range. key {key:?}, cell {cell:?}, prefix {prefix:x}, range {cell_prefix_range:x?}"
                );
            }
        }
        let cell_offset = prefix
            // cell_prefix_range.start is always u32 (but not cell_prefix_range.end)
            .checked_sub(cell_prefix_range.start as u32)
            .expect("Key prefix is out of cell prefix range");
        let cell_size = cell_prefix_range.end - cell_prefix_range.start;
        let micro_cell = rescale_u32(cell_offset, cell_size, HEADER_ELEMENTS as u32);
        micro_cell as usize
    }

    /// Reads a section of the index defined by the micro-cell
    fn read_micro_cell_section(
        &self,
        reader: &impl RandomRead,
        micro_cell: usize,
        metrics: &Metrics,
    ) -> Option<Bytes> {
        let now = Instant::now();
        let header_element =
            reader.read(micro_cell * HEADER_ELEMENT_SIZE..(micro_cell + 1) * HEADER_ELEMENT_SIZE);
        metrics
            .lookup_io_mcs
            .inc_by(now.elapsed().as_micros() as u64);
        metrics.lookup_io_bytes.inc_by(header_element.len() as u64);

        let mut header_element = &header_element[..];
        let from_offset = header_element.get_u32() as usize;
        let to_offset = header_element.get_u32() as usize;

        if from_offset == 0 && to_offset == 0 {
            return None;
        }

        let now = Instant::now();
        let buffer = reader.read(from_offset..to_offset);
        metrics
            .lookup_io_mcs
            .inc_by(now.elapsed().as_micros() as u64);
        metrics.lookup_io_bytes.inc_by(buffer.len() as u64);

        Some(buffer)
    }

    /// Binary-search `buffer` for the entry after/before `prev` in `direction`.
    ///
    /// Returns `(key, wal_position, entry_index)` on success, where `entry_index`
    /// is the buffer-relative entry index of the returned key.  The caller stores
    /// this as `last_returned_pos` in the cache so the *next* call can reach the
    /// subsequent entry in O(1) instead of O(log n).
    ///
    /// This function does not perform any I/O.
    fn search_in_micro_cell_buffer(
        buffer: &Bytes,
        prev: Option<&[u8]>,
        direction: Direction,
        ks: &KeySpaceDesc,
        metrics: &Metrics,
    ) -> Option<(Bytes, WalPosition, usize)> {
        let (key_size, element_size) = ks.index_key_element_size().unwrap();

        if buffer.is_empty() {
            return None;
        }

        let entry_count = buffer.len() / element_size;
        if entry_count == 0 {
            return None;
        }

        let start_pos = if let Some(prev_key) = prev {
            let (found_pos, insertion_point, _) =
                binary_search(buffer, prev_key, element_size, key_size, metrics);

            match direction {
                Direction::Forward => {
                    if let Some(pos) = found_pos {
                        pos + 1
                    } else {
                        insertion_point
                    }
                }
                Direction::Backward => {
                    if let Some(pos) = found_pos {
                        pos.checked_sub(1)?
                    } else {
                        insertion_point.checked_sub(1)?
                    }
                }
            }
        } else {
            direction.first_in_range(0..entry_count)
        };

        if start_pos >= entry_count {
            return None;
        }

        let entry_start = start_pos * element_size;
        let key = buffer.slice(entry_start..entry_start + key_size);
        let value_bytes = &buffer[entry_start + key_size..entry_start + element_size];
        let pos = WalPosition::from_slice(value_bytes);

        Some((key, pos, start_pos))
    }

    /// Same as `search_in_micro_cell_buffer` but for variable-length key sections.
    /// Uses the flat-buffer offset table for O(1) entry access and O(log n) binary search.
    fn search_in_varlen_micro_cell_buffer(
        buffer: &Bytes,
        prev: Option<&[u8]>,
        direction: Direction,
        metrics: &Metrics,
    ) -> Option<(Bytes, WalPosition, usize)> {
        let _timer = metrics.lookup_scan_mcs.clone().mcs_timer();
        let count = IndexTable::flat_varlen_count(buffer);
        if count == 0 {
            return None;
        }

        let start_pos = if let Some(prev_key) = prev {
            let lb = IndexTable::flat_varlen_lower_bound(buffer, prev_key);
            match direction {
                Direction::Forward => {
                    // If prev_key exists at lb, skip past it; otherwise lb is the insertion point.
                    let exact =
                        lb < count && IndexTable::flat_varlen_entry_at(buffer, lb).0 == prev_key;
                    if exact { lb + 1 } else { lb }
                }
                Direction::Backward => {
                    // Whether or not prev_key exists, entries before lb are all < prev_key.
                    lb.checked_sub(1)?
                }
            }
        } else {
            direction.first_in_range(0..count)
        };

        if start_pos >= count {
            return None;
        }

        let (key_slice, pos) = IndexTable::flat_varlen_entry_at(buffer, start_pos);
        let key = Self::varlen_key_to_bytes(buffer, key_slice);
        Some((key, pos, start_pos))
    }

    /// Convert a `key_slice` that borrows from `buffer` into a zero-copy `Bytes` sub-slice.
    fn varlen_key_to_bytes(buffer: &Bytes, key_slice: &[u8]) -> Bytes {
        let b_ptr = buffer.as_ptr() as usize;
        let key_abs = key_slice.as_ptr() as usize - b_ptr;
        buffer.slice(key_abs..key_abs + key_slice.len())
    }

    /// `next_entry_unloaded` implementation for variable-length key spaces.
    #[allow(clippy::too_many_arguments)]
    fn next_entry_unloaded_varlen(
        &self,
        reader: &impl RandomRead,
        prev: Option<&[u8]>,
        direction: Direction,
        metrics: &Metrics,
        position: WalPosition,
        cache: &mut Option<IndexIterCache>,
        start_micro_cell: usize,
    ) -> Option<(Bytes, WalPosition)> {
        let mut current_micro_cell = start_micro_cell;

        loop {
            let cache_hit: Option<(Bytes, usize)> = if let Some(IndexIterCache::LookupHeader {
                micro_cell: cached_mc,
                buffer,
                last_returned_pos,
                ..
            }) = cache.as_ref()
            {
                if *cached_mc == current_micro_cell {
                    Some((buffer.clone(), *last_returned_pos))
                } else {
                    None
                }
            } else {
                None
            };

            let result: Option<(Bytes, WalPosition, usize)> = if let Some((buffer, last_pos)) =
                cache_hit
            {
                let count = IndexTable::flat_varlen_count(&buffer);

                if let Some(prev_key) = prev {
                    let (key_at_last, _) = IndexTable::flat_varlen_entry_at(&buffer, last_pos);
                    if key_at_last == prev_key {
                        // O(1) sequential path: prev is exactly the last entry we cached.
                        let next_pos = match direction {
                            Direction::Forward => {
                                let p = last_pos + 1;
                                if p < count { Some(p) } else { None }
                            }
                            Direction::Backward => last_pos.checked_sub(1),
                        };
                        if let Some(next_pos) = next_pos {
                            let (key_slice, val) =
                                IndexTable::flat_varlen_entry_at(&buffer, next_pos);
                            let key = Self::varlen_key_to_bytes(&buffer, key_slice);
                            Some((key, val, next_pos))
                        } else {
                            *cache = None;
                            current_micro_cell = next_bounded(
                                current_micro_cell,
                                HEADER_ELEMENTS,
                                direction == Direction::Backward,
                            )?;
                            continue;
                        }
                    } else {
                        Self::search_in_varlen_micro_cell_buffer(&buffer, prev, direction, metrics)
                    }
                } else {
                    Self::search_in_varlen_micro_cell_buffer(&buffer, prev, direction, metrics)
                }
            } else {
                let Some(buffer) =
                    self.read_micro_cell_section(reader, current_micro_cell, metrics)
                else {
                    *cache = None;
                    current_micro_cell = next_bounded(
                        current_micro_cell,
                        HEADER_ELEMENTS,
                        direction == Direction::Backward,
                    )?;
                    continue;
                };

                let result =
                    Self::search_in_varlen_micro_cell_buffer(&buffer, prev, direction, metrics);
                *cache = Some(IndexIterCache::LookupHeader {
                    position,
                    micro_cell: current_micro_cell,
                    buffer,
                    last_returned_pos: 0,
                });
                result
            };

            if let Some((key, val, entry_pos)) = result {
                if let Some(IndexIterCache::LookupHeader {
                    last_returned_pos, ..
                }) = cache.as_mut()
                {
                    *last_returned_pos = entry_pos;
                }
                return Some((key, val));
            }

            *cache = None;
            current_micro_cell = next_bounded(
                current_micro_cell,
                HEADER_ELEMENTS,
                direction == Direction::Backward,
            )?;
        }
    }
}

impl IndexFormat for LookupHeaderIndex {
    fn serialize_index(&self, table: &IndexTable, ks: &KeySpaceDesc) -> Bytes {
        if ks.index_key_size().is_some() {
            // Fixed-length key path: existing streaming approach.
            let capacity = ks.index_element_size_for_capacity() * table.len() + HEADER_SIZE;
            let mut out = BytesMut::with_capacity(capacity);
            out.put_bytes(0, HEADER_SIZE);
            let mut header = IndexTableHeaderBuilder::new(ks);
            table.serialize_index_entries_with_visitor(ks, &mut out, &mut header);
            header.write_header(out.len(), &mut out[..HEADER_SIZE]);
            return Bytes::from(out.freeze());
        }

        // Variable-length key path: group entries by micro-cell, then write each
        // section directly into `out` (same flat-buffer format as the in-memory
        // buffer, so binary-search primitives work without re-parsing).
        //
        // Use a BTreeMap so only non-empty micro-cells allocate, and iteration
        // order matches the 0..HEADER_ELEMENTS micro-cell sequence.
        //
        // `iter_with_tombstones` yields tombstones as `INVALID`, which
        // `append_flat_varlen_section` persists verbatim. Callers that want
        // tombstones stripped should call `IndexTable::clean_self` first.
        let mut mc_entries: BTreeMap<usize, Vec<(Bytes, WalPosition)>> = BTreeMap::new();
        for (key, pos) in table.iter_with_tombstones() {
            mc_entries
                .entry(Self::key_micro_cell(ks, &key))
                .or_default()
                .push((key, pos));
        }

        let capacity = ks.index_element_size_for_capacity() * table.len() + HEADER_SIZE;
        let mut out = BytesMut::with_capacity(capacity);
        out.put_bytes(0, HEADER_SIZE);
        let mut header: Vec<(u32, u32)> = vec![(0, 0); HEADER_ELEMENTS];

        for (mc, entries) in mc_entries {
            let from = out.len() as u32;
            IndexTable::append_flat_varlen_section(entries, &mut out);
            header[mc] = (from, out.len() as u32);
        }

        for (i, (from, to)) in header.iter().enumerate() {
            let start = i * HEADER_ELEMENT_SIZE;
            out[start..start + 4].copy_from_slice(&from.to_be_bytes());
            out[start + 4..start + 8].copy_from_slice(&to.to_be_bytes());
        }

        Bytes::from(out.freeze())
    }

    fn deserialize_index(&self, ks: &KeySpaceDesc, b: Bytes) -> IndexTable {
        if ks.index_key_size().is_some() {
            return IndexTable::deserialize_index_entries(ks, b.slice(HEADER_SIZE..));
        }
        deserialize_varlen_index(b)
    }

    fn lookup_unloaded(
        &self,
        ks: &KeySpaceDesc,
        reader: &impl RandomRead,
        key: &[u8],
        metrics: &Metrics,
    ) -> Option<WalPosition> {
        if let Some(key_size) = ks.index_key_size() {
            assert_eq!(key.len(), key_size);
        }
        let micro_cell = Self::key_micro_cell(ks, key);

        let buffer = self.read_micro_cell_section(reader, micro_cell, metrics)?;

        if let Some((key_size, element_size)) = ks.index_key_element_size() {
            let (_, _, pos) = binary_search(&buffer, key, element_size, key_size, metrics);
            pos
        } else {
            binary_search_varlen(&buffer, key, metrics)
        }
    }

    fn next_entry_unloaded(
        &self,
        ks: &KeySpaceDesc,
        reader: &impl RandomRead,
        prev: Option<&[u8]>,
        direction: Direction,
        metrics: &Metrics,
        position: WalPosition,
        cache: &mut Option<IndexIterCache>,
    ) -> Option<(Bytes, WalPosition)> {
        // Invalidate a stale cache entry (flush detected between calls).
        if let Some(c) = cache.as_ref()
            && c.position() != position
        {
            *cache = None;
        }

        // Determine which micro-cell to start searching from.
        let start_micro_cell = if let Some(prev_key) = prev {
            if let Some(key_size) = ks.index_key_size() {
                assert_eq!(prev_key.len(), key_size);
            }
            Self::key_micro_cell(ks, prev_key)
        } else {
            direction.first_in_range(0..HEADER_ELEMENTS)
        };

        if ks.index_key_element_size().is_none() {
            return self.next_entry_unloaded_varlen(
                reader,
                prev,
                direction,
                metrics,
                position,
                cache,
                start_micro_cell,
            );
        }
        let (key_size, element_size) = ks.index_key_element_size().unwrap();
        let mut current_micro_cell = start_micro_cell;

        loop {
            // --- Cache check: do we have the buffer for this micro-cell? ---
            let cache_hit: Option<(Bytes, usize)> = if let Some(IndexIterCache::LookupHeader {
                micro_cell: cached_mc,
                buffer,
                last_returned_pos,
                ..
            }) = cache.as_ref()
            {
                if *cached_mc == current_micro_cell {
                    Some((buffer.clone(), *last_returned_pos))
                } else {
                    None
                }
            } else {
                None
            };

            // Returns (key, wal_position, entry_index) or None if exhausted.
            let result: Option<(Bytes, WalPosition, usize)> =
                if let Some((buffer, last_pos)) = cache_hit {
                    let entry_count = buffer.len() / element_size;

                    // O(1) sequential-access path: only when prev_key matches
                    // the last entry we returned (the common iterator case).
                    if let Some(prev_key) = prev {
                        let key_at_last =
                            &buffer[last_pos * element_size..last_pos * element_size + key_size];

                        if key_at_last == prev_key {
                            // Direct index arithmetic — no binary search needed.
                            let next_pos = match direction {
                                Direction::Forward => {
                                    let p = last_pos + 1;
                                    if p < entry_count { Some(p) } else { None }
                                }
                                Direction::Backward => last_pos.checked_sub(1),
                            };

                            if let Some(next_pos) = next_pos {
                                let entry_start = next_pos * element_size;
                                let key = buffer.slice(entry_start..entry_start + key_size);
                                let val = WalPosition::from_slice(
                                    &buffer[entry_start + key_size..entry_start + element_size],
                                );
                                Some((key, val, next_pos))
                            } else {
                                // Micro-cell exhausted — detected without binary search.
                                *cache = None;
                                current_micro_cell = next_bounded(
                                    current_micro_cell,
                                    HEADER_ELEMENTS,
                                    direction == Direction::Backward,
                                )?;
                                continue;
                            }
                        } else {
                            // Non-sequential access (e.g. first call after
                            // cache warm-up mid-cell): binary search fallback.
                            Self::search_in_micro_cell_buffer(&buffer, prev, direction, ks, metrics)
                        }
                    } else {
                        // No prev key (start of iteration): binary search for
                        // the first/last entry in the cached buffer.
                        Self::search_in_micro_cell_buffer(&buffer, prev, direction, ks, metrics)
                    }
                } else {
                    // Cache miss: read from disk and populate the cache.
                    let Some(buffer) =
                        self.read_micro_cell_section(reader, current_micro_cell, metrics)
                    else {
                        *cache = None;
                        current_micro_cell = next_bounded(
                            current_micro_cell,
                            HEADER_ELEMENTS,
                            direction == Direction::Backward,
                        )?;
                        continue;
                    };

                    let result =
                        Self::search_in_micro_cell_buffer(&buffer, prev, direction, ks, metrics);

                    // Populate cache with the freshly-read buffer.
                    // `last_returned_pos` is updated below when we find an entry.
                    *cache = Some(IndexIterCache::LookupHeader {
                        position,
                        micro_cell: current_micro_cell,
                        buffer,
                        last_returned_pos: 0,
                    });

                    result
                };

            if let Some((key, val, entry_pos)) = result {
                // Update the cached position so the next call uses O(1) lookup.
                if let Some(IndexIterCache::LookupHeader {
                    last_returned_pos, ..
                }) = cache.as_mut()
                {
                    *last_returned_pos = entry_pos;
                }
                return Some((key, val));
            }

            // Micro-cell exhausted via binary search: advance.
            *cache = None;
            current_micro_cell = next_bounded(
                current_micro_cell,
                HEADER_ELEMENTS,
                direction == Direction::Backward,
            )?;
        }
    }
}

/// Binary-search a variable-length key micro-cell section for `key`.
/// Delegates to the shared flat-buffer primitive so the logic is not duplicated.
fn binary_search_varlen(buffer: &[u8], key: &[u8], metrics: &Metrics) -> Option<WalPosition> {
    let _timer = metrics.lookup_scan_mcs.clone().mcs_timer();
    IndexTable::flat_varlen_lookup(buffer, key)
}

/// Deserialize a variable-length key index blob written by `LookupHeaderIndex::serialize_index`.
///
/// Each on-disk micro-cell section already has the exact in-memory varlen
/// flat-buffer layout (`[count][offsets][entries]`, same writer both sides).
/// We splice the sections into one unified flat buffer: raw-copy every
/// section's entries bytes, rewrite its offset table with a cumulative
/// offset, and wrap them in a single `[total_count][offsets * total_count]`
/// header. No per-entry decode, no (key, pos) intermediate Vec, no sort.
///
/// **Sortedness.** Sections are written in micro-cell-index order, and
/// `LookupHeaderIndex::key_micro_cell` is a monotonic function of the first
/// 4 bytes of the key's index prefix. Keys in micro-cell M have strictly
/// smaller first-4-byte prefix than keys in micro-cell M+1 (rescale is a
/// deterministic total function — two keys with the same prefix_u32 always
/// land in the same micro-cell), so cross-section concatenation is globally
/// sorted. Within a section, entries are already sorted by the writer.
///
/// **Tombstones.** The raw-copy path preserves on-disk tombstones verbatim:
/// `[key][offset=u64::MAX][encoded_len=0x8000_0000]` round-trips as
/// `IndexEntryKind::Removed`, matching the serializer's intent. The old
/// decode-and-rebuild path routed tombstones through
/// `WalPosition::into_update_position` which dropped the kind bits and
/// reconstructed them as `Clean` — a latent bug that would have surfaced
/// once two-level LSM started persisting tombstones in varlen L0 indices.
fn deserialize_varlen_index(b: Bytes) -> IndexTable {
    // First pass: gather non-empty sections and totals.
    let mut sections: Vec<(usize, usize, usize)> = Vec::new(); // (from, to, count)
    let mut total_count: usize = 0;
    let mut total_entry_bytes: usize = 0;

    for mc in 0..HEADER_ELEMENTS {
        let hs = mc * HEADER_ELEMENT_SIZE;
        let from = u32::from_be_bytes(b[hs..hs + 4].try_into().unwrap()) as usize;
        let to = u32::from_be_bytes(b[hs + 4..hs + 8].try_into().unwrap()) as usize;
        // from=0 is an unambiguous empty sentinel: all data starts at offsets
        // >= HEADER_SIZE, so a micro-cell with from=0 was never written.
        if from == 0 && to == 0 {
            continue;
        }

        let section = &b[from..to];
        let count = u32::from_be_bytes(section[0..4].try_into().unwrap()) as usize;
        if count == 0 {
            continue;
        }
        let entries_start = 4 + 4 * count;
        let entries_bytes = (to - from) - entries_start;

        sections.push((from, to, count));
        total_count += count;
        total_entry_bytes += entries_bytes;
    }

    if total_count == 0 {
        return IndexTable::from_varlen_flat_bytes(Bytes::default());
    }

    // Output: [total_count: u32][offsets: u32 * total_count][entries_concat].
    let header_size = 4 + 4 * total_count;
    let mut out: Vec<u8> = Vec::with_capacity(header_size + total_entry_bytes);
    out.extend_from_slice(&(total_count as u32).to_be_bytes());

    // Write offsets with cumulative byte adjustment. Section-local offsets are
    // relative to each section's entries start; unified offsets are relative
    // to the unified entries start (immediately after the offset table).
    let mut cum_entry_bytes: u32 = 0;
    for &(from, to, count) in &sections {
        let section = &b[from..to];
        for i in 0..count {
            let pos = 4 + 4 * i;
            let off_local = u32::from_be_bytes(section[pos..pos + 4].try_into().unwrap());
            let off_global = off_local + cum_entry_bytes;
            out.extend_from_slice(&off_global.to_be_bytes());
        }
        let entries_start = 4 + 4 * count;
        let entries_bytes = (to - from) - entries_start;
        cum_entry_bytes += entries_bytes as u32;
    }

    // Raw-copy each section's entries bytes.
    for &(from, to, count) in &sections {
        let entries_start = 4 + 4 * count;
        out.extend_from_slice(&b[from + entries_start..to]);
    }

    IndexTable::from_varlen_flat_bytes(Bytes::from(out))
}

pub struct IndexTableHeaderBuilder<'a> {
    ks: &'a KeySpaceDesc,
    header: Vec<(u32, u32)>,
    last_micro_cell: Option<usize>,
}

impl IndexSerializationVisitor for IndexTableHeaderBuilder<'_> {
    fn add_key(&mut self, key: &[u8], offset: usize) {
        self.add_key(key, offset)
    }
}

impl<'a> IndexTableHeaderBuilder<'a> {
    pub fn new(ks: &'a KeySpaceDesc) -> Self {
        let header = (0..HEADER_ELEMENTS).map(|_| (0, 0)).collect();
        Self {
            ks,
            header,
            last_micro_cell: None,
        }
    }

    pub fn add_key(&mut self, key: &[u8], offset: usize) {
        let offset = Self::check_offset(offset);
        let micro_cell = LookupHeaderIndex::key_micro_cell(self.ks, key);
        if let Some(last_micro_cell) = self.last_micro_cell {
            if last_micro_cell == micro_cell {
                return;
            }
            self.header[last_micro_cell].1 = offset;
        }
        self.last_micro_cell = Some(micro_cell);
        self.header[micro_cell].0 = offset;
    }

    pub fn write_header(self, end_offset: usize, mut buf: &mut [u8]) {
        let mut header = self.header;
        let end_offset = Self::check_offset(end_offset);
        if let Some(last_micro_cell) = self.last_micro_cell {
            header[last_micro_cell].1 = end_offset;
        }
        for (start, end) in header {
            buf.put_u32(start);
            buf.put_u32(end);
        }
    }

    fn check_offset(offset: usize) -> u32 {
        assert!(offset < u32::MAX as usize, "Index table is too large");
        offset as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::index_format::test::*;
    use crate::key_shape::{KeyIndexing, KeyShape, KeySpace, KeyType};
    use minibytes::Bytes;

    #[test]
    fn test_varlen_index_lookup_and_roundtrip() {
        let metrics = Metrics::new();
        let shape = KeyShape::new_single_config_indexing(
            KeyIndexing::variable_length(),
            1,
            KeyType::uniform(1),
            Default::default(),
        );
        let ks_id = KeySpace::first();
        let ks = shape.ks(ks_id);

        let mut index = IndexTable::default();
        // Empty key
        index.insert(Bytes::from(vec![]), WalPosition::test_value(1));
        // Single-byte keys spread across the key space (ensures multiple micro-cells)
        for b in 0u8..=255 {
            index.insert(Bytes::from(vec![b]), WalPosition::test_value(b as u64 + 10));
        }
        // Multi-byte keys
        index.insert(
            Bytes::from(vec![0, 1, 2, 3, 4]),
            WalPosition::test_value(300),
        );
        index.insert(
            Bytes::from(vec![255, 255, 255]),
            WalPosition::test_value(400),
        );

        let serialized = LookupHeaderIndex.clean_serialize_index(&mut index, ks);

        // Lookup present keys
        assert_eq!(
            Some(WalPosition::test_value(1)),
            LookupHeaderIndex.lookup_unloaded(ks, &serialized, &[], &metrics),
        );
        for b in 0u8..=255 {
            assert_eq!(
                Some(WalPosition::test_value(b as u64 + 10)),
                LookupHeaderIndex.lookup_unloaded(ks, &serialized, &[b], &metrics),
                "lookup failed for key [{b}]",
            );
        }
        assert_eq!(
            Some(WalPosition::test_value(300)),
            LookupHeaderIndex.lookup_unloaded(ks, &serialized, &[0, 1, 2, 3, 4], &metrics),
        );
        assert_eq!(
            Some(WalPosition::test_value(400)),
            LookupHeaderIndex.lookup_unloaded(ks, &serialized, &[255, 255, 255], &metrics),
        );

        // Lookup absent keys
        assert_eq!(
            None,
            LookupHeaderIndex.lookup_unloaded(ks, &serialized, &[0, 99], &metrics),
        );
        assert_eq!(
            None,
            LookupHeaderIndex.lookup_unloaded(ks, &serialized, &[1, 2, 3], &metrics),
        );

        // Deserialize and verify round-trip
        let deserialized = LookupHeaderIndex.deserialize_index(ks, serialized);
        assert_eq!(Some(WalPosition::test_value(1)), deserialized.get(&[]));
        for b in 0u8..=255 {
            assert_eq!(
                Some(WalPosition::test_value(b as u64 + 10)),
                deserialized.get(&[b]),
                "deserialized lookup failed for key [{b}]",
            );
        }
        assert_eq!(
            Some(WalPosition::test_value(300)),
            deserialized.get(&[0, 1, 2, 3, 4]),
        );
        assert_eq!(
            Some(WalPosition::test_value(400)),
            deserialized.get(&[255, 255, 255]),
        );
        assert_eq!(None, deserialized.get(&[0, 99]));
    }

    /// Regression test: tombstones in varlen serialized blobs must survive
    /// deserialize/serialize round-trips. The two-level LSM relies on L0
    /// preserving `Removed` entries so they can shadow keys still present in
    /// L1. An earlier implementation decoded flat entries via
    /// `into_update_position`, which produced `{u64::MAX, 0}` for a tombstone
    /// — not equal to `WalPosition::INVALID`'s `{u64::MAX, u32::MAX}`
    /// sentinel — and `from_disk` then re-encoded them as Clean entries
    /// pointing at offset `u64::MAX`, silently losing the tombstone.
    #[test]
    fn test_varlen_tombstone_roundtrip() {
        let shape = KeyShape::new_single_config_indexing(
            KeyIndexing::variable_length(),
            1,
            KeyType::uniform(1),
            Default::default(),
        );
        let ks_id = KeySpace::first();
        let ks = shape.ks(ks_id);
        let index_format = LookupHeaderIndex;

        // Populate across several micro-cells so the streaming deserialize
        // exercises multi-section splice.
        let mut table = IndexTable::default();
        for b in 0u8..=31 {
            table.insert(
                Bytes::from(vec![b]),
                WalPosition::test_value(b as u64 + 100),
            );
        }
        // Remove half of them — these produce Removed-kind entries in the
        // table and serialize as on-disk tombstones.
        for b in (0u8..=31).step_by(2) {
            table.remove(
                Bytes::from(vec![b]),
                WalPosition::test_value(9000 + b as u64),
            );
        }

        let serialized = index_format.serialize_index(&table, ks);
        let deserialized = index_format.deserialize_index(ks, serialized);

        for b in 0u8..=31 {
            let got = deserialized.get(&[b]);
            if b % 2 == 0 {
                assert_eq!(
                    got,
                    Some(WalPosition::INVALID),
                    "key [{b}] should be a tombstone after round-trip",
                );
            } else {
                assert_eq!(
                    got,
                    Some(WalPosition::test_value(b as u64 + 100)),
                    "key [{b}] should survive as a live entry",
                );
            }
        }
    }

    #[test]
    pub fn test_index_lookup() {
        let index = LookupHeaderIndex;
        test_index_lookup_inner(&index);
    }

    #[test]
    pub fn test_index_lookup_random() {
        let index = LookupHeaderIndex;
        test_index_lookup_random_inner(&index);
    }

    /// Tests for the next_entry_unloaded function
    #[test]
    fn test_next_entry_unloaded() {
        let index_format = LookupHeaderIndex;
        crate::index::index_format::test::test_next_entry_unloaded_inner(&index_format);
    }

    #[test]
    fn test_next_entry_unloaded_empty_index() {
        let index_format = LookupHeaderIndex;
        crate::index::index_format::test::test_next_entry_unloaded_empty_index_inner(&index_format);
    }

    #[test]
    fn test_next_entry_micro_cell_boundary() {
        let metrics = Metrics::new();
        // Create a key shape with a larger key space to ensure multiple micro cells
        let shape = KeyShape::new_single(8, 1, KeyType::uniform(1));
        let ks_id = KeySpace::first();
        let ks = shape.ks(ks_id);

        // Create an index with entries that span multiple micro cells
        let mut table = IndexTable::default();

        // Adding keys that should span across micro cells
        // The exact distribution depends on key_micro_cell function behavior
        for i in 0..20u64 {
            // Create keys with increasing values spread across the key space
            let key_value = i * (u64::MAX / 20);
            let key = key_value.to_be_bytes().to_vec();
            table.insert(Bytes::from(key), WalPosition::test_value(i));
        }

        // Convert the table to bytes
        let index_format = LookupHeaderIndex;
        let serialized = index_format.clean_serialize_index(&mut table, ks);

        // Use the MockRandomRead from the test module in index_format.rs
        let reader = MockRandomRead::new(serialized);
        let dummy_pos = WalPosition::test_value(0);

        // Test forward iteration across all entries to verify micro cell boundaries,
        // reusing the cache across calls as a real iterator would.
        let mut current_key: Option<Bytes> = None;
        let mut count = 0;
        let mut cache = None;

        loop {
            let result = index_format.next_entry_unloaded(
                ks,
                &reader,
                current_key.as_deref(),
                Direction::Forward,
                &metrics,
                dummy_pos,
                &mut cache,
            );

            if result.is_none() {
                break;
            }

            let (key, _) = result.unwrap();
            current_key = Some(key);
            count += 1;
        }

        // Verify we can iterate through all entries
        assert_eq!(count, 20, "Should iterate through all 20 entries");

        // Test backward iteration with a fresh cache
        let mut current_key: Option<Bytes> = None;
        let mut count = 0;
        let mut cache = None;

        loop {
            let result = index_format.next_entry_unloaded(
                ks,
                &reader,
                current_key.as_deref(),
                Direction::Backward,
                &metrics,
                dummy_pos,
                &mut cache,
            );

            if result.is_none() {
                break;
            }

            let (key, _) = result.unwrap();
            current_key = Some(key);
            count += 1;
        }

        // Verify we can iterate through all entries backwards
        assert_eq!(count, 20, "Should iterate through all 20 entries backward");
    }

    #[test]
    fn test_next_entry_cache_reduces_io() {
        let metrics = Metrics::new();
        // Use a small key space so all keys land in few micro-cells.
        let shape = KeyShape::new_single(8, 1, KeyType::uniform(1));
        let ks_id = KeySpace::first();
        let ks = shape.ks(ks_id);

        let mut table = IndexTable::default();
        // Pack many consecutive keys so they share micro-cells.
        for i in 0u64..50 {
            let key = (i * 100_000_000_000_000u64).to_be_bytes().to_vec();
            table.insert(Bytes::from(key), WalPosition::test_value(i));
        }

        let index_format = LookupHeaderIndex;
        let serialized = index_format.clean_serialize_index(&mut table, ks);
        let reader = MockRandomRead::new(serialized);
        let dummy_pos = WalPosition::test_value(42);

        // Iterate with cache enabled, counting reads.
        let mut current_key: Option<Bytes> = None;
        let mut count = 0;
        let mut cache = None;
        reader.reset_call_count();

        loop {
            let result = index_format.next_entry_unloaded(
                ks,
                &reader,
                current_key.as_deref(),
                Direction::Forward,
                &metrics,
                dummy_pos,
                &mut cache,
            );
            if result.is_none() {
                break;
            }
            let (key, _) = result.unwrap();
            current_key = Some(key);
            count += 1;
        }
        assert_eq!(count, 50);
        let cached_reads = reader.call_count();

        // Iterate again without cache (fresh None each call).
        let mut current_key: Option<Bytes> = None;
        reader.reset_call_count();

        loop {
            let result = index_format.next_entry_unloaded(
                ks,
                &reader,
                current_key.as_deref(),
                Direction::Forward,
                &metrics,
                dummy_pos,
                &mut None,
            );
            if result.is_none() {
                break;
            }
            let (key, _) = result.unwrap();
            current_key = Some(key);
        }
        let uncached_reads = reader.call_count();

        // With caching we should issue strictly fewer I/O calls.
        assert!(
            cached_reads < uncached_reads,
            "cache={cached_reads} should be less than no-cache={uncached_reads}"
        );
    }

    /// Mirrors `test_next_entry_micro_cell_boundary` for variable-length key spaces.
    /// Uses 256 single-byte keys to guarantee multiple micro-cells are populated,
    /// then verifies forward and backward iteration visits every key in order.
    #[test]
    fn test_varlen_next_entry_unloaded() {
        let metrics = Metrics::new();
        let shape = KeyShape::new_single_config_indexing(
            KeyIndexing::variable_length(),
            1,
            KeyType::uniform(1),
            Default::default(),
        );
        let ks_id = KeySpace::first();
        let ks = shape.ks(ks_id);

        // Build a sorted key list to compare against iterator output.
        let mut keys: Vec<Vec<u8>> = (0u8..=255).map(|b| vec![b]).collect();
        keys.push(vec![]); // empty key sorts first
        keys.sort();

        let mut table = IndexTable::default();
        for (i, key) in keys.iter().enumerate() {
            table.insert(Bytes::from(key.clone()), WalPosition::test_value(i as u64));
        }

        let index_format = LookupHeaderIndex;
        let serialized = index_format.clean_serialize_index(&mut table, ks);
        let reader = MockRandomRead::new(serialized);
        let dummy_pos = WalPosition::test_value(0);

        // Forward iteration
        let mut current_key: Option<Bytes> = None;
        let mut forward: Vec<Bytes> = Vec::new();
        let mut cache = None;
        loop {
            let result = index_format.next_entry_unloaded(
                ks,
                &reader,
                current_key.as_deref(),
                Direction::Forward,
                &metrics,
                dummy_pos,
                &mut cache,
            );
            let Some((key, _)) = result else { break };
            current_key = Some(key.clone());
            forward.push(key);
        }
        assert_eq!(forward.len(), keys.len(), "forward: wrong count");
        for (i, k) in forward.iter().enumerate() {
            assert_eq!(k.as_ref(), keys[i].as_slice(), "forward: wrong key at {i}");
        }

        // Backward iteration
        let mut current_key: Option<Bytes> = None;
        let mut backward: Vec<Bytes> = Vec::new();
        let mut cache = None;
        loop {
            let result = index_format.next_entry_unloaded(
                ks,
                &reader,
                current_key.as_deref(),
                Direction::Backward,
                &metrics,
                dummy_pos,
                &mut cache,
            );
            let Some((key, _)) = result else { break };
            current_key = Some(key.clone());
            backward.push(key);
        }
        assert_eq!(backward.len(), keys.len(), "backward: wrong count");
        for (i, k) in backward.iter().enumerate() {
            let expected = &keys[keys.len() - 1 - i];
            assert_eq!(
                k.as_ref(),
                expected.as_slice(),
                "backward: wrong key at position {i}"
            );
        }
    }
}
