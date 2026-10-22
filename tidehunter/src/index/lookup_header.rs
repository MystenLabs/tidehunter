use std::time::Instant;

use bytes::{Buf, BufMut, BytesMut};
use minibytes::Bytes;

use crate::index::index_ref::IndexRef;
use crate::index::index_table::{IndexSerializationVisitor, IndexTable};
use crate::index::micro_cells::{self, HEADER_ELEMENT_SIZE};
use crate::math::next_bounded;
use crate::metrics::{Metrics, TimerExt};
use crate::wal::position::WalPosition;
use crate::{key_shape::KeySpaceDesc, lookup::RandomRead};

use super::index_format::{Direction, IndexFormat, IndexIterCache, SerializedIndex, binary_search};

/// Index blob format `[header][sections]`: the header holds one
/// `(from: u32, to: u32)` element per micro-cell, and a point lookup reads
/// the key's element, then binary-searches its section. The writer picks the
/// micro-cell count per blob from the key space's `MicroCellPolicy` and
/// returns it in [`SerializedIndex`]; readers get it back from the blob's
/// `IndexRef`.
#[derive(Clone)]
pub struct LookupHeaderIndex;

impl LookupHeaderIndex {
    /// The key's u32 index prefix relative to its cell: the offset from the
    /// start of the cell's prefix range, and the range's width.
    fn key_cell_offset(ks: &KeySpaceDesc, key: &[u8]) -> (u32, u64) {
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
        let cell_width = cell_prefix_range.end - cell_prefix_range.start;
        (cell_offset, cell_width)
    }

    /// Micro-cell of `key` in a blob with `micro_cells` micro-cells.
    fn key_micro_cell(ks: &KeySpaceDesc, key: &[u8], micro_cells: u32) -> usize {
        let (cell_offset, cell_width) = Self::key_cell_offset(ks, key);
        micro_cells::micro_cell(cell_offset, cell_width, micro_cells)
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

    /// Serializes a fixed-size-key table.
    fn serialize_fixed(
        table: &IndexTable,
        ks: &KeySpaceDesc,
        element_size: usize,
    ) -> SerializedIndex {
        // `IndexTable::len` may overcount a key present on both the flat and
        // the btree side; the count only sizes the header, so that is fine.
        let data_len = table.len() * element_size;
        let micro_cells = ks.micro_cell_policy().micro_cells(data_len);
        let header_len = micro_cells::header_len(micro_cells);
        let mut out = BytesMut::with_capacity(header_len + data_len);
        out.put_bytes(0, header_len);
        let mut header = IndexTableHeaderBuilder::new(ks, micro_cells);
        table.serialize_index_entries_with_visitor(ks, &mut out, &mut header);
        header.write_header(out.len(), &mut out[..header_len]);
        SerializedIndex {
            bytes: Bytes::from(out.freeze()),
            micro_cells,
        }
    }

    /// Serializes a variable-length-key table: one flat varlen section per
    /// non-empty micro-cell, in micro-cell order (see `append_flat_varlen`).
    fn serialize_varlen(table: &IndexTable, ks: &KeySpaceDesc) -> SerializedIndex {
        let mut entries: Vec<(Bytes, WalPosition)> = Vec::with_capacity(table.len());
        let mut entry_bytes = 0usize;
        for (key, pos) in table.iter_with_tombstones() {
            entry_bytes += 2 + key.len() + WalPosition::LENGTH;
            entries.push((key, pos));
        }
        // Each entry also costs a 4-byte offset in its section's table; the
        // 4-byte count per non-empty section is left out of the estimate.
        let data_len = entry_bytes + 4 * entries.len();
        let micro_cells = ks.micro_cell_policy().micro_cells(data_len);
        let header_len = micro_cells::header_len(micro_cells);

        // Entries come in key order and the micro-cell is monotone in the
        // key's index prefix, so each micro-cell's entries are one run.
        let mcs: Vec<usize> = entries
            .iter()
            .map(|(key, _)| Self::key_micro_cell(ks, key, micro_cells))
            .collect();
        let mut out = BytesMut::with_capacity(header_len + data_len);
        out.put_bytes(0, header_len);
        let mut header: Vec<(u32, u32)> = vec![(0, 0); micro_cells as usize];
        let mut start = 0;
        while start < entries.len() {
            let mc = mcs[start];
            let mut end = start + 1;
            while end < entries.len() && mcs[end] == mc {
                end += 1;
            }
            assert_eq!(
                header[mc],
                (0, 0),
                "micro-cells must be non-decreasing in key order"
            );
            let from = out.len() as u32;
            IndexTable::append_flat_varlen_section(&entries[start..end], &mut out);
            header[mc] = (from, out.len() as u32);
            start = end;
        }
        for (i, (from, to)) in header.iter().enumerate() {
            let start = i * HEADER_ELEMENT_SIZE;
            out[start..start + 4].copy_from_slice(&from.to_be_bytes());
            out[start + 4..start + 8].copy_from_slice(&to.to_be_bytes());
        }
        SerializedIndex {
            bytes: Bytes::from(out.freeze()),
            micro_cells,
        }
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
        micro_cells: usize,
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
                                micro_cells,
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
                        micro_cells,
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
                micro_cells,
                direction == Direction::Backward,
            )?;
        }
    }
}

impl IndexFormat for LookupHeaderIndex {
    fn serialize_index(&self, table: &IndexTable, ks: &KeySpaceDesc) -> SerializedIndex {
        if let Some((_, element_size)) = ks.index_key_element_size() {
            Self::serialize_fixed(table, ks, element_size)
        } else {
            Self::serialize_varlen(table, ks)
        }
    }

    fn deserialize_index(&self, ks: &KeySpaceDesc, b: Bytes, index_ref: IndexRef) -> IndexTable {
        let micro_cells = index_ref.micro_cells();
        assert!(
            micro_cells >= 1,
            "a lookup-header blob has at least one micro-cell"
        );
        if ks.index_key_size().is_some() {
            let header_len = micro_cells::header_len(micro_cells);
            return IndexTable::deserialize_index_entries(ks, b.slice(header_len..));
        }
        deserialize_varlen_index(b, micro_cells)
    }

    fn lookup_unloaded(
        &self,
        ks: &KeySpaceDesc,
        reader: &impl RandomRead,
        index_ref: IndexRef,
        key: &[u8],
        metrics: &Metrics,
    ) -> Option<WalPosition> {
        let micro_cells = index_ref.micro_cells();
        if let Some(key_size) = ks.index_key_size() {
            assert_eq!(key.len(), key_size);
        }
        assert!(
            micro_cells >= 1,
            "a lookup-header blob has at least one micro-cell"
        );
        let micro_cell = Self::key_micro_cell(ks, key, micro_cells);

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
        index_ref: IndexRef,
        prev: Option<&[u8]>,
        direction: Direction,
        metrics: &Metrics,
        cache: &mut Option<IndexIterCache>,
    ) -> Option<(Bytes, WalPosition)> {
        let position = index_ref.position();
        let micro_cells = index_ref.micro_cells();
        // Invalidate a stale cache entry (flush detected between calls).
        if let Some(c) = cache.as_ref()
            && c.position() != position
        {
            *cache = None;
        }

        assert!(
            micro_cells >= 1,
            "a lookup-header blob has at least one micro-cell"
        );
        let micro_cells_count = micro_cells as usize;

        // Determine which micro-cell to start searching from.
        let start_micro_cell = if let Some(prev_key) = prev {
            if let Some(key_size) = ks.index_key_size() {
                assert_eq!(prev_key.len(), key_size);
            }
            Self::key_micro_cell(ks, prev_key, micro_cells)
        } else {
            direction.first_in_range(0..micro_cells_count)
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
                micro_cells_count,
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
                                    micro_cells_count,
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
                            micro_cells_count,
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
                micro_cells_count,
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

/// Deserialize a variable-length key index blob written by
/// `LookupHeaderIndex::serialize_index` with `micro_cells` micro-cells.
///
/// Each on-disk micro-cell section already has the exact in-memory varlen
/// flat-buffer layout (`[count][offsets][entries]`, same writer both sides).
/// We splice the sections into one unified flat buffer: raw-copy every
/// section's entries bytes, rewrite its offset table with a cumulative
/// offset, and wrap them in a single `[total_count][offsets * total_count]`
/// header. No per-entry decode, no (key, pos) intermediate Vec, no sort.
///
/// **Sortedness.** Sections are written in micro-cell-index order, and the
/// micro-cell is a monotonic function of the first 4 bytes of the key's
/// index prefix. Keys in micro-cell M have strictly smaller first-4-byte
/// prefix than keys in micro-cell M+1 (rescale is a deterministic total
/// function — two keys with the same prefix_u32 always land in the same
/// micro-cell), so cross-section concatenation is globally sorted. Within a
/// section, entries are already sorted by the writer.
///
/// **Tombstones.** The raw-copy path preserves on-disk tombstones verbatim:
/// `[key][offset=u64::MAX][encoded_len=0x8000_0000]` round-trips as
/// `IndexEntryKind::Removed`, matching the serializer's intent.
fn deserialize_varlen_index(b: Bytes, micro_cells: u32) -> IndexTable {
    // First pass: gather non-empty sections and totals.
    let mut sections: Vec<(usize, usize, usize)> = Vec::new(); // (from, to, count)
    let mut total_count: usize = 0;
    let mut total_entry_bytes: usize = 0;

    for mc in 0..micro_cells as usize {
        let hs = mc * HEADER_ELEMENT_SIZE;
        let from = u32::from_be_bytes(b[hs..hs + 4].try_into().unwrap()) as usize;
        let to = u32::from_be_bytes(b[hs + 4..hs + 8].try_into().unwrap()) as usize;
        // from=0 is an unambiguous empty sentinel: all data starts at offsets
        // >= the header length, so a micro-cell with from=0 was never written.
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

/// Streaming header builder for fixed-size-key blobs: records the section
/// range of each micro-cell as the entries are appended in key order.
pub struct IndexTableHeaderBuilder<'a> {
    ks: &'a KeySpaceDesc,
    micro_cells: u32,
    header: Vec<(u32, u32)>,
    last_micro_cell: Option<usize>,
}

impl IndexSerializationVisitor for IndexTableHeaderBuilder<'_> {
    fn add_key(&mut self, key: &[u8], offset: usize) {
        self.add_key(key, offset)
    }
}

impl<'a> IndexTableHeaderBuilder<'a> {
    pub fn new(ks: &'a KeySpaceDesc, micro_cells: u32) -> Self {
        Self {
            ks,
            micro_cells,
            header: vec![(0, 0); micro_cells as usize],
            last_micro_cell: None,
        }
    }

    pub fn add_key(&mut self, key: &[u8], offset: usize) {
        let offset = Self::check_offset(offset);
        let micro_cell = LookupHeaderIndex::key_micro_cell(self.ks, key, self.micro_cells);
        if let Some(last_micro_cell) = self.last_micro_cell {
            if last_micro_cell == micro_cell {
                return;
            }
            assert!(
                last_micro_cell < micro_cell,
                "micro-cells must be non-decreasing in key order"
            );
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
    use crate::index::micro_cells::{MicroCellPolicy, header_len};
    use crate::key_shape::{KeyIndexing, KeyShape, KeySpace, KeySpaceConfig, KeyType};
    use minibytes::Bytes;

    /// Policies every format test runs under: the legacy fixed count, the
    /// default, and a tiny section target that splits even small tables
    /// into many micro-cells.
    const POLICIES: [MicroCellPolicy; 3] = [
        MicroCellPolicy::LEGACY,
        MicroCellPolicy::DEFAULT,
        MicroCellPolicy::Scaled {
            section_target_log2: 5,
        },
    ];

    /// One uniform cell of variable-length keys under `policy`.
    fn varlen_shape(policy: MicroCellPolicy) -> KeyShape {
        KeyShape::new_single_config_indexing(
            KeyIndexing::variable_length(),
            1,
            KeyType::uniform(1),
            KeySpaceConfig::new().with_micro_cell_policy(policy),
        )
    }

    /// One uniform cell of 8-byte keys under `policy`.
    fn fixed_shape(policy: MicroCellPolicy) -> KeyShape {
        KeyShape::new_single_config(
            8,
            1,
            KeyType::uniform(1),
            KeySpaceConfig::new().with_micro_cell_policy(policy),
        )
    }

    /// The ref readers get for a blob the tests serialized.
    fn index_ref(serialized: &SerializedIndex) -> IndexRef {
        IndexRef::new(WalPosition::test_value(0), serialized.micro_cells)
    }

    #[test]
    fn test_varlen_index_lookup_and_roundtrip() {
        let metrics = Metrics::new();
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

        for policy in POLICIES {
            let shape = varlen_shape(policy);
            let ks = shape.ks(KeySpace::first());
            let serialized = LookupHeaderIndex.clean_serialize_index(&mut index, ks);
            let index_ref = index_ref(&serialized);
            let bytes = serialized.bytes;
            let lookup = |key: &[u8]| {
                LookupHeaderIndex.lookup_unloaded(ks, &bytes, index_ref, key, &metrics)
            };

            // Lookup present keys
            assert_eq!(Some(WalPosition::test_value(1)), lookup(&[]));
            for b in 0u8..=255 {
                assert_eq!(
                    Some(WalPosition::test_value(b as u64 + 10)),
                    lookup(&[b]),
                    "lookup failed for key [{b}] under {policy:?}",
                );
            }
            assert_eq!(Some(WalPosition::test_value(300)), lookup(&[0, 1, 2, 3, 4]));
            assert_eq!(Some(WalPosition::test_value(400)), lookup(&[255, 255, 255]));

            // Lookup absent keys
            assert_eq!(None, lookup(&[0, 99]));
            assert_eq!(None, lookup(&[1, 2, 3]));

            // Deserialize and verify round-trip
            let deserialized = LookupHeaderIndex.deserialize_index(ks, bytes, index_ref);
            assert_eq!(Some(WalPosition::test_value(1)), deserialized.get(&[]));
            for b in 0u8..=255 {
                assert_eq!(
                    Some(WalPosition::test_value(b as u64 + 10)),
                    deserialized.get(&[b]),
                    "deserialized lookup failed for key [{b}] under {policy:?}",
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

        for policy in POLICIES {
            let shape = varlen_shape(policy);
            let ks = shape.ks(KeySpace::first());
            let serialized = index_format.serialize_index(&table, ks);
            let index_ref = index_ref(&serialized);
            let deserialized = index_format.deserialize_index(ks, serialized.bytes, index_ref);

            for b in 0u8..=31 {
                let got = deserialized.get(&[b]);
                if b % 2 == 0 {
                    assert_eq!(
                        got,
                        Some(WalPosition::INVALID),
                        "key [{b}] should be a tombstone after round-trip under {policy:?}",
                    );
                } else {
                    assert_eq!(
                        got,
                        Some(WalPosition::test_value(b as u64 + 100)),
                        "key [{b}] should survive as a live entry under {policy:?}",
                    );
                }
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

    /// Walks the whole blob in `direction` through `next_entry_unloaded`,
    /// reusing the cache across calls as a real iterator would.
    fn walk(
        ks: &KeySpaceDesc,
        reader: &MockRandomRead,
        index_ref: IndexRef,
        direction: Direction,
        metrics: &Metrics,
    ) -> Vec<(Bytes, WalPosition)> {
        let mut current_key: Option<Bytes> = None;
        let mut out = Vec::new();
        let mut cache = None;
        loop {
            let result = LookupHeaderIndex.next_entry_unloaded(
                ks,
                reader,
                index_ref,
                current_key.as_deref(),
                direction,
                metrics,
                &mut cache,
            );
            let Some((key, pos)) = result else { break };
            current_key = Some(key.clone());
            out.push((key, pos));
        }
        out
    }

    #[test]
    fn test_next_entry_micro_cell_boundary() {
        let metrics = Metrics::new();

        // Create an index with entries that span multiple micro cells
        let mut table = IndexTable::default();

        // Adding keys that should span across micro cells
        for i in 0..20u64 {
            // Create keys with increasing values spread across the key space
            let key_value = i * (u64::MAX / 20);
            let key = key_value.to_be_bytes().to_vec();
            table.insert(Bytes::from(key), WalPosition::test_value(i));
        }

        for policy in POLICIES {
            let shape = fixed_shape(policy);
            let ks = shape.ks(KeySpace::first());
            let serialized = LookupHeaderIndex.clean_serialize_index(&mut table, ks);
            let index_ref = index_ref(&serialized);
            let reader = MockRandomRead::new(serialized.bytes);

            let forward = walk(ks, &reader, index_ref, Direction::Forward, &metrics);
            assert_eq!(
                forward.len(),
                20,
                "{policy:?}: should iterate through all 20 entries"
            );
            let backward = walk(ks, &reader, index_ref, Direction::Backward, &metrics);
            assert_eq!(
                backward.len(),
                20,
                "{policy:?}: should iterate through all 20 entries backward"
            );
            for (i, (key, pos)) in forward.iter().enumerate() {
                assert_eq!(key.as_ref(), &(i as u64 * (u64::MAX / 20)).to_be_bytes());
                assert_eq!(*pos, WalPosition::test_value(i as u64));
                assert_eq!(&backward[19 - i], &forward[i]);
            }
        }
    }

    #[test]
    fn test_next_entry_cache_reduces_io() {
        let metrics = Metrics::new();
        let mut table = IndexTable::default();
        // Pack many consecutive keys so they share micro-cells.
        for i in 0u64..50 {
            let key = (i * 100_000_000_000_000u64).to_be_bytes().to_vec();
            table.insert(Bytes::from(key), WalPosition::test_value(i));
        }

        for policy in POLICIES {
            let shape = fixed_shape(policy);
            let ks = shape.ks(KeySpace::first());
            let index_format = LookupHeaderIndex;
            let serialized = index_format.clean_serialize_index(&mut table, ks);
            let index_ref = IndexRef::new(WalPosition::test_value(42), serialized.micro_cells);
            let reader = MockRandomRead::new(serialized.bytes);

            // Iterate with cache enabled, counting reads.
            reader.reset_call_count();
            let cached = walk(ks, &reader, index_ref, Direction::Forward, &metrics);
            assert_eq!(cached.len(), 50);
            let cached_reads = reader.call_count();

            // Iterate again without cache (fresh None each call).
            let mut current_key: Option<Bytes> = None;
            reader.reset_call_count();

            loop {
                let result = index_format.next_entry_unloaded(
                    ks,
                    &reader,
                    index_ref,
                    current_key.as_deref(),
                    Direction::Forward,
                    &metrics,
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
                "{policy:?}: cache={cached_reads} should be less than no-cache={uncached_reads}"
            );
        }
    }

    /// Mirrors `test_next_entry_micro_cell_boundary` for variable-length key spaces.
    /// Uses 256 single-byte keys to guarantee multiple micro-cells are populated,
    /// then verifies forward and backward iteration visits every key in order.
    #[test]
    fn test_varlen_next_entry_unloaded() {
        let metrics = Metrics::new();

        // Build a sorted key list to compare against iterator output.
        let mut keys: Vec<Vec<u8>> = (0u8..=255).map(|b| vec![b]).collect();
        keys.push(vec![]); // empty key sorts first
        keys.sort();

        let mut table = IndexTable::default();
        for (i, key) in keys.iter().enumerate() {
            table.insert(Bytes::from(key.clone()), WalPosition::test_value(i as u64));
        }

        for policy in POLICIES {
            let shape = varlen_shape(policy);
            let ks = shape.ks(KeySpace::first());
            let serialized = LookupHeaderIndex.clean_serialize_index(&mut table, ks);
            let index_ref = index_ref(&serialized);
            let reader = MockRandomRead::new(serialized.bytes);

            let forward = walk(ks, &reader, index_ref, Direction::Forward, &metrics);
            assert_eq!(forward.len(), keys.len(), "{policy:?} forward: wrong count");
            for (i, (k, _)) in forward.iter().enumerate() {
                assert_eq!(
                    k.as_ref(),
                    keys[i].as_slice(),
                    "{policy:?} forward: wrong key at {i}"
                );
            }

            let backward = walk(ks, &reader, index_ref, Direction::Backward, &metrics);
            assert_eq!(
                backward.len(),
                keys.len(),
                "{policy:?} backward: wrong count"
            );
            for (i, (k, _)) in backward.iter().enumerate() {
                let expected = &keys[keys.len() - 1 - i];
                assert_eq!(
                    k.as_ref(),
                    expected.as_slice(),
                    "{policy:?} backward: wrong key at position {i}"
                );
            }
        }
    }

    /// The legacy policy reproduces the pre-ref blob layout byte for byte: a
    /// 1024-byte header of 128 elements, then the entries.
    #[test]
    fn test_legacy_policy_layout() {
        let shape = fixed_shape(MicroCellPolicy::LEGACY);
        let ks = shape.ks(KeySpace::first());
        let element_size = ks.index_key_element_size().unwrap().1;

        let mut table = IndexTable::default();
        let keys: Vec<u64> = vec![0, 1 << 40, 1 << 56, 3 << 61, u64::MAX];
        for (i, k) in keys.iter().enumerate() {
            table.insert(
                Bytes::from(k.to_be_bytes().to_vec()),
                WalPosition::test_value(i as u64),
            );
        }
        let serialized = LookupHeaderIndex.clean_serialize_index(&mut table, ks);
        let index_ref = index_ref(&serialized);
        let SerializedIndex { bytes, micro_cells } = serialized;
        assert_eq!(micro_cells, 128);
        assert_eq!(bytes.len(), 1024 + keys.len() * element_size);
        // Element of the micro-cell holding key 0 starts right after the header.
        let first = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
        assert_eq!(first, 1024);
        // u64::MAX lands in the last micro-cell, whose section ends the blob.
        let last_to = u32::from_be_bytes(bytes[127 * 8 + 4..128 * 8].try_into().unwrap()) as usize;
        assert_eq!(last_to, bytes.len());
        let table = LookupHeaderIndex.deserialize_index(ks, bytes, index_ref);
        assert_eq!(table.len(), keys.len());
    }

    /// Scaled blobs of every size from empty up to several micro-cell count
    /// steps, fixed-size keys: the count follows the policy, the blob has
    /// exactly header plus entries, every key is found, iteration visits
    /// every key in order, and the round trip holds.
    #[test]
    fn test_scaled_fixed_counts_step_with_size() {
        let metrics = Metrics::new();
        let policy = MicroCellPolicy::Scaled {
            section_target_log2: 5,
        };
        let shape = fixed_shape(policy);
        let ks = shape.ks(KeySpace::first());
        let element_size = ks.index_key_element_size().unwrap().1;
        let mut rng = 0x1234_5678_9abc_def0u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };

        let mut seen_counts = std::collections::BTreeSet::new();
        for e in 0..=300usize {
            let mut table = IndexTable::default();
            let mut expected = Vec::new();
            while table.len() < e {
                let key = next().to_be_bytes().to_vec();
                if table.get(&key).is_some() {
                    continue;
                }
                let pos = WalPosition::test_value(table.len() as u64 + 1);
                table.insert(Bytes::from(key.clone()), pos);
                expected.push((key, pos));
            }
            expected.sort();
            let serialized = LookupHeaderIndex.clean_serialize_index(&mut table, ks);
            let index_ref = index_ref(&serialized);
            let SerializedIndex { bytes, micro_cells } = serialized;
            assert_eq!(micro_cells, policy.micro_cells(e * element_size));
            assert!(micro_cells.is_power_of_two());
            assert_eq!(bytes.len(), header_len(micro_cells) + e * element_size);
            seen_counts.insert(micro_cells);

            for (key, pos) in &expected {
                assert_eq!(
                    LookupHeaderIndex.lookup_unloaded(ks, &bytes, index_ref, key, &metrics),
                    Some(*pos),
                    "e {e} n {micro_cells}"
                );
            }
            let probe = next().to_be_bytes();
            if !expected.iter().any(|(k, _)| k.as_slice() == probe) {
                assert_eq!(
                    LookupHeaderIndex.lookup_unloaded(ks, &bytes, index_ref, &probe, &metrics),
                    None
                );
            }
            let reader = MockRandomRead::new(bytes.clone());
            let forward = walk(ks, &reader, index_ref, Direction::Forward, &metrics);
            assert_eq!(forward.len(), e, "e {e} n {micro_cells}");
            for (got, want) in forward.iter().zip(&expected) {
                assert_eq!(got.0.as_ref(), want.0.as_slice());
                assert_eq!(got.1, want.1);
            }
            let backward = walk(ks, &reader, index_ref, Direction::Backward, &metrics);
            assert_eq!(backward.len(), e);
            let table = LookupHeaderIndex.deserialize_index(ks, bytes, index_ref);
            assert_eq!(table.len(), e);
            for (key, pos) in &expected {
                assert_eq!(table.get(key), Some(*pos));
            }
        }
        assert!(
            seen_counts.len() >= 4,
            "the sweep should cross several count steps, saw {seen_counts:?}"
        );
    }

    /// Skewed variable-length keys: every key shares its first four bytes,
    /// so all of them fall into one micro-cell whatever the count. Lookups
    /// and iteration must still work.
    #[test]
    fn test_scaled_varlen_skew() {
        let metrics = Metrics::new();
        let shape = varlen_shape(MicroCellPolicy::Scaled {
            section_target_log2: 5,
        });
        let ks = shape.ks(KeySpace::first());

        let mut table = IndexTable::default();
        let mut keys = Vec::new();
        for i in 0u32..300 {
            let mut key = vec![7, 7, 7, 7];
            key.extend_from_slice(&i.to_be_bytes());
            keys.push(key.clone());
            table.insert(Bytes::from(key), WalPosition::test_value(i as u64 + 1));
        }
        keys.sort();

        let serialized = LookupHeaderIndex.clean_serialize_index(&mut table, ks);
        let index_ref = index_ref(&serialized);
        let SerializedIndex { bytes, micro_cells } = serialized;
        assert!(micro_cells > 1, "blob of {} bytes", bytes.len());
        // exactly one non-empty header element
        let non_empty = (0..micro_cells as usize)
            .filter(|mc| bytes[mc * 8..mc * 8 + 8] != [0u8; 8])
            .count();
        assert_eq!(non_empty, 1);

        for (i, key) in keys.iter().enumerate() {
            let want = u32::from_be_bytes(key[4..8].try_into().unwrap()) as u64 + 1;
            assert_eq!(
                LookupHeaderIndex.lookup_unloaded(ks, &bytes, index_ref, key, &metrics),
                Some(WalPosition::test_value(want)),
                "key {i}"
            );
        }
        assert_eq!(
            LookupHeaderIndex.lookup_unloaded(ks, &bytes, index_ref, &[7, 7, 7, 8], &metrics),
            None
        );
        let reader = MockRandomRead::new(bytes.clone());
        let forward = walk(ks, &reader, index_ref, Direction::Forward, &metrics);
        assert_eq!(forward.len(), keys.len());
        for (got, want) in forward.iter().zip(&keys) {
            assert_eq!(got.0.as_ref(), want.as_slice());
        }
        let table = LookupHeaderIndex.deserialize_index(ks, bytes, index_ref);
        assert_eq!(table.len(), keys.len());
    }

    /// The count a blob was written with is not recoverable from its bytes;
    /// the legacy and the default policy give the same table different
    /// layouts, which is why the ref records the count.
    #[test]
    fn test_policies_choose_different_counts() {
        let legacy_shape = fixed_shape(MicroCellPolicy::LEGACY);
        let scaled_shape = fixed_shape(MicroCellPolicy::DEFAULT);
        let mut table = IndexTable::default();
        for i in 0u64..1000 {
            table.insert(
                Bytes::from(
                    i.wrapping_mul(0x0123_4567_89ab_cdefu64)
                        .to_be_bytes()
                        .to_vec(),
                ),
                WalPosition::test_value(i),
            );
        }
        let legacy =
            LookupHeaderIndex.clean_serialize_index(&mut table, legacy_shape.ks(KeySpace::first()));
        let scaled =
            LookupHeaderIndex.clean_serialize_index(&mut table, scaled_shape.ks(KeySpace::first()));
        assert_eq!(legacy.micro_cells, 128);
        assert_eq!(scaled.micro_cells, 1);
        assert_eq!(legacy.len(), scaled.len() + 1024 - 8);
    }
}
