use crate::WalPosition;
use crate::container::LargeTableContainer;
use crate::key_shape::KeyShape;
use crate::large_table::SnapshotEntryData;
use crate::metrics::Metrics;
use crate::wal::layout::WalLayout;
use crate::wal::position::WalFileId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};

/// On-disk control-region format version.
///
/// The file layout is either:
///
/// - **V1** (legacy): raw `bincode`-serialized [`legacy_v1::ControlRegionV1`]
///   starting at byte 0. No header. Distinguished by the fact that the first
///   8 bytes decode as a plausible `last_position: u64` — never the
///   versioned-header sentinel.
/// - **V2** (legacy) / **V3**: 8-byte little-endian sentinel `u64::MAX`,
///   followed by a 4-byte little-endian version number, followed by the
///   `bincode`-serialized control-region payload. The version field selects
///   the payload schema: V2 = pre-sharding `IndexLevels` (positions only);
///   V3 = current layout with optional shard btrees per cell. Using
///   `u64::MAX` as the discriminator works because the first field of the
///   V1 layout is `last_position: u64`, which would only take the value
///   `u64::MAX` in a pathological/corrupt file — never in a real database.
///
/// Writers always emit V3; readers detect the sentinel and fall back to V2
/// or V1 parsing + one-way migration as needed.
const CONTROL_REGION_V2_SENTINEL: u64 = u64::MAX;
const CONTROL_REGION_V2_VERSION: u32 = 2;
const CONTROL_REGION_V3_VERSION: u32 = 3;
const CONTROL_REGION_V2_HEADER_LEN: usize = 12;

#[derive(Serialize, Deserialize, Debug)]
#[doc(hidden)] // Used by tools/tideconsole for control region inspection
pub struct ControlRegion {
    /// 0 when wal is empty or nothing has been processed
    last_position: u64,
    snapshot: LargeTableContainer<SnapshotEntryData>,
    /// Keyspace names in canonical id order — used to verify that the
    /// canonical shape resolved at open matches the one this snapshot was
    /// written with, and to recover the canonical order for databases that
    /// predate the shape file.
    #[serde(default)]
    keyspace_names: Vec<String>,
}

pub(crate) struct ControlRegionStore {
    path: PathBuf,
    last_position: u64,
}

/// Set of index WAL files that should be force-relocated because their
/// live-byte occupancy is below threshold. Owns the `WalLayout` used to
/// compute it so membership can be tested directly against a `WalPosition`.
pub struct RelocateFiles {
    files: HashSet<WalFileId>,
    layout: WalLayout,
}

impl RelocateFiles {
    pub(crate) fn new(files: HashSet<WalFileId>, layout: WalLayout) -> Self {
        Self { files, layout }
    }

    /// Builds a `RelocateFiles` from a per-file live-bytes accumulator.
    ///
    /// A file is selected for relocation when its live bytes are below
    /// `wal_file_size * min_occupancy_pct / 100`. Files whose id is at or
    /// above the file id holding `exclude_from` are skipped — those files
    /// were created during or after the snapshot pass that built the
    /// accumulator, so their occupancy is transient.
    ///
    /// `min_occupancy_pct == 0` disables relocation and yields an empty set.
    pub(crate) fn from_accumulator(
        alive_bytes: HashMap<WalFileId, u64>,
        layout: WalLayout,
        min_occupancy_pct: u8,
        exclude_from: u64,
    ) -> Self {
        if min_occupancy_pct == 0 {
            return Self::new(HashSet::new(), layout);
        }
        let exclude_from_file = layout.locate_file(exclude_from);
        let threshold_bytes = layout
            .wal_file_size
            .saturating_mul(min_occupancy_pct as u64)
            / 100;
        let files = alive_bytes
            .into_iter()
            .filter_map(|(file_id, bytes)| {
                if file_id >= exclude_from_file {
                    return None;
                }
                if bytes < threshold_bytes {
                    Some(file_id)
                } else {
                    None
                }
            })
            .collect();
        Self::new(files, layout)
    }

    /// Returns true if `pos` lives in one of the low-occupancy files.
    pub fn contains(&self, pos: &WalPosition) -> bool {
        if self.files.is_empty() {
            return false;
        }
        self.files.contains(&self.layout.locate_file(pos.offset()))
    }

    /// Returns true if any position in `positions` lives in a low-occupancy file.
    pub fn contains_any(&self, mut positions: impl Iterator<Item = WalPosition>) -> bool {
        if self.files.is_empty() {
            return false;
        }
        positions.any(|p| self.contains(&p))
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
}

impl ControlRegion {
    pub fn new_empty(key_shape: &KeyShape) -> Self {
        let snapshot =
            LargeTableContainer::new_from_key_shape(key_shape, SnapshotEntryData::empty());
        let keyspace_names = key_shape
            .iter_ks()
            .map(|ks| ks.name().to_string())
            .collect();
        Self {
            last_position: 0,
            snapshot,
            keyspace_names,
        }
    }

    pub fn new(
        snapshot: LargeTableContainer<SnapshotEntryData>,
        last_position: u64,
        key_shape: &KeyShape,
    ) -> Self {
        let keyspace_names = key_shape
            .iter_ks()
            .map(|ks| ks.name().to_string())
            .collect();
        Self {
            snapshot,
            last_position,
            keyspace_names,
        }
    }

    pub fn read_or_create(path: &Path, key_shape: &KeyShape) -> Self {
        match Self::read(path, key_shape) {
            Ok(control_region) => control_region,
            Err(err) if err.kind() == ErrorKind::NotFound => ControlRegion::new_empty(key_shape),
            Err(err) => {
                panic!("Failed to read control region file: {err:?}")
            }
        }
    }

    #[doc(hidden)] // Used by tools/tideconsole for control region inspection
    pub fn read(path: &Path, key_shape: &KeyShape) -> io::Result<Self> {
        let bytes = fs::read(path)?;
        let mut control_region = Self::deserialize_any_version(&bytes)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
        control_region.populate_keyspace_names_if_empty(key_shape);
        control_region.verify_shape(key_shape);
        control_region.extend_snapshot_if_needed(key_shape);
        Ok(control_region)
    }

    /// Deserializes a control region from bytes, transparently handling
    /// V1 (pre-levels), V2 (pre-sharding `IndexLevels`), and V3 (current)
    /// formats.
    ///
    /// V1 control regions are migrated on read: each single-position cell
    /// becomes a one-level `IndexLevels`. V2 control regions are migrated
    /// on read: each cell's `IndexLevels` is rebuilt with an empty shard
    /// btree. The migration is one-way — the next successful `store`
    /// rewrites the file in V3 format.
    fn deserialize_any_version(bytes: &[u8]) -> Result<Self, bincode::Error> {
        if bytes.len() >= CONTROL_REGION_V2_HEADER_LEN {
            let sentinel = u64::from_le_bytes(bytes[..8].try_into().unwrap());
            if sentinel == CONTROL_REGION_V2_SENTINEL {
                let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
                let payload = &bytes[CONTROL_REGION_V2_HEADER_LEN..];
                return match version {
                    CONTROL_REGION_V3_VERSION => bincode::deserialize::<ControlRegion>(payload),
                    CONTROL_REGION_V2_VERSION => {
                        let v2: legacy_v2::ControlRegionV2 = bincode::deserialize(payload)?;
                        Ok(v2.migrate())
                    }
                    _ => panic!(
                        "Unsupported control region version {version}; expected \
                         {CONTROL_REGION_V3_VERSION} (or legacy {CONTROL_REGION_V2_VERSION}). \
                         This binary is older than the database it is trying to open."
                    ),
                };
            }
        }
        // No sentinel ⇒ legacy V1 layout.
        let v1: legacy_v1::ControlRegionV1 = bincode::deserialize(bytes)?;
        Ok(v1.migrate())
    }

    /// Populates keyspace names from key_shape if they're empty (for old control regions)
    fn populate_keyspace_names_if_empty(&mut self, key_shape: &KeyShape) {
        if self.keyspace_names.is_empty() {
            // This is an old control region without keyspace names
            // Populate names for existing keyspaces from the current key_shape
            self.keyspace_names = key_shape
                .iter_ks()
                .take(self.snapshot.data.len())
                .map(|ks| ks.name().to_string())
                .collect();
        }
    }

    /// Extends the snapshot to include new keyspaces if the key_shape has more keyspaces
    fn extend_snapshot_if_needed(&mut self, key_shape: &KeyShape) {
        let snapshot_len = self.snapshot.data.len();
        let num_ks = key_shape.num_ks();

        if snapshot_len < num_ks {
            // Add empty entries for new keyspaces
            for ks in key_shape.iter_ks().skip(snapshot_len) {
                let new_ks_snapshot =
                    LargeTableContainer::new_keyspace(ks, SnapshotEntryData::empty());
                self.snapshot.data.push(new_ks_snapshot);
                self.keyspace_names.push(ks.name().to_string());
            }
        }
    }

    /// Verifies that the canonical `key_shape` matches the shape this control
    /// region was written with. `Db::open` resolves the declared keyspace
    /// order to the canonical one before reading the control region, so a
    /// mismatch here means the canonical registry (shape file) and the
    /// control region have diverged — not a user-level reorder.
    fn verify_shape(&self, key_shape: &KeyShape) {
        let snapshot_len = self.snapshot.data.len();
        let num_ks = key_shape.num_ks();

        // We allow adding keyspaces at the end of the canonical order, but
        // not removing them
        if snapshot_len > num_ks {
            panic!(
                "Control region has {snapshot_len} key spaces, while configuration has {num_ks}. Removing key spaces is not supported"
            );
        }

        // Verify that existing keyspaces match by name in canonical order
        for (idx, ks_desc) in key_shape.iter_ks().enumerate().take(snapshot_len) {
            let current_name = ks_desc.name();

            // Check if we have stored keyspace names (backward compatibility: might be empty for old control regions)
            if idx < self.keyspace_names.len() {
                let stored_name = &self.keyspace_names[idx];
                if current_name != stored_name {
                    panic!(
                        "Keyspace mismatch at canonical position {idx}: control region has keyspace '{stored_name}', \
                         but configuration resolved to '{current_name}'. \
                         The shape file and control region disagree; renaming keyspaces is not supported."
                    );
                }
            }
        }
    }

    pub fn snapshot(&self) -> &LargeTableContainer<SnapshotEntryData> {
        &self.snapshot
    }

    pub fn last_position(&self) -> u64 {
        self.last_position
    }

    pub fn last_index_wal_position(&self) -> Option<WalPosition> {
        self.snapshot.iter_valid_val_positions().max()
    }
}

impl ControlRegionStore {
    pub fn new(path: PathBuf, control_region: &ControlRegion) -> Self {
        Self {
            path,
            last_position: control_region.last_position,
        }
    }

    pub fn store(
        &mut self,
        snapshot: LargeTableContainer<SnapshotEntryData>,
        last_position: u64,
        key_shape: &KeyShape,
        metrics: &Metrics,
    ) {
        assert!(
            last_position >= self.last_position,
            "control region last_position regressed: new={last_position} < old={}",
            self.last_position,
        );
        let control_region = ControlRegion::new(snapshot, last_position, key_shape);
        // V3 layout: sentinel + version + bincode payload. See the comment
        // on `CONTROL_REGION_V2_SENTINEL` for the rationale.
        let mut serialized = Vec::with_capacity(CONTROL_REGION_V2_HEADER_LEN + 4 * 1024);
        serialized.extend_from_slice(&CONTROL_REGION_V2_SENTINEL.to_le_bytes());
        serialized.extend_from_slice(&CONTROL_REGION_V3_VERSION.to_le_bytes());
        bincode::serialize_into(&mut serialized, &control_region)
            .expect("Failed to serialize control region");
        let temp_file = self.path.with_extension(".bak");
        fs::write(&temp_file, &serialized).unwrap_or_else(|e| {
            panic!(
                "Failed to write control region file {}: {}",
                temp_file.display(),
                e
            )
        });
        metrics
            .snapshot_written_bytes
            .inc_by(serialized.len() as u64);
        fs::rename(&temp_file, &self.path).unwrap_or_else(|e| {
            panic!(
                "Failed to rename control region file from {} to {}: {}",
                temp_file.display(),
                self.path.display(),
                e
            )
        });
        self.last_position = last_position;
    }

    /// The path to the control region file
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn last_position(&self) -> u64 {
        self.last_position
    }
}

/// Legacy on-disk representation — read-only, used for one-way migration of
/// V2 control regions to V3.
///
/// V2 stored each cell's `IndexLevels` as just the per-level positions
/// `SmallVec`. V3 keeps the same `positions` plus a `BTreeMap` of
/// auto-sharding shards (empty for cells that haven't split). The migration
/// rebuilds each cell's `IndexLevels` with `shards = BTreeMap::new()`.
mod legacy_v2 {
    use super::ControlRegion;
    use crate::cell::CellId;
    use crate::container::LargeTableContainer;
    use crate::index::levels::{INLINE_LEVELS, IndexLevels};
    use crate::large_table::SnapshotEntryData;
    use crate::wal::position::{LastProcessed, WalPosition};
    use serde::{Deserialize, Serialize};
    use smallvec::SmallVec;
    use std::collections::BTreeMap;

    /// Exact on-disk layout of `IndexLevels` prior to the auto-sharding
    /// schema change. Mirrors the V2 wire format so bincode reads cleanly.
    #[derive(Serialize, Deserialize, Debug)]
    pub(super) struct IndexLevelsV2 {
        pub(super) positions: SmallVec<[WalPosition; INLINE_LEVELS]>,
    }

    impl IndexLevelsV2 {
        fn migrate(self) -> IndexLevels {
            IndexLevels::from_legacy_v2_positions(self.positions)
        }
    }

    /// Exact on-disk layout of `SnapshotEntryData` prior to the
    /// auto-sharding schema change.
    #[derive(Serialize, Deserialize, Debug)]
    pub(super) struct SnapshotEntryDataV2 {
        pub(super) levels: IndexLevelsV2,
        pub(super) last_processed: LastProcessed,
    }

    impl SnapshotEntryDataV2 {
        fn migrate(self) -> SnapshotEntryData {
            SnapshotEntryData::from_levels(self.levels.migrate(), self.last_processed)
        }
    }

    /// Exact on-disk layout of `ControlRegion` prior to the auto-sharding
    /// schema change. `keyspace_names` keeps `#[serde(default)]` for
    /// pre-keyspace-names compatibility within V2 files.
    #[derive(Serialize, Deserialize, Debug)]
    pub(super) struct ControlRegionV2 {
        pub(super) last_position: u64,
        pub(super) snapshot: LargeTableContainer<SnapshotEntryDataV2>,
        #[serde(default)]
        pub(super) keyspace_names: Vec<String>,
    }

    impl ControlRegionV2 {
        pub(super) fn migrate(self) -> ControlRegion {
            let data: Vec<BTreeMap<CellId, SnapshotEntryData>> = self
                .snapshot
                .data
                .into_iter()
                .map(|ks_map| {
                    ks_map
                        .into_iter()
                        .map(|(cell, entry)| (cell, entry.migrate()))
                        .collect()
                })
                .collect();
            ControlRegion {
                last_position: self.last_position,
                snapshot: LargeTableContainer { data },
                keyspace_names: self.keyspace_names,
            }
        }
    }
}

/// Legacy on-disk representation — read-only, used for one-way migration of
/// V1 control regions.
///
/// V1 stored a single `WalPosition` per cell. Modern formats (V2/V3) store an
/// `IndexLevels` (a `SmallVec` of per-level `WalPosition`s, inline capacity
/// `INLINE_LEVELS`). The migration maps every valid single position to a
/// one-element level list; `WalPosition::INVALID` maps to an empty list.
mod legacy_v1 {
    use super::ControlRegion;
    use crate::cell::CellId;
    use crate::container::LargeTableContainer;
    use crate::index::levels::IndexLevels;
    use crate::large_table::SnapshotEntryData;
    use crate::wal::position::{LastProcessed, WalPosition};
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    /// Exact on-disk layout of `SnapshotEntryData` prior to the two-level
    /// LSM schema change. Field names and ordering must stay in sync with
    /// the pre-change definition.
    #[derive(Serialize, Deserialize, Clone, Copy, Debug)]
    pub(super) struct SnapshotEntryDataV1 {
        pub(super) position: WalPosition,
        pub(super) last_processed: LastProcessed,
    }

    impl SnapshotEntryDataV1 {
        fn migrate(self) -> SnapshotEntryData {
            SnapshotEntryData::from_levels(
                IndexLevels::from_legacy_position(self.position),
                self.last_processed,
            )
        }
    }

    /// Exact on-disk layout of `ControlRegion` prior to the two-level LSM
    /// schema change. `keyspace_names` keeps `#[serde(default)]` for
    /// pre-keyspace-names compatibility within V1 files.
    #[derive(Serialize, Deserialize, Debug)]
    pub(super) struct ControlRegionV1 {
        pub(super) last_position: u64,
        pub(super) snapshot: LargeTableContainer<SnapshotEntryDataV1>,
        #[serde(default)]
        pub(super) keyspace_names: Vec<String>,
    }

    impl ControlRegionV1 {
        pub(super) fn migrate(self) -> ControlRegion {
            let data: Vec<BTreeMap<CellId, SnapshotEntryData>> = self
                .snapshot
                .data
                .into_iter()
                .map(|ks_map| {
                    ks_map
                        .into_iter()
                        .map(|(cell, entry)| (cell, entry.migrate()))
                        .collect()
                })
                .collect();
            ControlRegion {
                last_position: self.last_position,
                snapshot: LargeTableContainer { data },
                keyspace_names: self.keyspace_names,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell::CellId;
    use crate::index::levels::{IndexLevels, IndexShard};
    use crate::key_shape::{KeyShape, KeyType, UniformKeyConfig};
    use crate::wal::position::{LastProcessed, WalPosition};
    use std::collections::BTreeMap;

    /// Returns a minimal key_shape with one uniform keyspace of a few cells,
    /// suitable for round-tripping control regions through disk.
    fn tiny_shape() -> KeyShape {
        KeyShape::new_single(
            /*key_size=*/ 8,
            /*mutexes=*/ 2,
            KeyType::Uniform(UniformKeyConfig::new(/*cells_per_mutex=*/ 2)),
        )
    }

    #[test]
    fn v3_roundtrip() {
        let dir = tempdir::TempDir::new("cr_v3_roundtrip").unwrap();
        let path = dir.path().join("cr");
        let shape = tiny_shape();

        let mut cr = ControlRegion::new_empty(&shape);
        // Populate two cells: one with a single L0, one with a sharded L1.
        // Round-tripping both flavors confirms the V3 payload handles the
        // shard btree (empty + non-empty) intact.
        cr.snapshot.data[0].insert(
            CellId::Integer(0),
            SnapshotEntryData::from_levels(
                IndexLevels::single(WalPosition::test_value(7777)),
                LastProcessed::new_test(42),
            ),
        );
        let mut shard_btree: BTreeMap<Vec<u8>, IndexShard> = BTreeMap::new();
        shard_btree.insert(
            b"aaaaaaaa".to_vec(),
            IndexShard::new(WalPosition::test_value(1001), b"low_max".to_vec()),
        );
        shard_btree.insert(
            b"mid".to_vec(),
            IndexShard::new(WalPosition::test_value(1002), b"zzzzzzzz".to_vec()),
        );
        cr.snapshot.data[0].insert(
            CellId::Integer(1),
            SnapshotEntryData::from_levels(
                IndexLevels::sharded(shard_btree.clone()),
                LastProcessed::new_test(43),
            ),
        );
        cr.last_position = 100;

        let mut store = ControlRegionStore::new(path.clone(), &cr);
        store.store(cr.snapshot, cr.last_position, &shape, &Metrics::new());

        let read_cr = ControlRegion::read(&path, &shape).unwrap();
        assert_eq!(read_cr.last_position, 100);
        let entry0 = read_cr.snapshot.data[0]
            .get(&CellId::Integer(0))
            .expect("cell 0 exists");
        assert_eq!(entry0.levels.len(), 1);
        assert_eq!(entry0.levels.latest(), Some(WalPosition::test_value(7777)));
        assert!(!entry0.levels.is_sharded());
        assert_eq!(entry0.last_processed, LastProcessed::new_test(42));

        let entry1 = read_cr.snapshot.data[0]
            .get(&CellId::Integer(1))
            .expect("cell 1 exists");
        assert!(entry1.levels.is_sharded());
        assert_eq!(entry1.levels.shards(), &shard_btree);
        assert_eq!(entry1.last_processed, LastProcessed::new_test(43));
    }

    #[test]
    fn v2_file_migrates_to_v3() {
        // Hand-build a V2 byte stream (sentinel + version=2 + V2 payload)
        // and read it through the public path to ensure the V2→V3 migration
        // walks every cell and zero-fills the shard btree.
        use smallvec::SmallVec;

        let dir = tempdir::TempDir::new("cr_v2_migrate").unwrap();
        let path = dir.path().join("cr_v2");
        let shape = tiny_shape();
        let num_ks = shape.num_ks();

        // [INVALID, L1] — the post-promote shape, exercised to make sure the
        // SmallVec round-trips faithfully through migration.
        let mut promoted: SmallVec<[WalPosition; 2]> = SmallVec::new();
        promoted.push(WalPosition::INVALID);
        promoted.push(WalPosition::test_value(9001));

        let mut single: SmallVec<[WalPosition; 2]> = SmallVec::new();
        single.push(WalPosition::test_value(9002));

        let mut ks_map: BTreeMap<CellId, legacy_v2::SnapshotEntryDataV2> = BTreeMap::new();
        ks_map.insert(
            CellId::Integer(0),
            legacy_v2::SnapshotEntryDataV2 {
                levels: legacy_v2::IndexLevelsV2 {
                    positions: promoted,
                },
                last_processed: LastProcessed::new_test(10),
            },
        );
        ks_map.insert(
            CellId::Integer(1),
            legacy_v2::SnapshotEntryDataV2 {
                levels: legacy_v2::IndexLevelsV2 { positions: single },
                last_processed: LastProcessed::new_test(11),
            },
        );
        let mut data = vec![ks_map];
        for _ in 1..num_ks {
            data.push(BTreeMap::new());
        }
        let keyspace_names: Vec<String> = shape.iter_ks().map(|ks| ks.name().to_string()).collect();
        let v2 = legacy_v2::ControlRegionV2 {
            last_position: 77,
            snapshot: LargeTableContainer { data },
            keyspace_names,
        };
        let payload = bincode::serialize(&v2).unwrap();
        let mut bytes = Vec::with_capacity(CONTROL_REGION_V2_HEADER_LEN + payload.len());
        bytes.extend_from_slice(&CONTROL_REGION_V2_SENTINEL.to_le_bytes());
        bytes.extend_from_slice(&CONTROL_REGION_V2_VERSION.to_le_bytes());
        bytes.extend_from_slice(&payload);
        std::fs::write(&path, &bytes).unwrap();

        let read_cr = ControlRegion::read(&path, &shape).unwrap();
        assert_eq!(read_cr.last_position, 77);
        let cell0 = read_cr.snapshot.data[0]
            .get(&CellId::Integer(0))
            .expect("cell 0 present");
        assert_eq!(cell0.levels.len(), 2);
        assert_eq!(cell0.levels.latest(), Some(WalPosition::test_value(9001)));
        assert!(
            !cell0.levels.is_sharded(),
            "V2 → V3 yields empty shard btree"
        );

        let cell1 = read_cr.snapshot.data[0]
            .get(&CellId::Integer(1))
            .expect("cell 1 present");
        assert_eq!(cell1.levels.len(), 1);
        assert_eq!(cell1.levels.latest(), Some(WalPosition::test_value(9002)));
        assert!(!cell1.levels.is_sharded());
    }

    #[test]
    fn v1_file_migrates_to_v3() {
        // Hand-build a V1 byte stream and read it through the public `read`
        // path to ensure the sentinel detection and migration do the right
        // thing on a file that predates the schema change.
        let dir = tempdir::TempDir::new("cr_v1_migrate").unwrap();
        let path = dir.path().join("cr_v1");
        let shape = tiny_shape();
        let num_ks = shape.num_ks();

        let mut ks_map: BTreeMap<CellId, legacy_v1::SnapshotEntryDataV1> = BTreeMap::new();
        ks_map.insert(
            CellId::Integer(0),
            legacy_v1::SnapshotEntryDataV1 {
                position: WalPosition::test_value(1234),
                last_processed: LastProcessed::new_test(10),
            },
        );
        ks_map.insert(
            CellId::Integer(1),
            legacy_v1::SnapshotEntryDataV1 {
                position: WalPosition::INVALID,
                last_processed: LastProcessed::none(),
            },
        );
        let mut data = vec![ks_map];
        // Pad to match shape's keyspace count; verify_shape requires equality.
        for _ in 1..num_ks {
            data.push(BTreeMap::new());
        }
        let keyspace_names: Vec<String> = shape.iter_ks().map(|ks| ks.name().to_string()).collect();
        let v1 = legacy_v1::ControlRegionV1 {
            last_position: 55,
            snapshot: LargeTableContainer { data },
            keyspace_names,
        };
        let bytes = bincode::serialize(&v1).unwrap();
        std::fs::write(&path, &bytes).unwrap();

        let read_cr = ControlRegion::read(&path, &shape).unwrap();
        assert_eq!(read_cr.last_position, 55);

        let cell0 = read_cr.snapshot.data[0]
            .get(&CellId::Integer(0))
            .expect("cell 0 present");
        assert_eq!(cell0.levels.len(), 1);
        assert_eq!(cell0.levels.latest(), Some(WalPosition::test_value(1234)));

        let cell1 = read_cr.snapshot.data[0]
            .get(&CellId::Integer(1))
            .expect("cell 1 present");
        assert!(cell1.levels.is_empty(), "INVALID ⇒ empty level list");
    }

    #[test]
    fn test_relocate_files_from_accumulator() {
        let layout = WalLayout::new_simple(1000);
        let file = |off: u64| layout.locate_file(off);

        // 4 files (ids 0..=3) with varying live-byte counts.
        // wal_file_size = 1000, threshold pct = 15 ⇒ threshold_bytes = 150.
        // file 0: 100 bytes  → low.
        // file 1: 500 bytes  → keep.
        // file 2:  50 bytes  → low.
        // file 3: 200 bytes  → keep.
        let mut acc: HashMap<WalFileId, u64> = HashMap::new();
        acc.insert(file(100), 100);
        acc.insert(file(1500), 500);
        acc.insert(file(2500), 50);
        acc.insert(file(3500), 200);

        // Without exclusion (set exclude_from past every file), files 0 and 2 are flagged.
        let rf = RelocateFiles::from_accumulator(acc.clone(), layout.clone(), 15, u64::MAX);
        assert!(rf.contains(&WalPosition::new(100, 1)));
        assert!(!rf.contains(&WalPosition::new(1500, 1)));
        assert!(rf.contains(&WalPosition::new(2500, 1)));
        assert!(!rf.contains(&WalPosition::new(3500, 1)));

        // exclude_from inside file 2 ⇒ files 2 and 3 dropped from candidates.
        let rf = RelocateFiles::from_accumulator(acc.clone(), layout.clone(), 15, 2200);
        assert!(rf.contains(&WalPosition::new(100, 1)));
        assert!(!rf.contains(&WalPosition::new(2500, 1)));
        assert!(!rf.contains(&WalPosition::new(3500, 1)));

        // pct == 0 disables the mechanism.
        let rf = RelocateFiles::from_accumulator(acc, layout, 0, u64::MAX);
        assert!(rf.is_empty());
    }
}
