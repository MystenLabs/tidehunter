use crate::crc::IntoBytesFixed;
use crate::key_shape::KeyShape;
use crate::large_table::{LargeTableContainer, Version};
use crate::wal::WalPosition;
use bytes::{Buf, BufMut, BytesMut};

pub struct ControlRegion {
    version: Version,
    /// WalPosition::INVALID when wal is empty
    last_position: WalPosition,
    snapshot: LargeTableContainer<WalPosition>,
}

impl IntoBytesFixed for ControlRegion {
    fn len(&self) -> usize {
        // similar logic in ControlRegion::len_bytes_from_key_shape
        let mut len = Version::LENGTH + WalPosition::LENGTH + 4;
        for ks_table in &self.snapshot.0 {
            let num_mutexes = ks_table.len();
            let per_mutex = ks_table.get(0).expect("Ks table can not be empty").len();
            len += 4;
            len += num_mutexes * per_mutex * WalPosition::LENGTH;
        }
        len
    }

    fn write_into_bytes(&self, buf: &mut BytesMut) {
        buf.put_u64(self.version.0);
        self.last_position.write_to_buf(buf);
        buf.put_u32(self.snapshot.0.len() as u32);
        for ks_table in &self.snapshot.0 {
            let num_mutexes = ks_table.len();
            let per_mutex = ks_table.get(0).expect("Ks table can not be empty").len();
            buf.put_u16(num_mutexes as u16);
            buf.put_u16(per_mutex as u16);
            for row in ks_table {
                assert_eq!(row.len(), per_mutex);
                for entry in row {
                    entry.write_to_buf(buf);
                }
            }
        }
    }
}

impl ControlRegion {
    pub fn new_empty(key_shape: &KeyShape) -> Self {
        let snapshot = LargeTableContainer::new_from_key_shape(key_shape, WalPosition::INVALID);
        Self {
            version: Version::ZERO,
            last_position: WalPosition::INVALID,
            snapshot,
        }
    }

    pub fn new(
        snapshot: LargeTableContainer<WalPosition>,
        version: Version,
        last_position: WalPosition,
    ) -> Self {
        Self {
            snapshot,
            version,
            last_position,
        }
    }

    pub fn version_from_bytes(mut bytes: &[u8]) -> Version {
        Version(bytes.get_u64())
    }

    pub fn from_slice(mut bytes: &[u8], key_shape: &KeyShape) -> Self {
        assert_eq!(bytes.len(), Self::len_bytes_from_key_shape(key_shape));
        let version = bytes.get_u64();
        let replay_from = WalPosition::read_from_buf(&mut bytes);
        let num_ks = bytes.get_u32() as usize;
        assert_eq!(num_ks, key_shape.num_ks());
        let mut snapshot = Vec::with_capacity(num_ks);
        for ks in key_shape.iter_ks() {
            let num_mutexes = bytes.get_u16() as usize;
            let per_mutex = bytes.get_u16() as usize;
            if num_mutexes != ks.num_mutexes() {
                panic!(
                    "Key space {} must have {} mutexes, snapshot has {}",
                    ks.id().as_usize(),
                    ks.num_mutexes(),
                    num_mutexes
                );
            }
            if per_mutex != ks.cells_per_mutex() {
                panic!(
                    "Key space {} must have {} entries per mutex, snapshot has {}",
                    ks.id().as_usize(),
                    ks.cells_per_mutex(),
                    per_mutex
                );
            }
            let mut ks_table = Vec::with_capacity(num_mutexes);
            for _ in 0..num_mutexes {
                let mut row = Vec::with_capacity(per_mutex);
                for _ in 0..per_mutex {
                    row.push(WalPosition::read_from_buf(&mut bytes));
                }
                ks_table.push(row);
            }
            snapshot.push(ks_table);
        }
        let snapshot = LargeTableContainer(snapshot);
        Self {
            version: Version(version),
            last_position: replay_from,
            snapshot,
        }
    }

    pub fn snapshot(&self) -> &LargeTableContainer<WalPosition> {
        &self.snapshot
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn last_position(&self) -> WalPosition {
        self.last_position
    }

    pub fn len_bytes_from_key_shape(key_shape: &KeyShape) -> usize {
        // similar logic in ControlRegion::len
        let mut len = Version::LENGTH + WalPosition::LENGTH + 4;
        for ks in key_shape.iter_ks() {
            let num_mutexes = ks.num_mutexes();
            let per_mutex = ks.cells_per_mutex();
            len += 4;
            len += num_mutexes * per_mutex * WalPosition::LENGTH;
        }
        len
    }
}

#[test]
fn test_control_region_serialization() {
    use crate::crc::CrcFrame;

    let (key_shape, _) = KeyShape::new_single(4, 12, 12);
    let cr = ControlRegion::new_empty(&key_shape);
    let mut bytes = BytesMut::new();
    cr.write_into_bytes(&mut bytes);
    assert_eq!(cr.len(), bytes.len());
    let crc_frame = CrcFrame::new(&cr);
    assert_eq!(crc_frame.len_with_header(), key_shape.cr_len());
}
