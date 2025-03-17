use crate::db::{WalEntry, MAX_KEY_LEN};
use crate::key_shape::KeySpace;
use crate::wal::PreparedWalWrite;
use minibytes::Bytes;

pub struct WriteBatch {
    pub(crate) writes: Vec<PreparedWrite>,
    pub(crate) deletes: Vec<PreparedDelete>,
}

impl WriteBatch {
    pub fn new() -> Self {
        WriteBatch {
            writes: Default::default(),
            deletes: Default::default(),
        }
    }

    pub fn write(&mut self, ks: KeySpace, k: impl Into<Bytes>, v: impl Into<Bytes>) {
        self.write_prepared(Self::prepare_write(ks, k, v))
    }

    pub fn delete(&mut self, ks: KeySpace, k: impl Into<Bytes>) {
        self.delete_prepared(Self::prepare_delete(ks, k));
    }

    pub fn write_prepared(&mut self, w: PreparedWrite) {
        self.writes.push(w)
    }

    pub fn delete_prepared(&mut self, w: PreparedDelete) {
        self.deletes.push(w)
    }

    pub fn prepare_write(
        ks: KeySpace,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> PreparedWrite {
        let key = key.into();
        let value = value.into();
        assert!(key.len() <= MAX_KEY_LEN, "Key exceeding max key length");
        let wal_write = PreparedWalWrite::new(&WalEntry::Record(ks, key.clone(), value.clone()));
        PreparedWrite {
            ks,
            key,
            value,
            wal_write,
        }
    }

    pub fn prepare_delete(ks: KeySpace, key: impl Into<Bytes>) -> PreparedDelete {
        let key = key.into();
        assert!(key.len() <= MAX_KEY_LEN, "Key exceeding max key length");
        let wal_write = PreparedWalWrite::new(&WalEntry::Remove(ks, key.clone()));
        PreparedDelete { ks, key, wal_write }
    }
}

pub struct PreparedWrite {
    pub(crate) ks: KeySpace,
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
    pub(crate) wal_write: PreparedWalWrite,
}

pub struct PreparedDelete {
    pub(crate) ks: KeySpace,
    pub(crate) key: Bytes,
    pub(crate) wal_write: PreparedWalWrite,
}
