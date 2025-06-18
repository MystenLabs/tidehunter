use crate::db::{WalEntry, MAX_KEY_LEN};
use crate::key_shape::KeySpace;
use crate::wal::PreparedWalWrite;
use minibytes::Bytes;

pub type BatchId = u16;

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
        PreparedWrite { ks, key, value }
    }

    pub fn prepare_delete(ks: KeySpace, key: impl Into<Bytes>) -> PreparedDelete {
        let key = key.into();
        assert!(key.len() <= MAX_KEY_LEN, "Key exceeding max key length");
        PreparedDelete { ks, key }
    }

    pub fn is_empty(&self) -> bool {
        self.writes.is_empty() && self.deletes.is_empty()
    }

    pub fn get_wal_write(&self, batch_id: BatchId) -> PreparedWalWrite {
        let count = (self.writes.len() + self.deletes.len()) as u16;
        PreparedWalWrite::new(&WalEntry::BatchStart(batch_id, count))
    }
}

pub struct PreparedWrite {
    pub(crate) ks: KeySpace,
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
}

impl PreparedWrite {
    pub fn get_wal_write(&self, batch_id: BatchId) -> PreparedWalWrite {
        PreparedWalWrite::new(&WalEntry::BatchRecord(
            batch_id,
            self.ks,
            self.key.clone(),
            self.value.clone(),
        ))
    }
}

pub struct PreparedDelete {
    pub(crate) ks: KeySpace,
    pub(crate) key: Bytes,
}

impl PreparedDelete {
    pub fn get_wal_write(&self, batch_id: BatchId) -> PreparedWalWrite {
        PreparedWalWrite::new(&WalEntry::BatchRemove(batch_id, self.ks, self.key.clone()))
    }
}
