use crate::cell::CellId;
use crate::db::{MAX_KEY_LEN, WalEntry};
use crate::key_shape::{KeySpace, KeySpaceDesc};
use crate::wal::PreparedWalWrite;
use minibytes::Bytes;

pub struct WriteBatch {
    pub(crate) updates: Vec<Update>,
    pub(crate) prepared_writes: Vec<PreparedWalWrite>,
    pub(crate) tag: String,
}

pub(crate) struct RelocatedWriteBatch {
    pub(crate) prepared_writes: Vec<PreparedWalWrite>,
    pub(crate) keys: Vec<Bytes>,
    pub(crate) last_processed: u64,
    pub(crate) ks: KeySpace,
    pub(crate) cell_id: CellId,
}

const MAX_BATCH_LEN: usize = 1_000_000;

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteBatch {
    pub fn new() -> Self {
        WriteBatch {
            updates: Default::default(),
            prepared_writes: Default::default(),
            tag: Default::default(),
        }
    }

    pub fn set_tag(&mut self, tag: String) {
        self.tag = tag;
    }

    pub fn write(&mut self, ks: KeySpace, k: impl Into<Bytes>, v: impl Into<Bytes>) {
        self.prepare_write(Update::Record(ks, k.into(), v.into()));
    }

    pub fn delete(&mut self, ks: KeySpace, k: impl Into<Bytes>) {
        self.prepare_write(Update::Remove(ks, k.into()));
    }

    pub fn prepare_write(&mut self, update: Update) {
        let (wal_write, key) = match update {
            Update::Record(ks, ref key, ref value) => (
                PreparedWalWrite::new(&WalEntry::Record(ks, key.clone(), value.clone(), false)),
                key,
            ),
            Update::Remove(ks, ref key) => (
                PreparedWalWrite::new(&WalEntry::Remove(ks, key.clone())),
                key,
            ),
        };
        assert!(key.len() <= MAX_KEY_LEN, "Key exceeding max key length");
        self.prepared_writes.push(wal_write);
        self.updates.push(update);
        assert!(
            self.updates.len() < MAX_BATCH_LEN,
            "Batch exceeds max length {MAX_BATCH_LEN}"
        );
    }
}

impl RelocatedWriteBatch {
    pub fn new(ks: KeySpace, cell_id: CellId, last_processed: u64) -> Self {
        Self {
            last_processed,
            keys: Default::default(),
            prepared_writes: Default::default(),
            ks,
            cell_id,
        }
    }

    pub fn write(&mut self, key: Bytes, value: Bytes) {
        let write =
            PreparedWalWrite::new(&WalEntry::Record(self.ks, key.clone(), value.clone(), true));
        self.prepared_writes.push(write);
        self.keys.push(key);
    }

    pub fn is_empty(&self) -> bool {
        self.prepared_writes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.prepared_writes.len()
    }
}

pub enum Update {
    Record(KeySpace, Bytes, Bytes),
    Remove(KeySpace, Bytes),
}

impl Update {
    pub fn ks(&self) -> KeySpace {
        match self {
            Update::Record(ks, _, _) => *ks,
            Update::Remove(ks, _) => *ks,
        }
    }

    pub fn reduced_key(&self, ks: &KeySpaceDesc) -> Bytes {
        let key = match self {
            Update::Record(_, key, _) => key,
            Update::Remove(_, key) => key,
        };
        ks.check_key(key);
        ks.reduced_key_bytes(key.clone())
    }
}
