use crate::cell::CellId;
use crate::db::{Db, DbResult, WalEntry};
use crate::index::pending_table::Transaction;
use crate::key_shape::KeySpace;
use crate::wal::PreparedWalWrite;
use minibytes::Bytes;
use std::sync::Arc;

pub struct WriteBatch {
    pub(crate) transaction: Transaction,
    db: Arc<Db>,
    pub(crate) writes: Vec<WriteBatchWrite>,
    pub(crate) tag: String,
    /// Operations to be applied to pending table on commit
    pub(crate) pending_ops: Vec<PendingOp>,
}

pub(crate) struct WriteBatchWrite {
    pub prepared_write: PreparedWalWrite,
    pub is_modified: bool,
    pub ks: KeySpace,
}

pub(crate) enum PendingOp {
    Insert {
        ks: KeySpace,
        reduced_key: Bytes,
        lru_update: Option<Bytes>,
    },
    Remove {
        ks: KeySpace,
        reduced_key: Bytes,
    },
}

pub(crate) struct RelocatedWriteBatch {
    pub(crate) prepared_writes: Vec<PreparedWalWrite>,
    pub(crate) keys: Vec<Bytes>,
    pub(crate) last_processed: u64,
    pub(crate) ks: KeySpace,
    pub(crate) cell_id: CellId,
}

const MAX_BATCH_LEN: usize = 1_000_000;

impl WriteBatch {
    pub fn new(db: Arc<Db>) -> Self {
        WriteBatch {
            db,
            transaction: Default::default(),
            writes: Default::default(),
            tag: Default::default(),
            pending_ops: Default::default(),
        }
    }

    pub fn set_tag(&mut self, tag: String) {
        self.tag = tag;
    }

    /// Write a key-value pair to the batch.
    pub fn write(&mut self, ks: KeySpace, k: impl Into<Bytes>, v: impl Into<Bytes>) {
        let k = k.into();
        let v = v.into();
        let context = self.db.ks_context(ks);
        context.ks_config.check_key(&k);
        let reduced_key = context.ks_config.reduced_key_bytes(k.clone());
        // Pass value for LRU cache if enabled
        let lru_update = context.ks_config.value_cache_size().map(|_| v.clone());

        // Store operation to be applied on commit
        self.pending_ops.push(PendingOp::Insert {
            ks,
            reduced_key,
            lru_update,
        });

        // todo transaction state is corrupted on panic
        self.prepare_write(WalEntry::Record(ks, k, v, false));
    }

    /// Delete a key from the batch.
    pub fn delete(&mut self, ks: KeySpace, k: impl Into<Bytes>) {
        let k = k.into();
        let context = self.db.ks_context(ks);
        context.ks_config.check_key(&k);
        let reduced_key = context.ks_config.reduced_key_bytes(k.clone());

        // Store operation to be applied on commit
        self.pending_ops.push(PendingOp::Remove { ks, reduced_key });

        // todo transaction state is corrupted on panic
        self.prepare_write(WalEntry::Remove(ks, k));
    }

    fn prepare_write(&mut self, wal_entry: WalEntry) {
        let (prepared_write, ks, is_modified) = match &wal_entry {
            WalEntry::Record(ks, _key, _value, _relocated) => {
                (PreparedWalWrite::new(&wal_entry), *ks, true)
            }
            WalEntry::Remove(ks, _key) => (PreparedWalWrite::new(&wal_entry), *ks, false),
            _ => unreachable!("WalEntry::Record should be remove or record"),
        };
        self.writes.push(WriteBatchWrite {
            is_modified,
            ks,
            prepared_write,
        });
        assert!(
            self.writes.len() < MAX_BATCH_LEN,
            "Batch exceeds max length {MAX_BATCH_LEN}"
        );
    }

    pub fn commit(self) -> DbResult<()> {
        self.db.clone().do_write_batch(self)
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
