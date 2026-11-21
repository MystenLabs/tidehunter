use crate::cell::CellId;
use crate::db::{Db, DbResult, WalEntry};
use crate::index::pending_table::Transaction;
use crate::key_shape::KeySpace;
use crate::wal::PreparedWalWrite;
use crate::wal::position::WalPosition;
use minibytes::Bytes;
use std::sync::Arc;

pub struct WriteBatch {
    pub(crate) transaction: Transaction,
    db: Arc<Db>,
    /// `WalEntry::Record`/`WalEntry::Remove` collected here; the framing — one
    /// frame per entry vs one compressed frame for the batch — is decided at
    /// commit time in `Db::do_write_batch`.
    pub(crate) writes: Vec<WalEntry>,
    pub(crate) tag: String,
    /// Operations to be applied to pending table on commit
    pub(crate) pending_ops: Vec<PendingOp>,
}

pub(crate) enum PendingOp {
    Insert {
        ks: KeySpace,
        reduced_key: Bytes,
        cell_id: CellId,
        /// `(full_key, value)` for LRU updates. None if LRU is not configured.
        lru_update: Option<(Bytes, Bytes)>,
    },
    Remove {
        ks: KeySpace,
        reduced_key: Bytes,
        cell_id: CellId,
    },
}

impl PendingOp {
    pub(crate) fn ks(&self) -> KeySpace {
        match self {
            PendingOp::Insert { ks, .. } | PendingOp::Remove { ks, .. } => *ks,
        }
    }

    pub(crate) fn cell_id(&self) -> &CellId {
        match self {
            PendingOp::Insert { cell_id, .. } | PendingOp::Remove { cell_id, .. } => cell_id,
        }
    }

    pub(crate) fn reduced_key(&self) -> &Bytes {
        match self {
            PendingOp::Insert { reduced_key, .. } | PendingOp::Remove { reduced_key, .. } => {
                reduced_key
            }
        }
    }
}

pub(crate) struct RelocatedWriteBatch {
    pub(crate) prepared_writes: Vec<PreparedWalWrite>,
    pub(crate) keys: Vec<Bytes>,
    pub(crate) size_bytes: usize,
    pub(crate) last_processed: u64,
    pub(crate) ks: KeySpace,
    pub(crate) cell_id: CellId,
    /// The WAL generation the copies are written to: 0 for the main WAL,
    /// otherwise the side WAL records are promoted to. Chosen by the pass
    /// through `Db::relocation_target_generation`.
    pub(crate) target_generation: u8,
    /// `Some` for a batch of copies of side-WAL records (side-WAL
    /// reclamation): the position each copy was made from, parallel to
    /// `keys`. Such a copy replaces an index entry only while the entry
    /// still holds that exact position. `None` for copies of main-WAL
    /// records, which are identified by `last_processed`.
    pub(crate) sources: Option<Vec<WalPosition>>,
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
        let cell_id = context.ks_config.cell_id(&reduced_key);
        // Pass (full_key, value) for LRU cache if enabled
        let lru_update = context
            .ks_config
            .value_cache_size()
            .map(|_| (k.clone(), v.clone()));

        // Store operation to be applied on commit
        self.pending_ops.push(PendingOp::Insert {
            ks,
            reduced_key,
            cell_id,
            lru_update,
        });

        // todo transaction state is corrupted on panic
        self.push_write(WalEntry::Record(ks, k, v, false));
    }

    /// Delete a key from the batch.
    pub fn delete(&mut self, ks: KeySpace, k: impl Into<Bytes>) {
        let k = k.into();
        let context = self.db.ks_context(ks);
        context.ks_config.check_key(&k);
        let reduced_key = context.ks_config.reduced_key_bytes(k.clone());
        let cell_id = context.ks_config.cell_id(&reduced_key);

        // Store operation to be applied on commit
        self.pending_ops.push(PendingOp::Remove {
            ks,
            reduced_key,
            cell_id,
        });

        // todo transaction state is corrupted on panic
        self.push_write(WalEntry::Remove(ks, k));
    }

    fn push_write(&mut self, entry: WalEntry) {
        self.writes.push(entry);
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
    pub fn new(ks: KeySpace, cell_id: CellId, last_processed: u64, target_generation: u8) -> Self {
        Self {
            last_processed,
            keys: Default::default(),
            prepared_writes: Default::default(),
            size_bytes: 0,
            ks,
            cell_id,
            target_generation,
            sources: None,
        }
    }

    /// A batch of copies of records that sit in the side WAL of
    /// `generation`, to be written back to the tail of the same side WAL.
    pub fn of_side_wal_records(ks: KeySpace, cell_id: CellId, generation: u8) -> Self {
        assert_ne!(generation, 0, "the main WAL is not a side WAL");
        Self {
            sources: Some(Vec::new()),
            ..Self::new(ks, cell_id, 0, generation)
        }
    }

    /// Adds the copy of a main-WAL record.
    pub fn write(&mut self, key: Bytes, value: Bytes) {
        assert!(
            self.sources.is_none(),
            "a batch of side-WAL records takes the position of each record"
        );
        self.push(key, value);
    }

    /// Adds the copy of the side-WAL record at `source`.
    pub fn write_from(&mut self, key: Bytes, value: Bytes, source: WalPosition) {
        self.sources
            .as_mut()
            .expect("not a batch of side-WAL records")
            .push(source);
        self.push(key, value);
    }

    fn push(&mut self, key: Bytes, value: Bytes) {
        let write =
            PreparedWalWrite::new(&WalEntry::Record(self.ks, key.clone(), value.clone(), true));
        self.size_bytes += write.len();
        self.prepared_writes.push(write);
        self.keys.push(key);
    }

    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    pub fn is_empty(&self) -> bool {
        self.prepared_writes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.prepared_writes.len()
    }
}
