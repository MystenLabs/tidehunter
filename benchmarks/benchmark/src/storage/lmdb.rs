use crate::storage::Storage;
use lmdb::{Cursor, Database, Environment, EnvironmentFlags, Transaction, WriteFlags};
use lmdb_sys::{MDB_CURRENT, MDB_LAST, MDB_PREV, MDB_SET_RANGE};
use minibytes::Bytes;
use std::path::Path;
use std::sync::Arc;

pub struct LmdbStorage {
    env: Environment,
    db: Database,
}

impl LmdbStorage {
    pub fn open(path: &Path) -> Arc<Self> {
        std::fs::create_dir_all(path).unwrap();

        let env = Environment::new()
            .set_flags(
                EnvironmentFlags::WRITE_MAP
                    | EnvironmentFlags::MAP_ASYNC
                    | EnvironmentFlags::NO_READAHEAD,
            )
            .set_max_readers(126)
            .set_max_dbs(1)
            .set_map_size(4 * 1024 * 1024 * 1024 * 1024)
            .open(path)
            .unwrap();

        let db = env.open_db(None).unwrap();

        Arc::new(Self { env, db })
    }
}

impl Storage for LmdbStorage {
    fn insert(&self, k: Bytes, v: Bytes) {
        let mut txn = self.env.begin_rw_txn().unwrap();
        txn.put(self.db, &k, &v, WriteFlags::empty()).unwrap();
        txn.commit().unwrap();
    }

    fn get(&self, k: &[u8]) -> Option<Bytes> {
        let txn = self.env.begin_ro_txn().unwrap();
        txn.get(self.db, &k)
            .ok()
            .map(|data| Bytes::from(data.to_vec()))
    }

    fn get_lt(&self, k: &[u8], iterations: usize) -> Vec<Bytes> {
        let txn = self.env.begin_ro_txn().unwrap();
        let cursor = txn.open_ro_cursor(self.db).unwrap();

        let mut result = Vec::with_capacity(iterations);

        // Position cursor at first key >= k
        let start_from = cursor.get(Some(k), None, MDB_SET_RANGE);
        if start_from.is_err() {
            // If no key >= k exists, position at last key
            let _ = cursor.get(None, None, MDB_LAST);
        } else {
            // Move to previous entry (less than k)
            let _ = cursor.get(None, None, MDB_PREV);
        }

        // Collect up to 'iterations' entries going backwards
        for _ in 0..iterations {
            match cursor.get(None, None, MDB_CURRENT) {
                Ok((_, value)) => {
                    result.push(Bytes::from(value.to_vec()));
                    if cursor.get(None, None, MDB_PREV).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }

        result
    }

    fn exists(&self, k: &[u8]) -> bool {
        let txn = self.env.begin_ro_txn().unwrap();
        txn.get(self.db, &k).is_ok()
    }

    fn name(&self) -> &'static str {
        "lmdb"
    }
}
