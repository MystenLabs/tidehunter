use crate::WalPosition;
use minibytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// PendingTable stores key-value pairs that can be committed at once via Transaction.
///
/// PendingTable::add_pending(key, transaction) can be used to insert pending value into this table.
///
/// Transaction::commit(values) can be then used to commit all pending updates at once.
/// When committing transactions values are given in the same order as add_pending calls.
///
/// Transaction can be re-used multiple times for same or different keys.
/// Transaction can be used across different PendingTables.
///
/// If transaction is dropped without call to Transaction::commit, pending updates are discarded.
/// If transaction is committed via Transaction::Commit(values), corresponding values will be returned via PendingTable::take_committed()
///
/// See pending_table_tests for ane example.
#[derive(Default)]
pub struct PendingTable {
    data: HashMap<Bytes, Vec<PendingUpdate>>,
    len: usize,
}

#[derive(Default)]
pub struct Transaction {
    status: TransactionStatus,
    committed: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub struct CommittedChange {
    pub key: Bytes,
    /// True if change was modification, false if change was deletion
    pub is_modified: bool,
    pub value: WalPosition,
    /// Value bytes for LRU cache updates. None for deletions or if LRU is not configured.
    pub lru_update: Option<Bytes>,
}

#[derive(Clone)]
struct PendingUpdateInner {
    update: WalPosition,
    lru_update: Option<Bytes>,
}

#[derive(Default, Clone)]
struct TransactionStatus {
    status: Arc<AtomicUsize>,
}

const TRANSACTION_STATUS_PENDING: usize = 0;
const TRANSACTION_STATUS_COMMITTED: usize = 1;
const TRANSACTION_STATUS_REVERTED: usize = 2;

#[derive(Clone)]
struct PendingUpdate {
    transaction_status: TransactionStatus,
    is_modified: bool,
    inner: PendingUpdateInner,
}

impl PendingTable {
    pub fn insert(
        &mut self,
        key: Bytes,
        position: WalPosition,
        lru_update: Option<Bytes>,
        transaction: &mut Transaction,
    ) {
        self.insert_pending(key, true, position, lru_update, transaction);
    }

    pub fn remove(&mut self, key: Bytes, position: WalPosition, transaction: &mut Transaction) {
        self.insert_pending(key, false, position, None, transaction);
    }

    fn insert_pending(
        &mut self,
        key: Bytes,
        is_modified: bool,
        position: WalPosition,
        lru_update: Option<Bytes>,
        transaction: &mut Transaction,
    ) {
        self.len += 1;
        let update = PendingUpdate::new(
            transaction.status.clone(),
            is_modified,
            position,
            lru_update,
        );
        self.data.entry(key).or_default().push(update);
    }

    pub fn take_committed_for(&mut self, key: &Bytes) -> (Vec<CommittedChange>, usize) {
        let Some(updates) = self.data.get_mut(key) else {
            return (vec![], 0);
        };
        let mut committed = Vec::with_capacity(updates.len());
        let removed = Self::do_take_committed(key, updates, &mut committed);
        self.len = self.len.checked_sub(removed).expect("len overflow");
        if updates.is_empty() {
            self.data.remove(key);
        }
        (committed, removed)
    }

    pub fn take_committed(&mut self) -> (Vec<CommittedChange>, usize) {
        let mut committed = Vec::with_capacity(self.len);
        let mut total_removed = 0;
        self.data.retain(|key, updates| {
            let removed = Self::do_take_committed(key, updates, &mut committed);
            self.len = self.len.checked_sub(removed).expect("len overflow");
            total_removed += removed;
            !updates.is_empty()
        });
        (committed, total_removed)
    }

    /// Take committed updates for a given key, add them to given vector
    /// and return number of entries removed.
    /// Number of removed entries and number of added updates may differ because
    /// some transactions might be reverted.
    fn do_take_committed(
        key: &Bytes,
        updates: &mut Vec<PendingUpdate>,
        committed: &mut Vec<CommittedChange>,
    ) -> usize {
        let mut removed = 0;
        updates.retain(|update| {
            let status = update.transaction_status.status.load(Ordering::SeqCst);
            if status == TRANSACTION_STATUS_PENDING {
                return true;
            }
            if status == TRANSACTION_STATUS_COMMITTED {
                let committed_change = CommittedChange {
                    key: key.clone(),
                    is_modified: update.is_modified,
                    value: update.inner.update,
                    lru_update: update.inner.lru_update.clone(),
                };
                committed.push(committed_change);
            } else {
                assert_eq!(
                    status, TRANSACTION_STATUS_REVERTED,
                    "Expected reverted transaction"
                );
            }
            removed += 1;
            false
        });
        removed
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

impl Transaction {
    pub fn commit(mut self) {
        self.status
            .status
            .store(TRANSACTION_STATUS_COMMITTED, Ordering::SeqCst);
        self.committed = true;
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.committed {
            self.status
                .status
                .store(TRANSACTION_STATUS_REVERTED, Ordering::SeqCst);
        }
    }
}

impl PendingUpdate {
    fn new(
        transaction_status: TransactionStatus,
        is_modified: bool,
        position: WalPosition,
        lru_update: Option<Bytes>,
    ) -> Self {
        let inner = PendingUpdateInner {
            update: position,
            lru_update,
        };
        Self {
            transaction_status,
            is_modified,
            inner,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use rand::prelude::SliceRandom;
    use rand::{Rng, thread_rng};
    use std::collections::{BTreeMap, HashSet};
    use std::sync::mpsc;
    use std::thread;

    #[test]
    fn pending_table_tests() {
        let mut table = PendingTable::default();
        let mut tx1 = Transaction::default();
        let mut tx2 = Transaction::default();

        table.insert(b(1), WalPosition::test_value(0), None, &mut tx1);
        table.insert(b(1), WalPosition::test_value(1), None, &mut tx2);
        table.insert(b(2), WalPosition::test_value(2), None, &mut tx2);
        let (committed, removed) = table.take_committed();
        assert_eq!(committed.len(), 0);
        assert_eq!(removed, 0);
        assert_eq!(table.len(), 3);
        drop(tx1); // drop tx1 without committing it
        // Nothing is committed but table len is reduced by 1 since tx1 update is discarded now
        let (committed, removed) = table.take_committed();
        assert_eq!(committed.len(), 0);
        assert_eq!(removed, 1);
        assert_eq!(table.len(), 2);

        tx2.commit(); // Commit tx2

        let (mut committed, removed) = table.take_committed();
        // 2 values are committed and table is now empty(no more pending update).
        assert_eq!(committed.len(), 2);
        assert_eq!(removed, 2);
        committed.sort_by_key(|c| c.key.clone());
        assert_eq!(table.len(), 0);
        assert_eq!(
            &committed[0],
            &CommittedChange {
                key: b(1),
                is_modified: true,
                value: WalPosition::test_value(1),
                lru_update: None,
            }
        );

        assert_eq!(
            &committed[1],
            &CommittedChange {
                key: b(2),
                is_modified: true,
                value: WalPosition::test_value(2),
                lru_update: None,
            }
        );

        // test take_committed_for
        let mut tx3 = Transaction::default();
        table.insert(b(3), WalPosition::test_value(3), None, &mut tx3);
        table.insert(b(4), WalPosition::test_value(4), None, &mut tx3);
        tx3.commit();
        let (committed, removed) = table.take_committed_for(&b(3));
        assert_eq!(committed.len(), 1);
        assert_eq!(removed, 1);
        assert_eq!(table.data.len(), 1);
        assert_eq!(table.len(), 1);
        assert_eq!(
            &committed[0],
            &CommittedChange {
                key: b(3),
                is_modified: true,
                value: WalPosition::test_value(3),
                lru_update: None,
            }
        );
        let (committed, removed) = table.take_committed();
        assert_eq!(committed.len(), 1);
        assert_eq!(removed, 1);
        assert_eq!(table.data.len(), 0);
        assert_eq!(table.len(), 0);
        assert_eq!(
            &committed[0],
            &CommittedChange {
                key: b(4),
                is_modified: true,
                value: WalPosition::test_value(4),
                lru_update: None,
            }
        );
    }

    #[test]
    fn pending_table_concurrent_test() {
        let num_tables = 32;
        let num_mutators = 16;
        let iterations = 5000;
        let tables: Vec<_> = (0..num_tables)
            .map(|_| Arc::new(Mutex::new(PendingTable::default())))
            .collect();
        let (senders, receivers): (Vec<_>, Vec<_>) =
            tables.iter().map(|_| mpsc::channel::<Vec<u64>>()).unzip();
        let threads: Vec<_> = tables
            .clone()
            .into_iter()
            .zip(receivers.into_iter())
            .enumerate()
            .map(|(id, (table, receiver))| {
                thread::spawn(move || {
                    let mut committed = HashSet::new();
                    while let Ok(expected_to_commit) = receiver.recv() {
                        let (changes, _removed) = table.lock().take_committed();
                        for c in changes {
                            println!("[{id}] committed {}", c.value.offset());
                            committed.insert(c.value.offset());
                        }
                        for num in expected_to_commit.into_iter() {
                            if !committed.remove(&num) {
                                println!("[{id}] Expected {num} to be committed but it is not");
                                std::process::exit(1);
                            };
                        }
                    }
                    if !committed.is_empty() {
                        panic!(
                            "[{id}] something is committed but should not be: {:?}",
                            committed
                        );
                    }
                })
            })
            .collect();
        let max_transactions = 32;
        let senders = Arc::new(senders);
        let tables = Arc::new(tables);
        let mut mutator_threads = vec![];
        for _ in 0..num_mutators {
            let senders = Arc::clone(&senders);
            let tables = Arc::clone(&tables);
            let thread = thread::spawn(move || {
                let mut transactions: Vec<(Vec<u64>, Transaction)> = vec![];
                let mut rng = thread_rng();
                for _ in 0..iterations {
                    let (transaction_changes, transaction) = if rng.gen_bool(
                        (max_transactions - transactions.len()) as f64 / (max_transactions as f64),
                    ) {
                        println!("new txn(total {})", transactions.len());
                        transactions.push(Default::default());
                        transactions.last_mut().unwrap()
                    } else {
                        println!("existing txn");
                        transactions.choose_mut(&mut rng).unwrap()
                    };
                    let num: u64 = rng.r#gen();
                    transaction_changes.push(num);
                    let table = tables.get(num as usize % tables.len()).unwrap();
                    table.lock().insert(
                        Bytes::from(num.to_be_bytes()),
                        WalPosition::test_value(num),
                        None,
                        transaction,
                    );

                    let transaction_to_commit = rng.gen_range(0..(transactions.len() * 4));
                    if transaction_to_commit >= transactions.len() {
                        continue;
                    }
                    println!("Committing random txn");
                    let (nums_to_commit, transaction_to_commit) =
                        transactions.remove(transaction_to_commit);
                    let mut tables_to_commit: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
                    for num_to_commit in nums_to_commit.iter() {
                        tables_to_commit
                            .entry((*num_to_commit as usize) % tables.len())
                            .or_default()
                            .push(*num_to_commit);
                    }
                    println!("Committing");
                    transaction_to_commit.commit();
                    for (table_id, message) in tables_to_commit {
                        println!("Sending to {table_id}: {message:?}");
                        senders[table_id].send(message).unwrap();
                    }
                }
            });
            mutator_threads.push(thread);
        }
        for thread in mutator_threads {
            thread.join().unwrap();
        }
        drop(senders);
        println!("Waiting for threads");
        for thread in threads {
            thread.join().unwrap();
        }
    }

    fn b(v: u64) -> Bytes {
        v.to_be_bytes().to_vec().into()
    }
}
