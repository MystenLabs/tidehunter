use crate::storage::Storage;
use fjall::{Config, Keyspace, PartitionCreateOptions};
use minibytes::Bytes;
use std::path::Path;
use std::sync::Arc;

pub struct FjallStorage {
    keyspace: Keyspace,
    partition: fjall::PartitionHandle,
}

impl FjallStorage {
    pub fn open(path: &Path) -> Arc<Self> {
        std::fs::create_dir_all(path).unwrap();

        // Configure Fjall with default settings
        // Fjall will automatically handle key-value separation for large values
        let config = Config::new(path);

        // Open the keyspace
        let keyspace = config.open().unwrap();

        // Create a single partition for all key-value pairs
        let partition = keyspace
            .open_partition("benchmark", PartitionCreateOptions::default())
            .unwrap();

        Arc::new(Self {
            keyspace,
            partition,
        })
    }
}

impl Storage for FjallStorage {
    fn insert(&self, k: Bytes, v: Bytes) {
        self.partition.insert(k.as_ref(), v.as_ref()).unwrap();

        // Fjall uses write buffering, but we don't need to explicitly flush
        // as it handles this automatically based on buffer size
    }

    fn get(&self, k: &[u8]) -> Option<Bytes> {
        self.partition
            .get(k)
            .unwrap()
            .map(|data| Bytes::from(data.to_vec()))
    }

    fn get_lt(&self, k: &[u8], iterations: usize) -> Vec<Bytes> {
        let mut result = Vec::with_capacity(iterations);

        // Create a reverse iterator starting from the key
        let range = ..k;
        let iter = self.partition.range(range).rev();

        // Collect up to 'iterations' entries
        for item in iter.take(iterations) {
            if let Ok((_key, value)) = item {
                result.push(Bytes::from(value.to_vec()));
            }
        }

        result
    }

    fn exists(&self, k: &[u8]) -> bool {
        self.partition.contains_key(k).unwrap()
    }

    fn name(&self) -> &'static str {
        "fjall"
    }
}

// Ensure proper cleanup when dropping
impl Drop for FjallStorage {
    fn drop(&mut self) {
        // Fjall handles cleanup automatically when the keyspace is dropped
        // Persist takes a mode parameter
        let _ = self.keyspace.persist(fjall::PersistMode::Buffer);
    }
}