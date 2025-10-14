#[cfg(test)]
mod tests {
    use crate::{Config, F2Config, F2Store, FasterKv};
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    // Test data structures
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    struct TestKey {
        id: u64,
        category: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    struct TestValue {
        data: Vec<u8>,
        timestamp: u64,
        metadata: String,
    }

    // Helper function to create test config with temp directory
    fn test_config() -> (Config, TempDir) {
        let dir = TempDir::new().unwrap();
        let config = Config::default().with_path(dir.path().join("faster"));
        (config, dir)
    }

    fn test_f2_config() -> (F2Config, TempDir) {
        let dir = TempDir::new().unwrap();
        let config = F2Config::default().with_path(dir.path().join("f2"));
        (config, dir)
    }

    #[test]
    fn test_faster_basic_operations() {
        let (config, _dir) = test_config();
        let store = FasterKv::new(config).unwrap();

        // Test binary key-value (using Vec<u8> to avoid lifetime issues)
        let key1 = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        store.insert(key1.clone(), value1.clone()).unwrap();
        assert_eq!(
            store.get::<Vec<u8>, Vec<u8>>(key1.clone()).unwrap(),
            Some(value1)
        );

        // Test update
        store.upsert(key1.clone(), value2.clone()).unwrap();
        assert_eq!(
            store.get::<Vec<u8>, Vec<u8>>(key1.clone()).unwrap(),
            Some(value2)
        );

        // Test delete
        store.delete(key1.clone()).unwrap();
        assert_eq!(store.get::<Vec<u8>, Vec<u8>>(key1).unwrap(), None);
    }

    #[test]
    fn test_faster_complex_types() {
        let (config, _dir) = test_config();
        let store = FasterKv::new(config).unwrap();

        let key = TestKey {
            id: 42,
            category: "test".to_string(),
        };

        let value = TestValue {
            data: vec![1, 2, 3, 4, 5],
            timestamp: 123456789,
            metadata: "test metadata".to_string(),
        };

        // Insert and retrieve (clone to pass ownership)
        store.insert(key.clone(), value.clone()).unwrap();
        let retrieved = store.get::<TestKey, TestValue>(key.clone()).unwrap();
        assert_eq!(retrieved, Some(value));

        // Delete
        store.delete(key.clone()).unwrap();
        let retrieved = store.get::<TestKey, TestValue>(key).unwrap();
        assert_eq!(retrieved, None);
    }

    #[test]
    fn test_faster_binary_data() {
        let (config, _dir) = test_config();
        let store = FasterKv::new(config).unwrap();

        let key = vec![1u8, 2, 3, 4];
        let value = vec![5u8, 6, 7, 8, 9, 10];

        store.insert(key.clone(), value.clone()).unwrap();
        assert_eq!(store.get::<Vec<u8>, Vec<u8>>(key).unwrap(), Some(value));
    }

    #[test]
    fn test_faster_multiple_operations() {
        let (config, _dir) = test_config();
        let store = FasterKv::new(config).unwrap();

        // Insert multiple items
        for i in 0..10 {
            let key = format!("key_{}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            store.insert(key, value).unwrap();
        }

        // Verify all items
        for i in 0..10 {
            let key = format!("key_{}", i).into_bytes();
            let expected = format!("value_{}", i).into_bytes();
            assert_eq!(store.get::<Vec<u8>, Vec<u8>>(key).unwrap(), Some(expected));
        }
    }

    #[test]
    fn test_f2_basic_operations() {
        let (f2_config, _dir) = test_f2_config();
        let store = F2Store::new(f2_config).unwrap();

        // Test binary key-value
        let key1 = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        store.insert(key1.clone(), value1.clone()).unwrap();
        assert_eq!(
            store.get::<Vec<u8>, Vec<u8>>(key1.clone()).unwrap(),
            Some(value1)
        );

        // Test update
        store.upsert(key1.clone(), value2.clone()).unwrap();
        assert_eq!(
            store.get::<Vec<u8>, Vec<u8>>(key1.clone()).unwrap(),
            Some(value2)
        );
    }

    #[test]
    fn test_f2_complex_types() {
        let (f2_config, _dir) = test_f2_config();
        let store = F2Store::new(f2_config).unwrap();

        let key = TestKey {
            id: 99,
            category: "f2test".to_string(),
        };

        let value = TestValue {
            data: vec![10, 20, 30],
            timestamp: 987654321,
            metadata: "f2 metadata".to_string(),
        };

        // Insert and retrieve
        store.insert(key.clone(), value.clone()).unwrap();
        let retrieved = store.get::<TestKey, TestValue>(key).unwrap();
        assert_eq!(retrieved, Some(value));
    }
}
