use serde::{de::DeserializeOwned, Serialize};

/// Trait for types that can be used as keys in FASTER/F2 stores
pub trait StoreKey: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Convert the key to bytes for storage
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    /// Create a key from bytes
    fn from_bytes(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }
}

/// Trait for types that can be used as values in FASTER/F2 stores
pub trait StoreValue: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Convert the value to bytes for storage
    fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    /// Create a value from bytes
    fn from_bytes(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }
}

// Blanket implementations for all types that meet the requirements
impl<T> StoreKey for T where T: Serialize + DeserializeOwned + Send + Sync + 'static {}
impl<T> StoreValue for T where T: Serialize + DeserializeOwned + Send + Sync + 'static {}
