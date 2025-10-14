use std::ffi::CString;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::{FasterError, Result};
use crate::traits::{StoreKey, StoreValue};

/// Global serial number counter for monotonic serial numbers
static SERIAL_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Get next monotonic serial number
fn next_serial() -> u64 {
    SERIAL_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Configuration for FASTER store
#[derive(Debug, Clone)]
pub struct Config {
    pub storage_path: String,
    pub initial_log_size: u64,
    pub max_log_size: u64,
    pub page_size: u64,
    pub segment_size: u64,
    pub hash_table_size: u64,
    pub enable_read_cache: bool,
    pub read_cache_size: u64,
    pub log_mutable_fraction: f64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            storage_path: "/tmp/faster".to_string(),
            initial_log_size: 1 << 30, // 1GB
            max_log_size: 1 << 40,     // 1TB
            page_size: 1 << 21,        // 2MB
            segment_size: 1 << 30,     // 1GB
            hash_table_size: 1 << 20,  // 1M buckets
            enable_read_cache: true,
            read_cache_size: 1 << 30, // 1GB
            log_mutable_fraction: 0.9,
        }
    }
}

impl Config {
    /// Create configuration optimized for 1TB dataset with 256GB RAM
    pub fn for_1tb_dataset() -> Self {
        Config {
            initial_log_size: 1 << 36, // 64GB
            max_log_size: 1 << 40,     // 1TB
            segment_size: 1 << 32,     // 4GB
            read_cache_size: 1 << 36,  // 64GB
            hash_table_size: 1 << 24,  // 16M buckets
            ..Default::default()
        }
    }

    /// Set the storage path
    pub fn with_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.storage_path = path.as_ref().to_string_lossy().to_string();
        self
    }
}

/// FASTER key-value store
pub struct FasterKv {
    inner: Arc<Inner>,
}

struct Inner {
    store: *mut faster_sys::faster_t,
}

// Safety: FASTER handles thread safety internally with sessions
unsafe impl Send for Inner {}
unsafe impl Sync for Inner {}

impl FasterKv {
    /// Create a new FASTER store with the given configuration
    pub fn new(config: Config) -> Result<Self> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        return Err(FasterError::PlatformNotSupported);

        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            // Convert path to C string
            let c_path = CString::new(config.storage_path.as_bytes())
                .map_err(|_| FasterError::InvalidPath)?;

            // Create FFI config
            let ffi_config = faster_sys::faster_config_t {
                storage_path: c_path.as_ptr(),
                initial_log_size: config.initial_log_size,
                max_log_size: config.max_log_size,
                page_size: config.page_size,
                segment_size: config.segment_size,
                hash_table_size: config.hash_table_size,
                enable_read_cache: config.enable_read_cache,
                read_cache_size: config.read_cache_size,
                log_mutable_fraction: config.log_mutable_fraction,
            };

            // Create the store
            let store = unsafe { faster_sys::faster_create(&ffi_config) };

            if store.is_null() {
                return Err(FasterError::InitializationFailed);
            }

            Ok(FasterKv {
                inner: Arc::new(Inner { store }),
            })
        }
    }

    /// Insert or update a key-value pair
    pub fn upsert<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: StoreKey,
        V: StoreValue,
    {
        let key_bytes = key.to_bytes()?;
        let value_bytes = value.to_bytes()?;
        let serial = next_serial();

        let status = unsafe {
            faster_sys::faster_upsert(
                self.inner.store,
                key_bytes.as_ptr() as *const _,
                key_bytes.len() as u64,
                value_bytes.as_ptr() as *const _,
                value_bytes.len() as u64,
                serial,
            )
        };

        // Complete pending operations for writes
        unsafe {
            faster_sys::faster_complete_pending(self.inner.store, false);
        }

        if status == faster_sys::faster_status::FASTER_SUCCESS {
            Ok(())
        } else {
            Err(FasterError::from_status(status))
        }
    }

    /// Insert a new key-value pair (alias for upsert)
    pub fn insert<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: StoreKey,
        V: StoreValue,
    {
        self.upsert(key, value)
    }

    /// Read a value by key
    pub fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: StoreKey,
        V: StoreValue,
    {
        let key_bytes = key.to_bytes()?;
        let serial = next_serial();

        let mut value_ptr: *mut std::ffi::c_void = std::ptr::null_mut();
        let mut value_len: u64 = 0;

        let status = unsafe {
            faster_sys::faster_read(
                self.inner.store,
                key_bytes.as_ptr() as *const _,
                key_bytes.len() as u64,
                &mut value_ptr,
                &mut value_len,
                serial,
            )
        };

        // Complete pending operations for reads (blocking)
        unsafe {
            faster_sys::faster_complete_pending(self.inner.store, true);
        }

        match status {
            faster_sys::faster_status::FASTER_SUCCESS => {
                if value_ptr.is_null() || value_len == 0 {
                    return Ok(None);
                }

                // Copy data from C allocation
                let value_bytes = unsafe {
                    std::slice::from_raw_parts(value_ptr as *const u8, value_len as usize)
                };

                // Deserialize the value
                let value = V::from_bytes(value_bytes)?;

                // Free the C allocation
                unsafe {
                    faster_sys::faster_free_value(value_ptr);
                }

                Ok(Some(value))
            }
            faster_sys::faster_status::FASTER_NOT_FOUND => Ok(None),
            _ => Err(FasterError::from_status(status)),
        }
    }

    /// Delete a key-value pair
    pub fn delete<K>(&self, key: K) -> Result<()>
    where
        K: StoreKey,
    {
        let key_bytes = key.to_bytes()?;
        let serial = next_serial();

        let status = unsafe {
            faster_sys::faster_delete(
                self.inner.store,
                key_bytes.as_ptr() as *const _,
                key_bytes.len() as u64,
                serial,
            )
        };

        // Complete pending operations
        unsafe {
            faster_sys::faster_complete_pending(self.inner.store, false);
        }

        if status == faster_sys::faster_status::FASTER_SUCCESS {
            Ok(())
        } else {
            Err(FasterError::from_status(status))
        }
    }

    /// Check if a key exists
    pub fn exists<K>(&self, key: K) -> bool
    where
        K: StoreKey,
    {
        self.get::<K, Vec<u8>>(key).unwrap_or(None).is_some()
    }

    /// Get the size of the store (number of records)
    pub fn size(&self) -> u64 {
        unsafe { faster_sys::faster_size(self.inner.store) }
    }

    /// Grow the index (rehash)
    pub fn grow_index(&self) -> bool {
        unsafe { faster_sys::faster_grow_index(self.inner.store) }
    }

    /// Refresh the current session
    pub fn refresh(&self) {
        unsafe { faster_sys::faster_refresh_session(self.inner.store) }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        unsafe {
            if !self.store.is_null() {
                faster_sys::faster_destroy(self.store);
            }
        }
    }
}

impl Clone for FasterKv {
    fn clone(&self) -> Self {
        FasterKv {
            inner: Arc::clone(&self.inner),
        }
    }
}
