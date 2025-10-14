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

/// Configuration for F2 store
#[derive(Debug, Clone)]
pub struct F2Config {
    pub storage_path: String,
    pub hot_store_size: u64,
    pub cold_store_size: u64,
    pub index_size: u64,
    pub read_cache_size: u64,
    pub hot_threshold: f64,
    pub cold_threshold: f64,
    pub segment_size: u64,
}

impl Default for F2Config {
    fn default() -> Self {
        F2Config {
            storage_path: "/tmp/f2".to_string(),
            hot_store_size: 1 << 34,  // 16GB
            cold_store_size: 1 << 38, // 256GB
            index_size: 1 << 20,      // 1M buckets
            read_cache_size: 1 << 32, // 4GB
            hot_threshold: 0.8,       // Move to cold when 80% full
            cold_threshold: 0.1,      // Promote to hot at 10% access rate
            segment_size: 1 << 30,    // 1GB
        }
    }
}

impl F2Config {
    /// Create configuration optimized for 1TB dataset with 256GB RAM
    pub fn for_1tb_dataset() -> Self {
        F2Config {
            hot_store_size: 1 << 37,  // 128GB (50% of RAM)
            cold_store_size: 1 << 40, // 1TB (full dataset)
            index_size: 1 << 24,      // 16M buckets
            read_cache_size: 1 << 36, // 64GB (25% of RAM)
            hot_threshold: 0.8,
            cold_threshold: 0.1,
            segment_size: 1 << 32, // 4GB
            ..Default::default()
        }
    }

    /// Set the storage path
    pub fn with_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.storage_path = path.as_ref().to_string_lossy().to_string();
        self
    }

    /// Set hot and cold thresholds
    pub fn with_thresholds(mut self, hot: f64, cold: f64) -> Self {
        self.hot_threshold = hot;
        self.cold_threshold = cold;
        self
    }
}

/// F2 two-tier key-value store
pub struct F2Store {
    inner: Arc<Inner>,
}

struct Inner {
    store: *mut faster_sys::f2_t,
}

// Safety: F2 handles thread safety internally with sessions
unsafe impl Send for Inner {}
unsafe impl Sync for Inner {}

impl F2Store {
    /// Create a new F2 store with the given configuration
    pub fn new(config: F2Config) -> Result<Self> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        return Err(FasterError::PlatformNotSupported);

        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            // Convert path to C string
            let c_path = CString::new(config.storage_path.as_bytes())
                .map_err(|_| FasterError::InvalidPath)?;

            // Create FFI config
            let ffi_config = faster_sys::f2_config_t {
                storage_path: c_path.as_ptr(),
                hot_store_size: config.hot_store_size,
                cold_store_size: config.cold_store_size,
                index_size: config.index_size,
                read_cache_size: config.read_cache_size,
                hot_threshold: config.hot_threshold,
                cold_threshold: config.cold_threshold,
                segment_size: config.segment_size,
            };

            // Create the store
            let store = unsafe { faster_sys::f2_create(&ffi_config) };

            if store.is_null() {
                return Err(FasterError::InitializationFailed);
            }

            Ok(F2Store {
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
            faster_sys::f2_upsert(
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
            faster_sys::f2_complete_pending(self.inner.store, false);
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
            faster_sys::f2_read(
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
            faster_sys::f2_complete_pending(self.inner.store, true);
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
            faster_sys::f2_delete(
                self.inner.store,
                key_bytes.as_ptr() as *const _,
                key_bytes.len() as u64,
                serial,
            )
        };

        // Complete pending operations
        unsafe {
            faster_sys::f2_complete_pending(self.inner.store, false);
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

    /// Get the total size of the store (number of records)
    pub fn size(&self) -> u64 {
        unsafe { faster_sys::f2_size(self.inner.store) }
    }

    /// Get the size of the hot store
    pub fn hot_size(&self) -> u64 {
        unsafe { faster_sys::f2_hot_size(self.inner.store) }
    }

    /// Get the size of the cold store
    pub fn cold_size(&self) -> u64 {
        unsafe { faster_sys::f2_cold_size(self.inner.store) }
    }

    /// Refresh the current session
    pub fn refresh(&self) {
        unsafe { faster_sys::f2_refresh_session(self.inner.store) }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        unsafe {
            if !self.store.is_null() {
                faster_sys::f2_destroy(self.store);
            }
        }
    }
}

impl Clone for F2Store {
    fn clone(&self) -> Self {
        F2Store {
            inner: Arc::clone(&self.inner),
        }
    }
}
