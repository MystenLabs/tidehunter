//! Safe wrapper for F2 (FASTER v2) key-value store

#[cfg(target_os = "linux")]
use crate::next_serial;
use crate::StoreError;
use minibytes::Bytes;
#[cfg(target_os = "linux")]
use std::ffi::CString;
use std::path::Path;
#[cfg(target_os = "linux")]
use std::sync::Arc;

#[cfg(target_os = "linux")]
use faster_sys::{
    f2_complete_pending, f2_create, f2_delete, f2_destroy, f2_insert, f2_read, f2_upsert,
    faster_free_value, F2Config, F2Kv, FasterStatus,
};

pub struct F2Store {
    #[cfg(target_os = "linux")]
    inner: Arc<F2StoreInner>,
    #[cfg(not(target_os = "linux"))]
    _phantom: std::marker::PhantomData<()>,
}

#[cfg(target_os = "linux")]
struct F2StoreInner {
    store: *mut F2Kv,
}

#[cfg(target_os = "linux")]
unsafe impl Send for F2StoreInner {}
#[cfg(target_os = "linux")]
unsafe impl Sync for F2StoreInner {}

#[cfg(target_os = "linux")]
impl Drop for F2StoreInner {
    fn drop(&mut self) {
        unsafe {
            if !self.store.is_null() {
                f2_destroy(self.store);
            }
        }
    }
}

impl F2Store {
    pub fn new(path: &Path) -> Result<Self, StoreError> {
        // Default configuration for backward compatibility
        Self::new_with_config(
            path,
            1 << 30,  // 1GB hot store
            1 << 34,  // 16GB cold store
            1 << 28,  // 256MB read cache
        )
    }

    pub fn new_with_config(
        path: &Path,
        hot_store_size: usize,
        cold_store_size: usize,
        read_cache_size: usize,
    ) -> Result<Self, StoreError> {
        #[cfg(target_os = "linux")]
        {
            let path_str = path.to_str().ok_or(StoreError::InvalidConfiguration)?;
            let c_path = CString::new(path_str).map_err(|_| StoreError::InvalidConfiguration)?;

            let config = F2Config {
                storage_path: c_path.as_ptr(),
                hot_store_size,
                cold_store_size,
                read_cache_size,
                enable_tiering: true,
                hot_threshold: 0.8,  // Move to cold when hot store is 80% full
                cold_threshold: 0.1, // Move back to hot if accessed frequently
            };

            let store = unsafe { f2_create(&config) };
            if store.is_null() {
                return Err(StoreError::OperationFailed);
            }

            Ok(F2Store {
                inner: Arc::new(F2StoreInner { store }),
            })
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = (path, hot_store_size, cold_store_size, read_cache_size);
            Err(StoreError::PlatformNotSupported)
        }
    }

    /// Create an F2 store optimized for large datasets (1TB) on high-memory machines (256GB RAM)
    pub fn new_for_large_dataset(path: &Path) -> Result<Self, StoreError> {
        Self::new_with_config(
            path,
            1 << 37,  // 128GB hot store (50% of RAM for hot data)
            1 << 40,  // 1TB cold store (full dataset on disk)
            1 << 36,  // 64GB read cache (25% of RAM for cache)
        )
    }

    pub fn insert(&self, key: Bytes, value: Bytes) -> Result<(), StoreError> {
        #[cfg(target_os = "linux")]
        {
            let serial = next_serial();
            let status = unsafe {
                f2_insert(
                    self.inner.store,
                    key.as_ptr() as *const _,
                    key.len(),
                    value.as_ptr() as *const _,
                    value.len(),
                    serial,
                )
            };

            unsafe {
                f2_complete_pending(self.inner.store, false);
            }

            match status {
                FasterStatus::FASTER_SUCCESS => Ok(()),
                _ => Err(StoreError::OperationFailed),
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = (key, value);
            Err(StoreError::PlatformNotSupported)
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Bytes, StoreError> {
        #[cfg(target_os = "linux")]
        {
            let serial = next_serial();
            let mut value_ptr: *mut libc::c_void = std::ptr::null_mut();
            let mut value_len: libc::size_t = 0;

            let status = unsafe {
                f2_read(
                    self.inner.store,
                    key.as_ptr() as *const _,
                    key.len(),
                    &mut value_ptr,
                    &mut value_len,
                    serial,
                )
            };

            unsafe {
                f2_complete_pending(self.inner.store, true);
            }

            match status {
                FasterStatus::FASTER_SUCCESS => {
                    if value_ptr.is_null() || value_len == 0 {
                        return Err(StoreError::NotFound);
                    }

                    // Copy the data and free the C allocation
                    let slice =
                        unsafe { std::slice::from_raw_parts(value_ptr as *const u8, value_len) };
                    let bytes = Bytes::from(slice.to_vec());

                    unsafe {
                        faster_free_value(value_ptr);
                    }

                    Ok(bytes)
                }
                FasterStatus::FASTER_NOT_FOUND => Err(StoreError::NotFound),
                _ => Err(StoreError::OperationFailed),
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = key;
            Err(StoreError::PlatformNotSupported)
        }
    }

    pub fn upsert(&self, key: Bytes, value: Bytes) -> Result<(), StoreError> {
        #[cfg(target_os = "linux")]
        {
            let serial = next_serial();
            let status = unsafe {
                f2_upsert(
                    self.inner.store,
                    key.as_ptr() as *const _,
                    key.len(),
                    value.as_ptr() as *const _,
                    value.len(),
                    serial,
                )
            };

            unsafe {
                f2_complete_pending(self.inner.store, false);
            }

            match status {
                FasterStatus::FASTER_SUCCESS => Ok(()),
                _ => Err(StoreError::OperationFailed),
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = (key, value);
            Err(StoreError::PlatformNotSupported)
        }
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        #[cfg(target_os = "linux")]
        {
            let serial = next_serial();
            let status = unsafe {
                f2_delete(
                    self.inner.store,
                    key.as_ptr() as *const _,
                    key.len(),
                    serial,
                )
            };

            unsafe {
                f2_complete_pending(self.inner.store, false);
            }

            match status {
                FasterStatus::FASTER_SUCCESS => Ok(()),
                _ => Err(StoreError::OperationFailed),
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = key;
            Err(StoreError::PlatformNotSupported)
        }
    }

    pub fn exists(&self, key: &[u8]) -> bool {
        self.get(key).is_ok()
    }
}
