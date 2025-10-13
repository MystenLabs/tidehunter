//! Safe wrapper for FASTER key-value store

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
    faster_complete_pending, faster_create, faster_delete, faster_destroy, faster_free_value,
    faster_insert, faster_read, faster_upsert, FasterConfig, FasterKv, FasterStatus,
};

pub struct FasterStore {
    #[cfg(target_os = "linux")]
    inner: Arc<FasterStoreInner>,
    #[cfg(not(target_os = "linux"))]
    _phantom: std::marker::PhantomData<()>,
}

#[cfg(target_os = "linux")]
struct FasterStoreInner {
    store: *mut FasterKv,
}

#[cfg(target_os = "linux")]
unsafe impl Send for FasterStoreInner {}
#[cfg(target_os = "linux")]
unsafe impl Sync for FasterStoreInner {}

#[cfg(target_os = "linux")]
impl Drop for FasterStoreInner {
    fn drop(&mut self) {
        unsafe {
            if !self.store.is_null() {
                faster_destroy(self.store);
            }
        }
    }
}

impl FasterStore {
    pub fn new(path: &Path) -> Result<Self, StoreError> {
        // Default configuration for backward compatibility
        Self::new_with_config(
            path,
            1 << 30,  // 1GB initial log
            1 << 34,  // 16GB max log
            1 << 30,  // 1GB segment
            1 << 28,  // 256MB read cache
        )
    }

    pub fn new_with_config(
        path: &Path,
        initial_log_size: usize,
        max_log_size: usize,
        segment_size: usize,
        read_cache_size: usize,
    ) -> Result<Self, StoreError> {
        #[cfg(target_os = "linux")]
        {
            let path_str = path.to_str().ok_or(StoreError::InvalidConfiguration)?;
            let c_path = CString::new(path_str).map_err(|_| StoreError::InvalidConfiguration)?;

            let config = FasterConfig {
                storage_path: c_path.as_ptr(),
                initial_log_size,
                max_log_size,
                page_size: 1 << 21,  // 2MB (keep constant)
                segment_size,
                enable_read_cache: true,
                read_cache_size,
            };

            let store = unsafe { faster_create(&config) };
            if store.is_null() {
                return Err(StoreError::OperationFailed);
            }

            Ok(FasterStore {
                inner: Arc::new(FasterStoreInner { store }),
            })
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = (path, initial_log_size, max_log_size, segment_size, read_cache_size);
            Err(StoreError::PlatformNotSupported)
        }
    }

    /// Create a FASTER store optimized for large datasets (1TB) on high-memory machines (256GB RAM)
    pub fn new_for_large_dataset(path: &Path) -> Result<Self, StoreError> {
        Self::new_with_config(
            path,
            1 << 36,  // 64GB initial log
            1 << 40,  // 1TB max log
            1 << 32,  // 4GB segment
            1 << 36,  // 64GB read cache
        )
    }

    pub fn insert(&self, key: Bytes, value: Bytes) -> Result<(), StoreError> {
        #[cfg(target_os = "linux")]
        {
            let serial = next_serial();
            let status = unsafe {
                faster_insert(
                    self.inner.store,
                    key.as_ptr() as *const _,
                    key.len(),
                    value.as_ptr() as *const _,
                    value.len(),
                    serial,
                )
            };

            unsafe {
                faster_complete_pending(self.inner.store, false);
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
                faster_read(
                    self.inner.store,
                    key.as_ptr() as *const _,
                    key.len(),
                    &mut value_ptr,
                    &mut value_len,
                    serial,
                )
            };

            unsafe {
                faster_complete_pending(self.inner.store, true);
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
                faster_upsert(
                    self.inner.store,
                    key.as_ptr() as *const _,
                    key.len(),
                    value.as_ptr() as *const _,
                    value.len(),
                    serial,
                )
            };

            unsafe {
                faster_complete_pending(self.inner.store, false);
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
                faster_delete(
                    self.inner.store,
                    key.as_ptr() as *const _,
                    key.len(),
                    serial,
                )
            };

            unsafe {
                faster_complete_pending(self.inner.store, false);
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
