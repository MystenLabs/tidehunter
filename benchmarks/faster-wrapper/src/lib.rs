//! Safe Rust wrapper for FASTER and F2 key-value stores

pub mod f2;
pub mod faster;

pub use f2::F2Store;
pub use faster::FasterStore;

#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicU64, Ordering};

// Global serial number generator
#[cfg(target_os = "linux")]
static SERIAL_COUNTER: AtomicU64 = AtomicU64::new(1);

#[cfg(target_os = "linux")]
pub(crate) fn next_serial() -> u64 {
    SERIAL_COUNTER.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreError {
    NotFound,
    OperationFailed,
    PlatformNotSupported,
    InvalidConfiguration,
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::NotFound => write!(f, "Key not found"),
            StoreError::OperationFailed => write!(f, "Operation failed"),
            StoreError::PlatformNotSupported => write!(f, "Platform not supported"),
            StoreError::InvalidConfiguration => write!(f, "Invalid configuration"),
        }
    }
}

impl std::error::Error for StoreError {}
