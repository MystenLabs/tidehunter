//! Safe Rust wrapper for FASTER and F2 key-value stores

// Only compile the actual implementation on Linux with the feature enabled
#[cfg(all(feature = "enable-faster", target_os = "linux"))]
pub mod error;
#[cfg(all(feature = "enable-faster", target_os = "linux"))]
pub mod f2_store;
#[cfg(all(feature = "enable-faster", target_os = "linux"))]
pub mod store;
#[cfg(all(feature = "enable-faster", target_os = "linux"))]
pub mod traits;

#[cfg(all(feature = "enable-faster", target_os = "linux", test))]
mod tests;

// Re-export main types when available
#[cfg(all(feature = "enable-faster", target_os = "linux"))]
pub use error::{FasterError, Result};
#[cfg(all(feature = "enable-faster", target_os = "linux"))]
pub use f2_store::{F2Config, F2Store};
#[cfg(all(feature = "enable-faster", target_os = "linux"))]
pub use store::{Config, FasterKv};
#[cfg(all(feature = "enable-faster", target_os = "linux"))]
pub use traits::{StoreKey, StoreValue};

// For backward compatibility with existing code
#[cfg(all(feature = "enable-faster", target_os = "linux"))]
pub use f2_store::F2Store as F2Kv;
#[cfg(all(feature = "enable-faster", target_os = "linux"))]
pub use store::FasterKv as FasterStore;

// Export availability flag
pub use faster_sys::FASTER_AVAILABLE;
