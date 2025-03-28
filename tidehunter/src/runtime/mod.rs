/// This module provides a runtime abstraction for the async runtime.
/// It uses std by default, but can be switched to tokio by enabling the `tokio` feature.

/// Blocks the current thread until the provided future has completed.
pub fn block_in_place<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    #[cfg(feature = "tokio")]
    return tokio::task::block_in_place(f);

    #[cfg(not(feature = "tokio"))]
    f()
}

/// A type that represents a point in time.
#[cfg(feature = "tokio")]
pub type Instant = tokio::time::Instant;
#[cfg(not(feature = "tokio"))]
pub type Instant = std::time::Instant;
