pub mod batch;
mod cell;
pub mod config;
mod context;
#[doc(hidden)] // Used by tools/wal_inspector for control region inspection
pub mod control;
#[doc(hidden)] // Used by benchmarks and tools for WAL writes
pub mod crc;
pub mod db;
#[cfg(test)]
mod failpoints;
pub mod file_reader;
mod flusher;
pub mod index;
pub mod iterators;
pub mod key_shape;
#[doc(hidden)] // Used by tools/wal_inspector for control region inspection
pub mod large_table;
pub mod lookup;
mod math;
pub mod metrics;
mod primitives;
mod relocation;
mod runtime;
mod state_snapshot;

pub(crate) mod wal;

#[cfg(test)]
mod latch;

// todo remove re-export
pub use minibytes;

pub use index::index_table::IndexWalPosition;
pub use relocation::{Decision, RelocationStrategy};

// WAL re-exports
#[doc(hidden)] // Used by tools and benchmarks
pub use wal::layout::{WalKind, WalLayout};
pub use wal::position::WalPosition;
#[doc(hidden)] // Used by benchmarks
pub use wal::{PreparedWalWrite, Wal, WalWriter};

#[cfg(feature = "test-utils")]
pub mod test_utils {
    pub use crate::db::WalEntry;
    pub use crate::metrics::Metrics;
    pub use crate::wal::layout::WalLayout;
    pub use crate::wal::{list_wal_files_with_sizes, Wal, WalError, WalIterator};
}
