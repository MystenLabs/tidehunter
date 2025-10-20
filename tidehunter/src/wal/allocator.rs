use super::layout::WalLayout;
use crate::wal::position::HasOffset;
use std::sync::atomic::{AtomicU64, Ordering};

/// A thread-safe WAL position allocator that atomically allocates space in the WAL.
pub struct WalAllocator {
    position: AtomicU64,
    // padding ensures position is alone on cache line
    _pad: [u8; 56], // 64 - 8 bytes for u64
    layout: WalLayout,
}

#[derive(Clone)]
pub struct AllocationResult {
    allocated_position: u64,
    previous_position: u64,
    len_aligned: u64,
}

impl WalAllocator {
    /// Create a new WAL allocator with the given layout and initial position.
    pub fn new(layout: WalLayout, initial_position: u64) -> Self {
        Self {
            position: AtomicU64::new(initial_position),
            _pad: [0u8; 56],
            layout,
        }
    }

    /// Atomically allocate space for an entry of the given size.
    /// Returns an AllocationResult containing the allocated position and previous position.
    pub fn allocate(&self, len: u64) -> AllocationResult {
        let len_aligned = self.layout.align(len);

        loop {
            // Load current position
            let current_pos = self.position.load(Ordering::Acquire);

            // Calculate where this allocation should go
            let allocated_pos = self.layout.next_position(current_pos, len_aligned);

            // Calculate next position after this allocation
            let next_pos = allocated_pos + len_aligned;

            // Try to atomically update position
            match self.position.compare_exchange(
                current_pos,
                next_pos,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return AllocationResult {
                        allocated_position: allocated_pos,
                        previous_position: current_pos,
                        len_aligned,
                    };
                }
                Err(_) => continue, // Contention detected, retry
            }
        }
    }

    /// Get the current position (for testing/monitoring).
    pub fn position(&self) -> u64 {
        self.position.load(Ordering::Acquire)
    }
}

impl AllocationResult {
    /// Returns if allocated position is in the beginning
    /// of new fragment and needs skip marker in previous fragment
    pub fn need_skip_marker(&self) -> Option<u64> {
        if self.previous_position != self.allocated_position {
            Some(self.previous_position)
        } else {
            None
        }
    }

    pub fn allocated_position(&self) -> u64 {
        self.allocated_position
    }

    pub fn next_position(&self) -> u64 {
        self.allocated_position + self.len_aligned
    }

    pub fn previous_position(&self) -> u64 {
        self.previous_position
    }

    pub fn len_aligned(&self) -> u64 {
        self.len_aligned
    }
}

impl HasOffset for AllocationResult {
    fn offset(&self) -> u64 {
        self.allocated_position
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::layout::WalKind;

    #[test]
    fn test_basic_allocation() {
        let layout = l(1024);

        let allocator = WalAllocator::new(layout, 0);

        // First allocation (100 bytes aligns to 104 with 8-byte alignment)
        let result1 = allocator.allocate(100);
        assert_eq!(result1.allocated_position, 0);
        assert_eq!(result1.previous_position, 0);
        assert_eq!(allocator.position(), 104);

        // Second allocation (200 bytes is already 8-byte aligned)
        let result2 = allocator.allocate(200);
        assert_eq!(result2.allocated_position, 104);
        assert_eq!(result2.previous_position, 104);
        assert_eq!(allocator.position(), 304);
    }

    #[test]
    fn test_fragment_boundary() {
        let layout = l(1024);

        let allocator = WalAllocator::new(layout, 900);

        // This allocation would cross fragment boundary, so it should skip to next fragment
        let result = allocator.allocate(200);
        assert_eq!(result.allocated_position, 1024); // Should skip to start of next fragment
        assert_eq!(result.previous_position, 900); // Previous position before allocation
        assert_eq!(allocator.position(), 1024 + 200);
    }

    #[test]
    fn test_concurrent_allocation() {
        use std::sync::Arc;
        use std::thread;

        let layout = l(1024 * 1024);

        let allocator = Arc::new(WalAllocator::new(layout, 0));
        let mut handles = vec![];

        // Spawn 10 threads, each allocating 100 times
        for _ in 0..10 {
            let allocator_clone = Arc::clone(&allocator);
            let handle = thread::spawn(move || {
                let mut positions = vec![];
                for _ in 0..100 {
                    let result = allocator_clone.allocate(100);
                    positions.push(result.allocated_position);
                }
                positions
            });
            handles.push(handle);
        }

        // Collect all positions
        let mut all_positions = vec![];
        for handle in handles {
            all_positions.extend(handle.join().unwrap());
        }

        // All positions should be unique
        all_positions.sort_unstable();
        for i in 1..all_positions.len() {
            assert_ne!(
                all_positions[i],
                all_positions[i - 1],
                "Duplicate position detected"
            );
        }

        // Final position should be 1000 * 104 = 104000 (100 bytes aligns to 104)
        assert_eq!(allocator.position(), 104_000);
    }

    #[test]
    fn test_need_skip_marker() {
        let layout = l(1024);
        let allocator = WalAllocator::new(layout, 0);
        let r = allocator.allocate(10);
        assert!(r.need_skip_marker().is_none());
        assert_eq!(0, r.allocated_position());

        let r = allocator.allocate(1020);
        assert_eq!(1024, r.allocated_position());
        assert_eq!(Some(16), r.need_skip_marker());
    }

    fn l(frag_size: u64) -> WalLayout {
        WalLayout {
            frag_size,
            max_maps: 10,
            direct_io: false,
            wal_file_size: frag_size * 10,
            kind: WalKind::Replay,
        }
    }
}
