use crate::index::index_format::Direction;
use crate::wal::WalPosition;
use minibytes::Bytes;
use std::cmp::Ordering;

/// Represents the possible outcomes when looking for the next entry
#[derive(Debug, PartialEq)]
pub enum NextEntryResult {
    /// No entry was found
    NotFound,
    /// A valid entry was found with its key and position
    Found(Bytes, WalPosition),
    /// A deleted entry was found that should be skipped
    SkipDeleted(Bytes),
}

/// Determines which is the next entry to return when there are two possible sources
/// (e.g., in-memory and on-disk).
///
/// This is used during iteration to correctly merge results from both memory and disk
/// when handling entries in a DirtyUnloaded state.
///
/// The function follows these rules:
/// - If both sources have no entries, return NotFound
/// - If only one source has an entry, return it (if valid) or SkipDeleted
/// - If both sources have entries, compare them based on the direction:
///   - For forward iteration, pick the smaller key
///   - For reverse iteration, pick the larger key
/// - If the keys are equal, prefer the in-memory version
pub fn take_next_entry(
    in_memory: Option<(Bytes, WalPosition)>,
    on_disk: Option<(Bytes, WalPosition)>,
    direction: Direction,
) -> NextEntryResult {
    match (in_memory, on_disk) {
        (None, None) => NextEntryResult::NotFound,

        (Some((k, v)), None) => {
            if v.is_valid() {
                NextEntryResult::Found(k, v)
            } else {
                // Need to skip this key and recursively try again
                NextEntryResult::SkipDeleted(k)
            }
        }

        (None, Some((k, v))) => NextEntryResult::Found(k, v),

        (Some((k_mem, v_mem)), Some((k_disk, v_disk))) => {
            // Compare keys based on direction
            let compare = k_mem.as_ref().cmp(k_disk.as_ref());
            let first_is_memory = match direction {
                Direction::Forward => compare == Ordering::Less,
                Direction::Backward => compare == Ordering::Greater,
            };

            if first_is_memory {
                if v_mem.is_valid() {
                    NextEntryResult::Found(k_mem, v_mem)
                } else {
                    // Skip deleted entry and continue with this key
                    NextEntryResult::SkipDeleted(k_mem)
                }
            } else if compare == Ordering::Equal {
                // Same key, prefer in-memory version
                if v_mem.is_valid() {
                    NextEntryResult::Found(k_mem, v_mem)
                } else {
                    // In-memory version is deleted, skip this key
                    NextEntryResult::SkipDeleted(k_mem)
                }
            } else {
                NextEntryResult::Found(k_disk, v_disk)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_take_next_entry() {
        let key1 = Bytes::from(vec![1, 2, 3]);
        let key2 = Bytes::from(vec![4, 5, 6]);
        let key3 = Bytes::from(vec![7, 8, 9]);

        let valid_pos = WalPosition::test_value(42);
        let invalid_pos = WalPosition::INVALID;

        // Case 1: Both None
        assert_eq!(
            take_next_entry(None, None, Direction::Forward),
            NextEntryResult::NotFound
        );

        // Case 2: Only in_memory has a value (valid)
        let result = take_next_entry(Some((key1.clone(), valid_pos)), None, Direction::Forward);
        assert_eq!(result, NextEntryResult::Found(key1.clone(), valid_pos));

        // Case 3: Only in_memory has a value (invalid)
        let result = take_next_entry(Some((key1.clone(), invalid_pos)), None, Direction::Forward);
        assert_eq!(result, NextEntryResult::SkipDeleted(key1.clone()));

        // Case 4: Only on_disk has a value
        let result = take_next_entry(None, Some((key2.clone(), valid_pos)), Direction::Forward);
        assert_eq!(result, NextEntryResult::Found(key2.clone(), valid_pos));

        // Case 5: Both have values, memory key < disk key (Forward direction)
        // Should pick memory (smaller) key in forward direction
        let result = take_next_entry(
            Some((key1.clone(), valid_pos)),
            Some((key2.clone(), valid_pos)),
            Direction::Forward,
        );
        assert_eq!(result, NextEntryResult::Found(key1.clone(), valid_pos));

        // Case 6: Both have values, memory key > disk key (Forward direction)
        // Should pick disk (smaller) key in forward direction
        let result = take_next_entry(
            Some((key2.clone(), valid_pos)),
            Some((key1.clone(), valid_pos)),
            Direction::Forward,
        );
        assert_eq!(result, NextEntryResult::Found(key1.clone(), valid_pos));

        // Case 7: Both have values, memory key < disk key (Backward direction)
        // Should pick disk (larger) key in backward direction
        let result = take_next_entry(
            Some((key1.clone(), valid_pos)),
            Some((key2.clone(), valid_pos)),
            Direction::Backward,
        );
        assert_eq!(result, NextEntryResult::Found(key2.clone(), valid_pos));

        // Case 8: Both have values, memory key > disk key (Backward direction)
        // Should pick memory (larger) key in backward direction
        let result = take_next_entry(
            Some((key2.clone(), valid_pos)),
            Some((key1.clone(), valid_pos)),
            Direction::Backward,
        );
        assert_eq!(result, NextEntryResult::Found(key2.clone(), valid_pos));

        // Case 9: Both have values with equal keys, with valid memory value
        // Should prefer memory version when keys are equal
        let result = take_next_entry(
            Some((key3.clone(), valid_pos)),
            Some((key3.clone(), WalPosition::test_value(43))),
            Direction::Forward,
        );
        assert_eq!(result, NextEntryResult::Found(key3.clone(), valid_pos));

        // Case 10: Both have values with equal keys, with invalid memory value
        // Should signal to skip the key when memory version is invalid
        let result = take_next_entry(
            Some((key3.clone(), invalid_pos)),
            Some((key3.clone(), valid_pos)),
            Direction::Forward,
        );
        assert_eq!(result, NextEntryResult::SkipDeleted(key3.clone()));

        // Case 11: Memory entry is invalid and should be skipped
        let result = take_next_entry(
            Some((key1.clone(), invalid_pos)),
            Some((key2.clone(), valid_pos)),
            Direction::Forward,
        );
        assert_eq!(result, NextEntryResult::SkipDeleted(key1.clone()));
    }
}
