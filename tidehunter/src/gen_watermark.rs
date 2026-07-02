//! Per-generation side-WAL durable watermark.
//!
//! Side WALs are not replayed on restart: index entries pointing into them
//! are recovered solely through the control region. That breaks the main
//! WAL's tail-recovery assumption in two ways. First, index blobs reference
//! any *completed* side-WAL frame regardless of the side tracker's frontier,
//! so a write that was still in flight at a crash can sit torn *below*
//! frames a persisted control region points at. Second, the open-time tail
//! scan stops at the first torn frame and zeroes the fragment tail from
//! there — which would destroy those durable, still-referenced frames and
//! let the resumed writer re-use their offsets.
//!
//! The watermark closes the hole: at every control-region store, after the
//! side WAL is fsynced, the side allocator's frontier is persisted to a
//! `durable` file in the generation's directory. Every position the control
//! region references was allocated (and completed, and fsynced) below that
//! frontier. On open, the tail scan resumes *from* the watermark instead of
//! offset 0: torn in-flight leftovers below it are unacknowledged garbage
//! that is never scanned, never zeroed, and never re-used — and the scan
//! cost is bounded by the un-synced tail instead of the whole side WAL.
//!
//! Write ordering within a control-region store: capture frontier → fsync
//! side WAL → save watermark → store control region. Saving before the CR
//! store matters: a CR must never reference positions above the persisted
//! watermark, or a torn frame between the stale watermark and those
//! positions could still truncate them at reopen.

use std::fs::{File, OpenOptions, rename};
use std::io::{self, Read, Write};
use std::path::Path;

const GEN_WATERMARK_FILE: &str = "durable";

/// Read the durable watermark for one generation directory. A missing file
/// (generation never covered by a control-region store) is watermark 0 —
/// the scan-from-zero behavior of a fresh side WAL.
pub(crate) fn read(gen_path: &Path) -> io::Result<u64> {
    let mut file = match File::open(gen_path.join(GEN_WATERMARK_FILE)) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    bincode::deserialize(&buffer).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to deserialize gen watermark: {e}"),
        )
    })
}

/// Atomically persist the durable watermark for one generation directory.
///
/// Caller contract: `durable` is the side allocator's frontier captured
/// after the flush snapshot was taken, and every side-WAL byte below the
/// frames it covers has been fsynced.
pub(crate) fn save(gen_path: &Path, durable: u64) -> io::Result<()> {
    let target = gen_path.join(GEN_WATERMARK_FILE);
    let tmp = target.with_extension("tmp");
    let serialized = bincode::serialize(&durable).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to serialize gen watermark: {e}"),
        )
    })?;
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)?;
    file.write_all(&serialized)?;
    file.sync_all()?;
    drop(file);
    rename(&tmp, &target)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gen_watermark_round_trip() {
        let dir = tempdir::TempDir::new("test_gen_watermark").unwrap();
        assert_eq!(read(dir.path()).unwrap(), 0, "missing file reads as 0");
        save(dir.path(), 12345).unwrap();
        assert_eq!(read(dir.path()).unwrap(), 12345);
        save(dir.path(), 99999).unwrap();
        assert_eq!(read(dir.path()).unwrap(), 99999);
    }
}
