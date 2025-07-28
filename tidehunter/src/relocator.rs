use std::{path::Path, sync::Arc};

use minibytes::Bytes;

use crate::cell::CellId;
use crate::{
    db::{Db, DbResult},
    iterators::IteratorResult,
    key_shape::KeySpace,
    WalPosition,
};

pub struct DefaultRelocator {
    db: Arc<Db>,
    ks: KeySpace,
    first_wal_position: WalPosition,
    threshold: u64,
    keys_to_relocate: Vec<(Bytes, WalPosition)>,
    supported_punch_hole: bool,
}

impl DefaultRelocator {
    pub fn new(
        db_path: &Path,
        db: Arc<Db>,
        ks: KeySpace,
        first_wal_position: WalPosition,
        threshold: u64,
    ) -> DbResult<Self> {
        Ok(DefaultRelocator {
            db,
            ks,
            first_wal_position,
            threshold,
            keys_to_relocate: Vec::with_capacity(threshold as usize),
            supported_punch_hole: Self::check_punch_hole_support(db_path)?,
        })
    }

    /// NOTE: If this function crashes halfway through, the keys that were relocated will be relocated again
    /// when the next relocation is triggered.
    pub fn relocate(&mut self) -> DbResult<WalPosition> {
        let ksd = self.db.ks(self.ks).clone();
        let mut next_cell = Some(ksd.first_cell());

        // Collect keys from all cells.
        while let Some(cell) = next_cell {
            // Collect keys within the current cell.
            self.collect_entries_within_cell(cell.clone())?;

            // Relocate the collected keys.
            for (key, wal_position) in self.keys_to_relocate.drain(..) {
                // Check with the application if the key should be relocated.
                // TODO

                let (_, value) = self.db.read_record(wal_position)?;
                // TODO: Before inserting this entry, we should check that there are no race conditions where
                // the application has overwritten the key in the meantime.
                self.db.insert(self.ks, key, value)?;
            }

            // Move to the next cell.
            next_cell = self.db.next_cell(&ksd, &cell, false);
        }

        // Truncate the WAL to remove relocated entries.
        if self.supported_punch_hole {
            // TODO: Punch hole and update the first WAL position.
        }

        Ok(self.first_wal_position)
    }

    fn collect_entries_within_cell(&mut self, cell: CellId) -> DbResult<()> {
        let mut prev_key = None;
        let end_cell_exclusive = Some(cell.clone()); // Only iterate within this cell.

        // TODO: this is naive, it acquires a lock over the row at every iteration.
        while let Some(result) = self.db.next_entry_position(
            self.ks,
            cell.clone(),
            prev_key,
            &end_cell_exclusive,
            false,
        )? {
            let IteratorResult {
                key,
                value: wal_position,
                ..
            } = result;

            // If the WAL position is below the threshold, we can relocate the entry.
            if wal_position.offset() <= self.first_wal_position.offset() + self.threshold {
                self.keys_to_relocate.push((key.clone(), wal_position));
            }

            prev_key = Some(key);
        }

        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn check_punch_hole_support(_path: &Path) -> DbResult<bool> {
        Ok(false) // Punch hole is not supported on non-Linux systems.
    }

    #[cfg(target_os = "linux")]
    /// It seems there is no reliably way to check if punch hole is supported,
    /// so we will just try to use it and handle the error if it occurs.
    fn check_punch_hole_support(path: &str) -> io::Result<bool> {
        use std::{
            fs::{remove_file, OpenOptions},
            io::{self, Write},
            os::fd::AsRawFd,
        };

        // Create a small test file
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        file.write_all(&[0u8; 4096])?;

        let fd = file.as_raw_fd();
        let res = unsafe {
            libc::fallocate(
                fd,
                libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                0,
                4096,
            )
        };
        let supported = if res == 0 {
            true
        } else {
            let err = io::Error::last_os_error();
            if let Some(code) = err.raw_os_error() {
                if code == libc::EOPNOTSUPP || code == libc::ENOSYS {
                    false
                } else {
                    return Err(err);
                }
            } else {
                return Err(err);
            }
        };

        // Clean up test file
        remove_file(path)?;

        Ok(supported)
    }

    #[cfg(not(target_os = "linux"))]
    fn punch_hole(_file: &std::fs::File, _len: u64) -> std::io::Result<()> {
        panic!("Punch hole is not supported on non-Linux systems");
    }

    #[cfg(target_os = "linux")]
    fn punch_hole(file: &std::fs::File, len: u64) -> std::io::Result<()> {
        use std::os::unix::io::AsRawFd;

        let fd = file.as_raw_fd();
        let res = unsafe {
            libc::fallocate(
                fd,
                libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                0,
                len as i64,
            )
        };
        if res != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}
