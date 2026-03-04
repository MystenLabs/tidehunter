use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::Path;

#[allow(dead_code)]
pub struct DbLock(File);

impl DbLock {
    pub fn acquire(db_path: &Path) -> io::Result<Self> {
        let lock_path = db_path.join("LOCK");
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;
        let fd = file.as_raw_fd();
        let mut flock = libc::flock {
            l_type: libc::F_WRLCK as i16, // Exclusive write lock
            l_whence: libc::SEEK_SET as i16,
            l_start: 0,
            l_len: 0, // Lock the entire file
            l_pid: 0,
        };
        let result = unsafe { libc::fcntl(fd, libc::F_SETLK, &mut flock) };
        if result == -1 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EWOULDBLOCK)
                || err.raw_os_error() == Some(libc::EAGAIN)
            {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "Database at {} is already locked by another process",
                        db_path.display()
                    ),
                ));
            }
            return Err(err);
        }
        Ok(DbLock(file))
    }
}
