use std::io::{self};
use std::path::Path;

pub fn truncate_file(path: &Path, length: u64) -> io::Result<()> {
    if length == 0 {
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    {
        use nix::fcntl::{fallocate, FallocateFlags};
        use std::fs::File;
        let file = File::options().write(true).open(path)?;
        Ok(fallocate(
            file,
            FallocateFlags::FALLOC_FL_COLLAPSE_RANGE,
            0,
            length as i64,
        )?)
    }
    #[cfg(target_os = "macos")]
    basic_truncate_file(path, length)
}

/// note: inefficient implementation, intended for testing and local development only
#[cfg(any(test, target_os = "macos"))]
pub fn basic_truncate_file(path: &Path, length: u64) -> io::Result<()> {
    use std::io::{Seek, SeekFrom};
    let temp_filepath = path.with_extension("tmp");
    let mut original_file = std::fs::File::open(path)?;
    let mut temp_file = std::fs::File::create(&temp_filepath)?;

    original_file.seek(SeekFrom::Start(length))?;
    io::copy(&mut original_file, &mut temp_file)?;
    std::fs::rename(&temp_filepath, path)
}
