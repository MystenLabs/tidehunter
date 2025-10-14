use thiserror::Error;

#[derive(Error, Debug)]
pub enum FasterError {
    #[error("Key not found")]
    NotFound,

    #[error("Operation failed")]
    OperationFailed,

    #[error("Out of memory")]
    OutOfMemory,

    #[error("I/O error")]
    IoError,

    #[error("Data corrupted")]
    Corrupted,

    #[error("Operation aborted")]
    Aborted,

    #[error("Store initialization failed")]
    InitializationFailed,

    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),

    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("Platform not supported")]
    PlatformNotSupported,

    #[error("FFI error: null pointer")]
    NullPointer,

    #[error("Invalid UTF-8 in path")]
    InvalidPath,
}

impl FasterError {
    /// Convert from FFI status code
    pub(crate) fn from_status(status: faster_sys::faster_status) -> Self {
        use faster_sys::faster_status::*;
        match status {
            FASTER_NOT_FOUND => FasterError::NotFound,
            FASTER_OUT_OF_MEMORY => FasterError::OutOfMemory,
            FASTER_IO_ERROR => FasterError::IoError,
            FASTER_CORRUPTED => FasterError::Corrupted,
            FASTER_ABORTED => FasterError::Aborted,
            _ => FasterError::OperationFailed,
        }
    }
}

/// Result type for FASTER operations
pub type Result<T> = std::result::Result<T, FasterError>;
