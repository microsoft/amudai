//! Cross-platform file system operations with configurable I/O buffering modes.
//!
//! This module provides a unified interface for file system operations across different
//! platforms (Unix-like systems and Windows) with support for both buffered and unbuffered
//! I/O modes. The implementation automatically selects the appropriate platform-specific
//! optimizations while providing consistent behavior across all supported platforms.
//!
//! # I/O Modes
//!
//! The module supports two I/O modes controlled by the [`IoMode`] enum:
//!
//! - [`IoMode::Buffered`]: Uses the operating system's page cache for I/O operations
//! - [`IoMode::Unbuffered`]: Bypasses the page cache using direct I/O (O_DIRECT on Unix,
//!   FILE_FLAG_NO_BUFFERING on Windows)
//!
//! # Platform-Specific Optimizations
//!
//! ## Unix/Linux
//! - Uses `O_DIRECT` flag for unbuffered I/O
//! - Leverages `O_TMPFILE` on Linux for secure anonymous temporary files
//! - Automatic buffer alignment requirements for direct I/O
//!
//! ## Windows
//! - Uses `FILE_FLAG_NO_BUFFERING` for unbuffered I/O
//! - Implements `FILE_FLAG_DELETE_ON_CLOSE` for automatic temporary file cleanup

#[cfg_attr(any(unix, target_os = "redox", target_os = "wasi"), path = "unix.rs")]
#[cfg_attr(windows, path = "windows.rs")]
mod platform;

mod shared;

pub use platform::*;

/// Specifies the I/O buffering mode for file operations.
///
/// This enum controls whether file operations use the operating system's
/// page cache or bypass it for direct I/O access. The choice affects
/// performance characteristics and memory usage patterns.
///
/// # Automatic Fallback
///
/// When [`IoMode::Unbuffered`] is requested but not supported (e.g., on certain
/// filesystems or when alignment requirements cannot be met), the implementation
/// automatically falls back to [`IoMode::Buffered`] to ensure operation reliability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMode {
    /// Use the operating system's page cache for I/O operations.
    Buffered,

    /// Bypass the operating system's page cache for direct I/O access.
    Unbuffered,
}

#[cfg(test)]
mod tests {
    use amudai_bytes::buffer::AlignedByteVec;

    use crate::fs::{IoMode, create_temporary_in};

    #[test]
    fn test_create_temp() {
        let temp_dir = std::env::temp_dir();
        let file = create_temporary_in(&temp_dir, IoMode::Unbuffered).unwrap();
        let mut buf = AlignedByteVec::with_capacity_and_alignment(20000, 4096);
        buf.resize(20000, 17);
        amudai_io::file::file_write_at(&file, 4096 * 3, &buf[..8192]).unwrap();
        amudai_io::file::file_read_at_exact(&file, 4096 * 2, &mut buf[..8192]).unwrap();
    }
}
