//! Windows-specific file system operations.
//!
//! This module provides file system operations optimized for Windows systems,
//! including support for unbuffered I/O using FILE_FLAG_NO_BUFFERING and
//! automatic temporary file cleanup using FILE_FLAG_DELETE_ON_CLOSE.
//!
//! The implementation leverages Windows-specific features like:
//! - `FILE_FLAG_NO_BUFFERING` for direct I/O operations
//! - `FILE_FLAG_DELETE_ON_CLOSE` for automatic temporary file cleanup
//! - `FILE_ATTRIBUTE_TEMPORARY` for optimized temporary file handling

use std::{fs::OpenOptions, path::Path};

use crate::fs::{IoMode, shared};

/// Opens an existing file for reading with the specified I/O mode.
///
/// # Arguments
///
/// * `file_path` - Path to the file to open
/// * `io_mode` - I/O mode to use (buffered or unbuffered)
///
/// # Returns
///
/// Returns a `Result` containing the opened file handle or an I/O error.
///
/// # I/O Modes
///
/// * [`IoMode::Buffered`] - Uses Windows' standard file caching
/// * [`IoMode::Unbuffered`] - Uses FILE_FLAG_NO_BUFFERING for direct I/O
///
/// # Platform Notes
///
/// On Windows, unbuffered mode uses the `FILE_FLAG_NO_BUFFERING` flag, which requires:
/// - Buffer alignment to sector boundaries (typically 512 or 4096 bytes)
/// - I/O size to be multiples of the sector size
/// - May not be supported on all storage devices or file systems
pub fn open(file_path: &Path, io_mode: IoMode) -> std::io::Result<std::fs::File> {
    let mut options = OpenOptions::new();
    options.read(true);

    match io_mode {
        IoMode::Buffered => (),
        IoMode::Unbuffered => {
            use std::os::windows::fs::OpenOptionsExt;
            options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_NO_BUFFERING);
        }
    }
    options.open(file_path)
}

/// Creates a new file with read and write access in the specified I/O mode.
///
/// # Arguments
///
/// * `file_path` - Path where the new file should be created
/// * `io_mode` - I/O mode to use (buffered or unbuffered)
///
/// # Returns
///
/// Returns a `Result` containing the created file handle or an I/O error.
///
/// # Behavior
///
/// This function will:
/// - Create a new file at the specified path
/// - Fail if the file already exists (uses `create_new(true)`)
/// - Apply the specified I/O mode
///
/// # I/O Modes
///
/// * [`IoMode::Buffered`] - Uses Windows' standard file caching
/// * [`IoMode::Unbuffered`] - Uses FILE_FLAG_NO_BUFFERING for direct I/O
///
/// # Errors
///
/// Returns an error if:
/// - The file already exists
/// - The parent directory doesn't exist
/// - Insufficient permissions to create the file
/// - The storage device doesn't support the requested I/O mode
pub fn create(file_path: &Path, io_mode: IoMode) -> std::io::Result<std::fs::File> {
    let mut options = OpenOptions::new();
    options.create_new(true).read(true).write(true);
    match io_mode {
        IoMode::Buffered => (),
        IoMode::Unbuffered => {
            use std::os::windows::fs::OpenOptionsExt;
            options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_NO_BUFFERING);
        }
    }
    options.open(file_path)
}

/// Creates a temporary file in the specified directory with automatic cleanup.
///
/// # Arguments
///
/// * `folder_path` - Directory where the temporary file should be created
/// * `io_mode` - Preferred I/O mode (buffered or unbuffered)
///
/// # Returns
///
/// Returns a `Result` containing the temporary file handle or an I/O error.
///
/// # Behavior
///
/// This function implements a fallback strategy:
/// 1. First attempts to create the temporary file with the requested I/O mode
/// 2. If unbuffered mode fails, automatically falls back to buffered mode
/// 3. The temporary file is automatically deleted when the file handle is closed
///
/// # Windows Features
///
/// * Uses `FILE_FLAG_DELETE_ON_CLOSE` for automatic cleanup
/// * Uses `FILE_ATTRIBUTE_TEMPORARY` to hint that the file is short-lived
/// * Sets exclusive access (`share_mode(0)`) to prevent other processes from accessing the file
pub fn create_temporary_in(folder_path: &Path, io_mode: IoMode) -> std::io::Result<std::fs::File> {
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_TEMPORARY, FILE_FLAG_DELETE_ON_CLOSE, FILE_FLAG_NO_BUFFERING,
    };

    let name = shared::generate_temp_file_name(12);
    let file_path = folder_path.join(name);
    let flags = match io_mode {
        IoMode::Buffered => FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE,
        IoMode::Unbuffered => FILE_FLAG_NO_BUFFERING | FILE_FLAG_DELETE_ON_CLOSE,
    };

    match create_temporary_impl(&file_path, flags) {
        Ok(file) => return Ok(file),
        Err(e) if io_mode == IoMode::Buffered => return Err(e),
        _ => (),
    }

    // Try buffered fallback
    create_temporary_impl(
        &file_path,
        FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE,
    )
}

/// Internal implementation for creating temporary files with specific Windows flags.
///
/// # Arguments
///
/// * `file_path` - Path where the temporary file should be created
/// * `flags` - Windows-specific flags to apply to the file creation
///
/// # Returns
///
/// Returns a `Result` containing the temporary file handle or an I/O error.
///
/// # Windows Flags
///
/// Common flag combinations used:
/// - `FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE`: Standard temporary file
/// - `FILE_FLAG_NO_BUFFERING | FILE_FLAG_DELETE_ON_CLOSE`: Unbuffered temporary file
fn create_temporary_impl(file_path: &Path, flags: u32) -> std::io::Result<std::fs::File> {
    use std::os::windows::fs::OpenOptionsExt;
    OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .share_mode(0)
        .custom_flags(flags)
        .open(file_path)
}
