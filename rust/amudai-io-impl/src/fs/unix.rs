//! Unix-specific file system operations.
//!
//! This module provides file system operations optimized for Unix-like systems,
//! including support for direct I/O (O_DIRECT) and efficient temporary file creation.
//!
//! The implementation leverages platform-specific features like:
//! - `O_DIRECT` flag for unbuffered I/O operations
//! - `O_TMPFILE` on Linux for secure temporary file creation

use std::{fs::OpenOptions, path::Path};

use crate::fs::IoMode;

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
/// * [`IoMode::Buffered`] - Uses the system's page cache for I/O operations
/// * [`IoMode::Unbuffered`] - Uses direct I/O (O_DIRECT), bypassing the page cache
///
/// # Platform Notes
///
/// On Unix systems, unbuffered mode uses the `O_DIRECT` flag, which requires:
/// - Buffer alignment to filesystem block boundaries
/// - I/O size to be multiples of the filesystem block size
/// - May not be supported on all filesystems
pub fn open(file_path: &Path, io_mode: IoMode) -> std::io::Result<std::fs::File> {
    let mut options = OpenOptions::new();
    options.read(true);

    match io_mode {
        IoMode::Buffered => (),
        IoMode::Unbuffered => {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_DIRECT);
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
/// * [`IoMode::Buffered`] - Uses the system's page cache for I/O operations
/// * [`IoMode::Unbuffered`] - Uses direct I/O (O_DIRECT), bypassing the page cache
///
/// # Errors
///
/// Returns an error if:
/// - The file already exists
/// - The parent directory doesn't exist
/// - Insufficient permissions to create the file
/// - The filesystem doesn't support the requested I/O mode
pub fn create(file_path: &Path, io_mode: IoMode) -> std::io::Result<std::fs::File> {
    let mut options = OpenOptions::new();
    options.create_new(true).read(true).write(true);
    match io_mode {
        IoMode::Buffered => (),
        IoMode::Unbuffered => {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_DIRECT);
        }
    }
    options.open(file_path)
}

/// Creates a temporary file in the specified directory with the given I/O mode.
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
/// 3. The temporary file is automatically unlinked from the filesystem but remains accessible
///    via the file handle
///
/// # Platform Optimizations
///
/// * **Linux**: Uses `O_TMPFILE` when available for secure, anonymous temporary files
/// * **Other Unix**: Falls back to creating named temporary files with secure permissions (0o600)
///
/// # Security
///
/// Temporary files are created with restrictive permissions (read/write for owner only)
/// and are immediately unlinked to prevent other processes from accessing them.
pub fn create_temporary_in(folder_path: &Path, io_mode: IoMode) -> std::io::Result<std::fs::File> {
    // Try to create the temporary file with the requested mode
    match create_temp(folder_path, io_mode) {
        Ok(file) => Ok(file),
        Err(_) if io_mode == IoMode::Unbuffered => {
            // If unbuffered mode failed, try again with buffered mode as fallback
            create_temp(folder_path, IoMode::Buffered)
        }
        Err(e) => Err(e),
    }
}

/// Internal function to create a temporary file with platform-specific optimizations.
///
/// # Arguments
///
/// * `folder_path` - Directory where the temporary file should be created
/// * `io_mode` - I/O mode to use (buffered or unbuffered)
///
/// # Returns
///
/// Returns a `Result` containing the temporary file handle or an I/O error.
///
/// # Platform Behavior
///
/// * **Linux**: Uses [`create_temp_linux`] which attempts `O_TMPFILE` first
/// * **Other Unix**: Uses [`create_temp_generic`] with named temporary files
fn create_temp(folder_path: &Path, io_mode: IoMode) -> std::io::Result<std::fs::File> {
    #[cfg(target_os = "linux")]
    {
        create_temp_linux(folder_path, io_mode)
    }
    #[cfg(not(target_os = "linux"))]
    {
        use crate::fs::shared;
        let file_path = folder_path.join(shared::generate_temp_file_name(12));
        create_temp_generic(&file_path, io_mode)
    }
}

/// Generic temporary file creation for Unix systems.
///
/// # Arguments
///
/// * `file_path` - Path where the temporary file should be created
/// * `io_mode` - I/O mode to use (buffered or unbuffered)
///
/// # Returns
///
/// Returns a `Result` containing the temporary file handle or an I/O error.
///
/// # Behavior
///
/// This function:
/// 1. Resolves relative paths to absolute paths using the current working directory
/// 2. Creates a new file with read/write permissions
/// 3. Sets secure file permissions (0o600) on non-WASI platforms
/// 4. Immediately unlinks the file from the filesystem
/// 5. Returns the file handle (file remains accessible until handle is dropped)
fn create_temp_generic(file_path: &Path, io_mode: IoMode) -> std::io::Result<std::fs::File> {
    let tmp;
    let file_path = if !file_path.is_absolute() {
        tmp = std::env::current_dir()?.join(file_path);
        tmp.as_path()
    } else {
        file_path
    };

    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);

    match io_mode {
        IoMode::Buffered => (),
        IoMode::Unbuffered => {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_DIRECT);
        }
    }

    #[cfg(not(target_os = "wasi"))]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }

    let f = options.open(file_path)?;
    let _ = std::fs::remove_file(file_path);
    Ok(f)
}

/// Linux-specific optimized temporary file creation using O_TMPFILE.
///
/// # Arguments
///
/// * `folder_path` - Directory where the temporary file should be created
/// * `io_mode` - I/O mode to use (buffered or unbuffered)
///
/// # Returns
///
/// Returns a `Result` containing the temporary file handle or an I/O error.
///
/// # Behavior
///
/// This function implements a two-stage approach:
/// 1. **First attempt**: Uses `O_TMPFILE` flag to create an anonymous temporary file
///    - More secure as the file never appears in the directory listing
///    - Automatically cleaned up when the file handle is closed
/// 2. **Fallback**: If `O_TMPFILE` is not supported, falls back to [`create_temp_generic`]
#[cfg(target_os = "linux")]
fn create_temp_linux(folder_path: &Path, io_mode: IoMode) -> std::io::Result<std::fs::File> {
    use crate::fs::shared::generate_temp_file_name;
    use std::os::unix::fs::OpenOptionsExt;

    let direct = if io_mode == IoMode::Unbuffered {
        libc::O_DIRECT
    } else {
        0
    };
    let res = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_TMPFILE | direct)
        .open(folder_path);
    if res.is_ok() {
        return res;
    }

    let file_path = folder_path.join(generate_temp_file_name(12));
    create_temp_generic(&file_path, io_mode)
}
