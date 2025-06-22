//! Direct file access implementation for unbuffered I/O.
//!
//! This module provides the `DirectFile` struct which wraps a file for unbuffered access:
//! - On Windows: Uses `FILE_FLAG_NO_BUFFERING` for direct disk access
//! - On Linux: Uses `O_DIRECT` flag for direct disk access
//!
//! The implementation handles platform-specific alignment requirements automatically.
//!
//! # Example
//!
//! ```rust,no_run
//! use amudai_io_impl::DirectFile;
//! use amudai_io::ReadAt;
//! use std::path::Path;
//!
//! // Opening a file for direct read access
//! let direct_file = DirectFile::open("data.bin").expect("Failed to open file");
//! println!("File alignment requirement: {} bytes", direct_file.alignment());
//!
//! // Reading data with proper alignment handling
//! let data = direct_file.read_at(0..1024).expect("Failed to read data");
//! println!("Read {} bytes", data.len());
//!
//! // Creating a new file for direct write access
//! let write_file = DirectFile::create("output.bin").expect("Failed to create file");
//! // Use DirectFileWriter for writing operations
//! ```
//!
//! # Platform Notes
//!
//! ## Windows
//! - Uses `FILE_FLAG_NO_BUFFERING` which requires sector-aligned buffers
//! - Automatically detects sector size using `GetDiskFreeSpaceW`
//! - Falls back to 4KB alignment if detection fails
//!
//! ## Linux/Unix
//! - Uses `O_DIRECT` flag for unbuffered I/O
//! - Uses conservative 4KB alignment for compatibility
//! - May not be supported on all filesystems (e.g., some network filesystems)
//!
//! # Performance Considerations
//!
//! Direct I/O bypasses the OS page cache, which can provide performance benefits for:
//! - Large sequential reads/writes
//! - Applications that manage their own caching
//! - Situations where cache pollution should be avoided
//!
//! However, direct I/O requires careful attention to alignment and may perform
//! worse than buffered I/O for small, random access patterns.

use std::{
    fs::File,
    ops::Range,
    path::Path,
    sync::{Arc, OnceLock},
};

use amudai_bytes::Bytes;
use amudai_io::{ReadAt, SealingWrite, StorageProfile};

/// A file wrapper that provides unbuffered/direct access to the underlying storage.
///
/// On Windows, this uses `FILE_FLAG_NO_BUFFERING` and handles proper alignment.
/// On Linux, this uses `O_DIRECT` for direct disk access.
pub struct DirectFile {
    inner: Arc<DirectFileInner>,
}

struct DirectFileInner {
    file: File,
    #[cfg(windows)]
    sector_size: u32,
    size: OnceLock<u64>,
}

impl DirectFile {
    /// Creates a new `DirectFile` by opening an existing file for direct read access.
    ///
    /// # Arguments
    /// * `path` - Path to the file to open
    ///
    /// # Returns
    /// * `Ok(DirectFile)` if the file was successfully opened with direct access
    /// * `Err(std::io::Error)` if the file could not be opened or direct access is not supported
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<DirectFile> {
        let file = Self::open_direct_read(&path)?;

        #[cfg(windows)]
        let sector_size = Self::get_sector_size(&path)?;

        Ok(DirectFile {
            inner: Arc::new(DirectFileInner {
                file,
                #[cfg(windows)]
                sector_size,
                size: OnceLock::new(),
            }),
        })
    }

    /// Creates a new `DirectFile` by creating a new file for direct write access.
    ///
    /// # Arguments
    /// * `path` - Path where the new file should be created
    ///
    /// # Returns
    /// * `Ok(DirectFile)` if the file was successfully created with direct access
    /// * `Err(std::io::Error)` if the file could not be created or direct access is not supported
    pub fn create<P: AsRef<Path>>(path: P) -> std::io::Result<DirectFile> {
        let file = Self::open_direct_write(&path)?;

        #[cfg(windows)]
        let sector_size = Self::get_sector_size(&path)?;

        Ok(DirectFile {
            inner: Arc::new(DirectFileInner {
                file,
                #[cfg(windows)]
                sector_size,
                size: OnceLock::new(),
            }),
        })
    }

    /// Returns the sector size/alignment requirement for this file.
    ///
    /// On Windows, this returns the actual sector size of the underlying storage.
    /// On Linux, this returns a conservative alignment (typically 4KB).
    pub fn alignment(&self) -> u32 {
        #[cfg(windows)]
        {
            self.inner.sector_size
        }
        #[cfg(not(windows))]
        {
            4096 // Conservative alignment for most filesystems
        }
    }

    /// Rounds up a size to the next alignment boundary.
    pub fn round_up_to_alignment(&self, size: u64) -> u64 {
        let alignment = self.alignment() as u64;
        (size + alignment - 1) & !(alignment - 1)
    }

    /// Checks if a buffer and offset are properly aligned for direct I/O.
    pub fn is_aligned(&self, buffer_ptr: *const u8, offset: u64, length: usize) -> bool {
        let alignment = self.alignment() as u64;
        let buffer_aligned = (buffer_ptr as usize) % (alignment as usize) == 0;
        let offset_aligned = offset % alignment == 0;
        let length_aligned = (length as u64) % alignment == 0;

        buffer_aligned && offset_aligned && length_aligned
    }

    #[cfg(windows)]
    fn open_direct_read<P: AsRef<Path>>(path: P) -> std::io::Result<File> {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_NO_BUFFERING, FILE_FLAG_RANDOM_ACCESS,
        };

        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_NO_BUFFERING | FILE_FLAG_RANDOM_ACCESS)
            .open(path)
    }

    #[cfg(not(windows))]
    fn open_direct_read<P: AsRef<Path>>(path: P) -> std::io::Result<File> {
        use std::os::unix::fs::OpenOptionsExt;

        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)
    }

    #[cfg(windows)]
    fn open_direct_write<P: AsRef<Path>>(path: P) -> std::io::Result<File> {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_NO_BUFFERING, FILE_FLAG_SEQUENTIAL_SCAN,
        };

        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .custom_flags(FILE_FLAG_NO_BUFFERING | FILE_FLAG_SEQUENTIAL_SCAN)
            .open(path)
    }

    #[cfg(not(windows))]
    fn open_direct_write<P: AsRef<Path>>(path: P) -> std::io::Result<File> {
        use std::os::unix::fs::OpenOptionsExt;

        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)
    }
    #[cfg(windows)]
    fn get_sector_size<P: AsRef<Path>>(path: P) -> std::io::Result<u32> {
        use std::ffi::OsStr;
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceW;

        let path = path.as_ref();

        // Get the root path (drive letter)
        let root_path = if let Some(root) = path.ancestors().last() {
            root
        } else {
            return Ok(4096); // Default sector size
        };

        let root_path_wide: Vec<u16> = OsStr::new(root_path).encode_wide().chain(Some(0)).collect();

        let mut sectors_per_cluster = 0u32;
        let mut bytes_per_sector = 0u32;
        let mut free_clusters = 0u32;
        let mut total_clusters = 0u32;

        let result = unsafe {
            GetDiskFreeSpaceW(
                root_path_wide.as_ptr(),
                &mut sectors_per_cluster,
                &mut bytes_per_sector,
                &mut free_clusters,
                &mut total_clusters,
            )
        };

        if result != 0 && bytes_per_sector > 0 {
            Ok(bytes_per_sector)
        } else {
            // Fallback to conservative default
            Ok(4096)
        }
    }

    fn get_size(&self) -> std::io::Result<u64> {
        if let Some(&size) = self.inner.size.get() {
            Ok(size)
        } else {
            let size = self.inner.file.metadata()?.len();
            let _ = self.inner.size.set(size);
            Ok(size)
        }
    }

    fn adjust_read_range(&self, range: Range<u64>) -> std::io::Result<Range<u64>> {
        let size = self.get_size()?;
        if range.start >= size || range.start == range.end {
            return Ok(0..0);
        }
        let range = range.start..std::cmp::min(range.end, size);
        Ok(range)
    }
}

impl ReadAt for DirectFile {
    fn size(&self) -> std::io::Result<u64> {
        self.get_size()
    }

    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes> {
        let range = self.adjust_read_range(range)?;
        if range.is_empty() {
            return Ok(Bytes::new());
        }

        let length = (range.end - range.start) as usize;
        let alignment = self.alignment() as usize;
        // For direct I/O, we need properly aligned buffers
        let aligned_length = ((length + alignment - 1) / alignment) * alignment;
        let aligned_offset = (range.start / alignment as u64) * alignment as u64;

        // Allocate aligned buffer
        let layout =
            std::alloc::Layout::from_size_align(aligned_length, alignment).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid alignment")
            })?;

        let aligned_buf = unsafe {
            let ptr = std::alloc::alloc_zeroed(layout);
            if ptr.is_null() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    "Failed to allocate aligned buffer",
                ));
            }
            std::slice::from_raw_parts_mut(ptr, aligned_length)
        };

        // Perform the aligned read
        let result = {
            #[cfg(windows)]
            {
                amudai_io::file::file_read_at_exact(&self.inner.file, aligned_offset, aligned_buf)
            }
            #[cfg(not(windows))]
            {
                use std::os::unix::fs::FileExt;
                self.inner.file.read_exact_at(aligned_buf, aligned_offset)
            }
        };

        let final_result = match result {
            Ok(()) => {
                // Extract the requested portion from the aligned read
                let start_in_buffer = (range.start - aligned_offset) as usize;
                let end_in_buffer = start_in_buffer + length;

                let mut result_bytes = amudai_bytes::BytesMut::zeroed(length);
                result_bytes.copy_from_slice(&aligned_buf[start_in_buffer..end_in_buffer]);
                Ok(result_bytes.into())
            }
            Err(e) => Err(e),
        };

        // Clean up aligned buffer
        unsafe {
            std::alloc::dealloc(aligned_buf.as_mut_ptr(), layout);
        }

        final_result
    }

    fn storage_profile(&self) -> StorageProfile {
        StorageProfile {
            min_io_size: self.alignment() as usize,
            max_io_size: 1024 * 1024, // 1MB max for direct I/O
        }
    }
}

/// Writer implementation for DirectFile
pub struct DirectFileWriter {
    inner: Arc<DirectFileInner>,
    position: u64,
}

impl DirectFileWriter {
    /// Creates a new DirectFileWriter from a DirectFile
    pub fn new(direct_file: DirectFile) -> DirectFileWriter {
        DirectFileWriter {
            inner: direct_file.inner,
            position: 0,
        }
    }

    /// Returns the alignment requirement for this writer
    pub fn alignment(&self) -> u32 {
        #[cfg(windows)]
        {
            self.inner.sector_size
        }
        #[cfg(not(windows))]
        {
            4096
        }
    }
}

impl SealingWrite for DirectFileWriter {
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let alignment = self.alignment() as usize;
        let aligned_length = ((buf.len() + alignment - 1) / alignment) * alignment;

        // Create aligned buffer
        let layout =
            std::alloc::Layout::from_size_align(aligned_length, alignment).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid alignment")
            })?;

        let aligned_buf = unsafe {
            let ptr = std::alloc::alloc_zeroed(layout);
            if ptr.is_null() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    "Failed to allocate aligned buffer",
                ));
            }
            let slice = std::slice::from_raw_parts_mut(ptr, aligned_length);
            slice[..buf.len()].copy_from_slice(buf);
            slice
        };

        let result = {
            #[cfg(windows)]
            {
                amudai_io::file::file_write_at(&self.inner.file, self.position, aligned_buf)
            }
            #[cfg(not(windows))]
            {
                use std::os::unix::fs::FileExt;
                self.inner.file.write_all_at(aligned_buf, self.position)
            }
        };

        // Clean up
        unsafe {
            std::alloc::dealloc(aligned_buf.as_mut_ptr(), layout);
        }

        match result {
            Ok(()) => {
                self.position += buf.len() as u64;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn seal(&mut self) -> std::io::Result<()> {
        // For direct I/O, we need to ensure all data is flushed
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            use windows_sys::Win32::Storage::FileSystem::FlushFileBuffers;

            let handle = self.inner.file.as_raw_handle();
            let result = unsafe { FlushFileBuffers(handle as _) };
            if result == 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
        #[cfg(not(windows))]
        {
            use std::os::unix::io::AsRawFd;

            let fd = self.inner.file.as_raw_fd();
            let result = unsafe { libc::fsync(fd) };
            if result != 0 {
                return Err(std::io::Error::last_os_error());
            }
        }

        Ok(())
    }

    fn storage_profile(&self) -> StorageProfile {
        StorageProfile {
            min_io_size: self.alignment() as usize,
            max_io_size: 1024 * 1024, // 1MB max for direct I/O
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_direct_file_creation() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test_direct.bin");

        // Create a regular file first
        std::fs::write(&file_path, b"Hello, World!").expect("Failed to write test file");

        // Try to open it with DirectFile (may fail on some filesystems/platforms)
        match DirectFile::open(&file_path) {
            Ok(direct_file) => {
                assert!(direct_file.alignment() > 0);
                println!(
                    "Direct file opened with alignment: {}",
                    direct_file.alignment()
                );
            }
            Err(e) => {
                println!("Direct file access not supported: {}", e);
                // This is acceptable as not all filesystems support direct I/O
            }
        }
    }

    #[test]
    fn test_alignment_calculations() {
        // Test with a mock alignment
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test_alignment.bin");
        std::fs::write(&file_path, b"test").expect("Failed to write test file");

        if let Ok(direct_file) = DirectFile::open(&file_path) {
            let alignment = direct_file.alignment();

            // Test round_up_to_alignment
            assert_eq!(direct_file.round_up_to_alignment(1), alignment as u64);
            assert_eq!(
                direct_file.round_up_to_alignment(alignment as u64),
                alignment as u64
            );
            assert_eq!(
                direct_file.round_up_to_alignment(alignment as u64 + 1),
                2 * alignment as u64
            );
        }
    }
}
