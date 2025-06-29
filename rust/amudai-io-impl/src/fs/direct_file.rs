//! Direct I/O file operations with proper alignment handling.
//!
//! This module provides file readers and writers that perform direct (unbuffered) I/O operations,
//! which bypass the operating system's page cache for better performance in certain scenarios.
//! Direct I/O requires all operations to be aligned to specific boundaries (I/O granularity),
//! which this module handles mostly transparently.
//!
//! # Key Components
//!
//! - [`DirectFileReader`]: A file reader that handles arbitrary reads by internally aligning
//!   to I/O granularity boundaries and slicing the results appropriately.
//! - [`DirectFileWriter`]: A buffered file writer that ensures all writes are properly aligned
//!   and handles alignment padding automatically.

use std::{
    fs::File,
    io::Write,
    ops::Range,
    sync::{Arc, OnceLock},
};

use amudai_bytes::{
    Bytes, BytesMut,
    align::{align_down_u64, align_up_u64, is_aligned_u64},
};
use amudai_io::{ReadAt, StorageProfile, verify};

use crate::fs::platform;

/// A file reader that performs direct (unbuffered) I/O operations.
///
/// This reader wraps an Arc<File> and ensures all I/O operations are properly
/// aligned. It supports arbitrary reads by internally aligning the requested range,
/// performing the aligned read, and then slicing the result to match the original
/// request.
#[derive(Clone)]
pub struct DirectFileReader {
    /// The underlying file handle wrapped in an Arc for shared ownership.
    file: Arc<File>,
    /// Cached file size to avoid repeated metadata calls.
    size: OnceLock<u64>,
    /// The I/O granularity required for direct I/O operations on this file.
    /// All reads must be aligned to this boundary.
    io_granularity: u64,
}

impl DirectFileReader {
    /// Creates a new DirectFileReader from an existing File.
    ///
    /// The I/O granularity is automatically determined from the platform and file.
    /// If a known file size is provided, it will be cached to avoid metadata calls.
    ///
    /// # Arguments
    ///
    /// * `file` - The file handle to wrap, must be opened with direct I/O flags
    /// * `known_size` - Optional pre-known file size to cache
    ///
    /// # Note
    ///
    /// The actual file opening with direct I/O flags should be done by the caller
    /// and passed to this constructor.
    pub fn new(file: Arc<File>, known_size: Option<u64>) -> DirectFileReader {
        let io_granularity = platform::get_io_granularity(&file).max(1);
        assert!(io_granularity.is_power_of_two());
        DirectFileReader {
            file,
            size: {
                let size = OnceLock::new();
                if let Some(known_size) = known_size {
                    let _ = size.set(known_size);
                }
                size
            },
            io_granularity,
        }
    }

    /// Creates a DirectFileReader from a File, wrapping it in an Arc.
    ///
    /// This is a convenience method that converts the file into an Arc and calls
    /// [`new`](Self::new) with no pre-known size.
    ///
    /// # Arguments
    ///
    /// * `file` - The file to wrap, will be converted to `Arc<File>`
    ///
    /// # Note
    ///
    /// The actual file opening with direct I/O flags should be done by the caller
    /// and passed to this constructor.
    pub fn from_file(file: impl Into<Arc<File>>) -> DirectFileReader {
        Self::new(file.into(), None)
    }

    /// Returns a reference to the underlying file handle.
    ///
    /// This provides access to the wrapped `Arc<File>` for operations that
    /// need direct file access.
    pub fn inner(&self) -> &Arc<File> {
        &self.file
    }

    /// Consumes the reader and returns the underlying file handle.
    ///
    /// This extracts the `Arc<File>` from the reader, consuming the reader
    /// in the process.
    pub fn into_inner(self) -> Arc<File> {
        self.file
    }

    /// Returns the I/O granularity for this file.
    ///
    /// This is the alignment boundary that all direct I/O operations must
    /// respect. All read positions and sizes are internally aligned to
    /// this granularity.
    pub fn io_granularity(&self) -> u64 {
        self.io_granularity
    }

    /// Gets the size of the file, caching it for subsequent calls.
    ///
    /// This method will query the file's metadata on the first call and cache
    /// the result. Subsequent calls will return the cached value without
    /// additional system calls.
    ///
    /// # Returns
    ///
    /// The size of the file in bytes, or an I/O error if the metadata
    /// cannot be retrieved.
    fn get_size(&self) -> std::io::Result<u64> {
        if let Some(&size) = self.size.get() {
            Ok(size)
        } else {
            let size = self.file.metadata()?.len();
            let _ = self.size.set(size);
            Ok(size)
        }
    }

    /// Adjusts the requested read range to ensure it doesn't exceed file bounds.
    ///
    /// This method clamps the read range to the actual file size and handles
    /// edge cases like reads starting beyond the end of the file or empty ranges.
    ///
    /// # Arguments
    ///
    /// * `range` - The requested read range
    ///
    /// # Returns
    ///
    /// An adjusted range that is guaranteed to be within file bounds,
    /// or an empty range (0..0) if the request is invalid.
    fn adjust_read_range(&self, range: Range<u64>) -> std::io::Result<Range<u64>> {
        let size = self.get_size()?;
        if range.start >= size || range.start == range.end {
            return Ok(0..0);
        }
        let range = range.start..std::cmp::min(range.end, size);
        Ok(range)
    }

    /// Performs the actual aligned read operation.
    ///
    /// This method assumes the provided range is already aligned to I/O granularity
    /// boundaries and performs a direct read operation. It creates an appropriately
    /// aligned buffer and reads the data directly from the file.
    ///
    /// # Arguments
    ///
    /// * `aligned_range` - A range that is aligned to I/O granularity boundaries
    ///
    /// # Returns
    ///
    /// The read data as `Bytes`, or an I/O error if the read fails.
    ///
    /// # Panics
    ///
    /// This method assumes the range is properly aligned and may panic or
    /// fail if alignment requirements are not met.
    fn read_at_aligned(&self, aligned_range: Range<u64>) -> std::io::Result<Bytes> {
        let len = (aligned_range.end - aligned_range.start) as usize;

        // Create an aligned buffer for direct I/O
        let mut buf = BytesMut::with_capacity_and_alignment(len, self.io_granularity as usize);
        buf.resize(len, 0);

        amudai_io::file::file_read_at_exact(&self.file, aligned_range.start, &mut buf)?;

        Ok(buf.into())
    }

    /// Performs a read that handles arbitrary ranges by aligning internally.
    ///
    /// This is the core method that enables arbitrary reads on direct I/O files.
    /// It takes an unaligned range, expands it to meet alignment requirements,
    /// performs the aligned read, and then slices the result to return only
    /// the originally requested data.
    ///
    /// # Arguments
    ///
    /// * `range` - The arbitrary range to read (doesn't need to be aligned)
    ///
    /// # Returns
    ///
    /// The requested data as `Bytes`, properly sliced from the aligned read.
    ///
    /// # Process
    ///
    /// 1. Align the start position down to the nearest boundary
    /// 2. Align the end position up to the nearest boundary  
    /// 3. Perform an aligned read of the expanded range
    /// 4. Slice the result to extract only the originally requested data
    fn read_at_impl(&self, range: Range<u64>) -> std::io::Result<Bytes> {
        if range.is_empty() {
            return Ok(Bytes::new());
        }

        let original_start = range.start;
        let original_end = range.end;
        let original_len = (original_end - original_start) as usize;

        // Align the read range to IO_GRANULARITY boundaries
        let aligned_start = align_down_u64(original_start, self.io_granularity);
        let aligned_end = align_up_u64(original_end, self.io_granularity);

        // Perform the aligned read
        let aligned_data = self.read_at_aligned(aligned_start..aligned_end)?;

        // Calculate the offset within the aligned buffer where our data starts
        let offset_in_aligned = (original_start - aligned_start) as usize;

        // Slice the aligned data to get only the requested range
        let result = aligned_data.slice(offset_in_aligned..offset_in_aligned + original_len);

        Ok(result)
    }
}

impl ReadAt for DirectFileReader {
    fn size(&self) -> std::io::Result<u64> {
        self.get_size()
    }

    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes> {
        verify!(range.end >= range.start);
        let range = self.adjust_read_range(range)?;
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        self.read_at_impl(range)
    }

    fn storage_profile(&self) -> StorageProfile {
        StorageProfile::default()
    }
}

/// A buffered file writer that performs direct (unbuffered) I/O operations.
///
/// This writer maintains an internal buffer and ensures all writes to the underlying
/// file are properly aligned to I/O granularity boundaries. It automatically handles
/// alignment padding and buffering to provide a standard `Write` interface while
/// meeting direct I/O requirements.
///
/// # Buffering Strategy
///
/// The writer uses an internal buffer that accumulates writes until either:
/// - The buffer reaches its capacity
/// - The buffered data reaches an aligned size
/// - The writer is explicitly flushed or closed
///
/// When flushing, the buffer is padded with zeros to meet alignment requirements
/// if necessary.
pub struct DirectFileWriter {
    /// The underlying file handle wrapped in an Arc for shared ownership.
    file: Arc<File>,
    /// The file position where the next buffer flush will write.
    /// This is always aligned to I/O granularity boundaries.
    buffer_pos: u64,
    /// Internal buffer for accumulating writes before flushing to disk.
    /// This buffer is properly aligned for direct I/O operations.
    buffer: BytesMut,
    /// The I/O granularity required for direct I/O operations on this file.
    /// All writes must be aligned to this boundary.
    io_granularity: u64,
}

impl DirectFileWriter {
    /// The capacity of the internal write buffer in bytes (64 KB).
    ///
    /// This buffer accumulates writes before flushing them to disk in
    /// properly aligned chunks.
    pub const BUFFER_CAPACITY: usize = 64 * 1024;

    /// Creates a new DirectFileWriter from an existing File.
    ///
    /// The writer will start at an aligned position based on the current file size.
    /// If the file is not empty and its size is not aligned, the writer will start
    /// at the next aligned boundary.
    ///
    /// # Arguments
    ///
    /// * `file` - The file handle to wrap, should be opened with direct I/O flags
    ///
    /// # Returns
    ///
    /// A new `DirectFileWriter` or an I/O error if file metadata cannot be read.
    ///
    /// # Note
    ///
    /// The file should be opened with direct I/O flags for optimal performance.
    pub fn new(file: Arc<File>) -> std::io::Result<Self> {
        let io_granularity = platform::get_io_granularity(&file).max(1);
        assert!(io_granularity.is_power_of_two());

        let current_size = file.metadata()?.len();
        // Ensure we start at an aligned position
        let buffer_pos = align_up_u64(current_size, io_granularity);
        Ok(DirectFileWriter {
            file,
            buffer_pos,
            buffer: BytesMut::with_capacity_and_alignment(
                Self::BUFFER_CAPACITY,
                io_granularity as usize,
            ),
            io_granularity,
        })
    }

    /// Creates a DirectFileWriter from a File, wrapping it in an Arc.
    ///
    /// This is a convenience method that converts the file into an Arc and
    /// calls [`new`](Self::new).
    ///
    /// # Arguments
    ///
    /// * `file` - The file to wrap, will be converted to `Arc<File>`
    ///
    /// # Returns
    ///
    /// A new `DirectFileWriter` or an I/O error if initialization fails.
    pub fn from_file(file: impl Into<Arc<File>>) -> std::io::Result<Self> {
        Self::new(file.into())
    }

    /// Returns the I/O granularity for this file.
    ///
    /// This is the alignment boundary that all direct I/O operations must
    /// respect. The writer ensures all file operations are aligned to this
    /// granularity.
    pub fn io_granularity(&self) -> u64 {
        self.io_granularity
    }

    /// Returns the current logical write position.
    ///
    /// This represents the total number of bytes that have been written
    /// through this writer, including both flushed and buffered data.
    /// The position advances as data is written, regardless of whether
    /// it has been flushed to disk yet.
    pub fn position(&self) -> u64 {
        self.buffer_pos + self.buffer.len() as u64
    }

    /// Truncates the file to the specified size.
    ///
    /// After truncation, the writer's position is set to `end_pos`, which
    /// becomes the new end of the file. Any subsequent writes will extend
    /// the file from this position.
    ///
    /// # Arguments
    ///
    /// * `end_pos` - The target file size in bytes after truncation
    pub fn truncate(&mut self, end_pos: u64) -> std::io::Result<()> {
        self.flush_buffer()?;
        self.set_position(end_pos)
    }

    /// Writes an aligned buffer directly to the file at the specified position.
    ///
    /// This method bypasses the internal buffering and writes directly to the file.
    /// All alignment requirements must be met by the caller:
    /// - Write position must be aligned to I/O granularity
    /// - Buffer size must be a multiple of I/O granularity  
    /// - Buffer memory address must be aligned to I/O granularity
    ///
    /// # Arguments
    ///
    /// * `pos` - The file position to write at (must be aligned)
    /// * `buffer` - The data to write (must meet alignment requirements)
    ///
    /// # Returns
    ///
    /// Success or an I/O error if the write fails or alignment requirements
    /// are not met.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` error if any alignment requirements are violated.
    pub fn write_aligned_buffer(&self, pos: u64, buffer: &[u8]) -> std::io::Result<()> {
        if !self.buffer.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "write_aligned_buffer unexpected (buffer_pos = {}, buffer len = {})",
                    self.buffer_pos,
                    self.buffer.len()
                ),
            ));
        }

        // Verify alignment requirements
        if !is_aligned_u64(pos, self.io_granularity) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Write position {} is not aligned to {}",
                    pos, self.io_granularity
                ),
            ));
        }

        if !is_aligned_u64(buffer.len() as u64, self.io_granularity) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Buffer size {} is not a multiple of {}",
                    buffer.len(),
                    self.io_granularity
                ),
            ));
        }

        if !is_aligned_u64(buffer.as_ptr() as u64, self.io_granularity) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Buffer is not properly aligned",
            ));
        }

        amudai_io::file::file_write_at(&self.file, pos, buffer)
    }

    /// Closes the writer and ensures the final file size is properly set.
    ///
    /// This method performs the following operations:
    /// 1. Flushes any remaining buffered data to disk (with padding if needed)
    /// 2. Returns the underlying file handle and final logical file size
    ///
    /// After calling this method, the writer is consumed and cannot be used
    /// for further operations.
    ///
    /// # Returns
    ///
    /// A tuple containing the underlying `Arc<File>` and the final logical file size,
    /// or an I/O error if the flush or file size setting fails.
    ///
    /// # Note
    ///
    /// This method flushes any remaining buffered data and sets the final file size.
    /// Due to padding, the actual file size on disk may be larger than the number
    /// of the logically written bytes.
    pub fn close(mut self) -> std::io::Result<FileWriteResult> {
        let logical_size = self.done()?;
        let file_size = self.file.metadata()?.len();
        Ok(FileWriteResult {
            file: self.file.clone(),
            logical_size,
            file_size,
        })
    }
}

impl DirectFileWriter {
    /// Flushes any buffered data to the file.
    ///
    /// This method writes the current buffer contents to disk, padding with
    /// zeros if necessary to meet I/O granularity alignment requirements.
    /// After flushing, the buffer is cleared and the buffer position is
    /// updated to reflect the new file position.
    ///
    /// # Process
    ///
    /// 1. If buffer is empty, returns immediately
    /// 2. Calculates aligned size by padding buffer to granularity boundary
    /// 3. Extends buffer with zeros if padding is needed
    /// 4. Writes the aligned buffer to the file
    /// 5. Updates buffer position and clears the buffer
    ///
    /// # Returns
    ///
    /// Success or an I/O error if the write operation fails.
    fn flush_buffer(&mut self) -> std::io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Pad buffer to alignment boundary
        let current_len = self.buffer.len();
        let aligned_len = align_up_u64(current_len as u64, self.io_granularity) as usize;

        // Extend buffer with zeros to reach alignment
        if current_len < aligned_len {
            self.buffer.resize(aligned_len, 0);
        }

        amudai_io::file::file_write_at(&self.file, self.buffer_pos, &self.buffer)?;

        let bytes_written = self.buffer.len() as u64;
        self.buffer_pos += bytes_written;
        self.buffer.clear();
        Ok(())
    }

    /// Sets the writer's logical position and prepares the buffer for subsequent writes.
    ///
    /// # Position Management
    ///
    /// ## Forward Seek (pos > current position):
    /// 1. Calculates aligned buffer position
    /// 2. If the target position is not aligned, fills buffer with zeros up to the target
    /// 3. Updates buffer position and file size
    ///
    /// ## Backward Seek (pos < current position):
    /// 1. Reads an aligned block from the file at the new position
    /// 2. Truncates the buffer to contain only data up to the target position
    /// 3. Updates buffer position and truncates the file
    ///
    /// # Preconditions
    ///
    /// - The internal buffer must be empty (enforced by assertion)
    ///
    /// These preconditions ensure that no unbuffered data is lost during the position change.
    ///
    /// # Parameters
    ///
    /// * `pos` - The new logical write position in bytes
    ///
    /// # Note
    ///
    /// This is an internal method used by higher-level operations like `truncate`.
    fn set_position(&mut self, pos: u64) -> std::io::Result<()> {
        if pos == self.position() {
            return Ok(());
        }
        assert!(self.buffer.is_empty());
        assert_eq!(self.position(), self.buffer_pos);

        let new_buffer_pos = align_down_u64(pos, self.io_granularity);
        if pos > self.buffer_pos {
            if new_buffer_pos < pos {
                self.buffer.resize((pos - new_buffer_pos) as usize, 0);
            }
        } else {
            self.buffer.resize(self.io_granularity as usize, 0);
            amudai_io::file::file_read_at_exact(&self.file, new_buffer_pos, &mut self.buffer)?;
            self.buffer.truncate((pos - new_buffer_pos) as usize);
        }
        self.buffer_pos = new_buffer_pos;
        self.file.set_len(new_buffer_pos)?;
        assert_eq!(self.position(), pos);
        Ok(())
    }

    /// Flushes the writer and ensures the final file size is properly set.
    ///
    /// This method performs the following operations:
    /// 1. Flushes any remaining buffered data to disk (with padding if needed)
    /// 2. Returns the final logical file size
    fn done(&mut self) -> std::io::Result<u64> {
        let logical_size = self.position();
        // Flush any remaining buffered data
        self.flush_buffer()?;
        Ok(logical_size)
    }
}

impl Write for DirectFileWriter {
    /// Writes data to the file through the internal buffer.
    ///
    /// This implementation provides a standard `Write` interface while handling
    /// all the complexity of direct I/O alignment requirements internally.
    ///
    /// # Strategy
    ///
    /// - For small writes or when buffer is not empty: data goes to internal buffer
    /// - For large aligned writes when buffer is empty: writes directly to file
    /// - Automatically flushes buffer when it reaches capacity or alignment boundaries
    ///
    /// # Arguments
    ///
    /// * `buf` - The data to write
    ///
    /// # Returns
    ///
    /// The number of bytes written, or an I/O error if the operation fails.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        if self.buffer.is_empty()
            && is_aligned_u64(buf.len() as u64, self.io_granularity)
            && is_aligned_u64(buf.as_ptr() as u64, self.io_granularity)
        {
            amudai_io::file::file_write_at(&self.file, self.buffer_pos, buf)?;
            self.buffer_pos += buf.len() as u64;
            return Ok(buf.len());
        }

        let mut bytes_written = 0;
        let mut remaining = buf;

        while !remaining.is_empty() {
            // Calculate how much space is available in the current buffer
            let buffer_space = Self::BUFFER_CAPACITY - self.buffer.len();

            if buffer_space == 0 {
                // Buffer is full, flush it
                self.flush_buffer()?;
                continue;
            }

            // Take as much as we can fit in the buffer
            let to_copy = std::cmp::min(remaining.len(), buffer_space);
            self.buffer.extend_from_slice(&remaining[..to_copy]);

            bytes_written += to_copy;
            remaining = &remaining[to_copy..];

            if is_aligned_u64(self.buffer.len() as u64, self.io_granularity) {
                self.flush_buffer()?;
            }
        }

        Ok(bytes_written)
    }

    /// Flushes the writer.
    ///
    /// For `DirectFileWriter`, this is a no-op because the writer manages
    /// its own buffering strategy and flushes automatically when needed.
    /// Data is guaranteed to be written when the buffer reaches capacity,
    /// becomes aligned, or when the writer is closed.
    ///
    /// # Note
    ///
    /// Unlike typical writers, this doesn't force an immediate flush to disk.
    /// Use the `close()` for that purpose.
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for DirectFileWriter {
    fn drop(&mut self) {
        let _ = self.done();
    }
}

/// The outcome of a file write operation.
///
/// This struct encapsulates the results of completing a direct I/O file write operation,
/// providing both the file handle and size information about the write results.
///
/// # Size Distinction
///
/// Due to direct I/O alignment requirements, there's an important distinction between
/// the logical size (actual data written) and the physical file size on disk:
///
/// - **Logical Size**: The exact number of bytes that were logically written through
///   the writer interface. This represents the meaningful data content.
/// - **File Size**: The actual size of the file on disk, which may be larger due to
///   alignment padding required by direct I/O operations.
#[derive(Clone)]
pub struct FileWriteResult {
    /// The completed file handle.
    ///
    /// This is the same file that was used for writing, now available for
    /// further operations such as reading or additional writes. The file
    /// is wrapped in an `Arc` to allow shared ownership.
    pub file: Arc<File>,

    /// The logical number of bytes written.
    ///
    /// This represents the exact amount of meaningful data that was written
    /// through the writer interface, without any alignment padding. This is
    /// the size that should be used for logical operations and data validation.
    pub logical_size: u64,

    /// The actual file size on disk.
    ///
    /// Due to direct I/O alignment requirements, the physical file may be larger
    /// than the logical size. This field represents the actual space consumed
    /// on the storage device, including any padding bytes that were added to
    /// meet alignment constraints.
    ///
    /// This size is always >= `logical_size` and is typically aligned to the
    /// I/O granularity boundary of the storage device.
    pub file_size: u64,
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use amudai_bytes::buffer::AlignedByteVec;
    use amudai_io::ReadAt;

    use crate::fs::{DirectFileReader, DirectFileWriter, IoMode, direct_file::FileWriteResult};

    #[test]
    fn test_from_file() {
        let mut file =
            crate::fs::platform::create_temporary_in(&std::env::temp_dir(), IoMode::Unbuffered)
                .unwrap();
        let mut buf = AlignedByteVec::with_capacity_and_alignment(16 * 1024, 4096);
        buf.resize(16 * 1024, 0);
        file.write_all(&buf).unwrap();
        file.set_len(4096 * 4).unwrap();
        let f = DirectFileReader::from_file(file);
        let _buf = f.read_at(4096..5000).unwrap();
    }

    #[test]
    fn test_direct_file_writer() {
        use std::sync::Arc;

        // Create a temporary file for testing
        let file =
            crate::fs::platform::create_temporary_in(&std::env::temp_dir(), IoMode::Unbuffered)
                .unwrap();
        let file_arc = Arc::new(file);

        // Test writing some data
        let test_data = b"Hello, World! This is a test of the DirectFileWriter.";
        let mut writer = DirectFileWriter::from_file(file_arc.clone()).unwrap();
        let io_granularity = writer.io_granularity();

        // Write the data
        writer.write_all(test_data).unwrap();
        writer.flush().unwrap();

        // Verify logical size
        assert_eq!(writer.position(), test_data.len() as u64);

        // Close the writer
        let size = writer.close().unwrap().file_size;
        assert_eq!(size, io_granularity);

        // Read back the data to verify
        let reader = DirectFileReader::from_file(file_arc);
        let read_data = reader.read_at(0..test_data.len() as u64).unwrap();
        assert_eq!(&read_data[..], test_data);
    }

    #[test]
    fn test_write_aligned_buffer() {
        use amudai_bytes::BytesMut;
        use std::sync::Arc;

        // Create a temporary file for testing
        let file =
            crate::fs::platform::create_temporary_in(&std::env::temp_dir(), IoMode::Unbuffered)
                .unwrap();
        let file_arc = Arc::new(file);

        let writer = DirectFileWriter::from_file(file_arc.clone()).unwrap();

        // Create an aligned buffer
        let mut aligned_buf = BytesMut::with_capacity_and_alignment(4096, 4096);
        aligned_buf.resize(4096, 0x42); // Fill with 0x42

        // Write the aligned buffer directly
        writer.write_aligned_buffer(0, &aligned_buf).unwrap();

        // Close the writer
        let size = writer.close().unwrap().file_size;
        assert_eq!(size, 4096);

        // Read back and verify
        let reader = DirectFileReader::from_file(file_arc);
        assert_eq!(reader.size().unwrap(), 4096);
        let read_data = reader.read_at(0..4096).unwrap();
        assert_eq!(read_data.len(), 4096);
        assert!(read_data.iter().all(|&b| b == 0x42));
    }

    #[test]
    fn test_truncate_to_smaller_size() {
        use std::sync::Arc;

        // Create a temporary file and write some data
        let file =
            crate::fs::platform::create_temporary_in(&std::env::temp_dir(), IoMode::Unbuffered)
                .unwrap();
        let file_arc = Arc::new(file);

        let mut writer = DirectFileWriter::from_file(file_arc.clone()).unwrap();
        let test_data = vec![0x42u8; 8192]; // 8KB of data
        writer.write_all(&test_data).unwrap();

        let original_pos = writer.position();
        assert_eq!(original_pos, 8192);

        // Truncate to smaller size
        let truncate_pos = 4096;
        writer.truncate(truncate_pos).unwrap();

        // Verify position is updated
        assert_eq!(writer.position(), truncate_pos);

        // Close and verify file size
        let FileWriteResult {
            file,
            logical_size: _,
            file_size,
        } = writer.close().unwrap();
        assert_eq!(file_size, truncate_pos);

        // Verify file content
        let reader = DirectFileReader::from_file(file);
        assert_eq!(reader.size().unwrap(), truncate_pos);
        let read_data = reader.read_at(0..truncate_pos).unwrap();
        assert_eq!(read_data.len(), truncate_pos as usize);
        assert!(read_data.iter().all(|&b| b == 0x42));
    }

    #[test]
    fn test_truncate_to_larger_size() {
        use std::sync::Arc;

        // Create a temporary file and write some data
        let file =
            crate::fs::platform::create_temporary_in(&std::env::temp_dir(), IoMode::Unbuffered)
                .unwrap();
        let file_arc = Arc::new(file);

        let mut writer = DirectFileWriter::from_file(file_arc.clone()).unwrap();
        let test_data = vec![0x42u8; 4096]; // 4KB of data
        writer.write_all(&test_data).unwrap();

        let original_pos = writer.position();
        assert_eq!(original_pos, 4096);

        // Truncate to larger size
        let truncate_pos = 8192;
        writer.truncate(truncate_pos).unwrap();

        // Verify position is updated
        assert_eq!(writer.position(), truncate_pos);

        // Write more data at the new position
        let additional_data = vec![0x24u8; 1024];
        writer.write_all(&additional_data).unwrap();

        // Close and verify
        let file = writer.close().unwrap().file;

        // Verify file content
        let reader = DirectFileReader::from_file(file);

        // Check original data is still there
        let original_read = reader.read_at(0..4096).unwrap();
        assert!(original_read.iter().all(|&b| b == 0x42));

        // Check zeros were inserted in the gap
        let gap_read = reader.read_at(4096..8192).unwrap();
        assert!(gap_read.iter().all(|&b| b == 0));

        // Check new data was written
        let new_read = reader.read_at(8192..9216).unwrap();
        assert!(new_read.iter().all(|&b| b == 0x24));
    }

    #[test]
    fn test_truncate_to_current_position() {
        use std::sync::Arc;

        let file =
            crate::fs::platform::create_temporary_in(&std::env::temp_dir(), IoMode::Unbuffered)
                .unwrap();
        let file_arc = Arc::new(file);

        let mut writer = DirectFileWriter::from_file(file_arc.clone()).unwrap();
        let test_data = vec![0x42u8; 4096];
        writer.write_all(&test_data).unwrap();

        let current_pos = writer.position();

        // Truncate to current position (should be no-op)
        writer.truncate(current_pos).unwrap();

        // Position should remain the same
        assert_eq!(writer.position(), current_pos);

        // File should be unchanged
        let final_file = writer.close().unwrap().file;
        let reader = DirectFileReader::from_file(final_file);
        let read_data = reader.read_at(0..current_pos).unwrap();
        assert_eq!(read_data.len(), current_pos as usize);
        assert!(read_data.iter().all(|&b| b == 0x42));
    }

    #[test]
    fn test_truncate_with_buffered_data() {
        use std::sync::Arc;

        let file =
            crate::fs::platform::create_temporary_in(&std::env::temp_dir(), IoMode::Unbuffered)
                .unwrap();
        let file_arc = Arc::new(file);

        let mut writer = DirectFileWriter::from_file(file_arc.clone()).unwrap();

        // Write some data that gets flushed
        let flushed_data = vec![0x42u8; 4096];
        writer.write_all(&flushed_data).unwrap();

        // Write some data that stays in buffer (less than alignment)
        let buffered_data = vec![0x24u8; 512];
        writer.write_all(&buffered_data).unwrap();

        let total_written = writer.position();
        assert_eq!(total_written, 4096 + 512);

        // Truncate to a position that includes both flushed and buffered data
        let truncate_pos = 4096 + 256; // Keep half the buffered data
        writer.truncate(truncate_pos).unwrap();

        // Verify position
        assert_eq!(writer.position(), truncate_pos);

        // Write more data to verify everything works
        let new_data = vec![0x11u8; 100];
        writer.write_all(&new_data).unwrap();

        let final_file = writer.close().unwrap().file;
        let reader = DirectFileReader::from_file(final_file);

        // Verify the data
        let flushed_read = reader.read_at(0..4096).unwrap();
        assert!(flushed_read.iter().all(|&b| b == 0x42));

        let partial_buffered_read = reader.read_at(4096..truncate_pos).unwrap();
        assert!(partial_buffered_read.iter().all(|&b| b == 0x24));

        let new_data_read = reader.read_at(truncate_pos..truncate_pos + 100).unwrap();
        assert!(new_data_read.iter().all(|&b| b == 0x11));
    }

    #[test]
    fn test_truncate_to_zero() {
        use std::sync::Arc;

        let file =
            crate::fs::platform::create_temporary_in(&std::env::temp_dir(), IoMode::Unbuffered)
                .unwrap();
        let file_arc = Arc::new(file);

        let mut writer = DirectFileWriter::from_file(file_arc.clone()).unwrap();

        // Write some data
        let test_data = vec![0x42u8; 8192];
        writer.write_all(&test_data).unwrap();
        assert_eq!(writer.position(), 8192);

        // Truncate to zero
        writer.truncate(0).unwrap();
        assert_eq!(writer.position(), 0);

        // Write new data from the beginning
        let new_data = vec![0x24u8; 1024];
        writer.write_all(&new_data).unwrap();

        let io_granularity = writer.io_granularity();
        let final_file = writer.close().unwrap().file;
        let reader = DirectFileReader::from_file(final_file);

        // Verify only the new data exists
        let read_data = reader.read_at(0..1024).unwrap();
        assert!(read_data.iter().all(|&b| b == 0x24));

        // Ensure file size is what we expect
        assert_eq!(reader.size().unwrap(), io_granularity);
    }

    #[test]
    fn test_truncate_to_unaligned_positions() {
        use std::sync::Arc;

        let file =
            crate::fs::platform::create_temporary_in(&std::env::temp_dir(), IoMode::Unbuffered)
                .unwrap();
        let file_arc = Arc::new(file);

        let mut writer = DirectFileWriter::from_file(file_arc.clone()).unwrap();
        let io_granularity = writer.io_granularity();

        // Write data larger than granularity
        let test_data = vec![0x42u8; (io_granularity * 2) as usize];
        writer.write_all(&test_data).unwrap();

        // Truncate to unaligned position
        let unaligned_pos = io_granularity + 123; // Not aligned to granularity
        writer.truncate(unaligned_pos).unwrap();
        assert_eq!(writer.position(), unaligned_pos);

        // Write more data
        let additional_data = vec![0x24u8; 456];
        writer.write_all(&additional_data).unwrap();

        let final_file = writer.close().unwrap().file;
        let reader = DirectFileReader::from_file(final_file);

        // Verify data integrity at unaligned boundary
        let before_truncate = reader.read_at(0..unaligned_pos).unwrap();
        assert!(before_truncate.iter().all(|&b| b == 0x42));

        let after_truncate = reader.read_at(unaligned_pos..unaligned_pos + 456).unwrap();
        assert!(after_truncate.iter().all(|&b| b == 0x24));
    }

    #[test]
    fn test_truncate_multiple_times() {
        use std::sync::Arc;

        let file =
            crate::fs::platform::create_temporary_in(&std::env::temp_dir(), IoMode::Unbuffered)
                .unwrap();
        let file_arc = Arc::new(file);

        let mut writer = DirectFileWriter::from_file(file_arc.clone()).unwrap();

        // Write initial data
        let data1 = vec![0x11u8; 8192];
        writer.write_all(&data1).unwrap();
        assert_eq!(writer.position(), 8192);

        // First truncation - shrink
        writer.truncate(4096).unwrap();
        assert_eq!(writer.position(), 4096);

        // Write more data
        let data2 = vec![0x22u8; 2048];
        writer.write_all(&data2).unwrap();
        assert_eq!(writer.position(), 6144);

        // Second truncation - shrink again
        writer.truncate(3072).unwrap();
        assert_eq!(writer.position(), 3072);

        // Third truncation - grow
        writer.truncate(10240).unwrap();
        assert_eq!(writer.position(), 10240);

        // Write final data
        let data3 = vec![0x33u8; 1024];
        writer.write_all(&data3).unwrap();

        let final_file = writer.close().unwrap().file;
        let reader = DirectFileReader::from_file(final_file);

        // Verify data patterns
        let region1 = reader.read_at(0..3072).unwrap();
        assert!(region1[0..3072].iter().all(|&b| b == 0x11)); // Original data up to final truncation

        let region2 = reader.read_at(3072..10240).unwrap();
        assert!(region2.iter().all(|&b| b == 0)); // Zero-filled gap

        let region3 = reader.read_at(10240..11264).unwrap();
        assert!(region3.iter().all(|&b| b == 0x33)); // Final data
    }

    #[test]
    fn test_truncate_empty_file() {
        use std::sync::Arc;

        let file =
            crate::fs::platform::create_temporary_in(&std::env::temp_dir(), IoMode::Unbuffered)
                .unwrap();
        let file_arc = Arc::new(file);

        let mut writer = DirectFileWriter::from_file(file_arc.clone()).unwrap();

        // Truncate empty file to zero (should be no-op)
        writer.truncate(0).unwrap();
        assert_eq!(writer.position(), 0);

        // Truncate empty file to non-zero (should extend)
        writer.truncate(2048).unwrap();
        assert_eq!(writer.position(), 2048);

        // Write some data
        let data = vec![0x42u8; 512];
        writer.write_all(&data).unwrap();

        let final_file = writer.close().unwrap().file;
        let reader = DirectFileReader::from_file(final_file);

        // Check zero-filled area
        let zeros = reader.read_at(0..2048).unwrap();
        assert!(zeros.iter().all(|&b| b == 0));

        // Check written data
        let written = reader.read_at(2048..2560).unwrap();
        assert!(written.iter().all(|&b| b == 0x42));
    }

    #[test]
    fn test_truncate_boundary_conditions() {
        use std::sync::Arc;

        let file =
            crate::fs::platform::create_temporary_in(&std::env::temp_dir(), IoMode::Unbuffered)
                .unwrap();
        let file_arc = Arc::new(file);

        let mut writer = DirectFileWriter::from_file(file_arc.clone()).unwrap();
        let io_granularity = writer.io_granularity();

        // Write data to align with granularity boundaries
        let aligned_size = io_granularity * 3;
        let test_data = vec![0x42u8; aligned_size as usize];
        writer.write_all(&test_data).unwrap();

        // Test truncating exactly at granularity boundaries
        writer.truncate(io_granularity * 2).unwrap();
        assert_eq!(writer.position(), io_granularity * 2);

        // Test truncating just before boundary
        writer.truncate(io_granularity * 2 - 1).unwrap();
        assert_eq!(writer.position(), io_granularity * 2 - 1);

        // Test truncating just after boundary
        writer.truncate(io_granularity * 2 + 1).unwrap();
        assert_eq!(writer.position(), io_granularity * 2 + 1);

        let final_file = writer.close().unwrap().file;
        let reader = DirectFileReader::from_file(final_file);

        // Verify data integrity
        let read_data = reader.read_at(0..io_granularity * 2 + 1).unwrap();
        assert_eq!(read_data.len(), (io_granularity * 2 + 1) as usize);

        // Original data should be intact up to the first truncation point
        assert!(
            read_data[0..(io_granularity * 2 - 1) as usize]
                .iter()
                .all(|&b| b == 0x42)
        );

        // The bytes after the truncation point should be zero-filled
        assert_eq!(read_data[(io_granularity * 2 - 1) as usize], 0); // Truncated byte
        assert_eq!(read_data[(io_granularity * 2) as usize], 0); // Extended byte 1
        assert_eq!(read_data[(io_granularity * 2 + 1 - 1) as usize], 0); // Extended byte 2
    }

    #[test]
    fn test_truncate_with_large_buffer() {
        use std::sync::Arc;

        let file =
            crate::fs::platform::create_temporary_in(&std::env::temp_dir(), IoMode::Unbuffered)
                .unwrap();
        let file_arc = Arc::new(file);

        let mut writer = DirectFileWriter::from_file(file_arc.clone()).unwrap();

        // Write a large amount of data that will require multiple buffer flushes
        let large_data = vec![0x42u8; DirectFileWriter::BUFFER_CAPACITY * 3];
        writer.write_all(&large_data).unwrap();
        let _total_written = writer.position();

        // Truncate to middle of the data
        let truncate_pos = DirectFileWriter::BUFFER_CAPACITY as u64 + 1000;
        writer.truncate(truncate_pos).unwrap();
        assert_eq!(writer.position(), truncate_pos);

        // Write new data
        let new_data = vec![0x24u8; 2000];
        writer.write_all(&new_data).unwrap();

        let final_file = writer.close().unwrap().file;
        let reader = DirectFileReader::from_file(final_file);

        // Verify truncated data
        let original_data = reader.read_at(0..truncate_pos).unwrap();
        assert!(original_data.iter().all(|&b| b == 0x42));

        // Verify new data
        let new_data_read = reader.read_at(truncate_pos..truncate_pos + 2000).unwrap();
        assert!(new_data_read.iter().all(|&b| b == 0x24));
    }
}
