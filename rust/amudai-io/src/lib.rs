//! I/O abstractions:
//! - `ReadAt`: positional reader with the ability to fetch a specified byte range from a file/blob.
//! - `SealingWrite`: sequential writer with a `seal()` operation, committing the write activity.
//!
//! Provides a couple of simple implementations: memory-based and file-based.

use std::{ops::Range, sync::Arc};

use amudai_bytes::Bytes;

pub mod temp_file_store;
pub mod utils;

pub use temp_file_store::TemporaryFileStore;
pub use utils::{
    align_write::AlignWrite, precached_read::PrecachedReadAt, read_adapter::ReadAdapter,
    sliced_file::SlicedFile,
};

/// A trait representing a conceptual file or buffer that supports reading from arbitrary
/// positions.
pub trait ReadAt: Send + Sync + 'static {
    /// Returns the size of the underlying object.
    fn size(&self) -> std::io::Result<u64>;

    /// Reads a specified range of bytes from the object.
    ///
    /// **NOTE**: `read_at` should not return with a short read, unless end-of-file
    /// is encountered.
    ///
    /// # Arguments
    ///
    /// * `range` - A `Range<u64>` that specifies the start and end positions for reading.
    ///   The function may return fewer bytes than requested if the range extends beyond
    ///   the end of the object.
    ///
    /// # Returns
    ///
    /// * `std::io::Result<Bytes>` - The result containing the bytes read, or an error
    ///   if the operation fails.
    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes>;

    /// Retrieves the storage profile associated with this reader.
    fn storage_profile(&self) -> StorageProfile;
}

/// A trait representing a conceptual file or buffer that supports writing at arbitrary
/// positions.
///
/// This trait provides random-access write capabilities, allowing data to be written at any
/// position within the underlying storage medium. The storage will be automatically expanded
/// if writes occur beyond the current end of the storage.
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to support concurrent access from multiple threads
/// and should correctly handle concurrent writes to non-overlapping regions.
///
/// **NOTE**: uncoordinated concurrent writes to overlapping regions may have unspecified
/// effects on the content of the underlying storage, though they will not compromise the
/// writer's integrity.
pub trait WriteAt: Send + Sync + 'static {
    /// Writes the provided buffer at the specified position, expanding the underlying
    /// storage if necessary.
    ///
    /// **NOTE**: `write_at` should not return with a short write, upon success the entire
    /// buffer is written.
    ///
    /// # Arguments
    ///
    /// * `pos` - The position at which to write the buffer.
    /// * `buf` - The buffer to write.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    fn write_at(&self, pos: u64, buf: &[u8]) -> std::io::Result<()>;

    /// Retrieves the storage profile associated with this writer.
    fn storage_profile(&self) -> StorageProfile;
}

/// A trait for sequential writing with explicit sealing semantics.
///
/// This trait represents a write-only interface that supports appending data
/// sequentially and provides explicit control over the finalization process
/// through the [`seal`](SealingWrite::seal) operation. Unlike standard
/// [`std::io::Write`], this trait requires explicit sealing to ensure data
/// is properly flushed and committed to the underlying storage medium.
///
/// The sealing semantics are particularly useful for:
/// - Ensuring data consistency in distributed or transactional storage systems
/// - Providing atomic write operations where data becomes visible only after sealing
/// - Supporting storage backends that require explicit commit operations
///
/// # Thread Safety
///
/// Implementations must be [`Send`] to support transfer between threads, though
/// the trait does not require [`Sync`] as writers typically require exclusive
/// access through `&mut self`.
pub trait SealingWrite: Send {
    /// Writes the entire buffer to the underlying storage, appending it to any
    /// previously written data.
    ///
    /// This method ensures that all bytes in the provided buffer are written
    /// to the storage medium. Unlike [`std::io::Write::write`], this method
    /// guarantees that either all bytes are written successfully, or an error
    /// is returned with no partial writes.
    ///
    /// # Arguments
    ///
    /// * `buf` - The buffer containing the data to write. All bytes in this
    ///   buffer will be appended to the storage.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if all bytes were successfully written, or an
    /// [`std::io::Error`] if the write operation failed.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - The underlying storage medium encounters an I/O error
    /// - The writer has already been sealed
    /// - Insufficient storage space is available
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()>;

    /// Seals the writer, ensuring all buffered data is flushed and committed
    /// to the underlying storage medium.
    ///
    /// This method finalizes the write operation by:
    /// 1. Flushing any internally buffered data to the storage medium
    /// 2. Performing any necessary synchronization or commit operations
    /// 3. Making the written data durable and visible to readers
    ///
    /// Once sealed, the writer should not accept any further write operations.
    /// Calling [`write_all`](SealingWrite::write_all) after sealing may result
    /// in an error.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the sealing operation completed successfully, or an
    /// [`std::io::Error`] if the operation failed.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - The underlying storage medium encounters an I/O error during flushing
    /// - The writer has already been sealed
    /// - A commit or synchronization operation fails
    fn seal(&mut self) -> std::io::Result<()>;

    /// Retrieves the storage profile associated with this writer.
    ///
    /// The storage profile provides information about the optimal I/O
    /// characteristics of the underlying storage medium, such as preferred
    /// block sizes for efficient operations.
    ///
    /// # Returns
    ///
    /// A [`StorageProfile`] describing the performance characteristics and
    /// optimal usage patterns for this writer.
    fn storage_profile(&self) -> StorageProfile;
}

/// A trait representing a write-only stream that supports appending data
/// and conversion into various reader types.
///
/// This trait extends [`std::io::Write`] to provide additional functionality for
/// stream manipulation and conversion. It's designed for scenarios where you need
/// to write data sequentially and then convert the stream into a reader for
/// subsequent operations.
pub trait IoStream: std::io::Write + Send + Sync + 'static {
    /// Returns the current size (i.e., the end position) of the stream.
    ///
    /// This represents the total number of bytes that have been written to the stream.
    ///
    /// # Returns
    ///
    /// The size of the stream in bytes as a `u64`.
    fn current_size(&self) -> u64;

    /// Truncates the stream to the specified size.
    ///
    /// If the specified size is smaller than the current size, the stream will be
    /// truncated. If it's larger, the behavior depends on the implementation but
    /// typically this will be a no-op.
    ///
    /// # Arguments
    ///
    /// * `end_pos` - The new size of the stream in bytes.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the truncation operation.
    fn truncate(&mut self, end_pos: u64) -> std::io::Result<()>;

    /// Converts the stream into a positioned reader [`ReadAt`].
    ///
    /// This consumes the stream and returns a reader that supports random access
    /// to the written data. If the implementation is backed by a temporary resource
    /// (e.g., a memory buffer or temporary file), the allocated resources will be
    /// released when the returned reader is dropped.
    ///
    /// # Returns
    ///
    /// A `Result` containing a shared [`ReadAt`] on success, or an [`std::io::Error`]
    /// on failure.
    fn into_read_at(self: Box<Self>) -> std::io::Result<Arc<dyn ReadAt>>;

    /// Converts the stream into a sequential reader [`std::io::Read`].
    ///
    /// This consumes the stream and returns a reader that supports sequential access
    /// to the written data from the beginning. If the implementation is backed by a
    /// temporary resource (e.g., a memory buffer or temporary file), the allocated
    /// resources will be released when the returned reader is dropped.
    ///
    /// # Returns
    ///
    /// A `Result` containing a boxed [`std::io::Read`] on success,
    /// or an [`std::io::Error`] on failure.
    fn into_reader(self: Box<Self>) -> std::io::Result<Box<dyn std::io::Read>>;
}

/// A trait representing a read/write buffer that supports random access operations
/// and conversion into reader types.
///
/// This trait extends [`ReadAt`] to provide mutable operations on a buffer, including
/// writing at arbitrary positions and resizing. It's designed for scenarios where you
/// need both read and write access to a buffer with the ability to convert it into a
/// read-only format when modifications are complete.
///
/// # Thread Safety
///
/// While this trait requires `Send + Sync` (inherited from [`ReadAt`]), the mutable
/// operations (`set_size` and `write_at`) require exclusive access (`&mut self`),
/// ensuring thread safety through Rust's borrowing rules.
pub trait ExclusiveIoBuffer: ReadAt {
    /// Resizes the buffer to the specified size.
    ///
    /// If the new size is smaller than the current size, the buffer will be truncated.
    /// If it's larger, the buffer will be expanded, with the new bytes typically being
    /// zero-initialized (though this depends on the specific implementation).
    ///
    /// # Arguments
    ///
    /// * `size` - The new size of the buffer in bytes.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the resize operation.
    fn set_size(&mut self, size: u64) -> std::io::Result<()>;

    /// Writes data at the specified position, expanding the buffer if necessary.
    ///
    /// If the write operation extends beyond the current buffer size, the buffer
    /// will be automatically expanded to accommodate the new data. Any gaps between
    /// the previous end of the buffer and the write position will typically be
    /// zero-filled.
    ///
    /// # Arguments
    ///
    /// * `pos` - The byte position at which to write the data.
    /// * `buf` - The data to write.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the write operation.
    fn write_at(&mut self, pos: u64, buf: &[u8]) -> std::io::Result<()>;

    /// Converts the buffer into a positioned reader [`ReadAt`].
    ///
    /// This consumes the buffer and returns a reader that supports random access
    /// to the buffer's data. If the implementation is backed by a temporary resource
    /// (e.g., a memory buffer or temporary file), the allocated resources will be
    /// released when the returned reader is dropped.
    ///
    /// # Returns
    ///
    /// A `Result` containing a shared [`ReadAt`] on success, or an [`std::io::Error`]
    /// on failure.
    fn into_read_at(self: Box<Self>) -> std::io::Result<Arc<dyn ReadAt>>;

    /// Converts the buffer into a sequential reader [`std::io::Read`].
    ///
    /// This consumes the buffer and returns a reader that supports sequential access
    /// to the buffer's data from the beginning. If the implementation is backed by a
    /// temporary resource (e.g., a memory buffer or temporary file), the allocated
    /// resources will be released when the returned reader is dropped.
    ///
    /// # Returns
    ///
    /// A `Result` containing a boxed [`std::io::Read`] on success,
    /// or an [`std::io::Error`] on failure.
    fn into_reader(self: Box<Self>) -> std::io::Result<Box<dyn std::io::Read>>;
}

/// A trait representing a shared read/write buffer that supports concurrent random access
/// operations and conversion into reader types.
///
/// This trait combines [`ReadAt`] and [`WriteAt`] to provide a buffer that supports
/// both reading and writing at arbitrary positions with shared access semantics.
/// Unlike [`ExclusiveIoBuffer`], this trait allows concurrent access through shared
/// references (`&self`), making it suitable for multi-threaded scenarios.
///
/// # Thread Safety
///
/// Implementations must handle concurrent access safely. While concurrent reads and
/// writes to non-overlapping regions should work correctly, uncoordinated concurrent
/// writes to overlapping regions may have unspecified effects on the content (though
/// the buffer's integrity will be maintained).
///
/// # Comparison with ExclusiveIoBuffer
///
/// - [`ExclusiveIoBuffer`]: Exclusive access (`&mut self`) - single-threaded usage
/// - [`SharedIoBuffer`]: Shared access (`&self`) - multi-threaded usage
pub trait SharedIoBuffer: ReadAt + WriteAt {
    /// Resizes the buffer to the specified size.
    ///
    /// If the new size is smaller than the current size, the buffer will be truncated.
    /// If it's larger, the buffer will be expanded, with the new bytes typically being
    /// zero-initialized (though this depends on the specific implementation).
    ///
    /// # Arguments
    ///
    /// * `size` - The new size of the buffer in bytes.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the resize operation.
    fn set_size(&self, size: u64) -> std::io::Result<()>;

    /// Converts the buffer into a positioned reader [`ReadAt`].
    ///
    /// This consumes the buffer and returns a reader that supports random access
    /// to the buffer's data. If the implementation is backed by a temporary resource
    /// (e.g., a memory buffer or temporary file), the allocated resources will be
    /// released when the returned reader is dropped.
    ///
    /// # Returns
    ///
    /// A `Result` containing a shared [`ReadAt`] on success, or an [`std::io::Error`]
    /// on failure.
    fn into_read_at(self: Box<Self>) -> std::io::Result<Arc<dyn ReadAt>>;

    /// Converts the buffer into a sequential reader [`std::io::Read`].
    ///
    /// This consumes the buffer and returns a reader that supports sequential access
    /// to the buffer's data from the beginning. If the implementation is backed by a
    /// temporary resource (e.g., a memory buffer or temporary file), the allocated
    /// resources will be released when the returned reader is dropped.
    ///
    /// # Returns
    ///
    /// A `Result` containing a boxed [`std::io::Read`] on success,
    /// or an [`std::io::Error`] on failure.
    fn into_reader(self: Box<Self>) -> std::io::Result<Box<dyn std::io::Read>>;
}

/// Characterizes the performance aspects of the underlying storage implementation.
#[derive(Debug, Clone)]
pub struct StorageProfile {
    /// Suggested minimum size for an effective I/O request.
    /// Using buffers smaller than this size may be inefficient, as the round-trip time
    /// could dominate the overall I/O operation time.
    pub min_io_size: usize,

    /// Suggested maximum size for a single I/O request.
    /// Buffers larger than this size won't enhance performance and might even degrade
    /// the system's efficiency.
    pub max_io_size: usize,
}

impl StorageProfile {
    /// Clamps a given I/O size to the recommended range defined by this profile.
    ///
    /// This function ensures that the provided `size` is within the bounds of
    /// `min_io_size` and `max_io_size`, adjusting it if necessary. The minimum
    /// size is guaranteed to be at least 1, and the maximum size is guaranteed
    /// to be at least the minimum size.
    ///
    /// # Arguments
    ///
    /// * `size`: The desired I/O size to clamp.
    ///
    /// # Returns
    ///
    /// The clamped I/O size, which will be between `min_io_size` and `max_io_size`
    /// (inclusive), or at least 1 if `min_io_size` or `max_io_size` are zero.
    pub fn clamp_io_size(&self, size: usize) -> usize {
        let min = self.min_io_size.max(1).min(self.max_io_size);
        let max = self.max_io_size.max(1).max(min);
        size.clamp(min, max)
    }
}

impl Default for StorageProfile {
    fn default() -> StorageProfile {
        Self {
            min_io_size: 4 * 1024,
            max_io_size: 4 * 1024 * 1024,
        }
    }
}

impl<T> ReadAt for Arc<T>
where
    T: ReadAt + ?Sized,
{
    fn size(&self) -> std::io::Result<u64> {
        self.as_ref().size()
    }

    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes> {
        self.as_ref().read_at(range)
    }

    fn storage_profile(&self) -> StorageProfile {
        self.as_ref().storage_profile()
    }
}

impl<T> WriteAt for Arc<T>
where
    T: WriteAt + ?Sized,
{
    fn write_at(&self, pos: u64, buf: &[u8]) -> std::io::Result<()> {
        self.as_ref().write_at(pos, buf)
    }

    fn storage_profile(&self) -> StorageProfile {
        self.as_ref().storage_profile()
    }
}

impl<T> WriteAt for Box<T>
where
    T: WriteAt + ?Sized,
{
    fn write_at(&self, pos: u64, buf: &[u8]) -> std::io::Result<()> {
        self.as_ref().write_at(pos, buf)
    }

    fn storage_profile(&self) -> StorageProfile {
        self.as_ref().storage_profile()
    }
}

impl<T> SealingWrite for Box<T>
where
    T: SealingWrite + ?Sized,
{
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.as_mut().write_all(buf)
    }

    fn seal(&mut self) -> std::io::Result<()> {
        self.as_mut().seal()
    }

    fn storage_profile(&self) -> StorageProfile {
        self.as_ref().storage_profile()
    }
}
