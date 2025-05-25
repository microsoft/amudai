use std::sync::Arc;

use crate::ReadAt;

pub mod null_temp_store;

/// The `TemporaryFileStore` trait provides temporary file-like objects
/// to consumers while managing the overall temporary storage budget.
pub trait TemporaryFileStore: Send + Sync + 'static {
    /// Allocates a temporary write-only stream that can be appended to
    /// and later converted into a reader.
    ///
    /// # Arguments
    ///
    /// * `size_hint` - An optional hint for the expected size of the stream.
    ///
    /// # Returns
    ///
    /// A `Result` containing a boxed `TemporaryWritable` on success, or an
    /// `io::Error` on failure.
    fn allocate_writable(
        &self,
        size_hint: Option<usize>,
    ) -> std::io::Result<Box<dyn TemporaryWritable>>;

    /// Allocates a temporary read/write buffer that supports appending, reading,
    /// and writing at arbitrary positions, and can be converted into a reader.
    ///
    /// # Arguments
    ///
    /// * `size_hint` - An optional hint for the expected size of the buffer.
    ///
    /// # Returns
    ///
    /// A `Result` containing a boxed `TemporaryBuffer` on success, or an `io::Error`
    /// on failure.
    fn allocate_buffer(
        &self,
        size_hint: Option<usize>,
    ) -> std::io::Result<Box<dyn TemporaryBuffer>>;
}

/// A trait representing a temporary write-only stream that can be appended to
/// and converted into a reader.
pub trait TemporaryWritable: std::io::Write + Send + Sync + 'static {
    /// Returns the current size (i.e., the end position) of the stream.
    ///
    /// # Returns
    ///
    /// The size of the stream as a `u64`.
    fn current_size(&self) -> u64;

    /// Truncates the stream to the specified size.
    ///
    /// # Arguments
    ///
    /// * `end_pos` - The position to truncate the stream to.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    fn truncate(&mut self, end_pos: u64) -> std::io::Result<()>;

    /// Converts the stream into a reader (`ReadAtBlocking`).
    /// The allocated storage will be released when this reader is dropped.
    ///
    /// # Returns
    ///
    /// A `Result` containing a boxed `ReadAtBlocking` on success,
    /// or an `io::Error` on failure.
    fn into_read_at(self: Box<Self>) -> std::io::Result<Arc<dyn ReadAt>>;

    /// Converts the stream into `std::io::Read`.
    /// The allocated storage will be released when this reader is dropped.
    ///
    /// # Returns
    ///
    /// A `Result` containing a boxed `std::io::Read` on success,
    /// or an `io::Error` on failure.
    fn into_reader(self: Box<Self>) -> std::io::Result<Box<dyn std::io::Read>>;
}

/// A trait representing a temporary read/write buffer that supports appending, reading,
/// and writing at arbitrary positions, and can be converted into a reader.
pub trait TemporaryBuffer: TemporaryWritable + ReadAt {
    /// Writes the provided buffer at the specified position, expanding the underlying
    /// storage if necessary.
    ///
    /// # Arguments
    ///
    /// * `pos` - The position at which to write the buffer.
    /// * `buf` - The buffer to write.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    fn write_at(&mut self, pos: u64, buf: &[u8]) -> std::io::Result<()>;
}
