use crate::{ExclusiveIoBuffer, IoStream, SharedIoBuffer};

pub mod null_temp_store;

/// A trait for managing temporary file-like objects with budget-aware allocation.
///
/// `TemporaryFileStore` provides an abstraction for creating temporary storage objects
/// such as streams and buffers, while managing the overall temporary storage budget.
///
/// # Implementation Notes
///
/// Implementations should:
/// - Respect the provided size hints to optimize storage allocation
/// - Handle resource cleanup when temporary objects are dropped
/// - Manage the overall storage budget to prevent excessive resource usage
/// - Provide thread-safe access to temporary storage allocation
pub trait TemporaryFileStore: Send + Sync + 'static {
    /// Allocates a temporary I/O stream for sequential data writing, that can be later
    /// converted into a reader.
    ///
    /// # Arguments
    ///
    /// * `size_hint` - An optional hint indicating the expected size of the stream
    ///   in bytes. This helps implementations optimize allocation strategy.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a boxed `IoStream` on success, or an
    /// `std::io::Error` on failure.
    fn allocate_stream(&self, size_hint: Option<usize>) -> std::io::Result<Box<dyn IoStream>>;

    /// Allocates a temporary read/write buffer that supports reading and writing
    /// at arbitrary positions, and can be converted into a reader.
    ///
    /// # Arguments
    ///
    /// * `size_hint` - An optional hint indicating the expected size of the stream
    ///   in bytes. This helps implementations optimize allocation strategy.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a boxed `ExclusiveIoBuffer` on success, or an
    /// `std::io::Error` on failure.
    fn allocate_exclusive_buffer(
        &self,
        size_hint: Option<usize>,
    ) -> std::io::Result<Box<dyn ExclusiveIoBuffer>>;

    /// Allocates a shared buffer for concurrent read/write access at arbitrary positions.
    ///
    /// # Arguments
    ///
    /// * `size_hint` - An optional hint indicating the expected size of the buffer
    ///   in bytes. This helps implementations optimize allocation strategy.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a boxed `SharedIoBuffer` on success, or an
    /// `std::io::Error` on failure.
    fn allocate_shared_buffer(
        &self,
        size_hint: Option<usize>,
    ) -> std::io::Result<Box<dyn SharedIoBuffer>>;
}
