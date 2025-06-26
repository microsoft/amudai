//! I/O abstractions:
//! - `ReadAt`: positional reader with the ability to fetch a specified byte range from a file/blob.
//! - `SealingWrite`: sequential writer with a `seal()` operation, committing the write activity.
//!
//! Provides a couple of simple implementations: memory-based and file-based.

use std::{ops::Range, sync::Arc};

use amudai_bytes::Bytes;

pub mod file;
pub mod io_extensions;
pub mod memory;
pub mod precached_read;
pub mod read_adapter;
pub mod sliced_read;
pub mod temp_file_store;
pub mod utils;

/// Random-Access Reader.
pub trait ReadAt: Send + Sync + 'static {
    /// Returns the size of the underlying object.
    fn size(&self) -> std::io::Result<u64>;

    /// Reads a specified range of bytes from the object.
    ///
    /// # Arguments
    ///
    /// * `range` - A `Range<u64>` that specifies the start and end positions for reading.
    ///   The function may return fewer bytes than requested if the range extends beyond
    ///   the end of the object.
    ///
    /// # Returns
    ///
    /// * `std::io::Result<Bytes>` - The result containing the bytes read, or an error if the operation fails.
    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes>;

    /// Retrieves the storage profile associated with this reader.
    fn storage_profile(&self) -> StorageProfile;
}

/// Sequential Writer with Sealing (Flush and Commit).
pub trait SealingWrite: Send {
    /// Writes (appends) the entire buffer to the underlying
    /// storage artifact.
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()>;

    /// Seals the writer, ensuring all data is flushed and committed.
    fn seal(&mut self) -> std::io::Result<()>;

    /// Retrieves the storage profile associated with this writer.
    fn storage_profile(&self) -> StorageProfile;
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
