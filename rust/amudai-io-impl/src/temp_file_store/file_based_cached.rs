//! File-based temporary file store with capacity management.
//!
//! This module provides a file-based implementation of the [`TemporaryFileStore`] trait
//! that creates temporary files on the local filesystem with budget tracking and capacity
//! management. The implementation ensures that temporary files are cleaned up automatically
//! when dropped.

use std::{io, path::Path, sync::Arc};

use amudai_io::temp_file_store::{TemporaryBuffer, TemporaryFileStore, TemporaryWritable};

use crate::temp_file_store::file_based_common::{LocalTempContainer, LocalTempFile};

/// A file-based temporary file store with capacity management and budget tracking.
///
/// `LocalTempFileStore` provides a thread-safe implementation of [`TemporaryFileStore`]
/// that creates temporary files on the local filesystem. It maintains a budget to track
/// allocated space and ensures that temporary files are automatically cleaned up when
/// no longer referenced.
#[derive(Clone)]
pub struct LocalTempFileStore(Arc<LocalTempContainer>);

impl LocalTempFileStore {
    /// Creates a new temporary file store with the specified capacity and optional parent path.
    ///
    /// The store will create a temporary directory to hold temporary files, and maintain
    /// a budget to track allocated space against the specified capacity. When the capacity
    /// is exceeded, subsequent allocations will fail with an I/O error.
    ///
    /// # Arguments
    ///
    /// * `capacity` - The maximum total size in bytes that can be allocated by this store
    /// * `parent_path` - Optional parent directory for the temporary directory. If `None`,
    ///   the system temporary directory will be used
    ///
    /// # Returns
    ///
    /// Returns a new `LocalTempFileStore` instance, or an I/O error if the temporary
    /// directory cannot be created.
    pub fn new(capacity: u64, parent_path: Option<&Path>) -> io::Result<LocalTempFileStore> {
        let container = LocalTempContainer::with_capacity(capacity, parent_path)?;
        Ok(LocalTempFileStore(Arc::new(container)))
    }

    /// Returns the path to the temporary directory used by this store.
    ///
    /// All temporary files created by this store will be located within this directory
    /// or its subdirectories.
    pub fn path(&self) -> &Path {
        self.0.path()
    }

    /// Returns the amount of space remaining in the budget.
    ///
    /// This represents the number of bytes that can still be allocated before
    /// reaching the capacity limit. The available space decreases as temporary
    /// files are created and increases as they are dropped or truncated.
    ///
    /// # Returns
    ///
    /// The number of bytes still available for allocation.
    pub fn available_space(&self) -> u64 {
        self.0.budget().remaining()
    }

    /// Creates a new temporary file with optional size hint for budget allocation.
    ///
    /// # Arguments
    ///
    /// * `size_hint` - Optional hint about the initial size of the file in bytes.
    ///   If provided, budget will be pre-allocated for this amount
    ///
    /// # Returns
    ///
    /// Returns a new `LocalTempFile` instance, or an I/O error if the file cannot
    /// be created or if there is insufficient budget.
    fn create_temp_file(&self, size_hint: Option<usize>) -> io::Result<LocalTempFile> {
        let size_hint = size_hint.unwrap_or(0) as u64;
        let mut allocation = self.0.budget().allocate(0)?;
        allocation.reserve(size_hint)?;
        if size_hint >= 1024 * 1024 {
            allocation.set_reservation_slice(128 * 1024);
        }
        let file = tempfile::tempfile_in(self.path())?;
        Ok(LocalTempFile::new(self.0.clone(), allocation, file))
    }
}

impl TemporaryFileStore for LocalTempFileStore {
    /// Allocates a new temporary file for sequential writes.
    ///
    /// Creates a new temporary file that implements [`TemporaryWritable`], allowing
    /// sequential writing operations. The file is created in the store's temporary
    /// directory and is automatically cleaned up when dropped.
    ///
    /// # Arguments
    ///
    /// * `size_hint` - Optional hint about the initial size of the file in bytes.
    ///   This is used for budget pre-allocation but doesn't limit the actual file size
    fn allocate_writable(
        &self,
        size_hint: Option<usize>,
    ) -> io::Result<Box<dyn TemporaryWritable>> {
        self.create_temp_file(size_hint).map(|f| Box::new(f) as _)
    }

    /// Allocates a new temporary buffer that supports random access writing.
    ///
    /// Creates a new temporary file that implements [`TemporaryBuffer`], allowing
    /// both sequential and random access write operations. This is useful for
    /// scenarios where data needs to be written at specific offsets.
    ///
    /// # Arguments
    ///
    /// * `size_hint` - Optional hint about the initial size of the buffer in bytes.
    ///   This is used for budget pre-allocation but doesn't limit the actual buffer size
    fn allocate_buffer(&self, size_hint: Option<usize>) -> io::Result<Box<dyn TemporaryBuffer>> {
        self.create_temp_file(size_hint).map(|f| Box::new(f) as _)
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use amudai_io::temp_file_store::TemporaryFileStore;

    use super::LocalTempFileStore;

    /// Tests basic store creation and space tracking functionality.
    #[test]
    fn test_create_store() -> io::Result<()> {
        let store = LocalTempFileStore::new(100000, None)?;
        println!("{}", store.path().to_str().unwrap());
        assert_eq!(store.available_space(), 100000);
        Ok(())
    }

    #[test]
    fn test_write_temp_file_sequential() -> io::Result<()> {
        let store = LocalTempFileStore::new(10000000, None)?;
        let mut temp_file = store.allocate_writable(None)?;
        let const_buf = (0..100u8).collect::<Vec<_>>();
        for _ in 0..100 {
            temp_file.write_all(&const_buf)?;
        }
        assert_eq!(temp_file.current_size(), 10000);
        assert_eq!(store.available_space(), 10000000 - 10000);

        let mut reader = temp_file.into_reader()?;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        assert_eq!(buf.len(), 10000);
        drop(reader);
        assert_eq!(store.available_space(), 10000000);
        Ok(())
    }

    #[test]
    fn test_write_temp_file_sparse() -> io::Result<()> {
        let store = LocalTempFileStore::new(10000000, None)?;
        let mut temp_file = store.allocate_buffer(None)?;
        let const_buf = (0..100u8).collect::<Vec<_>>();
        for i in 0..100 {
            temp_file.write_at(i * 1000, &const_buf)?;
        }
        assert_eq!(temp_file.current_size(), 99100);
        assert_eq!(store.available_space(), 10000000 - 99100);

        let mut reader = temp_file.into_reader()?;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        assert_eq!(buf.len(), 99100);
        assert_eq!(&buf[..4], &[0, 1, 2, 3]);
        assert_eq!(&buf[100..104], &[0, 0, 0, 0]);
        drop(reader);
        assert_eq!(store.available_space(), 10000000);
        Ok(())
    }

    #[test]
    fn test_truncate_basic() -> io::Result<()> {
        let store = LocalTempFileStore::new(1000000, None)?;
        let mut temp_file = store.allocate_writable(None)?;

        // Write some data
        let data = b"Hello, World! This is test data for truncation.";
        temp_file.write_all(data)?;
        assert_eq!(temp_file.current_size(), data.len() as u64);

        // Truncate to smaller size
        temp_file.truncate(20)?;
        assert_eq!(temp_file.current_size(), 20);

        // Verify the data is actually truncated
        let mut reader = temp_file.into_reader()?;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        assert_eq!(buf.len(), 20);
        assert_eq!(&buf, &data[..20]);

        Ok(())
    }

    /// Tests truncation to zero size.
    /// Verifies that truncating a file to empty works correctly.
    #[test]
    fn test_truncate_to_zero() -> io::Result<()> {
        let store = LocalTempFileStore::new(1000000, None)?;
        let mut temp_file = store.allocate_writable(None)?;

        // Write some data
        let data = b"Hello, World!";
        temp_file.write_all(data)?;
        assert_eq!(temp_file.current_size(), data.len() as u64);

        // Truncate to zero
        temp_file.truncate(0)?;
        assert_eq!(temp_file.current_size(), 0);

        // Verify the file is empty
        let mut reader = temp_file.into_reader()?;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        assert_eq!(buf.len(), 0);

        Ok(())
    }

    /// Tests that truncation has no effect when the target size is larger than current size.
    #[test]
    fn test_truncate_no_effect_when_larger() -> io::Result<()> {
        let store = LocalTempFileStore::new(1000000, None)?;
        let mut temp_file = store.allocate_writable(None)?;

        // Write some data
        let data = b"Hello, World!";
        temp_file.write_all(data)?;
        let original_size = temp_file.current_size(); // Try to truncate to larger size (should have no effect)
        temp_file.truncate(original_size + 10)?;
        assert_eq!(temp_file.current_size(), original_size);

        // Verify the data is unchanged
        let mut reader = temp_file.into_reader()?;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        assert_eq!(buf.len(), data.len());
        assert_eq!(&buf, data);

        Ok(())
    }

    /// Tests that truncation to the same size has no effect.
    #[test]
    fn test_truncate_to_same_size() -> io::Result<()> {
        let store = LocalTempFileStore::new(1000000, None)?;
        let mut temp_file = store.allocate_writable(None)?;

        // Write some data
        let data = b"Hello, World!";
        temp_file.write_all(data)?;
        let original_size = temp_file.current_size(); // Truncate to same size (should have no effect)
        temp_file.truncate(original_size)?;
        assert_eq!(temp_file.current_size(), original_size);

        // Verify the data is unchanged
        let mut reader = temp_file.into_reader()?;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        assert_eq!(buf.len(), data.len());
        assert_eq!(&buf, data);

        Ok(())
    }

    /// Tests multiple consecutive truncation operations.
    #[test]
    fn test_truncate_multiple_times() -> io::Result<()> {
        let store = LocalTempFileStore::new(1000000, None)?;
        let mut temp_file = store.allocate_writable(None)?;

        // Write some data
        let data = b"Hello, World! This is a longer test string for multiple truncations.";
        temp_file.write_all(data)?;
        assert_eq!(temp_file.current_size(), data.len() as u64);

        // First truncation
        temp_file.truncate(50)?;
        assert_eq!(temp_file.current_size(), 50);

        // Second truncation
        temp_file.truncate(30)?;
        assert_eq!(temp_file.current_size(), 30);

        // Third truncation
        temp_file.truncate(10)?;
        assert_eq!(temp_file.current_size(), 10);

        // Verify the final data
        let mut reader = temp_file.into_reader()?;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        assert_eq!(buf.len(), 10);
        assert_eq!(&buf, &data[..10]);

        Ok(())
    }

    /// Tests truncation behavior after sparse writes.
    /// Verifies that truncation works correctly with files that have gaps.
    #[test]
    fn test_truncate_after_sparse_writes() -> io::Result<()> {
        let store = LocalTempFileStore::new(1000000, None)?;
        let mut temp_file = store.allocate_buffer(None)?;

        // Write sparse data
        temp_file.write_at(0, b"start")?;
        temp_file.write_at(100, b"middle")?;
        temp_file.write_at(200, b"end")?;

        let initial_size = temp_file.current_size();
        assert_eq!(initial_size, 203); // 200 + 3 bytes for "end"

        // Truncate to middle of file
        temp_file.truncate(150)?;
        assert_eq!(temp_file.current_size(), 150);

        // Verify the data
        let reader = temp_file.into_read_at()?;
        let start_data = reader.read_at(0..5)?;
        assert_eq!(start_data.as_ref(), b"start");

        let middle_data = reader.read_at(100..106)?;
        assert_eq!(middle_data.as_ref(), b"middle");

        // End should be truncated away
        let end_data = reader.read_at(145..150)?;
        assert_eq!(end_data.len(), 5);
        // Should be zeros since we truncated before the "end" data
        assert_eq!(end_data.as_ref(), &[0, 0, 0, 0, 0]);

        Ok(())
    }

    /// Tests that the write position is properly handled after truncation.
    #[test]
    fn test_truncate_write_position_adjustment() -> io::Result<()> {
        let store = LocalTempFileStore::new(1000000, None)?;
        let mut temp_file = store.allocate_writable(None)?;

        // Write some data
        let data = b"Hello, World! This is test data.";
        temp_file.write_all(data)?;

        // Current write position should be at end
        assert_eq!(temp_file.current_size(), data.len() as u64);

        // Truncate to middle
        temp_file.truncate(10)?;
        assert_eq!(temp_file.current_size(), 10); // Write more data - should continue from current position
        temp_file.write_all(b" NEW")?;
        assert_eq!(temp_file.current_size(), 14);

        // Verify the data
        let mut reader = temp_file.into_reader()?;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        assert_eq!(buf.len(), 14);
        assert_eq!(&buf[..10], &data[..10]);
        assert_eq!(&buf[10..], b" NEW");

        Ok(())
    }

    /// Tests that truncation properly updates the budget tracking.
    #[test]
    fn test_truncate_budget_tracking() -> io::Result<()> {
        let store = LocalTempFileStore::new(1000, None)?;
        let mut temp_file = store.allocate_writable(None)?;

        // Write data to consume budget
        let data = vec![42u8; 800];
        temp_file.write_all(&data)?;
        assert_eq!(temp_file.current_size(), 800);
        assert_eq!(store.available_space(), 200);

        // Truncate to smaller size - should free up budget
        temp_file.truncate(400)?;
        assert_eq!(temp_file.current_size(), 400);
        assert_eq!(store.available_space(), 600);

        // Truncate to zero - should free all budget
        temp_file.truncate(0)?;
        assert_eq!(temp_file.current_size(), 0);
        assert_eq!(store.available_space(), 1000);

        Ok(())
    }

    /// Tests truncation on large files.
    /// Verifies that truncation works efficiently with larger amounts of data.
    #[test]
    fn test_truncate_large_file() -> io::Result<()> {
        let store = LocalTempFileStore::new(10_000_000, None)?;
        let mut temp_file = store.allocate_writable(None)?;

        // Write a large amount of data
        let chunk = vec![42u8; 1024];
        for _ in 0..1000 {
            temp_file.write_all(&chunk)?;
        }

        let initial_size = temp_file.current_size();
        assert_eq!(initial_size, 1024 * 1000);

        // Truncate to 1/4 size
        let target_size = initial_size / 4;
        temp_file.truncate(target_size)?;
        assert_eq!(temp_file.current_size(), target_size);

        // Verify the data
        let mut reader = temp_file.into_reader()?;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        assert_eq!(buf.len(), target_size as usize);

        // All bytes should be 42
        assert!(buf.iter().all(|&b| b == 42));

        Ok(())
    }

    /// Tests truncation behavior on empty files.
    /// Verifies that truncation operations on empty files are handled correctly.
    #[test]
    fn test_truncate_empty_file() -> io::Result<()> {
        let store = LocalTempFileStore::new(1000000, None)?;
        let mut temp_file = store.allocate_writable(None)?;

        // Truncate empty file to zero (should be no-op)
        assert_eq!(temp_file.current_size(), 0);
        temp_file.truncate(0)?;
        assert_eq!(temp_file.current_size(), 0);

        // Truncate empty file to larger size (should be no-op)
        temp_file.truncate(100)?;
        assert_eq!(temp_file.current_size(), 0);

        Ok(())
    }
}
