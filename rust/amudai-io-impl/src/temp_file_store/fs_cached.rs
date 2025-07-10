//! A file-based temporary storage implementation with automatic cleanup and budget
//! management, using regular (buffered) file access.

use std::{
    fs::File,
    ops::Range,
    path::Path,
    sync::{Arc, Mutex},
};

use amudai_bytes::{Bytes, BytesMut};
use amudai_io::{
    ExclusiveIoBuffer, IoStream, ReadAt, SharedIoBuffer, StorageProfile, WriteAt, file::FileReader,
    read_adapter::ReadAdapter, temp_file_store::TemporaryFileStore, verify,
};

use crate::{
    fs::IoMode,
    temp_file_store::fs_common::{FileAllocation, TempContainer},
};

/// A file-based temporary storage implementation with automatic cleanup and budget management.
///
/// `TempFileStore` provides a robust temporary file management system that creates and manages
/// temporary files within a dedicated directory. It implements budget tracking to prevent
/// excessive disk usage and supports multiple types of temporary storage objects including
/// streams, exclusive buffers, and shared buffers.
///
/// # Features
///
/// - **Budget Management**: Tracks allocated space against a capacity limit to prevent runaway
///   disk usage
/// - **Automatic Cleanup**: Temporary files are automatically removed when the store is dropped
/// - **Multiple Access Patterns**: Supports sequential streaming, exclusive read/write buffers,
///   and shared concurrent access
/// - **Configurable Location**: Allows custom parent directory or uses system temp directory
///
/// # Storage Types
///
/// The store can allocate three types of temporary storage objects:
///
/// - **Streams** (`IoStream`): Sequential write access with conversion to readers
/// - **Exclusive Buffers** (`ExclusiveIoBuffer`): Random access read/write for non-concurrent use
/// - **Shared Buffers** (`SharedIoBuffer`): Random access read/write for concurrent use
///
/// # Budget System
///
/// The store maintains a budget that tracks allocated space in bytes. When temporary files
/// are created, space is allocated from this budget. When files are dropped or truncated,
/// space is returned to the budget. Allocations fail with an I/O error when the budget
/// is exceeded.
///
/// # Thread Safety
///
/// `TempFileStore` is `Clone` and can be safely shared across threads. Each clone
/// shares the same underlying temporary directory and budget, enabling coordinated
/// temporary storage management across concurrent operations.
///
/// # Cleanup
///
/// The temporary directory and all files within it are automatically cleaned up
/// when the last reference to the store is dropped, ensuring no temporary files
/// are left behind.
#[derive(Clone)]
pub struct TempFileStore(Arc<TempContainer>);

impl TempFileStore {
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
    /// Returns a new `TempFileStore` instance, or an I/O error if the temporary
    /// directory cannot be created.
    pub fn new(capacity: u64, parent_path: Option<&Path>) -> std::io::Result<TempFileStore> {
        let container = TempContainer::with_capacity(capacity, parent_path)?;
        Ok(TempFileStore(Arc::new(container)))
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
    /// Returns a new `TempFile` instance, or an I/O error if the file cannot
    /// be created or if there is insufficient budget.
    fn create_temp_file(&self, size_hint: Option<usize>) -> std::io::Result<TempFile> {
        let allocation = self.0.allocate(size_hint.map(|s| s as u64))?;
        let file = crate::fs::create_temporary_in(self.path(), IoMode::Buffered)?;
        Ok(TempFile {
            container: self.0.clone(),
            allocation,
            file,
        })
    }
}

impl TemporaryFileStore for TempFileStore {
    fn allocate_stream(&self, size_hint: Option<usize>) -> std::io::Result<Box<dyn IoStream>> {
        self.create_temp_file(size_hint)
            .map(|t| Box::new(t.into_stream()) as Box<dyn IoStream>)
    }

    fn allocate_exclusive_buffer(
        &self,
        size_hint: Option<usize>,
    ) -> std::io::Result<Box<dyn ExclusiveIoBuffer>> {
        self.create_temp_file(size_hint)
            .map(|t| Box::new(t.into_exclusive_buffer()) as Box<dyn ExclusiveIoBuffer>)
    }

    fn allocate_shared_buffer(
        &self,
        size_hint: Option<usize>,
    ) -> std::io::Result<Box<dyn SharedIoBuffer>> {
        self.create_temp_file(size_hint)
            .map(|t| Box::new(t.into_shared_buffer()) as Box<dyn SharedIoBuffer>)
    }
}

struct TempFile {
    container: Arc<TempContainer>,
    allocation: FileAllocation,
    file: File,
}

impl TempFile {
    fn into_stream(self) -> TempFileStream {
        TempFileStream {
            inner: self,
            pos: 0,
        }
    }

    fn into_exclusive_buffer(self) -> ExclusiveTempFileBuffer {
        ExclusiveTempFileBuffer(self)
    }

    fn into_shared_buffer(self) -> SharedTempFileBuffer {
        SharedTempFileBuffer {
            container: self.container,
            allocation: Mutex::new(self.allocation),
            file: self.file,
        }
    }

    fn write_at(&mut self, pos: u64, buf: &[u8]) -> std::io::Result<()> {
        let end_pos = pos.checked_add(buf.len() as u64).expect("end_pos");
        self.allocation.ensure_at_least(end_pos)?;
        amudai_io::file::file_write_at(&self.file, pos, buf)?;
        Ok(())
    }

    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes> {
        verify!(range.end >= range.start);
        let size = self.allocation.size();
        let end = range.end.min(size);
        if end > range.start {
            let len = (end - range.start) as usize;
            let mut buf = BytesMut::zeroed(len);
            amudai_io::file::file_read_at_exact(&self.file, range.start, &mut buf)?;
            Ok(Bytes::from(buf))
        } else {
            Ok(Bytes::new())
        }
    }

    fn into_read_at_impl(self) -> TempReader {
        TempReader::new(self.file, self.allocation, self.container)
    }

    fn into_reader_impl(self) -> ReadAdapter<TempReader> {
        ReadAdapter::new(self.into_read_at_impl())
    }
}

struct TempFileStream {
    inner: TempFile,
    pos: u64,
}

impl std::io::Write for TempFileStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write_at(self.pos, buf)?;
        self.pos += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl IoStream for TempFileStream {
    fn current_size(&self) -> u64 {
        self.pos
    }

    fn truncate(&mut self, size: u64) -> std::io::Result<()> {
        if size >= self.pos {
            return Ok(());
        }
        self.inner.file.set_len(size)?;
        self.pos = size;
        self.inner.allocation.truncate(size);
        Ok(())
    }

    fn into_read_at(self: Box<Self>) -> std::io::Result<Arc<dyn ReadAt>> {
        Ok(Arc::new(self.inner.into_read_at_impl()))
    }

    fn into_reader(self: Box<Self>) -> std::io::Result<Box<dyn std::io::Read>> {
        Ok(Box::new(self.inner.into_reader_impl()))
    }
}

struct ExclusiveTempFileBuffer(TempFile);

impl ReadAt for ExclusiveTempFileBuffer {
    fn size(&self) -> std::io::Result<u64> {
        Ok(self.0.allocation.size())
    }

    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes> {
        self.0.read_at(range)
    }

    fn storage_profile(&self) -> StorageProfile {
        self.0.container.storage_profile()
    }
}

impl ExclusiveIoBuffer for ExclusiveTempFileBuffer {
    fn set_size(&mut self, size: u64) -> std::io::Result<()> {
        let current_size = self.0.allocation.size();
        if size > current_size {
            self.0.allocation.ensure_at_least(size)?;
            self.0.file.set_len(size)
        } else if size < current_size {
            self.0.file.set_len(size)?;
            self.0.allocation.truncate(size);
            Ok(())
        } else {
            Ok(())
        }
    }

    fn write_at(&mut self, pos: u64, buf: &[u8]) -> std::io::Result<()> {
        self.0.write_at(pos, buf)
    }

    fn into_read_at(self: Box<Self>) -> std::io::Result<Arc<dyn ReadAt>> {
        Ok(Arc::new(self.0.into_read_at_impl()))
    }

    fn into_reader(self: Box<Self>) -> std::io::Result<Box<dyn std::io::Read>> {
        Ok(Box::new(self.0.into_reader_impl()))
    }
}

struct SharedTempFileBuffer {
    container: Arc<TempContainer>,
    allocation: Mutex<FileAllocation>,
    file: File,
}

impl ReadAt for SharedTempFileBuffer {
    fn size(&self) -> std::io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes> {
        verify!(range.end >= range.start);
        let len = (range.end - range.start) as usize;
        if len != 0 {
            let mut buf = BytesMut::zeroed(len);
            amudai_io::file::file_read_at_exact(&self.file, range.start, &mut buf)?;
            Ok(Bytes::from(buf))
        } else {
            Ok(Bytes::new())
        }
    }

    fn storage_profile(&self) -> StorageProfile {
        self.container.storage_profile()
    }
}

impl WriteAt for SharedTempFileBuffer {
    fn write_at(&self, pos: u64, buf: &[u8]) -> std::io::Result<()> {
        let end_pos = pos.checked_add(buf.len() as u64).expect("end_pos");
        self.allocation.lock().unwrap().ensure_at_least(end_pos)?;
        amudai_io::file::file_write_at(&self.file, pos, buf)
    }

    fn storage_profile(&self) -> StorageProfile {
        self.container.storage_profile()
    }
}

impl SharedIoBuffer for SharedTempFileBuffer {
    fn set_size(&self, size: u64) -> std::io::Result<()> {
        self.allocation.lock().unwrap().ensure_at_least(size)?;
        self.file.set_len(size)
    }

    fn into_read_at(self: Box<Self>) -> std::io::Result<Arc<dyn ReadAt>> {
        Ok(Arc::new(TempReader::new(
            self.file,
            self.allocation
                .into_inner()
                .expect("self.allocation.into_inner"),
            self.container,
        )))
    }

    fn into_reader(self: Box<Self>) -> std::io::Result<Box<dyn std::io::Read>> {
        let reader = TempReader::new(
            self.file,
            self.allocation
                .into_inner()
                .expect("self.allocation.into_inner"),
            self.container,
        );
        Ok(Box::new(ReadAdapter::new(reader)))
    }
}

struct TempReader {
    container: Arc<TempContainer>,
    _allocation: FileAllocation,
    inner: FileReader,
}

impl TempReader {
    fn new(file: File, allocation: FileAllocation, container: Arc<TempContainer>) -> TempReader {
        TempReader {
            container,
            _allocation: allocation,
            inner: FileReader::new(file),
        }
    }
}

impl ReadAt for TempReader {
    fn size(&self) -> std::io::Result<u64> {
        self.inner.size()
    }

    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes> {
        self.inner.read_at(range)
    }

    fn storage_profile(&self) -> StorageProfile {
        self.container.storage_profile()
    }
}
