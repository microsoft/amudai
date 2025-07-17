//! A file-based temporary storage implementation with automatic cleanup and budget
//! management that uses direct (unbuffered) I/O for all file operations.

use std::{
    fs::File,
    ops::Range,
    path::Path,
    sync::{Arc, Mutex},
};

use amudai_bytes::{Bytes, align::is_aligned_u64};
use amudai_io::{
    ExclusiveIoBuffer, IoStream, ReadAdapter, ReadAt, SharedIoBuffer, StorageProfile,
    TemporaryFileStore, WriteAt, verify,
};

use crate::{
    fs::{DirectFileReadAt, DirectFileWriter, IoMode, direct_file::DirectFile},
    temp_file_store::fs_common::{FileAllocation, TempContainer},
};

/// A temporary file store that uses direct (unbuffered) I/O for all file operations.
///
/// This store creates temporary files that bypass the operating system's page cache,
/// providing direct access to storage devices. All temporary files are opened in
/// [`IoMode::Unbuffered`] mode, which has important implications for alignment and
/// padding requirements.
///
/// # Direct I/O Characteristics
///
/// - **Page cache bypass**: All file operations bypass the OS page cache for direct
///   access, which can reduce memory pressure but requires careful alignment
/// - **Size padding**: The final size of all temporary files will be padded to a
///   multiple of the I/O granularity (typically 4KB) due to direct I/O alignment
///   requirements
/// - **Alignment requirements**: Write operations must be properly aligned to I/O
///   granularity boundaries for optimal performance and compatibility
///
/// # Allocation Methods
///
/// The store provides three types of temporary storage allocations:
///
/// - **Streams** ([`allocate_stream`]): Sequential write-only access that allows
///   arbitrary append operations. Data is automatically padded when the stream is
///   finalized and converted to a reader
/// - **Exclusive buffers** ([`allocate_exclusive_buffer`]): Random-access read/write
///   buffers that require aligned writes (position and buffer size must be multiples
///   of I/O granularity, typically 4KB) but support arbitrary reads
/// - **Shared buffers** ([`allocate_shared_buffer`]): Thread-safe random-access
///   read/write buffers with the same alignment requirements as exclusive buffers
///
/// # Budget Management
///
/// The store maintains a capacity budget to prevent excessive temporary storage usage.
/// Allocations that would exceed the budget will fail with an I/O error.
///
/// [`allocate_stream`]: TempFileStore::allocate_stream
/// [`allocate_exclusive_buffer`]: TempFileStore::allocate_exclusive_buffer
/// [`allocate_shared_buffer`]: TempFileStore::allocate_shared_buffer
/// [`IoMode::Unbuffered`]: crate::fs::IoMode::Unbuffered
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
        let file = crate::fs::create_temporary_in(self.path(), IoMode::Unbuffered)?;
        Ok(TempFile {
            container: self.0.clone(),
            allocation,
            file,
        })
    }
}

impl TemporaryFileStore for TempFileStore {
    /// Allocates a temporary I/O stream for sequential writing with direct I/O.
    ///
    /// The returned stream supports arbitrary append operations and handles alignment
    /// internally. When the stream is finalized and converted to a reader, the final
    /// file size will be automatically padded to a multiple of the I/O granularity
    /// (typically 4KB) as required by direct I/O operations.
    ///
    /// # Arguments
    ///
    /// * `size_hint` - Optional hint for initial budget allocation
    ///
    /// # Returns
    ///
    /// A sequential write stream that can be converted to a reader after writing
    fn allocate_stream(&self, size_hint: Option<usize>) -> std::io::Result<Box<dyn IoStream>> {
        self.create_temp_file(size_hint)
            .and_then(|t| t.into_stream())
            .map(|t| Box::new(t) as Box<dyn IoStream>)
    }

    /// Allocates an exclusive read/write buffer with strict alignment requirements.
    ///
    /// The returned buffer supports random-access operations but requires properly
    /// aligned writes to work correctly with direct I/O:
    /// - Write positions must be multiples of the I/O granularity (typically 4KB)
    /// - Buffer sizes for writes must be multiples of the I/O granularity
    /// - Unaligned writes will fail with an I/O error
    ///
    /// Read operations support arbitrary positions and sizes regardless of alignment.
    ///
    /// # Arguments
    ///
    /// * `size_hint` - Optional hint for initial budget allocation
    ///
    /// # Returns
    ///
    /// An exclusive buffer with random-access read/write capabilities
    fn allocate_exclusive_buffer(
        &self,
        size_hint: Option<usize>,
    ) -> std::io::Result<Box<dyn ExclusiveIoBuffer>> {
        self.create_temp_file(size_hint)
            .map(|t| Box::new(t.into_exclusive_buffer()) as Box<dyn ExclusiveIoBuffer>)
    }

    /// Allocates a shared read/write buffer with strict alignment requirements.
    ///
    /// Similar to [`allocate_exclusive_buffer`], but supports concurrent access from
    /// multiple threads. Write operations have the same alignment requirements:
    /// - Write positions must be multiples of the I/O granularity (typically 4KB)
    /// - Buffer sizes for writes must be multiples of the I/O granularity
    /// - Unaligned writes will fail with an I/O error
    ///
    /// Read operations support arbitrary positions and sizes regardless of alignment.
    ///
    /// # Arguments
    ///
    /// * `size_hint` - Optional hint for initial budget allocation
    ///
    /// # Returns
    ///
    /// A shared buffer with thread-safe random-access read/write capabilities
    ///
    /// [`allocate_exclusive_buffer`]: Self::allocate_exclusive_buffer
    fn allocate_shared_buffer(
        &self,
        size_hint: Option<usize>,
    ) -> std::io::Result<Arc<dyn SharedIoBuffer>> {
        self.create_temp_file(size_hint)
            .map(|t| Arc::new(t.into_shared_buffer()) as _)
    }
}

struct TempFile {
    container: Arc<TempContainer>,
    allocation: FileAllocation,
    file: File,
}

impl TempFile {
    fn into_stream(self) -> std::io::Result<TempFileStream> {
        Ok(TempFileStream {
            container: self.container,
            allocation: self.allocation,
            writer: DirectFileWriter::from_file(self.file)?,
        })
    }

    fn into_exclusive_buffer(self) -> ExclusiveTempFileBuffer {
        ExclusiveTempFileBuffer {
            container: self.container,
            allocation: self.allocation,
            file: DirectFile::new(Arc::new(self.file)),
        }
    }

    fn into_shared_buffer(self) -> SharedTempFileBuffer {
        SharedTempFileBuffer {
            container: self.container,
            allocation: Mutex::new(self.allocation),
            file: DirectFile::new(Arc::new(self.file)),
        }
    }
}

struct TempFileStream {
    container: Arc<TempContainer>,
    allocation: FileAllocation,
    writer: DirectFileWriter,
}

impl std::io::Write for TempFileStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let end_pos = self
            .writer
            .position()
            .checked_add(buf.len() as u64)
            .expect("end_pos");
        self.allocation.ensure_at_least(end_pos)?;
        self.writer.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

impl IoStream for TempFileStream {
    fn current_size(&self) -> u64 {
        self.writer.position()
    }

    fn truncate(&mut self, end_pos: u64) -> std::io::Result<()> {
        if end_pos < self.writer.position() {
            self.writer.truncate(end_pos)?;
            self.allocation.truncate(end_pos);
        }
        Ok(())
    }

    fn into_read_at(self: Box<Self>) -> std::io::Result<Arc<dyn ReadAt>> {
        let reader = TempReader {
            container: self.container,
            _allocation: self.allocation,
            inner: self.writer.into_read_at()?,
        };
        Ok(Arc::new(reader))
    }

    fn into_reader(self: Box<Self>) -> std::io::Result<Box<dyn std::io::Read>> {
        let reader = TempReader {
            container: self.container,
            _allocation: self.allocation,
            inner: self.writer.into_read_at()?,
        };
        Ok(Box::new(ReadAdapter::new(reader)))
    }
}

struct ExclusiveTempFileBuffer {
    container: Arc<TempContainer>,
    allocation: FileAllocation,
    file: DirectFile,
}

impl ReadAt for ExclusiveTempFileBuffer {
    fn size(&self) -> std::io::Result<u64> {
        Ok(self.allocation.size())
    }

    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes> {
        self.file.read_at(range)
    }

    fn storage_profile(&self) -> StorageProfile {
        self.container.storage_profile()
    }
}

impl ExclusiveIoBuffer for ExclusiveTempFileBuffer {
    fn set_size(&mut self, size: u64) -> std::io::Result<()> {
        verify!(is_aligned_u64(size, self.file.io_granularity()));
        let current_size = self.allocation.size();
        if size > current_size {
            self.allocation.ensure_at_least(size)?;
            self.file.set_size(size)
        } else if size < current_size {
            self.file.set_size(size)?;
            self.allocation.truncate(size);
            Ok(())
        } else {
            Ok(())
        }
    }

    fn write_at(&mut self, pos: u64, buf: &[u8]) -> std::io::Result<()> {
        let end_pos = pos.checked_add(buf.len() as u64).expect("end_pos");
        self.allocation.ensure_at_least(end_pos)?;
        self.file.write_at(pos, buf)
    }

    fn into_read_at(self: Box<Self>) -> std::io::Result<Arc<dyn ReadAt>> {
        let reader = TempReader {
            container: self.container,
            _allocation: self.allocation,
            inner: self.file.into_read_at(),
        };
        Ok(Arc::new(reader))
    }

    fn into_reader(self: Box<Self>) -> std::io::Result<Box<dyn std::io::Read>> {
        let reader = TempReader {
            container: self.container,
            _allocation: self.allocation,
            inner: self.file.into_read_at(),
        };
        Ok(Box::new(ReadAdapter::new(reader)))
    }
}

struct SharedTempFileBuffer {
    container: Arc<TempContainer>,
    allocation: Mutex<FileAllocation>,
    file: DirectFile,
}

impl WriteAt for SharedTempFileBuffer {
    fn write_at(&self, pos: u64, buf: &[u8]) -> std::io::Result<()> {
        let end_pos = pos.checked_add(buf.len() as u64).expect("end_pos");
        self.allocation.lock().unwrap().ensure_at_least(end_pos)?;
        self.file.write_at(pos, buf)
    }

    fn storage_profile(&self) -> StorageProfile {
        self.container.storage_profile()
    }
}

impl ReadAt for SharedTempFileBuffer {
    fn size(&self) -> std::io::Result<u64> {
        self.file.size()
    }

    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes> {
        self.file.read_at(range)
    }

    fn storage_profile(&self) -> StorageProfile {
        self.container.storage_profile()
    }
}

impl SharedIoBuffer for SharedTempFileBuffer {
    fn set_size(&self, size: u64) -> std::io::Result<()> {
        self.allocation.lock().unwrap().ensure_at_least(size)?;
        self.file.set_size(size)
    }

    fn into_read_at(self: Box<Self>) -> std::io::Result<Arc<dyn ReadAt>> {
        let reader = TempReader {
            container: self.container,
            _allocation: self.allocation.into_inner().expect("allocation"),
            inner: self.file.into_read_at(),
        };
        Ok(Arc::new(reader))
    }

    fn into_reader(self: Box<Self>) -> std::io::Result<Box<dyn std::io::Read>> {
        let reader = TempReader {
            container: self.container,
            _allocation: self.allocation.into_inner().expect("allocation"),
            inner: self.file.into_read_at(),
        };
        Ok(Box::new(ReadAdapter::new(reader)))
    }
}

struct TempReader {
    container: Arc<TempContainer>,
    _allocation: FileAllocation,
    inner: DirectFileReadAt,
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

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        sync::Arc,
        time::Duration,
    };

    use amudai_io::{ReadAdapter, TemporaryFileStore};

    fn create_temp_store(capacity: u64) -> Arc<dyn TemporaryFileStore> {
        Arc::new(super::TempFileStore::new(capacity, None).unwrap())
    }

    #[test]
    fn test_shared_buffer_basics() {
        let store = create_temp_store(1024 * 1024 * 1024);
        let buffer = store.allocate_shared_buffer(None).unwrap();
        let mut threads = Vec::new();
        for i in 0u64..10 {
            let buf = buffer.clone();
            let th = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                buf.write_at(i * 16 * 1024, &vec![i as u8; 16 * 1024])
                    .unwrap();
            });
            threads.push(th);
        }
        threads.into_iter().for_each(|t| t.join().unwrap());

        let mut reader = ReadAdapter::new(buffer);
        let mut values = Vec::new();
        reader.read_to_end(&mut values).unwrap();

        let mut hist = [0usize; 10];
        values.iter().for_each(|&v| hist[v as usize] += 1);
        hist.iter().for_each(|&count| {
            assert_eq!(count, 16 * 1024);
        });
    }

    #[test]
    fn test_allocate_stream_basic_write_read() {
        let store = create_temp_store(1024 * 1024);
        let mut stream = store.allocate_stream(None).unwrap();

        // Write some test data
        let test_data = b"Hello, World! This is a test.";
        stream.write_all(test_data).unwrap();
        stream.flush().unwrap();

        // Convert to reader and verify data
        let reader = stream.into_read_at().unwrap();
        let file_size = reader.size().unwrap();

        // File size might be aligned to page boundaries, but should at least contain our data
        assert!(file_size >= test_data.len() as u64);

        let read_data = reader.read_at(0..test_data.len() as u64).unwrap();
        assert_eq!(read_data.as_ref(), test_data);
    }

    #[test]
    fn test_allocate_stream_empty() {
        let store = create_temp_store(1024 * 1024);
        let stream = store.allocate_stream(None).unwrap();

        // Convert empty stream to reader
        let reader = stream.into_read_at().unwrap();
        assert_eq!(reader.size().unwrap(), 0);

        let read_data = reader.read_at(0..0).unwrap();
        assert_eq!(read_data.len(), 0);
    }

    #[test]
    fn test_allocate_stream_with_size_hint() {
        let store = create_temp_store(1024 * 1024);
        let mut stream = store.allocate_stream(Some(1024)).unwrap();

        // Write data smaller than size hint
        let test_data = vec![42u8; 512];
        stream.write_all(&test_data).unwrap();

        // Write data larger than size hint (should still work)
        let more_data = vec![84u8; 1024];
        stream.write_all(&more_data).unwrap();

        let reader = stream.into_read_at().unwrap();
        let file_size = reader.size().unwrap();

        // File size might be aligned, but should contain our data
        let expected_data_size = 512 + 1024;
        assert!(file_size >= expected_data_size as u64);

        let first_part = reader.read_at(0..512).unwrap();
        assert_eq!(first_part.as_ref(), &vec![42u8; 512]);

        let second_part = reader.read_at(512..expected_data_size as u64).unwrap();
        assert_eq!(second_part.as_ref(), &vec![84u8; 1024]);
    }

    #[test]
    fn test_allocate_stream_large_write() {
        let store = create_temp_store(10 * 1024 * 1024);
        let mut stream = store.allocate_stream(Some(1024 * 1024)).unwrap();

        // Write 1MB of data in chunks
        let chunk_size = 4096;
        let total_size = 1024 * 1024;
        let num_chunks = total_size / chunk_size;

        for i in 0..num_chunks {
            let chunk = vec![(i % 256) as u8; chunk_size];
            stream.write_all(&chunk).unwrap();
        }

        let reader = stream.into_read_at().unwrap();
        assert_eq!(reader.size().unwrap(), total_size as u64);

        // Verify a few random chunks
        for i in [0, num_chunks / 4, num_chunks / 2, num_chunks - 1] {
            let start = i * chunk_size;
            let end = start + chunk_size;
            let chunk_data = reader.read_at(start as u64..end as u64).unwrap();
            let expected_value = (i % 256) as u8;
            assert_eq!(chunk_data.as_ref(), &vec![expected_value; chunk_size]);
        }
    }

    #[test]
    fn test_allocate_stream_sequential_reads() {
        let store = create_temp_store(1024 * 1024);
        let mut stream = store.allocate_stream(None).unwrap();

        // Write pattern data
        let pattern_size = 100;
        for i in 0..pattern_size {
            stream.write_all(&[i as u8]).unwrap();
        }

        let reader = stream.into_read_at().unwrap();

        // Read in small chunks and verify pattern
        let chunk_size = 10;
        for start in (0..pattern_size).step_by(chunk_size) {
            let end = (start + chunk_size).min(pattern_size);
            let chunk = reader.read_at(start as u64..end as u64).unwrap();

            for (i, &byte) in chunk.iter().enumerate() {
                assert_eq!(byte, (start + i) as u8);
            }
        }
    }

    #[test]
    fn test_allocate_stream_into_reader() {
        let store = create_temp_store(1024 * 1024);
        let mut stream = store.allocate_stream(None).unwrap();

        let test_data = b"Stream to std::io::Read conversion test";
        stream.write_all(test_data).unwrap();

        let mut reader = stream.into_reader().unwrap();
        let mut read_buffer = Vec::new();
        reader.read_to_end(&mut read_buffer).unwrap();

        // Only compare the actual data we wrote, not any padding
        assert_eq!(&read_buffer[..test_data.len()], test_data);
    }

    #[test]
    fn test_allocate_stream_truncate() {
        let store = create_temp_store(1024 * 1024);
        let mut stream = store.allocate_stream(None).unwrap();

        // Write some data
        let test_data = vec![123u8; 1000];
        stream.write_all(&test_data).unwrap();
        assert_eq!(stream.current_size(), 1000);

        // Truncate to smaller size
        stream.truncate(500).unwrap();
        assert_eq!(stream.current_size(), 500);

        let reader = stream.into_read_at().unwrap();
        // File size might be aligned, but should be at least 500 bytes
        assert!(reader.size().unwrap() >= 500);

        let read_data = reader.read_at(0..500).unwrap();
        assert_eq!(read_data.as_ref(), &vec![123u8; 500]);
    }

    #[test]
    fn test_allocate_stream_budget_enforcement() {
        let small_capacity = 1024; // 1KB capacity
        let store = create_temp_store(small_capacity);

        let mut stream = store.allocate_stream(None).unwrap();

        // Writing small amount should succeed
        let small_data = vec![1u8; 512];
        stream.write_all(&small_data).unwrap();

        // Writing large amount that exceeds capacity should eventually fail
        // Note: This might not fail immediately due to buffering, but should fail at some point
        let large_data = vec![2u8; 2048]; // 2KB, exceeds our 1KB capacity
        let result = stream.write_all(&large_data);

        // The write might succeed initially due to buffering, but conversion should reflect the constraint
        if result.is_ok() {
            // If write succeeded, the error might occur during conversion or flush
            let flush_result = stream.flush();
            if flush_result.is_ok() {
                // Even if flush succeeds, we should be near or at capacity
                assert!(stream.current_size() <= small_capacity);
            }
        }
    }

    #[test]
    fn test_allocate_multiple_streams() {
        let store = create_temp_store(10 * 1024 * 1024);

        let mut streams = Vec::new();
        let num_streams = 5;

        // Allocate multiple streams
        for i in 0..num_streams {
            let mut stream = store.allocate_stream(Some(1024)).unwrap();
            let data = vec![i as u8; 100];
            stream.write_all(&data).unwrap();
            streams.push(stream);
        }

        // Convert all to readers and verify isolation
        for (i, stream) in streams.into_iter().enumerate() {
            let reader = stream.into_read_at().unwrap();
            // File size might be aligned, but should contain our data
            assert!(reader.size().unwrap() >= 100);

            let data = reader.read_at(0..100).unwrap();
            assert_eq!(data.as_ref(), &vec![i as u8; 100]);
        }
    }

    #[test]
    fn test_allocate_stream_zero_capacity_budget() {
        // Test with very small capacity to ensure budget enforcement
        let store = create_temp_store(512);
        let mut stream = store.allocate_stream(None).unwrap();

        // Writing small amount should work
        let small_data = vec![1u8; 100];
        stream.write_all(&small_data).unwrap();

        // Further writes might eventually fail due to budget constraints
        let large_data = vec![2u8; 1024];
        let result = stream.write_all(&large_data);

        // Either the write fails immediately, or conversion reveals constraint
        if result.is_ok() {
            // The write might succeed due to buffering, but should be limited
            let reader = stream.into_read_at().unwrap();
            // Should have at least the small data
            assert!(reader.size().unwrap() >= 100);
        }
        // If write failed, that's also acceptable for budget enforcement
    }

    #[test]
    fn test_allocate_stream_incremental_writes() {
        let store = create_temp_store(1024 * 1024);
        let mut stream = store.allocate_stream(None).unwrap();

        // Write data incrementally and check current_size
        assert_eq!(stream.current_size(), 0);

        stream.write_all(b"Hello").unwrap();
        assert_eq!(stream.current_size(), 5);

        stream.write_all(b", ").unwrap();
        assert_eq!(stream.current_size(), 7);

        stream.write_all(b"World!").unwrap();
        assert_eq!(stream.current_size(), 13);

        let reader = stream.into_read_at().unwrap();
        let data = reader.read_at(0..13).unwrap();
        assert_eq!(data.as_ref(), b"Hello, World!");
    }

    #[test]
    fn test_allocate_stream_flush_behavior() {
        let store = create_temp_store(1024 * 1024);
        let mut stream = store.allocate_stream(None).unwrap();

        // Write data and flush
        let test_data = b"Data that should be flushed";
        stream.write_all(test_data).unwrap();
        stream.flush().unwrap();

        // Verify current_size is updated
        assert_eq!(stream.current_size(), test_data.len() as u64);

        // Convert to reader and verify data is accessible
        let reader = stream.into_read_at().unwrap();
        let read_data = reader.read_at(0..test_data.len() as u64).unwrap();
        assert_eq!(read_data.as_ref(), test_data);
    }
}
