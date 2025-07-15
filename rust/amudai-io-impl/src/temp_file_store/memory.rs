use std::{
    io,
    sync::{Arc, RwLock},
};

use amudai_budget_tracker::{Allocation, Budget};
use amudai_bytes::{Bytes, BytesMut};
use amudai_io::{
    ExclusiveIoBuffer, IoStream, ReadAt, SharedIoBuffer, StorageProfile, WriteAt,
    temp_file_store::TemporaryFileStore, verify,
};

pub struct InMemoryTempFileStore {
    budget: Budget,
}

impl InMemoryTempFileStore {
    pub fn new(capacity: u64) -> InMemoryTempFileStore {
        InMemoryTempFileStore {
            budget: Budget::new(capacity),
        }
    }

    pub fn available_space(&self) -> u64 {
        self.budget.remaining()
    }

    fn create_temp_buffer(&self, size_hint: Option<usize>) -> io::Result<InMemoryTempBuffer> {
        let size_hint = size_hint.unwrap_or(0) as u64;
        let mut allocation = self.budget.allocate(0)?;
        allocation.reserve(size_hint)?;
        if size_hint >= 1024 * 1024 {
            allocation.set_reservation_slice(64 * 1024);
        }
        Ok(InMemoryTempBuffer::new(allocation))
    }
}

impl TemporaryFileStore for InMemoryTempFileStore {
    fn allocate_stream(&self, size_hint: Option<usize>) -> std::io::Result<Box<dyn IoStream>> {
        self.create_temp_buffer(size_hint)
            .map(|t| Box::new(t) as Box<dyn IoStream>)
    }

    fn allocate_exclusive_buffer(
        &self,
        size_hint: Option<usize>,
    ) -> std::io::Result<Box<dyn ExclusiveIoBuffer>> {
        self.create_temp_buffer(size_hint)
            .map(|t| Box::new(t) as Box<dyn ExclusiveIoBuffer>)
    }

    fn allocate_shared_buffer(
        &self,
        size_hint: Option<usize>,
    ) -> std::io::Result<Box<dyn SharedIoBuffer>> {
        self.create_temp_buffer(size_hint)
            .map(|t| Box::new(Shared::new(t)) as Box<dyn SharedIoBuffer>)
    }
}

struct InMemoryTempBuffer {
    data: BytesMut,
    allocation: Allocation,
    write_pos: u64,
}

impl InMemoryTempBuffer {
    fn new(allocation: Allocation) -> InMemoryTempBuffer {
        assert_eq!(allocation.amount(), 0);
        InMemoryTempBuffer {
            data: BytesMut::new(),
            allocation,
            write_pos: 0,
        }
    }

    fn ensure_allocation_for_size(&mut self, size: usize) -> io::Result<()> {
        assert_eq!(self.allocation.amount(), self.current_size());
        if size > self.data.len() {
            let grow = size - self.data.len();
            self.allocation.grow(grow as u64)?;
        }
        Ok(())
    }

    fn write_impl(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_at_impl(self.write_pos, buf)?;
        self.write_pos += buf.len() as u64;
        Ok(buf.len())
    }

    fn set_size_impl(&mut self, size: u64) -> std::io::Result<()> {
        if size < self.current_size() {
            self.truncate_impl(size)
        } else {
            self.ensure_allocation_for_size(size as usize)?;
            self.data.resize(size as usize, 0);
            Ok(())
        }
    }

    fn truncate_impl(&mut self, end_pos: u64) -> std::io::Result<()> {
        assert_eq!(self.allocation.amount(), self.current_size());
        if end_pos < self.current_size() {
            self.data.truncate(end_pos as usize);
            self.allocation.shrink_to(end_pos);
            self.write_pos = std::cmp::min(self.write_pos, end_pos);
        }
        Ok(())
    }

    fn write_at_impl(&mut self, pos: u64, buf: &[u8]) -> std::io::Result<()> {
        let end_pos = pos + buf.len() as u64;
        self.ensure_allocation_for_size(end_pos as usize)?;

        if pos == self.current_size() {
            self.data.extend_from_slice(buf);
        } else {
            if end_pos > self.current_size() {
                self.data.resize(end_pos as usize, 0);
            }
            self.data[pos as usize..end_pos as usize].copy_from_slice(buf);
        }
        Ok(())
    }

    fn read_at_impl(&self, range: std::ops::Range<u64>) -> io::Result<Bytes> {
        verify!(range.end >= range.start);
        let end = range.end.min(self.data.len() as u64);
        verify!(end >= range.start);
        if end > range.start {
            Ok(Bytes::from(&self.data[range.start as usize..end as usize]))
        } else {
            Ok(Bytes::new())
        }
    }

    fn storage_profile_impl(&self) -> StorageProfile {
        StorageProfile {
            min_io_size: 1,
            max_io_size: 1024 * 1024,
        }
    }

    fn make_reader(self) -> InMemoryTempReader {
        assert_eq!(self.allocation.amount(), self.current_size());
        let mut allocation = self.allocation;
        let bytes = Bytes::from(self.data);
        allocation.shrink_to(bytes.len() as u64);
        InMemoryTempReader {
            bytes,
            read_pos: 0,
            _allocation: allocation,
        }
    }

    fn into_read_at_impl(self) -> std::io::Result<Arc<dyn ReadAt>> {
        Ok(Arc::new(self.make_reader()))
    }

    fn into_reader_impl(self) -> std::io::Result<Box<dyn std::io::Read>> {
        Ok(Box::new(self.make_reader()))
    }
}

impl io::Write for InMemoryTempBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_impl(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl ReadAt for InMemoryTempBuffer {
    fn size(&self) -> std::io::Result<u64> {
        Ok(self.data.len() as u64)
    }

    fn read_at(&self, range: std::ops::Range<u64>) -> io::Result<Bytes> {
        self.read_at_impl(range)
    }

    fn storage_profile(&self) -> StorageProfile {
        self.storage_profile_impl()
    }
}

impl IoStream for InMemoryTempBuffer {
    fn current_size(&self) -> u64 {
        self.data.len() as u64
    }

    fn truncate(&mut self, end_pos: u64) -> std::io::Result<()> {
        self.truncate_impl(end_pos)
    }

    fn into_read_at(self: Box<Self>) -> std::io::Result<Arc<dyn ReadAt>> {
        self.into_read_at_impl()
    }

    fn into_reader(self: Box<Self>) -> std::io::Result<Box<dyn std::io::Read>> {
        self.into_reader_impl()
    }
}

impl ExclusiveIoBuffer for InMemoryTempBuffer {
    fn set_size(&mut self, size: u64) -> std::io::Result<()> {
        self.set_size_impl(size)
    }

    fn write_at(&mut self, pos: u64, buf: &[u8]) -> std::io::Result<()> {
        self.write_at_impl(pos, buf)
    }

    fn into_read_at(self: Box<Self>) -> std::io::Result<Arc<dyn ReadAt>> {
        self.into_read_at_impl()
    }

    fn into_reader(self: Box<Self>) -> std::io::Result<Box<dyn std::io::Read>> {
        self.into_reader_impl()
    }
}

struct Shared(RwLock<InMemoryTempBuffer>);

impl Shared {
    fn new(buf: InMemoryTempBuffer) -> Shared {
        Shared(RwLock::new(buf))
    }
}

impl ReadAt for Shared {
    fn size(&self) -> std::io::Result<u64> {
        self.0.read().unwrap().size()
    }

    fn read_at(&self, range: std::ops::Range<u64>) -> std::io::Result<Bytes> {
        self.0.read().unwrap().read_at_impl(range)
    }

    fn storage_profile(&self) -> StorageProfile {
        self.0.read().unwrap().storage_profile_impl()
    }
}

impl WriteAt for Shared {
    fn write_at(&self, pos: u64, buf: &[u8]) -> std::io::Result<()> {
        self.0.write().unwrap().write_at_impl(pos, buf)
    }

    fn storage_profile(&self) -> StorageProfile {
        self.0.read().unwrap().storage_profile_impl()
    }
}

impl SharedIoBuffer for Shared {
    fn set_size(&self, size: u64) -> std::io::Result<()> {
        self.0.write().unwrap().set_size_impl(size)
    }

    fn into_read_at(self: Box<Self>) -> std::io::Result<Arc<dyn ReadAt>> {
        self.0.into_inner().unwrap().into_read_at_impl()
    }

    fn into_reader(self: Box<Self>) -> std::io::Result<Box<dyn std::io::Read>> {
        self.0.into_inner().unwrap().into_reader_impl()
    }
}

struct InMemoryTempReader {
    bytes: Bytes,
    read_pos: usize,
    _allocation: Allocation,
}

impl ReadAt for InMemoryTempReader {
    fn size(&self) -> std::io::Result<u64> {
        self.bytes.size()
    }

    fn read_at(&self, range: std::ops::Range<u64>) -> io::Result<Bytes> {
        self.bytes.read_at(range)
    }

    fn storage_profile(&self) -> StorageProfile {
        StorageProfile {
            min_io_size: 1,
            max_io_size: StorageProfile::default().max_io_size,
        }
    }
}

impl io::Read for InMemoryTempReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let end_pos = std::cmp::min(self.read_pos + buf.len(), self.bytes.len());
        let read_len = end_pos - self.read_pos;
        if read_len != 0 {
            buf[..read_len].copy_from_slice(&self.bytes[self.read_pos..end_pos]);
            self.read_pos += read_len;
            Ok(read_len)
        } else {
            Ok(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_temp_file_store_creation() {
        let store = InMemoryTempFileStore::new(1024);
        assert_eq!(store.available_space(), 1024);
    }

    #[test]
    fn test_in_memory_temp_file_store_allocate_writable() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1024);
        let writable = store.allocate_stream(Some(512))?;
        assert_eq!(store.available_space(), 512); // Initial reservation
        assert_eq!(writable.current_size(), 0);
        Ok(())
    }

    #[test]
    fn test_in_memory_temp_file_store_allocate_buffer() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1024);
        let buffer = store.allocate_exclusive_buffer(Some(512))?;
        assert_eq!(store.available_space(), 512);
        drop(buffer);
        assert_eq!(store.available_space(), 1024);
        Ok(())
    }

    #[test]
    fn test_in_memory_temp_file_store_allocate_no_hint() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1024);
        let writable = store.allocate_stream(None)?;
        assert_eq!(store.available_space(), 1024); // Initial reservation
        assert_eq!(writable.current_size(), 0);
        Ok(())
    }

    #[test]
    fn test_in_memory_temp_file_store_allocate_exceed_capacity() {
        let store = InMemoryTempFileStore::new(1024);
        let result = store.allocate_stream(Some(1024 * 1024));
        assert!(result.is_err());
    }

    #[test]
    fn test_in_memory_temp_buffer_write_and_read() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1024 * 1024);
        let mut buffer = store.allocate_stream(None)?;

        let data = b"hello world";
        buffer.write_all(data)?;
        assert_eq!(buffer.current_size(), data.len() as u64);

        let end_pos = buffer.current_size();
        let reader = buffer.into_read_at()?;
        let read_buffer = reader.read_at(0..end_pos)?;
        assert_eq!(read_buffer.as_ref(), data.as_slice());
        Ok(())
    }

    #[test]
    fn test_in_memory_temp_buffer_write_at() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1024 * 1024);
        let mut buffer = store.allocate_exclusive_buffer(None)?;

        let data1 = b"hello";
        buffer.write_at(0, data1)?;
        assert_eq!(buffer.size()?, data1.len() as u64);

        let data2 = b" world";
        buffer.write_at(data1.len() as u64, data2)?;
        assert_eq!(buffer.size()?, (data1.len() + data2.len()) as u64);

        let buf = buffer.read_at(0..(data1.len() + data2.len()) as u64)?;
        assert_eq!(buf.as_ref(), b"hello world");
        Ok(())
    }

    #[test]
    fn test_in_memory_temp_buffer_write_at_overwrite() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1024 * 1024);
        let mut buffer = store.allocate_exclusive_buffer(None)?;

        let data1 = b"hello world";
        buffer.write_at(0, data1)?;

        let data2 = b"cruel";
        buffer.write_at(2, data2)?;

        let buf = buffer.read_at(0..data1.len() as u64)?;
        assert_eq!(buf.as_ref(), b"hecruelorld");
        Ok(())
    }

    #[test]
    fn test_in_memory_temp_buffer_write_at_extend() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1024 * 1024);
        let mut buffer = store.allocate_exclusive_buffer(None)?;

        let data1 = b"hello";
        buffer.write_at(0, data1)?;

        let data2 = b"world";
        buffer.write_at(10, data2)?;

        let buf = buffer.read_at(0..15)?;
        assert_eq!(buf.as_ref(), b"hello\0\0\0\0\0world");
        Ok(())
    }

    #[test]
    fn test_in_memory_temp_buffer_truncate() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1024 * 1024);
        let mut buffer = store.allocate_stream(None)?;

        let data = b"hello world";
        buffer.write_all(data)?;
        assert_eq!(buffer.current_size(), data.len() as u64);

        buffer.truncate(5)?;
        assert_eq!(buffer.current_size(), 5);

        let mut read_buffer = Vec::new();
        let mut reader = buffer.into_reader()?;
        reader.read_to_end(&mut read_buffer)?;
        assert_eq!(read_buffer, b"hello");
        Ok(())
    }

    #[test]
    fn test_in_memory_temp_buffer_truncate_no_op() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1024 * 1024);
        let mut buffer = store.allocate_stream(None)?;

        let data = b"hello world";
        buffer.write_all(data)?;
        let original_size = buffer.current_size();

        buffer.truncate(original_size + 10)?;
        assert_eq!(buffer.current_size(), original_size);

        let mut read_buffer = Vec::new();
        let mut reader = buffer.into_reader()?;
        reader.read_to_end(&mut read_buffer)?;
        assert_eq!(read_buffer, data);
        Ok(())
    }

    #[test]
    fn test_in_memory_temp_buffer_read_at_range() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1024 * 1024);
        let mut buffer = store.allocate_exclusive_buffer(None)?;

        let data = b"hello world";
        buffer.write_at(0, data)?;

        let read_bytes = buffer.read_at(2..7)?;
        assert_eq!(read_bytes.as_ref(), b"llo w");

        let read_bytes = buffer.read_at(0..0)?;
        assert_eq!(read_bytes.as_ref(), b"");

        let read_bytes = buffer.read_at(10..15)?;
        assert_eq!(read_bytes.as_ref(), b"d");

        let read_bytes = buffer.read_at(11..15)?;
        assert_eq!(read_bytes.as_ref(), b"");
        Ok(())
    }

    #[test]
    fn test_in_memory_temp_reader_read_at() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1024 * 1024);
        let mut buffer = store.allocate_stream(None)?;

        let data = b"hello world";
        buffer.write_all(data)?;

        let reader = buffer.into_read_at()?;
        let read_bytes = reader.read_at(2..7)?;
        assert_eq!(read_bytes.as_ref(), b"llo w");

        let read_bytes = reader.read_at(0..0)?;
        assert_eq!(read_bytes.as_ref(), b"");

        let read_bytes = reader.read_at(10..15)?;
        assert_eq!(read_bytes.as_ref(), b"d");

        let read_bytes = reader.read_at(11..15)?;
        assert_eq!(read_bytes.as_ref(), b"");
        Ok(())
    }

    #[test]
    fn test_in_memory_temp_buffer_budget_exceeded() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1024);
        let mut buffer1 = store.allocate_stream(None)?;
        let mut buffer2 = store.allocate_stream(None)?;

        for _ in 0..63 {
            buffer1.write_all(b"12345678")?;
            buffer2.write_all(b"12345678")?;
        }

        assert!(buffer1.write_all(b"123456789abcd123456789abcd").is_err());
        assert!(buffer2.write_all(b"123456789abcd123456789abcd").is_err());

        assert!(buffer1.write_all(b"1234567812345678").is_ok());
        assert!(buffer2.write_all(b"1234").is_err());
        Ok(())
    }

    #[test]
    fn test_truncate_basic_memory() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1000000);
        let mut temp_file = store.allocate_stream(None)?;

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

    #[test]
    fn test_truncate_to_zero_memory() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1000000);
        let mut temp_file = store.allocate_stream(None)?;

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

    #[test]
    fn test_truncate_no_effect_when_larger_memory() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1000000);
        let mut temp_file = store.allocate_stream(None)?;

        // Write some data
        let data = b"Hello, World!";
        temp_file.write_all(data)?;
        let original_size = temp_file.current_size();

        // Try to truncate to larger size (should have no effect)
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

    #[test]
    fn test_truncate_to_same_size_memory() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1000000);
        let mut temp_file = store.allocate_stream(None)?;

        // Write some data
        let data = b"Hello, World!";
        temp_file.write_all(data)?;
        let original_size = temp_file.current_size();

        // Truncate to same size (should have no effect)
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

    #[test]
    fn test_truncate_multiple_times_memory() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1000000);
        let mut temp_file = store.allocate_stream(None)?;

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

    #[test]
    fn test_truncate_after_sparse_writes_memory() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1000000);
        let mut temp_file = store.allocate_exclusive_buffer(None)?;

        // Write sparse data
        temp_file.write_at(0, b"start")?;
        temp_file.write_at(100, b"middle")?;
        temp_file.write_at(200, b"end")?;

        let initial_size = temp_file.size()?;
        assert_eq!(initial_size, 203); // 200 + 3 bytes for "end"

        // Truncate to middle of file
        temp_file.set_size(150)?;
        assert_eq!(temp_file.size()?, 150);

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

    #[test]
    fn test_truncate_write_position_adjustment_memory() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1000000);
        let mut temp_file = store.allocate_stream(None)?;

        // Write some data
        let data = b"Hello, World! This is test data.";
        temp_file.write_all(data)?;

        // Current write position should be at end
        assert_eq!(temp_file.current_size(), data.len() as u64);

        // Truncate to middle
        temp_file.truncate(10)?;
        assert_eq!(temp_file.current_size(), 10);

        // Write more data - should continue from current position
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

    #[test]
    fn test_truncate_budget_tracking_memory() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1000);
        let mut temp_file = store.allocate_stream(None)?;

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

    #[test]
    fn test_truncate_large_buffer_memory() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(10_000_000);
        let mut temp_file = store.allocate_stream(None)?;

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

    #[test]
    fn test_truncate_empty_buffer_memory() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1000000);
        let mut temp_file = store.allocate_stream(None)?;

        // Truncate empty file to zero (should be no-op)
        assert_eq!(temp_file.current_size(), 0);
        temp_file.truncate(0)?;
        assert_eq!(temp_file.current_size(), 0);

        // Truncate empty file to larger size (should be no-op)
        temp_file.truncate(100)?;
        assert_eq!(temp_file.current_size(), 0);

        Ok(())
    }

    #[test]
    fn test_truncate_read_at_boundaries_memory() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1000000);
        let mut temp_file = store.allocate_exclusive_buffer(None)?;

        // Write test data with pattern
        let data = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        temp_file.write_at(0, data)?;
        assert_eq!(temp_file.size()?, data.len() as u64);

        // Truncate at various boundaries
        temp_file.set_size(25)?;
        assert_eq!(temp_file.size()?, 25);

        // Test read at exactly the boundary
        let boundary_read = temp_file.read_at(24..25)?;
        assert_eq!(boundary_read.as_ref(), b"O");

        // Test read beyond boundary (should be empty)
        let beyond_read = temp_file.read_at(25..30)?;
        assert_eq!(beyond_read.len(), 0);

        // Test read spanning boundary
        let span_read = temp_file.read_at(23..30)?;
        assert_eq!(span_read.as_ref(), b"NO");

        Ok(())
    }

    #[test]
    fn test_truncate_and_rewrite_memory() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1000000);
        let mut temp_file = store.allocate_exclusive_buffer(None)?;

        // Initial write
        temp_file.write_at(0, b"Hello, World!")?;
        assert_eq!(temp_file.size()?, 13);

        // Truncate to middle
        temp_file.set_size(7)?;
        assert_eq!(temp_file.size()?, 7);

        // Overwrite from beginning
        temp_file.write_at(0, b"Hi")?;
        assert_eq!(temp_file.size()?, 7);

        // Write beyond current size
        temp_file.write_at(10, b"New!")?;
        assert_eq!(temp_file.size()?, 14);

        // Verify final data
        let final_data = temp_file.read_at(0..14)?;
        let expected = b"Hillo, \x00\x00\x00New!";
        assert_eq!(final_data.as_ref(), expected);

        Ok(())
    }

    #[test]
    fn test_truncate_stress_test_memory() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1000000);
        let mut temp_file = store.allocate_stream(None)?;

        // Write data in chunks and truncate repeatedly
        for i in 0..100 {
            let data = vec![i as u8; 100];
            temp_file.write_all(&data)?;

            // Every 10 iterations, truncate to half size
            if i % 10 == 9 {
                let current = temp_file.current_size();
                temp_file.truncate(current / 2)?;
            }
        }

        // Verify we can still read the data
        let final_size = temp_file.current_size();
        assert!(final_size > 0);

        let mut reader = temp_file.into_reader()?;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        assert_eq!(buf.len(), final_size as usize);

        Ok(())
    }
}
