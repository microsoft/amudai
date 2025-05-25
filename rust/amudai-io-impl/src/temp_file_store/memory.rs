use std::{io, sync::Arc};

use amudai_budget_tracker::{Allocation, Budget};
use amudai_bytes::{Bytes, BytesMut};
use amudai_io::{
    temp_file_store::{TemporaryBuffer, TemporaryFileStore, TemporaryWritable},
    verify, ReadAt, StorageProfile,
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
    fn allocate_writable(
        &self,
        size_hint: Option<usize>,
    ) -> io::Result<Box<dyn TemporaryWritable>> {
        self.create_temp_buffer(size_hint)
            .map(|buf| Box::new(buf) as Box<dyn TemporaryWritable>)
    }

    fn allocate_buffer(&self, size_hint: Option<usize>) -> io::Result<Box<dyn TemporaryBuffer>> {
        self.create_temp_buffer(size_hint)
            .map(|buf| Box::new(buf) as Box<dyn TemporaryBuffer>)
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
}

impl io::Write for InMemoryTempBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_at(self.write_pos, buf)?;
        self.write_pos += buf.len() as u64;
        Ok(buf.len())
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
        verify!(range.end >= range.start);
        let end = range.end.min(self.data.len() as u64);
        verify!(end >= range.start);
        if end > range.start {
            Ok(Bytes::from(&self.data[range.start as usize..end as usize]))
        } else {
            Ok(Bytes::new())
        }
    }

    fn storage_profile(&self) -> StorageProfile {
        StorageProfile {
            min_io_size: 1,
            max_io_size: 1024 * 1024,
        }
    }
}

impl TemporaryWritable for InMemoryTempBuffer {
    fn current_size(&self) -> u64 {
        self.data.len() as u64
    }

    fn truncate(&mut self, end_pos: u64) -> io::Result<()> {
        assert_eq!(self.allocation.amount(), self.current_size());
        if end_pos < self.current_size() {
            self.data.truncate(end_pos as usize);
            self.allocation.shrink_to(end_pos);
            self.write_pos = std::cmp::min(self.write_pos, end_pos);
        }
        Ok(())
    }

    fn into_read_at(self: Box<Self>) -> io::Result<Arc<dyn amudai_io::ReadAt>> {
        Ok(Arc::new(self.make_reader()))
    }

    fn into_reader(self: Box<Self>) -> io::Result<Box<dyn io::Read>> {
        Ok(Box::new(self.make_reader()))
    }
}

impl TemporaryBuffer for InMemoryTempBuffer {
    fn write_at(&mut self, pos: u64, buf: &[u8]) -> io::Result<()> {
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
            max_io_size: self.bytes.len(),
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
        let writable = store.allocate_writable(Some(512))?;
        assert_eq!(store.available_space(), 512); // Initial reservation
        assert_eq!(writable.current_size(), 0);
        Ok(())
    }

    #[test]
    fn test_in_memory_temp_file_store_allocate_buffer() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1024);
        let buffer = store.allocate_buffer(Some(512))?;
        assert_eq!(store.available_space(), 512);
        drop(buffer);
        assert_eq!(store.available_space(), 1024);
        Ok(())
    }

    #[test]
    fn test_in_memory_temp_file_store_allocate_no_hint() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1024);
        let writable = store.allocate_writable(None)?;
        assert_eq!(store.available_space(), 1024); // Initial reservation
        assert_eq!(writable.current_size(), 0);
        Ok(())
    }

    #[test]
    fn test_in_memory_temp_file_store_allocate_exceed_capacity() {
        let store = InMemoryTempFileStore::new(1024);
        let result = store.allocate_writable(Some(1024 * 1024));
        assert!(result.is_err());
    }

    #[test]
    fn test_in_memory_temp_buffer_write_and_read() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1024 * 1024);
        let mut buffer = store.allocate_writable(None)?;

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
        let mut buffer = store.allocate_buffer(None)?;

        let data1 = b"hello";
        buffer.write_at(0, data1)?;
        assert_eq!(buffer.current_size(), data1.len() as u64);

        let data2 = b" world";
        buffer.write_at(data1.len() as u64, data2)?;
        assert_eq!(buffer.current_size(), (data1.len() + data2.len()) as u64);

        let buf = buffer.read_at(0..(data1.len() + data2.len()) as u64)?;
        assert_eq!(buf.as_ref(), b"hello world");
        Ok(())
    }

    #[test]
    fn test_in_memory_temp_buffer_write_at_overwrite() -> io::Result<()> {
        let store = InMemoryTempFileStore::new(1024 * 1024);
        let mut buffer = store.allocate_buffer(None)?;

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
        let mut buffer = store.allocate_buffer(None)?;

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
        let mut buffer = store.allocate_writable(None)?;

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
        let mut buffer = store.allocate_writable(None)?;

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
        let mut buffer = store.allocate_buffer(None)?;

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
        let mut buffer = store.allocate_writable(None)?;

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
        let mut buffer1 = store.allocate_buffer(None)?;
        let mut buffer2 = store.allocate_buffer(None)?;

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
}
