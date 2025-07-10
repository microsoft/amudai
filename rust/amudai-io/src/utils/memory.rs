//! Implementations of common I/O traits for memory buffers.

use std::ops::Range;

use amudai_bytes::Bytes;

use crate::{ExclusiveIoBuffer, IoStream, ReadAt, SealingWrite, StorageProfile, verify};

impl ReadAt for Vec<u8> {
    fn size(&self) -> std::io::Result<u64> {
        Ok(self.len() as u64)
    }

    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes> {
        verify!(range.end >= range.start);
        let start = range.start as usize;
        let end = std::cmp::min(range.end as usize, self.len());
        if start >= end {
            return Ok(Bytes::new());
        }
        Ok(Bytes::copy_from_slice(&self[start..end]))
    }

    fn storage_profile(&self) -> StorageProfile {
        StorageProfile {
            min_io_size: 1,
            max_io_size: StorageProfile::default().max_io_size,
        }
    }
}

impl ReadAt for Bytes {
    fn size(&self) -> std::io::Result<u64> {
        Ok(self.len() as u64)
    }

    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes> {
        verify!(range.end >= range.start);
        let start = range.start as usize;
        let end = std::cmp::min(range.end as usize, self.len());
        if start >= end {
            return Ok(Bytes::new());
        }
        Ok(self.slice(start..end))
    }

    fn storage_profile(&self) -> StorageProfile {
        StorageProfile {
            min_io_size: 1,
            max_io_size: StorageProfile::default().max_io_size,
        }
    }
}

impl SealingWrite for Vec<u8> {
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.extend_from_slice(buf);
        Ok(())
    }

    fn seal(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn storage_profile(&self) -> StorageProfile {
        StorageProfile {
            min_io_size: 1,
            ..Default::default()
        }
    }
}

impl IoStream for Vec<u8> {
    fn current_size(&self) -> u64 {
        self.len() as u64
    }

    fn truncate(&mut self, end_pos: u64) -> std::io::Result<()> {
        Vec::truncate(self, end_pos as usize);
        Ok(())
    }

    fn into_read_at(self: Box<Self>) -> std::io::Result<std::sync::Arc<dyn ReadAt>> {
        Ok(std::sync::Arc::new(*self))
    }

    fn into_reader(self: Box<Self>) -> std::io::Result<Box<dyn std::io::Read>> {
        Ok(Box::new(std::io::Cursor::new(*self)))
    }
}

impl ExclusiveIoBuffer for Vec<u8> {
    fn set_size(&mut self, size: u64) -> std::io::Result<()> {
        self.resize(size as usize, 0);
        Ok(())
    }

    fn write_at(&mut self, pos: u64, buf: &[u8]) -> std::io::Result<()> {
        let pos = pos as usize;
        let end_pos = pos + buf.len();

        if end_pos > self.len() {
            self.reserve(end_pos - self.len());
        }

        if pos > self.len() {
            self.resize(pos, 0);
            self.extend_from_slice(buf);
        } else if end_pos <= self.len() {
            self[pos..end_pos].copy_from_slice(buf);
        } else {
            self.truncate(pos);
            self.extend_from_slice(buf);
        }
        Ok(())
    }

    fn into_read_at(self: Box<Self>) -> std::io::Result<std::sync::Arc<dyn ReadAt>> {
        Ok(std::sync::Arc::new(*self))
    }

    fn into_reader(self: Box<Self>) -> std::io::Result<Box<dyn std::io::Read>> {
        Ok(Box::new(std::io::Cursor::new(*self)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{ReadAt, SealingWrite};

    #[test]
    fn test_mem_writer() {
        let mut buffer = Vec::<u8>::new();
        buffer.write_all(b"abcd").unwrap();
        buffer.write_all(b"123").unwrap();
        buffer.seal().unwrap();
        assert_eq!(buffer, b"abcd123");
    }

    #[test]
    fn test_mem_reader() {
        let blob = b"abcd123".to_vec();
        assert_eq!(blob.size().unwrap(), 7);
        let buf = blob.read_at(1..3).unwrap();
        assert_eq!(buf.as_ref(), b"bc");
        let buf = blob.read_at(4..200).unwrap();
        assert_eq!(buf.as_ref(), b"123");

        let blob = Arc::new(blob) as Arc<dyn ReadAt>;
        let buf = blob.read_at(1..3).unwrap();
        assert_eq!(buf.as_ref(), b"bc");
    }
}
