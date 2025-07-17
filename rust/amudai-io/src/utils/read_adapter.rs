//! A helper that turns any `ReadAt` implementation into a standard `std::io::Read`.

use crate::ReadAt;

/// A helper that turns any `ReadAt` implementation into a standard `std::io::Read` and `std::io::Seek`.
///
/// This adapter maintains an internal position and reads sequentially from the underlying
/// `ReadAt` source, making it compatible with APIs that expect `std::io::Read`. It also
/// supports seeking to different positions within the underlying source.
pub struct ReadAdapter<R> {
    inner: R,
    pos: u64,
    /// Cached size of the inner `ReadAt`
    size: Option<u64>,
}

impl<R> ReadAdapter<R> {
    /// Creates a new `ReadAdapter` wrapping the given `ReadAt` implementation.
    ///
    /// The adapter starts reading from position 0.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            pos: 0,
            size: None,
        }
    }

    /// Creates a new `ReadAdapter` starting from the specified position.
    pub fn new_at_position(inner: R, pos: u64) -> Self {
        Self {
            inner,
            pos,
            size: None,
        }
    }

    /// Returns the current read position.
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// Returns a reference to the underlying `ReadAt` implementation.
    pub fn inner(&self) -> &R {
        &self.inner
    }

    /// Consumes the adapter and returns the underlying `ReadAt` implementation.
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: ReadAt> ReadAdapter<R> {
    fn size(&mut self) -> std::io::Result<u64> {
        if let Some(size) = self.size {
            Ok(size)
        } else {
            let size = self.inner.size()?;
            self.size = Some(size);
            Ok(size)
        }
    }
}

impl<R: ReadAt> std::io::Read for ReadAdapter<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        // Get the size of the underlying source
        let size = self.size()?;

        // If we're at or past the end, return 0 (EOF)
        if self.pos >= size {
            return Ok(0);
        }

        // Calculate how much we can read
        let available = size - self.pos;
        let to_read = std::cmp::min(buf.len() as u64, available);

        if to_read == 0 {
            return Ok(0);
        }

        // Read from the underlying source
        let range = self.pos..self.pos + to_read;
        let bytes = self.inner.read_at(range)?;
        let bytes_read = bytes.len();

        // Copy to the output buffer
        buf[..bytes_read].copy_from_slice(&bytes);

        // Update position
        self.pos += bytes_read as u64;

        Ok(bytes_read)
    }
}

impl<R: ReadAt> std::io::Seek for ReadAdapter<R> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        use std::io::SeekFrom;

        let size = self.size()?;

        let new_pos = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(offset) => {
                if offset >= 0 {
                    size.saturating_add(offset as u64)
                } else {
                    size.saturating_sub((-offset) as u64)
                }
            }
            SeekFrom::Current(offset) => {
                if offset >= 0 {
                    self.pos.saturating_add(offset as u64)
                } else {
                    self.pos.saturating_sub((-offset) as u64)
                }
            }
        };

        // Allow seeking past the end of the file, as per std::io::Seek behavior
        self.pos = new_pos;
        Ok(self.pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use amudai_bytes::Bytes;
    use std::io::{Read, Seek};

    #[test]
    fn test_read_adapter_basic() {
        let data = b"Hello, World!";
        let bytes = Bytes::copy_from_slice(data);
        let mut adapter = ReadAdapter::new(bytes);

        let mut buf = [0u8; 5];
        let n = adapter.read(&mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"Hello");
        assert_eq!(adapter.position(), 5);

        let n = adapter.read(&mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b", Wor");
        assert_eq!(adapter.position(), 10);

        let mut buf = [0u8; 10];
        let n = adapter.read(&mut buf).unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf[..3], b"ld!");
        assert_eq!(adapter.position(), 13);

        // EOF
        let n = adapter.read(&mut buf).unwrap();
        assert_eq!(n, 0);
        assert_eq!(adapter.position(), 13);
    }

    #[test]
    fn test_read_adapter_empty_source() {
        let bytes = Bytes::new();
        let mut adapter = ReadAdapter::new(bytes);

        let mut buf = [0u8; 10];
        let n = adapter.read(&mut buf).unwrap();
        assert_eq!(n, 0);
        assert_eq!(adapter.position(), 0);
    }

    #[test]
    fn test_read_adapter_empty_buffer() {
        let data = b"Hello";
        let bytes = Bytes::copy_from_slice(data);
        let mut adapter = ReadAdapter::new(bytes);

        let mut buf = [];
        let n = adapter.read(&mut buf).unwrap();
        assert_eq!(n, 0);
        assert_eq!(adapter.position(), 0);
    }

    #[test]
    fn test_read_adapter_start_at_position() {
        let data = b"Hello, World!";
        let bytes = Bytes::copy_from_slice(data);
        let mut adapter = ReadAdapter::new_at_position(bytes, 7);

        let mut buf = [0u8; 6];
        let n = adapter.read(&mut buf).unwrap();
        assert_eq!(n, 6);
        assert_eq!(&buf, b"World!");
        assert_eq!(adapter.position(), 13);

        // EOF
        let n = adapter.read(&mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_read_adapter_start_past_end() {
        let data = b"Hello";
        let bytes = Bytes::copy_from_slice(data);
        let mut adapter = ReadAdapter::new_at_position(bytes, 10);

        let mut buf = [0u8; 5];
        let n = adapter.read(&mut buf).unwrap();
        assert_eq!(n, 0);
        assert_eq!(adapter.position(), 10);
    }

    #[test]
    fn test_read_adapter_into_inner() {
        let data = b"Hello";
        let bytes = Bytes::copy_from_slice(data);
        let adapter = ReadAdapter::new(bytes.clone());

        let inner = adapter.into_inner();
        assert_eq!(inner.size().unwrap(), 5);
    }

    #[test]
    fn test_read_adapter_large_buffer() {
        let data = b"Hi";
        let bytes = Bytes::copy_from_slice(data);
        let mut adapter = ReadAdapter::new(bytes);

        let mut buf = [0u8; 10];
        let n = adapter.read(&mut buf).unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], b"Hi");
        assert_eq!(adapter.position(), 2);
    }

    #[test]
    fn test_read_adapter_std_io_compatibility() {
        let data = b"Hello, World!";
        let bytes = Bytes::copy_from_slice(data);
        let mut adapter = ReadAdapter::new(bytes);

        // Test using std::io::Read methods
        let mut result = Vec::new();
        adapter.read_to_end(&mut result).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_read_adapter_seek_and_read_combined() {
        use std::io::{Read, Seek};

        let data = b"Hello, World!";
        let bytes = Bytes::copy_from_slice(data);
        let mut adapter = ReadAdapter::new(bytes);

        // Test combined seek and read operations
        adapter.seek(std::io::SeekFrom::Start(7)).unwrap();
        let mut result = Vec::new();
        adapter.read_to_end(&mut result).unwrap();
        assert_eq!(result, b"World!");

        // Seek back to start and read everything
        adapter.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut result = Vec::new();
        adapter.read_to_end(&mut result).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_read_adapter_seek() {
        let data = b"Hello, World!";
        let bytes = Bytes::copy_from_slice(data);
        let mut adapter = ReadAdapter::new(bytes);

        let mut buf = [0u8; 5];
        let n = adapter.read(&mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"Hello");
        assert_eq!(adapter.position(), 5);

        // Seek back to the beginning
        adapter.seek(std::io::SeekFrom::Start(0)).unwrap();
        let n = adapter.read(&mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"Hello");
        assert_eq!(adapter.position(), 5);

        // Seek past the end
        adapter.seek(std::io::SeekFrom::Start(20)).unwrap();
        let n = adapter.read(&mut buf).unwrap();
        assert_eq!(n, 0);
        assert_eq!(adapter.position(), 20);

        // Seek to a negative offset from the end
        adapter.seek(std::io::SeekFrom::End(-6)).unwrap();
        let mut buf = [0u8; 6]; // Fix buffer size to match expected read
        let n = adapter.read(&mut buf).unwrap();
        assert_eq!(n, 6);
        assert_eq!(&buf, b"World!");
        assert_eq!(adapter.position(), 13); // Fix expected position: 7 + 6 = 13
    }

    #[test]
    fn test_read_adapter_seek_start() {
        let data = b"Hello, World!";
        let bytes = Bytes::copy_from_slice(data);
        let mut adapter = ReadAdapter::new(bytes);

        // Seek to position 7
        let new_pos = adapter.seek(std::io::SeekFrom::Start(7)).unwrap();
        assert_eq!(new_pos, 7);
        assert_eq!(adapter.position(), 7);

        // Read from that position
        let mut buf = [0u8; 6];
        let n = adapter.read(&mut buf).unwrap();
        assert_eq!(n, 6);
        assert_eq!(&buf, b"World!");
    }

    #[test]
    fn test_read_adapter_seek_end() {
        let data = b"Hello, World!";
        let bytes = Bytes::copy_from_slice(data);
        let mut adapter = ReadAdapter::new(bytes);

        // Seek to 6 bytes before end
        let new_pos = adapter.seek(std::io::SeekFrom::End(-6)).unwrap();
        assert_eq!(new_pos, 7);
        assert_eq!(adapter.position(), 7);

        // Read from that position
        let mut buf = [0u8; 6];
        let n = adapter.read(&mut buf).unwrap();
        assert_eq!(n, 6);
        assert_eq!(&buf, b"World!");
    }

    #[test]
    fn test_read_adapter_seek_current() {
        let data = b"Hello, World!";
        let bytes = Bytes::copy_from_slice(data);
        let mut adapter = ReadAdapter::new(bytes);

        // Read first 5 bytes
        let mut buf = [0u8; 5];
        adapter.read(&mut buf).unwrap();
        assert_eq!(adapter.position(), 5);

        // Seek forward 2 bytes from current position
        let new_pos = adapter.seek(std::io::SeekFrom::Current(2)).unwrap();
        assert_eq!(new_pos, 7);
        assert_eq!(adapter.position(), 7);

        // Read from that position
        let mut buf = [0u8; 6];
        let n = adapter.read(&mut buf).unwrap();
        assert_eq!(n, 6);
        assert_eq!(&buf, b"World!");
    }

    #[test]
    fn test_read_adapter_seek_past_end() {
        let data = b"Hello";
        let bytes = Bytes::copy_from_slice(data);
        let mut adapter = ReadAdapter::new(bytes);

        // Seek past the end
        let new_pos = adapter.seek(std::io::SeekFrom::Start(10)).unwrap();
        assert_eq!(new_pos, 10);
        assert_eq!(adapter.position(), 10);

        // Reading should return 0 (EOF)
        let mut buf = [0u8; 5];
        let n = adapter.read(&mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_read_adapter_seek_before_start() {
        let data = b"Hello";
        let bytes = Bytes::copy_from_slice(data);
        let mut adapter = ReadAdapter::new(bytes);

        // Seek to middle, then try to seek before start using negative current offset
        adapter.seek(std::io::SeekFrom::Start(3)).unwrap();
        let new_pos = adapter.seek(std::io::SeekFrom::Current(-5)).unwrap();
        // Should saturate to 0
        assert_eq!(new_pos, 0);
        assert_eq!(adapter.position(), 0);

        // Should be able to read from beginning
        let mut buf = [0u8; 5];
        let n = adapter.read(&mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"Hello");
    }
}
