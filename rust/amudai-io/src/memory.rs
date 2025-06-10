use std::ops::Range;

use amudai_bytes::Bytes;

use crate::{ReadAt, SealingWrite, StorageProfile, verify};

impl<T> ReadAt for T
where
    T: details::SliceBytes + Send + Sync + 'static,
{
    fn size(&self) -> std::io::Result<u64> {
        Ok(self.len() as u64)
    }

    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes> {
        verify!(range.end >= range.start);
        let pos = range.start as usize;
        let len = (range.end - range.start) as usize;
        let content_len = self.len();
        if pos > content_len {
            return Ok(Bytes::new());
        }
        let len = std::cmp::min(len, content_len - pos);
        Ok(self.slice(pos..pos + len))
    }

    fn storage_profile(&self) -> StorageProfile {
        StorageProfile {
            min_io_size: 1,
            max_io_size: self.len().min(StorageProfile::default().max_io_size),
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

mod details {
    use std::ops::Range;

    use amudai_bytes::Bytes;

    pub trait SliceBytes {
        fn len(&self) -> usize;
        fn slice(&self, range: Range<usize>) -> Bytes;
    }

    impl SliceBytes for Bytes {
        fn len(&self) -> usize {
            Bytes::len(self)
        }

        fn slice(&self, range: Range<usize>) -> Bytes {
            Bytes::slice(self, range)
        }
    }

    impl SliceBytes for Vec<u8> {
        fn len(&self) -> usize {
            Vec::len(self)
        }

        fn slice(&self, range: Range<usize>) -> Bytes {
            Bytes::copy_from_slice(&self[range])
        }
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
