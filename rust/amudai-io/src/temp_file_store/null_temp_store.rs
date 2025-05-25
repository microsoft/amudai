use std::sync::Arc;

use amudai_bytes::Bytes;

use crate::ReadAt;

use super::{TemporaryBuffer, TemporaryFileStore, TemporaryWritable};

pub struct NullTempFileStore;

impl TemporaryFileStore for NullTempFileStore {
    fn allocate_writable(
        &self,
        _size_hint: Option<usize>,
    ) -> std::io::Result<Box<dyn TemporaryWritable>> {
        Ok(Box::new(NullTempBuffer))
    }

    fn allocate_buffer(
        &self,
        _size_hint: Option<usize>,
    ) -> std::io::Result<Box<dyn TemporaryBuffer>> {
        Ok(Box::new(NullTempBuffer))
    }
}

struct NullTempBuffer;

impl std::io::Write for NullTempBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl TemporaryWritable for NullTempBuffer {
    fn current_size(&self) -> u64 {
        0
    }

    fn truncate(&mut self, _end_pos: u64) -> std::io::Result<()> {
        Ok(())
    }

    fn into_read_at(self: Box<Self>) -> std::io::Result<Arc<dyn ReadAt>> {
        Ok(Arc::new(NullTempBuffer))
    }

    fn into_reader(self: Box<Self>) -> std::io::Result<Box<dyn std::io::Read>> {
        Ok(Box::new(std::io::empty()))
    }
}

impl ReadAt for NullTempBuffer {
    fn size(&self) -> std::io::Result<u64> {
        Ok(0)
    }

    fn read_at(&self, _range: std::ops::Range<u64>) -> std::io::Result<Bytes> {
        Ok(Bytes::new())
    }

    fn storage_profile(&self) -> crate::StorageProfile {
        Default::default()
    }
}

impl TemporaryBuffer for NullTempBuffer {
    fn write_at(&mut self, _pos: u64, _buf: &[u8]) -> std::io::Result<()> {
        Ok(())
    }
}
