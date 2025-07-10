use std::sync::Arc;

use amudai_bytes::Bytes;

use crate::{ExclusiveIoBuffer, IoStream, ReadAt, SharedIoBuffer, WriteAt};

use super::TemporaryFileStore;

pub struct NullTempFileStore;

impl TemporaryFileStore for NullTempFileStore {
    fn allocate_stream(&self, _size_hint: Option<usize>) -> std::io::Result<Box<dyn IoStream>> {
        Ok(Box::new(NullTempBuffer))
    }

    fn allocate_exclusive_buffer(
        &self,
        _size_hint: Option<usize>,
    ) -> std::io::Result<Box<dyn ExclusiveIoBuffer>> {
        Ok(Box::new(NullTempBuffer))
    }

    fn allocate_shared_buffer(
        &self,
        _size_hint: Option<usize>,
    ) -> std::io::Result<Box<dyn SharedIoBuffer>> {
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

impl WriteAt for NullTempBuffer {
    fn write_at(&self, _pos: u64, _buf: &[u8]) -> std::io::Result<()> {
        Ok(())
    }

    fn storage_profile(&self) -> crate::StorageProfile {
        Default::default()
    }
}

impl IoStream for NullTempBuffer {
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

impl ExclusiveIoBuffer for NullTempBuffer {
    fn set_size(&mut self, _size: u64) -> std::io::Result<()> {
        Ok(())
    }

    fn write_at(&mut self, _pos: u64, _buf: &[u8]) -> std::io::Result<()> {
        Ok(())
    }

    fn into_read_at(self: Box<Self>) -> std::io::Result<Arc<dyn ReadAt>> {
        Ok(Arc::new(NullTempBuffer))
    }

    fn into_reader(self: Box<Self>) -> std::io::Result<Box<dyn std::io::Read>> {
        Ok(Box::new(std::io::empty()))
    }
}

impl SharedIoBuffer for NullTempBuffer {
    fn set_size(&self, _size: u64) -> std::io::Result<()> {
        Ok(())
    }

    fn into_read_at(self: Box<Self>) -> std::io::Result<Arc<dyn ReadAt>> {
        Ok(Arc::new(NullTempBuffer))
    }

    fn into_reader(self: Box<Self>) -> std::io::Result<Box<dyn std::io::Read>> {
        Ok(Box::new(std::io::empty()))
    }
}
