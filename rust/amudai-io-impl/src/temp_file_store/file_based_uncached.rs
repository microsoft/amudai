use std::{io, ops::Range, path::Path, sync::Arc};

use amudai_budget_tracker::Allocation;
use amudai_bytes::Bytes;
use amudai_io::{
    ReadAt, StorageProfile,
    read_adapter::ReadAdapter,
    temp_file_store::{TemporaryBuffer, TemporaryFileStore, TemporaryWritable},
};

use crate::{
    fs::{DirectFileReader, DirectFileWriter, IoMode, direct_file::FileWriteResult},
    temp_file_store::file_based_common::{LocalTempContainer, LocalTempFile},
};

#[derive(Clone)]
pub struct LocalTempFileStore(Arc<LocalTempContainer>);

impl LocalTempFileStore {
    pub fn new(capacity: u64, parent_path: Option<&Path>) -> io::Result<LocalTempFileStore> {
        let container = LocalTempContainer::with_capacity(capacity, parent_path)?;
        Ok(LocalTempFileStore(Arc::new(container)))
    }

    pub fn path(&self) -> &Path {
        self.0.path()
    }

    pub fn available_space(&self) -> u64 {
        self.0.budget().remaining()
    }
}

impl LocalTempFileStore {
    fn create_temp_file_writer(&self, size_hint: Option<usize>) -> io::Result<LocalTempFileWriter> {
        let size_hint = size_hint.unwrap_or(0) as u64;
        let mut allocation = self.0.budget().allocate(0)?;
        allocation.reserve(size_hint)?;
        if size_hint >= 1024 * 1024 {
            allocation.set_reservation_slice(128 * 1024);
        }
        let file = crate::fs::create_temporary_in(self.path(), IoMode::Unbuffered)?;
        let writer = DirectFileWriter::from_file(file)?;
        Ok(LocalTempFileWriter {
            container: self.0.clone(),
            allocation,
            writer,
        })
    }

    fn create_temp_file_buffer(&self, size_hint: Option<usize>) -> io::Result<LocalTempFile> {
        let size_hint = size_hint.unwrap_or(0) as u64;
        let mut allocation = self.0.budget().allocate(0)?;
        allocation.reserve(size_hint)?;
        if size_hint >= 1024 * 1024 {
            allocation.set_reservation_slice(128 * 1024);
        }
        let file = crate::fs::create_temporary_in(self.path(), IoMode::Buffered)?;
        Ok(LocalTempFile::new(self.0.clone(), allocation, file))
    }
}

impl TemporaryFileStore for LocalTempFileStore {
    fn allocate_writable(
        &self,
        size_hint: Option<usize>,
    ) -> io::Result<Box<dyn TemporaryWritable>> {
        self.create_temp_file_writer(size_hint)
            .map(|f| Box::new(f) as _)
    }

    fn allocate_buffer(&self, size_hint: Option<usize>) -> io::Result<Box<dyn TemporaryBuffer>> {
        self.create_temp_file_buffer(size_hint)
            .map(|f| Box::new(f) as _)
    }
}

struct LocalTempFileWriter {
    container: Arc<LocalTempContainer>,
    allocation: Allocation,
    writer: DirectFileWriter,
}

impl LocalTempFileWriter {
    fn ensure_allocation_for_size(&mut self, size: u64) -> io::Result<()> {
        assert_eq!(self.allocation.amount(), self.current_size());
        if size > self.current_size() {
            let grow = size - self.current_size();
            self.allocation.grow(grow)?;
        }
        Ok(())
    }

    fn shrink_allocation_to_size(&mut self) {
        if self.current_size() <= self.allocation.amount() {
            self.allocation.shrink_to(self.current_size());
        }
    }

    fn into_read_at_impl(mut self) -> io::Result<LocalTempFileReadAt> {
        self.shrink_allocation_to_size();
        let FileWriteResult {
            file, logical_size, ..
        } = self.writer.close()?;
        let reader = DirectFileReader::new(file, Some(logical_size));
        Ok(LocalTempFileReadAt {
            _container: self.container,
            _allocation: self.allocation,
            reader,
        })
    }
}

impl io::Write for LocalTempFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let prev_pos = self.writer.position();
        let end_pos = prev_pos + buf.len() as u64;
        self.ensure_allocation_for_size(end_pos)?;
        match self.writer.write_all(buf) {
            Ok(()) => {
                assert_eq!(self.writer.position(), end_pos);
                Ok(buf.len())
            }
            Err(e) => {
                let _ = self.writer.truncate(prev_pos);
                self.shrink_allocation_to_size();
                Err(e)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl TemporaryWritable for LocalTempFileWriter {
    fn current_size(&self) -> u64 {
        self.writer.position()
    }

    fn truncate(&mut self, end_pos: u64) -> io::Result<()> {
        assert_eq!(self.allocation.amount(), self.current_size());
        if end_pos < self.current_size() {
            self.writer.truncate(end_pos)?;
            self.allocation.shrink_to(end_pos);
        }
        Ok(())
    }

    fn into_read_at(self: Box<Self>) -> io::Result<Arc<dyn ReadAt>> {
        self.into_read_at_impl().map(|r| Arc::new(r) as _)
    }

    fn into_reader(self: Box<Self>) -> io::Result<Box<dyn io::Read>> {
        self.into_read_at_impl()
            .map(|r| Box::new(ReadAdapter::new(r)) as Box<dyn io::Read>)
    }
}

struct LocalTempFileReadAt {
    _container: Arc<LocalTempContainer>,
    _allocation: Allocation,
    reader: DirectFileReader,
}

impl ReadAt for LocalTempFileReadAt {
    fn size(&self) -> std::io::Result<u64> {
        self.reader.size()
    }

    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes> {
        self.reader.read_at(range)
    }

    fn storage_profile(&self) -> StorageProfile {
        self.reader.storage_profile()
    }
}
