use std::{fs::File, io, ops::Range, path::Path, sync::Arc};

use amudai_budget_tracker::{Allocation, Budget};
use amudai_bytes::{Bytes, BytesMut};
use amudai_io::{
    temp_file_store::{TemporaryBuffer, TemporaryFileStore, TemporaryWritable},
    verify, ReadAt, StorageProfile,
};

#[derive(Clone)]
pub struct LocalTempFileStore(Arc<LocalTempContainer>);

impl LocalTempFileStore {
    pub fn new(capacity: u64, parent_path: Option<&Path>) -> io::Result<LocalTempFileStore> {
        let container = if let Some(parent) = parent_path {
            tempfile::tempdir_in(parent)?
        } else {
            tempfile::tempdir()?
        };
        let budget = Budget::new(capacity);
        Ok(LocalTempFileStore(Arc::new(LocalTempContainer {
            budget,
            container,
        })))
    }

    pub fn path(&self) -> &Path {
        self.0.container.path()
    }

    pub fn available_space(&self) -> u64 {
        self.0.budget.remaining()
    }

    fn create_temp_file(&self, size_hint: Option<usize>) -> io::Result<LocalTempFile> {
        let size_hint = size_hint.unwrap_or(0) as u64;
        let mut allocation = self.0.budget.allocate(0)?;
        allocation.reserve(size_hint)?;
        if size_hint >= 1024 * 1024 {
            allocation.set_reservation_slice(128 * 1024);
        }
        let file = tempfile::tempfile_in(self.path())?;
        Ok(LocalTempFile {
            _container: self.0.clone(),
            allocation,
            file,
            pos: 0,
            size: 0,
        })
    }
}

struct LocalTempContainer {
    budget: Budget,
    container: tempfile::TempDir,
}

impl TemporaryFileStore for LocalTempFileStore {
    fn allocate_writable(
        &self,
        size_hint: Option<usize>,
    ) -> io::Result<Box<dyn TemporaryWritable>> {
        self.create_temp_file(size_hint).map(|f| Box::new(f) as _)
    }

    fn allocate_buffer(&self, size_hint: Option<usize>) -> io::Result<Box<dyn TemporaryBuffer>> {
        self.create_temp_file(size_hint).map(|f| Box::new(f) as _)
    }
}

struct LocalTempFile {
    _container: Arc<LocalTempContainer>,
    allocation: Allocation,
    file: File,
    pos: u64,
    size: u64,
}

impl LocalTempFile {
    fn ensure_allocation_for_size(&mut self, size: u64) -> io::Result<()> {
        assert_eq!(self.allocation.amount(), self.current_size());
        if size > self.size {
            let grow = size - self.size;
            self.allocation.grow(grow)?;
        }
        Ok(())
    }

    fn shrink_allocation_to_size(&mut self) {
        if self.size <= self.allocation.amount() {
            self.allocation.shrink_to(self.size);
        }
    }
}

impl io::Write for LocalTempFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_at(self.pos, buf)?;
        self.pos += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl ReadAt for LocalTempFile {
    fn size(&self) -> std::io::Result<u64> {
        Ok(self.size)
    }

    fn read_at(&self, range: Range<u64>) -> io::Result<Bytes> {
        verify!(range.end >= range.start);
        let end = range.end.min(self.size);
        if end > range.start {
            let len = (end - range.start) as usize;
            let mut buf = BytesMut::zeroed(len);
            amudai_io::file::file_read_at_exact(&self.file, range.start, &mut buf)?;
            Ok(Bytes::from(buf))
        } else {
            Ok(Bytes::new())
        }
    }

    fn storage_profile(&self) -> StorageProfile {
        StorageProfile::default()
    }
}

impl TemporaryWritable for LocalTempFile {
    fn current_size(&self) -> u64 {
        self.size
    }

    fn truncate(&mut self, end_pos: u64) -> io::Result<()> {
        assert_eq!(self.allocation.amount(), self.current_size());
        if end_pos < self.current_size() {
            self.file.set_len(end_pos)?;
            self.allocation.shrink_to(end_pos);
            self.pos = std::cmp::min(self.pos, end_pos);
            self.size = end_pos;
        }
        Ok(())
    }

    fn into_read_at(mut self: Box<Self>) -> io::Result<Arc<dyn ReadAt>> {
        self.shrink_allocation_to_size();
        self.pos = 0;
        Ok(Arc::<LocalTempFile>::from(self) as _)
    }

    fn into_reader(mut self: Box<Self>) -> io::Result<Box<dyn io::Read>> {
        self.shrink_allocation_to_size();
        self.pos = 0;
        Ok(self)
    }
}

impl TemporaryBuffer for LocalTempFile {
    fn write_at(&mut self, pos: u64, buf: &[u8]) -> io::Result<()> {
        let end_pos = pos + buf.len() as u64;
        self.ensure_allocation_for_size(end_pos)?;
        match amudai_io::file::file_write_at(&self.file, pos, buf) {
            Ok(()) => {
                self.size = std::cmp::max(self.size, end_pos);
                Ok(())
            }
            Err(e) => {
                let _ = self.file.set_len(self.size);
                self.shrink_allocation_to_size();
                Err(e)
            }
        }
    }
}

impl io::Read for LocalTempFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        assert!(self.pos <= self.size);
        let max_len = (self.size - self.pos) as usize;
        let len = std::cmp::min(max_len, buf.len());
        if len != 0 {
            amudai_io::file::file_read_at_exact(&self.file, self.pos, &mut buf[..len])?;
            self.pos += len as u64;
            Ok(len)
        } else {
            Ok(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use amudai_io::temp_file_store::TemporaryFileStore;

    use super::LocalTempFileStore;

    #[test]
    fn test_create_store() -> io::Result<()> {
        let store = LocalTempFileStore::new(100000, None)?;
        println!("{}", store.path().to_str().unwrap());
        assert_eq!(store.available_space(), 100000);
        Ok(())
    }

    #[test]
    fn test_write_temp_file_sequential() -> io::Result<()> {
        let store = LocalTempFileStore::new(10000000, None)?;
        let mut temp_file = store.allocate_writable(None)?;
        let const_buf = (0..100u8).collect::<Vec<_>>();
        for _ in 0..100 {
            temp_file.write_all(&const_buf)?;
        }
        assert_eq!(temp_file.current_size(), 10000);
        assert_eq!(store.available_space(), 10000000 - 10000);

        let mut reader = temp_file.into_reader()?;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        assert_eq!(buf.len(), 10000);
        drop(reader);
        assert_eq!(store.available_space(), 10000000);
        Ok(())
    }

    #[test]
    fn test_write_temp_file_sparse() -> io::Result<()> {
        let store = LocalTempFileStore::new(10000000, None)?;
        let mut temp_file = store.allocate_buffer(None)?;
        let const_buf = (0..100u8).collect::<Vec<_>>();
        for i in 0..100 {
            temp_file.write_at(i * 1000, &const_buf)?;
        }
        assert_eq!(temp_file.current_size(), 99100);
        assert_eq!(store.available_space(), 10000000 - 99100);

        let mut reader = temp_file.into_reader()?;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        assert_eq!(buf.len(), 99100);
        assert_eq!(&buf[..4], &[0, 1, 2, 3]);
        assert_eq!(&buf[100..104], &[0, 0, 0, 0]);
        drop(reader);
        assert_eq!(store.available_space(), 10000000);
        Ok(())
    }
}
