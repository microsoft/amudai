use std::{fs::File, ops::Range, path::Path, sync::Arc};

use amudai_budget_tracker::{Allocation, Budget};
use amudai_bytes::{Bytes, BytesMut};
use amudai_io::{
    ReadAt, StorageProfile,
    temp_file_store::{TemporaryBuffer, TemporaryWritable},
    verify,
};

pub struct LocalTempContainer {
    budget: Budget,
    container: tempfile::TempDir,
}

impl LocalTempContainer {
    pub fn new(budget: Budget, container: tempfile::TempDir) -> LocalTempContainer {
        LocalTempContainer { budget, container }
    }

    pub fn with_capacity(
        capacity: u64,
        parent_path: Option<&Path>,
    ) -> std::io::Result<LocalTempContainer> {
        let container = if let Some(parent) = parent_path {
            tempfile::tempdir_in(parent)?
        } else {
            tempfile::tempdir()?
        };
        let budget = Budget::new(capacity);
        Ok(LocalTempContainer::new(budget, container))
    }

    pub fn budget(&self) -> &Budget {
        &self.budget
    }

    pub fn path(&self) -> &Path {
        self.container.path()
    }
}

pub struct LocalTempFile {
    _container: Arc<LocalTempContainer>,
    allocation: Allocation,
    file: File,
    pos: u64,
    size: u64,
}

impl LocalTempFile {
    pub fn new(
        container: Arc<LocalTempContainer>,
        allocation: Allocation,
        file: File,
    ) -> LocalTempFile {
        LocalTempFile {
            _container: container,
            allocation,
            file,
            pos: 0,
            size: 0,
        }
    }
}

impl LocalTempFile {
    fn ensure_allocation_for_size(&mut self, size: u64) -> std::io::Result<()> {
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

impl std::io::Write for LocalTempFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.write_at(self.pos, buf)?;
        self.pos += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

impl ReadAt for LocalTempFile {
    fn size(&self) -> std::io::Result<u64> {
        Ok(self.size)
    }

    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes> {
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

    fn truncate(&mut self, end_pos: u64) -> std::io::Result<()> {
        assert_eq!(self.allocation.amount(), self.current_size());
        if end_pos < self.current_size() {
            self.file.set_len(end_pos)?;
            self.allocation.shrink_to(end_pos);
            self.pos = std::cmp::min(self.pos, end_pos);
            self.size = end_pos;
        }
        Ok(())
    }

    fn into_read_at(mut self: Box<Self>) -> std::io::Result<Arc<dyn ReadAt>> {
        self.shrink_allocation_to_size();
        self.pos = 0;
        Ok(Arc::<LocalTempFile>::from(self) as _)
    }

    fn into_reader(mut self: Box<Self>) -> std::io::Result<Box<dyn std::io::Read>> {
        self.shrink_allocation_to_size();
        self.pos = 0;
        Ok(self)
    }
}

impl TemporaryBuffer for LocalTempFile {
    fn write_at(&mut self, pos: u64, buf: &[u8]) -> std::io::Result<()> {
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

impl std::io::Read for LocalTempFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
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
