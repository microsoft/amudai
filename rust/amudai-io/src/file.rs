use std::{
    fs::File,
    io::Write,
    ops::Range,
    path::Path,
    sync::{Arc, OnceLock},
};

use amudai_bytes::Bytes;

use crate::{ReadAt, SealingWrite, StorageProfile, verify};

pub struct FileReader {
    file: Arc<File>,
    size: OnceLock<u64>,
}

impl FileReader {
    pub fn new(file: impl Into<Arc<File>>) -> FileReader {
        FileReader {
            file: file.into(),
            size: Default::default(),
        }
    }

    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<FileReader> {
        Ok(FileReader::new(File::open(path)?))
    }
}

impl FileReader {
    fn get_size(&self) -> std::io::Result<u64> {
        if let Some(&size) = self.size.get() {
            Ok(size)
        } else {
            let size = self.file.metadata()?.len();
            let _ = self.size.set(size);
            Ok(size)
        }
    }

    fn adjust_read_range(&self, range: Range<u64>) -> std::io::Result<Range<u64>> {
        let size = self.get_size()?;
        if range.start >= size || range.start == range.end {
            return Ok(0..0);
        }
        let range = range.start..std::cmp::min(range.end, size);
        Ok(range)
    }

    fn read_at_impl(file: &File, range: Range<u64>) -> std::io::Result<Bytes> {
        use amudai_bytes::BytesMut;
        let len = (range.end - range.start) as usize;
        let mut buf = BytesMut::zeroed(len);
        file_read_at_exact(file, range.start, &mut buf)?;
        Ok(buf.into())
    }
}

impl ReadAt for FileReader {
    fn size(&self) -> std::io::Result<u64> {
        self.get_size()
    }

    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes> {
        verify!(range.end >= range.start);
        let range = self.adjust_read_range(range)?;
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        Self::read_at_impl(&self.file, range)
    }

    fn storage_profile(&self) -> StorageProfile {
        StorageProfile {
            min_io_size: 16 * 1024,
            max_io_size: 1024 * 1024,
        }
    }
}

pub struct FileWriter {
    file: Option<File>,
}

impl FileWriter {
    pub fn new(file: File) -> FileWriter {
        FileWriter { file: Some(file) }
    }

    pub fn create<P: AsRef<Path>>(path: P) -> std::io::Result<FileWriter> {
        Ok(FileWriter::new(File::create_new(path)?))
    }
}

impl SealingWrite for FileWriter {
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.file
            .as_mut()
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))?
            .write_all(buf)
    }

    fn seal(&mut self) -> std::io::Result<()> {
        let mut file = self
            .file
            .take()
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))?;
        file.flush()?;
        file.sync_all()?;
        Ok(())
    }

    fn storage_profile(&self) -> StorageProfile {
        Default::default()
    }
}

#[cfg(unix)]
pub fn file_read_at_exact(file: &File, pos: u64, buf: &mut [u8]) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;

    file.read_exact_at(buf, pos)?;
    Ok(())
}

#[cfg(unix)]
pub fn file_write_at(file: &File, pos: u64, buf: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;

    file.write_all_at(buf, pos)
}

#[cfg(windows)]
pub fn file_read_at_exact(file: &File, mut pos: u64, mut buf: &mut [u8]) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;

    while !buf.is_empty() {
        match file.seek_read(buf, pos) {
            Ok(0) => break,
            Ok(n) => {
                buf = &mut buf[n..];
                pos += n as u64;
            }
            Err(e) => return Err(e),
        }
    }
    if !buf.is_empty() {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    Ok(())
}

#[cfg(windows)]
pub fn file_write_at(file: &File, mut pos: u64, mut buf: &[u8]) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;

    while !buf.is_empty() {
        match file.seek_write(buf, pos) {
            Ok(n) => {
                buf = &buf[n..];
                pos += n as u64;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        ReadAt, SealingWrite,
        file::{FileReader, FileWriter},
    };

    #[test]
    fn test_file_reader_and_writer() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let path = tempdir.path().join("test.bin");
        let mut writer = FileWriter::create(&path).expect("create file");
        for _ in 0..10 {
            writer.write_all(b"abcdefgh").expect("write_all");
        }
        writer.seal().expect("seal");

        let reader = FileReader::open(&path).expect("open file");
        for pos in (0..80).step_by(8) {
            let buf = reader.read_at(pos..pos + 4).expect("read_at");
            assert_eq!(buf.as_ref(), b"abcd");
        }
    }
}
