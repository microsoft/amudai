use std::{fs::OpenOptions, path::Path};

use crate::fs::IoMode;

pub fn open(file_path: &Path, io_mode: IoMode) -> std::io::Result<std::fs::File> {
    let mut options = OpenOptions::new();
    options.read(true);

    match io_mode {
        IoMode::Buffered => (),
        IoMode::Unbuffered => {
            use std::os::windows::fs::OpenOptionsExt;
            options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_NO_BUFFERING);
        }
    }
    options.open(file_path)
}

pub fn create(file_path: &Path, io_mode: IoMode) -> std::io::Result<std::fs::File> {
    let mut options = OpenOptions::new();
    options.create_new(true).read(true).write(true);
    match io_mode {
        IoMode::Buffered => (),
        IoMode::Unbuffered => {
            use std::os::windows::fs::OpenOptionsExt;
            options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_NO_BUFFERING);
        }
    }
    options.open(file_path)
}

pub fn create_temporary_in(
    _folder_path: &Path,
    _io_mode: IoMode,
) -> std::io::Result<std::fs::File> {
    todo!()
}
