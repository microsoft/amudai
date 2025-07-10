use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use amudai_common::error::Error;
use amudai_io::{
    ReadAt, SealingWrite,
    utils::file::{FileReader, FileWriter},
};
use url::Url;

use crate::{ObjectStore, url::ObjectUrl};

/// A `LocalFsObjectStore` implementation that manages objects on the local filesystem,
/// confined to a specified local path.
///
/// The semantics of the URL used to access the artifacts depend on the
/// local filesystem mode.
///
/// In `Passthrough` mode, the implementation does not create a virtualized root
/// for its filesystem. All object URLs used with `LocalFsObjectStore` methods
/// must be absolute file URLs on the host filesystem, typically in the format
/// `file:///path/object`. However, the implementation ensures that access to files
/// outside the specified container is restricted.
///
/// In `VirtualRoot` mode, all URLs are treated as relative to this root.
///
/// Note: when calling `open()` or `create()` functions, the canonical form of the
/// local filesystem URL is e.g. `"file:///C:/docs/test.txt"` (on Windows) or
/// `"file:///home/user/test.txt"` (on Linux).
pub struct LocalFsObjectStore {
    /// The top-level directory for this object store.
    container_path: PathBuf,
    /// The URL representing the top-level directory of this object store.
    container_url: ObjectUrl,
    /// Specifies whether the object path is virtualized or treated as an absolute path
    /// on the host filesystem.
    mode: LocalFsMode,
}

impl LocalFsObjectStore {
    /// Creates a new `LocalFsObjectStore` with the given container directory.
    ///
    /// The container directory will be created if it does not exist.
    ///
    /// # Arguments
    ///
    /// * `container_path`: The path to the directory that will serve as the root
    ///   of the object store.
    pub fn new(
        container_path: &Path,
        mode: LocalFsMode,
    ) -> amudai_common::Result<LocalFsObjectStore> {
        let url = Url::from_directory_path(container_path).map_err(|()| {
            Error::invalid_arg(
                "container",
                format!("invalid path {container_path:?} for local object store"),
            )
        })?;
        let container_url = ObjectUrl::new(url)?;
        let _ = std::fs::create_dir_all(container_path);
        Ok(LocalFsObjectStore {
            container_path: container_path.to_path_buf(),
            container_url,
            mode,
        })
    }

    /// Creates a new unscoped `LocalFsObjectStore` instance.
    ///
    /// This method initializes a `LocalFsObjectStore` without a specific
    /// container path, effectively creating an object store that is not confined
    /// to any particular directory. The `container_path` is set to an empty
    /// `PathBuf`, and the `container_url` is set to the root of the local
    /// filesystem (`file:///`). The mode is set to `LocalFsMode::Passthrough`.
    ///
    /// This is useful for scenarios where the object store needs to operate
    /// without a predefined root directory, allowing access to any valid file
    /// path within the filesystem.
    ///
    /// # Returns
    ///
    /// A `LocalFsObjectStore` instance with no scoped container path.
    pub fn new_unscoped() -> LocalFsObjectStore {
        LocalFsObjectStore {
            container_path: PathBuf::from("/"),
            container_url: ObjectUrl::parse("file:///").expect("parse unscoped"),
            mode: LocalFsMode::Passthrough,
        }
    }

    /// Returns the file system path of the store's top-level container.
    pub fn container_path(&self) -> &Path {
        &self.container_path
    }

    /// Returns the URL of the store's top-level container.
    pub fn container_url(&self) -> &ObjectUrl {
        &self.container_url
    }

    /// Converts an [`ObjectUrl`] to a local filesystem path.
    ///
    /// The provided `url` must be within the container URL of this object store.
    ///
    /// # Arguments
    ///
    /// * `url`: The URL of the object.
    pub fn url_to_path(&self, url: &ObjectUrl) -> amudai_common::Result<PathBuf> {
        if self.mode == LocalFsMode::Passthrough {
            let relative_path = self.container_url.make_relative(url).ok_or_else(|| {
                Error::invalid_arg(
                    "url",
                    format!(
                        "object url '{}' cannot be made relative to the local fs container",
                        url.as_str()
                    ),
                )
            })?;
            let path = self.container_path.join(relative_path);
            Ok(path)
        } else {
            let relative_path = url.path().trim_start_matches('/');
            let path = self.container_path.join(relative_path);
            Ok(path)
        }
    }
}

impl ObjectStore for LocalFsObjectStore {
    /// Opens an object for reading.
    ///
    /// # Arguments
    ///
    /// * `url`: The URL of the object to open.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL is invalid or if the file cannot be opened.
    fn open(&self, url: &ObjectUrl) -> std::io::Result<Arc<dyn ReadAt>> {
        let path = self.url_to_path(url).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("open: invalid url: {e}"),
            )
        })?;

        let file = File::open(path)?;
        Ok(Arc::new(FileReader::new(file)))
    }

    /// Creates a new object for writing.
    ///
    /// # Arguments
    ///
    /// * `url`: The URL of the object to create.
    fn create(&self, url: &ObjectUrl) -> std::io::Result<Box<dyn SealingWrite>> {
        let path = self.url_to_path(url).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("create: invalid url: {e}"),
            )
        })?;

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;

        Ok(Box::new(FileWriter::new(file)))
    }
}

/// Local Filesystem Mode: Defines how the local filesystem `ObjectStore` implementation
/// interprets the `ObjectUrl`s of the artifacts being accessed. There are two available
/// modes:
///
/// 1. **Passthrough**: The URLs are treated as physical paths within the host's namespace.
///    The local filesystem ensures these paths are confined within the top-level container.
///
/// 2. **VirtualRoot**: The URLs are interpreted as being rooted in the filesystem's
///    top-level container, effectively making this container a new "virtual root".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalFsMode {
    /// In this mode, the implementation operates with full file paths and ensures they reside
    /// within the container folder.
    Passthrough,
    /// In this mode, the filesystem container acts as a virtual "root", and all paths
    /// are treated as relative to this root.
    VirtualRoot,
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use url::Url;

    use crate::{
        ObjectStore,
        url::{ObjectUrl, RelativePath},
    };

    use super::{LocalFsMode, LocalFsObjectStore};

    fn create_temp_fs(mode: LocalFsMode) -> (LocalFsObjectStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let fs = LocalFsObjectStore::new(dir.path(), mode).unwrap();
        (fs, dir)
    }

    #[test]
    fn test_local_fs_creation() {
        let (fs, dir) = create_temp_fs(LocalFsMode::Passthrough);
        let test_url = fs
            .container_url()
            .resolve_relative(RelativePath::new("aaa/bbb/test.txt").unwrap())
            .unwrap();
        let mut writer = fs.create(&test_url).unwrap();
        writer.write_all(b"abcdefg").unwrap();
        writer.seal().unwrap();

        let test_path = fs.url_to_path(&test_url).unwrap();
        assert!(test_path.is_file());

        let reader = fs.open(&test_url).unwrap();
        let buf = reader.read_at(0..7).unwrap();
        assert_eq!(buf.as_ref(), b"abcdefg");
        drop(dir);
    }

    #[test]
    fn test_local_fs_passthrough_mode() {
        let (fs, dir) = create_temp_fs(LocalFsMode::Passthrough);
        let (_fs2, dir2) = create_temp_fs(LocalFsMode::Passthrough);

        let container_path = dir.path();

        let file_path = container_path.join("test_file.txt");
        let file_url = ObjectUrl::new(Url::from_file_path(&file_path).unwrap()).unwrap();
        let mut writer = fs.create(&file_url).unwrap();
        writer.write_all(b"hello").unwrap();
        writer.seal().unwrap();
        assert!(file_path.exists());

        let reader = fs.open(&file_url).unwrap();
        let buf = reader.read_at(0..5).unwrap();
        assert_eq!(buf.as_ref(), b"hello");

        let subfolder_path = container_path.join("subfolder");
        let subfile_path = subfolder_path.join("subfile.txt");
        let subfile_url = ObjectUrl::new(Url::from_file_path(&subfile_path).unwrap()).unwrap();
        let mut writer = fs.create(&subfile_url).unwrap();
        writer.write_all(b"world").unwrap();
        writer.seal().unwrap();
        assert!(subfile_path.exists());

        let reader = fs.open(&subfile_url).unwrap();
        let buf = reader.read_at(0..5).unwrap();
        assert_eq!(buf.as_ref(), b"world");

        let outside_path = dir2.path().join("tmp/outside.txt");
        let outside_url = ObjectUrl::new(Url::from_file_path(outside_path).unwrap()).unwrap();
        let create_result = fs.create(&outside_url);
        assert!(create_result.is_err());

        let open_result = fs.open(&outside_url);
        assert!(open_result.is_err());

        let path = fs.url_to_path(&file_url).unwrap();
        assert_eq!(path, file_path);

        let invalid_url = ObjectUrl::new(Url::parse("file:///tmp/invalid.txt").unwrap()).unwrap();
        let path_result = fs.url_to_path(&invalid_url);
        assert!(path_result.is_err());
    }

    #[test]
    fn test_local_fs_virtual_root_mode() {
        let (fs, dir) = create_temp_fs(LocalFsMode::VirtualRoot);
        let container_path = dir.path();

        // Test creating a file within the virtual root
        let file_url = ObjectUrl::new(Url::parse("file:///test_file.txt").unwrap()).unwrap();
        let file_path = container_path.join("test_file.txt");
        let mut writer = fs.create(&file_url).unwrap();
        writer.write_all(b"hello").unwrap();
        writer.seal().unwrap();
        assert!(file_path.exists());

        // Test reading the file
        let reader = fs.open(&file_url).unwrap();
        let buf = reader.read_at(0..5).unwrap();
        assert_eq!(buf.as_ref(), b"hello");

        // Test creating a file in a subfolder
        let subfile_url =
            ObjectUrl::new(Url::parse("file:///subfolder/subfile.txt").unwrap()).unwrap();
        let subfile_path = container_path.join("subfolder").join("subfile.txt");
        let mut writer = fs.create(&subfile_url).unwrap();
        writer.write_all(b"world").unwrap();
        writer.seal().unwrap();
        assert!(subfile_path.exists());

        // Test reading the subfile
        let reader = fs.open(&subfile_url).unwrap();
        let buf = reader.read_at(0..5).unwrap();
        assert_eq!(buf.as_ref(), b"world");

        // Test url_to_path with a valid url
        let path = fs.url_to_path(&file_url).unwrap();
        assert_eq!(path, file_path);

        // Test creating a file with a leading slash
        let leading_slash_url =
            ObjectUrl::new(Url::parse("file:////leading_slash.txt").unwrap()).unwrap();
        let leading_slash_path = container_path.join("leading_slash.txt");
        let mut writer = fs.create(&leading_slash_url).unwrap();
        writer.write_all(b"leading").unwrap();
        writer.seal().unwrap();
        assert!(leading_slash_path.exists());
    }

    #[test]
    fn test_local_fs_create_existing_file() {
        let (fs, dir) = create_temp_fs(LocalFsMode::Passthrough);
        let container_path = dir.path();

        // Create a file first
        let file_path = container_path.join("existing_file.txt");
        let file_url = ObjectUrl::new(Url::from_file_path(&file_path).unwrap()).unwrap();
        std::fs::write(&file_path, b"initial content").unwrap();

        // Attempt to create the same file again, should fail
        let create_result = fs.create(&file_url);
        assert!(create_result.is_err());
    }

    #[test]
    fn test_local_fs_open_nonexistent_file() {
        let (fs, dir) = create_temp_fs(LocalFsMode::Passthrough);
        let container_path = dir.path();

        let file_path = container_path.join("nonexistent_file.txt");
        let file_url = ObjectUrl::new(Url::from_file_path(&file_path).unwrap()).unwrap();
        let open_result = fs.open(&file_url);
        assert!(open_result.is_err());
    }

    #[test]
    fn test_unscoped_local_fs() {
        let dir = TempDir::new().unwrap();
        let test_path = dir.path().join("test.txt");
        std::fs::write(&test_path, b"1234567").unwrap();

        let read_url = Url::from_file_path(&test_path).unwrap().to_string();
        println!("{read_url}");

        let fs = LocalFsObjectStore::new_unscoped();

        let reader = fs.open(&ObjectUrl::parse(&read_url).unwrap()).unwrap();
        let bytes = reader.read_at(0..7).unwrap();
        assert_eq!(bytes.as_ref(), b"1234567");

        let write_path = dir.path().join("test1.txt");
        let write_url = Url::from_file_path(&write_path).unwrap().to_string();
        let mut writer = fs.create(&ObjectUrl::parse(&write_url).unwrap()).unwrap();
        writer.write_all(b"123456789").unwrap();
        writer.seal().unwrap();

        let bytes = std::fs::read(&write_path).unwrap();
        assert_eq!(bytes, b"123456789");
    }

    #[test]
    fn test_local_fs_empty_path() {
        let (fs, _dir) = create_temp_fs(LocalFsMode::VirtualRoot);

        let empty_path_url = ObjectUrl::new(Url::parse("file:///").unwrap()).unwrap();
        dbg!(&empty_path_url);
        let create_result = fs.create(&empty_path_url);
        assert!(create_result.is_err());

        let empty_path_url = ObjectUrl::new(Url::parse("file://").unwrap()).unwrap();
        let create_result = fs.create(&empty_path_url);
        assert!(create_result.is_err());
    }
}
