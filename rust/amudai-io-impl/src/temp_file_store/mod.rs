//! In-memory and file-based implementations of the `TemporaryFileStore`.

use std::{path::Path, sync::Arc};

use amudai_io::temp_file_store::{TemporaryFileStore, null_temp_store::NullTempFileStore};

pub mod fs_cached;
pub mod fs_common;
pub mod fs_uncached;
pub mod memory;
#[cfg(test)]
mod tests;

/// Creates an in-memory temporary file store with the specified capacity.
pub fn create_in_memory(capacity: u64) -> std::io::Result<Arc<dyn TemporaryFileStore>> {
    Ok(Arc::new(memory::InMemoryTempFileStore::new(capacity)))
}

/// Creates a regular file-based temporary file store with the specified capacity.
pub fn create_fs_based(
    capacity: u64,
    parent_path: Option<&Path>,
) -> std::io::Result<Arc<dyn TemporaryFileStore>> {
    Ok(Arc::new(fs_cached::TempFileStore::new(
        capacity,
        parent_path,
    )?))
}

/// Creates an unbuffered file-based temporary file store with the specified capacity.
///
/// This implementation stores temporary files on disk and uses direct (unbuffered) I/O.
///
/// See [`fs_uncached`] for details.
pub fn create_fs_based_unbuffered(
    capacity: u64,
    parent_path: Option<&Path>,
) -> std::io::Result<Arc<dyn TemporaryFileStore>> {
    Ok(Arc::new(fs_uncached::TempFileStore::new(
        capacity,
        parent_path,
    )?))
}

/// Creates a null temporary file store that discards all data.
///
/// This implementation provides a no-op temporary file store that doesn't actually
/// store any data. Useful for some testing scenarios.
pub fn create_null() -> std::io::Result<Arc<dyn TemporaryFileStore>> {
    Ok(Arc::new(NullTempFileStore))
}
