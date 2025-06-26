//! In-memory and file-based implementations of the `TemporaryFileStore`.

use std::{path::Path, sync::Arc};

use amudai_io::temp_file_store::{TemporaryFileStore, null_temp_store::NullTempFileStore};

pub mod file_based_cached;
mod file_based_common;
pub mod file_based_uncached;
pub mod memory;
#[cfg(test)]
mod tests;

pub fn create_in_memory(capacity: u64) -> std::io::Result<Arc<dyn TemporaryFileStore>> {
    Ok(Arc::new(memory::InMemoryTempFileStore::new(capacity)))
}

pub fn create_file_based_cached(
    capacity: u64,
    parent_path: Option<&Path>,
) -> std::io::Result<Arc<dyn TemporaryFileStore>> {
    Ok(Arc::new(file_based_cached::LocalTempFileStore::new(
        capacity,
        parent_path,
    )?))
}

pub fn create_file_based_uncached(
    capacity: u64,
    parent_path: Option<&Path>,
) -> std::io::Result<Arc<dyn TemporaryFileStore>> {
    Ok(Arc::new(file_based_uncached::LocalTempFileStore::new(
        capacity,
        parent_path,
    )?))
}

pub fn create_null() -> std::io::Result<Arc<dyn TemporaryFileStore>> {
    Ok(Arc::new(NullTempFileStore))
}
