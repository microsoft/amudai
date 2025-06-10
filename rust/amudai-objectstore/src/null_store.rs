//! "null" object store: a no-op implementation of the `ObjectStore` trait.

use std::sync::Arc;

use amudai_io::{ReadAt, SealingWrite, StorageProfile};

use crate::{ObjectStore, url::ObjectUrl};

/// A null implementation of the `ObjectStore` trait.
///
/// This struct provides a no-op implementation of the `ObjectStore` trait,
/// useful for testing or situations where actual object storage is not needed.
///
/// `open` always returns an empty `ReadAt` implementation.
/// `create` always returns a `SealingWrite` implementation that discards all writes.
pub struct NullObjectStore;

impl ObjectStore for NullObjectStore {
    fn open(&self, _url: &ObjectUrl) -> std::io::Result<Arc<dyn ReadAt>> {
        Ok(Arc::new(Vec::<u8>::new()))
    }

    fn create(&self, _url: &ObjectUrl) -> std::io::Result<Box<dyn SealingWrite>> {
        struct NullWriter;

        impl SealingWrite for NullWriter {
            fn write_all(&mut self, _buf: &[u8]) -> std::io::Result<()> {
                Ok(())
            }

            fn seal(&mut self) -> std::io::Result<()> {
                Ok(())
            }

            fn storage_profile(&self) -> StorageProfile {
                Default::default()
            }
        }

        Ok(Box::new(NullWriter))
    }
}
