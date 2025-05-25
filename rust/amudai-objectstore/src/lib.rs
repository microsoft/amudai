//! *Object Store* abstraction: a hypothetical "storage service" client
//! capable of issuing `Reader`'s and `Writer`'s for a given object URL.

pub mod default_resolver;
pub mod local_store;
pub mod null_store;
pub mod url;

use std::sync::Arc;

use amudai_io::{ReadAt, SealingWrite};
use url::{ObjectUrl, RelativePath};

/// The `ObjectStore` trait represents a "storage service" abstraction.
/// It provides the ability to obtain readers for existing objects
/// via their URLs, as well as writers for creating new objects.
pub trait ObjectStore: Send + Sync + 'static {
    /// Opens a reader for an existing object specified by the given URL.
    fn open(&self, url: &ObjectUrl) -> std::io::Result<Arc<dyn ReadAt>>;

    /// Creates a writer for a new object at the specified URL.
    fn create(&self, url: &ObjectUrl) -> std::io::Result<Box<dyn SealingWrite>>;
}

/// The `ReferenceResolver` trait defines a policy for validating and resolving
/// sub-artifact storage references (either relative or absolute) within the context
/// of a base URL. This base URL typically points to the location of the "root"
/// shard directory artifact, and the policy determines the valid scope of `DataRef`
/// elements that refer to additional storage artifacts within this shard.
///
/// For example, a data shard located at `https://blobs/data.shard` might reference
/// multiple stripes (internal chunks of a shard) stored in separate files such as
/// `https://blobs/stripe1.stripe`, `https://blobs/stripe2.stripe`, and so forth.
/// When a reader opens the data shard and is about to access its stripes, the
/// `ReferenceResolver` is consulted to "approve" these accesses, ensuring, for instance,
/// that they are within the same container as the shard artifact itself.
///
/// **NOTE**: Implementing this trait correctly is crucial when accessing untrusted
/// data shards, for example, to prevent unintended access to arbitrary resources
/// and locations.
///
/// The default and most commonly used implementation of `ReferenceResolver` is
/// `ContainerScopedReferenceResolver`. This implementation ensures that no references
/// can escape the shard root container, which is the parent "folder" containing the root
/// shard metadata file.
pub trait ReferenceResolver: Send + Sync + 'static {
    /// Validates a given reference against a base `ObjectUrl`.
    ///
    /// This method checks if the provided `reference` (which can be either a relative path
    /// or an absolute URL) is valid in the context of the `base` URL. The definition of a
    /// "valid" reference is implementation-specific, but typically, the referenced artifact
    /// is not allowed to escape the `base` container.
    ///
    /// # Parameters
    ///
    /// * `base`: The base `ObjectUrl` against which the reference is being validated.
    /// * `reference`: The string reference to validate.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the reference is considered valid, or an error of type
    /// `amudai_common::Error` if validation fails.
    fn validate(&self, base: &ObjectUrl, reference: &str) -> amudai_common::Result<()>;

    /// Resolves a given reference against a base `ObjectUrl` into a concrete `ObjectUrl`.
    ///
    /// This method first validates the `reference` using the `validate` method.
    /// If validation is successful, it attempts to resolve the `reference` into a
    /// `ObjectUrl`.
    ///
    /// If the `reference` is a relative path, it is resolved relative to the `base` URL.
    /// Otherwise, the `reference` is parsed as an absolute `ObjectUrl`.
    ///
    /// # Parameters
    ///
    /// * `base`: The base `ObjectUrl` against which the reference is being resolved.
    /// * `reference`: The string reference to resolve.
    ///
    /// # Returns
    ///
    /// Returns `Ok(ObjectUrl)` containing the resolved `ObjectUrl` if successful, or
    /// an error of type `amudai_common::Error` if validation or resolution fails.
    fn resolve(&self, base: &ObjectUrl, reference: &str) -> amudai_common::Result<ObjectUrl> {
        self.validate(base, reference)?;
        if let Ok(rel_path) = RelativePath::new(reference) {
            base.resolve_relative(rel_path)
        } else {
            ObjectUrl::parse(reference)
        }
    }
}
