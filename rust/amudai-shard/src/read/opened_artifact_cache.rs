//! Resolver and cache for opened shard artifact references.

use std::sync::{Arc, RwLock};

use ahash::AHashMap;
use amudai_common::Result;
use amudai_objectstore::{ObjectStore, ReferenceResolver, url::ObjectUrl};

use super::artifact_reader::ArtifactReader;

/// Resolver and cache for opened shard artifact references.
///
/// The `OpenedArtifactCache` is designed to optimize access to shard artifacts
/// by maintaining a cache of already resolved and opened references. This avoids
/// redundant and potentially costly operations like reference resolution and
/// object opening.
///
/// **Note**: This is not a data cache, but a cache of resolved and opened object handles.
///
/// This structure is particularly useful in scenarios where the same shard artifacts
/// are accessed repeatedly, such as during query execution or data processing within
/// a shard.
pub struct OpenedArtifactCache {
    /// The underlying map.
    cache: RwLock<AHashMap<String, ArtifactReader>>,
    /// The base URL for resolving relative artifact URLs.
    ///
    /// This URL typically represents the "root" URL of the shard directory or container.
    /// When a relative URL is provided to the `get` method, it will be resolved against
    /// this `base_url` using the configured `resolver`.
    base_url: Arc<ObjectUrl>,
    /// The object store used to open artifacts.
    object_store: Arc<dyn ObjectStore>,
    /// The reference resolver used to resolve artifact URLs.
    ///
    /// The resolver is responsible for taking a potentially relative artifact URL and
    /// resolving it into an absolute URL based on the `base_url`. The resolver handles
    /// the specific logic for URL resolution within the shard context.
    reference_resolver: Arc<dyn ReferenceResolver>,
}

impl OpenedArtifactCache {
    /// Creates a new `OpenedArtifactCache`.
    ///
    /// # Arguments
    ///
    /// * `base_url`: The base URL for resolving relative artifact URLs. This should be the
    ///   "root" URL of the shard directory or container.
    /// * `object_store`: The `ObjectStore` to be used for opening artifact objects.
    /// * `resolver`: The `ReferenceResolver` to be used for resolving artifact URLs.
    pub fn new(
        base_url: Arc<ObjectUrl>,
        object_store: Arc<dyn ObjectStore>,
        reference_resolver: Option<Arc<dyn ReferenceResolver>>,
    ) -> OpenedArtifactCache {
        OpenedArtifactCache {
            cache: Default::default(),
            base_url,
            object_store,
            reference_resolver: reference_resolver
                .unwrap_or_else(|| amudai_objectstore::default_resolver::get()),
        }
    }

    /// Returns the object store implementation.
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
    }

    /// Retrieves an artifact from the cache, or opens and caches it if not already present.
    ///
    /// # Arguments
    ///
    /// * `url`: The URL of the artifact to retrieve. This can be an absolute URL or a relative
    ///   URL that will be resolved against the `base_url`.
    pub fn get(&self, url: &str) -> Result<ArtifactReader> {
        if let Some(cached) = self.try_get_cached(url) {
            return Ok(cached);
        }
        let resolved = self.reference_resolver.resolve(&self.base_url, url)?;
        let object = self.object_store.open(&resolved)?;
        let reader = ArtifactReader::new(object, Arc::new(resolved));
        self.put_into_cache(url, reader.clone());
        Ok(reader)
    }

    /// Inserts an artifact into the cache, potentially replacing an existing entry.
    ///
    /// This method is typically unnecessary for regular use, as the `get()` method
    /// automatically handles caching of artifacts. However, it can be used to manually
    /// cache an artifact if needed.
    pub fn put(&self, url: &str, reader: ArtifactReader) {
        self.put_into_cache(url, reader);
    }

    /// Attempts to retrieve an artifact from the cache without performing any resolution or opening.
    fn try_get_cached(&self, url: &str) -> Option<ArtifactReader> {
        self.cache.read().expect("read lock").get(url).cloned()
    }

    /// Puts an artifact into the cache.
    fn put_into_cache(&self, url: &str, reader: ArtifactReader) {
        self.cache
            .write()
            .expect("write lock")
            .insert(url.to_string(), reader);
    }
}
