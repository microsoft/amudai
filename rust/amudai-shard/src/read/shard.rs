use std::sync::Arc;

use amudai_common::Result;
use amudai_format::{
    defs::shard,
    schema::{Schema, SchemaId},
};
use amudai_objectstore::{ObjectStore, ReferenceResolver, url::ObjectUrl};

use super::{shard_context::ShardContext, stripe::Stripe};

pub use super::shard_context::ShardFieldDescriptor;

/// Represents a single data shard.
///
/// A shard is a fundamental unit of data storage in Amudai.  It contains
/// multiple stripes of data, along with metadata like the schema and URL
/// list.
///
/// This struct provides access to the shard's data and metadata.  It uses
/// an `Arc` internally, so cloning it is cheap.
#[derive(Clone)]
pub struct Shard(Arc<ShardContext>);

impl Shard {
    pub(in crate::read) fn from_ctx(ctx: Arc<ShardContext>) -> Shard {
        Shard(ctx)
    }

    /// Returns the shard directory element.
    pub fn directory(&self) -> &shard::ShardDirectory {
        self.0.directory()
    }

    /// Returns the URL of this shard.
    pub fn url(&self) -> &ObjectUrl {
        self.0.url()
    }

    /// Returns the schema for this shard.
    ///
    /// The schema is lazily loaded and cached on the first call to this method.
    pub fn fetch_schema(&self) -> Result<&Schema> {
        self.0.fetch_schema()
    }

    /// Returns the URL list for this shard.
    ///
    /// The URL list is lazily loaded and cached on the first call to this method.
    pub fn fetch_url_list(&self) -> Result<&shard::UrlList> {
        self.0.fetch_url_list()
    }

    /// Returns the number of stripes in this shard.
    pub fn stripe_count(&self) -> usize {
        self.directory().stripe_count as usize
    }

    /// Returns the stripe at the given index.
    ///
    /// # Arguments
    ///
    /// * `index`: The index of the stripe to retrieve.
    pub fn open_stripe(&self, index: usize) -> Result<Stripe> {
        // Ensure the stripe list is loaded.
        self.0.fetch_stripe_list()?;
        Stripe::new(index, self.0.clone())
    }

    /// Returns a shard field descriptor for the specified `schema_id`.
    pub fn fetch_field_descriptor(&self, schema_id: SchemaId) -> Result<Arc<ShardFieldDescriptor>> {
        self.0.fetch_field_descriptor(schema_id)
    }
}

impl std::fmt::Debug for Shard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shard")
            .field("url", &self.url().as_str())
            .field("directory", self.directory())
            .finish_non_exhaustive()
    }
}

/// Options for opening a `Shard`.
pub struct ShardOptions {
    object_store: Arc<dyn ObjectStore>,
    reference_resolver: Option<Arc<dyn ReferenceResolver>>,
}

impl ShardOptions {
    /// The size of the pre-cached suffix used when opening a shard.
    /// A buffer of this size is optimistically loaded from the end of the
    /// shard directory blob. This approach helps to minimize unnecessary
    /// read requests when parsing the basic shard metadata.
    pub const PRECACHED_SUFFIX_SIZE: u64 = 64 * 1024;

    /// Creates a new `ShardOptions` with the given object store.
    ///
    /// # Arguments
    ///
    /// * `object_store`: The object store to use for reading the shard data.
    pub fn new(object_store: Arc<dyn ObjectStore>) -> ShardOptions {
        ShardOptions {
            object_store,
            reference_resolver: None,
        }
    }

    /// Sets the reference resolver.
    pub fn reference_resolver(mut self, reference_resolver: Arc<dyn ReferenceResolver>) -> Self {
        self.reference_resolver = Some(reference_resolver);
        self
    }

    /// Opens a shard from the given URL.
    ///
    /// # Arguments
    ///
    /// * `shard_url`: The URL of the shard to open.  This can be any type
    ///   that can be converted into an `ObjectUrl`, such as `&str`.
    ///
    /// # Errors
    ///
    /// Returns an error if the shard cannot be opened or if the shard
    /// directory is invalid.
    pub fn open(
        self,
        shard_url: impl TryInto<ObjectUrl, Error: Into<amudai_common::error::Error>>,
    ) -> Result<Shard> {
        let shard_url = shard_url.try_into().map_err(|e| e.into())?;
        let ctx = ShardContext::open(
            Arc::new(shard_url),
            self.object_store,
            self.reference_resolver,
        )?;
        Ok(Shard(ctx))
    }
}

#[cfg(test)]
mod tests {
    use amudai_format::schema::SchemaId;
    use amudai_objectstore::url::ObjectUrl;

    use crate::{read::shard::ShardOptions, tests::shard_store::ShardStore};

    #[test]
    fn test_shard_open() {
        let shard_store = ShardStore::new();
        let shard_ref = shard_store.ingest_shard_with_nested_schema(10000);

        let shard = ShardOptions::new(shard_store.object_store.clone())
            .open(ObjectUrl::parse(&shard_ref.url).unwrap())
            .unwrap();
        assert_eq!(shard.directory().total_record_count, 10000);
        let schema = shard.fetch_schema().unwrap();
        assert_eq!(schema.len().unwrap(), 4);
        let url_list = shard.fetch_url_list().unwrap();
        assert_eq!(url_list.urls.len(), 1);

        let field_desc = shard.fetch_field_descriptor(SchemaId::zero()).unwrap();
        assert_eq!(field_desc.position_count, 10000);

        let stripe = shard.open_stripe(0).unwrap();
        assert!(stripe.get_field_descriptor(SchemaId::zero()).is_none());
        stripe
            .fetch_field_descriptor(SchemaId::zero())
            .unwrap()
            .field
            .as_ref()
            .unwrap();
        assert!(stripe.get_field_descriptor(SchemaId::zero()).is_some());

        let schema = shard.fetch_schema().unwrap();
        let field0 = schema.field_at(0).unwrap();
        let field = stripe.open_field(field0.data_type().unwrap()).unwrap();
        assert!(!field.descriptor().encodings.is_empty());
    }
}
