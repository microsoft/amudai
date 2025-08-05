use std::{ops::Range, sync::Arc};

use amudai_common::Result;
use amudai_format::{
    defs::{common::DataRef, shard},
    schema::{Schema, SchemaId},
};
use amudai_index_core::shard_markers;
use amudai_objectstore::{ObjectStore, ReferenceResolver, url::ObjectUrl};

use super::{shard_context::ShardContext, stripe::Stripe};

pub use super::shard_context::{
    ShardFieldDescriptor, ShardIndexCollection, ShardProperties, ShardPropertyBag,
};

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

    /// Returns the object store used by this shard.
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        self.0.object_store()
    }

    /// Returns the reference resolver used by this shard when accessing its data refs.
    pub fn reference_resolver(&self) -> &Arc<dyn ReferenceResolver> {
        self.0.reference_resolver()
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

    /// Returns the stripe list for this shard.
    ///
    /// The stripe list is lazily loaded and cached on the first call to this method.
    pub fn fetch_stripe_list(&self) -> Result<&shard::StripeList> {
        Ok(self.0.fetch_stripe_list()?.inner())
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

    /// Retrieves the shard properties with convenient typed access.
    ///
    /// This method returns a `ShardPropertyBag` that provides convenient access
    /// to shard-level properties, including creation timestamps and custom metadata.
    /// The properties are loaded from storage if not already cached.
    ///
    /// # Returns
    ///
    /// A `ShardPropertyBag` providing typed access to shard properties.
    ///
    /// # Errors
    ///
    /// Returns an error if the properties cannot be loaded from storage.
    pub fn fetch_properties(&self) -> Result<ShardPropertyBag> {
        self.0.fetch_properties()
    }

    /// Retrieves the shard-level index descriptor collection.
    ///
    /// This method returns metadata about indexes available at the shard level,
    /// such as inverted term indexes and other data structures that can accelerate
    /// queries across the entire shard. The index descriptors are loaded from storage
    /// if not already cached.
    ///
    /// # Returns
    ///
    /// A reference to the loaded `ShardIndexCollection`.
    ///
    /// # Errors
    ///
    /// Returns an error if the index descriptors cannot be loaded from storage.
    pub fn fetch_indexes(&self) -> Result<&ShardIndexCollection> {
        self.0.fetch_indexes()
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

impl shard_markers::DynShard for Shard {
    fn type_id(&self) -> std::any::TypeId {
        std::any::TypeId::of::<Self>()
    }

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send + Sync + 'static> {
        self
    }
}

impl TryFrom<Box<dyn shard_markers::DynShard>> for Shard {
    type Error = Box<dyn shard_markers::DynShard>;

    fn try_from(
        value: Box<dyn shard_markers::DynShard>,
    ) -> std::result::Result<Self, Box<dyn shard_markers::DynShard>> {
        if value.type_id() != std::any::TypeId::of::<Shard>() {
            return Err(value);
        }
        Ok(*value
            .into_any()
            .downcast::<Shard>()
            .expect("downcast Shard"))
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
    /// This method opens a shard by reading its directory from the specified URL.
    /// The shard directory is assumed to follow the standard Amudai format: it is
    /// either located at the very end of the blob, or it is the only element in
    /// the blob.
    ///
    /// # Shard Directory Format
    ///
    /// The expected format is:
    /// ```text
    /// amudai_header (8 bytes)
    /// ...(optional data)...
    /// directory_length: u32
    /// directory_protobuf_message
    /// directory_checksum: u32
    /// directory_length: u32
    /// amudai_footer (8 bytes)
    /// ```
    ///
    /// # Arguments
    ///
    /// * `shard_url`: The URL of the shard to open. This can be any type that can be
    ///   converted into an `ObjectUrl`, such as `&str`.
    ///
    /// # Returns
    ///
    /// A `Shard` instance that provides access to the shard's data and metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * The URL cannot be converted to an `ObjectUrl`
    /// * The shard cannot be accessed at the given URL
    /// * The shard directory format is invalid or corrupted
    /// * Required shard metadata is missing or malformed
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

    /// Opens a shard from a shard directory `DataRef`.
    ///
    /// This is a convenience method that extracts the URL and byte range from a `DataRef`
    /// and opens the shard directory at that specific location within the blob. This is
    /// useful when you have a reference to a shard directory embedded within a larger
    /// data blob.
    ///
    /// # Arguments
    ///
    /// * `data_ref`: A reference to the shard directory data, containing both the URL
    ///   of the blob and the byte range where the shard directory is located.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * The URL in the `DataRef` cannot be converted to an `ObjectUrl`
    /// * The shard cannot be opened at the specified location
    /// * The shard directory at the specified range is invalid
    pub fn open_data_ref(self, data_ref: &DataRef) -> Result<Shard> {
        self.open_at(data_ref.url.as_str(), Range::from(data_ref))
    }

    /// Opens a shard from a URL with the shard directory located at a specific byte range.
    ///
    /// This method allows opening a shard when the shard directory is embedded within
    /// a larger blob at a known byte offset and size, rather than being the last element
    /// in the blob. This is useful for scenarios where multiple shard directories or other
    /// data structures are packed together in a single file.
    ///
    /// # Arguments
    ///
    /// * `shard_url`: The URL of the blob containing the shard directory. This can be
    ///   any type that can be converted into an `ObjectUrl`, such as `&str`.
    /// * `directory_range`: The byte range within the blob where the shard directory
    ///   is located. The range uses standard Rust semantics (start inclusive, end exclusive).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * The URL cannot be converted to an `ObjectUrl`
    /// * The blob cannot be accessed at the given URL
    /// * The specified byte range is invalid or extends beyond the blob
    /// * The shard directory data at the specified range is corrupted or invalid
    pub fn open_at(
        self,
        shard_url: impl TryInto<ObjectUrl, Error: Into<amudai_common::error::Error>>,
        directory_range: Range<u64>,
    ) -> Result<Shard> {
        let shard_url = shard_url.try_into().map_err(|e| e.into())?;
        let ctx = ShardContext::open_at(
            Arc::new(shard_url),
            directory_range,
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
