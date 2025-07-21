use std::sync::{Arc, OnceLock};

use amudai_common::{Result, error::Error, verify_arg, verify_data};
use amudai_format::{
    defs::{
        AMUDAI_FOOTER_SIZE, CHECKSUM_SIZE, MESSAGE_LEN_SIZE,
        common::{self, DataRef},
        shard,
    },
    schema::{Schema, SchemaId, SchemaMessage},
};
use amudai_io::PrecachedReadAt;
use amudai_objectstore::{ObjectStore, ReferenceResolver, url::ObjectUrl};

use crate::read::properties::PropertyBag;

use super::{
    anchored_element::AnchoredElement, artifact_reader::ArtifactReader, element_memo::Memo,
    opened_artifact_cache::OpenedArtifactCache, shard::ShardOptions,
};

pub struct ShardContext {
    /// The shard directory.
    directory: shard::ShardDirectory,
    /// The URL of the shard.
    url: Arc<ObjectUrl>,
    /// The schema, lazily loaded.
    schema: OnceLock<Schema>,
    /// The URL list, lazily loaded.
    url_list: OnceLock<shard::UrlList>,
    /// Stripe list, lazily loaded.
    stripe_list: OnceLock<ShardStripeList>,
    /// Field refs list, lazily loaded.
    field_refs: OnceLock<ShardFieldRefs>,
    /// Shard properties
    properties: OnceLock<ShardProperties>,
    /// Shard indexes
    indexes: OnceLock<ShardIndexCollection>,
    /// Memo with loaded shard field descriptors by schema id.
    field_descriptors: Memo<SchemaId, Arc<ShardFieldDescriptor>>,
    /// Cache for resolved and opened artifacts.
    opened_artifacts: OpenedArtifactCache,
}

impl ShardContext {
    pub fn open(
        shard_url: Arc<ObjectUrl>,
        object_store: Arc<dyn ObjectStore>,
        reference_resolver: Option<Arc<dyn ReferenceResolver>>,
    ) -> Result<Arc<ShardContext>> {
        let opened_artifacts =
            OpenedArtifactCache::new(shard_url.clone(), object_store, reference_resolver);
        let (reader, size) = open_directory(shard_url.clone(), &opened_artifacts)?;
        let directory = read_directory(&reader, size)?;
        verify_directory(&directory)?;
        Ok(Arc::new(ShardContext {
            directory,
            url: shard_url,
            schema: Default::default(),
            url_list: Default::default(),
            stripe_list: Default::default(),
            field_refs: Default::default(),
            properties: Default::default(),
            indexes: Default::default(),
            field_descriptors: Memo::new(),
            opened_artifacts,
        }))
    }

    /// Opens an artifact referenced by a data reference and returns an `ArtifactReader`.
    ///
    /// This method resolves artifact URLs and provides cached access to shard artifacts. It handles
    /// both absolute URLs and relative references within the shard context, automatically managing
    /// URL resolution, object store access, and caching to optimize repeated access to the same
    /// artifacts.
    ///
    /// # URL Resolution Logic
    ///
    /// The method supports two types of data references:
    ///
    /// * **Absolute references**: When `data_ref` contains a non-empty URL, it's used directly
    ///   to locate the artifact.
    /// * **Relative references**: When `data_ref` is empty, the artifact is resolved relative to:
    ///   - The `anchor` URL if provided (typically the URL of the blob containing the DataRef)
    ///   - The shard directory URL if no anchor is provided
    ///
    /// # Caching Behavior
    ///
    /// This method leverages an internal `OpenedArtifactCache` to avoid redundant operations:
    /// * If an artifact for the resolved URL is already cached, it's returned immediately
    /// * Otherwise, the URL is resolved via the reference resolver, the object is opened via
    ///   the object store, and the resulting `ArtifactReader` is cached for future use
    ///
    /// # Arguments
    ///
    /// * `data_ref`: A data reference that can be converted to a URL-like string slice.
    ///   This is typically the `url` field from a protobuf `DataRef` message, which may be
    ///   empty for self-references or contain an absolute/relative URL to another artifact.
    ///
    /// * `anchor`: An optional artifact URL representing the location where the `data_ref`
    ///   itself is stored. This is used for resolving empty (self-referential) data references.
    ///   For example, when reading field descriptors from a field list, the anchor would be
    ///   the URL of the field list artifact itself.
    ///
    /// # Returns
    ///
    /// An `ArtifactReader` that provides methods to read data from the resolved artifact,
    /// including reading byte ranges, primitive types, and protobuf messages.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * The URL cannot be resolved by the reference resolver
    /// * The object store fails to open the resolved URL
    pub fn open_artifact(
        &self,
        data_ref: impl AsRef<str>,
        anchor: Option<&ObjectUrl>,
    ) -> Result<ArtifactReader> {
        let data_ref_url = data_ref.as_ref();
        let url = if data_ref_url.is_empty() {
            // DataRef points to a range within the same blob where the DataRef is located.
            // This is the anchor (if provided) or the shard directory itself.
            anchor.unwrap_or(self.url.as_ref()).as_str()
        } else {
            data_ref_url
        };
        self.opened_artifacts.get(url)
    }

    pub fn directory(&self) -> &shard::ShardDirectory {
        &self.directory
    }

    pub fn url(&self) -> &Arc<ObjectUrl> {
        &self.url
    }

    pub fn fetch_schema(&self) -> Result<&Schema> {
        if let Some(schema) = self.schema.get() {
            return Ok(schema);
        }
        let schema = self.load_schema()?;
        let _ = self.schema.set(schema);
        Ok(self.schema.get().expect("schema"))
    }

    pub fn fetch_url_list(&self) -> Result<&shard::UrlList> {
        if let Some(url_list) = self.url_list.get() {
            return Ok(url_list);
        }
        let url_list = self.load_url_list()?;
        let _ = self.url_list.set(url_list);
        Ok(self.url_list.get().expect("url_list"))
    }

    pub fn fetch_field_descriptor(&self, schema_id: SchemaId) -> Result<Arc<ShardFieldDescriptor>> {
        if let Some(desc) = self.field_descriptors.get(&schema_id) {
            return Ok(desc);
        }
        let desc = self.load_field_descriptor(schema_id)?;
        let desc = Arc::new(desc);
        self.field_descriptors.put(schema_id, desc.clone());
        Ok(desc)
    }

    pub fn load_field_descriptor(&self, schema_id: SchemaId) -> Result<ShardFieldDescriptor> {
        let field_refs = self.fetch_field_refs()?;
        verify_arg!(schema_id, schema_id.as_usize() < field_refs.len());
        let field_ref = field_refs.get_at(schema_id.as_usize());
        if field_ref.is_empty() {
            return Ok(ShardFieldDescriptor::new(
                Default::default(),
                field_refs.anchor().clone(),
            ));
        }
        let reader = self.open_artifact(&field_ref, Some(field_refs.anchor()))?;
        let descriptor = reader.read_message::<shard::FieldDescriptor>(&field_ref)?;
        let descriptor = ShardFieldDescriptor::new(descriptor, reader.url().clone());
        Ok(descriptor)
    }

    /// Returns a stripe list element, reading it if it wasn't loaded yet.
    pub fn fetch_stripe_list(&self) -> Result<&ShardStripeList> {
        if let Some(stripe_list) = self.stripe_list.get() {
            return Ok(stripe_list);
        }
        let stripe_list = self.load_stripe_list()?;
        let _ = self.stripe_list.set(stripe_list);
        Ok(self.stripe_list.get().expect("stripe_list"))
    }

    /// Returns a stripe list element if it was already loaded
    pub fn get_stripe_list(&self) -> Option<&ShardStripeList> {
        self.stripe_list.get()
    }

    pub fn fetch_field_refs(&self) -> Result<&ShardFieldRefs> {
        if let Some(field_refs) = self.field_refs.get() {
            return Ok(field_refs);
        }
        let field_refs = self.load_field_refs()?;
        let _ = self.field_refs.set(field_refs);
        Ok(self.field_refs.get().expect("field_refs"))
    }

    /// Retrieves the shard properties, loading them if not already cached.
    ///
    /// This method returns a convenient wrapper around the shard properties
    /// that provides typed access to both standard shard metadata (like creation
    /// timestamps) and custom properties.
    ///
    /// # Returns
    ///
    /// A `ShardPropertyBag` providing access to all shard-level properties.
    ///
    /// # Errors
    ///
    /// Returns an error if the properties cannot be loaded from storage.
    pub fn fetch_properties(&self) -> Result<ShardPropertyBag> {
        let props = self.fetch_properties_message()?;
        Ok(ShardPropertyBag {
            inner: props,
            _standard: PropertyBag::new(&props.standard_properties),
            custom: PropertyBag::new(&props.custom_properties),
        })
    }

    /// Retrieves the raw shard properties message, loading it if not already cached.
    ///
    /// This method returns the underlying protobuf message structure containing
    /// the shard properties. For convenient typed access, use `fetch_properties()`
    /// instead.
    ///
    /// # Returns
    ///
    /// A reference to the loaded `ShardProperties` message.
    ///
    /// # Errors
    ///
    /// Returns an error if the properties cannot be loaded from storage.
    pub fn fetch_properties_message(&self) -> Result<&ShardProperties> {
        if let Some(properties) = self.properties.get() {
            return Ok(properties);
        }
        let properties = self.load_properties()?;
        let _ = self.properties.set(properties);
        Ok(self.properties.get().expect("properties"))
    }

    /// Retrieves the shard-level index collection, loading it if not already cached.
    ///
    /// The index collection contains metadata about various indexes available
    /// for this shard, such as bloom filters and other auxiliary data structures
    /// that can accelerate queries.
    ///
    /// # Returns
    ///
    /// A reference to the loaded `ShardIndexCollection`.
    ///
    /// # Errors
    ///
    /// Returns an error if the index collection cannot be loaded from storage.
    pub fn fetch_indexes(&self) -> Result<&ShardIndexCollection> {
        if let Some(indexes) = self.indexes.get() {
            return Ok(indexes);
        }
        let indexes = self.load_index_collection()?;
        let _ = self.indexes.set(indexes);
        Ok(self.indexes.get().expect("indexes"))
    }

    fn load_schema(&self) -> Result<Schema> {
        let schema_ref = self
            .directory
            .schema_ref
            .as_ref()
            .ok_or_else(|| Error::invalid_format("schema ref"))?;
        let reader = self.open_artifact(schema_ref, None)?;
        let schema_bytes = reader.read_at(schema_ref)?;
        SchemaMessage::new(schema_bytes)?.schema()
    }

    fn load_url_list(&self) -> Result<shard::UrlList> {
        let list_ref = self
            .directory
            .url_list_ref
            .as_ref()
            .ok_or_else(|| Error::invalid_format("url list ref"))?;
        let reader = self.open_artifact(list_ref, None)?;
        reader.read_message(list_ref)
    }

    fn load_stripe_list(&self) -> Result<ShardStripeList> {
        let list_ref = self
            .directory
            .stripe_list_ref
            .as_ref()
            .ok_or_else(|| Error::invalid_format("stripe list ref"))?;
        let reader = self.open_artifact(list_ref, None)?;
        let stripe_list = reader.read_message::<shard::StripeList>(list_ref)?;
        verify_data!(
            stripe_list,
            stripe_list.stripes.len() == self.directory.stripe_count as usize
        );
        let stripe_list = ShardStripeList::new(stripe_list, reader.url().clone());
        Ok(stripe_list)
    }

    fn load_field_refs(&self) -> Result<ShardFieldRefs> {
        let list_ref = self
            .directory
            .field_list_ref
            .as_ref()
            .ok_or_else(|| Error::invalid_format("field list ref"))?;
        let reader = self.open_artifact(list_ref, None)?;
        let field_refs = reader.read_message(list_ref)?;
        let field_refs = ShardFieldRefs::new(field_refs, reader.url().clone());
        Ok(field_refs)
    }

    /// Loads the shard properties from storage.
    ///
    /// This internal method reads the properties protobuf message from the
    /// object store using the properties reference from the shard directory.
    /// If no properties reference exists, returns default empty properties.
    ///
    /// # Returns
    ///
    /// The loaded `ShardProperties` anchored to its storage location.
    ///
    /// # Errors
    ///
    /// Returns an error if the properties cannot be loaded from storage.
    fn load_properties(&self) -> Result<ShardProperties> {
        let Some(properties_ref) = self.directory.properties_ref.as_ref() else {
            return Ok(ShardProperties::new(Default::default(), self.url.clone()));
        };
        let reader = self.open_artifact(properties_ref, None)?;
        let properties = reader.read_message(properties_ref)?;
        let properties = ShardProperties::new(properties, reader.url().clone());
        Ok(properties)
    }

    /// Loads the shard index collection from storage.
    ///
    /// This internal method reads the index collection protobuf message from the
    /// object store using the indexes reference from the shard directory.
    /// If no indexes reference exists, returns a default empty index collection.
    ///
    /// # Returns
    ///
    /// The loaded `ShardIndexCollection` anchored to its storage location.
    ///
    /// # Errors
    ///
    /// Returns an error if the index collection cannot be loaded from storage.
    fn load_index_collection(&self) -> Result<ShardIndexCollection> {
        let Some(indexes_ref) = self.directory.indexes_ref.as_ref() else {
            return Ok(ShardIndexCollection::new(
                Default::default(),
                self.url.clone(),
            ));
        };
        let reader = self.open_artifact(indexes_ref, None)?;
        let indexes = reader.read_message(indexes_ref)?;
        let indexes = ShardIndexCollection::new(indexes, reader.url().clone());
        Ok(indexes)
    }
}

pub type ShardStripeList = AnchoredElement<shard::StripeList>;

impl ShardStripeList {
    pub fn len(&self) -> usize {
        self.inner().stripes.len()
    }

    pub fn get_at(&self, index: usize) -> &shard::StripeDirectory {
        &self.inner().stripes[index]
    }
}

pub type ShardFieldRefs = AnchoredElement<common::DataRefArray>;

impl ShardFieldRefs {
    pub fn len(&self) -> usize {
        self.inner().start.len()
    }

    pub fn get_at(&self, index: usize) -> DataRef {
        self.inner().get_at(index)
    }
}

pub type ShardFieldDescriptor = AnchoredElement<shard::FieldDescriptor>;

pub type ShardProperties = AnchoredElement<shard::ShardProperties>;

pub type ShardIndexCollection = AnchoredElement<shard::IndexCollection>;

/// A specialized property bag for shard-level properties.
///
/// `ShardPropertyBag` provides access to both standard and custom properties
/// associated with a shard, along with shard-specific metadata like creation timestamps.
/// It automatically delegates to the custom properties when used directly, while
/// providing dedicated methods for accessing standard shard properties.
///
/// This wrapper ensures type-safe access to shard properties while maintaining
/// compatibility with the general `PropertyBag` interface.
pub struct ShardPropertyBag<'a> {
    inner: &'a ShardProperties,
    /// Standard properties. Note: access to these properties will be through
    /// dedicated methods on the `ShardPropertyBag`.
    _standard: PropertyBag<'a>,
    /// Custom properties. Exposed through `Deref`.
    custom: PropertyBag<'a>,
}

impl<'a> ShardPropertyBag<'a> {
    /// Returns the minimum creation timestamp for the shard in ticks.
    ///
    /// The creation minimum represents the earliest timestamp of any data
    /// contained within this shard. Returns 0 if no minimum creation time
    /// is recorded.
    ///
    /// # Returns
    ///
    /// The minimum creation time as a 64-bit unsigned integer representing
    /// 100-ns ticks since 0001-01-01, or 0 if not available.
    pub fn creation_min(&self) -> u64 {
        self.inner.creation_min.map(|c| c.ticks).unwrap_or_default()
    }

    /// Returns the maximum creation timestamp for the shard in ticks.
    ///
    /// The creation maximum represents the latest timestamp of any data
    /// contained within this shard. Returns 0 if no maximum creation time
    /// is recorded.
    ///
    /// # Returns
    ///
    /// The maximum creation time as a 64-bit unsigned integer representing
    /// 100-ns ticks since 0001-01-01, or 0 if not available.
    pub fn creation_max(&self) -> u64 {
        self.inner.creation_max.map(|c| c.ticks).unwrap_or_default()
    }
}

impl<'a> std::ops::Deref for ShardPropertyBag<'a> {
    type Target = PropertyBag<'a>;

    fn deref(&self) -> &Self::Target {
        &self.custom
    }
}

/// Verifies that the shard directory has all required fields.
fn verify_directory(directory: &shard::ShardDirectory) -> Result<()> {
    verify_data!(schema_ref, directory.schema_ref.is_some());
    verify_data!(field_list_ref, directory.field_list_ref.is_some());
    verify_data!(stripe_list_ref, directory.stripe_list_ref.is_some());
    verify_data!(url_list_ref, directory.url_list_ref.is_some());
    Ok(())
}

/// Opens the shard directory and returns an `ArtifactReader` and the size
/// of the shard directory blob.
fn open_directory(
    shard_url: Arc<ObjectUrl>,
    opened_artifacts: &OpenedArtifactCache,
) -> Result<(ArtifactReader, u64)> {
    let reader = opened_artifacts.object_store().open(&shard_url)?;
    let reader = PrecachedReadAt::from_suffix(reader, ShardOptions::PRECACHED_SUFFIX_SIZE)?;
    let size = reader.object_size();
    verify_data!(
        "shard directory minimal size",
        size >= amudai_format::defs::SHARD_DIRECTORY_MIN_SIZE as u64
    );
    let reader = ArtifactReader::new(Arc::new(reader), shard_url.clone());
    opened_artifacts.put(shard_url.as_str(), reader.clone());
    Ok((reader, size))
}

/// Reads the shard directory from the given reader. `size` is the size of the
/// shard directory blob.
fn read_directory(reader: &ArtifactReader, size: u64) -> Result<shard::ShardDirectory> {
    reader.verify_footer()?;
    // Directory size is `u32` that immediately precedes the 8-byte footer.
    let directory_size =
        reader.read_u32(size - AMUDAI_FOOTER_SIZE as u64 - MESSAGE_LEN_SIZE as u64)? as u64;
    let directory_backoff = MESSAGE_LEN_SIZE as u64
        + directory_size
        + CHECKSUM_SIZE as u64
        + MESSAGE_LEN_SIZE as u64
        + AMUDAI_FOOTER_SIZE as u64;
    verify_data!("directory start", directory_backoff < size);
    let msg_start = size - directory_backoff;
    let msg_end = msg_start + MESSAGE_LEN_SIZE as u64 + directory_size + CHECKSUM_SIZE as u64;
    let directory = reader.read_message::<shard::ShardDirectory>(msg_start..msg_end)?;
    Ok(directory)
}
