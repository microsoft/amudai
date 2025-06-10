use std::sync::{Arc, OnceLock};

use amudai_common::{Result, verify_arg, verify_data};
use amudai_format::{
    defs::{
        AMUDAI_FOOTER_SIZE, CHECKSUM_SIZE, MESSAGE_LEN_SIZE,
        common::{self, DataRef},
        shard,
    },
    schema::{Schema, SchemaId, SchemaMessage},
};
use amudai_io::precached_read::PrecachedReadAt;
use amudai_objectstore::{ObjectStore, ReferenceResolver, url::ObjectUrl};

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
            field_descriptors: Memo::new(),
            opened_artifacts,
        }))
    }

    /// Returns an `ArtifactReader` for the provided `DataRef`.
    ///
    /// # Arguments
    ///
    /// * `data_ref`: A `DataRef`
    ///
    /// * `anchor`: An optional artifact URL where the `data_ref` itself is located.
    ///   When the `data_ref` has an empty URL, it refers to a range within that "anchor"
    ///   artifact.
    pub fn open_data_ref(
        &self,
        data_ref: &DataRef,
        anchor: Option<&ObjectUrl>,
    ) -> Result<ArtifactReader> {
        let url = if data_ref.url.is_empty() {
            // DataRef points to a range within the same blob where the DataRef is located.
            // This is the anchor (if provided) or the shard directory itself.
            anchor.unwrap_or(self.url.as_ref()).as_str()
        } else {
            data_ref.url.as_str()
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
        let reader = self.open_data_ref(&field_ref, Some(field_refs.anchor()))?;
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

    fn load_schema(&self) -> Result<Schema> {
        let schema_ref = self.directory.schema_ref.as_ref().expect("schema_ref");
        let reader = self.open_data_ref(schema_ref, None)?;
        let schema_bytes = reader.read_at(schema_ref)?;
        SchemaMessage::new(schema_bytes)?.schema()
    }

    fn load_url_list(&self) -> Result<shard::UrlList> {
        let list_ref = self.directory.url_list_ref.as_ref().expect("url_list_ref");
        let reader = self.open_data_ref(list_ref, None)?;
        reader.read_message(list_ref)
    }

    fn load_stripe_list(&self) -> Result<ShardStripeList> {
        let list_ref = self
            .directory
            .stripe_list_ref
            .as_ref()
            .expect("stripe_list_ref");
        let reader = self.open_data_ref(list_ref, None)?;
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
            .expect("field_list_ref");
        let reader = self.open_data_ref(list_ref, None)?;
        let field_refs = reader.read_message(list_ref)?;
        let field_refs = ShardFieldRefs::new(field_refs, reader.url().clone());
        Ok(field_refs)
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
