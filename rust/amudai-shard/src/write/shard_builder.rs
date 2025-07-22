//! This module provides structures and functions for building and managing data shards
//! and stripes.
//!
//! The core idea is to build data in a structured way: first, by defining a schema; then,
//! creating stripes of data; and finally, combining stripes into a shard.

use std::{borrow::Cow, sync::Arc};

use ahash::AHashSet;
use amudai_common::{Result, error::Error};
use amudai_encodings::block_encoder::BlockEncodingProfile;
use amudai_format::{
    defs::{
        CHECKSUM_SIZE, MESSAGE_LEN_SIZE,
        common::{DataRef, DataRefArray},
        shard::{self, ShardProperties},
    },
    schema::{DataType, Schema, SchemaId, SchemaMessage},
};
use amudai_io::temp_file_store::TemporaryFileStore;
use amudai_objectstore::{
    ObjectStore,
    url::{ObjectUrl, RelativePath},
};

use crate::write::properties::ShardPropertiesBuilder;

use super::{
    artifact_writer::ArtifactWriter,
    field_descriptor,
    format_elements_ext::{CompactDataRefs, DataRefExt},
    stripe_builder::{PreparedStripe, SealedStripe, StripeBuilder, StripeBuilderParams},
};

/// A builder for creating data shards.
pub struct ShardBuilder {
    params: ShardBuilderParams,
    schema: Schema,
    stripes: Vec<PreparedStripe>,
    properties: ShardPropertiesBuilder,
}

impl ShardBuilder {
    /// Creates a new `ShardBuilder` with the given parameters.
    pub fn new(params: ShardBuilderParams) -> Result<ShardBuilder> {
        let schema = params.schema.schema()?;
        Ok(ShardBuilder {
            params,
            schema,
            stripes: Vec::new(),
            properties: Default::default(),
        })
    }

    /// Returns a reference to the schema associated with this shard.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns the parameters and configuration for this `ShardBuilder`.
    pub fn params(&self) -> &ShardBuilderParams {
        &self.params
    }

    /// Creates a new `StripeBuilder` for constructing stripes associated with this shard.
    ///
    /// The returned `StripeBuilder` is not bound to the shard and can be discarded without
    /// affecting it.
    /// Multiple `StripeBuilder` instances can be created and populated concurrently, allowing
    /// for parallel stripe construction.
    pub fn build_stripe(&self) -> Result<StripeBuilder> {
        let params = StripeBuilderParams {
            schema: self.schema.clone(),
            temp_store: self.params.temp_store.clone(),
            encoding_profile: self.params.encoding_profile,
        };
        StripeBuilder::new(params)
    }

    /// Adds a prepared stripe to the shard.
    pub fn add_stripe(&mut self, stripe: PreparedStripe) -> Result<()> {
        self.stripes.push(stripe);
        Ok(())
    }

    /// Returns a reference to the shard properties builder.
    ///
    /// This method provides read-only access to the `ShardPropertiesBuilder`
    /// associated with this shard, allowing inspection of current property values
    /// and creation timestamps without the ability to modify them.
    ///
    /// # Returns
    ///
    /// A reference to the shard's `ShardPropertiesBuilder`.
    pub fn properties(&self) -> &ShardPropertiesBuilder {
        &self.properties
    }

    /// Returns a mutable reference to the shard properties builder.
    ///
    /// This method provides mutable access to the `ShardPropertiesBuilder`
    /// associated with this shard, allowing properties to be set or modified,
    /// including creation timestamps and custom metadata.
    ///
    /// # Returns
    ///
    /// A mutable reference to the shard's `ShardPropertiesBuilder`.
    pub fn properties_mut(&mut self) -> &mut ShardPropertiesBuilder {
        &mut self.properties
    }

    /// Creates a field decoder from the prepared stripe field with the given `schema_id`.
    ///
    /// This method constructs a [`FieldDecoder`](crate::read::field_decoder::FieldDecoder)
    /// that can read back the data that was encoded in the prepared stripe field.
    ///
    /// # Parameters
    ///
    /// - `stripe_index`: Index of the stripe
    /// - `data_type`: Field data type node
    pub fn decode_stripe_field(
        &self,
        stripe_index: usize,
        data_type: &DataType,
    ) -> Result<crate::read::field_decoder::FieldDecoder> {
        self.stripes
            .get(stripe_index)
            .ok_or_else(|| Error::invalid_arg("stripe_index", ""))?
            .decode_field(data_type.schema_id()?)
    }

    /// Finishes building the shard and returns a `PreparedShard`.
    pub fn finish(self) -> Result<PreparedShard> {
        Ok(PreparedShard {
            params: self.params,
            stripes: self.stripes,
            properties: self.properties,
        })
    }
}

/// File-level structure of the data shard: how each component of the shard format -
/// such as metadata messages, data buffers, and others — should be stored in files.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShardFileOrganization {
    /// The top level consists of a single "shard directory" file. This file includes
    /// a sequence of stripe directories, along with their field descriptors, shard index
    /// descriptors, the schema, and a table of contents that references all these elements.
    /// The second level comprises a collection of stripe data files, with one file
    /// for each stripe.
    #[default]
    TwoLevel,

    /// An entire shard written as a single file, an approach best suited for relatively
    /// small or short-lived shards.
    SingleFile,
}

/// Configuration parameters for constructing a `ShardBuilder`.
///
/// A shard is a self-contained data storage unit that consists of one or more stripes,
/// where each stripe contains columnar data organized according to a schema. The shard
/// building process involves creating temporary files during construction and then
/// committing the final shard to persistent storage.
///
/// # Usage
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use amudai_encodings::block_encoder::BlockEncodingProfile;
/// use amudai_format::schema::SchemaMessage;
/// use amudai_io_impl::temp_file_store;
///
/// let params = ShardBuilderParams {
///     schema: schema_message,
///     object_store: todo!(),
///     temp_store: temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap(),
///     encoding_profile: BlockEncodingProfile::Balanced,
/// };
/// let builder = ShardBuilder::new(params)?;
/// ```
#[derive(Clone)]
pub struct ShardBuilderParams {
    /// The schema definition that describes the structure and types of data to be stored.
    ///
    /// This schema is validated during `ShardBuilder` construction and used throughout
    /// the building process to ensure data consistency. It defines the field names,
    /// types, and hierarchical relationships for all data that will be written to the shard.
    pub schema: SchemaMessage,

    /// The persistent object store where the final shard files will be written.
    ///
    /// This store is used for the final shard output, including the main shard directory
    /// file and any associated stripe and index files. The object store must support
    /// creating new objects at the URLs specified during the `seal()` operation.
    ///
    /// # Note
    ///
    /// Wrapped in `Arc` for shared ownership across multiple stripe builders and
    /// to enable concurrent shard building operations.
    pub object_store: Arc<dyn ObjectStore>,

    /// The temporary file store used for intermediate files during shard construction.
    ///
    /// During the building process, stripe data is initially written to temporary storage
    /// before being finalized. This temporary storage should have sufficient capacity
    /// for the working set of data being processed. The temporary files are automatically
    /// cleaned up after the shard building process completes.
    ///
    /// # Performance Considerations
    ///
    /// For optimal performance, use fast storage (e.g., in-memory or SSD-based) for
    /// the temporary store, especially when processing large datasets.
    pub temp_store: Arc<dyn TemporaryFileStore>,

    /// The block encoding profile that determines compression and performance trade-offs.
    ///
    /// This profile influences how individual data blocks within stripes are encoded,
    /// affecting both the time required to build the shard and the final compressed size.
    /// The available profiles offer different trade-offs:
    ///
    /// - [`BlockEncodingProfile::Plain`] - No compression, fastest encoding
    /// - [`BlockEncodingProfile::MinimalCompression`] - Light compression, low CPU overhead
    /// - [`BlockEncodingProfile::Balanced`] - Balanced approach (default)
    /// - [`BlockEncodingProfile::HighCompression`] - Higher compression ratio
    /// - [`BlockEncodingProfile::MinimalSize`] - Maximum compression, slower encoding
    ///
    /// # Default
    ///
    /// When not specified, `BlockEncodingProfile::Balanced` provides a good balance
    /// between compression ratio and encoding performance for most use cases.
    pub encoding_profile: BlockEncodingProfile,

    /// File-level structure of the data shard.
    pub file_organization: ShardFileOrganization,
}

/// A prepared shard that is ready to be sealed.
pub struct PreparedShard {
    params: ShardBuilderParams,
    stripes: Vec<PreparedStripe>,
    properties: ShardPropertiesBuilder,
}

impl PreparedShard {
    /// Completes the shard build process and commits the shard to persistent storage.
    ///
    /// This method finalizes the construction of an Amudai shard by writing all prepared
    /// stripe data, metadata, and directory structures to the specified storage location.
    /// It serves as the primary entry point for converting a `PreparedShard` into a
    /// `SealedShard` that can be used for querying operations.
    ///
    /// # Storage Organization
    ///
    /// The sealing process creates one or more artifacts in the object store:
    ///
    /// - **Primary Artifact**: The shard directory file (typically named `*.amudai.shard`),
    ///   which contains the complete shard metadata, schema, and table of contents.
    /// - **Secondary Artifacts**: Additional stripe and index files, depending on the
    ///   configured file organization strategy (`ShardFileOrganization`).
    ///
    /// The shard directory is always written as the final step.
    ///
    /// # Arguments
    ///
    /// * `shard_url` - The target URL where the primary shard directory file will be stored.
    ///   This URL becomes the canonical identifier for the shard and must be accessible
    ///   by the configured object store. The URL typically uses a `.amudai.shard` extension
    ///   to indicate the shard directory file.
    ///
    /// # Returns
    ///
    /// Returns a `SealedShard` containing the finalized shard directory structure and
    /// metadata. The sealed shard is immediately ready for read operations and queries.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The provided URL cannot be parsed or is invalid for the object store
    /// - The object store fails to create the target artifacts
    /// - Stripe data or metadata cannot be serialized and written
    /// - The shard directory structure cannot be finalized
    pub fn seal(self, shard_url: &str) -> Result<SealedShard> {
        let shard_url = ObjectUrl::parse(shard_url)?;
        let mut writer = ArtifactWriter::framed(
            self.params.object_store.create(&shard_url)?,
            shard_url.as_str(),
        )?;
        let sealed_shard = self.seal_to_writer(&mut writer, None)?;
        writer.seal()?;
        Ok(sealed_shard)
    }

    /// Completes the shard build process by writing all artifacts to the provided writer.
    ///
    /// This method finalizes the shard construction by committing all prepared stripe data
    /// and metadata to storage through the provided `ArtifactWriter`. It handles the complete
    /// shard sealing workflow, including artifact placement, directory generation, and the
    /// creation of a sealed shard descriptor.
    ///
    /// # Artifact Organization
    ///
    /// The method supports different file organization strategies based on the shard's
    /// configuration:
    ///
    /// - **Two-Level Organization**: Stripe data files are written as separate artifacts
    ///   in the specified container, with the shard directory containing references to
    ///   these external files.
    /// - **Single-File Organization**: All stripe data is embedded directly into the
    ///   shard directory file, creating a self-contained artifact.
    ///
    /// # Storage Layout
    ///
    /// The final shard directory follows the Amudai format specification:
    /// ```text
    /// |header|stripe_dirs|field_descriptors|schema|properties|toc|directory_len|footer|
    /// ```
    ///
    /// # Arguments
    ///
    /// * `writer`: The target `ArtifactWriter` where the shard directory will be written.
    ///   This writer determines the final location and URL of the main shard file.
    /// * `container`: Optional container URL for storing shard sub-artifacts (stripes, indexes).
    ///   If `None`, sub-artifacts will be stored in the same container as the shard directory.
    ///   This parameter is ignored for single-file organization where all data is embedded.
    ///
    /// # Returns
    ///
    /// Returns a `SealedShard` containing the finalized shard directory structure and
    /// metadata, ready for querying operations.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Stripe data cannot be written to the object store
    /// - The shard directory structure cannot be serialized
    /// - URL parsing or container resolution fails
    /// - The artifact writer encounters I/O errors
    pub fn seal_to_writer(
        self,
        writer: &mut ArtifactWriter,
        container: Option<&ObjectUrl>,
    ) -> Result<SealedShard> {
        let PreparedShard {
            params,
            stripes,
            properties,
        } = self;

        let shard_url = ObjectUrl::parse(writer.url())?;
        let shard_url_container = shard_url.get_container()?;
        let container = container
            .map(|url| Cow::Borrowed(url))
            .unwrap_or(shard_url_container);

        let stripes = match params.file_organization {
            ShardFileOrganization::TwoLevel => {
                Self::write_stripes_as_separate_artifacts(stripes, &container, &params)?
            }
            ShardFileOrganization::SingleFile => {
                Self::write_stripes_to_single_artifact(stripes, writer)?
            }
        };

        let properties = properties.finish();

        let (directory_ref, directory) =
            Self::write_directory(writer, stripes, properties, &shard_url, &params)?;

        Ok(SealedShard {
            directory_ref,
            directory,
        })
    }

    fn write_directory(
        writer: &mut ArtifactWriter,
        stripes: Vec<SealedStripe>,
        properties: ShardProperties,
        shard_url: &ObjectUrl,
        params: &ShardBuilderParams,
    ) -> Result<(DataRef, shard::ShardDirectory)> {
        let directory_start = writer.position();

        let mut directory = Self::initialize_directory(&stripes);

        let url_list = Self::prepare_url_list(&stripes, shard_url);
        let fields = Self::prepare_field_list(&params.schema.schema()?, &stripes)?;

        directory.stripe_list_ref = Self::write_stripe_list(stripes, writer, shard_url)?.into();
        directory.url_list_ref = Self::write_url_list(writer, &url_list, shard_url)?.into();
        directory.field_list_ref = Self::write_field_list(writer, &fields, shard_url)?.into();
        directory.properties_ref = Self::write_properties(writer, &properties, shard_url)?;
        directory.schema_ref = Self::write_schema(writer, &params.schema)?.into();

        let directory_ref = writer.write_message(&directory)?;
        let directory_size =
            (directory_ref.len() as usize - CHECKSUM_SIZE - MESSAGE_LEN_SIZE) as u32;
        writer.write_all(&directory_size.to_le_bytes())?;

        let directory_end = writer.position();
        let directory_ref = DataRef::new(shard_url.as_str(), directory_start..directory_end);
        Ok((directory_ref, directory))
    }

    fn initialize_directory(stripes: &[SealedStripe]) -> shard::ShardDirectory {
        shard::ShardDirectory {
            total_record_count: stripes.iter().map(|stripe| stripe.total_record_count).sum(),
            deleted_record_count: stripes
                .iter()
                .map(|stripe| stripe.deleted_record_count)
                .sum(),
            stripe_count: stripes.len() as u64,
            ..Default::default()
        }
    }

    fn prepare_url_list(stripes: &[SealedStripe], shard_url: &ObjectUrl) -> shard::UrlList {
        let mut set = AHashSet::<String>::new();
        stripes
            .iter()
            .for_each(|stripe| stripe.collect_urls(&mut set));
        let mut list = shard::UrlList {
            urls: set.into_iter().collect(),
        };
        list.compact_data_refs(shard_url);
        list.urls.retain(|url| !url.is_empty());
        list
    }

    fn prepare_field_list(
        schema: &Schema,
        stripes: &[SealedStripe],
    ) -> Result<Vec<Option<shard::FieldDescriptor>>> {
        let last_schema_id = schema.next_schema_id()?;
        let mut descriptors = Vec::with_capacity(last_schema_id.as_usize());

        let mut schema_id = SchemaId::zero();
        while schema_id < last_schema_id {
            let mut field_present = false;
            let mut descriptor = shard::FieldDescriptor::default();
            for stripe in stripes {
                let Some(field) = stripe.fields.get(schema_id) else {
                    continue;
                };
                let stripe_desc = field
                    .descriptor
                    .field
                    .as_ref()
                    .expect("Stripe field should have a descriptor");

                // Merge the stripe-level field descriptor into the shard-level one
                field_descriptor::merge(&mut descriptor, stripe_desc)?;
                field_present = true;
            }

            descriptors.push(field_present.then_some(descriptor));
            schema_id = schema_id.next();
        }

        Ok(descriptors)
    }

    fn write_url_list(
        writer: &mut ArtifactWriter,
        url_list: &shard::UrlList,
        shard_url: &ObjectUrl,
    ) -> Result<DataRef> {
        let mut data_ref = writer.write_message(url_list)?;
        data_ref.try_make_relative(Some(shard_url));
        Ok(data_ref)
    }

    fn write_field_list(
        writer: &mut ArtifactWriter,
        fields: &[Option<shard::FieldDescriptor>],
        shard_url: &ObjectUrl,
    ) -> Result<DataRef> {
        let mut field_refs = DataRefArray::default();
        for field in fields {
            let field_ref = if let Some(field) = field.as_ref() {
                writer.write_message(field)?
            } else {
                DataRef::empty()
            };
            field_refs.push(field_ref);
        }
        let mut field_list_ref = writer.write_message(&field_refs)?;
        field_list_ref.try_make_relative(Some(shard_url));
        Ok(field_list_ref)
    }

    /// Serializes and writes shard properties to persistent storage.
    ///
    /// This method handles the storage of custom metadata associated with the shard,
    /// such as creation timestamps, user-defined properties, and other descriptive
    /// information. The properties are optional and only written if non-empty.
    ///
    /// # Arguments
    ///
    /// * `writer` - The artifact writer used to serialize the properties to storage
    /// * `properties` - The shard properties to be written, containing metadata like
    ///   creation time, custom key-value pairs, and other descriptive information
    /// * `shard_url` - The base URL of the shard, used to create relative references
    ///   when possible to reduce storage overhead and improve portability
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some(DataRef))` containing a reference to the written properties
    /// data if the properties contain any data. Returns `Ok(None)` if the properties
    /// are empty, indicating that no data was written and no storage space was consumed.
    ///
    /// # Storage Optimization
    ///
    /// When properties are written, the resulting `DataRef` is automatically converted
    /// to a relative reference when possible, reducing the size of stored URLs and
    /// making shards more portable between storage systems.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The properties cannot be serialized to the protobuf format
    /// - The underlying writer encounters an I/O error during writing
    /// - The relative URL conversion fails due to malformed URLs
    fn write_properties(
        writer: &mut ArtifactWriter,
        properties: &ShardProperties,
        shard_url: &ObjectUrl,
    ) -> Result<Option<DataRef>> {
        if properties.is_empty() {
            return Ok(None);
        }
        let mut data_ref = writer.write_message(properties)?;
        data_ref.try_make_relative(Some(shard_url));
        Ok(Some(data_ref))
    }

    fn write_schema(writer: &mut ArtifactWriter, schema: &SchemaMessage) -> Result<DataRef> {
        let start = writer.position();
        writer.write_all(schema.as_bytes())?;
        let end = writer.position();
        Ok(DataRef::new("", start..end))
    }

    fn write_stripe_list(
        stripes: Vec<SealedStripe>,
        writer: &mut ArtifactWriter,
        shard_url: &ObjectUrl,
    ) -> Result<DataRef> {
        let mut stripe_list = shard::StripeList::default();
        let mut record_offset = 0u64;
        for mut stripe in stripes {
            stripe.compact_data_refs(shard_url);

            let stripe_dir = stripe.into_directory(writer, record_offset, Some(shard_url))?;

            record_offset = record_offset
                .checked_add(stripe_dir.total_record_count)
                .expect("add record offset");

            stripe_list.stripes.push(stripe_dir);
        }
        let mut data_ref = writer.write_message(&stripe_list)?;
        data_ref.try_make_relative(Some(shard_url));
        Ok(data_ref)
    }

    fn write_stripes_as_separate_artifacts(
        stripes: Vec<PreparedStripe>,
        container: &ObjectUrl,
        params: &ShardBuilderParams,
    ) -> Result<Vec<SealedStripe>> {
        let mut sealed = Vec::with_capacity(stripes.len());
        for stripe in stripes {
            let url = Self::get_stripe_url(container)?;
            let writer = params.object_store.create(&url)?;
            let mut writer = ArtifactWriter::framed(writer, url.as_str())?;
            let stripe = stripe.seal(&mut writer)?;
            writer.seal()?;
            sealed.push(stripe);
        }
        Ok(sealed)
    }

    fn write_stripes_to_single_artifact(
        stripes: Vec<PreparedStripe>,
        writer: &mut ArtifactWriter,
    ) -> Result<Vec<SealedStripe>> {
        let mut sealed = Vec::with_capacity(stripes.len());
        for stripe in stripes {
            let stripe = stripe.seal(writer)?;
            sealed.push(stripe);
        }
        Ok(sealed)
    }

    fn get_stripe_url(shard_url: &ObjectUrl) -> Result<ObjectUrl> {
        let stripe_name = format!("{:x}.amudai.stripe", fastrand::u64(..));
        shard_url.resolve_relative(RelativePath::new(&stripe_name)?)
    }
}

/// Represents a completed shard that has been finalized and committed to persistent storage.
///
/// A `SealedShard` is the final result of the shard building process, containing all the
/// metadata and references necessary to read the shard data back from storage. Once sealed,
/// a shard becomes immutable and ready for querying operations.
///
/// The sealed shard provides two key pieces of information:
/// 1. A reference to where the shard directory is stored (`directory_ref`)
/// 2. The parsed shard directory structure itself (`directory`)
///
/// # Storage Format
///
/// The shard directory follows a specific binary format in storage:
/// ```text
/// |message_len:u32|protobuf_shard_directory|checksum:u32|directory_len:u32|
/// ```
///
/// Note that an additional length field is appended after the standard Amudai message
/// format to enable reverse reading of the directory from the end of the file.
#[derive(Debug, Clone)]
pub struct SealedShard {
    /// Reference to the shard directory's location and range within persistent storage.
    ///
    /// This `DataRef` contains both the URL of the storage blob containing the shard
    /// directory and the byte range within that blob where the serialized directory
    /// is located. The referenced data follows the Amudai message format with an
    /// additional trailing length field for reverse navigation.
    ///
    /// The directory reference enables:
    /// - Locating the shard directory for re-reading
    /// - Efficient storage of shard metadata alongside or separate from stripe data
    /// - Support for both single-file and multi-file shard organizations
    ///
    /// # Format Details
    ///
    /// The structure at the referenced location is:
    /// ```text
    /// |message_len:u32|protobuf_shard_directory|checksum:u32|directory_len:u32|
    /// ```
    ///
    /// Where:
    /// - `message_len`: Length of the protobuf message in little-endian format
    /// - `protobuf_shard_directory`: The serialized shard directory protobuf
    /// - `checksum`: xxh3-based checksum of the protobuf data for integrity verification
    /// - `directory_len`: Duplicate length field to enable reading from the end of the file
    pub directory_ref: DataRef,

    /// The shard directory containing the complete structural metadata of the shard.
    pub directory: shard::ShardDirectory,
}
