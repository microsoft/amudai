//! This module provides structures and functions for building and managing data shards
//! and stripes.
//!
//! The core idea is to build data in a structured way: first, by defining a schema; then,
//! creating stripes of data; and finally, combining stripes into a shard.

use std::sync::Arc;

use ahash::AHashSet;
use amudai_common::Result;
use amudai_encodings::block_encoder::BlockEncodingProfile;
use amudai_format::{
    defs::{
        CHECKSUM_SIZE, MESSAGE_LEN_SIZE,
        common::{DataRef, DataRefArray},
        shard,
    },
    schema::{Schema, SchemaId, SchemaMessage},
};
use amudai_io::temp_file_store::TemporaryFileStore;
use amudai_objectstore::{
    ObjectStore,
    url::{ObjectUrl, RelativePath},
};

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
}

impl ShardBuilder {
    /// Creates a new `ShardBuilder` with the given parameters.
    pub fn new(params: ShardBuilderParams) -> Result<ShardBuilder> {
        let schema = params.schema.schema()?;
        Ok(ShardBuilder {
            params,
            schema,
            stripes: Vec::new(),
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

    /// Finishes building the shard and returns a `PreparedShard`.
    pub fn finish(self) -> Result<PreparedShard> {
        Ok(PreparedShard {
            params: self.params,
            stripes: self.stripes,
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

    pub file_organization: ShardFileOrganization,
}

/// A prepared shard that is ready to be sealed.
pub struct PreparedShard {
    params: ShardBuilderParams,
    stripes: Vec<PreparedStripe>,
}

impl PreparedShard {
    /// Completes the shard build process and commits the shard to the store.
    ///
    /// # Arguments
    ///
    /// * `shard_url`: The URL of the resulting shard that points to the root shard file
    ///   (also known as the shard directory, typically named *.amudai.shard).
    pub fn seal(self, shard_url: &str) -> Result<SealedShard> {
        let PreparedShard { params, stripes } = self;
        let shard_url = ObjectUrl::parse(shard_url)?;
        let container = shard_url.get_container()?;
        let mut writer =
            ArtifactWriter::framed(params.object_store.create(&shard_url)?, shard_url.as_str())?;

        let stripes = match params.file_organization {
            ShardFileOrganization::TwoLevel => {
                Self::write_stripes_as_separate_artifacts(stripes, &container, &params)?
            }
            ShardFileOrganization::SingleFile => {
                Self::write_stripes_to_single_artifact(stripes, &mut writer)?
            }
        };

        let (directory_blob, directory) =
            Self::write_directory(writer, stripes, &shard_url, &params)?;

        Ok(SealedShard {
            directory_blob,
            directory,
        })
    }

    fn write_directory(
        mut writer: ArtifactWriter,
        stripes: Vec<SealedStripe>,
        shard_url: &ObjectUrl,
        params: &ShardBuilderParams,
    ) -> Result<(DataRef, shard::ShardDirectory)> {
        let mut directory = Self::initialize_directory(&stripes);

        let url_list = Self::prepare_url_list(&stripes, shard_url);
        let fields = Self::prepare_field_list(&params.schema.schema()?, &stripes)?;

        directory.stripe_list_ref =
            Self::write_stripe_list(stripes, &mut writer, shard_url)?.into();
        directory.url_list_ref = Self::write_url_list(&mut writer, &url_list, shard_url)?.into();
        directory.field_list_ref = Self::write_field_list(&mut writer, &fields, shard_url)?.into();
        directory.schema_ref = Self::write_schema(&mut writer, &params.schema)?.into();

        let directory_ref = writer.write_message(&directory)?;
        let directory_size =
            (directory_ref.len() as usize - CHECKSUM_SIZE - MESSAGE_LEN_SIZE) as u32;
        writer.write_all(&directory_size.to_le_bytes())?;

        let blob_ref = writer.seal()?;
        Ok((blob_ref, directory))
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

/// Represents a sealed shard that has been placed in its final storage location.
#[derive(Debug, Clone)]
pub struct SealedShard {
    pub directory_blob: DataRef,
    pub directory: shard::ShardDirectory,
}
