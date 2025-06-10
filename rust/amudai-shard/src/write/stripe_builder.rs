//! Creation of the shard's data stripes.

use std::sync::Arc;

use amudai_common::{Result, error::Error};
use amudai_encodings::block_encoder::BlockEncodingProfile;
use amudai_format::defs::common::DataRef;
use amudai_format::defs::{self, shard};
use amudai_format::schema::{DataType, FieldLocator, Schema, SchemaId};
use amudai_io::temp_file_store::TemporaryFileStore;
use amudai_keyed_vector::KeyedVector;
use amudai_objectstore::url::ObjectUrl;
use arrow_array::RecordBatch;

use super::format_elements_ext::{CompactDataRefs, DataRefExt};
use super::{
    artifact_writer::ArtifactWriter,
    field_builder::{
        FieldBuilder, FieldBuilderParams, FieldBuilders, PreparedStripeField, SealedStripeField,
    },
};

/// A builder for creating data stripes.
///
/// A `StripeBuilder` is responsible for collecting data records and preparing them
/// to be added to a shard.
pub struct StripeBuilder {
    params: StripeBuilderParams,
    fields: FieldBuilders,
    /// Logical position of the next record batch, aka the total number of records
    /// in already consumed batches.
    batch_pos: usize,
}

impl StripeBuilder {
    /// Creates a new `StripeBuilder` with the given parameters.
    pub fn new(params: StripeBuilderParams) -> Result<StripeBuilder> {
        let schema = params.schema.clone();
        Ok(StripeBuilder {
            params,
            fields: FieldBuilders::new(schema)?,
            batch_pos: 0,
        })
    }

    /// Returns a reference to the shard's schema associated with this stripe.
    pub fn schema(&self) -> &Schema {
        &self.params.schema
    }

    /// Returns a mutable reference to a `FieldBuilder` at the given index.
    pub fn get_field(&mut self, locator: &FieldLocator) -> Result<&mut FieldBuilder> {
        self.fields.get_or_create(locator, |index, data_type| {
            Self::create_field(&self.params, index, data_type)
        })
    }

    /// Appends a batch of records to the stripe.
    ///
    /// This function takes a `RecordBatch` and appends its data to the underlying fields
    /// of the stripe.
    ///
    /// **Note**: The structure of the `batch` and all of its fields, including nested ones,
    /// should generally match the stripe/shard schema. Missing record and struct fields
    /// are allowed. At this level, shard and stripe builders do not perform any
    /// transformation of the incoming data, aside from low-level value normalizations
    /// (such as converting Arrow's timestamps and dates to Amudai's ticks, or handling
    /// decimals). In particular, any necessary column shredding should have already been
    /// performed by higher-level data processing routines.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the batch is successfully appended.
    /// * `Err(Error)` if an error occurs during the process, such as inconsistent
    ///   batch and stripe schemas.
    pub fn push_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_columns() > self.fields.len() {
            // Possible data loss.
            return Err(Error::invalid_arg(
                "batch",
                format!(
                    "record batch has more columns ({}) than the stripe ({})",
                    batch.num_columns(),
                    self.fields.len()
                ),
            ));
        }

        // Capture the current record position before appending the new batch.
        // This position is used to synchronize the fields with the parent stripe.
        let record_pos = self.batch_pos;
        let batch_fields = batch.schema_ref().fields();
        let field_locators =
            create_field_locators(batch_fields.len(), |i| batch_fields[i].name().as_str());

        for (i, locator) in field_locators.enumerate() {
            let field = self.get_field(&locator)?;
            field.sync_with_parent(record_pos)?;
            field.push_array(batch.column(i).clone())?;
        }

        self.batch_pos += batch.num_rows();
        Ok(())
    }

    /// Finishes building the stripe and returns a `PreparedStripe`
    /// that can be added to the `ShardBuilder`.
    pub fn finish(self) -> Result<PreparedStripe> {
        let logical_pos = self.batch_pos;
        let mut fields = Vec::new();
        self.fields.finish(Some(logical_pos), &mut fields)?;
        Ok(PreparedStripe {
            fields: fields.into(),
            record_count: logical_pos as u64,
        })
    }
}

impl StripeBuilder {
    fn create_field(
        params: &StripeBuilderParams,
        _index: usize,
        data_type: &DataType,
    ) -> Result<FieldBuilder> {
        let params = FieldBuilderParams {
            data_type: data_type.clone(),
            temp_store: params.temp_store.clone(),
            encoding_profile: params.encoding_profile,
        };
        FieldBuilder::new(params)
    }
}

/// A prepared stripe that is ready to be added to a shard.
pub struct PreparedStripe {
    pub fields: KeyedVector<SchemaId, PreparedStripeField>,
    pub record_count: u64,
}

impl PreparedStripe {
    /// Finalizes this prepared stripe by sealing all its fields and writing their data
    /// to the provided artifact writer (typically a stripe data blob).
    ///
    /// # Parameters
    /// - `writer`: The [`ArtifactWriter`] used to persist the field data.
    ///
    /// # Returns
    /// - `Ok(SealedStripe)`: If all fields are successfully sealed and written.
    /// - `Err(Error)`: If any field fails to seal or write, or if an I/O error occurs.
    ///
    /// # Side Effects
    /// - Consumes the prepared stripe; it cannot be reused after calling this method.
    /// - Writes each field's data to artifact storage as part of the sealing process.
    pub fn seal(self, writer: &mut ArtifactWriter) -> Result<SealedStripe> {
        let mut fields = Vec::with_capacity(self.fields.len());
        for field in self.fields.into_values() {
            let field = field.seal(writer)?;
            fields.push(field);
        }
        Ok(SealedStripe {
            fields: fields.into(),
            total_record_count: self.record_count,
            deleted_record_count: 0,
        })
    }
}

/// A sealed stripe located in the final storage.
pub struct SealedStripe {
    /// Sealed stripe fields.
    pub fields: KeyedVector<SchemaId, SealedStripeField>,
    pub total_record_count: u64,
    pub deleted_record_count: u64,
}

impl SealedStripe {
    /// Finalizes the sealed stripe by writing all field metadata to the provided artifact writer,
    /// producing a `StripeDirectory` that references the written metadata.
    ///
    /// # Parameters
    /// - `writer`: The [`ArtifactWriter`] used to persist the stripe's metadata
    ///   (typically a shard directory blob).
    /// - `record_offset`: The logical offset of the first record in this stripe within the shard.
    /// - `shard_url`: An optional base URL for relativizing data references in the metadata.
    ///
    /// # Returns
    /// - `Ok(shard::StripeDirectory)`: A directory entry referencing the committed metadata
    ///   for this stripe.
    /// - `Err(Error)`: If writing the metadata fails.
    ///
    /// # Side Effects
    /// - Consumes the `SealedStripe`; it cannot be reused after this method is called.
    /// - Writes metadata for each field to artifact storage.
    /// - Optionally relativizes data references if `shard_url` is provided.
    pub fn into_directory(
        self,
        writer: &mut ArtifactWriter,
        record_offset: u64,
        shard_url: Option<&ObjectUrl>,
    ) -> Result<shard::StripeDirectory> {
        let mut field_list = defs::common::DataRefArray::default();
        let last_schema_id = self.fields.key_range().end;
        let mut schema_id = SchemaId::zero();
        while schema_id < last_schema_id {
            let field_ref = if let Some(field) = self.fields.get(schema_id) {
                field.write_descriptor(writer)?
            } else {
                DataRef::empty()
            };
            field_list.push(field_ref);
            schema_id = schema_id.next();
        }

        let mut field_list_ref = writer.write_message(&field_list)?;
        field_list_ref.try_make_relative(shard_url);

        Ok(shard::StripeDirectory {
            properties_ref: None,
            field_list_ref: Some(field_list_ref),
            indexes_ref: None,
            total_record_count: self.total_record_count,
            deleted_record_count: self.deleted_record_count,
            stored_data_size: None,
            stored_index_size: None,
            plain_data_size: None,
            record_offset,
        })
    }
}

impl CompactDataRefs for SealedStripe {
    fn collect_urls(&self, set: &mut ahash::AHashSet<String>) {
        self.fields
            .values()
            .iter()
            .for_each(|field| field.collect_urls(set));
    }

    fn compact_data_refs(&mut self, shard_url: &ObjectUrl) {
        self.fields
            .values_mut()
            .iter_mut()
            .for_each(|field| field.compact_data_refs(shard_url));
    }
}

#[derive(Clone)]
pub struct StripeBuilderParams {
    /// The schema for the stripe and the containing shard.
    pub schema: Schema,
    /// The temp file store to use when building the stripe.
    pub temp_store: Arc<dyn TemporaryFileStore>,
    /// Encoding profile for a stripe.
    /// See [`ShardBuilderParams::encoding_profile`](`crate::write::shard_builder::ShardBuilderParams::encoding_profile`)
    pub encoding_profile: BlockEncodingProfile,
}

/// Creates an iterator of `FieldLocator`s based on a count and a function that provides field names.
///
/// This function determines whether to use ordinal or named field locators based on whether any of the
/// field names returned by `field_name_fn` are empty. If any field name is empty, it will use ordinal
/// locators; otherwise, it will use named locators.
///
/// # Arguments
///
/// * `count`: The number of fields to generate locators for.
/// * `field_name_fn`: A function that takes a field index (usize) and returns a string representing
///   the field's name.
///
/// # Returns
///
/// An iterator that yields `FieldLocator`s. Each `FieldLocator` will be either an `Ordinal` variant
/// containing the field's index, or a `Name` variant containing the field's name, depending on whether
/// any of the field names were empty.
pub(in crate::write) fn create_field_locators<'a>(
    count: usize,
    field_name_fn: impl Fn(usize) -> &'a str,
) -> impl Iterator<Item = FieldLocator<'a>> {
    let use_ordinals = (0..count).any(|i| field_name_fn(i).is_empty());
    (0..count).map(move |i| {
        if use_ordinals {
            FieldLocator::Ordinal(i)
        } else {
            FieldLocator::Name(field_name_fn(i))
        }
    })
}
