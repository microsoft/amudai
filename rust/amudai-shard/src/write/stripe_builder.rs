//! Creation of the shard's data stripes.

use std::sync::Arc;

use amudai_common::{Result, error::Error};
use amudai_encodings::block_encoder::BlockEncodingProfile;
use amudai_format::defs::common::DataRef;
use amudai_format::defs::shard::StripeProperties;
use amudai_format::defs::{self, shard};
use amudai_format::schema::{DataType, FieldLocator, Schema, SchemaId};
use amudai_io::temp_file_store::TemporaryFileStore;
use amudai_keyed_vector::KeyedVector;
use amudai_objectstore::url::ObjectUrl;
use arrow_array::RecordBatch;

use crate::write::properties::StripePropertiesBuilder;

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
    properties: StripePropertiesBuilder,
}

impl StripeBuilder {
    /// Creates a new `StripeBuilder` with the given parameters.
    pub fn new(params: StripeBuilderParams) -> Result<StripeBuilder> {
        let schema = params.schema.clone();
        Ok(StripeBuilder {
            params,
            fields: FieldBuilders::new(schema)?,
            batch_pos: 0,
            properties: Default::default(),
        })
    }

    /// Returns a reference to the shard's schema associated with this stripe.
    pub fn schema(&self) -> &Schema {
        &self.params.schema
    }

    /// Retrieves or creates a mutable reference to a `FieldBuilder` for manual data insertion.
    ///
    /// This method provides direct access to individual field builders, enabling fine-grained
    /// control over data insertion when the high-level [`push_batch`](Self::push_batch) API
    /// is insufficient. It's particularly useful for memory-efficient processing of large
    /// or complex data structures, and for scenarios requiring custom data transformation
    /// or chunked processing.
    ///
    /// # Field Location
    ///
    /// Fields can be located in two ways:
    /// - **By ordinal position**: `FieldLocator::Ordinal(index)` - Uses the zero-based field index
    /// - **By name**: `FieldLocator::Name(name)` - Uses the field name from the schema
    ///
    /// The method will automatically create a new `FieldBuilder` if one doesn't already exist
    /// for the specified field, using the field's data type from the stripe's schema.
    ///
    /// # Use Cases
    ///
    /// ## Memory-Constrained Scenarios
    /// When creating complete `RecordBatch` instances would be impractical due to memory
    /// constraints, this method allows you to build field data incrementally:
    ///
    /// ```rust,ignore
    /// // For a List<i64> field with millions of items per list
    /// let list_field = stripe_builder.get_field(&FieldLocator::Name("large_numbers"))?;
    /// let items_field = list_field.get_child(&FieldLocator::Ordinal(0))?;
    ///
    /// // Push data in small chunks instead of one massive array
    /// for chunk in data_chunks {
    ///     items_field.push_array(chunk)?;
    /// }
    ///
    /// // Manually handle list offsets
    /// list_field.push_array(offsets_array)?;
    /// stripe_builder.set_next_record_position(total_lists);
    /// ```
    ///
    /// ## Custom Data Processing
    /// For scenarios requiring data transformation or custom encoding that doesn't fit
    /// the standard batch processing model:
    ///
    /// ```rust,ignore
    /// // Process nested structures with custom logic
    /// let struct_field = stripe_builder.get_field(&FieldLocator::Ordinal(2))?;
    /// let name_field = struct_field.get_child(&FieldLocator::Name("name"))?;
    /// let scores_field = struct_field.get_child(&FieldLocator::Name("scores"))?;
    ///
    /// // Apply custom transformations and push to individual fields
    /// name_field.push_array(transformed_names)?;
    /// scores_field.push_array(processed_scores)?;
    /// ```
    ///
    /// # Field Builder Operations
    ///
    /// Once you have a `FieldBuilder`, you can:
    /// - **Push arrays**: [`FieldBuilder::push_array`] to add Arrow arrays
    /// - **Access children**: [`FieldBuilder::get_child`] for nested structures
    /// - **Sync positions**: [`FieldBuilder::sync_with_parent`] to align with parent records
    ///
    /// # Important Notes
    ///
    /// - **Position tracking**: When using manual field insertion, you're responsible for
    ///   maintaining consistent record positions across all fields. Use
    ///   [`set_next_record_position`](Self::set_next_record_position) to coordinate.
    /// - **Schema compatibility**: Arrays pushed to fields must be compatible with the
    ///   field's data type as defined in the stripe's schema.
    /// - **Child field access**: For nested types (structs, lists, maps), use the returned
    ///   `FieldBuilder`'s [`get_child`](FieldBuilder::get_child) method to access nested fields.
    ///
    /// # Parameters
    ///
    /// * `locator` - A [`FieldLocator`] specifying the field by either ordinal index or name
    ///
    /// # Returns
    ///
    /// * `Ok(&mut FieldBuilder)` - A mutable reference to the field builder
    /// * `Err(Error)` - If the field cannot be found or created (e.g., invalid index/name)
    ///
    /// # See Also
    ///
    /// * [`push_batch`](Self::push_batch) - High-level API for pushing complete record batches
    /// * [`set_next_record_position`](Self::set_next_record_position) - For coordinating record positions
    /// * [`FieldBuilder`] - For operations available on individual field builders
    /// * [`FieldLocator`] - For field addressing options
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

    /// Manually sets the logical position of the next record to be added to the stripe.
    ///
    /// This method updates the internal record position counter that tracks the total number
    /// of records that have been logically added to the stripe. It's essential for maintaining
    /// consistency when using the low-level field manipulation APIs instead of the high-level
    /// [`push_batch`](Self::push_batch) method.
    ///
    /// # When to Use
    ///
    /// This method should **only** be used when manually manipulating stripe fields through
    /// [`get_field`](Self::get_field) and directly pushing arrays to individual
    /// [`FieldBuilder`] instances. It's not needed when using [`push_batch`](Self::push_batch),
    /// as that method automatically manages record positions.
    ///
    /// # Position Coordination
    ///
    /// When working with multiple fields manually, it's crucial to keep their logical record
    /// counts synchronized. This method helps coordinate the stripe-level position with
    /// field-level positions:
    ///
    /// ```rust,ignore
    /// // After manually adding 1000 records to various fields
    /// let name_field = stripe_builder.get_field(&FieldLocator::Name("name"))?;
    /// let age_field = stripe_builder.get_field(&FieldLocator::Name("age"))?;
    ///
    /// // Push arrays with 1000 elements each
    /// name_field.push_array(names_array)?;  // 1000 names
    /// age_field.push_array(ages_array)?;    // 1000 ages
    ///
    /// // Update stripe's logical position to reflect the new records
    /// stripe_builder.set_next_record_position(1000);
    /// ```
    ///
    /// # Memory-Efficient Processing
    ///
    /// This method is particularly useful in memory-constrained scenarios where you're
    /// processing data in chunks but need to maintain accurate record counts:
    ///
    /// ```rust,ignore
    /// let mut total_records = 0;
    /// for chunk in data_chunks {
    ///     let field = stripe_builder.get_field(&FieldLocator::Ordinal(0))?;
    ///     field.push_array(chunk.to_arrow_array())?;
    ///     total_records += chunk.len();
    /// }
    ///
    /// // Synchronize the stripe's logical position
    /// stripe_builder.set_next_record_position(total_records as u64);
    /// ```
    ///
    /// # Position Invariants
    ///
    /// The method enforces that the new position must be greater than or equal to the
    /// current position, preventing accidental position regression that could lead to
    /// data inconsistencies.
    ///
    /// # Parameters
    ///
    /// * `pos` - The logical position of the next record to be added. This represents
    ///   the total count of records that have been logically processed by the stripe.
    ///   Must be >= the current position.
    ///
    /// # Panics
    ///
    /// Panics if `pos` is less than the current record position, as this would indicate
    /// a programming error that could lead to data corruption.
    ///
    /// # Important Notes
    ///
    /// - **Not for batch processing**: Don't use this method when using [`push_batch`](Self::push_batch);
    ///   the batch API handles position management automatically.
    /// - **Position tracking**: The position represents logical records, not physical
    ///   array elements. For nested structures, this should reflect the number of
    ///   top-level records, not the total number of nested elements.
    ///
    /// # See Also
    ///
    /// * [`get_field`](Self::get_field) - For manual field manipulation
    /// * [`push_batch`](Self::push_batch) - High-level API with automatic position management
    pub fn set_next_record_position(&mut self, pos: u64) {
        assert!(pos >= self.batch_pos as u64);
        self.batch_pos = pos as usize;
    }

    /// Returns a reference to the stripe properties builder.
    ///
    /// This method provides read-only access to the `StripePropertiesBuilder`
    /// associated with this stripe, allowing inspection of current property values
    /// without the ability to modify them.
    ///
    /// # Returns
    ///
    /// A reference to the stripe's `StripePropertiesBuilder`.
    pub fn properties(&self) -> &StripePropertiesBuilder {
        &self.properties
    }

    /// Returns a mutable reference to the stripe properties builder.
    ///
    /// This method provides mutable access to the `StripePropertiesBuilder`
    /// associated with this stripe, allowing properties to be set or modified.
    ///
    /// # Returns
    ///
    /// A mutable reference to the stripe's `StripePropertiesBuilder`.
    pub fn properties_mut(&mut self) -> &mut StripePropertiesBuilder {
        &mut self.properties
    }

    /// Finishes building the stripe and returns a `PreparedStripe`
    /// that can be added to the `ShardBuilder`.
    pub fn finish(self) -> Result<PreparedStripe> {
        let logical_pos = self.batch_pos;
        let mut fields = Vec::new();
        self.fields.finish(Some(logical_pos), &mut fields)?;
        Ok(PreparedStripe {
            fields: fields.into(),
            properties: self.properties,
            record_count: logical_pos as u64,
            shard_position: 0, // Final value is assigned by the shard builder
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
    /// Encoded stripe fields mapped to their `SchemaId`.
    pub fields: KeyedVector<SchemaId, PreparedStripeField>,

    /// Stripe property bags.
    pub properties: StripePropertiesBuilder,

    /// Total number of records in this stripe.
    pub record_count: u64,

    /// The logical position of this stripe's first record within the containing
    /// shard.
    ///
    /// This represents the shard-absolute offset where this stripe begins.
    /// For example, if previous stripes in the shard contain 1000 records total,
    /// this stripe's `shard_position` would be 1000, and its records would be
    /// numbered from 1000 to 1000 + `record_count` - 1.
    ///
    /// This value is assigned by the shard builder when the stripe is added
    /// to a shard, and remains 0 until that point.
    pub shard_position: u64,
}

impl PreparedStripe {
    /// Returns the next shard-absolute logical record position after this stripe.
    pub fn next_record_position(&self) -> u64 {
        self.shard_position
            .checked_add(self.record_count)
            .expect("next record pos")
    }

    /// Creates a field decoder from the prepared stripe field with the given `schema_id`.
    ///
    /// This method constructs a [`FieldDecoder`](crate::read::field_decoder::FieldDecoder)
    /// that can read back the data that was encoded in the prepared stripe field.
    pub fn decode_field(
        &self,
        schema_id: SchemaId,
    ) -> Result<crate::read::field_decoder::FieldDecoder> {
        let field = self.fields.get(schema_id).ok_or_else(|| {
            Error::invalid_arg("schema_id", format!("field {schema_id:?} not found"))
        })?;
        field.create_decoder()
    }

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
            properties: self.properties.finish(),
            total_record_count: self.record_count,
            deleted_record_count: 0,
            shard_position: self.shard_position,
        })
    }
}

/// A sealed stripe located in the final storage.
pub struct SealedStripe {
    /// The collection of sealed fields that comprise this stripe, indexed by their
    /// schema IDs.
    pub fields: KeyedVector<SchemaId, SealedStripeField>,

    /// Additional metadata and configuration properties associated with this stripe.
    pub properties: StripeProperties,

    /// The total number of logical records encoded in this stripe, including any
    /// soft-deleted records.
    pub total_record_count: u64,

    /// The number of records marked as soft-deleted within this stripe.
    ///
    /// This value is always zero for newly encoded stripes and only becomes non-zero
    /// after deletion operations have been applied.
    pub deleted_record_count: u64,

    /// The starting position of this stripe within its parent shard.
    ///
    /// This value represents the zero-based index of the first record in this stripe
    /// relative to the beginning of the shard. For example, if previous stripes contain
    /// 1,000 records in total, this stripe's `shard_position` would be 1,000.
    pub shard_position: u64,
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
        shard_url: Option<&ObjectUrl>,
    ) -> Result<shard::StripeDirectory> {
        let mut field_list = defs::common::DataRefArray::default();
        let last_schema_id = self.fields.key_range().end;
        let mut schema_id = SchemaId::zero();

        // Calculate the total raw data size
        let total_raw_data_size = self.calculate_total_raw_data_size();

        while schema_id < last_schema_id {
            let mut field_ref = if let Some(field) = self.fields.get(schema_id) {
                field.write_descriptor(writer)?
            } else {
                DataRef::empty()
            };
            field_ref.try_make_relative(shard_url);
            field_list.push(field_ref);
            schema_id = schema_id.next();
        }

        let mut field_list_ref = writer.write_message(&field_list)?;
        field_list_ref.try_make_relative(shard_url);

        let properties_ref = if !self.properties.is_empty() {
            let mut properties_ref = writer.write_message(&self.properties)?;
            properties_ref.try_make_relative(shard_url);
            Some(properties_ref)
        } else {
            None
        };

        Ok(shard::StripeDirectory {
            properties_ref,
            field_list_ref: Some(field_list_ref),
            indexes_ref: None,
            total_record_count: self.total_record_count,
            deleted_record_count: self.deleted_record_count,
            raw_data_size: total_raw_data_size,
            shard_position: self.shard_position,
        })
    }

    /// Calculates the total raw data size by summing up raw_data_size from all fields.
    /// Fields without raw_data_size (None) are skipped.
    ///
    /// # Returns
    /// - `Some(total_size)`: If at least one field has raw_data_size information
    /// - `None`: If no fields have raw_data_size information
    pub fn calculate_total_raw_data_size(&self) -> Option<u64> {
        let mut total_size = 0u64;
        let mut has_any_size = false;

        let last_schema_id = self.fields.key_range().end;
        let mut schema_id = SchemaId::zero();

        while schema_id < last_schema_id {
            if let Some(field) = self.fields.get(schema_id) {
                if let Some(field_descriptor) = &field.descriptor.field {
                    if let Some(field_size) = field_descriptor.raw_data_size {
                        total_size += field_size;
                        has_any_size = true;
                    }
                    // Skip fields with None raw_data_size
                }
                // Fields without descriptors are skipped
            }
            schema_id = schema_id.next();
        }

        if has_any_size { Some(total_size) } else { None }
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
