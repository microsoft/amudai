//! Shard Record Batch Reader Module
//!
//! Provides high-level readers for producing Apache Arrow record batches
//! from Amudai shards.
//! Includes iterators for reading batches from shards and stripes, and
//! utilities for schema and field resolution.

use std::{ops::Range, sync::Arc};

use amudai_arrow_compat::amudai_to_arrow_error::ToArrowResult;
use amudai_collections::{
    range_iterators::{ChunkedRanges, RangeIteratorsExt, ShiftDownRanges},
    range_list::{RangeList, RangeListIntoIter},
};
use amudai_common::try_or_ret_some_err;
use amudai_format::schema::FieldList;
use amudai_shard::read::{shard::Shard, stripe::Stripe};
use arrow::{
    array::{ArrayRef, RecordBatch, RecordBatchOptions},
    error::ArrowError,
};

use crate::array_reader::ArrayReader;

/// Type alias for Arrow schema.
pub type ArrowSchema = arrow::datatypes::Schema;
/// Type alias for Arrow field.
pub type ArrowField = arrow::datatypes::Field;

/// Iterator for reading Arrow record batches from an Amudai shard.
/// Reads batches across stripes, handling position ranges and batch sizing.
pub struct ShardRecordBatchReader {
    shard: Shard,
    arrow_schema: Arc<ArrowSchema>,
    batch_size: u64,
    pos_ranges: RangeList<u64>,
    stripe_reader: Option<StripeRecordBatchReader>,
    next_stripe_ordinal: usize,
}

impl ShardRecordBatchReader {
    /// Constructs a new `ShardRecordBatchReader` for the given shard and schema.
    ///
    /// # Arguments
    /// * `shard` - The Amudai shard to read from.
    /// * `arrow_schema` - The Arrow schema to use for batches.
    /// * `batch_size` - The number of rows per batch.
    /// * `pos_ranges` - Optional position ranges to restrict reading.
    ///
    /// # Errors
    /// Returns an error if construction fails.
    pub fn new(
        shard: Shard,
        arrow_schema: Arc<ArrowSchema>,
        batch_size: u64,
        pos_ranges: Option<RangeList<u64>>,
    ) -> Result<ShardRecordBatchReader, ArrowError> {
        let pos_ranges = pos_ranges
            .unwrap_or_else(|| RangeList::from_elem(0..shard.directory().total_record_count));
        Ok(ShardRecordBatchReader {
            shard,
            arrow_schema,
            batch_size,
            pos_ranges,
            stripe_reader: None,
            next_stripe_ordinal: 0,
        })
    }
}

impl ShardRecordBatchReader {
    /// Attempts to open the next available stripe in the shard that overlaps
    /// with the desired position ranges.
    ///
    /// Returns a new `StripeRecordBatchReader` if a suitable stripe is found,
    /// or `None` if all stripes have been processed.
    /// Returns an error if opening the stripe or constructing the reader fails.
    fn open_next_stripe(&mut self) -> Option<Result<StripeRecordBatchReader, ArrowError>> {
        while self.next_stripe_ordinal < self.shard.stripe_count() {
            let stripe = try_or_ret_some_err!(
                self.shard
                    .open_stripe(self.next_stripe_ordinal)
                    .to_arrow_res()
            );
            self.next_stripe_ordinal += 1;

            let stripe_pos_span = stripe.shard_position_range();
            let stripe_ranges = self.pos_ranges.clamp(stripe_pos_span);
            if !stripe_ranges.is_empty() {
                return Some(StripeRecordBatchReader::new(
                    stripe,
                    self.arrow_schema.clone(),
                    self.batch_size,
                    Some(stripe_ranges),
                ));
            }
        }
        None
    }
}

impl Iterator for ShardRecordBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(stripe_reader) = self.stripe_reader.as_mut() {
                if let Some(batch_res) = stripe_reader.next() {
                    return Some(batch_res);
                } else {
                    // Got to the end of the current stripe. Reset and fall through
                    // to establish the next stripe reader.
                    self.stripe_reader = None;
                }
            }

            assert!(self.stripe_reader.is_none());
            let stripe_reader = try_or_ret_some_err!(self.open_next_stripe()?);
            self.stripe_reader = Some(stripe_reader);
        }
    }
}

/// Iterator for reading Arrow record batches from a single stripe
/// in an Amudai shard.
/// Handles batch sizing and field reading for the stripe.
pub struct StripeRecordBatchReader {
    _stripe: Stripe,
    arrow_schema: Arc<ArrowSchema>,
    batch_ranges: ChunkedRanges<ShiftDownRanges<RangeListIntoIter<u64>>>,
    field_readers: Vec<ArrayReader>,
}

impl StripeRecordBatchReader {
    /// Constructs a new `StripeRecordBatchReader` for the given stripe and schema.
    ///
    /// # Arguments
    /// * `stripe` - The stripe to read from.
    /// * `arrow_schema` - The Arrow schema to use for batches.
    /// * `batch_size` - The number of rows per batch.
    /// * `shard_pos_ranges` - Optional position ranges within the shard.
    ///
    /// # Errors
    /// Returns an error if construction or field reader creation fails.
    pub fn new(
        stripe: Stripe,
        arrow_schema: Arc<ArrowSchema>,
        batch_size: u64,
        shard_pos_ranges: Option<RangeList<u64>>,
    ) -> Result<StripeRecordBatchReader, ArrowError> {
        let shard_pos_ranges =
            shard_pos_ranges.unwrap_or_else(|| RangeList::from_elem(stripe.shard_position_range()));

        let stripe_pos_ranges = shard_pos_ranges
            .clone()
            .into_iter()
            .shift_down(stripe.directory().record_offset);

        let batch_ranges = stripe_pos_ranges.clone().chunk_ranges(batch_size);
        let field_readers = Self::create_field_readers(&stripe, &arrow_schema, stripe_pos_ranges)?;

        Ok(StripeRecordBatchReader {
            _stripe: stripe,
            arrow_schema,
            batch_ranges,
            field_readers,
        })
    }
}

impl StripeRecordBatchReader {
    /// Creates array readers for each field in the provided Arrow schema,
    /// using the given position ranges as a hint.
    ///
    /// Returns a vector of `ArrayReader` instances corresponding to the
    /// schema fields, or an error if any field cannot be resolved or a
    /// reader cannot be constructed.
    fn create_field_readers(
        stripe: &Stripe,
        arrow_schema: &ArrowSchema,
        pos_ranges_hint: impl Iterator<Item = Range<u64>> + Clone,
    ) -> Result<Vec<ArrayReader>, ArrowError> {
        let stripe_schema = stripe.fetch_schema().to_arrow_res()?;
        let field_list = stripe_schema.field_list().to_arrow_res()?;

        let mut readers = Vec::<ArrayReader>::with_capacity(arrow_schema.fields().len());
        for field in arrow_schema.fields() {
            let amudai_type = resolve_field(stripe, &field_list, field)?;
            let reader = create_array_reader(
                stripe,
                None,
                amudai_type,
                field.data_type().clone(),
                pos_ranges_hint.clone(),
            )?;
            readers.push(reader);
        }

        Ok(readers)
    }
}

impl Iterator for StripeRecordBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next_range = self.batch_ranges.next()?;
        let row_count = (next_range.end - next_range.start) as usize;
        let mut arrays = Vec::<ArrayRef>::with_capacity(self.field_readers.len());
        for reader in &mut self.field_readers {
            let array = try_or_ret_some_err!(reader.read(next_range.clone()));
            arrays.push(array);
        }
        let batch = try_or_ret_some_err!(RecordBatch::try_new_with_options(
            self.arrow_schema.clone(),
            arrays,
            &RecordBatchOptions::new()
                .with_match_field_names(false)
                .with_row_count(Some(row_count))
        ));
        Some(Ok(batch))
    }
}

/// Constructs an `ArrayReader` for a specific field in the Arrow conversion pipeline.
///
/// This function serves as a factory for creating appropriate array readers that can convert
/// Amudai field data into Arrow-compatible arrays. It handles the mapping between Amudai's
/// internal data representation and Arrow's type system, creating readers optimized for
/// the specific data types involved.
///
/// The function is a core component of the stripe reading process, where each Arrow schema
/// field needs a corresponding reader to extract and convert data from the underlying
/// Amudai stripe storage format.
///
/// # Arguments
///
/// * `stripe` - The Amudai stripe containing the field data to be read
/// * `parent_stripe_field` - Optional parent field for nested field readers.
///   The parent field context is requried in order to properly construct the readers
///   for missing (null) child fields, where the child position count is determined
///   by the parent's position count and data type.
/// * `amudai_type` - The Amudai data type definition for the field. If `None`, the field
///   is considered missing and will result in a null reader (not yet implemented)
/// * `arrow_type` - The target Arrow data type that the reader should produce
/// * `pos_ranges_hint` - An iterator over position ranges that provides hints about which
///   data ranges will be read. This allows the reader to optimize its internal structure
///   and pre-allocate resources appropriately
///
/// # Returns
///
/// Returns a configured `ArrayReader` that can convert data from the Amudai field format
/// to the specified Arrow array type.
///
/// # Errors
///
/// * Returns conversion errors if the stripe field cannot be opened from the Amudai type
/// * May return errors if there are incompatibilities between the Amudai and Arrow types
pub(crate) fn create_array_reader(
    stripe: &Stripe,
    _parent_stripe_field: Option<&amudai_shard::read::field::Field>,
    amudai_type: Option<amudai_format::schema::DataType>,
    arrow_type: arrow::datatypes::DataType,
    pos_ranges_hint: impl Iterator<Item = Range<u64>> + Clone,
) -> Result<ArrayReader, ArrowError> {
    let amudai_type = match amudai_type {
        Some(t) => t,
        None => {
            return Err(ArrowError::NotYetImplemented(
                "nulls for missing fields".to_string(),
            ));
        }
    };

    let stripe_field = stripe.open_field(amudai_type).to_arrow_res()?;
    ArrayReader::new(arrow_type, stripe_field, pos_ranges_hint)
}

/// Resolves the Amudai data type for a given Arrow field by performing field name lookup.
///
/// This function is a critical component of the schema mapping process between Arrow and Amudai
/// formats. It searches for a field by name in the provided Amudai field list and returns the
/// corresponding Amudai data type if found. This enables the Arrow-to-Amudai conversion pipeline
/// to understand how to read and convert data from Amudai's internal storage format.
///
/// The function performs field resolution at different levels of the schema hierarchy:
/// - **Top-level (stripe) resolution**: Used when resolving fields directly in the stripe schema
/// - **Nested resolution**: Used when resolving fields within complex types like structs
///
/// Field compatibility checking is planned but not yet implemented (see TODO comment in code).
///
/// # Arguments
///
/// * `stripe` - The Amudai stripe context.
/// * `field_list` - The Amudai field list to search within. This can be either a top-level
///   stripe field list or a nested field list from within a complex type (e.g., struct fields)
/// * `arrow_field` - The Arrow field definition containing the field name to search for and
///   the target Arrow data type. The field name is used for lookup, while the data type
///   is used for compatibility validation
///
/// # Returns
///
/// * `Ok(Some(data_type))` - Field was found by name in the field list. The returned
///   `DataType` is the Amudai type definition that can be used to create appropriate
///   field readers and decoders
/// * `Ok(None)` - Field was not found in the field list, indicating either a missing
///   field or a schema mismatch between Arrow and Amudai representations
/// * `Err(ArrowError)` - An error occurred during field lookup, typically due to
///   internal field list access issues
///
/// # Future Enhancements
///
/// The function includes a TODO for implementing compatibility checking between Arrow
/// and Amudai data types. This should validate that the found Amudai field type can be
/// properly converted to the requested Arrow type, returning `None` for incompatible
/// type combinations.
pub(crate) fn resolve_field(
    _stripe: &Stripe,
    field_list: &FieldList,
    arrow_field: &ArrowField,
) -> Result<Option<amudai_format::schema::DataType>, ArrowError> {
    let Some(data_type) = field_list
        .find(arrow_field.name())
        .to_arrow_res()?
        .map(|(_, data_type)| data_type)
    else {
        return Ok(None);
    };
    // TODO: check whether data_type is "compatible" with the requested Arrow type.
    // Otherwise, return None.
    Ok(Some(data_type))
}

#[cfg(test)]
mod tests {
    use crate::{builder::ArrowReaderBuilder, shard_reader::ShardRecordBatchReader};

    use amudai_collections::range_list::RangeList;
    use amudai_format::defs::common::DataRef;
    use amudai_shard::tests::{
        data_generator::{
            create_bytes_flat_test_schema, create_nested_test_schema,
            create_primitive_flat_test_schema,
        },
        shard_store::ShardStore,
    };
    use amudai_testkit::data_gen::{create_qlog_batch_iterator, load_qlog_schema};
    use arrow::array::RecordBatchIterator;

    #[test]
    fn test_basic_arrow_reader_primitives() {
        let schema = create_primitive_flat_test_schema();

        let shard_store = ShardStore::new();

        let shard_ref: DataRef = shard_store.ingest_shard_with_schema(&schema, 10000);

        let reader_builder = ArrowReaderBuilder::try_new(&shard_ref.url)
            .expect("Failed to create ArrowReaderBuilder")
            .with_object_store(shard_store.object_store.clone())
            .with_batch_size(8);

        let mut reader = reader_builder.build().unwrap();
        let frame = reader.next().unwrap().unwrap();
        assert_eq!(frame.num_rows(), 8);
        reader.collect::<Result<Vec<_>, _>>().unwrap();
    }

    #[test]
    fn test_basic_arrow_reader_bytes() {
        let schema = create_bytes_flat_test_schema();

        let shard_store = ShardStore::new();

        let shard_ref: DataRef = shard_store.ingest_shard_with_schema(&schema, 10000);

        let reader_builder = ArrowReaderBuilder::try_new(&shard_ref.url)
            .expect("Failed to create ArrowReaderBuilder")
            .with_object_store(shard_store.object_store.clone())
            .with_batch_size(8);

        let mut reader = reader_builder.build().unwrap();
        let frame = reader.next().unwrap().unwrap();
        assert_eq!(frame.num_rows(), 8);
        dbg!(&frame);
        reader.collect::<Result<Vec<_>, _>>().unwrap();
    }

    #[test]
    fn test_nested_schema_reader() {
        let schema = create_nested_test_schema();

        let shard_store = ShardStore::new();

        let shard_ref: DataRef = shard_store.ingest_shard_with_schema(&schema, 10000);

        let reader_builder = ArrowReaderBuilder::try_new(&shard_ref.url)
            .expect("Failed to create ArrowReaderBuilder")
            .with_object_store(shard_store.object_store.clone());

        let mut reader = reader_builder.build().unwrap();
        while let Some(frame) = reader.next() {
            let frame = frame.unwrap();
            dbg!(frame.num_rows());
        }
    }

    fn ingest_qlog_sample(record_count: usize) -> (ShardStore, DataRef) {
        // Generate 10K qlog records with nested schema
        let reader = create_qlog_batch_iterator(record_count, 1024)
            .expect("Failed to create qlog batch iterator");

        let schema = reader.schema();

        // Convert to RecordBatchIterator for ingestion
        let batch_iterator = RecordBatchIterator::new(reader, schema.clone());

        // Create shard store and ingest the data
        let shard_store = ShardStore::new();
        let shard_ref = shard_store.ingest_shard_from_record_batches(batch_iterator);
        (shard_store, shard_ref)
    }

    fn consume_all_records(reader: ShardRecordBatchReader, expected_count: usize) {
        let mut count = 0;
        for frame in reader {
            let frame = frame.unwrap();
            count += frame.num_rows();
        }
        assert_eq!(count, expected_count);
    }

    fn read_and_verify(reader: ShardRecordBatchReader) {
        let frame = reader.into_iter().next().unwrap().unwrap();
        let buf = Vec::new();
        let mut writer = arrow_json::LineDelimitedWriter::new(buf);
        writer.write_batches(&[&frame]).unwrap();
        writer.finish().unwrap();
        let output = String::from_utf8(writer.into_inner()).unwrap();
        assert!(output.contains("qeads-log-e193b243-7290-418c-bfb7-8a850e2fdad3"));
    }

    #[test]
    fn test_complex_nested_schema_reader() {
        let (shard_store, shard_ref) = ingest_qlog_sample(10000);

        let reader_builder = ArrowReaderBuilder::try_new(&shard_ref.url)
            .expect("Failed to create ArrowReaderBuilder")
            .with_object_store(shard_store.object_store.clone())
            .with_batch_size(500);

        let fetched_schema = reader_builder
            .fetch_arrow_schema()
            .expect("Failed to fetch arrow schema");
        assert_eq!(
            fetched_schema.fields().len(),
            load_qlog_schema().unwrap().fields().len()
        );

        let field_names: Vec<&str> = fetched_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert!(field_names.contains(&"recordId"));

        consume_all_records(reader_builder.build().unwrap(), 10000);

        let reader_builder = reader_builder.with_position_ranges(RangeList::from(vec![0u64..2]));
        read_and_verify(reader_builder.build().unwrap());
    }
}
