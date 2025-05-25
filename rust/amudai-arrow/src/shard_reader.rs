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
            let stripe = try_or_ret_some_err!(self
                .shard
                .open_stripe(self.next_stripe_ordinal)
                .to_arrow_res());
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
            let amudai_type = Self::resolve_field(stripe, &field_list, &field)?;
            let reader = Self::create_array_reader(
                stripe,
                amudai_type,
                field.data_type().clone(),
                pos_ranges_hint.clone(),
            )?;
            readers.push(reader);
        }

        Ok(readers)
    }

    /// Constructs an `ArrayReader` for a specific field, given its Amudai and Arrow
    /// data types and position ranges.
    ///
    /// Returns an error if the field is missing or if the reader cannot be created.
    fn create_array_reader(
        stripe: &Stripe,
        amudai_type: Option<amudai_format::schema::DataType>,
        arrow_type: arrow::datatypes::DataType,
        pos_ranges_hint: impl Iterator<Item = Range<u64>> + Clone,
    ) -> Result<ArrayReader, ArrowError> {
        let amudai_type = match amudai_type {
            Some(t) => t,
            None => {
                return Err(ArrowError::NotYetImplemented(
                    "nulls for missing fields".to_string(),
                ))
            }
        };

        let stripe_field = stripe.open_field(amudai_type).to_arrow_res()?;
        ArrayReader::new(arrow_type, stripe_field, pos_ranges_hint)
    }

    /// Resolves the Amudai data type for a given Arrow field by consulting
    /// the stripe's field list.
    ///
    /// Returns `Ok(Some(data_type))` if the field is found and compatible,
    /// `Ok(None)` if not found or incompatible, or an error if resolution
    /// fails.
    fn resolve_field(
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

#[cfg(test)]
mod tests {
    use crate::builder::ArrowReaderBuilder;

    use amudai_format::defs::common::DataRef;
    use amudai_shard::tests::{
        data_generator::{create_bytes_flat_test_schema, create_primitive_flat_test_schema},
        shard_store::ShardStore,
    };

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
}
