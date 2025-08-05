//! Frame reader for shard stripe data.
//!
//! This module provides [`FrameReader`], which reads a shard's stripe data
//! as a sequence of [`Frame`]s according to a provided set of record position ranges.
//! Each call to `next()` yields a `Frame` covering a contiguous chunk of records.
//!
//! FrameReader can be used on both sealed `Shard`/`Stripe` instances and
//! on transient `PreparedShard`/`PreparedStripe` instances.

use std::{ops::Range, sync::Arc};

use amudai_blockstream::read::block_stream::empty_hint;
use amudai_common::{Result, error::Error, verify_arg};
use amudai_format::{
    projection::{SchemaProjection, TypeProjectionRef},
    schema::{BasicType, DataType},
};
use amudai_ranges::{
    PositionSeries, RangeIteratorsExt, SharedRangeList,
    shared_range_list::SharedRangeListIntoIter,
    transform::{chunk::ChunkedRanges, shift::ShiftDownRanges},
};
use amudai_sequence::{frame::Frame, sequence_reader::SequenceReader};

use crate::read::{
    field_decoder::FieldDecoder,
    frame_reader::{
        fixed_list_reader::FixedListSequenceReader, list_reader::ListSequenceReader,
        map_reader::MapSequenceReader, struct_reader::StructSequenceReader,
        value_reader::ValueSequenceReader,
    },
};

mod fixed_list_reader;
mod list_reader;
mod map_reader;
mod struct_reader;
mod value_reader;

/// Type alias for the ranges of records within a stripe, represented as
/// chunked iterators over shifted, shared range lists. Each chunk
/// corresponds to a single frame's worth of records.
pub type StripeRecordRanges = ChunkedRanges<ShiftDownRanges<SharedRangeListIntoIter<u64>>>;

/// A reader that produces a sequence of [`Frame`]s by reading field data
/// according to a provided schema projection and record ranges.
///
/// Each call to `next()` returns a `Frame` containing the values for all
/// fields over a contiguous chunk of record positions. Internally, each
/// field is read by its own [`SequenceReader`].
pub struct FrameReader {
    schema: Arc<SchemaProjection>,
    record_ranges: StripeRecordRanges,
    field_readers: Vec<Box<dyn SequenceReader>>,
}

impl FrameReader {
    /// Creates a new `FrameReader`.
    ///
    /// # Arguments
    ///
    /// * `schema` – The projection of the schema defining which fields to read
    ///   and in what order.
    /// * `record_ranges` – An iterator over record position ranges within the
    ///   stripe, pre-chunked into frame-sized segments.
    /// * `create_decoder` – A factory function that, given a field's
    ///   [`DataType`], returns a [`FieldDecoder`] for that column.
    ///
    /// # Errors
    ///
    /// Returns an error if any field decoder or sequence reader fails to
    /// initialize.
    pub fn new<F>(
        schema: SchemaProjection,
        record_ranges: StripeRecordRanges,
        mut create_decoder: F,
    ) -> Result<FrameReader>
    where
        F: FnMut(DataType) -> Result<FieldDecoder>,
    {
        let field_readers = schema
            .fields()
            .iter()
            .map(|data_type| {
                Self::create_sequence_reader(
                    data_type.clone(),
                    record_ranges.clone(),
                    &mut create_decoder,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(FrameReader {
            schema: Arc::new(schema),
            record_ranges,
            field_readers,
        })
    }

    /// Translates shard-level record ranges into stripe-relative,
    /// chunked ranges suitable for frame creation.
    ///
    /// This will:
    /// 1. Clamp the input `shard_ranges` to the window of
    ///    `stripe_range_in_shard`.
    /// 2. Shift all start positions down by
    ///    `stripe_range_in_shard.start` so they begin at zero.
    /// 3. Split the resulting ranges into chunks of at most
    ///    `max_frame_size` records each.
    ///
    /// # Arguments
    ///
    /// * `shard_ranges` – The original record ranges at the shard level.
    /// * `stripe_range_in_shard` – The bounds of the stripe within the shard.
    /// * `max_frame_size` – The maximum number of records per frame chunk.
    pub fn translate_shard_ranges_to_stripe_ranges(
        shard_ranges: SharedRangeList<u64>,
        stripe_range_in_shard: Range<u64>,
        max_frame_size: u64,
    ) -> StripeRecordRanges {
        shard_ranges
            .clamp(stripe_range_in_shard.clone())
            .into_iter()
            .shift_down(stripe_range_in_shard.start)
            .chunk_ranges(max_frame_size)
    }
}

impl Iterator for FrameReader {
    type Item = Result<Frame>;

    /// Reads the next frame of data, yielding a [`Frame`] with all the
    /// fields for the next record‐range chunk.
    ///
    /// Returns `None` when there are no more record ranges to process.
    fn next(&mut self) -> Option<Result<Frame>> {
        let range = self.record_ranges.next()?;
        let range = range.start..range.end.max(range.start);
        let len = (range.end - range.start) as usize;

        let fields_res = self
            .field_readers
            .iter_mut()
            .map(|reader| reader.read_sequence(range.clone()))
            .collect::<Result<Vec<_>>>();

        let fields = match fields_res {
            Ok(fields) => fields,
            Err(e) => return Some(Err(e)),
        };

        Some(Ok(Frame::new(Some(self.schema.clone()), fields, len)))
    }
}

impl FrameReader {
    /// Creates an appropriate boxed `SequenceReader` for the given projected
    /// data type and record ranges by dispatching on its `BasicType`.
    ///
    /// # Arguments
    ///
    /// * `data_type` – The projection of the field’s data type to read.
    /// * `record_ranges` – The series of record position ranges to read.
    /// * `create_decoder` – Factory function that, given a `DataType`,
    ///   produces a `FieldDecoder`.
    ///
    /// # Errors
    ///
    /// Returns an error if no reader is implemented for the field’s type
    /// or if any underlying decoder fails to initialize.
    pub fn create_sequence_reader(
        data_type: TypeProjectionRef,
        record_ranges: impl PositionSeries<u64> + Clone,
        create_decoder: &mut impl FnMut(DataType) -> Result<FieldDecoder>,
    ) -> Result<Box<dyn SequenceReader>> {
        match data_type.basic_type() {
            BasicType::Boolean
            | BasicType::Int8
            | BasicType::Int16
            | BasicType::Int32
            | BasicType::Int64
            | BasicType::Float32
            | BasicType::Float64
            | BasicType::Binary
            | BasicType::FixedSizeBinary
            | BasicType::String
            | BasicType::Guid
            | BasicType::DateTime => {
                Self::create_value_sequence_reader(data_type, record_ranges, create_decoder)
            }
            BasicType::List => {
                Self::create_list_sequence_reader(data_type, record_ranges, create_decoder)
            }
            BasicType::FixedSizeList => {
                Self::create_fixed_list_sequence_reader(data_type, record_ranges, create_decoder)
            }
            BasicType::Map => {
                Self::create_map_sequence_reader(data_type, record_ranges, create_decoder)
            }
            BasicType::Struct => {
                Self::create_struct_sequence_reader(data_type, record_ranges, create_decoder)
            }
            BasicType::Union => todo!(),
            _ => Err(Error::not_implemented(format!(
                "create_sequence_reader: {}",
                data_type.data_type()
            ))),
        }
    }

    /// Creates a `ValueSequenceReader` for primitive and simple types.
    ///
    /// Uses the provided decoder to read value‐based fields
    /// (e.g. Int, Float, Binary, String, etc.) over the given `record_ranges`.
    fn create_value_sequence_reader(
        data_type: TypeProjectionRef,
        record_ranges: impl PositionSeries<u64> + Clone,
        create_decoder: &mut impl FnMut(DataType) -> Result<FieldDecoder>,
    ) -> Result<Box<dyn SequenceReader>> {
        let decoder = (create_decoder)(data_type.data_type().clone())?;
        let field_reader = decoder.create_reader(record_ranges)?;
        Ok(Box::new(ValueSequenceReader::new(data_type, field_reader)))
    }

    /// Creates a `StructSequenceReader` for struct types.
    ///
    /// Recursively builds child field readers for each member
    /// of the struct projection.
    fn create_struct_sequence_reader(
        data_type: TypeProjectionRef,
        record_ranges: impl PositionSeries<u64> + Clone,
        create_decoder: &mut impl FnMut(DataType) -> Result<FieldDecoder>,
    ) -> Result<Box<dyn SequenceReader>> {
        let struct_decoder = (create_decoder)(data_type.data_type().clone())?;
        let struct_reader = struct_decoder.create_reader(record_ranges.clone())?;
        let field_readers = data_type
            .children()
            .iter()
            .map(|field| {
                Self::create_sequence_reader(field.clone(), record_ranges.clone(), create_decoder)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Box::new(StructSequenceReader::new(
            data_type,
            struct_reader,
            field_readers,
        )))
    }

    /// Creates a `FixedListSequenceReader` for fixed‐size list types.
    ///
    /// The `list_size` is read from the child type’s schema,
    /// and an optional item reader is built if a child projection exists.
    fn create_fixed_list_sequence_reader(
        data_type: TypeProjectionRef,
        record_ranges: impl PositionSeries<u64> + Clone,
        create_decoder: &mut impl FnMut(DataType) -> Result<FieldDecoder>,
    ) -> Result<Box<dyn SequenceReader>> {
        let list_decoder = (create_decoder)(data_type.data_type().clone())?;
        let list_reader = list_decoder.create_reader(record_ranges)?;
        let list_size = data_type.data_type().child_at(0)?.describe()?.fixed_size as usize;
        verify_arg!(list_size, list_size != 0);
        let item_reader = if !data_type.children().is_empty() {
            verify_arg!(data_type, data_type.children().len() == 1);
            let child_type = data_type.children()[0].clone();
            // TODO: derive fixed list child range hint
            Some(Self::create_sequence_reader(
                child_type,
                empty_hint(),
                create_decoder,
            )?)
        } else {
            None
        };
        Ok(Box::new(FixedListSequenceReader::new(
            data_type,
            list_size,
            list_reader,
            item_reader,
        )))
    }

    /// Creates a `ListSequenceReader` for variable‐length list types.
    ///
    /// Builds an optional item reader for the element type
    /// if the projection includes a single child.
    fn create_list_sequence_reader(
        data_type: TypeProjectionRef,
        record_ranges: impl PositionSeries<u64> + Clone,
        create_decoder: &mut impl FnMut(DataType) -> Result<FieldDecoder>,
    ) -> Result<Box<dyn SequenceReader>> {
        let list_decoder = (create_decoder)(data_type.data_type().clone())?;
        let list_reader = list_decoder.create_reader(record_ranges)?;
        let item_reader = if !data_type.children().is_empty() {
            verify_arg!(data_type, data_type.children().len() == 1);
            // TODO: derive list child range hint
            Some(Self::create_sequence_reader(
                data_type.children()[0].clone(),
                empty_hint(),
                create_decoder,
            )?)
        } else {
            None
        };
        Ok(Box::new(ListSequenceReader::new(
            data_type,
            list_reader,
            item_reader,
        )))
    }

    /// Creates a `MapSequenceReader` for map types.
    ///
    /// Builds optional key and value readers according to the
    /// projected key and value child schemas.
    fn create_map_sequence_reader(
        data_type: TypeProjectionRef,
        record_ranges: impl PositionSeries<u64> + Clone,
        create_decoder: &mut impl FnMut(DataType) -> Result<FieldDecoder>,
    ) -> Result<Box<dyn SequenceReader>> {
        let map_decoder = (create_decoder)(data_type.data_type().clone())?;
        let map_reader = map_decoder.create_reader(record_ranges)?;

        let key_schema_id = data_type.data_type().child_at(0)?.schema_id()?;
        let value_schema_id = data_type.data_type().child_at(1)?.schema_id()?;

        let key_type = data_type
            .children()
            .iter()
            .find(|c| c.schema_id() == key_schema_id);
        let value_type = data_type
            .children()
            .iter()
            .find(|c| c.schema_id() == value_schema_id);

        // TODO: derive list child range hint
        let key_reader = key_type
            .map(|key_type| {
                Self::create_sequence_reader(key_type.clone(), empty_hint(), create_decoder)
            })
            .transpose()?;

        let value_reader = value_type
            .map(|value_type| {
                Self::create_sequence_reader(value_type.clone(), empty_hint(), create_decoder)
            })
            .transpose()?;

        Ok(Box::new(MapSequenceReader::new(
            data_type,
            map_reader,
            key_reader,
            value_reader,
        )))
    }
}
