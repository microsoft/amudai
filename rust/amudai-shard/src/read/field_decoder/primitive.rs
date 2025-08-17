//! Decoders for basic primitive type fields.

use std::ops::Range;

use amudai_blockstream::read::{
    block_stream::{BlockReaderPrefetch, DecodedBlock},
    primitive_buffer::{PrimitiveBufferDecoder, PrimitiveBufferReader},
};
use amudai_common::{Result, error::Error};
use amudai_format::{
    defs::{common::AnyValue, shard},
    schema::{BasicType, BasicTypeDescriptor},
};
use amudai_ranges::PositionSeries;
use amudai_sequence::sequence::ValueSequence;

use crate::{
    read::{field_context::FieldContext, field_decoder::FieldCursor},
    write::field_encoder::EncodedField,
};

use super::{ConstantFieldReader, FieldReader};

/// Decodes primitive typed fields from encoded buffer data in a stripe.
///
/// Handles numeric types like integers, floating point numbers, and other primitive
/// data that has fixed-size representations. This decoder accesses encoded primitive values
/// from block-based storage and provides methods to create readers for efficient access
/// to ranges of logical values.
#[derive(Clone)]
pub struct PrimitiveFieldDecoder {
    /// Decoder for the buffer containing the encoded primitive values
    value_buffer: Option<PrimitiveBufferDecoder>,
    /// If the field contains only constant values, this stores the constant
    constant_value: Option<AnyValue>,
    /// The basic type descriptor for this field
    basic_type: BasicTypeDescriptor,
    // TODO: add optional presence buffer decoder
    // (when nulls are not embedded in the value buffer)
}

impl PrimitiveFieldDecoder {
    /// Creates a new primitive field decoder from a primitive buffer decoder.
    ///
    /// # Arguments
    ///
    /// * `value_buffer` - Decoder for the buffer containing the encoded primitive values
    /// * `basic_type` - The basic type descriptor for the values in this field
    pub fn new(value_buffer: PrimitiveBufferDecoder) -> PrimitiveFieldDecoder {
        let basic_type = value_buffer.basic_type();
        PrimitiveFieldDecoder {
            value_buffer: Some(value_buffer),
            constant_value: None,
            basic_type,
        }
    }

    /// Creates a new primitive field decoder for a constant value.
    ///
    /// # Arguments
    ///
    /// * `constant_value` - The constant value for all positions in this field
    /// * `basic_type` - The basic type descriptor for the values in this field
    pub fn new_constant(
        constant_value: AnyValue,
        basic_type: BasicTypeDescriptor,
    ) -> PrimitiveFieldDecoder {
        PrimitiveFieldDecoder {
            value_buffer: None,
            constant_value: Some(constant_value),
            basic_type,
        }
    }

    /// Creates a primitive field decoder from a field context.
    ///
    /// Extracts the necessary encoding information from the field's descriptor,
    /// loads the appropriate buffer reference, and initializes a decoder for that buffer.
    ///
    /// # Arguments
    ///
    /// * `field` - Field context containing metadata and references to encoded data
    ///
    /// # Returns
    ///
    /// A result containing the initialized decoder or an error if the field
    /// cannot be decoded as a primitive field
    pub(crate) fn from_field(field: &FieldContext) -> Result<PrimitiveFieldDecoder> {
        let basic_type = field.data_type().describe()?;

        // Check if this is a constant field first
        if let Some(constant_value) = field.try_get_constant() {
            return Ok(PrimitiveFieldDecoder::new_constant(
                constant_value,
                basic_type,
            ));
        }

        let data_buffer = field.get_encoded_buffer(shard::BufferKind::Data)?;

        let reader = field.open_artifact(
            data_buffer
                .buffer
                .as_ref()
                .ok_or_else(|| Error::invalid_format("Missing buffer reference"))?,
        )?;

        let value_buffer = PrimitiveBufferDecoder::from_encoded_buffer(
            reader.into_inner(),
            data_buffer,
            basic_type,
        )?;

        Ok(PrimitiveFieldDecoder::new(value_buffer))
    }

    /// Creates a primitive field decoder from an encoded field.
    ///
    /// This method creates a decoder from a transient `EncodedField` that contains
    /// prepared encoded buffers ready for reading. Unlike `from_field`, this method
    /// works with encoded data that hasn't been written to permanent storage yet.
    ///
    /// # Arguments
    ///
    /// * `field` - The encoded field containing prepared buffers with primitive data
    /// * `basic_type` - The basic type descriptor describing the primitive data type
    pub(crate) fn from_encoded_field(
        field: &EncodedField,
        basic_type: BasicTypeDescriptor,
    ) -> Result<PrimitiveFieldDecoder> {
        // Check if this is a constant field first
        if let Some(constant_value) = &field.constant_value {
            return Ok(PrimitiveFieldDecoder::new_constant(
                constant_value.clone(),
                basic_type,
            ));
        }

        let prepared_buffer = field.get_encoded_buffer(shard::BufferKind::Data)?;
        let value_buffer =
            PrimitiveBufferDecoder::from_prepared_buffer(prepared_buffer, basic_type)?;
        Ok(PrimitiveFieldDecoder::new(value_buffer))
    }

    /// Returns the basic type descriptor for the values in this field.
    pub fn basic_type(&self) -> BasicTypeDescriptor {
        self.basic_type
    }

    /// Creates a reader for efficiently accessing specific positions in this field.
    ///
    /// # Arguments
    ///
    /// * `positions_hint` - An iterator of non-descending logical positions that are
    ///   likely to be accessed. These hints are used to optimize prefetching strategies
    ///   for better performance when reading from storage. The positions must be
    ///   in non-descending order but don't need to be unique or contiguous.
    ///
    /// # Returns
    ///
    /// A boxed field reader for accessing the primitive values at the specified positions.
    pub fn create_reader(
        &self,
        positions_hint: impl PositionSeries<u64>,
    ) -> Result<Box<dyn FieldReader>> {
        if let Some(constant_value) = &self.constant_value {
            return Ok(Box::new(ConstantFieldReader::new(
                constant_value.clone(),
                self.basic_type,
            )));
        }

        let buffer_reader = self
            .value_buffer
            .as_ref()
            .expect("value_buffer")
            .create_reader(positions_hint, BlockReaderPrefetch::Enabled)?;
        Ok(Box::new(PrimitiveFieldReader::new(buffer_reader)))
    }
}

/// A cursor for iterator-style access to primitive values in a stripe field.
///
/// `PrimitiveFieldCursor<T>` provides optimized access to primitive values through a
/// forward-moving access pattern with sparse position requests. This approach is both
/// more convenient and more performant than using `FieldReader` to read entire position
/// ranges when the access pattern involves reading individual values at specific
/// positions.
///
/// The cursor maintains an internal cache of the current data block and only fetches
/// new blocks when the requested position falls outside the cached block's range,
/// minimizing I/O overhead for sequential or nearby access patterns.
///
/// This cursor is best created through [`PrimitiveFieldDecoder::create_reader`], providing
/// a hint of the positions that are likely to be accessed for optimal prefetching.
///
/// # Type Parameter
///
/// * `T` - The primitive type of the values in the field. Must implement `bytemuck::AnyBitPattern`
///   for safe casting from raw bytes. The size of `T` must match the field's primitive size.
///
/// # Supported Types
///
/// `PrimitiveFieldCursor` supports all fixed-size primitive types:
/// - Integer types: `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `u64`
/// - Floating-point types: `f32`, `f64`
/// - Other primitive types like GUIDs and fixed-size binary data
pub struct PrimitiveFieldCursor<T> {
    block: DecodedBlock,
    reader: Box<dyn FieldReader>,
    basic_type: BasicTypeDescriptor,
    _p: std::marker::PhantomData<T>,
}

impl<T> PrimitiveFieldCursor<T> {
    /// Creates a new primitive field cursor with the specified reader and type descriptor.
    ///
    /// # Arguments
    ///
    /// * `reader` - A boxed field reader that provides access to the underlying data blocks
    /// * `basic_type` - The basic type descriptor defining the primitive field structure
    ///
    /// # Returns
    ///
    /// A new `PrimitiveFieldCursor<T>` ready to read typed data from the field.
    ///
    /// # Panics
    ///
    /// Panics if the field's primitive size doesn't match the size of type `T`.
    pub fn new(
        reader: Box<dyn FieldReader>,
        basic_type: BasicTypeDescriptor,
    ) -> PrimitiveFieldCursor<T> {
        let value_size = if basic_type.basic_type != BasicType::Boolean {
            basic_type.primitive_size().expect("fixed primitive size")
        } else {
            1
        };
        assert_eq!(value_size, std::mem::size_of::<T>());
        PrimitiveFieldCursor {
            block: DecodedBlock::empty(),
            reader,
            basic_type,
            _p: Default::default(),
        }
    }

    /// Returns the basic type descriptor for this field.
    ///
    /// # Returns
    ///
    /// A copy of the `BasicTypeDescriptor` that defines the structure of this field.
    pub fn basic_type(&self) -> BasicTypeDescriptor {
        self.basic_type
    }

    /// Checks if the value at the specified position is null.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position to check for null
    ///
    /// # Returns
    ///
    /// A `Result` containing `true` if the value is null, `false` otherwise,
    /// or an error if I/O fails.
    #[inline]
    pub fn is_null(&mut self, position: u64) -> Result<bool> {
        self.establish_block(position)?;
        let index = self.position_index(position);
        Ok(self.block.values.presence.is_null(index))
    }

    /// Ensures the current block contains the specified position.
    ///
    /// If the current cached block doesn't contain the position, fetches
    /// the appropriate block from the reader.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position that must be contained in the current block
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or an I/O error if block fetching fails.
    #[inline]
    fn establish_block(&mut self, position: u64) -> Result<()> {
        if !self.block.descriptor.contains(position) {
            self.fetch_block(position)
        } else {
            Ok(())
        }
    }

    /// Converts a logical position to a block-relative index.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position within the stripe field
    ///
    /// # Returns
    ///
    /// The zero-based index within the current block corresponding to the logical position.
    ///
    /// # Panics
    ///
    /// This method assumes the position is within the current block's range. Call
    /// [`establish_block`] first to ensure this precondition.
    ///
    /// [`establish_block`]: Self::establish_block
    #[inline]
    fn position_index(&self, position: u64) -> usize {
        self.block.descriptor.position_index(position)
    }

    /// Fetches a new block containing the specified position.
    ///
    /// This method is marked as cold since it should be called infrequently
    /// when the cursor needs to move to a different block.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position that must be contained in the new block
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or an I/O error if block fetching fails.
    #[cold]
    fn fetch_block(&mut self, position: u64) -> Result<()> {
        self.block = self.reader.read_containing_block(position)?;
        Ok(())
    }
}

impl<T> PrimitiveFieldCursor<T>
where
    T: bytemuck::AnyBitPattern,
{
    /// Fetches the primitive value at the specified logical position within the stripe field.
    ///
    /// This method returns the raw primitive data at the given position, ignoring null status.
    /// If the value slot at this position is null, the returned data will be a default value
    /// (typically zero-filled memory of the appropriate type).
    ///
    /// Use [`fetch_nullable`] if you need to distinguish between actual data and null values.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position of the value to fetch within the stripe field
    ///
    /// # Returns
    ///
    /// A `Result` containing the primitive value of type `T` at the specified position,
    /// or an error if I/O fails or the position is invalid.
    ///
    /// # Performance
    ///
    /// This method is optimized for sequential or nearby access patterns. If the requested
    /// position is within the currently cached block, the access is very fast. Moving to
    /// a new block triggers I/O and is handled as a cold path.
    ///
    /// [`fetch_nullable`]: Self::fetch_nullable
    #[inline]
    pub fn fetch(&mut self, position: u64) -> Result<T> {
        self.establish_block(position)?;

        let index = self.position_index(position);
        Ok(self.block.values.as_slice::<T>()[index])
    }

    /// Fetches the primitive value at the specified position, returning `None` if null.
    ///
    /// This method handles nullable fields by checking the presence information
    /// before retrieving the typed primitive data. Returns `None` for null values
    /// and `Some(value)` for valid values.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position of the value to fetch
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Option<T>` where `None` indicates a null value
    /// and `Some` contains the primitive data, or an error if I/O fails.
    #[inline]
    pub fn fetch_nullable(&mut self, position: u64) -> Result<Option<T>> {
        self.establish_block(position)?;

        let index = self.position_index(position);
        let is_valid = self.block.values.presence.is_valid(index);
        let res = is_valid.then(|| self.block.values.as_slice::<T>()[index]);
        Ok(res)
    }
}

impl<T> FieldCursor for PrimitiveFieldCursor<T>
where
    T: bytemuck::AnyBitPattern + bytemuck::NoUninit + Send + Sync,
{
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync + 'static) {
        self
    }

    #[inline]
    fn basic_type(&self) -> BasicTypeDescriptor {
        self.basic_type
    }

    #[inline]
    fn move_to(&mut self, position: u64) -> Result<Range<u64>> {
        self.establish_block(position)?;
        Ok(self.cached_range())
    }

    #[inline]
    fn cached_range(&self) -> Range<u64> {
        self.block.descriptor.logical_range.clone()
    }

    #[inline]
    fn cached_is_null(&self, position: u64) -> bool {
        debug_assert!(self.cached_range().contains(&position));
        let index = self.position_index(position);
        self.block.values.presence.is_null(index)
    }

    #[inline]
    fn cached_value_as_bytes(&self, position: u64) -> &[u8] {
        debug_assert!(self.cached_range().contains(&position));
        let index = self.position_index(position);
        bytemuck::bytes_of(&self.block.values.as_slice::<T>()[index])
    }

    #[inline]
    fn cached_nullable_as_bytes(&self, position: u64) -> Option<&[u8]> {
        debug_assert!(self.cached_range().contains(&position));
        if self.cached_is_null(position) {
            None
        } else {
            Some(self.cached_value_as_bytes(position))
        }
    }

    #[inline]
    fn fetch_is_null(&mut self, position: u64) -> Result<bool> {
        self.establish_block(position)?;
        Ok(self.cached_is_null(position))
    }

    #[inline]
    fn fetch_value_as_bytes(&mut self, position: u64) -> Result<&[u8]> {
        self.establish_block(position)?;
        Ok(self.cached_value_as_bytes(position))
    }

    #[inline]
    fn fetch_nullable_as_bytes(&mut self, position: u64) -> Result<Option<&[u8]>> {
        self.establish_block(position)?;
        Ok(self.cached_nullable_as_bytes(position))
    }
}

pub(in crate::read::field_decoder) struct PrimitiveFieldReader {
    buffer_reader: PrimitiveBufferReader,
}

impl PrimitiveFieldReader {
    pub(in crate::read::field_decoder) fn new(
        buffer_reader: PrimitiveBufferReader,
    ) -> PrimitiveFieldReader {
        PrimitiveFieldReader { buffer_reader }
    }
}

impl FieldReader for PrimitiveFieldReader {
    fn read_range(&mut self, pos_range: Range<u64>) -> Result<ValueSequence> {
        self.buffer_reader.read_range(pos_range)
    }

    fn read_containing_block(&mut self, position: u64) -> Result<DecodedBlock> {
        self.buffer_reader.read_containing_block(position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use amudai_format::defs::common::{AnyValue, any_value::Kind};
    use amudai_objectstore::url::ObjectUrl;

    use crate::{
        read::shard::ShardOptions,
        tests::{data_generator::create_primitive_flat_test_schema, shard_store::ShardStore},
        write::field_encoder::{EncodedField, EncodedFieldStatistics},
    };

    #[test]
    fn test_constant_field_reader_i32() {
        let basic_type = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: amudai_format::defs::schema_ext::KnownExtendedType::None,
        };

        let constant_value = AnyValue {
            kind: Some(Kind::I64Value(42)),
            annotation: None,
        };

        let encoded_field = EncodedField {
            buffers: vec![], // No buffers needed for constant fields
            statistics: EncodedFieldStatistics::Missing,
            dictionary_size: None,
            constant_value: Some(constant_value),
        };

        let decoder =
            PrimitiveFieldDecoder::from_encoded_field(&encoded_field, basic_type).unwrap();
        let mut reader = decoder.create_reader(std::iter::once(0..5)).unwrap();

        let sequence = reader.read_range(0..5).unwrap();

        // Verify the sequence has the correct length and values
        assert_eq!(sequence.len(), 5);
        let values = sequence.values.as_slice::<i32>();
        assert_eq!(values, &[42i32; 5]);
    }

    #[test]
    fn test_constant_field_reader_null() {
        let basic_type = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: amudai_format::defs::schema_ext::KnownExtendedType::None,
        };

        let constant_value = AnyValue {
            kind: Some(Kind::NullValue(amudai_format::defs::common::UnitValue {})),
            annotation: None,
        };

        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: EncodedFieldStatistics::Missing,
            dictionary_size: None,
            constant_value: Some(constant_value),
        };

        let decoder =
            PrimitiveFieldDecoder::from_encoded_field(&encoded_field, basic_type).unwrap();
        let mut reader = decoder.create_reader(std::iter::once(0..3)).unwrap();

        let sequence = reader.read_range(0..3).unwrap();

        // Verify the sequence has the correct length and all nulls
        assert_eq!(sequence.len(), 3);
        assert_eq!(sequence.presence.count_nulls(), 3);
    }

    #[test]
    fn test_primitive_field_reader() {
        let shard_store = ShardStore::new();

        let shard_ref =
            shard_store.ingest_shard_with_schema(&create_primitive_flat_test_schema(), 20000);

        let shard = ShardOptions::new(shard_store.object_store.clone())
            .open(ObjectUrl::parse(&shard_ref.url).unwrap())
            .unwrap();
        assert_eq!(shard.directory().total_record_count, 20000);
        let schema = shard.fetch_schema().unwrap();
        let (_, schema_field) = schema.find_field("num_i32").unwrap().unwrap();
        let stripe = shard.open_stripe(0).unwrap();
        let field = stripe
            .open_field(schema_field.data_type().unwrap())
            .unwrap();
        let decoder = field.create_decoder().unwrap();

        fastrand::seed(987321546);
        let ranges = amudai_arrow_processing::array_sequence::sample_subranges(
            shard.directory().total_record_count as usize,
            1,
            2000,
            shard.directory().total_record_count as usize / 2,
        )
        .into_iter()
        .map(|r| r.start as u64..r.end as u64)
        .collect::<Vec<_>>();

        let mut reader = decoder.create_reader(ranges.iter().cloned()).unwrap();

        let mut null_count = 0usize;
        for range in ranges {
            let seq = reader.read_range(range.clone()).unwrap();
            assert_eq!(seq.len(), (range.end - range.start) as usize);
            null_count += seq.presence.count_nulls();
        }
        assert!(null_count > 500);
    }
}
