//! Boolean field decoder for reading boolean values and their presence information.
//!
//! This module provides a decoder for boolean fields that handles both the boolean
//! values themselves and their nullability information. Boolean fields are encoded
//! using two separate bit buffers:
//! - Values buffer: Contains the actual boolean values (true/false)
//! - Presence buffer: Contains nullability information (null/non-null)

use std::ops::Range;

use amudai_blockstream::read::{
    bit_buffer::{BitBufferDecoder, BitBufferReader},
    block_stream::{BlockReaderPrefetch, DecodedBlock},
};
use amudai_common::{Result, error::Error, verify_arg, verify_data};
use amudai_format::{
    defs::{
        common::any_value::Kind,
        shard::{self, BufferKind},
    },
    schema::{BasicType, BasicTypeDescriptor},
};
use amudai_ranges::PositionSeries;
use amudai_sequence::sequence::ValueSequence;
use arrow_array::BooleanArray;
use arrow_buffer::NullBuffer;

use crate::{read::field_context::FieldContext, write::field_encoder::EncodedField};

use super::FieldReader;

/// A decoder for boolean field types.
///
/// This decoder handles the reading of boolean values along with their presence
/// information.
/// Boolean fields store both the actual boolean values and nullability information
/// in separate bit buffers.
///
/// # Storage Format
///
/// Boolean fields use two bit buffers:
/// - **Values buffer**: Contains packed boolean values (true/false)
/// - **Presence buffer**: Contains nullability information (null/non-null)
///
/// For non-nullable fields, the presence buffer may be omitted, indicating all values
/// are valid.
#[derive(Clone)]
pub struct BooleanFieldDecoder {
    /// The basic type descriptor for the boolean field.
    basic_type: BasicTypeDescriptor,
    /// Decoder for the boolean values bit buffer.
    values_decoder: BitBufferDecoder,
    /// Decoder for the presence bit buffer, or a constant decoder for non-nullable fields.
    presence_decoder: BitBufferDecoder,
    /// Total number of logical positions in the field.
    positions: u64,
}

impl BooleanFieldDecoder {
    /// Creates a new `BooleanFieldDecoder` with the specified components.
    ///
    /// # Arguments
    ///
    /// * `basic_type` - The basic type descriptor for the boolean field
    /// * `values_decoder` - Decoder for the boolean values bit buffer
    /// * `presence_decoder` - Decoder for the presence bit buffer, or a constant decoder for non-nullable fields
    /// * `positions` - Total number of logical positions in the field
    ///
    /// # Returns
    ///
    /// A new `BooleanFieldDecoder` instance configured with the provided parameters.
    pub fn new(
        basic_type: BasicTypeDescriptor,
        values_decoder: BitBufferDecoder,
        presence_decoder: BitBufferDecoder,
        positions: u64,
    ) -> BooleanFieldDecoder {
        BooleanFieldDecoder {
            basic_type,
            values_decoder,
            presence_decoder,
            positions,
        }
    }

    /// Creates decoders for constant boolean values.
    ///
    /// This helper method handles the creation of appropriate bit buffer decoders
    /// for constant boolean values, including both non-null boolean values and null values.
    ///
    /// # Arguments
    ///
    /// * `constant_value` - The constant value to create decoders for
    ///
    /// # Returns
    ///
    /// A tuple of (values_decoder, presence_decoder) configured for the constant value.
    ///
    /// # Errors
    ///
    /// Returns an error if the constant value type is not valid for boolean fields.
    fn create_constant_decoders(
        constant_value: &amudai_format::defs::common::AnyValue,
    ) -> Result<(BitBufferDecoder, BitBufferDecoder)> {
        match &constant_value.kind {
            Some(Kind::BoolValue(bool_val)) => {
                // For constant boolean values, create constant decoders
                Ok((
                    BitBufferDecoder::from_constant(*bool_val),
                    BitBufferDecoder::from_constant(true), // Non-null
                ))
            }
            Some(Kind::NullValue(_)) => {
                // For constant null values, value doesn't matter but presence is false
                Ok((
                    BitBufferDecoder::from_constant(false), // Value doesn't matter for nulls
                    BitBufferDecoder::from_constant(false), // All null
                ))
            }
            _ => Err(Error::invalid_format(format!(
                "Invalid constant value type for boolean field: {:?}",
                constant_value.kind
            ))),
        }
    }

    /// Creates a `BooleanFieldDecoder` from a field context.
    ///
    /// This method parses the field's metadata and encoded buffers to construct
    /// a decoder suitable for reading boolean values and their presence information.
    /// It handles both nullable fields (with separate presence buffers) and non-nullable
    /// fields (without presence buffers).
    ///
    /// # Arguments
    ///
    /// * `field` - The field context containing type information and encoded buffers
    ///
    /// # Returns
    ///
    /// A new `BooleanFieldDecoder` configured for the specified field.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The field's basic type is not `Boolean`
    /// - The encoded buffers are malformed or contain unexpected buffer types
    /// - The buffer references cannot be opened
    pub(crate) fn from_field(field: &FieldContext) -> Result<BooleanFieldDecoder> {
        let basic_type = field.data_type().describe()?;
        verify_data!(basic_type, basic_type.basic_type == BasicType::Boolean);

        // Check if this is a constant field first
        if let Some(constant_value) = field.try_get_constant() {
            let (values_decoder, presence_decoder) =
                Self::create_constant_decoders(&constant_value)?;

            return Ok(BooleanFieldDecoder::new(
                basic_type,
                values_decoder,
                presence_decoder,
                field.position_count(),
            ));
        }

        let encoded_buffers = field.get_encoded_buffers()?;

        let mut values_decoder = None;
        let mut presence_decoder = None;

        // Parse the encoded buffers to identify values and presence buffers
        for encoded_buffer in encoded_buffers {
            match encoded_buffer.kind {
                kind if kind == BufferKind::Data as i32 => {
                    let reader = field.open_artifact(
                        encoded_buffer
                            .buffer
                            .as_ref()
                            .ok_or_else(|| Error::invalid_format("Missing buffer reference"))?,
                    )?;
                    values_decoder = Some(BitBufferDecoder::from_encoded_buffer(
                        reader.into_inner(),
                        encoded_buffer,
                    )?);
                }
                kind if kind == BufferKind::Presence as i32 => {
                    let reader = field.open_artifact(
                        encoded_buffer
                            .buffer
                            .as_ref()
                            .ok_or_else(|| Error::invalid_format("Missing buffer reference"))?,
                    )?;
                    presence_decoder = Some(BitBufferDecoder::from_encoded_buffer(
                        reader.into_inner(),
                        encoded_buffer,
                    )?);
                }
                _ => {
                    return Err(Error::invalid_format(format!(
                        "Unexpected buffer kind: {}",
                        encoded_buffer.kind
                    )));
                }
            }
        }

        // Values buffer is required
        let values_decoder = values_decoder
            .ok_or_else(|| Error::invalid_format("Missing values buffer for boolean field"))?;

        // Presence buffer is optional - if missing, all values are non-null
        let presence_decoder =
            presence_decoder.unwrap_or_else(|| BitBufferDecoder::from_constant(true));

        Ok(BooleanFieldDecoder::new(
            basic_type,
            values_decoder,
            presence_decoder,
            field.position_count(),
        ))
    }

    /// Creates a Boolean field decoder from an encoded field.
    ///
    /// This method creates a decoder from a transient `EncodedField` that contains
    /// prepared encoded buffers ready for reading. Unlike `from_field`, this method
    /// works with encoded data that hasn't been written to permanent storage yet.
    ///
    /// # Arguments
    ///
    /// * `field` - The encoded field containing prepared buffers with primitive data
    /// * `basic_type` - The basic type descriptor describing the primitive data type
    /// * `positions` - The number of logical positions (value slots) in the encoded
    ///   field
    pub(crate) fn from_encoded_field(
        field: &EncodedField,
        basic_type: BasicTypeDescriptor,
        positions: u64,
    ) -> Result<BooleanFieldDecoder> {
        verify_data!(basic_type, basic_type.basic_type == BasicType::Boolean);

        // Check if this field has a constant value
        if let Some(constant_value) = &field.constant_value {
            let (values_decoder, presence_decoder) =
                Self::create_constant_decoders(constant_value)?;

            return Ok(BooleanFieldDecoder::new(
                basic_type,
                values_decoder,
                presence_decoder,
                positions,
            ));
        }

        // Handle buffered values
        let values_buffer = field.get_encoded_buffer(shard::BufferKind::Data)?;
        let presence_buffer = field.get_encoded_buffer(shard::BufferKind::Presence).ok();

        let values = BitBufferDecoder::from_prepared_buffer(values_buffer)?;
        let presence = presence_buffer
            .map(BitBufferDecoder::from_prepared_buffer)
            .unwrap_or_else(|| Ok(BitBufferDecoder::from_constant(true)))?;
        Ok(BooleanFieldDecoder::new(
            basic_type, values, presence, positions,
        ))
    }

    /// Returns the basic type descriptor for this field.
    ///
    /// # Returns
    ///
    /// The basic type descriptor describing the boolean field's type structure.
    pub fn basic_type(&self) -> BasicTypeDescriptor {
        self.basic_type
    }

    /// Creates a field reader optimized for the specified position ranges.
    ///
    /// This method creates a reader that can efficiently access boolean values and their
    /// presence information for logical positions within the field. The position hints
    /// are used to optimize data prefetching and caching strategies.
    ///
    /// # Arguments
    ///
    /// * `positions_hint` - An iterator of logical position ranges that are likely
    ///   to be accessed. These hints help optimize prefetching strategies for better
    ///   performance when reading from storage.
    ///
    /// # Returns
    ///
    /// A boxed field reader that can read boolean values and presence information for the
    /// specified ranges. This reader produces [`ValueSequence`] instances where the `values`
    /// field is a byte buffer with `0` or `1` valued bytes, with each byte corresponding to
    /// a single boolean value. The reader automatically applies optimizations based on the
    /// underlying BitBufferReader representations.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying bit buffer decoders fail to create readers.
    pub fn create_reader(
        &self,
        positions_hint: impl PositionSeries<u64> + Clone,
    ) -> Result<Box<dyn FieldReader>> {
        self.create_boolean_reader(positions_hint)
            .map(|reader| Box::new(reader) as _)
    }

    /// Creates a concrete boolean field reader optimized for the specified positions hint.
    ///
    /// Unlike [`create_reader`](Self::create_reader) which returns a boxed trait object,
    /// this method returns the concrete `BooleanFieldReader` type.
    /// This enables access to boolean-specific functionality such as
    /// [`read_array`](BooleanFieldReader::read_array) which returns Arrow `BooleanArray`
    /// instances directly.
    ///
    /// # Arguments
    ///
    /// * `positions_hint` - An iterator of logical positions or ranges that are likely
    ///   to be accessed. These hints help optimize prefetching strategies for better
    ///   performance when reading from storage.
    ///
    /// # Returns
    ///
    /// A concrete `BooleanFieldReader` that provides both the standard `FieldReader`
    /// interface and boolean-specific methods for enhanced functionality.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying bit buffer decoders fail to create readers.
    pub fn create_boolean_reader(
        &self,
        positions_hint: impl PositionSeries<u64> + Clone,
    ) -> Result<BooleanFieldReader> {
        let values_reader = self
            .values_decoder
            .create_reader(positions_hint.clone(), BlockReaderPrefetch::Enabled)?;
        let presence_reader = self
            .presence_decoder
            .create_reader(positions_hint, BlockReaderPrefetch::Enabled)?;

        Ok(BooleanFieldReader {
            values_reader,
            presence_reader,
            basic_type: self.basic_type,
            positions: self.positions,
        })
    }
}

/// A field reader for boolean fields with both values and presence buffers.
///
/// This reader handles boolean fields that store both boolean values and presence
/// information in separate bit buffers. It reads from both buffers and combines
/// them into a value sequence. The reader automatically applies optimizations based
/// on the underlying BitBufferReader representations.
pub struct BooleanFieldReader {
    values_reader: BitBufferReader,
    presence_reader: BitBufferReader,
    basic_type: BasicTypeDescriptor,
    positions: u64,
}

impl BooleanFieldReader {
    /// Reads boolean values from the specified position range and returns them
    /// as an Arrow `BooleanArray`.
    ///
    /// This method reads both boolean values and their nullability information from the
    /// underlying bit buffers, combining them into a standard Arrow `BooleanArray`.
    /// It automatically optimizes for cases where all values are non-null.
    ///
    /// # Arguments
    ///
    /// * `pos_range` - The range of logical positions to read boolean data for.
    ///   Must not exceed the total number of positions in the field.
    ///
    /// # Returns
    ///
    /// A `Result<BooleanArray>` containing the boolean values and nullability information
    /// for the specified position range.
    pub fn read_array(&mut self, pos_range: Range<u64>) -> Result<BooleanArray> {
        verify_arg!(pos_range, pos_range.end <= self.positions);
        let nulls: Option<NullBuffer>;
        if let Some(true) = self.presence_reader.try_get_constant() {
            nulls = None;
        } else {
            nulls = Some(NullBuffer::new(
                self.presence_reader
                    .read_range_as_arrow_boolean(pos_range.clone())?,
            ));
        }
        let values = self.values_reader.read_range_as_arrow_boolean(pos_range)?;
        Ok(BooleanArray::new(values, nulls))
    }
}

impl FieldReader for BooleanFieldReader {
    /// Reads boolean values and presence information for the specified logical position range.
    ///
    /// This method automatically applies optimizations based on the underlying BitBufferReader
    /// representations.
    ///
    /// # Arguments
    ///
    /// * `pos_range` - The range of logical positions to read boolean data for
    ///
    /// # Returns
    ///
    /// A `ValueSequence` containing boolean values and presence information.
    fn read_range(&mut self, pos_range: Range<u64>) -> Result<ValueSequence> {
        verify_arg!(pos_range, pos_range.end <= self.positions);
        let mut values = self.values_reader.read_range(pos_range.clone())?;
        values.presence = self.presence_reader.read_range_as_presence(pos_range)?;
        values.type_desc = self.basic_type;
        Ok(values)
    }

    fn read_containing_block(&mut self, position: u64) -> Result<DecodedBlock> {
        let mut block = self.values_reader.read_containing_block(position)?;
        let presence = self
            .presence_reader
            .read_range_as_presence(block.descriptor.logical_range.clone())?;
        block.values.presence = presence;
        Ok(block)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;

    use crate::{
        read::{field_decoder::FieldDecoder, shard::ShardOptions},
        tests::{data_generator::create_boolean_test_schema, shard_store::ShardStore},
        write::field_encoder::EncodedFieldStatistics,
    };

    #[test]
    fn test_boolean_field_reader() {
        let shard_store = ShardStore::new();

        let shard_ref = shard_store.ingest_shard_with_schema(&create_boolean_test_schema(), 20000);

        let shard = ShardOptions::new(shard_store.object_store.clone())
            .open(shard_ref.url.as_str())
            .unwrap();
        assert_eq!(shard.directory().total_record_count, 20000);
        let schema = shard.fetch_schema().unwrap();
        let (_, schema_field) = schema.find_field("x").unwrap().unwrap();

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
        for range in ranges.iter().cloned() {
            let seq = reader.read_range(range.clone()).unwrap();
            assert!(seq.values.as_bytes().iter().all(|&b| b == 0 || b == 1));
            assert_eq!(seq.len(), (range.end - range.start) as usize);
            null_count += seq.presence.count_nulls();
        }
        assert!(null_count > 500);

        let mut reader = match decoder {
            FieldDecoder::Boolean(decoder) => decoder
                .create_boolean_reader(ranges.iter().cloned())
                .unwrap(),
            _ => panic!("unexpected ecoder"),
        };

        let mut true_count = 0usize;
        let mut null_count = 0usize;
        for range in ranges.iter().cloned() {
            let arr = reader.read_array(range.clone()).unwrap();
            assert_eq!(arr.len(), (range.end - range.start) as usize);
            true_count += arr.true_count();
            null_count += arr.null_count();
        }
        assert!(null_count > 500);
        assert!(true_count > 2000);
    }

    #[test]
    fn test_boolean_constant_decoder() {
        use crate::write::field_encoder::EncodedField;
        use amudai_format::{
            defs::common::{AnyValue, any_value::Kind},
            schema::{BasicType, BasicTypeDescriptor},
        };

        // Test constant true value
        let constant_value = AnyValue {
            annotation: None,
            kind: Some(Kind::BoolValue(true)),
        };

        let mut encoded_field =
            EncodedField::new(Vec::new(), EncodedFieldStatistics::Missing, None);
        encoded_field.constant_value = Some(constant_value);

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Boolean,
            fixed_size: 0,
            signed: false,
            extended_type: Default::default(),
        };
        let decoder =
            BooleanFieldDecoder::from_encoded_field(&encoded_field, basic_type, 100).unwrap();

        let mut reader = decoder
            .create_boolean_reader(std::iter::empty::<u64>())
            .unwrap();

        // Test reading a range
        let array = reader.read_array(0..10).unwrap();
        assert_eq!(array.len(), 10);
        assert_eq!(array.true_count(), 10);
        assert_eq!(array.null_count(), 0);

        // Test using the generic FieldReader interface
        let mut reader = decoder.create_reader(std::iter::empty::<u64>()).unwrap();
        let sequence = reader.read_range(5..15).unwrap();
        assert_eq!(sequence.len(), 10);
        assert!(sequence.presence.is_trivial_non_null());
        assert!(sequence.values.as_bytes().iter().all(|&b| b == 1));

        // Test constant false value
        let constant_value = AnyValue {
            annotation: None,
            kind: Some(Kind::BoolValue(false)),
        };

        let mut encoded_field =
            EncodedField::new(Vec::new(), EncodedFieldStatistics::Missing, None);
        encoded_field.constant_value = Some(constant_value);

        let decoder =
            BooleanFieldDecoder::from_encoded_field(&encoded_field, basic_type, 50).unwrap();
        let mut reader = decoder
            .create_boolean_reader(std::iter::empty::<u64>())
            .unwrap();

        let array = reader.read_array(0..5).unwrap();
        assert_eq!(array.len(), 5);
        assert_eq!(array.true_count(), 0);
        assert_eq!(array.null_count(), 0);
    }

    #[test]
    fn test_boolean_null_constant_decoder() {
        use crate::write::field_encoder::EncodedField;
        use amudai_format::{
            defs::common::{AnyValue, UnitValue, any_value::Kind},
            schema::{BasicType, BasicTypeDescriptor},
        };

        // Test constant null value
        let constant_value = AnyValue {
            annotation: None,
            kind: Some(Kind::NullValue(UnitValue {})),
        };

        let mut encoded_field =
            EncodedField::new(Vec::new(), EncodedFieldStatistics::Missing, None);
        encoded_field.constant_value = Some(constant_value);

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Boolean,
            fixed_size: 0,
            signed: false,
            extended_type: Default::default(),
        };
        let decoder =
            BooleanFieldDecoder::from_encoded_field(&encoded_field, basic_type, 100).unwrap();

        let mut reader = decoder
            .create_boolean_reader(std::iter::empty::<u64>())
            .unwrap();

        // Test reading a range
        let array = reader.read_array(0..10).unwrap();
        assert_eq!(array.len(), 10);
        assert_eq!(array.true_count(), 0);
        assert_eq!(array.null_count(), 10);

        // Test using the generic FieldReader interface
        let mut reader = decoder.create_reader(std::iter::empty::<u64>()).unwrap();
        let sequence = reader.read_range(5..15).unwrap();
        assert_eq!(sequence.len(), 10);
        assert!(sequence.presence.is_trivial_all_null());
        assert!(sequence.values.as_bytes().iter().all(|&b| b == 0));
    }
}
