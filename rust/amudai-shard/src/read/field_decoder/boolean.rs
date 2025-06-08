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
    block_stream::BlockReaderPrefetch,
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::{error::Error, verify_arg, verify_data, Result};
use amudai_format::{
    defs::shard::BufferKind,
    schema::{BasicType, BasicTypeDescriptor},
};
use amudai_sequence::{presence::Presence, sequence::ValueSequence, values::Values};

use crate::read::field_context::FieldContext;

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

        let encoded_buffers = field.get_encoded_buffers()?;

        let mut values_decoder = None;
        let mut presence_decoder = None;

        // Parse the encoded buffers to identify values and presence buffers
        for encoded_buffer in encoded_buffers {
            match encoded_buffer.kind {
                kind if kind == BufferKind::Data as i32 => {
                    let reader = field.open_data_ref(
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
                    let reader = field.open_data_ref(
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
    /// presence information for logical positions within the field. The position range hints
    /// are used to optimize data prefetching and caching strategies.
    ///
    /// # Arguments
    ///
    /// * `pos_ranges_hint` - An iterator of logical position ranges that are likely
    ///   to be accessed. These hints help optimize prefetching strategies for better
    ///   performance when reading from storage.
    ///
    /// # Returns
    ///
    /// A boxed field reader that can read boolean values and presence information for the
    /// specified ranges. This reader produces [`ValueSequence`] instances where the `values`
    /// field is a byte buffer with `0` or `1` valued bytes, with each byte corresponding to
    /// a single boolean value.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying bit buffer decoders fail to create readers.
    pub fn create_reader(
        &self,
        pos_ranges_hint: impl Iterator<Item = Range<u64>> + Clone,
    ) -> Result<Box<dyn FieldReader>> {
        let values_reader = self
            .values_decoder
            .create_reader(pos_ranges_hint.clone(), BlockReaderPrefetch::Enabled)?;
        let presence_reader = self
            .presence_decoder
            .create_reader(pos_ranges_hint, BlockReaderPrefetch::Enabled)?;

        // Check if we can use optimized readers
        match (&values_reader, &presence_reader) {
            (BitBufferReader::Constant(_), BitBufferReader::Constant(presence_value)) => {
                // Both values and presence are constant
                assert!(*presence_value); // Presence should be true for constant values
                Ok(Box::new(NonNullBooleanFieldReader(
                    values_reader,
                    self.basic_type,
                    self.positions,
                )))
            }
            (_, BitBufferReader::Constant(presence_value)) if *presence_value => {
                // All values are non-null, only need to read values
                Ok(Box::new(NonNullBooleanFieldReader(
                    values_reader,
                    self.basic_type,
                    self.positions,
                )))
            }
            _ => {
                // General case: read both values and presence
                Ok(Box::new(BooleanFieldReader(
                    values_reader,
                    presence_reader,
                    self.basic_type,
                    self.positions,
                )))
            }
        }
    }
}

/// A field reader for boolean fields with both values and presence buffers.
///
/// This reader handles boolean fields that store both boolean values and presence
/// information in separate bit buffers. It reads from both buffers and combines
/// them into a value sequence.
struct BooleanFieldReader(BitBufferReader, BitBufferReader, BasicTypeDescriptor, u64);

impl FieldReader for BooleanFieldReader {
    /// Reads boolean values and presence information for the specified logical position range.
    ///
    /// # Arguments
    ///
    /// * `pos_range` - The range of logical positions to read boolean data for
    ///
    /// # Returns
    ///
    /// A `ValueSequence` containing boolean values and presence information.
    fn read(&mut self, pos_range: Range<u64>) -> Result<ValueSequence> {
        verify_arg!(pos_range, pos_range.end <= self.3);

        // Read boolean values
        let values_bits = self.0.read(pos_range.clone())?;

        // Read presence information
        let presence_bits = self.1.read(pos_range)?;

        // Convert boolean values to bytes
        // TODO: optimize to avoid expansion when possible
        let mut values_bytes = AlignedByteVec::zeroed(values_bits.len());
        for (bit, byte) in values_bits.iter().zip(values_bytes.iter_mut()) {
            *byte = bit as u8;
        }

        // Convert presence bits to bytes
        // TODO: optimize to avoid expansion when possible
        let mut presence_bytes = AlignedByteVec::zeroed(presence_bits.len());
        for (bit, byte) in presence_bits.iter().zip(presence_bytes.iter_mut()) {
            *byte = bit as u8;
        }

        Ok(ValueSequence {
            values: Values::from_vec(values_bytes),
            offsets: None,
            presence: Presence::Bytes(presence_bytes),
            type_desc: self.2,
        })
    }
}

/// A field reader for non-nullable boolean fields.
///
/// This reader handles boolean fields where all values are guaranteed to be non-null,
/// so only the values buffer needs to be read. Presence information is trivial.
struct NonNullBooleanFieldReader(BitBufferReader, BasicTypeDescriptor, u64);

impl FieldReader for NonNullBooleanFieldReader {
    /// Reads boolean values for the specified logical position range.
    ///
    /// Since this reader handles non-nullable fields, it generates trivial presence
    /// information indicating that all positions in the requested range contain valid values.
    ///
    /// # Arguments
    ///
    /// * `pos_range` - The range of logical positions to read boolean data for
    ///
    /// # Returns
    ///
    /// A `ValueSequence` containing boolean values with trivial presence.
    fn read(&mut self, pos_range: Range<u64>) -> Result<ValueSequence> {
        verify_arg!(pos_range, pos_range.end <= self.2);

        // Read boolean values
        let values_bits = self.0.read(pos_range.clone())?;

        // Convert boolean values to bytes
        let mut values_bytes = AlignedByteVec::zeroed(values_bits.len());
        for (bit, byte) in values_bits.iter().zip(values_bytes.iter_mut()) {
            *byte = bit as u8;
        }

        let len = (pos_range.end - pos_range.start) as usize;
        Ok(ValueSequence {
            values: Values::from_vec(values_bytes),
            offsets: None,
            presence: Presence::Trivial(len),
            type_desc: self.1,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        read::shard::ShardOptions,
        tests::{data_generator::create_boolean_test_schema, shard_store::ShardStore},
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
        let ranges = arrow_processing::array_sequence::sample_subranges(
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
            let seq = reader.read(range.clone()).unwrap();
            assert!(seq.values.as_bytes().iter().all(|&b| b == 0 || b == 1));
            assert_eq!(seq.len(), (range.end - range.start) as usize);
            null_count += seq.presence.count_nulls();
        }
        assert!(null_count > 500);
    }
}
