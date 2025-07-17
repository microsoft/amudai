//! Field decoders for `Struct` and `FixedSizeList` types.
//!
//! This module provides specialized decoders for compound data types that require
//! presence information to determine which logical positions contain valid values.
//! Unlike primitive types that store actual values, struct and fixed-size list fields
//! store only presence/nullability information that indicates whether child
//! fields contain valid data at each logical position.
//!
//! ## Field Types Handled
//!
//! - **Struct fields**: Composite types containing multiple named child fields
//! - **FixedSizeList fields**: Lists with a predetermined, fixed number of elements
//!
//! ## Presence Handling
//!
//! Both field types use presence buffers to track nullability:
//! - **With presence buffer**: Nullable fields store a bit buffer indicating which
//!   positions contain valid (non-null) values
//! - **Without presence buffer**: Non-nullable fields where all positions are valid

use std::ops::Range;

use amudai_blockstream::read::{
    bit_buffer::{BitBufferDecoder, BitBufferReader},
    block_stream::BlockReaderPrefetch,
};
use amudai_common::{Result, error::Error, verify_arg, verify_data};
use amudai_format::{
    defs::shard::{self, BufferKind},
    schema::{BasicType, BasicTypeDescriptor},
};
use amudai_sequence::{presence::Presence, sequence::ValueSequence, values::Values};

use crate::{
    read::{field_context::FieldContext, field_decoder::boolean::bits_to_byte_vec},
    write::field_encoder::EncodedField,
};

use super::FieldReader;

/// A decoder for `Struct` and `FixedSizeList` field types.
///
/// This decoder handles the reading of presence information for compound data types.
/// Rather than storing actual values, these fields store nullability information
/// that indicates whether child fields contain valid data at each logical position.
///
/// The decoder supports both nullable fields (with explicit presence buffers) and
/// non-nullable fields (where all positions are implicitly valid).
///
/// # Type Support
///
/// - `BasicType::Struct`: Multi-field composite types
/// - `BasicType::FixedSizeList`: Fixed-length array types
///
/// # Storage Format
///
/// Presence information is stored as bit buffers where each bit indicates whether
/// the corresponding logical position contains a valid (non-null) value. For
/// non-nullable fields, no presence buffer is stored and all positions are
/// considered valid.
#[derive(Clone)]
pub struct StructFieldDecoder {
    /// The basic type descriptor, indicating either `Struct` or `FixedSizeList`.
    basic_type: BasicTypeDescriptor,
    /// Decoder for the presence bit buffer, or a constant decoder for non-nullable fields.
    presence: BitBufferDecoder,
    /// Total number of logical positions in the field.
    positions: u64,
}

impl StructFieldDecoder {
    /// Creates a new `StructFieldDecoder` with the specified components.
    ///
    /// # Arguments
    ///
    /// * `basic_type` - The basic type descriptor for the field (Struct or FixedSizeList)
    /// * `presence` - Decoder for the presence bit buffer, or a constant decoder for non-nullable fields
    /// * `positions` - Total number of logical positions in the field
    ///
    /// # Returns
    ///
    /// A new `StructFieldDecoder` instance configured with the provided parameters.
    pub fn new(
        basic_type: BasicTypeDescriptor,
        presence: BitBufferDecoder,
        positions: u64,
    ) -> StructFieldDecoder {
        StructFieldDecoder {
            basic_type,
            presence,
            positions,
        }
    }

    /// Creates a `StructFieldDecoder` from a field context.
    ///
    /// This method parses the field's metadata and encoded buffers to construct
    /// a decoder suitable for reading presence information. It handles both nullable
    /// fields (with presence buffers) and non-nullable fields (without presence buffers).
    ///
    /// # Arguments
    ///
    /// * `field` - The field context containing type information and encoded buffers
    ///
    /// # Returns
    ///
    /// A new `StructFieldDecoder` configured for the specified field.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The field's basic type is not `Struct` or `FixedSizeList`
    /// - The encoded buffers are malformed or contain unexpected buffer types
    /// - The presence buffer reference cannot be opened
    pub(crate) fn from_field(field: &FieldContext) -> Result<StructFieldDecoder> {
        let basic_type = field.data_type().describe()?;
        verify_data!(
            basic_type,
            basic_type.basic_type == BasicType::Struct
                || basic_type.basic_type == BasicType::FixedSizeList
        );

        let encoded_buffers = field.get_encoded_buffers()?;
        if encoded_buffers.is_empty() {
            // Encoding has no presence buffer - the field is non-nullable.
            return Ok(StructFieldDecoder::new(
                basic_type,
                BitBufferDecoder::from_constant(true),
                field.position_count(),
            ));
        }

        verify_data!(encoded_buffers, encoded_buffers.len() == 1);
        let encoded_buffer = &encoded_buffers[0];
        verify_data!(
            encoded_buffer,
            encoded_buffer.kind == BufferKind::Presence as i32
        );

        let reader = field.open_data_ref(
            encoded_buffer
                .buffer
                .as_ref()
                .ok_or_else(|| Error::invalid_format("Missing buffer reference"))?,
        )?;

        let presence = BitBufferDecoder::from_encoded_buffer(reader.into_inner(), encoded_buffer)?;
        Ok(StructFieldDecoder::new(
            basic_type,
            presence,
            field.position_count(),
        ))
    }

    /// Creates a `StructFieldDecoder` from an encoded field.
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
    ) -> Result<StructFieldDecoder> {
        verify_data!(
            basic_type,
            basic_type.basic_type == BasicType::Struct
                || basic_type.basic_type == BasicType::FixedSizeList
        );

        // Try to get the presence buffer - it may not exist for non-nullable fields
        let presence_buffer = field.get_encoded_buffer(shard::BufferKind::Presence).ok();

        let presence = presence_buffer
            .map(|buf| BitBufferDecoder::from_prepared_buffer(buf))
            .unwrap_or_else(|| Ok(BitBufferDecoder::from_constant(true)))?;

        Ok(StructFieldDecoder::new(basic_type, presence, positions))
    }

    /// Returns the basic type descriptor for this field.
    ///
    /// The returned descriptor will indicate either `BasicType::Struct` for composite
    /// types with multiple named fields, or `BasicType::FixedSizeList` for fixed-length
    /// array types.
    ///
    /// # Returns
    ///
    /// The basic type descriptor describing the field's type structure.
    pub fn basic_type(&self) -> BasicTypeDescriptor {
        self.basic_type
    }

    /// Creates a field reader optimized for the specified position ranges.
    ///
    /// This method creates a reader that can efficiently access presence information
    /// for logical positions within the field. The position range hints are used to
    /// optimize data prefetching and caching strategies.
    ///
    /// Since struct and fixed-size list fields store only nullability information rather than
    /// actual values, the resulting `FieldReader` returns value sequences where only the
    /// `presence` and `type_desc` fields contain meaningful data. The `values` and `offsets`
    /// fields are always empty because these field types serve as containers that indicate
    /// which logical positions have valid child data, rather than storing values themselves.
    ///
    /// # Arguments
    ///
    /// * `pos_ranges_hint` - An iterator of logical position ranges that are likely
    ///   to be accessed. These hints help optimize prefetching strategies for better
    ///   performance when reading from storage.
    ///
    /// # Returns
    ///
    /// A boxed field reader that can efficiently read presence information for the
    /// specified ranges. The actual reader type depends on whether the field uses
    /// explicit presence buffers or trivial (all-valid) presence.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying bit buffer decoder fails to create a reader.
    pub fn create_reader(
        &self,
        pos_ranges_hint: impl Iterator<Item = Range<u64>> + Clone,
    ) -> Result<Box<dyn FieldReader>> {
        let reader = self
            .presence
            .create_reader(pos_ranges_hint, BlockReaderPrefetch::Enabled)?;
        match &reader {
            BitBufferReader::Blocks(_) => Ok(Box::new(PresenceFieldReader(
                reader,
                self.basic_type,
                self.positions,
            ))),
            BitBufferReader::Constant(value) => {
                assert!(*value);
                Ok(Box::new(TrivialPresenceFieldReader(
                    self.basic_type,
                    self.positions,
                )))
            }
        }
    }
}

/// A field reader for fields with explicit presence buffers.
///
/// This reader handles struct and fixed-size list fields that store presence
/// information in bit buffers. Each bit indicates whether the corresponding
/// logical position contains a valid (non-null) value.
///
/// The reader converts bit-level presence information into byte-level presence
/// data for consumption by higher-level components.
///
/// # Storage Format
///
/// Presence data is stored as compressed bit buffers, where each bit represents
/// the validity of one logical position. This reader decompresses these bits
/// and expands them to byte arrays for easier processing.
///
/// # Note
///
/// The current implementation expands bits to bytes, this should be optimized optimized
/// by returning bit buffers directly to avoid unnecessary expansion.
struct PresenceFieldReader(BitBufferReader, BasicTypeDescriptor, u64);

impl FieldReader for PresenceFieldReader {
    /// Reads presence information for the specified logical position range.
    ///
    /// # Arguments
    ///
    /// * `pos_range` - The range of logical positions to read presence information for
    ///
    /// # Returns
    ///
    /// A `ValueSequence` containing presence information as byte data. The sequence
    /// has empty values and offsets since this reader only provides presence data.
    ///
    /// # Performance Note
    ///
    /// Currently expands bits to bytes for compatibility. Future optimizations
    /// may return bit buffers directly to reduce memory usage and improve performance.
    fn read(&mut self, pos_range: Range<u64>) -> Result<ValueSequence> {
        verify_arg!(pos_range, pos_range.end <= self.2);

        let buf = self.0.read(pos_range)?;

        // TODO: avoid expansion, return it as-is (bit buffer presence) in the sequence.
        let bytes = bits_to_byte_vec(&buf);

        Ok(ValueSequence {
            values: Values::new(),
            offsets: None,
            presence: Presence::Bytes(bytes),
            type_desc: self.1,
        })
    }
}

/// A field reader for non-nullable fields with trivial (all-valid) presence.
///
/// This reader handles struct and fixed-size list fields that don't store explicit
/// presence buffers because all logical positions are guaranteed to contain valid
/// values. Instead of reading from storage, it generates trivial presence information
/// indicating that all requested positions are valid.
struct TrivialPresenceFieldReader(BasicTypeDescriptor, u64);

impl FieldReader for TrivialPresenceFieldReader {
    /// Reads trivial presence information for the specified logical position range.
    ///
    /// Since this reader handles non-nullable fields, it generates presence information
    /// indicating that all positions in the requested range contain valid values.
    /// No actual storage access is required.
    ///
    /// # Arguments
    ///
    /// * `pos_range` - The range of logical positions to generate presence information for
    ///
    /// # Returns
    ///
    /// A `ValueSequence` with trivial presence indicating that all positions in the
    /// range are valid. The sequence has empty values and offsets since this reader
    /// only provides presence data.
    fn read(&mut self, pos_range: Range<u64>) -> Result<ValueSequence> {
        verify_arg!(pos_range, pos_range.end <= self.1);

        let len = (pos_range.end - pos_range.start) as usize;
        Ok(ValueSequence {
            values: Values::new(),
            offsets: None,
            presence: Presence::Trivial(len),
            type_desc: self.0,
        })
    }
}
