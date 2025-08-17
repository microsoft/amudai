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
    bit_buffer::{BitBufferDecoder, BitBufferReader, create_ephemeral_presence_block},
    block_stream::{BlockReaderPrefetch, DecodedBlock},
};
use amudai_common::{Result, error::Error, verify_arg, verify_data};
use amudai_format::{
    defs::shard::{self, BufferKind},
    schema::{BasicType, BasicTypeDescriptor},
};
use amudai_ranges::PositionSeries;
use amudai_sequence::{presence::Presence, sequence::ValueSequence, values::Values};

use crate::{
    read::{field_context::FieldContext, field_decoder::FieldCursor},
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

        let reader = field.open_artifact(
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
            .map(BitBufferDecoder::from_prepared_buffer)
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
    /// * `positions_hint` - An iterator of logical positions or ranges that are likely
    ///   to be accessed. These hints help optimize prefetching strategies for better
    ///   performance when reading from storage.
    ///
    /// # Returns
    ///
    /// A boxed field reader that can efficiently read presence information for the
    /// specified positions or ranges. The actual reader type depends on whether the field
    /// uses explicit presence buffers or trivial (all-valid) presence.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying bit buffer decoder fails to create a reader.
    pub fn create_reader(
        &self,
        positions_hint: impl PositionSeries<u64> + Clone,
    ) -> Result<Box<dyn FieldReader>> {
        let reader = self
            .presence
            .create_reader(positions_hint, BlockReaderPrefetch::Enabled)?;
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

/// A cursor for iterator-style access to struct and fixed-size list presence information
/// in a stripe field.
///
/// `StructFieldCursor` provides optimized access to nullability information for composite
/// data types through a forward-moving access pattern with sparse position requests. Unlike
/// other field cursors that read actual values, this cursor only provides access to presence
/// information since struct and fixed-size list fields serve as containers that indicate
/// which logical positions have valid child data, rather than storing values themselves.
///
/// The cursor maintains an internal cache of the current data block and only fetches
/// new blocks when the requested position falls outside the cached block's range,
/// minimizing I/O overhead for sequential or nearby access patterns.
///
/// This cursor is best created through [`FieldDecoder::create_struct_cursor`](super::FieldDecoder::create_struct_cursor),
/// providing a hint of the positions that are likely to be accessed for optimal prefetching.
///
/// # Presence Information
///
/// Struct and fixed-size list fields store presence (nullability) information that indicates
/// whether child fields contain valid data at each logical position. This cursor provides
/// efficient access to this presence information through the [`is_null`] method.
///
/// For non-nullable fields, all positions are implicitly valid and [`is_null`] will always
/// return `false`. For nullable fields, the presence information is read from bit buffers
/// stored in the stripe format.
///
/// # Supported Types
///
/// `StructFieldCursor` supports the following field types:
/// - `Struct` - Multi-field composite types with named child fields
/// - `FixedSizeList` - Fixed-length array types with homogeneous elements
///
/// [`is_null`]: Self::is_null
pub struct StructFieldCursor {
    block: DecodedBlock,
    reader: Box<dyn FieldReader>,
    basic_type: BasicTypeDescriptor,
}

impl StructFieldCursor {
    /// Creates a new struct field cursor with the specified reader and type descriptor.
    ///
    /// # Arguments
    ///
    /// * `reader` - A boxed field reader that provides access to the underlying presence
    ///   data blocks. This reader should be configured with appropriate position hints
    ///   for optimal prefetching performance.
    /// * `basic_type` - The basic type descriptor defining the structure and properties
    ///   of the field. Must be either `BasicType::Struct` for composite types with
    ///   multiple named fields, or `BasicType::FixedSizeList` for fixed-length arrays.
    ///
    /// # Returns
    ///
    /// A new `StructFieldCursor` ready to read presence information from the field.
    ///
    /// # Panics
    ///
    /// Panics if the basic type is not `Struct` or `FixedSizeList`.
    pub fn new(reader: Box<dyn FieldReader>, basic_type: BasicTypeDescriptor) -> StructFieldCursor {
        assert!(matches!(
            basic_type.basic_type,
            BasicType::Struct | BasicType::FixedSizeList
        ));
        StructFieldCursor {
            block: DecodedBlock::empty(),
            reader,
            basic_type,
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
    /// For non-nullable fields, this method will always return `false` since all
    /// positions are guaranteed to contain valid data.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position to check for nullability within the stripe field
    ///
    /// # Returns
    ///
    /// A `Result` containing `true` if the value is null (indicating no valid child
    /// data at this position), `false` if the value is valid, or an error if I/O fails
    /// or the position is invalid.
    #[inline]
    pub fn is_null(&mut self, position: u64) -> Result<bool> {
        self.establish_block(position)?;
        let index = self.position_index(position);
        Ok(self.block.values.presence.is_null(index))
    }

    /// Ensures the current block contains the specified position.
    #[inline]
    fn establish_block(&mut self, position: u64) -> Result<()> {
        if !self.block.descriptor.contains(position) {
            self.fetch_block(position)
        } else {
            Ok(())
        }
    }

    /// Converts a logical position to a block-relative index.
    #[inline]
    fn position_index(&self, position: u64) -> usize {
        self.block.descriptor.position_index(position)
    }

    /// Fetches a new block containing the specified position.
    #[cold]
    fn fetch_block(&mut self, position: u64) -> Result<()> {
        self.block = self.reader.read_containing_block(position)?;
        Ok(())
    }
}

impl FieldCursor for StructFieldCursor {
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
        &[]
    }

    #[inline]
    fn cached_nullable_as_bytes(&self, position: u64) -> Option<&[u8]> {
        if self.cached_is_null(position) {
            None
        } else {
            Some(&[])
        }
    }

    #[inline]
    fn fetch_is_null(&mut self, position: u64) -> Result<bool> {
        self.is_null(position)
    }

    #[inline]
    fn fetch_value_as_bytes(&mut self, position: u64) -> Result<&[u8]> {
        self.establish_block(position)?;
        Ok(&[])
    }

    #[inline]
    fn fetch_nullable_as_bytes(&mut self, position: u64) -> Result<Option<&[u8]>> {
        if self.is_null(position)? {
            Ok(None)
        } else {
            Ok(Some(&[]))
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
/// The current implementation expands bits to bytes, this should be optimized
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
    fn read_range(&mut self, pos_range: Range<u64>) -> Result<ValueSequence> {
        verify_arg!(pos_range, pos_range.end <= self.2);
        Ok(ValueSequence {
            values: Values::new(),
            offsets: None,
            presence: self.0.read_range_as_presence(pos_range)?,
            type_desc: self.1,
        })
    }

    fn read_containing_block(&mut self, position: u64) -> Result<DecodedBlock> {
        verify_arg!(position, position < self.2);
        if let Some(value) = self.0.try_get_constant() {
            return Ok(create_ephemeral_presence_block(value, position));
        }
        let mut block = self.0.read_containing_block(position)?;
        let values = std::mem::replace(&mut block.values.values, Values::new());
        let presence = Presence::Bytes(values.into_inner());
        block.values.presence = presence;
        block.values.type_desc = self.1;
        Ok(block)
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
    fn read_range(&mut self, pos_range: Range<u64>) -> Result<ValueSequence> {
        verify_arg!(pos_range, pos_range.end <= self.1);
        let len = (pos_range.end - pos_range.start) as usize;
        Ok(ValueSequence {
            values: Values::new(),
            offsets: None,
            presence: Presence::Trivial(len),
            type_desc: self.0,
        })
    }

    fn read_containing_block(&mut self, position: u64) -> Result<DecodedBlock> {
        let mut block = create_ephemeral_presence_block(true, position);
        block.values.type_desc = self.0;
        Ok(block)
    }
}
