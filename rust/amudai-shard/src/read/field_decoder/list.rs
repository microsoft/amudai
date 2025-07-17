//! Decoder for the `List` typed fields.

use std::ops::Range;

use amudai_blockstream::read::{
    block_stream::BlockReaderPrefetch,
    primitive_buffer::{PrimitiveBufferDecoder, PrimitiveBufferReader},
};
use amudai_common::{Result, error::Error, verify_data};
use amudai_format::{
    defs::shard::{self, BufferKind},
    schema::{BasicType, BasicTypeDescriptor},
};
use amudai_sequence::sequence::ValueSequence;

use crate::{read::field_context::FieldContext, write::field_encoder::EncodedField};

use super::FieldReader;

/// Decodes `List` typed fields from encoded buffer data in a stripe.
///
/// List fields are represented by a sequence of offsets that define the start
/// and end positions of each list within a flattened child element buffer.
///
/// The offsets buffer contains `N+1` values, where `N` is the number of lists. Each
/// value indicates the starting offset of a list in the child elements, with the last
/// offset marking the end of the final list.
///
/// This decoder works in conjunction with a decoder for the child elements to reconstruct
/// the list structure.
#[derive(Clone)]
pub struct ListFieldDecoder {
    /// Decoder for the buffer containing the encoded list offsets.
    ///
    /// This `PrimitiveBufferDecoder` is specialized to read the `u64` offsets
    /// that define the boundaries of each list.
    offsets_buffer: PrimitiveBufferDecoder,
    // TODO: add presence buffer decoder when it exists
}

impl ListFieldDecoder {
    /// Creates a new `ListFieldDecoder` from a `PrimitiveBufferDecoder` for the offsets.
    ///
    /// This constructor is used when the `PrimitiveBufferDecoder` for the list's
    /// offsets has already been instantiated.
    ///
    /// # Arguments
    ///
    /// * `offsets_buffer` - A `PrimitiveBufferDecoder` configured to read the
    ///   list's offset values (`u64`).
    pub fn new(offsets_buffer: PrimitiveBufferDecoder) -> ListFieldDecoder {
        ListFieldDecoder { offsets_buffer }
    }

    /// Creates a `ListFieldDecoder` from a given `FieldContext`.
    ///
    /// This factory method inspects the `FieldContext` to find the necessary
    /// metadata and data references for the list's offsets. It then initializes
    /// a `PrimitiveBufferDecoder` for these offsets.
    ///
    /// The method verifies that the field is indeed a `List` type and that
    /// the associated encoded buffers are correctly structured, expecting
    /// an `OFFSETS` buffer. It also checks that the number of offsets
    /// matches the expected count based on the field's position count.
    ///
    /// # Arguments
    ///
    /// * `field` - The `FieldContext` which provides access to the schema,
    ///   field descriptor, and data references for the list field.
    ///
    /// # Returns
    ///
    /// A `Result` containing the initialized `ListFieldDecoder` if successful,
    /// or an `Error` if the field metadata is invalid, buffers are missing,
    /// or type mismatches occur.
    ///
    /// # Errors
    ///
    /// This function can return an error in several cases:
    /// - If the `FieldContext` does not describe a `List` type.
    /// - If the required `OFFSETS` buffer is not found or is malformed.
    /// - If there's a mismatch between the expected number of offsets and the
    ///   actual count in the buffer.
    /// - If underlying operations to open data references or create buffer decoders
    ///   fail.
    pub(crate) fn from_field(field: &FieldContext) -> Result<ListFieldDecoder> {
        let basic_type = field.data_type().describe()?;
        verify_data!(
            basic_type,
            basic_type.basic_type == BasicType::List || basic_type.basic_type == BasicType::Map
        );

        let encoded_buffer = field.get_encoded_buffer(BufferKind::Offsets)?;

        let reader = field.open_data_ref(
            encoded_buffer
                .buffer
                .as_ref()
                .ok_or_else(|| Error::invalid_format("Missing buffer reference"))?,
        )?;

        let offsets_buffer = PrimitiveBufferDecoder::from_encoded_buffer(
            reader.into_inner(),
            encoded_buffer,
            BasicTypeDescriptor {
                basic_type: BasicType::Int64,
                fixed_size: 0,
                signed: false,
                extended_type: Default::default(),
            },
        )?;

        let field_desc = field
            .descriptor()
            .field
            .as_ref()
            .expect("stripe field descriptor");
        let offset_count = offsets_buffer.block_stream().block_map().value_count()?;
        // The actual number of stored offsets must be one more than the number of logical
        // values in the List field.
        verify_data!(offset_count, offset_count == field_desc.position_count + 1);

        Ok(ListFieldDecoder::new(offsets_buffer))
    }

    /// Creates a `ListFieldDecoder` from an encoded field.
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
    ) -> Result<ListFieldDecoder> {
        verify_data!(
            basic_type,
            basic_type.basic_type == BasicType::List || basic_type.basic_type == BasicType::Map
        );

        let prepared_buffer = field.get_encoded_buffer(shard::BufferKind::Offsets)?;

        let offsets_buffer = PrimitiveBufferDecoder::from_prepared_buffer(
            prepared_buffer,
            BasicTypeDescriptor {
                basic_type: BasicType::Int64,
                fixed_size: 0,
                signed: false,
                extended_type: Default::default(),
            },
        )?;

        Ok(ListFieldDecoder::new(offsets_buffer))
    }

    /// Returns the basic type descriptor for this list field.
    ///
    /// This will always describe a `BasicType::List`.
    pub fn basic_type(&self) -> BasicTypeDescriptor {
        BasicTypeDescriptor {
            basic_type: BasicType::List,
            ..Default::default()
        }
    }

    /// Creates a `FieldReader` for efficiently accessing ranges of list offsets.
    ///
    /// The returned reader can be used to fetch specific ranges of offsets from the
    /// list field. These offsets define the structure of the lists.
    ///
    /// # Arguments
    ///
    /// * `pos_ranges_hint` - An iterator of logical position ranges (representing
    ///   list indices) that are likely to be accessed. These hints can be used
    ///   by the underlying buffer reader to optimize data access, for example,
    ///   by prefetching required data blocks.
    ///
    /// # Returns
    ///
    /// A `Result` containing a boxed `FieldReader` capable of reading list offsets,
    /// or an `Error` if the reader cannot be created (e.g., due to issues with
    /// the underlying `offsets_buffer`).
    pub fn create_reader(
        &self,
        pos_ranges_hint: impl Iterator<Item = Range<u64>> + Clone,
    ) -> Result<Box<dyn FieldReader>> {
        let buffer_reader = self
            .offsets_buffer
            .create_reader(pos_ranges_hint, BlockReaderPrefetch::Enabled)?;
        Ok(Box::new(ListFieldReader::new(buffer_reader)))
    }
}

/// A reader for accessing list offset data from a `List` field.
///
/// This reader provides access to ranges of offset values from an encoded
/// `OFFSETS` buffer associated with a `List` field.
///
/// **NOTES**:
///  1. `ListFieldReader::read()` returns a `Sequence` whose **`values`** field contains
///     the absolute list offsets within the stripe. The **`offsets`** field of `Sequence`
///     is always `None`.
///
///  2. To retrieve all relevant offsets for `N` lists, the range passed to the `read()`
///     method must include `N+1` positions; that is, it must cover the end of the last list.
struct ListFieldReader {
    buffer_reader: PrimitiveBufferReader,
}

impl ListFieldReader {
    fn new(buffer_reader: PrimitiveBufferReader) -> ListFieldReader {
        ListFieldReader { buffer_reader }
    }
}

impl FieldReader for ListFieldReader {
    fn read(&mut self, pos_range: Range<u64>) -> Result<ValueSequence> {
        let range_len = pos_range.end - pos_range.start;
        verify_data!(range_len, range_len >= 2);
        self.buffer_reader.read(pos_range)
    }
}
