//! Decoder for the `List` typed fields.

use std::ops::Range;

use amudai_blockstream::read::{
    block_stream::{BlockReaderPrefetch, DecodedBlock},
    primitive_buffer::{PrimitiveBufferDecoder, PrimitiveBufferReader},
};
use amudai_common::{Result, error::Error, verify_data};
use amudai_format::{
    defs::shard::{self, BufferKind},
    schema::{BasicType, BasicTypeDescriptor},
};
use amudai_ranges::PositionSeries;
use amudai_sequence::sequence::ValueSequence;

use crate::{
    read::{field_context::FieldContext, field_decoder::FieldCursor},
    write::field_encoder::EncodedField,
};

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

        let reader = field.open_artifact(
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

    /// Creates a reader for efficiently accessing specific positions in this field.
    ///
    /// # Arguments
    ///
    /// * `positions_hint` - An iterator of non-descending logical positions that are likely
    ///   to be accessed. These hints are used to optimize prefetching strategies
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
        let buffer_reader = self
            .offsets_buffer
            .create_reader(positions_hint, BlockReaderPrefetch::Enabled)?;
        Ok(Box::new(ListFieldReader::new(buffer_reader)))
    }
}

/// A cursor for iterator-style access to list or map items in a stripe field.
///
/// `ListFieldCursor` provides optimized access to list and map offset ranges through a
/// forward-moving access pattern with sparse position requests. This approach is both more
/// convenient and more performant than using `FieldReader` to read entire position ranges
/// when the access pattern involves reading individual list boundaries at specific positions.
///
/// This cursor is best created through [`FieldDecoder::create_list_cursor`](super::FieldDecoder::create_list_cursor),
/// providing a hint of the positions that are likely to be accessed for optimal prefetching.
///
/// # List Structure
///
/// List fields are encoded using offset arrays where each list is defined by a start and
/// end offset. The offsets point to positions in the child element arrays. For `N` lists,
/// there are `N+1` offsets stored, where:
/// - Offset `i` marks the start of list `i`
/// - Offset `i+1` marks the end of list `i` (and start of list `i+1`)
/// - The range `offsets[i]..offsets[i+1]` defines the child elements for list `i`
///
/// # Supported Types
///
/// `ListFieldCursor` supports the following field types:
/// - `List` - Variable-length lists of homogeneous elements
/// - `Map` - Key-value pair collections (encoded similarly to lists)
pub struct ListFieldCursor {
    block: DecodedBlock,
    next_block: DecodedBlock,
    reader: Box<dyn FieldReader>,
    basic_type: BasicTypeDescriptor,
    /// A pair of offsets that cross the `block`/`next_block` boundary.
    cached_boundary: [u64; 2],
}

impl ListFieldCursor {
    /// Creates a new list field cursor with the specified reader and type descriptor.
    ///
    /// # Arguments
    ///
    /// * `reader` - A boxed field reader that provides access to the underlying data blocks
    /// * `basic_type` - The basic type descriptor, which must be either `List` or `Map`
    ///
    /// # Returns
    ///
    /// A new `ListFieldCursor` ready to read offset data from the field.
    ///
    /// # Panics
    ///
    /// Panics if the basic type is not `List` or `Map`.
    pub fn new(reader: Box<dyn FieldReader>, basic_type: BasicTypeDescriptor) -> ListFieldCursor {
        assert!(matches!(
            basic_type.basic_type,
            BasicType::List | BasicType::Map
        ));
        ListFieldCursor {
            block: DecodedBlock::empty(),
            next_block: DecodedBlock::empty(),
            reader,
            basic_type,
            cached_boundary: [u64::MAX, u64::MAX],
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

    /// Fetches the offset range for a list or map at the specified position.
    ///
    /// This method retrieves the start and end offsets that define the boundaries
    /// of a list or map within the child element arrays. The method ignores null
    /// value slots, returning the stored range even for null lists (which may be
    /// empty ranges).
    ///
    /// Use [`fetch_nullable`] if you need to distinguish between actual data and null
    /// values.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position of the list/map to fetch within the stripe field
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Range<u64>` representing the start and end offsets
    /// of the list/map within the child element arrays, or an error if I/O fails.
    ///
    /// [`fetch_nullable`]: Self::fetch_nullable
    #[inline]
    pub fn fetch(&mut self, position: u64) -> Result<Range<u64>> {
        self.establish_block(position)?;
        let r = self.get_range_at(position);
        Ok(r[0]..r[1])
    }

    /// Fetches the offset range for a nullable list or map at the specified position.
    ///
    /// This method handles nullable fields by checking the presence information
    /// and returning the range only for non-null values. For null values, it returns
    /// `None` regardless of what offset data might be stored.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position of the list/map to fetch within the stripe field
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Option<Range<u64>>` where `None` indicates a null
    /// value and `Some` contains the offset range within the child element arrays,
    /// or an error if I/O fails.
    #[inline]
    pub fn fetch_nullable(&mut self, position: u64) -> Result<Option<Range<u64>>> {
        self.establish_block(position)?;
        let index = self.block.descriptor.position_index(position);
        let is_valid = self.block.values.presence.is_valid(index);
        if is_valid {
            let r = self.get_range_at(position);
            Ok(Some(r[0]..r[1]))
        } else {
            Ok(None)
        }
    }

    /// Checks if the list or map at the specified position is null.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position to check for null within the stripe field
    ///
    /// # Returns
    ///
    /// A `Result` containing `true` if the value is null, `false` otherwise,
    /// or an error if I/O fails.
    #[inline]
    pub fn is_null(&mut self, position: u64) -> Result<bool> {
        self.fetch_is_null(position)
    }

    /// Returns a two–element slice `[start, end]` giving the child-element offset range
    /// (inclusive..exclusive) for the list or map value at `position`.
    #[inline]
    fn get_range_at(&self, position: u64) -> &[u64] {
        // The list "value" is a range of child positions formed by a pair of consecutive
        // `u64` offsets that we read from the list's logical `position` and `position + 1`.
        // These might cross the current block boundary, thus we may also need to access
        // the next block.

        let index = self.block.descriptor.position_index(position);
        let values = self.block.values.as_slice::<u64>();
        if self.block.descriptor.contains(position + 1) {
            &values[index..index + 2]
        } else {
            assert_ne!(self.cached_boundary[0], u64::MAX);
            &self.cached_boundary
        }
    }

    /// Ensures the necessary blocks are cached for the specified position.
    ///
    /// This method ensures that both the current position and the next position
    /// (position + 1) are covered by cached blocks, since list operations typically
    /// need to access consecutive offsets to determine range boundaries. The dual-block
    /// strategy handles cases where the start and end offsets of a list span across
    /// block boundaries.
    ///
    /// If the required blocks are not cached, this method will fetch them from storage.
    #[inline]
    fn establish_block(&mut self, position: u64) -> Result<()> {
        if !self.block.descriptor.contains(position) {
            self.fetch_block(position)?;
        }
        if !self.block.descriptor.contains(position + 1)
            && !self.next_block.descriptor.contains(position + 1)
        {
            self.fetch_next_block(position + 1)?;
            self.cached_boundary = self.read_boundary_range();
        }
        Ok(())
    }

    /// Fetches the primary block containing the specified position.
    ///
    /// This method is marked as cold since it should be called infrequently
    /// when the cursor needs to move to a different block. It attempts to reuse
    /// the cached next block if it contains the requested position.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position that must be contained in the fetched block
    ///
    /// # Performance
    ///
    /// This is a cold path that involves I/O operations. The method optimizes for
    /// sequential access by promoting the next block to the primary block when possible.
    #[cold]
    fn fetch_block(&mut self, position: u64) -> Result<()> {
        if self.next_block.descriptor.contains(position) {
            self.block = std::mem::replace(&mut self.next_block, DecodedBlock::empty());
        } else {
            self.block = self.reader.read_containing_block(position)?;
        }
        // Reset the cached boundary range once the current block changes
        self.cached_boundary = [u64::MAX, u64::MAX];
        Ok(())
    }

    /// Fetches an additional block for the next position.
    ///
    /// This method is used when the current block doesn't contain the next
    /// position needed for range calculations. It loads the block containing
    /// the specified position into the secondary block cache.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position that must be contained in the next block
    ///
    /// # Performance
    ///
    /// This is a cold path that involves I/O operations. It's called when list
    /// range calculations require offsets from a block adjacent to the current
    /// primary block.
    #[cold]
    fn fetch_next_block(&mut self, position: u64) -> Result<()> {
        self.next_block = self.reader.read_containing_block(position)?;
        Ok(())
    }

    /// Reads and returns the cross‑block boundary offset pair needed when
    /// list or map value spans two adjacent decoded blocks.
    fn read_boundary_range(&self) -> [u64; 2] {
        assert_eq!(
            self.block.descriptor.logical_range.end,
            self.next_block.descriptor.logical_range.start
        );
        let values = self.block.values.as_slice::<u64>();
        let next_values = self.next_block.values.as_slice::<u64>();
        [*values.last().unwrap(), *next_values.first().unwrap()]
    }
}

impl FieldCursor for ListFieldCursor {
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
        if self.block.descriptor.logical_range.is_empty() {
            return self.block.descriptor.logical_range.clone();
        }

        let start = self.block.descriptor.logical_range.start;
        let end = if !self.next_block.descriptor.logical_range.is_empty()
            && self.block.descriptor.logical_range.end
                == self.next_block.descriptor.logical_range.start
        {
            self.next_block.descriptor.logical_range.end - 1
        } else {
            self.block.descriptor.logical_range.end - 1
        };
        start..end
    }

    #[inline]
    fn cached_is_null(&self, position: u64) -> bool {
        debug_assert!(self.cached_range().contains(&position));
        let index = self.block.descriptor.position_index(position);
        self.block.values.presence.is_null(index)
    }

    #[inline]
    fn cached_value_as_bytes(&self, position: u64) -> &[u8] {
        debug_assert!(self.cached_range().contains(&position));
        bytemuck::cast_slice(self.get_range_at(position))
    }

    #[inline]
    fn cached_nullable_as_bytes(&self, position: u64) -> Option<&[u8]> {
        debug_assert!(self.cached_range().contains(&position));
        let is_valid = !self.cached_is_null(position);
        is_valid.then(|| self.cached_value_as_bytes(position))
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

/// A reader for accessing list offset data from a `List` field.
///
/// This reader provides access to ranges of offset values from an encoded
/// `OFFSETS` buffer associated with a `List` field.
///
/// **NOTES**:
///  1. `ListFieldReader::read_range()` returns a `Sequence` whose **`values`** field contains
///     the absolute list offsets within the stripe. The **`offsets`** field of `Sequence`
///     is always `None`.
///
///  2. To retrieve all relevant offsets for `N` lists, the range passed to the `read_range()`
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
    fn read_range(&mut self, pos_range: Range<u64>) -> Result<ValueSequence> {
        let range_len = pos_range.end - pos_range.start;
        verify_data!(range_len, range_len >= 2);
        self.buffer_reader.read_range(pos_range)
    }

    fn read_containing_block(&mut self, position: u64) -> Result<DecodedBlock> {
        self.buffer_reader.read_containing_block(position)
    }
}
