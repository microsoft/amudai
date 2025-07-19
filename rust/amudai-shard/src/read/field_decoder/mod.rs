//! Field decoder traits and implementations.

use std::ops::Range;

use amudai_blockstream::read::block_stream::DecodedBlock;
use amudai_common::Result;
use amudai_format::schema::BasicTypeDescriptor;
use amudai_sequence::sequence::ValueSequence;
use boolean::BooleanFieldDecoder;
use bytes::BytesFieldDecoder;
use dictionary::DictionaryFieldDecoder;
use list::ListFieldDecoder;
use primitive::PrimitiveFieldDecoder;
use unit::StructFieldDecoder;

pub mod boolean;
pub mod bytes;
pub mod dictionary;
pub mod list;
pub mod primitive;
pub mod unit;

/// Decoder for a stripe field.
///
/// This enum wraps different field decoder implementations based on the underlying
/// field type. It serves as the entry point for reading field data from a stripe,
/// allowing clients to create readers for accessing ranges of field values.
#[derive(Clone)]
pub enum FieldDecoder {
    /// Decoder for primitive-typed fields (integers, floats, etc.)
    Primitive(PrimitiveFieldDecoder),
    /// Decoder for boolean fields
    Boolean(BooleanFieldDecoder),
    /// Decoder for bytes-like fields (strings, binary, GUID, etc.)
    Bytes(BytesFieldDecoder),
    /// Decoder for dictionary-encoded fields
    Dictionary(DictionaryFieldDecoder),
    /// List offsets decoder
    List(ListFieldDecoder),
    /// Struct presence decoder
    Struct(StructFieldDecoder),
}

impl FieldDecoder {
    /// Returns the `BasicTypeDescriptor` of the field.
    pub fn basic_type(&self) -> BasicTypeDescriptor {
        match self {
            FieldDecoder::Primitive(decoder) => decoder.basic_type(),
            FieldDecoder::Boolean(decoder) => decoder.basic_type(),
            FieldDecoder::Bytes(decoder) => decoder.basic_type(),
            FieldDecoder::Dictionary(decoder) => decoder.basic_type(),
            FieldDecoder::List(decoder) => decoder.basic_type(),
            FieldDecoder::Struct(decoder) => decoder.basic_type(),
        }
    }

    /// Creates a reader for efficiently accessing ranges of values in this field.
    ///
    /// # Arguments
    ///
    /// * `pos_ranges_hint` - An iterator of logical position ranges that are likely
    ///   to be accessed. These hints are used to optimize prefetching strategies
    ///   for better performance when reading from storage.
    ///
    /// # Returns
    ///
    /// A boxed field reader for accessing the field values.
    pub fn create_reader_with_ranges(
        &self,
        pos_ranges_hint: impl Iterator<Item = Range<u64>> + Clone,
    ) -> Result<Box<dyn FieldReader>> {
        match self {
            FieldDecoder::Primitive(decoder) => decoder.create_reader_with_ranges(pos_ranges_hint),
            FieldDecoder::Boolean(decoder) => decoder.create_reader_with_ranges(pos_ranges_hint),
            FieldDecoder::Bytes(decoder) => decoder.create_reader_with_ranges(pos_ranges_hint),
            FieldDecoder::Dictionary(decoder) => decoder.create_reader_with_ranges(pos_ranges_hint),
            FieldDecoder::List(decoder) => decoder.create_reader_with_ranges(pos_ranges_hint),
            FieldDecoder::Struct(decoder) => decoder.create_reader_with_ranges(pos_ranges_hint),
        }
    }

    /// Creates a reader for efficiently accessing specific positions in this field.
    ///
    /// This method is similar to `create_reader_with_ranges` but optimized for
    /// non-contiguous access patterns where only specific logical positions are
    /// likely to be accessed.
    ///
    /// # Arguments
    ///
    /// * `positions_hint` - An iterator of non-descending logical positions that are likely
    ///   to be accessed. These hints are used to optimize prefetching strategies
    ///   for better performance when reading from storage. The positions must be
    ///   in non-descending order but don't need to be unique.
    ///
    /// # Returns
    ///
    /// A boxed field reader for accessing the field values.
    pub fn create_reader_with_positions(
        &self,
        positions_hint: impl Iterator<Item = u64> + Clone,
    ) -> Result<Box<dyn FieldReader>> {
        match self {
            FieldDecoder::Primitive(decoder) => {
                decoder.create_reader_with_positions(positions_hint)
            }
            FieldDecoder::Boolean(decoder) => decoder.create_reader_with_positions(positions_hint),
            FieldDecoder::Bytes(decoder) => decoder.create_reader_with_positions(positions_hint),
            FieldDecoder::Dictionary(decoder) => {
                decoder.create_reader_with_positions(positions_hint)
            }
            FieldDecoder::List(decoder) => decoder.create_reader_with_positions(positions_hint),
            FieldDecoder::Struct(decoder) => decoder.create_reader_with_positions(positions_hint),
        }
    }
}

/// A reader for the stripe field data that efficiently accesses and decodes
/// ranges of logical values.
///
/// Field readers provide sequential or random access to values within a field,
/// regardless of how they're physically stored or encoded. They handle the details
/// of locating blocks, decoding values, and assembling the results into a sequence.
///
/// Field readers may maintain internal caches and prefetch data to improve
/// performance for sequential reads or reads within previously hinted ranges.
pub trait FieldReader: Send + Sync + 'static {
    /// Reads values from the specified logical position range.
    ///
    /// This method returns a `ValueSequence` containing the decoded field data for the requested
    /// logical position range. The exact content and structure of the returned sequence depends
    /// on the underlying field type and how different field types store their data.
    ///
    /// # Arguments
    ///
    /// * `pos_range` - The range of logical positions to read (inclusive start, exclusive end).
    ///
    /// # Returns
    ///
    /// A `ValueSequence` containing the decoded values for the requested range. The structure
    /// and content of the sequence varies by field type as described below.
    ///
    /// # ValueSequence Content by Field Type
    ///
    /// ## Primitive Fields (PrimitiveFieldDecoder)
    ///
    /// For primitive numeric types (integers, floating-point numbers, etc.):
    ///
    /// - **`values`**: Contains the raw primitive values stored as a contiguous byte buffer.
    ///   Values are stored in their native binary representation (e.g., 4 bytes per `i32`,
    ///   8 bytes per `f64`). Use `values.as_slice::<T>()` to access typed values.
    /// - **`offsets`**: Always `None` (primitive types have fixed sizes).
    /// - **`presence`**: Indicates which logical positions contain valid (non-null) values.
    ///   May be `Presence::Trivial(count)` for non-nullable fields, `Presence::Bytes(buffer)`
    ///   for fields with explicit null tracking, or other presence variants.
    /// - **`type_desc`**: Describes the primitive type (e.g., `Int32`, `Float64`) and
    ///   signedness information.
    ///
    /// ## Boolean Fields (BooleanFieldDecoder)
    ///
    /// For boolean-typed fields:
    ///
    /// - **`values`**: Contains boolean values as individual bytes, where each byte is either
    ///   `0` (false) or `1` (true). The buffer length equals the number of logical positions
    ///   in the range.
    /// - **`offsets`**: Always `None` (boolean values have fixed size).
    /// - **`presence`**: Indicates which logical positions contain valid (non-null) boolean
    ///   values. For non-nullable boolean fields, this will be `Presence::Trivial(count)`.
    ///   For nullable fields, this contains the actual nullability information.
    /// - **`type_desc`**: Describes the boolean type with `basic_type: Boolean`.
    ///
    /// ## Bytes Fields (BytesFieldDecoder)
    ///
    /// For variable-length binary data, fixed-size binary data and string fields (`Binary`,
    /// `String`, `FixedSizeBinary`, `Guid`, decimal extension type):
    ///
    /// - **`values`**: Contains the concatenated byte data for all values in the range.
    ///   For variable-length types (`Binary`, `String`), individual values are stored
    ///   consecutively without delimiters. For `FixedSizeBinary`, each value occupies
    ///   exactly `type_desc.fixed_size` bytes.
    /// - **`offsets`**: For variable-length types (`Binary`, `String`), contains `N+1` offset
    ///   values defining the byte boundaries of each value in the `values` buffer. The value
    ///   at logical index `i` spans bytes `offsets[i]..offsets[i+1]`. For fixed-size types
    ///   (`FixedSizeBinary`, `Guid`), this will be `None` since positions can be calculated
    ///   from the fixed size.
    /// - **`presence`**: Indicates which logical positions contain valid (non-null) values.
    /// - **`type_desc`**: Describes the bytes type, including `fixed_size` for fixed-size
    ///   binary types.
    ///
    /// ## List Fields (ListFieldDecoder)
    ///
    /// For list-typed fields that store offset information defining list boundaries:
    ///
    /// - **`values`**: Contains the actual list offset values stored as `u64` primitives.
    ///   These are absolute offsets within the stripe pointing to child element positions.
    ///   Use `values.as_slice::<u64>()` to access the offset values.
    /// - **`offsets`**: Always `None` (offsets are the actual data, not metadata).
    /// - **`presence`**: Indicates presence information for the list values themselves.
    /// - **`type_desc`**: Describes the list type with `basic_type: List`.
    ///
    /// **Important**: To read `N` complete lists, you must request `N+1` positions in the
    /// range because list boundaries are defined by pairs of consecutive offsets. The last
    /// offset marks the end of the final list.
    ///
    /// ## Struct Fields (StructFieldDecoder)
    ///
    /// For struct and fixed-size list fields that store only presence information:
    ///
    /// - **`values`**: Always empty (`Values::new()`). Struct and fixed-size list fields
    ///   serve as containers and don't store actual values themselves - only presence
    ///   information indicating which logical positions have valid child data.
    /// - **`offsets`**: Always `None`.
    /// - **`presence`**: The core data for these field types. Indicates which logical
    ///   positions contain valid (non-null) struct/list instances. For non-nullable fields,
    ///   this will be `Presence::Trivial(count)`. For nullable fields, this contains
    ///   the actual nullability bitmap as `Presence::Bytes(buffer)`.
    /// - **`type_desc`**: Describes the struct or fixed-size list type.
    ///
    /// # Notes
    ///
    /// - The returned sequence always contains exactly `pos_range.end - pos_range.start`
    ///   logical elements, including both valid values and nulls.
    /// - For variable-length data types that use offsets, null values are represented as
    ///   zero-length entries in the values buffer (consecutive equal offsets).
    /// - The presence information is always meaningful and should be checked to distinguish
    ///   between valid values and nulls, regardless of field type.
    /// - Fixed-size types store placeholder data (typically zeros) for null positions to
    ///   maintain consistent positioning in the values buffer.
    fn read_range(&mut self, pos_range: Range<u64>) -> Result<ValueSequence>;

    /// Reads and decodes the complete storage block that contains the specified logical position.
    ///
    /// This method provides a lower-level interface compared to `read_range`. Instead of
    /// returning a precisely sliced range of values, it returns the entire decoded block
    /// that happens to contain the requested logical position. This avoids the copying
    /// and allocation overhead that `read_range` may incur when slicing and concatenating
    /// multiple blocks into a contiguous result.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position of interest within the field
    ///
    /// # Returns
    ///
    /// A `DecodedBlock` containing:
    /// - **All values** in the storage block that contains the specified position
    /// - **Block metadata** including the logical range (`descriptor.logical_range`)
    ///   that defines which logical positions are covered by this block
    /// - **Storage information** about the block's physical representation
    ///
    /// The requested `position` is guaranteed to lie within `descriptor.logical_range`,
    /// but the block may contain many additional values before and after the position
    /// of interest.
    ///
    /// The `ValueSequence` in the returned `DecodedBlock` follows the same format and
    /// structure as documented for the `read_range` method above, including the field
    /// type-specific layout of `values`, `offsets`, `presence`, and `type_desc` fields.
    ///
    /// # Block Size and Location
    ///
    /// The size and logical range of the returned block depend entirely on the internal
    /// storage representation and encoding details, which are implementation-specific
    /// and should not be relied upon by callers:
    /// - Block boundaries are determined by the storage format and compression settings
    /// - A single block may contain hundreds to thousands of logical values
    /// - Block ranges are not aligned to any particular logical boundaries
    ///
    /// # Performance Characteristics
    ///
    /// This method is optimized for scenarios where:
    /// - **Sparse access patterns**: Accessing individual positions scattered throughout
    ///   the field rather than contiguous ranges
    /// - **Memory efficiency**: Avoiding unnecessary copying when the full block content
    ///   is acceptable or when only a single value is needed
    /// - **Iterator patterns**: Walking through specific positions where each position
    ///   may be processed independently
    ///
    /// For contiguous range access, prefer `read_range` which handles block boundary
    /// crossing and range extraction automatically.
    fn read_containing_block(&mut self, position: u64) -> Result<DecodedBlock>;
}
