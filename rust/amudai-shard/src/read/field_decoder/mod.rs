//! Unified decoder for accessing field data within a stripe.

use std::ops::Range;

use amudai_blockstream::read::block_stream::DecodedBlock;
use amudai_common::{Result, error::Error};
use amudai_format::schema::{BasicType, BasicTypeDescriptor};
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

/// Unified decoder for accessing field data within a stripe.
///
/// `FieldDecoder` serves as the primary interface for reading encoded field data from
/// stripes in the Amudai shard format. It provides a type-safe abstraction over the
/// various field encoding strategies, automatically dispatching to the appropriate
/// specialized decoder based on the field's type and encoding characteristics.
///
/// # Architecture
///
/// The decoder follows a two-level architecture:
/// 1. **Field-level decoding**: This enum handles type dispatch and provides common
///    operations like reader creation and type introspection
/// 2. **Value-level access**: Specialized readers and cursors provide access
///    to the actual field values with different access patterns
///
/// # Supported Field Types
///
/// The decoder supports all major field types in the Amudai schema:
///
/// ## Scalar Types
/// - **Primitive**: Fixed-size numeric types (`Int8`, `Int32`, `Float64`, etc.)
/// - **Boolean**: Bit-packed boolean values
/// - **Bytes**: Variable-length and fixed-length binary data (`String`, `Binary`,
///   `FixedSizeBinary`, `Guid`)
///
/// ## Composite Types
/// - **Struct**: Multi-field composite types with presence information
/// - **List**: Variable-length arrays and maps with offset-based indexing
/// - **Dictionary**: String/binary fields with dictionary encoding for compression
///
/// # Encoding Transparency
///
/// The decoder abstracts away encoding details, providing a consistent interface
/// regardless of the underlying storage format:
/// - **Compression**: Automatically handles block-compressed data
/// - **Dictionary encoding**: Transparently decodes dictionary-compressed fields
/// - **Null handling**: Manages presence/absence information for nullable fields
/// - **Block structure**: Manages access across block boundaries
///
/// # Access Patterns
///
/// `FieldDecoder` supports multiple access patterns for different use cases:
///
/// ## Range-based Access
/// Use [`create_reader_with_ranges`](Self::create_reader_with_ranges) when:
/// - Reading contiguous ranges of values
/// - Performing sequential scans
/// - Processing large batches of data
///
/// ## Position-based Access
/// Use [`create_reader_with_positions`](Self::create_reader_with_positions) when:
/// - Reading sparse, non-contiguous positions
/// - Following index-based lookups
/// - Implementing filtered queries
///
/// ## Iterator-style Cursors
/// Use specialized cursors for:
/// - Forward-only iteration with position hints
/// - Memory-efficient streaming access
/// - Type-safe value extraction without boxing
///
/// # Performance Considerations
///
/// - **Prefetching**: Position and range hints enable better I/O prefetching
/// - **Caching**: Readers maintain block-level caches to minimize repeated I/O
/// - **Zero-copy**: Where possible, values are accessed directly from decoded blocks
///
/// # Thread Safety
///
/// `FieldDecoder` is `Clone` and can be shared across threads. Each clone maintains
/// independent reader state, allowing concurrent access to the same field data
/// from multiple threads without coordination.
#[derive(Clone)]
pub enum FieldDecoder {
    /// Decoder for fixed-size primitive fields.
    ///
    /// Handles numeric and fixed-size data types including integers (`Int8`, `Int16`,
    /// `Int32`, `Int64`), floating-point numbers (`Float32`, `Float64`), and temporal
    /// data (`DateTime`). These fields store values in densely-packed arrays with
    /// optional run-length encoding and compression.
    Primitive(PrimitiveFieldDecoder),

    /// Decoder for boolean fields with bit-level packing.
    ///
    /// Handles boolean storage using bit-packed encoding where each boolean value
    /// consumes one bit.
    Boolean(BooleanFieldDecoder),

    /// Decoder for variable-length binary and string fields.
    ///
    /// Handles text and binary data types including variable-length strings (`String`),
    /// binary data (`Binary`), fixed-size binary arrays (`FixedSizeBinary`), and
    /// globally unique identifiers (`Guid`). Uses offset arrays to manage variable-length
    /// data.
    Bytes(BytesFieldDecoder),

    /// Decoder for dictionary-encoded fields.
    ///
    /// Provides transparent access to fields that use dictionary compression, where
    /// frequently repeated values are stored once in a dictionary and referenced by
    /// smaller integer indices.
    Dictionary(DictionaryFieldDecoder),

    /// Decoder for list and map fields with dynamic sizing.
    ///
    /// Manages variable-length arrays (`List`) and key-value collections (`Map`)
    /// using offset-based indexing. Each list/map is defined by start and end offsets
    /// that reference positions in child element arrays, enabling storage of nested
    /// and hierarchical data structures.
    List(ListFieldDecoder),

    /// Decoder for struct and fixed-size list fields.
    ///
    /// Primarily manages presence information (nullability) since the actual field values
    /// are stored in child fields.
    Struct(StructFieldDecoder),
}

impl FieldDecoder {
    /// Returns the basic type descriptor for this field.
    ///
    /// The `BasicTypeDescriptor` provides detailed information about the field's
    /// data type, including:
    /// - The fundamental type classification (e.g., `Int32`, `String`, `List`)
    /// - Size information for fixed-size types
    /// - Type-specific metadata
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

    /// Creates a reader for accessing ranges of values in this field.
    ///
    /// This method is designed for **contiguous access patterns** where you need to read
    /// sequential ranges of field values.
    ///
    /// # Access Pattern Optimization
    ///
    /// The range hints enable several optimizations:
    /// - Prefetching: Storage blocks covering the ranges are loaded proactively
    /// - I/O coalescing: Adjacent ranges may be read together to reduce I/O operations
    ///
    /// # Compared to Position-based Access
    ///
    /// Use this method when:
    /// - Reading contiguous ranges (e.g., rows 1000-2000, 5000-6000)
    /// - Processing data in sequential order
    /// - Performing full or partial table scans
    ///
    /// Use [`create_reader_with_positions`](Self::create_reader_with_positions) when:
    /// - Reading sparse, non-contiguous positions (e.g., rows 10, 157, 942)
    /// - Following index lookups or hash joins
    /// - Processing filtered result sets
    ///
    /// # Arguments
    ///
    /// * `pos_ranges_hint` - An iterator of logical position ranges (`Range<u64>`) that
    ///   are likely to be accessed. The ranges need to be sorted and non-overlapping.
    ///   Empty ranges hint (e.g. [`std::iter::empty()`]) is allowed.
    ///
    /// # Returns
    ///
    /// A boxed [`FieldReader`] that can access field values within the specified
    /// ranges.
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

    /// Creates a reader for accessing specific positions in this field.
    ///
    /// This method is optimized for **sparse access patterns** where you need to read
    /// individual values at specific, potentially non-contiguous positions.
    ///
    /// # Position Ordering Requirements
    ///
    /// **Important**: The positions must be provided in **non-descending order**
    /// (i.e., sorted). Positions may be duplicated, but they must not decrease.
    ///
    /// # Arguments
    ///
    /// * `positions_hint` - An iterator of logical positions (0-based row indices) in
    ///   **non-descending order**. Duplicates are allowed but the sequence must never
    ///   decrease. Position values must be less than the field's total position count.
    ///   Empty position hint (e.g. [`std::iter::empty()`]) is allowed.
    ///
    /// # Returns
    ///
    /// A boxed [`FieldReader`] optimized for accessing the specified positions.
    /// The reader pre-plans its access strategy based on the position distribution.
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

    /// Creates a specialized cursor for iterator-style access to binary data.
    ///
    /// `BytesFieldCursor` provides optimized access to binary values through a forward-moving
    /// access pattern with sparse position requests. This approach is both more convenient
    /// and more performant than using `FieldReader` to read entire position ranges when
    /// the access pattern involves reading individual values at specific positions.
    ///
    /// # Supported Field Types
    ///
    /// This cursor supports:
    /// - `Binary` - Variable-length binary data
    /// - `String` - Variable-length string data
    /// - `FixedSizeBinary` - Fixed-length binary data
    /// - All fixed-size primitive types (integers, floating-point numbers, GUIDs, etc.)
    ///
    /// When `BytesFieldCursor` is used with a primitive type field, the values are returned
    /// as byte slices with the appropriate length. In general, using `PrimitiveFieldCursor<T>`
    /// is preferable for those scenarios, but `BytesFieldCursor` is also supported.
    ///
    /// # Arguments
    ///
    /// * `positions_hint` - An iterator of non-descending logical positions that are likely
    ///   to be accessed. These hints are used to optimize prefetching strategies for better
    ///   performance when reading from storage. The positions must be in non-descending order
    ///   but don't need to be unique or contiguous.
    ///
    /// # Returns
    ///
    /// A specialized `BytesFieldCursor` configured for accessing binary data at the specified
    /// positions.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The field type is not compatible with binary cursors
    /// - The underlying reader creation fails
    /// - I/O errors occur during initialization
    pub fn create_bytes_cursor(
        &self,
        positions_hint: impl Iterator<Item = u64> + Clone,
    ) -> Result<bytes::BytesFieldCursor> {
        let basic_type = self.basic_type();
        match basic_type.basic_type {
            BasicType::Boolean | BasicType::Binary | BasicType::String => (),
            _ => {
                basic_type
                    .primitive_size()
                    .ok_or_else(|| Error::invalid_operation("create_bytes_cursor"))?;
            }
        }

        let reader = self.create_reader_with_positions(positions_hint)?;
        Ok(bytes::BytesFieldCursor::new(reader, basic_type))
    }

    /// Creates a specialized cursor for iterator-style access to string data.
    ///
    /// `StringFieldCursor` provides optimized access to string values through a forward-moving
    /// access pattern with sparse position requests. This approach is both more convenient
    /// and more performant than using `FieldReader` to read entire position ranges when
    /// the access pattern involves reading individual values at specific positions.
    ///
    /// This cursor supports `String` basic type - variable-length string data.
    ///
    /// # Arguments
    ///
    /// * `positions_hint` - An iterator of non-descending logical positions that are likely
    ///   to be accessed. These hints are used to optimize prefetching strategies for better
    ///   performance when reading from storage. The positions must be in non-descending order
    ///   but don't need to be unique or contiguous.
    ///
    /// # Returns
    ///
    /// A specialized `StringFieldCursor` configured for accessing string data at the specified
    /// positions.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The field type is not compatible with string cursor
    /// - The underlying reader creation fails
    /// - I/O errors occur during initialization
    pub fn create_string_cursor(
        &self,
        positions_hint: impl Iterator<Item = u64> + Clone,
    ) -> Result<bytes::StringFieldCursor> {
        let basic_type = self.basic_type();
        if !matches!(basic_type.basic_type, BasicType::String) {
            return Err(Error::invalid_operation("create_string_cursor"));
        }
        let reader = self.create_reader_with_positions(positions_hint)?;
        Ok(bytes::StringFieldCursor::new(reader, basic_type))
    }

    /// Creates a specialized cursor for iterator-style access to typed primitive values.
    ///
    /// `PrimitiveFieldCursor<T>` provides optimized access to primitive values through
    /// a forward-moving access pattern with sparse position requests. This approach is
    /// both more convenient and more performant than using `FieldReader` to read entire
    /// position ranges when the access pattern involves reading individual values at
    /// specific positions.
    ///
    /// # Type Parameter
    ///
    /// * `T` - The primitive type of the values in the field. Must implement `bytemuck::AnyBitPattern`
    ///   for safe casting from raw bytes. The size of `T` must match the field's primitive size.
    ///
    /// # Supported Field Types
    ///
    /// This cursor supports all fixed-size primitive types:
    /// - Integer types: `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `u64`
    /// - Floating-point types: `f32`, `f64`
    /// - Other primitive types like GUIDs and fixed-size binary data
    ///
    /// # Arguments
    ///
    /// * `positions_hint` - An iterator of non-descending logical positions that are likely
    ///   to be accessed. These hints are used to optimize prefetching strategies for better
    ///   performance when reading from storage. The positions must be in non-descending order
    ///   but don't need to be unique or contiguous.
    ///
    /// # Returns
    ///
    /// A specialized `PrimitiveFieldCursor<T>` configured for accessing typed primitive data
    /// at the specified positions.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The field type is not a fixed-size primitive type
    /// - The size of type `T` doesn't match the field's primitive size
    /// - The underlying reader creation fails
    /// - I/O errors occur during initialization
    pub fn create_primitive_cursor<T>(
        &self,
        positions_hint: impl Iterator<Item = u64> + Clone,
    ) -> Result<primitive::PrimitiveFieldCursor<T>> {
        let basic_type = self.basic_type();
        if basic_type.basic_type == BasicType::Boolean {
            if std::mem::size_of::<T>() != 1 {
                return Err(Error::invalid_operation(
                    "create_primitive_cursor: bool value size mismatch",
                ));
            }
        } else {
            let value_size = basic_type
                .primitive_size()
                .ok_or_else(|| Error::invalid_operation("create_primitive_cursor"))?;
            if value_size != std::mem::size_of::<T>() {
                return Err(Error::invalid_operation(
                    "create_primitive_cursor: value size mismatch",
                ));
            }
        }
        let reader = self.create_reader_with_positions(positions_hint)?;
        Ok(primitive::PrimitiveFieldCursor::new(reader, basic_type))
    }

    /// Creates a specialized cursor for iterator-style access to list offset ranges.
    ///
    /// `ListFieldCursor` provides optimized access to list and map offset ranges through a
    /// forward-moving access pattern with sparse position requests. This approach is both more
    /// convenient and more performant than using `FieldReader` to read entire position ranges
    /// when the access pattern involves reading individual list boundaries at specific positions.
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
    /// # Supported Field Types
    ///
    /// This cursor supports:
    /// - `List` - Variable-length lists of homogeneous elements
    /// - `Map` - Key-value pair collections (encoded similarly to lists)
    ///
    /// # Arguments
    ///
    /// * `positions_hint` - An iterator of non-descending logical positions that are likely
    ///   to be accessed. These hints are used to optimize prefetching strategies for better
    ///   performance when reading from storage. The positions must be in non-descending order
    ///   but don't need to be unique or contiguous.
    ///
    /// # Returns
    ///
    /// A specialized `ListFieldCursor` configured for accessing list offset ranges at the
    /// specified positions.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The field type is not `List` or `Map`
    /// - The underlying reader creation fails
    /// - I/O errors occur during initialization
    pub fn create_list_cursor(
        &self,
        positions_hint: impl Iterator<Item = u64> + Clone,
    ) -> Result<list::ListFieldCursor> {
        let basic_type = self.basic_type();
        if !matches!(basic_type.basic_type, BasicType::List | BasicType::Map) {
            return Err(Error::invalid_operation("create_list_cursor"));
        }
        let reader = self.create_reader_with_positions(positions_hint)?;
        Ok(list::ListFieldCursor::new(reader, basic_type))
    }

    /// Creates a specialized cursor for iterator-style access to struct and fixed-size list
    /// presence information.
    ///
    /// `StructFieldCursor` provides optimized access to nullability information for composite
    /// data types through a forward-moving access pattern with sparse position requests. Unlike
    /// other field cursors that read actual values, this cursor only provides access to presence
    /// information since struct and fixed-size list fields serve as containers that indicate
    /// which logical positions have valid child data, rather than storing values themselves.
    ///
    /// # Presence Information
    ///
    /// Struct and fixed-size list fields store presence (nullability) information that indicates
    /// whether child fields contain valid data at each logical position. This cursor provides
    /// efficient access to this presence information through null checking methods.
    ///
    /// For non-nullable fields, all positions are implicitly valid and null checks will always
    /// return `false`. For nullable fields, the presence information is read from bit buffers
    /// stored in the stripe format.
    ///
    /// # Supported Field Types
    ///
    /// This cursor supports:
    /// - `Struct` - Multi-field composite types with named child fields
    /// - `FixedSizeList` - Fixed-length array types with homogeneous elements
    ///
    /// # Arguments
    ///
    /// * `positions_hint` - An iterator of non-descending logical positions that are likely
    ///   to be accessed. These hints are used to optimize prefetching strategies for better
    ///   performance when reading from storage. The positions must be in non-descending order
    ///   but don't need to be unique or contiguous.
    ///
    /// # Returns
    ///
    /// A specialized `StructFieldCursor` configured for accessing presence information at the
    /// specified positions.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The field type is not `Struct` or `FixedSizeList`
    /// - The underlying reader creation fails
    /// - I/O errors occur during initialization
    pub fn create_struct_cursor(
        &self,
        positions_hint: impl Iterator<Item = u64> + Clone,
    ) -> Result<unit::StructFieldCursor> {
        let basic_type = self.basic_type();
        if !matches!(
            basic_type.basic_type,
            BasicType::Struct | BasicType::FixedSizeList
        ) {
            return Err(Error::invalid_operation("create_struct_cursor"));
        }
        let reader = self.create_reader_with_positions(positions_hint)?;
        Ok(unit::StructFieldCursor::new(reader, basic_type))
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
