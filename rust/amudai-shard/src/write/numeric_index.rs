//! Numeric range index implementation for the Amudai blockstream format.
//!
//! This module provides the unified implementation for building numeric range indexes
//! as specified in Chapter 8 of the Amudai specification. The indexes enable efficient
//! range-based queries by maintaining min/max values for blocks of numeric data.
//!
//! ## Supported Types
//!
//! The numeric index system supports the following data types:
//! - **Integer types**: `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `u64`
//! - **Floating-point types**: `f32`, `f64`
//! - **DateTime**: Stored as `u64` representing ticks
//! - **Decimal**: `d128` stored as `FixedSizeBinary(16)` with `KustoDecimal` extension type
//!
//! ## Usage
//!
//! Builders return `Some(PreparedEncodedBuffer)` when data is processed, or `None`
//! when no data is provided (indicating no index should be created).

use std::sync::Arc;

use amudai_blockstream::write::PreparedEncodedBuffer;
use amudai_common::{Result, error::Error};
use amudai_encodings::{
    block_encoder::{
        BlockChecksum, BlockEncoder, BlockEncodingParameters, BlockEncodingPolicy,
        BlockEncodingProfile, PresenceEncoding,
    },
    primitive_block_encoder::PrimitiveBlockEncoder,
};
use amudai_format::defs::{
    schema::BasicType,
    schema_ext::{BasicTypeDescriptor, KnownExtendedType},
    shard,
};
use amudai_io::temp_file_store::TemporaryFileStore;

use arrow_array::{
    Array, ArrowPrimitiveType, PrimitiveArray,
    cast::AsArray,
    types::{
        Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    },
};

/// Default logical block size for numeric indexes (typically 256 values per block)
pub const DEFAULT_LOGICAL_BLOCK_SIZE: usize = 256;

/// Size of the numeric range index header in bytes
pub const HEADER_SIZE: usize = 40;

/// A trait for building numeric range indexes for different data types.
///
/// This trait provides a unified interface for processing arrays of numeric data
/// and constructing efficient range indexes. The implementation maintains min/max
/// values for logical blocks of data to enable fast range-based queries.
///
/// # Type Safety
///
/// The trait is object-safe and thread-safe, requiring `Send + Sync + 'static`
/// bounds to ensure it can be used across thread boundaries safely.
///
/// # Usage
///
/// Implementations of this trait are created via the `create_builder` function
/// which returns the appropriate builder for a given basic type.
pub trait NumericIndexBuilder: Send + Sync + 'static {
    /// Process an array of values and incorporate them into the index.
    ///
    /// This method processes the provided Arrow array, extracting min/max values
    /// for each logical block and tracking invalid counts. The array must be compatible
    /// with the builder's expected type.
    ///
    /// # Arguments
    ///
    /// * `array` - An Arrow array containing the values to index
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the array type is incompatible
    /// or if processing fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use amudai_shard::write::numeric_index::NumericIndexBuilder;
    /// # fn example(mut builder: Box<dyn NumericIndexBuilder>, int32_array: arrow_array::Int32Array) -> amudai_common::Result<()> {
    /// builder.process_array(&int32_array)?;
    /// # Ok(())
    /// # }
    /// ```
    fn process_array(&mut self, array: &dyn Array) -> Result<()>;

    /// Process a sequence of invalid values.
    ///
    /// This method allows efficient processing of known invalid values without
    /// requiring a full Arrow array. It's useful when the caller knows that
    /// a certain number of consecutive values are invalid (null or NaN for floating-point types).
    ///
    /// # Arguments
    ///
    /// * `count` - The number of invalid values to process
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if processing fails.
    fn process_invalid(&mut self, count: usize) -> Result<()>;

    /// Finalize the index and return the encoded buffer.
    ///
    /// This method completes the index construction process, encoding all
    /// accumulated min/max values and invalid counts into the final buffer format
    /// specified in Chapter 8 of the Amudai specification.
    ///
    /// # Returns
    ///
    /// Returns `Some(PreparedEncodedBuffer)` containing the complete numeric range
    /// index if there is data to index, or `None` if no data was processed.
    ///
    /// # Note
    ///
    /// This method consumes the builder, as it represents the final step in
    /// the index construction process.
    fn finish(self: Box<Self>) -> Result<Option<PreparedEncodedBuffer>>;
}

/// Creates a numeric index builder for the specified data type.
///
/// This factory function returns the appropriate builder implementation based on
/// the provided basic type. It supports all numeric types defined in the Amudai
/// specification, including integers, floating-point numbers, DateTime values,
/// and decimal (d128) values.
///
/// For integer types, the `signed` field in the `BasicTypeDescriptor` determines
/// whether the type should be treated as signed or unsigned:
/// - `BasicType::Int8` with `signed: true` → `i8` builder
/// - `BasicType::Int8` with `signed: false` → `u8` builder  
/// - `BasicType::Int32` with `signed: true` → `i32` builder
/// - `BasicType::Int32` with `signed: false` → `u32` builder
/// - And so on for Int16 and Int64
///
/// For decimal types:
/// - `BasicType::FixedSizeBinary` with `fixed_size: 16` and `extended_type: KustoDecimal` → decimal (d128) builder
///
/// # Arguments
///
/// * `basic_type` - The basic type descriptor specifying the data type to index
/// * `encoding_profile` - The encoding profile to use for compressing index data
/// * `temp_store` - Temporary file storage for intermediate processing
///
/// # Returns
///
/// Returns a boxed builder implementing `NumericIndexBuilder` for the specified type,
/// or an error if the type is not supported for numeric indexing. When `finish()` is
/// called on the builder, it will return `Some(PreparedEncodedBuffer)` if data was
/// processed, or `None` if no data was provided.
///
/// # Supported Types
///
/// - `BasicType::Int8`, `BasicType::Int16`, `BasicType::Int32`, `BasicType::Int64` (signed and unsigned variants)
/// - `BasicType::Float32`, `BasicType::Float64`
/// - `BasicType::DateTime` (stored as `u64` ticks)
/// - `BasicType::FixedSizeBinary` with `fixed_size: 16` and `extended_type: KustoDecimal` (decimal d128)
///
/// # Errors
///
/// Returns an error if the basic type is not suitable for numeric range indexing.
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use amudai_format::defs::{schema::BasicType, schema_ext::BasicTypeDescriptor};
/// use amudai_encodings::block_encoder::BlockEncodingProfile;
/// use amudai_io::temp_file_store::TemporaryFileStore;
/// use amudai_shard::write::numeric_index::create_builder;
///
/// # fn example() -> amudai_common::Result<()> {
/// # let temp_store: Arc<dyn TemporaryFileStore> = todo!();
/// // For signed i32
/// let builder = create_builder(
///     BasicTypeDescriptor {
///         basic_type: BasicType::Int32,
///         signed: true,
///         fixed_size: 0,
///         extended_type: Default::default(),
///     },
///     BlockEncodingProfile::default(),
///     temp_store.clone()
/// )?;
///
/// // For unsigned u32
/// let builder = create_builder(
///     BasicTypeDescriptor {
///         basic_type: BasicType::Int32,
///         signed: false,
///         fixed_size: 0,
///         extended_type: Default::default(),
///     },
///     BlockEncodingProfile::default(),
///     temp_store
/// )?;
///
/// // ... process data ...
/// if let Some(index_buffer) = builder.finish()? {
///     // Use the index buffer
/// } else {
///     // No data was processed, no index created
/// }
/// # Ok(())
/// # }
/// ```
pub fn create_builder(
    basic_type: BasicTypeDescriptor,
    encoding_profile: BlockEncodingProfile,
    temp_store: Arc<dyn TemporaryFileStore>,
) -> Result<Box<dyn NumericIndexBuilder>> {
    // Check if this is a decimal type
    if basic_type.basic_type == BasicType::FixedSizeBinary
        && basic_type.fixed_size == 16
        && basic_type.extended_type == KnownExtendedType::KustoDecimal
    {
        return Ok(Box::new(super::decimal_index::DecimalIndexBuilder::new(
            temp_store,
            encoding_profile,
        )));
    }

    let builder: Box<dyn NumericIndexBuilder> = match basic_type.basic_type {
        BasicType::Int8 => {
            if basic_type.signed {
                Box::new(TypedNumericIndexBuilder::<I8Indexable>::new(
                    temp_store,
                    encoding_profile,
                ))
            } else {
                Box::new(TypedNumericIndexBuilder::<U8Indexable>::new(
                    temp_store,
                    encoding_profile,
                ))
            }
        }
        BasicType::Int16 => {
            if basic_type.signed {
                Box::new(TypedNumericIndexBuilder::<I16Indexable>::new(
                    temp_store,
                    encoding_profile,
                ))
            } else {
                Box::new(TypedNumericIndexBuilder::<U16Indexable>::new(
                    temp_store,
                    encoding_profile,
                ))
            }
        }
        BasicType::Int32 => {
            if basic_type.signed {
                Box::new(TypedNumericIndexBuilder::<I32Indexable>::new(
                    temp_store,
                    encoding_profile,
                ))
            } else {
                Box::new(TypedNumericIndexBuilder::<U32Indexable>::new(
                    temp_store,
                    encoding_profile,
                ))
            }
        }
        BasicType::Int64 => {
            if basic_type.signed {
                Box::new(TypedNumericIndexBuilder::<I64Indexable>::new(
                    temp_store,
                    encoding_profile,
                ))
            } else {
                Box::new(TypedNumericIndexBuilder::<U64Indexable>::new(
                    temp_store,
                    encoding_profile,
                ))
            }
        }
        BasicType::Float32 => Box::new(TypedNumericIndexBuilder::<F32Indexable>::new(
            temp_store,
            encoding_profile,
        )),
        BasicType::Float64 => Box::new(TypedNumericIndexBuilder::<F64Indexable>::new(
            temp_store,
            encoding_profile,
        )),
        BasicType::DateTime => Box::new(TypedNumericIndexBuilder::<DateTimeIndexable>::new(
            temp_store,
            encoding_profile,
        )),
        _ => {
            return Err(Error::invalid_arg(
                "basic_type",
                format!(
                    "Unsupported type for numeric index: {:?}",
                    basic_type.basic_type
                ),
            ));
        }
    };

    Ok(builder)
}

/// 40-byte header for numeric range index buffers (Chapter 8 compliant)
///
/// This header structure defines the layout of numeric range index files according
/// to the Amudai specification. It provides metadata about the index contents and
/// enables efficient parsing and validation.
///
/// # Memory Layout
///
/// The header has a fixed 40-byte layout with the following fields:
/// - `version` (2 bytes): Format version number (currently 1)
/// - `basic_type` (1 byte): Amudai BasicType identifier  
/// - `has_checksum` (1 byte): Boolean flag for checksum presence
/// - `logical_position_span` (8 bytes): Total number of values covered
/// - `payload0_size` (8 bytes): Size of min_values buffer + checksum
/// - `payload1_size` (8 bytes): Size of max_values buffer + checksum  
/// - `payload2_size` (8 bytes): Size of invalid_counts buffer + checksum
/// - `logical_block_size` (2 bytes): Values per logical block
/// - `padding` (2 bytes): Reserved space for future use
///
/// # Alignment
///
/// The complete index buffer (header + payloads) is aligned to 64-byte boundaries
/// for optimal memory access patterns.
pub struct NumericRangeIndexHeader {
    pub version: u16,               // Version number (currently 1) - [0..2]
    pub basic_type: u8,             // Amudai BasicType identifier - [2]
    pub has_checksum: u8,           // Boolean: 1 if checksum present, 0 if not - [3]
    pub logical_position_span: u64, // Total values covered by this index - [4..12]
    pub payload0_size: u64,         // Size of encoded min_values + optional checksum - [12..20]
    pub payload1_size: u64,         // Size of encoded max_values + optional checksum - [20..28]
    pub payload2_size: u64,         // Size of encoded invalid_counts + optional checksum - [28..36]
    pub logical_block_size: u16,    // Values per block (typically 256) - [36..38]
    pub padding: u16,               // Reserved for future use, must be 0 - [38..40]
}

impl NumericRangeIndexHeader {
    /// Convert header to 40-byte array
    ///
    /// Serializes the header structure into a byte array using little endian format.
    /// This method ensures the header can be written directly to storage while
    /// maintaining the exact field layout specified in the format documentation.
    ///
    /// # Returns
    ///
    /// A 40-byte array containing the serialized header data.
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut bytes = [0u8; HEADER_SIZE];
        bytes[0..2].copy_from_slice(&self.version.to_le_bytes());
        bytes[2] = self.basic_type;
        bytes[3] = self.has_checksum;
        bytes[4..12].copy_from_slice(&self.logical_position_span.to_le_bytes());
        bytes[12..20].copy_from_slice(&self.payload0_size.to_le_bytes());
        bytes[20..28].copy_from_slice(&self.payload1_size.to_le_bytes());
        bytes[28..36].copy_from_slice(&self.payload2_size.to_le_bytes());
        bytes[36..38].copy_from_slice(&self.logical_block_size.to_le_bytes());
        bytes[38..40].copy_from_slice(&self.padding.to_le_bytes());
        bytes
    }
}

/// Generic numeric index builder implementation for a specific data type.
///
/// This struct provides the core implementation of numeric range indexing for
/// a particular data type `T`. It accumulates min/max values and invalid counts
/// across logical blocks of data, then encodes them into the final index format.
///
/// # Type Parameter
///
/// * `T` - A type implementing `NumericIndexable` that defines the specific
///   numeric type being indexed (e.g., i32, f64, DateTime)
///
/// # Design
///
/// The builder processes data in logical blocks of `DEFAULT_LOGICAL_BLOCK_SIZE`
/// values (typically 256). For each block, it tracks:
/// - Minimum non-null, non-NaN value
/// - Maximum non-null, non-NaN value  
/// - Count of null values
///
/// When a block is complete, these statistics are stored and a new block begins.
/// The final index contains arrays of these per-block statistics.
struct TypedNumericIndexBuilder<T: NumericIndexable> {
    min_values: Vec<<T::ArrowPrimitive as ArrowPrimitiveType>::Native>,
    max_values: Vec<<T::ArrowPrimitive as ArrowPrimitiveType>::Native>,
    invalid_counts: Vec<u16>,
    current_block: NumericIndexBlock<T>,
    temp_store: Arc<dyn TemporaryFileStore>,
    encoding_profile: BlockEncodingProfile,
    logical_position_span: u64,
}

impl<T: NumericIndexable> TypedNumericIndexBuilder<T> {
    /// Creates a new numeric index builder for type T.
    ///
    /// Initializes the builder with empty collections and prepares it to process
    /// data of the specified type.
    ///
    /// # Arguments
    ///
    /// * `temp_store` - Temporary file storage for intermediate processing
    /// * `encoding_profile` - Profile defining how to encode the index data
    ///
    /// # Returns
    ///
    /// A new builder ready to process numeric data.
    fn new(
        temp_store: Arc<dyn TemporaryFileStore>,
        encoding_profile: BlockEncodingProfile,
    ) -> Self {
        TypedNumericIndexBuilder {
            min_values: Vec::with_capacity(128),
            max_values: Vec::with_capacity(128),
            invalid_counts: Vec::with_capacity(128),
            current_block: Default::default(),
            temp_store,
            encoding_profile,
            logical_position_span: 0,
        }
    }

    /// Finalizes the current logical block and adds its statistics to the index.
    ///
    /// This method is called when a logical block reaches its capacity. It extracts
    /// the min/max values and null count from the current block, adds them to the
    /// respective arrays, and resets the current block for new data.
    ///
    /// If the current block has no data, this method does nothing.
    fn finalize_current_block(&mut self) {
        if self.current_block.processed_count > 0 {
            let (min_val, max_val, invalid_count) =
                std::mem::take(&mut self.current_block).finalize();
            self.min_values.push(min_val);
            self.max_values.push(max_val);
            self.invalid_counts.push(invalid_count);
        }
    }

    /// Encodes a byte slice and writes it directly to a temporary buffer at the specified position.
    ///
    /// This helper method provides an optimized encoding path for byte data by writing directly
    /// to the target buffer instead of creating intermediate allocations. It uses the
    /// `PrimitiveBlockEncoder` infrastructure to compress the data according to the specified
    /// encoding policy.
    ///
    /// # Process
    ///
    /// 1. Creates a U8 basic type descriptor for byte-level encoding
    /// 2. Initializes a `PrimitiveBlockEncoder` with the provided encoding policy
    /// 3. Converts the input bytes to an Arrow `UInt8Array` for processing
    /// 4. Analyzes a sample (up to 1024 bytes) to determine optimal encoding parameters
    /// 5. Encodes the full byte array using the determined settings
    /// 6. Writes the encoded data directly to the buffer at the specified position
    ///
    /// # Arguments
    ///
    /// * `encoding_policy` - The encoding policy specifying compression and checksum settings
    /// * `buffer` - Mutable reference to the temporary buffer where encoded data will be written
    /// * `pos` - The byte offset within the buffer where writing should begin
    /// * `data` - The raw byte slice to encode and write
    ///
    /// # Returns
    ///
    /// Returns the number of bytes written to the buffer (length of encoded data),
    /// or an error if encoding or writing fails.
    ///
    /// # Performance Notes
    ///
    /// - Samples only the first 1024 bytes for analysis to balance encoding quality with performance
    /// - Writes directly to the buffer to avoid intermediate allocations and copies
    /// - The encoding includes optional checksums as specified by the encoding policy
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Sample analysis fails
    /// - Encoding the data fails
    /// - Writing to the buffer fails (e.g., insufficient space, I/O error)
    fn encode_bytes(
        &self,
        encoding_policy: &BlockEncodingPolicy,
        buffer: &mut dyn amudai_io::temp_file_store::TemporaryBuffer,
        pos: u64,
        data: &[u8],
    ) -> Result<u64> {
        // Create U8 basic type descriptor for byte encoding
        let u8_basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Int8,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };

        // Create encoder
        let mut encoder =
            PrimitiveBlockEncoder::new(encoding_policy.clone(), u8_basic_type, Default::default());

        // Convert bytes to Arrow array
        let array = arrow_array::UInt8Array::from(data.to_vec());

        // Analyze sample (up to 1024 bytes for efficiency)
        let sample_size = std::cmp::min(1024, array.len());
        let sample_array = array.slice(0, sample_size);
        encoder.analyze_sample(&sample_array)?;

        // Encode the full array
        let encoded = encoder.encode(&array)?;

        buffer.write_at(pos, encoded.as_ref())?;

        Ok(encoded.as_ref().len() as u64)
    }

    /// Encodes the accumulated index data into the final buffer format.
    ///
    /// This method performs the final encoding step, converting the collected
    /// min/max values and invalid counts into compressed buffers according to the
    /// Amudai specification. The result is a single buffer containing:
    /// 1. 40-byte header with metadata
    /// 2. Encoded min_values buffer with checksum
    /// 3. Encoded max_values buffer with checksum  
    /// 4. Encoded invalid_counts buffer with checksum (if any non-zero counts)
    ///
    /// # Optimizations
    ///
    /// - If all invalid counts are zero, the invalid_counts buffer is omitted entirely
    /// - All buffers are compressed using the specified encoding profile
    /// - The final result is aligned to 64-byte boundaries
    /// - Memory allocation is optimized by encoding directly into the final buffer
    ///   when possible to avoid intermediate copies
    ///
    /// # Returns
    ///
    /// A `PreparedEncodedBuffer` containing the complete encoded index.
    fn encode_index(&self) -> Result<PreparedEncodedBuffer> {
        // Writer for the result
        let mut writer = self.temp_store.allocate_buffer(None)?;

        // Create encoding policy with checksums enabled
        let encoding_policy = BlockEncodingPolicy {
            parameters: BlockEncodingParameters {
                checksum: BlockChecksum::Enabled,
                presence: PresenceEncoding::Disabled,
            },
            profile: self.encoding_profile,
            size_constraints: Default::default(),
        };

        let mut size = HEADER_SIZE as u64;
        // Convert values to bytes for encoding
        let payload0_size = self.encode_bytes(
            &encoding_policy,
            writer.as_mut(),
            size,
            T::as_bytes(&self.min_values),
        )?;
        size += payload0_size;

        let payload1_size = self.encode_bytes(
            &encoding_policy,
            writer.as_mut(),
            size,
            T::as_bytes(&self.max_values),
        )?;
        size += payload1_size;

        // Check if all invalid_counts are zero (optimization)
        let all_invalid_are_zero = self.invalid_counts.iter().all(|&count| count == 0);
        let payload2_size: u64 = if all_invalid_are_zero {
            0
        } else {
            let invalid_bytes = bytemuck::cast_slice::<u16, u8>(&self.invalid_counts);
            self.encode_bytes(&encoding_policy, writer.as_mut(), size, invalid_bytes)?
        };
        size += payload2_size;

        // Create the index header
        let header = NumericRangeIndexHeader {
            version: 1,
            basic_type: T::type_id() as u8,
            has_checksum: 1,
            logical_position_span: self.logical_position_span,
            payload0_size,
            payload1_size,
            payload2_size,
            logical_block_size: DEFAULT_LOGICAL_BLOCK_SIZE as u16,
            padding: 0,
        };

        writer.write_at(0, &header.to_bytes())?;

        let aligned_size = size.next_multiple_of(64);
        // Append padding if needed to align to 64-byte boundary
        if aligned_size > size {
            let padding_size = aligned_size - size;
            let padding = vec![0u8; padding_size as usize];
            writer.write_at(size, &padding)?;
        }

        let descriptor = shard::EncodedBuffer {
            kind: shard::BufferKind::RangeIndex as i32,
            embedded_presence: false,
            embedded_offsets: false,
            buffer: None,
            block_map: None,
            block_count: Some(0),
            block_checksums: false,
            buffer_id: Some(0),
            packed_group_index: Some(0),
        };

        Ok(PreparedEncodedBuffer {
            descriptor,
            data: writer.into_read_at()?,
            data_size: aligned_size,
            block_map_size: 0,
        })
    }
}

impl<T: NumericIndexable> NumericIndexBuilder for TypedNumericIndexBuilder<T> {
    fn process_array(&mut self, array: &dyn Array) -> Result<()> {
        let array = array.as_primitive::<T::ArrowPrimitive>();
        let mut array_offset = 0;

        self.logical_position_span += array.len() as u64;

        while array_offset < array.len() {
            let available_in_block = self.current_block.remaining_len();
            let to_process = std::cmp::min(available_in_block, array.len() - array_offset);

            self.current_block
                .process_values(array, array_offset, to_process);
            array_offset += to_process;

            if self.current_block.remaining_len() == 0 {
                self.finalize_current_block();
            }
        }

        Ok(())
    }

    fn process_invalid(&mut self, count: usize) -> Result<()> {
        let mut remaining_invalid = count;
        self.logical_position_span += count as u64;

        while remaining_invalid > 0 {
            let available_in_block = self.current_block.remaining_len();
            let to_process = std::cmp::min(available_in_block, remaining_invalid);

            self.current_block.process_invalid(to_process);
            remaining_invalid -= to_process;

            if self.current_block.remaining_len() == 0 {
                self.finalize_current_block();
            }
        }

        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<Option<PreparedEncodedBuffer>> {
        // Finalize any remaining partial block
        self.finalize_current_block();

        // If no data was processed, return None
        if self.logical_position_span == 0 {
            return Ok(None);
        }

        // Check if all values are invalid by summing invalid counts
        let total_invalid_count: u64 = self.invalid_counts.iter().map(|&count| count as u64).sum();
        if total_invalid_count == self.logical_position_span {
            return Ok(None);
        }

        // Encode the index using the three-buffer format
        Ok(Some(self.encode_index()?))
    }
}

/// Represents a logical block of numeric values being processed for indexing.
///
/// This struct accumulates statistics for a single logical block of up to
/// `DEFAULT_LOGICAL_BLOCK_SIZE` values. It tracks the minimum and maximum
/// non-null, non-NaN values encountered, as well as the count of null values.
///
/// # Type Parameter
///
/// * `T` - The numeric type being indexed, must implement `NumericIndexable`
///
/// # Design Notes
///
/// - NaN values in floating-point data are ignored for min/max calculations but counted as invalid
/// - The block automatically handles the case where all values are null
/// - Block capacity is enforced to prevent overflow of the logical block size
/// - Invalid count includes both null values and invalid values (like NaN)
struct NumericIndexBlock<T: NumericIndexable> {
    min_value: Option<<T::ArrowPrimitive as ArrowPrimitiveType>::Native>,
    max_value: Option<<T::ArrowPrimitive as ArrowPrimitiveType>::Native>,
    invalid_count: u16, // Includes nulls + NaN/invalid values
    processed_count: usize,
}

impl<T: NumericIndexable> NumericIndexBlock<T> {
    /// Process a range of values from an Arrow array.
    ///
    /// This method processes `count` values starting at `offset` in the provided
    /// Arrow array. For each value, it:
    /// - Increments null count if the value is null
    /// - Updates min/max if the value is non-null and non-NaN
    /// - Ignores NaN values for floating-point types
    ///
    /// # Arguments
    ///
    /// * `array` - The Arrow primitive array containing the values
    /// * `offset` - Starting index in the array
    /// * `count` - Number of values to process
    ///
    /// # Panics
    ///
    /// Panics if `count` exceeds the remaining capacity of this block.
    fn process_values(
        &mut self,
        array: &PrimitiveArray<T::ArrowPrimitive>,
        offset: usize,
        count: usize,
    ) {
        assert!(count <= self.remaining_len());
        for i in offset..offset + count {
            if array.is_null(i) {
                self.invalid_count += 1; // Nulls are invalid
            } else {
                let arrow_value = array.value(i);
                let value = T::from_arrow_native(arrow_value);
                if T::is_valid_candidate(&value) {
                    // Process valid values (non-NaN for floating-point types)
                    if let Some(current_min) = self.min_value {
                        self.min_value = Some(T::min_of(current_min, value));
                    } else {
                        self.min_value = Some(value);
                    }

                    if let Some(current_max) = self.max_value {
                        self.max_value = Some(T::max_of(current_max, value));
                    } else {
                        self.max_value = Some(value);
                    }
                } else {
                    // Non-null but invalid value (e.g., NaN)
                    self.invalid_count += 1;
                }
            }
        }
        self.processed_count += count;
    }

    /// Process a sequence of invalid values.
    ///
    /// This method efficiently handles known invalid values without requiring
    /// an Arrow array. It simply increments the invalid count and processed count.
    /// Invalid values include nulls and NaN values for floating-point types.
    ///
    /// # Arguments
    ///
    /// * `count` - Number of invalid values to process
    ///
    /// # Panics
    ///
    /// Panics if `count` exceeds the remaining capacity of this block.
    fn process_invalid(&mut self, count: usize) {
        assert!(count <= self.remaining_len());
        self.invalid_count += count as u16; // Invalid values (nulls, NaN, etc.)
        self.processed_count += count;
    }

    /// Returns the number of additional values this block can accept.
    ///
    /// # Returns
    ///
    /// The number of values that can be added before the block reaches capacity.
    fn remaining_len(&self) -> usize {
        DEFAULT_LOGICAL_BLOCK_SIZE - self.processed_count
    }

    /// Finalizes the block and returns its statistics.
    ///
    /// This method consumes the block and returns the final min/max values
    /// and invalid count (including both nulls and NaN values). If no non-null,
    /// non-NaN values were encountered, the default value for the type is returned
    /// for both min and max.
    ///
    /// # Returns
    ///
    /// A tuple containing (min_value, max_value, invalid_count).
    fn finalize(
        self,
    ) -> (
        <T::ArrowPrimitive as ArrowPrimitiveType>::Native,
        <T::ArrowPrimitive as ArrowPrimitiveType>::Native,
        u16,
    ) {
        let min_val = self.min_value.unwrap_or_default();
        let max_val = self.max_value.unwrap_or_default();
        (min_val, max_val, self.invalid_count)
    }
}

impl<T: NumericIndexable> Default for NumericIndexBlock<T> {
    fn default() -> Self {
        Self {
            min_value: Default::default(),
            max_value: Default::default(),
            invalid_count: 0,
            processed_count: 0,
        }
    }
}

/// Trait defining operations for types that can be indexed numerically.
///
/// This trait abstracts over different numeric types, providing a unified
/// interface for min/max operations, byte serialization, and type identification.
/// It enables the generic `TypedNumericIndexBuilder` to work with any supported
/// numeric type.
///
/// # Type Parameters
///
/// * `ArrowPrimitive` - The corresponding Arrow primitive type for this numeric type
/// * `Native` - The native Rust type used for computations
///
/// # Requirements
///
/// The `Native` type must be:
/// - `Copy` - For efficient value operations
/// - `Default` - To provide fallback values for empty blocks
/// - `PartialOrd` - For min/max comparisons
/// - `Send + Sync + 'static` - For thread safety and type erasure
///
/// # Implementation Notes
///
/// Implementors must handle special values correctly:
/// - NaN values should be detected and excluded from min/max calculations
/// - Type conversion between Arrow and native types must be lossless
/// - Byte serialization must preserve value ordering and be deterministic
pub trait NumericIndexable: Sized + Send + Sync + 'static {
    type ArrowPrimitive: ArrowPrimitiveType<
        Native: Copy + Default + PartialOrd + Send + Sync + bytemuck::Pod + 'static,
    >;

    /// Returns the minimum of two values.
    fn min_of(
        a: <Self::ArrowPrimitive as ArrowPrimitiveType>::Native,
        b: <Self::ArrowPrimitive as ArrowPrimitiveType>::Native,
    ) -> <Self::ArrowPrimitive as ArrowPrimitiveType>::Native;

    /// Returns the maximum of two values.
    fn max_of(
        a: <Self::ArrowPrimitive as ArrowPrimitiveType>::Native,
        b: <Self::ArrowPrimitive as ArrowPrimitiveType>::Native,
    ) -> <Self::ArrowPrimitive as ArrowPrimitiveType>::Native;

    /// Converts a slice of native values to a byte slice for serialization.
    fn as_bytes(values: &[<Self::ArrowPrimitive as ArrowPrimitiveType>::Native]) -> &[u8];

    /// Returns the Amudai BasicType identifier for this type.
    fn type_id() -> BasicType;

    /// Checks if a value represents NaN (Not a Number).
    ///
    /// For integer types, this should always return false.
    /// For floating-point types, this should detect NaN values.
    fn is_valid_candidate(value: &<Self::ArrowPrimitive as ArrowPrimitiveType>::Native) -> bool;

    /// Converts from Arrow's native type to our native type.
    fn from_arrow_native(
        value: <Self::ArrowPrimitive as ArrowPrimitiveType>::Native,
    ) -> <Self::ArrowPrimitive as ArrowPrimitiveType>::Native;
}

/// Helper macro to implement NumericIndexable for numeric types.
///
/// This macro generates a complete implementation of the `NumericIndexable` trait
/// for numeric types, handling the common patterns of min/max operations,
/// byte serialization, and type conversions. It supports both integer and floating-point
/// types with different NaN handling strategies.
///
/// # Parameters
///
/// * `$struct_name` - Name of the implementing struct (e.g., `I32Indexable`, `F32Indexable`)
/// * `$arrow_type` - Corresponding Arrow primitive type (e.g., `Int32Type`, `Float32Type`)
/// * `$native_type` - Native Rust type (e.g., `i32`, `f32`)
/// * `$basic_type` - Amudai BasicType enum variant (e.g., `BasicType::Int32`, `BasicType::Float32`)
/// * `$has_nan` - Boolean literal indicating if the type can have NaN values (`true` for floats, `false` for integers)
///
/// # Generated Implementation
///
/// The macro generates:
/// - Min/max operations using simple comparisons
/// - Byte serialization using `bytemuck` for zero-copy conversion
/// - Type ID mapping to Amudai BasicType
/// - NaN detection: always valid for integers, uses `is_nan()` for floating-point types
/// - Trivial type conversions for compatible types
macro_rules! impl_numeric_indexable_int {
    ($struct_name:ident, $arrow_type:ty, $native_type:ty, $basic_type:expr) => {
        pub struct $struct_name;

        impl NumericIndexable for $struct_name {
            type ArrowPrimitive = $arrow_type;

            fn min_of(
                a: <Self::ArrowPrimitive as ArrowPrimitiveType>::Native,
                b: <Self::ArrowPrimitive as ArrowPrimitiveType>::Native,
            ) -> <Self::ArrowPrimitive as ArrowPrimitiveType>::Native {
                if a < b { a } else { b }
            }

            fn max_of(
                a: <Self::ArrowPrimitive as ArrowPrimitiveType>::Native,
                b: <Self::ArrowPrimitive as ArrowPrimitiveType>::Native,
            ) -> <Self::ArrowPrimitive as ArrowPrimitiveType>::Native {
                if a > b { a } else { b }
            }

            fn as_bytes(values: &[<Self::ArrowPrimitive as ArrowPrimitiveType>::Native]) -> &[u8] {
                bytemuck::cast_slice(values)
            }

            fn type_id() -> BasicType {
                $basic_type
            }

            fn is_valid_candidate(
                _value: &<Self::ArrowPrimitive as ArrowPrimitiveType>::Native,
            ) -> bool {
                true
            }

            fn from_arrow_native(
                value: <Self::ArrowPrimitive as ArrowPrimitiveType>::Native,
            ) -> <Self::ArrowPrimitive as ArrowPrimitiveType>::Native {
                value
            }
        }
    };
}

macro_rules! impl_numeric_indexable_float {
    ($struct_name:ident, $arrow_type:ty, $native_type:ty, $basic_type:expr) => {
        pub struct $struct_name;

        impl NumericIndexable for $struct_name {
            type ArrowPrimitive = $arrow_type;

            fn min_of(
                a: <Self::ArrowPrimitive as ArrowPrimitiveType>::Native,
                b: <Self::ArrowPrimitive as ArrowPrimitiveType>::Native,
            ) -> <Self::ArrowPrimitive as ArrowPrimitiveType>::Native {
                if a < b { a } else { b }
            }

            fn max_of(
                a: <Self::ArrowPrimitive as ArrowPrimitiveType>::Native,
                b: <Self::ArrowPrimitive as ArrowPrimitiveType>::Native,
            ) -> <Self::ArrowPrimitive as ArrowPrimitiveType>::Native {
                if a > b { a } else { b }
            }

            fn as_bytes(values: &[<Self::ArrowPrimitive as ArrowPrimitiveType>::Native]) -> &[u8] {
                bytemuck::cast_slice(values)
            }

            fn type_id() -> BasicType {
                $basic_type
            }

            fn is_valid_candidate(
                value: &<Self::ArrowPrimitive as ArrowPrimitiveType>::Native,
            ) -> bool {
                !value.is_nan()
            }

            fn from_arrow_native(
                value: <Self::ArrowPrimitive as ArrowPrimitiveType>::Native,
            ) -> <Self::ArrowPrimitive as ArrowPrimitiveType>::Native {
                value
            }
        }
    };
}

// Implement NumericIndexable for all basic numeric types
//
// These implementations provide numeric indexing support for:
// - Signed integers: i8, i16, i32, i64
// - Unsigned integers: u8, u16, u32, u64
// - Floating-point numbers: f32, f64
impl_numeric_indexable_int!(I8Indexable, Int8Type, i8, BasicType::Int8);
impl_numeric_indexable_int!(I16Indexable, Int16Type, i16, BasicType::Int16);
impl_numeric_indexable_int!(I32Indexable, Int32Type, i32, BasicType::Int32);
impl_numeric_indexable_int!(I64Indexable, Int64Type, i64, BasicType::Int64);
impl_numeric_indexable_int!(U8Indexable, UInt8Type, u8, BasicType::Int8);
impl_numeric_indexable_int!(U16Indexable, UInt16Type, u16, BasicType::Int16);
impl_numeric_indexable_int!(U32Indexable, UInt32Type, u32, BasicType::Int32);
impl_numeric_indexable_int!(U64Indexable, UInt64Type, u64, BasicType::Int64);
impl_numeric_indexable_float!(F32Indexable, Float32Type, f32, BasicType::Float32);
impl_numeric_indexable_float!(F64Indexable, Float64Type, f64, BasicType::Float64);
impl_numeric_indexable_int!(DateTimeIndexable, UInt64Type, u64, BasicType::DateTime);

/// Header parser for numeric range index buffers.
impl NumericRangeIndexHeader {
    /// Parse header from a 40-byte buffer.
    ///
    /// Deserializes the header structure from a byte array using little endian format.
    /// This method validates the header format and ensures all fields are within
    /// expected ranges.
    ///
    /// # Arguments
    ///
    /// * `bytes` - Header bytes containing the serialized header data
    ///
    /// # Returns
    ///
    /// A parsed header structure, or an error if the data is invalid.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_SIZE {
            return Err(Error::invalid_format("Header buffer too short"));
        }

        let version = u16::from_le_bytes([bytes[0], bytes[1]]);
        if version != 1 {
            return Err(Error::invalid_format(format!(
                "Unsupported index version: {version}"
            )));
        }

        let basic_type = bytes[2];
        let has_checksum = bytes[3];
        let logical_position_span = u64::from_le_bytes([
            bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11],
        ]);
        let payload0_size = u64::from_le_bytes([
            bytes[12], bytes[13], bytes[14], bytes[15], bytes[16], bytes[17], bytes[18], bytes[19],
        ]);
        let payload1_size = u64::from_le_bytes([
            bytes[20], bytes[21], bytes[22], bytes[23], bytes[24], bytes[25], bytes[26], bytes[27],
        ]);
        let payload2_size = u64::from_le_bytes([
            bytes[28], bytes[29], bytes[30], bytes[31], bytes[32], bytes[33], bytes[34], bytes[35],
        ]);
        let logical_block_size = u16::from_le_bytes([bytes[36], bytes[37]]);
        let padding = u16::from_le_bytes([bytes[38], bytes[39]]);

        if padding != 0 {
            return Err(Error::invalid_format("Header padding must be zero"));
        }

        Ok(NumericRangeIndexHeader {
            version,
            basic_type,
            has_checksum,
            logical_position_span,
            payload0_size,
            payload1_size,
            payload2_size,
            logical_block_size,
            padding,
        })
    }
}
