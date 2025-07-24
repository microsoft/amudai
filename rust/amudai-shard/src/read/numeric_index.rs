//! Numeric range index decoder implementation for the Amudai blockstream format.
//!
//! This module provides the decoding implementation for numeric range indexes
//! as specified in Chapter 8 of the Amudai specification. The decoders enable efficient
//! querying of range statistics (min/max values and invalid counts) for logical blocks
//! of numeric data.
//!
//! ## Supported Types
//!
//! The numeric index decoder system supports the same data types as the writer:
//! - **Integer types**: `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `u64`
//! - **Floating-point types**: `f32`, `f64`
//! - **DateTime**: Stored as `u64` representing ticks
//! - **Decimal**: `d128` stored as `FixedSizeBinary(16)` with `KustoDecimal` extension type
//!
//! ## Usage
//!
//! Decoders are created via the `create_decoder` function which returns the appropriate
//! decoder for a given basic type and index buffer.

use std::mem::size_of;
use std::sync::Arc;

use amudai_common::{Result, error::Error};
use amudai_decimal::d128;
use amudai_encodings::{
    block_decoder::BlockDecoder,
    block_encoder::{BlockChecksum, BlockEncodingParameters, PresenceEncoding},
    primitive_block_decoder::PrimitiveBlockDecoder,
};
use amudai_format::defs::{
    schema::BasicType,
    schema_ext::{BasicTypeDescriptor, KnownExtendedType},
};
use amudai_io::ReadAt;
use amudai_sequence::values::Values;
use arrow_array::ArrowPrimitiveType;
use arrow_buffer::ArrowNativeType;

// Re-export necessary types from the write module
use crate::write::numeric_index::{
    DateTimeIndexable, F32Indexable, F64Indexable, HEADER_SIZE, I8Indexable, I16Indexable,
    I32Indexable, I64Indexable, NumericIndexable, NumericRangeIndexHeader, U8Indexable,
    U16Indexable, U32Indexable, U64Indexable,
};

/// Creates a numeric index decoder for the specified data type and buffer.
///
/// This factory function returns the appropriate decoder implementation based on
/// the provided basic type and index buffer. It parses the index header to validate
/// the format and creates a decoder that can efficiently query range statistics.
///
/// # Arguments
///
/// * `basic_type` - The basic type descriptor specifying the data type to read
/// * `reader` - The data source implementing ReadAt for the index buffer
///
/// # Returns
///
/// Returns a boxed decoder implementing `NumericIndexDecoder` for the specified type,
/// or an error if the index format is invalid or the type doesn't match.
///
/// # Errors
///
/// Returns an error if:
/// - The header cannot be read or parsed
/// - The basic type doesn't match the index header
/// - The index version is unsupported
/// - The buffer is corrupted or invalid
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use amudai_format::defs::{schema::BasicType, schema_ext::BasicTypeDescriptor};
/// use amudai_io::ReadAt;
/// use amudai_shard::read::numeric_index::create_decoder;
///
/// # fn example() -> amudai_common::Result<()> {
/// # let buffer_reader: Arc<dyn ReadAt> = todo!();
/// // For signed i32 index
/// let decoder = create_decoder(
///     BasicTypeDescriptor {
///         basic_type: BasicType::Int32,
///         signed: true,
///         fixed_size: 0,
///         extended_type: Default::default(),
///     },
///     buffer_reader.clone()
/// )?;
///
/// // For unsigned u32 index  
/// let decoder = create_decoder(
///     BasicTypeDescriptor {
///         basic_type: BasicType::Int32,
///         signed: false,
///         fixed_size: 0,
///         extended_type: Default::default(),
///     },
///     buffer_reader
/// )?;
///
/// // let stats = decoder.get_block_stats_array()?;
/// # Ok(())
/// # }
/// ```
pub fn create_decoder(
    basic_type: BasicTypeDescriptor,
    reader: Arc<dyn ReadAt>,
) -> Result<Box<dyn NumericIndexDecoder>> {
    // Check if this is a decimal type
    if basic_type.basic_type == BasicType::FixedSizeBinary
        && basic_type.fixed_size == 16
        && basic_type.extended_type == KnownExtendedType::KustoDecimal
    {
        return Ok(Box::new(super::decimal_index::DecimalIndexDecoder::new(
            reader,
        )?));
    }

    // Read and validate the header
    let header_bytes = reader
        .read_at(0..HEADER_SIZE as u64)
        .map_err(|e| Error::io("Failed to read index header", e))?;

    if header_bytes.len() < HEADER_SIZE {
        return Err(Error::invalid_format("Index header too short"));
    }

    let header = NumericRangeIndexHeader::from_bytes(&header_bytes)?;

    // Validate header fields
    if header.version != 1 {
        return Err(Error::invalid_format(format!(
            "Unsupported index version: {}. Only version 1 is supported",
            header.version
        )));
    }

    if header.logical_position_span == 0 {
        return Err(Error::invalid_format(
            "Index cannot have zero logical position span (no values covered)",
        ));
    }

    if header.logical_block_size == 0 {
        return Err(Error::invalid_format(
            "Index cannot have zero logical block size",
        ));
    }

    // Validate that we have at least min and max value payloads
    if header.payload0_size == 0 || header.payload1_size == 0 {
        return Err(Error::invalid_format(
            "Index must have both min and max value payloads",
        ));
    }

    // Validate the basic type matches
    if header.basic_type != basic_type.basic_type as u8 {
        return Err(Error::invalid_format(format!(
            "Index type mismatch: expected {:?}, found type ID {}",
            basic_type.basic_type, header.basic_type
        )));
    }

    // Create the appropriate typed reader
    let reader: Box<dyn NumericIndexDecoder> = match basic_type.basic_type {
        BasicType::Int8 => {
            if basic_type.signed {
                Box::new(TypedNumericIndexDecoder::<I8Indexable>::new(
                    reader, header,
                )?)
            } else {
                Box::new(TypedNumericIndexDecoder::<U8Indexable>::new(
                    reader, header,
                )?)
            }
        }
        BasicType::Int16 => {
            if basic_type.signed {
                Box::new(TypedNumericIndexDecoder::<I16Indexable>::new(
                    reader, header,
                )?)
            } else {
                Box::new(TypedNumericIndexDecoder::<U16Indexable>::new(
                    reader, header,
                )?)
            }
        }
        BasicType::Int32 => {
            if basic_type.signed {
                Box::new(TypedNumericIndexDecoder::<I32Indexable>::new(
                    reader, header,
                )?)
            } else {
                Box::new(TypedNumericIndexDecoder::<U32Indexable>::new(
                    reader, header,
                )?)
            }
        }
        BasicType::Int64 => {
            if basic_type.signed {
                Box::new(TypedNumericIndexDecoder::<I64Indexable>::new(
                    reader, header,
                )?)
            } else {
                Box::new(TypedNumericIndexDecoder::<U64Indexable>::new(
                    reader, header,
                )?)
            }
        }
        BasicType::Float32 => Box::new(TypedNumericIndexDecoder::<F32Indexable>::new(
            reader, header,
        )?),
        BasicType::Float64 => Box::new(TypedNumericIndexDecoder::<F64Indexable>::new(
            reader, header,
        )?),
        BasicType::DateTime => Box::new(TypedNumericIndexDecoder::<DateTimeIndexable>::new(
            reader, header,
        )?),
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

    Ok(reader)
}

/// A trait for decoding numeric range indexes for different data types.
///
/// This trait provides a unified interface for querying numeric range indexes
/// to obtain min/max values and invalid counts for logical blocks of data. The
/// implementation efficiently reads and decodes the index data without loading
/// the entire index into memory.
///
/// # Usage
///
/// Decoders are created via the `create_decoder` function which returns the
/// appropriate decoder for a given basic type and index buffer.
///
/// # Thread Safety
///
/// The trait is object-safe and thread-safe, requiring `Send + Sync + 'static`
/// bounds to ensure it can be used across thread boundaries safely.
pub trait NumericIndexDecoder: Send + Sync + 'static {
    /// Gets the range statistics for all logical blocks as arrays.
    ///
    /// This method retrieves the min/max values and null counts for all logical
    /// blocks in the index. The data is returned as raw byte arrays that can be
    /// interpreted according to the index's data type.
    ///
    /// # Returns
    ///
    /// Returns `BlockStatsArray` containing min/max value arrays and null count array,
    /// or an error if reading or decoding fails.
    fn get_block_stats_array(&mut self) -> Result<BlockStatsArray>;

    /// Gets the total number of logical blocks in the index.
    ///
    /// # Returns
    ///
    /// The number of logical blocks that can be queried.
    fn block_count(&self) -> u64;

    /// Gets the total number of logical positions covered by this index.
    ///
    /// # Returns
    ///
    /// The total span of logical positions represented by all blocks in the index.
    fn logical_position_span(&self) -> u64;

    /// Gets the logical block size used by this index.
    ///
    /// # Returns
    ///
    /// The number of values per logical block (typically 256).
    fn logical_block_size(&self) -> u16;
}

/// Container for block statistics arrays.
///
/// This struct provides access to the raw min/max values and invalid counts
/// for all blocks in the index. The data is stored as byte arrays that must
/// be interpreted according to the index's data type.
pub struct BlockStatsArray<'a> {
    /// Minimum values for each block as raw bytes
    pub min_values: &'a [u8],
    /// Maximum values for each block as raw bytes  
    pub max_values: &'a [u8],
    /// Number of null values in each block (None if all counts are zero)
    pub invalid_counts: Option<&'a [u16]>,
}

impl BlockStatsArray<'_> {
    /// Get the minimum value for a specific block as a typed value.
    ///
    /// # Arguments
    ///
    /// * `block_index` - Zero-based index of the block to query
    ///
    /// # Returns
    ///
    /// The minimum value for the specified block, cast to type T.
    ///
    /// # Safety
    ///
    /// The caller must ensure that T matches the data type of the index
    /// and that block_index is within bounds.
    pub fn get_block_min<T: Copy + bytemuck::Pod>(&self, block_index: usize) -> T {
        bytemuck::cast_slice(self.min_values)[block_index]
    }

    /// Get the minimum value for a specific block as a decimal.
    ///
    /// # Arguments
    ///
    /// * `block_index` - Zero-based index of the block to query
    ///
    /// # Returns
    ///
    /// The minimum decimal value for the specified block.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the index contains decimal data
    /// and that block_index is within bounds.
    pub fn get_block_min_as_decimal(&self, block_index: usize) -> d128 {
        let bytes = bytemuck::cast_slice::<_, [u8; 16]>(self.min_values)[block_index];
        unsafe { d128::from_raw_bytes(bytes) }
    }

    /// Get the maximum value for a specific block as a typed value.
    ///
    /// # Arguments
    ///
    /// * `block_index` - Zero-based index of the block to query
    ///
    /// # Returns
    ///
    /// The maximum value for the specified block, cast to type T.
    ///
    /// # Safety
    ///
    /// The caller must ensure that T matches the data type of the index
    /// and that block_index is within bounds.
    pub fn get_block_max<T: Copy + bytemuck::Pod>(&self, block_index: usize) -> T {
        bytemuck::cast_slice(self.max_values)[block_index]
    }

    /// Get the maximum value for a specific block as a decimal.
    ///
    /// # Arguments
    ///
    /// * `block_index` - Zero-based index of the block to query
    ///
    /// # Returns
    ///
    /// The maximum decimal value for the specified block.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the index contains decimal data
    /// and that block_index is within bounds.
    pub fn get_block_max_as_decimal(&self, block_index: usize) -> d128 {
        let bytes = bytemuck::cast_slice::<_, [u8; 16]>(self.max_values)[block_index];
        unsafe { d128::from_raw_bytes(bytes) }
    }

    /// Get the invalid count for a specific block.
    ///
    /// # Arguments
    ///
    /// * `block_index` - Zero-based index of the block to query
    ///
    /// # Returns
    ///
    /// The number of invalid (null/NaN) values in the specified block,
    /// or None if all invalid counts are zero (optimization).
    pub fn get_block_invalid_counts(&self, block_index: usize) -> Option<usize> {
        self.invalid_counts
            .map(|invalid_counts| invalid_counts[block_index] as usize)
    }
}

/// Typed numeric index decoder implementation for a specific data type.
///
/// This struct provides the core implementation for decoding numeric range indexes
/// for a particular data type `T`. It lazily decodes the min/max values and null
/// counts from the compressed index buffers as needed.
///
/// # Type Parameter
///
/// * `T` - A type implementing `NumericIndexable` that defines the specific
///   numeric type being decoded (e.g., i32, f64, DateTime)
///
/// # Design
///
/// The decoder maintains references to the three payload buffers (min_values,
/// max_values, invalid_counts) and decodes them on-demand. It uses the existing
/// `PrimitiveBlockDecoder` infrastructure to handle the decompression.
pub struct TypedNumericIndexDecoder<T: NumericIndexable> {
    reader: Arc<dyn ReadAt>,
    header: NumericRangeIndexHeader,
    min_values: Option<Values>,
    max_values: Option<Values>,
    invalid_counts: Option<Values>,
    decoder: PrimitiveBlockDecoder,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: NumericIndexable> TypedNumericIndexDecoder<T> {
    /// Creates a new numeric index decoder for type T.
    ///
    /// Initializes the decoder with the index buffer and header, preparing it
    /// to decode index data on-demand.
    ///
    /// # Arguments
    ///
    /// * `reader` - The data source for the index buffer
    /// * `header` - The parsed index header containing metadata
    ///
    /// # Returns
    ///
    /// A new decoder ready to serve range queries, or an error if initialization fails.
    pub fn new(reader: Arc<dyn ReadAt>, header: NumericRangeIndexHeader) -> Result<Self> {
        // Create decoder for u8 data (since we encoded as bytes)
        let u8_basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Int8,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };

        let parameters = BlockEncodingParameters {
            checksum: if header.has_checksum != 0 {
                BlockChecksum::Enabled
            } else {
                BlockChecksum::Disabled
            },
            presence: PresenceEncoding::Disabled,
        };

        let decoder = PrimitiveBlockDecoder::new(parameters, u8_basic_type, Default::default());

        Ok(Self {
            reader,
            header,
            min_values: None,
            max_values: None,
            invalid_counts: None,
            decoder,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Ensures min_values are loaded and decoded.
    fn ensure_min_values(&mut self) -> Result<()> {
        if self.min_values.is_some() {
            return Ok(());
        }

        if self.header.payload0_size == 0 {
            self.min_values = None;
            return Ok(());
        }

        let start_offset = HEADER_SIZE as u64; // After header
        let end_offset = start_offset + self.header.payload0_size;
        let buffer = self
            .reader
            .read_at(start_offset..end_offset)
            .map_err(|e| Error::io("Failed to read min_values buffer", e))?;

        let values = self.decode_values_buffer(&buffer)?;
        self.min_values = Some(values);
        Ok(())
    }

    /// Ensures max_values are loaded and decoded.
    fn ensure_max_values(&mut self) -> Result<()> {
        if self.max_values.is_some() {
            return Ok(());
        }

        if self.header.payload1_size == 0 {
            self.max_values = None;
            return Ok(());
        }

        let start_offset = HEADER_SIZE as u64 + self.header.payload0_size; // After header and payload0
        let end_offset = start_offset + self.header.payload1_size;
        let buffer = self
            .reader
            .read_at(start_offset..end_offset)
            .map_err(|e| Error::io("Failed to read max_values buffer", e))?;

        let values = self.decode_values_buffer(&buffer)?;
        self.max_values = Some(values);
        Ok(())
    }

    /// Ensures invalid_counts are loaded and decoded.
    fn ensure_invalid_counts(&mut self) -> Result<()> {
        if self.invalid_counts.is_some() {
            return Ok(());
        }

        if self.header.payload2_size == 0 {
            self.invalid_counts = None;
            return Ok(());
        }

        let start_offset =
            HEADER_SIZE as u64 + self.header.payload0_size + self.header.payload1_size;
        let end_offset = start_offset + self.header.payload2_size;
        let buffer = self
            .reader
            .read_at(start_offset..end_offset)
            .map_err(|e| Error::io("Failed to read invalid_counts buffer", e))?;

        let decoded_size = self.block_count() as usize * size_of::<u16>();

        // Ensure we have a valid decoded size (catch corrupt indexes early)
        if decoded_size == 0 {
            return Err(Error::invalid_format(
                "Cannot decode invalid_counts with zero size - indicates corrupt index",
            ));
        }

        // The buffer contains a single encoded block
        let sequence = self.decoder.decode(&buffer, decoded_size)?;

        self.invalid_counts = Some(sequence.values);
        Ok(())
    }

    /// Decodes a values buffer using the configured decoder.
    fn decode_values_buffer(&self, buffer: &[u8]) -> Result<Values> {
        let decoded_size = self.block_count() as usize
            * <T::ArrowPrimitive as ArrowPrimitiveType>::Native::get_byte_width();

        // Ensure we have a valid decoded size (catch corrupt indexes early)
        if decoded_size == 0 {
            return Err(Error::invalid_format(
                "Cannot decode values with zero size - indicates corrupt index",
            ));
        }

        // The buffer contains a single encoded block
        let sequence = self.decoder.decode(buffer, decoded_size)?;

        Ok(sequence.values)
    }
}

impl<T: NumericIndexable> NumericIndexDecoder for TypedNumericIndexDecoder<T> {
    fn get_block_stats_array(&mut self) -> Result<BlockStatsArray> {
        self.ensure_min_values()?;
        self.ensure_max_values()?;
        self.ensure_invalid_counts()?;
        Ok(BlockStatsArray {
            min_values: self.min_values.as_ref().unwrap().as_bytes(),
            max_values: self.max_values.as_ref().unwrap().as_bytes(),
            invalid_counts: self
                .invalid_counts
                .as_ref()
                .map(|values| values.as_slice::<u16>()),
        })
    }

    fn block_count(&self) -> u64 {
        if self.header.payload0_size == 0 && self.header.payload1_size == 0 {
            return 0; // Empty index
        }

        // Calculate block count from the logical position span and block size
        let block_size = self.header.logical_block_size as u64;
        if block_size == 0 {
            return 0;
        }

        self.header.logical_position_span.div_ceil(block_size)
    }

    fn logical_position_span(&self) -> u64 {
        self.header.logical_position_span
    }

    fn logical_block_size(&self) -> u16 {
        self.header.logical_block_size
    }
}
