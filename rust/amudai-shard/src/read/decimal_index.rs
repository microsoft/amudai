//! Decimal range index decoder implementation for the Amudai blockstream format.
//!
//! This module provides the decoding implementation for decimal range indexes
//! as specified in Chapter 8 of the Amudai specification. The decoders enable efficient
//! querying of range statistics (min/max values and invalid counts) for logical blocks
//! of decimal data.
//!
//! ## Decimal Type Support
//!
//! The decimal index decoder supports:
//! - **Decimal**: `d128` stored as `FixedSizeBinary(16)` with `KustoDecimal` extension type
//! - Proper handling of NaN values (excluded from min/max calculations)
//! - Efficient binary representation for storage and comparison

use std::mem::size_of;
use std::sync::Arc;

use amudai_common::{Result, error::Error};
use amudai_encodings::{
    block_decoder::BlockDecoder,
    block_encoder::{BlockChecksum, BlockEncodingParameters, PresenceEncoding},
    primitive_block_decoder::PrimitiveBlockDecoder,
};
use amudai_format::defs::{schema::BasicType, schema_ext::BasicTypeDescriptor};
use amudai_io::ReadAt;
use amudai_sequence::values::Values;

// Re-export necessary types from other modules
use super::numeric_index::{BlockStatsArray, NumericIndexDecoder};
use crate::write::numeric_index::{HEADER_SIZE, NumericRangeIndexHeader};

/// Size of a decimal128 value in bytes
const DECIMAL_SIZE: usize = 16;

/// Decimal index decoder implementation.
///
/// This struct provides specialized decoding for decimal range indexes,
/// handling the decoding and interpretation of decimal min/max values.
pub struct DecimalIndexDecoder {
    reader: Arc<dyn ReadAt>,
    header: NumericRangeIndexHeader,
    min_values: Option<Values>,
    max_values: Option<Values>,
    invalid_counts: Option<Values>,
    decoder: PrimitiveBlockDecoder,
}

impl DecimalIndexDecoder {
    /// Creates a new decimal index decoder.
    ///
    /// This method reads and validates the index header, ensuring it represents
    /// a valid decimal index, and initializes the decoder for decoding the compressed
    /// min/max values and invalid counts.
    ///
    /// # Arguments
    ///
    /// * `reader` - The data source implementing ReadAt for the index buffer
    ///
    /// # Returns
    ///
    /// A new decimal index decoder ready to serve range queries, or an error
    /// if the header is invalid or the index format is incorrect.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The header cannot be read or parsed
    /// - The index is not a decimal type (FixedSizeBinary)
    /// - The buffer is corrupted or invalid
    pub fn new(reader: Arc<dyn ReadAt>) -> Result<Self> {
        // Read and validate header
        let header_bytes = reader
            .read_at(0..HEADER_SIZE as u64)
            .map_err(|e| Error::io("Failed to read index header", e))?;

        let header = NumericRangeIndexHeader::from_bytes(&header_bytes)?;

        // Validate it's a decimal index
        if header.basic_type != BasicType::FixedSizeBinary as u8 {
            return Err(Error::invalid_format("Index is not a decimal index"));
        }

        // Create decoder for u8 data (since we encoded as bytes) - initialize once
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

        let start_offset = HEADER_SIZE as u64;
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

        let start_offset = HEADER_SIZE as u64 + self.header.payload0_size;
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

    /// Decodes a values buffer using the pre-initialized decoder.
    ///
    /// This method provides a shared decode path for both min and max value buffers,
    /// using the decoder instance that was initialized once in the constructor.
    fn decode_values_buffer(&self, buffer: &[u8]) -> Result<Values> {
        let decoded_size = self.block_count() as usize * DECIMAL_SIZE;

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

impl NumericIndexDecoder for DecimalIndexDecoder {
    fn get_block_stats_array(&mut self) -> Result<BlockStatsArray> {
        self.ensure_min_values()?;
        self.ensure_max_values()?;
        self.ensure_invalid_counts()?;

        let min_bytes = self
            .min_values
            .as_ref()
            .map(|v| v.as_bytes())
            .unwrap_or(&[]);
        let max_bytes = self
            .max_values
            .as_ref()
            .map(|v| v.as_bytes())
            .unwrap_or(&[]);
        let invalid_counts = self.invalid_counts.as_ref().map(|v| v.as_slice::<u16>());

        Ok(BlockStatsArray {
            min_values: min_bytes,
            max_values: max_bytes,
            invalid_counts,
        })
    }

    fn block_count(&self) -> u64 {
        if self.header.payload0_size == 0 && self.header.payload1_size == 0 {
            return 0;
        }

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
