//! Decimal range index implementation for the Amudai blockstream format.
//!
//! This module provides specialized indexing for decimal128 values, which require
//! custom handling due to their fixed-size binary representation and comparison semantics.
//! The index maintains min/max values for blocks of decimal data to enable efficient
//! range-based queries.
//!
//! ## Design
//!
//! Decimal indexes share the same header structure as numeric indexes but use a
//! specialized payload format optimized for 16-byte decimal values. The implementation
//! handles decimal comparisons correctly, preserving scale and precision during operations.

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
use amudai_format::defs::{schema::BasicType, schema_ext::BasicTypeDescriptor, shard};
use amudai_io::temp_file_store::TemporaryFileStore;

use amudai_decimal::d128;
use arrow_array::{Array, FixedSizeBinaryArray, cast::AsArray};

use super::numeric_index::{
    DEFAULT_LOGICAL_BLOCK_SIZE, HEADER_SIZE, NumericIndexBuilder, NumericRangeIndexHeader,
};

/// Size of a decimal128 value in bytes
const DECIMAL_SIZE: usize = 16;

/// Raw bytes representation of decimal zero (all zeros for decimal128)
const DECIMAL_ZERO_BYTES: [u8; DECIMAL_SIZE] = [0u8; DECIMAL_SIZE];

/// Decimal index builder implementation.
///
/// This struct provides specialized indexing for decimal128 values, handling
/// their unique comparison semantics and binary representation. It accumulates
/// min/max values and invalid counts across logical blocks of decimal data.
pub struct DecimalIndexBuilder {
    min_values: Vec<[u8; DECIMAL_SIZE]>,
    max_values: Vec<[u8; DECIMAL_SIZE]>,
    invalid_counts: Vec<u16>,
    current_block: DecimalIndexBlock,
    temp_store: Arc<dyn TemporaryFileStore>,
    encoding_profile: BlockEncodingProfile,
    logical_position_span: u64,
}

impl DecimalIndexBuilder {
    /// Creates a new decimal index builder.
    pub fn new(
        temp_store: Arc<dyn TemporaryFileStore>,
        encoding_profile: BlockEncodingProfile,
    ) -> Self {
        DecimalIndexBuilder {
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
    fn finalize_current_block(&mut self) {
        if self.current_block.processed_count > 0 {
            let (min_val, max_val, invalid_count) =
                std::mem::take(&mut self.current_block).finalize();
            self.min_values.push(min_val);
            self.max_values.push(max_val);
            self.invalid_counts.push(invalid_count);
        }
    }

    /// Encodes a byte slice using the PrimitiveBlockEncoder.
    fn encode_bytes(
        &self,
        encoding_policy: &BlockEncodingPolicy,
        buffer: &mut dyn amudai_io::ExclusiveIoBuffer,
        pos: u64,
        data: &[u8],
    ) -> Result<u64> {
        let u8_basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Int8,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };

        let mut encoder =
            PrimitiveBlockEncoder::new(encoding_policy.clone(), u8_basic_type, Default::default());

        let array = arrow_array::UInt8Array::from(data.to_vec());
        let sample_size = std::cmp::min(1024, array.len());
        let sample_array = array.slice(0, sample_size);
        encoder.analyze_sample(&sample_array)?;

        let encoded = encoder.encode(&array)?;
        buffer.write_at(pos, encoded.as_ref())?;

        Ok(encoded.as_ref().len() as u64)
    }

    /// Encodes the accumulated index data into the final buffer format.
    fn encode_index(&self) -> Result<PreparedEncodedBuffer> {
        let mut writer = self.temp_store.allocate_exclusive_buffer(None)?;

        let encoding_policy = BlockEncodingPolicy {
            parameters: BlockEncodingParameters {
                checksum: BlockChecksum::Enabled,
                presence: PresenceEncoding::Disabled,
            },
            profile: self.encoding_profile,
            size_constraints: Default::default(),
        };

        let mut position = HEADER_SIZE as u64;

        // Encode min values
        let min_bytes: Vec<u8> = self
            .min_values
            .iter()
            .flat_map(|v| v.iter().copied())
            .collect();
        let payload0_size =
            self.encode_bytes(&encoding_policy, writer.as_mut(), position, &min_bytes)?;
        position += payload0_size;

        // Encode max values
        let max_bytes: Vec<u8> = self
            .max_values
            .iter()
            .flat_map(|v| v.iter().copied())
            .collect();
        let payload1_size =
            self.encode_bytes(&encoding_policy, writer.as_mut(), position, &max_bytes)?;
        position += payload1_size;

        // Encode invalid counts (if any)
        let all_invalid_are_zero = self.invalid_counts.iter().all(|&count| count == 0);
        let payload2_size: u64 = if all_invalid_are_zero {
            0
        } else {
            let invalid_bytes = bytemuck::cast_slice::<u16, u8>(&self.invalid_counts);
            self.encode_bytes(&encoding_policy, writer.as_mut(), position, invalid_bytes)?
        };
        position += payload2_size;

        // Create header
        let header = NumericRangeIndexHeader {
            version: 1,
            basic_type: BasicType::FixedSizeBinary as u8,
            has_checksum: 1,
            logical_position_span: self.logical_position_span,
            payload0_size,
            payload1_size,
            payload2_size,
            logical_block_size: DEFAULT_LOGICAL_BLOCK_SIZE as u16,
            padding: 0,
        };

        writer.write_at(0, &header.to_bytes())?;

        // Align to 64-byte boundary
        let aligned_size = position.next_multiple_of(64);
        if aligned_size > position {
            let padding_size = aligned_size - position;
            let padding = vec![0u8; padding_size as usize];
            writer.write_at(position, &padding)?;
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

impl NumericIndexBuilder for DecimalIndexBuilder {
    fn process_array(&mut self, array: &dyn Array) -> Result<()> {
        let array = array.as_fixed_size_binary();
        if array.value_length() != DECIMAL_SIZE as i32 {
            return Err(Error::invalid_arg(
                "array",
                format!("Expected decimal array with {DECIMAL_SIZE} byte values"),
            ));
        }

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

        Ok(Some(self.encode_index()?))
    }
}

/// Represents a logical block of decimal values being processed for indexing.
#[derive(Default)]
struct DecimalIndexBlock {
    min_value: Option<[u8; DECIMAL_SIZE]>,
    max_value: Option<[u8; DECIMAL_SIZE]>,
    invalid_count: u16, // Includes nulls + NaN values
    processed_count: usize,
}

impl DecimalIndexBlock {
    /// Process a range of values from a FixedSizeBinaryArray.
    fn process_values(&mut self, array: &FixedSizeBinaryArray, offset: usize, count: usize) {
        assert!(count <= self.remaining_len());

        // Convert current min/max to d128 for efficient comparison within the loop
        let mut current_min_dec = self
            .min_value
            .map(|bytes| unsafe { d128::from_raw_bytes(bytes) });
        let mut current_max_dec = self
            .max_value
            .map(|bytes| unsafe { d128::from_raw_bytes(bytes) });

        for i in offset..offset + count {
            if array.is_null(i) {
                self.invalid_count += 1; // Nulls are invalid
            } else {
                let value_bytes = array.value(i);
                let mut value_array = [0u8; DECIMAL_SIZE];
                value_array.copy_from_slice(value_bytes);

                // Parse as decimal for comparison
                let value = unsafe { d128::from_raw_bytes(value_array) };

                // Check if value is NaN (invalid for decimal)
                if value.is_nan() {
                    self.invalid_count += 1; // NaN values are invalid
                    continue; // Skip for min/max calculations
                }

                // Update min value
                if let Some(current_min) = current_min_dec {
                    if value < current_min {
                        current_min_dec = Some(value);
                        self.min_value = Some(value_array);
                    }
                } else {
                    current_min_dec = Some(value);
                    self.min_value = Some(value_array);
                }

                // Update max value
                if let Some(current_max) = current_max_dec {
                    if value > current_max {
                        current_max_dec = Some(value);
                        self.max_value = Some(value_array);
                    }
                } else {
                    current_max_dec = Some(value);
                    self.max_value = Some(value_array);
                }
            }
        }
        self.processed_count += count;
    }

    /// Process a sequence of invalid values.
    ///
    /// This method allows efficient processing of known invalid values (such as nulls
    /// or NaN values for floating-point types) without requiring a full Arrow array.
    fn process_invalid(&mut self, count: usize) {
        assert!(count <= self.remaining_len());
        self.invalid_count += count as u16; // Invalid values (nulls, NaN, etc.)
        self.processed_count += count;
    }

    /// Returns the number of additional values this block can accept.
    fn remaining_len(&self) -> usize {
        DEFAULT_LOGICAL_BLOCK_SIZE - self.processed_count
    }

    /// Finalizes the block and returns its statistics.
    fn finalize(self) -> ([u8; DECIMAL_SIZE], [u8; DECIMAL_SIZE], u16) {
        let min_val = self.min_value.unwrap_or(DECIMAL_ZERO_BYTES);
        let max_val = self.max_value.unwrap_or(DECIMAL_ZERO_BYTES);
        (min_val, max_val, self.invalid_count)
    }
}
