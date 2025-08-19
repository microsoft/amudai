//! Bit-packed Boolean Buffer Decoding for Amudai Blockstream
//!
//! This module provides types and utilities for decoding streams of boolean values
//! that are packed as bits within byte-aligned buffers, as used in the Amudai columnar
//! data format. It supports both block-based (compressed, chunked) and constant-valued
//! representations of boolean fields.

use std::{ops::Range, sync::Arc};

use amudai_bits::bitpacking;
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::Result;
use amudai_format::{
    defs::shard,
    schema::{BasicType, BasicTypeDescriptor},
};
use amudai_io::ReadAt;
use amudai_ranges::{IntoIteratorExt, PositionSeries};
use amudai_sequence::{presence::Presence, sequence::ValueSequence, values::Values};
use arrow_buffer::BooleanBuffer;
use itertools::Itertools;

use crate::{
    read::{EPHEMERAL_BLOCK_SIZE, block_stream::DecodedBlock, get_ephemeral_block_descriptor},
    write::PreparedEncodedBuffer,
};

use super::{
    block_stream::{BlockReaderPrefetch, BlockStreamDecoder},
    primitive_buffer::{PrimitiveBufferDecoder, PrimitiveBufferReader},
};

/// Decoder for bit-packed boolean buffers.
///
/// This enum represents a decoder for a stream of boolean values encoded as a bit stream.
/// The bit stream may be represented in two ways:
/// - As a constant value (all bits are the same, either all true or all false).
/// - As a sequence of byte-valued blocks, where each bit represents a boolean value,
///   packed LSB-first into bytes (using a [`PrimitiveBufferDecoder`] with `Int8` type).
#[derive(Clone)]
pub enum BitBufferDecoder {
    /// Block-encoded bit stream, using a primitive buffer of bytes (u8).
    Blocks(PrimitiveBufferDecoder),
    /// Constant-valued bit stream (all true or all false).
    Constant(bool),
}

impl BitBufferDecoder {
    /// Constructs a [`BitBufferDecoder`] from an encoded buffer.
    ///
    /// # Arguments
    /// * `reader` - Shared reference to the underlying storage implementing [`ReadAt`].
    /// * `encoded_buffer` - The encoded buffer metadata describing the bit-packed data.
    ///
    /// # Returns
    /// A [`BitBufferDecoder`] instance for reading the encoded bit stream.
    pub fn from_encoded_buffer(
        reader: Arc<dyn ReadAt>,
        encoded_buffer: &shard::EncodedBuffer,
    ) -> Result<BitBufferDecoder> {
        let decoder = PrimitiveBufferDecoder::from_encoded_buffer(
            reader,
            encoded_buffer,
            BasicTypeDescriptor {
                basic_type: BasicType::Int8,
                fixed_size: 0,
                signed: false,
                extended_type: Default::default(),
            },
        )?;
        Ok(BitBufferDecoder::Blocks(decoder))
    }

    /// Constructs a [`BitBufferDecoder`] from a prepared (in-memory) encoded buffer.
    ///
    /// # Arguments
    /// * `prepared_buffer` - The prepared encoded buffer.
    ///
    /// # Returns
    /// A [`BitBufferDecoder`] instance for reading the bit stream.
    pub fn from_prepared_buffer(
        prepared_buffer: &PreparedEncodedBuffer,
    ) -> Result<BitBufferDecoder> {
        let decoder = PrimitiveBufferDecoder::from_prepared_buffer(
            prepared_buffer,
            BasicTypeDescriptor {
                basic_type: BasicType::Int8,
                fixed_size: 0,
                signed: false,
                extended_type: Default::default(),
            },
        )?;
        Ok(BitBufferDecoder::Blocks(decoder))
    }

    /// Constructs a [`BitBufferDecoder`] representing a constant-valued bit stream.
    ///
    /// # Arguments
    /// * `value` - The constant boolean value for all positions in the stream.
    ///
    /// # Returns
    /// A [`BitBufferDecoder`] that always yields the given value.
    pub fn from_constant(value: bool) -> BitBufferDecoder {
        BitBufferDecoder::Constant(value)
    }

    /// Creates a [`BitBufferReader`] for accessing bits or ranges of bits at the
    /// specified positions in the stream.
    ///
    /// # Arguments
    /// * `bit_positions` - Iterator over non-descending logical bit positions
    ///   to be accessed.
    /// * `prefetch` - Prefetching policy for block reads.
    ///
    /// # Returns
    /// A [`BitBufferReader`] for reading the specified bit positions.
    pub fn create_reader(
        &self,
        bit_positions: impl PositionSeries<u64>,
        prefetch: BlockReaderPrefetch,
    ) -> Result<BitBufferReader> {
        match self {
            BitBufferDecoder::Blocks(decoder) => {
                let byte_positions = BitPosToBytePos(bit_positions);
                let reader = decoder.create_reader(byte_positions, prefetch)?;
                Ok(BitBufferReader::Blocks(Box::new(reader)))
            }
            BitBufferDecoder::Constant(value) => Ok(BitBufferReader::Constant(*value)),
        }
    }

    /// Returns the underlying block stream decoder, if present.
    ///
    /// # Returns
    /// * `Some(&Arc<BlockStreamDecoder>)` if the decoder is block-based.
    /// * `None` if the decoder is constant-valued.
    pub fn block_stream(&self) -> Option<&Arc<BlockStreamDecoder>> {
        match self {
            BitBufferDecoder::Blocks(decoder) => Some(decoder.block_stream()),
            BitBufferDecoder::Constant(_) => None,
        }
    }
}

/// Reader for bit-packed boolean buffers.
///
/// This enum provides an interface for reading boolean values from a bit-packed buffer,
/// supporting both block-based and constant representations. It is typically constructed
/// via [`BitBufferDecoder::create_reader`].
pub enum BitBufferReader {
    Blocks(Box<PrimitiveBufferReader>),
    Constant(bool),
}

impl BitBufferReader {
    /// Reads a range of boolean values from the buffer and returns it as a
    /// `ValueSequence` of bytes with [`BasicType::Boolean`], where each byte is
    /// either `0` or `1`.
    ///
    /// # Arguments
    /// * `bit_range` - The range of bit positions to read.
    ///
    /// # Returns
    /// An [`arrow_buffer::BooleanBuffer`] containing the requested values.
    pub fn read_range(&mut self, bit_range: Range<u64>) -> Result<ValueSequence> {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Boolean,
            ..Default::default()
        };
        let res = match self.read_range_as_presence(bit_range)? {
            Presence::Trivial(len) => ValueSequence::from_value(len, 1u8, type_desc),
            Presence::Nulls(len) => ValueSequence::from_value(len, 0u8, type_desc),
            Presence::Bytes(vec) => ValueSequence {
                presence: Presence::Trivial(vec.len()),
                values: Values::from_vec(vec),
                offsets: None,
                type_desc,
            },
        };
        Ok(res)
    }

    /// Reads a range of boolean values from the buffer and returns it as Arrow
    /// `BooleanBuffer`.
    ///
    /// # Arguments
    /// * `bit_range` - The range of bit positions to read.
    ///
    /// # Returns
    /// An [`arrow_buffer::BooleanBuffer`] containing the requested values.
    ///
    /// # Notes
    /// - For block-based readers, this reads the corresponding bytes, extracts the bits,
    ///   and returns a BooleanBuffer with the correct offset and length.
    /// - For constant readers, this returns a BooleanBuffer filled with the constant value.
    pub fn read_range_as_arrow_boolean(&mut self, bit_range: Range<u64>) -> Result<BooleanBuffer> {
        let len = (bit_range.end - bit_range.start) as usize;
        match self {
            BitBufferReader::Blocks(reader) => {
                let byte_range = bit_range.start / 8..bit_range.end.div_ceil(8);
                let offset = (bit_range.start - byte_range.start * 8) as usize;
                let bytes = reader.read_range(byte_range)?.values.into_inner();
                let buffer = amudai_arrow_compat::buffers::aligned_vec_to_buf(bytes);
                Ok(BooleanBuffer::new(buffer, offset, len))
            }
            BitBufferReader::Constant(value) => {
                if *value {
                    Ok(BooleanBuffer::new_set(len))
                } else {
                    Ok(BooleanBuffer::new_unset(len))
                }
            }
        }
    }

    /// Reads and decodes the complete block that contains the specified bit position.
    ///
    /// This method finds the block containing the given bit position and returns the entire
    /// decoded block. The block data is converted from packed bits to individual boolean bytes,
    /// where each byte is either `0` (false) or `1` (true).
    ///
    /// # Arguments
    ///
    /// * `position` - A logical bit position within the desired block. The method will
    ///   find and return the block that contains this position, regardless of where
    ///   within the block the position falls.
    ///
    /// # Returns
    ///
    /// A `DecodedBlock` containing:
    /// - **`values`**: A `ValueSequence` of bytes with [`BasicType::Boolean`], where each
    ///   byte is either `0` (false) or `1` (true). The number of bytes equals the logical
    ///   size of the underlying byte block multiplied by 8.
    /// - **`descriptor`**: Block metadata with logical range adjusted to bit positions
    ///   (multiplied by 8 from the original byte positions).
    ///
    /// # Behavior by Reader Type
    ///
    /// - **Block-based reader**: Reads the corresponding byte block, unpacks each byte
    ///   into 8 individual boolean bytes, and adjusts the logical range to bit positions.
    /// - **Constant reader**: Returns an ephemeral block filled with the constant boolean
    ///   value, sized according to the standard ephemeral block size.
    ///
    /// # Errors
    ///
    /// Returns an error if the bit position is invalid or if the underlying block
    /// cannot be read or decoded.
    pub fn read_containing_block(&mut self, position: u64) -> Result<DecodedBlock> {
        match self {
            BitBufferReader::Blocks(reader) => {
                let byte_pos = position / 8;
                let mut block = reader.read_containing_block(byte_pos)?;
                let unpacked_len = block.descriptor.logical_size() * 8;
                let mut unpacked = AlignedByteVec::zeroed(unpacked_len);
                bitpacking::unpack_bits_to_bytes(block.values.as_slice(), 0, &mut unpacked);
                block.values.values = Values::from_vec(unpacked);
                block.values.type_desc.basic_type = BasicType::Boolean;
                block.descriptor.logical_range.start *= 8;
                block.descriptor.logical_range.end *= 8;
                Ok(block)
            }
            BitBufferReader::Constant(value) => Ok(create_ephemeral_bool_block(*value, position)),
        }
    }

    /// Reads a range of boolean values from the buffer and returns it as a
    /// [`Presence`] representation.
    ///
    /// This method provides a way to read boolean data that can be optimally represented
    /// based on the pattern of the data:
    /// - For constant `true` values, returns [`Presence::Trivial`]
    /// - For constant `false` values, returns [`Presence::Nulls`]
    /// - For mixed or block-based data, returns [`Presence::Bytes`] with unpacked bits
    ///
    /// # Arguments
    /// * `bit_range` - The range of bit positions to read as a half-open interval `[start, end)`
    ///
    /// # Returns
    /// A [`Presence`] enum representing the boolean values in the most suitable form:
    /// - [`Presence::Trivial(len)`] - All bits are `true`
    /// - [`Presence::Nulls(len)`] - All bits are `false`
    /// - [`Presence::Bytes(vec)`] - Mixed values as a byte vector where each byte is 0 or 1
    pub fn read_range_as_presence(&mut self, bit_range: Range<u64>) -> Result<Presence> {
        let len = (bit_range.end - bit_range.start) as usize;
        match self {
            BitBufferReader::Blocks(reader) => {
                let byte_range = bit_range.start / 8..bit_range.end.div_ceil(8);
                let offset = (bit_range.start - byte_range.start * 8) as usize;
                let bits = reader.read_range(byte_range)?.values.into_inner();
                let mut presence = AlignedByteVec::zeroed(len);
                bitpacking::unpack_bits_to_bytes(&bits, offset, &mut presence);
                Ok(Presence::Bytes(presence))
            }
            BitBufferReader::Constant(value) => {
                if *value {
                    Ok(Presence::Trivial(len))
                } else {
                    Ok(Presence::Nulls(len))
                }
            }
        }
    }

    /// Attempts to retrieve the constant boolean value if this reader
    /// represents constant data.
    pub fn try_get_constant(&self) -> Option<bool> {
        match self {
            BitBufferReader::Blocks(_) => None,
            BitBufferReader::Constant(value) => Some(*value),
        }
    }
}

pub fn create_ephemeral_bool_block(value: bool, position: u64) -> DecodedBlock {
    let values = create_constant_bool_value_sequence(value, EPHEMERAL_BLOCK_SIZE);
    DecodedBlock {
        values,
        descriptor: get_ephemeral_block_descriptor(position),
    }
}

pub fn create_ephemeral_presence_block(value: bool, position: u64) -> DecodedBlock {
    let presence = if value {
        Presence::Trivial(EPHEMERAL_BLOCK_SIZE)
    } else {
        Presence::Nulls(EPHEMERAL_BLOCK_SIZE)
    };
    DecodedBlock {
        values: ValueSequence {
            values: Values::new(),
            offsets: None,
            presence,
            type_desc: BasicTypeDescriptor {
                basic_type: BasicType::Boolean,
                ..Default::default()
            },
        },
        descriptor: get_ephemeral_block_descriptor(position),
    }
}

pub fn create_constant_bool_value_sequence(value: bool, len: usize) -> ValueSequence {
    ValueSequence::from_value(
        len,
        value as u8,
        BasicTypeDescriptor {
            basic_type: BasicType::Boolean,
            ..Default::default()
        },
    )
}

/// Turns the bit-level `PositionSeries` into a byte-level `PositionSeries`.
struct BitPosToBytePos<P>(P);

impl<P> PositionSeries<u64> for BitPosToBytePos<P>
where
    P: PositionSeries<u64>,
{
    const RANGES: bool = P::RANGES;

    fn into_ranges(self) -> impl IntoIterator<Item = Range<u64>> + Clone {
        self.0.into_ranges().mapped(bit_ranges_to_byte_ranges)
    }

    fn into_positions(self) -> impl IntoIterator<Item = u64> + Clone {
        self.0
            .into_positions()
            .mapped(bit_positions_to_byte_positions)
    }
}

/// Converts an iterator of bit ranges (`Range<u64>`) into an iterator of byte ranges
/// (`Range<u64>`), coalescing adjacent or overlapping byte ranges.
///
/// # Arguments
///
/// * `bit_ranges` - An iterator over ranges of bit positions (bit offsets) to be accessed.
///   Each range specifies a half-open interval `[start, end)` of bits.
///
/// # Returns
///
/// An iterator over byte ranges (`Range<u64>`) that cover the input bit ranges.
/// Each output range specifies a half-open interval of byte offsets. Adjacent or
/// overlapping byte ranges are merged into a single range.
fn bit_ranges_to_byte_ranges(
    bit_ranges: impl Iterator<Item = Range<u64>>,
) -> impl Iterator<Item = Range<u64>> {
    bit_ranges
        .into_iter()
        .map(|r| r.start / 8..r.end.div_ceil(8))
        .coalesce(|a, b| {
            if a.end >= b.start {
                Ok(a.start..b.end)
            } else {
                Err((a, b))
            }
        })
}

/// Converts an iterator of bit positions (`u64`) into an iterator of unique byte positions
/// (`u64`), removing duplicate byte positions.
///
/// # Arguments
///
/// * `bit_positions` - An iterator over individual bit positions (bit offsets) to be accessed.
///   Each position specifies a single bit within the bit stream.
///
/// # Returns
///
/// An iterator over byte positions (`u64`) that contain the input bit positions.
/// Each output position specifies a byte offset. Duplicate byte positions are removed,
/// so each byte position appears at most once in the output, even if multiple input
/// bit positions map to the same byte.
fn bit_positions_to_byte_positions(
    bit_positions: impl Iterator<Item = u64>,
) -> impl Iterator<Item = u64> {
    bit_positions.map(|pos| pos / 8).dedup()
}

#[cfg(test)]
#[allow(clippy::single_range_in_vec_init)]
mod tests {
    use std::ops::Range;

    use amudai_common::Result;
    use amudai_encodings::block_encoder::{
        BlockChecksum, BlockEncodingParameters, BlockEncodingPolicy, BlockEncodingProfile,
        PresenceEncoding,
    };
    use arrow_buffer::BooleanBuffer;

    use crate::{
        read::block_stream::empty_hint,
        write::{
            PreparedEncodedBuffer,
            bit_buffer::{BitBufferEncoder, EncodedBitBuffer},
        },
    };

    use super::{
        BitBufferDecoder, BitBufferReader, BlockReaderPrefetch, bit_positions_to_byte_positions,
        bit_ranges_to_byte_ranges,
    };

    fn create_bit_buffer_encoder() -> BitBufferEncoder {
        let temp_store =
            amudai_io_impl::temp_file_store::create_in_memory(8 * 1024 * 1024).unwrap();
        BitBufferEncoder::new(temp_store, Default::default())
    }

    fn create_test_buffer_from_pattern(pattern: &[bool]) -> Result<PreparedEncodedBuffer> {
        let mut encoder = create_bit_buffer_encoder();
        for &value in pattern {
            encoder.append_value(value)?;
        }
        match encoder.finish()? {
            EncodedBitBuffer::Constant(..) => {
                panic!("Expected blocks encoding but got constant");
            }
            EncodedBitBuffer::Blocks(buffer) => Ok(buffer),
        }
    }

    fn create_test_buffer_alternating(count: usize) -> Result<PreparedEncodedBuffer> {
        let pattern: Vec<bool> = (0..count).map(|i| i % 2 == 0).collect();
        create_test_buffer_from_pattern(&pattern)
    }

    fn create_test_buffer_runs(runs: &[(bool, usize)]) -> Result<PreparedEncodedBuffer> {
        let mut encoder = create_bit_buffer_encoder();
        for &(value, count) in runs {
            encoder.append_repeated(count, value)?;
        }
        match encoder.finish()? {
            EncodedBitBuffer::Constant(..) => {
                panic!("Expected blocks encoding but got constant");
            }
            EncodedBitBuffer::Blocks(buffer) => Ok(buffer),
        }
    }

    fn verify_boolean_buffer_values(buffer: &BooleanBuffer, expected_values: &[bool]) {
        assert_eq!(buffer.len(), expected_values.len());
        for (i, &expected) in expected_values.iter().enumerate() {
            assert_eq!(
                buffer.value(i),
                expected,
                "Mismatch at index {}: expected {}, got {}",
                i,
                expected,
                buffer.value(i)
            );
        }
    }

    fn verify_boolean_buffer_constant(buffer: &BooleanBuffer, value: bool, length: usize) {
        assert_eq!(buffer.len(), length);
        for i in 0..length {
            assert_eq!(
                buffer.value(i),
                value,
                "Expected constant {} but got {} at index {}",
                value,
                buffer.value(i),
                i
            );
        }
    }

    fn verify_bit_ranges_to_byte_ranges(bit_ranges: Vec<Range<u64>>, byte_ranges: Vec<Range<u64>>) {
        let res = bit_ranges_to_byte_ranges(bit_ranges.iter().cloned());
        let actual = res.collect::<Vec<_>>();
        assert_eq!(actual, byte_ranges);
    }

    #[test]
    fn test_bit_ranges_to_byte_ranges() {
        verify_bit_ranges_to_byte_ranges(vec![0..1], vec![0..1]);

        verify_bit_ranges_to_byte_ranges(vec![0..5, 6..8], vec![0..1]);

        verify_bit_ranges_to_byte_ranges(vec![0..5, 6..9], vec![0..2]);

        verify_bit_ranges_to_byte_ranges(
            vec![1000..2000, 2003..3000, 3020..3030],
            vec![125..375, 377..379],
        );

        verify_bit_ranges_to_byte_ranges(vec![0..16], vec![0..2]);

        verify_bit_ranges_to_byte_ranges(vec![8..16], vec![1..2]);

        verify_bit_ranges_to_byte_ranges(vec![0..8], vec![0..1]);

        verify_bit_ranges_to_byte_ranges(vec![0..4, 8..12, 16..24], vec![0..3]);

        verify_bit_ranges_to_byte_ranges(vec![], vec![]);

        verify_bit_ranges_to_byte_ranges(vec![3..10], vec![0..2]);

        verify_bit_ranges_to_byte_ranges(vec![2..6], vec![0..1]);

        verify_bit_ranges_to_byte_ranges(vec![6..10], vec![0..2]);
    }

    #[test]
    fn test_bit_ranges_to_byte_ranges_edge_cases() {
        // Single bit ranges at various positions
        verify_bit_ranges_to_byte_ranges(vec![0..1], vec![0..1]);
        verify_bit_ranges_to_byte_ranges(vec![7..8], vec![0..1]);
        verify_bit_ranges_to_byte_ranges(vec![8..9], vec![1..2]);
        verify_bit_ranges_to_byte_ranges(vec![15..16], vec![1..2]);

        // Ranges that span exactly one byte
        verify_bit_ranges_to_byte_ranges(vec![0..8], vec![0..1]);
        verify_bit_ranges_to_byte_ranges(vec![8..16], vec![1..2]);

        // Ranges that span multiple bytes
        verify_bit_ranges_to_byte_ranges(vec![0..17], vec![0..3]);
        verify_bit_ranges_to_byte_ranges(vec![3..19], vec![0..3]);

        // Adjacent ranges that should be coalesced
        verify_bit_ranges_to_byte_ranges(vec![0..8, 8..16], vec![0..2]);
        verify_bit_ranges_to_byte_ranges(vec![0..4, 4..8, 8..12], vec![0..2]);

        // Overlapping ranges
        verify_bit_ranges_to_byte_ranges(vec![0..10, 5..15], vec![0..2]);
        verify_bit_ranges_to_byte_ranges(vec![0..16, 8..24], vec![0..3]);
    }

    fn verify_bit_positions_to_byte_positions(
        bit_positions: Vec<u64>,
        expected_byte_positions: Vec<u64>,
    ) {
        let result = bit_positions_to_byte_positions(bit_positions.iter().cloned());
        let actual = result.collect::<Vec<_>>();
        assert_eq!(actual, expected_byte_positions);
    }

    #[test]
    fn test_bit_positions_to_byte_positions() {
        // Empty input
        verify_bit_positions_to_byte_positions(vec![], vec![]);

        // Single bit positions in first byte
        verify_bit_positions_to_byte_positions(vec![0], vec![0]);
        verify_bit_positions_to_byte_positions(vec![7], vec![0]);

        // Single bit positions in different bytes
        verify_bit_positions_to_byte_positions(vec![8], vec![1]);
        verify_bit_positions_to_byte_positions(vec![15], vec![1]);
        verify_bit_positions_to_byte_positions(vec![16], vec![2]);

        // Multiple positions in same byte - should deduplicate consecutive ones
        verify_bit_positions_to_byte_positions(vec![0, 1, 2, 7], vec![0]);
        verify_bit_positions_to_byte_positions(vec![8, 9, 15], vec![1]);

        // Multiple positions across different bytes
        verify_bit_positions_to_byte_positions(vec![0, 8], vec![0, 1]);
        verify_bit_positions_to_byte_positions(vec![7, 8], vec![0, 1]);
        verify_bit_positions_to_byte_positions(vec![0, 8, 16], vec![0, 1, 2]);

        // Mixed positions with consecutive duplicates (should deduplicate consecutive byte positions only)
        verify_bit_positions_to_byte_positions(vec![0, 1, 8, 9, 16], vec![0, 1, 2]);
        verify_bit_positions_to_byte_positions(vec![0, 3, 7, 8, 11, 15], vec![0, 1]);

        // Large bit positions
        verify_bit_positions_to_byte_positions(vec![1000, 2000, 3000], vec![125, 250, 375]);

        // Positions that map to same bytes (testing consecutive deduplication)
        verify_bit_positions_to_byte_positions(vec![0, 1, 2, 3, 4, 5, 6, 7], vec![0]);
        verify_bit_positions_to_byte_positions(vec![8, 9, 10, 11, 12, 13, 14, 15], vec![1]);

        // Mixed scenario with gaps and consecutive duplicates
        verify_bit_positions_to_byte_positions(vec![0, 2, 8, 10, 16, 18, 24, 26], vec![0, 1, 2, 3]);

        // Sequential positions (consecutive duplicates should be removed)
        verify_bit_positions_to_byte_positions(vec![15, 16, 17], vec![1, 2]);
        verify_bit_positions_to_byte_positions(vec![7, 8, 9], vec![0, 1]);
    }

    #[test]
    fn test_bit_positions_to_byte_positions_edge_cases() {
        // Byte boundary positions
        verify_bit_positions_to_byte_positions(vec![0, 8, 16, 24], vec![0, 1, 2, 3]);

        // Just before byte boundaries
        verify_bit_positions_to_byte_positions(vec![7, 15, 23, 31], vec![0, 1, 2, 3]);

        // Mix of boundary and non-boundary positions
        verify_bit_positions_to_byte_positions(vec![7, 8, 15, 16], vec![0, 1, 2]);

        // Large gaps between positions
        verify_bit_positions_to_byte_positions(vec![0, 1000, 2000], vec![0, 125, 250]);

        // Many consecutive positions mapping to few bytes (consecutive dedup should work)
        verify_bit_positions_to_byte_positions(
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            vec![0, 1],
        );

        // Sparse positions across many bytes
        verify_bit_positions_to_byte_positions(
            vec![0, 100, 200, 300, 400],
            vec![0, 12, 25, 37, 50],
        );

        // Test consecutive duplicate byte positions are removed
        verify_bit_positions_to_byte_positions(vec![0, 1, 8, 9], vec![0, 1]);
        verify_bit_positions_to_byte_positions(vec![16, 17, 24, 25], vec![2, 3]);

        // Test with positions at exact byte boundaries
        verify_bit_positions_to_byte_positions(vec![8, 16, 24, 32], vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_create_reader_empty_ranges() {
        let buffer = create_test_buffer_alternating(100).unwrap();
        let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
        let reader = decoder
            .create_reader(empty_hint(), BlockReaderPrefetch::Disabled)
            .unwrap();
        match reader {
            BitBufferReader::Blocks(_) => {}
            _ => panic!("Expected blocks reader"),
        }
    }

    #[test]
    fn test_create_reader_multiple_ranges() {
        let buffer = create_test_buffer_alternating(1000).unwrap();
        let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
        let reader = decoder
            .create_reader(
                vec![0..100, 200..300, 500..600].into_iter(),
                BlockReaderPrefetch::Enabled,
            )
            .unwrap();
        match reader {
            BitBufferReader::Blocks(_) => {}
            _ => panic!("Expected blocks reader"),
        }
    }

    #[test]
    fn test_reader_constant_true_various_ranges() {
        let decoder = BitBufferDecoder::from_constant(true);
        let mut reader = decoder
            .create_reader(vec![0..1000].into_iter(), BlockReaderPrefetch::Disabled)
            .unwrap();

        // Empty range
        let result = reader.read_range_as_arrow_boolean(0..0).unwrap();
        verify_boolean_buffer_constant(&result, true, 0);

        // Single bit
        let result = reader.read_range_as_arrow_boolean(0..1).unwrap();
        verify_boolean_buffer_constant(&result, true, 1);

        // Byte boundary
        let result = reader.read_range_as_arrow_boolean(0..8).unwrap();
        verify_boolean_buffer_constant(&result, true, 8);

        // Non-byte-aligned
        let result = reader.read_range_as_arrow_boolean(0..15).unwrap();
        verify_boolean_buffer_constant(&result, true, 15);

        // Large range
        let result = reader.read_range_as_arrow_boolean(100..500).unwrap();
        verify_boolean_buffer_constant(&result, true, 400);
    }

    #[test]
    fn test_reader_constant_false_various_ranges() {
        let decoder = BitBufferDecoder::from_constant(false);
        let mut reader = decoder
            .create_reader(vec![0..1000].into_iter(), BlockReaderPrefetch::Disabled)
            .unwrap();

        // Empty range
        let result = reader.read_range_as_arrow_boolean(0..0).unwrap();
        verify_boolean_buffer_constant(&result, false, 0);

        // Single bit
        let result = reader.read_range_as_arrow_boolean(0..1).unwrap();
        verify_boolean_buffer_constant(&result, false, 1);

        // Byte boundary
        let result = reader.read_range_as_arrow_boolean(0..8).unwrap();
        verify_boolean_buffer_constant(&result, false, 8);

        // Non-byte-aligned
        let result = reader.read_range_as_arrow_boolean(0..15).unwrap();
        verify_boolean_buffer_constant(&result, false, 15);

        // Large range
        let result = reader.read_range_as_arrow_boolean(100..500).unwrap();
        verify_boolean_buffer_constant(&result, false, 400);
    }

    #[test]
    fn test_reader_constant_arbitrary_positions() {
        let decoder = BitBufferDecoder::from_constant(true);
        let mut reader = decoder
            .create_reader(vec![0..10000].into_iter(), BlockReaderPrefetch::Disabled)
            .unwrap();

        // Test arbitrary start/end positions
        let ranges = vec![50..75, 123..456, 1000..1001, 5000..5100, 9900..10000];

        for range in ranges {
            let result = reader.read_range_as_arrow_boolean(range.clone()).unwrap();
            verify_boolean_buffer_constant(&result, true, (range.end - range.start) as usize);
        }
    }

    #[test]
    fn test_reader_blocks_exact_byte_boundaries() {
        let pattern: Vec<bool> = (0..64).map(|i| i % 2 == 0).collect(); // 8 bytes worth
        let buffer = create_test_buffer_from_pattern(&pattern).unwrap();
        let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
        let mut reader = decoder
            .create_reader(vec![0..64].into_iter(), BlockReaderPrefetch::Disabled)
            .unwrap();

        // Read exactly one byte
        let result = reader.read_range_as_arrow_boolean(0..8).unwrap();
        verify_boolean_buffer_values(&result, &pattern[0..8]);

        // Read exactly two bytes
        let result = reader.read_range_as_arrow_boolean(8..24).unwrap();
        verify_boolean_buffer_values(&result, &pattern[8..24]);

        // Read all bytes
        let result = reader.read_range_as_arrow_boolean(0..64).unwrap();
        verify_boolean_buffer_values(&result, &pattern);
    }

    #[test]
    fn test_reader_blocks_cross_byte_boundaries() {
        let pattern: Vec<bool> = (0..64).map(|i| i % 4 < 2).collect(); // 2 true, 2 false pattern
        let buffer = create_test_buffer_from_pattern(&pattern).unwrap();
        let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
        let mut reader = decoder
            .create_reader(vec![0..64].into_iter(), BlockReaderPrefetch::Disabled)
            .unwrap();

        // Read across single byte boundary
        let result = reader.read_range_as_arrow_boolean(4..12).unwrap();
        verify_boolean_buffer_values(&result, &pattern[4..12]);

        // Read across multiple byte boundaries
        let result = reader.read_range_as_arrow_boolean(10..50).unwrap();
        verify_boolean_buffer_values(&result, &pattern[10..50]);

        // Read with bit offset in first byte
        let result = reader.read_range_as_arrow_boolean(3..19).unwrap();
        verify_boolean_buffer_values(&result, &pattern[3..19]);
    }

    #[test]
    fn test_reader_blocks_partial_bits() {
        let pattern: Vec<bool> = (0..15).map(|i| i % 3 == 0).collect(); // 15 bits (not byte-aligned)
        let buffer = create_test_buffer_from_pattern(&pattern).unwrap();
        let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
        let mut reader = decoder
            .create_reader(vec![0..15].into_iter(), BlockReaderPrefetch::Disabled)
            .unwrap();

        // Read single bit
        let result = reader.read_range_as_arrow_boolean(0..1).unwrap();
        verify_boolean_buffer_values(&result, &pattern[0..1]);

        // Read partial first byte
        let result = reader.read_range_as_arrow_boolean(0..7).unwrap();
        verify_boolean_buffer_values(&result, &pattern[0..7]);

        // Read all bits
        let result = reader.read_range_as_arrow_boolean(0..15).unwrap();
        verify_boolean_buffer_values(&result, &pattern);

        // Read from middle
        let result = reader.read_range_as_arrow_boolean(5..12).unwrap();
        verify_boolean_buffer_values(&result, &pattern[5..12]);

        // Read last few bits
        let result = reader.read_range_as_arrow_boolean(10..15).unwrap();
        verify_boolean_buffer_values(&result, &pattern[10..15]);
    }

    #[test]
    fn test_reader_blocks_empty_range() {
        let buffer = create_test_buffer_alternating(64).unwrap();
        let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
        let mut reader = decoder
            .create_reader(vec![0..64].into_iter(), BlockReaderPrefetch::Disabled)
            .unwrap();

        let result = reader.read_range_as_arrow_boolean(0..0).unwrap();
        assert_eq!(result.len(), 0);

        let result = reader.read_range_as_arrow_boolean(32..32).unwrap();
        assert_eq!(result.len(), 0);

        let result = reader.read_range_as_arrow_boolean(64..64).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_reader_blocks_single_bit_reads() {
        let pattern: Vec<bool> = vec![true, false, true, true, false, false, true, false];
        let buffer = create_test_buffer_from_pattern(&pattern).unwrap();
        let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
        let mut reader = decoder
            .create_reader(vec![0..8].into_iter(), BlockReaderPrefetch::Disabled)
            .unwrap();

        for i in 0..8 {
            let result = reader.read_range_as_arrow_boolean(i..i + 1).unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result.value(0), pattern[i as usize]);
        }
    }

    #[test]
    fn test_roundtrip_constant_patterns() {
        // Test all-true constant
        let mut encoder = create_bit_buffer_encoder();
        encoder.append_repeated(1000, true).unwrap();
        match encoder.finish().unwrap() {
            EncodedBitBuffer::Constant(value, count) => {
                assert!(value);
                assert_eq!(count, 1000);

                let decoder = BitBufferDecoder::from_constant(value);
                let mut reader = decoder
                    .create_reader(vec![0..1000].into_iter(), BlockReaderPrefetch::Disabled)
                    .unwrap();
                let result = reader.read_range_as_arrow_boolean(0..1000).unwrap();
                verify_boolean_buffer_constant(&result, true, 1000);
            }
            _ => panic!("Expected constant encoding"),
        }

        // Test all-false constant
        let mut encoder = create_bit_buffer_encoder();
        encoder.append_repeated(1000, false).unwrap();
        match encoder.finish().unwrap() {
            EncodedBitBuffer::Constant(value, count) => {
                assert!(!value);
                assert_eq!(count, 1000);

                let decoder = BitBufferDecoder::from_constant(value);
                let mut reader = decoder
                    .create_reader(vec![0..1000].into_iter(), BlockReaderPrefetch::Disabled)
                    .unwrap();
                let result = reader.read_range_as_arrow_boolean(0..1000).unwrap();
                verify_boolean_buffer_constant(&result, false, 1000);
            }
            _ => panic!("Expected constant encoding"),
        }
    }

    #[test]
    fn test_roundtrip_alternating_pattern() {
        let pattern: Vec<bool> = (0..100).map(|i| i % 2 == 0).collect();
        let buffer = create_test_buffer_from_pattern(&pattern).unwrap();
        let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
        let mut reader = decoder
            .create_reader(vec![0..100].into_iter(), BlockReaderPrefetch::Disabled)
            .unwrap();

        let result = reader.read_range_as_arrow_boolean(0..100).unwrap();
        verify_boolean_buffer_values(&result, &pattern);
    }

    #[test]
    fn test_roundtrip_random_patterns() {
        for seed in [12345, 67890, 42, 9999, 1] {
            let pattern: Vec<bool> = {
                fastrand::seed(seed);
                (0..200).map(|_| fastrand::bool()).collect()
            };

            let buffer = create_test_buffer_from_pattern(&pattern).unwrap();
            let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
            let mut reader = decoder
                .create_reader(vec![0..200].into_iter(), BlockReaderPrefetch::Disabled)
                .unwrap();

            let result = reader.read_range_as_arrow_boolean(0..200).unwrap();
            verify_boolean_buffer_values(&result, &pattern);
        }
    }

    #[test]
    fn test_roundtrip_run_patterns() {
        // Test various run patterns
        let runs = vec![
            (true, 5),
            (false, 10),
            (true, 1),
            (false, 8),
            (true, 15),
            (false, 3),
        ];

        let mut expected_pattern = Vec::new();
        for &(value, count) in &runs {
            expected_pattern.extend(std::iter::repeat_n(value, count));
        }

        let buffer = create_test_buffer_runs(&runs).unwrap();
        let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
        let mut reader = decoder
            .create_reader(
                vec![0..expected_pattern.len() as u64].into_iter(),
                BlockReaderPrefetch::Disabled,
            )
            .unwrap();

        let result = reader
            .read_range_as_arrow_boolean(0..expected_pattern.len() as u64)
            .unwrap();
        verify_boolean_buffer_values(&result, &expected_pattern);
    }

    #[test]
    fn test_roundtrip_complex_patterns() {
        // Test block patterns
        let block_pattern: Vec<bool> = (0..128).map(|i| (i / 8) % 2 == 0).collect();
        let buffer = create_test_buffer_from_pattern(&block_pattern).unwrap();
        let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
        let mut reader = decoder
            .create_reader(vec![0..128].into_iter(), BlockReaderPrefetch::Disabled)
            .unwrap();
        let result = reader.read_range_as_arrow_boolean(0..128).unwrap();
        verify_boolean_buffer_values(&result, &block_pattern);

        // Test checkerboard pattern
        let checkerboard: Vec<bool> = (0..64).map(|i| (i % 2) == ((i / 8) % 2)).collect();
        let buffer = create_test_buffer_from_pattern(&checkerboard).unwrap();
        let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
        let mut reader = decoder
            .create_reader(vec![0..64].into_iter(), BlockReaderPrefetch::Disabled)
            .unwrap();
        let result = reader.read_range_as_arrow_boolean(0..64).unwrap();
        verify_boolean_buffer_values(&result, &checkerboard);
    }

    // Range Handling Tests

    #[test]
    fn test_reader_disjoint_ranges() {
        let pattern: Vec<bool> = (0..1000).map(|i| i % 7 < 3).collect();
        let buffer = create_test_buffer_from_pattern(&pattern).unwrap();
        let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();

        // Create reader with disjoint ranges
        let mut reader = decoder
            .create_reader(
                vec![0..200, 400..600, 800..1000].into_iter(),
                BlockReaderPrefetch::Enabled,
            )
            .unwrap();

        // Read from first range
        let result = reader.read_range_as_arrow_boolean(50..150).unwrap();
        verify_boolean_buffer_values(&result, &pattern[50..150]);

        // Read from second range
        let result = reader.read_range_as_arrow_boolean(450..550).unwrap();
        verify_boolean_buffer_values(&result, &pattern[450..550]);

        // Read from third range
        let result = reader.read_range_as_arrow_boolean(850..950).unwrap();
        verify_boolean_buffer_values(&result, &pattern[850..950]);
    }

    #[test]
    fn test_reader_overlapping_ranges() {
        let pattern: Vec<bool> = (0..100).map(|i| i % 5 == 0).collect();
        let buffer = create_test_buffer_from_pattern(&pattern).unwrap();
        let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
        let mut reader = decoder
            .create_reader(vec![0..100].into_iter(), BlockReaderPrefetch::Disabled)
            .unwrap();

        // Read overlapping ranges
        let result1 = reader.read_range_as_arrow_boolean(10..50).unwrap();
        let result2 = reader.read_range_as_arrow_boolean(30..70).unwrap();

        verify_boolean_buffer_values(&result1, &pattern[10..50]);
        verify_boolean_buffer_values(&result2, &pattern[30..70]);

        // Verify overlapping section matches
        for i in 0..20 {
            assert_eq!(result1.value(i + 20), result2.value(i));
        }
    }

    #[test]
    fn test_reader_sequential_reads() {
        let pattern: Vec<bool> = (0..200).map(|i| (i / 10) % 2 == 0).collect();
        let buffer = create_test_buffer_from_pattern(&pattern).unwrap();
        let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
        let mut reader = decoder
            .create_reader(vec![0..200].into_iter(), BlockReaderPrefetch::Disabled)
            .unwrap();

        // Read in sequential chunks
        for start in (0..200).step_by(25) {
            let end = (start + 25).min(200);
            let result = reader.read_range_as_arrow_boolean(start..end).unwrap();
            verify_boolean_buffer_values(&result, &pattern[start as usize..end as usize]);
        }
    }

    #[test]
    fn test_reader_random_access() {
        let pattern: Vec<bool> = (0..500).map(|i| i % 13 < 7).collect();
        let buffer = create_test_buffer_from_pattern(&pattern).unwrap();
        let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
        let mut reader = decoder
            .create_reader(vec![0..500].into_iter(), BlockReaderPrefetch::Disabled)
            .unwrap();

        // Random access reads
        let positions = vec![
            0..10,
            50..60,
            123..145,
            200..201,
            300..350,
            450..500,
            75..100,
        ];

        for range in positions {
            let result = reader.read_range_as_arrow_boolean(range.clone()).unwrap();
            verify_boolean_buffer_values(
                &result,
                &pattern[range.start as usize..range.end as usize],
            );
        }
    }

    // Edge Case and Size Variation Tests

    #[test]
    fn test_reader_various_bit_sizes() {
        let sizes = vec![
            3, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129,
        ];

        for size in sizes {
            let pattern: Vec<bool> = (0..size).map(|i| i % 3 == 0).collect();
            let buffer = create_test_buffer_from_pattern(&pattern).unwrap();
            let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
            let mut reader = decoder
                .create_reader(
                    vec![0..size as u64].into_iter(),
                    BlockReaderPrefetch::Disabled,
                )
                .unwrap();

            let result = reader.read_range_as_arrow_boolean(0..size as u64).unwrap();
            verify_boolean_buffer_values(&result, &pattern);
        }
    }

    #[test]
    fn test_reader_large_patterns() {
        let large_size = 10000;
        let pattern: Vec<bool> = (0..large_size).map(|i| (i / 100) % 2 == 0).collect();
        let buffer = create_test_buffer_from_pattern(&pattern).unwrap();
        let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
        let mut reader = decoder
            .create_reader(
                vec![0..large_size as u64].into_iter(),
                BlockReaderPrefetch::Enabled,
            )
            .unwrap();

        // Read the entire large buffer
        let result = reader
            .read_range_as_arrow_boolean(0..large_size as u64)
            .unwrap();
        verify_boolean_buffer_values(&result, &pattern);

        // Read chunks from the large buffer
        for chunk_start in (0..large_size).step_by(1000) {
            let chunk_end = (chunk_start + 1000).min(large_size);
            let result = reader
                .read_range_as_arrow_boolean(chunk_start as u64..chunk_end as u64)
                .unwrap();
            verify_boolean_buffer_values(&result, &pattern[chunk_start..chunk_end]);
        }
    }

    #[test]
    fn test_reader_boundary_conditions() {
        // Test patterns that align and don't align with byte boundaries
        for pattern_size in [7usize, 8, 9, 15, 16, 17, 63, 64, 65] {
            let pattern: Vec<bool> = (0..pattern_size).map(|i| i % 4 == 0).collect();
            let buffer = create_test_buffer_from_pattern(&pattern).unwrap();
            let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
            let mut reader = decoder
                .create_reader(
                    vec![0..pattern_size as u64].into_iter(),
                    BlockReaderPrefetch::Disabled,
                )
                .unwrap();

            // Test various read ranges within the pattern
            let ranges = vec![
                0..1,
                0..pattern_size.min(8),
                1..pattern_size.min(9),
                pattern_size.saturating_sub(8)..pattern_size,
                pattern_size.saturating_sub(1)..pattern_size,
            ];

            for range in ranges {
                if range.start < range.end && range.end <= pattern_size {
                    let result = reader
                        .read_range_as_arrow_boolean(range.start as u64..range.end as u64)
                        .unwrap();
                    verify_boolean_buffer_values(&result, &pattern[range.start..range.end]);
                }
            }
        }
    }

    // Prefetch and Configuration Tests

    #[test]
    fn test_prefetch_modes() {
        let pattern: Vec<bool> = (0..200).map(|i| i % 11 < 5).collect();
        let buffer = create_test_buffer_from_pattern(&pattern).unwrap();
        let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();

        // Test with prefetch disabled
        let mut reader_no_prefetch = decoder
            .create_reader(vec![0..200].into_iter(), BlockReaderPrefetch::Disabled)
            .unwrap();
        let result_no_prefetch = reader_no_prefetch
            .read_range_as_arrow_boolean(0..200)
            .unwrap();

        // Test with prefetch enabled
        let mut reader_prefetch = decoder
            .create_reader(vec![0..200].into_iter(), BlockReaderPrefetch::Enabled)
            .unwrap();
        let result_prefetch = reader_prefetch.read_range_as_arrow_boolean(0..200).unwrap();

        // Results should be identical regardless of prefetch mode
        verify_boolean_buffer_values(&result_no_prefetch, &pattern);
        verify_boolean_buffer_values(&result_prefetch, &pattern);

        for i in 0..200 {
            assert_eq!(result_no_prefetch.value(i), result_prefetch.value(i));
        }
    }

    // Integration Tests with Different Encoding Policies

    #[test]
    fn test_decoder_with_different_encoding_policies() {
        let pattern: Vec<bool> = (0..1000).map(|i| i % 17 < 8).collect();

        // Test with different encoding policies
        let policies = vec![
            BlockEncodingPolicy {
                parameters: BlockEncodingParameters {
                    checksum: BlockChecksum::Enabled,
                    presence: PresenceEncoding::Disabled,
                },
                profile: BlockEncodingProfile::MinimalCompression,
                size_constraints: None,
            },
            BlockEncodingPolicy {
                parameters: BlockEncodingParameters {
                    checksum: BlockChecksum::Disabled,
                    presence: PresenceEncoding::Disabled,
                },
                profile: BlockEncodingProfile::HighCompression,
                size_constraints: None,
            },
            BlockEncodingPolicy {
                parameters: BlockEncodingParameters {
                    checksum: BlockChecksum::Enabled,
                    presence: PresenceEncoding::Disabled,
                },
                profile: BlockEncodingProfile::Balanced,
                size_constraints: None,
            },
        ];

        for policy in policies {
            let temp_store =
                amudai_io_impl::temp_file_store::create_in_memory(8 * 1024 * 1024).unwrap();
            let mut encoder =
                crate::write::bit_buffer::BitBufferBlocksEncoder::with_policy(policy, temp_store)
                    .unwrap();

            for &value in &pattern {
                encoder.append_value(value).unwrap();
            }

            let buffer = encoder.finish().unwrap();
            let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
            let mut reader = decoder
                .create_reader(vec![0..1000].into_iter(), BlockReaderPrefetch::Enabled)
                .unwrap();

            let result = reader.read_range_as_arrow_boolean(0..1000).unwrap();
            verify_boolean_buffer_values(&result, &pattern);
        }
    }

    // Property-based and Stress Tests

    #[test]
    fn test_property_roundtrip_consistency() {
        // Test that any bit pattern can be encoded and decoded correctly
        for test_case in 0..20 {
            fastrand::seed(test_case * 12345);
            let size = fastrand::usize(1..=2000);
            let pattern: Vec<bool> = (0..size).map(|_| fastrand::bool()).collect();

            let buffer = create_test_buffer_from_pattern(&pattern).unwrap();
            let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
            let mut reader = decoder
                .create_reader(
                    vec![0..size as u64].into_iter(),
                    BlockReaderPrefetch::Disabled,
                )
                .unwrap();

            // Test reading the entire pattern
            let result = reader.read_range_as_arrow_boolean(0..size as u64).unwrap();
            verify_boolean_buffer_values(&result, &pattern);

            // Test reading random subranges
            for _ in 0..10 {
                let start = fastrand::usize(0..size);
                let end = fastrand::usize(start..=size);
                let result = reader
                    .read_range_as_arrow_boolean(start as u64..end as u64)
                    .unwrap();
                verify_boolean_buffer_values(&result, &pattern[start..end]);
            }
        }
    }

    #[test]
    fn test_stress_many_small_reads() {
        let pattern: Vec<bool> = (0..1000).map(|i| i % 23 < 11).collect();
        let buffer = create_test_buffer_from_pattern(&pattern).unwrap();
        let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
        let mut reader = decoder
            .create_reader(vec![0..1000].into_iter(), BlockReaderPrefetch::Enabled)
            .unwrap();

        // Perform many small reads
        for i in (0..1000).step_by(7) {
            let end = (i + 3).min(1000);
            let result = reader
                .read_range_as_arrow_boolean(i as u64..end as u64)
                .unwrap();
            verify_boolean_buffer_values(&result, &pattern[i..end]);
        }
    }

    #[test]
    fn test_stress_overlapping_reads() {
        let pattern: Vec<bool> = (0..500).map(|i| i % 19 < 9).collect();
        let buffer = create_test_buffer_from_pattern(&pattern).unwrap();
        let decoder = BitBufferDecoder::from_prepared_buffer(&buffer).unwrap();
        let mut reader = decoder
            .create_reader(vec![0..500].into_iter(), BlockReaderPrefetch::Disabled)
            .unwrap();

        // Perform many overlapping reads
        for start in (0..400).step_by(20) {
            let end = start + 50;
            let result = reader
                .read_range_as_arrow_boolean(start as u64..end as u64)
                .unwrap();
            verify_boolean_buffer_values(&result, &pattern[start..end]);
        }
    }
}
