use std::{borrow::Cow, ops::Range, sync::Arc};

use amudai_common::Result;
use amudai_encodings::block_decoder::BlockDecoder;
use amudai_format::schema::BasicTypeDescriptor;
use amudai_sequence::sequence::ValueSequence;

use super::{block_map::BlockMap, block_map_ops::BlockDescriptor, block_stream::BlockReader};

/// A generic reader for block-encoded value buffers that efficiently accesses and decodes
/// blocks of data.
///
/// `GenericBufferReader` provides a high-level interface for reading typed values
/// from an encoded block stream. It handles the details of:
/// - Block-level caching to improve performance for sequential or overlapping reads
/// - Finding and reading the correct blocks for a given logical position range
/// - Decoding binary block data
/// - Merging data from multiple blocks into a single result sequence
///
/// This reader is typically created by calling `PrimitiveBufferDecoder::create_reader()`.
pub struct GenericBufferReader<Decoder> {
    /// The underlying block reader that handles block I/O operations
    block_reader: BlockReader,

    /// Reference to the block map that translates logical positions to storage blocks
    block_map: Arc<BlockMap>,

    /// Decoder responsible for translating raw encoded block data into plain sequences.
    block_decoder: Decoder,

    /// Type information for the values stored in this buffer
    basic_type: BasicTypeDescriptor,

    /// The most recently decoded block descriptor, cached to optimize consecutive reads
    cached_block: Option<BlockDescriptor>,

    /// Cached decoded sequence data corresponding to the cached block
    cached_sequence: Option<ValueSequence>,
}

impl<Decoder> GenericBufferReader<Decoder> {
    pub fn new(
        basic_type: BasicTypeDescriptor,
        block_reader: BlockReader,
        block_decoder: Decoder,
    ) -> GenericBufferReader<Decoder> {
        let block_map = block_reader.block_map().clone();
        GenericBufferReader {
            block_reader,
            block_map,
            block_decoder,
            basic_type,
            cached_block: None,
            cached_sequence: None,
        }
    }

    /// Returns the basic type descriptor for values in this buffer.
    pub fn basic_type(&self) -> BasicTypeDescriptor {
        self.basic_type
    }
}

impl<Decoder: BlockDecoder> GenericBufferReader<Decoder> {
    /// Reads and decodes the specified logical position range of values
    /// from the underlying encoded buffer (block stream).
    ///
    /// This method handles all the details of:
    /// - Finding blocks that contain the requested data
    /// - Reading those blocks from storage
    /// - Decoding the binary data into typed values
    /// - Extracting only the requested range
    /// - Merging data from multiple blocks when necessary
    ///
    /// # Arguments
    ///
    /// * `pos_range` - A range of logical positions to read
    ///
    /// # Returns
    ///
    /// * `Ok(Sequence)` - A sequence containing the requested values
    /// * `Err` - If the requested range is invalid or an I/O error occurs
    ///
    /// # Performance Considerations
    ///
    /// The method caches the last block read, which can improve performance
    /// for subsequent sequential or overlapping reads.
    pub fn read(&mut self, pos_range: Range<u64>) -> Result<ValueSequence> {
        // Handle empty range case
        if pos_range.start >= pos_range.end {
            return Ok(ValueSequence::empty(self.basic_type));
        }

        // Find the block range that covers the requested position range
        let block_map = &self.block_map;
        let first_block_idx = block_map.find_block_ordinal(pos_range.start)?;
        let last_block_idx = block_map.find_block_ordinal(pos_range.end.saturating_sub(1))?;

        let mut result = ValueSequence::with_capacity(
            self.basic_type,
            (pos_range.end - pos_range.start) as usize,
        );

        // Process each block in the range
        for ordinal in first_block_idx..=last_block_idx {
            let (decoded, block) = self.decode_block(ordinal)?;
            let block_range = block.logical_range.clone();

            // Calculate the overlap between request range and this block
            let start_pos = pos_range.start.max(block_range.start);
            let end_pos = pos_range.end.min(block_range.end);
            assert!(
                start_pos < end_pos,
                "Invalid range: {start_pos} >= {end_pos}"
            );

            // Calculate relative offset within the block
            let block_offset = start_pos - block_range.start;
            let value_count = end_pos - start_pos;

            // Extract requested range from the decoded data
            result.extend_from_sequence(&decoded, block_offset as usize, value_count as usize);

            if ordinal == last_block_idx {
                if let Cow::Owned(sequence) = decoded {
                    self.cached_block = Some(block);
                    self.cached_sequence = Some(sequence);
                }
            }
        }

        Ok(result)
    }
}

impl<Decoder: BlockDecoder> GenericBufferReader<Decoder> {
    /// Decodes a specific block identified by its ordinal index.
    ///
    /// This method handles block caching to optimize performance:
    /// - If the requested block is already cached, returns the cached data
    /// - Otherwise, reads the block from storage and decodes it
    ///
    /// # Arguments
    ///
    /// * `ordinal` - The ordinal index of the block to decode
    ///
    /// # Returns
    ///
    /// * `Ok((Cow<Sequence>, BlockDescriptor))` - The decoded sequence data and block descriptor
    /// * `Err` - If the block cannot be read or decoded
    fn decode_block(&mut self, ordinal: usize) -> Result<(Cow<ValueSequence>, BlockDescriptor)> {
        let cached_ordinal = self
            .cached_block
            .as_ref()
            .map(|block| block.ordinal as usize);
        if cached_ordinal == Some(ordinal) {
            let sequence = self.cached_sequence.as_ref().expect("cached sequence");
            let block = self.cached_block.clone().unwrap();
            Ok((Cow::Borrowed(sequence), block))
        } else {
            let block = self.block_reader.read_block(ordinal as u32)?;
            block.verify_checksum()?;
            let count = block.descriptor.logical_size();
            let decoded = self.block_decoder.decode(block.data(), count)?;
            Ok((Cow::Owned(decoded), block.descriptor))
        }
    }
}
