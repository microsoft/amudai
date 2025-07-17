use std::{ops::Range, sync::Arc};

use amudai_common::Result;
use amudai_encodings::{
    block_encoder::{BlockEncodingParameters, PresenceEncoding},
    primitive_block_decoder::PrimitiveBlockDecoder,
};
use amudai_format::{defs::shard, schema::BasicTypeDescriptor};
use amudai_io::ReadAt;

use crate::{read::block_stream::BlockStreamDecoder, write::PreparedEncodedBuffer};

use super::{block_stream::BlockReaderPrefetch, generic_buffer::GenericBufferReader};

/// A decoder for primitive-typed buffers that provides access to encoded primitive
/// values.
///
/// This struct serves as the entry point for working with encoded primitive buffers
/// and provides methods to create readers that can efficiently access and decode
/// specific ranges of values.
#[derive(Clone)]
pub struct PrimitiveBufferDecoder {
    /// The underlying block stream containing the encoded blocks
    block_stream: Arc<BlockStreamDecoder>,

    /// Indicates whether presence information is embedded in the block stream
    /// When true, blocks contain presence bits indicating which values are null
    embedded_presence: bool,

    /// The specific primitive type that describes values in this buffer
    basic_type: BasicTypeDescriptor,
}

impl PrimitiveBufferDecoder {
    /// Creates a new `PrimitiveBufferDecoder` from an encoded buffer descriptor.
    ///
    /// # Arguments
    ///
    /// * `reader` - A reference to the storage artifact containing encoded data
    /// * `encoded_buffer` - The buffer descriptor containing metadata
    ///   about the encoded buffer
    /// * `basic_type` - The primitive type descriptor for values in this buffer
    ///
    /// # Returns
    ///
    /// A result containing the initialized decoder if successful
    pub fn from_encoded_buffer(
        reader: Arc<dyn ReadAt>,
        encoded_buffer: &shard::EncodedBuffer,
        basic_type: BasicTypeDescriptor,
    ) -> Result<PrimitiveBufferDecoder> {
        let block_stream = BlockStreamDecoder::from_encoded_buffer(reader, encoded_buffer)?;
        Ok(PrimitiveBufferDecoder {
            block_stream,
            embedded_presence: encoded_buffer.embedded_presence,
            basic_type,
        })
    }

    /// Creates a new `PrimitiveBufferDecoder` from a [`PreparedEncodedBuffer`].
    ///
    /// This constructor is used when you have a fully prepared, in-memory encoded buffer
    /// (typically produced by an encoder in the same process) and want to create a decoder
    /// for reading its contents.
    ///
    /// # Arguments
    ///
    /// * `prepared_buffer` - The prepared encoded buffer containing both the encoded data
    ///   and its descriptor.
    /// * `basic_type` - The primitive type descriptor for values in this buffer.
    ///
    /// # Returns
    ///
    /// Returns a [`Result`] containing the initialized [`PrimitiveBufferDecoder`] if successful,
    /// or an error if the buffer could not be decoded.
    pub fn from_prepared_buffer(
        prepared_buffer: &PreparedEncodedBuffer,
        basic_type: BasicTypeDescriptor,
    ) -> Result<PrimitiveBufferDecoder> {
        Self::from_encoded_buffer(
            prepared_buffer.data.clone(),
            &prepared_buffer.descriptor,
            basic_type,
        )
    }

    /// Returns whether this buffer has embedded presence information.
    pub fn has_embedded_presence(&self) -> bool {
        self.embedded_presence
    }

    /// Returns the basic type descriptor for the values in this buffer.
    pub fn basic_type(&self) -> BasicTypeDescriptor {
        self.basic_type
    }

    /// Creates a reader for accessing specific ranges of values from this buffer.
    ///
    /// This method:
    /// 1. Translates the requested logical position ranges to block ranges
    /// 2. Optimizes those ranges for efficient reading
    /// 3. Creates a reader instance configured to access those blocks
    ///
    /// The returned reader provides a high-level interface for efficiently reading and
    /// decoding values from the specified ranges.
    ///
    /// # Arguments
    ///
    /// * `pos_ranges` - Iterator of logical position ranges to read
    /// * `prefetch` - Whether to enable prefetching of blocks for improved performance
    ///
    /// # Returns
    ///
    /// A result containing the initialized reader if successful
    pub fn create_reader(
        &self,
        pos_ranges: impl Iterator<Item = Range<u64>> + Clone,
        prefetch: BlockReaderPrefetch,
    ) -> Result<PrimitiveBufferReader> {
        let block_reader = self
            .block_stream
            .create_reader_with_position_ranges(pos_ranges, prefetch)?;
        Ok(PrimitiveBufferReader::new(
            self.basic_type,
            block_reader,
            PrimitiveBlockDecoder::new(
                self.encoding_params(),
                self.basic_type,
                Arc::new(Default::default()),
            ),
        ))
    }

    /// Returns the underlying block stream.
    pub fn block_stream(&self) -> &Arc<BlockStreamDecoder> {
        &self.block_stream
    }

    /// Returns the block encoding parameters for this buffer that were used during
    /// the encoding.
    ///
    /// These parameters describe:
    /// - Whether checksums are enabled for data integrity verification
    /// - Whether presence information for null values is encoded in the blocks
    ///
    /// # Returns
    ///
    /// The block encoding parameters applicable to this buffer
    fn encoding_params(&self) -> BlockEncodingParameters {
        BlockEncodingParameters {
            checksum: self.block_stream.checksum_config(),
            presence: if self.embedded_presence {
                PresenceEncoding::Enabled
            } else {
                PresenceEncoding::Disabled
            },
        }
    }
}

/// A reader for primitive value buffers that efficiently accesses and decodes
/// blocks of data.
pub type PrimitiveBufferReader = GenericBufferReader<PrimitiveBlockDecoder>;

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use amudai_common::Result;
    use amudai_encodings::block_encoder::{
        BlockChecksum, BlockEncodingParameters, BlockEncodingPolicy, BlockEncodingProfile,
        PresenceEncoding,
    };
    use amudai_format::schema::{BasicType, BasicTypeDescriptor};
    use arrow_array::Int32Array;

    use crate::{
        read::block_stream::BlockReaderPrefetch,
        write::{PreparedEncodedBuffer, primitive_buffer::PrimitiveBufferEncoder},
    };

    use super::PrimitiveBufferDecoder;

    #[test]
    fn test_decoder_basics() {
        let buffer = create_test_buffer(2000, 100..150).unwrap();
        let decoder = PrimitiveBufferDecoder::from_encoded_buffer(
            buffer.data,
            &buffer.descriptor,
            BasicTypeDescriptor {
                basic_type: BasicType::Int32,
                fixed_size: 0,
                signed: true,
                extended_type: Default::default(),
            },
        )
        .unwrap();
        let ranges = [10u64..100, 200..500];
        let mut reader = decoder
            .create_reader(ranges.iter().cloned(), BlockReaderPrefetch::Enabled)
            .unwrap();
        let seq = reader.read(300..350).unwrap();
        assert_eq!(seq.len(), 50);
        assert_eq!(seq.values.as_slice::<i32>()[0], 300);
        assert_eq!(seq.values.as_slice::<i32>()[49], 349);
    }

    fn encode_test_buffer(
        num_blocks: usize,
        block_value_count: Range<usize>,
    ) -> Result<PrimitiveBufferEncoder> {
        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(16 * 1024 * 1024)?;

        let policy = BlockEncodingPolicy {
            profile: BlockEncodingProfile::Balanced,
            parameters: BlockEncodingParameters {
                presence: PresenceEncoding::Enabled,
                checksum: BlockChecksum::Enabled,
            },
            size_constraints: None,
        };

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: std::mem::size_of::<i32>() as u32,
            extended_type: Default::default(),
        };

        let mut encoder = PrimitiveBufferEncoder::new(policy, basic_type, temp_store)?;

        let mut v = 0i32;
        for _ in 0..num_blocks {
            let value_count = fastrand::usize(block_value_count.clone());
            let values: Vec<i32> = (v..v + value_count as i32).collect();
            v += value_count as i32;
            let array = Int32Array::from(values);
            encoder.encode_block(&array)?;
        }
        Ok(encoder)
    }

    fn create_test_buffer(
        num_blocks: usize,
        block_value_count: Range<usize>,
    ) -> Result<PreparedEncodedBuffer> {
        let encoder = encode_test_buffer(num_blocks, block_value_count)?;
        encoder.finish()
    }

    #[test]
    fn test_reader_exact_block_boundaries() {
        // Create a buffer with exactly 100 values per block
        let buffer = create_test_buffer(10, 100..101).unwrap();
        let decoder = PrimitiveBufferDecoder::from_encoded_buffer(
            buffer.data,
            &buffer.descriptor,
            BasicTypeDescriptor {
                basic_type: BasicType::Int32,
                fixed_size: 0,
                signed: true,
                extended_type: Default::default(),
            },
        )
        .unwrap();

        let mut reader = decoder
            .create_reader(vec![0..1000].into_iter(), BlockReaderPrefetch::Enabled)
            .unwrap();

        // Read exactly one block
        let seq = reader.read(0..100).unwrap();
        assert_eq!(seq.len(), 100);
        assert_eq!(seq.values.as_slice::<i32>()[0], 0);
        assert_eq!(seq.values.as_slice::<i32>()[99], 99);

        // Read exactly two blocks
        let seq = reader.read(100..300).unwrap();
        assert_eq!(seq.len(), 200);
        assert_eq!(seq.values.as_slice::<i32>()[0], 100);
        assert_eq!(seq.values.as_slice::<i32>()[199], 299);
    }

    #[test]
    fn test_reader_cross_block_boundaries() {
        // Create a buffer with exactly 100 values per block
        let buffer = create_test_buffer(10, 100..101).unwrap();
        let decoder = PrimitiveBufferDecoder::from_encoded_buffer(
            buffer.data,
            &buffer.descriptor,
            BasicTypeDescriptor {
                basic_type: BasicType::Int32,
                fixed_size: 0,
                signed: true,
                extended_type: Default::default(),
            },
        )
        .unwrap();

        let mut reader = decoder
            .create_reader(vec![0..1000].into_iter(), BlockReaderPrefetch::Enabled)
            .unwrap();

        // Read across block boundary
        let seq = reader.read(50..150).unwrap();
        assert_eq!(seq.len(), 100);
        assert_eq!(seq.values.as_slice::<i32>()[0], 50);
        assert_eq!(seq.values.as_slice::<i32>()[99], 149);

        // Read across multiple block boundaries
        let seq = reader.read(250..550).unwrap();
        assert_eq!(seq.len(), 300);
        assert_eq!(seq.values.as_slice::<i32>()[0], 250);
        assert_eq!(seq.values.as_slice::<i32>()[299], 549);
    }

    #[test]
    fn test_reader_partial_blocks() {
        // Create a buffer with variable-sized blocks
        let buffer = create_test_buffer(20, 50..150).unwrap();
        let decoder = PrimitiveBufferDecoder::from_encoded_buffer(
            buffer.data,
            &buffer.descriptor,
            BasicTypeDescriptor {
                basic_type: BasicType::Int32,
                fixed_size: 0,
                signed: true,
                extended_type: Default::default(),
            },
        )
        .unwrap();

        let mut reader = decoder
            .create_reader(vec![0..1000].into_iter(), BlockReaderPrefetch::Enabled)
            .unwrap();

        // Get total value count to determine actual buffer size
        let block_map = decoder.block_stream.block_map();
        let total_values = block_map.value_count().unwrap();

        // Read a small section from the middle
        let mid_point = total_values / 2;
        let seq = reader.read(mid_point - 25..mid_point + 25).unwrap();
        assert_eq!(seq.len(), 50);

        // Values should be consecutive integers
        let values = seq.values.as_slice::<i32>();
        for i in 0..50 {
            assert_eq!(values[i], (mid_point as i32 - 25) + i as i32);
        }
    }

    #[test]
    fn test_reader_empty_range() {
        let buffer = create_test_buffer(10, 100..101).unwrap();
        let decoder = PrimitiveBufferDecoder::from_encoded_buffer(
            buffer.data,
            &buffer.descriptor,
            BasicTypeDescriptor {
                basic_type: BasicType::Int32,
                fixed_size: 0,
                signed: true,
                extended_type: Default::default(),
            },
        )
        .unwrap();

        let mut reader = decoder
            .create_reader(vec![0..1000].into_iter(), BlockReaderPrefetch::Enabled)
            .unwrap();

        // Empty range at the start
        let seq = reader.read(0..0).unwrap();
        assert_eq!(seq.len(), 0);

        // Empty range in the middle
        let seq = reader.read(500..500).unwrap();
        assert_eq!(seq.len(), 0);

        // Empty range at the end
        let seq = reader.read(1000..1000).unwrap();
        assert_eq!(seq.len(), 0);
    }

    #[test]
    fn test_reader_entire_buffer() {
        // Create a buffer with exactly 100 values per block, 10 blocks total
        let buffer = create_test_buffer(10, 100..101).unwrap();
        let decoder = PrimitiveBufferDecoder::from_encoded_buffer(
            buffer.data,
            &buffer.descriptor,
            BasicTypeDescriptor {
                basic_type: BasicType::Int32,
                fixed_size: 0,
                signed: true,
                extended_type: Default::default(),
            },
        )
        .unwrap();

        let mut reader = decoder
            .create_reader(vec![0..1000].into_iter(), BlockReaderPrefetch::Enabled)
            .unwrap();

        // Read the entire buffer
        let seq = reader.read(0..1000).unwrap();
        assert_eq!(seq.len(), 1000);

        // Verify all values are present and correct
        let values = seq.values.as_slice::<i32>();
        for i in 0..1000 {
            assert_eq!(values[i], i as i32);
        }
    }

    #[test]
    fn test_reader_out_of_bounds() {
        let buffer = create_test_buffer(10, 100..101).unwrap();
        let decoder = PrimitiveBufferDecoder::from_encoded_buffer(
            buffer.data,
            &buffer.descriptor,
            BasicTypeDescriptor {
                basic_type: BasicType::Int32,
                fixed_size: 0,
                signed: true,
                extended_type: Default::default(),
            },
        )
        .unwrap();

        let mut reader = decoder
            .create_reader(vec![0..1000].into_iter(), BlockReaderPrefetch::Enabled)
            .unwrap();

        // Read range that extends beyond the end
        let result = reader.read(990..1010);
        assert!(result.is_err());
    }

    #[test]
    fn test_reader_disjoint_ranges() {
        let buffer = create_test_buffer(20, 100..101).unwrap();
        let decoder = PrimitiveBufferDecoder::from_encoded_buffer(
            buffer.data,
            &buffer.descriptor,
            BasicTypeDescriptor {
                basic_type: BasicType::Int32,
                fixed_size: 0,
                signed: true,
                extended_type: Default::default(),
            },
        )
        .unwrap();

        // Create reader with disjoint block ranges
        let mut reader = decoder
            .create_reader(
                vec![0..500, 1000..1500, 1800..2000].into_iter(),
                BlockReaderPrefetch::Enabled,
            )
            .unwrap();

        // Read from first range
        let seq = reader.read(100..200).unwrap();
        assert_eq!(seq.len(), 100);
        assert_eq!(seq.values.as_slice::<i32>()[0], 100);

        // Read from second range
        let seq = reader.read(1100..1200).unwrap();
        assert_eq!(seq.len(), 100);
        assert_eq!(seq.values.as_slice::<i32>()[0], 1100);

        // Read from third range
        let seq = reader.read(1900..1950).unwrap();
        assert_eq!(seq.len(), 50);
        assert_eq!(seq.values.as_slice::<i32>()[0], 1900);

        // Read across ranges (should work even with gaps)
        let seq = reader.read(450..1050).unwrap();
        assert_eq!(seq.len(), 600);
        assert_eq!(seq.values.as_slice::<i32>()[0], 450);
        assert_eq!(seq.values.as_slice::<i32>()[599], 1049);
    }

    #[test]
    fn test_reader_multiple_reads() {
        let buffer = create_test_buffer(20, 100..101).unwrap();
        let decoder = PrimitiveBufferDecoder::from_encoded_buffer(
            buffer.data,
            &buffer.descriptor,
            BasicTypeDescriptor {
                basic_type: BasicType::Int32,
                fixed_size: 0,
                signed: true,
                extended_type: Default::default(),
            },
        )
        .unwrap();

        let mut reader = decoder
            .create_reader(vec![0..2000].into_iter(), BlockReaderPrefetch::Enabled)
            .unwrap();

        // Perform sequential reads
        for start in (0i32..2000).step_by(100) {
            let end = start + 50;
            let seq = reader.read(start as u64..end as u64).unwrap();
            assert_eq!(seq.len(), 50);

            let values = seq.values.as_slice::<i32>();
            for i in 0i32..50 {
                assert_eq!(values[i as usize], { start + i });
            }
        }

        // Perform random access reads
        let positions = vec![150, 750, 1200, 1900];
        for &pos in &positions {
            let seq = reader.read(pos..pos + 1).unwrap();
            assert_eq!(seq.len(), 1);
            assert_eq!(seq.values.as_slice::<i32>()[0], pos as i32);
        }
    }

    #[test]
    fn test_reader_overlapping_reads() {
        let buffer = create_test_buffer(10, 100..101).unwrap();
        let decoder = PrimitiveBufferDecoder::from_prepared_buffer(
            &buffer,
            BasicTypeDescriptor {
                basic_type: BasicType::Int32,
                fixed_size: 0,
                signed: true,
                extended_type: Default::default(),
            },
        )
        .unwrap();

        let mut reader = decoder
            .create_reader(vec![0..1000].into_iter(), BlockReaderPrefetch::Enabled)
            .unwrap();

        // Read overlapping ranges
        let seq1 = reader.read(100..300).unwrap();
        assert_eq!(seq1.len(), 200);

        let seq2 = reader.read(200..400).unwrap();
        assert_eq!(seq2.len(), 200);

        // Check that the overlapping section contains the same values in both results
        let values1 = seq1.values.as_slice::<i32>();
        let values2 = seq2.values.as_slice::<i32>();

        for i in 0..100 {
            assert_eq!(values1[i + 100], values2[i]);
            assert_eq!(values1[i + 100], (i + 200) as i32);
        }
    }

    #[test]
    fn test_consume() {
        let encoder = encode_test_buffer(3, 10..11).unwrap();
        let decoder = encoder.consume().unwrap();
        let pos_count = decoder.block_stream().block_map().value_count().unwrap();

        // Read back the data and verify
        let mut reader = decoder
            .create_reader(
                vec![0..pos_count].into_iter(),
                BlockReaderPrefetch::Disabled,
            )
            .unwrap();

        let seq = reader.read(0..pos_count).unwrap();
        assert_eq!(seq.len(), pos_count as usize);
        let values = seq.values.as_slice::<i32>();
        assert_eq!(values[0], 0);
        assert_eq!(values[values.len() - 1], values.len() as i32 - 1);
    }
}
