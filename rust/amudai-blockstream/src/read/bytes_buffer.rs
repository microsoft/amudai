use std::sync::Arc;

use amudai_common::Result;
use amudai_encodings::{
    binary_block_decoder::BinaryBlockDecoder,
    block_encoder::{BlockEncodingParameters, PresenceEncoding},
};
use amudai_format::{defs::shard, schema::BasicTypeDescriptor};
use amudai_io::ReadAt;
use amudai_ranges::PositionSeries;

use crate::{read::block_stream::BlockStreamDecoder, write::PreparedEncodedBuffer};

use super::{block_stream::BlockReaderPrefetch, generic_buffer::GenericBufferReader};

/// A decoder for primitive-typed buffers that provides access to encoded primitive
/// values.
///
/// This struct serves as the entry point for working with encoded primitive buffers
/// and provides methods to create readers that can efficiently access and decode
/// specific ranges of values.
#[derive(Clone)]
pub struct BytesBufferDecoder {
    /// The underlying block stream containing the encoded blocks
    block_stream: Arc<BlockStreamDecoder>,

    /// Indicates whether presence information is embedded in the block stream
    /// When true, blocks contain presence bits indicating which values are null
    embedded_presence: bool,

    /// The specific primitive type that describes values in this buffer
    basic_type: BasicTypeDescriptor,
}

impl BytesBufferDecoder {
    /// Creates a new `BytesBufferDecoder` from an encoded buffer descriptor.
    ///
    /// # Arguments
    ///
    /// * `reader` - A reference to the storage artifact containing encoded data
    /// * `encoded_buffer` - The buffer descriptor containing metadata
    ///   about the encoded buffer
    /// * `basic_type` - The primitive type descriptor for values in this buffer.
    ///   The types supported by this decoder are `String`, `Binary` or `FixedSizeBinary(n)`.
    ///
    /// # Returns
    ///
    /// A result containing the initialized decoder if successful
    pub fn from_encoded_buffer(
        reader: Arc<dyn ReadAt>,
        encoded_buffer: &shard::EncodedBuffer,
        basic_type: BasicTypeDescriptor,
    ) -> Result<BytesBufferDecoder> {
        let block_stream = BlockStreamDecoder::from_encoded_buffer(reader, encoded_buffer)?;
        Ok(BytesBufferDecoder {
            block_stream,
            embedded_presence: encoded_buffer.embedded_presence,
            basic_type,
        })
    }

    /// Creates a new `BytesBufferDecoder` from a [`PreparedEncodedBuffer`].
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
    /// Returns a [`Result`] containing the initialized [`BytesBufferDecoder`] if successful,
    /// or an error if the buffer could not be decoded.
    pub fn from_prepared_buffer(
        prepared_buffer: &PreparedEncodedBuffer,
        basic_type: BasicTypeDescriptor,
    ) -> Result<BytesBufferDecoder> {
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
    /// 1. Translates the requested logical positions to block ranges
    /// 2. Optimizes those ranges for efficient reading
    /// 3. Creates a reader instance configured to access those blocks
    ///
    /// The returned reader provides a high-level interface for efficiently reading and
    /// decoding values from the specified ranges.
    ///
    /// # Arguments
    ///
    /// * `positions` - Iterator of logical position ranges or positions to read
    /// * `prefetch` - Whether to enable prefetching of blocks for improved performance
    ///
    /// # Returns
    ///
    /// A result containing the initialized reader if successful
    pub fn create_reader(
        &self,
        positions: impl PositionSeries<u64>,
        prefetch: BlockReaderPrefetch,
    ) -> Result<BytesBufferReader> {
        let block_reader = self.block_stream.create_reader(positions, prefetch)?;
        Ok(BytesBufferReader::new(
            self.basic_type,
            block_reader,
            BinaryBlockDecoder::new(
                self.encoding_params(),
                self.basic_type,
                Arc::new(Default::default()),
            ),
        ))
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

/// A reader for string and binary buffers that efficiently accesses and decodes
/// blocks of data.
pub type BytesBufferReader = GenericBufferReader<BinaryBlockDecoder>;

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use amudai_common::Result;
    use amudai_encodings::block_encoder::{
        BlockChecksum, BlockEncodingParameters, BlockEncodingPolicy, BlockEncodingProfile,
        PresenceEncoding,
    };
    use amudai_format::schema::{BasicType, BasicTypeDescriptor};
    use arrow_array::{StringArray, builder::FixedSizeBinaryBuilder};

    use crate::{
        read::block_stream::BlockReaderPrefetch,
        write::{PreparedEncodedBuffer, bytes_buffer::BytesBufferEncoder},
    };

    use super::BytesBufferDecoder;

    #[test]
    fn test_decoder_basics() {
        let buffer = create_test_buffer(50, 10..20).unwrap();
        let decoder = BytesBufferDecoder::from_encoded_buffer(
            buffer.data,
            &buffer.descriptor,
            BasicTypeDescriptor {
                basic_type: BasicType::String,
                fixed_size: 0,
                signed: false,
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(decoder.basic_type().basic_type, BasicType::String);
        assert!(decoder.has_embedded_presence());

        let ranges = [10u64..100, 200..300];
        let mut reader = decoder
            .create_reader(ranges.iter().cloned(), BlockReaderPrefetch::Enabled)
            .unwrap();

        let seq = reader.read_range(25..35).unwrap();
        assert_eq!(seq.len(), 10);
        assert!(seq.binary_at(0).ends_with(b"_25"));
        assert!(seq.binary_at(9).ends_with(b"_34"));
    }

    #[test]
    fn test_reader_exact_block_boundaries() {
        // Create a buffer with exactly 100 values per block
        let buffer = create_test_buffer(10, 100..101).unwrap();
        let decoder = BytesBufferDecoder::from_encoded_buffer(
            buffer.data,
            &buffer.descriptor,
            BasicTypeDescriptor {
                basic_type: BasicType::String,
                fixed_size: 0,
                signed: false,
                extended_type: Default::default(),
            },
        )
        .unwrap();

        let mut reader = decoder
            .create_reader(vec![0..1000].into_iter(), BlockReaderPrefetch::Enabled)
            .unwrap();

        // Read exactly one block
        let seq = reader.read_range(0..100).unwrap();
        assert_eq!(seq.len(), 100);
        assert!(seq.binary_at(0).ends_with(b"_0"));
        assert!(seq.binary_at(99).ends_with(b"_99"));

        // Read exactly two blocks
        let seq = reader.read_range(100..300).unwrap();
        assert_eq!(seq.len(), 200);
        assert!(seq.binary_at(0).ends_with(b"_100"));
        assert!(seq.binary_at(199).ends_with(b"_299"));
    }

    #[test]
    fn test_reader_cross_block_boundaries() {
        // Create a buffer with exactly 100 values per block
        let buffer = create_test_buffer(10, 100..101).unwrap();
        let decoder = BytesBufferDecoder::from_encoded_buffer(
            buffer.data,
            &buffer.descriptor,
            BasicTypeDescriptor {
                basic_type: BasicType::String,
                fixed_size: 0,
                signed: false,
                extended_type: Default::default(),
            },
        )
        .unwrap();

        let mut reader = decoder
            .create_reader(vec![0..1000].into_iter(), BlockReaderPrefetch::Enabled)
            .unwrap();

        // Read across block boundary
        let seq = reader.read_range(50..150).unwrap();
        assert_eq!(seq.len(), 100);
        assert!(seq.binary_at(0).ends_with(b"_50"));
        assert!(seq.binary_at(99).ends_with(b"_149"));

        // Read across multiple block boundaries
        let seq = reader.read_range(250..550).unwrap();
        assert_eq!(seq.len(), 300);
        assert!(seq.binary_at(0).ends_with(b"_250"));
        assert!(seq.binary_at(299).ends_with(b"_549"));
    }

    #[test]
    fn test_reader_partial_blocks() {
        // Create a buffer with variable-sized blocks
        let buffer = create_test_buffer(20, 50..150).unwrap();
        let decoder = BytesBufferDecoder::from_encoded_buffer(
            buffer.data,
            &buffer.descriptor,
            BasicTypeDescriptor {
                basic_type: BasicType::String,
                fixed_size: 0,
                signed: false,
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
        let seq = reader.read_range(mid_point - 25..mid_point + 25).unwrap();
        assert_eq!(seq.len(), 50);

        // Values should have the expected contents
        for i in 0..50 {
            let expected_idx = (mid_point as i32 - 25) + i;
            assert!(
                seq.binary_at(i as usize)
                    .ends_with(format!("_{expected_idx}").as_bytes())
            );
        }
    }

    #[test]
    fn test_reader_empty_range() {
        let buffer = create_test_buffer(10, 100..101).unwrap();
        let decoder = BytesBufferDecoder::from_encoded_buffer(
            buffer.data,
            &buffer.descriptor,
            BasicTypeDescriptor {
                basic_type: BasicType::String,
                fixed_size: 0,
                signed: false,
                extended_type: Default::default(),
            },
        )
        .unwrap();

        let mut reader = decoder
            .create_reader(vec![0..1000].into_iter(), BlockReaderPrefetch::Enabled)
            .unwrap();

        // Empty range at the start
        let seq = reader.read_range(0..0).unwrap();
        assert_eq!(seq.len(), 0);

        // Empty range in the middle
        let seq = reader.read_range(500..500).unwrap();
        assert_eq!(seq.len(), 0);

        // Empty range at the end
        let seq = reader.read_range(1000..1000).unwrap();
        assert_eq!(seq.len(), 0);
    }

    #[test]
    fn test_reader_entire_buffer() {
        // Create a buffer with exactly 100 values per block, 10 blocks total
        let buffer = create_test_buffer(10, 100..101).unwrap();
        let decoder = BytesBufferDecoder::from_encoded_buffer(
            buffer.data,
            &buffer.descriptor,
            BasicTypeDescriptor {
                basic_type: BasicType::String,
                fixed_size: 0,
                signed: false,
                extended_type: Default::default(),
            },
        )
        .unwrap();

        let mut reader = decoder
            .create_reader(vec![0..1000].into_iter(), BlockReaderPrefetch::Enabled)
            .unwrap();

        // Read the entire buffer
        let seq = reader.read_range(0..1000).unwrap();
        assert_eq!(seq.len(), 1000);

        // Verify all values are present and correctly ordered
        for i in 0..1000 {
            assert!(seq.binary_at(i).ends_with(format!("_{i}").as_bytes()));
        }
    }

    #[test]
    fn test_reader_out_of_bounds() {
        let buffer = create_test_buffer(10, 100..101).unwrap();
        let decoder = BytesBufferDecoder::from_encoded_buffer(
            buffer.data,
            &buffer.descriptor,
            BasicTypeDescriptor {
                basic_type: BasicType::String,
                fixed_size: 0,
                signed: false,
                extended_type: Default::default(),
            },
        )
        .unwrap();

        let mut reader = decoder
            .create_reader(vec![0..1000].into_iter(), BlockReaderPrefetch::Enabled)
            .unwrap();

        // Read range that extends beyond the end
        let result = reader.read_range(990..1010);
        assert!(result.is_err());
    }

    #[test]
    fn test_reader_disjoint_ranges() {
        let buffer = create_test_buffer(20, 100..101).unwrap();
        let decoder = BytesBufferDecoder::from_encoded_buffer(
            buffer.data,
            &buffer.descriptor,
            BasicTypeDescriptor {
                basic_type: BasicType::String,
                fixed_size: 0,
                signed: false,
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
        let seq = reader.read_range(100..200).unwrap();
        assert_eq!(seq.len(), 100);
        assert!(seq.binary_at(0).ends_with(b"_100"));

        // Read from second range
        let seq = reader.read_range(1100..1200).unwrap();
        assert_eq!(seq.len(), 100);
        assert!(seq.binary_at(0).ends_with(b"_1100"));

        // Read from third range
        let seq = reader.read_range(1900..1950).unwrap();
        assert_eq!(seq.len(), 50);
        assert!(seq.binary_at(0).ends_with(b"_1900"));

        // Read across ranges (should work even with gaps)
        let seq = reader.read_range(450..1050).unwrap();
        assert_eq!(seq.len(), 600);
        assert!(seq.binary_at(0).ends_with(b"_450"));
        assert!(seq.binary_at(599).ends_with(b"_1049"));
    }

    #[test]
    fn test_reader_multiple_reads() {
        let buffer = create_test_buffer(20, 100..101).unwrap();
        let decoder = BytesBufferDecoder::from_encoded_buffer(
            buffer.data,
            &buffer.descriptor,
            BasicTypeDescriptor {
                basic_type: BasicType::String,
                fixed_size: 0,
                signed: false,
                extended_type: Default::default(),
            },
        )
        .unwrap();

        let mut reader = decoder
            .create_reader(vec![0..2000].into_iter(), BlockReaderPrefetch::Enabled)
            .unwrap();

        // Perform sequential reads
        for start in (0u64..2000).step_by(100) {
            let end = start + 50;
            let seq = reader.read_range(start..end).unwrap();
            assert_eq!(seq.len(), 50);

            // Check each value in the sequence
            for i in 0..50 {
                let expected_idx = start + i as u64;
                assert!(
                    seq.binary_at(i)
                        .ends_with(format!("_{expected_idx}").as_bytes())
                );
            }
        }

        // Perform random access reads
        let positions = vec![150u64, 750, 1200, 1900];
        for &pos in &positions {
            let seq = reader.read_range(pos..pos + 1).unwrap();
            assert_eq!(seq.len(), 1);
            assert!(seq.binary_at(0).ends_with(format!("_{pos}").as_bytes()));
        }
    }

    #[test]
    fn test_reader_overlapping_reads() {
        let buffer = create_test_buffer(10, 100..101).unwrap();
        let decoder = BytesBufferDecoder::from_encoded_buffer(
            buffer.data,
            &buffer.descriptor,
            BasicTypeDescriptor {
                basic_type: BasicType::String,
                fixed_size: 0,
                signed: false,
                extended_type: Default::default(),
            },
        )
        .unwrap();

        let mut reader = decoder
            .create_reader(vec![0..1000].into_iter(), BlockReaderPrefetch::Enabled)
            .unwrap();

        // Read overlapping ranges
        let seq1 = reader.read_range(100..300).unwrap();
        assert_eq!(seq1.len(), 200);

        let seq2 = reader.read_range(200..400).unwrap();
        assert_eq!(seq2.len(), 200);

        // Check that the overlapping section contains the same values in both results
        for i in 0..100 {
            let val1 = seq1.binary_at(i + 100);
            let val2 = seq2.binary_at(i);
            assert_eq!(val1, val2);
            assert!(val1.ends_with(format!("_{}", i + 200).as_bytes()));
        }
    }

    #[test]
    fn test_fixed_size_binary() {
        // Test with fixed-size binary type
        let buffer = create_test_fixed_size_buffer(5, 50..51).unwrap();
        let decoder = BytesBufferDecoder::from_prepared_buffer(
            &buffer,
            BasicTypeDescriptor {
                basic_type: BasicType::FixedSizeBinary,
                fixed_size: 8, // 8-byte fixed size binary
                signed: false,
                extended_type: Default::default(),
            },
        )
        .unwrap();

        let mut reader = decoder
            .create_reader(vec![0..250].into_iter(), BlockReaderPrefetch::Enabled)
            .unwrap();

        let seq = reader.read_range(50..100).unwrap();
        assert_eq!(seq.len(), 50);

        // All values should be exactly 8 bytes
        for i in 0..50 {
            let binary = seq.binary_at(i);
            assert_eq!(binary.len(), 8);
        }
    }

    fn create_test_buffer(
        num_blocks: usize,
        block_value_count: Range<usize>,
    ) -> Result<PreparedEncodedBuffer> {
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
            basic_type: BasicType::String,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };

        let mut encoder = BytesBufferEncoder::new(policy, basic_type, temp_store)?;

        let mut value_offset = 0;
        for block_idx in 0..num_blocks {
            let value_count = fastrand::usize(block_value_count.clone());
            let mut values = Vec::with_capacity(value_count);

            for i in 0..value_count {
                let val = format!("value_{}_{}", block_idx, value_offset + i);
                values.push(Some(val));
            }

            value_offset += value_count;
            let array = StringArray::from(values);
            let array = amudai_arrow_processing::cast::binary_like_to_large_binary(&array).unwrap();
            encoder.encode_block(&array)?;
        }

        encoder.finish()
    }

    fn create_test_fixed_size_buffer(
        num_blocks: usize,
        block_value_count: Range<usize>,
    ) -> Result<PreparedEncodedBuffer> {
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
            basic_type: BasicType::FixedSizeBinary,
            signed: false,
            fixed_size: 8,
            extended_type: Default::default(),
        };

        let mut encoder = BytesBufferEncoder::new(policy, basic_type, temp_store)?;

        let mut value_offset = 0;
        for _block_idx in 0..num_blocks {
            let value_count = fastrand::usize(block_value_count.clone());
            let mut builder = FixedSizeBinaryBuilder::with_capacity(value_count, 8);

            for i in 0..value_count {
                // Create 8-byte fixed size binary values
                let value_idx = value_offset + i;
                let mut padded = format!("f{value_idx:07}").into_bytes();
                padded.truncate(8);
                builder.append_value(&padded).unwrap();
            }

            value_offset += value_count;
            let array = builder.finish();
            encoder.encode_block(&array)?;
        }

        encoder.finish()
    }
}
