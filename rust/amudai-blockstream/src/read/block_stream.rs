//! BlockStream format decoder.

use std::{
    ops::Range,
    sync::{Arc, Weak},
};

use amudai_bytes::Bytes;
use amudai_common::{error::Error, verify_data, Result};
use amudai_encodings::block_encoder::BlockChecksum;
use amudai_format::defs::shard;
use amudai_io::{sliced_read::SlicedReadAt, ReadAt};
use amudai_io_impl::prefetch_read::PrefetchReadAt;

use super::{block_map::BlockMap, block_map_ops::BlockDescriptor};

/// A decoder for the BlockStream format which provides access to encoded data blocks.
///
/// The BlockStream format consists of two main components:
/// - A data section containing encoded data blocks
/// - A block map section containing metadata about block locations and sizes
///
/// This decoder provides methods to access individual blocks or ranges of blocks,
/// with optional prefetching for improved performance.
///
/// # Structure
///
/// The decoder uses weak self-references to support creating readers that hold
/// references back to the decoder:
///
/// ```text
/// BlockStreamDecoder ─┐
///         │           │
///         ▼           │
///    BlockReader ─────┘
/// ```
pub struct BlockStreamDecoder {
    /// Weak reference to self, allowing readers to reference the decoder
    this: Weak<BlockStreamDecoder>,
    /// Reader for the data section containing encoded blocks
    data_reader: SlicedReadAt<Arc<dyn ReadAt>>,
    /// Block map containing metadata about block locations
    block_map: Arc<BlockMap>,
    /// Configuration for block checksums. Whether the checksums are available
    /// is determined by the policy of the encoded buffer during its creation.
    checksum_config: BlockChecksum,
}

impl BlockStreamDecoder {
    /// Creates a new BlockStreamDecoder from separate data and block map readers.
    ///
    /// # Arguments
    ///
    /// * `data_reader` - Reader for the data section containing encoded blocks
    /// * `block_map_reader` - Reader for the block map section
    /// * `checksum_config` - Configuration specifying whether blocks have checksums
    ///
    /// # Returns
    ///
    /// Arc-wrapped BlockStreamDecoder instance
    pub fn new(
        data_reader: SlicedReadAt<Arc<dyn ReadAt>>,
        block_map_reader: SlicedReadAt<Arc<dyn ReadAt>>,
        checksum_config: BlockChecksum,
    ) -> Arc<BlockStreamDecoder> {
        let profile = data_reader.storage_profile();
        let block_map = Arc::new(BlockMap::new(block_map_reader, profile));
        Arc::new_cyclic(|this| BlockStreamDecoder {
            this: this.clone(),
            data_reader,
            block_map,
            checksum_config,
        })
    }

    /// Creates a BlockStreamDecoder from an encoded buffer descriptor.
    ///
    /// This method parses an `EncodedBuffer` descriptor to extract the ranges of the
    /// data and block map sections, then creates a decoder using those ranges.
    ///
    /// # Arguments
    ///
    /// * `reader` - Reader containing the entire encoded data
    /// * `encoded_buffer` - Descriptor containing references to buffer sections
    ///
    /// # Returns
    ///
    /// Result containing an Arc-wrapped BlockStreamDecoder or an error
    ///
    /// # Errors
    ///
    /// Returns errors if:
    /// - Buffer references are missing from the descriptor
    /// - Block map range is invalid
    /// - Block map size is invalid
    pub fn from_encoded_buffer(
        reader: Arc<dyn ReadAt>,
        encoded_buffer: &shard::EncodedBuffer,
    ) -> Result<Arc<BlockStreamDecoder>> {
        let buffer_range: Range<u64> = encoded_buffer
            .buffer
            .as_ref()
            .ok_or_else(|| Error::invalid_format("missing buffer ref in EncodedBuffer"))?
            .into();
        let block_map_range: Range<u64> = encoded_buffer
            .block_map
            .as_ref()
            .ok_or_else(|| Error::invalid_format("missing block_map ref in EncodedBuffer"))?
            .into();
        verify_data!(block_map_range, block_map_range.start >= buffer_range.start);
        verify_data!(block_map_range, block_map_range.end <= buffer_range.end);
        let block_map_size = block_map_range.end.saturating_sub(block_map_range.start);
        verify_data!(block_map_size, block_map_size != 0);
        verify_data!(block_map_size, block_map_size % 8 == 0);

        let data_reader = SlicedReadAt::new(reader.clone(), buffer_range);
        let block_map_reader = SlicedReadAt::new(reader, block_map_range);
        let checksum_config = if encoded_buffer.block_checksums {
            BlockChecksum::Enabled
        } else {
            BlockChecksum::Disabled
        };

        Ok(BlockStreamDecoder::new(
            data_reader,
            block_map_reader,
            checksum_config,
        ))
    }

    /// Returns an Arc to this decoder.
    ///
    /// This method upgrades the weak self-reference to a strong reference.
    ///
    /// # Returns
    ///
    /// An Arc-wrapped reference to this decoder
    pub fn shared_from_self(&self) -> Arc<BlockStreamDecoder> {
        self.this.upgrade().expect("upgrade")
    }

    pub fn checksum_config(&self) -> BlockChecksum {
        self.checksum_config
    }

    /// Returns a reference to the block map.
    ///
    /// The block map provides information about block locations and sizes.
    ///
    /// # Returns
    ///
    /// Reference to the block map
    pub fn block_map(&self) -> &Arc<BlockMap> {
        &self.block_map
    }

    /// Creates a reader for accessing blocks within specified ranges.
    ///
    /// # Arguments
    ///
    /// * `block_ranges` - Ranges of block ordinals expected to be read. Can be empty,
    ///   in which case the reader will use a more conservative but potentially less
    ///   efficient storage access pattern.
    /// * `enable_prefetch` - Whether to enable background prefetching (read-ahead)
    ///   for blocks. Has no effect if `block_ranges` is empty.
    ///
    /// # Returns
    ///
    /// Result containing a `BlockReader` or an error.
    pub fn create_reader(
        &self,
        block_ranges: Vec<Range<u32>>,
        prefetch: BlockReaderPrefetch,
    ) -> Result<BlockReader> {
        Ok(BlockReader::new(
            self.shared_from_self(),
            Arc::new(block_ranges),
            prefetch,
        ))
    }
}

impl BlockStreamDecoder {
    /// Reads raw block data from the data section.
    ///
    /// # Arguments
    ///
    /// * `data_range` - Storage range to read
    ///
    /// # Returns
    ///
    /// Result containing the read data or an error
    fn read_block_data(&self, data_range: Range<u64>) -> Result<Bytes> {
        Ok(self.data_reader.read_at(data_range)?)
    }
}

/// Controls whether a `BlockReader` should prefetch blocks in the background to optimize read performance.
///
/// Prefetching can significantly improve read performance when blocks are accessed in a predictable pattern,
/// particularly for sequential reads, by reading ahead and caching data before it's explicitly requested.
/// This reduces latency for subsequent block reads and can better utilize I/O bandwidth.
///
/// When prefetching is enabled, the reader creates a background prefetcher that reads ahead based on the
/// provided block ranges. This works best when the actual access pattern closely matches the predicted
/// ranges.
///
/// When prefetching is disabled or when no block ranges are provided, the reader falls back to reading
/// blocks on-demand, which may be more appropriate for random access patterns or when memory usage
/// needs to be minimized.
///
/// # Performance considerations
///
/// - **I/O efficiency**: Prefetching can reduce the number of separate I/O operations by batching reads,
///   which is particularly beneficial for high-latency storage systems.
///
/// - **Memory usage**: Prefetching consumes additional memory to cache data ahead of actual requests.
///
/// - **Access patterns**: The effectiveness of prefetching depends greatly on how well the actual
///   access pattern matches the predicted pattern provided in the block ranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockReaderPrefetch {
    /// Disables background prefetching. Each block will be read on-demand when requested.
    ///
    /// This mode is appropriate when:
    /// - Blocks are accessed in an unpredictable or random order
    /// - Memory usage needs to be minimized
    /// - The access pattern doesn't match the provided block ranges
    /// - The underlying storage has very low latency (e.g., memory-mapped files)
    /// - Debugging or analyzing precise I/O patterns
    ///
    /// Using this mode with random access patterns may result in better memory efficiency
    /// but potentially worse I/O performance due to more frequent, smaller reads.
    Disabled,

    /// Enables background prefetching of blocks based on the provided block ranges.
    ///
    /// When this mode is selected and non-empty block ranges are provided to
    /// `BlockStreamDecoder::create_reader`, the reader will create a background prefetcher
    /// that reads ahead along the predicted access path. This can significantly reduce latency
    /// for sequential or predictable access patterns.
    ///
    /// This mode is appropriate when:
    /// - Blocks are accessed in a sequential or predictable order
    /// - The access pattern closely matches the provided block ranges
    /// - The underlying storage has higher latency (e.g., remote files, HDD)
    /// - Sufficient memory is available to cache prefetched blocks
    ///
    /// Note: Enabling prefetching with empty block ranges will have no effect, as there
    /// is no predictable access pattern to follow. In this case, the reader will behave
    /// as if prefetching was disabled.
    ///
    /// The prefetcher implementation automatically manages a queue of prefetch operations
    /// and tracks hit/miss statistics to evaluate prefetch effectiveness.
    Enabled,
}

/// A reader for accessing blocks from a BlockStream.
///
/// BlockReader provides efficient sequential or random access to blocks within
/// a BlockStream. It supports prefetching for improved performance when reading
/// blocks in a predictable pattern.
///
/// The reader maintains a cache of currently loaded block data to avoid repeated
/// reads when accessing adjacent blocks.
pub struct BlockReader {
    /// Reference to the parent decoder
    block_stream: Arc<BlockStreamDecoder>,
    /// Ranges of blocks that are expected to be read
    block_ranges: Arc<Vec<Range<u32>>>,
    /// Optional prefetcher for background loading of data
    prefetcher: Option<PrefetchReadAt>,
    /// Current range index within block_ranges
    range_idx: usize,
    /// Currently loaded block range
    block_range: Range<u32>,
    /// Buffer containing raw data for the current block range
    data: Bytes,
    /// Storage range covered by the current data buffer
    data_range: Range<u64>,
}

impl BlockReader {
    /// Creates a new BlockReader.
    ///
    /// # Arguments
    ///
    /// * `block_stream` - Reference to the parent decoder
    /// * `block_ranges` - Ranges of blocks that are expected to be read
    /// * `enable_prefetch` - Whether to enable background prefetching
    ///
    /// # Returns
    ///
    /// A new BlockReader instance
    pub fn new(
        block_stream: Arc<BlockStreamDecoder>,
        block_ranges: Arc<Vec<Range<u32>>>,
        prefetch: BlockReaderPrefetch,
    ) -> BlockReader {
        let prefetcher = if !block_ranges.is_empty() && prefetch == BlockReaderPrefetch::Enabled {
            let ranges_iter = StorageRangesFromBlockRangesIter::new(
                block_ranges.clone(),
                block_stream.block_map().clone(),
            );
            let prefetcher = PrefetchReadAt::new(
                Arc::new(block_stream.data_reader.clone()),
                Box::new(ranges_iter),
            );
            Some(prefetcher)
        } else {
            None
        };

        BlockReader {
            block_stream,
            block_ranges,
            prefetcher,
            range_idx: 0,
            block_range: 0..0,
            data: Bytes::new(),
            data_range: 0..0,
        }
    }

    /// Returns a reference to the block map.
    ///
    /// # Returns
    ///
    /// Reference to the block map from the parent decoder
    pub fn block_map(&self) -> &Arc<BlockMap> {
        self.block_stream.block_map()
    }

    /// Reads a block with the specified ordinal.
    ///
    /// If the block is not already in the current data buffer, this method
    /// will load it first.
    ///
    /// # Arguments
    ///
    /// * `ordinal` - Ordinal (index) of the block to read
    ///
    /// # Returns
    ///
    /// Result containing the block data or an error
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The block ordinal is out of range
    /// - There's an I/O error when reading the block
    /// - The block data is corrupted
    pub fn read_block(&mut self, ordinal: u32) -> Result<OpaqueBlock> {
        if !self.has_block(ordinal) {
            self.load_block(ordinal)?;
        }
        self.slice_block(ordinal)
    }
}

impl BlockReader {
    /// Checks if the specified block is in the current data buffer.
    ///
    /// # Arguments
    ///
    /// * `ordinal` - Ordinal of the block to check
    ///
    /// # Returns
    ///
    /// True if the block is in the current data buffer, false otherwise
    fn has_block(&self, ordinal: u32) -> bool {
        ordinal >= self.block_range.start && ordinal < self.block_range.end
    }

    /// Extracts a block from the current data buffer.
    ///
    /// # Arguments
    ///
    /// * `ordinal` - Ordinal of the block to slice
    ///
    /// # Returns
    ///
    /// Result containing the sliced block or an error
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The block ordinal is out of range
    /// - The block's storage range extends beyond the current data buffer
    fn slice_block(&self, ordinal: u32) -> Result<OpaqueBlock> {
        let block = self.block_stream.block_map().get_block(ordinal as usize)?;
        verify_data!(block, block.storage_range.start >= self.data_range.start);
        let offset = (block.storage_range.start - self.data_range.start) as usize;
        let len = block.storage_size();
        verify_data!(block, offset + len <= self.data.len());
        let data = self.data.slice(offset..offset + len);
        self.construct_block(block, data)
    }

    /// Loads the block with the specified ordinal into the data buffer.
    ///
    /// If the ordinal is within one of the specified block ranges, loads that entire range.
    /// Otherwise, extends the ordinal to a range that satisfies minimum I/O size.
    ///
    /// # Arguments
    ///
    /// * `ordinal` - Ordinal of the block to load
    ///
    /// # Returns
    ///
    /// Result indicating success or an error
    ///
    /// # Errors
    ///
    /// Returns an error if there's an I/O error loading the block data
    fn load_block(&mut self, ordinal: u32) -> Result<()> {
        while self.range_idx < self.block_ranges.len()
            && self.block_ranges[self.range_idx].end <= ordinal
        {
            self.range_idx += 1;
        }
        let block_range = if self.range_idx < self.block_ranges.len()
            && self.block_ranges[self.range_idx].contains(&ordinal)
        {
            self.block_ranges[self.range_idx].clone()
        } else {
            // Extend to a block range covering min_io_size
            self.block_stream
                .block_map()
                .extend_block_range(ordinal..ordinal + 1)?
        };
        self.load_block_range(block_range)
    }

    /// Loads a range of blocks into the data buffer.
    ///
    /// # Arguments
    ///
    /// * `block_range` - Range of block ordinals to load
    ///
    /// # Returns
    ///
    /// Result indicating success or an error
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The block range is invalid
    /// - There's an I/O error loading the block data
    fn load_block_range(&mut self, block_range: Range<u32>) -> Result<()> {
        let map = self.block_stream.block_map();
        let first = map.get_block(block_range.start as usize)?;
        let last = map.get_block(block_range.end as usize - 1)?;
        verify_data!(first, first.ordinal == block_range.start as u64);
        verify_data!(last, last.ordinal == block_range.end as u64 - 1);
        let data_range = first.storage_range.start..last.storage_range.end;
        self.data = self.read_block_data(data_range.clone())?;
        self.block_range = block_range;
        self.data_range = data_range;
        Ok(())
    }

    /// Reads block data from either the prefetcher or the underlying storage.
    ///
    /// # Arguments
    ///
    /// * `stg_range` - Storage range to read
    ///
    /// # Returns
    ///
    /// Result containing the read data or an error
    fn read_block_data(&mut self, stg_range: Range<u64>) -> Result<Bytes> {
        if let Some(prefetcher) = self.prefetcher.as_mut() {
            prefetcher.read_at_mut(stg_range).map_err(|e| e.into())
        } else {
            self.block_stream.read_block_data(stg_range)
        }
    }

    /// Constructs an OpaqueBlock from raw block data.
    ///
    /// Handles checksums according to the decoder's checksum configuration.
    ///
    /// # Arguments
    ///
    /// * `descriptor` - Metadata for the block
    /// * `raw_data` - Raw block data, potentially including checksum
    ///
    /// # Returns
    ///
    /// Result containing the constructed OpaqueBlock or an error
    ///
    /// # Errors
    ///
    /// Returns an error if the block data is too small to contain a checksum
    fn construct_block(&self, descriptor: BlockDescriptor, raw_data: Bytes) -> Result<OpaqueBlock> {
        if self.block_stream.checksum_config == BlockChecksum::Enabled {
            let len = raw_data.len();
            verify_data!(len, len >= 4);
            let data_len = len - 4;
            let checksum = u32::from_le_bytes(raw_data[data_len..].try_into().unwrap());
            let data = raw_data.slice(0..data_len);
            Ok(OpaqueBlock {
                descriptor,
                data,
                checksum: Some(checksum),
            })
        } else {
            Ok(OpaqueBlock {
                descriptor,
                data: raw_data,
                checksum: None,
            })
        }
    }
}

struct StorageRangesFromBlockRangesIter {
    block_ranges: Arc<Vec<Range<u32>>>,
    block_map: Arc<BlockMap>,
    index: usize,
}

impl StorageRangesFromBlockRangesIter {
    fn new(
        block_ranges: Arc<Vec<Range<u32>>>,
        block_map: Arc<BlockMap>,
    ) -> StorageRangesFromBlockRangesIter {
        StorageRangesFromBlockRangesIter {
            block_ranges,
            block_map,
            index: 0,
        }
    }
}

impl Iterator for StorageRangesFromBlockRangesIter {
    type Item = Range<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.block_ranges.len() {
            return None;
        }
        let block_range = self.block_ranges[self.index].clone();
        self.index += 1;
        self.block_map
            .block_range_to_storage_range(block_range)
            .ok()
    }
}

#[derive(Debug, Clone)]
pub struct OpaqueBlock {
    pub descriptor: BlockDescriptor,
    pub data: Bytes,
    pub checksum: Option<u32>,
}

impl OpaqueBlock {
    pub fn verify_checksum(&self) -> Result<()> {
        if let Some(checksum) = self.checksum {
            amudai_format::checksum::validate_buffer(&self.data, checksum, Some("value block"))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use amudai_common::Result;
    use amudai_encodings::block_encoder::{
        BlockChecksum, BlockEncodingParameters, BlockEncodingPolicy, BlockEncodingProfile,
        PresenceEncoding,
    };
    use amudai_format::schema::{BasicType, BasicTypeDescriptor};
    use amudai_io::StorageProfile;
    use arrow_array::Int32Array;

    use crate::{
        read::{
            block_map_ops::BlockMapOps,
            block_stream::{BlockReaderPrefetch, BlockStreamDecoder},
        },
        write::{primitive_buffer::PrimitiveBufferEncoder, PreparedEncodedBuffer},
    };

    #[test]
    fn test_create_test_block_stream() {
        let buffer = create_test_block_stream(3000, 256..257).unwrap();

        assert!(buffer.data_size > 0);
        assert!(buffer.block_map_size > 0);
        assert_eq!(buffer.descriptor.block_count, Some(3000));
        assert!(buffer.descriptor.block_checksums);

        let decoder =
            BlockStreamDecoder::from_encoded_buffer(buffer.data, &buffer.descriptor).unwrap();

        let block_map = decoder.block_map();
        assert_eq!(block_map.block_count().unwrap(), 3000);
        let block_list = decoder.block_map().establish_block_list().unwrap();
        assert_eq!(block_list.find_block_ordinal(1200).unwrap(), 4);
    }

    #[test]
    fn test_block_reader_single_block() {
        let buffer = create_test_block_stream(100, 256..257).unwrap();
        let decoder =
            BlockStreamDecoder::from_encoded_buffer(buffer.data, &buffer.descriptor).unwrap();

        let mut reader = decoder
            .create_reader(vec![], BlockReaderPrefetch::Enabled)
            .unwrap();
        let block = reader.read_block(50).unwrap();

        assert_eq!(block.descriptor.ordinal, 50);
        assert!(block.data.len() > 0);
        assert!(block.checksum.is_some());
        block.verify_checksum().unwrap();
    }

    #[test]
    fn test_block_reader_sequential_blocks() {
        let buffer = create_test_block_stream(100, 100..500).unwrap();
        let decoder =
            BlockStreamDecoder::from_encoded_buffer(buffer.data, &buffer.descriptor).unwrap();

        let mut reader = decoder
            .create_reader(vec![], BlockReaderPrefetch::Enabled)
            .unwrap();

        for i in 0..10 {
            let block = reader.read_block(i).unwrap();
            assert_eq!(block.descriptor.ordinal, i as u64);
            block.verify_checksum().unwrap();
        }
    }

    #[test]
    fn test_block_reader_with_ranges() {
        let buffer = create_test_block_stream(100, 50..500).unwrap();
        let decoder =
            BlockStreamDecoder::from_encoded_buffer(buffer.data, &buffer.descriptor).unwrap();

        let ranges = vec![10..20, 50..60];
        let mut reader = decoder
            .create_reader(ranges, BlockReaderPrefetch::Enabled)
            .unwrap();

        // Block in first range
        let block15 = reader.read_block(15).unwrap();
        assert_eq!(block15.descriptor.ordinal, 15);

        // Block in second range
        let block55 = reader.read_block(55).unwrap();
        assert_eq!(block55.descriptor.ordinal, 55);

        // Block outside both ranges
        let block30 = reader.read_block(30).unwrap();
        assert_eq!(block30.descriptor.ordinal, 30);
    }

    #[test]
    fn test_block_reader_random_access() {
        let buffer = create_test_block_stream(100, 50..500).unwrap();
        let decoder =
            BlockStreamDecoder::from_encoded_buffer(buffer.data, &buffer.descriptor).unwrap();

        let mut reader = decoder
            .create_reader(vec![], BlockReaderPrefetch::Enabled)
            .unwrap();

        // Access blocks in non-sequential order
        let indices = [42, 17, 99, 0, 63];
        for &idx in &indices {
            let block = reader.read_block(idx).unwrap();
            assert_eq!(block.descriptor.ordinal, idx as u64);
            block.verify_checksum().unwrap();
        }
    }

    #[test]
    fn test_opaque_block_checksums() {
        let buffer = create_test_block_stream(10, 50..500).unwrap();
        let decoder =
            BlockStreamDecoder::from_encoded_buffer(buffer.data, &buffer.descriptor).unwrap();

        let mut reader = decoder
            .create_reader(vec![], BlockReaderPrefetch::Enabled)
            .unwrap();

        for i in 0..10 {
            let block = reader.read_block(i).unwrap();
            assert!(block.checksum.is_some());
            // Should not fail if checksum is correct
            block.verify_checksum().unwrap();
        }
    }

    #[test]
    fn test_block_reader_boundary_conditions() {
        let buffer = create_test_block_stream(5, 100..400).unwrap();
        let decoder =
            BlockStreamDecoder::from_encoded_buffer(buffer.data, &buffer.descriptor).unwrap();

        let mut reader = decoder
            .create_reader(vec![], BlockReaderPrefetch::Enabled)
            .unwrap();

        let first = reader.read_block(0).unwrap();
        assert_eq!(first.descriptor.ordinal, 0);

        let last = reader.read_block(4).unwrap();
        assert_eq!(last.descriptor.ordinal, 4);

        let result = reader.read_block(5);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_all_blocks() {
        let buffer = create_test_block_stream(3000, 100..300).unwrap();

        let decoder =
            BlockStreamDecoder::from_encoded_buffer(buffer.data, &buffer.descriptor).unwrap();
        let value_count = decoder.block_map().value_count().unwrap();
        assert!(value_count > 300000);

        let block_list = decoder.block_map().establish_block_list().unwrap();

        let profile = StorageProfile {
            min_io_size: 4 * 1024,
            max_io_size: 64 * 1024,
        };
        let block_ranges = block_list
            .pos_ranges_to_block_ranges(vec![0..value_count].into_iter(), &profile)
            .unwrap();
        let block_ranges = block_list
            .compute_read_optimized_block_ranges(block_ranges.into_iter(), &profile)
            .unwrap();
        assert!(block_ranges.len() > 3);

        let mut reader = decoder
            .create_reader(block_ranges, BlockReaderPrefetch::Enabled)
            .unwrap();
        let block_count = buffer.descriptor.block_count.unwrap() as u32;

        for i in 0..block_count {
            let block = reader.read_block(i).unwrap();

            assert_eq!(block.descriptor.ordinal, i as u64);
            assert!(!block.data.is_empty());
            assert!(block.checksum.is_some());

            block.verify_checksum().unwrap();

            let logical_size = block.descriptor.logical_size();
            assert!(
                logical_size >= 100 && logical_size < 300,
                "Block {} has unexpected logical size: {}",
                i,
                logical_size
            );
        }

        let result = reader.read_block(block_count);
        assert!(result.is_err());
    }

    fn create_test_block_stream(
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
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: std::mem::size_of::<i32>(),
        };

        let mut encoder = PrimitiveBufferEncoder::new(policy, basic_type, temp_store)?;

        for block_idx in 0..num_blocks {
            let value_count = fastrand::usize(block_value_count.clone());
            let values: Vec<i32> = (0..value_count)
                .map(|i| (block_idx * 10000 + i) as i32)
                .collect();
            let array = Int32Array::from(values);
            encoder.encode_block(&array)?;
        }
        encoder.finish()
    }
}
