//! BlockStream format decoder.

use std::{
    ops::Range,
    sync::{Arc, Weak},
};

use amudai_bytes::Bytes;
use amudai_common::{Result, error::Error, verify_data};
use amudai_encodings::block_encoder::BlockChecksum;
use amudai_format::{
    defs::shard,
    schema::{BasicType, BasicTypeDescriptor},
};
use amudai_io::{ReadAt, SlicedFile};
use amudai_io_impl::prefetch_read::PrefetchReadAt;
use amudai_sequence::sequence::ValueSequence;

use crate::write::PreparedEncodedBuffer;

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
    data_reader: SlicedFile<Arc<dyn ReadAt>>,
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
        data_reader: SlicedFile<Arc<dyn ReadAt>>,
        block_map_reader: SlicedFile<Arc<dyn ReadAt>>,
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

        let data_reader = SlicedFile::new(reader.clone(), buffer_range);
        let block_map_reader = SlicedFile::new(reader, block_map_range);
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

    /// Creates a [`BlockStreamDecoder`] from a [`PreparedEncodedBuffer`].
    ///
    /// This method is a convenience wrapper that constructs a decoder using the
    /// data and descriptor from a newly encoded, in-memory buffer.
    ///
    /// # Arguments
    ///
    /// * `prepared_buffer` - The prepared encoded buffer containing both the encoded data
    ///   and its descriptor.
    ///
    /// # Returns
    ///
    /// Returns a [`Result`] containing an [`Arc<BlockStreamDecoder>`] if successful,
    /// or an error if the buffer could not be decoded.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The buffer or block map references are missing from the descriptor.
    /// - The block map range is invalid.
    /// - The block map size is invalid.
    pub fn from_prepared_buffer(
        prepared_buffer: &PreparedEncodedBuffer,
    ) -> Result<Arc<BlockStreamDecoder>> {
        Self::from_encoded_buffer(prepared_buffer.data.clone(), &prepared_buffer.descriptor)
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

    /// Returns configuration for block checksums. Whether the checksums are
    /// available is determined by the policy of the encoded buffer during its
    /// creation.
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

    /// Creates a reader for accessing blocks within specified logical position ranges.
    ///
    /// This method provides a higher-level interface for creating block readers by accepting
    /// logical position ranges (value indices) rather than block ordinal ranges. It automatically
    /// converts position ranges to the corresponding block ranges and optimizes them for
    /// efficient I/O access patterns.
    ///
    /// # Arguments
    ///
    /// * `pos_ranges` - Iterator of ascending non--overlapping logical position ranges within
    ///   the data stream. Each range represents a span of values (not blocks) that are expected
    ///   to be read. Positions are zero-based value indices that span across all blocks in the stream.
    ///
    /// * `prefetch` - Controls whether to enable background prefetching for improved
    ///   read performance. See [`BlockReaderPrefetch`] for detailed behavior descriptions.
    ///
    /// # Returns
    ///
    /// Result containing a `BlockReader` configured for the specified position ranges,
    /// or an error if the conversion or reader creation fails.
    ///
    /// # Errors
    ///
    /// Returns errors if:
    /// - Position ranges extend beyond the total value count in the stream
    /// - The block map cannot be accessed or is corrupted
    /// - Block range optimization fails due to storage constraints
    /// - Reader creation fails due to invalid block ranges
    ///
    /// # See Also
    ///
    /// - [`Self::create_reader_with_block_ranges`] for direct block-based reader creation
    /// - [`BlockReaderPrefetch`] for prefetching behavior details
    /// - Block map methods for understanding position-to-block mapping
    pub fn create_reader_with_ranges(
        &self,
        pos_ranges: impl Iterator<Item = Range<u64>> + Clone,
        prefetch: BlockReaderPrefetch,
    ) -> Result<BlockReader> {
        let block_ranges = self.block_map.pos_ranges_to_block_ranges(pos_ranges)?;
        let block_ranges = self
            .block_map
            .compute_read_optimized_block_ranges(block_ranges.into_iter())?;
        self.create_reader_with_block_ranges(block_ranges, prefetch)
    }

    /// Creates a reader for accessing blocks containing specified logical positions.
    ///
    /// This method provides an efficient way to create block readers when you know the exact
    /// logical positions (value indices) you need to access, rather than position ranges.
    /// It automatically determines which blocks contain those positions and creates an
    /// optimized reader for accessing them.
    ///
    /// # Arguments
    ///
    /// * `positions` - Iterator of ascending logical positions (value indices) that will be
    ///   accessed. Positions are zero-based indices that span across all blocks in the stream.
    ///   Duplicate positions are automatically deduplicated - if multiple positions fall
    ///   within the same block, that block will only be included once in the reader.
    ///
    /// * `prefetch` - Controls whether to enable background prefetching for improved
    ///   read performance. See [`BlockReaderPrefetch`] for detailed behavior descriptions.
    ///
    /// # Returns
    ///
    /// Result containing a `BlockReader` configured to access blocks containing the specified
    /// positions, or an error if the conversion or reader creation fails.
    ///
    /// # Behavior
    ///
    /// The method performs several optimization steps:
    ///
    /// 1. **Position-to-block mapping**: Each logical position is mapped to the block
    ///    that contains it, creating individual block ranges (each containing exactly one block).
    ///
    /// 2. **Deduplication**: If multiple positions fall within the same block, that block
    ///    is only included once in the final block ranges.
    ///
    /// 3. **I/O optimization**: The resulting block ranges are analyzed and potentially
    ///    coalesced to minimize I/O operations while respecting storage constraints.
    ///    Adjacent or nearby blocks may be combined into larger ranges for more efficient
    ///    reading.
    ///
    /// # Performance Considerations
    ///
    /// This method is particularly efficient for:
    /// - **Sparse access patterns**: When you need to access specific values scattered
    ///   across the stream, rather than contiguous ranges
    /// - **Index-based lookups**: When you have a list of specific indices to retrieve
    ///
    /// # See Also
    ///
    /// - [`Self::create_reader_with_ranges`] for contiguous position ranges
    /// - [`Self::create_reader_with_block_ranges`] for direct block-based reader creation
    /// - [`BlockReaderPrefetch`] for prefetching behavior details
    pub fn create_reader_with_positions(
        &self,
        positions: impl Iterator<Item = u64> + Clone,
        prefetch: BlockReaderPrefetch,
    ) -> Result<BlockReader> {
        let block_ranges = self.block_map.positions_to_block_ranges(positions)?;
        let block_ranges = self
            .block_map
            .compute_read_optimized_block_ranges(block_ranges.into_iter())?;
        self.create_reader_with_block_ranges(block_ranges, prefetch)
    }

    /// Creates a reader for accessing blocks within specified ranges.
    ///
    /// # Arguments
    ///
    /// * `block_ranges` - Ranges of block ordinals expected to be read. When empty,
    ///   the reader uses a conservative access pattern that loads blocks on-demand
    ///   with minimal memory usage. When provided, these ranges guide prefetching
    ///   and block loading strategies.
    /// * `prefetch` - Controls whether to enable background prefetching for improved
    ///   read performance. Prefetching only occurs when `block_ranges` is non-empty
    ///   and provides predictable access patterns.
    ///
    /// # Returns
    ///
    /// Result containing a `BlockReader` or an error.
    ///
    /// # Performance Considerations
    ///
    /// The `block_ranges` parameter significantly affects memory usage and I/O patterns:
    ///
    /// - **Block range sizing**: The implementation preloads entire block ranges when
    ///   accessing blocks within them. Each range should be appropriately sized for
    ///   your access pattern - typically the optimized ranges computed by the block map.
    ///
    /// - **Memory impact**: Large block ranges will consume proportionally more memory
    ///   since the entire range is loaded into memory. Consider your available memory
    ///   when specifying ranges.
    ///
    /// - **Worst case scenario**: Passing a single range that spans all blocks will
    ///   cause the entire dataset to be loaded into memory, which may result in
    ///   excessive memory consumption and potential out-of-memory conditions.
    ///
    /// - **Empty ranges**: When `block_ranges` is empty, the reader falls back to
    ///   loading individual blocks or small ranges based on I/O size constraints,
    ///   using less memory but potentially requiring more I/O operations.
    ///
    /// For optimal performance, use block ranges that match your actual access patterns
    /// and are appropriately sized for your memory constraints.
    pub fn create_reader_with_block_ranges(
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
    /// `BlockStreamDecoder::create_reader_with_ranges`, the reader will create a background prefetcher
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
            Ok(OpaqueBlock {
                descriptor,
                raw_data,
                checksum: Some(checksum),
            })
        } else {
            Ok(OpaqueBlock {
                descriptor,
                raw_data,
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

/// A container for encoded block data read from a BlockStream.
///
/// An `OpaqueBlock` represents a single encoded data block that has been read
/// from storage but not yet decoded into its constituent values. It serves as
/// an intermediate representation between the raw bytes stored in the encoded
/// stream and the final decoded value sequence.
///
/// The block contains three key components:
/// - **Metadata**: Block positioning and size information via the descriptor
/// - **Encoded data**: The actual compressed/encoded block content
/// - **Integrity verification**: Optional checksum for data validation
///
/// # Checksum Behavior
///
/// Whether a block includes a checksum depends on the encoding policy used when
/// the BlockStream was created:
/// - When checksums are enabled, `raw_data` contains the encoded block data
///   followed by a 4-byte little-endian checksum, and `checksum` contains the
///   extracted value
/// - When checksums are disabled, `raw_data` contains only the encoded block data,
///   and `checksum` is `None`
///
/// # Usage
///
/// `OpaqueBlock` instances are typically obtained from a [`BlockReader`] and then
/// passed to field-specific decoders that understand the internal encoding format.
/// The block's opaque nature allows the BlockStream layer to handle storage and
/// retrieval without needing knowledge of the specific encoding schemes used for
/// different data types.
#[derive(Debug, Clone)]
pub struct OpaqueBlock {
    /// Block metadata describing its logical position and storage location.
    ///
    /// The descriptor contains the block's ordinal (sequential index), the range of
    /// logical positions it covers, and the byte range it occupies in storage.
    pub descriptor: BlockDescriptor,

    /// Raw block data including encoded content and optional checksum.
    ///
    /// This field contains the complete block as stored, which includes:
    /// - The encoded block data (variable length)
    /// - A 4-byte little-endian checksum appended at the end (when checksums
    ///   are enabled)
    ///
    /// Use the [`data()`](Self::data) method to access only the encoded content
    /// without the checksum suffix.
    pub raw_data: Bytes,

    /// Extracted checksum value when integrity verification is enabled.
    ///
    /// This field contains the 4-byte checksum value extracted from the end of
    /// `raw_data` when the BlockStream was encoded with checksum verification enabled.
    /// When `Some(value)`, the last 4 bytes of `raw_data` represent this checksum.
    /// When `None`, no checksum was included during encoding and `raw_data` contains
    /// only the encoded block content.
    pub checksum: Option<u32>,
}

impl OpaqueBlock {
    /// Returns the encoded block data without the trailing checksum.
    ///
    /// This method extracts only the meaningful encoded content from `raw_data`,
    /// excluding any trailing checksum bytes. The returned slice contains the
    /// actual encoded block data that should be passed to field-specific decoders.
    ///
    /// # Returns
    ///
    /// A byte slice containing the encoded block data. When checksums are enabled,
    /// this excludes the final 4 bytes which contain the checksum. When checksums
    /// are disabled, this returns the entire `raw_data` content.
    pub fn data(&self) -> &[u8] {
        let checksum_len = self.checksum.map(|_| 4usize).unwrap_or(0usize);
        let len = self.raw_data.len() - checksum_len;
        &self.raw_data[0..len]
    }

    /// Verifies the integrity of the block data using its checksum.
    ///
    /// This method validates the block's content against its stored checksum to detect
    /// data corruption during storage or transmission. The verification is performed
    /// only when the block was encoded with checksum verification enabled.
    ///
    /// The checksum algorithm used is consistent with the BlockStream format's
    /// integrity verification scheme. If verification fails, it indicates the block
    /// data has been corrupted and should not be trusted.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the checksum verification passes or if no checksum is present.
    ///
    /// # Performance Notes
    ///
    /// Checksum verification requires computing a hash (`xxh3_64` to be precise) over
    /// the entire block data, which has a computational cost proportional to the block
    /// size.
    pub fn verify_checksum(&self) -> Result<()> {
        if let Some(checksum) = self.checksum {
            amudai_format::checksum::validate_buffer(self.data(), checksum, Some("value block"))?;
        }
        Ok(())
    }
}

/// A container for decoded block data and its associated metadata.
///
/// `DecodedBlock` represents the result of successfully decoding an [`OpaqueBlock`] into
/// its constituent typed values. While [`OpaqueBlock`] contains raw encoded bytes,
/// `DecodedBlock` contains the actual decoded values ready for consumption.
///
/// This structure serves as the intermediate result in the block decoding pipeline:
/// 1. **Storage** → [`OpaqueBlock`] (raw encoded bytes + metadata)
/// 2. **Decoding** → `DecodedBlock` (typed values + metadata)  
/// 3. **Application** → Individual values or value ranges
///
/// # Relationship to OpaqueBlock
///
/// `DecodedBlock` maintains the same [`BlockDescriptor`] metadata as its source
/// [`OpaqueBlock`], ensuring that logical positioning and storage information is
/// preserved throughout the decoding process. This allows higher-level components
/// to correlate decoded values with their original storage locations and logical
/// positions within the stream.
///
/// # Caching and Performance
///
/// `DecodedBlock` instances are commonly cached by readers like [`GenericBufferReader`]
/// to optimize repeated access to the same block. Since decoding can be computationally
/// expensive (involving decompression, type conversion, etc.), caching decoded blocks
/// improves performance for sequential or overlapping read patterns.
///
/// # Memory Considerations
///
/// Decoded blocks typically consume more memory than their encoded counterparts since:
/// - Compression has been removed
/// - Values are in their native representation  
///
/// # Usage
///
/// `DecodedBlock` instances are typically:
/// - Created by block decoders when processing [`OpaqueBlock`] data
/// - Cached by buffer readers for performance optimization  
/// - Used to extract value ranges via [`ValueSequence`] methods
/// - Passed between layers that need both values and positional metadata
///
/// [`GenericBufferReader`]: super::generic_buffer::GenericBufferReader
#[derive(Debug, Clone)]
pub struct DecodedBlock {
    /// The decoded value sequence containing the actual typed data from this block.
    ///
    /// This field contains the decoded values in their native representation.
    /// The structure and content of the sequence depends on the field type
    /// and encoding scheme used during block creation.
    ///
    /// The sequence spans exactly the logical position range specified in the
    /// `descriptor.logical_range`, meaning the first value corresponds to logical
    /// position `logical_range.start` and the last value corresponds to logical
    /// position `logical_range.end - 1`.
    pub values: ValueSequence,

    /// Block metadata describing its logical position and storage location.
    ///
    /// The descriptor contains the block's ordinal (sequential index), the range of
    /// logical positions it covers, and the byte range it occupies in storage.
    /// This metadata is preserved from the original [`OpaqueBlock`] to maintain
    /// traceability between decoded values and their storage representation.
    ///
    /// Key information includes:
    /// - `ordinal`: Sequential block index within the stream
    /// - `logical_range`: Range of logical positions covered by this block's values
    /// - `storage_range`: Byte range in storage where this block's data is located
    pub descriptor: BlockDescriptor,
}

impl DecodedBlock {
    /// Creates an empty `DecodedBlock` with no values and empty logical range.
    ///
    /// This method constructs a placeholder `DecodedBlock` that represents an empty block
    /// with no actual data. It is commonly used for initialization in field decoders and
    /// other scenarios where a valid but empty block instance is needed.
    ///
    /// # Structure
    ///
    /// - `values`: An empty `ValueSequence` with `Unit` type (represents no data)
    /// - `descriptor`: A default `BlockDescriptor` with:
    ///   - `ordinal`: 0
    ///   - `logical_range`: Empty range (0..0)
    ///   - `storage_range`: Empty range (0..0)
    ///
    /// # Returns
    ///
    /// A `DecodedBlock` instance representing an empty block that can be used as
    /// a placeholder or initial value.
    pub fn empty() -> DecodedBlock {
        DecodedBlock {
            values: ValueSequence::empty(BasicTypeDescriptor {
                basic_type: BasicType::Unit,
                ..Default::default()
            }),
            descriptor: Default::default(),
        }
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
        write::{PreparedEncodedBuffer, primitive_buffer::PrimitiveBufferEncoder},
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
            .create_reader_with_block_ranges(vec![], BlockReaderPrefetch::Enabled)
            .unwrap();
        let block = reader.read_block(50).unwrap();

        assert_eq!(block.descriptor.ordinal, 50);
        assert!(!block.data().is_empty());
        assert!(block.checksum.is_some());
        block.verify_checksum().unwrap();
    }

    #[test]
    fn test_block_reader_sequential_blocks() {
        let buffer = create_test_block_stream(100, 100..500).unwrap();
        let decoder =
            BlockStreamDecoder::from_encoded_buffer(buffer.data, &buffer.descriptor).unwrap();

        let mut reader = decoder
            .create_reader_with_block_ranges(vec![], BlockReaderPrefetch::Enabled)
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
            .create_reader_with_block_ranges(ranges, BlockReaderPrefetch::Enabled)
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
            .create_reader_with_block_ranges(vec![], BlockReaderPrefetch::Enabled)
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
            .create_reader_with_block_ranges(vec![], BlockReaderPrefetch::Enabled)
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
            .create_reader_with_block_ranges(vec![], BlockReaderPrefetch::Enabled)
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
            .create_reader_with_block_ranges(block_ranges, BlockReaderPrefetch::Enabled)
            .unwrap();
        let block_count = buffer.descriptor.block_count.unwrap() as u32;

        for i in 0..block_count {
            let block = reader.read_block(i).unwrap();

            assert_eq!(block.descriptor.ordinal, i as u64);
            assert!(!block.data().is_empty());
            assert!(block.checksum.is_some());

            block.verify_checksum().unwrap();

            let logical_size = block.descriptor.logical_size();
            assert!(
                (100..300).contains(&logical_size),
                "Block {i} has unexpected logical size: {logical_size}"
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
            fixed_size: std::mem::size_of::<i32>() as u32,
            extended_type: Default::default(),
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
