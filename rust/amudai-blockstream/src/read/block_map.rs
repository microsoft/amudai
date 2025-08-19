use std::{
    ops::Range,
    sync::{Arc, OnceLock},
};

use amudai_common::{Result, error::Error, verify_arg, verify_data};
use amudai_io::{ReadAt, SlicedFile, StorageProfile};
use amudai_ranges::PositionSeries;

use super::{
    block_map_decoder::BlockMapDecoder,
    block_map_ops::{BlockDescriptor, BlockMapOps},
};

/// An efficient, fully-expanded representation of a block map that provides fast lookups
/// between logical positions and storage locations.
///
/// `BlockList` maintains parallel arrays of logical and storage offsets, with each pair
/// of entries defining a block's boundaries. The structure also includes acceleration
/// data structures (buckets) to optimize position lookups.
///
/// # Performance Characteristics
///
/// - Constant-time block lookup by ordinal
/// - Near-constant-time logical position lookup via bucket acceleration
/// - Higher memory usage than compressed formats but faster access
///
/// # Structure
///
/// The block list consists of:
/// - `logical_offsets`: Start positions of each block in the logical space, plus an end position
/// - `storage_offsets`: Start positions of each block in storage, plus an end position
/// - `buckets`: Accelerator array dividing logical space into fixed-size buckets for fast lookups
/// - `bucket_size`: Size of each bucket in the logical space
///
/// # Example
///
/// A block list with 3 blocks might look like:
/// - `logical_offsets = [0, 100, 250, 450]` (3 blocks covering logical positions 0-450)
/// - `storage_offsets = [0, 200, 450, 750]` (3 blocks in storage positions 0-750)
#[derive(Debug, Clone)]
pub struct BlockList {
    /// Logical positions where each block starts, plus the end position of the last block.
    /// The length is always `block_count + 1`.
    pub logical_offsets: Vec<u64>,

    /// Storage offsets where each block starts, plus the end offset of the last block.
    /// The length is always `block_count + 1` and matches the length of `logical_offsets`.
    pub storage_offsets: Vec<u64>,

    /// Accelerator array for fast lookups of blocks by logical position.
    /// Each entry contains the index of the first block that contains the logical
    /// position of the bucket's start.
    pub buckets: Vec<u64>,

    /// Size of each bucket in logical position space.
    /// Larger buckets mean smaller acceleration array but potentially more linear
    /// scanning.
    pub bucket_size: u64,
}

impl BlockList {
    /// Creates a new `BlockList` from arrays of logical and storage offsets.
    ///
    /// This constructor validates that:
    /// - Both offset arrays are non-empty
    /// - Both arrays have the same length
    /// - The first logical offset is 0 (starting at the beginning)
    ///
    /// The constructor automatically computes optimal buckets for accelerated lookups.
    ///
    /// # Arguments
    ///
    /// * `logical_offsets` - Array of logical positions where each block starts,
    ///   plus the end position of the last block
    /// * `block_offsets` - Array of storage offsets where each block starts,
    ///   plus the end offset of the last block
    ///
    /// # Returns
    ///
    /// A new `BlockList` instance with computed buckets
    ///
    /// # Panics
    ///
    /// Panics if any of the validation checks fail
    pub fn new(logical_offsets: Vec<u64>, block_offsets: Vec<u64>) -> BlockList {
        assert!(!logical_offsets.is_empty());
        assert!(!block_offsets.is_empty());
        assert_eq!(logical_offsets.len(), block_offsets.len());
        assert_eq!(logical_offsets[0], 0);

        let (buckets, bucket_size) = Self::compute_buckets(&logical_offsets);
        BlockList {
            logical_offsets,
            storage_offsets: block_offsets,
            buckets,
            bucket_size,
        }
    }

    /// Verifies the integrity and consistency of the block list.
    ///
    /// This method checks that:
    /// - The logical and storage offset arrays have the same length
    /// - Logical offsets are strictly ascending (each block starts after the previous)
    /// - Storage offsets are non-decreasing (blocks are contiguous or gapped in storage)
    ///
    /// # Returns
    ///
    /// `Ok(())` if the block list is valid, or an `Error` describing the problem
    pub fn verify(&self) -> Result<()> {
        verify_data!(
            self.logical_offsets,
            self.logical_offsets.len() == self.storage_offsets.len()
        );
        for (&prev, &next) in self
            .logical_offsets
            .iter()
            .zip(self.logical_offsets.iter().skip(1))
        {
            if prev >= next {
                return Err(Error::invalid_format(format!(
                    "BlockList::verify: logical offsets: {prev} < {next}"
                )));
            }
        }
        for (&prev, &next) in self
            .storage_offsets
            .iter()
            .zip(self.storage_offsets.iter().skip(1))
        {
            if prev > next {
                return Err(Error::invalid_format(format!(
                    "BlockList::verify: block offsets: {prev} <= {next}"
                )));
            }
        }
        Ok(())
    }

    /// Computes acceleration buckets for efficient logical position lookups.
    ///
    /// This method divides the logical position space into fixed-size buckets,
    /// with each bucket storing the index of the first block that contains or
    /// exceeds its starting position. This accelerates `find_block_ordinal` by
    /// providing a starting point for the search.
    ///
    /// # Arguments
    ///
    /// * `logical_offsets` - Array of logical positions where each block starts,
    ///   plus the end position of the last block
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - The computed bucket array
    /// - The bucket size in logical position units
    ///
    /// # Algorithm
    ///
    /// The bucket size is calculated based on the average block size to balance
    /// memory usage with lookup performance. Specifically, it's set to 8x the average
    /// block size, which provides a good tradeoff.
    fn compute_buckets(logical_offsets: &[u64]) -> (Vec<u64>, u64) {
        if logical_offsets.len() <= 1 {
            return (Vec::new(), 1);
        }
        let len = logical_offsets.len();
        let block_count = len - 1;
        let value_count = logical_offsets[block_count];
        let avg_block_size = ((value_count as f64 / block_count as f64).round() as u64).max(1);
        let bucket_size = avg_block_size.checked_mul(8).expect("mul");
        let mut buckets = Vec::with_capacity((value_count / bucket_size + 1) as usize);

        let mut logical_pos = 0u64;
        let mut block_index = 0usize;
        while logical_pos < value_count {
            while logical_offsets[block_index + 1] <= logical_pos {
                block_index += 1;
            }
            buckets.push(block_index as u64);
            logical_pos += bucket_size;
        }

        (buckets, bucket_size)
    }
}

impl BlockMapOps for BlockList {
    fn value_count(&self) -> u64 {
        *self.logical_offsets.last().unwrap()
    }

    fn block_count(&self) -> u64 {
        self.logical_offsets.len() as u64 - 1
    }

    fn storage_size(&self) -> u64 {
        self.storage_offsets
            .last()
            .unwrap()
            .checked_sub(self.storage_offsets[0])
            .expect("storage size")
    }

    fn find_block_ordinal(&self, logical_pos: u64) -> Result<usize> {
        verify_data!(self.logical_offsets, self.logical_offsets.len() > 1);
        assert_ne!(self.bucket_size, 0);
        let bucket_id = (logical_pos / self.bucket_size) as usize;
        verify_data!(bucket_id, bucket_id < self.buckets.len());
        let mut i = self.buckets[bucket_id] as usize;
        assert!(i < self.logical_offsets.len());
        while i < self.logical_offsets.len() - 1 {
            if self.logical_offsets[i] <= logical_pos && self.logical_offsets[i + 1] > logical_pos {
                return Ok(i);
            }
            i += 1;
        }
        Err(Error::invalid_arg(
            "logical_pos",
            format!("Cannot find block ordinal for logical pos {logical_pos}"),
        ))
    }

    fn get_block(&self, block_ordinal: usize) -> Result<BlockDescriptor> {
        verify_arg!(
            block_ordinal,
            block_ordinal + 1 < self.logical_offsets.len()
        );
        let logical_range =
            self.logical_offsets[block_ordinal]..self.logical_offsets[block_ordinal + 1];
        let storage_range =
            self.storage_offsets[block_ordinal]..self.storage_offsets[block_ordinal + 1];
        Ok(BlockDescriptor {
            ordinal: block_ordinal as u64,
            logical_range,
            storage_range,
        })
    }

    fn block_range_to_storage_range(&self, block_range: Range<u32>) -> Result<Range<u64>> {
        verify_arg!(
            block_range,
            (block_range.start as usize) < self.storage_offsets.len()
        );
        verify_arg!(
            block_range,
            (block_range.end as usize) < self.storage_offsets.len()
        );
        Ok(self.storage_offsets[block_range.start as usize]
            ..self.storage_offsets[block_range.end as usize])
    }
}

/// A block map that provides efficient translation between logical value positions
/// and storage block ranges.
///
/// `BlockMap` maintains an index mapping logical values to their physical storage
/// locations. It supports two internal representations:
///
/// 1. `BlockMapDecoder`: A compact, memory-efficient representation that loads data
///    on demand
/// 2. `BlockList`: A fully expanded representation optimized for frequent lookups
///
/// The structure automatically switches between these representations based on access
/// patterns to optimize for either memory usage or performance.
///
/// # Caching Behavior
///
/// The block map lazily initializes internal structures when needed:
/// - The decoder is created on first access and cached
/// - The full block list is only expanded when access patterns indicate it would be
///   beneficial
pub struct BlockMap {
    reader: SlicedFile<Arc<dyn ReadAt>>,
    profile: StorageProfile,
    decoder: OnceLock<BlockMapDecoder>,
    list: OnceLock<BlockList>,
}

impl BlockMap {
    /// Creates a new `BlockMap` from a reader and storage profile.
    ///
    /// This constructor creates a lightweight wrapper around the provided reader.
    /// The actual decoder and block list are only initialized when needed.
    ///
    /// # Arguments
    ///
    /// * `reader` - A sliced reader pointing to the encoded block map data
    /// * `profile` - Storage profile containing I/O constraints used for optimizations
    ///
    /// # Returns
    ///
    /// A new `BlockMap` instance that lazily initializes internal structures
    pub fn new(reader: SlicedFile<Arc<dyn ReadAt>>, profile: StorageProfile) -> BlockMap {
        BlockMap {
            reader,
            profile,
            decoder: Default::default(),
            list: Default::default(),
        }
    }

    /// Returns the storage profile associated with this block map.
    ///
    /// The storage profile contains parameters used for I/O optimizations.
    ///
    /// # Returns
    ///
    /// A clone of the storage profile
    pub fn storage_profile(&self) -> StorageProfile {
        self.profile.clone()
    }

    /// Ensures the decoder is initialized and returns a reference to it.
    ///
    /// If the decoder hasn't been initialized yet, this method will create it.
    ///
    /// # Returns
    ///
    /// A result containing a reference to the initialized decoder, or an error
    /// if the decoder couldn't be created.
    pub fn establish_decoder(&self) -> Result<&BlockMapDecoder> {
        if let Some(decoder) = self.decoder.get() {
            Ok(decoder)
        } else {
            self.create_decoder()
        }
    }

    /// Returns a reference to the decoder if it has been initialized.
    ///
    /// Unlike `establish_decoder()`, this method never initializes the decoder.
    ///
    /// # Returns
    ///
    /// `Some` reference to the decoder if initialized, or `None` if not yet
    /// initialized.
    pub fn get_decoder(&self) -> Option<&BlockMapDecoder> {
        self.decoder.get()
    }

    /// Ensures the block list is initialized and returns a reference to it.
    ///
    /// If the block list hasn't been initialized yet, this method will create it
    /// by expanding the decoder.
    ///
    /// # Returns
    ///
    /// A result containing a reference to the initialized block list, or an error
    /// if the list couldn't be created.
    pub fn establish_block_list(&self) -> Result<&BlockList> {
        if let Some(list) = self.list.get() {
            Ok(list)
        } else {
            self.create_block_list()
        }
    }

    /// Returns a reference to the block list if it has been initialized.
    ///
    /// Unlike `establish_block_list()`, this method never initializes the block list.
    ///
    /// # Returns
    ///
    /// `Some` reference to the block list if initialized, or `None` if not yet initialized.
    pub fn get_block_list(&self) -> Option<&BlockList> {
        self.list.get()
    }

    /// Returns the total number of blocks in the block map.
    ///
    /// # Returns
    ///
    /// A result containing the block count, or an error if the decoder couldn't be accessed.
    pub fn block_count(&self) -> Result<u64> {
        Ok(self.establish_decoder()?.block_count())
    }

    /// Returns the total number of logical values in the block map.
    ///
    /// # Returns
    ///
    /// A result containing the value count, or an error if the decoder couldn't be accessed.
    pub fn value_count(&self) -> Result<u64> {
        Ok(self.establish_decoder()?.value_count())
    }

    /// Returns the total size in bytes of all blocks in storage.
    ///
    /// # Returns
    ///
    /// A result containing the storage size in bytes, or an error if the decoder couldn't be accessed.
    pub fn storage_size(&self) -> Result<u64> {
        Ok(self.establish_decoder()?.storage_size())
    }

    /// Finds the block ordinal containing the given logical position.
    ///
    /// This method uses the most efficient representation available (block list if loaded,
    /// otherwise decoder).
    ///
    /// # Arguments
    ///
    /// * `logical_pos` - The logical position to search for
    ///
    /// # Returns
    ///
    /// A result containing the block ordinal, or an error if the position is invalid.
    pub fn find_block_ordinal(&self, logical_pos: u64) -> Result<usize> {
        if let Some(list) = self.get_block_list() {
            list.find_block_ordinal(logical_pos)
        } else {
            self.establish_decoder()?.find_block_ordinal(logical_pos)
        }
    }

    /// Returns the block descriptor for a given block ordinal.
    ///
    /// This method uses the most efficient representation available (block list if loaded,
    /// otherwise decoder).
    ///
    /// # Arguments
    ///
    /// * `block_ordinal` - The block ordinal to retrieve
    ///
    /// # Returns
    ///
    /// A result containing the requested `BlockDescriptor`, or an error if the ordinal is invalid.
    pub fn get_block(&self, block_ordinal: usize) -> Result<BlockDescriptor> {
        if let Some(list) = self.get_block_list() {
            list.get_block(block_ordinal)
        } else {
            self.establish_decoder()?.get_block(block_ordinal)
        }
    }

    /// Finds the block containing the given logical position.
    ///
    /// This method uses the most efficient representation available (block list if loaded,
    /// otherwise decoder).
    ///
    /// # Arguments
    ///
    /// * `logical_pos` - The logical position to search for
    ///
    /// # Returns
    ///
    /// A result containing the `BlockDescriptor` for the block containing the position,
    /// or an error if the position is invalid.
    pub fn find_block(&self, logical_pos: u64) -> Result<BlockDescriptor> {
        if let Some(list) = self.get_block_list() {
            list.find_block(logical_pos)
        } else {
            self.establish_decoder()?.find_block(logical_pos)
        }
    }

    /// Converts a range of block ordinals to their corresponding storage byte range.
    ///
    /// This method uses the most efficient representation available (block list if loaded,
    /// otherwise decoder).
    ///
    /// # Arguments
    ///
    /// * `block_range` - The range of block ordinals to convert
    ///
    /// # Returns
    ///
    /// A result containing the storage byte range, or an error if the block range is invalid.
    pub fn block_range_to_storage_range(&self, block_range: Range<u32>) -> Result<Range<u64>> {
        if let Some(list) = self.get_block_list() {
            list.block_range_to_storage_range(block_range)
        } else {
            self.establish_decoder()?
                .block_range_to_storage_range(block_range)
        }
    }

    /// Translates a sequence of logical positions or logical position ranges to the
    /// corresponding storage block ranges.
    ///
    /// This method provides a unified interface for converting either individual positions
    /// or position ranges into block ranges. It leverages the `PositionSeries` trait to
    /// automatically dispatch to the appropriate conversion method based on the input type.
    ///
    /// # Type Flexibility
    ///
    /// The method accepts any type that implements `PositionSeries<u64>`, which includes:
    ///
    /// - **Individual positions**: Arrays, slices, vectors, or iterators of `u64` values
    /// - **Position ranges**: Arrays, slices, vectors, or iterators of `Range<u64>`
    ///
    /// # Arguments
    ///
    /// * `positions` - A sequence that implements `PositionSeries<u64>`. This can be:
    ///   - An iterator, array, slice, or vector of individual `u64` positions.
    ///     The positions must be sorted in ascending order.
    ///   - An iterator, array, slice, or vector of `Range<u64>` position ranges.
    ///     The ranges must be in ascending order and must not overlap.
    ///   
    /// # Returns
    ///
    /// A `Result` containing:
    /// - `Ok(Vec<Range<u32>>)`: Vector of block ranges that contain the specified positions.
    ///   Each range represents a contiguous sequence of blocks by their ordinals.
    /// - `Err`: If any position is out of bounds or if block lookup fails
    ///
    /// # Behavior
    ///
    /// The method inspect the provided `PositionSeries` to determine whether the input
    /// contains individual positions or ranges, then dispatches to the appropriate specialized
    /// implementation:
    ///
    /// - For individual positions: Calls [`map_positions_to_block_ranges`](Self::map_positions_to_block_ranges)
    /// - For position ranges: Calls [`map_position_ranges_to_block_ranges`](Self::map_position_ranges_to_block_ranges)
    ///
    /// # Performance Considerations
    ///
    /// - **Individual positions**: Each position potentially requires a block lookup. Adjacent
    ///   positions within the same block are automatically deduplicated.
    /// - **Position ranges**: More efficient for contiguous data access as entire ranges
    ///   can be processed together.
    ///
    /// # See Also
    ///
    /// - [`map_positions_to_block_ranges`](Self::map_positions_to_block_ranges) - direct method
    ///   for individual positions
    /// - [`map_position_ranges_to_block_ranges`](Self::map_position_ranges_to_block_ranges) - direct
    ///   method for position ranges
    /// - [`PositionSeries`] - The trait that enables this type flexibility
    pub fn positions_to_blocks(
        &self,
        positions: impl PositionSeries<u64>,
    ) -> Result<Vec<Range<u32>>> {
        if positions.is_ranges() {
            self.map_position_ranges_to_block_ranges(positions.into_ranges())
        } else {
            self.map_positions_to_block_ranges(positions.into_positions())
        }
    }

    /// Converts logical position ranges to optimized block ranges for reading.
    ///
    /// This method intelligently chooses between decoder and block list based on:
    /// - Whether the block list is already loaded
    /// - Whether the request pattern would benefit from loading the block list
    ///
    /// # Arguments
    ///
    /// * `pos_ranges` - Iterator of logical position ranges to convert
    ///
    /// # Returns
    ///
    /// A result containing vector of block ranges optimized for reading, or an error
    /// if any position is invalid.
    pub fn map_position_ranges_to_block_ranges(
        &self,
        pos_ranges: impl IntoIterator<Item = Range<u64>> + Clone,
    ) -> Result<Vec<Range<u32>>> {
        if let Some(list) = self.get_block_list() {
            list.map_position_ranges_to_block_ranges(pos_ranges, &self.profile)
        } else if self.should_load_block_list_for_pos_ranges(pos_ranges.clone()) {
            self.establish_block_list()?
                .map_position_ranges_to_block_ranges(pos_ranges, &self.profile)
        } else {
            self.establish_decoder()?
                .map_position_ranges_to_block_ranges(pos_ranges, &self.profile)
        }
    }

    /// Converts a sequence of logical positions to block ranges.
    ///
    /// This method intelligently chooses between decoder and block list based on:
    /// - Whether the block list is already loaded
    /// - Whether the request pattern would benefit from loading the block list
    ///
    /// # Arguments
    ///
    /// * `positions` - Iterator of logical positions to convert
    ///
    /// # Returns
    ///
    /// A result containing vector of block ranges covering the positions, or an error
    /// if any position is invalid.
    pub fn map_positions_to_block_ranges(
        &self,
        positions: impl IntoIterator<Item = u64> + Clone,
    ) -> Result<Vec<Range<u32>>> {
        if let Some(list) = self.get_block_list() {
            list.map_positions_to_block_ranges(positions)
        } else if self.should_load_block_list_for_positions(positions.clone()) {
            self.establish_block_list()?
                .map_positions_to_block_ranges(positions)
        } else {
            self.establish_decoder()?
                .map_positions_to_block_ranges(positions)
        }
    }

    /// Optimizes block ranges for efficient I/O operations based on storage profile.
    ///
    /// This method intelligently chooses between decoder and block list based on:
    /// - Whether the block list is already loaded
    /// - The number of block ranges to process
    ///
    /// # Arguments
    ///
    /// * `block_ranges` - Iterator of block ranges to optimize
    ///
    /// # Returns
    ///
    /// A result containing vector of optimized block ranges, or an error
    /// if any block range is invalid.
    pub fn compute_read_optimized_block_ranges(
        &self,
        block_ranges: impl IntoIterator<Item = Range<u32>> + Clone,
    ) -> Result<Vec<Range<u32>>> {
        if let Some(list) = self.get_block_list() {
            list.compute_read_optimized_block_ranges(block_ranges, &self.profile)
        } else if block_ranges.clone().into_iter().count() as u64 * 2
            > Self::MAX_BLOCKS_FOR_BLOCK_MAP_DECODER
        {
            self.establish_block_list()?
                .compute_read_optimized_block_ranges(block_ranges, &self.profile)
        } else {
            self.establish_decoder()?
                .compute_read_optimized_block_ranges(block_ranges, &self.profile)
        }
    }

    /// Converts block ranges to their corresponding storage byte ranges.
    ///
    /// This method intelligently chooses between decoder and block list based on:
    /// - Whether the block list is already loaded
    /// - The number of block ranges to process
    ///
    /// # Arguments
    ///
    /// * `block_ranges` - Iterator of block ranges to convert
    ///
    /// # Returns
    ///
    /// A result containing vector of storage byte ranges, or an error
    /// if any block range is invalid.
    pub fn block_ranges_to_storage_ranges(
        &self,
        block_ranges: impl IntoIterator<Item = Range<u32>> + Clone,
    ) -> Result<Vec<Range<u64>>> {
        if let Some(list) = self.get_block_list() {
            list.block_ranges_to_storage_ranges(block_ranges)
        } else if block_ranges.clone().into_iter().count() as u64 * 2
            > Self::MAX_BLOCKS_FOR_BLOCK_MAP_DECODER
        {
            self.establish_block_list()?
                .block_ranges_to_storage_ranges(block_ranges)
        } else {
            self.establish_decoder()?
                .block_ranges_to_storage_ranges(block_ranges)
        }
    }

    /// Extends a given block range to ensure it meets the minimum I/O size preference.
    ///
    /// This method adjusts the end of the block range to include additional blocks
    /// if the range is smaller than the estimated minimum block count recommended for
    /// the storage profile's minimum I/O size. The range is clamped to the total
    /// number of blocks in the block map.
    ///
    /// # Arguments
    ///
    /// * `block_range` - The range of block ordinals to extend.
    ///
    /// # Returns
    ///
    /// A result containing the extended block range, or an error if the decoder
    /// could not be established.
    pub fn extend_block_range(&self, block_range: Range<u32>) -> Result<Range<u32>> {
        let decoder = self.establish_decoder()?;
        let end_block = decoder.block_count() as u32;
        if block_range.start >= end_block || block_range.end >= end_block {
            return Ok(block_range);
        }
        let min_count =
            decoder.estimate_block_count_for_storage_size(self.profile.min_io_size as u64);
        let end = block_range.start.saturating_add(min_count as u32);
        Ok(block_range.start..block_range.end.max(end).min(end_block))
    }
}

impl BlockMap {
    /// Maximum number of blocks to process using the decoder before switching
    /// to the block list.
    ///
    /// When operations involve more than this number of blocks, the implementation
    /// will load the full block list to improve performance.
    const MAX_BLOCKS_FOR_BLOCK_MAP_DECODER: u64 = 16u64;

    /// Creates the internal block map decoder.
    ///
    /// This method is marked with `#[cold]` as it's expected to be called just once
    /// during the lifetime of a `BlockMap`.
    ///
    /// # Returns
    ///
    /// A result containing a reference to the newly created decoder, or an error
    /// if the decoder couldn't be initialized.
    #[cold]
    fn create_decoder(&self) -> Result<&BlockMapDecoder> {
        let decoder = BlockMapDecoder::new(self.reader.clone())?;
        let _ = self.decoder.set(decoder);
        Ok(self.decoder.get().expect("decoder"))
    }

    /// Creates the internal block list by expanding the decoder.
    ///
    /// This method is marked with `#[cold]` as it's expected to be called just once
    /// during the lifetime of a `BlockMap` when access patterns indicate that the
    /// full block list would be beneficial.
    ///
    /// The implementation first ensures the decoder is preloaded (fully in memory)
    /// for optimal expansion performance, then expands it into a full block list.
    ///
    /// # Returns
    ///
    /// A result containing a reference to the newly created block list, or an error
    /// if the list couldn't be initialized.
    #[cold]
    fn create_block_list(&self) -> Result<&BlockList> {
        let decoder = self.establish_decoder()?.preload()?;
        let list = decoder.expand()?;
        let _ = self.list.set(list);
        Ok(self.list.get().expect("decoder"))
    }

    /// Determines whether to load the full block list based on position range
    /// access patterns.
    ///
    /// This method implements an optimization strategy in `BlockMap` by deciding
    /// when to switch from the memory-efficient `BlockMapDecoder` to the performance-optimized
    /// `BlockList`. The decision is based on estimating how many blocks would be
    /// accessed when processing the given position ranges.
    fn should_load_block_list_for_pos_ranges(
        &self,
        pos_ranges: impl IntoIterator<Item = Range<u64>>,
    ) -> bool {
        estimate_block_count_for_pos_ranges(
            self.estimate_values_per_block() as f64,
            pos_ranges.into_iter(),
            Self::MAX_BLOCKS_FOR_BLOCK_MAP_DECODER,
        ) >= Self::MAX_BLOCKS_FOR_BLOCK_MAP_DECODER
    }

    /// Determines whether to load the full block list based on position access patterns.
    ///
    /// This method implements an optimization strategy in `BlockMap` by deciding
    /// when to switch from the memory-efficient `BlockMapDecoder` to the performance-optimized
    /// `BlockList`. The decision is based on estimating how many blocks would be
    /// accessed when processing the given sequence of logical positions.
    fn should_load_block_list_for_positions(
        &self,
        positions: impl IntoIterator<Item = u64>,
    ) -> bool {
        estimate_block_count_for_positions(
            self.estimate_values_per_block() as f64,
            positions.into_iter(),
            Self::MAX_BLOCKS_FOR_BLOCK_MAP_DECODER,
        ) >= Self::MAX_BLOCKS_FOR_BLOCK_MAP_DECODER
    }

    /// Estimates the average number of logical values per block.
    ///
    /// This is a utility method used by the block estimation algorithms to determine
    /// the approximate size of blocks in the logical value space.
    ///
    /// # Returns
    ///
    /// The estimated number of logical values per block, guaranteed to be at least 1.
    /// If block count or value count information is unavailable, defaults to 1.
    fn estimate_values_per_block(&self) -> u64 {
        let block_count = self.block_count().unwrap_or(1).max(1);
        let value_count = self.value_count().unwrap_or(1).max(1);
        (value_count / block_count).max(1)
    }
}

/// Estimates the number of blocks covered by a list of position ranges.
///
/// # Arguments
///
/// * `mean_block_size`: Mean block size estimate.
/// * `pos_ranges`: An iterator yielding sorted, non-overlapping index ranges `start..end`.
/// * `max_blocks`: Stops the computation once `max_blocks` is reached.
fn estimate_block_count_for_pos_ranges(
    mean_block_size: f64,
    pos_ranges: impl Iterator<Item = Range<u64>>,
    max_blocks: u64,
) -> u64 {
    let mean_block_size = mean_block_size.max(1.0);
    let mut total_estimate: f64 = 0.0;
    let mut pos_ranges = pos_ranges.peekable();

    while let Some(first_range) = pos_ranges.next() {
        if first_range.start >= first_range.end {
            // Skip invalid or empty range
            continue;
        }

        // Start a new conceptually merged block
        let mut merged_end = first_range.end;
        let mut merged_len: f64 = (first_range.end - first_range.start) as f64;

        // Check subsequent ranges for merging
        while let Some(next_range) = pos_ranges.peek() {
            if next_range.start >= next_range.end {
                // Consume and skip invalid range
                pos_ranges.next();
                continue;
            }

            // Calculate the gap between the end of the current merged block and the start
            // of the next range.
            let gap = next_range.start.saturating_sub(merged_end);

            // Check if the gap is smaller than the mean block size
            if (gap as f64) < mean_block_size {
                // Gap is small: Merge this range into the current block.
                // Consume the range from the iterator.
                let range_to_merge = pos_ranges.next().unwrap(); // We know it exists from peek()

                // Update the end marker of the merged block
                merged_end = range_to_merge.end;
                // Update the length of the merged range
                merged_len = (merged_end - first_range.start) as f64;
            } else {
                // Gap is large enough: Stop merging for this block.
                break;
            }
        }

        // Calculate the estimate for the completed merged block.
        total_estimate += merged_len / mean_block_size + 1.0;
        let estimate = total_estimate.round() as u64;
        if estimate >= max_blocks {
            return estimate;
        }
    }

    total_estimate.round() as u64
}

/// Estimates the number of blocks covered by a sequence of ascending positions.
///
/// This method uses a probabilistic model to estimate how many distinct blocks
/// would be accessed when reading the given logical positions. The approach is
/// based on the distances between consecutive positions relative to the mean
/// block size.
///
/// # Arguments
///
/// * `mean_block_size`: Mean block size estimate.
/// * `positions`: An iterator yielding `u64` values representing ascending positions
///   within the sequence.
/// * `max_blocks`: Stops the computation once `max_blocks` is reached.
///
/// # Implementation Details
///
/// The method uses a probabilistic model based on the mean block size:
/// - It assumes the probability of two positions `p_i` and `p_{i+1}` being
///   in different blocks depends on the distance between them (`d = p_{i+1} - p_i`)
///   and the mean block size (`m`).
/// - The probability is approximated as `1 - exp(-d / m)`.
/// - The total estimated block count starts at 1 (for the first position) and increments
///   by this probability for each subsequent position relative to its predecessor.
fn estimate_block_count_for_positions(
    mean_block_size: f64,
    mut positions: impl Iterator<Item = u64>,
    max_blocks: u64,
) -> u64 {
    let mean_block_size = mean_block_size.max(1.0);

    let mut prev_pos = match positions.next() {
        Some(pos) => pos,
        None => return 0, // No positions, no blocks covered.
    };

    // The first position covers at least one block.
    let mut estimated_blocks = 1.0f64;

    for current_pos in positions {
        if current_pos > prev_pos {
            let distance = (current_pos - prev_pos) as f64;

            // Calculate the probability that current_pos is in a different block than
            // prev_pos:
            let prob_different_block = 1.0 - (-distance / mean_block_size).exp();

            // Add this probability to the total estimate
            estimated_blocks += prob_different_block;

            let estimate = estimated_blocks.round() as u64;
            if estimate >= max_blocks {
                return estimate;
            }
        }
        prev_pos = current_pos;
    }

    estimated_blocks.round() as u64
}

#[cfg(test)]
#[allow(clippy::single_range_in_vec_init)]
mod tests {
    use std::ops::Range;

    use crate::read::{
        block_map::{estimate_block_count_for_pos_ranges, estimate_block_count_for_positions},
        block_map_ops::BlockMapOps,
    };

    use super::BlockList;

    #[test]
    fn test_block_list_creation() {
        fastrand::seed(12345);
        for block_count in 1..50 {
            let list = BlockList::new(
                generate_list(block_count, 1..50),
                generate_list(block_count, 100..120),
            );
            list.verify().unwrap();
            for i in 0..list.value_count() {
                let idx = list.find_block_ordinal(i).unwrap();
                let block = list.get_block(idx).unwrap();
                assert!(block.logical_range.contains(&i));
            }
            assert!(list.find_block_ordinal(list.value_count()).is_err());
            assert!(list.find_block_ordinal(list.value_count() + 1).is_err());
            assert!(list.find_block_ordinal(list.value_count() + 100).is_err());
        }
    }

    fn generate_list(count: usize, step: Range<u64>) -> Vec<u64> {
        (0..count)
            .map(|_| fastrand::u64(step.clone()))
            .scan(0u64, |state, x| {
                let y = *state;
                *state += x;
                Some(y)
            })
            .collect()
    }

    #[test]
    fn test_block_list_value_and_block_count() {
        let logical_offsets = vec![0, 100, 250, 400];
        let storage_offsets = vec![0, 200, 450, 700];

        let list = BlockList::new(logical_offsets, storage_offsets);

        assert_eq!(list.value_count(), 400);
        assert_eq!(list.block_count(), 3);
        assert_eq!(list.storage_size(), 700);
    }

    #[test]
    fn test_block_list_get_block() {
        let logical_offsets = vec![0, 100, 250, 400];
        let storage_offsets = vec![0, 200, 450, 700];

        let list = BlockList::new(logical_offsets, storage_offsets);

        // Check first block
        let block = list.get_block(0).unwrap();
        assert_eq!(block.ordinal, 0);
        assert_eq!(block.logical_range, 0..100);
        assert_eq!(block.storage_range, 0..200);

        // Check middle block
        let block = list.get_block(1).unwrap();
        assert_eq!(block.ordinal, 1);
        assert_eq!(block.logical_range, 100..250);
        assert_eq!(block.storage_range, 200..450);

        // Check last block
        let block = list.get_block(2).unwrap();
        assert_eq!(block.ordinal, 2);
        assert_eq!(block.logical_range, 250..400);
        assert_eq!(block.storage_range, 450..700);

        // Out of bounds block
        assert!(list.get_block(3).is_err());
    }

    #[test]
    fn test_find_block_ordinal() {
        let logical_offsets = vec![0, 100, 250, 400];
        let storage_offsets = vec![0, 200, 450, 700];

        let list = BlockList::new(logical_offsets, storage_offsets);

        // Test boundaries and middle of each block
        assert_eq!(list.find_block_ordinal(0).unwrap(), 0);
        assert_eq!(list.find_block_ordinal(50).unwrap(), 0);
        assert_eq!(list.find_block_ordinal(99).unwrap(), 0);

        assert_eq!(list.find_block_ordinal(100).unwrap(), 1);
        assert_eq!(list.find_block_ordinal(175).unwrap(), 1);
        assert_eq!(list.find_block_ordinal(249).unwrap(), 1);

        assert_eq!(list.find_block_ordinal(250).unwrap(), 2);
        assert_eq!(list.find_block_ordinal(325).unwrap(), 2);
        assert_eq!(list.find_block_ordinal(399).unwrap(), 2);

        // Out of bounds
        assert!(list.find_block_ordinal(400).is_err());
        assert!(list.find_block_ordinal(500).is_err());
    }

    #[test]
    fn test_block_list_compute_buckets() {
        // Test with small logical ranges
        let logical_offsets = vec![0, 10, 20, 30];
        let storage_offsets = vec![0, 100, 200, 300];

        let list = BlockList::new(logical_offsets, storage_offsets);

        // Buckets should be optimized for this size
        assert!(list.bucket_size > 0);
        assert!(!list.buckets.is_empty());

        // Test with larger logical ranges
        let logical_offsets = vec![0, 1000, 2000, 3000];
        let storage_offsets = vec![0, 1500, 3000, 4500];

        let list = BlockList::new(logical_offsets, storage_offsets);

        // Bucket size should be larger
        assert!(list.bucket_size > 0);
        assert!(!list.buckets.is_empty());

        // Test bucket effectiveness by ensuring each lookup starts at a reasonable position
        for pos in [0, 500, 999, 1000, 1500, 1999, 2000, 2500, 2999] {
            let bucket_id = (pos / list.bucket_size) as usize;
            if bucket_id < list.buckets.len() {
                let block_idx = list.buckets[bucket_id] as usize;
                assert!(block_idx < list.logical_offsets.len() - 1);
                // Verify the starting block is either containing or before the position
                assert!(list.logical_offsets[block_idx] <= pos);
            }
        }
    }

    #[test]
    fn test_block_range_to_storage_range() {
        let logical_offsets = vec![0, 100, 250, 400, 550];
        let storage_offsets = vec![0, 200, 450, 700, 900];

        let list = BlockList::new(logical_offsets, storage_offsets);

        // Single block range
        let storage_range = list.block_range_to_storage_range(0..1).unwrap();
        assert_eq!(storage_range, 0..200);

        // Multiple block range
        let storage_range = list.block_range_to_storage_range(1..3).unwrap();
        assert_eq!(storage_range, 200..700);

        // Full range
        let storage_range = list.block_range_to_storage_range(0..4).unwrap();
        assert_eq!(storage_range, 0..900);

        // Invalid range
        assert!(list.block_range_to_storage_range(5..6).is_err());
    }

    #[test]
    fn test_verify_validates_correct_block_lists() {
        // Valid ascending logical offsets, non-decreasing storage offsets
        let logical_offsets = vec![0, 100, 250, 400];
        let storage_offsets = vec![0, 200, 450, 700];

        let list = BlockList::new(logical_offsets, storage_offsets);
        assert!(list.verify().is_ok());

        // Valid with some equal storage offsets (zero-sized blocks in storage)
        let logical_offsets = vec![0, 100, 250, 400];
        let storage_offsets = vec![0, 200, 200, 700]; // Middle block has zero storage size

        let list = BlockList::new(logical_offsets, storage_offsets);
        assert!(list.verify().is_ok());
    }

    #[test]
    fn test_verify_detects_invalid_block_lists() {
        // Mismatched lengths
        let logical_offsets = vec![0, 100, 250, 400];
        let storage_offsets = vec![0, 200, 450]; // Missing last offset

        let list = BlockList {
            logical_offsets,
            storage_offsets,
            buckets: vec![],
            bucket_size: 1,
        };
        assert!(list.verify().is_err());

        // Non-ascending logical offsets
        let logical_offsets = vec![0, 250, 100, 400]; // Out of order
        let storage_offsets = vec![0, 200, 450, 700];

        let list = BlockList {
            logical_offsets,
            storage_offsets,
            buckets: vec![],
            bucket_size: 1,
        };
        assert!(list.verify().is_err());

        // Decreasing storage offsets
        let logical_offsets = vec![0, 100, 250, 400];
        let storage_offsets = vec![0, 700, 450, 900]; // Out of order

        let list = BlockList {
            logical_offsets,
            storage_offsets,
            buckets: vec![],
            bucket_size: 1,
        };
        assert!(list.verify().is_err());
    }

    #[test]
    fn test_edge_cases() {
        // Minimum valid block list (one block)
        let logical_offsets = vec![0, 10];
        let storage_offsets = vec![0, 20];

        let list = BlockList::new(logical_offsets, storage_offsets);
        assert_eq!(list.block_count(), 1);
        assert_eq!(list.value_count(), 10);
        assert_eq!(list.storage_size(), 20);

        // Block with zero logical size but non-zero storage size
        let logical_offsets = vec![0, 0, 10];
        let storage_offsets = vec![0, 5, 20];

        let list = BlockList {
            logical_offsets,
            storage_offsets,
            buckets: vec![],
            bucket_size: 1,
        };
        // This should fail verification because logical offsets must be strictly ascending
        assert!(list.verify().is_err());
    }

    #[test]
    fn test_estimate_block_count_for_pos_ranges() {
        // Test with empty ranges
        let ranges: Vec<Range<u64>> = Vec::new();
        assert_eq!(
            estimate_block_count_for_pos_ranges(10.0, ranges.into_iter(), 100),
            0
        );

        // Test with a single range smaller than mean block size
        let ranges = vec![0..5];
        assert_eq!(
            estimate_block_count_for_pos_ranges(10.0, ranges.into_iter(), 100),
            2
        );

        // Test with a single range larger than mean block size
        let ranges = vec![0..25];
        assert_eq!(
            estimate_block_count_for_pos_ranges(10.0, ranges.into_iter(), 100),
            4
        );

        // Test with multiple non-overlapping ranges with gaps smaller than mean block size
        // These should be treated as part of the same conceptual block
        let ranges = vec![0..5, 10..15, 20..25];
        assert_eq!(
            estimate_block_count_for_pos_ranges(10.0, ranges.into_iter(), 100),
            4
        );

        // Test with multiple non-overlapping ranges with gaps larger than mean block size
        // These should be treated as separate blocks
        let ranges = vec![0..5, 20..25, 40..45];
        assert_eq!(
            estimate_block_count_for_pos_ranges(10.0, ranges.into_iter(), 100),
            5
        );

        // Test with a mix of small and large gaps
        let ranges = vec![0..5, 10..15, 40..45, 46..50];
        assert_eq!(
            estimate_block_count_for_pos_ranges(10.0, ranges.into_iter(), 100),
            5
        );

        // Test with ranges that would lead to more blocks than max_blocks
        let ranges = vec![0..100, 150..250, 300..400, 500..600, 700..800];
        assert_eq!(
            estimate_block_count_for_pos_ranges(10.0, ranges.into_iter(), 20),
            22
        );

        // Test with very large ranges compared to mean block size
        let ranges = vec![0..1000];
        assert_eq!(
            estimate_block_count_for_pos_ranges(10.0, ranges.into_iter(), 200),
            101
        );

        // Test with very small mean block size
        let ranges = vec![0..100, 200..300];
        assert_eq!(
            estimate_block_count_for_pos_ranges(1.0, ranges.into_iter(), 300),
            202
        );

        // Test with edge case of mean block size = 0.0 (should not crash)
        let ranges = vec![0..10];
        let result = estimate_block_count_for_pos_ranges(0.0, ranges.into_iter(), 100);
        assert!(result > 0); // We don't care about the exact value, just that it didn't crash

        // Test early termination once max_blocks is reached
        let ranges = vec![0..1000, 2000..3000, 4000..5000];
        assert_eq!(
            estimate_block_count_for_pos_ranges(10.0, ranges.into_iter(), 50),
            101
        );
    }

    #[test]
    fn test_estimate_block_count_for_positions() {
        let positions: Vec<u64> = Vec::new();
        assert_eq!(
            estimate_block_count_for_positions(10.0, positions.into_iter(), 100),
            0
        );

        let positions = vec![5];
        assert_eq!(
            estimate_block_count_for_positions(10.0, positions.into_iter(), 100),
            1
        );

        let positions = vec![5, 8, 12, 15];
        let result = estimate_block_count_for_positions(10.0, positions.into_iter(), 100);
        assert!((1..=2).contains(&result));

        let positions = vec![5, 20, 35, 50];
        let result = estimate_block_count_for_positions(10.0, positions.into_iter(), 100);
        assert!(result >= 3);

        let positions = vec![5, 3, 20, 15, 40];
        let result = estimate_block_count_for_positions(10.0, positions.into_iter(), 100);
        assert!((1..=5).contains(&result));

        let positions: Vec<u64> = (0..1000).map(|i| i * 20).collect();
        assert_eq!(
            estimate_block_count_for_positions(10.0, positions.into_iter(), 50),
            50
        );

        let positions = vec![1, 3, 5, 7, 9];
        let result = estimate_block_count_for_positions(1.0, positions.into_iter(), 100);
        assert!(result >= 4);

        let positions = vec![5, 5, 5, 10, 20];
        let result = estimate_block_count_for_positions(10.0, positions.into_iter(), 100);
        assert!((2..=3).contains(&result));
    }

    #[test]
    fn test_integration_with_block_list() {
        // Mean block size is 100 logical units

        let positions = vec![50, 60, 70, 80];
        let value_per_block = 100.0;
        let result =
            estimate_block_count_for_positions(value_per_block, positions.into_iter(), 100);
        assert_eq!(result, 1);

        let positions = vec![50, 150, 250, 350];
        let result =
            estimate_block_count_for_positions(value_per_block, positions.into_iter(), 100);
        assert!(result >= 3);

        let ranges = vec![50..80];
        let result = estimate_block_count_for_pos_ranges(value_per_block, ranges.into_iter(), 100);
        assert_eq!(result, 1);

        let ranges = vec![50..250];
        let result = estimate_block_count_for_pos_ranges(value_per_block, ranges.into_iter(), 100);
        assert_eq!(result, 3);

        let ranges = vec![50..80, 150..180, 250..280];
        let result = estimate_block_count_for_pos_ranges(value_per_block, ranges.into_iter(), 100);
        assert_eq!(result, 3);
    }
}
