//! Block map common definitions and operations.

use std::ops::Range;

use amudai_common::Result;
use amudai_io::StorageProfile;
use itertools::Itertools;

/// Describes a block within the block map.
///
/// A `BlockDescriptor` represents a single contiguous block in a block map,
/// linking a range of logical positions to their corresponding physical storage range.
///
/// # Fields
///
/// * `ordinal` - The sequential index of this block within the block map
/// * `logical_range` - The range of logical positions covered by this block
/// * `storage_range` - The range of bytes in storage where this block is physically stored
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BlockDescriptor {
    /// Block ordinal.
    pub ordinal: u64,
    /// Logical value range of this block.
    pub logical_range: Range<u64>,
    /// Location of this block in the encoded buffer.
    pub storage_range: Range<u64>,
}

impl BlockDescriptor {
    /// Checks if this block descriptor represents a valid block.
    ///
    /// A valid block must have a non-empty logical range.
    ///
    /// # Returns
    ///
    /// `true` if the block is valid (has a non-empty logical range), `false` otherwise.
    #[inline]
    pub fn is_valid(&self) -> bool {
        !self.logical_range.is_empty()
    }

    /// Checks if this block contains the specified logical position.
    ///
    /// # Arguments
    ///
    /// * `logical_pos` - The logical position to check
    ///
    /// # Returns
    ///
    /// `true` if the position is within this block's logical range, `false` otherwise.
    #[inline]
    pub fn contains(&self, logical_pos: u64) -> bool {
        self.logical_range.contains(&logical_pos)
    }

    /// Checks if this block fully contains the specified logical position range.
    ///
    /// # Arguments
    ///
    /// * `pos` - The logical position range to check
    ///
    /// # Returns
    ///
    /// `true` if the entire range is within this block's logical range, `false` otherwise.
    #[inline]
    pub fn contains_range(&self, pos: &Range<u64>) -> bool {
        pos.start >= self.logical_range.start && pos.end <= self.logical_range.end
    }

    /// Returns the size of this block in storage (bytes).
    ///
    /// # Returns
    ///
    /// The number of bytes this block occupies in storage.
    #[inline]
    pub fn storage_size(&self) -> usize {
        (self.storage_range.end - self.storage_range.start) as usize
    }

    /// Returns the logical size of this block (number of logical values).
    ///
    /// # Returns
    ///
    /// The number of logical values contained in this block.
    #[inline]
    pub fn logical_size(&self) -> usize {
        (self.logical_range.end - self.logical_range.start) as usize
    }

    /// Converts a logical position to a block-relative index.
    ///
    /// This method translates a global logical position within the data stream
    /// to an index within this specific block's value array. The returned index
    /// is zero-based and relative to the start of this block.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position within the data stream that falls
    ///   within this block's logical range
    ///
    /// # Returns
    ///
    /// The zero-based index within the block corresponding to the logical position.
    ///
    /// # Preconditions
    ///
    /// The caller must verify that `position` is **within this block's logical range**
    /// (i.e., `self.contains(position)` returns `true`) before calling this method.
    /// This precondition is enforced by a debug assertion but not checked in release builds.
    #[inline]
    pub fn position_index(&self, position: u64) -> usize {
        debug_assert!(self.contains(position));
        (position - self.logical_range.start) as usize
    }
}

/// Defines operations for accessing and navigating block maps, which map logical
/// value positions to physical storage locations.
///
/// A block map maintains a mapping between:
/// - Logical positions in a sequence of values
/// - Physical storage locations of encoded blocks where those values are stored
///
/// Implementors of this trait represent different manifestations of a block map,
/// such as an in-memory representation or a decoded on-disk format.
///
/// The trait supports:
/// - Querying metadata about the block map (value count, block count, storage size)
/// - Finding blocks by logical position
/// - Converting between logical ranges and storage ranges
/// - Optimizing block ranges for efficient I/O operations
///
/// # Key Concepts
///
/// - **Block**: A contiguous range of logical values mapped to a contiguous range
///   of bytes in storage
/// - **Logical Position**: An index within the logical value space
/// - **Storage Range**: A byte range within the physical storage
/// - **Block Ordinal**: The sequential index of a block within the block map
pub trait BlockMapOps {
    /// Returns the total number of logical values in the block map.
    fn value_count(&self) -> u64;

    /// Returns the total number of blocks in the block map.
    fn block_count(&self) -> u64;

    /// Returns `true` if this map contains no blocks.
    fn is_empty(&self) -> bool {
        self.block_count() == 0
    }

    /// Returns the total size in bytes of all blocks in storage.
    fn storage_size(&self) -> u64;

    /// Returns the block ordinal containing the given logical position.
    fn find_block_ordinal(&self, logical_pos: u64) -> Result<usize>;

    /// Returns the block descriptor for the given block ordinal.
    fn get_block(&self, block_ordinal: usize) -> Result<BlockDescriptor>;

    /// Returns the block descriptor containing the given logical position.
    fn find_block(&self, logical_pos: u64) -> Result<BlockDescriptor> {
        self.find_block_ordinal(logical_pos)
            .and_then(|i| self.get_block(i))
    }

    /// Converts a range of block ordinals to their corresponding storage byte range.
    fn block_range_to_storage_range(&self, block_range: Range<u32>) -> Result<Range<u64>> {
        let start = self
            .get_block(block_range.start as usize)?
            .storage_range
            .start;
        let end = if !block_range.is_empty() {
            self.get_block(block_range.end as usize - 1)?
                .storage_range
                .end
        } else {
            start
        };
        Ok(start..end)
    }

    /// Estimates the number of blocks needed to span the given number
    /// of bytes in storage.
    fn estimate_block_count_for_storage_size(&self, size: u64) -> usize {
        let block_count = self.block_count();
        if block_count < 2 {
            return 1;
        }
        let storage_size = self.storage_size().max(1);
        (block_count.saturating_mul(size) / storage_size).max(1) as usize
    }

    /// Converts logical position ranges to optimized block ranges for reading.
    fn map_position_ranges_to_block_ranges(
        &self,
        pos_ranges: impl IntoIterator<Item = Range<u64>>,
        profile: &StorageProfile,
    ) -> Result<Vec<Range<u32>>> {
        let mut block_ranges = Vec::<Range<u32>>::new();
        let mut last_block: Option<BlockDescriptor> = None;
        for mut pos_range in pos_ranges {
            if pos_range.is_empty() {
                continue;
            }
            if let Some(last_block) = last_block.as_ref() {
                if last_block.contains_range(&pos_range) {
                    continue;
                }
                if last_block.contains(pos_range.start) {
                    assert!(!last_block.contains(pos_range.end - 1));
                    pos_range.start = last_block.logical_range.end;
                }
            }
            self.add_block_ranges_for_pos_range(&pos_range, profile, &mut block_ranges)?;
            let last_block_idx = block_ranges.last().unwrap().end - 1;
            last_block = Some(self.get_block(last_block_idx as usize)?);
        }
        Ok(block_ranges)
    }

    /// Converts a sequence of logical positions to block ranges.
    fn map_positions_to_block_ranges(
        &self,
        positions: impl IntoIterator<Item = u64>,
    ) -> Result<Vec<Range<u32>>> {
        let mut block_ranges = Vec::<Range<u32>>::new();
        let mut last_block = 0u64..0u64;
        for pos in positions {
            if pos >= last_block.start && pos < last_block.end {
                continue;
            }
            let next_block = self.find_block(pos)?;
            block_ranges.push(next_block.ordinal as u32..next_block.ordinal as u32 + 1);
            last_block = next_block.logical_range;
        }
        Ok(block_ranges)
    }

    /// Optimizes block ranges for efficient I/O operations based on storage profile.
    ///
    /// # Default implementation details
    ///
    /// This method takes multiple block ranges and intelligently combines them to minimize
    /// the number of I/O operations while respecting storage constraints. It essentially
    /// "coalesces" adjacent or nearby block ranges when doing so would be more efficient
    /// than reading them separately.
    /// The coalescing decision happens in coalesce_block_ranges, which makes two key checks:
    ///
    /// 1. *Gap size check*: If the storage gap between two block ranges is smaller than
    ///    `profile.min_io_size``, it's more efficient to read them together.
    /// 2. *Total size check*: Only combine block ranges if the resulting storage range size
    ///    is less than `profile.max_io_size`.
    fn compute_read_optimized_block_ranges(
        &self,
        block_ranges: impl IntoIterator<Item = Range<u32>>,
        profile: &StorageProfile,
    ) -> Result<Vec<Range<u32>>> {
        Ok(block_ranges
            .into_iter()
            .coalesce(|a, b| self.coalesce_block_ranges(a, b, profile))
            .collect())
    }

    /// Converts block ranges to their corresponding storage byte ranges.
    fn block_ranges_to_storage_ranges(
        &self,
        block_ranges: impl IntoIterator<Item = Range<u32>>,
    ) -> Result<Vec<Range<u64>>> {
        block_ranges
            .into_iter()
            .map(|r| self.block_range_to_storage_range(r))
            .collect()
    }

    /// Adds block ranges to the provided vector that cover the given logical position range.
    ///
    /// This method breaks down a logical position range into block ranges, considering the
    /// storage profile's I/O size constraints. The ranges are sized to optimize I/O operations.
    ///
    /// # Arguments
    ///
    /// * `pos_range` - The logical position range to convert
    /// * `profile` - Storage profile containing I/O constraints
    /// * `block_ranges` - Vector to append the generated block ranges to
    ///
    /// # Returns
    ///
    /// `Ok(())` if the ranges were successfully added, or an error if any block lookup failed.
    fn add_block_ranges_for_pos_range(
        &self,
        pos_range: &Range<u64>,
        profile: &StorageProfile,
        block_ranges: &mut Vec<Range<u32>>,
    ) -> Result<()> {
        let start = self.find_block_ordinal(pos_range.start)? as u32;
        let end = self.find_block_ordinal(pos_range.end - 1)? as u32 + 1;
        let step = self.estimate_block_count_for_storage_size(profile.max_io_size as u64);
        block_ranges.extend(
            (start..end)
                .step_by(step)
                .zip(
                    (start + step as u32..end + step as u32)
                        .step_by(step)
                        .map(move |i| i.min(end)),
                )
                .map(|(s, e)| s..e),
        );
        Ok(())
    }

    /// Attempts to coalesce two adjacent block ranges based on I/O optimization criteria.
    ///
    /// Combines two block ranges if:
    /// - The gap between them is smaller than the minimum I/O size
    /// - The total combined range is smaller than the maximum I/O size
    ///
    /// # Arguments
    ///
    /// * `a` - First block range
    /// * `b` - Second block range (must be adjacent to or after `a`)
    /// * `profile` - Storage profile containing I/O size constraints
    ///
    /// # Returns
    ///
    /// * `Ok(range)` - Combined range if ranges can be coalesced
    /// * `Err((a, b))` - Original ranges if they cannot be coalesced
    fn coalesce_block_ranges(
        &self,
        a: Range<u32>,
        b: Range<u32>,
        profile: &StorageProfile,
    ) -> std::result::Result<Range<u32>, (Range<u32>, Range<u32>)> {
        let a_range = self
            .block_range_to_storage_range(a.clone())
            .map_err(|_| (a.clone(), b.clone()))?;
        let b_range = self
            .block_range_to_storage_range(b.clone())
            .map_err(|_| (a.clone(), b.clone()))?;
        assert!(a_range.end <= b_range.start);
        if b_range.start - a_range.end <= profile.min_io_size as u64
            && b_range.end - a_range.start <= profile.max_io_size as u64
        {
            Ok(a.start..b.end)
        } else {
            Err((a, b))
        }
    }
}
