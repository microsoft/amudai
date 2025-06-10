use std::{
    ops::Range,
    sync::{Arc, OnceLock},
};

use amudai_bytes::Bytes;
use amudai_common::{Result, error::Error, verify_arg, verify_data};
use amudai_io::{ReadAt, sliced_read::SlicedReadAt};

use super::{
    block_map::BlockList,
    block_map_ops::{BlockDescriptor, BlockMapOps},
};

/// Decodes a block map, providing access to block descriptors.
///
/// A block map is a data structure that maps logical value positions to the containing blocks
/// and their storage ranges (offsets in the physical storage). This decoder provides random
/// access to block descriptors within the encoded block map.
///
/// The encoded block map is organized into segments to enable efficient navigation and search
/// operations without loading the entire structure into memory.
///
/// # Structure
///
/// The block map consists of:
/// - A footer containing metadata about the block map (value count, block count, etc.)
/// - A segment list containing offsets to individual segments
/// - Multiple segments, each containing information about a range of blocks
///
/// # Performance
///
/// This decoder uses lazy loading and caching to minimize I/O operations:
/// - Segments are loaded on-demand and cached in memory
/// - Binary search is used to quickly locate blocks by logical position
/// - The entire block map can be preloaded for better performance
pub struct BlockMapDecoder {
    /// A reader that provides access to a slice of the underlying storage.
    /// Points to the exact range containing the encoded block map data.
    /// Used to read the footer, segment list, and individual segments as needed.
    /// May be either a file-based reader or an in-memory buffer (when preloaded).
    reader: SlicedReadAt<Arc<dyn ReadAt>>,

    /// Contains metadata about the entire block map:
    /// - `value_count`: Total number of logical values (sum of all block sizes).
    /// - `block_count`: Total number of blocks in the map.
    /// - `segment_count`: Number of segments the block map is divided into.
    /// - `segment_size`: Maximum number of blocks per segment.
    ///
    /// Located at the end of the block map data and read during initialization.
    footer: Footer,

    /// Contains two arrays mapping segments to their locations:
    /// - `logical_offsets`: Logical positions where each segment starts.
    /// - `record_offsets`: Storage offsets where each segment's data is stored.
    ///
    /// Used to locate the correct segment for a given logical position.
    /// Enables efficient binary search to find segments without loading everything.
    segment_list: SegmentList,

    /// Lazily initialized cache for decoded segments.
    /// Stores segments after they're loaded from storage to avoid re-reading
    /// and re-parsing.
    /// Improves performance for repeated accesses to the same segment.
    /// Initialized on first access to any segment.
    segment_memo: OnceLock<BlockMapSegmentMemo>,
}

impl BlockMapDecoder {
    /// Creates a new `BlockMapDecoder` from a `SlicedReadAt` reader.
    ///
    /// The reader should be sliced to the exact range of the encoded block map.
    ///
    /// # Arguments
    ///
    /// * `reader` - A `SlicedReadAt` reader providing access to the block map data.
    pub fn new(reader: SlicedReadAt<Arc<dyn ReadAt>>) -> Result<BlockMapDecoder> {
        let footer = Self::read_footer(&reader)?;
        let segment_list = Self::read_segment_list(&reader, &footer)?;
        Ok(BlockMapDecoder {
            reader,
            footer,
            segment_list,
            segment_memo: OnceLock::new(),
        })
    }

    /// Returns the underlying reader.
    ///
    /// This provides access to the raw sliced reader that contains the encoded
    /// block map data.
    ///
    /// # Returns
    ///
    /// A reference to the `SlicedReadAt` reader used by this decoder.
    pub fn reader(&self) -> &SlicedReadAt<Arc<dyn ReadAt>> {
        &self.reader
    }

    /// Loads the entire encoded block map into memory and creates a `BlockMapDecoder`
    /// on top of it.
    ///
    /// This is useful for improving performance when the block map is small enough
    /// to fit in memory, or when you need to reduce I/O operations.
    ///
    /// # Arguments
    ///
    /// * `reader` - A `SlicedReadAt` reader providing access to the block map data.
    ///
    /// # Returns
    ///
    /// A `Result` containing the initialized memory-resident `BlockMapDecoder` or
    /// an error if the block map couldn't be read.
    ///
    /// # Performance
    ///
    /// This method trades memory usage for performance by loading the entire block map
    /// into memory, eliminating I/O operations for subsequent reads.
    pub fn preloaded(reader: SlicedReadAt<Arc<dyn ReadAt>>) -> Result<BlockMapDecoder> {
        let size = reader.slice_size();
        let bytes = reader.read_all()?;
        let reader = SlicedReadAt::new(Arc::new(bytes) as Arc<dyn ReadAt>, 0..size);
        BlockMapDecoder::new(reader)
    }

    /// Loads the entire encoded block map into memory, replacing the existing reader
    /// with an in-memory buffer.
    ///
    /// This returns a new `BlockMapDecoder` operating on the preloaded data.
    ///
    /// # Returns
    ///
    /// A `Result` containing a new `BlockMapDecoder` with the entire block map loaded
    /// into memory, or an error if the block map couldn't be read.
    ///
    /// # Performance
    ///
    /// This method is useful when you've already created a decoder but want to switch
    /// to in-memory operation for better performance. It's particularly beneficial for
    /// expanding the enstire block map into a `BlockList`.
    pub fn preload(&self) -> Result<BlockMapDecoder> {
        BlockMapDecoder::preloaded(self.reader.clone())
    }

    /// Locates a range of blocks containing the specified logical position range.
    ///
    /// This method is useful when you need to operate on a consecutive range of
    /// logical positions that might span multiple blocks.
    ///
    /// # Arguments
    ///
    /// * `pos_range` - The logical position range to search for within the block map.
    ///
    /// # Returns
    ///
    /// A `Result` containing a vector of `BlockDescriptor`s describing the blocks
    /// that contain the logical position range, ordered by their position in the range.
    /// Returns an error if the range is invalid.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - The start position is outside the range of the block map
    /// - A segment containing part of the range couldn't be read
    pub fn find_blocks(&self, pos_range: Range<u64>) -> Result<Vec<BlockDescriptor>> {
        let start = self.find_block_ordinal(pos_range.start)?;
        let end = if pos_range.is_empty() {
            start + 1
        } else {
            self.find_block_ordinal(pos_range.end - 1)? + 1
        };
        (start..end).map(|i| self.get_block(i)).collect()
    }

    /// Expands the encoded block map into a `BlockList`.
    ///
    /// This method decodes the entire block map and produces a flat representation
    /// as a `BlockList`, which contains vectors of logical and storage offsets.
    ///
    /// # Returns
    ///
    /// A `Result` containing the expanded `BlockList`, or an error if the block map
    /// couldn't be expanded.
    ///
    /// # Performance
    ///
    /// This operation requires loading all segments from the block map, which may
    /// involve significant I/O for large block maps. Consider using `preload()` first
    /// if you plan to expand a large block map.
    pub fn expand(&self) -> Result<BlockList> {
        if self.block_count() == 0 {
            return Ok(BlockList::new(vec![0], vec![0]));
        }
        let mut logical_offsets = Vec::<u64>::with_capacity(self.block_count() as usize + 1);
        let mut block_offsets = Vec::<u64>::with_capacity(self.block_count() as usize + 1);
        for segment_idx in 0..self.footer.segment_count as usize {
            // When expanding the entire block map, there is no need to memoize the segments,
            // thus read them directly.
            let segment = self.read_segment(segment_idx)?;
            segment.expand_logical_offsets(&mut logical_offsets);
            segment.expand_block_offsets(&mut block_offsets);
        }
        verify_data!(
            logical_offsets,
            logical_offsets.len() as u64 == self.block_count() + 1
        );
        verify_data!(
            logical_offsets,
            *logical_offsets.last().unwrap() == self.value_count()
        );
        Ok(BlockList::new(logical_offsets, block_offsets))
    }
}

impl BlockMapDecoder {
    /// Reads the footer of the block map from the provided reader.
    ///
    /// The footer contains essential metadata about the block map, including:
    /// - The total number of values
    /// - The total number of blocks
    /// - The number of segments
    /// - The size of each segment
    ///
    /// # Arguments
    ///
    /// * `reader` - The reader providing access to the block map data.
    ///
    /// # Returns
    ///
    /// A `Result` containing the parsed `Footer`, or an error if the footer
    /// couldn't be read or is invalid.
    fn read_footer(reader: &SlicedReadAt<Arc<dyn ReadAt>>) -> Result<Footer> {
        verify_arg!(reader, reader.slice_size() >= Footer::SIZE);
        let end = reader.slice_size();
        let start = end - Footer::SIZE;
        let bytes = reader.read_at(start..end)?;
        let value_count = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let block_count = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let segment_count = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let segment_size = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        let data_size = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
        verify_data!(value_count, value_count >= block_count);
        verify_data!(block_count, block_count >= segment_count);
        verify_data!(
            segment_count,
            segment_count.saturating_mul(segment_size) >= block_count
        );
        Ok(Footer {
            value_count,
            block_count,
            segment_count,
            segment_size,
            data_size,
        })
    }

    /// Reads the segment list from the provided reader, using the footer information.
    ///
    /// The segment list contains offsets to individual segments within the block map,
    /// allowing the decoder to locate and load segments on demand.
    ///
    /// # Arguments
    ///
    /// * `reader` - The reader providing access to the block map data.
    /// * `footer` - The footer containing metadata about the block map.
    ///
    /// # Returns
    ///
    /// A `Result` containing the parsed `SegmentList`, or an error if the list
    /// couldn't be read or is invalid.
    fn read_segment_list(
        reader: &SlicedReadAt<Arc<dyn ReadAt>>,
        footer: &Footer,
    ) -> Result<SegmentList> {
        let start = footer.get_segment_list_offset(reader.slice_size())?;
        let end = start + 16 * (footer.segment_count + 1);
        let bytes = reader.read_at(start..end)?;
        verify_data!(bytes, bytes.is_aligned(8));
        let logical_offsets = bytes.slice(0..(footer.segment_count as usize + 1) * 8);
        let record_offsets = bytes.slice((footer.segment_count as usize + 1) * 8..);
        let segment_list = SegmentList {
            logical_offsets,
            record_offsets,
        };
        segment_list.verify()?;
        Ok(segment_list)
    }

    /// Fetches a segment from the memoized segment cache, loading it if necessary.
    ///
    /// This function retrieves the segment at the specified index from an internal cache.
    /// If the segment is not yet loaded, it will read it from storage using `read_segment`.
    ///
    /// # Arguments
    ///
    /// * `segment_index` - The index of the segment to fetch.
    ///
    /// # Returns
    ///
    /// A result containing a reference to the requested `BlockMapSegment`, or an error
    /// if the segment could not be read.
    ///
    /// # Performance
    ///
    /// This method implements lazy loading and caching of segments to minimize I/O operations.
    /// Once a segment is loaded, subsequent accesses to the same segment will not require
    /// additional I/O.
    fn fetch_segment(&self, segment_index: usize) -> Result<&BlockMapSegment> {
        assert!(segment_index < self.footer.segment_count as usize);
        self.segment_memo
            .get_or_init(|| BlockMapSegmentMemo::new(self.footer.segment_count as usize))
            .get(segment_index, |i| self.read_segment(i))
    }

    /// Reads a specific segment from the block map.
    ///
    /// This function reads and parses a `BlockMapSegment` from the underlying storage
    /// at the specified segment index. The segment boundaries are determined from the
    /// segment list's record offsets.
    ///
    /// # Arguments
    ///
    /// * `segment_index` - The index of the segment to read.
    ///
    /// # Returns
    ///
    /// A result containing the parsed `BlockMapSegment`, or an error if the segment
    /// couldn't be read or parsed properly.
    ///
    /// # Note
    ///
    /// This is a low-level function that performs I/O and parsing. For cached access
    /// to segments, use `fetch_segment` instead.
    fn read_segment(&self, segment_index: usize) -> Result<BlockMapSegment> {
        let offsets = self.segment_list.record_offsets();
        let start = offsets[segment_index];
        let end = offsets[segment_index + 1];
        let bytes = self.reader.read_at(start..end)?;
        BlockMapSegment::parse(segment_index as u64 * self.footer.segment_size, bytes)
    }
}

impl BlockMapOps for BlockMapDecoder {
    fn value_count(&self) -> u64 {
        self.footer.value_count
    }

    fn block_count(&self) -> u64 {
        self.footer.block_count
    }

    fn storage_size(&self) -> u64 {
        self.footer.data_size
    }

    fn find_block_ordinal(&self, logical_pos: u64) -> Result<usize> {
        self.find_block(logical_pos)
            .map(|desc| desc.ordinal as usize)
    }

    fn get_block(&self, block_ordinal: usize) -> Result<BlockDescriptor> {
        let block_ordinal = block_ordinal as u64;
        verify_arg!(block_ordinal, block_ordinal < self.footer.block_count);
        let segment_idx = (block_ordinal / self.footer.segment_size) as usize;
        let segment = self.fetch_segment(segment_idx)?;
        let block = segment.get_block(block_ordinal);
        Ok(block)
    }

    fn find_block(&self, logical_pos: u64) -> Result<BlockDescriptor> {
        let segment_idx = self.segment_list.find_segment(logical_pos).ok_or_else(|| {
            Error::invalid_arg(
                "logical_pos",
                format!("failed to find segment for logical pos {logical_pos}"),
            )
        })?;
        let segment = self.fetch_segment(segment_idx)?;
        segment.find_block(logical_pos).ok_or_else(|| {
            Error::invalid_arg(
                "logical_pos",
                format!("failed to find segment for logical pos {logical_pos}"),
            )
        })
    }
}

/// Contains metadata about the block map.
#[derive(Debug)]
struct Footer {
    /// Total count of logical values
    value_count: u64,
    /// Total count of blocks
    block_count: u64,
    /// Number of segments
    segment_count: u64,
    /// Number of blocks per segment (except the last segment).
    segment_size: u64,
    /// Encoded data size: sum of all encoded block sizes.
    data_size: u64,
}

impl Footer {
    const SIZE: u64 = 40;

    /// Calculates the offset of the segment list within the block map.
    fn get_segment_list_offset(&self, block_map_size: u64) -> Result<u64> {
        let list_backoff = Self::SIZE + (self.segment_count + 1) * 16;
        verify_data!(list_backoff, list_backoff <= block_map_size);
        Ok(block_map_size - list_backoff)
    }
}

/// A list of segments within the block map, containing logical offsets of the segments
/// and the segment locations.
struct SegmentList {
    logical_offsets: Bytes,
    record_offsets: Bytes,
}

impl SegmentList {
    /// Finds the segment index containing the given logical position.
    fn find_segment(&self, logical_pos: u64) -> Option<usize> {
        let logical_offsets = self.logical_offsets();
        if logical_pos >= *logical_offsets.last().unwrap() {
            return None;
        }
        let i = logical_offsets
            .binary_search(&logical_pos)
            .unwrap_or_else(|i| i - 1);
        Some(i)
    }

    /// Verifies the integrity of the segment list.
    fn verify(&self) -> Result<()> {
        let logical_offsets = self.logical_offsets();
        verify_data!(logical_offsets, !logical_offsets.is_empty());
        verify_data!(logical_offsets, logical_offsets[0] == 0);
        for (&next, &prev) in logical_offsets.iter().skip(1).zip(logical_offsets) {
            if prev >= next {
                return Err(Error::invalid_format(format!(
                    "SegmentList::verify: logical offsets: {prev} < {next}"
                )));
            }
        }
        let record_offsets = self.record_offsets();
        verify_data!(record_offsets, !record_offsets.is_empty());
        for (&next, &prev) in record_offsets.iter().skip(1).zip(record_offsets) {
            if prev >= next {
                return Err(Error::invalid_format(format!(
                    "SegmentList::verify: record offsets: {prev} < {next}"
                )));
            }
        }
        Ok(())
    }

    /// Returns a slice of logical offsets for each segment.
    fn logical_offsets(&self) -> &[u64] {
        bytemuck::cast_slice(&self.logical_offsets)
    }

    /// Returns a slice of segment record offsets for each segment.
    fn record_offsets(&self) -> &[u64] {
        bytemuck::cast_slice(&self.record_offsets)
    }
}

struct BlockMapSegmentMemo(Vec<OnceLock<BlockMapSegment>>);

impl BlockMapSegmentMemo {
    fn new(segment_count: usize) -> BlockMapSegmentMemo {
        let mut segments = Vec::with_capacity(segment_count);
        segments.resize_with(segment_count, Default::default);
        BlockMapSegmentMemo(segments)
    }

    fn get(
        &self,
        segment_index: usize,
        read_fn: impl FnOnce(usize) -> Result<BlockMapSegment>,
    ) -> Result<&BlockMapSegment> {
        // TODO: refactor to get_or_try_init() once it's stable.

        if let Some(segment) = self.0[segment_index].get() {
            return Ok(segment);
        }
        let segment = read_fn(segment_index)?;
        let _ = self.0[segment_index].set(segment);
        Ok(self.0[segment_index].get().expect("segment"))
    }
}

/// Represents a segment within the block map, containing metadata and packed
/// block size information.
struct BlockMapSegment {
    block_ordinal: u64,
    logical_pos: u64,
    block_offset: u64,
    min_logical_size: u32,
    min_block_size: u32,
    block_count: usize,
    logical_size_bits: u8,
    block_size_bits: u8,
    packed_logical_sizes: Bytes,
    packed_block_sizes: Bytes,
    logical_offsets: OnceLock<Vec<u64>>,
    block_offsets: OnceLock<Vec<u64>>,
}

impl BlockMapSegment {
    /// Parses a `BlockMapSegment` from a byte slice.
    fn parse(block_ordinal: u64, bytes: Bytes) -> Result<BlockMapSegment> {
        verify_arg!(bytes, bytes.len() >= 28);
        let logical_pos = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let block_offset = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let min_logical_size = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
        let min_block_size = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
        let logical_size_bits = bytes[24];
        verify_data!(
            logical_size_bits,
            Self::is_valid_pack_bits(logical_size_bits)
        );
        let block_size_bits = bytes[25];
        verify_data!(block_size_bits, Self::is_valid_pack_bits(block_size_bits));
        let block_count = u16::from_le_bytes(bytes[26..28].try_into().unwrap()) as usize;

        let logical_sizes_len = block_count * logical_size_bits as usize / 8;
        let logical_sizes_padded_len = (logical_sizes_len + 3) & !3;
        let block_sizes_len = block_count * block_size_bits as usize / 8;

        verify_data!(logical_sizes_len, bytes.len() >= 28 + logical_sizes_len);
        verify_data!(
            block_sizes_len,
            bytes.len() >= 28 + logical_sizes_padded_len + block_sizes_len
        );

        let packed_logical_sizes = bytes.slice(28..28 + logical_sizes_len);
        verify_data!(
            packed_logical_sizes,
            packed_logical_sizes.is_aligned(logical_size_bits as usize / 8)
        );

        let packed_block_sizes = bytes
            .slice(28 + logical_sizes_padded_len..28 + logical_sizes_padded_len + block_sizes_len);
        verify_data!(
            packed_block_sizes,
            packed_block_sizes.is_aligned(block_size_bits as usize / 8)
        );

        Ok(BlockMapSegment {
            block_ordinal,
            logical_pos,
            block_offset,
            min_logical_size,
            min_block_size,
            block_count,
            logical_size_bits,
            block_size_bits,
            packed_logical_sizes,
            packed_block_sizes,
            logical_offsets: Default::default(),
            block_offsets: Default::default(),
        })
    }

    /// Finds the block within the segment containing the given logical position.
    fn find_block(&self, logical_pos: u64) -> Option<BlockDescriptor> {
        let logical_offsets = self.logical_offsets();
        assert!(logical_offsets.len() >= 2);
        if logical_pos < logical_offsets[0] || logical_pos >= *logical_offsets.last().unwrap() {
            return None;
        }

        let i = logical_offsets
            .binary_search(&logical_pos)
            .unwrap_or_else(|i| i - 1);

        let logical_range = logical_offsets[i]..logical_offsets[i + 1];
        let block_offsets = self.block_offsets();
        let storage_range = block_offsets[i]..block_offsets[i + 1];
        Some(BlockDescriptor {
            ordinal: self.block_ordinal + i as u64,
            logical_range,
            storage_range,
        })
    }

    /// Gets the block descriptor for a specific block ordinal within the segment.
    fn get_block(&self, block_ordinal: u64) -> BlockDescriptor {
        assert!(block_ordinal >= self.start_block_ordinal());
        assert!(block_ordinal < self.end_block_ordinal());
        let logical_offsets = self.logical_offsets();
        let block_offsets = self.block_offsets();
        let i = (block_ordinal - self.block_ordinal) as usize;
        let logical_range = logical_offsets[i]..logical_offsets[i + 1];
        let storage_range = block_offsets[i]..block_offsets[i + 1];
        BlockDescriptor {
            ordinal: block_ordinal,
            logical_range,
            storage_range,
        }
    }

    /// Returns the starting block ordinal for this segment.
    fn start_block_ordinal(&self) -> u64 {
        self.block_ordinal
    }

    /// Returns the ending block ordinal for this segment.
    fn end_block_ordinal(&self) -> u64 {
        self.block_ordinal + self.block_count as u64
    }

    /// Returns a slice of logical offsets for each block in the segment.
    ///
    /// The slice length is `block_count + 1`.
    fn logical_offsets(&self) -> &[u64] {
        self.logical_offsets
            .get_or_init(|| self.expand_logical_offsets_to_vec())
    }

    /// Returns a slice of block offsets for each block in the segment.
    ///
    /// The slice length is `block_count + 1`.
    fn block_offsets(&self) -> &[u64] {
        self.block_offsets
            .get_or_init(|| self.expand_block_offsets_to_vec())
    }

    /// Expands the packed logical offsets into a vector of `u64` values with
    /// the length `block_count + 1`.
    fn expand_logical_offsets_to_vec(&self) -> Vec<u64> {
        let mut res = Vec::<u64>::with_capacity(self.block_count + 1);
        self.expand_logical_offsets(&mut res);
        res
    }

    /// Expands the packed block offsets into a vector of `u64` values with
    /// the length `block_count + 1`.
    fn expand_block_offsets_to_vec(&self) -> Vec<u64> {
        let mut res = Vec::<u64>::with_capacity(self.block_count + 1);
        self.expand_block_offsets(&mut res);
        res
    }

    /// Expands the packed logical offsets into the provided output vector.
    fn expand_logical_offsets(&self, output: &mut Vec<u64>) {
        unpack_to_vec(
            &self.packed_logical_sizes,
            self.logical_size_bits,
            self.logical_pos,
            self.min_logical_size,
            output,
        );
    }

    /// Expands the packed block offsets into the provided output vector.
    fn expand_block_offsets(&self, output: &mut Vec<u64>) {
        unpack_to_vec(
            &self.packed_block_sizes,
            self.block_size_bits,
            self.block_offset,
            self.min_block_size,
            output,
        );
    }

    fn is_valid_pack_bits(bits: u8) -> bool {
        bits == 8 || bits == 16 || bits == 32
    }
}

/// Unpacks a packed array of offsets into a vector of u64 values.
fn unpack_to_vec(src: &[u8], bits: u8, first: u64, min: u32, dst: &mut Vec<u64>) {
    let n = src.len() / (bits as usize / 8);
    let from = if dst.is_empty() {
        dst.reserve(n + 1);
        dst.push(first);
        1
    } else {
        assert_eq!(*dst.last().unwrap(), first);
        dst.len()
    };
    dst.resize(from + n, 0);
    unpack(src, bits, first, min, &mut dst[from..]);
}

/// Unpacks a packed array of offsets into a slice of u64 values.
fn unpack(src: &[u8], bits: u8, first: u64, min: u32, dst: &mut [u64]) {
    match bits {
        8 => {
            let mut acc = first;
            for (&s, t) in src.iter().zip(dst.iter_mut()) {
                acc += (s as u32 + min) as u64;
                *t = acc;
            }
        }
        16 => {
            let mut acc = first;
            for (&s, t) in bytemuck::cast_slice::<_, u16>(src)
                .iter()
                .zip(dst.iter_mut())
            {
                acc += (s as u32 + min) as u64;
                *t = acc;
            }
        }
        32 => {
            let mut acc = first;
            for (&s, t) in bytemuck::cast_slice::<_, u32>(src)
                .iter()
                .zip(dst.iter_mut())
            {
                acc += (s + min) as u64;
                *t = acc;
            }
        }
        _ => panic!("invalid pack bits {bits}"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use amudai_bytes::Bytes;
    use amudai_common::Result;
    use amudai_io::{ReadAt, sliced_read::SlicedReadAt};

    use crate::{read::block_map_ops::BlockMapOps, write::block_map_encoder::BlockMapEncoder};

    use super::BlockMapDecoder;

    #[test]
    fn test_decoder_basics() {
        let mut encoder = BlockMapEncoder::new();
        for i in 0..100000 {
            encoder.add_block(1000 + (i % 20), 500 + (i % 2000));
        }
        let encoded = encoder.finish();
        let len = encoded.len();
        let reader = Arc::new(encoded) as Arc<dyn ReadAt>;
        let decoder = BlockMapDecoder::new(SlicedReadAt::new(reader, 0..len as u64)).unwrap();

        let s0 = decoder.read_segment(0).unwrap();
        assert_eq!(s0.block_count, 1024);
        let s1 = decoder.read_segment(97).unwrap();
        assert_eq!(s1.block_count, 672);
        let offsets = s1.expand_logical_offsets_to_vec();
        assert_eq!(offsets.len(), 673);

        let block = decoder.find_block(90456123).unwrap();
        assert!(block.logical_range.contains(&90456123));
        let blocks = decoder.find_blocks(90456123..90460000).unwrap();
        assert_eq!(blocks.len(), 5);
        assert!(blocks[0].logical_range.contains(&90456123));
        assert!(blocks[4].logical_range.contains(&90459999));

        let decoder = decoder.preload().unwrap();
        let blocks = decoder.find_blocks(90456123..90460000).unwrap();
        assert_eq!(blocks.len(), 5);
        assert!(blocks[0].logical_range.contains(&90456123));
        assert!(blocks[4].logical_range.contains(&90459999));
    }

    #[test]
    fn test_decoder_expand() {
        let mut encoder = BlockMapEncoder::new();
        for i in 0..100000 {
            encoder.add_block(1000 + (i % 20), 500 + (i % 2000));
        }
        let encoded = encoder.finish();
        let len = encoded.len();
        let reader = Arc::new(encoded) as Arc<dyn ReadAt>;
        let decoder = BlockMapDecoder::new(SlicedReadAt::new(reader, 0..len as u64)).unwrap();

        let flat = decoder.expand().unwrap();
        flat.verify().unwrap();
        assert_eq!(flat.logical_offsets.len(), 100001);
    }

    fn create_decoder_from_encoder(encoder: BlockMapEncoder) -> Result<BlockMapDecoder> {
        let encoded = encoder.finish();
        let len = encoded.len();
        let reader = Arc::new(encoded) as Arc<dyn ReadAt>;
        BlockMapDecoder::new(SlicedReadAt::new(reader, 0..len as u64))
    }

    fn create_decoder_from_bytes(bytes: Bytes) -> Result<BlockMapDecoder> {
        let len = bytes.len();
        let reader = Arc::new(bytes) as Arc<dyn ReadAt>;
        BlockMapDecoder::new(SlicedReadAt::new(reader, 0..len as u64))
    }

    #[test]
    fn test_empty_block_map() {
        let encoder = BlockMapEncoder::new();
        let decoder_result = create_decoder_from_encoder(encoder);
        assert!(decoder_result.is_ok());
        let decoder = decoder_result.unwrap();
        assert_eq!(decoder.value_count(), 0);
        assert_eq!(decoder.block_count(), 0);
    }

    #[test]
    fn test_single_block_map() {
        let mut encoder = BlockMapEncoder::new();
        encoder.add_block(100, 200);
        let decoder = create_decoder_from_encoder(encoder).unwrap();
        assert_eq!(decoder.value_count(), 100);
        assert_eq!(decoder.block_count(), 1);

        let block = decoder.find_block(50).unwrap();
        assert_eq!(block.logical_range, 0..100);
        assert_eq!(block.storage_range, 0..200);
        let block = decoder.find_block(0).unwrap();
        assert_eq!(block.logical_range, 0..100);
        assert_eq!(block.storage_range, 0..200);
    }

    #[test]
    fn test_multiple_segments() {
        let mut encoder = BlockMapEncoder::new();
        for _ in 0..3000 {
            // Enough blocks to create multiple segments
            encoder.add_block(100, 200);
        }
        let decoder = create_decoder_from_encoder(encoder).unwrap();
        assert_eq!(decoder.block_count(), 3000);
        assert!(decoder.footer.segment_count > 1);

        let block = decoder.find_block(150000).unwrap();
        assert!(block.logical_range.contains(&150000));
        let blocks = decoder.find_blocks(100000..200000).unwrap();
        assert!(!blocks.is_empty());
    }

    #[test]
    fn test_find_block_boundaries() {
        let mut encoder = BlockMapEncoder::new();
        encoder.add_block(100, 200);
        encoder.add_block(150, 250);
        let decoder = create_decoder_from_encoder(encoder).unwrap();

        let block1_start = decoder.find_block(0).unwrap();
        assert_eq!(block1_start.logical_range, 0..100);
        let block1_end = decoder.find_block(99).unwrap();
        assert_eq!(block1_end, block1_start);

        let block2_start = decoder.find_block(100).unwrap();
        assert_eq!(block2_start.logical_range, 100..250);
        let block2_end = decoder.find_block(249).unwrap();
        assert_eq!(block2_end, block2_start);
    }

    #[test]
    fn test_find_block_invalid_pos() {
        let mut encoder = BlockMapEncoder::new();
        encoder.add_block(100, 200);
        let decoder = create_decoder_from_encoder(encoder).unwrap();

        let res = decoder.find_block(u64::MAX);
        assert!(res.is_err());
        let res = decoder.find_block(100);
        assert!(res.is_err());
        let res = decoder.find_block(0);
        assert!(res.is_ok());
        let res = decoder.find_block(50).unwrap();
        assert_eq!(res.ordinal, 0);
        assert_eq!(res.logical_range, 0..100);
        assert_eq!(res.storage_range, 0..200);
    }

    #[test]
    fn test_find_blocks_single_block_range() {
        let mut encoder = BlockMapEncoder::new();
        encoder.add_block(100, 200);
        encoder.add_block(150, 250);
        let decoder = create_decoder_from_encoder(encoder).unwrap();

        let blocks = decoder.find_blocks(50..70).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].logical_range, 0..100);

        let blocks = decoder.find_blocks(120..140).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].logical_range, 100..250);
    }

    #[test]
    fn test_find_blocks_multi_block_range() {
        let mut encoder = BlockMapEncoder::new();
        encoder.add_block(100, 200);
        encoder.add_block(150, 250);
        encoder.add_block(200, 300);
        let decoder = create_decoder_from_encoder(encoder).unwrap();

        let blocks = decoder.find_blocks(50..120).unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].logical_range, 0..100);
        assert_eq!(blocks[1].logical_range, 100..250);

        let blocks = decoder.find_blocks(80..220).unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].logical_range, 0..100);
        assert_eq!(blocks[1].logical_range, 100..250); // Corrected expected range to 100..250, not 100..200
    }

    #[test]
    fn test_find_blocks_range_within_block() {
        let mut encoder = BlockMapEncoder::new();
        encoder.add_block(100, 200);
        let decoder = create_decoder_from_encoder(encoder).unwrap();

        let blocks = decoder.find_blocks(20..80).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].logical_range, 0..100);
    }

    #[test]
    fn test_find_blocks_empty_range() {
        let mut encoder = BlockMapEncoder::new();
        encoder.add_block(100, 200);
        let decoder = create_decoder_from_encoder(encoder).unwrap();

        let blocks = decoder.find_blocks(50..50).unwrap();
        assert_eq!(blocks.len(), 1); // Should still return the block containing the start position
        assert_eq!(blocks[0].logical_range, 0..100);
    }

    #[test]
    fn test_find_blocks_invalid_range() {
        let mut encoder = BlockMapEncoder::new();
        encoder.add_block(100, 200);
        let decoder = create_decoder_from_encoder(encoder).unwrap();

        let blocks_invalid_start = decoder.find_blocks(150..200); // Start position out of range
        assert!(blocks_invalid_start.is_err());
    }

    #[test]
    fn test_preload() {
        let mut encoder = BlockMapEncoder::new();
        for i in 0..1000 {
            encoder.add_block(100 + (i % 20), 50 + (i % 200));
        }
        let decoder = create_decoder_from_encoder(encoder).unwrap();
        let decoder_preloaded = decoder.preload().unwrap();

        assert_eq!(decoder_preloaded.value_count(), decoder.value_count());
        assert_eq!(decoder_preloaded.block_count(), decoder.block_count());

        let block1 = decoder.find_block(50000).unwrap();
        let block2 = decoder_preloaded.find_block(50000).unwrap();
        assert_eq!(block1, block2);
    }

    #[test]
    fn test_expand_empty_blockmap() {
        let encoder = BlockMapEncoder::new();
        let decoder = create_decoder_from_encoder(encoder).unwrap();
        let flat_map = decoder.expand().unwrap();
        flat_map.verify().unwrap();
        assert_eq!(flat_map.logical_offsets.len(), 1); // Always at least one offset (0)
        assert_eq!(flat_map.storage_offsets.len(), 1);
    }

    #[test]
    fn test_expand_single_block() {
        let mut encoder = BlockMapEncoder::new();
        encoder.add_block(100, 200);
        let decoder = create_decoder_from_encoder(encoder).unwrap();
        let flat_map = decoder.expand().unwrap();
        flat_map.verify().unwrap();
        assert_eq!(flat_map.logical_offsets.len(), 2);
        assert_eq!(flat_map.storage_offsets.len(), 2);
        assert_eq!(flat_map.logical_offsets, vec![0, 100]);
        assert_eq!(flat_map.storage_offsets, vec![0, 200]);
    }

    #[test]
    fn test_expand_multiple_blocks() {
        let mut encoder = BlockMapEncoder::new();
        encoder.add_block(100, 200);
        encoder.add_block(150, 250);
        encoder.add_block(200, 300);
        let decoder = create_decoder_from_encoder(encoder).unwrap();
        let flat_map = decoder.expand().unwrap();
        flat_map.verify().unwrap();
        assert_eq!(flat_map.logical_offsets.len(), 4);
        assert_eq!(flat_map.storage_offsets.len(), 4);
        assert_eq!(flat_map.logical_offsets, vec![0, 100, 250, 450]);
        assert_eq!(flat_map.storage_offsets, vec![0, 200, 450, 750]);
    }

    #[test]
    fn test_decoder_invalid_data_truncated_footer() {
        let mut encoder = BlockMapEncoder::new();
        encoder.add_block(100, 200);
        let encoded = Bytes::from(encoder.finish());
        let truncated_encoded = encoded.slice(0..encoded.len() - 10); // Truncate footer
        let decoder_result = create_decoder_from_bytes(truncated_encoded);
        assert!(decoder_result.is_err());
    }

    #[test]
    fn test_decoder_invalid_data_truncated_segment_list() {
        let mut encoder = BlockMapEncoder::new();
        for _ in 0..2 {
            // Create at least one segment list entry
            encoder.add_block(100, 200);
        }
        let encoded = Bytes::from(encoder.finish());
        let footer_size = 32;
        let segment_list_entry_size = 16;
        let segment_list_size = 2 * segment_list_entry_size;
        let truncation_size = footer_size + segment_list_size - 10; // Truncate segment list
        let truncated_encoded = encoded.slice(0..encoded.len() - truncation_size as usize);
        let decoder_result = create_decoder_from_bytes(truncated_encoded);
        assert!(decoder_result.is_err());
    }

    #[test]
    fn test_get_block() {
        let mut encoder = BlockMapEncoder::new();
        encoder.add_block(100, 200);
        encoder.add_block(150, 250);
        encoder.add_block(200, 300);
        let decoder = create_decoder_from_encoder(encoder).unwrap();

        // Get first block by ordinal
        let block0 = decoder.get_block(0).unwrap();
        assert_eq!(block0.ordinal, 0);
        assert_eq!(block0.logical_range, 0..100);
        assert_eq!(block0.storage_range, 0..200);

        // Get second block by ordinal
        let block1 = decoder.get_block(1).unwrap();
        assert_eq!(block1.ordinal, 1);
        assert_eq!(block1.logical_range, 100..250);
        assert_eq!(block1.storage_range, 200..450);

        // Get third block by ordinal
        let block2 = decoder.get_block(2).unwrap();
        assert_eq!(block2.ordinal, 2);
        assert_eq!(block2.logical_range, 250..450);
        assert_eq!(block2.storage_range, 450..750);

        // Test against block found through find_block
        let found_block = decoder.find_block(150).unwrap();
        let retrieved_block = decoder.get_block(found_block.ordinal as usize).unwrap();
        assert_eq!(found_block, retrieved_block);
    }

    #[test]
    fn test_get_block_invalid_ordinal() {
        let mut encoder = BlockMapEncoder::new();
        encoder.add_block(100, 200);
        let decoder = create_decoder_from_encoder(encoder).unwrap();

        // Valid block
        let result = decoder.get_block(0);
        assert!(result.is_ok());

        // Invalid block - ordinal beyond block count
        let result = decoder.get_block(1);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_block_across_segments() {
        let mut encoder = BlockMapEncoder::new();
        // Add enough blocks to create multiple segments
        for _ in 0..3000 {
            encoder.add_block(100, 200);
        }
        let decoder = create_decoder_from_encoder(encoder).unwrap();

        // Get block from first segment
        let block0 = decoder.get_block(0).unwrap();
        assert_eq!(block0.ordinal, 0);

        // Get block from middle segment
        let middle_ordinal = 1500u64;
        let block_middle = decoder.get_block(middle_ordinal as usize).unwrap();
        assert_eq!(block_middle.ordinal, middle_ordinal);

        // Get block from last segment
        let last_ordinal = 2999u64;
        let block_last = decoder.get_block(last_ordinal as usize).unwrap();
        assert_eq!(block_last.ordinal, last_ordinal);
    }

    #[test]
    fn test_block_range_to_storage_range() {
        let mut encoder = BlockMapEncoder::new();
        encoder.add_block(100, 200); // Block 0: logical 0..100, storage 0..200
        encoder.add_block(150, 250); // Block 1: logical 100..250, storage 200..450
        encoder.add_block(200, 300); // Block 2: logical 250..450, storage 450..750
        let decoder = create_decoder_from_encoder(encoder).unwrap();

        // Test with a range containing a single block
        let storage_range = decoder.block_range_to_storage_range(0..1).unwrap();
        assert_eq!(storage_range, 0..200);

        // Test with a range containing multiple blocks
        let storage_range = decoder.block_range_to_storage_range(0..3).unwrap();
        assert_eq!(storage_range, 0..750);

        // Test with a non-zero starting block
        let storage_range = decoder.block_range_to_storage_range(1..3).unwrap();
        assert_eq!(storage_range, 200..750);

        // Test with a middle subset of blocks
        let storage_range = decoder.block_range_to_storage_range(1..2).unwrap();
        assert_eq!(storage_range, 200..450);

        // Test with an empty range
        let storage_range = decoder.block_range_to_storage_range(0..0).unwrap();
        assert_eq!(storage_range, 0..0);

        // Test with an invalid range (end index too large)
        let result = decoder.block_range_to_storage_range(0..4);
        assert!(result.is_err());

        // Test with an invalid range (start index too large)
        let result = decoder.block_range_to_storage_range(3..4);
        assert!(result.is_err());
    }
}
