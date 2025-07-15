//! Segmented vector collection for parallel processing and storage.
//!
//! This module provides [`SegmentedVec<C>`], a specialized collection that organizes
//! data into uniformly-sized contiguous segments to enable parallel operations and
//! optimized storage dumping/loading.

use std::sync::Arc;

use amudai_bytes::align::is_aligned_u64;
use amudai_io::{SlicedFile, WriteAt, verify};

use crate::{
    Collection, Collector, Dump, Load,
    manifest::{Manifest, PAGE_SIZE},
};

/// An appendable collection with indexed access built on top of uniformly-sized contiguous segments.
///
/// `SegmentedVec<C>` provides a Vec-like interface while organizing data into fixed-size segments
/// that can be processed independently and in parallel. This design is particularly beneficial for:
///
/// - **Parallel processing**: Each segment can be operated on independently
/// - **Efficient storage operations**: Segments can be dumped and loaded in parallel
/// - **Memory management**: Large collections can be processed without requiring
///   contiguous memory allocation
/// - **Append-heavy workloads**: New segments are allocated only when needed
///
/// # Segment Size Requirements
///
/// All segment sizes must be:
/// - Greater than 1
/// - A power of two (for efficient bit-shift operations)
///
/// The default segment size is 64K elements.
///
/// # Type Parameter
///
/// - `C`: The collection type used for individual segments. Must implement appropriate
///   traits like [`Collection`], [`Collector`], [`Dump`], and/or [`Load`] depending
///   on the operations needed.
///
/// # Performance Characteristics
///
/// - **Push**: O(1) amortized, with occasional O(segment_size) when allocating new segments
/// - **Index access**: O(1)
/// - **Parallel processing**: Each segment can be processed independently
/// - **Memory usage**: More efficient than Vec for very large collections
pub struct SegmentedVec<C> {
    /// The individual segments that make up the collection
    segments: Vec<C>,
    /// The number of items per segment (must be power of two). Uniform for all segments
    /// except the last one.
    segment_size: usize,
    /// Number of bits to shift for fast division by segment_size
    offset_bits: usize,
    /// Total number of elements across all segments
    len: usize,
}

impl<C> SegmentedVec<C> {
    /// Default segment size: 64K items.
    pub const DEFAULT_SEGMENT_SIZE: usize = 64 * 1024;

    /// Creates a new, empty `SegmentedVec` with the default segment size.
    ///
    /// This is equivalent to calling `with_segment_size(DEFAULT_SEGMENT_SIZE)`.
    pub fn new() -> SegmentedVec<C> {
        Self::with_segment_size(Self::DEFAULT_SEGMENT_SIZE)
    }

    /// Creates a new, empty `SegmentedVec` with the specified segment size.
    ///
    /// The segment size must be greater than 1 and a power of two for efficient
    /// bit-shift operations when mapping indices to segments.
    ///
    /// # Parameters
    ///
    /// * `segment_size` - Number of elements per segment (must be > 1 and power of two)
    pub fn with_segment_size(segment_size: usize) -> SegmentedVec<C> {
        assert!(segment_size > 1);
        assert!(segment_size.is_power_of_two());
        let offset_bits = segment_size.trailing_zeros() as usize;
        SegmentedVec {
            segments: Vec::new(),
            segment_size,
            offset_bits,
            len: 0,
        }
    }

    /// Returns the total number of elements in the segmented vector.
    ///
    /// This is the sum of elements across all segments.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the segmented vector contains no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Removes all elements from the segmented vector.
    ///
    /// This clears all segments and resets the length to zero, but does not
    /// affect the configured segment size.
    pub fn clear(&mut self) {
        self.segments.clear();
        self.len = 0;
    }

    /// Returns the number of elements per segment.
    ///
    /// This value is fixed when the `SegmentedVec` is created and cannot be changed.
    #[inline]
    pub fn segment_size(&self) -> usize {
        self.segment_size
    }

    /// Returns a slice of all segments in the vector.
    ///
    /// This provides access to the underlying segments for parallel processing
    /// or direct manipulation.
    #[inline]
    pub fn segments(&self) -> &[C] {
        &self.segments
    }

    /// Maps a linear index to a (segment_id, offset) pair.
    ///
    /// This is used internally for index access using bit-shift operations
    /// instead of division/modulo.
    ///
    /// # Parameters
    ///
    /// * `index` - The linear index into the segmented vector
    ///
    /// # Returns
    ///
    /// A tuple of (segment_index, offset_within_segment)
    #[inline]
    pub fn map_index(&self, index: usize) -> (usize, usize) {
        let sid = index >> self.offset_bits;
        let offset = index & (self.segment_size - 1);
        (sid, offset)
    }

    /// Returns `true` if the last segment is at full capacity.
    ///
    /// This is used internally to determine when a new segment needs to be allocated.
    #[inline]
    fn is_last_segment_full(&self) -> bool {
        (self.len & (self.segment_size - 1)) == 0
    }
}

impl<C> SegmentedVec<C>
where
    C: Collection,
{
    /// Creates a `SegmentedVec` from a vector of existing segments.
    ///
    /// All segments except the last must be exactly `segment_size` length.
    /// The last segment may be partially filled (length ≤ `segment_size`).
    ///
    /// # Parameters
    ///
    /// * `segments` - Vector of collection segments
    /// * `segment_size` - Size of each segment (must be > 1 and power of two)
    ///
    /// # Returns
    ///
    /// A `SegmentedVec` containing the provided segments, or an error if the
    /// segments don't conform to the size requirements.
    pub fn from_segments(
        segments: Vec<C>,
        segment_size: usize,
    ) -> std::io::Result<SegmentedVec<C>> {
        verify!(segment_size > 1);
        verify!(segment_size.is_power_of_two());

        if segments.is_empty() {
            return Ok(SegmentedVec::with_segment_size(segment_size));
        }

        verify!(
            segments[..segments.len() - 1]
                .iter()
                .all(|segment| segment.len() == segment_size)
        );
        verify!(segments.last().unwrap().len() <= segment_size);

        let len = (segments.len() - 1) * segment_size + segments.last().unwrap().len();
        let offset_bits = segment_size.trailing_zeros() as usize;
        Ok(SegmentedVec {
            segments,
            segment_size,
            offset_bits,
            len,
        })
    }
}

impl<C> TryFrom<Vec<C>> for SegmentedVec<C>
where
    C: Collection,
{
    type Error = std::io::Error;

    /// Converts a vector of collections into a `SegmentedVec`.
    ///
    /// The segment size is inferred from the input:
    /// - If only one collection is provided, the segment size is the maximum of
    ///   the collection's length and `DEFAULT_SEGMENT_SIZE`, rounded up to the next
    ///   power of two
    /// - If multiple collections are provided, the segment size is assumed to be
    ///   the length of the first collection
    ///
    /// # Errors
    ///
    /// Returns an error if the collections don't form valid segments according
    /// to the inferred segment size.
    fn try_from(segments: Vec<C>) -> std::io::Result<SegmentedVec<C>> {
        if segments.is_empty() {
            return Ok(SegmentedVec::new());
        }

        let segment_size = if segments.len() == 1 {
            segments[0]
                .len()
                .max(Self::DEFAULT_SEGMENT_SIZE)
                .next_power_of_two()
        } else {
            segments[0].len()
        };
        SegmentedVec::from_segments(segments, segment_size)
    }
}

impl<C> SegmentedVec<C>
where
    C: Collector,
{
    /// Appends an element to the end of the segmented vector.
    ///
    /// If the current segment is full, a new segment is automatically allocated.
    /// This operation is O(1) amortized, with occasional O(segment_size) cost
    /// when allocating new segments.
    ///
    /// # Parameters
    ///
    /// * `value` - Reference to the item to append
    #[inline]
    pub fn push(&mut self, value: &C::Item) {
        if self.is_last_segment_full() {
            self.append_segment();
        }
        self.segments.last_mut().unwrap().push(value);
        self.len += 1;
    }

    /// Allocates and appends a new segment to the segments vector.
    ///
    /// This is called when the current segment is full and more capacity is needed.
    #[cold]
    fn append_segment(&mut self) {
        self.segments.push(C::with_item_capacity(self.segment_size));
    }
}

impl<C> std::ops::Index<usize> for SegmentedVec<C>
where
    C: std::ops::Index<usize>,
{
    type Output = C::Output;

    /// Provides indexed access to elements in the segmented vector.
    ///
    /// Uses bit-shift operations to map the linear index to the appropriate
    /// segment and offset within that segment.
    ///
    /// # Parameters
    ///
    /// * `index` - The linear index of the element to access
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds, same as standard vector indexing.
    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        let (sid, offset) = self.map_index(index);
        &self.segments[sid][offset]
    }
}

impl<C: Dump> Dump for SegmentedVec<C> {
    /// Computes the total size required to dump this segmented vector.
    ///
    /// This is the sum of the dump sizes of all individual segments.
    fn compute_size(&self) -> u64 {
        self.segments
            .iter()
            .map(|segment| segment.compute_size())
            .sum()
    }

    /// Dumps the segmented vector to external storage.
    ///
    /// Each segment is dumped independently, enabling parallel processing.
    /// The manifest contains metadata about the total length, segment size,
    /// and child manifests for each segment.
    ///
    /// # Future Optimization
    ///
    /// The current implementation dumps segments sequentially, but this could
    /// be parallelized for better performance with large numbers of segments.
    ///
    /// # Parameters
    ///
    /// * `writer` - The sliced file writer for storage operations
    ///
    /// # Returns
    ///
    /// A manifest containing metadata about the dumped segmented vector,
    /// including child manifests for each segment.
    fn dump<W>(
        &self,
        writer: amudai_io::SlicedFile<W>,
    ) -> std::io::Result<crate::manifest::Manifest>
    where
        W: amudai_io::WriteAt + Clone,
    {
        // TODO: parallel
        let mut manifest = Manifest::default();
        let mut pos = 0u64;
        for segment in self.segments.iter() {
            let next_pos = pos + segment.compute_size();
            let child_manifest = segment
                .dump(writer.slice(pos..next_pos)?)?
                .into_nested(pos..next_pos);
            manifest.child_list.push(child_manifest);
            pos = next_pos;
        }
        manifest.put("len", self.len() as u64);
        manifest.put("segment_size", self.segment_size() as u64);
        Ok(manifest)
    }
}

impl<C: Load + Collection> Load for SegmentedVec<C> {
    /// Loads a segmented vector from external storage.
    ///
    /// Reconstructs the segmented vector from a manifest and reader, loading
    /// each segment independently. This enables parallel loading of segments
    /// for better performance.
    ///
    /// # Future Optimization
    ///
    /// The current implementation loads segments sequentially, but this could
    /// be parallelized for better performance with large numbers of segments.
    ///
    /// # Parameters
    ///
    /// * `reader` - The sliced file reader for storage operations
    /// * `manifest` - Manifest containing metadata about the segmented vector
    ///
    /// # Returns
    ///
    /// A reconstructed `SegmentedVec` with the same data as when it was dumped.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The manifest contains invalid metadata
    /// - Segment size is not valid (not > 1 or not power of two)
    /// - The total length doesn't match the sum of segment lengths
    /// - Any individual segment fails to load
    fn load<R>(reader: amudai_io::SlicedFile<R>, manifest: &Manifest) -> std::io::Result<Self>
    where
        R: amudai_io::ReadAt + Clone,
    {
        // TODO: parallel

        let expected_len = manifest.get::<u64>("len")? as usize;
        let segment_size = manifest.get::<u64>("segment_size")? as usize;
        verify!(segment_size > 1);
        verify!(segment_size.is_power_of_two());

        let mut segments = Vec::with_capacity(manifest.child_list.len());
        let mut len = 0;
        for child_manifest in manifest.child_list.iter() {
            let segment = C::load(
                reader.slice(child_manifest.range.clone())?,
                &child_manifest.manifest,
            )?;
            len += segment.len();
            segments.push(segment);
        }
        verify!(len == expected_len);
        SegmentedVec::from_segments(segments, segment_size)
    }
}

impl<C> Collection for SegmentedVec<C> {
    fn len(&self) -> usize {
        SegmentedVec::len(self)
    }
}

impl<C: Collector> Collector for SegmentedVec<C> {
    type Item = C::Item;

    fn with_item_capacity(_capacity: usize) -> Self {
        SegmentedVec::new()
    }

    fn with_data_capacity(_capacity_bytes: usize) -> Self {
        SegmentedVec::new()
    }

    fn reserve_items(&mut self, _additional: usize) {}

    fn reserve_data(&mut self, _additional_bytes: usize) {}

    fn push(&mut self, value: &Self::Item) {
        SegmentedVec::push(self, value);
    }

    fn clear(&mut self) {
        SegmentedVec::clear(self);
    }
}

/// A streaming writer for building and dumping [`SegmentedVec`] collections directly
/// to external storage.
///
/// `SegmentedVecWriter<C>` provides an append-only interface for building segmented vector
/// collections that are written directly to storage as segments fill up.
/// This enables processing of datasets that exceed available memory by streaming data
/// through fixed-size segments without requiring the entire collection to be held in memory.
///
/// The dump format written by `SegmentedVecWriter` can be loaded back into `SegmentedVec`.
///
/// # Usage Pattern
///
/// The typical workflow follows a streaming pattern:
/// 1. Create a writer with [`new()`](Self::new) or [`with_segment_size()`](Self::with_segment_size)
/// 2. Stream data using [`push()`](Self::push) - segments flush automatically when full
/// 3. Complete the stream with [`finish()`](Self::finish) or [`finish_and_seal()`](Self::finish_and_seal)
///
/// # Memory Efficiency
///
/// Unlike building a full [`SegmentedVec`] in memory before dumping, `SegmentedVecWriter`
/// maintains constant memory usage regardless of the total dataset size:
///
/// - Only one segment worth of data is held in memory
/// - Full segments are immediately written to storage
///
/// # Segment Size Considerations
///
/// The segment size affects both memory usage and I/O efficiency:
///
/// - Larger segments: Better I/O efficiency, higher memory usage
/// - Smaller segments: Lower memory usage, more I/O operations
///
/// Choose segment size based on your memory constraints and I/O patterns.
///
/// # Type Parameters
///
/// - `C`: The collection type used for individual segments. Must implement
///   [`Collector`] for building and [`Dump`] for storage operations.
///
/// # Performance Characteristics
///
/// - Push: O(1) amortized, with occasional flush overhead when segments fill
/// - Memory usage: O(segment_size) - near constant regardless of total data size
/// - I/O pattern: Sequential writes
/// - Flush frequency: based on segment size and data rate
pub struct SegmentedVecWriter<C> {
    /// Current in-progress segment being built
    current: C,
    /// Storage writer for flushing completed segments
    writer: Arc<dyn WriteAt>,
    /// Current position in the storage writer (where the next full segment will be flushed)
    pos: u64,
    /// In-progress manifest of the final segmented vec dump
    manifest: Manifest,
    /// The number of items per segment (must be power of two). Uniform for all segments
    /// except the last one.
    segment_size: usize,
    /// Total number of elements across all segments (including current)
    len: usize,
}

impl<C> SegmentedVecWriter<C> {
    /// Creates a new `SegmentedVecWriter` with the default segment size.
    ///
    /// This is equivalent to calling [`with_segment_size(DEFAULT_SEGMENT_SIZE, writer)`](Self::with_segment_size).
    /// The default segment size is 64K elements.
    ///
    /// # Parameters
    ///
    /// * `writer` - Storage writer where segments will be written. Must support
    ///   writing at arbitrary offsets.
    pub fn new(writer: Arc<dyn WriteAt>) -> SegmentedVecWriter<C>
    where
        C: Collector,
    {
        Self::with_segment_size(SegmentedVec::<C>::DEFAULT_SEGMENT_SIZE, writer)
    }

    /// Creates a `SegmentedVecWriter` with a specific segment size.
    ///
    /// Allows customization of the segment size to optimize for specific memory and I/O
    /// constraints. Larger segments reduce I/O overhead but increase memory usage, while
    /// smaller segments use less memory but may increase I/O operations.
    ///
    /// # Parameters
    ///
    /// * `segment_size` - Number of elements per segment. Must be greater than 1 and
    ///   a power of two for efficient bit-shift operations.
    /// * `writer` - Storage writer where segments will be written.
    pub fn with_segment_size(segment_size: usize, writer: Arc<dyn WriteAt>) -> SegmentedVecWriter<C>
    where
        C: Collector,
    {
        assert!(segment_size > 1);
        assert!(segment_size.is_power_of_two());

        let mut manifest = Manifest::default();
        manifest.put("segment_size", segment_size as u64);

        SegmentedVecWriter {
            current: C::with_item_capacity(segment_size),
            writer,
            pos: 0,
            manifest,
            segment_size,
            len: 0,
        }
    }

    /// Returns the total number of elements pushed to this writer.
    ///
    /// This includes elements in both completed segments (already flushed to storage)
    /// and the current in-progress segment. The count is maintained across all
    /// flush operations.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the writer has not processed any elements.
    ///
    /// This is equivalent to checking if [`len()`](Self::len) returns 0.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the number of elements per segment.
    ///
    /// This value is fixed when the `SegmentedVecWriter` is created and cannot be changed.
    #[inline]
    pub fn segment_size(&self) -> usize {
        self.segment_size
    }
}

impl<C> SegmentedVecWriter<C>
where
    C: Collector + Dump,
{
    /// Appends an element to the writer.
    ///
    /// Elements are added to the current in-progress segment. When the segment reaches
    /// the configured [`segment_size()`](Self::segment_size), it is automatically flushed
    /// to storage and a new segment is started.
    ///
    /// # Parameters
    ///
    /// * `value` - Reference to the element to append. The element is copied/cloned
    ///   into the current segment.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an I/O error if the flush operation fails
    /// when a segment becomes full.
    ///
    /// # Memory Usage
    ///
    /// The writer maintains near constant memory usage equal to one segment regardless
    /// of how many elements are pushed. When segments fill up, they are immediately
    /// written to storage and cleared from memory.
    #[inline]
    pub fn push(&mut self, value: &C::Item) -> std::io::Result<()> {
        // Check if current segment is full
        if self.current.len() >= self.segment_size {
            self.flush_current_segment()?;
        }

        self.current.push(value);
        self.len += 1;
        Ok(())
    }

    /// Flushes the current segment to storage and starts a new segment.
    ///
    /// This method performs the core streaming operation: it dumps the current segment
    /// to the storage writer, records its manifest information, and prepares for the
    /// next segment. This is called automatically by [`push()`](Self::push) when the
    /// current segment reaches capacity.
    ///
    /// # Storage Layout
    ///
    /// Segments are written sequentially to storage with page-aligned boundaries:
    /// ```text
    /// ┌─────────────┬─────────────┬─────────────┬─────────────┐
    /// │  Segment 0  │  Segment 1  │  Segment 2  │     ...     │
    /// └─────────────┴─────────────┴─────────────┴─────────────┘
    /// ```
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful flush, or an I/O error if the storage
    /// operation fails.
    #[cold]
    fn flush_current_segment(&mut self) -> std::io::Result<()> {
        if self.current.len() == 0 {
            return Ok(()); // Nothing to flush
        }

        let size = self.current.compute_size();
        verify!(is_aligned_u64(size, PAGE_SIZE));
        let next_pos = self.pos + size;

        let sliced_writer = SlicedFile::new(self.writer.clone(), self.pos..next_pos);

        let child_manifest = self
            .current
            .dump(sliced_writer)?
            .into_nested(self.pos..next_pos);

        self.manifest.child_list.push(child_manifest);
        self.pos = next_pos;

        // Start a new segment
        self.current.clear();

        Ok(())
    }

    /// Completes the streaming operation and returns the final manifest of the
    /// segmented vector.
    ///
    /// This method finalizes the segmented vector by flushing any remaining data
    /// in the current segment and producing a complete manifest that describes
    /// the entire collection. The manifest contains all information needed to
    /// reconstruct the segmented vector from storage.
    ///
    /// # Manifest Structure
    ///
    /// The returned manifest contains:
    /// - `"len"`: Total number of elements across all segments
    /// - `"segment_size"`: The segment size used for this collection
    /// - `child_list`: Array of nested manifests, one for each flushed segment
    ///
    /// # Usage
    ///
    /// After calling `finish()`, the writer should not be used for further operations.
    /// The manifest can be used with [`SegmentedVec::load()`] to reconstruct the data.
    ///
    /// # Returns
    ///
    /// Returns the complete manifest on success, or an I/O error if the final
    /// flush operation fails.
    pub fn finish(&mut self) -> std::io::Result<Manifest> {
        // The manifest will be empty if `finish()` is accidentally called twice.
        verify!(self.manifest.get::<u64>("segment_size").is_ok());

        // Flush any remaining data in the current segment
        if self.current.len() > 0 {
            self.flush_current_segment()?;
        }

        // Set the final manifest properties
        self.manifest.put("len", self.len as u64);

        Ok(std::mem::take(&mut self.manifest))
    }

    /// Completes the streaming operation and creates a sealed, self-describing file.
    ///
    /// This method combines [`finish()`](Self::finish) with manifest sealing to create
    /// a complete, self-contained file that includes both the data and metadata needed
    /// for loading. The resulting file can be loaded without requiring a separate
    /// manifest.
    ///
    /// # File Structure
    ///
    /// The sealed file has the following layout:
    /// ```text
    /// ┌─────────────┬─────────────┬─────────────┬──────────┬─────────────┐
    /// │  Segment 0  │  Segment 1  │     ...     │ Manifest │   Footer    │
    /// └─────────────┴─────────────┴─────────────┴──────────┴─────────────┘
    /// ```
    ///
    /// The footer contains metadata that allows the manifest to be located and
    /// loaded without external information.
    ///
    /// # Self-Describing Files
    ///
    /// Sealed files are completely self-contained and can be loaded using
    /// [`Load::load_sealed()`]:
    ///
    /// # Ownership
    ///
    /// This method consumes the writer (`self`) since sealed files represent
    /// completed, immutable datasets.
    ///
    /// # Returns
    ///
    /// Returns a [`SlicedFile`] representing the complete sealed file that contains
    /// both data and manifest, or an I/O error if the sealing process fails.
    pub fn finish_and_seal(mut self) -> std::io::Result<SlicedFile<Arc<dyn WriteAt>>> {
        let manifest = self.finish()?;
        let footer = manifest.save_as_footer(self.writer.clone(), self.pos)?;
        Ok(SlicedFile::new(self.writer, 0..footer.end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{bytes_vec::BytesCollector, pod_vec::PodCollector};

    #[test]
    fn test_pod_collector_basic_operations() {
        // Test basic construction and operations
        let mut segmented = SegmentedVec::<PodCollector<i32>>::new();
        assert_eq!(segmented.len(), 0);
        assert!(segmented.is_empty());
        assert_eq!(
            segmented.segment_size(),
            SegmentedVec::<PodCollector<i32>>::DEFAULT_SEGMENT_SIZE
        );

        // Test pushing elements
        segmented.push(&42);
        segmented.push(&100);
        segmented.push(&-50);

        assert_eq!(segmented.len(), 3);
        assert!(!segmented.is_empty());
        assert_eq!(segmented.segments().len(), 1); // All should fit in one segment

        // Test indexed access
        assert_eq!(segmented[0], 42);
        assert_eq!(segmented[1], 100);
        assert_eq!(segmented[2], -50);
    }

    #[test]
    fn test_pod_collector_with_custom_segment_size() {
        // Use a small segment size for testing
        let segment_size = 4;
        let mut segmented = SegmentedVec::<PodCollector<u64>>::with_segment_size(segment_size);

        assert_eq!(segmented.segment_size(), segment_size);

        // Fill first segment
        for i in 0..segment_size {
            segmented.push(&(i as u64));
        }

        assert_eq!(segmented.len(), segment_size);
        assert_eq!(segmented.segments().len(), 1);

        // Add one more to trigger new segment
        segmented.push(&(segment_size as u64));

        assert_eq!(segmented.len(), segment_size + 1);
        assert_eq!(segmented.segments().len(), 2);

        // Verify access across segments
        for i in 0..=segment_size {
            assert_eq!(segmented[i], i as u64);
        }
    }

    #[test]
    fn test_pod_collector_multiple_segments() {
        let segment_size = 8;
        let mut segmented = SegmentedVec::<PodCollector<i16>>::with_segment_size(segment_size);

        // Fill multiple segments
        let total_elements = segment_size * 3 + 2; // 3 full segments + 2 in partial
        for i in 0..total_elements {
            segmented.push(&(i as i16));
        }

        assert_eq!(segmented.len(), total_elements);
        assert_eq!(segmented.segments().len(), 4); // 3 full + 1 partial

        // Verify all elements are accessible
        for i in 0..total_elements {
            assert_eq!(segmented[i], i as i16);
        }

        // Verify segment mapping
        for i in 0..total_elements {
            let (seg_id, offset) = segmented.map_index(i);
            let expected_seg = i / segment_size;
            let expected_offset = i % segment_size;
            assert_eq!(seg_id, expected_seg);
            assert_eq!(offset, expected_offset);
        }
    }

    #[test]
    fn test_pod_collector_clear() {
        let mut segmented = SegmentedVec::<PodCollector<f32>>::with_segment_size(4);

        // Add some elements
        for i in 0..10 {
            segmented.push(&(i as f32 * 1.5));
        }

        assert_eq!(segmented.len(), 10);
        assert_eq!(segmented.segments().len(), 3);

        // Clear and verify
        segmented.clear();
        assert_eq!(segmented.len(), 0);
        assert!(segmented.is_empty());
        assert_eq!(segmented.segments().len(), 0);
        assert_eq!(segmented.segment_size(), 4); // Segment size should be preserved

        // Should be able to add elements again
        segmented.push(&99.9);
        assert_eq!(segmented.len(), 1);
        assert_eq!(segmented[0], 99.9);
    }

    #[test]
    fn test_bytes_collector_basic_operations() {
        let mut segmented = SegmentedVec::<BytesCollector>::new();
        assert_eq!(segmented.len(), 0);
        assert!(segmented.is_empty());

        // Test pushing byte slices
        segmented.push(b"hello");
        segmented.push(b"world");
        segmented.push(b"test");

        assert_eq!(segmented.len(), 3);
        assert!(!segmented.is_empty());
        assert_eq!(segmented.segments().len(), 1);

        // Test indexed access
        assert_eq!(&segmented[0], b"hello");
        assert_eq!(&segmented[1], b"world");
        assert_eq!(&segmented[2], b"test");
    }

    #[test]
    fn test_bytes_collector_with_custom_segment_size() {
        let segment_size = 2;
        let mut segmented = SegmentedVec::<BytesCollector>::with_segment_size(segment_size);

        // Add elements to fill segments
        let data = vec![
            &b"first"[..],
            &b"second"[..],
            &b"third"[..],
            &b"fourth"[..],
            &b"fifth"[..],
        ];
        for item in &data {
            segmented.push(item);
        }

        assert_eq!(segmented.len(), 5);
        assert_eq!(segmented.segments().len(), 3); // ceil(5/2) = 3 segments

        // Verify access
        for (i, expected) in data.iter().enumerate() {
            assert_eq!(&segmented[i], *expected);
        }
    }

    #[test]
    fn test_bytes_collector_large_data() {
        let segment_size = 16;
        let mut segmented = SegmentedVec::<BytesCollector>::with_segment_size(segment_size);

        // Add various sized byte arrays
        let mut expected_data = Vec::new();
        for i in 0..50 {
            let data = format!("data_item_{:03}", i).into_bytes();
            expected_data.push(data.clone());
            segmented.push(&data);
        }

        assert_eq!(segmented.len(), 50);

        // Verify all data
        for (i, expected) in expected_data.iter().enumerate() {
            assert_eq!(&segmented[i], expected.as_slice());
        }
    }

    #[test]
    fn test_bytes_collector_empty_and_varied_slices() {
        let mut segmented = SegmentedVec::<BytesCollector>::with_segment_size(4);

        // Test with empty slices and varied lengths
        segmented.push(b"");
        segmented.push(b"a");
        segmented.push(b"longer text here");
        segmented.push(b"");
        segmented.push(b"final");

        assert_eq!(segmented.len(), 5);

        assert_eq!(&segmented[0], b"");
        assert_eq!(&segmented[1], b"a");
        assert_eq!(&segmented[2], b"longer text here");
        assert_eq!(&segmented[3], b"");
        assert_eq!(&segmented[4], b"final");
    }

    #[test]
    fn test_from_segments_pod_collector() {
        let segment_size = 4;

        // Create individual segments
        let mut seg1 = PodCollector::<u32>::new();
        let mut seg2 = PodCollector::<u32>::new();
        let mut seg3 = PodCollector::<u32>::new();

        // Fill segments
        for i in 0..4 {
            seg1.push(i);
        }
        for i in 4..8 {
            seg2.push(i);
        }
        for i in 8..10 {
            // Partial segment
            seg3.push(i);
        }

        let segments = vec![seg1, seg2, seg3];
        let segmented = SegmentedVec::from_segments(segments, segment_size).unwrap();

        assert_eq!(segmented.len(), 10);
        assert_eq!(segmented.segments().len(), 3);
        assert_eq!(segmented.segment_size(), segment_size);

        // Verify all values
        for i in 0..10 {
            assert_eq!(segmented[i], i as u32);
        }
    }

    #[test]
    fn test_from_segments_bytes_collector() {
        let segment_size = 2;

        // Create individual segments
        let mut seg1 = BytesCollector::new();
        let mut seg2 = BytesCollector::new();

        seg1.push(b"first");
        seg1.push(b"second");

        seg2.push(b"third"); // Partial segment

        let segments = vec![seg1, seg2];
        let segmented = SegmentedVec::from_segments(segments, segment_size).unwrap();

        assert_eq!(segmented.len(), 3);
        assert_eq!(segmented.segments().len(), 2);

        assert_eq!(&segmented[0], b"first");
        assert_eq!(&segmented[1], b"second");
        assert_eq!(&segmented[2], b"third");
    }

    #[test]
    fn test_map_index_functionality() {
        let segment_size = 8;
        let segmented = SegmentedVec::<PodCollector<i32>>::with_segment_size(segment_size);

        // Test various indices
        let test_cases = [
            (0, (0, 0)),
            (1, (0, 1)),
            (7, (0, 7)),
            (8, (1, 0)),
            (9, (1, 1)),
            (15, (1, 7)),
            (16, (2, 0)),
            (23, (2, 7)),
            (24, (3, 0)),
        ];

        for (index, expected) in test_cases {
            assert_eq!(
                segmented.map_index(index),
                expected,
                "Failed for index {}",
                index
            );
        }
    }

    #[test]
    fn test_is_last_segment_full() {
        let segment_size = 4;
        let mut segmented = SegmentedVec::<PodCollector<u8>>::with_segment_size(segment_size);

        assert!(segmented.is_last_segment_full()); // Empty is considered "full"

        segmented.push(&1);
        assert!(!segmented.is_last_segment_full());

        segmented.push(&2);
        segmented.push(&3);
        assert!(!segmented.is_last_segment_full());

        segmented.push(&4);
        assert!(segmented.is_last_segment_full()); // Exactly at segment boundary

        segmented.push(&5);
        assert!(!segmented.is_last_segment_full()); // New segment with one element
    }

    #[test]
    fn test_large_pod_dataset() {
        let segment_size = 1024;
        let mut segmented = SegmentedVec::<PodCollector<u64>>::with_segment_size(segment_size);

        let total_elements = segment_size * 5 + 100; // 5+ segments
        for i in 0..total_elements {
            segmented.push(&(i as u64));
        }

        assert_eq!(segmented.len(), total_elements);
        assert_eq!(segmented.segments().len(), 6); // 5 full + 1 partial

        // Spot check some values across different segments
        assert_eq!(segmented[0], 0);
        assert_eq!(segmented[segment_size - 1], (segment_size - 1) as u64);
        assert_eq!(segmented[segment_size], segment_size as u64);
        assert_eq!(segmented[segment_size * 2], (segment_size * 2) as u64);
        assert_eq!(segmented[total_elements - 1], (total_elements - 1) as u64);
    }

    #[test]
    fn test_large_bytes_dataset() {
        let segment_size = 256;
        let mut segmented = SegmentedVec::<BytesCollector>::with_segment_size(segment_size);

        let total_elements = segment_size * 3 + 50;
        let mut expected_data = Vec::new();

        for i in 0..total_elements {
            let data = format!("item_{:06}", i).into_bytes();
            expected_data.push(data.clone());
            segmented.push(&data);
        }

        assert_eq!(segmented.len(), total_elements);
        assert_eq!(segmented.segments().len(), 4); // 3 full + 1 partial

        // Verify random access
        for _ in 0..100 {
            let idx = (std::ptr::addr_of!(segmented) as usize * 17 + 42) % total_elements; // Pseudo-random
            assert_eq!(&segmented[idx], expected_data[idx].as_slice());
        }
    }

    #[test]
    fn test_segment_size_requirements() {
        // Test that invalid segment sizes are rejected

        // Size must be > 1
        assert!(
            std::panic::catch_unwind(|| {
                SegmentedVec::<PodCollector<i32>>::with_segment_size(1)
            })
            .is_err()
        );

        assert!(
            std::panic::catch_unwind(|| {
                SegmentedVec::<PodCollector<i32>>::with_segment_size(0)
            })
            .is_err()
        );

        // Size must be power of two
        assert!(
            std::panic::catch_unwind(|| {
                SegmentedVec::<PodCollector<i32>>::with_segment_size(3)
            })
            .is_err()
        );

        assert!(
            std::panic::catch_unwind(|| {
                SegmentedVec::<PodCollector<i32>>::with_segment_size(5)
            })
            .is_err()
        );

        assert!(
            std::panic::catch_unwind(|| {
                SegmentedVec::<PodCollector<i32>>::with_segment_size(12)
            })
            .is_err()
        );

        // Valid powers of two should work
        let valid_sizes = [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024];
        for size in valid_sizes {
            let segmented = SegmentedVec::<PodCollector<i32>>::with_segment_size(size);
            assert_eq!(segmented.segment_size(), size);
        }
    }

    #[test]
    fn test_try_from_vec_single_segment() {
        // Create a single segment and convert
        let mut segment = PodCollector::<f64>::new();
        for i in 0..10 {
            segment.push(i as f64 + 0.5);
        }

        let segments = vec![segment];
        let segmented: SegmentedVec<PodCollector<f64>> = segments.try_into().unwrap();

        // Should use next power of two >= max(len, DEFAULT_SEGMENT_SIZE)
        let expected_segment_size =
            SegmentedVec::<PodCollector<f64>>::DEFAULT_SEGMENT_SIZE.next_power_of_two();
        assert_eq!(segmented.segment_size(), expected_segment_size);
        assert_eq!(segmented.len(), 10);

        for i in 0..10 {
            assert_eq!(segmented[i], i as f64 + 0.5);
        }
    }

    #[test]
    fn test_try_from_vec_multiple_segments() {
        let segment_size = 4;

        // Create multiple segments
        let mut segments = Vec::new();
        for seg_idx in 0..3 {
            let mut segment = PodCollector::<i32>::new();
            for i in 0..segment_size {
                segment.push((seg_idx * segment_size + i) as i32);
            }
            segments.push(segment);
        }

        let segmented: SegmentedVec<PodCollector<i32>> = segments.try_into().unwrap();

        // Should infer segment size from first segment
        assert_eq!(segmented.segment_size(), segment_size);
        assert_eq!(segmented.len(), 12);

        for i in 0..12 {
            assert_eq!(segmented[i], i as i32);
        }
    }

    #[test]
    fn test_empty_segmented_vec() {
        let segmented = SegmentedVec::<PodCollector<u8>>::new();
        assert_eq!(segmented.len(), 0);
        assert!(segmented.is_empty());
        assert_eq!(segmented.segments().len(), 0);

        let segmented_bytes = SegmentedVec::<BytesCollector>::new();
        assert_eq!(segmented_bytes.len(), 0);
        assert!(segmented_bytes.is_empty());
        assert_eq!(segmented_bytes.segments().len(), 0);
    }

    #[test]
    fn test_mixed_operations() {
        let segment_size = 4;
        let mut segmented = SegmentedVec::<PodCollector<i32>>::with_segment_size(segment_size);

        // Add some elements
        for i in 0..10 {
            segmented.push(&i);
        }

        // Test various operations
        assert_eq!(segmented.len(), 10);
        assert_eq!(segmented.segments().len(), 3); // 4 + 4 + 2

        // Verify each segment has correct length
        assert_eq!(segmented.segments()[0].len(), 4);
        assert_eq!(segmented.segments()[1].len(), 4);
        assert_eq!(segmented.segments()[2].len(), 2);

        // Test map_index for all elements
        for i in 0..10 {
            let (seg_id, offset) = segmented.map_index(i);
            assert_eq!(segmented.segments()[seg_id][offset], i as i32);
        }

        // Clear and start over
        segmented.clear();
        assert_eq!(segmented.len(), 0);
        assert_eq!(segmented.segments().len(), 0);

        // Add different data
        for i in 100..105 {
            segmented.push(&i);
        }

        assert_eq!(segmented.len(), 5);
        for i in 0..5 {
            assert_eq!(segmented[i], (100 + i) as i32);
        }
    }

    #[test]
    fn test_access_patterns() {
        let segment_size = 8;
        let mut segmented = SegmentedVec::<BytesCollector>::with_segment_size(segment_size);

        // Create test data with predictable pattern
        let mut test_data = Vec::new();
        for i in 0..25 {
            let data = format!("data_{:02}", i);
            test_data.push(data.clone());
            segmented.push(data.as_bytes());
        }

        // Test sequential access
        for i in 0..25 {
            assert_eq!(&segmented[i], test_data[i].as_bytes());
        }

        // Test reverse access
        for i in (0..25).rev() {
            assert_eq!(&segmented[i], test_data[i].as_bytes());
        }

        // Test random access pattern
        let indices = [0, 24, 7, 15, 3, 12, 20, 1, 8, 16, 23];
        for &idx in &indices {
            assert_eq!(&segmented[idx], test_data[idx].as_bytes());
        }
    }

    #[test]
    fn test_from_segments_error_cases() {
        // Test invalid segment size validation
        let segments = vec![PodCollector::<u32>::new()];
        assert!(SegmentedVec::from_segments(segments, 0).is_err());
        assert!(SegmentedVec::from_segments(vec![PodCollector::<u32>::new()], 3).is_err());

        // Test invalid segment lengths
        let mut seg1 = PodCollector::<u32>::new();
        let mut seg2 = PodCollector::<u32>::new();

        // Fill first segment with incorrect size
        for i in 0..3 {
            seg1.push(i);
        }
        for i in 3..5 {
            seg2.push(i);
        }

        // First segment should be exactly segment_size (4), but it's 3
        assert!(SegmentedVec::from_segments(vec![seg1, seg2], 4).is_err());
    }

    #[test]
    fn test_two_element_segments() {
        let segment_size = 2;
        let mut segmented = SegmentedVec::<PodCollector<u8>>::with_segment_size(segment_size);

        segmented.push(&1);
        segmented.push(&2);
        segmented.push(&3);

        assert_eq!(segmented.len(), 3);
        assert_eq!(segmented.segments().len(), 2); // [1,2], [3]

        assert_eq!(segmented[0], 1);
        assert_eq!(segmented[1], 2);
        assert_eq!(segmented[2], 3);
    }

    #[test]
    fn test_bytes_collector_unicode_and_special_chars() {
        let mut segmented = SegmentedVec::<BytesCollector>::with_segment_size(4);

        // Test with Unicode and special characters
        let test_data = [
            "Hello, 世界".as_bytes(),
            "🦀 Rust".as_bytes(),
            "".as_bytes(), // Empty
            "Special chars: \n\t\r\0".as_bytes(),
            "Numbers: 123.456".as_bytes(),
        ];

        for data in &test_data {
            segmented.push(data);
        }

        assert_eq!(segmented.len(), 5);
        for (i, expected) in test_data.iter().enumerate() {
            assert_eq!(&segmented[i], *expected);
        }
    }

    #[test]
    fn test_pod_collector_different_types() {
        // Test with float
        let mut float_segmented = SegmentedVec::<PodCollector<f32>>::with_segment_size(2);
        let float_data = [1.5, -2.7, 0.0, f32::INFINITY, f32::NEG_INFINITY];
        for &f in &float_data {
            float_segmented.push(&f);
        }
        for (i, &expected) in float_data.iter().enumerate() {
            assert_eq!(float_segmented[i], expected);
        }
    }

    #[test]
    fn test_concurrent_access_pattern() {
        let segment_size = 16;
        let mut segmented = SegmentedVec::<PodCollector<usize>>::with_segment_size(segment_size);

        // Fill with data
        let total_elements = segment_size * 4 + 7;
        for i in 0..total_elements {
            segmented.push(&i);
        }

        // Simulate concurrent-style access patterns (accessing different segments)
        let access_pattern = [
            0,
            segment_size,
            segment_size * 2,
            segment_size * 3,
            total_elements - 1,
        ];
        for &idx in &access_pattern {
            assert_eq!(segmented[idx], idx);

            // Test map_index for these key positions
            let (seg_id, offset) = segmented.map_index(idx);
            assert_eq!(segmented.segments()[seg_id][offset], idx);
        }
    }

    #[test]
    fn test_memory_efficiency_patterns() {
        // Test that segments are created only when needed
        let segment_size = 1024;
        let mut segmented = SegmentedVec::<PodCollector<u64>>::with_segment_size(segment_size);

        // Should start with no segments
        assert_eq!(segmented.segments().len(), 0);

        // Add one element - should create first segment
        segmented.push(&0);
        assert_eq!(segmented.segments().len(), 1);
        assert_eq!(segmented.len(), 1);

        // Fill the first segment
        for i in 1..segment_size {
            segmented.push(&(i as u64));
        }
        assert_eq!(segmented.segments().len(), 1);
        assert_eq!(segmented.len(), segment_size);

        // Add one more - should create second segment
        segmented.push(&(segment_size as u64));
        assert_eq!(segmented.segments().len(), 2);
        assert_eq!(segmented.len(), segment_size + 1);
    }

    #[test]
    fn test_extreme_segment_sizes() {
        // Test with minimum valid segment size
        let mut small_segmented = SegmentedVec::<PodCollector<u8>>::with_segment_size(2);
        for i in 0..10 {
            small_segmented.push(&(i as u8));
        }
        assert_eq!(small_segmented.segments().len(), 5); // ceil(10/2)

        // Test with larger segment size
        let large_segment_size = 4096;
        let mut large_segmented =
            SegmentedVec::<PodCollector<u16>>::with_segment_size(large_segment_size);
        for i in 0..100 {
            large_segmented.push(&(i as u16));
        }
        assert_eq!(large_segmented.segments().len(), 1); // All fit in one segment
        assert_eq!(large_segmented.len(), 100);

        // Verify all values
        for i in 0..100 {
            assert_eq!(large_segmented[i], i as u16);
        }
    }

    #[test]
    fn test_bytes_collector_binary_data() {
        let mut segmented = SegmentedVec::<BytesCollector>::with_segment_size(8);

        // Test with various binary patterns
        let binary_data = vec![
            vec![0x00, 0xFF, 0xAA, 0x55],       // Pattern data
            vec![0x01, 0x02, 0x03, 0x04, 0x05], // Sequential
            vec![],                             // Empty
            vec![0xFF; 256],                    // Large uniform data
            vec![0x00],                         // Single byte
        ];

        for data in &binary_data {
            segmented.push(data);
        }

        assert_eq!(segmented.len(), binary_data.len());
        for (i, expected) in binary_data.iter().enumerate() {
            assert_eq!(&segmented[i], expected.as_slice());
        }
    }

    #[test]
    fn test_try_from_empty_vec() {
        // Test converting empty vector
        let empty_segments: Vec<PodCollector<i32>> = vec![];
        let segmented: SegmentedVec<PodCollector<i32>> = empty_segments.try_into().unwrap();

        assert_eq!(segmented.len(), 0);
        assert!(segmented.is_empty());
        assert_eq!(segmented.segments().len(), 0);
        assert_eq!(
            segmented.segment_size(),
            SegmentedVec::<PodCollector<i32>>::DEFAULT_SEGMENT_SIZE
        );
    }

    #[test]
    fn test_segment_boundaries_exact() {
        let segment_size = 4;
        let mut segmented = SegmentedVec::<PodCollector<i32>>::with_segment_size(segment_size);

        // Add exactly enough elements to fill multiple complete segments
        let total_segments = 3;
        let total_elements = segment_size * total_segments;

        for i in 0..total_elements {
            segmented.push(&(i as i32));
        }

        assert_eq!(segmented.len(), total_elements);
        assert_eq!(segmented.segments().len(), total_segments);

        // Each segment should be exactly full
        for seg in segmented.segments() {
            assert_eq!(seg.len(), segment_size);
        }

        // Test last segment is considered "full"
        assert!(segmented.is_last_segment_full());

        // Verify all elements are correct
        for i in 0..total_elements {
            assert_eq!(segmented[i], i as i32);
        }
    }

    #[test]
    fn test_segmented_pod_collector_dump_and_load() {
        use amudai_io::SharedIoBuffer;
        use std::sync::Arc;

        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        let file = Arc::<dyn SharedIoBuffer>::from(file);
        assert_eq!(file.size().unwrap(), 0);

        // Create a SegmentedVec with small segment size to ensure multiple segments
        let segment_size = 4;
        let mut segmented = SegmentedVec::<PodCollector<i32>>::with_segment_size(segment_size);

        // Add data that will span multiple segments
        let test_data: Vec<i32> = (0..15).collect(); // 15 elements = 4 segments (4,4,4,3)
        for &value in &test_data {
            segmented.push(&value);
        }

        assert_eq!(segmented.len(), 15);
        assert_eq!(segmented.segments().len(), 4); // 4 segments

        // Verify original data before dumping
        for (i, &expected) in test_data.iter().enumerate() {
            assert_eq!(segmented[i], expected);
        }

        // Dump and seal the segmented vector
        let end_pos = segmented.dump_and_seal(file.clone()).unwrap();
        dbg!(&end_pos);

        // Load it back
        let loaded_segmented =
            SegmentedVec::<crate::pod_vec::PodView<i32>>::load_sealed(file.clone()).unwrap();

        // Verify the loaded data matches the original
        assert_eq!(loaded_segmented.len(), 15);
        assert_eq!(loaded_segmented.segment_size(), segment_size);
        assert_eq!(loaded_segmented.segments().len(), 4);

        // Check all values
        for (i, &expected) in test_data.iter().enumerate() {
            assert_eq!(loaded_segmented[i], expected);
        }

        // Verify segment sizes
        assert_eq!(loaded_segmented.segments()[0].len(), 4);
        assert_eq!(loaded_segmented.segments()[1].len(), 4);
        assert_eq!(loaded_segmented.segments()[2].len(), 4);
        assert_eq!(loaded_segmented.segments()[3].len(), 3); // Last segment is partial
    }

    #[test]
    fn test_segmented_bytes_collector_dump_and_load() {
        use amudai_io::SharedIoBuffer;
        use std::sync::Arc;

        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        let file = Arc::<dyn SharedIoBuffer>::from(file);
        assert_eq!(file.size().unwrap(), 0);

        // Create a SegmentedVec with small segment size for multiple segments
        let segment_size = 2;
        let mut segmented = SegmentedVec::<BytesCollector>::with_segment_size(segment_size);

        // Add various byte sequences that will span multiple segments
        let test_data = vec![
            b"hello".as_slice(),
            b"world".as_slice(),
            b"test".as_slice(),
            b"data".as_slice(),
            b"".as_slice(), // empty slice
            b"final".as_slice(),
        ];

        for data in &test_data {
            segmented.push(data);
        }

        assert_eq!(segmented.len(), 6);
        assert_eq!(segmented.segments().len(), 3); // 6 elements / 2 per segment = 3 segments

        // Verify original data before dumping
        for (i, &expected) in test_data.iter().enumerate() {
            assert_eq!(&segmented[i], expected);
        }

        // Dump and seal the segmented vector
        let end_pos = segmented.dump_and_seal(file.clone()).unwrap();
        dbg!(&end_pos);

        // Load it back
        let loaded_segmented =
            SegmentedVec::<crate::bytes_vec::BytesView>::load_sealed(file.clone()).unwrap();

        // Verify the loaded data matches the original
        assert_eq!(loaded_segmented.len(), 6);
        assert_eq!(loaded_segmented.segment_size(), segment_size);
        assert_eq!(loaded_segmented.segments().len(), 3);

        // Check all values
        for (i, &expected) in test_data.iter().enumerate() {
            assert_eq!(&loaded_segmented[i], expected);
        }

        // Verify segment sizes
        assert_eq!(loaded_segmented.segments()[0].len(), 2); // "hello", "world"
        assert_eq!(loaded_segmented.segments()[1].len(), 2); // "test", "data"
        assert_eq!(loaded_segmented.segments()[2].len(), 2); // "", "final"
    }

    #[test]
    fn test_segmented_pod_collector_single_segment_dump_and_load() {
        use amudai_io::SharedIoBuffer;
        use std::sync::Arc;

        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        let file = Arc::<dyn SharedIoBuffer>::from(file);

        // Create a SegmentedVec with large segment size so everything fits in one segment
        let segment_size = 64;
        let mut segmented = SegmentedVec::<PodCollector<u64>>::with_segment_size(segment_size);

        // Add data that will fit in a single segment
        let test_data: Vec<u64> = (100..120).collect(); // 20 elements
        for &value in &test_data {
            segmented.push(&value);
        }

        assert_eq!(segmented.len(), 20);
        assert_eq!(segmented.segments().len(), 1); // Single segment

        // Dump and load
        let _end_pos = segmented.dump_and_seal(file.clone()).unwrap();
        let loaded_segmented =
            SegmentedVec::<crate::pod_vec::PodView<u64>>::load_sealed(file.clone()).unwrap();

        // Verify
        assert_eq!(loaded_segmented.len(), 20);
        assert_eq!(loaded_segmented.segment_size(), segment_size);
        assert_eq!(loaded_segmented.segments().len(), 1);

        for (i, &expected) in test_data.iter().enumerate() {
            assert_eq!(loaded_segmented[i], expected);
        }
    }

    #[test]
    fn test_segmented_bytes_collector_empty_dump_and_load() {
        use amudai_io::SharedIoBuffer;
        use std::sync::Arc;

        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        let file = Arc::<dyn SharedIoBuffer>::from(file);

        // Create an empty SegmentedVec
        let segmented = SegmentedVec::<BytesCollector>::new();

        assert_eq!(segmented.len(), 0);
        assert_eq!(segmented.segments().len(), 0);

        // Dump and load empty vector
        let _end_pos = segmented.dump_and_seal(file.clone()).unwrap();
        let loaded_segmented =
            SegmentedVec::<crate::bytes_vec::BytesView>::load_sealed(file.clone()).unwrap();

        // Verify empty state is preserved
        assert_eq!(loaded_segmented.len(), 0);
        assert_eq!(loaded_segmented.segments().len(), 0);
        assert!(loaded_segmented.is_empty());
    }

    #[test]
    fn test_segmented_pod_collector_large_data_dump_and_load() {
        use amudai_io::SharedIoBuffer;
        use std::sync::Arc;

        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        let file = Arc::<dyn SharedIoBuffer>::from(file);

        // Create a SegmentedVec with many segments
        let segment_size = 8;
        let mut segmented = SegmentedVec::<PodCollector<u32>>::with_segment_size(segment_size);

        // Add a large amount of data to ensure many segments
        let num_elements = 100usize;
        let test_data: Vec<u32> = (0..num_elements).map(|i| (i * i) as u32).collect(); // squares

        for &value in &test_data {
            segmented.push(&value);
        }

        let expected_segments = (num_elements + segment_size - 1) / segment_size; // ceil division
        assert_eq!(segmented.len(), num_elements);
        assert_eq!(segmented.segments().len(), expected_segments);

        // Dump and load
        let _end_pos = segmented.dump_and_seal(file.clone()).unwrap();
        let loaded_segmented =
            SegmentedVec::<crate::pod_vec::PodView<u32>>::load_sealed(file.clone()).unwrap();

        // Verify
        assert_eq!(loaded_segmented.len(), num_elements);
        assert_eq!(loaded_segmented.segment_size(), segment_size);
        assert_eq!(loaded_segmented.segments().len(), expected_segments);

        // Check all values
        for (i, &expected) in test_data.iter().enumerate() {
            assert_eq!(loaded_segmented[i], expected, "Mismatch at index {}", i);
        }

        // Verify segment sizes (all should be segment_size except possibly the last)
        for (i, segment) in loaded_segmented.segments().iter().enumerate() {
            if i == expected_segments - 1 {
                // Last segment might be partial
                let expected_size = num_elements % segment_size;
                let expected_size = if expected_size == 0 {
                    segment_size
                } else {
                    expected_size
                };
                assert_eq!(segment.len(), expected_size);
            } else {
                assert_eq!(segment.len(), segment_size);
            }
        }
    }

    #[test]
    fn test_segmented_pod_collector_boundary_conditions_dump_load() {
        use amudai_io::SharedIoBuffer;
        use std::sync::Arc;

        // Test exact segment boundary conditions
        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        let file = Arc::<dyn SharedIoBuffer>::from(file);

        let segment_size = 16;
        let mut segmented = SegmentedVec::<PodCollector<i32>>::with_segment_size(segment_size);

        // Add exactly segment_size * 3 elements (perfect boundary)
        let total_elements = segment_size * 3;
        for i in 0..total_elements {
            segmented.push(&(i as i32));
        }

        assert_eq!(segmented.len(), total_elements);
        assert_eq!(segmented.segments().len(), 3); // Exactly 3 full segments

        // Dump and load
        let _end_pos = segmented.dump_and_seal(file.clone()).unwrap();
        let loaded_segmented =
            SegmentedVec::<crate::pod_vec::PodView<i32>>::load_sealed(file.clone()).unwrap();

        // Verify perfect boundary loading
        assert_eq!(loaded_segmented.len(), total_elements);
        assert_eq!(loaded_segmented.segments().len(), 3);

        // All segments should be full
        for segment in loaded_segmented.segments() {
            assert_eq!(segment.len(), segment_size);
        }

        // Verify data integrity
        for i in 0..total_elements {
            assert_eq!(loaded_segmented[i], i as i32);
        }
    }

    #[test]
    fn test_segmented_bytes_collector_edge_cases_dump_load() {
        use amudai_io::SharedIoBuffer;
        use std::sync::Arc;

        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        let file = Arc::<dyn SharedIoBuffer>::from(file);

        let segment_size = 2;
        let mut segmented = SegmentedVec::<BytesCollector>::with_segment_size(segment_size);

        // Edge case data similar to bytes_vec.rs patterns
        let long_string = b"a".repeat(10000);
        let edge_cases = vec![
            b"".as_slice(),                 // empty
            b"\x00".as_slice(),             // null byte
            b"\x00\x01\x02\x03".as_slice(), // binary data
            b"\xff\xfe\xfd".as_slice(),     // high bytes
            b"\n\r\t".as_slice(),           // whitespace
            long_string.as_slice(),         // very long string
        ];

        for data in &edge_cases {
            segmented.push(data);
        }

        assert_eq!(segmented.len(), edge_cases.len());
        assert_eq!(segmented.segments().len(), edge_cases.len() / segment_size);

        // Dump and load
        let _end_pos = segmented.dump_and_seal(file.clone()).unwrap();
        let loaded_segmented =
            SegmentedVec::<crate::bytes_vec::BytesView>::load_sealed(file.clone()).unwrap();

        // Verify edge case handling
        assert_eq!(loaded_segmented.len(), edge_cases.len());
        assert_eq!(loaded_segmented.segment_size(), segment_size);
        assert_eq!(
            loaded_segmented.segments().len(),
            edge_cases.len() / segment_size
        );

        // Verify each edge case
        for (i, &expected) in edge_cases.iter().enumerate() {
            assert_eq!(&loaded_segmented[i], expected, "Edge case {} failed", i);
        }
    }

    #[test]
    fn test_segmented_pod_collector_different_numeric_types_dump_load() {
        use amudai_io::SharedIoBuffer;
        use std::sync::Arc;

        // Test i8 type
        {
            let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
            let file = temp_store.allocate_shared_buffer(None).unwrap();
            let file = Arc::<dyn SharedIoBuffer>::from(file);

            let segment_size = 32;
            let mut segmented = SegmentedVec::<PodCollector<i8>>::with_segment_size(segment_size);

            // Full i8 range
            for i in i8::MIN..=i8::MAX {
                segmented.push(&i);
            }

            assert_eq!(segmented.len(), 256);

            let _end_pos = segmented.dump_and_seal(file.clone()).unwrap();
            let loaded_segmented =
                SegmentedVec::<crate::pod_vec::PodView<i8>>::load_sealed(file.clone()).unwrap();

            assert_eq!(loaded_segmented.len(), 256);
            for (i, expected) in (i8::MIN..=i8::MAX).enumerate() {
                assert_eq!(loaded_segmented[i], expected);
            }
        }

        // Test f32 type with special values
        {
            let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
            let file = temp_store.allocate_shared_buffer(None).unwrap();
            let file = Arc::<dyn SharedIoBuffer>::from(file);

            let segment_size = 8;
            let mut segmented = SegmentedVec::<PodCollector<f32>>::with_segment_size(segment_size);

            let test_floats = vec![
                0.0f32,
                -0.0f32,
                1.0f32,
                -1.0f32,
                f32::INFINITY,
                f32::NEG_INFINITY,
                f32::NAN,
                f32::MIN,
                f32::MAX,
                f32::EPSILON,
                3.14159f32,
                2.71828f32,
                1.41421f32,
            ];

            for &value in &test_floats {
                segmented.push(&value);
            }

            let _end_pos = segmented.dump_and_seal(file.clone()).unwrap();
            let loaded_segmented =
                SegmentedVec::<crate::pod_vec::PodView<f32>>::load_sealed(file.clone()).unwrap();

            assert_eq!(loaded_segmented.len(), test_floats.len());
            for (i, &expected) in test_floats.iter().enumerate() {
                if expected.is_nan() {
                    assert!(
                        loaded_segmented[i].is_nan(),
                        "NaN not preserved at index {}",
                        i
                    );
                } else {
                    assert_eq!(
                        loaded_segmented[i], expected,
                        "Float mismatch at index {}",
                        i
                    );
                }
            }
        }
    }

    #[test]
    fn test_segmented_mixed_empty_segments_dump_load() {
        use amudai_io::SharedIoBuffer;
        use std::sync::Arc;

        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        let file = Arc::<dyn SharedIoBuffer>::from(file);

        // Test BytesCollector with many empty entries
        let segment_size = 4;
        let mut segmented = SegmentedVec::<BytesCollector>::with_segment_size(segment_size);

        // Mix of empty and non-empty entries
        let patterns = vec![
            b"".as_slice(),      // empty
            b"a".as_slice(),     // single char
            b"".as_slice(),      // empty again
            b"hello".as_slice(), // normal
            b"".as_slice(),      // empty
            b"".as_slice(),      // empty
            b"world".as_slice(), // normal
            b"".as_slice(),      // empty
        ];

        for pattern in &patterns {
            segmented.push(pattern);
        }

        assert_eq!(segmented.len(), patterns.len());

        let _end_pos = segmented.dump_and_seal(file.clone()).unwrap();
        let loaded_segmented =
            SegmentedVec::<crate::bytes_vec::BytesView>::load_sealed(file.clone()).unwrap();

        assert_eq!(loaded_segmented.len(), patterns.len());
        assert_eq!(loaded_segmented.segment_size(), segment_size);

        for (i, &expected) in patterns.iter().enumerate() {
            assert_eq!(
                &loaded_segmented[i], expected,
                "Pattern mismatch at index {}",
                i
            );
        }
    }

    #[test]
    fn test_segmented_pod_collector_stress_many_segments_dump_load() {
        use amudai_io::SharedIoBuffer;
        use std::sync::Arc;

        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        let file = Arc::<dyn SharedIoBuffer>::from(file);

        // Use very small segment size to create many segments
        let segment_size = 2;
        let mut segmented = SegmentedVec::<PodCollector<u16>>::with_segment_size(segment_size);

        // Create enough data for 500 segments
        let total_elements = 1000;
        for i in 0..total_elements {
            segmented.push(&(i as u16));
        }

        assert_eq!(segmented.len(), total_elements);
        assert_eq!(segmented.segments().len(), total_elements / segment_size); // 500 segments

        let _end_pos = segmented.dump_and_seal(file.clone()).unwrap();
        let loaded_segmented =
            SegmentedVec::<crate::pod_vec::PodView<u16>>::load_sealed(file.clone()).unwrap();

        // Verify many-segment loading works correctly
        assert_eq!(loaded_segmented.len(), total_elements);
        assert_eq!(loaded_segmented.segment_size(), segment_size);
        assert_eq!(
            loaded_segmented.segments().len(),
            total_elements / segment_size
        );

        // Spot check across all segments
        for i in (0..total_elements).step_by(50) {
            assert_eq!(loaded_segmented[i], i as u16);
        }

        // Verify first and last elements
        assert_eq!(loaded_segmented[0], 0);
        assert_eq!(
            loaded_segmented[total_elements - 1],
            (total_elements - 1) as u16
        );
    }

    #[test]
    fn test_segmented_pod_collector_comprehensive_dump_load() {
        use amudai_io::SharedIoBuffer;
        use std::sync::Arc;

        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        let file = Arc::<dyn SharedIoBuffer>::from(file);
        assert_eq!(file.size().unwrap(), 0);

        // Test with different POD types and patterns similar to pod_vec.rs
        let segment_size = 8;
        let mut segmented = SegmentedVec::<PodCollector<u64>>::with_segment_size(segment_size);

        // Add comprehensive test data covering various ranges
        let test_data: Vec<u64> = (0..10000).collect(); // Large dataset like pod_vec.rs
        for &value in &test_data {
            segmented.push(&value);
        }

        assert_eq!(segmented.len(), 10000);
        let expected_segments = (10000 + segment_size - 1) / segment_size; // 1250 segments
        assert_eq!(segmented.segments().len(), expected_segments);

        // Dump and seal
        let _end_pos = segmented.dump_and_seal(file.clone()).unwrap();

        // Load it back as view
        let loaded_segmented =
            SegmentedVec::<crate::pod_vec::PodView<u64>>::load_sealed(file.clone()).unwrap();

        // Comprehensive verification
        assert_eq!(loaded_segmented.len(), 10000);
        assert_eq!(loaded_segmented.segment_size(), segment_size);
        assert_eq!(loaded_segmented.segments().len(), expected_segments);

        // Verify data integrity for all elements
        for (i, &expected) in test_data.iter().enumerate() {
            assert_eq!(loaded_segmented[i], expected, "Mismatch at index {}", i);
        }

        // Verify first and last segments have correct sizes
        assert_eq!(loaded_segmented.segments()[0].len(), segment_size);
        let last_segment_expected_size = if 10000 % segment_size == 0 {
            segment_size
        } else {
            10000 % segment_size
        };
        assert_eq!(
            loaded_segmented.segments()[expected_segments - 1].len(),
            last_segment_expected_size
        );
    }

    #[test]
    fn test_segmented_bytes_collector_comprehensive_dump_load() {
        use amudai_io::SharedIoBuffer;
        use std::sync::Arc;

        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        let file = Arc::<dyn SharedIoBuffer>::from(file);

        // Test with pattern similar to bytes_vec.rs
        let segment_size = 4;
        let mut segmented = SegmentedVec::<BytesCollector>::with_segment_size(segment_size);

        // Create varied test data like bytes_vec.rs test
        let mut test_data = Vec::new();

        // Add various types of byte sequences
        test_data.push(b"hello".as_slice());
        test_data.push(b"world".as_slice());
        test_data.push(b"test".as_slice());
        test_data.push(b"data".as_slice());
        test_data.push(b"".as_slice()); // empty slice
        test_data.push(b"final_entry".as_slice());

        // Add some larger entries like in bytes_vec.rs
        let large_data = vec![b'X'; 1000];
        test_data.push(&large_data);

        let very_large_data = vec![b'Y'; 5000];
        test_data.push(&very_large_data);

        // Add entries with various patterns
        for i in 0..20 {
            let entry = format!("entry_{:04}_with_content", i);
            segmented.push(entry.as_bytes());
        }

        // Add the predefined test data
        for data in &test_data {
            segmented.push(data);
        }

        let original_len = segmented.len();
        assert_eq!(original_len, 28); // 20 generated + 8 predefined

        // Dump and seal
        let _end_pos = segmented.dump_and_seal(file.clone()).unwrap();

        // Load back
        let loaded_segmented =
            SegmentedVec::<crate::bytes_vec::BytesView>::load_sealed(file.clone()).unwrap();

        // Verify loaded data
        assert_eq!(loaded_segmented.len(), original_len);
        assert_eq!(loaded_segmented.segment_size(), segment_size);

        // Verify generated entries
        for i in 0..20 {
            let expected = format!("entry_{:04}_with_content", i);
            assert_eq!(&loaded_segmented[i], expected.as_bytes());
        }

        // Verify predefined entries
        for (i, &expected) in test_data.iter().enumerate() {
            assert_eq!(&loaded_segmented[20 + i], expected);
        }

        // Verify large entries specifically
        assert_eq!(loaded_segmented[26].len(), 1000);
        assert!(loaded_segmented[26].iter().all(|&b| b == b'X'));
        assert_eq!(loaded_segmented[27].len(), 5000);
        assert!(loaded_segmented[27].iter().all(|&b| b == b'Y'));
    }

    #[test]
    fn test_segmented_vec_writer_pod_basic() {
        use crate::pod_vec::PodView;
        use amudai_io::SharedIoBuffer;
        use std::sync::Arc;

        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        let file = Arc::<dyn SharedIoBuffer>::from(file);

        // Create a SegmentedVecWriter with small segment size to test multiple segments
        let segment_size = 4;
        let mut writer =
            SegmentedVecWriter::<PodCollector<i32>>::with_segment_size(segment_size, file.clone());

        // Test initial state
        assert_eq!(writer.len(), 0);
        assert!(writer.is_empty());
        assert_eq!(writer.segment_size(), segment_size);

        // Push test data that will span multiple segments
        let test_data: Vec<i32> = (0..15).collect(); // 15 elements = 4 segments (4,4,4,3)
        for &value in &test_data {
            writer.push(&value).unwrap();
        }

        assert_eq!(writer.len(), 15);
        assert!(!writer.is_empty());

        // Finish and seal the writer
        let _sealed_file = writer.finish_and_seal().unwrap();

        // Load back as SegmentedVec
        let loaded_segmented = SegmentedVec::<PodView<i32>>::load_sealed(file.clone()).unwrap();

        // Verify loaded data
        assert_eq!(loaded_segmented.len(), 15);
        assert_eq!(loaded_segmented.segment_size(), segment_size);
        assert_eq!(loaded_segmented.segments().len(), 4); // 4 segments

        // Verify all elements match
        for (i, &expected) in test_data.iter().enumerate() {
            assert_eq!(loaded_segmented[i], expected);
        }
    }

    #[test]
    fn test_segmented_vec_writer_pod_empty() {
        use crate::pod_vec::PodView;
        use amudai_io::SharedIoBuffer;
        use std::sync::Arc;

        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        let file = Arc::<dyn SharedIoBuffer>::from(file);

        // Create writer and immediately finish without pushing any data
        let writer = SegmentedVecWriter::<PodCollector<u64>>::new(file.clone());

        assert_eq!(writer.len(), 0);
        assert!(writer.is_empty());

        // Finish and seal empty writer
        let _sealed_file = writer.finish_and_seal().unwrap();

        // Load back and verify empty state is preserved
        let loaded_segmented = SegmentedVec::<PodView<u64>>::load_sealed(file.clone()).unwrap();

        assert_eq!(loaded_segmented.len(), 0);
        assert!(loaded_segmented.is_empty());
        assert_eq!(loaded_segmented.segments().len(), 0);
    }

    #[test]
    fn test_segmented_vec_writer_pod_single_segment() {
        use crate::pod_vec::PodView;
        use amudai_io::SharedIoBuffer;
        use std::sync::Arc;

        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        let file = Arc::<dyn SharedIoBuffer>::from(file);

        // Use default segment size and push fewer elements than segment size
        let mut writer = SegmentedVecWriter::<PodCollector<f64>>::new(file.clone());

        let test_data = vec![1.0, 2.5, 3.14159, -42.0, 0.0];
        for &value in &test_data {
            writer.push(&value).unwrap();
        }

        assert_eq!(writer.len(), 5);

        // Finish and seal
        let _sealed_file = writer.finish_and_seal().unwrap();

        // Load back
        let loaded_segmented = SegmentedVec::<PodView<f64>>::load_sealed(file.clone()).unwrap();

        // Should have only one segment since we didn't fill a full segment
        assert_eq!(loaded_segmented.len(), 5);
        assert_eq!(loaded_segmented.segments().len(), 1);

        // Verify all elements
        for (i, &expected) in test_data.iter().enumerate() {
            assert_eq!(loaded_segmented[i], expected);
        }
    }

    #[test]
    fn test_segmented_vec_writer_bytes_basic() {
        use crate::bytes_vec::BytesView;
        use amudai_io::SharedIoBuffer;
        use std::sync::Arc;

        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        let file = Arc::<dyn SharedIoBuffer>::from(file);

        // Create a SegmentedVecWriter with small segment size for multiple segments
        let segment_size = 4;
        let mut writer =
            SegmentedVecWriter::<BytesCollector>::with_segment_size(segment_size, file.clone());

        // Test initial state
        assert_eq!(writer.len(), 0);
        assert!(writer.is_empty());
        assert_eq!(writer.segment_size(), segment_size);

        // Push test data that will span multiple segments
        let test_data = vec![
            b"hello".as_slice(),
            b"world".as_slice(),
            b"test".as_slice(),
            b"data".as_slice(),
            b"".as_slice(), // empty slice
            b"final_entry".as_slice(),
            b"extra".as_slice(),
        ];

        for data in &test_data {
            writer.push(data).unwrap();
        }

        assert_eq!(writer.len(), 7);
        assert!(!writer.is_empty());

        // Finish and seal the writer
        let _sealed_file = writer.finish_and_seal().unwrap();

        // Load back as SegmentedVec
        let loaded_segmented = SegmentedVec::<BytesView>::load_sealed(file.clone()).unwrap();

        // Verify loaded data
        assert_eq!(loaded_segmented.len(), 7);
        assert_eq!(loaded_segmented.segment_size(), segment_size);
        assert_eq!(loaded_segmented.segments().len(), 2); // 7 elements with segment_size 4 = 2 segments (4,3)

        // Verify all elements match
        for (i, &expected) in test_data.iter().enumerate() {
            assert_eq!(&loaded_segmented[i], expected);
        }
    }

    #[test]
    fn test_segmented_vec_writer_bytes_empty() {
        use crate::bytes_vec::BytesView;
        use amudai_io::SharedIoBuffer;
        use std::sync::Arc;

        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        let file = Arc::<dyn SharedIoBuffer>::from(file);

        // Create writer and immediately finish without pushing any data
        let writer = SegmentedVecWriter::<BytesCollector>::new(file.clone());

        assert_eq!(writer.len(), 0);
        assert!(writer.is_empty());

        // Finish and seal empty writer
        let _sealed_file = writer.finish_and_seal().unwrap();

        // Load back and verify empty state is preserved
        let loaded_segmented = SegmentedVec::<BytesView>::load_sealed(file.clone()).unwrap();

        assert_eq!(loaded_segmented.len(), 0);
        assert!(loaded_segmented.is_empty());
        assert_eq!(loaded_segmented.segments().len(), 0);
    }

    #[test]
    fn test_segmented_vec_writer_bytes_large_data() {
        use crate::bytes_vec::BytesView;
        use amudai_io::SharedIoBuffer;
        use std::sync::Arc;

        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        let file = Arc::<dyn SharedIoBuffer>::from(file);

        // Use small segment size to force many segments
        let segment_size = 2;
        let mut writer =
            SegmentedVecWriter::<BytesCollector>::with_segment_size(segment_size, file.clone());

        // Generate larger test data
        let mut test_data = Vec::new();

        // Add some regular entries
        for i in 0..10 {
            test_data.push(format!("entry_{:03}", i).into_bytes());
        }

        // Add some large entries
        test_data.push(vec![b'X'; 1000]); // Large entry
        test_data.push(vec![b'Y'; 5000]); // Very large entry
        test_data.push(b"".to_vec()); // Empty entry
        test_data.push(b"final".to_vec());

        for data in &test_data {
            writer.push(data.as_slice()).unwrap();
        }

        let total_len = test_data.len();
        assert_eq!(writer.len(), total_len);

        // Finish and seal
        let _sealed_file = writer.finish_and_seal().unwrap();

        // Load back
        let loaded_segmented = SegmentedVec::<BytesView>::load_sealed(file.clone()).unwrap();

        // Verify loaded data
        assert_eq!(loaded_segmented.len(), total_len);
        assert_eq!(loaded_segmented.segment_size(), segment_size);

        // With segment_size 2 and 14 elements, we should have 7 segments
        assert_eq!(loaded_segmented.segments().len(), 7);

        // Verify all elements match
        for (i, expected) in test_data.iter().enumerate() {
            assert_eq!(&loaded_segmented[i], expected.as_slice());
        }

        // Verify specific large entries
        assert_eq!(loaded_segmented[10].len(), 1000);
        assert!(loaded_segmented[10].iter().all(|&b| b == b'X'));
        assert_eq!(loaded_segmented[11].len(), 5000);
        assert!(loaded_segmented[11].iter().all(|&b| b == b'Y'));
        assert_eq!(loaded_segmented[12].len(), 0); // Empty entry
        assert_eq!(&loaded_segmented[13], b"final");
    }

    #[test]
    fn test_segmented_vec_writer_mixed_pod_types() {
        use crate::pod_vec::PodView;
        use amudai_io::SharedIoBuffer;
        use std::sync::Arc;

        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file1 = temp_store.allocate_shared_buffer(None).unwrap();
        let file1 = Arc::<dyn SharedIoBuffer>::from(file1);
        let file2 = temp_store.allocate_shared_buffer(None).unwrap();
        let file2 = Arc::<dyn SharedIoBuffer>::from(file2);

        // Test with u8 type
        {
            let mut writer =
                SegmentedVecWriter::<PodCollector<u8>>::with_segment_size(8, file1.clone());
            let test_data: Vec<u8> = (0..20).collect();

            for &value in &test_data {
                writer.push(&value).unwrap();
            }

            let _sealed_file = writer.finish_and_seal().unwrap();
            let loaded = SegmentedVec::<PodView<u8>>::load_sealed(file1.clone()).unwrap();

            assert_eq!(loaded.len(), 20);
            for (i, &expected) in test_data.iter().enumerate() {
                assert_eq!(loaded[i], expected);
            }
        }

        // Test with struct (using a simple POD struct that should work with bytemuck)
        #[derive(Clone, Copy, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
        #[repr(C)]
        struct TestPoint {
            x: f32,
            y: f32,
        }

        {
            let mut writer =
                SegmentedVecWriter::<PodCollector<TestPoint>>::with_segment_size(4, file2.clone());
            let test_points = vec![
                TestPoint { x: 1.0, y: 2.0 },
                TestPoint { x: 3.0, y: 4.0 },
                TestPoint { x: -1.0, y: -2.0 },
                TestPoint { x: 0.0, y: 0.0 },
                TestPoint { x: 100.0, y: 200.0 },
            ];

            for &point in &test_points {
                writer.push(&point).unwrap();
            }

            let _sealed_file = writer.finish_and_seal().unwrap();
            let loaded = SegmentedVec::<PodView<TestPoint>>::load_sealed(file2.clone()).unwrap();

            assert_eq!(loaded.len(), 5);
            assert_eq!(loaded.segments().len(), 2); // 5 elements with segment_size 4 = 2 segments (4,1)

            for (i, &expected) in test_points.iter().enumerate() {
                assert_eq!(loaded[i], expected);
            }
        }
    }
}
