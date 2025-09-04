//! A segment implementation that stores positions as an explicit list.

use std::ops::{Add, Range};

use crate::{
    bit_array::BitArray,
    segment::{
        bits::BitSegment,
        ranges::{RangeSegment, Run},
    },
};

/// A segment implementation that stores positions as an explicit list.
///
/// `ListSegment` is one of several segment representations in the position set data structure,
/// optimized for sparse data where relatively few positions are set within the segment's span.
/// This representation stores actual position values as a sorted vector of 32-bit offsets
/// relative to the segment's starting position.
///
/// # Memory Efficiency
///
/// This representation is most efficient when the number of set positions is small relative
/// to the segment span size. Each position requires 2 bytes of storage. When the number of
/// positions exceeds [`Segment::MAX_LIST_LEN`] (typically 4096 positions for a 64K-position span),
/// the segment should be converted to a [`BitSegment`] representation for better memory efficiency.
///
/// # Position Storage
///
/// - Positions are stored as `u32` values representing offsets from `span.start`
/// - All positions are guaranteed to be within the segment's span
/// - The list maintains sorted order and contains no duplicates
///
/// [`BitSegment`]: crate::segment::bits::BitSegment
/// [`Segment::MAX_LIST_LEN`]: crate::segment::Segment::MAX_LIST_LEN
#[derive(Clone)]
pub struct ListSegment {
    /// The logical range of positions this segment is representing.
    ///
    /// This defines the segment's "responsibility zone" - the contiguous range of
    /// logical positions that this segment must handle, regardless of which positions
    /// are actually set within that range.
    ///
    /// # Invariants
    ///
    /// - `span.start` is a multiple of [`Segment::SPAN`] (1,048,576 positions)
    /// - `span.end - span.start` is usually [`Segment::SPAN`], except for the last segment
    ///   in a position set which may be smaller
    ///
    /// [`Segment::SPAN`]: crate::segment::Segment::SPAN
    span: Range<u64>,

    /// Sorted list of positions stored as offsets relative to `span.start`.
    ///
    /// Each value represents a position that is "set" within this segment,
    /// stored as a `u16` offset from the beginning of the segment's span.
    ///
    /// # Invariants
    ///
    /// - **Sorted**: Values are in strictly ascending order
    /// - **Unique**: No duplicate values (each position appears at most once)
    /// - **Bounded**: All values are `< (span.end - span.start)` to ensure they represent
    ///   valid positions within the segment's span
    /// - **Relative**: Each value represents `(absolute_position - span.start)`
    ///
    /// [`Segment::MAX_LIST_LEN`]: crate::segment::Segment::MAX_LIST_LEN
    values: Vec<u16>,
}

impl ListSegment {
    pub(crate) fn new(span: Range<u64>, values: Vec<u16>) -> ListSegment {
        let segment = ListSegment { span, values };
        segment.check_basic_invariants();

        #[cfg(debug_assertions)]
        segment.check_full_invariants();

        segment
    }

    /// Creates a new empty `ListSegment` for the given span.
    ///
    /// This constructor creates a segment with no pre-allocated capacity for the internal
    /// vector. For better performance when the expected number of positions is known,
    /// consider using [`with_capacity`] instead.
    ///
    /// # Arguments
    ///
    /// * `span` - The logical range of positions this segment will cover. Must be
    ///   aligned according to segment boundaries (typically 1MB spans).
    ///
    /// [`with_capacity`]: Self::with_capacity
    pub fn empty(span: Range<u64>) -> ListSegment {
        Self::with_capacity(span, 0)
    }

    pub fn full(span: Range<u64>) -> ListSegment {
        let len = (span.end - span.start) as usize;
        Self::new(span, (0..len).map(|pos| pos as u16).collect())
    }

    /// Creates a new empty `ListSegment` with pre-allocated capacity.
    ///
    /// This constructor allows specifying an initial capacity for the internal vector
    /// storing relative positions, which can improve performance when the expected
    /// number of positions is known in advance.
    ///
    /// # Arguments
    ///
    /// * `span` - The logical range of positions this segment will cover. Must be
    ///   aligned according to segment boundaries (typically 1MB spans).
    /// * `capacity` - The initial capacity to allocate for the internal vector.
    ///   This should be the expected number of positions to be stored.
    pub fn with_capacity(span: Range<u64>, capacity: usize) -> ListSegment {
        ListSegment {
            span,
            values: Vec::with_capacity(capacity),
        }
    }

    pub fn from_position_slice(span: Range<u64>, positions: &[u64]) -> ListSegment {
        if positions.is_empty() {
            return ListSegment::empty(span);
        }
        let mut values = Vec::with_capacity(positions.len());
        values.extend(positions.iter().map(|&pos| (pos - span.start) as u16));
        ListSegment::new(span, values)
    }

    pub fn from_relative_position_slice(span: Range<u64>, positions: &[u16]) -> ListSegment {
        if positions.is_empty() {
            return ListSegment::empty(span);
        }
        ListSegment::new(span, positions.to_vec())
    }

    /// Creates a new `ListSegment` from an iterator of absolute positions.
    ///
    /// This method constructs a segment by converting absolute positions to relative
    /// positions within the segment's span. The positions are stored as 32-bit offsets
    /// relative to `span.start`.
    ///
    /// # Arguments
    ///
    /// * `span` - The logical range this segment covers. Positions outside this range
    ///   will cause debug assertions to fail.
    /// * `positions` - An iterator yielding absolute positions (u64) that should be
    ///   included in this segment. Positions must be:
    ///   - Within the `span` range
    ///   - Sorted in ascending order
    ///   - Unique
    ///
    /// # Panics
    ///
    /// - If the number of positions exceeds the span length
    /// - If any relative position exceeds the span length when converted to u32
    pub fn from_positions(span: Range<u64>, positions: impl Iterator<Item = u64>) -> ListSegment {
        let span_size = (span.end - span.start) as usize;
        let (lower_bound, upper_bound) = positions.size_hint();
        let capacity = upper_bound.unwrap_or(lower_bound).min(span_size);
        let mut values = Vec::<u16>::with_capacity(capacity);
        values.extend(positions.map(|pos| (pos - span.start) as u16));
        ListSegment::new(span, values)
    }

    pub fn from_positions_with_count(
        span: Range<u64>,
        positions: impl Iterator<Item = u64>,
        count: usize,
    ) -> ListSegment {
        let span_size = (span.end - span.start) as usize;
        assert!(count <= span_size);
        let mut values = Vec::<u16>::with_capacity(count);
        values.extend(positions.map(|pos| (pos - span.start) as u16));
        assert_eq!(values.len(), values.capacity());
        ListSegment::new(span, values)
    }

    /// Creates a new `ListSegment` from an iterator of ranges.
    ///
    /// This method constructs a segment by flattening the provided ranges into individual
    /// positions and storing them as relative offsets within the segment's span. Each range
    /// is expanded to include all positions from `range.start` to `range.end - 1`.
    ///
    /// # Arguments
    ///
    /// * `span` - The logical range this segment covers. All positions from the input
    ///   ranges must fall within this span, or debug assertions will fail.
    /// * `ranges` - An iterator yielding ranges (`Range<u64>`) that should be included
    ///   in this segment. The ranges must be:
    ///   - Non-overlapping and sorted in ascending order
    ///   - Contain positions only within the `span` range
    ///   - When flattened, produce a sorted sequence of unique positions
    ///
    /// # Panics
    ///
    /// - If the total number of positions from all ranges exceeds the span length
    /// - If any position from the flattened ranges is outside the segment's span
    pub fn from_ranges(span: Range<u64>, ranges: impl Iterator<Item = Range<u64>>) -> ListSegment {
        Self::from_positions(span, ranges.flatten())
    }

    pub fn from_runs(span: Range<u64>, runs: &[Run]) -> ListSegment {
        let count = runs.iter().map(|r| r.len()).sum::<usize>();
        assert!(count <= (span.end - span.start) as usize);
        let mut values = Vec::<u16>::with_capacity(count);
        values.extend(runs.iter().flat_map(|r| r.first..=r.last));
        assert_eq!(values.len(), values.capacity());
        ListSegment::new(span, values)
    }

    /// Returns a copy of the segment's span.
    ///
    /// The span defines the logical range of positions that this segment is responsible
    /// for handling, regardless of which positions are actually set within that range.
    ///
    /// # Returns
    ///
    /// A `Range<u64>` representing the segment's logical address space. The range
    /// is typically aligned to segment boundaries (e.g., 1MB spans), with `start`
    /// being a multiple of the segment size.
    #[inline]
    pub fn span(&self) -> Range<u64> {
        self.span.clone()
    }

    /// Returns the number of set positions stored in this segment.
    ///
    /// This is the length of the internal list of relative positions and
    /// is equivalent to `self.relative_positions().len()`.
    ///
    /// # Returns
    ///
    /// The count of positions currently stored in the segment.
    ///
    /// # Performance
    ///
    /// O(1)
    #[inline]
    pub fn count_positions(&self) -> usize {
        self.values.len()
    }

    /// Counts the number of contiguous runs of set positions.
    ///
    /// A run is a maximal sequence of consecutive relative positions. For example,
    /// values `[0,1,2,  4,5,  9]` contain 3 runs: `[0-2]`, `[4-5]`, and `[9]`.
    #[inline]
    pub fn count_runs(&self) -> usize {
        count_runs(&self.values)
    }

    /// Adds a position to the segment.
    ///
    /// This method appends a position to the segment's internal list, converting it
    /// to a relative position (offset from `span.start`). The position must be within
    /// the segment's span and greater than or equal to any previously added position
    /// to maintain the sorted order invariant.
    ///
    /// # Arguments
    ///
    /// * `pos` - The absolute position to add. Must be within the segment's span
    ///   and >= any previously added position.
    ///
    /// # Panics
    ///
    /// - In debug builds, panics if `pos` is outside the segment's span
    /// - In debug builds, panics if `pos` is less than the last added position
    ///
    /// # Performance
    ///
    /// This is an O(1) operation as it appends to the end of the vector, relying
    /// on the caller to maintain sorted order.
    #[inline]
    pub fn push(&mut self, pos: u64) {
        debug_assert!(
            self.span.contains(&pos),
            "Position {} is outside segment span {:?}",
            pos,
            self.span
        );

        let rel_pos = (pos - self.span.start) as u16;

        match self.values.last() {
            Some(&last_pos) if rel_pos <= last_pos => {
                debug_assert_eq!(
                    rel_pos, last_pos,
                    "Attempted to insert position {rel_pos} which is less than last position {last_pos}"
                );
            }
            _ => self.values.push(rel_pos),
        }
    }

    /// Returns all relative positions stored in this segment.
    ///
    /// The returned slice contains positions as 32-bit offsets from the segment's
    /// starting position (`span.start`). These values are guaranteed to be:
    /// - Sorted in ascending order
    /// - Unique (no duplicates)
    /// - Less than the segment's span length
    ///
    /// # Returns
    ///
    /// A slice of `u32` values representing relative positions within the segment.
    /// To convert to absolute positions, add `span().start` to each value.
    pub fn relative_positions(&self) -> &[u16] {
        &self.values
    }

    /// Returns relative positions that fall within the specified absolute range.
    ///
    /// This method efficiently finds all positions stored in this segment that fall
    /// within the given absolute range, returning them as relative positions (offsets
    /// from `span.start`). The input range is automatically clamped to the segment's
    /// span, so it's safe to pass ranges that extend beyond the segment boundaries.
    ///
    /// # Arguments
    ///
    /// * `range` - The absolute range to query. Only positions within both this range
    ///   and the segment's span will be included in the result.
    ///
    /// # Returns
    ///
    /// A slice of `u32` values representing relative positions within the specified
    /// range. The slice is empty if no positions fall within the range or if the
    /// range doesn't overlap with the segment's span.
    ///
    /// # Performance
    ///
    /// This method uses binary search to efficiently locate the relevant positions,
    /// providing O(log n) performance where n is the number of positions in the segment.
    pub fn relative_positions_within(&self, range: Range<u64>) -> &[u16] {
        // Convert absolute range to relative range within this segment
        let segment_start = self.span.start;
        let segment_end = self.span.end;

        // Clamp the input range to the segment's span
        let clamped_start = range.start.max(segment_start);
        let clamped_end = range.end.min(segment_end);

        // If the clamped range is empty or invalid, return empty slice
        if clamped_start >= clamped_end {
            return &[];
        }

        if clamped_start == segment_start && clamped_end == segment_end {
            return &self.values;
        }

        // Convert to relative positions
        let rel_start = (clamped_start - segment_start) as u16;
        let rel_end = (clamped_end - segment_start) as u16;

        // Find the range of indices in self.values that fall within [rel_start, rel_end)
        let start_idx = if clamped_start == self.span.start {
            0
        } else {
            self.values
                .binary_search(&rel_start)
                .unwrap_or_else(|idx| idx)
        };

        // Search for rel_end only within the remaining slice [start_idx..]
        let end_idx = if clamped_end == self.span.end {
            self.values.len()
        } else {
            self.values[start_idx..]
                .binary_search(&rel_end)
                .unwrap_or_else(|idx| idx)
                + start_idx
        };

        &self.values[start_idx..end_idx]
    }

    /// Returns an iterator over all absolute positions in this segment.
    ///
    /// The iterator converts the internally stored relative positions back to absolute
    /// positions by adding the segment's starting position. The positions are yielded
    /// in ascending order.
    ///
    /// # Returns
    ///
    /// A [`PositionsIter`] that yields `u64` absolute positions. The iterator implements
    /// [`ExactSizeIterator`] and [`DoubleEndedIterator`] for efficient traversal in
    /// both directions.
    pub fn positions(&self) -> PositionsIter<'_> {
        PositionsIter::new(&self.values, self.span.start)
    }

    /// Returns an iterator over absolute positions within the specified range.
    ///
    /// This method combines the functionality of [`relative_positions_within`] and
    /// [`positions`], efficiently finding positions within the given range and
    /// converting them to absolute positions. The input range is automatically
    /// clamped to the segment's span.
    ///
    /// # Arguments
    ///
    /// * `range` - The absolute range to query. Only positions within both this range
    ///   and the segment's span will be included in the iterator.
    ///
    /// # Returns
    ///
    /// A [`PositionsIter`] that yields `u64` absolute positions within the specified
    /// range. The iterator is empty if no positions fall within the range.
    ///
    /// # Performance
    ///
    /// Uses binary search to efficiently locate relevant positions, providing
    /// O(log n) setup time where n is the number of positions in the segment.
    ///
    /// [`relative_positions_within`]: Self::relative_positions_within
    /// [`positions`]: Self::positions
    pub fn positions_within(&self, range: Range<u64>) -> PositionsIter<'_> {
        PositionsIter::new(self.relative_positions_within(range), self.span.start)
    }

    /// Returns an iterator over maximal contiguous ranges of set positions.
    ///
    /// Consecutive positions in this segment are coalesced into half-open
    /// absolute ranges `[start, end)` and yielded in ascending order.
    ///
    /// # Returns
    ///
    /// A [`RangesIter`] that yields `Range<u64>` values. The iterator is empty
    /// if the segment contains no positions.
    ///
    /// # Performance
    ///
    /// O(n), where n is the number of positions in the segment.
    pub fn ranges(&self) -> RangesIter<'_> {
        RangesIter::new(&self.values, self.span.start)
    }

    /// Returns an iterator over contiguous ranges within the specified absolute range.
    ///
    /// This method finds all positions in the given range (clamped to the segment's
    /// span) and coalesces consecutive positions into half-open absolute ranges
    /// `[start, end)`. If a run crosses the boundary of the input range, only the
    /// in-range portion is returned.
    ///
    /// # Arguments
    ///
    /// * `range` - The absolute range to query. Only positions within both this
    ///   range and the segment's span are considered.
    ///
    /// # Returns
    ///
    /// A [`RangesIter`] that yields `Range<u64>` values within the specified
    /// range. The iterator is empty if no positions fall within the range.
    ///
    /// # Performance
    ///
    /// Uses binary search to locate the relevant slice (O(log n) setup), then
    /// scans that slice once to generate ranges.
    pub fn ranges_within(&self, range: Range<u64>) -> RangesIter<'_> {
        RangesIter::new(self.relative_positions_within(range), self.span.start)
    }

    /// Returns the smallest absolute position stored in this segment.
    ///
    /// # Returns
    ///
    /// - `Some(pos)` if the segment contains at least one position
    /// - `None` if the segment is empty
    ///
    /// # Performance
    ///
    /// O(1)
    #[inline]
    pub fn min_pos(&self) -> Option<u64> {
        self.values.first().map(|v| *v as u64 + self.span.start)
    }

    /// Returns the largest absolute position stored in this segment.
    ///
    /// # Returns
    ///
    /// - `Some(pos)` if the segment contains at least one position
    /// - `None` if the segment is empty
    ///
    /// # Performance
    ///
    /// O(1)
    #[inline]
    pub fn max_pos(&self) -> Option<u64> {
        self.values.last().map(|v| *v as u64 + self.span.start)
    }

    /// Checks whether the specified position is contained in this segment.
    ///
    /// This method uses binary search to determine if the position is actually
    /// set within the segment.
    ///
    /// # Arguments
    ///
    /// * `pos` - The absolute position to check for membership.
    ///
    /// # Returns
    ///
    /// `true` if the position is both within the segment's span and is set in the
    /// segment, `false` otherwise.
    ///
    /// # Performance
    ///
    /// This is an O(log n) operation where n is the number of positions stored in
    /// the segment, due to the binary search on the sorted values.
    pub fn contains(&self, pos: u64) -> bool {
        debug_assert!(self.span.contains(&pos));
        let rel_pos = (pos - self.span.start) as u16;
        self.values.binary_search(&rel_pos).is_ok()
    }

    /// Calls `f(rank, pos)` for every set position in ascending order.
    ///
    /// - `rank` is the 0-based index among set positions within this segment.
    /// - `pos` is the absolute position (i.e., `self.span.start + relative_index`).
    ///
    /// Equivalent to:
    /// `for (i, p) in self.positions().enumerate() { f(i, p) }`
    ///
    /// Runs in O(k) where k = `self.count_positions()`, and does not allocate.
    pub fn for_each_position(&self, mut f: impl FnMut(usize, u64)) -> usize {
        let base = self.span.start;
        for (i, &rel) in self.values.iter().enumerate() {
            f(i, base + rel as u64);
        }
        self.values.len()
    }

    /// Computes the union of two segments (set of positions present in either).
    ///
    /// Panics if `other.span != self.span`.
    pub fn union(&self, other: &ListSegment) -> ListSegment {
        assert_eq!(
            self.span, other.span,
            "ListSegment spans must match for union: left={:?}, right={:?}",
            self.span, other.span
        );

        let span_len = (self.span.end - self.span.start) as usize;
        let mut out = Vec::with_capacity((self.values.len() + other.values.len()).min(span_len));

        let mut i = 0;
        let mut j = 0;

        while i < self.values.len() && j < other.values.len() {
            match self.values[i].cmp(&other.values[j]) {
                std::cmp::Ordering::Equal => {
                    out.push(self.values[i]);
                    i += 1;
                    j += 1;
                }
                std::cmp::Ordering::Less => {
                    out.push(self.values[i]);
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    out.push(other.values[j]);
                    j += 1;
                }
            }
        }

        if i < self.values.len() {
            out.extend_from_slice(&self.values[i..]);
        } else if j < other.values.len() {
            out.extend_from_slice(&other.values[j..]);
        }

        out.shrink_to_fit();
        ListSegment::new(self.span(), out)
    }

    /// In-place union with `other`.
    pub fn union_with(&mut self, other: &ListSegment) {
        assert_eq!(
            self.span, other.span,
            "ListSegment spans must match for union_with: left={:?}, right={:?}",
            self.span, other.span
        );

        if other.values.is_empty() {
            return;
        }

        if self.values.is_empty() {
            self.values = other.values.clone();
            return;
        }

        let span_len = (self.span.end - self.span.start) as usize;
        if self.values.len() == span_len {
            return;
        }

        if other.values.len() == span_len {
            self.values = other.values.clone();
            return;
        }

        let res = self.union(other);
        self.values = res.values;
    }

    /// Computes the intersection of two segments (positions present in both).
    ///
    /// Panics if `other.span != self.span`.
    pub fn intersect(&self, other: &ListSegment) -> ListSegment {
        assert_eq!(
            self.span, other.span,
            "ListSegment spans must match for intersect: left={:?}, right={:?}",
            self.span, other.span
        );

        // Intersection cannot have more elements than the smaller input.
        let mut out = Vec::with_capacity(self.values.len().min(other.values.len()));

        let mut i = 0;
        let mut j = 0;

        while i < self.values.len() && j < other.values.len() {
            match self.values[i].cmp(&other.values[j]) {
                std::cmp::Ordering::Equal => {
                    out.push(self.values[i]);
                    i += 1;
                    j += 1;
                }
                std::cmp::Ordering::Less => {
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    j += 1;
                }
            }
        }

        out.shrink_to_fit();
        ListSegment::new(self.span(), out)
    }

    /// In-place intersection with `other`.
    pub fn intersect_with(&mut self, other: &ListSegment) {
        assert_eq!(
            self.span, other.span,
            "ListSegment spans must match for intersect_with: left={:?}, right={:?}",
            self.span, other.span
        );

        if self.values.is_empty() {
            return;
        }

        if other.values.is_empty() {
            self.values = Vec::new();
            return;
        }

        let span_len = (self.span.end - self.span.start) as usize;

        if other.values.len() == span_len {
            return;
        }

        if self.values.len() == span_len {
            let n = other.values.len();
            self.values[..n].copy_from_slice(&other.values);
            self.values.truncate(n);
            return;
        }

        let mut write = 0usize;
        let mut i = 0usize;
        let mut j = 0usize;
        let a_len = self.values.len();
        let b_len = other.values.len();

        while i < a_len && j < b_len {
            use std::cmp::Ordering;
            match self.values[i].cmp(&other.values[j]) {
                Ordering::Equal => {
                    self.values[write] = self.values[i];
                    write += 1;
                    i += 1;
                    j += 1;
                }
                Ordering::Less => {
                    i += 1;
                }
                Ordering::Greater => {
                    j += 1;
                }
            }
        }

        self.values.truncate(write);
    }

    /// Checks if this segment is equal to another list segment.
    ///
    /// Two list segments are considered equal if they have the same span and
    /// represent the same set of positions.
    ///
    /// # Arguments
    ///
    /// * `other` - The list segment to compare with
    ///
    /// # Returns
    ///
    /// `true` if the segments represent the same set of positions within the same span,
    /// `false` otherwise.
    ///
    pub fn is_equal_to(&self, other: &ListSegment) -> bool {
        self.span == other.span && self.values == other.values
    }

    /// Computes the complement (negation) within this segment's span.
    ///
    /// Returns all positions in the span that are NOT present in this segment.
    pub fn complement(&self) -> ListSegment {
        let span_len = (self.span.end - self.span.start) as u32;

        // The complement size is at most span_len - self.values.len()
        let mut out = Vec::with_capacity((span_len as usize).saturating_sub(self.values.len()));

        // Emit gaps between consecutive set positions
        let mut cur: u32 = 0;
        for &v in &self.values {
            let v = v as u32;
            if cur < v {
                // push all values in [cur, v)
                out.extend(cur as u16..=(v - 1) as u16);
            }
            // skip v
            cur = v.saturating_add(1);
        }
        if cur < span_len {
            out.extend(cur as u16..=(span_len - 1) as u16);
        }

        ListSegment {
            span: self.span.clone(),
            values: out,
        }
    }

    /// Computes the complement (negation) within this segment's span.
    ///
    /// Returns all positions in the span that are NOT present in this segment,
    /// as Bits segment.
    pub fn complement_as_bits(&self) -> BitSegment {
        let span_len = (self.span.end - self.span.start) as usize;
        let mut bits = BitArray::full(span_len);
        for &pos in &self.values {
            bits.reset(pos as usize);
        }
        BitSegment::new(self.span(), bits)
    }

    /// Converts this list segment into a bitmap-based segment.
    pub fn to_bits(&self) -> BitSegment {
        BitSegment::from_positions(self.span(), self.positions())
    }

    /// Converts this list segment into a range-based segment.
    pub fn to_ranges(&self) -> RangeSegment {
        let count = self.count_runs();
        let mut runs = Vec::with_capacity(count);
        runs.extend(self.ranges().map(|r| Run::translated(self.span.start, r)));
        assert_eq!(runs.len(), runs.capacity());
        RangeSegment::new(self.span(), runs)
    }

    pub fn compact(&mut self) {
        self.values.shrink_to_fit();
    }

    /// Returns the number of heap-allocated bytes used by this segment.
    ///
    /// This accounts only for the heap storage of the underlying `Vec`
    /// that backs the list. It does not include the size of the `ListSegment`
    /// struct itself, the `span` range, or any allocator bookkeeping overhead.
    pub fn heap_size_bytes(&self) -> usize {
        self.values.capacity() * std::mem::size_of::<u16>()
    }

    pub fn check_basic_invariants(&self) {
        if self.values.is_empty() {
            return;
        }
        let span_size = (self.span.end - self.span.start) as usize;
        assert!(self.values.len() <= span_size);
        let first = *self.values.first().unwrap();
        let last = *self.values.last().unwrap();
        assert!(self.values.len() == 1 || first < last);
        assert!((last as usize) < span_size);
    }

    pub fn check_full_invariants(&self) {
        if self.values.is_empty() {
            return;
        }
        self.check_basic_invariants();
        if !is_strictly_increasing(&self.values) {
            dbg!(&self.values);
        }
        assert!(is_strictly_increasing(&self.values));
    }
}

/// Iterator over absolute positions in a `ListSegment`.
///
/// This iterator converts relative positions (stored as `u32` offsets) back to
/// absolute positions by adding the segment's starting position.
#[derive(Clone)]
pub struct PositionsIter<'a> {
    /// Iterator over the relative positions stored in the segment
    values_iter: std::slice::Iter<'a, u16>,
    /// The starting position of the segment's span, used to convert relative positions to absolute
    span_start: u64,
}

impl<'a> PositionsIter<'a> {
    /// Creates a new iterator over the positions in a `ListSegment`.
    ///
    /// # Arguments
    ///
    /// * `values` - Slice of relative positions (u32 offsets from span start)
    /// * `span_start` - The absolute starting position of the segment's span
    fn new(values: &'a [u16], span_start: u64) -> Self {
        Self {
            values_iter: values.iter(),
            span_start,
        }
    }
}

impl<'a> Iterator for PositionsIter<'a> {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.values_iter
            .next()
            .map(|&rel_pos| rel_pos as u64 + self.span_start)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.values_iter.size_hint()
    }
}

impl<'a> ExactSizeIterator for PositionsIter<'a> {
    #[inline]
    fn len(&self) -> usize {
        self.values_iter.len()
    }
}

impl<'a> DoubleEndedIterator for PositionsIter<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.values_iter
            .next_back()
            .map(|&rel_pos| rel_pos as u64 + self.span_start)
    }
}

/// Iterator over contiguous absolute ranges in a `ListSegment`.
#[derive(Clone)]
pub struct RangesIter<'a> {
    values: &'a [u16],
    base: u64,
    index: usize,
}

impl<'a> RangesIter<'a> {
    #[inline]
    fn new(values: &'a [u16], base: u64) -> Self {
        Self {
            values,
            base,
            index: 0,
        }
    }
}

impl<'a> Iterator for RangesIter<'a> {
    type Item = Range<u64>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let len = self.values.len();
        if self.index >= len {
            return None;
        }
        let start_rel = self.values[self.index];
        self.index += 1;

        // Extend forward while consecutive
        while self.index < len && self.values[self.index] - self.values[self.index - 1] == 1 {
            self.index += 1;
        }

        let end_rel = self.values[self.index - 1] as u64 + 1;
        let base = self.base;
        Some((base + start_rel as u64)..(base + end_rel))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        // At most one range per remaining element
        let remaining = self.values.len().saturating_sub(self.index);
        (0, Some(remaining))
    }
}

impl std::ops::BitOr for &ListSegment {
    type Output = ListSegment;
    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

impl std::ops::BitAnd for &ListSegment {
    type Output = ListSegment;
    fn bitand(self, rhs: Self) -> Self::Output {
        self.intersect(rhs)
    }
}

impl std::ops::Not for &ListSegment {
    type Output = ListSegment;
    fn not(self) -> Self::Output {
        self.complement()
    }
}

#[inline]
pub fn count_runs<T>(values: &[T]) -> usize
where
    T: Copy + PartialOrd + Add<T, Output = T> + From<u8>,
{
    if values.is_empty() {
        return 0;
    }
    let mut count = 1;
    let one = T::from(1u8);
    let len = values.len();
    for i in 1..len {
        count += (values[i] != values[i - 1] + one) as usize;
    }
    count
}

/// Returns true if the slice is in strictly increasing order.
///
/// This is equivalent to “sorted ascending with no duplicates”:
/// for every consecutive pair `(a[i], a[i+1])`, it requires `a[i] < a[i+1]`.
///
/// Notes:
/// - For types with only a partial order (e.g., floating-point with NaN),
///   any incomparable or non-increasing pair will cause this to return `false`.
/// - Runs in O(n) time and uses a chunked scan to help the optimizer vectorize
///   comparisons while still checking boundaries between chunks.
pub fn is_strictly_increasing<T>(slice: &[T]) -> bool
where
    T: PartialOrd,
{
    const CHUNK_SIZE: usize = 33;
    if slice.len() < CHUNK_SIZE {
        return slice.windows(2).all(|w| w[0] < w[1]);
    }
    let mut i = 0;
    while i < slice.len() - CHUNK_SIZE {
        let chunk = &slice[i..i + CHUNK_SIZE];
        if !chunk.windows(2).fold(true, |acc, w| acc & (w[0] < w[1])) {
            return false;
        }
        i += CHUNK_SIZE - 1;
    }
    slice[i..].windows(2).all(|w| w[0] < w[1])
}
