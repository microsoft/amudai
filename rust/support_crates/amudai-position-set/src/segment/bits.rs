//! A segment implementation that stores positions in a compact bitset.

use std::ops::Range;

use crate::{
    bit_array::{BitArray, BitArrayIter, BitArrayRangesIter},
    segment::{
        list::ListSegment,
        ranges::{RangeSegment, Run},
    },
};

/// A segment implementation that stores positions in a compact bitset.
///
/// `BitSegment` represents a segment's span using one bit per logical position.
/// It's efficient when the number of set positions is large or when arbitrary
/// updates/queries over ranges are common.
#[derive(Clone)]
pub struct BitSegment {
    /// The logical range of positions this segment represents.
    span: Range<u64>,
    /// A bit per position within `span`, stored as a `BitArray` of length `span.len()`.
    bits: BitArray,
}

impl BitSegment {
    pub fn new(span: Range<u64>, bits: BitArray) -> BitSegment {
        let span_size = (span.end - span.start) as usize;
        assert_eq!(span_size, bits.len());
        BitSegment { span, bits }
    }

    /// Creates a new empty `BitSegment` for the given span.
    pub fn empty(span: Range<u64>) -> BitSegment {
        let len = (span.end - span.start) as usize;
        let bits = BitArray::empty(len);
        BitSegment { span, bits }
    }

    /// Creates a new full `BitSegment` (all positions set) for the given span.
    pub fn full(span: Range<u64>) -> BitSegment {
        let len = (span.end - span.start) as usize;
        let bits = BitArray::full(len);
        BitSegment { span, bits }
    }

    /// Creates a new `BitSegment` from an iterator of absolute positions.
    pub fn from_positions(span: Range<u64>, positions: impl Iterator<Item = u64>) -> BitSegment {
        let len = (span.end - span.start) as usize;
        let start = span.start;
        let rel_positions = positions.map(|pos| {
            debug_assert!(
                span.contains(&pos),
                "Position {pos} is outside segment span {span:?}"
            );
            (pos - start) as usize
        });
        let bits = BitArray::from_positions(rel_positions, len);
        BitSegment::new(span, bits)
    }

    /// Creates a new `BitSegment` from an iterator of absolute ranges.
    pub fn from_ranges(span: Range<u64>, ranges: impl Iterator<Item = Range<u64>>) -> BitSegment {
        let len = (span.end - span.start) as usize;
        let start = span.start;
        let rel_ranges = ranges.map(|r| ((r.start - start) as usize)..((r.end - start) as usize));
        let bits = BitArray::from_ranges(rel_ranges, len);
        BitSegment::new(span, bits)
    }

    pub fn from_runs(span: Range<u64>, runs: impl Iterator<Item = Run>) -> BitSegment {
        let len = (span.end - span.start) as usize;
        let bits = BitArray::from_ranges(runs.map(|r| r.as_range()), len);
        BitSegment::new(span, bits)
    }

    pub fn from_relative_position_slice(span: Range<u64>, positions: &[u16]) -> BitSegment {
        let len = (span.end - span.start) as usize;
        let bits = BitArray::from_positions(positions.iter().map(|&pos| pos as usize), len);
        BitSegment::new(span, bits)
    }

    /// Returns a copy of the segment's span.
    #[inline]
    pub fn span(&self) -> Range<u64> {
        self.span.clone()
    }

    /// Returns the number of set positions in this segment.
    ///
    /// This is the population count (number of 1 bits) of the underlying
    /// `BitArray` backing this segment.
    pub fn count_positions(&self) -> usize {
        self.bits.count_ones()
    }

    /// Counts the number of contiguous runs of set positions.
    ///
    /// A run is a maximal sequence of consecutive set bit indices. For example,
    /// set bits at indices `[0,1,2,  4,5,  9]` contain 3 runs: `[0-2]`, `[4-5]`, and `[9]`.
    pub fn count_runs(&self) -> usize {
        self.bits.count_runs()
    }

    /// Sets a position within the segment.
    #[inline]
    pub fn set(&mut self, pos: u64) {
        debug_assert!(
            self.span.contains(&pos),
            "Position {} is outside segment span {:?}",
            pos,
            self.span
        );
        let idx = (pos - self.span.start) as usize;
        self.bits.set(idx);
    }

    /// Checks whether the specified absolute position is set in this segment.
    #[inline]
    pub fn contains(&self, pos: u64) -> bool {
        debug_assert!(self.span.contains(&pos));
        let idx = (pos - self.span.start) as usize;
        self.bits.contains(idx)
    }

    /// Returns an iterator over the relative positions (bit indices) that are set.
    #[inline]
    pub fn relative_positions(&self) -> BitArrayIter<'_> {
        self.bits.iter()
    }

    /// Returns an iterator over the relative positions that fall within the given absolute range.
    #[inline]
    pub fn relative_positions_within(&self, range: Range<u64>) -> BitArrayIter<'_> {
        // Clamp to span
        let start = range.start.max(self.span.start);
        let end = range.end.min(self.span.end);
        if start >= end {
            return BitArrayIter::empty();
        }
        let rel = ((start - self.span.start) as usize)..((end - self.span.start) as usize);
        self.bits.iter_within(rel)
    }

    /// Returns an iterator over absolute positions in ascending order.
    pub fn positions(&self) -> PositionsIter<'_> {
        PositionsIter::new(self.bits.iter(), self.span.start)
    }

    /// Returns an iterator over absolute positions within the specified absolute range.
    pub fn positions_within(&self, range: Range<u64>) -> PositionsIter<'_> {
        // Clamp to span
        let start = range.start.max(self.span.start);
        let end = range.end.min(self.span.end);
        if start >= end {
            return PositionsIter::empty();
        }
        let rel = ((start - self.span.start) as usize)..((end - self.span.start) as usize);
        PositionsIter::new(self.bits.iter_within(rel), self.span.start)
    }

    /// Returns an iterator over absolute contiguous ranges of set positions.
    pub fn ranges(&self) -> RangesIter<'_> {
        RangesIter::new(self.bits.ranges_iter(), self.span.start)
    }

    /// Returns an iterator over absolute contiguous ranges of set positions
    /// within the given absolute range.
    pub fn ranges_within(&self, range: Range<u64>) -> RangesIter<'_> {
        // Clamp to span
        let start = range.start.max(self.span.start);
        let end = range.end.min(self.span.end);
        if start >= end {
            return RangesIter::empty();
        }
        let rel = ((start - self.span.start) as usize)..((end - self.span.start) as usize);
        RangesIter::new(self.bits.ranges_iter_within(rel), self.span.start)
    }

    /// Computes the union of two segments (set of positions present in either).
    ///
    /// Panics if `other.span != self.span`.
    pub fn union(&self, other: &BitSegment) -> BitSegment {
        assert_eq!(
            self.span, other.span,
            "BitSegment spans must match for union: left={:?}, right={:?}",
            self.span, other.span
        );
        let bits = &self.bits | &other.bits;
        BitSegment {
            span: self.span.clone(),
            bits,
        }
    }

    /// In-place union (bitwise OR) with `other`.
    pub fn union_with(&mut self, other: &BitSegment) {
        assert_eq!(
            self.span, other.span,
            "BitSegment spans must match for union: left={:?}, right={:?}",
            self.span, other.span
        );
        self.bits |= &other.bits;
    }

    /// Computes the intersection of two segments (positions present in both).
    ///
    /// Panics if `other.span != self.span`.
    pub fn intersect(&self, other: &BitSegment) -> BitSegment {
        assert_eq!(
            self.span, other.span,
            "BitSegment spans must match for intersect: left={:?}, right={:?}",
            self.span, other.span
        );
        let bits = &self.bits & &other.bits;
        BitSegment {
            span: self.span.clone(),
            bits,
        }
    }

    /// In-place intersection (bitwise AND) with `other`.
    pub fn intersect_with(&mut self, other: &BitSegment) {
        assert_eq!(
            self.span, other.span,
            "BitSegment spans must match for intersect: left={:?}, right={:?}",
            self.span, other.span
        );
        self.bits &= &other.bits;
    }

    /// Computes the complement (negation) within this segment's span.
    pub fn complement(&self) -> BitSegment {
        let bits = !&self.bits;
        BitSegment {
            span: self.span.clone(),
            bits,
        }
    }

    /// In-place negation.
    pub fn complement_in_place(&mut self) {
        self.bits.negate();
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
    ///
    /// # Returns
    ///
    /// The number of positions (total rank).
    pub fn for_each_position(&self, mut f: impl FnMut(usize, u64)) -> usize {
        let base = self.span.start;
        self.bits.for_each_set_bit(|i, pos| {
            f(i, pos + base);
        })
    }

    /// Converts this bitset segment into a list-based segment.
    ///
    /// Builds and returns a `ListSegment` containing the absolute positions of all set bits
    /// in ascending order, preserving the same `span` as `self`. The method pre-allocates
    /// capacity equal to the number of set positions to minimize reallocations and runs in
    /// time proportional to the number of set positions.
    pub fn to_list(&self) -> ListSegment {
        // Convert set bit positions (relative) to absolute positions and build a ListSegment.
        let count = self.count_positions();
        let mut values = Vec::with_capacity(count);
        values.extend(self.relative_positions().map(|pos| pos as u16));
        assert_eq!(values.capacity(), values.len());
        ListSegment::new(self.span(), values)
    }

    /// Converts this bitset segment into a range-based segment.
    pub fn to_ranges(&self) -> RangeSegment {
        let count = self.count_runs();
        let mut runs = Vec::<Run>::with_capacity(count);
        runs.extend(self.bits.ranges_iter().map(Run::from));
        assert_eq!(runs.capacity(), runs.len());
        RangeSegment::new(self.span(), runs)
    }

    pub fn compact(&mut self) {}

    /// Returns the number of heap-allocated bytes used by this segment.
    ///
    /// This accounts only for the heap storage of the underlying `BitArray`
    /// that backs the bitset (i.e., the boxed slice of `u64` words). It does
    /// not include the size of the `BitSegment` struct itself, the `span`
    /// range, or any allocator bookkeeping overhead.
    ///
    /// Equivalent to: `self.bits.heap_size_bytes()`.
    pub fn heap_size_bytes(&self) -> usize {
        self.bits.heap_size_bytes()
    }

    pub fn check_basic_invariants(&self) {
        let len = (self.span.end - self.span.start) as usize;
        assert_eq!(self.bits.len(), len);
    }

    pub fn check_full_invariants(&self) {
        self.check_basic_invariants();
    }
}

/// Iterator over absolute positions in a `BitSegment`.
#[derive(Clone)]
pub struct PositionsIter<'a> {
    iter: BitArrayIter<'a>,
    span_start: u64,
}

impl<'a> PositionsIter<'a> {
    fn new(iter: BitArrayIter<'a>, span_start: u64) -> Self {
        Self { iter, span_start }
    }

    fn empty() -> Self {
        Self::new(BitArrayIter::empty(), 0)
    }
}

impl<'a> Iterator for PositionsIter<'a> {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|rel| rel as u64 + self.span_start)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

/// Iterator over absolute contiguous ranges in a `BitSegment`.
#[derive(Clone)]
pub struct RangesIter<'a> {
    iter: BitArrayRangesIter<'a>,
    span_start: u64,
}

impl<'a> RangesIter<'a> {
    fn new(iter: BitArrayRangesIter<'a>, span_start: u64) -> Self {
        Self { iter, span_start }
    }

    fn empty() -> Self {
        Self::new(BitArrayRangesIter::empty(), 0)
    }
}

impl<'a> Iterator for RangesIter<'a> {
    type Item = Range<u64>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let base = self.span_start;
        self.iter
            .next()
            .map(|r| (r.start as u64 + base)..(r.end as u64 + base))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl std::ops::BitOr for &BitSegment {
    type Output = BitSegment;
    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

impl std::ops::BitAnd for &BitSegment {
    type Output = BitSegment;
    fn bitand(self, rhs: Self) -> Self::Output {
        self.intersect(rhs)
    }
}

impl std::ops::Not for &BitSegment {
    type Output = BitSegment;
    fn not(self) -> Self::Output {
        self.complement()
    }
}
