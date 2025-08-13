//! A segment implementation that stores positions as a set of disjoint
//! ascending ranges.

use std::ops::Range;

use amudai_ranges::set_ops;
use itertools::Itertools;

use crate::segment::{Segment, bits::BitSegment, list::ListSegment};

/// A range of segment positions `[first, last]` (both inclusive).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Run {
    /// Segment-relative run start position
    pub first: u16,
    /// Inclusive segment-relative run end position
    pub last: u16,
}

impl Run {
    #[inline]
    pub fn point(value: u16) -> Run {
        Run {
            first: value,
            last: value,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.last as usize - self.first as usize + 1
    }

    #[inline]
    pub fn translated(segment_start: u64, range: Range<u64>) -> Run {
        Run::from(range.start - segment_start..range.end - segment_start)
    }

    #[inline]
    pub fn as_range(&self) -> Range<usize> {
        self.first as usize..self.last as usize + 1
    }

    #[inline]
    pub fn as_range_u32(&self) -> Range<u32> {
        self.first as u32..self.last as u32 + 1
    }

    #[inline]
    pub fn rebase(&self, base: u64) -> Range<u64> {
        self.first as u64 + base..self.last as u64 + 1 + base
    }

    #[inline]
    pub fn contains(&self, pos: u16) -> bool {
        pos >= self.first && pos <= self.last
    }

    #[inline]
    pub fn is_disjoint(&self, other: Run) -> bool {
        self.last < other.first || self.first > other.last
    }

    #[inline]
    pub fn is_adjacent(&self, other: Run) -> bool {
        self.last < other.first && other.first - self.last == 1
    }

    #[inline]
    pub fn is_adjacent_or_overlaps(&self, other: Run) -> bool {
        self.last >= other.first || other.first - self.last == 1
    }

    #[inline]
    pub fn coalesce(&self, next: Run) -> Result<Run, (Run, Run)> {
        if self.first <= next.first && self.is_adjacent_or_overlaps(next) {
            Ok(Run {
                first: self.first,
                last: self.last.max(next.last),
            })
        } else {
            Err((*self, next))
        }
    }
}

impl From<Range<usize>> for Run {
    #[inline]
    fn from(r: Range<usize>) -> Self {
        debug_assert!(r.start < r.end);
        debug_assert!(r.start <= u16::MAX as usize);
        debug_assert!(r.end <= u16::MAX as usize + 1);
        Run {
            first: r.start as u16,
            last: (r.end - 1) as u16,
        }
    }
}

impl From<Range<u32>> for Run {
    #[inline]
    fn from(r: Range<u32>) -> Self {
        debug_assert!(r.start < r.end);
        debug_assert!(r.start <= u16::MAX as u32);
        debug_assert!(r.end <= u16::MAX as u32 + 1);
        Run {
            first: r.start as u16,
            last: (r.end - 1) as u16,
        }
    }
}

impl From<Range<u64>> for Run {
    #[inline]
    fn from(r: Range<u64>) -> Self {
        debug_assert!(r.start < r.end);
        debug_assert!(r.start <= u16::MAX as u64);
        debug_assert!(r.end <= u16::MAX as u64 + 1);
        Run {
            first: r.start as u16,
            last: (r.end - 1) as u16,
        }
    }
}

impl From<Run> for Range<u64> {
    #[inline]
    fn from(run: Run) -> Self {
        run.rebase(0)
    }
}

impl From<Run> for Range<usize> {
    #[inline]
    fn from(run: Run) -> Self {
        run.as_range()
    }
}

/// A segment implementation that stores positions as a set of disjoint
/// ascending ranges.
///
/// `RangeSegment` is efficient when positions tend to form contiguous runs.
/// Ranges are stored relative to `span.start` as `u32` offsets, are sorted by
/// start, non-overlapping, and each has start < end.
#[derive(Clone)]
pub struct RangeSegment {
    /// The logical range of positions represented by this segment
    span: Range<u64>,
    /// Disjoint, sorted runs of relative positions within the span
    runs: Vec<Run>,
}

impl RangeSegment {
    pub(crate) fn new(span: Range<u64>, runs: Vec<Run>) -> RangeSegment {
        assert!(span.start.is_multiple_of(Segment::SPAN));
        assert_ne!(span.start, span.end);

        let segment = RangeSegment { span, runs };
        segment.check_basic_invariants();

        #[cfg(debug_assertions)]
        segment.check_full_invariants();

        segment
    }

    /// Creates a new empty `RangeSegment` for the given span.
    pub fn empty(span: Range<u64>) -> Self {
        RangeSegment::new(span, Vec::new())
    }

    /// Creates a new empty `RangeSegment` with capacity for `capacity` ranges.
    pub fn with_capacity(span: Range<u64>, capacity: usize) -> Self {
        RangeSegment::new(span, Vec::with_capacity(capacity))
    }

    /// Creates a `RangeSegment` with all positions in the span set.
    pub fn full(span: Range<u64>) -> Self {
        let len = span.end - span.start;
        assert_ne!(len, 0);
        assert!(len <= Segment::SPAN);
        RangeSegment::new(span.clone(), vec![Run::translated(span.start, span)])
    }

    /// Creates a `RangeSegment` from an iterator of absolute positions (sorted, unique).
    pub fn from_positions(span: Range<u64>, positions: impl Iterator<Item = u64>) -> Self {
        Self::from_ranges(span, positions.map(|pos| pos..pos + 1))
    }

    /// Creates a `RangeSegment` from absolute ranges (sorted, disjoint, within span).
    pub fn from_ranges(span: Range<u64>, ranges: impl Iterator<Item = Range<u64>>) -> Self {
        let span_size = (span.end - span.start) as usize;
        let (lower_bound, upper_bound) = ranges.size_hint();
        let capacity = upper_bound.unwrap_or(lower_bound).min(span_size);

        let mut runs = Vec::<Run>::with_capacity(capacity);
        let mapped = ranges
            .map(|r| Run::translated(span.start, r))
            .coalesce(|prev, next| prev.coalesce(next));
        runs.extend(mapped);
        RangeSegment::new(span, runs)
    }

    pub fn from_ranges_with_count(
        span: Range<u64>,
        ranges: impl Iterator<Item = Range<u64>>,
        count: usize,
    ) -> Self {
        let span_size = (span.end - span.start) as usize;
        assert!(count <= span_size);
        let mut runs = Vec::<Run>::with_capacity(count);
        let mapped = ranges
            .map(|r| Run::translated(span.start, r))
            .coalesce(|prev, next| prev.coalesce(next));
        runs.extend(mapped);
        runs.shrink_to_fit();
        RangeSegment::new(span, runs)
    }

    pub fn from_relative_position_slice(
        span: Range<u64>,
        positions: &[u16],
        run_count: Option<usize>,
    ) -> Self {
        let run_count = run_count.unwrap_or_else(|| super::list::count_runs(positions));
        if positions.is_empty() {
            return RangeSegment::empty(span);
        }
        let mut runs = Vec::with_capacity(run_count);
        runs.extend(
            positions
                .iter()
                .map(|&pos| Run::point(pos))
                .coalesce(|prev, next| prev.coalesce(next)),
        );
        RangeSegment::new(span, runs)
    }

    /// Returns a copy of the segment's span.
    #[inline]
    pub fn span(&self) -> Range<u64> {
        self.span.clone()
    }

    /// Returns a slice of relative ranges within this segment.
    #[inline]
    pub fn runs(&self) -> &[Run] {
        &self.runs
    }

    /// Returns the number of ranges within this segment.
    #[inline]
    pub fn count_runs(&self) -> usize {
        self.runs.len()
    }

    /// Returns an iterator over absolute ranges [start, end) within this segment's span.
    pub fn ranges(&self) -> RangesIter<'_> {
        RangesIter::new(
            self.runs.iter(),
            self.span.start,
            self.span.start,
            self.span.end,
        )
    }

    /// Returns an iterator over absolute ranges [start, end) that lie within `range`,
    /// clipped to this segment's span and stored ranges.
    pub fn ranges_within(&self, range: Range<u64>) -> RangesIter<'_> {
        let start = range.start.max(self.span.start);
        let end = range.end.min(self.span.end);

        if start >= end || self.runs.is_empty() {
            return RangesIter::empty();
        }

        // Relative bounds to the segment base
        let rel_start = (start - self.span.start) as u16; // inclusive
        let rel_end_u32 = (end - self.span.start) as u32; // exclusive, may be u16::MAX + 1

        // First run whose last >= rel_start (i.e., could overlap)
        let start_idx = self.runs.partition_point(|run| run.last < rel_start);
        // First run whose first >= rel_end (i.e., cannot overlap)
        let end_idx = self.runs[start_idx..]
            .partition_point(|run| (run.first as u32) < rel_end_u32)
            + start_idx;

        // Ensure a valid empty slice if there's no overlap
        if end_idx == start_idx {
            return RangesIter::empty();
        }

        RangesIter::new(
            self.runs[start_idx..end_idx].iter(),
            self.span.start,
            start,
            end,
        )
    }

    /// Returns an iterator over all absolute positions in this segment.
    pub fn positions(&self) -> PositionsIter<'_> {
        PositionsIter::new(self.ranges().flatten())
    }

    /// Returns an iterator over absolute positions within the specified absolute range.
    pub fn positions_within(&self, range: Range<u64>) -> PositionsIter<'_> {
        PositionsIter::new(self.ranges_within(range).flatten())
    }

    /// Returns the number of positions covered by the ranges.
    #[inline]
    pub fn count_positions(&self) -> usize {
        self.runs.iter().map(|r| r.len()).sum()
    }

    /// Returns the smallest absolute position contained in this segment, if any.
    ///
    /// Returns `None` if the segment contains no positions.
    /// Complexity: O(1).
    #[inline]
    pub fn min_pos(&self) -> Option<u64> {
        self.runs.first().map(|r| r.first as u64 + self.span.start)
    }

    /// Returns the largest absolute position contained in this segment, if any.
    ///
    /// The value is inclusive (i.e., the last set position).
    /// Returns `None` if the segment contains no positions.
    /// Complexity: O(1).
    #[inline]
    pub fn max_pos(&self) -> Option<u64> {
        self.runs.last().map(|r| r.last as u64 + self.span.start)
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
        let mut rank: usize = 0;

        for &Run { first, last } in &self.runs {
            for rel in first..=last {
                f(rank, base + rel as u64);
                rank += 1;
            }
        }
        rank
    }

    /// Adds a single absolute position. Must be greater than any previously added position.
    #[inline]
    pub fn push(&mut self, pos: u64) {
        debug_assert!(
            self.span.contains(&pos),
            "Position {} is outside segment span {:?}",
            pos,
            self.span
        );
        let rel = (pos - self.span.start) as u16;
        match self.runs.last_mut() {
            None => self.runs.push(Run::point(rel)),
            Some(last) => {
                debug_assert!(rel > last.last);
                if rel == last.last + 1 {
                    last.last = rel;
                } else {
                    self.runs.push(Run::point(rel));
                }
            }
        }
    }

    /// Adds an absolute range [start, end). Must start after the last inserted point.
    #[inline]
    pub fn push_range(&mut self, range: Range<u64>) {
        if range.start >= range.end {
            return;
        }
        debug_assert!(
            range.start >= self.span.start && range.end <= self.span.end,
            "Range {:?} is outside segment span {:?}",
            range,
            self.span
        );
        let r = Run::translated(self.span.start, range);
        match self.runs.last_mut() {
            None => self.runs.push(r),
            Some(last) => {
                debug_assert!(r.first > last.last);
                if r.first == last.last + 1 {
                    last.last = r.last;
                } else {
                    self.runs.push(r.clone());
                }
            }
        }
    }

    /// Checks whether an absolute position is contained in any range.
    pub fn contains(&self, pos: u64) -> bool {
        debug_assert!(self.span.contains(&pos));
        let rel = (pos - self.span.start) as u16;
        // Find the first run with last >= rel
        let idx = self.runs.partition_point(|run| run.last < rel);
        match self.runs.get(idx) {
            Some(run) => run.first <= rel,
            None => false,
        }
    }

    /// Computes the union of two `RangeSegment`s.
    /// Panics if spans differ.
    pub fn union(&self, other: &RangeSegment) -> RangeSegment {
        assert_eq!(
            self.span, other.span,
            "RangeSegment spans must match for union"
        );

        let ranges =
            set_ops::union::union_ranges(self.relative_ranges_u32(), other.relative_ranges_u32());
        RangeSegment::from_relative_ranges_u32(self.span(), ranges)
    }

    /// Computes the intersection of two `RangeSegment`s.
    /// Panics if spans differ.
    pub fn intersect(&self, other: &RangeSegment) -> RangeSegment {
        assert_eq!(
            self.span, other.span,
            "RangeSegment spans must match for intersect"
        );
        let ranges = set_ops::intersection::intersect_ranges(
            self.relative_ranges_u32(),
            other.relative_ranges_u32(),
        );
        RangeSegment::from_relative_ranges_u32(self.span(), ranges)
    }

    /// Computes the complement within this segment's span.
    pub fn complement(&self) -> RangeSegment {
        if self.runs.is_empty() {
            return RangeSegment::full(self.span.clone());
        }
        let size = (self.span.end - self.span.start) as u32;
        let ranges = set_ops::complement::complement_ranges(size, self.relative_ranges_u32());
        RangeSegment::from_relative_ranges_u32(self.span(), ranges)
    }

    /// Converts this ranges segment into a list-based segment.
    ///
    /// Builds and returns a `ListSegment` containing the absolute positions of all
    /// ranges in ascending order, preserving the same `span` as `self`.
    pub fn to_list(&self) -> ListSegment {
        let count = self.count_positions();
        ListSegment::from_positions_with_count(self.span(), self.positions(), count)
    }

    /// Converts this ranges segment into a bit-based segment.
    ///
    /// Returns a `BitSegment` with the same absolute `span` as `self`, with all
    /// positions covered by this segment marked as set. This is equivalent to:
    ///
    /// `BitSegment::from_ranges(self.span(), self.ranges())`
    pub fn to_bits(&self) -> BitSegment {
        BitSegment::from_ranges(self.span(), self.ranges())
    }

    pub fn compact(&mut self) {
        self.runs.shrink_to_fit();
    }

    /// Returns the number of heap-allocated bytes used by this segment.
    ///
    /// This accounts only for the heap storage of the underlying `Vec`
    /// that backs the range list. It does not include the size of the `RangeSegment`
    /// struct itself, the `span` range, or any allocator bookkeeping overhead.
    pub fn heap_size_bytes(&self) -> usize {
        self.runs.capacity() * std::mem::size_of::<Run>()
    }

    pub fn check_basic_invariants(&self) {
        if self.runs.is_empty() {
            return;
        }
        let span_len_u64 = self.span.end - self.span.start;
        assert!(span_len_u64 <= Segment::SPAN, "Span length Segment::SPAN");

        // Upper bound on number of disjoint non-empty ranges
        assert!(
            self.runs.len() <= span_len_u64 as usize,
            "Too many ranges: {} > span_len {}",
            self.runs.len(),
            span_len_u64
        );

        let first = self.runs.first().unwrap();
        let last = self.runs.last().unwrap();

        assert!(
            (last.last as u64) < span_len_u64,
            "Last run end {} must be < span_len {}",
            last.last,
            span_len_u64
        );

        if self.runs.len() > 1 {
            assert!(
                first.first < last.last,
                "Degenerate ordering: first.start is {}, last.end is {}",
                first.first,
                last.last
            );
        }
    }

    pub fn check_full_invariants(&self) {
        if self.runs.is_empty() {
            return;
        }
        self.check_basic_invariants();

        let span_len = (self.span.end - self.span.start) as usize;

        let mut prev: Option<&Run> = None;
        for r in &self.runs {
            // In-bounds and non-empty
            assert!(r.first <= r.last, "Valid range: {:?}", r);
            assert!(
                (r.last as usize) < span_len,
                "Run end {} must be less than span_len {}",
                r.last,
                span_len
            );

            if let Some(p) = prev {
                assert!(
                    p.last < r.first && r.first - p.last > 1,
                    "Runs must be sorted strictly disjoint: prev={:?}, next={:?}",
                    p,
                    r
                );
            }
            prev = Some(r);
        }
    }

    fn relative_ranges_u32(&self) -> impl Iterator<Item = Range<u32>> {
        self.runs.iter().map(|r| r.as_range_u32())
    }

    fn from_relative_ranges_u32(
        span: Range<u64>,
        it: impl Iterator<Item = Range<u32>>,
    ) -> RangeSegment {
        let runs = it.map(|r| Run::from(r)).collect::<Vec<_>>();
        RangeSegment::new(span, runs)
    }
}

impl std::ops::BitOr for &RangeSegment {
    type Output = RangeSegment;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

impl std::ops::BitAnd for &RangeSegment {
    type Output = RangeSegment;

    fn bitand(self, rhs: Self) -> Self::Output {
        self.intersect(rhs)
    }
}

impl std::ops::Not for &RangeSegment {
    type Output = RangeSegment;

    fn not(self) -> Self::Output {
        self.complement()
    }
}

/// Iterator over absolute u64 ranges of a `RangeSegment` clipped within a given absolute range.
#[derive(Clone)]
pub struct RangesIter<'a> {
    runs: std::slice::Iter<'a, Run>,
    base: u64,
    clip_start: u64,
    clip_end: u64,
}

impl<'a> RangesIter<'a> {
    #[inline]
    fn empty() -> Self {
        RangesIter {
            runs: [].iter(),
            base: 0,
            clip_start: 0,
            clip_end: 0,
        }
    }

    #[inline]
    fn new(runs: std::slice::Iter<'a, Run>, base: u64, clip_start: u64, clip_end: u64) -> Self {
        RangesIter {
            runs,
            base,
            clip_start,
            clip_end,
        }
    }

    #[inline]
    fn map_run(&self, run: Run) -> Range<u64> {
        let start = run.first as u64 + self.base;
        let end = run.last as u64 + self.base + 1;
        let range = start.max(self.clip_start)..end.min(self.clip_end);
        debug_assert!(range.start < range.end);
        range
    }
}

impl<'a> Iterator for RangesIter<'a> {
    type Item = Range<u64>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.runs.next().map(|&run| self.map_run(run))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.runs.size_hint()
    }
}

impl<'a> DoubleEndedIterator for RangesIter<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.runs.next_back().map(|&run| self.map_run(run))
    }
}

impl<'a> ExactSizeIterator for RangesIter<'a> {}

impl<'a> std::iter::FusedIterator for RangesIter<'a> {}

// Iterator over absolute positions of a `RangeSegment`, implemented by flattening `RangesIter`.
#[derive(Clone)]
pub struct PositionsIter<'a>(std::iter::Flatten<RangesIter<'a>>);

impl<'a> PositionsIter<'a> {
    #[inline]
    fn new(inner: std::iter::Flatten<RangesIter<'a>>) -> Self {
        PositionsIter(inner)
    }
}

impl<'a> Iterator for PositionsIter<'a> {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<'a> DoubleEndedIterator for PositionsIter<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back()
    }
}

impl<'a> std::iter::FusedIterator for PositionsIter<'a> {}
