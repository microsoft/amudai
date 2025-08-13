//! Ordered set of u64 positions over a fixed conceptual space.
//!
//! PositionSet models a set S ⊆ [0, span) where span is a mandatory upper bound
//! (span = 1 + max_possible_position). The span is part of the identity of a
//! PositionSet: two sets with the same elements but different spans are distinct.
//!
//! Representation
//! - The space [0, span) is partitioned into fixed-size segments of size Segment::SPAN.
//! - Each segment uses an adaptive representation selected from:
//!   - Empty: no positions present.
//!   - Full: all positions in the segment present.
//!   - List: sparse small lists of positions.
//!   - Bits: dense bitset for the segment.
//!   - Ranges: run-length/range encoding for clustered positions.
//!
//! This design efficiently covers sparse, dense, and mixed workloads while keeping
//! membership queries fast by O(1) segment lookup plus O(1) or O(log n) work inside
//! the segment.
//!
//! Key properties and invariants
//! - span is fixed per instance and defines the domain [0, span).
//! - Internally, segments fully and contiguously cover [0, span).
//! - No segment extends beyond span (the final segment is truncated if needed).
//! - Methods that take a position may panic if the position is outside [0, span).
//!
//! Typical usage
//! - Construct via empty(span), full(span), or from_positions(span, iter).
//! - Intersect/union/complement the set.
//! - Query membership with contains(pos).
//! - Iterate over positions or ranges

use std::ops::Range;

use amudai_ranges::{
    put_back::PutBack,
    slice_ext::SliceExt,
    transform::take_within::{TakeRangesWithinExt, TakeWithinExt},
};

use crate::segment::{Segment, SegmentKind};

/// An ordered set of u64 positions constrained to the domain [0, span).
///
/// PositionSet is optimized for sparse, dense, and mixed distributions. Internally it
/// partitions the space into fixed-size segments (Segment::SPAN) and picks an adaptive
/// representation per segment to balance memory and speed.
pub struct PositionSet {
    segments: Vec<Segment>,
    span: u64,
}

impl PositionSet {
    pub(crate) fn new(segments: Vec<Segment>, span: u64) -> PositionSet {
        let set = PositionSet { segments, span };
        set.check_basic_invariants();
        set
    }

    /// Create an empty set over [0, span).
    ///
    /// All membership tests return false for positions in range.
    pub fn empty(span: u64) -> PositionSet {
        Self::new_with_segment_fn(span, |range| Segment::empty(range))
    }

    /// Create a full set over [0, span).
    ///
    /// All positions in [0, span) are present.
    pub fn full(span: u64) -> PositionSet {
        Self::new_with_segment_fn(span, |range| Segment::full(range))
    }

    /// Build a set from an iterator of positions.
    ///
    /// Requirements:
    /// - The iterator must be monotonically increasing.
    /// - Positions < span are considered; positions ≥ span are ignored.
    ///
    /// Complexity: O(n) where n is the number of consumed positions < span.
    pub fn from_positions(span: u64, positions: impl Iterator<Item = u64>) -> PositionSet {
        let mut positions = PutBack::new(positions);
        let mut segments = Vec::with_capacity(span.div_ceil(Segment::SPAN) as usize);

        for segment_span in Self::segment_spans(span) {
            let segment = if let Some(&next_pos) = positions.peek() {
                if segment_span.contains(&next_pos) {
                    Segment::from_positions(
                        segment_span.clone(),
                        positions.take_within(segment_span.end),
                    )
                } else {
                    Segment::empty(segment_span)
                }
            } else {
                Segment::empty(segment_span)
            };
            segments.push(segment);
        }

        PositionSet { segments, span }
    }

    /// Build a set from an iterator of half-open ranges [start, end).
    ///
    /// Requirements:
    /// - The iterator must be sorted by `start`, strictly increasing, and non-overlapping.
    /// - Empty ranges are allowed and are ignored.
    /// - Portions outside `[0, span)` are clipped; fully out-of-span ranges are skipped.
    ///
    /// Complexity: O(r) where r is the total number of positions covered by ranges
    /// intersecting `[0, span)`.
    pub fn from_ranges(span: u64, ranges: impl Iterator<Item = Range<u64>>) -> PositionSet {
        let mut ranges = PutBack::new(ranges);
        let mut segments = Vec::with_capacity(span.div_ceil(Segment::SPAN) as usize);

        for segment_span in Self::segment_spans(span) {
            let segment = if let Some(next_range) = ranges.peek().cloned() {
                if segment_span.contains(&next_range.start) {
                    Segment::from_ranges(
                        segment_span.clone(),
                        ranges.take_ranges_within(segment_span.end),
                    )
                } else {
                    Segment::empty(segment_span)
                }
            } else {
                Segment::empty(segment_span)
            };
            segments.push(segment);
        }

        PositionSet { segments, span }
    }

    /// Return the span that defines the domain [0, span).
    #[inline]
    pub fn span(&self) -> u64 {
        self.span
    }

    /// Test membership of pos in the set.
    ///
    /// Returns true iff pos ∈ S, for pos in [0, span).
    ///
    /// Complexity: O(1) segment index + O(1) or O(log n) within the segment.
    ///
    /// Panics: if pos ≥ span.
    #[inline]
    pub fn contains(&self, pos: u64) -> bool {
        self.segments[Self::segment_of(pos)].contains(pos)
    }

    /// Set a single position in the set.
    ///
    /// Behavior is delegated to the underlying segment that owns `pos`:
    /// - [`SegmentKind::Bits`]: O(1) bit set.
    /// - [`SegmentKind::Full`]: no-op.
    /// - [`SegmentKind::Empty`]: converts that segment to bits and sets the bit
    ///   (allocates a fixed `SPAN/8` bytes once).
    /// - [`SegmentKind::List`] or [`SegmentKind::Ranges`]: this will panic (see
    ///   [`Segment::set`](crate::segment::Segment::set)).
    ///
    /// Recommendation:
    /// - If you plan to perform point updates on arbitrary sets, call
    ///   [`convert_to_bits`](Self::convert_to_bits) first to force non-empty/non-full segments
    ///   into a bitset representation and avoid panics and repeated allocations.
    ///
    /// Not ideal for bulk construction:
    /// - This method is not an efficient way to build large sets by repeatedly inserting
    ///   positions. Prefer using [`PositionSetBuilder`](crate::position_set_builder::PositionSetBuilder)
    ///   (or constructors like [`from_positions`](Self::from_positions) /
    ///   [`from_ranges`](Self::from_ranges)) to stream data in order and let the library choose
    ///   optimal segment encodings with amortized allocations.
    ///
    /// Complexity:
    /// - O(1) on `Bits`/`Full`.
    /// - First call into an `Empty` segment is O(1) plus a one-time allocation of `SPAN/8` bytes.
    ///
    /// Panics:
    /// - If `pos >= span`.
    /// - If the target segment is encoded as `List` or `Ranges`.
    #[inline]
    pub fn set(&mut self, pos: u64) {
        self.segments[Self::segment_of(pos)].set(pos);
    }

    /// Count the total number of positions present in the set.
    pub fn count_positions(&self) -> u64 {
        self.segments
            .iter()
            .map(|segment| segment.count_positions() as u64)
            .sum()
    }

    /// Parallel count of total positions.
    ///
    /// Behavior matches [`count_positions`](Self::count_positions) but processes
    /// segments in parallel.
    pub fn par_count_positions(&self) -> u64 {
        self.par_unary(|segment| segment.count_positions() as u64)
            .into_iter()
            .sum()
    }

    /// Returns an iterator over all set positions.
    pub fn positions(&self) -> PositionsIter<'_> {
        PositionsIter::all(self)
    }

    /// Returns an iterator over absolute positions within the specified range.
    pub fn positions_within(&self, range: Range<u64>) -> PositionsIter<'_> {
        PositionsIter::within(self, range)
    }

    /// Returns an iterator over contiguous ranges of set positions.
    pub fn ranges(&self) -> RangesIter<'_> {
        RangesIter::all(self)
    }

    /// Returns an iterator over contiguous ranges within the specified absolute range.
    pub fn ranges_within(&self, range: Range<u64>) -> RangesIter<'_> {
        RangesIter::within(self, range)
    }

    /// Set union (A ∪ B) with another set of the same span.
    ///
    /// Returns a new set containing positions present in either input.
    ///
    /// Panics: if `self.span != other.span`.
    pub fn union(&self, other: &PositionSet) -> PositionSet {
        assert_eq!(self.span, other.span);
        PositionSet::new(
            self.segments
                .iter()
                .zip(other.segments.iter())
                .map(|(a, b)| a.union(b))
                .collect(),
            self.span,
        )
    }

    /// Parallel set union using internal data-parallelism.
    ///
    /// Behavior matches [`union`](Self::union) but processes segments in parallel.
    /// Useful for very large sets; uses the crate's global worker pool.
    ///
    /// Panics: if `self.span != other.span`.
    pub fn par_union(&self, other: &PositionSet) -> PositionSet {
        let res = self.par_binary(other, |a, b| a.union(b));
        PositionSet::new(res, self.span)
    }

    /// Set intersection (A ∩ B) with another set of the same span.
    ///
    /// Returns a new set containing positions present in both inputs.
    ///
    /// Panics: if `self.span != other.span`.
    pub fn intersect(&self, other: &PositionSet) -> PositionSet {
        assert_eq!(self.span, other.span);
        PositionSet::new(
            self.segments
                .iter()
                .zip(other.segments.iter())
                .map(|(a, b)| a.intersect(b))
                .collect(),
            self.span,
        )
    }

    /// Parallel set intersection using internal data-parallelism.
    ///
    /// Behavior matches [`intersect`](Self::intersect) but processes segments in parallel.
    /// Useful for very large sets; uses the crate's global worker pool.
    ///
    /// Panics: if `self.span != other.span`.
    pub fn par_intersect(&self, other: &PositionSet) -> PositionSet {
        let res = self.par_binary(other, |a, b| a.intersect(b));
        PositionSet::new(res, self.span)
    }

    /// Logical complement over the fixed domain `[0, span)`.
    ///
    /// Returns a new set where each position `p` in `[0, span)` is present iff it is
    /// absent in `self`.
    pub fn invert(&self) -> PositionSet {
        PositionSet::new(
            self.segments.iter().map(|s| s.complement()).collect(),
            self.span,
        )
    }

    /// Parallel logical complement using internal data-parallelism.
    ///
    /// Behavior matches [`invert`](Self::invert) but processes segments in parallel.
    pub fn par_invert(&self) -> PositionSet {
        PositionSet::new(self.par_unary(|s| s.complement()), self.span)
    }

    /// Write all present positions into the provided slice, in ascending order.
    ///
    /// Behavior:
    /// - Positions are written contiguously starting at `output[0]`.
    /// - Exactly `self.count_positions()` values are written.
    /// - Any remaining tail in `output` (if longer) is left untouched.
    ///
    /// Requirements:
    /// - `output.len() >= self.count_positions()`.
    ///
    /// Panics:
    /// - If `output` is too small (slice indexing will panic).
    ///
    /// Complexity:
    /// - O(#segments + number_of_positions).
    ///
    /// Notes:
    /// - Values are absolute positions in `[0, self.span)`.
    /// - Allocation-free, eager alternative to iterating via [`positions`](Self::positions).
    pub fn collect_positions(&self, output: &mut [u64]) -> usize {
        let mut offset = 0usize;
        for segment in &self.segments {
            let n = segment.collect_positions(&mut output[offset..]);
            offset += n;
        }
        offset
    }

    /// Parallel write of all present positions into the provided slice, in ascending order.
    ///
    /// Behavior:
    /// - Produces the same result as [`collect_positions`](Self::collect_positions).
    ///
    /// Requirements:
    /// - `output.len() >= self.count_positions()`.
    ///
    /// Panics:
    /// - If `output` is too small (slice indexing will panic).
    pub fn par_collect_positions(&self, output: &mut [u64]) -> usize {
        let segments_per_chunk = self.get_par_chunk_size();

        let chunk_pos_counts = amudai_workflow::data_parallel::map(
            None,
            self.segments.chunks(segments_per_chunk),
            |segments| segments.iter().map(|s| s.count_positions()).sum::<usize>(),
        )
        .collect::<Vec<_>>();

        let chunk_outputs = output.split_at_sizes_mut(chunk_pos_counts.iter().copied());

        let items = self
            .segments
            .chunks(segments_per_chunk)
            .zip(chunk_outputs.into_iter());

        amudai_workflow::data_parallel::for_each(None, items, |(segments, output)| {
            let mut offset = 0usize;
            for segment in segments {
                let n = segment.collect_positions(&mut output[offset..]);
                offset += n;
            }
        });
        chunk_pos_counts.iter().sum::<usize>()
    }

    /// Internal consistency checks for debug/testing.
    ///
    /// Ensures that:
    /// - The last segment’s end equals span.
    ///
    /// Panics: if invariants are violated.
    pub fn check_basic_invariants(&self) {
        assert_eq!(
            self.segments
                .last()
                .map(|segment| segment.span().end)
                .unwrap_or(0),
            self.span
        );
        self.segments
            .iter()
            .for_each(|segment| segment.check_basic_invariants());
    }

    /// Run comprehensive internal invariants checks in debug/tests.
    ///
    /// Ensures per-segment invariants in addition to basic layout checks.
    ///
    /// Panics: if any invariant is violated.
    pub fn check_full_invariants(&self) {
        self.check_basic_invariants();
        self.segments
            .iter()
            .for_each(|segment| segment.check_full_invariants());
    }

    /// Re-encode segments in-place to their currently optimal representation.
    ///
    /// This selects the most compact encoding per segment given its contents
    /// (e.g., converting between List, Bits, Ranges, Empty, or Full) without
    /// changing logical membership.
    ///
    /// Useful after many updates to restore compactness; no-op if already optimal.
    pub fn optimize(&mut self) {
        self.segments.iter_mut().for_each(|segment| {
            segment.optimize();
        });
    }

    /// Re-encode all segments using a bitset representation, in place.
    ///
    /// Behavior:
    /// - Leaves [`SegmentKind::Empty`] and [`SegmentKind::Full`] segments unchanged.
    /// - Converts [`SegmentKind::List`] and [`SegmentKind::Ranges`] segments to
    ///   [`SegmentKind::Bits`] by calling [`Segment::convert_to_bits`].
    /// - Preserves the logical contents of the set.
    ///
    /// When to use:
    /// - To enforce a uniform, predictable layout (bitset) for all non-empty/non-full segments.
    /// - To benchmark or estimate memory usage where each segment uses roughly `span.len()/8` bytes.
    ///
    /// Complexity:
    /// - O(n) over the size of affected segments (number of positions/runs materialized).
    ///
    /// Notes:
    /// - This mutates the set in place; clone first if you need to preserve current encodings.
    pub fn convert_to_bits(&mut self) {
        self.segments
            .iter_mut()
            .for_each(|segment| segment.convert_to_bits());
    }

    /// Compute summary statistics about the set and its internal representation.
    ///
    /// These stats are useful for diagnostics and profiling (e.g., distribution of
    /// segment kinds). This does not mutate the set.
    pub fn compute_stats(&self) -> PositionSetStats {
        self.check_basic_invariants();
        let mut stats = PositionSetStats::default();
        stats.span = self.span;
        stats.position_count = self.count_positions();
        stats.total_segments = self.segments.len();
        stats.heap_size = self.heap_size_bytes();
        for segment in &self.segments {
            match segment.kind() {
                SegmentKind::Empty => stats.empty_segments += 1,
                SegmentKind::Full => stats.full_segments += 1,
                SegmentKind::List => stats.list_segments += 1,
                SegmentKind::Bits => stats.bit_segments += 1,
                SegmentKind::Ranges => stats.range_segments += 1,
            }
        }
        stats
    }

    /// Returns the number of heap-allocated bytes used by this `PositionSet`.
    pub fn heap_size_bytes(&self) -> usize {
        self.segments
            .iter()
            .map(|segment| segment.heap_size_bytes())
            .sum::<usize>()
            + self.segments.len() * std::mem::size_of::<Segment>()
    }
}

impl PositionSet {
    #[inline]
    fn segment_of(pos: u64) -> usize {
        (pos / Segment::SPAN) as usize
    }

    fn new_with_segment_fn(span: u64, mut f: impl FnMut(Range<u64>) -> Segment) -> PositionSet {
        let segment_count = span.div_ceil(Segment::SPAN) as usize;
        let segments = (0..segment_count)
            .map(|i| {
                let start = i as u64 * Segment::SPAN;
                let end = start.checked_add(Segment::SPAN).expect("end").min(span);
                f(start..end)
            })
            .collect();
        let set = PositionSet { segments, span };
        set.check_basic_invariants();
        set
    }

    fn segment_spans(span: u64) -> impl Iterator<Item = Range<u64>> {
        let end = span.next_multiple_of(Segment::SPAN);
        (0..end)
            .step_by(Segment::SPAN as usize)
            .map(move |s| s..(s + Segment::SPAN).min(span))
    }

    fn par_binary<R: Send>(
        &self,
        other: &PositionSet,
        f: impl Fn(&Segment, &Segment) -> R + Send + Sync,
    ) -> Vec<R> {
        assert_eq!(self.span, other.span);
        let chunk_size = self.get_par_chunk_size();
        amudai_workflow::data_parallel::map(
            None,
            self.segments
                .chunks(chunk_size)
                .zip(other.segments.chunks(chunk_size)),
            |(left, right)| {
                left.iter()
                    .zip(right)
                    .map(|(a, b)| f(a, b))
                    .collect::<Vec<_>>()
            },
        )
        .flatten()
        .collect()
    }

    fn par_unary<R: Send>(&self, f: impl Fn(&Segment) -> R + Send + Sync) -> Vec<R> {
        let chunk_size = self.get_par_chunk_size();
        amudai_workflow::data_parallel::map(None, self.segments.chunks(chunk_size), |chunk| {
            chunk.iter().map(|segment| f(segment)).collect::<Vec<_>>()
        })
        .flatten()
        .collect()
    }

    fn get_par_chunk_size(&self) -> usize {
        let n = self.segments.len()
            / (amudai_workflow::eager_pool::EagerPool::global().thread_count() * 8);
        std::cmp::max(n, 16)
    }
}

/// Summary statistics for a PositionSet and its internal layout.
///
/// Fields:
/// - span: the domain size [0, span).
/// - heap_size: memory allocated by this instance.
/// - position_count: total number of present positions.
/// - total_segments: number of internal segments covering [0, span).
/// - empty_segments: segments with no positions.
/// - full_segments: segments that are completely filled.
/// - bit_segments: segments represented as bitsets.
/// - list_segments: segments represented as small position lists.
/// - range_segments: segments represented as range lists.
#[derive(Debug, Clone, Default)]
pub struct PositionSetStats {
    /// Domain size [0, span).
    pub span: u64,
    /// Total number of allocated bytes for this `PositionSet`.
    pub heap_size: usize,
    /// Total present positions across all segments.
    pub position_count: u64,
    /// Number of segments used to cover [0, span).
    pub total_segments: usize,
    /// Count of empty segments.
    pub empty_segments: usize,
    /// Count of full segments.
    pub full_segments: usize,
    /// Count of bitset-backed segments.
    pub bit_segments: usize,
    /// Count of list-backed segments.
    pub list_segments: usize,
    /// Count of range-backed segments.
    pub range_segments: usize,
}

/// Iterator over absolute positions across all segments in a `PositionSet`.
pub struct PositionsIter<'a> {
    segments: std::slice::Iter<'a, Segment>,
    current: Option<crate::segment::PositionsIter<'a>>,
    clamp: Range<u64>,
}

impl<'a> PositionsIter<'a> {
    fn new(segments: &'a [Segment], clamp: Range<u64>) -> Self {
        PositionsIter {
            segments: segments.iter(),
            current: None,
            clamp,
        }
    }

    fn empty() -> Self {
        PositionsIter {
            segments: [].iter(),
            current: None,
            clamp: 0..0,
        }
    }

    fn all(set: &'a PositionSet) -> Self {
        Self::new(&set.segments, 0..set.span)
    }

    fn within(set: &'a PositionSet, mut range: Range<u64>) -> Self {
        if set.span == 0 {
            return Self::empty();
        }
        // Clamp to [0, span)
        range.start = range.start.min(set.span);
        range.end = range.end.min(set.span);
        if range.start >= range.end {
            return Self::empty();
        }
        let start_idx = (range.start / Segment::SPAN) as usize;
        let last_idx = ((range.end - 1) / Segment::SPAN) as usize;
        Self::new(&set.segments[start_idx..last_idx + 1], range)
    }
}

impl<'a> Iterator for PositionsIter<'a> {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(cur) = &mut self.current {
                if let Some(v) = cur.next() {
                    return Some(v);
                }
                self.current = None; // exhausted current segment
            }

            let it = self.segments.next()?.positions_within(self.clamp.clone());
            self.current = Some(it);
            // loop will attempt to pull from current
        }
    }
}

/// Iterator over contiguous absolute ranges across all segments in a `PositionSet`.
pub struct RangesIter<'a> {
    segments: std::slice::Iter<'a, Segment>,
    current: Option<crate::segment::RangesIter<'a>>,
    clamp: Range<u64>,
    pending: Option<Range<u64>>, // coalescing buffer across segment boundaries
}

impl<'a> RangesIter<'a> {
    fn new(segments: &'a [Segment], clamp: Range<u64>) -> Self {
        RangesIter {
            segments: segments.iter(),
            current: None,
            clamp,
            pending: None,
        }
    }

    fn empty() -> Self {
        Self::new(&[], 0..0)
    }

    fn all(set: &'a PositionSet) -> Self {
        Self::new(&set.segments, 0..set.span)
    }

    fn within(set: &'a PositionSet, mut range: Range<u64>) -> Self {
        if set.span == 0 {
            return Self::empty();
        }
        // Clamp to [0, span)
        range.start = range.start.min(set.span);
        range.end = range.end.min(set.span);
        if range.start >= range.end {
            return Self::empty();
        }
        let start_idx = (range.start / Segment::SPAN) as usize;
        let last_idx = ((range.end - 1) / Segment::SPAN) as usize;
        Self::new(&set.segments[start_idx..last_idx + 1], range)
    }
}

impl<'a> Iterator for RangesIter<'a> {
    type Item = Range<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If we have a current segment iterator, try to pull next range from it
            if let Some(cur) = &mut self.current {
                if let Some(next_range) = cur.next() {
                    if let Some(pending) = &mut self.pending {
                        if pending.end == next_range.start {
                            // Coalesce adjacent ranges across segment boundaries
                            pending.end = next_range.end;
                            continue;
                        } else {
                            // Emit previous accumulated range and start a new one
                            let out = self.pending.replace(next_range).unwrap();
                            return Some(out);
                        }
                    } else {
                        // Do not emit the range, buffer it for coalescing
                        self.pending = Some(next_range);
                        continue;
                    }
                }

                // Current segment exhausted
                self.current = None;
                // Loop to either emit pending or move to next segment
            }

            if let Some(segment) = self.segments.next() {
                let it = segment.ranges_within(self.clamp.clone());
                self.current = Some(it);
            } else {
                // No current iterator; if we have pending and no more segments to scan, emit it
                return self.pending.take();
            }
            // Loop continues to try pulling from the new iterator
        }
    }
}
