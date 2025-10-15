//! Segments are compact encodings of a fixed-size, half-open interval of absolute positions.
//! - Each segment owns [start, end) with 0 < end - start <= SPAN and start % SPAN == 0.
//! - The final segment in a set may be shorter than SPAN (but never zero).
//! - Encodings trade space for speed: Empty, Full, List(u16 offsets), Bits(bitset), Ranges(u16 pairs).
//! - Constructors pick an encoding automatically; optimize() can re-encode later.
//! - Inputs must be sorted, unique, and inside span; debug assertions enforce these contracts.
//! - All positions yielded/consumed are absolute (not rebased to 0).
//! - Typical costs are O(n) in the size of the current representation.

use std::{borrow::Cow, ops::Range};

use crate::{
    bit_array::BitArray,
    segment::{
        bits::BitSegment,
        list::ListSegment,
        ranges::{RangeSegment, Run},
    },
};

pub mod bits;
pub mod builder;
pub mod list;
pub mod ranges;

/// A fixed-size window over the logical position space, stored in one of several
/// encodings.
///
/// Overview
/// - Owns a half-open span [start, end) aligned to SPAN; length is usually SPAN,
///   the last segment may be shorter.
/// - Encodings:
///   - Empty: no positions set
///   - Full: all positions in span set
///   - List: sparse positions as u32 offsets from span.start
///   - Bits: dense bitset, one bit per position
///   - Ranges: runs as (start, end) u32 pairs relative to span.start
///
/// Construction and evolution
/// - Use empty/full, from_positions, or from_ranges; heuristics pick an efficient
///   encoding.
/// - Call optimize() to re-encode based on current contents; infer_optimal_kind()
///   predicts without changing state.
///
/// Contracts and costs
/// - Inputs must be sorted, unique, and inside the span; debug assertions enforce
///   contracts.
/// - Most operations are O(n) in the size of the current encoding.
///
/// Constants
/// - SPAN: logical width per segment (64K positions)
/// - BIT_SEGMENT_SIZE, MAX_LIST_LEN, MAX_RANGES_LEN: storage thresholds derived
///   from SPAN
#[derive(Clone)]
pub enum Segment {
    Empty(EmptySegment),
    Full(FullSegment),
    List(ListSegment),
    Bits(BitSegment),
    Ranges(RangeSegment),
}

/// Identifies the storage encoding used by a `Segment`.
/// Returned by `Segment::kind`, selected by `optimize()`/`infer_optimal_kind()`,
/// or requested via `from_positions_with_kind`/`from_ranges_with_kind`.
/// Describes only how positions within the owned span are stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SegmentKind {
    /// No positions set within the span.
    Empty,
    /// All positions in the span are set.
    Full,
    /// Sparse set of individual positions as `u16` offsets from `span.start`.
    /// Best for very sparse data.
    List,
    /// Dense bitset, one bit per position across the span (fixed size: `SPAN/8` bytes).
    /// Good for high-density data.
    Bits,
    /// Run-length encoding as `(start, end)` `u16` pairs relative to `span.start`.
    /// Efficient when positions form few long runs.
    Ranges,
}

impl Segment {
    /// The size of the logical position range represented by a single `Segment`.
    pub const SPAN: u64 = u16::MAX as u64 + 1;

    /// The size in bytes required for `Bits` segment storage.
    ///
    /// This value is derived from `Self::SPAN` and represents the buffer size
    /// needed to store `Self::SPAN` bits (1 bit per position).
    pub const BIT_SEGMENT_SIZE: u64 = Self::SPAN / 8;

    /// The maximum number of positions that can be stored in a `List` segment.
    ///
    /// Positions in the list are stored as `u16` values relative to the segment's
    /// span start. When the number of positions exceeds this threshold, the segment
    /// should be converted to a `Bits` representation for better efficiency.
    pub const MAX_LIST_LEN: u64 = Self::BIT_SEGMENT_SIZE / 2;

    /// The maximum number of ranges that can be stored in a `Ranges` segment.
    ///
    /// Ranges are stored as pairs of `u16` values (start, inclusive_end) relative to the
    /// segment's span start. When the number of ranges exceeds this threshold,
    /// the segment should be converted to a `Bits` representation for better efficiency.
    pub const MAX_RANGES_LEN: u64 = Self::BIT_SEGMENT_SIZE / 4;
}

impl Segment {
    /// Creates an empty segment for the given `span`.
    ///
    /// Behavior
    /// - Produces a [`Segment::Empty`] that represents the `span` with no positions
    ///   set inside it.
    /// - `span.start` is expected to be aligned to [`Segment::SPAN`]. The length
    ///   is typically exactly `SPAN`, except possibly for the last segment which
    ///   may be shorter but never zero.
    ///
    /// Panics
    /// - May panic if `span` is invalid (e.g. `start >= end`) or
    ///   exceeds [`Segment::SPAN`].
    pub fn empty(span: Range<u64>) -> Segment {
        assert!(span.start.is_multiple_of(Self::SPAN));
        assert!(span.end - span.start <= Self::SPAN);
        Segment::Empty(EmptySegment::new(span))
    }

    /// Creates a full segment for the given `span`.
    ///
    /// Behavior
    /// - Produces a [`Segment::Full`] that represents the `span` with all positions
    ///   set inside it.
    ///
    /// Panics
    /// - May panic in debug builds if `span` is invalid (e.g. `start >= end`) or
    ///   exceeds [`Segment::SPAN`].
    pub fn full(span: Range<u64>) -> Segment {
        assert!(span.start.is_multiple_of(Self::SPAN));
        assert!(span.end - span.start <= Self::SPAN);
        Segment::Full(FullSegment::new(span))
    }

    /// Builds a segment from an iterator of absolute positions within `span`.
    ///
    /// Input handling
    /// - Positions are absolute `u64` values; they must be sorted and unique.
    /// - Positions must be within `span`.
    ///
    /// Representation
    /// - Chooses an efficient representation automatically based on density and
    ///   heuristics (e.g. [`Segment::MAX_LIST_LEN`], [`Segment::MAX_RANGES_LEN`]).
    /// - May return [`SegmentKind::Empty`] if no positions fall in `span`, or
    ///   [`SegmentKind::Full`] if the set covers the whole `span`.
    /// - For sparse inputs it prefers `List`; for dense inputs it prefers `Bits`;
    ///   if long consecutive runs are detected it may prefer `Ranges`.
    ///
    /// Complexity
    /// - O(n) over the number of items produced by `positions`.
    ///
    /// Panics
    /// - May panic if `span` is invalid.
    pub fn from_positions(span: Range<u64>, positions: impl Iterator<Item = u64>) -> Segment {
        let mut segment = Segment::List(ListSegment::from_positions(span, positions));
        segment.optimize();
        segment
    }

    /// Builds a segment from an iterator of half-open ranges `[start, end)` of
    /// absolute positions.
    ///
    /// Input handling
    /// - Ranges must be sorted and non-overlapping.
    /// - Ranges must be contained within `span`.
    ///
    /// Representation
    /// - Chooses an efficient representation automatically based on the total
    ///   coverage and number of runs. May produce `Empty`, `Full`, `Ranges`,
    ///   `List` (if overall cardinality is very small), or `Bits`.
    ///
    /// Panics
    /// - May panic if `span` or `ranges` is invalid.
    pub fn from_ranges(span: Range<u64>, pos_ranges: impl Iterator<Item = Range<u64>>) -> Segment {
        let mut segment = Segment::Ranges(RangeSegment::from_ranges(span, pos_ranges));
        segment.optimize();
        segment
    }

    /// Builds a segment from a slice of relative positions within `span`.
    ///
    /// Inputs
    /// - `positions` are `u16` offsets relative to `span.start`.
    /// - Must be sorted, unique, and less than `span.len()`.
    ///
    /// Representation
    /// - Chooses `List`, `Ranges`, or `Bits` based on density and run count.
    /// - May yield `Empty` or `Full` for trivial cases.
    ///
    /// Complexity
    /// - O(n) over `positions.len()`.
    ///
    /// Panics
    /// - May panic if `span` is invalid.
    pub fn from_relative_position_slice(span: Range<u64>, positions: &[u16]) -> Segment {
        let span_len = span.end - span.start;
        let run_count = list::count_runs(positions);
        let kind = Self::infer_optimal_kind_by_size(
            span_len,
            positions.len(),
            run_count,
            SegmentKind::List,
        );
        match kind {
            SegmentKind::Empty => Segment::empty(span),
            SegmentKind::Full => Segment::full(span),
            SegmentKind::List => {
                Segment::List(ListSegment::from_relative_position_slice(span, positions))
            }
            SegmentKind::Bits => {
                Segment::Bits(BitSegment::from_relative_position_slice(span, positions))
            }
            SegmentKind::Ranges => Segment::Ranges(RangeSegment::from_relative_position_slice(
                span,
                positions,
                Some(run_count),
            )),
        }
    }

    /// Builds a segment from a slice of runs within `span`.
    ///
    /// Inputs
    /// - `runs` are contiguous ranges described by [`Run`], relative to `span.start`.
    /// - Must be sorted, non-overlapping, and within `span`.
    ///
    /// Representation
    /// - Chooses `Ranges`, `List`, or `Bits` based on coverage and run count.
    /// - May yield `Empty` or `Full` when applicable.
    ///
    /// Complexity
    /// - O(n) over the number of runs and covered positions.
    ///
    /// Panics
    /// - May panic if `span` or `runs` are invalid.
    pub fn from_run_slice(span: Range<u64>, runs: &[Run]) -> Segment {
        let span_len = span.end - span.start;
        let run_count = runs.len();
        let pos_count = runs.iter().map(|r| r.len()).sum::<usize>();
        let kind =
            Self::infer_optimal_kind_by_size(span_len, pos_count, run_count, SegmentKind::Ranges);
        match kind {
            SegmentKind::Empty => Segment::empty(span),
            SegmentKind::Full => Segment::full(span),
            SegmentKind::List => {
                let span_start = span.start;
                Segment::List(ListSegment::from_positions_with_count(
                    span,
                    runs.iter().flat_map(|&r| r.rebase(span_start)),
                    pos_count,
                ))
            }
            SegmentKind::Bits => Segment::Bits(BitSegment::from_runs(span, runs.iter().copied())),
            SegmentKind::Ranges => Segment::Ranges(RangeSegment::new(span, runs.to_vec())),
        }
    }

    /// Builds a segment from a bitset for the given `span`.
    ///
    /// Inputs
    /// - `bits` must have width equal to `span.len()` (1 bit per position).
    ///
    /// Representation
    /// - Picks among `Ranges`, `List`, or `Bits` based on density and run count.
    /// - May yield `Empty` or `Full` when applicable.
    ///
    /// Complexity
    /// - O(n) over the bitset size (fixed upper bound: `SPAN/8` bytes).
    ///
    /// Panics
    /// - May panic if `span` is invalid.
    pub fn from_bits(span: Range<u64>, bits: BitArray) -> Segment {
        let span_len = span.end - span.start;
        let run_count = bits.count_runs();
        let pos_count = bits.count_ones();
        let kind =
            Self::infer_optimal_kind_by_size(span_len, pos_count, run_count, SegmentKind::Ranges);
        match kind {
            SegmentKind::Empty => Segment::empty(span),
            SegmentKind::Full => Segment::full(span),
            SegmentKind::List => {
                let span_start = span.start;
                Segment::List(ListSegment::from_positions_with_count(
                    span,
                    bits.iter().map(|pos| pos as u64 + span_start),
                    pos_count,
                ))
            }
            SegmentKind::Bits => Segment::Bits(BitSegment::new(span, bits)),
            SegmentKind::Ranges => {
                let span_start = span.start;
                Segment::Ranges(RangeSegment::from_ranges_with_count(
                    span,
                    bits.ranges_iter()
                        .map(|r| r.start as u64 + span_start..r.end as u64 + span_start),
                    run_count,
                ))
            }
        }
    }

    /// Re-encodes this segment in-place into a representation that is likely the
    /// most efficient for its current contents.
    ///
    /// Behavior
    /// - Uses the same heuristics as [`Segment::infer_optimal_kind`] to decide
    ///   between `Empty`, `Full`, `List`, `Ranges`, and `Bits`.
    /// - When the current representation is already optimal, this is a no-op.
    /// - Returns the resulting [`SegmentKind`].
    ///
    /// Complexity
    /// - Typically O(n) with respect to the size of the current representation.
    pub fn optimize(&mut self) -> SegmentKind {
        let kind = self.kind();
        let optimal_kind = self.infer_optimal_kind();
        if optimal_kind == kind {
            self.compact();
            return kind;
        }

        let span = self.span();
        let new_self = match optimal_kind {
            SegmentKind::Empty => Segment::Empty(EmptySegment::new(span)),
            SegmentKind::Full => Segment::Full(FullSegment::new(span)),
            SegmentKind::List => match self {
                Segment::Empty(_) | Segment::Full(_) | Segment::List(_) => {
                    panic!("Segment kind switch {kind:?} -> {optimal_kind:?} unexpected")
                }
                Segment::Bits(bits) => Segment::List(bits.to_list()),
                Segment::Ranges(ranges) => Segment::List(ranges.to_list()),
            },
            SegmentKind::Bits => match self {
                Segment::Empty(_) | Segment::Full(_) | Segment::Bits(_) => {
                    panic!("Segment kind switch {kind:?} -> {optimal_kind:?} unexpected")
                }
                Segment::List(list) => Segment::Bits(list.to_bits()),
                Segment::Ranges(ranges) => Segment::Bits(ranges.to_bits()),
            },
            SegmentKind::Ranges => match self {
                Segment::Empty(_) | Segment::Full(_) | Segment::Ranges(_) => {
                    panic!("Segment kind switch {kind:?} -> {optimal_kind:?} unexpected")
                }
                Segment::List(list) => Segment::Ranges(list.to_ranges()),
                Segment::Bits(bits) => Segment::Ranges(bits.to_ranges()),
            },
        };
        *self = new_self;
        optimal_kind
    }

    /// Compacts the internal storage of this segment without changing its contents.
    ///
    /// Behavior
    /// - For `List` and `Ranges`, shrinks internal buffers/capacities and
    ///   removes encoding-specific slack to reduce heap usage.
    /// - No-op for `Bits`, `Empty`, and `Full`.
    ///
    /// Semantics
    /// - Does not change which positions are set and does not change the segment kind.
    ///
    /// Complexity
    /// - O(n) in the size of the current representation; O(1) for `Empty`/`Full`/`Bits`.
    pub fn compact(&mut self) {
        match self {
            Segment::List(segment) => segment.compact(),
            Segment::Bits(segment) => segment.compact(),
            Segment::Ranges(segment) => segment.compact(),
            _ => (),
        }
    }

    /// Converts this segment in-place to the `Bits` representation when applicable.
    ///
    /// Behavior
    /// - For `List` and `Ranges`, rebuilds a dense bitset for the segment's span and
    ///   replaces the current variant with `Segment::Bits`.
    /// - No-op if the segment is `Empty`, `Full`, or already `Bits`.
    ///
    /// Use cases
    /// - Prefer before operations that benefit from bitwise ops or random access.
    ///
    /// Complexity
    /// - O(n) in the size of the current representation (`positions` or `runs`).
    /// - Allocates a fixed buffer of `SPAN / 8` bytes for the bitset.
    ///
    /// Postconditions
    /// - After return, `self.kind()` is `SegmentKind::Bits` unless it was `Empty` or `Full`.
    pub fn convert_to_bits(&mut self) {
        let converted = match self {
            Segment::Empty(_) | Segment::Full(_) | Segment::Bits(_) => return,
            Segment::List(list) => list.to_bits(),
            Segment::Ranges(ranges) => ranges.to_bits(),
        };
        *self = Segment::Bits(converted);
    }

    /// Returns the logical interval `[start, end)` owned by this segment.
    ///
    /// Each segment is responsible for a fixed, half-open range of logical positions,
    /// independent of which individual positions are set within that range. This method
    /// returns that ownership interval.
    ///
    /// Alignment and size:
    /// - `start` is always a multiple of [`Segment::SPAN`].
    /// - The length is typically [`Segment::SPAN`]; the final segment in a position set
    ///   may be shorter (but never zero).
    ///
    /// The returned range is half-open: `start` is inclusive and `end` is exclusive.
    #[inline]
    pub fn span(&self) -> Range<u64> {
        match self {
            Segment::Empty(empty) => empty.span(),
            Segment::Full(full) => full.span(),
            Segment::List(list) => list.span(),
            Segment::Bits(bits) => bits.span(),
            Segment::Ranges(ranges) => ranges.span(),
        }
    }

    /// Returns the discriminant of this segment as a [`SegmentKind`].
    ///
    /// See also [`Segment::span`] for the logical range a segment covers.
    #[inline]
    pub fn kind(&self) -> SegmentKind {
        match self {
            Segment::Empty(_) => SegmentKind::Empty,
            Segment::Full(_) => SegmentKind::Full,
            Segment::List(_) => SegmentKind::List,
            Segment::Bits(_) => SegmentKind::Bits,
            Segment::Ranges(_) => SegmentKind::Ranges,
        }
    }

    /// Returns the number of positions set within this segment's span.
    ///
    /// Notes
    /// - The count only includes positions in the segment’s ownership interval
    ///   returned by [`Segment::span`].
    pub fn count_positions(&self) -> usize {
        match self {
            Segment::Empty(empty) => empty.count_positions(),
            Segment::Full(full) => full.count_positions(),
            Segment::List(list) => list.count_positions(),
            Segment::Bits(bits) => bits.count_positions(),
            Segment::Ranges(ranges) => ranges.count_positions(),
        }
    }

    /// Returns whether the given absolute position is set in this segment.
    ///
    /// Contracts
    /// - `pos` must lie within this segment’s [`span`](Segment::span). Concrete
    ///   implementations may enforce this with assertions.
    ///
    /// Behavior is undefined if `pos` is outside the span.
    #[inline]
    pub fn contains(&self, pos: u64) -> bool {
        match self {
            Segment::Empty(empty) => empty.contains(pos),
            Segment::Full(full) => full.contains(pos),
            Segment::List(list) => list.contains(pos),
            Segment::Bits(bits) => bits.contains(pos),
            Segment::Ranges(ranges) => ranges.contains(pos),
        }
    }

    /// Calls `f(rank, pos)` for every set position in ascending order.
    ///
    /// - `rank` is the 0-based index among set positions within this segment.
    /// - `pos` is the absolute position (i.e., `self.span.start + relative_index`).
    ///
    /// Equivalent to:
    /// `for (i, p) in self.positions().enumerate() { f(i, p) }`
    ///
    /// # Returns
    ///
    /// The number of positions (total rank).
    pub fn for_each_position(&self, f: impl FnMut(usize, u64)) -> usize {
        match self {
            Segment::Empty(segment) => segment.for_each_position(f),
            Segment::Full(segment) => segment.for_each_position(f),
            Segment::List(segment) => segment.for_each_position(f),
            Segment::Bits(segment) => segment.for_each_position(f),
            Segment::Ranges(segment) => segment.for_each_position(f),
        }
    }

    /// Copies all set absolute positions in ascending order into the provided buffer.
    ///
    /// Returns the number of positions written into `out`.
    ///
    /// Semantics
    /// - Positions are written in the same order as [`Segment::positions`] and
    ///   [`Segment::for_each_position`] would produce them.
    /// - Writes exactly `self.count_positions()` items.
    ///
    /// Contracts
    /// - `out.len()` must be at least `self.count_positions()`. If it is smaller, this
    ///   function will panic due to out-of-bounds indexing.
    /// - If `out.len()` is larger, the extra elements are left unchanged.
    ///
    /// Notes
    /// - Prefer this when you want a zero-allocation copy into a caller-provided buffer.
    /// - If you don’t know the required size, call `self.count_positions()` first or
    ///   consider returning a `Vec<u64>` from a helper.
    pub fn collect_positions(&self, out: &mut [u64]) -> usize {
        debug_assert!(
            out.len() >= self.count_positions(),
            "buffer too small for collect_positions"
        );
        self.for_each_position(|i, pos| {
            out[i] = pos;
        })
    }

    /// Sets the given absolute position within this segment.
    ///
    /// Contracts
    /// - `pos` must lie within this segment’s [`span`](Self::span).
    ///
    /// Behavior
    /// - `Bits`: sets the bit for `pos`.
    /// - `Full`: no-op (the position is already set).
    /// - `Empty`: converts to `Bits` and sets `pos` in the new bitset.
    /// - `List`/`Ranges`: unsupported and will panic.
    ///
    /// Complexity
    /// - O(1) for `Bits` and `Full`.
    /// - First call on `Empty` allocates a fixed `SPAN/8`-byte bitset.
    ///
    /// Notes
    /// - For repeated single-position updates on non-bit representations, call
    ///   [`convert_to_bits`](Self::convert_to_bits) first to avoid panics and re-allocate only once.
    #[inline]
    pub fn set(&mut self, pos: u64) {
        match self {
            Segment::Bits(bits) => bits.set(pos),
            Segment::Full(_) => (),
            _ => self.set_pos_fallback(pos),
        }
    }

    /// Returns an iterator over all absolute positions in this segment.
    pub fn positions(&self) -> PositionsIter<'_> {
        match self {
            Segment::Empty(_) => PositionsIter::Empty(std::iter::empty()),
            Segment::Full(segment) => PositionsIter::Full(segment.span()),
            Segment::List(segment) => PositionsIter::List(segment.positions()),
            Segment::Bits(segment) => PositionsIter::Bits(segment.positions()),
            Segment::Ranges(segment) => PositionsIter::Ranges(segment.positions()),
        }
    }

    /// Returns an iterator over absolute positions within the specified range.
    pub fn positions_within(&self, range: Range<u64>) -> PositionsIter<'_> {
        match self {
            Segment::Empty(_) => PositionsIter::Empty(std::iter::empty()),
            Segment::Full(segment) => PositionsIter::Full(segment.positions_within(range)),
            Segment::List(segment) => PositionsIter::List(segment.positions_within(range)),
            Segment::Bits(segment) => PositionsIter::Bits(segment.positions_within(range)),
            Segment::Ranges(segment) => PositionsIter::Ranges(segment.positions_within(range)),
        }
    }

    /// Returns an iterator over contiguous ranges of set positions.
    pub fn ranges(&self) -> RangesIter<'_> {
        match self {
            Segment::Empty(_) => RangesIter::Empty(std::iter::empty()),
            Segment::Full(segment) => RangesIter::Full(Some(segment.span())),
            Segment::List(segment) => RangesIter::List(segment.ranges()),
            Segment::Bits(segment) => RangesIter::Bits(segment.ranges()),
            Segment::Ranges(segment) => RangesIter::Ranges(segment.ranges()),
        }
    }

    /// Returns an iterator over contiguous ranges within the specified absolute range.
    pub fn ranges_within(&self, range: Range<u64>) -> RangesIter<'_> {
        match self {
            Segment::Empty(_) => RangesIter::Empty(std::iter::empty()),
            Segment::Full(segment) => RangesIter::Full(segment.ranges_within(range)),
            Segment::List(segment) => RangesIter::List(segment.ranges_within(range)),
            Segment::Bits(segment) => RangesIter::Bits(segment.ranges_within(range)),
            Segment::Ranges(segment) => RangesIter::Ranges(segment.ranges_within(range)),
        }
    }

    /// Computes the union of two segments with the same `span`.
    ///
    /// Behavior
    /// - Fast paths for matching representations when result fits thresholds.
    /// - Falls back to bitwise union and then optimizes the result.
    ///
    /// Contracts
    /// - Panics if `self.span() != other.span()`.
    ///
    /// Complexity
    /// - O(n) in the size of the inputs’ current representations.
    pub fn union(&self, other: &Segment) -> Segment {
        assert_eq!(
            self.span(),
            other.span(),
            "Segment spans must match for union: left={:?}, right={:?}",
            self.span(),
            other.span()
        );

        match (self, other) {
            (Segment::Empty(_), other) => other.clone(),
            (other, Segment::Empty(_)) => other.clone(),

            (Segment::Full(full), _) | (_, Segment::Full(full)) => Segment::Full(full.clone()),

            (Segment::Bits(left), Segment::Bits(right)) => Segment::Bits(left.union(right)),
            (Segment::List(left), Segment::List(right))
                if left.count_positions() + right.count_positions()
                    <= Segment::MAX_LIST_LEN as usize =>
            {
                Segment::List(left.union(right))
            }
            (Segment::Ranges(left), Segment::Ranges(right))
                if left.count_runs() + right.count_runs() <= Segment::MAX_RANGES_LEN as usize =>
            {
                Segment::Ranges(left.union(right))
            }

            // For mixed types or when the result size estimation might exceed the representation
            // thresholds, convert both to bits and perform union.
            (left, right) => {
                let left_bits = left.to_bits();
                let right_bits = right.to_bits();
                let result_bits = left_bits.union(&right_bits);
                // Optimize the result to choose the best representation
                let mut result = Segment::Bits(result_bits);
                result.optimize();
                result
            }
        }
    }

    /// In-place union with `other`.
    pub fn union_with(&mut self, other: &Segment) {
        let span = self.span();
        assert_eq!(
            span,
            other.span(),
            "Segment spans must match for union: left={:?}, right={:?}",
            span,
            other.span()
        );

        if matches!(self, Segment::Empty(_)) {
            *self = other.clone();
            return;
        }

        if matches!(other, Segment::Full(_)) {
            *self = Segment::Full(FullSegment::new(span));
            return;
        }

        match (self, other) {
            (_, Segment::Empty(_)) => (),
            (Segment::Full(_), _) => (),
            (Segment::Bits(this), Segment::Bits(right)) => {
                this.union_with(right);
            }
            (Segment::List(this), Segment::List(right))
                if this.count_positions() + right.count_positions()
                    <= Segment::MAX_LIST_LEN as usize =>
            {
                this.union_with(right);
            }
            (Segment::Ranges(this), Segment::Ranges(right))
                if this.count_runs() + right.count_runs() <= Segment::MAX_RANGES_LEN as usize =>
            {
                *this = this.union(right);
            }

            // For mixed types or when the result size estimation might exceed the representation
            // thresholds, convert both to bits and perform union.
            (Segment::Bits(this), right) => {
                this.union_with(&right.to_bits());
            }
            (this, right) => {
                let mut bits = this.to_bits().into_owned();
                let right_bits = right.to_bits();
                bits.union_with(&right_bits);
                // Optimize the result to choose the best representation
                let mut result = Segment::Bits(bits);
                result.optimize();
                *this = result;
            }
        }
    }

    /// Merges (unions) `other` into `self`, consuming `other`.
    ///
    /// Semantics
    /// - Set operation: `self <- self ∪ other`.
    /// - Equivalent to calling [`union_with`] with a borrow of `other`, but takes
    ///   ownership so it can avoid cloning in fast paths (e.g. when `self` is empty).
    /// - After return, all positions set in either original segment are set in `self`.
    ///
    /// Fast paths
    /// - If `self` is `Empty`, it is replaced by `other` (moved, no clone).
    /// - If `other` is `Full`, `self` becomes `Full`.
    /// - Otherwise it defers to the same logic as [`union_with`].
    pub fn merge_with(&mut self, other: Segment) {
        let span = self.span();
        assert_eq!(
            span,
            other.span(),
            "Segment spans must match for merge: left={:?}, right={:?}",
            span,
            other.span()
        );

        if matches!(self, Segment::Empty(_)) || matches!(other, Segment::Full(_)) {
            *self = other;
            return;
        }

        if matches!(self, Segment::Full(_)) || matches!(other, Segment::Empty(_)) {
            return;
        }

        self.union_with(&other);
    }

    /// Computes the intersection of two segments with the same `span`.
    ///
    /// Behavior
    /// - Fast paths for matching representations.
    /// - Otherwise converts to bits, intersects, then optimizes.
    ///
    /// Contracts
    /// - Panics if `self.span() != other.span()`.
    ///
    /// Complexity
    /// - O(n) in the size of the inputs’ current representations.
    pub fn intersect(&self, other: &Segment) -> Segment {
        assert_eq!(
            self.span(),
            other.span(),
            "Segment spans must match for intersect: left={:?}, right={:?}",
            self.span(),
            other.span()
        );

        match (self, other) {
            // Empty intersect anything = empty
            (Segment::Empty(empty), _) | (_, Segment::Empty(empty)) => {
                Segment::Empty(empty.clone())
            }

            // Full intersect anything = the other
            (Segment::Full(_), other) => other.clone(),
            (other, Segment::Full(_)) => other.clone(),

            // Same type intersections
            (Segment::Bits(left), Segment::Bits(right)) => Segment::Bits(left.intersect(right)),
            (Segment::List(left), Segment::List(right)) => Segment::List(left.intersect(right)),
            (Segment::Ranges(left), Segment::Ranges(right)) => {
                Segment::Ranges(left.intersect(right))
            }

            // For mixed types, convert both to bits and perform intersection
            (left, right) => {
                let left_bits = left.to_bits();
                let right_bits = right.to_bits();
                let result_bits = left_bits.intersect(&right_bits);
                // Optimize the result to choose the best representation
                let mut result = Segment::Bits(result_bits);
                result.optimize();
                result
            }
        }
    }

    /// In-place intersection with `other`.
    pub fn intersect_with(&mut self, other: &Segment) {
        let span = self.span();
        assert_eq!(
            span,
            other.span(),
            "Segment spans must match for intersect_with: left={:?}, right={:?}",
            span,
            other.span()
        );

        if matches!(self, Segment::Empty(_)) || matches!(other, Segment::Full(_)) {
            return;
        }

        if matches!(other, Segment::Empty(_)) {
            *self = Segment::Empty(EmptySegment::new(span));
            return;
        }

        if matches!(self, Segment::Full(_)) {
            *self = other.clone();
            return;
        }

        match (self, other) {
            (Segment::Bits(this), Segment::Bits(right)) => {
                this.intersect_with(right);
            }
            (Segment::List(this), Segment::List(right)) => {
                this.intersect_with(right);
            }
            (Segment::Ranges(this), Segment::Ranges(right)) => {
                *this = this.intersect(right);
            }

            // If self already bits, just AND with other's bits view.
            (Segment::Bits(this), right) => {
                this.intersect_with(&right.to_bits());
            }

            // Mixed types (self not Bits). Convert self to bits, AND, then optimize.
            (this, right) => {
                let mut bits = this.to_bits().into_owned();
                let right_bits = right.to_bits();
                bits.intersect_with(&right_bits);
                let mut result = Segment::Bits(bits);
                result.optimize();
                *this = result;
            }
        }
    }

    pub fn complement(&self) -> Segment {
        match self {
            Segment::Empty(empty) => Segment::Full(empty.complement()),
            Segment::Full(full) => Segment::Empty(full.complement()),
            Segment::List(list) => {
                let complement_bits = list.complement_as_bits();
                Segment::Bits(complement_bits)
            }
            Segment::Bits(bits) => {
                let complement_bits = bits.complement();
                let mut result = Segment::Bits(complement_bits);
                result.optimize();
                result
            }
            Segment::Ranges(ranges) => {
                let complement_ranges = ranges.complement();
                let mut result = Segment::Ranges(complement_ranges);
                result.optimize();
                result
            }
        }
    }

    pub fn complement_in_place(&mut self) {
        let span = self.span();
        match self {
            Segment::Empty(_) => *self = Segment::Full(FullSegment::new(span)),
            Segment::Full(_) => *self = Segment::Empty(EmptySegment::new(span)),
            Segment::List(list) => {
                let complement_bits = list.complement_as_bits();
                *self = Segment::Bits(complement_bits);
            }
            Segment::Bits(bits) => {
                let complement_bits = bits.complement();
                let mut result = Segment::Bits(complement_bits);
                result.optimize();
                *self = result;
            }
            Segment::Ranges(ranges) => {
                let complement_ranges = ranges.complement();
                let mut result = Segment::Ranges(complement_ranges);
                result.optimize();
                *self = result;
            }
        }
    }

    /// Checks if this segment is equal to another segment.
    ///
    /// Two segments are considered equal if they have the same span and represent
    /// the same set of positions, regardless of their internal representation.
    ///
    /// # Arguments
    ///
    /// * `other` - The segment to compare with
    ///
    /// # Returns
    ///
    /// `true` if the segments represent the same set of positions within the same span,
    /// `false` otherwise.
    ///
    pub fn is_equal_to(&self, other: &Segment) -> bool {
        if self.span() != other.span() {
            return false;
        }
        match (self, other) {
            (Segment::Empty(_), Segment::Empty(_)) => true,
            (Segment::Empty(_), other) => other.count_positions() == 0,
            (other, Segment::Empty(_)) => other.count_positions() == 0,
            (Segment::Full(_), Segment::Full(_)) => true,
            (Segment::Full(full), other) => {
                let span_len = (full.span().end - full.span().start) as usize;
                other.count_positions() == span_len
            }
            (other, Segment::Full(full)) => {
                let span_len = (full.span().end - full.span().start) as usize;
                other.count_positions() == span_len
            }
            (Segment::Bits(left), Segment::Bits(right)) => left.is_equal_to(right),
            (Segment::List(left), Segment::List(right)) => left.is_equal_to(right),
            (Segment::Ranges(left), Segment::Ranges(right)) => left.is_equal_to(right),
            (left, right) => {
                let left_bits = left.to_bits();
                let right_bits = right.to_bits();
                left_bits.is_equal_to(&*right_bits)
            }
        }
    }

    /// Computes the most efficient [`SegmentKind`] for the current contents
    /// without modifying the segment.
    ///
    /// Behavior
    /// - Returns [`SegmentKind::Empty`] or [`SegmentKind::Full`] when applicable;
    ///   otherwise picks among `List`, `Ranges`, or `Bits`.
    pub fn infer_optimal_kind(&self) -> SegmentKind {
        let span = self.span();
        let span_len = span.end - span.start;

        match self {
            Segment::Empty(_) => SegmentKind::Empty,
            Segment::Full(_) => SegmentKind::Full,
            Segment::List(list) => {
                let n = list.count_positions();
                let runs = list.count_runs();
                Self::infer_optimal_kind_by_size(span_len, n, runs, SegmentKind::List)
            }
            Segment::Bits(bits) => {
                let n = bits.count_positions();
                let runs = bits.count_runs();
                Self::infer_optimal_kind_by_size(span_len, n, runs, SegmentKind::Bits)
            }
            Segment::Ranges(ranges) => {
                let n = ranges.count_positions();
                let runs = ranges.count_runs();
                Self::infer_optimal_kind_by_size(span_len, n, runs, SegmentKind::Ranges)
            }
        }
    }

    /// Validates basic invariants.
    ///
    /// Checks
    /// - Span shape and non-emptiness.
    /// - Lightweight representation-specific consistency.
    ///
    /// Panics
    /// - Panics if any invariant is violated.
    pub fn check_basic_invariants(&self) {
        match self {
            Segment::Empty(segment) => assert!(!segment.span().is_empty()),
            Segment::Full(segment) => assert!(!segment.span().is_empty()),
            Segment::List(segment) => segment.check_basic_invariants(),
            Segment::Bits(segment) => segment.check_basic_invariants(),
            Segment::Ranges(segment) => segment.check_basic_invariants(),
        }
    }

    /// Validates full invariants in debug builds.
    ///
    /// Checks
    /// - Ordering, bounds, uniqueness, run structure, and internal consistency.
    ///
    /// Complexity
    /// - O(n) in the size of the current representation.
    ///
    /// Panics
    /// - Panics if any invariant is violated. Intended for tests and debug use.
    pub fn check_full_invariants(&self) {
        self.check_basic_invariants();
        match self {
            Segment::Empty(_) => (),
            Segment::Full(_) => (),
            Segment::List(segment) => segment.check_full_invariants(),
            Segment::Bits(segment) => segment.check_full_invariants(),
            Segment::Ranges(segment) => segment.check_full_invariants(),
        }
    }

    /// Returns a `Bits` view of this segment.
    ///
    /// Behavior
    /// - Borrows if already `Bits`; otherwise allocates and converts.
    ///
    /// Complexity
    /// - O(1) if already `Bits`, else O(n) in the current representation.
    ///
    /// Semantics
    /// - Does not modify `self`.
    pub fn to_bits(&self) -> Cow<'_, BitSegment> {
        match self {
            Segment::Empty(segment) => Cow::Owned(segment.to_bits()),
            Segment::Full(segment) => Cow::Owned(segment.to_bits()),
            Segment::List(segment) => Cow::Owned(segment.to_bits()),
            Segment::Bits(segment) => Cow::Borrowed(segment),
            Segment::Ranges(segment) => Cow::Owned(segment.to_bits()),
        }
    }

    /// Returns the number of heap-allocated bytes used by this segment.
    pub fn heap_size_bytes(&self) -> usize {
        match self {
            Segment::Empty(segment) => segment.heap_size_bytes(),
            Segment::Full(segment) => segment.heap_size_bytes(),
            Segment::List(segment) => segment.heap_size_bytes(),
            Segment::Bits(segment) => segment.heap_size_bytes(),
            Segment::Ranges(segment) => segment.heap_size_bytes(),
        }
    }
}

impl Segment {
    fn infer_optimal_kind_by_size(
        span_len: u64,
        pos_count: usize,
        run_count: usize,
        current: SegmentKind,
    ) -> SegmentKind {
        if pos_count == 0 {
            return SegmentKind::Empty;
        }
        if span_len as usize == pos_count {
            return SegmentKind::Full;
        }

        let bits_size = span_len.div_ceil(8);
        let list_size = 2u64 * (pos_count as u64);
        let ranges_size = 4u64 * (run_count as u64);

        let min_size = list_size.min(ranges_size).min(bits_size);

        // Prefer to keep current representation if it is among the minima to avoid churn.
        match current {
            SegmentKind::List if list_size == min_size => return SegmentKind::List,
            SegmentKind::Ranges if ranges_size == min_size => return SegmentKind::Ranges,
            SegmentKind::Bits if bits_size == min_size => return SegmentKind::Bits,
            _ => {}
        }

        // Otherwise, prefer List for very sparse data, then Ranges, then Bits.
        if list_size == min_size {
            SegmentKind::List
        } else if ranges_size == min_size {
            SegmentKind::Ranges
        } else {
            SegmentKind::Bits
        }
    }

    #[cold]
    fn set_pos_fallback(&mut self, pos: u64) {
        match self {
            Segment::Bits(_) => unreachable!(),
            Segment::Empty(_) => {
                let span = self.span();
                let mut bits = BitSegment::empty(span);
                bits.set(pos);
                *self = Segment::Bits(bits);
            }
            Segment::List(_) | Segment::Ranges(_) => {
                panic!("Unsupported segment kind for `set`")
            }
            Segment::Full(_) => unreachable!(),
        }
    }
}

#[derive(Clone)]
pub struct EmptySegment(Range<u64>);

impl EmptySegment {
    /// Creates a new empty segment for the given span.
    #[inline]
    pub fn new(span: Range<u64>) -> Self {
        debug_assert!(span.start < span.end);
        debug_assert!((span.end - span.start) <= Segment::SPAN);
        debug_assert!(span.start.is_multiple_of(Segment::SPAN));
        EmptySegment(span)
    }

    pub fn from_positions(span: Range<u64>, positions: impl Iterator<Item = u64>) -> Self {
        assert_eq!(positions.count(), 0);
        Self::new(span)
    }

    pub fn from_ranges(span: Range<u64>, ranges: impl Iterator<Item = Range<u64>>) -> Self {
        assert_eq!(ranges.map(|r| (r.end - r.start) as usize).sum::<usize>(), 0);
        Self::new(span)
    }

    /// Returns a copy of the segment's span.
    #[inline]
    pub fn span(&self) -> Range<u64> {
        self.0.clone()
    }

    /// Returns the number of positions set in this segment (always 0).
    #[inline]
    pub fn count_positions(&self) -> usize {
        0
    }

    /// Returns the number of contiguous runs (always 0).
    #[inline]
    pub fn count_runs(&self) -> usize {
        0
    }

    /// Returns an iterator over absolute positions (always empty).
    #[inline]
    pub fn positions(&self) -> std::iter::Empty<u64> {
        std::iter::empty()
    }

    /// Returns an iterator over absolute positions within the given range (always empty).
    #[inline]
    pub fn positions_within(&self, _range: Range<u64>) -> std::iter::Empty<u64> {
        std::iter::empty()
    }

    /// Checks whether `pos` is contained in this segment (always false; asserts pos within span).
    #[inline]
    pub fn contains(&self, pos: u64) -> bool {
        debug_assert!(self.0.contains(&pos));
        false
    }

    /// Calls `f(rank, pos)` for every set position in ascending order.
    /// Since this is an empty segment, does nothing.
    pub fn for_each_position(&self, _f: impl FnMut(usize, u64)) -> usize {
        0
    }

    /// Union of two empty segments (still empty). Panics if spans differ.
    pub fn union(&self, other: &EmptySegment) -> EmptySegment {
        assert_eq!(self.0, other.0, "EmptySegment spans must match for union");
        self.clone()
    }

    /// Intersection of two empty segments (still empty). Panics if spans differ.
    pub fn intersect(&self, other: &EmptySegment) -> EmptySegment {
        assert_eq!(
            self.0, other.0,
            "EmptySegment spans must match for intersect"
        );
        self.clone()
    }

    /// Complement within span: full segment.
    pub fn complement(&self) -> FullSegment {
        FullSegment(self.0.clone())
    }

    pub fn to_bits(&self) -> BitSegment {
        BitSegment::empty(self.span())
    }

    pub fn to_list(&self) -> ListSegment {
        ListSegment::empty(self.span())
    }

    pub fn to_ranges(&self) -> RangeSegment {
        RangeSegment::empty(self.span())
    }

    /// Returns the number of heap-allocated bytes used by this segment.
    pub fn heap_size_bytes(&self) -> usize {
        0
    }
}

impl std::ops::BitOr for &EmptySegment {
    type Output = EmptySegment;
    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

impl std::ops::BitAnd for &EmptySegment {
    type Output = EmptySegment;
    fn bitand(self, rhs: Self) -> Self::Output {
        self.intersect(rhs)
    }
}

impl std::ops::Not for &EmptySegment {
    type Output = FullSegment;
    fn not(self) -> Self::Output {
        self.complement()
    }
}

#[derive(Clone)]
pub struct FullSegment(Range<u64>);

impl FullSegment {
    /// Creates a new full segment for the given span.
    #[inline]
    pub fn new(span: Range<u64>) -> Self {
        assert!(span.start < span.end);
        assert!((span.end - span.start) <= Segment::SPAN);
        assert!(span.start.is_multiple_of(Segment::SPAN));
        FullSegment(span)
    }

    /// Returns a copy of the segment's span.
    #[inline]
    pub fn span(&self) -> Range<u64> {
        self.0.clone()
    }

    /// Returns the number of positions set in this segment (span length).
    #[inline]
    pub fn count_positions(&self) -> usize {
        (self.0.end - self.0.start) as usize
    }

    /// Returns the number of contiguous runs (0 for empty span, else 1).
    #[inline]
    pub fn count_runs(&self) -> usize {
        1
    }

    /// Returns an iterator over all absolute positions in ascending order.
    #[inline]
    pub fn positions(&self) -> std::ops::Range<u64> {
        self.0.start..self.0.end
    }

    /// Returns an iterator over absolute positions within the specified range.
    #[inline]
    pub fn positions_within(&self, range: Range<u64>) -> Range<u64> {
        let start = range.start.max(self.0.start);
        let end = range.end.min(self.0.end);
        start..end
    }

    #[inline]
    pub fn ranges_within(&self, range: Range<u64>) -> Option<Range<u64>> {
        let start = range.start.max(self.0.start);
        let end = range.end.min(self.0.end);
        if start < end { Some(start..end) } else { None }
    }

    /// Checks whether `pos` is contained in this segment (always true if pos within span).
    #[inline]
    pub fn contains(&self, pos: u64) -> bool {
        debug_assert!(self.0.contains(&pos));
        true
    }

    /// Calls `f(rank, pos)` for every set position in ascending order.
    ///
    /// - `rank` is the 0-based index among set positions within this segment.
    /// - `pos` is the absolute position (i.e., `self.span.start + relative_index`).
    pub fn for_each_position(&self, mut f: impl FnMut(usize, u64)) -> usize {
        for (i, pos) in self.0.clone().enumerate() {
            f(i, pos);
        }
        (self.0.end - self.0.start) as usize
    }

    /// Union of two full segments (still full). Panics if spans differ.
    pub fn union(&self, other: &FullSegment) -> FullSegment {
        assert_eq!(self.0, other.0, "FullSegment spans must match for union");
        self.clone()
    }

    /// Intersection of two full segments (still full). Panics if spans differ.
    pub fn intersect(&self, other: &FullSegment) -> FullSegment {
        assert_eq!(
            self.0, other.0,
            "FullSegment spans must match for intersect"
        );
        self.clone()
    }

    /// Complement within span: empty segment.
    pub fn complement(&self) -> EmptySegment {
        EmptySegment(self.0.clone())
    }

    pub fn to_bits(&self) -> BitSegment {
        BitSegment::full(self.span())
    }

    pub fn to_list(&self) -> ListSegment {
        ListSegment::full(self.span())
    }

    pub fn to_ranges(&self) -> RangeSegment {
        RangeSegment::full(self.span())
    }

    /// Returns the number of heap-allocated bytes used by this segment.
    pub fn heap_size_bytes(&self) -> usize {
        0
    }
}

impl std::ops::BitOr for &FullSegment {
    type Output = FullSegment;
    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

impl std::ops::BitAnd for &FullSegment {
    type Output = FullSegment;
    fn bitand(self, rhs: Self) -> Self::Output {
        self.intersect(rhs)
    }
}

impl std::ops::Not for &FullSegment {
    type Output = EmptySegment;
    fn not(self) -> Self::Output {
        self.complement()
    }
}

#[derive(Clone)]
pub enum PositionsIter<'a> {
    Empty(std::iter::Empty<u64>),
    Full(Range<u64>),
    List(list::PositionsIter<'a>),
    Bits(bits::PositionsIter<'a>),
    Ranges(ranges::PositionsIter<'a>),
}

impl<'a> Iterator for PositionsIter<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            PositionsIter::Empty(it) => it.next(),
            PositionsIter::Full(it) => it.next(),
            PositionsIter::List(it) => it.next(),
            PositionsIter::Bits(it) => it.next(),
            PositionsIter::Ranges(it) => it.next(),
        }
    }
}

#[derive(Clone)]
pub enum RangesIter<'a> {
    Empty(std::iter::Empty<Range<u64>>),
    Full(Option<Range<u64>>),
    List(list::RangesIter<'a>),
    Bits(bits::RangesIter<'a>),
    Ranges(ranges::RangesIter<'a>),
}

impl<'a> Iterator for RangesIter<'a> {
    type Item = Range<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            RangesIter::Empty(it) => it.next(),
            RangesIter::Full(it) => it.take(),
            RangesIter::List(it) => it.next(),
            RangesIter::Bits(it) => it.next(),
            RangesIter::Ranges(it) => it.next(),
        }
    }
}
