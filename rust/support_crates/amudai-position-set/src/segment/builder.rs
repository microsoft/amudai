//! Segment builder for efficiently constructing a `Segment` over a fixed span.
//!
//! This module provides `SegmentBuilder`, a stateful helper used to incrementally
//! build a `Segment` that covers a specific half-open span `[span.start, span.end)`.
//! As positions and/or ranges are appended, the builder automatically chooses the
//! most compact internal representation among:
//! - `List` of relative positions
//! - `Bits` (bitmap) of positions within the span
//! - `Ranges` (run-length encoded relative ranges)
//!
//! Invariants and expectations:
//! - Inserts must be in strictly increasing order without duplicates.
//!   The builder enforces this via `min_next_pos()` and panics if violated.
//! - Only values within the current span are accepted. Any tail that does not
//!   fit the span is returned to the caller (e.g. by `push_range` and
//!   `extend_from_position_slice`).
//! - The span must be aligned to `Segment::SPAN` and be non-empty.
//!
//! Typical usage pattern:
//! - Create the builder with `SegmentBuilder::new(span)`.
//! - Append positions via `push_position`, ranges via `push_range`, or bulk
//!   positions via `extend_from_position_slice`.
//! - Prefer `extend_from_position_slice` (bulk appends).
//! - Call `build()` to obtain the final `Segment` for the span.
//! - Reuse the builder for a new span with `reset(span)`.
//!
//! Notes on performance and representation switching:
//! - The builder starts in `List` mode and will switch to `Bits` if the position
//!   list would exceed `Segment::MAX_LIST_LEN`.
//! - For ranges, the builder prefers `Ranges` when the number of runs is small
//!   enough (<= `Segment::MAX_RANGES_LEN`), otherwise switches to `Bits`.
//! - Switching is performed lazily and existing data is migrated as needed.

use std::ops::Range;

use amudai_ranges::RangeListSlice;
use itertools::Itertools;

use crate::{
    bit_array::BitArray,
    segment::{Segment, SegmentKind, list::count_runs, ranges::Run},
};

/// Incrementally constructs a `Segment` for a given span by accepting sorted,
/// unique positions and ranges while automatically choosing the most compact
/// internal representation (`List`, `Bits`, or `Ranges`).
pub struct SegmentBuilder {
    /// The half-open span `[start, end)` of the current segment being built.
    /// Must be aligned to `Segment::SPAN` and non-empty.
    span: Range<u64>,
    /// Currently active internal representation.
    kind: SegmentKind,
    /// Minimal absolute position that may be appended next. Used to enforce that
    /// positions/ranges are strictly increasing and non-overlapping.
    min_next_pos: u64,
    /// Relative positions (offsets from `span.start`). Relevant when `kind == List`.
    values: Vec<u16>,
    /// Bitmap of set positions within the span. Relevant when `kind == Bits`.
    bits: BitArray,
    runs: Vec<Run>,
}

impl SegmentBuilder {
    /// Create a new builder for the given `span`.
    ///
    /// Panics if `span.start` is not a multiple of `Segment::SPAN` or if the span is empty.
    pub fn new(span: Range<u64>) -> SegmentBuilder {
        assert!(span.start.is_multiple_of(Segment::SPAN));
        assert!(!span.is_empty());
        SegmentBuilder {
            span: span.clone(),
            kind: SegmentKind::List,
            min_next_pos: span.start,
            values: Vec::with_capacity(64),
            bits: BitArray::empty(0),
            runs: Vec::new(),
        }
    }

    /// Reset the builder to start constructing a segment for a new `span`.
    /// Clears all internal state and sets the representation back to `List`.
    ///
    /// Panics if `span.start` is not a multiple of `Segment::SPAN` or if the span is empty.
    pub fn reset(&mut self, span: Range<u64>) {
        assert!(span.start.is_multiple_of(Segment::SPAN));
        assert!(!span.is_empty());
        assert!(span.start >= self.span.end);
        assert!(span.start >= self.min_next_pos);
        self.min_next_pos = span.start;
        self.span = span;
        self.kind = SegmentKind::List;
        self.values.clear();
        self.runs.clear();
        self.bits = BitArray::empty(0);
    }

    /// Finalize and produce a `Segment` for the current span.
    ///
    /// After building, the builder is placed into a neutral state with an empty
    /// span at `self.span.end..self.span.end`, `kind = List`, and `min_next_pos`
    /// equal to the original `span.end`. To build another segment, call `reset`.
    pub fn build(&mut self, span_end: Option<u64>) -> Segment {
        let span_end = span_end.unwrap_or(self.span.end);
        assert!(span_end > self.span.start);
        assert!(span_end >= self.min_next_pos);
        self.span = self.span.start..span_end;

        let span = self.span.clone();
        let res = match self.kind {
            SegmentKind::List => Segment::from_relative_position_slice(span, &self.values),
            SegmentKind::Bits => {
                let len = (span.end - span.start) as usize;
                let bits = std::mem::replace(&mut self.bits, BitArray::empty(0));
                let bits = bits.into_truncated(len);
                Segment::from_bits(span, bits)
            }
            SegmentKind::Ranges => Segment::from_run_slice(span, &self.runs),
            SegmentKind::Empty => Segment::empty(span),
            SegmentKind::Full => Segment::full(span),
        };
        self.kind = SegmentKind::List;
        self.values.clear();
        self.runs.clear();
        self.min_next_pos = self.span.end;
        self.span = self.span.end..self.span.end;
        res
    }

    /// Get the minimal absolute position that may be appended next.
    #[inline]
    pub fn min_next_pos(&self) -> u64 {
        self.min_next_pos
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.min_next_pos == self.span.start
    }

    #[inline]
    pub fn span(&self) -> Range<u64> {
        self.span.clone()
    }

    /// Append a single absolute position.
    ///
    /// Returns `false` if `pos` is outside the current span (i.e. does not fit);
    /// otherwise returns `true` and updates the builder state.
    ///
    /// Panics if `pos < min_next_pos()`, i.e. if inputs are not strictly
    /// increasing or contain duplicates.
    #[inline]
    pub fn push_position(&mut self, pos: u64) -> bool {
        assert!(
            pos >= self.min_next_pos,
            "Positions must be sorted and unique: pos={} < min_next_pos={}",
            pos,
            self.min_next_pos
        );
        if pos >= self.span.end {
            return false;
        }

        let rel_pos = (pos - self.span.start) as u16;
        match self.kind {
            SegmentKind::List => {
                self.values.push(rel_pos);
                if self.values.len() > Segment::MAX_LIST_LEN as usize {
                    self.switch_representation(SegmentKind::Bits);
                }
            }
            SegmentKind::Bits => {
                self.bits.set(rel_pos as usize);
            }
            SegmentKind::Ranges => {
                self.push_relative_range(rel_pos as u32..rel_pos as u32 + 1);
            }
            SegmentKind::Empty | SegmentKind::Full => {
                // Builder never remains in these kinds during construction.
                unreachable!("Builder should not be in Empty/Full during building");
            }
        }
        self.min_next_pos = pos + 1;
        true
    }

    /// Append a half-open absolute range `[range.start, range.end)`.
    ///
    /// Returns a tail range that did not fit into the current span. If the
    /// entire input range fits, the returned tail is empty (`end..end`). If the
    /// input is empty or starts beyond the span, the original `range` is
    /// returned unchanged.
    ///
    /// Panics if `range.start < min_next_pos()`.
    pub fn push_range(&mut self, range: Range<u64>) -> Range<u64> {
        assert!(
            range.start >= self.min_next_pos,
            "range.start: {}, min_next_pos: {}",
            range.start,
            self.min_next_pos
        );
        if range.start >= self.span.end || range.is_empty() {
            return range;
        }

        let start = range.start;
        let end = range.end.min(self.span.end);
        let tail = end..range.end;
        let relative = (start - self.span.start) as u32..(end - self.span.start) as u32;
        self.push_relative_range(relative);
        self.min_next_pos = end;
        tail
    }

    /// Extend the builder with a sorted, unique slice of absolute positions.
    ///
    /// Returns a subslice pointing to the portion of `positions` that did not
    /// fit into the current span (the “tail”). If everything fits, the returned
    /// slice is empty. If the entire input is beyond the span, the input slice
    /// is returned unchanged.
    ///
    /// Panics if `positions` is non-empty and `positions[0] < min_next_pos()`.
    pub fn extend_from_position_slice<'a>(&mut self, positions: &'a [u64]) -> &'a [u64] {
        if positions.is_empty() {
            return positions;
        }

        assert!(
            positions[0] >= self.min_next_pos,
            "positions[0]: {}, min_next_pos: {}",
            positions[0],
            self.min_next_pos
        );

        // If everything is beyond the segment, nothing to add.
        if positions[0] >= self.span.end {
            return positions;
        }

        // Determine the portion of positions that fit into this segment.
        let fit_end = if *positions.last().unwrap() < self.span.end {
            positions.len()
        } else {
            positions.partition_point(|&p| p < self.span.end)
        };
        let in_slice = &positions[..fit_end];
        if in_slice.is_empty() {
            return positions;
        }

        match self.kind {
            SegmentKind::List => {
                let add_len = in_slice.len();
                if self.values.len() + add_len <= Segment::MAX_LIST_LEN as usize {
                    // Common fast path: bulk-extend the relative positions.
                    self.values.reserve(add_len);
                    let base = self.span.start;
                    self.values
                        .extend(in_slice.iter().map(|&p| (p - base) as u16));
                } else {
                    // Overflow list: switch to bits and set bits for incoming positions.
                    self.switch_representation(SegmentKind::Bits);
                    let base = self.span.start as usize;
                    for &p in in_slice {
                        self.bits.set((p as usize) - base);
                    }
                }
            }
            SegmentKind::Bits => {
                let base = self.span.start as usize;
                for &p in in_slice {
                    self.bits.set((p as usize) - base);
                }
            }
            SegmentKind::Ranges => {
                // Coalesce contiguous positions into ranges and push as ranges.
                let base = self.span.start;
                let mut run_start = (in_slice[0] - base) as u32;
                let mut prev = run_start;
                for p in in_slice[1..].iter().map(move |&p| (p - base) as u32) {
                    if p == prev + 1 {
                        prev = p;
                    } else {
                        // [run_start, prev] inclusive => ..prev+1 exclusive
                        self.push_relative_range(run_start..prev + 1);
                        run_start = p;
                        prev = p;
                    }
                }
                self.push_relative_range(run_start..prev + 1);
            }
            SegmentKind::Empty | SegmentKind::Full => {
                unreachable!("Builder should not be in Empty/Full during building");
            }
        }

        // Update min_next_pos to one past the last inserted position.
        self.min_next_pos = in_slice[in_slice.len() - 1] + 1;

        // Return the tail that did not fit this segment.
        &positions[fit_end..]
    }

    pub fn extend_from_range_slice<'a>(
        &mut self,
        slice: RangeListSlice<'a, u64>,
    ) -> RangeListSlice<'a, u64> {
        let Range {
            start: slice_start,
            end: slice_end,
        } = slice.bounds();
        assert!(slice_start >= self.min_next_pos);
        if slice_start >= self.span.end {
            return slice;
        }

        let (slice, tail) = if slice_end > self.span.end {
            slice.split_at_position(self.span.end)
        } else {
            (slice, RangeListSlice::empty())
        };

        let span_start = self.span.start;
        for range in slice
            .iter()
            .map(|r| (r.start - span_start) as u32..(r.end - span_start) as u32)
        {
            self.push_relative_range(range);
        }

        self.min_next_pos = slice.bounds().end;
        tail
    }

    /// Switch the internal representation, migrating already appended data as needed.
    ///
    /// This is an internal helper that is called when thresholds are exceeded or
    /// when a more compact representation is detected. Panics if asked to switch
    /// from an unexpected state (e.g. from `Empty`/`Full`).
    fn switch_representation(&mut self, kind: SegmentKind) {
        match (self.kind, kind) {
            (SegmentKind::List, SegmentKind::Bits) => {
                let len = (self.span.end - self.span.start) as usize;
                self.bits = BitArray::from_positions(self.values.iter().map(|&v| v as usize), len);
                self.values.clear();
            }
            (SegmentKind::List, SegmentKind::Ranges) => {
                let count = count_runs(&self.values);
                self.runs.clear();
                self.runs.reserve(count);
                self.runs.extend(
                    self.values
                        .iter()
                        .map(|&v| Run::point(v))
                        .coalesce(|p, n| p.coalesce(n)),
                );
                self.values.clear();
            }
            (SegmentKind::List, SegmentKind::List) => (),
            (SegmentKind::Bits, SegmentKind::Ranges) => {
                let count = self.bits.count_runs();
                self.runs.clear();
                self.runs.reserve(count);
                self.runs
                    .extend(self.bits.ranges_iter().map(|v| Run::from(v)));
                self.bits = BitArray::empty(0);
            }
            (SegmentKind::Bits, SegmentKind::Bits) => (),
            (SegmentKind::Ranges, SegmentKind::Bits) => {
                let len = (self.span.end - self.span.start) as usize;
                self.bits = BitArray::from_ranges(self.runs.iter().map(|r| r.as_range()), len);
                self.runs.clear();
            }
            (SegmentKind::Ranges, SegmentKind::Ranges) => (),
            _ => panic!(
                "Switching builder from {:?} to {kind:?} is unexpected",
                self.kind
            ),
        }
        self.kind = kind;
    }

    /// Push a range specified relative to `span.start` (i.e. within `0..(span.len())`).
    ///
    /// This helper may trigger representation switching to honor
    /// `Segment::MAX_LIST_LEN` and `Segment::MAX_RANGES_LEN` constraints. In
    /// `Ranges` mode, adjacent ranges are coalesced.
    fn push_relative_range(&mut self, range: Range<u32>) {
        let range_len = (range.end - range.start) as usize;
        match self.kind {
            SegmentKind::List => {
                if self.values.len() + range_len > Segment::MAX_LIST_LEN as usize {
                    let runs = count_runs(&self.values);
                    if runs + 1 <= Segment::MAX_RANGES_LEN as usize {
                        self.switch_representation(SegmentKind::Ranges);
                        self.push_relative_range(range);
                    } else {
                        self.switch_representation(SegmentKind::Bits);
                        self.push_relative_range(range);
                    }
                } else {
                    self.values.reserve(range_len);
                    self.values
                        .extend(range.start as u16..=(range.end - 1) as u16);
                }
            }
            SegmentKind::Bits => {
                self.bits
                    .set_range(range.start as usize..range.end as usize);
            }
            SegmentKind::Ranges => {
                let run = Run::from(range);
                match self.runs.last_mut() {
                    Some(last) => {
                        if last.is_adjacent(run) {
                            last.last = run.last;
                        } else {
                            self.runs.push(run);
                        }
                    }
                    None => {
                        self.runs.push(run);
                    }
                }
                if self.runs.len() as u64 > Segment::MAX_RANGES_LEN {
                    self.switch_representation(SegmentKind::Bits);
                }
            }
            SegmentKind::Empty | SegmentKind::Full => {
                // Builder never remains in these kinds during construction.
                unreachable!("Builder should not be in Empty/Full during building");
            }
        }
    }
}
