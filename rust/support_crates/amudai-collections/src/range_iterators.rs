//! Iterator adapters for working with `Range<u64>` sequences.
//!
//! This module provides utilities for manipulating iterators of `Range<u64>`,
//! including chunking ranges into smaller pieces and shifting ranges up or down
//! by a fixed amount. These adapters are useful for efficiently processing
//! large or fragmented ranges, such as when splitting I/O requests or
//! normalizing logical regions.
//!
//! # Provided Adapters
//!
//! - [`ChunkedRanges`]: Splits each input range into subranges of at most a given size.
//! - [`ShiftDownRanges`]: Shifts all range bounds down by a fixed amount (with underflow checking).
//! - [`ShiftUpRanges`]: Shifts all range bounds up by a fixed amount (with overflow checking).
//!
//! The [`RangeIteratorsExt`] trait is implemented for all iterators over `Range<u64>`,
//! providing convenient methods to construct these adapters.

use std::ops::Range;

/// An iterator adapter that chunks ranges from an underlying iterator.
///
/// Given an iterator yielding `Range<u64>`, this adapter yields `Range<u64>`
/// such that no outputted range has a length greater than `chunk_size`.
/// If an input range is larger than `chunk_size`, it's split into multiple
/// smaller ranges. The adapter preserves empty ranges and ranges smaller than
/// `chunk_size` as-is.
#[derive(Debug, Clone)]
pub struct ChunkedRanges<I>
where
    I: Iterator<Item = Range<u64>>,
{
    /// The underlying iterator of ranges.
    inner: I,
    /// The maximum size of each output chunk.
    chunk_size: u64,
    /// Stores the remainder of a range being processed.
    range_remainder: Range<u64>,
}

impl<I> ChunkedRanges<I>
where
    I: Iterator<Item = Range<u64>>,
{
    /// Creates a new `ChunkedRanges` iterator.
    ///
    /// # Panics
    ///
    /// Panics if `chunk_size` is 0, as this would lead to infinite loops or prevent progress.
    ///
    /// # Arguments
    ///
    /// * `inner` - The underlying iterator of `Range<u64>`.
    /// * `chunk_size` - The maximum size of each output chunk. Must be greater than 0.
    pub fn new(inner: I, chunk_size: u64) -> Self {
        if chunk_size == 0 {
            panic!("chunk_size must be greater than 0");
        }
        Self {
            inner,
            chunk_size,
            range_remainder: 0..0,
        }
    }
}

impl<I> Iterator for ChunkedRanges<I>
where
    I: Iterator<Item = Range<u64>>,
{
    type Item = Range<u64>;

    /// Returns the next chunked range from the iterator.
    ///
    /// If the current range is larger than `chunk_size`, it is split and the remainder
    /// is stored for subsequent calls.
    fn next(&mut self) -> Option<Self::Item> {
        if self.range_remainder.is_empty() {
            self.range_remainder = self.inner.next()?;
        }

        let range_remainder = std::mem::replace(&mut self.range_remainder, 0..0);

        let current_len = if range_remainder.start >= range_remainder.end {
            0
        } else {
            range_remainder.end - range_remainder.start
        };

        if current_len <= self.chunk_size {
            Some(range_remainder)
        } else {
            let split_point = range_remainder.start + self.chunk_size;
            let output_chunk = range_remainder.start..split_point;
            self.range_remainder = split_point..range_remainder.end;
            Some(output_chunk)
        }
    }
}

/// An iterator adapter that shifts all ranges down by a fixed amount.
///
/// Each range's `start` and `end` are decreased by `amount`. Panics if underflow occurs.
#[derive(Debug, Clone)]
pub struct ShiftDownRanges<I>
where
    I: Iterator<Item = Range<u64>>,
{
    /// The underlying iterator of ranges.
    inner: I,
    /// The amount to shift each range down.
    amount: u64,
}

impl<I> ShiftDownRanges<I>
where
    I: Iterator<Item = Range<u64>>,
{
    /// Creates a new `ShiftDownRanges` iterator.
    ///
    /// # Arguments
    ///
    /// * `inner` - The underlying iterator of `Range<u64>`.
    /// * `amount` - The amount to subtract from each range's start and end.
    pub fn new(inner: I, amount: u64) -> Self {
        ShiftDownRanges { inner, amount }
    }
}

impl<I> Iterator for ShiftDownRanges<I>
where
    I: Iterator<Item = Range<u64>>,
{
    type Item = Range<u64>;

    /// Returns the next shifted range from the iterator.
    ///
    /// Panics if subtracting `amount` would underflow either bound.
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|range| {
            let new_start = range.start.checked_sub(self.amount).unwrap_or_else(|| {
                panic!(
                    "Arithmetic underflow: cannot shift range start {} down by {}",
                    range.start, self.amount
                )
            });
            let new_end = range.end.checked_sub(self.amount).unwrap_or_else(|| {
                panic!(
                    "Arithmetic underflow: cannot shift range end {} down by {}",
                    range.end, self.amount
                )
            });
            new_start..new_end
        })
    }

    /// Returns the size hint of the underlying iterator.
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<I> ExactSizeIterator for ShiftDownRanges<I>
where
    I: ExactSizeIterator<Item = Range<u64>>,
{
    /// Returns the exact length of the iterator.
    fn len(&self) -> usize {
        self.inner.len()
    }
}

/// An iterator adapter that shifts all ranges up by a fixed amount.
///
/// Each range's `start` and `end` are increased by `amount`. Panics if overflow occurs.
#[derive(Debug, Clone)]
pub struct ShiftUpRanges<I>
where
    I: Iterator<Item = Range<u64>>,
{
    /// The underlying iterator of ranges.
    inner: I,
    /// The amount to shift each range up.
    amount: u64,
}

impl<I> ShiftUpRanges<I>
where
    I: Iterator<Item = Range<u64>>,
{
    /// Creates a new `ShiftUpRanges` iterator.
    ///
    /// # Arguments
    ///
    /// * `inner` - The underlying iterator of `Range<u64>`.
    /// * `amount` - The amount to add to each range's start and end.
    pub fn new(inner: I, amount: u64) -> Self {
        ShiftUpRanges { inner, amount }
    }
}

impl<I> Iterator for ShiftUpRanges<I>
where
    I: Iterator<Item = Range<u64>>,
{
    type Item = Range<u64>;

    /// Returns the next shifted range from the iterator.
    ///
    /// Panics if adding `amount` would overflow either bound.
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|range| {
            let new_start = range.start.checked_add(self.amount).unwrap_or_else(|| {
                panic!(
                    "Arithmetic overflow: cannot shift range start {} up by {}",
                    range.start, self.amount
                )
            });
            let new_end = range.end.checked_add(self.amount).unwrap_or_else(|| {
                panic!(
                    "Arithmetic overflow: cannot shift range end {} up by {}",
                    range.end, self.amount
                )
            });
            new_start..new_end
        })
    }

    /// Returns the size hint of the underlying iterator.
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<I> ExactSizeIterator for ShiftUpRanges<I>
where
    I: ExactSizeIterator<Item = Range<u64>>,
{
    /// Returns the exact length of the iterator.
    fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Extension trait for more idiomatic usage of the range iterator adapters.
///
/// This trait provides convenient methods to adapt iterators of `Range<u64>`
/// into chunked or shifted forms.
pub trait RangeIteratorsExt: Iterator<Item = Range<u64>> + Sized {
    /// Adapts an iterator of `Range<u64>` to yield ranges chunked to a maximum size.
    ///
    /// Each output range will have a length at most `chunk_size`.
    ///
    /// # Panics
    ///
    /// Panics if `chunk_size` is 0.
    fn chunk_ranges(self, chunk_size: u64) -> ChunkedRanges<Self> {
        ChunkedRanges::new(self, chunk_size)
    }

    /// Adapts an iterator of `Range<u64>` to yield ranges shifted down by `amount`.
    ///
    /// Each output range will have its `start` and `end` decreased by `amount`.
    ///
    /// # Panics
    ///
    /// Panics if subtracting `amount` would underflow either bound.
    fn shift_down(self, amount: u64) -> ShiftDownRanges<Self> {
        ShiftDownRanges::new(self, amount)
    }

    /// Adapts an iterator of `Range<u64>` to yield ranges shifted up by `amount`.
    ///
    /// Each output range will have its `start` and `end` increased by `amount`.
    ///
    /// # Panics
    ///
    /// Panics if adding `amount` would overflow either bound.
    fn shift_up(self, amount: u64) -> ShiftUpRanges<Self> {
        ShiftUpRanges::new(self, amount)
    }
}

impl<I: Iterator<Item = Range<u64>>> RangeIteratorsExt for I {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_chunking() {
        let ranges = vec![10..20, 40..60, 100..105, 110..115];
        let chunk_size = 10;
        let chunked_iter = ranges.into_iter().chunk_ranges(chunk_size);
        let result: Vec<Range<u64>> = chunked_iter.collect();

        assert_eq!(result, vec![10..20, 40..50, 50..60, 100..105, 110..115]);
    }

    #[test]
    fn test_smaller_chunks() {
        let ranges = vec![0..10];
        let chunk_size = 3;
        let chunked_iter = ranges.into_iter().chunk_ranges(chunk_size);
        let result: Vec<Range<u64>> = chunked_iter.collect();
        assert_eq!(result, vec![0..3, 3..6, 6..9, 9..10]);
    }

    #[test]
    fn test_empty_input() {
        let ranges: Vec<Range<u64>> = vec![];
        let chunk_size = 10;
        let chunked_iter = ranges.into_iter().chunk_ranges(chunk_size);
        let result: Vec<Range<u64>> = chunked_iter.collect();
        assert_eq!(result, vec![]);
    }

    #[test]
    fn test_ranges_smaller_than_chunk_size() {
        let ranges = vec![0..5, 10..12];
        let chunk_size = 10;
        let chunked_iter = ranges.into_iter().chunk_ranges(chunk_size);
        let result: Vec<Range<u64>> = chunked_iter.collect();
        assert_eq!(result, vec![0..5, 10..12]);
    }

    #[test]
    fn test_ranges_equal_to_chunk_size() {
        let ranges = vec![0..10, 20..30];
        let chunk_size = 10;
        let chunked_iter = ranges.into_iter().chunk_ranges(chunk_size);
        let result: Vec<Range<u64>> = chunked_iter.collect();
        assert_eq!(result, vec![0..10, 20..30]);
    }

    #[test]
    fn test_empty_ranges_in_input() {
        // Empty ranges (e.g., 5..5) have length 0, so they are <= chunk_size and passed through.
        let ranges = vec![5..5, 10..20, 25..25, 30..32];
        let chunk_size = 7;
        let chunked_iter = ranges.into_iter().chunk_ranges(chunk_size);
        let result: Vec<Range<u64>> = chunked_iter.collect();
        assert_eq!(result, vec![5..5, 10..17, 17..20, 25..25, 30..32]);
    }

    #[test]
    fn test_single_large_range() {
        let ranges = vec![0..25];
        let chunk_size = 10;
        let chunked_iter = ranges.into_iter().chunk_ranges(chunk_size);
        let result: Vec<Range<u64>> = chunked_iter.collect();
        assert_eq!(result, vec![0..10, 10..20, 20..25]);
    }

    #[test]
    #[should_panic(expected = "chunk_size must be greater than 0")]
    fn test_chunk_size_zero() {
        let ranges: Vec<Range<u64>> = vec![0..10];
        // This should panic, so we don't need to collect or assert results.
        let _ = ranges.into_iter().chunk_ranges(0);
    }

    #[test]
    fn test_chunk_size_one() {
        let ranges = vec![0..3, 5..6];
        let chunk_size = 1;
        let chunked_iter = ranges.into_iter().chunk_ranges(chunk_size);
        let result: Vec<Range<u64>> = chunked_iter.collect();
        assert_eq!(result, vec![0..1, 1..2, 2..3, 5..6]);
    }

    #[test]
    fn test_range_ends_exactly_on_chunk_boundary() {
        // This tests that if a split results in a remainder that itself is exactly chunk_size,
        // or if an original range is a multiple of chunk_size.
        let ranges = vec![0..20]; // length 20
        let chunk_size = 10;
        let chunked_iter = ranges.into_iter().chunk_ranges(chunk_size);
        let result: Vec<Range<u64>> = chunked_iter.collect();
        assert_eq!(result, vec![0..10, 10..20]);
    }

    #[test]
    fn test_u64_max_edge_cases() {
        let chunk_size = 10;

        // Case 1: Range smaller than chunk size, near u64::MAX
        let ranges1 = vec![(u64::MAX - 5)..u64::MAX];
        let chunked_iter1 = ranges1.into_iter().chunk_ranges(chunk_size);
        let result1: Vec<Range<u64>> = chunked_iter1.collect();
        assert_eq!(result1, vec![(u64::MAX - 5)..u64::MAX]);

        // Case 2: Range larger than chunk size, near u64::MAX
        let ranges2 = vec![(u64::MAX - 15)..u64::MAX]; // length 15
        let chunked_iter2 = ranges2.into_iter().chunk_ranges(chunk_size);
        let result2: Vec<Range<u64>> = chunked_iter2.collect();
        assert_eq!(
            result2,
            vec![(u64::MAX - 15)..(u64::MAX - 5), (u64::MAX - 5)..u64::MAX]
        );

        // Case 3: Range ends exactly on u64::MAX, multiple chunks
        let ranges3 = vec![(u64::MAX - 20)..u64::MAX]; // length 20
        let chunked_iter3 = ranges3.into_iter().chunk_ranges(chunk_size);
        let result3: Vec<Range<u64>> = chunked_iter3.collect();
        assert_eq!(
            result3,
            vec![(u64::MAX - 20)..(u64::MAX - 10), (u64::MAX - 10)..u64::MAX]
        );
    }

    #[test]
    fn test_shift_down_normal() {
        let ranges = vec![10..20, 40..60];
        let result: Vec<_> = ranges.into_iter().shift_down(5).collect();
        assert_eq!(result, vec![5..15, 35..55]);
    }

    #[test]
    fn test_shift_up_normal() {
        let ranges = vec![10..20, 40..60];
        let result: Vec<_> = ranges.into_iter().shift_up(5).collect();
        assert_eq!(result, vec![15..25, 45..65]);
    }

    #[test]
    fn test_shift_down_to_zero() {
        let ranges = vec![10..20];
        let result: Vec<_> = ranges.into_iter().shift_down(10).collect();
        assert_eq!(result, vec![0..10]);
    }

    #[test]
    #[should_panic(expected = "Arithmetic underflow: cannot shift range start 5 down by 6")]
    fn test_shift_down_underflow_start() {
        let ranges = vec![5..10];
        // Consume the iterator to trigger the panic
        let _ = ranges.into_iter().shift_down(6).collect::<Vec<_>>();
    }

    #[test]
    #[should_panic(expected = "Arithmetic underflow: cannot shift range end 5 down by 6")]
    fn test_shift_down_underflow_end_specifically() {
        // This range is valid, start won't underflow, but end will.
        let ranges = vec![10..5]; // This is an empty range in Rust (start > end)
        // Shifting 10..5 down by 6 would be 4..(-1), so end underflows.
        // The panic message will be about `end` (5)
        let _ = ranges.into_iter().shift_down(6).collect::<Vec<_>>();
    }

    #[test]
    #[should_panic(expected = "Arithmetic overflow: cannot shift range start")]
    fn test_shift_up_overflow_start() {
        let ranges = vec![(u64::MAX - 5)..(u64::MAX - 1)];
        let _ = ranges.into_iter().shift_up(10).collect::<Vec<_>>();
    }

    #[test]
    #[should_panic(expected = "Arithmetic overflow: cannot shift range end")]
    fn test_shift_up_overflow_end() {
        // Start is fine, end overflows
        let ranges = vec![(u64::MAX - 10)..(u64::MAX - 2)];
        let _ = ranges.into_iter().shift_up(5).collect::<Vec<_>>();
    }

    #[test]
    fn test_shift_by_zero() {
        let ranges = vec![10..20, 30..40];
        let original_ranges_clone = ranges.clone();
        let down_result: Vec<_> = ranges.iter().cloned().shift_down(0).collect();
        assert_eq!(down_result, original_ranges_clone);
        let up_result: Vec<_> = ranges.into_iter().shift_up(0).collect();
        assert_eq!(up_result, original_ranges_clone);
    }

    #[test]
    fn test_empty_input_iterator() {
        let ranges: Vec<Range<u64>> = vec![];
        let result_down: Vec<_> = ranges.iter().cloned().shift_down(10).collect();
        assert!(result_down.is_empty());
        let result_up: Vec<_> = ranges.into_iter().shift_up(10).collect();
        assert!(result_up.is_empty());
    }

    #[test]
    fn test_exact_size_iterator_impl() {
        let ranges = vec![10..20, 40..60, 100..105];
        let iter_down = ranges.iter().cloned().shift_down(5);
        assert_eq!(iter_down.len(), 3);
        assert_eq!(iter_down.size_hint(), (3, Some(3)));

        let iter_up = ranges.into_iter().shift_up(5);
        assert_eq!(iter_up.len(), 3);
        assert_eq!(iter_up.size_hint(), (3, Some(3)));
    }
}
