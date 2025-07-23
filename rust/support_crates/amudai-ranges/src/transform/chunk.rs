//! An iterator adapter that chunks ranges from an underlying iterator.

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

        let current_len = range_remainder.end.saturating_sub(range_remainder.start);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::RangeIteratorsExt;

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
}
