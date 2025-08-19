//! An iterator adapter that shifts all ranges down by a fixed amount.

use std::ops::Range;

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

#[cfg(test)]
#[allow(clippy::single_range_in_vec_init)]
#[allow(clippy::reversed_empty_ranges)]
mod tests {
    use super::*;
    use crate::transform::RangeIteratorsExt;

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
