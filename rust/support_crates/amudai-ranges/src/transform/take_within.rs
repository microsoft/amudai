use std::ops::Range;

use crate::put_back::PutBack;

/// Iterator adapter that yields `u64` items strictly below a threshold,
/// stopping right before the first item that is `>= threshold`.
///
/// Behavior:
/// - Items `< threshold` are yielded as-is.
/// - The first item `>= threshold` is pushed back into the underlying
///   [`PutBack`] and iteration ends.
/// - If the underlying iterator is exhausted, iteration ends.
///
/// This is useful for reading a monotonic stream up to (but not crossing)
/// a boundary while leaving the boundary item available for subsequent consumers.
///
/// Complexity: O(1) per element; uses at most one buffered item.
///
/// Edge cases:
/// - If the first item is already `>= threshold`, nothing is yielded and that
///   item is put back.
/// - Equal to threshold is considered crossing and will be put back.
///
/// Panics:
/// - In debug builds, will panic if it would attempt to push back when the
///   buffer is already occupied (see [`PutBack::put_back`](crate::put_back::PutBack::put_back)).
pub struct TakeWithin<'a, I: Iterator<Item = u64>> {
    inner: &'a mut PutBack<I>,
    threshold: u64,
}

impl<'a, I: Iterator<Item = u64>> TakeWithin<'a, I> {
    /// Creates a new adapter that yields items < `threshold`.
    /// When an item >= `threshold` is encountered, it is put back and the iterator ends.
    pub fn new(inner: &'a mut PutBack<I>, threshold: u64) -> Self {
        Self { inner, threshold }
    }
}

impl<'a, I: Iterator<Item = u64>> Iterator for TakeWithin<'a, I> {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(v) if v < self.threshold => Some(v),
            Some(v) => {
                // Crossed (or reached) threshold; put back and finish.
                self.inner.put_back(v);
                None
            }
            _ => None,
        }
    }
}

pub trait TakeWithinExt {
    type It: Iterator<Item = u64>;

    fn take_within(&mut self, threshold: u64) -> TakeWithin<'_, Self::It>;
}

impl<I: Iterator<Item = u64>> TakeWithinExt for PutBack<I> {
    type It = I;

    fn take_within(&mut self, threshold: u64) -> TakeWithin<'_, Self::It> {
        TakeWithin::new(self, threshold)
    }
}

/// Iterator adapter that yields ranges entirely below a threshold,
/// splitting at the threshold when necessary and stopping before the first
/// range that starts at or after the threshold.
///
/// Behavior for an incoming `Range<u64> r = r.start..r.end`:
/// - If `r.end <= threshold`: yield `r` unchanged.
/// - If `r.start >= threshold`: push back `r` and end iteration.
/// - Otherwise (`r.start < threshold < r.end`): split into
///   `left = r.start..threshold` and `right = threshold..r.end`;
///   yield `left`, push back `right`, then end iteration.
///
/// This allows consuming the portion of a range stream that lies strictly
/// below a boundary, without consuming or losing any data at/after the boundary.
///
/// Complexity: O(1) per yielded range; uses at most one buffered range.
///
/// Edge cases:
/// - If a range starts exactly at `threshold`, it is not yielded; it is put back
///   and iteration ends.
/// - If a range ends exactly at `threshold`, it is yielded unchanged.
/// - Empty ranges (`start == end`) are treated per the rules above and will be yielded
///   if `end <= threshold`.
///
/// Panics:
/// - In debug builds, will panic if it would attempt to push back when the
///   buffer is already occupied (see [`PutBack::put_back`](crate::put_back::PutBack::put_back)).
pub struct TakeRangesWithin<'a, I: Iterator<Item = Range<u64>>> {
    inner: &'a mut PutBack<I>,
    threshold: u64,
}

impl<'a, I: Iterator<Item = Range<u64>>> TakeRangesWithin<'a, I> {
    /// Creates a new adapter that yields ranges completely below `threshold`.
    /// If a range crosses the threshold, it is split: [start, threshold) is yielded,
    /// and [threshold, end) is put back. If a range starts at or after the threshold,
    /// it is put back and iteration ends.
    pub fn new(inner: &'a mut PutBack<I>, threshold: u64) -> Self {
        Self { inner, threshold }
    }
}

impl<'a, I: Iterator<Item = Range<u64>>> Iterator for TakeRangesWithin<'a, I> {
    type Item = Range<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        let r = self.inner.next()?;

        // Entirely before the threshold: yield as-is.
        if r.end <= self.threshold {
            return Some(r);
        }

        // Entirely at/after the threshold: put back and finish.
        if r.start >= self.threshold {
            self.inner.put_back(r);
            return None;
        }

        // The range crosses the threshold: split and put back the tail.
        let left = r.start..self.threshold;
        let right = self.threshold..r.end;
        self.inner.put_back(right);
        Some(left)
    }
}

/// Extension trait for adding `take_ranges_within` to range streams wrapped in
/// [`PutBack`].
///
/// The returned iterator yields only the portions of the stream that lie strictly
/// below a given `threshold`, without consuming anything at or after the threshold:
/// - Ranges entirely below the threshold are yielded unchanged.
/// - If a range crosses the threshold, its left portion `[start, threshold)` is yielded
///   and the right portion `[threshold, end)` is pushed back into the `PutBack` and
///   iteration ends.
/// - If a range starts at or after the threshold, it is pushed back intact and
///   iteration ends without yielding it.
///
/// This is a convenience for constructing [`TakeRangesWithin`] from a `PutBack`.
///
/// Example
/// -------
/// ```rust
/// use std::ops::Range;
/// use amudai_ranges::put_back::PutBack;
/// use amudai_ranges::transform::take_within::TakeRangesWithinExt;
///
/// // Stream of half-open ranges
/// let src = vec![0u64..3, 8..13];
/// let mut pb = PutBack::new(src.into_iter());
///
/// // Consume all content that lies below 10
/// let taken: Vec<Range<u64>> = pb.take_ranges_within(10).collect();
/// assert_eq!(taken, vec![0..3, 8..10]);
///
/// // The right tail was pushed back for later consumers
/// let rest: Vec<Range<u64>> = pb.collect();
/// assert_eq!(rest, vec![10..13]);
/// ```
pub trait TakeRangesWithinExt {
    type It: Iterator<Item = Range<u64>>;

    /// Returns an iterator adapter that yields ranges strictly below `threshold`.
    ///
    /// Semantics:
    /// - Yields any `r` with `r.end <= threshold` unchanged.
    /// - If `r.start >= threshold`, pushes `r` back and ends iteration.
    /// - If `r.start < threshold < r.end`, yields `r.start..threshold`,
    ///   pushes back `threshold..r.end`, and ends iteration.
    ///
    /// See [`TakeRangesWithin`] for details and complexity guarantees.
    fn take_ranges_within(&mut self, threshold: u64) -> TakeRangesWithin<'_, Self::It>;
}

/// Implements [`TakeRangesWithinExt`] for any `PutBack<I>` where `I` yields `Range<u64>`.
///
/// This simply constructs a [`TakeRangesWithin`] over `self`.
impl<I: Iterator<Item = Range<u64>>> TakeRangesWithinExt for PutBack<I> {
    type It = I;

    fn take_ranges_within(&mut self, threshold: u64) -> TakeRangesWithin<'_, Self::It> {
        TakeRangesWithin::new(self, threshold)
    }
}

#[cfg(test)]
#[allow(clippy::single_range_in_vec_init)]
mod tests {
    use super::{TakeRangesWithin, TakeWithin};
    use crate::{put_back::PutBack, transform::take_within::TakeRangesWithinExt};
    use std::ops::Range;

    fn collect_rest_u64<I: Iterator<Item = u64>>(pb: &mut PutBack<I>) -> Vec<u64> {
        let mut out = Vec::new();
        for v in pb.by_ref() {
            out.push(v);
        }
        out
    }

    fn collect_rest_ranges<I: Iterator<Item = Range<u64>>>(pb: &mut PutBack<I>) -> Vec<Range<u64>> {
        let mut out = Vec::new();
        for r in pb.by_ref() {
            out.push(r);
        }
        out
    }

    #[test]
    fn take_within_all_before_threshold() {
        let src = vec![1, 2, 3];
        let mut pb = PutBack::new(src.into_iter());

        let mut it = TakeWithin::new(&mut pb, 10);
        let taken: Vec<u64> = it.by_ref().collect();
        assert_eq!(
            taken,
            vec![1, 2, 3],
            "all items below threshold should be yielded"
        );

        // Nothing was put back, underlying is exhausted.
        let rest = collect_rest_u64(&mut pb);
        assert_eq!(rest, Vec::<u64>::new());
    }

    #[test]
    fn take_within_crosses_at_threshold_value() {
        let src = vec![1, 5, 10, 11];
        let mut pb = PutBack::new(src.into_iter());

        let mut it = TakeWithin::new(&mut pb, 10);
        let taken: Vec<u64> = it.by_ref().collect();
        assert_eq!(
            taken,
            vec![1, 5],
            "items strictly below threshold should be yielded"
        );

        // The first item >= threshold (10) is put back, followed by remaining items.
        let rest = collect_rest_u64(&mut pb);
        assert_eq!(rest, vec![10, 11]);
    }

    #[test]
    fn take_within_first_item_at_or_after_threshold() {
        // Starts exactly at threshold
        let src = vec![10, 11];
        let mut pb = PutBack::new(src.into_iter());

        let mut it = TakeWithin::new(&mut pb, 10);
        let taken: Vec<u64> = it.by_ref().collect();
        assert_eq!(
            taken,
            Vec::<u64>::new(),
            "nothing yielded when first item >= threshold"
        );

        let rest = collect_rest_u64(&mut pb);
        assert_eq!(rest, vec![10, 11]);

        // Starts after threshold
        let src2 = vec![11, 12];
        let mut pb2 = PutBack::new(src2.into_iter());
        let mut it2 = TakeWithin::new(&mut pb2, 10);
        let taken2: Vec<u64> = it2.by_ref().collect();
        assert_eq!(taken2, Vec::<u64>::new());

        let rest2 = collect_rest_u64(&mut pb2);
        assert_eq!(rest2, vec![11, 12]);
    }

    #[test]
    fn take_within_empty_and_zero_threshold() {
        // Empty input
        let src: Vec<u64> = vec![];
        let mut pb = PutBack::new(src.into_iter());
        let mut it = TakeWithin::new(&mut pb, 10);
        let taken: Vec<u64> = it.by_ref().collect();
        assert_eq!(taken, Vec::<u64>::new());
        let rest = collect_rest_u64(&mut pb);
        assert_eq!(rest, Vec::<u64>::new());

        // Threshold 0: first item is >= threshold, so nothing yielded; first item put back
        let src2 = vec![0, 1, 2];
        let mut pb2 = PutBack::new(src2.into_iter());
        let mut it2 = TakeWithin::new(&mut pb2, 0);
        let taken2: Vec<u64> = it2.by_ref().collect();
        assert_eq!(taken2, Vec::<u64>::new());

        let rest2 = collect_rest_u64(&mut pb2);
        assert_eq!(rest2, vec![0, 1, 2]);
    }

    #[test]
    fn take_within_repeated_threshold_values() {
        let src = vec![10, 10, 12];
        let mut pb = PutBack::new(src.into_iter());

        let mut it = TakeWithin::new(&mut pb, 10);
        let taken: Vec<u64> = it.by_ref().collect();
        assert_eq!(taken, Vec::<u64>::new());

        let rest = collect_rest_u64(&mut pb);
        assert_eq!(rest, vec![10, 10, 12]);
    }

    #[test]
    fn take_ranges_within_entirely_before_and_end_at_threshold() {
        // Entirely before threshold
        let src = vec![0u64..5];
        let mut pb = PutBack::new(src.into_iter());

        let mut it = TakeRangesWithin::new(&mut pb, 10);
        let taken: Vec<Range<u64>> = it.by_ref().collect();
        assert_eq!(taken, vec![0..5]);

        let rest = collect_rest_ranges(&mut pb);
        assert_eq!(rest, Vec::<Range<u64>>::new());

        // Ends exactly at threshold: yielded unchanged
        let src2 = vec![0u64..10];
        let mut pb2 = PutBack::new(src2.into_iter());

        let mut it2 = TakeRangesWithin::new(&mut pb2, 10);
        let taken2: Vec<Range<u64>> = it2.by_ref().collect();
        assert_eq!(taken2, vec![0..10]);

        let rest2 = collect_rest_ranges(&mut pb2);
        assert_eq!(rest2, Vec::<Range<u64>>::new());
    }

    #[test]
    fn take_ranges_within_start_at_or_after_threshold_put_back() {
        let src = vec![10u64..20];
        let mut pb = PutBack::new(src.into_iter());

        let mut it = TakeRangesWithin::new(&mut pb, 10);
        let taken: Vec<Range<u64>> = it.by_ref().collect();
        assert_eq!(
            taken,
            Vec::<Range<u64>>::new(),
            "range starting at threshold should not be yielded"
        );

        let rest = collect_rest_ranges(&mut pb);
        assert_eq!(
            rest,
            vec![10..20],
            "range starting at threshold should be put back intact"
        );
    }

    #[test]
    fn take_ranges_within_crosses_threshold_split_and_put_back_tail() {
        let src = vec![5u64..15];
        let mut pb = PutBack::new(src.into_iter());

        let mut it = TakeRangesWithin::new(&mut pb, 10);
        let taken: Vec<Range<u64>> = it.by_ref().collect();
        assert_eq!(taken, vec![5..10], "left piece should be yielded");

        // Right tail is put back for subsequent consumption
        let rest = collect_rest_ranges(&mut pb);
        assert_eq!(rest, vec![10..15]);
    }

    #[test]
    fn take_ranges_within_multiple_ranges_stop_before_first_start_ge_threshold() {
        let src = vec![0u64..3, 3..5, 5..8, 8..10, 10..12, 12..15];
        let mut pb = PutBack::new(src.into_iter());

        let mut it = TakeRangesWithin::new(&mut pb, 10);
        let taken: Vec<Range<u64>> = it.by_ref().collect();
        assert_eq!(taken, vec![0..3, 3..5, 5..8, 8..10]);

        // The first range with start >= threshold is put back (10..12), and the rest follow.
        let rest = collect_rest_ranges(&mut pb);
        assert_eq!(rest, vec![10..12, 12..15]);
    }

    #[test]
    fn take_ranges_within_mixed_with_crossing_last() {
        let src = vec![0u64..3, 8..13];
        let mut pb = PutBack::new(src.into_iter());

        let mut it = TakeRangesWithin::new(&mut pb, 10);
        let taken: Vec<Range<u64>> = it.by_ref().collect();
        assert_eq!(taken, vec![0..3, 8..10]);

        let rest = collect_rest_ranges(&mut pb);
        assert_eq!(rest, vec![10..13]);
    }

    #[test]
    fn take_ranges_within_empty_ranges_behavior() {
        // Empty range below threshold is yielded as-is
        let src = vec![5u64..5];
        let mut pb = PutBack::new(src.into_iter());

        let mut it = TakeRangesWithin::new(&mut pb, 10);
        let taken: Vec<Range<u64>> = it.by_ref().collect();
        assert_eq!(taken, vec![5..5]);

        let rest = collect_rest_ranges(&mut pb);
        assert!(rest.is_empty());

        // Empty range at threshold: ends == threshold, should be yielded as-is
        let src2 = vec![10u64..10];
        let mut pb2 = PutBack::new(src2.into_iter());

        let mut it2 = TakeRangesWithin::new(&mut pb2, 10);
        let taken2: Vec<Range<u64>> = it2.by_ref().collect();
        assert_eq!(taken2, vec![10..10]);

        let rest2 = collect_rest_ranges(&mut pb2);
        assert!(rest2.is_empty());
    }

    #[test]
    fn test_intervals_consumption() {
        fn process(it: impl Iterator<Item = Range<u64>>) -> Vec<Range<u64>> {
            it.collect()
        }

        let src = (0u64..200).step_by(7).map(|i| i..i + 4).collect::<Vec<_>>();
        let expected_positions = src.iter().cloned().flatten().collect::<Vec<_>>();

        let mut inner = PutBack::new(src.clone().into_iter());

        let mut result = Vec::new();
        for t in (10u64..210).step_by(10) {
            let res = process(inner.take_ranges_within(t));
            result.extend(res);
        }

        let actual_positions = result.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(actual_positions, expected_positions);
    }

    #[test]
    fn test_step_by() {
        let span = 99u64;
        let end = span.next_multiple_of(10);
        let res = (0u64..end)
            .step_by(10)
            .map(|s| s..s + 10)
            .collect::<Vec<_>>();
        dbg!(&res);
    }
}
