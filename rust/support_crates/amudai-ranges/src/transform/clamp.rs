//! An iterator adapter that clamps ranges to given bounds.
use std::ops::Range;

/// Iterator adapter that yields the intersection of each input `Range<u64>`
/// with a fixed clamp range.
///
/// Behavior
/// - For each input range produced by `inner`, yields at most one range: the intersection
///   with `clamp_range`.
/// - If the intersection is empty, the input contributes no output.
/// - If `clamp_range` itself is empty, the iterator yields no items.
/// - The upper bound of `size_hint` mirrors the upper bound of the underlying iterator,
///   and the lower bound is 0 because some items may be filtered out.
#[derive(Debug, Clone)]
pub struct ClampedRanges<I>
where
    I: Iterator<Item = Range<u64>>,
{
    /// The underlying iterator that produces half-open ranges [start, end) of `u64`.
    ///
    /// For each input range yielded by `inner`, this adapter yields at most one
    /// clamped range (or none, if the intersection is empty).
    inner: I,

    /// The half-open bounds [start, end) used to clamp incoming ranges.
    ///
    /// If this range is empty (start >= end), the iterator yields no items.
    clamp_range: Range<u64>,
}

impl<I> ClampedRanges<I>
where
    I: Iterator<Item = Range<u64>>,
{
    /// Creates a new `ClampedRanges` iterator.
    ///
    /// Yields at most one range for each input range: the intersection of
    /// that input range with `clamp_range` (if non-empty).
    ///
    /// If `clamp_range` is empty, this iterator will yield no items.
    pub fn new(inner: I, clamp_range: Range<u64>) -> Self {
        Self { inner, clamp_range }
    }
}

impl<I> Iterator for ClampedRanges<I>
where
    I: Iterator<Item = Range<u64>>,
{
    type Item = Range<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        for r in self.inner.by_ref() {
            // Compute intersection [max(starts), min(ends))
            let start = r.start.max(self.clamp_range.start);
            let end = r.end.min(self.clamp_range.end);
            if start < end {
                return Some(start..end);
            }
            // Otherwise, no overlap: keep scanning.
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (_, upper) = self.inner.size_hint();
        (0, upper)
    }
}

#[cfg(test)]
mod tests {
    use crate::transform::RangeIteratorsExt;

    #[test]
    fn test_clamp_basic_overlap() {
        let ranges = vec![10..20, 30..40, 50..60];
        let clamped: Vec<_> = ranges.into_iter().clamp_to(15..55).collect();
        assert_eq!(clamped, vec![15..20, 30..40, 50..55]);
    }

    #[test]
    fn test_clamp_no_overlap() {
        let ranges = vec![10..20, 30..40];
        let clamped: Vec<_> = ranges.into_iter().clamp_to(21..29).collect();
        assert!(clamped.is_empty());
    }

    #[test]
    fn test_clamp_empty_bounds() {
        let ranges = vec![10..20, 30..40];
        let clamped: Vec<_> = ranges.into_iter().clamp_to(15..15).collect();
        assert!(clamped.is_empty());
    }

    #[test]
    fn test_clamp_exact_boundaries() {
        let ranges = vec![10..20, 20..30, 30..40];
        let clamped: Vec<_> = ranges.into_iter().clamp_to(10..30).collect();
        assert_eq!(clamped, vec![10..20, 20..30]);
    }

    #[test]
    fn test_clamp_u64_edges() {
        let near_max = (u64::MAX - 10)..u64::MAX; // length 10
        let ranges = vec![near_max.clone()];
        let clamped: Vec<_> = ranges
            .into_iter()
            .clamp_to((u64::MAX - 5)..u64::MAX)
            .collect();
        assert_eq!(clamped, vec![(u64::MAX - 5)..u64::MAX]);

        let ranges = vec![near_max];
        let clamped: Vec<_> = ranges
            .into_iter()
            .clamp_to((u64::MAX - 20)..(u64::MAX - 15))
            .collect();
        assert!(clamped.is_empty());
    }
}
