use std::{iter::Peekable, ops::Range};

/// Yields the complement (gaps) within the half‑open universe [0, size) of a
/// strictly ascending, non‑duplicated stream of single values.
///
/// The output is a stream of non‑overlapping ranges covering all positions in
/// [0, size) that are NOT present in the input. This avoids iterating
/// element‑by‑element.
///
/// Assumptions on the input:
/// - Items are strictly ascending, without duplicates.
/// - All items are comparable; items >= `size` are ignored.
///
/// Type constraints:
/// - `T: Default` is used as the lower bound (assumes `T::default()` is the minimum, e.g. 0).
/// - `T: From<u8> + Add<Output = T>` is used to compute the successor (`x + 1`) when
///   advancing past a present element.
///
/// Complexity: O(n) over the input, constant extra memory.
pub fn complement<T, I>(size: T, positions: I) -> Complement<I::IntoIter, T>
where
    T: Ord + Clone + Default + From<u8> + std::ops::Add<Output = T>,
    I: IntoIterator<Item = T>,
{
    Complement::new(size, positions.into_iter())
}

/// Yields the complement (gaps) within the half‑open universe [0, size) of a
/// stream of ordered, non‑overlapping (adjacent allowed) ranges.
///
/// The output is a stream of non‑overlapping ranges covering all positions in
/// [0, size) that are NOT covered by the input ranges. Adjacent or overlapping
/// input ranges are handled correctly in a streaming manner (overlaps are
/// effectively coalesced).
///
/// Assumptions on the input:
/// - Ranges are yielded in ascending order by `start`.
/// - Ranges within the input do not overlap (adjacent is fine). Minor overlaps
///   are tolerated and coalesced on the fly.
///
/// Type constraints:
/// - `T: Default` is used as the lower bound (assumes `T::default()` is the minimum, e.g. 0).
///
/// Complexity: O(n) over the input, constant extra memory.
pub fn complement_ranges<T, I>(size: T, ranges: I) -> ComplementRanges<I::IntoIter, T>
where
    T: Ord + Clone + Default,
    I: IntoIterator<Item = Range<T>>,
{
    ComplementRanges::new(size, ranges.into_iter())
}

/// Iterator adapter implementing [`complement`].
pub struct Complement<I, T>
where
    I: Iterator<Item = T>,
{
    it: Peekable<I>,
    size: T,
    cursor: T,
    done: bool,
}

impl<I, T> Complement<I, T>
where
    I: Iterator<Item = T>,
    T: Ord + Clone + Default + From<u8> + std::ops::Add<Output = T>,
{
    pub fn new(size: T, it: I) -> Self {
        Self {
            it: it.peekable(),
            size,
            cursor: T::default(),
            done: false,
        }
    }
}

impl<I, T> Iterator for Complement<I, T>
where
    I: Iterator<Item = T>,
    T: Ord + Clone + Default + From<u8> + std::ops::Add<Output = T>,
{
    type Item = Range<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            if self.cursor >= self.size {
                self.done = true;
                return None;
            }

            match self.it.peek() {
                None => {
                    // No more positions: tail gap [cursor, size)
                    let start = self.cursor.clone();
                    let end = self.size.clone();
                    self.cursor = self.size.clone();
                    if start < end {
                        return Some(start..end);
                    } else {
                        self.done = true;
                        return None;
                    }
                }
                Some(p) => {
                    // Ignore positions >= size
                    if *p >= self.size {
                        let start = self.cursor.clone();
                        let end = self.size.clone();
                        self.cursor = self.size.clone();
                        if start < end {
                            return Some(start..end);
                        } else {
                            self.done = true;
                            return None;
                        }
                    }

                    // If p < cursor (unexpected with strictly ascending input), skip it.
                    if *p < self.cursor {
                        self.it.next();
                        continue;
                    }

                    // If p == cursor, the single value at cursor is present; advance past it.
                    if *p == self.cursor {
                        // cursor = cursor + 1
                        self.cursor = self.cursor.clone() + T::from(1u8);
                        self.it.next();
                        continue;
                    }

                    // p > cursor: gap [cursor, min(p, size)) ⇒ since p < size here, it's [cursor, p)
                    let start = self.cursor.clone();
                    let end = p.clone();
                    self.cursor = end.clone();
                    if start < end {
                        return Some(start..end);
                    }
                    // Should not happen; continue defensively.
                }
            }
        }
    }
}

/// Iterator adapter implementing [`complement_ranges`].
pub struct ComplementRanges<I, T>
where
    I: Iterator<Item = Range<T>>,
{
    it: Peekable<I>,
    size: T,
    cursor: T,
    done: bool,
    _marker: std::marker::PhantomData<T>,
}

impl<I, T> ComplementRanges<I, T>
where
    I: Iterator<Item = Range<T>>,
    T: Ord + Clone + Default,
{
    pub fn new(size: T, it: I) -> Self {
        Self {
            it: it.peekable(),
            size,
            cursor: T::default(),
            done: false,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<I, T> Iterator for ComplementRanges<I, T>
where
    I: Iterator<Item = Range<T>>,
    T: Ord + Clone + Default,
{
    type Item = Range<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            if self.cursor >= self.size {
                self.done = true;
                return None;
            }

            match self.it.peek() {
                None => {
                    // No more covered ranges: tail gap [cursor, size)
                    let start = self.cursor.clone();
                    let end = self.size.clone();
                    self.cursor = self.size.clone();
                    if start < end {
                        return Some(start..end);
                    } else {
                        self.done = true;
                        return None;
                    }
                }
                Some(r) => {
                    // Treat empty/degenerate ranges as no-op
                    if !(r.start < r.end) {
                        self.it.next();
                        continue;
                    }

                    // If this range is entirely before cursor, consume it.
                    if r.end <= self.cursor {
                        self.it.next();
                        continue;
                    }

                    // If this range starts after cursor: emit the gap [cursor, min(r.start, size))
                    if r.start > self.cursor {
                        let gap_end = if r.start <= self.size {
                            r.start.clone()
                        } else {
                            self.size.clone()
                        };
                        let start = self.cursor.clone();
                        self.cursor = gap_end.clone();
                        if start < gap_end {
                            return Some(start..gap_end);
                        } else {
                            // Shouldn't happen; continue defensively.
                            continue;
                        }
                    }

                    // Otherwise r.start <= cursor < r.end: advance cursor to max(cursor, min(r.end, size))
                    let range_end_clamped = if r.end <= self.size {
                        r.end.clone()
                    } else {
                        self.size.clone()
                    };
                    if range_end_clamped > self.cursor {
                        self.cursor = range_end_clamped;
                    }
                    // Consume this range and continue (may coalesce multiple overlapping/adjacent)
                    self.it.next();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complement_empty_positions() {
        let got: Vec<std::ops::Range<u32>> = complement(5u32, std::iter::empty()).collect();
        let expected: Vec<std::ops::Range<u32>> = vec![0u32..5];
        assert_eq!(got, expected);
    }

    #[test]
    fn complement_full_coverage_positions() {
        let got: Vec<_> = complement(5u32, vec![0u32, 1, 2, 3, 4]).collect();
        let expected: Vec<std::ops::Range<u32>> = vec![];
        assert_eq!(got, expected);
    }

    #[test]
    fn complement_mixed_positions() {
        let got: Vec<_> = complement(10u32, vec![0u32, 2, 3, 5, 8]).collect();
        let expected: Vec<std::ops::Range<u32>> = vec![1u32..2, 4..5, 6..8, 9..10];
        assert_eq!(got, expected);
    }

    #[test]
    fn complement_positions_out_of_order_and_duplicates_tolerated() {
        // Though input is expected strictly ascending and unique, the iterator tolerates
        // duplicates and earlier items by skipping them.
        let got: Vec<_> = complement(10u32, vec![0u32, 0, 1, 3, 2, 7, 7, 7]).collect();
        let expected: Vec<std::ops::Range<u32>> = vec![2u32..3, 4..7, 8..10];
        assert_eq!(got, expected);
    }

    #[test]
    fn complement_positions_ge_size_ignored() {
        let got: Vec<_> = complement(5u32, vec![1u32, 3, 7, 8]).collect();
        let expected: Vec<std::ops::Range<u32>> = vec![0u32..1, 2..3, 4..5];
        assert_eq!(got, expected);
    }

    #[test]
    fn complement_size_zero() {
        let got: Vec<std::ops::Range<u32>> = complement(0u32, vec![0u32, 1]).collect();
        let expected: Vec<std::ops::Range<u32>> = vec![];
        assert_eq!(got, expected);
    }

    #[test]
    fn complement_ranges_empty_input() {
        let got: Vec<_> =
            complement_ranges(5u32, std::iter::empty::<std::ops::Range<u32>>()).collect();
        let expected: Vec<std::ops::Range<u32>> = vec![0u32..5];
        assert_eq!(got, expected);
    }

    #[test]
    fn complement_ranges_gap_before_and_after() {
        let got: Vec<_> = complement_ranges(6u32, vec![2u32..4]).collect();
        let expected: Vec<std::ops::Range<u32>> = vec![0u32..2, 4..6];
        assert_eq!(got, expected);
    }

    #[test]
    fn complement_ranges_disjoint() {
        let got: Vec<_> = complement_ranges(7u32, vec![1u32..2, 4..5]).collect();
        let expected: Vec<std::ops::Range<u32>> = vec![0u32..1, 2..4, 5..7];
        assert_eq!(got, expected);
    }

    #[test]
    fn complement_ranges_adjacent_coalesced() {
        let got: Vec<_> = complement_ranges(6u32, vec![1u32..3, 3..5]).collect();
        let expected: Vec<std::ops::Range<u32>> = vec![0u32..1, 5..6];
        assert_eq!(got, expected);
    }

    #[test]
    fn complement_ranges_overlapping_coalesced() {
        let got: Vec<_> = complement_ranges(6u32, vec![1u32..4, 3..5]).collect();
        let expected: Vec<std::ops::Range<u32>> = vec![0u32..1, 5..6];
        assert_eq!(got, expected);
    }

    #[test]
    fn complement_ranges_range_extends_beyond_size() {
        let got: Vec<_> = complement_ranges(5u32, vec![1u32..100]).collect();
        let expected: Vec<std::ops::Range<u32>> = vec![0u32..1];
        assert_eq!(got, expected);
    }

    #[test]
    fn complement_ranges_range_starts_after_size() {
        let got: Vec<_> = complement_ranges(5u32, vec![8u32..10]).collect();
        let expected: Vec<std::ops::Range<u32>> = vec![0u32..5];
        assert_eq!(got, expected);
    }

    #[test]
    fn complement_ranges_complete_coverage() {
        let got: Vec<_> = complement_ranges(7u32, vec![0u32..3, 3..7]).collect();
        let expected: Vec<std::ops::Range<u32>> = vec![];
        assert_eq!(got, expected);
    }
}
