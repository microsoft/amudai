//! An immutable, cheaply cloneable, and sliceable list of ranges.
//!
//! A `RangeList<T>` stores a list of non-overlapping, ascending ranges
//! (typically over integral types) in a shared, immutable view. It builds
//! on top of `SharedVec<Range<T>>`.
//!
//! In addition to borrowing the underlying slice of `Range<T>` values,
//! it "trims" the first and last range. This means the logical start of the
//! first range can be greater than `inner[0].start`, and the logical end
//! of the last range can be less than `inner[inner.len()-1].end`.

use amudai_shared_vec::SharedVec;
use std::{
    convert::identity,
    ops::{Range, RangeBounds},
};

/// An immutable, cheaply cloneable, and sliceable list of non-overlapping,
/// ascending ranges.
///
/// `RangeList` provides a view over a shared sequence of `Range<T>`.
/// The key features are:
/// - **Immutability**: Once created, a `RangeList` cannot be modified.
/// - **Cheap Clones and Slices**: Cloning and slicing a `RangeList` is
///   inexpensive as it shares the underlying data (similar to `Arc<[T]>`).
/// - **Trimming**: The first and last ranges in the list can be "trimmed".
///   The `first` field defines the start of the first logical range and the
///   `last` field defines the end of the last logical range.
///
/// Typically, `T` is an integral type (e.g., `u32`, `u64`, `usize`).
/// The ranges stored are expected to be sorted by their start points and
/// non-overlapping.
#[derive(Clone)]
pub struct RangeList<T> {
    /// Underlying shared slice containing the ranges.
    /// These ranges are guaranteed to be sorted and non-overlapping.
    /// `self.first` and `self.last` override `self.inner[0]` and
    /// `self.inner[last_idx]`.
    inner: SharedVec<Range<T>>,

    /// First logical range, "overrides" `inner[0]`.
    ///
    /// `first.start` lies between `inner[0].start` and `inner[0].end`
    /// (inclusive of start).
    /// If `first.start` does not equal `inner[0].start`, then the first
    /// logical range is truncated on the left.
    /// `first.end` is always equal to `inner[0].end` when there are at least
    /// two ranges in the list. If `inner` is empty, this field's value is not
    /// semantically meaningful beyond satisfying type constraints (e.g. `T::default()`).
    first: Range<T>,

    /// Last logical range, "overrides" `inner[inner.len() - 1]`.
    ///
    /// `last.end` lies between `inner[last_idx].start` and `inner[last_idx].end`
    /// (inclusive of end).
    /// If `last.end` does not equal `inner[last_idx].end`, then the last
    /// logical range is truncated on the right.
    /// `last.start` is always equal to `inner[last_idx].start` when there are
    /// at least two ranges in the list. If `inner` is empty, this field's
    /// value is not semantically meaningful.
    last: Range<T>,
}

impl<T: Clone> RangeList<T> {
    /// Constructs a `RangeList` containing a single range.
    ///
    /// # Arguments
    ///
    /// * `r` - The range to store.
    ///
    /// # Returns
    ///
    /// A `RangeList` with one logical range.
    pub fn from_elem(r: Range<T>) -> Self {
        RangeList {
            inner: SharedVec::from_elem(r.clone()),
            first: r.clone(),
            last: r,
        }
    }

    /// Returns the tight logical bounds of the entire list as a single range.
    ///
    /// The returned range starts at the logical start of the first range
    /// and ends at the logical end of the last range.
    pub fn bounds(&self) -> Range<T> {
        self.first.start.clone()..self.last.end.clone()
    }

    /// Returns a vector containing all logical ranges in this list.
    ///
    /// # Returns
    ///
    /// A `Vec<Range<T>>` with each logical range, including trimming.
    pub fn to_vec(&self) -> Vec<Range<T>> {
        self.iter().cloned().collect()
    }
}

impl<T: Default + Clone> From<Vec<Range<T>>> for RangeList<T> {
    fn from(vec: Vec<Range<T>>) -> Self {
        RangeList::new(vec.into())
    }
}

impl<T: Default + Clone> From<&[Range<T>]> for RangeList<T> {
    fn from(slice: &[Range<T>]) -> Self {
        RangeList::from_slice(slice)
    }
}

impl<T: Clone> From<Range<T>> for RangeList<T> {
    fn from(range: Range<T>) -> Self {
        RangeList::from_elem(range)
    }
}

impl<T: Default + Clone> std::iter::FromIterator<Range<T>> for RangeList<T> {
    fn from_iter<I: IntoIterator<Item = Range<T>>>(iter: I) -> Self {
        let vec: Vec<Range<T>> = iter.into_iter().collect();
        RangeList::from(vec)
    }
}

impl<T: Default + Clone> RangeList<T> {
    /// Creates a new, empty `RangeList<T>`.
    ///
    /// # Returns
    ///
    /// An empty `RangeList`.
    pub fn empty() -> Self {
        RangeList {
            inner: SharedVec::empty(),
            first: T::default()..T::default(),
            last: T::default()..T::default(),
        }
    }

    /// Constructs a new `RangeList<T>` from a shared slice of ranges.
    ///
    /// # Arguments
    ///
    /// * `inner` - Shared, sorted, non-overlapping ranges.
    ///
    /// # Returns
    ///
    /// A `RangeList` covering the full extent of the provided ranges.
    pub fn new(inner: SharedVec<Range<T>>) -> Self {
        if inner.is_empty() {
            Self::empty()
        } else {
            let first = inner[0].clone();
            let last = inner.last().unwrap().clone();
            Self { inner, first, last }
        }
    }

    /// Constructs a `RangeList` from a slice of ranges.
    ///
    /// # Arguments
    ///
    /// * `slice` - Slice of sorted, non-overlapping ranges.
    ///
    /// # Returns
    ///
    /// A `RangeList` covering the full extent of the provided ranges.
    pub fn from_slice(slice: &[Range<T>]) -> Self {
        Self::new(SharedVec::from_slice(slice))
    }
}

impl<T: PartialOrd> RangeList<T> {
    /// Searches for the logical range containing the specified position.
    ///
    /// Performs a binary search over the logical ranges in this `RangeList`
    /// to locate the range that contains `pos`.
    ///
    /// # Arguments
    ///
    /// * `pos` - The position to search for.
    ///
    /// # Returns
    ///
    /// - `Ok(index)` if `pos` is contained within the logical range at `index`.
    /// - `Err(index)` if no such range exists; `index` is the position where
    ///   a new range containing `pos` could be inserted while maintaining order.
    ///
    /// # Complexity
    ///
    /// Uses binary search (`O(log n)`).
    ///
    /// # Details
    ///
    /// - If the list is empty, always returns `Err(0)`.
    /// - If `pos` is less than the start of the first logical range, returns `Err(0)`.
    /// - If `pos` is greater than or equal to the end of the last logical range, returns `Err(len)`.
    /// - If `pos` falls between two ranges, returns `Err(index)` for the insertion point.
    pub fn search_position(&self, pos: T) -> Result<usize, usize> {
        let len = self.len();
        if len == 0 {
            return Err(0);
        }
        if pos < self.first.start {
            return Err(0);
        }
        if pos >= self.last.end {
            return Err(len);
        }

        self.inner.binary_search_by(|range| {
            if pos < range.start {
                std::cmp::Ordering::Greater
            } else if pos >= range.end {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        })
    }

    /// Returns `true` if the specified position is contained within any logical range in the list.
    ///
    /// Performs a binary search to determine whether `pos` falls within any of the logical ranges
    /// represented by this `RangeList`.
    ///
    /// # Arguments
    ///
    /// * `pos` - The position to check for containment.
    ///
    /// # Returns
    ///
    /// `true` if `pos` is contained in any logical range, `false` otherwise.
    ///
    /// # Complexity
    ///
    /// Runs in `O(log n)` time, where `n` is the number of ranges.
    pub fn contains_position(&self, pos: T) -> bool {
        self.search_position(pos).is_ok()
    }
}

impl<T: Default + Clone + PartialOrd> RangeList<T> {
    /// Splits the range list into two `RangeList`s at the specified position.
    ///
    /// The first returned `RangeList` contains all portions of ranges strictly
    /// before `pos`.
    /// The second contains all portions of ranges starting at or after `pos`.
    ///
    /// If `pos` falls within a range, that range is split between the two outputs.
    /// If `pos` is outside the bounds of the list, one of the outputs will be empty.
    ///
    /// # Arguments
    ///
    /// * `pos` - The position at which to split the range list.
    ///
    /// # Returns
    ///
    /// A tuple `(left, right)` where:
    /// - `left` contains all elements strictly before `pos`
    /// - `right` contains all elements at or after `pos`
    pub fn split_at_position(&self, pos: T) -> (Self, Self) {
        let bounds = self.bounds();
        let left = self.clamp(bounds.start..pos.clone());
        let right = self.clamp(pos..bounds.end);
        (left, right)
    }

    /// Returns a new `RangeList<T>` containing only the portions of ranges within
    /// the specified bounds.
    ///
    /// This method restricts the current `RangeList` to the intersection with the
    /// given `bounds`.
    /// Any ranges or parts of ranges outside `bounds` are excluded from the result.
    /// The first and last ranges in the result are trimmed as needed to fit exactly
    /// within `bounds`.
    ///
    /// # Arguments
    ///
    /// * `bounds` - The range specifying the lower (inclusive) and upper (exclusive)
    ///   bounds.
    ///
    /// # Returns
    ///
    /// A new `RangeList<T>` containing only the elements within `bounds`.
    /// Returns an empty list if the original list is empty, if `bounds` is empty,
    /// or if there is no overlap.
    ///
    /// # Complexity
    ///
    /// `O(log n)`, uses binary search to locate the relevant ranges.
    pub fn clamp(&self, bounds: Range<T>) -> RangeList<T> {
        if self.is_empty() || bounds.start >= bounds.end {
            return Self::empty();
        }

        if bounds.start <= self.first.start && self.last.end <= bounds.end {
            return self.clone();
        }

        // Index of the first range that has positions above or equal to bounds.start
        let start_idx = self
            .search_position(bounds.start.clone())
            .unwrap_or_else(identity);
        if start_idx >= self.len() {
            return Self::empty();
        }

        // Index of the first range where _all_ positions are above or equal to bounds.end
        let end_idx = match self.search_position(bounds.end.clone()) {
            Ok(idx) => {
                let range = self.get(idx).unwrap();
                if bounds.end > range.start {
                    idx + 1
                } else {
                    idx
                }
            }
            Err(idx) => idx,
        };
        if start_idx == end_idx {
            return Self::empty();
        }

        let sliced = self.slice(start_idx..end_idx);
        Self::new_trimmed(sliced.inner, bounds.start, bounds.end)
    }

    /// Constructs a new `RangeList<T>` from a shared slice of ranges,
    /// trimming the first and last ranges to the specified bounds.
    ///
    /// This method creates a `RangeList` that logically starts at `start`
    /// and ends at `end`, even if these do not coincide with the boundaries
    /// of the first and last ranges in `inner`.
    /// The first range is truncated on the left to begin at `start` (if `start > inner[0].start`),
    /// and the last range is truncated on the right to end at `end` (if `end < inner[last].end`).
    /// If the trimmed region falls entirely within a single range, both the first and last ranges
    /// are set to the trimmed subrange.
    ///
    /// # Arguments
    ///
    /// * `inner` - Shared, sorted, non-overlapping ranges.
    /// * `start` - The logical start of the first range (inclusive).
    /// * `end` - The logical end of the last range (exclusive).
    ///
    /// # Panics
    ///
    /// Panics if `inner` is non-empty and `start` is not less than the end of the first range,
    /// or if `end` is not greater than the start of the last range.
    ///
    /// # Returns
    ///
    /// A `RangeList<T>` representing the trimmed view. Returns an empty list if `inner` is empty.
    fn new_trimmed(inner: SharedVec<Range<T>>, start: T, end: T) -> Self {
        if inner.is_empty() {
            Self::empty()
        } else {
            let mut first = inner[0].clone();
            let mut last = inner.last().unwrap().clone();
            assert!(start < first.end, "start must be less than first.end");
            assert!(end > last.start, "end must be greater than last.start");
            if start > first.start {
                first.start = start.clone();
            }
            if end < first.end {
                assert_eq!(inner.len(), 1);
                first.end = end.clone();
            }
            if start > last.start {
                assert_eq!(inner.len(), 1);
                last.start = start;
            }
            if end < last.end {
                last.end = end;
            }
            Self { inner, first, last }
        }
    }
}

impl<T> RangeList<T> {
    /// Returns the number of logical ranges in the list.
    ///
    /// # Returns
    ///
    /// The count of logical ranges.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the list contains no logical ranges.
    ///
    /// # Returns
    ///
    /// `true` if empty, `false` otherwise.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the logical range at the given index, with trimming applied to the first and last.
    ///
    /// # Arguments
    ///
    /// * `index` - Index of the logical range.
    ///
    /// # Returns
    ///
    /// `Some(&Range<T>)` if the index is valid, `None` otherwise.
    pub fn get(&self, index: usize) -> Option<&Range<T>> {
        let len = self.len();
        if len == 0 {
            None
        } else if index == 0 {
            Some(&self.first)
        } else if index == len - 1 {
            Some(&self.last)
        } else {
            self.inner.get(index)
        }
    }

    /// Returns a reference to the underlying shared slice of ranges.
    pub fn inner(&self) -> &SharedVec<Range<T>> {
        &self.inner
    }

    /// Returns an iterator over the logical ranges in this list.
    ///
    /// # Returns
    ///
    /// A `RangeListIter<T>` that yields each logical range, including trimming.
    pub fn iter(&self) -> RangeListIter<T> {
        RangeListIter {
            list: self,
            index: 0,
        }
    }
}

impl<T: Default + Clone> RangeList<T> {
    /// Returns a sub-slice of this list, using a range of element indices.
    ///
    /// # Panics
    ///
    /// Panics if the range is out of bounds.
    pub fn slice<R>(&self, range: R) -> Self
    where
        R: RangeBounds<usize>,
    {
        use std::ops::Bound::*;
        let len = self.len();
        let start = match range.start_bound() {
            Included(&n) => n,
            Excluded(&n) => n + 1,
            Unbounded => 0,
        };
        let end = match range.end_bound() {
            Included(&n) => n + 1,
            Excluded(&n) => n,
            Unbounded => len,
        };
        assert!(start <= end && end <= len, "slice out of bounds");

        if start == end {
            return Self::empty();
        }

        let new_inner = self.inner.slice(start..end);

        let new_first = if start == 0 {
            self.first.clone()
        } else {
            self.inner[start].clone()
        };
        let new_last = if end == len {
            self.last.clone()
        } else {
            self.inner[end - 1].clone()
        };

        Self {
            inner: new_inner,
            first: new_first,
            last: new_last,
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for RangeList<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries((0..self.len()).map(|i| self.get(i).unwrap()))
            .finish()
    }
}

impl<T: Default + Clone> Default for RangeList<T> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<T: PartialEq> PartialEq for RangeList<T> {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        for (a, b) in self.iter().zip(other.iter()) {
            if a != b {
                return false;
            }
        }
        true
    }
}

impl<T: Eq> Eq for RangeList<T> {}

/// Iterator over logical ranges in a `RangeList`, applying any trimming
/// to the first and last ranges.
///
/// This struct is created by [`RangeList::iter`].
#[derive(Clone)]
pub struct RangeListIter<'a, T> {
    list: &'a RangeList<T>,
    index: usize,
}

impl<'a, T> Iterator for RangeListIter<'a, T> {
    type Item = &'a Range<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.list.len() {
            let item = self.list.get(self.index);
            self.index += 1;
            item
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.list.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

impl<T> ExactSizeIterator for RangeListIter<'_, T> {}

impl<'a, T> IntoIterator for &'a RangeList<T> {
    type Item = &'a Range<T>;

    type IntoIter = RangeListIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        RangeListIter {
            list: self,
            index: 0,
        }
    }
}

/// Owned iterator over logical ranges in a `RangeList`, applying any trimming
/// to the first and last ranges.
///
/// This struct is created by consuming a `RangeList`.
#[derive(Clone)]
pub struct RangeListIntoIter<T> {
    list: RangeList<T>,
    index: usize,
}

impl<T: Clone> Iterator for RangeListIntoIter<T> {
    type Item = Range<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.list.len() {
            let item = self.list.get(self.index).cloned();
            self.index += 1;
            item
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.list.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

impl<T: Clone> ExactSizeIterator for RangeListIntoIter<T> {}

impl<T: Clone> IntoIterator for RangeList<T> {
    type Item = Range<T>;
    type IntoIter = RangeListIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        RangeListIntoIter {
            list: self,
            index: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Range;

    fn rl(ranges: &[Range<usize>]) -> RangeList<usize> {
        RangeList::from_slice(ranges)
    }

    #[test]
    fn test_empty() {
        let rl = RangeList::<usize>::empty();
        assert_eq!(rl.search_position(0), Err(0));
        assert_eq!(rl.search_position(100), Err(0));
    }

    #[test]
    fn test_single_range_no_trim() {
        let rl = rl(&[10..20]);
        assert_eq!(rl.search_position(10), Ok(0));
        assert_eq!(rl.search_position(15), Ok(0));
        assert_eq!(rl.search_position(19), Ok(0));
        assert_eq!(rl.search_position(20), Err(1));
        assert_eq!(rl.search_position(9), Err(0));
        assert_eq!(rl.search_position(21), Err(1));
    }

    #[test]
    fn test_single_range_trimmed() {
        let base = rl(&[10..20]);
        let trimmed = RangeList::new_trimmed(base.inner().clone(), 12, 18);
        assert_eq!(trimmed.first, 12..18);
        assert_eq!(trimmed.last, 12..18);
        assert_eq!(trimmed.len(), 1);
        assert_eq!(trimmed.search_position(11), Err(0));
        assert_eq!(trimmed.search_position(12), Ok(0));
        assert_eq!(trimmed.search_position(17), Ok(0));
        assert_eq!(trimmed.search_position(18), Err(1));
        assert_eq!(trimmed.search_position(20), Err(1));
    }

    #[test]
    fn test_multiple_ranges_no_trim() {
        let rl = rl(&[1..5, 10..15, 20..25]);
        // Inside each range
        assert_eq!(rl.search_position(1), Ok(0));
        assert_eq!(rl.search_position(4), Ok(0));
        assert_eq!(rl.search_position(10), Ok(1));
        assert_eq!(rl.search_position(14), Ok(1));
        assert_eq!(rl.search_position(20), Ok(2));
        assert_eq!(rl.search_position(24), Ok(2));
        // At boundaries
        assert_eq!(rl.search_position(5), Err(1));
        assert_eq!(rl.search_position(15), Err(2));
        assert_eq!(rl.search_position(0), Err(0));
        assert_eq!(rl.search_position(25), Err(3));
        // Between ranges
        assert_eq!(rl.search_position(7), Err(1));
        assert_eq!(rl.search_position(17), Err(2));
    }

    #[test]
    fn test_multiple_ranges_trimmed() {
        let base = rl(&[1..5, 10..15, 20..25]);
        let trimmed = RangeList::new_trimmed(base.inner().clone(), 2, 23);
        // First range trimmed
        assert_eq!(trimmed.search_position(1), Err(0));
        assert_eq!(trimmed.search_position(2), Ok(0));
        assert_eq!(trimmed.search_position(4), Ok(0));
        assert_eq!(trimmed.search_position(5), Err(1));
        // Last range trimmed
        assert_eq!(trimmed.search_position(22), Ok(2));
        assert_eq!(trimmed.search_position(23), Err(3));
        assert_eq!(trimmed.search_position(24), Err(3));

        assert_eq!(trimmed.search_position(12), Ok(1));
        assert_eq!(trimmed.search_position(7), Err(1));
        assert_eq!(trimmed.search_position(15), Err(2));
    }

    #[test]
    fn test_adjacent_ranges() {
        let rl = rl(&[1..3, 3..5, 5..7]);
        assert_eq!(rl.search_position(2), Ok(0));
        assert_eq!(rl.search_position(3), Ok(1));
        assert_eq!(rl.search_position(4), Ok(1));
        assert_eq!(rl.search_position(5), Ok(2));
        assert_eq!(rl.search_position(6), Ok(2));
        assert_eq!(rl.search_position(7), Err(3));
    }

    #[test]
    fn test_non_adjacent_ranges() {
        let rl = rl(&[1..3, 5..7, 10..12]);
        assert_eq!(rl.search_position(2), Ok(0));
        assert_eq!(rl.search_position(3), Err(1));
        assert_eq!(rl.search_position(4), Err(1));
        assert_eq!(rl.search_position(5), Ok(1));
        assert_eq!(rl.search_position(6), Ok(1));
        assert_eq!(rl.search_position(7), Err(2));
        assert_eq!(rl.search_position(9), Err(2));
        assert_eq!(rl.search_position(10), Ok(2));
        assert_eq!(rl.search_position(11), Ok(2));
        assert_eq!(rl.search_position(12), Err(3));
    }

    #[test]
    fn test_range_length_one() {
        let rl = rl(&[3..4, 6..7]);
        assert_eq!(rl.search_position(3), Ok(0));
        assert_eq!(rl.search_position(4), Err(1));
        assert_eq!(rl.search_position(6), Ok(1));
        assert_eq!(rl.search_position(7), Err(2));
    }

    #[test]
    fn test_large_rangelist() {
        let mut ranges = Vec::new();
        for i in 0..100 {
            ranges.push((i * 10)..(i * 10 + 5));
        }
        let rl = rl(&ranges);
        // Test a few inside and outside
        assert_eq!(rl.search_position(0), Ok(0));
        assert_eq!(rl.search_position(4), Ok(0));
        assert_eq!(rl.search_position(5), Err(1));
        assert_eq!(rl.search_position(55), Err(6));
        assert_eq!(rl.search_position(999), Err(100));
    }

    #[test]
    fn test_insertion_points() {
        let rl = rl(&[10..20, 30..40, 50..60]);
        assert_eq!(rl.search_position(5), Err(0));
        assert_eq!(rl.search_position(25), Err(1));
        assert_eq!(rl.search_position(45), Err(2));
        assert_eq!(rl.search_position(65), Err(3));
    }

    #[test]
    fn test_new_trimmed() {
        let empty = SharedVec::empty();
        let trimmed = RangeList::new_trimmed(empty, 0, 0);
        assert!(trimmed.is_empty());
        assert_eq!(trimmed.len(), 0);

        let base = rl(&[10..20]);
        let trimmed = RangeList::new_trimmed(base.inner().clone(), 10, 20);
        assert_eq!(trimmed.first, 10..20);
        assert_eq!(trimmed.last, 10..20);
        assert_eq!(trimmed.len(), 1);

        let base = rl(&[10..20]);
        let trimmed = RangeList::new_trimmed(base.inner().clone(), 12, 20);
        assert_eq!(trimmed.first, 12..20);
        assert_eq!(trimmed.last, 12..20);
        assert_eq!(trimmed.len(), 1);

        let base = rl(&[10..20]);
        let trimmed = RangeList::new_trimmed(base.inner().clone(), 10, 18);
        assert_eq!(trimmed.first, 10..18);
        assert_eq!(trimmed.last, 10..18);
        assert_eq!(trimmed.len(), 1);

        let base = rl(&[10..20]);
        let trimmed = RangeList::new_trimmed(base.inner().clone(), 15, 16);
        assert_eq!(trimmed.first, 15..16);
        assert_eq!(trimmed.last, 15..16);
        assert_eq!(trimmed.len(), 1);
    }

    #[test]
    #[should_panic]
    fn test_trimmed_single_range_start_too_high() {
        let base = rl(&[10..20]);
        let _ = RangeList::new_trimmed(base.inner().clone(), 20, 20);
    }

    #[test]
    #[should_panic]
    fn test_trimmed_single_range_end_too_low() {
        let base = rl(&[10..20]);
        let _ = RangeList::new_trimmed(base.inner().clone(), 10, 10);
    }

    #[test]
    fn test_trimmed_multiple_ranges_no_trim() {
        let base = rl(&[1..5, 10..15, 20..25]);
        let trimmed = RangeList::new_trimmed(base.inner().clone(), 1, 25);
        assert_eq!(trimmed.first, 1..5);
        assert_eq!(trimmed.last, 20..25);
        assert_eq!(trimmed.len(), 3);
    }

    #[test]
    fn test_trimmed_multiple_ranges_trim_first() {
        let base = rl(&[1..5, 10..15, 20..25]);
        let trimmed = RangeList::new_trimmed(base.inner().clone(), 3, 25);
        assert_eq!(trimmed.first, 3..5);
        assert_eq!(trimmed.last, 20..25);
        assert_eq!(trimmed.len(), 3);
    }

    #[test]
    fn test_trimmed_multiple_ranges_trim_last() {
        let base = rl(&[1..5, 10..15, 20..25]);
        let trimmed = RangeList::new_trimmed(base.inner().clone(), 1, 22);
        assert_eq!(trimmed.first, 1..5);
        assert_eq!(trimmed.last, 20..22);
        assert_eq!(trimmed.len(), 3);
    }

    #[test]
    fn test_trimmed_multiple_ranges_trim_both() {
        let base = rl(&[1..5, 10..15, 20..25]);
        let trimmed = RangeList::new_trimmed(base.inner().clone(), 2, 22);
        assert_eq!(trimmed.first, 2..5);
        assert_eq!(trimmed.last, 20..22);
        assert_eq!(trimmed.len(), 3);
    }

    #[test]
    fn test_clamp_empty_list() {
        let rl = RangeList::<usize>::empty();
        let result = rl.clamp(10..20);
        assert!(result.is_empty());
        verify_range_list(&result, 0..0);
    }

    #[test]
    fn test_clamp_empty_bounds() {
        let rl = rl(&[10..20]);
        let result = rl.clamp(15..15);
        assert!(result.is_empty());
        verify_range_list(&result, 0..0);
    }

    #[test]
    fn test_clamp_bounds_before_all() {
        let rl = rl(&[10..20, 30..40]);
        let result = rl.clamp(0..5);
        assert!(result.is_empty());
        verify_range_list(&result, 0..0);
    }

    #[test]
    fn test_clamp_bounds_after_all() {
        let rl = rl(&[10..20, 30..40]);
        let result = rl.clamp(50..60);
        verify_range_list(&result, 50..60);
        assert!(result.is_empty());
    }

    #[test]
    fn test_clamp_single_range_inside() {
        let rl = rl(&[10..20]);
        let result = rl.clamp(12..18);
        verify_range_list(&result, 12..18);
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(0), Some(&(12..18)));
    }

    #[test]
    fn test_clamp_single_range_exact() {
        let rl = rl(&[10..20]);
        let result = rl.clamp(10..20);
        verify_range_list(&result, 10..20);
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(0), Some(&(10..20)));
    }

    #[test]
    fn test_clamp_single_range_partial_left() {
        let rl = rl(&[10..20]);
        let result = rl.clamp(5..15);
        verify_range_list(&result, 5..15);
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(0), Some(&(10..15)));
    }

    #[test]
    fn test_clamp_single_range_partial_right() {
        let rl = rl(&[10..20]);
        let result = rl.clamp(15..25);
        verify_range_list(&result, 15..25);
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(0), Some(&(15..20)));
    }

    #[test]
    fn test_clamp_single_range_outside_left() {
        let rl = rl(&[10..20]);
        let result = rl.clamp(0..10);
        verify_range_list(&result, 0..0);
        assert!(result.is_empty());
    }

    #[test]
    fn test_clamp_single_range_outside_right() {
        let rl = rl(&[10..20]);
        let result = rl.clamp(20..30);
        verify_range_list(&result, 0..0);
        assert!(result.is_empty());
    }

    #[test]
    fn test_clamp_multiple_ranges_full_cover() {
        let rl = rl(&[10..20, 30..40, 50..60]);
        let result = rl.clamp(10..60);
        verify_range_list(&result, 10..60);
        assert_eq!(result.len(), 3);
        assert_eq!(result.get(0), Some(&(10..20)));
        assert_eq!(result.get(1), Some(&(30..40)));
        assert_eq!(result.get(2), Some(&(50..60)));
    }

    #[test]
    fn test_clamp_multiple_ranges_partial_cover() {
        let rl = rl(&[10..20, 30..40, 50..60]);
        let result = rl.clamp(15..55);
        verify_range_list(&result, 15..55);
        assert_eq!(result.len(), 3);
        assert_eq!(result.get(0), Some(&(15..20)));
        assert_eq!(result.get(1), Some(&(30..40)));
        assert_eq!(result.get(2), Some(&(50..55)));
    }

    #[test]
    fn test_clamp_multiple_ranges_inside_middle() {
        let rl = rl(&[10..20, 30..40, 50..60]);
        let result = rl.clamp(32..38);
        verify_range_list(&result, 32..38);
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(0), Some(&(32..38)));
    }

    #[test]
    fn test_clamp_multiple_ranges_between_ranges() {
        let rl = rl(&[10..20, 30..40, 50..60]);
        let result = rl.clamp(21..29);
        verify_range_list(&result, 0..0);
        assert!(result.is_empty());
    }

    #[test]
    fn test_clamp_multiple_ranges_boundaries() {
        let rl = rl(&[10..20, 30..40, 50..60]);
        let result = rl.clamp(10..30);
        verify_range_list(&result, 10..30);
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(0), Some(&(10..20)));

        let result = rl.clamp(10..31);
        verify_range_list(&result, 10..31);
        assert_eq!(result.len(), 2);
        assert_eq!(result.get(0), Some(&(10..20)));
        assert_eq!(result.get(1), Some(&(30..31)));
    }

    #[test]
    fn test_clamp_multiple_ranges_overlap_first_only() {
        let rl = rl(&[10..20, 30..40, 50..60]);
        let result = rl.clamp(15..18);
        verify_range_list(&result, 15..18);
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(0), Some(&(15..18)));
    }

    #[test]
    fn test_clamp_multiple_ranges_overlap_last_only() {
        let rl = rl(&[10..20, 30..40, 50..60]);
        let result = rl.clamp(58..65);
        verify_range_list(&result, 58..65);
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(0), Some(&(58..60)));
    }

    #[test]
    fn test_clamp_multiple_ranges_overlap_first_and_second() {
        let rl = rl(&[10..20, 30..40, 50..60]);
        let result = rl.clamp(15..35);
        verify_range_list(&result, 15..35);
        assert_eq!(result.len(), 2);
        assert_eq!(result.get(0), Some(&(15..20)));
        assert_eq!(result.get(1), Some(&(30..35)));
    }

    #[test]
    fn test_clamp_multiple_ranges_overlap_second_and_last() {
        let rl = rl(&[10..20, 30..40, 50..60]);
        let result = rl.clamp(35..55);
        verify_range_list(&result, 35..55);
        assert_eq!(result.len(), 2);
        assert_eq!(result.get(0), Some(&(35..40)));
        assert_eq!(result.get(1), Some(&(50..55)));
    }

    #[test]
    fn test_clamp_to_empty() {
        let rl = rl(&[10..20, 30..40]);
        let result = rl.clamp(15..15);
        verify_range_list(&result, 0..0);
        assert!(result.is_empty());
    }

    #[test]
    fn test_clamp_to_single_point() {
        let rl = rl(&[10..20, 30..40]);
        let result = rl.clamp(15..16);
        verify_range_list(&result, 15..16);
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(0), Some(&(15..16)));
    }

    #[test]
    fn test_clamp_exactly_first_range_start() {
        let rl = rl(&[10..20, 30..40]);
        let result = rl.clamp(10..15);
        verify_range_list(&result, 10..15);
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(0), Some(&(10..15)));
    }

    #[test]
    fn test_clamp_exactly_last_range_end() {
        let rl = rl(&[10..20, 30..40]);
        let result = rl.clamp(35..40);
        verify_range_list(&result, 35..40);
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(0), Some(&(35..40)));
    }

    #[test]
    fn test_clamp_large_list_various_bounds() {
        let mut ranges = Vec::new();
        for i in 0..100 {
            ranges.push((i * 10)..(i * 10 + 5));
        }
        let rl = rl(&ranges);

        // Clamp to cover all
        let result = rl.clamp(0..1000);
        verify_range_list(&result, 0..1000);
        assert_eq!(result.len(), 100);

        // Clamp to skip all but one
        let result = rl.clamp(250..255);
        verify_range_list(&result, 250..255);
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(0), Some(&(250..255)));

        // Clamp to skip all
        let result = rl.clamp(1000..1100);
        verify_range_list(&result, 1000..1100);
        assert!(result.is_empty());

        // Clamp to partials
        let result = rl.clamp(5..15);
        verify_range_list(&result, 5..15);
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(0), Some(&(10..15)));
    }

    #[track_caller]
    fn verify_range_list<T: Default + Copy + Clone + Ord + Eq + std::fmt::Debug>(
        list: &RangeList<T>,
        bounds: Range<T>,
    ) {
        if list.is_empty() {
            assert!(list.inner.is_empty());
            assert!(list.first.is_empty());
            assert!(list.last.is_empty());
            return;
        }

        let list_bounds = list.bounds();
        assert!(
            list_bounds.start >= bounds.start,
            "list_bounds.start ({:?}) < bounds.start ({:?})",
            list_bounds.start,
            bounds.start
        );
        assert!(
            list_bounds.end <= bounds.end,
            "list_bounds.end ({:?}) > bounds.end ({:?})",
            list_bounds.end,
            bounds.end
        );
        if !list.is_empty() {
            let inner = list.inner();
            let first = &list.first;
            let last = &list.last;
            assert!(
                first.start >= inner[0].start,
                "first.start ({:?}) < inner[0].start ({:?})",
                first.start,
                inner[0].start
            );
            assert!(
                first.end <= inner[0].end,
                "first.end ({:?}) > inner[0].end ({:?})",
                first.end,
                inner[0].end
            );
            let last_idx = inner.len() - 1;
            assert!(
                last.end <= inner[last_idx].end,
                "last.end ({:?}) > inner[last_idx].end ({:?})",
                last.end,
                inner[last_idx].end
            );
            assert!(
                last.start >= inner[last_idx].start,
                "last.start ({:?}) < inner[last_idx].start ({:?})",
                last.start,
                inner[last_idx].start
            );
        }
        for i in 0..list.len() {
            let r = list.get(i).unwrap();
            assert!(r.start < r.end, "range at index {i} is empty: {r:?}");
            if i + 1 < list.len() {
                let next = list.get(i + 1).unwrap();
                assert!(
                    r.end <= next.start,
                    "ranges at {} and {} overlap or not ascending: {:?}, {:?}",
                    i,
                    i + 1,
                    r,
                    next
                );
            }
        }
    }

    #[test]
    fn test_owned_iterator_equivalence() {
        let cases: &[&[Range<usize>]] = &[&[], &[10..20], &[1..5, 10..15, 20..25], &[3..4, 6..7]];
        for &ranges in cases {
            let rl = RangeList::from_slice(ranges);
            let borrowed: Vec<_> = rl.iter().cloned().collect();
            let owned: Vec<_> = rl.clone().into_iter().collect();
            assert_eq!(
                borrowed, owned,
                "Owned iterator did not match borrowed for {ranges:?}"
            );
        }

        // Also test trimmed
        let base = RangeList::from_slice(&[10..20]);
        let trimmed = RangeList::new_trimmed(base.inner().clone(), 12, 18);
        let borrowed: Vec<_> = trimmed.iter().cloned().collect();
        let owned: Vec<_> = trimmed.clone().into_iter().collect();
        assert_eq!(
            borrowed, owned,
            "Owned iterator did not match borrowed for trimmed list"
        );
    }
}
