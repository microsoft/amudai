//! An immutable, owning container for a sorted, non-overlapping list of half-open ranges.

use amudai_shared_vec::SharedVec;
use std::{
    convert::identity,
    ops::{Range, RangeBounds},
};

use crate::{RangeListSlice, range_list_slice::trim_edges};

/// An immutable, owning container for a sorted, non-overlapping list of half-open ranges.
///
/// `SharedRangeList<T>` mirrors [`RangeListSlice`], but it owns its underlying storage via
/// a shared backing container ([`SharedVec<Range<T>>`](amudai_shared_vec::SharedVec)).
/// Cloning a `SharedRangeList` is cheap (O(1)) because it shares the underlying storage;
/// creating sub-views via [`slice`](Self::slice) is also O(1) and returns another
/// `SharedRangeList` that logically references a subset of the same backing data.
///
/// Like its borrowed counterpart, the first and last logical ranges can be “trimmed”: the
/// logical start of the first range may be greater than the physical `inner[0].start`, and the
/// logical end of the last range may be less than `inner[last].end`. This allows efficient
/// sub-views without copying or modifying the physical ranges.
///
/// Key characteristics:
/// - Owning, shareable storage: backed by `SharedVec<Range<T>>` for cheap clones and slices.
/// - Immutability: once constructed, ranges cannot be mutated through this type.
/// - Trimming: first/last logical ranges can be adjusted to represent sub-views whose
///   boundaries do not align with physical range boundaries.
/// - Half-open semantics: every `Range<T>` is interpreted as `[start, end)` (end-exclusive).
///
/// Invariants and expectations:
/// - The stored ranges are sorted by `start` and non-overlapping:
///   `inner[i].end <= inner[i + 1].start` for all valid `i`.
/// - `T` must at least be `PartialOrd` for search operations. Methods that count or compute
///   positions additionally require `T: Default + Clone + Add<Output = T> + Sub<Output = T>`.
/// - This is a logical view over owned storage: `self.first`/`self.last` may differ from the
///   first/last physical ranges in `inner` to reflect trimming.
///
/// When to choose which type:
/// - Use `SharedRangeList` when you need an owned container you can store, clone, and pass
///   around while keeping allocations minimal.
/// - Use [`RangeListSlice`] when you only need a lightweight borrowed view over an existing
///   `&[Range<T>]`.
///
/// Complexity overview:
/// - `search_position` / `contains_position`: O(log n)
/// - `slice`, `clamp`, `split_at_position`: O(log n) to locate boundaries + O(1) to build views
/// - Iteration: O(n)
#[derive(Clone)]
pub struct SharedRangeList<T> {
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

impl<T: Clone> SharedRangeList<T> {
    /// Constructs a `SharedRangeList` containing a single range.
    ///
    /// # Arguments
    ///
    /// * `r` - The range to store.
    ///
    /// # Returns
    ///
    /// A `SharedRangeList` with one logical range.
    pub fn from_elem(r: Range<T>) -> Self {
        SharedRangeList {
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

    /// Returns a borrowed `RangeListSlice` view of this list.
    ///
    /// The returned view:
    /// - Borrows the underlying storage (no allocation or copying).
    /// - Preserves the current logical trimming (`first`/`last`) of this list.
    /// - Is cheap to clone and slice (O(1)).
    ///
    /// Lifetime:
    /// - The view is tied to `&self`.
    ///
    /// Complexity: O(1).
    #[inline]
    pub fn as_slice(&self) -> RangeListSlice<'_, T> {
        RangeListSlice::from_parts(self.first.clone(), &self.inner, self.last.clone())
    }
}

impl<T: Default + Clone> From<Vec<Range<T>>> for SharedRangeList<T> {
    fn from(vec: Vec<Range<T>>) -> Self {
        SharedRangeList::new(vec.into())
    }
}

impl<T: Default + Clone> From<&[Range<T>]> for SharedRangeList<T> {
    fn from(slice: &[Range<T>]) -> Self {
        SharedRangeList::from_slice(slice)
    }
}

impl<T: Clone> From<Range<T>> for SharedRangeList<T> {
    fn from(range: Range<T>) -> Self {
        SharedRangeList::from_elem(range)
    }
}

impl<T: Default + Clone> std::iter::FromIterator<Range<T>> for SharedRangeList<T> {
    fn from_iter<I: IntoIterator<Item = Range<T>>>(iter: I) -> Self {
        let vec: Vec<Range<T>> = iter.into_iter().collect();
        SharedRangeList::from(vec)
    }
}

impl<T: Default + Clone> SharedRangeList<T> {
    /// Creates a new, empty `SharedRangeList<T>`.
    ///
    /// # Returns
    ///
    /// An empty `SharedRangeList`.
    pub fn empty() -> Self {
        SharedRangeList {
            inner: SharedVec::empty(),
            first: T::default()..T::default(),
            last: T::default()..T::default(),
        }
    }

    /// Constructs a new `SharedRangeList<T>` from a shared slice of ranges.
    ///
    /// # Arguments
    ///
    /// * `inner` - Shared, sorted, non-overlapping ranges.
    ///
    /// # Returns
    ///
    /// A `SharedRangeList` covering the full extent of the provided ranges.
    pub fn new(inner: SharedVec<Range<T>>) -> Self {
        if inner.is_empty() {
            Self::empty()
        } else {
            let first = inner[0].clone();
            let last = inner.last().unwrap().clone();
            Self { inner, first, last }
        }
    }

    /// Constructs a `SharedRangeList` from a slice of ranges.
    ///
    /// # Arguments
    ///
    /// * `slice` - Slice of sorted, non-overlapping ranges.
    ///
    /// # Returns
    ///
    /// A `SharedRangeList` covering the full extent of the provided ranges.
    pub fn from_slice(slice: &[Range<T>]) -> Self {
        Self::new(SharedVec::from_slice(slice))
    }
}

impl<T: PartialOrd + Clone> SharedRangeList<T> {
    /// Searches for the logical range containing the specified position.
    ///
    /// Performs a binary search over the logical ranges in this `SharedRangeList`
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
        self.as_slice().search_position(pos)
    }

    /// Returns `true` if the specified position is contained within any logical range in the list.
    ///
    /// Performs a binary search to determine whether `pos` falls within any of the logical ranges
    /// represented by this `SharedRangeList`.
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

    /// Returns the total number of positions covered by all logical ranges in the list.
    ///
    /// This method sums up the sizes of all logical ranges to calculate the total count
    /// of positions represented by this `SharedRangeList`.
    ///
    /// # Returns
    ///
    /// The total count of positions across all ranges. For an empty list, returns `T::default()`
    /// (typically 0 for numeric types).
    ///
    /// # Relationship to `position_at_count`
    ///
    /// This method is the inverse operation of [`position_at_count`](Self::position_at_count).
    /// If `count_positions()` returns `n`, then `position_at_count(i)` will return `Some(_)`
    /// for all `i` in `0..n` and `None` for `i >= n`.
    pub fn count_positions(&self) -> T
    where
        T: Default + std::ops::Sub<Output = T> + std::ops::Add<Output = T> + Clone,
    {
        self.as_slice().count_positions()
    }

    /// Finds the range index and position corresponding to the given count of positions.
    ///
    /// This method performs the inverse operation of `count_positions()`. Given a count representing
    /// the number of positions from the start of the list, it returns the range containing that
    /// position and the actual position value at that count.
    ///
    /// # Arguments
    ///
    /// * `count` - The number of positions from the start of the list.
    ///
    /// # Returns
    ///
    /// * `Some((range_index, position))` where:
    ///   - `range_index` is the index of the range containing the position
    ///   - `position` is the actual position value at that count. If that value is passed to
    ///     [`split_at_position`](Self::split_at_position), the left range would contain exactly
    ///     `count` positions.
    /// * `None` if `count` equals or exceeds the total count of positions in the list
    ///
    /// # Example
    ///
    /// ```
    /// # use amudai_ranges::SharedRangeList;
    /// let list = SharedRangeList::from(vec![10..15, 20..25, 30..35]);
    /// // Total positions: 5 + 5 + 5 = 15
    ///
    /// assert_eq!(list.position_at_count(0), Some((0, 10)));   // First position
    /// assert_eq!(list.position_at_count(3), Some((0, 13)));   // Position 13 in first range
    /// assert_eq!(list.position_at_count(5), Some((1, 20)));   // First position of second range
    /// assert_eq!(list.position_at_count(7), Some((1, 22)));   // Position 22 in second range
    /// assert_eq!(list.position_at_count(10), Some((2, 30)));  // First position of third range
    /// assert_eq!(list.position_at_count(14), Some((2, 34)));  // Last position
    /// assert_eq!(list.position_at_count(15), None);           // Out of bounds
    /// ```
    pub fn position_at_count(&self, count: T) -> Option<(usize, T)>
    where
        T: Default + std::ops::Sub<Output = T> + std::ops::Add<Output = T> + Clone,
    {
        self.as_slice().position_at_count(count)
    }
}

impl<T: Default + Clone + PartialOrd> SharedRangeList<T> {
    /// Splits the range list into two `SharedRangeList`s at the specified position.
    ///
    /// The first returned `SharedRangeList` contains all portions of ranges strictly
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
        self.split_at_position_with_range_hint(pos, None)
    }

    /// Splits the range list into two parts at the position corresponding to the given count.
    ///
    /// This method finds the position that would be at the given count (using `position_at_count`)
    /// and then splits the list at that position. The first returned `SharedRangeList` contains
    /// exactly `count` positions, and the second contains the remaining positions.
    ///
    /// # Arguments
    ///
    /// * `count` - The number of positions that should be in the first (left) part.
    ///
    /// # Returns
    ///
    /// A tuple `(left, right)` where:
    /// - `left` contains exactly `count` positions (or all positions if `count` exceeds the total)
    /// - `right` contains the remaining positions
    ///
    /// # Example
    ///
    /// ```
    /// # use amudai_ranges::SharedRangeList;
    /// let list = SharedRangeList::from(vec![10..15, 20..25, 30..35]);
    /// // Total positions: 5 + 5 + 5 = 15
    ///
    /// let (left, right) = list.split_at_position_count(7);
    /// assert_eq!(left.count_positions(), 7);
    /// assert_eq!(right.count_positions(), 8);
    /// // The split happens at position 22 (which is at count 7)
    /// ```
    ///
    /// # Behavior
    ///
    /// - If `count` is 0, returns `(empty, self.clone())`
    /// - If `count` equals or exceeds the total number of positions, returns
    ///   `(self.clone(), empty)`
    /// - Otherwise, splits at the position corresponding to the given count
    pub fn split_at_position_count(&self, count: T) -> (Self, Self)
    where
        T: std::ops::Sub<Output = T> + std::ops::Add<Output = T>,
    {
        // Handle edge cases
        if count == T::default() {
            // count is 0, return (empty, self)
            return (Self::empty(), self.clone());
        }

        // Check if we have a position at this count
        match self.position_at_count(count.clone()) {
            Some((range_idx, position)) => {
                // Split at the found position
                self.split_at_position_with_range_hint(position, Some(range_idx))
            }
            _ => {
                // count equals or exceeds total positions, return (self, empty)
                (self.clone(), Self::empty())
            }
        }
    }

    /// Returns a new `SharedRangeList<T>` containing only the portions of ranges within
    /// the specified bounds.
    ///
    /// This method restricts the current `SharedRangeList` to the intersection with the
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
    /// A new `SharedRangeList<T>` containing only the elements within `bounds`.
    /// Returns an empty list if the original list is empty, if `bounds` is empty,
    /// or if there is no overlap.
    ///
    /// # Complexity
    ///
    /// `O(log n)`, uses binary search to locate the relevant ranges.
    pub fn clamp(&self, bounds: Range<T>) -> SharedRangeList<T> {
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

    /// Constructs a new `SharedRangeList<T>` from a shared slice of ranges,
    /// trimming the first and last ranges to the specified bounds.
    ///
    /// This method creates a `SharedRangeList` that logically starts at `start`
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
    /// A `SharedRangeList<T>` representing the trimmed view. Returns an empty list if `inner` is empty.
    fn new_trimmed(inner: SharedVec<Range<T>>, start: T, end: T) -> Self {
        if inner.is_empty() {
            Self::empty()
        } else {
            let (first, last) = trim_edges(&inner, start, end);
            Self { inner, first, last }
        }
    }

    /// Splits the range list into two `SharedRangeList`s at the specified position.
    ///
    /// The first returned `SharedRangeList` contains all portions of ranges strictly
    /// before `pos`.
    /// The second contains all portions of ranges starting at or after `pos`.
    ///
    /// If `pos` falls within a range, that range is split between the two outputs.
    /// If `pos` is outside the bounds of the list, one of the outputs will be empty.
    ///
    /// # Arguments
    ///
    /// * `pos` - The position at which to split the range list.
    /// * `range_idx` - The index of the first range that contains at least some positions
    ///   that are equal to or greater than `pos` (optional hint).
    pub fn split_at_position_with_range_hint(
        &self,
        pos: T,
        range_idx: Option<usize>,
    ) -> (Self, Self) {
        if self.is_empty() {
            return (self.clone(), self.clone());
        }

        let bounds = self.bounds();
        if pos <= bounds.start {
            return (Self::empty(), self.clone());
        }
        if pos >= bounds.end {
            return (self.clone(), Self::empty());
        }

        // Find the range containing or after pos
        let idx = match range_idx {
            Some(idx) if self.range_covers_or_follows_position(idx, pos.clone()) => idx,
            _ => {
                // No hint or invalid hint provided, use search
                self.search_position(pos.clone()).unwrap_or_else(identity)
            }
        };

        let range = self.get(idx).expect("range");
        if pos <= range.start {
            // Split index is between the ranges (before idx)
            let first = self.slice(0..idx);
            let second = self.slice(idx..);
            (first, second)
        } else {
            // Split index is on the range
            let first = Self::new_trimmed(self.slice(0..idx + 1).inner, bounds.start, pos.clone());
            let second = Self::new_trimmed(self.slice(idx..).inner, pos, bounds.end);
            (first, second)
        }
    }

    /// Checks if a given range index covers the position or if the position falls in the gap
    /// immediately before it.
    ///
    /// This method is used as a hint validator for split operations. It determines whether
    /// the range at `range_idx` is the correct range to use when splitting at `pos`.
    ///
    /// # Arguments
    ///
    /// * `range_idx` - The index of the range to check. Can be equal to `self.len()` to check
    ///   if the position is at or after the end of all ranges.
    /// * `pos` - The position to check.
    ///
    /// # Returns
    ///
    /// * `true` if:
    ///   - The range at `range_idx` contains `pos`, or
    ///   - `pos` falls in the gap between the previous range and the range at `range_idx`, or
    ///   - `range_idx == 0` and `pos` is before the first range, or
    ///   - `range_idx == self.len()` and `pos` is at or after the end of the last range
    /// * `false` otherwise
    ///
    /// # Panics
    ///
    /// Panics if `range_idx > self.len()`.
    pub fn range_covers_or_follows_position(&self, range_idx: usize, pos: T) -> bool {
        self.as_slice()
            .range_covers_or_follows_position(range_idx, pos)
    }
}

impl<T> SharedRangeList<T> {
    /// Returns the number of logical ranges in the list.
    ///
    /// # Returns
    ///
    /// The count of logical ranges.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the list contains no logical ranges.
    ///
    /// # Returns
    ///
    /// `true` if empty, `false` otherwise.
    #[inline]
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
    #[inline]
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
    pub fn iter(&self) -> SharedRangeListIter<'_, T> {
        SharedRangeListIter {
            list: self,
            index: 0,
        }
    }
}

impl<T: Default + Clone> SharedRangeList<T> {
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
            _ => 0,
        };
        let end = match range.end_bound() {
            Included(&n) => n + 1,
            Excluded(&n) => n,
            _ => len,
        };
        assert!(start <= end && end <= len, "slice out of bounds");

        if start == end {
            return Self::empty();
        }

        let new_inner = self.inner.slice(start..end);

        let new_first = self.get(start).expect("start").clone();
        let new_last = self.get(end - 1).expect("end").clone();
        Self {
            inner: new_inner,
            first: new_first,
            last: new_last,
        }
    }
}

impl<T> std::ops::Index<usize> for SharedRangeList<T> {
    type Output = Range<T>;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        match self.get(index) {
            Some(range) => range,
            None => panic!(
                "index out of bounds: the len is {} but the index is {}",
                self.len(),
                index
            ),
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for SharedRangeList<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries((0..self.len()).map(|i| self.get(i).unwrap()))
            .finish()
    }
}

impl<T: Default + Clone> Default for SharedRangeList<T> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<T: PartialEq> PartialEq for SharedRangeList<T> {
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

impl<T: Eq> Eq for SharedRangeList<T> {}

impl<'a, T: Default + Clone> From<RangeListSlice<'a, T>> for SharedRangeList<T> {
    fn from(slice: RangeListSlice<'a, T>) -> Self {
        // Preserve logical trimming by cloning the logical ranges into owned storage
        SharedRangeList::from(slice.to_vec())
    }
}

/// Iterator over logical ranges in a `SharedRangeList`, applying any trimming
/// to the first and last ranges.
///
/// This struct is created by [`SharedRangeList::iter`].
#[derive(Clone)]
pub struct SharedRangeListIter<'a, T> {
    list: &'a SharedRangeList<T>,
    index: usize,
}

impl<'a, T> Iterator for SharedRangeListIter<'a, T> {
    type Item = &'a Range<T>;

    #[inline]
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

impl<T> ExactSizeIterator for SharedRangeListIter<'_, T> {}

impl<'a, T> IntoIterator for &'a SharedRangeList<T> {
    type Item = &'a Range<T>;

    type IntoIter = SharedRangeListIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        SharedRangeListIter {
            list: self,
            index: 0,
        }
    }
}

/// Owned iterator over logical ranges in a `SharedRangeList`, applying any trimming
/// to the first and last ranges.
///
/// This struct is created by consuming a `SharedRangeList`.
#[derive(Clone)]
pub struct SharedRangeListIntoIter<T> {
    list: SharedRangeList<T>,
    index: usize,
}

impl<T: Clone> Iterator for SharedRangeListIntoIter<T> {
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

impl<T: Clone> ExactSizeIterator for SharedRangeListIntoIter<T> {}

impl<T: Clone> IntoIterator for SharedRangeList<T> {
    type Item = Range<T>;
    type IntoIter = SharedRangeListIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        SharedRangeListIntoIter {
            list: self,
            index: 0,
        }
    }
}

#[cfg(test)]
#[allow(clippy::single_range_in_vec_init)]
mod tests {
    use super::*;
    use std::ops::Range;

    fn rl(ranges: &[Range<usize>]) -> SharedRangeList<usize> {
        SharedRangeList::from_slice(ranges)
    }

    #[test]
    fn test_empty() {
        let rl = SharedRangeList::<usize>::empty();
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
        let trimmed = SharedRangeList::new_trimmed(base.inner().clone(), 12, 18);
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
        let trimmed = SharedRangeList::new_trimmed(base.inner().clone(), 2, 23);
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
        let trimmed = SharedRangeList::new_trimmed(empty, 0, 0);
        assert!(trimmed.is_empty());
        assert_eq!(trimmed.len(), 0);

        let base = rl(&[10..20]);
        let trimmed = SharedRangeList::new_trimmed(base.inner().clone(), 10, 20);
        assert_eq!(trimmed.first, 10..20);
        assert_eq!(trimmed.last, 10..20);
        assert_eq!(trimmed.len(), 1);

        let base = rl(&[10..20]);
        let trimmed = SharedRangeList::new_trimmed(base.inner().clone(), 12, 20);
        assert_eq!(trimmed.first, 12..20);
        assert_eq!(trimmed.last, 12..20);
        assert_eq!(trimmed.len(), 1);

        let base = rl(&[10..20]);
        let trimmed = SharedRangeList::new_trimmed(base.inner().clone(), 10, 18);
        assert_eq!(trimmed.first, 10..18);
        assert_eq!(trimmed.last, 10..18);
        assert_eq!(trimmed.len(), 1);

        let base = rl(&[10..20]);
        let trimmed = SharedRangeList::new_trimmed(base.inner().clone(), 15, 16);
        assert_eq!(trimmed.first, 15..16);
        assert_eq!(trimmed.last, 15..16);
        assert_eq!(trimmed.len(), 1);
    }

    #[test]
    #[should_panic]
    fn test_trimmed_single_range_start_too_high() {
        let base = rl(&[10..20]);
        let _ = SharedRangeList::new_trimmed(base.inner().clone(), 20, 20);
    }

    #[test]
    #[should_panic]
    fn test_trimmed_single_range_end_too_low() {
        let base = rl(&[10..20]);
        let _ = SharedRangeList::new_trimmed(base.inner().clone(), 10, 10);
    }

    #[test]
    fn test_trimmed_multiple_ranges_no_trim() {
        let base = rl(&[1..5, 10..15, 20..25]);
        let trimmed = SharedRangeList::new_trimmed(base.inner().clone(), 1, 25);
        assert_eq!(trimmed.first, 1..5);
        assert_eq!(trimmed.last, 20..25);
        assert_eq!(trimmed.len(), 3);
    }

    #[test]
    fn test_trimmed_multiple_ranges_trim_first() {
        let base = rl(&[1..5, 10..15, 20..25]);
        let trimmed = SharedRangeList::new_trimmed(base.inner().clone(), 3, 25);
        assert_eq!(trimmed.first, 3..5);
        assert_eq!(trimmed.last, 20..25);
        assert_eq!(trimmed.len(), 3);
    }

    #[test]
    fn test_trimmed_multiple_ranges_trim_last() {
        let base = rl(&[1..5, 10..15, 20..25]);
        let trimmed = SharedRangeList::new_trimmed(base.inner().clone(), 1, 22);
        assert_eq!(trimmed.first, 1..5);
        assert_eq!(trimmed.last, 20..22);
        assert_eq!(trimmed.len(), 3);
    }

    #[test]
    fn test_trimmed_multiple_ranges_trim_both() {
        let base = rl(&[1..5, 10..15, 20..25]);
        let trimmed = SharedRangeList::new_trimmed(base.inner().clone(), 2, 22);
        assert_eq!(trimmed.first, 2..5);
        assert_eq!(trimmed.last, 20..22);
        assert_eq!(trimmed.len(), 3);
    }

    #[test]
    fn test_clamp_empty_list() {
        let rl = SharedRangeList::<usize>::empty();
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
        list: &SharedRangeList<T>,
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
            let rl = SharedRangeList::from_slice(ranges);
            let borrowed: Vec<_> = rl.iter().cloned().collect();
            let owned: Vec<_> = rl.clone().into_iter().collect();
            assert_eq!(
                borrowed, owned,
                "Owned iterator did not match borrowed for {ranges:?}"
            );
        }

        // Also test trimmed
        let base = SharedRangeList::from_slice(&[10..20]);
        let trimmed = SharedRangeList::new_trimmed(base.inner().clone(), 12, 18);
        let borrowed: Vec<_> = trimmed.iter().cloned().collect();
        let owned: Vec<_> = trimmed.clone().into_iter().collect();
        assert_eq!(
            borrowed, owned,
            "Owned iterator did not match borrowed for trimmed list"
        );
    }

    #[test]
    fn test_count_positions_empty() {
        let rl = SharedRangeList::<usize>::empty();
        assert_eq!(rl.count_positions(), 0);
    }

    #[test]
    fn test_count_positions_single_range() {
        let range_list = rl(&[10..20]);
        assert_eq!(range_list.count_positions(), 10); // 20 - 10 = 10 positions

        let single_pos = rl(&[5..6]);
        assert_eq!(single_pos.count_positions(), 1); // 6 - 5 = 1 position
    }

    #[test]
    fn test_count_positions_multiple_ranges() {
        let range_list = rl(&[10..15, 20..25, 30..35]);
        // Range sizes: 5 + 5 + 5 = 15 total positions
        assert_eq!(range_list.count_positions(), 15);

        let smaller_ranges = rl(&[1..3, 5..7, 10..12]);
        // Range sizes: 2 + 2 + 2 = 6 total positions
        assert_eq!(smaller_ranges.count_positions(), 6);
    }

    #[test]
    fn test_count_positions_trimmed() {
        let base = rl(&[10..20, 30..40]);
        let trimmed = SharedRangeList::new_trimmed(base.inner().clone(), 12, 38);
        // First range: 12..20 (8 positions), Second range: 30..38 (8 positions)
        assert_eq!(trimmed.count_positions(), 16);
    }

    #[test]
    fn test_position_at_count_empty() {
        let rl = SharedRangeList::<usize>::empty();
        assert_eq!(rl.position_at_count(0), None);
        assert_eq!(rl.position_at_count(5), None);
    }

    #[test]
    fn test_position_at_count_single_range() {
        let rl = rl(&[10..15]); // 5 positions: 10, 11, 12, 13, 14

        assert_eq!(rl.position_at_count(0), Some((0, 10)));
        assert_eq!(rl.position_at_count(1), Some((0, 11)));
        assert_eq!(rl.position_at_count(2), Some((0, 12)));
        assert_eq!(rl.position_at_count(3), Some((0, 13)));
        assert_eq!(rl.position_at_count(4), Some((0, 14)));
        assert_eq!(rl.position_at_count(5), None); // Out of bounds
    }

    #[test]
    fn test_position_at_count_multiple_ranges() {
        let rl = rl(&[10..15, 20..25, 30..35]);
        // Range 0: positions 10-14 (counts 0-4)
        // Range 1: positions 20-24 (counts 5-9)
        // Range 2: positions 30-34 (counts 10-14)
        // Total: 15 positions

        // First range
        assert_eq!(rl.position_at_count(0), Some((0, 10)));
        assert_eq!(rl.position_at_count(3), Some((0, 13)));
        assert_eq!(rl.position_at_count(4), Some((0, 14)));

        // Second range
        assert_eq!(rl.position_at_count(5), Some((1, 20)));
        assert_eq!(rl.position_at_count(7), Some((1, 22)));
        assert_eq!(rl.position_at_count(9), Some((1, 24)));

        // Third range
        assert_eq!(rl.position_at_count(10), Some((2, 30)));
        assert_eq!(rl.position_at_count(12), Some((2, 32)));
        assert_eq!(rl.position_at_count(14), Some((2, 34)));

        // Out of bounds
        assert_eq!(rl.position_at_count(15), None);
        assert_eq!(rl.position_at_count(100), None);
    }

    #[test]
    fn test_position_at_count_single_position_ranges() {
        let rl = rl(&[5..6, 10..11, 15..16]);
        // Each range has 1 position

        assert_eq!(rl.position_at_count(0), Some((0, 5)));
        assert_eq!(rl.position_at_count(1), Some((1, 10)));
        assert_eq!(rl.position_at_count(2), Some((2, 15)));
        assert_eq!(rl.position_at_count(3), None);
    }

    #[test]
    fn test_count_and_position_consistency() {
        let test_cases = vec![
            vec![10..15],
            vec![10..15, 20..25, 30..35],
            vec![1..3, 5..7, 10..12, 20..22],
            vec![0..1, 2..3, 4..5],
        ];

        for ranges in test_cases {
            let rl = rl(&ranges);
            let total_count = rl.count_positions();

            // Test that all valid counts return valid positions
            for count in 0..total_count {
                let result = rl.position_at_count(count);
                assert!(
                    result.is_some(),
                    "count {count} should be valid for ranges {ranges:?}"
                );

                let (range_idx, position) = result.unwrap();
                assert!(
                    range_idx < rl.len(),
                    "range index {range_idx} out of bounds"
                );

                let range = rl.get(range_idx).unwrap();
                assert!(
                    position >= range.start && position < range.end,
                    "position {position} not in range {range:?}"
                );
            }

            // Test that count == total_count returns None
            assert_eq!(rl.position_at_count(total_count), None);
            assert_eq!(rl.position_at_count(total_count + 10), None);
        }
    }

    #[test]
    fn test_split_at_position_edge_cases() {
        let r = SharedRangeList::<usize>::empty();
        let (left, right) = r.split_at_position(0);
        assert!(left.is_empty());
        assert!(right.is_empty());

        let (left, right) = r.split_at_position(1);
        assert!(left.is_empty());
        assert!(right.is_empty());

        let (left, right) = r.split_at_position(100);
        assert!(left.is_empty());
        assert!(right.is_empty());

        let r = rl(&[10..20]);
        let (left, right) = r.split_at_position(0);
        assert!(left.is_empty());
        assert_eq!(right.bounds(), 10..20);
        let (left, right) = r.split_at_position(10);
        assert!(left.is_empty());
        assert_eq!(right.bounds(), 10..20);
        let (left, right) = r.split_at_position(11);
        assert_eq!(left.bounds(), 10..11);
        assert_eq!(right.bounds(), 11..20);

        let r = rl(&[0..20]);
        let (left, right) = r.split_at_position(0);
        assert!(left.is_empty());
        assert_eq!(right.bounds(), 0..20);
        let (left, right) = r.split_at_position(1);
        assert_eq!(left.bounds(), 0..1);
        assert_eq!(right.bounds(), 1..20);
        let (left, right) = r.split_at_position(19);
        assert_eq!(left.bounds(), 0..19);
        assert_eq!(right.bounds(), 19..20);
        let (left, right) = r.split_at_position(20);
        assert_eq!(left.bounds(), 0..20);
        assert!(right.is_empty());
    }

    #[test]
    fn test_split_at_position_with_count_positions() {
        let rl = rl(&[10..15, 20..25, 30..35]);
        let total_positions = rl.count_positions(); // 15 positions

        // Test splitting at various positions and verify counts
        for count in 0..=total_positions {
            if count == total_positions {
                // At the end, split should give (full_list, empty)
                let bounds = rl.bounds();
                let (left, right) = rl.split_at_position(bounds.end);
                assert_eq!(left.count_positions(), total_positions);
                assert_eq!(right.count_positions(), 0);
                assert!(right.is_empty());
            } else {
                // Find the position for this count
                let (_, position) = rl.position_at_count(count).unwrap();

                // Split at that position
                let (left, right) = rl.split_at_position(position);

                // Verify counts
                let left_count = left.count_positions();
                let right_count = right.count_positions();

                // The left part contains positions strictly before 'position'
                // The right part contains positions at or after 'position'
                // Since position_at_count(count) returns the position of the count-th element,
                // left should have exactly 'count' positions, and right should have the rest
                assert_eq!(
                    left_count, count,
                    "left count mismatch at position {position} (count {count})"
                );
                assert_eq!(
                    left_count + right_count,
                    total_positions,
                    "total count mismatch after split at position {position} (count {count})"
                );

                // Verify that right part starts at or after the split position
                if !right.is_empty() {
                    let right_bounds = right.bounds();
                    assert!(
                        right_bounds.start >= position,
                        "right part should start at or after split position {} but starts at {}",
                        position,
                        right_bounds.start
                    );
                }

                // Verify that left part ends before or at the split position
                if !left.is_empty() {
                    let left_bounds = left.bounds();
                    assert!(
                        left_bounds.end <= position,
                        "left part should end before or at split position {} but ends at {}",
                        position,
                        left_bounds.end
                    );
                }
            }
        }
    }

    #[test]
    fn test_split_count_roundtrip() {
        let rl = rl(&[10..15, 20..25, 30..35]);

        // Test that splitting and then counting gives us the expected results
        for count in 0..rl.count_positions() {
            let (_, split_pos) = rl.position_at_count(count).unwrap();
            let (left, right) = rl.split_at_position(split_pos);

            // The left part should have exactly 'count' positions
            assert_eq!(left.count_positions(), count);

            // The right part should have the remaining positions
            assert_eq!(right.count_positions(), rl.count_positions() - count);

            // If we have a non-empty right part, its first position should be split_pos
            if !right.is_empty() {
                let right_bounds = right.bounds();
                assert_eq!(right_bounds.start, split_pos);
            }
        }
    }

    #[test]
    fn test_position_at_count_with_gaps() {
        let rl = rl(&[1..3, 10..12, 100..103]);
        // Range 0: positions 1,2 (counts 0,1)
        // Range 1: positions 10,11 (counts 2,3)
        // Range 2: positions 100,101,102 (counts  4,5,6)

        assert_eq!(rl.position_at_count(0), Some((0, 1)));
        assert_eq!(rl.position_at_count(1), Some((0, 2)));
        assert_eq!(rl.position_at_count(2), Some((1, 10)));
        assert_eq!(rl.position_at_count(3), Some((1, 11)));
        assert_eq!(rl.position_at_count(4), Some((2, 100)));
        assert_eq!(rl.position_at_count(5), Some((2, 101)));
        assert_eq!(rl.position_at_count(6), Some((2, 102)));
        assert_eq!(rl.position_at_count(7), None);

        assert_eq!(rl.count_positions(), 7);
    }

    #[test]
    fn test_split_at_position_count_empty() {
        let rl = SharedRangeList::<usize>::empty();

        // Split at count 0 on empty list
        let (left, right) = rl.split_at_position_count(0);
        assert!(left.is_empty());
        assert!(right.is_empty());

        // Split at count > 0 on empty list
        let (left, right) = rl.split_at_position_count(5);
        assert!(left.is_empty());
        assert!(right.is_empty());
    }

    #[test]
    fn test_split_at_position_count_single_range() {
        let rl = rl(&[10..15]); // 5 positions: 10, 11, 12, 13, 14

        // Split at count 0 (beginning)
        let (left, right) = rl.split_at_position_count(0);
        assert!(left.is_empty());
        assert_eq!(right.count_positions(), 5);
        assert_eq!(right.bounds(), 10..15);

        // Split at count 1 (after first position)
        let (left, right) = rl.split_at_position_count(1);
        assert_eq!(left.count_positions(), 1);
        assert_eq!(right.count_positions(), 4);
        assert_eq!(left.bounds(), 10..11);
        assert_eq!(right.bounds(), 11..15);

        // Split at count 3 (middle)
        let (left, right) = rl.split_at_position_count(3);
        assert_eq!(left.count_positions(), 3);
        assert_eq!(right.count_positions(), 2);
        assert_eq!(left.bounds(), 10..13);
        assert_eq!(right.bounds(), 13..15);

        // Split at count 5 (end)
        let (left, right) = rl.split_at_position_count(5);
        assert_eq!(left.count_positions(), 5);
        assert!(right.is_empty());
        assert_eq!(left.bounds(), 10..15);

        // Split at count > total
        let (left, right) = rl.split_at_position_count(10);
        assert_eq!(left.count_positions(), 5);
        assert!(right.is_empty());
        assert_eq!(left.bounds(), 10..15);
    }

    #[test]
    fn test_split_at_position_count_multiple_ranges() {
        let rl = rl(&[10..15, 20..25, 30..35]);
        // Range sizes: 5 + 5 + 5 = 15 total positions
        // Range 0: positions 10-14 (counts 0-4)
        // Range 1: positions 20-24 (counts 5-9)
        // Range 2: positions 30-34 (counts 10-14)

        // Split at beginning of first range
        let (left, right) = rl.split_at_position_count(0);
        assert!(left.is_empty());
        assert_eq!(right.count_positions(), 15);
        assert_eq!(right.bounds(), 10..35);

        // Split in middle of first range
        let (left, right) = rl.split_at_position_count(3);
        assert_eq!(left.count_positions(), 3);
        assert_eq!(right.count_positions(), 12);
        assert_eq!(left.bounds(), 10..13);
        assert_eq!(right.bounds(), 13..35);

        // Split at beginning of second range
        let (left, right) = rl.split_at_position_count(5);
        assert_eq!(left.count_positions(), 5);
        assert_eq!(right.count_positions(), 10);
        assert_eq!(left.bounds(), 10..15);
        assert_eq!(right.bounds(), 20..35);

        // Split in middle of second range
        let (left, right) = rl.split_at_position_count(7);
        assert_eq!(left.count_positions(), 7);
        assert_eq!(right.count_positions(), 8);
        assert_eq!(left.bounds(), 10..22);
        assert_eq!(right.bounds(), 22..35);

        // Split at beginning of third range
        let (left, right) = rl.split_at_position_count(10);
        assert_eq!(left.count_positions(), 10);
        assert_eq!(right.count_positions(), 5);
        assert_eq!(left.bounds(), 10..25);
        assert_eq!(right.bounds(), 30..35);

        // Split in middle of third range
        let (left, right) = rl.split_at_position_count(12);
        assert_eq!(left.count_positions(), 12);
        assert_eq!(right.count_positions(), 3);
        assert_eq!(left.bounds(), 10..32);
        assert_eq!(right.bounds(), 32..35);

        // Split at end
        let (left, right) = rl.split_at_position_count(15);
        assert_eq!(left.count_positions(), 15);
        assert!(right.is_empty());
        assert_eq!(left.bounds(), 10..35);

        // Split beyond end
        let (left, right) = rl.split_at_position_count(20);
        assert_eq!(left.count_positions(), 15);
        assert!(right.is_empty());
        assert_eq!(left.bounds(), 10..35);
    }

    #[test]
    fn test_split_at_position_count_with_gaps() {
        let rl = rl(&[1..3, 10..12, 100..103]);
        // Range 0: positions 1,2 (counts 0,1) - 2 positions
        // Range 1: positions 10,11 (counts 2,3) - 2 positions
        // Range 2: positions 100,101,102 (counts 4,5,6) - 3 positions
        // Total: 7 positions

        let (left, right) = rl.split_at_position_count(0);
        assert!(left.is_empty());
        assert_eq!(right.count_positions(), 7);

        let (left, right) = rl.split_at_position_count(1);
        assert_eq!(left.count_positions(), 1);
        assert_eq!(right.count_positions(), 6);
        assert_eq!(left.bounds(), 1..2);
        assert_eq!(right.bounds(), 2..103);

        let (left, right) = rl.split_at_position_count(2);
        assert_eq!(left.count_positions(), 2);
        assert_eq!(right.count_positions(), 5);
        assert_eq!(left.bounds(), 1..3);
        assert_eq!(right.bounds(), 10..103);

        let (left, right) = rl.split_at_position_count(4);
        assert_eq!(left.count_positions(), 4);
        assert_eq!(right.count_positions(), 3);
        assert_eq!(left.bounds(), 1..12);
        assert_eq!(right.bounds(), 100..103);

        let (left, right) = rl.split_at_position_count(6);
        assert_eq!(left.count_positions(), 6);
        assert_eq!(right.count_positions(), 1);
        assert_eq!(left.bounds(), 1..102);
        assert_eq!(right.bounds(), 102..103);

        let (left, right) = rl.split_at_position_count(7);
        assert_eq!(left.count_positions(), 7);
        assert!(right.is_empty());
        assert_eq!(left.bounds(), 1..103);
    }

    #[test]
    fn test_split_at_position_count_trimmed_ranges() {
        let base = rl(&[10..20, 30..40]);
        let trimmed = SharedRangeList::new_trimmed(base.inner().clone(), 12, 38);
        // First range: 12..20 (8 positions), Second range: 30..38 (8 positions)
        // Total: 16 positions
        assert_eq!(trimmed.count_positions(), 16);

        let (left, right) = trimmed.split_at_position_count(0);
        assert!(left.is_empty());
        assert_eq!(right.count_positions(), 16);
        assert_eq!(right.bounds(), 12..38);

        let (left, right) = trimmed.split_at_position_count(4);
        assert_eq!(left.count_positions(), 4);
        assert_eq!(right.count_positions(), 12);
        assert_eq!(left.bounds(), 12..16);
        assert_eq!(right.bounds(), 16..38);

        let (left, right) = trimmed.split_at_position_count(8);
        assert_eq!(left.count_positions(), 8);
        assert_eq!(right.count_positions(), 8);
        assert_eq!(left.bounds(), 12..20);
        assert_eq!(right.bounds(), 30..38);

        let (left, right) = trimmed.split_at_position_count(12);
        assert_eq!(left.count_positions(), 12);
        assert_eq!(right.count_positions(), 4);
        assert_eq!(left.bounds(), 12..34);
        assert_eq!(right.bounds(), 34..38);

        let (left, right) = trimmed.split_at_position_count(16);
        assert_eq!(left.count_positions(), 16);
        assert!(right.is_empty());
        assert_eq!(left.bounds(), 12..38);
    }

    #[test]
    fn test_split_at_position_count_consistency_with_position_at_count() {
        let test_cases = vec![
            vec![10..15],
            vec![10..15, 20..25, 30..35],
            vec![1..3, 5..7, 10..12, 20..22],
            vec![0..1, 2..3, 4..5],
            vec![5..6, 10..11, 15..16],
        ];

        for ranges in test_cases {
            let rl = rl(&ranges);
            let total_count = rl.count_positions();

            for count in 0..=total_count {
                let (left, right) = rl.split_at_position_count(count);

                // Left should have exactly 'count' positions
                assert_eq!(
                    left.count_positions(),
                    count,
                    "Left count mismatch for ranges {ranges:?} at count {count}"
                );

                // Left + right should equal original
                assert_eq!(
                    left.count_positions() + right.count_positions(),
                    total_count,
                    "Total count mismatch for ranges {ranges:?} at count {count}"
                );

                // If count < total_count, the split position should match position_at_count
                if count < total_count {
                    let (_, expected_position) = rl.position_at_count(count).unwrap();
                    if !right.is_empty() {
                        assert_eq!(
                            right.bounds().start,
                            expected_position,
                            "Right start mismatch for ranges {ranges:?} at count {count}"
                        );
                    }
                }

                // Edge cases
                if count == 0 {
                    assert!(left.is_empty(), "Left should be empty at count 0");
                    if !rl.is_empty() {
                        assert_eq!(
                            right.bounds(),
                            rl.bounds(),
                            "Right should equal original at count 0"
                        );
                    }
                } else if count == total_count {
                    assert!(right.is_empty(), "Right should be empty at count == total");
                    if !rl.is_empty() {
                        assert_eq!(
                            left.bounds(),
                            rl.bounds(),
                            "Left should equal original at count == total"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_split_at_position_count_large_ranges() {
        let mut ranges = Vec::new();
        for i in 0..10 {
            ranges.push((i * 100)..(i * 100 + 50)); // Each range has 50 positions
        }
        let rl = rl(&ranges); // Total: 500 positions

        // Test various split points
        let (left, right) = rl.split_at_position_count(0);
        assert!(left.is_empty());
        assert_eq!(right.count_positions(), 500);

        let (left, right) = rl.split_at_position_count(25); // Middle of first range
        assert_eq!(left.count_positions(), 25);
        assert_eq!(right.count_positions(), 475);
        assert_eq!(left.bounds(), 0..25);
        assert_eq!(right.bounds(), 25..950);

        let (left, right) = rl.split_at_position_count(50); // End of first range
        assert_eq!(left.count_positions(), 50);
        assert_eq!(right.count_positions(), 450);
        assert_eq!(left.bounds(), 0..50); // Includes gap
        assert_eq!(right.bounds(), 100..950);

        let (left, right) = rl.split_at_position_count(250); // Middle overall
        assert_eq!(left.count_positions(), 250);
        assert_eq!(right.count_positions(), 250);

        let (left, right) = rl.split_at_position_count(500); // End
        assert_eq!(left.count_positions(), 500);
        assert!(right.is_empty());
        assert_eq!(left.bounds(), 0..950);
    }
}
