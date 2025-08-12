//! An immutable, non-owning view over a sorted, non-overlapping list of half-open ranges.

use std::{
    convert::identity,
    ops::{Range, RangeBounds},
};

/// An immutable, non-owning view over a sorted, non-overlapping list of half-open ranges.
///
/// `RangeListSlice<'a, T>` mirrors [`SharedRangeList`](crate::shared_range_list::SharedRangeList),
/// but borrows a slice (`&'a [Range<T>]`) instead of owning the underlying storage. It is
/// therefore cheap to clone and to slice, and is ideal when you already have a slice of
/// ranges and want to work with logical sub-views without allocating.
///
/// Key characteristics:
/// - Borrowed view: holds a reference to `&'a [Range<T>]` and never takes ownership.
/// - Cheap clones and slices: cloning and slicing are O(1) operations that create new views.
/// - Trimming: the first and last logical ranges can be “trimmed” to represent sub-views
///   whose start/end do not necessarily align with physical range boundaries.
/// - Half-open semantics: each `Range<T>` is interpreted as `[start, end)` (end-exclusive).
///
/// Invariants and expectations:
/// - The underlying slice is sorted by `start` and non-overlapping:
///   `inner[i].end <= inner[i + 1].start` for all valid `i`.
/// - `T` must at least be `PartialOrd` for search operations. Methods that count or compute
///   positions additionally require `T: Default + Clone + Add<Output = T> + Sub<Output = T>`.
/// - This type is a logical view: `self.first` and `self.last` may differ from the first and
///   last physical ranges in `inner` to reflect trimming.
///
/// When to use which type:
/// - Use [`SharedRangeList`](crate::shared_range_list::SharedRangeList) when you need an owned,
///   shareable container for ranges (e.g., to pass around across threads or store long-term).
/// - Use `RangeListSlice` when you want a lightweight, borrowed view over an existing slice of
///   ranges without additional allocations.
///
/// Complexity overview:
/// - `search_position` / `contains_position`: O(log n)
/// - `slice`, `clamp`, `split_at_position`: O(log n) to locate boundaries + O(1) to build views
/// - Iteration: O(n)
#[derive(Clone)]
pub struct RangeListSlice<'a, T> {
    /// Underlying slice containing the ranges.
    /// These ranges are guaranteed to be sorted and non-overlapping.
    /// `self.first` and `self.last` override `self.inner[0]` and
    /// `self.inner[last_idx]`.
    inner: &'a [Range<T>],

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

impl<'a, T> RangeListSlice<'a, T> {
    /// Constructs a trimmed view from explicit parts.
    ///
    /// This low-level constructor builds a `RangeListSlice` that borrows `inner`
    /// and uses `first`/`last` to override the logical bounds of the first and
    /// last ranges, respectively. No validation is performed here; the caller
    /// must ensure all invariants hold.
    ///
    /// Caller invariants:
    /// - `inner` is sorted by `start` and non-overlapping.
    /// - If `inner` is non-empty:
    ///   - `first.start` is in [`inner[0].start`, `inner[0].end`].
    ///   - `last.end` is in [`inner[last_idx].start`, `inner[last_idx].end`].
    ///   - If `inner.len() > 1`:
    ///     - `first.end == inner[0].end`
    ///     - `last.start == inner[last_idx].start`
    /// - If `inner` is empty, `first`/`last` are placeholders and not used.
    ///
    /// Notes:
    /// - Violating these invariants can cause later operations (e.g. search, split,
    ///   clamp) to panic or behave incorrectly.
    /// - Prefer higher-level constructors like [`RangeListSlice::new`] or
    ///   [`RangeListSlice::from_slice`], and use operations like [`slice`]
    ///   and [`clamp`] to obtain trimmed views.
    #[inline]
    pub(crate) fn from_parts(
        first: Range<T>,
        inner: &'a [Range<T>],
        last: Range<T>,
    ) -> RangeListSlice<'a, T> {
        RangeListSlice { inner, first, last }
    }
}

impl<'a, T: Clone> RangeListSlice<'a, T> {
    /// Constructs a `RangeSlice` containing exactly one range by borrowing it.
    pub fn from_one(range: &'a Range<T>) -> Self {
        let inner = std::slice::from_ref(range);
        RangeListSlice {
            inner,
            first: range.clone(),
            last: range.clone(),
        }
    }

    /// Returns the tight logical bounds of the entire list as a single range.
    pub fn bounds(&self) -> Range<T> {
        self.first.start.clone()..self.last.end.clone()
    }

    /// Returns a vector containing all logical ranges in this list.
    pub fn to_vec(&self) -> Vec<Range<T>> {
        self.iter().cloned().collect()
    }
}

impl<'a, T: Default + Clone> From<&'a [Range<T>]> for RangeListSlice<'a, T> {
    fn from(slice: &'a [Range<T>]) -> Self {
        RangeListSlice::from_slice(slice)
    }
}

impl<'a, T: Default + Clone> RangeListSlice<'a, T> {
    /// Creates a new, empty `RangeSlice`.
    pub fn empty() -> Self {
        RangeListSlice {
            inner: &[],
            first: T::default()..T::default(),
            last: T::default()..T::default(),
        }
    }

    /// Constructs a new `RangeSlice<T>` from a slice of ranges.
    /// Covers the full extent of the provided ranges.
    pub fn new(inner: &'a [Range<T>]) -> Self {
        if inner.is_empty() {
            Self::empty()
        } else {
            let first = inner[0].clone();
            let last = inner.last().unwrap().clone();
            Self { inner, first, last }
        }
    }

    /// Constructs a `RangeSlice` from a slice of ranges.
    pub fn from_slice(slice: &'a [Range<T>]) -> Self {
        Self::new(slice)
    }
}

impl<'a, T: PartialOrd> RangeListSlice<'a, T> {
    /// Binary search for the range containing `pos` within the logical view.
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

    /// Returns `true` if `pos` is within any logical range.
    pub fn contains_position(&self, pos: T) -> bool {
        self.search_position(pos).is_ok()
    }

    /// Total number of positions covered by the logical ranges.
    pub fn count_positions(&self) -> T
    where
        T: Default + std::ops::Sub<Output = T> + std::ops::Add<Output = T> + Clone,
    {
        self.iter()
            .map(|range| range.end.clone() - range.start.clone())
            .fold(T::default(), |acc, size| acc + size)
    }

    /// Inverse of `count_positions`: find the (range_index, position) at `count`.
    pub fn position_at_count(&self, count: T) -> Option<(usize, T)>
    where
        T: Default + std::ops::Sub<Output = T> + std::ops::Add<Output = T> + Clone,
    {
        let mut accumulated = T::default();

        for (index, range) in self.iter().enumerate() {
            let range_size = range.end.clone() - range.start.clone();
            let next_accumulated = accumulated.clone() + range_size.clone();

            if count < next_accumulated {
                let offset = count - accumulated;
                let position = range.start.clone() + offset;
                return Some((index, position));
            }

            accumulated = next_accumulated;
        }

        None
    }
}

impl<'a, T: Default + Clone + PartialOrd> RangeListSlice<'a, T> {
    /// Split the list at position `pos` into (left, right).
    ///
    /// The first returned `RangeListSlice` contains all portions of ranges strictly
    /// before `pos`.
    /// The second contains all portions of ranges starting at or after `pos`.
    ///
    pub fn split_at_position(&self, pos: T) -> (Self, Self) {
        self.split_at_position_with_range_hint(pos, None)
    }

    /// Split at the position corresponding to `count` positions.
    pub fn split_at_position_count(&self, count: T) -> (Self, Self)
    where
        T: std::ops::Sub<Output = T> + std::ops::Add<Output = T>,
    {
        if count == T::default() {
            return (Self::empty(), self.clone());
        }

        match self.position_at_count(count.clone()) {
            Some((range_idx, position)) => {
                self.split_at_position_with_range_hint(position, Some(range_idx))
            }
            None => (self.clone(), Self::empty()),
        }
    }

    /// Return a view clamped to `bounds`.
    pub fn clamp(&self, bounds: Range<T>) -> Self {
        if self.is_empty() || bounds.start >= bounds.end {
            return Self::empty();
        }

        if bounds.start <= self.first.start && self.last.end <= bounds.end {
            return self.clone();
        }

        // Index of the first range that has positions >= bounds.start
        let start_idx = self
            .search_position(bounds.start.clone())
            .unwrap_or_else(identity);
        if start_idx >= self.len() {
            return Self::empty();
        }

        // Index of the first range where all positions are >= bounds.end
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

    fn new_trimmed(inner: &'a [Range<T>], start: T, end: T) -> Self {
        if inner.is_empty() {
            Self::empty()
        } else {
            let (first, last) = trim_edges(inner, start, end);
            Self { inner, first, last }
        }
    }

    /// Split with an optional hint for the range index.
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
            _ => self.search_position(pos.clone()).unwrap_or_else(identity),
        };

        let range = self.get(idx).expect("range");
        if pos <= range.start {
            let first = self.slice(0..idx);
            let second = self.slice(idx..);
            (first, second)
        } else {
            let first = Self::new_trimmed(self.slice(0..idx + 1).inner, bounds.start, pos.clone());
            let second = Self::new_trimmed(self.slice(idx..).inner, pos, bounds.end);
            (first, second)
        }
    }

    /// Check if `range_idx` is a valid hint for splitting at `pos`.
    pub fn range_covers_or_follows_position(&self, range_idx: usize, pos: T) -> bool {
        assert!(range_idx <= self.len());
        let bounds = self.bounds();
        if range_idx == self.len() {
            return self.is_empty() || pos >= bounds.end;
        }
        let range = self.get(range_idx).expect("range");
        if range.contains(&pos) {
            true
        } else if range_idx != 0 {
            let prev_range = self.get(range_idx).expect("prev_range");
            pos < range.start && pos >= prev_range.end
        } else {
            pos < range.start
        }
    }
}

impl<'a, T> RangeListSlice<'a, T> {
    /// Number of logical ranges.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Is the list empty?
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Get logical range at `index` (with trimming for first/last).
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

    /// Access underlying slice of physical ranges.
    pub fn inner(&self) -> &'a [Range<T>] {
        self.inner
    }

    /// Iterate logical ranges.
    pub fn iter(&self) -> RangeListSliceIter<'_, 'a, T> {
        RangeListSliceIter {
            list: self,
            index: 0,
        }
    }
}

impl<'a, T: Default + Clone> RangeListSlice<'a, T> {
    /// Sub-slice by indices.
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

        let new_inner = &self.inner[start..end];
        let new_first = self.get(start).expect("start").clone();
        let new_last = self.get(end - 1).expect("end").clone();
        Self {
            inner: new_inner,
            first: new_first,
            last: new_last,
        }
    }
}

impl<T> std::ops::Index<usize> for RangeListSlice<'_, T> {
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

impl<T: std::fmt::Debug> std::fmt::Debug for RangeListSlice<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries((0..self.len()).map(|i| self.get(i).unwrap()))
            .finish()
    }
}

impl<'a, T: Default + Clone> Default for RangeListSlice<'a, T> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<T: PartialEq> PartialEq for RangeListSlice<'_, T> {
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

impl<T: Eq> Eq for RangeListSlice<'_, T> {}

/// Iterator over logical ranges in a `RangeSlice`, applying trimming for first/last.
#[derive(Clone)]
pub struct RangeListSliceIter<'s, 'a, T> {
    list: &'s RangeListSlice<'a, T>,
    index: usize,
}

impl<'s, 'a, T> Iterator for RangeListSliceIter<'s, 'a, T> {
    type Item = &'s Range<T>;

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

impl<T> ExactSizeIterator for RangeListSliceIter<'_, '_, T> {}

/// Owned iterator over logical ranges in a `RangeSlice` (yields owned `Range<T>`).
#[derive(Clone)]
pub struct RangeListSliceIntoIter<'a, T> {
    list: RangeListSlice<'a, T>,
    index: usize,
}

impl<'a, T: Clone> Iterator for RangeListSliceIntoIter<'a, T> {
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

impl<'a, T: Clone> IntoIterator for RangeListSlice<'a, T> {
    type Item = Range<T>;
    type IntoIter = RangeListSliceIntoIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        RangeListSliceIntoIter {
            list: self,
            index: 0,
        }
    }
}

impl<'s, 'a, T> IntoIterator for &'s RangeListSlice<'a, T> {
    type Item = &'s Range<T>;
    type IntoIter = RangeListSliceIter<'s, 'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[inline]
pub(crate) fn trim_edges<T: Clone + PartialOrd>(
    inner: &[Range<T>],
    start: T,
    end: T,
) -> (Range<T>, Range<T>) {
    assert!(!inner.is_empty());

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
    (first, last)
}
