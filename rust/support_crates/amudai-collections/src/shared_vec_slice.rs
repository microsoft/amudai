//! An immutable, cheaply cloneable, and sliceable shared vector slice.
//!
//! This type provides a shared, immutable view into a vector, using `Arc<Vec<T>>`
//! for shared ownership. Slicing and cloning are zero-cost operations.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::{Deref, RangeBounds};
use std::sync::Arc;

/// An immutable, cheaply cloneable, and sliceable shared vector slice.
///
/// Internally uses `Arc<Vec<T>>` for shared ownership and stores an offset and length
/// to represent a view into the vector.
#[derive(Clone)]
pub struct SharedVecSlice<T> {
    inner: Arc<Vec<T>>,
    offset: usize,
    len: usize,
}

impl<T> SharedVecSlice<T> {
    /// Creates a new `SharedVecSlice` from a single element.
    pub fn from_elem(elem: T) -> Self {
        SharedVecSlice::from_vec(vec![elem])
    }

    /// Creates a new `SharedVecSlice` from a `Vec<T>`, owning the data.
    pub fn from_vec(vec: Vec<T>) -> Self {
        let len = vec.len();
        SharedVecSlice {
            inner: Arc::new(vec),
            offset: 0,
            len,
        }
    }

    /// Creates a new `SharedVecSlice` from an `Arc<Vec<T>>`, viewing the entire vector.
    pub fn from_arc_vec(arc: Arc<Vec<T>>) -> Self {
        let len = arc.len();
        SharedVecSlice {
            inner: arc,
            offset: 0,
            len,
        }
    }

    /// Creates a new `SharedVecSlice` from a slice by cloning the data.
    pub fn from_slice(slice: &[T]) -> Self
    where
        T: Clone,
    {
        SharedVecSlice::from_vec(slice.to_vec())
    }

    /// Returns an empty `SharedVecSlice`.
    pub fn empty() -> Self {
        SharedVecSlice {
            inner: Arc::new(Vec::new()),
            offset: 0,
            len: 0,
        }
    }

    /// Returns the length of the slice.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the slice is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns a reference to the element at the given index, or `None` if out of bounds.
    pub fn get(&self, index: usize) -> Option<&T> {
        if index < self.len {
            Some(&self.inner[self.offset + index])
        } else {
            None
        }
    }

    /// Returns a reference to the first element, or `None` if empty.
    pub fn first(&self) -> Option<&T> {
        self.get(0)
    }

    /// Returns a reference to the last element, or `None` if empty.
    pub fn last(&self) -> Option<&T> {
        if self.len == 0 {
            None
        } else {
            self.get(self.len - 1)
        }
    }

    /// Returns the slice as a `&[T]`.
    pub fn as_slice(&self) -> &[T] {
        &self.inner[self.offset..self.offset + self.len]
    }

    /// Returns an iterator over the slice.
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.as_slice().iter()
    }

    /// Returns a sub-slice of this slice, using a range.
    ///
    /// # Panics
    ///
    /// Panics if the range is out of bounds.
    pub fn slice<R>(&self, range: R) -> Self
    where
        R: RangeBounds<usize>,
    {
        use std::ops::Bound::*;
        let start = match range.start_bound() {
            Included(&n) => n,
            Excluded(&n) => n + 1,
            Unbounded => 0,
        };
        let end = match range.end_bound() {
            Included(&n) => n + 1,
            Excluded(&n) => n,
            Unbounded => self.len,
        };
        assert!(start <= end && end <= self.len, "slice out of bounds");
        SharedVecSlice {
            inner: self.inner.clone(),
            offset: self.offset + start,
            len: end - start,
        }
    }

    /// Returns a sub-slice of this slice, starting at `offset` and of length `len`.
    ///
    /// # Panics
    ///
    /// Panics if the range is out of bounds.
    pub fn subslice(&self, offset: usize, len: usize) -> Self {
        assert!(
            offset <= self.len && offset + len <= self.len,
            "subslice out of bounds"
        );
        SharedVecSlice {
            inner: self.inner.clone(),
            offset: self.offset + offset,
            len,
        }
    }

    /// Converts to a `Vec<T>` by cloning the data.
    pub fn to_vec(&self) -> Vec<T>
    where
        T: Clone,
    {
        self.as_slice().to_vec()
    }

    /// Returns the underlying `Arc<Vec<T>>`.
    pub fn inner(&self) -> &Arc<Vec<T>> {
        &self.inner
    }

    /// Returns the underlying `Arc<Vec<T>>`.
    pub fn into_inner(self) -> Arc<Vec<T>> {
        self.inner
    }
}

impl<T> Deref for SharedVecSlice<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> AsRef<[T]> for SharedVecSlice<T> {
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> Borrow<[T]> for SharedVecSlice<T> {
    fn borrow(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T: fmt::Debug> fmt::Debug for SharedVecSlice<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SharedVecSlice")
            .field(&self.as_slice())
            .finish()
    }
}

impl<T: PartialEq> PartialEq for SharedVecSlice<T> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}
impl<T: Eq> Eq for SharedVecSlice<T> {}

impl<T: PartialOrd> PartialOrd for SharedVecSlice<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_slice().partial_cmp(other.as_slice())
    }
}
impl<T: Ord> Ord for SharedVecSlice<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl<T: Hash> Hash for SharedVecSlice<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state)
    }
}

impl<T> Default for SharedVecSlice<T> {
    fn default() -> Self {
        SharedVecSlice::empty()
    }
}

impl<'a, T> IntoIterator for &'a SharedVecSlice<T> {
    type Item = &'a T;

    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

impl<T> From<Vec<T>> for SharedVecSlice<T> {
    fn from(vec: Vec<T>) -> Self {
        SharedVecSlice::from_vec(vec)
    }
}

impl<T> From<Arc<Vec<T>>> for SharedVecSlice<T> {
    fn from(arc: Arc<Vec<T>>) -> Self {
        SharedVecSlice::from_arc_vec(arc)
    }
}

impl<T: Clone> From<&[T]> for SharedVecSlice<T> {
    fn from(slice: &[T]) -> Self {
        SharedVecSlice::from_slice(slice)
    }
}

impl<T> From<Box<[T]>> for SharedVecSlice<T> {
    fn from(b: Box<[T]>) -> Self {
        SharedVecSlice::from_vec(b.into_vec())
    }
}

/// SharedVecSlice iterator for by-value iteration.
#[derive(Clone)]
pub struct SharedVecSliceIntoIter<T> {
    inner: std::sync::Arc<Vec<T>>,
    offset: usize,
    len: usize,
    pos: usize,
}

impl<T: Clone> Iterator for SharedVecSliceIntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        if self.pos < self.len {
            let item = self.inner[self.offset + self.pos].clone();
            self.pos += 1;
            Some(item)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len - self.pos;
        (remaining, Some(remaining))
    }
}

impl<T: Clone> ExactSizeIterator for SharedVecSliceIntoIter<T> {}

impl<T: Clone> IntoIterator for SharedVecSlice<T> {
    type Item = T;
    type IntoIter = SharedVecSliceIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        SharedVecSliceIntoIter {
            inner: self.inner,
            offset: self.offset,
            len: self.len,
            pos: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_usage() {
        let v = vec![1, 2, 3, 4, 5];
        let shared = SharedVecSlice::from_vec(v);
        assert_eq!(shared.len(), 5);
        assert_eq!(shared[2], 3);
        let sub = shared.slice(1..4);
        assert_eq!(&*sub, &[2, 3, 4]);
        let sub2 = sub.subslice(1, 2);
        assert_eq!(&*sub2, &[3, 4]);
        assert!(sub2.contains(&3));
        assert!(sub2.starts_with(&[3]));
        assert!(sub2.ends_with(&[4]));
    }

    #[test]
    fn empty_and_default() {
        let empty = SharedVecSlice::<i32>::empty();
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
        assert_eq!(empty, SharedVecSlice::default());
    }

    #[test]
    fn into_iter_by_value_clones_elements() {
        let v = vec![10, 20, 30];
        let shared = SharedVecSlice::from_vec(v.clone());
        let collected: Vec<_> = shared.clone().into_iter().collect();
        assert_eq!(collected, v);
    }

    #[test]
    fn from_elem_creates_singleton_slice() {
        let x = 42;
        let shared = SharedVecSlice::from_elem(x);
        assert_eq!(shared.len(), 1);
        assert_eq!(shared[0], x);
    }
}
