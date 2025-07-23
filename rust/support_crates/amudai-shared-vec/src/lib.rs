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
pub struct SharedVec<T> {
    inner: Arc<Vec<T>>,
    offset: usize,
    len: usize,
}

impl<T> SharedVec<T> {
    /// Creates a new `SharedVec` from a single element.
    pub fn from_elem(elem: T) -> Self {
        SharedVec::from_vec(vec![elem])
    }

    /// Creates a new `SharedVec` from a `Vec<T>`, owning the data.
    pub fn from_vec(vec: Vec<T>) -> Self {
        let len = vec.len();
        SharedVec {
            inner: Arc::new(vec),
            offset: 0,
            len,
        }
    }

    /// Creates a new `SharedVec` from an `Arc<Vec<T>>`, viewing the entire vector.
    pub fn from_arc_vec(arc: Arc<Vec<T>>) -> Self {
        let len = arc.len();
        SharedVec {
            inner: arc,
            offset: 0,
            len,
        }
    }

    /// Creates a new `SharedVec` from a slice by cloning the data.
    pub fn from_slice(slice: &[T]) -> Self
    where
        T: Clone,
    {
        SharedVec::from_vec(slice.to_vec())
    }

    /// Returns an empty `SharedVec`.
    pub fn empty() -> Self {
        SharedVec {
            inner: Arc::new(Vec::new()),
            offset: 0,
            len: 0,
        }
    }

    /// Returns the length of the slice.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the slice is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns a reference to the element at the given index, or `None` if out of bounds.
    #[inline]
    pub fn get(&self, index: usize) -> Option<&T> {
        if index < self.len {
            Some(&self.inner[self.offset + index])
        } else {
            None
        }
    }

    /// Returns a reference to the element at the given index, panics if out of bounds.
    #[inline]
    pub fn at(&self, index: usize) -> &T {
        &self.inner[self.offset + index]
    }

    /// Returns a reference to the first element, or `None` if empty.
    #[inline]
    pub fn first(&self) -> Option<&T> {
        self.get(0)
    }

    /// Returns a reference to the last element, or `None` if empty.
    #[inline]
    pub fn last(&self) -> Option<&T> {
        if self.len == 0 {
            None
        } else {
            self.get(self.len - 1)
        }
    }

    /// Returns the slice as a `&[T]`.
    #[inline]
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
        SharedVec {
            inner: self.inner.clone(),
            offset: self.offset + start,
            len: end - start,
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

impl<T> Deref for SharedVec<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> std::ops::Index<usize> for SharedVec<T> {
    type Output = T;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        self.at(index)
    }
}

impl<T> AsRef<[T]> for SharedVec<T> {
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> Borrow<[T]> for SharedVec<T> {
    fn borrow(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T: fmt::Debug> fmt::Debug for SharedVec<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SharedVec").field(&self.as_slice()).finish()
    }
}

impl<T: PartialEq> PartialEq for SharedVec<T> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}
impl<T: Eq> Eq for SharedVec<T> {}

impl<T: PartialOrd> PartialOrd for SharedVec<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_slice().partial_cmp(other.as_slice())
    }
}
impl<T: Ord> Ord for SharedVec<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl<T: Hash> Hash for SharedVec<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state)
    }
}

impl<T> Default for SharedVec<T> {
    fn default() -> Self {
        SharedVec::empty()
    }
}

impl<'a, T> IntoIterator for &'a SharedVec<T> {
    type Item = &'a T;

    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

impl<T> From<Vec<T>> for SharedVec<T> {
    fn from(vec: Vec<T>) -> Self {
        SharedVec::from_vec(vec)
    }
}

impl<T> From<Arc<Vec<T>>> for SharedVec<T> {
    fn from(arc: Arc<Vec<T>>) -> Self {
        SharedVec::from_arc_vec(arc)
    }
}

impl<T: Clone> From<&[T]> for SharedVec<T> {
    fn from(slice: &[T]) -> Self {
        SharedVec::from_slice(slice)
    }
}

impl<T> From<Box<[T]>> for SharedVec<T> {
    fn from(b: Box<[T]>) -> Self {
        SharedVec::from_vec(b.into_vec())
    }
}

/// SharedVec iterator for by-value iteration.
#[derive(Clone)]
pub struct SharedVecIntoIter<T> {
    inner: std::sync::Arc<Vec<T>>,
    offset: usize,
    len: usize,
    pos: usize,
}

impl<T: Clone> Iterator for SharedVecIntoIter<T> {
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

impl<T: Clone> ExactSizeIterator for SharedVecIntoIter<T> {}

impl<T: Clone> IntoIterator for SharedVec<T> {
    type Item = T;
    type IntoIter = SharedVecIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        SharedVecIntoIter {
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
        let shared = SharedVec::from_vec(v);
        assert_eq!(shared.len(), 5);
        assert_eq!(shared[2], 3);
        let sub = shared.slice(1..4);
        assert_eq!(&*sub, &[2, 3, 4]);
    }

    #[test]
    fn empty_and_default() {
        let empty = SharedVec::<i32>::empty();
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
        assert_eq!(empty, SharedVec::default());
    }

    #[test]
    fn into_iter_by_value_clones_elements() {
        let v = vec![10, 20, 30];
        let shared = SharedVec::from_vec(v.clone());
        let collected: Vec<_> = shared.clone().into_iter().collect();
        assert_eq!(collected, v);
    }

    #[test]
    fn from_elem_creates_singleton_slice() {
        let x = 42;
        let shared = SharedVec::from_elem(x);
        assert_eq!(shared.len(), 1);
        assert_eq!(shared[0], x);
    }
}
