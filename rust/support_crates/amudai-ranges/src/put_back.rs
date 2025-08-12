//! A single-item "put-back" iterator adapter.
//!
//! `PutBack<I>` wraps an iterator `I` and allows pushing exactly one item
//! back so that it will be returned on the next call to `next()`.

/// Iterator adapter that supports pushing a single item back onto the front.
pub struct PutBack<I: Iterator> {
    /// The underlying iterator being wrapped.
    iter: I,
    /// A single buffered item to be returned first on the next call to `next()`.
    next: Option<I::Item>,
}

impl<I: Iterator> PutBack<I> {
    /// Creates a new `PutBack` around the given iterator.
    pub fn new(iter: I) -> Self {
        PutBack { iter, next: None }
    }

    /// Pushes an item back so it is returned on the next call to `next()`.
    ///
    /// Panics:
    /// - In debug builds, this panics if an item is already buffered.
    ///
    /// Behavior in release builds:
    /// - If an item is already buffered, it is silently overwritten.
    #[inline]
    pub fn put_back(&mut self, item: I::Item) {
        debug_assert!(self.next.is_none());
        self.next = Some(item);
    }

    pub fn peek(&mut self) -> Option<&I::Item> {
        if self.next.is_none() {
            self.next = self.iter.next();
        }
        self.next.as_ref()
    }
}

impl<I: Iterator> Iterator for PutBack<I> {
    type Item = I::Item;

    /// Returns the next item, preferring a buffered item if present.
    ///
    /// If an item has been pushed back via [`PutBack::put_back`], it is
    /// returned first; otherwise, this delegates to the underlying iterator.
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.next.is_some() {
            self.next.take()
        } else {
            self.iter.next()
        }
    }

    /// Provides a size hint, accounting for a buffered item when present.
    ///
    /// - The lower bound is the underlying iterator’s lower bound, plus one if
    ///   an item is buffered.
    /// - The upper bound is the underlying iterator’s upper bound, plus one if
    ///   an item is buffered and the upper bound is known.
    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lower, upper) = self.iter.size_hint();
        if self.next.is_some() {
            (
                lower.saturating_add(1),
                upper.and_then(|n| n.checked_add(1)),
            )
        } else {
            (lower, upper)
        }
    }
}
