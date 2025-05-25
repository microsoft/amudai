use std::ops::Range;

use amudai_bytes::Bytes;

use crate::{ReadAt, StorageProfile};

/// A `ReadAt` adapter that restricts reads to a specified range of the underlying reader.
///
/// `SlicedReadAt` allows you to treat a portion of a larger `ReadAt` source as a separate,
/// independent `ReadAt` instance.  All read operations are **relative to the slice's starting
/// position**.
///
/// For example, if the underlying reader has a size of 100, and a `SlicedReadAt` is created
/// with a range of `10..20`, then:
///
/// *   `size()` will return `10` (20 - 10).
/// *   `read_at(0..5)` will read bytes 10-15 from the underlying reader.
/// *   `read_at(5..10)` will read bytes 15-20 from the underlying reader.
/// *   `read_at(0..15)` will read bytes 10-20 from the underlying reader
///     (clamped to the slice size).
pub struct SlicedReadAt<R> {
    inner: R,
    range: Range<u64>,
}

impl<R> SlicedReadAt<R> {
    /// Creates a new `SlicedReadAt` adapter.
    ///
    /// # Panics
    ///
    /// Panics if `range.start > range.end`.
    pub fn new(inner: R, range: Range<u64>) -> Self {
        assert!(range.start <= range.end);
        Self { inner, range }
    }

    /// Returns the size of the slice.
    pub fn slice_size(&self) -> u64 {
        self.range.end - self.range.start
    }

    /// Returns the range of the slice within the underlying reader.
    pub fn slice_range(&self) -> Range<u64> {
        self.range.clone()
    }

    /// Returns a reference to the underlying reader.
    pub fn inner(&self) -> &R {
        &self.inner
    }

    /// Consumes the `SlicedReadAt`, returning the underlying reader.
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: Clone> SlicedReadAt<R> {
    /// Creates a new `SlicedReadAt` from the current instance, using the specified `range`
    /// relative to the starting position of this slice.
    pub fn slice(&self, range: Range<u64>) -> SlicedReadAt<R> {
        let this_size = self.slice_size();
        assert!(range.start <= this_size);
        assert!(range.end <= this_size);
        SlicedReadAt {
            inner: self.inner.clone(),
            range: self.range.start + range.start..self.range.start + range.end,
        }
    }
}

impl<R: ReadAt> SlicedReadAt<R> {
    /// Reads the entire range of bytes represented by this`SlicedReadAt`
    /// from the underlying reader.
    pub fn read_all(&self) -> std::io::Result<Bytes> {
        self.inner.read_at(self.range.clone())
    }
}

impl<R: ReadAt> ReadAt for SlicedReadAt<R> {
    fn size(&self) -> std::io::Result<u64> {
        Ok(self.slice_size())
    }

    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes> {
        let slice_size = self.slice_size();
        let start = range.start;
        let end = std::cmp::min(slice_size, range.end);
        if start >= end {
            return Ok(Bytes::new());
        }

        let inner_start = self.range.start.saturating_add(start);
        let inner_end = self.range.start.saturating_add(end);
        self.inner.read_at(inner_start..inner_end)
    }

    fn storage_profile(&self) -> StorageProfile {
        self.inner.storage_profile()
    }
}

impl<R: Clone> Clone for SlicedReadAt<R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            range: self.range.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sliced_read_at_new() {
        let inner = vec![1u8, 2, 3, 4, 5];
        let sliced = SlicedReadAt::new(inner, 1..4);
        assert_eq!(sliced.slice_size(), 3);
        assert_eq!(sliced.slice_range(), 1..4);
    }

    #[test]
    #[should_panic]
    fn test_sliced_read_at_new_panics() {
        let inner = vec![1u8, 2, 3, 4, 5];
        let _sliced = SlicedReadAt::new(inner, Range { start: 4, end: 1 });
    }

    #[test]
    fn test_sliced_read_at_size() {
        let inner = vec![1u8, 2, 3, 4, 5];
        let sliced = SlicedReadAt::new(inner, 1..4);
        assert_eq!(sliced.size().unwrap(), 3);
    }

    #[test]
    fn test_sliced_read_at_read_at() {
        let inner = vec![1, 2, 3, 4, 5];
        let sliced = SlicedReadAt::new(inner, 1..4);
        let data = sliced.read_at(0..2).unwrap();
        assert_eq!(&data[..], &[2, 3]);
    }

    #[test]
    fn test_sliced_read_at_read_at_empty_range() {
        let inner = vec![1, 2, 3, 4, 5];
        let sliced = SlicedReadAt::new(inner, 1..4);
        let data = sliced.read_at(2..2).unwrap();
        assert!(data.is_empty());
    }

    #[test]
    fn test_sliced_read_at_read_at_out_of_bounds() {
        let inner = vec![1, 2, 3, 4, 5];
        let sliced = SlicedReadAt::new(inner, 1..4);
        let data = sliced.read_at(0..5).unwrap(); // Requesting more than slice size
        assert_eq!(&data[..], &[2, 3, 4]);
    }

    #[test]
    fn test_sliced_read_at_read_at_offset() {
        let inner = vec![1, 2, 3, 4, 5];
        let sliced = SlicedReadAt::new(inner, 2..5);
        let data = sliced.read_at(1..2).unwrap();
        assert_eq!(&data[..], &[4]);
    }

    #[test]
    fn test_sliced_read_at_read_at_large_offset() {
        let inner = vec![0u8; 1024];
        let sliced = SlicedReadAt::new(inner, 512..1024);
        let data = sliced.read_at(0..10).unwrap();
        assert_eq!(&data[..], &[0u8; 10]);
    }

    #[test]
    fn test_sliced_read_at_read_at_overflow_protection() {
        let inner = vec![1, 2, 3, 4, 5];
        let sliced = SlicedReadAt::new(inner, (u64::MAX - 2)..u64::MAX);
        let data = sliced.read_at(0..1).unwrap();
        assert!(data.is_empty());
    }

    #[test]
    fn test_sliced_read_at_inner() {
        let inner = vec![1, 2, 3, 4, 5];
        let sliced = SlicedReadAt::new(inner.clone(), 1..4);
        assert_eq!(sliced.inner(), &inner);
    }
}
