//! A `ReadAt`/`WriteAt` adapter that restricts I/O to a specified range of the underlying
//! reader and/or writer.

use std::ops::Range;

use amudai_bytes::Bytes;

use crate::{ReadAt, StorageProfile, WriteAt, verify};

/// A `ReadAt`/`WriteAt` adapter that restricts I/O to a specified range of the underlying
/// reader and/or writer.
///
/// `SlicedFile` allows you to treat a portion of a larger `ReadAt`/`WriteAt` source as a separate,
/// independent `ReadAt`/`WriteAt` instance.  All I/O operations are **relative to the slice's starting
/// position**.
///
/// For example, if the underlying reader has a size of 100, and a `SlicedFile` is created
/// with a range of `10..20`, then:
///
/// *   `size()` will return `10` (20 - 10).
/// *   `read_at(0..5)` will read bytes 10-15 from the underlying reader.
/// *   `read_at(5..10)` will read bytes 15-20 from the underlying reader.
/// *   `read_at(0..15)` will read bytes 10-20 from the underlying reader
///     (clamped to the slice size).
pub struct SlicedFile<F> {
    inner: F,
    range: Range<u64>,
}

impl<F> SlicedFile<F> {
    /// Creates a new `SlicedFile` adapter.
    ///
    /// # Panics
    ///
    /// Panics if `range.start > range.end`.
    pub fn new(inner: F, range: Range<u64>) -> Self {
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
    pub fn inner(&self) -> &F {
        &self.inner
    }

    /// Consumes the `SlicedFile`, returning the underlying reader.
    pub fn into_inner(self) -> F {
        self.inner
    }

    /// Converts the current instance into a new sub-slice, using the specified `range`
    /// relative to the starting position of this slice.
    pub fn into_slice(self, range: Range<u64>) -> std::io::Result<SlicedFile<F>> {
        let this_size = self.slice_size();
        verify!(range.start <= this_size);
        verify!(range.end <= this_size);
        Ok(SlicedFile {
            inner: self.inner,
            range: self.range.start + range.start..self.range.start + range.end,
        })
    }
}

impl<F: Clone> SlicedFile<F> {
    /// Creates a new sub-slice from this `SlicedFile` using the specified range.
    ///
    /// This method creates a new `SlicedFile` that represents a subset of the current slice.
    /// The provided `range` is interpreted **relative to the starting position of this slice**,
    /// not the underlying reader. The new slice will share the same underlying reader through
    /// cloning.
    ///
    /// Unlike [`into_slice`](SlicedFile::into_slice), this method borrows `self` and clones
    /// the underlying reader, allowing you to keep the original slice intact while creating
    /// new sub-slices.
    ///
    /// # Arguments
    ///
    /// * `range` - A byte range relative to the start of this slice. Both `start` and `end`
    ///   must be within the bounds of this slice (i.e., `<= slice_size()`).
    ///
    /// # Returns
    ///
    /// A new `SlicedFile` representing the specified sub-range of this slice.
    ///
    /// # Errors
    ///
    /// Returns an `std::io::Error` with kind `InvalidInput` if:
    /// - `range.start > slice_size()`
    /// - `range.end > slice_size()`
    pub fn slice(&self, range: Range<u64>) -> std::io::Result<SlicedFile<F>> {
        let this_size = self.slice_size();
        verify!(range.start <= this_size);
        verify!(range.end <= this_size);
        Ok(SlicedFile {
            inner: self.inner.clone(),
            range: self.range.start + range.start..self.range.start + range.end,
        })
    }

    /// Splits off the first `size` bytes from this slice's range and returns a new
    /// `SlicedFile` containing those bytes.
    ///
    /// This method mutates the current `SlicedFile` by advancing its starting position
    /// by `size` bytes, effectively removing the first `size` bytes from its range.
    /// The returned `SlicedFile` contains exactly the bytes that were removed from
    /// the current slice.
    ///
    /// # Arguments
    ///
    /// * `size` - The number of bytes to split off from the beginning of this slice.
    ///   Must be less than or equal to the current slice size.
    ///
    /// # Returns
    ///
    /// A new `SlicedFile` containing the first `size` bytes of the original slice.
    ///
    /// # Errors
    ///
    /// Returns an error if `size` is greater than the current slice size.
    pub fn split_first(&mut self, size: u64) -> std::io::Result<SlicedFile<F>> {
        let this_size = self.slice_size();
        verify!(size <= this_size);

        // Create the new slice for the first `size` bytes
        let first_slice = SlicedFile {
            inner: self.inner.clone(),
            range: self.range.start..self.range.start + size,
        };

        // Update this slice to remove the first `size` bytes
        self.range.start += size;

        Ok(first_slice)
    }

    /// Splits off the last `size` bytes from this slice's range and returns a new
    /// `SlicedFile` containing those bytes.
    ///
    /// This method mutates the current `SlicedFile` by reducing its ending position
    /// by `size` bytes, effectively removing the last `size` bytes from its range.
    /// The returned `SlicedFile` contains exactly the bytes that were removed from
    /// the current slice.
    ///
    /// # Arguments
    ///
    /// * `size` - The number of bytes to split off from the end of this slice.
    ///   Must be less than or equal to the current slice size.
    ///
    /// # Returns
    ///
    /// A new `SlicedFile` containing the last `size` bytes of the original slice.
    ///
    /// # Errors
    ///
    /// Returns an error if `size` is greater than the current slice size.
    pub fn split_last(&mut self, size: u64) -> std::io::Result<SlicedFile<F>> {
        let this_size = self.slice_size();
        verify!(size <= this_size);

        // Create the new slice for the last `size` bytes
        let last_slice = SlicedFile {
            inner: self.inner.clone(),
            range: self.range.end - size..self.range.end,
        };

        // Update this slice to remove the last `size` bytes
        self.range.end -= size;

        Ok(last_slice)
    }
}

impl<R: ReadAt> SlicedFile<R> {
    /// Reads the entire range of bytes represented by this `SlicedFile`
    /// from the underlying reader.
    pub fn read_all(&self) -> std::io::Result<Bytes> {
        self.inner.read_at(self.range.clone())
    }
}

impl<R: ReadAt> ReadAt for SlicedFile<R> {
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

        let inner_start = self.range.start.checked_add(start).expect("inner_start");
        let inner_end = self.range.start.checked_add(end).expect("inner_end");
        self.inner.read_at(inner_start..inner_end)
    }

    fn storage_profile(&self) -> StorageProfile {
        self.inner.storage_profile()
    }
}

impl<W: WriteAt> WriteAt for SlicedFile<W> {
    fn write_at(&self, pos: u64, buf: &[u8]) -> std::io::Result<()> {
        let end_pos = pos.checked_add(buf.len() as u64).expect("end_pos");
        if end_pos > self.range.end {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "write beyond slice bounds",
            ));
        }

        if buf.is_empty() {
            return Ok(());
        }
        let inner_pos = self.range.start.checked_add(pos).expect("inner_pos");
        self.inner.write_at(inner_pos, buf)
    }

    fn storage_profile(&self) -> StorageProfile {
        self.inner.storage_profile()
    }
}

impl<R: Clone> Clone for SlicedFile<R> {
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
        let sliced = SlicedFile::new(inner, 1..4);
        assert_eq!(sliced.slice_size(), 3);
        assert_eq!(sliced.slice_range(), 1..4);
    }

    #[test]
    #[should_panic]
    fn test_sliced_read_at_new_panics() {
        let inner = vec![1u8, 2, 3, 4, 5];
        let _sliced = SlicedFile::new(inner, Range { start: 4, end: 1 });
    }

    #[test]
    fn test_sliced_read_at_size() {
        let inner = vec![1u8, 2, 3, 4, 5];
        let sliced = SlicedFile::new(inner, 1..4);
        assert_eq!(sliced.size().unwrap(), 3);
    }

    #[test]
    fn test_sliced_read_at_read_at() {
        let inner = vec![1, 2, 3, 4, 5];
        let sliced = SlicedFile::new(inner, 1..4);
        let data = sliced.read_at(0..2).unwrap();
        assert_eq!(&data[..], &[2, 3]);
    }

    #[test]
    fn test_sliced_read_at_read_at_empty_range() {
        let inner = vec![1, 2, 3, 4, 5];
        let sliced = SlicedFile::new(inner, 1..4);
        let data = sliced.read_at(2..2).unwrap();
        assert!(data.is_empty());
    }

    #[test]
    fn test_sliced_read_at_read_at_out_of_bounds() {
        let inner = vec![1, 2, 3, 4, 5];
        let sliced = SlicedFile::new(inner, 1..4);
        let data = sliced.read_at(0..5).unwrap(); // Requesting more than slice size
        assert_eq!(&data[..], &[2, 3, 4]);
    }

    #[test]
    fn test_sliced_read_at_read_at_offset() {
        let inner = vec![1, 2, 3, 4, 5];
        let sliced = SlicedFile::new(inner, 2..5);
        let data = sliced.read_at(1..2).unwrap();
        assert_eq!(&data[..], &[4]);
    }

    #[test]
    fn test_sliced_read_at_read_at_large_offset() {
        let inner = vec![0u8; 1024];
        let sliced = SlicedFile::new(inner, 512..1024);
        let data = sliced.read_at(0..10).unwrap();
        assert_eq!(&data[..], &[0u8; 10]);
    }

    #[test]
    fn test_sliced_read_at_read_at_overflow_protection() {
        let inner = vec![1, 2, 3, 4, 5];
        let sliced = SlicedFile::new(inner, (u64::MAX - 2)..u64::MAX);
        let data = sliced.read_at(0..1).unwrap();
        assert!(data.is_empty());
    }

    #[test]
    fn test_sliced_read_at_inner() {
        let inner = vec![1, 2, 3, 4, 5];
        let sliced = SlicedFile::new(inner.clone(), 1..4);
        assert_eq!(sliced.inner(), &inner);
    }

    #[test]
    fn test_split_first() {
        let inner = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut sliced = SlicedFile::new(inner, 2..8); // Contains bytes [3, 4, 5, 6, 7, 8]

        // Split first 2 bytes
        let first_part = sliced.split_first(2).unwrap();

        // Verify the first part contains the expected range
        assert_eq!(first_part.slice_size(), 2);
        assert_eq!(first_part.slice_range(), 2..4);
        let data = first_part.read_all().unwrap();
        assert_eq!(&data[..], &[3, 4]);

        // Verify the remaining slice is updated correctly
        assert_eq!(sliced.slice_size(), 4);
        assert_eq!(sliced.slice_range(), 4..8);
        let remaining_data = sliced.read_all().unwrap();
        assert_eq!(&remaining_data[..], &[5, 6, 7, 8]);
    }

    #[test]
    fn test_split_first_entire_slice() {
        let inner = vec![1, 2, 3, 4, 5];
        let mut sliced = SlicedFile::new(inner, 1..4); // Contains bytes [2, 3, 4]

        // Split the entire slice
        let first_part = sliced.split_first(3).unwrap();

        // Verify the first part contains the entire original range
        assert_eq!(first_part.slice_size(), 3);
        assert_eq!(first_part.slice_range(), 1..4);
        let data = first_part.read_all().unwrap();
        assert_eq!(&data[..], &[2, 3, 4]);

        // Verify the remaining slice is empty
        assert_eq!(sliced.slice_size(), 0);
        assert_eq!(sliced.slice_range(), 4..4);
        let remaining_data = sliced.read_all().unwrap();
        assert!(remaining_data.is_empty());
    }

    #[test]
    fn test_split_first_zero_size() {
        let inner = vec![1, 2, 3, 4, 5];
        let mut sliced = SlicedFile::new(inner, 1..4);
        let original_range = sliced.slice_range();

        // Split zero bytes
        let first_part = sliced.split_first(0).unwrap();

        // Verify the first part is empty
        assert_eq!(first_part.slice_size(), 0);
        assert_eq!(first_part.slice_range(), 1..1);
        let data = first_part.read_all().unwrap();
        assert!(data.is_empty());

        // Verify the original slice is unchanged
        assert_eq!(sliced.slice_range(), original_range);
    }

    #[test]
    #[should_panic]
    fn test_split_first_size_too_large() {
        let inner = vec![1, 2, 3, 4, 5];
        let mut sliced = SlicedFile::new(inner, 1..4); // Size is 3

        // Try to split 4 bytes (more than available)
        sliced.split_first(4).unwrap();
    }

    #[test]
    fn test_split_last() {
        let inner = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut sliced = SlicedFile::new(inner, 2..8); // Contains bytes [3, 4, 5, 6, 7, 8]

        // Split last 2 bytes
        let last_part = sliced.split_last(2).unwrap();

        // Verify the last part contains the expected range
        assert_eq!(last_part.slice_size(), 2);
        assert_eq!(last_part.slice_range(), 6..8);
        let data = last_part.read_all().unwrap();
        assert_eq!(&data[..], &[7, 8]);

        // Verify the remaining slice is updated correctly
        assert_eq!(sliced.slice_size(), 4);
        assert_eq!(sliced.slice_range(), 2..6);
        let remaining_data = sliced.read_all().unwrap();
        assert_eq!(&remaining_data[..], &[3, 4, 5, 6]);
    }

    #[test]
    fn test_split_last_entire_slice() {
        let inner = vec![1, 2, 3, 4, 5];
        let mut sliced = SlicedFile::new(inner, 1..4); // Contains bytes [2, 3, 4]

        // Split the entire slice
        let last_part = sliced.split_last(3).unwrap();

        // Verify the last part contains the entire original range
        assert_eq!(last_part.slice_size(), 3);
        assert_eq!(last_part.slice_range(), 1..4);
        let data = last_part.read_all().unwrap();
        assert_eq!(&data[..], &[2, 3, 4]);

        // Verify the remaining slice is empty
        assert_eq!(sliced.slice_size(), 0);
        assert_eq!(sliced.slice_range(), 1..1);
        let remaining_data = sliced.read_all().unwrap();
        assert!(remaining_data.is_empty());
    }

    #[test]
    fn test_split_last_zero_size() {
        let inner = vec![1, 2, 3, 4, 5];
        let mut sliced = SlicedFile::new(inner, 1..4);
        let original_range = sliced.slice_range();

        // Split zero bytes
        let last_part = sliced.split_last(0).unwrap();

        // Verify the last part is empty
        assert_eq!(last_part.slice_size(), 0);
        assert_eq!(last_part.slice_range(), 4..4);
        let data = last_part.read_all().unwrap();
        assert!(data.is_empty());

        // Verify the original slice is unchanged
        assert_eq!(sliced.slice_range(), original_range);
    }

    #[test]
    #[should_panic]
    fn test_split_last_size_too_large() {
        let inner = vec![1, 2, 3, 4, 5];
        let mut sliced = SlicedFile::new(inner, 1..4); // Size is 3

        // Try to split 4 bytes (more than available)
        sliced.split_last(4).unwrap();
    }

    #[test]
    fn test_split_first_and_split_last_combined() {
        let inner = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut sliced = SlicedFile::new(inner, 1..9); // Contains bytes [2, 3, 4, 5, 6, 7, 8, 9]

        // Split first 2 bytes
        let first_part = sliced.split_first(2).unwrap();
        assert_eq!(first_part.slice_range(), 1..3);
        let first_data = first_part.read_all().unwrap();
        assert_eq!(&first_data[..], &[2, 3]);

        // Split last 2 bytes from remaining slice
        let last_part = sliced.split_last(2).unwrap();
        assert_eq!(last_part.slice_range(), 7..9);
        let last_data = last_part.read_all().unwrap();
        assert_eq!(&last_data[..], &[8, 9]);

        // Verify the middle slice remains
        assert_eq!(sliced.slice_size(), 4);
        assert_eq!(sliced.slice_range(), 3..7);
        let middle_data = sliced.read_all().unwrap();
        assert_eq!(&middle_data[..], &[4, 5, 6, 7]);
    }
}
