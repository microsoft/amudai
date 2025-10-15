//! A collection of offsets for variable-length data.

use std::ops::Range;

use crate::values::Values;

/// A collection of offsets for variable-length data.
///
/// Stores a sequence of monotonically non-decreasing offsets, where each pair of
/// adjacent offsets defines the range of a single item. The first offset is
/// always included, representing the start position of the first item.
#[derive(Debug, Clone)]
pub struct Offsets(Values);

impl Offsets {
    /// Creates a new empty `Offsets` collection.
    ///
    /// The resulting collection will have a single offset at position 0.
    pub fn new() -> Offsets {
        Self::with_capacity(0)
    }

    /// Creates a new `Offsets` collection from an existing `Values` instance.
    #[allow(clippy::len_zero)]
    pub fn from_values(values: Values) -> Offsets {
        assert!(values.len::<u64>() > 0);
        Offsets(values)
    }

    /// Creates a new `Offsets` collection with the specified capacity.
    ///
    /// The resulting collection will have a single offset at position 0,
    /// and space reserved for `capacity` additional offsets.
    pub fn with_capacity(capacity: usize) -> Offsets {
        let mut buf = Values::with_capacity::<u64>(capacity + 1);
        buf.push(0u64);
        Offsets(buf)
    }

    /// Creates a new `Offsets` collection filled with zeros.
    ///
    /// The resulting collection will have `len + 1` offsets, all set to 0.
    pub fn zeroed(len: usize) -> Offsets {
        Offsets(Values::zeroed::<u64>(len + 1))
    }

    /// Returns the number of items represented by these offsets.
    ///
    /// This is one less than the number of stored offsets.
    #[inline]
    pub fn item_count(&self) -> usize {
        self.0.len::<u64>() - 1
    }

    /// Returns `true` if the collection contains no items.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.item_count() == 0
    }

    /// Returns a reference to the underlying slice of offsets.
    #[inline]
    pub fn as_slice(&self) -> &[u64] {
        self.0.as_slice()
    }

    /// Returns a mutable reference to the underlying slice of offsets.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u64] {
        self.0.as_mut_slice()
    }

    /// Returns the first offset, which is always the start position.
    #[inline]
    pub fn first(&self) -> u64 {
        self.as_slice()[0]
    }

    /// Returns the last offset, which marks the end of the last item.
    #[inline]
    pub fn last(&self) -> u64 {
        *self.as_slice().last().unwrap()
    }

    /// Returns the total length of all items combined.
    ///
    /// This is calculated as the last offset minus the first offset.
    #[inline]
    pub fn span(&self) -> u64 {
        let r = self.range();
        r.end - r.start
    }

    /// Returns a range covering all items from the first to the last offset.
    #[inline]
    pub fn range(&self) -> Range<u64> {
        let s = self.as_slice();
        s[0]..*s.last().unwrap()
    }

    /// Returns an iterator over the ranges of each item.
    #[inline]
    pub fn ranges(&self) -> OffsetsIter<'_> {
        OffsetsIter::new(self)
    }

    /// Returns a range at a given logical index.
    #[inline]
    pub fn range_at(&self, index: usize) -> Range<u64> {
        let offsets = self.as_slice();
        offsets[index]..offsets[index + 1]
    }

    /// Adds a new offset to the end of the collection.
    ///
    /// # Panics
    ///
    /// Panics if `next_offset` is less than the current last offset.
    #[inline]
    pub fn push_offset(&mut self, next_offset: u64) {
        assert!(next_offset >= self.last());
        self.0.push(next_offset);
    }

    /// Adds a new offset by incrementing the last offset by the given length.
    #[inline]
    pub fn push_length(&mut self, len: usize) {
        let last = self.last();
        self.0.push(last + len as u64);
    }

    /// Appends `count` zero-sized items to the collection.
    ///
    /// This efficiently adds multiple empty items by resizing the collection
    /// and repeating the last offset value.
    #[inline]
    pub fn push_empty(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        let current_len = self.item_count();
        let last = self.last();
        self.resize(current_len + count, last);
    }

    /// Appends all item offsets from another `Offsets` collection.
    ///
    /// The offsets are adjusted to be continuous with the current collection.
    pub fn extend_from_offsets(&mut self, offsets: &Offsets) {
        self.extend_from_offsets_slice(offsets.as_slice());
    }

    /// Appends item offsets from a range of another `Offsets` collection.
    ///
    /// The offsets are adjusted to be continuous with the current collection.
    ///
    /// # Arguments
    ///
    /// * `offsets` - The source offsets collection.
    /// * `start` - The starting index in the source collection.
    /// * `len` - The number of offsets (items) to append.
    pub fn extend_from_offsets_range(&mut self, offsets: &Offsets, start: usize, len: usize) {
        self.extend_from_offsets_slice(&offsets.as_slice()[start..start + len + 1]);
    }

    /// Appends item offsets from a slice of raw offsets.
    ///
    /// The offsets are adjusted to be continuous with the current collection.
    /// The first offset in the slice is used as the base for adjustment.
    /// Does nothing if the input slice has fewer than 2 elements.
    pub fn extend_from_offsets_slice(&mut self, offsets: &[u64]) {
        if offsets.len() < 2 {
            return;
        }
        let last = self.last();
        let base = offsets[0];
        self.0.append::<u64, _>(offsets.len() - 1, |dst_slice| {
            dst_slice
                .iter_mut()
                .zip(offsets[1..].iter())
                .for_each(|(dst, src_offset)| *dst = *src_offset - base + last);
        });
    }

    /// Resizes the offsets collection to the specified length.
    ///
    /// If `new_len` is greater than the current length, the collection is extended
    /// with copies of `value`. Note that this includes the sentinel offset.
    pub fn resize(&mut self, new_len: usize, value: u64) {
        self.0.resize(new_len + 1, value);
    }

    /// Clears the collection, leaving only the initial offset at 0.
    pub fn clear(&mut self) {
        self.0.clear();
        self.0.push(0u64);
    }

    /// Increases all offsets by the given amount.
    ///
    /// This is useful when rebasing a set of offsets to a new starting position.
    /// All offsets, including the initial offset, are incremented by `shift`.
    ///
    /// # Arguments
    ///
    /// * `shift` - The value to add to each offset.
    pub fn shift_up(&mut self, shift: u64) {
        self.as_mut_slice().iter_mut().for_each(|off| *off += shift);
    }

    /// Decreases all offsets by the given amount.
    ///
    /// This is useful when normalizing offsets so that the first offset is zero,
    /// or when rebasing to a lower starting position. All offsets, including the
    /// initial offset, are decremented by `shift`.
    ///
    /// # Arguments
    ///
    /// * `shift` - The value to subtract from each offset.
    pub fn shift_down(&mut self, shift: u64) {
        self.as_mut_slice().iter_mut().for_each(|off| *off -= shift);
    }

    /// Normalizes the offsets so that the first offset is zero.
    ///
    /// If the first offset is not zero, all offsets are shifted down by the value
    /// of the first offset. This is useful for ensuring that the offsets are
    /// zero-based.
    pub fn normalize(&mut self) {
        let first = self.first();
        if first != 0 {
            self.shift_down(self.first());
        }
    }

    /// Consumes the `Offsets` collection and returns the underlying `Values`
    /// buffer.
    ///
    /// This method can be used to extract the internal storage for reuse or
    /// serialization.
    /// The returned [`Values`] contains all offset values as `u64`, including
    /// the initial offset.
    pub fn into_inner(self) -> Values {
        self.0
    }
}

impl Default for Offsets {
    fn default() -> Self {
        Self::new()
    }
}

impl std::ops::Deref for Offsets {
    type Target = [u64];

    /// Provides direct access to the underlying offset slice.
    #[inline]
    fn deref(&self) -> &[u64] {
        self.as_slice()
    }
}

/// Iterator over the ranges of items defined by the offsets.
pub struct OffsetsIter<'a> {
    offsets: &'a [u64],
    index: usize,
    len: usize,
}

impl<'a> OffsetsIter<'a> {
    pub fn new(offsets: &'a Offsets) -> Self {
        Self {
            offsets,
            index: 0,
            len: offsets.len(),
        }
    }
}

impl Iterator for OffsetsIter<'_> {
    type Item = Range<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index + 1 < self.len {
            let start = self.offsets[self.index];
            let end = self.offsets[self.index + 1];
            self.index += 1;
            Some(start as usize..end as usize)
        } else {
            None
        }
    }
}

/// Iterator over the ranges of items of fixed size.
pub struct FixedSizeOffsetsIter {
    current_offset: usize,
    max_offset: usize,
    fixed_size: usize,
}

impl FixedSizeOffsetsIter {
    pub fn new(fixed_size: usize, count: usize) -> Self {
        assert!(fixed_size > 0);
        Self {
            current_offset: 0,
            max_offset: count * fixed_size,
            fixed_size,
        }
    }
}

impl Iterator for FixedSizeOffsetsIter {
    type Item = Range<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_offset < self.max_offset {
            let start = self.current_offset;
            let end = start + self.fixed_size;
            self.current_offset += self.fixed_size;
            Some(start..end)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let offsets = Offsets::new();
        assert_eq!(offsets.item_count(), 0);
        assert_eq!(offsets.as_slice(), &[0]);
        assert!(offsets.is_empty());
    }

    #[test]
    fn test_zeroed() {
        let offsets = Offsets::zeroed(5);
        assert_eq!(offsets.item_count(), 5);
        assert_eq!(offsets.as_slice(), &[0, 0, 0, 0, 0, 0]);
        assert!(!offsets.is_empty());
    }

    #[test]
    fn test_item_count() {
        let mut offsets = Offsets::new();
        assert_eq!(offsets.item_count(), 0);

        offsets.push_offset(5);
        assert_eq!(offsets.item_count(), 1);

        offsets.push_offset(10);
        assert_eq!(offsets.item_count(), 2);
    }

    #[test]
    fn test_is_empty() {
        let mut offsets = Offsets::new();
        assert!(offsets.is_empty());

        offsets.push_offset(5);
        assert!(!offsets.is_empty());
    }

    #[test]
    fn test_as_slice() {
        let mut offsets = Offsets::new();
        offsets.push_offset(5);
        offsets.push_offset(10);

        assert_eq!(offsets.as_slice(), &[0, 5, 10]);
    }

    #[test]
    fn test_as_mut_slice() {
        let mut offsets = Offsets::new();
        offsets.push_offset(5);
        offsets.push_offset(10);

        {
            let slice = offsets.as_mut_slice();
            slice[1] = 7;
        }

        assert_eq!(offsets.as_slice(), &[0, 7, 10]);
    }

    #[test]
    fn test_first() {
        let offsets = Offsets::new();
        assert_eq!(offsets.first(), 0);

        let mut values = Values::with_capacity::<u64>(3);
        values.push(3u64);
        values.push(5u64);
        values.push(10u64);
        let offsets = Offsets(values);

        assert_eq!(offsets.first(), 3);
    }

    #[test]
    fn test_last() {
        let mut offsets = Offsets::new();
        assert_eq!(offsets.last(), 0);

        offsets.push_offset(5);
        assert_eq!(offsets.last(), 5);

        offsets.push_offset(10);
        assert_eq!(offsets.last(), 10);
    }

    #[test]
    fn test_span() {
        let mut offsets = Offsets::new();
        assert_eq!(offsets.span(), 0);

        offsets.push_offset(5);
        assert_eq!(offsets.span(), 5);

        offsets.push_offset(10);
        assert_eq!(offsets.span(), 10);

        let mut values = Values::with_capacity::<u64>(3);
        values.push(3u64);
        values.push(8u64);
        values.push(15u64);
        let offsets = Offsets(values);

        assert_eq!(offsets.span(), 12);
    }

    #[test]
    fn test_range() {
        let mut offsets = Offsets::new();
        assert_eq!(offsets.range(), 0..0);

        offsets.push_offset(5);
        assert_eq!(offsets.range(), 0..5);

        offsets.push_offset(10);
        assert_eq!(offsets.range(), 0..10);

        let mut values = Values::with_capacity::<u64>(3);
        values.push(3u64);
        values.push(8u64);
        values.push(15u64);
        let offsets = Offsets(values);

        assert_eq!(offsets.range(), 3..15);
    }

    #[test]
    fn test_push_offset() {
        let mut offsets = Offsets::new();
        offsets.push_offset(5);
        offsets.push_offset(10);
        offsets.push_offset(10);

        assert_eq!(offsets.as_slice(), &[0, 5, 10, 10]);
        assert_eq!(offsets.item_count(), 3);
    }

    #[test]
    #[should_panic]
    fn test_push_offset_panic() {
        let mut offsets = Offsets::new();
        offsets.push_offset(5);
        offsets.push_offset(3);
    }

    #[test]
    fn test_push_length() {
        let mut offsets = Offsets::new();
        offsets.push_length(5);
        assert_eq!(offsets.as_slice(), &[0, 5]);

        offsets.push_length(3);
        assert_eq!(offsets.as_slice(), &[0, 5, 8]);

        offsets.push_length(0);
        assert_eq!(offsets.as_slice(), &[0, 5, 8, 8]);
    }

    #[test]
    fn test_push_empty() {
        let mut offsets = Offsets::new();
        offsets.push_length(5);
        assert_eq!(offsets.as_slice(), &[0, 5]);

        offsets.push_empty(3);
        assert_eq!(offsets.as_slice(), &[0, 5, 5, 5, 5]);
        assert_eq!(offsets.item_count(), 4);

        offsets.push_empty(0);
        assert_eq!(offsets.as_slice(), &[0, 5, 5, 5, 5]);
        assert_eq!(offsets.item_count(), 4);
    }

    #[test]
    fn test_extend_from_offsets() {
        let mut offsets1 = Offsets::new();
        offsets1.push_length(5);
        offsets1.push_length(3);
        assert_eq!(offsets1.as_slice(), &[0, 5, 8]);

        let mut offsets2 = Offsets::new();
        offsets2.push_length(2);
        offsets2.push_length(4);
        assert_eq!(offsets2.as_slice(), &[0, 2, 6]);

        offsets1.extend_from_offsets(&offsets2);
        assert_eq!(offsets1.as_slice(), &[0, 5, 8, 10, 14]);
    }

    #[test]
    fn test_extend_from_offsets_slice() {
        let mut offsets = Offsets::new();
        offsets.push_length(5);
        assert_eq!(offsets.as_slice(), &[0, 5]);

        offsets.extend_from_offsets_slice(&[0, 3, 7, 7]);
        assert_eq!(offsets.as_slice(), &[0, 5, 8, 12, 12]);

        offsets.extend_from_offsets_slice(&[]);
        assert_eq!(offsets.as_slice(), &[0, 5, 8, 12, 12]);

        offsets.extend_from_offsets_slice(&[10]);
        assert_eq!(offsets.as_slice(), &[0, 5, 8, 12, 12]);
    }

    #[test]
    fn test_extend_from_offsets_with_non_zero_base() {
        let mut offsets1 = Offsets::new();
        offsets1.push_length(5);
        offsets1.push_length(3);
        assert_eq!(offsets1.as_slice(), &[0, 5, 8]);

        let mut values = Values::with_capacity::<u64>(3);
        values.push(10u64);
        values.push(15u64);
        values.push(20u64);
        let offsets2 = Offsets(values);

        offsets1.extend_from_offsets(&offsets2);
        assert_eq!(offsets1.as_slice(), &[0, 5, 8, 13, 18]);
    }

    #[test]
    fn test_resize() {
        let mut offsets = Offsets::new();
        offsets.push_offset(5);
        offsets.push_offset(10);

        offsets.resize(4, 15);
        assert_eq!(offsets.as_slice(), &[0, 5, 10, 15, 15]);

        offsets.resize(2, 0); // Value ignored for truncation
        assert_eq!(offsets.as_slice(), &[0, 5, 10]);
    }

    #[test]
    fn test_clear() {
        let mut offsets = Offsets::new();
        offsets.push_offset(5);
        offsets.push_offset(10);

        offsets.clear();
        assert_eq!(offsets.as_slice(), &[0]);
        assert_eq!(offsets.item_count(), 0);
        assert!(offsets.is_empty());
    }

    #[test]
    fn test_deref() {
        let mut offsets = Offsets::new();
        offsets.push_offset(5);
        offsets.push_offset(10);

        assert_eq!(offsets[0], 0);
        assert_eq!(offsets[1], 5);
        assert_eq!(offsets[2], 10);

        assert_eq!(offsets.len(), 3);
    }

    #[test]
    fn test_complex_scenario() {
        let mut offsets = Offsets::with_capacity(5);

        offsets.push_length(3);
        offsets.push_length(0);
        offsets.push_length(5);

        assert_eq!(offsets.as_slice(), &[0, 3, 3, 8]);

        offsets.push_empty(2);
        assert_eq!(offsets.as_slice(), &[0, 3, 3, 8, 8, 8]);

        let mut other = Offsets::new();
        other.push_length(4);
        other.push_length(2);

        offsets.extend_from_offsets(&other);
        assert_eq!(offsets.as_slice(), &[0, 3, 3, 8, 8, 8, 12, 14]);

        offsets.clear();
        assert_eq!(offsets.as_slice(), &[0]);

        offsets.push_length(10);
        offsets.push_length(5);
        assert_eq!(offsets.as_slice(), &[0, 10, 15]);
        assert_eq!(offsets.span(), 15);
        assert_eq!(offsets.range(), 0..15);
    }

    #[test]
    fn test_offsets_iter() {
        let mut offsets = Offsets::new();
        offsets.push_offset(5);
        offsets.push_offset(10);
        offsets.push_offset(15);

        let mut iter = offsets.ranges();
        assert_eq!(iter.next(), Some(0..5));
        assert_eq!(iter.next(), Some(5..10));
        assert_eq!(iter.next(), Some(10..15));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_fixed_size_offsets_iter() {
        let mut iter = FixedSizeOffsetsIter::new(4, 3);
        assert_eq!(iter.next(), Some(0..4));
        assert_eq!(iter.next(), Some(4..8));
        assert_eq!(iter.next(), Some(8..12));
        assert_eq!(iter.next(), None);

        let mut iter = FixedSizeOffsetsIter::new(4, 0);
        assert_eq!(iter.next(), None);
    }
}
