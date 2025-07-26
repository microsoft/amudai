//! Additional methods for `DataRef`, `DataRefArray` and related structures.

use std::ops::Range;

use super::common::{DataRef, DataRefArray, UInt64Range};

impl UInt64Range {
    /// Returns the length of this range.
    pub fn len(&self) -> u64 {
        self.end - self.start
    }

    /// Returns `true` if this range is empty (has zero length).
    pub fn is_empty(&self) -> bool {
        self.end == self.start
    }
}

impl From<Range<u64>> for UInt64Range {
    fn from(range: Range<u64>) -> Self {
        UInt64Range {
            start: range.start,
            end: range.end,
        }
    }
}

impl From<UInt64Range> for Range<u64> {
    fn from(range: UInt64Range) -> Range<u64> {
        range.start..range.end
    }
}

impl From<&DataRef> for Range<u64> {
    fn from(data_ref: &DataRef) -> Range<u64> {
        data_ref.range.unwrap_or_default().into()
    }
}

impl DataRef {
    /// Creates a new `DataRef` with the given URL and range.
    pub fn new(url: impl Into<String>, range: Range<u64>) -> DataRef {
        DataRef {
            url: url.into(),
            range: Some(range.into()),
        }
    }

    /// Creates an empty `DataRef`, which has an empty URL and no range.
    pub fn empty() -> DataRef {
        DataRef::default()
    }

    /// Returns the length of the byte range for this `DataRef`.
    pub fn len(&self) -> u64 {
        self.range.map(|r| r.len()).unwrap_or(0)
    }

    /// Returns `true` if the byte range of this `DataRef` is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Splits the `DataRef` into two `DataRef`s at the given offset.
    ///
    /// # Panics
    ///
    /// Panics if the `DataRef` does not have a range or if the offset is greater
    /// than the length of the range.
    pub fn split_at_offset(self, offset: u64) -> (DataRef, DataRef) {
        let DataRef { url, range } = self;
        let range = range.expect("DataRef must have range");
        let len = range.end - range.start;
        assert!(offset <= len);
        let first = range.start..range.start + offset;
        let second = range.start + offset..range.end;
        (DataRef::new(url.clone(), first), DataRef::new(url, second))
    }

    /// Creates a new `DataRef` that is a slice of the original `DataRef`
    /// starting at the given offset.
    ///
    /// # Panics
    ///
    /// Panics if the `DataRef` does not have a range or if the offset is greater
    /// than the length of the range.
    pub fn slice_from_offset(&self, offset: u64) -> DataRef {
        let range = self.range.expect("DataRef must have range");
        let len = range.end - range.start;
        assert!(offset <= len);
        let slice = range.start + offset..range.end;
        DataRef::new(self.url.clone(), slice)
    }
}

/// Provides access to the URL string of a `DataRef`.
///
/// Returns a reference to the URL string contained within the `DataRef`.
/// This implementation allows `DataRef` to be used anywhere a string reference
/// is expected.
impl AsRef<str> for DataRef {
    #[inline]
    fn as_ref(&self) -> &str {
        &self.url
    }
}

impl DataRefArray {
    pub fn push(&mut self, data_ref: DataRef) {
        assert_eq!(self.url.len(), self.start.len());
        assert_eq!(self.url.len(), self.end.len());
        self.url.push(data_ref.url);
        let range = data_ref.range.unwrap_or_default();
        self.start.push(range.start);
        self.end.push(range.end);
    }

    pub fn get_at(&self, index: usize) -> DataRef {
        DataRef::new(&self.url[index], self.start[index]..self.end[index])
    }
}

#[cfg(test)]
mod tests {
    use crate::defs::common::DataRef;

    #[test]
    fn test_split_at_offset() {
        let data_ref = DataRef::new("test_url", 0..10);
        let (first, second) = data_ref.split_at_offset(5);

        assert_eq!(first.url, "test_url");
        assert_eq!(first.range, Some((0..5).into()));

        assert_eq!(second.url, "test_url");
        assert_eq!(second.range, Some((5..10).into()));
    }

    #[test]
    #[should_panic]
    fn test_split_at_offset_panic_if_offset_too_large() {
        let data_ref = DataRef::new("test_url", 0..10);
        data_ref.split_at_offset(11);
    }

    #[test]
    #[should_panic]
    fn test_split_at_offset_panic_if_no_range() {
        let data_ref = DataRef {
            url: "test_url".to_string(),
            range: None,
        };
        data_ref.split_at_offset(5);
    }

    #[test]
    fn test_slice_from_offset() {
        let data_ref = DataRef::new("test_url", 0..10);
        let slice = data_ref.slice_from_offset(5);

        assert_eq!(slice.url, "test_url");
        assert_eq!(slice.range, Some((5..10).into()));
    }

    #[test]
    #[should_panic]
    fn test_slice_from_offset_panic_if_offset_too_large() {
        let data_ref = DataRef::new("test_url", 0..10);
        data_ref.slice_from_offset(11);
    }

    #[test]
    #[should_panic]
    fn test_slice_from_offset_panic_if_no_range() {
        let data_ref = DataRef {
            url: "test_url".to_string(),
            range: None,
        };
        data_ref.slice_from_offset(5);
    }
}
