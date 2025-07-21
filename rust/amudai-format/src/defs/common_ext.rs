use std::ops::Range;

use crate::defs::common::BytesList;

use super::common::{AnyValue, DataRef, DataRefArray, UInt64Range, any_value};

impl AnyValue {
    pub fn as_i64(&self) -> Option<i64> {
        match self.kind.as_ref()? {
            any_value::Kind::U64Value(v) if *v < i64::MAX as u64 => Some(*v as i64),
            any_value::Kind::I64Value(v) => Some(*v),
            any_value::Kind::DatetimeValue(v) if v.ticks < i64::MAX as u64 => Some(v.ticks as i64),
            any_value::Kind::TimespanValue(v) if v.ticks < i64::MAX as u64 => Some(v.ticks as i64),
            _ => None,
        }
    }

    /// Attempts to extract a `u64` value from this `AnyValue`.
    ///
    /// Returns `Some(u64)` if this `AnyValue` contains a compatible numeric type that can be
    /// safely converted to `u64`, including:
    /// - Direct `u64` values
    /// - Non-negative `i64` values
    /// - DateTime tick values
    /// - Timespan tick values
    ///
    /// Returns `None` if the value cannot be converted to `u64` or is null.
    pub fn as_u64(&self) -> Option<u64> {
        match self.kind.as_ref()? {
            any_value::Kind::U64Value(v) => Some(*v),
            any_value::Kind::I64Value(v) if *v >= 0 => Some(*v as u64),
            any_value::Kind::DatetimeValue(v) => Some(v.ticks),
            any_value::Kind::TimespanValue(v) => Some(v.ticks),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self.kind.as_ref()? {
            any_value::Kind::DoubleValue(v) => Some(*v),
            _ => None,
        }
    }

    /// Attempts to extract a boolean value from this `AnyValue`.
    ///
    /// Returns `Some(bool)` if this `AnyValue` contains a boolean value,
    /// `None` otherwise or if the value is null.
    pub fn as_bool(&self) -> Option<bool> {
        match self.kind.as_ref()? {
            any_value::Kind::BoolValue(v) => Some(*v),
            _ => None,
        }
    }

    /// Attempts to extract a string slice from this `AnyValue`.
    ///
    /// Returns `Some(&str)` if this `AnyValue` contains a string value,
    /// `None` otherwise or if the value is null.
    pub fn as_str(&self) -> Option<&str> {
        match self.kind.as_ref()? {
            any_value::Kind::StringValue(v) => Some(v.as_str()),
            _ => None,
        }
    }

    /// Attempts to extract a byte slice from this `AnyValue`.
    ///
    /// Returns `Some(&[u8])` if this `AnyValue` contains binary data or string data
    /// (strings are converted to their UTF-8 byte representation), `None` otherwise
    /// or if the value is null.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self.kind.as_ref()? {
            any_value::Kind::StringValue(v) => Some(v.as_bytes()),
            any_value::Kind::BytesValue(v) => Some(v.as_slice()),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self.kind, Some(any_value::Kind::NullValue(_)))
    }
}

impl From<i64> for AnyValue {
    fn from(value: i64) -> AnyValue {
        AnyValue {
            kind: Some(any_value::Kind::I64Value(value)),
            annotation: None,
        }
    }
}

/// Converts a `u64` value into an `AnyValue`.
///
/// The resulting `AnyValue` will contain the unsigned 64-bit integer value
/// with no annotation.
impl From<u64> for AnyValue {
    fn from(value: u64) -> AnyValue {
        AnyValue {
            kind: Some(any_value::Kind::U64Value(value)),
            annotation: None,
        }
    }
}

/// Converts an `f64` value into an `AnyValue`.
///
/// The resulting `AnyValue` will contain the floating-point value
/// with no annotation.
impl From<f64> for AnyValue {
    fn from(value: f64) -> AnyValue {
        AnyValue {
            kind: Some(any_value::Kind::DoubleValue(value)),
            annotation: None,
        }
    }
}

impl From<String> for AnyValue {
    fn from(s: String) -> AnyValue {
        AnyValue {
            kind: Some(any_value::Kind::StringValue(s)),
            annotation: None,
        }
    }
}

impl From<&str> for AnyValue {
    fn from(s: &str) -> AnyValue {
        AnyValue::from(s.to_string())
    }
}

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

impl BytesList {
    /// Returns the number of byte buffers stored in this `BytesList`.
    ///
    /// The length is calculated as `offsets.len() - 1` because the offsets array
    /// contains one more element than the number of buffers (the first offset is
    /// always 0, and each subsequent offset marks the end of a buffer).
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    /// Returns `true` if this `BytesList` contains no byte buffers.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the length in bytes of the buffer at the specified index.
    ///
    /// # Arguments
    /// * `index` - The zero-based index of the buffer to query
    ///
    /// # Panics
    /// Panics if `index` is out of bounds (>= `len()`).
    #[inline]
    pub fn length_at(&self, index: usize) -> usize {
        (self.offsets[index + 1] - self.offsets[index]) as usize
    }

    /// Returns a reference to the byte buffer at the specified index.
    ///
    /// The returned slice contains the bytes for the buffer at the given index,
    /// extracted from the concatenated `data` field using the offset information.
    ///
    /// # Arguments  
    /// * `index` - The zero-based index of the buffer to retrieve
    ///
    /// # Returns
    /// A byte slice containing the buffer data at the specified index.
    ///
    /// # Panics
    /// Panics if `index` is out of bounds (>= `len()`).
    #[inline]
    pub fn value_at(&self, index: usize) -> &[u8] {
        let start = self.offsets[index] as usize;
        let end = self.offsets[index + 1] as usize;
        &self.data[start..end]
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
