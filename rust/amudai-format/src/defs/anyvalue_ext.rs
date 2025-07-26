//! Extensions for working with AnyValue instances.
//!
//! This module provides utility functions for AnyValue operations
//! such as comparison and ordering.

use crate::defs::common::{
    AnyValue,
    any_value::{self, Kind},
};
use amudai_common::Result;
use amudai_decimal::d128;

impl AnyValue {
    /// Compares two AnyValue instances, returning -1, 0, or 1.
    ///
    /// This function provides ordering comparison for AnyValue instances to support
    /// range statistics merging and other operations requiring value comparison.
    /// It handles the most common value types used in range statistics.
    ///
    /// # Arguments
    ///
    /// * `other` - The other AnyValue to compare against
    ///
    /// # Returns
    ///
    /// * `-1` if self < other
    /// * `0` if self == other or types cannot be compared
    /// * `1` if self > other
    ///
    /// # Examples
    ///
    /// ```rust
    /// use amudai_format::defs::common::{AnyValue, any_value::Kind};
    ///
    /// let left = AnyValue {
    ///     kind: Some(Kind::I64Value(5)),
    ///     annotation: None,
    /// };
    /// let right = AnyValue {
    ///     kind: Some(Kind::I64Value(10)),
    ///     annotation: None,
    /// };
    ///
    /// let result = left.compare(&right).unwrap();
    /// assert_eq!(result, -1); // 5 < 10
    /// ```
    pub fn compare(&self, other: &AnyValue) -> Result<i32> {
        match (&self.kind, &other.kind) {
            (Some(Kind::I64Value(l)), Some(Kind::I64Value(r))) => Ok(l.cmp(r) as i32),
            (Some(Kind::DoubleValue(l)), Some(Kind::DoubleValue(r))) => {
                if l < r {
                    Ok(-1)
                } else if l > r {
                    Ok(1)
                } else {
                    Ok(0)
                }
            }
            (Some(Kind::U64Value(l)), Some(Kind::U64Value(r))) => Ok(l.cmp(r) as i32),
            (Some(Kind::StringValue(l)), Some(Kind::StringValue(r))) => Ok(l.cmp(r) as i32),
            (Some(Kind::BoolValue(l)), Some(Kind::BoolValue(r))) => Ok(l.cmp(r) as i32),
            (Some(Kind::DecimalValue(l)), Some(Kind::DecimalValue(r))) => {
                // Compare decimal values using their binary representation
                compare_decimal_bytes(l, r)
            }
            // Add more type comparisons as needed
            _ => {
                // For now, if we can't compare the types, consider them equal
                // This could be enhanced to handle more AnyValue types or return an error
                Ok(0)
            }
        }
    }

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

/// Converts a `i64` value into an `AnyValue`.
///
/// The resulting `AnyValue` will contain the signed 64-bit integer value
/// with no annotation.
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

/// Helper function to compare two decimal byte arrays by reconstructing d128 values.
///
/// This function converts the byte arrays back to d128 values and compares them numerically,
/// ensuring proper decimal comparison rather than lexicographic byte comparison.
///
/// # Arguments
/// * `left_bytes` - The byte array representing the left decimal value
/// * `right_bytes` - The byte array representing the right decimal value
///
/// # Returns
/// * `-1` if left < right
/// * `0` if left == right  
/// * `1` if left > right
/// * An error if the byte arrays are invalid
fn compare_decimal_bytes(left_bytes: &[u8], right_bytes: &[u8]) -> Result<i32> {
    // Validate byte array lengths
    if left_bytes.len() != 16 {
        return Err(amudai_common::error::Error::invalid_operation(format!(
            "Invalid decimal byte length: expected 16, got {}",
            left_bytes.len()
        )));
    }
    if right_bytes.len() != 16 {
        return Err(amudai_common::error::Error::invalid_operation(format!(
            "Invalid decimal byte length: expected 16, got {}",
            right_bytes.len()
        )));
    }

    // Convert byte arrays to fixed-size arrays
    let mut left_array = [0u8; 16];
    let mut right_array = [0u8; 16];
    left_array.copy_from_slice(left_bytes);
    right_array.copy_from_slice(right_bytes);

    // Reconstruct d128 values from raw bytes
    let left_decimal = unsafe { d128::from_raw_bytes(left_array) };
    let right_decimal = unsafe { d128::from_raw_bytes(right_array) };

    // Compare the d128 values numerically
    if left_decimal < right_decimal {
        Ok(-1)
    } else if left_decimal > right_decimal {
        Ok(1)
    } else {
        Ok(0)
    }
}

/// Standalone function for comparing two AnyValue instances.
///
/// This is a convenience function that provides the same functionality as the
/// `AnyValueExt::compare` method but as a standalone function. This is useful
/// when you need to pass a comparison function as a parameter or when you
/// prefer functional style.
///
/// # Arguments
///
/// * `left` - The first AnyValue to compare
/// * `right` - The second AnyValue to compare
///
/// # Returns
///
/// * `-1` if left < right
/// * `0` if left == right or types cannot be compared
/// * `1` if left > right
pub fn compare_any_values(left: &AnyValue, right: &AnyValue) -> Result<i32> {
    left.compare(right)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_i64_values() {
        let left = AnyValue {
            kind: Some(Kind::I64Value(5)),
            annotation: None,
        };
        let right = AnyValue {
            kind: Some(Kind::I64Value(10)),
            annotation: None,
        };

        assert_eq!(left.compare(&right).unwrap(), -1);
        assert_eq!(right.compare(&left).unwrap(), 1);
        assert_eq!(left.compare(&left).unwrap(), 0);
    }

    #[test]
    fn test_compare_double_values() {
        let left = AnyValue {
            kind: Some(Kind::DoubleValue(std::f64::consts::PI)),
            annotation: None,
        };
        let right = AnyValue {
            kind: Some(Kind::DoubleValue(2.71)),
            annotation: None,
        };

        assert_eq!(left.compare(&right).unwrap(), 1);
        assert_eq!(right.compare(&left).unwrap(), -1);
        assert_eq!(left.compare(&left).unwrap(), 0);
    }

    #[test]
    fn test_compare_u64_values() {
        let left = AnyValue {
            kind: Some(Kind::U64Value(100)),
            annotation: None,
        };
        let right = AnyValue {
            kind: Some(Kind::U64Value(200)),
            annotation: None,
        };

        assert_eq!(left.compare(&right).unwrap(), -1);
        assert_eq!(right.compare(&left).unwrap(), 1);
        assert_eq!(left.compare(&left).unwrap(), 0);
    }

    #[test]
    fn test_compare_string_values() {
        let left = AnyValue {
            kind: Some(Kind::StringValue("apple".to_string())),
            annotation: None,
        };
        let right = AnyValue {
            kind: Some(Kind::StringValue("banana".to_string())),
            annotation: None,
        };

        assert_eq!(left.compare(&right).unwrap(), -1);
        assert_eq!(right.compare(&left).unwrap(), 1);
        assert_eq!(left.compare(&left).unwrap(), 0);
    }

    #[test]
    fn test_compare_bool_values() {
        let left = AnyValue {
            kind: Some(Kind::BoolValue(false)),
            annotation: None,
        };
        let right = AnyValue {
            kind: Some(Kind::BoolValue(true)),
            annotation: None,
        };

        assert_eq!(left.compare(&right).unwrap(), -1);
        assert_eq!(right.compare(&left).unwrap(), 1);
        assert_eq!(left.compare(&left).unwrap(), 0);
    }

    #[test]
    fn test_compare_double_values_additional() {
        let left = AnyValue {
            kind: Some(Kind::DoubleValue(1.5)),
            annotation: None,
        };
        let right = AnyValue {
            kind: Some(Kind::DoubleValue(2.5)),
            annotation: None,
        };

        assert_eq!(left.compare(&right).unwrap(), -1);
        assert_eq!(right.compare(&left).unwrap(), 1);
        assert_eq!(left.compare(&left).unwrap(), 0);
    }

    #[test]
    fn test_compare_mismatched_types() {
        let left = AnyValue {
            kind: Some(Kind::I64Value(5)),
            annotation: None,
        };
        let right = AnyValue {
            kind: Some(Kind::StringValue("hello".to_string())),
            annotation: None,
        };

        // Mismatched types should return 0 (equal)
        assert_eq!(left.compare(&right).unwrap(), 0);
        assert_eq!(right.compare(&left).unwrap(), 0);
    }

    #[test]
    fn test_compare_none_values() {
        let left = AnyValue {
            kind: None,
            annotation: None,
        };
        let right = AnyValue {
            kind: Some(Kind::I64Value(5)),
            annotation: None,
        };

        // None kind should return 0 (equal)
        assert_eq!(left.compare(&right).unwrap(), 0);
        assert_eq!(right.compare(&left).unwrap(), 0);
        assert_eq!(left.compare(&left).unwrap(), 0);
    }

    #[test]
    fn test_standalone_compare_function() {
        let left = AnyValue {
            kind: Some(Kind::I64Value(5)),
            annotation: None,
        };
        let right = AnyValue {
            kind: Some(Kind::I64Value(10)),
            annotation: None,
        };

        assert_eq!(compare_any_values(&left, &right).unwrap(), -1);
        assert_eq!(compare_any_values(&right, &left).unwrap(), 1);
        assert_eq!(compare_any_values(&left, &left).unwrap(), 0);
    }

    #[test]
    fn test_compare_decimal_values() {
        use std::str::FromStr;

        // Create decimal AnyValues with binary representation
        let decimal1 = d128::from_str("123.45").unwrap();
        let decimal2 = d128::from_str("678.90").unwrap();
        let decimal3 = d128::from_str("123.45").unwrap(); // Same as decimal1

        let value1 = AnyValue {
            kind: Some(Kind::DecimalValue(decimal1.to_raw_bytes().to_vec())),
            annotation: None,
        };
        let value2 = AnyValue {
            kind: Some(Kind::DecimalValue(decimal2.to_raw_bytes().to_vec())),
            annotation: None,
        };
        let value3 = AnyValue {
            kind: Some(Kind::DecimalValue(decimal3.to_raw_bytes().to_vec())),
            annotation: None,
        };

        // Test comparisons
        assert_eq!(value1.compare(&value2).unwrap(), -1); // 123.45 < 678.90
        assert_eq!(value2.compare(&value1).unwrap(), 1); // 678.90 > 123.45
        assert_eq!(value1.compare(&value3).unwrap(), 0); // 123.45 == 123.45

        // Test with negative numbers
        let negative_decimal = d128::from_str("-50.25").unwrap();
        let negative_value = AnyValue {
            kind: Some(Kind::DecimalValue(negative_decimal.to_raw_bytes().to_vec())),
            annotation: None,
        };

        assert_eq!(negative_value.compare(&value1).unwrap(), -1); // -50.25 < 123.45
        assert_eq!(value1.compare(&negative_value).unwrap(), 1); // 123.45 > -50.25
    }
}
