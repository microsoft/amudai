//! Extensions for working with AnyValue instances.
//!
//! This module provides utility functions for AnyValue operations
//! such as comparison and ordering.

use crate::defs::common::{AnyValue, any_value::Kind};
use amudai_common::Result;

/// Extension trait for AnyValue providing comparison operations.
pub trait AnyValueExt {
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
    /// use amudai_format::defs::anyvalue_ext::AnyValueExt;
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
    fn compare(&self, other: &AnyValue) -> Result<i32>;
}

impl AnyValueExt for AnyValue {
    fn compare(&self, other: &AnyValue) -> Result<i32> {
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
            // Add more type comparisons as needed
            _ => {
                // For now, if we can't compare the types, consider them equal
                // This could be enhanced to handle more AnyValue types or return an error
                Ok(0)
            }
        }
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
}
