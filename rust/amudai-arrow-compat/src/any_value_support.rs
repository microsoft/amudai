//! Convenience methods for working with `AnyValue`.

use amudai_format::defs::common::{AnyValue, any_value::Kind};

/// A trait for converting a type into an `AnyValue`.
///
/// This trait provides a generic way to convert various Rust types into the
/// `AnyValue` enum used by the Amudai shard format.
pub trait ToAnyValue {
    /// Converts the implementing type into an `AnyValue`.
    fn to_any_value(&self) -> AnyValue;
}

/// Implements `ToAnyValue` for `i64`.
///
/// Converts an `i64` into an `AnyValue` with the `Kind::I64Value` variant.
impl ToAnyValue for i64 {
    fn to_any_value(&self) -> AnyValue {
        AnyValue {
            annotation: None,
            kind: Some(Kind::I64Value(*self)),
        }
    }
}

/// Implements `ToAnyValue` for `i32`.
///
/// Converts an `i32` into an `AnyValue` with the `Kind::I64Value` variant, casting to `i64`.
impl ToAnyValue for i32 {
    fn to_any_value(&self) -> AnyValue {
        AnyValue {
            annotation: None,
            kind: Some(Kind::I64Value(*self as i64)),
        }
    }
}

/// Implements `ToAnyValue` for `i16`.
///
/// Converts an `i16` into an `AnyValue` with the `Kind::I64Value` variant, casting to `i64`.
impl ToAnyValue for i16 {
    fn to_any_value(&self) -> AnyValue {
        AnyValue {
            annotation: None,
            kind: Some(Kind::I64Value(*self as i64)),
        }
    }
}

/// Implements `ToAnyValue` for `i8`.
///
/// Converts an `i8` into an `AnyValue` with the `Kind::I64Value` variant, casting to `i64`.
impl ToAnyValue for i8 {
    fn to_any_value(&self) -> AnyValue {
        AnyValue {
            annotation: None,
            kind: Some(Kind::I64Value(*self as i64)),
        }
    }
}

/// Implements `ToAnyValue` for `u64`.
///
/// Converts a `u64` into an `AnyValue` with the `Kind::U64Value` variant.
impl ToAnyValue for u64 {
    fn to_any_value(&self) -> AnyValue {
        AnyValue {
            annotation: None,
            kind: Some(Kind::U64Value(*self)),
        }
    }
}

/// Implements `ToAnyValue` for `u32`.
///
/// Converts a `u32` into an `AnyValue` with the `Kind::U64Value` variant, casting to `u64`.
impl ToAnyValue for u32 {
    fn to_any_value(&self) -> AnyValue {
        AnyValue {
            annotation: None,
            kind: Some(Kind::U64Value(*self as u64)),
        }
    }
}

/// Implements `ToAnyValue` for `u16`.
///
/// Converts a `u16` into an `AnyValue` with the `Kind::U64Value` variant, casting to `u64`.
impl ToAnyValue for u16 {
    fn to_any_value(&self) -> AnyValue {
        AnyValue {
            annotation: None,
            kind: Some(Kind::U64Value(*self as u64)),
        }
    }
}

/// Implements `ToAnyValue` for `u8`.
///
/// Converts a `u8` into an `AnyValue` with the `Kind::U64Value` variant, casting to `u64`.
impl ToAnyValue for u8 {
    fn to_any_value(&self) -> AnyValue {
        AnyValue {
            annotation: None,
            kind: Some(Kind::U64Value(*self as u64)),
        }
    }
}

/// Implements `ToAnyValue` for `f32`.
///
/// Converts an `f32` into an `AnyValue` with the `Kind::DoubleValue` variant, casting to `f64`.
impl ToAnyValue for f32 {
    fn to_any_value(&self) -> AnyValue {
        AnyValue {
            annotation: None,
            kind: Some(Kind::DoubleValue(*self as f64)),
        }
    }
}

/// Implements `ToAnyValue` for `f64`.
///
/// Converts an `f64` into an `AnyValue` with the `Kind::DoubleValue` variant.
impl ToAnyValue for f64 {
    fn to_any_value(&self) -> AnyValue {
        AnyValue {
            annotation: None,
            kind: Some(Kind::DoubleValue(*self)),
        }
    }
}

/// Implements `ToAnyValue` for `str`.
///
/// Converts a `str` into an `AnyValue` with the `Kind::StringValue` variant.
impl ToAnyValue for str {
    fn to_any_value(&self) -> AnyValue {
        AnyValue {
            annotation: None,
            kind: Some(Kind::StringValue(self.to_string())),
        }
    }
}

/// Implements `ToAnyValue` for `[u8]`.
///
/// Converts a byte slice `[u8]` into an `AnyValue` with the `Kind::BytesValue` variant.
impl ToAnyValue for [u8] {
    fn to_any_value(&self) -> AnyValue {
        AnyValue {
            annotation: None,
            kind: Some(Kind::BytesValue(self.to_vec())),
        }
    }
}

/// Implements `ToAnyValue` for `bool`.
///
/// Converts a `bool` into an `AnyValue` with the `Kind::BoolValue` variant.
impl ToAnyValue for bool {
    fn to_any_value(&self) -> AnyValue {
        AnyValue {
            annotation: None,
            kind: Some(Kind::BoolValue(*self)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_i64_to_any_value() {
        let value: i64 = 12345;
        let any_value = value.to_any_value();
        assert_eq!(any_value.kind, Some(Kind::I64Value(12345)));
    }

    #[test]
    fn test_i32_to_any_value() {
        let value: i32 = 6789;
        let any_value = value.to_any_value();
        assert_eq!(any_value.kind, Some(Kind::I64Value(6789)));
    }

    #[test]
    fn test_i16_to_any_value() {
        let value: i16 = 1011;
        let any_value = value.to_any_value();
        assert_eq!(any_value.kind, Some(Kind::I64Value(1011)));
    }

    #[test]
    fn test_i8_to_any_value() {
        let value: i8 = 12;
        let any_value = value.to_any_value();
        assert_eq!(any_value.kind, Some(Kind::I64Value(12)));
    }

    #[test]
    fn test_u64_to_any_value() {
        let value: u64 = 54321;
        let any_value = value.to_any_value();
        assert_eq!(any_value.kind, Some(Kind::U64Value(54321)));
    }

    #[test]
    fn test_u32_to_any_value() {
        let value: u32 = 9876;
        let any_value = value.to_any_value();
        assert_eq!(any_value.kind, Some(Kind::U64Value(9876)));
    }

    #[test]
    fn test_u16_to_any_value() {
        let value: u16 = 1100;
        let any_value = value.to_any_value();
        assert_eq!(any_value.kind, Some(Kind::U64Value(1100)));
    }

    #[test]
    fn test_u8_to_any_value() {
        let value: u8 = 255;
        let any_value = value.to_any_value();
        assert_eq!(any_value.kind, Some(Kind::U64Value(255)));
    }

    #[test]
    fn test_f32_to_any_value() {
        let value: f32 = 2.14;
        let any_value = value.to_any_value();
        let d = 2.14f32 as f64;
        assert_eq!(any_value.kind, Some(Kind::DoubleValue(d)));
    }

    #[test]
    fn test_f64_to_any_value() {
        let value: f64 = 5.71828;
        let any_value = value.to_any_value();
        assert_eq!(any_value.kind, Some(Kind::DoubleValue(5.71828)));
    }

    #[test]
    fn test_str_to_any_value() {
        let value = "hello";
        let any_value = value.to_any_value();
        assert_eq!(any_value.kind, Some(Kind::StringValue("hello".to_string())));
    }

    #[test]
    fn test_bytes_to_any_value() {
        let value: [u8; 4] = [0x01, 0x02, 0x03, 0x04];
        let any_value = value.to_any_value();
        assert_eq!(
            any_value.kind,
            Some(Kind::BytesValue(vec![0x01, 0x02, 0x03, 0x04]))
        );
    }

    #[test]
    fn test_bool_to_any_value() {
        let value = true;
        let any_value = value.to_any_value();
        assert_eq!(any_value.kind, Some(Kind::BoolValue(true)));

        let value = false;
        let any_value = value.to_any_value();
        assert_eq!(any_value.kind, Some(Kind::BoolValue(false)));
    }
}
