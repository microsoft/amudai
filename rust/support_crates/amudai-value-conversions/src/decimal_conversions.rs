//! Decimal conversion utilities for Apache Arrow decimal arrays.
//!
//! This module provides functionality to convert between Arrow decimal types and
//! the `decimal::d128` type used throughout the Amudai system.

use amudai_decimal::d128;
use arrow_buffer;
use std::{io::Error, str::FromStr};

/// Converts an i128 value from Arrow Decimal128Array to a decimal::d128.
///
/// # Arguments
/// * `value` - The i128 value representing the decimal
/// * `scale` - The scale (number of digits after decimal point)
///
/// # Returns
/// A `Result` containing the converted `d128` value or an error
pub fn convert_i128_to_decimal(value: i128, scale: i8) -> Result<d128, Error> {
    // For i128, we need to handle the case where it might not fit in d128's range
    // First try to convert directly if it fits in i64 range
    if let Ok(value_i64) = i64::try_from(value) {
        let decimal = d128::from(value_i64);
        apply_decimal_scale(decimal, scale)
    } else {
        // Fall back to string conversion for values outside i64 range
        let value_str = value.to_string();
        let decimal = d128::from_str(&value_str).map_err(|e| {
            Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse i128 as decimal: {e:?}"),
            )
        })?;
        apply_decimal_scale(decimal, scale)
    }
}

/// Converts an i256 value from Arrow Decimal256Array to a decimal::d128.
///
/// Note: This is a simplified conversion as d128 may not represent the full
/// range of i256 values. In production, you might want to use a larger decimal type.
///
/// # Arguments
/// * `value` - The i256 value representing the decimal
/// * `scale` - The scale (number of digits after decimal point)
///
/// # Returns
/// A `Result` containing the converted `d128` value or an error
pub fn convert_i256_to_decimal(value: arrow_buffer::i256, scale: i8) -> Result<d128, Error> {
    // For i256, use string conversion since it's likely to exceed d128's range
    let value_str = value.to_string();
    let decimal = d128::from_str(&value_str).map_err(|e| {
        Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Failed to parse i256 as decimal: {e:?}"),
        )
    })?;

    apply_decimal_scale(decimal, scale)
}

/// Creates a decimal scale factor for the given scale value.
///
/// # Arguments
/// * `scale` - The scale value (positive for division, negative for multiplication)
///
/// # Returns
/// A `Result` containing the scale factor as a `d128` value
fn create_decimal_scale_factor(scale: i8) -> Result<d128, Error> {
    if scale == 0 {
        return Ok(d128::from(1));
    }

    let abs_scale = scale.unsigned_abs() as u32;
    let power_of_10 = 10_i128.pow(abs_scale);

    // Try to convert to i64 first, fallback to string if too large
    if let Ok(power_i64) = i64::try_from(power_of_10) {
        Ok(d128::from(power_i64))
    } else {
        d128::from_str(&power_of_10.to_string()).map_err(|e| {
            Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to create scale factor: {e:?}"),
            )
        })
    }
}

/// Applies a scale to a decimal value.
///
/// # Arguments
/// * `value` - The decimal value to scale
/// * `scale` - The scale to apply (positive for division, negative for multiplication)
///
/// # Returns
/// A `Result` containing the scaled decimal value
fn apply_decimal_scale(value: d128, scale: i8) -> Result<d128, Error> {
    use std::cmp::Ordering;

    match scale.cmp(&0) {
        Ordering::Equal => Ok(value),
        Ordering::Greater => {
            // Divide by 10^scale to get the actual decimal value
            let scale_factor = create_decimal_scale_factor(scale)?;
            Ok(value / scale_factor)
        }
        Ordering::Less => {
            // Negative scale means multiply by 10^(-scale)
            let scale_factor = create_decimal_scale_factor(-scale)?;
            Ok(value * scale_factor)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_i128_to_decimal() {
        assert_eq!(
            convert_i128_to_decimal(12345, 2).unwrap().to_string(),
            "123.45"
        );
        assert_eq!(
            convert_i128_to_decimal(12345, 0).unwrap().to_string(),
            "12345"
        );
        assert_eq!(
            convert_i128_to_decimal(-12345, 2).unwrap().to_string(),
            "-123.45"
        );
    }

    #[test]
    fn test_convert_i128_large_numbers() {
        // Test large i128 values that exceed i64 range but are within d128 precision
        let large_value = 12345678901234567890123456789i128;
        let result = convert_i128_to_decimal(large_value, 0).unwrap();
        // d128 may use scientific notation for very large numbers
        assert!(
            result.to_string().contains("12345678901234567890123456789")
                || result
                    .to_string()
                    .contains("1.2345678901234567890123456789E+28")
        );

        // Test large negative value
        let large_negative = -12345678901234567890123456789i128;
        let result = convert_i128_to_decimal(large_negative, 0).unwrap();
        assert!(
            result
                .to_string()
                .contains("-12345678901234567890123456789")
                || result
                    .to_string()
                    .contains("-1.2345678901234567890123456789E+28")
        );

        // Test large value with scale (original implementation uses apply_decimal_scale)
        let result = convert_i128_to_decimal(123456789012345678901234567890i128, 10).unwrap();
        // With apply_decimal_scale, this will be divided by 10^10
        let result_str = result.to_string();
        // Should contain the decimal representation (may lose some precision due to d128 limits)
        assert!(
            result_str.contains("12345678901234567890.123456789")
                || result_str.contains("1.2345678901234567890123456789E+19")
        );
    }

    #[test]
    fn test_convert_i128_high_scale() {
        // Test when scale >= number of digits (leading zeros)
        assert_eq!(
            convert_i128_to_decimal(123, 5).unwrap().to_string(),
            "0.00123"
        );
        assert_eq!(convert_i128_to_decimal(1, 3).unwrap().to_string(), "0.001");
        assert_eq!(
            convert_i128_to_decimal(-456, 6).unwrap().to_string(),
            "-0.000456"
        );
    }

    #[test]
    fn test_convert_i128_negative_scale() {
        // Negative scale means multiply by 10^(-scale)
        assert_eq!(
            convert_i128_to_decimal(123, -2).unwrap().to_string(),
            "12300"
        );
        assert_eq!(
            convert_i128_to_decimal(456, -3).unwrap().to_string(),
            "456000"
        );
        assert_eq!(
            convert_i128_to_decimal(-789, -1).unwrap().to_string(),
            "-7890"
        );
    }

    #[test]
    fn test_convert_i128_edge_cases() {
        // Zero
        let result = convert_i128_to_decimal(0, 2).unwrap();
        // d128 may not preserve trailing zeros in string representation
        assert!(result.to_string() == "0" || result.to_string() == "0.00");

        let result = convert_i128_to_decimal(0, 0).unwrap();
        assert_eq!(result.to_string(), "0");

        // Single digit
        assert_eq!(convert_i128_to_decimal(5, 1).unwrap().to_string(), "0.5");

        // Scale equals digit count
        assert_eq!(
            convert_i128_to_decimal(12345, 5).unwrap().to_string(),
            "0.12345"
        );
    }

    #[test]
    fn test_convert_i256_to_decimal() {
        use arrow_buffer::i256;

        // Basic positive number
        let value = i256::from_i128(12345);
        assert_eq!(
            convert_i256_to_decimal(value, 2).unwrap().to_string(),
            "123.45"
        );

        // Zero scale
        let value = i256::from_i128(12345);
        assert_eq!(
            convert_i256_to_decimal(value, 0).unwrap().to_string(),
            "12345"
        );

        // Negative number
        let value = i256::from_i128(-12345);
        assert_eq!(
            convert_i256_to_decimal(value, 2).unwrap().to_string(),
            "-123.45"
        );
    }

    #[test]
    fn test_convert_i256_large_numbers() {
        use arrow_buffer::i256;

        // Test with large i256 values that are within d128 precision range
        let large_value = i256::from_string("12345678901234567890123456789").unwrap();
        let result = convert_i256_to_decimal(large_value, 0).unwrap();
        // d128 may use scientific notation for very large numbers
        assert!(
            result.to_string().contains("12345678901234567890123456789")
                || result
                    .to_string()
                    .contains("1.2345678901234567890123456789E+28")
        );

        // Test large value with scale (original implementation uses apply_decimal_scale)
        let large_value = i256::from_string("123456789012345678901234567890").unwrap();
        let result = convert_i256_to_decimal(large_value, 10).unwrap();
        // With apply_decimal_scale, this will be divided by 10^10
        let result_str = result.to_string();
        // Should contain the decimal representation (may lose some precision due to d128 limits)
        assert!(
            result_str.contains("12345678901234567890.123456789")
                || result_str.contains("1.2345678901234567890123456789E+19")
        );
    }

    #[test]
    fn test_convert_i256_high_scale() {
        use arrow_buffer::i256;

        // Test when scale >= number of digits (leading zeros)
        let value = i256::from_i128(123);
        assert_eq!(
            convert_i256_to_decimal(value, 5).unwrap().to_string(),
            "0.00123"
        );

        let value = i256::from_i128(1);
        assert_eq!(
            convert_i256_to_decimal(value, 3).unwrap().to_string(),
            "0.001"
        );

        let value = i256::from_i128(-456);
        assert_eq!(
            convert_i256_to_decimal(value, 6).unwrap().to_string(),
            "-0.000456"
        );
    }

    #[test]
    fn test_convert_i256_negative_scale() {
        use arrow_buffer::i256;

        // Negative scale means multiply by 10^(-scale)
        let value = i256::from_i128(123);
        assert_eq!(
            convert_i256_to_decimal(value, -2).unwrap().to_string(),
            "12300"
        );

        let value = i256::from_i128(456);
        assert_eq!(
            convert_i256_to_decimal(value, -3).unwrap().to_string(),
            "456000"
        );

        let value = i256::from_i128(-789);
        assert_eq!(
            convert_i256_to_decimal(value, -1).unwrap().to_string(),
            "-7890"
        );
    }

    #[test]
    fn test_convert_i256_edge_cases() {
        use arrow_buffer::i256;

        // Zero
        let zero = i256::from_i128(0);
        let result = convert_i256_to_decimal(zero, 2).unwrap();
        // d128 may not preserve trailing zeros in string representation
        assert!(result.to_string() == "0" || result.to_string() == "0.00");

        let result = convert_i256_to_decimal(zero, 0).unwrap();
        assert_eq!(result.to_string(), "0");

        // Single digit
        let value = i256::from_i128(5);
        assert_eq!(
            convert_i256_to_decimal(value, 1).unwrap().to_string(),
            "0.5"
        );

        // Scale equals digit count
        let value = i256::from_i128(12345);
        assert_eq!(
            convert_i256_to_decimal(value, 5).unwrap().to_string(),
            "0.12345"
        );
    }

    #[test]
    fn test_decimal_precision_preservation() {
        // Test that precision is preserved when converting large numbers
        let large_i128 = 999999999999999999999999999999i128;
        let result = convert_i128_to_decimal(large_i128, 15).unwrap();
        assert_eq!(result.to_string(), "999999999999999.999999999999999");

        // Test edge case where all digits are after decimal point
        let value = 123456789i128;
        let result = convert_i128_to_decimal(value, 9).unwrap();
        assert_eq!(result.to_string(), "0.123456789");
    }
}
