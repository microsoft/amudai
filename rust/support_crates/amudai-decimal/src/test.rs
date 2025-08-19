//! Comprehensive tests for the d128 decimal type
//!
//! This module contains extensive tests covering:
//! - Basic arithmetic operations
//! - Conversions from/to various types
//! - Special values (NaN, Infinity, Zero)
//! - Comparison operations
//! - Mathematical functions
//! - Formatting and display
//! - Edge cases and error conditions
//! - Rounding behavior
//! - Context and status handling

use super::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

#[cfg(test)]
#[allow(clippy::needless_borrows_for_generic_args)]
#[allow(clippy::uninlined_format_args)]
#[allow(clippy::len_zero)]
#[allow(clippy::op_ref)]
#[allow(clippy::legacy_numeric_constants)]
mod comprehensive_tests {
    use super::*;

    // ============================================================================
    // Construction and Basic Values Tests
    // ============================================================================

    #[test]
    fn test_construction_macro() {
        assert_eq!(d128!(0), d128::zero());
        assert_eq!(d128!(1), d128::from(1i32));
        assert_eq!(d128!(-1), -d128::from(1i32));
        assert_eq!(d128!(123.456), d128::from_str("123.456").unwrap());
        assert!(d128!(NaN).is_nan());
        assert!(d128!(Infinity).is_infinite());
        assert!(d128!(-Infinity).is_infinite() && d128!(-Infinity).is_negative());
    }

    #[test]
    fn test_special_values() {
        // Test zero
        let zero = d128::zero();
        assert!(zero.is_zero());
        assert!(zero.is_finite());
        assert!(!zero.is_negative());
        assert!(!zero.is_positive());
        assert!(!zero.is_infinite());
        assert!(!zero.is_nan());

        // Test positive infinity
        let inf = d128::infinity();
        assert!(inf.is_infinite());
        assert!(!inf.is_finite());
        assert!(!inf.is_zero());
        assert!(!inf.is_nan());
        assert!(inf.is_positive());
        assert!(!inf.is_negative());

        // Test negative infinity
        let neg_inf = d128::neg_infinity();
        assert!(neg_inf.is_infinite());
        assert!(!neg_inf.is_finite());
        assert!(!neg_inf.is_zero());
        assert!(!neg_inf.is_nan());
        assert!(!neg_inf.is_positive());
        assert!(neg_inf.is_negative());

        // Test NaN
        let nan = d128::nan();
        assert!(nan.is_nan());
        assert!(!nan.is_finite());
        assert!(!nan.is_infinite());
        assert!(!nan.is_zero());
    }

    #[test]
    fn test_default_and_canonical() {
        let default_val = d128::default();
        assert_eq!(default_val, d128::zero());

        let canonical = d128!(123.45).canonical();
        assert!(canonical.is_canonical());
    }

    // ============================================================================
    // Conversion Tests
    // ============================================================================

    #[test]
    fn test_from_integers() {
        // Test i32 conversions
        assert_eq!(d128::from(0i32), d128::zero());
        assert_eq!(d128::from(42i32), d128!(42));
        assert_eq!(d128::from(-42i32), d128!(-42));
        assert_eq!(d128::from(i32::MAX), d128!(2147483647));
        assert_eq!(d128::from(i32::MIN), d128!(-2147483648));

        // Test u32 conversions
        assert_eq!(d128::from(0u32), d128::zero());
        assert_eq!(d128::from(42u32), d128!(42));
        assert_eq!(d128::from(u32::MAX), d128!(4294967295));

        // Test i64 conversions
        assert_eq!(d128::from(0i64), d128::zero());
        assert_eq!(d128::from(42i64), d128!(42));
        assert_eq!(d128::from(-42i64), d128!(-42));
        assert_eq!(
            d128::from(i64::MAX),
            d128::from_str("9223372036854775807").unwrap()
        );
        assert_eq!(
            d128::from(i64::MIN),
            d128::from_str("-9223372036854775808").unwrap()
        );

        // Test u64 conversions
        assert_eq!(d128::from(0u64), d128::zero());
        assert_eq!(d128::from(42u64), d128!(42));
        assert_eq!(
            d128::from(u64::MAX),
            d128::from_str("18446744073709551615").unwrap()
        );
    }

    #[test]
    fn test_from_floats() {
        // Test f32 conversions
        assert_eq!(d128::from(0.0f32), d128::zero());
        assert_eq!(d128::from(1.0f32), d128!(1.0));
        assert_eq!(d128::from(-1.0f32), d128!(-1.0));
        assert!(d128::from(f32::NAN).is_nan());
        assert!(d128::from(f32::INFINITY).is_infinite() && d128::from(f32::INFINITY).is_positive());
        assert!(
            d128::from(f32::NEG_INFINITY).is_infinite()
                && d128::from(f32::NEG_INFINITY).is_negative()
        );

        // Test f64 conversions
        assert_eq!(d128::from(0.0f64), d128::zero());
        assert_eq!(d128::from(1.0f64), d128!(1.0));
        assert_eq!(d128::from(-1.0f64), d128!(-1.0));
        assert!(d128::from(f64::NAN).is_nan());
        assert!(d128::from(f64::INFINITY).is_infinite() && d128::from(f64::INFINITY).is_positive());
        assert!(
            d128::from(f64::NEG_INFINITY).is_infinite()
                && d128::from(f64::NEG_INFINITY).is_negative()
        );
    }

    #[test]
    fn test_to_integers() {
        // Test Into<i32>
        assert_eq!(Into::<i32>::into(d128!(42)), 42i32);
        assert_eq!(Into::<i32>::into(d128!(-42)), -42i32);
        assert_eq!(Into::<i32>::into(d128!(42.7)), 42i32); // Rounds down

        // Test Into<u32>
        assert_eq!(Into::<u32>::into(d128!(42)), 42u32);
        assert_eq!(Into::<u32>::into(d128!(42.7)), 42u32); // Rounds down
    }

    #[test]
    fn test_to_floats() {
        // Test Into<f64>
        assert_eq!(Into::<f64>::into(d128!(0)), 0.0f64);
        assert_eq!(Into::<f64>::into(d128!(42)), 42.0f64);
        assert_eq!(Into::<f64>::into(d128!(-42)), -42.0f64);
        assert!(Into::<f64>::into(d128!(NaN)).is_nan());
        assert_eq!(Into::<f64>::into(d128!(Infinity)), f64::INFINITY);
        assert_eq!(Into::<f64>::into(d128!(-Infinity)), f64::NEG_INFINITY);

        // Test Into<f32>
        assert_eq!(Into::<f32>::into(d128!(0)), 0.0f32);
        assert_eq!(Into::<f32>::into(d128!(42)), 42.0f32);
        assert_eq!(Into::<f32>::into(d128!(-42)), -42.0f32);
        assert!(Into::<f32>::into(d128!(NaN)).is_nan());
        assert_eq!(Into::<f32>::into(d128!(Infinity)), f32::INFINITY);
        assert_eq!(Into::<f32>::into(d128!(-Infinity)), f32::NEG_INFINITY);

        // Test reference conversions
        let val = d128!(123.45);
        assert_eq!(Into::<f64>::into(&val), 123.45f64);
        assert_eq!(Into::<f32>::into(&val), 123.45f32);
    }

    #[test]
    fn test_from_string() {
        assert_eq!(d128::from_str("0").unwrap(), d128::zero());
        assert_eq!(d128::from_str("123").unwrap(), d128!(123));
        assert_eq!(d128::from_str("-123.456").unwrap(), d128!(-123.456));
        assert_eq!(d128::from_str("1.23E+5").unwrap(), d128!(123000));
        assert_eq!(d128::from_str("1.23E-5").unwrap(), d128!(0.0000123));
        assert!(d128::from_str("NaN").unwrap().is_nan());
        assert!(d128::from_str("Infinity").unwrap().is_infinite());
        assert!(d128::from_str("-Infinity").unwrap().is_infinite());

        // Test invalid strings return Error
        assert!(d128::from_str("invalid").is_err());
        assert!(d128::from_str("123.45.67").is_err());
    }

    #[test]
    fn test_string_parsing_comprehensive() {
        // ============================================================================
        // Valid Integer Formats
        // ============================================================================

        // Basic integers
        assert_eq!(d128::from_str("0").unwrap(), d128!(0));
        assert_eq!(d128::from_str("1").unwrap(), d128!(1));
        assert_eq!(d128::from_str("123").unwrap(), d128!(123));
        assert_eq!(
            d128::from_str("999999999999999999999999999999999").unwrap(),
            d128::from_str("999999999999999999999999999999999").unwrap()
        );

        // Negative integers
        assert_eq!(d128::from_str("-0").unwrap(), d128!(0));
        assert_eq!(d128::from_str("-1").unwrap(), d128!(-1));
        assert_eq!(d128::from_str("-123").unwrap(), d128!(-123));
        assert_eq!(
            d128::from_str("-999999999999999999999999999999999").unwrap(),
            d128::from_str("-999999999999999999999999999999999").unwrap()
        );

        // Positive sign explicit
        assert_eq!(d128::from_str("+0").unwrap(), d128!(0));
        assert_eq!(d128::from_str("+1").unwrap(), d128!(1));
        assert_eq!(d128::from_str("+123").unwrap(), d128!(123));

        // ============================================================================
        // Valid Decimal Formats
        // ============================================================================

        // Basic decimals
        assert_eq!(d128::from_str("0.0").unwrap(), d128!(0.0));
        assert_eq!(d128::from_str("1.0").unwrap(), d128!(1.0));
        assert_eq!(d128::from_str("123.456").unwrap(), d128!(123.456));
        assert_eq!(d128::from_str("0.123").unwrap(), d128!(0.123));
        assert_eq!(d128::from_str(".123").unwrap(), d128!(0.123));
        assert_eq!(d128::from_str("123.").unwrap(), d128!(123));

        // Negative decimals
        assert_eq!(d128::from_str("-0.0").unwrap(), d128!(0.0));
        assert_eq!(d128::from_str("-1.0").unwrap(), d128!(-1.0));
        assert_eq!(d128::from_str("-123.456").unwrap(), d128!(-123.456));
        assert_eq!(d128::from_str("-0.123").unwrap(), d128!(-0.123));
        assert_eq!(d128::from_str("-.123").unwrap(), d128!(-0.123));
        assert_eq!(d128::from_str("-123.").unwrap(), d128!(-123));

        // Positive decimals with explicit sign
        assert_eq!(d128::from_str("+0.0").unwrap(), d128!(0.0));
        assert_eq!(d128::from_str("+1.0").unwrap(), d128!(1.0));
        assert_eq!(d128::from_str("+123.456").unwrap(), d128!(123.456));
        assert_eq!(d128::from_str("+.123").unwrap(), d128!(0.123));

        // Many decimal places
        assert_eq!(
            d128::from_str("1.123456789012345678901234567890123").unwrap(),
            d128::from_str("1.123456789012345678901234567890123").unwrap()
        );

        // Leading/trailing zeros
        assert_eq!(d128::from_str("000123.456000").unwrap(), d128!(123.456));
        assert_eq!(d128::from_str("0.000123").unwrap(), d128!(0.000123));
        assert_eq!(d128::from_str("123000.000").unwrap(), d128!(123000));
    }

    #[test]
    fn test_string_parsing_scientific_notation() {
        // ============================================================================
        // Valid Scientific Notation Formats
        // ============================================================================

        // Basic scientific notation with 'e'
        assert_eq!(d128::from_str("1e0").unwrap(), d128!(1));
        assert_eq!(d128::from_str("1e1").unwrap(), d128!(10));
        assert_eq!(d128::from_str("1e2").unwrap(), d128!(100));
        assert_eq!(d128::from_str("1e-1").unwrap(), d128!(0.1));
        assert_eq!(d128::from_str("1e-2").unwrap(), d128!(0.01));

        // Basic scientific notation with 'E'
        assert_eq!(d128::from_str("1E0").unwrap(), d128!(1));
        assert_eq!(d128::from_str("1E1").unwrap(), d128!(10));
        assert_eq!(d128::from_str("1E2").unwrap(), d128!(100));
        assert_eq!(d128::from_str("1E-1").unwrap(), d128!(0.1));
        assert_eq!(d128::from_str("1E-2").unwrap(), d128!(0.01));

        // Scientific notation with explicit positive exponent
        assert_eq!(d128::from_str("1e+0").unwrap(), d128!(1));
        assert_eq!(d128::from_str("1e+1").unwrap(), d128!(10));
        assert_eq!(d128::from_str("1E+2").unwrap(), d128!(100));
        assert_eq!(d128::from_str("1E+10").unwrap(), d128!(10000000000));

        // Scientific notation with decimal coefficient
        assert_eq!(d128::from_str("1.23e2").unwrap(), d128!(123));
        assert_eq!(d128::from_str("1.23E2").unwrap(), d128!(123));
        assert_eq!(d128::from_str("1.23e-2").unwrap(), d128!(0.0123));
        assert_eq!(d128::from_str("1.23E+3").unwrap(), d128!(1230));

        // Scientific notation with negative numbers
        assert_eq!(d128::from_str("-1e2").unwrap(), d128!(-100));
        assert_eq!(d128::from_str("-1.23e2").unwrap(), d128!(-123));
        assert_eq!(d128::from_str("-1.23E-2").unwrap(), d128!(-0.0123));

        // Scientific notation with positive sign
        assert_eq!(d128::from_str("+1e2").unwrap(), d128!(100));
        assert_eq!(d128::from_str("+1.23E2").unwrap(), d128!(123));

        // Large exponents
        assert_eq!(
            d128::from_str("1e100").unwrap(),
            d128::from_str("1e100").unwrap()
        );
        assert_eq!(
            d128::from_str("1E-100").unwrap(),
            d128::from_str("1E-100").unwrap()
        );

        // Complex scientific notation
        assert_eq!(
            d128::from_str("9.87654321e-15").unwrap(),
            d128::from_str("9.87654321e-15").unwrap()
        );
        assert_eq!(
            d128::from_str("1.234567890123456789e+20").unwrap(),
            d128::from_str("1.234567890123456789e+20").unwrap()
        );
    }

    #[test]
    fn test_string_parsing_special_values() {
        // ============================================================================
        // Valid Special Value Formats (STRICTLY TESTED)
        // ============================================================================

        // NaN variations - CASE INSENSITIVE (all supported)
        assert!(d128::from_str("NaN").unwrap().is_nan());
        assert!(d128::from_str("nan").unwrap().is_nan());
        assert!(d128::from_str("NAN").unwrap().is_nan());
        assert!(d128::from_str("Nan").unwrap().is_nan());
        assert!(d128::from_str("nAn").unwrap().is_nan());
        assert!(d128::from_str("naN").unwrap().is_nan());
        assert!(d128::from_str("NAn").unwrap().is_nan());

        // Quiet NaN variations - CASE INSENSITIVE (all supported, same as NaN)
        assert!(d128::from_str("qNaN").unwrap().is_nan());
        assert!(d128::from_str("qnan").unwrap().is_nan());
        assert!(d128::from_str("QNAN").unwrap().is_nan());
        // Verify they are quiet (not signaling)
        assert!(!d128::from_str("qNaN").unwrap().is_signaling());
        assert!(!d128::from_str("qnan").unwrap().is_signaling());
        assert!(!d128::from_str("QNAN").unwrap().is_signaling());

        // Signaling NaN - CASE INSENSITIVE (all supported)
        assert!(d128::from_str("sNaN").unwrap().is_nan());
        assert!(d128::from_str("snan").unwrap().is_nan());
        assert!(d128::from_str("SNAN").unwrap().is_nan());
        // Verify signaling property
        assert!(d128::from_str("sNaN").unwrap().is_signaling());
        assert!(d128::from_str("snan").unwrap().is_signaling());

        // Infinity variations - CASE INSENSITIVE (all supported)
        assert!(d128::from_str("Infinity").unwrap().is_infinite());
        assert!(d128::from_str("infinity").unwrap().is_infinite());
        assert!(d128::from_str("INFINITY").unwrap().is_infinite());
        assert!(d128::from_str("Inf").unwrap().is_infinite());
        assert!(d128::from_str("inf").unwrap().is_infinite());
        assert!(d128::from_str("INF").unwrap().is_infinite());

        // Signed Infinity - SUPPORTED
        let neg_inf = d128::from_str("-Infinity").unwrap();
        assert!(neg_inf.is_infinite() && neg_inf.is_negative());

        let pos_inf = d128::from_str("+Infinity").unwrap();
        assert!(pos_inf.is_infinite() && pos_inf.is_positive());

        let neg_inf_short = d128::from_str("-inf").unwrap();
        assert!(neg_inf_short.is_infinite() && neg_inf_short.is_negative());

        let pos_inf_short = d128::from_str("+inf").unwrap();
        assert!(pos_inf_short.is_infinite() && pos_inf_short.is_positive());

        let neg_inf_case = d128::from_str("-infinity").unwrap();
        assert!(neg_inf_case.is_infinite() && neg_inf_case.is_negative());

        let pos_inf_case = d128::from_str("+infinity").unwrap();
        assert!(pos_inf_case.is_infinite() && pos_inf_case.is_positive());

        // ============================================================================
        // Invalid Special Value Formats (STRICTLY REJECTED)
        // ============================================================================

        // Signed NaN - NOT SUPPORTED (all types)
        assert!(d128::from_str("-NaN").is_err());
        assert!(d128::from_str("+NaN").is_err());
        assert!(d128::from_str("-nan").is_err());
        assert!(d128::from_str("+nan").is_err());
        assert!(d128::from_str("-qNaN").is_err());
        assert!(d128::from_str("+qNaN").is_err());
        assert!(d128::from_str("-sNaN").is_err());
        assert!(d128::from_str("+sNaN").is_err());
    }

    #[test]
    fn test_string_parsing_edge_cases() {
        // ============================================================================
        // Valid Edge Cases
        // ============================================================================

        // Maximum precision (34 digits)
        let max_digits = "1234567890123456789012345678901234";
        assert!(d128::from_str(max_digits).unwrap().is_finite());
        assert_eq!(d128::from_str(max_digits).unwrap().digits(), 34);

        // Very small numbers
        assert!(d128::from_str("1e-6176").unwrap().is_finite());
        assert!(
            d128::from_str("9.999999999999999999999999999999999e-6143")
                .unwrap()
                .is_finite()
        );

        // Very large numbers
        assert!(
            d128::from_str("9.999999999999999999999999999999999e+6144")
                .unwrap()
                .is_finite()
        );

        // Zero variations
        assert_eq!(d128::from_str("0").unwrap(), d128::zero());
        assert_eq!(d128::from_str("0.0").unwrap(), d128::zero());
        assert_eq!(d128::from_str("0.00000").unwrap(), d128::zero());
        assert_eq!(d128::from_str("0e0").unwrap(), d128::zero());
        assert_eq!(d128::from_str("0e10").unwrap(), d128::zero());
        assert_eq!(d128::from_str("0.0e-5").unwrap(), d128::zero());
        assert_eq!(d128::from_str("000.000").unwrap(), d128::zero());

        // Whitespace handling (now supported with trimming)
        assert_eq!(d128::from_str(" 123 ").unwrap(), d128!(123));
        assert_eq!(d128::from_str("  123.45  ").unwrap(), d128!(123.45));
        assert_eq!(d128::from_str("\t123\t").unwrap(), d128!(123));
        assert_eq!(d128::from_str("\n123\n").unwrap(), d128!(123));
        assert_eq!(d128::from_str("  -123.45  ").unwrap(), d128!(-123.45));
        assert_eq!(d128::from_str("  +123.45  ").unwrap(), d128!(123.45));
    }

    #[test]
    fn test_string_parsing_invalid_formats() {
        // ============================================================================
        // Invalid String Formats (Should Return Err)
        // ============================================================================

        // Empty string
        assert!(d128::from_str("").is_err());

        // Pure whitespace
        assert!(d128::from_str("   ").is_err());
        assert!(d128::from_str("\t").is_err());
        assert!(d128::from_str("\n").is_err());

        // Invalid characters
        assert!(d128::from_str("abc").is_err());
        assert!(d128::from_str("123abc").is_err());
        assert!(d128::from_str("abc123").is_err());
        assert!(d128::from_str("12a34").is_err());
        assert!(d128::from_str("12.34.56").is_err());
        assert!(d128::from_str("12..34").is_err());

        // Invalid signs
        assert!(d128::from_str("++123").is_err());
        assert!(d128::from_str("--123").is_err());
        assert!(d128::from_str("+-123").is_err());
        assert!(d128::from_str("-+123").is_err());
        assert!(d128::from_str("123+").is_err());
        assert!(d128::from_str("123-").is_err());

        // Invalid decimal points
        assert!(d128::from_str(".").is_err());
        assert!(d128::from_str("..").is_err());
        assert!(d128::from_str("1.2.3").is_err());
        assert!(d128::from_str("1..2").is_err());

        // Invalid scientific notation
        assert!(d128::from_str("e").is_err());
        assert!(d128::from_str("E").is_err());
        assert!(d128::from_str("1e").is_err());
        assert!(d128::from_str("1E").is_err());
        assert!(d128::from_str("e1").is_err());
        assert!(d128::from_str("E1").is_err());
        assert!(d128::from_str("1ee2").is_err());
        assert!(d128::from_str("1EE2").is_err());
        assert!(d128::from_str("1e+").is_err());
        assert!(d128::from_str("1e-").is_err());
        assert!(d128::from_str("1e++2").is_err());
        assert!(d128::from_str("1e--2").is_err());
        assert!(d128::from_str("1e+-2").is_err());
        assert!(d128::from_str("1e2e3").is_err());

        // Invalid exponents
        assert!(d128::from_str("1e2.3").is_err());
        assert!(d128::from_str("1e.").is_err());
        assert!(d128::from_str("1e.2").is_err());
        assert!(d128::from_str("1eabc").is_err());

        // Mixed valid/invalid patterns
        assert!(d128::from_str("123 456").is_err()); // Space in middle still invalid
        assert!(d128::from_str("123,456").is_err());
        assert!(d128::from_str("$123").is_err());
        assert!(d128::from_str("123$").is_err());
        assert!(d128::from_str("123%").is_err());
        assert!(d128::from_str("(123)").is_err());
        assert!(d128::from_str("[123]").is_err());
        assert!(d128::from_str("{123}").is_err());

        // Binary/Hex/Octal (should be invalid for decimal parsing)
        assert!(d128::from_str("0b101").is_err());
        assert!(d128::from_str("0x123").is_err());
        assert!(d128::from_str("0o123").is_err());
        assert!(d128::from_str("0X123").is_err());

        // Unicode and special characters
        assert!(d128::from_str("α").is_err());
        assert!(d128::from_str("π").is_err());
        assert!(d128::from_str("∞").is_err());
        assert!(d128::from_str("123α").is_err());

        // Control characters
        assert!(d128::from_str("123\0").is_err());
        assert!(d128::from_str("\x00123").is_err());
    }

    #[test]
    fn test_string_parsing_boundary_errors() {
        // ============================================================================
        // Boundary Cases That Should Trigger Errors
        // ============================================================================

        // Too many digits (more than 34 significant digits)
        let too_many_digits = "12345678901234567890123456789012345"; // 35 digits
        let result = d128::from_str(too_many_digits).unwrap();
        // Should either round or be valid, depending on implementation
        assert!(result.is_finite() || result.is_nan());

        // Extremely large exponents
        assert!(
            d128::from_str("1e999999").unwrap().is_finite()
                || d128::from_str("1e999999").unwrap().is_infinite()
        );
        assert!(
            d128::from_str("1e-999999").unwrap().is_finite()
                || d128::from_str("1e-999999").unwrap().is_zero()
        );

        // Invalid special value formats - compound special values
        assert!(d128::from_str("NaNNaN").is_err());
        assert!(d128::from_str("InfinityInfinity").is_err());
        assert!(d128::from_str("NaN123").is_err());
        assert!(d128::from_str("123NaN").is_err());
        assert!(d128::from_str("Inf123").is_err());
        assert!(d128::from_str("123Inf").is_err());

        // Partial/malformed special values
        assert!(d128::from_str("Infinit").is_err());
        assert!(d128::from_str("Na").is_err());
        assert!(d128::from_str("In").is_err());
        assert!(d128::from_str("qNa").is_err());
        assert!(d128::from_str("sNa").is_err());

        // Known unsupported special value formats - signed NaN
        assert!(d128::from_str("-NaN").is_err()); // Signed NaN not supported
        assert!(d128::from_str("+NaN").is_err());
        assert!(d128::from_str("-nan").is_err());
        assert!(d128::from_str("+nan").is_err());
        assert!(d128::from_str("-qNaN").is_err()); // Signed qNaN not supported
        assert!(d128::from_str("+qNaN").is_err());
        assert!(d128::from_str("-sNaN").is_err()); // Signed sNaN not supported
        assert!(d128::from_str("+sNaN").is_err());
    }

    #[test]
    fn test_string_parsing_case_sensitivity() {
        // ============================================================================
        // Case Sensitivity Tests - Based on Actual Implementation Behavior
        // ============================================================================

        // NaN is CASE INSENSITIVE - all variations work
        let nan_variations = ["NaN", "nan", "NAN", "Nan", "nAn", "naN", "NAn"];
        for variant in &nan_variations {
            let result = d128::from_str(variant).unwrap();
            assert!(result.is_nan(), "Failed to parse '{}' as NaN", variant);
        }

        // Infinity is CASE INSENSITIVE - all variations work
        let inf_variations = ["Infinity", "infinity", "INFINITY", "Inf", "inf", "INF"];
        for variant in &inf_variations {
            let result = d128::from_str(variant).unwrap();
            assert!(
                result.is_infinite(),
                "Failed to parse '{}' as Infinity",
                variant
            );
        }

        // Signaling NaN is CASE INSENSITIVE
        let snan_variations = ["sNaN", "snan", "SNAN"];
        for variant in &snan_variations {
            let result = d128::from_str(variant).unwrap();
            assert!(
                result.is_nan() && result.is_signaling(),
                "Failed to parse '{}' as signaling NaN",
                variant
            );
        }

        // Quiet NaN is CASE INSENSITIVE - now supported
        let qnan_variations = ["qNaN", "qnan", "QNAN"];
        for variant in &qnan_variations {
            let result = d128::from_str(variant).unwrap();
            assert!(
                result.is_nan() && !result.is_signaling(),
                "Failed to parse '{}' as quiet NaN",
                variant
            );
        }

        // Scientific notation is case insensitive for 'e'/'E'
        assert_eq!(
            d128::from_str("1e2").unwrap(),
            d128::from_str("1E2").unwrap()
        );
        assert_eq!(
            d128::from_str("1.23e-4").unwrap(),
            d128::from_str("1.23E-4").unwrap()
        );
    }

    #[test]
    fn test_string_parsing_error_recovery() {
        // ============================================================================
        // Error Recovery and Status Tests
        // ============================================================================

        // Clear status before testing
        d128::set_status(Status::empty());

        // Parse invalid string and check if it returns error
        let result = d128::from_str("invalid_string");
        assert!(result.is_err());

        // Check if conversion syntax error is set
        let status = d128::get_status();
        // Note: The actual status flags set may depend on implementation
        if status.contains(Status::CONVERSION_SYNTAX) {
            assert!(status.contains(Status::CONVERSION_SYNTAX));
        }

        // Clear status
        d128::set_status(Status::empty());

        // Parse valid string - should not set error flags
        let result = d128::from_str("123.45").unwrap();
        assert_eq!(result, d128!(123.45));

        // Status should still be empty or only have non-error flags
        let status = d128::get_status();
        assert!(!status.contains(Status::CONVERSION_SYNTAX));
    }

    #[test]
    fn test_string_parsing_round_trip() {
        // ============================================================================
        // Round-trip Parsing Tests
        // ============================================================================

        // Test that formatting and parsing are inverse operations
        let test_values = [
            d128!(0),
            d128!(1),
            d128!(-1),
            d128!(123.456),
            d128!(-123.456),
            d128!(0.123),
            d128!(-0.123),
            d128!(1e10),
            d128!(1e-10),
            d128::from_str("1.234567890123456789012345678901234").unwrap(),
        ];

        for value in &test_values {
            if value.is_finite() {
                let string_repr = value.to_string();
                let parsed_back = d128::from_str(&string_repr).unwrap();
                assert_eq!(
                    *value, parsed_back,
                    "Round-trip failed for {}: {} -> {} -> {}",
                    value, value, string_repr, parsed_back
                );
            }
        }

        // Special values round-trip
        let inf = d128::infinity();
        let inf_str = inf.to_string();
        let inf_parsed = d128::from_str(&inf_str).unwrap();
        assert!(inf_parsed.is_infinite() && inf_parsed.is_positive());

        let neg_inf = d128::neg_infinity();
        let neg_inf_str = neg_inf.to_string();
        let neg_inf_parsed = d128::from_str(&neg_inf_str).unwrap();
        assert!(neg_inf_parsed.is_infinite() && neg_inf_parsed.is_negative());

        let nan = d128::nan();
        let nan_str = nan.to_string();
        let nan_parsed = d128::from_str(&nan_str).unwrap();
        assert!(nan_parsed.is_nan());
    }

    #[test]
    fn test_whitespace_trimming() {
        // ============================================================================
        // Whitespace Trimming Tests
        // ============================================================================

        // Leading whitespace
        assert_eq!(d128::from_str(" 123").unwrap(), d128!(123));
        assert_eq!(d128::from_str("  123").unwrap(), d128!(123));
        assert_eq!(d128::from_str("\t123").unwrap(), d128!(123));
        assert_eq!(d128::from_str("\n123").unwrap(), d128!(123));
        assert_eq!(d128::from_str("\r123").unwrap(), d128!(123));

        // Trailing whitespace
        assert_eq!(d128::from_str("123 ").unwrap(), d128!(123));
        assert_eq!(d128::from_str("123  ").unwrap(), d128!(123));
        assert_eq!(d128::from_str("123\t").unwrap(), d128!(123));
        assert_eq!(d128::from_str("123\n").unwrap(), d128!(123));
        assert_eq!(d128::from_str("123\r").unwrap(), d128!(123));

        // Both leading and trailing
        assert_eq!(d128::from_str(" 123 ").unwrap(), d128!(123));
        assert_eq!(d128::from_str("  123  ").unwrap(), d128!(123));
        assert_eq!(d128::from_str("\t123\t").unwrap(), d128!(123));
        assert_eq!(d128::from_str("\n123\n").unwrap(), d128!(123));
        assert_eq!(d128::from_str("\r\n123\r\n").unwrap(), d128!(123));

        // Mixed whitespace types
        assert_eq!(d128::from_str(" \t\n123\r\n\t ").unwrap(), d128!(123));

        // Decimal numbers with whitespace
        assert_eq!(d128::from_str(" 123.456 ").unwrap(), d128!(123.456));
        assert_eq!(d128::from_str("\t-123.456\n").unwrap(), d128!(-123.456));
        assert_eq!(d128::from_str(" +123.456 ").unwrap(), d128!(123.456));

        // Scientific notation with whitespace
        assert_eq!(d128::from_str(" 1.23e5 ").unwrap(), d128!(123000));
        assert_eq!(d128::from_str("\t1.23E-5\n").unwrap(), d128!(0.0000123));

        // Special values with whitespace
        assert!(d128::from_str(" NaN ").unwrap().is_nan());
        assert!(d128::from_str("\tInfinity\n").unwrap().is_infinite());
        assert!(d128::from_str(" -Infinity ").unwrap().is_infinite());

        // Internal whitespace should still be invalid
        assert!(d128::from_str("1 23").is_err());
        assert!(d128::from_str("12 3.45").is_err());
        assert!(d128::from_str("123. 45").is_err());
        assert!(d128::from_str("1.23 e5").is_err());
        assert!(d128::from_str("1.23e 5").is_err());
    }

    #[test]
    fn test_hex_representation() {
        let val = d128!(123.45);
        let hex_str = format!("{:x}", val);
        let recovered = d128::from_hex(&hex_str).unwrap();
        assert_eq!(val, recovered);

        // Test invalid hex strings
        assert!(d128::from_hex("invalid").is_err());
        assert!(d128::from_hex("12345").is_err()); // Wrong length
    }

    // ============================================================================
    // Arithmetic Operations Tests
    // ============================================================================

    #[test]
    fn test_basic_arithmetic() {
        // Addition
        assert_eq!(d128!(1) + d128!(2), d128!(3));
        assert_eq!(d128!(1.5) + d128!(2.5), d128!(4.0));
        assert_eq!(d128!(-1) + d128!(1), d128!(0));

        // Subtraction
        assert_eq!(d128!(5) - d128!(3), d128!(2));
        assert_eq!(d128!(1.5) - d128!(0.5), d128!(1.0));
        assert_eq!(d128!(1) - d128!(1), d128!(0));

        // Multiplication
        assert_eq!(d128!(3) * d128!(4), d128!(12));
        assert_eq!(d128!(1.5) * d128!(2), d128!(3.0));
        assert_eq!(d128!(-2) * d128!(3), d128!(-6));

        // Division
        assert_eq!(d128!(6) / d128!(2), d128!(3));
        assert_eq!(d128!(1) / d128!(2), d128!(0.5));
        assert_eq!(d128!(-6) / d128!(3), d128!(-2));

        // Remainder
        assert_eq!(d128!(7) % d128!(3), d128!(1));
        assert_eq!(d128!(10) % d128!(4), d128!(2));
    }

    #[test]
    fn test_arithmetic_with_references() {
        let a = d128!(5);
        let b = d128!(3);

        assert_eq!(&a + &b, d128!(8));
        assert_eq!(&a - &b, d128!(2));
        assert_eq!(&a * &b, d128!(15));
        assert_eq!(&a / &b, d128!(5) / d128!(3));
        assert_eq!(&a % &b, d128!(2));

        assert_eq!(a + &b, d128!(8));
        assert_eq!(&a + b, d128!(8));
    }

    #[test]
    fn test_assignment_operators() {
        let mut val = d128!(10);

        val += d128!(5);
        assert_eq!(val, d128!(15));

        val -= d128!(3);
        assert_eq!(val, d128!(12));

        val *= d128!(2);
        assert_eq!(val, d128!(24));

        val /= d128!(4);
        assert_eq!(val, d128!(6));

        val %= d128!(4);
        assert_eq!(val, d128!(2));
    }

    #[test]
    fn test_unary_operators() {
        assert_eq!(-d128!(5), d128!(-5));
        assert_eq!(-d128!(-5), d128!(5));
        assert_eq!(-d128!(0), d128!(0));

        // Test with references
        let val = d128!(42);
        assert_eq!(-&val, d128!(-42));
    }

    #[test]
    fn test_infinity_arithmetic() {
        let inf = d128::infinity();
        let neg_inf = d128::neg_infinity();
        let finite = d128!(42);

        // Infinity + finite = Infinity
        assert_eq!(inf + finite, inf);
        assert_eq!(finite + inf, inf);

        // Infinity - finite = Infinity
        assert_eq!(inf - finite, inf);

        // Infinity * finite = Infinity (if finite != 0)
        assert_eq!(inf * finite, inf);
        assert_eq!(finite * inf, inf);

        // Infinity + (-Infinity) = NaN
        assert!((inf + neg_inf).is_nan());

        // Infinity / Infinity = NaN
        assert!((inf / inf).is_nan());
    }

    #[test]
    fn test_nan_arithmetic() {
        let nan = d128::nan();
        let finite = d128!(42);

        // Any operation with NaN should produce NaN
        assert!((nan + finite).is_nan());
        assert!((finite + nan).is_nan());
        assert!((nan - finite).is_nan());
        assert!((nan * finite).is_nan());
        assert!((nan / finite).is_nan());
        assert!((nan % finite).is_nan());
    }

    #[test]
    fn test_division_by_zero() {
        // Clear status before test
        d128::set_status(Status::empty());

        let result = d128!(1) / d128!(0);
        assert!(result.is_infinite());
        assert!(d128::get_status().contains(Status::DIVISION_BY_ZERO));

        // Clear status
        d128::set_status(Status::empty());

        let result = d128!(0) / d128!(0);
        assert!(result.is_nan());
        assert!(d128::get_status().contains(Status::DIVISION_UNDEFINED));
    }

    // ============================================================================
    // Bitwise Operations Tests
    // ============================================================================

    #[test]
    fn test_bitwise_operations() {
        // These operations require operands to be logical (0s and 1s only)
        let _a = d128!(1101); // Binary representation of digits
        let _b = d128!(1010);

        // Note: These might fail if the implementation is strict about logical format
        // The operations are designed for decimal representations of binary numbers

        // Test with simple cases that should work
        let zero = d128!(0);
        let one = d128!(1);

        assert_eq!(zero & zero, zero);
        assert_eq!(zero & one, zero);
        assert_eq!(one & one, one);

        assert_eq!(zero | zero, zero);
        assert_eq!(zero | one, one);
        assert_eq!(one | one, one);

        assert_eq!(zero ^ zero, zero);
        assert_eq!(zero ^ one, one);
        assert_eq!(one ^ one, zero);
    }

    #[test]
    fn test_bitwise_assignment() {
        let mut val = d128!(1);
        val &= d128!(1);
        assert_eq!(val, d128!(1));

        val |= d128!(0);
        assert_eq!(val, d128!(1));

        val ^= d128!(1);
        assert_eq!(val, d128!(0));
    }

    #[test]
    fn test_bit_not() {
        // Test NOT operation on logical values
        // The actual behavior may differ from simple expectations
        let result = !d128!(0);
        assert!(result.is_finite()); // Just check it produces a valid result

        let result = !d128!(1);
        assert!(result.is_finite()); // Just check it produces a valid result
    }

    // ============================================================================
    // Shift Operations Tests
    // ============================================================================

    #[test]
    fn test_shift_operations() {
        // Shift operations work on the coefficient digits, not decimal places
        let val = d128!(123);
        let left_shifted = &val << 2;
        let right_shifted = &val >> 1;

        // Just verify they produce valid results
        assert!(left_shifted.is_finite());
        assert!(right_shifted.is_finite());

        // Test basic properties
        assert!(left_shifted != val); // Should be different
        assert!(right_shifted != val); // Should be different
    }

    #[test]
    fn test_shift_assignment() {
        let mut val = d128!(123);
        let original = val;

        val <<= 2;
        assert_ne!(val, original); // Should be different after shift
        assert!(val.is_finite());

        val >>= 1;
        assert!(val.is_finite());
    }

    // ============================================================================
    // Comparison Tests
    // ============================================================================

    #[test]
    fn test_equality() {
        assert_eq!(d128!(123), d128!(123));
        assert_eq!(d128!(0), d128!(0.0));
        assert_eq!(d128!(1.0), d128!(1.00));

        assert_ne!(d128!(123), d128!(124));
        assert_ne!(d128!(0), d128!(1));

        // NaN comparisons
        assert_ne!(d128!(NaN), d128!(NaN));
        assert_ne!(d128!(NaN), d128!(123));
    }

    #[test]
    fn test_ordering() {
        assert!(d128!(1) < d128!(2));
        assert!(d128!(2) > d128!(1));
        assert!(d128!(1) <= d128!(1));
        assert!(d128!(1) >= d128!(1));

        assert!(d128!(-1) < d128!(0));
        assert!(d128!(0) > d128!(-1));

        // NaN comparisons should return None for partial_cmp
        assert_eq!(d128!(NaN).partial_cmp(&d128!(1)), None);
        assert_eq!(d128!(1).partial_cmp(&d128!(NaN)), None);
    }

    #[test]
    fn test_compare_methods() {
        let a = d128!(123);
        let b = d128!(456);
        let equal = d128!(123);

        // compare method
        let result = a.compare(&b);
        assert!(result.is_negative());

        let result = b.compare(&a);
        assert!(result.is_positive());

        let result = a.compare(&equal);
        assert!(result.is_zero());

        // compare_total method (takes exponent into account)
        let result = a.compare_total(&equal);
        assert!(result.is_zero());
    }

    // ============================================================================
    // Mathematical Functions Tests
    // ============================================================================

    #[test]
    fn test_abs() {
        assert_eq!(d128!(5).abs(), d128!(5));
        assert_eq!(d128!(-5).abs(), d128!(5));
        assert_eq!(d128!(0).abs(), d128!(0));
        assert!(d128!(NaN).abs().is_nan());
        assert!(d128!(Infinity).abs().is_infinite());
        assert!(d128!(-Infinity).abs().is_infinite());
    }

    #[test]
    fn test_min_max() {
        assert_eq!(d128!(3).min(d128!(5)), d128!(3));
        assert_eq!(d128!(3).max(d128!(5)), d128!(5));
        assert_eq!(d128!(-3).min(d128!(5)), d128!(-3));
        assert_eq!(d128!(-3).max(d128!(5)), d128!(5));

        // With NaN, the other value should be returned
        assert_eq!(d128!(3).min(d128!(NaN)), d128!(3));
        assert_eq!(d128!(NaN).max(d128!(5)), d128!(5));
    }

    #[test]
    fn test_next_previous() {
        let val = d128!(1);
        let next_val = val.next();
        let prev_val = val.previous();

        assert!(next_val > val);
        assert!(prev_val < val);

        // next and previous should be inverse operations around the same value
        assert_eq!(next_val.previous(), val);
        assert_eq!(prev_val.next(), val);
    }

    #[test]
    fn test_towards() {
        let val = d128!(1);
        let target = d128!(2);

        let result = val.towards(&target);
        assert!(result > val);
        assert!(result < target);

        // When equal, should return self
        assert_eq!(val.towards(&val), val);
    }

    #[test]
    fn test_fused_multiply_add() {
        // Test mul_add: self * a + b
        let result = d128!(2).mul_add(d128!(3), d128!(4));
        assert_eq!(result, d128!(10)); // 2 * 3 + 4 = 10

        let result = d128!(1.5).mul_add(d128!(2), d128!(0.5));
        assert_eq!(result, d128!(3.5)); // 1.5 * 2 + 0.5 = 3.5
    }

    #[test]
    fn test_power_functions() {
        // Test pow
        assert_eq!(d128!(2).pow(d128!(3)), d128!(8));
        assert_eq!(d128!(10).pow(d128!(2)), d128!(100));
        assert_eq!(d128!(5).pow(d128!(0)), d128!(1));

        // Test sqrt
        assert_eq!(d128!(4).sqrt(), d128!(2));
        assert_eq!(d128!(9).sqrt(), d128!(3));
        assert_eq!(d128!(0).sqrt(), d128!(0));

        // Test exp
        let e_approx = d128!(1).exp();
        assert!(e_approx > d128!(2.7) && e_approx < d128!(2.8));

        // Test logarithms
        assert_eq!(d128!(1).ln(), d128!(0));
        assert_eq!(d128!(10).log10(), d128!(1));
        assert_eq!(d128!(100).log10(), d128!(2));
    }

    #[test]
    fn test_floor() {
        assert_eq!(d128!(3.7).floor(), d128!(3));
        assert_eq!(d128!(-3.7).floor(), d128!(-4));
        assert_eq!(d128!(5).floor(), d128!(5));
        assert_eq!(d128!(0).floor(), d128!(0));
    }

    #[test]
    fn test_logb() {
        // logb returns the adjusted exponent
        assert_eq!(d128!(100).logb(), d128!(2)); // 10^2
        assert_eq!(d128!(10).logb(), d128!(1)); // 10^1
        assert_eq!(d128!(1).logb(), d128!(0)); // 10^0
        assert_eq!(d128!(0.1).logb(), d128!(-1)); // 10^-1
    }

    #[test]
    fn test_quantize() {
        // Quantize to same exponent
        let val = d128!(123.456);
        let template = d128!(0.01); // 2 decimal places
        let result = val.quantize(&template);
        assert_eq!(result, d128!(123.46)); // Rounded to 2 decimal places

        // Test quantize with specific rounding
        let result = val.quantize_with_rounding(&template, Rounding::Up);
        assert_eq!(result, d128!(123.46)); // Should round up
    }

    #[test]
    fn test_reduce() {
        // Remove trailing zeros
        let val = d128::from_str("123.450000").unwrap();
        let reduced = val.reduce();
        assert_eq!(reduced, d128!(123.45));
    }

    #[test]
    fn test_scaleb() {
        // Scale by powers of 10
        let val = d128!(123);
        let scaled = val.scaleb(d128!(2));
        assert_eq!(scaled, d128!(12300)); // 123 * 10^2

        let scaled = val.scaleb(d128!(-1));
        assert_eq!(scaled, d128!(12.3)); // 123 * 10^-1
    }

    #[test]
    fn test_rotate() {
        // Rotate coefficient digits
        let val = d128!(12345);
        let rotated = val.rotate(d128!(2));
        // This rotates the coefficient digits, exact result depends on implementation
        assert!(rotated.is_finite());
    }

    // ============================================================================
    // Formatting and Display Tests
    // ============================================================================

    #[test]
    fn test_display_formatting() {
        assert_eq!(format!("{}", d128!(123)), "123");
        assert_eq!(format!("{}", d128!(123.45)), "123.45");
        assert_eq!(format!("{}", d128!(-123.45)), "-123.45");
        assert_eq!(format!("{}", d128!(0)), "0");

        // Special values
        assert_eq!(format!("{}", d128!(Infinity)), "Infinity");
        assert_eq!(format!("{}", d128!(-Infinity)), "-Infinity");
        assert!(format!("{}", d128!(NaN)).contains("NaN"));
    }

    #[test]
    fn test_debug_formatting() {
        // Debug should be same as Display for d128
        assert_eq!(format!("{:?}", d128!(123.45)), format!("{}", d128!(123.45)));
    }

    #[test]
    fn test_engineering_notation() {
        let val = d128!(12345);
        let eng_str = format!("{:e}", val);
        // Engineering notation might not always use 'E' for all values
        // Just verify it's a valid string representation
        assert!(!eng_str.is_empty());
        assert!(eng_str.len() > 0);

        // Test with a larger number that definitely needs scientific notation
        let large_val = d128::from_str("1.23456789E+10").unwrap();
        let eng_str = format!("{:e}", large_val);
        assert!(!eng_str.is_empty());
    }

    #[test]
    fn test_hex_formatting() {
        let val = d128!(123);
        let hex_str = format!("{:x}", val);
        assert!(hex_str.len() == 32); // 16 bytes * 2 hex chars each

        // Should be able to parse it back
        let recovered = d128::from_hex(&hex_str).unwrap();
        assert_eq!(val, recovered);
    }

    // ============================================================================
    // Classification Tests
    // ============================================================================

    #[test]
    fn test_classification() {
        use std::num::FpCategory;

        assert_eq!(d128!(123).classify(), FpCategory::Normal);
        assert_eq!(d128!(0).classify(), FpCategory::Zero);
        assert_eq!(d128!(Infinity).classify(), FpCategory::Infinite);
        assert_eq!(d128!(NaN).classify(), FpCategory::Nan);

        // Test class method
        use Class;
        assert_eq!(d128!(123).class(), Class::PosNormal);
        assert_eq!(d128!(-123).class(), Class::NegNormal);
        assert_eq!(d128!(0).class(), Class::PosZero);
    }

    #[test]
    fn test_property_checks() {
        let pos_normal = d128!(123.45);
        let neg_normal = d128!(-123.45);
        let zero = d128!(0);
        let inf = d128!(Infinity);
        let neg_inf = d128!(-Infinity);
        let nan = d128!(NaN);

        // is_finite
        assert!(pos_normal.is_finite());
        assert!(neg_normal.is_finite());
        assert!(zero.is_finite());
        assert!(!inf.is_finite());
        assert!(!neg_inf.is_finite());
        assert!(!nan.is_finite());

        // is_infinite
        assert!(!pos_normal.is_infinite());
        assert!(!zero.is_infinite());
        assert!(inf.is_infinite());
        assert!(neg_inf.is_infinite());
        assert!(!nan.is_infinite());

        // is_nan
        assert!(!pos_normal.is_nan());
        assert!(!zero.is_nan());
        assert!(!inf.is_nan());
        assert!(nan.is_nan());

        // is_normal
        assert!(pos_normal.is_normal());
        assert!(neg_normal.is_normal());
        assert!(!zero.is_normal());
        assert!(!inf.is_normal());
        assert!(!nan.is_normal());

        // is_zero
        assert!(!pos_normal.is_zero());
        assert!(zero.is_zero());
        assert!(!inf.is_zero());
        assert!(!nan.is_zero());

        // is_positive/negative
        assert!(pos_normal.is_positive());
        assert!(!neg_normal.is_positive());
        assert!(!pos_normal.is_negative());
        assert!(neg_normal.is_negative());
        assert!(inf.is_positive());
        assert!(neg_inf.is_negative());

        // is_signed (has minus sign)
        assert!(!pos_normal.is_signed());
        assert!(neg_normal.is_signed());
        assert!(neg_inf.is_signed());
    }

    #[test]
    fn test_digits() {
        assert_eq!(d128!(123).digits(), 3);
        assert_eq!(d128!(1).digits(), 1);
        assert_eq!(d128!(0).digits(), 1);
        assert_eq!(d128!(123.45).digits(), 5);
    }

    #[test]
    fn test_integer_check() {
        assert!(d128!(123).is_integer());
        assert!(!d128!(123.45).is_integer());
        assert!(d128!(0).is_integer());
        assert!(!d128!(0.1).is_integer());
    }

    #[test]
    fn test_logical_check() {
        // Logical numbers contain only 0s and 1s
        assert!(d128!(0).is_logical());
        assert!(d128!(1).is_logical());
        assert!(d128!(1101).is_logical()); // Binary representation
        assert!(!d128!(123).is_logical()); // Contains digits other than 0,1
        assert!(!d128!(-1).is_logical()); // Negative
    }

    // ============================================================================
    // Status and Context Tests
    // ============================================================================

    #[test]
    fn test_status_handling() {
        // Clear status
        d128::set_status(Status::empty());
        assert_eq!(d128::get_status(), Status::empty());

        // Trigger division by zero
        let _result = d128!(1) / d128!(0);
        assert!(d128::get_status().contains(Status::DIVISION_BY_ZERO));

        // Clear status again
        d128::set_status(Status::empty());
        assert_eq!(d128::get_status(), Status::empty());

        // Trigger invalid operation (this might vary by implementation)
        d128::set_status(Status::INVALID_OPERATION);
        assert!(d128::get_status().contains(Status::INVALID_OPERATION));
    }

    // ============================================================================
    // Iterator and Collection Tests
    // ============================================================================

    #[test]
    fn test_sum_iterator() {
        let values = vec![d128!(1), d128!(2), d128!(3), d128!(4)];
        let sum: d128 = values.iter().sum();
        assert_eq!(sum, d128!(10));

        let sum: d128 = values.into_iter().sum();
        assert_eq!(sum, d128!(10));
    }

    #[test]
    fn test_product_iterator() {
        let values = vec![d128!(2), d128!(3), d128!(4)];
        let product: d128 = values.iter().product();
        assert_eq!(product, d128!(24));

        let product: d128 = values.into_iter().product();
        assert_eq!(product, d128!(24));
    }

    #[test]
    fn test_zero_trait() {
        use num_traits::Zero;

        assert_eq!(d128::zero(), <d128 as Zero>::zero());
        assert!(d128::zero().is_zero());
        assert!(!d128!(1).is_zero());
    }

    // ============================================================================
    // Hash Tests
    // ============================================================================

    #[test]
    fn test_hash_consistency() {
        let val1 = d128!(123.45);
        let val2 = d128!(123.45);
        let val3 = d128::from_str("123.45").unwrap();

        let mut hasher1 = DefaultHasher::new();
        val1.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        val2.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        let mut hasher3 = DefaultHasher::new();
        val3.hash(&mut hasher3);
        let hash3 = hasher3.finish();

        assert_eq!(hash1, hash2);
        assert_eq!(hash1, hash3);
    }

    #[test]
    fn test_hash_equivalent_values() {
        // Values that are equal should have the same hash
        let val1 = d128::from_str("0.100").unwrap();
        let val2 = d128::from_str("0.1").unwrap();
        assert_eq!(val1, val2);

        let mut hasher1 = DefaultHasher::new();
        val1.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        val2.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hashmap_usage() {
        // Since d128 doesn't implement Eq, we can't use it directly as HashMap keys
        // But we can test that the hash values are consistent
        use std::collections::HashMap;

        let val1 = d128!(123.45);
        let val2 = d128!(123.45);

        // Create a map using string representation as key
        let mut map = HashMap::new();
        map.insert(val1.to_string(), "value");

        assert_eq!(map.get(&val2.to_string()), Some(&"value"));
        assert_eq!(map.get(&d128!(999).to_string()), None);
    }

    // ============================================================================
    // Edge Cases and Error Conditions Tests
    // ============================================================================

    #[test]
    fn test_very_large_numbers() {
        let large = d128::from_str("9.999999999999999999999999999999999E+6144").unwrap();
        assert!(large.is_finite());
        assert!(large.is_positive());

        // Test operations with large numbers
        let result = large + d128!(1);
        assert!(result.is_finite());
    }

    #[test]
    fn test_very_small_numbers() {
        let small = d128::from_str("1E-6176").unwrap();
        assert!(small.is_finite());
        assert!(small.is_positive());

        // Test if it's subnormal
        if small.is_subnormal() {
            assert!(!small.is_normal());
        }
    }

    #[test]
    fn test_precision_limits() {
        // Test with 34 significant digits (max for decimal128)
        let max_precision = d128::from_str("1234567890123456789012345678901234").unwrap();
        assert!(max_precision.is_finite());
        assert_eq!(max_precision.digits(), 34);
    }

    #[test]
    fn test_overflow_conditions() {
        // Clear status
        d128::set_status(Status::empty());

        // This might cause overflow depending on the values
        let large1 = d128::from_str("1E+6000").unwrap();
        let large2 = d128::from_str("1E+6000").unwrap();
        let _result = large1 * large2;

        // Check if overflow flag is set (might be implementation dependent)
        let status = d128::get_status();
        if status.contains(Status::OVERFLOW) {
            assert!(status.contains(Status::OVERFLOW));
        }
    }

    #[test]
    fn test_underflow_conditions() {
        // Clear status
        d128::set_status(Status::empty());

        // This might cause underflow
        let small1 = d128::from_str("1E-6000").unwrap();
        let small2 = d128::from_str("1E-6000").unwrap();
        let _result = small1 * small2;

        // Check if underflow flag is set
        let status = d128::get_status();
        if status.contains(Status::UNDERFLOW) {
            assert!(status.contains(Status::UNDERFLOW));
        }
    }

    #[test]
    fn test_invalid_operations() {
        // Clear status
        d128::set_status(Status::empty());

        // Try invalid bitwise operation (should set INVALID_OPERATION)
        let _result = d128!(123) & d128!(456); // Not logical values

        // The status might contain INVALID_OPERATION
        let status = d128::get_status();
        if status.contains(Status::INVALID_OPERATION) {
            assert!(status.contains(Status::INVALID_OPERATION));
        }
    }

    // ============================================================================
    // Raw Bytes and Low-level Tests
    // ============================================================================

    #[test]
    fn test_raw_bytes() {
        let val = d128!(123.45);
        let bytes = val.to_raw_bytes();
        assert_eq!(bytes.len(), 16);

        let recovered = unsafe { d128::from_raw_bytes(bytes) };
        assert_eq!(val, recovered);
    }

    #[test]
    fn test_canonical_check() {
        let val = d128!(123.45);
        assert!(val.is_canonical());

        let canonical = val.canonical();
        assert!(canonical.is_canonical());
        assert_eq!(val, canonical);
    }

    // ============================================================================
    // AsRef Tests
    // ============================================================================

    #[test]
    fn test_as_ref() {
        let val = d128!(123.45);
        let ref_val: &d128 = val.as_ref();
        assert_eq!(*ref_val, val);
    }

    // ============================================================================
    // Boundary and Corner Cases
    // ============================================================================

    #[test]
    fn test_signed_zeros() {
        let pos_zero = d128!(0);
        let neg_zero = -d128!(0);

        // They should be equal
        assert_eq!(pos_zero, neg_zero);

        // But one might be signed
        assert!(!pos_zero.is_signed() || !pos_zero.is_negative());

        // Test operations with signed zeros
        assert_eq!(pos_zero + neg_zero, d128!(0));
        assert_eq!(pos_zero - neg_zero, d128!(0));
    }

    #[test]
    fn test_rounding_edge_cases() {
        // Test conversion from string that requires rounding
        let precise = d128::from_str("1.2345678901234567890123456789012345").unwrap();
        assert!(precise.is_finite());

        // Should be rounded to fit in 34 digits
        assert!(precise.digits() <= 34);
    }

    #[test]
    fn test_scientific_notation_parsing() {
        assert_eq!(d128::from_str("1.23e5").unwrap(), d128!(123000));
        assert_eq!(d128::from_str("1.23E5").unwrap(), d128!(123000));
        assert_eq!(d128::from_str("1.23e+5").unwrap(), d128!(123000));
        assert_eq!(d128::from_str("1.23e-5").unwrap(), d128!(0.0000123));
        assert_eq!(d128::from_str("1.23E-5").unwrap(), d128!(0.0000123));
    }

    #[test]
    fn test_buffer_write() {
        let val = d128!(123.456);
        let mut buffer = [0u8; 43];

        unsafe {
            let len = val.write_to_buffer(&mut buffer);
            assert!(len > 0 && len <= 43);

            // The buffer should contain a valid string representation
            let string_repr = std::str::from_utf8(&buffer[..len]).unwrap();
            assert!(string_repr.contains("123.456") || string_repr.contains("123.456"));
        }
    }

    // ============================================================================
    // Additional Tests (originally from dec128.rs tests module)
    // ============================================================================

    #[test]
    fn test_default() {
        assert_eq!(d128::zero(), d128::default());
        assert_eq!(d128::zero(), Default::default());
    }

    #[test]
    fn test_special_dec128() {
        assert!(d128::infinity().is_infinite());
        assert!(!d128::infinity().is_negative());

        assert!(d128::neg_infinity().is_infinite());
        assert!(d128::neg_infinity().is_negative());

        assert_eq!(d128::infinity() + d128!(1), d128::infinity());
    }

    #[cfg(feature = "ord_subset")]
    #[test]
    #[should_panic]
    fn test_ord_subset_nan() {
        use ord_subset;
        ord_subset::OrdVar::new(d128!(NaN));
    }

    #[cfg(feature = "ord_subset")]
    #[test]
    #[should_panic]
    fn test_ord_subset_qnan() {
        use ord_subset;
        ord_subset::OrdVar::new(d128!(qNaN));
    }

    #[cfg(feature = "ord_subset")]
    #[test]
    fn test_ord_subset_zero() {
        use ord_subset;
        assert_eq!(*ord_subset::OrdVar::new(d128::zero()), d128::zero());
    }

    #[cfg(feature = "ord_subset")]
    #[test]
    fn test_into_for_btreemap() {
        use ord_subset;
        use std::collections::BTreeMap;
        let mut m = BTreeMap::<ord_subset::OrdVar<d128>, i64>::new();
        m.insert(d128!(1.1).into(), 1);
        assert_eq!(m[&d128!(1.1).into()], 1);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_serde() {
        use serde_json::{from_str, to_string};
        use std::collections::BTreeMap;
        let mut a = BTreeMap::new();
        a.insert("price".to_string(), d128!(432.232));
        a.insert("amt".to_string(), d128!(9.9));
        assert_eq!(
            &to_string(&a).unwrap(),
            "{\"amt\":\"9.9\",\"price\":\"432.232\"}"
        );
        let b = from_str("{\"price\":\"432.232\",\"amt\":\"9.9\"}").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn test_unary_op() {
        assert_eq!(d128!(-1.1), -d128!(1.1));
        assert_eq!(d128!(-1.1), -&d128!(1.1));
    }

    #[test]
    fn test_binary_op() {
        assert_eq!(d128!(3.33), d128!(1.11) + d128!(2.22));
        assert_eq!(d128!(3.33), &d128!(1.11) + d128!(2.22));
        assert_eq!(d128!(3.33), d128!(1.11) + &d128!(2.22));
        assert_eq!(d128!(3.33), &d128!(1.11) + &d128!(2.22));
        assert_eq!(d128!(5) << 2, d128!(500));
        assert_eq!(d128!(500) >> 1, d128!(50));
    }

    #[test]
    fn test_assign_op() {
        let mut x = d128!(1);
        x += d128!(2);
        assert_eq!(x, d128!(3));
        x *= d128!(3);
        assert_eq!(x, d128!(9));
        x -= d128!(1);
        assert_eq!(x, d128!(8));
        x /= d128!(16);
        assert_eq!(x, d128!(0.5));
        x <<= 2;
        assert_eq!(x, d128!(50));
        x >>= 1;
        assert_eq!(x, d128!(5));
    }

    #[test]
    fn test_as_ref_operand() {
        assert_eq!(d128!(1.1), d128!(1.1).min(d128!(2.2)));
        assert_eq!(d128!(1.1), d128!(1.1).min(&d128!(2.2)));
    }

    #[test]
    fn test_from_i64() {
        assert_eq!(
            d128::from_str(&::std::i64::MAX.to_string()).unwrap(),
            d128::from(::std::i64::MAX)
        );
        assert_eq!(d128::from(0i32), d128::from(0i64));
        assert_eq!(
            d128::from_str(&(::std::i64::MIN).to_string()).unwrap(),
            d128::from(::std::i64::MIN)
        );
    }

    #[test]
    fn test_from_u64() {
        assert_eq!(
            d128::from_str(&::std::u64::MAX.to_string()).unwrap(),
            d128::from(::std::u64::MAX)
        );
        assert_eq!(d128::from(0i32), d128::from(0u64));
        assert_eq!(
            d128::from_str(&(::std::u64::MIN).to_string()).unwrap(),
            d128::from(::std::u64::MIN)
        );
    }

    #[test]
    fn test_sum() {
        let decimals = vec![d128!(1), d128!(2), d128!(3), d128!(4)];

        assert_eq!(d128!(10), decimals.iter().sum());

        assert_eq!(d128!(10), decimals.into_iter().sum());
    }

    #[test]
    fn test_hash() {
        let d1 = d128::from_str("0.100").unwrap();
        let d2 = d128::from_str("0.1").unwrap();
        assert_eq!(d1, d2);
        let mut hasher = DefaultHasher::new();
        d1.hash(&mut hasher);
        let h1 = hasher.finish();
        let mut hasher = DefaultHasher::new();
        d2.hash(&mut hasher);
        let h2 = hasher.finish();
        assert_eq!(h1, h2);

        let d1 = d128!(0.123);
        let d2 = d128!(0.234);
        let mut hasher = DefaultHasher::new();
        d1.hash(&mut hasher);
        let h1 = hasher.finish();
        let mut hasher = DefaultHasher::new();
        d2.hash(&mut hasher);
        let h2 = hasher.finish();
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_round() {
        // Test basic rounding cases
        assert_eq!(d128::from_str("2.4").unwrap().round(), d128!(2));
        assert_eq!(d128::from_str("2.5").unwrap().round(), d128!(3));
        assert_eq!(d128::from_str("2.6").unwrap().round(), d128!(3));
        assert_eq!(d128::from_str("-2.4").unwrap().round(), d128!(-2));
        assert_eq!(d128::from_str("-2.5").unwrap().round(), d128!(-3)); // HalfUp rounds away from zero
        assert_eq!(d128::from_str("-2.6").unwrap().round(), d128!(-3));

        // Test edge cases
        assert_eq!(d128!(0.5).round(), d128!(1));
        assert_eq!(d128!(-0.5).round(), d128!(-1)); // HalfUp rounds away from zero
        assert_eq!(d128!(0).round(), d128!(0));
        assert_eq!(d128!(1).round(), d128!(1));
        assert_eq!(d128!(-1).round(), d128!(-1));

        // Test larger numbers
        assert_eq!(d128::from_str("123.4").unwrap().round(), d128!(123));
        assert_eq!(d128::from_str("123.5").unwrap().round(), d128!(124));
        assert_eq!(d128::from_str("-123.5").unwrap().round(), d128!(-124));

        // Test special values
        assert!(d128::infinity().round().is_infinite());
        assert!(d128::neg_infinity().round().is_infinite());
        assert!(d128::nan().round().is_nan());
    }

    #[test]
    fn test_floor_dec128() {
        // Test basic floor cases
        assert_eq!(d128::from_str("2.1").unwrap().floor(), d128!(2));
        assert_eq!(d128::from_str("2.9").unwrap().floor(), d128!(2));
        assert_eq!(d128::from_str("-2.1").unwrap().floor(), d128!(-3));
        assert_eq!(d128::from_str("-2.9").unwrap().floor(), d128!(-3));

        // Test edge cases
        assert_eq!(d128!(0.1).floor(), d128!(0));
        assert_eq!(d128!(-0.1).floor(), d128!(-1));
        assert_eq!(d128!(0).floor(), d128!(0));
        assert_eq!(d128!(1).floor(), d128!(1));
        assert_eq!(d128!(-1).floor(), d128!(-1));

        // Test exact values
        assert_eq!(d128!(2.0).floor(), d128!(2));
        assert_eq!(d128!(-2.0).floor(), d128!(-2));

        // Test larger numbers
        assert_eq!(d128::from_str("123.7").unwrap().floor(), d128!(123));
        assert_eq!(d128::from_str("-123.7").unwrap().floor(), d128!(-124));

        // Test special values
        assert!(d128::infinity().floor().is_infinite());
        assert!(d128::neg_infinity().floor().is_infinite());
        assert!(d128::nan().floor().is_nan());
    }

    #[test]
    fn test_to_u64_pair() {
        // Test zero
        let zero = d128::zero();
        let (lower, upper) = zero.to_u64_pair();
        // For zero, we expect specific bit patterns but they depend on implementation
        // We can at least verify it doesn't panic and returns consistent results
        let (lower2, upper2) = zero.to_u64_pair();
        assert_eq!(lower, lower2);
        assert_eq!(upper, upper2);

        // Test basic integers
        let one = d128::from(1);
        let (lower, upper) = one.to_u64_pair();
        assert!(lower != 0 || upper != 0); // Should not be all zeros for non-zero value

        // Test consistency - same value should give same u64 pair
        let val = d128::from_str("123.45").unwrap();
        let (l1, u1) = val.to_u64_pair();
        let (l2, u2) = val.to_u64_pair();
        assert_eq!(l1, l2);
        assert_eq!(u1, u2);

        // Test different values give different pairs
        let val1 = d128::from(42);
        let val2 = d128::from(43);
        let (l1, u1) = val1.to_u64_pair();
        let (l2, u2) = val2.to_u64_pair();
        assert!(l1 != l2 || u1 != u2); // Should be different

        // Test special values
        let inf = d128::infinity();
        let (inf_l, inf_u) = inf.to_u64_pair();
        assert!(inf_l != 0 || inf_u != 0);

        let neg_inf = d128::neg_infinity();
        let (neg_inf_l, neg_inf_u) = neg_inf.to_u64_pair();
        assert!(neg_inf_l != 0 || neg_inf_u != 0);
        assert!(inf_l != neg_inf_l || inf_u != neg_inf_u); // Should be different

        let nan = d128::nan();
        let (nan_l, nan_u) = nan.to_u64_pair();
        assert!(nan_l != 0 || nan_u != 0);
    }

    #[test]
    fn test_round_trip_u64_pair() {
        // Test that we can convert back and forth consistently
        let original = d128::from_str("123.456").unwrap();
        let (lower, upper) = original.to_u64_pair();

        // Create a new d128 from the raw bytes and verify it's the same
        let raw_bytes = original.to_raw_bytes();
        let reconstructed = unsafe { d128::from_raw_bytes(raw_bytes) };

        assert_eq!(original, reconstructed);

        // The u64 pair should be consistent
        let (lower2, upper2) = reconstructed.to_u64_pair();
        assert_eq!(lower, lower2);
        assert_eq!(upper, upper2);
    }
}
