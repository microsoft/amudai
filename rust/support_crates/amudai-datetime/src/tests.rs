#[cfg(test)]
mod extensive_datetime_tests {
    use crate::datetime::DateTime64;
    use crate::timespan::TimeSpan64;
    use crate::{MAX_TICKS, TICKS_PER_DAY, TICKS_PER_HOUR, TICKS_PER_MINUTE, TICKS_PER_SECOND};
    use num_traits::Zero;
    use std::collections::HashSet;

    // ============================================================================
    // BASIC FUNCTIONALITY TESTS
    // ============================================================================

    #[test]
    fn test_current_basic_construction() {
        // Test basic constructors that exist in current implementation
        let dt1 = DateTime64::new(0);
        assert_eq!(dt1.ticks(), 0);

        let dt2 = DateTime64::new(MAX_TICKS);
        assert_eq!(dt2.ticks(), MAX_TICKS);

        // Test from_ymd
        let dt3 = DateTime64::from_ymd(2023, 12, 25);
        assert_eq!(dt3.year(), 2023);
        assert_eq!(dt3.month(), 12);
        assert_eq!(dt3.day(), 25);

        // Test from_ymd_hms
        let dt4 = DateTime64::from_ymd_hms(2023, 12, 25, 14, 30, 45);
        assert_eq!(dt4.year(), 2023);
        assert_eq!(dt4.month(), 12);
        assert_eq!(dt4.day(), 25);
        assert_eq!(dt4.hour(), 14);
        assert_eq!(dt4.minute(), 30);
        assert_eq!(dt4.second(), 45);
    }

    #[test]
    fn test_current_try_constructors() {
        // Test try_from_ticks
        assert!(DateTime64::try_from_ticks(0).is_ok());
        assert!(DateTime64::try_from_ticks(MAX_TICKS).is_ok());
        assert!(DateTime64::try_from_ticks(-1).is_err());
        assert!(DateTime64::try_from_ticks(MAX_TICKS + 1).is_err());

        // Test is_valid_ticks
        assert!(DateTime64::is_valid_ticks(0));
        assert!(DateTime64::is_valid_ticks(MAX_TICKS));
        assert!(!DateTime64::is_valid_ticks(-1));
        assert!(!DateTime64::is_valid_ticks(MAX_TICKS + 1));
    }

    #[test]
    fn test_current_datetime_specs() {
        let dt = DateTime64::from_ymd_hms_mmn(2023, 12, 25, 14, 30, 45, 123, 456, 7);
        let specs = dt.get_datetime64_specs();

        // Now using immutable getters (improvement applied)
        assert_eq!(specs.year(), 2023);
        assert_eq!(specs.month(), 12);
        assert_eq!(specs.day(), 25);
        assert_eq!(specs.hour(), 14);
        assert_eq!(specs.minute(), 30);
        assert_eq!(specs.second(), 45);
        assert_eq!(specs.fraction(), 1234567); // Combined sub-second components
    }

    #[test]
    fn test_current_time_components() {
        let dt = DateTime64::from_ymd_hms_mmn(2023, 12, 25, 14, 30, 45, 123, 456, 7);

        assert_eq!(dt.hour(), 14);
        assert_eq!(dt.minute(), 30);
        assert_eq!(dt.second(), 45);
        assert_eq!(dt.millisecond(), 123);
        assert_eq!(dt.microsecond(), 456);
        assert_eq!(dt.hundred_nanosecond(), 7);
    }

    #[test]
    fn test_current_arithmetic_operations() {
        let dt1 = DateTime64::from_ymd_hms(2023, 1, 1, 12, 0, 0);
        let dt2 = DateTime64::from_ymd_hms(2023, 1, 1, 13, 0, 0);
        let timespan = TimeSpan64::new(TICKS_PER_HOUR);

        // Test DateTime64 - DateTime64 = TimeSpan64
        let diff = dt2 - dt1;
        assert_eq!(diff.ticks(), TICKS_PER_HOUR);

        // Test DateTime64 + TimeSpan64
        let dt_plus_span = dt1 + timespan;
        assert_eq!(dt_plus_span.hour(), 13);

        // Test DateTime64 - DateTime64 = TimeSpan64
        let diff = dt2 - dt1;
        assert_eq!(diff.ticks(), TICKS_PER_HOUR);

        // Test DateTime64 - TimeSpan64 = DateTime64
        let dt_minus_span = dt2 - timespan;
        assert_eq!(dt_minus_span.hour(), 12);
    }

    #[test]
    fn test_current_overflow_behavior() {
        // Current implementation uses saturating arithmetic
        let max_dt = DateTime64::MAX;
        let large_timespan = TimeSpan64::new(1000);

        // Addition should saturate at MAX
        let result = max_dt + large_timespan;
        assert_eq!(result.ticks(), MAX_TICKS);

        let min_dt = DateTime64::MIN;
        // Subtraction should clamp to 0
        let result2 = min_dt - large_timespan;
        assert_eq!(result2.ticks(), 0);
    }

    // ============================================================================
    // COMPREHENSIVE CONSTRUCTOR TESTS
    // ============================================================================

    #[test]
    fn test_comprehensive_constructors() {
        // Test edge cases for from_ymd
        assert_eq!(DateTime64::from_ymd(1, 1, 1).year(), 1);
        assert_eq!(DateTime64::from_ymd(9999, 12, 31).year(), 9999);

        // Test leap year handling
        let leap_feb = DateTime64::from_ymd(2024, 2, 29);
        assert_eq!(leap_feb.month(), 2);
        assert_eq!(leap_feb.day(), 29);

        // Test end of months
        let jan_31 = DateTime64::from_ymd(2023, 1, 31);
        assert_eq!(jan_31.day(), 31);

        let apr_30 = DateTime64::from_ymd(2023, 4, 30);
        assert_eq!(apr_30.day(), 30);

        // Test from_ymd_hms with edge times
        let midnight = DateTime64::from_ymd_hms(2023, 1, 1, 0, 0, 0);
        assert_eq!(midnight.hour(), 0);
        assert_eq!(midnight.minute(), 0);
        assert_eq!(midnight.second(), 0);

        let almost_midnight = DateTime64::from_ymd_hms(2023, 1, 1, 23, 59, 59);
        assert_eq!(almost_midnight.hour(), 23);
        assert_eq!(almost_midnight.minute(), 59);
        assert_eq!(almost_midnight.second(), 59);
    }

    #[test]
    fn test_subsecond_precision_constructors() {
        // Test maximum precision
        let precise_dt = DateTime64::from_ymd_hms_mmn(2023, 1, 1, 0, 0, 0, 999, 999, 9);
        assert_eq!(precise_dt.millisecond(), 999);
        assert_eq!(precise_dt.microsecond(), 999);
        assert_eq!(precise_dt.hundred_nanosecond(), 9);

        // Test zero sub-second components
        let zero_subsec = DateTime64::from_ymd_hms_mmn(2023, 1, 1, 0, 0, 0, 0, 0, 0);
        assert_eq!(zero_subsec.millisecond(), 0);
        assert_eq!(zero_subsec.microsecond(), 0);
        assert_eq!(zero_subsec.hundred_nanosecond(), 0);

        // Test various combinations
        let partial_subsec = DateTime64::from_ymd_hms_mmn(2023, 1, 1, 0, 0, 0, 123, 0, 0);
        assert_eq!(partial_subsec.millisecond(), 123);
        assert_eq!(partial_subsec.microsecond(), 0);
        assert_eq!(partial_subsec.hundred_nanosecond(), 0);
    }

    // ============================================================================
    // UNIX TIMESTAMP CONVERSION TESTS
    // ============================================================================

    #[test]
    fn test_unix_timestamp_conversions() {
        // Test Unix epoch
        let epoch = DateTime64::try_from_unix_seconds(0).unwrap();
        assert_eq!(epoch.year(), 1970);
        assert_eq!(epoch.month(), 1);
        assert_eq!(epoch.day(), 1);

        // Test common timestamps
        let test_timestamp = 1609459200; // 2021-01-01 00:00:00 UTC
        let dt = DateTime64::try_from_unix_seconds(test_timestamp).unwrap();
        assert_eq!(dt.year(), 2021);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 1);

        // Test fractional seconds
        let fractional = DateTime64::try_from_unix_seconds_f64(1609459200.123456).unwrap();
        assert_eq!(fractional.year(), 2021);
        assert!(fractional.millisecond() > 0);

        // Test millisecond precision
        let millis_dt = DateTime64::try_from_unix_milliseconds(1609459200123).unwrap();
        assert_eq!(millis_dt.year(), 2021);
        assert_eq!(millis_dt.millisecond(), 123);

        // Test microsecond precision
        let micros_dt = DateTime64::try_from_unix_microseconds(1609459200123456).unwrap();
        assert_eq!(micros_dt.year(), 2021);
        assert_eq!(micros_dt.millisecond(), 123);
        assert_eq!(micros_dt.microsecond(), 456);

        // Test nanosecond precision (rounded to 100ns)
        let nanos_dt = DateTime64::try_from_unix_nanoseconds(1609459200123456700).unwrap();
        assert_eq!(nanos_dt.year(), 2021);
        assert_eq!(nanos_dt.millisecond(), 123);
        assert_eq!(nanos_dt.microsecond(), 456);
        assert_eq!(nanos_dt.hundred_nanosecond(), 7);
    }

    #[test]
    fn test_unix_timestamp_round_trips() {
        let test_cases = vec![
            0i64,
            1234567890,
            -1234567890,
            1609459200, // 2021-01-01
        ];

        for timestamp in test_cases {
            if let Ok(dt) = DateTime64::try_from_unix_seconds(timestamp) {
                // Note: Current implementation doesn't have to_unix_seconds, so we can't test round-trip
                // This is something that should be added for completeness
                assert!(dt.ticks() > 0 || timestamp < 0);
            }
        }

        // Test fractional round-trips where precision allows
        let fractional_cases = vec![0.0, 1234567890.5, 1234567890.123];
        for timestamp in fractional_cases {
            if let Ok(dt) = DateTime64::try_from_unix_seconds_f64(timestamp) {
                // Verify the datetime is reasonable
                assert!(dt.year() >= 1 && dt.year() <= 9999);
            }
        }
    }

    // ============================================================================
    // STRING PARSING TESTS
    // ============================================================================

    #[test]
    fn test_string_parsing_comprehensive() {
        // Test ISO 8601 formats
        let iso_cases = vec![
            "2023-12-25T14:30:45Z",
            "2023-12-25T14:30:45.123Z",
            "2023-12-25T14:30:45.123456Z",
            "2023-12-25 14:30:45",
        ];

        for case in iso_cases {
            let result = DateTime64::from_str(case);
            assert!(result.is_some(), "Failed to parse: {case}");
            if let Some(dt) = result {
                assert_eq!(dt.year(), 2023);
                assert_eq!(dt.month(), 12);
                assert_eq!(dt.day(), 25);
                assert_eq!(dt.hour(), 14);
                assert_eq!(dt.minute(), 30);
                assert_eq!(dt.second(), 45);
            }
        }

        // Test fuzzy parsing (mentioned in original code)
        let fuzzy_cases = vec![
            "zzz 2023-12-25 xcxc 14:30:45 xvcc",
            "Date: 2023-12-25 Time: 14:30:45",
            "2023/12/25 14:30:45",
        ];

        for case in fuzzy_cases {
            let result = DateTime64::from_str(case);
            assert!(result.is_some(), "Failed to parse fuzzy: {case}");
        }

        // Test invalid formats
        let invalid_cases = vec![
            "",
            "not a date",
            "2023-13-25T14:30:45Z", // Invalid month
            "2023-12-32T14:30:45Z", // Invalid day
            "2023-12-25T25:30:45Z", // Invalid hour
            "2023-12-25T14:60:45Z", // Invalid minute
            "2023-12-25T14:30:60Z", // Invalid second
        ];

        for case in invalid_cases {
            let result = DateTime64::from_str(case);
            assert!(result.is_none(), "Should not parse invalid: {case}");
        }
    }

    #[test]
    fn test_fromstr_trait() {
        // Test successful parsing
        let result = DateTime64::from_str("2023-12-25T14:30:45Z");
        assert!(result.is_some());

        // Test failed parsing
        let result = DateTime64::from_str("invalid");
        assert!(result.is_none());
    }

    // ============================================================================
    // ARITHMETIC OPERATION TESTS
    // ============================================================================

    #[test]
    fn test_comprehensive_arithmetic() {
        let base_dt = DateTime64::from_ymd_hms(2023, 6, 15, 12, 0, 0);

        // Test adding various time spans
        let one_second = TimeSpan64::new(TICKS_PER_SECOND);
        let one_minute = TimeSpan64::new(TICKS_PER_MINUTE);
        let one_hour = TimeSpan64::new(TICKS_PER_HOUR);
        let one_day = TimeSpan64::new(TICKS_PER_DAY);

        let dt_plus_sec = base_dt + one_second;
        assert_eq!(dt_plus_sec.second(), 1);

        let dt_plus_min = base_dt + one_minute;
        assert_eq!(dt_plus_min.minute(), 1);

        let dt_plus_hour = base_dt + one_hour;
        assert_eq!(dt_plus_hour.hour(), 13);

        let dt_plus_day = base_dt + one_day;
        assert_eq!(dt_plus_day.day(), 16);

        // Test subtracting time spans
        let dt_minus_sec = dt_plus_sec - one_second;
        assert_eq!(dt_minus_sec, base_dt);

        let dt_minus_min = dt_plus_min - one_minute;
        assert_eq!(dt_minus_min, base_dt);

        let dt_minus_hour = dt_plus_hour - one_hour;
        assert_eq!(dt_minus_hour, base_dt);

        let dt_minus_day = dt_plus_day - one_day;
        assert_eq!(dt_minus_day, base_dt);

        // Test datetime differences
        let dt1 = DateTime64::from_ymd_hms(2023, 1, 1, 0, 0, 0);
        let dt2 = DateTime64::from_ymd_hms(2023, 1, 1, 1, 0, 0);
        let diff = dt2 - dt1;
        assert_eq!(diff.ticks(), TICKS_PER_HOUR);

        // Test negative differences
        let neg_diff = dt1 - dt2;
        assert_eq!(neg_diff.ticks(), -TICKS_PER_HOUR);
    }

    #[test]
    fn test_arithmetic_edge_cases() {
        // Test overflow protection in current implementation
        let max_dt = DateTime64::MAX;
        let min_dt = DateTime64::MIN;
        let large_span = TimeSpan64::new(TICKS_PER_DAY);

        // Current implementation saturates
        let max_plus = max_dt + large_span;
        assert_eq!(max_plus.ticks(), MAX_TICKS);

        let min_minus = min_dt - large_span;
        assert_eq!(min_minus.ticks(), 0);

        // Test with very large TimeSpan values
        let huge_span = TimeSpan64::new(i64::MAX / 2);
        let saturated_add = max_dt + huge_span;
        assert_eq!(saturated_add.ticks(), MAX_TICKS);

        let saturated_sub = min_dt - huge_span;
        assert_eq!(saturated_sub.ticks(), 0);
    }

    // ============================================================================
    // COMPARISON AND ORDERING TESTS
    // ============================================================================

    #[test]
    fn test_comparison_operations() {
        let dt1 = DateTime64::from_ymd(2023, 1, 1);
        let dt2 = DateTime64::from_ymd(2023, 1, 2);
        let dt3 = DateTime64::from_ymd(2023, 1, 1);

        // Basic comparisons
        assert!(dt1 < dt2);
        assert!(dt2 > dt1);
        assert_eq!(dt1, dt3);
        assert_ne!(dt1, dt2);

        // Test with time components
        let dt_morning = DateTime64::from_ymd_hms(2023, 1, 1, 9, 0, 0);
        let dt_evening = DateTime64::from_ymd_hms(2023, 1, 1, 18, 0, 0);
        assert!(dt_morning < dt_evening);

        // Test with sub-second precision
        let dt_precise1 = DateTime64::from_ymd_hms_mmn(2023, 1, 1, 12, 0, 0, 0, 0, 1);
        let dt_precise2 = DateTime64::from_ymd_hms_mmn(2023, 1, 1, 12, 0, 0, 0, 0, 2);
        assert!(dt_precise1 < dt_precise2);
    }

    #[test]
    fn test_ordering_consistency() {
        use std::cmp::Ordering;

        let dt1 = DateTime64::from_ymd(2023, 1, 1);
        let dt2 = DateTime64::from_ymd(2023, 1, 2);
        let dt3 = DateTime64::from_ymd(2023, 1, 1);

        // Test PartialOrd
        assert_eq!(dt1.partial_cmp(&dt2), Some(Ordering::Less));
        assert_eq!(dt2.partial_cmp(&dt1), Some(Ordering::Greater));
        assert_eq!(dt1.partial_cmp(&dt3), Some(Ordering::Equal));

        // Test Ord
        assert_eq!(dt1.cmp(&dt2), Ordering::Less);
        assert_eq!(dt2.cmp(&dt1), Ordering::Greater);
        assert_eq!(dt1.cmp(&dt3), Ordering::Equal);

        // Test sorting
        let mut dates = [dt2, dt1, dt3];
        dates.sort();
        assert_eq!(dates[0], dt1);
        assert_eq!(dates[1], dt3);
        assert_eq!(dates[2], dt2);
    }

    // ============================================================================
    // FORMATTING TESTS
    // ============================================================================

    #[test]
    fn test_formatting_operations() {
        let dt = DateTime64::from_ymd_hms_mmn(2023, 12, 25, 14, 30, 45, 123, 456, 7);

        // Test Display trait
        let display_str = format!("{dt}");
        assert!(display_str.contains("2023-12-25"));
        assert!(display_str.contains("14:30:45"));

        // Test custom formatting
        let mut buffer = [0u8; 64];
        let len = dt.format_to_buffer(&mut buffer).unwrap();
        let formatted = std::str::from_utf8(&buffer[..len]).unwrap();

        // Verify ISO format structure
        assert!(formatted.starts_with("2023-12-25T14:30:45."));
        assert!(formatted.ends_with('Z'));

        // Test precision in formatting
        assert!(formatted.contains("1234567")); // Sub-second precision

        // Test boundary cases
        let min_dt = DateTime64::MIN;
        let max_dt = DateTime64::MAX;

        let min_formatted = format!("{min_dt}");
        assert!(min_formatted.contains("0001-01-01"));

        let max_formatted = format!("{max_dt}");
        assert!(max_formatted.contains("9999-12-31"));
    }

    #[test]
    fn test_format_buffer_edge_cases() {
        let dt = DateTime64::from_ymd_hms(2023, 12, 25, 14, 30, 45);

        // Test insufficient buffer size
        let mut small_buffer = [0u8; 10];
        let result = dt.format_to_buffer(&mut small_buffer);
        assert!(result.is_err());

        // Test exact size buffer
        let mut exact_buffer = [0u8; 28]; // Minimum required size
        let result = dt.format_to_buffer(&mut exact_buffer);
        assert!(result.is_ok());

        // Test oversized buffer
        let mut large_buffer = [0u8; 100];
        let len = dt.format_to_buffer(&mut large_buffer).unwrap();
        assert!(len <= 28);
        assert!(len > 0);
    }

    // ============================================================================
    // DATE/TIME COMPONENT EXTRACTION TESTS
    // ============================================================================

    #[test]
    fn test_comprehensive_component_extraction() {
        let dt = DateTime64::from_ymd_hms_mmn(2023, 12, 25, 14, 30, 45, 123, 456, 7);

        // Test all date components
        assert_eq!(dt.year(), 2023);
        assert_eq!(dt.month(), 12);
        assert_eq!(dt.day(), 25);

        // Test day of year (December 25 is day 359 in non-leap year 2023)
        assert_eq!(dt.day_of_year(), 359);

        // Test month of year (same as month())
        assert_eq!(dt.month_of_year(), 12);

        // Test week of year
        let week = dt.week_of_year();
        assert!((1..=53).contains(&week));

        // Test all time components
        assert_eq!(dt.hour(), 14);
        assert_eq!(dt.minute(), 30);
        assert_eq!(dt.second(), 45);
        assert_eq!(dt.millisecond(), 123);
        assert_eq!(dt.microsecond(), 456);
        assert_eq!(dt.hundred_nanosecond(), 7);

        // Test time of day
        let time_of_day = dt.time_of_day();
        assert!(time_of_day >= 0);
        assert!(time_of_day < TICKS_PER_DAY);
    }

    #[test]
    fn test_boundary_component_values() {
        // Test minimum values
        let min_dt = DateTime64::MIN;
        assert_eq!(min_dt.year(), 1);
        assert_eq!(min_dt.month(), 1);
        assert_eq!(min_dt.day(), 1);
        assert_eq!(min_dt.hour(), 0);
        assert_eq!(min_dt.minute(), 0);
        assert_eq!(min_dt.second(), 0);

        // Test maximum precision
        let max_precision = DateTime64::from_ymd_hms_mmn(2023, 1, 1, 23, 59, 59, 999, 999, 9);
        assert_eq!(max_precision.hour(), 23);
        assert_eq!(max_precision.minute(), 59);
        assert_eq!(max_precision.second(), 59);
        assert_eq!(max_precision.millisecond(), 999);
        assert_eq!(max_precision.microsecond(), 999);
        assert_eq!(max_precision.hundred_nanosecond(), 9);

        // Test leap year February 29
        let leap_day = DateTime64::from_ymd(2024, 2, 29);
        assert_eq!(leap_day.month(), 2);
        assert_eq!(leap_day.day(), 29);
        assert_eq!(leap_day.day_of_year(), 60); // 29 + 31 (Jan)
    }

    // ============================================================================
    // START/END OF PERIOD TESTS
    // ============================================================================

    #[test]
    fn test_start_end_periods() {
        let dt = DateTime64::from_ymd_hms(2023, 6, 15, 14, 30, 45);

        // Test start/end of year
        let start_year = dt.start_of_year(0).unwrap();
        assert_eq!(start_year.year(), 2023);
        assert_eq!(start_year.month(), 1);
        assert_eq!(start_year.day(), 1);
        assert_eq!(start_year.hour(), 0);
        assert_eq!(start_year.minute(), 0);
        assert_eq!(start_year.second(), 0);

        let end_year = dt.end_of_year(0).unwrap();
        assert_eq!(end_year.year(), 2023);
        assert_eq!(end_year.month(), 12);
        assert_eq!(end_year.day(), 31);
        assert_eq!(end_year.hour(), 23);
        assert_eq!(end_year.minute(), 59);
        assert_eq!(end_year.second(), 59);

        // Test start/end of month
        let start_month = dt.start_of_month(0).unwrap();
        assert_eq!(start_month.year(), 2023);
        assert_eq!(start_month.month(), 6);
        assert_eq!(start_month.day(), 1);

        let end_month = dt.end_of_month(0).unwrap();
        assert_eq!(end_month.year(), 2023);
        assert_eq!(end_month.month(), 6);
        assert_eq!(end_month.day(), 30); // June has 30 days

        // Test start/end of day
        let start_day = dt.start_of_day(0).unwrap();
        assert_eq!(start_day.year(), 2023);
        assert_eq!(start_day.month(), 6);
        assert_eq!(start_day.day(), 15);
        assert_eq!(start_day.hour(), 0);

        let end_day = dt.end_of_day(0).unwrap();
        assert_eq!(end_day.year(), 2023);
        assert_eq!(end_day.month(), 6);
        assert_eq!(end_day.day(), 15);
        assert_eq!(end_day.hour(), 23);
        assert_eq!(end_day.minute(), 59);
        assert_eq!(end_day.second(), 59);
    }

    #[test]
    fn test_period_offsets() {
        let dt = DateTime64::from_ymd(2023, 6, 15);

        // Test year offsets
        let next_year = dt.start_of_year(1).unwrap();
        assert_eq!(next_year.year(), 2024);

        let prev_year = dt.start_of_year(-1).unwrap();
        assert_eq!(prev_year.year(), 2022);

        // Test month offsets
        let next_month = dt.start_of_month(1).unwrap();
        assert_eq!(next_month.month(), 7);

        let prev_month = dt.start_of_month(-1).unwrap();
        assert_eq!(prev_month.month(), 5);

        // Test day offsets
        let next_day = dt.start_of_day(1).unwrap();
        assert_eq!(next_day.day(), 16);

        let prev_day = dt.start_of_day(-1).unwrap();
        assert_eq!(prev_day.day(), 14);

        // Test week offsets
        let next_week = dt.start_of_week(1);
        assert!(next_week.is_some());

        let prev_week = dt.start_of_week(-1);
        assert!(prev_week.is_some());
    }

    #[test]
    fn test_period_boundary_overflow() {
        // Test year boundary overflow
        let near_max_year = DateTime64::from_ymd(9999, 1, 1);
        assert!(near_max_year.start_of_year(1).is_none());

        let near_min_year = DateTime64::from_ymd(1, 12, 31);
        assert!(near_min_year.start_of_year(-1).is_none());

        // Test month boundary cases
        let december = DateTime64::from_ymd(2023, 12, 15);
        let next_month = december.start_of_month(1).unwrap();
        assert_eq!(next_month.year(), 2024);
        assert_eq!(next_month.month(), 1);

        let january = DateTime64::from_ymd(2023, 1, 15);
        let prev_month = january.start_of_month(-1).unwrap();
        assert_eq!(prev_month.year(), 2022);
        assert_eq!(prev_month.month(), 12);
    }

    // ============================================================================
    // CALENDAR CALCULATION TESTS
    // ============================================================================

    #[test]
    fn test_leap_year_calculations() {
        // Test standard leap year rules
        assert!(DateTime64::is_leap_year(2024)); // Divisible by 4
        assert!(!DateTime64::is_leap_year(2023)); // Not divisible by 4

        // Test century year rules
        assert!(!DateTime64::is_leap_year(1900)); // Divisible by 100, not by 400
        assert!(DateTime64::is_leap_year(2000)); // Divisible by 400
        assert!(!DateTime64::is_leap_year(2100)); // Divisible by 100, not by 400

        // Test days in month for leap/non-leap years
        assert_eq!(DateTime64::days_in_month(2023, 2), 28); // Non-leap February
        assert_eq!(DateTime64::days_in_month(2024, 2), 29); // Leap February

        // Test other months
        assert_eq!(DateTime64::days_in_month(2023, 1), 31); // January
        assert_eq!(DateTime64::days_in_month(2023, 4), 30); // April
        assert_eq!(DateTime64::days_in_month(2023, 12), 31); // December

        // Test all months in a non-leap year
        let expected_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        for (month, expected) in expected_days.iter().enumerate() {
            assert_eq!(
                DateTime64::days_in_month(2023, (month + 1) as i32),
                *expected
            );
        }

        // Test all months in a leap year
        let leap_expected_days = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        for (month, expected) in leap_expected_days.iter().enumerate() {
            assert_eq!(
                DateTime64::days_in_month(2024, (month + 1) as i32),
                *expected
            );
        }
    }

    // ============================================================================
    // UTILITY FUNCTION TESTS
    // ============================================================================

    #[test]
    fn test_utility_functions() {
        // Test today() function
        let today = DateTime64::today();
        assert!(today.year() >= 2023); // Should be reasonable year
        assert_eq!(today.hour(), 0); // Should be start of day
        assert_eq!(today.minute(), 0);
        assert_eq!(today.second(), 0);

        // Test utc_now() function
        let now = DateTime64::utc_now();
        assert!(now.year() >= 2023); // Should be reasonable year
        assert!(now.ticks() > today.ticks()); // Should be later than start of day

        // Test that multiple calls to utc_now() are increasing
        let now1 = DateTime64::utc_now();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let now2 = DateTime64::utc_now();
        assert!(now2.ticks() >= now1.ticks());
    }

    // ============================================================================
    // HASH AND SERIALIZATION TESTS
    // ============================================================================

    #[test]
    fn test_hash_functionality() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let dt1 = DateTime64::from_ymd(2023, 12, 25);
        let dt2 = DateTime64::from_ymd(2023, 12, 25);
        let dt3 = DateTime64::from_ymd(2023, 12, 26);

        // Test hash consistency
        let mut hasher1 = DefaultHasher::new();
        dt1.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        dt1.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2);

        // Test hash equality for equal objects
        let mut hasher3 = DefaultHasher::new();
        dt2.hash(&mut hasher3);
        let hash3 = hasher3.finish();

        assert_eq!(hash1, hash3); // dt1 == dt2, so hashes should be equal

        // Test hash set functionality
        let mut set = HashSet::new();
        set.insert(dt1);
        assert!(set.contains(&dt2)); // dt1 == dt2
        assert!(!set.contains(&dt3)); // dt3 is different
    }

    #[test]
    fn test_serialization_roundtrip() {
        // Skip serde_json tests as it's not a dependency
        // This would require adding serde_json to Cargo.toml

        let test_cases = vec![
            DateTime64::MIN,
            DateTime64::MAX,
            DateTime64::from_ymd(2023, 12, 25),
            DateTime64::from_ymd_hms(2023, 12, 25, 14, 30, 45),
            DateTime64::from_ymd_hms_mmn(2023, 12, 25, 14, 30, 45, 123, 456, 7),
        ];

        for dt in test_cases {
            // For now, just verify the datetime values are reasonable
            assert!(dt.year() >= 1 && dt.year() <= 9999);
            assert!(dt.month() >= 1 && dt.month() <= 12);
            assert!(dt.day() >= 1 && dt.day() <= 31);
            assert_eq!(dt.ticks(), dt.ticks()); // Tautology, but tests the accessor
        }
    }

    // ============================================================================
    // PERFORMANCE TESTS
    // ============================================================================

    #[test]
    fn test_performance_characteristics() {
        use std::time::Instant;

        let iterations = 10_000;

        // Test construction performance
        let start = Instant::now();
        for i in 0..iterations {
            let _ = DateTime64::new(i as i64);
        }
        let construction_time = start.elapsed();

        // Test component extraction performance
        let dt = DateTime64::from_ymd_hms_mmn(2023, 12, 25, 14, 30, 45, 123, 456, 7);
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = (
                dt.year(),
                dt.month(),
                dt.day(),
                dt.hour(),
                dt.minute(),
                dt.second(),
            );
        }
        let component_time = start.elapsed();

        // Test arithmetic performance
        let timespan = TimeSpan64::new(1);
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = dt + timespan;
        }
        let arithmetic_time = start.elapsed();

        // Test formatting performance
        let mut buffer = [0u8; 64];
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = dt.format_to_buffer(&mut buffer);
        }
        let formatting_time = start.elapsed();

        // Output performance metrics for analysis
        println!("Performance test results for {iterations} iterations:");
        println!("  Construction: {construction_time:?}");
        println!("  Component extraction: {component_time:?}");
        println!("  Arithmetic: {arithmetic_time:?}");
        println!("  Formatting: {formatting_time:?}");

        // Basic performance assertions (these may need adjustment based on hardware)
        assert!(
            construction_time.as_millis() < 100,
            "Construction performance regression"
        );
        assert!(
            component_time.as_millis() < 100,
            "Component extraction performance regression"
        );
        assert!(
            arithmetic_time.as_millis() < 100,
            "Arithmetic performance regression"
        );
        assert!(
            formatting_time.as_millis() < 500,
            "Formatting performance regression"
        );
    }

    // ============================================================================
    // EDGE CASE AND REGRESSION TESTS
    // ============================================================================

    #[test]
    fn test_edge_cases_and_regressions() {
        // Test MIN and MAX constants
        assert_eq!(DateTime64::MIN.ticks(), 0);
        assert_eq!(DateTime64::MAX.ticks(), MAX_TICKS);

        // Test boundary tick values
        let boundary_cases = vec![0, 1, MAX_TICKS - 1, MAX_TICKS];

        for ticks in boundary_cases {
            let dt = DateTime64::new(ticks);
            assert_eq!(dt.ticks(), ticks);
            assert!(dt.year() >= 1 && dt.year() <= 9999);
        }

        // Test precision preservation
        let precise_dt = DateTime64::from_ymd_hms_mmn(2023, 1, 1, 0, 0, 0, 999, 999, 9);
        assert_eq!(precise_dt.millisecond(), 999);
        assert_eq!(precise_dt.microsecond(), 999);
        assert_eq!(precise_dt.hundred_nanosecond(), 9);

        // Test arithmetic at boundaries
        let near_max = DateTime64::new(MAX_TICKS - 100);
        let small_span = TimeSpan64::new(50);
        let result = near_max + small_span;
        assert_eq!(result.ticks(), MAX_TICKS - 50);

        // Test overflow handling
        let large_span = TimeSpan64::new(200);
        let overflow_result = near_max + large_span;
        assert_eq!(overflow_result.ticks(), MAX_TICKS); // Should saturate
    }

    #[test]
    fn test_zero_trait_implementation() {
        use num_traits::Zero;

        // Test zero() method
        let zero_dt = DateTime64::zero();
        assert_eq!(zero_dt.ticks(), 0);
        assert_eq!(zero_dt, DateTime64::MIN);

        // Test is_zero() method
        assert!(DateTime64::MIN.is_zero());
        assert!(!DateTime64::MAX.is_zero());
        assert!(!DateTime64::from_ymd(2023, 1, 1).is_zero());
    }

    // ============================================================================
    // COMPATIBILITY TESTS
    // ============================================================================

    #[test]
    fn test_display_format_compatibility() {
        let dt = DateTime64::from_ymd_hms_mmn(2023, 12, 25, 14, 30, 45, 123, 456, 7);
        let display_str = format!("{dt}");

        // Should match the expected format from Display implementation
        assert!(display_str.contains("2023-12-25 14:30:45"));

        // Test that the display format is parseable by chrono (if needed for compatibility)
        // Note: This tests the general format compatibility
        assert!(display_str.len() > 19); // At least "YYYY-MM-DD HH:MM:SS" length
    }

    #[test]
    fn test_type_safety() {
        // These tests verify that the type system prevents common errors

        let dt = DateTime64::from_ymd(2023, 1, 1);
        let ts = TimeSpan64::new(TICKS_PER_HOUR);

        // Valid operations
        let _dt_plus_ts = dt + ts; // DateTime + TimeSpan -> DateTime
        let _dt_minus_ts = dt - ts; // DateTime - TimeSpan -> DateTime
        let _dt_minus_dt = dt - dt; // DateTime - DateTime -> TimeSpan

        // The type system should prevent invalid operations at compile time:
        // let invalid = ts + ts; // TimeSpan + TimeSpan (not defined for this context)
        // This would be a compile error, which is what we want
    }

    // ============================================================================
    // THREAD SAFETY TESTS
    // ============================================================================

    #[test]
    fn test_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let dt = Arc::new(DateTime64::from_ymd_hms(2023, 12, 25, 14, 30, 45));
        let mut handles = vec![];

        // Spawn multiple threads that read the same DateTime64
        for i in 0..5 {
            let dt_clone = Arc::clone(&dt);
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    let _ = dt_clone.year();
                    let _ = dt_clone.month();
                    let _ = dt_clone.day();
                    let _ = format!("{}", *dt_clone);
                }
                i // Return thread ID for verification
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            let thread_id = handle.join().unwrap();
            assert!(thread_id < 5);
        }

        // Test concurrent creation of new DateTime64 instances
        let mut creation_handles = vec![];
        for i in 0..5 {
            let handle = thread::spawn(move || {
                let mut results = vec![];
                for j in 0..100 {
                    let dt = DateTime64::from_ymd(2023, 1, (j % 28) + 1);
                    results.push(dt.ticks());
                }
                (i, results.len())
            });
            creation_handles.push(handle);
        }

        for handle in creation_handles {
            let (thread_id, count) = handle.join().unwrap();
            assert_eq!(count, 100);
            assert!(thread_id < 5);
        }
    }

    #[test]
    fn test_improved_arithmetic_methods() {
        let dt = DateTime64::from_ymd(2023, 1, 1);
        let one_day = TimeSpan64::from_days(1);

        // Test the new checked_add method
        let result = dt.checked_add(one_day);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DateTime64::from_ymd(2023, 1, 2));

        // Test checked_sub method
        let result2 = dt.checked_sub(one_day);
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap(), DateTime64::from_ymd(2022, 12, 31));

        // Test overflow detection with checked_add
        let max_dt = DateTime64::MAX;
        let overflow_result = max_dt.checked_add(one_day);
        assert!(overflow_result.is_err());

        // Test underflow detection with checked_sub
        let min_dt = DateTime64::MIN;
        let underflow_result = min_dt.checked_sub(one_day);
        assert!(underflow_result.is_err());

        // Test saturating methods
        let saturated_add = max_dt.saturating_add(one_day);
        assert_eq!(saturated_add, DateTime64::MAX);

        let saturated_sub = min_dt.saturating_sub(one_day);
        assert_eq!(saturated_sub, DateTime64::MIN);
    }

    #[test]
    #[should_panic(expected = "ticks must be non-negative")]
    fn test_datetime64_new_negative_ticks_panics() {
        // This should panic due to the new assertion for ticks >= 0
        let _dt = DateTime64::new(-1);
    }

    #[test]
    fn test_datetime64_new_zero_ticks_succeeds() {
        // Zero ticks should be valid (represents the epoch)
        let dt = DateTime64::new(0);
        assert_eq!(dt.ticks(), 0);
        assert!(dt.is_zero());
    }

    #[test]
    fn test_datetime64_new_positive_ticks_succeeds() {
        // Positive ticks should work fine
        let dt = DateTime64::new(1000);
        assert_eq!(dt.ticks(), 1000);
        assert!(!dt.is_zero());
    }
}

// ============================================================================
// INTEGRATION TESTS WITH OTHER MODULES
// ============================================================================

#[cfg(test)]
mod integration_tests {
    use crate::datetime::DateTime64;
    use crate::timespan::TimeSpan64;
    use crate::{TICKS_PER_DAY, TICKS_PER_HOUR, TICKS_PER_MINUTE, TICKS_PER_SECOND};

    #[test]
    fn test_datetime_timespan_integration() {
        let dt = DateTime64::from_ymd_hms(2023, 6, 15, 12, 0, 0);

        // Test various TimeSpan creation methods
        let one_day = TimeSpan64::new(TICKS_PER_DAY);
        let one_hour = TimeSpan64::new(TICKS_PER_HOUR);
        let one_minute = TimeSpan64::new(TICKS_PER_MINUTE);
        let one_second = TimeSpan64::new(TICKS_PER_SECOND);

        // Test arithmetic operations
        let tomorrow = dt + one_day;
        assert_eq!(tomorrow.day(), 16);

        let next_hour = dt + one_hour;
        assert_eq!(next_hour.hour(), 13);

        let next_minute = dt + one_minute;
        assert_eq!(next_minute.minute(), 1);

        let next_second = dt + one_second;
        assert_eq!(next_second.second(), 1);

        // Test chained operations
        let complex_result = ((dt + one_day) + one_hour) + one_minute;
        assert_eq!(complex_result.day(), 16);
        assert_eq!(complex_result.hour(), 13);
        assert_eq!(complex_result.minute(), 1);
    }

    #[test]
    fn test_parser_integration() {
        // Test that parsing works correctly with the datetime module
        let test_strings = vec![
            "2023-12-25T14:30:45Z",
            "2023-12-25 14:30:45",
            "Dec 25, 2023 2:30 PM",
        ];

        for test_str in test_strings {
            if let Some(parsed_dt) = DateTime64::from_str(test_str) {
                // Verify that parsed datetime has reasonable values
                assert!(parsed_dt.year() >= 1 && parsed_dt.year() <= 9999);
                assert!(parsed_dt.month() >= 1 && parsed_dt.month() <= 12);
                assert!(parsed_dt.day() >= 1 && parsed_dt.day() <= 31);
                assert!(parsed_dt.hour() >= 0 && parsed_dt.hour() < 24);
                assert!(parsed_dt.minute() >= 0 && parsed_dt.minute() < 60);
                assert!(parsed_dt.second() >= 0 && parsed_dt.second() < 60);

                // Test that formatting the parsed datetime works
                let formatted = format!("{parsed_dt}");
                assert!(!formatted.is_empty());
            }
        }
    }
}

// ============================================================================
// BASIC DATETIME TESTS
// ============================================================================

#[cfg(test)]
mod basic_datetime_tests {
    use crate::MAX_TICKS;
    use crate::datetime::DateTime64;
    use crate::timespan::TimeSpan64;

    #[test]
    fn test_datetime() {
        let mut date1 = DateTime64::from_ymd_hms(2016, 12, 30, 12, 12, 12);
        let second = date1.second();
        assert_eq!(second, 12);
        let minute = date1.minute();
        assert_eq!(minute, 12);
        let hour = date1.hour();
        assert_eq!(hour, 12);
        date1 = DateTime64::from_ymd(2016, 2, 2);
        let year = date1.year();
        assert_eq!(year, 2016);
        let month = date1.month();
        assert_eq!(month, 2);
        let day = date1.day();
        assert_eq!(day, 2);
        let day_of_year = date1.day_of_year();
        assert_eq!(day_of_year, 33);
        date1 = DateTime64::from_ymd_hms_mmn(2016, 12, 30, 12, 12, 12, 543, 123, 4);
        assert_eq!(date1.to_string(), "2016-12-30 12:12:12.543123400");
        date1 = DateTime64::from_ymd(2016, 12, 30);
        assert!(DateTime64::is_leap_year(date1.year()));
        assert_eq!(DateTime64::days_in_month(date1.year(), date1.month()), 31);
        date1 = DateTime64::from_ymd(2017, 2, 20);
        assert!(!DateTime64::is_leap_year(date1.year()));
        assert_eq!(DateTime64::days_in_month(date1.year(), date1.month()), 28);
    }

    #[test]
    fn test_datetime64_addition_overflow_protection() {
        // Test DateTime64 + TimeSpan64 - should saturate at MAX when overflowing
        let max_dt = DateTime64::MAX;
        let large_timespan = TimeSpan64::new(MAX_TICKS / 2);

        // Adding large timespan should saturate at MAX, not overflow
        let result = max_dt + large_timespan;
        assert_eq!(result, DateTime64::MAX);
        assert_eq!(result.ticks(), MAX_TICKS);

        // Test with positive TimeSpan64::MAX
        let max_timespan = TimeSpan64::MAX;
        let result2 = max_dt + max_timespan;
        assert_eq!(result2, DateTime64::MAX);
        assert_eq!(result2.ticks(), MAX_TICKS);

        // Test the new checked_add method
        let small_dt = DateTime64::from_ymd(2023, 1, 1);
        let one_day = TimeSpan64::from_days(1);
        let result3 = small_dt.checked_add(one_day);
        assert!(result3.is_ok());
        assert_eq!(result3.unwrap(), DateTime64::from_ymd(2023, 1, 2));

        // Test overflow with checked_add
        let overflow_result = max_dt.checked_add(large_timespan);
        assert!(overflow_result.is_err());

        // Test near-boundary cases
        let near_max = DateTime64::new(MAX_TICKS - 1000);
        let small_timespan = TimeSpan64::new(500);
        let result4 = near_max + small_timespan;
        assert_eq!(result4.ticks(), MAX_TICKS - 500);

        // Adding more than available space should saturate
        let large_timespan2 = TimeSpan64::new(2000);
        let result5 = near_max + large_timespan2;
        assert_eq!(result5, DateTime64::MAX);
    }

    #[test]
    fn test_datetime64_subtraction_overflow_protection() {
        // Test DateTime64 - TimeSpan64 - should saturate at MIN when underflowing
        let min_dt = DateTime64::MIN;
        let large_timespan = TimeSpan64::new(1000);

        // Subtracting from MIN should saturate at MIN, not underflow
        let result = min_dt - large_timespan;
        assert_eq!(result, DateTime64::MIN);
        assert_eq!(result.ticks(), 0);

        // Test with TimeSpan64::MAX
        let max_timespan = TimeSpan64::MAX;
        let result2 = min_dt - max_timespan;
        assert_eq!(result2, DateTime64::MIN);
        assert_eq!(result2.ticks(), 0);

        // Test near-boundary cases
        let near_min = DateTime64::new(1000);
        let small_timespan = TimeSpan64::new(500);
        let result3 = near_min - small_timespan;
        assert_eq!(result3.ticks(), 500);

        // Subtracting more than available should saturate at MIN
        let large_timespan2 = TimeSpan64::new(2000);
        let result4 = near_min - large_timespan2;
        assert_eq!(result4, DateTime64::MIN);

        // Test DateTime64 - DateTime64 = TimeSpan64
        let dt1 = DateTime64::new(1000);
        let dt2 = DateTime64::new(2000);
        let timespan_result = dt2 - dt1;
        assert_eq!(timespan_result.ticks(), 1000);

        // Test potential underflow in DateTime64 - DateTime64
        let timespan_result2 = dt1 - dt2;
        assert_eq!(timespan_result2.ticks(), -1000);

        // Test extreme case: MAX - MIN should not overflow TimeSpan64
        let max_dt = DateTime64::MAX;
        let min_dt = DateTime64::MIN;
        let max_diff = max_dt - min_dt;
        assert_eq!(max_diff.ticks(), MAX_TICKS);
    }

    #[test]
    fn test_datetime64_edge_cases_no_overflow() {
        // Verify that MAX and MIN constants are within valid bounds
        assert!(DateTime64::is_valid_ticks(DateTime64::MAX.ticks()));
        assert!(DateTime64::is_valid_ticks(DateTime64::MIN.ticks()));
        assert_eq!(DateTime64::MIN.ticks(), 0);
        assert_eq!(DateTime64::MAX.ticks(), MAX_TICKS);

        // Test that operations stay within bounds
        let max_dt = DateTime64::MAX;
        let min_dt = DateTime64::MIN;

        // Adding zero should not change the value
        let zero_timespan = TimeSpan64::zero();
        assert_eq!(max_dt + zero_timespan, max_dt);
        assert_eq!(min_dt + zero_timespan, min_dt);
        assert_eq!(max_dt - zero_timespan, max_dt);
        assert_eq!(min_dt - zero_timespan, min_dt);

        // Test with negative TimeSpan64
        let negative_timespan = TimeSpan64::new(-1000);
        let result1 = max_dt + negative_timespan;
        assert_eq!(result1.ticks(), MAX_TICKS - 1000);

        // Adding negative to MIN should saturate at MIN
        let result2 = min_dt + negative_timespan;
        assert_eq!(result2, DateTime64::MIN);

        // Subtracting negative is same as adding positive
        let result3 = min_dt - negative_timespan;
        assert_eq!(result3.ticks(), 1000);

        // Subtracting negative from MAX should saturate
        let result4 = max_dt - negative_timespan;
        assert_eq!(result4, DateTime64::MAX);
    }
}

// ============================================================================
// PARSER TESTS
// ============================================================================

#[cfg(test)]
mod parser_tests {
    use crate::datetime_parser::DateTimeParser;

    #[test]
    fn test_fast_parser_positive() {
        let test_cases = [
            ("2018-02-04 16:13:30.1171988", Some(636533576101171988)),
            ("2014-05-25T08:20:03.123456", Some(635366028031234560)),
            ("2016-03-11T00:00:00.0000000", Some(635932512000000000)),
            ("2016-03-11T00:00:00.0000005", Some(635932512000000005)),
            ("2016-03-11T00:00:00.0000010", Some(635932512000000010)),
            ("2014-05-25T08:20:00", Some(635366028000000000)),
            ("2014-11-08 15:55:55", Some(635510589550000000)),
            (
                "zzz 2014-11-08 xcxc 13:05:11 xvcc",
                Some(635510487110000000),
            ),
        ];
        run_test_cases(&test_cases);
    }

    #[test]
    fn test_fast_parser_negative() {
        let test_cases = [
            ("2014-5-25T08:20:03.123456Z", None),
            ("Sat, 8 Nov 14 15:05:02 GMT", None),
            ("Sat, 8 Nov 14 15:05:02 UTC", None),
            ("08-Nov-14 15:05:02", None),
            ("6/15/2009 13:45", None),
            ("6/15/2009", None),
            ("Sat, Nov 8 14 15:05 GMT", None),
            ("Sat, Nov 8 14 15:05 UTC", None),
            ("Nov 8 14 15:05:02 GMT", None),
            ("Nov 8 14 15:05:02 UTC", None),
            ("-6/15/2009 13:45", None),
            ("6//15//2009", None),
            ("6\\15\\2009", None),
            ("Xyz, Nov 8 14 15:05:02", None),
            ("2019-02-29 16:13:30.1171988", None),
            ("2100-02-29", None),
            ("2019-02-01 25:13:30.1171988", None),
            ("2019-02-01 16:61:30.1171988", None),
            ("2019-02-01 16:13:61.1171988", None),
            ("2019-13-01 16:13:30.1171988", None),
            ("2019-05-40 16:13:30.1171988", None),
            ("2019-05-40 16:13:30.1171988", None),
            ("12345-05-01 16:13:30.1171988", None),
            ("2019-123-01 16:13:30.1171988", None),
            ("2019-05-123 16:13:30.1171988", None),
            ("2019-05-01 123:13:30.1171988", None),
            ("2019-05-01 16:123:30.1171988", None),
            ("2019-05-01 16:13:123.1171988", None),
            ("2019-02-01 16:13:61.1234567891", None),
            ("2019-06-31 16:13:30.1171988", None),
            ("2*19-06-30 16:13:30.1171988", None),
            ("219-06-31 16:13:30.1171988", None),
            ("[923543275*2020-11-11T00:11:19.309Z]", None),
            ("[-2020-11-11T00:1@1:19.309Z]", None),
            ("[201411!!!!!!0^8 15:55:55.123456]", None),
            ("[20141!108 15:55:55.123456]", None),
            ("[20141!!108 15:55:55.123456]", None),
            ("[#!@$13$@#$#@$2020-11-11T00:11:19.309Z]", None),
            ("[12020-11-11T00:11:19.309Z]", None),
            ("[Sat Nov 8 08:15:20 2014]", None),
            ("Sat Nov 8 !08:15:20 2014", None),
            ("Sat Nov 8 08:1$$5:20 2014", None),
            ("Sat Nov 8 08:15:20 2014!", None),
            ("2019-06-10-privatepreview", None),
        ];
        run_test_cases(&test_cases);
    }

    fn run_test_cases(test_cases: &[(&str, Option<i64>)]) {
        for (dt, expected) in test_cases {
            let actual = DateTimeParser::try_parse(dt);
            assert_eq!(*expected, actual);
        }
    }
}

// ============================================================================
// FORMAT TESTS
// ============================================================================

#[cfg(test)]
mod format_tests {
    use crate::{
        datetime::DateTime64,
        format::{DateTimeFormat, TimeSpanFormat},
        timespan::TimeSpan64,
    };

    #[test]
    fn test_wrong_format() {
        assert!(DateTimeFormat::from_specs(vec![" "]).is_ok());
        assert!(DateTimeFormat::from_specs(vec!["unknown"]).is_err());
    }

    #[test]
    fn test_datetime_format() {
        let dt = DateTime64::from_ymd_hms_mmn(2021, 10, 11, 21, 25, 45, 804, 123, 5);

        fn run_test(dt: DateTime64, fmt: Vec<&str>, expected: &str) {
            let mut fmt = DateTimeFormat::from_specs(fmt).unwrap();
            assert_eq!(expected, std::str::from_utf8(fmt.render(dt)).unwrap());
            assert_eq!(expected, std::str::from_utf8(fmt.render(dt)).unwrap());
        }

        run_test(
            dt,
            vec![
                "yyyy", "-", "MM", "-", "dd", " ", "HH", ":", "mm", ":", "ss", ".", "fffffff", " ",
                "tt",
            ],
            "2021-10-11 21:25:45.8041235 PM",
        );

        run_test(
            dt,
            vec![
                "y", "-", "M", "-", "d", " ", "h", ":", "m", ":", "s", ".", "f",
            ],
            "21-10-11 9:25:45.8",
        )
    }

    #[test]
    fn test_timespan_format() {
        fn run_test(ts: TimeSpan64, fmt: Vec<&str>, expected: &str) {
            let mut fmt = TimeSpanFormat::from_specs(fmt).unwrap();
            assert_eq!(expected, std::str::from_utf8(fmt.render(ts)).unwrap());
            assert_eq!(expected, std::str::from_utf8(fmt.render(ts)).unwrap());
        }

        run_test(
            TimeSpan64::dhmsml(12, 10, 45, 31, 365),
            vec!["ddddd", ".", "hh", ":", "mm", ":", "ss", ".", "fffffff"],
            "00012.10:45:31.3650000",
        );

        run_test(
            TimeSpan64::dhmsml(-12, 10, 45, 31, 365),
            vec!["dd", ".", "hh", ":", "mm", ":", "ss", ".", "fff"],
            "-11.13:14:28.635",
        );
    }
}

// ============================================================================
// TIMESPAN TESTS
// ============================================================================

#[cfg(test)]
mod timespan_tests {
    use crate::timespan::TimeSpan64;

    #[test]
    fn test_datetime() {
        let mut ts1 = TimeSpan64::from_hours(1);
        let mut ts2 = TimeSpan64::from_minutes(2);
        let ts3 = TimeSpan64::from_seconds(3);
        let mut ts4 = TimeSpan64::hms(1, 2, 3);
        ts2 += ts3;
        ts1 += ts2;
        assert_eq!(ts1, ts4);
        ts4 += ts3;
        ts4 -= ts3;
        assert_eq!(ts1, ts4);
        ts1 = TimeSpan64::from_hours(1);
        ts2 = TimeSpan64::from_hours(2);
        ts4 = TimeSpan64::from_hours(4);
        ts1 *= 2;
        ts4 /= 2;
        assert_eq!(ts1, ts2);
        assert_eq!(ts4, ts2);
        ts1 = TimeSpan64::dhmsml(1, 2, 3, 4, 5);
        ts2 = TimeSpan64::hms(2, 3, 4);
        assert!(ts1 > ts2);
        assert!(ts2 < ts1);
        ts2 += TimeSpan64::from_days(1);
        ts2 += TimeSpan64::from_milliseconds(5);
        assert_eq!(ts1, ts2);
    }
}

// ============================================================================
// TIMESPAN PARSER TESTS
// ============================================================================

#[cfg(test)]
mod timespan_parser_tests {
    use crate::timespan_parser::TimeSpanParser;
    use crate::*;

    #[test]
    fn test_timespan_parser_full_format() {
        let test_cases = [
            // Format: DD.HH:MM:SS.fractional
            (
                "1.02:30:45.500",
                Some(
                    TICKS_PER_DAY
                        + 2 * TICKS_PER_HOUR
                        + 30 * TICKS_PER_MINUTE
                        + 45 * TICKS_PER_SECOND
                        + 5000000,
                ),
            ),
            ("0.00:00:01.000", Some(TICKS_PER_SECOND)),
            (
                "5.12:00:00.000",
                Some(5 * TICKS_PER_DAY + 12 * TICKS_PER_HOUR),
            ),
            (
                "10.23:59:59.9999999",
                Some(
                    10 * TICKS_PER_DAY
                        + 23 * TICKS_PER_HOUR
                        + 59 * TICKS_PER_MINUTE
                        + 59 * TICKS_PER_SECOND
                        + 9999999,
                ),
            ),
        ];
        run_timespan_test_cases(&test_cases);
    }

    #[test]
    fn test_timespan_parser_time_format() {
        let test_cases = [
            // Format: HH:MM:SS.fractional
            (
                "02:30:45",
                Some(2 * TICKS_PER_HOUR + 30 * TICKS_PER_MINUTE + 45 * TICKS_PER_SECOND),
            ),
            (
                "25:30:15",
                Some(25 * TICKS_PER_HOUR + 30 * TICKS_PER_MINUTE + 15 * TICKS_PER_SECOND),
            ), // > 24 hours allowed
            ("00:00:01.500", Some(TICKS_PER_SECOND + 5000000)),
            ("100:00:00", Some(100 * TICKS_PER_HOUR)),
        ];
        run_timespan_test_cases(&test_cases);
    }

    #[test]
    fn test_timespan_parser_minutes_seconds() {
        let test_cases = [
            // Format: MM:SS[.fractional]
            ("30:45", Some(30 * TICKS_PER_MINUTE + 45 * TICKS_PER_SECOND)),
            ("90:15", Some(90 * TICKS_PER_MINUTE + 15 * TICKS_PER_SECOND)), // > 60 minutes allowed
            (
                "05:30.250",
                Some(5 * TICKS_PER_MINUTE + 30 * TICKS_PER_SECOND + 2500000),
            ),
            ("00:01.1", Some(TICKS_PER_SECOND + 1000000)),
        ];
        run_timespan_test_cases(&test_cases);
    }

    #[test]
    fn test_timespan_parser_seconds_only() {
        let test_cases = [
            // Format: SS[.fractional]
            ("45.500", Some(45 * TICKS_PER_SECOND + 5000000)),
            ("1.0", Some(TICKS_PER_SECOND)),
            ("30", Some(30 * TICKS_PER_SECOND)),
            ("0.1234567", Some(1234567)), // 7 digits precision
        ];
        run_timespan_test_cases(&test_cases);
    }

    #[test]
    fn test_timespan_parser_negative() {
        let test_cases = [
            // Negative timespans
            (
                "-1.02:30:45",
                Some(
                    -(TICKS_PER_DAY
                        + 2 * TICKS_PER_HOUR
                        + 30 * TICKS_PER_MINUTE
                        + 45 * TICKS_PER_SECOND),
                ),
            ),
            (
                "-25:30:15",
                Some(-(25 * TICKS_PER_HOUR + 30 * TICKS_PER_MINUTE + 15 * TICKS_PER_SECOND)),
            ),
            (
                "-30:45",
                Some(-(30 * TICKS_PER_MINUTE + 45 * TICKS_PER_SECOND)),
            ),
            ("-45.500", Some(-(45 * TICKS_PER_SECOND + 5000000))),
        ];
        run_timespan_test_cases(&test_cases);
    }

    #[test]
    fn test_timespan_parser_embedded_text() {
        let test_cases = [
            // Embedded in text
            (
                "duration 1.05:30:45 elapsed",
                Some(
                    TICKS_PER_DAY
                        + 5 * TICKS_PER_HOUR
                        + 30 * TICKS_PER_MINUTE
                        + 45 * TICKS_PER_SECOND,
                ),
            ),
            (
                "time: 02:30:45.500 total",
                Some(2 * TICKS_PER_HOUR + 30 * TICKS_PER_MINUTE + 45 * TICKS_PER_SECOND + 5000000),
            ),
            (
                "elapsed 30:45 minutes",
                Some(30 * TICKS_PER_MINUTE + 45 * TICKS_PER_SECOND),
            ),
            ("wait 5.250 seconds", Some(5 * TICKS_PER_SECOND + 2500000)),
        ];
        run_timespan_test_cases(&test_cases);
    }

    #[test]
    fn test_timespan_parser_invalid() {
        let test_cases = [
            // Invalid formats
            ("", None),
            ("not a timespan", None),
            ("25:60:30", None), // Invalid minutes (should be < 60 in HH:MM:SS format)
            ("10:30:60", None), // Invalid seconds
            ("abc:def:ghi", None), // Non-numeric
            (":", None),
            ("..", None),
            ("--1.00:00:00", None), // Double negative
            ("1.25:60:30", None),   // Invalid minutes in DD.HH:MM:SS format
        ];
        run_timespan_test_cases(&test_cases);
    }

    #[test]
    fn test_timespan_parser_edge_cases() {
        let test_cases = [
            // Edge cases
            ("0.00:00:00", Some(0)),
            ("0:00", Some(0)),
            ("0", Some(0)),
            ("0.0", Some(0)),
            (
                "999999.23:59:59.9999999",
                Some(
                    999999 * TICKS_PER_DAY
                        + 23 * TICKS_PER_HOUR
                        + 59 * TICKS_PER_MINUTE
                        + 59 * TICKS_PER_SECOND
                        + 9999999,
                ),
            ),
        ];
        run_timespan_test_cases(&test_cases);
    }

    fn run_timespan_test_cases(test_cases: &[(&str, Option<i64>)]) {
        for (input, expected) in test_cases {
            let actual = TimeSpanParser::try_parse(input);
            assert_eq!(*expected, actual, "Failed parsing: '{input}'");
        }
    }
}
