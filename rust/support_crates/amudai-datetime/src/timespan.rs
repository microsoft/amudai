use super::*;
use crate::datetime::DateTime64;
use num_traits::Zero;
use serde::{Deserialize, Serialize};
use std::cmp::*;
use std::io;
use std::ops::*;

/// A high-precision time span representation using 100-nanosecond ticks.
///
/// `TimeSpan64` represents a duration or elapsed time as a 64-bit signed integer
/// of 100-nanosecond ticks. This provides compatibility with .NET's TimeSpan
/// structure while offering high precision for time interval calculations.
///
/// # Precision and Range
///
/// - **Precision**: 100 nanoseconds (1 tick = 100 nanoseconds)
/// - **Range**: Approximately ±29,227 years
/// - **Positive values**: Represent forward time spans
/// - **Negative values**: Represent backward time spans
///
/// # Tick Definition
///
/// A tick is the smallest unit of time measurable by `TimeSpan64`:
/// - 1 tick = 100 nanoseconds
/// - 1 microsecond = 10 ticks
/// - 1 millisecond = 10,000 ticks
/// - 1 second = 10,000,000 ticks
/// - 1 minute = 600,000,000 ticks
/// - 1 hour = 36,000,000,000 ticks
/// - 1 day = 864,000,000,000 ticks
///
/// # Examples
///
/// ```
/// # use amudai_datetime::timespan::TimeSpan64;
/// // Create a timespan of 1 hour, 30 minutes, and 45 seconds
/// let ts = TimeSpan64::hms(1, 30, 45);
///
/// // Create a timespan from days, hours, minutes, seconds, and milliseconds
/// let detailed_ts = TimeSpan64::dhmsml(2, 3, 30, 15, 500);
///
/// // Create from individual units
/// let one_day = TimeSpan64::from_days(1);
/// let two_hours = TimeSpan64::from_hours(2);
/// let combined = one_day + two_hours; // 1 day and 2 hours
///
/// // Arithmetic operations
/// let half_day = one_day / 2; // 12 hours
/// let double_time = ts * 2;   // 3 hours, 1 minute, 30 seconds
/// ```
///
/// # .NET Compatibility
///
/// This type is designed to be compatible with .NET's `TimeSpan` structure,
/// using the same tick-based representation and epoch. Values can be safely
/// exchanged between .NET and Rust code.
///
/// # Serialization
///
/// `TimeSpan64` supports serialization and deserialization through serde,
/// storing the internal tick count as a 64-bit signed integer.
#[derive(Copy, Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[repr(C)]
pub struct TimeSpan64 {
    ticks: i64,
}

/// Detailed breakdown of a `TimeSpan64` value into its component parts.
///
/// `TimeSpan64Specs` provides a decomposed view of a timespan, breaking it down
/// into days, hours, minutes, seconds, and fractional seconds (in 100-nanosecond ticks).
/// This is useful for display purposes, formatting, and component-wise analysis.
///
/// # Components
///
/// - **days**: Complete days in the timespan
/// - **hours**: Hours component (0-23)
/// - **minutes**: Minutes component (0-59)
/// - **seconds**: Seconds component (0-59)
/// - **fraction**: Fractional seconds in 100-nanosecond ticks (0-9,999,999)
/// - **is_negative**: Whether the timespan represents a negative duration
///
/// # Examples
///
/// ```
/// # use amudai_datetime::timespan::TimeSpan64;
/// let ts = TimeSpan64::dhmsml(5, 14, 30, 45, 250);
/// let specs = ts.get_timespan64_specs();
///
/// assert_eq!(specs.get_days(), 5);
/// assert_eq!(specs.get_hours(), 14);
/// assert_eq!(specs.get_minutes(), 30);
/// assert_eq!(specs.get_seconds(), 45);
/// assert_eq!(specs.get_fraction(), 2500000); // 250ms in 100ns ticks
/// assert!(!specs.is_negative());
/// ```
pub struct TimeSpan64Specs {
    days: i32,
    hours: i32,
    minutes: i32,
    seconds: i32,
    fraction: i32,
    is_negative: bool,
}

impl TimeSpan64Specs {
    /// Creates a new `TimeSpan64Specs` with the specified component values.
    ///
    /// This constructor allows manual creation of timespan specifications, useful
    /// for building custom timespan representations or testing scenarios.
    ///
    /// # Parameters
    ///
    /// * `days` - Number of complete days
    /// * `hours` - Hours component (should be 0-23 for standard representation)
    /// * `minutes` - Minutes component (should be 0-59 for standard representation)
    /// * `seconds` - Seconds component (should be 0-59 for standard representation)
    /// * `fraction` - Fractional seconds in 100-nanosecond ticks (0-9,999,999)
    /// * `is_negative` - Whether this represents a negative timespan
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64Specs;
    /// // Create specs for 2 days, 5 hours, 30 minutes, 15 seconds, 500ms
    /// let specs = TimeSpan64Specs::new(2, 5, 30, 15, 5000000, false);
    ///
    /// assert_eq!(specs.get_days(), 2);
    /// assert_eq!(specs.get_hours(), 5);
    /// assert_eq!(specs.get_fraction(), 5000000); // 500ms in ticks
    /// ```
    pub fn new(
        days: i32,
        hours: i32,
        minutes: i32,
        seconds: i32,
        fraction: i32,
        is_negative: bool,
    ) -> TimeSpan64Specs {
        TimeSpan64Specs {
            days,
            hours,
            minutes,
            seconds,
            fraction,
            is_negative,
        }
    }

    /// Returns the number of complete days in this timespan.
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// let ts = TimeSpan64::dhms(5, 25, 30, 45); // 5 days, 25 hours becomes 6 days, 1 hour
    /// let specs = ts.get_timespan64_specs();
    /// assert_eq!(specs.get_days(), 6); // Days component after normalization
    /// ```
    pub fn get_days(&self) -> i32 {
        self.days
    }

    /// Returns the hours component (0-23) of this timespan.
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// let ts = TimeSpan64::hms(14, 30, 45);
    /// let specs = ts.get_timespan64_specs();
    /// assert_eq!(specs.get_hours(), 14);
    /// ```
    pub fn get_hours(&self) -> i32 {
        self.hours
    }

    /// Returns the minutes component (0-59) of this timespan.
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// let ts = TimeSpan64::hms(2, 45, 30);
    /// let specs = ts.get_timespan64_specs();
    /// assert_eq!(specs.get_minutes(), 45);
    /// ```
    pub fn get_minutes(&self) -> i32 {
        self.minutes
    }

    /// Returns the seconds component (0-59) of this timespan.
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// let ts = TimeSpan64::hms(1, 30, 42);
    /// let specs = ts.get_timespan64_specs();
    /// assert_eq!(specs.get_seconds(), 42);
    /// ```
    pub fn get_seconds(&self) -> i32 {
        self.seconds
    }

    /// Returns the fractional seconds component in 100-nanosecond ticks.
    ///
    /// The fractional component represents the sub-second portion of the timespan
    /// as a number of 100-nanosecond ticks (0-9,999,999).
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// let ts = TimeSpan64::dhmsml(0, 0, 0, 0, 250); // 250 milliseconds
    /// let specs = ts.get_timespan64_specs();
    /// assert_eq!(specs.get_fraction(), 2500000); // 250ms = 2,500,000 ticks
    /// ```
    pub fn get_fraction(&self) -> i32 {
        self.fraction
    }

    /// Returns whether this timespan represents a negative duration.
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// let positive_ts = TimeSpan64::from_hours(5);
    /// let negative_ts = TimeSpan64::from_hours(-5);
    ///
    /// assert!(!positive_ts.get_timespan64_specs().is_negative());
    /// assert!(negative_ts.get_timespan64_specs().is_negative());
    /// ```
    pub fn is_negative(&self) -> bool {
        self.is_negative
    }
}

impl TimeSpan64 {
    /// The minimum possible `TimeSpan64` value (most negative).
    ///
    /// Represents approximately -29,227 years.
    pub const MIN: TimeSpan64 = TimeSpan64 { ticks: i64::MIN };

    /// The maximum possible `TimeSpan64` value (most positive).
    ///
    /// Represents approximately +29,227 years.
    pub const MAX: TimeSpan64 = TimeSpan64 { ticks: i64::MAX };

    /// Returns the total number of 100-nanosecond ticks in this timespan.
    ///
    /// This is the fundamental unit of measurement for `TimeSpan64`. One tick
    /// equals 100 nanoseconds. Negative values indicate backward time spans.
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// let ts = TimeSpan64::from_seconds(1);
    /// assert_eq!(ts.ticks(), 10_000_000); // 1 second = 10 million ticks
    ///
    /// let negative_ts = TimeSpan64::from_hours(-2);
    /// assert!(negative_ts.ticks() < 0);
    /// ```
    pub fn ticks(&self) -> i64 {
        self.ticks
    }

    /// Returns a detailed breakdown of this timespan into its component parts.
    ///
    /// This method decomposes the timespan into days, hours, minutes, seconds,
    /// and fractional seconds, which is useful for display and formatting purposes.
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// let ts = TimeSpan64::dhmsml(3, 14, 30, 45, 500);
    /// let specs = ts.get_timespan64_specs();
    ///
    /// assert_eq!(specs.get_days(), 3);
    /// assert_eq!(specs.get_hours(), 14);
    /// assert_eq!(specs.get_minutes(), 30);
    /// assert_eq!(specs.get_seconds(), 45);
    /// assert_eq!(specs.get_fraction(), 5_000_000); // 500ms in ticks
    /// ```
    pub fn get_timespan64_specs(&self) -> TimeSpan64Specs {
        let mut total_ticks = self.ticks;

        if total_ticks < 0 && total_ticks > TimeSpan64::MIN.ticks {
            total_ticks = -total_ticks;
        }

        let days = total_ticks / TICKS_PER_DAY;
        let mut remainder = total_ticks % TICKS_PER_DAY;
        let hours = remainder / TICKS_PER_HOUR;
        remainder %= TICKS_PER_HOUR;
        let minutes = remainder / TICKS_PER_MINUTE;
        remainder %= TICKS_PER_MINUTE;
        let seconds = remainder / TICKS_PER_SECOND;
        remainder %= TICKS_PER_SECOND;
        let fraction = remainder;
        TimeSpan64Specs::new(
            days as i32,
            hours as i32,
            minutes as i32,
            seconds as i32,
            fraction as i32,
            self.ticks < 0,
        )
    }

    /// Formats this timespan to a string and writes it to the specified writer.
    ///
    /// The timespan is formatted in the standard format:
    /// `{days}.{hours}:{minutes}:{seconds}.{fraction}`
    ///
    /// # Parameters
    ///
    /// * `w` - A writer that implements `std::io::Write`
    ///
    /// # Returns
    ///
    /// * `Ok(usize)` - Number of bytes written
    /// * `Err(std::io::Error)` - If writing fails
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// let ts = TimeSpan64::dhmsml(1, 2, 30, 45, 500);
    /// let mut buffer = Vec::new();
    /// let bytes_written = ts.format_to_writer(&mut buffer).unwrap();
    ///
    /// let formatted = String::from_utf8(buffer).unwrap();
    /// // Format: "1.02:30:45.5000000"
    /// ```
    pub fn format_to_writer<W>(&self, w: &mut W) -> io::Result<usize>
    where
        W: io::Write + ?Sized,
    {
        let mut buf = [0u8; 64];
        let len = self.format_to_buffer(&mut buf)?;
        w.write_all(&buf[..len])?;
        Ok(len)
    }

    /// Formats this timespan to a string and writes it to the provided byte buffer.
    ///
    /// The timespan is formatted in the standard format:
    /// `{days}.{hours}:{minutes}:{seconds}.{fraction}`
    ///
    /// For example: `1.02:30:45.5000000` represents 1 day, 2 hours, 30 minutes,
    /// 45 seconds, and 500 milliseconds.
    ///
    /// # Parameters
    ///
    /// * `bytes` - A mutable byte slice to write the formatted string into.
    ///   Should be at least 34 bytes to accommodate the maximum possible length.
    ///
    /// # Returns
    ///
    /// * `Ok(usize)` - Number of bytes written to the buffer
    /// * `Err(std::io::Error)` - If the buffer is too small or formatting fails
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// let ts = TimeSpan64::dhmsml(0, 1, 30, 45, 250);
    /// let mut buffer = [0u8; 64];
    /// let len = ts.format_to_buffer(&mut buffer).unwrap();
    ///
    /// let formatted = std::str::from_utf8(&buffer[..len]).unwrap();
    /// // Format: "0.01:30:45.2500000"
    /// ```
    ///
    /// # Buffer Size Requirements
    ///
    /// The maximum formatted length is 34 bytes:
    /// - Days: up to 19 digits (i64::MAX)
    /// - Hours, minutes, seconds: 2 digits each (6 total)
    /// - Fraction: 7 digits
    /// - Separators: 4 characters (".", ":", ":", ".")
    pub fn format_to_buffer(&self, bytes: &mut [u8]) -> io::Result<usize> {
        use amudai_unicode::ascii_string_builder::AsciiStringBuilder;

        // formats the timespan value into this format:
        // {days}.{hours}:{minutes}:{seconds}.{fraction}
        // e.g: print tostring(1d + 1h + 5m + 20s + 8000000tick) => 1.01:05:20.8000000
        // the assumption is that buffer size is enough large.
        // formatted string is :
        // days max length as is number of digits of max i64 which is 19 digits.
        // fraction max length is 5 digits.
        // hours, minutes and seconds are of size 2 each
        // in total the max size should be 19 + 5 + 2 * 3 + 4 (2 ':' and 2 '.') = 34
        // all functions below should succeed.

        if bytes.len() < 34 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Insufficient buffer for TimeSpan64::write",
            ));
        }

        let timespan64_specs = self.get_timespan64_specs();
        let mut sb = AsciiStringBuilder::new(bytes);

        if timespan64_specs.is_negative() {
            if self.ticks == TimeSpan64::MIN.ticks {
                sb.write_str_slice("-10675199.02:48:05.4775808".as_bytes())?;
                return Ok(sb.len());
            } else {
                // write "-"
                sb.append_ascii_character(b'-')?;
            }
        }

        if timespan64_specs.get_days() != 0 {
            sb.append_i64_as_str(timespan64_specs.get_days() as i64)?;
            sb.append_ascii_character(b'.')?;
        }

        sb.safe_append_n_digits_number(timespan64_specs.get_hours() as i64, 2)?;
        sb.append_ascii_character(b':')?;
        sb.safe_append_n_digits_number(timespan64_specs.get_minutes() as i64, 2)?;
        sb.append_ascii_character(b':')?;
        sb.safe_append_n_digits_number(timespan64_specs.get_seconds() as i64, 2)?;

        if timespan64_specs.get_fraction() != 0 {
            sb.append_ascii_character(b'.')?;
            sb.safe_append_n_digits_number(timespan64_specs.get_fraction() as i64, 7)?;
        }

        Ok(sb.len())
    }
}

// Constructors
impl TimeSpan64 {
    /// Creates a new `TimeSpan64` from the specified number of 100-nanosecond ticks.
    ///
    /// This is the most fundamental constructor, allowing direct creation from
    /// the underlying tick representation.
    ///
    /// # Parameters
    ///
    /// * `ticks` - The number of 100-nanosecond ticks. Negative values create
    ///   negative timespans.
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// // Create 1 second (10 million ticks)
    /// let one_second = TimeSpan64::new(10_000_000);
    ///
    /// // Create negative timespan
    /// let negative = TimeSpan64::new(-5_000_000); // -0.5 seconds
    /// ```
    pub fn new(ticks: i64) -> TimeSpan64 {
        TimeSpan64 { ticks }
    }

    /// Creates a `TimeSpan64` from hours, minutes, and seconds.
    ///
    /// This constructor is convenient for creating timespans from common
    /// time components without needing to specify days or fractional seconds.
    ///
    /// # Parameters
    ///
    /// * `hours` - Number of hours (can be negative for negative timespans)
    /// * `minutes` - Number of minutes
    /// * `seconds` - Number of seconds
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// // Create 2 hours, 30 minutes, 15 seconds
    /// let ts = TimeSpan64::hms(2, 30, 15);
    ///
    /// // Create negative timespan
    /// let negative_ts = TimeSpan64::hms(-1, 30, 45);
    /// ```
    pub fn hms(hours: i32, minutes: i32, seconds: i32) -> TimeSpan64 {
        TimeSpan64::new(
            hours as i64 * TICKS_PER_HOUR
                + minutes as i64 * TICKS_PER_MINUTE
                + seconds as i64 * TICKS_PER_SECOND,
        )
    }

    /// Creates a `TimeSpan64` from days, hours, minutes, and seconds.
    ///
    /// This constructor allows creation of longer timespans that include
    /// day components along with the time-of-day components.
    ///
    /// # Parameters
    ///
    /// * `days` - Number of days (can be negative for negative timespans)
    /// * `hours` - Number of hours
    /// * `minutes` - Number of minutes
    /// * `seconds` - Number of seconds
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// // Create 5 days, 12 hours, 30 minutes, 45 seconds
    /// let ts = TimeSpan64::dhms(5, 12, 30, 45);
    ///
    /// // Create negative timespan
    /// let negative_ts = TimeSpan64::dhms(-2, 6, 15, 30);
    /// ```
    pub fn dhms(days: i32, hours: i32, minutes: i32, seconds: i32) -> TimeSpan64 {
        TimeSpan64::new(
            days as i64 * TICKS_PER_DAY
                + hours as i64 * TICKS_PER_HOUR
                + minutes as i64 * TICKS_PER_MINUTE
                + seconds as i64 * TICKS_PER_SECOND,
        )
    }

    /// Attempts to create a `TimeSpan64` from days, hours, minutes, and fractional seconds.
    ///
    /// This constructor allows precise timespan creation with fractional seconds
    /// while performing overflow checking to ensure the result fits within the
    /// valid range of `TimeSpan64`.
    ///
    /// # Parameters
    ///
    /// * `days` - Number of days (must be non-negative)
    /// * `hours` - Number of hours
    /// * `minutes` - Number of minutes
    /// * `seconds` - Number of seconds (can include fractional part)
    ///
    /// # Returns
    ///
    /// * `Ok(TimeSpan64)` - Successfully created timespan
    /// * `Err(TimeSpanError)` - If parameters are invalid or result would overflow
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// // Create timespan with fractional seconds
    /// let result = TimeSpan64::try_from_dhms_fp(1, 2, 30, 45.5);
    /// assert!(result.is_ok());
    ///
    /// // Invalid negative days
    /// let result = TimeSpan64::try_from_dhms_fp(-1, 0, 0, 0.0);
    /// assert!(result.is_err());
    /// ```
    pub fn try_from_dhms_fp(
        days: i32,
        hours: i32,
        minutes: i32,
        seconds: f64,
    ) -> timespan::TimeSpanResult {
        if days < 0 {
            return Err(TimeSpanError::InvalidArgument(format!("days={days}")));
        }
        let int_seconds = seconds.trunc();
        let time_ticks = Self::try_time_to_ticks(hours, minutes, int_seconds as i32)?;
        let seconds_ticks = int_seconds as i64 * TICKS_PER_SECOND;
        let subsec_ticks =
            (seconds * TICKS_PER_SECOND as f64 - seconds_ticks as f64).round() as i64;
        let ticks = days as i64 * TICKS_PER_DAY + time_ticks + subsec_ticks;
        if (0..=MAX_TICKS).contains(&ticks) {
            Ok(TimeSpan64::new(ticks))
        } else {
            Err(TimeSpanError::TimeSpanOverflow)
        }
    }

    /// Creates a `TimeSpan64` from days, hours, minutes, seconds, and milliseconds.
    ///
    /// This is the most comprehensive constructor for creating precise timespans
    /// with millisecond precision. It's particularly useful when you need to
    /// specify all common time components.
    ///
    /// # Parameters
    ///
    /// * `days` - Number of days (can be negative for negative timespans)
    /// * `hours` - Number of hours
    /// * `minutes` - Number of minutes
    /// * `seconds` - Number of seconds
    /// * `milliseconds` - Number of milliseconds
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// // Create 2 days, 5 hours, 30 minutes, 45 seconds, 500 milliseconds
    /// let ts = TimeSpan64::dhmsml(2, 5, 30, 45, 500);
    ///
    /// // Create negative timespan
    /// let negative_ts = TimeSpan64::dhmsml(-1, 2, 15, 30, 250);
    /// ```
    pub fn dhmsml(
        days: i32,
        hours: i32,
        minutes: i32,
        seconds: i32,
        milliseconds: i32,
    ) -> TimeSpan64 {
        TimeSpan64::new(
            days as i64 * TICKS_PER_DAY
                + hours as i64 * TICKS_PER_HOUR
                + minutes as i64 * TICKS_PER_MINUTE
                + seconds as i64 * TICKS_PER_SECOND
                + milliseconds as i64 * TICKS_PER_MILLISECOND,
        )
    }

    /// Creates a `TimeSpan64` representing the specified number of days.
    ///
    /// # Parameters
    ///
    /// * `days` - Number of days (can be negative)
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// let week = TimeSpan64::from_days(7);
    /// let past_day = TimeSpan64::from_days(-1);
    /// ```
    pub fn from_days(days: i32) -> TimeSpan64 {
        TimeSpan64::new(days as i64 * TICKS_PER_DAY)
    }

    /// Creates a `TimeSpan64` representing the specified number of hours.
    ///
    /// # Parameters
    ///
    /// * `hours` - Number of hours (can be negative)
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// let eight_hours = TimeSpan64::from_hours(8);
    /// let past_hour = TimeSpan64::from_hours(-1);
    /// ```
    pub fn from_hours(hours: i32) -> TimeSpan64 {
        TimeSpan64::new(hours as i64 * TICKS_PER_HOUR)
    }

    /// Creates a `TimeSpan64` representing the specified number of minutes.
    ///
    /// # Parameters
    ///
    /// * `minutes` - Number of minutes (can be negative)
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// let thirty_minutes = TimeSpan64::from_minutes(30);
    /// let past_minutes = TimeSpan64::from_minutes(-15);
    /// ```
    pub fn from_minutes(minutes: i32) -> TimeSpan64 {
        TimeSpan64::new(minutes as i64 * TICKS_PER_MINUTE)
    }

    /// Creates a `TimeSpan64` representing the specified number of seconds.
    ///
    /// # Parameters
    ///
    /// * `seconds` - Number of seconds (can be negative)
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// let one_minute = TimeSpan64::from_seconds(60);
    /// let past_seconds = TimeSpan64::from_seconds(-30);
    /// ```
    pub fn from_seconds(seconds: i32) -> TimeSpan64 {
        TimeSpan64::new(seconds as i64 * TICKS_PER_SECOND)
    }

    /// Creates a `TimeSpan64` representing the specified number of milliseconds.
    ///
    /// # Parameters
    ///
    /// * `milliseconds` - Number of milliseconds (can be negative)
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// let half_second = TimeSpan64::from_milliseconds(500);
    /// let past_ms = TimeSpan64::from_milliseconds(-250);
    /// ```
    pub fn from_milliseconds(milliseconds: i32) -> TimeSpan64 {
        TimeSpan64::new(milliseconds as i64 * TICKS_PER_MILLISECOND)
    }

    /// Creates a `TimeSpan64` representing the specified number of microseconds.
    ///
    /// # Parameters
    ///
    /// * `microseconds` - Number of microseconds (can be negative)
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// let small_interval = TimeSpan64::from_microseconds(1500);
    /// let past_us = TimeSpan64::from_microseconds(-750);
    /// ```
    pub fn from_microseconds(microseconds: i64) -> TimeSpan64 {
        TimeSpan64::new(microseconds * TICKS_PER_MICROSECOND)
    }

    /// Creates a `TimeSpan64` representing zero duration.
    ///
    /// This is equivalent to `TimeSpan64::new(0)` but more expressive for
    /// code clarity when a zero timespan is needed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// let no_time = TimeSpan64::zero();
    /// assert_eq!(no_time.ticks(), 0);
    ///
    /// let ts = TimeSpan64::from_hours(5);
    /// let result = ts - ts; // Should equal zero
    /// assert_eq!(result, TimeSpan64::zero());
    /// ```
    pub fn zero() -> TimeSpan64 {
        TimeSpan64::new(0)
    }

    fn try_time_to_ticks(
        hour: i32,
        minute: i32,
        second: i32,
    ) -> std::result::Result<i64, TimeSpanError> {
        if !(0..=23).contains(&hour) {
            return Err(TimeSpanError::InvalidArgument(format!("hours={hour}")));
        }
        if !(0..=59).contains(&minute) {
            return Err(TimeSpanError::InvalidArgument(format!("minutes={minute}")));
        }
        if !(0..=59).contains(&second) {
            return Err(TimeSpanError::InvalidArgument(format!("seconds={second}")));
        }
        const MAX_SECONDS: i64 = i64::MAX / TICKS_PER_SECOND;
        let total_seconds: i64 = (hour as i64) * 3600 + (minute as i64) * 60 + second as i64;
        if total_seconds > MAX_SECONDS {
            Err(TimeSpanError::TimeSpanOverflow)
        } else {
            Ok(total_seconds * TICKS_PER_SECOND)
        }
    }
}

// Getters
impl TimeSpan64 {
    pub fn total_seconds(&self) -> f64 {
        self.ticks as f64 / TICKS_PER_SECOND as f64
    }

    pub fn total_milliseconds(&self) -> f64 {
        self.ticks as f64 / TICKS_PER_MILLISECOND as f64
    }
}

// Operators
impl Ord for TimeSpan64 {
    fn cmp(&self, other: &TimeSpan64) -> Ordering {
        self.ticks.cmp(&other.ticks)
    }
}

impl PartialOrd for TimeSpan64 {
    fn partial_cmp(&self, other: &TimeSpan64) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Add for TimeSpan64 {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        TimeSpan64 {
            ticks: self.ticks + other.ticks,
        }
    }
}

impl Add<DateTime64> for TimeSpan64 {
    type Output = Self;

    fn add(self, other: DateTime64) -> Self {
        TimeSpan64 {
            ticks: self.ticks + other.ticks(),
        }
    }
}

impl Sub for TimeSpan64 {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        TimeSpan64 {
            ticks: self.ticks - other.ticks,
        }
    }
}

impl AddAssign for TimeSpan64 {
    fn add_assign(&mut self, other: TimeSpan64) {
        *self = TimeSpan64 {
            ticks: self.ticks + other.ticks,
        };
    }
}

impl SubAssign for TimeSpan64 {
    fn sub_assign(&mut self, other: Self) {
        *self = TimeSpan64 {
            ticks: self.ticks - other.ticks,
        };
    }
}

impl Mul<i64> for TimeSpan64 {
    type Output = Self;

    fn mul(self, coeff: i64) -> Self {
        let ticks = self.ticks * coeff;
        TimeSpan64::new(ticks)
    }
}

impl MulAssign<i64> for TimeSpan64 {
    fn mul_assign(&mut self, coeff: i64) {
        *self = TimeSpan64 {
            ticks: self.ticks * coeff,
        };
    }
}

impl Div<i64> for TimeSpan64 {
    type Output = Self;

    fn div(self, coeff: i64) -> Self {
        assert_ne!(coeff, 0, "Cannot divide by zero-valued `Rational`!");
        let ticks = self.ticks / coeff;
        TimeSpan64::new(ticks)
    }
}

impl DivAssign<i64> for TimeSpan64 {
    fn div_assign(&mut self, coeff: i64) {
        *self = TimeSpan64 {
            ticks: self.ticks / coeff,
        };
    }
}

impl ::std::fmt::Display for TimeSpan64 {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        let mut buf = [0u8; 64];
        let res = self.format_to_buffer(&mut buf);
        match res {
            Ok(len) => {
                let s = unsafe { std::str::from_utf8_unchecked(&buf[..len]) };
                f.write_str(s)
            }
            Err(_) => Err(::std::fmt::Error),
        }
    }
}

impl Zero for TimeSpan64 {
    fn zero() -> Self {
        TimeSpan64::new(0)
    }

    fn is_zero(&self) -> bool {
        self.ticks == 0
    }
}
