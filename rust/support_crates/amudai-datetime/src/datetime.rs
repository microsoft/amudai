use super::*;
use crate::datetime_parser::DateTimeParser;
use crate::timespan::TimeSpan64;
use chrono::Duration;
use chrono::{DateTime, Datelike, NaiveDateTime, Timelike, Utc};
use num_traits::Zero;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::io;
use std::ops::{Add, Sub};
use std::str;
use std::str::FromStr;

const DAYS_TO_MONTH_365: [i32; 13] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365];
const DAYS_TO_MONTH_366: [i32; 13] = [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366];

const MIN_SECONDS: i64 = -62135596800;
const MAX_SECONDS: i64 = 253402300800;
const MIN_MILLISECONDS: i64 = MIN_SECONDS * 1000;
const MAX_MILLISECONDS: i64 = MAX_SECONDS * 1000;
const MIN_MICROSECONDS: i64 = MIN_MILLISECONDS * 1000;
const MAX_MICROSECONDS: i64 = MAX_MILLISECONDS * 1000;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PartOfDate {
    Year = 0,
    DayOfYear = 1,
    Month = 2,
    Day = 3,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum DayOfWeek {
    Sunday = 0,
    Monday = 1,
    Tuesday = 2,
    Wednesday = 3,
    Thursday = 4,
    Friday = 5,
    Saturday = 6,
}

#[derive(Copy, Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[repr(C)]
pub struct DateTime64 {
    ticks: i64,
}

impl DateTime64 {
    pub const MIN: DateTime64 = DateTime64 { ticks: 0 };
    pub const MAX: DateTime64 = DateTime64 { ticks: MAX_TICKS };

    /// Returns the number of 100-nanosecond ticks representing this DateTime64.
    ///
    /// Ticks are counted from January 1, 0001 at 00:00:00.000 (compatible with .NET DateTime).
    /// Each tick represents 100 nanoseconds.
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let dt = DateTime64::new(123456);
    /// assert_eq!(dt.ticks(), 123456);
    /// ```
    pub fn ticks(&self) -> i64 {
        self.ticks
    }
}

// Constructors
impl DateTime64 {
    /// Creates a new DateTime64 from the specified number of 100-nanosecond ticks.
    ///
    /// # Arguments
    /// * `ticks` - The number of 100-nanosecond ticks since January 1, 0001 at 00:00:00.000
    ///
    /// # Panics
    /// Panics if ticks is negative or exceeds MAX_TICKS (3652058 days from year 1).
    /// For fallible construction, use `try_from_ticks`.
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let dt = DateTime64::new(636000000000000000); // January 1, 2001
    /// assert_eq!(dt.ticks(), 636000000000000000);
    /// ```
    pub fn new(ticks: i64) -> DateTime64 {
        assert!(
            ticks >= 0,
            "Invalid ticks argument: DateTime64::new({ticks}) - ticks must be non-negative"
        );
        assert!(
            ticks <= MAX_TICKS,
            "Invalid ticks argument: DateTime64::new({ticks}) - exceeds MAX_TICKS"
        );
        DateTime64 { ticks }
    }

    /// Attempts to create a DateTime64 from the specified number of ticks.
    ///
    /// # Arguments
    /// * `ticks` - The number of 100-nanosecond ticks since January 1, 0001 at 00:00:00.000
    ///
    /// # Returns
    /// * `Ok(DateTime64)` if ticks is valid (0 <= ticks <= MAX_TICKS)
    /// * `Err(DateTimeError::DateTimeOverflow)` if ticks is out of range
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let result = DateTime64::try_from_ticks(636000000000000000);
    /// assert!(result.is_ok());
    ///
    /// let invalid = DateTime64::try_from_ticks(-1);
    /// assert!(invalid.is_err());
    /// ```
    pub fn try_from_ticks(ticks: i64) -> datetime::DateTimeResult {
        if !(0..=MAX_TICKS).contains(&ticks) {
            Err(DateTimeError::DateTimeOverflow)
        } else {
            Ok(DateTime64 { ticks })
        }
    }

    /// Checks if the specified number of ticks represents a valid DateTime64.
    ///
    /// # Arguments
    /// * `ticks` - The number of ticks to validate
    ///
    /// # Returns
    /// `true` if ticks is in the valid range (0 <= ticks <= MAX_TICKS), `false` otherwise
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// assert!(DateTime64::is_valid_ticks(0));
    /// assert!(!DateTime64::is_valid_ticks(-1));
    /// ```
    pub fn is_valid_ticks(ticks: i64) -> bool {
        (0..=MAX_TICKS).contains(&ticks)
    }

    /// Returns the broken-down components of this DateTime64.
    ///
    /// The returned `DateTime64Specs` provides access to individual date and time components
    /// like year, month, day, hour, minute, second, and sub-second parts.
    ///
    /// # Returns
    /// A `DateTime64Specs` struct with year, month, day, hour, minute, second, and fraction
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let dt = DateTime64::from_ymd_hms(2023, 5, 15, 14, 30, 45);
    /// let specs = dt.get_datetime64_specs();
    /// assert_eq!(specs.year(), 2023);
    /// assert_eq!(specs.month(), 5);
    /// assert_eq!(specs.day(), 15);
    /// ```
    pub fn get_datetime64_specs(&self) -> DateTime64Specs {
        let naive_datetime = DateTime64::new(self.ticks).create_naive_representation();
        let year = naive_datetime.year();
        let month = naive_datetime.month();
        let day = naive_datetime.day();
        let hour = naive_datetime.hour();
        let minute = naive_datetime.minute();
        let second = naive_datetime.second();
        let fraction = naive_datetime.nanosecond() / NANOSECONDS_PER_TICK as u32; // we want 7 most significant digits, nanosecond has 9 digits.
        DateTime64Specs::new(
            year,
            month as i32,
            day as i32,
            hour as i32,
            minute as i32,
            second as i32,
            fraction as i32,
        )
    }

    /// Creates a DateTime64 from the specified year, month, and day at midnight (00:00:00.000).
    ///
    /// # Arguments
    /// * `year` - The year (1-9999)
    /// * `month` - The month (1-12)
    /// * `day` - The day of the month (1-31, depending on month)
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let dt = DateTime64::from_ymd(2023, 12, 25);
    /// let specs = dt.get_datetime64_specs();
    /// assert_eq!(specs.year(), 2023);
    /// assert_eq!(specs.month(), 12);
    /// assert_eq!(specs.day(), 25);
    /// assert_eq!(specs.hour(), 0);
    /// ```
    pub fn from_ymd(year: i32, month: i32, day: i32) -> DateTime64 {
        DateTime64::new(DateTime64::date_to_ticks(year, month, day))
    }

    /// Creates a DateTime64 from the specified year, month, day, hour, minute, and second.
    ///
    /// # Arguments
    /// * `year` - The year (1-9999)
    /// * `month` - The month (1-12)
    /// * `day` - The day of the month (1-31, depending on month)
    /// * `hour` - The hour (0-23)
    /// * `minute` - The minute (0-59)
    /// * `second` - The second (0-59)
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let dt = DateTime64::from_ymd_hms(2023, 6, 15, 14, 30, 45);
    /// let specs = dt.get_datetime64_specs();
    /// assert_eq!(specs.hour(), 14);
    /// assert_eq!(specs.minute(), 30);
    /// assert_eq!(specs.second(), 45);
    /// ```
    pub fn from_ymd_hms(
        year: i32,
        month: i32,
        day: i32,
        hour: i32,
        minute: i32,
        second: i32,
    ) -> DateTime64 {
        DateTime64::new(
            DateTime64::date_to_ticks(year, month, day)
                + DateTime64::time_to_ticks(hour, minute, second),
        )
    }

    /// Creates a DateTime64 with precise sub-second components.
    ///
    /// # Arguments
    /// * `year` - The year (1-9999)
    /// * `month` - The month (1-12)
    /// * `day` - The day of the month (1-31, depending on month)
    /// * `hour` - The hour (0-23)
    /// * `minute` - The minute (0-59)
    /// * `second` - The second (0-59)
    /// * `milliseconds` - Milliseconds (0-999)
    /// * `microseconds` - Additional microseconds (0-999)
    /// * `hundreds_nanoseconds` - Additional 100-nanosecond units (0-9)
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let dt = DateTime64::from_ymd_hms_mmn(2023, 1, 1, 12, 0, 0, 123, 456, 7);
    /// let specs = dt.get_datetime64_specs();
    /// assert_eq!(specs.hour(), 12);
    /// assert_eq!(specs.minute(), 0);
    /// assert_eq!(specs.second(), 0);
    /// // fraction includes milliseconds, microseconds, and hundreds of nanoseconds
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub fn from_ymd_hms_mmn(
        year: i32,
        month: i32,
        day: i32,
        hour: i32,
        minute: i32,
        second: i32,
        milliseconds: i32,
        microseconds: i32,
        hundreds_nanoseconds: i32,
    ) -> DateTime64 {
        DateTime64::new(
            DateTime64::date_to_ticks(year, month, day)
                + DateTime64::exact_time_to_ticks(
                    hour,
                    minute,
                    second,
                    milliseconds,
                    microseconds,
                    hundreds_nanoseconds,
                ),
        )
    }

    /// Parses datetime represented by the string using all possible formats
    /// and fuzzy methods ("zzz 2014-11-08 xcxc 13:05:11 xvcc" is recognized
    /// as a valid datetime).
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<DateTime64> {
        match DateTimeParser::try_parse(s) {
            None => None,
            Some(ticks) => {
                if ticks <= MAX_TICKS {
                    Some(DateTime64::new(ticks))
                } else {
                    None
                }
            }
        }
    }

    /// Attempts to create a DateTime64 from date and time components with fractional seconds.
    ///
    /// # Arguments
    /// * `year` - The year (1-9999)
    /// * `month` - The month (1-12)
    /// * `day` - The day of the month (1-31, depending on month)
    /// * `hour` - The hour (0-23)
    /// * `minute` - The minute (0-59)
    /// * `second` - The second with fractional part (0.0-59.999...)
    ///
    /// # Returns
    /// * `Ok(DateTime64)` if all components are valid and within range
    /// * `Err(DateTimeError)` if any component is invalid or the result overflows
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let result = DateTime64::try_from_ymd_hms_fp(2023, 6, 15, 14, 30, 45.123);
    /// assert!(result.is_ok());
    /// ```
    pub fn try_from_ymd_hms_fp(
        year: i32,
        month: i32,
        day: i32,
        hour: i32,
        minute: i32,
        second: f64,
    ) -> datetime::DateTimeResult {
        let int_second = second.trunc();
        let date_ticks = Self::try_date_to_ticks(year, month, day)?;
        let time_ticks = Self::try_time_to_ticks(hour, minute, int_second as i32)?;
        let subsec_ticks = second * TICKS_PER_SECOND as f64 - int_second * TICKS_PER_SECOND as f64;
        let total_ticks = date_ticks + time_ticks + subsec_ticks.round() as i64;
        if total_ticks <= MAX_TICKS {
            Ok(DateTime64::new(total_ticks))
        } else {
            Err(DateTimeError::DateTimeOverflow)
        }
    }

    /// Converts UNIX timestamp (seconds since 1970-01-01 00:00:00 UTC) to DateTime64.
    ///
    /// # Arguments
    /// * `seconds` - The number of seconds since the UNIX epoch
    ///
    /// # Returns
    /// * `Ok(DateTime64)` if the timestamp is within the valid range
    /// * `Err(DateTimeError::DateTimeOverflow)` if the timestamp is out of range
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let result = DateTime64::try_from_unix_seconds(1609459200); // 2021-01-01 00:00:00 UTC
    /// assert!(result.is_ok());
    /// ```
    pub fn try_from_unix_seconds(seconds: i64) -> datetime::DateTimeResult {
        if !(MIN_SECONDS..MAX_SECONDS).contains(&seconds) {
            return Err(DateTimeError::DateTimeOverflow);
        }
        DateTime64::try_from_ticks(seconds * TICKS_PER_SECOND + TICKS_TILL_UNIX_TIME)
    }

    /// Converts UNIX timestamp with fractional seconds to DateTime64.
    ///
    /// # Arguments
    /// * `seconds` - The number of seconds since the UNIX epoch (with fractional part)
    ///
    /// # Returns
    /// * `Ok(DateTime64)` if the timestamp is within the valid range
    /// * `Err(DateTimeError::DateTimeOverflow)` if the timestamp is out of range
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let result = DateTime64::try_from_unix_seconds_f64(1609459200.123);
    /// assert!(result.is_ok());
    /// ```
    pub fn try_from_unix_seconds_f64(seconds: f64) -> datetime::DateTimeResult {
        if seconds < MIN_SECONDS as f64 || seconds >= MAX_SECONDS as f64 {
            return Err(DateTimeError::DateTimeOverflow);
        }
        let int_seconds = seconds.trunc();
        let sub_seconds = seconds - int_seconds;
        let ticks = int_seconds as i64 * TICKS_PER_SECOND
            + (sub_seconds * TICKS_PER_SECOND as f64) as i64
            + TICKS_TILL_UNIX_TIME;
        DateTime64::try_from_ticks(ticks)
    }

    /// Converts UNIX timestamp in milliseconds to DateTime64.
    ///
    /// # Arguments
    /// * `milliseconds` - The number of milliseconds since the UNIX epoch
    ///
    /// # Returns
    /// * `Ok(DateTime64)` if the timestamp is within the valid range
    /// * `Err(DateTimeError::DateTimeOverflow)` if the timestamp is out of range
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let result = DateTime64::try_from_unix_milliseconds(1609459200123);
    /// assert!(result.is_ok());
    /// ```
    pub fn try_from_unix_milliseconds(milliseconds: i64) -> datetime::DateTimeResult {
        if !(MIN_MILLISECONDS..MAX_MILLISECONDS).contains(&milliseconds) {
            return Err(DateTimeError::DateTimeOverflow);
        }
        DateTime64::try_from_ticks(milliseconds * TICKS_PER_MILLISECOND + TICKS_TILL_UNIX_TIME)
    }

    /// Converts UNIX timestamp in fractional milliseconds to DateTime64.
    ///
    /// # Arguments
    /// * `milliseconds` - The number of milliseconds since the UNIX epoch (with fractional part)
    ///
    /// # Returns
    /// * `Ok(DateTime64)` if the timestamp is within the valid range
    /// * `Err(DateTimeError::DateTimeOverflow)` if the timestamp is out of range
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let result = DateTime64::try_from_unix_milliseconds_f64(1609459200123.456);
    /// assert!(result.is_ok());
    /// ```
    pub fn try_from_unix_milliseconds_f64(milliseconds: f64) -> datetime::DateTimeResult {
        if milliseconds < MIN_MILLISECONDS as f64 || milliseconds >= MAX_MILLISECONDS as f64 {
            return Err(DateTimeError::DateTimeOverflow);
        }
        let int_millis = milliseconds.trunc();
        let sub_millis = milliseconds - int_millis;
        let ticks = int_millis as i64 * TICKS_PER_MILLISECOND
            + (sub_millis * TICKS_PER_MILLISECOND as f64) as i64
            + TICKS_TILL_UNIX_TIME;
        DateTime64::try_from_ticks(ticks)
    }

    /// Converts UNIX timestamp in microseconds to DateTime64.
    ///
    /// # Arguments
    /// * `microseconds` - The number of microseconds since the UNIX epoch
    ///
    /// # Returns
    /// * `Ok(DateTime64)` if the timestamp is within the valid range
    /// * `Err(DateTimeError::DateTimeOverflow)` if the timestamp is out of range
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let result = DateTime64::try_from_unix_microseconds(1609459200123456);
    /// assert!(result.is_ok());
    /// ```
    pub fn try_from_unix_microseconds(microseconds: i64) -> datetime::DateTimeResult {
        if !(MIN_MICROSECONDS..MAX_MICROSECONDS).contains(&microseconds) {
            return Err(DateTimeError::DateTimeOverflow);
        }
        DateTime64::try_from_ticks(microseconds * TICKS_PER_MICROSECOND + TICKS_TILL_UNIX_TIME)
    }

    /// Converts UNIX timestamp in fractional microseconds to DateTime64.
    ///
    /// # Arguments
    /// * `microseconds` - The number of microseconds since the UNIX epoch (with fractional part)
    ///
    /// # Returns
    /// * `Ok(DateTime64)` if the timestamp is within the valid range
    /// * `Err(DateTimeError::DateTimeOverflow)` if the timestamp is out of range
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let result = DateTime64::try_from_unix_microseconds_f64(1609459200123456.78);
    /// assert!(result.is_ok());
    /// ```
    pub fn try_from_unix_microseconds_f64(microseconds: f64) -> datetime::DateTimeResult {
        if microseconds < MIN_MICROSECONDS as f64 || microseconds >= MAX_MICROSECONDS as f64 {
            return Err(DateTimeError::DateTimeOverflow);
        }
        let int_micros = microseconds.trunc();
        let sub_micros = microseconds - int_micros;
        let ticks = int_micros as i64 * TICKS_PER_MICROSECOND
            + (sub_micros * TICKS_PER_MICROSECOND as f64) as i64
            + TICKS_TILL_UNIX_TIME;
        DateTime64::try_from_ticks(ticks)
    }

    /// Converts UNIX timestamp in nanoseconds to DateTime64.
    ///
    /// Note: Since DateTime64 uses 100-nanosecond ticks, precision below 100ns is lost.
    ///
    /// # Arguments
    /// * `nanoseconds` - The number of nanoseconds since the UNIX epoch
    ///
    /// # Returns
    /// * `Ok(DateTime64)` if the timestamp is within the valid range
    /// * `Err(DateTimeError::DateTimeOverflow)` if the timestamp is out of range
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let result = DateTime64::try_from_unix_nanoseconds(1609459200123456789);
    /// assert!(result.is_ok());
    /// ```
    pub fn try_from_unix_nanoseconds(nanoseconds: i64) -> datetime::DateTimeResult {
        DateTime64::try_from_ticks((nanoseconds / NANOSECONDS_PER_TICK) + TICKS_TILL_UNIX_TIME)
    }

    /// Converts UNIX timestamp in fractional nanoseconds to DateTime64.
    ///
    /// Note: Since DateTime64 uses 100-nanosecond ticks, precision below 100ns is lost.
    ///
    /// # Arguments
    /// * `nanoseconds` - The number of nanoseconds since the UNIX epoch (with fractional part)
    ///
    /// # Returns
    /// * `Ok(DateTime64)` if the timestamp is within the valid range
    /// * `Err(DateTimeError::DateTimeOverflow)` if the timestamp is out of range
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let result = DateTime64::try_from_unix_nanoseconds_f64(1609459200123456789.5);
    /// assert!(result.is_ok());
    /// ```
    pub fn try_from_unix_nanoseconds_f64(nanoseconds: f64) -> datetime::DateTimeResult {
        if nanoseconds < i64::MIN as f64 || nanoseconds > i64::MAX as f64 {
            return Err(DateTimeError::DateTimeOverflow);
        }
        DateTime64::try_from_ticks(
            (nanoseconds.trunc() as i64 / NANOSECONDS_PER_TICK) + TICKS_TILL_UNIX_TIME,
        )
    }
}

pub struct DateTime64Specs {
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    minute: i32,
    second: i32,
    fraction: i32,
}

impl DateTime64Specs {
    /// Creates a new DateTime64Specs with the specified components.
    ///
    /// # Arguments
    /// * `year` - The year component
    /// * `month` - The month component (1-12)
    /// * `day` - The day component (1-31)
    /// * `hour` - The hour component (0-23)
    /// * `minute` - The minute component (0-59)
    /// * `second` - The second component (0-59)
    /// * `fraction` - The fractional second component in 100-nanosecond units
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64Specs;
    /// let specs = DateTime64Specs::new(2023, 6, 15, 14, 30, 45, 1234567);
    /// assert_eq!(specs.year(), 2023);
    /// assert_eq!(specs.hour(), 14);
    /// ```
    pub fn new(
        year: i32,
        month: i32,
        day: i32,
        hour: i32,
        minute: i32,
        second: i32,
        fraction: i32,
    ) -> DateTime64Specs {
        DateTime64Specs {
            year,
            month,
            day,
            hour,
            minute,
            second,
            fraction,
        }
    }

    /// Get year component (immutable)
    pub const fn year(&self) -> i32 {
        self.year
    }

    /// Get month component (immutable)
    pub const fn month(&self) -> i32 {
        self.month
    }

    /// Get day component (immutable)
    pub const fn day(&self) -> i32 {
        self.day
    }

    /// Get hour component (immutable)
    pub const fn hour(&self) -> i32 {
        self.hour
    }

    /// Get minute component (immutable)
    pub const fn minute(&self) -> i32 {
        self.minute
    }

    /// Get second component (immutable)
    pub const fn second(&self) -> i32 {
        self.second
    }

    /// Get fraction component (immutable)
    pub const fn fraction(&self) -> i32 {
        self.fraction
    }
}

impl FromStr for DateTime64 {
    type Err = ::std::io::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        use std::io::{Error, ErrorKind};

        DateTime64::from_str(s).ok_or(Error::from(ErrorKind::InvalidInput))
    }
}

impl PartialOrd for DateTime64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DateTime64 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ticks.cmp(&other.ticks)
    }
}

// Note: DateTime + DateTime removed as it's semantically invalid
// Use DateTime + TimeSpan64 for adding durations to dates

impl Add<TimeSpan64> for DateTime64 {
    type Output = Self;

    fn add(self, other: TimeSpan64) -> DateTime64 {
        let result = self.ticks.saturating_add(other.ticks());
        DateTime64 {
            ticks: result.clamp(0, MAX_TICKS),
        }
    }
}

impl Sub for DateTime64 {
    type Output = TimeSpan64;

    fn sub(self, other: DateTime64) -> TimeSpan64 {
        TimeSpan64::new(self.ticks.saturating_sub(other.ticks))
    }
}

impl Sub<TimeSpan64> for DateTime64 {
    type Output = Self;

    fn sub(self, other: TimeSpan64) -> DateTime64 {
        let result = self.ticks.saturating_sub(other.ticks());
        DateTime64 {
            ticks: result.clamp(0, MAX_TICKS),
        }
    }
}

// Improved arithmetic operations with explicit error handling
impl DateTime64 {
    /// Add a TimeSpan64 to this DateTime64, returning an error if overflow occurs.
    /// Use this for explicit overflow checking instead of the saturating `+` operator.
    pub fn checked_add(self, timespan: TimeSpan64) -> Result<DateTime64, DateTimeError> {
        let result_ticks = self
            .ticks
            .checked_add(timespan.ticks())
            .ok_or(DateTimeError::DateTimeOverflow)?;

        if !(0..=MAX_TICKS).contains(&result_ticks) {
            Err(DateTimeError::DateTimeOverflow)
        } else {
            Ok(DateTime64::new(result_ticks))
        }
    }

    /// Subtract a TimeSpan64 from this DateTime64, returning an error if underflow occurs.
    /// Use this for explicit underflow checking instead of the saturating `-` operator.
    pub fn checked_sub(self, timespan: TimeSpan64) -> Result<DateTime64, DateTimeError> {
        let result_ticks = self
            .ticks
            .checked_sub(timespan.ticks())
            .ok_or(DateTimeError::DateTimeOverflow)?;

        if !(0..=MAX_TICKS).contains(&result_ticks) {
            Err(DateTimeError::DateTimeOverflow)
        } else {
            Ok(DateTime64::new(result_ticks))
        }
    }

    /// Add a TimeSpan64 to this DateTime64, saturating at the bounds.
    /// This is equivalent to the `+` operator but more explicit about the behavior.
    pub fn saturating_add(self, timespan: TimeSpan64) -> DateTime64 {
        let result_ticks = self.ticks.saturating_add(timespan.ticks());
        DateTime64::new(result_ticks.clamp(0, MAX_TICKS))
    }

    /// Subtract a TimeSpan64 from this DateTime64, saturating at the bounds.
    /// This is equivalent to the `-` operator but more explicit about the behavior.
    pub fn saturating_sub(self, timespan: TimeSpan64) -> DateTime64 {
        let result_ticks = self.ticks.saturating_sub(timespan.ticks());
        DateTime64::new(result_ticks.clamp(0, MAX_TICKS))
    }
}

// Public get methods
impl DateTime64 {
    /// Returns the year component of this DateTime64.
    ///
    /// # Returns
    /// The year as an integer (1-9999)
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let dt = DateTime64::from_ymd(2023, 6, 15);
    /// assert_eq!(dt.year(), 2023);
    /// ```
    pub fn year(&self) -> i32 {
        let naive_date_time: NaiveDateTime = self.create_naive_representation();
        naive_date_time.year()
    }

    /// Returns the month component of this DateTime64.
    ///
    /// # Returns
    /// The month as an integer (1-12)
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let dt = DateTime64::from_ymd(2023, 6, 15);
    /// assert_eq!(dt.month(), 6);
    /// ```
    pub fn month(&self) -> i32 {
        let naive_date_time: NaiveDateTime = self.create_naive_representation();
        naive_date_time.month() as i32
    }

    /// Returns the day component of this DateTime64.
    ///
    /// # Returns
    /// The day of the month as an integer (1-31)
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let dt = DateTime64::from_ymd(2023, 6, 15);
    /// assert_eq!(dt.day(), 15);
    /// ```
    pub fn day(&self) -> i32 {
        let naive_date_time: NaiveDateTime = self.create_naive_representation();
        naive_date_time.day() as i32
    }

    /// Returns a DateTime64 representing the date portion (without time) of this DateTime64.
    ///
    /// The returned DateTime64 will have the same year, month, and day, but with
    /// hour, minute, second, and fractional components set to zero.
    ///
    /// # Returns
    /// A new DateTime64 representing midnight of the same date
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let dt = DateTime64::from_ymd_hms(2023, 6, 15, 14, 30, 45);
    /// let date_only = dt.date();
    /// assert_eq!(date_only.year(), 2023);
    /// assert_eq!(date_only.month(), 6);
    /// assert_eq!(date_only.day(), 15);
    /// // Time components should be zero
    /// let specs = date_only.get_datetime64_specs();
    /// assert_eq!(specs.hour(), 0);
    /// assert_eq!(specs.minute(), 0);
    /// assert_eq!(specs.second(), 0);
    /// ```
    pub fn date(&self) -> DateTime64 {
        // Get the date components and create a new DateTime64 at midnight
        let specs = self.get_datetime64_specs();
        DateTime64::from_ymd(specs.year(), specs.month(), specs.day())
    }

    /// Returns the day of the year (1-366) for this DateTime64.
    ///
    /// # Returns
    /// The ordinal day of the year (1 = January 1st)
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let dt = DateTime64::from_ymd(2023, 1, 1);
    /// assert_eq!(dt.day_of_year(), 1);
    ///
    /// let dt2 = DateTime64::from_ymd(2023, 12, 31);
    /// assert_eq!(dt2.day_of_year(), 365); // Non-leap year
    /// ```
    pub fn day_of_year(&self) -> i32 {
        let naive_date_time: NaiveDateTime = self.create_naive_representation();
        naive_date_time.ordinal() as i32
    }

    /// Returns the month of the year (1-12) for this DateTime64.
    ///
    /// This is an alias for the `month()` method.
    ///
    /// # Returns
    /// The month as an integer (1-12)
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let dt = DateTime64::from_ymd(2023, 6, 15);
    /// assert_eq!(dt.month_of_year(), 6);
    /// assert_eq!(dt.month_of_year(), dt.month());
    /// ```
    pub fn month_of_year(&self) -> i32 {
        let naive_date_time: NaiveDateTime = self.create_naive_representation();
        naive_date_time.month() as i32
    }

    /// Returns the ISO week number (1-53) for this DateTime64.
    ///
    /// Uses the ISO 8601 definition where week 1 is the first week with at least
    /// 4 days in the new year, and weeks start on Monday.
    ///
    /// # Returns
    /// The ISO week number (1-53)
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let dt = DateTime64::from_ymd(2023, 1, 2); // Monday of first week
    /// assert_eq!(dt.week_of_year(), 1);
    /// ```
    pub fn week_of_year(&self) -> i32 {
        let naive_date_time: NaiveDateTime = self.create_naive_representation();
        naive_date_time.iso_week().week() as i32
    }

    /// Returns the start of the year (January 1st at midnight) with an optional year offset.
    ///
    /// # Arguments
    /// * `years_offset` - Number of years to add/subtract (can be negative)
    ///
    /// # Returns
    /// * `Some(DateTime64)` representing January 1st of the target year at midnight
    /// * `None` if the target year is outside the valid range (1-9999)
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let dt = DateTime64::from_ymd(2023, 6, 15);
    /// let start_of_year = dt.start_of_year(0).unwrap();
    /// assert_eq!(start_of_year.year(), 2023);
    /// assert_eq!(start_of_year.month(), 1);
    /// assert_eq!(start_of_year.day(), 1);
    ///
    /// let next_year = dt.start_of_year(1).unwrap();
    /// assert_eq!(next_year.year(), 2024);
    /// ```
    pub fn start_of_year(&self, years_offset: i64) -> Option<DateTime64> {
        let naive_date_time: NaiveDateTime = self.create_naive_representation();
        let year = naive_date_time.year() + years_offset as i32;
        if !(1..=9999).contains(&year) {
            return None;
        }
        Some(DateTime64::from_ymd(year, 1, 1))
    }

    /// Returns the end of the year (December 31st at 23:59:59.9999999) with an optional year offset.
    ///
    /// # Arguments
    /// * `years_offset` - Number of years to add/subtract (can be negative)
    ///
    /// # Returns
    /// * `Some(DateTime64)` representing December 31st of the target year at 23:59:59.9999999
    /// * `None` if the target year is outside the valid range (1-9999)
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let dt = DateTime64::from_ymd(2023, 6, 15);
    /// let end_of_year = dt.end_of_year(0).unwrap();
    /// assert_eq!(end_of_year.year(), 2023);
    /// assert_eq!(end_of_year.month(), 12);
    /// assert_eq!(end_of_year.day(), 31);
    ///
    /// let prev_year = dt.end_of_year(-1).unwrap();
    /// assert_eq!(prev_year.year(), 2022);
    /// ```
    pub fn end_of_year(&self, years_offset: i64) -> Option<DateTime64> {
        let naive_date_time: NaiveDateTime = self.create_naive_representation();
        let year = naive_date_time.year() + years_offset as i32;
        if !(1..=9999).contains(&year) {
            return None;
        }
        Some(DateTime64::from_ymd_hms_mmn(
            year, 12, 31, 23, 59, 59, 999, 999, 9,
        ))
    }

    /// Returns the start of the month (1st day at midnight) with an optional month offset.
    ///
    /// # Arguments
    /// * `months_offset` - Number of months to add/subtract (can be negative)
    ///
    /// # Returns
    /// * `Some(DateTime64)` representing the 1st day of the target month at midnight
    /// * `None` if the calculation results in a date outside the valid range (years 1-9999)
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let dt = DateTime64::from_ymd(2023, 6, 15);
    /// let start_of_month = dt.start_of_month(0).unwrap();
    /// assert_eq!(start_of_month.year(), 2023);
    /// assert_eq!(start_of_month.month(), 6);
    /// assert_eq!(start_of_month.day(), 1);
    ///
    /// let next_month = dt.start_of_month(1).unwrap();
    /// assert_eq!(next_month.month(), 7);
    /// ```
    pub fn start_of_month(&self, months_offset: i64) -> Option<DateTime64> {
        let naive_date_time = self.create_naive_representation();
        let years_offset = months_offset as i32 / 12;
        let months_offset = months_offset as i32 - years_offset * 12;
        let mut year = naive_date_time.year() + years_offset;
        let mut month = naive_date_time.month() as i32 + months_offset;
        if month <= 0 {
            month += 12;
            year -= 1;
        } else if month > 12 {
            month -= 12;
            year += 1;
        }
        if !(1..=9999).contains(&year) {
            return None;
        }
        Some(DateTime64::from_ymd(year, month, 1))
    }

    /// Returns the end of the month (last day at 23:59:59.9999999) with an optional month offset.
    ///
    /// # Arguments
    /// * `months_offset` - Number of months to add/subtract (can be negative)
    ///
    /// # Returns
    /// * `Some(DateTime64)` representing the last day of the target month at 23:59:59.9999999
    /// * `None` if the calculation results in a date outside the valid range (years 1-9999)
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let dt = DateTime64::from_ymd(2023, 6, 15);
    /// let end_of_month = dt.end_of_month(0).unwrap();
    /// assert_eq!(end_of_month.year(), 2023);
    /// assert_eq!(end_of_month.month(), 6);
    /// assert_eq!(end_of_month.day(), 30); // June has 30 days
    /// ```
    pub fn end_of_month(&self, months_offset: i64) -> Option<DateTime64> {
        let naive_date_time = self.create_naive_representation();
        let years_offset = months_offset as i32 / 12;
        let months_offset = months_offset as i32 - years_offset * 12;
        let mut year = naive_date_time.year() + years_offset;
        let mut month = naive_date_time.month() as i32 + months_offset;
        if month <= 0 {
            month += 12;
            year -= 1;
        } else if month > 12 {
            month -= 12;
            year += 1;
        }
        if !(1..=9999).contains(&year) {
            return None;
        }
        let day = Self::days_in_month(year, month);
        Some(DateTime64::from_ymd_hms_mmn(
            year, month, day, 23, 59, 59, 999, 999, 9,
        ))
    }

    /// Returns the start of the week (Sunday at midnight) with an optional week offset.
    ///
    /// # Arguments
    /// * `weeks_offset` - Number of weeks to add/subtract (can be negative)
    ///
    /// # Returns
    /// * `Some(DateTime64)` representing the Sunday of the target week at midnight
    /// * `None` if the calculation results in a date outside the valid range (years 1-9999)
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let dt = DateTime64::from_ymd(2023, 6, 15); // Thursday
    /// let start_of_week = dt.start_of_week(0).unwrap();
    /// // Will be the Sunday before June 15th, 2023
    /// assert_eq!(start_of_week.day(), 11); // June 11, 2023 was a Sunday
    /// ```
    pub fn start_of_week(&self, weeks_offset: i64) -> Option<DateTime64> {
        let naive_date_time = self.create_naive_representation();
        let days_offset = Duration::try_days(weeks_offset * 7);
        days_offset.and_then(|days_offset| {
            naive_date_time
                .checked_add_signed(days_offset)
                .and_then(|d| {
                    let d = d.sub(
                        Duration::try_days(d.weekday().num_days_from_sunday() as i64)
                            .expect("Duration: less than a week"),
                    );
                    let year = d.year();
                    if !(1..=9999).contains(&year) {
                        None
                    } else {
                        Some(DateTime64::from_ymd(year, d.month() as i32, d.day() as i32))
                    }
                })
        })
    }

    /// Returns the end of the week (Saturday at 23:59:59.9999999) with an optional week offset.
    ///
    /// # Arguments
    /// * `weeks_offset` - Number of weeks to add/subtract (can be negative)
    ///
    /// # Returns
    /// * `Some(DateTime64)` representing the Saturday of the target week at 23:59:59.9999999
    /// * `None` if the calculation results in a date outside the valid range (years 1-9999)
    ///
    /// # Examples
    /// ```rust
    /// # use amudai_datetime::datetime::DateTime64;
    /// let dt = DateTime64::from_ymd(2023, 6, 15); // Thursday
    /// let end_of_week = dt.end_of_week(0).unwrap();
    /// // Will be the Saturday after June 15th, 2023
    /// assert_eq!(end_of_week.day(), 17); // June 17, 2023 was a Saturday
    /// ```
    pub fn end_of_week(&self, weeks_offset: i64) -> Option<DateTime64> {
        let naive_date_time = self.create_naive_representation();
        let days_offset = Duration::try_days((weeks_offset + 1) * 7);
        days_offset.and_then(|days_offset| {
            naive_date_time
                .checked_add_signed(days_offset)
                .and_then(|d| {
                    let d = d.sub(
                        Duration::try_days(d.weekday().num_days_from_sunday() as i64)
                            .expect("Duration: less than a week"),
                    );
                    let year = d.year();
                    if !(1..=9999).contains(&year) {
                        None
                    } else {
                        Some(
                            DateTime64::from_ymd(year, d.month() as i32, d.day() as i32)
                                .sub(TimeSpan64::new(1)),
                        )
                    }
                })
        })
    }

    pub fn start_of_day(&self, days_offset: i64) -> Option<DateTime64> {
        let naive_date_time = self.create_naive_representation();
        Duration::try_days(days_offset).and_then(|days_offset| {
            naive_date_time
                .checked_add_signed(days_offset)
                .and_then(|d| {
                    let year = d.year();
                    if !(1..=9999).contains(&year) {
                        None
                    } else {
                        Some(DateTime64::from_ymd(year, d.month() as i32, d.day() as i32))
                    }
                })
        })
    }

    pub fn end_of_day(&self, days_offset: i64) -> Option<DateTime64> {
        let naive_date_time = self.create_naive_representation();
        Duration::try_days(days_offset + 1).and_then(|days_offset| {
            naive_date_time
                .checked_add_signed(days_offset)
                .and_then(|d| {
                    let year = d.year();
                    if !(1..=9999).contains(&year) {
                        None
                    } else {
                        Some(
                            DateTime64::from_ymd(year, d.month() as i32, d.day() as i32)
                                .sub(TimeSpan64::new(1)),
                        )
                    }
                })
        })
    }

    pub fn hour(&self) -> i32 {
        ((self.ticks / TICKS_PER_HOUR) % 24) as i32
    }

    pub fn minute(&self) -> i32 {
        ((self.ticks / TICKS_PER_MINUTE) % 60) as i32
    }

    pub fn second(&self) -> i32 {
        ((self.ticks / TICKS_PER_SECOND) % 60) as i32
    }

    pub fn millisecond(&self) -> i32 {
        ((self.ticks / TICKS_PER_MILLISECOND) % 1000) as i32
    }

    pub fn microsecond(&self) -> i32 {
        ((self.ticks / TICKS_PER_MICROSECOND) % 1000) as i32
    }

    pub fn hundred_nanosecond(&self) -> i32 {
        (self.ticks % 10) as i32
    }

    pub fn time_of_day(&self) -> i64 {
        self.ticks % TICKS_PER_DAY
    }

    pub fn today() -> DateTime64 {
        let utc: DateTime<Utc> = Utc::now();
        let result: DateTime64 =
            DateTime64::from_ymd(utc.year(), utc.month() as i32, utc.day() as i32);
        result
    }

    pub fn utc_now() -> DateTime64 {
        let utc: DateTime<Utc> = Utc::now();
        let seconds = utc.timestamp();
        let nanoseconds = utc.timestamp_subsec_nanos();
        DateTime64::new(
            TICKS_TILL_UNIX_TIME
                + seconds * TICKS_PER_SECOND
                + (nanoseconds as i64) / NANOSECONDS_PER_TICK,
        )
    }

    pub fn format_to_writer<W>(&self, w: &mut W) -> io::Result<usize>
    where
        W: io::Write + ?Sized,
    {
        let mut buf = [0u8; 64];
        let len = self.format_to_buffer(&mut buf)?;
        w.write_all(&buf[..len])?;
        Ok(len)
    }

    pub fn format_to_buffer(&self, bytes: &mut [u8]) -> io::Result<usize> {
        use amudai_unicode::ascii_string_builder::AsciiStringBuilder;

        // formats the datetime value into this format:
        // '{year}-{month}-{day}T{hour}:{minute}:{second}.{millisecond}{microsecond}{nanosecond}Z'
        // e.g: tostring(datetime(2019-07-22 19:05:40.54543224)) => "2019-07-22T19:05:40.5454322Z"
        // the assumption is that buffer size is enough large:
        // year - 4 chars.
        // month - 2 chars.
        // day - 2 chars.
        // hour - 2 chars.
        // minute - 2 chars.
        // second - 2 chars.
        // fraction - 7 chars(7 MSD in fraction).
        // delimiters - 7 chars.
        // total = 28
        // all functions below should succeed.

        if bytes.len() < 28 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Insufficient buffer for DateTime64::write_iso_format",
            ));
        }

        let datetime_specs = self.get_datetime64_specs();
        let mut sb = AsciiStringBuilder::new(bytes);
        sb.safe_append_n_digits_number(datetime_specs.year() as i64, 4)?;
        sb.append_ascii_character(b'-')?;
        sb.safe_append_n_digits_number(datetime_specs.month() as i64, 2)?;
        sb.append_ascii_character(b'-')?;
        sb.safe_append_n_digits_number(datetime_specs.day() as i64, 2)?;
        sb.append_ascii_character(b'T')?;
        sb.safe_append_n_digits_number(datetime_specs.hour() as i64, 2)?;
        sb.append_ascii_character(b':')?;
        sb.safe_append_n_digits_number(datetime_specs.minute() as i64, 2)?;
        sb.append_ascii_character(b':')?;
        sb.safe_append_n_digits_number(datetime_specs.second() as i64, 2)?;
        sb.append_ascii_character(b'.')?;
        sb.safe_append_n_digits_number(datetime_specs.fraction() as i64, 7)?;
        sb.append_ascii_character(b'Z')?;

        Ok(sb.len())
    }
}

impl ::std::fmt::Display for DateTime64 {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        self.create_naive_representation()
            .format("%Y-%m-%d %H:%M:%S.%f")
            .fmt(f)
    }
}

impl DateTime64 {
    pub fn create_naive_representation(&self) -> NaiveDateTime {
        let diff_from_unix_ticks = self.ticks - TICKS_TILL_UNIX_TIME;
        let mut secs = (self.ticks - TICKS_TILL_UNIX_TIME) / TICKS_PER_SECOND;
        let mut nsecs = ((i64::abs(diff_from_unix_ticks) % TICKS_PER_SECOND) * 100) as u32;
        if (diff_from_unix_ticks < 0) && (nsecs > 0) {
            secs -= 1;
            nsecs = 1000000000 - nsecs;
        }
        let naive_date_time: NaiveDateTime = DateTime::from_timestamp(secs, nsecs)
            .expect("naive_date_time")
            .naive_utc();
        naive_date_time
    }
}

// Static utility methods
impl DateTime64 {
    pub fn is_leap_year(year: i32) -> bool {
        assert!(
            (1..=9999).contains(&year),
            "Invalid year argument: DateTime64::is_leap_year({year})"
        );
        year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
    }

    fn date_to_ticks(year: i32, month: i32, day: i32) -> i64 {
        Self::try_date_to_ticks(year, month, day).expect("invalid date")
    }

    pub fn try_date_to_ticks(
        year: i32,
        month: i32,
        day: i32,
    ) -> std::result::Result<i64, DateTimeError> {
        if !(1..=9999).contains(&year) {
            return Err(DateTimeError::InvalidArgument(format!("year={year}")));
        }
        if !(1..=12).contains(&month) {
            return Err(DateTimeError::InvalidArgument(format!("month={month}")));
        }

        let days = if DateTime64::is_leap_year(year) {
            DAYS_TO_MONTH_366
        } else {
            DAYS_TO_MONTH_365
        };
        if day < 1 || day > days[month as usize] - days[(month - 1) as usize] {
            return Err(DateTimeError::InvalidArgument(format!("day={day}")));
        }

        let y = year - 1;
        let n = y * 365 + y / 4 - y / 100 + y / 400 + days[(month - 1) as usize] + day - 1;
        Ok(n as i64 * TICKS_PER_DAY)
    }

    fn time_to_ticks(hour: i32, minute: i32, second: i32) -> i64 {
        Self::try_time_to_ticks(hour, minute, second).expect("invalid time")
    }

    fn try_time_to_ticks(
        hour: i32,
        minute: i32,
        second: i32,
    ) -> std::result::Result<i64, DateTimeError> {
        if !(0..=23).contains(&hour) {
            return Err(DateTimeError::InvalidArgument(format!("hour={hour}")));
        }
        if !(0..=59).contains(&minute) {
            return Err(DateTimeError::InvalidArgument(format!("minute={minute}")));
        }
        if !(0..=59).contains(&second) {
            return Err(DateTimeError::InvalidArgument(format!("second={second}")));
        }
        const MAX_SECONDS: i64 = i64::MAX / TICKS_PER_SECOND;
        let total_seconds: i64 = (hour as i64) * 3600 + (minute as i64) * 60 + second as i64;
        if total_seconds > MAX_SECONDS {
            Err(DateTimeError::DateTimeOverflow)
        } else {
            Ok(total_seconds * TICKS_PER_SECOND)
        }
    }

    fn exact_time_to_ticks(
        hour: i32,
        minute: i32,
        second: i32,
        milliseconds: i32,
        microseconds: i32,
        hundreds_nanoseconds: i32,
    ) -> i64 {
        assert!(
            (0..24).contains(&hour),
            "Invalid hour argument: DateTime64::exact_time_to_ticks({hour})"
        );
        assert!(
            (0..60).contains(&minute),
            "Invalid minute argument: DateTime64::exact_time_to_ticks({hour})"
        );
        assert!(
            (0..60).contains(&second),
            "Invalid second argument: DateTime64::exact_time_to_ticks({hour})"
        );
        assert!(
            (0..1000).contains(&milliseconds),
            "Invalid milliseconds argument: DateTime64::exact_time_to_ticks({milliseconds})"
        );
        assert!(
            (0..1000).contains(&microseconds),
            "Invalid microseconds argument: DateTime64::exact_time_to_ticks({microseconds})"
        );
        assert!(
            (0..10).contains(&hundreds_nanoseconds),
            "Invalid nanoseconds argument: DateTime64::exact_time_to_ticks({hundreds_nanoseconds})"
        );

        const MAX_SECONDS: i64 = i64::MAX / TICKS_PER_SECOND;

        // totalSeconds is bounded by 2^31 * 2^12 + 2^31 * 2^8 + 2^31,
        // which is less than 2^44, meaning we won't overflow totalSeconds.
        let total_seconds: i64 = (hour as i64) * 3600 + (minute as i64) * 60 + second as i64;
        if total_seconds > MAX_SECONDS {
            panic!("Invalid totalSeconds in exact_time_to_ticks {total_seconds}");
        } else {
            total_seconds * TICKS_PER_SECOND
                + milliseconds as i64 * TICKS_PER_MILLISECOND
                + microseconds as i64 * TICKS_PER_MICROSECOND
                + hundreds_nanoseconds as i64
        }
    }

    pub fn days_in_month(year: i32, month: i32) -> i32 {
        assert!(
            (1..=12).contains(&month),
            "Invalid month argument: DateTime64::days_in_month({month})"
        );
        // IsLeapYear checks the year argument
        let days = if DateTime64::is_leap_year(year) {
            DAYS_TO_MONTH_366
        } else {
            DAYS_TO_MONTH_365
        };
        days[month as usize] - days[(month - 1) as usize]
    }
}

// Note: Add<Self> implementation kept only for Zero trait compatibility
// This should not be used directly in user code - use DateTime + TimeSpan64 instead
impl Add for DateTime64 {
    type Output = Self;

    fn add(self, other: Self) -> DateTime64 {
        // For Zero trait compatibility only - this should not be used in practice
        let result = self.ticks.saturating_add(other.ticks);
        DateTime64 {
            ticks: result.min(MAX_TICKS),
        }
    }
}

impl Zero for DateTime64 {
    fn zero() -> Self {
        DateTime64::new(0)
    }

    fn is_zero(&self) -> bool {
        self.ticks == 0
    }
}
