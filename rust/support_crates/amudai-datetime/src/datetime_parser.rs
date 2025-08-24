use super::*;
use amudai_unicode::ascii_check::IsAsciiFast;
use chrono::NaiveDate;
use std::sync::OnceLock;

pub(crate) struct DateTimeParser;

static MIN_DATE: OnceLock<NaiveDate> = OnceLock::new();

fn get_min_date() -> &'static NaiveDate {
    MIN_DATE.get_or_init(|| NaiveDate::from_ymd_opt(1, 1, 1).unwrap())
}

impl DateTimeParser {
    const NANOSECONDS_EXPONENT: usize = 9;
    const PADDING_FACTORS: &'static [u32] = &[
        1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000,
    ];

    /// Attempts to parse a datetime string using a flexible, fast parser.
    ///
    /// This function performs very loose parsing of datetime strings, allowing extraction
    /// of valid datetime components even from strings with additional non-datetime text.
    /// It's designed to be much faster than trying multiple predefined datetime formats.
    ///
    /// # Features
    ///
    /// - **Flexible format parsing**: Extracts datetime from strings with embedded text
    /// - **Multiple format support**: Handles ISO 8601, space-separated, and mixed formats
    /// - **Microsecond precision**: Supports up to 7 decimal places for fractional seconds
    /// - **Fast performance**: Optimized for speed over strict format validation
    ///
    /// # Expected Format Patterns
    ///
    /// The parser looks for patterns like:
    /// - `YYYY-MM-DD HH:MM:SS.fractional`
    /// - `YYYY-MM-DDTHH:MM:SS.fractional` (ISO 8601)
    /// - Embedded in text: `"prefix 2014-11-08 text 13:05:11 suffix"`
    ///
    /// # Parameters
    ///
    /// * `s` - The input string containing datetime information
    ///
    /// # Returns
    ///
    /// * `Some(i64)` - The number of 100-nanosecond ticks since January 1, 2001 UTC
    /// * `None` - If the string cannot be parsed as a valid datetime
    ///
    /// # Examples
    ///
    /// ```text
    /// // Standard ISO format
    /// let ticks = DateTimeParser::try_parse("2018-02-04T16:13:30.1171988");
    /// assert!(ticks.is_some());
    ///
    /// // Space-separated format
    /// let ticks = DateTimeParser::try_parse("2014-11-08 15:55:55");
    /// assert!(ticks.is_some());
    ///
    /// // Embedded in text
    /// let ticks = DateTimeParser::try_parse("log 2014-11-08 info 13:05:11 end");
    /// assert!(ticks.is_some());
    ///
    /// // Invalid format
    /// let ticks = DateTimeParser::try_parse("not a date");
    /// assert!(ticks.is_none());
    /// ```
    ///
    /// # Validation Rules
    ///
    /// - Year: 1-9999 (4 digits expected)
    /// - Month: 1-12
    /// - Day: 1-31 (validated against month)
    /// - Hour: 0-23
    /// - Minute: 0-59
    /// - Second: 0-59
    /// - Fractional seconds: Up to 7 digits (100-nanosecond precision)
    /// - Leap year validation for February 29th
    ///
    /// # Performance Notes
    ///
    /// This parser prioritizes speed over strict format compliance. It may accept
    /// some malformed inputs that stricter parsers would reject, but will always
    /// validate that the extracted datetime components represent a valid date and time.
    pub fn try_parse(s: &str) -> Option<i64> {
        if !s.is_ascii_fast() {
            return None;
        }
        let s = Self::trim_utc_timezones(s);
        let s = Self::skip_non_digits(s, false, false);

        // Expect at least 8 digits, 4 for year, 2 for month and 2 for day
        if s.len() < 8 {
            return None;
        }

        let (year, s) = s.split_at(4);
        let year = Self::str_to_u32(year)? as i32; // Casting from u32 to i32 will never hit overflow as number is maximum of 4 digits

        let s = Self::skip_non_digits(s, false, false);

        // Expect at least 4 digits, 2 for month and 2 for day
        if s.len() < 4 {
            return None;
        }

        let (month, s) = s.split_at(2);
        let month = Self::str_to_u32(month)?;
        if !(1..=12).contains(&month) {
            return None;
        }

        let s = Self::skip_non_digits(s, false, false);

        // Expect at least 2 digits for day
        if s.len() < 2 {
            return None;
        }

        let (day, s) = s.split_at(2);
        let day = Self::str_to_u32(day)?;
        if !(1..=31).contains(&day) {
            return None;
        }

        let s = Self::skip_non_digits(s, false, false);

        // Expect at least 6 digits, 2 for hours, 2 for minutes and 2 for seconds
        if s.len() < 6 {
            return None;
        }

        let (hour, s) = s.split_at(2);
        let hour = Self::str_to_u32(hour)?;
        if hour > 23 {
            return None;
        }

        let s = Self::skip_non_digits(s, false, false);

        // Expect at least 4 digits for minutes and seconds
        if s.len() < 4 {
            return None;
        }

        let (minute, s) = s.split_at(2);
        let minute = Self::str_to_u32(minute)?;
        if minute > 59 {
            return None;
        }

        let s = Self::skip_non_digits(s, false, false);

        // Expect at least 2 digits for seconds
        if s.len() < 2 {
            return None;
        }

        let (second, s) = s.split_at(2);
        let second = Self::str_to_u32(second)?;
        if second > 59 {
            return None;
        }

        // Remove non-digit prefixes until the end or until reach a dot, in that case, try parse nanoseconds
        let s = Self::skip_non_digits(s, true, false);

        // No nanoseconds provided - A valid case
        if s.is_empty() {
            return Self::extract_ticks_from_date(year, month, day, hour, minute, second, 0);
        }

        if !s.starts_with('.') {
            return None;
        }
        let s = &s[1..]; // Remove dot

        // Remove non-digit suffixes
        let nanosecond = Self::skip_non_digits(s, false, true);

        // No nanoseconds provided - A valid case
        if nanosecond.is_empty() {
            return Self::extract_ticks_from_date(year, month, day, hour, minute, second, 0);
        }

        if nanosecond.is_empty() || nanosecond.len() > Self::NANOSECONDS_EXPONENT {
            return None;
        }

        let num_of_nano_digits = nanosecond.len();
        let nanosecond = Self::str_to_u32(nanosecond)?;
        let padding_index = Self::NANOSECONDS_EXPONENT - num_of_nano_digits;
        let nanosecond = nanosecond * Self::PADDING_FACTORS[padding_index];

        Self::extract_ticks_from_date(year, month, day, hour, minute, second, nanosecond)
    }

    fn trim_utc_timezones(mut s: &str) -> &str {
        if s.ends_with("Z") {
            s = &s[..s.len() - 1];
        } else if s.ends_with("GMT") || s.ends_with("UTC") {
            s = &s[..s.len() - 3];
        }
        s.trim()
    }

    #[inline]
    fn skip_non_digits(s: &str, keep_dot: bool, backward: bool) -> &str {
        let str_len = s.len();

        if str_len == 0 {
            return s;
        }

        let mut iter = s.chars();
        let mut num_of_chars_to_skip = 0;

        while num_of_chars_to_skip < str_len {
            let c = if backward {
                // Go over the string from the end backwards in order to remove non-digit suffixes
                iter.next_back()
            } else {
                // Go over the string from the beginning forwards in order to remove non-digit prefixes
                iter.next()
            };

            if let Some(c) = c {
                // Break when first encounter a digit or a dot in case parsing nanoseconds
                if c.is_ascii_digit() || (c == '.' && keep_dot) {
                    break;
                }
            } else {
                return s;
            }

            num_of_chars_to_skip += 1;
        }

        if backward {
            // Remove non-digit suffixes
            let to_remove = str_len - num_of_chars_to_skip;
            &s[..to_remove]
        } else {
            // Remove non-digit prefixes
            &s[num_of_chars_to_skip..]
        }
    }

    fn str_to_u32(s: &str) -> Option<u32> {
        let mut res: u32 = 0;

        for &item in s.as_bytes().iter() {
            let c = item as char;
            if c.is_ascii_digit() {
                res *= 10;
                res += c as u32 - '0' as u32;
            } else {
                return None;
            }
        }

        Some(res)
    }

    fn extract_ticks_from_date(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
        nanosecond: u32,
    ) -> Option<i64> {
        chrono::NaiveDate::from_ymd_opt(year, month, day).and_then(|span| {
            let span = span.signed_duration_since(*get_min_date());
            Self::extract_ticks_from_span(span, 0).map(|res| {
                let mut ticks = res + (hour as i64 * TICKS_PER_HOUR);
                ticks += minute as i64 * TICKS_PER_MINUTE;
                ticks += second as i64 * TICKS_PER_SECOND;
                ticks += (nanosecond as i64) / 100;

                ticks
            })
        })
    }

    fn extract_ticks_from_span(span: chrono::Duration, nanosecond: u32) -> Option<i64> {
        // In the year of this writing (2019 CE) number of nanoseconds passed since 0001-01-01 00:00
        // won't fit in 64 bit integer
        if let Some(ns) = span.num_nanoseconds() {
            Some(ns / NANOSECONDS_PER_TICK)
        } else {
            span.num_microseconds().map(|ms| {
                ms * TICKS_PER_MICROSECOND + ((nanosecond % 1000) as i64 / NANOSECONDS_PER_TICK)
            })
        }
    }
}
