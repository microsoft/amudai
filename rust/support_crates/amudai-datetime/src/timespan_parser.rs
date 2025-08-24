use super::*;
use amudai_unicode::ascii_check::IsAsciiFast;

/// A fast, flexible parser for timespan strings.
///
/// `TimeSpanParser` provides efficient parsing of timespan strings in various formats,
/// allowing extraction of duration information even from strings with embedded text.
/// The parser is designed for speed and flexibility rather than strict format compliance.
///
/// # Supported Formats
///
/// The parser can handle various timespan format patterns:
/// - `DD.HH:MM:SS.fractional` - Full format with days
/// - `HH:MM:SS.fractional` - Time-only format
/// - `MM:SS.fractional` - Minutes and seconds
/// - `SS.fractional` - Seconds only
/// - `DD.HH:MM:SS` - Without fractional seconds
/// - Embedded in text: `"duration 2.05:30:45.500 elapsed"`
///
/// # Examples
///
/// ```text
/// // Standard timespan formats
/// TimeSpanParser::try_parse("1.02:30:45.500") // 1 day, 2h 30m 45.5s
/// TimeSpanParser::try_parse("25:30:15") // 25h 30m 15s
/// TimeSpanParser::try_parse("90:45") // 90m 45s
/// TimeSpanParser::try_parse("30.750") // 30.75 seconds
///
/// // Embedded in text
/// TimeSpanParser::try_parse("elapsed 1.05:30:45 total") // 1 day 5h 30m 45s
/// ```
pub struct TimeSpanParser;

impl TimeSpanParser {
    /// Attempts to parse a timespan string using a flexible, fast parser.
    ///
    /// This function performs loose parsing of timespan strings, allowing extraction
    /// of valid duration components even from strings with additional non-timespan text.
    /// It's designed to be much faster than trying multiple predefined timespan formats.
    ///
    /// # Features
    ///
    /// - **Flexible format parsing**: Extracts timespan from strings with embedded text
    /// - **Multiple format support**: Handles various timespan notations
    /// - **High precision**: Supports up to 7 decimal places for fractional seconds
    /// - **Fast performance**: Optimized for speed over strict format validation
    /// - **Negative timespans**: Supports negative duration parsing
    ///
    /// # Expected Format Patterns
    ///
    /// The parser looks for patterns like:
    /// - `DD.HH:MM:SS.fractional` - Full format
    /// - `HH:MM:SS.fractional` - Time format
    /// - `MM:SS.fractional` - Minutes:seconds
    /// - `SS.fractional` - Seconds only
    /// - Embedded: `"text 1.05:30:45.250 more text"`
    ///
    /// # Parameters
    ///
    /// * `s` - The input string containing timespan information
    ///
    /// # Returns
    ///
    /// * `Some(i64)` - The number of 100-nanosecond ticks representing the timespan
    /// * `None` - If the string cannot be parsed as a valid timespan
    ///
    /// # Examples
    ///
    /// ```text
    /// // Full format
    /// let ticks = TimeSpanParser::try_parse("1.02:30:45.500");
    ///
    /// // Time only
    /// let ticks = TimeSpanParser::try_parse("25:30:15");
    ///
    /// // Embedded in text
    /// let ticks = TimeSpanParser::try_parse("duration 05:30:45 elapsed");
    ///
    /// // Negative timespan
    /// let ticks = TimeSpanParser::try_parse("-1.05:30:45");
    ///
    /// // Invalid format
    /// let ticks = TimeSpanParser::try_parse("not a timespan");
    /// assert!(ticks.is_none());
    /// ```
    ///
    /// # Validation Rules
    ///
    /// - Days: Any positive integer (no upper limit enforced)
    /// - Hours: 0-23 for standard format, any value for time-only format
    /// - Minutes: 0-59
    /// - Seconds: 0-59
    /// - Fractional seconds: Up to 7 digits (100-nanosecond precision)
    /// - Negative sign: Must appear at the beginning
    ///
    /// # Performance Notes
    ///
    /// This parser prioritizes speed over strict format compliance. It may accept
    /// some malformed inputs that stricter parsers would reject, but will always
    /// validate that the extracted timespan components represent a valid duration.
    pub fn try_parse(s: &str) -> Option<i64> {
        if !s.is_ascii_fast() {
            return None;
        }

        let s = s.trim();
        if s.is_empty() {
            return None;
        }

        // Check for negative sign
        let (is_negative, s) = if let Some(remaining) = s.strip_prefix('-') {
            // Check for double negative or invalid negative format
            if remaining.starts_with('-') || remaining.trim().is_empty() {
                return None;
            }
            (true, remaining)
        } else {
            (false, s)
        };

        // Skip non-digit characters from the beginning to find the timespan
        let s = Self::skip_non_digits(s)?;

        // Try to parse the timespan components
        let ticks = Self::parse_timespan_components(s)?;

        Some(if is_negative { -ticks } else { ticks })
    }

    /// Skips non-digit characters and finds the start of timespan data
    #[inline]
    fn skip_non_digits(s: &str) -> Option<&str> {
        // Find the first digit or colon
        let start_pos = s.find(|c: char| c.is_ascii_digit())?;
        Some(&s[start_pos..])
    }

    /// Parses timespan components from a cleaned string
    fn parse_timespan_components(s: &str) -> Option<i64> {
        // Extract the timespan portion and stop at first space or non-timespan character
        let timespan_part = Self::extract_timespan_portion(s)?;

        // Look for different timespan patterns
        // Priority: DD.HH:MM:SS.fff > HH:MM:SS.fff > MM:SS.fff > SS.fff

        if let Some(ticks) = Self::try_parse_full_format(timespan_part) {
            return Some(ticks);
        }

        if let Some(ticks) = Self::try_parse_time_format(timespan_part) {
            return Some(ticks);
        }

        if let Some(ticks) = Self::try_parse_minutes_seconds(timespan_part) {
            return Some(ticks);
        }

        if let Some(ticks) = Self::try_parse_seconds_only(timespan_part) {
            return Some(ticks);
        }

        None
    }

    /// Extract the timespan portion from the input string
    fn extract_timespan_portion(s: &str) -> Option<&str> {
        // Find start and end positions of the timespan
        let start = s.find(|c: char| c.is_ascii_digit())?;

        // Find the end - stop at first character that's not digit, colon, or dot
        let mut end = start;
        let chars: Vec<char> = s.chars().collect();

        for (i, &c) in chars.iter().enumerate().skip(start) {
            if c.is_ascii_digit() || c == ':' || c == '.' {
                end = i + 1;
            } else {
                break;
            }
        }

        if end > start {
            Some(&s[start..end])
        } else {
            None
        }
    }

    /// Try to parse full format: DD.HH:MM:SS.fff
    fn try_parse_full_format(s: &str) -> Option<i64> {
        // Must contain at least one dot to distinguish from time format
        if !s.contains('.') {
            return None;
        }

        // Look for pattern: digits.digits:digits:digits[.digits]
        let dot_pos = s.find('.')?;
        let days_part = &s[..dot_pos];
        let time_part = &s[dot_pos + 1..];

        // Parse days
        let days = Self::parse_number(days_part)? as i64;

        // Parse the time portion
        let time_parts: Vec<&str> = time_part.split(':').collect();
        if time_parts.len() < 3 {
            return None;
        }

        let hours = Self::parse_number(time_parts[0])? as i64;
        let minutes = Self::parse_number(time_parts[1])? as i64;

        // Handle seconds with potential fractional part
        let (seconds, fraction_ticks) = Self::parse_seconds_with_fraction(time_parts[2])?;

        // Validate ranges for full format
        if hours >= 24 || minutes >= 60 || seconds >= 60 {
            return None;
        }

        let ticks = days * TICKS_PER_DAY
            + hours * TICKS_PER_HOUR
            + minutes * TICKS_PER_MINUTE
            + seconds * TICKS_PER_SECOND
            + fraction_ticks;

        Some(ticks)
    }

    /// Try to parse time format: HH:MM:SS[.fff]
    fn try_parse_time_format(s: &str) -> Option<i64> {
        // Must not contain dots except for fractional seconds
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 3 {
            return None;
        }

        let hours = Self::parse_number(parts[0])? as i64;
        let minutes = Self::parse_number(parts[1])? as i64;

        // Handle seconds with potential fractional part
        let (seconds, fraction_ticks) = Self::parse_seconds_with_fraction(parts[2])?;

        // For time-only format, allow hours > 23 but validate minutes/seconds
        if minutes >= 60 || seconds >= 60 {
            return None;
        }

        let ticks = hours * TICKS_PER_HOUR
            + minutes * TICKS_PER_MINUTE
            + seconds * TICKS_PER_SECOND
            + fraction_ticks;

        Some(ticks)
    }

    /// Try to parse minutes:seconds format: MM:SS[.fff]
    fn try_parse_minutes_seconds(s: &str) -> Option<i64> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return None;
        }

        let minutes = Self::parse_number(parts[0])? as i64;

        // Handle seconds with potential fractional part
        let (seconds, fraction_ticks) = Self::parse_seconds_with_fraction(parts[1])?;

        // For minutes:seconds format, allow minutes > 59 but validate seconds
        if seconds >= 60 {
            return None;
        }

        let ticks = minutes * TICKS_PER_MINUTE + seconds * TICKS_PER_SECOND + fraction_ticks;

        Some(ticks)
    }

    /// Try to parse seconds only: SS[.fff]
    fn try_parse_seconds_only(s: &str) -> Option<i64> {
        // Should not contain colons (that would make it a different format)
        if s.contains(':') {
            return None;
        }

        let (seconds, fraction_ticks) = Self::parse_seconds_with_fraction(s)?;
        Some(seconds * TICKS_PER_SECOND + fraction_ticks)
    }

    /// Parse seconds with optional fractional part
    fn parse_seconds_with_fraction(s: &str) -> Option<(i64, i64)> {
        if let Some(dot_pos) = s.find('.') {
            let seconds_part = &s[..dot_pos];
            let fraction_part = &s[dot_pos + 1..];

            let seconds = Self::parse_number(seconds_part)? as i64;
            let fraction_ticks = Self::parse_fractional_seconds(fraction_part)?;

            Some((seconds, fraction_ticks))
        } else {
            let seconds = Self::parse_number(s)? as i64;
            Some((seconds, 0))
        }
    }

    /// Parse a number from a string, stopping at first non-digit
    fn parse_number(s: &str) -> Option<u32> {
        if s.is_empty() {
            return None;
        }

        let mut result = 0u32;
        let mut found_digits = false;

        for c in s.chars() {
            if c.is_ascii_digit() {
                found_digits = true;
                let digit = (c as u8 - b'0') as u32;
                result = result.checked_mul(10)?.checked_add(digit)?;
            } else {
                break; // Stop at first non-digit
            }
        }

        if found_digits { Some(result) } else { None }
    }

    /// Parse fractional seconds and convert to ticks
    fn parse_fractional_seconds(s: &str) -> Option<i64> {
        if s.is_empty() {
            return Some(0);
        }

        let mut fraction = 0u32;
        let mut digits = 0;

        for c in s.chars() {
            if c.is_ascii_digit() && digits < 7 {
                fraction = fraction * 10 + (c as u8 - b'0') as u32;
                digits += 1;
            } else {
                break;
            }
        }

        if digits == 0 {
            return Some(0);
        }

        // Convert to 100-nanosecond ticks
        // Each digit represents a power of 10 smaller than a second
        let multiplier = match digits {
            1 => 1_000_000, // tenths -> 100ns ticks
            2 => 100_000,   // hundredths
            3 => 10_000,    // milliseconds
            4 => 1_000,     // ten-thousandths
            5 => 100,       // hundred-thousandths
            6 => 10,        // millionths
            7 => 1,         // ten-millionths (100ns)
            _ => return None,
        };

        Some(fraction as i64 * multiplier)
    }
}
