use smallvec::SmallVec;
use std::io::Write;

use crate::{datetime::DateTime64, timespan::TimeSpan64};

#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
enum DateTimeFormatSpec {
    d,
    dd,
    f,
    ff,
    fff,
    ffff,
    fffff,
    ffffff,
    fffffff,
    F,
    FF,
    FFF,
    FFFF,
    FFFFF,
    FFFFFF,
    FFFFFFF,
    h,
    hh,
    H,
    HH,
    m,
    mm,
    M,
    MM,
    s,
    ss,
    y,
    yy,
    yyyy,
    tt,
    Delimiter(u8),
}

impl DateTimeFormatSpec {
    pub fn from_str(s: &str) -> Result<DateTimeFormatSpec, String> {
        let spec = match s {
            "d" => DateTimeFormatSpec::d,
            "dd" => DateTimeFormatSpec::dd,
            "f" => DateTimeFormatSpec::f,
            "ff" => DateTimeFormatSpec::ff,
            "fff" => DateTimeFormatSpec::fff,
            "ffff" => DateTimeFormatSpec::ffff,
            "fffff" => DateTimeFormatSpec::fffff,
            "ffffff" => DateTimeFormatSpec::ffffff,
            "fffffff" => DateTimeFormatSpec::fffffff,
            "F" => DateTimeFormatSpec::F,
            "FF" => DateTimeFormatSpec::FF,
            "FFF" => DateTimeFormatSpec::FFF,
            "FFFF" => DateTimeFormatSpec::FFFF,
            "FFFFF" => DateTimeFormatSpec::FFFFF,
            "FFFFFF" => DateTimeFormatSpec::FFFFFF,
            "FFFFFFF" => DateTimeFormatSpec::FFFFFFF,
            "h" => DateTimeFormatSpec::h,
            "hh" => DateTimeFormatSpec::hh,
            "H" => DateTimeFormatSpec::H,
            "HH" => DateTimeFormatSpec::HH,
            "m" => DateTimeFormatSpec::m,
            "mm" => DateTimeFormatSpec::mm,
            "M" => DateTimeFormatSpec::M,
            "MM" => DateTimeFormatSpec::MM,
            "s" => DateTimeFormatSpec::s,
            "ss" => DateTimeFormatSpec::ss,
            "y" => DateTimeFormatSpec::y,
            "yy" => DateTimeFormatSpec::yy,
            "yyyy" => DateTimeFormatSpec::yyyy,
            "tt" => DateTimeFormatSpec::tt,
            _ if s.len() == 1 => DateTimeFormatSpec::Delimiter(s.as_bytes()[0]),
            _ => return Err(format!("Invalid format specifier: {s}")),
        };
        Ok(spec)
    }

    pub fn write<W>(&self, dt: DateTime64, w: &mut W) -> Result<(), std::io::Error>
    where
        W: Write,
    {
        match *self {
            DateTimeFormatSpec::d => write!(w, "{}", dt.day()),
            DateTimeFormatSpec::dd => write!(w, "{:02}", dt.day()),
            DateTimeFormatSpec::f => write!(w, "{}", dt.millisecond() / 100),
            DateTimeFormatSpec::ff => write!(w, "{:02}", dt.millisecond() / 10),
            DateTimeFormatSpec::fff => write!(w, "{:03}", dt.millisecond()),
            DateTimeFormatSpec::ffff => write!(
                w,
                "{:04}",
                (1000 * dt.millisecond() + dt.microsecond()) / 100
            ),
            DateTimeFormatSpec::fffff => write!(
                w,
                "{:05}",
                (1000 * dt.millisecond() + dt.microsecond()) / 10
            ),
            DateTimeFormatSpec::ffffff => {
                write!(w, "{:06}", 1000 * dt.millisecond() + dt.microsecond())
            }
            DateTimeFormatSpec::fffffff => write!(
                w,
                "{:07}",
                10000 * dt.millisecond() + dt.microsecond() * 10 + dt.hundred_nanosecond()
            ),
            DateTimeFormatSpec::F => {
                let value = dt.millisecond() / 100;
                if value != 0 {
                    write!(w, "{value}")
                } else {
                    Ok(())
                }
            }
            DateTimeFormatSpec::FF => {
                let value = dt.millisecond() / 10;
                if value != 0 {
                    write!(w, "{value:02}")
                } else {
                    Ok(())
                }
            }
            DateTimeFormatSpec::FFF => {
                let value = dt.millisecond();
                if value != 0 {
                    write!(w, "{value:03}")
                } else {
                    Ok(())
                }
            }
            DateTimeFormatSpec::FFFF => {
                let value = (1000 * dt.millisecond() + dt.microsecond()) / 100;
                if value != 0 {
                    write!(w, "{value:04}")
                } else {
                    Ok(())
                }
            }
            DateTimeFormatSpec::FFFFF => {
                let value = (1000 * dt.millisecond() + dt.microsecond()) / 10;
                if value != 0 {
                    write!(w, "{value:05}")
                } else {
                    Ok(())
                }
            }
            DateTimeFormatSpec::FFFFFF => {
                let value = 1000 * dt.millisecond() + dt.microsecond();
                if value != 0 {
                    write!(w, "{value:06}")
                } else {
                    Ok(())
                }
            }
            DateTimeFormatSpec::FFFFFFF => {
                let value =
                    10000 * dt.millisecond() + 10 * dt.microsecond() + dt.hundred_nanosecond();
                if value != 0 {
                    write!(w, "{value:07}")
                } else {
                    Ok(())
                }
            }
            DateTimeFormatSpec::h => write!(w, "{}", Self::get_ampm_hour(dt.hour())),
            DateTimeFormatSpec::hh => write!(w, "{:02}", Self::get_ampm_hour(dt.hour())),
            DateTimeFormatSpec::H => write!(w, "{}", dt.hour()),
            DateTimeFormatSpec::HH => write!(w, "{:02}", dt.hour()),
            DateTimeFormatSpec::m => write!(w, "{}", dt.minute()),
            DateTimeFormatSpec::mm => write!(w, "{:02}", dt.minute()),
            DateTimeFormatSpec::M => write!(w, "{}", dt.month()),
            DateTimeFormatSpec::MM => write!(w, "{:02}", dt.month()),
            DateTimeFormatSpec::s => write!(w, "{}", dt.second()),
            DateTimeFormatSpec::ss => write!(w, "{:02}", dt.second()),
            DateTimeFormatSpec::y => write!(w, "{}", dt.year() % 100),
            DateTimeFormatSpec::yy => write!(w, "{:02}", dt.year() % 100),
            DateTimeFormatSpec::yyyy => write!(w, "{:04}", dt.year()),
            DateTimeFormatSpec::tt => w.write_all(Self::get_ampm_str(dt.hour())),
            DateTimeFormatSpec::Delimiter(ch) => w.write_all(&[ch]),
        }
    }

    #[inline]
    fn get_ampm_hour(hour: i32) -> i32 {
        if hour == 0 {
            12
        } else if hour > 12 {
            hour - 12
        } else {
            hour
        }
    }

    #[inline]
    fn get_ampm_str(hour: i32) -> &'static [u8] {
        if hour < 12 { b"AM" } else { b"PM" }
    }
}

/// A high-performance datetime formatter that supports custom format patterns.
///
/// `DateTimeFormat` provides a flexible and efficient way to format `DateTime64` values
/// using format specification strings. It uses an internal buffer for optimal performance
/// when formatting multiple datetime values with the same format pattern.
///
/// # Format Specifications
///
/// The formatter supports the following format specifiers:
///
/// ## Date Components
/// - `d` - Day of month (1-31)
/// - `dd` - Day of month with leading zero (01-31)
/// - `M` - Month (1-12)
/// - `MM` - Month with leading zero (01-12)
/// - `y` - Year, last two digits (00-99)
/// - `yy` - Year, last two digits (00-99)
/// - `yyyy` - Four-digit year (e.g., 2021)
///
/// ## Time Components
/// - `h` - Hour in 12-hour format (1-12)
/// - `hh` - Hour in 12-hour format with leading zero (01-12)
/// - `H` - Hour in 24-hour format (0-23)
/// - `HH` - Hour in 24-hour format with leading zero (00-23)
/// - `m` - Minute (0-59)
/// - `mm` - Minute with leading zero (00-59)
/// - `s` - Second (0-59)
/// - `ss` - Second with leading zero (00-59)
/// - `tt` - AM/PM designator
///
/// ## Fractional Seconds
/// - `f` - Tenths of a second
/// - `ff` - Hundredths of a second
/// - `fff` - Milliseconds (3 digits)
/// - `ffff` - Ten-thousandths of a second (4 digits)
/// - `fffff` - Hundred-thousandths of a second (5 digits)
/// - `ffffff` - Millionths of a second (6 digits)
/// - `fffffff` - Ten-millionths of a second (7 digits)
/// - `F` through `FFFFFFF` - Same as above but without trailing zeros
///
/// ## Custom Delimiters
/// Any literal text can be included between format specifiers.
///
/// # Performance
///
/// The formatter uses an internal buffer that is reused across multiple formatting
/// operations, minimizing memory allocations for optimal performance.
pub struct DateTimeFormat {
    specs: Vec<DateTimeFormatSpec>,
    buf: SmallVec<[u8; 256]>,
}

impl DateTimeFormat {
    /// Creates a new `DateTimeFormat` from an iterable collection of format specification strings.
    ///
    /// This method constructs a datetime formatter by parsing each format specification
    /// string in the provided collection. The specifications define how datetime components
    /// should be formatted and any literal text to include in the output.
    ///
    /// # Parameters
    ///
    /// * `specs` - An iterable collection of format specification strings. Each string
    ///   should contain a valid format specifier (like "yyyy", "MM", "dd") or literal text
    ///   to include in the formatted output.
    ///
    /// # Returns
    ///
    /// * `Ok(DateTimeFormat)` - A new formatter configured with the provided specifications
    /// * `Err(String)` - An error message if any format specification is invalid
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::format::DateTimeFormat;
    /// # use amudai_datetime::datetime::DateTime64;
    /// // Create a formatter for ISO 8601 format
    /// let mut formatter = DateTimeFormat::from_specs(vec![
    ///     "yyyy", "-", "MM", "-", "dd", "T", "HH", ":", "mm", ":", "ss"
    /// ]).unwrap();
    ///
    /// // Create a formatter with fractional seconds
    /// let mut detailed_formatter = DateTimeFormat::from_specs(vec![
    ///     "yyyy", "-", "MM", "-", "dd", " ", "HH", ":", "mm", ":", "ss", ".", "fffffff"
    /// ]).unwrap();
    ///
    /// // Invalid format specification will return an error
    /// let result = DateTimeFormat::from_specs(vec!["invalid"]);
    /// assert!(result.is_err());
    /// ```
    ///
    /// # Format Specification Examples
    ///
    /// - `["yyyy", "-", "MM", "-", "dd"]` → "2021-10-11"
    /// - `["h", ":", "mm", " ", "tt"]` → "9:25 PM"
    /// - `["yyyy", "-", "MM", "-", "dd", " ", "HH", ":", "mm", ":", "ss", ".", "fff"]` → "2021-10-11 21:25:45.804"
    pub fn from_specs<I, S>(specs: I) -> Result<DateTimeFormat, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let specs = specs
            .into_iter()
            .map(|spec| DateTimeFormatSpec::from_str(spec.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(DateTimeFormat {
            specs,
            buf: SmallVec::new(),
        })
    }

    /// Formats a `DateTime64` value and writes the result to the specified writer.
    ///
    /// This method applies the format specifications configured in this formatter
    /// to the provided datetime value and writes the formatted string directly
    /// to any type that implements the `Write` trait.
    ///
    /// # Parameters
    ///
    /// * `dt` - The `DateTime64` value to format
    /// * `w` - A mutable reference to a writer that implements `std::io::Write`
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If the formatting and writing succeeded
    /// * `Err(std::io::Error)` - If an I/O error occurred during writing
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::format::DateTimeFormat;
    /// # use amudai_datetime::datetime::DateTime64;
    /// # use std::io::Write;
    /// let formatter = DateTimeFormat::from_specs(vec![
    ///     "yyyy", "-", "MM", "-", "dd", " ", "HH", ":", "mm", ":", "ss"
    /// ]).unwrap();
    ///
    /// let dt = DateTime64::from_ymd_hms(2021, 10, 11, 21, 25, 45);
    /// let mut buffer = Vec::new();
    ///
    /// formatter.write(dt, &mut buffer).unwrap();
    /// assert_eq!(String::from_utf8(buffer).unwrap(), "2021-10-11 21:25:45");
    /// ```
    ///
    /// # Performance Notes
    ///
    /// This method avoids internal buffer allocation and writes directly to the
    /// provided writer, making it suitable for streaming applications or when
    /// writing to files, network sockets, or other I/O destinations.
    pub fn write<W>(&self, dt: DateTime64, w: &mut W) -> Result<(), std::io::Error>
    where
        W: Write,
    {
        for spec in self.specs.iter() {
            spec.write(dt, w)?;
        }
        Ok(())
    }

    /// Formats a `DateTime64` value using the internal buffer and returns a byte slice reference.
    ///
    /// This method is optimized for high-performance formatting scenarios where the same
    /// formatter is used repeatedly. It reuses an internal buffer to minimize memory
    /// allocations and returns a reference to the formatted datetime string as bytes.
    ///
    /// # Parameters
    ///
    /// * `dt` - The `DateTime64` value to format
    ///
    /// # Returns
    ///
    /// A byte slice (`&[u8]`) containing the formatted datetime string. The returned
    /// reference is valid until the next call to `render()` on the same formatter instance.
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::format::DateTimeFormat;
    /// # use amudai_datetime::datetime::DateTime64;
    /// let mut formatter = DateTimeFormat::from_specs(vec![
    ///     "yyyy", "-", "MM", "-", "dd", " ", "HH", ":", "mm", ":", "ss", ".", "fff"
    /// ]).unwrap();
    ///
    /// let dt = DateTime64::from_ymd_hms_mmn(2021, 10, 11, 21, 25, 45, 804, 123, 5);
    /// let formatted_bytes = formatter.render(dt);
    /// let formatted_str = std::str::from_utf8(formatted_bytes).unwrap();
    ///
    /// assert_eq!(formatted_str, "2021-10-11 21:25:45.804");
    /// ```
    ///
    /// # Performance Notes
    ///
    /// - The internal buffer is reused across multiple calls, making this method
    ///   very efficient for repeated formatting operations
    /// - The buffer is automatically sized to handle typical datetime strings
    /// - For maximum performance, reuse the same `DateTimeFormat` instance
    ///
    /// # Safety
    ///
    /// The returned byte slice reference becomes invalid when:
    /// - The formatter is dropped
    /// - `render()` is called again on the same formatter
    /// - The formatter is moved
    pub fn render(&mut self, dt: DateTime64) -> &[u8] {
        unsafe { self.buf.set_len(0) }
        for spec in self.specs.iter() {
            spec.write(dt, &mut self.buf).expect("write to buf");
        }
        &self.buf
    }
}

#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
enum TimeSpanFormatSpec {
    d,
    dd,
    ddd,
    dddd,
    ddddd,
    dddddd,
    ddddddd,
    dddddddd,
    f,
    ff,
    fff,
    ffff,
    fffff,
    ffffff,
    fffffff,
    F,
    FF,
    FFF,
    FFFF,
    FFFFF,
    FFFFFF,
    FFFFFFF,
    h,
    hh,
    H,
    HH,
    m,
    mm,
    s,
    ss,
    Delimiter(u8),
}

impl TimeSpanFormatSpec {
    pub fn from_str(s: &str) -> Result<TimeSpanFormatSpec, String> {
        let spec = match s {
            "d" => TimeSpanFormatSpec::d,
            "dd" => TimeSpanFormatSpec::dd,
            "ddd" => TimeSpanFormatSpec::ddd,
            "dddd" => TimeSpanFormatSpec::dddd,
            "ddddd" => TimeSpanFormatSpec::ddddd,
            "dddddd" => TimeSpanFormatSpec::dddddd,
            "ddddddd" => TimeSpanFormatSpec::ddddddd,
            "dddddddd" => TimeSpanFormatSpec::dddddddd,
            "f" => TimeSpanFormatSpec::f,
            "ff" => TimeSpanFormatSpec::ff,
            "fff" => TimeSpanFormatSpec::fff,
            "ffff" => TimeSpanFormatSpec::ffff,
            "fffff" => TimeSpanFormatSpec::fffff,
            "ffffff" => TimeSpanFormatSpec::ffffff,
            "fffffff" => TimeSpanFormatSpec::fffffff,
            "F" => TimeSpanFormatSpec::F,
            "FF" => TimeSpanFormatSpec::FF,
            "FFF" => TimeSpanFormatSpec::FFF,
            "FFFF" => TimeSpanFormatSpec::FFFF,
            "FFFFF" => TimeSpanFormatSpec::FFFFF,
            "FFFFFF" => TimeSpanFormatSpec::FFFFFF,
            "FFFFFFF" => TimeSpanFormatSpec::FFFFFFF,
            "h" => TimeSpanFormatSpec::h,
            "hh" => TimeSpanFormatSpec::hh,
            "H" => TimeSpanFormatSpec::H,
            "HH" => TimeSpanFormatSpec::HH,
            "m" => TimeSpanFormatSpec::m,
            "mm" => TimeSpanFormatSpec::mm,
            "s" => TimeSpanFormatSpec::s,
            "ss" => TimeSpanFormatSpec::ss,
            _ if s.len() == 1 => TimeSpanFormatSpec::Delimiter(s.as_bytes()[0]),
            _ => return Err(format!("Invalid format specifier: {s}")),
        };
        Ok(spec)
    }

    pub fn write<W>(&self, ts: TimeSpan64, w: &mut W) -> Result<(), std::io::Error>
    where
        W: Write,
    {
        let specs = ts.get_timespan64_specs();
        match *self {
            TimeSpanFormatSpec::d => write!(w, "{}", specs.get_days()),
            TimeSpanFormatSpec::dd => write!(w, "{:02}", specs.get_days()),
            TimeSpanFormatSpec::ddd => write!(w, "{:03}", specs.get_days()),
            TimeSpanFormatSpec::dddd => write!(w, "{:04}", specs.get_days()),
            TimeSpanFormatSpec::ddddd => write!(w, "{:05}", specs.get_days()),
            TimeSpanFormatSpec::dddddd => write!(w, "{:06}", specs.get_days()),
            TimeSpanFormatSpec::ddddddd => write!(w, "{:07}", specs.get_days()),
            TimeSpanFormatSpec::dddddddd => write!(w, "{:08}", specs.get_days()),

            TimeSpanFormatSpec::f => write!(w, "{}", specs.get_fraction() / 1000000),
            TimeSpanFormatSpec::ff => write!(w, "{:02}", specs.get_fraction() / 100000),
            TimeSpanFormatSpec::fff => write!(w, "{:03}", specs.get_fraction() / 10000),
            TimeSpanFormatSpec::ffff => write!(w, "{:04}", specs.get_fraction() / 1000),
            TimeSpanFormatSpec::fffff => write!(w, "{:05}", specs.get_fraction() / 100),
            TimeSpanFormatSpec::ffffff => write!(w, "{:06}", specs.get_fraction() / 10),
            TimeSpanFormatSpec::fffffff => write!(w, "{:07}", specs.get_fraction()),

            TimeSpanFormatSpec::F => {
                let value = specs.get_fraction() / 1000000;
                if value != 0 {
                    write!(w, "{value}")
                } else {
                    Ok(())
                }
            }
            TimeSpanFormatSpec::FF => {
                let value = specs.get_fraction() / 100000;
                if value != 0 {
                    write!(w, "{value:02}")
                } else {
                    Ok(())
                }
            }
            TimeSpanFormatSpec::FFF => {
                let value = specs.get_fraction() / 10000;
                if value != 0 {
                    write!(w, "{value:03}")
                } else {
                    Ok(())
                }
            }
            TimeSpanFormatSpec::FFFF => {
                let value = specs.get_fraction() / 1000;
                if value != 0 {
                    write!(w, "{value:04}")
                } else {
                    Ok(())
                }
            }
            TimeSpanFormatSpec::FFFFF => {
                let value = specs.get_fraction() / 100;
                if value != 0 {
                    write!(w, "{value:05}")
                } else {
                    Ok(())
                }
            }
            TimeSpanFormatSpec::FFFFFF => {
                let value = specs.get_fraction() / 10;
                if value != 0 {
                    write!(w, "{value:06}")
                } else {
                    Ok(())
                }
            }
            TimeSpanFormatSpec::FFFFFFF => {
                let value = specs.get_fraction();
                if value != 0 {
                    write!(w, "{value:07}")
                } else {
                    Ok(())
                }
            }
            TimeSpanFormatSpec::h | TimeSpanFormatSpec::H => write!(w, "{}", specs.get_hours()),
            TimeSpanFormatSpec::hh | TimeSpanFormatSpec::HH => {
                write!(w, "{:02}", specs.get_hours())
            }
            TimeSpanFormatSpec::m => write!(w, "{}", specs.get_minutes()),
            TimeSpanFormatSpec::mm => write!(w, "{:02}", specs.get_minutes()),
            TimeSpanFormatSpec::s => write!(w, "{}", specs.get_seconds()),
            TimeSpanFormatSpec::ss => write!(w, "{:02}", specs.get_seconds()),
            TimeSpanFormatSpec::Delimiter(ch) => w.write_all(&[ch]),
        }
    }
}

/// A high-performance timespan formatter that supports custom format patterns.
///
/// `TimeSpanFormat` provides a flexible and efficient way to format `TimeSpan64` values
/// using format specification strings. It uses an internal buffer for optimal performance
/// when formatting multiple timespan values with the same format pattern.
///
/// # Format Specifications
///
/// The formatter supports the following format specifiers for timespan components:
///
/// ## Day Components
/// - `d` - Total days (can be any number)
/// - `dd` - Total days with at least 2 digits (zero-padded)
/// - `ddd` - Total days with at least 3 digits (zero-padded)
/// - `dddd` - Total days with at least 4 digits (zero-padded)
/// - `ddddd` - Total days with at least 5 digits (zero-padded)
/// - `dddddd` - Total days with at least 6 digits (zero-padded)
/// - `ddddddd` - Total days with at least 7 digits (zero-padded)
///
/// ## Time Components
/// - `h` - Hours (0-23)
/// - `hh` - Hours with leading zero (00-23)
/// - `m` - Minutes (0-59)
/// - `mm` - Minutes with leading zero (00-59)
/// - `s` - Seconds (0-59)
/// - `ss` - Seconds with leading zero (00-59)
///
/// ## Fractional Seconds
/// - `f` - Tenths of a second
/// - `ff` - Hundredths of a second
/// - `fff` - Milliseconds (3 digits)
/// - `ffff` - Ten-thousandths of a second (4 digits)
/// - `fffff` - Hundred-thousandths of a second (5 digits)
/// - `ffffff` - Millionths of a second (6 digits)
/// - `fffffff` - Ten-millionths of a second (7 digits)
/// - `F` through `FFFFFFF` - Same as above but without trailing zeros
///
/// ## Custom Delimiters
/// Any literal text can be included between format specifiers.
///
/// # Negative Timespans
///
/// Negative timespans are automatically prefixed with a minus sign (-) and the
/// absolute values of all components are displayed.
///
/// # Performance
///
/// The formatter uses an internal buffer that is reused across multiple formatting
/// operations, minimizing memory allocations for optimal performance.
pub struct TimeSpanFormat {
    specs: Vec<TimeSpanFormatSpec>,
    buf: SmallVec<[u8; 256]>,
}

impl TimeSpanFormat {
    /// Creates a new `TimeSpanFormat` from an iterable collection of format specification strings.
    ///
    /// This method constructs a timespan formatter by parsing each format specification
    /// string in the provided collection. The specifications define how timespan components
    /// should be formatted and any literal text to include in the output.
    ///
    /// # Parameters
    ///
    /// * `specs` - An iterable collection of format specification strings. Each string
    ///   should contain a valid timespan format specifier (like "ddd", "hh", "mm") or literal text
    ///   to include in the formatted output.
    ///
    /// # Returns
    ///
    /// * `Ok(TimeSpanFormat)` - A new formatter configured with the provided specifications
    /// * `Err(String)` - An error message if any format specification is invalid
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::format::TimeSpanFormat;
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// // Create a formatter for standard timespan format
    /// let mut formatter = TimeSpanFormat::from_specs(vec![
    ///     "ddd", ".", "hh", ":", "mm", ":", "ss"
    /// ]).unwrap();
    ///
    /// // Create a formatter with fractional seconds
    /// let mut detailed_formatter = TimeSpanFormat::from_specs(vec![
    ///     "dd", ".", "hh", ":", "mm", ":", "ss", ".", "fffffff"
    /// ]).unwrap();
    ///
    /// // Invalid format specification will return an error
    /// let result = TimeSpanFormat::from_specs(vec!["invalid"]);
    /// assert!(result.is_err());
    /// ```
    ///
    /// # Format Specification Examples
    ///
    /// - `["ddd", ".", "hh", ":", "mm", ":", "ss"]` → "012.10:45:31"
    /// - `["h", ":", "mm"]` → "10:45"
    /// - `["ddddd", ".", "hh", ":", "mm", ":", "ss", ".", "fff"]` → "00012.10:45:31.365"
    pub fn from_specs<I, S>(specs: I) -> Result<TimeSpanFormat, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let specs = specs
            .into_iter()
            .map(|spec| TimeSpanFormatSpec::from_str(spec.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TimeSpanFormat {
            specs,
            buf: SmallVec::new(),
        })
    }

    /// Formats a `TimeSpan64` value and writes the result to the specified writer.
    ///
    /// This method applies the format specifications configured in this formatter
    /// to the provided timespan value and writes the formatted string directly
    /// to any type that implements the `Write` trait. Negative timespans are
    /// automatically prefixed with a minus sign.
    ///
    /// # Parameters
    ///
    /// * `ts` - The `TimeSpan64` value to format
    /// * `w` - A mutable reference to a writer that implements `std::io::Write`
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If the formatting and writing succeeded
    /// * `Err(std::io::Error)` - If an I/O error occurred during writing
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::format::TimeSpanFormat;
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// # use std::io::Write;
    /// let formatter = TimeSpanFormat::from_specs(vec![
    ///     "ddd", ".", "hh", ":", "mm", ":", "ss"
    /// ]).unwrap();
    ///
    /// let ts = TimeSpan64::dhmsml(12, 10, 45, 31, 365);
    /// let mut buffer = Vec::new();
    ///
    /// formatter.write(ts, &mut buffer).unwrap();
    /// assert_eq!(String::from_utf8(buffer).unwrap(), "012.10:45:31");
    /// ```
    ///
    /// # Negative Timespan Handling
    ///
    /// Negative timespans are automatically handled by prefixing the result with
    /// a minus sign and using absolute values for all components:
    ///
    /// ```text
    /// TimeSpan64::dhmsml(-1, 2, 3, 4, 500) → "-00.21:56:55"
    /// ```
    ///
    /// # Performance Notes
    ///
    /// This method avoids internal buffer allocation and writes directly to the
    /// provided writer, making it suitable for streaming applications or when
    /// writing to files, network sockets, or other I/O destinations.
    pub fn write<W>(&self, ts: TimeSpan64, w: &mut W) -> Result<(), std::io::Error>
    where
        W: Write,
    {
        if ts.ticks() < 0 {
            w.write_all(b"-")?;
        }
        for spec in self.specs.iter() {
            spec.write(ts, w)?;
        }
        Ok(())
    }

    /// Formats a `TimeSpan64` value using the internal buffer and returns a byte slice reference.
    ///
    /// This method is optimized for high-performance formatting scenarios where the same
    /// formatter is used repeatedly. It reuses an internal buffer to minimize memory
    /// allocations and returns a reference to the formatted timespan string as bytes.
    /// Negative timespans are automatically prefixed with a minus sign.
    ///
    /// # Parameters
    ///
    /// * `ts` - The `TimeSpan64` value to format
    ///
    /// # Returns
    ///
    /// A byte slice (`&[u8]`) containing the formatted timespan string. The returned
    /// reference is valid until the next call to `render()` on the same formatter instance.
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_datetime::format::TimeSpanFormat;
    /// # use amudai_datetime::timespan::TimeSpan64;
    /// let mut formatter = TimeSpanFormat::from_specs(vec![
    ///     "ddddd", ".", "hh", ":", "mm", ":", "ss", ".", "fffffff"
    /// ]).unwrap();
    ///
    /// let ts = TimeSpan64::dhmsml(12, 10, 45, 31, 365);
    /// let formatted_bytes = formatter.render(ts);
    /// let formatted_str = std::str::from_utf8(formatted_bytes).unwrap();
    ///
    /// assert_eq!(formatted_str, "00012.10:45:31.3650000");
    ///
    /// // Negative timespan example
    /// let negative_ts = TimeSpan64::dhmsml(-12, 10, 45, 31, 365);
    /// let formatted_bytes = formatter.render(negative_ts);
    /// let formatted_str = std::str::from_utf8(formatted_bytes).unwrap();
    ///
    /// assert_eq!(formatted_str, "-00011.13:14:28.6350000");
    /// ```
    ///
    /// # Performance Notes
    ///
    /// - The internal buffer is reused across multiple calls, making this method
    ///   very efficient for repeated formatting operations
    /// - The buffer is automatically sized to handle typical timespan strings
    /// - For maximum performance, reuse the same `TimeSpanFormat` instance
    ///
    /// # Safety
    ///
    /// The returned byte slice reference becomes invalid when:
    /// - The formatter is dropped
    /// - `render()` is called again on the same formatter
    /// - The formatter is moved
    pub fn render(&mut self, ts: TimeSpan64) -> &[u8] {
        unsafe { self.buf.set_len(0) }
        if ts.ticks() < 0 {
            self.buf.write_all(b"-").expect("write to buf");
        }
        for spec in self.specs.iter() {
            spec.write(ts, &mut self.buf).expect("write to buf");
        }
        &self.buf
    }
}
