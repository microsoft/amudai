pub mod datetime;
pub mod datetime_parser;
pub mod format;
pub mod timespan;
pub mod timespan_parser;

#[cfg(test)]
mod tests;

pub const TICKS_PER_MICROSECOND: i64 = 10;
pub const TICKS_PER_MILLISECOND: i64 = 10000;
pub const TICKS_PER_SECOND: i64 = TICKS_PER_MILLISECOND * 1000;
pub const TICKS_PER_MINUTE: i64 = TICKS_PER_SECOND * 60;
pub const TICKS_PER_HOUR: i64 = TICKS_PER_MINUTE * 60;
pub const TICKS_PER_DAY: i64 = TICKS_PER_HOUR * 24;
pub const TICKS_TILL_UNIX_TIME: i64 = 621355968000000000;
pub const TICK_PER_DAY: i64 = 24 * 60 * 60 * TICKS_PER_SECOND;

pub const NANOSECONDS_PER_TICK: i64 = 100;

pub const DAYS_PER_YEAR: i32 = 365;
pub const DAYS_PER_4_YEARS: i32 = DAYS_PER_YEAR * 4 + 1; // 1461
pub const DAYS_PER_100_YEARS: i32 = DAYS_PER_4_YEARS * 25 - 1; // 36524
pub const DAYS_PER_400_YEARS: i32 = DAYS_PER_100_YEARS * 4 + 1; // 146097
pub const DAYS_TO_10000: i32 = DAYS_PER_400_YEARS * 25 - 366; // 3652059
pub const MAX_TICKS: i64 = DAYS_TO_10000 as i64 * TICKS_PER_DAY - 1;

#[derive(Debug)]
pub enum DateTimeError {
    InvalidArgument(String),
    DateTimeOverflow,
}

impl std::fmt::Display for DateTimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DateTimeError::InvalidArgument(desc) => write!(f, "Invalid argument: {desc}"),
            DateTimeError::DateTimeOverflow => {
                write!(
                    f,
                    "A valid datetime value must be between '0001-01-01 00:00:00' and '9999-12-31 23:59:59.9999999'"
                )
            }
        }
    }
}

#[derive(Debug)]
pub enum TimeSpanError {
    InvalidArgument(String),
    TimeSpanOverflow,
}

impl std::fmt::Display for TimeSpanError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TimeSpanError::InvalidArgument(desc) => write!(f, "Invalid argument: {desc}"),
            TimeSpanError::TimeSpanOverflow => {
                write!(
                    f,
                    "A valid timespan value must be between '00:00:00' and '10000000.23:59:59.9999999'"
                )
            }
        }
    }
}

pub type DateTimeResult = std::result::Result<datetime::DateTime64, DateTimeError>;
pub type TimeSpanResult = std::result::Result<timespan::TimeSpan64, TimeSpanError>;
