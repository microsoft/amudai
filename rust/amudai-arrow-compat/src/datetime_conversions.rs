use std::{sync::Arc, time::SystemTime};

use amudai_common::{Result, error::Error};
use amudai_format::schema::BasicType;
use amudai_sequence::sequence::ValueSequence;
use arrow_array::{
    Array, ArrowPrimitiveType, PrimitiveArray, UInt64Array,
    cast::AsArray,
    types::{
        Date32Type, Date64Type, TimestampMicrosecondType, TimestampMillisecondType,
        TimestampNanosecondType, TimestampSecondType,
    },
};
use arrow_schema::TimeUnit;

/// Count of 100-ns ticks elapsed since 0001-01-01T00:00:00Z to 1970-01-01​T00:00:00Z
/// (start of the UNIX epoch).
pub const UNIX_EPOCH_TICKS: i64 = 621_355_968_000_000_000;

pub fn array_to_datetime_ticks(array: Arc<dyn Array>) -> Arc<dyn Array> {
    match array.data_type() {
        arrow_schema::DataType::Timestamp(TimeUnit::Second, _) => {
            array_to_array_i64::<TimestampSecondType>(&array, unix_seconds_to_ticks)
        }
        arrow_schema::DataType::Timestamp(TimeUnit::Millisecond, _) => {
            array_to_array_i64::<TimestampMillisecondType>(&array, unix_milliseconds_to_ticks)
        }
        arrow_schema::DataType::Timestamp(TimeUnit::Microsecond, _) => {
            array_to_array_i64::<TimestampMicrosecondType>(&array, unix_microseconds_to_ticks)
        }
        arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            array_to_array_i64::<TimestampNanosecondType>(&array, unix_nanoseconds_to_ticks)
        }
        arrow_schema::DataType::Date32 => {
            array_to_array_i32::<Date32Type>(&array, unix_days_to_ticks)
        }
        arrow_schema::DataType::Date64 => {
            array_to_array_i64::<Date64Type>(&array, unix_milliseconds_to_ticks)
        }
        _ => array,
    }
}

pub fn datetime_sequence_to_array(
    seq: ValueSequence,
    arrow_type: &arrow_schema::DataType,
) -> Result<Arc<dyn Array>> {
    assert_eq!(seq.type_desc.basic_type, BasicType::DateTime);
    match arrow_type {
        arrow_schema::DataType::Timestamp(TimeUnit::Second, _) => Ok(sequence_to_array::<
            TimestampSecondType,
        >(
            seq, ticks_to_unix_seconds
        )),
        arrow_schema::DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Ok(sequence_to_array::<TimestampMillisecondType>(
                seq,
                ticks_to_unix_milliseconds,
            ))
        }
        arrow_schema::DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Ok(sequence_to_array::<TimestampMicrosecondType>(
                seq,
                ticks_to_unix_microseconds,
            ))
        }
        arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Ok(sequence_to_array::<TimestampNanosecondType>(
                seq,
                ticks_to_unix_nanoseconds,
            ))
        }
        arrow_schema::DataType::Date32 => {
            Ok(sequence_to_array::<Date32Type>(seq, ticks_to_unix_days))
        }
        arrow_schema::DataType::Date64 => Ok(sequence_to_array::<Date64Type>(
            seq,
            ticks_to_unix_milliseconds_days,
        )),
        _ => Err(Error::invalid_arg(
            "arrow_type",
            format!("Cannot convert DateTime to Arrow {arrow_type}"),
        )),
    }
}

fn array_to_array_i64<T: ArrowPrimitiveType<Native = i64>>(
    array: &dyn Array,
    f: fn(i64) -> Option<u64>,
) -> Arc<dyn Array> {
    let array = array.as_primitive::<T>();
    Arc::new(UInt64Array::from_iter(array.iter().map(|v| v.and_then(f))))
}

fn array_to_array_i32<T: ArrowPrimitiveType<Native = i32>>(
    array: &dyn Array,
    f: fn(i32) -> Option<u64>,
) -> Arc<dyn Array> {
    let array = array.as_primitive::<T>();
    Arc::new(UInt64Array::from_iter(array.iter().map(|v| v.and_then(f))))
}

fn sequence_to_array<T: ArrowPrimitiveType>(
    seq: ValueSequence,
    f: fn(u64) -> Option<T::Native>,
) -> Arc<dyn Array> {
    let len = seq.len();
    if seq.presence.is_trivial_all_null() {
        return arrow_array::new_null_array(&T::DATA_TYPE, len);
    }

    let values = seq.values.as_slice::<u64>();
    let arr = PrimitiveArray::<T>::from_iter(
        (0..len).map(|i| seq.presence.is_valid(i).then(|| f(values[i])).flatten()),
    );
    Arc::new(arr)
}

#[inline]
pub fn unix_days_to_ticks(days: i32) -> Option<u64> {
    let days = days as i64;
    u64::try_from(
        days.checked_mul(10_000_000 * 3600 * 24)?
            .checked_add(UNIX_EPOCH_TICKS)?,
    )
    .ok()
}

#[inline]
pub fn ticks_to_unix_days(ticks: u64) -> Option<i32> {
    i32::try_from(
        i64::try_from(ticks)
            .ok()?
            .checked_sub(UNIX_EPOCH_TICKS)?
            .checked_div(10_000_000 * 3600 * 24)?,
    )
    .ok()
}

#[inline]
pub fn unix_seconds_to_ticks(seconds: i64) -> Option<u64> {
    u64::try_from(
        seconds
            .checked_mul(10_000_000)?
            .checked_add(UNIX_EPOCH_TICKS)?,
    )
    .ok()
}

#[inline]
pub fn ticks_to_unix_seconds(ticks: u64) -> Option<i64> {
    i64::try_from(ticks)
        .ok()?
        .checked_sub(UNIX_EPOCH_TICKS)?
        .checked_div(10_000_000)
}

#[inline]
pub fn unix_milliseconds_to_ticks(msec: i64) -> Option<u64> {
    u64::try_from(msec.checked_mul(10_000)?.checked_add(UNIX_EPOCH_TICKS)?).ok()
}

#[inline]
pub fn ticks_to_unix_milliseconds(ticks: u64) -> Option<i64> {
    i64::try_from(ticks)
        .ok()?
        .checked_sub(UNIX_EPOCH_TICKS)?
        .checked_div(10_000)
}

#[inline]
pub fn ticks_to_unix_milliseconds_days(ticks: u64) -> Option<i64> {
    i64::try_from(ticks)
        .ok()?
        .checked_sub(UNIX_EPOCH_TICKS)?
        .checked_div(10_000_000 * 3600 * 24)?
        .checked_mul(1000 * 3600 * 24)
}

#[inline]
pub fn unix_microseconds_to_ticks(usec: i64) -> Option<u64> {
    u64::try_from(usec.checked_mul(10)?.checked_add(UNIX_EPOCH_TICKS)?).ok()
}

#[inline]
pub fn ticks_to_unix_microseconds(ticks: u64) -> Option<i64> {
    i64::try_from(ticks)
        .ok()?
        .checked_sub(UNIX_EPOCH_TICKS)?
        .checked_div(10)
}

#[inline]
pub fn unix_nanoseconds_to_ticks(nsec: i64) -> Option<u64> {
    u64::try_from(nsec.checked_div(100)?.checked_add(UNIX_EPOCH_TICKS)?).ok()
}

#[inline]
pub fn ticks_to_unix_nanoseconds(ticks: u64) -> Option<i64> {
    i64::try_from(ticks)
        .ok()?
        .checked_sub(UNIX_EPOCH_TICKS)?
        .checked_mul(100)
}

pub fn now_unix_milliseconds() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

pub fn now_unix_microseconds() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_micros()
        .try_into()
        .unwrap()
}

pub fn now_unix_nanoseconds() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .try_into()
        .unwrap()
}

pub fn now_ticks() -> u64 {
    unix_nanoseconds_to_ticks(now_unix_nanoseconds()).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Date32Array, Date64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };

    mod conversion_tests {
        use super::*;

        #[test]
        fn test_unix_seconds_to_ticks_basic() {
            assert_eq!(unix_seconds_to_ticks(0), Some(UNIX_EPOCH_TICKS as u64));

            assert_eq!(
                unix_seconds_to_ticks(1),
                Some((UNIX_EPOCH_TICKS + 10_000_000) as u64)
            );

            assert_eq!(
                unix_seconds_to_ticks(-1),
                Some((UNIX_EPOCH_TICKS - 10_000_000) as u64)
            );
        }

        #[test]
        fn test_unix_seconds_to_ticks_edge_cases() {
            let max_timestamp = i64::MAX / 10_000_000 - UNIX_EPOCH_TICKS / 10_000_000;
            assert!(unix_seconds_to_ticks(max_timestamp).is_some());
            assert_eq!(unix_seconds_to_ticks(i64::MAX), None);
        }

        #[test]
        fn test_unix_milliseconds_to_ticks_basic() {
            assert_eq!(unix_milliseconds_to_ticks(0), Some(UNIX_EPOCH_TICKS as u64));

            assert_eq!(
                unix_milliseconds_to_ticks(1),
                Some((UNIX_EPOCH_TICKS + 10_000) as u64)
            );

            assert_eq!(
                unix_milliseconds_to_ticks(1000),
                Some((UNIX_EPOCH_TICKS + 10_000_000) as u64)
            );

            assert_eq!(
                unix_milliseconds_to_ticks(-1000),
                Some((UNIX_EPOCH_TICKS - 10_000_000) as u64)
            );
        }

        #[test]
        fn test_unix_microseconds_to_ticks_basic() {
            // Test conversion at Unix epoch
            assert_eq!(unix_microseconds_to_ticks(0), Some(UNIX_EPOCH_TICKS as u64));

            assert_eq!(
                unix_microseconds_to_ticks(1),
                Some((UNIX_EPOCH_TICKS + 10) as u64)
            );

            assert_eq!(
                unix_microseconds_to_ticks(1_000_000),
                Some((UNIX_EPOCH_TICKS + 10_000_000) as u64)
            );
        }

        #[test]
        fn test_unix_nanoseconds_to_ticks_basic() {
            assert_eq!(unix_nanoseconds_to_ticks(0), Some(UNIX_EPOCH_TICKS as u64));

            assert_eq!(
                unix_nanoseconds_to_ticks(100),
                Some((UNIX_EPOCH_TICKS + 1) as u64)
            );

            assert_eq!(
                unix_nanoseconds_to_ticks(1_000_000_000),
                Some((UNIX_EPOCH_TICKS + 10_000_000) as u64)
            );

            assert_eq!(unix_nanoseconds_to_ticks(99), Some(UNIX_EPOCH_TICKS as u64));
        }

        #[test]
        fn test_unix_days_to_ticks_basic() {
            assert_eq!(unix_days_to_ticks(0), Some(UNIX_EPOCH_TICKS as u64));

            let expected_day_ticks = 86400i64 * 10_000_000;
            assert_eq!(
                unix_days_to_ticks(1),
                Some((UNIX_EPOCH_TICKS + expected_day_ticks) as u64)
            );

            assert_eq!(
                unix_days_to_ticks(-1),
                Some((UNIX_EPOCH_TICKS - expected_day_ticks) as u64)
            );
        }
    }

    mod reverse_conversion_tests {
        use super::*;

        #[test]
        fn test_ticks_to_unix_seconds_basic() {
            assert_eq!(ticks_to_unix_seconds(UNIX_EPOCH_TICKS as u64), Some(0));

            assert_eq!(
                ticks_to_unix_seconds((UNIX_EPOCH_TICKS + 10_000_000) as u64),
                Some(1)
            );

            assert_eq!(
                ticks_to_unix_seconds((UNIX_EPOCH_TICKS - 10_000_000) as u64),
                Some(-1)
            );
        }

        #[test]
        fn test_ticks_to_unix_milliseconds_basic() {
            assert_eq!(ticks_to_unix_milliseconds(UNIX_EPOCH_TICKS as u64), Some(0));

            assert_eq!(
                ticks_to_unix_milliseconds((UNIX_EPOCH_TICKS + 10_000) as u64),
                Some(1)
            );

            assert_eq!(
                ticks_to_unix_milliseconds((UNIX_EPOCH_TICKS + 10_000_000) as u64),
                Some(1000)
            );
        }

        #[test]
        fn test_ticks_to_unix_microseconds_basic() {
            assert_eq!(ticks_to_unix_microseconds(UNIX_EPOCH_TICKS as u64), Some(0));

            assert_eq!(
                ticks_to_unix_microseconds((UNIX_EPOCH_TICKS + 10) as u64),
                Some(1)
            );

            assert_eq!(
                ticks_to_unix_microseconds((UNIX_EPOCH_TICKS + 10_000_000) as u64),
                Some(1_000_000)
            );
        }

        #[test]
        fn test_ticks_to_unix_nanoseconds_basic() {
            assert_eq!(ticks_to_unix_nanoseconds(UNIX_EPOCH_TICKS as u64), Some(0));

            assert_eq!(
                ticks_to_unix_nanoseconds((UNIX_EPOCH_TICKS + 1) as u64),
                Some(100)
            );

            assert_eq!(
                ticks_to_unix_nanoseconds((UNIX_EPOCH_TICKS + 10_000_000) as u64),
                Some(1_000_000_000)
            );
        }
    }

    mod roundtrip_tests {
        use super::*;

        #[test]
        fn test_seconds_roundtrip() {
            let test_values = vec![0i64, 1, -1, 3600, -3600, 86400, -86400];

            for seconds in test_values {
                if let Some(ticks) = unix_seconds_to_ticks(seconds) {
                    let back_to_seconds = ticks_to_unix_seconds(ticks);
                    assert_eq!(
                        back_to_seconds,
                        Some(seconds),
                        "Round-trip failed for {seconds} seconds"
                    );
                }
            }
        }

        #[test]
        fn test_milliseconds_roundtrip() {
            let test_values = vec![0i64, 1, -1, 1000, -1000, 3600000, -3600000];

            for milliseconds in test_values {
                if let Some(ticks) = unix_milliseconds_to_ticks(milliseconds) {
                    let back_to_milliseconds = ticks_to_unix_milliseconds(ticks);
                    assert_eq!(
                        back_to_milliseconds,
                        Some(milliseconds),
                        "Round-trip failed for {milliseconds} milliseconds"
                    );
                }
            }
        }

        #[test]
        fn test_microseconds_roundtrip() {
            let test_values = vec![0i64, 1, -1, 1000000, -1000000, 3600000000, -3600000000];

            for microseconds in test_values {
                if let Some(ticks) = unix_microseconds_to_ticks(microseconds) {
                    let back_to_microseconds = ticks_to_unix_microseconds(ticks);
                    assert_eq!(
                        back_to_microseconds,
                        Some(microseconds),
                        "Round-trip failed for {microseconds} microseconds"
                    );
                }
            }
        }

        #[test]
        fn test_nanoseconds_roundtrip() {
            let test_values = vec![0i64, 100, -100, 1000000000, -1000000000];

            for nanoseconds in test_values {
                if let Some(ticks) = unix_nanoseconds_to_ticks(nanoseconds) {
                    let back_to_nanoseconds = ticks_to_unix_nanoseconds(ticks);
                    assert_eq!(
                        back_to_nanoseconds,
                        Some(nanoseconds),
                        "Round-trip failed for {nanoseconds} nanoseconds"
                    );
                }
            }
        }

        #[test]
        fn test_nanoseconds_precision_loss() {
            assert_eq!(
                unix_nanoseconds_to_ticks(150),
                unix_nanoseconds_to_ticks(100)
            );
            assert_eq!(
                unix_nanoseconds_to_ticks(199),
                unix_nanoseconds_to_ticks(100)
            );
        }
    }

    mod edge_case_tests {
        use super::*;

        #[test]
        fn test_overflow_cases() {
            assert_eq!(unix_seconds_to_ticks(i64::MAX), None);
            assert_eq!(unix_milliseconds_to_ticks(i64::MAX), None);
            assert_eq!(unix_microseconds_to_ticks(i64::MAX), None);

            let large_seconds = i64::MAX / 10_000_000;
            assert!(unix_seconds_to_ticks(large_seconds).is_none());
        }

        #[test]
        fn test_boundary_values() {
            let near_max_seconds = (u64::MAX as i64 - UNIX_EPOCH_TICKS) / 10_000_000;
            if near_max_seconds > 0 {
                assert!(unix_seconds_to_ticks(near_max_seconds - 1).is_some());
            }

            assert!(unix_seconds_to_ticks(0).is_some());
            assert!(unix_milliseconds_to_ticks(0).is_some());
            assert!(unix_microseconds_to_ticks(0).is_some());
            assert!(unix_nanoseconds_to_ticks(0).is_some());
            assert!(unix_days_to_ticks(0).is_some());
        }

        #[test]
        fn test_reverse_conversion_edge_cases() {
            assert_eq!(ticks_to_unix_seconds(u64::MAX), None);
            assert_eq!(ticks_to_unix_milliseconds(u64::MAX), None);
            assert_eq!(ticks_to_unix_microseconds(u64::MAX), None);
            assert_eq!(ticks_to_unix_nanoseconds(u64::MAX), None);

            assert_eq!(ticks_to_unix_seconds(0), Some(-62135596800));
            assert_eq!(ticks_to_unix_milliseconds(0), Some(-62135596800000));
            assert_eq!(ticks_to_unix_microseconds(0), Some(-62135596800000000));
            assert_eq!(ticks_to_unix_nanoseconds(0), None);

            assert!(ticks_to_unix_seconds(UNIX_EPOCH_TICKS as u64).is_some());
            assert!(ticks_to_unix_milliseconds(UNIX_EPOCH_TICKS as u64).is_some());
            assert!(ticks_to_unix_microseconds(UNIX_EPOCH_TICKS as u64).is_some());
            assert!(ticks_to_unix_nanoseconds(UNIX_EPOCH_TICKS as u64).is_some());
        }
    }

    mod reference_date_tests {
        use super::*;

        #[test]
        fn test_known_dates() {
            assert_eq!(unix_seconds_to_ticks(0), Some(UNIX_EPOCH_TICKS as u64));

            let year_2000_seconds = 946684800i64;
            let year_2000_ticks = unix_seconds_to_ticks(year_2000_seconds);
            assert!(year_2000_ticks.is_some());

            if let Some(ticks) = year_2000_ticks {
                assert_eq!(ticks_to_unix_seconds(ticks), Some(year_2000_seconds));
            }
        }

        #[test]
        fn test_leap_year_handling() {
            let leap_day = unix_days_to_ticks(11016);
            assert!(leap_day.is_some());

            let leap_day_seconds = 11016i64 * 86400;
            let leap_day_from_seconds = unix_seconds_to_ticks(leap_day_seconds);
            assert_eq!(leap_day, leap_day_from_seconds);
        }
    }

    mod time_functions_tests {
        use super::*;

        #[test]
        fn test_now_functions_return_reasonable_values() {
            let now_micro = now_unix_microseconds();
            let now_nano = now_unix_nanoseconds();
            let now_ticks_val = now_ticks();

            assert!(now_micro > 0);
            assert!(now_nano > 0);
            assert!(now_ticks_val > UNIX_EPOCH_TICKS as u64);
        }

        #[test]
        fn test_now_functions_monotonicity() {
            let first_micro = now_unix_microseconds();
            let second_micro = now_unix_microseconds();

            assert!(
                second_micro >= first_micro - 1000,
                "Time went backwards: {first_micro} -> {second_micro}"
            );

            let first_nano = now_unix_nanoseconds();
            let second_nano = now_unix_nanoseconds();
            assert!(
                second_nano >= first_nano - 100000,
                "Nanosecond time went backwards: {first_nano} -> {second_nano}"
            );
        }
    }

    mod array_conversion_tests {
        use super::*;

        #[test]
        fn test_timestamp_second_array_conversion() {
            let values = vec![Some(0i64), Some(1), Some(-1), None, Some(3600)];
            let array = TimestampSecondArray::from(values.clone());
            let array_arc = Arc::new(array) as Arc<dyn Array>;

            let result = array_to_datetime_ticks(array_arc);
            let result_u64 = result.as_any().downcast_ref::<UInt64Array>().unwrap();

            assert_eq!(result_u64.len(), 5);
            assert_eq!(result_u64.value(0), UNIX_EPOCH_TICKS as u64);
            assert_eq!(result_u64.value(1), (UNIX_EPOCH_TICKS + 10_000_000) as u64);
            assert_eq!(result_u64.value(2), (UNIX_EPOCH_TICKS - 10_000_000) as u64);
            assert!(result_u64.is_null(3));
            assert_eq!(
                result_u64.value(4),
                (UNIX_EPOCH_TICKS + 36_000_000_000) as u64
            );
        }

        #[test]
        fn test_timestamp_millisecond_array_conversion() {
            let values = vec![Some(0i64), Some(1000), Some(-1000), None];
            let array = TimestampMillisecondArray::from(values);
            let array_arc = Arc::new(array) as Arc<dyn Array>;

            let result = array_to_datetime_ticks(array_arc);
            let result_u64 = result.as_any().downcast_ref::<UInt64Array>().unwrap();

            assert_eq!(result_u64.len(), 4);
            assert_eq!(result_u64.value(0), UNIX_EPOCH_TICKS as u64);
            assert_eq!(result_u64.value(1), (UNIX_EPOCH_TICKS + 10_000_000) as u64);
            assert_eq!(result_u64.value(2), (UNIX_EPOCH_TICKS - 10_000_000) as u64);
            assert!(result_u64.is_null(3));
        }

        #[test]
        fn test_timestamp_microsecond_array_conversion() {
            let values = vec![Some(0i64), Some(1_000_000), Some(-1_000_000), None];
            let array = TimestampMicrosecondArray::from(values);
            let array_arc = Arc::new(array) as Arc<dyn Array>;

            let result = array_to_datetime_ticks(array_arc);
            let result_u64 = result.as_any().downcast_ref::<UInt64Array>().unwrap();

            assert_eq!(result_u64.len(), 4);
            assert_eq!(result_u64.value(0), UNIX_EPOCH_TICKS as u64);
            assert_eq!(result_u64.value(1), (UNIX_EPOCH_TICKS + 10_000_000) as u64);
            assert_eq!(result_u64.value(2), (UNIX_EPOCH_TICKS - 10_000_000) as u64);
            assert!(result_u64.is_null(3));
        }

        #[test]
        fn test_timestamp_nanosecond_array_conversion() {
            let values = vec![Some(0i64), Some(1_000_000_000), Some(-1_000_000_000), None];
            let array = TimestampNanosecondArray::from(values);
            let array_arc = Arc::new(array) as Arc<dyn Array>;

            let result = array_to_datetime_ticks(array_arc);
            let result_u64 = result.as_any().downcast_ref::<UInt64Array>().unwrap();

            assert_eq!(result_u64.len(), 4);
            assert_eq!(result_u64.value(0), UNIX_EPOCH_TICKS as u64);
            assert_eq!(result_u64.value(1), (UNIX_EPOCH_TICKS + 10_000_000) as u64);
            assert_eq!(result_u64.value(2), (UNIX_EPOCH_TICKS - 10_000_000) as u64);
            assert!(result_u64.is_null(3));
        }

        #[test]
        fn test_date32_array_conversion() {
            let values = vec![Some(0i32), Some(1), Some(-1), None];
            let array = Date32Array::from(values);
            let array_arc = Arc::new(array) as Arc<dyn Array>;

            let result = array_to_datetime_ticks(array_arc);
            let result_u64 = result.as_any().downcast_ref::<UInt64Array>().unwrap();

            assert_eq!(result_u64.len(), 4);
            assert_eq!(result_u64.value(0), UNIX_EPOCH_TICKS as u64);
            assert_eq!(
                result_u64.value(1),
                (UNIX_EPOCH_TICKS + 864_000_000_000) as u64
            );
            assert_eq!(
                result_u64.value(2),
                (UNIX_EPOCH_TICKS - 864_000_000_000) as u64
            );
            assert!(result_u64.is_null(3));
        }

        #[test]
        fn test_date64_array_conversion() {
            let values = vec![Some(0i64), Some(86400000), Some(-86400000), None];
            let array = Date64Array::from(values);
            let array_arc = Arc::new(array) as Arc<dyn Array>;

            let result = array_to_datetime_ticks(array_arc);
            let result_u64 = result.as_any().downcast_ref::<UInt64Array>().unwrap();

            assert_eq!(result_u64.len(), 4);
            assert_eq!(result_u64.value(0), UNIX_EPOCH_TICKS as u64);
            assert_eq!(
                result_u64.value(1),
                (UNIX_EPOCH_TICKS + 864_000_000_000) as u64
            );
            assert_eq!(
                result_u64.value(2),
                (UNIX_EPOCH_TICKS - 864_000_000_000) as u64
            );
            assert!(result_u64.is_null(3));
        }

        #[test]
        fn test_empty_array_conversion() {
            let array = TimestampSecondArray::from(Vec::<Option<i64>>::new());
            let array_arc = Arc::new(array) as Arc<dyn Array>;

            let result = array_to_datetime_ticks(array_arc);
            let result_u64 = result.as_any().downcast_ref::<UInt64Array>().unwrap();

            assert_eq!(result_u64.len(), 0);
        }

        #[test]
        fn test_all_null_array_conversion() {
            let values = vec![None, None, None];
            let array = TimestampSecondArray::from(values);
            let array_arc = Arc::new(array) as Arc<dyn Array>;

            let result = array_to_datetime_ticks(array_arc);
            let result_u64 = result.as_any().downcast_ref::<UInt64Array>().unwrap();

            assert_eq!(result_u64.len(), 3);
            assert!(result_u64.is_null(0));
            assert!(result_u64.is_null(1));
            assert!(result_u64.is_null(2));
        }
    }

    mod array_overflow_tests {
        use super::*;

        #[test]
        fn test_array_with_overflow_values() {
            let values = vec![Some(0i64), Some(i64::MAX), Some(-1)];
            let array = TimestampSecondArray::from(values);
            let array_arc = Arc::new(array) as Arc<dyn Array>;

            let result = array_to_datetime_ticks(array_arc);
            let result_u64 = result.as_any().downcast_ref::<UInt64Array>().unwrap();

            assert_eq!(result_u64.len(), 3);
            assert_eq!(result_u64.value(0), UNIX_EPOCH_TICKS as u64);
            assert!(result_u64.is_null(1)); // Should be null due to overflow
            assert_eq!(result_u64.value(2), (UNIX_EPOCH_TICKS - 10_000_000) as u64);
        }
    }
}
