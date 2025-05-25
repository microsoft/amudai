use super::value::{IntegerValue, NumericValue};
use crate::encodings::{EncodingKind, NullMask};
use bitflags::bitflags;
use std::{collections::HashMap, hash::Hash};

bitflags! {
    /// Flags to specify which statistics to collect.
    #[derive(Clone, Copy)]
    pub struct NumericStatsCollectorFlags: u32 {
        const MIN_MAX = 1;
        const UNIQUE_VALUES = 2;
        const AVG_RUN_LENGTH = 4;
        const IS_SORTED = 8;
        const ZEROS_COUNT = 16;
        const MIN_BITS_COUNT = 32;
        const DELTAS_MIN_BITS_COUNT = 64;
    }
}

/// Statistics collected from a sequence of values.
pub struct NumericStats<T> {
    /// Size of the data in bytes.
    pub original_size: u32,

    /// Number of values in the data.
    pub values_count: u32,

    /// Number of null values in the data.
    pub nulls_count: u32,

    /// Minimal value in the data.
    pub min: Option<T>,

    /// Maximal value in the data.
    pub max: Option<T>,

    /// Unique values and their number of occurrences.
    pub unique_values: Option<HashMap<T, u32>>,

    /// Average length of repeated values sequence.
    pub avg_run_length: Option<u32>,

    /// Ratio of sorted values in the data.
    ///  0.0 - none of the value pairs appear in order.
    ///  1.0 - all the values are sorted.
    pub sorted_ratio: Option<f64>,

    /// Whether all values are sorted.
    pub is_sorted: Option<bool>,

    /// Number of zeroes in the data, as defined by `T::is_zero()`.
    pub zeros_count: Option<u32>,

    /// Minimum number of bits required to represent all the values.
    /// This statistic is only applicable for integer values.
    pub min_bits_count: Option<usize>,

    /// Minimum number of bits required to represent all the deltas between consecutive values.
    /// If delta can't be calculated because of overflow, then the value is `None`.
    /// This statistic is only applicable for integer values.
    pub deltas_min_bits_count: Option<usize>,
}

impl<T> Default for NumericStats<T> {
    fn default() -> Self {
        Self {
            original_size: 0,
            values_count: 0,
            nulls_count: 0,
            min: None,
            max: None,
            unique_values: None,
            avg_run_length: None,
            sorted_ratio: None,
            is_sorted: None,
            zeros_count: None,
            min_bits_count: None,
            deltas_min_bits_count: None,
        }
    }
}

impl<T> NumericStats<T> {
    pub fn is_single_value(&self) -> bool {
        self.unique_values
            .as_ref()
            .is_some_and(|unique_values| unique_values.len() == 1)
    }

    /// Global encodings prefilter based on the statistics.
    pub fn is_encoding_allowed(&self, encoding: EncodingKind) -> bool {
        if self.is_single_value() {
            return encoding == EncodingKind::SingleValue;
        }
        true
    }
}

pub trait StatsCollector<T>: Send + Sync {
    fn collect(
        &self,
        values: &[T],
        null_mask: &NullMask,
        flags: NumericStatsCollectorFlags,
    ) -> NumericStats<T>;
}

/// Collects statistics from sequence of numeric values, as specified
/// by `NumStatsCollectorFlags`.
pub struct NumericStatsCollector<T>(std::marker::PhantomData<T>);

impl<T> NumericStatsCollector<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> StatsCollector<T> for NumericStatsCollector<T>
where
    T: NumericValue,
{
    fn collect(
        &self,
        values: &[T],
        null_mask: &NullMask,
        flags: NumericStatsCollectorFlags,
    ) -> NumericStats<T> {
        assert!(!values.is_empty());

        let values_count = values.len();
        let mut stats = NumericStats {
            original_size: (values_count * T::SIZE) as _,
            values_count: values_count as _,
            nulls_count: null_mask.null_count() as _,
            ..Default::default()
        };

        if flags.intersects(
            NumericStatsCollectorFlags::MIN_MAX
                | NumericStatsCollectorFlags::MIN_BITS_COUNT
                | NumericStatsCollectorFlags::DELTAS_MIN_BITS_COUNT,
        ) {
            stats.min = values.iter().min().copied();
            stats.max = values.iter().max().copied();
        }

        if flags.contains(NumericStatsCollectorFlags::AVG_RUN_LENGTH) {
            let boundaries_count = values.windows(2).filter(|pair| pair[0] != pair[1]).count();
            if boundaries_count > 0 {
                stats.avg_run_length = Some(values_count as u32 / boundaries_count as u32);
            } else {
                stats.avg_run_length = Some(values_count as _);
            }
        }

        if flags.contains(NumericStatsCollectorFlags::IS_SORTED) && values_count > 1 {
            stats.sorted_ratio = Some(
                values.windows(2).filter(|pair| pair[0] <= pair[1]).count() as f64
                    / (values_count - 1) as f64,
            );
            stats.is_sorted = stats.sorted_ratio.map(|r| r == 1.0);
        }

        if flags.contains(NumericStatsCollectorFlags::UNIQUE_VALUES) {
            let mut unique_vals = HashMap::<T, u32>::new();
            let mut too_many_unique_vals = false;
            for val in values {
                if unique_vals.len() > values_count / 2 {
                    too_many_unique_vals = true;
                    break;
                }
                *unique_vals.entry(*val).or_default() += 1;
            }
            if !too_many_unique_vals {
                stats.unique_values = Some(unique_vals);
            }
        }

        if flags.contains(NumericStatsCollectorFlags::ZEROS_COUNT) {
            stats.zeros_count = Some(values.iter().filter(|&x| x.is_zero()).count() as _);
        }

        stats
    }
}

/// Collects statistics from sequence of integer values, as specified
/// by `NumStatsCollectorFlags`.
pub struct IntegerStatsCollector<T>(NumericStatsCollector<T>);

impl<T> IntegerStatsCollector<T> {
    pub fn new() -> Self {
        Self(NumericStatsCollector::new())
    }
}

impl<T> StatsCollector<T> for IntegerStatsCollector<T>
where
    T: IntegerValue,
{
    fn collect(
        &self,
        values: &[T],
        null_mask: &NullMask,
        flags: NumericStatsCollectorFlags,
    ) -> NumericStats<T> {
        let mut stats = self.0.collect(values, null_mask, flags);

        if flags.contains(NumericStatsCollectorFlags::MIN_BITS_COUNT) {
            stats.min_bits_count = stats
                .min
                .and_then(|min| stats.max.map(|max| (min, max)))
                .map(|(min, max)| {
                    T::BITS_COUNT - std::cmp::min(min.leading_zeros(), max.leading_zeros()) as usize
                });
        }

        if flags.contains(NumericStatsCollectorFlags::DELTAS_MIN_BITS_COUNT) {
            stats.deltas_min_bits_count = stats
                .min
                .and_then(|min| stats.max.map(|max| (min, max)))
                .map(|(min, max)| max.wrapping_sub(&min))
                .map(|delta| T::BITS_COUNT - delta.leading_zeros() as usize);
        }

        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encodings::numeric::value::FloatValue;

    #[test]
    pub fn test_numeric_stats_collector() {
        let collector = NumericStatsCollector::new();
        let flags = NumericStatsCollectorFlags::all();

        let sample = vec![0, 1, 2, 3, 4, 5, 7, 6, 8, 9];
        let stats = collector.collect(&sample, &NullMask::None, flags);
        assert_eq!(Some(0), stats.min);
        assert_eq!(Some(9), stats.max);
        assert_eq!(Some(1), stats.avg_run_length);
        assert!(!stats.is_sorted.unwrap());
        assert_eq!(0.8888888888888888, stats.sorted_ratio.unwrap());
        assert_eq!(None, stats.unique_values);
        assert_eq!(Some(1), stats.zeros_count);

        let sample = vec![1, 1, 1, 1, 2, 2, 2, 3, 3, 4];
        let stats = collector.collect(&sample, &NullMask::None, flags);
        assert_eq!(Some(1), stats.min);
        assert_eq!(Some(4), stats.max);
        assert_eq!(Some(3), stats.avg_run_length);
        assert!(stats.is_sorted.unwrap());
        assert_eq!(Some(4), stats.unique_values.map(|v| v.len() as _));
        assert_eq!(Some(0), stats.zeros_count);

        let sample: Vec<FloatValue<f64>> = vec![
            1.0.into(),
            1.01.into(),
            std::f64::consts::PI.into(),
            0.0.into(),
            1.0.into(),
            2.0001.into(),
            (-1.567).into(),
            5.32.into(),
            5.32.into(),
            5.32.into(),
            5.32.into(),
            5.32.into(),
            5.32.into(),
            5.32.into(),
            5.32.into(),
        ];
        let fp_collector = NumericStatsCollector::new();
        let stats = fp_collector.collect(&sample, &NullMask::None, flags);
        assert_eq!(Some((-1.567).into()), stats.min);
        assert_eq!(Some(5.32.into()), stats.max);
        assert_eq!(Some(2), stats.avg_run_length);
        assert!(!stats.is_sorted.unwrap());
        assert_eq!(Some(7), stats.unique_values.map(|v| v.len() as _));
        assert_eq!(Some(1), stats.zeros_count);
    }
}
