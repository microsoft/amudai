//! Decimal statistics collection for d128 decimal values.
//!
//! This module provides functionality to collect statistics from d128 decimal values
//! that have been converted from Arrow decimal array types.

use decimal::d128;

/// Statistics collector for d128 decimal values.
///
/// This collector processes d128 values and computes statistics
/// relevant for decimal data, including minimum and maximum values.
#[derive(Debug, Default)]
pub struct DecimalStatsCollector {
    min_value: Option<d128>,
    max_value: Option<d128>,
    null_count: u64,
    total_count: u64,
    zero_count: u64,
    positive_count: u64,
    negative_count: u64,
    nan_count: u64,
}

impl DecimalStatsCollector {
    /// Creates a new decimal statistics collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Processes a single d128 value and updates statistics.
    ///
    /// # Arguments
    /// * `value` - The decimal value to process
    pub fn process_value(&mut self, value: d128) {
        self.total_count += 1;
        self.update_stats(&value);
    }

    /// Processes a null value and updates statistics.
    pub fn process_null(&mut self) {
        self.total_count += 1;
        self.null_count += 1;
    }

    /// Processes an optional d128 value and updates statistics.
    ///
    /// # Arguments
    /// * `value` - The optional decimal value to process
    pub fn process_optional(&mut self, value: Option<d128>) {
        match value {
            Some(val) => self.process_value(val),
            None => self.process_null(),
        }
    }

    /// Processes a batch of d128 values and updates statistics.
    ///
    /// # Arguments
    /// * `values` - Slice of decimal values to process
    pub fn process_batch(&mut self, values: &[d128]) {
        for value in values {
            self.process_value(*value);
        }
    }

    /// Processes a batch of optional d128 values and updates statistics.
    ///
    /// # Arguments
    /// * `values` - Slice of optional decimal values to process
    pub fn process_optional_batch(&mut self, values: &[Option<d128>]) {
        for value in values {
            self.process_optional(*value);
        }
    }

    /// Processes a specified number of null values and updates the statistics.
    ///
    /// This method is more efficient than creating a null array when you only need to
    /// track null counts without processing actual values.
    ///
    /// # Arguments
    /// * `null_count` - The number of null values to add to the statistics
    pub fn process_nulls(&mut self, null_count: usize) {
        self.total_count += null_count as u64;
        self.null_count += null_count as u64;
    }
    /// Updates min and max values with a new decimal value and tracks zero/positive/negative counts.
    ///
    /// # Arguments
    /// * `value` - The decimal value
    fn update_stats(&mut self, value: &d128) {
        // Check for NaN first
        if value.is_nan() {
            self.nan_count += 1;
            return; // Don't update min/max or other counts for NaN values
        }

        // Update min/max
        if let Some(ref current_min) = self.min_value {
            if value < current_min {
                self.min_value = Some(*value);
            }
        } else {
            self.min_value = Some(*value);
        }

        if let Some(ref current_max) = self.max_value {
            if value > current_max {
                self.max_value = Some(*value);
            }
        } else {
            self.max_value = Some(*value);
        }

        // Update zero/positive/negative counts
        if value.is_zero() {
            self.zero_count += 1;
        } else if value.is_positive() {
            self.positive_count += 1;
        } else {
            self.negative_count += 1;
        }
    }

    /// Finalizes the statistics collection and returns the computed statistics.
    ///
    /// # Returns
    /// A `DecimalStats` struct containing the collected statistics
    pub fn finalize(self) -> DecimalStats {
        DecimalStats {
            min_value: self.min_value,
            max_value: self.max_value,
            null_count: self.null_count,
            total_count: self.total_count,
            zero_count: self.zero_count,
            positive_count: self.positive_count,
            negative_count: self.negative_count,
            nan_count: self.nan_count,
        }
    }
}

/// Statistics collected from decimal data.
///
/// This struct contains statistics suitable for decimal data.
#[derive(Debug, Clone, PartialEq)]
pub struct DecimalStats {
    /// Minimum decimal value in the dataset.
    pub min_value: Option<d128>,
    /// Maximum decimal value in the dataset.
    pub max_value: Option<d128>,
    /// Number of null values in the dataset.
    pub null_count: u64,
    /// Total number of values (including nulls) in the dataset.
    pub total_count: u64,
    /// Number of decimal values that are zero.
    pub zero_count: u64,
    /// Number of decimal values that are positive (greater than zero).
    pub positive_count: u64,
    /// Number of decimal values that are negative (less than zero).
    pub negative_count: u64,
    /// Number of decimal values that are NaN (Not a Number).
    pub nan_count: u64,
}

impl DecimalStats {
    /// Creates a new `DecimalStats` with default values.
    pub fn new() -> Self {
        Self {
            min_value: None,
            max_value: None,
            null_count: 0,
            total_count: 0,
            zero_count: 0,
            positive_count: 0,
            negative_count: 0,
            nan_count: 0,
        }
    }

    /// Returns the count of non-null values.
    ///
    /// # Returns
    /// The number of non-null values in the dataset
    pub fn non_null_count(&self) -> u64 {
        self.total_count - self.null_count
    }
}

impl Default for DecimalStats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_decimal_stats_collector_basic() {
        let mut collector = DecimalStatsCollector::new();

        // Process some decimal values
        collector.process_value(d128::from_str("123.45").unwrap());
        collector.process_value(d128::from_str("678.90").unwrap());
        collector.process_value(d128::from_str("111.11").unwrap());

        let stats = collector.finalize();

        assert_eq!(stats.total_count, 3);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.min_value, Some(d128::from_str("111.11").unwrap()));
        assert_eq!(stats.max_value, Some(d128::from_str("678.90").unwrap()));
    }

    #[test]
    fn test_decimal_stats_collector_with_nulls() {
        let mut collector = DecimalStatsCollector::new();

        // Process mixed values and nulls
        collector.process_optional(Some(d128::from_str("123.45").unwrap()));
        collector.process_optional(None);
        collector.process_optional(Some(d128::from_str("678.90").unwrap()));

        let stats = collector.finalize();

        assert_eq!(stats.total_count, 3);
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.non_null_count(), 2);
    }

    #[test]
    fn test_decimal_stats_collector_batch() {
        let mut collector = DecimalStatsCollector::new();

        let values = vec![
            d128::from_str("100.00").unwrap(),
            d128::from_str("200.00").unwrap(),
            d128::from_str("300.00").unwrap(),
        ];

        collector.process_batch(&values);
        let stats = collector.finalize();

        assert_eq!(stats.total_count, 3);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_decimal_stats_zero_positive_negative() {
        let mut collector = DecimalStatsCollector::new();

        collector.process_value(d128::from_str("0.00").unwrap());
        collector.process_value(d128::from_str("123.45").unwrap());
        collector.process_value(d128::from_str("-50.00").unwrap());
        collector.process_value(d128::from_str("0.00").unwrap());
        collector.process_value(d128::from_str("-25.00").unwrap());

        let stats = collector.finalize();

        assert_eq!(stats.zero_count, 2);
        assert_eq!(stats.positive_count, 1);
        assert_eq!(stats.negative_count, 2);
        assert_eq!(stats.total_count, 5);
    }

    #[test]
    fn test_decimal_comparison() {
        let a = d128::from_str("123.45").unwrap();
        let b = d128::from_str("123.46").unwrap();
        let c = d128::from_str("123.45").unwrap();

        assert!(a < b);
        assert!(a == c);
        assert!(b > a);
    }

    #[test]
    fn test_nan_counting() {
        let mut collector = DecimalStatsCollector::new();

        // Add some normal values
        collector.process_value(d128::from_str("123.45").unwrap());
        collector.process_value(d128::from_str("-50.00").unwrap());

        // Create and add NaN values
        let zero = d128::from(0);
        let nan_val = zero / zero; // Create NaN
        collector.process_value(nan_val);
        collector.process_value(nan_val); // Add another NaN

        // Add a zero value
        collector.process_value(d128::from(0));

        let stats = collector.finalize();

        assert_eq!(stats.nan_count, 2);
        assert_eq!(stats.positive_count, 1); // Only 123.45
        assert_eq!(stats.negative_count, 1); // Only -50.00
        assert_eq!(stats.zero_count, 1); // Only the explicit zero
        assert_eq!(stats.total_count, 5);

        // Verify that min/max are not affected by NaN values
        assert_eq!(stats.min_value, Some(d128::from_str("-50.00").unwrap()));
        assert_eq!(stats.max_value, Some(d128::from_str("123.45").unwrap()));
    }
}
