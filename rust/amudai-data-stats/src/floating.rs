//! Floating-point statistics collection for f32 and f64 values.
//!
//! This module provides functionality to collect statistics from floating-point values
//! that include special values like NaN, positive/negative infinity, and finite values.

/// Statistics collector for floating-point values.
///
/// This collector processes f32/f64 values and computes statistics
/// relevant for floating-point data, including counts of special values.
#[derive(Debug, Default)]
pub struct FloatingStatsCollector {
    min_value: Option<f64>,
    max_value: Option<f64>,
    null_count: u64,
    total_count: u64,
    zero_count: u64,
    positive_count: u64,
    negative_count: u64,
    nan_count: u64,
    positive_infinity_count: u64,
    negative_infinity_count: u64,
}

impl FloatingStatsCollector {
    /// Creates a new floating-point statistics collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Processes a single f32 value and updates statistics.
    ///
    /// # Arguments
    /// * `value` - The f32 value to process
    pub fn process_f32_value(&mut self, value: f32) {
        self.total_count += 1;
        self.update_stats(value as f64);
    }

    /// Processes a single f64 value and updates statistics.
    ///
    /// # Arguments
    /// * `value` - The f64 value to process
    pub fn process_f64_value(&mut self, value: f64) {
        self.total_count += 1;
        self.update_stats(value);
    }

    /// Processes a null value and updates statistics.
    pub fn process_null(&mut self) {
        self.total_count += 1;
        self.null_count += 1;
    }

    /// Processes an optional f32 value and updates statistics.
    ///
    /// # Arguments
    /// * `value` - The optional f32 value to process
    pub fn process_optional_f32(&mut self, value: Option<f32>) {
        match value {
            Some(val) => self.process_f32_value(val),
            None => self.process_null(),
        }
    }

    /// Processes an optional f64 value and updates statistics.
    ///
    /// # Arguments
    /// * `value` - The optional f64 value to process
    pub fn process_optional_f64(&mut self, value: Option<f64>) {
        match value {
            Some(val) => self.process_f64_value(val),
            None => self.process_null(),
        }
    }

    /// Processes a batch of f32 values and updates statistics.
    ///
    /// # Arguments
    /// * `values` - Slice of f32 values to process
    pub fn process_f32_batch(&mut self, values: &[f32]) {
        for value in values {
            self.process_f32_value(*value);
        }
    }

    /// Processes a batch of f64 values and updates statistics.
    ///
    /// # Arguments
    /// * `values` - Slice of f64 values to process
    pub fn process_f64_batch(&mut self, values: &[f64]) {
        for value in values {
            self.process_f64_value(*value);
        }
    }

    /// Processes a batch of optional f32 values and updates statistics.
    ///
    /// # Arguments
    /// * `values` - Slice of optional f32 values to process
    pub fn process_optional_f32_batch(&mut self, values: &[Option<f32>]) {
        for value in values {
            self.process_optional_f32(*value);
        }
    }

    /// Processes a batch of optional f64 values and updates statistics.
    ///
    /// # Arguments
    /// * `values` - Slice of optional f64 values to process
    pub fn process_optional_f64_batch(&mut self, values: &[Option<f64>]) {
        for value in values {
            self.process_optional_f64(*value);
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

    /// Updates statistics with a floating-point value.
    ///
    /// # Arguments
    /// * `value` - The floating-point value
    fn update_stats(&mut self, value: f64) {
        // Check for NaN first
        if value.is_nan() {
            self.nan_count += 1;
            return; // Don't update min/max or other counts for NaN values
        }

        // Check for infinities
        if value.is_infinite() {
            if value.is_sign_positive() {
                self.positive_infinity_count += 1;
            } else {
                self.negative_infinity_count += 1;
            }
            // Infinities are included in min/max calculations but not in finite counts
        }

        // Update min/max (includes infinities but excludes NaN)
        if let Some(current_min) = self.min_value {
            if value < current_min {
                self.min_value = Some(value);
            }
        } else {
            self.min_value = Some(value);
        }

        if let Some(current_max) = self.max_value {
            if value > current_max {
                self.max_value = Some(value);
            }
        } else {
            self.max_value = Some(value);
        }

        // Update zero/positive/negative counts (only for finite values)
        if value.is_finite() {
            if value == 0.0 {
                self.zero_count += 1;
            } else if value > 0.0 {
                self.positive_count += 1;
            } else {
                self.negative_count += 1;
            }
        }
    }

    /// Finalizes the statistics collection and returns the computed statistics.
    ///
    /// # Returns
    /// A `FloatingStats` struct containing the collected statistics
    pub fn finalize(self) -> FloatingStats {
        FloatingStats {
            min_value: self.min_value,
            max_value: self.max_value,
            null_count: self.null_count,
            total_count: self.total_count,
            zero_count: self.zero_count,
            positive_count: self.positive_count,
            negative_count: self.negative_count,
            nan_count: self.nan_count,
            positive_infinity_count: self.positive_infinity_count,
            negative_infinity_count: self.negative_infinity_count,
        }
    }
}

/// Statistics collected from floating-point data.
///
/// This struct contains statistics suitable for floating-point data including
/// special values like NaN and infinities.
#[derive(Debug, Clone, PartialEq)]
pub struct FloatingStats {
    /// Minimum floating-point value in the dataset (excludes NaN).
    pub min_value: Option<f64>,
    /// Maximum floating-point value in the dataset (excludes NaN).
    pub max_value: Option<f64>,
    /// Number of null values in the dataset.
    pub null_count: u64,
    /// Total number of values (including nulls) in the dataset.
    pub total_count: u64,
    /// Number of floating-point values that are zero.
    pub zero_count: u64,
    /// Number of floating-point values that are positive (greater than zero).
    pub positive_count: u64,
    /// Number of floating-point values that are negative (less than zero).
    pub negative_count: u64,
    /// Number of floating-point values that are NaN (Not a Number).
    pub nan_count: u64,
    /// Number of floating-point values that are positive infinity.
    pub positive_infinity_count: u64,
    /// Number of floating-point values that are negative infinity.
    pub negative_infinity_count: u64,
}

impl FloatingStats {
    /// Creates a new `FloatingStats` with default values.
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
            positive_infinity_count: 0,
            negative_infinity_count: 0,
        }
    }

    /// Returns the count of non-null values.
    ///
    /// # Returns
    /// The number of non-null values in the dataset
    pub fn non_null_count(&self) -> u64 {
        self.total_count - self.null_count
    }

    /// Returns the count of finite values (not NaN or infinity).
    ///
    /// # Returns
    /// The number of finite values in the dataset
    pub fn finite_count(&self) -> u64 {
        self.zero_count + self.positive_count + self.negative_count
    }

    /// Returns the count of infinite values (positive and negative infinity).
    ///
    /// # Returns
    /// The number of infinite values in the dataset
    pub fn infinity_count(&self) -> u64 {
        self.positive_infinity_count + self.negative_infinity_count
    }
}

impl Default for FloatingStats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_floating_stats_collector_f32() {
        let mut collector = FloatingStatsCollector::new();

        // Test finite values
        collector.process_f32_value(1.5);
        collector.process_f32_value(-2.5);
        collector.process_f32_value(0.0);

        // Test special values
        collector.process_f32_value(f32::NAN);
        collector.process_f32_value(f32::INFINITY);
        collector.process_f32_value(f32::NEG_INFINITY);

        // Test null
        collector.process_null();

        let stats = collector.finalize();

        assert_eq!(stats.total_count, 7);
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.zero_count, 1);
        assert_eq!(stats.positive_count, 1);
        assert_eq!(stats.negative_count, 1);
        assert_eq!(stats.nan_count, 1);
        assert_eq!(stats.positive_infinity_count, 1);
        assert_eq!(stats.negative_infinity_count, 1);
        assert_eq!(stats.min_value, Some(f64::NEG_INFINITY));
        assert_eq!(stats.max_value, Some(f64::INFINITY));
    }

    #[test]
    fn test_floating_stats_collector_f64() {
        let mut collector = FloatingStatsCollector::new();

        // Test finite values
        collector.process_f64_value(1.5);
        collector.process_f64_value(-2.5);
        collector.process_f64_value(0.0);

        // Test special values
        collector.process_f64_value(f64::NAN);
        collector.process_f64_value(f64::INFINITY);
        collector.process_f64_value(f64::NEG_INFINITY);

        // Test null
        collector.process_null();

        let stats = collector.finalize();

        assert_eq!(stats.total_count, 7);
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.zero_count, 1);
        assert_eq!(stats.positive_count, 1);
        assert_eq!(stats.negative_count, 1);
        assert_eq!(stats.nan_count, 1);
        assert_eq!(stats.positive_infinity_count, 1);
        assert_eq!(stats.negative_infinity_count, 1);
    }

    #[test]
    fn test_floating_stats_finite_values_only() {
        let mut collector = FloatingStatsCollector::new();

        collector.process_f64_value(1.0);
        collector.process_f64_value(2.0);
        collector.process_f64_value(3.0);

        let stats = collector.finalize();

        assert_eq!(stats.finite_count(), 3);
        assert_eq!(stats.infinity_count(), 0);
        assert_eq!(stats.nan_count, 0);
        assert_eq!(stats.min_value, Some(1.0));
        assert_eq!(stats.max_value, Some(3.0));
    }

    #[test]
    fn test_floating_stats_batch_processing() {
        let mut collector = FloatingStatsCollector::new();

        let f32_values = vec![1.0f32, 2.0f32, 3.0f32];
        let f64_values = vec![4.0f64, 5.0f64, 6.0f64];

        collector.process_f32_batch(&f32_values);
        collector.process_f64_batch(&f64_values);

        let stats = collector.finalize();

        assert_eq!(stats.total_count, 6);
        assert_eq!(stats.positive_count, 6);
        assert_eq!(stats.min_value, Some(1.0));
        assert_eq!(stats.max_value, Some(6.0));
    }

    #[test]
    fn test_floating_stats_optional_processing() {
        let mut collector = FloatingStatsCollector::new();

        let optional_f32_values = vec![Some(1.0f32), None, Some(3.0f32)];
        let optional_f64_values = vec![Some(4.0f64), None, Some(6.0f64)];

        collector.process_optional_f32_batch(&optional_f32_values);
        collector.process_optional_f64_batch(&optional_f64_values);

        let stats = collector.finalize();

        assert_eq!(stats.total_count, 6);
        assert_eq!(stats.null_count, 2);
        assert_eq!(stats.positive_count, 4);
        assert_eq!(stats.min_value, Some(1.0));
        assert_eq!(stats.max_value, Some(6.0));
    }
}
