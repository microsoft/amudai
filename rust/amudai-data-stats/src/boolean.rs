//! Boolean statistics collection for Apache Arrow boolean arrays.
//!
//! This module provides functionality to collect statistics from Arrow boolean arrays,
//! including counts of true/false values, null values, and various utility methods
//! for analyzing boolean data distributions.
//!
//! # Example
//!
//! ```rust
//! use amudai_data_stats::boolean::BooleanStatsCollector;
//! use arrow_array::BooleanArray;
//!
//! let mut collector = BooleanStatsCollector::new();
//! let array = BooleanArray::from(vec![Some(true), None, Some(false), Some(true)]);
//! collector.process_array(&array).unwrap();
//! let stats = collector.finish().unwrap();
//!
//! assert_eq!(stats.count, 4);
//! assert_eq!(stats.null_count, 1);
//! assert_eq!(stats.true_count, 2);
//! assert_eq!(stats.false_count, 1);
//! ```

use amudai_common::{Result, error::Error};
use arrow_array::{Array, cast::AsArray};
use std::sync::Arc;

/// A struct to collect statistics for boolean types.
pub struct BooleanStatsCollector {
    count: usize,
    null_count: usize,
    true_count: usize,
    false_count: usize,
}

impl BooleanStatsCollector {
    /// Creates a new `BooleanStatsCollector`.
    ///
    /// # Returns
    ///
    /// A new instance of `BooleanStatsCollector`.
    pub fn new() -> BooleanStatsCollector {
        BooleanStatsCollector {
            count: 0,
            null_count: 0,
            true_count: 0,
            false_count: 0,
        }
    }
    /// Processes an array and updates the statistics.
    ///
    /// # Arguments
    ///
    /// * `array` - A reference to a boolean array to be processed.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    pub fn process_array(&mut self, array: &dyn Array) -> Result<()> {
        match array.data_type() {
            arrow_schema::DataType::Boolean => {}
            _ => {
                return Err(Error::invalid_arg(
                    "array",
                    format!("expected Boolean array, got {:?}", array.data_type()),
                ));
            }
        }

        let boolean_array = array.as_boolean();

        let total_count = boolean_array.len();
        let null_count = boolean_array.null_count();
        let true_count = boolean_array.true_count();
        let false_count = total_count - null_count - true_count;

        self.count += total_count;
        self.null_count += null_count;
        self.true_count += true_count;
        self.false_count += false_count;

        Ok(())
    }

    /// Processes a count of null values without creating an array.
    ///
    /// This is more efficient than creating a null array and calling `process_array`.
    ///
    /// # Arguments
    ///
    /// * `null_count` - The number of null values to add to the statistics
    pub fn process_nulls(&mut self, null_count: usize) {
        self.count += null_count;
        self.null_count += null_count;
        // No change to true_count or false_count since nulls don't contribute to these
    }

    /// Finalizes the statistics collection and returns the collected statistics.
    ///
    /// # Returns
    ///
    /// A `Result` containing the collected `BooleanStats` or an error.
    pub fn finish(self) -> Result<BooleanStats> {
        // Ensure that the counts are consistent
        assert_eq!(
            self.null_count + self.true_count + self.false_count,
            self.count,
            "Boolean statistics counts are inconsistent: null_count({}) + true_count({}) + false_count({}) != count({})",
            self.null_count,
            self.true_count,
            self.false_count,
            self.count
        );

        // If all values are null, no data storage is needed at all
        let raw_data_size = if self.count == self.null_count {
            0 // No data and no null bitmap needed when everything is null
        } else {
            // Calculate data size: boolean values are stored as bits, packed into bytes
            let data_size = (self.count as u64).div_ceil(8); // ceil(count / 8) bytes for boolean data

            // Calculate null bitmap overhead if there are any null values
            let null_bitmap_size = if self.null_count > 0 {
                (self.count as u64).div_ceil(8) // ceil(count / 8)
            } else {
                0
            };
            data_size + null_bitmap_size
        };

        Ok(BooleanStats {
            count: self.count,
            null_count: self.null_count,
            true_count: self.true_count,
            false_count: self.false_count,
            raw_data_size,
        })
    }
}

impl Default for BooleanStatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// A struct to hold the collected statistics for boolean types.
#[derive(Debug, Clone, PartialEq)]
pub struct BooleanStats {
    pub count: usize,
    pub null_count: usize,
    pub true_count: usize,
    pub false_count: usize,
    /// Total size of raw data in bytes (actual data + null bitmap overhead if nulls present).
    pub raw_data_size: u64,
}

impl BooleanStats {
    /// Creates an empty `BooleanStats` instance.
    ///
    /// # Returns
    ///
    /// A new `BooleanStats` with all counts set to zero.
    pub fn empty() -> Self {
        BooleanStats {
            count: 0,
            null_count: 0,
            true_count: 0,
            false_count: 0,
            raw_data_size: 0,
        }
    }

    /// Returns the count of non-null values.
    ///
    /// # Returns
    ///
    /// The number of non-null boolean values.
    pub fn non_null_count(&self) -> usize {
        self.count - self.null_count
    }

    /// Returns `true` if there are no values at all.
    ///
    /// # Returns
    ///
    /// `true` if the total count is zero, `false` otherwise.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns `true` if all values are null.
    ///
    /// # Returns
    ///
    /// `true` if all values are null, `false` otherwise.
    pub fn is_all_nulls(&self) -> bool {
        self.count > 0 && self.null_count == self.count
    }

    /// Returns `true` if all non-null values are true.
    ///
    /// # Returns
    ///
    /// `true` if all non-null values are true, `false` otherwise.
    pub fn is_all_true(&self) -> bool {
        self.non_null_count() > 0 && self.false_count == 0
    }

    /// Returns `true` if all non-null values are false.
    ///
    /// # Returns
    ///
    /// `true` if all non-null values are false, `false` otherwise.
    pub fn is_all_false(&self) -> bool {
        self.non_null_count() > 0 && self.true_count == 0
    }

    /// Returns the ratio of true values to non-null values.
    ///
    /// # Returns
    ///
    /// The ratio of true values (0.0 to 1.0), or `None` if there are no non-null values.
    pub fn true_ratio(&self) -> Option<f64> {
        let non_null = self.non_null_count();
        if non_null == 0 {
            None
        } else {
            Some(self.true_count as f64 / non_null as f64)
        }
    }

    /// Returns the ratio of false values to non-null values.
    ///
    /// # Returns
    ///
    /// The ratio of false values (0.0 to 1.0), or `None` if there are no non-null values.
    pub fn false_ratio(&self) -> Option<f64> {
        let non_null = self.non_null_count();
        if non_null == 0 {
            None
        } else {
            Some(self.false_count as f64 / non_null as f64)
        }
    }

    /// Returns the ratio of null values to total values.
    ///
    /// # Returns
    ///
    /// The ratio of null values (0.0 to 1.0), or `None` if there are no values.
    pub fn null_ratio(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.null_count as f64 / self.count as f64)
        }
    }
}

/// Processes multiple boolean arrays and collects comprehensive statistics.
///
/// This function provides a convenient way to process multiple arrays and collect
/// combined statistics from all of them.
///
/// # Arguments
/// * `arrays` - A slice of Arrow arrays to process
///
/// # Returns
/// A `Result` containing `BooleanStats` with combined statistics from all arrays
///
/// # Errors
/// Returns an error if any array is not a boolean type or if processing fails
pub fn collect_boolean_stats(arrays: &[Arc<dyn Array>]) -> Result<BooleanStats> {
    let mut collector = BooleanStatsCollector::new();

    for array in arrays {
        collector.process_array(array.as_ref())?;
    }

    collector.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use amudai_common::Result;
    use arrow_array::BooleanArray;

    #[test]
    fn test_simple_boolean_stats_collector() {
        let collector = BooleanStatsCollector::new();
        let stats = collector.finish().unwrap();
        assert_eq!(stats.count, 0);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.true_count, 0);
        assert_eq!(stats.false_count, 0);
    }

    #[test]
    fn test_boolean_stats_collector_all_true() -> Result<()> {
        let mut collector = BooleanStatsCollector::new();
        let array = BooleanArray::from(vec![true, true, true, true, true]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;

        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.true_count, 5);
        assert_eq!(stats.false_count, 0);
        assert!(stats.is_all_true());
        assert!(!stats.is_all_false());
        assert_eq!(stats.true_ratio(), Some(1.0));
        assert_eq!(stats.false_ratio(), Some(0.0));
        Ok(())
    }

    #[test]
    fn test_boolean_stats_collector_all_false() -> Result<()> {
        let mut collector = BooleanStatsCollector::new();
        let array = BooleanArray::from(vec![false, false, false]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;

        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.true_count, 0);
        assert_eq!(stats.false_count, 3);
        assert!(!stats.is_all_true());
        assert!(stats.is_all_false());
        assert_eq!(stats.true_ratio(), Some(0.0));
        assert_eq!(stats.false_ratio(), Some(1.0));
        Ok(())
    }

    #[test]
    fn test_boolean_stats_collector_mixed() -> Result<()> {
        let mut collector = BooleanStatsCollector::new();
        let array = BooleanArray::from(vec![true, false, true, false, true]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;

        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.true_count, 3);
        assert_eq!(stats.false_count, 2);
        assert!(!stats.is_all_true());
        assert!(!stats.is_all_false());
        assert_eq!(stats.true_ratio(), Some(0.6));
        assert_eq!(stats.false_ratio(), Some(0.4));
        Ok(())
    }

    #[test]
    fn test_boolean_stats_collector_with_nulls() -> Result<()> {
        let mut collector = BooleanStatsCollector::new();
        let array = BooleanArray::from(vec![Some(true), None, Some(false), Some(true), None]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;

        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 2);
        assert_eq!(stats.true_count, 2);
        assert_eq!(stats.false_count, 1);
        assert_eq!(stats.non_null_count(), 3);
        assert!(!stats.is_all_true());
        assert!(!stats.is_all_false());
        assert!(!stats.is_all_nulls());
        assert_eq!(stats.true_ratio(), Some(2.0 / 3.0));
        assert_eq!(stats.false_ratio(), Some(1.0 / 3.0));
        assert_eq!(stats.null_ratio(), Some(0.4));
        Ok(())
    }

    #[test]
    fn test_boolean_stats_collector_all_nulls() -> Result<()> {
        let mut collector = BooleanStatsCollector::new();
        let array = BooleanArray::from(vec![None::<bool>; 4]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;

        assert_eq!(stats.count, 4);
        assert_eq!(stats.null_count, 4);
        assert_eq!(stats.true_count, 0);
        assert_eq!(stats.false_count, 0);
        assert_eq!(stats.non_null_count(), 0);
        assert!(!stats.is_all_true());
        assert!(!stats.is_all_false());
        assert!(stats.is_all_nulls());
        assert_eq!(stats.true_ratio(), None);
        assert_eq!(stats.false_ratio(), None);
        assert_eq!(stats.null_ratio(), Some(1.0));
        Ok(())
    }

    #[test]
    fn test_boolean_stats_collector_empty_array() -> Result<()> {
        let mut collector = BooleanStatsCollector::new();
        let array = BooleanArray::from(Vec::<bool>::new());
        collector.process_array(&array)?;
        let stats = collector.finish()?;

        assert_eq!(stats.count, 0);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.true_count, 0);
        assert_eq!(stats.false_count, 0);
        assert!(stats.is_empty());
        assert!(!stats.is_all_nulls());
        assert_eq!(stats.true_ratio(), None);
        assert_eq!(stats.false_ratio(), None);
        assert_eq!(stats.null_ratio(), None);
        Ok(())
    }

    #[test]
    fn test_boolean_stats_collector_multiple_arrays() -> Result<()> {
        let mut collector = BooleanStatsCollector::new();

        // Process first array
        let array1 = BooleanArray::from(vec![true, false, true]);
        collector.process_array(&array1)?;

        // Process second array
        let array2 = BooleanArray::from(vec![Some(false), None, Some(true)]);
        collector.process_array(&array2)?;

        let stats = collector.finish()?;

        assert_eq!(stats.count, 6);
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.true_count, 3);
        assert_eq!(stats.false_count, 2);
        assert_eq!(stats.non_null_count(), 5);
        assert_eq!(stats.true_ratio(), Some(0.6));
        assert_eq!(stats.false_ratio(), Some(0.4));
        Ok(())
    }

    #[test]
    fn test_boolean_stats_empty() {
        let stats = BooleanStats::empty();
        assert_eq!(stats.count, 0);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.true_count, 0);
        assert_eq!(stats.false_count, 0);
        assert!(stats.is_empty());
        assert!(!stats.is_all_nulls());
    }
    #[test]
    fn test_boolean_stats_collector_wrong_type() {
        let mut collector = BooleanStatsCollector::new();
        let array = arrow_array::Int32Array::from(vec![1, 2, 3]);
        let result = collector.process_array(&array);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("expected Boolean array")
        );
    }
    #[test]
    fn test_collect_boolean_stats_multiple_arrays() -> Result<()> {
        let array1: Arc<dyn Array> = Arc::new(BooleanArray::from(vec![true, false, true]));
        let array2: Arc<dyn Array> =
            Arc::new(BooleanArray::from(vec![Some(false), None, Some(true)]));
        let arrays = vec![array1, array2];

        let stats = collect_boolean_stats(&arrays)?;

        assert_eq!(stats.count, 6);
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.true_count, 3);
        assert_eq!(stats.false_count, 2);
        assert_eq!(stats.non_null_count(), 5);
        Ok(())
    }

    #[test]
    fn test_collect_boolean_stats_empty_arrays() -> Result<()> {
        let arrays: Vec<Arc<dyn Array>> = vec![];
        let stats = collect_boolean_stats(&arrays)?;

        assert_eq!(stats.count, 0);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.true_count, 0);
        assert_eq!(stats.false_count, 0);
        assert!(stats.is_empty());
        Ok(())
    }

    #[test]
    fn test_collect_boolean_stats_with_unsupported_array() {
        use std::sync::Arc;
        let array1: Arc<dyn Array> = Arc::new(BooleanArray::from(vec![true, false]));
        let array2: Arc<dyn Array> = Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]));
        let arrays = vec![array1, array2];

        let result = collect_boolean_stats(&arrays);
        assert!(result.is_err());
    }
    #[test]
    fn test_boolean_stats_collector_process_nulls_method() -> Result<()> {
        let mut collector = BooleanStatsCollector::new();

        // Process some actual boolean data first
        let array = BooleanArray::from(vec![Some(true), Some(false), None, Some(true)]);
        collector.process_array(&array)?;

        // Now efficiently process additional nulls
        collector.process_nulls(3);

        let stats = collector.finish()?;

        // Verify the statistics
        assert_eq!(stats.count, 7); // 4 from array + 3 from process_nulls
        assert_eq!(stats.null_count, 4); // 1 from array + 3 from process_nulls
        assert_eq!(stats.true_count, 2); // Only from array (nulls don't affect true/false counts)
        assert_eq!(stats.false_count, 1); // Only from array
        assert_eq!(stats.non_null_count(), 3); // 7 total - 4 nulls

        Ok(())
    }

    #[test]
    fn test_boolean_stats_collector_process_nulls_only() {
        let mut collector = BooleanStatsCollector::new();

        // Process only nulls
        collector.process_nulls(5);

        let stats = collector.finish().unwrap();

        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 5);
        assert_eq!(stats.true_count, 0);
        assert_eq!(stats.false_count, 0);
        assert!(stats.is_all_nulls());
        assert_eq!(stats.non_null_count(), 0);
    }

    #[test]
    fn test_boolean_stats_raw_data_size_all_nulls() -> Result<()> {
        let mut collector = BooleanStatsCollector::new();
        let array = BooleanArray::from(vec![None::<bool>; 100]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;

        assert_eq!(stats.count, 100);
        assert_eq!(stats.null_count, 100);
        assert_eq!(stats.true_count, 0);
        assert_eq!(stats.false_count, 0);
        assert!(stats.is_all_nulls());
        // When all values are null, no storage is needed at all
        assert_eq!(stats.raw_data_size, 0);
        Ok(())
    }
}
