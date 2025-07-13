//! Binary statistics collection for Apache Arrow binary arrays.
//!
//! This module provides functionality to collect statistics from Arrow binary array types
//! including `BinaryArray`, `LargeBinaryArray`, and `FixedSizeBinaryArray`.

use amudai_bloom_filters::{BloomFilterCollector, BloomFilterConfig};
use amudai_common::{Result, error::Error};
use amudai_encodings::block_encoder::BlockEncodingProfile;
use amudai_format::defs::schema::BasicType;
use arrow_array::{Array, BinaryArray, FixedSizeBinaryArray, LargeBinaryArray};
use arrow_schema::DataType;

/// Statistics collector for binary data in Arrow arrays.
///
/// This collector processes various Arrow binary array types and computes statistics
/// relevant for binary data, including minimum and maximum lengths of binary values.
/// Bloom filters are optionally created based on the encoding profile configuration
/// and explicit bloom filter collection flag.
pub struct BinaryStatsCollector {
    min_length: Option<u64>,
    max_length: Option<u64>,
    min_non_empty_length: Option<u64>,
    null_count: u64,
    total_count: u64,
    // Bloom filter support
    bloom_filter_collector: Option<BloomFilterCollector>,
    // Flag to control bloom filter collection
    collect_bloom_filters: bool,
}

impl BinaryStatsCollector {
    /// Creates a new binary statistics collector.
    ///
    /// Bloom filters are automatically enabled for GUID types due to their
    /// high value for lookups and filtering operations. For other types,
    /// bloom filters are enabled based on the encoding profile.
    ///
    /// # Arguments
    ///
    /// * `basic_type` - The basic type of the data being collected
    /// * `encoding_profile` - The encoding profile to determine bloom filter settings
    ///
    /// # Returns
    ///
    /// A new instance of `BinaryStatsCollector` with appropriate bloom filter settings.
    pub fn new(basic_type: BasicType, encoding_profile: BlockEncodingProfile) -> Self {
        // Enable bloom filters for GUIDs regardless of encoding profile,
        // or for other types when not using Plain encoding
        let collect_bloom_filters = basic_type == BasicType::Guid
            || !matches!(encoding_profile, BlockEncodingProfile::Plain);

        let bloom_filter_collector = if collect_bloom_filters {
            Some(BloomFilterCollector::new(BloomFilterConfig::default()))
        } else {
            None
        };

        Self {
            min_length: None,
            max_length: None,
            min_non_empty_length: None,
            null_count: 0,
            total_count: 0,
            bloom_filter_collector,
            collect_bloom_filters,
        }
    }

    /// Processes a `BinaryArray` and collects statistics.
    ///
    /// # Arguments
    /// * `array` - The binary array to process
    ///
    /// # Returns
    /// A `Result` indicating success or failure
    pub fn process_binary_array(&mut self, array: &BinaryArray) -> Result<()> {
        self.total_count += array.len() as u64;

        for i in 0..array.len() {
            if array.is_null(i) {
                self.null_count += 1;
            } else {
                let value = array.value(i);
                let length = value.len() as u64;
                self.update_lengths(length)?;

                // Update bloom filter if enabled
                if let Some(ref mut collector) = self.bloom_filter_collector {
                    if self.collect_bloom_filters {
                        collector.process_value(value);
                    }
                }
            }
        }

        Ok(())
    }

    /// Processes a `LargeBinaryArray` and collects statistics.
    ///
    /// # Arguments
    /// * `array` - The large binary array to process
    ///
    /// # Returns
    /// A `Result` indicating success or failure
    pub fn process_large_binary_array(&mut self, array: &LargeBinaryArray) -> Result<()> {
        self.total_count += array.len() as u64;

        for i in 0..array.len() {
            if array.is_null(i) {
                self.null_count += 1;
            } else {
                let value = array.value(i);
                let length = value.len() as u64;
                self.update_lengths(length)?;

                // Update bloom filter if enabled
                if let Some(ref mut collector) = self.bloom_filter_collector {
                    if self.collect_bloom_filters {
                        collector.process_value(value);
                    }
                }
            }
        }

        Ok(())
    }

    /// Processes a `FixedSizeBinaryArray` and collects statistics.
    ///
    /// # Arguments
    /// * `array` - The fixed-size binary array to process
    ///
    /// # Returns
    /// A `Result` indicating success or failure
    pub fn process_fixed_size_binary_array(&mut self, array: &FixedSizeBinaryArray) -> Result<()> {
        self.total_count += array.len() as u64;
        let fixed_length = array.value_length() as u64;

        for i in 0..array.len() {
            if array.is_null(i) {
                self.null_count += 1;
            } else {
                let value = array.value(i);
                self.update_lengths(fixed_length)?;

                // Update bloom filter if enabled
                if let Some(ref mut collector) = self.bloom_filter_collector {
                    if self.collect_bloom_filters {
                        collector.process_value(value);
                    }
                }
            }
        }

        Ok(())
    }

    /// Processes any Arrow array that contains binary data.
    ///
    /// This method automatically detects the array type and delegates to the appropriate
    /// processing method.
    ///
    /// # Arguments
    /// * `array` - The Arrow array to process
    ///
    /// # Returns
    /// A `Result` indicating success or failure, or an error if the array type is unsupported
    pub fn process_array(&mut self, array: &dyn Array) -> Result<()> {
        match array.data_type() {
            DataType::Binary => {
                let binary_array =
                    array
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or_else(|| {
                            Error::invalid_arg("array", "Failed to downcast to BinaryArray")
                        })?;
                self.process_binary_array(binary_array)
            }
            DataType::LargeBinary => {
                let large_binary_array = array
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| {
                        Error::invalid_arg("array", "Failed to downcast to LargeBinaryArray")
                    })?;
                self.process_large_binary_array(large_binary_array)
            }
            DataType::FixedSizeBinary(_) => {
                let fixed_size_binary_array = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| {
                        Error::invalid_arg("array", "Failed to downcast to FixedSizeBinaryArray")
                    })?;
                self.process_fixed_size_binary_array(fixed_size_binary_array)
            }
            _ => Err(Error::invalid_arg(
                "array",
                format!(
                    "Unsupported array type for binary statistics: {:?}",
                    array.data_type()
                ),
            )),
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
        // No changes to length-related statistics since nulls don't contribute to min/max lengths
    }

    /// Updates the length statistics with a new binary value length.
    ///
    /// # Arguments
    /// * `length` - The length of the binary value
    ///
    /// # Returns
    /// A `Result` indicating success or failure
    fn update_lengths(&mut self, length: u64) -> Result<()> {
        // Update min_length
        self.min_length = Some(match self.min_length {
            Some(current_min) => current_min.min(length),
            None => length,
        });

        // Update max_length
        self.max_length = Some(match self.max_length {
            Some(current_max) => current_max.max(length),
            None => length,
        });

        // Update min_non_empty_length (only for non-empty values)
        if length > 0 {
            self.min_non_empty_length = Some(match self.min_non_empty_length {
                Some(current_min) => current_min.min(length),
                None => length,
            });
        }

        Ok(())
    }

    /// Finalizes the statistics collection and returns the computed statistics.
    ///
    /// # Returns
    /// A `Result` containing the collected `BinaryStats` or an error.
    pub fn finalize(self) -> Result<BinaryStats> {
        // Handle the case where no binary data was processed
        let (min_length, max_length) = if self.total_count == self.null_count {
            (0, 0)
        } else {
            (self.min_length.unwrap_or(0), self.max_length.unwrap_or(0))
        };

        // Build bloom filter if available
        let bloom_filter = self
            .bloom_filter_collector
            .and_then(|mut collector| collector.finish().then_some(collector))
            .and_then(|collector| {
                let filter_data = collector.filter_data_as_bytes();
                (!filter_data.is_empty()).then(|| {
                    amudai_format::defs::shard::SplitBlockBloomFilter {
                        num_blocks: collector.num_blocks(),
                        target_fpp: collector.target_fpp(),
                        num_values: collector.num_values(),
                        hash_algorithm: collector.hash_algorithm().to_string(),
                        data: filter_data.to_vec(),
                    }
                })
            });

        Ok(BinaryStats {
            min_length,
            max_length,
            min_non_empty_length: self.min_non_empty_length,
            null_count: self.null_count,
            total_count: self.total_count,
            bloom_filter,
        })
    }
}

/// Statistics collected from binary data arrays.
///
/// This struct contains length-based statistics suitable for binary data.
#[derive(Debug, Clone, PartialEq)]
pub struct BinaryStats {
    /// Minimum length of any binary value in the dataset.
    pub min_length: u64,
    /// Maximum length of any binary value in the dataset.
    pub max_length: u64,
    /// Minimum length of non-empty binary values (excludes zero-length values).
    pub min_non_empty_length: Option<u64>,
    /// Number of null values in the dataset.
    pub null_count: u64,
    /// Total number of values (including nulls) in the dataset.
    pub total_count: u64,
    /// Optional bloom filter protobuf if one was successfully constructed during statistics collection.
    pub bloom_filter: Option<amudai_format::defs::shard::SplitBlockBloomFilter>,
}

impl BinaryStats {
    /// Creates empty binary statistics (no data processed).
    ///
    /// # Returns
    /// A `BinaryStats` instance with default values
    pub fn empty() -> Self {
        Self {
            min_length: 0,
            max_length: 0,
            min_non_empty_length: None,
            null_count: 0,
            total_count: 0,
            bloom_filter: None,
        }
    }

    /// Returns whether the dataset contains only null values.
    ///
    /// # Returns
    /// `true` if all values are null, `false` otherwise
    pub fn is_all_nulls(&self) -> bool {
        self.total_count > 0 && self.null_count == self.total_count
    }

    /// Returns whether the dataset is empty (no values processed).
    ///
    /// # Returns
    /// `true` if no values were processed, `false` otherwise
    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    /// Returns the count of non-null values.
    ///
    /// # Returns
    /// The number of non-null values in the dataset
    pub fn non_null_count(&self) -> u64 {
        self.total_count - self.null_count
    }
}

impl Default for BinaryStatsCollector {
    fn default() -> Self {
        Self::new(BasicType::Binary, BlockEncodingProfile::Balanced)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, builder::FixedSizeBinaryBuilder};
    use std::sync::Arc;

    /// Processes multiple binary arrays and collects comprehensive statistics.
    /// This helper function is only used in tests.
    fn collect_binary_stats(arrays: &[Arc<dyn Array>]) -> Result<BinaryStats> {
        let mut collector =
            BinaryStatsCollector::new(BasicType::Binary, BlockEncodingProfile::Balanced);

        for array in arrays {
            collector.process_array(array.as_ref())?;
        }

        collector.finalize()
    }

    #[test]
    fn test_empty_binary_array() {
        let array = BinaryArray::from_vec(Vec::<&[u8]>::new());
        let mut collector = BinaryStatsCollector::default();
        collector.process_binary_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 0);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.min_length, 0);
        assert_eq!(stats.max_length, 0);
        assert_eq!(stats.min_non_empty_length, None);
        assert!(stats.is_empty());
    }

    #[test]
    fn test_binary_array_with_nulls() {
        let data: Vec<Option<&[u8]>> = vec![Some(b"hello"), None, Some(b"world"), None, Some(b"")];
        let array = BinaryArray::from_opt_vec(data);
        let mut collector = BinaryStatsCollector::default();
        collector.process_binary_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 5);
        assert_eq!(stats.null_count, 2);
        assert_eq!(stats.non_null_count(), 3);
        assert_eq!(stats.min_length, 0); // Empty string
        assert_eq!(stats.max_length, 5); // "hello" and "world"
        assert_eq!(stats.min_non_empty_length, Some(5)); // Excludes empty string
        assert!(!stats.is_empty());
        assert!(!stats.is_all_nulls());
    }

    #[test]
    fn test_binary_array_various_lengths() {
        let data: Vec<&[u8]> = vec![
            b"a",        // 1 byte
            b"ab",       // 2 bytes
            b"abc",      // 3 bytes
            b"abcdefgh", // 8 bytes
        ];
        let array = BinaryArray::from_vec(data);
        let mut collector = BinaryStatsCollector::default();
        collector.process_binary_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 4);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.min_length, 1);
        assert_eq!(stats.max_length, 8);
        assert_eq!(stats.min_non_empty_length, Some(1));
    }

    #[test]
    fn test_large_binary_array() {
        let data: Vec<Option<&[u8]>> = vec![
            Some(b"small"),
            Some(b"medium length"),
            None,
            Some(b"very long binary data with more content"),
        ];
        let array = LargeBinaryArray::from_opt_vec(data);
        let mut collector = BinaryStatsCollector::default();
        collector.process_large_binary_array(&array).unwrap();
        let stats = collector.finalize().unwrap();
        assert_eq!(stats.total_count, 4);
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.min_length, 5); // "small"
        assert_eq!(stats.max_length, 39); // "very long binary data with more content"
        assert_eq!(stats.min_non_empty_length, Some(5));
    }

    #[test]
    fn test_fixed_size_binary_array() {
        let mut builder = FixedSizeBinaryBuilder::with_capacity(4, 5);
        builder.append_value(b"12345").unwrap();
        builder.append_null();
        builder.append_value(b"abcde").unwrap();
        builder.append_value(b"fghij").unwrap();
        let array = builder.finish();

        let mut collector = BinaryStatsCollector::default();
        collector.process_fixed_size_binary_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 4);
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.min_length, 5); // Fixed size
        assert_eq!(stats.max_length, 5); // Fixed size
        assert_eq!(stats.min_non_empty_length, Some(5));
    }

    #[test]
    fn test_process_array_binary() {
        let data: Vec<&[u8]> = vec![b"test1", b"test22", b"test333"];
        let array = BinaryArray::from_vec(data);
        let mut collector = BinaryStatsCollector::default();
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 3);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.min_length, 5);
        assert_eq!(stats.max_length, 7);
    }

    #[test]
    fn test_process_array_large_binary() {
        let data: Vec<&[u8]> = vec![b"a", b"bb", b"ccc"];
        let array = LargeBinaryArray::from_vec(data);
        let mut collector = BinaryStatsCollector::default();
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 3);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.min_length, 1);
        assert_eq!(stats.max_length, 3);
    }

    #[test]
    fn test_process_array_fixed_size_binary() {
        let array = FixedSizeBinaryArray::from(vec![b"abcd", b"efgh", b"ijkl"]);
        let mut collector = BinaryStatsCollector::default();
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 3);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.min_length, 4);
        assert_eq!(stats.max_length, 4);
    }

    #[test]
    fn test_unsupported_array_type() {
        let array = arrow_array::Int32Array::from(vec![1, 2, 3]);
        let mut collector = BinaryStatsCollector::default();
        let result = collector.process_array(&array);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unsupported array type")
        );
    }

    #[test]
    fn test_all_nulls() {
        let data: Vec<Option<&[u8]>> = vec![None, None, None];
        let array = BinaryArray::from_opt_vec(data);
        let mut collector = BinaryStatsCollector::default();
        collector.process_binary_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 3);
        assert_eq!(stats.null_count, 3);
        assert_eq!(stats.min_length, 0);
        assert_eq!(stats.max_length, 0);
        assert_eq!(stats.min_non_empty_length, None);
        assert!(stats.is_all_nulls());
        assert!(!stats.is_empty());
    }

    #[test]
    fn test_empty_strings_only() {
        let data: Vec<&[u8]> = vec![b"", b"", b""];
        let array = BinaryArray::from_vec(data);
        let mut collector = BinaryStatsCollector::default();
        collector.process_binary_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 3);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.min_length, 0);
        assert_eq!(stats.max_length, 0);
        assert_eq!(stats.min_non_empty_length, None); // No non-empty values
    }

    #[test]
    fn test_mixed_empty_and_non_empty() {
        let data: Vec<&[u8]> = vec![b"", b"hello", b"", b"world", b""];
        let array = BinaryArray::from_vec(data);
        let mut collector = BinaryStatsCollector::default();
        collector.process_binary_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 5);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.min_length, 0);
        assert_eq!(stats.max_length, 5);
        assert_eq!(stats.min_non_empty_length, Some(5)); // "hello" and "world"
    }

    #[test]
    fn test_binary_with_special_bytes() {
        let data: Vec<&[u8]> = vec![
            &[0x00, 0x01, 0x02],       // 3 bytes with null byte
            &[0xFF, 0xFE, 0xFD, 0xFC], // 4 bytes
            &[0x80],                   // 1 byte
            &[],                       // Empty
        ];
        let array = BinaryArray::from_vec(data);
        let mut collector = BinaryStatsCollector::default();
        collector.process_binary_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 4);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.min_length, 0);
        assert_eq!(stats.max_length, 4);
        assert_eq!(stats.min_non_empty_length, Some(1));
    }
    #[test]
    fn test_empty_stats() {
        let stats = BinaryStats::empty();
        assert_eq!(stats.min_length, 0);
        assert_eq!(stats.max_length, 0);
        assert_eq!(stats.min_non_empty_length, None);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.total_count, 0);
        assert!(stats.is_empty());
        assert!(!stats.is_all_nulls());
    }

    #[test]
    fn test_collect_binary_stats_multiple_arrays() {
        let array1 = Arc::new(BinaryArray::from_vec(vec![b"hello", b"world"])) as Arc<dyn Array>;
        let array2 = Arc::new(LargeBinaryArray::from_opt_vec(vec![
            Some(b"foo"),
            None,
            Some(b"bar"),
        ])) as Arc<dyn Array>;
        let array3 = Arc::new(FixedSizeBinaryArray::from(vec![b"test", b"data"])) as Arc<dyn Array>;

        let arrays = vec![array1, array2, array3];
        let stats = collect_binary_stats(&arrays).unwrap();

        assert_eq!(stats.total_count, 7); // 2 + 3 + 2
        assert_eq!(stats.null_count, 1); // Only from array2
        assert_eq!(stats.min_length, 3); // "foo", "bar"
        assert_eq!(stats.max_length, 5); // "hello", "world"
    }

    #[test]
    fn test_collect_binary_stats_empty_arrays() {
        let arrays: Vec<Arc<dyn Array>> = vec![];
        let stats = collect_binary_stats(&arrays).unwrap();

        assert!(stats.is_empty());
        assert_eq!(stats.total_count, 0);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_collect_binary_stats_with_unsupported_array() {
        let array1 = Arc::new(BinaryArray::from_vec(vec![b"hello"])) as Arc<dyn Array>;
        let array2 = Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;

        let arrays = vec![array1, array2];
        let result = collect_binary_stats(&arrays);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unsupported array type")
        );
    }

    #[test]
    fn test_large_binary_data() {
        // Create a large binary value (1MB)
        let large_data = vec![b'A'; 1_000_000];
        let data: Vec<&[u8]> = vec![b"small", large_data.as_slice(), b"medium"];
        let array = BinaryArray::from_vec(data);
        let mut collector = BinaryStatsCollector::default();
        collector.process_binary_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 3);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.min_length, 5); // "small"
        assert_eq!(stats.max_length, 1_000_000); // Large data
        assert_eq!(stats.min_non_empty_length, Some(5));
    }

    #[test]
    fn test_utf8_encoded_binary() {
        // Test with UTF-8 encoded text as binary data
        let data: Vec<&[u8]> = vec![
            "Hello, 世界!".as_bytes(), // Multi-byte UTF-8
            "🚀🦀".as_bytes(),         // Emoji
            "العربية".as_bytes(),      // Arabic
            "русский".as_bytes(),      // Cyrillic
        ];
        let array = BinaryArray::from_vec(data);
        let mut collector = BinaryStatsCollector::default();
        collector.process_binary_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 4);
        assert_eq!(stats.null_count, 0);
        // UTF-8 strings have varying byte lengths        assert!(stats.min_length > 0);
        assert!(stats.max_length >= stats.min_length);
        assert!(stats.min_non_empty_length.is_some());
    }

    #[test]
    fn test_binary_stats_collector_process_nulls_method() {
        let mut collector = BinaryStatsCollector::default();
        let data: Vec<Option<&[u8]>> = vec![Some(b"hello"), None, Some(b"world")];
        let array = BinaryArray::from_opt_vec(data);
        collector.process_binary_array(&array).unwrap();

        // Add additional nulls using the dedicated method
        collector.process_nulls(3);
        let stats = collector.finalize().unwrap();
        assert_eq!(stats.total_count, 6); // 3 from array + 3 from process_nulls
        assert_eq!(stats.null_count, 4); // 1 from array + 3 from process_nulls
        assert_eq!(stats.min_length, 5); // Both "hello" and "world" are 5 bytes
        assert_eq!(stats.max_length, 5);
    }

    #[test]
    fn test_binary_stats_collector_process_nulls_only() {
        let mut collector = BinaryStatsCollector::default();

        // Only process nulls without any array data
        collector.process_nulls(5);
        let stats = collector.finalize().unwrap();
        assert_eq!(stats.total_count, 5);
        assert_eq!(stats.null_count, 5);
        assert_eq!(stats.min_length, 0); // No non-null values processed
        assert_eq!(stats.max_length, 0);
        assert_eq!(stats.min_non_empty_length, None);
    }

    #[test]
    fn test_bloom_filter_creation_with_balanced_profile() {
        let mut collector =
            BinaryStatsCollector::new(BasicType::Binary, BlockEncodingProfile::Balanced);
        let data: Vec<&[u8]> = vec![
            b"value1", b"value2", b"value3", b"value4", b"value5", b"value6", b"value7", b"value8",
            b"value9", b"value10",
        ];
        let array = BinaryArray::from_vec(data);
        collector.process_binary_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 10);
        assert_eq!(stats.null_count, 0);
        // With balanced profile, bloom filter should be present
        assert!(stats.bloom_filter.is_some());
        let bloom_filter = stats.bloom_filter.unwrap();
        assert_eq!(bloom_filter.num_values, 10);
        assert!(!bloom_filter.data.is_empty());
    }

    #[test]
    fn test_bloom_filter_disabled_with_plain_profile() {
        let mut collector =
            BinaryStatsCollector::new(BasicType::Binary, BlockEncodingProfile::Plain);
        let data: Vec<&[u8]> = vec![b"value1", b"value2", b"value3", b"value4", b"value5"];
        let array = BinaryArray::from_vec(data);
        collector.process_binary_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 5);
        assert_eq!(stats.null_count, 0);
        // With plain profile, bloom filter should be disabled
        assert!(stats.bloom_filter.is_none());
    }

    #[test]
    fn test_bloom_filter_with_different_encoding_profiles() {
        // Test high compression profile enables bloom filters
        let mut collector =
            BinaryStatsCollector::new(BasicType::Binary, BlockEncodingProfile::HighCompression);
        let data: Vec<&[u8]> = vec![b"test1", b"test2", b"test3"];
        let array = BinaryArray::from_vec(data);
        collector.process_binary_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert!(stats.bloom_filter.is_some());
        let bloom_filter = stats.bloom_filter.unwrap();
        assert_eq!(bloom_filter.num_values, 3);
    }

    #[test]
    fn test_binary_stats_collector_with_bloom_filter_flag() {
        // Test GUID type (should enable bloom filters even with Plain profile)
        let mut collector = BinaryStatsCollector::new(BasicType::Guid, BlockEncodingProfile::Plain);
        let data: Vec<&[u8]> = vec![b"test1", b"test2", b"test3"];
        let array = BinaryArray::from_vec(data);
        collector.process_binary_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 3);
        assert!(stats.bloom_filter.is_some());

        // Test Binary type with Plain profile (should disable bloom filters)
        let mut collector =
            BinaryStatsCollector::new(BasicType::Binary, BlockEncodingProfile::Plain);
        let data: Vec<&[u8]> = vec![b"test1", b"test2", b"test3"];
        let array = BinaryArray::from_vec(data);
        collector.process_binary_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 3);
        assert!(stats.bloom_filter.is_none());
    }

    #[test]
    fn test_binary_stats_collector_for_guid_type() {
        // GUID type should enable bloom filters even with Plain encoding profile
        let mut collector = BinaryStatsCollector::new(BasicType::Guid, BlockEncodingProfile::Plain);

        // Create GUID-like data (16 bytes each)
        let guid1 = [0u8; 16];
        let guid2 = [1u8; 16];
        let guid3 = [2u8; 16];
        let data: Vec<&[u8]> = vec![&guid1, &guid2, &guid3];
        let array = BinaryArray::from_vec(data);

        collector.process_binary_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 3);
        assert_eq!(stats.min_length, 16);
        assert_eq!(stats.max_length, 16);
        // Bloom filter should be enabled for GUIDs even with Plain profile
        assert!(stats.bloom_filter.is_some());
        let bloom_filter = stats.bloom_filter.unwrap();
        assert_eq!(bloom_filter.num_values, 3);
    }

    #[test]
    fn test_binary_stats_collector_for_non_guid_type_with_plain_profile() {
        // Non-GUID type with Plain encoding should not enable bloom filters
        let mut collector =
            BinaryStatsCollector::new(BasicType::Binary, BlockEncodingProfile::Plain);
        let data: Vec<&[u8]> = vec![b"test1", b"test2", b"test3"];
        let array = BinaryArray::from_vec(data);
        collector.process_binary_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 3);
        // Bloom filter should be disabled for non-GUID types with Plain profile
        assert!(stats.bloom_filter.is_none());
    }

    #[test]
    fn test_binary_stats_collector_for_non_guid_type_with_balanced_profile() {
        // Non-GUID type with Balanced encoding should enable bloom filters
        let mut collector =
            BinaryStatsCollector::new(BasicType::Binary, BlockEncodingProfile::Balanced);
        let data: Vec<&[u8]> = vec![b"test1", b"test2", b"test3"];
        let array = BinaryArray::from_vec(data);
        collector.process_binary_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.total_count, 3);
        // Bloom filter should be enabled for non-Plain profiles
        assert!(stats.bloom_filter.is_some());
    }
}
