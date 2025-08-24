use crate::sketch_ext::SketchBuilderExt;
use amudai_bloom_filters::{BloomFilterCollector, BloomFilterConfig};
use amudai_common::Result;
use amudai_encodings::block_encoder::BlockEncodingProfile;
use amudai_format::defs::shard::StringStats as ProtoStringStats;
use amudai_hll::HllSketch;
use amudai_unicode::ascii_check::IsAsciiFast;
use arrow_array::{Array, LargeStringArray, StringArray, StringViewArray, cast::AsArray};
use std::cmp;

/// Maximum size (in bytes) of a string value to track for constant detection.
/// If all string values are the same and not larger than this limit, the value will be stored
/// for potential constant field optimization.
const MAX_CONSTANT_STRING_SIZE: u64 = 128;

/// A struct to collect statistics for string types.
pub struct StringStatsCollector {
    min_size: u64,
    min_non_empty_size: Option<u64>,
    max_size: u64,
    ascii_count: u64,
    count: usize,
    null_count: usize,
    raw_data_size: u64,
    // Bloom filter support
    bloom_filter_collector: Option<BloomFilterCollector>,
    // Min/max value tracking for constant detection
    min_value: Option<Vec<u8>>,
    max_value: Option<Vec<u8>>,
    value_tracking_enabled: bool,
    // HyperLogLog for cardinality estimation
    sketch_builder: HllSketch,
}

impl StringStatsCollector {
    /// Creates a new `StringStatsCollector` with encoding profile-based bloom filter configuration.
    ///
    /// The decision to enable bloom filters is made internally based on the encoding profile:
    /// - Plain encoding: Bloom filters disabled (prioritizes speed)
    /// - All other profiles: Bloom filters enabled with default configuration
    ///
    /// # Arguments
    ///
    /// * `encoding_profile` - The block encoding profile that determines bloom filter settings
    ///
    /// # Returns
    ///
    /// A new instance of `StringStatsCollector` with profile-appropriate bloom filter settings.
    pub fn new(encoding_profile: BlockEncodingProfile) -> Self {
        let bloom_filter_collector = match encoding_profile {
            BlockEncodingProfile::Plain => {
                // Plain encoding prioritizes speed - skip bloom filters
                None
            }
            _ => {
                // All other profiles benefit from bloom filters with default 1% FPP
                Some(BloomFilterCollector::new(BloomFilterConfig::default()))
            }
        };

        Self {
            min_size: u64::MAX,
            min_non_empty_size: None,
            max_size: 0,
            ascii_count: 0,
            count: 0,
            null_count: 0,
            raw_data_size: 0,
            bloom_filter_collector,
            min_value: None,
            max_value: None,
            value_tracking_enabled: true,
            sketch_builder: HllSketch::new_default(),
        }
    }

    /// Processes an array and updates the statistics.
    ///
    /// # Arguments
    ///
    /// * `array` - A reference to an array of string values to be processed.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    pub fn process_array(&mut self, array: &dyn Array) -> Result<()> {
        match array.data_type() {
            arrow_schema::DataType::Utf8 => {
                let string_array = array.as_string::<i32>();
                self.process_string_array(string_array);
            }
            arrow_schema::DataType::LargeUtf8 => {
                let string_array = array.as_string::<i64>();
                self.process_large_string_array(string_array);
            }
            arrow_schema::DataType::Utf8View => {
                let string_view_array = array.as_string_view();
                self.process_string_view_array(string_view_array);
            }
            _ => {
                return Err(amudai_common::error::Error::invalid_arg(
                    "array_type".to_string(),
                    format!(
                        "Unsupported data type for string statistics: {:?}",
                        array.data_type()
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Processes a specified number of null values and updates the statistics.
    ///
    /// This method is more efficient than creating a null array when you only need to
    /// track null counts without processing actual values.
    ///
    /// # Arguments
    ///
    /// * `null_count` - The number of null values to add to the statistics
    pub fn process_nulls(&mut self, null_count: usize) {
        self.count += null_count;
        self.null_count += null_count;
        // No changes to size-related statistics since nulls don't contribute to min/max sizes
    }

    /// Processes a single non-null string value repeated `count` times.
    ///
    /// This method is primarily used for cases where you want to process individual string values
    /// without going through an entire array.
    ///
    /// # Arguments
    ///
    /// * `value` - A reference to the string value to be processed
    /// * `count` - The number of times this value should be counted in the statistics
    pub fn process_value(&mut self, value: &str, count: usize) -> Result<()> {
        self.count += count;
        self.update_stats_for_string(value, count as u64);
        Ok(())
    }

    /// Finalizes the statistics collection and returns the collected statistics.
    ///
    /// # Returns
    ///
    /// A `Result` containing the collected `StringStats` or an error.
    pub fn finalize(self) -> Result<StringStats> {
        // Handle the case where no strings were processed
        let (min_size, max_size) = if self.count == self.null_count {
            // All values are null
            (0, 0)
        } else {
            (
                if self.min_size == u64::MAX {
                    0
                } else {
                    self.min_size
                },
                self.max_size,
            )
        };

        // Calculate raw_data_size according to proto specification:
        // - If no nulls: just the sum of string lengths
        // - If some nulls: sum of string lengths + N bits for null bitmap (where N = total_count)
        // - If all nulls: 0
        let raw_data_size = if self.count == 0 {
            // No data processed
            0
        } else if self.null_count == self.count {
            // All values are null
            0
        } else if self.null_count == 0 {
            // No null values - just the actual data
            self.raw_data_size
        } else {
            // Some null values - add null bitmap overhead (N bits = N/8 bytes, rounded up)
            let null_bitmap_bytes = (self.count as u64).div_ceil(8); // Round up to nearest byte
            self.raw_data_size + null_bitmap_bytes
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
                        hash_seed: collector.hash_seed(),
                        data: filter_data.to_vec(),
                    }
                })
            });

        // Create cardinality info if we have non-null values
        let cardinality_info = if self.count > self.null_count {
            Some(self.sketch_builder.to_cardinality_info())
        } else {
            None
        };

        Ok(StringStats {
            min_size,
            min_non_empty_size: self.min_non_empty_size,
            max_size,
            ascii_count: self.ascii_count,
            count: self.count,
            null_count: self.null_count,
            raw_data_size,
            bloom_filter,
            min_value: self.min_value,
            max_value: self.max_value,
            cardinality_info,
        })
    }

    /// Processes a standard StringArray (UTF-8).
    fn process_string_array(&mut self, array: &StringArray) {
        self.count += array.len();
        self.null_count += array.null_count();

        for string_val in array.iter().flatten() {
            self.update_stats_for_string(string_val, 1);
        }
    }
    /// Processes a LargeStringArray (large UTF-8).
    fn process_large_string_array(&mut self, array: &LargeStringArray) {
        self.count += array.len();
        self.null_count += array.null_count();

        for string_val in array.iter().flatten() {
            self.update_stats_for_string(string_val, 1);
        }
    }
    /// Processes a StringViewArray.
    fn process_string_view_array(&mut self, array: &StringViewArray) {
        self.count += array.len();
        self.null_count += array.null_count();

        for string_val in array.iter().flatten() {
            self.update_stats_for_string(string_val, 1);
        }
    }

    /// Updates statistics for a single string value.
    fn update_stats_for_string(&mut self, string_val: &str, count: u64) {
        let byte_len = string_val.len() as u64;

        // Update min/max size
        self.min_size = cmp::min(self.min_size, byte_len);
        self.max_size = cmp::max(self.max_size, byte_len);

        // Update min_non_empty_size
        if byte_len > 0 {
            self.min_non_empty_size = Some(match self.min_non_empty_size {
                Some(current) => cmp::min(current, byte_len),
                None => byte_len,
            });
        }

        // Check if string contains only ASCII characters
        if string_val.is_ascii_fast() {
            self.ascii_count += count;
        }

        // Update raw_data_size (sum of UTF-8 byte lengths of all non-null strings)
        self.raw_data_size += byte_len * count;

        // Update min/max values for constant detection (optimized tracking)
        if self.value_tracking_enabled {
            let string_bytes = string_val.as_bytes();

            // First calculate if this value would change the current bounds
            let would_be_new_min = self.min_value.is_none()
                || string_bytes < self.min_value.as_ref().unwrap().as_slice();
            let would_be_new_max = self.max_value.is_none()
                || string_bytes > self.max_value.as_ref().unwrap().as_slice();

            if byte_len <= MAX_CONSTANT_STRING_SIZE {
                // Small value - always track and update bounds
                if would_be_new_min {
                    self.min_value = Some(string_bytes.to_vec());
                }
                if would_be_new_max {
                    self.max_value = Some(string_bytes.to_vec());
                }
            } else {
                // Large value (>128 bytes) - only disable tracking if it would change bounds
                if would_be_new_min || would_be_new_max {
                    // Large value would become new min or max - disable tracking
                    self.value_tracking_enabled = false;
                    self.min_value = None;
                    self.max_value = None;
                }
                // If large value is between current min/max, we can ignore it and keep tracking
            }
        }

        // Update bloom filter if enabled
        if let Some(ref mut collector) = self.bloom_filter_collector {
            collector.process_value(string_val.as_bytes());
        }

        self.sketch_builder.add(string_val.as_bytes());
    }
}

impl Default for StringStatsCollector {
    fn default() -> Self {
        Self::new(BlockEncodingProfile::Balanced)
    }
}

/// A struct to hold the collected statistics for string types.
#[derive(Debug, Clone)]
pub struct StringStats {
    /// Indicates the minimum size of the string, in bytes.
    pub min_size: u64,
    /// Indicates the minimum size of a non-empty string, in bytes.
    pub min_non_empty_size: Option<u64>,
    /// Indicates the maximum size of the string, in bytes.
    pub max_size: u64,
    /// Represents the number of value slots containing ASCII-only strings.
    pub ascii_count: u64,
    /// Total count of values processed.
    pub count: usize,
    /// Count of null values processed.
    pub null_count: usize,
    /// Sum of the UTF-8 byte lengths of all non-null strings.
    pub raw_data_size: u64,
    /// Optional bloom filter protobuf if one was successfully constructed during statistics collection.
    pub bloom_filter: Option<amudai_format::defs::shard::SplitBlockBloomFilter>,
    /// Minimum string value (lexicographically, byte-wise comparison).
    /// Only tracked if value size <= MAX_CONSTANT_STRING_SIZE.
    pub min_value: Option<Vec<u8>>,
    /// Maximum string value (lexicographically, byte-wise comparison).
    /// Only tracked if value size <= MAX_CONSTANT_STRING_SIZE.
    pub max_value: Option<Vec<u8>>,
    /// Cardinality information including HLL sketch
    pub cardinality_info: Option<amudai_format::defs::shard::CardinalityInfo>,
}

impl StringStats {
    /// Creates empty string statistics (no data processed).
    ///
    /// # Returns
    /// A `StringStats` instance with default values
    pub fn empty() -> Self {
        Self {
            min_size: 0,
            min_non_empty_size: None,
            max_size: 0,
            ascii_count: 0,
            count: 0,
            null_count: 0,
            raw_data_size: 0,
            bloom_filter: None,
            min_value: None,
            max_value: None,
            cardinality_info: None,
        }
    }

    /// Converts to the protobuf StringStats format.
    pub fn to_proto(&self) -> ProtoStringStats {
        ProtoStringStats {
            min_size: self.min_size,
            min_non_empty_size: self.min_non_empty_size,
            max_size: self.max_size,
            ascii_count: Some(self.ascii_count),
        }
    }

    /// Detects if this field has a constant value based on the statistics.
    /// Returns Some(AnyValue) if all values are the same (all null or all the same non-null value).
    /// Returns None if the field has varying values or a mix of null and non-null values.
    pub fn try_get_constant(&self) -> Option<amudai_format::defs::common::AnyValue> {
        use amudai_format::defs::common::{AnyValue, UnitValue, any_value::Kind};

        if self.count == 0 {
            return None;
        }

        // Check if all values are null
        if self.null_count == self.count {
            return Some(AnyValue {
                annotation: None,
                kind: Some(Kind::NullValue(UnitValue {})),
            });
        }

        // Only check for constant non-null values if there are no nulls
        if self.null_count == 0 {
            // Check if min_value == max_value (constant non-null value)
            if let (Some(min_val), Some(max_val)) = (&self.min_value, &self.max_value) {
                if min_val == max_val {
                    // Convert back to string for the AnyValue
                    if let Ok(string_value) = String::from_utf8(min_val.clone()) {
                        return Some(AnyValue {
                            annotation: None,
                            kind: Some(Kind::StringValue(string_value)),
                        });
                    }
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{LargeStringArray, StringArray, StringViewArray};

    #[test]
    fn test_empty_array() {
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(Vec::<&str>::new());
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.min_size, 0);
        assert_eq!(stats.max_size, 0);
        assert_eq!(stats.min_non_empty_size, None);
        assert_eq!(stats.ascii_count, 0);
        assert_eq!(stats.count, 0);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_all_null_array() {
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec![None::<&str>; 3]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.min_size, 0);
        assert_eq!(stats.max_size, 0);
        assert_eq!(stats.min_non_empty_size, None);
        assert_eq!(stats.ascii_count, 0);
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 3);
        // When all values are null, no storage is needed at all
        assert_eq!(stats.raw_data_size, 0);
    }

    #[test]
    fn test_string_array_with_values() {
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec!["hello", "world", "rust", ""]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.min_size, 0); // Empty string
        assert_eq!(stats.max_size, 5); // "hello" and "world"
        assert_eq!(stats.min_non_empty_size, Some(4)); // "rust"
        assert_eq!(stats.ascii_count, 4); // All are ASCII
        assert_eq!(stats.count, 4);
        assert_eq!(stats.null_count, 0);
        // With balanced profile, bloom filter should be present
        assert!(stats.bloom_filter.is_some());
    }

    #[test]
    fn test_string_array_with_nulls() {
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec![Some("hello"), None, Some("world")]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.min_size, 5); // Both "hello" and "world" are 5 chars
        assert_eq!(stats.max_size, 5);
        assert_eq!(stats.min_non_empty_size, Some(5));
        assert_eq!(stats.ascii_count, 2);
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 1);
        // With balanced profile, bloom filter should be present
        assert!(stats.bloom_filter.is_some());
    }

    #[test]
    fn test_unicode_strings() {
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec!["hello", "こんにちは", "world"]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.min_size, 5); // "hello" and "world"
        assert_eq!(stats.max_size, 15); // "こんにちは" is 15 bytes in UTF-8
        assert_eq!(stats.min_non_empty_size, Some(5));
        assert_eq!(stats.ascii_count, 2); // Only "hello" and "world" are ASCII
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
        // With balanced profile, bloom filter should be present
        assert!(stats.bloom_filter.is_some());
    }

    #[test]
    fn test_large_string_array() {
        let mut collector = StringStatsCollector::default();
        let array = LargeStringArray::from(vec!["small", "medium string", "a"]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.min_size, 1); // "a"
        assert_eq!(stats.max_size, 13); // "medium string"
        assert_eq!(stats.min_non_empty_size, Some(1));
        assert_eq!(stats.ascii_count, 3);
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_string_view_array() {
        let mut collector = StringStatsCollector::default();
        let array = StringViewArray::from(vec!["view1", "view2", "view3"]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.min_size, 5); // All are 5 characters
        assert_eq!(stats.max_size, 5);
        assert_eq!(stats.min_non_empty_size, Some(5));
        assert_eq!(stats.ascii_count, 3);
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_mixed_empty_and_non_empty() {
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec!["", "a", "", "abc", ""]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.min_size, 0); // Empty strings
        assert_eq!(stats.max_size, 3); // "abc"
        assert_eq!(stats.min_non_empty_size, Some(1)); // "a"
        assert_eq!(stats.ascii_count, 5);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_multiple_arrays() {
        let mut collector = StringStatsCollector::default();

        // Process first array
        let array1 = StringArray::from(vec!["hello", "world"]);
        collector.process_array(&array1).unwrap();

        // Process second array
        let array2 = StringArray::from(vec!["a", "longer string"]);
        collector.process_array(&array2).unwrap();

        let stats = collector.finalize().unwrap();

        assert_eq!(stats.min_size, 1); // "a"
        assert_eq!(stats.max_size, 13); // "longer string"
        assert_eq!(stats.min_non_empty_size, Some(1));
        assert_eq!(stats.ascii_count, 4);
        assert_eq!(stats.count, 4);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_to_proto_conversion() {
        let stats = StringStats {
            min_size: 1,
            min_non_empty_size: Some(2),
            max_size: 10,
            ascii_count: 5,
            count: 8,
            null_count: 1,
            raw_data_size: 42,
            bloom_filter: None,
            min_value: None,
            max_value: None,
            cardinality_info: None,
        };

        let proto_stats = stats.to_proto();
        assert_eq!(proto_stats.min_size, 1);
        assert_eq!(proto_stats.min_non_empty_size, Some(2));
        assert_eq!(proto_stats.max_size, 10);
        assert_eq!(proto_stats.ascii_count, Some(5));
    }

    #[test]
    fn test_unsupported_array_type() {
        let mut collector = StringStatsCollector::default();
        let array = arrow_array::Int32Array::from(vec![1, 2, 3]);
        let result = collector.process_array(&array);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unsupported data type")
        );
    }

    #[test]
    fn test_unicode_emoji_strings() {
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec!["👋", "🌍", "🚀", "💻"]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        // Each emoji is 4 bytes in UTF-8
        assert_eq!(stats.min_size, 4);
        assert_eq!(stats.max_size, 4);
        assert_eq!(stats.min_non_empty_size, Some(4));
        assert_eq!(stats.ascii_count, 0); // No ASCII characters
        assert_eq!(stats.count, 4);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_unicode_mixed_scripts() {
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec![
            "Hello", // ASCII - 5 bytes
            "мир",   // Cyrillic - 6 bytes
            "世界",  // Chinese - 6 bytes
            "नमस्ते",  // Devanagari - 18 bytes
            "🌍",    // Emoji - 4 bytes
        ]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.min_size, 4); // Emoji
        assert_eq!(stats.max_size, 18); // Devanagari
        assert_eq!(stats.min_non_empty_size, Some(4));
        assert_eq!(stats.ascii_count, 1); // Only "Hello" is ASCII
        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_unicode_combining_characters() {
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec![
            "é", // Precomposed - 2 bytes
            "é", // Decomposed (e + combining acute) - 3 bytes
            "ñ", // Precomposed - 2 bytes
            "n̂", // Decomposed (n + combining circumflex) - 3 bytes
        ]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.min_size, 2); // Precomposed characters
        assert_eq!(stats.max_size, 3); // Decomposed characters
        assert_eq!(stats.min_non_empty_size, Some(2));
        assert_eq!(stats.ascii_count, 0); // No ASCII characters
        assert_eq!(stats.count, 4);
        assert_eq!(stats.null_count, 0);
    }
    #[test]
    fn test_unicode_zero_width_characters() {
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec![
            "a\u{200B}b", // Zero-width space - 5 bytes
            "c\u{FEFF}d", // Zero-width no-break space (BOM) - 5 bytes
            "e\u{200D}f", // Zero-width joiner - 5 bytes
        ]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.min_size, 5);
        assert_eq!(stats.max_size, 5);
        assert_eq!(stats.min_non_empty_size, Some(5));
        assert_eq!(stats.ascii_count, 0); // Contains non-ASCII zero-width chars
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_unicode_surrogate_pairs() {
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec![
            "𝕳𝖊𝖑𝖑𝖔", // Mathematical bold - each char is 4 bytes, total 20 bytes
            "𝓗𝓮𝓵𝓵𝓸", // Mathematical script - each char is 4 bytes, total 20 bytes
            "🏳️‍🌈",    // Flag with rainbow (complex emoji sequence) - 14 bytes
        ]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.min_size, 14); // Complex emoji
        assert_eq!(stats.max_size, 20); // Mathematical characters
        assert_eq!(stats.min_non_empty_size, Some(14));
        assert_eq!(stats.ascii_count, 0); // No ASCII characters
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
    }
    #[test]
    fn test_unicode_right_to_left_text() {
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec![
            "שלום",       // Hebrew - 8 bytes
            "مرحبا",      // Arabic - 10 bytes
            "Hello שלום", // Mixed LTR/RTL - 14 bytes
        ]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.min_size, 8); // Hebrew
        assert_eq!(stats.max_size, 14); // Mixed text
        assert_eq!(stats.min_non_empty_size, Some(8));
        assert_eq!(stats.ascii_count, 0); // Contains non-ASCII characters
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_unicode_with_ascii_mixed() {
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec![
            "ASCII",  // Pure ASCII - 5 bytes
            "café",   // Mixed ASCII + accented - 5 bytes
            "naïve",  // Mixed ASCII + accented - 6 bytes
            "résumé", // Mixed ASCII + accented - 8 bytes
            "123",    // Pure ASCII numbers - 3 bytes
        ]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.min_size, 3); // "123"
        assert_eq!(stats.max_size, 8); // "résumé"
        assert_eq!(stats.min_non_empty_size, Some(3));
        assert_eq!(stats.ascii_count, 2); // Only "ASCII" and "123" are pure ASCII
        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_unicode_empty_vs_whitespace() {
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec![
            "",         // Empty string - 0 bytes
            " ",        // ASCII space - 1 byte
            "\t",       // ASCII tab - 1 byte
            "\u{00A0}", // Non-breaking space - 2 bytes
            "\u{2000}", // En quad - 3 bytes
            "\u{3000}", // Ideographic space - 3 bytes
        ]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.min_size, 0); // Empty string
        assert_eq!(stats.max_size, 3); // Ideographic space
        assert_eq!(stats.min_non_empty_size, Some(1)); // ASCII space/tab
        assert_eq!(stats.ascii_count, 3); // Empty, space, and tab are ASCII
        assert_eq!(stats.count, 6);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_unicode_normalization_forms() {
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec![
            "café",         // NFC normalized - 5 bytes
            "cafe\u{0301}", // NFD normalized (e + combining acute) - 6 bytes
            "Ω",            // Greek capital omega - 2 bytes
            "Ω",            // Mathematical capital omega - 3 bytes (different codepoint)
        ]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.min_size, 2); // Greek omega
        assert_eq!(stats.max_size, 6); // NFD normalized
        assert_eq!(stats.min_non_empty_size, Some(2));
        assert_eq!(stats.ascii_count, 0); // All contain non-ASCII characters
        assert_eq!(stats.count, 4);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_unicode_with_nulls_and_empty() {
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec![
            Some("🦀"),       // Rust crab emoji - 4 bytes
            None,             // Null
            Some(""),         // Empty string - 0 bytes
            Some("Ελληνικά"), // Greek - 16 bytes
            None,             // Null
            Some("中文"),     // Chinese - 6 bytes
        ]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();
        assert_eq!(stats.min_size, 0); // Empty string
        assert_eq!(stats.max_size, 16); // Greek text
        assert_eq!(stats.min_non_empty_size, Some(4)); // Crab emoji
        assert_eq!(stats.ascii_count, 1); // Only empty string is ASCII
        assert_eq!(stats.count, 6);
        assert_eq!(stats.null_count, 2);
    }

    #[test]
    fn test_string_stats_collector_process_nulls_method() {
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec![Some("hello"), None, Some("world")]);
        collector.process_array(&array).unwrap();

        // Add additional nulls using the dedicated method
        collector.process_nulls(3);

        let stats = collector.finalize().unwrap();
        assert_eq!(stats.count, 6); // 3 from array + 3 from process_nulls
        assert_eq!(stats.null_count, 4); // 1 from array + 3 from process_nulls
        assert_eq!(stats.min_size, 5); // "hello" and "world" are both 5 characters
        assert_eq!(stats.max_size, 5);
    }

    #[test]
    fn test_string_stats_collector_process_nulls_only() {
        let mut collector = StringStatsCollector::default();

        // Only process nulls without any array data
        collector.process_nulls(5);

        let stats = collector.finalize().unwrap();
        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 5);
        assert_eq!(stats.min_size, 0); // No non-null values processed
        assert_eq!(stats.max_size, 0);
        assert_eq!(stats.min_non_empty_size, None);
        assert_eq!(stats.ascii_count, 0);
    }

    #[test]
    fn test_plain_encoding_profile_disables_bloom_filter() {
        let mut collector = StringStatsCollector::new(BlockEncodingProfile::Plain);
        let array = StringArray::from(vec!["hello", "world", "test"]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
        // Plain encoding should NOT have bloom filter for performance
        assert!(stats.bloom_filter.is_none());
    }

    #[test]
    fn test_balanced_encoding_profile_enables_bloom_filter() {
        let mut collector = StringStatsCollector::new(BlockEncodingProfile::Balanced);
        let array = StringArray::from(vec!["hello", "world", "test"]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
        // Balanced encoding should have bloom filter for query optimization
        assert!(stats.bloom_filter.is_some());
    }

    #[test]
    fn test_high_compression_encoding_profile_enables_bloom_filter() {
        let mut collector = StringStatsCollector::new(BlockEncodingProfile::HighCompression);
        let array = StringArray::from(vec!["hello", "world", "test"]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
        // High compression encoding should have bloom filter
        assert!(stats.bloom_filter.is_some());
    }

    #[test]
    fn test_raw_data_size_calculation() {
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec![
            Some("hello"),      // 5 bytes (UTF-8)
            None,               // 0 bytes (null)
            Some("world"),      // 5 bytes (UTF-8)
            Some(""),           // 0 bytes (empty string)
            Some("こんにちは"), // 15 bytes (UTF-8)
        ]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();

        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 1);
        // raw_data_size should be sum of UTF-8 byte lengths of non-null strings + null bitmap overhead
        // Data: 5 + 5 + 0 + 15 = 25 bytes
        // Null bitmap: 5 bits = 1 byte (rounded up)
        // Total: 25 + 1 = 26 bytes
        assert_eq!(stats.raw_data_size, 26);
        assert_eq!(stats.min_size, 0); // Empty string
        assert_eq!(stats.max_size, 15); // "こんにちは"
        assert_eq!(stats.ascii_count, 3); // "hello", "world", and ""
    }

    #[test]
    fn test_string_value_tracking_optimized_for_large_values_between_bounds() {
        // Test that large values between current min/max don't disable tracking
        let mut collector = StringStatsCollector::default();

        // First establish small min/max bounds
        let small_data = vec!["A", "Z"]; // min="A", max="Z"
        let small_array = StringArray::from(small_data);
        collector.process_array(&small_array).unwrap();

        // Then process a large value that falls between the bounds
        let large_middle_value = "M".repeat(200); // 200 bytes, but "M" is between "A" and "Z"
        let large_array = StringArray::from(vec![large_middle_value.as_str()]);
        collector.process_array(&large_array).unwrap();

        let stats = collector.finalize().unwrap();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
        // Value tracking should still be enabled since large value was between bounds
        assert_eq!(stats.min_value, Some("A".as_bytes().to_vec()));
        assert_eq!(stats.max_value, Some("Z".as_bytes().to_vec()));
    }

    #[test]
    fn test_string_value_tracking_disabled_when_large_value_becomes_new_min() {
        // Test that large values that would become new min disable tracking
        let mut collector = StringStatsCollector::default();

        // First establish small bounds
        let small_data = vec!["M", "Z"]; // min="M", max="Z"
        let small_array = StringArray::from(small_data);
        collector.process_array(&small_array).unwrap();

        // Then process a large value that would become new min
        let large_min_value = "A".repeat(200); // 200 bytes, and "AAA..." < "M"
        let large_array = StringArray::from(vec![large_min_value.as_str()]);
        collector.process_array(&large_array).unwrap();

        let stats = collector.finalize().unwrap();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
        // Value tracking should be disabled since large value would be new min
        assert_eq!(stats.min_value, None);
        assert_eq!(stats.max_value, None);
    }

    #[test]
    fn test_string_value_tracking_disabled_when_large_value_becomes_new_max() {
        // Test that large values that would become new max disable tracking
        let mut collector = StringStatsCollector::default();

        // First establish small bounds
        let small_data = vec!["A", "M"]; // min="A", max="M"
        let small_array = StringArray::from(small_data);
        collector.process_array(&small_array).unwrap();

        // Then process a large value that would become new max
        let large_max_value = "Z".repeat(200); // 200 bytes, and "ZZZ..." > "M"
        let large_array = StringArray::from(vec![large_max_value.as_str()]);
        collector.process_array(&large_array).unwrap();

        let stats = collector.finalize().unwrap();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
        // Value tracking should be disabled since large value would be new max
        assert_eq!(stats.min_value, None);
        assert_eq!(stats.max_value, None);
    }

    #[test]
    fn test_optimized_value_tracking_comprehensive_scenario() {
        // Comprehensive test showing the optimization benefits
        let mut collector = StringStatsCollector::default();

        // Start with small values establishing bounds: min="A", max="Z"
        let small_array = StringArray::from(vec!["A", "Z"]);
        collector.process_array(&small_array).unwrap();

        // Process many large values that fall between the bounds
        let large_middle_values = vec![
            "B".repeat(300),  // 300 bytes, but between "A" and "Z"
            "M".repeat(500),  // 500 bytes, but between "A" and "Z"
            "Y".repeat(1000), // 1000 bytes, but between "A" and "Z"
        ];

        for value in &large_middle_values {
            let array = StringArray::from(vec![value.as_str()]);
            collector.process_array(&array).unwrap();
        }

        let stats = collector.finalize().unwrap();
        assert_eq!(stats.count, 5); // 2 small + 3 large
        assert_eq!(stats.null_count, 0);
        // Optimization: tracking should still be enabled since large values were between bounds
        assert_eq!(stats.min_value, Some("A".as_bytes().to_vec()));
        assert_eq!(stats.max_value, Some("Z".as_bytes().to_vec()));

        // This demonstrates that we can still detect constant-like patterns
        // even when most values are large, as long as the actual min/max are small
    }

    #[test]
    fn test_string_constant_value_detection() {
        use amudai_format::defs::common::any_value::Kind;

        // Test 1: All null values should return null constant
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec![None::<&str>; 3]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();
        let constant = stats.try_get_constant();
        assert!(constant.is_some());
        let constant = constant.unwrap();
        assert!(matches!(constant.kind, Some(Kind::NullValue(_))));

        // Test 2: All same non-null values should return that constant
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec!["hello", "hello", "hello"]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();
        let constant = stats.try_get_constant();
        assert!(constant.is_some());
        let constant = constant.unwrap();
        if let Some(Kind::StringValue(value)) = constant.kind {
            assert_eq!(value, "hello");
        } else {
            panic!("Expected StringValue, got {:?}", constant.kind);
        }

        // Test 3: Mixed null and non-null values should NOT return constant
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec![Some("hello"), None, Some("hello")]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();
        let constant = stats.try_get_constant();
        assert!(constant.is_none());

        // Test 4: Different non-null values should NOT return constant
        let mut collector = StringStatsCollector::default();
        let array = StringArray::from(vec!["hello", "world", "rust"]);
        collector.process_array(&array).unwrap();
        let stats = collector.finalize().unwrap();
        let constant = stats.try_get_constant();
        assert!(constant.is_none());
    }

    #[test]
    fn test_string_hll_cardinality_estimation() {
        let mut collector = StringStatsCollector::default();

        // Add some unique values
        let array = StringArray::from(vec!["apple", "banana", "cherry", "date", "elderberry"]);
        collector.process_array(&array).unwrap();

        let stats = collector.finalize().unwrap();

        // Should have cardinality info since we have non-null values
        assert!(stats.cardinality_info.is_some());

        let cardinality_info = stats.cardinality_info.unwrap();
        assert!(cardinality_info.count.is_some());
        assert!(cardinality_info.is_estimate);
        assert!(cardinality_info.hll_sketch.is_some());

        // Estimated count should be around 5 (with wider tolerance for HLL estimation error)
        let estimated_count = cardinality_info.count.unwrap();
        assert!(
            (3..=8).contains(&estimated_count),
            "Expected count around 5, got {estimated_count}"
        );

        // Check HLL sketch is present and has expected structure
        let hll_sketch = cardinality_info.hll_sketch.unwrap();
        assert_eq!(hll_sketch.hash_algorithm, "xxh3_64");
        assert!(hll_sketch.bits_per_index > 0);
        assert!(!hll_sketch.counters.is_empty());
    }

    #[test]
    fn test_string_hll_with_duplicates() {
        let mut collector = StringStatsCollector::default();

        // Add values with duplicates
        let array = StringArray::from(vec![
            "apple", "banana", "apple", "cherry", "banana", "apple",
        ]);
        collector.process_array(&array).unwrap();

        let stats = collector.finalize().unwrap();

        // Should have cardinality info
        assert!(stats.cardinality_info.is_some());

        let cardinality_info = stats.cardinality_info.unwrap();
        let estimated_count = cardinality_info.count.unwrap();

        // Should estimate around 3 unique values despite 6 total values (wider tolerance)
        assert!(
            (2..=5).contains(&estimated_count),
            "Expected count around 3, got {estimated_count}"
        );
    }

    #[test]
    fn test_string_hll_all_nulls() {
        let mut collector = StringStatsCollector::default();

        // Add only null values
        let array = StringArray::from(vec![None::<&str>; 5]);
        collector.process_array(&array).unwrap();

        let stats = collector.finalize().unwrap();

        // Should NOT have cardinality info for all-null data
        assert!(stats.cardinality_info.is_none());
    }

    #[test]
    fn test_string_hll_large_dataset() {
        let mut collector = StringStatsCollector::default();

        // Generate a large dataset with known cardinality
        let mut values = Vec::new();
        for i in 0..1000 {
            values.push(format!("value_{i}"));
        }
        // Add some duplicates to test cardinality estimation with larger datasets
        for i in 0..100 {
            values.push(format!("value_{i}")); // Duplicate first 100 values
        }

        let string_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        let array = StringArray::from(string_refs);
        collector.process_array(&array).unwrap();

        let stats = collector.finalize().unwrap();

        // Should have cardinality info
        assert!(stats.cardinality_info.is_some());

        let cardinality_info = stats.cardinality_info.unwrap();
        let estimated_count = cardinality_info.count.unwrap();

        // For 1000 unique values, HLL estimation error can be significant
        // Standard error is approximately 1.04/sqrt(2^bits_per_index)
        // With default bits (likely 12), error is about 1.04/sqrt(4096) ≈ 1.6%
        // But in practice, we should allow for wider tolerance
        assert!(
            (800..=1200).contains(&estimated_count),
            "Expected count around 1000, got {estimated_count}"
        );

        assert_eq!(stats.count, 1100); // Total count including duplicates
        assert!(cardinality_info.is_estimate);
    }
}
