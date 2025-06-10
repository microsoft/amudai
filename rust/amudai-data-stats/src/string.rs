use amudai_common::Result;
use amudai_format::defs::shard::StringStats as ProtoStringStats;
use arrow_array::{Array, LargeStringArray, StringArray, StringViewArray, cast::AsArray};
use std::cmp;

/// A struct to collect statistics for string types.
pub struct StringStatsCollector {
    min_size: u64,
    min_non_empty_size: Option<u64>,
    max_size: u64,
    ascii_count: u64,
    count: usize,
    null_count: usize,
}

impl StringStatsCollector {
    /// Creates a new `StringStatsCollector`.
    ///
    /// # Returns
    ///
    /// A new instance of `StringStatsCollector`.
    pub fn new() -> Self {
        Self {
            min_size: u64::MAX,
            min_non_empty_size: None,
            max_size: 0,
            ascii_count: 0,
            count: 0,
            null_count: 0,
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

    /// Finalizes the statistics collection and returns the collected statistics.
    ///
    /// # Returns
    ///
    /// A `Result` containing the collected `StringStats` or an error.
    pub fn finish(self) -> Result<StringStats> {
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

        Ok(StringStats {
            min_size,
            min_non_empty_size: self.min_non_empty_size,
            max_size,
            ascii_count: self.ascii_count,
            count: self.count,
            null_count: self.null_count,
        })
    }

    /// Processes a standard StringArray (UTF-8).
    fn process_string_array(&mut self, array: &StringArray) {
        self.count += array.len();
        self.null_count += array.null_count();

        for value in array.iter() {
            if let Some(string_val) = value {
                self.update_stats_for_string(string_val);
            }
        }
    }

    /// Processes a LargeStringArray (large UTF-8).
    fn process_large_string_array(&mut self, array: &LargeStringArray) {
        self.count += array.len();
        self.null_count += array.null_count();

        for value in array.iter() {
            if let Some(string_val) = value {
                self.update_stats_for_string(string_val);
            }
        }
    }

    /// Processes a StringViewArray.
    fn process_string_view_array(&mut self, array: &StringViewArray) {
        self.count += array.len();
        self.null_count += array.null_count();

        for value in array.iter() {
            if let Some(string_val) = value {
                self.update_stats_for_string(string_val);
            }
        }
    }

    /// Updates statistics for a single string value.
    fn update_stats_for_string(&mut self, string_val: &str) {
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
        if string_val.is_ascii() {
            self.ascii_count += 1;
        }
    }
}

impl Default for StringStatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// A struct to hold the collected statistics for string types.
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
}

impl StringStats {
    /// Converts to the protobuf StringStats format.
    pub fn to_proto(&self) -> ProtoStringStats {
        ProtoStringStats {
            min_size: self.min_size,
            min_non_empty_size: self.min_non_empty_size,
            max_size: self.max_size,
            ascii_count: Some(self.ascii_count),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{LargeStringArray, StringArray, StringViewArray};

    #[test]
    fn test_empty_array() {
        let mut collector = StringStatsCollector::new();
        let array = StringArray::from(Vec::<&str>::new());
        collector.process_array(&array).unwrap();
        let stats = collector.finish().unwrap();

        assert_eq!(stats.min_size, 0);
        assert_eq!(stats.max_size, 0);
        assert_eq!(stats.min_non_empty_size, None);
        assert_eq!(stats.ascii_count, 0);
        assert_eq!(stats.count, 0);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_all_null_array() {
        let mut collector = StringStatsCollector::new();
        let array = StringArray::from(vec![None::<&str>; 3]);
        collector.process_array(&array).unwrap();
        let stats = collector.finish().unwrap();

        assert_eq!(stats.min_size, 0);
        assert_eq!(stats.max_size, 0);
        assert_eq!(stats.min_non_empty_size, None);
        assert_eq!(stats.ascii_count, 0);
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 3);
    }

    #[test]
    fn test_string_array_with_values() {
        let mut collector = StringStatsCollector::new();
        let array = StringArray::from(vec!["hello", "world", "rust", ""]);
        collector.process_array(&array).unwrap();
        let stats = collector.finish().unwrap();

        assert_eq!(stats.min_size, 0); // Empty string
        assert_eq!(stats.max_size, 5); // "hello" and "world"
        assert_eq!(stats.min_non_empty_size, Some(4)); // "rust"
        assert_eq!(stats.ascii_count, 4); // All are ASCII
        assert_eq!(stats.count, 4);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_string_array_with_nulls() {
        let mut collector = StringStatsCollector::new();
        let array = StringArray::from(vec![Some("hello"), None, Some("world")]);
        collector.process_array(&array).unwrap();
        let stats = collector.finish().unwrap();

        assert_eq!(stats.min_size, 5); // Both "hello" and "world" are 5 chars
        assert_eq!(stats.max_size, 5);
        assert_eq!(stats.min_non_empty_size, Some(5));
        assert_eq!(stats.ascii_count, 2);
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 1);
    }

    #[test]
    fn test_unicode_strings() {
        let mut collector = StringStatsCollector::new();
        let array = StringArray::from(vec!["hello", "こんにちは", "world"]);
        collector.process_array(&array).unwrap();
        let stats = collector.finish().unwrap();

        assert_eq!(stats.min_size, 5); // "hello" and "world"
        assert_eq!(stats.max_size, 15); // "こんにちは" is 15 bytes in UTF-8
        assert_eq!(stats.min_non_empty_size, Some(5));
        assert_eq!(stats.ascii_count, 2); // Only "hello" and "world" are ASCII
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_large_string_array() {
        let mut collector = StringStatsCollector::new();
        let array = LargeStringArray::from(vec!["small", "medium string", "a"]);
        collector.process_array(&array).unwrap();
        let stats = collector.finish().unwrap();

        assert_eq!(stats.min_size, 1); // "a"
        assert_eq!(stats.max_size, 13); // "medium string"
        assert_eq!(stats.min_non_empty_size, Some(1));
        assert_eq!(stats.ascii_count, 3);
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_string_view_array() {
        let mut collector = StringStatsCollector::new();
        let array = StringViewArray::from(vec!["test", "string", "view"]);
        collector.process_array(&array).unwrap();
        let stats = collector.finish().unwrap();

        assert_eq!(stats.min_size, 4); // "test" and "view"
        assert_eq!(stats.max_size, 6); // "string"
        assert_eq!(stats.min_non_empty_size, Some(4));
        assert_eq!(stats.ascii_count, 3);
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_mixed_empty_and_non_empty() {
        let mut collector = StringStatsCollector::new();
        let array = StringArray::from(vec!["", "a", "", "abc", ""]);
        collector.process_array(&array).unwrap();
        let stats = collector.finish().unwrap();

        assert_eq!(stats.min_size, 0); // Empty strings
        assert_eq!(stats.max_size, 3); // "abc"
        assert_eq!(stats.min_non_empty_size, Some(1)); // "a"
        assert_eq!(stats.ascii_count, 5);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_multiple_arrays() {
        let mut collector = StringStatsCollector::new();

        // Process first array
        let array1 = StringArray::from(vec!["hello", "world"]);
        collector.process_array(&array1).unwrap();

        // Process second array
        let array2 = StringArray::from(vec!["a", "longer string"]);
        collector.process_array(&array2).unwrap();

        let stats = collector.finish().unwrap();

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
        };

        let proto_stats = stats.to_proto();
        assert_eq!(proto_stats.min_size, 1);
        assert_eq!(proto_stats.min_non_empty_size, Some(2));
        assert_eq!(proto_stats.max_size, 10);
        assert_eq!(proto_stats.ascii_count, Some(5));
    }

    #[test]
    fn test_unsupported_array_type() {
        let mut collector = StringStatsCollector::new();
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
        let mut collector = StringStatsCollector::new();
        let array = StringArray::from(vec!["👋", "🌍", "🚀", "💻"]);
        collector.process_array(&array).unwrap();
        let stats = collector.finish().unwrap();

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
        let mut collector = StringStatsCollector::new();
        let array = StringArray::from(vec![
            "Hello", // ASCII - 5 bytes
            "мир",   // Cyrillic - 6 bytes
            "世界",  // Chinese - 6 bytes
            "नमस्ते",  // Devanagari - 18 bytes
            "🌍",    // Emoji - 4 bytes
        ]);
        collector.process_array(&array).unwrap();
        let stats = collector.finish().unwrap();

        assert_eq!(stats.min_size, 4); // Emoji
        assert_eq!(stats.max_size, 18); // Devanagari
        assert_eq!(stats.min_non_empty_size, Some(4));
        assert_eq!(stats.ascii_count, 1); // Only "Hello" is ASCII
        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_unicode_combining_characters() {
        let mut collector = StringStatsCollector::new();
        let array = StringArray::from(vec![
            "é", // Precomposed - 2 bytes
            "é", // Decomposed (e + combining acute) - 3 bytes
            "ñ", // Precomposed - 2 bytes
            "n̂", // Decomposed (n + combining circumflex) - 3 bytes
        ]);
        collector.process_array(&array).unwrap();
        let stats = collector.finish().unwrap();

        assert_eq!(stats.min_size, 2); // Precomposed characters
        assert_eq!(stats.max_size, 3); // Decomposed characters
        assert_eq!(stats.min_non_empty_size, Some(2));
        assert_eq!(stats.ascii_count, 0); // No ASCII characters
        assert_eq!(stats.count, 4);
        assert_eq!(stats.null_count, 0);
    }
    #[test]
    fn test_unicode_zero_width_characters() {
        let mut collector = StringStatsCollector::new();
        let array = StringArray::from(vec![
            "a\u{200B}b", // Zero-width space - 5 bytes
            "c\u{FEFF}d", // Zero-width no-break space (BOM) - 5 bytes
            "e\u{200D}f", // Zero-width joiner - 5 bytes
        ]);
        collector.process_array(&array).unwrap();
        let stats = collector.finish().unwrap();

        assert_eq!(stats.min_size, 5);
        assert_eq!(stats.max_size, 5);
        assert_eq!(stats.min_non_empty_size, Some(5));
        assert_eq!(stats.ascii_count, 0); // Contains non-ASCII zero-width chars
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_unicode_surrogate_pairs() {
        let mut collector = StringStatsCollector::new();
        let array = StringArray::from(vec![
            "𝕳𝖊𝖑𝖑𝖔", // Mathematical bold - each char is 4 bytes, total 20 bytes
            "𝓗𝓮𝓵𝓵𝓸", // Mathematical script - each char is 4 bytes, total 20 bytes
            "🏳️‍🌈",    // Flag with rainbow (complex emoji sequence) - 14 bytes
        ]);
        collector.process_array(&array).unwrap();
        let stats = collector.finish().unwrap();

        assert_eq!(stats.min_size, 14); // Complex emoji
        assert_eq!(stats.max_size, 20); // Mathematical characters
        assert_eq!(stats.min_non_empty_size, Some(14));
        assert_eq!(stats.ascii_count, 0); // No ASCII characters
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
    }
    #[test]
    fn test_unicode_right_to_left_text() {
        let mut collector = StringStatsCollector::new();
        let array = StringArray::from(vec![
            "שלום",       // Hebrew - 8 bytes
            "مرحبا",      // Arabic - 10 bytes
            "Hello שלום", // Mixed LTR/RTL - 14 bytes
        ]);
        collector.process_array(&array).unwrap();
        let stats = collector.finish().unwrap();

        assert_eq!(stats.min_size, 8); // Hebrew
        assert_eq!(stats.max_size, 14); // Mixed text
        assert_eq!(stats.min_non_empty_size, Some(8));
        assert_eq!(stats.ascii_count, 0); // Contains non-ASCII characters
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_unicode_with_ascii_mixed() {
        let mut collector = StringStatsCollector::new();
        let array = StringArray::from(vec![
            "ASCII",  // Pure ASCII - 5 bytes
            "café",   // Mixed ASCII + accented - 5 bytes
            "naïve",  // Mixed ASCII + accented - 6 bytes
            "résumé", // Mixed ASCII + accented - 8 bytes
            "123",    // Pure ASCII numbers - 3 bytes
        ]);
        collector.process_array(&array).unwrap();
        let stats = collector.finish().unwrap();

        assert_eq!(stats.min_size, 3); // "123"
        assert_eq!(stats.max_size, 8); // "résumé"
        assert_eq!(stats.min_non_empty_size, Some(3));
        assert_eq!(stats.ascii_count, 2); // Only "ASCII" and "123" are pure ASCII
        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_unicode_empty_vs_whitespace() {
        let mut collector = StringStatsCollector::new();
        let array = StringArray::from(vec![
            "",         // Empty string - 0 bytes
            " ",        // ASCII space - 1 byte
            "\t",       // ASCII tab - 1 byte
            "\u{00A0}", // Non-breaking space - 2 bytes
            "\u{2000}", // En quad - 3 bytes
            "\u{3000}", // Ideographic space - 3 bytes
        ]);
        collector.process_array(&array).unwrap();
        let stats = collector.finish().unwrap();

        assert_eq!(stats.min_size, 0); // Empty string
        assert_eq!(stats.max_size, 3); // Ideographic space
        assert_eq!(stats.min_non_empty_size, Some(1)); // ASCII space/tab
        assert_eq!(stats.ascii_count, 3); // Empty, space, and tab are ASCII
        assert_eq!(stats.count, 6);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_unicode_normalization_forms() {
        let mut collector = StringStatsCollector::new();
        let array = StringArray::from(vec![
            "café",         // NFC normalized - 5 bytes
            "cafe\u{0301}", // NFD normalized (e + combining acute) - 6 bytes
            "Ω",            // Greek capital omega - 2 bytes
            "Ω",            // Mathematical capital omega - 3 bytes (different codepoint)
        ]);
        collector.process_array(&array).unwrap();
        let stats = collector.finish().unwrap();

        assert_eq!(stats.min_size, 2); // Greek omega
        assert_eq!(stats.max_size, 6); // NFD normalized
        assert_eq!(stats.min_non_empty_size, Some(2));
        assert_eq!(stats.ascii_count, 0); // All contain non-ASCII characters
        assert_eq!(stats.count, 4);
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_unicode_with_nulls_and_empty() {
        let mut collector = StringStatsCollector::new();
        let array = StringArray::from(vec![
            Some("🦀"),       // Rust crab emoji - 4 bytes
            None,             // Null
            Some(""),         // Empty string - 0 bytes
            Some("Ελληνικά"), // Greek - 16 bytes
            None,             // Null
            Some("中文"),     // Chinese - 6 bytes
        ]);
        collector.process_array(&array).unwrap();
        let stats = collector.finish().unwrap();

        assert_eq!(stats.min_size, 0); // Empty string
        assert_eq!(stats.max_size, 16); // Greek text
        assert_eq!(stats.min_non_empty_size, Some(4)); // Crab emoji
        assert_eq!(stats.ascii_count, 1); // Only empty string is ASCII
        assert_eq!(stats.count, 6);
        assert_eq!(stats.null_count, 2);
    }
}
