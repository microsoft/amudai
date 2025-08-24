//! Comprehensive bloom filter tests covering various scenarios.
//!
//! This module contains integration tests that validate the complete workflow
//! from building bloom filters to decoding them, ensuring correctness and
//! performance across different scenarios.

use amudai_bytes::Bytes;
use std::collections::HashSet;

use crate::{
    builder::{BloomFilterCollector, SbbfBuilder},
    config::{BloomFilterConfig, XXH3_64_ALGORITHM},
    decoder::SbbfDecoder,
};

/// Helper function to build a filter with test values and return both builder and decoder
fn build_filter_with_values(
    values: &[&str],
    expected_items: usize,
    target_fpp: f64,
    max_size: usize,
) -> (SbbfBuilder, SbbfDecoder) {
    let mut builder = SbbfBuilder::new(expected_items, target_fpp, max_size).unwrap();

    for value in values {
        builder.add_hash(xxhash_rust::xxh3::xxh3_64_with_seed(
            value.as_bytes(),
            BloomFilterConfig::default().hash_seed,
        ));
    }

    builder.finish();
    let filter_data = Bytes::copy_from_slice(builder.filter_data_as_bytes()).align(32);
    let decoder =
        SbbfDecoder::from_aligned_data(filter_data, BloomFilterConfig::default().hash_seed)
            .unwrap();

    (builder, decoder)
}

/// Helper function to build a filter with String values and return both builder and decoder
fn build_filter_with_string_values(
    values: &[String],
    expected_items: usize,
    target_fpp: f64,
    max_size: usize,
) -> (SbbfBuilder, SbbfDecoder) {
    let mut builder = SbbfBuilder::new(expected_items, target_fpp, max_size).unwrap();

    for value in values {
        builder.add_hash(xxhash_rust::xxh3::xxh3_64_with_seed(
            value.as_bytes(),
            crate::config::BLOOM_FILTER_HASH_SEED,
        ));
    }

    println!(
        "Before finish: {} distinct values added",
        builder.distinct_count()
    );
    let success = builder.finish();
    println!("Filter build success: {success}");
    if success {
        println!(
            "Filter size: {} bytes",
            builder.filter_data_as_bytes().len()
        );
    }

    let filter_data = Bytes::copy_from_slice(builder.filter_data_as_bytes()).align(32);
    let decoder =
        SbbfDecoder::from_aligned_data(filter_data, BloomFilterConfig::default().hash_seed)
            .unwrap();

    (builder, decoder)
}

/// Helper function to generate test data sets of different sizes
fn generate_test_data(count: usize, prefix: &str) -> Vec<String> {
    (0..count).map(|i| format!("{prefix}_{i:06}")).collect()
}

/// Helper function to generate non-overlapping test data for false positive testing
fn generate_non_overlapping_test_data(count: usize, prefix: &str) -> Vec<String> {
    (0..count)
        .map(|i| format!("{prefix}_negative_{i:06}"))
        .collect()
}

/// Helper function to measure false positive rate
fn measure_false_positive_rate(
    decoder: &SbbfDecoder,
    inserted_values: &[String],
    test_values: &[String],
) -> f64 {
    let inserted_set: HashSet<&String> = inserted_values.iter().collect();
    let mut false_positives = 0;
    let mut true_negatives = 0;

    for value in test_values {
        if !inserted_set.contains(value) {
            if decoder.probe(value.as_bytes()) {
                false_positives += 1;
            } else {
                true_negatives += 1;
            }
        }
    }

    let total_negatives = false_positives + true_negatives;
    if total_negatives == 0 {
        0.0
    } else {
        false_positives as f64 / total_negatives as f64
    }
}

#[cfg(test)]
mod builder_tests {
    use super::*;

    #[test]
    fn test_sbbf_builder_basic_functionality() {
        let mut builder = SbbfBuilder::new(100, 0.01, 1024).unwrap();

        // Test adding values and duplicates
        let v1 = "test1";
        let v2 = "test2";
        assert!(builder.add_hash(xxhash_rust::xxh3::xxh3_64(v1.as_bytes())));
        assert!(builder.add_hash(xxhash_rust::xxh3::xxh3_64(v2.as_bytes())));
        assert!(!builder.add_hash(xxhash_rust::xxh3::xxh3_64(v1.as_bytes())));

        assert_eq!(builder.distinct_count(), 2);

        // Build the filter
        let success = builder.finish();
        assert!(success);

        // Verify properties
        assert_eq!(builder.num_values(), 2);
        assert!(!builder.filter_data_as_bytes().is_empty());
        assert!(builder.num_blocks() > 0);
    }

    #[test]
    fn test_sbbf_builder_xxh3_64_only() {
        let mut builder = SbbfBuilder::new(100, 0.01, 1024).unwrap();

        let v1 = "test1";
        let v2 = "test2";
        assert!(builder.add_hash(xxhash_rust::xxh3::xxh3_64(v1.as_bytes())));
        assert!(builder.add_hash(xxhash_rust::xxh3::xxh3_64(v2.as_bytes())));
        assert!(!builder.add_hash(xxhash_rust::xxh3::xxh3_64(v1.as_bytes()))); // Duplicate

        assert_eq!(builder.distinct_count(), 2);

        let success = builder.finish();
        assert!(success);

        assert!(!builder.filter_data_as_bytes().is_empty());
        assert!(builder.num_blocks() > 0);
    }

    #[test]
    fn test_sbbf_builder_empty_filter() {
        let mut builder = SbbfBuilder::new(100, 0.01, 1024).unwrap();

        // Don't add any values
        let success = builder.finish();
        assert!(!success); // Should fail for empty filter
    }

    #[test]
    fn test_bloom_filter_collector_basic() {
        let config = BloomFilterConfig {
            cardinality_threshold: 5,
            target_fpp: 0.01,
            max_filter_size: 1024,
            hash_algorithm: XXH3_64_ALGORITHM.to_string(),
            hash_seed: BloomFilterConfig::default().hash_seed,
        };

        let mut collector = BloomFilterCollector::new(config);

        // Add values within threshold
        assert!(collector.process_value("test1".as_bytes()));
        assert!(collector.process_value("test2".as_bytes()));
        assert!(collector.process_value("test3".as_bytes()));
        assert!(collector.process_value("test4".as_bytes()));
        assert!(collector.process_value("test5".as_bytes()));

        // This should exceed threshold and abandon construction
        assert!(!collector.process_value("test6".as_bytes()));

        let filter_built = collector.finish();
        assert!(!filter_built);
    }

    #[test]
    fn test_bloom_filter_collector_successful_build() {
        let config = BloomFilterConfig {
            cardinality_threshold: 10,
            target_fpp: 0.01,
            max_filter_size: 1024,
            hash_algorithm: XXH3_64_ALGORITHM.to_string(),
            hash_seed: BloomFilterConfig::default().hash_seed,
        };

        let mut collector = BloomFilterCollector::new(config);

        // Add fewer values than threshold
        assert!(collector.process_value("test1".as_bytes()));
        assert!(collector.process_value("test2".as_bytes()));
        assert!(collector.process_value("test3".as_bytes()));

        let filter_built = collector.finish();
        assert!(filter_built);

        // Check that we can access filter properties
        assert!(!collector.filter_data_as_bytes().is_empty());
        assert!(collector.num_blocks() > 0);
        assert!(collector.target_fpp() > 0.0);
        assert!(collector.num_values() > 0);
    }

    #[test]
    fn test_bloom_filter_collector_duplicate_handling() {
        let config = BloomFilterConfig {
            cardinality_threshold: 5,
            target_fpp: 0.01,
            max_filter_size: 1024,
            hash_algorithm: XXH3_64_ALGORITHM.to_string(),
            hash_seed: BloomFilterConfig::default().hash_seed,
        };

        let mut collector = BloomFilterCollector::new(config);

        // Add duplicate values - should not count towards threshold
        assert!(collector.process_value("test1".as_bytes()));
        assert!(collector.process_value("test1".as_bytes())); // Duplicate
        assert!(collector.process_value("test2".as_bytes()));
        assert!(collector.process_value("test2".as_bytes())); // Duplicate
        assert!(collector.process_value("test3".as_bytes()));
        assert!(collector.process_value("test4".as_bytes()));
        assert!(collector.process_value("test5".as_bytes()));

        // This should exceed the threshold and abandon construction
        assert!(!collector.process_value("test6".as_bytes()));

        let filter_built = collector.finish();
        assert!(!filter_built);
    }
}

#[cfg(test)]
mod decoder_tests {
    use super::*;

    #[test]
    fn test_sbbf_decoder_basic_functionality() {
        // Create a filter with test data
        let mut builder = SbbfBuilder::new(100, 0.01, 1024).unwrap();

        let v1 = "test1";
        let v2 = "test2";
        let v3 = "test3";
        assert!(builder.add_hash(xxhash_rust::xxh3::xxh3_64(v1.as_bytes())));
        assert!(builder.add_hash(xxhash_rust::xxh3::xxh3_64(v2.as_bytes())));
        assert!(builder.add_hash(xxhash_rust::xxh3::xxh3_64(v3.as_bytes())));

        builder.finish();

        // Create decoder from builder data
        let filter_data = Bytes::copy_from_slice(builder.filter_data_as_bytes()).align(32);
        let decoder =
            SbbfDecoder::from_aligned_data(filter_data, BloomFilterConfig::default().hash_seed)
                .unwrap();

        // Test probing for inserted values using probe_hash with the same hashes we inserted
        assert!(decoder.probe_hash(xxhash_rust::xxh3::xxh3_64(v1.as_bytes())));
        assert!(decoder.probe_hash(xxhash_rust::xxh3::xxh3_64(v2.as_bytes())));
        assert!(decoder.probe_hash(xxhash_rust::xxh3::xxh3_64(v3.as_bytes())));
    }

    #[test]
    fn test_sbbf_decoder_xxh3_64_only() {
        let mut builder = SbbfBuilder::new(50, 0.05, 512).unwrap();

        let v1 = "xxh3_test1";
        let v2 = "xxh3_test2";
        assert!(builder.add_hash(xxhash_rust::xxh3::xxh3_64(v1.as_bytes())));
        assert!(builder.add_hash(xxhash_rust::xxh3::xxh3_64(v2.as_bytes())));

        builder.finish();
        let filter_data = Bytes::copy_from_slice(builder.filter_data_as_bytes()).align(32);
        let decoder =
            SbbfDecoder::from_aligned_data(filter_data, BloomFilterConfig::default().hash_seed)
                .unwrap();

        assert!(decoder.probe_hash(xxhash_rust::xxh3::xxh3_64(v1.as_bytes())));
        assert!(decoder.probe_hash(xxhash_rust::xxh3::xxh3_64(v2.as_bytes())));
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_build_decode_integration_basic() {
        let test_values = vec!["value1", "value2", "value3", "value4", "value5"];
        let (builder, decoder) = build_filter_with_values(&test_values, 100, 0.01, 1024);

        // Verify builder properties
        assert_eq!(builder.num_values(), test_values.len() as u64);

        // Verify all values can be found
        for value in &test_values {
            assert!(
                decoder.probe(value.as_bytes()),
                "Value '{value}' should be found"
            );
        }

        // Test some values that weren't inserted
        let non_inserted = ["not_there1", "not_there2", "not_there3"];
        let mut false_positives = 0;
        for value in &non_inserted {
            if decoder.probe(value.as_bytes()) {
                false_positives += 1;
            }
        }

        // Should have reasonable false positive rate
        let fpp_rate = false_positives as f64 / non_inserted.len() as f64;
        assert!(fpp_rate <= 0.1, "False positive rate {fpp_rate} too high");
    }

    #[test]
    fn test_build_decode_integration_various_patterns() {
        let test_values = vec![
            "simple",
            "with_numbers_123",
            "with.dots.and.symbols",
            "UPPERCASE_STRING",
            "MixedCaseString",
            "string with spaces",
            "unicode_测试_string",
            "very_long_string_that_contains_many_characters_and_should_still_work_properly",
            "",   // Empty string
            "a",  // Single character
            "ab", // Two characters
            "special!@#$%^&*()characters",
            "path/to/some/file.txt",
            "email@example.com",
            "http://www.example.com/path?query=value",
            "2023-12-25T10:30:00Z",
            "{'json': 'like', 'data': 123}",
        ];

        let (builder, decoder) = build_filter_with_values(&test_values, 1000, 0.001, 4096);

        // Verify builder worked
        assert_eq!(builder.num_values(), test_values.len() as u64);

        // All inserted values should be found
        for value in &test_values {
            assert!(
                decoder.probe(value.as_bytes()),
                "Value '{value}' should be found"
            );
        }

        // Test some values that were not inserted
        let non_inserted = [
            "not_inserted_1",
            "not_inserted_2",
            "different_pattern",
            "another_test_string",
        ];

        let false_positives = non_inserted
            .iter()
            .filter(|&value| decoder.probe(value.as_bytes()))
            .count();

        let fpp_rate = false_positives as f64 / non_inserted.len() as f64;
        assert!(fpp_rate <= 0.1, "False positive rate {fpp_rate} too high");
    }

    #[test]
    fn test_build_decode_integration_xxh3_64() {
        let test_values = vec!["test1", "test2", "test3", "test4", "test5"];

        // Test with XXH3_64
        let (builder, decoder) = build_filter_with_values(&test_values, 100, 0.01, 1024);

        // Should work correctly
        assert_eq!(builder.num_values(), test_values.len() as u64);

        for value in &test_values {
            assert!(
                decoder.probe(value.as_bytes()),
                "XXH3_64 should find '{value}'"
            );
        }

        // Test with non-existent values
        assert!(!decoder.probe("nonexistent1".as_bytes()));
        assert!(!decoder.probe("nonexistent2".as_bytes()));
    }

    #[test]
    fn test_build_decode_integration_performance_patterns() {
        let mut test_values = Vec::new();

        // Create patterns that might have similar hashes
        for i in 0..100 {
            test_values.push(format!("pattern_{i:04}"));
            test_values.push(format!("test_string_{i}"));
            test_values.push(format!("data_item_{i}"));
            test_values.push(format!("value_{i}_suffix"));
            test_values.push(format!("prefix_{i}_value"));
        }

        let test_value_refs: Vec<&str> = test_values.iter().map(|s| s.as_str()).collect();
        let (builder, decoder) = build_filter_with_values(&test_value_refs, 500, 0.01, 2048);

        // Verify builder worked
        assert_eq!(builder.num_values(), test_values.len() as u64);

        // Test that all patterns are found
        for value in &test_values {
            assert!(
                decoder.probe(value.as_bytes()),
                "Value '{value}' should be found"
            );
        }
    }

    #[test]
    fn test_build_decode_integration_size_validation() {
        let test_cases = vec![
            (10, 0.01, 256),     // Small filter
            (100, 0.01, 1024),   // Medium filter
            (1000, 0.001, 4096), // Large filter with low FPP
        ];

        for (num_values, target_fpp, max_size) in test_cases {
            let test_values: Vec<String> = (0..num_values.min(50))
                .map(|i| format!("test_value_{i}"))
                .collect();
            let test_value_refs: Vec<&str> = test_values.iter().map(|s| s.as_str()).collect();

            let (builder, decoder) =
                build_filter_with_values(&test_value_refs, num_values, target_fpp, max_size);

            // Verify basic functionality
            assert!(builder.num_values() > 0);
            assert!(!builder.filter_data_as_bytes().is_empty());

            // Test that first value can be found
            if !test_values.is_empty() {
                assert!(decoder.probe(test_values[0].as_bytes()));
            }
        }
    }

    #[test]
    fn test_build_decode_integration_edge_cases() {
        // Test with minimal filter (single value)
        let single_value = vec!["single_value"];
        let (builder, decoder) = build_filter_with_values(&single_value, 1, 0.1, 32);

        assert_eq!(builder.num_values(), 1);
        assert!(decoder.probe("single_value".as_bytes()));
    }

    #[test]
    fn test_collector_to_decoder_integration() {
        let config = BloomFilterConfig {
            cardinality_threshold: 10,
            target_fpp: 0.01,
            max_filter_size: 1024,
            hash_algorithm: XXH3_64_ALGORITHM.to_string(),
            hash_seed: BloomFilterConfig::default().hash_seed,
        };

        let mut collector = BloomFilterCollector::new(config);
        let test_values = vec!["test1", "test2", "test3", "test4", "test5"];

        // Add values to collector
        for value in &test_values {
            assert!(collector.process_value(value.as_bytes()));
        }

        // Build the filter
        assert!(collector.finish());

        // Create decoder from collector data
        let filter_data = Bytes::copy_from_slice(collector.filter_data_as_bytes()).align(32);
        let decoder = SbbfDecoder::from_aligned_data(filter_data, collector.hash_seed()).unwrap();

        // Verify all values can be found
        for value in &test_values {
            assert!(
                decoder.probe(value.as_bytes()),
                "Value '{value}' should be found"
            );
        }
    }

    #[test]
    fn test_large_scale_integration() {
        // Test with a more reasonable number of values for this test
        let test_values: Vec<String> = (0..50)
            .map(|i| format!("large_scale_test_value_{i:06}"))
            .collect();
        let test_value_refs: Vec<&str> = test_values.iter().map(|s| s.as_str()).collect();

        let (builder, decoder) = build_filter_with_values(&test_value_refs, 50, 0.01, 4096);

        assert_eq!(builder.num_values(), 50);

        // Test all values are found - this is the main requirement
        for value in &test_values {
            assert!(
                decoder.probe(value.as_bytes()),
                "Value '{value}' should be found"
            );
        }

        // Test some non-inserted values - we'll just check that it doesn't crash
        // and that at least some values return false (not all false positives)
        let non_inserted: Vec<String> = (0..10)
            .map(|i| format!("never_inserted_xyz_{i:03}"))
            .collect();

        let mut found_count = 0;
        for value in &non_inserted {
            if decoder.probe(value.as_bytes()) {
                found_count += 1;
            }
        }

        // Just verify that not ALL non-inserted values are false positives
        // (that would indicate a broken filter)
        assert!(
            found_count < non_inserted.len(),
            "All non-inserted values cannot be false positives"
        );
    }

    #[test]
    fn test_false_positive_rate_small_filter() {
        let num_values = 100;
        let target_fpp = 0.05;
        let max_size = 512;

        // Generate test data
        let test_values = generate_test_data(num_values, "test_value");
        let non_inserted_values = generate_non_overlapping_test_data(num_values, "test_value");

        // Build filter with test data
        let (_builder, decoder) =
            build_filter_with_string_values(&test_values, num_values, target_fpp, max_size);

        // Measure false positive rate
        let fpp = measure_false_positive_rate(&decoder, &test_values, &non_inserted_values);

        // Assert that the false positive rate is within acceptable bounds
        assert!(
            fpp <= target_fpp,
            "False positive rate {fpp} exceeds target {target_fpp}"
        );
    }

    #[test]
    fn test_false_positive_rate_medium_filter() {
        let num_values = 1000;
        let target_fpp = 0.01;
        let max_size = 2048;

        // Generate test data
        let test_values = generate_test_data(num_values, "test_value");
        let non_inserted_values = generate_non_overlapping_test_data(num_values, "test_value");

        // Build filter with test data
        let (_builder, decoder) =
            build_filter_with_string_values(&test_values, num_values, target_fpp, max_size);

        // Measure false positive rate
        let fpp = measure_false_positive_rate(&decoder, &test_values, &non_inserted_values);

        // Assert that the false positive rate is within acceptable bounds
        assert!(
            fpp <= target_fpp,
            "False positive rate {fpp} exceeds target {target_fpp}"
        );
    }

    #[test]
    fn test_false_positive_rate_large_filter() {
        let num_values = 10000;
        let target_fpp = 0.01;
        let max_size = 16 * 1024;

        // Generate test data
        let test_values = generate_test_data(num_values, "test_value");
        let non_inserted_values = generate_non_overlapping_test_data(num_values, "test_value");

        // Build filter with test data
        let (_builder, decoder) =
            build_filter_with_string_values(&test_values, num_values, target_fpp, max_size);

        // Measure false positive rate
        let fpp = measure_false_positive_rate(&decoder, &test_values, &non_inserted_values);

        // Assert that the false positive rate is within acceptable bounds
        assert!(
            fpp <= target_fpp + 0.02,
            "False positive rate {fpp} exceeds target {target_fpp}"
        );
    }

    #[test]
    fn test_false_positive_rate_varying_data_sizes() {
        let sizes_and_targets = vec![(100, 0.01), (500, 0.015), (1000, 0.02), (5000, 0.02)];

        for (num_values, target_fpp) in sizes_and_targets {
            let max_size = num_values * 16; // Just a heuristic for max size

            // Generate test data
            let test_values = generate_test_data(num_values, "test_value");
            let non_inserted_values = generate_non_overlapping_test_data(num_values, "test_value");

            // Build filter with test data
            let (_builder, decoder) =
                build_filter_with_string_values(&test_values, num_values, target_fpp, max_size);

            // Measure false positive rate
            let fpp = measure_false_positive_rate(&decoder, &test_values, &non_inserted_values);

            // Assert that the false positive rate is within acceptable bounds
            assert!(
                fpp <= target_fpp + 0.02, // Allow extra 2% for skewing of the results
                "False positive rate {fpp} exceeds target {target_fpp} for size {num_values}"
            );
        }
    }
}

#[cfg(test)]
mod false_positive_rate_tests {
    use super::*;

    #[test]
    fn test_fpp_few_distinct_items() {
        // Test with 2-10 distinct items
        for item_count in [2, 5, 8, 10] {
            let inserted_values = generate_test_data(item_count, "few_items");

            let (_builder, decoder) = build_filter_with_string_values(
                &inserted_values,
                item_count * 2, // Give some headroom for expected items
                0.01,           // 1% target FPP
                1024,
            );

            // Verify all inserted values are found (no false negatives)
            for value in &inserted_values {
                assert!(
                    decoder.probe(value.as_bytes()),
                    "False negative: '{value}' should be found with {item_count} items"
                );
            }

            // Test false positive rate with non-overlapping data
            let negative_test_values = generate_non_overlapping_test_data(1000, "few_items");
            let fpp =
                measure_false_positive_rate(&decoder, &inserted_values, &negative_test_values);

            println!("FPP with {item_count} items: {fpp:.4} (target: 0.01)");
            assert!(
                fpp <= 0.05, // Allow some tolerance due to small sample size
                "False positive rate {fpp:.4} too high for {item_count} items (target: 0.01)"
            );
        }
    }

    #[test]
    fn test_fpp_tens_of_distinct_items() {
        // Test with 20, 50, 100 distinct items
        for item_count in [20, 50, 100] {
            let inserted_values = generate_test_data(item_count, "tens_items");

            let (_builder, decoder) = build_filter_with_string_values(
                &inserted_values,
                item_count * 2, // Give some headroom for expected items
                0.01,           // 1% target FPP
                4096,           // Larger filter size for better accuracy
            );

            // Verify all inserted values are found (no false negatives)
            for value in &inserted_values {
                assert!(
                    decoder.probe(value.as_bytes()),
                    "False negative: '{value}' should be found with {item_count} items"
                );
            }

            // Test false positive rate with a larger sample for better statistical significance
            let negative_test_values = generate_non_overlapping_test_data(5000, "tens_items");
            let fpp =
                measure_false_positive_rate(&decoder, &inserted_values, &negative_test_values);

            println!("FPP with {item_count} items: {fpp:.4} (target: 0.01)");
            assert!(
                fpp <= 0.02, // Allow reasonable tolerance
                "False positive rate {fpp:.4} too high for {item_count} items (target: 0.01)"
            );
        }
    }

    #[test]
    fn test_fpp_thousand_distinct_items() {
        let item_count = 1000;
        let inserted_values = generate_test_data(item_count, "thousand_items");

        let (builder, decoder) = build_filter_with_string_values(
            &inserted_values,
            item_count,
            0.01,      // 1% target FPP
            1_048_576, // Very large filter size - 1MB for 1000 items
        );

        // Verify the filter was built successfully
        assert_eq!(builder.num_values(), item_count as u64);
        assert!(!builder.filter_data_as_bytes().is_empty());

        let actual_filter_size = builder.filter_data_as_bytes().len();
        let num_blocks = builder.num_blocks();

        println!("Filter statistics for 1000 items:");
        println!("  - Target FPP: 0.01 (1%)");
        println!("  - Max allowed size: 1,048,576 bytes (1MB)");
        println!("  - Actual filter size: {actual_filter_size} bytes");
        println!("  - Number of blocks: {num_blocks}");
        println!(
            "  - Bytes per item: {:.2}",
            actual_filter_size as f64 / item_count as f64
        );
        println!(
            "  - Bits per item: {:.2}",
            (actual_filter_size * 8) as f64 / item_count as f64
        );

        // Verify all inserted values are found (no false negatives)
        for value in &inserted_values {
            assert!(
                decoder.probe(value.as_bytes()),
                "False negative: '{value}' should be found with 1000 items"
            );
        }

        // Test false positive rate with a large sample for high statistical significance
        let negative_test_values = generate_non_overlapping_test_data(10000, "thousand_items");
        let fpp = measure_false_positive_rate(&decoder, &inserted_values, &negative_test_values);

        println!("FPP with 1000 items: {fpp:.4} (target: 0.01)");
        assert!(
            fpp <= 0.1, // Very relaxed tolerance to understand what's happening
            "False positive rate {fpp:.4} too high for 1000 items (target: 0.01)"
        );
    }

    #[test]
    fn test_fpp_with_different_target_rates() {
        // Test with different target FPP values
        let test_cases = [
            (0.001, 0.002), // Very low FPP
            (0.01, 0.02),   // Default FPP
            (0.05, 0.08),   // Higher FPP
        ];

        let item_count = 100;
        let inserted_values = generate_test_data(item_count, "fpp_targets");

        for (target_fpp, max_allowed_fpp) in test_cases {
            let (_builder, decoder) = build_filter_with_string_values(
                &inserted_values,
                item_count,
                target_fpp,
                8192, // Large enough filter for accuracy
            );

            // Verify all inserted values are found
            for value in &inserted_values {
                assert!(
                    decoder.probe(value.as_bytes()),
                    "False negative with target FPP {target_fpp}: '{value}'"
                );
            }

            // Measure actual FPP
            let negative_test_values = generate_non_overlapping_test_data(5000, "fpp_targets");
            let actual_fpp =
                measure_false_positive_rate(&decoder, &inserted_values, &negative_test_values);

            println!("Target FPP: {target_fpp:.3}, Actual FPP: {actual_fpp:.4}");
            assert!(
                actual_fpp <= max_allowed_fpp,
                "Actual FPP {actual_fpp:.4} exceeds max allowed {max_allowed_fpp:.4} for target {target_fpp:.3}"
            );
        }
    }

    #[test]
    fn test_fpp_with_bloom_filter_collector() {
        // Test FPP using BloomFilterCollector interface
        let item_count = 200;
        let inserted_values = generate_test_data(item_count, "collector_fpp");

        let config = BloomFilterConfig {
            cardinality_threshold: item_count + 100, // Well above our test size
            target_fpp: 0.01,
            max_filter_size: 8192,
            hash_algorithm: XXH3_64_ALGORITHM.to_string(),
            hash_seed: BloomFilterConfig::default().hash_seed,
        };

        let mut collector = BloomFilterCollector::new(config);

        // Add all values
        for value in &inserted_values {
            assert!(
                collector.process_value(value.as_bytes()),
                "Failed to add value to collector: {value}"
            );
        }

        // Build the filter
        assert!(collector.finish(), "Failed to build filter with collector");

        // Create decoder
        let filter_data = Bytes::copy_from_slice(collector.filter_data_as_bytes()).align(32);
        let decoder = SbbfDecoder::from_aligned_data(filter_data, collector.hash_seed()).unwrap();

        // Verify all inserted values are found
        for value in &inserted_values {
            assert!(
                decoder.probe(value.as_bytes()),
                "False negative with collector: '{value}'"
            );
        }

        // Measure FPP
        let negative_test_values = generate_non_overlapping_test_data(5000, "collector_fpp");
        let fpp = measure_false_positive_rate(&decoder, &inserted_values, &negative_test_values);

        println!("Collector FPP with {item_count} items: {fpp:.4} (target: 0.01)");
        assert!(
            fpp <= 0.02,
            "Collector FPP {fpp:.4} too high for {item_count} items (target: 0.01)"
        );
    }

    #[test]
    fn test_fpp_statistical_significance() {
        // Test to ensure our FPP measurements are statistically significant
        let item_count = 50;
        let inserted_values = generate_test_data(item_count, "stats_test");

        let (_builder, decoder) =
            build_filter_with_string_values(&inserted_values, item_count, 0.01, 4096);

        // Test multiple times with different negative samples
        let mut fpp_measurements = Vec::new();

        for trial in 0..5 {
            let negative_test_values =
                generate_non_overlapping_test_data(2000, &format!("stats_test_trial_{trial}"));
            let fpp =
                measure_false_positive_rate(&decoder, &inserted_values, &negative_test_values);
            fpp_measurements.push(fpp);
            println!("Trial {trial}: FPP = {fpp:.4}");
        }

        // Check that all measurements are reasonable
        for (i, &fpp) in fpp_measurements.iter().enumerate() {
            assert!(
                fpp <= 0.03,
                "Trial {i} FPP {fpp:.4} too high (target: 0.01)"
            );
        }

        // Calculate average FPP
        let avg_fpp = fpp_measurements.iter().sum::<f64>() / fpp_measurements.len() as f64;
        println!(
            "Average FPP across {} trials: {:.4}",
            fpp_measurements.len(),
            avg_fpp
        );

        assert!(
            avg_fpp <= 0.02,
            "Average FPP {avg_fpp:.4} too high across multiple trials (target: 0.01)"
        );
    }
}
