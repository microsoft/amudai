//! Comprehensive integration tests for bloom filter functionality in shards.
//!
//! This module provides a complete test suite for bloom filter creation, storage,
//! and querying across different cardinality scenarios with multiple stripes.

use std::sync::Arc;

use amudai_bloom_filters::SbbfDecoder;
use amudai_encodings::block_encoder::BlockEncodingProfile;
use amudai_format::schema::DataType as AmudaiDataType;
use arrow_array::{RecordBatch, builder::StringBuilder};
use arrow_schema::{DataType, Field, Schema};

use crate::tests::shard_store::ShardStore;

// ============================================================================
// Test Configuration and Utilities
// ============================================================================

/// Test configuration for different cardinality scenarios
#[derive(Debug, Clone)]
pub struct CardinalityTestConfig {
    pub name: &'static str,
    pub distinct_values: usize,
    pub total_records: usize,
    pub records_per_stripe: usize,
    pub expected_bloom_filter: bool,
}

impl CardinalityTestConfig {
    /// Create a low cardinality configuration
    pub fn low_cardinality() -> Self {
        Self {
            name: "low_cardinality",
            distinct_values: 10,
            total_records: 1000,
            records_per_stripe: 300,
            expected_bloom_filter: true,
        }
    }

    /// Create a moderate cardinality configuration
    pub fn moderate_cardinality() -> Self {
        Self {
            name: "moderate_cardinality",
            distinct_values: 100,
            total_records: 2000,
            records_per_stripe: 500,
            expected_bloom_filter: true,
        }
    }

    /// Create a high cardinality configuration
    pub fn high_cardinality() -> Self {
        Self {
            name: "high_cardinality",
            distinct_values: 1000,
            total_records: 5000,
            records_per_stripe: 1000,
            expected_bloom_filter: true, // Still within 1024 threshold per stripe
        }
    }

    /// Create a very high cardinality configuration
    pub fn very_high_cardinality() -> Self {
        Self {
            name: "very_high_cardinality",
            distinct_values: 10000,
            total_records: 20000,
            records_per_stripe: 2000,
            expected_bloom_filter: false, // Exceeds 1024 distinct values per stripe threshold
        }
    }

    /// Create a configuration that definitely exceeds the bloom filter threshold
    pub fn exceeds_bloom_filter_threshold() -> Self {
        Self {
            name: "exceeds_bloom_filter_threshold",
            distinct_values: 2000,
            total_records: 4000,
            records_per_stripe: 2000, // Single stripe with 2000 distinct values > 1024
            expected_bloom_filter: false,
        }
    }

    /// Get all standard test configurations
    pub fn all_standard_configs() -> Vec<Self> {
        vec![
            Self::low_cardinality(),
            Self::moderate_cardinality(),
            Self::high_cardinality(),
            Self::very_high_cardinality(),
            Self::exceeds_bloom_filter_threshold(),
        ]
    }

    /// Create a single stripe configuration for edge case testing
    pub fn single_stripe() -> Self {
        Self {
            name: "single_stripe",
            distinct_values: 200,
            total_records: 1000,
            records_per_stripe: 2000, // Larger than total records
            expected_bloom_filter: true,
        }
    }

    /// Create a configuration with exact stripe boundaries
    pub fn exact_stripe_boundaries() -> Self {
        Self {
            name: "exact_stripe_boundaries",
            distinct_values: 500,
            total_records: 3000,
            records_per_stripe: 1000, // Exactly 3 stripes
            expected_bloom_filter: true,
        }
    }
}

/// Test setup helper that encapsulates common shard creation and field access patterns
pub struct BloomFilterTestSetup {
    pub shard_store: ShardStore,
    pub shard_url: String,
    pub config: CardinalityTestConfig,
    pub test_data: Vec<String>,
}

impl BloomFilterTestSetup {
    /// Create a new test setup with the given configuration
    pub fn new(config: CardinalityTestConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let shard_store = ShardStore::new();
        let test_data = generate_string_data(config.distinct_values, config.total_records);
        let schema = create_string_column_schema("test_column");

        let shard_url = create_multi_stripe_shard(
            &shard_store,
            schema,
            test_data.clone(),
            config.records_per_stripe,
        )?;

        Ok(Self {
            shard_store,
            shard_url,
            config,
            test_data,
        })
    }

    /// Get the opened shard for testing
    pub fn shard(&self) -> Result<crate::read::shard::Shard, Box<dyn std::error::Error>> {
        Ok(self.shard_store.open_shard(&self.shard_url))
    }

    /// Get the field data type for the test column
    pub fn field_data_type(&self) -> Result<AmudaiDataType, Box<dyn std::error::Error>> {
        let shard = self.shard()?;
        let shard_schema = shard.fetch_schema()?;
        let (_, field) = shard_schema
            .find_field("test_column")?
            .ok_or("test_column field should exist")?;
        Ok(field.data_type()?)
    }

    /// Execute a test on each stripe with the provided closure
    pub fn for_each_stripe<F>(&self, mut test_fn: F) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnMut(
            usize,
            &crate::read::stripe::Stripe,
            &AmudaiDataType,
        ) -> Result<(), Box<dyn std::error::Error>>,
    {
        let shard = self.shard()?;
        let field_data_type = self.field_data_type()?;

        for stripe_idx in 0..shard.stripe_count() {
            let stripe = shard.open_stripe(stripe_idx)?;
            test_fn(stripe_idx, &stripe, &field_data_type)?;
        }
        Ok(())
    }

    /// Verify basic shard properties match the configuration
    pub fn verify_shard_structure(&self) -> Result<(), Box<dyn std::error::Error>> {
        let shard = self.shard()?;
        let expected_stripes = self
            .config
            .total_records
            .div_ceil(self.config.records_per_stripe);

        assert_eq!(
            shard.stripe_count(),
            expected_stripes,
            "Unexpected stripe count for {}. Expected: {}, Actual: {}",
            self.config.name,
            expected_stripes,
            shard.stripe_count()
        );
        Ok(())
    }

    /// Get a known value that should exist in the test data
    pub fn known_value(&self) -> String {
        if self.config.distinct_values > 0 {
            format!("value_{:06}", 0)
        } else {
            String::new()
        }
    }

    /// Get a value that should not exist in the test data
    pub fn unknown_value(&self) -> String {
        format!("nonexistent_value_{}", self.config.distinct_values + 1000)
    }

    /// Get multiple known values for comprehensive testing
    pub fn known_values(&self) -> Vec<String> {
        (0..std::cmp::min(10, self.config.distinct_values))
            .map(|i| format!("value_{:06}", i))
            .collect()
    }

    /// Get multiple unknown values for comprehensive testing
    pub fn unknown_values(&self) -> Vec<String> {
        (0..10)
            .map(|i| format!("unknown_value_{}", self.config.distinct_values + 1000 + i))
            .collect()
    }

    /// Get the actual test data that was written to the shard
    pub fn actual_test_data(&self) -> &[String] {
        &self.test_data
    }

    /// Get a sample of actual values from the test data for verification
    pub fn actual_known_values(&self) -> Vec<String> {
        // Get unique values from the actual test data
        let mut unique_values: std::collections::HashSet<String> = std::collections::HashSet::new();
        for value in &self.test_data {
            unique_values.insert(value.clone());
            if unique_values.len() >= 10 {
                break;
            }
        }
        unique_values.into_iter().collect()
    }

    /// Verify that the test data matches expected patterns
    pub fn verify_test_data_integrity(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Verify we have the expected number of records
        assert_eq!(
            self.test_data.len(),
            self.config.total_records,
            "Test data length doesn't match configuration"
        );

        // Verify all values follow the expected pattern
        for value in &self.test_data {
            assert!(
                value.starts_with("value_"),
                "Test data value '{}' doesn't match expected pattern",
                value
            );
        }

        // Verify cardinality
        let unique_values: std::collections::HashSet<&String> = self.test_data.iter().collect();
        assert_eq!(
            unique_values.len(),
            self.config.distinct_values,
            "Test data cardinality doesn't match configuration"
        );

        Ok(())
    }

    /// Calculate the actual distinct values that would appear in each stripe
    pub fn analyze_stripe_cardinality(&self) -> Vec<usize> {
        let mut stripe_cardinalities = Vec::new();
        let records_per_stripe = self.config.records_per_stripe;
        let mut processed_records = 0;

        while processed_records < self.test_data.len() {
            let end_idx =
                std::cmp::min(processed_records + records_per_stripe, self.test_data.len());
            let stripe_data = &self.test_data[processed_records..end_idx];

            // Count unique values in this stripe
            let unique_values: std::collections::HashSet<&String> = stripe_data.iter().collect();
            stripe_cardinalities.push(unique_values.len());

            processed_records = end_idx;
        }

        stripe_cardinalities
    }

    /// Log detailed cardinality analysis for debugging
    pub fn log_cardinality_analysis(&self) {
        let stripe_cardinalities = self.analyze_stripe_cardinality();

        println!("Cardinality analysis for {}:", self.config.name);
        println!("  Total distinct values: {}", self.config.distinct_values);
        println!("  Total records: {}", self.config.total_records);
        println!("  Records per stripe: {}", self.config.records_per_stripe);
        println!("  Number of stripes: {}", stripe_cardinalities.len());

        for (stripe_idx, cardinality) in stripe_cardinalities.iter().enumerate() {
            println!(
                "  Stripe {} distinct values: {} (threshold: 1024)",
                stripe_idx, cardinality
            );
            if *cardinality > 1024 {
                println!("    ⚠️  Exceeds 1024 threshold - should NOT have bloom filter");
            } else {
                println!("    ✓  Within 1024 threshold - should have bloom filter");
            }
        }
    }
}

/// Log detailed bloom filter size information for debugging high false positive rates
fn log_bloom_filter_size_info(
    stripe: &crate::read::stripe::Stripe,
    field_data_type: &AmudaiDataType,
    stripe_idx: usize,
    config_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let field_ctx = stripe.open_field(field_data_type.clone())?;
    let field_descriptor = field_ctx
        .descriptor()
        .field
        .as_ref()
        .ok_or("Field descriptor should exist")?;

    // Get SSBF bytes to analyze size
    if let Some(ssbf_bytes) = field_descriptor.try_get_sbbf_bytes() {
        let ssbf_size_bytes = ssbf_bytes.len();
        let ssbf_size_kb = ssbf_size_bytes as f64 / 1024.0;

        println!(
            "  📊 Split-Block Bloom Filter size analysis for stripe {} of {}:",
            stripe_idx, config_name
        );
        println!(
            "     Size: {} bytes ({:.2} KB)",
            ssbf_size_bytes, ssbf_size_kb
        );

        // Try to get more detailed information from the SSBF decoder
        match SbbfDecoder::from_aligned_data(ssbf_bytes) {
            Ok(_decoder) => {
                // The decoder doesn't expose internal structure directly, but we can infer some info
                println!("     Successfully decoded SSBF structure");

                // Estimate density based on size and expected elements
                // This is a rough heuristic - smaller filters relative to data size tend to have higher false positive rates
                if ssbf_size_bytes < 1024 {
                    println!(
                        "     ⚠️  Small filter size may contribute to high false positive rate"
                    );
                } else if ssbf_size_bytes > 10240 {
                    println!(
                        "     ✓  Large filter size - high false positive rate may indicate other issues"
                    );
                } else {
                    println!("     ℹ️  Medium filter size");
                }
            }
            Err(e) => {
                println!("     ❌ Failed to decode SSBF: {}", e);
            }
        }

        // Log size categories for analysis
        match ssbf_size_bytes {
            0..=256 => println!("     Category: Very Small (≤256 bytes)"),
            257..=1024 => println!("     Category: Small (257-1024 bytes)"),
            1025..=4096 => println!("     Category: Medium (1-4 KB)"),
            4097..=16384 => println!("     Category: Large (4-16 KB)"),
            _ => println!("     Category: Very Large (>16 KB)"),
        }
    } else {
        println!(
            "  ❌ No SSBF bytes found for stripe {} of {}",
            stripe_idx, config_name
        );
    }

    Ok(())
}

// ============================================================================
// Data Generation Utilities
// ============================================================================

/// Generates test data with specified cardinality
pub fn generate_string_data(distinct_values: usize, total_records: usize) -> Vec<String> {
    let mut data = Vec::with_capacity(total_records);

    // Generate distinct values with consistent formatting
    let distinct_strings: Vec<String> = (0..distinct_values)
        .map(|i| format!("value_{i:06}"))
        .collect();

    // Fill records by cycling through distinct values
    for i in 0..total_records {
        let value_index = i % distinct_values;
        data.push(distinct_strings[value_index].clone());
    }

    data
}

/// Generate data with specific patterns for edge case testing
pub fn generate_edge_case_data() -> Vec<String> {
    vec![
        String::new(),                   // Empty string
        "a".to_string(),                 // Single character
        "a".repeat(1000),                // Very long string
        "special!@#$%^&*()".to_string(), // Special characters
        "unicode_μλφα".to_string(),      // Unicode characters
        " leading_space".to_string(),    // Leading space
        "trailing_space ".to_string(),   // Trailing space
        "\t\n\r".to_string(),            // Whitespace characters
    ]
}

/// Generates test GUID data with specified cardinality
pub fn generate_guid_data(distinct_values: usize, total_records: usize) -> Vec<[u8; 16]> {
    let mut data = Vec::with_capacity(total_records);

    // Generate distinct GUID values with predictable patterns for testing
    let distinct_guids: Vec<[u8; 16]> = (0..distinct_values)
        .map(|i| {
            let mut guid = [0u8; 16];
            // Use a simple pattern: first 4 bytes contain the index, rest are zero
            guid[0..4].copy_from_slice(&(i as u32).to_le_bytes());
            // Add some variation in the middle bytes for better bloom filter distribution
            guid[8..12].copy_from_slice(&((i * 7) as u32).to_le_bytes());
            guid
        })
        .collect();

    // Fill records by cycling through distinct GUIDs
    for i in 0..total_records {
        let guid_index = i % distinct_values;
        data.push(distinct_guids[guid_index]);
    }

    data
}

/// Generates test binary data with specified cardinality
pub fn generate_binary_data(distinct_values: usize, total_records: usize) -> Vec<Vec<u8>> {
    let mut data = Vec::with_capacity(total_records);

    // Generate distinct binary values with varying lengths for testing
    let distinct_binaries: Vec<Vec<u8>> = (0..distinct_values)
        .map(|i| {
            let mut binary = Vec::new();
            // Create binary data with variable length (4-20 bytes)
            let length = 4 + (i % 17); // Length between 4 and 20
            for j in 0..length {
                binary.push(((i * 13 + j * 7) % 256) as u8);
            }
            binary
        })
        .collect();

    // Fill records by cycling through distinct binary values
    for i in 0..total_records {
        let binary_index = i % distinct_values;
        data.push(distinct_binaries[binary_index].clone());
    }

    data
}

/// Creates a simple Arrow schema with a single string column
pub fn create_string_column_schema(column_name: &str) -> Schema {
    Schema::new(vec![Field::new(column_name, DataType::Utf8, false)])
}

/// Creates a RecordBatch from string data
pub fn create_string_record_batch(
    schema: &Arc<Schema>,
    data: &[String],
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let mut builder = StringBuilder::new();
    for value in data {
        builder.append_value(value);
    }

    let array = builder.finish();
    Ok(RecordBatch::try_new(schema.clone(), vec![Arc::new(array)])?)
}

/// Creates a simple Arrow schema with a single GUID column
pub fn create_guid_column_schema(column_name: &str) -> Schema {
    Schema::new(vec![Field::new(
        column_name,
        DataType::FixedSizeBinary(16),
        false,
    )])
}

/// Creates a simple Arrow schema with a single binary column
pub fn create_binary_column_schema(column_name: &str) -> Schema {
    Schema::new(vec![Field::new(column_name, DataType::Binary, false)])
}

/// Creates a RecordBatch from GUID data
pub fn create_guid_record_batch(
    schema: &Arc<Schema>,
    data: &[[u8; 16]],
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let values: Vec<&[u8]> = data.iter().map(|guid| guid.as_slice()).collect();
    let array = arrow_array::FixedSizeBinaryArray::from(values);
    Ok(RecordBatch::try_new(schema.clone(), vec![Arc::new(array)])?)
}

/// Creates a RecordBatch from binary data
pub fn create_binary_record_batch(
    schema: &Arc<Schema>,
    data: &[Vec<u8>],
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let values: Vec<&[u8]> = data.iter().map(|bin| bin.as_slice()).collect();
    let array = arrow_array::BinaryArray::from_vec(values);
    Ok(RecordBatch::try_new(schema.clone(), vec![Arc::new(array)])?)
}

/// Creates a shard with multiple stripes for testing
pub fn create_multi_stripe_shard(
    shard_store: &ShardStore,
    schema: Schema,
    data: Vec<String>,
    records_per_stripe: usize,
) -> Result<String, Box<dyn std::error::Error>> {
    let arc_schema = Arc::new(schema);
    let mut shard_builder = shard_store.create_shard_builder(&arc_schema);

    // Create multiple stripes based on records_per_stripe
    let mut processed_records = 0;
    while processed_records < data.len() {
        let end_idx = std::cmp::min(processed_records + records_per_stripe, data.len());
        let stripe_data = &data[processed_records..end_idx];

        let batch = create_string_record_batch(&arc_schema, stripe_data)?;

        let mut stripe_builder = shard_builder.build_stripe()?;
        stripe_builder.push_batch(&batch)?;

        let stripe = stripe_builder.finish()?;
        shard_builder.add_stripe(stripe)?;

        processed_records = end_idx;
    }

    let shard_ref = shard_store.seal_shard_builder(shard_builder);
    Ok(shard_ref.url)
}

/// Creates a shard with GUID data for testing
pub fn create_guid_shard(
    shard_store: &ShardStore,
    schema: Schema,
    data: Vec<[u8; 16]>,
    records_per_stripe: usize,
) -> Result<String, Box<dyn std::error::Error>> {
    let arc_schema = Arc::new(schema);
    let mut shard_builder = shard_store.create_shard_builder(&arc_schema);

    // Create multiple stripes based on records_per_stripe
    let mut processed_records = 0;
    while processed_records < data.len() {
        let end_idx = std::cmp::min(processed_records + records_per_stripe, data.len());
        let stripe_data = &data[processed_records..end_idx];

        let batch = create_guid_record_batch(&arc_schema, stripe_data)?;

        let mut stripe_builder = shard_builder.build_stripe()?;
        stripe_builder.push_batch(&batch)?;

        let stripe = stripe_builder.finish()?;
        shard_builder.add_stripe(stripe)?;

        processed_records = end_idx;
    }

    let shard_ref = shard_store.seal_shard_builder(shard_builder);
    Ok(shard_ref.url)
}

/// Creates a shard with binary data for testing
pub fn create_binary_shard(
    shard_store: &ShardStore,
    schema: Schema,
    data: Vec<Vec<u8>>,
    records_per_stripe: usize,
) -> Result<String, Box<dyn std::error::Error>> {
    let arc_schema = Arc::new(schema);
    let mut shard_builder = shard_store.create_shard_builder(&arc_schema);

    // Create multiple stripes based on records_per_stripe
    let mut processed_records = 0;
    while processed_records < data.len() {
        let end_idx = std::cmp::min(processed_records + records_per_stripe, data.len());
        let stripe_data = &data[processed_records..end_idx];

        let batch = create_binary_record_batch(&arc_schema, stripe_data)?;

        let mut stripe_builder = shard_builder.build_stripe()?;
        stripe_builder.push_batch(&batch)?;

        let stripe = stripe_builder.finish()?;
        shard_builder.add_stripe(stripe)?;

        processed_records = end_idx;
    }

    let shard_ref = shard_store.seal_shard_builder(shard_builder);
    Ok(shard_ref.url)
}

// ============================================================================
// Main Test Functions
// ============================================================================

/// Test bloom filter creation and basic functionality across different cardinality scenarios
#[test]
fn test_bloom_filter_cardinality_scenarios() {
    let test_configs = CardinalityTestConfig::all_standard_configs();

    for config in test_configs {
        println!("\n=== Testing cardinality scenario: {} ===", config.name);

        let setup = BloomFilterTestSetup::new(config.clone()).expect("Failed to create test setup");

        // Log cardinality analysis for debugging
        setup.log_cardinality_analysis();

        // Verify test data integrity first
        setup
            .verify_test_data_integrity()
            .expect("Failed to verify test data integrity");

        // Verify shard structure
        setup
            .verify_shard_structure()
            .expect("Failed to verify shard structure");

        // Test bloom filter presence and basic functionality
        test_bloom_filter_presence_and_probing(&setup).expect("Failed bloom filter presence test");

        println!("✅ {} completed successfully\n", config.name);
    }
}

/// Test bloom filter presence and basic probing functionality
fn test_bloom_filter_presence_and_probing(
    setup: &BloomFilterTestSetup,
) -> Result<(), Box<dyn std::error::Error>> {
    setup.for_each_stripe(|stripe_idx, stripe, field_data_type| {
        let field_ctx = stripe.open_field(field_data_type.clone())?;
        let bloom_filter = field_ctx.get_bloom_filter();

        if setup.config.expected_bloom_filter {
            // We expect a bloom filter to be present
            let filter = bloom_filter.ok_or_else(|| {
                format!(
                    "Expected bloom filter for {} in stripe {} but none found. This configuration should have bloom filters.",
                    setup.config.name,
                    stripe_idx
                )
            })?;

            println!("✓ Found expected bloom filter in stripe {} of {}", stripe_idx, setup.config.name);

            // Test with actual known values from the test data (should return true)
            for known_value in setup.actual_known_values() {
                let probe_result = filter.probe(known_value.as_bytes());
                assert!(
                    probe_result,
                    "Bloom filter should return true for actual test value '{}' in stripe {} of {}",
                    known_value,
                    stripe_idx,
                    setup.config.name
                );
            }

            // Also test with traditional known values as a fallback
            for known_value in setup.known_values() {
                let probe_result = filter.probe(known_value.as_bytes());
                assert!(
                    probe_result,
                    "Bloom filter should return true for known value '{}' in stripe {} of {}",
                    known_value,
                    stripe_idx,
                    setup.config.name
                );
            }

            // Test with unknown values and measure false positive rate
            let unknown_values = setup.unknown_values();
            let mut false_positives = 0;
            for unknown_value in &unknown_values {
                if filter.probe(unknown_value.as_bytes()) {
                    false_positives += 1;
                }
            }

            let false_positive_rate = false_positives as f64 / unknown_values.len() as f64;
            // For proper statistical significance, test with more unknown values
            let large_unknown_test: Vec<String> = (0..1000)
                .map(|i| format!("large_unknown_test_{}_{}", stripe_idx, i + 100000))
                .collect();

            let mut large_false_positives = 0;
            for unknown_value in &large_unknown_test {
                if filter.probe(unknown_value.as_bytes()) {
                    large_false_positives += 1;
                }
            }

            let large_false_positive_rate = large_false_positives as f64 / large_unknown_test.len() as f64;
            println!(
                "Stripe {} FPP: {:.3}% ({}/{}) | Large sample FPP: {:.3}% ({}/{})",
                stripe_idx,
                false_positive_rate * 100.0, false_positives, unknown_values.len(),
                large_false_positive_rate * 100.0, large_false_positives, large_unknown_test.len()
            );

            // Assert that false positive rate is close to 1% target (allowing reasonable tolerance)
            assert!(
                large_false_positive_rate <= 0.05, // 5% maximum tolerance
                "False positive rate {:.3}% in stripe {} of {} exceeds 5% maximum (target: 1%)",
                large_false_positive_rate * 100.0,
                stripe_idx,
                setup.config.name
            );

            // For configurations with sufficient distinct values, expect closer to 1%
            if setup.config.distinct_values >= 100 {
                assert!(
                    large_false_positive_rate <= 0.03, // 3% maximum for moderate/high cardinality
                    "False positive rate {:.3}% in stripe {} of {} exceeds 3% for sufficient cardinality (target: 1%)",
                    large_false_positive_rate * 100.0,
                    stripe_idx,
                    setup.config.name
                );
            }

            // Log warning for rates significantly above 1%
            if large_false_positive_rate > 0.02 {
                println!(
                    "WARNING: FPP {:.3}% in stripe {} of {} is above 2% (target: 1%)",
                    large_false_positive_rate * 100.0,
                    stripe_idx,
                    setup.config.name
                );

                // Log bloom filter size information for debugging
                log_bloom_filter_size_info(stripe, field_data_type, stripe_idx, &setup.config.name)?;
            }
        } else {
            // We expect NO bloom filter to be present (high cardinality case)
            if bloom_filter.is_some() {
                return Err(format!(
                    "UNEXPECTED: Found bloom filter in stripe {} of {} but none was expected. High cardinality data (>1024 distinct values per stripe) should not have bloom filters.",
                    stripe_idx,
                    setup.config.name
                ).into());
            }

            println!("✓ Correctly found no bloom filter in stripe {} of {} (high cardinality scenario)", 
                    stripe_idx, setup.config.name);
        }

        Ok(())
    })
}

/// Test bloom filter with single stripe configuration
#[test]
fn test_bloom_filter_single_stripe() -> Result<(), Box<dyn std::error::Error>> {
    let config = CardinalityTestConfig::single_stripe();
    let setup = BloomFilterTestSetup::new(config)?;

    // Verify test data integrity
    setup.verify_test_data_integrity()?;

    // Verify we have exactly one stripe
    let shard = setup.shard()?;
    assert_eq!(
        shard.stripe_count(),
        1,
        "Should have exactly 1 stripe for single stripe test"
    );

    // Test bloom filter functionality
    setup.for_each_stripe(|stripe_idx, stripe, field_data_type| {
        let field_ctx = stripe.open_field(field_data_type.clone())?;
        let bloom_filter = field_ctx.get_bloom_filter();

        assert!(
            bloom_filter.is_some(),
            "Single stripe should have bloom filter in stripe {}",
            stripe_idx
        );

        // Test with actual known values from test data
        let filter = bloom_filter.unwrap();
        for actual_value in setup.actual_known_values() {
            let probe_result = filter.probe(actual_value.as_bytes());
            assert!(
                probe_result,
                "Single stripe bloom filter should return true for actual test value '{}'",
                actual_value
            );
        }

        // Also test with synthetic known value as backup
        let known_value = setup.known_value();
        let probe_result = filter.probe(known_value.as_bytes());
        assert!(
            probe_result,
            "Single stripe bloom filter should return true for known value"
        );

        Ok(())
    })?;

    Ok(())
}

/// Test bloom filter behavior with exact stripe boundaries
#[test]
fn test_bloom_filter_exact_stripe_boundaries() -> Result<(), Box<dyn std::error::Error>> {
    let config = CardinalityTestConfig::exact_stripe_boundaries();
    let setup = BloomFilterTestSetup::new(config)?;

    // Verify test data integrity
    setup.verify_test_data_integrity()?;

    let shard = setup.shard()?;
    assert_eq!(shard.stripe_count(), 3, "Should have exactly 3 stripes");

    // Test that each stripe has its own bloom filter with correct behavior
    setup.for_each_stripe(|stripe_idx, stripe, field_data_type| {
        let field_ctx = stripe.open_field(field_data_type.clone())?;
        let bloom_filter = field_ctx.get_bloom_filter();

        assert!(
            bloom_filter.is_some(),
            "Stripe {} should have bloom filter",
            stripe_idx
        );

        // Each stripe should contain some of our actual test values
        let filter = bloom_filter.unwrap();
        let mut found_actual_value = false;

        // Check with actual test data values
        for actual_value in setup.actual_known_values() {
            if filter.probe(actual_value.as_bytes()) {
                found_actual_value = true;
                break;
            }
        }

        // Also check with synthetic known values as backup
        let mut found_known_value = false;
        for known_value in setup.known_values() {
            if filter.probe(known_value.as_bytes()) {
                found_known_value = true;
                break;
            }
        }

        // At least one value should be found in each stripe
        // (since we cycle through values when creating data)
        assert!(
            found_actual_value || found_known_value,
            "Stripe {} should contain at least one known value (actual or synthetic)",
            stripe_idx
        );

        Ok(())
    })?;

    Ok(())
}

/// Test bloom filter field extension utilities
#[test]
fn test_bloom_filter_field_extensions() -> Result<(), Box<dyn std::error::Error>> {
    let config = CardinalityTestConfig::moderate_cardinality();
    let setup = BloomFilterTestSetup::new(config)?;

    setup.for_each_stripe(|stripe_idx, stripe, field_data_type| {
        let field_ctx = stripe.open_field(field_data_type.clone())?;
        let field_descriptor = field_ctx
            .descriptor()
            .field
            .as_ref()
            .ok_or("Field descriptor should exist")?;

        // Test SSBF bytes access
        let ssbf_bytes = field_descriptor.try_get_sbbf_bytes();
        assert!(
            ssbf_bytes.is_some(),
            "Field should have SSBF bytes in stripe {}",
            stripe_idx
        );

        // Test SSBF decoder functionality
        let ssbf_decoder = SbbfDecoder::from_aligned_data(ssbf_bytes.unwrap())?;

        // Test with a value that definitely exists
        let known_value = setup.known_value();
        let probe_result = ssbf_decoder.probe(known_value.as_bytes());
        assert!(
            probe_result,
            "SSBF decoder should return true for known value in stripe {}",
            stripe_idx
        );

        // Test with unknown value (may be false positive, but shouldn't crash)
        let unknown_value = setup.unknown_value();
        let _unknown_result = ssbf_decoder.probe(&unknown_value.as_bytes());
        // Don't assert false since bloom filters can have false positives

        Ok(())
    })?;

    Ok(())
}

/// Test bloom filter with edge case data patterns
#[test]
fn test_bloom_filter_edge_case_data() -> Result<(), Box<dyn std::error::Error>> {
    let shard_store = ShardStore::new();
    let edge_case_data = generate_edge_case_data();
    let schema = create_string_column_schema("test_column");

    let shard_url = create_multi_stripe_shard(
        &shard_store,
        schema,
        edge_case_data.clone(),
        4, // Small stripe size to create multiple stripes
    )?;

    let shard = shard_store.open_shard(&shard_url);
    let shard_schema = shard.fetch_schema()?;
    let (_, field) = shard_schema
        .find_field("test_column")?
        .ok_or("test_column field should exist")?;
    let field_data_type = field.data_type()?;

    // Test each stripe
    for stripe_idx in 0..shard.stripe_count() {
        let stripe = shard.open_stripe(stripe_idx)?;
        let field_ctx = stripe.open_field(field_data_type.clone())?;
        let bloom_filter = field_ctx.get_bloom_filter();

        assert!(
            bloom_filter.is_some(),
            "Edge case data should have bloom filter in stripe {}",
            stripe_idx
        );

        let filter = bloom_filter.unwrap();

        // Test with the original edge case values
        for value in &edge_case_data {
            let probe_result = filter.probe(value.as_bytes());
            // We don't assert true here since the value might not be in this specific stripe
            // but we verify the probe doesn't crash with edge case data
            let _ = probe_result;
        }
    }

    Ok(())
}

/// Test bloom filter memory and performance characteristics
#[test]
fn test_bloom_filter_performance_characteristics() -> Result<(), Box<dyn std::error::Error>> {
    let config = CardinalityTestConfig::high_cardinality();
    let setup = BloomFilterTestSetup::new(config)?;

    // Measure basic performance characteristics
    let start_time = std::time::Instant::now();

    let mut total_probes = 0;
    setup.for_each_stripe(|_stripe_idx, stripe, field_data_type| {
        let field_ctx = stripe.open_field(field_data_type.clone())?;
        let bloom_filter = field_ctx.get_bloom_filter();

        if let Some(filter) = bloom_filter {
            // Perform multiple probes to test performance
            for i in 0..100 {
                let test_value = format!("perf_test_value_{}", i);
                let _result = filter.probe(test_value.as_bytes());
                total_probes += 1;
            }
        }

        Ok(())
    })?;

    let elapsed = start_time.elapsed();
    println!(
        "Performed {} probes in {:?} ({:.2} probes/ms)",
        total_probes,
        elapsed,
        total_probes as f64 / elapsed.as_millis() as f64
    );

    // Basic performance assertion - should be able to do at least 1000 probes per second
    assert!(
        total_probes as f64 / elapsed.as_secs_f64() > 1000.0,
        "Bloom filter performance should be at least 1000 probes/second"
    );

    Ok(())
}

/// Test false positive rate characteristics
#[test]
fn test_bloom_filter_false_positive_rate() -> Result<(), Box<dyn std::error::Error>> {
    let config = CardinalityTestConfig::moderate_cardinality();
    let setup = BloomFilterTestSetup::new(config)?;

    let mut total_false_positives = 0;
    let mut total_tests = 0;

    setup.for_each_stripe(|stripe_idx, stripe, field_data_type| {
        let field_ctx = stripe.open_field(field_data_type.clone())?;
        let bloom_filter = field_ctx.get_bloom_filter();

        if let Some(filter) = bloom_filter {
            // Test with more values to get a statistically significant sample
            let test_values: Vec<String> = (0..200)
                .map(|i| format!("definitely_nonexistent_value_{}_stripe_{}", i, stripe_idx))
                .collect();

            let mut false_positives = 0;
            for value in &test_values {
                if filter.probe(value.as_bytes()) {
                    false_positives += 1;
                }
            }

            let false_positive_rate = false_positives as f64 / test_values.len() as f64;

            println!(
                "Stripe {} false positive rate: {:.3}% ({}/{})",
                stripe_idx,
                false_positive_rate * 100.0,
                false_positives,
                test_values.len()
            );

            // Assert that false positive rate is close to 1% target
            assert!(
                false_positive_rate <= 0.05, // 5% maximum tolerance
                "False positive rate {:.3}% too high in stripe {}: ({}/{}). Target is 1%, maximum allowed is 5%.",
                false_positive_rate * 100.0,
                stripe_idx,
                false_positives,
                test_values.len()
            );

            // For moderate cardinality, expect even better performance
            if setup.config.distinct_values >= 100 {
                assert!(
                    false_positive_rate <= 0.03, // 3% for sufficient cardinality
                    "False positive rate {:.3}% too high in stripe {} for moderate cardinality: ({}/{}). With sufficient data, target is 1%, maximum allowed is 3%.",
                    false_positive_rate * 100.0,
                    stripe_idx,
                    false_positives,
                    test_values.len()
                );
            }

            // Log bloom filter size info for rates above 2%
            if false_positive_rate > 0.02 {
                println!("  WARNING: FPP above 2% (target: 1%), analyzing bloom filter size:");
                log_bloom_filter_size_info(stripe, field_data_type, stripe_idx, "false_positive_rate_test")?;
            }

            total_false_positives += false_positives;
            total_tests += test_values.len();
        }

        Ok(())
    })?;

    // Overall false positive rate across all stripes should also meet 1% target
    if total_tests > 0 {
        let overall_false_positive_rate = total_false_positives as f64 / total_tests as f64;
        println!(
            "Overall false positive rate: {:.3}% ({}/{})",
            overall_false_positive_rate * 100.0,
            total_false_positives,
            total_tests
        );

        assert!(
            overall_false_positive_rate <= 0.05, // 5% maximum tolerance
            "Overall false positive rate {:.3}% exceeds 5% maximum (target: 1%): ({}/{})",
            overall_false_positive_rate * 100.0,
            total_false_positives,
            total_tests
        );

        // For sufficient test samples, expect closer to 1%
        if total_tests >= 500 {
            assert!(
                overall_false_positive_rate <= 0.03, // 3% for large samples
                "Overall false positive rate {:.3}% exceeds 3% for large sample (target: 1%): ({}/{})",
                overall_false_positive_rate * 100.0,
                total_false_positives,
                total_tests
            );
        }

        // Warn if significantly above target
        if overall_false_positive_rate > 0.02 {
            println!(
                "WARNING: Overall FPP {:.3}% is above 2% (target: 1%)",
                overall_false_positive_rate * 100.0
            );
        }
    }

    Ok(())
}

/// Test bloom filter accuracy: count misses and ensure they meet minimum requirements
#[test]
fn test_bloom_filter_miss_rate_accuracy() -> Result<(), Box<dyn std::error::Error>> {
    let config = CardinalityTestConfig::low_cardinality(); // Use low cardinality for more predictable results
    let setup = BloomFilterTestSetup::new(config)?;

    setup.verify_test_data_integrity()?;

    setup.for_each_stripe(|stripe_idx, stripe, field_data_type| {
        let field_ctx = stripe.open_field(field_data_type.clone())?;
        let bloom_filter = field_ctx.get_bloom_filter();

        if let Some(filter) = bloom_filter {
            // Test 1: All actual values should be hits (no false negatives allowed)
            let actual_values = setup.actual_known_values();
            let mut false_negatives = 0;

            for actual_value in &actual_values {
                if !filter.probe(actual_value.as_bytes()) {
                    false_negatives += 1;
                    println!("FALSE NEGATIVE: '{}' not found in stripe {}", actual_value, stripe_idx);
                }
            }

            assert_eq!(
                false_negatives, 0,
                "Bloom filter has {} false negatives in stripe {} - this should never happen!",
                false_negatives, stripe_idx
            );

            // Test 2: Count true negatives (misses) for unknown values
            let unknown_test_values: Vec<String> = (0..150)
                .map(|i| format!("absolutely_unknown_value_{}_{}", stripe_idx, i + 50000))
                .collect();

            let mut true_negatives = 0; // Correctly identified as not present
            let mut false_positives = 0; // Incorrectly identified as present

            for unknown_value in &unknown_test_values {
                if filter.probe(unknown_value.as_bytes()) {
                    false_positives += 1;
                } else {
                    true_negatives += 1;
                }
            }

            let miss_rate = true_negatives as f64 / unknown_test_values.len() as f64;
            let false_positive_rate = false_positives as f64 / unknown_test_values.len() as f64;

            println!(
                "Stripe {}: Miss rate: {:.3}% ({}/{}), False positive rate: {:.3}% ({}/{})",
                stripe_idx,
                miss_rate * 100.0, true_negatives, unknown_test_values.len(),
                false_positive_rate * 100.0, false_positives, unknown_test_values.len()
            );

            // Assert minimum miss rate - bloom filter should correctly reject most unknown values
            assert!(
                miss_rate >= 0.15, // At least 15% should be correctly identified as misses
                "Miss rate too low in stripe {}: {:.3}%. Bloom filter should correctly reject more unknown values.",
                stripe_idx,
                miss_rate * 100.0
            );

            // Assert false positive rate is close to 1% target
            assert!(
                false_positive_rate <= 0.05, // 5% maximum tolerance
                "False positive rate {:.3}% too high in stripe {} (target: 1%, max: 5%)",
                false_positive_rate * 100.0,
                stripe_idx
            );

            // For low cardinality with sufficient data, expect even better performance
            assert!(
                false_positive_rate <= 0.03, // 3% for low cardinality
                "False positive rate {:.3}% too high in stripe {} for low cardinality (target: 1%, max: 3%)",
                false_positive_rate * 100.0,
                stripe_idx
            );

            // Warn if significantly above 1% target
            if false_positive_rate > 0.02 {
                println!(
                    "WARNING: FPP {:.3}% in stripe {} above 2% (target: 1%)",
                    false_positive_rate * 100.0,
                    stripe_idx
                );
            }

            // Sanity check: miss rate + false positive rate should equal 100%
            let total_rate = miss_rate + false_positive_rate;
            assert!(
                (total_rate - 1.0).abs() < 0.001,
                "Miss rate + false positive rate should equal 100%, got {:.3}%",
                total_rate * 100.0
            );
        }

        Ok(())
    })?;

    Ok(())
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    /// Integration test that combines multiple scenarios
    #[test]
    fn test_comprehensive_bloom_filter_integration() -> Result<(), Box<dyn std::error::Error>> {
        // Test all configurations in sequence
        let all_configs = vec![
            CardinalityTestConfig::low_cardinality(),
            CardinalityTestConfig::moderate_cardinality(),
            CardinalityTestConfig::single_stripe(),
            CardinalityTestConfig::exact_stripe_boundaries(),
        ];

        for config in all_configs {
            println!("Integration testing: {}", config.name);

            let setup = BloomFilterTestSetup::new(config)?;

            // Comprehensive verification
            setup.verify_shard_structure()?;
            test_bloom_filter_presence_and_probing(&setup)?;

            println!("✓ {} passed integration test", setup.config.name);
        }

        Ok(())
    }

    /// Test that demonstrates the value of using actual test data vs synthetic data
    #[test]
    fn test_actual_vs_synthetic_data_validation() -> Result<(), Box<dyn std::error::Error>> {
        let config = CardinalityTestConfig::moderate_cardinality();
        let setup = BloomFilterTestSetup::new(config)?;

        // Verify test data integrity first
        setup.verify_test_data_integrity()?;

        println!(
            "Test data contains {} total records with {} distinct values",
            setup.actual_test_data().len(),
            setup.config.distinct_values
        );

        // Demonstrate actual test data usage
        let actual_values = setup.actual_known_values();
        let synthetic_values = setup.known_values();

        println!(
            "Actual test values (first 5): {:?}",
            actual_values.iter().take(5).collect::<Vec<_>>()
        );
        println!(
            "Synthetic test values (first 5): {:?}",
            synthetic_values.iter().take(5).collect::<Vec<_>>()
        );

        // Test that both actual and synthetic values work with bloom filters
        setup.for_each_stripe(|stripe_idx, stripe, field_data_type| {
            let field_ctx = stripe.open_field(field_data_type.clone())?;
            if let Some(filter) = field_ctx.get_bloom_filter() {
                println!(
                    "Testing stripe {} with both actual and synthetic values",
                    stripe_idx
                );

                // Test with actual values - these MUST be in the bloom filter
                let mut actual_matches = 0;
                for actual_value in &actual_values {
                    if filter.probe(actual_value.as_bytes()) {
                        actual_matches += 1;
                    }
                }

                // Test with synthetic values - these should also be in the bloom filter
                // since they follow the same pattern as the actual data
                let mut synthetic_matches = 0;
                for synthetic_value in &synthetic_values {
                    if filter.probe(synthetic_value.as_bytes()) {
                        synthetic_matches += 1;
                    }
                }

                println!(
                    "Stripe {}: Actual value matches: {}/{}, Synthetic value matches: {}/{}",
                    stripe_idx,
                    actual_matches,
                    actual_values.len(),
                    synthetic_matches,
                    synthetic_values.len()
                );

                // Both should have at least some matches since the data follows the same pattern
                assert!(
                    actual_matches > 0 || synthetic_matches > 0,
                    "Stripe {} should match at least some actual or synthetic values",
                    stripe_idx
                );
            }

            Ok(())
        })?;

        Ok(())
    }

    /// Test bloom filter threshold behavior - filters should not be created for high cardinality
    #[test]
    fn test_bloom_filter_cardinality_threshold() -> Result<(), Box<dyn std::error::Error>> {
        // Test configurations around the 1024 threshold
        let test_configs = vec![
            // Below threshold - should have bloom filters
            CardinalityTestConfig {
                name: "just_below_threshold",
                distinct_values: 1000,
                total_records: 1000,
                records_per_stripe: 1000, // 1000 distinct values per stripe
                expected_bloom_filter: true,
            },
            // At threshold - should have bloom filters
            CardinalityTestConfig {
                name: "at_threshold",
                distinct_values: 1024,
                total_records: 1024,
                records_per_stripe: 1024, // Exactly 1024 distinct values per stripe
                expected_bloom_filter: true,
            },
            // Above threshold - should NOT have bloom filters
            CardinalityTestConfig {
                name: "just_above_threshold",
                distinct_values: 1100,
                total_records: 1100,
                records_per_stripe: 1100, // 1100 distinct values per stripe
                expected_bloom_filter: false,
            },
            // Well above threshold - should NOT have bloom filters
            CardinalityTestConfig {
                name: "well_above_threshold",
                distinct_values: 2000,
                total_records: 2000,
                records_per_stripe: 2000, // 2000 distinct values per stripe
                expected_bloom_filter: false,
            },
        ];

        for config in test_configs {
            println!(
                "Testing cardinality threshold scenario: {} ({} distinct values per stripe)",
                config.name, config.distinct_values
            );

            let setup = BloomFilterTestSetup::new(config.clone())?;

            // Verify test data integrity
            setup.verify_test_data_integrity()?;

            // Verify shard structure
            setup.verify_shard_structure()?;

            // Test bloom filter presence according to expectations
            setup.for_each_stripe(|stripe_idx, stripe, field_data_type| {
                let field_ctx = stripe.open_field(field_data_type.clone())?;
                let bloom_filter = field_ctx.get_bloom_filter();

                if config.expected_bloom_filter {
                    assert!(
                        bloom_filter.is_some(),
                        "Expected bloom filter in stripe {} of {} (cardinality: {}), but none found",
                        stripe_idx,
                        config.name,
                        config.distinct_values
                    );
                    println!("✓ Found expected bloom filter in {} (cardinality: {})", 
                             config.name, config.distinct_values);
                } else {
                    assert!(
                        bloom_filter.is_none(),
                        "Expected NO bloom filter in stripe {} of {} (cardinality: {} > 1024), but one was found! High cardinality data should not have bloom filters.",
                        stripe_idx,
                        config.name,
                        config.distinct_values
                    );
                    println!("✓ Correctly found no bloom filter in {} (cardinality: {} > 1024)", 
                             config.name, config.distinct_values);
                }

                Ok(())
            })?;

            println!("✓ {} passed threshold test", config.name);
        }

        Ok(())
    }

    /// Test that bloom filters achieve the expected 1% false positive rate target
    #[test]
    fn test_bloom_filter_one_percent_fpp_target() -> Result<(), Box<dyn std::error::Error>> {
        let test_configs = vec![
            CardinalityTestConfig::low_cardinality(),
            CardinalityTestConfig::moderate_cardinality(),
            CardinalityTestConfig::high_cardinality(),
        ];

        for config in test_configs {
            println!("\n=== Testing 1% FPP target for: {} ===", config.name);

            let setup = BloomFilterTestSetup::new(config.clone())?;
            setup.verify_test_data_integrity()?;

            let mut overall_false_positives = 0;
            let mut overall_tests = 0;

            setup.for_each_stripe(|stripe_idx, stripe, field_data_type| {
                let field_ctx = stripe.open_field(field_data_type.clone())?;

                if let Some(filter) = field_ctx.get_bloom_filter() {
                    // Large test sample for statistical significance
                    let test_values: Vec<String> = (0..2000)
                        .map(|i| format!("fpp_test_{}_{}_stripe_{}", config.name, i, stripe_idx))
                        .collect();

                    let mut false_positives = 0;
                    for value in &test_values {
                        if filter.probe(value.as_bytes()) {
                            false_positives += 1;
                        }
                    }

                    let false_positive_rate = false_positives as f64 / test_values.len() as f64;

                    println!(
                        "Stripe {} FPP: {:.3}% ({}/{}) - Target: 1.0%",
                        stripe_idx,
                        false_positive_rate * 100.0,
                        false_positives,
                        test_values.len()
                    );

                    // Assert strict 1% FPP target with reasonable tolerance
                    assert!(
                        false_positive_rate <= 0.03, // 3% maximum - allows for statistical variation
                        "False positive rate {:.3}% in stripe {} of {} exceeds 3% maximum (target: 1%)",
                        false_positive_rate * 100.0,
                        stripe_idx,
                        config.name
                    );

                    // For high cardinality with sufficient data, expect very close to 1%
                    if config.distinct_values >= 500 {
                        assert!(
                            false_positive_rate <= 0.025, // 2.5% for high cardinality
                            "False positive rate {:.3}% in stripe {} of {} exceeds 2.5% for high cardinality (target: 1%)",
                            false_positive_rate * 100.0,
                            stripe_idx,
                            config.name
                        );
                    }

                    // Track overall statistics
                    overall_false_positives += false_positives;
                    overall_tests += test_values.len();

                    // Log warning for rates significantly above target
                    if false_positive_rate > 0.015 {
                        println!(
                            "  WARNING: FPP {:.3}% above 1.5% target in stripe {}",
                            false_positive_rate * 100.0,
                            stripe_idx
                        );
                    }
                }

                Ok(())
            })?;

            // Check overall performance for this configuration
            if overall_tests > 0 {
                let overall_fpp = overall_false_positives as f64 / overall_tests as f64;
                println!(
                    "Overall {} FPP: {:.3}% ({}/{}) - Target: 1.0%",
                    config.name,
                    overall_fpp * 100.0,
                    overall_false_positives,
                    overall_tests
                );

                assert!(
                    overall_fpp <= 0.025, // 2.5% overall maximum
                    "Overall false positive rate {:.3}% for {} exceeds 2.5% (target: 1%)",
                    overall_fpp * 100.0,
                    config.name
                );

                println!(
                    "✅ {} meets 1% FPP target (actual: {:.3}%)",
                    config.name,
                    overall_fpp * 100.0
                );
            }
        }

        Ok(())
    }

    /// Test GUID bloom filter creation and basic functionality
    #[test]
    fn test_guid_bloom_filter_creation() -> Result<(), Box<dyn std::error::Error>> {
        println!("\n=== Testing GUID bloom filter creation ===");

        let shard_store = ShardStore::new();
        let guid_data = generate_guid_data(100, 500); // 100 distinct GUIDs, 500 total records
        let schema = create_guid_column_schema("guid_column");

        let shard_url = create_guid_shard(&shard_store, schema, guid_data.clone(), 200)?;

        let shard = shard_store.open_shard(&shard_url);
        let shard_schema = shard.fetch_schema()?;
        let (_, field) = shard_schema
            .find_field("guid_column")?
            .ok_or("guid_column field should exist")?;
        let field_data_type = field.data_type()?;

        // Verify GUID column has bloom filter in each stripe
        for stripe_idx in 0..shard.stripe_count() {
            let stripe = shard.open_stripe(stripe_idx)?;
            let field_ctx = stripe.open_field(field_data_type.clone())?;
            let bloom_filter = field_ctx.get_bloom_filter();

            assert!(
                bloom_filter.is_some(),
                "GUID column should have bloom filter in stripe {}",
                stripe_idx
            );

            let filter = bloom_filter.unwrap();

            // Test with actual GUID values (should return true)
            let test_guid = &guid_data[stripe_idx * 10 % guid_data.len()];
            let probe_result = filter.probe(test_guid);
            assert!(
                probe_result,
                "Bloom filter should return true for actual GUID value in stripe {}",
                stripe_idx
            );

            // Test with unknown GUID and measure false positive rate
            let unknown_guids: Vec<[u8; 16]> = (0..100)
                .map(|i| {
                    let mut guid = [0u8; 16];
                    // Use a different pattern from test data to ensure they're truly unknown
                    guid[0..4].copy_from_slice(&((i + 50000) as u32).to_le_bytes());
                    guid[12..16].copy_from_slice(&((i * 17 + 99999) as u32).to_le_bytes());
                    guid
                })
                .collect();

            let mut false_positives = 0;
            for unknown_guid in &unknown_guids {
                if filter.probe(unknown_guid) {
                    false_positives += 1;
                }
            }

            let false_positive_rate = false_positives as f64 / unknown_guids.len() as f64;
            println!(
                "GUID stripe {} FPP: {:.3}% ({}/{})",
                stripe_idx,
                false_positive_rate * 100.0,
                false_positives,
                unknown_guids.len()
            );

            // Assert that false positive rate is reasonable (within 5% tolerance)
            assert!(
                false_positive_rate <= 0.05,
                "GUID bloom filter false positive rate {:.3}% too high in stripe {}",
                false_positive_rate * 100.0,
                stripe_idx
            );
        }

        println!("✅ GUID bloom filter test completed successfully");
        Ok(())
    }

    /// Test GUID bloom filter probing with various patterns
    #[test]
    fn test_guid_bloom_filter_probing() -> Result<(), Box<dyn std::error::Error>> {
        println!("\n=== Testing GUID bloom filter probing ===");

        let shard_store = ShardStore::new();

        // Create well-known GUID patterns for testing
        let test_guids = vec![
            [0u8; 16],  // All zeros
            [0xFF; 16], // All ones
            {
                let mut guid = [0u8; 16];
                guid[0..4].copy_from_slice(&42u32.to_le_bytes());
                guid[8..12].copy_from_slice(&123u32.to_le_bytes());
                guid
            },
            {
                let mut guid = [0u8; 16];
                guid[0..8].copy_from_slice(&0x1234567890ABCDEFu64.to_le_bytes());
                guid[8..16].copy_from_slice(&0xFEDCBA0987654321u64.to_le_bytes());
                guid
            },
        ];

        let schema = create_guid_column_schema("test_guid");
        let shard_url = create_guid_shard(&shard_store, schema, test_guids.clone(), 100)?;

        let shard = shard_store.open_shard(&shard_url);
        let shard_schema = shard.fetch_schema()?;
        let (_, field) = shard_schema
            .find_field("test_guid")?
            .ok_or("test_guid field should exist")?;
        let field_data_type = field.data_type()?;

        // Test bloom filter with the exact GUID values we inserted
        for stripe_idx in 0..shard.stripe_count() {
            let stripe = shard.open_stripe(stripe_idx)?;
            let field_ctx = stripe.open_field(field_data_type.clone())?;
            let bloom_filter = field_ctx.get_bloom_filter();

            assert!(
                bloom_filter.is_some(),
                "GUID column should have bloom filter in stripe {}",
                stripe_idx
            );

            let filter = bloom_filter.unwrap();

            // All inserted GUIDs should be found (no false negatives allowed)
            for (guid_idx, test_guid) in test_guids.iter().enumerate() {
                let found = filter.probe(test_guid);
                assert!(
                    found,
                    "GUID bloom filter should find inserted GUID {} in stripe {}",
                    guid_idx, stripe_idx
                );
            }

            println!(
                "✓ Stripe {} correctly found all {} inserted GUIDs",
                stripe_idx,
                test_guids.len()
            );
        }

        println!("✅ GUID bloom filter probing test completed successfully");
        Ok(())
    }

    /// Test that non-GUID binary types do NOT get bloom filters when using Plain encoding profile
    #[test]
    fn test_non_guid_binary_no_bloom_filter_plain_profile() -> Result<(), Box<dyn std::error::Error>>
    {
        println!(
            "\n=== Testing that non-GUID binary types don't get bloom filters with Plain profile ==="
        );

        let shard_store = ShardStore::new();
        // Set encoding profile to Plain explicitly to ensure bloom filters are disabled
        shard_store.set_shard_encoding_profile(
            amudai_encodings::block_encoder::BlockEncodingProfile::Plain,
        );

        let binary_data = generate_binary_data(50, 200); // 50 distinct binary values, 200 total records
        let schema = create_binary_column_schema("binary_column");

        let shard_url = create_binary_shard(&shard_store, schema, binary_data, 100)?;

        let shard = shard_store.open_shard(&shard_url);
        let shard_schema = shard.fetch_schema()?;
        let (_, field) = shard_schema
            .find_field("binary_column")?
            .ok_or("binary_column field should exist")?;
        let field_data_type = field.data_type()?;

        // Verify binary column does NOT have bloom filter in any stripe when using Plain profile
        for stripe_idx in 0..shard.stripe_count() {
            let stripe = shard.open_stripe(stripe_idx)?;
            let field_ctx = stripe.open_field(field_data_type.clone())?;
            let bloom_filter = field_ctx.get_bloom_filter();

            // Non-GUID binary types with Plain encoding profile should NOT have bloom filters
            assert!(
                bloom_filter.is_none(),
                "Non-GUID binary column should NOT have bloom filter in stripe {} with Plain encoding profile",
                stripe_idx
            );

            println!(
                "✓ Stripe {} correctly has no bloom filter for non-GUID binary data",
                stripe_idx
            );
        }

        println!("✅ Non-GUID binary no bloom filter test completed successfully");
        Ok(())
    }

    /// Test that fixed-size non-GUID binary types do NOT get bloom filters when using Plain encoding profile
    #[test]
    fn test_fixed_size_non_guid_binary_no_bloom_filter() -> Result<(), Box<dyn std::error::Error>> {
        println!("\n=== Testing that fixed-size non-GUID binary types don't get bloom filters ===");

        let shard_store = ShardStore::new();
        // Set encoding profile to Plain explicitly to ensure bloom filters are disabled
        shard_store.set_shard_encoding_profile(BlockEncodingProfile::Plain);

        // Create fixed-size binary data (8 bytes each, not GUID which is 16 bytes)
        let fixed_binary_data: Vec<[u8; 8]> = (0..100)
            .map(|i| {
                let mut data = [0u8; 8];
                data[0..4].copy_from_slice(&(i as u32).to_le_bytes());
                data[4..8].copy_from_slice(&((i * 3) as u32).to_le_bytes());
                data
            })
            .collect();

        // Create schema for 8-byte fixed-size binary (not GUID)
        let schema = Schema::new(vec![Field::new(
            "fixed_binary_column",
            DataType::FixedSizeBinary(8),
            false,
        )]);
        let arc_schema = Arc::new(schema);
        let mut shard_builder = shard_store.create_shard_builder(&arc_schema);

        // Create record batch with fixed-size binary data
        let values: Vec<&[u8]> = fixed_binary_data
            .iter()
            .map(|data| data.as_slice())
            .collect();
        let array = arrow_array::FixedSizeBinaryArray::from(values);
        let batch = RecordBatch::try_new(arc_schema.clone(), vec![Arc::new(array)])?;

        let mut stripe_builder = shard_builder.build_stripe()?;
        stripe_builder.push_batch(&batch)?;
        let stripe = stripe_builder.finish()?;
        shard_builder.add_stripe(stripe)?;

        let shard_ref = shard_store.seal_shard_builder(shard_builder);
        let shard = shard_store.open_shard(&shard_ref.url);

        let shard_schema = shard.fetch_schema()?;
        let (_, field) = shard_schema
            .find_field("fixed_binary_column")?
            .ok_or("fixed_binary_column field should exist")?;
        let field_data_type = field.data_type()?;

        // Verify fixed-size binary column does NOT have bloom filter when it's not a GUID
        for stripe_idx in 0..shard.stripe_count() {
            let stripe = shard.open_stripe(stripe_idx)?;
            let field_ctx = stripe.open_field(field_data_type.clone())?;
            let bloom_filter = field_ctx.get_bloom_filter();

            // 8-byte fixed-size binary should NOT have bloom filter (only 16-byte GUIDs should)
            assert!(
                bloom_filter.is_none(),
                "8-byte fixed-size binary column should NOT have bloom filter in stripe {} (only GUIDs should)",
                stripe_idx
            );

            println!(
                "✓ Stripe {} correctly has no bloom filter for 8-byte fixed-size binary data",
                stripe_idx
            );
        }

        println!("✅ Fixed-size non-GUID binary no bloom filter test completed successfully");
        Ok(())
    }
}
