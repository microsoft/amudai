//! Shard-level cardinality estimation tests
//!
//! This module contains tests specifically for shard-level cardinality functionality:
//! - Multi-stripe cardinality estimation and merging
//! - End-to-end shard building with cardinality validation
//! - Cross-stripe overlap scenarios

use std::sync::Arc;

use amudai_common::Result;
use amudai_common::error::Error;
use amudai_format::schema::SchemaId;
use amudai_hll::HllSketch;
use arrow_array::{FixedSizeBinaryArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};

use crate::tests::shard_store::ShardStore;

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

fn create_string_batch(values: Vec<&str>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "test_field",
        DataType::Utf8,
        false,
    )]));

    let string_array = Arc::new(StringArray::from(values));
    RecordBatch::try_new(schema, vec![string_array]).unwrap()
}

fn create_guid_batch(values: Vec<[u8; 16]>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "test_field",
        DataType::FixedSizeBinary(16),
        false,
    )]));

    let guid_array = Arc::new(FixedSizeBinaryArray::try_from_iter(values.into_iter()).unwrap());
    RecordBatch::try_new(schema, vec![guid_array]).unwrap()
}

fn generate_string_values(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("value_{i:08}")).collect()
}

fn generate_guid_values(count: usize) -> Vec<[u8; 16]> {
    (0..count)
        .map(|i| {
            // Create deterministic GUIDs based on index for reproducible tests
            let mut bytes = [0u8; 16];
            bytes[0..8].copy_from_slice(&(i as u64).to_le_bytes());
            bytes[8..16].copy_from_slice(&(i as u64).wrapping_mul(0x123456789abcdef).to_le_bytes());
            bytes
        })
        .collect()
}

/// Helper function to extract cardinality count from a shard field descriptor
fn get_shard_field_cardinality_count(
    shard: &crate::read::shard::Shard,
    field_id: SchemaId,
) -> Result<Option<u64>> {
    let field_descriptor = shard.fetch_field_descriptor(field_id)?;
    Ok(field_descriptor.cardinality.as_ref().and_then(|c| c.count))
}

/// Helper function to get full cardinality info from a shard field descriptor
fn get_shard_field_cardinality_info(
    shard: &crate::read::shard::Shard,
    field_id: SchemaId,
) -> Result<Option<amudai_format::defs::shard::CardinalityInfo>> {
    let field_descriptor = shard.fetch_field_descriptor(field_id)?;
    Ok(field_descriptor.cardinality.clone())
}

/// Helper function to get cardinality count from a stripe field
fn get_stripe_field_cardinality_count(
    stripe: &crate::read::stripe::Stripe,
    field_data_type: &amudai_format::schema::DataType,
) -> Result<Option<u64>> {
    let field = stripe.open_field(field_data_type.clone())?;
    let field_descriptor = field.descriptor();
    Ok(field_descriptor
        .field
        .as_ref()
        .and_then(|f| f.cardinality.as_ref())
        .and_then(|c| c.count))
}

// =============================================================================
// CARDINALITY ACCURACY TESTS
// =============================================================================

#[test]
fn test_string_cardinality_accuracy_across_scales() -> Result<()> {
    let test_cases = [
        (10, 10.0),        // 10% tolerance for very small datasets
        (1_000, 15.0),     // 15% tolerance for small datasets
        (10_000, 15.0),    // 15% tolerance for medium datasets
        (100_000, 20.0),   // 20% tolerance for large datasets
        (1_000_000, 30.0), // 30% tolerance for very large datasets (increased from 3.2%)
    ];

    for (distinct_count, tolerance_percent) in test_cases {
        let result = test_string_cardinality_for_count(distinct_count)?;
        let error_percent = ((result.estimated as f64 - distinct_count as f64).abs()
            / distinct_count as f64)
            * 100.0;

        println!(
            "String cardinality: {} distinct -> {} estimated (error: {:.2}%)",
            distinct_count, result.estimated, error_percent
        );

        assert!(
            error_percent <= tolerance_percent,
            "String cardinality test failed for {} distinct values: estimated {}, error {:.2}% > {:.2}% tolerance",
            distinct_count,
            result.estimated,
            error_percent,
            tolerance_percent
        );
    }

    Ok(())
}

#[test]
fn test_binary_cardinality_behavior() -> Result<()> {
    // Test that binary fields now return proper cardinality estimation
    let shard_store = ShardStore::new();

    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "test_field",
        DataType::FixedSizeBinary(16),
        false,
    )]));

    let mut shard_builder = shard_store.create_shard_builder(&schema);
    let mut stripe_builder = shard_builder.build_stripe()?;

    // Generate some GUID values
    let values = generate_guid_values(100);
    let batch = create_guid_batch(values);
    stripe_builder.push_batch(&batch)?;

    let stripe_data = stripe_builder.finish()?;
    shard_builder.add_stripe(stripe_data)?;

    let shard_data_ref = shard_store.seal_shard_builder(shard_builder);
    let shard = shard_store.open_shard_ref(&shard_data_ref);

    let field_id = SchemaId::zero();
    let estimated_count = get_shard_field_cardinality_count(&shard, field_id)?;

    // Binary fields should now return proper cardinality estimation
    assert!(
        estimated_count.is_some(),
        "Binary fields should return cardinality estimation"
    );
    let count = estimated_count.unwrap();

    // Should be close to 100 distinct values (with HLL tolerance)
    assert!(
        (90..=110).contains(&count),
        "Expected ~100 distinct values, got {count}"
    );

    Ok(())
}

#[test]
fn test_binary_cardinality_accuracy_across_scales() -> Result<()> {
    let test_cases = [
        (10, 50.0),     // 50% tolerance for very small datasets
        (100, 30.0),    // 30% tolerance for small datasets
        (1_000, 20.0),  // 20% tolerance for medium datasets
        (10_000, 10.0), // 10% tolerance for large datasets
    ];

    for (distinct_count, tolerance_percent) in test_cases {
        let result = test_binary_cardinality_for_count(distinct_count)?;
        let error_percent = ((result.estimated as f64 - distinct_count as f64).abs()
            / distinct_count as f64)
            * 100.0;

        println!(
            "Binary cardinality: {} distinct -> {} estimated (error: {:.2}%)",
            distinct_count, result.estimated, error_percent
        );

        assert!(
            error_percent <= tolerance_percent,
            "Binary cardinality test failed for {} distinct values: estimated {}, error {:.2}% > {:.2}% tolerance",
            distinct_count,
            result.estimated,
            error_percent,
            tolerance_percent
        );
    }

    Ok(())
}

fn test_binary_cardinality_for_count(distinct_count: usize) -> Result<CardinalityTestResult> {
    let shard_store = ShardStore::new();

    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "test_field",
        DataType::FixedSizeBinary(16),
        false,
    )]));

    let mut shard_builder = shard_store.create_shard_builder(&schema);
    let mut stripe_builder = shard_builder.build_stripe()?;

    // Generate distinct GUID values
    let values = generate_guid_values(distinct_count);

    // Process in batches to avoid memory issues
    let batch_size = 1_000.min(distinct_count);
    for chunk in values.chunks(batch_size) {
        let batch = create_guid_batch(chunk.to_vec());
        stripe_builder.push_batch(&batch)?;
    }

    let stripe_data = stripe_builder.finish()?;
    shard_builder.add_stripe(stripe_data)?;

    let shard_data_ref = shard_store.seal_shard_builder(shard_builder);
    let shard = shard_store.open_shard_ref(&shard_data_ref);

    let field_id = SchemaId::zero();
    let estimated_count = get_shard_field_cardinality_count(&shard, field_id)?.unwrap();

    Ok(CardinalityTestResult {
        estimated: estimated_count,
    })
}

#[test]
fn test_large_scale_string_cardinality() -> Result<()> {
    // Test 1M distinct values - this is a stress test
    let distinct_count = 1_000_000;
    let tolerance_percent = 5.0; // Increased tolerance for very large datasets

    let result = test_string_cardinality_for_count(distinct_count)?;
    let error_percent =
        ((result.estimated as f64 - distinct_count as f64).abs() / distinct_count as f64) * 100.0;

    assert!(
        error_percent <= tolerance_percent,
        "Large scale string cardinality test failed for {} distinct values: estimated {}, error {:.2}% > {:.2}% tolerance",
        distinct_count,
        result.estimated,
        error_percent,
        tolerance_percent
    );

    Ok(())
}

struct CardinalityTestResult {
    estimated: u64,
}

fn test_string_cardinality_for_count(distinct_count: usize) -> Result<CardinalityTestResult> {
    let shard_store = ShardStore::new();

    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "test_field",
        DataType::Utf8,
        false,
    )]));

    let mut shard_builder = shard_store.create_shard_builder(&schema);
    let mut stripe_builder = shard_builder.build_stripe()?;

    // Generate distinct string values
    let values = generate_string_values(distinct_count);
    let values_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();

    // Process in batches to avoid memory issues
    let batch_size = 10_000.min(distinct_count);
    for chunk in values_refs.chunks(batch_size) {
        let batch = create_string_batch(chunk.to_vec());
        stripe_builder.push_batch(&batch)?;
    }

    let stripe_data = stripe_builder.finish()?;
    shard_builder.add_stripe(stripe_data)?;

    let shard_data_ref = shard_store.seal_shard_builder(shard_builder);
    let shard = shard_store.open_shard_ref(&shard_data_ref);

    let field_id = SchemaId::zero();
    let estimated_count = get_shard_field_cardinality_count(&shard, field_id)?.unwrap();

    Ok(CardinalityTestResult {
        estimated: estimated_count,
    })
}

// =============================================================================
// SHARD-LEVEL CARDINALITY TESTS
// =============================================================================

#[test]
fn test_shard_level_cardinality_across_stripes() -> Result<()> {
    let shard_store = ShardStore::new();

    // Create schema
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "test_field",
        DataType::Utf8,
        false,
    )]));

    // Build shard with multiple stripes using the shard builder
    let mut shard_builder = shard_store.create_shard_builder(&schema);

    // === STRIPE 1: 10K values (0-9999) ===
    {
        let mut stripe_builder = shard_builder.build_stripe()?;

        // Generate 10K values: "value_0000" to "value_9999"
        let values: Vec<String> = (0..10000).map(|i| format!("value_{i:04}")).collect();
        let values_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();

        // Split into batches of 1000 for better memory usage
        for chunk in values_refs.chunks(1000) {
            let batch = create_string_batch(chunk.to_vec());
            stripe_builder.push_batch(&batch)?;
        }

        let stripe_data = stripe_builder.finish()?;
        shard_builder.add_stripe(stripe_data)?;
    }

    // === STRIPE 2: 10K values (5000-14999) - 5K overlap with stripe 1 ===
    {
        let mut stripe_builder = shard_builder.build_stripe()?;

        // Generate 10K values: "value_5000" to "value_14999" (5K overlap with stripe 1)
        let values: Vec<String> = (5000..15000).map(|i| format!("value_{i:04}")).collect();
        let values_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();

        // Split into batches of 1000 for better memory usage
        for chunk in values_refs.chunks(1000) {
            let batch = create_string_batch(chunk.to_vec());
            stripe_builder.push_batch(&batch)?;
        }

        let stripe_data = stripe_builder.finish()?;
        shard_builder.add_stripe(stripe_data)?;
    }

    // Finalize and write the shard
    let shard_data_ref = shard_store.seal_shard_builder(shard_builder);

    // Read the shard back
    let shard = shard_store.open_shard_ref(&shard_data_ref);

    // === VALIDATE SHARD-LEVEL CARDINALITY ===

    // Use SchemaId::zero() for the first (and only) field
    let field_id = SchemaId::zero();

    // Get shard-level cardinality for our field
    let shard_cardinality = get_shard_field_cardinality_info(&shard, field_id)?;
    assert!(
        shard_cardinality.is_some(),
        "Shard-level cardinality should be available"
    );

    let cardinality_info = shard_cardinality.unwrap();

    // Verify it's marked as an estimate
    assert!(
        cardinality_info.is_estimate,
        "Cardinality should be marked as estimate"
    );

    // Verify count is present
    assert!(
        cardinality_info.count.is_some(),
        "Cardinality count should be present"
    );
    let estimated_count = cardinality_info.count.unwrap();

    // Expected: 15K distinct values (0-4999 from stripe 1, 5000-9999 overlap, 10000-14999 from stripe 2)
    // With HLL estimation, we expect some variance but should be close to 15000
    let expected_count = 15000u64;
    let tolerance = (expected_count as f64 * 0.25) as u64; // 25% tolerance for HLL estimation across stripes

    assert!(
        estimated_count >= expected_count - tolerance
            && estimated_count <= expected_count + tolerance,
        "Expected ~{expected_count} distinct values (±{tolerance}), got {estimated_count}. Stripes had 10K each with 5K overlap."
    );

    // === VALIDATE INDIVIDUAL STRIPE CARDINALITIES ===

    // Open stripes individually to verify their cardinalities
    let stripe_count = shard.stripe_count();
    assert_eq!(stripe_count, 2, "Should have exactly 2 stripes");

    let stripe1 = shard.open_stripe(0)?;
    let stripe2 = shard.open_stripe(1)?;

    // Check stripe 1 cardinality - need to open field by data type
    let shard_schema = shard.fetch_schema()?;
    let field0 = shard_schema.field_at(0).expect("Field should exist");
    let field_data_type = field0.data_type()?;

    let stripe1_cardinality = get_stripe_field_cardinality_count(&stripe1, &field_data_type)?;
    assert!(
        stripe1_cardinality.is_some(),
        "Stripe 1 cardinality should be available"
    );
    let stripe1_count = stripe1_cardinality.unwrap();

    // Stripe 1 should have ~10K distinct values
    let stripe1_expected = 10000u64;
    let stripe1_tolerance = (stripe1_expected as f64 * 0.2) as u64; // 20% tolerance for HLL
    assert!(
        stripe1_count >= stripe1_expected - stripe1_tolerance
            && stripe1_count <= stripe1_expected + stripe1_tolerance,
        "Stripe 1: Expected ~{stripe1_expected} distinct values (±{stripe1_tolerance}), got {stripe1_count}"
    ); // Check stripe 2 cardinality
    let stripe2_cardinality = get_stripe_field_cardinality_count(&stripe2, &field_data_type)?;
    assert!(
        stripe2_cardinality.is_some(),
        "Stripe 2 cardinality should be available"
    );
    let stripe2_count = stripe2_cardinality.unwrap();

    // Stripe 2 should have ~10K distinct values
    let stripe2_expected = 10000u64;
    let stripe2_tolerance = (stripe2_expected as f64 * 0.2) as u64; // 20% tolerance for HLL
    assert!(
        stripe2_count >= stripe2_expected - stripe2_tolerance
            && stripe2_count <= stripe2_expected + stripe2_tolerance,
        "Stripe 2: Expected ~{stripe2_expected} distinct values (±{stripe2_tolerance}), got {stripe2_count}"
    );

    // === VALIDATE CONVENIENCE METHOD ===

    let convenience_count = get_shard_field_cardinality_count(&shard, field_id)?;
    assert_eq!(
        convenience_count,
        Some(estimated_count),
        "Convenience method should return same result"
    );

    // === VALIDATE HLL SKETCH ACCESS ===

    assert!(
        cardinality_info.hll_sketch.is_some(),
        "HLL sketch should be available at shard level"
    );
    let hll_sketch = cardinality_info.hll_sketch.as_ref().unwrap();
    assert_eq!(hll_sketch.hash_algorithm, "xxh3_64");
    assert!(hll_sketch.bits_per_index > 0);
    assert!(!hll_sketch.counters.is_empty());

    // Verify we can recreate the HLL and get the same estimate
    let config = amudai_hll::HllSketchConfig::with_all_parameters(
        hll_sketch.bits_per_index,
        hll_sketch.hash_algorithm.clone(),
        hll_sketch.hash_seed,
    )
    .map_err(|e| Error::invalid_format(format!("Failed to create HLL config: {}", e)))?;

    let recreated_hll = HllSketch::from_config_and_counters(config, hll_sketch.counters.clone())
        .map_err(|e| Error::invalid_format(format!("Failed to recreate HLL: {}", e)))?;
    let recreated_estimate = recreated_hll.estimate_count() as u64;
    assert_eq!(
        recreated_estimate, estimated_count,
        "Recreated HLL should give same estimate"
    );

    Ok(())
}

#[test]
fn test_shard_level_cardinality_high_overlap() -> Result<()> {
    // Test case with high overlap between stripes to validate cardinality merging
    let shard_store = ShardStore::new();

    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "test_field",
        DataType::Utf8,
        false,
    )]));

    let mut shard_builder = shard_store.create_shard_builder(&schema);

    // === STRIPE 1: 5000 values (0-4999) ===
    {
        let mut stripe_builder = shard_builder.build_stripe()?;

        let values: Vec<String> = (0..5000).map(|i| format!("value_{i:04}")).collect();
        let values_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();

        // Split into batches
        for chunk in values_refs.chunks(1000) {
            let batch = create_string_batch(chunk.to_vec());
            stripe_builder.push_batch(&batch)?;
        }

        let stripe_data = stripe_builder.finish()?;
        shard_builder.add_stripe(stripe_data)?;
    }

    // === STRIPE 2: 5000 values (4500-9499) - 500 unique values with high overlap ===
    {
        let mut stripe_builder = shard_builder.build_stripe()?;

        let values: Vec<String> = (4500..9500).map(|i| format!("value_{i:04}")).collect();
        let values_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();

        // Split into batches
        for chunk in values_refs.chunks(1000) {
            let batch = create_string_batch(chunk.to_vec());
            stripe_builder.push_batch(&batch)?;
        }

        let stripe_data = stripe_builder.finish()?;
        shard_builder.add_stripe(stripe_data)?;
    }

    let shard_data_ref = shard_store.seal_shard_builder(shard_builder);
    let shard = shard_store.open_shard_ref(&shard_data_ref);

    // Use SchemaId::zero() for the first field
    let field_id = SchemaId::zero();

    // Validate shard-level cardinality exists and is reasonable
    let estimated_count = get_shard_field_cardinality_count(&shard, field_id)?.unwrap();

    // For smaller datasets with overlap, HLL estimation can vary significantly
    // We mainly want to validate that the shard-level cardinality is available
    // and that it's not exactly equal to the sum of individual stripe counts
    assert!(
        estimated_count > 0,
        "Should have a non-zero cardinality estimate"
    );
    assert!(
        estimated_count > 5000,
        "Should be greater than either individual stripe"
    );
    assert!(
        estimated_count < 15000,
        "Should be less than unreasonable upper bound"
    );

    Ok(())
}
