use std::sync::Arc;

use crate::write::field_encoder::{EncodedFieldStatistics, FieldEncoder, FieldEncoderParams};
use amudai_common::Result;
use amudai_data_stats::floating::FloatingStatsCollector;
use amudai_format::defs::schema_ext::BasicTypeDescriptor;
use amudai_format::schema::BasicType;
use amudai_io_impl::temp_file_store;
use arrow_array::{Float32Array, Float64Array};

#[test]
#[allow(clippy::approx_constant)]
fn test_floating_statistics_integration_f32() -> Result<()> {
    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

    let basic_type = BasicTypeDescriptor {
        basic_type: BasicType::Float32,
        fixed_size: 0,
        signed: false,
        extended_type: Default::default(),
    };

    let mut encoder = FieldEncoder::new(FieldEncoderParams {
        basic_type,
        temp_store,
        encoding_profile: Default::default(),
    })?;

    // Create test data with various floating-point values
    let values = vec![
        1.5f32,
        -2.5f32,
        0.0f32,
        f32::NAN,
        f32::INFINITY,
        f32::NEG_INFINITY,
        3.14f32,
        -1.0f32,
    ];
    let array = Arc::new(Float32Array::from(values));
    encoder.push_array(array)?;

    // Add some nulls
    encoder.push_nulls(2)?;

    let encoded_field = encoder.finish()?;

    // Verify we got floating statistics
    assert!(encoded_field.statistics.is_some());
    if let Some(EncodedFieldStatistics::Floating(stats)) = encoded_field.statistics {
        assert_eq!(stats.total_count, 10); // 8 values + 2 nulls
        assert_eq!(stats.null_count, 2);
        assert_eq!(stats.zero_count, 1);
        assert_eq!(stats.positive_count, 2); // 1.5, 3.14 (infinities counted separately)
        assert_eq!(stats.negative_count, 2); // -2.5, -1.0 (infinities counted separately)
        assert_eq!(stats.nan_count, 1);
        assert_eq!(stats.positive_infinity_count, 1);
        assert_eq!(stats.negative_infinity_count, 1);

        // Min/max should exclude NaN but include infinities
        assert_eq!(stats.min_value, Some(f64::NEG_INFINITY));
        assert_eq!(stats.max_value, Some(f64::INFINITY));
    } else {
        panic!(
            "Expected FloatingStats but got {:?}",
            encoded_field.statistics
        );
    }

    Ok(())
}

#[test]
fn test_floating_statistics_integration_f64() -> Result<()> {
    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

    let basic_type = BasicTypeDescriptor {
        basic_type: BasicType::Float64,
        fixed_size: 0,
        signed: false,
        extended_type: Default::default(),
    };

    let mut encoder = FieldEncoder::new(FieldEncoderParams {
        basic_type,
        temp_store,
        encoding_profile: Default::default(),
    })?;

    // Create test data with finite values only
    let values = vec![1.0f64, 2.0f64, 3.0f64, -1.0f64, -2.0f64, 0.0f64];
    let array = Arc::new(Float64Array::from(values));
    encoder.push_array(array)?;

    let encoded_field = encoder.finish()?;

    // Verify we got floating statistics
    assert!(encoded_field.statistics.is_some());
    if let Some(EncodedFieldStatistics::Floating(stats)) = encoded_field.statistics {
        assert_eq!(stats.total_count, 6);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.zero_count, 1);
        assert_eq!(stats.positive_count, 3); // 1.0, 2.0, 3.0
        assert_eq!(stats.negative_count, 2); // -1.0, -2.0
        assert_eq!(stats.nan_count, 0);
        assert_eq!(stats.positive_infinity_count, 0);
        assert_eq!(stats.negative_infinity_count, 0);

        // Min/max for finite values
        assert_eq!(stats.min_value, Some(-2.0));
        assert_eq!(stats.max_value, Some(3.0));

        // Helper methods
        assert_eq!(stats.finite_count(), 6);
        assert_eq!(stats.infinity_count(), 0);
    } else {
        panic!(
            "Expected FloatingStats but got {:?}",
            encoded_field.statistics
        );
    }

    Ok(())
}

#[test]
#[allow(clippy::approx_constant)]
fn test_floating_stats_collector_directly() {
    let mut collector = FloatingStatsCollector::new();

    // Test f32 values
    collector.process_f32_value(1.5);
    collector.process_f32_value(-2.5);
    collector.process_f32_value(0.0);
    collector.process_f32_value(f32::NAN);
    collector.process_f32_value(f32::INFINITY);

    // Test f64 values
    collector.process_f64_value(3.14);
    collector.process_f64_value(f64::NEG_INFINITY);

    // Test nulls
    collector.process_null();
    collector.process_nulls(2);

    let stats = collector.finalize();

    assert_eq!(stats.total_count, 10); // 7 values + 3 nulls
    assert_eq!(stats.null_count, 3);
    assert_eq!(stats.zero_count, 1);
    assert_eq!(stats.positive_count, 2); // 1.5, 3.14 (finite positives)
    assert_eq!(stats.negative_count, 1); // -2.5 (finite negative)
    assert_eq!(stats.nan_count, 1);
    assert_eq!(stats.positive_infinity_count, 1);
    assert_eq!(stats.negative_infinity_count, 1);

    // Helper methods
    assert_eq!(stats.finite_count(), 4); // 1.5, -2.5, 0.0, 3.14
    assert_eq!(stats.infinity_count(), 2);
    assert_eq!(stats.non_null_count(), 7);
}
