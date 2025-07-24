use std::sync::Arc;

use crate::{
    tests::{data_generator::generate_field, shard_store::ShardStore},
    write::field_encoder::{
        DictionaryEncoding, EncodedFieldStatistics, FieldEncoder, FieldEncoderParams,
    },
};
use amudai_blockstream::read::block_stream::empty_hint;
use amudai_common::Result;
use amudai_data_stats::floating::FloatingStatsCollector;
use amudai_format::{defs::schema_ext::BasicTypeDescriptor, schema::BasicType};
use amudai_io_impl::temp_file_store;
use arrow_array::{ArrayRef, Float32Array, Float64Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};

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
        dictionary_encoding: DictionaryEncoding::Enabled,
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
    assert!(encoded_field.statistics.is_present());
    if let EncodedFieldStatistics::Floating(stats) = encoded_field.statistics {
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
        dictionary_encoding: DictionaryEncoding::Enabled,
    })?;

    // Create test data with finite values only
    let values = vec![1.0f64, 2.0f64, 3.0f64, -1.0f64, -2.0f64, 0.0f64];
    let array = Arc::new(Float64Array::from(values));
    encoder.push_array(array)?;

    let encoded_field = encoder.finish()?;

    // Verify we got floating statistics
    assert!(encoded_field.statistics.is_present());
    if let EncodedFieldStatistics::Floating(stats) = encoded_field.statistics {
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

#[test]
fn test_constant_f32_stripe_integration() -> Result<()> {
    let shard_store = ShardStore::new();

    // Create a schema with f32 field
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Float32, false),
        Field::new("description", DataType::Utf8, true),
    ]));

    // Create a constant f32 value
    let constant_f32_value = 3.14159_f32;

    // Generate batches with constant f32 values
    let record_count = 1000;
    let batches =
        generate_constant_f32_batches(schema.clone(), 50..100, record_count, constant_f32_value);
    let batch_iter = RecordBatchIterator::new(batches.map(Ok), schema.clone());

    // Write to shard
    let data_ref = shard_store.ingest_shard_from_record_batches(batch_iter);

    // Verify shard was created successfully
    let shard = shard_store.open_shard(&data_ref.url);
    assert_eq!(shard.directory().total_record_count, 1000);
    assert_eq!(shard.directory().stripe_count, 1);

    // Open the stripe and verify schema
    let stripe = shard.open_stripe(0)?;
    let stripe_schema = stripe.fetch_schema()?;

    // Find the f32 field
    let value_field_info = stripe_schema.find_field("value")?.unwrap();

    // Open the f32 field
    let value_field = stripe.open_field(value_field_info.1.data_type().unwrap())?;

    // Verify field type
    assert_eq!(value_field.data_type().basic_type()?, BasicType::Float32);

    // Create decoder and reader for the value field
    let value_decoder = value_field.create_decoder()?;
    let mut value_reader = value_decoder.create_reader(empty_hint())?;

    // Read a range of values and verify they're all the same constant
    let seq = value_reader.read_range(0..10)?;
    assert_eq!(seq.len(), 10);

    // For primitive types, there should be no offsets
    assert!(
        seq.offsets.is_none(),
        "Primitive f32 should not have offsets"
    );

    // Verify all values are the same constant f32
    let values_bytes = seq.values.as_bytes();
    assert_eq!(values_bytes.len(), 10 * 4); // 10 f32 values * 4 bytes each

    // Convert bytes back to f32 and verify
    let f32_values = seq.values.as_slice::<f32>();
    for &value in f32_values {
        assert_eq!(value, constant_f32_value);
    }

    // Test reading different ranges
    let seq2 = value_reader.read_range(100..150)?;
    assert_eq!(seq2.len(), 50);

    let f32_values2 = seq2.values.as_slice::<f32>();
    for &value in f32_values2 {
        assert_eq!(value, constant_f32_value);
    }

    println!("✓ Constant f32 stripe integration test passed!");
    println!("  Successfully created and read constant f32 values from stripe");
    println!("  Verified {record_count} records with constant f32 value: {constant_f32_value}");

    Ok(())
}

#[test]
fn test_constant_f64_stripe_integration() -> Result<()> {
    let shard_store = ShardStore::new();

    // Create a schema with f64 field
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Float64, false),
        Field::new("description", DataType::Utf8, true),
    ]));

    // Create a constant f64 value
    let constant_f64_value = 2.718281828459045_f64; // e

    // Generate batches with constant f64 values
    let record_count = 1000;
    let batches =
        generate_constant_f64_batches(schema.clone(), 50..100, record_count, constant_f64_value);
    let batch_iter = RecordBatchIterator::new(batches.map(Ok), schema.clone());

    // Write to shard
    let data_ref = shard_store.ingest_shard_from_record_batches(batch_iter);

    // Verify shard was created successfully
    let shard = shard_store.open_shard(&data_ref.url);
    assert_eq!(shard.directory().total_record_count, 1000);
    assert_eq!(shard.directory().stripe_count, 1);

    // Open the stripe and verify schema
    let stripe = shard.open_stripe(0)?;
    let stripe_schema = stripe.fetch_schema()?;

    // Find the f64 field
    let value_field_info = stripe_schema.find_field("value")?.unwrap();

    // Open the f64 field
    let value_field = stripe.open_field(value_field_info.1.data_type().unwrap())?;

    // Verify field type
    assert_eq!(value_field.data_type().basic_type()?, BasicType::Float64);

    // Create decoder and reader for the value field
    let value_decoder = value_field.create_decoder()?;
    let mut value_reader = value_decoder.create_reader(empty_hint())?;

    // Read a range of values and verify they're all the same constant
    let seq = value_reader.read_range(0..10)?;
    assert_eq!(seq.len(), 10);

    // For primitive types, there should be no offsets
    assert!(
        seq.offsets.is_none(),
        "Primitive f64 should not have offsets"
    );

    // Verify all values are the same constant f64
    let values_bytes = seq.values.as_bytes();
    assert_eq!(values_bytes.len(), 10 * 8); // 10 f64 values * 8 bytes each

    // Convert bytes back to f64 and verify
    let f64_values = seq.values.as_slice::<f64>();
    for &value in f64_values {
        assert_eq!(value, constant_f64_value);
    }

    // Test reading different ranges
    let seq2 = value_reader.read_range(100..150)?;
    assert_eq!(seq2.len(), 50);

    let f64_values2 = seq2.values.as_slice::<f64>();
    for &value in f64_values2 {
        assert_eq!(value, constant_f64_value);
    }

    println!("✓ Constant f64 stripe integration test passed!");
    println!("  Successfully created and read constant f64 values from stripe");
    println!("  Verified {record_count} records with constant f64 value: {constant_f64_value}");

    Ok(())
}

/// Generate batches with constant f32 values for testing
fn generate_constant_f32_batches(
    schema: Arc<Schema>,
    batch_size: std::ops::Range<usize>,
    record_count: usize,
    constant_f32_value: f32,
) -> Box<dyn Iterator<Item = RecordBatch>> {
    let batch_sizes = amudai_arrow_processing::array_sequence::random_split(
        record_count,
        batch_size.start,
        batch_size.end,
    );
    Box::new(batch_sizes.into_iter().map(move |batch_size| {
        generate_constant_f32_batch(schema.clone(), batch_size, constant_f32_value)
    }))
}

/// Generate a single batch with constant f32 values
fn generate_constant_f32_batch(
    schema: Arc<Schema>,
    batch_size: usize,
    constant_f32_value: f32,
) -> RecordBatch {
    let mut columns = Vec::<ArrayRef>::new();

    for field in schema.fields() {
        let array = match field.name().as_str() {
            "value" => {
                // Generate constant f32 values
                let values: Vec<f32> = (0..batch_size).map(|_| constant_f32_value).collect();
                Arc::new(Float32Array::from(values))
            }
            _ => {
                // Use the normal generator for other fields
                generate_field(field, batch_size)
            }
        };
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns).expect("Failed to create constant f32 batch")
}

/// Generate batches with constant f64 values for testing
fn generate_constant_f64_batches(
    schema: Arc<Schema>,
    batch_size: std::ops::Range<usize>,
    record_count: usize,
    constant_f64_value: f64,
) -> Box<dyn Iterator<Item = RecordBatch>> {
    let batch_sizes = amudai_arrow_processing::array_sequence::random_split(
        record_count,
        batch_size.start,
        batch_size.end,
    );
    Box::new(batch_sizes.into_iter().map(move |batch_size| {
        generate_constant_f64_batch(schema.clone(), batch_size, constant_f64_value)
    }))
}

/// Generate a single batch with constant f64 values
fn generate_constant_f64_batch(
    schema: Arc<Schema>,
    batch_size: usize,
    constant_f64_value: f64,
) -> RecordBatch {
    let mut columns = Vec::<ArrayRef>::new();

    for field in schema.fields() {
        let array = match field.name().as_str() {
            "value" => {
                // Generate constant f64 values
                let values: Vec<f64> = (0..batch_size).map(|_| constant_f64_value).collect();
                Arc::new(Float64Array::from(values))
            }
            _ => {
                // Use the normal generator for other fields
                generate_field(field, batch_size)
            }
        };
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns).expect("Failed to create constant f64 batch")
}
