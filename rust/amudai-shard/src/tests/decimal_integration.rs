use amudai_blockstream::read::block_stream::empty_hint;
use amudai_common::Result;
use amudai_format::schema::BasicType;
use arrow_array::{ArrayRef, FixedSizeBinaryArray, RecordBatch, RecordBatchIterator};
use std::sync::Arc;

use crate::{
    read::shard::ShardOptions,
    tests::{
        data_generator::{
            create_decimal_test_schema, create_mixed_schema_with_decimals, generate_batches,
            generate_field,
        },
        shard_store::ShardStore,
    },
};

#[test]
fn test_decimal_shard_basic_ingest() -> Result<()> {
    let shard_store = ShardStore::new();

    // Create schema with decimal fields
    let schema = create_decimal_test_schema();

    // Generate test data with decimal values
    let batches = generate_batches(schema.clone(), 50..100, 1000);
    let batch_iter = RecordBatchIterator::new(batches.map(Ok), schema.clone());

    // Write to shard - this should succeed without errors
    let data_ref = shard_store.ingest_shard_from_record_batches(batch_iter);

    // Verify shard was created and can be opened
    let shard = shard_store.open_shard(&data_ref.url);

    // Verify basic shard properties
    assert_eq!(shard.directory().total_record_count, 1000);
    assert_eq!(shard.directory().stripe_count, 1);
    // Verify schema preservation at shard level
    let shard_schema = shard.fetch_schema()?;
    assert_eq!(shard_schema.field_list()?.len()?, 4); // id, amount, price, description

    // Verify decimal fields exist and have correct metadata
    let amount_field = shard_schema.find_field("amount")?;
    assert!(amount_field.is_some());

    let price_field = shard_schema.find_field("price")?;
    assert!(price_field.is_some());

    println!("✓ Decimal shard basic ingest test passed!");
    Ok(())
}

#[test]
fn test_decimal_stripe_level_access() -> Result<()> {
    let shard_store = ShardStore::new();

    // Create schema with decimal fields
    let schema = create_decimal_test_schema();

    // Generate test data
    let batches = generate_batches(schema.clone(), 80..120, 500);
    let batch_iter = RecordBatchIterator::new(batches.map(Ok), schema.clone());
    let data_ref = shard_store.ingest_shard_from_record_batches(batch_iter);

    // Open shard and stripe
    let shard = shard_store.open_shard(&data_ref.url);
    let stripe = shard.open_stripe(0)?;

    // Verify stripe schema includes decimal fields
    let stripe_schema = stripe.fetch_schema()?;
    let amount_field_info = stripe_schema.find_field("amount")?.unwrap();
    let price_field_info = stripe_schema.find_field("price")?.unwrap();

    // Open the decimal fields
    let amount_field = stripe.open_field(amount_field_info.1.data_type().unwrap())?;
    let price_field = stripe.open_field(price_field_info.1.data_type().unwrap())?;

    // Verify field types
    assert_eq!(
        amount_field.data_type().basic_type()?,
        BasicType::FixedSizeBinary
    );
    assert_eq!(
        price_field.data_type().basic_type()?,
        BasicType::FixedSizeBinary
    );

    // Verify both fields have data
    assert!(amount_field.descriptor().field.is_some());
    assert!(price_field.descriptor().field.is_some());

    let amount_desc = amount_field.descriptor().field.as_ref().unwrap();
    let price_desc = price_field.descriptor().field.as_ref().unwrap();

    assert_eq!(amount_desc.position_count, 500);
    assert_eq!(price_desc.position_count, 500);

    println!("✓ Decimal stripe level access test passed!");
    Ok(())
}

#[test]
fn test_decimal_mixed_with_other_types() -> Result<()> {
    let shard_store = ShardStore::new();

    // Create schema with decimals mixed with other types
    let schema = create_mixed_schema_with_decimals();

    // Generate test data
    let batches = generate_batches(schema.clone(), 80..120, 750);
    let batch_iter = RecordBatchIterator::new(batches.map(Ok), schema.clone());

    // Write to shard
    let data_ref = shard_store.ingest_shard_from_record_batches(batch_iter);

    // Verify shard properties
    let shard = shard_store.open_shard(&data_ref.url);
    assert_eq!(shard.directory().total_record_count, 750);
    // Verify schema has the correct mix of field types
    let shard_schema = shard.fetch_schema()?;
    assert_eq!(shard_schema.field_list()?.len()?, 6); // id, name, balance, active, score, metadata

    // Verify each field exists
    assert!(shard_schema.find_field("id")?.is_some());
    assert!(shard_schema.find_field("name")?.is_some());
    assert!(shard_schema.find_field("balance")?.is_some()); // decimal
    assert!(shard_schema.find_field("active")?.is_some());
    assert!(shard_schema.find_field("score")?.is_some()); // decimal
    assert!(shard_schema.find_field("metadata")?.is_some());

    // Open stripe and verify decimal fields can be accessed
    let stripe = shard.open_stripe(0)?;
    let stripe_schema = stripe.fetch_schema()?;

    let balance_field_info = stripe_schema.find_field("balance")?.unwrap();
    let score_field_info = stripe_schema.find_field("score")?.unwrap();

    let balance_field = stripe.open_field(balance_field_info.1.data_type().unwrap())?;
    let score_field = stripe.open_field(score_field_info.1.data_type().unwrap())?;

    assert_eq!(
        balance_field.data_type().basic_type()?,
        BasicType::FixedSizeBinary
    );
    assert_eq!(
        score_field.data_type().basic_type()?,
        BasicType::FixedSizeBinary
    );

    println!("✓ Decimal mixed with other types test passed!");
    Ok(())
}

#[test]
fn test_large_decimal_dataset() -> Result<()> {
    let shard_store = ShardStore::new();

    // Create schema with decimal fields
    let schema = create_decimal_test_schema();

    // Generate a larger dataset to test performance and compression
    let large_record_count = 10000;
    let batches = generate_batches(schema.clone(), 200..300, large_record_count);
    let batch_iter = RecordBatchIterator::new(batches.map(Ok), schema.clone());

    // Write to shard
    let data_ref = shard_store.ingest_shard_from_record_batches(batch_iter);

    // Verify shard was created successfully
    let shard = shard_store.open_shard(&data_ref.url);
    assert_eq!(
        shard.directory().total_record_count as usize,
        large_record_count
    );

    // Verify we can access the decimal fields in the large dataset
    let stripe = shard.open_stripe(0)?;
    let stripe_schema = stripe.fetch_schema()?;

    let amount_field_info = stripe_schema.find_field("amount")?.unwrap();
    let price_field_info = stripe_schema.find_field("price")?.unwrap();

    let amount_field = stripe.open_field(amount_field_info.1.data_type().unwrap())?;
    let price_field = stripe.open_field(price_field_info.1.data_type().unwrap())?;

    // Verify field descriptors show correct counts
    let amount_desc = amount_field.descriptor().field.as_ref().unwrap();
    let price_desc = price_field.descriptor().field.as_ref().unwrap();

    assert_eq!(amount_desc.position_count as usize, large_record_count);
    assert_eq!(price_desc.position_count as usize, large_record_count);

    println!("✓ Large decimal dataset test passed with {large_record_count} records!");
    Ok(())
}

#[test]
fn test_decimal_field_statistics() -> Result<()> {
    let shard_store = ShardStore::new();

    // Create schema with decimal fields
    let schema = create_decimal_test_schema();

    // Generate test data
    let batches = generate_batches(schema.clone(), 100..150, 1000);
    let batch_iter = RecordBatchIterator::new(batches.map(Ok), schema.clone());
    let data_ref = shard_store.ingest_shard_from_record_batches(batch_iter);

    // Open shard and check field statistics
    let shard = shard_store.open_shard(&data_ref.url);
    let stripe = shard.open_stripe(0)?;
    let stripe_schema = stripe.fetch_schema()?;

    let amount_field_info = stripe_schema.find_field("amount")?.unwrap();
    let amount_field = stripe.open_field(amount_field_info.1.data_type().unwrap())?;

    // Verify field statistics were collected
    let field_desc = amount_field.descriptor().field.as_ref().unwrap();
    assert_eq!(field_desc.position_count, 1000);

    // Check if decimal-specific statistics exist
    if let Some(amudai_format::defs::shard::field_descriptor::TypeSpecific::DecimalStats(
        decimal_stats,
    )) = &field_desc.type_specific
    {
        // We should have decimal statistics
        println!("Decimal statistics found:");
        println!("  Zero count: {}", decimal_stats.zero_count);
        println!("  Positive count: {}", decimal_stats.positive_count);
        println!("  Negative count: {}", decimal_stats.negative_count);
    }

    println!("✓ Decimal field statistics test passed!");
    Ok(())
}

#[test]
fn test_multiple_stripes_with_decimals() -> Result<()> {
    let shard_store = ShardStore::new();
    let schema = create_decimal_test_schema();

    // Create shard builder manually to add multiple stripes
    let mut shard_builder = shard_store.create_shard_builder(&schema);

    // Add multiple stripes with decimal data
    for stripe_num in 0..3 {
        let mut stripe_builder = shard_builder.build_stripe()?;

        // Generate data for this stripe
        let batches = generate_batches(schema.clone(), 100..150, 500);
        for batch in batches {
            stripe_builder.push_batch(&batch)?;
        }

        let stripe = stripe_builder.finish()?;
        shard_builder.add_stripe(stripe)?;

        println!("Added stripe {} with decimal data", stripe_num + 1);
    }

    // Seal the shard
    let sealed_shard = shard_builder
        .finish()?
        .seal("test:///multi_stripe_decimals.shard")?;

    // Verify the shard properties
    assert_eq!(sealed_shard.directory.total_record_count, 1500); // 3 stripes * 500 records each
    assert_eq!(sealed_shard.directory.stripe_count, 3);

    // Open the shard and verify we can access all stripes
    let shard = ShardOptions::new(shard_store.object_store.clone())
        .open(sealed_shard.directory_ref.url.as_str())?;

    assert_eq!(shard.stripe_count(), 3);

    // Verify each stripe has decimal fields
    for stripe_idx in 0..3 {
        let stripe = shard.open_stripe(stripe_idx)?;
        let stripe_schema = stripe.fetch_schema()?;

        let amount_field_info = stripe_schema.find_field("amount")?.unwrap();
        let amount_field = stripe.open_field(amount_field_info.1.data_type().unwrap())?;

        let field_desc = amount_field.descriptor().field.as_ref().unwrap();
        assert_eq!(field_desc.position_count, 500);

        println!("Verified stripe {stripe_idx} has decimal data");
    }

    println!("✓ Multiple stripes with decimals test passed!");
    Ok(())
}

#[test]
fn test_decimal_field_with_nan_values() -> Result<()> {
    use arrow_array::{RecordBatch, builder::FixedSizeBinaryBuilder};
    use arrow_schema::{DataType, Field, Schema};
    use decimal::d128;
    use std::sync::Arc;

    let shard_store = ShardStore::new(); // Create a decimal field with proper KustoDecimal metadata
    let schema = Arc::new(Schema::new(vec![
        Field::new("decimal_with_nans", DataType::FixedSizeBinary(16), true).with_metadata(
            [(
                "ARROW:extension:name".to_string(),
                "KustoDecimal".to_string(),
            )]
            .into(),
        ),
    ]));

    // Create test data with various values including NaN
    let zero = d128::from(0);
    let nan_val = zero / zero;

    let test_values = vec![
        d128::from(123),  // Positive value
        d128::from(0),    // Zero value
        nan_val,          // NaN value
        d128::from(-456), // Negative value
        nan_val,          // Another NaN value
        d128::from(789),  // Another positive value
    ];

    // Convert to binary representation for FixedSizeBinary array
    let mut builder = FixedSizeBinaryBuilder::new(16);
    for value in &test_values {
        let raw_bytes = value.to_raw_bytes();
        builder.append_value(raw_bytes).unwrap();
    }

    // Create record batch
    let decimal_array = Arc::new(builder.finish());
    let batch = RecordBatch::try_new(schema.clone(), vec![decimal_array]).unwrap();

    // Ingest the data
    let batch_iter = RecordBatchIterator::new([Ok(batch)], schema.clone());
    let data_ref = shard_store.ingest_shard_from_record_batches(batch_iter);

    // Open shard and verify basic properties
    let shard = shard_store.open_shard(&data_ref.url);
    assert_eq!(shard.directory().total_record_count, 6);

    // Open stripe and get field statistics
    let stripe = shard.open_stripe(0)?;
    let stripe_schema = stripe.fetch_schema()?;
    let field_info = stripe_schema.find_field("decimal_with_nans")?.unwrap();
    let field = stripe.open_field(field_info.1.data_type().unwrap())?;

    // Verify field statistics
    let field_desc = field.descriptor().field.as_ref().unwrap();
    assert_eq!(field_desc.position_count, 6); // Check decimal-specific statistics
    if let Some(ref type_specific) = field_desc.type_specific {
        if let amudai_format::defs::shard::field_descriptor::TypeSpecific::DecimalStats(
            decimal_stats,
        ) = type_specific
        {
            println!("Decimal statistics with NaN values:");
            println!("  Total count: {}", field_desc.position_count);
            println!("  Zero count: {}", decimal_stats.zero_count);
            println!("  Positive count: {}", decimal_stats.positive_count);
            println!("  Negative count: {}", decimal_stats.negative_count);
            println!("  NaN count: {}", decimal_stats.nan_count);

            // Verify the statistics are correct
            assert_eq!(decimal_stats.zero_count, 1); // One zero
            assert_eq!(decimal_stats.positive_count, 2); // 123, 789
            assert_eq!(decimal_stats.negative_count, 1); // -456
            assert_eq!(decimal_stats.nan_count, 2); // Two NaN values

            // Total non-NaN values should equal zero + positive + negative
            assert_eq!(
                decimal_stats.zero_count
                    + decimal_stats.positive_count
                    + decimal_stats.negative_count,
                field_desc.position_count - decimal_stats.nan_count
            );
        } else {
            panic!("Expected DecimalStats in type_specific field, but got a different type");
        }
    } else {
        panic!("Expected type_specific statistics for decimal field");
    }

    println!("✓ Decimal field with NaN values test passed!");
    Ok(())
}

#[test]
fn test_decimal_end_to_end_read_back() -> Result<()> {
    use arrow_array::{RecordBatch, builder::FixedSizeBinaryBuilder};
    use arrow_schema::{DataType, Field, Schema};
    use decimal::d128;
    use std::sync::Arc;

    let shard_store = ShardStore::new();

    // Create a decimal field with proper KustoDecimal metadata
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("amount", DataType::FixedSizeBinary(16), true).with_metadata(
            [(
                "ARROW:extension:name".to_string(),
                "KustoDecimal".to_string(),
            )]
            .into(),
        ),
    ]));

    // Create test data with known decimal values
    let test_decimals = vec![
        d128::from(123456789),                   // Large positive
        d128::from(0),                           // Zero
        d128::from(-987654321),                  // Large negative
        d128::from(42) / d128::from(100),        // Fractional: 0.42
        d128::from(314159) / d128::from(100000), // Pi approximation: 3.14159
    ];

    // Create record batch with decimal values
    let id_array = Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3, 4, 5]));

    let mut decimal_builder = FixedSizeBinaryBuilder::new(16);
    for decimal_val in &test_decimals {
        let raw_bytes = decimal_val.to_raw_bytes();
        decimal_builder.append_value(raw_bytes).unwrap();
    }
    let decimal_array = Arc::new(decimal_builder.finish());

    let batch = RecordBatch::try_new(schema.clone(), vec![id_array, decimal_array]).unwrap();

    // Ingest the data
    let batch_iter = RecordBatchIterator::new([Ok(batch)], schema.clone());
    let data_ref = shard_store.ingest_shard_from_record_batches(batch_iter);

    // Open shard and read back the decimal values
    let shard = shard_store.open_shard(&data_ref.url);
    let stripe = shard.open_stripe(0)?;
    let stripe_schema = stripe.fetch_schema()?;

    let amount_field_info = stripe_schema.find_field("amount")?.unwrap();
    let amount_field = stripe.open_field(amount_field_info.1.data_type().unwrap())?;

    // Create decoder and read back the values
    let mut amount_decoder = amount_field.create_decoder()?.create_reader(empty_hint())?;

    let amount_seq = amount_decoder.read_range(0..5)?;

    // Verify we got all 5 values (no nulls)
    assert_eq!(amount_seq.presence.count_non_nulls(), 5);

    // Extract the raw bytes and convert back to decimals
    let decoded_bytes = amount_seq.values.as_slice::<u8>();
    assert_eq!(decoded_bytes.len(), 5 * 16); // 5 decimals * 16 bytes each

    // Verify each decimal value matches what we put in
    for (i, expected_decimal) in test_decimals.iter().enumerate() {
        let start = i * 16;
        let end = start + 16;
        let decimal_bytes = &decoded_bytes[start..end];

        // Convert bytes back to d128 and compare
        let decoded_decimal = unsafe { d128::from_raw_bytes(decimal_bytes.try_into().unwrap()) };

        assert_eq!(
            decoded_decimal, *expected_decimal,
            "Decimal value mismatch at position {i}: expected {expected_decimal}, got {decoded_decimal}"
        );
    }

    println!("✓ End-to-end decimal read-back test passed!");
    println!(
        "  Successfully read back {} decimal values:",
        test_decimals.len()
    );
    for (i, decimal) in test_decimals.iter().enumerate() {
        println!("    [{i}] = {decimal}");
    }

    Ok(())
}

#[test]
fn test_decimal_with_index_end_to_end() -> Result<()> {
    use arrow_array::{RecordBatch, builder::FixedSizeBinaryBuilder};
    use arrow_schema::{DataType, Field, Schema};
    use decimal::d128;
    use std::sync::Arc;

    let shard_store = ShardStore::new();

    // Create a decimal field with proper KustoDecimal metadata
    let schema = Arc::new(Schema::new(vec![
        Field::new("amount", DataType::FixedSizeBinary(16), false).with_metadata(
            [(
                "ARROW:extension:name".to_string(),
                "KustoDecimal".to_string(),
            )]
            .into(),
        ),
    ]));

    // Create test data with a larger set for indexing
    let mut test_decimals = Vec::new();
    for i in 0..1000 {
        test_decimals.push(d128::from(i * 123 + 456)); // Generate varied values
    }

    // Create record batch with decimal values
    let mut decimal_builder = FixedSizeBinaryBuilder::new(16);
    for decimal_val in &test_decimals {
        let raw_bytes = decimal_val.to_raw_bytes();
        decimal_builder.append_value(raw_bytes).unwrap();
    }
    let decimal_array = Arc::new(decimal_builder.finish());

    let batch = RecordBatch::try_new(schema.clone(), vec![decimal_array]).unwrap();

    // Ingest the data
    let batch_iter = RecordBatchIterator::new([Ok(batch)], schema.clone());
    let data_ref = shard_store.ingest_shard_from_record_batches(batch_iter);

    // Open shard and examine the buffers
    let shard = shard_store.open_shard(&data_ref.url);
    let stripe = shard.open_stripe(0)?;
    let stripe_schema = stripe.fetch_schema()?;

    let amount_field_info = stripe_schema.find_field("amount")?.unwrap();
    let amount_field = stripe.open_field(amount_field_info.1.data_type().unwrap())?;

    // Check how many buffers this field has (should be 2: data + index)
    let encoded_buffers = amount_field.get_encoded_buffers()?;
    println!(
        "Decimal field has {} encoded buffers:",
        encoded_buffers.len()
    );
    for (i, buffer) in encoded_buffers.iter().enumerate() {
        println!("  Buffer {}: {:?}", i, buffer.kind());
    }

    // Verify we have both data and index buffers
    assert_eq!(
        encoded_buffers.len(),
        2,
        "Expected 2 buffers (data + index) for indexed decimal field"
    );

    let has_data_buffer = encoded_buffers
        .iter()
        .any(|b| b.kind() == amudai_format::defs::shard::BufferKind::Data);
    let has_index_buffer = encoded_buffers
        .iter()
        .any(|b| b.kind() == amudai_format::defs::shard::BufferKind::RangeIndex);

    assert!(has_data_buffer, "Expected data buffer for decimal field");
    assert!(has_index_buffer, "Expected index buffer for decimal field");

    // Create decoder and read back some values
    let mut amount_decoder = amount_field.create_decoder()?.create_reader(empty_hint())?;

    // Read back a subset of values
    let amount_seq = amount_decoder.read_range(10..20)?;

    // Verify we got all 10 values
    assert_eq!(amount_seq.presence.count_non_nulls(), 10);

    // Extract and validate a few values
    let decoded_bytes = amount_seq.values.as_slice::<u8>();
    assert_eq!(decoded_bytes.len(), 10 * 16); // 10 decimals * 16 bytes each

    // Check the first decoded value
    let first_decimal_bytes = &decoded_bytes[0..16];
    let decoded_decimal = unsafe { d128::from_raw_bytes(first_decimal_bytes.try_into().unwrap()) };
    let expected_decimal = test_decimals[10]; // Position 10 in the original data

    assert_eq!(
        decoded_decimal, expected_decimal,
        "First decimal value mismatch: expected {expected_decimal}, got {decoded_decimal}"
    );

    println!("✓ Decimal with index end-to-end test passed!");
    println!(
        "  Successfully handled indexed decimal field with {} buffers",
        encoded_buffers.len()
    );
    println!("  Read back values: first decoded decimal = {decoded_decimal}");

    Ok(())
}

#[test]
fn test_constant_decimal_stripe_integration() -> Result<()> {
    let shard_store = ShardStore::new();

    // Create a schema with decimal fields
    let schema = create_decimal_test_schema();

    // Create a constant decimal value (same as used in unit tests)
    let constant_decimal_bytes = [
        0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD,
        0xEF,
    ];

    // Generate batches with constant decimal values
    let record_count = 1000;
    let batches = generate_constant_decimal_batches(
        schema.clone(),
        50..100,
        record_count,
        constant_decimal_bytes,
    );
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

    // Find the decimal fields
    let amount_field_info = stripe_schema.find_field("amount")?.unwrap();
    let price_field_info = stripe_schema.find_field("price")?.unwrap();

    // Open the decimal fields
    let amount_field = stripe.open_field(amount_field_info.1.data_type().unwrap())?;
    let price_field = stripe.open_field(price_field_info.1.data_type().unwrap())?;

    // Verify field types
    assert_eq!(
        amount_field.data_type().basic_type()?,
        BasicType::FixedSizeBinary
    );
    assert_eq!(
        price_field.data_type().basic_type()?,
        BasicType::FixedSizeBinary
    );

    // Create decoder and reader for the amount field
    let amount_decoder = amount_field.create_decoder()?;
    let mut amount_reader = amount_decoder.create_reader(empty_hint())?;

    // Read a range of values and verify they're all the same constant
    let seq = amount_reader.read_range(0..10)?;
    assert_eq!(seq.len(), 10);

    // For fixed-size decimals, there should be no offsets
    assert!(
        seq.offsets.is_none(),
        "Decimals should not have offsets as they are fixed-size"
    );

    // Verify all values are the same constant decimal
    let values_bytes = seq.values.as_bytes();
    assert_eq!(values_bytes.len(), 10 * 16); // 10 decimals * 16 bytes each

    for i in 0..10 {
        let start = i * 16;
        let end = start + 16;
        let decimal_bytes = &values_bytes[start..end];
        assert_eq!(decimal_bytes, constant_decimal_bytes);
    }

    // Test reading different ranges
    let seq2 = amount_reader.read_range(100..150)?;
    assert_eq!(seq2.len(), 50);

    let values_bytes2 = seq2.values.as_bytes();
    assert_eq!(values_bytes2.len(), 50 * 16);

    // Verify all values in this range are also the same constant
    for i in 0..50 {
        let start = i * 16;
        let end = start + 16;
        let decimal_bytes = &values_bytes2[start..end];
        assert_eq!(decimal_bytes, constant_decimal_bytes);
    }

    // Test the price field as well
    let price_decoder = price_field.create_decoder()?;
    let mut price_reader = price_decoder.create_reader(empty_hint())?;

    let price_seq = price_reader.read_range(0..5)?;
    assert_eq!(price_seq.len(), 5);

    let price_values = price_seq.values.as_bytes();
    for i in 0..5 {
        let start = i * 16;
        let end = start + 16;
        let decimal_bytes = &price_values[start..end];
        assert_eq!(decimal_bytes, constant_decimal_bytes);
    }

    println!("✓ Constant decimal stripe integration test passed!");
    println!("  Successfully created and read constant decimal values from stripe");
    println!("  Verified {record_count} records with constant decimal values");

    Ok(())
}

/// Generate batches with constant decimal values for testing
fn generate_constant_decimal_batches(
    schema: Arc<arrow_schema::Schema>,
    batch_size: std::ops::Range<usize>,
    record_count: usize,
    constant_decimal_bytes: [u8; 16],
) -> Box<dyn Iterator<Item = RecordBatch>> {
    let batch_sizes = amudai_arrow_processing::array_sequence::random_split(
        record_count,
        batch_size.start,
        batch_size.end,
    );
    Box::new(batch_sizes.into_iter().map(move |batch_size| {
        generate_constant_decimal_batch(schema.clone(), batch_size, constant_decimal_bytes)
    }))
}

/// Generate a single batch with constant decimal values
fn generate_constant_decimal_batch(
    schema: Arc<arrow_schema::Schema>,
    batch_size: usize,
    constant_decimal_bytes: [u8; 16],
) -> RecordBatch {
    let mut columns = Vec::<ArrayRef>::new();

    for field in schema.fields() {
        let array = match field.name().as_str() {
            "amount" | "price" => {
                // Generate constant decimal values
                let values: Vec<Option<&[u8]>> = (0..batch_size)
                    .map(|_| Some(constant_decimal_bytes.as_slice()))
                    .collect();
                Arc::new(FixedSizeBinaryArray::from(values))
            }
            _ => {
                // Use the normal generator for other fields
                generate_field(field, batch_size)
            }
        };
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns).expect("Failed to create constant decimal batch")
}
