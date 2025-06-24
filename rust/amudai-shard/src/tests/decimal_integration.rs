use amudai_common::Result;
use amudai_format::schema::BasicType;
use arrow_array::RecordBatchIterator;

use crate::{
    read::shard::ShardOptions,
    tests::{
        data_generator::{
            create_decimal_test_schema, create_mixed_schema_with_decimals, generate_batches,
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

    println!(
        "✓ Large decimal dataset test passed with {} records!",
        large_record_count
    );
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
    if let Some(ref type_specific) = field_desc.type_specific {
        if let amudai_format::defs::shard::field_descriptor::TypeSpecific::DecimalStats(
            decimal_stats,
        ) = type_specific
        {
            // We should have decimal statistics
            println!("Decimal statistics found:");
            println!("  Zero count: {}", decimal_stats.zero_count);
            println!("  Positive count: {}", decimal_stats.positive_count);
            println!("  Negative count: {}", decimal_stats.negative_count);
        }
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
        .open(sealed_shard.directory_blob.url.as_str())?;

    assert_eq!(shard.stripe_count(), 3);

    // Verify each stripe has decimal fields
    for stripe_idx in 0..3 {
        let stripe = shard.open_stripe(stripe_idx)?;
        let stripe_schema = stripe.fetch_schema()?;

        let amount_field_info = stripe_schema.find_field("amount")?.unwrap();
        let amount_field = stripe.open_field(amount_field_info.1.data_type().unwrap())?;

        let field_desc = amount_field.descriptor().field.as_ref().unwrap();
        assert_eq!(field_desc.position_count, 500);

        println!("Verified stripe {} has decimal data", stripe_idx);
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
