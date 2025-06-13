use crate::write::stripe_builder::{StripeBuilder, StripeBuilderParams};
use amudai_common::{Result, error::Error};
use amudai_encodings::block_encoder::BlockEncodingProfile;
use amudai_format::schema::BasicType;
use amudai_format::schema_builder::{FieldBuilder, SchemaBuilder};
use amudai_io_impl::temp_file_store;
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use std::sync::Arc;

#[test]
fn test_end_to_end_statistics_integration() -> Result<()> {
    // Create schema with a primitive int32 field
    let arrow_field = ArrowField::new("test_field", ArrowDataType::Int32, false);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));

    // Create amudai schema
    let field = FieldBuilder::new("test_field", BasicType::Int32, Some(true), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?; // Create test data with known statistics
    let test_data = vec![10, 5, 20, 15, 30]; // min=5, max=30, count=5
    let array = Arc::new(Int32Array::from(test_data));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array])
        .map_err(|e| Error::invalid_arg("batch", format!("Failed to create RecordBatch: {}", e)))?; // Create stripe builder with statistics enabled
    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut stripe_builder = StripeBuilder::new(stripe_params)?;
    stripe_builder.push_batch(&batch)?;
    let prepared_stripe = stripe_builder.finish()?; // Verify that the field descriptor contains statistics
    let field_data_type = schema.find_field("test_field")?.unwrap().1.data_type()?;
    let schema_id = field_data_type.schema_id()?;
    let field_descriptor = &prepared_stripe.fields.get(schema_id).unwrap().descriptor;

    assert_eq!(field_descriptor.position_count, 5);
    assert_eq!(field_descriptor.null_count, Some(0)); // No nulls
    assert!(field_descriptor.nan_count.is_none()); // No NaNs for integers

    // Verify range statistics
    let range_stats = field_descriptor.range_stats.as_ref().unwrap();
    assert!(range_stats.min_value.is_some());
    assert!(range_stats.max_value.is_some());

    // Check min/max values
    let min_val = range_stats.min_value.as_ref().unwrap();
    let max_val = range_stats.max_value.as_ref().unwrap();

    // Extract values from AnyValue
    use amudai_format::defs::common::any_value::Kind;
    if let Some(Kind::I64Value(min)) = &min_val.kind {
        assert_eq!(*min, 5);
    } else {
        panic!("Expected I64Value for min");
    }

    if let Some(Kind::I64Value(max)) = &max_val.kind {
        assert_eq!(*max, 30);
    } else {
        panic!("Expected I64Value for max");
    }

    println!("✓ End-to-end statistics integration test passed!");
    Ok(())
}

#[test]
fn test_statistics_aggregation_across_stripes() -> Result<()> {
    // This test simulates multiple stripes with different statistics
    // and verifies they are properly aggregated at the shard level
    // Test data for different stripes
    let stripe1_data = vec![1, 3, 5]; // min=1, max=5
    let stripe2_data = vec![2, 8, 10]; // min=2, max=10
    let stripe3_data = vec![0, 15, 25]; // min=0, max=25

    // Overall expected: min=0, max=25, total_count=9, null_count=0

    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

    // Create schema
    let arrow_field = ArrowField::new("test_field", ArrowDataType::Int32, false);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));
    let field = FieldBuilder::new("test_field", BasicType::Int32, Some(true), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    let mut field_descriptors = Vec::new(); // Process each stripe and collect field descriptors
    for (i, data) in [stripe1_data, stripe2_data, stripe3_data]
        .iter()
        .enumerate()
    {
        let array = Arc::new(Int32Array::from(data.clone()));
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array]).map_err(|e| {
            Error::invalid_arg("batch", format!("Failed to create RecordBatch: {}", e))
        })?;

        let stripe_params = StripeBuilderParams {
            schema: schema.clone(),
            temp_store: temp_store.clone(),
            encoding_profile: BlockEncodingProfile::default(),
        };

        let mut stripe_builder = StripeBuilder::new(stripe_params)?;
        stripe_builder.push_batch(&batch)?;
        let prepared_stripe = stripe_builder.finish()?;

        let field_desc = {
            let field_data_type = schema.find_field("test_field")?.unwrap().1.data_type()?;
            let schema_id = field_data_type.schema_id()?;
            prepared_stripe
                .fields
                .get(schema_id)
                .unwrap()
                .descriptor
                .clone()
        };
        field_descriptors.push(field_desc);

        println!("Stripe {}: processed {} records", i + 1, data.len());
    } // Simulate shard-level aggregation

    // Convert to Vec<Option<_>> and use merge_field_descriptors
    let field_descriptors_optional: Vec<Option<_>> =
        field_descriptors.into_iter().map(Some).collect();
    let shard_field_desc =
        crate::write::field_descriptor::merge_field_descriptors(field_descriptors_optional)?
            .expect("Should have merged field descriptors successfully");

    // Verify aggregated statistics
    assert_eq!(shard_field_desc.position_count, 9); // 3 + 3 + 3
    assert_eq!(shard_field_desc.null_count, Some(0)); // No nulls across all stripes

    // Verify aggregated range statistics
    let range_stats = shard_field_desc.range_stats.as_ref().unwrap();

    use amudai_format::defs::common::any_value::Kind;
    if let Some(Kind::I64Value(min)) = &range_stats.min_value.as_ref().unwrap().kind {
        assert_eq!(*min, 0); // Minimum across all stripes
    } else {
        panic!("Expected I64Value for aggregated min");
    }

    if let Some(Kind::I64Value(max)) = &range_stats.max_value.as_ref().unwrap().kind {
        assert_eq!(*max, 25); // Maximum across all stripes
    } else {
        panic!("Expected I64Value for aggregated max");
    }

    println!("✓ Statistics aggregation across stripes test passed!");
    Ok(())
}

#[test]
fn test_string_statistics_integration() -> Result<()> {
    // Create schema with a string field
    let arrow_field = ArrowField::new("test_string_field", ArrowDataType::Utf8, false);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));

    // Create amudai schema
    let field = FieldBuilder::new("test_string_field", BasicType::String, Some(false), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    // Create test data with known string characteristics
    let test_strings = vec![
        "hello",         // 5 bytes, ASCII
        "world",         // 5 bytes, ASCII
        "test",          // 4 bytes, ASCII
        "a",             // 1 byte, ASCII - min size
        "longer string", // 13 bytes, ASCII - max size
    ];
    let array = Arc::new(arrow_array::StringArray::from(test_strings.clone()));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array])
        .map_err(|e| Error::invalid_arg("batch", format!("Failed to create RecordBatch: {}", e)))?;

    // Create stripe builder
    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut stripe_builder = StripeBuilder::new(stripe_params)?;
    stripe_builder.push_batch(&batch)?;
    let prepared_stripe = stripe_builder.finish()?;

    // Verify that the field descriptor contains string statistics
    let field_data_type = schema
        .find_field("test_string_field")?
        .unwrap()
        .1
        .data_type()?;
    let schema_id = field_data_type.schema_id()?;
    let field_descriptor = &prepared_stripe.fields.get(schema_id).unwrap().descriptor;

    assert_eq!(field_descriptor.position_count, 5);
    assert_eq!(field_descriptor.null_count, Some(0)); // No nulls

    // Verify string-specific statistics
    if let Some(amudai_format::defs::shard::field_descriptor::TypeSpecific::StringStats(
        string_stats,
    )) = &field_descriptor.type_specific
    {
        assert_eq!(string_stats.min_size, 1); // "a"
        assert_eq!(string_stats.max_size, 13); // "longer string"
        assert_eq!(string_stats.min_non_empty_size, Some(1)); // "a"
        assert_eq!(string_stats.ascii_count, Some(5)); // All strings are ASCII
    } else {
        panic!("Expected string statistics in type_specific field");
    }

    println!("✓ String statistics integration test passed!");
    Ok(())
}

#[test]
fn test_string_statistics_aggregation_across_stripes() -> Result<()> {
    // Create schema with a string field
    let arrow_field = ArrowField::new("test_strings", ArrowDataType::Utf8, false);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));

    // Create amudai schema
    let field = FieldBuilder::new("test_strings", BasicType::String, Some(false), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    let mut field_descriptors = Vec::new();

    // Create multiple stripes with different string characteristics
    let stripe_data = vec![
        vec!["a", "bb", "ccc"],        // Stripe 1: min=1, max=3, ascii_count=3
        vec!["dddd", "eeeee"],         // Stripe 2: min=4, max=5, ascii_count=2
        vec!["", "ffffff", "ggggggg"], // Stripe 3: min=0, max=7, ascii_count=3
    ];

    for (i, strings) in stripe_data.iter().enumerate() {
        let array = Arc::new(arrow_array::StringArray::from(strings.clone()));
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array]).map_err(|e| {
            Error::invalid_arg("batch", format!("Failed to create RecordBatch: {}", e))
        })?;

        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let stripe_params = StripeBuilderParams {
            schema: schema.clone(),
            temp_store: temp_store.clone(),
            encoding_profile: BlockEncodingProfile::default(),
        };

        let mut stripe_builder = StripeBuilder::new(stripe_params)?;
        stripe_builder.push_batch(&batch)?;
        let prepared_stripe = stripe_builder.finish()?;

        let field_desc = {
            let field_data_type = schema.find_field("test_strings")?.unwrap().1.data_type()?;
            let schema_id = field_data_type.schema_id()?;
            prepared_stripe
                .fields
                .get(schema_id)
                .unwrap()
                .descriptor
                .clone()
        };
        field_descriptors.push(field_desc);

        println!("Stripe {}: processed {} strings", i + 1, strings.len());
    }

    // Simulate shard-level aggregation using our merge function
    let field_descriptors_optional: Vec<Option<_>> =
        field_descriptors.into_iter().map(Some).collect();
    let shard_field_desc =
        crate::write::field_descriptor::merge_field_descriptors(field_descriptors_optional)?
            .expect("Should have merged field descriptors successfully");

    // Verify aggregated position count and null count
    assert_eq!(shard_field_desc.position_count, 8); // 3 + 2 + 3
    assert_eq!(shard_field_desc.null_count, Some(0)); // No nulls across all stripes

    // Verify aggregated string statistics
    if let Some(amudai_format::defs::shard::field_descriptor::TypeSpecific::StringStats(
        string_stats,
    )) = &shard_field_desc.type_specific
    {
        assert_eq!(string_stats.min_size, 0); // Empty string in stripe 3
        assert_eq!(string_stats.max_size, 7); // "ggggggg" in stripe 3
        assert_eq!(string_stats.min_non_empty_size, Some(1)); // "a" in stripe 1
        assert_eq!(string_stats.ascii_count, Some(8)); // 3 + 2 + 3, all strings are ASCII
    } else {
        panic!("Expected string statistics in type_specific field");
    }

    println!("✓ String statistics aggregation across stripes test passed!");
    Ok(())
}
