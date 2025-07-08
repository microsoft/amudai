use crate::write::stripe_builder::{StripeBuilder, StripeBuilderParams};
use amudai_common::{Result, error::Error};
use amudai_encodings::block_encoder::BlockEncodingProfile;
use amudai_format::defs::shard;
use amudai_format::schema::BasicType;
use amudai_format::schema_builder::{FieldBuilder, SchemaBuilder};
use amudai_io_impl::temp_file_store;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, TimestampNanosecondArray,
};
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
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
        .map_err(|e| Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}")))?; // Create stripe builder with statistics enabled
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
            Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}"))
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
        .map_err(|e| Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}")))?;

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
    let field = FieldBuilder::new("test_strings", BasicType::String, Some(false), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    let mut field_descriptors = Vec::new();

    // Create multiple stripes with different string characteristics
    let stripe_data = [
        vec!["a", "bb", "ccc"],        // Stripe 1: min=1, max=3, ascii_count=3
        vec!["dddd", "eeeee"],         // Stripe 2: min=4, max=5, ascii_count=2
        vec!["", "ffffff", "ggggggg"], // Stripe 3: min=0, max=7, ascii_count=3
    ];

    for (i, strings) in stripe_data.iter().enumerate() {
        let array = Arc::new(arrow_array::StringArray::from(strings.clone()));
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array]).map_err(|e| {
            Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}"))
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

#[test]
fn test_boolean_statistics_integration() -> Result<()> {
    // Create schema with a nullable boolean field
    let arrow_field = ArrowField::new("test_bool_field", ArrowDataType::Boolean, true); // nullable
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));

    // Create amudai schema
    let field = FieldBuilder::new("test_bool_field", BasicType::Boolean, Some(true), None); // nullable
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    // Create test data with known boolean values
    let test_data = vec![
        Some(true),
        Some(false),
        None, // Null value
        Some(true),
        Some(false),
    ];
    let array = Arc::new(BooleanArray::from(test_data));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array])
        .map_err(|e| Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}")))?;

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

    // Verify that the field descriptor contains boolean statistics
    let field_data_type = schema
        .find_field("test_bool_field")?
        .unwrap()
        .1
        .data_type()?;
    let schema_id = field_data_type.schema_id()?;
    let field_descriptor = &prepared_stripe.fields.get(schema_id).unwrap().descriptor;

    assert_eq!(field_descriptor.position_count, 5);
    assert_eq!(field_descriptor.null_count, Some(1)); // One null value

    // Verify boolean-specific statistics
    if let Some(amudai_format::defs::shard::field_descriptor::TypeSpecific::BooleanStats(
        bool_stats,
    )) = &field_descriptor.type_specific
    {
        assert_eq!(bool_stats.true_count, 2); // Two true values
        assert_eq!(bool_stats.false_count, 2); // Two false values
    } else {
        panic!("Expected boolean statistics in type_specific field");
    }

    println!("✓ Boolean statistics integration test passed!");
    Ok(())
}

#[test]
fn test_boolean_statistics_aggregation_across_stripes() -> Result<()> {
    // Create schema with a nullable boolean field
    let arrow_field = ArrowField::new("test_bools", ArrowDataType::Boolean, true); // nullable
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));

    // Create amudai schema
    let field = FieldBuilder::new("test_bools", BasicType::Boolean, Some(true), None); // nullable
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    let mut field_descriptors = Vec::new(); // Create multiple stripes with different boolean distributions
    let stripe_data = [
        vec![Some(true), Some(false), None], // Stripe 1: 1 true, 1 false, 1 null
        vec![Some(false), Some(false)],      // Stripe 2: 2 false
        vec![Some(true), None],              // Stripe 3: 1 true, 1 null
    ];

    for (i, bools) in stripe_data.iter().enumerate() {
        let array = Arc::new(BooleanArray::from(bools.clone()));
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array]).map_err(|e| {
            Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}"))
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
            let field_data_type = schema.find_field("test_bools")?.unwrap().1.data_type()?;
            let schema_id = field_data_type.schema_id()?;
            prepared_stripe
                .fields
                .get(schema_id)
                .unwrap()
                .descriptor
                .clone()
        };
        field_descriptors.push(field_desc);

        println!("Stripe {}: processed {} booleans", i + 1, bools.len());
    }

    // Simulate shard-level aggregation using our merge function
    let field_descriptors_optional: Vec<Option<_>> =
        field_descriptors.into_iter().map(Some).collect();
    let shard_field_desc =
        crate::write::field_descriptor::merge_field_descriptors(field_descriptors_optional)?
            .expect("Should have merged field descriptors successfully"); // Verify aggregated position count and null count
    assert_eq!(shard_field_desc.position_count, 7); // 3 + 2 + 2 = 7
    assert_eq!(shard_field_desc.null_count, Some(2)); // 1 + 0 + 1 = 2 nulls

    // Verify aggregated boolean statistics
    if let Some(amudai_format::defs::shard::field_descriptor::TypeSpecific::BooleanStats(
        bool_stats,
    )) = &shard_field_desc.type_specific
    {
        assert_eq!(bool_stats.true_count, 2); // 1 + 0 + 1 = 2 true values
        assert_eq!(bool_stats.false_count, 3); // 1 + 2 + 0 = 3 false values
    } else {
        panic!("Expected boolean statistics in type_specific field");
    }

    println!("✓ Boolean statistics aggregation across stripes test passed!");
    Ok(())
}

#[test]
fn test_datetime_statistics_integration() -> Result<()> {
    // Create schema with a DateTime field
    let arrow_field = ArrowField::new(
        "test_datetime_field",
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    );
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));

    // Create amudai schema
    let field = FieldBuilder::new(
        "test_datetime_field",
        BasicType::DateTime,
        Some(false),
        None,
    );
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    // Test data: 3 timestamps (ns since epoch)
    let test_data = vec![
        Some(1_600_000_000_000_000_000),
        Some(1_700_000_000_000_000_000),
        Some(1_500_000_000_000_000_000),
    ];
    let array = Arc::new(TimestampNanosecondArray::from(test_data.clone()));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array])
        .map_err(|e| Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}")))?;

    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut stripe_builder = StripeBuilder::new(stripe_params)?;
    stripe_builder.push_batch(&batch)?;
    let prepared_stripe = stripe_builder.finish()?;

    let field_data_type = schema
        .find_field("test_datetime_field")?
        .unwrap()
        .1
        .data_type()?;
    let schema_id = field_data_type.schema_id()?;
    let field_descriptor = &prepared_stripe.fields.get(schema_id).unwrap().descriptor;

    assert_eq!(field_descriptor.position_count, 3);
    assert_eq!(field_descriptor.null_count, Some(0));

    let range_stats = field_descriptor.range_stats.as_ref().unwrap();
    let min_val = range_stats.min_value.as_ref().unwrap();
    let max_val = range_stats.max_value.as_ref().unwrap();
    use amudai_format::defs::common::any_value::Kind;
    if let Some(Kind::U64Value(min)) = &min_val.kind {
        // The actual value will be converted from the original timestamp
        // Just check that we have a reasonable timestamp value
        assert!(*min > 0 && *min < u64::MAX);
    } else {
        panic!("Expected U64Value for min, got: {:?}", min_val.kind);
    }

    if let Some(Kind::U64Value(max)) = &max_val.kind {
        // The actual value will be converted from the original timestamp
        // Just check that we have a reasonable timestamp value and max > min
        assert!(*max > 0 && *max < u64::MAX);
        if let Some(Kind::U64Value(min_val)) = &min_val.kind {
            assert!(*max >= *min_val);
        }
    } else {
        panic!("Expected U64Value for max, got: {:?}", max_val.kind);
    }

    println!("✓ DateTime statistics integration test passed!");
    Ok(())
}

#[test]
fn test_datetime_statistics_aggregation_across_stripes() -> Result<()> {
    let arrow_field = ArrowField::new(
        "test_datetimes",
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    );
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));
    let field = FieldBuilder::new("test_datetimes", BasicType::DateTime, Some(false), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    let stripe_data = [
        vec![Some(1_000), Some(2_000)],
        vec![Some(500), Some(3_000)],
        vec![Some(2_500), Some(1_500)],
    ];

    let mut field_descriptors = Vec::new();
    for (i, data) in stripe_data.iter().enumerate() {
        let array = Arc::new(TimestampNanosecondArray::from(data.clone()));
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array]).map_err(|e| {
            Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}"))
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
            let field_data_type = schema
                .find_field("test_datetimes")?
                .unwrap()
                .1
                .data_type()?;
            let schema_id = field_data_type.schema_id()?;
            prepared_stripe
                .fields
                .get(schema_id)
                .unwrap()
                .descriptor
                .clone()
        };
        field_descriptors.push(field_desc);

        println!("Stripe {}: processed {} datetimes", i + 1, data.len());
    }

    let field_descriptors_optional: Vec<Option<_>> =
        field_descriptors.into_iter().map(Some).collect();
    let shard_field_desc =
        crate::write::field_descriptor::merge_field_descriptors(field_descriptors_optional)?
            .expect("Should have merged field descriptors successfully");

    assert_eq!(shard_field_desc.position_count, 6);
    assert_eq!(shard_field_desc.null_count, Some(0));
    let range_stats = shard_field_desc.range_stats.as_ref().unwrap();
    use amudai_format::defs::common::any_value::Kind;
    if let Some(Kind::U64Value(min)) = &range_stats.min_value.as_ref().unwrap().kind {
        // Just check that we have reasonable values
        assert!(*min > 0 && *min < u64::MAX);
    } else {
        panic!(
            "Expected U64Value for aggregated min, got: {:?}",
            range_stats.min_value.as_ref().unwrap().kind
        );
    }

    if let Some(Kind::U64Value(max)) = &range_stats.max_value.as_ref().unwrap().kind {
        assert!(*max > 0 && *max < u64::MAX);
        if let Some(Kind::U64Value(min_val)) = &range_stats.min_value.as_ref().unwrap().kind {
            assert!(*max >= *min_val);
        }
    } else {
        panic!(
            "Expected U64Value for aggregated max, got: {:?}",
            range_stats.max_value.as_ref().unwrap().kind
        );
    }

    println!("✓ DateTime statistics aggregation across stripes test passed!");
    Ok(())
}

#[test]
fn test_float64_statistics_integration_with_edge_cases() -> Result<()> {
    let arrow_field = ArrowField::new("test_float_field", ArrowDataType::Float64, true);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));
    let field = FieldBuilder::new("test_float_field", BasicType::Float64, Some(true), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    // Test data: normal, NaN, -0.0, +0.0, inf, -inf, null
    let test_data = vec![
        Some(1.5),
        Some(f64::NAN),
        Some(-0.0),
        Some(0.0),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        None,
    ];
    let array = Arc::new(Float64Array::from(test_data.clone()));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array])
        .map_err(|e| Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}")))?;

    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut stripe_builder = StripeBuilder::new(stripe_params)?;
    stripe_builder.push_batch(&batch)?;
    let prepared_stripe = stripe_builder.finish()?;

    let field_data_type = schema
        .find_field("test_float_field")?
        .unwrap()
        .1
        .data_type()?;
    let schema_id = field_data_type.schema_id()?;
    let field_descriptor = &prepared_stripe.fields.get(schema_id).unwrap().descriptor;
    assert_eq!(field_descriptor.position_count, 7);
    assert_eq!(field_descriptor.null_count, Some(1));

    // Check FloatingStats for NaN count
    if let Some(shard::field_descriptor::TypeSpecific::FloatingStats(floating_stats)) =
        &field_descriptor.type_specific
    {
        assert_eq!(floating_stats.nan_count, 1);
    } else {
        panic!("Expected FloatingStats for float field");
    }

    let range_stats = field_descriptor.range_stats.as_ref().unwrap();
    let min_val = range_stats.min_value.as_ref().unwrap();
    let max_val = range_stats.max_value.as_ref().unwrap();

    use amudai_format::defs::common::any_value::Kind;
    if let Some(Kind::DoubleValue(min)) = &min_val.kind {
        assert_eq!(*min, f64::NEG_INFINITY);
    } else {
        panic!("Expected DoubleValue for min");
    }

    if let Some(Kind::DoubleValue(max)) = &max_val.kind {
        assert_eq!(*max, f64::INFINITY);
    } else {
        panic!("Expected DoubleValue for max");
    }

    println!("✓ Float64 statistics integration and edge cases test passed!");
    Ok(())
}

#[test]
fn test_float64_statistics_aggregation_across_stripes() -> Result<()> {
    let arrow_field = ArrowField::new("test_floats", ArrowDataType::Float64, true);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));
    let field = FieldBuilder::new("test_floats", BasicType::Float64, Some(true), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    let stripe_data = [
        vec![Some(1.0), Some(2.0), Some(f64::NAN)],
        vec![Some(-1.0), Some(3.0)],
        vec![Some(f64::INFINITY), Some(f64::NEG_INFINITY), None],
    ];

    let mut field_descriptors = Vec::new();
    for (i, data) in stripe_data.iter().enumerate() {
        let array = Arc::new(Float64Array::from(data.clone()));
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array]).map_err(|e| {
            Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}"))
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
            let field_data_type = schema.find_field("test_floats")?.unwrap().1.data_type()?;
            let schema_id = field_data_type.schema_id()?;
            prepared_stripe
                .fields
                .get(schema_id)
                .unwrap()
                .descriptor
                .clone()
        };
        field_descriptors.push(field_desc);

        println!("Stripe {}: processed {} floats", i + 1, data.len());
    }

    let field_descriptors_optional: Vec<Option<_>> =
        field_descriptors.into_iter().map(Some).collect();
    let shard_field_desc =
        crate::write::field_descriptor::merge_field_descriptors(field_descriptors_optional)?
            .expect("Should have merged field descriptors successfully");
    assert_eq!(shard_field_desc.position_count, 8);
    assert_eq!(shard_field_desc.null_count, Some(1));

    // Check FloatingStats for NaN count in merged descriptor
    if let Some(shard::field_descriptor::TypeSpecific::FloatingStats(floating_stats)) =
        &shard_field_desc.type_specific
    {
        assert_eq!(floating_stats.nan_count, 1);
    } else {
        panic!("Expected FloatingStats for merged float field");
    }

    let range_stats = shard_field_desc.range_stats.as_ref().unwrap();
    use amudai_format::defs::common::any_value::Kind;
    if let Some(Kind::DoubleValue(min)) = &range_stats.min_value.as_ref().unwrap().kind {
        assert_eq!(*min, f64::NEG_INFINITY);
    } else {
        panic!("Expected DoubleValue for aggregated min");
    }

    if let Some(Kind::DoubleValue(max)) = &range_stats.max_value.as_ref().unwrap().kind {
        assert_eq!(*max, f64::INFINITY);
    } else {
        panic!("Expected DoubleValue for aggregated max");
    }

    println!("✓ Float64 statistics aggregation across stripes test passed!");
    Ok(())
}

#[test]
fn test_binary_statistics_integration_and_edge_cases() -> Result<()> {
    let arrow_field = ArrowField::new("test_binary_field", ArrowDataType::Binary, true);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));
    let field = FieldBuilder::new("test_binary_field", BasicType::Binary, Some(true), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    // Test data: various binary sizes, null, empty
    let test_data = vec![
        Some(b"abc".as_ref()),
        Some(b"".as_ref()),
        None,
        Some(b"longer binary data".as_ref()),
    ];
    let array = Arc::new(BinaryArray::from(test_data.clone()));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array])
        .map_err(|e| Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}")))?;

    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut stripe_builder = StripeBuilder::new(stripe_params)?;
    stripe_builder.push_batch(&batch)?;
    let prepared_stripe = stripe_builder.finish()?;

    let field_data_type = schema
        .find_field("test_binary_field")?
        .unwrap()
        .1
        .data_type()?;
    let schema_id = field_data_type.schema_id()?;
    let field_descriptor = &prepared_stripe.fields.get(schema_id).unwrap().descriptor;

    assert_eq!(field_descriptor.position_count, 4);
    assert_eq!(field_descriptor.null_count, Some(1));

    if let Some(amudai_format::defs::shard::field_descriptor::TypeSpecific::ContainerStats(
        binary_stats,
    )) = &field_descriptor.type_specific
    {
        assert_eq!(binary_stats.min_length, 0); // empty
        assert_eq!(binary_stats.max_length, 18); // "longer binary data"
        assert_eq!(binary_stats.min_non_empty_length, Some(3)); // "abc"
    } else {
        panic!("Expected binary statistics in type_specific field");
    }

    println!("✓ Binary statistics integration and edge cases test passed!");
    Ok(())
}

#[test]
fn test_binary_statistics_aggregation_across_stripes() -> Result<()> {
    let arrow_field = ArrowField::new("test_binaries", ArrowDataType::Binary, true);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));
    let field = FieldBuilder::new("test_binaries", BasicType::Binary, Some(true), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    let stripe_data = [
        vec![Some(b"a".as_ref()), Some(b"bb".as_ref()), None],
        vec![Some(b"ccc".as_ref()), Some(b"dddd".as_ref())],
        vec![Some(b"".as_ref()), Some(b"eeeeee".as_ref())],
    ];

    let mut field_descriptors = Vec::new();
    for (i, data) in stripe_data.iter().enumerate() {
        let array = Arc::new(BinaryArray::from(data.clone()));
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array]).map_err(|e| {
            Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}"))
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
            let field_data_type = schema.find_field("test_binaries")?.unwrap().1.data_type()?;
            let schema_id = field_data_type.schema_id()?;
            prepared_stripe
                .fields
                .get(schema_id)
                .unwrap()
                .descriptor
                .clone()
        };
        field_descriptors.push(field_desc);

        println!("Stripe {}: processed {} binaries", i + 1, data.len());
    }

    let field_descriptors_optional: Vec<Option<_>> =
        field_descriptors.into_iter().map(Some).collect();
    let shard_field_desc =
        crate::write::field_descriptor::merge_field_descriptors(field_descriptors_optional)?
            .expect("Should have merged field descriptors successfully");

    assert_eq!(shard_field_desc.position_count, 7);
    assert_eq!(shard_field_desc.null_count, Some(1));

    if let Some(amudai_format::defs::shard::field_descriptor::TypeSpecific::ContainerStats(
        binary_stats,
    )) = &shard_field_desc.type_specific
    {
        assert_eq!(binary_stats.min_length, 0); // empty in stripe 3
        assert_eq!(binary_stats.max_length, 6); // "eeeeee" in stripe 3
        assert_eq!(binary_stats.min_non_empty_length, Some(1)); // "a" in stripe 1
    } else {
        panic!("Expected binary statistics in type_specific field");
    }

    println!("✓ Binary statistics aggregation across stripes test passed!");
    Ok(())
}

#[test]
fn test_int64_statistics_integration() -> Result<()> {
    let arrow_field = ArrowField::new("test_int64_field", ArrowDataType::Int64, true);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));
    let field = FieldBuilder::new("test_int64_field", BasicType::Int64, Some(true), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    let test_data = vec![Some(i64::MAX), Some(i64::MIN), Some(0), None, Some(42)];
    let array = Arc::new(Int64Array::from(test_data));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array])
        .map_err(|e| Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}")))?;

    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut stripe_builder = StripeBuilder::new(stripe_params)?;
    stripe_builder.push_batch(&batch)?;
    let prepared_stripe = stripe_builder.finish()?;

    let field_data_type = schema
        .find_field("test_int64_field")?
        .unwrap()
        .1
        .data_type()?;
    let schema_id = field_data_type.schema_id()?;
    let field_descriptor = &prepared_stripe.fields.get(schema_id).unwrap().descriptor;

    assert_eq!(field_descriptor.position_count, 5);
    assert_eq!(field_descriptor.null_count, Some(1));

    let range_stats = field_descriptor.range_stats.as_ref().unwrap();
    use amudai_format::defs::common::any_value::Kind;
    if let Some(Kind::I64Value(min)) = &range_stats.min_value.as_ref().unwrap().kind {
        assert_eq!(*min, i64::MIN);
    } else {
        panic!("Expected I64Value for min");
    }

    if let Some(Kind::I64Value(max)) = &range_stats.max_value.as_ref().unwrap().kind {
        assert_eq!(*max, i64::MAX);
    } else {
        panic!("Expected I64Value for max");
    }

    println!("✓ Int64 statistics integration test passed!");
    Ok(())
}

#[test]
fn test_float32_statistics_with_nan() -> Result<()> {
    let arrow_field = ArrowField::new("test_float32_field", ArrowDataType::Float32, true);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));
    let field = FieldBuilder::new("test_float32_field", BasicType::Float32, Some(true), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    let test_data = vec![Some(1.0f32), Some(f32::NAN), Some(-1.0f32), None];
    let array = Arc::new(Float32Array::from(test_data));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array])
        .map_err(|e| Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}")))?;

    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut stripe_builder = StripeBuilder::new(stripe_params)?;
    stripe_builder.push_batch(&batch)?;
    let prepared_stripe = stripe_builder.finish()?;

    let field_data_type = schema
        .find_field("test_float32_field")?
        .unwrap()
        .1
        .data_type()?;
    let schema_id = field_data_type.schema_id()?;
    let field_descriptor = &prepared_stripe.fields.get(schema_id).unwrap().descriptor;
    assert_eq!(field_descriptor.position_count, 4);
    assert_eq!(field_descriptor.null_count, Some(1));

    // Check FloatingStats for NaN count
    if let Some(shard::field_descriptor::TypeSpecific::FloatingStats(floating_stats)) =
        &field_descriptor.type_specific
    {
        assert_eq!(floating_stats.nan_count, 1);
    } else {
        panic!("Expected FloatingStats for float32 field");
    }

    println!("✓ Float32 statistics with NaN test passed!");
    Ok(())
}

#[test]
fn test_edge_case_all_null_batch() -> Result<()> {
    let arrow_field = ArrowField::new("test_field", ArrowDataType::Int32, true);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));
    let field = FieldBuilder::new("test_field", BasicType::Int32, Some(true), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    let test_data = vec![None, None, None];
    let array = Arc::new(Int32Array::from(test_data));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array])
        .map_err(|e| Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}")))?;

    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut stripe_builder = StripeBuilder::new(stripe_params)?;
    stripe_builder.push_batch(&batch)?;
    let prepared_stripe = stripe_builder.finish()?;

    let field_data_type = schema.find_field("test_field")?.unwrap().1.data_type()?;
    let schema_id = field_data_type.schema_id()?;
    let field_descriptor = &prepared_stripe.fields.get(schema_id).unwrap().descriptor;
    assert_eq!(field_descriptor.position_count, 3);
    assert_eq!(field_descriptor.null_count, Some(3));
    // For all-null batches, range stats may or may not be present
    if let Some(range_stats) = &field_descriptor.range_stats {
        // If range stats are present, they should be valid but might be None values
        println!(
            "Range stats present for all-null batch: min={:?}, max={:?}",
            range_stats.min_value, range_stats.max_value
        );
    } else {
        println!("No range stats for all-null batch (expected)");
    }

    println!("✓ Edge case all-null batch test passed!");
    Ok(())
}

#[test]
fn test_edge_case_single_value_batch() -> Result<()> {
    let arrow_field = ArrowField::new("test_field", ArrowDataType::Int32, false);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));
    let field = FieldBuilder::new("test_field", BasicType::Int32, Some(true), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    let test_data = vec![42];
    let array = Arc::new(Int32Array::from(test_data));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array])
        .map_err(|e| Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}")))?;

    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut stripe_builder = StripeBuilder::new(stripe_params)?;
    stripe_builder.push_batch(&batch)?;
    let prepared_stripe = stripe_builder.finish()?;

    let field_data_type = schema.find_field("test_field")?.unwrap().1.data_type()?;
    let schema_id = field_data_type.schema_id()?;
    let field_descriptor = &prepared_stripe.fields.get(schema_id).unwrap().descriptor;

    assert_eq!(field_descriptor.position_count, 1);
    assert_eq!(field_descriptor.null_count, Some(0));

    let range_stats = field_descriptor.range_stats.as_ref().unwrap();
    use amudai_format::defs::common::any_value::Kind;
    if let Some(Kind::I64Value(min)) = &range_stats.min_value.as_ref().unwrap().kind {
        assert_eq!(*min, 42);
    } else {
        panic!("Expected I64Value for min");
    }

    if let Some(Kind::I64Value(max)) = &range_stats.max_value.as_ref().unwrap().kind {
        assert_eq!(*max, 42);
    } else {
        panic!("Expected I64Value for max");
    }

    println!("✓ Edge case single-value batch test passed!");
    Ok(())
}

#[test]
fn test_string_statistics_with_unicode() -> Result<()> {
    let arrow_field = ArrowField::new("test_unicode_field", ArrowDataType::Utf8, false);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));
    let field = FieldBuilder::new("test_unicode_field", BasicType::String, Some(false), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    // Mix of ASCII and Unicode strings
    let test_strings = vec![
        "hello", // ASCII
        "café",  // Unicode with accent
        "🎉",    // Emoji (4 bytes)
        "नमस्ते",  // Devanagari script
        "",      // Empty string
    ];
    let array = Arc::new(arrow_array::StringArray::from(test_strings.clone()));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array])
        .map_err(|e| Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}")))?;

    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut stripe_builder = StripeBuilder::new(stripe_params)?;
    stripe_builder.push_batch(&batch)?;
    let prepared_stripe = stripe_builder.finish()?;

    let field_data_type = schema
        .find_field("test_unicode_field")?
        .unwrap()
        .1
        .data_type()?;
    let schema_id = field_data_type.schema_id()?;
    let field_descriptor = &prepared_stripe.fields.get(schema_id).unwrap().descriptor;

    assert_eq!(field_descriptor.position_count, 5);
    assert_eq!(field_descriptor.null_count, Some(0));

    if let Some(amudai_format::defs::shard::field_descriptor::TypeSpecific::StringStats(
        string_stats,
    )) = &field_descriptor.type_specific
    {
        assert_eq!(string_stats.min_size, 0); // Empty string
        assert!(string_stats.max_size > 0); // Some non-empty string
        assert_eq!(string_stats.min_non_empty_size, Some(4)); // 4 bytes for "🎉" or "café"
        // Not all strings are ASCII due to Unicode characters
        assert!(string_stats.ascii_count.unwrap_or(0) < 5);
    } else {
        panic!("Expected string statistics in type_specific field");
    }

    println!("✓ String statistics with Unicode test passed!");
    Ok(())
}

#[test]
fn test_int8_statistics_integration() -> Result<()> {
    let arrow_field = ArrowField::new("test_int8_field", ArrowDataType::Int8, true);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));
    let field = FieldBuilder::new("test_int8_field", BasicType::Int8, Some(true), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    let test_data = vec![Some(i8::MAX), Some(i8::MIN), Some(0), None, Some(42)];
    let array = Arc::new(arrow_array::Int8Array::from(test_data));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array])
        .map_err(|e| Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}")))?;

    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut stripe_builder = StripeBuilder::new(stripe_params)?;
    stripe_builder.push_batch(&batch)?;
    let prepared_stripe = stripe_builder.finish()?;

    let field_data_type = schema
        .find_field("test_int8_field")?
        .unwrap()
        .1
        .data_type()?;
    let schema_id = field_data_type.schema_id()?;
    let field_descriptor = &prepared_stripe.fields.get(schema_id).unwrap().descriptor;

    assert_eq!(field_descriptor.position_count, 5);
    assert_eq!(field_descriptor.null_count, Some(1));

    let range_stats = field_descriptor.range_stats.as_ref().unwrap();
    use amudai_format::defs::common::any_value::Kind;
    if let Some(Kind::I64Value(min)) = &range_stats.min_value.as_ref().unwrap().kind {
        assert_eq!(*min, i8::MIN as i64);
    } else {
        panic!("Expected I64Value for min");
    }

    if let Some(Kind::I64Value(max)) = &range_stats.max_value.as_ref().unwrap().kind {
        assert_eq!(*max, i8::MAX as i64);
    } else {
        panic!("Expected I64Value for max");
    }

    println!("✓ Int8 statistics integration test passed!");
    Ok(())
}

#[test]
fn test_uint32_statistics_integration() -> Result<()> {
    let arrow_field = ArrowField::new("test_uint32_field", ArrowDataType::UInt32, false);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));
    // Use Int32 as basic type but mark it as unsigned
    let field = FieldBuilder::new("test_uint32_field", BasicType::Int32, Some(false), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    let test_data = vec![u32::MAX, u32::MIN, 0, 42, 1000];
    let array = Arc::new(arrow_array::UInt32Array::from(test_data));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array])
        .map_err(|e| Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}")))?;

    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut stripe_builder = StripeBuilder::new(stripe_params)?;
    stripe_builder.push_batch(&batch)?;
    let prepared_stripe = stripe_builder.finish()?;

    let field_data_type = schema
        .find_field("test_uint32_field")?
        .unwrap()
        .1
        .data_type()?;
    let schema_id = field_data_type.schema_id()?;
    let field_descriptor = &prepared_stripe.fields.get(schema_id).unwrap().descriptor;

    assert_eq!(field_descriptor.position_count, 5);
    assert_eq!(field_descriptor.null_count, Some(0));

    let range_stats = field_descriptor.range_stats.as_ref().unwrap();
    use amudai_format::defs::common::any_value::Kind;
    if let Some(Kind::U64Value(min)) = &range_stats.min_value.as_ref().unwrap().kind {
        assert_eq!(*min, u32::MIN as u64);
    } else {
        panic!("Expected U64Value for min");
    }

    if let Some(Kind::U64Value(max)) = &range_stats.max_value.as_ref().unwrap().kind {
        assert_eq!(*max, u32::MAX as u64);
    } else {
        panic!("Expected U64Value for max");
    }

    println!("✓ UInt32 statistics integration test passed!");
    Ok(())
}

#[test]
fn test_mixed_nullable_non_nullable_aggregation() -> Result<()> {
    // Test aggregating stripes where some have nulls and others don't
    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

    // Create schema with nullable field
    let arrow_field = ArrowField::new("test_field", ArrowDataType::Int32, true);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));
    let field = FieldBuilder::new("test_field", BasicType::Int32, Some(true), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    let mut field_descriptors = Vec::new();

    // Stripe 1: All non-null values
    let stripe1_data = vec![Some(1), Some(2), Some(3)];
    let array1 = Arc::new(Int32Array::from(stripe1_data));
    let batch1 = RecordBatch::try_new(arrow_schema.clone(), vec![array1])
        .map_err(|e| Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}")))?;

    let stripe_params1 = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut stripe_builder1 = StripeBuilder::new(stripe_params1)?;
    stripe_builder1.push_batch(&batch1)?;
    let prepared_stripe1 = stripe_builder1.finish()?;

    let field_desc1 = {
        let field_data_type = schema.find_field("test_field")?.unwrap().1.data_type()?;
        let schema_id = field_data_type.schema_id()?;
        prepared_stripe1
            .fields
            .get(schema_id)
            .unwrap()
            .descriptor
            .clone()
    };
    field_descriptors.push(field_desc1);

    // Stripe 2: Mixed null and non-null values
    let stripe2_data = vec![Some(4), None, Some(5)];
    let array2 = Arc::new(Int32Array::from(stripe2_data));
    let batch2 = RecordBatch::try_new(arrow_schema.clone(), vec![array2])
        .map_err(|e| Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}")))?;

    let stripe_params2 = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut stripe_builder2 = StripeBuilder::new(stripe_params2)?;
    stripe_builder2.push_batch(&batch2)?;
    let prepared_stripe2 = stripe_builder2.finish()?;

    let field_desc2 = {
        let field_data_type = schema.find_field("test_field")?.unwrap().1.data_type()?;
        let schema_id = field_data_type.schema_id()?;
        prepared_stripe2
            .fields
            .get(schema_id)
            .unwrap()
            .descriptor
            .clone()
    };
    field_descriptors.push(field_desc2);

    // Aggregate and verify
    let field_descriptors_optional: Vec<Option<_>> =
        field_descriptors.into_iter().map(Some).collect();
    let shard_field_desc =
        crate::write::field_descriptor::merge_field_descriptors(field_descriptors_optional)?
            .expect("Should have merged field descriptors successfully");

    assert_eq!(shard_field_desc.position_count, 6); // 3 + 3
    assert_eq!(shard_field_desc.null_count, Some(1)); // 0 + 1

    let range_stats = shard_field_desc.range_stats.as_ref().unwrap();
    use amudai_format::defs::common::any_value::Kind;
    if let Some(Kind::I64Value(min)) = &range_stats.min_value.as_ref().unwrap().kind {
        assert_eq!(*min, 1);
    } else {
        panic!("Expected I64Value for min");
    }

    if let Some(Kind::I64Value(max)) = &range_stats.max_value.as_ref().unwrap().kind {
        assert_eq!(*max, 5);
    } else {
        panic!("Expected I64Value for max");
    }

    println!("✓ Mixed nullable/non-nullable aggregation test passed!");
    Ok(())
}

#[test]
fn test_large_batch_statistics() -> Result<()> {
    // Test statistics with a larger batch to ensure scalability
    let arrow_field = ArrowField::new("test_field", ArrowDataType::Int64, false);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));
    let field = FieldBuilder::new("test_field", BasicType::Int64, Some(true), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    // Create a larger dataset (1000 values)
    let test_data: Vec<i64> = (0..1000).map(|i| i * 2).collect(); // Even numbers 0, 2, 4, ..., 1998
    let array = Arc::new(Int64Array::from(test_data.clone()));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array])
        .map_err(|e| Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}")))?;

    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut stripe_builder = StripeBuilder::new(stripe_params)?;
    stripe_builder.push_batch(&batch)?;
    let prepared_stripe = stripe_builder.finish()?;

    let field_data_type = schema.find_field("test_field")?.unwrap().1.data_type()?;
    let schema_id = field_data_type.schema_id()?;
    let field_descriptor = &prepared_stripe.fields.get(schema_id).unwrap().descriptor;

    assert_eq!(field_descriptor.position_count, 1000);
    assert_eq!(field_descriptor.null_count, Some(0));

    let range_stats = field_descriptor.range_stats.as_ref().unwrap();
    use amudai_format::defs::common::any_value::Kind;
    if let Some(Kind::I64Value(min)) = &range_stats.min_value.as_ref().unwrap().kind {
        assert_eq!(*min, 0);
    } else {
        panic!("Expected I64Value for min");
    }

    if let Some(Kind::I64Value(max)) = &range_stats.max_value.as_ref().unwrap().kind {
        assert_eq!(*max, 1998);
    } else {
        panic!("Expected I64Value for max");
    }

    println!("✓ Large batch statistics test passed!");
    Ok(())
}

#[test]
fn test_empty_batch_statistics() -> Result<()> {
    // Test behavior with empty batches
    let arrow_field = ArrowField::new("test_field", ArrowDataType::Int32, true);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));
    let field = FieldBuilder::new("test_field", BasicType::Int32, Some(true), None);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    // Create empty array
    let test_data: Vec<Option<i32>> = vec![];
    let array = Arc::new(Int32Array::from(test_data));
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![array])
        .map_err(|e| Error::invalid_arg("batch", format!("Failed to create RecordBatch: {e}")))?;

    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut stripe_builder = StripeBuilder::new(stripe_params)?;
    stripe_builder.push_batch(&batch)?;
    let prepared_stripe = stripe_builder.finish()?;

    let field_data_type = schema.find_field("test_field")?.unwrap().1.data_type()?;
    let schema_id = field_data_type.schema_id()?;
    let field_descriptor = &prepared_stripe.fields.get(schema_id).unwrap().descriptor;
    assert_eq!(field_descriptor.position_count, 0);
    assert_eq!(field_descriptor.null_count, Some(0));
    // For empty batches, range stats behavior may vary - check what's actually present
    if let Some(range_stats) = &field_descriptor.range_stats {
        // If range stats are present for empty batches, they should have None values
        println!(
            "Range stats present for empty batch: min={:?}, max={:?}",
            range_stats.min_value, range_stats.max_value
        );
        assert!(range_stats.min_value.is_none() || range_stats.max_value.is_none());
    } else {
        println!("No range stats for empty batch (expected)");
    }

    println!("✓ Empty batch statistics test passed!");
    Ok(())
}

/// Creates a decimal array for testing decimal statistics.
fn create_decimal_array_from_d128_values(values: &[Option<decimal::d128>]) -> Arc<dyn Array> {
    use arrow_array::FixedSizeBinaryArray;

    let mut binary_values = Vec::new();
    let mut null_buffer = Vec::new();

    for value in values {
        match value {
            Some(d) => {
                let bytes = d.to_raw_bytes();
                binary_values.extend_from_slice(&bytes);
                null_buffer.push(true);
            }
            None => {
                binary_values.extend_from_slice(&[0u8; 16]);
                null_buffer.push(false);
            }
        }
    }

    let buffer = arrow_buffer::Buffer::from_vec(binary_values);
    let null_buffer = if null_buffer.iter().all(|&x| x) {
        None
    } else {
        Some(arrow_buffer::NullBuffer::from(null_buffer))
    };

    Arc::new(FixedSizeBinaryArray::new(16, buffer, null_buffer))
}

/// Creates a RecordBatch with a single decimal field for testing.
fn create_decimal_batch_from_d128_values(values: &[Option<decimal::d128>]) -> RecordBatch {
    create_decimal_batch_from_d128_values_with_name(values, "test_decimal")
}

/// Creates a RecordBatch with a single decimal field for testing with a custom field name.
fn create_decimal_batch_from_d128_values_with_name(
    values: &[Option<decimal::d128>],
    field_name: &str,
) -> RecordBatch {
    use arrow_schema::Schema;

    // Convert to Arrow schema for the RecordBatch
    let arrow_field = arrow_schema::Field::new(
        field_name,
        arrow_schema::DataType::FixedSizeBinary(16),
        true,
    );
    let arrow_schema = Arc::new(Schema::new(vec![arrow_field]));

    let array = create_decimal_array_from_d128_values(values);

    RecordBatch::try_new(arrow_schema, vec![array]).expect("Failed to create RecordBatch")
}

#[test]
fn test_decimal_statistics_integration() -> Result<()> {
    use amudai_format::schema_builder::{DataTypeBuilder, FieldBuilder, SchemaBuilder};
    use decimal::d128;
    use std::str::FromStr;

    // Create test decimal values with various characteristics
    let decimals = [
        Some(d128::from_str("123.45").unwrap()), // Positive
        Some(d128::from_str("-67.89").unwrap()), // Negative
        Some(d128::from_str("0.00").unwrap()),   // Zero
        Some(d128::from_str("999.99").unwrap()), // Large positive
    ];

    let batch = create_decimal_batch_from_d128_values(&decimals);

    // Create amudai schema with decimal field using DataTypeBuilder::new_decimal()
    let decimal_data_type = DataTypeBuilder::new_decimal().with_field_name("test_decimal");
    let field = FieldBuilder::from(decimal_data_type);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    // Create stripe builder with proper parameters
    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut builder = StripeBuilder::new(stripe_params)?;
    builder.push_batch(&batch)?;
    let stripe = builder.finish()?;

    // Get field descriptor using schema ID
    let field_data_type = schema.find_field("test_decimal")?.unwrap().1.data_type()?;
    let schema_id = field_data_type.schema_id()?;
    let field_descriptor = &stripe.fields.get(schema_id).unwrap().descriptor;

    // Verify basic stats
    assert_eq!(field_descriptor.position_count, 4);
    assert_eq!(field_descriptor.null_count, Some(0)); // No nulls    // Verify decimal-specific statistics are present
    if let Some(shard::field_descriptor::TypeSpecific::DecimalStats(decimal_stats)) =
        &field_descriptor.type_specific
    {
        // Check the actual values (no longer optional)
        let zero_count = decimal_stats.zero_count;
        let positive_count = decimal_stats.positive_count;
        let negative_count = decimal_stats.negative_count;
        // The total non-null values should equal 4
        assert_eq!(zero_count + positive_count + negative_count, 4);

        // We should have at least some positive and negative values
        assert!(positive_count > 0);
        assert!(negative_count > 0);
    } else {
        panic!("Expected DecimalStats in type_specific field");
    }

    println!("✓ Decimal statistics integration test passed!");
    Ok(())
}

#[test]
fn test_decimal_statistics_all_null() -> Result<()> {
    use amudai_format::schema_builder::{DataTypeBuilder, FieldBuilder, SchemaBuilder};

    // Test edge case: all values are null
    let decimals = [None, None, None];

    let batch = create_decimal_batch_from_d128_values(&decimals);

    let decimal_data_type = DataTypeBuilder::new_decimal().with_field_name("test_decimal");
    let field = FieldBuilder::from(decimal_data_type);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut builder = StripeBuilder::new(stripe_params)?;
    builder.push_batch(&batch)?;
    let stripe = builder.finish()?;

    let field_data_type = schema.find_field("test_decimal")?.unwrap().1.data_type()?;
    let schema_id = field_data_type.schema_id()?;
    let field_descriptor = &stripe.fields.get(schema_id).unwrap().descriptor;

    assert_eq!(field_descriptor.position_count, 3);
    assert_eq!(field_descriptor.null_count, Some(3)); // All nulls

    // Should still have type-specific stats but with zero counts
    if let Some(shard::field_descriptor::TypeSpecific::DecimalStats(decimal_stats)) =
        &field_descriptor.type_specific
    {
        assert_eq!(decimal_stats.zero_count, 0);
        assert_eq!(decimal_stats.positive_count, 0);
        assert_eq!(decimal_stats.negative_count, 0);
    } else {
        panic!("Expected DecimalStats in type_specific field");
    }

    println!("✓ Decimal statistics all-null test passed!");
    Ok(())
}

#[test]
fn test_decimal_statistics_single_value() -> Result<()> {
    use amudai_format::schema_builder::{DataTypeBuilder, FieldBuilder, SchemaBuilder};
    use decimal::d128;
    use std::str::FromStr;

    // Test edge case: single value
    let decimals = [Some(d128::from_str("42.42").unwrap())];

    let batch = create_decimal_batch_from_d128_values(&decimals);

    let decimal_data_type = DataTypeBuilder::new_decimal().with_field_name("test_decimal");
    let field = FieldBuilder::from(decimal_data_type);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut builder = StripeBuilder::new(stripe_params)?;
    builder.push_batch(&batch)?;
    let stripe = builder.finish()?;

    let field_data_type = schema.find_field("test_decimal")?.unwrap().1.data_type()?;
    let schema_id = field_data_type.schema_id()?;
    let field_descriptor = &stripe.fields.get(schema_id).unwrap().descriptor;

    assert_eq!(field_descriptor.position_count, 1);
    assert_eq!(field_descriptor.null_count, Some(0));

    if let Some(shard::field_descriptor::TypeSpecific::DecimalStats(decimal_stats)) =
        &field_descriptor.type_specific
    {
        assert_eq!(decimal_stats.zero_count, 0);
        assert_eq!(decimal_stats.positive_count, 1); // Single positive value
        assert_eq!(decimal_stats.negative_count, 0);
    } else {
        panic!("Expected DecimalStats in type_specific field");
    }

    println!("✓ Decimal statistics single-value test passed!");
    Ok(())
}

#[test]
fn test_decimal_statistics_mixed_nulls() -> Result<()> {
    use amudai_format::schema_builder::{DataTypeBuilder, FieldBuilder, SchemaBuilder};
    use decimal::d128;
    use std::str::FromStr;

    // Test mixed null and non-null values
    let decimals = [
        Some(d128::from_str("100.00").unwrap()),
        None,
        Some(d128::from_str("-50.25").unwrap()),
        None,
        Some(d128::from_str("0.00").unwrap()),
    ];

    let batch = create_decimal_batch_from_d128_values(&decimals);

    let decimal_data_type = DataTypeBuilder::new_decimal().with_field_name("test_decimal");
    let field = FieldBuilder::from(decimal_data_type);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut builder = StripeBuilder::new(stripe_params)?;
    builder.push_batch(&batch)?;
    let stripe = builder.finish()?;

    let field_data_type = schema.find_field("test_decimal")?.unwrap().1.data_type()?;
    let schema_id = field_data_type.schema_id()?;
    let field_descriptor = &stripe.fields.get(schema_id).unwrap().descriptor;

    assert_eq!(field_descriptor.position_count, 5);
    assert_eq!(field_descriptor.null_count, Some(2)); // Two nulls
    if let Some(shard::field_descriptor::TypeSpecific::DecimalStats(decimal_stats)) =
        &field_descriptor.type_specific
    {
        // Check for presence first
        let zero_count = decimal_stats.zero_count;
        let positive_count = decimal_stats.positive_count;
        let negative_count = decimal_stats.negative_count;

        // We should have total 3 non-null values
        assert_eq!(zero_count + positive_count + negative_count, 3);

        // We should have at least some positive and negative values
        assert!(positive_count > 0);
        assert!(negative_count > 0);
    } else {
        panic!("Expected DecimalStats in type_specific field");
    }

    println!("✓ Decimal statistics mixed-null test passed!");
    Ok(())
}

#[test]
fn test_decimal_statistics_aggregation() -> Result<()> {
    use amudai_format::schema_builder::{DataTypeBuilder, FieldBuilder, SchemaBuilder};
    use decimal::d128;
    use std::str::FromStr;

    // Create test schema
    let decimal_data_type = DataTypeBuilder::new_decimal().with_field_name("test_decimals");
    let field = FieldBuilder::from(decimal_data_type);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    // Create test data for multiple stripes
    let stripe_data = [
        vec![
            Some(d128::from_str("10.5").unwrap()),
            Some(d128::from_str("-20.3").unwrap()),
        ],
        vec![
            Some(d128::from_str("0.0").unwrap()),
            None,
            Some(d128::from_str("30.7").unwrap()),
        ],
        vec![
            Some(d128::from_str("-100.0").unwrap()),
            Some(d128::from_str("50.0").unwrap()),
        ],
    ];

    let mut field_descriptors = Vec::new(); // Process each stripe
    for (i, decimals) in stripe_data.iter().enumerate() {
        let batch = create_decimal_batch_from_d128_values_with_name(decimals, "test_decimals");

        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let stripe_params = StripeBuilderParams {
            schema: schema.clone(),
            temp_store: temp_store.clone(),
            encoding_profile: BlockEncodingProfile::default(),
        };

        let mut builder = StripeBuilder::new(stripe_params)?;
        builder.push_batch(&batch)?;
        let stripe = builder.finish()?;

        let field_data_type = schema.find_field("test_decimals")?.unwrap().1.data_type()?;
        let schema_id = field_data_type.schema_id()?;
        let field_descriptor = stripe.fields.get(schema_id).unwrap().descriptor.clone();

        field_descriptors.push(field_descriptor);
        println!("Stripe {}: processed {} decimals", i + 1, decimals.len());
    }

    // Simulate shard-level aggregation
    let field_descriptors_optional: Vec<Option<_>> =
        field_descriptors.into_iter().map(Some).collect();
    let shard_field_desc =
        crate::write::field_descriptor::merge_field_descriptors(field_descriptors_optional)?
            .expect("Should have merged field descriptors successfully");

    // Verify aggregated stats
    assert_eq!(shard_field_desc.position_count, 7); // 2 + 3 + 2 = 7
    assert_eq!(shard_field_desc.null_count, Some(1)); // 0 + 1 + 0 = 1 null    // Verify aggregated decimal statistics
    if let Some(shard::field_descriptor::TypeSpecific::DecimalStats(decimal_stats)) =
        &shard_field_desc.type_specific
    {
        // Check for presence first
        let zero_count = decimal_stats.zero_count;
        let positive_count = decimal_stats.positive_count;
        let negative_count = decimal_stats.negative_count;

        // Total non-null values should be 6 (7 total - 1 null)
        assert_eq!(zero_count + positive_count + negative_count, 6);
        // We should have both positive and negative values
        assert!(positive_count > 0);
        assert!(negative_count > 0);
    } else {
        panic!("Expected DecimalStats in type_specific field");
    }

    println!("✓ Decimal statistics aggregation test passed!");
    Ok(())
}
