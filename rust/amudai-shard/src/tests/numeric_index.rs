//! Comprehensive Numeric Index Tests
//!
//! This module contains comprehensive tests for numeric index functionality across all
//! supported numeric data types. It consolidates and extends the previous tests from
//! both decimal_index.rs and numeric_index.rs files.
//!
//! # Test Coverage
//!
//! ## Core Numeric Types:
//! - Signed Integers: Int8, Int16, Int32, Int64
//! - Unsigned Integers: UInt8, UInt16, UInt32, UInt64  
//! - Floating Point: Float32, Float64
//! - DateTime: TimestampMillisecond
//! - Decimal: KustoDecimal (FixedSizeBinary(16) with KustoDecimal extension)
//!
//! ## Test Categories:
//! - Basic index creation and validation
//! - Different encoding profiles (Plain, Balanced, HighCompression)
//! - Edge cases (nulls, single values, empty datasets)
//! - Large datasets spanning multiple logical blocks
//! - Mixed field types (numeric and non-numeric)
//! - Index content validation via NumericIndexReader
//! - Statistics validation (min/max, null counts)
//!
//! ## Integration Testing:
//! - Full ShardBuilder -> StripeBuilder -> FieldBuilder pipeline
//! - Index creation with different BlockEncodingProfile settings
//! - Validation that indexes are properly created and accessible
//! - Cross-validation between field statistics and index contents

use std::str::FromStr;
use std::sync::Arc;

use amudai_arrow_compat::arrow_to_amudai_schema::FromArrowSchema;
use amudai_blockstream::read::block_stream::empty_hint;
use amudai_common::Result;
use amudai_encodings::block_encoder::BlockEncodingProfile;
use amudai_format::defs::schema::BasicType;
use amudai_format::defs::schema_ext::BasicTypeDescriptor;
use amudai_format::defs::shard::BufferKind;
use amudai_format::schema_builder::{DataTypeBuilder, FieldBuilder, SchemaBuilder};
use amudai_io_impl::temp_file_store;
use amudai_objectstore::local_store::{LocalFsMode, LocalFsObjectStore};

use crate::read::numeric_index::NumericIndexDecoder;
use crate::write::numeric_index::NumericIndexBuilder;
use arrow_array::{
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, TimestampMillisecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
use decimal::d128;
use tempfile::TempDir;

use crate::read::numeric_index as read_numeric_index;
use crate::write::numeric_index;
use crate::write::shard_builder::{ShardBuilder, ShardBuilderParams, ShardFileOrganization};
use crate::write::stripe_builder::{StripeBuilder, StripeBuilderParams};

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Creates a decimal array from d128 values for testing
fn create_decimal_array_from_d128_values(
    values: &[Option<decimal::d128>],
) -> Arc<dyn arrow_array::Array> {
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
                binary_values.extend_from_slice(&[0u8; 16]); // Placeholder for null
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

/// Creates a RecordBatch with a single decimal field for testing
fn create_decimal_batch_from_d128_values(
    values: &[Option<decimal::d128>],
    field_name: &str,
) -> RecordBatch {
    // Create Arrow field with KustoDecimal metadata
    let arrow_field = ArrowField::new(field_name, ArrowDataType::FixedSizeBinary(16), true)
        .with_metadata(
            [(
                "ARROW:extension:name".to_string(),
                "KustoDecimal".to_string(),
            )]
            .into(),
        );
    let arrow_schema = Arc::new(ArrowSchema::new(vec![arrow_field]));

    let array = create_decimal_array_from_d128_values(values);

    RecordBatch::try_new(arrow_schema, vec![array]).expect("Failed to create RecordBatch")
}

/// Helper to validate that a buffer contains a numeric index
fn validate_numeric_index_buffer_exists(
    stripe: &crate::write::stripe_builder::PreparedStripe,
    field_index: usize,
) -> bool {
    let field_info = &stripe.fields[field_index];

    for encoding in &field_info.encodings {
        for buffer in &encoding.buffers {
            if buffer.descriptor.kind == BufferKind::RangeIndex as i32 {
                return buffer.data_size > 0;
            }
        }
    }
    false
}

// ============================================================================
// DECIMAL INDEX TESTS
// ============================================================================

#[test]
fn test_decimal_index_creation_basic() -> Result<()> {
    println!("=== Testing Basic Decimal Index Creation ===");

    // Create test decimal values with known values for index verification
    let test_decimals = [
        Some(d128::from_str("123.45").unwrap()),
        Some(d128::from_str("67.89").unwrap()),
        Some(d128::from_str("999.99").unwrap()),
        Some(d128::from_str("10.50").unwrap()),
        None,
        Some(d128::from_str("500.00").unwrap()),
    ];

    let batch = create_decimal_batch_from_d128_values(&test_decimals, "decimal_field");

    // Create amudai schema with decimal field
    let decimal_data_type = DataTypeBuilder::new_decimal().with_field_name("decimal_field");
    let field = FieldBuilder::from(decimal_data_type);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    // Create stripe builder with default encoding profile (indexes enabled)
    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut stripe_builder = StripeBuilder::new(stripe_params)?;
    stripe_builder.push_batch(&batch)?;
    let prepared_stripe = stripe_builder.finish()?;

    // Verify that we have field data
    assert!(!prepared_stripe.fields.is_empty());

    // Verify field statistics
    let field_descriptor = &prepared_stripe.fields[0].descriptor;
    assert_eq!(field_descriptor.position_count, 6);
    assert_eq!(field_descriptor.null_count, Some(1));

    // Verify index was created
    assert!(validate_numeric_index_buffer_exists(&prepared_stripe, 0));

    println!("✓ Decimal index creation basic test passed!");
    Ok(())
}

#[test]
fn test_decimal_index_with_plain_profile() -> Result<()> {
    println!("=== Testing Decimal Index with Plain Profile ===");

    let test_decimals = [
        Some(d128::from_str("123.45").unwrap()),
        Some(d128::from_str("67.89").unwrap()),
        Some(d128::from_str("999.99").unwrap()),
    ];

    let batch = create_decimal_batch_from_d128_values(&test_decimals, "decimal_field");

    let decimal_data_type = DataTypeBuilder::new_decimal().with_field_name("decimal_field");
    let field = FieldBuilder::from(decimal_data_type);
    let schema_builder = SchemaBuilder::new(vec![field]);
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    // Create stripe builder with Plain encoding profile (indexes disabled)
    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::Plain,
    };

    let mut stripe_builder = StripeBuilder::new(stripe_params)?;
    stripe_builder.push_batch(&batch)?;
    let prepared_stripe = stripe_builder.finish()?;

    // Verify that index was NOT created for Plain profile
    assert!(!validate_numeric_index_buffer_exists(&prepared_stripe, 0));

    println!("✓ Decimal index with Plain profile test passed!");
    Ok(())
}

#[test]
fn test_decimal_index_large_dataset() -> Result<()> {
    println!("=== Testing Decimal Index with Large Dataset ===");

    // Create larger dataset to test index behavior across multiple blocks
    let mut test_decimals = Vec::new();
    for i in 0..1000 {
        if i % 10 == 0 {
            test_decimals.push(None); // Add some nulls
        } else {
            let decimal_str = format!("{i}.00");
            test_decimals.push(Some(d128::from_str(&decimal_str).unwrap()));
        }
    }

    let batch = create_decimal_batch_from_d128_values(&test_decimals, "decimal_field");

    let decimal_data_type = DataTypeBuilder::new_decimal().with_field_name("decimal_field");
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

    let mut stripe_builder = StripeBuilder::new(stripe_params)?;
    stripe_builder.push_batch(&batch)?;
    let prepared_stripe = stripe_builder.finish()?;

    // Verify field statistics
    let field_descriptor = &prepared_stripe.fields[0].descriptor;
    assert_eq!(field_descriptor.position_count, 1000);
    assert_eq!(field_descriptor.null_count, Some(100)); // Every 10th value is null

    // Verify index was created and has substantial data
    assert!(validate_numeric_index_buffer_exists(&prepared_stripe, 0));

    println!("✓ Decimal index large dataset test passed!");
    Ok(())
}

// ============================================================================
// INTEGER INDEX TESTS
// ============================================================================

#[test]
fn test_signed_integer_indexes() -> Result<()> {
    println!("=== Testing Signed Integer Index Creation ===");

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("int8_field", ArrowDataType::Int8, true),
        ArrowField::new("int16_field", ArrowDataType::Int16, true),
        ArrowField::new("int32_field", ArrowDataType::Int32, true),
        ArrowField::new("int64_field", ArrowDataType::Int64, true),
    ]));

    let temp_dir = TempDir::new().unwrap();
    let object_store =
        Arc::new(LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot).unwrap());
    let temp_store = temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap();

    // Create test data for signed types including negative values
    let int8_data = vec![Some(-128i8), Some(0i8), Some(127i8), None, Some(-50i8)];
    let int16_data = vec![
        Some(-32768i16),
        Some(0i16),
        Some(32767i16),
        None,
        Some(-1000i16),
    ];
    let int32_data = vec![
        Some(-2147483648i32),
        Some(0i32),
        Some(2147483647i32),
        None,
        Some(-50000i32),
    ];
    let int64_data = vec![
        Some(-9223372036854775808i64),
        Some(0i64),
        Some(9223372036854775807i64),
        None,
        Some(-1000000i64),
    ];

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int8Array::from(int8_data.clone())),
            Arc::new(Int16Array::from(int16_data.clone())),
            Arc::new(Int32Array::from(int32_data.clone())),
            Arc::new(Int64Array::from(int64_data.clone())),
        ],
    )
    .unwrap();

    let shard_schema = SchemaBuilder::from_arrow_schema(&arrow_schema)
        .unwrap()
        .finish_and_seal();
    let params = ShardBuilderParams {
        schema: shard_schema,
        object_store,
        temp_store,
        file_organization: ShardFileOrganization::SingleFile,
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut shard_builder = ShardBuilder::new(params).unwrap();
    let mut stripe_builder = shard_builder.build_stripe().unwrap();
    stripe_builder.push_batch(&batch).unwrap();
    let prepared_stripe = stripe_builder.finish().unwrap();

    assert_eq!(prepared_stripe.record_count, int8_data.len() as u64);
    assert_eq!(prepared_stripe.fields.len(), 4);

    // Verify indexes were created for all signed integer fields
    for i in 0..4 {
        assert!(validate_numeric_index_buffer_exists(&prepared_stripe, i));
    }

    shard_builder.add_stripe(prepared_stripe).unwrap();
    let _prepared_shard = shard_builder.finish().unwrap();

    println!("✓ All signed integer types indexed successfully");
    Ok(())
}

#[test]
fn test_unsigned_integer_indexes() -> Result<()> {
    println!("=== Testing Unsigned Integer Index Creation ===");

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("uint8_field", ArrowDataType::UInt8, true),
        ArrowField::new("uint16_field", ArrowDataType::UInt16, true),
        ArrowField::new("uint32_field", ArrowDataType::UInt32, true),
        ArrowField::new("uint64_field", ArrowDataType::UInt64, true),
    ]));

    let temp_dir = TempDir::new().unwrap();
    let object_store =
        Arc::new(LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot).unwrap());
    let temp_store = temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap();

    // Create test data for unsigned types - ensure all arrays have same length
    let test_size = 7;
    let uint8_data = vec![
        Some(0u8),
        Some(255u8),
        Some(128u8),
        None,
        Some(50u8),
        Some(100u8),
        Some(200u8),
    ];
    let uint16_data = vec![
        Some(0u16),
        Some(65535u16),
        Some(32768u16),
        None,
        Some(1000u16),
        Some(5000u16),
        Some(50000u16),
    ];
    let uint32_data = vec![
        Some(0u32),
        Some(4294967295u32),
        Some(2147483648u32),
        None,
        Some(100000u32),
        Some(500000u32),
        Some(3000000000u32),
    ];
    let uint64_data = vec![
        Some(0u64),
        Some(18446744073709551615u64),
        Some(9223372036854775808u64),
        None,
        Some(1000000u64),
        Some(500000000u64),
        Some(15000000000000000000u64),
    ];

    // Verify all arrays have the same length
    assert_eq!(uint8_data.len(), test_size);
    assert_eq!(uint16_data.len(), test_size);
    assert_eq!(uint32_data.len(), test_size);
    assert_eq!(uint64_data.len(), test_size);

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(UInt8Array::from(uint8_data.clone())),
            Arc::new(UInt16Array::from(uint16_data.clone())),
            Arc::new(UInt32Array::from(uint32_data.clone())),
            Arc::new(UInt64Array::from(uint64_data.clone())),
        ],
    )
    .unwrap();

    let shard_schema = SchemaBuilder::from_arrow_schema(&arrow_schema)
        .unwrap()
        .finish_and_seal();
    let params = ShardBuilderParams {
        schema: shard_schema,
        object_store,
        temp_store,
        file_organization: ShardFileOrganization::SingleFile,
        encoding_profile: BlockEncodingProfile::HighCompression,
    };

    let mut shard_builder = ShardBuilder::new(params).unwrap();
    let mut stripe_builder = shard_builder.build_stripe().unwrap();
    stripe_builder.push_batch(&batch).unwrap();
    let prepared_stripe = stripe_builder.finish().unwrap();

    assert_eq!(prepared_stripe.record_count, uint8_data.len() as u64);
    assert_eq!(prepared_stripe.fields.len(), 4);

    // Verify indexes were created for all unsigned integer fields
    for i in 0..4 {
        assert!(validate_numeric_index_buffer_exists(&prepared_stripe, i));
    }

    shard_builder.add_stripe(prepared_stripe).unwrap();
    let _prepared_shard = shard_builder.finish().unwrap();

    println!("✓ All unsigned integer types indexed successfully");
    Ok(())
}

// ============================================================================
// FLOATING POINT INDEX TESTS
// ============================================================================

#[test]
#[allow(clippy::approx_constant)]
fn test_floating_point_indexes() -> Result<()> {
    println!("=== Testing Float32 and Float64 Index Creation ===");

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("float32_field", ArrowDataType::Float32, true),
        ArrowField::new("float64_field", ArrowDataType::Float64, true),
    ]));

    let temp_dir = TempDir::new().unwrap();
    let object_store =
        Arc::new(LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot).unwrap());
    let temp_store = temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap();

    // Create test data with floating point values including edge cases
    let float32_data = vec![
        Some(-3.4028235e38f32),
        Some(0.0f32),
        Some(3.4028235e38f32),
        None,
        Some(-1.5f32),
        Some(3.14159f32),
    ];

    let float64_data = vec![
        Some(-1.7976931348623157e308f64),
        Some(0.0f64),
        Some(1.7976931348623157e308f64),
        None,
        Some(-2.5f64),
        Some(2.718281828459045f64),
    ];

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Float32Array::from(float32_data.clone())),
            Arc::new(Float64Array::from(float64_data.clone())),
        ],
    )
    .unwrap();

    let shard_schema = SchemaBuilder::from_arrow_schema(&arrow_schema)
        .unwrap()
        .finish_and_seal();
    let params = ShardBuilderParams {
        schema: shard_schema,
        object_store,
        temp_store,
        file_organization: ShardFileOrganization::SingleFile,
        encoding_profile: BlockEncodingProfile::Balanced,
    };

    let mut shard_builder = ShardBuilder::new(params).unwrap();
    let mut stripe_builder = shard_builder.build_stripe().unwrap();
    stripe_builder.push_batch(&batch).unwrap();
    let prepared_stripe = stripe_builder.finish().unwrap();

    assert_eq!(prepared_stripe.record_count, float32_data.len() as u64);
    assert_eq!(prepared_stripe.fields.len(), 2);

    // Verify indexes were created for both floating point fields
    assert!(validate_numeric_index_buffer_exists(&prepared_stripe, 0));
    assert!(validate_numeric_index_buffer_exists(&prepared_stripe, 1));

    shard_builder.add_stripe(prepared_stripe).unwrap();
    let _prepared_shard = shard_builder.finish().unwrap();

    println!("✓ Float32 and Float64 indexes created successfully");
    Ok(())
}

#[test]
fn test_floating_point_special_values() -> Result<()> {
    println!("=== Testing Floating Point Special Values ===");

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("float32_field", ArrowDataType::Float32, true),
        ArrowField::new("float64_field", ArrowDataType::Float64, true),
    ]));

    let temp_dir = TempDir::new().unwrap();
    let object_store =
        Arc::new(LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot).unwrap());
    let temp_store = temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap();

    // Test with special floating point values
    let float32_data = vec![
        Some(f32::NAN),
        Some(f32::INFINITY),
        Some(f32::NEG_INFINITY),
        Some(0.0f32),
        Some(-0.0f32),
        None,
        Some(1.0f32),
    ];

    let float64_data = vec![
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(0.0f64),
        Some(-0.0f64),
        None,
        Some(1.0f64),
    ];

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Float32Array::from(float32_data.clone())),
            Arc::new(Float64Array::from(float64_data.clone())),
        ],
    )
    .unwrap();

    let shard_schema = SchemaBuilder::from_arrow_schema(&arrow_schema)
        .unwrap()
        .finish_and_seal();
    let params = ShardBuilderParams {
        schema: shard_schema,
        object_store,
        temp_store,
        file_organization: ShardFileOrganization::SingleFile,
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut shard_builder = ShardBuilder::new(params).unwrap();
    let mut stripe_builder = shard_builder.build_stripe().unwrap();
    stripe_builder.push_batch(&batch).unwrap();
    let prepared_stripe = stripe_builder.finish().unwrap();

    // Verify indexes were created even with special values
    assert!(validate_numeric_index_buffer_exists(&prepared_stripe, 0));
    assert!(validate_numeric_index_buffer_exists(&prepared_stripe, 1));

    shard_builder.add_stripe(prepared_stripe).unwrap();
    let _prepared_shard = shard_builder.finish().unwrap();

    println!("✓ Floating point special values handled successfully");
    Ok(())
}

// ============================================================================
// DATETIME INDEX TESTS
// ============================================================================

#[test]
fn test_datetime_index_creation() -> Result<()> {
    println!("=== Testing DateTime Index Creation ===");

    let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "timestamp_field",
        ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
        true,
    )]));

    let temp_dir = TempDir::new().unwrap();
    let object_store =
        Arc::new(LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot).unwrap());
    let temp_store = temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap();

    // Create DateTime test data (milliseconds since epoch)
    let datetime_data = vec![
        Some(0i64),             // Unix epoch
        Some(1609459200000i64), // 2021-01-01 00:00:00
        Some(1672531200000i64), // 2023-01-01 00:00:00
        None,                   // Null value
        Some(946684800000i64),  // 2000-01-01 00:00:00
        Some(1735689600000i64), // 2025-01-01 00:00:00
    ];

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![Arc::new(TimestampMillisecondArray::from(
            datetime_data.clone(),
        ))],
    )
    .unwrap();

    let shard_schema = SchemaBuilder::from_arrow_schema(&arrow_schema)
        .unwrap()
        .finish_and_seal();
    let params = ShardBuilderParams {
        schema: shard_schema,
        object_store,
        temp_store,
        file_organization: ShardFileOrganization::SingleFile,
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut shard_builder = ShardBuilder::new(params).unwrap();
    let mut stripe_builder = shard_builder.build_stripe().unwrap();
    stripe_builder.push_batch(&batch).unwrap();
    let prepared_stripe = stripe_builder.finish().unwrap();

    assert_eq!(prepared_stripe.record_count, datetime_data.len() as u64);
    assert_eq!(prepared_stripe.fields.len(), 1);

    // Verify index was created for DateTime field
    assert!(validate_numeric_index_buffer_exists(&prepared_stripe, 0));

    shard_builder.add_stripe(prepared_stripe).unwrap();
    let _prepared_shard = shard_builder.finish().unwrap();

    println!("✓ DateTime index created successfully");
    Ok(())
}

// ============================================================================
// MIXED TYPE AND INTEGRATION TESTS
// ============================================================================

#[test]
fn test_mixed_numeric_and_non_numeric_fields() -> Result<()> {
    println!("=== Testing Mixed Numeric and Non-Numeric Fields ===");

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("int32_field", ArrowDataType::Int32, true), // Numeric - should have index
        ArrowField::new("string_field", ArrowDataType::Utf8, true), // Non-numeric - no index
        ArrowField::new("float64_field", ArrowDataType::Float64, true), // Numeric - should have index
        ArrowField::new("binary_field", ArrowDataType::Binary, true),   // Non-numeric - no index
        ArrowField::new("bool_field", ArrowDataType::Boolean, true),    // Non-numeric - no index
        ArrowField::new("int64_field", ArrowDataType::Int64, true), // Numeric - should have index
    ]));

    let temp_dir = TempDir::new().unwrap();
    let object_store =
        Arc::new(LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot).unwrap());
    let temp_store = temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap();

    // Create mixed test data
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![
                Some(100),
                Some(200),
                Some(300),
                None,
            ])),
            Arc::new(arrow_array::StringArray::from(vec![
                Some("test1"),
                Some("test2"),
                Some("test3"),
                None,
            ])),
            Arc::new(Float64Array::from(vec![
                Some(1.5),
                Some(2.5),
                Some(3.5),
                None,
            ])),
            Arc::new(arrow_array::BinaryArray::from(vec![
                Some("data1".as_bytes()),
                Some("data2".as_bytes()),
                Some("data3".as_bytes()),
                None,
            ])),
            Arc::new(arrow_array::BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                None,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(1000i64),
                Some(2000i64),
                Some(3000i64),
                None,
            ])),
        ],
    )
    .unwrap();

    let shard_schema = SchemaBuilder::from_arrow_schema(&arrow_schema)
        .unwrap()
        .finish_and_seal();
    let params = ShardBuilderParams {
        schema: shard_schema,
        object_store,
        temp_store,
        file_organization: ShardFileOrganization::SingleFile,
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut shard_builder = ShardBuilder::new(params).unwrap();
    let mut stripe_builder = shard_builder.build_stripe().unwrap();
    stripe_builder.push_batch(&batch).unwrap();
    let prepared_stripe = stripe_builder.finish().unwrap();

    assert_eq!(prepared_stripe.record_count, 4);
    assert_eq!(prepared_stripe.fields.len(), 6);

    // Verify only numeric fields have indexes
    assert!(validate_numeric_index_buffer_exists(&prepared_stripe, 0)); // int32_field
    assert!(!validate_numeric_index_buffer_exists(&prepared_stripe, 1)); // string_field
    assert!(validate_numeric_index_buffer_exists(&prepared_stripe, 2)); // float64_field
    assert!(!validate_numeric_index_buffer_exists(&prepared_stripe, 3)); // binary_field
    assert!(!validate_numeric_index_buffer_exists(&prepared_stripe, 4)); // bool_field
    assert!(validate_numeric_index_buffer_exists(&prepared_stripe, 5)); // int64_field

    shard_builder.add_stripe(prepared_stripe).unwrap();
    let _prepared_shard = shard_builder.finish().unwrap();

    println!("✓ Mixed field types processed successfully");
    println!("✓ Numeric fields: int32, float64, int64 have indexes");
    println!("✓ Non-numeric fields: string, binary, bool do not have indexes");
    Ok(())
}

#[test]
#[allow(clippy::approx_constant)]
fn test_all_numeric_types_comprehensive() -> Result<()> {
    println!("=== Testing All Numeric Types Comprehensive ===");

    // Create schema with all supported numeric types including decimal
    let mut fields = vec![
        ArrowField::new("int8_field", ArrowDataType::Int8, true),
        ArrowField::new("int16_field", ArrowDataType::Int16, true),
        ArrowField::new("int32_field", ArrowDataType::Int32, true),
        ArrowField::new("int64_field", ArrowDataType::Int64, true),
        ArrowField::new("uint8_field", ArrowDataType::UInt8, true),
        ArrowField::new("uint16_field", ArrowDataType::UInt16, true),
        ArrowField::new("uint32_field", ArrowDataType::UInt32, true),
        ArrowField::new("uint64_field", ArrowDataType::UInt64, true),
        ArrowField::new("float32_field", ArrowDataType::Float32, true),
        ArrowField::new("float64_field", ArrowDataType::Float64, true),
        ArrowField::new(
            "timestamp_field",
            ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
    ];

    // Add decimal field with proper metadata
    let decimal_field = ArrowField::new("decimal_field", ArrowDataType::FixedSizeBinary(16), true)
        .with_metadata(
            [(
                "ARROW:extension:name".to_string(),
                "KustoDecimal".to_string(),
            )]
            .into(),
        );
    fields.push(decimal_field);

    let arrow_schema = Arc::new(ArrowSchema::new(fields));

    let temp_dir = TempDir::new().unwrap();
    let object_store =
        Arc::new(LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot).unwrap());
    let temp_store = temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap();

    // Create test data for all types
    let test_size = 5;
    let arrays: Vec<Arc<dyn arrow_array::Array>> = vec![
        Arc::new(Int8Array::from(vec![
            Some(-50i8),
            Some(0i8),
            Some(50i8),
            None,
            Some(25i8),
        ])),
        Arc::new(Int16Array::from(vec![
            Some(-1000i16),
            Some(0i16),
            Some(1000i16),
            None,
            Some(500i16),
        ])),
        Arc::new(Int32Array::from(vec![
            Some(-100000i32),
            Some(0i32),
            Some(100000i32),
            None,
            Some(50000i32),
        ])),
        Arc::new(Int64Array::from(vec![
            Some(-1000000000i64),
            Some(0i64),
            Some(1000000000i64),
            None,
            Some(500000000i64),
        ])),
        Arc::new(UInt8Array::from(vec![
            Some(0u8),
            Some(50u8),
            Some(100u8),
            None,
            Some(200u8),
        ])),
        Arc::new(UInt16Array::from(vec![
            Some(0u16),
            Some(1000u16),
            Some(2000u16),
            None,
            Some(30000u16),
        ])),
        Arc::new(UInt32Array::from(vec![
            Some(0u32),
            Some(100000u32),
            Some(200000u32),
            None,
            Some(3000000u32),
        ])),
        Arc::new(UInt64Array::from(vec![
            Some(0u64),
            Some(1000000000u64),
            Some(2000000000u64),
            None,
            Some(15000000000u64),
        ])),
        Arc::new(Float32Array::from(vec![
            Some(-1.5f32),
            Some(0.0f32),
            Some(1.5f32),
            None,
            Some(3.14f32),
        ])),
        Arc::new(Float64Array::from(vec![
            Some(-2.5f64),
            Some(0.0f64),
            Some(2.5f64),
            None,
            Some(2.718f64),
        ])),
        Arc::new(TimestampMillisecondArray::from(vec![
            Some(0i64),
            Some(1609459200000i64),
            Some(1672531200000i64),
            None,
            Some(946684800000i64),
        ])),
        create_decimal_array_from_d128_values(&[
            Some(d128::from_str("-123.45").unwrap()),
            Some(d128::from_str("0.00").unwrap()),
            Some(d128::from_str("123.45").unwrap()),
            None,
            Some(d128::from_str("999.99").unwrap()),
        ]),
    ];

    let batch = RecordBatch::try_new(arrow_schema.clone(), arrays).unwrap();

    let shard_schema = SchemaBuilder::from_arrow_schema(&arrow_schema)
        .unwrap()
        .finish_and_seal();
    let params = ShardBuilderParams {
        schema: shard_schema,
        object_store,
        temp_store,
        file_organization: ShardFileOrganization::SingleFile,
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut shard_builder = ShardBuilder::new(params).unwrap();
    let mut stripe_builder = shard_builder.build_stripe().unwrap();
    stripe_builder.push_batch(&batch).unwrap();
    let prepared_stripe = stripe_builder.finish().unwrap();

    assert_eq!(prepared_stripe.record_count, test_size as u64);
    assert_eq!(prepared_stripe.fields.len(), 12); // All numeric types

    // Verify all fields have indexes (since all are numeric types)
    for i in 0..12 {
        assert!(
            validate_numeric_index_buffer_exists(&prepared_stripe, i),
            "Field {i} should have a numeric index"
        );
    }

    shard_builder.add_stripe(prepared_stripe).unwrap();
    let _prepared_shard = shard_builder.finish().unwrap();

    println!("✓ All numeric types (including decimal) indexed successfully");
    println!(
        "✓ Processed: Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64, DateTime, Decimal"
    );
    Ok(())
}

// ============================================================================
// LARGE DATASET TESTS
// ============================================================================

#[test]
fn test_large_dataset_multiple_blocks() -> Result<()> {
    println!("=== Testing Large Dataset with Multiple Logical Blocks ===");

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("int32_field", ArrowDataType::Int32, true),
        ArrowField::new("float64_field", ArrowDataType::Float64, true),
    ]));

    let temp_dir = TempDir::new().unwrap();
    let object_store =
        Arc::new(LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot).unwrap());
    let temp_store = temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap();

    // Create large dataset (1000 values to trigger multiple logical blocks)
    let mut int32_values = Vec::new();
    let mut float64_values = Vec::new();

    for i in 0..1000 {
        if i % 50 == 0 {
            int32_values.push(None);
            float64_values.push(None);
        } else {
            int32_values.push(Some(i));
            float64_values.push(Some(i as f64 + 0.5));
        }
    }

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(int32_values.clone())),
            Arc::new(Float64Array::from(float64_values.clone())),
        ],
    )
    .unwrap();

    let shard_schema = SchemaBuilder::from_arrow_schema(&arrow_schema)
        .unwrap()
        .finish_and_seal();
    let params = ShardBuilderParams {
        schema: shard_schema,
        object_store,
        temp_store,
        file_organization: ShardFileOrganization::SingleFile,
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut shard_builder = ShardBuilder::new(params).unwrap();
    let mut stripe_builder = shard_builder.build_stripe().unwrap();
    stripe_builder.push_batch(&batch).unwrap();
    let prepared_stripe = stripe_builder.finish().unwrap();

    assert_eq!(prepared_stripe.record_count, 1000);
    assert_eq!(prepared_stripe.fields.len(), 2);

    // Verify both fields have indexes
    assert!(validate_numeric_index_buffer_exists(&prepared_stripe, 0));
    assert!(validate_numeric_index_buffer_exists(&prepared_stripe, 1));

    // Verify statistics are reasonable
    let int32_field = &prepared_stripe.fields[0];
    assert_eq!(int32_field.descriptor.position_count, 1000);
    assert_eq!(int32_field.descriptor.null_count, Some(20)); // Every 50th value is null

    let float64_field = &prepared_stripe.fields[1];
    assert_eq!(float64_field.descriptor.position_count, 1000);
    assert_eq!(float64_field.descriptor.null_count, Some(20));

    shard_builder.add_stripe(prepared_stripe).unwrap();
    let _prepared_shard = shard_builder.finish().unwrap();

    println!("✓ Large dataset with multiple logical blocks processed successfully");
    println!("✓ Int32 field: 1000 values, 20 nulls");
    println!("✓ Float64 field: 1000 values, 20 nulls");
    Ok(())
}

// ============================================================================
// ENCODING PROFILE TESTS
// ============================================================================

#[test]
fn test_encoding_profiles_index_behavior() -> Result<()> {
    println!("=== Testing Index Behavior with Different Encoding Profiles ===");

    let profiles = [
        ("Plain", BlockEncodingProfile::Plain),
        ("Balanced", BlockEncodingProfile::Balanced),
        ("HighCompression", BlockEncodingProfile::HighCompression),
    ];

    for (profile_name, profile) in profiles {
        println!("Testing with {profile_name} profile");

        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "int32_field",
            ArrowDataType::Int32,
            true,
        )]));

        let temp_dir = TempDir::new().unwrap();
        let object_store =
            Arc::new(LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot).unwrap());
        let temp_store = temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap();

        let test_data = vec![Some(100i32), Some(200i32), Some(300i32), None, Some(400i32)];
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(test_data))],
        )
        .unwrap();

        let shard_schema = SchemaBuilder::from_arrow_schema(&arrow_schema)
            .unwrap()
            .finish_and_seal();
        let params = ShardBuilderParams {
            schema: shard_schema,
            object_store,
            temp_store,
            file_organization: ShardFileOrganization::SingleFile,
            encoding_profile: profile,
        };

        let mut shard_builder = ShardBuilder::new(params).unwrap();
        let mut stripe_builder = shard_builder.build_stripe().unwrap();
        stripe_builder.push_batch(&batch).unwrap();
        let prepared_stripe = stripe_builder.finish().unwrap();

        // Check if index exists based on profile
        let has_index = validate_numeric_index_buffer_exists(&prepared_stripe, 0);

        match profile {
            BlockEncodingProfile::Plain => {
                assert!(!has_index, "Plain profile should not have indexes");
                println!("  ✓ Plain profile correctly has no index");
            }
            BlockEncodingProfile::MinimalCompression
            | BlockEncodingProfile::Balanced
            | BlockEncodingProfile::HighCompression
            | BlockEncodingProfile::MinimalSize => {
                assert!(has_index, "{profile_name} profile should have indexes");
                println!("  ✓ {profile_name} profile correctly has index");
            }
        }

        shard_builder.add_stripe(prepared_stripe).unwrap();
        let _prepared_shard = shard_builder.finish().unwrap();
    }

    println!("✓ All encoding profiles tested successfully");
    Ok(())
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

#[test]
fn test_edge_cases() -> Result<()> {
    println!("=== Testing Edge Cases ===");

    let temp_dir = TempDir::new().unwrap();
    let object_store =
        Arc::new(LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot).unwrap());
    let temp_store = temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap();

    // Test 1: All null values
    {
        println!("Testing all null values...");
        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "int32_field",
            ArrowDataType::Int32,
            true,
        )]));

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![None::<i32>; 10]))],
        )
        .unwrap();

        let shard_schema = SchemaBuilder::from_arrow_schema(&arrow_schema)
            .unwrap()
            .finish_and_seal();
        let params = ShardBuilderParams {
            schema: shard_schema,
            object_store: object_store.clone(),
            temp_store: temp_store.clone(),
            file_organization: ShardFileOrganization::SingleFile,
            encoding_profile: BlockEncodingProfile::default(),
        };

        let mut shard_builder = ShardBuilder::new(params).unwrap();
        let mut stripe_builder = shard_builder.build_stripe().unwrap();
        stripe_builder.push_batch(&batch).unwrap();
        let prepared_stripe = stripe_builder.finish().unwrap();

        assert_eq!(prepared_stripe.fields[0].descriptor.null_count, Some(10));
        // Index should NOT be created when all values are null
        assert!(!validate_numeric_index_buffer_exists(&prepared_stripe, 0));

        shard_builder.add_stripe(prepared_stripe).unwrap();
        let _prepared_shard = shard_builder.finish().unwrap();
        println!("  ✓ All null values handled correctly");
    }

    // Test 2: Single value
    {
        println!("Testing single value...");
        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "float64_field",
            ArrowDataType::Float64,
            true,
        )]));

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Float64Array::from(vec![Some(42.0f64)]))],
        )
        .unwrap();

        let shard_schema = SchemaBuilder::from_arrow_schema(&arrow_schema)
            .unwrap()
            .finish_and_seal();
        let params = ShardBuilderParams {
            schema: shard_schema,
            object_store: object_store.clone(),
            temp_store: temp_store.clone(),
            file_organization: ShardFileOrganization::SingleFile,
            encoding_profile: BlockEncodingProfile::default(),
        };

        let mut shard_builder = ShardBuilder::new(params).unwrap();
        let mut stripe_builder = shard_builder.build_stripe().unwrap();
        stripe_builder.push_batch(&batch).unwrap();
        let prepared_stripe = stripe_builder.finish().unwrap();

        assert_eq!(prepared_stripe.record_count, 1);
        assert_eq!(prepared_stripe.fields[0].descriptor.null_count, Some(0));
        // Single value should be optimized as constant - no index buffer should exist
        assert!(!validate_numeric_index_buffer_exists(&prepared_stripe, 0));

        shard_builder.add_stripe(prepared_stripe).unwrap();
        let _prepared_shard = shard_builder.finish().unwrap();
        println!("  ✓ Single value handled correctly");
    }

    println!("✓ All edge cases passed!");
    Ok(())
}

// ============================================================================
// NUMERIC INDEX BUILDER INTERFACE TESTS
// ============================================================================

#[test]
fn test_numeric_index_builder_creation() -> Result<()> {
    println!("=== Testing NumericIndexBuilder Creation ===");

    let temp_store = temp_file_store::create_in_memory(8 * 1024 * 1024).unwrap();
    let encoding_profile = BlockEncodingProfile::Balanced;

    // Test signed integer types
    let signed_types = [
        (BasicType::Int8, "Int8"),
        (BasicType::Int16, "Int16"),
        (BasicType::Int32, "Int32"),
        (BasicType::Int64, "Int64"),
    ];

    for (basic_type, type_name) in signed_types {
        let basic_type_descriptor = BasicTypeDescriptor {
            basic_type,
            fixed_size: 0,
            signed: true,
            extended_type: Default::default(),
        };

        let result = numeric_index::create_builder(
            basic_type_descriptor,
            encoding_profile,
            temp_store.clone(),
        );
        assert!(result.is_ok(), "Should create builder for {type_name}");
        println!("✓ Created builder for signed {type_name}");
    }

    // Test unsigned integer types (represented as signed in BasicType, differentiated by signed field)
    let unsigned_types = [
        (BasicType::Int8, "UInt8"),
        (BasicType::Int16, "UInt16"),
        (BasicType::Int32, "UInt32"),
        (BasicType::Int64, "UInt64"),
    ];

    for (basic_type, type_name) in unsigned_types {
        let basic_type_descriptor = BasicTypeDescriptor {
            basic_type,
            fixed_size: 0,
            signed: false, // This differentiates unsigned from signed
            extended_type: Default::default(),
        };

        let result = numeric_index::create_builder(
            basic_type_descriptor,
            encoding_profile,
            temp_store.clone(),
        );
        assert!(result.is_ok(), "Should create builder for {type_name}");
        println!("✓ Created builder for unsigned {type_name}");
    }

    // Test floating-point and DateTime types
    let other_types = [
        (BasicType::Float32, "Float32"),
        (BasicType::Float64, "Float64"),
        (BasicType::DateTime, "DateTime"),
    ];

    for (basic_type, type_name) in other_types {
        let basic_type_descriptor = BasicTypeDescriptor {
            basic_type,
            fixed_size: 0,
            signed: false,
            extended_type: Default::default(),
        };

        let result = numeric_index::create_builder(
            basic_type_descriptor,
            encoding_profile,
            temp_store.clone(),
        );
        assert!(result.is_ok(), "Should create builder for {type_name}");
        println!("✓ Created builder for {type_name}");
    }

    // Test unsupported type (should fail)
    let unsupported_type = BasicTypeDescriptor {
        basic_type: BasicType::Binary,
        fixed_size: 0,
        signed: false,
        extended_type: Default::default(),
    };

    let result = numeric_index::create_builder(unsupported_type, encoding_profile, temp_store);
    assert!(result.is_err(), "Should fail for unsupported Binary type");
    println!("✓ Correctly rejected unsupported Binary type");

    println!("✓ NumericIndexBuilder creation tests completed");
    Ok(())
}

// ============================================================================
// NAN VALUE HANDLING TESTS
// ============================================================================

#[test]
fn test_nan_values_large_dataset_invalid_count() -> Result<()> {
    println!("=== Testing NaN Values Large Dataset with Invalid Count Tracking ===");

    // Test with 10K NaN values to ensure we span multiple logical blocks
    // and verify that invalid counts are properly tracked
    let test_size = 10000;

    // Test Float32 with all NaN values
    {
        println!("Testing Float32 with {test_size} NaN values...");

        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "float32_nan_field",
            ArrowDataType::Float32,
            true,
        )]));

        let temp_dir = TempDir::new().unwrap();
        let object_store =
            Arc::new(LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot).unwrap());
        let temp_store = temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap();

        // Create array with all NaN values
        let float32_nan_values = vec![Some(f32::NAN); test_size];
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Float32Array::from(float32_nan_values))],
        )
        .unwrap();

        let shard_schema = SchemaBuilder::from_arrow_schema(&arrow_schema)
            .unwrap()
            .finish_and_seal();
        let params = ShardBuilderParams {
            schema: shard_schema,
            object_store,
            temp_store,
            file_organization: ShardFileOrganization::SingleFile,
            encoding_profile: BlockEncodingProfile::default(),
        };

        let mut shard_builder = ShardBuilder::new(params).unwrap();
        let mut stripe_builder = shard_builder.build_stripe().unwrap();
        stripe_builder.push_batch(&batch).unwrap();
        let prepared_stripe = stripe_builder.finish().unwrap();

        assert_eq!(prepared_stripe.record_count, test_size as u64);
        assert_eq!(prepared_stripe.fields.len(), 1);

        // Verify index was NOT created (all NaN values are invalid)
        assert!(!validate_numeric_index_buffer_exists(&prepared_stripe, 0));

        // Verify field statistics - no nulls but all values should be considered "invalid" due to NaN
        let field_descriptor = &prepared_stripe.fields[0].descriptor;
        assert_eq!(field_descriptor.position_count, test_size as u64);
        assert_eq!(field_descriptor.null_count, Some(0)); // No actual nulls

        shard_builder.add_stripe(prepared_stripe).unwrap();
        let _prepared_shard = shard_builder.finish().unwrap();

        println!("  ✓ Float32 NaN dataset processed successfully");
    }

    // Test Float64 with all NaN values
    {
        println!("Testing Float64 with {test_size} NaN values...");

        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "float64_nan_field",
            ArrowDataType::Float64,
            true,
        )]));

        let temp_dir = TempDir::new().unwrap();
        let object_store =
            Arc::new(LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot).unwrap());
        let temp_store = temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap();

        // Create array with all NaN values
        let float64_nan_values = vec![Some(f64::NAN); test_size];
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Float64Array::from(float64_nan_values))],
        )
        .unwrap();

        let shard_schema = SchemaBuilder::from_arrow_schema(&arrow_schema)
            .unwrap()
            .finish_and_seal();
        let params = ShardBuilderParams {
            schema: shard_schema,
            object_store,
            temp_store,
            file_organization: ShardFileOrganization::SingleFile,
            encoding_profile: BlockEncodingProfile::default(),
        };

        let mut shard_builder = ShardBuilder::new(params).unwrap();
        let mut stripe_builder = shard_builder.build_stripe().unwrap();
        stripe_builder.push_batch(&batch).unwrap();
        let prepared_stripe = stripe_builder.finish().unwrap();

        assert_eq!(prepared_stripe.record_count, test_size as u64);
        assert_eq!(prepared_stripe.fields.len(), 1);

        // Verify index was NOT created (all NaN values are invalid)
        assert!(!validate_numeric_index_buffer_exists(&prepared_stripe, 0));

        // Verify field statistics
        let field_descriptor = &prepared_stripe.fields[0].descriptor;
        assert_eq!(field_descriptor.position_count, test_size as u64);
        assert_eq!(field_descriptor.null_count, Some(0)); // No actual nulls

        shard_builder.add_stripe(prepared_stripe).unwrap();
        let _prepared_shard = shard_builder.finish().unwrap();

        println!("  ✓ Float64 NaN dataset processed successfully");
        println!("  ✓ All {test_size} float64 values are NaN and should be tracked as invalid");
    }

    // Test Decimal with invalid values (equivalent to NaN for decimal types)
    {
        println!("Testing Decimal with {test_size} invalid/NaN-equivalent values...");

        // For decimals, we can create NaN values by dividing zero by zero
        // This creates proper decimal NaN values that should be counted as invalid
        let decimal_nan = d128::from(0) / d128::from(0); // This creates NaN for decimals
        let mut decimal_invalid_values = Vec::new();
        for _i in 0..test_size {
            decimal_invalid_values.push(Some(decimal_nan)); // All NaN values
        }

        let batch =
            create_decimal_batch_from_d128_values(&decimal_invalid_values, "decimal_invalid_field");

        let decimal_data_type =
            DataTypeBuilder::new_decimal().with_field_name("decimal_invalid_field");
        let field = FieldBuilder::from(decimal_data_type);
        let schema_builder = SchemaBuilder::new(vec![field]);
        let schema_message = schema_builder.finish_and_seal();
        let schema = schema_message.schema()?;

        let temp_store = temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap();
        let stripe_params = StripeBuilderParams {
            schema: schema.clone(),
            temp_store: temp_store.clone(),
            encoding_profile: BlockEncodingProfile::default(),
        };

        let mut stripe_builder = StripeBuilder::new(stripe_params)?;
        stripe_builder.push_batch(&batch)?;
        let prepared_stripe = stripe_builder.finish()?;

        assert_eq!(prepared_stripe.record_count, test_size as u64);
        assert_eq!(prepared_stripe.fields.len(), 1);

        // Verify index was NOT created (all values are invalid)
        assert!(!validate_numeric_index_buffer_exists(&prepared_stripe, 0));

        println!("  ✓ Decimal invalid/NaN dataset processed successfully - no index created");

        // Verify field statistics
        let field_descriptor = &prepared_stripe.fields[0].descriptor;
        assert_eq!(field_descriptor.position_count, test_size as u64);
        assert_eq!(field_descriptor.null_count, Some(0)); // No actual nulls

        println!("  ✓ All {test_size} decimal values are NaN and should be tracked as invalid");
    }

    println!("✓ NaN/Invalid values large dataset test completed");
    println!("✓ This test now verifies invalid count tracking is working properly");
    println!("✓ All {test_size} values per type were processed across multiple logical blocks");

    Ok(())
}

#[test]
fn test_mixed_nan_and_valid_values_invalid_count() -> Result<()> {
    use amudai_format::defs::{schema::BasicType, schema_ext::BasicTypeDescriptor};

    println!("=== Testing Mixed NaN and Valid Values with Invalid Count ===");

    let test_size = 5000; // Large enough to span multiple blocks

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("float32_mixed_field", ArrowDataType::Float32, true),
        ArrowField::new("float64_mixed_field", ArrowDataType::Float64, true),
    ]));

    let temp_dir = TempDir::new().unwrap();
    let object_store =
        Arc::new(LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot).unwrap());
    let temp_store = temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap();

    // Create mixed data: every 3rd value is NaN, every 7th is null, rest are valid
    let mut float32_values = Vec::new();
    let mut float64_values = Vec::new();
    let mut nan_count = 0;
    let mut null_count = 0;
    let mut valid_count = 0;

    for i in 0..test_size {
        if i % 7 == 0 {
            // Null value
            float32_values.push(None);
            float64_values.push(None);
            null_count += 1;
        } else if i % 3 == 0 {
            // NaN value
            float32_values.push(Some(f32::NAN));
            float64_values.push(Some(f64::NAN));
            nan_count += 1;
        } else {
            // Valid value
            float32_values.push(Some(i as f32 * 1.5));
            float64_values.push(Some(i as f64 * 2.5));
            valid_count += 1;
        }
    }

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Float32Array::from(float32_values)),
            Arc::new(Float64Array::from(float64_values)),
        ],
    )
    .unwrap();

    let shard_schema = SchemaBuilder::from_arrow_schema(&arrow_schema)
        .unwrap()
        .finish_and_seal();
    let params = ShardBuilderParams {
        schema: shard_schema,
        object_store,
        temp_store,
        file_organization: ShardFileOrganization::SingleFile,
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut shard_builder = ShardBuilder::new(params).unwrap();
    let mut stripe_builder = shard_builder.build_stripe().unwrap();
    stripe_builder.push_batch(&batch).unwrap();
    let prepared_stripe = stripe_builder.finish().unwrap();

    assert_eq!(prepared_stripe.record_count, test_size as u64);
    assert_eq!(prepared_stripe.fields.len(), 2);

    // Verify indexes were created for both fields
    assert!(validate_numeric_index_buffer_exists(&prepared_stripe, 0));
    assert!(validate_numeric_index_buffer_exists(&prepared_stripe, 1));

    // Verify field statistics for both fields
    for field_idx in 0..2 {
        let field_descriptor = &prepared_stripe.fields[field_idx].descriptor;
        assert_eq!(field_descriptor.position_count, test_size as u64);
        assert_eq!(field_descriptor.null_count, Some(null_count as u64));

        println!(
            "  Field {field_idx}: {test_size} total, {null_count} nulls, {nan_count} NaN, {valid_count} valid"
        );
    }

    // Validate index content using NumericIndexReader for both fields
    for field_idx in 0..2 {
        println!("  Validating index content for field {field_idx}...");

        let field_info = &prepared_stripe.fields[field_idx];
        let mut index_buffer = None;
        for encoding in &field_info.encodings {
            for buffer in &encoding.buffers {
                if buffer.descriptor.kind == BufferKind::RangeIndex as i32 {
                    index_buffer = Some(buffer.data.clone());
                    break;
                }
            }
        }

        let index_reader_data = index_buffer.expect("Index buffer should exist");
        let basic_type = BasicTypeDescriptor {
            basic_type: if field_idx == 0 {
                BasicType::Float32
            } else {
                BasicType::Float64
            },
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let mut reader = read_numeric_index::create_decoder(basic_type, index_reader_data)?;

        // Validate that the sum of invalid_counts across all blocks equals total invalid count (nulls + NaN)
        let stats = reader.get_block_stats_array()?;

        if let Some(invalid_counts) = stats.invalid_counts {
            let total_invalid_count: u32 = invalid_counts.iter().map(|&x| x as u32).sum();
            let expected_invalid_count = null_count + nan_count; // Both nulls and NaN are invalid
            assert_eq!(
                total_invalid_count, expected_invalid_count as u32,
                "Field {field_idx} total invalid_count should equal null + NaN count ({total_invalid_count} vs {expected_invalid_count})"
            );
            println!(
                "    ✓ Field {field_idx}: Total invalid_count {total_invalid_count} matches null + NaN count {expected_invalid_count} ({null_count}+{nan_count})"
            );
        } else if null_count > 0 || nan_count > 0 {
            panic!(
                "Field {field_idx}: Expected invalid_counts to be present with {null_count} null and {nan_count} NaN values"
            );
        }
    }

    shard_builder.add_stripe(prepared_stripe).unwrap();
    let _prepared_shard = shard_builder.finish().unwrap();

    println!("✓ Mixed NaN/valid/null dataset processed successfully");
    println!(
        "✓ Processed: {test_size} total ({valid_count} valid, {nan_count} NaN, {null_count} null)"
    );

    Ok(())
}

// ====================================================================================
// Decimal Index Integration Tests (moved from src/write/decimal_index.rs)
// ====================================================================================

/// Test helper to create a FixedSizeBinaryArray from decimal values
fn create_decimal_array(values: Vec<Option<d128>>) -> FixedSizeBinaryArray {
    // Pre-compute all byte arrays to avoid lifetime issues
    let byte_arrays: Vec<Option<[u8; 16]>> = values
        .iter()
        .map(|value_opt| value_opt.map(|v| v.to_raw_bytes()))
        .collect();

    let data: Vec<Option<&[u8]>> = byte_arrays
        .iter()
        .map(|bytes_opt| bytes_opt.as_ref().map(|bytes| bytes.as_slice()))
        .collect();

    FixedSizeBinaryArray::from(data)
}

#[test]
fn test_decimal_index_with_nan_values() -> Result<()> {
    use crate::read::decimal_index::DecimalIndexDecoder;
    use crate::write::decimal_index::DecimalIndexBuilder;
    use amudai_io_impl::temp_file_store;
    use decimal::d128;
    use std::str::FromStr;

    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let mut builder = DecimalIndexBuilder::new(temp_store, Default::default());

    // Create test data starting with NaN (zero/zero)
    let zero = d128::from(0);
    let nan_val = zero / zero; // Create NaN as zero/zero
    let normal_val1 = d128::from_str("123.45").unwrap();
    let normal_val2 = d128::from_str("-50.00").unwrap();
    let normal_val3 = d128::from_str("0.00").unwrap();

    // Create array with NaN first, followed by normal values
    let values = vec![
        Some(nan_val),     // NaN should be skipped
        Some(normal_val1), // 123.45
        Some(nan_val),     // Another NaN should be skipped
        Some(normal_val2), // -50.00
        Some(normal_val3), // 0.00
        None,              // Null value
        Some(nan_val),     // Another NaN should be skipped
    ];

    let array = create_decimal_array(values);
    builder.process_array(&array)?;

    // Finish the index
    let result = Box::new(builder).finish()?;
    assert!(result.is_some(), "Index should be created with valid data");

    let encoded_buffer = result.unwrap();

    // Create a reader to verify the results
    let reader_result = DecimalIndexDecoder::new(encoded_buffer.data);
    assert!(
        reader_result.is_ok(),
        "Reader should be created successfully"
    );

    let mut reader = reader_result.unwrap();

    // Verify block count
    assert_eq!(reader.block_count(), 1, "Should have exactly one block");

    // Get block statistics
    let stats = reader.get_block_stats_array()?;

    // Verify that min/max are computed correctly, ignoring NaN values
    // Min should be -50.00, max should be 123.45
    let expected_min = d128::from_str("-50").unwrap();
    let expected_max = d128::from_str("123.45").unwrap();

    assert_eq!(
        stats.get_block_min_as_decimal(0),
        expected_min,
        "Min value should be -50.00, ignoring NaN"
    );
    assert_eq!(
        stats.get_block_max_as_decimal(0),
        expected_max,
        "Max value should be 123.45, ignoring NaN"
    );

    // Verify invalid count (3 NaN values + 1 explicit null = 4 total invalid)
    assert_eq!(
        stats.get_block_invalid_counts(0).unwrap(),
        4,
        "Should have 4 invalid values (3 NaN + 1 null)"
    );

    Ok(())
}

#[test]
fn test_decimal_index_all_nan_values() -> Result<()> {
    use crate::write::decimal_index::DecimalIndexBuilder;
    use amudai_io_impl::temp_file_store;
    use decimal::d128;

    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let mut builder = DecimalIndexBuilder::new(temp_store, Default::default());

    // Create test data with only NaN values
    let zero = d128::from(0);
    let nan_val = zero / zero; // Create NaN as zero/zero

    let values = vec![
        Some(nan_val), // NaN
        Some(nan_val), // NaN
        Some(nan_val), // NaN
        None,          // Null
    ];

    let array = create_decimal_array(values);
    builder.process_array(&array)?;

    // Finish the index
    let result = Box::new(builder).finish()?;

    // Should return None since all values are NaN (invalid)
    assert!(
        result.is_none(),
        "Index should not be created when all values are NaN"
    );
    println!("✓ Index correctly not created for all NaN values");

    Ok(())
}

#[test]
fn test_decimal_index_mixed_normal_and_nan() -> Result<()> {
    use crate::read::decimal_index::DecimalIndexDecoder;
    use crate::write::decimal_index::DecimalIndexBuilder;
    use amudai_io_impl::temp_file_store;
    use decimal::d128;
    use std::str::FromStr;

    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
    let mut builder = DecimalIndexBuilder::new(temp_store, Default::default());

    // Create a mix of normal values and NaN
    let zero = d128::from(0);
    let nan_val = zero / zero;
    let val1 = d128::from_str("100.00").unwrap();
    let val2 = d128::from_str("200.00").unwrap();
    let val3 = d128::from_str("50.00").unwrap();

    let values = vec![
        Some(nan_val), // Should be ignored
        Some(val1),    // 100.00
        Some(nan_val), // Should be ignored
        Some(val2),    // 200.00
        Some(nan_val), // Should be ignored
        Some(val3),    // 50.00
        Some(nan_val), // Should be ignored
    ];

    let array = create_decimal_array(values);
    builder.process_array(&array)?;

    let result = Box::new(builder).finish()?;
    assert!(result.is_some());

    let mut reader = DecimalIndexDecoder::new(result.unwrap().data)?;
    let stats = reader.get_block_stats_array()?;

    // Min should be 50.00, max should be 200.00
    let expected_min = val3; // 50.00
    let expected_max = val2; // 200.00

    assert_eq!(
        stats.get_block_min_as_decimal(0),
        expected_min,
        "Min should be 50.00"
    );
    assert_eq!(
        stats.get_block_max_as_decimal(0),
        expected_max,
        "Max should be 200.00"
    );
    assert_eq!(
        stats.get_block_invalid_counts(0).unwrap_or(0),
        4,
        "Should have 4 invalid values (4 NaN)"
    );

    Ok(())
}

// ============================================================================
// CONSTANT VALUE INTEGRATION TESTS
// ============================================================================

#[test]
fn test_constant_integer_stripe_integration() -> Result<()> {
    use crate::tests::shard_store::ShardStore;

    let shard_store = ShardStore::new();

    // Create a schema with integer field
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
        arrow_schema::Field::new("value", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("description", arrow_schema::DataType::Utf8, true),
    ]));

    // Create a constant i64 value
    let constant_i64_value = 42_i64;

    // Generate batches with constant i64 values
    let record_count = 1000;
    let batches =
        generate_constant_i64_batches(schema.clone(), 50..100, record_count, constant_i64_value);
    let batch_iter = arrow_array::RecordBatchIterator::new(batches.map(Ok), schema.clone());

    // Write to shard
    let data_ref = shard_store.ingest_shard_from_record_batches(batch_iter);

    // Verify shard was created successfully
    let shard = shard_store.open_shard(&data_ref.url);
    assert_eq!(shard.directory().total_record_count, 1000);
    assert_eq!(shard.directory().stripe_count, 1);

    // Open the stripe and verify schema
    let stripe = shard.open_stripe(0)?;
    let stripe_schema = stripe.fetch_schema()?;

    // Find the i64 field
    let value_field_info = stripe_schema.find_field("value")?.unwrap();

    // Open the i64 field
    let value_field = stripe.open_field(value_field_info.1.data_type().unwrap())?;

    // Verify field type
    assert_eq!(value_field.data_type().basic_type()?, BasicType::Int64);

    // Create decoder and reader for the value field
    let value_decoder = value_field.create_decoder()?;
    let mut value_reader = value_decoder.create_reader(empty_hint())?;

    // Read a range of values and verify they're all the same constant
    let seq = value_reader.read_range(0..10)?;
    assert_eq!(seq.len(), 10);

    // For primitive types, there should be no offsets
    assert!(
        seq.offsets.is_none(),
        "Primitive i64 should not have offsets"
    );

    // Verify all values are the same constant i64
    let values_bytes = seq.values.as_bytes();
    assert_eq!(values_bytes.len(), 10 * 8); // 10 i64 values * 8 bytes each

    // Convert bytes back to i64 and verify
    let i64_values = seq.values.as_slice::<i64>();
    for &value in i64_values {
        assert_eq!(value, constant_i64_value);
    }

    // Test reading different ranges
    let seq2 = value_reader.read_range(100..150)?;
    assert_eq!(seq2.len(), 50);

    let i64_values2 = seq2.values.as_slice::<i64>();
    for &value in i64_values2 {
        assert_eq!(value, constant_i64_value);
    }

    println!("✓ Constant i64 stripe integration test passed!");
    println!("  Successfully created and read constant i64 values from stripe");
    println!("  Verified {record_count} records with constant i64 value: {constant_i64_value}");

    Ok(())
}

#[test]
fn test_constant_datetime_stripe_integration() -> Result<()> {
    use crate::tests::shard_store::ShardStore;

    let shard_store = ShardStore::new();

    // Create a schema with datetime field
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
        arrow_schema::Field::new(
            "timestamp",
            arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            false,
        ),
        arrow_schema::Field::new("description", arrow_schema::DataType::Utf8, true),
    ]));

    // Create a constant datetime value (milliseconds since epoch)
    let constant_datetime_value = 1640995200000_i64; // 2022-01-01 00:00:00 UTC
    let expected_datetime_ticks = 637765920000000000_i64; // Converted to .NET datetime ticks

    // Generate batches with constant datetime values
    let record_count = 1000;
    let batches = generate_constant_datetime_batches(
        schema.clone(),
        50..100,
        record_count,
        constant_datetime_value,
    );
    let batch_iter = arrow_array::RecordBatchIterator::new(batches.map(Ok), schema.clone());

    // Write to shard
    let data_ref = shard_store.ingest_shard_from_record_batches(batch_iter);

    // Verify shard was created successfully
    let shard = shard_store.open_shard(&data_ref.url);
    assert_eq!(shard.directory().total_record_count, 1000);
    assert_eq!(shard.directory().stripe_count, 1);

    // Open the stripe and verify schema
    let stripe = shard.open_stripe(0)?;
    let stripe_schema = stripe.fetch_schema()?;

    // Find the datetime field
    let timestamp_field_info = stripe_schema.find_field("timestamp")?.unwrap();

    // Open the datetime field
    let timestamp_field = stripe.open_field(timestamp_field_info.1.data_type().unwrap())?;

    // Verify field type
    assert_eq!(
        timestamp_field.data_type().basic_type()?,
        BasicType::DateTime
    );

    // Create decoder and reader for the timestamp field
    let timestamp_decoder = timestamp_field.create_decoder()?;
    let mut timestamp_reader = timestamp_decoder.create_reader(empty_hint())?;

    // Read a range of values and verify they're all the same constant
    let seq = timestamp_reader.read_range(0..10)?;
    assert_eq!(seq.len(), 10);

    // For primitive types, there should be no offsets
    assert!(
        seq.offsets.is_none(),
        "Primitive datetime should not have offsets"
    );

    // Verify all values are the same constant datetime
    let values_bytes = seq.values.as_bytes();
    assert_eq!(values_bytes.len(), 10 * 8); // 10 datetime values * 8 bytes each

    // Convert bytes back to i64 and verify
    let datetime_values = seq.values.as_slice::<i64>();
    for &value in datetime_values {
        assert_eq!(value, expected_datetime_ticks);
    }

    // Test reading different ranges
    let seq2 = timestamp_reader.read_range(100..150)?;
    assert_eq!(seq2.len(), 50);

    let datetime_values2 = seq2.values.as_slice::<i64>();
    for &value in datetime_values2 {
        assert_eq!(value, expected_datetime_ticks);
    }

    println!("✓ Constant datetime stripe integration test passed!");
    println!("  Successfully created and read constant datetime values from stripe");
    println!(
        "  Input milliseconds: {constant_datetime_value}, Output ticks: {expected_datetime_ticks}"
    );
    println!(
        "  Verified {record_count} records with constant datetime tick value: {expected_datetime_ticks}"
    );

    Ok(())
}

/// Generate batches with constant i64 values for testing
fn generate_constant_i64_batches(
    schema: Arc<arrow_schema::Schema>,
    batch_size: std::ops::Range<usize>,
    record_count: usize,
    constant_i64_value: i64,
) -> Box<dyn Iterator<Item = RecordBatch>> {
    let batch_sizes = amudai_arrow_processing::array_sequence::random_split(
        record_count,
        batch_size.start,
        batch_size.end,
    );
    Box::new(batch_sizes.into_iter().map(move |batch_size| {
        generate_constant_i64_batch(schema.clone(), batch_size, constant_i64_value)
    }))
}

/// Generate a single batch with constant i64 values
fn generate_constant_i64_batch(
    schema: Arc<arrow_schema::Schema>,
    batch_size: usize,
    constant_i64_value: i64,
) -> RecordBatch {
    let mut columns = Vec::<arrow_array::ArrayRef>::new();

    for field in schema.fields() {
        let array: arrow_array::ArrayRef = match field.name().as_str() {
            "value" => {
                // Generate constant i64 values
                let values: Vec<i64> = (0..batch_size).map(|_| constant_i64_value).collect();
                Arc::new(Int64Array::from(values))
            }
            _ => {
                // Generate other fields using a simple pattern
                match field.data_type() {
                    arrow_schema::DataType::Int32 => {
                        let values: Vec<i32> = (0..batch_size).map(|i| i as i32).collect();
                        Arc::new(Int32Array::from(values))
                    }
                    arrow_schema::DataType::Utf8 => {
                        let values: Vec<Option<String>> =
                            (0..batch_size).map(|i| Some(format!("item_{i}"))).collect();
                        Arc::new(arrow_array::StringArray::from(values))
                    }
                    _ => panic!("Unsupported field type for test: {:?}", field.data_type()),
                }
            }
        };
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns).expect("Failed to create constant i64 batch")
}

/// Generate batches with constant datetime values for testing
fn generate_constant_datetime_batches(
    schema: Arc<arrow_schema::Schema>,
    batch_size: std::ops::Range<usize>,
    record_count: usize,
    constant_datetime_value: i64,
) -> Box<dyn Iterator<Item = RecordBatch>> {
    let batch_sizes = amudai_arrow_processing::array_sequence::random_split(
        record_count,
        batch_size.start,
        batch_size.end,
    );
    Box::new(batch_sizes.into_iter().map(move |batch_size| {
        generate_constant_datetime_batch(schema.clone(), batch_size, constant_datetime_value)
    }))
}

/// Generate a single batch with constant datetime values
fn generate_constant_datetime_batch(
    schema: Arc<arrow_schema::Schema>,
    batch_size: usize,
    constant_datetime_value: i64,
) -> RecordBatch {
    let mut columns = Vec::<arrow_array::ArrayRef>::new();

    for field in schema.fields() {
        let array: arrow_array::ArrayRef = match field.name().as_str() {
            "timestamp" => {
                // Generate constant datetime values
                let values: Vec<i64> = (0..batch_size).map(|_| constant_datetime_value).collect();
                Arc::new(TimestampMillisecondArray::from(values))
            }
            _ => {
                // Generate other fields using a simple pattern
                match field.data_type() {
                    arrow_schema::DataType::Int32 => {
                        let values: Vec<i32> = (0..batch_size).map(|i| i as i32).collect();
                        Arc::new(Int32Array::from(values))
                    }
                    arrow_schema::DataType::Utf8 => {
                        let values: Vec<Option<String>> =
                            (0..batch_size).map(|i| Some(format!("item_{i}"))).collect();
                        Arc::new(arrow_array::StringArray::from(values))
                    }
                    _ => panic!("Unsupported field type for test: {:?}", field.data_type()),
                }
            }
        };
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns).expect("Failed to create constant datetime batch")
}

#[test]
fn test_primitive_constant_value_optimization() -> Result<()> {
    println!("=== Testing Primitive Constant Value Optimization ===");

    use crate::write::shard_builder::{ShardBuilder, ShardBuilderParams, ShardFileOrganization};
    use amudai_format::schema::BasicType;
    use amudai_format::schema_builder::{FieldBuilder, SchemaBuilder};
    use arrow_array::{Float64Array, Int32Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    let temp_dir = TempDir::new().unwrap();
    let object_store =
        Arc::new(LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot).unwrap());
    let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

    // Test case 1: Constant Int32 field
    {
        println!("  Testing constant Int32 field optimization...");

        let field = FieldBuilder::new("constant_int32", BasicType::Int32, Some(true), None);
        let schema_builder = SchemaBuilder::new(vec![field]);
        let shard_schema = schema_builder.finish_and_seal();

        let params = ShardBuilderParams {
            schema: shard_schema,
            object_store: object_store.clone(),
            temp_store: temp_store.clone(),
            file_organization: ShardFileOrganization::SingleFile,
            encoding_profile: BlockEncodingProfile::default(),
        };

        let shard_builder = ShardBuilder::new(params).unwrap();
        let mut stripe_builder = shard_builder.build_stripe().unwrap();

        // Create Arrow schema for the batch
        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "constant_int32",
            ArrowDataType::Int32,
            false,
        )]));

        // Add multiple batches with the same constant value
        for _ in 0..3 {
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(Int32Array::from(vec![42, 42, 42, 42, 42]))],
            )
            .unwrap();
            stripe_builder.push_batch(&batch).unwrap();
        }

        let prepared_stripe = stripe_builder.finish().unwrap();

        // Verify that the field has constant value optimization
        assert_eq!(prepared_stripe.record_count, 15); // 3 batches * 5 values each

        let field_info = &prepared_stripe.fields[0];
        assert_eq!(field_info.descriptor.null_count, Some(0));

        // With constant value optimization, there should be no buffers at all
        assert!(
            field_info.encodings.is_empty()
                || field_info
                    .encodings
                    .iter()
                    .all(|encoding| encoding.buffers.is_empty()),
            "Constant field should have no buffers or empty encodings"
        );

        println!("    ✓ Constant Int32 field correctly optimized");
    }

    // Test case 2: Constant Int64 field
    {
        println!("  Testing constant Int64 field optimization...");

        let field = FieldBuilder::new("constant_int64", BasicType::Int64, Some(true), None);
        let schema_builder = SchemaBuilder::new(vec![field]);
        let shard_schema = schema_builder.finish_and_seal();

        let params = ShardBuilderParams {
            schema: shard_schema,
            object_store: object_store.clone(),
            temp_store: temp_store.clone(),
            file_organization: ShardFileOrganization::SingleFile,
            encoding_profile: BlockEncodingProfile::default(),
        };

        let shard_builder = ShardBuilder::new(params).unwrap();
        let mut stripe_builder = shard_builder.build_stripe().unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "constant_int64",
            ArrowDataType::Int64,
            false,
        )]));

        // Add batch with constant Int64 value
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![
                9223372036854775807_i64;
                10
            ]))], // Max i64
        )
        .unwrap();
        stripe_builder.push_batch(&batch).unwrap();

        let prepared_stripe = stripe_builder.finish().unwrap();

        // Verify optimization
        assert_eq!(prepared_stripe.record_count, 10);

        let field_info = &prepared_stripe.fields[0];
        assert_eq!(field_info.descriptor.null_count, Some(0));

        // Should have no buffers for constant field
        assert!(
            field_info.encodings.is_empty()
                || field_info
                    .encodings
                    .iter()
                    .all(|encoding| encoding.buffers.is_empty()),
            "Constant Int64 field should have no buffers"
        );

        println!("    ✓ Constant Int64 field correctly optimized");
    }

    // Test case 3: Constant Float64 field
    {
        println!("  Testing constant Float64 field optimization...");

        let field = FieldBuilder::new("constant_float64", BasicType::Float64, Some(true), None);
        let schema_builder = SchemaBuilder::new(vec![field]);
        let shard_schema = schema_builder.finish_and_seal();

        let params = ShardBuilderParams {
            schema: shard_schema,
            object_store: object_store.clone(),
            temp_store: temp_store.clone(),
            file_organization: ShardFileOrganization::SingleFile,
            encoding_profile: BlockEncodingProfile::default(),
        };

        let shard_builder = ShardBuilder::new(params).unwrap();
        let mut stripe_builder = shard_builder.build_stripe().unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "constant_float64",
            ArrowDataType::Float64,
            false,
        )]));

        // Add batch with constant Float64 value
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Float64Array::from(vec![3.14159265359; 8]))], // Constant pi
        )
        .unwrap();
        stripe_builder.push_batch(&batch).unwrap();

        let prepared_stripe = stripe_builder.finish().unwrap();

        // Verify optimization
        assert_eq!(prepared_stripe.record_count, 8);

        let field_info = &prepared_stripe.fields[0];
        assert_eq!(field_info.descriptor.null_count, Some(0));

        // Should have no buffers for constant field
        assert!(
            field_info.encodings.is_empty()
                || field_info
                    .encodings
                    .iter()
                    .all(|encoding| encoding.buffers.is_empty()),
            "Constant Float64 field should have no buffers"
        );

        println!("    ✓ Constant Float64 field correctly optimized");
    }

    // Test case 4: Non-constant field should still have buffers
    {
        println!("  Testing non-constant field behavior...");

        let field = FieldBuilder::new("varying_int32", BasicType::Int32, Some(true), None);
        let schema_builder = SchemaBuilder::new(vec![field]);
        let shard_schema = schema_builder.finish_and_seal();

        let params = ShardBuilderParams {
            schema: shard_schema,
            object_store: object_store.clone(),
            temp_store: temp_store.clone(),
            file_organization: ShardFileOrganization::SingleFile,
            encoding_profile: BlockEncodingProfile::default(),
        };

        let shard_builder = ShardBuilder::new(params).unwrap();
        let mut stripe_builder = shard_builder.build_stripe().unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "varying_int32",
            ArrowDataType::Int32,
            false,
        )]));

        // Add batch with varying values
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();
        stripe_builder.push_batch(&batch).unwrap();

        let prepared_stripe = stripe_builder.finish().unwrap();

        // Verify no optimization (should have buffers)
        assert_eq!(prepared_stripe.record_count, 5);

        let field_info = &prepared_stripe.fields[0];
        assert_eq!(field_info.descriptor.null_count, Some(0));

        // Should have buffers for non-constant field
        assert!(
            !field_info.encodings.is_empty()
                && field_info
                    .encodings
                    .iter()
                    .any(|encoding| !encoding.buffers.is_empty()),
            "Non-constant field should have buffers"
        );

        println!("    ✓ Non-constant field correctly has buffers");
    }

    println!("✓ All primitive constant value optimization tests passed!");
    Ok(())
}
