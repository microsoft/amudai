//! Integration tests for bytes field encoding/decoding with constant value optimization
//!
//! This module tests the complete pipeline for bytes-based fields including:
//! - Constant strings (under 128 bytes)
//! - Constant GUIDs
//! - Constant binary data
//! - Mixed scenarios with nulls and constants
//!
//! Tests verify that constant value optimization works correctly at the stripe level,
//! ensuring efficient storage and retrieval of repeated byte values.

use amudai_arrow_compat::arrow_fields::make_guid;
use amudai_common::Result;
use amudai_format::schema::BasicType;
use arrow_array::{
    ArrayRef, BinaryArray, FixedSizeBinaryArray, RecordBatch, RecordBatchIterator, StringArray,
};
use std::sync::Arc;

use crate::tests::shard_store::ShardStore;

// ============================================================================
// CONSTANT STRING INTEGRATION TESTS
// ============================================================================

#[test]
fn test_constant_string_stripe_integration() -> Result<()> {
    let shard_store = ShardStore::new();

    // Create a schema with string field
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
        arrow_schema::Field::new("message", arrow_schema::DataType::Utf8, false),
        arrow_schema::Field::new("description", arrow_schema::DataType::Utf8, true),
    ]));

    // Create a constant string value (under 128 bytes)
    let constant_string_value = "Hello, World! This is a constant string value for testing.";

    // Generate batches with constant string values
    let record_count = 1000;
    let batches = generate_constant_string_batches(
        schema.clone(),
        50..100,
        record_count,
        constant_string_value,
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

    // Find the string field
    let message_field_info = stripe_schema.find_field("message")?.unwrap();

    // Open the string field
    let message_field = stripe.open_field(message_field_info.1.data_type().unwrap())?;

    // Verify field type
    assert_eq!(message_field.data_type().basic_type()?, BasicType::String);

    // Create decoder and reader for the message field
    let message_decoder = message_field.create_decoder()?;
    let mut message_reader = message_decoder.create_reader_with_ranges(std::iter::empty())?;

    // Read a range of values and verify they're all the same constant
    let seq = message_reader.read_range(0..10)?;
    assert_eq!(seq.len(), 10);

    // For bytes types, there should be offsets
    assert!(seq.offsets.is_some(), "String field should have offsets");

    // Verify all values are the same constant string
    let offsets = seq.offsets.as_ref().unwrap();
    let values_bytes = seq.values.as_bytes();

    for i in 0..10 {
        let start = offsets[i] as usize;
        let end = offsets[i + 1] as usize;
        let string_value = std::str::from_utf8(&values_bytes[start..end]).unwrap();
        assert_eq!(string_value, constant_string_value);
    }

    // Test reading different ranges
    let seq2 = message_reader.read_range(100..150)?;
    assert_eq!(seq2.len(), 50);

    let offsets2 = seq2.offsets.as_ref().unwrap();
    let values_bytes2 = seq2.values.as_bytes();

    for i in 0..50 {
        let start = offsets2[i] as usize;
        let end = offsets2[i + 1] as usize;
        let string_value = std::str::from_utf8(&values_bytes2[start..end]).unwrap();
        assert_eq!(string_value, constant_string_value);
    }

    println!("✓ Constant string stripe integration test passed!");
    println!("  Successfully created and read constant string values from stripe");
    println!(
        "  Verified {record_count} records with constant string value: '{constant_string_value}'"
    );

    Ok(())
}

#[test]
fn test_constant_short_string_stripe_integration() -> Result<()> {
    let shard_store = ShardStore::new();

    // Create a schema with string field
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
        arrow_schema::Field::new("code", arrow_schema::DataType::Utf8, false),
        arrow_schema::Field::new("description", arrow_schema::DataType::Utf8, true),
    ]));

    // Create a constant short string value (well under 128 bytes)
    let constant_string_value = "OK";

    // Generate batches with constant short string values
    let record_count = 1000;
    let batches = generate_constant_short_string_batches(
        schema.clone(),
        50..100,
        record_count,
        constant_string_value,
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

    // Find the string field
    let code_field_info = stripe_schema.find_field("code")?.unwrap();

    // Open the string field
    let code_field = stripe.open_field(code_field_info.1.data_type().unwrap())?;

    // Verify field type
    assert_eq!(code_field.data_type().basic_type()?, BasicType::String);

    // Create decoder and reader for the code field
    let code_decoder = code_field.create_decoder()?;
    let mut code_reader = code_decoder.create_reader_with_ranges(std::iter::empty())?;

    // Read a range of values and verify they're all the same constant
    let seq = code_reader.read_range(0..10)?;
    assert_eq!(seq.len(), 10);

    // For bytes types, there should be offsets
    assert!(seq.offsets.is_some(), "String field should have offsets");

    // Verify all values are the same constant string
    let offsets = seq.offsets.as_ref().unwrap();
    let values_bytes = seq.values.as_bytes();

    for i in 0..10 {
        let start = offsets[i] as usize;
        let end = offsets[i + 1] as usize;
        let string_value = std::str::from_utf8(&values_bytes[start..end]).unwrap();
        assert_eq!(string_value, constant_string_value);
    }

    // Test reading different ranges
    let seq2 = code_reader.read_range(500..520)?;
    assert_eq!(seq2.len(), 20);

    let offsets2 = seq2.offsets.as_ref().unwrap();
    let values_bytes2 = seq2.values.as_bytes();

    for i in 0..20 {
        let start = offsets2[i] as usize;
        let end = offsets2[i + 1] as usize;
        let string_value = std::str::from_utf8(&values_bytes2[start..end]).unwrap();
        assert_eq!(string_value, constant_string_value);
    }

    println!("✓ Constant short string stripe integration test passed!");
    println!("  Successfully created and read constant short string values from stripe");
    println!(
        "  Verified {record_count} records with constant short string value: '{constant_string_value}'"
    );

    Ok(())
}

// ============================================================================
// CONSTANT GUID INTEGRATION TESTS
// ============================================================================

#[test]
fn test_constant_guid_stripe_integration() -> Result<()> {
    let shard_store = ShardStore::new();

    // Create a schema with GUID field
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
        make_guid("uuid"),
        arrow_schema::Field::new("description", arrow_schema::DataType::Utf8, true),
    ]));

    // Create a constant GUID value (16 bytes)
    let constant_guid_value = [
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde,
        0xf0,
    ];

    // Generate batches with constant GUID values
    let record_count = 1000;
    let batches =
        generate_constant_guid_batches(schema.clone(), 50..100, record_count, constant_guid_value);
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

    // Find the GUID field
    let uuid_field_info = stripe_schema.find_field("uuid")?.unwrap();

    // Open the GUID field
    let uuid_field = stripe.open_field(uuid_field_info.1.data_type().unwrap())?;

    // Verify field type
    assert_eq!(uuid_field.data_type().basic_type()?, BasicType::Guid);

    // Create decoder and reader for the UUID field
    let uuid_decoder = uuid_field.create_decoder()?;
    let mut uuid_reader = uuid_decoder.create_reader_with_ranges(std::iter::empty())?;

    // Read a range of values and verify they're all the same constant
    let seq = uuid_reader.read_range(0..10)?;
    assert_eq!(seq.len(), 10);

    // For fixed-size binary types, there should be no offsets
    assert!(seq.offsets.is_none(), "GUID field should not have offsets");

    // Verify all values are the same constant GUID
    let values_bytes = seq.values.as_bytes();
    assert_eq!(values_bytes.len(), 10 * 16); // 10 GUID values * 16 bytes each

    for i in 0..10 {
        let start = i * 16;
        let end = start + 16;
        let guid_bytes = &values_bytes[start..end];
        assert_eq!(guid_bytes, &constant_guid_value);
    }

    // Test reading different ranges
    let seq2 = uuid_reader.read_range(250..270)?;
    assert_eq!(seq2.len(), 20);

    let values_bytes2 = seq2.values.as_bytes();
    assert_eq!(values_bytes2.len(), 20 * 16); // 20 GUID values * 16 bytes each

    for i in 0..20 {
        let start = i * 16;
        let end = start + 16;
        let guid_bytes = &values_bytes2[start..end];
        assert_eq!(guid_bytes, &constant_guid_value);
    }

    println!("✓ Constant GUID stripe integration test passed!");
    println!("  Successfully created and read constant GUID values from stripe");
    println!("  Verified {record_count} records with constant GUID value: {constant_guid_value:?}");

    Ok(())
}

// ============================================================================
// CONSTANT BINARY INTEGRATION TESTS
// ============================================================================

#[test]
fn test_constant_binary_stripe_integration() -> Result<()> {
    let shard_store = ShardStore::new();

    // Create a schema with binary field
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
        arrow_schema::Field::new("data", arrow_schema::DataType::Binary, false),
        arrow_schema::Field::new("description", arrow_schema::DataType::Utf8, true),
    ]));

    // Create a constant binary value (under 128 bytes)
    let constant_binary_value = vec![
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f, 0x20,
    ];

    // Generate batches with constant binary values
    let record_count = 1000;
    let batches = generate_constant_binary_batches(
        schema.clone(),
        50..100,
        record_count,
        constant_binary_value.clone(),
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

    // Find the binary field
    let data_field_info = stripe_schema.find_field("data")?.unwrap();

    // Open the binary field
    let data_field = stripe.open_field(data_field_info.1.data_type().unwrap())?;

    // Verify field type
    assert_eq!(data_field.data_type().basic_type()?, BasicType::Binary);

    // Create decoder and reader for the data field
    let data_decoder = data_field.create_decoder()?;
    let mut data_reader = data_decoder.create_reader_with_ranges(std::iter::empty())?;

    // Read a range of values and verify they're all the same constant
    let seq = data_reader.read_range(0..10)?;
    assert_eq!(seq.len(), 10);

    // For bytes types, there should be offsets
    assert!(seq.offsets.is_some(), "Binary field should have offsets");

    // Verify all values are the same constant binary
    let offsets = seq.offsets.as_ref().unwrap();
    let values_bytes = seq.values.as_bytes();

    for i in 0..10 {
        let start = offsets[i] as usize;
        let end = offsets[i + 1] as usize;
        let binary_value = &values_bytes[start..end];
        assert_eq!(binary_value, &constant_binary_value);
    }

    // Test reading different ranges
    let seq2 = data_reader.read_range(300..320)?;
    assert_eq!(seq2.len(), 20);

    let offsets2 = seq2.offsets.as_ref().unwrap();
    let values_bytes2 = seq2.values.as_bytes();

    for i in 0..20 {
        let start = offsets2[i] as usize;
        let end = offsets2[i + 1] as usize;
        let binary_value = &values_bytes2[start..end];
        assert_eq!(binary_value, &constant_binary_value);
    }

    println!("✓ Constant binary stripe integration test passed!");
    println!("  Successfully created and read constant binary values from stripe");
    println!(
        "  Verified {record_count} records with constant binary value: {constant_binary_value:?}"
    );

    Ok(())
}

// ============================================================================
// MIXED CONSTANT AND NULL TESTS
// ============================================================================

#[test]
fn test_mixed_constant_null_string_stripe_integration() -> Result<()> {
    let shard_store = ShardStore::new();

    // Create a schema with nullable string field
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
        arrow_schema::Field::new("optional_message", arrow_schema::DataType::Utf8, true),
        arrow_schema::Field::new("description", arrow_schema::DataType::Utf8, true),
    ]));

    // Create batches with mixed constant and null values
    let record_count = 1000;
    let batches =
        generate_mixed_constant_null_string_batches(schema.clone(), 50..100, record_count);
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

    // Find the nullable string field
    let optional_message_field_info = stripe_schema.find_field("optional_message")?.unwrap();

    // Open the nullable string field
    let optional_message_field =
        stripe.open_field(optional_message_field_info.1.data_type().unwrap())?;

    // Verify field type
    assert_eq!(
        optional_message_field.data_type().basic_type()?,
        BasicType::String
    );

    // Create decoder and reader for the optional_message field
    let optional_message_decoder = optional_message_field.create_decoder()?;
    let mut optional_message_reader =
        optional_message_decoder.create_reader_with_ranges(std::iter::empty())?;

    // Read a range of values and verify the pattern
    let seq = optional_message_reader.read_range(0..20)?;
    assert_eq!(seq.len(), 20);

    // For bytes types, there should be offsets
    assert!(seq.offsets.is_some(), "String field should have offsets");

    // Verify presence indicates which values are null vs non-null
    let presence = seq.presence;
    let offsets = seq.offsets.as_ref().unwrap();
    let values_bytes = seq.values.as_bytes();

    // Check that we have both null and non-null values
    let mut null_count = 0;
    let mut non_null_count = 0;

    for i in 0..20 {
        if presence.is_valid(i) {
            non_null_count += 1;
            let start = offsets[i] as usize;
            let end = offsets[i + 1] as usize;
            let string_value = std::str::from_utf8(&values_bytes[start..end]).unwrap();
            // Should be our constant value
            assert_eq!(string_value, "constant_message");
        } else {
            null_count += 1;
        }
    }

    // We should have both null and non-null values in our mixed scenario
    assert!(null_count > 0, "Should have some null values");
    assert!(non_null_count > 0, "Should have some non-null values");

    println!("✓ Mixed constant/null string stripe integration test passed!");
    println!("  Successfully created and read mixed constant/null string values from stripe");
    println!(
        "  Found {null_count} null values and {non_null_count} non-null values in first 20 records"
    );

    Ok(())
}

// ============================================================================
// BATCH GENERATION HELPER FUNCTIONS
// ============================================================================

/// Generate batches with constant string values for testing
fn generate_constant_string_batches(
    schema: Arc<arrow_schema::Schema>,
    batch_size: std::ops::Range<usize>,
    record_count: usize,
    constant_string_value: &str,
) -> Box<dyn Iterator<Item = RecordBatch>> {
    let batch_sizes = amudai_arrow_processing::array_sequence::random_split(
        record_count,
        batch_size.start,
        batch_size.end,
    );
    let constant_string_value = constant_string_value.to_string();
    Box::new(batch_sizes.into_iter().map(move |batch_size| {
        generate_constant_string_batch(schema.clone(), batch_size, &constant_string_value)
    }))
}

/// Generate a single batch with constant string values
fn generate_constant_string_batch(
    schema: Arc<arrow_schema::Schema>,
    batch_size: usize,
    constant_string_value: &str,
) -> RecordBatch {
    let mut columns = Vec::<ArrayRef>::new();

    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "message" => {
                // Generate constant string values
                let values: Vec<String> = (0..batch_size)
                    .map(|_| constant_string_value.to_string())
                    .collect();
                Arc::new(StringArray::from(values))
            }
            _ => {
                // Generate other fields using a simple pattern
                match field.data_type() {
                    arrow_schema::DataType::Int32 => {
                        let values: Vec<i32> = (0..batch_size).map(|i| i as i32).collect();
                        Arc::new(arrow_array::Int32Array::from(values))
                    }
                    arrow_schema::DataType::Utf8 => {
                        let values: Vec<Option<String>> =
                            (0..batch_size).map(|i| Some(format!("item_{i}"))).collect();
                        Arc::new(StringArray::from(values))
                    }
                    _ => panic!("Unsupported field type for test: {:?}", field.data_type()),
                }
            }
        };
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns).expect("Failed to create constant string batch")
}

/// Generate batches with constant short string values for testing
fn generate_constant_short_string_batches(
    schema: Arc<arrow_schema::Schema>,
    batch_size: std::ops::Range<usize>,
    record_count: usize,
    constant_string_value: &str,
) -> Box<dyn Iterator<Item = RecordBatch>> {
    let batch_sizes = amudai_arrow_processing::array_sequence::random_split(
        record_count,
        batch_size.start,
        batch_size.end,
    );
    let constant_string_value = constant_string_value.to_string();
    Box::new(batch_sizes.into_iter().map(move |batch_size| {
        generate_constant_short_string_batch(schema.clone(), batch_size, &constant_string_value)
    }))
}

/// Generate a single batch with constant short string values
fn generate_constant_short_string_batch(
    schema: Arc<arrow_schema::Schema>,
    batch_size: usize,
    constant_string_value: &str,
) -> RecordBatch {
    let mut columns = Vec::<ArrayRef>::new();

    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "code" => {
                // Generate constant short string values
                let values: Vec<String> = (0..batch_size)
                    .map(|_| constant_string_value.to_string())
                    .collect();
                Arc::new(StringArray::from(values))
            }
            _ => {
                // Generate other fields using a simple pattern
                match field.data_type() {
                    arrow_schema::DataType::Int32 => {
                        let values: Vec<i32> = (0..batch_size).map(|i| i as i32).collect();
                        Arc::new(arrow_array::Int32Array::from(values))
                    }
                    arrow_schema::DataType::Utf8 => {
                        let values: Vec<Option<String>> =
                            (0..batch_size).map(|i| Some(format!("item_{i}"))).collect();
                        Arc::new(StringArray::from(values))
                    }
                    _ => panic!("Unsupported field type for test: {:?}", field.data_type()),
                }
            }
        };
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns).expect("Failed to create constant short string batch")
}

/// Generate batches with constant GUID values for testing
fn generate_constant_guid_batches(
    schema: Arc<arrow_schema::Schema>,
    batch_size: std::ops::Range<usize>,
    record_count: usize,
    constant_guid_value: [u8; 16],
) -> Box<dyn Iterator<Item = RecordBatch>> {
    let batch_sizes = amudai_arrow_processing::array_sequence::random_split(
        record_count,
        batch_size.start,
        batch_size.end,
    );
    Box::new(batch_sizes.into_iter().map(move |batch_size| {
        generate_constant_guid_batch(schema.clone(), batch_size, constant_guid_value)
    }))
}

/// Generate a single batch with constant GUID values
fn generate_constant_guid_batch(
    schema: Arc<arrow_schema::Schema>,
    batch_size: usize,
    constant_guid_value: [u8; 16],
) -> RecordBatch {
    let mut columns = Vec::<ArrayRef>::new();

    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "uuid" => {
                // Generate constant GUID values
                let values: Vec<&[u8]> = (0..batch_size)
                    .map(|_| constant_guid_value.as_slice())
                    .collect();
                Arc::new(FixedSizeBinaryArray::from(values))
            }
            _ => {
                // Generate other fields using a simple pattern
                match field.data_type() {
                    arrow_schema::DataType::Int32 => {
                        let values: Vec<i32> = (0..batch_size).map(|i| i as i32).collect();
                        Arc::new(arrow_array::Int32Array::from(values))
                    }
                    arrow_schema::DataType::Utf8 => {
                        let values: Vec<Option<String>> =
                            (0..batch_size).map(|i| Some(format!("item_{i}"))).collect();
                        Arc::new(StringArray::from(values))
                    }
                    _ => panic!("Unsupported field type for test: {:?}", field.data_type()),
                }
            }
        };
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns).expect("Failed to create constant GUID batch")
}

/// Generate batches with constant binary values for testing
fn generate_constant_binary_batches(
    schema: Arc<arrow_schema::Schema>,
    batch_size: std::ops::Range<usize>,
    record_count: usize,
    constant_binary_value: Vec<u8>,
) -> Box<dyn Iterator<Item = RecordBatch>> {
    let batch_sizes = amudai_arrow_processing::array_sequence::random_split(
        record_count,
        batch_size.start,
        batch_size.end,
    );
    Box::new(batch_sizes.into_iter().map(move |batch_size| {
        generate_constant_binary_batch(schema.clone(), batch_size, constant_binary_value.clone())
    }))
}

/// Generate a single batch with constant binary values
fn generate_constant_binary_batch(
    schema: Arc<arrow_schema::Schema>,
    batch_size: usize,
    constant_binary_value: Vec<u8>,
) -> RecordBatch {
    let mut columns = Vec::<ArrayRef>::new();

    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "data" => {
                // Generate constant binary values
                let values: Vec<&[u8]> = (0..batch_size)
                    .map(|_| constant_binary_value.as_slice())
                    .collect();
                Arc::new(BinaryArray::from_vec(values))
            }
            _ => {
                // Generate other fields using a simple pattern
                match field.data_type() {
                    arrow_schema::DataType::Int32 => {
                        let values: Vec<i32> = (0..batch_size).map(|i| i as i32).collect();
                        Arc::new(arrow_array::Int32Array::from(values))
                    }
                    arrow_schema::DataType::Utf8 => {
                        let values: Vec<Option<String>> =
                            (0..batch_size).map(|i| Some(format!("item_{i}"))).collect();
                        Arc::new(StringArray::from(values))
                    }
                    _ => panic!("Unsupported field type for test: {:?}", field.data_type()),
                }
            }
        };
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns).expect("Failed to create constant binary batch")
}

/// Generate batches with mixed constant and null string values for testing
fn generate_mixed_constant_null_string_batches(
    schema: Arc<arrow_schema::Schema>,
    batch_size: std::ops::Range<usize>,
    record_count: usize,
) -> Box<dyn Iterator<Item = RecordBatch>> {
    let batch_sizes = amudai_arrow_processing::array_sequence::random_split(
        record_count,
        batch_size.start,
        batch_size.end,
    );
    Box::new(batch_sizes.into_iter().map(move |batch_size| {
        generate_mixed_constant_null_string_batch(schema.clone(), batch_size)
    }))
}

/// Generate a single batch with mixed constant and null string values
fn generate_mixed_constant_null_string_batch(
    schema: Arc<arrow_schema::Schema>,
    batch_size: usize,
) -> RecordBatch {
    let mut columns = Vec::<ArrayRef>::new();

    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "optional_message" => {
                // Generate mixed constant and null string values
                let values: Vec<Option<String>> = (0..batch_size)
                    .map(|i| {
                        if i % 3 == 0 {
                            None // Every third value is null
                        } else {
                            Some("constant_message".to_string())
                        }
                    })
                    .collect();
                Arc::new(StringArray::from(values))
            }
            _ => {
                // Generate other fields using a simple pattern
                match field.data_type() {
                    arrow_schema::DataType::Int32 => {
                        let values: Vec<i32> = (0..batch_size).map(|i| i as i32).collect();
                        Arc::new(arrow_array::Int32Array::from(values))
                    }
                    arrow_schema::DataType::Utf8 => {
                        let values: Vec<Option<String>> =
                            (0..batch_size).map(|i| Some(format!("item_{i}"))).collect();
                        Arc::new(StringArray::from(values))
                    }
                    _ => panic!("Unsupported field type for test: {:?}", field.data_type()),
                }
            }
        };
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns)
        .expect("Failed to create mixed constant/null string batch")
}
