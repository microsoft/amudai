use std::sync::Arc;

use amudai_decimal::d128;
use amudai_format::defs::common::{AnyValue, any_value::Kind};
use amudai_objectstore::url::ObjectUrl;
use arrow_array::{BooleanArray, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_buffer::Buffer;
use arrow_schema::{DataType, Field, Schema as ArrowSchema};

use crate::read::shard::ShardOptions;
use amudai_common::Result;

/// Helper to create a test schema with various data types that we'll make constant
fn create_constant_fields_test_schema() -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new("constant_int", DataType::Int64, false),
        Field::new("constant_string", DataType::Utf8, false),
        Field::new("constant_bool", DataType::Boolean, false),
        Field::new("varying_int", DataType::Int64, false),
    ])
}

/// Helper to create a record batch where some fields have constant values
fn create_constant_batch_1(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(create_constant_fields_test_schema());

    // Constant fields across all batches
    let constant_int = Int64Array::from(vec![42i64; num_rows]);
    let constant_string = StringArray::from(vec!["hello"; num_rows]);
    let constant_bool = BooleanArray::from(vec![true; num_rows]);

    // Varying field - different values
    let varying_int = Int64Array::from((0..num_rows as i64).collect::<Vec<_>>());

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(constant_int),
            Arc::new(constant_string),
            Arc::new(constant_bool),
            Arc::new(varying_int),
        ],
    )
    .unwrap()
}

/// Helper to create another record batch with the SAME constant values
fn create_constant_batch_2(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(create_constant_fields_test_schema());

    // Same constant values as batch 1
    let constant_int = Int64Array::from(vec![42i64; num_rows]);
    let constant_string = StringArray::from(vec!["hello"; num_rows]);
    let constant_bool = BooleanArray::from(vec![true; num_rows]);

    // Different varying field values
    let varying_int = Int64Array::from((1000..1000 + num_rows as i64).collect::<Vec<_>>());

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(constant_int),
            Arc::new(constant_string),
            Arc::new(constant_bool),
            Arc::new(varying_int),
        ],
    )
    .unwrap()
}

/// Helper to create a record batch with DIFFERENT constant values (to test non-constant behavior)
fn create_different_constant_batch(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(create_constant_fields_test_schema());

    // Different constant values - this should make the shard-level field non-constant
    let constant_int = Int64Array::from(vec![99i64; num_rows]);
    let constant_string = StringArray::from(vec!["world"; num_rows]);
    let constant_bool = BooleanArray::from(vec![false; num_rows]);

    // Varying field
    let varying_int = Int64Array::from((2000..2000 + num_rows as i64).collect::<Vec<_>>());

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(constant_int),
            Arc::new(constant_string),
            Arc::new(constant_bool),
            Arc::new(varying_int),
        ],
    )
    .unwrap()
}

/// Helper to create a test schema with decimal fields
fn create_decimal_constant_fields_test_schema() -> ArrowSchema {
    use amudai_arrow_compat::arrow_fields::make_decimal;

    ArrowSchema::new(vec![
        make_decimal("constant_decimal").with_nullable(false),
        make_decimal("varying_decimal").with_nullable(false),
        Field::new("constant_int", DataType::Int64, false),
    ])
}

/// Helper function to convert d128 to bytes for FixedSizeBinaryArray
fn decimal_to_bytes(decimal: d128) -> [u8; 16] {
    decimal.to_raw_bytes()
}

/// Helper to create a record batch with constant decimal values
fn create_decimal_constant_batch_1(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(create_decimal_constant_fields_test_schema());

    // Constant decimal value: 123.45
    let constant_decimal_value = d128!(123.45);
    let constant_decimal_bytes = vec![decimal_to_bytes(constant_decimal_value); num_rows];

    // Create constant decimal array
    let constant_decimal_data: Vec<u8> = constant_decimal_bytes.into_iter().flatten().collect();
    let constant_decimal_buffer = Buffer::from_vec(constant_decimal_data);
    let constant_decimal =
        FixedSizeBinaryArray::try_new(16, constant_decimal_buffer, None).unwrap();

    // Varying decimal values
    let varying_decimal_bytes: Vec<[u8; 16]> = (0..num_rows)
        .map(|i| decimal_to_bytes(d128::from(i as i64 + 100)))
        .collect();
    let varying_decimal_data: Vec<u8> = varying_decimal_bytes.into_iter().flatten().collect();
    let varying_decimal_buffer = Buffer::from_vec(varying_decimal_data);
    let varying_decimal = FixedSizeBinaryArray::try_new(16, varying_decimal_buffer, None).unwrap();

    // Constant int value
    let constant_int = Int64Array::from(vec![999i64; num_rows]);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(constant_decimal),
            Arc::new(varying_decimal),
            Arc::new(constant_int),
        ],
    )
    .unwrap()
}

/// Helper to create another record batch with the SAME constant decimal values
fn create_decimal_constant_batch_2(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(create_decimal_constant_fields_test_schema());

    // Same constant decimal value: 123.45
    let constant_decimal_value = d128!(123.45);
    let constant_decimal_bytes = vec![decimal_to_bytes(constant_decimal_value); num_rows];

    // Create constant decimal array
    let constant_decimal_data: Vec<u8> = constant_decimal_bytes.into_iter().flatten().collect();
    let constant_decimal_buffer = Buffer::from_vec(constant_decimal_data);
    let constant_decimal =
        FixedSizeBinaryArray::try_new(16, constant_decimal_buffer, None).unwrap();

    // Different varying decimal values
    let varying_decimal_bytes: Vec<[u8; 16]> = (0..num_rows)
        .map(|i| decimal_to_bytes(d128::from(i as i64 + 1000)))
        .collect();
    let varying_decimal_data: Vec<u8> = varying_decimal_bytes.into_iter().flatten().collect();
    let varying_decimal_buffer = Buffer::from_vec(varying_decimal_data);
    let varying_decimal = FixedSizeBinaryArray::try_new(16, varying_decimal_buffer, None).unwrap();

    // Same constant int value
    let constant_int = Int64Array::from(vec![999i64; num_rows]);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(constant_decimal),
            Arc::new(varying_decimal),
            Arc::new(constant_int),
        ],
    )
    .unwrap()
}

/// Helper to create a record batch with DIFFERENT constant decimal values
fn create_decimal_different_constant_batch(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(create_decimal_constant_fields_test_schema());

    // Different constant decimal value: 678.90
    let constant_decimal_value = d128!(678.90);
    let constant_decimal_bytes = vec![decimal_to_bytes(constant_decimal_value); num_rows];

    // Create constant decimal array
    let constant_decimal_data: Vec<u8> = constant_decimal_bytes.into_iter().flatten().collect();
    let constant_decimal_buffer = Buffer::from_vec(constant_decimal_data);
    let constant_decimal =
        FixedSizeBinaryArray::try_new(16, constant_decimal_buffer, None).unwrap();

    // Varying decimal values
    let varying_decimal_bytes: Vec<[u8; 16]> = (0..num_rows)
        .map(|i| decimal_to_bytes(d128::from(i as i64 + 2000)))
        .collect();
    let varying_decimal_data: Vec<u8> = varying_decimal_bytes.into_iter().flatten().collect();
    let varying_decimal_buffer = Buffer::from_vec(varying_decimal_data);
    let varying_decimal = FixedSizeBinaryArray::try_new(16, varying_decimal_buffer, None).unwrap();

    // Different constant int value
    let constant_int = Int64Array::from(vec![777i64; num_rows]);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(constant_decimal),
            Arc::new(varying_decimal),
            Arc::new(constant_int),
        ],
    )
    .unwrap()
}

#[test]
fn test_constant_field_across_stripes_same_values() -> Result<()> {
    use crate::tests::shard_store::ShardStore;

    let arrow_schema = Arc::new(create_constant_fields_test_schema());
    let shard_store = ShardStore::new();

    // Create shard using ShardStore to handle size requirements properly
    let mut shard_builder = shard_store.create_shard_builder(&arrow_schema);

    // Create first stripe with constant values
    let mut stripe_builder1 = shard_builder.build_stripe().unwrap();
    let batch1 = create_constant_batch_1(1000);
    stripe_builder1.push_batch(&batch1).unwrap();
    let stripe1 = stripe_builder1.finish().unwrap();
    shard_builder.add_stripe(stripe1).unwrap();

    // Create second stripe with the SAME constant values
    let mut stripe_builder2 = shard_builder.build_stripe().unwrap();
    let batch2 = create_constant_batch_2(1000);
    stripe_builder2.push_batch(&batch2).unwrap();
    let stripe2 = stripe_builder2.finish().unwrap();
    shard_builder.add_stripe(stripe2).unwrap();

    // Seal the shard using ShardStore
    let shard_directory_blob = shard_store.seal_shard_builder(shard_builder);

    // Re-open the shard for reading using proper Shard API
    let shard = ShardOptions::new(shard_store.object_store.clone())
        .open(ObjectUrl::parse(&shard_directory_blob.url).unwrap())
        .unwrap();

    let amudai_schema = shard.fetch_schema().unwrap();

    // Find the schema IDs for our fields
    let mut constant_int_schema_id = None;
    let mut constant_string_schema_id = None;
    let mut constant_bool_schema_id = None;
    let mut varying_int_schema_id = None;

    for i in 0..amudai_schema.len()? {
        let field = amudai_schema.field_at(i)?;
        let data_type = field.data_type()?;
        match data_type.name()? {
            "constant_int" => constant_int_schema_id = Some(data_type.schema_id()?),
            "constant_string" => constant_string_schema_id = Some(data_type.schema_id()?),
            "constant_bool" => constant_bool_schema_id = Some(data_type.schema_id()?),
            "varying_int" => varying_int_schema_id = Some(data_type.schema_id()?),
            _ => {}
        }
    }

    // Check constant_int field - get shard-level field descriptor
    let constant_int_desc = shard
        .fetch_field_descriptor(constant_int_schema_id.unwrap())
        .unwrap();
    assert!(
        constant_int_desc.inner().constant_value.is_some(),
        "constant_int should have a constant value"
    );
    if let Some(AnyValue {
        kind: Some(Kind::I64Value(value)),
        ..
    }) = &constant_int_desc.inner().constant_value
    {
        assert_eq!(*value, 42, "constant_int should have value 42");
    } else {
        panic!("constant_int constant_value should be I64Value(42)");
    }

    // Check constant_string field
    let constant_string_desc = shard
        .fetch_field_descriptor(constant_string_schema_id.unwrap())
        .unwrap();
    assert!(
        constant_string_desc.inner().constant_value.is_some(),
        "constant_string should have a constant value"
    );
    if let Some(AnyValue {
        kind: Some(Kind::StringValue(value)),
        ..
    }) = &constant_string_desc.inner().constant_value
    {
        assert_eq!(value, "hello", "constant_string should have value 'hello'");
    } else {
        panic!("constant_string constant_value should be StringValue('hello')");
    }

    // Check constant_bool field
    let constant_bool_desc = shard
        .fetch_field_descriptor(constant_bool_schema_id.unwrap())
        .unwrap();
    assert!(
        constant_bool_desc.inner().constant_value.is_some(),
        "constant_bool should have a constant value"
    );
    if let Some(AnyValue {
        kind: Some(Kind::BoolValue(value)),
        ..
    }) = &constant_bool_desc.inner().constant_value
    {
        assert!(*value, "constant_bool should have value true");
    } else {
        panic!("constant_bool constant_value should be BoolValue(true)");
    }

    // Check varying_int field - should NOT have a constant value
    let varying_int_desc = shard
        .fetch_field_descriptor(varying_int_schema_id.unwrap())
        .unwrap();
    assert!(
        varying_int_desc.inner().constant_value.is_none(),
        "varying_int should NOT have a constant value"
    );

    // Verify record counts
    assert_eq!(shard.directory().total_record_count, 2000); // 1000 + 1000
    assert_eq!(shard.stripe_count(), 2);

    Ok(())
}

#[test]
fn test_constant_field_across_stripes_different_values() -> Result<()> {
    use crate::tests::shard_store::ShardStore;

    let arrow_schema = Arc::new(create_constant_fields_test_schema());
    let shard_store = ShardStore::new();

    let mut shard_builder = shard_store.create_shard_builder(&arrow_schema);

    // Create first stripe with one set of constant values
    let mut stripe_builder1 = shard_builder.build_stripe().unwrap();
    let batch1 = create_constant_batch_1(1000);
    stripe_builder1.push_batch(&batch1).unwrap();
    let stripe1 = stripe_builder1.finish().unwrap();
    shard_builder.add_stripe(stripe1).unwrap();

    // Create second stripe with DIFFERENT constant values
    let mut stripe_builder2 = shard_builder.build_stripe().unwrap();
    let batch2 = create_different_constant_batch(1000);
    stripe_builder2.push_batch(&batch2).unwrap();
    let stripe2 = stripe_builder2.finish().unwrap();
    shard_builder.add_stripe(stripe2).unwrap();

    let shard_directory_blob = shard_store.seal_shard_builder(shard_builder);

    // Re-open the shard for reading using proper Shard API
    let shard = ShardOptions::new(shard_store.object_store.clone())
        .open(ObjectUrl::parse(&shard_directory_blob.url).unwrap())
        .unwrap();

    let amudai_schema = shard.fetch_schema().unwrap();

    // Find the schema IDs for our fields
    let mut constant_int_schema_id = None;
    let mut constant_string_schema_id = None;
    let mut constant_bool_schema_id = None;
    let mut varying_int_schema_id = None;

    for i in 0..amudai_schema.len()? {
        let field = amudai_schema.field_at(i)?;
        let data_type = field.data_type()?;
        match data_type.name()? {
            "constant_int" => constant_int_schema_id = Some(data_type.schema_id()?),
            "constant_string" => constant_string_schema_id = Some(data_type.schema_id()?),
            "constant_bool" => constant_bool_schema_id = Some(data_type.schema_id()?),
            "varying_int" => varying_int_schema_id = Some(data_type.schema_id()?),
            _ => {}
        }
    }

    // All the "constant" fields should NOT have constant values at shard level
    // because they have different values across stripes

    let constant_int_desc = shard
        .fetch_field_descriptor(constant_int_schema_id.unwrap())
        .unwrap();
    assert!(
        constant_int_desc.inner().constant_value.is_none(),
        "constant_int should NOT have a constant value due to different values across stripes"
    );

    let constant_string_desc = shard
        .fetch_field_descriptor(constant_string_schema_id.unwrap())
        .unwrap();
    assert!(
        constant_string_desc.inner().constant_value.is_none(),
        "constant_string should NOT have a constant value due to different values across stripes"
    );

    let constant_bool_desc = shard
        .fetch_field_descriptor(constant_bool_schema_id.unwrap())
        .unwrap();
    assert!(
        constant_bool_desc.inner().constant_value.is_none(),
        "constant_bool should NOT have a constant value due to different values across stripes"
    );

    let varying_int_desc = shard
        .fetch_field_descriptor(varying_int_schema_id.unwrap())
        .unwrap();
    assert!(
        varying_int_desc.inner().constant_value.is_none(),
        "varying_int should NOT have a constant value"
    );

    // Verify record counts
    assert_eq!(shard.directory().total_record_count, 2000); // 1000 + 1000
    assert_eq!(shard.stripe_count(), 2);

    Ok(())
}

#[test]
fn test_single_stripe_constant_field() -> Result<()> {
    use crate::tests::shard_store::ShardStore;

    let arrow_schema = Arc::new(create_constant_fields_test_schema());
    let shard_store = ShardStore::new();

    let mut shard_builder = shard_store.create_shard_builder(&arrow_schema);

    // Create a single stripe with constant values
    let mut stripe_builder = shard_builder.build_stripe().unwrap();
    let batch = create_constant_batch_1(1000);
    stripe_builder.push_batch(&batch).unwrap();
    let stripe = stripe_builder.finish().unwrap();
    shard_builder.add_stripe(stripe).unwrap();

    let shard_directory_blob = shard_store.seal_shard_builder(shard_builder);

    // Re-open the shard for reading using proper Shard API
    let shard = ShardOptions::new(shard_store.object_store.clone())
        .open(ObjectUrl::parse(&shard_directory_blob.url).unwrap())
        .unwrap();

    let amudai_schema = shard.fetch_schema().unwrap();

    // Find the schema IDs for our fields
    let mut constant_int_schema_id = None;
    let mut constant_string_schema_id = None;
    let mut constant_bool_schema_id = None;
    let mut varying_int_schema_id = None;

    for i in 0..amudai_schema.len()? {
        let field = amudai_schema.field_at(i)?;
        let data_type = field.data_type()?;
        match data_type.name()? {
            "constant_int" => constant_int_schema_id = Some(data_type.schema_id()?),
            "constant_string" => constant_string_schema_id = Some(data_type.schema_id()?),
            "constant_bool" => constant_bool_schema_id = Some(data_type.schema_id()?),
            "varying_int" => varying_int_schema_id = Some(data_type.schema_id()?),
            _ => {}
        }
    }

    // With a single stripe, constant fields should retain their constant values at shard level
    let constant_int_desc = shard
        .fetch_field_descriptor(constant_int_schema_id.unwrap())
        .unwrap();
    assert!(
        constant_int_desc.inner().constant_value.is_some(),
        "constant_int should have a constant value"
    );
    if let Some(AnyValue {
        kind: Some(Kind::I64Value(value)),
        ..
    }) = &constant_int_desc.inner().constant_value
    {
        assert_eq!(*value, 42, "constant_int should have value 42");
    } else {
        panic!("constant_int constant_value should be I64Value(42)");
    }

    let constant_string_desc = shard
        .fetch_field_descriptor(constant_string_schema_id.unwrap())
        .unwrap();
    assert!(
        constant_string_desc.inner().constant_value.is_some(),
        "constant_string should have a constant value"
    );
    if let Some(AnyValue {
        kind: Some(Kind::StringValue(value)),
        ..
    }) = &constant_string_desc.inner().constant_value
    {
        assert_eq!(value, "hello", "constant_string should have value 'hello'");
    } else {
        panic!("constant_string constant_value should be StringValue('hello')");
    }

    let constant_bool_desc = shard
        .fetch_field_descriptor(constant_bool_schema_id.unwrap())
        .unwrap();
    assert!(
        constant_bool_desc.inner().constant_value.is_some(),
        "constant_bool should have a constant value"
    );
    if let Some(AnyValue {
        kind: Some(Kind::BoolValue(value)),
        ..
    }) = &constant_bool_desc.inner().constant_value
    {
        assert!(*value, "constant_bool should have value true");
    } else {
        panic!("constant_bool constant_value should be BoolValue(true)");
    }

    // Varying field should not have a constant value
    let varying_int_desc = shard
        .fetch_field_descriptor(varying_int_schema_id.unwrap())
        .unwrap();
    assert!(
        varying_int_desc.inner().constant_value.is_none(),
        "varying_int should NOT have a constant value"
    );

    // Verify record counts
    assert_eq!(shard.directory().total_record_count, 1000);
    assert_eq!(shard.stripe_count(), 1);

    Ok(())
}

#[test]
fn test_decimal_constant_field_across_stripes_same_values() -> Result<()> {
    use crate::tests::shard_store::ShardStore;

    let arrow_schema = Arc::new(create_decimal_constant_fields_test_schema());
    let shard_store = ShardStore::new();

    let mut shard_builder = shard_store.create_shard_builder(&arrow_schema);

    // Create first stripe with constant decimal values
    let mut stripe_builder1 = shard_builder.build_stripe().unwrap();
    let batch1 = create_decimal_constant_batch_1(1000);
    stripe_builder1.push_batch(&batch1).unwrap();
    let stripe1 = stripe_builder1.finish().unwrap();
    shard_builder.add_stripe(stripe1).unwrap();

    // Create second stripe with the SAME constant decimal values
    let mut stripe_builder2 = shard_builder.build_stripe().unwrap();
    let batch2 = create_decimal_constant_batch_2(1000);
    stripe_builder2.push_batch(&batch2).unwrap();
    let stripe2 = stripe_builder2.finish().unwrap();
    shard_builder.add_stripe(stripe2).unwrap();

    let shard_directory_blob = shard_store.seal_shard_builder(shard_builder);

    // Re-open the shard for reading using proper Shard API
    let shard = ShardOptions::new(shard_store.object_store.clone())
        .open(ObjectUrl::parse(&shard_directory_blob.url).unwrap())
        .unwrap();

    let amudai_schema = shard.fetch_schema().unwrap();

    // Find the schema IDs for our fields
    let mut constant_decimal_schema_id = None;
    let mut varying_decimal_schema_id = None;
    let mut constant_int_schema_id = None;

    for i in 0..amudai_schema.len()? {
        let field = amudai_schema.field_at(i)?;
        let data_type = field.data_type()?;
        match data_type.name()? {
            "constant_decimal" => constant_decimal_schema_id = Some(data_type.schema_id()?),
            "varying_decimal" => varying_decimal_schema_id = Some(data_type.schema_id()?),
            "constant_int" => constant_int_schema_id = Some(data_type.schema_id()?),
            _ => {}
        }
    }

    // Check constant_decimal field - should have constant value 123.45
    let constant_decimal_desc = shard
        .fetch_field_descriptor(constant_decimal_schema_id.unwrap())
        .unwrap();
    println!(
        "constant_decimal_desc constant_value: {:?}",
        constant_decimal_desc.inner().constant_value
    );
    assert!(
        constant_decimal_desc.inner().constant_value.is_some(),
        "constant_decimal should have a constant value"
    );
    if let Some(AnyValue {
        kind: Some(Kind::DecimalValue(decimal_bytes)),
        ..
    }) = &constant_decimal_desc.inner().constant_value
    {
        // Verify the decimal value is 123.45
        assert_eq!(decimal_bytes.len(), 16, "decimal should be 16 bytes");
        let expected_bytes = decimal_to_bytes(d128!(123.45));
        assert_eq!(
            decimal_bytes.as_slice(),
            &expected_bytes,
            "constant_decimal should have value 123.45"
        );
    } else {
        panic!("constant_decimal constant_value should be DecimalValue with 123.45");
    }

    // Check varying_decimal field - should NOT have a constant value
    let varying_decimal_desc = shard
        .fetch_field_descriptor(varying_decimal_schema_id.unwrap())
        .unwrap();
    assert!(
        varying_decimal_desc.inner().constant_value.is_none(),
        "varying_decimal should NOT have a constant value"
    );

    // Check constant_int field - should have constant value 999
    let constant_int_desc = shard
        .fetch_field_descriptor(constant_int_schema_id.unwrap())
        .unwrap();
    assert!(
        constant_int_desc.inner().constant_value.is_some(),
        "constant_int should have a constant value"
    );
    if let Some(AnyValue {
        kind: Some(Kind::I64Value(value)),
        ..
    }) = &constant_int_desc.inner().constant_value
    {
        assert_eq!(*value, 999, "constant_int should have value 999");
    } else {
        panic!("constant_int constant_value should be I64Value(999)");
    }

    // Verify record counts
    assert_eq!(shard.directory().total_record_count, 2000); // 1000 + 1000
    assert_eq!(shard.stripe_count(), 2);

    Ok(())
}

#[test]
fn test_decimal_constant_field_across_stripes_different_values() -> Result<()> {
    use crate::tests::shard_store::ShardStore;

    let arrow_schema = Arc::new(create_decimal_constant_fields_test_schema());
    let shard_store = ShardStore::new();

    let mut shard_builder = shard_store.create_shard_builder(&arrow_schema);

    // Create first stripe with one set of constant decimal values
    let mut stripe_builder1 = shard_builder.build_stripe().unwrap();
    let batch1 = create_decimal_constant_batch_1(1000);
    stripe_builder1.push_batch(&batch1).unwrap();
    let stripe1 = stripe_builder1.finish().unwrap();
    shard_builder.add_stripe(stripe1).unwrap();

    // Create second stripe with DIFFERENT constant decimal values
    let mut stripe_builder2 = shard_builder.build_stripe().unwrap();
    let batch2 = create_decimal_different_constant_batch(1000);
    stripe_builder2.push_batch(&batch2).unwrap();
    let stripe2 = stripe_builder2.finish().unwrap();
    shard_builder.add_stripe(stripe2).unwrap();

    let shard_directory_blob = shard_store.seal_shard_builder(shard_builder);

    // Re-open the shard for reading using proper Shard API
    let shard = ShardOptions::new(shard_store.object_store.clone())
        .open(ObjectUrl::parse(&shard_directory_blob.url).unwrap())
        .unwrap();

    let amudai_schema = shard.fetch_schema().unwrap();

    // Find the schema IDs for our fields
    let mut constant_decimal_schema_id = None;
    let mut varying_decimal_schema_id = None;
    let mut constant_int_schema_id = None;

    for i in 0..amudai_schema.len()? {
        let field = amudai_schema.field_at(i)?;
        let data_type = field.data_type()?;
        match data_type.name()? {
            "constant_decimal" => constant_decimal_schema_id = Some(data_type.schema_id()?),
            "varying_decimal" => varying_decimal_schema_id = Some(data_type.schema_id()?),
            "constant_int" => constant_int_schema_id = Some(data_type.schema_id()?),
            _ => {}
        }
    }

    // All fields should NOT have constant values at shard level
    // because they have different values across stripes

    let constant_decimal_desc = shard
        .fetch_field_descriptor(constant_decimal_schema_id.unwrap())
        .unwrap();
    assert!(
        constant_decimal_desc.inner().constant_value.is_none(),
        "constant_decimal should NOT have a constant value due to different values across stripes"
    );

    let varying_decimal_desc = shard
        .fetch_field_descriptor(varying_decimal_schema_id.unwrap())
        .unwrap();
    assert!(
        varying_decimal_desc.inner().constant_value.is_none(),
        "varying_decimal should NOT have a constant value"
    );

    let constant_int_desc = shard
        .fetch_field_descriptor(constant_int_schema_id.unwrap())
        .unwrap();
    assert!(
        constant_int_desc.inner().constant_value.is_none(),
        "constant_int should NOT have a constant value due to different values across stripes"
    );

    // Verify record counts
    assert_eq!(shard.directory().total_record_count, 2000); // 1000 + 1000
    assert_eq!(shard.stripe_count(), 2);

    Ok(())
}

#[test]
fn test_single_stripe_decimal_constant_field() -> Result<()> {
    use crate::tests::shard_store::ShardStore;

    let arrow_schema = Arc::new(create_decimal_constant_fields_test_schema());
    let shard_store = ShardStore::new();

    let mut shard_builder = shard_store.create_shard_builder(&arrow_schema);

    // Create a single stripe with constant decimal values
    let mut stripe_builder = shard_builder.build_stripe().unwrap();
    let batch = create_decimal_constant_batch_1(1000);
    stripe_builder.push_batch(&batch).unwrap();
    let stripe = stripe_builder.finish().unwrap();
    shard_builder.add_stripe(stripe).unwrap();

    let shard_directory_blob = shard_store.seal_shard_builder(shard_builder);

    // Re-open the shard for reading using proper Shard API
    let shard = ShardOptions::new(shard_store.object_store.clone())
        .open(ObjectUrl::parse(&shard_directory_blob.url).unwrap())
        .unwrap();

    let amudai_schema = shard.fetch_schema().unwrap();

    // Find the schema IDs for our fields
    let mut constant_decimal_schema_id = None;
    let mut varying_decimal_schema_id = None;
    let mut constant_int_schema_id = None;

    for i in 0..amudai_schema.len()? {
        let field = amudai_schema.field_at(i)?;
        let data_type = field.data_type()?;
        match data_type.name()? {
            "constant_decimal" => constant_decimal_schema_id = Some(data_type.schema_id()?),
            "varying_decimal" => varying_decimal_schema_id = Some(data_type.schema_id()?),
            "constant_int" => constant_int_schema_id = Some(data_type.schema_id()?),
            _ => {}
        }
    }

    // With a single stripe, constant fields should retain their constant values at shard level
    let constant_decimal_desc = shard
        .fetch_field_descriptor(constant_decimal_schema_id.unwrap())
        .unwrap();
    assert!(
        constant_decimal_desc.inner().constant_value.is_some(),
        "constant_decimal should have a constant value"
    );
    if let Some(AnyValue {
        kind: Some(Kind::DecimalValue(decimal_bytes)),
        ..
    }) = &constant_decimal_desc.inner().constant_value
    {
        // Verify the decimal value is 123.45
        assert_eq!(decimal_bytes.len(), 16, "decimal should be 16 bytes");
        let expected_bytes = decimal_to_bytes(d128!(123.45));
        assert_eq!(
            decimal_bytes.as_slice(),
            &expected_bytes,
            "constant_decimal should have value 123.45"
        );
    } else {
        panic!("constant_decimal constant_value should be DecimalValue with 123.45");
    }

    // Varying decimal field should not have a constant value
    let varying_decimal_desc = shard
        .fetch_field_descriptor(varying_decimal_schema_id.unwrap())
        .unwrap();
    assert!(
        varying_decimal_desc.inner().constant_value.is_none(),
        "varying_decimal should NOT have a constant value"
    );

    // Constant int field should have a constant value
    let constant_int_desc = shard
        .fetch_field_descriptor(constant_int_schema_id.unwrap())
        .unwrap();
    assert!(
        constant_int_desc.inner().constant_value.is_some(),
        "constant_int should have a constant value"
    );
    if let Some(AnyValue {
        kind: Some(Kind::I64Value(value)),
        ..
    }) = &constant_int_desc.inner().constant_value
    {
        assert_eq!(*value, 999, "constant_int should have value 999");
    } else {
        panic!("constant_int constant_value should be I64Value(999)");
    }

    // Verify record counts
    assert_eq!(shard.directory().total_record_count, 1000);
    assert_eq!(shard.stripe_count(), 1);

    Ok(())
}
