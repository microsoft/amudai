use std::sync::Arc;

use amudai_arrow_compat::arrow_to_amudai_schema::FromArrowSchema;
use amudai_format::{schema::BasicType, schema_builder::SchemaBuilder};
use amudai_io_impl::temp_file_store;
use amudai_objectstore::null_store::NullObjectStore;
use arrow_array::{
    Array, RecordBatchIterator,
    builder::{Int8Builder, StringBuilder},
};
use arrow_schema::{DataType, Schema};
use decimal::d128;

use crate::{
    tests::{
        data_generator::{self, FieldKind, FieldProperties, create_nested_test_schema, make_field},
        shard_store::ShardStore,
    },
    write::shard_builder::{ShardBuilder, ShardBuilderParams, ShardFileOrganization},
};

/// Helper function to convert 16-byte slice to d128 using the correct raw bytes approach
fn bytes_to_d128_safe(bytes: &[u8]) -> Result<d128, String> {
    if bytes.len() != 16 {
        return Err(format!("Expected 16 bytes for d128, got {}", bytes.len()));
    }
    let mut array = [0u8; 16];
    array.copy_from_slice(bytes);
    Ok(unsafe { d128::from_raw_bytes(array) })
}

#[test]
fn test_basic_shard_builder_flow() {
    let arrow_schema = create_id_name_test_schema();

    let shard_schema = SchemaBuilder::from_arrow_schema(&arrow_schema).unwrap();

    let mut shard_builder = ShardBuilder::new(ShardBuilderParams {
        schema: shard_schema.into(),
        object_store: Arc::new(NullObjectStore),
        temp_store: temp_file_store::create_in_memory(32 * 1024 * 1024).unwrap(),
        encoding_profile: Default::default(),
        file_organization: Default::default(),
    })
    .unwrap();

    let mut stripe_builder = shard_builder.build_stripe().unwrap();

    let batches = data_generator::generate_batches(arrow_schema, 100..200, 1000);
    for batch in batches {
        stripe_builder.push_batch(&batch).unwrap();
    }
    let stripe = stripe_builder.finish().unwrap();
    shard_builder.add_stripe(stripe).unwrap();
    let _shard = shard_builder
        .finish()
        .unwrap()
        .seal("null:///tmp/test.amudai.shard")
        .unwrap();
}

#[test]
fn test_nested_schema_shard_builder_flow() {
    let arrow_schema = create_nested_test_schema();

    let shard_schema = SchemaBuilder::from_arrow_schema(&arrow_schema).unwrap();

    let mut shard_builder = ShardBuilder::new(ShardBuilderParams {
        schema: shard_schema.into(),
        object_store: Arc::new(NullObjectStore),
        temp_store: temp_file_store::create_in_memory(32 * 1024 * 1024).unwrap(),
        encoding_profile: Default::default(),
        file_organization: Default::default(),
    })
    .unwrap();

    let mut stripe_builder = shard_builder.build_stripe().unwrap();

    let batches = data_generator::generate_batches(arrow_schema, 100..200, 1000);
    for batch in batches {
        stripe_builder.push_batch(&batch).unwrap();
    }
    let stripe = stripe_builder.finish().unwrap();
    shard_builder.add_stripe(stripe).unwrap();
    let shard = shard_builder
        .finish()
        .unwrap()
        .seal("null:///tmp/test.amudai.shard")
        .unwrap();

    assert_eq!(shard.directory.total_record_count, 1000);
    assert_eq!(shard.directory.deleted_record_count, 0);
    assert!(shard.directory.schema_ref.is_some());
    assert!(shard.directory.url_list_ref.is_some());
    assert!(shard.directory.field_list_ref.is_some());
    assert!(shard.directory.stripe_list_ref.is_some());
    assert!(shard.directory_blob.len() > 100);
}

#[test]
fn test_list_encoding() {
    let shard_store = ShardStore::new();

    let list_data = generate_list_data(10); // Generate 10 lists: [], [1], [1,2], etc.
    let schema = list_data.schema().clone();

    let shard_ref = shard_store.ingest_shard_from_record_batches(RecordBatchIterator::new(
        std::iter::once(list_data).map(Ok),
        schema,
    ));

    let shard = shard_store.open_shard(&shard_ref.url);
    let stripe = shard.open_stripe(0).unwrap();
    let schema = stripe.fetch_schema().unwrap();
    let field = schema.find_field("numbers").unwrap().unwrap().1;
    let field = stripe.open_field(field.data_type().unwrap()).unwrap();
    assert_eq!(field.data_type().basic_type().unwrap(), BasicType::List);
    assert_eq!(
        field.descriptor().field.as_ref().unwrap().position_count,
        10
    ); // 10 logical "values" (lists)
    let mut reader = field
        .create_decoder()
        .unwrap()
        .create_reader(std::iter::empty())
        .unwrap();
    let seq = reader.read(0..11).unwrap();
    assert_eq!(
        seq.values.as_slice::<u64>(),
        &[0, 0, 1, 3, 6, 10, 15, 21, 28, 36, 45,]
    );
}

#[test]
fn test_map_encoding() {
    let shard_store = ShardStore::new();

    let map_data = generate_map_data(10);
    let schema = map_data.schema().clone();

    let shard_ref = shard_store.ingest_shard_from_record_batches(RecordBatchIterator::new(
        std::iter::once(map_data).map(Ok),
        schema,
    ));

    let shard = shard_store.open_shard(&shard_ref.url);
    let stripe = shard.open_stripe(0).unwrap();
    let schema = stripe.fetch_schema().unwrap();
    let field = schema.find_field("properties").unwrap().unwrap().1;
    let field = stripe.open_field(field.data_type().unwrap()).unwrap();
    assert_eq!(field.data_type().basic_type().unwrap(), BasicType::Map);
    assert_eq!(
        field.descriptor().field.as_ref().unwrap().position_count,
        10
    );

    let mut reader = field
        .create_decoder()
        .unwrap()
        .create_reader(std::iter::empty())
        .unwrap();
    let seq = reader.read(0..11).unwrap();
    assert_eq!(
        seq.values.as_slice::<u64>(),
        &[0, 0, 1, 3, 6, 10, 15, 21, 28, 36, 45,]
    );

    let mut keys = field
        .open_child_at(0)
        .unwrap()
        .create_decoder()
        .unwrap()
        .create_reader(std::iter::empty())
        .unwrap();
    let mut values = field
        .open_child_at(1)
        .unwrap()
        .create_decoder()
        .unwrap()
        .create_reader(std::iter::empty())
        .unwrap();

    keys.read(0..45).unwrap();
    values.read(0..45).unwrap();
}

#[test]
fn test_single_file_shard_organization() {
    let shard_store = ShardStore::new();
    shard_store.set_shard_file_organization(ShardFileOrganization::SingleFile);
    let shard_ref = shard_store.ingest_shard_with_nested_schema(10000);
    let shard = shard_store.open_shard(&shard_ref.url);
    assert!(shard.fetch_url_list().unwrap().urls.is_empty());
    shard_store.verify_shard(shard);

    shard_store.set_shard_file_organization(ShardFileOrganization::TwoLevel);
    let shard_ref = shard_store.ingest_shard_with_nested_schema(10000);
    let shard = shard_store.open_shard(&shard_ref.url);
    assert!(
        shard
            .fetch_url_list()
            .unwrap()
            .urls
            .iter()
            .any(|url| url.contains(".amudai.stripe"))
    );
    shard_store.verify_shard(shard);
}

#[test]
fn test_struct_encoding() {
    let shard_store = ShardStore::new();

    let struct_data = generate_struct_data(5); // Generate 5 struct records
    let schema = struct_data.schema().clone();

    let shard_ref = shard_store.ingest_shard_from_record_batches(RecordBatchIterator::new(
        std::iter::once(struct_data).map(Ok),
        schema,
    ));

    let shard = shard_store.open_shard(&shard_ref.url);
    let stripe = shard.open_stripe(0).unwrap();
    let schema = stripe.fetch_schema().unwrap();

    // Test simple struct field
    let simple_field = schema.find_field("simple_struct").unwrap().unwrap().1;
    let simple_field_decoder = stripe
        .open_field(simple_field.data_type().unwrap())
        .unwrap();
    assert_eq!(
        simple_field_decoder.data_type().basic_type().unwrap(),
        BasicType::Struct
    );
    assert_eq!(simple_field_decoder.position_count(), 5); // 5 logical struct values

    // Test nested struct field
    let nested_field = schema.find_field("nested_struct").unwrap().unwrap().1;
    let nested_field_decoder = stripe
        .open_field(nested_field.data_type().unwrap())
        .unwrap();
    assert_eq!(
        nested_field_decoder.data_type().basic_type().unwrap(),
        BasicType::Struct
    );
    assert_eq!(nested_field_decoder.position_count(), 5);

    // Read back the data to verify round-trip integrity
    let mut simple_reader = simple_field_decoder
        .create_decoder()
        .unwrap()
        .create_reader(std::iter::empty())
        .unwrap();
    let simple_seq = simple_reader.read(0..5).unwrap();
    assert_eq!(simple_seq.type_desc.basic_type, BasicType::Struct);
    assert_eq!(simple_seq.presence.count_non_nulls(), 5);
    assert_eq!(simple_seq.values.bytes_len(), 0);
    assert!(simple_seq.offsets.is_none());

    let mut nested_reader = nested_field_decoder
        .create_decoder()
        .unwrap()
        .create_reader(std::iter::empty())
        .unwrap();
    let nested_seq = nested_reader.read(0..5).unwrap();
    assert_eq!(nested_seq.type_desc.basic_type, BasicType::Struct);
    assert_eq!(nested_seq.presence.count_non_nulls(), 5);
    assert_eq!(nested_seq.values.bytes_len(), 0);
    assert!(nested_seq.offsets.is_none());
}

#[test]
fn test_decimal_encoding() {
    use crate::tests::data_generator::create_decimal_test_schema;
    use arrow_array::FixedSizeBinaryArray;

    let shard_store = ShardStore::new();

    // Generate decimal test data with a controlled batch size for easier validation
    let schema = create_decimal_test_schema();
    // Create a single batch with known size for easier validation
    let test_batch = data_generator::generate_batch(schema.clone(), 100);

    // Store original decimal arrays for comparison (clone to avoid borrow issues)
    let original_amount = test_batch.column(1).clone();
    let original_price = test_batch.column(2).clone();

    let original_amount = original_amount
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("amount field should be FixedSizeBinaryArray");
    let original_price = original_price
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("price field should be FixedSizeBinaryArray");

    // Log some sample decimal values to show actual data content
    println!("Sample original decimal values:");
    for i in 0..std::cmp::min(5, 100) {
        if !original_amount.is_null(i) {
            if let Ok(decimal_val) = bytes_to_d128_safe(original_amount.value(i)) {
                println!("  amount[{i}] = {decimal_val}");
            }
        }
        if !original_price.is_null(i) {
            if let Ok(decimal_val) = bytes_to_d128_safe(original_price.value(i)) {
                println!("  price[{i}] = {decimal_val}");
            }
        }
    }

    let shard_ref = shard_store.ingest_shard_from_record_batches(RecordBatchIterator::new(
        std::iter::once(Ok(test_batch)),
        schema.clone(),
    ));

    // Verify the shard was created successfully
    let shard = shard_store.open_shard(&shard_ref.url);
    let stripe = shard.open_stripe(0).unwrap();
    let stripe_schema = stripe.fetch_schema().unwrap();

    // Verify decimal fields are present and correctly typed
    let amount_field = stripe_schema.find_field("amount").unwrap();
    assert!(amount_field.is_some());

    let price_field = stripe_schema.find_field("price").unwrap();
    assert!(price_field.is_some());

    // Test amount field (nullable)
    let (_, amount_field_desc) = amount_field.unwrap();
    let amount_field_reader = stripe
        .open_field(amount_field_desc.data_type().unwrap())
        .unwrap();
    assert!(amount_field_reader.descriptor().field.is_some());
    assert_eq!(amount_field_reader.position_count(), 100);

    // Read back the amount data and validate
    let mut amount_decoder = amount_field_reader
        .create_decoder()
        .unwrap()
        .create_reader(std::iter::empty())
        .unwrap();

    let amount_seq = amount_decoder.read(0..100).unwrap();

    // Count original nulls in amount field
    let original_amount_nulls = (0..100).filter(|&i| original_amount.is_null(i)).count();
    let decoded_amount_nulls = 100 - amount_seq.presence.count_non_nulls() as usize;

    assert_eq!(
        decoded_amount_nulls, original_amount_nulls,
        "Amount field null count mismatch"
    );

    // Validate non-null amount values
    let decoded_amount_bytes = amount_seq.values.as_slice::<u8>();
    let mut decoded_idx = 0;

    for i in 0..100 {
        if !original_amount.is_null(i) {
            let start = decoded_idx * 16;
            let end = start + 16;
            let decoded_slice = &decoded_amount_bytes[start..end];
            let original_slice = original_amount.value(i);

            // Validate binary representation matches
            assert_eq!(
                decoded_slice, original_slice,
                "Amount decimal value mismatch at position {i}"
            );

            // Also validate we can parse both as decimal values and they match
            if let (Ok(decoded_decimal), Ok(original_decimal)) = (
                bytes_to_d128_safe(decoded_slice),
                bytes_to_d128_safe(original_slice),
            ) {
                assert_eq!(
                    decoded_decimal, original_decimal,
                    "Parsed decimal value mismatch at position {i}: decoded={decoded_decimal}, original={original_decimal}"
                );
            }

            decoded_idx += 1;
        }
    }

    // Test price field (non-nullable in schema but can have nulls in data)
    let (_, price_field_desc) = price_field.unwrap();
    let price_field_reader = stripe
        .open_field(price_field_desc.data_type().unwrap())
        .unwrap();
    assert!(price_field_reader.descriptor().field.is_some());
    assert_eq!(price_field_reader.position_count(), 100);

    // Read back the price data and validate
    let mut price_decoder = price_field_reader
        .create_decoder()
        .unwrap()
        .create_reader(std::iter::empty())
        .unwrap();

    let price_seq = price_decoder.read(0..100).unwrap();

    // Count original nulls in price field
    let original_price_nulls = (0..100).filter(|&i| original_price.is_null(i)).count();
    let decoded_price_nulls = 100 - price_seq.presence.count_non_nulls() as usize;

    assert_eq!(
        decoded_price_nulls, original_price_nulls,
        "Price field null count mismatch"
    );

    // Validate non-null price values
    let decoded_price_bytes = price_seq.values.as_slice::<u8>();
    let mut decoded_idx = 0;

    for i in 0..100 {
        if !original_price.is_null(i) {
            let start = decoded_idx * 16;
            let end = start + 16;
            let decoded_slice = &decoded_price_bytes[start..end];
            let original_slice = original_price.value(i);

            // Validate binary representation matches
            assert_eq!(
                decoded_slice, original_slice,
                "Price decimal value mismatch at position {i}"
            );

            // Also validate we can parse both as decimal values and they match
            if let (Ok(decoded_decimal), Ok(original_decimal)) = (
                bytes_to_d128_safe(decoded_slice),
                bytes_to_d128_safe(original_slice),
            ) {
                assert_eq!(
                    decoded_decimal, original_decimal,
                    "Parsed price decimal value mismatch at position {i}: decoded={decoded_decimal}, original={original_decimal}"
                );
            }

            decoded_idx += 1;
        }
    }

    // Verify field statistics if available
    if let Some(field_desc) = amount_field_reader.descriptor().field.as_ref() {
        println!(
            "Amount field stats - position_count: {}, null_count: {}",
            field_desc.position_count, decoded_amount_nulls
        );

        // Check if decimal-specific statistics are available
        if let Some(type_specific) = &field_desc.type_specific {
            match type_specific {
                amudai_format::defs::shard::field_descriptor::TypeSpecific::DecimalStats(stats) => {
                    println!(
                        "Amount decimal stats: zero_count={}, positive_count={}, negative_count={}",
                        stats.zero_count, stats.positive_count, stats.negative_count
                    );
                    // Verify statistics make sense
                    let non_null_count = amount_seq.presence.count_non_nulls();
                    assert_eq!(
                        stats.zero_count + stats.positive_count + stats.negative_count,
                        non_null_count as u64,
                        "Sum of decimal categories should equal non-null count"
                    );
                }
                _ => {
                    println!("Amount field has type-specific stats but not decimal stats");
                }
            }
        } else {
            println!("Amount field has no type-specific statistics");
        }
    }

    if let Some(field_desc) = price_field_reader.descriptor().field.as_ref() {
        println!(
            "Price field stats - position_count: {}, null_count: {}",
            field_desc.position_count, decoded_price_nulls
        );
    }

    println!("✓ Decimal encoding test passed with comprehensive data validation!");
    println!(
        "  - Validated {} amount values ({} non-null)",
        100,
        amount_seq.presence.count_non_nulls()
    );
    println!(
        "  - Validated {} price values ({} non-null)",
        100,
        price_seq.presence.count_non_nulls()
    );
}

fn generate_list_data(count: usize) -> arrow_array::RecordBatch {
    use arrow_array::builder::{Int32Builder, ListBuilder};

    let mut list_builder = ListBuilder::new(Int32Builder::new());

    for i in 0..count {
        for j in 0..i {
            list_builder.values().append_value((j + 1) as i32); // values start from 1
        }
        list_builder.append(true); // Append a non-null list
    }

    let list_array = list_builder.finish();

    arrow_array::RecordBatch::try_from_iter(vec![(
        "numbers",
        Arc::new(list_array) as arrow_array::ArrayRef,
    )])
    .unwrap()
}

fn generate_map_data(count: usize) -> arrow_array::RecordBatch {
    use arrow_array::builder::{Int32Builder, MapBuilder};

    let mut map_builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());

    for i in 0..count {
        for j in 0..i {
            map_builder.keys().append_value(format!("key_{j}"));
            map_builder.values().append_value(j as i32);
        }
        map_builder.append(true).unwrap();
    }

    let map_array = map_builder.finish();

    arrow_array::RecordBatch::try_from_iter(vec![(
        "properties",
        Arc::new(map_array) as arrow_array::ArrayRef,
    )])
    .unwrap()
}

fn generate_struct_data(count: usize) -> arrow_array::RecordBatch {
    use arrow_array::builder::{Int32Builder, Int64Builder, StringBuilder, StructBuilder};

    // Create simple struct: {id: Int32, name: String}
    let simple_struct_fields = vec![
        Arc::new(arrow_schema::Field::new("id", DataType::Int32, false)),
        Arc::new(arrow_schema::Field::new("name", DataType::Utf8, false)),
    ];
    let mut simple_struct_builder = StructBuilder::new(
        simple_struct_fields.clone(),
        vec![
            Box::new(Int32Builder::new()) as Box<dyn arrow_array::builder::ArrayBuilder>,
            Box::new(StringBuilder::new()) as Box<dyn arrow_array::builder::ArrayBuilder>,
        ],
    );

    // Create nested struct: {metadata: {props: {count: Int64, active: Boolean}}}
    let inner_props_fields = vec![
        Arc::new(arrow_schema::Field::new("count", DataType::Int64, false)),
        Arc::new(arrow_schema::Field::new("active", DataType::Int8, false)),
    ];
    let props_struct_type = DataType::Struct(inner_props_fields.clone().into());

    let metadata_fields = vec![Arc::new(arrow_schema::Field::new(
        "props",
        props_struct_type,
        false,
    ))];

    let mut nested_struct_builder = StructBuilder::new(
        metadata_fields.clone(),
        vec![Box::new(StructBuilder::new(
            inner_props_fields.clone(),
            vec![
                Box::new(Int64Builder::new()) as Box<dyn arrow_array::builder::ArrayBuilder>,
                Box::new(Int8Builder::new()) as Box<dyn arrow_array::builder::ArrayBuilder>,
            ],
        )) as Box<dyn arrow_array::builder::ArrayBuilder>],
    );

    // Generate data
    for i in 0..count {
        // Simple struct data
        simple_struct_builder
            .field_builder::<Int32Builder>(0)
            .unwrap()
            .append_value(i as i32);
        simple_struct_builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value(format!("name_{i}"));
        simple_struct_builder.append(true);

        // Nested struct data
        let props_builder = nested_struct_builder
            .field_builder::<StructBuilder>(0)
            .unwrap();
        props_builder
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value((i * 10) as i64);
        props_builder
            .field_builder::<Int8Builder>(1)
            .unwrap()
            .append_value((i % 2) as i8);
        props_builder.append(true);
        nested_struct_builder.append(true);
    }

    let simple_struct_array = simple_struct_builder.finish();
    let nested_struct_array = nested_struct_builder.finish();

    arrow_array::RecordBatch::try_from_iter(vec![
        (
            "simple_struct",
            Arc::new(simple_struct_array) as arrow_array::ArrayRef,
        ),
        (
            "nested_struct",
            Arc::new(nested_struct_array) as arrow_array::ArrayRef,
        ),
    ])
    .unwrap()
}

fn create_id_name_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        make_field(
            "Id",
            DataType::Utf8,
            FieldProperties {
                kind: FieldKind::Unique,
                nulls_fraction: 0.0,
                word_dictionary: 1000,
                ..Default::default()
            },
        ),
        make_field(
            "Text",
            DataType::Utf8,
            FieldProperties {
                kind: FieldKind::Line,
                nulls_fraction: 0.1,
                min_words: 0,
                max_words: 6,
                word_dictionary: 10,
                ..Default::default()
            },
        ),
    ]))
}
