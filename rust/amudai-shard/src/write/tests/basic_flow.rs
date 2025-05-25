use std::sync::Arc;

use amudai_arrow_compat::arrow_to_amudai_schema::FromArrowSchema;
use amudai_format::{schema::BasicType, schema_builder::SchemaBuilder};
use amudai_io_impl::temp_file_store;
use amudai_objectstore::null_store::NullObjectStore;
use arrow_array::RecordBatchIterator;
use arrow_schema::{DataType, Schema};

use crate::{
    tests::{
        data_generator::{self, create_nested_test_schema, make_field, FieldKind, FieldProperties},
        shard_store::ShardStore,
    },
    write::shard_builder::{ShardBuilder, ShardBuilderParams},
};

#[test]
fn test_basic_shard_builder_flow() {
    let arrow_schema = create_id_name_test_schema();

    let shard_schema = SchemaBuilder::from_arrow_schema(&arrow_schema).unwrap();

    let mut shard_builder = ShardBuilder::new(ShardBuilderParams {
        schema: shard_schema.into(),
        object_store: Arc::new(NullObjectStore),
        temp_store: temp_file_store::create_in_memory(32 * 1024 * 1024).unwrap(),
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
