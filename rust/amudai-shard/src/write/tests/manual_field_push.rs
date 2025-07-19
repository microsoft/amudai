use std::sync::Arc;

use amudai_format::schema::FieldLocator;
use arrow_array::Int64Array;
use arrow_schema::{DataType, Field, Schema};

use crate::tests::shard_store::ShardStore;

#[test]
fn test_manual_list_push() {
    let shard_store = ShardStore::new();
    let list_field = Field::new(
        "list",
        DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, true))),
        true,
    );
    let schema = Arc::new(Schema::new(vec![list_field]));

    let mut shard_builder = shard_store.create_shard_builder(&schema);
    let mut stripe_builder = shard_builder.build_stripe().unwrap();

    let values = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6]));

    let offsets = Arc::new(arrow_array::Int64Array::from(vec![0i64, 3, 5, 5, 5, 6]));

    let list_field_builder = stripe_builder.get_field(&FieldLocator::Ordinal(0)).unwrap();

    let items_field_builder = list_field_builder
        .get_child(&FieldLocator::Ordinal(0))
        .unwrap();

    for _ in 0..10 {
        items_field_builder.push_array(values.clone()).unwrap();
    }

    for _ in 0..10 {
        list_field_builder.push_array(offsets.clone()).unwrap();
    }
    assert_eq!(list_field_builder.encoder().logical_len(), 50);

    // Set the next record position to match the number of lists we created
    stripe_builder.set_next_record_position(50);

    let prepared_stripe = stripe_builder.finish().unwrap();

    assert_eq!(prepared_stripe.fields.len(), 2); // Should have list field and items field

    shard_builder.add_stripe(prepared_stripe).unwrap();
    let shard_url = shard_store.seal_shard_builder(shard_builder).url;

    let shard = shard_store.open_shard(&shard_url);
    let schema = shard.fetch_schema().unwrap();
    let offsets_field = schema.field_at(0).unwrap();
    assert_eq!(offsets_field.name().unwrap(), "list");
    let stripe = shard.open_stripe(0).unwrap();
    assert_eq!(stripe.record_count(), 50);

    let mut offsets_reader = stripe
        .open_field(offsets_field.data_type().unwrap())
        .unwrap()
        .create_decoder()
        .unwrap()
        .create_reader_with_ranges(std::iter::empty())
        .unwrap();
    let offsets = offsets_reader.read_range(0..51).unwrap();
    assert_eq!(*offsets.values.as_slice::<u64>().last().unwrap(), 60);
}
