use amudai_format::schema::FieldLocator;

use crate::tests::{
    data_generator::{create_primitive_flat_test_schema, generate_batch},
    shard_store::ShardStore,
};

#[test]
fn test_shard_without_properties() {
    let shard_store = ShardStore::new();
    let shard_ref = shard_store.ingest_shard_with_nested_schema(100);
    let shard = shard_store.open_shard(&shard_ref);
    let props = shard.fetch_properties().unwrap();
    assert!(props.is_empty());
    assert_eq!(props.creation_min(), 0);
    assert_eq!(props.creation_max(), 0);

    let stripe = shard.open_stripe(0).unwrap();
    let props = stripe.fetch_properties().unwrap();
    assert!(props.is_empty());

    let field = stripe.open_named_field("Text").unwrap();
    assert!(field.properties().is_empty());
}

#[test]
fn test_shard_with_properties() {
    let shard_store = ShardStore::new();

    let schema = create_primitive_flat_test_schema();
    let batch = generate_batch(schema.clone(), 2000);

    let mut builder = shard_store.create_shard_builder(&schema);

    let mut stripe = builder.build_stripe().unwrap();

    stripe.push_batch(&batch).unwrap();
    stripe.push_batch(&batch).unwrap();

    stripe
        .get_field(&FieldLocator::Name("num_i32"))
        .unwrap()
        .properties_mut()
        .set("prop_str", "num_i32_prop_value");
    stripe
        .get_field(&FieldLocator::Name("num_i64"))
        .unwrap()
        .properties_mut()
        .set("prop_str", "num_i64_prop_value");

    stripe.properties_mut().set("stripe_prop_u64", 987654u64);
    stripe.properties_mut().set("stripe_prop_str", "qqwww");

    builder.add_stripe(stripe.finish().unwrap()).unwrap();

    builder
        .properties_mut()
        .set_creation_time(amudai_arrow_compat::datetime_conversions::now_ticks());
    builder.properties_mut().set("prop_u64", 123456u64);
    builder.properties_mut().set("prop_str", "abcde");

    let shard_ref = builder
        .finish()
        .unwrap()
        .seal(&shard_store.generate_shard_url())
        .unwrap()
        .directory_ref;

    let shard = shard_store.open_shard(&shard_ref);
    let props = shard.fetch_properties().unwrap();
    assert!(!props.is_empty());
    assert_eq!(props.creation_min(), props.creation_min());
    let now = amudai_arrow_compat::datetime_conversions::now_ticks();
    assert!(props.creation_max() < now + 100000000 && props.creation_min() > now - 100000000);
    assert_eq!(props.get_u64("prop_u64").unwrap(), 123456);
    assert_eq!(props.get_str("prop_str").unwrap(), "abcde");

    let stripe = shard.open_stripe(0).unwrap();
    let props = stripe.fetch_properties().unwrap();
    assert!(!props.is_empty());
    assert_eq!(props.get_u64("stripe_prop_u64").unwrap(), 987654u64);
    assert_eq!(props.get_str("stripe_prop_str").unwrap(), "qqwww");

    let field = stripe.open_named_field("num_i32").unwrap();
    assert!(!field.properties().is_empty());
    assert_eq!(
        field.properties().get_str("prop_str").unwrap(),
        "num_i32_prop_value"
    );

    let field = stripe.open_named_field("num_i64").unwrap();
    assert!(!field.properties().is_empty());
    assert_eq!(
        field.properties().get_str("prop_str").unwrap(),
        "num_i64_prop_value"
    );
}
