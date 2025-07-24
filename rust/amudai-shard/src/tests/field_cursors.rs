use amudai_blockstream::read::block_stream::empty_hint;
use amudai_format::schema::SchemaId;
use amudai_testkit::data_gen::procedural::generate_amudatum_record_batches;

use crate::tests::shard_store::ShardStore;

#[test]
fn test_field_cursor_read_list() {
    let store = ShardStore::new();

    let shard_ref =
        store.ingest_shard_from_record_batches(generate_amudatum_record_batches(5000, 512));

    let shard = store.open_shard_ref(&shard_ref);
    assert_eq!(shard.stripe_count(), 1);
    let stripe = shard.open_stripe(0).unwrap();

    let data_str_field = stripe.open_named_field("data_string").unwrap();
    let mut cursor = data_str_field
        .create_decoder()
        .unwrap()
        .create_string_cursor(empty_hint())
        .unwrap();

    for pos in 0..data_str_field.position_count() {
        let value = cursor.fetch(pos).unwrap();
        assert_eq!(value, format!("string_data_{pos}"));
    }

    let num_list_field = stripe.open_named_field("num_list").unwrap();
    let num_list_item_field = num_list_field.open_child_at(0).unwrap();
    assert!(num_list_field.position_count() < num_list_item_field.position_count());

    let mut list_cursor = num_list_field
        .create_decoder()
        .unwrap()
        .create_list_cursor(empty_hint())
        .unwrap();
    let mut item_cursor = num_list_item_field
        .create_decoder()
        .unwrap()
        .create_primitive_cursor::<i32>(empty_hint())
        .unwrap();

    for list_pos in 0..num_list_field.position_count() {
        let range = list_cursor.fetch(list_pos).unwrap();
        for item_pos in range {
            let num = item_cursor.fetch(item_pos).unwrap();
            assert!(num < 10);
        }
    }
}

#[test]
fn test_field_cursors_multiple_types_and_patterns() {
    let store = ShardStore::new();

    let shard_ref =
        store.ingest_shard_from_record_batches(generate_amudatum_record_batches(20000, 512));

    let shard = store.open_shard(&shard_ref.url);
    assert_eq!(shard.stripe_count(), 1);
    let stripe = shard.open_stripe(0).unwrap();

    let schema = shard.fetch_schema().unwrap();
    let last_schema_id = schema.next_schema_id().unwrap();

    let mut schema_id = SchemaId::zero();
    while schema_id < last_schema_id {
        let data_type = schema.resolve_schema_id(schema_id).unwrap().unwrap();
        let field = stripe.open_field(data_type.clone()).unwrap();
        let position_count = field.position_count();
        let decoder = field.create_decoder().unwrap();
        consume_field_with_cursor(&decoder, data_type, position_count);
        schema_id = schema_id.next();
    }
}

fn consume_primitive_field_cursor<T: bytemuck::Pod>(
    decoder: &crate::read::field_decoder::FieldDecoder,
    position_count: u64,
) {
    let mut cursor = decoder.create_primitive_cursor::<T>(empty_hint()).unwrap();
    for pos in 0..position_count {
        cursor.fetch(pos).unwrap();
        cursor.is_null(pos).unwrap();
    }

    let mut cursor = decoder.create_primitive_cursor::<T>(empty_hint()).unwrap();
    for pos in (0..position_count).rev() {
        cursor.fetch(pos).unwrap();
        cursor.is_null(pos).unwrap();
    }

    let mut cursor = decoder.create_primitive_cursor::<T>(empty_hint()).unwrap();
    let mut pos = 1000u64;
    for i in 2000u64..2050 {
        cursor.fetch(pos).unwrap();
        cursor.is_null(pos).unwrap();
        pos = (pos + i) % position_count;
    }

    // Cursors can also provide primitives as fixed-size bytes.
    consume_fixed_bytes_field_cursor(decoder, std::mem::size_of::<T>(), position_count);
}

fn consume_struct_field_cursor(
    decoder: &crate::read::field_decoder::FieldDecoder,
    position_count: u64,
) {
    let mut cursor = decoder.create_struct_cursor(empty_hint()).unwrap();
    for pos in 0..position_count {
        cursor.is_null(pos).unwrap();
    }

    let mut cursor = decoder.create_struct_cursor(empty_hint()).unwrap();
    for pos in (0..position_count).rev() {
        cursor.is_null(pos).unwrap();
    }

    let mut cursor = decoder.create_struct_cursor(empty_hint()).unwrap();
    let mut pos = 1000u64;
    for i in 2000u64..2050 {
        cursor.is_null(pos).unwrap();
        pos = (pos + i) % position_count;
    }
}

fn consume_fixed_bytes_field_cursor(
    decoder: &crate::read::field_decoder::FieldDecoder,
    value_size: usize,
    position_count: u64,
) {
    let mut cursor = decoder.create_bytes_cursor(empty_hint()).unwrap();
    for pos in 0..position_count {
        let value = cursor.fetch(pos).unwrap();
        assert_eq!(value.len(), value_size);
    }

    let mut cursor = decoder.create_bytes_cursor(empty_hint()).unwrap();
    for pos in (0..position_count).rev() {
        let value = cursor.fetch(pos).unwrap();
        assert_eq!(value.len(), value_size);
    }

    let mut cursor = decoder.create_bytes_cursor(empty_hint()).unwrap();
    let mut pos = 1000u64;
    for i in 2000u64..2050 {
        let value = cursor.fetch(pos).unwrap();
        assert_eq!(value.len(), value_size);
        pos = (pos + i) % position_count;
    }
}

fn consume_bytes_field_cursor(
    decoder: &crate::read::field_decoder::FieldDecoder,
    position_count: u64,
) {
    let mut cursor = decoder.create_bytes_cursor(empty_hint()).unwrap();
    for pos in 0..position_count {
        cursor.fetch(pos).unwrap();
    }

    let mut cursor = decoder.create_bytes_cursor(empty_hint()).unwrap();
    for pos in (0..position_count).rev() {
        cursor.fetch(pos).unwrap();
    }

    let mut cursor = decoder.create_bytes_cursor(empty_hint()).unwrap();
    let mut pos = 1000u64;
    for i in 2000u64..2050 {
        cursor.fetch(pos).unwrap();
        pos = (pos + i) % position_count;
    }
}

fn consume_string_field_cursor(
    decoder: &crate::read::field_decoder::FieldDecoder,
    position_count: u64,
) {
    let mut cursor = decoder.create_string_cursor(empty_hint()).unwrap();
    for pos in 0..position_count {
        cursor.fetch(pos).unwrap();
        cursor.is_null(pos).unwrap();
    }

    let mut cursor = decoder.create_string_cursor(empty_hint()).unwrap();
    for pos in (0..position_count).rev() {
        cursor.fetch(pos).unwrap();
        cursor.fetch_nullable(pos).unwrap();
    }

    let mut cursor = decoder.create_string_cursor(empty_hint()).unwrap();
    let mut pos = 1000u64;
    for i in 2000u64..2050 {
        cursor.fetch(pos).unwrap();
        pos = (pos + i) % position_count;
    }
}

fn consume_list_field_cursor(
    decoder: &crate::read::field_decoder::FieldDecoder,
    position_count: u64,
) {
    let mut cursor = decoder.create_list_cursor(empty_hint()).unwrap();
    let mut last_offset = 0u64;
    for pos in 0..position_count {
        let range = cursor.fetch(pos).unwrap();
        assert_eq!(range.start, last_offset);
        last_offset = range.end;
    }

    let mut cursor = decoder.create_list_cursor(empty_hint()).unwrap();
    let mut last_offset = cursor.fetch(position_count - 1).unwrap().end;
    for pos in (0..position_count).rev() {
        let range = cursor.fetch(pos).unwrap();
        assert_eq!(range.end, last_offset);
        last_offset = range.start;
    }

    let mut cursor = decoder.create_list_cursor(empty_hint()).unwrap();
    let mut pos = 1000u64;
    for i in 2000u64..2050 {
        cursor.fetch(pos).unwrap();
        pos = (pos + i) % position_count;
    }
}

fn consume_field_with_cursor(
    decoder: &crate::read::field_decoder::FieldDecoder,
    data_type: &amudai_format::schema::DataType,
    position_count: u64,
) {
    use amudai_format::schema::BasicType;

    let type_desc = data_type.describe().unwrap();
    let basic_type = type_desc.basic_type;

    match basic_type {
        BasicType::String => consume_string_field_cursor(decoder, position_count),
        BasicType::Binary => consume_bytes_field_cursor(decoder, position_count),
        BasicType::Int8 => consume_primitive_field_cursor::<u8>(decoder, position_count),
        BasicType::Int16 => consume_primitive_field_cursor::<u16>(decoder, position_count),
        BasicType::Int32 => consume_primitive_field_cursor::<u32>(decoder, position_count),
        BasicType::Int64 => consume_primitive_field_cursor::<u64>(decoder, position_count),
        BasicType::Float32 => consume_primitive_field_cursor::<f32>(decoder, position_count),
        BasicType::Float64 => consume_primitive_field_cursor::<f64>(decoder, position_count),
        BasicType::Boolean => consume_primitive_field_cursor::<u8>(decoder, position_count),
        BasicType::FixedSizeBinary => {
            consume_fixed_bytes_field_cursor(decoder, type_desc.fixed_size as usize, position_count)
        }
        BasicType::DateTime => consume_primitive_field_cursor::<u64>(decoder, position_count),
        BasicType::List => consume_list_field_cursor(decoder, position_count),
        BasicType::Map => consume_list_field_cursor(decoder, position_count),
        BasicType::Struct => consume_struct_field_cursor(decoder, position_count),
        BasicType::Guid => consume_fixed_bytes_field_cursor(decoder, 16, position_count),
        _ => (),
    }
}
