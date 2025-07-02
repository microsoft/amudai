use amudai_arrow_builders_macros::struct_builder;
use amudai_arrow_processing::array_to_json::{array_to_ndjson, record_batch_to_ndjson};
use arrow_array::{Array, cast::AsArray};

use crate::{
    ArrayBuilder, BinaryBuilder, FixedSizeBinaryBuilder, Int64Builder, ListBuilder, MapBuilder,
    StringBuilder, primitive::Int32Builder, record_batch::RecordBatchBuilder,
};

struct_builder!(
    struct Event {
        name: String,
        labels: List<List<String>>,
    },
    crate
);

struct_builder!(
    struct Person {
        id: i64,
        name: String,
        age: i32,
        email: String,
        scores: List<Float64>,
        metadata: Map<String, String>,
    },
    crate
);

struct_builder!(
    struct BinaryRecord {
        id: String,
        data: Binary,
        checksum: FixedSizeBinary<32>,
        chunks: List<FixedSizeBinary<16>>,
    },
    crate
);

struct_builder!(
    struct NestedData {
        outer_id: i64,
        nested_lists: List<List<i32>>,
        nested_maps: List<Map<String, i64>>,
        tags: Map<String, List<String>>,
    },
    crate
);

struct_builder!(
    struct NodeRecord {
        id: String,
        flag: bool,
        properties: FluidStruct,
    },
    crate
);

#[test]
fn test_basic_event_builder() {
    let mut builder = EventBuilder::default();

    // First event with empty labels
    builder.name_field().push("event1");
    builder.labels_field().finish_list();
    builder.finish_struct();

    // Second event with nested labels
    builder.name_field().push("event2");
    {
        let labels = builder.labels_field();
        let inner_list = labels.item();
        inner_list.item().push("tag1");
        inner_list.item().push("tag2");
        inner_list.finish_list();

        let inner_list = labels.item();
        inner_list.item().push("category1");
        inner_list.finish_list();

        labels.finish_list();
    }
    builder.finish_struct();

    // Third event - null
    builder.finish_null_struct();

    let array = builder.build();
    let json_output = array_to_ndjson(&array).unwrap();
    println!("Event JSON:\n{}", json_output);

    // Verify array structure
    let struct_array = array.as_struct();
    assert_eq!(struct_array.len(), 3);
    assert_eq!(struct_array.column_names(), &["name", "labels"]);
    assert!(struct_array.is_null(2)); // Third record is null
}

#[test]
fn test_person_builder_comprehensive() {
    let mut builder = PersonBuilder::default();

    // Person 1: Complete record
    builder.id_field().push(1);
    builder.name_field().push("Alice Johnson");
    builder.age_field().push(30);
    builder.email_field().push("alice@example.com");
    {
        let scores = builder.scores_field();
        scores.item().push(95.5);
        scores.item().push(87.2);
        scores.item().push(92.8);
        scores.finish_list();
    }
    {
        let metadata = builder.metadata_field();
        metadata.key().push("department");
        metadata.value().push("Engineering");
        metadata.key().push("level");
        metadata.value().push("Senior");
        metadata.finish_map();
    }
    builder.finish_struct();

    // Person 2: Minimal record with nulls
    builder.id_field().push(2);
    builder.name_field().push("Bob Smith");
    // age is null (not set)
    builder.email_field().push("bob@example.com");
    builder.scores_field().finish_list(); // Empty scores
    builder.metadata_field().finish_map(); // Empty metadata
    builder.finish_struct();

    // Person 3: Only required fields
    builder.id_field().push(3);
    builder.name_field().push("Charlie Brown");
    builder.age_field().push(25);
    // email is null
    {
        let scores = builder.scores_field();
        scores.item().push(78.5);
        scores.finish_list();
    }
    {
        let metadata = builder.metadata_field();
        metadata.key().push("status");
        metadata.value().push("Active");
        metadata.finish_map();
    }
    builder.finish_struct();

    let array = builder.build();
    let json_output = array_to_ndjson(&array).unwrap();
    println!("Person JSON:\n{}", json_output);

    // Verify array structure
    let struct_array = array.as_struct();
    assert_eq!(struct_array.len(), 3);
    assert_eq!(
        struct_array.column_names(),
        &["id", "name", "age", "email", "scores", "metadata"]
    );
}

#[test]
fn test_binary_record_builder() {
    let mut builder = BinaryRecordBuilder::default();

    // Record 1: Complete binary data
    builder.id_field().push("record_001");
    builder
        .data_field()
        .push(b"Hello, World! This is some binary data.");
    builder
        .checksum_field()
        .push(b"abcdef1234567890abcdef1234567890"); // 32 bytes
    {
        let chunks = builder.chunks_field();
        chunks.item().push(b"chunk_data_001ab"); // 16 bytes
        chunks.item().push(b"chunk_data_002cd"); // 16 bytes
        chunks.item().push(b"chunk_data_003ef"); // 16 bytes
        chunks.finish_list();
    }
    builder.finish_struct();

    // Record 2: Minimal binary data
    builder.id_field().push("record_002");
    builder.data_field().push(b"Small data");
    builder
        .checksum_field()
        .push(b"1234567890abcdef1234567890abcdef"); // 32 bytes
    builder.chunks_field().finish_list(); // Empty chunks
    builder.finish_struct();

    // Record 3: Null binary data
    builder.id_field().push("record_003");
    // data is null
    builder
        .checksum_field()
        .push(b"fedcba0987654321fedcba0987654321"); // 32 bytes
    {
        let chunks = builder.chunks_field();
        chunks.item().push(b"single_chunk_16b"); // 16 bytes
        chunks.finish_list();
    }
    builder.finish_struct();

    let array = builder.build();
    let json_output = array_to_ndjson(&array).unwrap();
    println!("Binary Record JSON:\n{}", json_output);

    // Verify array structure
    let struct_array = array.as_struct();
    assert_eq!(struct_array.len(), 3);
    assert_eq!(
        struct_array.column_names(),
        &["id", "data", "checksum", "chunks"]
    );
}

#[test]
fn test_nested_data_builder() {
    let mut builder = NestedDataBuilder::default();

    // Record 1: Complex nested structure
    builder.outer_id_field().push(100);

    // Nested lists: [[1, 2, 3], [4, 5], [6]]
    {
        let nested_lists = builder.nested_lists_field();

        let inner_list = nested_lists.item();
        inner_list.item().push(1);
        inner_list.item().push(2);
        inner_list.item().push(3);
        inner_list.finish_list();

        let inner_list = nested_lists.item();
        inner_list.item().push(4);
        inner_list.item().push(5);
        inner_list.finish_list();

        let inner_list = nested_lists.item();
        inner_list.item().push(6);
        inner_list.finish_list();

        nested_lists.finish_list();
    }

    // Nested maps: [{"a": 1, "b": 2}, {"c": 3}]
    {
        let nested_maps = builder.nested_maps_field();

        let inner_map = nested_maps.item();
        inner_map.key().push("a");
        inner_map.value().push(1);
        inner_map.key().push("b");
        inner_map.value().push(2);
        inner_map.finish_map();

        let inner_map = nested_maps.item();
        inner_map.key().push("c");
        inner_map.value().push(3);
        inner_map.finish_map();

        nested_maps.finish_list();
    }

    // Tags: {"colors": ["red", "blue"], "sizes": ["large"]}
    {
        let tags = builder.tags_field();

        tags.key().push("colors");
        let color_list = tags.value();
        color_list.item().push("red");
        color_list.item().push("blue");
        color_list.finish_list();

        tags.key().push("sizes");
        let size_list = tags.value();
        size_list.item().push("large");
        size_list.finish_list();

        tags.finish_map();
    }
    builder.finish_struct();

    // Record 2: Simpler nested structure
    builder.outer_id_field().push(200);

    // Empty nested lists
    builder.nested_lists_field().finish_list();

    // Single nested map
    {
        let nested_maps = builder.nested_maps_field();
        let inner_map = nested_maps.item();
        inner_map.key().push("single");
        inner_map.value().push(42);
        inner_map.finish_map();
        nested_maps.finish_list();
    }

    // Empty tags
    builder.tags_field().finish_map();
    builder.finish_struct();

    let array = builder.build();
    let json_output = array_to_ndjson(&array).unwrap();
    println!("Nested Data JSON:\n{}", json_output);

    // Verify array structure
    let struct_array = array.as_struct();
    assert_eq!(struct_array.len(), 2);
    assert_eq!(
        struct_array.column_names(),
        &["outer_id", "nested_lists", "nested_maps", "tags"]
    );
}

#[test]
fn test_primitive_builders_standalone() {
    // Test individual primitive builders
    let mut string_builder = StringBuilder::default();
    string_builder.push("test1");
    string_builder.push("test2");
    string_builder.push_null();
    string_builder.push("test3");
    let string_array = string_builder.build();
    assert_eq!(string_array.len(), 4);

    let mut int_builder = Int64Builder::default();
    int_builder.push(100);
    int_builder.push(-50);
    int_builder.push_null();
    int_builder.push(0);
    let int_array = int_builder.build();
    assert_eq!(int_array.len(), 4);

    let mut binary_builder = BinaryBuilder::default();
    binary_builder.push(b"binary1");
    binary_builder.push(b"binary2");
    binary_builder.push_null();
    let binary_array = binary_builder.build();
    assert_eq!(binary_array.len(), 3);
}

#[test]
fn test_list_builder_standalone() {
    let mut list_builder = ListBuilder::<Int32Builder>::default();

    // List 1: [1, 2, 3]
    list_builder.item().push(1);
    list_builder.item().push(2);
    list_builder.item().push(3);
    list_builder.finish_list();

    // List 2: [] (empty)
    list_builder.finish_list();

    // List 3: null
    list_builder.finish_null_list();

    // List 4: [42]
    list_builder.item().push(42);
    list_builder.finish_list();

    let array = list_builder.build();
    let list_array = array.as_list::<i64>();
    assert_eq!(list_array.len(), 4);
    assert!(list_array.is_null(2)); // Third list is null
}

#[test]
fn test_map_builder_standalone() {
    let mut map_builder = MapBuilder::<StringBuilder, Int32Builder>::default();

    // Map 1: {"key1": 10, "key2": 20}
    map_builder.key().push("key1");
    map_builder.value().push(10);
    map_builder.key().push("key2");
    map_builder.value().push(20);
    map_builder.finish_map();

    // Map 2: null
    map_builder.finish_null_map();

    // Map 3: {"single": 42}
    map_builder.key().push("single");
    map_builder.value().push(42);
    map_builder.finish_map();

    let array = map_builder.build();
    let map_array = array.as_map();
    assert_eq!(map_array.len(), 3);
    assert!(map_array.is_null(1)); // Second map is null
}

#[test]
fn test_fixed_size_binary_builder() {
    let mut builder = FixedSizeBinaryBuilder::<8>::default();

    builder.push(b"12345678"); // Exactly 8 bytes
    builder.push(b"abcdefgh"); // Exactly 8 bytes
    builder.push_null();
    builder.push(b"ZYXWVUTS"); // Exactly 8 bytes

    let array = builder.build();
    assert_eq!(array.len(), 4);
}

#[test]
fn test_mixed_null_handling() {
    let mut builder = PersonBuilder::default();

    // Record with mixed nulls
    builder.id_field().push(1);
    // name is null (not set)
    builder.age_field().push(25);
    builder.email_field().push("test@example.com");
    builder.scores_field().finish_list(); // Empty list (not null)
    builder.metadata_field().finish_null_map(); // Null map
    builder.finish_struct();

    // Completely null record
    builder.finish_null_struct();

    // Record with null collections
    builder.id_field().push(3);
    builder.name_field().push("Jane");
    // age is null
    // email is null
    builder.scores_field().finish_null_list(); // Null list
    {
        let metadata = builder.metadata_field();
        metadata.key().push("status");
        metadata.value().push("active");
        metadata.finish_map();
    }
    builder.finish_struct();

    let array = builder.build();
    let struct_array = array.as_struct();
    assert_eq!(struct_array.len(), 3);
    assert!(struct_array.is_null(1)); // Second record is completely null
}

#[test]
fn test_large_dataset() {
    let mut builder = PersonBuilder::default();

    // Generate a larger dataset to test performance and correctness
    for i in 0..1000 {
        builder.id_field().push(i as i64);
        builder.name_field().push(format!("Person {}", i));
        builder.age_field().push(20 + (i % 60));
        builder
            .email_field()
            .push(format!("person{}@example.com", i));

        // Add some scores
        {
            let scores = builder.scores_field();
            scores.item().push(50.0 + (i % 50) as f64);
            if i % 3 == 0 {
                scores.item().push(75.0 + (i % 25) as f64);
            }
            scores.finish_list();
        }

        // Add metadata every 5th record
        if i % 5 == 0 {
            let metadata = builder.metadata_field();
            metadata.key().push("batch");
            metadata.value().push(format!("batch_{}", i / 100));
            metadata.finish_map();
        } else {
            builder.metadata_field().finish_map();
        }

        builder.finish_struct();
    }

    let array = builder.build();
    let struct_array = array.as_struct();
    assert_eq!(struct_array.len(), 1000);

    println!(
        "Successfully built array with {} records",
        struct_array.len()
    );
}

#[test]
fn test_deeply_nested_structures() {
    // Create a struct with very deep nesting
    struct_builder!(
        struct DeepNested {
            level1: List<List<List<String>>>,
            complex_map: Map<String, List<Map<String, i64>>>,
        },
        crate
    );

    let mut builder = DeepNestedBuilder::default();

    // Build deeply nested list: [[[["a", "b"], ["c"]], [["d"]]], [[["e"]]]]
    {
        let level1 = builder.level1_field();

        // First outer list element
        let level2_1 = level1.item();

        // First middle list in first outer element
        let level3_1 = level2_1.item();
        level3_1.item().push("a");
        level3_1.item().push("b");
        level3_1.finish_list();

        let level3_2 = level2_1.item();
        level3_2.item().push("c");
        level3_2.finish_list();

        level2_1.finish_list();

        // Second middle list in first outer element
        let level2_2 = level1.item();
        let level3_3 = level2_2.item();
        level3_3.item().push("d");
        level3_3.finish_list();
        level2_2.finish_list();

        level1.finish_list();
    }

    // Build complex map: {"key1": [{"inner1": 1}, {"inner2": 2}], "key2": []}
    {
        let complex_map = builder.complex_map_field();

        complex_map.key().push("key1");
        let list_value = complex_map.value();

        let map1 = list_value.item();
        map1.key().push("inner1");
        map1.value().push(1);
        map1.finish_map();

        let map2 = list_value.item();
        map2.key().push("inner2");
        map2.value().push(2);
        map2.finish_map();

        list_value.finish_list();

        complex_map.key().push("key2");
        let empty_list = complex_map.value();
        empty_list.finish_list();

        complex_map.finish_map();
    }

    builder.finish_struct();

    let array = builder.build();
    let json_output = array_to_ndjson(&array).unwrap();
    println!("Deep Nested JSON:\n{}", json_output);

    let struct_array = array.as_struct();
    assert_eq!(struct_array.len(), 1);
}

#[test]
fn test_record_batch() {
    let mut builder = RecordBatchBuilder::<PersonFields>::default();

    for i in 0..5 {
        builder.age_field().push(20 + i);

        builder.scores_field().item().push(1.1 * i as f64);
        builder.scores_field().item().push(2.2 * i as f64);
        builder.scores_field().finish_list();

        builder.email_field().push("aaa@example.com");

        builder.metadata_field().key().push("key0");
        builder.metadata_field().value().push(format!("value_{i}"));
        builder.metadata_field().finish_map();

        builder.finish_record();
    }

    let record_batch = builder.build();
    let json_output = record_batch_to_ndjson(&record_batch).unwrap();
    println!("Record batch JSON:\n{}", json_output);
    assert_eq!(record_batch.num_rows(), 5);
}

#[test]
fn test_fluid_struct_builder() {
    let mut builder = NodeRecordBuilder::default();
    builder
        .properties
        .register_field("a", &arrow_schema::DataType::Int64);
    builder
        .properties
        .register_field("b", &arrow_schema::DataType::LargeUtf8);
    builder
        .properties
        .register_field("c", &arrow_schema::DataType::FixedSizeBinary(10));

    for i in 0..3 {
        builder.id_field().push(i.to_string());
        builder
            .properties_field()
            .field("b")
            .try_push_raw_value(Some(b"abcd"))
            .unwrap();
        builder
            .properties_field()
            .field("c")
            .try_push_raw_value(Some(b"1234567890"))
            .unwrap();
        builder.properties_field().finish_struct();
        builder.finish_struct();
    }

    let arr = builder.build();
    assert_eq!(arr.len(), 3);
    let c = arr
        .as_struct()
        .column_by_name("properties")
        .unwrap()
        .as_struct()
        .column_by_name("c")
        .unwrap();
    let c = c.as_fixed_size_binary();
    assert_eq!(c.len(), 3);
    assert_eq!(c.value(0), b"1234567890");
    assert_eq!(c.value(2), b"1234567890");
}

#[test]
fn test_fluid_struct_builder_no_fields() {
    let mut builder = NodeRecordBuilder::default();
    for i in 0..3 {
        builder.id_field().push(i.to_string());
        builder.properties_field().finish_struct();
        builder.finish_struct();
    }

    let arr = builder.build();
    assert_eq!(arr.len(), 3);
    let s = amudai_arrow_processing::array_to_json::array_to_ndjson(&arr).unwrap();
    assert!(s.contains(r#""properties":{}"#));
}

#[test]
fn test_bool_builder() {
    let mut builder = RecordBatchBuilder::<NodeRecordFields>::default();

    for i in 0..5 {
        builder.id_field().push(i.to_string());
        if i != 3 {
            builder.flag_field().push(i % 2 == 0);
        }
        builder.finish_record();
    }

    let batch = builder.build();
    let s = amudai_arrow_processing::array_to_json::record_batch_to_ndjson(&batch).unwrap();
    assert!(s.contains(r#""flag":true"#));
    assert!(s.contains(r#""flag":false"#));
}
