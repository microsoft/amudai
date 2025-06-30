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

#[test]
fn test_basic_event_builder() {
    let mut builder = EventBuilder::default();

    // First event with empty labels
    builder.name_field().set("event1");
    builder.labels_field().finish_list();
    builder.finish_struct();

    // Second event with nested labels
    builder.name_field().set("event2");
    {
        let labels = builder.labels_field();
        let inner_list = labels.item();
        inner_list.item().set("tag1");
        inner_list.item().set("tag2");
        inner_list.finish_list();

        let inner_list = labels.item();
        inner_list.item().set("category1");
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
    builder.id_field().set(1);
    builder.name_field().set("Alice Johnson");
    builder.age_field().set(30);
    builder.email_field().set("alice@example.com");
    {
        let scores = builder.scores_field();
        scores.item().set(95.5);
        scores.item().set(87.2);
        scores.item().set(92.8);
        scores.finish_list();
    }
    {
        let metadata = builder.metadata_field();
        metadata.key().set("department");
        metadata.value().set("Engineering");
        metadata.key().set("level");
        metadata.value().set("Senior");
        metadata.finish_map();
    }
    builder.finish_struct();

    // Person 2: Minimal record with nulls
    builder.id_field().set(2);
    builder.name_field().set("Bob Smith");
    // age is null (not set)
    builder.email_field().set("bob@example.com");
    builder.scores_field().finish_list(); // Empty scores
    builder.metadata_field().finish_map(); // Empty metadata
    builder.finish_struct();

    // Person 3: Only required fields
    builder.id_field().set(3);
    builder.name_field().set("Charlie Brown");
    builder.age_field().set(25);
    // email is null
    {
        let scores = builder.scores_field();
        scores.item().set(78.5);
        scores.finish_list();
    }
    {
        let metadata = builder.metadata_field();
        metadata.key().set("status");
        metadata.value().set("Active");
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
    builder.id_field().set("record_001");
    builder
        .data_field()
        .set(b"Hello, World! This is some binary data.");
    builder
        .checksum_field()
        .set(b"abcdef1234567890abcdef1234567890"); // 32 bytes
    {
        let chunks = builder.chunks_field();
        chunks.item().set(b"chunk_data_001ab"); // 16 bytes
        chunks.item().set(b"chunk_data_002cd"); // 16 bytes
        chunks.item().set(b"chunk_data_003ef"); // 16 bytes
        chunks.finish_list();
    }
    builder.finish_struct();

    // Record 2: Minimal binary data
    builder.id_field().set("record_002");
    builder.data_field().set(b"Small data");
    builder
        .checksum_field()
        .set(b"1234567890abcdef1234567890abcdef"); // 32 bytes
    builder.chunks_field().finish_list(); // Empty chunks
    builder.finish_struct();

    // Record 3: Null binary data
    builder.id_field().set("record_003");
    // data is null
    builder
        .checksum_field()
        .set(b"fedcba0987654321fedcba0987654321"); // 32 bytes
    {
        let chunks = builder.chunks_field();
        chunks.item().set(b"single_chunk_16b"); // 16 bytes
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
    builder.outer_id_field().set(100);

    // Nested lists: [[1, 2, 3], [4, 5], [6]]
    {
        let nested_lists = builder.nested_lists_field();

        let inner_list = nested_lists.item();
        inner_list.item().set(1);
        inner_list.item().set(2);
        inner_list.item().set(3);
        inner_list.finish_list();

        let inner_list = nested_lists.item();
        inner_list.item().set(4);
        inner_list.item().set(5);
        inner_list.finish_list();

        let inner_list = nested_lists.item();
        inner_list.item().set(6);
        inner_list.finish_list();

        nested_lists.finish_list();
    }

    // Nested maps: [{"a": 1, "b": 2}, {"c": 3}]
    {
        let nested_maps = builder.nested_maps_field();

        let inner_map = nested_maps.item();
        inner_map.key().set("a");
        inner_map.value().set(1);
        inner_map.key().set("b");
        inner_map.value().set(2);
        inner_map.finish_map();

        let inner_map = nested_maps.item();
        inner_map.key().set("c");
        inner_map.value().set(3);
        inner_map.finish_map();

        nested_maps.finish_list();
    }

    // Tags: {"colors": ["red", "blue"], "sizes": ["large"]}
    {
        let tags = builder.tags_field();

        tags.key().set("colors");
        let color_list = tags.value();
        color_list.item().set("red");
        color_list.item().set("blue");
        color_list.finish_list();

        tags.key().set("sizes");
        let size_list = tags.value();
        size_list.item().set("large");
        size_list.finish_list();

        tags.finish_map();
    }
    builder.finish_struct();

    // Record 2: Simpler nested structure
    builder.outer_id_field().set(200);

    // Empty nested lists
    builder.nested_lists_field().finish_list();

    // Single nested map
    {
        let nested_maps = builder.nested_maps_field();
        let inner_map = nested_maps.item();
        inner_map.key().set("single");
        inner_map.value().set(42);
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
    string_builder.set("test1");
    string_builder.set("test2");
    string_builder.set_null();
    string_builder.set("test3");
    let string_array = string_builder.build();
    assert_eq!(string_array.len(), 4);

    let mut int_builder = Int64Builder::default();
    int_builder.set(100);
    int_builder.set(-50);
    int_builder.set_null();
    int_builder.set(0);
    let int_array = int_builder.build();
    assert_eq!(int_array.len(), 4);

    let mut binary_builder = BinaryBuilder::default();
    binary_builder.set(b"binary1");
    binary_builder.set(b"binary2");
    binary_builder.set_null();
    let binary_array = binary_builder.build();
    assert_eq!(binary_array.len(), 3);
}

#[test]
fn test_list_builder_standalone() {
    let mut list_builder = ListBuilder::<Int32Builder>::default();

    // List 1: [1, 2, 3]
    list_builder.item().set(1);
    list_builder.item().set(2);
    list_builder.item().set(3);
    list_builder.finish_list();

    // List 2: [] (empty)
    list_builder.finish_list();

    // List 3: null
    list_builder.finish_null_list();

    // List 4: [42]
    list_builder.item().set(42);
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
    map_builder.key().set("key1");
    map_builder.value().set(10);
    map_builder.key().set("key2");
    map_builder.value().set(20);
    map_builder.finish_map();

    // Map 2: null
    map_builder.finish_null_map();

    // Map 3: {"single": 42}
    map_builder.key().set("single");
    map_builder.value().set(42);
    map_builder.finish_map();

    let array = map_builder.build();
    let map_array = array.as_map();
    assert_eq!(map_array.len(), 3);
    assert!(map_array.is_null(1)); // Second map is null
}

#[test]
fn test_fixed_size_binary_builder() {
    let mut builder = FixedSizeBinaryBuilder::<8>::default();

    builder.set(b"12345678"); // Exactly 8 bytes
    builder.set(b"abcdefgh"); // Exactly 8 bytes
    builder.set_null();
    builder.set(b"ZYXWVUTS"); // Exactly 8 bytes

    let array = builder.build();
    assert_eq!(array.len(), 4);
}

#[test]
fn test_mixed_null_handling() {
    let mut builder = PersonBuilder::default();

    // Record with mixed nulls
    builder.id_field().set(1);
    // name is null (not set)
    builder.age_field().set(25);
    builder.email_field().set("test@example.com");
    builder.scores_field().finish_list(); // Empty list (not null)
    builder.metadata_field().finish_null_map(); // Null map
    builder.finish_struct();

    // Completely null record
    builder.finish_null_struct();

    // Record with null collections
    builder.id_field().set(3);
    builder.name_field().set("Jane");
    // age is null
    // email is null
    builder.scores_field().finish_null_list(); // Null list
    {
        let metadata = builder.metadata_field();
        metadata.key().set("status");
        metadata.value().set("active");
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
        builder.id_field().set(i as i64);
        builder.name_field().set(format!("Person {}", i));
        builder.age_field().set(20 + (i % 60));
        builder
            .email_field()
            .set(format!("person{}@example.com", i));

        // Add some scores
        {
            let scores = builder.scores_field();
            scores.item().set(50.0 + (i % 50) as f64);
            if i % 3 == 0 {
                scores.item().set(75.0 + (i % 25) as f64);
            }
            scores.finish_list();
        }

        // Add metadata every 5th record
        if i % 5 == 0 {
            let metadata = builder.metadata_field();
            metadata.key().set("batch");
            metadata.value().set(format!("batch_{}", i / 100));
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
        level3_1.item().set("a");
        level3_1.item().set("b");
        level3_1.finish_list();

        let level3_2 = level2_1.item();
        level3_2.item().set("c");
        level3_2.finish_list();

        level2_1.finish_list();

        // Second middle list in first outer element
        let level2_2 = level1.item();
        let level3_3 = level2_2.item();
        level3_3.item().set("d");
        level3_3.finish_list();
        level2_2.finish_list();

        level1.finish_list();
    }

    // Build complex map: {"key1": [{"inner1": 1}, {"inner2": 2}], "key2": []}
    {
        let complex_map = builder.complex_map_field();

        complex_map.key().set("key1");
        let list_value = complex_map.value();

        let map1 = list_value.item();
        map1.key().set("inner1");
        map1.value().set(1);
        map1.finish_map();

        let map2 = list_value.item();
        map2.key().set("inner2");
        map2.value().set(2);
        map2.finish_map();

        list_value.finish_list();

        complex_map.key().set("key2");
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
        builder.age_field().set(20 + i);

        builder.scores_field().item().set(1.1 * i as f64);
        builder.scores_field().item().set(2.2 * i as f64);
        builder.scores_field().finish_list();

        builder.email_field().set("aaa@example.com");

        builder.metadata_field().key().set("key0");
        builder.metadata_field().value().set(format!("value_{i}"));
        builder.metadata_field().finish_map();

        builder.finish_record();
    }

    let record_batch = builder.build();
    let json_output = record_batch_to_ndjson(&record_batch).unwrap();
    println!("Record batch JSON:\n{}", json_output);
    assert_eq!(record_batch.num_rows(), 5);
}
