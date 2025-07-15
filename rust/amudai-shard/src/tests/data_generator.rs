use std::{
    collections::HashMap,
    ops::Range,
    str::FromStr,
    sync::{Arc, LazyLock},
};

use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, ListArray, MapArray, RecordBatch, StructArray,
    builder::{
        BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
        StringBuilder,
    },
};
use arrow_buffer::{NullBuffer, OffsetBufferBuilder};
use arrow_schema::{DataType, Field, Fields, Schema};

pub fn create_nested_test_schema() -> Arc<Schema> {
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
        make_field(
            "Props",
            DataType::Struct(
                vec![
                    make_field("Prop1", DataType::Int32, Default::default()),
                    make_field(
                        "Prop2",
                        DataType::Float64,
                        FieldProperties {
                            kind: FieldKind::Unspecified,
                            nulls_fraction: 0.2,
                            ..Default::default()
                        },
                    ),
                ]
                .into(),
            ),
            FieldProperties {
                kind: FieldKind::Unspecified,
                nulls_fraction: 0.1,
                min_words: 0,
                max_words: 6,
                word_dictionary: 10,
                ..Default::default()
            },
        ),
        make_field(
            "Values",
            DataType::List(Arc::new(make_field(
                "item",
                DataType::Utf8,
                FieldProperties {
                    kind: FieldKind::Word,
                    nulls_fraction: 0.2,
                    ..Default::default()
                },
            ))),
            FieldProperties {
                kind: FieldKind::Unspecified,
                nulls_fraction: 0.1,
                min_value: 0,
                max_value: 10,
                ..Default::default()
            },
        ),
    ]))
}

pub fn create_map_test_schema() -> Arc<Schema> {
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
            "Props",
            DataType::Map(
                Arc::new(make_field(
                    "entries",
                    DataType::Struct(
                        vec![
                            make_field(
                                "key",
                                DataType::Utf8,
                                FieldProperties {
                                    kind: FieldKind::Word,
                                    nulls_fraction: 0.0,
                                    ..Default::default()
                                },
                            ),
                            make_field(
                                "value",
                                DataType::Utf8,
                                FieldProperties {
                                    kind: FieldKind::Word,
                                    nulls_fraction: 0.1,
                                    ..Default::default()
                                },
                            ),
                        ]
                        .into(),
                    ),
                    FieldProperties::default(),
                )),
                false,
            ),
            FieldProperties {
                kind: FieldKind::Unspecified,
                nulls_fraction: 0.1,
                min_value: 0,
                max_value: 10,
                ..Default::default()
            },
        ),
    ]))
}

pub fn create_primitive_flat_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        make_field(
            "num_i32",
            DataType::Int32,
            FieldProperties {
                kind: FieldKind::Unspecified,
                nulls_fraction: 0.1,
                min_value: 0,
                max_value: 10,
                ..Default::default()
            },
        ),
        make_field(
            "num_i64",
            DataType::Int64,
            FieldProperties {
                kind: FieldKind::Unspecified,
                nulls_fraction: 0.1,
                min_value: 0,
                max_value: 10,
                ..Default::default()
            },
        ),
    ]))
}

pub fn create_boolean_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![make_field(
        "x",
        DataType::Boolean,
        FieldProperties {
            kind: FieldKind::Unspecified,
            nulls_fraction: 0.1,
            ..Default::default()
        },
    )]))
}

pub fn create_bytes_flat_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        make_field(
            "str_words",
            DataType::Utf8,
            FieldProperties {
                kind: FieldKind::Line,
                nulls_fraction: 0.1,
                min_words: 0,
                max_words: 10,
                ..Default::default()
            },
        ),
        make_field(
            "bin_word",
            DataType::Binary,
            FieldProperties {
                kind: FieldKind::Word,
                nulls_fraction: 0.2,
                ..Default::default()
            },
        ),
    ]))
}

pub fn generate_batches(
    schema: Arc<arrow_schema::Schema>,
    batch_size: Range<usize>,
    record_count: usize,
) -> Box<dyn Iterator<Item = RecordBatch>> {
    let batch_sizes = amudai_arrow_processing::array_sequence::random_split(
        record_count,
        batch_size.start,
        batch_size.end,
    );
    Box::new(
        batch_sizes
            .into_iter()
            .map(move |batch_size| generate_batch(schema.clone(), batch_size)),
    )
}

pub fn generate_batch(schema: Arc<arrow_schema::Schema>, batch_size: usize) -> RecordBatch {
    let mut columns = Vec::<ArrayRef>::new();

    for field in schema.fields() {
        let array = generate_field(field, batch_size);
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns).expect("Failed to create record batch")
}

pub fn generate_field(field: &arrow_schema::Field, len: usize) -> Arc<dyn Array> {
    match field.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            generate_string_field(field, len)
        }
        DataType::Binary | DataType::LargeBinary => generate_binary_field(field, len),
        DataType::FixedSizeBinary(16) if is_decimal_field(field) => {
            generate_decimal_field(field, len)
        }
        DataType::FixedSizeBinary(_) => generate_fixed_size_binary_field(field, len),
        DataType::Int32 => generate_int32_field(field, len),
        DataType::Int64 => generate_int64_field(field, len),
        DataType::Float32 => generate_float32_field(field, len),
        DataType::Float64 => generate_float64_field(field, len),
        DataType::Boolean => generate_boolean_field(field, len),
        DataType::Struct(fields) => generate_struct(
            &FieldProperties::from_metadata(field.metadata()),
            fields,
            len,
        ),
        DataType::List(item) => {
            generate_list(&FieldProperties::from_metadata(field.metadata()), item, len)
        }
        DataType::LargeList(item) => {
            generate_list(&FieldProperties::from_metadata(field.metadata()), item, len)
        }
        DataType::Map(entry_field, _) => generate_map(
            &FieldProperties::from_metadata(field.metadata()),
            entry_field,
            len,
        ),
        _ => unimplemented!("generate_field for {:?}", field.data_type()),
    }
}

pub fn generate_struct(props: &FieldProperties, fields: &Fields, len: usize) -> Arc<dyn Array> {
    let mut generated_fields = Vec::with_capacity(fields.len());
    for field in fields {
        generated_fields.push(generate_field(field, len));
    }

    let validity = if props.nulls_fraction > 0.0 {
        let mut validity_builder = Vec::with_capacity(len);
        for _ in 0..len {
            if fastrand::f64() < props.nulls_fraction {
                validity_builder.push(false);
            } else {
                validity_builder.push(true);
            }
        }
        Some(NullBuffer::from(validity_builder))
    } else {
        None
    };

    let struct_array = StructArray::new(fields.clone(), generated_fields, validity);
    Arc::new(struct_array)
}

pub fn generate_list(props: &FieldProperties, field: &Arc<Field>, len: usize) -> Arc<dyn Array> {
    let mut offsets = OffsetBufferBuilder::<i32>::new(len);

    let min_list_size = props.min_value as usize;
    let max_list_size = (props.max_value as usize + 1).max(min_list_size + 1);

    let list_sizes = (0..len)
        .map(|_| {
            let is_valid = if props.nulls_fraction > 0.0 {
                fastrand::f64() >= props.nulls_fraction
            } else {
                true
            };
            let size = if is_valid {
                fastrand::usize(min_list_size..max_list_size)
            } else {
                0
            };
            (is_valid, size)
        })
        .collect::<Vec<_>>();

    let total_child_len = list_sizes.iter().map(|e| e.1).sum::<usize>();

    let child = generate_field(field, total_child_len);

    for &(_, size) in &list_sizes {
        offsets.push_length(size);
    }

    let validity = if props.nulls_fraction > 0.0 {
        let mut validity_builder = Vec::with_capacity(len);
        for &(is_valid, _) in &list_sizes {
            validity_builder.push(is_valid);
        }
        Some(NullBuffer::from(validity_builder))
    } else {
        None
    };

    Arc::new(ListArray::new(
        field.clone(),
        offsets.finish(),
        child,
        validity,
    ))
}

pub fn generate_map(
    props: &FieldProperties,
    entry_field: &Arc<Field>,
    len: usize,
) -> Arc<dyn Array> {
    let mut offsets = OffsetBufferBuilder::<i32>::new(len);

    let min_list_size = props.min_value as usize;
    let max_list_size = (props.max_value as usize + 1).max(min_list_size + 1);

    let list_sizes = (0..len)
        .map(|_| {
            let is_valid = if props.nulls_fraction > 0.0 {
                fastrand::f64() >= props.nulls_fraction
            } else {
                true
            };
            let size = if is_valid {
                fastrand::usize(min_list_size..max_list_size)
            } else {
                0
            };
            (is_valid, size)
        })
        .collect::<Vec<_>>();

    let total_child_len = list_sizes.iter().map(|e| e.1).sum::<usize>();

    let DataType::Struct(kv_fields) = entry_field.data_type() else {
        panic!("not a struct")
    };
    assert_eq!(kv_fields.len(), 2);

    let key = generate_field(&kv_fields[0], total_child_len);
    let value = generate_field(&kv_fields[1], total_child_len);

    for &(_, size) in &list_sizes {
        offsets.push_length(size);
    }

    let validity = if props.nulls_fraction > 0.0 {
        let mut validity_builder = Vec::with_capacity(len);
        for &(is_valid, _) in &list_sizes {
            validity_builder.push(is_valid);
        }
        Some(NullBuffer::from(validity_builder))
    } else {
        None
    };

    Arc::new(MapArray::new(
        entry_field.clone(),
        offsets.finish(),
        StructArray::new(kv_fields.clone(), vec![key, value], None),
        validity,
        false,
    ))
}

pub fn generate_int32_field(field: &arrow_schema::Field, len: usize) -> Arc<dyn Array> {
    let props = FieldProperties::from_metadata(field.metadata());
    let mut builder = Int32Builder::new();
    for _ in 0..len {
        if props.nulls_fraction > 0.0 && fastrand::f64() < props.nulls_fraction {
            builder.append_null();
        } else {
            let value = fastrand::i64(props.min_value..props.max_value) as i32;
            builder.append_value(value);
        }
    }
    Arc::new(builder.finish())
}

pub fn generate_int64_field(field: &arrow_schema::Field, len: usize) -> Arc<dyn Array> {
    let props = FieldProperties::from_metadata(field.metadata());
    let mut builder = Int64Builder::new();
    for _ in 0..len {
        if props.nulls_fraction > 0.0 && fastrand::f64() < props.nulls_fraction {
            builder.append_null();
        } else {
            let value = fastrand::i64(props.min_value..props.max_value);
            builder.append_value(value);
        }
    }
    Arc::new(builder.finish())
}

pub fn generate_float32_field(field: &arrow_schema::Field, len: usize) -> Arc<dyn Array> {
    let props = FieldProperties::from_metadata(field.metadata());
    let mut builder = Float32Builder::new();
    for _ in 0..len {
        if props.nulls_fraction > 0.0 && fastrand::f64() < props.nulls_fraction {
            builder.append_null();
        } else {
            let value = fastrand::f64() * (props.max_value - props.min_value) as f64
                + props.min_value as f64;
            builder.append_value(value as f32);
        }
    }
    Arc::new(builder.finish())
}

pub fn generate_float64_field(field: &arrow_schema::Field, len: usize) -> Arc<dyn Array> {
    let props = FieldProperties::from_metadata(field.metadata());
    let mut builder = Float64Builder::new();
    for _ in 0..len {
        if props.nulls_fraction > 0.0 && fastrand::f64() < props.nulls_fraction {
            builder.append_null();
        } else {
            let value = fastrand::f64() * (props.max_value - props.min_value) as f64
                + props.min_value as f64;
            builder.append_value(value);
        }
    }
    Arc::new(builder.finish())
}

pub fn generate_string_field(field: &arrow_schema::Field, len: usize) -> Arc<dyn Array> {
    let props = FieldProperties::from_metadata(field.metadata());
    let mut builder = StringBuilder::new();
    for _ in 0..len {
        if props.nulls_fraction > 0.0 && fastrand::f64() < props.nulls_fraction {
            builder.append_null();
        } else {
            match props.kind {
                FieldKind::Word => builder.append_value(pick_word(props.word_dictionary)),
                FieldKind::Line => builder.append_value(generate_line(
                    props.min_words..props.max_words,
                    props.word_dictionary,
                )),
                FieldKind::Character => {
                    builder.append_value(fastrand::char('a'..='z').to_string());
                }
                FieldKind::Unique | FieldKind::Unspecified => builder.append_value(format!(
                    "{}{}",
                    pick_word(props.word_dictionary),
                    fastrand::u32(..)
                )),
            }
        }
    }
    Arc::new(builder.finish())
}

pub fn generate_binary_field(field: &arrow_schema::Field, len: usize) -> Arc<dyn Array> {
    let props = FieldProperties::from_metadata(field.metadata());
    let mut builder = BinaryBuilder::new();
    for _ in 0..len {
        if props.nulls_fraction > 0.0 && fastrand::f64() < props.nulls_fraction {
            builder.append_null();
        } else {
            match props.kind {
                FieldKind::Word => builder.append_value(pick_word(props.word_dictionary)),
                FieldKind::Line => builder.append_value(generate_line(
                    props.min_words..props.max_words,
                    props.word_dictionary,
                )),
                FieldKind::Character => {
                    builder.append_value(fastrand::char('a'..='z').to_string());
                }
                FieldKind::Unique | FieldKind::Unspecified => builder.append_value(format!(
                    "{}{}",
                    pick_word(props.word_dictionary),
                    fastrand::u32(..)
                )),
            }
        }
    }
    Arc::new(builder.finish())
}

pub fn generate_boolean_field(field: &arrow_schema::Field, len: usize) -> Arc<dyn Array> {
    let props = FieldProperties::from_metadata(field.metadata());
    let mut builder = BooleanBuilder::new();
    for _ in 0..len {
        if props.nulls_fraction > 0.0 && fastrand::f64() < props.nulls_fraction {
            builder.append_null();
        } else {
            let value = fastrand::bool();
            builder.append_value(value);
        }
    }
    Arc::new(builder.finish())
}

pub fn generate_line(num_words: Range<usize>, dictionary_size: usize) -> String {
    let num_words = fastrand::usize(num_words);
    let mut sentence = String::new();
    for i in 0..num_words {
        sentence.push_str(pick_word(dictionary_size));
        if i < num_words - 1 {
            sentence.push(' ');
        }
    }
    sentence
}

pub fn pick_word(dictionary_size: usize) -> &'static String {
    static WORDS: LazyLock<Vec<String>> = LazyLock::new(|| {
        fastrand::seed(85402635);
        generate_dictionary(5000, 5..12)
    });
    let index = fastrand::usize(0..dictionary_size.min(WORDS.len()));
    &WORDS[index]
}

pub fn generate_dictionary(dictionary_size: usize, word_length: Range<usize>) -> Vec<String> {
    (0..dictionary_size)
        .map(|_| generate_word(fastrand::usize(word_length.clone())))
        .collect()
}

pub fn generate_word(length: usize) -> String {
    if length == 0 {
        return String::new();
    }

    let vowels = ['a', 'e', 'i', 'o', 'u'];
    let consonants = ['c', 'd', 'g', 'k', 'l', 'm', 'n', 'p', 'q', 'r', 's', 't'];

    let mut word = String::with_capacity(length);
    let mut use_vowel = fastrand::bool();

    for _ in 0..length {
        if use_vowel {
            let vowel = vowels[fastrand::usize(..vowels.len())];
            word.push(vowel);
        } else {
            let consonant = consonants[fastrand::usize(..consonants.len())];
            word.push(consonant);
        }
        use_vowel = !use_vowel;
    }

    word
}

pub fn make_field(
    name: impl Into<String>,
    data_type: arrow_schema::DataType,
    props: FieldProperties,
) -> arrow_schema::Field {
    arrow_schema::Field::new(name, data_type, props.nulls_fraction > 0.0)
        .with_metadata(props.to_metadata())
}

#[derive(Debug, Clone)]
pub struct FieldProperties {
    pub kind: FieldKind,
    pub nulls_fraction: f64,
    pub word_dictionary: usize,
    pub cardinality: usize,
    pub min_words: usize,
    pub max_words: usize,
    pub min_value: i64,
    pub max_value: i64,
}

impl Default for FieldProperties {
    fn default() -> Self {
        Self {
            kind: FieldKind::Unspecified,
            nulls_fraction: 0.0,
            word_dictionary: 100,
            cardinality: 100,
            min_words: 0,
            max_words: 10,
            min_value: 0,
            max_value: 100,
        }
    }
}

impl FieldProperties {
    pub fn to_metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert("kind".to_string(), format!("{:?}", self.kind));
        metadata.insert(
            "nulls_fraction".to_string(),
            self.nulls_fraction.to_string(),
        );
        metadata.insert(
            "word_dictionary".to_string(),
            self.word_dictionary.to_string(),
        );
        metadata.insert("cardinality".to_string(), self.cardinality.to_string());
        metadata.insert("min_words".to_string(), self.min_words.to_string());
        metadata.insert("max_words".to_string(), self.max_words.to_string());
        metadata.insert("min_value".to_string(), self.min_value.to_string());
        metadata.insert("max_value".to_string(), self.max_value.to_string());
        metadata
    }

    pub fn from_metadata(metadata: &HashMap<String, String>) -> FieldProperties {
        let mut properties = FieldProperties::default();

        if let Some(kind_str) = metadata.get("kind") {
            if let Ok(kind) = FieldKind::from_str(kind_str) {
                properties.kind = kind;
            }
        }

        if let Some(nulls_fraction_str) = metadata.get("nulls_fraction") {
            if let Ok(nulls_fraction) = nulls_fraction_str.parse::<f64>() {
                properties.nulls_fraction = nulls_fraction;
            }
        }

        if let Some(word_dictionary_str) = metadata.get("word_dictionary") {
            if let Ok(word_dictionary) = word_dictionary_str.parse::<usize>() {
                properties.word_dictionary = word_dictionary;
            }
        }

        if let Some(cardinality_str) = metadata.get("cardinality") {
            if let Ok(cardinality) = cardinality_str.parse::<usize>() {
                properties.cardinality = cardinality;
            }
        }

        if let Some(min_words_str) = metadata.get("min_words") {
            if let Ok(min_words) = min_words_str.parse::<usize>() {
                properties.min_words = min_words;
            }
        }

        if let Some(max_words_str) = metadata.get("max_words") {
            if let Ok(max_words) = max_words_str.parse::<usize>() {
                properties.max_words = max_words;
            }
        }

        if let Some(min_value_str) = metadata.get("min_value") {
            if let Ok(min_value) = min_value_str.parse::<i64>() {
                properties.min_value = min_value;
            }
        }

        if let Some(max_value_str) = metadata.get("max_value") {
            if let Ok(max_value) = max_value_str.parse::<i64>() {
                properties.max_value = max_value;
            }
        }

        properties
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    Unspecified,
    Word,
    Line,
    Character,
    Unique,
}

impl FromStr for FieldKind {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Unspecified" => Ok(FieldKind::Unspecified),
            "Word" => Ok(FieldKind::Word),
            "Line" => Ok(FieldKind::Line),
            "Character" => Ok(FieldKind::Character),
            "Unique" => Ok(FieldKind::Unique),
            _ => Err(()),
        }
    }
}

/// Checks if a field represents a decimal type based on its extension metadata
fn is_decimal_field(field: &arrow_schema::Field) -> bool {
    field.metadata().get("ARROW:extension:name") == Some(&"KustoDecimal".to_string())
}

/// Generates a FixedSizeBinary field for decimal values (16 bytes for 128-bit decimals)
fn generate_decimal_field(field: &arrow_schema::Field, len: usize) -> Arc<dyn Array> {
    let props = FieldProperties::from_metadata(field.metadata());

    // Use Vec<[u8; 16]> to ensure all data has the same size
    let mut values: Vec<[u8; 16]> = Vec::with_capacity(len);
    let mut validity = Vec::with_capacity(len);

    for _ in 0..len {
        if props.nulls_fraction > 0.0 && fastrand::f64() < props.nulls_fraction {
            validity.push(false);
            values.push([0u8; 16]); // Placeholder for null
        } else {
            validity.push(true);
            // Generate a decimal value
            let raw_value = fastrand::i64(-100000000..=100000000);
            values.push(decimal_i64_to_bytes(raw_value));
        }
    }

    // Convert to the format expected by FixedSizeBinaryArray
    let data: Vec<Option<&[u8]>> = values
        .iter()
        .enumerate()
        .map(|(i, bytes)| {
            if validity[i] {
                Some(bytes.as_slice())
            } else {
                None
            }
        })
        .collect();

    Arc::new(FixedSizeBinaryArray::from(data))
}

/// Generates a regular FixedSizeBinary field (non-decimal)
fn generate_fixed_size_binary_field(field: &arrow_schema::Field, len: usize) -> Arc<dyn Array> {
    let props = FieldProperties::from_metadata(field.metadata());
    let size = if let DataType::FixedSizeBinary(s) = field.data_type() {
        *s as usize
    } else {
        16 // default size
    };

    let mut values: Vec<Vec<u8>> = Vec::with_capacity(len);
    let mut validity = Vec::with_capacity(len);

    for _ in 0..len {
        if props.nulls_fraction > 0.0 && fastrand::f64() < props.nulls_fraction {
            validity.push(false);
            values.push(vec![0u8; size]); // Placeholder for null
        } else {
            validity.push(true);
            // Generate random bytes
            let mut bytes = vec![0u8; size];
            for byte in &mut bytes {
                *byte = fastrand::u8(..);
            }
            values.push(bytes);
        }
    }

    // Convert to the format expected by FixedSizeBinaryArray
    let data: Vec<Option<&[u8]>> = values
        .iter()
        .enumerate()
        .map(|(i, bytes)| {
            if validity[i] {
                Some(bytes.as_slice())
            } else {
                None
            }
        })
        .collect();

    Arc::new(FixedSizeBinaryArray::from(data))
}

/// Converts an i64 representing a decimal with 2 decimal places to 16-byte representation
/// This is a simplified implementation for testing purposes
fn decimal_i64_to_bytes(value: i64) -> [u8; 16] {
    // For simplicity, we'll store the i64 value in the first 8 bytes of a 16-byte array
    // In a real implementation, this would use proper decimal encoding
    let mut bytes = [0u8; 16];
    let value_bytes = value.to_le_bytes();
    bytes[0..8].copy_from_slice(&value_bytes);
    bytes
}

/// Creates a test schema with decimal fields for testing
pub fn create_decimal_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        make_field("id", DataType::Int32, FieldProperties::default()),
        Field::new("amount", DataType::FixedSizeBinary(16), true).with_metadata(
            [(
                "ARROW:extension:name".to_string(),
                "KustoDecimal".to_string(),
            )]
            .into(),
        ),
        Field::new("price", DataType::FixedSizeBinary(16), false).with_metadata(
            [(
                "ARROW:extension:name".to_string(),
                "KustoDecimal".to_string(),
            )]
            .into(),
        ),
        make_field("description", DataType::Utf8, FieldProperties::default()),
    ]))
}

/// Creates a mixed schema with decimals and other types for comprehensive testing
pub fn create_mixed_schema_with_decimals() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        make_field("id", DataType::Int64, FieldProperties::default()),
        make_field("name", DataType::Utf8, FieldProperties::default()),
        Field::new("balance", DataType::FixedSizeBinary(16), true).with_metadata(
            [(
                "ARROW:extension:name".to_string(),
                "KustoDecimal".to_string(),
            )]
            .into(),
        ),
        make_field("active", DataType::Boolean, FieldProperties::default()),
        Field::new("score", DataType::FixedSizeBinary(16), false).with_metadata(
            [(
                "ARROW:extension:name".to_string(),
                "KustoDecimal".to_string(),
            )]
            .into(),
        ),
        make_field("metadata", DataType::Binary, FieldProperties::default()),
    ]))
}

#[test]
fn test_generate_list() {
    let list = generate_list(
        &FieldProperties {
            kind: FieldKind::Unspecified,
            nulls_fraction: 0.2,
            min_value: 0,
            max_value: 2,
            ..Default::default()
        },
        &Arc::new(make_field(
            "item",
            DataType::Utf8,
            FieldProperties {
                kind: FieldKind::Word,
                nulls_fraction: 0.2,
                ..Default::default()
            },
        )),
        20,
    );
    assert_eq!(list.len(), 20);
}
