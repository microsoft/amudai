//! A module providing traits and implementations for converting Apache Arrow schema components
//! into corresponding structures within the `amudai_format` crate.
//!
//! This module facilitates interoperability between the Apache Arrow data format definitions
//! and the custom schema representation used by `amudai_format`. It defines traits for converting
//! Arrow schemas, fields, and data types, and provides concrete implementations building
//! `amudai_format` schema components.
//!
//! # Traits
//!
//! This module defines three core traits:
//!
//! *   [`FromArrowSchema`]: For converting an entire Arrow schema into an `amudai_format` schema.
//! *   [`FromArrowField`]: For converting an Arrow field into an `amudai_format` field or data type builder.
//! *   [`FromArrowDataType`]: For converting an Arrow data type into an `amudai_format` data type builder.
//!
//! # Implementations
//!
//! The module provides implementations of these traits for the following `amudai_format` types:
//!
//! *   [`amudai_format::schema_builder::DataTypeBuilder`]: Used to build data type definitions.
//! *   [`amudai_format::schema_builder::FieldBuilder`]: Used to build field definitions.
//! *   [`amudai_format::schema_builder::SchemaBuilder`]: Used to build complete schema definitions.
//!
//! These implementations handle the mapping of various Arrow data types to their corresponding
//! `amudai_format` representations, including basic types, lists, structs, unions, and maps.
//!
//! # Error Handling
//!
//! The conversion process can fail if an unsupported Arrow data type is encountered. In such cases,
//! the conversion functions return a `Result` with an `Error` indicating the specific issue.

use amudai_common::{error::Error, Result};

/// A trait for converting an Apache Arrow schema into a corresponding type.
pub trait FromArrowSchema {
    /// Converts an Apache Arrow schema into the implementing type.
    ///
    /// # Arguments
    ///
    /// *   `schema`: A reference to the `arrow_schema::Schema` to convert.
    ///
    /// # Returns
    ///
    /// A `Result` containing the converted type or an `Error` if the conversion fails.
    fn from_arrow_schema(schema: &arrow_schema::Schema) -> Result<Self>
    where
        Self: Sized;
}

/// A trait for converting an Apache Arrow field into a corresponding type.
pub trait FromArrowField {
    /// Converts an Apache Arrow field into the implementing type.
    ///
    /// # Arguments
    ///
    /// *   `field`: A reference to the `arrow_schema::Field` to convert.
    ///
    /// # Returns
    ///
    /// A `Result` containing the converted type or an `Error` if the conversion fails.
    fn from_arrow_field(field: &arrow_schema::Field) -> Result<Self>
    where
        Self: Sized;
}

/// A trait for converting an Apache Arrow data type into a corresponding type.
pub trait FromArrowDataType {
    /// Converts an Apache Arrow data type into the implementing type.
    ///
    /// # Arguments
    ///
    /// *   `data_type`: A reference to the `arrow_schema::DataType` to convert.
    ///
    /// # Returns
    ///
    /// A `Result` containing the converted type or an `Error` if the conversion fails.
    fn from_arrow_data_type(data_type: &arrow_schema::DataType) -> Result<Self>
    where
        Self: Sized;
}

impl FromArrowDataType for amudai_format::schema_builder::DataTypeBuilder {
    fn from_arrow_data_type(data_type: &arrow_schema::DataType) -> Result<Self> {
        use amudai_format::schema::BasicType;
        use amudai_format::schema_builder::DataTypeBuilder;
        use arrow_schema::DataType as ArrowDataType;

        match data_type {
            ArrowDataType::Duration(_) => return Ok(DataTypeBuilder::new_timespan()),
            ArrowDataType::Decimal128(_, _) | ArrowDataType::Decimal256(_, _) => {
                return Ok(DataTypeBuilder::new_decimal())
            }
            _ => (),
        }

        let (basic_type, signed, fixed_size, children) = match data_type {
            ArrowDataType::Boolean => (BasicType::Boolean, false, None, vec![]),
            ArrowDataType::Int8 => (BasicType::Int8, true, None, vec![]),
            ArrowDataType::Int16 => (BasicType::Int16, true, None, vec![]),
            ArrowDataType::Int32 => (BasicType::Int32, true, None, vec![]),
            ArrowDataType::Int64 => (BasicType::Int64, true, None, vec![]),
            ArrowDataType::UInt8 => (BasicType::Int8, false, None, vec![]),
            ArrowDataType::UInt16 => (BasicType::Int16, false, None, vec![]),
            ArrowDataType::UInt32 => (BasicType::Int32, false, None, vec![]),
            ArrowDataType::UInt64 => (BasicType::Int64, false, None, vec![]),
            ArrowDataType::Float16 | ArrowDataType::Float32 => {
                (BasicType::Float32, false, None, vec![])
            }
            ArrowDataType::Float64 => (BasicType::Float64, false, None, vec![]),
            ArrowDataType::Timestamp(_, _) | ArrowDataType::Date32 | ArrowDataType::Date64 => {
                (BasicType::DateTime, false, None, vec![])
            }
            ArrowDataType::FixedSizeBinary(size) => (
                BasicType::FixedSizeBinary,
                false,
                Some(*size as u64),
                vec![],
            ),
            ArrowDataType::Binary | ArrowDataType::LargeBinary | ArrowDataType::BinaryView => {
                (BasicType::Binary, false, None, vec![])
            }
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
                (BasicType::String, false, None, vec![])
            }
            ArrowDataType::List(field) => (
                BasicType::List,
                false,
                None,
                vec![DataTypeBuilder::from_arrow_data_type(field.data_type())?],
            ),
            ArrowDataType::ListView(field) => (
                BasicType::List,
                false,
                None,
                vec![DataTypeBuilder::from_arrow_data_type(field.data_type())?],
            ),
            ArrowDataType::FixedSizeList(field, size) => (
                BasicType::FixedSizeList,
                false,
                Some(*size as u64),
                vec![DataTypeBuilder::from_arrow_data_type(field.data_type())?],
            ),
            ArrowDataType::LargeList(field) => (
                BasicType::List,
                false,
                None,
                vec![DataTypeBuilder::from_arrow_data_type(field.data_type())?],
            ),
            ArrowDataType::LargeListView(field) => (
                BasicType::List,
                false,
                None,
                vec![DataTypeBuilder::from_arrow_data_type(field.data_type())?],
            ),
            ArrowDataType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|field| DataTypeBuilder::from_arrow_field(field))
                    .collect::<Result<Vec<_>>>()?;
                (BasicType::Struct, false, None, fields)
            }
            ArrowDataType::Union(union_fields, _union_mode) => {
                let fields = union_fields
                    .iter()
                    .map(|(_, field)| DataTypeBuilder::from_arrow_field(field))
                    .collect::<Result<Vec<_>>>()?;
                (BasicType::Union, false, None, fields)
            }
            ArrowDataType::Dictionary(_, value_type) => {
                return DataTypeBuilder::from_arrow_data_type(value_type)
            }
            ArrowDataType::Map(entry, _) => match entry.data_type() {
                ArrowDataType::Struct(fields) if fields.len() == 2 => {
                    let key_type = DataTypeBuilder::from_arrow_data_type(fields[0].data_type())?;
                    let value_type = DataTypeBuilder::from_arrow_data_type(fields[1].data_type())?;
                    (BasicType::Map, false, None, vec![key_type, value_type])
                }
                _ => {
                    return Err(Error::invalid_arg(
                        "data_type",
                        format!("unsupported map entry data type {:?}", entry.data_type()),
                    ))
                }
            },
            ArrowDataType::RunEndEncoded(_, value_field) => {
                return DataTypeBuilder::from_arrow_data_type(value_field.data_type())
            }
            _ => {
                return Err(Error::invalid_arg(
                    "data_type",
                    format!("unsupported data type {data_type:?}"),
                ))
            }
        };

        let builder = DataTypeBuilder::new("", basic_type, signed, fixed_size, children);
        Ok(builder)
    }
}

impl FromArrowField for amudai_format::schema_builder::DataTypeBuilder {
    fn from_arrow_field(field: &arrow_schema::Field) -> Result<Self> {
        use amudai_format::schema_builder::DataTypeBuilder;
        use arrow_schema::DataType as ArrowDataType;

        let arrow_data_type = field.data_type();
        let amudai_data_type: DataTypeBuilder;

        let metadata_map = field.metadata();
        if let Some(extension_name) = metadata_map.get("ARROW:extension:name") {
            match (extension_name.as_str(), arrow_data_type) {
                (
                    "arrow.json",
                    ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View,
                ) => {
                    amudai_data_type = DataTypeBuilder::new_dynamic();
                }
                ("arrow.uuid", ArrowDataType::FixedSizeBinary(16)) => {
                    amudai_data_type = DataTypeBuilder::new_guid();
                }
                _ => {
                    amudai_data_type = DataTypeBuilder::from_arrow_data_type(arrow_data_type)?;
                }
            }
        } else {
            amudai_data_type = DataTypeBuilder::from_arrow_data_type(arrow_data_type)?;
        }

        Ok(amudai_data_type.with_field_name(field.name()))
    }
}

impl FromArrowField for amudai_format::schema_builder::FieldBuilder {
    fn from_arrow_field(field: &arrow_schema::Field) -> Result<Self> {
        use amudai_format::schema_builder::DataTypeBuilder;
        use amudai_format::schema_builder::FieldBuilder;

        let data_type = DataTypeBuilder::from_arrow_field(field)?;
        Ok(FieldBuilder::from(data_type))
    }
}

impl FromArrowSchema for amudai_format::schema_builder::SchemaBuilder {
    fn from_arrow_schema(schema: &arrow_schema::Schema) -> Result<Self> {
        use amudai_format::schema_builder::FieldBuilder;
        let fields = schema
            .fields()
            .iter()
            .map(|field| FieldBuilder::from_arrow_field(field))
            .collect::<Result<Vec<_>>>()?;
        Ok(amudai_format::schema_builder::SchemaBuilder::new(fields))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use amudai_format::{schema::BasicType, schema_builder::DataTypeBuilder};
    use arrow_schema::{DataType, UnionFields};

    use crate::arrow_to_amudai_schema::{FromArrowDataType, FromArrowSchema};

    #[test]
    fn test_from_arrow_data_type_basic() {
        let test_cases = vec![
            (DataType::Boolean, BasicType::Boolean, false, None, 0),
            (DataType::Int8, BasicType::Int8, true, None, 0),
            (DataType::Int16, BasicType::Int16, true, None, 0),
            (DataType::Int32, BasicType::Int32, true, None, 0),
            (DataType::Int64, BasicType::Int64, true, None, 0),
            (DataType::UInt8, BasicType::Int8, false, None, 0),
            (DataType::UInt16, BasicType::Int16, false, None, 0),
            (DataType::UInt32, BasicType::Int32, false, None, 0),
            (DataType::UInt64, BasicType::Int64, false, None, 0),
            (DataType::Float16, BasicType::Float32, false, None, 0),
            (DataType::Float32, BasicType::Float32, false, None, 0),
            (DataType::Float64, BasicType::Float64, false, None, 0),
            (
                DataType::Timestamp(arrow_schema::TimeUnit::Second, None),
                BasicType::DateTime,
                false,
                None,
                0,
            ),
            (DataType::Date32, BasicType::DateTime, false, None, 0),
            (DataType::Date64, BasicType::DateTime, false, None, 0),
            (
                DataType::Duration(arrow_schema::TimeUnit::Second),
                BasicType::Int64,
                true,
                None,
                0,
            ),
            (
                DataType::FixedSizeBinary(10),
                BasicType::FixedSizeBinary,
                false,
                Some(10),
                0,
            ),
            (DataType::Binary, BasicType::Binary, false, None, 0),
            (DataType::LargeBinary, BasicType::Binary, false, None, 0),
            (DataType::BinaryView, BasicType::Binary, false, None, 0),
            (DataType::Utf8, BasicType::String, false, None, 0),
            (DataType::LargeUtf8, BasicType::String, false, None, 0),
            (DataType::Utf8View, BasicType::String, false, None, 0),
        ];

        for (arrow_type, basic_type, signed, fixed_size, children_len) in test_cases {
            let result = DataTypeBuilder::from_arrow_data_type(&arrow_type).unwrap();
            assert_eq!(result.data_type().basic_type, basic_type);
            assert_eq!(result.data_type().signed, signed);
            assert_eq!(result.data_type().fixed_size, fixed_size.unwrap_or(0));
            assert_eq!(result.children().len(), children_len);
        }
    }

    #[test]
    fn test_from_arrow_data_type_list() {
        let inner_type = DataType::Int32;
        let list_type = DataType::List(Arc::new(arrow_schema::Field::new(
            "item",
            inner_type.clone(),
            false,
        )));
        let result = DataTypeBuilder::from_arrow_data_type(&list_type).unwrap();
        assert_eq!(result.data_type().basic_type, BasicType::List);
        assert_eq!(result.children().len(), 1);
        assert_eq!(
            result.children()[0].data_type().basic_type,
            BasicType::Int32
        );

        let list_view_type = DataType::ListView(Arc::new(arrow_schema::Field::new(
            "item",
            inner_type.clone(),
            false,
        )));
        let result = DataTypeBuilder::from_arrow_data_type(&list_view_type).unwrap();
        assert_eq!(result.data_type().basic_type, BasicType::List);
        assert_eq!(result.children().len(), 1);
        assert_eq!(
            result.children()[0].data_type().basic_type,
            BasicType::Int32
        );

        let large_list_type = DataType::LargeList(Arc::new(arrow_schema::Field::new(
            "item",
            inner_type.clone(),
            false,
        )));
        let result = DataTypeBuilder::from_arrow_data_type(&large_list_type).unwrap();
        assert_eq!(result.data_type().basic_type, BasicType::List);
        assert_eq!(result.children().len(), 1);
        assert_eq!(
            result.children()[0].data_type().basic_type,
            BasicType::Int32
        );

        let large_list_view_type = DataType::LargeListView(Arc::new(arrow_schema::Field::new(
            "item",
            inner_type.clone(),
            false,
        )));
        let result = DataTypeBuilder::from_arrow_data_type(&large_list_view_type).unwrap();
        assert_eq!(result.data_type().basic_type, BasicType::List);
        assert_eq!(result.children().len(), 1);
        assert_eq!(
            result.children()[0].data_type().basic_type,
            BasicType::Int32
        );

        let fixed_size_list_type = DataType::FixedSizeList(
            Arc::new(arrow_schema::Field::new("item", inner_type.clone(), false)),
            5,
        );
        let result = DataTypeBuilder::from_arrow_data_type(&fixed_size_list_type).unwrap();
        assert_eq!(result.data_type().basic_type, BasicType::FixedSizeList);
        assert_eq!(result.data_type().fixed_size, 5);
        assert_eq!(result.children().len(), 1);
        assert_eq!(
            result.children()[0].data_type().basic_type,
            BasicType::Int32
        );
    }

    #[test]
    fn test_from_arrow_data_type_struct() {
        let field1 = arrow_schema::Field::new("field1", DataType::Int32, false);
        let field2 = arrow_schema::Field::new("field2", DataType::Utf8, false);
        let struct_type = DataType::Struct(arrow_schema::Fields::from(vec![field1, field2]));

        let result = DataTypeBuilder::from_arrow_data_type(&struct_type).unwrap();
        assert_eq!(result.data_type().basic_type, BasicType::Struct);
        assert_eq!(result.children().len(), 2);
        assert_eq!(
            result.children()[0].data_type().basic_type,
            BasicType::Int32
        );
        assert_eq!(result.children()[0].data_type().field_name, "field1");
        assert_eq!(
            result.children()[1].data_type().basic_type,
            BasicType::String
        );
        assert_eq!(result.children()[1].data_type().field_name, "field2");
    }

    #[test]
    fn test_from_arrow_data_type_union() {
        let field1 = arrow_schema::Field::new("field1", DataType::Int32, false);
        let field2 = arrow_schema::Field::new("field2", DataType::Utf8, false);
        let union_type = DataType::Union(
            UnionFields::from_iter(vec![(0, Arc::new(field1)), (1, Arc::new(field2))]),
            arrow_schema::UnionMode::Dense,
        );

        let result = DataTypeBuilder::from_arrow_data_type(&union_type).unwrap();
        assert_eq!(result.data_type().basic_type, BasicType::Union);
        assert_eq!(result.children().len(), 2);
        assert_eq!(
            result.children()[0].data_type().basic_type,
            BasicType::Int32
        );
        assert_eq!(result.children()[0].data_type().field_name, "field1");
        assert_eq!(
            result.children()[1].data_type().basic_type,
            BasicType::String
        );
        assert_eq!(result.children()[1].data_type().field_name, "field2");
    }

    #[test]
    fn test_from_arrow_data_type_dictionary() {
        let value_type = DataType::Utf8;
        let dict_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(value_type.clone()));

        let result = DataTypeBuilder::from_arrow_data_type(&dict_type).unwrap();
        assert_eq!(result.data_type().basic_type, BasicType::String);
    }

    #[test]
    fn test_from_arrow_data_type_map() {
        let key_type = DataType::Int32;
        let value_type = DataType::Utf8;
        let entry_field = arrow_schema::Field::new(
            "entry",
            DataType::Struct(arrow_schema::Fields::from(vec![
                arrow_schema::Field::new("key", key_type.clone(), false),
                arrow_schema::Field::new("value", value_type.clone(), false),
            ])),
            false,
        );
        let map_type = DataType::Map(Arc::new(entry_field), false);

        let result = DataTypeBuilder::from_arrow_data_type(&map_type).unwrap();
        assert_eq!(result.data_type().basic_type, BasicType::Map);
        assert_eq!(result.children().len(), 2);
        assert_eq!(
            result.children()[0].data_type().basic_type,
            BasicType::Int32
        );
        assert_eq!(
            result.children()[1].data_type().basic_type,
            BasicType::String
        );
    }

    #[test]
    fn test_from_arrow_data_type_run_end_encoded() {
        let value_type = DataType::Int32;
        let run_end_encoded_type = DataType::RunEndEncoded(
            Arc::new(arrow_schema::Field::new("", DataType::Int32, false)),
            Arc::new(arrow_schema::Field::new("value", value_type.clone(), false)),
        );

        let result = DataTypeBuilder::from_arrow_data_type(&run_end_encoded_type).unwrap();
        assert_eq!(result.data_type().basic_type, BasicType::Int32);
    }

    #[test]
    fn test_from_arrow_schema() {
        let field1 = arrow_schema::Field::new("field1", DataType::Int32, false);
        let field2 = arrow_schema::Field::new("field2", DataType::Utf8, false);
        let arrow_schema = arrow_schema::Schema::new(vec![field1, field2]);

        let result =
            amudai_format::schema_builder::SchemaBuilder::from_arrow_schema(&arrow_schema).unwrap();
        assert_eq!(result.fields().len(), 2);
        assert_eq!(
            result.fields()[0].data_type().basic_type(),
            BasicType::Int32
        );
        assert_eq!(result.fields()[0].name(), "field1");
        assert_eq!(
            result.fields()[1].data_type().basic_type(),
            BasicType::String
        );
        assert_eq!(result.fields()[1].name(), "field2");
    }

    #[test]
    fn test_from_arrow_schema_empty() {
        let arrow_schema = arrow_schema::Schema::empty();
        let result =
            amudai_format::schema_builder::SchemaBuilder::from_arrow_schema(&arrow_schema).unwrap();
        assert_eq!(result.fields().len(), 0);
    }

    #[test]
    fn test_from_arrow_data_type_nested_struct() {
        let inner_field = arrow_schema::Field::new("inner", DataType::Int32, false);
        let outer_field = arrow_schema::Field::new(
            "outer",
            DataType::Struct(arrow_schema::Fields::from(vec![inner_field])),
            false,
        );
        let outer_struct_type = DataType::Struct(arrow_schema::Fields::from(vec![outer_field]));

        let result = DataTypeBuilder::from_arrow_data_type(&outer_struct_type).unwrap();
        assert_eq!(result.data_type().basic_type, BasicType::Struct);
        assert_eq!(result.children().len(), 1);
        assert_eq!(
            result.children()[0].data_type().basic_type,
            BasicType::Struct
        );
        assert_eq!(result.children()[0].children().len(), 1);
        assert_eq!(
            result.children()[0].children()[0].data_type().basic_type,
            BasicType::Int32
        );
        assert_eq!(
            result.children()[0].children()[0].data_type().field_name,
            "inner"
        );
        assert_eq!(result.children()[0].data_type().field_name, "outer");
    }

    #[test]
    fn test_from_arrow_data_type_nested_list() {
        let inner_type = DataType::Int32;
        let list_type = DataType::List(Arc::new(arrow_schema::Field::new(
            "item", inner_type, false,
        )));
        let outer_list_type =
            DataType::List(Arc::new(arrow_schema::Field::new("list", list_type, false)));

        let result = DataTypeBuilder::from_arrow_data_type(&outer_list_type).unwrap();
        assert_eq!(result.data_type().basic_type, BasicType::List);
        assert_eq!(result.children().len(), 1);
        assert_eq!(result.children()[0].data_type().basic_type, BasicType::List);
        assert_eq!(result.children()[0].children().len(), 1);
        assert_eq!(
            result.children()[0].children()[0].data_type().basic_type,
            BasicType::Int32
        );
    }
}
