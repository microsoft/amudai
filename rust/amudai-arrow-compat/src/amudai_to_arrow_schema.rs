//! A module providing traits and implementations for converting Amudai schema components
//! into corresponding structures within the `arrow_schema` crate.
//!
//! This module facilitates interoperability between the Amudai data format definitions
//! and the Apache Arrow schema representation. It defines traits for converting
//! Amudai schemas, fields, and data types, and provides concrete implementations building
//! `arrow_schema` components.

use amudai_common::{error::Error, Result};
use amudai_format::{
    schema::{
        BasicType as AmudaiBasicType, DataType as AmudaiDataType, Field as AmudaiField,
        Schema as AmudaiSchema,
    },
    schema_builder::DataTypeBuilder,
};
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields, Schema as ArrowSchema,
    TimeUnit, UnionMode,
};
use std::{collections::HashMap, sync::Arc};

/// A trait for converting an Amudai data type into an `arrow_schema::DataType`.
pub trait ToArrowDataType {
    /// Converts this Amudai data type into an `arrow_schema::DataType`.
    ///
    /// # Returns
    ///
    /// A `Result` containing the converted `arrow_schema::DataType` or an `Error` if the conversion fails.
    fn to_arrow_data_type(&self) -> Result<ArrowDataType>;
}

/// A trait for converting an Amudai field into an `arrow_schema::Field`.
pub trait ToArrowField {
    /// Converts this Amudai field into an `arrow_schema::Field`.
    ///
    /// # Returns
    ///
    /// A `Result` containing the converted `arrow_schema::Field` or an `Error` if the conversion fails.
    fn to_arrow_field(&self) -> Result<ArrowField>;
}

/// A trait for converting an Amudai schema into an `arrow_schema::Schema`.
pub trait ToArrowSchema {
    /// Converts this Amudai schema into an `arrow_schema::Schema`.
    ///
    /// # Returns
    ///
    /// A `Result` containing the converted `arrow_schema::Schema` or an `Error` if the conversion fails.
    fn to_arrow_schema(&self) -> Result<ArrowSchema>;
}

impl ToArrowDataType for AmudaiDataType {
    fn to_arrow_data_type(&self) -> Result<ArrowDataType> {
        let desc = self.describe()?;
        let extension_label = self.extension_label()?;
        let arrow_dt = match self.basic_type()? {
            AmudaiBasicType::Boolean => ArrowDataType::Boolean,
            AmudaiBasicType::Int8 => {
                if desc.signed {
                    ArrowDataType::Int8
                } else {
                    ArrowDataType::UInt8
                }
            }
            AmudaiBasicType::Int16 => {
                if desc.signed {
                    ArrowDataType::Int16
                } else {
                    ArrowDataType::UInt16
                }
            }
            AmudaiBasicType::Int32 => {
                if desc.signed {
                    ArrowDataType::Int32
                } else {
                    ArrowDataType::UInt32
                }
            }
            AmudaiBasicType::Int64 => {
                if extension_label == Some(DataTypeBuilder::KUSTO_TIMESPAN_EXTENSION_LABEL) {
                    ArrowDataType::Duration(TimeUnit::Nanosecond)
                } else if desc.signed {
                    ArrowDataType::Int64
                } else {
                    ArrowDataType::UInt64
                }
            }
            AmudaiBasicType::Float32 => ArrowDataType::Float32,
            AmudaiBasicType::Float64 => ArrowDataType::Float64,
            AmudaiBasicType::Binary => {
                if extension_label == Some(DataTypeBuilder::KUSTO_DYNAMIC_EXTENSION_LABEL) {
                    ArrowDataType::LargeUtf8
                } else {
                    ArrowDataType::LargeBinary
                }
            }
            AmudaiBasicType::FixedSizeBinary => {
                ArrowDataType::FixedSizeBinary(desc.fixed_size as i32)
            }
            AmudaiBasicType::String => ArrowDataType::LargeUtf8,
            AmudaiBasicType::DateTime => ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            AmudaiBasicType::Guid => ArrowDataType::FixedSizeBinary(16),
            AmudaiBasicType::List => {
                let child_count = self.child_count()?;
                if child_count != 1 {
                    return Err(Error::invalid_arg(
                        "self",
                        "List type must have exactly one child.",
                    ));
                }
                let child_amudai_type = self.child_at(0)?;
                {
                    let child_arrow_type = child_amudai_type.to_arrow_data_type()?;
                    ArrowDataType::LargeList(Arc::new(ArrowField::new(
                        "item",
                        child_arrow_type,
                        true, // Amudai types are nullable by default
                    )))
                }
            }
            AmudaiBasicType::FixedSizeList => {
                let child_count = self.child_count()?;
                if child_count != 1 {
                    return Err(Error::invalid_arg(
                        "self",
                        "FixedSizeList type must have exactly one child.",
                    ));
                }
                let child_amudai_type = self.child_at(0)?;
                {
                    let child_arrow_type = child_amudai_type.to_arrow_data_type()?;
                    ArrowDataType::FixedSizeList(
                        Arc::new(ArrowField::new(
                            "item",
                            child_arrow_type,
                            true, // Amudai types are nullable by default
                        )),
                        desc.fixed_size as i32,
                    )
                }
            }
            AmudaiBasicType::Struct => {
                let mut arrow_fields = Vec::new();
                let child_count = self.child_count()?;
                for i in 0..child_count {
                    let child_amudai_type = self.child_at(i)?;
                    {
                        let child_arrow_type = child_amudai_type.to_arrow_data_type()?;
                        arrow_fields.push(ArrowField::new(
                            child_amudai_type.name()?,
                            child_arrow_type,
                            true, // Amudai types are nullable by default
                        ));
                    }
                }
                ArrowDataType::Struct(ArrowFields::from(arrow_fields))
            }
            AmudaiBasicType::Map => {
                let child_count = self.child_count()?;
                if child_count != 2 {
                    return Err(Error::invalid_arg(
                        "self",
                        "Map type must have exactly two children (key and value).",
                    ));
                }
                let key_amudai_type = self.child_at(0)?;
                let value_amudai_type = self.child_at(1)?;
                {
                    let key_arrow_type = key_amudai_type.to_arrow_data_type()?;
                    let value_arrow_type = value_amudai_type.to_arrow_data_type()?;

                    let entry_struct = ArrowDataType::Struct(ArrowFields::from(vec![
                        ArrowField::new("key", key_arrow_type, false), // Map keys are not nullable
                        ArrowField::new("value", value_arrow_type, true),
                    ]));

                    ArrowDataType::Map(
                        Arc::new(ArrowField::new(
                            "entries",
                            entry_struct,
                            false, // The map field itself (the struct of key-value pairs) is not nullable
                        )),
                        false, // keys_sorted - Amudai spec doesn't specify, assume false
                    )
                }
            }
            AmudaiBasicType::Union => {
                let mut arrow_fields = Vec::new();
                let mut type_ids = Vec::new();
                let child_count = self.child_count()?;
                for i in 0..child_count {
                    let child_amudai_type = self.child_at(i)?;
                    {
                        let child_arrow_type = child_amudai_type.to_arrow_data_type()?;
                        arrow_fields.push(ArrowField::new(
                            child_amudai_type.name()?,
                            child_arrow_type,
                            true, // Amudai types are nullable by default
                        ));
                        type_ids.push(i as i8); // Arrow type_ids are i8
                    }
                }
                ArrowDataType::Union(
                    arrow_schema::UnionFields::new(type_ids, arrow_fields),
                    UnionMode::Dense,
                ) // Defaulting to Dense, Amudai spec doesn't specify
            }
            // AmudaiBasicType::Unit is not directly mapped as it's for null_value in AnyValue,
            // not a schema type. If it appears in a schema context, it's an error.
            AmudaiBasicType::Unit => {
                return Err(Error::invalid_arg(
                    "self",
                    "Amudai Unit type cannot be directly converted to an Arrow schema type.",
                ))
            }
        };
        Ok(arrow_dt)
    }
}

impl ToArrowField for AmudaiField {
    fn to_arrow_field(&self) -> Result<ArrowField> {
        let amudai_data_type = self.data_type()?;
        let extension_label = amudai_data_type.extension_label()?;
        let arrow_data_type = amudai_data_type.to_arrow_data_type()?;
        let mut field = ArrowField::new(
            self.name()?,
            arrow_data_type,
            true, // Amudai types are nullable by default.
        );

        if amudai_data_type.basic_type()? == AmudaiBasicType::Guid {
            field.set_metadata(HashMap::from([(
                "ARROW:extension:name".to_string(),
                "arrow.uuid".to_string(),
            )]));
        } else if let Some(DataTypeBuilder::KUSTO_DYNAMIC_EXTENSION_LABEL) = extension_label {
            field.set_metadata(HashMap::from([(
                "ARROW:extension:name".to_string(),
                "arrow.json".to_string(),
            )]));
        }

        Ok(field)
    }
}

impl ToArrowSchema for AmudaiSchema {
    fn to_arrow_schema(&self) -> Result<ArrowSchema> {
        let field_list = self.field_list()?;
        let len = field_list.len()?;
        let mut fields = Vec::with_capacity(len);
        for i in 0..len {
            let amudai_field = self.field_at(i)?;
            fields.push(amudai_field.to_arrow_field()?);
        }
        Ok(ArrowSchema::new(fields))
    }
}

#[cfg(test)]
mod tests {
    use amudai_format::schema::BasicType as AmudaiBasicType;
    use amudai_format::schema_builder::{DataTypeBuilder, FieldBuilder, SchemaBuilder};
    use arrow_schema::{
        DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields,
        Schema as ArrowSchema, TimeUnit, UnionMode,
    };
    use std::sync::Arc;

    use crate::amudai_to_arrow_schema::ToArrowSchema;

    // Helper function to create an Amudai schema from a list of FieldBuilders
    // and convert it to an ArrowSchema.
    fn build_and_convert_schema(
        fields: Vec<FieldBuilder>,
    ) -> Result<ArrowSchema, amudai_common::error::Error> {
        let amudai_schema = SchemaBuilder::new(fields).finish_and_seal();
        let amudai_schema_view = amudai_schema.schema()?;
        amudai_schema_view.to_arrow_schema()
    }

    #[test]
    fn test_empty_schema() {
        let arrow_schema = build_and_convert_schema(vec![]).unwrap();
        assert_eq!(arrow_schema.fields.len(), 0);
    }

    #[test]
    fn test_boolean_type() {
        let fields = vec![FieldBuilder::new(
            "bool_field",
            AmudaiBasicType::Boolean,
            None,
            None,
        )];
        let arrow_schema = build_and_convert_schema(fields).unwrap();
        assert_eq!(arrow_schema.fields.len(), 1);
        let arrow_field = &arrow_schema.fields[0];
        assert_eq!(arrow_field.name(), "bool_field");
        assert_eq!(arrow_field.data_type(), &ArrowDataType::Boolean);
        assert!(arrow_field.is_nullable());
    }

    #[test]
    fn test_int8_type() {
        let fields_signed = vec![FieldBuilder::new(
            "i8_field",
            AmudaiBasicType::Int8,
            Some(true),
            None,
        )];
        let arrow_schema_signed = build_and_convert_schema(fields_signed).unwrap();
        assert_eq!(
            arrow_schema_signed.fields[0].data_type(),
            &ArrowDataType::Int8
        );

        let fields_unsigned = vec![FieldBuilder::new(
            "u8_field",
            AmudaiBasicType::Int8,
            Some(false),
            None,
        )];
        let arrow_schema_unsigned = build_and_convert_schema(fields_unsigned).unwrap();
        assert_eq!(
            arrow_schema_unsigned.fields[0].data_type(),
            &ArrowDataType::UInt8
        );
    }

    #[test]
    fn test_int16_type() {
        let fields_signed = vec![FieldBuilder::new(
            "i16_field",
            AmudaiBasicType::Int16,
            Some(true),
            None,
        )];
        let arrow_schema_signed = build_and_convert_schema(fields_signed).unwrap();
        assert_eq!(
            arrow_schema_signed.fields[0].data_type(),
            &ArrowDataType::Int16
        );

        let fields_unsigned = vec![FieldBuilder::new(
            "u16_field",
            AmudaiBasicType::Int16,
            Some(false),
            None,
        )];
        let arrow_schema_unsigned = build_and_convert_schema(fields_unsigned).unwrap();
        assert_eq!(
            arrow_schema_unsigned.fields[0].data_type(),
            &ArrowDataType::UInt16
        );
    }

    #[test]
    fn test_int32_type() {
        let fields_signed = vec![FieldBuilder::new(
            "i32_field",
            AmudaiBasicType::Int32,
            Some(true),
            None,
        )];
        let arrow_schema_signed = build_and_convert_schema(fields_signed).unwrap();
        assert_eq!(
            arrow_schema_signed.fields[0].data_type(),
            &ArrowDataType::Int32
        );

        let fields_unsigned = vec![FieldBuilder::new(
            "u32_field",
            AmudaiBasicType::Int32,
            Some(false),
            None,
        )];
        let arrow_schema_unsigned = build_and_convert_schema(fields_unsigned).unwrap();
        assert_eq!(
            arrow_schema_unsigned.fields[0].data_type(),
            &ArrowDataType::UInt32
        );
    }

    #[test]
    fn test_int64_type() {
        let fields_signed = vec![FieldBuilder::new(
            "i64_field",
            AmudaiBasicType::Int64,
            Some(true),
            None,
        )];
        let arrow_schema_signed = build_and_convert_schema(fields_signed).unwrap();
        assert_eq!(
            arrow_schema_signed.fields[0].data_type(),
            &ArrowDataType::Int64
        );

        let fields_unsigned = vec![FieldBuilder::new(
            "u64_field",
            AmudaiBasicType::Int64,
            Some(false),
            None,
        )];
        let arrow_schema_unsigned = build_and_convert_schema(fields_unsigned).unwrap();
        assert_eq!(
            arrow_schema_unsigned.fields[0].data_type(),
            &ArrowDataType::UInt64
        );
    }

    #[test]
    fn test_float32_type() {
        let fields = vec![FieldBuilder::new(
            "f32_field",
            AmudaiBasicType::Float32,
            None,
            None,
        )];
        let arrow_schema = build_and_convert_schema(fields).unwrap();
        assert_eq!(arrow_schema.fields[0].data_type(), &ArrowDataType::Float32);
    }

    #[test]
    fn test_float64_type() {
        let fields = vec![FieldBuilder::new(
            "f64_field",
            AmudaiBasicType::Float64,
            None,
            None,
        )];
        let arrow_schema = build_and_convert_schema(fields).unwrap();
        assert_eq!(arrow_schema.fields[0].data_type(), &ArrowDataType::Float64);
    }

    #[test]
    fn test_binary_type() {
        let fields = vec![FieldBuilder::new(
            "bin_field",
            AmudaiBasicType::Binary,
            None,
            None,
        )];
        let arrow_schema = build_and_convert_schema(fields).unwrap();
        assert_eq!(
            arrow_schema.fields[0].data_type(),
            &ArrowDataType::LargeBinary
        );
    }

    #[test]
    fn test_string_type() {
        let fields = vec![FieldBuilder::new(
            "str_field",
            AmudaiBasicType::String,
            None,
            None,
        )];
        let arrow_schema = build_and_convert_schema(fields).unwrap();
        assert_eq!(
            arrow_schema.fields[0].data_type(),
            &ArrowDataType::LargeUtf8
        );
    }

    #[test]
    fn test_datetime_type() {
        let fields = vec![FieldBuilder::new(
            "dt_field",
            AmudaiBasicType::DateTime,
            None,
            None,
        )];
        let arrow_schema = build_and_convert_schema(fields).unwrap();
        assert_eq!(
            arrow_schema.fields[0].data_type(),
            &ArrowDataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    #[test]
    fn test_guid_type() {
        let fields = vec![FieldBuilder::new(
            "guid_field",
            AmudaiBasicType::Guid,
            None,
            None,
        )];
        let arrow_schema = build_and_convert_schema(fields).unwrap();
        assert_eq!(
            arrow_schema.fields[0].data_type(),
            &ArrowDataType::FixedSizeBinary(16)
        );
    }

    #[test]
    fn test_fixed_size_binary_type() {
        let fields = vec![FieldBuilder::new(
            "fsb_field",
            AmudaiBasicType::FixedSizeBinary,
            None,
            Some(10),
        )];
        let arrow_schema = build_and_convert_schema(fields).unwrap();
        assert_eq!(
            arrow_schema.fields[0].data_type(),
            &ArrowDataType::FixedSizeBinary(10)
        );
    }

    #[test]
    fn test_list_type() {
        let mut list_field = FieldBuilder::new("list_field", AmudaiBasicType::List, None, None);
        list_field.add_child(DataTypeBuilder::new(
            "item",
            AmudaiBasicType::Int32,
            Some(true),
            None,
            vec![],
        ));
        let fields = vec![list_field];
        let arrow_schema = build_and_convert_schema(fields).unwrap();

        let expected_arrow_field = ArrowField::new("item", ArrowDataType::Int32, true);
        assert_eq!(
            arrow_schema.fields[0].data_type(),
            &ArrowDataType::LargeList(Arc::new(expected_arrow_field))
        );
    }

    #[test]
    fn test_list_type_unnamed_child() {
        let mut list_field =
            FieldBuilder::new("list_field_unnamed", AmudaiBasicType::List, None, None);
        list_field.add_child(DataTypeBuilder::new(
            "",
            AmudaiBasicType::String,
            None,
            None,
            vec![],
        )); // Unnamed child
        let fields = vec![list_field];
        let arrow_schema = build_and_convert_schema(fields).unwrap();

        let expected_arrow_field = ArrowField::new(
            "item", // Default name for unnamed list items
            ArrowDataType::LargeUtf8,
            true,
        );
        assert_eq!(arrow_schema.fields[0].name(), "list_field_unnamed");
        assert_eq!(
            arrow_schema.fields[0].data_type(),
            &ArrowDataType::LargeList(Arc::new(expected_arrow_field))
        );
    }

    #[test]
    fn test_fixed_size_list_type() {
        let mut fsl_field =
            FieldBuilder::new("fsl_field", AmudaiBasicType::FixedSizeList, None, Some(5));
        fsl_field.add_child(DataTypeBuilder::new(
            "element",
            AmudaiBasicType::Float64,
            None,
            None,
            vec![],
        ));
        let fields = vec![fsl_field];
        let arrow_schema = build_and_convert_schema(fields).unwrap();

        let expected_child_field = ArrowField::new("item", ArrowDataType::Float64, true);
        assert_eq!(
            arrow_schema.fields[0].data_type(),
            &ArrowDataType::FixedSizeList(Arc::new(expected_child_field), 5)
        );
    }

    #[test]
    fn test_struct_type() {
        let mut struct_field =
            FieldBuilder::new("struct_field", AmudaiBasicType::Struct, None, None);
        struct_field.add_child(DataTypeBuilder::new(
            "s_bool",
            AmudaiBasicType::Boolean,
            None,
            None,
            vec![],
        ));
        struct_field.add_child(DataTypeBuilder::new(
            "s_i64",
            AmudaiBasicType::Int64,
            Some(true),
            None,
            vec![],
        ));
        struct_field.add_child(DataTypeBuilder::new(
            "s_str",
            AmudaiBasicType::String,
            None,
            None,
            vec![],
        ));
        let fields = vec![struct_field];
        let arrow_schema = build_and_convert_schema(fields).unwrap();

        let expected_arrow_fields = ArrowFields::from(vec![
            ArrowField::new("s_bool", ArrowDataType::Boolean, true),
            ArrowField::new("s_i64", ArrowDataType::Int64, true),
            ArrowField::new("s_str", ArrowDataType::LargeUtf8, true),
        ]);
        assert_eq!(
            arrow_schema.fields[0].data_type(),
            &ArrowDataType::Struct(expected_arrow_fields)
        );
    }

    #[test]
    fn test_map_type() {
        let mut map_field = FieldBuilder::new("map_field", AmudaiBasicType::Map, None, None);
        map_field.add_child(DataTypeBuilder::new(
            "key",
            AmudaiBasicType::String,
            None,
            None,
            vec![],
        )); // Key
        map_field.add_child(DataTypeBuilder::new(
            "value",
            AmudaiBasicType::Int32,
            Some(true),
            None,
            vec![],
        )); // Value
        let fields = vec![map_field];
        let arrow_schema = build_and_convert_schema(fields).unwrap();

        let key_field = ArrowField::new("key", ArrowDataType::LargeUtf8, false); // Map keys are not nullable
        let value_field = ArrowField::new("value", ArrowDataType::Int32, true);
        let struct_field = ArrowField::new(
            "entries", // Default name for map entries
            ArrowDataType::Struct(ArrowFields::from(vec![key_field, value_field])),
            false, // The map field itself (the struct of key-value pairs) is not nullable
        );

        assert_eq!(
            arrow_schema.fields[0].data_type(),
            &ArrowDataType::Map(Arc::new(struct_field), false)
        );
    }

    #[test]
    fn test_map_type_unnamed_children() {
        let mut map_field =
            FieldBuilder::new("map_field_unnamed", AmudaiBasicType::Map, None, None);
        map_field.add_child(DataTypeBuilder::new(
            "",
            AmudaiBasicType::Int64,
            Some(true),
            None,
            vec![],
        )); // Unnamed Key
        map_field.add_child(DataTypeBuilder::new(
            "",
            AmudaiBasicType::Boolean,
            None,
            None,
            vec![],
        )); // Unnamed Value
        let fields = vec![map_field];
        let arrow_schema = build_and_convert_schema(fields).unwrap();

        let key_field = ArrowField::new("key", ArrowDataType::Int64, false);
        let value_field = ArrowField::new("value", ArrowDataType::Boolean, true);
        let struct_field = ArrowField::new(
            "entries",
            ArrowDataType::Struct(ArrowFields::from(vec![key_field, value_field])),
            false,
        );
        assert_eq!(arrow_schema.fields[0].name(), "map_field_unnamed");
        assert_eq!(
            arrow_schema.fields[0].data_type(),
            &ArrowDataType::Map(Arc::new(struct_field), false)
        );
    }

    #[test]
    fn test_union_type() {
        let mut union_field = FieldBuilder::new("union_field", AmudaiBasicType::Union, None, None);
        union_field.add_child(DataTypeBuilder::new(
            "var_int",
            AmudaiBasicType::Int32,
            Some(true),
            None,
            vec![],
        ));
        union_field.add_child(DataTypeBuilder::new(
            "var_str",
            AmudaiBasicType::String,
            None,
            None,
            vec![],
        ));
        let fields = vec![union_field];
        let arrow_schema = build_and_convert_schema(fields).unwrap();

        let expected_union_fields = vec![
            ArrowField::new("var_int", ArrowDataType::Int32, true),
            ArrowField::new("var_str", ArrowDataType::LargeUtf8, true),
        ];
        let expected_type_ids = vec![0i8, 1i8];
        let union_fields_obj =
            arrow_schema::UnionFields::new(expected_type_ids, expected_union_fields);

        assert_eq!(
            arrow_schema.fields[0].data_type(),
            &ArrowDataType::Union(union_fields_obj, UnionMode::Dense)
        );
    }

    #[test]
    fn test_nested_struct_with_list() {
        let mut inner_struct =
            DataTypeBuilder::new("inner_struct", AmudaiBasicType::Struct, None, None, vec![]);
        inner_struct.add_child(DataTypeBuilder::new(
            "text_val",
            AmudaiBasicType::String,
            None,
            None,
            vec![],
        ));

        let mut list_of_structs =
            DataTypeBuilder::new("list_items", AmudaiBasicType::List, None, None, vec![]);
        list_of_structs.add_child(inner_struct);

        let mut outer_struct_field =
            FieldBuilder::new("outer_struct", AmudaiBasicType::Struct, None, None);
        outer_struct_field.add_child(DataTypeBuilder::new(
            "id_val",
            AmudaiBasicType::Int64,
            Some(true),
            None,
            vec![],
        ));
        outer_struct_field.add_child(list_of_structs);

        let fields = vec![outer_struct_field];
        let arrow_schema = build_and_convert_schema(fields).unwrap();

        // Expected Arrow Schema for inner_struct
        let inner_arrow_struct_fields = ArrowFields::from(vec![ArrowField::new(
            "text_val",
            ArrowDataType::LargeUtf8,
            true,
        )]);
        let inner_arrow_struct_type = ArrowDataType::Struct(inner_arrow_struct_fields);

        // Expected Arrow Schema for list_of_structs
        let list_item_arrow_field = ArrowField::new("item", inner_arrow_struct_type, true);
        let list_arrow_type = ArrowDataType::LargeList(Arc::new(list_item_arrow_field));

        // Expected Arrow Schema for outer_struct
        let outer_arrow_struct_fields = ArrowFields::from(vec![
            ArrowField::new("id_val", ArrowDataType::Int64, true),
            ArrowField::new("list_items", list_arrow_type, true),
        ]);
        let expected_outer_struct_type = ArrowDataType::Struct(outer_arrow_struct_fields);

        assert_eq!(arrow_schema.fields.len(), 1);
        assert_eq!(arrow_schema.fields[0].name(), "outer_struct");
        assert_eq!(
            arrow_schema.fields[0].data_type(),
            &expected_outer_struct_type
        );
    }

    #[test]
    fn test_schema_with_multiple_fields() {
        let fields = vec![
            FieldBuilder::new("f1_bool", AmudaiBasicType::Boolean, None, None),
            FieldBuilder::new("f2_i32", AmudaiBasicType::Int32, Some(true), None),
            FieldBuilder::new("f3_str", AmudaiBasicType::String, None, None),
        ];
        let arrow_schema = build_and_convert_schema(fields).unwrap();
        assert_eq!(arrow_schema.fields.len(), 3);
        assert_eq!(arrow_schema.fields[0].name(), "f1_bool");
        assert_eq!(arrow_schema.fields[0].data_type(), &ArrowDataType::Boolean);
        assert_eq!(arrow_schema.fields[1].name(), "f2_i32");
        assert_eq!(arrow_schema.fields[1].data_type(), &ArrowDataType::Int32);
        assert_eq!(arrow_schema.fields[2].name(), "f3_str");
        assert_eq!(
            arrow_schema.fields[2].data_type(),
            &ArrowDataType::LargeUtf8
        );
    }

    #[test]
    fn test_list_with_unnamed_child_item_name() {
        let mut list_field = FieldBuilder::new("my_list", AmudaiBasicType::List, None, None);
        // Child DataTypeBuilder has an empty field_name
        list_field.add_child(DataTypeBuilder::new(
            "",
            AmudaiBasicType::Int64,
            Some(true),
            None,
            vec![],
        ));
        let fields = vec![list_field];
        let arrow_schema = build_and_convert_schema(fields).unwrap();

        let list_arrow_field = &arrow_schema.fields[0];
        assert_eq!(list_arrow_field.name(), "my_list");
        if let ArrowDataType::LargeList(item_field_arc) = list_arrow_field.data_type() {
            assert_eq!(item_field_arc.name(), "item"); // Default name for unnamed list items
            assert_eq!(item_field_arc.data_type(), &ArrowDataType::Int64);
            assert!(item_field_arc.is_nullable());
        } else {
            panic!("Expected LargeList type");
        }
    }

    #[test]
    fn test_map_with_unnamed_key_value_names() {
        let mut map_field = FieldBuilder::new("my_map", AmudaiBasicType::Map, None, None);
        // Key and Value DataTypeBuilders have empty field_names
        map_field.add_child(DataTypeBuilder::new(
            "",
            AmudaiBasicType::String,
            None,
            None,
            vec![],
        ));
        map_field.add_child(DataTypeBuilder::new(
            "",
            AmudaiBasicType::Boolean,
            None,
            None,
            vec![],
        ));
        let fields = vec![map_field];
        let arrow_schema = build_and_convert_schema(fields).unwrap();

        let map_arrow_field = &arrow_schema.fields[0];
        assert_eq!(map_arrow_field.name(), "my_map");
        if let ArrowDataType::Map(entry_field_arc, _keys_sorted) = map_arrow_field.data_type() {
            assert_eq!(entry_field_arc.name(), "entries"); // Default name for map entries struct
            if let ArrowDataType::Struct(kv_fields) = entry_field_arc.data_type() {
                assert_eq!(kv_fields.len(), 2);
                assert_eq!(kv_fields[0].name(), "key"); // Default name for key
                assert_eq!(kv_fields[0].data_type(), &ArrowDataType::LargeUtf8);
                assert!(!kv_fields[0].is_nullable()); // Keys are not nullable

                assert_eq!(kv_fields[1].name(), "value"); // Default name for value
                assert_eq!(kv_fields[1].data_type(), &ArrowDataType::Boolean);
                assert!(kv_fields[1].is_nullable());
            } else {
                panic!("Expected Struct type for map entries");
            }
        } else {
            panic!("Expected Map type");
        }
    }
}
