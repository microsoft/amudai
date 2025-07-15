//! Schema string parser for command-line table schema definitions

use anyhow::{Context, Result, anyhow};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use std::collections::HashMap;

/// Parse a schema string into an Arrow Schema
///
/// Format: "(field_name1: string, field_name2: long, field_name3: double)"
///
/// Supported types:
/// - string -> Utf8
/// - int, int32, i32 -> Int32
/// - long, int64, i64 -> Int64
/// - double, float64, f64 -> Float64
/// - datetime -> Timestamp(Nanosecond, None)
/// - guid, uuid -> FixedSizeBinary(16) with arrow.uuid extension
///
/// All fields are nullable by default.
pub fn parse_schema_string(schema_str: &str) -> Result<Schema> {
    let schema_str = schema_str.trim();

    // Check for outer parentheses
    if !schema_str.starts_with('(') || !schema_str.ends_with(')') {
        return Err(anyhow!(
            "Schema string must be enclosed in parentheses: (field1: type1, field2: type2, ...)"
        ));
    }

    // Remove outer parentheses
    let inner = &schema_str[1..schema_str.len() - 1].trim();

    if inner.is_empty() {
        return Err(anyhow!("Schema string cannot be empty"));
    }

    // Build type mapping
    let type_map = build_type_mapping();

    // Split by commas and parse each field
    let mut fields = Vec::new();

    for field_def in inner.split(',') {
        let field_def = field_def.trim();
        if field_def.is_empty() {
            continue;
        }

        let field = parse_field_definition(field_def, &type_map)
            .with_context(|| format!("Failed to parse field definition: '{field_def}'"))?;

        fields.push(field);
    }

    if fields.is_empty() {
        return Err(anyhow!("Schema must contain at least one field"));
    }

    Ok(Schema::new(fields))
}

fn parse_field_definition(field_def: &str, type_map: &HashMap<&str, DataType>) -> Result<Field> {
    // Split on the last colon to handle field names that might contain colons
    let colon_pos = field_def
        .rfind(':')
        .ok_or_else(|| anyhow!("Field definition must contain ':' separator"))?;

    let field_name = field_def[..colon_pos].trim();
    let type_name = field_def[colon_pos + 1..].trim();

    if field_name.is_empty() {
        return Err(anyhow!("Field name cannot be empty"));
    }

    if type_name.is_empty() {
        return Err(anyhow!("Field type cannot be empty"));
    }

    // Validate field name (basic validation - no spaces, valid identifier)
    if !is_valid_field_name(field_name) {
        return Err(anyhow!(
            "Invalid field name '{}'. Field names must be valid identifiers (letters, numbers, underscores, no spaces)",
            field_name
        ));
    }

    let data_type = type_map.get(type_name)
        .ok_or_else(|| anyhow!(
            "Unsupported type '{}'. Supported types: string, int/int32/i32, long/int64/i64, double/float64/f64, datetime, guid/uuid",
            type_name
        ))?
        .clone();

    // Create field with metadata for GUID types
    let mut field = Field::new(field_name, data_type.clone(), true);

    // Add UUID extension metadata for GUID types
    if matches!(data_type, DataType::FixedSizeBinary(16))
        && (type_name == "guid" || type_name == "uuid")
    {
        use std::collections::HashMap;
        field.set_metadata(HashMap::from([(
            "ARROW:extension:name".to_string(),
            "arrow.uuid".to_string(),
        )]));
    }

    Ok(field)
}

fn build_type_mapping() -> HashMap<&'static str, DataType> {
    let mut map = HashMap::new();

    // String type
    map.insert("string", DataType::Utf8);

    // Integer types (32-bit)
    map.insert("int", DataType::Int32);
    map.insert("int32", DataType::Int32);
    map.insert("i32", DataType::Int32);

    // Long types (64-bit)
    map.insert("long", DataType::Int64);
    map.insert("int64", DataType::Int64);
    map.insert("i64", DataType::Int64);

    // Double/Float types
    map.insert("double", DataType::Float64);
    map.insert("float64", DataType::Float64);
    map.insert("f64", DataType::Float64);

    // Datetime type
    map.insert("datetime", DataType::Timestamp(TimeUnit::Nanosecond, None));

    // GUID/UUID type
    map.insert("guid", DataType::FixedSizeBinary(16));
    map.insert("uuid", DataType::FixedSizeBinary(16));

    map
}

fn is_valid_field_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }

    // First character must be letter or underscore
    let mut chars = name.chars();
    if let Some(first) = chars.next() {
        if !first.is_ascii_alphabetic() && first != '_' {
            return false;
        }
    }

    // Rest can be letters, numbers, or underscores
    for ch in chars {
        if !ch.is_ascii_alphanumeric() && ch != '_' {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    #[test]
    fn test_parse_simple_schema() {
        let schema_str = "(name: string, age: int, score: double)";
        let schema = parse_schema_string(schema_str).unwrap();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert!(schema.field(0).is_nullable());

        assert_eq!(schema.field(1).name(), "age");
        assert_eq!(schema.field(1).data_type(), &DataType::Int32);

        assert_eq!(schema.field(2).name(), "score");
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
    }

    #[test]
    fn test_parse_with_guid_types() {
        let schema_str = "(id: guid, uuid_field: uuid, name: string)";
        let schema = parse_schema_string(schema_str).unwrap();

        assert_eq!(schema.fields().len(), 3);

        // Check GUID field
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(
            schema.field(0).metadata().get("ARROW:extension:name"),
            Some(&"arrow.uuid".to_string())
        );

        // Check UUID field
        assert_eq!(schema.field(1).name(), "uuid_field");
        assert_eq!(schema.field(1).data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(
            schema.field(1).metadata().get("ARROW:extension:name"),
            Some(&"arrow.uuid".to_string())
        );

        // Check regular string field
        assert_eq!(schema.field(2).name(), "name");
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
        assert!(schema.field(2).metadata().is_empty());
    }

    #[test]
    fn test_parse_with_aliases() {
        let schema_str = "(id: i64, count: int32, value: f64, timestamp: datetime)";
        let schema = parse_schema_string(schema_str).unwrap();

        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Int32);
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
        assert_eq!(
            schema.field(3).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    #[test]
    fn test_parse_with_whitespace() {
        let schema_str = "( name : string , age : int , score : double )";
        let schema = parse_schema_string(schema_str).unwrap();
        assert_eq!(schema.fields().len(), 3);
    }

    #[test]
    fn test_invalid_schema_format() {
        assert!(parse_schema_string("name: string, age: int").is_err()); // Missing parentheses
        assert!(parse_schema_string("(name string, age: int)").is_err()); // Missing colon
        assert!(parse_schema_string("()").is_err()); // Empty schema
        assert!(parse_schema_string("(name: invalid_type)").is_err()); // Invalid type
    }

    #[test]
    fn test_field_name_validation() {
        assert!(parse_schema_string("(valid_name: string)").is_ok());
        assert!(parse_schema_string("(_valid: string)").is_ok());
        assert!(parse_schema_string("(valid123: string)").is_ok());

        assert!(parse_schema_string("(123invalid: string)").is_err()); // Starts with number
        assert!(parse_schema_string("(invalid name: string)").is_err()); // Contains space
        assert!(parse_schema_string("(: string)").is_err()); // Empty name
    }
}
