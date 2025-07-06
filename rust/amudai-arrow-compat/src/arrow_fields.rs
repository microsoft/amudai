//! Creation of Arrow Field definitions suitable for direct ingestion
//! into Amudai shards with minimal data conversions.
//!
//! This module provides factory functions for creating Arrow fields that are optimized
//! for direct ingestion into Amudai shards. The fields created here may differ from
//! the "canonical" commonly used Arrow fields and data types, as they are specifically
//! designed to minimize data conversions during the ingestion process.
//!
//! # Key Features
//!
//! - **Direct Ingestion**: Fields are optimized for minimal conversion overhead
//! - **Extension Type Support**: Provides utilities for adding Amudai-specific extension types
//! - **Nullable Fields**: All fields are created as nullable by default for flexibility
//! - **Large Types**: Uses `LargeBinary` and `LargeUtf8` to support large data frames.

use amudai_format::defs::schema_ext::KnownExtendedType;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;

/// A trait for adding Amudai-specific extension type annotations to Arrow fields.
///
/// This trait provides a convenient way to attach [`KnownExtendedType`] annotations
/// to Arrow fields, enabling better semantic understanding of the data and optimized
/// processing within the Amudai ecosystem.
///
/// Extension types enhance the basic Arrow data types with additional semantic meaning
/// without changing the underlying storage representation. For example, a `KustoDecimal`
/// is stored as `FixedSizeBinary(16)` but carries additional metadata indicating it
/// should be interpreted as a high-precision decimal number.
pub trait ArrowFieldKnownExtType {
    /// Attaches a known extension type to this Arrow field.
    ///
    /// This method adds the appropriate extension type metadata to the field,
    /// allowing downstream consumers to understand the semantic meaning of the data.
    ///
    /// # Arguments
    ///
    /// * `extended_type` - The [`KnownExtendedType`] to attach to this field
    ///
    /// # Returns
    ///
    /// The field with the extension type metadata applied
    fn with_known_extended_type(self, extended_type: KnownExtendedType) -> Self;
}

impl ArrowFieldKnownExtType for ArrowField {
    fn with_known_extended_type(mut self, ty: KnownExtendedType) -> Self {
        set_extension_type_name(&mut self, ty.as_str().expect("name"));
        self
    }
}

/// Creates a Boolean field.
pub fn make_boolean(name: &str) -> ArrowField {
    ArrowField::new(name, ArrowDataType::Boolean, true)
}

/// Creates a signed 8-bit integer field.
pub fn make_int8(name: &str) -> ArrowField {
    ArrowField::new(name, ArrowDataType::Int8, true)
}

/// Creates a signed 16-bit integer field.
pub fn make_int16(name: &str) -> ArrowField {
    ArrowField::new(name, ArrowDataType::Int16, true)
}

/// Creates a signed 32-bit integer field.
pub fn make_int32(name: &str) -> ArrowField {
    ArrowField::new(name, ArrowDataType::Int32, true)
}

/// Creates a signed 64-bit integer field optimized for direct Amudai ingestion.
pub fn make_int64(name: &str) -> ArrowField {
    ArrowField::new(name, ArrowDataType::Int64, true)
}

/// Creates an unsigned 8-bit integer field optimized for direct Amudai ingestion.
pub fn make_uint8(name: &str) -> ArrowField {
    ArrowField::new(name, ArrowDataType::UInt8, true)
}

/// Creates an unsigned 16-bit integer field optimized for direct Amudai ingestion.
pub fn make_uint16(name: &str) -> ArrowField {
    ArrowField::new(name, ArrowDataType::UInt16, true)
}

/// Creates an unsigned 32-bit integer field optimized for direct Amudai ingestion.
pub fn make_uint32(name: &str) -> ArrowField {
    ArrowField::new(name, ArrowDataType::UInt32, true)
}

/// Creates an unsigned 64-bit integer field optimized for direct Amudai ingestion.
pub fn make_uint64(name: &str) -> ArrowField {
    ArrowField::new(name, ArrowDataType::UInt64, true)
}

/// Creates a 32-bit floating-point field optimized for direct Amudai ingestion.
pub fn make_float32(name: &str) -> ArrowField {
    ArrowField::new(name, ArrowDataType::Float32, true)
}

/// Creates a 64-bit floating-point field optimized for direct Amudai ingestion.
pub fn make_float64(name: &str) -> ArrowField {
    ArrowField::new(name, ArrowDataType::Float64, true)
}

/// Creates a binary field.
///
/// This function creates an Arrow field with the [`ArrowDataType::LargeBinary`] type.
pub fn make_binary(name: &str) -> ArrowField {
    ArrowField::new(name, ArrowDataType::LargeBinary, true)
}

/// Creates a fixed-size binary field.
pub fn make_fixed_size_binary(name: &str, size: i32) -> ArrowField {
    ArrowField::new(name, ArrowDataType::FixedSizeBinary(size), true)
}

/// Creates a string field.
///
/// This function creates an Arrow field with the [`ArrowDataType::LargeUtf8`] type.
pub fn make_string(name: &str) -> ArrowField {
    ArrowField::new(name, ArrowDataType::LargeUtf8, true)
}

/// Creates a GUID/UUID field.
///
/// This function creates an Arrow field with the `ArrowDataType::FixedSizeBinary(16)`
/// type and adds the standard Arrow UUID extension type metadata.
pub fn make_guid(name: &str) -> ArrowField {
    let mut field = ArrowField::new(name, ArrowDataType::FixedSizeBinary(16), true);
    set_extension_type_name(&mut field, "arrow.uuid");
    field
}

/// Creates a DateTime field.
///
/// This function creates an Arrow field with the [`ArrowDataType::UInt64`] type
/// and adds Kusto DateTime extension type metadata. The field stores datetime values
/// as 64-bit unsigned integers representing 100-nanosecond ticks since the .NET epoch
/// (0001-01-01T00:00:00Z).
///
/// This representation is optimized for compatibility with Kusto's datetime format
/// and provides high precision for temporal data storage and operations.
pub fn make_datetime(name: &str) -> ArrowField {
    let mut field = ArrowField::new(name, ArrowDataType::UInt64, true);
    set_extension_type_name(&mut field, KnownExtendedType::KUSTO_DATETIME_LABEL);
    field
}

/// Creates a KustoDecimal field.
///
/// This function creates an Arrow field with the `ArrowDataType::FixedSizeBinary(16)` type
/// and adds the KustoDecimal extension type metadata. This field stores high-precision
/// decimal values using the DecNumber128 format (IEEE 754-2019 decimal128 floating-point).
pub fn make_decimal(name: &str) -> ArrowField {
    ArrowField::new(name, ArrowDataType::FixedSizeBinary(16), true)
        .with_known_extended_type(KnownExtendedType::KustoDecimal)
}

/// Creates a `KustoTimeSpan` field.
///
/// This function creates an Arrow field with the [`ArrowDataType::Int64`] type
/// and adds the KustoTimeSpan extension type metadata. The field stores time span
/// values as 64-bit signed integers representing 100-nanosecond ticks.
///
/// This representation is compatible with Kusto's timespan format and provides
/// high precision for duration measurements and time interval calculations.
pub fn make_timespan(name: &str) -> ArrowField {
    ArrowField::new(name, ArrowDataType::Int64, true)
        .with_known_extended_type(KnownExtendedType::KustoTimeSpan)
}

/// Creates a `KustoDynamic` field.
///
/// This function creates an Arrow field with the [`ArrowDataType::LargeBinary`] type
/// and adds the KustoDynamic extension type metadata. The field stores semi-structured
/// data as binary-encoded values compatible with Kusto's dynamic type encoding.
pub fn make_dynamic(name: &str) -> ArrowField {
    ArrowField::new(name, ArrowDataType::LargeBinary, true)
        .with_known_extended_type(KnownExtendedType::KustoDynamic)
}

/// Sets the extension type name metadata on an Arrow field.
///
/// This function adds or updates the extension type name metadata on an Arrow field,
/// which is used by Arrow consumers to understand the semantic meaning of the data
/// beyond its basic storage type. The extension type name is stored in the field's
/// metadata using the standard Arrow extension type key.
///
/// This function will clone the existing metadata and add the extension type name,
/// preserving any other metadata that may already be present on the field.
pub fn set_extension_type_name(field: &mut ArrowField, name: impl Into<String>) {
    let mut metadata = field.metadata().clone();
    metadata.insert(
        arrow_schema::extension::EXTENSION_TYPE_NAME_KEY.to_string(),
        name.into(),
    );
    field.set_metadata(metadata);
}
