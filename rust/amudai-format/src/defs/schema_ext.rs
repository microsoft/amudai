use amudai_bytes::Bytes;
use amudai_common::{error::Error, result::Result};

use super::schema::BasicType;

/// Represents the known extension types supported by the Amudai format.
///
/// Extension types serve as semantic annotations on basic types, enhancing their meaning
/// and refining the set of applicable operations without changing the underlying storage
/// representation. They are particularly important for format evolution and extensibility,
/// as new extension types can be added without disrupting existing readers.
///
/// The distinction between basic types and extension types allows Amudai to:
/// - Maintain backward compatibility when introducing new semantic types
/// - Provide hints for more suitable data encoding and indexing
/// - Bridge compatibility with systems like Kusto while using standardized storage formats
///
/// # Extension Type Concept
///
/// An extension type can be thought of as an annotation that:
/// - Enhances semantics of the underlying basic type
/// - Refines the set of applicable operations (such as scalar functions)  
/// - Limits the range of permissible values
/// - Provides encoding/indexing hints to the storage layer
///
/// For example, a `KustoTimeSpan` is stored as an `i64` (basic type) representing
/// 100-nanosecond ticks, but the extension type annotation tells query engines
/// that this should be interpreted as a time duration with specific operations available.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KnownExtendedType {
    /// No extension type is applied. The field uses only its basic type semantics.
    #[default]
    None,

    /// Kusto decimal type with 128-bit precision.
    ///
    /// This represents a decimal number with high precision, stored as a
    /// `FixedSizeBinary<16>` basic type using the DecNumber128 format
    /// (IEEE 754-2019 decimal128 floating-point format).
    ///
    /// When accessed through Arrow interoperability, this may be converted
    /// to a string representation for compatibility.
    KustoDecimal,

    /// Kusto time span (duration) type.
    ///
    /// Represents a duration or time interval, stored as an `i64` basic type
    /// containing 100-nanosecond ticks. This provides high precision timing
    /// information while maintaining efficient storage.
    ///
    /// When accessed through Arrow interoperability, this is converted to
    /// Arrow's `Duration(nanosecond)` type.
    KustoTimeSpan,

    /// Kusto dynamic type for semi-structured data.
    ///
    /// Represents dynamically-typed values that can contain complex nested
    /// structures, arrays, objects, or primitive values. The data is stored
    /// as a `Binary` basic type containing a compact encoded representation
    /// equivalent to Kusto's current dynamic encoding format.
    ///
    /// This type is essential for semi-structured data scenarios where the
    /// exact schema is not known at ingestion time or varies between records.
    ///
    /// When accessed through Arrow interoperability, this is converted to
    /// a JSON string with `{ "ARROW:extension:name": "arrow.json" }` metadata.
    KustoDynamic,

    /// Represents an unknown or unrecognized extension type.
    ///
    /// This variant is used when parsing extension type labels that are not
    /// among the known types. It allows the system to handle forward compatibility
    /// gracefully when encountering newer extension types that weren't known
    /// when this code was compiled.
    Other,
}

impl KnownExtendedType {
    /// String label for the Kusto decimal extension type.
    ///
    /// This constant defines the canonical string representation used to identify
    /// the `KustoDecimal` extension type in serialized metadata and when parsing
    /// extension type labels from external sources.
    pub const KUSTO_DECIMAL_LABEL: &'static str = "KustoDecimal";

    /// String label for the Kusto time span extension type.
    ///
    /// This constant defines the canonical string representation used to identify
    /// the `KustoTimeSpan` extension type in serialized metadata and when parsing
    /// extension type labels from external sources.
    pub const KUSTO_TIMESPAN_LABEL: &'static str = "KustoTimeSpan";

    /// String label for the Kusto dynamic extension type.
    ///
    /// This constant defines the canonical string representation used to identify
    /// the `KustoDynamic` extension type in serialized metadata and when parsing
    /// extension type labels from external sources.
    pub const KUSTO_DYNAMIC_LABEL: &'static str = "KustoDynamic";

    /// Returns the canonical string representation of this extension type.
    ///
    /// This method provides the standard string label used to identify the extension
    /// type in serialized formats, metadata, and external interfaces. The returned
    /// string matches the constants defined for each known type.
    ///
    /// # Returns
    ///
    /// - `Ok("")` for `None` - indicates no extension type is applied
    /// - `Ok("KustoDecimal")` for `KustoDecimal`
    /// - `Ok("KustoTimeSpan")` for `KustoTimeSpan`  
    /// - `Ok("KustoDynamic")` for `KustoDynamic`
    /// - `Err(...)` for `Other` - unknown extension types cannot be converted to strings
    ///
    /// # Errors
    ///
    /// Returns an error for `KnownExtendedType::Other` since unknown extension types
    /// don't have a defined string representation in this enumeration.
    pub fn as_str(&self) -> amudai_common::Result<&'static str> {
        match self {
            KnownExtendedType::None => Ok(""),
            KnownExtendedType::KustoDecimal => Ok(Self::KUSTO_DECIMAL_LABEL),
            KnownExtendedType::KustoTimeSpan => Ok(Self::KUSTO_TIMESPAN_LABEL),
            KnownExtendedType::KustoDynamic => Ok(Self::KUSTO_DYNAMIC_LABEL),
            KnownExtendedType::Other => Err(Error::invalid_operation(
                "as_str for KnownExtendedType::Other",
            )),
        }
    }
}

/// Implements string parsing for `KnownExtendedType`.
///
/// This implementation allows converting string labels into their corresponding
/// `KnownExtendedType` variants. It's primarily used when deserializing extension
/// type information from metadata or when parsing extension type labels from
/// external sources.
///
/// # Parsing Rules
///
/// - `"KustoDecimal"` → `KnownExtendedType::KustoDecimal`
/// - `"KustoTimeSpan"` → `KnownExtendedType::KustoTimeSpan`
/// - `"KustoDynamic"` → `KnownExtendedType::KustoDynamic`
/// - `""` (empty string) → `KnownExtendedType::None`
/// - Any other string → `KnownExtendedType::Other`
///
/// Note that unknown extension types are mapped to `Other` rather than producing
/// an error, which provides forward compatibility when encountering newer extension
/// types that weren't known when this code was compiled.
impl std::str::FromStr for KnownExtendedType {
    type Err = amudai_common::error::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            Self::KUSTO_DECIMAL_LABEL => Ok(Self::KustoDecimal),
            Self::KUSTO_TIMESPAN_LABEL => Ok(Self::KustoTimeSpan),
            Self::KUSTO_DYNAMIC_LABEL => Ok(Self::KustoDynamic),
            "" => Ok(Self::None),
            _ => Ok(Self::Other),
        }
    }
}

impl BasicType {
    /// Returns `true` if the type is a composite (container) type.
    ///
    /// Composite types include:
    /// - `List`: Variable-length list of values of the same type
    /// - `FixedSizeList`: Fixed-length list of values of the same type
    /// - `Struct`: Collection of named fields
    /// - `Map`: Key-value pairs
    /// - `Union`: Allows values of different types
    pub fn is_composite(&self) -> bool {
        matches!(
            self,
            BasicType::List
                | BasicType::FixedSizeList
                | BasicType::Struct
                | BasicType::Map
                | BasicType::Union
        )
    }

    /// Returns the maximum allowed number of child `DataType` nodes for this type.
    ///
    /// This indicates how many child types can be associated with this type in the
    /// schema:
    /// - Primitive types: 0 (they don't have child types)
    /// - `List` and `FixedSizeList`: 1 (the element type)
    /// - `Map`: 2 (key and value types)
    /// - `Struct` and `Union`: up to 1000000
    pub fn max_children(&self) -> usize {
        match self {
            BasicType::Unit
            | BasicType::Boolean
            | BasicType::Int8
            | BasicType::Int16
            | BasicType::Int32
            | BasicType::Int64
            | BasicType::Float32
            | BasicType::Float64
            | BasicType::Binary
            | BasicType::FixedSizeBinary
            | BasicType::String
            | BasicType::Guid
            | BasicType::DateTime => 0,
            BasicType::List => 1,
            BasicType::FixedSizeList => 1,
            BasicType::Struct => 1_000_000,
            BasicType::Map => 2,
            BasicType::Union => 1_000_000,
        }
    }

    /// Returns `true` if children of this type must have names.
    ///
    /// Currently, only `Struct` types require their children to be named.
    /// Each field in a struct must have a name that identifies it.
    pub fn requires_named_children(&self) -> bool {
        matches!(self, BasicType::Struct)
    }

    /// Returns `true` if children of this type can have names.
    ///
    /// Both `Struct` and `Union` types allow named children:
    /// - For `Struct`: Names are required for all fields
    /// - For `Union`: Names are optional but can be used to identify different
    ///   variants
    pub fn allows_named_children(&self) -> bool {
        matches!(self, BasicType::Struct | BasicType::Union)
    }

    /// Returns `true` if the value sequence for this type requires offset encoding.
    ///
    /// Types that require offsets have variable-length values or collections:
    /// - `Binary`: Variable-length byte arrays
    /// - `String`: Variable-length UTF-8 encoded strings
    /// - `List`: Variable-length collections
    /// - `Map`: Key-value collections
    pub fn requires_offsets(&self) -> bool {
        matches!(
            self,
            BasicType::Binary | BasicType::String | BasicType::List | BasicType::Map
        )
    }

    /// Returns `true` if this is one of the integer types (i8, i16, i32, or i64).
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            BasicType::Int8 | BasicType::Int16 | BasicType::Int32 | BasicType::Int64
        )
    }
    /// Returns `true` if this is one of the integer-valued types (integer or DateTime).
    pub fn is_integer_or_datetime(&self) -> bool {
        self.is_integer() || *self == BasicType::DateTime
    }

    /// Returns `true` if this is one of the floating-point types (f32 or f64).
    pub fn is_float(&self) -> bool {
        matches!(self, BasicType::Float32 | BasicType::Float64)
    }
}

/// Describes a basic data type with its essential characteristics for storage and encoding.
///
/// `BasicTypeDescriptor` is a compact, self-contained description of a data type that
/// encapsulates all the information needed by encoders, decoders, and statistics collectors
/// to process values of that type. For nested `DataType` nodes, this struct defines the
/// fundamental type of the node itself (e.g., `Struct`, `List`, `Map`, etc.), without
/// considering any child nodes.
///
/// # Role in Amudai
///
/// This struct serves as a bridge between the schema system and the data processing
/// layers:
/// - **Encoders** use it to select appropriate encoding strategies and parameters
/// - **Decoders** use it to reconstruct original values from encoded blocks
/// - **Statistics collectors** use it to determine which statistics to gather and how
/// - **Arrow compatibility** uses it to map between Amudai and Arrow type systems
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BasicTypeDescriptor {
    /// The underlying physical type that determines storage layout and encoding behavior.
    ///
    /// This field specifies the fundamental data type (e.g., `Int32`, `String`, `List`)
    /// and is the primary factor in determining:
    /// - How values are stored in memory buffers
    /// - Which block encoders and decoders to use
    /// - Whether the type requires offset arrays for variable-length data
    /// - The maximum number of child types allowed
    ///
    /// See [`BasicType`] for the complete list of supported types.
    pub basic_type: BasicType,

    /// The fixed size for types that have a predetermined size, measured in the appropriate units.
    ///
    /// This field is meaningful only for specific basic types:
    /// - **`FixedSizeBinary`**: Size in bytes of each binary value (e.g., 16 for UUIDs)
    /// - **`FixedSizeList`**: Number of elements in each list value
    /// - **All other types**: Must be 0 (ignored)
    ///
    /// # Examples
    /// - GUID: `basic_type = FixedSizeBinary`, `fixed_size = 16`
    /// - 3D Point: `basic_type = FixedSizeList`, `fixed_size = 3` (with Float64 child)
    /// - String: `basic_type = String`, `fixed_size = 0` (variable-length)
    pub fixed_size: u32,

    /// Indicates whether integer types use signed representation.
    ///
    /// This field determines the interpretation of integer values.
    ///
    /// # Valid Combinations
    /// - **`true`**: Only valid for `Int8`, `Int16`, `Int32`, `Int64`
    /// - **`false`**: Valid for all integer types (treated as unsigned) and required
    ///   for all non-integer types
    ///
    /// # Examples
    /// - Signed 64-bit integer: `basic_type = Int64`, `signed = true` → Arrow `Int64`
    /// - Unsigned 32-bit integer: `basic_type = Int32`, `signed = false` → Arrow `UInt32`
    /// - String: `basic_type = String`, `signed = false` (always false for non-integers)
    pub signed: bool,

    /// Optional semantic extension type that enhances the basic type with additional meaning.
    ///
    /// Extension types provide semantic annotations without changing the underlying storage
    /// format, enabling:
    /// - **Forward compatibility**: New extension types can be added without breaking existing code
    /// - **Semantic precision**: Same storage type can represent different logical concepts
    /// - **Query optimization**: Engines can apply type-specific optimizations
    /// - **System interoperability**: Bridge between different type systems (e.g., Kusto ↔ Arrow)
    ///
    /// # Important Notes
    /// - This is a **best-effort** compact representation of the extension type
    /// - Unknown or complex extension types are represented as [`KnownExtendedType::Other`]
    /// - For `Other` cases, consult the full `DataType` node for complete extension information
    /// - When `None`, the field uses only its basic type semantics
    ///
    /// # Examples
    /// - Kusto decimal: `basic_type = FixedSizeBinary(16)`, `extended_type = KustoDecimal`
    /// - Kusto timespan: `basic_type = Int64`, `extended_type = KustoTimeSpan`
    /// - Regular string: `basic_type = String`, `extended_type = None`
    pub extended_type: KnownExtendedType,
}

impl BasicTypeDescriptor {
    /// Returns the fixed size in bytes for primitive types, or `None` for variable-length
    /// or composite types.
    ///
    /// This method is essential for memory layout calculations, buffer allocation, and
    /// determining whether a type requires offset arrays for indexing.
    ///
    /// # Return Values
    ///
    /// ## Fixed-Size Primitives
    /// - **`Int8`**: `Some(1)` - 8-bit integers
    /// - **`Int16`**: `Some(2)` - 16-bit integers  
    /// - **`Int32`**: `Some(4)` - 32-bit integers
    /// - **`Int64`**: `Some(8)` - 64-bit integers and DateTime
    /// - **`Float32`**: `Some(4)` - 32-bit floating point
    /// - **`Float64`**: `Some(8)` - 64-bit floating point
    /// - **`FixedSizeBinary`**: `Some(fixed_size)` - Fixed-length binary data
    /// - **`Guid`**: `Some(16)` - 128-bit globally unique identifiers
    /// - **`DateTime`**: `Some(8)` - 64-bit timestamp values
    ///
    /// ## Variable-Length and Special Types
    /// - **`Boolean`**: `None` - Bit-packed, size depends on count
    /// - **`Binary`**: `None` - Variable-length byte arrays
    /// - **`String`**: `None` - Variable-length UTF-8 strings
    ///
    /// ## Composite Types
    /// - **`List`**: `None` - Variable-length collections
    /// - **`FixedSizeList`**: `None` - Fixed-count collections (size depends on element type)
    /// - **`Struct`**: `None` - Composite records with multiple fields
    /// - **`Map`**: `None` - Key-value collections
    /// - **`Union`**: `None` - Discriminated unions
    pub fn primitive_size(&self) -> Option<usize> {
        match self.basic_type {
            BasicType::Unit | BasicType::Boolean => None,
            BasicType::Int8 => Some(1),
            BasicType::Int16 => Some(2),
            BasicType::Int32 => Some(4),
            BasicType::Int64 => Some(8),
            BasicType::Float32 => Some(4),
            BasicType::Float64 => Some(8),
            BasicType::Binary => None,
            BasicType::FixedSizeBinary => Some(self.fixed_size as usize),
            BasicType::String => None,
            BasicType::Guid => Some(16),
            BasicType::DateTime => Some(8),
            BasicType::List
            | BasicType::FixedSizeList
            | BasicType::Struct
            | BasicType::Map
            | BasicType::Union => None,
        }
    }
}

/// Provides a default `BasicTypeDescriptor` representing a unit type.
///
/// The default instance represents a conceptual "unit" or "void" type that carries
/// no data and is primarily used as a placeholder or in generic contexts where
/// a type descriptor is required but no actual data is present.
///
/// # Default Values
/// - `basic_type`: [`BasicType::Unit`] - No data storage
/// - `fixed_size`: `0` - Not applicable for unit type
/// - `signed`: `false` - Not applicable for unit type  
/// - `extended_type`: [`KnownExtendedType::None`] - No extension semantics
impl Default for BasicTypeDescriptor {
    fn default() -> Self {
        Self {
            basic_type: BasicType::Unit,
            fixed_size: 0,
            signed: false,
            extended_type: KnownExtendedType::None,
        }
    }
}

pub(crate) struct OwnedTableRef<RefType: details::TableRefType>(RefType::T<'static>, Bytes);

impl<RefType: details::TableRefType> OwnedTableRef<RefType> {
    pub fn new_as_root(buf: Bytes) -> Result<OwnedTableRef<RefType>>
    where
        for<'a> RefType::T<'a>: planus::ReadAsRoot<'a>,
    {
        use planus::ReadAsRoot;
        let inner = RefType::T::read_as_root(&buf)?;
        Ok(OwnedTableRef(
            #[allow(clippy::unnecessary_cast)]
            unsafe { &*(&inner as *const RefType::T<'_> as *const RefType::T<'static>) }.clone(),
            Self::clone_buf(&buf),
        ))
    }

    pub unsafe fn wrap(buf: Bytes, inner: RefType::T<'_>) -> OwnedTableRef<RefType> {
        OwnedTableRef(
            #[allow(clippy::unnecessary_cast)]
            unsafe {
                (*(&inner as *const RefType::T<'_> as *const RefType::T<'static>)).clone()
            },
            Self::clone_buf(&buf),
        )
    }

    pub fn get<'a>(&'a self) -> RefType::T<'a> {
        #[allow(clippy::unnecessary_cast)]
        unsafe { &*(&self.0 as *const RefType::T<'_> as *const RefType::T<'a>) }.clone()
    }

    pub fn map<MappedType: details::TableRefType, F>(
        &self,
        f: F,
    ) -> Result<OwnedTableRef<MappedType>>
    where
        F: for<'a> FnOnce(RefType::T<'a>, &'a ()) -> Result<MappedType::T<'a>>,
    {
        let res = f(self.0.clone(), &())?;
        Ok(unsafe { OwnedTableRef::wrap(Self::clone_buf(&self.1), res) })
    }

    fn clone_buf(src: &Bytes) -> Bytes {
        let buf = src.clone();
        assert_eq!(src.as_ptr(), buf.as_ptr());
        buf
    }
}

impl<RefType: details::TableRefType> Clone for OwnedTableRef<RefType> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), Self::clone_buf(&self.1))
    }
}

pub(crate) type OwnedSchemaRef = OwnedTableRef<crate::defs::schema::Schema>;

pub(crate) type OwnedFieldRef = OwnedTableRef<crate::defs::schema::Field>;

pub(crate) type OwnedDataTypeRef = OwnedTableRef<crate::defs::schema::DataType>;

mod details {
    pub trait TableRefType {
        type T<'a>: Clone;
    }

    impl TableRefType for crate::defs::schema::Schema {
        type T<'a> = crate::defs::schema::SchemaRef<'a>;
    }

    impl TableRefType for crate::defs::schema::Field {
        type T<'a> = crate::defs::schema::FieldRef<'a>;
    }

    impl TableRefType for crate::defs::schema::DataType {
        type T<'a> = crate::defs::schema::DataTypeRef<'a>;
    }
}

#[cfg(test)]
mod tests {
    use super::KnownExtendedType;
    use crate::defs::schema::BasicType;
    use std::str::FromStr;

    #[test]
    fn test_known_extended_type_from_str() {
        // Test known types
        assert_eq!(
            KnownExtendedType::from_str("KustoDecimal").unwrap(),
            KnownExtendedType::KustoDecimal
        );
        assert_eq!(
            KnownExtendedType::from_str("KustoTimeSpan").unwrap(),
            KnownExtendedType::KustoTimeSpan
        );
        assert_eq!(
            KnownExtendedType::from_str("KustoDynamic").unwrap(),
            KnownExtendedType::KustoDynamic
        );

        // Test empty string -> None
        assert_eq!(
            KnownExtendedType::from_str("").unwrap(),
            KnownExtendedType::None
        );

        // Test unknown types -> Other
        assert_eq!(
            KnownExtendedType::from_str("SomeUnknownType").unwrap(),
            KnownExtendedType::Other
        );
        assert_eq!(
            KnownExtendedType::from_str("CustomType").unwrap(),
            KnownExtendedType::Other
        );
    }

    #[test]
    fn test_is_float() {
        // Floating-point types should return true
        assert!(BasicType::Float32.is_float());
        assert!(BasicType::Float64.is_float());

        // All other types should return false
        assert!(!BasicType::Unit.is_float());
        assert!(!BasicType::Boolean.is_float());
        assert!(!BasicType::Int8.is_float());
        assert!(!BasicType::Int16.is_float());
        assert!(!BasicType::Int32.is_float());
        assert!(!BasicType::Int64.is_float());
        assert!(!BasicType::Binary.is_float());
        assert!(!BasicType::FixedSizeBinary.is_float());
        assert!(!BasicType::String.is_float());
        assert!(!BasicType::Guid.is_float());
        assert!(!BasicType::DateTime.is_float());
        assert!(!BasicType::List.is_float());
        assert!(!BasicType::FixedSizeList.is_float());
        assert!(!BasicType::Struct.is_float());
        assert!(!BasicType::Map.is_float());
        assert!(!BasicType::Union.is_float());
    }
}
