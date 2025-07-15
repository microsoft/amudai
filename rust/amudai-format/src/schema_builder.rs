use std::{borrow::Borrow, hash::Hash, str::FromStr};

use ahash::AHashMap;

use crate::{
    checksum,
    defs::{
        self, hash_lookup_ext::hash_field_name, schema::HashLookup, schema_ext::KnownExtendedType,
    },
    schema::{BasicType, BasicTypeDescriptor, SchemaId, SchemaMessage},
};

/// A builder for creating a schema with fields.
#[derive(Default)]
pub struct SchemaBuilder {
    /// A list of field builders for the schema.
    fields: Vec<FieldBuilder>,
    /// A map from field names to their indices in the `fields` vector.
    field_map: AHashMap<String, usize>,
}

impl SchemaBuilder {
    /// Creates a new `SchemaBuilder`, initially empty.
    pub fn new(fields: Vec<FieldBuilder>) -> SchemaBuilder {
        let mut builder = SchemaBuilder {
            fields: Default::default(),
            field_map: Default::default(),
        };
        for field in fields {
            builder.add_field(field);
        }
        builder
    }

    /// Adds a field to the schema.
    ///
    /// # Panics
    ///
    /// Panics if the field name is empty or if a field with the same name already exists.
    pub fn add_field(&mut self, field: FieldBuilder) {
        assert!(!field.name().is_empty());

        assert!(!self.field_map.contains_key(field.name()));
        self.field_map
            .insert(field.name().to_string(), self.fields.len());
        self.fields.push(field);
    }

    /// Returns a slice of the fields in the schema.
    pub fn fields(&self) -> &[FieldBuilder] {
        &self.fields
    }

    /// Returns a mutable slice of the fields in the schema.
    pub fn fields_mut(&mut self) -> &mut [FieldBuilder] {
        &mut self.fields
    }

    /// Finds a field by name and returns a reference to it.
    pub fn find_field<Q>(&self, name: &Q) -> Option<&FieldBuilder>
    where
        Q: Hash + Eq + ?Sized,
        String: Borrow<Q>,
    {
        self.field_map.get(name).map(|&i| &self.fields[i])
    }

    /// Finds a field by name and returns a mutable reference to it.
    pub fn find_field_mut<Q>(&mut self, name: &Q) -> Option<&mut FieldBuilder>
    where
        Q: Hash + Eq + ?Sized,
        String: Borrow<Q>,
    {
        self.field_map.get(name).map(|&i| &mut self.fields[i])
    }

    /// Assigns schema IDs to the fields in the schema.
    /// Returns the next available schema id.
    pub fn assign_schema_ids(&mut self) -> SchemaId {
        let mut next = SchemaId::zero();
        for field in &mut self.fields {
            next = field.data_type_mut().assign_schema_ids(next);
        }
        next
    }

    /// Finishes building the schema and returns it.
    pub fn finish(mut self) -> defs::schema::Schema {
        let next_schema_id = self.assign_schema_ids();

        let fields = self
            .fields
            .into_iter()
            .map(FieldBuilder::finish)
            .collect::<Vec<_>>();

        let field_lookup = HashLookup::build(fields.len(), |i| {
            hash_field_name(&fields[i].data_type.field_name)
        });

        defs::schema::Schema {
            fields,
            schema_id_count: next_schema_id.as_u32(),
            field_lookup: Some(Box::new(field_lookup)),
        }
    }

    /// Finishes building the schema and returns it as a sealed `Bytes` object.
    pub fn finish_and_seal(self) -> SchemaMessage {
        let schema = self.finish();
        Self::create_schema_message(&schema)
    }

    /// Creates a valid (length-prefixed and checksummed) schema message
    /// from the given schema.
    pub fn create_schema_message(schema: &defs::schema::Schema) -> SchemaMessage {
        let mut builder = planus::Builder::new();
        let fbs = builder.finish(schema, None);
        SchemaMessage::new(checksum::create_message_vec(fbs).into())
            .expect("valid new schema message")
    }
}

/// A builder for creating a field in a schema.
///
/// `FieldBuilder` provides a fluent interface for constructing schema fields with
/// their associated data types. It wraps a `DataTypeBuilder` and allows for additional
/// field-specific metadata like internal annotations.
#[derive(Debug, Clone)]
pub struct FieldBuilder {
    data_type: DataTypeBuilder,
    internal_kind: Option<String>,
}

impl FieldBuilder {
    /// Creates a new `FieldBuilder` with the specified parameters.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field (can be empty and set later)
    /// * `basic_type` - The basic data type of the field
    /// * `signed` - Whether the field is signed (for numeric types)
    /// * `fixed_size` - The fixed size in bytes or number of elements
    ///   (for fixed-size types)
    pub fn new(
        field_name: impl Into<String>,
        basic_type: BasicType,
        signed: impl Into<Option<bool>>,
        fixed_size: impl Into<Option<u64>>,
    ) -> FieldBuilder {
        FieldBuilder {
            data_type: DataTypeBuilder::new(field_name, basic_type, signed, fixed_size, vec![]),
            internal_kind: None,
        }
    }

    /// Creates a new string field builder.
    ///
    /// This is a convenience method for creating a field with `BasicType::String`.
    /// The field name is initially empty and should be set using `with_name()`.
    pub fn new_str() -> FieldBuilder {
        FieldBuilder::new("", BasicType::String, None, None)
    }

    /// Creates a new 64-bit signed integer field builder.
    ///
    /// This is a convenience method for creating a field with `BasicType::Int64`.
    /// The field name is initially empty and should be set using `with_name()`.
    pub fn new_i64() -> FieldBuilder {
        FieldBuilder::new("", BasicType::Int64, true, None)
    }

    /// Creates a new list field builder.
    ///
    /// This is a convenience method for creating a field with `BasicType::List`.
    /// The field name is initially empty and should be set using `with_name()`.
    /// A single child element should be added using `add_child()`.
    pub fn new_list() -> FieldBuilder {
        FieldBuilder::new("", BasicType::List, None, None)
    }

    /// Creates a new struct field builder.
    ///
    /// This is a convenience method for creating a field with `BasicType::Struct`.
    /// The field name is initially empty and should be set using `with_name()`.
    /// Child fields should be added using `add_child()`.
    pub fn new_struct() -> FieldBuilder {
        FieldBuilder::new("", BasicType::Struct, None, None)
    }

    /// Sets the field name and returns the modified builder.
    ///
    /// This method allows for fluent method chaining when building fields.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name to assign to this field
    pub fn with_name(mut self, field_name: impl Into<String>) -> FieldBuilder {
        self.data_type = self.data_type.with_field_name(field_name);
        self
    }

    /// Returns the name of the field.
    pub fn name(&self) -> &str {
        self.data_type.field_name()
    }

    /// Returns the basic type of the field.
    pub fn basic_type(&self) -> BasicType {
        self.data_type.basic_type()
    }

    /// Returns the fixed size of the field, if applicable.
    ///
    /// Returns `None` for variable-size types and `Some(size)` for fixed-size types.
    pub fn fixed_size(&self) -> Option<usize> {
        self.data_type.fixed_size()
    }

    /// Returns whether the field is signed (for numeric types).
    pub fn is_signed(&self) -> bool {
        self.data_type.is_signed()
    }

    /// Returns a reference to the underlying data type builder.
    pub fn data_type(&self) -> &DataTypeBuilder {
        &self.data_type
    }

    /// Returns a mutable reference to the underlying data type builder.
    ///
    /// This allows for direct modification of the data type configuration.
    pub fn data_type_mut(&mut self) -> &mut DataTypeBuilder {
        &mut self.data_type
    }

    /// Sets the internal kind annotation for the field.
    ///
    /// # Arguments
    ///
    /// * `kind` - The internal kind annotation
    pub fn set_internal_kind(&mut self, kind: impl Into<String>) {
        self.internal_kind = Some(kind.into());
    }

    /// Sets the extended type annotation for the field.
    ///
    /// Extended type annotations provide additional semantic information about
    /// the field's data type beyond the basic type system.
    ///
    /// # Arguments
    ///
    /// * `label` - The extended type label
    pub fn set_extended_type(&mut self, label: impl Into<String>) {
        self.data_type.set_extended_type(label);
    }

    /// Adds a child data type to this field.
    ///
    /// This is used for composite types like structs and lists that contain
    /// other data types as children.
    ///
    /// # Arguments
    ///
    /// * `child` - The child data type to add
    ///
    /// # Panics
    ///
    /// Panics if the field's basic type doesn't support children.
    pub fn add_child(&mut self, child: DataTypeBuilder) {
        self.data_type.add_child(child);
    }

    /// Returns a slice of the child data types.
    pub fn children(&self) -> &[DataTypeBuilder] {
        self.data_type.children()
    }

    /// Returns a mutable slice of the child data types.
    pub fn children_mut(&mut self) -> &mut [DataTypeBuilder] {
        self.data_type.children_mut()
    }

    /// Finds a child data type by name.
    ///
    /// Returns `None` if no child with the given name exists.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the child to find
    pub fn find_child<Q>(&self, name: &Q) -> Option<&DataTypeBuilder>
    where
        Q: Hash + Eq + ?Sized,
        String: Borrow<Q>,
    {
        self.data_type.find_child(name)
    }

    /// Finds a child data type by name and returns a mutable reference.
    ///
    /// Returns `None` if no child with the given name exists.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the child to find
    pub fn find_child_mut<Q>(&mut self, name: &Q) -> Option<&mut DataTypeBuilder>
    where
        Q: Hash + Eq + ?Sized,
        String: Borrow<Q>,
    {
        self.data_type.find_child_mut(name)
    }

    /// Finishes building the field and returns the completed field definition.
    ///
    /// This consumes the builder and produces a `defs::schema::Field` that can
    /// be used in the final schema.
    pub fn finish(self) -> defs::schema::Field {
        defs::schema::Field {
            data_type: Box::new(self.data_type.finish()),
            internal_field_annotation: self
                .internal_kind
                .map(|kind| Box::new(defs::schema::InternalFieldAnnotation { kind })),
        }
    }
}

impl From<DataTypeBuilder> for FieldBuilder {
    fn from(data_type: DataTypeBuilder) -> Self {
        FieldBuilder {
            data_type,
            internal_kind: None,
        }
    }
}

/// A builder for creating data type definitions in a schema.
///
/// `DataTypeBuilder` provides a fluent interface for constructing data types with
/// their associated metadata, child types, and extended type annotations. It serves
/// as the foundation for building complex schema structures including primitives,
/// composites (structs, lists), and specialized types (timestamps, decimals, etc.).
///
/// # Child Management
///
/// For composite types (structs, lists, unions), the builder manages child data types:
///
/// - Child types are stored in order of addition
/// - Named children are indexed by name for fast lookup
/// - Child validation ensures type system constraints are met
/// - Large numbers of named children automatically get hash-based lookup tables
///
/// # Schema ID Assignment
///
/// During schema finalization, each data type is assigned a unique schema ID that
/// is used for efficient field lookups and serialization. Schema IDs are assigned
/// in depth-first order during the build process.
#[derive(Debug, Clone)]
pub struct DataTypeBuilder {
    /// The partial, in-progress data type definition.
    data_type: defs::schema::DataType,
    /// A list of child data type builders.
    children: Vec<DataTypeBuilder>,
    /// A map from child field names to their indices in the `children` vector.
    child_map: AHashMap<String, usize>,
}

impl DataTypeBuilder {
    /// Creates a new `DataTypeBuilder` with the specified parameters.
    ///
    /// This is the primary constructor for creating data type builders. It accepts
    /// all the basic parameters needed to define a data type and optionally includes
    /// child types for composite data types.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field (can be empty and set later with `with_field_name()`)
    /// * `basic_type` - The fundamental data type (e.g., `BasicType::String`, `BasicType::Int64`)
    /// * `signed` - Whether numeric types are signed (ignored for non-numeric types)
    /// * `fixed_size` - Size in bytes for fixed-size types (0 for variable-size types)
    /// * `children` - Child data type builders for composite types (structs, lists, etc.)
    ///
    /// # Panics
    ///
    /// This method will panic if:
    /// - The children vector contains types incompatible with the basic type
    /// - Required child constraints are violated (e.g., named children for structs)
    pub fn new(
        field_name: impl Into<String>,
        basic_type: BasicType,
        signed: impl Into<Option<bool>>,
        fixed_size: impl Into<Option<u64>>,
        children: Vec<DataTypeBuilder>,
    ) -> DataTypeBuilder {
        let mut builder = DataTypeBuilder {
            data_type: defs::schema::DataType {
                basic_type,
                schema_id: SchemaId::invalid().as_u32(),
                field_name: field_name.into(),
                field_aliases: None,
                children: Default::default(),
                signed: signed.into().unwrap_or(false),
                fixed_size: fixed_size.into().unwrap_or(0),
                extended_type: None,
                lookup: None,
            },
            children: Default::default(),
            child_map: Default::default(),
        };
        for child in children {
            builder.add_child(child);
        }
        builder
    }

    /// Creates a new string data type builder.
    ///
    /// This is a convenience method for creating a variable-length string field.
    /// The field name is initially empty and should be set using `with_field_name()`.
    pub fn new_str() -> DataTypeBuilder {
        DataTypeBuilder::new("", BasicType::String, false, 0, vec![])
    }

    /// Creates a new 64-bit signed integer data type builder.
    ///
    /// This is a convenience method for creating a signed 64-bit integer field.
    /// The field name is initially empty and should be set using `with_field_name()`.
    pub fn new_i64() -> DataTypeBuilder {
        DataTypeBuilder::new("", BasicType::Int64, true, 0, vec![])
    }

    /// Creates a new binary data type builder.
    ///
    /// This is a convenience method for creating a variable-length binary field
    /// that can store arbitrary byte sequences. The field name is initially empty
    /// and should be set using `with_field_name()`.
    pub fn new_binary() -> DataTypeBuilder {
        DataTypeBuilder::new("", BasicType::Binary, false, 0, vec![])
    }

    /// Creates a new GUID (Globally Unique Identifier) data type builder.
    ///
    /// This is a convenience method for creating a fixed-size 16-byte field that
    /// stores UUIDs/GUIDs. The field name is initially empty and should be set
    /// using `with_field_name()`.
    pub fn new_guid() -> DataTypeBuilder {
        DataTypeBuilder::new("", BasicType::Guid, false, 16, vec![])
    }

    /// Creates a new timespan data type builder.
    ///
    /// This is a convenience method for creating a timespan field that represents
    /// a duration of time. Internally, timespans are stored as signed 64-bit integers
    /// with extended type metadata to indicate the timespan semantic.
    ///
    /// The field name is initially empty and should be set using `with_field_name()`.
    ///
    /// # Storage Format
    ///
    /// Timespans are stored as 64-bit signed integers representing time units
    /// (100-nanoseconds ticks) with the extended type annotation
    /// `KnownExtendedType::KUSTO_TIMESPAN_LABEL` to preserve semantic meaning.
    ///
    /// # Compatibility
    ///
    /// This format is compatible with Kusto/Azure Data Explorer timespan types.
    pub fn new_timespan() -> DataTypeBuilder {
        let mut dt = DataTypeBuilder::new("", BasicType::Int64, true, 0, vec![]);
        dt.set_extended_type(KnownExtendedType::KUSTO_TIMESPAN_LABEL);
        dt
    }

    /// Creates a new datetime data type builder.
    ///
    /// This is a convenience method for creating a datetime field that represents
    /// a specific point in time. The field name is initially empty and should be
    /// set using `with_field_name()`.
    pub fn new_datetime() -> DataTypeBuilder {
        DataTypeBuilder::new("", BasicType::DateTime, false, 0, vec![])
    }

    /// Creates a new decimal data type builder.
    ///
    /// This is a convenience method for creating a decimal field that represents
    /// high-precision decimal numbers. Internally, decimals are stored as 16-byte
    /// fixed-size binary values with extended type metadata to indicate the decimal
    /// semantic.
    ///
    /// The field name is initially empty and should be set using `with_field_name()`.
    ///
    /// # Storage Format
    ///
    /// Decimals are stored as 16-byte binary values with the extended type annotation
    /// `KnownExtendedType::KUSTO_DECIMAL_LABEL` to preserve semantic meaning and enable
    /// proper decimal arithmetic.
    ///
    /// This field stores high-precision decimal values using the DecNumber128 format
    /// (IEEE 754-2019 decimal128 floating-point).
    ///
    /// # Compatibility
    ///
    /// This format is compatible with Kusto/Azure Data Explorer decimal types.
    pub fn new_decimal() -> DataTypeBuilder {
        let mut dt = DataTypeBuilder::new("", BasicType::FixedSizeBinary, false, 16, vec![]);
        dt.set_extended_type(KnownExtendedType::KUSTO_DECIMAL_LABEL);
        dt
    }

    /// Creates a new dynamic data type builder.
    ///
    /// This is a convenience method for creating a dynamic field that can store
    /// arbitrary JSON-like data structures. Internally, dynamic values are stored
    /// as variable-length binary data with extended type metadata to indicate the
    /// dynamic semantic.
    ///
    /// The field name is initially empty and should be set using `with_field_name()`.
    ///
    /// # Storage Format
    ///
    /// Dynamic values are stored as variable-length binary data with the extended
    /// type annotation `KnownExtendedType::KUSTO_DYNAMIC_LABEL` to preserve
    /// semantic meaning and enable proper dynamic value handling.
    ///
    /// # Use Cases
    ///
    /// Dynamic fields are useful for:
    /// - Storing JSON-like nested data structures
    /// - Handling schema-less or semi-structured data
    /// - Storing configuration or metadata objects
    /// - Accommodating varying data structures within the same field
    ///
    /// # Compatibility
    ///
    /// This format is compatible with Kusto/Azure Data Explorer dynamic types.
    pub fn new_dynamic() -> DataTypeBuilder {
        let mut dt = DataTypeBuilder::new("", BasicType::Binary, false, 0, vec![]);
        dt.set_extended_type(KnownExtendedType::KUSTO_DYNAMIC_LABEL);
        dt
    }

    /// Sets the field name and returns the modified builder for method chaining.
    ///
    /// This method allows for fluent method chaining when building data types.
    /// The field name is used to identify the field within its parent container
    /// and for serialization/deserialization operations.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name to assign to this field
    pub fn with_field_name(mut self, field_name: impl Into<String>) -> DataTypeBuilder {
        self.data_type.field_name = field_name.into();
        self
    }

    /// Sets the extended type annotation and returns the modified builder for method chaining.
    ///
    /// Extended type annotations provide additional semantic information about
    /// the field's data type beyond the basic type system. This is useful for
    /// specialized data types like timestamps, decimals, or custom application-specific
    /// types.
    ///
    /// # Arguments
    ///
    /// * `label` - The extended type label to assign
    ///
    /// # Common Extended Types
    ///
    /// Some common extended type labels include:
    /// - `"KustoTimeSpan"` - For timespan/duration values
    /// - `"KustoDecimal"` - For high-precision decimal values
    /// - `"KustoDynamic"` - For dynamic/JSON-like values
    /// - Custom application-specific labels
    pub fn with_extended_type(mut self, label: impl Into<String>) -> DataTypeBuilder {
        self.data_type
            .extended_type
            .get_or_insert_with(|| {
                Box::new(defs::schema::ExtendedTypeAnnotation {
                    ..Default::default()
                })
            })
            .label = label.into();
        self
    }

    /// Returns the field name of this data type.
    ///
    /// The field name is used to identify the field within its parent container
    /// and for serialization/deserialization operations.
    ///
    /// # Returns
    ///
    /// A string slice containing the field name, or an empty string if no name
    /// has been set.
    pub fn field_name(&self) -> &str {
        &self.data_type.field_name
    }

    /// Returns the schema ID assigned to this data type.
    ///
    /// Schema IDs are unique identifiers assigned to each data type within a schema
    /// during the finalization process. They are used for efficient field lookups
    /// and serialization operations.
    ///
    /// # Returns
    ///
    /// A `SchemaId` that uniquely identifies this data type within the schema.
    /// Returns an invalid ID if schema IDs have not been assigned yet.
    ///
    /// # Note
    ///
    /// Schema IDs are typically assigned during schema finalization and should
    /// not be relied upon during the building process.
    pub fn schema_id(&self) -> SchemaId {
        self.data_type.schema_id.into()
    }

    /// Returns the basic type of this data type.
    ///
    /// The basic type determines the fundamental storage format and operations
    /// available for values of this data type.
    ///
    /// # Returns
    ///
    /// The `BasicType` enum value indicating the fundamental data type.
    pub fn basic_type(&self) -> BasicType {
        self.data_type.basic_type
    }

    /// Returns a reference to the underlying data type definition.
    ///
    /// This method provides access to the internal `defs::schema::DataType` structure
    /// that contains the raw schema definition. This is useful for low-level operations
    /// or when interfacing with other parts of the system that expect the raw format.
    ///
    /// # Returns
    ///
    /// A reference to the underlying `defs::schema::DataType` structure.
    ///
    /// # Note
    ///
    /// This method exposes internal implementation details and should be used
    /// with caution. Prefer using the higher-level accessor methods when possible.
    pub fn data_type(&self) -> &defs::schema::DataType {
        &self.data_type
    }

    /// Returns the fixed size of this data type, if applicable.
    ///
    /// For fixed-size data types, this returns the size of the value in bytes
    /// or in the number of elements. For variable-size data types, this returns `None`.
    ///
    /// # Returns
    ///
    /// - `Some(size)` - The size in bytes (for `FixedSizeBinary` type) or in elements
    ///   (for `FixedSizeList`` type)
    /// - `None` - For variable-size types
    ///
    /// # Fixed-Size Types
    ///
    /// Common fixed-size types include:
    /// - GUID: 16 bytes
    /// - Decimal: 16 bytes
    /// - Fixed-size binary: specified size
    /// - Fixed-size lists: specified element count
    pub fn fixed_size(&self) -> Option<usize> {
        let size = self.data_type.fixed_size;
        (size != 0).then_some(size as usize)
    }

    /// Returns whether this data type represents signed numeric values.
    ///
    /// This property is meaningful for numeric data types and determines whether
    /// the type can represent negative values.
    ///
    /// # Returns
    ///
    /// - `true` - For signed numeric types
    /// - `false` - For unsigned numeric types or non-numeric types
    ///
    /// # Note
    ///
    /// For non-numeric types, this property has no semantic meaning.
    pub fn is_signed(&self) -> bool {
        self.data_type.signed
    }

    /// Returns the known extended type for this data type.
    ///
    /// This method parses the extended type annotation and returns a strongly-typed
    /// enum value representing known extended types.
    ///
    /// # Returns
    ///
    /// A `KnownExtendedType` enum value representing the extended type.
    pub fn known_extended_type(&self) -> KnownExtendedType {
        self.extended_type_name()
            .and_then(|name| KnownExtendedType::from_str(name).ok())
            .unwrap_or_default()
    }

    /// Returns a descriptor containing the basic type information for this data type.
    ///
    /// This method creates a `BasicTypeDescriptor` that encapsulates the fundamental
    /// characteristics of this data type, including its basic type, size, signedness,
    /// and extended type information. This descriptor can be used for type comparison,
    /// serialization, and other operations that need a compact representation of the
    /// type's characteristics.
    ///
    /// # Returns
    ///
    /// A `BasicTypeDescriptor` containing the type's fundamental characteristics.
    pub fn describe(&self) -> BasicTypeDescriptor {
        BasicTypeDescriptor {
            basic_type: self.basic_type(),
            fixed_size: self.fixed_size().unwrap_or_default() as u32,
            signed: self.is_signed(),
            extended_type: self.known_extended_type(),
        }
    }

    /// Returns the extended type annotation label for this data type.
    ///
    /// This method returns the raw extended type label string if one has been set,
    /// or `None` if no extended type annotation exists. The label is used to provide
    /// additional semantic information about the data type beyond the basic type system.
    ///
    /// # Returns
    ///
    /// - `Some(label)` - The extended type label string if set
    /// - `None` - If no extended type annotation exists
    pub fn extended_type_name(&self) -> Option<&str> {
        self.data_type
            .extended_type
            .as_ref()
            .map(|annotation| annotation.label.as_str())
    }

    /// Sets the extended type annotation for the data type.
    ///
    /// This method allows you to specify additional semantic information about
    /// the data type beyond the basic type system. Extended type annotations
    /// are used to distinguish between different uses of the same basic type,
    /// such as timestamps, decimals, or custom application-specific types.
    ///
    /// # Arguments
    ///
    /// * `label` - The extended type label to assign
    pub fn set_extended_type(&mut self, label: impl Into<String>) {
        self.data_type
            .extended_type
            .get_or_insert_with(|| {
                Box::new(defs::schema::ExtendedTypeAnnotation {
                    ..Default::default()
                })
            })
            .label = label.into();
    }

    /// Adds a child data type to this data type.
    ///
    /// This method is used to build composite data types such as structs, lists,
    /// unions, and other container types. The child data type becomes part of this
    /// data type's structure and will be included in the final schema.
    ///
    /// # Arguments
    ///
    /// * `child` - The child data type builder to add
    ///
    /// # Type Constraints
    ///
    /// This method enforces several constraints based on the parent data type:
    /// - The parent must be a composite type (struct, list, union, etc.)
    /// - The number of children must not exceed the maximum allowed for the parent type
    /// - For types that require named children (like structs), the child must have a non-empty field name
    /// - Child names must be unique within the parent (for types that support named children)
    ///
    /// # Panics
    ///
    /// This method will panic if:
    /// - The parent data type is not a composite type
    /// - The maximum number of children for the parent type would be exceeded
    /// - A child with the same name already exists (for types with named children)
    /// - The child field name is empty when the parent type requires named children
    ///
    /// # Child Indexing
    ///
    /// For composite types that support named children, an internal index is maintained
    /// to enable fast lookups by name. When the number of named children exceeds a
    /// threshold, a hash-based lookup table is automatically created for optimal performance.
    pub fn add_child(&mut self, child: DataTypeBuilder) {
        assert!(self.basic_type().is_composite());

        assert!(self.children.len() < self.basic_type().max_children());

        if self.basic_type().requires_named_children() {
            assert!(!child.field_name().is_empty());
        }

        if self.basic_type().allows_named_children() && !child.field_name().is_empty() {
            assert!(!self.child_map.contains_key(child.field_name()));
            self.child_map
                .insert(child.field_name().to_string(), self.children.len());
        }

        self.children.push(child);
    }

    /// Returns a slice of the child data type builders.
    ///
    /// This method provides read-only access to all child data types that have been
    /// added to this data type. The children are returned in the order they were added.
    ///
    /// # Returns
    ///
    /// A slice containing references to all child data type builders.
    pub fn children(&self) -> &[DataTypeBuilder] {
        &self.children
    }

    /// Returns a mutable slice of the child data type builders.
    ///
    /// This method provides mutable access to all child data types that have been
    /// added to this data type. The children are returned in the order they were added.
    /// This allows for direct modification of child data types after they have been added.
    ///
    /// # Returns
    ///
    /// A mutable slice containing references to all child data type builders.
    ///
    /// # Safety
    ///
    /// When modifying children directly, be careful not to change field names in ways
    /// that would break the internal name-to-index mapping. Use the provided methods
    /// on the parent data type for safe modifications.
    pub fn children_mut(&mut self) -> &mut [DataTypeBuilder] {
        &mut self.children
    }

    /// Finds a child data type by name.
    ///
    /// This method searches for a child data type with the specified name and returns
    /// a reference to it if found. The search is performed using the internal name-to-index
    /// mapping, making it efficient even for data types with many children.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the child data type to find
    ///
    /// # Returns
    ///
    /// - `Some(&DataTypeBuilder)` - A reference to the child data type if found
    /// - `None` - If no child with the specified name exists
    ///
    /// # Type Parameters
    ///
    /// * `Q` - The type of the search key, which must be hashable and comparable to `String`
    ///
    /// # Performance
    ///
    /// This method uses a hash map for lookups, providing O(1) average-case performance
    /// for finding children by name.
    pub fn find_child<Q>(&self, name: &Q) -> Option<&DataTypeBuilder>
    where
        Q: Hash + Eq + ?Sized,
        String: Borrow<Q>,
    {
        self.child_map.get(name).map(|&i| &self.children[i])
    }

    /// Finds a child data type by name and returns a mutable reference.
    ///
    /// This method searches for a child data type with the specified name and returns
    /// a mutable reference to it if found. The search is performed using the internal
    /// name-to-index mapping, making it efficient even for data types with many children.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the child data type to find
    ///
    /// # Returns
    ///
    /// - `Some(&mut DataTypeBuilder)` - A mutable reference to the child data type if found
    /// - `None` - If no child with the specified name exists
    ///
    /// # Type Parameters
    ///
    /// * `Q` - The type of the search key, which must be hashable and comparable to `String`
    ///
    /// # Performance
    ///
    /// This method uses a hash map for lookups, providing O(1) average-case performance
    /// for finding children by name.
    ///
    /// # Safety
    ///
    /// When modifying the child through the returned mutable reference, be careful not
    /// to change field names in ways that would break the internal name-to-index mapping.
    pub fn find_child_mut<Q>(&mut self, name: &Q) -> Option<&mut DataTypeBuilder>
    where
        Q: Hash + Eq + ?Sized,
        String: Borrow<Q>,
    {
        self.child_map.get(name).map(|&i| &mut self.children[i])
    }

    /// Finishes building the data type and returns the completed data type definition.
    ///
    /// This method consumes the builder and produces a `defs::schema::DataType` that
    /// represents the complete data type definition. It performs final processing
    /// including building lookup tables for child names and converting all child
    /// builders to their final forms.
    ///
    /// # Returns
    ///
    /// A complete `defs::schema::DataType` that can be used in the final schema.
    ///
    /// # Processing
    ///
    /// This method performs several finalization steps:
    /// 1. Builds hash lookup tables for child names if needed
    /// 2. Recursively calls `finish()` on all child builders
    /// 3. Attaches the lookup table to the data type if applicable
    /// 4. Returns the complete data type definition
    ///
    /// # Consumption
    ///
    /// This method consumes the builder, making it unusable after the call.
    /// All child builders are also consumed during the process.
    pub fn finish(self) -> defs::schema::DataType {
        let lookup = self.build_child_lookup();
        let mut data_type = self.data_type;
        data_type.children = self
            .children
            .into_iter()
            .map(DataTypeBuilder::finish)
            .collect();
        data_type.lookup = lookup;
        data_type
    }
}

impl DataTypeBuilder {
    /// Minimum number of named children required before creating a hash lookup table.
    ///
    /// When a data type has fewer than this many named children, lookups are performed
    /// using linear search. Once this threshold is exceeded, a hash-based lookup table
    /// is created for optimal performance.
    const MIN_NAMED_CHILDREN_FOR_LOOKUP: usize = 10;

    /// Assigns schema IDs to this data type and all its children recursively.
    ///
    /// This method performs a depth-first traversal of the data type tree, assigning
    /// unique schema IDs to each data type. Schema IDs are used for efficient field
    /// lookups and serialization operations.
    ///
    /// # Arguments
    ///
    /// * `next` - The next available schema ID to assign
    ///
    /// # Returns
    ///
    /// The next available schema ID after assigning IDs to this data type and all
    /// its children.
    ///
    /// # Panics
    ///
    /// Panics if the provided `next` schema ID is invalid.
    fn assign_schema_ids(&mut self, mut next: SchemaId) -> SchemaId {
        assert!(next.is_valid());
        self.data_type.schema_id = next.as_u32();
        next = next.next();
        for child in &mut self.children {
            next = child.assign_schema_ids(next);
        }
        next
    }

    /// Builds a hash lookup table for child names if needed.
    ///
    /// This method creates a hash-based lookup table for child data types when
    /// certain conditions are met:
    /// 1. The data type supports named children
    /// 2. The number of named children exceeds the minimum threshold
    ///
    /// The lookup table enables efficient O(1) average-case lookups for child
    /// data types by name, which is especially important for data types with
    /// many children.
    ///
    /// # Returns
    ///
    /// - `Some(Box<HashLookup>)` - A hash lookup table if conditions are met
    /// - `None` - If no lookup table is needed
    ///
    /// # Performance
    ///
    /// The lookup table is only created when the number of named children exceeds
    /// `MIN_NAMED_CHILDREN_FOR_LOOKUP` to avoid the overhead for small numbers
    /// of children where linear search is sufficient.
    fn build_child_lookup(&self) -> Option<Box<HashLookup>> {
        if !self.basic_type().allows_named_children() {
            return None;
        }

        if self.child_map.len() < Self::MIN_NAMED_CHILDREN_FOR_LOOKUP {
            return None;
        }

        let lookup = HashLookup::build(self.children.len(), |i| {
            hash_field_name(self.children[i].field_name())
        });
        Some(Box::new(lookup))
    }
}

#[cfg(test)]
mod tests {
    use crate::{schema::BasicType, schema_builder::DataTypeBuilder};

    use super::{FieldBuilder, SchemaBuilder};

    #[test]
    fn test_basic_schema_building() {
        let mut schema = SchemaBuilder::new(vec![]);
        schema.add_field(FieldBuilder::new_i64().with_name("id"));
        schema.add_field(FieldBuilder::new_str().with_name("source"));
        schema.add_field(FieldBuilder::new_struct().with_name("props"));

        for i in 0..20 {
            let name = format!("prop{i}");
            schema
                .find_field_mut("props")
                .unwrap()
                .add_child(DataTypeBuilder::new_str().with_field_name(name));
        }

        let schema_message = schema.finish_and_seal();
        let schema = schema_message.schema().unwrap();
        assert_eq!(schema.len().unwrap(), 3);
        assert_eq!(schema.field_at(1).unwrap().name().unwrap(), "source");

        assert_eq!(
            schema
                .find_field("source")
                .unwrap()
                .unwrap()
                .1
                .name()
                .unwrap(),
            "source"
        );

        let field = schema.field_at(2).unwrap();
        let dt = field.data_type().unwrap();
        assert_eq!(dt.name().unwrap(), "props");
        assert_eq!(dt.basic_type().unwrap(), BasicType::Struct);
        assert_eq!(dt.child_at(0).unwrap().name().unwrap(), "prop0");

        let child = dt.find_child("prop10").unwrap().unwrap().1;
        assert_eq!(child.name().unwrap(), "prop10");
        assert!(dt.find_child("prop30").unwrap().is_none());
    }
}
