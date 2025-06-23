use std::str::FromStr;

use amudai_bytes::Bytes;
use amudai_common::{Result, error::Error};

use crate::{
    checksum::validate_message,
    defs::{
        hash_lookup_ext::hash_field_name,
        schema_ext::{KnownExtendedType, OwnedDataTypeRef, OwnedFieldRef, OwnedSchemaRef},
    },
    schema_builder::SchemaBuilder,
};

pub use crate::defs::schema::BasicType;
pub use crate::defs::schema_ext::BasicTypeDescriptor;

/// Represents a schema message that includes a buffer, prefixed with its length
/// and suffixed with a checksum.
///
/// This struct encapsulates the raw bytes of a complete schema message and
/// its validated length.
#[derive(Clone)]
pub struct SchemaMessage {
    /// `length-message-checksum` buffer.
    buf: Bytes,
    /// Length of the flatbuffers Schema message, which starts after a 4-byte length
    /// prefix within the `buf`.
    len: usize,
}

impl SchemaMessage {
    /// Creates a new `SchemaMessage` from the provided buffer after validating it.
    ///
    /// # Arguments
    ///
    /// * `buf` - A `Bytes` buffer containing the schema message data.
    ///
    /// # Errors
    ///
    /// Returns an error if the message validation fails.
    pub fn new(buf: Bytes) -> Result<SchemaMessage> {
        let len = validate_message(&buf)?.len();
        Ok(SchemaMessage { buf, len })
    }

    /// Retrieves the `Schema` from the message.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema extraction fails.
    pub fn schema(&self) -> Result<Schema> {
        Ok(Schema(OwnedSchemaRef::new_as_root(
            self.buf.slice(4..self.len + 4),
        )?))
    }

    /// Provides access to the raw bytes of the schema message.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }
}

impl From<SchemaBuilder> for SchemaMessage {
    fn from(builder: SchemaBuilder) -> SchemaMessage {
        builder.finish_and_seal()
    }
}

/// Represents a schema containing multiple fields.
///
/// This struct acts as a wrapper for a zero-deserialization Schema element
/// and provides methods to interact with the schema's fields.
#[derive(Clone)]
pub struct Schema(OwnedSchemaRef);

impl Schema {
    // Returns the number of fields in the schema.
    ///
    /// # Errors
    ///
    /// Returns an error if retrieving the fields count fails.
    pub fn len(&self) -> Result<usize> {
        Ok(self.0.get().fields()?.len())
    }

    /// Checks whether the schema contains no fields.
    ///
    /// # Errors
    ///
    /// Returns an error if checking the fields count fails.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Returns the next available `schema_id` that is not currently in use.
    pub fn next_schema_id(&self) -> Result<SchemaId> {
        Ok(self.0.get().schema_id_count()?.into())
    }

    /// Retrieves the field at the specified index within the schema.
    ///
    /// # Arguments
    ///
    /// * `index` - The zero-based index of the field to retrieve.
    ///
    /// # Errors
    ///
    /// Returns an error if the index is out of bounds or if accessing the field fails.
    pub fn field_at(&self, index: usize) -> Result<Field> {
        let field = self.0.map::<crate::defs::schema::Field, _>(|schema, ()| {
            let field = schema
                .fields()?
                .get(index)
                .ok_or_else(|| Error::invalid_arg("index", "invalid schema field index"))??;
            Ok(field)
        })?;
        Ok(Field(field))
    }

    /// Returns the fields of the schema encapsulated in a `FieldList`.
    pub fn field_list(&self) -> Result<FieldList> {
        Ok(FieldList::from(self.clone()))
    }

    /// Finds the field with the specified name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the field.
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some((index, field)))` if a field with the given name is found.
    /// The `index` is the position of the field within the schema's field list,
    /// and `field` is the field itself.
    ///
    /// Returns `Ok(None)` if no field with the given name is found.
    ///
    /// # Errors
    ///
    /// Returns an error if accessing the field fails.
    pub fn find_field(&self, name: &str) -> Result<Option<(usize, Field)>> {
        let inner = self.0.get();
        let fields = inner.fields()?;
        let lookup = inner.field_lookup()?;
        if let Some(lookup) = lookup {
            let h = hash_field_name(name);
            let res = lookup.find(h, |i| {
                let field = fields
                    .get(i)
                    .ok_or_else(|| Error::invalid_arg("index", ""))??;
                Ok(field.data_type()?.field_name()? == name)
            });
            match res {
                Ok(Some(i)) => return self.field_at(i).map(|field| Some((i, field))),
                Ok(None) => return Ok(None),
                Err(_) => {
                    // Fallback to linear search
                }
            }
        }

        for (i, field) in fields.iter().enumerate() {
            let field = field?;
            if field.data_type()?.field_name()? == name {
                return self.field_at(i).map(|field| Some((i, field)));
            }
        }

        Ok(None)
    }
}

/// Represents a single field within a schema.
///
/// This struct encapsulates a Field sub-buffer from the Schema without requiring
/// deserialization and provides methods to access field properties.
#[derive(Clone)]
pub struct Field(OwnedFieldRef);

impl Field {
    /// Retrieves the name of the field.
    ///
    /// # Errors
    ///
    /// Returns an error if accessing the field name fails.
    pub fn name(&self) -> Result<&str> {
        Ok(self.0.get().data_type()?.field_name()?)
    }

    /// Retrieves the data type of the field.
    ///
    /// # Errors
    ///
    /// Returns an error if accessing the data type fails.
    pub fn data_type(&self) -> Result<DataType> {
        let data_type = self
            .0
            .map::<crate::defs::schema::DataType, _>(|field, ()| Ok(field.data_type()?))?;
        Ok(DataType(data_type))
    }
}

/// Represents the data type of a field within a schema. This can be the root
/// of a data type sub-tree that includes nested types.
///
/// This struct encapsulates the zero-deserialization DataType sub-buffer
/// of the schema field and offers methods to examine the data type.
#[derive(Clone)]
pub struct DataType(OwnedDataTypeRef);

impl DataType {
    /// Retrieves the name of the data type.
    ///
    /// # Errors
    ///
    /// Returns an error if accessing the data type name fails.
    pub fn name(&self) -> Result<&str> {
        Ok(self.0.get().field_name()?)
    }

    /// Retrieves the basic type of the data type.
    ///
    /// # Errors
    ///
    /// Returns an error if accessing the basic type fails.
    pub fn basic_type(&self) -> Result<BasicType> {
        Ok(self.0.get().basic_type()?)
    }

    /// Retrieves the basic type descriptor of the data type.
    ///
    /// # Errors
    ///
    /// Returns an error if accessing the basic type fails.
    pub fn describe(&self) -> Result<BasicTypeDescriptor> {
        let this = self.0.get();
        Ok(BasicTypeDescriptor {
            basic_type: this.basic_type()?,
            fixed_size: u32::try_from(this.fixed_size()?)
                .map_err(|_| Error::invalid_format("DataType::fixed_size"))?,
            signed: this.signed()?,
            extended_type: KnownExtendedType::from_str(
                self.extension_label()?.unwrap_or_default(),
            )?,
        })
    }

    /// Returns the number of child data types, if any.
    ///
    /// # Errors
    ///
    /// Returns an error if accessing the children count fails.
    pub fn child_count(&self) -> Result<usize> {
        Ok(self.0.get().children()?.len())
    }

    /// Retrieves the child data type at the specified index.
    ///
    /// # Arguments
    ///
    /// * `index` - The zero-based index of the child data type to retrieve.
    ///
    /// # Errors
    ///
    /// Returns an error if the index is out of bounds or if accessing the child data type fails.
    pub fn child_at(&self, index: usize) -> Result<DataType> {
        let data_type = self
            .0
            .map::<crate::defs::schema::DataType, _>(|data_type, ()| {
                let child = data_type.children()?.get(index).ok_or_else(|| {
                    Error::invalid_arg("index", "invalid child data type index")
                })??;
                Ok(child)
            })?;
        Ok(DataType(data_type))
    }

    /// Returns a list of child `DataType` nodes encapsulated within a `FieldList`.
    pub fn field_list(&self) -> Result<FieldList> {
        Ok(FieldList::from(self.clone()))
    }

    /// Finds the child data type with the specified name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the child data type.
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some((index, data_type)))` if a child with the given name is found.
    /// The `index` is the position of the child within the node's children list,
    /// and `data_type` is the child data type node itself.
    ///
    /// Returns `Ok(None)` if no field with the given name is found.
    ///
    /// # Errors
    ///
    /// Returns an error if accessing the child data type fails.
    pub fn find_child(&self, name: &str) -> Result<Option<(usize, DataType)>> {
        let inner = self.0.get();
        let children = inner.children()?;
        let lookup = inner.lookup()?;
        if let Some(lookup) = lookup {
            let h = hash_field_name(name);
            let res = lookup.find(h, |i| {
                let child = children
                    .get(i)
                    .ok_or_else(|| Error::invalid_arg("index", ""))??;
                Ok(child.field_name()? == name)
            });
            match res {
                Ok(Some(i)) => return self.child_at(i).map(|child| Some((i, child))),
                Ok(None) => return Ok(None),
                Err(_) => {
                    // Fallback to linear search
                }
            }
        }

        for (i, child) in children.iter().enumerate() {
            let child = child?;
            if child.field_name()? == name {
                return self.child_at(i).map(|child| Some((i, child)));
            }
        }

        Ok(None)
    }

    /// Retrieves the schema ID associated with this data type node.
    ///
    ///
    /// # Errors
    ///
    /// Returns an error if accessing the schema ID fails.
    pub fn schema_id(&self) -> Result<SchemaId> {
        Ok(self.0.get().schema_id()?.into())
    }

    /// Retrieves the extension type label of this data type node, if present.
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some(label))` if the data type node has an associated extension type label,
    /// where `label` is a string slice representing the label.
    /// Returns `Ok(None)` if the data type node does not have an extension type.
    ///
    /// # Errors
    ///
    /// Returns an error if accessing the extension type or its label fails.
    pub fn extension_label(&self) -> Result<Option<&str>> {
        let ext_type = self.0.get().extended_type()?;
        let Some(ext_type) = ext_type else {
            return Ok(None);
        };
        let label = ext_type.label()?;
        Ok(Some(label))
    }
}

impl std::fmt::Debug for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataType")
            .field("name", &self.name().unwrap_or_default())
            .field(
                "schema_id",
                &self.schema_id().unwrap_or(SchemaId::invalid()),
            )
            .field("basic_type", &self.describe().unwrap_or_default())
            .field("child_count", &self.child_count().unwrap_or_default())
            .finish_non_exhaustive()
    }
}

/// Represents a unique identifier for a shard schema element, which can be a top-level
/// or nested field.
///
/// This is a typed integer wrapper, starting from zero and ranging up to the total number
/// of elements in the shard schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaId(u32);

impl SchemaId {
    /// Creates a `SchemaId` initialized to zero.
    pub const fn zero() -> SchemaId {
        SchemaId(0)
    }

    /// Creates an invalid `SchemaId`, represented by the maximum value of a `u32`.
    pub const fn invalid() -> SchemaId {
        SchemaId(u32::MAX)
    }

    /// Returns the next sequential `SchemaId`.
    pub const fn next(&self) -> SchemaId {
        SchemaId(self.0 + 1)
    }

    /// Checks if the `SchemaId` is valid.
    pub const fn is_valid(&self) -> bool {
        self.0 != u32::MAX
    }

    /// Returns the underlying `u32` value of the `SchemaId`.
    pub const fn as_u32(&self) -> u32 {
        self.0
    }

    /// Converts the `SchemaId` to a `usize`.
    pub const fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl From<u32> for SchemaId {
    fn from(value: u32) -> Self {
        SchemaId(value)
    }
}

impl From<usize> for SchemaId {
    fn from(value: usize) -> Self {
        assert!(u32::try_from(value).is_ok());
        SchemaId(value as u32)
    }
}

impl From<SchemaId> for usize {
    fn from(value: SchemaId) -> Self {
        value.as_usize()
    }
}

/// Represents a locator for a field within a `DataType` node or `Schema`.
///
/// `FieldLocator` is used to identify a specific field, either by its numerical
/// position (ordinal) or by its name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FieldLocator<'a> {
    /// Represents a field located at a specific numerical index.
    Ordinal(usize),
    /// Represents a field located by its name.
    Name(&'a str),
}

/// `FieldList` represents an ordered list of fields, which can either be at the `Schema`
/// level or as the children of a `DataType` node.
#[derive(Clone)]
pub enum FieldList {
    /// An empty list of fields.
    Empty,
    /// A list of fields within a schema.
    Schema(Schema),
    /// A list of child fields of a `DataType` node.
    Node(DataType),
}

impl FieldList {
    /// Returns the number of fields in the list.
    pub fn len(&self) -> Result<usize> {
        match self {
            FieldList::Empty => Ok(0),
            FieldList::Schema(schema) => schema.len(),
            FieldList::Node(data_type) => data_type.child_count(),
        }
    }

    /// Returns true if the list contains no fields.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Retrieves a field from the list based on the provided `FieldLocator`.
    ///
    /// If the `FieldLocator` is an `Ordinal`, it retrieves the field at the given index.
    /// If the `FieldLocator` is a `Name`, it searches for the field with the given name.
    ///
    /// # Arguments
    ///
    /// * `locator` - The `FieldLocator` specifying how to find the field.
    ///
    /// # Returns
    ///
    /// Returns `Some((index, DataType))` if a field is found, where `index` is the field's
    /// position in the list and `DataType` is the field's data type. Returns `None` if no
    /// field is found.
    pub fn get(&self, locator: &FieldLocator) -> Result<Option<(usize, DataType)>> {
        match locator {
            FieldLocator::Ordinal(index) => Ok(Some((*index, self.get_at(*index)?))),
            FieldLocator::Name(name) => self.find(name),
        }
    }

    /// Returns the `DataType` node at the specified index.
    ///
    /// # Errors
    ///
    /// Returns an error if the index is out of bounds or if the list is empty.
    pub fn get_at(&self, index: usize) -> Result<DataType> {
        match self {
            FieldList::Empty => Err(Error::invalid_arg(
                "index",
                "invalid field index (empty list)",
            )),
            FieldList::Schema(schema) => schema.field_at(index)?.data_type(),
            FieldList::Node(data_type) => data_type.child_at(index),
        }
    }

    /// Finds a field by its name and returns its index and `DataType`.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the field to find.
    ///
    /// # Returns
    ///
    /// Returns `Some((index, DataType))` if a field with the given name is found, where `index`
    /// is the field's position in the list and `DataType` is the field's data type. Returns `None`
    /// if no field with the given name is found.
    pub fn find(&self, name: &str) -> Result<Option<(usize, DataType)>> {
        match self {
            FieldList::Empty => Ok(None),
            FieldList::Schema(schema) => {
                if let Some((i, field)) = schema.find_field(name)? {
                    Ok(Some((i, field.data_type()?)))
                } else {
                    Ok(None)
                }
            }
            FieldList::Node(data_type) => data_type.find_child(name),
        }
    }
}

impl From<Schema> for FieldList {
    fn from(value: Schema) -> FieldList {
        FieldList::Schema(value)
    }
}

impl From<&Schema> for FieldList {
    fn from(value: &Schema) -> FieldList {
        FieldList::from(value.clone())
    }
}

impl From<DataType> for FieldList {
    fn from(value: DataType) -> FieldList {
        FieldList::Node(value)
    }
}

impl From<&DataType> for FieldList {
    fn from(value: &DataType) -> FieldList {
        FieldList::from(value.clone())
    }
}
