use std::{borrow::Borrow, hash::Hash};

use ahash::AHashMap;

use crate::{
    checksum,
    defs::{
        self, hash_lookup_ext::hash_field_name, schema::HashLookup, schema_ext::KnownExtendedType,
    },
    schema::{BasicType, SchemaId, SchemaMessage},
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
#[derive(Debug, Clone)]
pub struct FieldBuilder {
    data_type: DataTypeBuilder,
    internal_kind: Option<String>,
}

impl FieldBuilder {
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

    pub fn new_str() -> FieldBuilder {
        FieldBuilder::new("", BasicType::String, None, None)
    }

    pub fn new_i64() -> FieldBuilder {
        FieldBuilder::new("", BasicType::Int64, true, None)
    }

    pub fn new_list() -> FieldBuilder {
        FieldBuilder::new("", BasicType::List, None, None)
    }

    pub fn new_struct() -> FieldBuilder {
        FieldBuilder::new("", BasicType::Struct, None, None)
    }

    pub fn with_name(mut self, field_name: impl Into<String>) -> FieldBuilder {
        self.data_type = self.data_type.with_field_name(field_name);
        self
    }

    pub fn name(&self) -> &str {
        self.data_type.field_name()
    }

    pub fn basic_type(&self) -> BasicType {
        self.data_type.basic_type()
    }

    pub fn fixed_size(&self) -> Option<usize> {
        self.data_type.fixed_size()
    }

    pub fn is_signed(&self) -> bool {
        self.data_type.is_signed()
    }

    pub fn data_type(&self) -> &DataTypeBuilder {
        &self.data_type
    }

    pub fn data_type_mut(&mut self) -> &mut DataTypeBuilder {
        &mut self.data_type
    }

    pub fn set_internal_kind(&mut self, kind: impl Into<String>) {
        self.internal_kind = Some(kind.into());
    }

    pub fn set_extended_type(&mut self, label: impl Into<String>) {
        self.data_type.set_extended_type(label);
    }

    pub fn add_child(&mut self, child: DataTypeBuilder) {
        self.data_type.add_child(child);
    }

    pub fn children(&self) -> &[DataTypeBuilder] {
        self.data_type.children()
    }

    pub fn children_mut(&mut self) -> &mut [DataTypeBuilder] {
        self.data_type.children_mut()
    }

    pub fn find_child<Q>(&self, name: &Q) -> Option<&DataTypeBuilder>
    where
        Q: Hash + Eq + ?Sized,
        String: Borrow<Q>,
    {
        self.data_type.find_child(name)
    }

    pub fn find_child_mut<Q>(&mut self, name: &Q) -> Option<&mut DataTypeBuilder>
    where
        Q: Hash + Eq + ?Sized,
        String: Borrow<Q>,
    {
        self.data_type.find_child_mut(name)
    }

    /// Finishes building the field and returns it.
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

/// A builder for creating a data type in a schema.
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

    pub fn new_str() -> DataTypeBuilder {
        DataTypeBuilder::new("", BasicType::String, false, 0, vec![])
    }

    pub fn new_i64() -> DataTypeBuilder {
        DataTypeBuilder::new("", BasicType::Int64, true, 0, vec![])
    }

    pub fn new_binary() -> DataTypeBuilder {
        DataTypeBuilder::new("", BasicType::Binary, false, 0, vec![])
    }

    pub fn new_guid() -> DataTypeBuilder {
        DataTypeBuilder::new("", BasicType::Guid, false, 0, vec![])
    }

    pub fn new_timespan() -> DataTypeBuilder {
        let mut dt = DataTypeBuilder::new("", BasicType::Int64, true, 0, vec![]);
        dt.set_extended_type(KnownExtendedType::KUSTO_TIMESPAN_LABEL);
        dt
    }

    pub fn new_decimal() -> DataTypeBuilder {
        let mut dt = DataTypeBuilder::new("", BasicType::FixedSizeBinary, false, 16, vec![]);
        dt.set_extended_type(KnownExtendedType::KUSTO_DECIMAL_LABEL);
        dt
    }

    pub fn new_dynamic() -> DataTypeBuilder {
        let mut dt = DataTypeBuilder::new("", BasicType::Binary, false, 0, vec![]);
        dt.set_extended_type(KnownExtendedType::KUSTO_DYNAMIC_LABEL);
        dt
    }

    pub fn with_field_name(mut self, field_name: impl Into<String>) -> DataTypeBuilder {
        self.data_type.field_name = field_name.into();
        self
    }

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

    pub fn field_name(&self) -> &str {
        &self.data_type.field_name
    }

    pub fn schema_id(&self) -> SchemaId {
        self.data_type.schema_id.into()
    }

    pub fn basic_type(&self) -> BasicType {
        self.data_type.basic_type
    }

    pub fn data_type(&self) -> &defs::schema::DataType {
        &self.data_type
    }

    pub fn fixed_size(&self) -> Option<usize> {
        let size = self.data_type.fixed_size;
        (size != 0).then_some(size as usize)
    }

    pub fn is_signed(&self) -> bool {
        self.data_type.signed
    }

    /// Sets the extended type annotation for the data type.
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

    pub fn children(&self) -> &[DataTypeBuilder] {
        &self.children
    }

    pub fn children_mut(&mut self) -> &mut [DataTypeBuilder] {
        &mut self.children
    }

    pub fn find_child<Q>(&self, name: &Q) -> Option<&DataTypeBuilder>
    where
        Q: Hash + Eq + ?Sized,
        String: Borrow<Q>,
    {
        self.child_map.get(name).map(|&i| &self.children[i])
    }

    pub fn find_child_mut<Q>(&mut self, name: &Q) -> Option<&mut DataTypeBuilder>
    where
        Q: Hash + Eq + ?Sized,
        String: Borrow<Q>,
    {
        self.child_map.get(name).map(|&i| &mut self.children[i])
    }

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
    const MIN_NAMED_CHILDREN_FOR_LOOKUP: usize = 10;

    fn assign_schema_ids(&mut self, mut next: SchemaId) -> SchemaId {
        assert!(next.is_valid());
        self.data_type.schema_id = next.as_u32();
        next = next.next();
        for child in &mut self.children {
            next = child.assign_schema_ids(next);
        }
        next
    }

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
