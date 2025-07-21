use std::sync::{Arc, OnceLock};

use amudai_common::{Result, error::Error, verify_arg, verify_data};
use amudai_format::{
    defs::shard,
    schema::{DataType, SchemaId},
};

use crate::read::properties::PropertyBag;

use super::{
    anchored_element::AnchoredElement,
    element_memo::Memo,
    field_context::FieldContext,
    shard_context::{ShardContext, ShardFieldRefs},
};

pub struct StripeContext {
    ordinal: usize,
    field_refs: OnceLock<StripeFieldRefs>,
    field_descriptors: Memo<SchemaId, Arc<StripeFieldDescriptor>>,
    properties: OnceLock<StripeProperties>,
    indexes: OnceLock<StripeIndexCollection>,
    shard: Arc<ShardContext>,
}

impl StripeContext {
    pub fn open(ordinal: usize, shard: Arc<ShardContext>) -> Result<StripeContext> {
        let stripe_list = shard
            .get_stripe_list()
            .expect("stripe list must be loaded at this point");
        verify_arg!(index, ordinal < stripe_list.len());
        let directory = stripe_list.get_at(ordinal);
        verify_data!(field_list_ref, directory.field_list_ref.is_some());
        Ok(StripeContext {
            ordinal,
            field_refs: Default::default(),
            field_descriptors: Memo::new(),
            properties: Default::default(),
            indexes: Default::default(),
            shard,
        })
    }

    pub fn ordinal(&self) -> usize {
        self.ordinal
    }

    pub fn directory(&self) -> &shard::StripeDirectory {
        self.shard
            .get_stripe_list()
            .expect("loaded stripe list")
            .get_at(self.ordinal)
    }

    pub fn shard(&self) -> &Arc<ShardContext> {
        &self.shard
    }

    pub fn open_field(self: &Arc<Self>, data_type: DataType) -> Result<FieldContext> {
        let schema_id = data_type.schema_id()?;
        let descriptor = self.fetch_field_descriptor(schema_id)?;
        let field_ctx = FieldContext::new(data_type, descriptor, self.clone());
        Ok(field_ctx)
    }

    pub fn get_field_descriptor(&self, schema_id: SchemaId) -> Option<Arc<StripeFieldDescriptor>> {
        self.field_descriptors.get(&schema_id)
    }

    pub fn fetch_field_descriptor(
        &self,
        schema_id: SchemaId,
    ) -> Result<Arc<StripeFieldDescriptor>> {
        if let Some(desc) = self.field_descriptors.get(&schema_id) {
            return Ok(desc);
        }
        let desc = self.load_field_descriptor(schema_id)?;
        desc.field
            .as_ref()
            .ok_or_else(|| Error::invalid_format("field descriptor inner field"))?;
        let desc = Arc::new(desc);
        self.field_descriptors.put(schema_id, desc.clone());
        Ok(desc)
    }

    pub fn load_field_descriptor(&self, schema_id: SchemaId) -> Result<StripeFieldDescriptor> {
        let field_refs = self.fetch_field_refs()?;
        if schema_id.as_usize() >= field_refs.len() {
            // Stripe field descriptors list may be truncated if the fields in question
            // are not present in that stripe.
            return Ok(StripeFieldDescriptor::new(
                self.create_missing_field_descriptor(schema_id)?,
                field_refs.anchor().clone(),
            ));
        }
        let field_ref = field_refs.get_at(schema_id.as_usize());
        if field_ref.is_empty() {
            return Ok(StripeFieldDescriptor::new(
                Default::default(),
                field_refs.anchor().clone(),
            ));
        }
        let reader = self
            .shard
            .open_artifact(&field_ref, Some(field_refs.anchor()))?;
        let descriptor = reader.read_message::<shard::StripeFieldDescriptor>(&field_ref)?;
        Ok(StripeFieldDescriptor::new(descriptor, reader.url().clone()))
    }

    pub fn fetch_field_refs(&self) -> Result<&StripeFieldRefs> {
        if let Some(field_refs) = self.field_refs.get() {
            return Ok(field_refs);
        }
        let field_refs = self.load_field_refs()?;
        let _ = self.field_refs.set(field_refs);
        Ok(self.field_refs.get().expect("field_refs"))
    }

    /// Retrieves the stripe properties, loading them if not already cached.
    ///
    /// This method returns a convenient wrapper around the stripe properties
    /// that provides typed access to both standard stripe metadata and
    /// custom properties.
    ///
    /// # Returns
    ///
    /// A `StripePropertyBag` providing access to all stripe-level properties.
    ///
    /// # Errors
    ///
    /// Returns an error if the properties cannot be loaded from storage.
    pub fn fetch_properties(&self) -> Result<StripePropertyBag> {
        let props = self.fetch_properties_message()?;
        Ok(StripePropertyBag {
            _standard: PropertyBag::new(&props.standard_properties),
            custom: PropertyBag::new(&props.custom_properties),
        })
    }

    /// Retrieves the raw stripe properties message, loading it if not already cached.
    ///
    /// This method returns the underlying protobuf message structure containing
    /// the stripe properties. For convenient typed access, use `fetch_properties()`
    /// instead.
    ///
    /// # Returns
    ///
    /// A reference to the loaded `StripeProperties` message.
    ///
    /// # Errors
    ///
    /// Returns an error if the properties cannot be loaded from storage.
    pub fn fetch_properties_message(&self) -> Result<&StripeProperties> {
        if let Some(properties) = self.properties.get() {
            return Ok(properties);
        }
        let properties = self.load_properties()?;
        let _ = self.properties.set(properties);
        Ok(self.properties.get().expect("properties"))
    }

    /// Retrieves the stripe-level index descriptor collection, loading it if not already
    /// cached.
    ///
    /// The index collection contains metadata about various indexes available for this stripe.
    ///
    /// # Returns
    ///
    /// A reference to the loaded `StripeIndexCollection`.
    ///
    /// # Errors
    ///
    /// Returns an error if the index collection cannot be loaded from storage.
    pub fn fetch_indexes(&self) -> Result<&StripeIndexCollection> {
        if let Some(indexes) = self.indexes.get() {
            return Ok(indexes);
        }
        let indexes = self.load_indexes()?;
        let _ = self.indexes.set(indexes);
        Ok(self.indexes.get().expect("indexes"))
    }

    fn load_field_refs(&self) -> Result<StripeFieldRefs> {
        let stripe_list = self.shard.get_stripe_list().expect("loaded stripe list");
        let directory_anchor = stripe_list.anchor();
        let directory = stripe_list.get_at(self.ordinal);
        let list_ref = directory
            .field_list_ref
            .as_ref()
            .ok_or_else(|| Error::invalid_format("stripe field list ref"))?;
        let reader = self.shard.open_artifact(list_ref, Some(directory_anchor))?;
        let field_refs = reader.read_message(list_ref)?;
        let field_refs = StripeFieldRefs::new(field_refs, reader.url().clone());
        Ok(field_refs)
    }

    /// Loads the stripe properties from storage.
    ///
    /// This internal method reads the properties protobuf message from the
    /// object store using the properties reference from the stripe directory.
    /// If no properties reference exists, returns default empty properties.
    ///
    /// # Returns
    ///
    /// The loaded `StripeProperties` anchored to its storage location.
    ///
    /// # Errors
    ///
    /// Returns an error if the properties cannot be loaded from storage.
    fn load_properties(&self) -> Result<StripeProperties> {
        let stripe_list = self.shard.get_stripe_list().expect("loaded stripe list");
        let directory_anchor = stripe_list.anchor();
        let directory = stripe_list.get_at(self.ordinal);
        let Some(properties_ref) = directory.properties_ref.as_ref() else {
            return Ok(StripeProperties::new(
                Default::default(),
                directory_anchor.clone(),
            ));
        };
        let reader = self
            .shard
            .open_artifact(properties_ref, Some(directory_anchor))?;
        let properties = reader.read_message(properties_ref)?;
        let properties = StripeProperties::new(properties, reader.url().clone());
        Ok(properties)
    }

    /// Loads the stripe index descriptor collection from storage.
    ///
    /// This internal method reads the index collection protobuf message from the
    /// object store using the indexes reference from the stripe directory.
    /// If no indexes reference exists, returns a default empty index collection.
    ///
    /// # Returns
    ///
    /// The loaded `StripeIndexCollection` anchored to its storage location.
    ///
    /// # Errors
    ///
    /// Returns an error if the index collection cannot be loaded from storage.
    fn load_indexes(&self) -> Result<StripeIndexCollection> {
        let stripe_list = self.shard.get_stripe_list().expect("loaded stripe list");
        let directory_anchor = stripe_list.anchor();
        let directory = stripe_list.get_at(self.ordinal);
        let Some(indexes_ref) = directory.indexes_ref.as_ref() else {
            return Ok(StripeIndexCollection::new(
                Default::default(),
                directory_anchor.clone(),
            ));
        };
        let reader = self
            .shard
            .open_artifact(indexes_ref, Some(directory_anchor))?;
        let indexes = reader.read_message(indexes_ref)?;
        let indexes = StripeIndexCollection::new(indexes, reader.url().clone());
        Ok(indexes)
    }

    fn create_missing_field_descriptor(
        &self,
        _schema_id: SchemaId,
    ) -> Result<shard::StripeFieldDescriptor> {
        let _schema = self.shard().fetch_schema()?;
        // TODO:
        // 1. Locate the parent node (fetch_field_descriptor) of this schema_id
        //    and determine the proper "simulated" position_count for this missing field:
        //     - Parent is Struct or Union: use the same position_count as the parent
        //     - Parent is Map or List: position_count is zero
        //     - Parent is FixedSizeList: position_count is parent_position_count * list_size
        //     - No parent (top-level field): position_count is the number of stripe records
        // 2. Check the shard-level field descriptor for a constant value and transfer it
        //    to the stripe-level field descriptor.
        Ok(Default::default())
    }
}

pub type StripeFieldRefs = ShardFieldRefs;

pub type StripeFieldDescriptor = AnchoredElement<shard::StripeFieldDescriptor>;

pub type StripeProperties = AnchoredElement<shard::StripeProperties>;

pub type StripeIndexCollection = AnchoredElement<shard::IndexCollection>;

/// A specialized property bag for stripe-level properties.
///
/// `StripePropertyBag` provides access to both standard and custom properties
/// associated with a specific stripe within a shard. It automatically delegates
/// to the custom properties when used directly, while maintaining access to
/// standard properties through its internal structure.
///
/// This wrapper ensures type-safe access to stripe properties while maintaining
/// compatibility with the general `PropertyBag` interface.
pub struct StripePropertyBag<'a> {
    _standard: PropertyBag<'a>,
    custom: PropertyBag<'a>,
}

impl<'a> std::ops::Deref for StripePropertyBag<'a> {
    type Target = PropertyBag<'a>;

    fn deref(&self) -> &Self::Target {
        &self.custom
    }
}
