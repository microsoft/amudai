use std::sync::{Arc, OnceLock};

use amudai_common::{Result, verify_arg, verify_data};
use amudai_format::{
    defs::shard,
    schema::{DataType, SchemaId},
};

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
            .open_data_ref(&field_ref, Some(field_refs.anchor()))?;
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

    fn load_field_refs(&self) -> Result<StripeFieldRefs> {
        let stripe_list = self.shard.get_stripe_list().expect("loaded stripe list");
        let directory_anchor = stripe_list.anchor();
        let directory = stripe_list.get_at(self.ordinal);
        let list_ref = directory.field_list_ref.as_ref().expect("field_list_ref");
        let reader = self.shard.open_data_ref(list_ref, Some(directory_anchor))?;
        let field_refs = reader.read_message(list_ref)?;
        let field_refs = StripeFieldRefs::new(field_refs, reader.url().clone());
        Ok(field_refs)
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
