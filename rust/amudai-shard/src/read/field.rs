use std::sync::Arc;

use amudai_common::Result;
use amudai_format::schema::{self, SchemaId};

use super::{
    field_context::FieldContext,
    field_decoder::FieldDecoder,
    stripe::{Stripe, StripeFieldDescriptor},
};

/// Represents a field within a shard's stripe that can be read.
///
/// Provides access to the field's schema information, associated stripe data,
/// and methods to create decoders for reading the field's data.
pub struct Field(Arc<FieldContext>);

impl Field {
    /// Creates a new `Field` instance from a field context.
    ///
    /// This constructor is restricted to the crate's read module.
    pub(in crate::read) fn new(field: Arc<FieldContext>) -> Field {
        Field(field)
    }

    /// Returns a reference to the data type of this field.
    ///
    /// The data type contains information about the field's name, type,
    /// and nested structure.
    pub fn data_type(&self) -> &schema::DataType {
        self.0.data_type()
    }

    /// Returns the schema ID of this field.
    ///
    /// Schema IDs uniquely identify fields within a schema.
    pub fn schema_id(&self) -> SchemaId {
        self.0.schema_id()
    }

    /// Returns a reference to the stripe field descriptor for this field.
    ///
    /// The descriptor contains metadata about the field's representation
    /// in the stripe.
    pub fn descriptor(&self) -> &StripeFieldDescriptor {
        self.0.descriptor()
    }

    /// Returns the total number of logical positions (value slots) in the stripe field.
    pub fn position_count(&self) -> u64 {
        self.0.position_count()
    }

    /// Returns the stripe that contains this field.
    pub fn get_stripe(&self) -> Stripe {
        Stripe::from_ctx(self.0.stripe().clone())
    }

    /// Creates a decoder for reading this field's data.
    ///
    /// # Errors
    ///
    /// Returns an error if the decoder cannot be created due to issues with
    /// the underlying data or incompatible encoding.
    pub fn create_decoder(&self) -> Result<FieldDecoder> {
        self.0.create_decoder()
    }
}
