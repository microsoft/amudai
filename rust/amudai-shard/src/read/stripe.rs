use std::{ops::Range, sync::Arc};

use amudai_common::{Result, error::Error};
use amudai_format::{
    defs::shard,
    schema::{DataType, Schema, SchemaId},
};

use super::{
    field::Field, shard::Shard, shard_context::ShardContext, stripe_context::StripeContext,
};

pub use super::stripe_context::{
    StripeFieldDescriptor, StripeIndexCollection, StripeProperties, StripePropertyBag,
};

/// A `Stripe` represents a horizontal segment of data within a shard.
#[derive(Clone)]
pub struct Stripe(Arc<StripeContext>);

impl Stripe {
    /// Creates a new `Stripe`.
    ///
    /// This function is only intended for internal use within the `crate::read` module.
    ///
    /// # Arguments
    ///
    /// * `index`: The index of the stripe within the shard.
    /// * `shard_ctx`: An `Arc` containing the `ShardContext` to which this stripe belongs.
    pub(in crate::read) fn new(index: usize, shard_ctx: Arc<ShardContext>) -> Result<Stripe> {
        let stripe_ctx = StripeContext::open(index, shard_ctx)?;
        Ok(Stripe(Arc::new(stripe_ctx)))
    }

    pub(in crate::read) fn from_ctx(ctx: Arc<StripeContext>) -> Stripe {
        Stripe(ctx)
    }

    /// Returns the stripe's ordinal within the shard.
    pub fn ordinal(&self) -> usize {
        self.0.ordinal()
    }

    pub fn get_shard(&self) -> Shard {
        Shard::from_ctx(self.0.shard().clone())
    }

    /// Returns a reference to the `StripeDirectory`.
    pub fn directory(&self) -> &shard::StripeDirectory {
        self.0.directory()
    }

    /// Returns the total number of records within this `Stripe`.
    pub fn record_count(&self) -> u64 {
        self.0.directory().total_record_count
    }

    /// Returns a range of logical record positions of this stripe within the shard.
    pub fn shard_position_range(&self) -> Range<u64> {
        let start = self.directory().shard_position;
        let end = start
            .checked_add(self.record_count())
            .expect("shard pos range end");
        start..end
    }

    /// Opens a field within this stripe using the provided `data_type` identifying the
    /// schema id of the field.
    ///
    /// This method returns a [`Field`] object, which provides access to the field's schema
    /// information, descriptor, and methods for reading data from the stripe.
    ///
    /// # Arguments
    ///
    /// * `data_type` - The [`DataType`] describing the field to open.
    ///
    /// # Errors
    ///
    /// Returns an error if the field context or descriptor cannot be loaded, or if the
    /// data type is invalid.
    pub fn open_field(&self, data_type: DataType) -> Result<Field> {
        let field_ctx = self.0.open_field(data_type)?;
        Ok(Field::new(Arc::new(field_ctx)))
    }

    /// Opens a top-level stripe field with the specified name.
    ///
    /// This method returns a [`Field`] object, which provides access  to the field's schema
    /// information, descriptor, and methods for reading data from the stripe.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the top-level field to open.
    ///
    /// # Errors
    ///
    /// Returns an error if the field with the specified name was not found or if the field context
    /// or descriptor cannot be loaded.
    pub fn open_named_field(&self, name: &str) -> Result<Field> {
        let schema = self.fetch_schema()?;
        let (_, field) = schema
            .find_field(name)?
            .ok_or_else(|| Error::invalid_arg("name", format!("Field '{name}' not found")))?;
        self.open_field(field.data_type()?)
    }

    /// Fetches the `Schema` associated with the `Shard` containing this `Stripe`.
    pub fn fetch_schema(&self) -> Result<&Schema> {
        self.0.shard().fetch_schema()
    }

    /// Returns an `Arc` containing the `StripeFieldDescriptor` for the given `schema_id`,
    /// if it was already loaded using `fetch_field_descriptor`.
    ///
    /// Returns `None` if the field descriptor for the given `schema_id` is not yet loaded.
    pub fn get_field_descriptor(&self, schema_id: SchemaId) -> Option<Arc<StripeFieldDescriptor>> {
        self.0.get_field_descriptor(schema_id)
    }

    /// Fetches the `StripeFieldDescriptor` for the given `schema_id`.
    ///
    /// This method completes immediately if the field descriptor is already loaded.
    pub fn fetch_field_descriptor(
        &self,
        schema_id: SchemaId,
    ) -> Result<Arc<StripeFieldDescriptor>> {
        self.0.fetch_field_descriptor(schema_id)
    }

    /// Retrieves the stripe properties with convenient typed access.
    ///
    /// This method returns a `StripePropertyBag` that provides convenient access
    /// to stripe-level properties and custom metadata. The properties are loaded
    /// from storage if not already cached.
    ///
    /// # Returns
    ///
    /// A `StripePropertyBag` providing typed access to stripe properties.
    ///
    /// # Errors
    ///
    /// Returns an error if the properties cannot be loaded from storage.
    pub fn fetch_properties(&self) -> Result<StripePropertyBag> {
        self.0.fetch_properties()
    }

    /// Retrieves the stripe-level index collection.
    ///
    /// This method returns metadata about indexes available at the stripe level.
    ///
    /// # Returns
    ///
    /// A reference to the loaded `StripeIndexCollection`.
    ///
    /// # Errors
    ///
    /// Returns an error if the index collection cannot be loaded from storage.
    pub fn fetch_indexes(&self) -> Result<&StripeIndexCollection> {
        self.0.fetch_indexes()
    }
}
