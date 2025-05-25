//! Field context (field's data type, its descriptor, and the stripe it belongs to).

use std::sync::Arc;

use amudai_common::{error::Error, Result};
use amudai_format::{
    defs::{common::DataRef, shard},
    schema::{self, BasicType, SchemaId},
};

use super::{
    artifact_reader::ArtifactReader,
    field_decoder::{
        bytes::BytesFieldDecoder, list::ListFieldDecoder, primitive::PrimitiveFieldDecoder,
        FieldDecoder,
    },
    stripe::StripeFieldDescriptor,
    stripe_context::StripeContext,
};

/// Represents the context for a specific field within a stripe.
///
/// This struct holds information about the field's data type, its descriptor,
/// and the stripe it belongs to. It provides methods to access this information
/// and to create decoders for reading the field's data.
pub struct FieldContext {
    /// The data type "subtree" of the field.
    data_type: schema::DataType,
    /// The descriptor for the field within the stripe, along with an anchor
    /// (storage artifact containing this descriptor).
    descriptor: Arc<StripeFieldDescriptor>,
    /// The context of the stripe to which this field belongs.
    stripe: Arc<StripeContext>,
}

impl FieldContext {
    pub fn new(
        data_type: schema::DataType,
        descriptor: Arc<StripeFieldDescriptor>,
        stripe: Arc<StripeContext>,
    ) -> FieldContext {
        FieldContext {
            data_type,
            descriptor,
            stripe,
        }
    }

    /// Returns a reference to the data type of this field.
    ///
    /// The data type contains information about the field's name, type,
    /// and nested structure.
    pub fn data_type(&self) -> &schema::DataType {
        &self.data_type
    }

    /// Returns the schema ID of this field.
    ///
    /// Schema IDs uniquely identify fields within a schema.
    pub fn schema_id(&self) -> SchemaId {
        self.data_type.schema_id().expect("schema_id")
    }

    /// Returns a reference to the stripe field descriptor for this field.
    ///
    /// The descriptor contains metadata about the field's representation
    /// in the stripe, such as logical position count, encoding and statistics.
    pub fn descriptor(&self) -> &Arc<StripeFieldDescriptor> {
        &self.descriptor
    }

    /// Returns the total number of logical positions (value slots) in the stripe field.
    pub fn position_count(&self) -> u64 {
        self.descriptor()
            .field
            .as_ref()
            .expect("field descriptor")
            .position_count
    }

    /// Returns a reference to the stripe context that contains this field.
    pub fn stripe(&self) -> &Arc<StripeContext> {
        &self.stripe
    }

    /// Opens an `ArtifactReader` for the given `DataRef` within the context of this field.
    ///
    /// The `DataRef` is resolved relative to the anchor URL of the field's descriptor.
    ///
    /// # Arguments
    ///
    /// * `data_ref`: A reference to the `DataRef` pointing to the data to be read.
    ///
    /// # Errors
    ///
    /// Returns an error if the `DataRef` cannot be opened or resolved.
    pub fn open_data_ref(&self, data_ref: &DataRef) -> Result<ArtifactReader> {
        self.stripe()
            .shard()
            .open_data_ref(data_ref, Some(self.descriptor.anchor()))
    }

    /// Creates a `FieldDecoder` for this field.
    ///
    /// The type of decoder created depends on the basic type of the field.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * The stripe field descriptor is missing.
    /// * The field is entirely null or a constant null (not yet implemented).
    /// * The field is a constant value (not yet implemented).
    /// * A decoder for the field's basic type is not yet implemented.
    pub fn create_decoder(self: &Arc<Self>) -> Result<FieldDecoder> {
        let basic_type = self.data_type().describe()?;
        let descriptor = self
            .descriptor()
            .field
            .as_ref()
            .ok_or_else(|| Error::invalid_format("missing stripe field descriptor"))?;

        if descriptor.null_count == Some(descriptor.position_count)
            || descriptor
                .constant_value
                .as_ref()
                .is_some_and(|c| c.is_null())
        {
            return Err(Error::not_implemented("null field decoder"));
        }

        if descriptor.constant_value.is_some() {
            return Err(Error::not_implemented("constant field decoder"));
        }

        match basic_type.basic_type {
            BasicType::Unit => (),
            BasicType::Boolean => (),
            BasicType::Int8
            | BasicType::Int16
            | BasicType::Int32
            | BasicType::Int64
            | BasicType::Float32
            | BasicType::Float64
            | BasicType::DateTime => {
                let decoder = PrimitiveFieldDecoder::from_field(self)?;
                return Ok(FieldDecoder::Primitive(decoder));
            }
            BasicType::Binary
            | BasicType::FixedSizeBinary
            | BasicType::String
            | BasicType::Guid => {
                let decoder = BytesFieldDecoder::from_field(self)?;
                return Ok(FieldDecoder::Bytes(decoder));
            }
            BasicType::List => {
                let decoder = ListFieldDecoder::from_field(self)?;
                return Ok(FieldDecoder::List(decoder));
            }
            BasicType::FixedSizeList => (),
            BasicType::Struct => (),
            BasicType::Map => (),
            BasicType::Union => (),
        }

        Err(Error::not_implemented(format!(
            "Field decoder for {basic_type:?} is not yet implemented"
        )))
    }

    /// Returns a slice of `EncodedBuffer`s for this field's data.
    ///
    /// This method retrieves the buffer information from the field's data encoding.
    /// It currently only supports native encodings.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * Data encodings are missing for the field.
    /// * The data encoding is missing or not a native encoding.
    pub fn get_encoded_buffers(&self) -> Result<&[shard::EncodedBuffer]> {
        let data_encoding = self
            .descriptor
            .encodings
            .first()
            .ok_or_else(|| Error::invalid_format("missing data encodings"))?;

        let native_encoding = match &data_encoding.kind {
            Some(shard::data_encoding::Kind::Native(native)) => native,
            _ => {
                return Err(Error::not_implemented(
                    "Missing or non-native data encoding",
                ))
            }
        };
        Ok(&native_encoding.buffers)
    }
}
