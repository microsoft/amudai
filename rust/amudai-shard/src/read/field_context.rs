//! Field context (field's data type, its descriptor, and the stripe it belongs to).

use std::sync::Arc;

use amudai_common::{Result, error::Error};
use amudai_format::{
    defs::{common::DataRef, shard},
    schema::{self, BasicType, SchemaId},
};

use super::{
    artifact_reader::ArtifactReader,
    field_decoder::{
        FieldDecoder, boolean::BooleanFieldDecoder, bytes::BytesFieldDecoder,
        list::ListFieldDecoder, primitive::PrimitiveFieldDecoder, unit::StructFieldDecoder,
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
            BasicType::Boolean => {
                let decoder = BooleanFieldDecoder::from_field(self)?;
                return Ok(FieldDecoder::Boolean(decoder));
            }
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
            BasicType::FixedSizeList | BasicType::Struct => {
                let decoder = StructFieldDecoder::from_field(self)?;
                return Ok(FieldDecoder::Struct(decoder));
            }
            BasicType::Map => (),
            BasicType::Union => (),
        }

        Err(Error::not_implemented(format!(
            "Field decoder for {basic_type:?} is not yet implemented"
        )))
    }

    /// Returns a slice of `EncodedBuffer` descriptors for this field's data.
    ///
    /// This method retrieves metadata about the encoded buffers that store this field's data
    /// within the stripe. Each buffer descriptor contains information about a specific aspect
    /// of the field's encoding (e.g., values, nullability, offsets).
    ///
    /// # Buffer Types
    ///
    /// Fields can have different types of buffers depending on their data type and encoding:
    ///
    /// * **Data Buffer** (`BufferKind::Data`): Contains the primary sequence of encoded values
    /// * **Presence Buffer** (`BufferKind::Presence`): Stores nullability information as a
    ///   compressed bitmap indicating which positions contain valid (non-null) values
    /// * **Offsets Buffer** (`BufferKind::Offsets`): For variable-sized types (strings, binary),
    ///   stores the end-of-value offsets defining byte ranges in the data buffer
    /// * **Value Dictionary** (`BufferKind::ValueDictionary`): For dictionary-encoded fields,
    ///   maps unique integer IDs to distinct field values
    /// * **Opaque Dictionary** (`BufferKind::OpaqueDictionary`): Shared compression dictionary
    ///   used by block compressors across multiple blocks
    ///
    /// # Expected Buffer Count
    ///
    /// Most field types typically have 1-2 buffers:
    /// * **Primitive types** (int, float, boolean): 1 data buffer (presence may be embedded)
    /// * **Variable-sized types** (string, binary): 1 data buffer (offsets typically embedded)
    /// * **Nullable fields**: May have an additional presence buffer if not embedded
    /// * **Struct/FixedSizeList**: 0-1 presence buffers (no data buffer, only nullability)
    /// * **List fields**: 1 offsets buffer (plus optional presence buffer)
    ///
    /// # Buffer Descriptors
    ///
    /// Each `EncodedBuffer` descriptor contains:
    /// * `kind`: The buffer's role in the encoding scheme
    /// * `buffer`: Reference to the actual encoded data in storage
    /// * `block_map`: Optional reference to block metadata for compressed data
    /// * `embedded_presence`/`embedded_offsets`: Whether auxiliary data is embedded in blocks
    /// * Block compression and checksum information
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * Data encodings are missing for the field
    /// * The data encoding is missing or not a native encoding (external formats not supported)
    ///
    /// # Note
    ///
    /// This method currently only supports native Amudai encodings. External encodings
    /// (such as Parquet) will return an error.
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
                ));
            }
        };
        Ok(&native_encoding.buffers)
    }
}
