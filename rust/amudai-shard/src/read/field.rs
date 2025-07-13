use std::sync::Arc;

use amudai_blockstream::read::block_stream::BlockStreamDecoder;
use amudai_bloom_filters::decoder::SbbfDecoder;
use amudai_common::Result;
use amudai_format::{
    defs::shard,
    schema::{self, SchemaId},
};
use amudai_io::ReadAt;

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

    /// Opens a child field at the specified index.
    pub fn open_child_at(&self, index: usize) -> Result<Field> {
        let child_type = self.data_type().child_at(index)?;
        self.get_stripe().open_field(child_type)
    }

    /// Opens a child field with the specified name.
    pub fn open_child(&self, name: &str) -> Result<Field> {
        let (_, child_type) = self.data_type().find_child(name)?.ok_or_else(|| {
            amudai_common::error::Error::invalid_arg(
                "field name",
                format!(
                    "Child field '{name}' of '{}' not found",
                    self.data_type().name().unwrap_or_default()
                ),
            )
        })?;
        self.get_stripe().open_field(child_type)
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

    /// Returns a slice of `EncodedBuffer` descriptors for this field's data.
    ///
    /// This method retrieves metadata about the encoded buffers that store this field's data
    /// within the stripe. Each buffer descriptor contains information about a specific aspect
    /// of the field's encoding (e.g., values, nullability, offsets).
    ///
    /// # Note
    ///
    /// This is a low-level API primarily useful for inspection and diagnostic purposes.
    /// For typical use cases involving reading field data, use [`create_decoder()`](Self::create_decoder)
    /// instead, which provides a higher-level interface for accessing field values.
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
        self.0.get_encoded_buffers()
    }

    /// Returns an `EncodedBuffer` descriptor of the specified kind for this field's data.
    pub fn get_encoded_buffer(&self, kind: shard::BufferKind) -> Result<&shard::EncodedBuffer> {
        self.0.get_encoded_buffer(kind)
    }

    /// Opens a buffer reader for the given encoded buffer.
    ///
    /// This method creates a reader that can access the data referenced by the
    /// encoded buffer within the context of this field's stripe.
    ///
    /// # Note
    ///
    /// This is a low-level API primarily useful for inspection and diagnostic purposes.
    /// For typical use cases involving reading field data, use [`create_decoder()`](Self::create_decoder)
    /// instead, which provides a higher-level interface for accessing field values.
    ///
    /// # Arguments
    ///
    /// * `encoded_buffer` - The encoded buffer descriptor containing metadata
    ///   about the buffer to open
    ///
    /// # Returns
    ///
    /// An Arc-wrapped reader that implements `ReadAt` for accessing the buffer data
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer reference cannot be opened or resolved.
    pub fn open_buffer(&self, encoded_buffer: &shard::EncodedBuffer) -> Result<Arc<dyn ReadAt>> {
        let data_ref = encoded_buffer.buffer.as_ref().ok_or_else(|| {
            amudai_common::error::Error::invalid_format("Missing buffer reference")
        })?;

        let reader = self.0.open_data_ref(data_ref)?;
        Ok(reader.into_inner())
    }

    /// Opens a block stream decoder for the given encoded buffer.
    ///
    /// This method creates a `BlockStreamDecoder` that can decode the block-structured
    /// data referenced by the encoded buffer.
    ///
    /// # Note
    ///
    /// This is a low-level API primarily useful for inspection and diagnostic purposes.
    /// For typical use cases involving reading field data, use [`create_decoder()`](Self::create_decoder)
    /// instead, which provides a higher-level interface for accessing field values.
    ///
    /// # Arguments
    ///
    /// * `encoded_buffer` - The encoded buffer descriptor containing metadata
    ///   about the block stream to decode
    ///
    /// # Returns
    ///
    /// An Arc-wrapped `BlockStreamDecoder` for accessing the encoded block data
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * The buffer reference is missing from the descriptor
    /// * The block stream cannot be created or decoded
    /// * The buffer data is invalid or corrupted
    pub fn open_block_stream(
        &self,
        encoded_buffer: &shard::EncodedBuffer,
    ) -> Result<Arc<BlockStreamDecoder>> {
        let reader = self.open_buffer(encoded_buffer)?;
        BlockStreamDecoder::from_encoded_buffer(reader, encoded_buffer)
    }

    /// Returns the bloom filter for this field, if present.
    pub fn get_bloom_filter(&self) -> Option<&SbbfDecoder> {
        self.0.get_bloom_filter()
    }
}
