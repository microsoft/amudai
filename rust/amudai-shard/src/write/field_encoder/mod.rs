//! Field encoder, responsible for the creation of the `DataEncoding` element
//! of the shard format.

mod boolean;
mod bytes;
mod empty;
mod list_container;
mod primitive;
mod unit_container;

use std::sync::Arc;

use amudai_blockstream::write::PreparedEncodedBuffer;
use amudai_common::Result;
use amudai_encodings::block_encoder::BlockEncodingProfile;
use amudai_format::defs::shard;
use amudai_format::{defs::schema_ext::BasicTypeDescriptor, schema::BasicType};
use amudai_io::temp_file_store::TemporaryFileStore;
use arrow_array::Array;

use super::artifact_writer::ArtifactWriter;

/// A `FieldEncoder` is tasked with encoding a sequence of values for a specific
/// field within a stripe, independently of any child fields.
/// The result of the field encoder is a temporary `DataEncoding` element for that
/// field, which contains one or more `EncodedBuffers`. This `DataEncoding` is
/// subsequently incorporated into the field descriptor, and the buffers are written
/// to permanent storage.
pub struct FieldEncoder {
    params: FieldEncoderParams,
    inner: Box<dyn FieldEncoderOps>,
    logical_len: usize,
}

impl FieldEncoder {
    /// Creates a default field encoder for the specified basic type.
    pub fn new(params: FieldEncoderParams) -> Result<FieldEncoder> {
        Ok(FieldEncoder {
            inner: Self::create_encoder(&params)?,
            params,
            logical_len: 0,
        })
    }

    /// Basic data type of the encoder.
    pub fn basic_type(&self) -> BasicTypeDescriptor {
        self.params.basic_type
    }

    /// Current number of populated logical slots.
    pub fn logical_len(&self) -> usize {
        self.logical_len
    }

    pub fn is_empty(&self) -> bool {
        self.logical_len() == 0
    }

    pub fn push_array(&mut self, array: Arc<dyn Array>) -> Result<()> {
        let len = self.get_logical_len(&array);
        self.inner.push_array(array)?;
        self.logical_len += len;
        Ok(())
    }

    pub fn push_nulls(&mut self, count: usize) -> Result<()> {
        self.inner.push_nulls(count)?;
        self.logical_len += count;
        Ok(())
    }

    pub fn finish(self) -> Result<EncodedField> {
        self.inner.finish()
    }
}

impl FieldEncoder {
    fn create_encoder(params: &FieldEncoderParams) -> Result<Box<dyn FieldEncoderOps>> {
        let basic_type = params.basic_type;
        match basic_type.basic_type {
            BasicType::Unit => Ok(empty::EmptyFieldEncoder::create()),
            BasicType::Boolean => boolean::BooleanFieldEncoder::create(params),
            BasicType::Int8 => {
                let arrow_type = if basic_type.signed {
                    arrow_schema::DataType::Int8
                } else {
                    arrow_schema::DataType::UInt8
                };
                primitive::PrimitiveFieldEncoder::create(params, arrow_type)
            }
            BasicType::Int16 => {
                let arrow_type = if basic_type.signed {
                    arrow_schema::DataType::Int16
                } else {
                    arrow_schema::DataType::UInt16
                };
                primitive::PrimitiveFieldEncoder::create(params, arrow_type)
            }
            BasicType::Int32 => {
                let arrow_type = if basic_type.signed {
                    arrow_schema::DataType::Int32
                } else {
                    arrow_schema::DataType::UInt32
                };
                primitive::PrimitiveFieldEncoder::create(params, arrow_type)
            }
            BasicType::Int64 => {
                let arrow_type = if basic_type.signed {
                    arrow_schema::DataType::Int64
                } else {
                    arrow_schema::DataType::UInt64
                };
                primitive::PrimitiveFieldEncoder::create(params, arrow_type)
            }
            BasicType::Float32 => {
                primitive::PrimitiveFieldEncoder::create(params, arrow_schema::DataType::Float32)
            }
            BasicType::Float64 => {
                primitive::PrimitiveFieldEncoder::create(params, arrow_schema::DataType::Float64)
            }
            BasicType::DateTime => {
                primitive::PrimitiveFieldEncoder::create(params, arrow_schema::DataType::UInt64)
            }
            BasicType::Binary
            | BasicType::FixedSizeBinary
            | BasicType::String
            | BasicType::Guid => bytes::BytesFieldEncoder::create(params),
            BasicType::List | BasicType::Map => {
                list_container::ListContainerFieldEncoder::create(params)
            }
            BasicType::FixedSizeList | BasicType::Struct => {
                unit_container::UnitContainerFieldEncoder::create(params)
            }
            BasicType::Union => todo!("Implement union encoding support"),
        }
    }

    fn get_logical_len(&self, array: &dyn Array) -> usize {
        use arrow_schema::DataType;
        let len = array.len();
        if matches!(
            self.basic_type().basic_type,
            BasicType::List | BasicType::Map
        ) && matches!(array.data_type(), DataType::Int32 | DataType::Int64)
        {
            // List offsets supplied as a plain integer array, the actual logical
            // length is one less the number of offsets.
            assert_ne!(len, 0);
            len - 1
        } else {
            len
        }
    }
}

/// Represents an encoded stripe field as a collection of encoded buffers,
/// which are ready to be written into the final artifact in the object store.
/// `EncodedField` corresponds to the `DataEncoding` element in the shard format,
/// in its "uncommitted" state.
#[must_use]
#[derive(Debug)]
pub struct EncodedField {
    pub buffers: Vec<PreparedEncodedBuffer>,
    /// Optional statistics collected during encoding for primitive types
    pub statistics: Option<EncodedFieldStatistics>,
}

/// Statistics collected during field encoding
#[derive(Debug, Clone)]
pub enum EncodedFieldStatistics {
    /// Statistics for primitive types (integers, floats, datetime)
    Primitive(amudai_data_stats::primitive::PrimitiveStats),
    /// Statistics for string types
    String(amudai_data_stats::string::StringStats),
    /// Statistics for boolean types
    Boolean(amudai_data_stats::boolean::BooleanStats),
    /// Statistics for binary types
    Binary(amudai_data_stats::binary::BinaryStats),
}

impl EncodedField {
    /// Finalizes this field encoding by writing all prepared buffers to the artifact writer
    /// and constructing the corresponding metadata descriptor.
    ///
    /// This method consumes the `EncodedField`, writing each `PreparedEncodedBuffer` to the
    /// provided writer and transforming them into `EncodedBuffer` descriptors with proper
    /// data references. The result is a `DataEncoding` with the `Native` variant containing
    /// all buffer metadata needed for the shard format.
    ///
    /// # Arguments
    ///
    /// * `writer` - A mutable reference to an `ArtifactWriter` where the encoded data
    ///   will be written (typically a stripe storage blob).
    ///
    /// # Returns
    ///
    /// A `shard::DataEncoding` with the `Native` variant containing:
    /// - Buffer descriptors with data references pointing to the written data
    /// - Block map references when applicable
    ///
    /// # Errors
    ///
    /// Returns an error if writing any buffer data to the artifact writer fails.
    ///
    /// # Notes
    ///
    /// - Empty buffers (where `data_size + block_map_size == 0`) are preserved in the
    ///   metadata but no actual data is written to the artifact
    /// - Block maps, when present, are written immediately following their associated data
    pub fn seal(self, writer: &mut ArtifactWriter) -> Result<shard::DataEncoding> {
        let mut buffers = Vec::with_capacity(self.buffers.len());
        for buffer in self.buffers {
            let buffer = Self::write_buffer(buffer, writer)?;
            buffers.push(buffer);
        }

        Ok(shard::DataEncoding {
            kind: Some(shard::data_encoding::Kind::Native(
                shard::NativeDataEncoding {
                    buffers,
                    packed_group: false,
                },
            )),
        })
    }

    fn write_buffer(
        buffer: PreparedEncodedBuffer,
        writer: &mut ArtifactWriter,
    ) -> Result<shard::EncodedBuffer> {
        let PreparedEncodedBuffer {
            mut descriptor,
            data,
            data_size,
            block_map_size,
        } = buffer;
        if data_size + block_map_size != 0 {
            let data_ref = writer.write_from_slice(&data, 0..data_size + block_map_size)?;
            if block_map_size != 0 {
                // Block map follows the data.
                let block_map_ref = data_ref.slice_from_offset(data_size);
                descriptor.block_map = Some(block_map_ref);
            }
            descriptor.buffer = Some(data_ref);
        }
        Ok(descriptor)
    }
}

/// Configuration parameters for field encoding operations.
///
/// `FieldEncoderParams` provides the essential configuration needed to create and operate
/// a [`FieldEncoder`]. It encapsulates the field's data type characteristics, storage
/// requirements, and encoding preferences that determine how field data will be encoded
/// into the shard format.
///
/// This struct is typically created by higher-level components (such as [`crate::write::shard_builder::ShardBuilder`])
/// and passed to [`FieldEncoder::new()`] to configure field-specific encoding behavior.
///
/// # Field Encoding Process
///
/// During shard construction, each field in the schema requires its own encoder configured
/// with appropriate parameters:
/// 1. The `basic_type` determines which specific encoder implementation to use
/// 2. The `temp_store` provides scratch space for intermediate encoding operations
/// 3. The `encoding_profile` controls the compression vs. speed trade-offs
///
/// Statistics collection is always enabled for primitive field types and provides
/// valuable metadata (min/max values, null counts, NaN counts) that gets stored
/// in field descriptors for optimization purposes.
///
/// # See Also
///
/// - [`FieldEncoder`] - The encoder that uses these parameters
/// - [`BasicTypeDescriptor`] - Detailed type information and constraints
/// - [`BlockEncodingProfile`] - Available compression profiles
/// - [`crate::write::shard_builder::ShardBuilderParams`] - Higher-level configuration
#[derive(Clone)]
pub struct FieldEncoderParams {
    /// The basic type descriptor that defines the fundamental characteristics of the field.
    pub basic_type: BasicTypeDescriptor,

    /// Temporary file storage provider for intermediate encoding operations.
    ///
    /// During field encoding, the encoder may need to buffer large amounts of data
    /// or perform multi-pass operations that require temporary storage. The `temp_store`
    /// provides this capability through either in-memory buffers or temporary files
    /// on disk, depending on the implementation.
    pub temp_store: Arc<dyn TemporaryFileStore>,
    /// Block encoding profile that controls the trade-off between compression ratio
    /// and performance.
    /// See [`ShardBuilderParams::encoding_profile`](`crate::write::shard_builder::ShardBuilderParams::encoding_profile`)
    pub encoding_profile: BlockEncodingProfile,
}

pub(crate) trait FieldEncoderOps: Send + Sync + 'static {
    fn push_array(&mut self, array: Arc<dyn Array>) -> Result<()>;

    fn push_nulls(&mut self, count: usize) -> Result<()>;

    fn finish(self: Box<Self>) -> Result<EncodedField>;
}
