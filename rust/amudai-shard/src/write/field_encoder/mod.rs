//! Field encoder, responsible for the creation of the `DataEncoding` element
//! of the shard format.

mod boolean;
mod bytes;
mod decimal;
mod dictionary;
mod empty;
mod list_container;
mod primitive;
mod unit_container;

use std::sync::Arc;

use arrow_array::Array;

use amudai_blockstream::write::PreparedEncodedBuffer;
use amudai_common::Result;
use amudai_encodings::block_encoder::BlockEncodingProfile;
use amudai_format::defs::schema_ext::{BasicTypeDescriptor, KnownExtendedType};
use amudai_format::defs::shard;
use amudai_format::schema::BasicType;
use amudai_io::temp_file_store::TemporaryFileStore;

use super::artifact_writer::ArtifactWriter;

pub(crate) use dictionary::ValueDictionaryHeader;

// Re-export field encoders for testing
#[cfg(test)]
pub(crate) use boolean::BooleanFieldEncoder;
#[cfg(test)]
pub(crate) use bytes::{BytesFieldEncoder, BytesStatsCollector};
#[cfg(test)]
pub(crate) use decimal::DecimalFieldEncoder;
#[cfg(test)]
pub(crate) use dictionary::PreparedDictionary;
#[cfg(test)]
pub(crate) use primitive::PrimitiveFieldEncoder;

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
    pub(crate) fn create_encoder(params: &FieldEncoderParams) -> Result<Box<dyn FieldEncoderOps>> {
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
            BasicType::Binary | BasicType::String | BasicType::Guid => {
                if params.dictionary_encoding == DictionaryEncoding::Enabled {
                    dictionary::AdaptiveDictionaryFieldEncoder::create(params)
                } else {
                    bytes::BytesFieldEncoder::create(params)
                }
            }
            BasicType::FixedSizeBinary => {
                // Check if this is a decimal type
                if basic_type.extended_type == KnownExtendedType::KustoDecimal {
                    decimal::DecimalFieldEncoder::create(params)
                } else if params.dictionary_encoding == DictionaryEncoding::Enabled {
                    dictionary::AdaptiveDictionaryFieldEncoder::create(params)
                } else {
                    bytes::BytesFieldEncoder::create(params)
                }
            }
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
    /// Statistics collected during encoding
    pub statistics: EncodedFieldStatistics,
    /// The size of the dictionary, if dictionary encoding was used.
    pub dictionary_size: Option<u64>,
    /// If present, indicates that all values in the field are constant.
    /// This allows optimizing storage by avoiding buffers for constant fields.
    pub constant_value: Option<amudai_format::defs::common::AnyValue>,
}

impl EncodedField {
    /// Creates a field decoder from the transient encoded field.
    ///
    /// This method constructs a [`FieldDecoder`] that can read back the data that was
    /// encoded in this `EncodedField`. It analyzes the field's type and encoding
    /// characteristics to instantiate the appropriate decoder implementation.
    ///
    /// # Arguments
    ///
    /// * `basic_type` - The basic type descriptor that defines the field's data type
    ///   and characteristics. This must match the type used during encoding.
    /// * `positions` - The total number of logical positions in the field. This is
    ///   used for validation and to properly configure presence/nullability handling.
    ///
    /// # Returns
    ///
    /// A `Result` containing the appropriate [`FieldDecoder`] variant for this field type,
    /// or an error if the decoder cannot be created.
    ///
    /// [`FieldDecoder`]: crate::read::field_decoder::FieldDecoder
    /// [`DictionaryFieldDecoder`]: crate::read::field_decoder::dictionary::DictionaryFieldDecoder
    pub fn create_decoder(
        &self,
        basic_type: BasicTypeDescriptor,
        positions: u64,
    ) -> Result<crate::read::field_decoder::FieldDecoder> {
        use crate::read::field_decoder::{
            FieldDecoder, boolean::BooleanFieldDecoder, bytes::BytesFieldDecoder,
            dictionary::DictionaryFieldDecoder, list::ListFieldDecoder,
            primitive::PrimitiveFieldDecoder, unit::StructFieldDecoder,
        };

        if self.is_dictionary_encoded() {
            let decoder = DictionaryFieldDecoder::from_encoded_field(self, basic_type)?;
            return Ok(FieldDecoder::Dictionary(decoder));
        }

        match basic_type.basic_type {
            BasicType::Unit => (),
            BasicType::Boolean => {
                let decoder = BooleanFieldDecoder::from_encoded_field(self, basic_type, positions)?;
                return Ok(FieldDecoder::Boolean(decoder));
            }
            BasicType::Int8
            | BasicType::Int16
            | BasicType::Int32
            | BasicType::Int64
            | BasicType::Float32
            | BasicType::Float64
            | BasicType::DateTime => {
                let decoder = PrimitiveFieldDecoder::from_encoded_field(self, basic_type)?;
                return Ok(FieldDecoder::Primitive(decoder));
            }
            BasicType::Binary
            | BasicType::FixedSizeBinary
            | BasicType::String
            | BasicType::Guid => {
                let decoder = BytesFieldDecoder::from_encoded_field(self, basic_type)?;
                return Ok(FieldDecoder::Bytes(decoder));
            }
            BasicType::List | BasicType::Map => {
                let decoder = ListFieldDecoder::from_encoded_field(self, basic_type)?;
                return Ok(FieldDecoder::List(decoder));
            }
            BasicType::FixedSizeList | BasicType::Struct => {
                let decoder = StructFieldDecoder::from_encoded_field(self, basic_type, positions)?;
                return Ok(FieldDecoder::Struct(decoder));
            }
            BasicType::Union => (),
        }

        Err(amudai_common::error::Error::not_implemented(format!(
            "Field decoder for {basic_type:?} is not yet implemented"
        )))
    }

    /// Locates the encoded buffer of the specified kind.
    ///
    /// This method searches through the field's encoded buffers to find one with the
    /// specified [`BufferKind`]. Each encoded field typically contains multiple buffers
    /// serving different purposes (data, presence, offsets, etc.), and this method
    /// provides a convenient way to access a specific buffer type.
    ///
    /// # Arguments
    ///
    /// * `kind` - The type of buffer to locate.
    ///
    /// # Returns
    ///
    /// A `Result` containing a reference to the [`PreparedEncodedBuffer`] with the
    /// specified kind, or an error if no buffer of that kind exists.
    ///
    /// [`PreparedEncodedBuffer`]: amudai_blockstream::write::PreparedEncodedBuffer
    pub fn get_encoded_buffer(&self, kind: shard::BufferKind) -> Result<&PreparedEncodedBuffer> {
        self.buffers
            .iter()
            .find(|buf| buf.descriptor.kind == kind as i32)
            .ok_or_else(|| {
                amudai_common::error::Error::invalid_arg(
                    "kind",
                    format!("Encoded buffer '{kind:?}' not found"),
                )
            })
    }

    /// Returns true if this is a dictionary encoded field.
    ///
    /// This method determines whether the field uses dictionary encoding by checking
    /// for the presence of a [`BufferKind::ValueDictionary`] buffer.
    ///
    /// # Returns
    ///
    /// - `true` if the field contains a `ValueDictionary` buffer, indicating dictionary
    ///   encoding
    /// - `false` if no dictionary buffer is present, indicating standard encoding
    ///
    /// [`BufferKind::ValueDictionary`]: amudai_format::defs::shard::BufferKind::ValueDictionary
    pub fn is_dictionary_encoded(&self) -> bool {
        self.get_encoded_buffer(shard::BufferKind::ValueDictionary)
            .is_ok()
    }

    /// Creates a new EncodedField with automatic constant value optimization.
    /// If the statistics indicate all values are constant, buffers are omitted for optimization.
    pub fn new(
        buffers: Vec<PreparedEncodedBuffer>,
        statistics: EncodedFieldStatistics,
        dictionary_size: Option<u64>,
    ) -> Self {
        // Check for constant values and optimize storage
        let constant_value = statistics.try_get_constant();

        EncodedField {
            buffers,
            statistics,
            dictionary_size,
            constant_value,
        }
    }
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
    /// Statistics for decimal types
    Decimal(amudai_data_stats::decimal::DecimalStats),
    /// Statistics for floating-point types
    Floating(amudai_data_stats::floating::FloatingStats),
    /// No statistics available for this field
    Missing,
}

impl EncodedFieldStatistics {
    /// Returns true if statistics are present (not Missing)
    pub fn is_present(&self) -> bool {
        !matches!(self, EncodedFieldStatistics::Missing)
    }

    /// Returns true if no statistics are available
    pub fn is_missing(&self) -> bool {
        matches!(self, EncodedFieldStatistics::Missing)
    }

    /// Detects if this field has a constant value based on the statistics.
    /// Returns Some(AnyValue) if all non-null values are the same, or if all values are null.
    /// Returns None if the field has varying values.
    pub fn try_get_constant(&self) -> Option<amudai_format::defs::common::AnyValue> {
        match self {
            EncodedFieldStatistics::Primitive(stats) => stats.try_get_constant(),
            EncodedFieldStatistics::Boolean(stats) => stats.try_get_constant(),
            EncodedFieldStatistics::Floating(stats) => stats.try_get_constant(),
            EncodedFieldStatistics::Decimal(stats) => stats.try_get_constant(),
            EncodedFieldStatistics::String(stats) => stats.try_get_constant(),
            EncodedFieldStatistics::Binary(stats) => stats.try_get_constant(),
            EncodedFieldStatistics::Missing => None,
        }
    }
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
            assert_eq!(data_size % 8, 0);
            assert_eq!(block_map_size % 8, 0);
            writer.align(amudai_format::defs::ENCODED_BUFFER_ARTIFACT_ALIGNMENT)?;
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

    /// Whether to attempt dictionary encoding for this field.
    ///
    /// This is typically enabled for string and binary types
    /// where dictionary compression can significantly reduce storage size
    /// and improve performance.
    pub dictionary_encoding: DictionaryEncoding,
}

/// Controls whether dictionary encoding is enabled for a field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DictionaryEncoding {
    Enabled,
    Disabled,
}

pub(crate) trait FieldEncoderOps: Send + Sync + 'static {
    fn push_array(&mut self, array: Arc<dyn Array>) -> Result<()>;

    fn push_nulls(&mut self, count: usize) -> Result<()>;

    fn finish(self: Box<Self>) -> Result<EncodedField>;
}
