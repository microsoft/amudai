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
    basic_type: BasicTypeDescriptor,
    inner: Box<dyn FieldEncoderOps>,
    logical_len: usize,
}

impl FieldEncoder {
    /// Creates a default field encoder for the specified basic type.
    pub fn new(
        basic_type: BasicTypeDescriptor,
        temp_store: Arc<dyn TemporaryFileStore>,
    ) -> Result<FieldEncoder> {
        Ok(FieldEncoder {
            basic_type,
            inner: Self::create_encoder(basic_type, temp_store)?,
            logical_len: 0,
        })
    }

    /// Basic data type of the encoder.
    pub fn basic_type(&self) -> BasicTypeDescriptor {
        self.basic_type
    }

    /// Current number of populated logical slots.
    pub fn logical_len(&self) -> usize {
        self.logical_len
    }

    pub fn is_empty(&self) -> bool {
        self.logical_len() == 0
    }

    pub fn push_array(&mut self, array: Arc<dyn Array>) -> Result<()> {
        let len = array.len();
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
    fn create_encoder(
        basic_type: BasicTypeDescriptor,
        temp_store: Arc<dyn TemporaryFileStore>,
    ) -> Result<Box<dyn FieldEncoderOps>> {
        match basic_type.basic_type {
            BasicType::Unit => Ok(empty::EmptyFieldEncoder::create()),
            BasicType::Boolean => boolean::BooleanFieldEncoder::create(basic_type, temp_store),
            BasicType::Int8 => {
                let arrow_type = if basic_type.signed {
                    arrow_schema::DataType::Int8
                } else {
                    arrow_schema::DataType::UInt8
                };
                primitive::PrimitiveFieldEncoder::create(basic_type, arrow_type, temp_store)
            }
            BasicType::Int16 => {
                let arrow_type = if basic_type.signed {
                    arrow_schema::DataType::Int16
                } else {
                    arrow_schema::DataType::UInt16
                };
                primitive::PrimitiveFieldEncoder::create(basic_type, arrow_type, temp_store)
            }
            BasicType::Int32 => {
                let arrow_type = if basic_type.signed {
                    arrow_schema::DataType::Int32
                } else {
                    arrow_schema::DataType::UInt32
                };
                primitive::PrimitiveFieldEncoder::create(basic_type, arrow_type, temp_store)
            }
            BasicType::Int64 => {
                let arrow_type = if basic_type.signed {
                    arrow_schema::DataType::Int64
                } else {
                    arrow_schema::DataType::UInt64
                };
                primitive::PrimitiveFieldEncoder::create(basic_type, arrow_type, temp_store)
            }
            BasicType::Float32 => primitive::PrimitiveFieldEncoder::create(
                basic_type,
                arrow_schema::DataType::Float32,
                temp_store,
            ),
            BasicType::Float64 => primitive::PrimitiveFieldEncoder::create(
                basic_type,
                arrow_schema::DataType::Float64,
                temp_store,
            ),
            BasicType::DateTime => {
                // TODO: provide a "normalizer" function to convert the source arrays to DateTime ticks.
                primitive::PrimitiveFieldEncoder::create(
                    basic_type,
                    arrow_schema::DataType::Int64,
                    temp_store,
                )
            }
            BasicType::Binary
            | BasicType::FixedSizeBinary
            | BasicType::String
            | BasicType::Guid => bytes::BytesFieldEncoder::create(basic_type, temp_store),
            BasicType::List | BasicType::Map => {
                list_container::ListContainerFieldEncoder::create(basic_type, temp_store)
            }
            BasicType::FixedSizeList | BasicType::Struct => {
                unit_container::UnitContainerFieldEncoder::create(basic_type, temp_store)
            }
            BasicType::Union => todo!(),
        }
    }
}

/// Represents an encoded stripe field as a collection of encoded buffers,
/// which are ready to be written into the final artifact in the object store.
/// `EncodedField` corresponds to the `DataEncoding` element in the shard format,
/// in its "uncommitted" state.
#[derive(Debug)]
pub struct EncodedField {
    pub buffers: Vec<PreparedEncodedBuffer>,
}

impl EncodedField {
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

trait FieldEncoderOps: Send + Sync + 'static {
    fn push_array(&mut self, array: Arc<dyn Array>) -> Result<()>;

    fn push_nulls(&mut self, count: usize) -> Result<()>;

    fn finish(self: Box<Self>) -> Result<EncodedField>;
}
