//! Boolean buffer and field encoders, essentially bitmap builders.

use std::sync::Arc;

use amudai_blockstream::write::bit_buffer::{BitBufferEncoder, EncodedBitBuffer};
use amudai_common::{error::Error, Result};
use amudai_format::defs::{schema_ext::BasicTypeDescriptor, shard};
use amudai_io::temp_file_store::TemporaryFileStore;
use arrow_array::{cast::AsArray, Array};

use super::{EncodedField, FieldEncoderOps};

/// Encoder for boolean fields that handles both boolean values and their presence (null/non-null).
///
/// Uses two separate `BitBufferEncoder` instances:
/// - `values_encoder`: Encodes the actual boolean values (true/false)
/// - `presence_encoder`: Encodes whether each position has a value or is null
pub struct BooleanFieldEncoder {
    _basic_type: BasicTypeDescriptor,
    values_encoder: BitBufferEncoder,
    presence_encoder: BitBufferEncoder,
}

impl BooleanFieldEncoder {
    /// Creates a `BooleanFieldEncoder`.
    ///
    /// # Arguments
    ///
    /// * `basic_type`: The basic type descriptor.
    /// * `temp_store`: Temporary file store for intermediate storage.
    pub fn create(
        basic_type: BasicTypeDescriptor,
        temp_store: Arc<dyn TemporaryFileStore>,
    ) -> Result<Box<dyn FieldEncoderOps>> {
        Ok(Box::new(BooleanFieldEncoder {
            _basic_type: basic_type,
            values_encoder: BitBufferEncoder::new(temp_store.clone()),
            presence_encoder: BitBufferEncoder::new(temp_store),
        }))
    }
}

impl FieldEncoderOps for BooleanFieldEncoder {
    fn push_array(&mut self, array: Arc<dyn Array>) -> Result<()> {
        if !matches!(array.data_type(), arrow_schema::DataType::Boolean) {
            return Err(Error::invalid_arg(
                "array",
                format!("expected Boolean array, got {:?}", array.data_type()),
            ));
        }

        let boolean_array = array.as_boolean();

        // Append the boolean values
        self.values_encoder.append(boolean_array.values())?;

        // Append presence information (null/non-null)
        if let Some(nulls) = boolean_array.nulls() {
            self.presence_encoder.append(nulls.inner())?;
        } else {
            // All values are non-null
            self.presence_encoder.append_repeated(array.len(), true)?;
        }

        Ok(())
    }

    fn push_nulls(&mut self, count: usize) -> Result<()> {
        // For null values, we mark presence as false and append placeholder values
        self.presence_encoder.append_repeated(count, false)?;
        self.values_encoder.append_repeated(count, false)?; // Placeholder values for nulls
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<EncodedField> {
        let mut buffers = Vec::new();

        // Finish the values encoder
        match self.values_encoder.finish()? {
            EncodedBitBuffer::Constant(_value, _count) => {
                // TODO: Consider optimizing constant boolean values
                // For now, we'll still create a buffer even for constants
                // This should be optimized in the future by storing the constant in the field descriptor
            }
            EncodedBitBuffer::Blocks(mut values_buffer) => {
                values_buffer.descriptor.kind = shard::BufferKind::Data as i32;
                values_buffer.descriptor.embedded_presence = false;
                values_buffer.descriptor.embedded_offsets = false;
                buffers.push(values_buffer);
            }
        }

        // Finish the presence encoder
        match self.presence_encoder.finish()? {
            EncodedBitBuffer::Constant(value, _count) => {
                if !value {
                    // All values are null
                    // TODO: mark the field as constant null in the field descriptor
                    return Err(Error::not_implemented("constant null encoding (boolean)"));
                }

                // All values are non-null, no need to write a dedicated presence buffer
                // The absence of a presence buffer indicates all values are present
            }
            EncodedBitBuffer::Blocks(mut presence_buffer) => {
                presence_buffer.descriptor.kind = shard::BufferKind::Presence as i32;
                presence_buffer.descriptor.embedded_presence = false;
                presence_buffer.descriptor.embedded_offsets = false;
                buffers.push(presence_buffer);
            }
        }

        Ok(EncodedField { buffers })
    }
}
