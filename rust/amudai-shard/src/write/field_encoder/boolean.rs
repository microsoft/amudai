//! Boolean buffer and field encoders, essentially bitmap builders.

use std::sync::Arc;

use amudai_blockstream::write::bit_buffer::{BitBufferEncoder, EncodedBitBuffer};
use amudai_common::{Result, error::Error};
use amudai_format::{defs::shard, schema::BasicType};
use arrow_array::{Array, cast::AsArray};

use crate::write::field_encoder::FieldEncoderParams;

use super::{EncodedField, FieldEncoderOps};

/// Encoder for boolean fields that handles both boolean values and their presence (null/non-null).
///
/// Uses two separate `BitBufferEncoder` instances:
/// - `values_encoder`: Encodes the actual boolean values (true/false)
/// - `presence_encoder`: Encodes whether each position has a value or is null
pub struct BooleanFieldEncoder {
    params: FieldEncoderParams,
    values_encoder: BitBufferEncoder,
    presence_encoder: BitBufferEncoder,
}

impl BooleanFieldEncoder {
    /// Creates a `BooleanFieldEncoder`.
    pub fn create(params: &FieldEncoderParams) -> Result<Box<dyn FieldEncoderOps>> {
        assert_eq!(params.basic_type.basic_type, BasicType::Boolean);
        let temp_store = params.temp_store.clone();
        Ok(Box::new(BooleanFieldEncoder {
            params: params.clone(),
            values_encoder: BitBufferEncoder::new(temp_store.clone(), params.encoding_profile),
            presence_encoder: BitBufferEncoder::new(temp_store, params.encoding_profile),
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
            EncodedBitBuffer::Constant(value, count) => {
                // TODO: Consider optimizing constant boolean values
                // For now, we'll still create a buffer even for constants
                // This should be optimized in the future by storing the constant in the field descriptor
                let mut values_buffer =
                    BitBufferEncoder::encode_blocks(count, value, self.params.temp_store.clone())?;
                values_buffer.descriptor.kind = shard::BufferKind::Data as i32;
                values_buffer.descriptor.embedded_presence = false;
                values_buffer.descriptor.embedded_offsets = false;
                buffers.push(values_buffer);
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
            EncodedBitBuffer::Constant(value, count) => {
                if !value {
                    // All values are null
                    // TODO: mark the field as constant null in the field descriptor
                    let mut presence_buffer = BitBufferEncoder::encode_blocks(
                        count,
                        value,
                        self.params.temp_store.clone(),
                    )?;
                    presence_buffer.descriptor.kind = shard::BufferKind::Data as i32;
                    presence_buffer.descriptor.embedded_presence = false;
                    presence_buffer.descriptor.embedded_offsets = false;
                    buffers.push(presence_buffer);
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

        Ok(EncodedField {
            buffers,
            statistics: None, // Boolean fields don't collect primitive statistics
        })
    }
}
