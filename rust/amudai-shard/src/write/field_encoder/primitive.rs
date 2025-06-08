//! Primitive field encoder.

use std::sync::Arc;

use amudai_blockstream::write::{
    primitive_buffer::PrimitiveBufferEncoder, staging_buffer::PrimitiveStagingBuffer,
};
use amudai_common::Result;
use amudai_format::defs::schema_ext::BasicTypeDescriptor;
use amudai_io::temp_file_store::TemporaryFileStore;
use arrow_array::Array;

use super::{EncodedField, FieldEncoderOps};

/// Encoder implementation for primitive numeric fields: integers, FP, `DateTime`, `TimeSpan`.
///
/// The `PrimitiveFieldEncoder` utilizes a number of "canonical" data types
/// to standardize the accumulated values before passing them to the block encoder.
///
/// These canonical types are derived from the formal type of the field in the shard schema:
/// - Signed and unsigned integers with 8-, 16-, 32- and 64-bit width (where `Int64` is also
///   used to represent `DateTime` and `TimeSpan`).
/// - `Float32` and `Float64`.
///
/// The `basic_type` still represents the original formal type as defined
/// by the schema.
pub struct PrimitiveFieldEncoder {
    /// Staging buffer for accumulating values before flushing.
    staging: PrimitiveStagingBuffer,
    /// Descriptor of the basic type being encoded.
    _basic_type: BasicTypeDescriptor,
    /// The canonical Arrow data type used for casting accumulated values
    /// before passing them to the block encoder.
    normalized_arrow_type: arrow_schema::DataType,
    /// Value buffer encoder.
    buffer_encoder: PrimitiveBufferEncoder,
}

impl PrimitiveFieldEncoder {
    /// Creates a `PrimitiveFieldEncoder`.
    ///
    /// # Arguments
    ///
    /// * `basic_type`: The basic type descriptor.
    /// * `normalized_arrow_type`: The canonical Arrow data type used for casting
    ///   accumulated values before passing them to the block encoder.
    /// * `temp_store`: Temp store provider for the encoded buffers.
    pub fn create(
        basic_type: BasicTypeDescriptor,
        normalized_arrow_type: arrow_schema::DataType,
        temp_store: Arc<dyn TemporaryFileStore>,
    ) -> Result<Box<dyn FieldEncoderOps>> {
        Ok(Box::new(PrimitiveFieldEncoder {
            staging: PrimitiveStagingBuffer::new(normalized_arrow_type.clone()),
            _basic_type: basic_type,
            normalized_arrow_type,
            buffer_encoder: PrimitiveBufferEncoder::new(
                Default::default(),
                basic_type,
                temp_store,
            )?,
        }))
    }
}

impl PrimitiveFieldEncoder {
    /// Default (fallback) block value count when it cannot be inferred
    /// by the block encoder.
    const DEFAULT_BLOCK_SIZE: usize = 1024;

    fn flush(&mut self, force: bool) -> Result<()> {
        if self.staging.is_empty() {
            return Ok(());
        }
        let block_size = self.get_preferred_block_size_from_sample()?;
        assert_ne!(block_size, 0);
        let min_block_size = if force { 1 } else { block_size };

        while self.staging.len() >= min_block_size {
            let values = self.staging.dequeue(block_size)?;
            self.buffer_encoder.encode_block(&values)?;
            // TODO: if presence is not embedded into the value blocks, push this slice
            // to the presence encoder as well.
        }

        Ok(())
    }

    fn may_flush(&self) -> bool {
        self.staging.len() >= PrimitiveStagingBuffer::DEFAULT_LEN_THRESHOLD
    }

    fn get_preferred_block_size_from_sample(&mut self) -> Result<usize> {
        let sample_size = self
            .buffer_encoder
            .sample_size()
            .map_or(0, |size| size.value_count.end.max(1) - 1);
        let block_size = if sample_size != 0 {
            let sample = self.staging.sample(sample_size)?;
            if sample.is_empty() {
                Self::DEFAULT_BLOCK_SIZE
            } else {
                self.buffer_encoder
                    .analyze_sample(&sample)?
                    .value_count
                    .end
                    .max(2)
                    - 1
            }
        } else {
            Self::DEFAULT_BLOCK_SIZE
        };
        Ok(block_size)
    }
}

impl FieldEncoderOps for PrimitiveFieldEncoder {
    fn push_array(&mut self, array: Arc<dyn Array>) -> Result<()> {
        self.staging.append(array);
        if self.may_flush() {
            self.flush(false)?;
        }
        Ok(())
    }

    fn push_nulls(&mut self, count: usize) -> Result<()> {
        self.staging.append(arrow_array::new_null_array(
            &self.normalized_arrow_type,
            count,
        ));
        if self.may_flush() {
            self.flush(false)?;
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<EncodedField> {
        if !self.staging.is_empty() {
            self.flush(true)?;
        }
        let encoded_buffer = self.buffer_encoder.finish()?;
        Ok(EncodedField {
            buffers: vec![encoded_buffer],
        })
    }
}
