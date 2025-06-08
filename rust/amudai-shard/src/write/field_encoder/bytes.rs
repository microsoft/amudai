use std::sync::Arc;

use amudai_blockstream::write::{
    bytes_buffer::BytesBufferEncoder, staging_buffer::BytesStagingBuffer,
};
use amudai_common::Result;
use amudai_encodings::{
    binary_block_encoder::BinaryBlockEncoder, block_encoder::BlockSizeConstraints,
};
use amudai_format::{defs::schema_ext::BasicTypeDescriptor, schema::BasicType};
use amudai_io::temp_file_store::TemporaryFileStore;
use arrow_array::Array;

use super::{EncodedField, FieldEncoderOps};

/// Encodes variable-sized and fixed-size binary-like fields (`Binary`,
/// `FixedSizeBinary`, `String`, `Guid`).
///
/// Uses a staging buffer to accumulate data before encoding it in optimally
/// sized blocks.
pub struct BytesFieldEncoder {
    /// The descriptor of the basic type being encoded
    _basic_type: BasicTypeDescriptor,
    /// Buffer for accumulating data before encoding
    staging: BytesStagingBuffer,
    /// The normalized Arrow data type used for processing
    normalized_arrow_type: arrow_schema::DataType,
    /// Encoder for converting byte data into encoded shard buffers.
    buffer_encoder: BytesBufferEncoder,
}

impl BytesFieldEncoder {
    /// Creates a new `BytesFieldEncoder` for the specified basic type.
    ///
    /// # Arguments
    /// * `basic_type` - The basic type descriptor (must be Binary, FixedSizeBinary,
    ///   String, or Guid)
    /// * `temp_store` - Temporary file storage for managing intermediate artifacts
    ///
    /// # Returns
    /// A boxed `FieldEncoderOps` trait object
    pub fn create(
        basic_type: BasicTypeDescriptor,
        temp_store: Arc<dyn TemporaryFileStore>,
    ) -> Result<Box<dyn FieldEncoderOps>> {
        assert!(matches!(
            basic_type.basic_type,
            BasicType::Binary | BasicType::FixedSizeBinary | BasicType::String | BasicType::Guid
        ));
        let normalized_arrow_type = Self::get_normalized_arrow_type(basic_type);
        Ok(Box::new(BytesFieldEncoder {
            _basic_type: basic_type,
            staging: BytesStagingBuffer::new(normalized_arrow_type.clone()),
            normalized_arrow_type,
            buffer_encoder: BytesBufferEncoder::new(Default::default(), basic_type, temp_store)?,
        }))
    }
}

impl BytesFieldEncoder {
    /// Flushes data from staging buffer to the encoder.
    ///
    /// # Arguments
    /// * `force` - When true, flushes even small amounts of data; otherwise
    ///   respects minimum block size constraints
    ///
    /// # Returns
    /// Result indicating success or failure
    fn flush(&mut self, force: bool) -> Result<()> {
        if self.staging.is_empty() {
            return Ok(());
        }

        let block_size = self.get_preferred_block_size_from_sample()?;

        let (min_count, min_size) = if force {
            (1, 0)
        } else {
            (block_size.value_count.start, block_size.data_size.start)
        };

        while self.staging.len() >= min_count && self.staging.data_size() >= min_size {
            let values = self.staging.dequeue_at_most(
                block_size.value_count.end.max(2) - 1,
                block_size.data_size.end.max(2) - 1,
            )?;
            self.buffer_encoder.encode_block(&values)?;
        }
        Ok(())
    }

    /// Determines if the staging buffer should be flushed based on size thresholds.
    ///
    /// # Returns
    /// `true` if the buffer exceeds the configured thresholds
    fn may_flush(&self) -> bool {
        self.staging.len() >= BytesStagingBuffer::DEFAULT_LEN_THRESHOLD
            || self.staging.data_size() >= BytesStagingBuffer::DEFAULT_DATA_SIZE_THRESHOLD
    }

    /// Converts a basic type descriptor to its normalized Arrow data type.
    ///
    /// # Arguments
    /// * `basic_type` - The basic type descriptor to convert
    ///
    /// # Returns
    /// The corresponding Arrow data type. All variable-sized types are normalized to
    /// `LargeBinary`.
    ///
    /// # Panics
    /// Panics if the basic type is not supported
    fn get_normalized_arrow_type(basic_type: BasicTypeDescriptor) -> arrow_schema::DataType {
        match basic_type.basic_type {
            BasicType::Binary => arrow_schema::DataType::LargeBinary,
            BasicType::FixedSizeBinary => {
                arrow_schema::DataType::FixedSizeBinary(basic_type.fixed_size as i32)
            }
            BasicType::String => arrow_schema::DataType::LargeBinary,
            BasicType::Guid => arrow_schema::DataType::FixedSizeBinary(16),
            _ => panic!(
                "BytesFieldEncoder::arrow_data_type: unexpected basic type {:?}",
                basic_type.basic_type
            ),
        }
    }

    /// Determines optimal block size constraints based on a sample from the staging buffer.
    ///
    /// # Returns
    /// Block size constraints for optimal encoding
    fn get_preferred_block_size_from_sample(&mut self) -> Result<BlockSizeConstraints> {
        let sample_size = self.buffer_encoder.sample_size();
        if let Some(sample_size) = sample_size {
            let sample = self.staging.sample(
                sample_size.value_count.end.max(2) - 1,
                sample_size.data_size.end.max(2) - 1,
            )?;
            if sample.is_empty() {
                Ok(BinaryBlockEncoder::DEFAULT_SIZE_CONSTRAINTS)
            } else {
                self.buffer_encoder.analyze_sample(&sample)
            }
        } else {
            Ok(BinaryBlockEncoder::DEFAULT_SIZE_CONSTRAINTS)
        }
    }
}

impl FieldEncoderOps for BytesFieldEncoder {
    fn push_array(&mut self, array: Arc<dyn Array>) -> Result<()> {
        if array.is_empty() {
            return Ok(());
        }
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
