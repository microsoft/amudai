use std::sync::Arc;

use amudai_blockstream::write::{
    bytes_buffer::BytesBufferEncoder, staging_buffer::BytesStagingBuffer,
};
use amudai_common::Result;
use amudai_data_stats::{binary::BinaryStatsCollector, string::StringStatsCollector};
use amudai_encodings::{
    binary_block_encoder::BinaryBlockEncoder,
    block_encoder::BlockEncodingProfile,
    block_encoder::{
        BlockChecksum, BlockEncodingParameters, BlockEncodingPolicy, BlockSizeConstraints,
        PresenceEncoding,
    },
};
use amudai_format::defs::schema_ext::KnownExtendedType;
use amudai_format::{defs::schema_ext::BasicTypeDescriptor, schema::BasicType};
use arrow_array::Array;

use crate::write::field_encoder::FieldEncoderParams;

use super::{EncodedField, EncodedFieldStatistics, FieldEncoderOps};

/// Statistics collector that dispatches to either String or Binary statistics
/// based on the field type.
pub enum BytesStatsCollector {
    None,
    String(StringStatsCollector),
    Binary(BinaryStatsCollector),
}

impl BytesStatsCollector {
    /// Creates a new statistics collector for the given basic type with bloom filter
    /// configuration derived from the encoding profile.
    ///
    /// For string fields, bloom filters are automatically enabled unless the encoding
    /// profile is Plain (which prioritizes speed over query optimization features).
    pub fn new(basic_type: BasicType, encoding_profile: BlockEncodingProfile) -> Self {
        match basic_type {
            BasicType::String => {
                let collector = StringStatsCollector::new(encoding_profile);
                Self::String(collector)
            }
            BasicType::Binary | BasicType::Guid | BasicType::FixedSizeBinary => {
                Self::Binary(BinaryStatsCollector::new(basic_type, encoding_profile))
            }
            _ => Self::None,
        }
    }

    /// Process an array and update statistics.
    pub fn process_array(&mut self, array: &dyn Array) -> Result<()> {
        match self {
            Self::None => Ok(()),
            Self::String(collector) => collector.process_array(array),
            Self::Binary(collector) => collector.process_array(array),
        }
    }

    /// Process null values efficiently without creating an array.
    ///
    /// This method directly updates null counts without processing array data,
    /// which is more efficient than creating null arrays.
    pub fn process_nulls(&mut self, null_count: usize) -> Result<()> {
        match self {
            Self::None => { /* No-op for None variant */ }
            Self::String(collector) => collector.process_nulls(null_count),
            Self::Binary(collector) => collector.process_nulls(null_count),
        }
        Ok(())
    }

    /// Process a single non-null binary value repeated `count` times.
    ///
    /// This method is primarily used for cases where you want to process individual binary values
    /// without going through an entire array.
    ///
    /// If the collector is for `string` type, the value is assumed as valid UTF-8
    /// without further validation.
    ///
    /// # Arguments
    /// * `value` - A reference to the binary value to be processed
    /// * `count` - The number of times this value should be counted in statistics
    ///
    /// # Returns
    /// A `Result` indicating success or failure
    pub fn process_value(&mut self, value: &[u8], count: usize) -> Result<()> {
        match self {
            Self::None => Ok(()),
            Self::String(collector) => {
                collector.process_value(unsafe { std::str::from_utf8_unchecked(value) }, count)
            }
            Self::Binary(collector) => collector.process_value(value, count),
        }
    }

    /// Finish statistics collection and return the appropriate statistics.
    pub fn finish(self) -> Result<EncodedFieldStatistics> {
        match self {
            Self::None => Ok(EncodedFieldStatistics::Missing),
            Self::String(collector) => {
                let stats = collector.finalize()?;
                Ok(EncodedFieldStatistics::String(stats))
            }
            Self::Binary(collector) => {
                let stats = collector.finalize()?;
                Ok(EncodedFieldStatistics::Binary(stats))
            }
        }
    }
}

/// Encodes variable-sized and fixed-size binary-like fields (`Binary`,
/// `FixedSizeBinary`, `String`, `Guid`).
///
/// Uses a staging buffer to accumulate data before encoding it in optimally
/// sized blocks.
pub struct BytesFieldEncoder {
    /// Buffer for accumulating data before encoding
    staging: BytesStagingBuffer,
    /// The normalized Arrow data type used for processing
    normalized_arrow_type: arrow_schema::DataType,
    /// Encoder for converting byte data into encoded shard buffers.
    buffer_encoder: BytesBufferEncoder,
    /// Statistics collector that dispatches to the appropriate type
    stats_collector: BytesStatsCollector,
}

impl BytesFieldEncoder {
    /// Creates a new `BytesFieldEncoder` for the specified basic type.
    ///
    /// # Returns
    /// A boxed `FieldEncoderOps` trait object
    pub(crate) fn create(params: &FieldEncoderParams) -> Result<Box<dyn FieldEncoderOps>> {
        assert!(matches!(
            params.basic_type.basic_type,
            BasicType::Binary | BasicType::FixedSizeBinary | BasicType::String | BasicType::Guid
        ));

        // BytesFieldEncoder should not handle decimal fields - those go to DecimalFieldEncoder
        assert!(
            params.basic_type.basic_type != BasicType::FixedSizeBinary
                || params.basic_type.extended_type != KnownExtendedType::KustoDecimal,
            "Decimal fields should be handled by DecimalFieldEncoder, not BytesFieldEncoder"
        );

        let normalized_arrow_type = Self::get_normalized_arrow_type(params.basic_type);

        Ok(Box::new(BytesFieldEncoder {
            staging: BytesStagingBuffer::new(normalized_arrow_type.clone()),
            normalized_arrow_type,
            buffer_encoder: BytesBufferEncoder::new(
                BlockEncodingPolicy {
                    parameters: BlockEncodingParameters {
                        checksum: BlockChecksum::Enabled,
                        presence: PresenceEncoding::Enabled,
                    },
                    profile: params.encoding_profile,
                    ..Default::default()
                },
                params.basic_type,
                params.temp_store.clone(),
            )?,
            stats_collector: BytesStatsCollector::new(
                params.basic_type.basic_type,
                params.encoding_profile,
            ),
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
        } // Collect statistics
        self.stats_collector.process_array(array.as_ref())?;

        self.staging.append(array);
        if self.may_flush() {
            self.flush(false)?;
        }
        Ok(())
    }
    fn push_nulls(&mut self, count: usize) -> Result<()> {
        let null_array = arrow_array::new_null_array(&self.normalized_arrow_type, count);
        // Collect statistics for null values efficiently
        self.stats_collector.process_nulls(count)?;

        self.staging.append(null_array);
        if self.may_flush() {
            self.flush(false)?;
        }
        Ok(())
    }
    fn finish(mut self: Box<Self>) -> Result<EncodedField> {
        // Flush any remaining data from the staging buffer
        if !self.staging.is_empty() {
            self.flush(true)?;
        }

        // Finalize statistics collection
        let statistics = self.stats_collector.finish()?;

        // Early return optimization: if all values are constant, skip expensive buffer operations
        // The EncodedField::new constructor will automatically handle constant value detection
        if statistics.try_get_constant().is_some() {
            return Ok(EncodedField::new(vec![], statistics, None));
        }

        // Non-constant field: proceed with normal encoding
        let encoded_buffer = self.buffer_encoder.finish()?;
        let buffers = vec![encoded_buffer];

        Ok(EncodedField::new(buffers, statistics, None))
    }
}
