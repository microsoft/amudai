//! Decimal field encoder for storing decimal values in their native d128 representation.

use std::sync::Arc;

use amudai_arrow_compat::decimal_conversions::{convert_i128_to_decimal, convert_i256_to_decimal};
use amudai_blockstream::write::{
    bytes_buffer::BytesBufferEncoder, staging_buffer::BytesStagingBuffer,
};
use amudai_common::Result;
use amudai_data_stats::decimal::DecimalStatsCollector;
use amudai_encodings::block_encoder::{
    BlockChecksum, BlockEncodingParameters, BlockEncodingPolicy, PresenceEncoding,
};
use amudai_format::defs::schema_ext::{BasicTypeDescriptor, KnownExtendedType};
use amudai_format::schema::BasicType;
use arrow_array::{Array, Decimal128Array, Decimal256Array, FixedSizeBinaryArray};
use arrow_schema::DataType;
use decimal::d128;

use super::{EncodedField, EncodedFieldStatistics, FieldEncoderOps, FieldEncoderParams};
use crate::write::numeric_index::NumericIndexBuilder;

/// Encoder for decimal fields that stores values in their native d128 representation.
pub struct DecimalFieldEncoder {
    /// Buffer for accumulating data before encoding
    staging: BytesStagingBuffer,
    /// Encoder for converting byte data into encoded shard buffers.
    buffer_encoder: BytesBufferEncoder,
    /// Statistics collector for decimal values
    stats_collector: DecimalStatsCollector,
    /// Optional decimal index collector for building indexes during encoding
    index_builder: Option<Box<dyn NumericIndexBuilder>>,
}

impl DecimalFieldEncoder {
    /// Creates a new decimal field encoder.
    pub(crate) fn create(params: &FieldEncoderParams) -> Result<Box<dyn FieldEncoderOps>> {
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            fixed_size: 16, // d128 is always 16 bytes
            signed: false,
            extended_type: KnownExtendedType::KustoDecimal,
        };

        // Use FixedSizeBinary(16) as the normalized type for d128 storage
        let normalized_arrow_type = DataType::FixedSizeBinary(16);

        // Create index collector for decimal type (disabled for Plain profile)
        let index_builder = if params.encoding_profile
            != amudai_encodings::block_encoder::BlockEncodingProfile::Plain
        {
            crate::write::numeric_index::create_builder(
                basic_type,
                params.encoding_profile,
                params.temp_store.clone(),
            )
            .ok()
        } else {
            None
        };

        Ok(Box::new(DecimalFieldEncoder {
            staging: BytesStagingBuffer::new(normalized_arrow_type),
            buffer_encoder: BytesBufferEncoder::new(
                BlockEncodingPolicy {
                    parameters: BlockEncodingParameters {
                        checksum: BlockChecksum::Enabled,
                        presence: PresenceEncoding::Enabled,
                    },
                    profile: params.encoding_profile,
                    ..Default::default()
                },
                basic_type,
                params.temp_store.clone(),
            )?,
            stats_collector: DecimalStatsCollector::new(),
            index_builder,
        }))
    }

    /// Converts an Arrow array to d128 representation and creates a FixedSizeBinary array.
    /// Statistics collection is done separately after conversion.
    fn convert_to_d128_array(&mut self, array: &dyn Array) -> Result<Arc<dyn Array>> {
        let d128_array = match array.data_type() {
            DataType::Decimal128(_, _) => {
                let decimal_array = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .expect("Expected Decimal128Array");

                self.convert_decimal128_to_binary(decimal_array)?
            }
            DataType::Decimal256(_, _) => {
                let decimal_array = array
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .expect("Expected Decimal256Array");

                self.convert_decimal256_to_binary(decimal_array)?
            }
            DataType::FixedSizeBinary(16) => {
                let binary_array = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .expect("Expected FixedSizeBinaryArray");

                Arc::new(binary_array.clone()) as Arc<dyn Array>
            }
            _ => {
                return Err(amudai_common::error::Error::invalid_operation(format!(
                    "Unsupported array type for decimal encoder: {:?}",
                    array.data_type()
                )));
            }
        };

        // Collect statistics from the d128 array after conversion
        self.collect_stats_from_d128_array(&d128_array)?;

        Ok(d128_array)
    }

    /// Converts Decimal128Array to FixedSizeBinary array with d128 data.
    fn convert_decimal128_to_binary(&mut self, array: &Decimal128Array) -> Result<Arc<dyn Array>> {
        let mut builder = arrow_array::builder::FixedSizeBinaryBuilder::new(16);
        for i in 0..array.len() {
            if array.is_null(i) {
                builder.append_null();
            } else {
                let decimal_value = array.value(i);
                // Convert i128 to d128 using the conversion function
                let d128_value = convert_i128_to_decimal(decimal_value, array.scale())?;
                let raw_bytes = d128_value.to_raw_bytes();
                builder.append_value(raw_bytes).map_err(|e| {
                    amudai_common::error::Error::invalid_operation(format!(
                        "Failed to append d128 bytes: {e}"
                    ))
                })?;
            }
        }

        Ok(Arc::new(builder.finish()) as Arc<dyn Array>)
    }

    /// Converts Decimal256Array to FixedSizeBinary array with d128 data.
    fn convert_decimal256_to_binary(&mut self, array: &Decimal256Array) -> Result<Arc<dyn Array>> {
        let mut builder = arrow_array::builder::FixedSizeBinaryBuilder::new(16);
        for i in 0..array.len() {
            if array.is_null(i) {
                builder.append_null();
            } else {
                let decimal_value = array.value(i);
                // Convert i256 to d128 using the conversion function
                let d128_value = convert_i256_to_decimal(decimal_value, array.scale())?;
                let raw_bytes = d128_value.to_raw_bytes();
                builder.append_value(raw_bytes).map_err(|e| {
                    amudai_common::error::Error::invalid_operation(format!(
                        "Failed to append d128 bytes: {e}"
                    ))
                })?;
            }
        }

        Ok(Arc::new(builder.finish()) as Arc<dyn Array>)
    }

    /// Collects statistics from a FixedSizeBinary array containing d128 data.
    fn collect_stats_from_d128_array(&mut self, d128_array: &Arc<dyn Array>) -> Result<()> {
        let binary_array = d128_array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("Expected FixedSizeBinaryArray for d128 data");

        for i in 0..binary_array.len() {
            if binary_array.is_null(i) {
                self.stats_collector.process_null();
            } else {
                let bytes = binary_array.value(i);
                if bytes.len() != 16 {
                    return Err(amudai_common::error::Error::invalid_operation(
                        "Expected 16-byte fixed size binary for d128".to_string(),
                    ));
                }
                let mut byte_array = [0u8; 16];
                byte_array.copy_from_slice(bytes);
                let d128_value = unsafe { d128::from_raw_bytes(byte_array) };
                self.stats_collector.process_value(d128_value);
            }
        }
        Ok(())
    }

    /// Flushes data from staging buffer to the encoder.
    fn flush(&mut self, force: bool) -> Result<()> {
        if self.staging.is_empty() {
            return Ok(());
        }

        let block_size = self.get_preferred_block_size()?;

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
    fn may_flush(&self) -> bool {
        self.staging.len() >= BytesStagingBuffer::DEFAULT_LEN_THRESHOLD
            || self.staging.data_size() >= BytesStagingBuffer::DEFAULT_DATA_SIZE_THRESHOLD
    }

    /// Gets preferred block size constraints for decimal encoding.
    fn get_preferred_block_size(
        &mut self,
    ) -> Result<amudai_encodings::block_encoder::BlockSizeConstraints> {
        let sample_size = self.buffer_encoder.sample_size();
        if let Some(sample_size) = sample_size {
            let sample = self.staging.sample(
                sample_size.value_count.end.max(2) - 1,
                sample_size.data_size.end.max(2) - 1,
            )?;
            if sample.is_empty() {
                Ok(amudai_encodings::block_encoder::BlockSizeConstraints {
                    value_count: 4096..4097,
                    data_size: 4096 * 16..4096 * 16 + 1,
                })
            } else {
                self.buffer_encoder.analyze_sample(&sample)
            }
        } else {
            Ok(amudai_encodings::block_encoder::BlockSizeConstraints {
                value_count: 4096..4097,
                data_size: 4096 * 16..4096 * 16 + 1,
            })
        }
    }
}

impl FieldEncoderOps for DecimalFieldEncoder {
    fn push_array(&mut self, array: Arc<dyn Array>) -> Result<()> {
        if array.is_empty() {
            return Ok(());
        }

        // Convert to d128 binary format and collect statistics in a single pass
        let binary_array = self.convert_to_d128_array(array.as_ref())?;

        // Collect index statistics if enabled
        if let Some(ref mut index_builder) = self.index_builder {
            index_builder.process_array(binary_array.as_ref())?;
        }

        // Stage the converted array
        self.staging.append(binary_array);

        // Flush if necessary
        if self.may_flush() {
            self.flush(false)?;
        }

        Ok(())
    }

    fn push_nulls(&mut self, count: usize) -> Result<()> {
        if count == 0 {
            return Ok(());
        }

        // Update statistics for null values
        self.stats_collector.process_nulls(count);

        // Collect index statistics for null values if enabled
        if let Some(ref mut index_builder) = self.index_builder {
            index_builder.process_invalid(count)?;
        }

        // Create null array in FixedSizeBinary(16) format
        let null_array = arrow_array::new_null_array(&DataType::FixedSizeBinary(16), count);
        self.staging.append(null_array);

        // Flush if necessary
        if self.may_flush() {
            self.flush(false)?;
        }

        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<EncodedField> {
        // Flush any remaining data
        if !self.staging.is_empty() {
            self.flush(true)?;
        }

        // Finalize statistics first to check for constant values
        let statistics = EncodedFieldStatistics::Decimal(self.stats_collector.finalize());

        // Check if all values are constant - if so, skip expensive operations
        if let Some(_const_val) = statistics.try_get_constant() {
            // For constant values, we can skip buffer encoding and index creation
            // The EncodedField::new method will handle the constant value optimization internally
            return Ok(EncodedField::new(vec![], statistics, None));
        }

        // Non-constant values: proceed with normal encoding
        let encoded_buffer = self.buffer_encoder.finish()?;
        let mut buffers = vec![encoded_buffer];

        // Create index buffer if enabled
        if let Some(index_builder) = self.index_builder {
            if let Some(index_buffer) = index_builder.finish()? {
                buffers.push(index_buffer);
            }
        }

        // Use the new constructor which handles constant value optimization
        Ok(EncodedField::new(buffers, statistics, None))
    }
}
