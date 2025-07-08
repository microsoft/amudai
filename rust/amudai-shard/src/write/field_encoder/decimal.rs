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
    pub fn create(params: &FieldEncoderParams) -> Result<Box<dyn FieldEncoderOps>> {
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

        // Finish encoding
        let encoded_buffer = self.buffer_encoder.finish()?;

        // Finalize statistics
        let statistics = Some(EncodedFieldStatistics::Decimal(
            self.stats_collector.finalize(),
        ));

        // Create index buffer if enabled
        let mut buffers = vec![encoded_buffer];
        if let Some(index_builder) = self.index_builder {
            if let Some(index_buffer) = index_builder.finish()? {
                buffers.push(index_buffer);
            }
        }

        Ok(EncodedField {
            buffers,
            statistics,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use amudai_format::defs::schema_ext::{BasicTypeDescriptor, KnownExtendedType};
    use amudai_format::schema::BasicType;
    use amudai_io_impl::temp_file_store;
    use arrow_array::FixedSizeBinaryArray;

    #[test]
    fn test_decimal_field_encoder_basic() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            fixed_size: 16,
            signed: false,
            extended_type: KnownExtendedType::KustoDecimal,
        };

        let params = FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
        };

        let mut encoder = DecimalFieldEncoder::create(&params)?;

        // Create test decimal data
        let decimal1 = [0u8; 16]; // Represents decimal value 0
        let decimal2 = [1u8; 16]; // Represents decimal value with some bits set
        let array = Arc::new(FixedSizeBinaryArray::from(vec![&decimal1, &decimal2]));

        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Verify we got statistics
        assert!(encoded_field.statistics.is_some());
        if let Some(EncodedFieldStatistics::Decimal(stats)) = encoded_field.statistics {
            assert_eq!(stats.total_count, 2);
            assert_eq!(stats.null_count, 0);
        } else {
            panic!("Expected decimal statistics");
        }

        // Verify we got encoded buffers
        assert!(!encoded_field.buffers.is_empty());

        Ok(())
    }
    #[test]
    fn test_decimal_field_encoder_with_nulls() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            fixed_size: 16,
            signed: false,
            extended_type: KnownExtendedType::KustoDecimal,
        };

        let params = FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
        };

        let mut encoder = DecimalFieldEncoder::create(&params)?;

        // Create test data with nulls
        let decimal1 = [0u8; 16];
        let array = Arc::new(FixedSizeBinaryArray::from(vec![
            Some(&decimal1[..]),
            None,
            Some(&decimal1[..]),
        ]));

        encoder.push_array(array)?;

        // Also test push_nulls
        encoder.push_nulls(2)?;

        let encoded_field = encoder.finish()?;

        // Verify statistics
        if let Some(EncodedFieldStatistics::Decimal(stats)) = encoded_field.statistics {
            assert_eq!(stats.total_count, 5); // 3 from array + 2 from push_nulls
            assert_eq!(stats.null_count, 3); // 1 from array + 2 from push_nulls
        } else {
            panic!("Expected decimal statistics");
        }

        Ok(())
    }

    #[test]
    fn test_decimal_field_encoder_with_statistics() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            fixed_size: 16,
            signed: false,
            extended_type: KnownExtendedType::KustoDecimal,
        };

        let params = FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
        };

        let mut encoder = DecimalFieldEncoder::create(&params)?;

        // Create decimal data (16 bytes each)
        let decimal1 = [0u8; 16]; // Represents decimal value 0
        let decimal2 = [1u8; 16]; // Represents decimal value with some bits set
        let array = Arc::new(FixedSizeBinaryArray::from(vec![&decimal1, &decimal2]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Should have decimal statistics
        assert!(encoded_field.statistics.is_some());
        if let Some(EncodedFieldStatistics::Decimal(stats)) = encoded_field.statistics {
            assert_eq!(stats.total_count, 2);
            assert_eq!(stats.null_count, 0);
            assert!(stats.min_value.is_some());
            assert!(stats.max_value.is_some());
        } else {
            panic!("Expected Decimal statistics for decimal field");
        }

        Ok(())
    }

    #[test]
    fn test_decimal128_array_conversion() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            fixed_size: 16,
            signed: false,
            extended_type: KnownExtendedType::KustoDecimal,
        };

        let params = FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
        };

        let mut encoder = DecimalFieldEncoder::create(&params)?;

        // Create a Decimal128 array with some test values
        let decimal_array = Arc::new(
            Decimal128Array::from(vec![
                Some(123456789i128),
                Some(-987654321i128),
                None,
                Some(0i128),
            ])
            .with_precision_and_scale(10, 2)
            .unwrap(),
        );

        encoder.push_array(decimal_array)?;

        let encoded_field = encoder.finish()?;

        // Verify statistics
        if let Some(EncodedFieldStatistics::Decimal(stats)) = encoded_field.statistics {
            assert_eq!(stats.total_count, 4);
            assert_eq!(stats.null_count, 1);
            // Should have min/max values since we have non-null decimal values
            assert!(stats.min_value.is_some());
            assert!(stats.max_value.is_some());
        } else {
            panic!("Expected decimal statistics");
        }

        // Verify we got encoded buffers
        assert!(!encoded_field.buffers.is_empty());

        Ok(())
    }

    #[test]
    fn test_decimal128_binary_representation_validation() -> Result<()> {
        use amudai_arrow_compat::decimal_conversions::convert_i128_to_decimal;
        use amudai_blockstream::read::{
            block_stream::BlockReaderPrefetch, bytes_buffer::BytesBufferDecoder,
        };
        use decimal::d128;

        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            fixed_size: 16,
            signed: false,
            extended_type: KnownExtendedType::KustoDecimal,
        };

        let params = FieldEncoderParams {
            basic_type,
            temp_store: temp_store.clone(),
            encoding_profile: Default::default(),
        };

        let mut encoder = DecimalFieldEncoder::create(&params)?;

        // Create test Decimal128 values with known decimal representations
        let test_values = vec![
            Some(123456789i128),  // 1234567.89 with scale 2
            Some(-987654321i128), // -9876543.21 with scale 2
            None,                 // NULL value
            Some(0i128),          // 0.00 with scale 2
        ];

        let decimal_array = Arc::new(
            Decimal128Array::from(test_values.clone())
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );

        encoder.push_array(decimal_array.clone())?;
        let encoded_field = encoder.finish()?;

        // Now decode the buffer and validate the binary representation
        assert_eq!(
            encoded_field.buffers.len(),
            2,
            "Should have data buffer + index buffer"
        );

        // Use the first buffer (data buffer) for validation
        let prepared_buffer = &encoded_field.buffers[0];

        // Verify the first buffer is the data buffer
        assert_eq!(
            prepared_buffer.descriptor.kind,
            amudai_format::defs::shard::BufferKind::Data as i32
        );

        // Get the artifact reader from the prepared buffer
        let artifact_reader = prepared_buffer.data.clone();
        let shard_buffer = &prepared_buffer.descriptor;

        // Create a decoder for the encoded buffer
        let decoder =
            BytesBufferDecoder::from_encoded_buffer(artifact_reader, shard_buffer, basic_type)?;

        // Create a reader to read all the values
        let mut reader = decoder.create_reader(
            std::iter::once(0u64..test_values.len() as u64),
            BlockReaderPrefetch::Disabled,
        )?;

        // Read the sequence of binary values
        let sequence = reader.read(0..test_values.len() as u64)?;
        assert_eq!(sequence.len(), test_values.len());

        // Validate each value's binary representation
        for (i, &original_value) in test_values.iter().enumerate() {
            if let Some(value) = original_value {
                // Convert the original i128 to expected d128 bytes
                let expected_d128 = convert_i128_to_decimal(value, decimal_array.scale())?;
                let expected_bytes = expected_d128.to_raw_bytes();

                // Get the actual bytes from the decoded sequence
                let actual_bytes = sequence.binary_at(i);

                assert_eq!(
                    actual_bytes.len(),
                    16,
                    "Each decimal should be exactly 16 bytes"
                );

                assert_eq!(
                    actual_bytes, expected_bytes,
                    "Binary representation mismatch for value {value} at index {i}. \
                     Expected d128 bytes: {expected_bytes:?}, but got: {actual_bytes:?}"
                );

                // Also verify we can reconstruct the d128 from the stored bytes
                let reconstructed_d128 = unsafe {
                    d128::from_raw_bytes(actual_bytes.try_into().map_err(|_| {
                        amudai_common::error::Error::invalid_operation(
                            "Failed to convert bytes to [u8; 16]".to_string(),
                        )
                    })?)
                };

                assert_eq!(
                    reconstructed_d128, expected_d128,
                    "Reconstructed d128 doesn't match expected for value {value} at index {i}"
                );
            } else {
                // For null values, we should get 16 zero bytes (or the system's null representation)
                let actual_bytes = sequence.binary_at(i);
                assert_eq!(
                    actual_bytes.len(),
                    16,
                    "Null decimal should still be 16 bytes"
                );
                // Note: The actual null representation might not be all zeros,
                // depending on how the encoder handles nulls in FixedSizeBinary arrays
            }
        }

        Ok(())
    }

    #[test]
    fn test_fixed_size_binary_decimal_binary_representation() -> Result<()> {
        use amudai_blockstream::read::{
            block_stream::BlockReaderPrefetch, bytes_buffer::BytesBufferDecoder,
        };
        use decimal::d128;
        use std::str::FromStr;

        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            fixed_size: 16,
            signed: false,
            extended_type: KnownExtendedType::KustoDecimal,
        };

        let params = FieldEncoderParams {
            basic_type,
            temp_store: temp_store.clone(),
            encoding_profile: Default::default(),
        };

        let mut encoder = DecimalFieldEncoder::create(&params)?;

        // Create known d128 values and their byte representations
        let d128_val1 = d128::from_str("123.45").unwrap();
        let d128_val2 = d128::from_str("-987.65").unwrap();
        let d128_val3 = d128::from_str("0.00").unwrap();

        let expected_bytes = [
            d128_val1.to_raw_bytes(),
            d128_val2.to_raw_bytes(),
            d128_val3.to_raw_bytes(),
        ];

        // Create FixedSizeBinary array with the d128 byte representations
        let binary_array = Arc::new(FixedSizeBinaryArray::from(vec![
            &expected_bytes[0][..],
            &expected_bytes[1][..],
            &expected_bytes[2][..],
        ]));

        encoder.push_array(binary_array)?;
        let encoded_field = encoder.finish()?;

        // Decode and validate
        let prepared_buffer = &encoded_field.buffers[0];
        let artifact_reader = prepared_buffer.data.clone();
        let shard_buffer = &prepared_buffer.descriptor;

        let decoder =
            BytesBufferDecoder::from_encoded_buffer(artifact_reader, shard_buffer, basic_type)?;

        let mut reader =
            decoder.create_reader(std::iter::once(0u64..3u64), BlockReaderPrefetch::Disabled)?;

        let sequence = reader.read(0..3)?;
        assert_eq!(sequence.len(), 3);

        // Validate that the stored bytes exactly match our expected d128 bytes
        for (i, expected) in expected_bytes.iter().enumerate() {
            let actual_bytes = sequence.binary_at(i);

            assert_eq!(
                actual_bytes, expected,
                "Binary representation mismatch at index {i}. \
                 Expected: {expected:?}, got: {actual_bytes:?}"
            );

            // Verify round-trip: bytes -> d128 -> bytes
            let reconstructed_d128 = unsafe { d128::from_raw_bytes(*expected) };
            let reconstructed_bytes = reconstructed_d128.to_raw_bytes();

            assert_eq!(
                reconstructed_bytes, *expected,
                "Round-trip conversion failed for d128 at index {i}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_decimal_field_encoder_no_index_when_all_values_null() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            fixed_size: 16,
            signed: false,
            extended_type: KnownExtendedType::KustoDecimal,
        };

        let params = FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: amudai_encodings::block_encoder::BlockEncodingProfile::Balanced, // Use Balanced to enable index
        };

        let mut encoder = DecimalFieldEncoder::create(&params)?;

        // Push only null values - should not create an index
        encoder.push_nulls(100)?; // All nulls

        // Push more null values using push_nulls which is the correct way
        encoder.push_nulls(5)?; // More nulls

        encoder.push_nulls(50)?; // More nulls

        let encoded_field = encoder.finish()?;

        // Verify that NO index buffer is created when all values are null
        // Should have only 1 buffer: data buffer (no index buffer)
        assert_eq!(encoded_field.buffers.len(), 1);

        // The only buffer should be the data buffer
        assert_eq!(
            encoded_field.buffers[0].descriptor.kind,
            amudai_format::defs::shard::BufferKind::Data as i32
        );

        // Verify statistics are still collected (null count should be tracked)
        assert!(encoded_field.statistics.is_some());
        if let Some(EncodedFieldStatistics::Decimal(stats)) = encoded_field.statistics {
            assert_eq!(stats.total_count, 155); // 100 + 5 + 50
            assert_eq!(stats.null_count, 155); // All are null
        } else {
            panic!("Expected decimal statistics");
        }

        Ok(())
    }

    #[test]
    fn test_decimal_field_encoder_no_index_when_all_values_nan() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            fixed_size: 16,
            signed: false,
            extended_type: KnownExtendedType::KustoDecimal,
        };

        let params = FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: amudai_encodings::block_encoder::BlockEncodingProfile::Balanced, // Use Balanced to enable index
        };

        let mut encoder = DecimalFieldEncoder::create(&params)?;

        // Create NaN decimal values (zero / zero creates NaN)
        let zero = decimal::d128::from(0);
        let nan_decimal = (zero / zero).to_raw_bytes();

        // Push only NaN and null values - should not create an index
        let nan_array = Arc::new(FixedSizeBinaryArray::from(vec![
            Some(&nan_decimal[..]),
            Some(&nan_decimal[..]),
            None,
            Some(&nan_decimal[..]),
        ]));
        encoder.push_array(nan_array)?;

        encoder.push_nulls(10)?; // More nulls

        // More NaN values
        let more_nan_array = Arc::new(FixedSizeBinaryArray::from(vec![
            Some(&nan_decimal[..]),
            Some(&nan_decimal[..]),
        ]));
        encoder.push_array(more_nan_array)?;

        let encoded_field = encoder.finish()?;

        // Verify that NO index buffer is created when all values are NaN/null
        // Should have only 1 buffer: data buffer (no index buffer)
        assert_eq!(encoded_field.buffers.len(), 1);

        // The only buffer should be the data buffer
        assert_eq!(
            encoded_field.buffers[0].descriptor.kind,
            amudai_format::defs::shard::BufferKind::Data as i32
        );

        // Verify statistics are still collected
        assert!(encoded_field.statistics.is_some());

        Ok(())
    }
}
