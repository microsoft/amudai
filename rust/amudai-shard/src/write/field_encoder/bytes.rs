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
    pub fn finish(self) -> Result<Option<EncodedFieldStatistics>> {
        match self {
            Self::None => Ok(None),
            Self::String(collector) => {
                let stats = collector.finalize()?;
                Ok(Some(EncodedFieldStatistics::String(stats)))
            }
            Self::Binary(collector) => {
                let stats = collector.finalize()?;
                Ok(Some(EncodedFieldStatistics::Binary(stats)))
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
        if !self.staging.is_empty() {
            self.flush(true)?;
        }
        let encoded_buffer = self.buffer_encoder.finish()?;

        // Finalize statistics
        let statistics = self.stats_collector.finish()?;

        Ok(EncodedField {
            buffers: vec![encoded_buffer],
            statistics,
            dictionary_size: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::field_encoder::{
        DictionaryEncoding, EncodedFieldStatistics, FieldEncoderParams,
    };
    use amudai_format::defs::schema_ext::BasicTypeDescriptor;
    use amudai_format::schema::BasicType;
    use amudai_io_impl::temp_file_store;
    use arrow_array::BinaryArray;
    use std::sync::Arc;

    #[test]
    fn test_binary_field_encoder_with_statistics() -> amudai_common::Result<()> {
        // Create a temporary store for testing
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap(); // Create a binary field encoder
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Binary,
            fixed_size: 0,
            signed: false,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = BytesFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // Test data with various binary values and nulls
        let array1 = Arc::new(BinaryArray::from_opt_vec(vec![
            Some(b"hello" as &[u8]),
            Some(b"world"),
            None,
            Some(b""), // Empty binary
            Some(b"test123"),
        ]));
        encoder.push_array(array1)?;

        let array2 = Arc::new(BinaryArray::from_vec(vec![
            b"binary",
            b"data",
            b"statistics",
        ]));
        encoder.push_array(array2)?;

        // Test push_nulls
        encoder.push_nulls(2)?;

        // Finish encoding and get statistics
        let encoded_field = encoder.finish()?;

        // Verify that statistics were collected
        assert!(
            encoded_field.statistics.is_some(),
            "Statistics should be collected for Binary fields"
        );

        if let Some(EncodedFieldStatistics::Binary(stats)) = encoded_field.statistics {
            // Verify counts:
            // Array1: 4 non-null values (hello, world, "", test123), 1 null
            // Array2: 3 non-null values (binary, data, statistics), 0 nulls
            // push_nulls: 2 nulls
            // Total: 7 non-null values, 3 nulls, 10 total count
            assert_eq!(stats.total_count, 10);
            assert_eq!(stats.null_count, 3);
            assert_eq!(stats.non_null_count(), 7);

            // Verify length statistics
            assert_eq!(stats.min_length, 0); // Empty binary value ""
            assert_eq!(stats.max_length, 10); // "statistics" is 10 bytes
            assert_eq!(stats.min_non_empty_length, Some(4)); // "data" is 4 bytes
        } else {
            panic!("Expected Binary statistics");
        }

        Ok(())
    }

    #[test]
    fn test_fixed_size_binary_field_encoder_with_statistics() -> amudai_common::Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            fixed_size: 4,
            signed: false,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = BytesFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // FixedSizeBinary arrays - all values have the same length
        let array = Arc::new(arrow_array::FixedSizeBinaryArray::from(vec![
            b"abcd", b"efgh", b"ijkl",
        ]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        assert!(encoded_field.statistics.is_some());
        if let Some(EncodedFieldStatistics::Binary(stats)) = encoded_field.statistics {
            assert_eq!(stats.total_count, 3);
            assert_eq!(stats.null_count, 0);
            assert_eq!(stats.min_length, 4); // Fixed size
            assert_eq!(stats.max_length, 4); // Fixed size
            assert_eq!(stats.min_non_empty_length, Some(4));
        } else {
            panic!("Expected Binary statistics");
        }

        Ok(())
    }

    #[test]
    fn test_guid_field_encoder_with_statistics() -> amudai_common::Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Guid,
            fixed_size: 16,
            signed: false,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = BytesFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // GUID data (16 bytes each)
        let guid1 = [0u8; 16];
        let guid2 = [1u8; 16];
        let array = Arc::new(arrow_array::FixedSizeBinaryArray::from(vec![
            &guid1, &guid2,
        ]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        assert!(encoded_field.statistics.is_some());
        if let Some(EncodedFieldStatistics::Binary(stats)) = encoded_field.statistics {
            assert_eq!(stats.total_count, 2);
            assert_eq!(stats.null_count, 0);
            assert_eq!(stats.min_length, 16); // GUID size
            assert_eq!(stats.max_length, 16); // GUID size
            assert_eq!(stats.min_non_empty_length, Some(16));
        } else {
            panic!("Expected Binary statistics");
        }

        Ok(())
    }

    #[test]
    fn test_string_field_encoder_still_works() -> amudai_common::Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::String,
            fixed_size: 0,
            signed: false,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = BytesFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        let array = Arc::new(arrow_array::StringArray::from(vec!["hello", "world"]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // String fields should still get String statistics, not Binary statistics
        assert!(encoded_field.statistics.is_some());
        if let Some(EncodedFieldStatistics::String(_stats)) = encoded_field.statistics {
            // This is correct - String fields should get String statistics
        } else {
            panic!("Expected String statistics for String field");
        }

        Ok(())
    }

    #[test]
    fn test_bytes_stats_collector_enum_dispatching() -> amudai_common::Result<()> {
        // Test that the enum correctly dispatches to String statistics
        let mut string_collector =
            BytesStatsCollector::new(BasicType::String, BlockEncodingProfile::Balanced);
        let string_array = Arc::new(arrow_array::StringArray::from(vec!["hello", "world"]));
        string_collector.process_array(string_array.as_ref())?;

        if let Some(EncodedFieldStatistics::String(stats)) = string_collector.finish()? {
            assert_eq!(stats.count, 2);
            assert_eq!(stats.null_count, 0);
        } else {
            panic!("Expected String statistics");
        } // Test that the enum correctly dispatches to Binary statistics
        let mut binary_collector =
            BytesStatsCollector::new(BasicType::Binary, BlockEncodingProfile::Balanced);
        let binary_array = Arc::new(arrow_array::BinaryArray::from_vec(vec![b"hello", b"world"]));
        binary_collector.process_array(binary_array.as_ref())?;

        if let Some(EncodedFieldStatistics::Binary(stats)) = binary_collector.finish()? {
            assert_eq!(stats.total_count, 2);
            assert_eq!(stats.null_count, 0);
        } else {
            panic!("Expected Binary statistics");
        } // Test that None returns no statistics
        let mut none_collector =
            BytesStatsCollector::new(BasicType::Int32, BlockEncodingProfile::Balanced); // Unsupported type
        let int_array = Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]));
        let result = none_collector.process_array(int_array.as_ref());
        assert!(result.is_ok()); // Should not fail, just do nothing

        assert!(none_collector.finish()?.is_none());

        Ok(())
    }

    #[test]
    #[should_panic(
        expected = "Decimal fields should be handled by DecimalFieldEncoder, not BytesFieldEncoder"
    )]
    fn test_bytes_field_encoder_rejects_decimal_fields() {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            fixed_size: 16,
            signed: false,
            extended_type: KnownExtendedType::KustoDecimal,
        };

        // This should panic because BytesFieldEncoder should not handle decimal fields
        let _encoder = BytesFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        });
    }
}
