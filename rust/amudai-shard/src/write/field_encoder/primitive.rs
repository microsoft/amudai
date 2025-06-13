//! Primitive field encoder.

use std::sync::Arc;

use amudai_arrow_compat::datetime_conversions;
use amudai_blockstream::write::{
    primitive_buffer::PrimitiveBufferEncoder, staging_buffer::PrimitiveStagingBuffer,
};
use amudai_common::{Result, verify_arg};
use amudai_data_stats::primitive::PrimitiveStatsCollector;
use amudai_encodings::block_encoder::{
    BlockChecksum, BlockEncodingParameters, BlockEncodingPolicy, PresenceEncoding,
};
use amudai_format::defs::schema_ext::BasicTypeDescriptor;
use arrow_array::Array;

use crate::write::field_encoder::FieldEncoderParams;

use super::{EncodedField, EncodedFieldStatistics, FieldEncoderOps};

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
    /// Field encoder parameters (temp store, encoding profile, basic type)
    params: FieldEncoderParams,
    /// Staging buffer for accumulating values before flushing.
    staging: PrimitiveStagingBuffer,
    /// The canonical Arrow data type used for casting accumulated values
    /// before passing them to the block encoder.
    normalized_arrow_type: arrow_schema::DataType,
    /// Value buffer encoder.
    buffer_encoder: PrimitiveBufferEncoder,
    /// Optional statistics collector for gathering field statistics
    stats_collector: Option<PrimitiveStatsCollector>,
}

impl PrimitiveFieldEncoder {
    /// Creates a `PrimitiveFieldEncoder`.
    ///
    /// # Arguments
    ///
    /// * `params`: Encoding parameters (temp store, profile, basic type).
    /// * `normalized_arrow_type`: The canonical Arrow data type used for casting
    ///   accumulated values before passing them to the block encoder.
    pub fn create(
        params: &FieldEncoderParams,
        normalized_arrow_type: arrow_schema::DataType,
    ) -> Result<Box<dyn FieldEncoderOps>> {
        let stats_collector = Some(PrimitiveStatsCollector::new(params.basic_type));

        Ok(Box::new(PrimitiveFieldEncoder {
            staging: PrimitiveStagingBuffer::new(normalized_arrow_type.clone()),
            params: params.clone(),
            normalized_arrow_type,
            buffer_encoder: PrimitiveBufferEncoder::new(
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
            stats_collector,
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

    fn convert_array(
        array: Arc<dyn Array>,
        basic_type: BasicTypeDescriptor,
    ) -> Result<Arc<dyn Array>> {
        if basic_type.basic_type.is_integer_or_datetime() {
            if matches!(
                array.data_type(),
                arrow_schema::DataType::Timestamp(_, _)
                    | arrow_schema::DataType::Date32
                    | arrow_schema::DataType::Date64
            ) {
                Self::convert_datetime(array)
            } else {
                verify_arg!(array, array.data_type().is_integer());
                Ok(array)
            }
        } else {
            Ok(array)
        }
    }

    fn convert_datetime(array: Arc<dyn Array>) -> Result<Arc<dyn Array>> {
        Ok(datetime_conversions::array_to_datetime_ticks(array))
    }
}

impl FieldEncoderOps for PrimitiveFieldEncoder {
    fn push_array(&mut self, array: Arc<dyn Array>) -> Result<()> {
        let array = Self::convert_array(array, self.params.basic_type)?;

        // Collect statistics if enabled
        if let Some(ref mut stats_collector) = self.stats_collector {
            stats_collector.process_array(array.as_ref())?;
        }

        self.staging.append(array);
        if self.may_flush() {
            self.flush(false)?;
        }
        Ok(())
    }

    fn push_nulls(&mut self, count: usize) -> Result<()> {
        let null_array = arrow_array::new_null_array(&self.normalized_arrow_type, count);

        // Collect statistics for null array if enabled
        if let Some(ref mut stats_collector) = self.stats_collector {
            stats_collector.process_array(&null_array)?;
        }

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

        // Finalize statistics if enabled
        let statistics = if let Some(stats_collector) = self.stats_collector {
            let primitive_stats = stats_collector.finish()?;
            Some(EncodedFieldStatistics::Primitive(primitive_stats))
        } else {
            None
        };

        Ok(EncodedField {
            buffers: vec![encoded_buffer],
            statistics,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use amudai_blockstream::read::block_stream::BlockReaderPrefetch;
    use amudai_blockstream::read::primitive_buffer::PrimitiveBufferDecoder;
    use amudai_format::schema::BasicType;
    use amudai_io_impl::temp_file_store;
    use arrow_array::builder::{TimestampMillisecondBuilder, TimestampNanosecondBuilder};
    use arrow_array::{Float32Array, Int32Array};
    use std::{sync::Arc, time::SystemTime};

    #[test]
    fn test_datetime_field_encoder() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::DateTime,
            fixed_size: 0,
            signed: false,
        };

        let now = SystemTime::now();
        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store,
                encoding_profile: Default::default(),
            },
            arrow_schema::DataType::UInt64,
        )?;

        for _ in 0..10 {
            let now_millis = datetime_conversions::now_unix_milliseconds();
            let mut ts_builder = TimestampMillisecondBuilder::with_capacity(10000);
            for i in 0..5000 {
                let ts = now_millis + (i * 10);
                ts_builder.append_value(ts);
            }
            let ts_array = Arc::new(ts_builder.finish());
            encoder.push_array(ts_array)?;

            let now_nanos = datetime_conversions::now_unix_nanoseconds();
            let mut ts_builder = TimestampNanosecondBuilder::with_capacity(10000);
            for i in 0..5000 {
                let ts = now_nanos + (i * 10);
                ts_builder.append_value(ts);
            }
            let ts_array = Arc::new(ts_builder.finish());
            encoder.push_array(ts_array)?;
        }

        let encoded_field = encoder.finish()?;
        assert_eq!(encoded_field.buffers.len(), 1);
        let prepared_buffer = encoded_field.buffers.into_iter().next().unwrap();
        let decoder = PrimitiveBufferDecoder::from_prepared_buffer(&prepared_buffer, basic_type)?;
        let mut reader =
            decoder.create_reader(std::iter::empty(), BlockReaderPrefetch::Disabled)?;
        let seq = reader.read(1000..2000).unwrap();
        let t = seq.values.as_slice::<u64>()[0];
        let t = datetime_conversions::ticks_to_unix_seconds(t).unwrap();
        assert!(
            t < now
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                + 200
        );
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collection_int32() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            fixed_size: 0,
            signed: true,
        };
        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store,
                encoding_profile: Default::default(),
            },
            arrow_schema::DataType::Int32,
        )?;

        // Push some test data
        let array = Arc::new(Int32Array::from(vec![1, 5, 3, 8, 2]));
        encoder.push_array(array)?;

        let array_with_nulls = Arc::new(Int32Array::from(vec![
            Some(10),
            None,
            Some(15),
            None,
            Some(20),
        ]));
        encoder.push_array(array_with_nulls)?;

        let encoded_field = encoder.finish()?;

        // Verify statistics were collected
        assert!(encoded_field.statistics.is_some());
        if let Some(EncodedFieldStatistics::Primitive(stats)) = encoded_field.statistics {
            // Verify basic counts
            assert_eq!(stats.count, 10); // 5 + 5 values
            assert_eq!(stats.null_count, 2); // 2 nulls in second array
            assert_eq!(stats.nan_count, 0); // No NaNs for integers

            // Verify range statistics
            assert!(stats.range_stats.min_value.is_some());
            assert!(stats.range_stats.max_value.is_some());

            let min_val = stats.range_stats.min_value.unwrap().as_i64().unwrap();
            let max_val = stats.range_stats.max_value.unwrap().as_i64().unwrap();

            assert_eq!(min_val, 1);
            assert_eq!(max_val, 20);
        } else {
            panic!("Expected primitive statistics but got different type or None");
        }

        Ok(())
    }

    #[test]
    fn test_primitive_stats_collection_float32_with_nan() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Float32,
            fixed_size: 0,
            signed: true,
        };
        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store,
                encoding_profile: Default::default(),
            },
            arrow_schema::DataType::Float32,
        )?;

        // Push test data with NaN values
        let array = Arc::new(Float32Array::from(vec![
            Some(1.5),
            Some(f32::NAN),
            Some(3.7),
            None,
            Some(2.1),
            Some(f32::NAN),
        ]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Verify statistics were collected
        assert!(encoded_field.statistics.is_some());
        if let Some(EncodedFieldStatistics::Primitive(stats)) = encoded_field.statistics {
            // Verify basic counts
            assert_eq!(stats.count, 6);
            assert_eq!(stats.null_count, 1);
            assert_eq!(stats.nan_count, 2);

            // Verify range statistics (should exclude NaN values)
            assert!(stats.range_stats.min_value.is_some());
            assert!(stats.range_stats.max_value.is_some());

            let min_val = stats.range_stats.min_value.unwrap().as_f64().unwrap();
            let max_val = stats.range_stats.max_value.unwrap().as_f64().unwrap();

            assert!((min_val - 1.5).abs() < 0.001);
            assert!((max_val - 3.7).abs() < 0.001);
        } else {
            panic!("Expected primitive statistics but got different type or None");
        }

        Ok(())
    }

    #[test]
    fn test_primitive_stats_collection_always_enabled() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            fixed_size: 0,
            signed: true,
        };

        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store,
                encoding_profile: Default::default(),
            },
            arrow_schema::DataType::Int32,
        )?;

        // Push some test data
        let array = Arc::new(Int32Array::from(vec![1, 5, 3, 8, 2]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Verify statistics were collected (always enabled now)
        assert!(encoded_field.statistics.is_some());
        if let Some(EncodedFieldStatistics::Primitive(stats)) = encoded_field.statistics {
            assert_eq!(stats.count, 5);
            assert_eq!(stats.null_count, 0);
            assert_eq!(stats.nan_count, 0);
        } else {
            panic!("Expected primitive statistics");
        }

        Ok(())
    }
}
