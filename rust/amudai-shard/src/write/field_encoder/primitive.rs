//! Primitive field encoder.

use std::sync::Arc;

use amudai_arrow_compat::datetime_conversions;
use amudai_blockstream::write::{
    primitive_buffer::PrimitiveBufferEncoder, staging_buffer::PrimitiveStagingBuffer,
};
use amudai_common::{Result, error::Error, verify_arg};
use amudai_data_stats::floating::FloatingStatsCollector;
use amudai_data_stats::primitive::PrimitiveStatsCollector;
use amudai_encodings::block_encoder::{
    BlockChecksum, BlockEncodingParameters, BlockEncodingPolicy, PresenceEncoding,
};
use amudai_format::defs::schema_ext::BasicTypeDescriptor;
use arrow_array::{Array, Float32Array, Float64Array};

use crate::write::field_encoder::FieldEncoderParams;
use crate::write::numeric_index::NumericIndexBuilder;

use super::{EncodedField, EncodedFieldStatistics, FieldEncoderOps};

/// Enum to handle different types of statistics collectors for primitive fields
enum StatsCollector {
    Primitive(PrimitiveStatsCollector),
    Floating(FloatingStatsCollector),
}

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
    stats_collector: Option<StatsCollector>,
    /// Optional numeric index collector for building indexes during encoding
    index_builder: Option<Box<dyn NumericIndexBuilder>>,
}

impl PrimitiveFieldEncoder {
    /// Creates a `PrimitiveFieldEncoder`.
    ///
    /// # Arguments
    ///   
    /// * `params`: Encoding parameters (temp store, profile, basic type).
    /// * `normalized_arrow_type`: The canonical Arrow data type used for casting
    ///   accumulated values before passing them to the block encoder.
    pub(crate) fn create(
        params: &FieldEncoderParams,
        normalized_arrow_type: arrow_schema::DataType,
    ) -> Result<Box<dyn FieldEncoderOps>> {
        use amudai_format::schema::BasicType;

        let stats_collector = match params.basic_type.basic_type {
            BasicType::Float32 | BasicType::Float64 => {
                Some(StatsCollector::Floating(FloatingStatsCollector::new()))
            }
            _ => Some(StatsCollector::Primitive(PrimitiveStatsCollector::new(
                params.basic_type,
            ))),
        };

        // Create index collector for supported numeric types (disabled for Plain profile)
        let index_builder = if params.encoding_profile
            != amudai_encodings::block_encoder::BlockEncodingProfile::Plain
        {
            Some(crate::write::numeric_index::create_builder(
                params.basic_type,
                params.encoding_profile,
                params.temp_store.clone(),
            )?)
        } else {
            None
        };

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
            index_builder,
        }))
    }
}

impl PrimitiveFieldEncoder {
    /// Default (fallback) block value count when it cannot be inferred
    /// by the block encoder.
    const DEFAULT_BLOCK_SIZE: usize = 1024;
    /// Collects floating-point statistics from an Arrow array.
    fn collect_floating_stats_from_array(
        collector: &mut FloatingStatsCollector,
        array: &dyn Array,
    ) -> Result<()> {
        use arrow_schema::DataType;

        match array.data_type() {
            DataType::Float32 => {
                let float_array = array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| Error::invalid_arg("array_type", "Expected Float32Array"))?;
                for i in 0..float_array.len() {
                    if float_array.is_null(i) {
                        collector.process_null();
                    } else {
                        collector.process_f32_value(float_array.value(i));
                    }
                }
            }
            DataType::Float64 => {
                let float_array = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| Error::invalid_arg("array_type", "Expected Float64Array"))?;
                for i in 0..float_array.len() {
                    if float_array.is_null(i) {
                        collector.process_null();
                    } else {
                        collector.process_f64_value(float_array.value(i));
                    }
                }
            }
            _ => {
                return Err(Error::invalid_arg(
                    "array_type",
                    "Floating statistics collector called with non-floating point array",
                ));
            }
        }
        Ok(())
    }

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
            match stats_collector {
                StatsCollector::Primitive(collector) => {
                    collector.process_array(array.as_ref())?;
                }
                StatsCollector::Floating(collector) => {
                    Self::collect_floating_stats_from_array(collector, array.as_ref())?;
                }
            }
        }

        // Collect index statistics if enabled
        if let Some(ref mut index_builder) = self.index_builder {
            index_builder.process_array(array.as_ref())?;
        }

        self.staging.append(array);
        if self.may_flush() {
            self.flush(false)?;
        }
        Ok(())
    }

    fn push_nulls(&mut self, count: usize) -> Result<()> {
        let null_array = arrow_array::new_null_array(&self.normalized_arrow_type, count);

        // Collect statistics for null values efficiently
        if let Some(ref mut stats_collector) = self.stats_collector {
            match stats_collector {
                StatsCollector::Primitive(collector) => {
                    collector.process_nulls(count);
                }
                StatsCollector::Floating(collector) => {
                    collector.process_nulls(count);
                }
            }
        }

        // Collect index statistics for null values efficiently
        if let Some(ref mut index_builder) = self.index_builder {
            index_builder.process_invalid(count)?;
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
            match stats_collector {
                StatsCollector::Primitive(collector) => {
                    let primitive_stats = collector.finish()?;
                    Some(EncodedFieldStatistics::Primitive(primitive_stats))
                }
                StatsCollector::Floating(collector) => {
                    let floating_stats = collector.finalize();
                    Some(EncodedFieldStatistics::Floating(floating_stats))
                }
            }
        } else {
            None
        };

        // Create index buffer if enabled
        let mut buffers = vec![encoded_buffer];
        if let Some(index_builder) = self.index_builder {
            if let Some(index_buffer) = index_builder.finish()? {
                // Add the complete index buffer
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
    use amudai_blockstream::read::block_stream::BlockReaderPrefetch;
    use amudai_blockstream::read::primitive_buffer::PrimitiveBufferDecoder;
    use amudai_encodings::block_encoder::BlockEncodingProfile;
    use amudai_format::defs::schema_ext::KnownExtendedType;
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
            extended_type: Default::default(),
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
        assert_eq!(encoded_field.buffers.len(), 2); // data + 1 combined index buffer

        // The first buffer should be the data buffer
        assert_eq!(
            encoded_field.buffers[0].descriptor.kind,
            amudai_format::defs::shard::BufferKind::Data as i32
        );

        // The second buffer should be the combined index buffer
        assert_eq!(
            encoded_field.buffers[1].descriptor.kind,
            amudai_format::defs::shard::BufferKind::RangeIndex as i32
        );

        let prepared_buffer = &encoded_field.buffers[0]; // Use the data buffer for decoding
        let decoder = PrimitiveBufferDecoder::from_prepared_buffer(prepared_buffer, basic_type)?;
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
            extended_type: KnownExtendedType::None,
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
            extended_type: KnownExtendedType::None,
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

        // Verify statistics were collected - f32 should use FloatingStats now
        assert!(encoded_field.statistics.is_some());
        if let Some(EncodedFieldStatistics::Floating(stats)) = encoded_field.statistics {
            // Verify basic counts
            assert_eq!(stats.total_count, 6);
            assert_eq!(stats.null_count, 1);
            assert_eq!(stats.nan_count, 2);
            assert_eq!(stats.positive_count, 3); // 1.5, 3.7, 2.1
            assert_eq!(stats.negative_count, 0);
            assert_eq!(stats.zero_count, 0);
            assert_eq!(stats.positive_infinity_count, 0);
            assert_eq!(stats.negative_infinity_count, 0);

            // Verify range statistics (should exclude NaN values)
            assert!(stats.min_value.is_some());
            assert!(stats.max_value.is_some());

            let min_val = stats.min_value.unwrap();
            let max_val = stats.max_value.unwrap();

            assert!((min_val - 1.5).abs() < 0.001);
            assert!((max_val - 3.7).abs() < 0.001);
        } else {
            panic!("Expected floating statistics but got different type or None");
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
            extended_type: KnownExtendedType::None,
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
        } else {
            panic!("Expected primitive statistics");
        }
        Ok(())
    }

    #[test]
    fn test_primitive_field_encoder_push_nulls_optimization() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            fixed_size: 0,
            signed: true,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store,
                encoding_profile: Default::default(),
            },
            arrow_schema::DataType::Int32,
        )?;

        // Push some actual data
        let array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        encoder.push_array(array)?;

        // Push nulls using the optimized method
        encoder.push_nulls(5)?;

        // Push more data
        let array = Arc::new(Int32Array::from(vec![4, 5]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Verify statistics were collected correctly
        assert!(encoded_field.statistics.is_some());
        if let Some(EncodedFieldStatistics::Primitive(stats)) = encoded_field.statistics {
            assert_eq!(stats.count, 10); // 3 + 5 nulls + 2 = 10 total values
            assert_eq!(stats.null_count, 5); // 5 nulls from push_nulls

            // Verify range statistics (should only consider non-null values)
            assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 1);
            assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 5);
        } else {
            panic!("Expected primitive statistics");
        }

        Ok(())
    }

    #[test]
    fn test_primitive_field_encoder_with_range_index_enabled() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            fixed_size: 0,
            signed: true,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store: temp_store.clone(),
                encoding_profile: BlockEncodingProfile::Balanced, // Use Balanced to enable indexing
            },
            arrow_schema::DataType::Int32,
        )?;

        // Push test data with known values across multiple blocks
        // Since we can't control exactly how the encoder batches data,
        // let's push enough data to ensure multiple blocks and verify the overall structure

        let array1 = Arc::new(Int32Array::from(vec![1, 5, 3, 8]));
        encoder.push_array(array1)?;

        let array2 = Arc::new(Int32Array::from(vec![Some(10), None, Some(15), None]));
        encoder.push_array(array2)?;

        let array3 = Arc::new(Int32Array::from(vec![20, 25, 22, 30]));
        encoder.push_array(array3)?;

        // Push more data to ensure we exceed block size thresholds
        let array4 = Arc::new(Int32Array::from(vec![100, 105, 103, 108]));
        encoder.push_array(array4)?;

        let encoded_field = encoder.finish()?;

        // Verify basic properties - should now have 2 buffers (data + 1 combined index buffer) when indexing is enabled
        assert_eq!(encoded_field.buffers.len(), 2);
        assert!(encoded_field.statistics.is_some());

        // The first buffer should be the data buffer
        assert_eq!(
            encoded_field.buffers[0].descriptor.kind,
            amudai_format::defs::shard::BufferKind::Data as i32
        );

        // The second buffer should be the combined index buffer
        assert_eq!(
            encoded_field.buffers[1].descriptor.kind,
            amudai_format::defs::shard::BufferKind::RangeIndex as i32
        );

        // Verify one of the index buffers has data
        assert!(encoded_field.buffers[1].data_size > 0); // Should have data

        Ok(())
    }

    #[test]
    fn test_primitive_field_encoder_index_with_all_null_blocks() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            fixed_size: 0,
            signed: true,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store: temp_store.clone(),
                encoding_profile: BlockEncodingProfile::Balanced, // Use Balanced to enable index
            },
            arrow_schema::DataType::Int32,
        )?;

        // Push data with one all-null block and one mixed block
        let array1 = Arc::new(Int32Array::from(vec![Some(10), None, Some(20)])); // Block 1: min=10, max=20, nulls=1
        encoder.push_array(array1)?;

        // Push all nulls
        encoder.push_nulls(3)?; // Block 2: all nulls

        let array2 = Arc::new(Int32Array::from(vec![Some(30), Some(40), None])); // Block 3: min=30, max=40, nulls=1
        encoder.push_array(array2)?;

        let encoded_field = encoder.finish()?;

        // Verify that index buffers are included in the encoded field
        // Should have 2 buffers: data + 1 combined index buffer
        assert_eq!(encoded_field.buffers.len(), 2);

        // The first buffer should be the data buffer
        assert_eq!(
            encoded_field.buffers[0].descriptor.kind,
            amudai_format::defs::shard::BufferKind::Data as i32
        );

        // The second buffer should be the combined index buffer
        assert_eq!(
            encoded_field.buffers[1].descriptor.kind,
            amudai_format::defs::shard::BufferKind::RangeIndex as i32
        );
        assert!(encoded_field.buffers[1].data_size > 0); // Should have data

        Ok(())
    }

    #[test]
    fn test_range_index_with_float32_values() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Float32,
            fixed_size: 0,
            signed: true,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store: temp_store.clone(),
                encoding_profile: BlockEncodingProfile::Balanced, // Use Balanced to enable index
            },
            arrow_schema::DataType::Float32,
        )?;

        // Push float data with some edge cases
        let array = Arc::new(Float32Array::from(vec![
            Some(1.5),
            Some(3.7),
            Some(2.1),
            Some(8.9), // Block 1: min=1.5, max=8.9, nulls=0
            Some(-5.2),
            None,
            Some(0.0),
            Some(10.5), // Block 2: min=-5.2, max=10.5, nulls=1
        ]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Verify that index buffers are included in the encoded field
        // Should have 2 buffers: data + 1 combined index buffer
        assert_eq!(encoded_field.buffers.len(), 2);

        // The first buffer should be the data buffer
        assert_eq!(
            encoded_field.buffers[0].descriptor.kind,
            amudai_format::defs::shard::BufferKind::Data as i32
        );

        // The second buffer should be the combined index buffer
        assert_eq!(
            encoded_field.buffers[1].descriptor.kind,
            amudai_format::defs::shard::BufferKind::RangeIndex as i32
        );
        assert!(encoded_field.buffers[1].data_size > 0); // Should have data

        Ok(())
    }

    #[test]
    fn test_range_index_with_datetime_values() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::DateTime,
            fixed_size: 0,
            signed: false,
            extended_type: Default::default(),
        };

        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store: temp_store.clone(),
                encoding_profile: BlockEncodingProfile::Balanced, // Use Balanced to enable index
            },
            arrow_schema::DataType::UInt64,
        )?;

        // Create timestamp data (DateTime is stored as Int64 internally)
        let base_millis = datetime_conversions::now_unix_milliseconds();
        let mut ts_builder = TimestampMillisecondBuilder::with_capacity(6);

        // Add timestamps in chronological order
        for i in 0..6 {
            let ts = base_millis + (i * 1000); // 1 second apart
            ts_builder.append_value(ts);
        }
        let ts_array = Arc::new(ts_builder.finish());
        encoder.push_array(ts_array)?;

        let encoded_field = encoder.finish()?;

        // Verify that index buffers are included in the encoded field
        // Should have 2 buffers: data + 1 index buffer
        assert_eq!(encoded_field.buffers.len(), 2);

        // The first buffer should be the data buffer
        assert_eq!(
            encoded_field.buffers[0].descriptor.kind,
            amudai_format::defs::shard::BufferKind::Data as i32
        );

        // The second buffer should be the index buffer
        assert_eq!(
            encoded_field.buffers[1].descriptor.kind,
            amudai_format::defs::shard::BufferKind::RangeIndex as i32
        );
        assert!(encoded_field.buffers[1].data_size > 0); // Should have data

        Ok(())
    }

    #[test]
    fn test_range_index_enabled_by_default() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            fixed_size: 0,
            signed: true,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store: temp_store.clone(),
                encoding_profile: Default::default(), // Default is Balanced profile - indexing enabled
            },
            arrow_schema::DataType::Int32,
        )?;

        let array = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Verify that index buffers are included in the encoded field for default profile
        // Should have 2 buffers: data + 1 index buffer (indexing enabled by default)
        assert_eq!(encoded_field.buffers.len(), 2);

        // The first buffer should be the data buffer
        assert_eq!(
            encoded_field.buffers[0].descriptor.kind,
            amudai_format::defs::shard::BufferKind::Data as i32
        );

        // The second buffer should be the index buffer
        assert_eq!(
            encoded_field.buffers[1].descriptor.kind,
            amudai_format::defs::shard::BufferKind::RangeIndex as i32
        );

        Ok(())
    }

    #[test]
    fn test_range_index_with_default_block_size() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            fixed_size: 0,
            signed: true,
            extended_type: KnownExtendedType::None,
        };

        // Test with default block size
        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store: temp_store.clone(),
                encoding_profile: BlockEncodingProfile::Balanced, // Use Balanced to enable index
            },
            arrow_schema::DataType::Int32,
        )?;

        // 6 values should be processed with default block size
        let array = Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50, 60]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Verify that index buffers are included in the encoded field
        // Should have 2 buffers: data + 1 index buffer
        assert_eq!(encoded_field.buffers.len(), 2);

        // The first buffer should be the data buffer
        assert_eq!(
            encoded_field.buffers[0].descriptor.kind,
            amudai_format::defs::shard::BufferKind::Data as i32
        );

        // The second buffer should be the index buffer
        assert_eq!(
            encoded_field.buffers[1].descriptor.kind,
            amudai_format::defs::shard::BufferKind::RangeIndex as i32
        );
        assert!(encoded_field.buffers[1].data_size > 0); // Should have data

        Ok(())
    }

    #[test]
    fn test_range_index_roundtrip_with_checksum() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Int64,
            fixed_size: 0,
            signed: true,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store: temp_store.clone(),
                encoding_profile: BlockEncodingProfile::Balanced, // Use Balanced to enable index
            },
            arrow_schema::DataType::Int64,
        )?;

        // Add larger integer values to test Int64 specifically
        let array = Arc::new(arrow_array::Int64Array::from(vec![
            1000000000i64,
            2000000000i64,
            3000000000i64,
            4000000000i64,
            5000000000i64,
            6000000000i64,
        ]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Verify that index buffers are included in the encoded field
        // Should have 2 buffers: data + 1 index buffer
        assert_eq!(encoded_field.buffers.len(), 2);

        // The first buffer should be the data buffer
        assert_eq!(
            encoded_field.buffers[0].descriptor.kind,
            amudai_format::defs::shard::BufferKind::Data as i32
        );

        // The second buffer should be the index buffer
        assert_eq!(
            encoded_field.buffers[1].descriptor.kind,
            amudai_format::defs::shard::BufferKind::RangeIndex as i32
        );
        assert!(encoded_field.buffers[1].data_size > 0); // Should have data

        Ok(())
    }

    #[test]
    fn test_range_index_disabled_for_plain_profile() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            fixed_size: 0,
            signed: true,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store: temp_store.clone(),
                encoding_profile: BlockEncodingProfile::Plain, // Plain profile should NOT create indexes
            },
            arrow_schema::DataType::Int32,
        )?;

        let array = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Verify that NO index buffers are included for Plain profile
        // Should have only 1 buffer: data (no index buffer for Plain profile)
        assert_eq!(encoded_field.buffers.len(), 1);

        // The only buffer should be the data buffer
        assert_eq!(
            encoded_field.buffers[0].descriptor.kind,
            amudai_format::defs::shard::BufferKind::Data as i32
        );

        Ok(())
    }

    #[test]
    fn test_primitive_field_encoder_no_index_when_all_values_invalid() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            fixed_size: 0,
            signed: true,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store: temp_store.clone(),
                encoding_profile: BlockEncodingProfile::Balanced, // Use Balanced to enable index
            },
            arrow_schema::DataType::Int32,
        )?;

        // Push only null values - should not create an index
        encoder.push_nulls(100)?; // All nulls

        // Push more null arrays
        let null_array = Arc::new(Int32Array::from(vec![None, None, None, None, None]));
        encoder.push_array(null_array)?;

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

        Ok(())
    }

    #[test]
    fn test_primitive_field_encoder_no_index_when_all_float_values_nan() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Float32,
            fixed_size: 0,
            signed: true,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store: temp_store.clone(),
                encoding_profile: BlockEncodingProfile::Balanced, // Use Balanced to enable index
            },
            arrow_schema::DataType::Float32,
        )?;

        // Push only NaN and null values - should not create an index
        let nan_array = Arc::new(Float32Array::from(vec![
            Some(f32::NAN),
            Some(f32::NAN),
            None,
            Some(f32::NAN),
        ]));
        encoder.push_array(nan_array)?;

        encoder.push_nulls(10)?; // More nulls

        // More NaN values
        let more_nan_array = Arc::new(Float32Array::from(vec![Some(f32::NAN), Some(f32::NAN)]));
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

        Ok(())
    }
}
