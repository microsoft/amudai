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

        // Finalize statistics first to check for constant values
        let statistics = if let Some(stats_collector) = self.stats_collector.take() {
            match stats_collector {
                StatsCollector::Primitive(collector) => {
                    let primitive_stats = collector.finish()?;
                    EncodedFieldStatistics::Primitive(primitive_stats)
                }
                StatsCollector::Floating(collector) => {
                    let floating_stats = collector.finalize();
                    EncodedFieldStatistics::Floating(floating_stats)
                }
            }
        } else {
            EncodedFieldStatistics::Missing
        };

        // Check if all values are constant - if so, skip expensive operations
        if statistics.try_get_constant().is_some() {
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
                // Add the complete index buffer
                buffers.push(index_buffer);
            }
        }

        // Use the new constructor which handles constant value optimization
        Ok(EncodedField::new(buffers, statistics, None))
    }
}
