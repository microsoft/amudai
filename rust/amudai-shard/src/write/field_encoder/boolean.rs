//! Boolean buffer and field encoders, essentially bitmap builders.

use std::sync::Arc;

use amudai_blockstream::write::bit_buffer::{BitBufferEncoder, EncodedBitBuffer};
use amudai_common::{Result, error::Error};
use amudai_data_stats::boolean::BooleanStatsCollector;
use amudai_format::{defs::shard, schema::BasicType};
use arrow_array::{Array, cast::AsArray};

use crate::write::field_encoder::FieldEncoderParams;

use super::{EncodedField, EncodedFieldStatistics, FieldEncoderOps};

/// Encoder for boolean fields that handles both boolean values and their presence (null/non-null).
///
/// Uses two separate `BitBufferEncoder` instances:
/// - `values_encoder`: Encodes the actual boolean values (true/false)
/// - `presence_encoder`: Encodes whether each position has a value or is null
///
/// Additionally collects statistics about the boolean data during encoding.
pub struct BooleanFieldEncoder {
    values_encoder: BitBufferEncoder,
    presence_encoder: BitBufferEncoder,
    /// Statistics collector for gathering boolean field statistics
    stats_collector: Option<BooleanStatsCollector>,
}

impl BooleanFieldEncoder {
    /// Creates a `BooleanFieldEncoder`.
    pub(crate) fn create(params: &FieldEncoderParams) -> Result<Box<dyn FieldEncoderOps>> {
        assert_eq!(params.basic_type.basic_type, BasicType::Boolean);
        let temp_store = params.temp_store.clone();
        Ok(Box::new(BooleanFieldEncoder {
            values_encoder: BitBufferEncoder::new(temp_store.clone(), params.encoding_profile),
            presence_encoder: BitBufferEncoder::new(temp_store.clone(), params.encoding_profile),
            stats_collector: Some(BooleanStatsCollector::new()),
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

        // Collect statistics if enabled
        if let Some(ref mut stats_collector) = self.stats_collector {
            stats_collector.process_array(array.as_ref())?;
        }

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
        // Efficiently collect statistics for null values
        if let Some(ref mut stats_collector) = self.stats_collector {
            stats_collector.process_nulls(count);
        }

        // For null values, we mark presence as false and append placeholder values
        self.presence_encoder.append_repeated(count, false)?;
        self.values_encoder.append_repeated(count, false)?; // Placeholder values for nulls
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<EncodedField> {
        // Finalize statistics first
        let statistics = if let Some(stats_collector) = self.stats_collector {
            let boolean_stats = stats_collector.finish()?;

            // Check for constant values immediately - return early if constant
            let stats = EncodedFieldStatistics::Boolean(boolean_stats);
            if stats.try_get_constant().is_some() {
                return Ok(EncodedField::new(vec![], stats, None));
            }

            stats
        } else {
            EncodedFieldStatistics::Missing
        };

        // Non-constant field - proceed with encoding
        let mut buffers = Vec::new();

        // Finish the encoders
        let values_result = self.values_encoder.finish()?;
        let presence_result = self.presence_encoder.finish()?;

        // Create values buffer - should always be Blocks since statistics confirmed non-constant values
        let mut values_buffer = match values_result {
            EncodedBitBuffer::Constant(_, _) => {
                // This should never happen since statistics confirmed the field values are not constant
                unreachable!(
                    "Values encoder returned constant result despite non-constant statistics"
                )
            }
            EncodedBitBuffer::Blocks(values_buffer) => values_buffer,
        };
        values_buffer.descriptor.kind = shard::BufferKind::Data as i32;
        values_buffer.descriptor.embedded_presence = false;
        values_buffer.descriptor.embedded_offsets = false;
        buffers.push(values_buffer);

        match presence_result {
            EncodedBitBuffer::Constant(presence, _count) => {
                if !presence {
                    // All values are null - this should have been caught by statistics as a constant field
                    unreachable!(
                        "All-null field should have been handled by constant statistics check"
                    )
                }
                // All values are non-null, no need for presence buffer (optimized away)
            }
            EncodedBitBuffer::Blocks(mut presence_buffer) => {
                presence_buffer.descriptor.kind = shard::BufferKind::Presence as i32;
                presence_buffer.descriptor.embedded_presence = false;
                presence_buffer.descriptor.embedded_offsets = false;
                buffers.push(presence_buffer);
            }
        }

        // Use the optimized constructor that handles constant detection and buffer optimization
        Ok(EncodedField::new(buffers, statistics, None))
    }
}
