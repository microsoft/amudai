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
    params: FieldEncoderParams,
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
            params: params.clone(),
            values_encoder: BitBufferEncoder::new(temp_store.clone(), params.encoding_profile),
            presence_encoder: BitBufferEncoder::new(temp_store, params.encoding_profile),
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
        let mut buffers = Vec::new();

        // Finish the values encoder
        match self.values_encoder.finish()? {
            EncodedBitBuffer::Constant(value, count) => {
                // TODO: Consider optimizing constant boolean values
                // For now, we'll still create a buffer even for constants
                // This should be optimized in the future by storing the constant in the field descriptor
                let mut values_buffer =
                    BitBufferEncoder::encode_blocks(count, value, self.params.temp_store.clone())?;
                values_buffer.descriptor.kind = shard::BufferKind::Data as i32;
                values_buffer.descriptor.embedded_presence = false;
                values_buffer.descriptor.embedded_offsets = false;
                buffers.push(values_buffer);
            }
            EncodedBitBuffer::Blocks(mut values_buffer) => {
                values_buffer.descriptor.kind = shard::BufferKind::Data as i32;
                values_buffer.descriptor.embedded_presence = false;
                values_buffer.descriptor.embedded_offsets = false;
                buffers.push(values_buffer);
            }
        }

        // Finish the presence encoder
        match self.presence_encoder.finish()? {
            EncodedBitBuffer::Constant(value, count) => {
                if !value {
                    // All values are null
                    // TODO: mark the field as constant null in the field descriptor
                    let mut presence_buffer = BitBufferEncoder::encode_blocks(
                        count,
                        value,
                        self.params.temp_store.clone(),
                    )?;
                    presence_buffer.descriptor.kind = shard::BufferKind::Data as i32;
                    presence_buffer.descriptor.embedded_presence = false;
                    presence_buffer.descriptor.embedded_offsets = false;
                    buffers.push(presence_buffer);
                }

                // All values are non-null, no need to write a dedicated presence buffer
                // The absence of a presence buffer indicates all values are present
            }
            EncodedBitBuffer::Blocks(mut presence_buffer) => {
                presence_buffer.descriptor.kind = shard::BufferKind::Presence as i32;
                presence_buffer.descriptor.embedded_presence = false;
                presence_buffer.descriptor.embedded_offsets = false;
                buffers.push(presence_buffer);
            }
        }

        // Finalize statistics if enabled
        let statistics = if let Some(stats_collector) = self.stats_collector {
            let boolean_stats = stats_collector.finish()?;
            Some(EncodedFieldStatistics::Boolean(boolean_stats))
        } else {
            None
        };

        Ok(EncodedField {
            buffers,
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
    use arrow_array::BooleanArray;
    use std::sync::Arc;

    #[test]
    fn test_boolean_field_encoder_with_statistics() -> amudai_common::Result<()> {
        // Create a temporary store for testing
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        // Create a boolean field encoder
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Boolean,
            fixed_size: 0,
            signed: false,
            extended_type: Default::default(),
        };

        let mut encoder = BooleanFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // Test data with mixed true/false values and nulls
        let array1 = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
            None,
            Some(false),
        ]));
        encoder.push_array(array1)?;

        let array2 = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(false),
            Some(true),
        ]));
        encoder.push_array(array2)?;

        // Test push_nulls
        encoder.push_nulls(3)?;

        // Finish encoding and get statistics
        let encoded_field = encoder.finish()?;

        // Verify that statistics were collected
        assert!(
            encoded_field.statistics.is_some(),
            "Statistics should be collected"
        );

        if let Some(EncodedFieldStatistics::Boolean(stats)) = encoded_field.statistics {
            // Verify counts:
            // Array1: 2 true, 2 false, 1 null
            // Array2: 3 true, 1 false, 0 null
            // push_nulls: 0 true, 0 false, 3 null
            // Total: 5 true, 3 false, 4 null, 12 total count
            assert_eq!(stats.true_count, 5, "Expected 5 true values");
            assert_eq!(stats.false_count, 3, "Expected 3 false values");
            assert_eq!(stats.null_count, 4, "Expected 4 null values");
            assert_eq!(stats.count, 12, "Expected 12 total values");

            // Test utility methods
            assert_eq!(stats.non_null_count(), 8, "Expected 8 non-null values");
            assert!(!stats.is_empty(), "Stats should not be empty");
            assert!(!stats.is_all_nulls(), "Not all values are null");
            assert!(!stats.is_all_true(), "Not all values are true");
            assert!(!stats.is_all_false(), "Not all values are false"); // Test ratios (approximately)
            let true_ratio = stats.true_ratio().expect("True ratio should be available");
            let false_ratio = stats
                .false_ratio()
                .expect("False ratio should be available");
            let null_ratio = stats.null_ratio().expect("Null ratio should be available");

            // true_ratio and false_ratio are relative to non-null count (8)
            // null_ratio is relative to total count (12)
            assert!(
                (true_ratio - 5.0 / 8.0).abs() < 0.001,
                "True ratio should be ~0.625, got {true_ratio}"
            );
            assert!(
                (false_ratio - 3.0 / 8.0).abs() < 0.001,
                "False ratio should be ~0.375, got {false_ratio}"
            );
            assert!(
                (null_ratio - 4.0 / 12.0).abs() < 0.001,
                "Null ratio should be ~0.333, got {null_ratio}"
            );
        } else {
            panic!("Expected boolean statistics but got different type or None");
        }

        // Verify that buffers were created
        assert!(
            !encoded_field.buffers.is_empty(),
            "Should have created encoded buffers"
        );

        Ok(())
    }

    #[test]
    fn test_boolean_field_encoder_all_true() -> amudai_common::Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Boolean,
            fixed_size: 0,
            signed: false,
            extended_type: Default::default(),
        };

        let mut encoder = BooleanFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // All true values
        let array = Arc::new(BooleanArray::from(vec![true, true, true, true, true]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        if let Some(EncodedFieldStatistics::Boolean(stats)) = encoded_field.statistics {
            assert_eq!(stats.true_count, 5);
            assert_eq!(stats.false_count, 0);
            assert_eq!(stats.null_count, 0);
            assert!(stats.is_all_true());
            assert!(!stats.is_all_false());
            assert!(!stats.is_all_nulls());
        } else {
            panic!("Expected boolean statistics");
        }

        Ok(())
    }
    #[test]
    fn test_boolean_field_encoder_all_false() -> amudai_common::Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Boolean,
            fixed_size: 0,
            signed: false,
            extended_type: Default::default(),
        };

        let mut encoder = BooleanFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // All false values
        let array = Arc::new(BooleanArray::from(vec![false, false, false]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        if let Some(EncodedFieldStatistics::Boolean(stats)) = encoded_field.statistics {
            assert_eq!(stats.true_count, 0);
            assert_eq!(stats.false_count, 3);
            assert_eq!(stats.null_count, 0);
            assert!(!stats.is_all_true());
            assert!(stats.is_all_false());
            assert!(!stats.is_all_nulls());
        } else {
            panic!("Expected boolean statistics");
        }

        Ok(())
    }

    #[test]
    fn test_boolean_field_encoder_all_nulls() -> amudai_common::Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Boolean,
            fixed_size: 0,
            signed: false,
            extended_type: Default::default(),
        };

        let mut encoder = BooleanFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // All null values
        let array = Arc::new(BooleanArray::from(vec![None, None, None, None]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        if let Some(EncodedFieldStatistics::Boolean(stats)) = encoded_field.statistics {
            assert_eq!(stats.true_count, 0);
            assert_eq!(stats.false_count, 0);
            assert_eq!(stats.null_count, 4);
            assert!(!stats.is_all_true());
            assert!(!stats.is_all_false());
            assert!(stats.is_all_nulls());
            assert_eq!(stats.non_null_count(), 0);
        } else {
            panic!("Expected boolean statistics");
        }

        Ok(())
    }

    #[test]
    fn test_boolean_field_encoder_empty() -> amudai_common::Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Boolean,
            fixed_size: 0,
            signed: false,
            extended_type: Default::default(),
        };

        let encoder = BooleanFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // No arrays pushed
        let encoded_field = encoder.finish()?;

        if let Some(EncodedFieldStatistics::Boolean(stats)) = encoded_field.statistics {
            assert_eq!(stats.true_count, 0);
            assert_eq!(stats.false_count, 0);
            assert_eq!(stats.null_count, 0);
            assert_eq!(stats.count, 0);
            assert!(stats.is_empty());
        } else {
            panic!("Expected boolean statistics");
        }

        Ok(())
    }
}
