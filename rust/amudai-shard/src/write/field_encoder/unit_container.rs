//! `Struct` and `FixedSizeList` field encoder.

use std::sync::Arc;

use amudai_blockstream::write::bit_buffer::{BitBufferEncoder, EncodedBitBuffer};
use amudai_common::{Result, error::Error};
use amudai_data_stats::container::StructStatsCollector;
use amudai_format::defs::shard;
use arrow_array::{Array, cast::AsArray};

use crate::write::field_encoder::FieldEncoderParams;

use super::{EncodedField, FieldEncoderOps};

/// Encoder for fields that represent single-item containers, such as
/// `Struct` or `FixedSizeList` (in contrast to multi-item containers
/// like `List` or `Map`).
///
/// The "values" encoded by this encoder are validity bits, which indicate
/// presence or non-null status.
pub struct UnitContainerFieldEncoder {
    params: FieldEncoderParams,
    presence_encoder: BitBufferEncoder,
    stats_collector: StructStatsCollector,
}

impl UnitContainerFieldEncoder {
    pub(crate) fn create(params: &FieldEncoderParams) -> Result<Box<dyn FieldEncoderOps>> {
        Ok(Box::new(UnitContainerFieldEncoder {
            params: params.clone(),
            presence_encoder: BitBufferEncoder::new(
                params.temp_store.clone(),
                params.encoding_profile,
            ),
            stats_collector: StructStatsCollector::new(),
        }))
    }
}

impl FieldEncoderOps for UnitContainerFieldEncoder {
    fn push_array(&mut self, array: Arc<dyn Array>) -> Result<()> {
        let buf_opt = if array.data_type().is_nested() {
            array.nulls().map(arrow_buffer::NullBuffer::inner)
        } else if matches!(array.data_type(), arrow_schema::DataType::Boolean) {
            Some(array.as_boolean().values())
        } else {
            return Err(Error::invalid_arg(
                "array",
                format!("unexpected array type {:?}", array.data_type()),
            ));
        };

        let array_len = array.len();
        if let Some(buf) = buf_opt {
            self.presence_encoder.append(buf)?;
            self.stats_collector
                .process_structs(array_len, array.null_count());
        } else {
            self.presence_encoder.append_repeated(array_len, true)?;
            self.stats_collector.process_structs(array_len, 0);
        }
        Ok(())
    }

    fn push_nulls(&mut self, count: usize) -> Result<()> {
        self.presence_encoder.append_repeated(count, false)?;
        // Add null containers to statistics
        self.stats_collector.process_nulls(count as u64);
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<EncodedField> {
        let mut buffers = Vec::new();

        // Collect container statistics
        let container_stats = self.stats_collector.finalize();

        match self.presence_encoder.finish()? {
            EncodedBitBuffer::Constant(value, count) => {
                if !value {
                    // TODO: do not write out any buffers, mark the field as constant Null
                    // in the field descriptor.
                    let mut presence = BitBufferEncoder::encode_blocks(
                        count,
                        value,
                        self.params.temp_store.clone(),
                    )?;
                    presence.descriptor.kind = shard::BufferKind::Presence as i32;
                    presence.descriptor.embedded_presence = false;
                    presence.descriptor.embedded_offsets = false;
                    buffers.push(presence);
                }

                // The field has a trivial presence in all stripe positions, no need to write
                // out dedicated presence buffer.
            }
            EncodedBitBuffer::Blocks(mut presence) => {
                presence.descriptor.kind = shard::BufferKind::Presence as i32;
                presence.descriptor.embedded_presence = false;
                presence.descriptor.embedded_offsets = false;
                buffers.push(presence);
            }
        }
        Ok(EncodedField {
            buffers,
            statistics: super::EncodedFieldStatistics::StructContainer(container_stats),
            dictionary_size: None,
            constant_value: None,
        })
    }
}
