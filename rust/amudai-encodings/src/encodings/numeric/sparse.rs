use super::{
    EncodingContext, NumericEncoding,
    stats::{NumericStats, NumericStatsCollectorFlags},
    value::{NumericValue, ValueReader, ValueWriter},
};
use crate::encodings::{
    ALIGNMENT_BYTES, AlignedEncMetadata, AnalysisOutcome, EncodingConfig, EncodingKind,
    EncodingParameters, EncodingPlan, NullMask,
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;

pub struct SparseEncoding<T>(std::marker::PhantomData<T>);

impl<T> SparseEncoding<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> NumericEncoding<T> for SparseEncoding<T>
where
    T: NumericValue,
{
    fn kind(&self) -> EncodingKind {
        EncodingKind::Sparse
    }

    fn is_suitable(&self, _config: &EncodingConfig, stats: &NumericStats<T>) -> bool {
        (stats.nulls_count as f64 / stats.values_count as f64) > 0.995
    }

    fn stats_collector_flags(&self) -> NumericStatsCollectorFlags {
        NumericStatsCollectorFlags::empty()
    }

    fn analyze(
        &self,
        values: &[T],
        null_mask: &NullMask,
        config: &EncodingConfig,
        stats: &NumericStats<T>,
        context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>> {
        let mut encoded_size = 2 + SparseMetadata::size();

        let mut positions = context.buffers.get_buffer();
        let mut valid_values = context.buffers.get_buffer();
        for (i, &value) in values.iter().enumerate() {
            if !null_mask.is_null(i) {
                positions.push_typed::<u32>(i as u32);
                valid_values.push_typed::<T>(value);
            }
        }

        let mut cascading_encodings = vec![None, None];

        let positions_outcome = context.numeric_encoders.get::<u32>().analyze(
            positions.typed_data::<u32>(),
            &NullMask::None,
            &config.make_cascading_config(),
            context,
        )?;
        if let Some(outcome) = positions_outcome {
            encoded_size += outcome.encoded_size;
            cascading_encodings[0] = Some(outcome);
        } else {
            encoded_size += positions.len();
        }

        encoded_size = encoded_size.next_multiple_of(ALIGNMENT_BYTES);

        // Only analyze values if we have non-null values
        if !valid_values.is_empty() {
            let values_outcome = context.numeric_encoders.get::<T>().analyze(
                valid_values.typed_data::<T>(),
                &NullMask::None,
                &config.make_cascading_config(),
                context,
            )?;
            if let Some(cascading_outcome) = values_outcome {
                encoded_size += cascading_outcome.encoded_size;
                cascading_encodings[1] = Some(cascading_outcome);
            } else {
                encoded_size += valid_values.len();
            }
        }

        Ok(Some(AnalysisOutcome {
            encoding: self.kind(),
            cascading_outcomes: cascading_encodings,
            encoded_size,
            compression_ratio: stats.original_size as f64 / encoded_size as f64,
            parameters: Default::default(),
        }))
    }

    fn encode(
        &self,
        values: &[T],
        null_mask: &NullMask,
        target: &mut AlignedByteVec,
        plan: &EncodingPlan,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize> {
        let initial_size = target.len();
        target.write_value::<u16>(self.kind() as u16);

        let mut metadata = SparseMetadata::initialize(target);

        let mut positions = context.buffers.get_buffer();
        let mut valid_values = context.buffers.get_buffer();
        for (i, &value) in values.iter().enumerate() {
            if !null_mask.is_null(i) {
                positions.push_typed::<u32>(i as u32);
                valid_values.push_typed::<T>(value);
            }
        }
        metadata.positions_count = positions.len() / std::mem::size_of::<u32>();

        // Encode non-null value positions.
        let prev_pos = target.len();
        if let Some(positions_plan) = plan.cascading_encodings[0].as_ref() {
            context.numeric_encoders.get::<u32>().encode(
                positions.typed_data::<u32>(),
                &NullMask::None,
                target,
                positions_plan,
                context,
            )?;
            metadata.positions_cascading = true;
        } else {
            target.extend_from_slice(positions.as_slice());
        }
        metadata.positions_size = target.len() - prev_pos;

        let alignment = target.len().next_multiple_of(ALIGNMENT_BYTES);
        target.resize(alignment, 0);

        // Encode non-null values, if any
        if !valid_values.is_empty() {
            if let Some(values_plan) = plan.cascading_encodings[1].as_ref() {
                context.numeric_encoders.get::<T>().encode(
                    valid_values.typed_data::<T>(),
                    &NullMask::None,
                    target,
                    values_plan,
                    context,
                )?;
                metadata.values_cascading = true;
            } else {
                target.extend_from_slice(valid_values.as_slice());
            }
        }

        metadata.finalize(target);

        Ok(target.len() - initial_size)
    }

    fn decode(
        &self,
        buffer: &[u8],
        value_count: usize,
        _params: Option<&EncodingParameters>,
        target: &mut AlignedByteVec,
        context: &EncodingContext,
    ) -> amudai_common::Result<()> {
        let (metadata, buffer) = SparseMetadata::read_from(buffer)?;

        // Decode non-null values positions.
        if buffer.len() < metadata.positions_size {
            return Err(Error::invalid_format("Encoded buffer size is too small"));
        }
        let encoded_positions = &buffer[..metadata.positions_size];
        let mut positions = context.buffers.get_buffer();
        if metadata.positions_cascading {
            context.numeric_encoders.get::<u32>().decode(
                encoded_positions,
                metadata.positions_count,
                None,
                &mut positions,
                context,
            )?;
        } else {
            positions.extend_from_slice(encoded_positions);
        }

        let values_offset = metadata.positions_size.next_multiple_of(ALIGNMENT_BYTES);
        let values_buffer = &buffer[values_offset..];

        target.resize_zeroed::<T>(value_count);

        // Decode and populate non-null values, if any
        if metadata.positions_count > 0 {
            let mut valid_values = context.buffers.get_buffer();
            if metadata.values_cascading {
                context.numeric_encoders.get::<T>().decode(
                    values_buffer,
                    metadata.positions_count,
                    None,
                    &mut valid_values,
                    context,
                )?;
            } else {
                let values_count = metadata.positions_count;
                let values_size = values_count * T::SIZE;
                if values_buffer.len() < values_size {
                    return Err(Error::invalid_format("Encoded buffer size is too small"));
                }
                valid_values.extend_from_slice(&values_buffer[..values_size]);
            }

            let positions = positions.typed_data::<u32>();
            let valid_values = valid_values.typed_data::<T>();
            let target_slice = target.typed_data_mut::<T>();
            for (&pos, &value) in positions.iter().zip(valid_values) {
                target_slice[pos as usize] = value;
            }
        }

        Ok(())
    }

    fn inspect(
        &self,
        buffer: &[u8],
        context: &EncodingContext,
    ) -> amudai_common::Result<EncodingPlan> {
        let (metadata, buffer) = SparseMetadata::read_from(buffer)?;
        let positions_plan = if metadata.positions_cascading {
            Some(
                context
                    .numeric_encoders
                    .get::<u32>()
                    .inspect(&buffer[..metadata.positions_size], context)?,
            )
        } else {
            None
        };
        let values_buffer = &buffer[metadata.positions_size..];
        let values_plan = if metadata.values_cascading {
            Some(
                context
                    .numeric_encoders
                    .get::<T>()
                    .inspect(values_buffer, context)?,
            )
        } else {
            None
        };
        Ok(EncodingPlan {
            encoding: self.kind(),
            parameters: Default::default(),
            cascading_encodings: vec![positions_plan, values_plan],
        })
    }
}

struct SparseMetadata {
    start_offset: usize,
    pub positions_count: usize,
    pub positions_size: usize,
    pub positions_cascading: bool,
    pub values_cascading: bool,
}

impl AlignedEncMetadata for SparseMetadata {
    fn own_size() -> usize {
        10
    }

    fn initialize(target: &mut AlignedByteVec) -> Self {
        let start_offset = target.len();
        target.resize(start_offset + Self::size(), 0);
        Self {
            start_offset,
            positions_count: 0,
            positions_size: 0,
            positions_cascading: false,
            values_cascading: false,
        }
    }

    fn finalize(self, target: &mut AlignedByteVec) {
        target.write_value_at::<u32>(self.start_offset, self.positions_count as u32);
        target.write_value_at::<u32>(self.start_offset + 4, self.positions_size as u32);
        target.write_value_at::<u8>(
            self.start_offset + 8,
            if self.positions_cascading { 1u8 } else { 0u8 },
        );
        target.write_value_at::<u8>(
            self.start_offset + 9,
            if self.values_cascading { 1u8 } else { 0u8 },
        );
    }

    fn read_from(buffer: &[u8]) -> amudai_common::Result<(Self, &[u8])> {
        if buffer.len() < Self::size() {
            return Err(Error::invalid_format("Encoded buffer size is too small"));
        }
        Ok((
            Self {
                start_offset: 0,
                positions_count: buffer.read_value::<u32>(0) as usize,
                positions_size: buffer.read_value::<u32>(4) as usize,
                positions_cascading: buffer.read_value::<u8>(8) != 0,
                values_cascading: buffer.read_value::<u8>(9) != 0,
            },
            &buffer[Self::size()..],
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::encodings::{EncodingConfig, EncodingContext, EncodingKind, EncodingPlan, NullMask};
    use amudai_bytes::buffer::AlignedByteVec;

    #[test]
    fn test_round_trip() {
        let config = EncodingConfig::default().with_allowed_encodings(&[EncodingKind::Sparse]);
        let context = EncodingContext::new();

        let mut valid_positions = [
            (0..65436).map(|_| false).collect::<Vec<_>>(),
            (0..100).map(|_| true).collect::<Vec<_>>(),
        ]
        .concat();
        fastrand::shuffle(&mut valid_positions);
        let nulls = arrow_buffer::NullBuffer::from(valid_positions);
        let data: Vec<i64> = (0..nulls.len())
            .map(|i| {
                if nulls.is_null(i) {
                    0
                } else {
                    fastrand::i64(-1000..1000)
                }
            })
            .collect();
        let null_mask = NullMask::from(Some(&nulls));

        let outcome = context
            .numeric_encoders
            .get::<i64>()
            .analyze(&data, &null_mask, &config, &context)
            .unwrap();
        assert!(outcome.is_some());
        let outcome = outcome.unwrap();
        let encoded_size1 = outcome.encoded_size;
        let plan = outcome.into_plan();

        assert_eq!(
            EncodingPlan {
                encoding: EncodingKind::Sparse,
                parameters: Default::default(),
                cascading_encodings: vec![None, None]
            },
            plan
        );

        let mut encoded = AlignedByteVec::new();
        let encoded_size2 = context
            .numeric_encoders
            .get::<i64>()
            .encode(&data, &null_mask, &mut encoded, &plan, &context)
            .unwrap();
        assert_eq!(encoded_size1, encoded_size2);

        // Test that inspect() returns the same encoding plan
        let inspected_plan = context
            .numeric_encoders
            .get::<i64>()
            .inspect(&encoded, &context)
            .unwrap();
        assert_eq!(plan, inspected_plan);

        let mut decoded = AlignedByteVec::new();
        context
            .numeric_encoders
            .get::<i64>()
            .decode(&encoded, data.len(), None, &mut decoded, &context)
            .unwrap();
        for (a, b) in data.iter().zip(decoded.typed_data::<i64>()) {
            assert_eq!(a, b);
        }
    }

    #[test]
    fn test_all_null_values() {
        let context = EncodingContext::new();

        // Create data where all values are null
        let data_len = 1000;
        let data: Vec<i64> = (0..data_len).map(|_| 0).collect();
        let nulls = arrow_buffer::NullBuffer::new_null(data_len);
        let null_mask = NullMask::from(Some(&nulls));

        let plan = EncodingPlan {
            encoding: EncodingKind::Sparse,
            parameters: Default::default(),
            cascading_encodings: vec![
                None, // positions encoding
                Some(EncodingPlan {
                    encoding: EncodingKind::SingleValue,
                    parameters: Default::default(),
                    cascading_encodings: vec![],
                }),
            ],
        };

        let mut encoded = AlignedByteVec::new();
        let encoded_size = context
            .numeric_encoders
            .get::<i64>()
            .encode(&data, &null_mask, &mut encoded, &plan, &context)
            .unwrap();

        // ensure encoding size is greater than zero (metadata should be present)
        assert!(encoded_size > 0);

        // Test round-trip decode
        let mut decoded = AlignedByteVec::new();
        context
            .numeric_encoders
            .get::<i64>()
            .decode(&encoded, data.len(), None, &mut decoded, &context)
            .unwrap();

        // All decoded values should be zero (since all were null)
        let decoded_data = decoded.typed_data::<i64>();
        assert_eq!(decoded_data.len(), data_len);
        for &value in decoded_data {
            assert_eq!(value, 0); // Default value for null entries
        }
    }
}
