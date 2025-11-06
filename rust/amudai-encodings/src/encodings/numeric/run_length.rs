use super::{
    AnalysisOutcome, EncodingContext, EncodingKind, NumericEncoding,
    stats::{NumericStats, NumericStatsCollectorFlags},
    value::{NumericValue, ValueReader, ValueWriter},
};
use crate::encodings::{
    ALIGNMENT_BYTES, AlignedEncMetadata, EncodingConfig, EncodingParameters, EncodingPlan, NullMask,
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;

/// Run-length encoding that stores repeated values as a single value
/// and the number of repetitions. This encoding operates on all numeric
/// values, e.g. integers or floating-point numbers.
pub struct RLEEncoding<T>(std::marker::PhantomData<T>);

impl<T> RLEEncoding<T>
where
    T: NumericValue,
{
    const MIN_RUN_LENGTH: u32 = 2;

    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }

    fn find_run_end(values: &[T], start: usize) -> usize {
        let mut end = start + 1;
        while end < values.len() && values[end] == values[start] {
            end += 1;
        }
        end
    }
}

impl<T> NumericEncoding<T> for RLEEncoding<T>
where
    T: NumericValue,
{
    fn kind(&self) -> EncodingKind {
        EncodingKind::RunLength
    }

    fn is_suitable(&self, _config: &EncodingConfig, stats: &NumericStats<T>) -> bool {
        stats
            .avg_run_length
            .is_some_and(|avg_run_len| avg_run_len >= Self::MIN_RUN_LENGTH)
    }

    fn stats_collector_flags(&self) -> NumericStatsCollectorFlags {
        NumericStatsCollectorFlags::AVG_RUN_LENGTH
    }

    fn analyze(
        &self,
        values: &[T],
        _null_mask: &NullMask,
        config: &EncodingConfig,
        stats: &NumericStats<T>,
        context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>> {
        let mut encoded_size = 2 + RunLengthMetadata::size();

        let mut repeated_vals = context.buffers.get_buffer();
        let mut lengths = context.buffers.get_buffer();
        let mut run_start = 0;
        while run_start < values.len() {
            let run_end = Self::find_run_end(values, run_start);
            let run_length = run_end - run_start;
            repeated_vals.push_typed::<T>(values[run_start]);
            lengths.push_typed::<u32>(run_length as u32);
            run_start = run_end;
        }

        let mut cascading_encodings = vec![None, None];

        // Encode values section either using a cascading or plain encoding.
        let values_outcome = context.numeric_encoders.get::<T>().analyze(
            repeated_vals.typed_data(),
            &NullMask::None,
            &config.make_cascading_config(),
            context,
        )?;
        if let Some(values_outcome) = values_outcome {
            encoded_size += values_outcome.encoded_size;
            cascading_encodings[0] = Some(values_outcome);
        } else {
            encoded_size += repeated_vals.len();
        }

        encoded_size = encoded_size.next_multiple_of(ALIGNMENT_BYTES);

        // Encode lengths section either using a cascading or plain encoding.
        let lengths_outcome = context.numeric_encoders.get::<u32>().analyze(
            lengths.typed_data(),
            &NullMask::None,
            &config.make_cascading_config(),
            context,
        )?;
        if let Some(lengths_outcome) = lengths_outcome {
            encoded_size += lengths_outcome.encoded_size;
            cascading_encodings[1] = Some(lengths_outcome);
        } else {
            encoded_size += lengths.len();
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
        _null_mask: &NullMask,
        target: &mut AlignedByteVec,
        plan: &EncodingPlan,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize> {
        let initial_size = target.len();
        target.write_value::<u16>(self.kind() as u16);

        let mut metadata = RunLengthMetadata::initialize(target);

        let mut repeated_vals = context.buffers.get_buffer();
        let mut lengths = context.buffers.get_buffer();
        let mut run_start = 0;
        while run_start < values.len() {
            let run_end = Self::find_run_end(values, run_start);
            let run_length = run_end - run_start;
            repeated_vals.push_typed::<T>(values[run_start]);
            lengths.push_typed::<u32>(run_length as u32);
            run_start = run_end;
        }
        metadata.runs_count = repeated_vals.len() / T::SIZE;

        // Encode values section either using a cascading or plain encoding.
        let prev_pos = target.len();
        if let Some(values_plan) = plan.cascading_encodings[0].as_ref() {
            context.numeric_encoders.get::<T>().encode(
                repeated_vals.typed_data(),
                &NullMask::None,
                target,
                values_plan,
                context,
            )?;
            metadata.values_cascading = true;
        } else {
            target.write_values(&repeated_vals);
        }
        metadata.values_size = target.len() - prev_pos;

        let alignment = target.len().next_multiple_of(ALIGNMENT_BYTES);
        target.resize(alignment, 0);

        // Encode lengths section either using a cascading or plain encoding.
        if let Some(lengths_plan) = plan.cascading_encodings[1].as_ref() {
            context.numeric_encoders.get::<u32>().encode(
                lengths.typed_data(),
                &NullMask::None,
                target,
                lengths_plan,
                context,
            )?;
            metadata.lengths_cascading = true;
        } else {
            target.write_values(&lengths);
        }

        metadata.finalize(target);

        Ok(target.len() - initial_size)
    }

    fn decode(
        &self,
        buffer: &[u8],
        _value_count: usize,
        params: Option<&EncodingParameters>,
        target: &mut AlignedByteVec,
        context: &EncodingContext,
    ) -> amudai_common::Result<()> {
        let (metadata, buffer) = RunLengthMetadata::read_from(buffer)?;

        // Decode repeated values.
        if buffer.len() < metadata.values_size {
            return Err(Error::invalid_format("Encoded buffer size is too small"));
        }
        let encoded_values = &buffer[..metadata.values_size];
        let mut values = context.buffers.get_buffer();
        if metadata.values_cascading {
            context.numeric_encoders.get::<T>().decode(
                encoded_values,
                metadata.runs_count,
                params,
                &mut values,
                context,
            )?;
        } else {
            values.extend_from_slice(encoded_values);
        }
        let values = values.typed_data::<T>();

        let lengths_offset = metadata.values_size.next_multiple_of(ALIGNMENT_BYTES);
        let encoded_lengths = &buffer[lengths_offset..];

        // Decode run lengths.
        let mut lengths = context.buffers.get_buffer();
        if metadata.lengths_cascading {
            context.numeric_encoders.get::<u32>().decode(
                encoded_lengths,
                metadata.runs_count,
                params,
                &mut lengths,
                context,
            )?;
        } else {
            let lengths_size = metadata.runs_count * std::mem::size_of::<u32>();
            if encoded_lengths.len() < lengths_size {
                return Err(Error::invalid_format("Encoded buffer size is too small"));
            }
            lengths.extend_from_slice(&encoded_lengths[..lengths_size]);
        }
        let lengths = lengths.typed_data::<u32>();

        for (value, &length) in values.iter().zip(lengths) {
            target.resize_typed(target.len() / T::SIZE + length as usize, *value);
        }

        Ok(())
    }

    fn inspect(
        &self,
        buffer: &[u8],
        context: &EncodingContext,
    ) -> amudai_common::Result<EncodingPlan> {
        let (metadata, buffer) = RunLengthMetadata::read_from(buffer)?;
        let values_plan = if metadata.values_cascading {
            Some(
                context
                    .numeric_encoders
                    .get::<T>()
                    .inspect(&buffer[..metadata.values_size], context)?,
            )
        } else {
            None
        };
        let lengths_offset = metadata.values_size.next_multiple_of(ALIGNMENT_BYTES);
        let encoded_lengths = &buffer[lengths_offset..];
        let lengths_plan = if metadata.lengths_cascading {
            Some(
                context
                    .numeric_encoders
                    .get::<u32>()
                    .inspect(encoded_lengths, context)?,
            )
        } else {
            None
        };
        Ok(EncodingPlan {
            encoding: self.kind(),
            parameters: Default::default(),
            cascading_encodings: vec![values_plan, lengths_plan],
        })
    }
}

struct RunLengthMetadata {
    start_offset: usize,
    /// Size of the encoded values section in bytes.
    pub values_size: usize,
    /// Number of runs.
    pub runs_count: usize,
    /// Whether the values section encoding is cascading.
    pub values_cascading: bool,
    /// Whether the lengths section encoding is cascading.
    pub lengths_cascading: bool,
}

impl AlignedEncMetadata for RunLengthMetadata {
    fn own_size() -> usize {
        10
    }

    fn initialize(target: &mut AlignedByteVec) -> Self {
        let start_offset = target.len();
        target.resize(start_offset + Self::size(), 0);
        Self {
            start_offset,
            values_size: 0,
            runs_count: 0,
            values_cascading: false,
            lengths_cascading: false,
        }
    }

    fn finalize(self, target: &mut AlignedByteVec) {
        target.write_value_at::<u32>(self.start_offset, self.values_size as u32);
        target.write_value_at::<u32>(self.start_offset + 4, self.runs_count as u32);
        target.write_value_at::<u8>(
            self.start_offset + 8,
            if self.values_cascading { 1 } else { 0 },
        );
        target.write_value_at::<u8>(
            self.start_offset + 9,
            if self.lengths_cascading { 1 } else { 0 },
        );
    }

    fn read_from(buffer: &[u8]) -> amudai_common::Result<(Self, &[u8])> {
        if buffer.len() < Self::size() {
            return Err(Error::invalid_format("Encoded buffer size is too small"));
        }
        Ok((
            Self {
                start_offset: 0,
                values_size: buffer.read_value::<u32>(0) as usize,
                runs_count: buffer.read_value::<u32>(4) as usize,
                values_cascading: buffer.read_value::<u8>(8) != 0,
                lengths_cascading: buffer.read_value::<u8>(9) != 0,
            },
            &buffer[Self::size()..],
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::encodings::{
        EncodingConfig, EncodingKind, EncodingPlan, NullMask, numeric::EncodingContext,
    };
    use amudai_bytes::buffer::AlignedByteVec;

    #[test]
    fn test_round_trip() {
        let config = EncodingConfig::default().with_allowed_encodings(&[EncodingKind::RunLength]);
        let context = EncodingContext::new();
        let data: Vec<i64> = (0..640).flat_map(|i| (0..100).map(move |_| i)).collect();
        let outcome = context
            .numeric_encoders
            .get::<i64>()
            .analyze(&data, &NullMask::None, &config, &context)
            .unwrap();
        assert!(outcome.is_some());
        let outcome = outcome.unwrap();
        let encoded_size1 = outcome.encoded_size;
        let plan = outcome.into_plan();

        assert_eq!(
            EncodingPlan {
                encoding: EncodingKind::RunLength,
                parameters: Default::default(),
                cascading_encodings: vec![
                    Some(EncodingPlan {
                        encoding: EncodingKind::Delta,
                        parameters: Default::default(),
                        cascading_encodings: vec![Some(EncodingPlan {
                            encoding: EncodingKind::SingleValue,
                            parameters: Default::default(),
                            cascading_encodings: vec![]
                        })]
                    }),
                    Some(EncodingPlan {
                        encoding: EncodingKind::SingleValue,
                        parameters: Default::default(),
                        cascading_encodings: vec![]
                    })
                ]
            },
            plan
        );

        let mut encoded = AlignedByteVec::new();
        let encoded_size2 = context
            .numeric_encoders
            .get::<i64>()
            .encode(&data, &NullMask::None, &mut encoded, &plan, &context)
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
}
