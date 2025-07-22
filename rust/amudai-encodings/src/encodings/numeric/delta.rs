use super::{
    AnalysisOutcome, EncodingKind, NumericEncoding,
    stats::{NumericStats, NumericStatsCollectorFlags},
    value::{IntegerValue, ValueReader, ValueWriter},
};
use crate::encodings::{
    AlignedEncMetadata, EncodingConfig, EncodingContext, EncodingParameters, EncodingPlan, NullMask,
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;

/// Delta encoding that stores the difference between consecutive values.
/// Since the subtraction operation between consecutive values is not safe
/// for floating point numbers, this encoding is only available for integers.
pub struct DeltaEncoding<T>(std::marker::PhantomData<T>);

impl<T> DeltaEncoding<T> {
    /// Minimum number of values required for delta encoding to be considered suitable.
    const MIN_VALUES_COUNT: u32 = 8;

    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> NumericEncoding<T> for DeltaEncoding<T>
where
    T: IntegerValue,
{
    fn kind(&self) -> EncodingKind {
        EncodingKind::Delta
    }

    fn is_suitable(&self, config: &EncodingConfig, stats: &NumericStats<T>) -> bool {
        if config.max_cascading_levels < 2 {
            // For efficient compression, diffs stream must be encoded with
            // a cascading encoding scheme.
            return false;
        }
        if stats.values_count < Self::MIN_VALUES_COUNT {
            return false;
        }
        if let Some(deltas_min_bits_count) = stats.deltas_min_bits_count {
            if deltas_min_bits_count == 0
                || (T::BITS_COUNT as f64 / deltas_min_bits_count as f64)
                    < config.min_compression_ratio
            {
                return false;
            }
        }
        true
    }

    fn stats_collector_flags(&self) -> NumericStatsCollectorFlags {
        NumericStatsCollectorFlags::DELTAS_MIN_BITS_COUNT
    }

    fn analyze(
        &self,
        values: &[T],
        null_mask: &NullMask,
        config: &EncodingConfig,
        stats: &NumericStats<T>,
        context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>> {
        if values.len() < Self::MIN_VALUES_COUNT as usize {
            return Ok(None);
        }
        let encoded_size = 2 + DeltaMetadata::<T>::size();

        let mut diffs = context.buffers.get_buffer();
        for pair in values.windows(2) {
            diffs.push_typed::<T>(pair[1].wrapping_sub(&pair[0]));
        }

        let diff_enc = context.numeric_encoders.get::<T>();
        Ok(diff_enc
            .analyze(
                diffs.typed_data(),
                null_mask,
                &config.make_cascading_config(),
                context,
            )?
            .map(|outcome| {
                let encoded_size = encoded_size + outcome.encoded_size;
                AnalysisOutcome {
                    encoding: self.kind(),
                    cascading_outcomes: vec![Some(outcome)],
                    encoded_size,
                    compression_ratio: stats.original_size as f64 / encoded_size as f64,
                    parameters: Default::default(),
                }
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
        if values.len() < Self::MIN_VALUES_COUNT as usize {
            // Fallback to plain encoding if there are not enough values.
            return context.numeric_encoders.get::<T>().encode(
                values,
                null_mask,
                target,
                &EncodingPlan {
                    encoding: EncodingKind::Plain,
                    parameters: Default::default(),
                    cascading_encodings: vec![],
                },
                context,
            );
        }

        let initial_size = target.len();
        target.write_value::<u16>(self.kind() as u16);

        let mut metadata = DeltaMetadata::initialize(target);
        metadata.base = values[0];

        let mut diffs = context.buffers.get_buffer();
        for pair in values.windows(2) {
            diffs.push_typed::<T>(pair[1].wrapping_sub(&pair[0]));
        }

        let diff_plan = plan.cascading_encodings[0]
            .as_ref()
            .expect("Differences encoding plan must be provided");
        context.numeric_encoders.get::<T>().encode(
            diffs.typed_data(),
            null_mask,
            target,
            diff_plan,
            context,
        )?;

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
        let (metadata, buffer) = DeltaMetadata::read_from(buffer)?;

        let mut diffs = context.buffers.get_buffer();
        context.numeric_encoders.get::<T>().decode(
            buffer,
            value_count - 1,
            None,
            &mut diffs,
            context,
        )?;
        let diffs = diffs.typed_data::<T>();

        let mut base: T = metadata.base;
        target.push_typed::<T>(base);
        for diff in diffs {
            base = base.wrapping_add(diff);
            target.push_typed::<T>(base);
        }

        Ok(())
    }

    fn inspect(
        &self,
        buffer: &[u8],
        context: &EncodingContext,
    ) -> amudai_common::Result<EncodingPlan> {
        let (_, buffer) = DeltaMetadata::<T>::read_from(buffer)?;
        let diffs_plan = context
            .numeric_encoders
            .get::<T>()
            .inspect(buffer, context)?;
        Ok(EncodingPlan {
            encoding: self.kind(),
            parameters: Default::default(),
            cascading_encodings: vec![Some(diffs_plan)],
        })
    }
}

struct DeltaMetadata<T> {
    start_offset: usize,
    pub base: T,
}

impl<T> AlignedEncMetadata for DeltaMetadata<T>
where
    T: IntegerValue,
{
    fn own_size() -> usize {
        std::mem::size_of::<T>()
    }

    fn initialize(target: &mut AlignedByteVec) -> Self {
        let start_offset = target.len();
        target.resize(start_offset + Self::size(), 0);
        Self {
            start_offset,
            base: T::zero(),
        }
    }

    fn finalize(self, target: &mut AlignedByteVec) {
        target.write_value_at::<T>(self.start_offset, self.base);
    }

    fn read_from(buffer: &[u8]) -> amudai_common::Result<(Self, &[u8])> {
        if buffer.len() < Self::size() {
            return Err(Error::invalid_format("Encoded buffer size is too small"));
        }
        Ok((
            Self {
                start_offset: 0,
                base: buffer.read_value::<T>(0),
            },
            &buffer[Self::size()..],
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::encodings::{EncodingConfig, EncodingContext, EncodingKind, NullMask};
    use amudai_bytes::buffer::AlignedByteVec;

    #[test]
    fn test_round_trip() {
        let config = EncodingConfig::default().with_allowed_encodings(&[EncodingKind::Delta]);
        let context = EncodingContext::new();
        let data: Vec<i64> = (10000011..10065547).collect();
        let outcome = context
            .numeric_encoders
            .get::<i64>()
            .analyze(&data, &NullMask::None, &config, &context)
            .unwrap();
        assert!(outcome.is_some());
        let outcome = outcome.unwrap();
        assert_eq!(outcome.encoding, EncodingKind::Delta);
        let encoded_size1 = outcome.encoded_size;
        let plan = outcome.into_plan();

        let mut encoded = AlignedByteVec::new();
        let encoded_size2 = context
            .numeric_encoders
            .get::<i64>()
            .encode(&data, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();
        assert_eq!(encoded_size1, encoded_size2);

        // Validate that inspect() returns the same encoding plan as used for encoding
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
