use super::{
    EncodingContext, NumericEncoding,
    stats::{NumericStats, NumericStatsCollectorFlags},
    value::{SIntegerValue, ValueReader, ValueWriter},
};
use crate::encodings::{
    AlignedEncMetadata, AnalysisOutcome, EncodingConfig, EncodingKind, EncodingParameters,
    EncodingPlan, NullMask,
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;
use num_traits::{AsPrimitive, One};

pub struct ZigZagEncoding<T>(std::marker::PhantomData<T>);

impl<T> ZigZagEncoding<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }

    fn convert_to_unsigned(values: &[T], target: &mut AlignedByteVec)
    where
        T: SIntegerValue,
    {
        target.reserve(values.len() * T::SIZE);
        for &value in values {
            target.push_typed::<T::UnsignedType>(
                ((value << 1) ^ (value >> (T::BITS_COUNT - 1))).as_(),
            );
        }
    }

    fn convert_to_signed(unsigned_values: &[T::UnsignedType], target: &mut AlignedByteVec)
    where
        T: SIntegerValue,
    {
        target.reserve(unsigned_values.len() * T::SIZE);
        for &value in unsigned_values {
            let shr1 = value >> 1;
            let a1: T = (value & T::UnsignedType::one()).as_();
            let neg: T::UnsignedType = (-a1).as_();
            let or = shr1 ^ neg;
            target.push_typed::<T>(or.as_());
        }
    }
}

impl<T> NumericEncoding<T> for ZigZagEncoding<T>
where
    T: SIntegerValue,
{
    fn kind(&self) -> EncodingKind {
        EncodingKind::ZigZag
    }

    fn is_suitable(&self, _config: &EncodingConfig, stats: &NumericStats<T>) -> bool {
        if let Some(min) = stats.min {
            min < T::zero()
        } else {
            false
        }
    }

    fn stats_collector_flags(&self) -> NumericStatsCollectorFlags {
        NumericStatsCollectorFlags::MIN_MAX
    }

    fn analyze(
        &self,
        values: &[T],
        null_mask: &NullMask,
        config: &EncodingConfig,
        stats: &NumericStats<T>,
        context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>> {
        let mut encoded_size = 2 + ZigZagMetadata::size();

        let mut unsigned_values = context.buffers.get_buffer();
        Self::convert_to_unsigned(values, &mut unsigned_values);

        let mut cascading_encodings = vec![None];
        let cascading_outcome = context.numeric_encoders.get::<T::UnsignedType>().analyze(
            unsigned_values.typed_data(),
            null_mask,
            &config.make_cascading_config(),
            context,
        )?;
        if let Some(cascading_outcome) = cascading_outcome {
            encoded_size += cascading_outcome.encoded_size;
            cascading_encodings[0] = Some(cascading_outcome);
        } else {
            encoded_size += unsigned_values.len() * T::SIZE;
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

        let mut metadata = ZigZagMetadata::initialize(target);

        let mut unsigned_values = context.buffers.get_buffer();
        unsigned_values.reserve(values.len() * T::SIZE);
        for &value in values {
            unsigned_values.push_typed::<T::UnsignedType>(
                ((value << 1) ^ (value >> (T::BITS_COUNT - 1))).as_(),
            );
        }

        if let Some(cascading_plan) = plan.cascading_encodings[0].as_ref() {
            context.numeric_encoders.get::<T::UnsignedType>().encode(
                unsigned_values.typed_data(),
                null_mask,
                target,
                cascading_plan,
                context,
            )?;
            metadata.cascading = true;
        } else {
            target.extend_from_slice(unsigned_values.as_slice());
        }

        metadata.finalize(target);

        Ok(target.len() - initial_size)
    }

    fn decode(
        &self,
        buffer: &[u8],
        value_count: usize,
        params: Option<&EncodingParameters>,
        target: &mut AlignedByteVec,
        context: &EncodingContext,
    ) -> amudai_common::Result<()> {
        let (metadata, buffer) = ZigZagMetadata::read_from(buffer)?;
        if metadata.cascading {
            let mut decoded = context.buffers.get_buffer();
            context.numeric_encoders.get::<T::UnsignedType>().decode(
                buffer,
                value_count,
                params,
                &mut decoded,
                context,
            )?;
            Self::convert_to_signed(decoded.typed_data::<T::UnsignedType>(), target);
        } else {
            let unsigned_values: &[T::UnsignedType] = unsafe { std::mem::transmute(buffer) };
            Self::convert_to_signed(unsigned_values, target);
        }

        Ok(())
    }

    fn inspect(
        &self,
        buffer: &[u8],
        context: &EncodingContext,
    ) -> amudai_common::Result<EncodingPlan> {
        let (metadata, buffer) = ZigZagMetadata::read_from(buffer)?;
        let cascading_plan = if metadata.cascading {
            Some(
                context
                    .numeric_encoders
                    .get::<T::UnsignedType>()
                    .inspect(buffer, context)?,
            )
        } else {
            None
        };
        Ok(EncodingPlan {
            encoding: self.kind(),
            parameters: Default::default(),
            cascading_encodings: vec![cascading_plan],
        })
    }
}

struct ZigZagMetadata {
    start_offset: usize,
    pub cascading: bool,
}

impl AlignedEncMetadata for ZigZagMetadata {
    fn own_size() -> usize {
        1
    }

    fn initialize(target: &mut AlignedByteVec) -> Self {
        let start_offset = target.len();
        target.resize(start_offset + Self::size(), 0);
        Self {
            start_offset,
            cascading: false,
        }
    }

    fn finalize(self, target: &mut AlignedByteVec) {
        target.write_value_at::<u8>(self.start_offset, if self.cascading { 1u8 } else { 0u8 });
    }

    fn read_from(buffer: &[u8]) -> amudai_common::Result<(Self, &[u8])> {
        if buffer.len() < Self::size() {
            return Err(Error::invalid_format("Encoded buffer size is too small"));
        }
        Ok((
            Self {
                start_offset: 0,
                cascading: buffer.read_value::<u8>(0) != 0,
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
        let config = EncodingConfig::default().with_allowed_encodings(&[EncodingKind::ZigZag]);
        let context = EncodingContext::new();
        let data: Vec<i32> = (-127..128).flat_map(|i| (0..250).map(move |_| i)).collect();
        let outcome = context
            .numeric_encoders
            .get::<i32>()
            .analyze(&data, &NullMask::None, &config, &context)
            .unwrap();
        assert!(outcome.is_some());
        let outcome = outcome.unwrap();
        let encoded_size1 = outcome.encoded_size;
        let plan = outcome.into_plan();

        assert_eq!(
            EncodingPlan {
                encoding: EncodingKind::ZigZag,
                parameters: Default::default(),
                cascading_encodings: vec![Some(EncodingPlan {
                    encoding: EncodingKind::RunLength,
                    parameters: None,
                    cascading_encodings: vec![
                        Some(EncodingPlan {
                            encoding: EncodingKind::Delta,
                            parameters: None,
                            cascading_encodings: vec![Some(EncodingPlan {
                                encoding: EncodingKind::RunLength,
                                parameters: None,
                                cascading_encodings: vec![None, None]
                            })]
                        }),
                        Some(EncodingPlan {
                            encoding: EncodingKind::SingleValue,
                            parameters: None,
                            cascading_encodings: vec![]
                        })
                    ]
                })]
            },
            plan
        );

        let mut encoded = AlignedByteVec::new();
        let encoded_size2 = context
            .numeric_encoders
            .get::<i32>()
            .encode(&data, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();
        assert_eq!(encoded_size1, encoded_size2);

        // Validate that inspect() returns the same encoding plan as used for encoding
        let inspected_plan = context
            .numeric_encoders
            .get::<i32>()
            .inspect(&encoded, &context)
            .unwrap();
        assert_eq!(plan, inspected_plan);

        let mut decoded = AlignedByteVec::new();
        context
            .numeric_encoders
            .get::<i32>()
            .decode(&encoded, data.len(), None, &mut decoded, &context)
            .unwrap();
        for (a, b) in data.iter().zip(decoded.typed_data::<i32>()) {
            assert_eq!(a, b);
        }
    }
}
