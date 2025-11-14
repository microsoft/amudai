use super::{
    EncodingContext, NumericEncoding,
    stats::{NumericStats, NumericStatsCollectorFlags},
    value::{SIntegerValue, UIntegerValue, ValueReader, ValueWriter},
};
use crate::encodings::{
    AlignedEncMetadata, AnalysisOutcome, EncodingConfig, EncodingKind, EncodingParameters,
    EncodingPlan, NullMask,
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;

pub trait TruncatableSInteger: SIntegerValue {
    fn can_truncate_to_u8(self) -> bool;
    fn can_truncate_to_u16(self) -> bool;
    fn can_truncate_to_u32(self) -> bool;
    fn can_truncate_to_u64(self) -> bool;
    fn truncate_to_u8(self) -> u8;
    fn truncate_to_u16(self) -> u16;
    fn truncate_to_u32(self) -> u32;
    fn truncate_to_u64(self) -> u64;
    fn from_truncated_u8(value: u8) -> Self;
    fn from_truncated_u16(value: u16) -> Self;
    fn from_truncated_u32(value: u32) -> Self;
    fn from_truncated_u64(value: u64) -> Self;
}

macro_rules! impl_truncatable_sinteger {
    ($T:ty) => {
        impl TruncatableSInteger for $T {
            #[inline]
            fn can_truncate_to_u8(self) -> bool {
                self <= i8::MAX as $T && self >= i8::MIN as $T
            }
            #[inline]
            fn can_truncate_to_u16(self) -> bool {
                self <= i16::MAX as $T && self >= i16::MIN as $T
            }
            #[inline]
            fn can_truncate_to_u32(self) -> bool {
                self <= i32::MAX as $T && self >= i32::MIN as $T
            }
            #[inline]
            fn can_truncate_to_u64(self) -> bool {
                self <= i64::MAX as $T && self >= i64::MIN as $T
            }
            #[inline]
            fn truncate_to_u8(self) -> u8 {
                self as u8
            }
            #[inline]
            fn truncate_to_u16(self) -> u16 {
                self as u16
            }
            #[inline]
            fn truncate_to_u32(self) -> u32 {
                self as u32
            }
            #[inline]
            fn truncate_to_u64(self) -> u64 {
                self as u64
            }
            #[inline]
            fn from_truncated_u8(value: u8) -> Self {
                (value as i8) as $T
            }
            #[inline]
            fn from_truncated_u16(value: u16) -> Self {
                (value as i16) as $T
            }
            #[inline]
            fn from_truncated_u32(value: u32) -> Self {
                (value as i32) as $T
            }
            #[inline]
            fn from_truncated_u64(value: u64) -> Self {
                (value as i64) as $T
            }
        }
    };
}

impl_truncatable_sinteger!(i8);
impl_truncatable_sinteger!(i16);
impl_truncatable_sinteger!(i32);
impl_truncatable_sinteger!(i64);
impl_truncatable_sinteger!(i128);

/// This encoding truncates signed integers to u8 type if possible.
pub struct TruncateU8Encoding<T>(std::marker::PhantomData<T>);

impl<T> TruncateU8Encoding<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> NumericEncoding<T> for TruncateU8Encoding<T>
where
    T: TruncatableSInteger,
{
    fn kind(&self) -> EncodingKind {
        EncodingKind::TruncateU8
    }

    fn is_suitable(&self, _config: &EncodingConfig, stats: &NumericStats<T>) -> bool {
        if let (Some(min), Some(max)) = (stats.min, stats.max) {
            min.can_truncate_to_u8() && max.can_truncate_to_u8()
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
        let mut truncated_values = context.buffers.get_buffer();
        truncated_values.reserve(values.len() * std::mem::size_of::<u8>());
        for value in values {
            truncated_values.push_typed::<u8>(value.truncate_to_u8());
        }
        analyze_truncated_values(
            truncated_values.typed_data::<u8>(),
            null_mask,
            self.kind(),
            config,
            stats,
            context,
        )
    }

    fn encode(
        &self,
        values: &[T],
        null_mask: &NullMask,
        target: &mut AlignedByteVec,
        plan: &EncodingPlan,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize> {
        // Quickly check whether the values are truncatable to u8.
        // If some values are not, fallback to truncation to u16.
        if values.iter().any(|v| !v.can_truncate_to_u8()) {
            return context.numeric_encoders.get::<T>().encode(
                values,
                null_mask,
                target,
                &EncodingPlan {
                    encoding: EncodingKind::TruncateU16,
                    parameters: plan.parameters.clone(),
                    cascading_encodings: plan.cascading_encodings.clone(),
                },
                context,
            );
        }

        let mut truncated_values = context.buffers.get_buffer();
        truncated_values.reserve(values.len() * std::mem::size_of::<u8>());
        for value in values {
            truncated_values.push_typed::<u8>(value.truncate_to_u8());
        }
        encode_truncated_values(
            truncated_values.typed_data::<u8>(),
            null_mask,
            self.kind(),
            target,
            plan,
            context,
        )
    }

    fn decode(
        &self,
        buffer: &[u8],
        value_count: usize,
        params: Option<&EncodingParameters>,
        target: &mut AlignedByteVec,
        context: &EncodingContext,
    ) -> amudai_common::Result<()> {
        let (metadata, buffer) = TruncationMetadata::read_from(buffer)?;
        if metadata.cascading {
            let mut decoded = context.buffers.get_buffer();
            context.numeric_encoders.get::<u8>().decode(
                buffer,
                value_count,
                params,
                &mut decoded,
                context,
            )?;
            for value in decoded.iter() {
                target.push_typed::<T>(T::from_truncated_u8(*value));
            }
        } else {
            for value in buffer.iter() {
                target.push_typed::<T>(T::from_truncated_u8(*value));
            }
        }

        Ok(())
    }

    fn inspect(
        &self,
        buffer: &[u8],
        context: &EncodingContext,
    ) -> amudai_common::Result<EncodingPlan> {
        let (metadata, buffer) = TruncationMetadata::read_from(buffer)?;
        let cascading_plan = if metadata.cascading {
            Some(
                context
                    .numeric_encoders
                    .get::<u8>()
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

/// This encoding truncates signed integers to u16 type if possible.
pub struct TruncateU16Encoding<T>(std::marker::PhantomData<T>);

impl<T> TruncateU16Encoding<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> NumericEncoding<T> for TruncateU16Encoding<T>
where
    T: TruncatableSInteger,
{
    fn kind(&self) -> EncodingKind {
        EncodingKind::TruncateU16
    }

    fn is_suitable(&self, _config: &EncodingConfig, stats: &NumericStats<T>) -> bool {
        if let (Some(min), Some(max)) = (stats.min, stats.max) {
            // Make sure this is minimal truncation encoding:
            min.can_truncate_to_u16()
                && max.can_truncate_to_u16()
                && (!min.can_truncate_to_u8() || !max.can_truncate_to_u8())
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
        let mut truncated_values = context.buffers.get_buffer();
        truncated_values.reserve(values.len() * std::mem::size_of::<u16>());
        for value in values {
            truncated_values.push_typed::<u16>(value.truncate_to_u16());
        }
        analyze_truncated_values(
            truncated_values.typed_data::<u16>(),
            null_mask,
            self.kind(),
            config,
            stats,
            context,
        )
    }

    fn encode(
        &self,
        values: &[T],
        null_mask: &NullMask,
        target: &mut AlignedByteVec,
        plan: &EncodingPlan,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize> {
        // Quickly check whether the values are truncatable to u16.
        // If some values are not, fallback to truncation to u32.
        if values.iter().any(|v| !v.can_truncate_to_u16()) {
            return context.numeric_encoders.get::<T>().encode(
                values,
                null_mask,
                target,
                &EncodingPlan {
                    encoding: EncodingKind::TruncateU32,
                    parameters: plan.parameters.clone(),
                    cascading_encodings: plan.cascading_encodings.clone(),
                },
                context,
            );
        }

        let mut truncated_values = context.buffers.get_buffer();
        truncated_values.reserve(values.len() * std::mem::size_of::<u16>());
        for value in values {
            truncated_values.push_typed::<u16>(value.truncate_to_u16());
        }
        encode_truncated_values(
            truncated_values.typed_data::<u16>(),
            null_mask,
            self.kind(),
            target,
            plan,
            context,
        )
    }

    fn decode(
        &self,
        buffer: &[u8],
        value_count: usize,
        params: Option<&EncodingParameters>,
        target: &mut AlignedByteVec,
        context: &EncodingContext,
    ) -> amudai_common::Result<()> {
        let (metadata, buffer) = TruncationMetadata::read_from(buffer)?;
        if metadata.cascading {
            let mut decoded = context.buffers.get_buffer();
            context.numeric_encoders.get::<u16>().decode(
                buffer,
                value_count,
                params,
                &mut decoded,
                context,
            )?;
            for value in decoded.typed_data() {
                target.push_typed::<T>(T::from_truncated_u16(*value));
            }
        } else {
            for chunk in buffer.chunks_exact(2) {
                target.push_typed::<T>(T::from_truncated_u16(u16::from_le_bytes([
                    chunk[0], chunk[1],
                ])));
            }
        }

        Ok(())
    }

    fn inspect(
        &self,
        buffer: &[u8],
        context: &EncodingContext,
    ) -> amudai_common::Result<EncodingPlan> {
        let (metadata, buffer) = TruncationMetadata::read_from(buffer)?;
        let cascading_plan = if metadata.cascading {
            Some(
                context
                    .numeric_encoders
                    .get::<u16>()
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

/// This encoding truncates signed integers to u32 type if possible.
pub struct TruncateU32Encoding<T>(std::marker::PhantomData<T>);

impl<T> TruncateU32Encoding<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> NumericEncoding<T> for TruncateU32Encoding<T>
where
    T: TruncatableSInteger,
{
    fn kind(&self) -> EncodingKind {
        EncodingKind::TruncateU32
    }

    fn is_suitable(&self, _config: &EncodingConfig, stats: &NumericStats<T>) -> bool {
        if let (Some(min), Some(max)) = (stats.min, stats.max) {
            // Make sure this is minimal truncation encoding:
            min.can_truncate_to_u32()
                && max.can_truncate_to_u32()
                && (!min.can_truncate_to_u16() || !max.can_truncate_to_u16())
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
        let mut truncated_values = context.buffers.get_buffer();
        truncated_values.reserve(values.len() * std::mem::size_of::<u32>());
        for value in values {
            truncated_values.push_typed::<u32>(value.truncate_to_u32());
        }
        analyze_truncated_values(
            truncated_values.typed_data::<u32>(),
            null_mask,
            self.kind(),
            config,
            stats,
            context,
        )
    }

    fn encode(
        &self,
        values: &[T],
        null_mask: &NullMask,
        target: &mut AlignedByteVec,
        plan: &EncodingPlan,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize> {
        // Quickly check whether the values are truncatable to u32.
        // If some values are not, fallback to truncation to u64.
        if values.iter().any(|v| !v.can_truncate_to_u32()) {
            return context.numeric_encoders.get::<T>().encode(
                values,
                null_mask,
                target,
                &EncodingPlan {
                    encoding: EncodingKind::TruncateU64,
                    parameters: plan.parameters.clone(),
                    cascading_encodings: plan.cascading_encodings.clone(),
                },
                context,
            );
        }

        let mut truncated_values = context.buffers.get_buffer();
        truncated_values.reserve(values.len() * std::mem::size_of::<u32>());
        for value in values {
            truncated_values.push_typed::<u32>(value.truncate_to_u32());
        }
        encode_truncated_values(
            truncated_values.typed_data::<u32>(),
            null_mask,
            self.kind(),
            target,
            plan,
            context,
        )
    }

    fn decode(
        &self,
        buffer: &[u8],
        value_count: usize,
        params: Option<&EncodingParameters>,
        target: &mut AlignedByteVec,
        context: &EncodingContext,
    ) -> amudai_common::Result<()> {
        let (metadata, buffer) = TruncationMetadata::read_from(buffer)?;
        if metadata.cascading {
            let mut decoded = context.buffers.get_buffer();
            context.numeric_encoders.get::<u32>().decode(
                buffer,
                value_count,
                params,
                &mut decoded,
                context,
            )?;
            for value in decoded.typed_data() {
                target.push_typed::<T>(T::from_truncated_u32(*value));
            }
        } else {
            for chunk in buffer.chunks_exact(4) {
                target.push_typed::<T>(T::from_truncated_u32(u32::from_le_bytes([
                    chunk[0], chunk[1], chunk[2], chunk[3],
                ])));
            }
        }

        Ok(())
    }

    fn inspect(
        &self,
        buffer: &[u8],
        context: &EncodingContext,
    ) -> amudai_common::Result<EncodingPlan> {
        let (metadata, buffer) = TruncationMetadata::read_from(buffer)?;
        let cascading_plan = if metadata.cascading {
            Some(
                context
                    .numeric_encoders
                    .get::<u32>()
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

/// This encoding truncates signed integers to u64 type if possible.
pub struct TruncateU64Encoding<T>(std::marker::PhantomData<T>);

impl<T> TruncateU64Encoding<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> NumericEncoding<T> for TruncateU64Encoding<T>
where
    T: TruncatableSInteger,
{
    fn kind(&self) -> EncodingKind {
        EncodingKind::TruncateU64
    }

    fn is_suitable(&self, _config: &EncodingConfig, stats: &NumericStats<T>) -> bool {
        if let (Some(min), Some(max)) = (stats.min, stats.max) {
            // Make sure this is minimal truncation encoding:
            min.can_truncate_to_u64()
                && max.can_truncate_to_u64()
                && (!min.can_truncate_to_u32() || !max.can_truncate_to_u32())
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
        let mut truncated_values = context.buffers.get_buffer();
        truncated_values.reserve(values.len() * std::mem::size_of::<u64>());
        for value in values {
            truncated_values.push_typed::<u64>(value.truncate_to_u64());
        }
        analyze_truncated_values(
            truncated_values.typed_data::<u64>(),
            null_mask,
            self.kind(),
            config,
            stats,
            context,
        )
    }

    fn encode(
        &self,
        values: &[T],
        null_mask: &NullMask,
        target: &mut AlignedByteVec,
        plan: &EncodingPlan,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize> {
        // Quickly check whether the values are truncatable to u64.
        // If some values are not, fallback to plain encoding.
        if values.iter().any(|v| !v.can_truncate_to_u64()) {
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

        let mut truncated_values = context.buffers.get_buffer();
        truncated_values.reserve(values.len() * std::mem::size_of::<u64>());
        for value in values {
            truncated_values.push_typed::<u64>(value.truncate_to_u64());
        }
        encode_truncated_values(
            truncated_values.typed_data::<u64>(),
            null_mask,
            self.kind(),
            target,
            plan,
            context,
        )
    }

    fn decode(
        &self,
        buffer: &[u8],
        value_count: usize,
        params: Option<&EncodingParameters>,
        target: &mut AlignedByteVec,
        context: &EncodingContext,
    ) -> amudai_common::Result<()> {
        let (metadata, buffer) = TruncationMetadata::read_from(buffer)?;
        if metadata.cascading {
            let mut decoded = context.buffers.get_buffer();
            context.numeric_encoders.get::<u64>().decode(
                buffer,
                value_count,
                params,
                &mut decoded,
                context,
            )?;
            for value in decoded.typed_data() {
                target.push_typed::<T>(T::from_truncated_u64(*value));
            }
        } else {
            for chunk in buffer.chunks_exact(8) {
                target.push_typed::<T>(T::from_truncated_u64(u64::from_le_bytes([
                    chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
                ])));
            }
        }

        Ok(())
    }

    fn inspect(
        &self,
        buffer: &[u8],
        context: &EncodingContext,
    ) -> amudai_common::Result<EncodingPlan> {
        let (metadata, buffer) = TruncationMetadata::read_from(buffer)?;
        let cascading_plan = if metadata.cascading {
            Some(
                context
                    .numeric_encoders
                    .get::<u64>()
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

fn analyze_truncated_values<OrigType, TruncatedType>(
    truncated_values: &[TruncatedType],
    null_mask: &NullMask,
    encoding: EncodingKind,
    config: &EncodingConfig,
    stats: &NumericStats<OrigType>,
    context: &EncodingContext,
) -> amudai_common::Result<Option<AnalysisOutcome>>
where
    TruncatedType: UIntegerValue,
{
    let mut encoded_size = 2 + TruncationMetadata::size();

    let mut cascading_encodings = vec![None];
    let cascading_outcome = context.numeric_encoders.get::<TruncatedType>().analyze(
        truncated_values,
        null_mask,
        &config.make_cascading_config(),
        context,
    )?;
    if let Some(cascading_outcome) = cascading_outcome {
        encoded_size += cascading_outcome.encoded_size;
        cascading_encodings[0] = Some(cascading_outcome);
    } else {
        encoded_size += truncated_values.len() * TruncatedType::SIZE;
    }

    Ok(Some(AnalysisOutcome {
        encoding,
        cascading_outcomes: cascading_encodings,
        encoded_size,
        compression_ratio: stats.original_size as f64 / encoded_size as f64,
        parameters: Default::default(),
    }))
}

fn encode_truncated_values<T>(
    truncated_values: &[T],
    null_mask: &NullMask,
    encoding_kind: EncodingKind,
    target: &mut AlignedByteVec,
    plan: &EncodingPlan,
    context: &EncodingContext,
) -> amudai_common::Result<usize>
where
    T: UIntegerValue,
{
    let initial_size = target.len();
    target.write_value::<u16>(encoding_kind as u16);

    let mut metadata = TruncationMetadata::initialize(target);

    if let Some(cascading_plan) = plan.cascading_encodings[0].as_ref() {
        context.numeric_encoders.get::<T>().encode(
            truncated_values,
            null_mask,
            target,
            cascading_plan,
            context,
        )?;
        metadata.cascading = true;
    } else {
        target.write_values(truncated_values);
    }

    metadata.finalize(target);

    Ok(target.len() - initial_size)
}

struct TruncationMetadata {
    start_offset: usize,
    pub cascading: bool,
}

impl AlignedEncMetadata for TruncationMetadata {
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
    fn test_truncate_u8_encode_decode() {
        let config = EncodingConfig::default().with_allowed_encodings(&[
            EncodingKind::TruncateU8,
            EncodingKind::TruncateU16,
            EncodingKind::TruncateU32,
            EncodingKind::TruncateU64,
        ]);
        let context = EncodingContext::new();
        let data: Vec<i128> = (-127..128)
            .flat_map(|i| (0..250).map(move |_| i as i128))
            .collect();
        let outcome = context
            .numeric_encoders
            .get::<i128>()
            .analyze(&data, &NullMask::None, &config, &context)
            .unwrap();
        assert!(outcome.is_some());
        let outcome = outcome.unwrap();
        let encoded_size1 = outcome.encoded_size;
        let plan = outcome.into_plan();

        assert_eq!(
            EncodingPlan {
                encoding: EncodingKind::TruncateU8,
                parameters: Default::default(),
                cascading_encodings: vec![Some(EncodingPlan {
                    encoding: EncodingKind::RunLength,
                    parameters: Default::default(),
                    cascading_encodings: vec![
                        None,
                        Some(EncodingPlan {
                            encoding: EncodingKind::SingleValue,
                            parameters: Default::default(),
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
            .get::<i128>()
            .encode(&data, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();
        assert_eq!(encoded_size1, encoded_size2);

        // Validate that inspect() returns the same encoding plan as used for encoding
        let inspect_plan = context
            .numeric_encoders
            .get::<i128>()
            .inspect(&encoded, &context)
            .unwrap();
        assert_eq!(plan, inspect_plan);

        let mut decoded = AlignedByteVec::new();
        context
            .numeric_encoders
            .get::<i128>()
            .decode(&encoded, data.len(), None, &mut decoded, &context)
            .unwrap();
        for (a, b) in data.iter().zip(decoded.typed_data()) {
            assert_eq!(a, b);
        }
    }

    #[test]
    fn test_truncate_u8_fallback() {
        let config = EncodingConfig::default().with_allowed_encodings(&[
            EncodingKind::TruncateU8,
            EncodingKind::TruncateU16,
            EncodingKind::TruncateU32,
            EncodingKind::TruncateU64,
        ]);
        let context = EncodingContext::new();
        let sample: Vec<i128> = (-127..128)
            .flat_map(|i| (0..250).map(move |_| i as i128))
            .collect();
        // Real data contains values that cannot be truncated to u8.
        let data: Vec<i128> = (-320..320)
            .flat_map(|i| (0..100).map(move |_| i * i32::MAX as i128))
            .collect();
        let outcome = context
            .numeric_encoders
            .get::<i128>()
            .analyze(&sample, &NullMask::None, &config, &context)
            .unwrap();
        assert!(outcome.is_some());
        let outcome = outcome.unwrap();
        let plan = outcome.into_plan();

        assert_eq!(
            EncodingPlan {
                encoding: EncodingKind::TruncateU8,
                parameters: Default::default(),
                cascading_encodings: vec![Some(EncodingPlan {
                    encoding: EncodingKind::RunLength,
                    parameters: Default::default(),
                    cascading_encodings: vec![
                        None,
                        Some(EncodingPlan {
                            encoding: EncodingKind::SingleValue,
                            parameters: Default::default(),
                            cascading_encodings: vec![]
                        })
                    ]
                })]
            },
            plan
        );

        let mut encoded = AlignedByteVec::new();
        context
            .numeric_encoders
            .get::<i128>()
            .encode(&data, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();

        // Validate that inspect() returns the same encoding plan as used for encoding
        let inspect_plan = context
            .numeric_encoders
            .get::<i128>()
            .inspect(&encoded, &context)
            .unwrap();
        assert_eq!(
            EncodingPlan {
                encoding: EncodingKind::TruncateU64,
                parameters: Default::default(),
                cascading_encodings: vec![Some(EncodingPlan {
                    encoding: EncodingKind::RunLength,
                    parameters: Default::default(),
                    cascading_encodings: vec![
                        None,
                        Some(EncodingPlan {
                            encoding: EncodingKind::SingleValue,
                            parameters: Default::default(),
                            cascading_encodings: vec![]
                        })
                    ]
                })]
            },
            inspect_plan
        );

        let mut decoded = AlignedByteVec::new();
        context
            .numeric_encoders
            .get::<i128>()
            .decode(&encoded, data.len(), None, &mut decoded, &context)
            .unwrap();
        for (a, b) in data.iter().zip(decoded.typed_data()) {
            assert_eq!(a, b);
        }
    }

    #[test]
    fn test_truncate_u16_encode_decode() {
        let config = EncodingConfig::default().with_allowed_encodings(&[
            EncodingKind::TruncateU8,
            EncodingKind::TruncateU16,
            EncodingKind::TruncateU32,
            EncodingKind::TruncateU64,
        ]);
        let context = EncodingContext::new();
        let data: Vec<i128> = (-320..320)
            .flat_map(|i| (0..100).map(move |_| (i * 100) as i128))
            .collect();
        let outcome = context
            .numeric_encoders
            .get::<i128>()
            .analyze(&data, &NullMask::None, &config, &context)
            .unwrap();
        assert!(outcome.is_some());
        let outcome = outcome.unwrap();
        let encoded_size1 = outcome.encoded_size;
        let plan = outcome.into_plan();

        assert_eq!(
            EncodingPlan {
                encoding: EncodingKind::TruncateU16,
                parameters: Default::default(),
                cascading_encodings: vec![Some(EncodingPlan {
                    encoding: EncodingKind::RunLength,
                    parameters: Default::default(),
                    cascading_encodings: vec![
                        None,
                        Some(EncodingPlan {
                            encoding: EncodingKind::SingleValue,
                            parameters: Default::default(),
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
            .get::<i128>()
            .encode(&data, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();
        assert_eq!(encoded_size1, encoded_size2);

        // Validate that inspect() returns the same encoding plan as used for encoding
        let inspect_plan = context
            .numeric_encoders
            .get::<i128>()
            .inspect(&encoded, &context)
            .unwrap();
        assert_eq!(plan, inspect_plan);

        let mut decoded = AlignedByteVec::new();
        context
            .numeric_encoders
            .get::<i128>()
            .decode(&encoded, data.len(), None, &mut decoded, &context)
            .unwrap();
        for (a, b) in data.iter().zip(decoded.typed_data()) {
            assert_eq!(a, b);
        }
    }

    #[test]
    fn test_truncate_u32_encode_decode() {
        let config = EncodingConfig::default().with_allowed_encodings(&[
            EncodingKind::TruncateU8,
            EncodingKind::TruncateU16,
            EncodingKind::TruncateU32,
            EncodingKind::TruncateU64,
        ]);
        let context = EncodingContext::new();
        let data: Vec<i128> = (-320..320)
            .flat_map(|i| (0..100).map(move |_| i * 200))
            .collect();
        let outcome = context
            .numeric_encoders
            .get::<i128>()
            .analyze(&data, &NullMask::None, &config, &context)
            .unwrap();
        assert!(outcome.is_some());
        let outcome = outcome.unwrap();
        let encoded_size1 = outcome.encoded_size;
        let plan = outcome.into_plan();

        assert_eq!(
            EncodingPlan {
                encoding: EncodingKind::TruncateU32,
                parameters: Default::default(),
                cascading_encodings: vec![Some(EncodingPlan {
                    encoding: EncodingKind::RunLength,
                    parameters: Default::default(),
                    cascading_encodings: vec![
                        None,
                        Some(EncodingPlan {
                            encoding: EncodingKind::SingleValue,
                            parameters: Default::default(),
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
            .get::<i128>()
            .encode(&data, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();
        assert_eq!(encoded_size1, encoded_size2);

        // Validate that inspect() returns the same encoding plan as used for encoding
        let inspect_plan = context
            .numeric_encoders
            .get::<i128>()
            .inspect(&encoded, &context)
            .unwrap();
        assert_eq!(plan, inspect_plan);

        let mut decoded = AlignedByteVec::new();
        context
            .numeric_encoders
            .get::<i128>()
            .decode(&encoded, data.len(), None, &mut decoded, &context)
            .unwrap();
        for (a, b) in data.iter().zip(decoded.typed_data()) {
            assert_eq!(a, b);
        }
    }

    #[test]
    fn test_truncate_u64_encode_decode() {
        let config = EncodingConfig::default().with_allowed_encodings(&[
            EncodingKind::TruncateU8,
            EncodingKind::TruncateU16,
            EncodingKind::TruncateU32,
            EncodingKind::TruncateU64,
        ]);
        let context = EncodingContext::new();
        let data: Vec<i128> = (-320..320)
            .flat_map(|i| (0..100).map(move |_| i * i32::MAX as i128))
            .collect();
        let outcome = context
            .numeric_encoders
            .get::<i128>()
            .analyze(&data, &NullMask::None, &config, &context)
            .unwrap();
        assert!(outcome.is_some());
        let outcome = outcome.unwrap();
        let encoded_size1 = outcome.encoded_size;
        let plan = outcome.into_plan();

        assert_eq!(
            EncodingPlan {
                encoding: EncodingKind::TruncateU64,
                parameters: Default::default(),
                cascading_encodings: vec![Some(EncodingPlan {
                    encoding: EncodingKind::RunLength,
                    parameters: Default::default(),
                    cascading_encodings: vec![
                        None,
                        Some(EncodingPlan {
                            encoding: EncodingKind::SingleValue,
                            parameters: Default::default(),
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
            .get::<i128>()
            .encode(&data, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();
        assert_eq!(encoded_size1, encoded_size2);

        // Validate that inspect() returns the same encoding plan as used for encoding
        let inspect_plan = context
            .numeric_encoders
            .get::<i128>()
            .inspect(&encoded, &context)
            .unwrap();
        assert_eq!(plan, inspect_plan);

        let mut decoded = AlignedByteVec::new();
        context
            .numeric_encoders
            .get::<i128>()
            .decode(&encoded, data.len(), None, &mut decoded, &context)
            .unwrap();
        for (a, b) in data.iter().zip(decoded.typed_data()) {
            assert_eq!(a, b);
        }
    }
}
