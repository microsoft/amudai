use super::{
    AnalysisOutcome, EncodingKind, NumericEncoding, fastlanes,
    stats::{NumericStats, NumericStatsCollectorFlags},
    value::{IntegerValue, ValueReader, ValueWriter},
};
use crate::encodings::{
    AlignedEncMetadata, EncodingConfig, EncodingContext, EncodingParameters, EncodingPlan, NullMask,
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;

/// Frame-of-reference encoding that stores the difference between each value
/// and the base value (a minimal value in the block is chosen as a base value).
/// Since the subtraction operation is not safe for floating point numbers,
/// this encoding is only available for integer values.
pub struct FOREncoding<T>(std::marker::PhantomData<T>);

impl<T> FOREncoding<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> NumericEncoding<T> for FOREncoding<T>
where
    T: IntegerValue,
{
    fn kind(&self) -> EncodingKind {
        EncodingKind::FrameOfReference
    }

    fn is_suitable(&self, config: &EncodingConfig, stats: &NumericStats<T>) -> bool {
        if config.max_cascading_levels < 2 {
            // For efficient compression, ranges stream must be encoded with
            // a cascading encoding scheme.
            return false;
        }
        stats
            .min
            .and_then(|min| {
                if min == T::zero() {
                    None
                } else {
                    stats.max.map(|max| (min, max))
                }
            })
            .and_then(|(min, max)| max.checked_sub(&min))
            .is_some_and(|diff| {
                diff > T::zero() && diff.leading_zeros() >= (T::BITS_COUNT / 4) as u32
            })
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
        let encoded_size = 2 + FORMetadata::<T>::size();

        let reference = stats.min.unwrap();
        let mut deltas = context.buffers.get_buffer();
        for &value in values {
            deltas.push_typed::<T::UnsignedType>((value - reference).as_());
        }

        Ok(context
            .numeric_encoders
            .get::<T::UnsignedType>()
            .analyze(
                deltas.typed_data(),
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
        let initial_size = target.len();
        target.write_value::<u16>(self.kind() as u16);

        let mut metadata = FORMetadata::initialize(target);

        let reference = *values
            .iter()
            .min_by_key(|&v| v)
            .expect("Values sequence must not be empty");
        metadata.reference = reference;

        let mut deltas = context.buffers.get_buffer();
        for &value in values {
            deltas.push_typed::<T::UnsignedType>((value - reference).as_());
        }

        let deltas_plan = plan.cascading_encodings[0]
            .as_ref()
            .expect("Deltas encoding plan must be provided");
        context.numeric_encoders.get::<T::UnsignedType>().encode(
            deltas.typed_data(),
            null_mask,
            target,
            deltas_plan,
            context,
        )?;

        metadata.finalize(target);

        Ok(target.len() - initial_size)
    }

    fn decode(
        &self,
        buffer: &[u8],
        value_count: usize,
        params: &EncodingParameters,
        target: &mut AlignedByteVec,
        context: &EncodingContext,
    ) -> amudai_common::Result<()> {
        let (metadata, buffer) = FORMetadata::<T>::read_from(buffer)?;
        let mut deltas = context.buffers.get_buffer();
        context.numeric_encoders.get::<T::UnsignedType>().decode(
            buffer,
            value_count,
            params,
            &mut deltas,
            context,
        )?;
        let deltas = deltas.typed_data::<T>();
        for &delta in deltas {
            target.push_typed::<T>(metadata.reference + delta);
        }
        Ok(())
    }
}

struct FORMetadata<T> {
    start_offset: usize,
    pub reference: T,
}

impl<T> AlignedEncMetadata for FORMetadata<T>
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
            reference: T::zero(),
        }
    }

    fn finalize(self, target: &mut AlignedByteVec) {
        target.write_value_at::<T>(self.start_offset, self.reference);
    }

    fn read_from(buffer: &[u8]) -> amudai_common::Result<(Self, &[u8])> {
        if buffer.len() < Self::size() {
            return Err(Error::invalid_format("Encoded buffer size is too small"));
        }
        Ok((
            Self {
                start_offset: 0,
                reference: buffer.read_value::<T>(0),
            },
            &buffer[Self::size()..],
        ))
    }
}

/// Frame-of-reference encoding that's fused with the BitPacking encoding
/// representing a single SIMD sequence, based on FastLanes approach.
pub struct FFOREncoding<T>(std::marker::PhantomData<T>);

impl<T> FFOREncoding<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }

    fn encode_values(
        values: &[T],
        width: usize,
        reference: T,
        target: &mut AlignedByteVec,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize>
    where
        T: IntegerValue,
    {
        let initial_size = target.len();
        target.write_value::<u16>(EncodingKind::FusedFrameOfReference as u16);

        let mut metadata = FFORMetadata::<T>::initialize(target);
        metadata.reference = reference;
        metadata.width = width;

        let values: &[T::UnsignedType] = unsafe { std::mem::transmute(values) };
        let reference = reference.as_();

        let chunks_count =
            values.len().next_multiple_of(fastlanes::BLOCK_SIZE) / fastlanes::BLOCK_SIZE;
        let encoded_chunk_len = fastlanes::BLOCK_SIZE * width / T::BITS_COUNT;
        let encoded_chunk_size = encoded_chunk_len * T::SIZE;
        let data_offset = target.len();
        target.resize(data_offset + chunks_count * encoded_chunk_size, 0);

        let (prefix, encoded_chunks, suffix) =
            unsafe { target[data_offset..].align_to_mut::<T::UnsignedType>() };
        assert!(
            prefix.is_empty() && suffix.is_empty(),
            "Target buffer must be aligned"
        );
        let mut encoded_chunks_it = encoded_chunks.chunks_exact_mut(encoded_chunk_len);

        let mut chunks_iter = values.chunks_exact(fastlanes::BLOCK_SIZE);
        for chunk in chunks_iter.by_ref() {
            let encoded_chunk = encoded_chunks_it.next().unwrap();
            fastlanes::FusedFrameOfReference::ffor_pack(width, chunk, reference, encoded_chunk);
        }

        let rem = chunks_iter.remainder();
        if !rem.is_empty() {
            // Pad the remaining values up to `fastlanes::BLOCK_SIZE` with zeros.
            let mut padded_rem = context.buffers.get_buffer();
            padded_rem.resize_zeroed::<T::UnsignedType>(fastlanes::BLOCK_SIZE);
            padded_rem.typed_data_mut()[..rem.len()].copy_from_slice(rem);
            let encoded_chunk = encoded_chunks_it.next().unwrap();
            fastlanes::FusedFrameOfReference::ffor_pack(
                width,
                padded_rem.typed_data(),
                reference,
                encoded_chunk,
            );
        }

        metadata.finalize(target);

        Ok(target.len() - initial_size)
    }
}

impl<T> NumericEncoding<T> for FFOREncoding<T>
where
    T: IntegerValue,
{
    fn kind(&self) -> EncodingKind {
        EncodingKind::FusedFrameOfReference
    }

    fn is_suitable(&self, config: &EncodingConfig, stats: &NumericStats<T>) -> bool {
        stats
            .min
            .and_then(|min| {
                if min == T::zero() {
                    None
                } else {
                    stats.max.map(|max| (min, max))
                }
            })
            .and_then(|(min, max)| max.checked_sub(&min))
            .is_some_and(|diff| {
                diff > T::zero()
                    && (T::BITS_COUNT as f64
                        / (T::BITS_COUNT - diff.leading_zeros() as usize) as f64)
                        > config.min_compression_ratio
            })
    }

    fn stats_collector_flags(&self) -> NumericStatsCollectorFlags {
        NumericStatsCollectorFlags::MIN_MAX
    }

    fn analyze(
        &self,
        values: &[T],
        _null_mask: &NullMask,
        _config: &EncodingConfig,
        stats: &NumericStats<T>,
        _context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>> {
        let width = if let (Some(min), Some(max)) = (stats.min, stats.max) {
            let diff = max - min;
            T::BITS_COUNT - diff.leading_zeros() as usize
        } else {
            return Ok(None);
        };
        let encoded_chunk_size = fastlanes::BLOCK_SIZE * width / T::BITS_COUNT;
        let encoded_size = 2
            + FFORMetadata::<T>::size()
            + (values.len().next_multiple_of(fastlanes::BLOCK_SIZE) / fastlanes::BLOCK_SIZE)
                * encoded_chunk_size
                * T::SIZE;
        Ok(Some(AnalysisOutcome {
            encoding: self.kind(),
            cascading_outcomes: vec![],
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
        _plan: &EncodingPlan,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize> {
        let (min, max) = values
            .iter()
            .fold((T::max_value(), T::min_value()), |(min, max), &v| {
                (min.min(v), max.max(v))
            });
        let diff = max - min;
        let width = T::BITS_COUNT - diff.leading_zeros() as usize;
        let reference = min;
        let encoded_size = Self::encode_values(values, width, reference, target, context)?;
        Ok(encoded_size)
    }

    fn decode(
        &self,
        buffer: &[u8],
        value_count: usize,
        _params: &EncodingParameters,
        target: &mut AlignedByteVec,
        _context: &EncodingContext,
    ) -> amudai_common::Result<()> {
        let (metadata, buffer) = FFORMetadata::<T>::read_from(buffer)?;
        let reference = metadata.reference.as_();

        let encoded_chunk_len = fastlanes::BLOCK_SIZE * metadata.width / T::BITS_COUNT;
        let encoded_chunk_size = encoded_chunk_len * T::SIZE;
        if buffer.len() % encoded_chunk_size != 0 {
            return Err(Error::invalid_format(
                "Invalid FFOREncoding encoded buffer size",
            ));
        }

        let chunks_count = buffer.len() / encoded_chunk_size;
        target.resize_zeroed::<T>(chunks_count * fastlanes::BLOCK_SIZE);
        let mut target_chunks_it = target
            .typed_data_mut::<T>()
            .chunks_exact_mut(fastlanes::BLOCK_SIZE);

        for chunk in buffer.chunks_exact(encoded_chunk_size) {
            let chunk: &[T::UnsignedType] = unsafe { std::mem::transmute(chunk) };
            let target_chunk: &mut [T::UnsignedType] =
                unsafe { std::mem::transmute(target_chunks_it.next().unwrap()) };
            fastlanes::FusedFrameOfReference::ffor_unpack(
                metadata.width,
                chunk,
                reference,
                target_chunk,
            );
        }

        // Truncate to the actual values count because the last chunk may be padded.
        target.truncate(value_count * T::SIZE);

        Ok(())
    }
}

struct FFORMetadata<T> {
    start_offset: usize,
    pub reference: T,
    pub width: usize,
}

impl<T> AlignedEncMetadata for FFORMetadata<T>
where
    T: IntegerValue,
{
    fn own_size() -> usize {
        std::mem::size_of::<T>() + 1
    }

    fn initialize(target: &mut AlignedByteVec) -> Self {
        let start_offset = target.len();
        target.resize(start_offset + Self::size(), 0);
        Self {
            start_offset,
            reference: T::zero(),
            width: 0,
        }
    }

    fn finalize(self, target: &mut AlignedByteVec) {
        target.write_value_at::<T>(self.start_offset, self.reference);
        target.write_value_at::<u8>(self.start_offset + T::SIZE, self.width as u8);
    }

    fn read_from(buffer: &[u8]) -> amudai_common::Result<(Self, &[u8])> {
        if buffer.len() < Self::size() {
            return Err(Error::invalid_format("Encoded buffer size is too small"));
        }
        Ok((
            Self {
                start_offset: 0,
                reference: buffer.read_value::<T>(0),
                width: buffer.read_value::<u8>(T::SIZE) as usize,
            },
            &buffer[Self::size()..],
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::encodings::{
        EncodingConfig, EncodingContext, EncodingKind, EncodingPlan, NullMask, numeric::fastlanes,
    };
    use amudai_bytes::buffer::AlignedByteVec;

    #[test]
    fn test_round_trip() {
        let config =
            EncodingConfig::default().with_allowed_encodings(&[EncodingKind::FrameOfReference]);
        let context = EncodingContext::new();
        let data: Vec<i64> = (0..65535)
            .map(|_| 10000000 + fastrand::i64(0..1000))
            .collect();
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
                encoding: EncodingKind::FrameOfReference,
                parameters: Default::default(),
                cascading_encodings: vec![Some(EncodingPlan {
                    encoding: EncodingKind::FLBitPack,
                    parameters: Default::default(),
                    cascading_encodings: vec![]
                })],
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

        let mut decoded = AlignedByteVec::new();
        context
            .numeric_encoders
            .get::<i64>()
            .decode(
                &encoded,
                data.len(),
                &Default::default(),
                &mut decoded,
                &context,
            )
            .unwrap();
        for (a, b) in data.iter().zip(decoded.typed_data()) {
            assert_eq!(a, b);
        }
    }

    #[test]
    fn test_ffor_encode_decode() {
        for data in [
            (0..10),
            (0..101),
            (0..fastlanes::BLOCK_SIZE as u32),
            (0..12345),
        ] {
            let config = EncodingConfig::default()
                .with_allowed_encodings(&[EncodingKind::FusedFrameOfReference])
                .with_min_compression_ratio(0.0);
            let context = EncodingContext::new();
            let data: Vec<i64> = data.map(|_| 10000000 + fastrand::i64(0..1000)).collect();
            let outcome = context
                .numeric_encoders
                .get::<i64>()
                .analyze(&data, &NullMask::None, &config, &context)
                .unwrap();
            assert!(outcome.is_some());
            let outcome = outcome.unwrap();
            assert_eq!(outcome.encoding, EncodingKind::FusedFrameOfReference);
            assert!(outcome.cascading_outcomes.is_empty());
            let encoded_size1 = outcome.encoded_size;
            let plan = outcome.into_plan();

            let mut encoded = AlignedByteVec::new();
            let encoded_size2 = context
                .numeric_encoders
                .get::<i64>()
                .encode(&data, &NullMask::None, &mut encoded, &plan, &context)
                .unwrap();
            assert_eq!(encoded_size1, encoded_size2);

            let mut decoded = AlignedByteVec::new();
            context
                .numeric_encoders
                .get::<i64>()
                .decode(
                    &encoded,
                    data.len(),
                    &Default::default(),
                    &mut decoded,
                    &context,
                )
                .unwrap();
            for (a, b) in data.iter().zip(decoded.typed_data()) {
                assert_eq!(a, b);
            }
        }
    }
}
