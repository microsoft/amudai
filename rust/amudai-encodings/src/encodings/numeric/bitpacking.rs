use super::{
    NumericEncoding, fastlanes,
    stats::{NumericStats, NumericStatsCollectorFlags},
    value::{UIntegerValue, ValueReader, ValueWriter},
};
use crate::encodings::{
    AlignedEncMetadata, AnalysisOutcome, EncodingConfig, EncodingContext, EncodingKind,
    EncodingParameters, EncodingPlan, NullMask,
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;

/// Bit-Packing encoding based on FastLanes for sequences of unsigned integer values.
/// TODO: implement bitpacking with exceptions.
pub struct FlBitPackingEncoding<T>(std::marker::PhantomData<T>);

impl<T> FlBitPackingEncoding<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }

    fn encode_values(
        values: &[T],
        width: usize,
        target: &mut AlignedByteVec,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize>
    where
        T: UIntegerValue,
    {
        let initial_size = target.len();
        target.write_value::<u16>(EncodingKind::FLBitPack as u16);

        let mut metadata = BitPackingMetadata::initialize(target);
        metadata.width = width;

        let chunks_count =
            values.len().next_multiple_of(fastlanes::BLOCK_SIZE) / fastlanes::BLOCK_SIZE;
        let encoded_chunk_len = fastlanes::BLOCK_SIZE * width / T::BITS_COUNT;
        let encoded_chunk_size = encoded_chunk_len * T::SIZE;
        let data_offset = target.len();
        target.resize(data_offset + chunks_count * encoded_chunk_size, 0);

        let (prefix, encoded_chunks, suffix) = unsafe { target[data_offset..].align_to_mut::<T>() };
        assert!(
            prefix.is_empty() && suffix.is_empty(),
            "Target buffer must be aligned"
        );
        let mut encoded_chunks_it = encoded_chunks.chunks_exact_mut(encoded_chunk_len);

        let mut chunks_iter = values.chunks_exact(fastlanes::BLOCK_SIZE);
        for chunk in chunks_iter.by_ref() {
            let encoded_chunk = encoded_chunks_it.next().unwrap();
            fastlanes::BitPacking::pack(width, chunk, encoded_chunk);
        }

        let rem = chunks_iter.remainder();
        if !rem.is_empty() {
            // Pad the remaining values up to `fastlanes::BLOCK_SIZE` with zeros.
            let mut padded_rem = context.buffers.get_buffer();
            padded_rem.resize_zeroed::<T>(fastlanes::BLOCK_SIZE);
            padded_rem.typed_data_mut()[..rem.len()].copy_from_slice(rem);
            let encoded_chunk = encoded_chunks_it.next().unwrap();
            fastlanes::BitPacking::pack(width, padded_rem.typed_data(), encoded_chunk);
        }

        metadata.finalize(target);

        Ok(target.len() - initial_size)
    }
}

impl<T> NumericEncoding<T> for FlBitPackingEncoding<T>
where
    T: UIntegerValue,
{
    fn kind(&self) -> EncodingKind {
        EncodingKind::FLBitPack
    }

    fn is_suitable(&self, config: &EncodingConfig, stats: &NumericStats<T>) -> bool {
        if T::SIZE > 8 {
            return false;
        }
        if let Some(min_bits) = stats.min_bits_count {
            (min_bits > 0)
                && (T::BITS_COUNT as f64 / min_bits as f64 > config.min_compression_ratio)
        } else {
            false
        }
    }

    fn stats_collector_flags(&self) -> NumericStatsCollectorFlags {
        NumericStatsCollectorFlags::MIN_BITS_COUNT
    }

    fn analyze(
        &self,
        values: &[T],
        _null_mask: &NullMask,
        _config: &EncodingConfig,
        stats: &NumericStats<T>,
        _context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>> {
        let Some(width) = stats.min_bits_count else {
            return Ok(None);
        };
        if width == 0 {
            return Ok(None);
        }
        let encoded_chunk_len = fastlanes::BLOCK_SIZE * width / T::BITS_COUNT;
        let encoded_size = 2
            + BitPackingMetadata::size()
            + (values.len().next_multiple_of(fastlanes::BLOCK_SIZE) / fastlanes::BLOCK_SIZE)
                * encoded_chunk_len
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
        null_mask: &NullMask,
        target: &mut AlignedByteVec,
        _plan: &EncodingPlan,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize> {
        let min_leading_zeroes = values
            .iter()
            .map(|v| v.leading_zeros())
            .min_by_key(|&v| v)
            .expect("Values sequence must not be empty");
        let width = T::BITS_COUNT - min_leading_zeroes as usize;

        if width == 0 {
            // Fallback to SingleValue encoding if the width is zero, which means that all values are 0.
            return context.numeric_encoders.get::<T>().encode(
                values,
                null_mask,
                target,
                &EncodingPlan {
                    encoding: EncodingKind::SingleValue,
                    parameters: Default::default(),
                    cascading_encodings: vec![],
                },
                context,
            );
        }

        let encoded_size = Self::encode_values(values, width, target, context)?;
        Ok(encoded_size)
    }

    fn decode(
        &self,
        buffer: &[u8],
        value_count: usize,
        _params: Option<&EncodingParameters>,
        target: &mut AlignedByteVec,
        _context: &EncodingContext,
    ) -> amudai_common::Result<()> {
        let (metadata, buffer) = BitPackingMetadata::read_from(buffer)?;

        let encoded_chunk_len = fastlanes::BLOCK_SIZE * metadata.width / T::BITS_COUNT;
        let encoded_chunk_size = encoded_chunk_len * T::SIZE;
        if buffer.len() % encoded_chunk_size != 0 {
            return Err(Error::invalid_format(
                "Invalid BitPacking encoded buffer size",
            ));
        }

        let chunks_count = buffer.len() / encoded_chunk_size;
        target.resize_zeroed::<T>(chunks_count * fastlanes::BLOCK_SIZE);
        let mut target_chunks_it = target
            .typed_data_mut()
            .chunks_exact_mut(fastlanes::BLOCK_SIZE);

        for chunk in buffer.chunks_exact(encoded_chunk_size) {
            let chunk: &[T] = unsafe { std::mem::transmute(chunk) };
            let target_chunk = target_chunks_it.next().unwrap();
            fastlanes::BitPacking::unpack(metadata.width, chunk, target_chunk);
        }

        // Truncate to the actual values count because the last chunk may be padded.
        target.truncate(value_count * T::SIZE);

        Ok(())
    }

    fn inspect(
        &self,
        _buffer: &[u8],
        _context: &EncodingContext,
    ) -> amudai_common::Result<EncodingPlan> {
        Ok(EncodingPlan {
            encoding: self.kind(),
            parameters: Default::default(),
            cascading_encodings: vec![],
        })
    }
}

struct BitPackingMetadata {
    start_offset: usize,
    pub width: usize,
}

impl AlignedEncMetadata for BitPackingMetadata {
    fn own_size() -> usize {
        1
    }

    fn initialize(target: &mut AlignedByteVec) -> Self {
        let start_offset = target.len();
        target.resize(start_offset + Self::size(), 0);
        Self {
            start_offset,
            width: 0,
        }
    }

    fn finalize(self, target: &mut AlignedByteVec) {
        target.write_value_at::<u8>(self.start_offset, self.width as u8);
    }

    fn read_from(buffer: &[u8]) -> amudai_common::Result<(Self, &[u8])> {
        if buffer.len() < Self::size() {
            return Err(Error::invalid_format("Encoded buffer size is too small"));
        }
        Ok((
            Self {
                start_offset: 0,
                width: buffer.read_value::<u8>(0) as usize,
            },
            &buffer[Self::size()..],
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::encodings::{
        EncodingConfig, EncodingContext, EncodingKind, NullMask, numeric::fastlanes,
    };
    use amudai_bytes::buffer::AlignedByteVec;

    #[test]
    fn test_round_trip() {
        for data in [
            (1..2),
            (0..10),
            (0..101),
            (0..fastlanes::BLOCK_SIZE as u32),
            (0..12345),
        ]
        .iter()
        .cloned()
        {
            let config = EncodingConfig::default()
                .with_allowed_encodings(&[EncodingKind::FLBitPack])
                .with_min_compression_ratio(0.0);
            let context = EncodingContext::new();
            let data: Vec<u32> = data.collect();
            let outcome = context
                .numeric_encoders
                .get::<u32>()
                .analyze(&data, &NullMask::None, &config, &context)
                .unwrap();
            assert!(outcome.is_some());
            let outcome = outcome.unwrap();
            let encoded_size1 = outcome.encoded_size;
            assert_eq!(outcome.encoding, EncodingKind::FLBitPack);
            assert_eq!(0, outcome.cascading_outcomes.len());
            let plan = outcome.into_plan();

            let mut encoded = AlignedByteVec::new();
            let encoded_size2 = context
                .numeric_encoders
                .get::<u32>()
                .encode(&data, &NullMask::None, &mut encoded, &plan, &context)
                .unwrap();
            assert_eq!(encoded_size1, encoded_size2);

            // Validate that inspect() returns the same encoding plan as used for encoding
            let inspect_plan = context
                .numeric_encoders
                .get::<u32>()
                .inspect(&encoded, &context)
                .unwrap();
            assert_eq!(plan, inspect_plan);

            let mut decoded = AlignedByteVec::new();
            context
                .numeric_encoders
                .get::<u32>()
                .decode(&encoded, data.len(), None, &mut decoded, &context)
                .unwrap();
            for (a, b) in data.iter().zip(decoded.typed_data()) {
                assert_eq!(a, b);
            }
        }
    }
}
