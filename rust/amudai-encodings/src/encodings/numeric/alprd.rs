// ! This code is based on original code from repository: https://github.com/spiraldb/alp

use super::{
    NumericEncoding,
    stats::{NumericStats, NumericStatsCollectorFlags},
    value::{AsMutByteSlice, DecimalValue, FloatValue, UIntegerValue, ValueReader, ValueWriter},
};
use crate::encodings::{
    AlignedEncMetadata, AlpRdParameters, AnalysisOutcome, EncodingConfig, EncodingContext,
    EncodingKind, EncodingParameters, EncodingPlan, NullMask,
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;
use num_traits::One;
use std::{
    collections::HashMap,
    ops::{Shl, Shr},
};
use tinyvec::ArrayVec;

/// Adaptive Lossless floating-Point for Real Doubles encoding as described
/// in the paper "ALP: Adaptive Lossless floating-Point Compression".
/// <https://dl.acm.org/doi/pdf/10.1145/3626717>
pub struct ALPRDEncoding<T>(std::marker::PhantomData<T>);

macro_rules! bit_width {
    ($value:expr) => {
        if $value == 0 {
            1
        } else {
            $value.ilog2().wrapping_add(1) as usize
        }
    };
}

pub const MAX_DICT_SIZE: usize = 8;

impl<T> ALPRDEncoding<T>
where
    T: ALPRDDecimalValue,
{
    /// Max number of bits to cut from the MSB section of each float.
    const CUT_LIMIT: usize = 16;

    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }

    /// Calculate reusable parameters for the ALP-RD encoding based on provided
    /// sample of decimal values.
    pub fn calculate_parameters(&self, sample: &[T], context: &EncodingContext) -> AlpRdParameters {
        let dictionary = self.find_best_dictionary(sample, context);
        AlpRdParameters {
            right_bit_width: dictionary.right_bit_width,
            left_parts_dict: dictionary.codes,
        }
    }

    /// Encode the floating point values into a result type.
    fn split(
        doubles: &[T],
        params: &AlpRdParameters,
        left_parts: &mut Vec<u16>,
        right_parts: &mut Vec<T::UnsignedIntType>,
        exception_pos: &mut Vec<u32>,
        exception_values: &mut Vec<u16>,
    ) {
        left_parts.reserve(doubles.len());
        right_parts.reserve(doubles.len());
        exception_pos.reserve(doubles.len() / 4);
        exception_values.reserve(doubles.len() / 4);

        // mask for right-parts
        let right_mask =
            T::UnsignedIntType::one().shl(params.right_bit_width as _) - T::UnsignedIntType::one();

        for v in doubles.iter().copied() {
            right_parts.push(T::to_bits(v) & right_mask);
            left_parts.push(<T as ALPRDDecimalValue>::to_u16(
                T::to_bits(v).shr(params.right_bit_width as _),
            ));
        }

        // dict-encode the left-parts, keeping track of exceptions
        for (idx, left) in left_parts.iter_mut().enumerate() {
            // TODO: revisit if we need to change the branch order for perf.
            if let Some(code) = params.left_parts_dict.iter().position(|v| *v == *left) {
                *left = code as u16;
            } else {
                exception_values.push(*left);
                exception_pos.push(idx as _);
                *left = 0u16;
            }
        }
    }

    /// Find the best "cut point" for a set of floating point values such that we can
    /// cast them all to the relevant value instead.
    fn find_best_dictionary(&self, samples: &[T], _context: &EncodingContext) -> ALPRDDictionary {
        let mut best_est_size = f64::MAX;
        let mut best_dict = ALPRDDictionary::default();

        // The following objects are attempted to be reused to avoid reallocations.
        let mut counts = HashMap::<u16, usize>::new();
        let mut sorted_bit_counts = vec![];

        for p in 1..=Self::CUT_LIMIT {
            let candidate_right_bw = (T::BITS_COUNT - p) as u8;
            counts.clear();
            sorted_bit_counts.clear();
            let (dictionary, exception_count) = Self::build_left_parts_dictionary(
                samples,
                candidate_right_bw,
                &mut counts,
                &mut sorted_bit_counts,
            );
            let estimated_size = Self::estimate_compression_size(
                dictionary.right_bit_width,
                dictionary.left_bit_width,
                exception_count,
                samples.len(),
            );
            if estimated_size < best_est_size {
                best_est_size = estimated_size;
                best_dict = dictionary;
            }
        }

        best_dict
    }

    /// Build dictionary of the leftmost bits.
    fn build_left_parts_dictionary(
        samples: &[T],
        right_bw: u8,
        counts: &mut HashMap<u16, usize>,
        sorted_bit_counts: &mut Vec<(u16, usize)>,
    ) -> (ALPRDDictionary, usize) {
        assert!(
            right_bw >= (T::BITS_COUNT - Self::CUT_LIMIT) as _,
            "left-parts must be <= 16 bits"
        );

        // Count the number of occurrences of each left bit pattern
        samples
            .iter()
            .copied()
            .map(|v| <T as ALPRDDecimalValue>::to_u16(T::to_bits(v).shr(right_bw as _)))
            .for_each(|item| *counts.entry(item).or_default() += 1);

        // Sorted counts: sort by negative count so that heavy hitters sort first.
        sorted_bit_counts.extend(counts.iter().map(|(&k, &v)| (k, v)));
        sorted_bit_counts.sort_by_key(|(_, count)| count.wrapping_neg());

        // Assign the most-frequently occurring left-bits as dictionary codes, up to `MAX_DICT_SIZE`.
        let mut code = 0u16;
        let mut codes = ArrayVec::<[u16; MAX_DICT_SIZE]>::new();
        while code < (MAX_DICT_SIZE as _) && (code as usize) < sorted_bit_counts.len() {
            let (bits, _) = sorted_bit_counts[code as usize];
            codes.push(bits);
            code += 1;
        }

        // ...and the rest are exceptions.
        let exception_count: usize = sorted_bit_counts
            .iter()
            .skip(code as _)
            .map(|(_, count)| *count)
            .sum();

        // Left bit-width is determined based on the actual dictionary size.
        let max_code = codes.len() - 1;
        let left_bw = bit_width!(max_code) as u8;

        (
            ALPRDDictionary {
                codes,
                right_bit_width: right_bw,
                left_bit_width: left_bw,
            },
            exception_count,
        )
    }

    /// Estimate the bits-per-value when using these compression settings.
    fn estimate_compression_size(
        right_bw: u8,
        left_bw: u8,
        exception_count: usize,
        sample_n: usize,
    ) -> f64 {
        const EXC_POSITION_SIZE: usize = 16; // two bytes for exception position.
        const EXC_SIZE: usize = 16; // two bytes for each exception (up to 16 front bits).

        let exceptions_size = exception_count * (EXC_POSITION_SIZE + EXC_SIZE);
        (right_bw as f64) + (left_bw as f64) + ((exceptions_size as f64) / (sample_n as f64))
    }

    pub fn decode_chunk(
        left_parts: &[u16],
        right_parts: &[T::UnsignedIntType],
        exceptions_pos: &[u32],
        exceptions_values: &[u16],
        params: &AlpRdParameters,
        context: &EncodingContext,
        target: &mut AlignedByteVec,
    ) -> amudai_common::Result<()> {
        if left_parts.len() != right_parts.len() {
            return Err(Error::invalid_format(
                "Bad ALPRD encoded chunk: left and right parts must have the same length",
            ));
        }
        if exceptions_pos.len() != exceptions_values.len() {
            return Err(Error::invalid_format(
                "Bad ALPRD encoded chunk: exception positions and values must have the same length",
            ));
        }

        let mut left_parts_decoded = context.buffers.get_buffer();
        left_parts_decoded.reserve(left_parts.len() * T::SIZE);

        // Decode with bit-packing and dict unpacking.
        for code in left_parts {
            left_parts_decoded.push_typed(<T as ALPRDDecimalValue>::from_u16(
                params.left_parts_dict[*code as usize],
            ));
        }

        // Apply the exception patches to left_parts
        let left_parts_decoded = left_parts_decoded.typed_data_mut();
        for (pos, val) in exceptions_pos.iter().zip(exceptions_values.iter()) {
            left_parts_decoded[*pos as usize] = <T as ALPRDDecimalValue>::from_u16(*val);
        }

        // recombine the left-and-right parts, adjusting by the right_bit_width.
        for (&left, right) in left_parts_decoded.iter().zip(right_parts.iter().copied()) {
            target.push_typed::<T>(T::from_bits(
                (left << (params.right_bit_width as usize)) | right,
            ));
        }

        Ok(())
    }
}

/// The ALP-RD dictionary, encoding the "left parts" and their dictionary encoding.
#[derive(Debug, Default)]
struct ALPRDDictionary {
    /// Top `MAX_DICT_SIZE` most-frequently occurring left parts.
    codes: ArrayVec<[u16; MAX_DICT_SIZE]>,
    /// The (compressed) left bit width. This is after bit-packing the dictionary codes.
    left_bit_width: u8,
    /// The right bit width. This is the bit-packed width of each of the "real double" values.
    right_bit_width: u8,
}

pub trait ALPRDDecimalValue: DecimalValue {
    /// The unsigned integer type with the same bit-width as the floating-point type.
    type UnsignedIntType: UIntegerValue;

    /// Bit-wise transmute from the unsigned integer type to the floating-point type.
    fn from_bits(bits: Self::UnsignedIntType) -> Self;

    /// Bit-wise transmute into the unsigned integer type.
    fn to_bits(value: Self) -> Self::UnsignedIntType;

    /// Truncating conversion from the unsigned integer type to `u16`.
    fn to_u16(bits: Self::UnsignedIntType) -> u16;

    /// Type-widening conversion from `u16` to the unsigned integer type.
    fn from_u16(value: u16) -> Self::UnsignedIntType;
}

impl ALPRDDecimalValue for FloatValue<f64> {
    type UnsignedIntType = u64;

    fn from_bits(bits: Self::UnsignedIntType) -> Self {
        FloatValue::new(f64::from_bits(bits))
    }

    fn to_bits(value: Self) -> Self::UnsignedIntType {
        value.0.0.to_bits()
    }

    fn to_u16(bits: Self::UnsignedIntType) -> u16 {
        bits as u16
    }

    fn from_u16(value: u16) -> Self::UnsignedIntType {
        value as u64
    }
}

impl ALPRDDecimalValue for FloatValue<f32> {
    type UnsignedIntType = u32;

    fn from_bits(bits: Self::UnsignedIntType) -> Self {
        FloatValue::new(f32::from_bits(bits))
    }

    fn to_bits(value: Self) -> Self::UnsignedIntType {
        value.0.0.to_bits()
    }

    fn to_u16(bits: Self::UnsignedIntType) -> u16 {
        bits as u16
    }

    fn from_u16(value: u16) -> Self::UnsignedIntType {
        value as u32
    }
}

impl<T> NumericEncoding<T> for ALPRDEncoding<T>
where
    T: ALPRDDecimalValue,
{
    fn kind(&self) -> EncodingKind {
        EncodingKind::AlpRd
    }

    fn is_suitable(&self, config: &EncodingConfig, _stats: &NumericStats<T>) -> bool {
        if config.max_cascading_levels < 2 {
            // For efficient compression, ALP-RD encoding artifacts must be encoded themselves
            // using a cascading encoding scheme.
            return false;
        }
        true
    }

    fn stats_collector_flags(&self) -> NumericStatsCollectorFlags {
        NumericStatsCollectorFlags::empty()
    }

    fn analyze(
        &self,
        values: &[T],
        _null_mask: &NullMask,
        config: &EncodingConfig,
        stats: &NumericStats<T>,
        context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>> {
        let mut left_parts = vec![];
        let mut right_parts = vec![];
        let mut exception_pos = vec![];
        let mut exception_values = vec![];

        let alprd_params = self.calculate_parameters(values, context);

        Self::split(
            values,
            &alprd_params,
            &mut left_parts,
            &mut right_parts,
            &mut exception_pos,
            &mut exception_values,
        );

        if (exception_values.len() as f64 / values.len() as f64) > 0.05
            || exception_values.len() > u16::MAX as usize
        {
            // If there are too many exceptions, the encoding compression ratio won't be good enough.
            return Ok(None);
        }

        let mut encoded_size = 2
            + ALPRDMetadata::size()
            + alprd_params.left_parts_dict.len() * std::mem::size_of::<u16>();

        let mut cascading_encodings = Vec::with_capacity(2);

        // Encode the left parts section using a cascading encoding, or bail out.
        let left_parts_outcome = context.numeric_encoders.get::<u16>().analyze(
            &left_parts,
            &NullMask::None,
            &config.make_cascading_config(),
            context,
        )?;
        if let Some(left_parts_outcome) = left_parts_outcome {
            encoded_size += left_parts_outcome.encoded_size;
            cascading_encodings.push(Some(left_parts_outcome));
        } else {
            return Ok(None);
        };

        // Encode the right parts using BitPacking encoding.
        let right_parts_outcome = context
            .numeric_encoders
            .get::<T::UnsignedIntType>()
            .analyze(
                &right_parts,
                &NullMask::None,
                &config
                    .make_cascading_config()
                    .with_min_compression_ratio(1.0)
                    .with_allowed_encodings(&[EncodingKind::FLBitPack]),
                context,
            )?;
        right_parts_outcome
            .map(|right_parts_outcome| {
                encoded_size += right_parts_outcome.encoded_size;
                cascading_encodings.push(Some(right_parts_outcome));

                if !exception_values.is_empty() {
                    // Encode the exceptions positions and values.
                    encoded_size += exception_pos.len() * std::mem::size_of::<u32>()
                        + exception_values.len() * std::mem::size_of::<u16>();
                }

                Ok(AnalysisOutcome {
                    encoding: self.kind(),
                    cascading_outcomes: cascading_encodings,
                    encoded_size,
                    compression_ratio: stats.original_size as f64 / encoded_size as f64,
                    parameters: Some(EncodingParameters::AlpRd(alprd_params)),
                })
            })
            .transpose()
    }

    fn encode(
        &self,
        values: &[T],
        _null_mask: &NullMask,
        target: &mut AlignedByteVec,
        plan: &EncodingPlan,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize> {
        let Some(EncodingParameters::AlpRd(alprd_params)) = plan.parameters else {
            return Err(Error::invalid_arg(
                "plan",
                "ALPRD encoding parameters are missing",
            ));
        };

        let mut left_parts = vec![];
        let mut right_parts = vec![];
        let mut exception_pos = vec![];
        let mut exception_values = vec![];

        Self::split(
            values,
            &alprd_params,
            &mut left_parts,
            &mut right_parts,
            &mut exception_pos,
            &mut exception_values,
        );

        let prev_len = target.len();
        target.write_value::<u16>(self.kind() as u16);

        let mut metadata = ALPRDMetadata::initialize(target);
        metadata.exceptions_count = exception_values.len();
        metadata.right_bit_width = alprd_params.right_bit_width;
        metadata.dictionary_len = alprd_params.left_parts_dict.len();

        target.write_values(&alprd_params.left_parts_dict);

        let left_parts_plan = plan.cascading_encodings[0]
            .as_ref()
            .expect("Left parts encoding plan must be provided");
        metadata.left_parts_size = context.numeric_encoders.get::<u16>().encode(
            &left_parts,
            &NullMask::None,
            target,
            left_parts_plan,
            context,
        )?;

        let right_parts_plan = plan.cascading_encodings[1]
            .as_ref()
            .expect("Right parts encoding plan must be provided");
        metadata.right_parts_size = context
            .numeric_encoders
            .get::<T::UnsignedIntType>()
            .encode(
                &right_parts,
                &NullMask::None,
                target,
                right_parts_plan,
                context,
            )?;

        if !exception_values.is_empty() {
            // Encode the exceptions positions and values.
            target.write_values(&exception_pos);
            target.write_values(&exception_values);
        }

        metadata.finalize(target);

        Ok(target.len() - prev_len)
    }

    fn decode(
        &self,
        buffer: &[u8],
        value_count: usize,
        params: Option<&EncodingParameters>,
        target: &mut AlignedByteVec,
        context: &EncodingContext,
    ) -> amudai_common::Result<()> {
        let (metadata, buffer) = ALPRDMetadata::read_from(buffer)?;

        let dict_size = metadata.dictionary_len * std::mem::size_of::<u16>();
        let mut left_parts_dict = ArrayVec::<[u16; MAX_DICT_SIZE]>::new();
        left_parts_dict.resize(metadata.dictionary_len, 0);
        left_parts_dict
            .as_mut_slice()
            .as_mut_byte_slice()
            .copy_from_slice(&buffer[..dict_size]);
        let buffer = &buffer[dict_size..];
        let alprd_params = AlpRdParameters {
            right_bit_width: metadata.right_bit_width,
            left_parts_dict,
        };

        let mut left_parts = AlignedByteVec::new();
        let mut right_parts = AlignedByteVec::new();

        context.numeric_encoders.get::<u16>().decode(
            &buffer[..metadata.left_parts_size],
            value_count,
            params,
            &mut left_parts,
            context,
        )?;
        let left_parts = left_parts.typed_data::<u16>();
        let buffer = &buffer[metadata.left_parts_size..];

        context
            .numeric_encoders
            .get::<T::UnsignedIntType>()
            .decode(
                &buffer[..metadata.right_parts_size],
                value_count,
                params,
                &mut right_parts,
                context,
            )?;
        let right_parts = right_parts.typed_data::<T::UnsignedIntType>();
        let buffer = &buffer[metadata.right_parts_size..];

        let mut exceptions_pos = context.buffers.get_buffer();
        let mut exceptions_values = context.buffers.get_buffer();
        let (exceptions_pos, exceptions_values) = if metadata.exceptions_count > 0 {
            let exceptions_pos_size = metadata.exceptions_count * size_of::<u32>();
            let exceptions_values_size = metadata.exceptions_count * size_of::<u16>();
            if buffer.len() < exceptions_pos_size + exceptions_values_size {
                return Err(Error::invalid_format("Encoded buffer size is too small"));
            }
            exceptions_pos.extend_from_slice(&buffer[..exceptions_pos_size]);
            exceptions_values.extend_from_slice(&buffer[exceptions_pos_size..]);
            (
                exceptions_pos.typed_data::<u32>(),
                exceptions_values.typed_data::<u16>(),
            )
        } else {
            (&[] as &[u32], &[] as &[u16])
        };

        Self::decode_chunk(
            left_parts,
            right_parts,
            exceptions_pos,
            exceptions_values,
            &alprd_params,
            context,
            target,
        )
    }

    fn inspect(
        &self,
        buffer: &[u8],
        context: &EncodingContext,
    ) -> amudai_common::Result<EncodingPlan> {
        let (metadata, buffer) = ALPRDMetadata::read_from(buffer)?;

        let dict_size = metadata.dictionary_len * std::mem::size_of::<u16>();
        let buffer = &buffer[dict_size..];
        let left_parts_plan = context
            .numeric_encoders
            .get::<u16>()
            .inspect(&buffer[..metadata.left_parts_size], context)?;

        let buffer = &buffer[metadata.left_parts_size..];
        let right_parts_plan = context
            .numeric_encoders
            .get::<T::UnsignedIntType>()
            .inspect(&buffer[..metadata.right_parts_size], context)?;

        Ok(EncodingPlan {
            encoding: self.kind(),
            parameters: Default::default(),
            cascading_encodings: vec![Some(left_parts_plan), Some(right_parts_plan)],
        })
    }
}

struct ALPRDMetadata {
    start_offset: usize,
    /// Encoded left parts section size in bytes.
    pub left_parts_size: usize,
    /// Encoded right parts section size in bytes.
    pub right_parts_size: usize,
    /// Count of exception values.
    pub exceptions_count: usize,
    /// Right bit width.
    pub right_bit_width: u8,
    /// Number of values in the dictionary.
    pub dictionary_len: usize,
}

impl AlignedEncMetadata for ALPRDMetadata {
    fn own_size() -> usize {
        12
    }

    fn initialize(target: &mut AlignedByteVec) -> Self {
        let start_offset = target.len();
        target.resize(start_offset + Self::size(), 0);
        Self {
            start_offset,
            left_parts_size: 0,
            right_parts_size: 0,
            exceptions_count: 0,
            right_bit_width: 0,
            dictionary_len: 0,
        }
    }

    fn finalize(self, target: &mut AlignedByteVec) {
        target.write_value_at::<u32>(self.start_offset, self.left_parts_size as u32);
        target.write_value_at::<u32>(self.start_offset + 4, self.right_parts_size as u32);
        target.write_value_at::<u16>(self.start_offset + 8, self.exceptions_count as u16);
        target.write_value_at::<u8>(self.start_offset + 10, self.right_bit_width);
        target.write_value_at::<u8>(self.start_offset + 11, self.dictionary_len as u8);
    }

    fn read_from(buffer: &[u8]) -> amudai_common::Result<(Self, &[u8])> {
        if buffer.len() < Self::size() {
            return Err(Error::invalid_format("Encoded buffer size is too small"));
        }
        Ok((
            Self {
                start_offset: 0,
                left_parts_size: buffer.read_value::<u32>(0) as usize,
                right_parts_size: buffer.read_value::<u32>(4) as usize,
                exceptions_count: buffer.read_value::<u16>(8) as usize,
                right_bit_width: buffer.read_value::<u8>(10),
                dictionary_len: buffer.read_value::<u8>(11) as usize,
            },
            &buffer[Self::size()..],
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::encodings::{
        EncodingConfig, EncodingContext, EncodingKind, EncodingPlan, NullMask,
        numeric::value::FloatValue,
    };
    use amudai_bytes::buffer::AlignedByteVec;

    #[test]
    fn test_round_trip() {
        let config = EncodingConfig::default()
            .with_allowed_encodings(&[EncodingKind::AlpRd])
            .with_min_compression_ratio(1.1);
        let context = EncodingContext::new();
        let data: Vec<FloatValue<f64>> = (0..10000)
            .map(|_| {
                FloatValue::new(
                    format!("{:.2}", fastrand::f64() * 30.0 + 10.0)
                        .parse::<f64>()
                        .unwrap(),
                )
            })
            .collect();
        let outcome = context
            .numeric_encoders
            .get::<FloatValue<f64>>()
            .analyze(&data, &NullMask::None, &config, &context)
            .unwrap();
        assert!(outcome.is_some());
        let outcome = outcome.unwrap();
        let encoded_size1 = outcome.encoded_size;
        let parameters = outcome.parameters.clone();
        let plan = outcome.into_plan();

        assert_eq!(
            EncodingPlan {
                encoding: EncodingKind::AlpRd,
                parameters,
                cascading_encodings: vec![
                    Some(EncodingPlan {
                        encoding: EncodingKind::FLBitPack,
                        parameters: Default::default(),
                        cascading_encodings: vec![]
                    }),
                    Some(EncodingPlan {
                        encoding: EncodingKind::FLBitPack,
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
            .get::<FloatValue<f64>>()
            .encode(&data, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();
        assert_eq!(encoded_size1, encoded_size2);

        // Validate that inspect() returns the same encoding plan as used for encoding
        let mut inspected_plan = context
            .numeric_encoders
            .get::<FloatValue<f64>>()
            .inspect(&encoded, &context)
            .unwrap();
        inspected_plan.parameters = plan.parameters.clone();
        assert_eq!(plan, inspected_plan);

        let mut decoded = AlignedByteVec::new();
        context
            .numeric_encoders
            .get::<FloatValue<f64>>()
            .decode(&encoded, data.len(), None, &mut decoded, &context)
            .unwrap();

        for (a, b) in data.iter().zip(decoded.typed_data()) {
            assert_eq!(a, b);
        }
    }
}
