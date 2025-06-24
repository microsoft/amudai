// ! This code is based on original code from repository: https://github.com/spiraldb/alp
use super::{
    NumericEncoding,
    stats::{NumericStats, NumericStatsCollectorFlags},
    value::{AsMutByteSlice, DecimalValue, FloatValue, IntegerValue, ValueReader, ValueWriter},
};
use crate::encodings::{
    AlignedEncMetadata, AlpParameters, AnalysisOutcome, EncodingConfig, EncodingContext,
    EncodingKind, EncodingParameters, EncodingPlan, NullMask,
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;
use num_traits::{CheckedSub, ToPrimitive};

/// Adaptive Lossless floating-Point encoding as described
/// in the paper "ALP: Adaptive Lossless floating-Point Compression".
/// <https://dl.acm.org/doi/pdf/10.1145/3626717>
pub struct ALPEncoding<T>(std::marker::PhantomData<T>);

impl<T> ALPEncoding<T>
where
    T: ALPDecimalValue,
{
    const SAMPLE_SIZE: usize = 32;

    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }

    pub fn calculate_parameters(
        values: &[T],
        integers: &mut Vec<T::ALPIntegerType>,
        patch_indices: &mut Vec<u32>,
        patch_values: &mut Vec<T>,
    ) -> AlpParameters {
        let mut best_params = AlpParameters {
            exponent_e: 0,
            factor_f: 0,
        };
        let mut best_nbytes: usize = usize::MAX;

        let sample: Option<Vec<T>> = (values.len() > Self::SAMPLE_SIZE).then(|| {
            values
                .iter()
                .step_by(values.len() / Self::SAMPLE_SIZE)
                .copied()
                .collect()
        });

        for e in (0..T::MAX_EXPONENT).rev() {
            for f in 0..e {
                integers.clear();
                patch_indices.clear();
                patch_values.clear();
                Self::encode(
                    sample.as_deref().unwrap_or(values),
                    AlpParameters {
                        exponent_e: e,
                        factor_f: f,
                    },
                    integers,
                    patch_indices,
                    patch_values,
                );
                let size = Self::estimate_encoded_size(integers, patch_values);
                if size < best_nbytes {
                    best_nbytes = size;
                    best_params = AlpParameters {
                        exponent_e: e,
                        factor_f: f,
                    };
                } else if size == best_nbytes
                    && e - f < best_params.exponent_e - best_params.factor_f
                {
                    best_params = AlpParameters {
                        exponent_e: e,
                        factor_f: f,
                    };
                }
            }
        }

        best_params
    }

    fn estimate_encoded_size(encoded: &[T::ALPIntegerType], patches: &[T]) -> usize {
        let minmax = encoded.iter().fold(None, |minmax, next| {
            let (min, max) = minmax.unwrap_or((next, next));
            Some((min.min(next), max.max(next)))
        });
        let bits_per_encoded = minmax
            // estimating bits per encoded value assuming frame-of-reference + bitpacking-without-patches
            .and_then(|(min, max)| max.checked_sub(min))
            .and_then(|range_size: T::ALPIntegerType| range_size.to_u64())
            .and_then(|range_size| {
                range_size
                    .checked_ilog2()
                    .map(|bits| (bits + 1) as usize)
                    .or(Some(0))
            })
            .unwrap_or(size_of::<T::ALPIntegerType>() * 8);

        let encoded_bytes = (encoded.len() * bits_per_encoded).div_ceil(8);
        // each patch is a value + a position
        // in practice, patch positions are in [0, u16::MAX] because of how we chunk
        let patch_bytes = patches.len() * (size_of::<Self>() + size_of::<u16>());

        encoded_bytes + patch_bytes
    }

    fn encode(
        values: &[T],
        params: AlpParameters,
        integers: &mut Vec<T::ALPIntegerType>,
        patch_indices: &mut Vec<u32>,
        patch_values: &mut Vec<T>,
    ) {
        let mut fill_value: Option<T::ALPIntegerType> = None;

        // this is intentionally branchless
        // we batch this into 32KB of values at a time to make it more L1 cache friendly
        let encode_chunk_size: usize = (32 << 10) / size_of::<T::ALPIntegerType>();
        for chunk in values.chunks(encode_chunk_size) {
            Self::encode_chunk(
                chunk,
                params,
                integers,
                patch_indices,
                patch_values,
                &mut fill_value,
            );
        }
    }

    fn encode_chunk(
        chunk: &[T],
        params: AlpParameters,
        integers: &mut Vec<T::ALPIntegerType>,
        patch_indices: &mut Vec<u32>,
        patch_values: &mut Vec<T>,
        fill_value: &mut Option<T::ALPIntegerType>,
    ) {
        let num_prev_encoded = integers.len();
        let num_prev_patches = patch_indices.len();
        assert_eq!(patch_indices.len(), patch_values.len());
        let has_filled = fill_value.is_some();

        // encode the chunk, counting the number of patches
        let mut chunk_patch_count = 0;
        integers.extend(chunk.iter().map(|v| {
            let encoded = unsafe { Self::encode_single(*v, params) };
            let decoded = Self::decode_single(encoded, params);
            let neq = (decoded != *v) as usize;
            chunk_patch_count += neq;
            encoded
        }));
        let chunk_patch_count = chunk_patch_count; // immutable hereafter
        assert_eq!(integers.len(), num_prev_encoded + chunk.len());

        if chunk_patch_count > 0 {
            // we need to gather the patches for this chunk
            // preallocate space for the patches (plus one because our loop may attempt to write one past the end)
            patch_indices.reserve(chunk_patch_count + 1);
            patch_values.reserve(chunk_patch_count + 1);

            // record the patches in this chunk
            let patch_indices_mut = patch_indices.spare_capacity_mut();
            let patch_values_mut = patch_values.spare_capacity_mut();
            let mut chunk_patch_index = 0;
            for i in num_prev_encoded..integers.len() {
                let decoded = Self::decode_single(integers[i], params);
                // write() is only safe to call more than once because the values are primitive (i.e., Drop is a no-op)
                patch_indices_mut[chunk_patch_index].write(i as u32);
                patch_values_mut[chunk_patch_index].write(chunk[i - num_prev_encoded]);
                chunk_patch_index += (decoded != chunk[i - num_prev_encoded]) as usize;
            }
            assert_eq!(chunk_patch_index, chunk_patch_count);
            unsafe {
                patch_indices.set_len(num_prev_patches + chunk_patch_count);
                patch_values.set_len(num_prev_patches + chunk_patch_count);
            }
        }

        // find the first successfully encoded value (i.e., not patched)
        // this is our fill value for missing values
        if fill_value.is_none() && (num_prev_encoded + chunk_patch_count < integers.len()) {
            assert_eq!(num_prev_encoded, num_prev_patches);
            for i in num_prev_encoded..integers.len() {
                if i >= patch_indices.len() || patch_indices[i] != i as u32 {
                    *fill_value = Some(integers[i]);
                    break;
                }
            }
        }

        // replace the patched values in the encoded array with the fill value
        // for better downstream compression
        if let Some(fill_value) = fill_value {
            // handle the edge case where the first N >= 1 chunks are all patches
            let start_patch = if has_filled { num_prev_patches } else { 0 };
            for patch_idx in &patch_indices[start_patch..] {
                integers[*patch_idx as usize] = *fill_value;
            }
        }
    }

    fn decode_chunk(
        encoded: &[T::ALPIntegerType],
        params: AlpParameters,
        patch_indices: &[u32],
        patch_values: &[T],
        target: &mut AlignedByteVec,
    ) {
        let pos = target.len();
        for &encoded in encoded {
            target.push_typed::<T>(Self::decode_single(encoded, params));
        }
        let target = target.typed_data_mut();
        for (i, &patch_idx) in patch_indices.iter().enumerate() {
            target[pos + patch_idx as usize] = patch_values[i];
        }
    }

    #[inline]
    fn decode_single(encoded: T::ALPIntegerType, params: AlpParameters) -> T {
        T::from_int(encoded)
            * T::F10[params.factor_f as usize]
            * T::IF10[params.exponent_e as usize]
    }

    #[inline]
    unsafe fn encode_single(value: T, params: AlpParameters) -> T::ALPIntegerType {
        (value * T::F10[params.exponent_e as usize] * T::IF10[params.factor_f as usize])
            .fast_round()
            .as_int()
    }
}

pub trait ALPDecimalValue: DecimalValue {
    type ALPIntegerType: IntegerValue;

    const FRACTIONAL_BITS: u8;
    const MAX_EXPONENT: u8;
    const SWEET: Self;
    const F10: &'static [Self];
    const IF10: &'static [Self];

    /// Equivalent to calling `as` to cast the primitive float to the target integer type.
    fn as_int(self) -> Self::ALPIntegerType;

    /// Convert from the integer type back to the float type using `as`.
    fn from_int(n: Self::ALPIntegerType) -> Self;

    /// Round to the nearest floating integer by shifting in and out of the low precision range.
    #[inline]
    fn fast_round(self) -> Self {
        (self + Self::SWEET) - Self::SWEET
    }
}

impl ALPDecimalValue for FloatValue<f32> {
    type ALPIntegerType = i32;
    const FRACTIONAL_BITS: u8 = 23;
    const MAX_EXPONENT: u8 = 10;
    const SWEET: Self =
        Self::new((1 << Self::FRACTIONAL_BITS) as f32 + (1 << (Self::FRACTIONAL_BITS - 1)) as f32);

    const F10: &'static [Self] = &[
        Self::new(1.0),
        Self::new(10.0),
        Self::new(100.0),
        Self::new(1000.0),
        Self::new(10000.0),
        Self::new(100000.0),
        Self::new(1000000.0),
        Self::new(10000000.0),
        Self::new(100000000.0),
        Self::new(1000000000.0),
        Self::new(10000000000.0), // 10^10
    ];
    const IF10: &'static [Self] = &[
        Self::new(1.0),
        Self::new(0.1),
        Self::new(0.01),
        Self::new(0.001),
        Self::new(0.0001),
        Self::new(0.00001),
        Self::new(0.000001),
        Self::new(0.0000001),
        Self::new(0.00000001),
        Self::new(0.000000001),
        Self::new(0.0000000001), // 10^-10
    ];

    #[inline]
    fn as_int(self) -> Self::ALPIntegerType {
        self.0.0 as _
    }

    #[inline]
    fn from_int(n: Self::ALPIntegerType) -> Self {
        Self::new(n as f32)
    }
}

impl ALPDecimalValue for FloatValue<f64> {
    type ALPIntegerType = i64;
    const FRACTIONAL_BITS: u8 = 52;
    const MAX_EXPONENT: u8 = 18; // 10^18 is the maximum i64
    const SWEET: Self = Self::new(
        (1u64 << Self::FRACTIONAL_BITS) as f64 + (1u64 << (Self::FRACTIONAL_BITS - 1)) as f64,
    );
    const F10: &'static [Self] = &[
        Self::new(1.0),
        Self::new(10.0),
        Self::new(100.0),
        Self::new(1000.0),
        Self::new(10000.0),
        Self::new(100000.0),
        Self::new(1000000.0),
        Self::new(10000000.0),
        Self::new(100000000.0),
        Self::new(1000000000.0),
        Self::new(10000000000.0),
        Self::new(100000000000.0),
        Self::new(1000000000000.0),
        Self::new(10000000000000.0),
        Self::new(100000000000000.0),
        Self::new(1000000000000000.0),
        Self::new(10000000000000000.0),
        Self::new(100000000000000000.0),
        Self::new(1000000000000000000.0),
        Self::new(10000000000000000000.0),
        Self::new(100000000000000000000.0),
        Self::new(1000000000000000000000.0),
        Self::new(10000000000000000000000.0),
        Self::new(100000000000000000000000.0), // 10^23
    ];

    const IF10: &'static [Self] = &[
        Self::new(1.0),
        Self::new(0.1),
        Self::new(0.01),
        Self::new(0.001),
        Self::new(0.0001),
        Self::new(0.00001),
        Self::new(0.000001),
        Self::new(0.0000001),
        Self::new(0.00000001),
        Self::new(0.000000001),
        Self::new(0.0000000001),
        Self::new(0.00000000001),
        Self::new(0.000000000001),
        Self::new(0.0000000000001),
        Self::new(0.00000000000001),
        Self::new(0.000000000000001),
        Self::new(0.0000000000000001),
        Self::new(0.00000000000000001),
        Self::new(0.000000000000000001),
        Self::new(0.0000000000000000001),
        Self::new(0.00000000000000000001),
        Self::new(0.000000000000000000001),
        Self::new(0.0000000000000000000001),
        Self::new(0.00000000000000000000001), // 10^-23
    ];

    #[inline]
    fn as_int(self) -> Self::ALPIntegerType {
        self.0.0 as _
    }

    #[inline]
    fn from_int(n: Self::ALPIntegerType) -> Self {
        Self::new(n as f64)
    }
}

impl<T> NumericEncoding<T> for ALPEncoding<T>
where
    T: ALPDecimalValue,
{
    fn kind(&self) -> EncodingKind {
        EncodingKind::Alp
    }

    fn is_suitable(&self, config: &EncodingConfig, _stats: &NumericStats<T>) -> bool {
        if config.max_cascading_levels < 2 {
            // For efficient compression, integers sequence must be encoded with a cascading
            // encoding.
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
        let mut integers = Vec::with_capacity(values.len());
        let mut patch_indices = vec![];
        let mut patch_values = vec![];

        let alp_params = Self::calculate_parameters(
            values,
            &mut integers,
            &mut patch_indices,
            &mut patch_values,
        );

        integers.clear();
        patch_indices.clear();
        patch_values.clear();

        Self::encode(
            values,
            alp_params,
            &mut integers,
            &mut patch_indices,
            &mut patch_values,
        );

        if patch_values.len() > u16::MAX as usize {
            // If there are too many exceptions, the encoding compression ratio won't be good enough.
            return Ok(None);
        }

        let mut encoded_size = 2 + ALPMetadata::size();

        let integers_outcome = context
            .numeric_encoders
            .get::<T::ALPIntegerType>()
            .analyze(
                &integers,
                &NullMask::None,
                &config.make_cascading_config(),
                context,
            )?;
        integers_outcome
            .map(|integers_outcome| {
                encoded_size += integers_outcome.encoded_size;
                if !patch_values.is_empty() {
                    encoded_size +=
                        patch_values.len() * T::SIZE + patch_indices.len() * size_of::<u32>();
                }
                Ok(AnalysisOutcome {
                    encoding: self.kind(),
                    cascading_outcomes: vec![Some(integers_outcome)],
                    encoded_size,
                    compression_ratio: stats.original_size as f64 / encoded_size as f64,
                    parameters: Some(EncodingParameters::Alp(alp_params)),
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
        let Some(EncodingParameters::Alp(alp_params)) = plan.parameters else {
            return Err(Error::invalid_arg(
                "plan",
                "ALP encoding parameters are missing",
            ));
        };

        let mut integers = Vec::with_capacity(values.len());
        let mut patch_indices = vec![];
        let mut patch_values = vec![];

        Self::encode(
            values,
            alp_params,
            &mut integers,
            &mut patch_indices,
            &mut patch_values,
        );

        let initial_size = target.len();
        target.write_value::<u16>(self.kind() as u16);

        let mut metadata = ALPMetadata::initialize(target);
        metadata.exceptions_count = patch_values.len();
        metadata.parameters = alp_params;

        let integers_plan = plan.cascading_encodings[0]
            .as_ref()
            .expect("Integers encoding plan must be provided");
        metadata.integers_size = context.numeric_encoders.get::<T::ALPIntegerType>().encode(
            &integers,
            &NullMask::None,
            target,
            integers_plan,
            context,
        )?;

        if !patch_values.is_empty() {
            target.write_values(&patch_indices);
            target.write_values(&patch_values);
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
        let (metadata, buffer) = ALPMetadata::read_from(buffer)?;

        let mut integers = context.buffers.get_buffer();
        let mut patch_indices = vec![];
        let mut patch_values = vec![];

        context.numeric_encoders.get::<T::ALPIntegerType>().decode(
            &buffer[..metadata.integers_size],
            value_count,
            params,
            &mut integers,
            context,
        )?;
        let integers = integers.typed_data::<T::ALPIntegerType>();

        let buffer = &buffer[metadata.integers_size..];
        if metadata.exceptions_count > 0 {
            let patch_indices_size = metadata.exceptions_count * size_of::<u32>();
            let patch_values_size = metadata.exceptions_count * size_of::<T>();
            if buffer.len() < patch_indices_size + patch_values_size {
                return Err(Error::invalid_format("Encoded buffer size is too small"));
            }

            patch_indices.resize(metadata.exceptions_count, 0);
            patch_indices
                .as_mut_slice()
                .as_mut_byte_slice()
                .copy_from_slice(&buffer[..patch_indices_size]);

            patch_values.resize(metadata.exceptions_count, T::zero());
            patch_values
                .as_mut_slice()
                .as_mut_byte_slice()
                .copy_from_slice(&buffer[patch_indices_size..]);
        }

        Self::decode_chunk(
            integers,
            metadata.parameters,
            &patch_indices,
            &patch_values,
            target,
        );

        Ok(())
    }

    fn inspect(
        &self,
        buffer: &[u8],
        context: &EncodingContext,
    ) -> amudai_common::Result<EncodingPlan> {
        let (metadata, _) = ALPMetadata::read_from(buffer)?;
        let integers_plan = context
            .numeric_encoders
            .get::<T::ALPIntegerType>()
            .inspect(&buffer[..metadata.integers_size], context)?;
        Ok(EncodingPlan {
            encoding: self.kind(),
            parameters: Some(EncodingParameters::Alp(metadata.parameters)),
            cascading_encodings: vec![Some(integers_plan)],
        })
    }
}

struct ALPMetadata {
    start_offset: usize,
    /// Size of the encoded integers section in bytes.
    pub integers_size: usize,
    /// Number of exceptions.
    pub exceptions_count: usize,
    /// ALP exponents.
    pub parameters: AlpParameters,
}

impl AlignedEncMetadata for ALPMetadata {
    fn own_size() -> usize {
        8
    }

    fn initialize(target: &mut AlignedByteVec) -> Self {
        let start_offset = target.len();
        target.resize(start_offset + Self::size(), 0);
        Self {
            start_offset,
            integers_size: 0,
            exceptions_count: 0,
            parameters: AlpParameters {
                exponent_e: 0,
                factor_f: 0,
            },
        }
    }

    fn finalize(self, target: &mut AlignedByteVec) {
        target.write_value_at::<u32>(self.start_offset, self.integers_size as u32);
        target.write_value_at::<u16>(self.start_offset + 4, self.exceptions_count as u16);
        target.write_value_at::<u8>(self.start_offset + 6, self.parameters.exponent_e);
        target.write_value_at::<u8>(self.start_offset + 7, self.parameters.factor_f);
    }

    fn read_from(buffer: &[u8]) -> amudai_common::Result<(Self, &[u8])> {
        if buffer.len() < Self::size() {
            return Err(Error::invalid_format("Encoded buffer size is too small"));
        }
        Ok((
            Self {
                start_offset: 0,
                integers_size: buffer.read_value::<u32>(0) as usize,
                exceptions_count: buffer.read_value::<u16>(4) as usize,
                parameters: AlpParameters {
                    exponent_e: buffer.read_value::<u8>(6),
                    factor_f: buffer.read_value::<u8>(7),
                },
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
        let config = EncodingConfig::default().with_allowed_encodings(&[EncodingKind::Alp]);
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
        let parameters = outcome.parameters.clone();
        let plan = outcome.into_plan();

        assert_eq!(
            EncodingPlan {
                encoding: EncodingKind::Alp,
                parameters,
                cascading_encodings: vec![Some(EncodingPlan {
                    encoding: EncodingKind::FusedFrameOfReference,
                    parameters: Default::default(),
                    cascading_encodings: vec![]
                })]
            },
            plan
        );

        let mut encoded = AlignedByteVec::new();
        context
            .numeric_encoders
            .get::<FloatValue<f64>>()
            .encode(&data, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();

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
