use super::{
    AnalysisOutcome, EncodingConfig, EncodingContext, EncodingKind, EncodingParameters,
    EncodingPlan, NullMask,
};
use alp::{ALPDecimalValue, ALPEncoding};
use alprd::{ALPRDDecimalValue, ALPRDEncoding};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;
use bitpacking::FlBitPackingEncoding;
use delta::DeltaEncoding;
use dictionary::BlockDictionaryEncoding;
use frame_of_reference::{FFOREncoding, FOREncoding};
use generic::{GenericEncoding, LZ4Encoder, ZSTDEncoder};
use itertools::Itertools;
use plain::PlainEncoding;
use run_length::RLEEncoding;
use single_value::SingleValueEncoding;
use sparse::SparseEncoding;
use stats::{
    IntegerStatsCollector, NumericStats, NumericStatsCollector, NumericStatsCollectorFlags,
    StatsCollector,
};
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};
use truncation::{
    TruncatableSInteger, TruncateU8Encoding, TruncateU16Encoding, TruncateU32Encoding,
    TruncateU64Encoding,
};
use value::{DecimalValue, FloatValue, NumericValue, SIntegerValue, UIntegerValue, ValueReader};
use zigzag::ZigZagEncoding;

mod alp;
pub(crate) mod alprd;
mod bitpacking;
mod delta;
mod dictionary;
mod fastlanes;
mod frame_of_reference;
pub(crate) mod generic;
mod plain;
mod run_length;
mod single_value;
mod sparse;
mod stats;
mod truncation;
pub(crate) mod value;
mod zigzag;

pub trait NumericEncoding<T>: Send + Sync {
    /// Unique encoder name.
    fn kind(&self) -> EncodingKind;

    /// Checks if the encoding is suitable based on the collected statistics.
    fn is_suitable(&self, config: &EncodingConfig, stats: &NumericStats<T>) -> bool;

    /// Returns statistics to gather, which are required for this encoding.
    /// This is used for limiting the amount of statistics that are collected.
    fn stats_collector_flags(&self) -> NumericStatsCollectorFlags;

    /// This method implements analysis phase of the encoding process, where this encoding efficiency is
    /// evaluated based on the collected statistics and the sample data.
    ///
    /// # Parameters
    ///
    /// - `values`: Typed sequence of numeric values to analyze. This is typically a sample of the
    ///   entire data set.
    /// - `null_mask`: Null mask that indicates which values are null.
    /// - `config`: The encoding configuration that specifies the encoding options.
    /// - `stats`: The statistics collected from the data.
    /// - `context`: The encoding context that provides an access to shared resources.
    ///
    /// # Returns
    ///
    /// Returns structure that contains the cascade of applied encodings as well as the encoder parameters
    /// used if the compression ratio of the encoding is greater  than the minimum required,
    /// otherwise returns `None`.
    fn analyze(
        &self,
        values: &[T],
        null_mask: &NullMask,
        config: &EncodingConfig,
        stats: &NumericStats<T>,
        context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>>;

    /// Encodes the given data using this encoder and provided encoding plan, and returns the outcome.
    /// The encoded data is written to the `target` vector, which should be large enough to store
    /// the encoded content.
    ///
    /// # Parameters
    ///
    /// - `values`: Typed sequence of numeric values to encode.
    /// - `null_mask`: Null mask that indicates which values are null.
    /// - `target`: The target byte buffer to write the encoded data into.
    /// - `plan`: The encoding plan that specifies the encoding schema.
    /// - `context`: The encoding context that provides an access to shared resources.
    ///
    /// Null mask is not encoded, but it's served as a hint to choose appropriate encoding, or
    /// it's involved into encoding of values themselves whenever it's important to know whether
    /// a value is null or not.
    fn encode(
        &self,
        values: &[T],
        null_mask: &NullMask,
        target: &mut AlignedByteVec,
        plan: &EncodingPlan,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize>;

    /// Decodes encoded values from `buffer` byte buffer, and writes them into `target`
    /// vector.
    ///
    /// The encoded buffer typically starts with encoding-specific metadata header.
    ///
    /// # Parameters
    ///
    /// - `buffer`: The encoded byte buffer containing the data to decode.
    /// - `value_count`: The number of values to decode.
    /// - `params`: Encoding specific parameters.
    /// - `target`: The target byte buffer to write the decoded data into.
    /// - `context`: The encoding context that provides an access to shared resources.
    ///
    /// # Returns
    ///
    /// The method returns a result indicating success or failure of the decoding operation.
    fn decode(
        &self,
        buffer: &[u8],
        value_count: usize,
        params: Option<&EncodingParameters>,
        target: &mut AlignedByteVec,
        context: &EncodingContext,
    ) -> amudai_common::Result<()>;

    /// Inspects the encoded data in the buffer and returns an encoding plan that describes
    /// the encoding schema used for the data.
    ///
    /// # Parameters
    ///
    /// - `buffer`: The encoded byte buffer containing the data to inspect.
    /// - `context`: The encoding context that provides an access to shared resources.
    ///
    /// # Returns
    ///
    /// The method returns an `EncodingPlan` that describes the encoding schema used for the data.
    fn inspect(
        &self,
        buffer: &[u8],
        context: &EncodingContext,
    ) -> amudai_common::Result<EncodingPlan>;
}

/// A collection of encodings suitable for encoding a sequence
/// of numeric values with type `T`.
///
/// # Type Parameters
///
/// - `T`: The numeric value type. It must implement the `NumericValue` trait.
pub struct NumericEncodings<T>
where
    T: NumericValue,
{
    stats_collector: Arc<dyn StatsCollector<T>>,
    encodings_by_name: Arc<HashMap<EncodingKind, Arc<dyn NumericEncoding<T>>>>,
    encodings: Vec<Arc<dyn NumericEncoding<T>>>,
}

impl<T> NumericEncodings<T>
where
    T: NumericValue,
{
    fn new(
        encodings: Vec<Arc<dyn NumericEncoding<T>>>,
        stats_collector: Arc<dyn StatsCollector<T>>,
    ) -> Self {
        assert!(
            encodings
                .iter()
                .map(|encoding| encoding.kind())
                .all_unique()
        );
        let encodings_by_name = encodings
            .iter()
            .map(|encoding| (encoding.kind(), Arc::clone(encoding)))
            .collect();
        Self {
            stats_collector,
            encodings_by_name: Arc::new(encodings_by_name),
            encodings,
        }
    }

    /// This method implements analysis phase of the encoding process, where the best suitable
    /// encoder and encoding parameters are selected based on the collected statistics and the
    /// sample data.
    ///
    /// Null mask is not encoded, but it's served as a hint to choose appropriate encoding, or
    /// it's involved into encoding of values themselves whenever it's important to know whether
    /// a value is null or not.
    ///
    /// # Parameters
    ///
    /// - `values`: Typed sequence of numeric values to analyze. This is typically a sample of the
    ///   entire data set.
    /// - `null_mask`: Null mask that indicates which values are null.
    /// - `config`: The encoding configuration that specifies the encoding options.
    /// - `context`: The encoding context that provides an access to shared resources.
    ///
    /// # Returns
    ///
    /// Returns the encoding outcome if the compression ratio of the best encoding schema is greater
    /// than the minimum required, otherwise returns `None`.
    pub fn analyze(
        &self,
        values: &[T],
        null_mask: &NullMask,
        config: &EncodingConfig,
        context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>> {
        if config.max_cascading_levels == 0 {
            return Ok(None);
        }

        let stats = self.collect_stats(values, null_mask, config);

        let suitable_encodings = self.encodings.iter().filter(|encoding| {
            config.is_encoding_allowed(encoding.kind())
                && stats.is_encoding_allowed(encoding.kind())
                && encoding.is_suitable(config, &stats)
        });

        let mut best_outcome: Option<AnalysisOutcome> = None;
        let mut best_compression_ratio = 0.0;

        for encoding in suitable_encodings {
            if let Some(outcome) = encoding.analyze(values, null_mask, config, &stats, context)? {
                let compression_ratio = outcome.compression_ratio;
                if compression_ratio > config.min_compression_ratio
                    && compression_ratio > best_compression_ratio
                {
                    best_outcome = Some(outcome);
                    best_compression_ratio = compression_ratio;
                }
            }
        }
        Ok(best_outcome)
    }

    /// Encodes the data using the provided encoding plan, built during the analysis phase.
    /// A concrete encoding may deviate from the plan if the plan is not suitable for the
    /// provided data.
    ///
    /// # Parameters
    ///
    /// - `values`: Typed sequence of numeric values to encode.
    /// - `null_mask`: Null mask that indicates which values are null.
    /// - `target`: The target byte buffer to write the encoded data into.
    /// - `plan`: The encoding plan that specifies the encoding schema.
    /// - `context`: The encoding context that provides an access to shared resources.
    ///
    /// # Returns
    ///
    /// The method returns the encoded size in bytes.
    pub fn encode(
        &self,
        values: &[T],
        null_mask: &NullMask,
        target: &mut AlignedByteVec,
        plan: &EncodingPlan,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize> {
        let encoding = self.encodings_by_name.get(&plan.encoding).ok_or_else(|| {
            Error::invalid_arg("plan", format!("Unsupported encoding: {:?}", plan.encoding))
        })?;
        encoding.encode(values, null_mask, target, plan, context)
    }

    /// Decodes the encoded data from the encoded byte byffer and writes the decoded values into
    /// the target vector. It is guaranteed that the written data is aligned to the original
    /// data type size.
    ///
    /// The encoded buffer starts with a 2-byte encoding code, which is used to identify the
    /// encoding used for the data. The method reads this code and uses it to find the appropriate
    /// decoder to decode the data.
    ///
    /// # Parameters
    ///
    /// - `buffer` -     The encoded byte buffer containing the data to decode.
    /// - `value_count`- The number of values to decode.
    /// - `params` -     Encoding specific parameters.
    /// - `target` -     The target byte buffer to write the decoded data into.
    /// - `context` -    The encoding context that provides an access to shared resources.
    ///
    /// # Returns
    ///
    /// The method returns a result indicating success or failure of the decoding operation.
    pub fn decode(
        &self,
        buffer: &[u8],
        value_count: usize,
        params: Option<&EncodingParameters>,
        target: &mut AlignedByteVec,
        context: &EncodingContext,
    ) -> amudai_common::Result<()> {
        let (encoding, buffer) = self.read_encoding(buffer)?;
        encoding.decode(buffer, value_count, params, target, context)
    }

    /// Inspects the encoded data in the buffer and returns an encoding plan that describes
    /// the encoding schema used for the data.
    ///
    /// # Parameters
    ///
    /// - `buffer`: The encoded byte buffer containing the data to inspect.
    /// - `context`: The encoding context that provides an access to shared resources.
    ///
    /// # Returns
    ///
    /// The method returns an `EncodingPlan` that describes the encoding schema used for the data.
    pub fn inspect(
        &self,
        buffer: &[u8],
        context: &EncodingContext,
    ) -> amudai_common::Result<EncodingPlan> {
        let (encoding, buffer) = self.read_encoding(buffer)?;
        encoding.inspect(buffer, context)
    }

    /// Reads the encoding code from the buffer and returns the corresponding encoding
    /// along with the remaining buffer.
    fn read_encoding<'a, 'b>(
        &'a self,
        buffer: &'b [u8],
    ) -> amudai_common::Result<(&'a dyn NumericEncoding<T>, &'b [u8])> {
        if buffer.len() < 2 {
            return Err(amudai_common::error::Error::invalid_format(
                "Buffer is too small".to_string(),
            ));
        }
        let encoding_code = buffer.read_value::<u16>(0);
        if let Ok(encoding_kind) = EncodingKind::try_from(encoding_code) {
            if let Some(encoding) = self.encodings_by_name.get(&encoding_kind) {
                Ok((encoding.as_ref(), &buffer[2..]))
            } else {
                Err(amudai_common::error::Error::invalid_format(format!(
                    "Unsupported encoding: {encoding_kind:?}"
                )))
            }
        } else {
            Err(amudai_common::error::Error::invalid_format(format!(
                "Unrecognized encoding code: {encoding_code}"
            )))
        }
    }

    fn collect_stats(
        &self,
        values: &[T],
        null_mask: &NullMask,
        config: &EncodingConfig,
    ) -> NumericStats<T> {
        let stats_flags = self
            .encodings_by_name
            .values()
            .filter(|encoding| config.is_encoding_allowed(encoding.kind()))
            .map(|encoding| encoding.stats_collector_flags())
            .fold(NumericStatsCollectorFlags::empty(), |a, b| a | b);
        self.stats_collector.collect(values, null_mask, stats_flags)
    }
}

/// The pool that contains pre-initialized encodings sets
/// for all supported numeric data types.
pub struct NumericEncodingsPool {
    encodings: EncodingsMap,
}

impl NumericEncodingsPool {
    pub fn new() -> Self {
        let mut encodings = EncodingsMap::new();
        encodings.insert(Self::create_sinteger_encodings::<i8>());
        encodings.insert(Self::create_sinteger_encodings::<i16>());
        encodings.insert(Self::create_sinteger_encodings::<i32>());
        encodings.insert(Self::create_sinteger_encodings::<i64>());
        encodings.insert(Self::create_sinteger_encodings::<i128>());
        encodings.insert(Self::create_uinteger_encodings::<u8>());
        encodings.insert(Self::create_uinteger_encodings::<u16>());
        encodings.insert(Self::create_uinteger_encodings::<u32>());
        encodings.insert(Self::create_uinteger_encodings::<u64>());
        encodings.insert(Self::create_uinteger_encodings::<u128>());
        encodings.insert(Self::create_decimal_encodings::<FloatValue<f32>>());
        encodings.insert(Self::create_decimal_encodings::<FloatValue<f64>>());
        Self { encodings }
    }

    /// Returns the encodings set for the numeric data type `T`.
    pub fn get<T>(&self) -> &NumericEncodings<T>
    where
        T: NumericValue,
    {
        self.encodings.get().unwrap_or_else(|| {
            panic!(
                "Numeric encoder not found for type: {}",
                std::any::type_name::<T>()
            )
        })
    }

    /// Initialize encodings set for signed integer type `T`.
    fn create_sinteger_encodings<T>() -> NumericEncodings<T>
    where
        T: SIntegerValue + TruncatableSInteger,
    {
        let mut encodings = vec![
            Arc::new(FFOREncoding::<T>::new()) as Arc<dyn NumericEncoding<T>>,
            // TODO: is there a need for having both FOR and FFOR?
            Arc::new(FOREncoding::<T>::new()),
            Arc::new(DeltaEncoding::<T>::new()),
            Arc::new(RLEEncoding::<T>::new()),
            Arc::new(SingleValueEncoding::<T>::new()),
            Arc::new(BlockDictionaryEncoding::<T>::new()),
            Arc::new(PlainEncoding::<T>::new()),
            Arc::new(GenericEncoding::<T, _>::new(ZSTDEncoder)),
            Arc::new(GenericEncoding::<T, _>::new(LZ4Encoder)),
            Arc::new(ZigZagEncoding::<T>::new()),
            Arc::new(SparseEncoding::<T>::new()),
        ];
        if T::SIZE > 1 {
            encodings.push(Arc::new(TruncateU8Encoding::<T>::new()));
        }
        if T::SIZE > 2 {
            encodings.push(Arc::new(TruncateU16Encoding::<T>::new()));
        }
        if T::SIZE > 4 {
            encodings.push(Arc::new(TruncateU32Encoding::<T>::new()));
        }
        if T::SIZE > 8 {
            encodings.push(Arc::new(TruncateU64Encoding::<T>::new()));
        }
        NumericEncodings::new(encodings, Arc::new(IntegerStatsCollector::<T>::new()) as _)
    }

    /// Initialize encodings set for unsigned integer type `T`.
    fn create_uinteger_encodings<T>() -> NumericEncodings<T>
    where
        T: UIntegerValue,
    {
        let mut encodings = vec![];
        if T::SIZE < 16 {
            encodings
                .push(Arc::new(FlBitPackingEncoding::<T>::new()) as Arc<dyn NumericEncoding<T>>);
        }
        encodings.extend([
            Arc::new(FFOREncoding::<T>::new()) as Arc<dyn NumericEncoding<T>>,
            // TODO: is there a need for having both FOR and FFOR?
            Arc::new(FOREncoding::<T>::new()),
            Arc::new(DeltaEncoding::<T>::new()),
            Arc::new(RLEEncoding::<T>::new()),
            Arc::new(SingleValueEncoding::<T>::new()),
            Arc::new(BlockDictionaryEncoding::<T>::new()),
            Arc::new(PlainEncoding::<T>::new()),
            Arc::new(GenericEncoding::<T, _>::new(ZSTDEncoder)),
            Arc::new(GenericEncoding::<T, _>::new(LZ4Encoder)),
            Arc::new(SparseEncoding::<T>::new()),
        ]);
        NumericEncodings::new(encodings, Arc::new(IntegerStatsCollector::<T>::new()) as _)
    }

    /// Initialize encodings set for floating point type `T`.
    fn create_decimal_encodings<T>() -> NumericEncodings<T>
    where
        T: DecimalValue + ALPDecimalValue + ALPRDDecimalValue,
    {
        let encodings = vec![
            Arc::new(ALPEncoding::<T>::new()) as Arc<dyn NumericEncoding<T>>,
            Arc::new(ALPRDEncoding::<T>::new()),
            Arc::new(RLEEncoding::<T>::new()),
            Arc::new(SingleValueEncoding::<T>::new()),
            Arc::new(BlockDictionaryEncoding::<T>::new()),
            Arc::new(PlainEncoding::<T>::new()),
            Arc::new(GenericEncoding::<T, _>::new(ZSTDEncoder)),
            Arc::new(GenericEncoding::<T, _>::new(LZ4Encoder)),
            Arc::new(SparseEncoding::<T>::new()),
        ];
        NumericEncodings::new(encodings, Arc::new(NumericStatsCollector::<T>::new()) as _)
    }
}

impl Default for NumericEncodingsPool {
    fn default() -> Self {
        Self::new()
    }
}

/// A hashmap for storing encodings sets per numeric type.
struct EncodingsMap {
    encodings: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl EncodingsMap {
    fn new() -> Self {
        Self {
            encodings: HashMap::new(),
        }
    }

    fn insert<T>(&mut self, encodings: NumericEncodings<T>)
    where
        T: NumericValue,
    {
        self.encodings
            .insert(TypeId::of::<T>(), Arc::new(encodings));
    }

    fn get<T>(&self) -> Option<&NumericEncodings<T>>
    where
        T: NumericValue,
    {
        self.encodings
            .get(&TypeId::of::<T>())
            .and_then(|enc| enc.downcast_ref::<NumericEncodings<T>>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encodings::{EncodingContext, EncodingPlan};

    #[test]
    pub fn test_round_trip_sorted() {
        let context = EncodingContext::new();
        let encodings = context.numeric_encoders.get::<i64>();
        let config = EncodingConfig::default();

        let data: Vec<i64> = (10000001..10064001).collect();
        let outcome = encodings
            .analyze(&data, &NullMask::None, &config, &context)
            .unwrap();
        assert!(outcome.is_some());
        let outcome = outcome.unwrap();
        assert!(outcome.compression_ratio >= 23000.0);
        let encoded_size1 = outcome.encoded_size;
        let plan = outcome.into_plan();

        assert_eq!(
            EncodingPlan {
                encoding: EncodingKind::TruncateU32,
                parameters: Default::default(),
                cascading_encodings: vec![Some(EncodingPlan {
                    encoding: EncodingKind::Delta,
                    parameters: Default::default(),
                    cascading_encodings: vec![Some(EncodingPlan {
                        encoding: EncodingKind::SingleValue,
                        parameters: Default::default(),
                        cascading_encodings: vec![],
                    }),],
                })]
            },
            plan,
        );

        let mut encoded = AlignedByteVec::new();
        let encoded_size2 = encodings
            .encode(&data, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();
        assert_eq!(encoded_size1, encoded_size2);

        let mut decoded = AlignedByteVec::new();
        encodings
            .decode(&encoded, data.len(), None, &mut decoded, &context)
            .unwrap();
        for (a, b) in data.iter().zip(decoded.typed_data::<i64>()) {
            assert_eq!(a, b);
        }
    }

    #[test]
    pub fn test_round_trip_unsorted() {
        let context = EncodingContext::new();
        let encodings = context.numeric_encoders.get::<i64>();
        let config = EncodingConfig::default();

        let data: Vec<i64> = (0..65536)
            .map(|_| fastrand::i64(-100..100) + 1000000)
            .collect();
        let outcome = encodings
            .analyze(&data, &NullMask::None, &config, &context)
            .unwrap();
        assert!(outcome.is_some());
        let outcome = outcome.unwrap();
        assert!(outcome.compression_ratio >= 7.0);
        let encoded_size1 = outcome.encoded_size;

        let plan = outcome.into_plan();
        assert_eq!(
            EncodingPlan {
                encoding: EncodingKind::FusedFrameOfReference,
                parameters: Default::default(),
                cascading_encodings: vec![],
            },
            plan,
        );

        let mut encoded = AlignedByteVec::new();
        let encoded_size2 = encodings
            .encode(&data, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();
        assert_eq!(encoded_size1, encoded_size2);

        let mut decoded = AlignedByteVec::new();
        encodings
            .decode(&encoded, data.len(), None, &mut decoded, &context)
            .unwrap();
        for (a, b) in data.iter().zip(decoded.typed_data::<i64>()) {
            assert_eq!(a, b);
        }
    }
}
