use super::{
    AnalysisOutcome, EncodingConfig, EncodingContext, EncodingKind, EncodingParameters,
    EncodingPlan, NullMask, numeric::value::ValueReader,
};
use crate::encodings::numeric::generic::{LZ4Encoder, ZSTDEncoder};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;
use amudai_format::schema::BasicTypeDescriptor;
use amudai_sequence::{presence::Presence, sequence::ValueSequence};
use arrow_array::Array;
use dictionary::BlockDictionaryEncoding;
use fsst::FSSTEncoding;
use generic::GenericEncoding;
use itertools::Itertools;
use plain::PlainEncoding;
use single_value::SingleValueEncoding;
use stats::{BinaryStats, BinaryStatsCollector, BinaryStatsCollectorFlags};
use std::{borrow::Cow, collections::HashMap, sync::Arc};

mod dictionary;
mod fsst;
mod generic;
mod offsets;
mod plain;
mod single_value;
mod stats;

pub trait StringEncoding: Send + Sync {
    /// Unique encoder name.
    fn kind(&self) -> EncodingKind;

    /// Checks if the encoding is suitable based on the collected statistics.
    fn is_suitable(&self, config: &EncodingConfig, stats: &BinaryStats) -> bool;

    /// Returns statistics to gather, which are required for this encoding.
    /// This is used for limiting the amount of statistics that are collected.
    fn stats_collector_flags(&self) -> BinaryStatsCollectorFlags;

    /// This method implements analysis phase of the encoding process, where this encoding efficiency is
    /// evaluated based on the collected statistics and the sample data.
    ///
    /// # Parameters
    ///
    /// - `values` - The sequence of binary values to be analyzed. This is typically a sample of the
    ///   actual data that will be encoded.
    /// - `null_mask` - The null mask indicating which values are null.
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
        values: &BinaryValuesSequence,
        null_mask: &NullMask,
        config: &EncodingConfig,
        stats: &BinaryStats,
        context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>>;

    /// Encodes the given data using this encoder and provided encoding plan, and returns the outcome.
    /// The encoded data is written to the `target` vector, which should be large enough to store
    /// the encoded content.
    ///
    /// # Parameters
    ///
    /// - `values` - The sequence of binary values to be encoded.
    /// - `null_mask` - The null mask indicating which values are null.
    /// - `target` - The target buffer to store the encoded data.
    /// - `plan` - The encoding plan that specifies the encoding scheme to use.
    /// - `context` - The encoding context that provides an access to shared resources.
    ///
    /// # Returns
    ///
    /// Returns the size of the encoded data in bytes.
    fn encode(
        &self,
        values: &BinaryValuesSequence,
        null_mask: &NullMask,
        target: &mut AlignedByteVec,
        plan: &EncodingPlan,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize>;

    /// Decodes encoded values from `buffer` byte buffer, and returns the decoded sequence
    /// of values.
    ///
    /// The encoded buffer typically starts with encoding-specific metadata header.
    ///
    /// # Parameters
    ///
    /// - `buffer` - The byte buffer containing the encoded data.
    /// - `presence` - The presence information indicating which values are present.
    /// - `type_desc` - The type descriptor for the data being decoded.
    /// - `params`: Encoding specific parameters.
    /// - `context` - The encoding context that provides an access to shared resources.
    ///
    /// # Returns
    ///
    /// Returns the decoded sequence of values.
    fn decode(
        &self,
        buffer: &[u8],
        presence: Presence,
        type_desc: BasicTypeDescriptor,
        params: Option<&EncodingParameters>,
        context: &EncodingContext,
    ) -> amudai_common::Result<ValueSequence>;

    /// Inspects the provided encoding buffer and returns the encoding plan
    /// that was used when encoding the data.
    ///
    /// # Parameters
    ///
    /// - `buffer` - The byte buffer containing the encoded data.
    /// - `context` - The encoding context that provides an access to shared resources.
    ///
    /// # Returns
    ///
    /// Returns the encoding plan that was used to encode the data.
    fn inspect(
        &self,
        buffer: &[u8],
        context: &EncodingContext,
    ) -> amudai_common::Result<EncodingPlan>;
}

/// A collection of encodings suitable for encoding a sequence
/// of binary buffer values.
pub struct BinaryEncodings {
    stats_collector: BinaryStatsCollector,
    encodings_by_name: Arc<HashMap<EncodingKind, Box<dyn StringEncoding>>>,
}

impl BinaryEncodings {
    pub fn new() -> Self {
        let encodings = vec![
            Box::new(BlockDictionaryEncoding::new()) as Box<dyn StringEncoding>,
            Box::new(FSSTEncoding::new()),
            Box::new(SingleValueEncoding::new()),
            Box::new(PlainEncoding::new()),
            Box::new(GenericEncoding::new(ZSTDEncoder)),
            Box::new(GenericEncoding::new(LZ4Encoder)),
        ];
        assert!(
            encodings
                .iter()
                .map(|encoding| encoding.kind())
                .all_unique()
        );
        let encodings_by_name = encodings
            .into_iter()
            .map(|encoding| (encoding.kind(), encoding))
            .collect();
        Self {
            stats_collector: BinaryStatsCollector::new(),
            encodings_by_name: Arc::new(encodings_by_name),
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
    /// - `values` - The sequence of binary values to be analyzed. This is typically a sample of the
    ///   actual data that will be encoded.
    /// - `null_mask` - The null mask indicating which values are null.
    /// - `config`: The encoding configuration that specifies the encoding options.
    /// - `context`: The encoding context that provides an access to shared resources.
    ///
    /// # Returns
    ///
    /// Returns the encoding outcome if the compression ratio of the best encoding schema is greater
    /// than the minimum required, otherwise returns `None`.
    pub fn analyze(
        &self,
        values: &BinaryValuesSequence,
        null_mask: &NullMask,
        config: &EncodingConfig,
        context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>> {
        if config.max_cascading_levels == 0 {
            return Ok(None);
        }

        let stats = self.collect_stats(values, null_mask, config);

        let suitable_encodings = self.encodings_by_name.values().filter(|encoding| {
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
    /// - `values` - The sequence of binary values to be encoded.
    /// - `null_mask` - The null mask indicating which values are null.
    /// - `target` - The target buffer to store the encoded data.
    /// - `plan` - The encoding plan that specifies the encoding scheme to use.
    /// - `context` - The encoding context that provides an access to shared resources.
    ///
    /// # Returns
    ///
    /// The method returns the encoded size in bytes.
    pub fn encode(
        &self,
        values: &BinaryValuesSequence,
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

    /// Decodes the encoded data from the encoded byte byffer and returns a sequence
    /// of decoded values.
    ///
    /// The encoded buffer starts with a 2-byte encoding code, which is used to identify the
    /// encoding used for the data. The method reads this code and uses it to find the appropriate
    /// decoder to decode the data.
    ///
    /// # Parameters
    ///
    /// - `buffer` -    The byte buffer containing the encoded data.
    /// - `presence` -  The presence information indicating which values are present.
    /// - `type_desc` - The type descriptor for the data being decoded.
    /// - `params` -    Encoding specific parameters.
    /// - `context` -   The encoding context that provides an access to shared resources.
    ///
    /// # Returns
    ///
    /// Returns the decoded sequence of values.
    pub fn decode(
        &self,
        buffer: &[u8],
        presence: Presence,
        type_desc: BasicTypeDescriptor,
        params: Option<&EncodingParameters>,
        context: &EncodingContext,
    ) -> amudai_common::Result<ValueSequence> {
        let (encoding, buffer) = self.read_encoding(buffer)?;
        encoding.decode(buffer, presence, type_desc, params, context)
    }

    /// Inspects the provided encoding buffer and returns the encoding plan
    /// that was used when encoding the data.
    ///
    /// # Parameters
    ///
    /// - `buffer` - The byte buffer containing the encoded data.
    /// - `context` - The encoding context that provides an access to shared resources.
    ///
    /// # Returns
    ///
    /// Returns the encoding plan that was used to encode the data.
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
    ) -> amudai_common::Result<(&'a dyn StringEncoding, &'b [u8])> {
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
        values: &BinaryValuesSequence,
        null_mask: &NullMask,
        config: &EncodingConfig,
    ) -> BinaryStats {
        let stats_flags = self
            .encodings_by_name
            .values()
            .filter(|encoding| config.is_encoding_allowed(encoding.kind()))
            .map(|encoding| encoding.stats_collector_flags())
            .fold(BinaryStatsCollectorFlags::empty(), |a, b| a | b);
        self.stats_collector.collect(values, null_mask, stats_flags)
    }
}

impl Default for BinaryEncodings {
    fn default() -> Self {
        Self::new()
    }
}

/// Sequence of binary values that serves as an input for encoding.
#[allow(clippy::enum_variant_names)]
pub enum BinaryValuesSequence<'a> {
    ArrowBinaryArray(Cow<'a, arrow_array::array::BinaryArray>),
    ArrowLargeBinaryArray(Cow<'a, arrow_array::array::LargeBinaryArray>),
    ArrowFixedSizeBinaryArray(Cow<'a, arrow_array::array::FixedSizeBinaryArray>),
}

impl BinaryValuesSequence<'_> {
    /// Returns the value at the given index.
    ///
    /// The method doesn't indicate whether the value is null or not,
    /// it's the responsibility of the caller to check the null mask.
    pub fn get(&self, index: usize) -> &[u8] {
        match self {
            BinaryValuesSequence::ArrowBinaryArray(arr) => arr.value(index),
            BinaryValuesSequence::ArrowLargeBinaryArray(arr) => arr.value(index),
            BinaryValuesSequence::ArrowFixedSizeBinaryArray(arr) => arr.value(index),
        }
    }

    /// Returns the length of the sequence, including null values.
    pub fn len(&self) -> usize {
        match self {
            BinaryValuesSequence::ArrowBinaryArray(arr) => arr.len(),
            BinaryValuesSequence::ArrowLargeBinaryArray(arr) => arr.len(),
            BinaryValuesSequence::ArrowFixedSizeBinaryArray(arr) => arr.len(),
        }
    }

    /// Returns `true` if the sequence is empty, otherwise `false`.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> BinaryValuesSequenceIter {
        BinaryValuesSequenceIter::new(self)
    }

    /// Returns concatenated values buffer.
    fn values(&self) -> &[u8] {
        if self.is_empty() {
            return &[];
        }
        match self {
            BinaryValuesSequence::ArrowBinaryArray(arr) => {
                let first_offset = *arr.offsets().first().unwrap() as usize;
                let last_offset = *arr.offsets().last().unwrap() as usize;
                let data = arr.value_data();
                // Returning a slice is required in case the array is a slice of a larger array,
                // in which case data is a slice of the original data.
                &data[first_offset..last_offset]
            }
            BinaryValuesSequence::ArrowLargeBinaryArray(arr) => {
                let first_offset = *arr.offsets().first().unwrap() as usize;
                let last_offset = *arr.offsets().last().unwrap() as usize;
                let data = arr.value_data();
                // Returning a slice is required in case the array is a slice of a larger array,
                // in which case data is a slice of the original data.
                &data[first_offset..last_offset]
            }
            BinaryValuesSequence::ArrowFixedSizeBinaryArray(arr) => arr.value_data(),
        }
    }

    /// Returns fixed value size, if the values have fixed size, otherwise returns `None`.
    fn fixed_value_size(&self) -> Option<u32> {
        match self {
            BinaryValuesSequence::ArrowBinaryArray(_) => None,
            BinaryValuesSequence::ArrowLargeBinaryArray(_) => None,
            BinaryValuesSequence::ArrowFixedSizeBinaryArray(arr) => Some(arr.value_length() as u32),
        }
    }

    /// Returns value offsets if values size is not fixed, otherwise returns `None`.
    /// If underlying offset type is i32, creates a new buffer containing translated to `u64` offsets.
    fn offsets(&self) -> Option<Cow<[u64]>> {
        match self {
            BinaryValuesSequence::ArrowBinaryArray(arr) => {
                if arr.is_empty() {
                    return Some(Cow::Borrowed(&[]));
                }
                let first_offset = *arr.offsets().first().unwrap() as u64;
                // Subtracting the first offset from all offsets in case the array is a slice of another one.
                Some(Cow::Owned(
                    arr.offsets()
                        .inner()
                        .iter()
                        .map(|&v| v as u64 - first_offset)
                        .collect(),
                ))
            }
            BinaryValuesSequence::ArrowLargeBinaryArray(arr) => {
                if arr.is_empty() {
                    return Some(Cow::Borrowed(&[]));
                }
                let first_offset = *arr.offsets().first().unwrap() as u64;
                if first_offset > 0 {
                    // Subtracting the first offset from all offsets because the array is a slice of another one.
                    Some(Cow::Owned(
                        arr.offsets()
                            .inner()
                            .iter()
                            .map(|&v| v as u64 - first_offset)
                            .collect(),
                    ))
                } else {
                    // If the first offset is 0, we can return the offsets as is.
                    Some(Cow::Borrowed(unsafe {
                        std::mem::transmute::<&[i64], &[u64]>(arr.offsets().inner().as_ref())
                    }))
                }
            }
            BinaryValuesSequence::ArrowFixedSizeBinaryArray(_) => None,
        }
    }

    /// Returns the size in bytes required to store the binary data and offsets
    /// in this sequence.
    fn data_size(&self) -> u32 {
        match self {
            BinaryValuesSequence::ArrowBinaryArray(arr) => {
                (arr.offsets().inner().inner().len() + self.values().len()) as u32
            }
            BinaryValuesSequence::ArrowLargeBinaryArray(arr) => {
                (arr.offsets().inner().inner().len() + self.values().len()) as u32
            }
            BinaryValuesSequence::ArrowFixedSizeBinaryArray(arr) => arr.value_data().len() as u32,
        }
    }
}

impl<'a> TryFrom<&'a dyn arrow_array::array::Array> for BinaryValuesSequence<'a> {
    type Error = Error;

    fn try_from(array: &'a dyn arrow_array::array::Array) -> Result<Self, Self::Error> {
        if let Some(arr) = array
            .as_any()
            .downcast_ref::<arrow_array::array::BinaryArray>()
        {
            Ok(BinaryValuesSequence::ArrowBinaryArray(Cow::Borrowed(arr)))
        } else if let Some(arr) = array
            .as_any()
            .downcast_ref::<arrow_array::array::LargeBinaryArray>()
        {
            Ok(BinaryValuesSequence::ArrowLargeBinaryArray(Cow::Borrowed(
                arr,
            )))
        } else if let Some(arr) = array
            .as_any()
            .downcast_ref::<arrow_array::array::FixedSizeBinaryArray>()
        {
            Ok(BinaryValuesSequence::ArrowFixedSizeBinaryArray(
                Cow::Borrowed(arr),
            ))
        } else {
            Err(Error::invalid_arg("array", "Unsupported array type"))
        }
    }
}

impl From<BinaryValuesSequence<'_>> for Box<dyn arrow_array::array::Array> {
    fn from(val: BinaryValuesSequence<'_>) -> Self {
        match val {
            BinaryValuesSequence::ArrowBinaryArray(arr) => Box::new(arr.into_owned()),
            BinaryValuesSequence::ArrowLargeBinaryArray(arr) => Box::new(arr.into_owned()),
            BinaryValuesSequence::ArrowFixedSizeBinaryArray(arr) => Box::new(arr.into_owned()),
        }
    }
}

pub struct BinaryValuesSequenceIter<'a, 'b> {
    sequence: &'a BinaryValuesSequence<'b>,
    index: usize,
    len: usize,
}

impl<'a, 'b> BinaryValuesSequenceIter<'a, 'b> {
    pub fn new(sequence: &'a BinaryValuesSequence<'b>) -> Self {
        let len = sequence.len();
        Self {
            sequence,
            index: 0,
            len,
        }
    }
}

impl<'a> Iterator for BinaryValuesSequenceIter<'a, '_> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.len {
            let value = self.sequence.get(self.index);
            self.index += 1;
            Some(value)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encodings::binary::BinaryEncodings;
    use std::vec;

    #[test]
    pub fn test_round_trip() {
        let context = EncodingContext::new();
        let str_enc = BinaryEncodings::new();
        let config = EncodingConfig::default();

        let unique_values = [b"first" as &[u8], b"second", b"third", b"fourth", b"fifth"];
        let values = (0..10000)
            .map(|_| {
                fastrand::choice(unique_values.iter())
                    .map(|&v| v.to_vec())
                    .unwrap()
            })
            .collect::<Vec<_>>();

        let array = arrow_array::array::LargeBinaryArray::from_iter_values(values.iter());
        let sequence = BinaryValuesSequence::ArrowLargeBinaryArray(Cow::Owned(array));
        let outcome = str_enc
            .analyze(&sequence, &NullMask::None, &config, &context)
            .unwrap();
        assert!(outcome.is_some());
        let outcome = outcome.unwrap();
        let encoded_size1 = outcome.encoded_size;
        let plan = outcome.into_plan();

        assert_eq!(
            EncodingPlan {
                encoding: EncodingKind::BlockDictionary,
                parameters: Default::default(),
                cascading_encodings: vec![
                    Some(EncodingPlan {
                        encoding: EncodingKind::FLBitPack,
                        parameters: Default::default(),
                        cascading_encodings: vec![]
                    }),
                    None
                ]
            },
            plan
        );

        let mut encoded = AlignedByteVec::new();
        let encoded_size2 = str_enc
            .encode(&sequence, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();
        assert_eq!(encoded_size1, encoded_size2);

        let decoded = str_enc
            .decode(
                &encoded,
                Presence::Trivial(values.len()),
                Default::default(),
                None,
                &context,
            )
            .unwrap();
        assert_eq!(values.len(), decoded.presence.len());
        for (a, b) in values.iter().zip(decoded.binary_values().unwrap()) {
            assert_eq!(a.as_slice(), b);
        }
    }
}
