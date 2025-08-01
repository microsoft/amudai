use super::{
    BinaryValuesSequence, StringEncoding,
    stats::{BinaryStats, BinaryStatsCollectorFlags},
};
use crate::encodings::{
    ALIGNMENT_BYTES, AlignedEncMetadata, AnalysisOutcome, EncodingConfig, EncodingContext,
    EncodingKind, EncodingParameters, EncodingPlan, NullMask,
    numeric::value::{ValueReader, ValueWriter},
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;
use amudai_format::schema::BasicTypeDescriptor;
use amudai_sequence::{
    presence::Presence,
    value_sequence::{BinarySequenceBuilder, FixedSizeBinarySequenceBuilder, ValueSequence},
};
use std::{collections::HashMap, hash::Hash};

/// Dictionary encoding that embeds dictionary into each encoded block.
pub struct BlockDictionaryEncoding;

impl BlockDictionaryEncoding {
    pub fn new() -> Self {
        Self
    }
}

impl StringEncoding for BlockDictionaryEncoding {
    fn kind(&self) -> EncodingKind {
        EncodingKind::BlockDictionary
    }

    fn is_suitable(&self, config: &EncodingConfig, stats: &BinaryStats) -> bool {
        if config.max_cascading_levels < 2 {
            // For efficient compression, codes stream must be encoded with
            // a cascading encoding scheme.
            return false;
        }
        if let Some(unique_values) = stats.unique_values.as_ref() {
            let non_null_count = stats.values_count - stats.nulls_count;
            unique_values.len() < (non_null_count as usize / 2)
        } else {
            false
        }
    }

    fn stats_collector_flags(&self) -> BinaryStatsCollectorFlags {
        BinaryStatsCollectorFlags::UNIQUE_VALUES
    }

    fn analyze(
        &self,
        values: &BinaryValuesSequence,
        null_mask: &NullMask,
        config: &EncodingConfig,
        stats: &BinaryStats,
        context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>> {
        let mut encoded_size = 2 + DictionaryMetadata::size();

        let unique_values = stats
            .unique_values
            .as_ref()
            .expect("Unique values must be present in statistics");

        // Sort unique values by popularity to allow better compression of dictionary codes
        // with BitPacking encoding.
        let mut values_by_popularity = Vec::with_capacity(unique_values.len());
        for (value, &count) in unique_values {
            values_by_popularity.push((value, count));
        }
        values_by_popularity.sort_unstable_by_key(|(_, count)| std::cmp::Reverse(*count));

        let mut cascading_outcomes = vec![None, None];
        {
            let mut codes = context.buffers.get_buffer();
            generate_codes(values, null_mask, &values_by_popularity, &mut codes);

            if let Some(codes_outcome) = context.numeric_encoders.get::<u32>().analyze(
                codes.typed_data(),
                &NullMask::None,
                &config
                    .make_cascading_config()
                    .with_disallowed_encodings(&[EncodingKind::BlockDictionary]),
                context,
            )? {
                encoded_size += codes_outcome.encoded_size;
                cascading_outcomes[0] = Some(codes_outcome);
            } else {
                return Ok(None);
            }
        }

        encoded_size = encoded_size.next_multiple_of(ALIGNMENT_BYTES);

        let mut total_size = 0;
        // Only encode lengths if values are not fixed size.
        if values.fixed_value_size().is_none() {
            let mut lengths = context.buffers.get_buffer();
            lengths.reserve(values_by_popularity.len() * std::mem::size_of::<u32>());
            for (value, _) in values_by_popularity.iter() {
                lengths.push_typed::<u32>(value.len() as u32);
                total_size += value.len();
            }

            if let Some(length_outcome) = context.numeric_encoders.get::<u32>().analyze(
                lengths.typed_data(),
                &NullMask::None,
                &config.make_cascading_config(),
                context,
            )? {
                encoded_size += length_outcome.encoded_size;
                cascading_outcomes[1] = Some(length_outcome);
            } else {
                encoded_size += lengths.len();
            }
        }
        encoded_size += total_size;

        Ok(Some(AnalysisOutcome {
            encoding: self.kind(),
            cascading_outcomes,
            encoded_size,
            compression_ratio: stats.original_size as f64 / encoded_size as f64,
            parameters: Default::default(),
        }))
    }

    fn encode(
        &self,
        values: &BinaryValuesSequence,
        null_mask: &NullMask,
        target: &mut AlignedByteVec,
        plan: &EncodingPlan,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize> {
        let initial_size = target.len();
        target.write_value::<u16>(self.kind() as u16);

        let mut metadata = DictionaryMetadata::initialize(target);

        let mut unique_values = HashMap::new();
        for i in 0..values.len() {
            *unique_values.entry(values.get(i)).or_insert(0) += 1;
        }
        metadata.unique_values_count = unique_values.len();

        // Sort unique values by popularity to allow better compression of dictionary codes
        // with BitPacking encoding.
        let mut values_by_popularity = unique_values.into_iter().collect::<Vec<_>>();
        values_by_popularity.sort_unstable_by_key(|(_, count)| std::cmp::Reverse(*count));

        {
            let mut codes = context.buffers.get_buffer();
            generate_codes(values, null_mask, &values_by_popularity, &mut codes);

            let codes_plan = plan.cascading_encodings[0]
                .as_ref()
                .expect("Codes encoding plan must be provided");
            metadata.codes_encoded_size = context.numeric_encoders.get::<u32>().encode(
                codes.typed_data(),
                &NullMask::None,
                target,
                codes_plan,
                context,
            )?;
        }

        let alignment = target.len().next_multiple_of(ALIGNMENT_BYTES);
        target.resize(alignment, 0);

        // Only encode lengths if values are not fixed size.
        if values.fixed_value_size().is_none() {
            let mut lengths = context.buffers.get_buffer();
            lengths.reserve(values_by_popularity.len() * std::mem::size_of::<u32>());
            for (value, _) in values_by_popularity.iter() {
                lengths.push_typed::<u32>(value.len() as u32);
            }

            let prev_pos = target.len();
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
                target.extend_from_slice(&lengths);
            }
            metadata.lengths_encoded_size = target.len() - prev_pos;
        }

        for (value, _) in values_by_popularity.drain(..) {
            target.write_values(value);
        }

        metadata.finalize(target);

        Ok(target.len() - initial_size)
    }

    fn decode(
        &self,
        buffer: &[u8],
        presence: Presence,
        type_desc: BasicTypeDescriptor,
        params: Option<&EncodingParameters>,
        context: &EncodingContext,
    ) -> amudai_common::Result<ValueSequence> {
        let (metadata, buffer) = DictionaryMetadata::read_from(buffer)?;

        let mut codes = context.buffers.get_buffer();
        context.numeric_encoders.get::<u32>().decode(
            &buffer[..metadata.codes_encoded_size],
            presence.len(),
            None,
            &mut codes,
            context,
        )?;
        let codes = codes.typed_data::<u32>();

        if type_desc.fixed_size > 0 {
            let value_size = type_desc.fixed_size as usize;
            let mut seq_builder = FixedSizeBinarySequenceBuilder::new(type_desc);
            for &code in codes {
                if code == 0 {
                    seq_builder.add_null();
                } else {
                    let value_pos = code as usize - 1;
                    let value_offset = value_pos * value_size;
                    seq_builder.add_value(&buffer[value_offset..value_offset + value_size]);
                }
            }
            return Ok(seq_builder.build());
        }

        let lengths_pos = metadata
            .codes_encoded_size
            .next_multiple_of(ALIGNMENT_BYTES);
        let buffer = &buffer[lengths_pos..];

        let mut lengths = context.buffers.get_buffer();
        if metadata.lengths_cascading {
            context.numeric_encoders.get::<u32>().decode(
                &buffer[..metadata.lengths_encoded_size],
                metadata.unique_values_count,
                params,
                &mut lengths,
                context,
            )?;
        } else {
            lengths.extend_from_slice(&buffer[..metadata.lengths_encoded_size]);
        }
        let buffer = &buffer[metadata.lengths_encoded_size..];
        let lengths = lengths.typed_data::<u32>();

        let mut offsets = context.buffers.get_buffer();
        offsets.reserve(lengths.len() * std::mem::size_of::<usize>());
        let mut offset = 0;
        for &len in lengths {
            offsets.push_typed::<usize>(offset);
            offset += len as usize;
        }
        let offsets = offsets.typed_data::<usize>();

        let mut seq_builder = BinarySequenceBuilder::new(type_desc);
        for &code in codes {
            if code == 0 {
                seq_builder.add_null();
            } else {
                let value_pos = code as usize - 1;
                let value_offset = offsets[value_pos];
                let value_size = lengths[value_pos] as usize;
                seq_builder.add_value(&buffer[value_offset..value_offset + value_size]);
            }
        }
        Ok(seq_builder.build())
    }

    fn inspect(
        &self,
        buffer: &[u8],
        context: &EncodingContext,
    ) -> amudai_common::Result<EncodingPlan> {
        let (metadata, buffer) = DictionaryMetadata::read_from(buffer)?;
        let codes_plan = Some(
            context
                .numeric_encoders
                .get::<u32>()
                .inspect(&buffer[..metadata.codes_encoded_size], context)?,
        );

        let lengths_pos = metadata
            .codes_encoded_size
            .next_multiple_of(ALIGNMENT_BYTES);
        let buffer = &buffer[lengths_pos..];

        let lenghts_plan = if metadata.lengths_cascading {
            Some(
                context
                    .numeric_encoders
                    .get::<u32>()
                    .inspect(&buffer[..metadata.lengths_encoded_size], context)?,
            )
        } else {
            None
        };

        Ok(EncodingPlan {
            encoding: self.kind(),
            parameters: Default::default(),
            cascading_encodings: vec![codes_plan, lenghts_plan],
        })
    }
}

struct DictionaryMetadata {
    start_offset: usize,
    /// Encoded codes section size in bytes.
    pub codes_encoded_size: usize,
    /// Encoded values' lengths section size in bytes.
    pub lengths_encoded_size: usize,
    /// Number of unique values in the dictionary.
    pub unique_values_count: usize,
    /// Whether lengths section is encoded using cascading encoding.
    pub lengths_cascading: bool,
}

impl AlignedEncMetadata for DictionaryMetadata {
    fn own_size() -> usize {
        13
    }

    fn initialize(target: &mut AlignedByteVec) -> Self {
        let start_offset = target.len();
        target.resize(start_offset + Self::size(), 0);
        Self {
            start_offset,
            codes_encoded_size: 0,
            lengths_encoded_size: 0,
            unique_values_count: 0,
            lengths_cascading: false,
        }
    }

    fn finalize(self, target: &mut AlignedByteVec) {
        target.write_value_at::<u32>(self.start_offset, self.codes_encoded_size as u32);
        target.write_value_at::<u32>(self.start_offset + 4, self.lengths_encoded_size as u32);
        target.write_value_at::<u32>(self.start_offset + 8, self.unique_values_count as u32);
        target.write_value_at::<u8>(
            self.start_offset + 12,
            if self.lengths_cascading { 1 } else { 0 },
        );
    }

    fn read_from(buffer: &[u8]) -> amudai_common::Result<(Self, &[u8])> {
        if buffer.len() < Self::size() {
            return Err(Error::invalid_format("Encoded buffer size is too small"));
        }
        Ok((
            Self {
                codes_encoded_size: buffer.read_value::<u32>(0) as usize,
                lengths_encoded_size: buffer.read_value::<u32>(4) as usize,
                unique_values_count: buffer.read_value::<u32>(8) as usize,
                lengths_cascading: buffer.read_value::<u8>(12) != 0,
                start_offset: 0,
            },
            &buffer[Self::size()..],
        ))
    }
}

fn generate_codes<V>(
    values: &BinaryValuesSequence,
    null_mask: &NullMask,
    values_by_popularity: &[(V, u32)],
    codes: &mut AlignedByteVec,
) where
    V: AsRef<[u8]> + Eq + Hash,
{
    codes.reserve(values.len() * std::mem::size_of::<u32>());
    let mut values_codes = HashMap::with_capacity(values_by_popularity.len());
    for (code, (value, _)) in values_by_popularity.iter().enumerate() {
        values_codes.insert(value.as_ref(), code as u32 + 1);
    }
    for i in 0..values.len() {
        if null_mask.is_null(i) {
            // 0 is reserved for the null value.
            codes.push_typed::<u32>(0);
        } else {
            let value = values.get(i);
            let code = *values_codes
                .get(value)
                .expect("Value must present in the list of unique values");
            codes.push_typed::<u32>(code);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{borrow::Cow, vec};

    #[test]
    pub fn test_round_trip() {
        let context = EncodingContext::new();
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
        let outcome = context
            .binary_encoders
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
                        cascading_encodings: vec![],
                    }),
                    None
                ],
            },
            plan
        );

        let mut encoded = AlignedByteVec::new();
        let encoded_size2 = context
            .binary_encoders
            .encode(&sequence, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();
        assert_eq!(encoded_size1, encoded_size2);

        // Validate that inspect() returns the same encoding plan as used for encoding
        let inspect_plan = context.binary_encoders.inspect(&encoded, &context).unwrap();
        assert_eq!(plan, inspect_plan);

        let decoded = context
            .binary_encoders
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
