use super::{
    AnalysisOutcome, EncodingKind, NumericEncoding,
    stats::{NumericStats, NumericStatsCollectorFlags},
    value::{NumericValue, ValueReader, ValueWriter},
};
use crate::encodings::{
    AlignedEncMetadata, EncodingConfig, EncodingContext, EncodingParameters, EncodingPlan, NullMask,
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;
use std::collections::HashMap;

/// Dictionary encoding for numeric values that embeds dictionary into each encoded block.
pub struct BlockDictionaryEncoding<T>(std::marker::PhantomData<T>);

impl<T> BlockDictionaryEncoding<T>
where
    T: NumericValue,
{
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }

    fn generate_codes(
        &self,
        values: &[T],
        null_mask: &NullMask,
        values_by_popularity: &[(T, u32)],
        codes: &mut AlignedByteVec,
    ) {
        codes.reserve(values.len() * std::mem::size_of::<u32>());
        let mut values_codes = HashMap::with_capacity(values_by_popularity.len());
        for (code, &(value, _)) in values_by_popularity.iter().enumerate() {
            values_codes.insert(value, code as u32 + 1);
        }
        for (i, value) in values.iter().enumerate() {
            if null_mask.is_null(i) {
                // 0 is reserved for the null value.
                codes.push_typed::<u32>(0);
            } else {
                let code = *values_codes
                    .get(value)
                    .expect("Value must present in the list of unique values");
                codes.push_typed::<u32>(code);
            }
        }
    }
}

impl<T> NumericEncoding<T> for BlockDictionaryEncoding<T>
where
    T: NumericValue,
{
    fn kind(&self) -> EncodingKind {
        EncodingKind::BlockDictionary
    }

    fn is_suitable(&self, config: &EncodingConfig, stats: &NumericStats<T>) -> bool {
        if config.max_cascading_levels < 2 {
            // For efficient compression, diffs stream must be encoded with
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

    fn stats_collector_flags(&self) -> NumericStatsCollectorFlags {
        NumericStatsCollectorFlags::UNIQUE_VALUES
    }

    fn analyze(
        &self,
        values: &[T],
        null_mask: &NullMask,
        config: &EncodingConfig,
        stats: &NumericStats<T>,
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
        for (&value, &count) in unique_values {
            values_by_popularity.push((value, count));
        }
        values_by_popularity.sort_unstable_by_key(|(_, count)| std::cmp::Reverse(*count));

        let mut cascading_outcomes = vec![None, None];
        {
            let mut codes = context.buffers.get_buffer();
            self.generate_codes(values, null_mask, &values_by_popularity, &mut codes);

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

        let mut values_dict = context.buffers.get_buffer();
        values_dict.reserve(values_by_popularity.len() * T::SIZE);
        for &(value, _) in values_by_popularity.iter() {
            values_dict.push_typed::<T>(value);
        }
        if let Some(values_outcome) = context.numeric_encoders.get::<T>().analyze(
            values_dict.typed_data(),
            &NullMask::None,
            &config.make_cascading_config(),
            context,
        )? {
            encoded_size += values_outcome.encoded_size;
            cascading_outcomes[1] = Some(values_outcome);
        } else {
            encoded_size += values_dict.len() * T::SIZE;
        }

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
        values: &[T],
        null_mask: &NullMask,
        target: &mut AlignedByteVec,
        plan: &EncodingPlan,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize> {
        let initial_size = target.len();
        target.write_value::<u16>(self.kind() as u16);

        let mut metadata = DictionaryMetadata::initialize(target);

        let mut unique_values = HashMap::new();
        for &value in values {
            *unique_values.entry(value).or_insert(0) += 1;
        }
        metadata.unique_values_count = unique_values.len();

        // Sort unique values by popularity to allow better compression of dictionary codes
        // with BitPacking encoding.
        let mut values_by_popularity = unique_values.into_iter().collect::<Vec<_>>();
        values_by_popularity.sort_unstable_by_key(|(_, count)| std::cmp::Reverse(*count));

        {
            let mut codes = context.buffers.get_buffer();
            self.generate_codes(values, null_mask, &values_by_popularity, &mut codes);

            let codes_plan = plan.cascading_encodings[0]
                .as_ref()
                .expect("Codes encoding plan must be provided");
            metadata.codes_size = context.numeric_encoders.get::<u32>().encode(
                codes.typed_data(),
                &NullMask::None,
                target,
                codes_plan,
                context,
            )?;
        }

        let mut values_dict = context.buffers.get_buffer();
        values_dict.reserve(values_by_popularity.len() * T::SIZE);
        for &(value, _) in values_by_popularity.iter() {
            values_dict.push_typed::<T>(value);
        }
        if let Some(values_plan) = plan.cascading_encodings[1].as_ref() {
            context.numeric_encoders.get::<T>().encode(
                values_dict.typed_data(),
                &NullMask::None,
                target,
                values_plan,
                context,
            )?;
            metadata.values_cascading = true;
        } else {
            target.write_values(&values_dict);
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
        let (metadata, buffer) = DictionaryMetadata::read_from(buffer)?;

        let mut codes = context.buffers.get_buffer();
        context.numeric_encoders.get::<u32>().decode(
            &buffer[..metadata.codes_size],
            value_count,
            params,
            &mut codes,
            context,
        )?;
        let codes = codes.typed_data::<u32>();
        let values_buffer = &buffer[metadata.codes_size..];

        let mut values_dict = context.buffers.get_buffer();
        if metadata.values_cascading {
            context.numeric_encoders.get::<T>().decode(
                values_buffer,
                metadata.unique_values_count,
                params,
                &mut values_dict,
                context,
            )?;
        } else {
            let values_size = metadata.unique_values_count * T::SIZE;
            if values_size > values_buffer.len() {
                return Err(Error::invalid_format("Values buffer is too small"));
            }
            values_dict.extend_from_slice(&values_buffer[..values_size]);
        }
        let values_dict = values_dict.typed_data::<T>();

        for &code in codes {
            if code == 0 {
                target.push_typed::<T>(T::zero());
            } else {
                target.push_typed::<T>(values_dict[code as usize - 1]);
            }
        }
        Ok(())
    }

    fn inspect(
        &self,
        buffer: &[u8],
        context: &EncodingContext,
    ) -> amudai_common::Result<EncodingPlan> {
        let (metadata, buffer) = DictionaryMetadata::read_from(buffer)?;
        let codes_plan = context
            .numeric_encoders
            .get::<u32>()
            .inspect(&buffer[..metadata.codes_size], context)?;
        let values_buffer = &buffer[metadata.codes_size..];
        let values_plan = if metadata.values_cascading {
            Some(
                context
                    .numeric_encoders
                    .get::<T>()
                    .inspect(values_buffer, context)?,
            )
        } else {
            None
        };
        Ok(EncodingPlan {
            encoding: self.kind(),
            parameters: Default::default(),
            cascading_encodings: vec![Some(codes_plan), values_plan],
        })
    }
}

struct DictionaryMetadata {
    start_offset: usize,
    /// Encoded codes section size in bytes.
    pub codes_size: usize,
    /// Number of unique values in the dictionary.
    pub unique_values_count: usize,
    /// Whether values section is encoded using cascading encoding.
    pub values_cascading: bool,
}

impl AlignedEncMetadata for DictionaryMetadata {
    fn own_size() -> usize {
        9
    }

    fn initialize(target: &mut AlignedByteVec) -> Self {
        let start_offset = target.len();
        target.resize(start_offset + Self::size(), 0);
        Self {
            start_offset,
            codes_size: 0,
            unique_values_count: 0,
            values_cascading: false,
        }
    }

    fn finalize(self, target: &mut AlignedByteVec) {
        target.write_value_at::<u32>(self.start_offset, self.codes_size as u32);
        target.write_value_at::<u32>(self.start_offset + 4, self.unique_values_count as u32);
        target.write_value_at::<u8>(
            self.start_offset + 8,
            if self.values_cascading { 1 } else { 0 },
        );
    }

    fn read_from(buffer: &[u8]) -> amudai_common::Result<(Self, &[u8])> {
        if buffer.len() < Self::size() {
            return Err(Error::invalid_format("Encoded buffer size is too small"));
        }
        Ok((
            Self {
                start_offset: 0,
                codes_size: buffer.read_value::<u32>(0) as usize,
                unique_values_count: buffer.read_value::<u32>(4) as usize,
                values_cascading: buffer.read_value::<u8>(8) != 0,
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
        let config =
            EncodingConfig::default().with_allowed_encodings(&[EncodingKind::BlockDictionary]);
        let context = EncodingContext::new();
        let data: Vec<i64> = (0..10000).map(|_| fastrand::i64(1..100)).collect();
        let outcome = context
            .numeric_encoders
            .get::<i64>()
            .analyze(&data, &NullMask::None, &config, &context)
            .unwrap();
        assert!(outcome.is_some());
        let outcome = outcome.unwrap();
        assert_eq!(outcome.encoding, EncodingKind::BlockDictionary);
        let encoded_size1 = outcome.encoded_size;
        let plan = outcome.into_plan();

        let mut encoded = AlignedByteVec::new();
        let encoded_size2 = context
            .numeric_encoders
            .get::<i64>()
            .encode(&data, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();
        assert_eq!(encoded_size1, encoded_size2);

        // Inspect the encoding plan and validate that it matches the original plan.
        let inspected_plan = context
            .numeric_encoders
            .get::<i64>()
            .inspect(encoded.typed_data(), &context)
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
