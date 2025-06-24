use super::{
    AnalysisOutcome, EncodingContext, EncodingKind, NumericEncoding,
    stats::{NumericStats, NumericStatsCollectorFlags},
    value::{NumericValue, ValueReader, ValueWriter},
};
use crate::encodings::{EncodingConfig, EncodingParameters, EncodingPlan, NullMask};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;

/// Encoding for sequences with a single numeric value.
pub struct SingleValueEncoding<T>(std::marker::PhantomData<T>);

impl<T> SingleValueEncoding<T>
where
    T: NumericValue,
{
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> NumericEncoding<T> for SingleValueEncoding<T>
where
    T: NumericValue,
{
    fn kind(&self) -> EncodingKind {
        EncodingKind::SingleValue
    }

    fn is_suitable(&self, _config: &EncodingConfig, stats: &NumericStats<T>) -> bool {
        stats.is_single_value()
    }

    fn stats_collector_flags(&self) -> NumericStatsCollectorFlags {
        NumericStatsCollectorFlags::UNIQUE_VALUES
    }

    fn analyze(
        &self,
        _values: &[T],
        _null_mask: &NullMask,
        _config: &EncodingConfig,
        stats: &NumericStats<T>,
        _context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>> {
        let pos = 2 + T::SIZE;
        Ok(Some(AnalysisOutcome {
            encoding: self.kind(),
            cascading_outcomes: vec![],
            encoded_size: pos,
            compression_ratio: stats.original_size as f64 / pos as f64,
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
        // Quickly validate that the values are all the same, and fallback
        // to RLE encoding in case they are not.
        if values.iter().skip(1).any(|v| *v != values[0]) {
            let rle_plan = EncodingPlan {
                encoding: EncodingKind::RunLength,
                parameters: Default::default(),
                cascading_encodings: vec![
                    // No cascading encoding for values as the assumption is that there
                    // aren't many.
                    None,
                    // Cascading encoding for lengths.
                    Some(EncodingPlan {
                        encoding: EncodingKind::FLBitPack,
                        parameters: Default::default(),
                        cascading_encodings: vec![],
                    }),
                ],
            };
            return context
                .numeric_encoders
                .get::<T>()
                .encode(values, null_mask, target, &rle_plan, context);
        }

        let initial_size = target.len();
        target.write_value::<u16>(self.kind() as u16);
        target.write_value::<T>(values[0]);
        Ok(target.len() - initial_size)
    }

    fn decode(
        &self,
        buffer: &[u8],
        value_count: usize,
        _params: Option<&EncodingParameters>,
        target: &mut AlignedByteVec,
        _context: &EncodingContext,
    ) -> amudai_common::Result<()> {
        if buffer.len() < T::SIZE {
            return Err(Error::invalid_format("Encoded buffer size is too small"));
        }
        let value = buffer.read_value::<T>(0);
        target.resize_typed(value_count, value);
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

#[cfg(test)]
mod tests {
    use crate::encodings::{
        EncodingConfig, EncodingKind, EncodingPlan, NullMask,
        numeric::{EncodingContext, NumericEncoding, single_value::SingleValueEncoding},
    };
    use amudai_bytes::buffer::AlignedByteVec;

    #[test]
    fn test_round_trip() {
        let config = EncodingConfig::default();
        let context = EncodingContext::new();
        let data: Vec<i64> = (0..65536).map(|_| 1234).collect();
        let outcome = context
            .numeric_encoders
            .get::<i64>()
            .analyze(&data, &NullMask::None, &config, &context)
            .unwrap();
        assert!(outcome.is_some());
        let outcome = outcome.unwrap();
        assert_eq!(EncodingKind::SingleValue, outcome.encoding);
        assert_eq!(0, outcome.cascading_outcomes.len());
        let encoded_size1 = outcome.encoded_size;
        let plan = outcome.into_plan();

        assert_eq!(
            EncodingPlan {
                encoding: EncodingKind::SingleValue,
                parameters: Default::default(),
                cascading_encodings: vec![]
            },
            plan,
        );

        let mut encoded = AlignedByteVec::new();
        context
            .numeric_encoders
            .get::<i64>()
            .encode(&data, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();
        assert_eq!(encoded_size1, encoded.len());

        let mut decoded = AlignedByteVec::new();
        context
            .numeric_encoders
            .get::<i64>()
            .decode(&encoded, data.len(), None, &mut decoded, &context)
            .unwrap();
        for (a, b) in data.iter().zip(decoded.typed_data()) {
            assert_eq!(a, b);
        }
    }

    #[test]
    fn test_fallback_to_rle() {
        let context = EncodingContext::new();
        let data: Vec<i64> = (0..65536)
            .map(|i| if i % 10000 != 0 { 1234 } else { 2345 })
            .collect();
        let single_encoding = SingleValueEncoding::<i64>::new();
        let plan = EncodingPlan {
            encoding: EncodingKind::SingleValue,
            parameters: Default::default(),
            cascading_encodings: vec![],
        };
        let mut encoded = AlignedByteVec::new();
        single_encoding
            .encode(&data, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();

        let mut decoded = AlignedByteVec::new();
        context
            .numeric_encoders
            .get::<i64>()
            .decode(&encoded, data.len(), None, &mut decoded, &context)
            .unwrap();
        for (a, b) in data.iter().zip(decoded.typed_data()) {
            assert_eq!(a, b);
        }
    }
}
