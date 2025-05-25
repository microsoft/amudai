use super::{
    stats::{BinaryStats, BinaryStatsCollectorFlags},
    AnalysisOutcome, BinaryValuesSequence, EncodingKind, StringEncoding,
};
use crate::encodings::{
    numeric::value::ValueWriter, EncodingConfig, EncodingContext, EncodingParameters, EncodingPlan,
    NullMask,
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_format::schema::BasicTypeDescriptor;
use amudai_sequence::{
    offsets::Offsets, presence::Presence, sequence::ValueSequence, values::Values,
};

/// Encoding for sequences with a single value.
pub struct SingleValueEncoding;

impl SingleValueEncoding {
    pub fn new() -> Self {
        Self
    }
}

impl StringEncoding for SingleValueEncoding {
    fn kind(&self) -> EncodingKind {
        EncodingKind::SingleValue
    }

    fn is_suitable(&self, _config: &EncodingConfig, stats: &BinaryStats) -> bool {
        stats.is_single_value()
    }

    fn stats_collector_flags(&self) -> BinaryStatsCollectorFlags {
        BinaryStatsCollectorFlags::UNIQUE_VALUES
    }

    fn analyze(
        &self,
        values: &BinaryValuesSequence,
        _null_mask: &NullMask,
        _config: &EncodingConfig,
        stats: &BinaryStats,
        _context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>> {
        let pos = 2 + values.get(0).len();
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
        values: &BinaryValuesSequence,
        null_mask: &NullMask,
        target: &mut AlignedByteVec,
        _plan: &EncodingPlan,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize> {
        // Quickly validate that the values are all the same
        // and fallback to dictionary encoding in case they are not.
        let first = values.get(0);
        if values.iter().skip(1).any(|v| v != first) {
            let dictionary_plan = EncodingPlan {
                encoding: EncodingKind::Dictionary,
                parameters: Default::default(),
                cascading_encodings: vec![
                    Some(EncodingPlan {
                        encoding: EncodingKind::FLBitPack,
                        parameters: Default::default(),
                        cascading_encodings: vec![],
                    }),
                    None,
                ],
            };
            return context.binary_encoders.encode(
                values,
                null_mask,
                target,
                &dictionary_plan,
                context,
            );
        }

        let initial_size = target.len();
        target.write_value::<u16>(self.kind() as u16);
        target.write_values(values.get(0));
        Ok(target.len() - initial_size)
    }

    fn decode(
        &self,
        buffer: &[u8],
        presence: Presence,
        type_desc: BasicTypeDescriptor,
        _params: &EncodingParameters,
        _context: &EncodingContext,
    ) -> amudai_common::Result<ValueSequence> {
        let value_len = buffer.len();
        let mut offsets = Offsets::new();
        let mut values = Values::new();
        let mut offset = 0;
        for i in 0..presence.len() {
            if !presence.is_null(i) {
                offset += value_len;
                values.extend_from_slice(buffer);
            }
            offsets.push_offset(offset as u64);
        }
        Ok(ValueSequence {
            values,
            offsets: Some(offsets),
            presence,
            type_desc,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::encodings::{
        binary::BinaryValuesSequence, EncodingConfig, EncodingContext, EncodingKind, EncodingPlan,
        NullMask,
    };
    use amudai_bytes::buffer::AlignedByteVec;
    use amudai_sequence::presence::Presence;
    use std::borrow::Cow;

    #[test]
    fn test_round_trip() {
        let config = EncodingConfig::default();
        let context = EncodingContext::new();
        let values = (0..65536).map(|_| b"abcdef".to_vec()).collect::<Vec<_>>();
        let array = arrow_array::array::LargeBinaryArray::from_iter_values(values.iter());
        let sequence = BinaryValuesSequence::ArrowLargeBinaryArray(Cow::Owned(array));
        let outcome = context
            .binary_encoders
            .analyze(&sequence, &NullMask::None, &config, &context)
            .unwrap();
        assert!(outcome.is_some());
        let outcome = outcome.unwrap();
        assert_eq!(EncodingKind::SingleValue, outcome.encoding);
        assert_eq!(0, outcome.cascading_outcomes.len());
        let encoded_size1 = outcome.encoded_size;
        let plan = outcome.into_plan();

        let mut encoded = AlignedByteVec::new();
        let encoded_size2 = context
            .binary_encoders
            .encode(&sequence, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();
        assert_eq!(encoded_size1, encoded_size2);

        let decoded = context
            .binary_encoders
            .decode(
                &encoded,
                Presence::Trivial(values.len()),
                Default::default(),
                &Default::default(),
                &context,
            )
            .unwrap();

        assert_eq!(values.len(), decoded.presence.len());
        for (a, b) in values.iter().zip(decoded.binary_values().unwrap()) {
            assert_eq!(a.as_slice(), b);
        }
    }

    #[test]
    fn test_fallback_to_dictionary_encoding() {
        let context = EncodingContext::new();
        let values = (0..65536)
            .map(|_| fastrand::choice(&[b"abcdef", b"123456"]).unwrap().to_vec())
            .collect::<Vec<_>>();
        let array = arrow_array::array::LargeBinaryArray::from_iter_values(values.iter());
        let sequence = BinaryValuesSequence::ArrowLargeBinaryArray(Cow::Owned(array));
        let plan = EncodingPlan {
            encoding: EncodingKind::SingleValue,
            parameters: Default::default(),
            cascading_encodings: vec![],
        };
        let mut encoded = AlignedByteVec::new();
        context
            .binary_encoders
            .encode(&sequence, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();

        let decoded = context
            .binary_encoders
            .decode(
                &encoded,
                Presence::Trivial(values.len()),
                Default::default(),
                &Default::default(),
                &context,
            )
            .unwrap();

        assert_eq!(values.len(), decoded.presence.len());
        for (a, b) in values.iter().zip(decoded.binary_values().unwrap()) {
            assert_eq!(a.as_slice(), b);
        }
    }
}
