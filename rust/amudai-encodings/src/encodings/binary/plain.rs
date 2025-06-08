use super::{
    stats::{BinaryStats, BinaryStatsCollectorFlags},
    AnalysisOutcome, BinaryValuesSequence, EncodingContext, EncodingKind, StringEncoding,
};
use crate::encodings::{
    numeric::value::{ValueReader, ValueWriter},
    EncodingConfig, EncodingParameters, EncodingPlan, NullMask,
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;
use amudai_format::schema::BasicTypeDescriptor;
use amudai_sequence::{
    offsets::Offsets, presence::Presence, sequence::ValueSequence, values::Values,
};

pub struct PlainEncoding;

impl PlainEncoding {
    pub fn new() -> Self {
        Self
    }
}

impl StringEncoding for PlainEncoding {
    fn kind(&self) -> EncodingKind {
        EncodingKind::Plain
    }

    fn is_suitable(&self, _config: &EncodingConfig, _stats: &BinaryStats) -> bool {
        // The encoding is called explicitly.
        false
    }

    fn stats_collector_flags(&self) -> BinaryStatsCollectorFlags {
        BinaryStatsCollectorFlags::empty()
    }

    fn analyze(
        &self,
        _values: &BinaryValuesSequence,
        _null_mask: &NullMask,
        _config: &EncodingConfig,
        _stats: &BinaryStats,
        _context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>> {
        unimplemented!(
            "PlainEncoding::analyze is not implemented as this encoding is used explicitly."
        )
    }

    fn encode(
        &self,
        values: &BinaryValuesSequence,
        _null_mask: &NullMask,
        target: &mut AlignedByteVec,
        _plan: &EncodingPlan,
        _context: &EncodingContext,
    ) -> amudai_common::Result<usize> {
        let initial_size = target.len();
        target.write_value::<u16>(self.kind() as u16);

        let offset_width_pos = target.len();
        target.write_value::<u8>(0u8);

        if values.fixed_value_size().is_some() {
            // Zero offset width indicates fixed size values.
        } else if let Some(offsets) = values.offsets() {
            target.write_values(&offsets);
            target.write_value_at::<u8>(offset_width_pos, 8u8);
        } else {
            return Err(Error::invalid_arg(
                "values",
                "Unsupported sequence: either fixed size or offsets must be present",
            ));
        }
        target.write_values(values.values());

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
        let offset_width = buffer.read_value::<u8>(0) as usize;
        let buffer = &buffer[1..];

        let decoded = if offset_width == 0 {
            // Zero offset width indicates fixed size values.
            let mut values = Values::new();
            values.extend_from_slice(&buffer);

            ValueSequence {
                type_desc,
                values,
                offsets: None,
                presence,
            }
        } else if offset_width == 8 {
            let data_offset = (presence.len() + 1) * offset_width;

            let mut values = Values::new();
            values.extend_from_slice(&buffer[data_offset..]);

            let mut offsets = AlignedByteVec::new();
            offsets.extend_from_slice(&buffer[..data_offset]);
            let offsets = Offsets::from_values(Values::from_vec(offsets));

            ValueSequence {
                type_desc,
                values,
                offsets: Some(offsets),
                presence,
            }
        } else {
            return Err(Error::invalid_format("Unsupported offset width"));
        };

        Ok(decoded)
    }
}

#[cfg(test)]
mod tests {
    use crate::encodings::{
        binary::{BinaryValuesSequence, EncodingContext},
        EncodingKind, EncodingPlan, NullMask,
    };
    use amudai_bytes::buffer::AlignedByteVec;
    use amudai_format::schema::{BasicType, BasicTypeDescriptor};
    use amudai_sequence::presence::Presence;
    use std::borrow::Cow;

    #[test]
    fn test_round_trip() {
        let context = EncodingContext::new();
        let values = (0..65536)
            .map(|_| fastrand::i64(0..10000).to_string().as_bytes().to_vec())
            .collect::<Vec<_>>();
        let array = arrow_array::array::LargeBinaryArray::from_iter_values(values.iter());
        let sequence = BinaryValuesSequence::ArrowLargeBinaryArray(Cow::Owned(array));

        let mut encoded = AlignedByteVec::new();
        let plan = EncodingPlan {
            encoding: EncodingKind::Plain,
            parameters: Default::default(),
            cascading_encodings: vec![],
        };
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

    #[test]
    pub fn test_round_trip_fixed_size() {
        let context = EncodingContext::new();

        let unique_values = (0..5)
            .map(|_| fastrand::i128(-1000..1000).to_le_bytes())
            .collect::<Vec<_>>();
        let values = (0..10000)
            .map(|_| {
                fastrand::choice(unique_values.iter())
                    .map(|&v| v.to_vec())
                    .unwrap()
            })
            .collect::<Vec<_>>();

        let array = arrow_array::array::FixedSizeBinaryArray::try_from_iter(values.iter()).unwrap();
        let sequence = BinaryValuesSequence::ArrowFixedSizeBinaryArray(Cow::Owned(array));
        let mut encoded = AlignedByteVec::new();
        let plan = EncodingPlan {
            encoding: EncodingKind::Plain,
            parameters: Default::default(),
            cascading_encodings: vec![],
        };
        context
            .binary_encoders
            .encode(&sequence, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();

        let decoded = context
            .binary_encoders
            .decode(
                &encoded,
                Presence::Trivial(values.len()),
                BasicTypeDescriptor {
                    basic_type: BasicType::FixedSizeBinary,
                    signed: false,
                    fixed_size: 16,
                },
                &Default::default(),
                &context,
            )
            .unwrap();

        assert_eq!(values.len(), decoded.presence.len());
        for (a, b) in values.iter().zip(decoded.fixed_binary_values().unwrap()) {
            assert_eq!(a.as_slice(), b);
        }
    }
}
