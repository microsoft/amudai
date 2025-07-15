use super::{
    AnalysisOutcome, BinaryValuesSequence, EncodingKind, StringEncoding,
    offsets::{analyze_offsets, decode_offsets_and_make_sequence, encode_offsets},
    stats::{BinaryStats, BinaryStatsCollectorFlags},
};
use crate::encodings::{
    ALIGNMENT_BYTES, AlignedEncMetadata, EncodingConfig, EncodingContext, EncodingParameters,
    EncodingPlan, NullMask,
    binary::offsets::inspect_offsets,
    numeric::{
        generic::GenericEncoder,
        value::{ValueReader, ValueWriter},
    },
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;
use amudai_format::schema::BasicTypeDescriptor;
use amudai_sequence::{presence::Presence, sequence::ValueSequence};

/// Generic-purpose encoding for binary values.
pub struct GenericEncoding<E> {
    encoder: E,
}

impl<E> GenericEncoding<E> {
    pub fn new(encoder: E) -> Self {
        Self { encoder }
    }
}

impl<E> StringEncoding for GenericEncoding<E>
where
    E: GenericEncoder,
{
    fn kind(&self) -> EncodingKind {
        self.encoder.encoding_kind()
    }

    fn is_suitable(&self, _config: &EncodingConfig, _stats: &BinaryStats) -> bool {
        true
    }

    fn stats_collector_flags(&self) -> BinaryStatsCollectorFlags {
        BinaryStatsCollectorFlags::empty()
    }

    fn analyze(
        &self,
        values: &BinaryValuesSequence,
        _null_mask: &NullMask,
        config: &EncodingConfig,
        stats: &BinaryStats,
        context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>> {
        let mut encoded_size = 2 + GenericMetadata::size();

        let mut target = context.buffers.get_buffer();
        self.encoder.encode(values.values(), &mut target, None)?;
        encoded_size += target.len();
        encoded_size = encoded_size.next_multiple_of(ALIGNMENT_BYTES);

        let (offsets_outcome, offsets_size) = analyze_offsets(values, config, context)?;
        encoded_size += offsets_size;

        Ok(Some(AnalysisOutcome {
            encoding: self.kind(),
            cascading_outcomes: vec![offsets_outcome],
            encoded_size,
            compression_ratio: stats.original_size as f64 / encoded_size as f64,
            parameters: Default::default(),
        }))
    }

    fn encode(
        &self,
        values: &BinaryValuesSequence,
        _null_mask: &NullMask,
        target: &mut AlignedByteVec,
        plan: &EncodingPlan,
        context: &EncodingContext,
    ) -> amudai_common::Result<usize> {
        let initial_size = target.len();
        target.write_value::<u16>(self.kind() as u16);

        let mut metadata = GenericMetadata::initialize(target);

        let prev_pos = target.len();
        self.encoder
            .encode(values.values(), &mut *target, plan.parameters.as_ref())?;
        metadata.values_size = target.len() - prev_pos;

        let alignment = target.len().next_multiple_of(ALIGNMENT_BYTES);
        target.resize(alignment, 0);

        let (offset_width, offsets_cascading, _) = encode_offsets(
            values,
            target,
            plan.cascading_encodings[0].as_ref(),
            context,
        )?;

        metadata.offset_width = offset_width;
        metadata.offsets_cascading = offsets_cascading;
        metadata.finalize(target);

        Ok(target.len() - initial_size)
    }

    fn decode(
        &self,
        buffer: &[u8],
        presence: Presence,
        type_desc: BasicTypeDescriptor,
        _params: Option<&EncodingParameters>,
        context: &EncodingContext,
    ) -> amudai_common::Result<ValueSequence> {
        let (metadata, buffer) = GenericMetadata::read_from(buffer)?;

        let mut values = AlignedByteVec::new();
        self.encoder
            .decode(&buffer[..metadata.values_size], &mut values)?;

        let offsets_pos = metadata.values_size.next_multiple_of(ALIGNMENT_BYTES);
        let buffer = &buffer[offsets_pos..];
        decode_offsets_and_make_sequence(
            buffer,
            values,
            presence,
            type_desc,
            metadata.offset_width,
            metadata.offsets_cascading,
            context,
        )
    }

    fn inspect(
        &self,
        buffer: &[u8],
        context: &EncodingContext,
    ) -> amudai_common::Result<EncodingPlan> {
        let (metadata, buffer) = GenericMetadata::read_from(buffer)?;
        let offsets_pos = metadata.values_size.next_multiple_of(ALIGNMENT_BYTES);
        let offsets_plan = inspect_offsets(
            &buffer[offsets_pos..],
            metadata.offset_width,
            metadata.offsets_cascading,
            context,
        )?;
        Ok(EncodingPlan {
            encoding: self.kind(),
            parameters: None,
            cascading_encodings: vec![offsets_plan],
        })
    }
}

struct GenericMetadata {
    start_offset: usize,
    /// Encoded values size.
    pub values_size: usize,
    /// Values offsets width.
    pub offset_width: u8,
    /// Whether the offsets encoding is cascading.
    pub offsets_cascading: bool,
}

impl AlignedEncMetadata for GenericMetadata {
    fn own_size() -> usize {
        6
    }

    fn initialize(target: &mut AlignedByteVec) -> Self {
        let start_offset = target.len();
        target.resize(start_offset + Self::size(), 0);
        Self {
            start_offset,
            values_size: 0,
            offset_width: 0,
            offsets_cascading: false,
        }
    }

    fn finalize(self, target: &mut AlignedByteVec) {
        target.write_value_at::<u32>(self.start_offset, self.values_size as u32);
        target.write_value_at::<u8>(self.start_offset + 4, self.offset_width);
        target.write_value_at::<u8>(
            self.start_offset + 5,
            if self.offsets_cascading { 1 } else { 0 },
        );
    }

    fn read_from(buffer: &[u8]) -> amudai_common::Result<(Self, &[u8])> {
        if buffer.len() < Self::size() {
            return Err(Error::invalid_format("Encoded buffer size is too small"));
        }
        Ok((
            Self {
                start_offset: 0,
                values_size: buffer.read_value::<u32>(0) as usize,
                offset_width: buffer.read_value::<u8>(4),
                offsets_cascading: buffer.read_value::<u8>(5) != 0,
            },
            &buffer[Self::size()..],
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::encodings::{
        EncodingConfig, EncodingContext, EncodingKind, EncodingPlan, NullMask,
        binary::BinaryValuesSequence,
    };
    use amudai_bytes::buffer::AlignedByteVec;
    use amudai_format::schema::{BasicType, BasicTypeDescriptor};
    use amudai_sequence::presence::Presence;
    use std::borrow::Cow;

    #[test]
    fn test_round_trip() {
        for encoding in [EncodingKind::Zstd, EncodingKind::Lz4] {
            let context = EncodingContext::new();
            let config = EncodingConfig::default().with_allowed_encodings(&[encoding]);
            let values = (0..65536)
                .map(|_| fastrand::i64(0..10).to_string().as_bytes().to_vec())
                .collect::<Vec<_>>();
            let array = arrow_array::array::BinaryArray::from_iter_values(values.iter());
            let sequence = BinaryValuesSequence::ArrowBinaryArray(Cow::Owned(array));
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
                    encoding,
                    parameters: None,
                    cascading_encodings: vec![Some(EncodingPlan {
                        encoding: EncodingKind::Delta,
                        parameters: Default::default(),
                        cascading_encodings: vec![Some(EncodingPlan {
                            encoding: EncodingKind::SingleValue,
                            parameters: Default::default(),
                            cascading_encodings: vec![]
                        })]
                    })]
                },
                plan
            );

            let mut encoded = AlignedByteVec::new();
            let encoded_size = context
                .binary_encoders
                .encode(&sequence, &NullMask::None, &mut encoded, &plan, &context)
                .unwrap();
            assert_eq!(encoded_size1, encoded_size);

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

    #[test]
    pub fn test_round_trip_fixed_size() {
        for encoding in [EncodingKind::Zstd, EncodingKind::Lz4] {
            let context = EncodingContext::new();
            let config = EncodingConfig::default().with_allowed_encodings(&[encoding]);

            let unique_values = (0..5)
                .map(|_| fastrand::i128(100..150).to_le_bytes())
                .collect::<Vec<_>>();
            let values = (0..10000)
                .map(|_| {
                    fastrand::choice(unique_values.iter())
                        .map(|&v| v.to_vec())
                        .unwrap()
                })
                .collect::<Vec<_>>();

            let array =
                arrow_array::array::FixedSizeBinaryArray::try_from_iter(values.iter()).unwrap();
            let sequence = BinaryValuesSequence::ArrowFixedSizeBinaryArray(Cow::Owned(array));

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
                    encoding,
                    parameters: Default::default(),
                    cascading_encodings: vec![None]
                },
                plan
            );

            let mut encoded = AlignedByteVec::new();
            let encoded_size = context
                .binary_encoders
                .encode(&sequence, &NullMask::None, &mut encoded, &plan, &context)
                .unwrap();
            assert_eq!(encoded_size1, encoded_size);

            // Validate that inspect() returns the same encoding plan as used for encoding
            let inspect_plan = context.binary_encoders.inspect(&encoded, &context).unwrap();
            assert_eq!(plan, inspect_plan);

            let decoded = context
                .binary_encoders
                .decode(
                    &encoded,
                    Presence::Trivial(values.len()),
                    BasicTypeDescriptor {
                        basic_type: BasicType::FixedSizeBinary,
                        signed: false,
                        fixed_size: 16,
                        extended_type: Default::default(),
                    },
                    None,
                    &context,
                )
                .unwrap();

            assert_eq!(values.len(), decoded.presence.len());
            for (a, b) in values.iter().zip(decoded.fixed_binary_values().unwrap()) {
                assert_eq!(a.as_slice(), b);
            }
        }
    }
}
