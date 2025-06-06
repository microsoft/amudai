use crate::{
    block_decoder::BlockDecoder,
    block_encoder::BlockEncodingParameters,
    encodings::{self, numeric::value::FloatValue, EncodingContext, ALIGNMENT_BYTES},
    primitive_block_encoder::PrimitiveBlockEncoderMetadata,
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_format::schema::BasicTypeDescriptor;
use amudai_sequence::{presence::Presence, sequence::ValueSequence, values::Values};
use std::sync::Arc;

pub struct PrimitiveBlockDecoder {
    _parameters: BlockEncodingParameters,
    basic_type: BasicTypeDescriptor,
    context: Arc<EncodingContext>,
}

impl PrimitiveBlockDecoder {
    pub fn new(
        parameters: BlockEncodingParameters,
        basic_type: BasicTypeDescriptor,
        context: Arc<EncodingContext>,
    ) -> Self {
        Self {
            context,
            _parameters: parameters,
            basic_type,
        }
    }

    fn decode_values(
        encoded: &[u8],
        presence: Presence,
        type_desc: BasicTypeDescriptor,
        value_count: usize,
        context: &EncodingContext,
    ) -> amudai_common::Result<ValueSequence> {
        let mut values = AlignedByteVec::new();
        match type_desc.basic_type {
            amudai_format::schema::BasicType::Int8 => {
                if type_desc.signed {
                    context.numeric_encoders.get::<i8>().decode(
                        encoded,
                        value_count,
                        &Default::default(),
                        &mut values,
                        context,
                    )?;
                } else {
                    context.numeric_encoders.get::<u8>().decode(
                        encoded,
                        value_count,
                        &Default::default(),
                        &mut values,
                        context,
                    )?;
                }
            }
            amudai_format::schema::BasicType::Int16 => {
                if type_desc.signed {
                    context.numeric_encoders.get::<i16>().decode(
                        encoded,
                        value_count,
                        &Default::default(),
                        &mut values,
                        context,
                    )?;
                } else {
                    context.numeric_encoders.get::<u16>().decode(
                        encoded,
                        value_count,
                        &Default::default(),
                        &mut values,
                        context,
                    )?;
                }
            }
            amudai_format::schema::BasicType::Int32 => {
                if type_desc.signed {
                    context.numeric_encoders.get::<i32>().decode(
                        encoded,
                        value_count,
                        &Default::default(),
                        &mut values,
                        context,
                    )?;
                } else {
                    context.numeric_encoders.get::<u32>().decode(
                        encoded,
                        value_count,
                        &Default::default(),
                        &mut values,
                        context,
                    )?;
                }
            }
            amudai_format::schema::BasicType::Int64 => {
                if type_desc.signed {
                    context.numeric_encoders.get::<i64>().decode(
                        encoded,
                        value_count,
                        &Default::default(),
                        &mut values,
                        context,
                    )?;
                } else {
                    context.numeric_encoders.get::<u64>().decode(
                        encoded,
                        value_count,
                        &Default::default(),
                        &mut values,
                        context,
                    )?;
                }
            }
            amudai_format::schema::BasicType::Float32 => {
                context.numeric_encoders.get::<FloatValue<f32>>().decode(
                    encoded,
                    value_count,
                    &Default::default(),
                    &mut values,
                    context,
                )?;
            }
            amudai_format::schema::BasicType::Float64 => {
                context.numeric_encoders.get::<FloatValue<f64>>().decode(
                    encoded,
                    value_count,
                    &Default::default(),
                    &mut values,
                    context,
                )?;
            }
            _ => panic!("unexpected type: {:?}", type_desc.basic_type),
        }

        Ok(ValueSequence {
            values: Values::from_vec(values),
            offsets: None,
            presence,
            type_desc,
        })
    }
}

impl BlockDecoder for PrimitiveBlockDecoder {
    fn decode(&self, encoded: &[u8], value_count: usize) -> amudai_common::Result<ValueSequence> {
        let encoded = encoded.as_ref();
        let metadata = PrimitiveBlockEncoderMetadata::read_from(encoded)?;

        let aligned_presence_size = metadata.presence_size.next_multiple_of(ALIGNMENT_BYTES);
        let values_end_offset =
            PrimitiveBlockEncoderMetadata::size() + aligned_presence_size + metadata.values_size;
        if encoded.len() < values_end_offset {
            return Err(amudai_common::error::Error::invalid_format(
                "Encoded block is too short",
            ));
        }

        let encoded = &encoded[PrimitiveBlockEncoderMetadata::size()..];

        let encoded_presence = &encoded[..metadata.presence_size];
        let presence = encodings::presence::decode_presence(encoded_presence, value_count)?;

        let encoded_data =
            &encoded[aligned_presence_size..aligned_presence_size + metadata.values_size];
        Self::decode_values(
            encoded_data,
            presence,
            self.basic_type,
            value_count,
            &self.context,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        block_decoder::BlockDecoder,
        block_encoder::{
            BlockChecksum, BlockEncoder, BlockEncodingParameters, BlockEncodingPolicy,
            BlockEncodingProfile, EncodedBlock, PresenceEncoding,
        },
        encodings::EncodingContext,
        primitive_block_encoder::PrimitiveBlockEncoder,
    };
    use amudai_format::schema::BasicTypeDescriptor;

    use super::PrimitiveBlockDecoder;
    use amudai_bytes::buffer::AlignedByteVec;
    use std::sync::Arc;

    #[test]
    fn test_round_trip() {
        let policy = BlockEncodingPolicy {
            profile: BlockEncodingProfile::Balanced,
            parameters: BlockEncodingParameters {
                presence: PresenceEncoding::Enabled,
                checksum: BlockChecksum::Enabled,
            },
            size_constraints: None,
        };
        let basic_type = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::Int64,
            signed: true,
            fixed_size: std::mem::size_of::<i64>(),
        };

        let context = Arc::new(EncodingContext::default());
        let mut encoder =
            PrimitiveBlockEncoder::new(policy.clone(), basic_type, Arc::clone(&context));
        let sample_len = encoder.sample_size().unwrap().value_count.start;
        let sample = arrow_array::array::Int64Array::from_iter(
            (0..sample_len).map(|_| fastrand::i64(-100..100) + 1000000),
        );
        let block_constraints = encoder.analyze_sample(&sample).unwrap();

        let blocks_count = 16;
        let mut encoded = AlignedByteVec::new();
        let values_len = block_constraints.value_count.start;
        let mut block_offsets = vec![0];
        let mut original_data = vec![];
        for _ in 0..blocks_count {
            let nulls = arrow_buffer::NullBuffer::from_iter(
                (0..values_len).map(|_| fastrand::f64() >= 0.1),
            );
            let values = arrow_array::array::Int64Array::from_iter_values_with_nulls(
                (0..values_len).map(|_| fastrand::i64(-100..100) + 1000000),
                Some(nulls),
            );
            let encoded_block = encoder.encode(&values).unwrap();
            encoded.extend_from_slice(encoded_block.as_ref());
            block_offsets.push(encoded.len());
            original_data.push(values);
        }

        let decoder =
            PrimitiveBlockDecoder::new(policy.parameters, basic_type, Arc::clone(&context));
        for block in 0..blocks_count {
            let encoded_block =
                EncodedBlock::Borrowed(&encoded[block_offsets[block]..block_offsets[block + 1]]);
            let decoded = decoder.decode(encoded_block.as_ref(), values_len).unwrap();
            assert_eq!(
                original_data[block].values().iter().as_slice(),
                decoded.values.as_slice::<i64>()
            );
        }
    }
}
