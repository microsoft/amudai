use crate::{
    block_decoder::BlockDecoder,
    block_encoder::BlockEncodingParameters,
    encodings::{self, ALIGNMENT_BYTES, EncodingContext, numeric::value::FloatValue},
    primitive_block_encoder::PrimitiveBlockEncoderMetadata,
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_format::schema::BasicTypeDescriptor;
use amudai_sequence::{presence::Presence, sequence::ValueSequence, values::Values};
use std::sync::Arc;

/// Block decoder specialized for primitive (fixed-size) data types.
///
/// The `PrimitiveBlockDecoder` is the counterpart to `PrimitiveBlockEncoder`
/// and handles decoding of encoded primitive data blocks back into their
/// original form. It supports all the same data types and encoding schemes
/// as the encoder.
///
/// # Supported Decodings
///
/// - Plain encoding (direct memory copy)
/// - Bit-packing with various bit widths
/// - Frame-of-reference decoding
/// - Delta decoding
/// - Run-length decoding
/// - Single-value expansion
/// - Complex cascaded encodings
///
/// # Usage
///
/// The decoder requires the same encoding parameters that were used during
/// encoding to properly reconstruct the original data. This includes the
/// basic type descriptor and block encoding parameters.
pub struct PrimitiveBlockDecoder {
    /// Encoding parameters used during block creation (for future use).
    _parameters: BlockEncodingParameters,
    /// Type descriptor for the primitive data being decoded.
    basic_type: BasicTypeDescriptor,
    /// Shared encoding context containing decoders and configuration.
    context: Arc<EncodingContext>,
}

impl PrimitiveBlockDecoder {
    /// Creates a new primitive block decoder.
    ///
    /// # Arguments
    ///
    /// * `parameters` - Block encoding parameters that were used during encoding
    /// * `basic_type` - Type descriptor for the primitive data being decoded
    /// * `context` - Shared encoding context with available decoders
    ///
    /// # Returns
    ///
    /// A new `PrimitiveBlockDecoder` instance ready to decode blocks of the
    /// specified primitive type.
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

    /// Dispatches the decoding of values based on the type descriptor.
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
                        None,
                        &mut values,
                        context,
                    )?;
                } else {
                    context.numeric_encoders.get::<u8>().decode(
                        encoded,
                        value_count,
                        None,
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
                        None,
                        &mut values,
                        context,
                    )?;
                } else {
                    context.numeric_encoders.get::<u16>().decode(
                        encoded,
                        value_count,
                        None,
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
                        None,
                        &mut values,
                        context,
                    )?;
                } else {
                    context.numeric_encoders.get::<u32>().decode(
                        encoded,
                        value_count,
                        None,
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
                        None,
                        &mut values,
                        context,
                    )?;
                } else {
                    context.numeric_encoders.get::<u64>().decode(
                        encoded,
                        value_count,
                        None,
                        &mut values,
                        context,
                    )?;
                }
            }
            amudai_format::schema::BasicType::Float32 => {
                context.numeric_encoders.get::<FloatValue<f32>>().decode(
                    encoded,
                    value_count,
                    None,
                    &mut values,
                    context,
                )?;
            }
            amudai_format::schema::BasicType::Float64 => {
                context.numeric_encoders.get::<FloatValue<f64>>().decode(
                    encoded,
                    value_count,
                    None,
                    &mut values,
                    context,
                )?;
            }
            amudai_format::schema::BasicType::DateTime => {
                context.numeric_encoders.get::<u64>().decode(
                    encoded,
                    value_count,
                    None,
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

    fn inspect_values(
        buffer: &[u8],
        type_desc: BasicTypeDescriptor,
        context: &EncodingContext,
    ) -> amudai_common::Result<encodings::EncodingPlan> {
        match type_desc.basic_type {
            amudai_format::schema::BasicType::Int8 => {
                if type_desc.signed {
                    context
                        .numeric_encoders
                        .get::<i8>()
                        .inspect(buffer, context)
                } else {
                    context
                        .numeric_encoders
                        .get::<u8>()
                        .inspect(buffer, context)
                }
            }
            amudai_format::schema::BasicType::Int16 => {
                if type_desc.signed {
                    context
                        .numeric_encoders
                        .get::<i16>()
                        .inspect(buffer, context)
                } else {
                    context
                        .numeric_encoders
                        .get::<u16>()
                        .inspect(buffer, context)
                }
            }
            amudai_format::schema::BasicType::Int32 => {
                if type_desc.signed {
                    context
                        .numeric_encoders
                        .get::<i32>()
                        .inspect(buffer, context)
                } else {
                    context
                        .numeric_encoders
                        .get::<u32>()
                        .inspect(buffer, context)
                }
            }
            amudai_format::schema::BasicType::Int64 => {
                if type_desc.signed {
                    context
                        .numeric_encoders
                        .get::<i64>()
                        .inspect(buffer, context)
                } else {
                    context
                        .numeric_encoders
                        .get::<u64>()
                        .inspect(buffer, context)
                }
            }
            amudai_format::schema::BasicType::Float32 => context
                .numeric_encoders
                .get::<FloatValue<f32>>()
                .inspect(buffer, context),
            amudai_format::schema::BasicType::Float64 => context
                .numeric_encoders
                .get::<FloatValue<f64>>()
                .inspect(buffer, context),
            amudai_format::schema::BasicType::DateTime => context
                .numeric_encoders
                .get::<u64>()
                .inspect(buffer, context),
            _ => panic!("unexpected type: {:?}", type_desc.basic_type),
        }
    }
}

impl BlockDecoder for PrimitiveBlockDecoder {
    fn decode(&self, encoded: &[u8], value_count: usize) -> amudai_common::Result<ValueSequence> {
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

    fn inspect(&self, encoded: &[u8]) -> amudai_common::Result<encodings::EncodingPlan> {
        let metadata = PrimitiveBlockEncoderMetadata::read_from(encoded)?;
        let values_pos = PrimitiveBlockEncoderMetadata::size()
            + metadata.presence_size.next_multiple_of(ALIGNMENT_BYTES);
        let encoded = &encoded[values_pos..values_pos + metadata.values_size];
        Self::inspect_values(encoded, self.basic_type, &self.context)
    }
}

#[cfg(test)]
mod tests {
    use super::PrimitiveBlockDecoder;
    use crate::{
        block_decoder::BlockDecoder,
        block_encoder::{
            BlockChecksum, BlockEncoder, BlockEncodingParameters, BlockEncodingPolicy,
            BlockEncodingProfile, EncodedBlock, PresenceEncoding,
        },
        encodings::EncodingContext,
        primitive_block_encoder::PrimitiveBlockEncoder,
    };
    use amudai_bytes::buffer::AlignedByteVec;
    use amudai_format::schema::BasicTypeDescriptor;
    use amudai_sequence::presence::Presence;
    use bytemuck::Pod;
    use core::panic;
    use itertools::Itertools;
    use std::sync::Arc;

    fn run_round_trip_test<ArrowType, Gen, I>(
        policy: BlockEncodingPolicy,
        basic_type: BasicTypeDescriptor,
        data_gen: Gen,
    ) where
        ArrowType: arrow_array::ArrowPrimitiveType,
        ArrowType::Native: Pod,
        Gen: Fn(usize) -> I,
        I: IntoIterator<Item = ArrowType::Native>,
    {
        let context = Arc::new(EncodingContext::default());
        let mut encoder =
            PrimitiveBlockEncoder::new(policy.clone(), basic_type, Arc::clone(&context));

        // Generate a sample to analyze it.
        let sample_len = encoder.sample_size().unwrap().value_count.start;
        let sample =
            arrow_array::array::PrimitiveArray::<ArrowType>::from_iter_values(data_gen(sample_len));
        let block_constraints = encoder.analyze_sample(&sample).unwrap();

        // Generate multiple blocks of data and encode them.
        let blocks_count = 16;
        let mut encoded = AlignedByteVec::new();
        let values_len = block_constraints.value_count.start;
        let mut block_offsets = vec![0];
        let mut original_data = vec![];
        let mut original_presence = vec![];
        for _ in 0..blocks_count {
            let nulls = arrow_buffer::NullBuffer::from_iter(
                (0..values_len).map(|_| fastrand::f64() >= 0.1),
            );
            original_presence.push(nulls.iter().map(|p| if p { 1 } else { 0 }).collect_vec());
            let values =
                arrow_array::array::PrimitiveArray::<ArrowType>::from_iter_values_with_nulls(
                    data_gen(values_len),
                    Some(nulls),
                );
            let encoded_block = encoder.encode(&values).unwrap();
            encoded.extend_from_slice(encoded_block.as_ref());
            block_offsets.push(encoded.len());
            original_data.push(values);
        }

        // Decode the encoded data and compare with original one.
        let decoder =
            PrimitiveBlockDecoder::new(policy.parameters, basic_type, Arc::clone(&context));
        for block in 0..blocks_count {
            let encoded_block =
                EncodedBlock::Borrowed(&encoded[block_offsets[block]..block_offsets[block + 1]]);
            let decoded = decoder.decode(encoded_block.as_ref(), values_len).unwrap();
            assert_eq!(decoded.type_desc, basic_type);
            assert_eq!(
                original_data[block].values().iter().as_slice(),
                decoded.values.as_slice::<ArrowType::Native>()
            );
            let Presence::Bytes(presence_bytes) = decoded.presence else {
                panic!("Expected decoded presence to be Presence::Bytes");
            };
            assert_eq!(
                original_presence[block].as_slice(),
                presence_bytes.as_slice()
            );
        }
    }

    #[test]
    fn test_round_trip_int64() {
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
            fixed_size: std::mem::size_of::<i64>() as u32,
            extended_type: Default::default(),
        };

        run_round_trip_test::<arrow_array::types::Int64Type, _, _>(policy, basic_type, |len| {
            (0..len).map(|_| fastrand::i64(-100..100) + 1000000)
        });
    }

    #[test]
    fn test_round_trip_float32() {
        let policy = BlockEncodingPolicy {
            profile: BlockEncodingProfile::Balanced,
            parameters: BlockEncodingParameters {
                presence: PresenceEncoding::Enabled,
                checksum: BlockChecksum::Enabled,
            },
            size_constraints: None,
        };
        let basic_type = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::Float32,
            signed: true,
            fixed_size: std::mem::size_of::<f32>() as u32,
            extended_type: Default::default(),
        };

        run_round_trip_test::<arrow_array::types::Float32Type, _, _>(policy, basic_type, |len| {
            (0..len).map(|_| fastrand::f32())
        });
    }

    #[test]
    fn test_round_trip_float64() {
        let policy = BlockEncodingPolicy {
            profile: BlockEncodingProfile::Balanced,
            parameters: BlockEncodingParameters {
                presence: PresenceEncoding::Enabled,
                checksum: BlockChecksum::Enabled,
            },
            size_constraints: None,
        };
        let basic_type = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::Float64,
            signed: true,
            fixed_size: std::mem::size_of::<f64>() as u32,
            extended_type: Default::default(),
        };

        run_round_trip_test::<arrow_array::types::Float64Type, _, _>(policy, basic_type, |len| {
            (0..len).map(|_| fastrand::f64())
        });
    }

    #[test]
    fn test_round_trip_datetime() {
        let policy = BlockEncodingPolicy {
            profile: BlockEncodingProfile::Balanced,
            parameters: BlockEncodingParameters {
                presence: PresenceEncoding::Enabled,
                checksum: BlockChecksum::Enabled,
            },
            size_constraints: None,
        };
        let basic_type = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::DateTime,
            signed: false,
            fixed_size: std::mem::size_of::<u64>() as u32,
            extended_type: Default::default(),
        };

        run_round_trip_test::<arrow_array::types::UInt64Type, _, _>(policy, basic_type, |len| {
            (0..len).map(|_| 638843822550000000 + fastrand::u64(0..1000))
        });
    }
}
