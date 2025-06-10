use crate::{
    block_encoder::{
        BlockChecksum, BlockEncoder, BlockEncodingPolicy, BlockEncodingProfile,
        BlockSizeConstraints, EncodedBlock, PresenceEncoding,
    },
    encodings::{
        self, ALIGNMENT_BYTES, AnalysisOutcome, EncodingConfig, EncodingContext, EncodingKind,
        EncodingParameters, EncodingPlan, NullMask, ZstdCompressionLevel, ZstdParameters,
        numeric::value::{FloatValue, ValueReader, ValueWriter},
    },
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_format::schema::BasicTypeDescriptor;
use arrow_array::cast::AsArray;
use std::sync::Arc;

pub struct PrimitiveBlockEncoder {
    context: Arc<EncodingContext>,
    policy: BlockEncodingPolicy,
    basic_type: BasicTypeDescriptor,
    encoding_plan: EncodingPlan,
}

impl PrimitiveBlockEncoder {
    const DEFAULT_SAMPLE_VALUE_COUNT: usize = 1024;
    const NUMERIC_ENC_BLOCK_VALUE_COUNT: usize = 8000;
    const GENERIC_ENC_BLOCK_VALUE_COUNT: usize = 64000;

    pub fn new(
        policy: BlockEncodingPolicy,
        basic_type: BasicTypeDescriptor,
        context: Arc<EncodingContext>,
    ) -> Self {
        Self {
            context,
            policy,
            basic_type,
            encoding_plan: EncodingPlan {
                encoding: EncodingKind::Plain,
                parameters: Default::default(),
                cascading_encodings: vec![],
            },
        }
    }

    fn analyze_values(
        values: &dyn arrow_array::Array,
        basic_type: BasicTypeDescriptor,
        policy: &BlockEncodingPolicy,
        context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>> {
        let mut config = EncodingConfig::default();
        match policy.profile {
            BlockEncodingProfile::Plain => return Ok(None),
            BlockEncodingProfile::MinimalCompression => {
                config = config
                    .with_max_cascading_levels(1)
                    .with_disallowed_encodings(&[EncodingKind::Lz4, EncodingKind::Zstd]);
            }
            BlockEncodingProfile::Balanced => {
                config = config.with_disallowed_encodings(&[EncodingKind::Zstd])
            }
            BlockEncodingProfile::HighCompression => {}
            BlockEncodingProfile::MinimalSize => {
                config = config.with_cascading_use_all_encodings();
            }
        }

        let null_mask = NullMask::from(values.nulls());
        match basic_type.basic_type {
            amudai_format::schema::BasicType::Int8 => {
                if basic_type.signed {
                    context.numeric_encoders.get::<i8>().analyze(
                        values
                            .as_primitive::<arrow_array::types::Int8Type>()
                            .values(),
                        &null_mask,
                        &config,
                        context,
                    )
                } else {
                    context.numeric_encoders.get::<u8>().analyze(
                        values
                            .as_primitive::<arrow_array::types::UInt8Type>()
                            .values(),
                        &null_mask,
                        &config,
                        context,
                    )
                }
            }
            amudai_format::schema::BasicType::Int16 => {
                if basic_type.signed {
                    context.numeric_encoders.get::<i16>().analyze(
                        values
                            .as_primitive::<arrow_array::types::Int16Type>()
                            .values(),
                        &null_mask,
                        &config,
                        context,
                    )
                } else {
                    context.numeric_encoders.get::<u16>().analyze(
                        values
                            .as_primitive::<arrow_array::types::UInt16Type>()
                            .values(),
                        &null_mask,
                        &config,
                        context,
                    )
                }
            }
            amudai_format::schema::BasicType::Int32 => {
                if basic_type.signed {
                    context.numeric_encoders.get::<i32>().analyze(
                        values
                            .as_primitive::<arrow_array::types::Int32Type>()
                            .values(),
                        &null_mask,
                        &config,
                        context,
                    )
                } else {
                    context.numeric_encoders.get::<u32>().analyze(
                        values
                            .as_primitive::<arrow_array::types::UInt32Type>()
                            .values(),
                        &null_mask,
                        &config,
                        context,
                    )
                }
            }
            amudai_format::schema::BasicType::Int64 => {
                if basic_type.signed {
                    context.numeric_encoders.get::<i64>().analyze(
                        values
                            .as_primitive::<arrow_array::types::Int64Type>()
                            .values(),
                        &null_mask,
                        &config,
                        context,
                    )
                } else {
                    context.numeric_encoders.get::<u64>().analyze(
                        values
                            .as_primitive::<arrow_array::types::UInt64Type>()
                            .values(),
                        &null_mask,
                        &config,
                        context,
                    )
                }
            }
            amudai_format::schema::BasicType::Float32 => {
                let mut adapted_values = context.buffers.get_buffer();
                adapted_values.reserve(values.len() * std::mem::size_of::<FloatValue<f32>>());
                for &value in values
                    .as_primitive::<arrow_array::types::Float32Type>()
                    .values()
                {
                    adapted_values.push_typed::<FloatValue<f32>>(FloatValue::new(value));
                }
                context.numeric_encoders.get::<FloatValue<f32>>().analyze(
                    adapted_values.typed_data(),
                    &null_mask,
                    &config,
                    context,
                )
            }
            amudai_format::schema::BasicType::Float64 => {
                let mut adapted_values = context.buffers.get_buffer();
                adapted_values.reserve(values.len() * std::mem::size_of::<FloatValue<f64>>());
                for &value in values
                    .as_primitive::<arrow_array::types::Float64Type>()
                    .values()
                {
                    adapted_values.push_typed::<FloatValue<f64>>(FloatValue::new(value));
                }
                context.numeric_encoders.get::<FloatValue<f64>>().analyze(
                    adapted_values.typed_data(),
                    &null_mask,
                    &config,
                    context,
                )
            }
            amudai_format::schema::BasicType::DateTime => {
                context.numeric_encoders.get::<u64>().analyze(
                    values
                        .as_primitive::<arrow_array::types::UInt64Type>()
                        .values(),
                    &null_mask,
                    &config,
                    context,
                )
            }
            _ => panic!("unexpected type: {:?}", basic_type.basic_type),
        }
    }

    fn encode_values(
        values: &dyn arrow_array::Array,
        basic_type: BasicTypeDescriptor,
        encoding_plan: &EncodingPlan,
        context: &EncodingContext,
        target: &mut AlignedByteVec,
    ) -> amudai_common::Result<usize> {
        let null_mask = NullMask::from(values.nulls());
        match basic_type.basic_type {
            amudai_format::schema::BasicType::Int8 => {
                if basic_type.signed {
                    context.numeric_encoders.get::<i8>().encode(
                        values
                            .as_primitive::<arrow_array::types::Int8Type>()
                            .values(),
                        &null_mask,
                        target,
                        encoding_plan,
                        context,
                    )
                } else {
                    context.numeric_encoders.get::<u8>().encode(
                        values
                            .as_primitive::<arrow_array::types::UInt8Type>()
                            .values(),
                        &null_mask,
                        target,
                        encoding_plan,
                        context,
                    )
                }
            }
            amudai_format::schema::BasicType::Int16 => {
                if basic_type.signed {
                    context.numeric_encoders.get::<i16>().encode(
                        values
                            .as_primitive::<arrow_array::types::Int16Type>()
                            .values(),
                        &null_mask,
                        target,
                        encoding_plan,
                        context,
                    )
                } else {
                    context.numeric_encoders.get::<u16>().encode(
                        values
                            .as_primitive::<arrow_array::types::UInt16Type>()
                            .values(),
                        &null_mask,
                        target,
                        encoding_plan,
                        context,
                    )
                }
            }
            amudai_format::schema::BasicType::Int32 => {
                if basic_type.signed {
                    context.numeric_encoders.get::<i32>().encode(
                        values
                            .as_primitive::<arrow_array::types::Int32Type>()
                            .values(),
                        &null_mask,
                        target,
                        encoding_plan,
                        context,
                    )
                } else {
                    context.numeric_encoders.get::<u32>().encode(
                        values
                            .as_primitive::<arrow_array::types::UInt32Type>()
                            .values(),
                        &null_mask,
                        target,
                        encoding_plan,
                        context,
                    )
                }
            }
            amudai_format::schema::BasicType::Int64 => {
                if basic_type.signed {
                    context.numeric_encoders.get::<i64>().encode(
                        values
                            .as_primitive::<arrow_array::types::Int64Type>()
                            .values(),
                        &null_mask,
                        target,
                        encoding_plan,
                        context,
                    )
                } else {
                    context.numeric_encoders.get::<u64>().encode(
                        values
                            .as_primitive::<arrow_array::types::UInt64Type>()
                            .values(),
                        &null_mask,
                        target,
                        encoding_plan,
                        context,
                    )
                }
            }
            amudai_format::schema::BasicType::Float32 => {
                let mut adapted_values = context.buffers.get_buffer();
                adapted_values.reserve(values.len() * std::mem::size_of::<FloatValue<f32>>());
                for &value in values
                    .as_primitive::<arrow_array::types::Float32Type>()
                    .values()
                {
                    adapted_values.push_typed::<FloatValue<f32>>(FloatValue::new(value));
                }
                context.numeric_encoders.get::<FloatValue<f32>>().encode(
                    adapted_values.typed_data(),
                    &null_mask,
                    target,
                    encoding_plan,
                    context,
                )
            }
            amudai_format::schema::BasicType::Float64 => {
                let mut adapted_values = context.buffers.get_buffer();
                adapted_values.reserve(values.len() * std::mem::size_of::<FloatValue<f64>>());
                for &value in values
                    .as_primitive::<arrow_array::types::Float64Type>()
                    .values()
                {
                    adapted_values.push_typed::<FloatValue<f64>>(FloatValue::new(value));
                }
                context.numeric_encoders.get::<FloatValue<f64>>().encode(
                    adapted_values.typed_data(),
                    &null_mask,
                    target,
                    encoding_plan,
                    context,
                )
            }
            amudai_format::schema::BasicType::DateTime => {
                context.numeric_encoders.get::<u64>().encode(
                    values
                        .as_primitive::<arrow_array::types::UInt64Type>()
                        .values(),
                    &null_mask,
                    target,
                    encoding_plan,
                    context,
                )
            }
            _ => panic!("unexpected type: {:?}", basic_type.basic_type),
        }
    }
}

impl BlockEncoder for PrimitiveBlockEncoder {
    fn sample_size(&self) -> Option<BlockSizeConstraints> {
        let primitive_size = self.basic_type.primitive_size().expect("primitive size");
        Some(BlockSizeConstraints {
            value_count: Self::DEFAULT_SAMPLE_VALUE_COUNT..Self::DEFAULT_SAMPLE_VALUE_COUNT + 1,
            data_size: Self::DEFAULT_SAMPLE_VALUE_COUNT * primitive_size
                ..Self::DEFAULT_SAMPLE_VALUE_COUNT * primitive_size + 1,
        })
    }

    fn analyze_sample(
        &mut self,
        sample: &dyn arrow_array::Array,
    ) -> amudai_common::Result<BlockSizeConstraints> {
        let mut block_value_count = Self::NUMERIC_ENC_BLOCK_VALUE_COUNT;

        if let Some(outcome) =
            Self::analyze_values(sample, self.basic_type, &self.policy, &self.context)?
        {
            self.encoding_plan = outcome.into_plan();
        } else {
            self.encoding_plan = match self.policy.profile {
                BlockEncodingProfile::Plain => EncodingPlan {
                    encoding: EncodingKind::Plain,
                    parameters: Default::default(),
                    cascading_encodings: vec![],
                },
                BlockEncodingProfile::MinimalCompression => EncodingPlan {
                    encoding: EncodingKind::Zstd,
                    parameters: EncodingParameters::Zstd(ZstdParameters {
                        level: ZstdCompressionLevel::MinimalFastest,
                    }),
                    cascading_encodings: vec![Some(EncodingPlan {
                        encoding: EncodingKind::FusedFrameOfReference,
                        parameters: Default::default(),
                        cascading_encodings: vec![],
                    })],
                },
                BlockEncodingProfile::Balanced => EncodingPlan {
                    encoding: EncodingKind::Zstd,
                    parameters: EncodingParameters::Zstd(ZstdParameters {
                        level: ZstdCompressionLevel::Default,
                    }),
                    cascading_encodings: vec![Some(EncodingPlan {
                        encoding: EncodingKind::FusedFrameOfReference,
                        parameters: Default::default(),
                        cascading_encodings: vec![],
                    })],
                },
                BlockEncodingProfile::HighCompression | BlockEncodingProfile::MinimalSize => {
                    EncodingPlan {
                        encoding: EncodingKind::Zstd,
                        parameters: EncodingParameters::Zstd(ZstdParameters {
                            level: ZstdCompressionLevel::MaximalSlowest,
                        }),
                        cascading_encodings: vec![Some(EncodingPlan {
                            encoding: EncodingKind::FusedFrameOfReference,
                            parameters: Default::default(),
                            cascading_encodings: vec![],
                        })],
                    }
                }
            };
            block_value_count = Self::GENERIC_ENC_BLOCK_VALUE_COUNT;
        }

        let primitive_size = self.basic_type.primitive_size().expect("primitive size");
        Ok(BlockSizeConstraints {
            value_count: block_value_count..block_value_count + 1,
            data_size: block_value_count * primitive_size..block_value_count * primitive_size + 1,
        })
    }

    fn encode(&mut self, input: &dyn arrow_array::Array) -> amudai_common::Result<EncodedBlock> {
        let mut buffer = self.context.buffers.get_buffer();
        let mut metadata = PrimitiveBlockEncoderMetadata::initialize(&mut buffer);

        // TODO: implement embedded presence encoding.
        if self.policy.parameters.presence == PresenceEncoding::Enabled {
            metadata.presence_size = encodings::presence::encode_presence(
                input.nulls(),
                &mut buffer,
                &self.context,
                self.policy.profile,
            )?;

            // Make sure the target buffer is aligned before encoding the values.
            let alignment = buffer.len().next_multiple_of(ALIGNMENT_BYTES);
            buffer.resize(alignment, 0);
        }

        metadata.values_size = Self::encode_values(
            input,
            self.basic_type,
            &self.encoding_plan,
            &self.context,
            &mut buffer,
        )?;

        metadata.finalize(&mut buffer);

        if self.policy.parameters.checksum == BlockChecksum::Enabled {
            let checksum = amudai_format::checksum::compute(&buffer);
            buffer.write_value::<u32>(checksum);
        }

        Ok(EncodedBlock::Pooled(buffer))
    }
}

pub(crate) struct PrimitiveBlockEncoderMetadata {
    start_offset: usize,
    pub presence_size: usize,
    pub values_size: usize,
}

impl PrimitiveBlockEncoderMetadata {
    pub const fn size() -> usize {
        8
    }

    pub fn initialize(target: &mut AlignedByteVec) -> Self {
        let start_offset = target.len();
        target.resize(start_offset + Self::size(), 0);
        Self {
            start_offset,
            presence_size: 0,
            values_size: 0,
        }
    }

    pub fn finalize(self, target: &mut AlignedByteVec) {
        target.write_value_at::<u32>(self.start_offset, self.presence_size as u32);
        target.write_value_at::<u32>(self.start_offset + 4, self.values_size as u32);
    }

    pub fn read_from(buffer: &[u8]) -> amudai_common::Result<Self> {
        if buffer.len() < Self::size() {
            return Err(amudai_common::error::Error::invalid_format(
                "Encoded buffer size is too small",
            ));
        }
        Ok(Self {
            start_offset: 0,
            presence_size: buffer.read_value::<u32>(0) as usize,
            values_size: buffer.read_value::<u32>(4) as usize,
        })
    }
}
