use crate::{
    block_encoder::{
        BlockChecksum, BlockEncoder, BlockEncodingPolicy, BlockEncodingProfile,
        BlockSizeConstraints, EncodedBlock, PresenceEncoding,
    },
    encodings::{
        self, ALIGNMENT_BYTES, AnalysisOutcome, EncodingConfig, EncodingContext, EncodingKind,
        EncodingParameters, EncodingPlan, Lz4CompressionMode, Lz4Parameters, NullMask,
        ZstdCompressionLevel, ZstdParameters,
        binary::BinaryValuesSequence,
        numeric::value::{ValueReader, ValueWriter},
    },
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_format::schema::BasicTypeDescriptor;
use std::sync::Arc;

pub struct BinaryBlockEncoder {
    context: Arc<EncodingContext>,
    policy: BlockEncodingPolicy,
    _basic_type: BasicTypeDescriptor,
    encoding_plan: EncodingPlan,
}

impl BinaryBlockEncoder {
    const DEFAULT_SAMPLE_VALUE_COUNT: usize = 128;
    const DEFAULT_SAMPLE_SIZE: usize = Self::DEFAULT_SAMPLE_VALUE_COUNT * 16;
    const DEFAULT_BLOCK_VALUE_COUNT: usize = 8000;
    const DEFAULT_BLOCK_SIZE: usize = Self::DEFAULT_BLOCK_VALUE_COUNT * 16;
    const MINIMAL_SIZE_BLOCK_VALUE_COUNT: usize = 64000;
    const MINIMAL_SIZE_BLOCK_SIZE: usize = Self::MINIMAL_SIZE_BLOCK_VALUE_COUNT * 16;

    pub const DEFAULT_SIZE_CONSTRAINTS: BlockSizeConstraints = BlockSizeConstraints {
        value_count: Self::DEFAULT_BLOCK_VALUE_COUNT..Self::DEFAULT_BLOCK_VALUE_COUNT + 1,
        data_size: Self::DEFAULT_BLOCK_SIZE..Self::DEFAULT_BLOCK_SIZE + 1,
    };

    pub fn new(
        policy: BlockEncodingPolicy,
        basic_type: BasicTypeDescriptor,
        context: Arc<EncodingContext>,
    ) -> Self {
        Self {
            context,
            policy,
            _basic_type: basic_type,
            encoding_plan: EncodingPlan {
                encoding: EncodingKind::Plain,
                parameters: Default::default(),
                cascading_encodings: vec![],
            },
        }
    }

    fn analyze_values(
        values: &dyn arrow_array::Array,
        policy: &BlockEncodingPolicy,
        context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>> {
        let mut config = EncodingConfig::default();
        match policy.profile {
            BlockEncodingProfile::Plain => return Ok(None),
            BlockEncodingProfile::MinimalCompression => {
                config = config
                    .with_max_cascading_levels(1)
                    .with_disallowed_encodings(&[
                        EncodingKind::Lz4,
                        EncodingKind::Zstd,
                        EncodingKind::Fsst,
                    ]);
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
        let values_sequence = BinaryValuesSequence::try_from(values)?;
        context
            .binary_encoders
            .analyze(&values_sequence, &null_mask, &config, context)
    }

    fn encode_values(
        values: &dyn arrow_array::Array,
        encoding_plan: &EncodingPlan,
        context: &EncodingContext,
        target: &mut AlignedByteVec,
    ) -> amudai_common::Result<usize> {
        let null_mask = NullMask::from(values.nulls());
        let values_sequence = BinaryValuesSequence::try_from(values)?;
        context
            .binary_encoders
            .encode(&values_sequence, &null_mask, target, encoding_plan, context)
    }
}

impl BlockEncoder for BinaryBlockEncoder {
    fn sample_size(&self) -> Option<BlockSizeConstraints> {
        Some(BlockSizeConstraints {
            value_count: Self::DEFAULT_SAMPLE_VALUE_COUNT..Self::DEFAULT_SAMPLE_VALUE_COUNT + 1,
            data_size: Self::DEFAULT_SAMPLE_SIZE..Self::DEFAULT_SAMPLE_SIZE + 1,
        })
    }

    fn analyze_sample(
        &mut self,
        sample: &dyn arrow_array::Array,
    ) -> amudai_common::Result<BlockSizeConstraints> {
        self.encoding_plan =
            if let Some(outcome) = Self::analyze_values(sample, &self.policy, &self.context)? {
                outcome.into_plan()
            } else {
                match self.policy.profile {
                    BlockEncodingProfile::Plain => EncodingPlan {
                        encoding: EncodingKind::Plain,
                        parameters: Default::default(),
                        cascading_encodings: vec![],
                    },
                    BlockEncodingProfile::MinimalCompression => EncodingPlan {
                        encoding: EncodingKind::Lz4,
                        parameters: EncodingParameters::Lz4(Lz4Parameters {
                            mode: Lz4CompressionMode::Fast,
                        }),
                        cascading_encodings: vec![Some(EncodingPlan {
                            encoding: EncodingKind::FusedFrameOfReference,
                            parameters: Default::default(),
                            cascading_encodings: vec![],
                        })],
                    },
                    BlockEncodingProfile::Balanced => EncodingPlan {
                        encoding: EncodingKind::Lz4,
                        parameters: EncodingParameters::Lz4(Lz4Parameters {
                            mode: Lz4CompressionMode::HighCompression,
                        }),
                        cascading_encodings: vec![Some(EncodingPlan {
                            encoding: EncodingKind::FusedFrameOfReference,
                            parameters: Default::default(),
                            cascading_encodings: vec![],
                        })],
                    },
                    BlockEncodingProfile::HighCompression => EncodingPlan {
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
                    BlockEncodingProfile::MinimalSize => EncodingPlan {
                        encoding: EncodingKind::Zstd,
                        parameters: EncodingParameters::Zstd(ZstdParameters {
                            level: ZstdCompressionLevel::MaximalSlowest,
                        }),
                        cascading_encodings: vec![Some(EncodingPlan {
                            encoding: EncodingKind::FusedFrameOfReference,
                            parameters: Default::default(),
                            cascading_encodings: vec![],
                        })],
                    },
                }
            };

        match self.policy.profile {
            BlockEncodingProfile::Plain
            | BlockEncodingProfile::MinimalCompression
            | BlockEncodingProfile::Balanced
            | BlockEncodingProfile::HighCompression => Ok(BlockSizeConstraints {
                value_count: Self::DEFAULT_BLOCK_VALUE_COUNT..Self::DEFAULT_BLOCK_VALUE_COUNT + 1,
                data_size: Self::DEFAULT_BLOCK_SIZE..Self::DEFAULT_BLOCK_SIZE + 1,
            }),
            BlockEncodingProfile::MinimalSize => Ok(BlockSizeConstraints {
                value_count: Self::MINIMAL_SIZE_BLOCK_VALUE_COUNT
                    ..Self::MINIMAL_SIZE_BLOCK_VALUE_COUNT + 1,
                data_size: Self::MINIMAL_SIZE_BLOCK_SIZE..Self::MINIMAL_SIZE_BLOCK_SIZE + 1,
            }),
        }
    }

    fn encode(&mut self, input: &dyn arrow_array::Array) -> amudai_common::Result<EncodedBlock> {
        let mut buffer = self.context.buffers.get_buffer();
        let mut metadata = BinaryBlockEncoderMetadata::initialize(&mut buffer);

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

        metadata.values_size =
            Self::encode_values(input, &self.encoding_plan, &self.context, &mut buffer)?;

        metadata.finalize(&mut buffer);

        if self.policy.parameters.checksum == BlockChecksum::Enabled {
            let checksum = amudai_format::checksum::compute(&buffer);
            buffer.extend_from_slice(&checksum.to_le_bytes());
        }

        Ok(EncodedBlock::Pooled(buffer))
    }
}

pub(crate) struct BinaryBlockEncoderMetadata {
    start_offset: usize,
    pub presence_size: usize,
    pub values_size: usize,
}

impl BinaryBlockEncoderMetadata {
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
