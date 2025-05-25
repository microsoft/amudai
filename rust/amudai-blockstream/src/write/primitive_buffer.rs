use std::sync::Arc;

use amudai_common::{error::Error, Result};
use amudai_encodings::{
    block_encoder::{
        BlockEncoder, BlockEncodingParameters, BlockEncodingPolicy, BlockSizeConstraints,
    },
    primitive_block_encoder::PrimitiveBlockEncoder,
};
use amudai_format::{
    defs::{common::DataRef, shard},
    schema::BasicTypeDescriptor,
};
use amudai_io::temp_file_store::TemporaryFileStore;

use super::{block_stream::BlockStreamEncoder, PreparedEncodedBuffer};

/// Generates an *encoded buffer* for a stripe-level sequence of primitive values,
/// as specified in the shard format documentation. This encoded buffer consists
/// of a series of efficiently encoded blocks that contain the field's data.
pub struct PrimitiveBufferEncoder {
    block_encoder: PrimitiveBlockEncoder,
    block_stream: BlockStreamEncoder,
    block_params: BlockEncodingParameters,
}

impl PrimitiveBufferEncoder {
    pub fn new(
        policy: BlockEncodingPolicy,
        basic_type: BasicTypeDescriptor,
        temp_store: Arc<dyn TemporaryFileStore>,
    ) -> Result<PrimitiveBufferEncoder> {
        // TODO: consider passing reasonable size hint.
        let writer = temp_store
            .allocate_writable(None)
            .map_err(|e| Error::io("allocate_writable", e))?;
        Ok(PrimitiveBufferEncoder {
            block_params: policy.parameters.clone(),
            block_encoder: PrimitiveBlockEncoder::new(
                policy,
                basic_type,
                // TODO: decide how we deal with the context
                Arc::new(Default::default()),
            ),
            block_stream: BlockStreamEncoder::new(writer),
        })
    }

    /// Returns the preferred sample size for the block encoder.
    pub fn sample_size(&self) -> Option<BlockSizeConstraints> {
        self.block_encoder.sample_size()
    }

    /// Analyzes a data sample to determine the most appropriate encoding strategy.
    pub fn analyze_sample(
        &mut self,
        sample: &dyn arrow_array::Array,
    ) -> Result<BlockSizeConstraints> {
        self.block_encoder.analyze_sample(sample)
    }

    /// Encodes a sequence of input values into a single block.
    pub fn encode_block(&mut self, values: &dyn arrow_array::Array) -> Result<()> {
        let encoded = self.block_encoder.encode(values)?;
        self.block_stream
            .push_block(encoded.as_ref(), values.len())?;
        Ok(())
    }

    pub fn finish(self) -> Result<PreparedEncodedBuffer> {
        let blocks = self.block_stream.finish()?;

        let res = PreparedEncodedBuffer {
            descriptor: shard::EncodedBuffer {
                kind: shard::BufferKind::Data as i32,
                buffer: Some(DataRef::new("", 0..blocks.total_size())),
                block_map: Some(DataRef::new("", blocks.data_size()..blocks.total_size())),
                block_count: Some(blocks.block_count),
                block_checksums: self.block_params.checksum.is_enabled(),
                embedded_presence: self.block_params.presence.is_enabled(),
                embedded_offsets: false,
                buffer_id: None,
                packed_group_index: None,
            },
            data_size: blocks.data_size,
            block_map_size: blocks.block_map_size,
            data: blocks.block_stream,
        };
        Ok(res)
    }
}
