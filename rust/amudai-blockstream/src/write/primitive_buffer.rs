//! Buffer encoder for primitive fixed-size values.

use std::sync::Arc;

use amudai_common::{Result, error::Error};
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

use crate::read::primitive_buffer::PrimitiveBufferDecoder;

use super::{PreparedEncodedBuffer, block_stream::BlockStreamEncoder};

/// Encodes a sequence of primitive values into a buffer of block-encoded data,
/// as specified by the shard format. This encoder accumulates values, encodes
/// them into blocks using the selected encoding policy, and writes the encoded
/// blocks to temporary storage. The resulting buffer can be finalized and
/// written to the object store, or read back for further processing.
///
/// The encoded buffer consists of a series of efficiently encoded blocks,
/// along with metadata describing the encoding and block structure.
pub struct PrimitiveBufferEncoder {
    /// The block encoder responsible for encoding primitive values into blocks.
    block_encoder: PrimitiveBlockEncoder,
    /// The stream encoder that manages writing encoded blocks to storage.
    block_stream: BlockStreamEncoder,
    /// Parameters describing the encoding policy for blocks.
    block_params: BlockEncodingParameters,
    /// Descriptor of the basic type being encoded.
    basic_type: BasicTypeDescriptor,
}

impl PrimitiveBufferEncoder {
    /// Creates a new [`PrimitiveBufferEncoder`] with the specified encoding policy,
    /// basic type descriptor, and temporary file store for intermediate storage.
    ///
    /// # Arguments
    ///
    /// * `policy` - The block encoding policy to use for encoding blocks.
    /// * `basic_type` - Descriptor of the primitive type being encoded.
    /// * `temp_store` - Temporary file store for writing encoded data.
    ///
    /// # Errors
    ///
    /// Returns an error if the temporary file store cannot allocate a writable buffer.
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
            basic_type,
        })
    }

    /// Returns the [`BasicTypeDescriptor`] for the values being encoded.
    pub fn basic_type(&self) -> BasicTypeDescriptor {
        self.basic_type
    }

    /// Returns the preferred sample size for the block encoder, if available.
    ///
    /// This can be used to determine how much data to sample for optimal encoding.
    pub fn sample_size(&self) -> Option<BlockSizeConstraints> {
        self.block_encoder.sample_size()
    }

    /// Analyzes a data sample to determine the most appropriate encoding strategy
    /// and block size constraints for the current encoding policy.
    ///
    /// # Arguments
    ///
    /// * `sample` - An Arrow array representing a sample of the data to be encoded.
    ///
    /// # Returns
    ///
    /// Returns the recommended [`BlockSizeConstraints`] for encoding.
    pub fn analyze_sample(
        &mut self,
        sample: &dyn arrow_array::Array,
    ) -> Result<BlockSizeConstraints> {
        self.block_encoder.analyze_sample(sample)
    }

    /// Encodes a sequence of input values into a single block and appends it to the buffer.
    ///
    /// # Arguments
    ///
    /// * `values` - An Arrow array containing the values to encode.
    ///
    /// # Errors
    ///
    /// Returns an error if encoding or writing the block fails.
    pub fn encode_block(&mut self, values: &dyn arrow_array::Array) -> Result<()> {
        let encoded = self.block_encoder.encode(values)?;
        self.block_stream
            .push_block(encoded.as_ref(), values.len())?;
        Ok(())
    }

    /// Finalizes the encoding process and returns a [`PreparedEncodedBuffer`]
    /// containing all encoded data and metadata.
    ///
    /// After calling this method, the encoder can no longer be used.
    ///
    /// # Errors
    ///
    /// Returns an error if finalizing the block stream fails.
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

    /// Finalizes the encoding process and returns a [`PrimitiveBufferDecoder`]
    /// for reading back the encoded data.
    ///
    /// Use this method when you want to stop encoding and immediately access all values
    /// encoded so far - for example, to support re-encoding decisions during ingestion.
    ///
    /// This is a convenience method that calls [`Self::finish`] and constructs
    /// a decoder from the resulting buffer, allowing for seamless transition from
    /// encoding to decoding within the same workflow.
    ///
    /// # Errors
    ///
    /// Returns an error if finalizing the buffer or creating the decoder fails.
    pub fn consume(self) -> Result<PrimitiveBufferDecoder> {
        let basic_type = self.basic_type;
        let prepared = self.finish()?;
        PrimitiveBufferDecoder::from_prepared_buffer(&prepared, basic_type)
    }
}
