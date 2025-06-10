use std::sync::Arc;

use amudai_common::{Result, error::Error};
use amudai_encodings::{
    binary_block_encoder::BinaryBlockEncoder,
    block_encoder::{BlockEncoder, BlockEncodingPolicy, BlockSizeConstraints},
};
use amudai_format::{
    defs::{common::DataRef, shard},
    schema::BasicTypeDescriptor,
};
use amudai_io::temp_file_store::TemporaryFileStore;

use super::{PreparedEncodedBuffer, block_stream::BlockStreamEncoder};

/// Encodes a sequence of binary data (either Utf8 strings or Binary) into a single
/// *encoded buffer* for a stripe-level field in the shard format.
///
/// Note: when using `BytesBufferEncoder`, the resulting encoded blocks are self-sufficient
/// (they contain both the block data and the local offsets).
pub struct BytesBufferEncoder {
    /// The inner block encoder that handles the actual binary data compression
    block_encoder: BinaryBlockEncoder,

    /// Manages the block stream and block map construction
    block_stream: BlockStreamEncoder,

    /// Encoding parameters specifying presence encoding and checksum settings
    policy: BlockEncodingPolicy,
}

impl BytesBufferEncoder {
    /// Creates a new binary buffer encoder with specified encoding policy and basic type.
    ///
    /// # Arguments
    ///
    /// * `policy` - Defines the encoding strategy, compression level, and other parameters
    /// * `basic_type` - The basic type descriptor for the data being encoded (Binary, String, etc.)
    /// * `temp_store` - Storage for temporary block data during encoding
    ///
    /// # Returns
    ///
    /// A new `BinaryBufferEncoder` instance ready to encode data, or an error if initialization fails
    pub fn new(
        policy: BlockEncodingPolicy,
        basic_type: BasicTypeDescriptor,
        temp_store: Arc<dyn TemporaryFileStore>,
    ) -> Result<BytesBufferEncoder> {
        // TODO: consider passing reasonable size hint.
        let writer = temp_store
            .allocate_writable(None)
            .map_err(|e| Error::io("allocate_writable", e))?;
        Ok(BytesBufferEncoder {
            block_encoder: BinaryBlockEncoder::new(
                policy.clone(),
                basic_type,
                // TODO: decide how we deal with the context
                Arc::new(Default::default()),
            ),
            block_stream: BlockStreamEncoder::new(writer),
            policy,
        })
    }

    /// Returns the preferred sample size for data analysis.
    ///
    /// Provides guidance on the ideal number of values and byte size that should be
    /// sampled for analyzing encoding strategies.
    ///
    /// # Returns
    ///
    /// Size constraints for optimal sampling, or `None` if specific constraints aren't
    /// needed.
    pub fn sample_size(&self) -> Option<BlockSizeConstraints> {
        self.block_encoder.sample_size()
    }

    /// Analyzes a sample of data to determine the optimal encoding strategy.
    ///
    /// This method normalizes the input array to a consistent format before analysis
    /// and configures the encoder to use the most appropriate encoding for this data.
    ///
    /// # Arguments
    ///
    /// * `sample` - A representative sample of the data to be encoded
    ///
    /// # Returns
    ///
    /// Constraints for the block size when encoding actual data
    pub fn analyze_sample(
        &mut self,
        sample: &dyn arrow_array::Array,
    ) -> Result<BlockSizeConstraints> {
        self.block_encoder.analyze_sample(sample)
    }

    /// Encodes a sequence of string or binary values into a single block.
    ///
    /// # Arguments
    ///
    /// * `values` - The array of values to encode as a block
    ///
    /// # Returns
    ///
    /// `Ok(())` if encoding succeeds, or an error if encoding fails
    pub fn encode_block(&mut self, values: &dyn arrow_array::Array) -> Result<()> {
        let encoded = self.block_encoder.encode(values)?;
        self.block_stream
            .push_block(encoded.as_ref(), values.len())?;
        Ok(())
    }

    /// Finalizes the encoding process and returns the prepared encoded buffer.
    ///
    /// This method completes the block stream, constructs the final buffer descriptor,
    /// and returns a `PreparedEncodedBuffer` that contains the encoded data and metadata.
    ///
    /// # Returns
    ///
    /// A `PreparedEncodedBuffer` containing the encoded data blocks and block map
    pub fn finish(self) -> Result<PreparedEncodedBuffer> {
        let blocks = self.block_stream.finish()?;

        let res = PreparedEncodedBuffer {
            descriptor: shard::EncodedBuffer {
                kind: shard::BufferKind::Data as i32,
                buffer: Some(DataRef::new("", 0..blocks.total_size())),
                block_map: Some(DataRef::new("", blocks.data_size()..blocks.total_size())),
                block_count: Some(blocks.block_count),
                block_checksums: self.policy.parameters.checksum.is_enabled(),
                embedded_presence: self.policy.parameters.presence.is_enabled(),
                embedded_offsets: true,
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
