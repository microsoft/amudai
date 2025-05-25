//! Creation and encoding of the block-based `EncodedBuffers`.

use std::sync::Arc;

use amudai_format::defs::shard;
use amudai_io::ReadAt;

pub mod block_map_encoder;
pub mod block_stream;
pub mod bytes_buffer;
pub mod primitive_buffer;

/// `PreparedEncodedBuffer` represents a fully encoded sequence of values
/// for a stripe field, typically structured as a series of compressed blocks.
///
/// Note: For a given sequence of primitive values within a stripe, multiple
/// encoded buffers may exist to capture different aspects of the sequence.
///
/// In the shard format specification, `PreparedEncodedBuffer` corresponds to
/// the `EncodedBuffer` element in its "uncommitted" state.
#[must_use]
pub struct PreparedEncodedBuffer {
    /// Non-final `EncodedBuffer` descriptor, referring to the temporary buffer
    /// in the `data` field.
    pub descriptor: shard::EncodedBuffer,
    /// Temporary encoded buffer as `ReadAt`.
    pub data: Arc<dyn ReadAt>,
    /// Size of the data part.
    pub data_size: u64,
    /// Size of the block map part.
    pub block_map_size: u64,
}

impl std::fmt::Debug for PreparedEncodedBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedEncodedBuffer")
            .field("descriptor", &self.descriptor)
            .field("data_size", &self.data_size)
            .field("block_map_size", &self.block_map_size)
            .finish_non_exhaustive()
    }
}
