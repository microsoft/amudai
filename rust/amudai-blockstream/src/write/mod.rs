//! Block-based encoding for Amudai stripe fields.
//!
//! This module provides the core types and utilities for constructing and encoding
//! block-based `EncodedBuffer` objects, which are the primary storage units for
//! encoded data in the Amudai shard format. It includes support for bit-level and
//! byte-level buffering, block map encoding, and staging of encoded buffers prior
//! to finalization.
//!
//! The main abstraction is [`PreparedEncodedBuffer`], representing an encoded
//! sequence of values (typically for a single field in a stripe) as a series of
//! compressed blocks. This abstraction allows for efficient, staged construction
//! and manipulation of encoded data before it is committed to its final form.
//!
//! # See also
//! - The Amudai shard format specification for details on the `EncodedBuffer` structure.
//! - [`amudai_format::defs::shard::EncodedBuffer`] for the descriptor definition.
//!
//! This module is intended for internal use by Amudai writers and advanced users
//! implementing custom encoding pipelines.

use std::sync::Arc;

use amudai_format::defs::shard;
use amudai_io::ReadAt;

pub mod bit_buffer;
pub mod block_map_encoder;
pub mod block_stream;
pub mod bytes_buffer;
pub mod primitive_buffer;
pub mod staging_buffer;

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
