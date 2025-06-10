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

use amudai_common::{Result, verify_arg};
use amudai_format::defs::{
    common::DataRef,
    shard::{self, BufferKind},
};
use amudai_io::{ReadAt, temp_file_store::TemporaryWritable};

use crate::{
    read::block_stream::{BlockReaderPrefetch, BlockStreamDecoder},
    write::block_stream::BlockStreamEncoder,
};

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

impl PreparedEncodedBuffer {
    /// Concatenates multiple `PreparedEncodedBuffer` sources into a single consolidated buffer.
    ///
    /// This method combines encoded blocks from multiple source buffers into a single buffer,
    /// preserving the order of sources and maintaining the internal structure of each block.
    /// The source buffers typically represent data for the same field from multiple temporary
    /// stripes (e.g., created in parallel processing) that are being consolidated into a
    /// final stripe.
    ///
    /// The concatenation process:
    /// 1. Validates source compatibility using [`Self::verify_concatenated_sources`]
    /// 2. Reads all blocks from each source buffer in sequence order
    /// 3. Verifies block checksums during the copy process
    /// 4. Writes blocks to the target buffer without re-encoding
    /// 5. Constructs a new consolidated block map covering the entire value range
    /// 6. Creates a new descriptor with the same encoding properties as the sources
    ///
    /// The resulting buffer contains all values from the source buffers in the order
    /// they appear in the `sources` slice, with value positions adjusted to form a
    /// contiguous sequence.
    ///
    /// # Limitations
    ///
    /// This method is suitable only for fields that do not have child offsets as part of
    /// their representation (i.e., this is **not suitable for List and Map data types**).
    /// For fields with complex nested structures, concatenation would invalidate the offset
    /// relationships between parent and child elements.
    ///
    /// # Arguments
    ///
    /// * `sources` - A slice of `PreparedEncodedBuffer` instances to concatenate. All sources
    ///   must be compatible (same buffer kind, checksum settings, and encoding flags).
    ///   The sources will be concatenated in the order they appear in the slice.
    /// * `writer` - A temporary storage writer for the concatenated buffer. Must be a fresh
    ///   writer starting at position 0.
    ///
    /// # Returns
    ///
    /// Returns a `Result<PreparedEncodedBuffer>` containing the concatenated buffer if successful.
    /// The resulting buffer will have:
    /// - All blocks from the source buffers concatenated in sequence order
    /// - A consolidated block map covering the entire value range across all sources
    /// - The same encoding properties (buffer kind, checksums, presence/offset flags) as the sources
    /// - Updated value counts reflecting the combined data from all sources
    ///
    /// # Errors
    ///
    /// Returns errors if:
    /// - Source validation fails (see [`Self::verify_concatenated_sources`] for compatibility rules)
    /// - I/O errors occur while reading from sources or writing to the target
    /// - Block checksum verification fails for any source block
    /// - Block map construction fails due to size constraints or invalid block data
    pub fn concatenate(
        sources: &[&PreparedEncodedBuffer],
        writer: Box<dyn TemporaryWritable>,
    ) -> Result<PreparedEncodedBuffer> {
        Self::verify_concatenated_sources(sources)?;

        let mut block_stream = BlockStreamEncoder::new(writer);
        for source in sources {
            let decoder = BlockStreamDecoder::from_prepared_buffer(source)?;
            let block_count =
                u32::try_from(decoder.block_map().block_count()?).expect("block_count");
            let last_pos = decoder.block_map().value_count()?;
            let mut reader = decoder.create_reader_with_position_ranges(
                std::iter::once(0..last_pos),
                BlockReaderPrefetch::Enabled,
            )?;

            for block_idx in 0..block_count {
                let block = reader.read_block(block_idx)?;
                block.verify_checksum()?;
                block_stream.push_block(&block.raw_data, block.descriptor.logical_size())?;
            }
        }

        let blocks = block_stream.finish()?;

        let buffer = PreparedEncodedBuffer {
            descriptor: shard::EncodedBuffer {
                kind: sources[0].descriptor.kind,
                buffer: Some(DataRef::new("", 0..blocks.total_size())),
                block_map: Some(DataRef::new("", blocks.data_size()..blocks.total_size())),
                block_count: Some(blocks.block_count),
                block_checksums: sources[0].descriptor.block_checksums,
                embedded_presence: sources[0].descriptor.embedded_presence,
                embedded_offsets: sources[0].descriptor.embedded_offsets,
                buffer_id: None,
                packed_group_index: None,
            },
            data_size: blocks.data_size,
            block_map_size: blocks.block_map_size,
            data: blocks.block_stream,
        };
        Ok(buffer)
    }

    /// Validates that the provided source buffers are compatible for concatenation through
    /// [`Self::concatenate`].
    ///
    /// This method performs comprehensive validation to ensure that all source buffers
    /// can be safely concatenated without creating inconsistent or invalid encoded data.
    /// The validation rules ensure that the concatenated buffer will maintain the encoding
    /// properties and structural integrity expected by consumers.
    ///
    /// # Validation Rules
    ///
    /// All source buffers must satisfy the following compatibility requirements:
    ///
    /// 1. **Non-empty sources**: At least one source buffer must be provided
    /// 2. **Compatible buffer kinds**: All sources must have the same `BufferKind`
    ///    (either all `Data` or all `Presence` buffers)
    /// 3. **Supported buffer types**: Only `Data` and `Presence` buffer kinds are
    ///    supported for concatenation operations (other kinds like `Offsets` are not supported)
    /// 4. **Consistent checksum settings**: All sources must have the same checksum
    ///    configuration (`block_checksums` flag must match across all sources)
    /// 5. **Consistent presence encoding**: All sources must have the same embedded
    ///    presence configuration (`embedded_presence` flag must match)
    /// 6. **Consistent offset encoding**: All sources must have the same embedded
    ///    offsets configuration (`embedded_offsets` flag must match)
    ///
    /// # Why Validation is Required
    ///
    /// These compatibility checks are essential because:
    /// - **Buffer kind consistency**: Mixing different buffer types would produce
    ///   semantically invalid results (e.g., concatenating data and presence buffers)
    /// - **Checksum consistency**: Mixed checksum settings would create buffers
    ///   where some blocks have checksums and others don't, violating format expectations
    /// - **Encoding consistency**: Different presence or offset encoding settings
    ///   would result in inconsistent block structure that consumers couldn't interpret
    /// - **Structural integrity**: Concatenation assumes all sources use the same
    ///   encoding scheme and can be combined without breaking logical relationships
    ///
    /// # Arguments
    ///
    /// * `sources` - A slice of `PreparedEncodedBuffer` instances to validate for
    ///   concatenation compatibility. The validation uses the first source as the reference
    ///   for compatibility checks against all other sources.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if all sources are compatible for concatenation.
    ///
    /// # Errors
    ///
    /// Returns errors if any validation rule is violated.
    pub fn verify_concatenated_sources(sources: &[&PreparedEncodedBuffer]) -> Result<()> {
        verify_arg!(sources, !sources.is_empty());

        let buffer_kind = sources[0].descriptor.kind;
        verify_arg!(
            sources,
            buffer_kind == BufferKind::Data as i32 || buffer_kind == BufferKind::Presence as i32
        );
        verify_arg!(
            sources,
            sources.iter().all(|s| s.descriptor.kind == buffer_kind)
        );

        verify_arg!(sources, sources.iter().all(|s| s.block_map_size != 0));

        let checksums = sources[0].descriptor.block_checksums;
        verify_arg!(
            sources,
            sources
                .iter()
                .all(|s| s.descriptor.block_checksums == checksums)
        );

        let presence = sources[0].descriptor.embedded_presence;
        verify_arg!(
            sources,
            sources
                .iter()
                .all(|s| s.descriptor.embedded_presence == presence)
        );

        let offsets = sources[0].descriptor.embedded_offsets;
        verify_arg!(
            sources,
            sources
                .iter()
                .all(|s| s.descriptor.embedded_offsets == offsets)
        );
        Ok(())
    }
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
