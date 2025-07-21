//! Blockstream Read Module
//!
//! This module provides functionality for reading and decoding block-based
//! `EncodedBuffers` in the Amudai columnar data format. It includes support for
//! value-level and byte-level buffer access, block map decoding, and generic/primitive
//! buffer handling. The submodules expose the core types and utilities needed to
//! efficiently interpret encoded data streams.

pub mod bit_buffer;
pub mod block_map;
pub mod block_map_decoder;
pub mod block_map_ops;
pub mod block_stream;
pub mod bytes_buffer;
pub mod generic_buffer;
pub mod primitive_buffer;

use crate::read::block_map_ops::BlockDescriptor;
use std::ops::Range;
const EPHEMERAL_BLOCK_SIZE: usize = 256;

pub fn get_ephemeral_block_range(position: u64) -> Range<u64> {
    let ordinal = position / EPHEMERAL_BLOCK_SIZE as u64;
    let logical_start = ordinal * EPHEMERAL_BLOCK_SIZE as u64;
    let logical_end = logical_start + EPHEMERAL_BLOCK_SIZE as u64;
    logical_start..logical_end
}

pub fn get_ephemeral_block_descriptor(position: u64) -> BlockDescriptor {
    let ordinal = position / EPHEMERAL_BLOCK_SIZE as u64;
    let range = get_ephemeral_block_range(position);
    BlockDescriptor {
        logical_range: range.clone(),
        ordinal,
        storage_range: range,
    }
}
