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
