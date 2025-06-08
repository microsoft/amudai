//! Blockstream encoding and decoding for the Amudai storage format.
//!
//! This crate provides APIs for creating and decoding block-based Amudai
//! `EncodedBuffers`.
//!
//! It exposes modules for reading (`read`) and writing (`write`) blockstreams,
//! which are used as the core mechanism for efficient storage and retrieval
//! of Amudai data.
//!
//! # Modules
//! - [`mod@read`]: Functions and types for decoding blockstreams.
//! - [`mod@write`]: Functions and types for encoding blockstreams.

pub mod read;
pub mod write;
