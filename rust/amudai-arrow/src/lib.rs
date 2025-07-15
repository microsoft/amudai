//! Amudai Arrow
//!
//! This crate provides utilities for reading Amudai data shards and exposing
//! them as Apache Arrow arrays and record batches. It bridges the Amudai data
//! format with the Arrow ecosystem.

pub mod array_reader;
pub mod builder;
pub mod shard_reader;
