//! Bloom filter support for Amudai with Split-Block Bloom Filter (SBBF) implementation.
//!
//! This crate provides bloom filter functionality that can be used across the Amudai project
//! without creating circular dependencies. It includes:
//!
//! - Core SBBF structures and serialization using XXH3-64 hash function
//! - Builder and decoder implementations optimized for XXH3-64
//! - Utility functions for bloom filter operations

pub mod builder;
pub mod config;
pub mod decoder;

#[cfg(test)]
mod test;

pub use builder::BloomFilterCollector;
pub use config::*;
pub use decoder::SbbfDecoder;
