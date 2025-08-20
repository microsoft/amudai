//! High-performance inverted text index for Amudai storage format.
//!
//! This crate provides a complete inverted text index implementation optimized for
//! the Amudai storage format. It supports both writing (indexing) and reading
//! (querying) text data with efficient storage, compression, and retrieval.
//!
//! # Main Entry Points
//!
//! This crate exposes two primary interfaces for text indexing operations:
//!
//! ## Building Indexes: [`write::builder::TextIndexBuilder`]
//!
//! The [`TextIndexBuilder`] is the main entry point for constructing searchable text indexes.
//! It provides a high-level API for processing text data and building optimized indexes
//! with automatic memory management and disk spilling. The builder supports configurable
//! tokenizers, collation strategies, and memory limits for efficient processing of large
//! datasets through a fragment-based architecture with automatic spilling to disk.
//!
//! ## Querying Indexes: [`read::index::TextIndex`]
//!
//! The [`TextIndex`] provides the primary interface for querying existing text indexes.
//! It supports both exact term lookups and prefix matching with efficient streaming results.
//! The index operates on the two-shard architecture and provides memory-efficient access
//! to indexed text data through lazy loading and streaming iterators.
//!
//! # Architecture Overview
//!
//! The text index uses a two-shard architecture:
//! - **Terms Shard**: Contains the B-Tree structure with terms and navigation information
//! - **Positions Shard**: Contains the actual position lists referenced by terms
//!
//! This separation enables optimal compression and query performance by keeping
//! the navigation structure compact while allowing efficient access to detailed
//! position information only when needed.
//!
//! # Supporting Components
//!
//! ## Text Processing
//! - [`tokenizers`]: Text tokenization strategies for different content types
//! - [`collation`]: Text comparison and normalization strategies
//!
//! ## Position Handling
//! - [`pos_list`]: Position list representations with precision/space tradeoffs
//!
//! ## Low-Level APIs
//! - [`mod@write`]: Low-level encoding components and internal builders
//! - [`read`]: Low-level decoding components and internal decoders
//!
//! # Usage Patterns
//!
//! The index is designed for scenarios requiring:
//! - High-volume text indexing with memory efficiency
//! - Fast exact and prefix term lookups
//! - Position-aware text search (phrase queries, proximity)
//! - Horizontally partitioned data (stripes) across multiple fields
//!
//! All operations are designed to work efficiently with the Amudai storage
//! format's columnar layout and compression strategies.

// Re-export main entry points for convenient access
pub use read::index::TextIndex;
pub use write::builder::{TextIndexBuilder, TextIndexBuilderConfig};

pub mod collation;
pub mod pos_list;
pub mod read;
pub mod tokenizers;
pub mod write;
