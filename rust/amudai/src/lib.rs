//! # Amudai: Advanced Columnar Storage Format
//!
//! Amudai is a high-performance, open-standard columnar storage format designed for modern
//! analytical workloads. Evolved from the battle-tested internal storage format of Microsoft's
//! Kusto (Azure Data Explorer and Microsoft Fabric EventHouse), Amudai currently powers
//! exabyte-scale data storage infrastructure.
//!
//! ## Key Features
//!
//! * **Multi-modal Data Support**: Efficient analytics over structured, semi-structured (JSON, dynamic types),
//!   unstructured (free text), and embedding vectors
//! * **Advanced Encoding & Compression**: Sophisticated data encoding with specialized optimizations
//!   for various data types and distributions
//! * **Rich Indexing**: Support for inverted term indexes (fast text search), vector similarity indexes,
//!   and predicate pushdown on semi-structured data
//! * **Immutable Design**: Copy-on-write operations, efficient shard merging by referencing existing
//!   data stripes, and simplified caching
//! * **Extensible Architecture**: Support for new data types, metadata structures, and indexes
//! * **Wide Schema Support**: Efficient handling of schemas with many columns
//! * **Tiered Storage**: Hot/cold data artifacts can be placed in different storage locations
//! * **Table Format Integration**: Seamless integration with formats like Apache Iceberg
//!
//! ## Architecture
//!
//! Amudai is built as a modular system with specialized crates for different functionality areas.
//! This main crate serves as a convenient entry point that re-exports all the core components,
//! allowing users to access the full Amudai ecosystem through a single dependency.
//!
//! ## Module Organization
//!
//! The crate is organized into several specialized modules:
//!
//! * [`arrow`] - Apache Arrow integration and compatibility
//! * [`arrow_compat`] - Arrow compatibility utilities
//! * [`blockstream`] - Low-level block-based encoded buffers implementation
//! * [`common`] - Common types and utilities shared across components
//! * [`data_stats`] - Data statistics and profiling
//! * [`encodings`] - Data encoding and compression algorithms
//! * [`amudai_format`] - Core format definitions and structures
//! * [`io`] - I/O abstractions and interfaces
//! * [`io_impl`] - Concrete I/O implementations
//! * [`objectstore`] - Object storage integration
//! * [`sequence`] - Sequence abstractions for columnar data retrieval and manipulation
//! * [`shard`] - Amudai Data shard creation, consumption and management
//! * [`text_index`] - Text indexing and search capabilities
//!
//! ### Support Modules
//!
//! The [`support`] module contains additional utilities:
//!
//! * [`support::budget_tracker`] - Numerical budget tracking utility
//! * [`support::bytes`] - Shared byte buffers
//! * [`support::collections`] - Specialized collection types
//! * [`support::keyed_vector`] - Specialized Key-value vector structure
//! * [`support::arrow_processing`] - Arrow data processing utilities
//!
//! ## Performance Characteristics
//!
//! Amudai is designed for high-performance analytical workloads with:
//!
//! * Optimized columnar layout for analytical queries
//! * Advanced compression reducing storage footprint
//! * Efficient predicate pushdown and filtering
//! * Minimal memory allocation during query execution
//! * Support for vectorized operations
//!
//! ## Getting Started
//!
//! For detailed information about the Amudai format specification, architecture decisions,
//! and implementation details, please refer to the [Amudai book](https://github.com/microsoft/amudai)
//! in the project documentation.
//!
//! ## Comparison to Other Formats
//!
//! Amudai addresses limitations found in existing data lake formats like Apache Parquet,
//! particularly when dealing with:
//!
//! * Complex semi-structured data (JSON, dynamic types)
//! * Full-text search requirements
//! * Vector similarity search
//! * Time series analytics
//! * Graph query patterns
//! * Wide schemas with many columns
//!
//! ## Stability and Compatibility
//!
//! As an evolving format, Amudai maintains backward compatibility while allowing for
//! future extensions. The immutable design ensures that existing data remains accessible
//! as the format evolves.

pub use amudai_arrow as arrow;
pub use amudai_arrow_compat as arrow_compat;
pub use amudai_blockstream as blockstream;
pub use amudai_common as common;
pub use amudai_data_stats as data_stats;
pub use amudai_encodings as encodings;
pub use amudai_format;
pub use amudai_hashmap_index as hashmap_index;
pub use amudai_index_core as index_core;
pub use amudai_io as io;
pub use amudai_io_impl as io_impl;
pub use amudai_objectstore as objectstore;
pub use amudai_sequence as sequence;
pub use amudai_shard as shard;
pub use amudai_spill_data as spill_data;
pub use amudai_text_index as text_index;

pub mod support {
    pub use amudai_arrow_builders as arrow_builders;
    pub use amudai_arrow_builders_macros as arrow_builders_macros;
    pub use amudai_arrow_processing as arrow_processing;
    pub use amudai_bloom_filters as bloom_filters;
    pub use amudai_budget_tracker as budget_tracker;
    pub use amudai_bytes as bytes;
    pub use amudai_collections as collections;
    pub use amudai_common_traits as common_traits;
    pub use amudai_decimal as decimal;
    pub use amudai_keyed_vector as keyed_vector;
    pub use amudai_page_alloc as page_alloc;
    pub use amudai_position_set as position_set;
    pub use amudai_ranges as ranges;
    pub use amudai_shared_vec as shared_vec;
    pub use amudai_value_conversions as value_conversions;
    pub use amudai_workflow as workflow;
}
