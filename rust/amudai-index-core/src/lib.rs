//! Core index infrastructure for the Amudai data format.
//!
//! This crate provides the foundational traits and types for implementing
//! various index types that can accelerate queries on Amudai shards.
//!
//! # Overview
//!
//! The index system is built around the [`IndexType`] trait, which defines
//! how different index implementations integrate with the Amudai ecosystem.
//! Index types can be registered with the [`registry`] module and are used
//! to create builders, readers, and maintainers for specific index implementations.
//!
//! # Architecture
//!
//! The index system consists of several key components:
//!
//! - **Index Types**: Implementations of [`IndexType`] that define specific
//!   indexing strategies (e.g., inverted term index, numeric range index)
//! - **Builders**: Created via [`IndexType::create_builder`], responsible for
//!   constructing index artifacts from data frames
//! - **Readers**: Created via [`IndexType::open_reader`], provide read access
//!   to index data for query acceleration
//! - **Maintainers**: Created via [`IndexType::create_maintainer`], handle
//!   index maintenance operations

use std::sync::Arc;

use amudai_common::Result;
use amudai_format::defs::shard;

use crate::{metadata::Capabilities, shard_markers::DynShard};

pub mod builder;
pub mod maintainer;
pub mod metadata;
pub mod reader;
pub mod registry;
pub mod shard_markers;

pub use builder::IndexBuilder;
pub use maintainer::IndexMaintainer;
pub use reader::IndexReader;

/// Defines a type of index that can be created and used with Amudai shards.
///
/// The `IndexType` trait is the central abstraction for index implementations.
/// Each index type defines its own storage format, query capabilities, and
/// processing logic while conforming to this common interface.
///
/// # Implementation Guidelines
///
/// Implementers should:
/// - Provide a unique, stable name via [`name`](Self::name)
/// - Accurately report capabilities via [`capabilities`](Self::capabilities)
/// - Ensure builders, readers, and maintainers are compatible with each other
/// - Handle index-specific properties in the `shard::IndexDescriptor`
///
/// # Thread Safety
///
/// All methods must be thread-safe as index types are shared across threads
/// via `Arc`. The trait requires `Send + Sync + 'static` for this reason.
pub trait IndexType: Send + Sync + 'static {
    /// Returns the unique name of this index type.
    ///
    /// This name is used to:
    /// - Register the index type in the global registry
    /// - Identify the index type in shard metadata
    /// - Match index descriptors to their implementations
    ///
    /// # Examples
    ///
    /// - `"inverted-term-index-v1"`
    /// - `"hashmap-index-v1"`
    ///
    /// The name should be stable across versions and include versioning
    /// information to handle format changes.
    fn name(&self) -> &str;

    /// Returns the capabilities supported by this index type.
    ///
    /// Capabilities indicate what kinds of queries and operations the index
    /// can accelerate. This information helps shard writers and query planners
    /// decide when to use the index.
    ///
    /// See [`Capabilities`] for the full list of supported capabilities.
    fn capabilities(&self) -> &Capabilities;

    /// Creates a new index builder with the specified parameters.
    ///
    /// The builder is responsible for processing data frames and constructing
    /// the index artifacts that will be stored alongside the shard data.
    ///
    /// # Arguments
    ///
    /// * `parameters` - Configuration parameters for index construction,
    ///   including field mappings, temp storage, and index-specific settings
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The parameters are invalid for this index type
    /// - Required resources cannot be allocated
    /// - The index type doesn't support the requested configuration
    fn create_builder(&self, parameters: builder::Parameters) -> Result<Box<dyn IndexBuilder>>;

    /// Opens an index reader for an existing index.
    ///
    /// The reader provides query-time access to the index data, allowing
    /// efficient filtering and searching operations.
    ///
    /// # Arguments
    ///
    /// * `descriptor` - The index descriptor from the shard metadata,
    ///   containing index configuration and artifact locations
    /// * `shard` - Optional reference to the parent shard, which may be
    ///   needed for certain index types that require access to the data
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The descriptor is incompatible with this index type
    /// - Index artifacts cannot be accessed or are corrupted
    /// - Required resources cannot be allocated
    fn open_reader(
        &self,
        descriptor: &shard::IndexDescriptor,
        shard: Option<Box<dyn DynShard>>,
    ) -> Result<Arc<dyn IndexReader>>;

    /// Creates an index maintainer for this index type.
    ///
    /// Maintainers handle ongoing index operations such as:
    /// - Merging indexes from multiple shards
    /// - Updating indexes when data changes
    /// - Optimizing index structures
    /// - Validating index integrity
    ///
    /// Not all index types require maintenance operations, so this method
    /// may return a no-op maintainer.
    ///
    /// # Errors
    ///
    /// Returns an error if the maintainer cannot be created due to
    /// resource constraints or configuration issues.
    fn create_maintainer(&self) -> Result<Box<dyn IndexMaintainer>>;
}
