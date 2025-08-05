//! Index reader infrastructure for querying index data.
//!
//! This module defines the core traits and types for reading and querying
//! indexes in the Amudai system. Index readers provide a uniform interface
//! for different index implementations to execute queries and return position
//! sets that identify matching records.
//!
//! # Overview
//!
//! The index reader system is built around two main components:
//!
//! - [`IndexReader`]: A trait that index implementations must implement to
//!   provide query capabilities
//! - [`IndexQuery`]: An enum that encapsulates different types of queries
//!   that can be executed against an index
//!
//! # Architecture
//!
//! Index readers are typically created by [`IndexType::open_reader`] and are
//! responsible for:
//!
//! 1. Loading and managing index data structures
//! 2. Executing queries efficiently against the index
//! 3. Returning position sets that identify which records match the query

use std::sync::Arc;

use amudai_common::Result;
use amudai_position_set::TernaryPositionSet;

use crate::{IndexType, metadata::IndexMetadata};

/// A trait for reading and querying index data.
///
/// `IndexReader` provides a uniform interface for different index implementations
/// to execute queries and return sets of record positions that match the query
/// criteria. Implementations of this trait handle the details of how to load,
/// cache, and search their specific index structures.
///
/// # Thread Safety
///
/// Index readers must be thread-safe (`Send + Sync`) as they may be shared
/// across multiple query execution contexts. Implementations should use
/// appropriate synchronization primitives if they maintain mutable state.
///
/// # Lifetime
///
/// The `'static` bound ensures that index readers can be stored in long-lived
/// data structures without lifetime complications. Implementations typically
/// achieve this by owning their data or using reference-counted pointers.
pub trait IndexReader: Send + Sync + 'static {
    /// Returns a reference to the index type that created this reader.
    fn index_type(&self) -> &Arc<dyn IndexType>;

    /// Returns the metadata associated with this index.
    ///
    /// The metadata includes information such as:
    /// - Index type identifier
    /// - Capabilities of this index instance
    /// - Index-specific configuration
    /// - Statistics about the indexed data
    fn metadata(&self) -> &IndexMetadata;

    /// Executes a query against the index and returns matching positions.
    ///
    /// This method is the primary interface for querying an index. It takes
    /// an [`IndexQuery`] that encapsulates the query parameters and returns
    /// a [`TernaryPositionSet`] that identifies which records match the query.
    ///
    /// # Arguments
    ///
    /// * `query` - The query to execute against the index
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// - `Ok(TernaryPositionSet)`: A set of record positions that match the
    ///   query. The position set may indicate:
    ///   - Definitely matching positions
    ///   - Definitely non-matching positions
    ///   - Unknown positions (require further evaluation)
    /// - `Err(Error)`: If the query execution fails
    fn query(&self, query: &IndexQuery) -> Result<TernaryPositionSet>;
}

/// Represents a query that can be executed against an index.
///
/// `IndexQuery` is an enum that can encapsulate different types of queries
/// supported by various index implementations.
///
/// **TODO**:
/// Extend and define structured query types:
///  - Term query
///  - Range query
///  - etc.
pub enum IndexQuery {
    /// An opaque binary query whose format is defined by the index type.
    ///
    /// The interpretation of the binary data is entirely up to the index
    /// implementation.
    Opaque(Arc<[u8]>),
}
