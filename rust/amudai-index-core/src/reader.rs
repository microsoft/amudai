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

    /// A field hashing index query for exact field value lookups.
    ///
    /// This query type is used with hash-based indexes to perform efficient
    /// exact-match queries on specific fields. The hash index provides O(1)
    /// average-case lookup performance for equality comparisons.
    FieldHashing(FieldHashingQuery),
}

/// Parameters for a field hashing index query.
///
/// This struct encapsulates the parameters needed to perform an exact field
/// value lookup using a hash-based index.
pub struct FieldHashingQuery {
    /// Hierarchical path to the field being queried.
    ///
    /// Represented as a sequence of path segments that must match the indexed
    /// leaf path exactly (case-sensitive, as defined in the schema).
    ///
    /// Path rules and examples:
    /// - Top-level field: `["user_id"]`
    /// - Nested struct: `["user", "profile", "id"]`
    /// - Lists/arrays: include the special segment `"item"` to address list elements.
    ///   For a list field `events: List<Struct{ type: String }>` use
    ///   `["events", "item", "type"]`. A match returns positions where any list item
    ///   equals the provided value.
    /// - The path must point to a leaf of a supported primitive type (see `value`).
    ///
    /// Important:
    /// - Field names are case-sensitive and must match the schema exactly.
    /// - The path must correspond to a field that was indexed; otherwise lookups will
    ///   simply return no matches.
    pub path: Arc<Vec<Arc<str>>>,

    /// The field value to search for, encoded as bytes.
    ///
    /// Use the canonical binary encoding for the field's logical type:
    /// - Null: empty byte slice (length 0)
    /// - Boolean: a single ASCII byte — `b'0'` for false, `b'1'` for true
    /// - Integers (Int8/16/32/64; signedness as defined by the field): little-endian
    ///   two's-complement bytes of the appropriate width
    /// - String: UTF-8 bytes (no null-termination)
    /// - GUID: 16 raw bytes as stored
    /// - Binary: raw bytes as stored
    /// - FixedSizeBinary(N): exactly N raw bytes as stored
    /// - DateTime: i64 timestamp bytes in little-endian format
    ///
    /// Unsupported in field-hashing: floating-point (f32/f64) and complex container types
    /// (Map, Union, nested non-leaf). Providing values for unsupported types will not match
    /// any indexed keys.
    ///
    /// Caution: both a null value and an empty string encode to an empty byte slice, and are
    /// therefore indistinguishable for lookup purposes in this index.
    pub value: Arc<[u8]>,

    /// The exclusive upper bound of the positions domain: positions are in [0, span).
    ///
    /// What this is:
    /// - Defines the size of the logical space for returned positions. It is passed
    ///   through to the underlying index lookup and used to construct the resulting
    ///   PositionSet/TernaryPositionSet. Internally, positions >= `span` are ignored.
    ///
    /// How to choose it:
    /// - For record-granularity field-hashing indexes, set this to the total number of
    ///   records in the target data domain (typically the shard/batch being queried).
    ///   In other words, `span` should be 1 + max_possible_position for that domain.
    ///
    /// Notes:
    /// - This is required by the current API and is forwarded to
    ///   HashmapIndex::lookup, which then builds a PositionSet via
    ///   PositionSet::from_positions(span, positions_iter).
    pub span: u64,
}
