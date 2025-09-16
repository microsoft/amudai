//! Text index querying and decoding components.
//!
//! This module provides the infrastructure for querying inverted text indexes
//! stored in the Amudai format. It implements the reading side of the two-shard
//! architecture, coordinating between B-Tree navigation and position data retrieval.
//!
//! # Architecture Overview
//!
//! The read components implement efficient query processing:
//! - **High-level Decoder**: [`index::TextIndex`] provides the main query interface
//! - **B-Tree Navigation**: Internal B-Tree decoder handles term lookup and traversal
//! - **Position Retrieval**: Internal positions decoder fetches detailed occurrence data
//! - **Result Streaming**: Iterators provide memory-efficient result processing
//!
//! # Query Types
//!
//! The module supports two primary query patterns:
//! - **Exact Term Lookup**: Find all occurrences of a specific term
//! - **Prefix Matching**: Find all terms that start with a given prefix
//!
//! Both query types return streaming iterators that yield [`TermPositions`] results,
//! providing detailed information about where terms appear in the indexed data.
//!
//! # Performance Characteristics
//!
//! - **Lazy Loading**: Position data is retrieved only when needed
//! - **Memory Efficient**: Streaming results avoid large memory allocations
//! - **Storage Optimized**: Two-shard architecture enables efficient compression
//! - **Collation Aware**: Respects configured comparison and ordering rules

use amudai_format::schema::SchemaId;

use crate::pos_list::PositionList;

mod btree;
mod positions;
pub(crate) mod reader;

/// Represents a single term occurrence with its detailed position information.
///
/// `TermPositions` encapsulates complete information about where a specific term
/// appears within the indexed data, including the data partition (stripe), field
/// identifier, and all positions where the term was found within that field.
///
/// This structure is the primary result type returned by text index queries,
/// providing all necessary information for relevance scoring, highlighting,
/// and other text search operations that require position-aware processing.
///
/// # Usage in Query Results
///
/// Query operations return iterators that yield `TermPositions` instances,
/// with each instance representing one occurrence context (stripe + field)
/// for a given term. Multiple `TermPositions` may be returned for the same
/// term if it appears in different stripes or fields.
#[derive(Debug, Clone)]
pub struct TermPositions {
    /// The data stripe identifier where the term appears.
    ///
    /// Stripes represent horizontal partitions of the data, enabling parallel
    /// processing and efficient storage organization. This field identifies
    /// which specific data partition contains the term occurrences.
    pub _stripe: u16,

    /// The field identifier within the document schema where the term appears.
    ///
    /// This references a specific field in the document schema, allowing
    /// queries to distinguish between occurrences of the same term in
    /// different fields (e.g., title vs. content vs. metadata fields).
    pub _field: SchemaId,

    /// The positions of the term within the specified field.
    ///
    /// Contains all position information for the term within this specific
    /// stripe and field combination. The position list may use different
    /// representations (exact positions, ranges, etc.) depending on the
    /// indexing strategy and data characteristics.
    pub positions: PositionList,
}
