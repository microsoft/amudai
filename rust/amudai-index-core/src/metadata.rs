//! Index metadata definitions for the Amudai indexing system.
//!
//! This module provides the core metadata structures and enums that describe the characteristics
//! and capabilities of indexes in the Amudai system. These types are used throughout the indexing
//! infrastructure to configure index builders, describe index properties, and ensure compatibility
//! between different components.
//!
//! # Overview
//!
//! The module defines several key concepts:
//!
//! - **Scope**: Defines what fields are indexed and at what structural level (stripe or shard)
//! - **Granularity**: Determines the precision of position tracking within indexed data
//! - **Position Fidelity**: Describes the reliability of search results (exact vs approximate)
//! - **Query Types**: Categorizes the different search operations supported by indexes
//! - **Capabilities**: Declares what an index builder can create
//! - **Index Metadata**: Describes the actual properties of a created index
//!
//! # Architecture
//!
//! The metadata system follows a capability-based design where:
//!
//! 1. `IndexType` implementors declare their [`Capabilities`] - what types of indexes they can create
//! 2. Shard writers configure the index builders with the subset of capabilities they need.
//! 3. Created indexes store their actual configuration as [`IndexMetadata`]
//! 4. Index consumers verify their requirements using the same metadata types

use std::ops::Range;

use ahash::AHashSet;
use amudai_format::{defs::shard, schema::BasicType};

/// Defines the scope of what fields are being indexed within a shard or stripe.
///
/// The scope determines which fields the index covers and at what level
/// (stripe or shard). This is used to configure indexes that operate on
/// specific fields within the data structure.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Scope {
    /// Index covers a single field within a stripe.
    ///
    /// This is the smallest field-level scope, indexing one field
    /// in the context of a single stripe.
    StripeField(shard::IndexedField),

    /// Index covers multiple fields within stripes.
    ///
    /// Allows indexing across multiple fields at the stripe level.
    MultiStripeField(Vec<shard::IndexedField>),

    /// Index covers a single field at the shard level.
    ///
    /// Indexes the field across the entire shard, providing
    /// shard-wide field access.
    ShardField(shard::IndexedField),

    /// Index covers multiple fields at the shard level.
    ///
    /// Enables multi-field indexing across the entire shard,
    /// supporting scenarios like full-text search.
    MultiShardField(Vec<shard::IndexedField>),
}

/// Represents the type of scope for indexed values.
///
/// This enum provides a simplified categorization of the `Scope` enum,
/// useful for capability declarations and type checking without
/// needing the actual field references.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScopeKind {
    /// Single field within a stripe
    StripeField,
    /// Multiple fields within a stripe
    MultiStripeField,
    /// Single field across the shard
    ShardField,
    /// Multiple fields across the shard
    MultiShardField,
}

/// Defines the level of detail for an inverted index's position list.
///
/// This determines at what structural level a "hit" is recorded when a value
/// is found within a nested data structure. The granularity affects both
/// the precision of search results and the storage requirements of the index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Granularity {
    /// The finest granularity level.
    ///
    /// The index will store the logical position of the individual *value*
    /// within its own field. This provides the most precise location information
    /// but requires the most storage space.
    ///
    /// # Example
    /// For the data `[[a, b], [c, a]]`, an index for 'a' at `Element`
    /// granularity would point to positions (0) and (3), representing logical
    /// positions in the flattened leaf value field.
    Element,

    /// An intermediate granularity level.
    ///
    /// The index will store the logical position of the *parent list* that
    /// contains the value. All occurrences of a value within the same list
    /// resolve to a single positional entry. This balances precision with
    /// storage efficiency.
    ///
    /// # Example
    /// For `[[a, b], [c, a]]`, an index for 'a' at `List` granularity
    /// would point to positions (0) and (1), representing the two inner lists.
    List,

    /// The coarsest granularity level.
    ///
    /// The index will store the logical position of the *top-level record* (or row)
    /// that contains the value. This is useful for simply finding which records
    /// contain a value, regardless of where or how many times. This provides
    /// the most compact index at the cost of precision.
    ///
    /// # Example
    /// For a frame with two records `r1 = [[a, b]]` and `r2 = [[c, d]]`,
    /// an index for 'a' at `Record` granularity would just point to record `r1`.
    Record,
}

/// Describes the reliability of the positions returned by a search query.
///
/// When an index (e.g., inverted index) returns a list of positions for a query
/// (e.g., a term), the nature of that list can vary based on the index's configuration
/// and implementation. Some configurations guarantee that every returned position
/// is a true match, while others (often for performance or space-saving reasons, like
/// using Bloom filters) return a list of candidates that may include false positives.
///
/// Depending on the `PositionFidelity`, the consumer of the search results may need
/// to perform a post-processing verification step to filter out false positives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PositionFidelity {
    /// Every position in the returned list is guaranteed to satisfy the predicate.
    ///
    /// Indicates that every position in the returned list is guaranteed to satisfy
    /// the predicate of the index query. The positions are precise and can be used
    /// directly without further checks. This represents a high-fidelity result that
    /// trades storage space or build time for query accuracy.
    Exact,

    /// The returned list of positions may contain false positives.
    ///
    /// Indicates that the returned list of positions may contain false positives.
    /// Each position is a **candidate** and must be verified against the original
    /// source value (e.g., to confirm the presence of the term). This represents a
    /// low-fidelity result that requires a verification step but offers space or
    /// performance benefits.
    Approximate,

    /// The returned position list contains both exact and approximate positions.
    ///
    /// The returned position list may be a mix of `Exact` and `Approximate` kinds.
    /// This hybrid approach might be used when different parts of the index have
    /// different fidelity characteristics.
    Mixed,
}

/// Defines the types of queries that can be performed on an index.
///
/// This enum categorizes the different query operations supported by index
/// implementations. Each variant represents a specific type of search pattern
/// or operation that can be executed against the indexed data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryKind {
    /// Exact match query for a single term.
    ///
    /// Searches for documents containing an exact match of the specified term.
    /// This is the most basic and common query type.
    Term,

    /// Exact match query for any term in a set.
    ///
    /// Searches for documents containing at least one term from a provided set
    /// of terms. Equivalent to an OR operation across multiple terms.
    TermSet,

    /// Prefix match query for terms.
    ///
    /// Searches for documents containing terms that start with the specified
    /// prefix. Useful for autocomplete and wildcard-like searches.
    TermPrefix,

    /// Prefix match query for any prefix in a set.
    ///
    /// Searches for documents containing terms that start with any of the
    /// prefixes in the provided set. Combines multiple prefix searches.
    TermPrefixSet,

    /// Range query for lexicographic term ordering.
    ///
    /// Searches for documents containing terms that fall within a specified
    /// lexicographic range (e.g., all terms between "apple" and "banana").
    TermRange,

    /// Range query for numeric values.
    ///
    /// Searches for documents containing numeric values within a specified
    /// range (e.g., all values between 10 and 100). Requires numeric indexing.
    NumericRange,

    /// Hash-based lookup query.
    ///
    /// Uses hash values for fast exact match lookups. Typically used for
    /// optimized equality searches where hash collisions are handled.
    Hash,

    /// Custom query type.
    ///
    /// Allows for implementation-specific query types that don't fit into
    /// the standard categories. The interpretation depends on the index
    /// implementation.
    Custom,
}

/// Describes the capabilities supported by an index builder.
///
/// This struct defines what types of indexes can be built by a particular
/// index builder implementation. It specifies the supported scopes, granularities,
/// position fidelities, and data types that the builder can handle.
#[derive(Debug, Clone)]
pub struct Capabilities {
    /// Set of supported scope kinds for indexing.
    ///
    /// Defines whether the index builder can create indexes at the stripe field,
    /// multi-stripe field, shard field, or multi-shard field level.
    pub scope: AHashSet<ScopeKind>,

    /// Set of supported granularity levels.
    ///
    /// Specifies whether the index builder supports Element, List, or Record
    /// level granularity for position tracking.
    pub granularity: AHashSet<Granularity>,

    /// Set of supported position fidelity levels.
    ///
    /// Indicates whether the builder can create indexes with Exact, Approximate,
    /// or Mixed fidelity for search results.
    pub fidelity: AHashSet<PositionFidelity>,

    /// Set of basic data types that can be indexed.
    ///
    /// Specifies which [`BasicType`] values (e.g., String, Int64, Float64) the
    /// index builder can process and index effectively.
    pub data_types: AHashSet<BasicType>,

    /// Set of supported query types.
    ///
    /// Specifies which [`QueryKind`] operations (e.g., Term, TermPrefix, NumericRange)
    /// the index builder can handle. This determines what types of searches can be
    /// performed against indexes created by this builder.
    pub query_kind: AHashSet<QueryKind>,

    /// A range that specifies the minimum (inclusive) and maximum (exclusive) size
    /// of the value that can be processed by the index builder. This is relevant mostly
    /// for variable-sized data types (`String` and `Binary`).
    pub value_size: Range<usize>,
}

/// Contains metadata about a stored index.
///
/// This struct holds the configuration and properties of an index after it has
/// been created. It describes the actual characteristics of a specific index
/// instance, as opposed to the capabilities of an index builder.
#[derive(Debug, Clone)]
pub struct IndexMetadata {
    /// The actual scope of fields covered by this index.
    ///
    /// Specifies which fields are indexed and at what level (stripe or shard).
    pub scope: Scope,

    /// The granularity level used by this index.
    ///
    /// Indicates whether positions are tracked at the Element, List, or Record level.
    pub granularity: Granularity,

    /// The fidelity of positions returned by this index.
    ///
    /// Specifies whether search results are Exact, Approximate, or Mixed.
    pub fidelity: PositionFidelity,

    /// Set of supported query types.
    ///
    /// Specifies which [`QueryKind`] operations (e.g., Term, TermPrefix, NumericRange)
    /// the index can handle.
    pub query_kind: AHashSet<QueryKind>,

    /// Additional metadata stored as name-value pairs.
    ///
    /// This flexible storage allows index implementations to store custom
    /// metadata that doesn't fit into the standard fields. Examples might
    /// include configuration parameters, statistics, or version information.
    pub bag: Vec<amudai_format::defs::common::NameValuePair>,
}
