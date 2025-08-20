//! Te//! # Architecture Overview
//!
//! The write components implement a separation of concerns:
//! - **High-level Builder**: [`builder::TextIndexBuilder`] coordinates the overall process
//! - **B-Tree Construction**: Internal B-Tree encoder manages term navigation structure
//! - **Position Storage**: Internal positions encoder handles detailed occurrence data
//! - **Support Structures**: Internal utilities for page management and data formatting
//!
//! # Usage Pattern
//!
//! The primary entry point is [`builder::TextIndexBuilder`], which provides a simple
//! interface for adding term postings in sorted order. The builder automatically
//! manages the complex details of B-Tree page construction, position encoding,
//! and data distribution across the two-shard architecture.ruction and encoding components.
//!
//! This module provides the infrastructure for building inverted text indexes
//! from term postings. It implements the encoding side of the two-shard
//! architecture, coordinating between B-Tree construction and position storage.
//!
//! # Architecture Overview
//!
//! The write components implement a separation of concerns:
//! - **High-level Builder**: [`builder::TextIndexBuilder`] coordinates the overall process
//! - **B-Tree Construction**: Internal B-Tree encoder manages term navigation structure
//! - **Position Storage**: Internal positions encoder handles detailed occurrence data
//! - **Support Structures**: Internal utilities for page management and data formatting
//!
//! # Usage Pattern
//!
//! The primary entry point is [`builder::TextIndexBuilder`], which provides a simple
//! interface for adding term postings in sorted order. The builder automatically
//! manages the complex details of B-Tree page construction, position encoding,
//! and data distribution across the two-shard architecture.
//!
//! # Internal Components
//!
//! Most components in this module are internal implementation details:
//! - B-Tree page builders and management
//! - Position list optimization and encoding
//! - Page record formatting and serialization
//! - Term dictionary construction utilities
//!
//! These internals are not exposed to maintain a clean public API and allow
//! for implementation changes without breaking compatibility.

use std::cmp::Ordering;

use amudai_format::schema::SchemaId;

pub(crate) mod btree;
pub mod builder;
mod encoder;
mod fragments;
mod page_record;
mod positions;
mod poslist_builder;
mod term_dictionary;

/// Represents a single posting for a term in an inverted text index.
///
/// A term posting is a fundamental data structure in text indexing that records
/// the occurrence of a specific term within a document. Each posting contains
/// the necessary information to locate where a term appears, including the
/// data stripe, field identifier, and position within that field.
///
/// The structure is designed to be compact and efficient for storage and
/// retrieval operations in large-scale text indexing scenarios. Postings
/// are typically sorted by stripe, field, and position to enable efficient
/// range queries and intersection operations.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TermPosting {
    /// The data stripe identifier where the term appears.
    ///
    /// Stripes are used for horizontal partitioning of data to enable
    /// parallel processing and efficient storage organization. This field
    /// identifies which stripe contains the document where the term was found.
    pub stripe: u16,

    /// The field identifier within the document schema where the term appears.
    ///
    /// This references a specific field in the document schema, allowing
    /// the index to distinguish between occurrences of the same term in
    /// different fields (e.g., title vs. content vs. metadata fields).
    pub field: SchemaId,

    /// The position of the term within the specified field.
    ///
    /// This represents the ordinal position of the term occurrence within
    /// the field content, typically counting from zero. Position information
    /// enables phrase queries, proximity searches, and other advanced
    /// text search operations that require knowledge of term ordering.
    pub pos: u32,
}

impl Ord for TermPosting {
    /// Compares two term postings for ordering.
    ///
    /// The ordering is lexicographic, first by stripe, then by field,
    /// and finally by position. This ordering ensures that postings
    /// are grouped by stripe and field, with positions sorted within
    /// each field, which is optimal for index traversal and query
    /// processing operations.
    fn cmp(&self, other: &TermPosting) -> Ordering {
        (self.stripe, self.field, self.pos).cmp(&(other.stripe, other.field, other.pos))
    }
}

impl PartialOrd for TermPosting {
    /// Provides partial ordering comparison for term postings.
    ///
    /// Since TermPosting implements total ordering, this always returns
    /// Some() with the result of the total ordering comparison.
    fn partial_cmp(&self, other: &TermPosting) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A streaming interface for consuming term postings during inverted index operations.
///
/// The `TermPostingSink` trait defines a protocol for processing term postings
/// in a streaming fashion during inverted index construction, merging, or other
/// bulk operations. It provides a structured approach to handle the hierarchical
/// nature of inverted indexes where terms contain multiple postings.
///
/// The interface follows a three-phase protocol for each term:
/// 1. **Start Phase**: Initialize processing for a new term with metadata
/// 2. **Push Phase**: Stream all postings associated with the term
/// 3. **End Phase**: Finalize processing for the current term
///
/// This streaming approach enables memory-efficient processing of large inverted
/// indexes by avoiding the need to materialize entire term posting lists in memory
/// simultaneously. Implementations can incrementally build indexes, perform merges,
/// or apply transformations as data flows through the sink.
///
/// Terms are processed in lexicographic order, and postings within each term
/// are provided in sorted order (by stripe, field, then position).
pub trait TermPostingSink {
    /// Begins processing a new term in the inverted index.
    ///
    /// This method is called once for each term before any of its postings
    /// are provided via `push_posting`. It signals the start of a new term
    /// group and provides essential metadata about the term.
    ///
    /// # Parameters
    ///
    /// * `term_ordinal` - A monotonically increasing identifier for the term
    ///   within the current processing context. This provides a stable ordering
    ///   reference that can be used for term lookups and cross-references.
    ///
    /// * `is_null` - Indicates whether this term represents a null value.
    ///   In text indexing, null terms typically represent missing or undefined
    ///   field values that still need to be tracked for completeness.
    ///
    /// * `term` - The byte representation of the actual term. This is provided
    ///   as a byte slice to support various text encodings and binary term types
    ///   without assuming UTF-8 encoding.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful initialization, or an error if the sink
    /// cannot accept the new term (e.g., due to resource constraints or
    /// validation failures).
    fn start_term(
        &mut self,
        term_ordinal: usize,
        is_null: bool,
        term: &[u8],
    ) -> amudai_common::Result<()>;

    /// Adds a posting to the current term being processed.
    ///
    /// This method is called one or more times between `start_term` and
    /// `end_term` to provide all postings associated with the current term.
    /// Postings are guaranteed to be provided in sorted order according to
    /// the `TermPosting` ordering (stripe, field, position).
    ///
    /// # Important Requirements
    ///
    /// **Every term MUST have at least one posting.** You must call this method
    /// at least once for each term started with `start_term`. Terms without
    /// postings are not valid and may be filtered out or cause errors in some
    /// implementations.
    ///
    /// # Parameters
    ///
    /// * `posting` - A reference to the term posting to be processed. The
    ///   posting contains location information (stripe, field, position)
    ///   indicating where the current term appears in the indexed data.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the posting was successfully processed, or an error
    /// if the sink cannot accept the posting (e.g., due to resource limits,
    /// validation failures, or corruption detection).
    fn push_posting(&mut self, posting: &TermPosting) -> amudai_common::Result<()>;

    /// Finalizes processing for the current term.
    ///
    /// This method is called once after all postings for the current term
    /// have been provided via `push_posting`. It signals the completion of
    /// the current term group and allows the sink to perform any necessary
    /// finalization, validation, or cleanup operations.
    ///
    /// After this method returns successfully, the sink should be ready to
    /// accept a new term via `start_term` or complete processing entirely.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the term was successfully finalized, or an error
    /// if finalization failed (e.g., due to validation errors, write failures,
    /// or resource constraints).
    fn end_term(&mut self) -> amudai_common::Result<()>;
}
