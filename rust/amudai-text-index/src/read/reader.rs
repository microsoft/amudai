//! Text index decoder providing high-level querying interface.
//!
//! This module implements the primary reader interface for querying inverted text indexes
//! stored in the Amudai format. It coordinates between the B-Tree term navigation and
//! position data retrieval to provide efficient text search capabilities.

use std::sync::Arc;

use amudai_common::{Result, error::Error};
use amudai_format::defs::shard::IndexDescriptor;
use amudai_index_core::{
    IndexReader, IndexType,
    metadata::{Granularity, IndexMetadata, PositionFidelity, QueryKind, Scope},
    reader::IndexQuery,
};
use amudai_objectstore::{ObjectStore, ReferenceResolver};
use amudai_position_set::TernaryPositionSet;

use crate::collation;
use crate::read::TermPositions;
use crate::read::btree::{BTreeEntry, ByPrefixEntryIterator, EntryIterator};
use crate::read::{btree::BTreeDecoder, positions::PositionsDecoder};

/// High-level interface for querying inverted text indexes.
///
/// The `TextIndex` provides the primary entry point for text search operations
/// against indexes stored in the Amudai format. It manages the coordination
/// between term lookup (via B-Tree navigation) and position data retrieval.
///
/// # Architecture
///
/// The index operates on a two-shard architecture:
/// - **Terms Shard**: Contains B-Tree pages with term navigation and references
/// - **Positions Shard**: Contains the actual position lists for memory efficiency
///
/// This design enables fast term lookups while keeping detailed position information
/// separate and efficiently compressed.
///
/// # Usage
///
/// The index supports two primary query patterns:
/// - **Exact Term Lookup**: Find all occurrences of a specific term
/// - **Prefix Matching**: Find all terms that start with a given prefix
///
/// Both operations return iterators that stream results efficiently without
/// requiring large amounts of memory for intermediate storage.
pub struct TextIndex {
    /// B-Tree decoder for term navigation and lookup operations.
    btree: BTreeDecoder,
    /// Position decoder for retrieving detailed occurrence information.
    positions: PositionsDecoder,
}

impl TextIndex {
    /// Creates a new text index reader from the specified shard URLs.
    ///
    /// This method initializes both the B-Tree decoder for term navigation and the
    /// positions decoder for retrieving detailed occurrence information. The collation
    /// strategy determines how terms are compared and ordered during lookups.
    ///
    /// # Arguments
    ///
    /// * `terms_shard_url` - URL to the shard containing B-Tree structure and term metadata
    /// * `positions_shard_url` - URL to the shard containing compressed position lists
    /// * `object_store` - Storage backend for accessing the shards
    /// * `collation` - Collation strategy name for term comparison (e.g., "utf8_general_ci", "binary")
    ///
    /// # Returns
    ///
    /// A new `TextIndex` instance ready for querying, or an error if initialization fails.
    ///
    /// # Shard Architecture
    ///
    /// The two-shard design separates concerns for optimal performance:
    /// - **Terms Shard**: Contains B-Tree pages with term strings, metadata, and position references
    /// - **Positions Shard**: Contains compressed position lists referenced by the terms shard
    ///
    /// This separation enables efficient term navigation without loading position data until needed.
    ///
    /// # Collation Support
    ///
    /// The collation parameter controls term comparison behavior:
    /// - **Case Sensitivity**: Determines if "Term" matches "term"
    /// - **Locale Rules**: Handles language-specific sorting and comparison
    /// - **Unicode Normalization**: Ensures consistent handling of composed characters
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The collation strategy is not recognized or supported
    /// - Either shard URL is malformed or inaccessible
    /// - The shard schemas are incompatible with the expected text index format
    /// - Storage access permissions are insufficient
    /// - Network connectivity issues prevent shard access
    pub fn new(
        terms_shard_url: String,
        positions_shard_url: String,
        object_store: Arc<dyn ObjectStore>,
        collation: &str,
    ) -> Result<Self> {
        let collation = collation::create_collation(collation)?;
        let btree = BTreeDecoder::new(terms_shard_url, Arc::clone(&object_store), collation)?;
        let positions = PositionsDecoder::new(positions_shard_url, object_store)?;
        Ok(Self { btree, positions })
    }

    /// Performs an exact term lookup and returns an iterator over matching positions.
    ///
    /// This method searches for occurrences of the exact specified term within the
    /// indexed data. Term matching is performed according to the configured collation
    /// strategy, which may perform case-insensitive or locale-specific comparisons.
    ///
    /// # Arguments
    ///
    /// * `term` - The exact term to search for (will be compared using the configured collation)
    ///
    /// # Returns
    ///
    /// An iterator that yields `TermPositions` for each occurrence of the term,
    /// or an error if the lookup operation fails.
    ///
    /// # Lookup Process
    ///
    /// The method performs:
    /// 1. **B-Tree Navigation**: Uses the configured collation to locate matching terms
    /// 2. **Entry Filtering**: Returns only entries that exactly match the search term
    /// 3. **Lazy Position Loading**: Position data is loaded on-demand as iterator advances
    ///
    /// # Collation Behavior
    ///
    /// Term matching respects the collation strategy:
    /// - **Case Sensitivity**: Depends on collation (e.g., "ci" vs "cs" suffixes)
    /// - **Unicode Normalization**: Handles composed/decomposed character equivalence  
    /// - **Locale Rules**: Applies language-specific comparison rules
    ///
    /// # Performance
    ///
    /// - **B-Tree Efficiency**: O(log n) term location using balanced tree navigation
    /// - **Streaming Results**: Memory usage scales with position list size, not result count
    /// - **Early Termination**: Iteration can stop without processing remaining matches
    /// - **Cache Friendly**: Sequential access patterns for position data retrieval
    pub fn lookup_term(&self, term: &str) -> Result<TermPositionsIterator<'_, EntryIterator<'_>>> {
        Ok(TermPositionsIterator {
            entries_iter: self.btree.lookup_term(term)?,
            positions: &self.positions,
        })
    }

    /// Performs a prefix search and returns an iterator over all matching terms.
    ///
    /// This method finds all indexed terms that begin with the specified prefix.
    /// The search uses the configured collation strategy for prefix matching,
    /// enabling case-insensitive or locale-aware prefix queries.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix to search for (matching uses the configured collation)
    ///
    /// # Returns
    ///
    /// An iterator that yields `TermPositions` for each occurrence of terms
    /// matching the prefix, or an error if the lookup operation fails.
    ///
    /// # Search Process
    ///
    /// The method performs:
    /// 1. **Range Identification**: Finds the range of terms that start with the prefix
    /// 2. **B-Tree Range Scan**: Efficiently iterates through the matching term range
    /// 3. **Collation-Aware Matching**: Applies collation rules to prefix comparison
    /// 4. **Lazy Position Loading**: Position data is loaded on-demand during iteration
    ///
    /// # Ordering
    ///
    /// Results are returned in lexicographic order according to the configured collation:
    /// - **Term Order**: Terms are sorted by their collated comparison value
    /// - **Position Order**: Within each term, positions are ordered by (stripe, field, position)
    /// - **Deterministic**: Same prefix query always returns results in the same order
    ///
    /// # Performance
    ///
    /// - **Range Scan Efficiency**: Uses B-Tree structure to locate prefix range without full scan
    /// - **Memory Efficiency**: Streams results without buffering all matching terms
    /// - **Early Termination**: Can stop iteration without processing remaining matches
    /// - **Collation Optimization**: Leverages indexed collation keys for fast comparison
    ///
    /// # Use Cases
    ///
    /// Prefix searches enable:
    /// - **Auto-completion**: Find terms for user input completion
    /// - **Wildcard Queries**: Implement "term*" style searches  
    /// - **Fuzzy Matching**: Combined with edit distance for approximate search
    /// - **Faceted Search**: Explore term space within a specific namespace
    pub fn lookup_term_prefix(
        &self,
        prefix: &str,
    ) -> Result<TermPositionsIterator<'_, ByPrefixEntryIterator<'_>>> {
        Ok(TermPositionsIterator {
            entries_iter: self.btree.lookup_term_prefix(prefix)?,
            positions: &self.positions,
        })
    }
}

/// Iterator that yields term positions by coordinating B-Tree entries with position data.
///
/// This iterator provides a streaming interface for retrieving detailed position information
/// from text index lookups. It operates by combining B-Tree navigation results with
/// position data retrieval, ensuring memory-efficient processing of large result sets.
///
/// # Type Parameters
///
/// * `I` - The underlying iterator type that yields B-Tree entries (either `EntryIterator`
///         for exact matches or `ByPrefixEntryIterator` for prefix matches)
///
/// # Lazy Loading Architecture
///
/// Position data is retrieved lazily as the iterator is consumed. This approach provides:
/// - **Memory Efficiency**: Only current position data is held in memory
/// - **Early Termination**: Queries can stop without loading remaining data
/// - **Streaming Processing**: Large result sets can be processed incrementally
/// - **Storage Optimization**: Avoids bulk reads when only partial results are needed
///
/// # Data Flow
///
/// Each iteration step performs:
/// 1. Retrieves the next B-Tree entry containing term metadata
/// 2. Uses the entry's `repr_type` and `range` to locate position data  
/// 3. Reads and decompresses the position list from the positions shard
/// 4. Combines metadata (stripe, field) with positions into `TermPositions`
///
/// # Error Handling
///
/// The iterator may yield errors during iteration if position data cannot be retrieved
/// or if there are issues accessing the underlying storage. Common error scenarios:
/// - **Storage Failures**: Network issues, permission problems, missing files
/// - **Data Corruption**: Invalid position data format or compression errors
/// - **Resource Exhaustion**: Memory allocation failures during decompression
///
/// Callers should handle these errors appropriately based on their fault tolerance
/// requirements and may choose to continue processing remaining results.
pub struct TermPositionsIterator<'a, I>
where
    I: Iterator<Item = Result<BTreeEntry>>,
{
    /// Iterator over B-Tree entries containing term metadata and position references.
    entries_iter: I,
    /// Decoder for retrieving position lists from the positions shard.
    positions: &'a PositionsDecoder,
}

impl<'a, I> Iterator for TermPositionsIterator<'a, I>
where
    I: Iterator<Item = Result<BTreeEntry>>,
{
    type Item = Result<TermPositions>;

    /// Advances the iterator and returns the next term position information.
    ///
    /// This method retrieves the next B-Tree entry and lazily loads the corresponding
    /// position data from the positions shard. Each yielded item contains complete
    /// information about where a term appears, including stripe, field, and detailed
    /// position information.
    ///
    /// # Returns
    ///
    /// * `Some(Ok(TermPositions))` - Successfully retrieved term position information
    /// * `Some(Err(...))` - Error occurred during B-Tree navigation or position retrieval  
    /// * `None` - No more results available (end of iteration)
    ///
    /// # Data Retrieval Process
    ///
    /// For each successful iteration:
    /// 1. Gets the next `BTreeEntry` from the underlying B-Tree navigation
    /// 2. Extracts `repr_type` and `range` fields to locate position data
    /// 3. Calls `positions.read()` to retrieve and decompress position list
    /// 4. Combines stripe/field metadata with positions into `TermPositions`
    ///
    /// # Error Conditions
    ///
    /// Errors may occur due to:
    /// - **B-Tree Navigation**: Corrupted B-Tree structure or storage access failures
    /// - **Position Retrieval**: Invalid range references or missing position data
    /// - **Decompression**: Corrupted compressed data or unsupported encoding
    /// - **Storage Access**: Network failures, permission issues, or missing files
    /// - **Memory**: Resource exhaustion during position list processing
    ///
    /// # Performance Characteristics
    ///
    /// - **Lazy Loading**: Position data is only retrieved when iterator advances
    /// - **Memory Efficiency**: Only current position list is held in memory
    /// - **Streaming**: Large result sets can be processed without buffering
    /// - **Early Termination**: Iteration can stop without loading remaining data
    fn next(&mut self) -> Option<Self::Item> {
        match self.entries_iter.next() {
            Some(Ok(entry)) => match self.positions.read(entry.repr_type, entry.range) {
                Err(e) => Some(Err(e)),
                Ok(pos_list) => Some(Ok(TermPositions {
                    _stripe: entry.stripe,
                    _field: entry.field,
                    positions: pos_list,
                })),
            },
            Some(Err(e)) => Some(Err(e)),
            _ => None,
        }
    }
}

/// Index reader implementation for text indexes.
///
/// The `TextIndexReader` implements the `IndexReader` trait and provides querying capabilities
/// for text indexes stored in the Amudai format. It wraps the lower-level `TextIndex` struct
/// and converts its results to the position sets expected by the index infrastructure.
///
/// This reader supports text-based queries such as exact term lookups and prefix searches,
/// converting the detailed position information from the text index into `TernaryPositionSet`
/// results that can be used by query planners and other index consumers.
///
/// # Thread Safety
///
/// The reader stores immutable references to the underlying text index components and
/// index metadata. The actual storage access is handled by the `ObjectStore` which manages
/// its own thread safety requirements. Multiple concurrent queries can be executed safely.
///
/// # Query Semantics
///
/// All multi-term queries (TermSet, TermPrefixSet) use OR semantics - a position matches
/// if it contains any of the specified terms or prefixes. This allows for efficient
/// keyword searching and flexible text matching patterns.
///
/// # Supported Query Types
///
/// - **Term**: Exact match for a single term
/// - **TermPrefix**: Prefix match for a single prefix  
/// - **TermSet**: Exact match for any term in a set (OR semantics)
/// - **TermPrefixSet**: Prefix match for any prefix in a set (OR semantics)
pub struct TextIndexReader {
    /// The underlying text index that performs the actual querying.
    text_index: TextIndex,
    /// Reference to the index type that created this reader
    index_type: Arc<dyn IndexType>,
    /// Metadata describing this index's characteristics
    metadata: IndexMetadata,
}

impl TextIndexReader {
    /// Opens a new text index reader from the provided descriptor and object store.
    ///
    /// This method creates a new text index reader by parsing the index descriptor,
    /// extracting the required artifacts (terms and positions shards), and initializing
    /// the underlying text index components.
    ///
    /// # Arguments
    ///
    /// * `index_type` - The index type that should match the descriptor's type
    /// * `descriptor` - Index descriptor containing metadata and artifact references
    /// * `object_store` - Storage backend for accessing the index data
    /// * `_reference_resolver` - Currently unused reference resolver (for future extensibility)
    ///
    /// # Returns
    ///
    /// A new `TextIndexReader` instance ready for querying, or an error if opening fails.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The descriptor's index type doesn't match the provided index type
    /// - Required collation property is missing or invalid
    /// - Insufficient artifacts are provided (needs at least terms and positions shards)
    /// - Any artifact reference is invalid or inaccessible
    /// - The underlying text index cannot be initialized
    pub fn open(
        index_type: Arc<dyn IndexType>,
        descriptor: &IndexDescriptor,
        object_store: Arc<dyn ObjectStore>,
        _reference_resolver: Option<Arc<dyn ReferenceResolver>>,
    ) -> Result<Self> {
        if descriptor.index_type != index_type.name() {
            return Err(Error::invalid_arg(
                "descriptor",
                format!(
                    "Descriptor index type '{}' does not match provided index type '{}'",
                    descriptor.index_type,
                    index_type.name()
                ),
            ));
        }

        let text_index = Self::open_index(descriptor, object_store)?;
        let metadata = Self::build_metadata_from_descriptor(descriptor);

        Ok(Self {
            text_index,
            index_type,
            metadata,
        })
    }

    /// Builds index metadata from the provided descriptor.
    ///
    /// This method extracts metadata information from the index descriptor and constructs
    /// an `IndexMetadata` instance that describes the capabilities and characteristics
    /// of this text index. The metadata is used by query planners and other index
    /// consumers to understand what operations this index supports.
    ///
    /// # Arguments
    ///
    /// * `descriptor` - The index descriptor containing field mappings and configuration properties
    ///
    /// # Returns
    ///
    /// An `IndexMetadata` instance describing the index's scope, granularity, fidelity,
    /// supported query kinds, and additional properties from the descriptor.
    ///
    /// # Index Characteristics
    ///
    /// The generated metadata specifies:
    /// - **Scope**:
    ///   - `ShardField` for single-field indexes (most common case)
    ///   - `MultiShardField` for indexes spanning multiple fields
    /// - **Granularity**: `Element` - tracks individual token/term positions
    /// - **Fidelity**: `Exact` - provides precise position information
    /// - **Query Kinds**: Supports all text-based query types:
    ///   - `Term`: Exact term matching
    ///   - `TermPrefix`: Prefix-based term matching  
    ///   - `TermSet`: OR semantics across multiple exact terms
    ///   - `TermPrefixSet`: OR semantics across multiple prefixes
    ///
    /// # Property Preservation
    ///
    /// All properties from the descriptor are preserved in the metadata's property bag,
    /// allowing downstream consumers to access configuration details like collation
    /// settings, normalization rules, or custom indexing parameters.
    fn build_metadata_from_descriptor(descriptor: &IndexDescriptor) -> IndexMetadata {
        use ahash::AHashSet;
        let scope = if descriptor.indexed_fields.len() == 1 {
            Scope::ShardField(descriptor.indexed_fields[0].clone())
        } else {
            Scope::MultiShardField(descriptor.indexed_fields.clone())
        };
        let mut query_kind = AHashSet::new();
        query_kind.insert(QueryKind::Term);
        query_kind.insert(QueryKind::TermPrefix);
        query_kind.insert(QueryKind::TermSet);
        query_kind.insert(QueryKind::TermPrefixSet);

        IndexMetadata {
            scope,
            granularity: Granularity::Element,
            fidelity: PositionFidelity::Exact,
            query_kind,
            bag: descriptor.properties.clone(),
        }
    }

    /// Opens the underlying text index from the descriptor's artifacts.
    ///
    /// This method extracts the required collation property and artifact URLs from the
    /// descriptor and initializes the low-level `TextIndex` that handles B-Tree and
    /// position data access. It validates the descriptor structure and ensures all
    /// required components are present and properly configured.
    ///
    /// # Arguments
    ///
    /// * `descriptor` - Index descriptor containing collation settings and artifact references
    /// * `object_store` - Storage backend for accessing the shard data
    ///
    /// # Returns
    ///
    /// A new `TextIndex` instance, or an error if initialization fails.
    ///
    /// # Required Properties
    ///
    /// The descriptor must contain a "collation" property with a string value specifying
    /// the collation strategy to use for term comparisons. Common values include:
    /// - "utf8_general_ci": Case-insensitive Unicode collation
    /// - "binary": Exact byte-for-byte comparison
    /// - "utf8_bin": Binary Unicode comparison
    ///
    /// # Required Artifacts
    ///
    /// The descriptor must contain at least two artifacts in the following order:
    /// 1. **Terms Shard** (index 0): Contains the B-Tree structure for term navigation
    /// 2. **Positions Shard** (index 1): Contains the compressed position lists
    ///
    /// Each artifact must have a valid `data_ref` with a non-empty URL pointing to the
    /// actual shard data in the object store.
    ///
    /// # Validation Process
    ///
    /// The method performs comprehensive validation:
    /// 1. Searches for and validates the collation property type and value
    /// 2. Ensures at least two artifacts are present in the descriptor
    /// 3. Validates each artifact has a valid data reference with non-empty URL
    /// 4. Initializes the underlying text index with the extracted configuration
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The collation property is missing, not a string, or has an unsupported value
    /// - Fewer than two artifacts are provided in the descriptor
    /// - Any artifact is missing its `data_ref` or has an empty URL
    /// - The underlying text index cannot be created with the provided parameters
    /// - Storage access fails during text index initialization
    fn open_index(
        descriptor: &IndexDescriptor,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<TextIndex> {
        let mut collation_prop: Option<String> = None;
        for prop in &descriptor.properties {
            if prop.name.eq_ignore_ascii_case("collation") {
                if let Some(val) = &prop.value {
                    if let Some(amudai_format::defs::common::any_value::Kind::StringValue(s)) =
                        &val.kind
                    {
                        collation_prop = Some(s.clone());
                    } else {
                        return Err(Error::invalid_arg(
                            "descriptor.properties.collation",
                            "collation property must be a string value",
                        ));
                    }
                }
            }
        }
        let collation = collation_prop.ok_or_else(|| {
            Error::invalid_arg(
                "descriptor.properties",
                "text index descriptor missing required 'collation' property",
            )
        })?;

        // Expect two artifacts: terms shard and positions shard. For now, use the first two.
        if descriptor.artifacts.len() < 2 {
            return Err(Error::invalid_arg(
                "descriptor.artifacts",
                format!(
                    "expected at least 2 artifacts (terms + positions), found {}",
                    descriptor.artifacts.len()
                ),
            ));
        }

        // Helper to extract URL from artifact
        let extract_url = |idx: usize| -> Result<String> {
            let art = &descriptor.artifacts[idx];
            let data_ref = art.data_ref.as_ref().ok_or_else(|| {
                Error::invalid_arg(
                    "descriptor.artifacts",
                    format!("artifact {} missing data_ref", idx),
                )
            })?;
            if data_ref.url.is_empty() {
                return Err(Error::invalid_arg(
                    "descriptor.artifacts",
                    format!("artifact {} has empty url", idx),
                ));
            }
            Ok(data_ref.url.clone())
        };

        TextIndex::new(
            extract_url(0)?,
            extract_url(1)?,
            Arc::clone(&object_store),
            &collation,
        )
    }
}

impl IndexReader for TextIndexReader {
    /// Returns a reference to the index type that created this reader.
    ///
    /// This method provides access to the index type implementation that was used
    /// to create this reader instance. The index type defines the behavior and
    /// capabilities specific to text indexing.
    ///
    /// # Returns
    ///
    /// A reference to the `IndexType` implementation for text indexes.
    fn index_type(&self) -> &Arc<dyn IndexType> {
        &self.index_type
    }

    /// Returns the metadata associated with this index.
    ///
    /// The metadata describes the characteristics and capabilities of this text index,
    /// including its scope (which fields are indexed), granularity (element-level),
    /// position fidelity (exact), and supported query types.
    ///
    /// # Returns
    ///
    /// A reference to the `IndexMetadata` describing this index's characteristics.
    fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    /// Executes a query against the text index and returns matching positions.
    ///
    /// This method processes various types of text queries and returns a `TernaryPositionSet`
    /// containing the positions where the query conditions are satisfied. The method supports
    /// multiple query types with OR semantics across multiple terms or prefixes.
    ///
    /// # Arguments
    ///
    /// * `query` - The query to execute, must be a `Term` variant with appropriate sub-type
    ///
    /// # Returns
    ///
    /// A `TernaryPositionSet` containing the positions where the query matches, or an error
    /// if the query cannot be executed.
    ///
    /// # Supported Query Types
    ///
    /// - **Term**: Exact match for a single term
    /// - **TermPrefix**: Prefix match for a single prefix
    /// - **TermSet**: Exact match for any term in a set (OR semantics)
    /// - **TermPrefixSet**: Prefix match for any prefix in a set (OR semantics)
    ///
    /// # Query Execution Strategy
    ///
    /// The method builds results using OR semantics across terms/prefixes by:
    /// 1. Starting with an all-false position set for the query span
    /// 2. Iterating through each term/prefix in the query
    /// 3. Performing the appropriate lookup (exact or prefix)
    /// 4. Converting position lists to ternary sets with the query span
    /// 5. Unioning each result into the accumulator
    ///
    /// # Validation
    ///
    /// The method validates query structure before execution:
    /// - Single-term queries (Term, TermPrefix) must have exactly one term
    /// - Multi-term queries (TermSet, TermPrefixSet) must have at least one term
    /// - Only Term query variants are supported (not Range, Numeric, etc.)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The query is not a supported `Term` variant  
    /// - Term/prefix count doesn't match the query type requirements
    /// - B-Tree navigation fails due to storage issues
    /// - Position data cannot be retrieved or decompressed
    fn query(&self, query: &IndexQuery) -> Result<TernaryPositionSet> {
        let term_query = match query {
            IndexQuery::Term(t) => t,
            other => {
                return Err(Error::invalid_arg(
                    "query",
                    format!(
                        "TextIndexReader only supports Term queries (Term/TermSet/Prefix/PrefixSet); received unsupported variant: {:?}",
                        std::mem::discriminant(other)
                    ),
                ));
            }
        };

        // Basic validation of term vector cardinality relative to query kind.
        match term_query.query_kind {
            QueryKind::Term | QueryKind::TermPrefix => {
                if term_query.terms.len() != 1 {
                    return Err(Error::invalid_arg(
                        "query.terms",
                        format!(
                            "Expected exactly 1 term/prefix for {:?} query, found {}",
                            term_query.query_kind,
                            term_query.terms.len()
                        ),
                    ));
                }
            }
            QueryKind::TermSet | QueryKind::TermPrefixSet => {
                if term_query.terms.is_empty() {
                    return Err(Error::invalid_arg(
                        "query.terms",
                        format!(
                            "Expected at least 1 term/prefix for {:?} query, found 0",
                            term_query.query_kind
                        ),
                    ));
                }
            }
            // Any other QueryKind is not yet supported by this reader implementation.
            ref k => {
                return Err(Error::invalid_arg(
                    "query.kind",
                    format!("Unsupported QueryKind for text index reader: {k:?}"),
                ));
            }
        }

        let span = term_query.span;
        // Start with an all-false definite set; we build up via unions (OR semantics across terms / prefixes).
        let mut accumulator = TernaryPositionSet::make_false(span);

        match term_query.query_kind {
            QueryKind::Term => {
                let iter = self.text_index.lookup_term(term_query.terms[0].as_ref())?;
                for tp_res in iter {
                    let ternary = tp_res?.positions.into_ternary_set(span);
                    accumulator = accumulator.union(&ternary);
                }
            }
            QueryKind::TermPrefix => {
                let iter = self
                    .text_index
                    .lookup_term_prefix(term_query.terms[0].as_ref())?;
                for tp_res in iter {
                    let ternary = tp_res?.positions.into_ternary_set(span);
                    accumulator = accumulator.union(&ternary);
                }
            }
            QueryKind::TermSet => {
                for term in &term_query.terms {
                    let iter = self.text_index.lookup_term(term.as_ref())?;
                    for tp_res in iter {
                        let ternary = tp_res?.positions.into_ternary_set(span);
                        accumulator = accumulator.union(&ternary);
                    }
                }
            }
            QueryKind::TermPrefixSet => {
                for prefix in &term_query.terms {
                    let iter = self.text_index.lookup_term_prefix(prefix.as_ref())?;
                    for tp_res in iter {
                        let ternary = tp_res?.positions.into_ternary_set(span);
                        accumulator = accumulator.union(&ternary);
                    }
                }
            }
            _ => unreachable!("validated unsupported kinds above"),
        }

        Ok(accumulator)
    }
}
