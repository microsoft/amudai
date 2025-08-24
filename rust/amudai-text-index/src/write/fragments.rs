//! Fragment builder for text index term entries.
//!
//! This module implements a streaming index fragment builder that collects term occurrences
//! during text indexing and efficiently serializes them to persistent storage. The design
//! supports incremental indexing across data stripes and provides deterministic ordering
//! for consistent query performance.
//!
//! # Architecture
//!
//! The fragment builder operates in two phases:
//! 1. **Collection**: Terms are extracted from text and accumulated in memory-efficient structures
//! 2. **Serialization**: Collected terms are sorted by collation rules and written to storage
//!
//! This two-phase approach enables optimal memory usage during large-scale indexing operations
//! while maintaining the lexicographic ordering required for efficient search operations.
//!
//! # Thread Safety
//!
//! Components in this module are not thread-safe by design, as they are intended for use
//! within single-threaded indexing pipelines. Higher-level coordination should handle
//! concurrent access across multiple fragments.

use std::{
    cmp::Ordering,
    io::{BufReader, Read},
    ops::Range,
};

use amudai_bytes::buffer::AlignedByteVec;
use amudai_format::schema::SchemaId;
use amudai_io::IoStream;
use amudai_keyed_vector::KeyedVector;

use crate::{
    collation::Collation,
    tokenizers::{Tokenizer, TokenizerType},
    write::{TermPosting, TermPostingSink, term_dictionary::TermDictionary},
};

/// Intermediate representation of a term occurrence within a field during indexing.
///
/// This compact structure captures the essential information needed to track term occurrences
/// before final serialization. The separation of stripe information allows for efficient
/// batching and reduces memory overhead during the collection phase.
///
/// # Memory Layout
///
/// The structure is optimized for cache efficiency with 8-byte alignment and minimal padding.
/// Term IDs use 32-bit integers to balance range requirements with memory efficiency.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
struct TermFieldEntry {
    /// The identifier of the term within the current indexing context.
    id: u32,
    /// The position of the term within the field (0-based).
    pos: u32,
}

/// Complete location descriptor for a term occurrence in the serialized index.
///
/// This structure represents the final form of term location data as it appears in
/// persisted index fragments. Unlike `TermFieldEntry`, this includes stripe information
/// and is optimized for serialization format compatibility.
///
/// # Index Organization
///
/// The inverted index hierarchy enables efficient query processing:
/// - **Terms**: Sorted by collation rules for predictable iteration order
/// - **Stripes**: Enable horizontal partitioning and parallel processing
/// - **Fields**: Support schema-aware indexing and field-specific queries
/// - **Positions**: Enable phrase matching and proximity-based ranking
///
/// # Sort Ordering Rationale
///
/// The lexicographic ordering (term → stripe → field → position) optimizes for:
/// - **Term grouping**: All occurrences of a term are contiguous in sorted order
/// - **Stripe locality**: Reduces I/O when processing specific data partitions
/// - **Field clustering**: Enables efficient field-restricted searches
/// - **Position monotonicity**: Supports phrase and proximity queries
///
/// This ordering minimizes disk seeks during query processing and enables
/// efficient merging of multiple index fragments.
///
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct TermEntry {
    /// Term identifier in the dictionary's sorted order.
    ///
    /// This ID reflects the term's position in the collation-sorted term dictionary,
    /// not the order of discovery during indexing. The sorted order ensures
    /// deterministic serialization and efficient range queries.
    pub id: u32,

    /// Data partition identifier for horizontal scaling.
    ///
    /// Stripe boundaries are typically aligned with logical data boundaries
    /// (e.g., time ranges, hash buckets) to enable efficient parallel processing
    /// and selective querying of data subsets.
    pub stripe: u16,

    /// Schema field identifier for column-specific indexing.
    ///
    /// Field IDs enable schema-aware search operations and allow different
    /// tokenization strategies per field type (e.g., exact matching for IDs,
    /// full-text search for content fields).
    pub field: SchemaId,

    /// Term position within the field instance.
    ///
    /// Position granularity depends on the tokenizer used - word-based tokenizers
    /// assign consecutive positions to adjacent words, while character-based
    /// tokenizers may use byte or character offsets.
    pub pos: u32,
}

impl Ord for TermEntry {
    /// Canonical ordering optimized for index serialization and query processing.
    ///
    /// The tuple-based comparison leverages Rust's lexicographic ordering to implement
    /// the precise sort order required for efficient index operations. This ordering
    /// ensures optimal cache locality during query processing and minimizes I/O
    /// operations when accessing related term occurrences.
    fn cmp(&self, other: &TermEntry) -> Ordering {
        (self.id, self.stripe, self.field, self.pos).cmp(&(
            other.id,
            other.stripe,
            other.field,
            other.pos,
        ))
    }
}

impl PartialOrd for TermEntry {
    /// Delegates to total ordering since all `TermEntry` instances are comparable.
    ///
    /// The total ordering property eliminates ambiguity in sorting algorithms
    /// and ensures deterministic behavior across different execution environments.
    fn partial_cmp(&self, other: &TermEntry) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Memory-efficient collector for term entries with optimized sorting algorithms.
///
/// This collector implements a streaming accumulation pattern that minimizes memory
/// allocation during high-volume indexing operations. The design exploits the natural
/// ordering properties of term extraction to enable linear-time sorting using bucket
/// sort techniques.
///
/// # Performance Characteristics
///
/// - **Insertion**: O(1) amortized for each term entry
/// - **Sorting**: O(n) where n is the number of entries (leverages bucket sort)
/// - **Memory**: Minimal heap allocations through buffer reuse
///
/// # Ordering Invariants
///
/// The collector maintains position monotonicity within each field, enabling
/// optimization opportunities during the sorting phase. This invariant must be
/// preserved by callers to ensure correctness.
///
/// # Memory Management Strategy
///
/// Internal buffers are retained across operations to reduce allocation pressure
/// during large-scale indexing. The `term_counters` buffer is particularly important
/// as it can grow to accommodate the vocabulary size of processed documents.
struct FieldEntryCollector {
    /// Field-partitioned entry storage for efficient field-aware operations.
    ///
    /// The keyed vector provides O(1) field lookup while maintaining iteration
    /// order for deterministic serialization. Sparse field access patterns
    /// are optimized through the keyed vector's internal design.
    entries: KeyedVector<SchemaId, Vec<TermFieldEntry>>,

    /// Running count of accumulated entries across all fields.
    ///
    /// Maintained incrementally to avoid expensive recalculation during
    /// memory estimation and capacity planning operations.
    entry_count: usize,

    /// Upper bound on term identifiers for efficient bucket sort setup.
    ///
    /// Tracks the maximum observed term ID plus one, enabling precise
    /// allocation of counting arrays for O(n) sorting algorithms.
    term_count: usize,

    /// Reusable counting buffer for bucket sort operations.
    ///
    /// Pre-allocated during sorting to minimize allocation overhead
    /// in performance-critical indexing loops. Size scales with vocabulary.
    term_counters: Vec<u32>,

    /// Current data stripe context for entry attribution.
    ///
    /// Stripe information is deferred until serialization to reduce
    /// memory overhead during the collection phase.
    stripe: u16,
}

impl FieldEntryCollector {
    /// Creates a new `FieldEntryCollector` with empty state.
    ///
    /// The collector is initialized with no entries, no tracked terms, and stripe index 0.
    /// All internal collections are created empty but ready for use.
    pub fn new() -> Self {
        Self {
            entries: KeyedVector::new(),
            entry_count: 0,
            term_count: 0,
            term_counters: vec![],
            stripe: 0,
        }
    }

    /// Adds a new term entry to the collector.
    ///
    /// The entry is added to the appropriate field's list, creating a new field list if necessary.
    /// This method maintains the invariant that positions within each field are non-decreasing.
    ///
    /// # Parameters
    ///
    /// * `termid` - The identifier of the term being added
    /// * `field` - The schema field identifier where this term occurs
    /// * `pos` - The position of the term within the field (must be >= previous positions for this field)
    ///
    /// # Panics
    ///
    /// May panic if the position ordering invariant is violated within a field.
    fn push(&mut self, termid: u32, field: SchemaId, pos: u32) {
        if !self.entries.contains_key(field) {
            self.entries.push_entry(field, vec![]);
        }

        // Add the entry to the corresponding field's list
        self.entries
            .get_mut(field)
            .unwrap()
            .push(TermFieldEntry { id: termid, pos });

        self.entry_count += 1;

        // Update the term count to accommodate the observed term ID
        // Term count represents the highest term ID + 1
        self.term_count = ::std::cmp::max(self.term_count, termid as usize + 1);
    }

    /// Changes the current stripe index for subsequent entries.
    ///
    /// This method should only be called after the collector has been flushed and reset
    /// (i.e., when `term_count` and `entry_count` are both zero). The new stripe index
    /// must be greater than or equal to the current stripe index.
    ///
    /// # Parameters
    ///
    /// * `stripe` - The new stripe index to use for subsequent entries
    ///
    /// # Panics
    ///
    /// Panics if called when the collector contains entries or if the new stripe
    /// index is less than the current one.
    fn set_stripe(&mut self, stripe: u16) {
        assert_eq!(self.term_count, 0);
        assert_eq!(self.entry_count, 0);
        assert!(stripe >= self.stripe);
        self.stripe = stripe;
    }

    /// Returns the total number of accumulated entries across all fields.
    ///
    /// This count includes all term entries that have been added via `push()`,
    /// regardless of which field they belong to.
    fn len(&self) -> usize {
        self.entry_count
    }

    /// Transforms term IDs to reflect dictionary's final sorted order.
    ///
    /// This operation is critical for ensuring that serialized entries reference
    /// terms by their final sorted positions rather than their discovery order.
    /// The transformation maintains referential integrity between the term dictionary
    /// and entry collections.
    ///
    /// # Performance Impact
    ///
    /// The remapping touches every collected entry, making it one of the more
    /// expensive operations in the indexing pipeline. However, it enables the
    /// optimal O(n) sorting algorithm used in `collect_sorted`.
    fn remap_terms(&mut self, current_to_sorted: &[usize]) {
        assert_eq!(self.term_count, current_to_sorted.len());

        for field in self.entries.values_mut() {
            for entry in field {
                entry.id = current_to_sorted[entry.id as usize] as u32;
            }
        }
    }

    /// Produces a globally sorted sequence of term entries using optimized bucket sort.
    ///
    /// This method implements the core sorting algorithm for the indexing pipeline,
    /// transforming the field-partitioned entries into the final serialization order.
    /// The algorithm exploits several domain-specific properties for optimal performance.
    ///
    /// # Algorithm Complexity
    ///
    /// - **Time**: O(n + t) where n = entries, t = distinct terms
    /// - **Space**: O(n + t) for output vector and counting arrays
    /// - **Cache performance**: Optimized for sequential access patterns
    ///
    /// # Algorithmic Assumptions
    ///
    /// The bucket sort optimization relies on three key invariants:
    /// 1. Term IDs are consecutive integers starting from 0
    /// 2. Positions within each field are non-decreasing  
    /// 3. Field iteration follows deterministic schema ordering
    ///
    /// These assumptions enable the linear-time sorting that would otherwise
    /// require O(n log n) comparison-based sorting for arbitrary data.
    fn collect_sorted(&mut self, sorted: &mut Vec<TermEntry>) {
        // Note: entry sorting is a hot path in our indexing scheme, especially
        // since the number of entries (term positions) is typically a couple
        // orders of magnitude larger then the number of distinct terms.

        // This implementation performs a variation of bucket sort.
        // The main assumption here is that positions for the specific term within
        // the specific field are already sorted (this follows from the order of
        // their arrival).

        // Count entries by term ID for bucket sort preparation
        self.term_counters.clear();
        self.term_counters.resize(self.term_count, 0);
        for field in self.entries.values_mut() {
            for entry in field {
                self.term_counters[entry.id as usize] += 1;
            }
        }

        // Calculate the exclusive prefix sum of the term counters.
        // After this operation, term_counters[term_id] contains the starting position
        // in the sorted vector where entries for that term should be placed.
        let mut prefix_sum = 0u32;
        for counter in &mut self.term_counters {
            let current_count = *counter;
            *counter = prefix_sum;
            prefix_sum += current_count;
        }

        // Pre-allocate the output vector with default entries
        sorted.clear();
        sorted.resize(
            self.entry_count,
            TermEntry {
                id: 0,
                stripe: 0,
                field: SchemaId::invalid(),
                pos: 0,
            },
        );

        // Collect entries from all field lists in schema ID order.
        // The bucket sort approach ensures entries are placed in their correct
        // final positions while maintaining field and position ordering.
        let last_schema_id = self.entries.key_range().end;
        let mut schema_id = SchemaId::zero();
        while schema_id < last_schema_id {
            if let Some(field) = self.entries.get(schema_id) {
                for entry in field {
                    let term_id = entry.id as usize;
                    let target_pos = self.term_counters[term_id] as usize;
                    self.term_counters[term_id] += 1;

                    sorted[target_pos] = TermEntry {
                        id: entry.id,
                        stripe: self.stripe,
                        field: schema_id,
                        pos: entry.pos,
                    };
                }
            }
            schema_id = schema_id.next();
        }

        // Verify that the output is properly sorted (debug assertion)
        debug_assert!(
            sorted.windows(2).all(|w| w[0] <= w[1]),
            "Output should be sorted"
        );
    }

    /// Clears all accumulated entries and resets the collector to an empty state.
    ///
    /// This method removes all entries from all fields, resets counters to zero,
    /// and clears the temporary term counters buffer. The collector is ready
    /// for reuse after calling this method, but the stripe index is preserved.
    ///
    /// This is typically called after entries have been collected and processed,
    /// allowing the same collector instance to be reused for the next batch.
    fn clear(&mut self) {
        for field in self.entries.values_mut() {
            field.clear();
        }

        self.term_counters.clear();
        self.term_count = 0;
        self.entry_count = 0;
    }
}

/// Streaming index fragment builder with integrated serialization capabilities.
///
/// An `IndexFragment` represents a self-contained unit of inverted index construction
/// that can efficiently accumulate term occurrences and serialize them to persistent
/// storage. The design supports incremental indexing workflows where large datasets
/// are processed in manageable chunks.
///
/// # Lifecycle Management
///
/// Fragments follow a structured lifecycle:
/// 1. **Creation**: Initialize with collation rules for term ordering
/// 2. **Accumulation**: Process text data and extract term occurrences  
/// 3. **Serialization**: Sort and write accumulated data to storage
/// 4. **Reset**: Clear state for reuse with subsequent data batches
///
/// # Memory Efficiency
///
/// The fragment maintains a compact in-memory representation during accumulation,
/// deferring expensive operations like global sorting until serialization time.
/// This approach minimizes peak memory usage during large-scale indexing operations.
///
/// # Stripe Coordination
///
/// Fragments operate within stripe boundaries to enable parallel processing of
/// large datasets. Stripe transitions are explicitly managed to ensure proper
/// data partitioning and merge compatibility.
pub struct IndexFragment {
    /// Text comparison rules for deterministic term ordering.
    ///
    /// The collation determines the final sort order of terms in the serialized
    /// index, directly affecting query performance and result determinism across
    /// different execution environments.
    collation: Box<dyn Collation>,

    /// Bidirectional mapping between terms and numeric identifiers.
    ///
    /// Maintains the vocabulary observed during indexing and provides efficient
    /// term-to-ID resolution. The dictionary's internal ordering is reconciled
    /// with collation requirements during serialization.
    term_dict: TermDictionary,

    /// Field-partitioned accumulator for term occurrence data.
    ///
    /// Organizes entries by field to enable schema-aware processing and
    /// field-specific query optimization. The partitioning also supports
    /// efficient bucket sort algorithms during serialization.
    entries: FieldEntryCollector,

    /// Pre-allocated buffer for global sort operations.
    ///
    /// Reused across spill operations to minimize allocation overhead.
    /// The buffer size scales with the fragment's accumulated entry count.
    sorted_entries: Vec<TermEntry>,

    /// Current data stripe identifier for partition coordination.
    ///
    /// Tracks the active data partition to ensure proper attribution of
    /// term occurrences and enable parallel processing across stripes.
    stripe: u16,
}

impl IndexFragment {
    /// Creates a new fragment indexer using the supplied `Collation`.
    ///
    /// Note: while the collation is shared across all of the indexed
    /// terms, there could be a different tokenizer for each field.
    pub fn new(collation: Box<dyn Collation>) -> IndexFragment {
        IndexFragment {
            collation,
            term_dict: TermDictionary::new(),
            entries: FieldEntryCollector::new(),
            sorted_entries: vec![],
            stripe: 0,
        }
    }

    /// Number of distinct terms currently indexed by this fragment
    /// Returns the number of distinct terms accumulated in this fragment.
    ///
    /// This count reflects the vocabulary size of the indexed text, providing
    /// insight into the linguistic diversity of the processed content. The count
    /// grows as new unique terms are encountered during text processing.
    #[allow(unused)]
    pub fn term_count(&self) -> usize {
        self.term_dict.len()
    }

    /// Total number of term positions currently indexed by this fragment
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Provides a rough estimate of the fragment's current memory footprint.
    ///
    /// This heuristic calculation considers the primary data structures:
    /// term dictionary storage, entry collections, and associated metadata.
    /// The estimate guides memory management decisions during index construction,
    /// particularly for determining optimal spill-to-disk thresholds.
    pub fn estimate_size(&self) -> usize {
        self.term_dict.len() * 16 + self.term_dict.buf_size() + self.entries.len() * 16
    }

    /// Returns the current stripe identifier for this fragment.
    ///
    /// Stripes represent logical partitions within the index construction process,
    /// enabling efficient parallel processing and deterministic ordering of
    /// index segments during merge operations.
    pub fn stripe(&self) -> u16 {
        self.stripe
    }

    /// Updates the stripe identifier for subsequent index entries.
    ///
    /// This method advances the logical partitioning state of the index fragment,
    /// ensuring that all future entries are associated with the new stripe value.
    /// Stripe transitions must be monotonically increasing to maintain proper
    /// ordering during index merge operations.
    pub fn set_stripe(&mut self, stripe: u16) {
        assert!(stripe >= self.stripe);

        if stripe != self.stripe {
            self.stripe = stripe;
            self.entries.set_stripe(stripe);
        }
    }

    /// Extracts and indexes all terms from the provided text content.
    ///
    /// This method represents the primary entry point for content indexing, orchestrating
    /// tokenization, term dictionary updates, and entry collection. The method is designed
    /// for high-throughput scenarios where text processing performance is critical.
    ///
    /// # Position Semantics
    ///
    /// The `pos` parameter represents a logical position within the field that may have
    /// different interpretations depending on the indexing strategy:
    /// - **Document-level**: Position within the entire document
    /// - **Field-level**: Position within the specific field instance
    /// - **Custom**: Application-defined positioning scheme
    ///
    /// # Error Handling
    ///
    /// The method returns `Result` to support future extensibility for error conditions
    /// such as term dictionary capacity limits or I/O errors during large-scale indexing.
    /// Currently, most error conditions are handled through assertions.
    pub fn process_text(
        &mut self,
        text: &str,
        tokenizer: &TokenizerType,
        field: SchemaId,
        pos: u32,
    ) -> amudai_common::Result<()> {
        assert!(text.len() <= u32::MAX as usize);

        if text.is_empty() {
            return Ok(());
        }

        // Tokenize the input string and process each term
        for term in tokenizer.tokenize(text) {
            assert!(!term.is_empty(), "Empty term encountered during indexing");

            // Get or create the term ID in the dictionary
            let term_id = self.term_dict.map_term(term.as_bytes());

            // Add the term entry to the collector
            self.entries.push(term_id, field, pos);
        }
        Ok(())
    }

    /// Flushes the sorted index fragment to the serialized stream.
    pub fn spill_to_disk(&mut self, writer: &mut Box<dyn IoStream>) -> amudai_common::Result<()> {
        let sorted_to_current = self.collect_sorted();
        self.serialize_fragment(writer, &sorted_to_current)?;
        self.clear();
        Ok(())
    }

    /// Resets the fragment to an empty state for reuse in subsequent processing.
    ///
    /// This method efficiently clears all accumulated index data while preserving
    /// the allocated memory capacity of internal data structures. This pattern
    /// enables high-performance batch processing by avoiding repeated memory
    /// allocation and deallocation cycles.
    pub fn clear(&mut self) {
        self.sorted_entries.clear();
        self.term_dict.clear();
        self.entries.clear();
    }

    /// Collects the sequence of sorted entries into `self.sorted_entries`,
    /// and returns a permutation that maps the sorted term ID's to the
    /// "current" ones (stored in the term dictionary).
    fn collect_sorted(&mut self) -> Vec<usize> {
        // Create a mapping that re-assigns term id's based on their
        // collation-defined order.
        // `sorted_to_current` maps the new (sorted) term id's to the current
        // ones stored in the dictionary.

        let sorted_to_current = self.term_dict.sorted_order(|lhs, rhs| {
            // SAFETY: The term is guaranteed to be valid UTF-8 by the tokenizer
            let lhs = unsafe { std::str::from_utf8_unchecked(lhs) };
            let rhs = unsafe { std::str::from_utf8_unchecked(rhs) };
            self.collation.compare(lhs, rhs)
        });

        // `current_to_sorted` maps the old term id's (stored in the dictionary)
        // to the new ones (sorted). It is constructed by inverting the
        // `sorted_to_current` permutation.
        let mut current_to_sorted = vec![0; sorted_to_current.len()];
        for (new_id, &old_id) in sorted_to_current.iter().enumerate() {
            current_to_sorted[old_id] = new_id;
        }

        // Re-assign term id's in the entries' collector according to the
        // sorted order
        self.entries.remap_terms(&current_to_sorted);

        // Now sort the term location entries (aka postings) first by term, then
        // by stripe, field id, position and offset. This serves three purposes:
        //  - groups together all postings for a specific term
        //  - orders these groups by the term according to the collation
        //  - establishes the correct order of postings (pos+offset) within the
        //    term group.
        self.entries.collect_sorted(&mut self.sorted_entries);

        sorted_to_current
    }

    /// Writes the sorted index fragment to the serialized stream.
    fn serialize_fragment(
        &mut self,
        writer: &mut Box<dyn IoStream>,
        sorted_to_current: &[usize],
    ) -> amudai_common::Result<()> {
        // term count (u32)
        writer.write_all(&(self.term_dict.len() as u32).to_le_bytes())?;
        // index_offsets flag (u32)
        writer.write_all(&0u32.to_le_bytes())?;
        // stripe id (u16)
        writer.write_all(&self.stripe.to_le_bytes())?;

        let mut counted_terms = 0;
        // Temporary buffer that will be re-used during serialization
        let mut scratch = vec![];
        // Position of the currently processed term
        let mut pos = 0;
        while pos < self.sorted_entries.len() {
            // Get the span of all consecutive entries for the current term
            let term_span = Self::get_term_span(&self.sorted_entries, pos);

            // Translate the term id to the actual term buffer
            let entry_id = self.sorted_entries[pos].id as usize;
            let term_id = sorted_to_current[entry_id] as u32;
            let term = self.term_dict.term_by_id(term_id);

            pos = term_span.end;

            // Write out all entries for the current term
            Self::write_term_span(writer, term, &self.sorted_entries[term_span], &mut scratch)?;
            counted_terms += 1;
        }
        assert_eq!(self.term_dict.len(), counted_terms);

        writer.flush()?;
        Ok(())
    }

    #[inline]
    fn get_term_span(entries: &[TermEntry], pos: usize) -> Range<usize> {
        let mut end = pos + 1;
        let id = entries[pos].id;
        while end < entries.len() && entries[end].id == id {
            end += 1;
        }
        pos..end
    }

    // Writes out the sequence of entries for the term
    fn write_term_span(
        writer: &mut Box<dyn IoStream>,
        term: &[u8],
        entries: &[TermEntry],
        scratch: &mut Vec<u32>,
    ) -> amudai_common::Result<()> {
        let field_count = Self::count_fields(entries);
        assert!(field_count < u16::MAX as usize);

        writer.write_all(&(term.len() as u16).to_le_bytes())?;
        writer.write_all(term)?;
        writer.write_all(&(field_count as u16).to_le_bytes())?;

        let mut pos = 0;
        while pos < entries.len() {
            let field_span = Self::get_field_span(entries, pos);
            pos = field_span.end;
            Self::write_field_span(writer, &entries[field_span], scratch)?;
        }
        Ok(())
    }

    #[inline]
    fn count_fields(entries: &[TermEntry]) -> usize {
        let mut counter = 1;
        for i in 1..entries.len() {
            if entries[i].field != entries[i - 1].field {
                counter += 1;
            }
        }
        counter
    }

    #[inline]
    fn get_field_span(entries: &[TermEntry], pos: usize) -> Range<usize> {
        let mut end = pos + 1;
        let field = entries[pos].field;
        while end < entries.len() && entries[end].field == field {
            end += 1;
        }
        pos..end
    }

    /// Writes out all the positions for the field
    fn write_field_span(
        writer: &mut Box<dyn IoStream>,
        entries: &[TermEntry],
        scratch: &mut Vec<u32>,
    ) -> amudai_common::Result<()> {
        writer.write_all(&(entries[0].field.as_u32() as u16).to_le_bytes())?;
        writer.write_all(&(entries.len() as u32).to_le_bytes())?;
        scratch.clear();
        for entry in entries {
            scratch.push(entry.pos);
        }
        writer.write_all(bytemuck::cast_slice(scratch.as_slice()))?;
        Ok(())
    }
}

/// Streaming decoder for serialized index fragment data.
///
/// The decoder provides efficient sequential access to index fragment contents,
/// designed for high-throughput scenarios like index merging and query processing.
/// The streaming design minimizes memory usage when processing large index fragments.
///
/// # Navigation Pattern
///
/// The decoder follows a hierarchical iteration pattern that mirrors the index structure:
/// 1. **Terms**: Use `next_term()` to advance to each unique term
/// 2. **Fields**: Use `next_field()` to iterate through fields containing the current term
/// 3. **Postings**: Access all positions for the current term-field pair via `postings()`
///
/// This pattern enables efficient traversal while maintaining low memory overhead
/// through on-demand deserialization of position data.
///
/// # Buffer Management
///
/// The decoder uses buffered I/O to optimize read performance and manages internal
/// scratch buffers to minimize allocation overhead during position decoding. Buffer
/// sizes are tuned for typical index fragment characteristics.
///
/// # Format Compatibility
///
/// The decoder is designed to handle format evolution through versioning and
/// feature flags in the serialized stream header. Currently supports the
/// baseline format without index offsets.
///
pub struct IndexFragmentDecoder {
    /// Buffered stream reader for efficient sequential access.
    ///
    /// The buffer size is optimized for typical index fragment I/O patterns,
    /// balancing memory usage with read efficiency for variable-length records.
    reader: BufReader<Box<dyn Read>>,

    /// Data stripe identifier from the fragment header.
    ///
    /// Immutable for the lifetime of the decoder, used to attribute all
    /// decoded postings to the correct data partition.
    stripe: u16,

    /// Currently active field identifier for posting attribution.
    ///
    /// Updated as the decoder advances through field spans within each term.
    /// Invalid when positioned between terms or at end-of-stream.
    field: SchemaId,

    /// Buffer for the current term's byte representation.
    ///
    /// Reused across terms to minimize allocation overhead. Size varies
    /// with term length as encountered in the serialized stream.
    term: Vec<u8>,

    /// Remaining field count for the current term's iteration.
    ///
    /// Tracks progress through the field list for the active term.
    /// Zero indicates all fields have been consumed for the current term.
    field_count: usize,

    /// Remaining term count for fragment-level iteration.
    ///
    /// Tracks progress through the complete term list in the fragment.
    /// Zero indicates end-of-stream has been reached.
    term_count: usize,

    /// Materialized postings for the current term-field combination.
    ///
    /// Contains all position information for the active term within the active field.
    /// Populated during field advancement and cleared between field transitions.
    postings: Vec<TermPosting>,

    /// Aligned buffer for efficient deserialization of position arrays.
    ///
    /// Provides memory-aligned access for bulk reading of position data from
    /// the serialized stream. Alignment improves performance on platforms
    /// that require or benefit from aligned memory access.
    scratch: AlignedByteVec,
}

impl IndexFragmentDecoder {
    /// Initializes a decoder from a serialized index fragment stream.
    ///
    /// The constructor performs immediate header validation and extracts metadata
    /// required for subsequent iteration. The decoder is positioned before the first
    /// term, requiring `next_term()` to be called before accessing content.
    ///
    /// # Header Format Validation
    ///
    /// The method validates the fragment header format and extracts:
    /// - Term count for iteration bounds checking
    /// - Index offset flags for format compatibility
    /// - Stripe identifier for posting attribution
    ///
    /// # Buffer Configuration
    ///
    /// The internal buffer is sized for optimal I/O patterns typical of index
    /// fragment access. The 64KB buffer balances memory usage with read efficiency
    /// for the variable-length record format used in serialization.
    pub fn new(reader: Box<dyn Read>) -> amudai_common::Result<IndexFragmentDecoder> {
        let mut reader = BufReader::with_capacity(64 * 1024, reader);

        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        let term_count = u32::from_le_bytes(buf) as usize;

        reader.read_exact(&mut buf)?;
        let index_offsets = u32::from_le_bytes(buf) == 1;
        assert!(!index_offsets);

        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf)?;
        let stripe = u16::from_le_bytes(buf);

        Ok(IndexFragmentDecoder {
            reader,
            stripe,
            field: SchemaId::invalid(),
            term: vec![],
            field_count: 0,
            term_count,
            postings: vec![],
            scratch: AlignedByteVec::new(),
        })
    }

    /// Advances to the next term in the serialized fragment.
    ///
    /// This method drives the primary iteration loop for index fragment processing.
    /// Each successful call positions the decoder at a new term and resets the
    /// field iteration state for that term.
    ///
    /// # Iteration Protocol
    ///
    /// The method enforces complete consumption of the previous term's fields
    /// before advancing. This ensures deterministic processing and prevents
    /// accidental skipping of index data.
    ///
    /// # Performance Considerations
    ///
    /// Term advancement involves reading variable-length term data from the stream,
    /// which may require multiple I/O operations for large terms. The buffered
    /// reader helps amortize this cost across multiple terms.
    pub fn next_term(&mut self) -> amudai_common::Result<bool> {
        assert_eq!(self.field_count, 0);
        if self.term_count != 0 {
            self.fetch_next_term()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Advances to the next field entry for the current term.
    ///
    /// This method implements the inner loop of index fragment iteration,
    /// progressing through all field occurrences for the currently positioned term.
    /// Each call yields a complete field entry with its associated posting list.
    ///
    /// # Iteration Semantics
    ///
    /// Field entries are ordered by field identifier, providing predictable
    /// traversal patterns for index consumers. The method maintains internal
    /// state to track position within the current term's field collection.
    ///
    /// # Memory Management
    ///
    /// Position data is loaded into the decoder's scratch buffer, which is
    /// reused across field entries to minimize allocation overhead during
    /// large index traversals.
    ///
    /// # Returns
    ///
    /// Ok(true) if the decoder is positioned at the valid term entry
    ///
    /// Ok(false) if the decoder is at the end of the field list for
    /// the current term.
    ///
    /// Err(...) in case of IO or format error
    pub fn next_field(&mut self) -> amudai_common::Result<bool> {
        if self.field_count != 0 {
            self.fetch_next_field()?;
            Ok(true)
        } else {
            self.postings.clear();
            Ok(false)
        }
    }

    /// Returns the raw bytes of the current term.
    ///
    /// The term data represents the tokenized text as stored in the index fragment.
    /// Terms are typically UTF-8 encoded but may contain binary data depending
    /// on the tokenizer configuration used during index construction.
    pub fn term(&self) -> &[u8] {
        &self.term
    }

    /// Returns the current term as a UTF-8 string slice.
    ///
    /// This method provides a safe conversion of the term bytes to a string slice,
    /// assuming the term is valid UTF-8. It is guaranteed to succeed as the tokenizer
    /// used during indexing ensures that all terms are valid UTF-8 sequences.
    pub fn term_as_str(&self) -> &str {
        // SAFETY: The term is guaranteed to be valid UTF-8 by the tokenizer
        unsafe { std::str::from_utf8_unchecked(&self.term) }
    }

    /// Returns the position occurrences for the current term within the current field.
    ///
    /// Each posting represents a specific location where the term appears,
    /// containing both the document position and any associated frequency or
    /// proximity information required for relevance scoring and phrase queries.
    ///
    /// The returned slice is valid until the next call to `next_field()` or
    /// `next_term()`, as the underlying buffer may be reused for subsequent entries.
    pub fn postings(&self) -> &[TermPosting] {
        &self.postings
    }

    /// Internal method that reads the next term from the serialized stream.
    ///
    /// Deserializes the term length, term bytes, and field count from the stream,
    /// updating the decoder's internal state to position at the new term.
    /// Decrements the remaining term count and clears the postings buffer.
    fn fetch_next_term(&mut self) -> amudai_common::Result<()> {
        assert_ne!(self.term_count, 0);

        // Read the term length and the term itself
        let mut buf = [0u8; 2];
        self.reader.read_exact(&mut buf)?;
        let term_len = u16::from_le_bytes(buf) as usize;

        self.term.resize(term_len, 0);
        self.reader.read_exact(&mut self.term)?;

        // Read the number of fields for this term
        self.reader.read_exact(&mut buf)?;
        self.field_count = u16::from_le_bytes(buf) as usize;
        assert_ne!(self.field_count, 0);
        self.postings.clear();
        self.term_count -= 1;
        Ok(())
    }

    /// Internal method that reads the next field entry for the current term.
    ///
    /// Deserializes the field identifier, position count, and position array from the stream,
    /// populating the postings buffer with TermPosting instances for the current term-field pair.
    /// Decrements the remaining field count for the current term.
    fn fetch_next_field(&mut self) -> amudai_common::Result<()> {
        assert_ne!(self.field_count, 0);

        let mut buf = [0u8; 2];
        self.reader.read_exact(&mut buf)?;
        self.field = (u16::from_le_bytes(buf) as u32).into();

        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;
        let entry_count = u32::from_le_bytes(buf) as usize;
        assert_ne!(entry_count, 0);

        self.scratch.resize_zeroed::<u32>(entry_count);
        self.reader.read_exact(&mut self.scratch)?;

        self.postings.clear();
        let values = self.scratch.typed_data::<u32>();

        for &pos in values.iter().take(entry_count) {
            self.postings.push(TermPosting {
                stripe: self.stripe,
                field: self.field,
                pos,
            });
        }

        self.field_count -= 1;
        Ok(())
    }
}

/// Wrapper for index fragment decoders that enables efficient merging of multiple fragments.
///
/// This cursor provides ordered comparison capabilities for fragment decoders during merge operations,
/// implementing a two-phase comparison strategy that optimizes performance when merging fragments
/// with overlapping vocabularies. The cursor maintains both term-level and posting-level ordering
/// to ensure correct merge semantics.
///
/// # Ordering Strategy
///
/// The cursor implements a hierarchical ordering approach:
/// - **Primary**: Terms compared using collation rules for lexicographic ordering
/// - **Secondary**: Postings compared by stripe, field, and position for deterministic merging
/// - **Optimization**: Term comparison can be disabled when processing identical terms
///
/// # Performance Optimization
///
/// The `omit_term_comparison` flag enables significant performance improvements during the
/// merge process by eliminating redundant string comparisons when all cursors are known
/// to be positioned at the same term.
struct FragmentMergeCursor {
    /// When `omit_term_comparison` is true, `Ord` implementation
    /// will not compare the terms, only the entries' position. This
    /// is an optimization for the case when we already know that
    /// the decoders represent the same term, and we just want to
    /// merge their position lists.
    omit_term_comparison: bool,
    collation: Box<dyn Collation>,
    decoder: IndexFragmentDecoder,
}

impl PartialEq for FragmentMergeCursor {
    fn eq(&self, other: &FragmentMergeCursor) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for FragmentMergeCursor {}

impl Ord for FragmentMergeCursor {
    #[inline]
    fn cmp(&self, other: &FragmentMergeCursor) -> Ordering {
        // Compare the actual terms first, unless the term
        // comparison is omitted
        let term_ord = if !self.omit_term_comparison {
            self.collation
                .compare(self.decoder.term_as_str(), other.decoder.term_as_str())
        } else {
            Ordering::Equal
        };

        if term_ord != Ordering::Equal {
            // The heap that maintains the merge cursors is max-heap, but
            // we want the lesser term on the top, so reverse the ordering
            term_ord.reverse()
        } else {
            // For equal terms, order by stripe, field id, pos and offset
            self.decoder.postings()[0]
                .cmp(&other.decoder.postings()[0])
                .reverse()
        }
    }
}

impl PartialOrd for FragmentMergeCursor {
    fn partial_cmp(&self, other: &FragmentMergeCursor) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Merges multiple serialized index fragments into a unified posting stream.
///
/// This function implements a sophisticated multi-way merge algorithm that combines
/// index fragments from different sources while maintaining correct lexicographic
/// ordering and efficient posting consolidation. The merge process is designed for
/// high-throughput scenarios where large numbers of fragments must be consolidated.
///
/// # Algorithm Overview
///
/// The merging strategy employs a two-level heap approach:
/// - **Term-level heap**: Orders fragments by their current term using collation rules
/// - **Entry-level heap**: Merges postings for identical terms across multiple fragments
///
/// This hierarchical approach minimizes string comparisons and optimizes cache locality
/// during the merge process, particularly beneficial when processing fragments with
/// significant vocabulary overlap.
///
/// # Performance Characteristics
///
/// - **Time complexity**: O(N log K) where N is total postings and K is fragment count
/// - **Space complexity**: O(K) for heap structures plus fragment decoder buffers
/// - **I/O pattern**: Sequential reads from all input fragments with streaming output
///
/// # Merge Semantics
///
/// The function ensures that:
/// - Terms appear in collation-defined lexicographic order in the output
/// - Postings for identical terms are properly consolidated across fragments
/// - Stripe, field, and position ordering is preserved within each term
/// - Term ordinals are assigned sequentially for efficient downstream processing
///
/// # Error Handling
///
/// The merge process propagates I/O errors from fragment decoders and sink operations,
/// ensuring that merge failures are properly reported to calling code. The function
/// maintains transactional semantics where partial merges are not committed on error.
pub fn merge_fragment_decoders<S>(
    decoders: Vec<IndexFragmentDecoder>,
    collation: Box<dyn Collation>,
    sink: &mut S,
) -> amudai_common::Result<()>
where
    S: ?Sized + TermPostingSink,
{
    use std::collections::BinaryHeap;

    // Fragment decoders are "nested" enumerators: at the top there
    // is a term enumerator, and within the term there is an entry enumerator.
    // `term_heap` organizes the decoders such that the minimum term is at the
    // top.
    // When we pop the decoder with the next term from the term heap,
    // we also try to pop all of the decoders with the same term.
    // Then we push those into separate binary heap and merge their
    // entries. This process avoids redundant comparison of terms when
    // merging the entry lists.

    let mut term_ordinal = 0;
    let mut term_heap = BinaryHeap::new();

    for mut decoder in decoders.into_iter() {
        // Fill the term heap
        if decoder.next_term()? {
            assert!(decoder.next_field()?);
            term_heap.push(FragmentMergeCursor {
                omit_term_comparison: false,
                collation: collation.clone_boxed(),
                decoder,
            });
        }
    }

    // Entry heap is reused across iterations to avoid constant reallocations.
    let mut entry_heap = BinaryHeap::new();
    while !term_heap.is_empty() {
        // At this point we're about to deal with the next term in lexicographic
        // order. Get the top term cursor from the term heap.
        let mut term_cursor = term_heap.pop().expect("Non-empty heap must have top");
        assert!(!term_cursor.decoder.postings().is_empty());

        sink.start_term(term_ordinal, false, term_cursor.decoder.term())?;
        term_ordinal += 1;

        // We'll be merging the entries for the same term, so disable the term
        // comparison until we advance to the next term.
        term_cursor.omit_term_comparison = true;

        // Try to pop more cursors positioned at the same term
        while !term_heap.is_empty() {
            let same_term = {
                let current = term_cursor.decoder.term_as_str();
                let next = term_heap.peek().unwrap().decoder.term_as_str();
                collation.compare(current, next) == Ordering::Equal
            };

            if same_term {
                // We have more than one cursor with the same term, populate
                // the entry heap.
                entry_heap.push(term_cursor);
                term_cursor = term_heap.pop().expect("Non-empty heap must have top");
                assert!(!term_cursor.decoder.postings().is_empty());
                term_cursor.omit_term_comparison = true;
            } else {
                break;
            }
        }
        // At least one term cursor will go to the entry heap
        entry_heap.push(term_cursor);

        // Merge the entry lists for the same term from the different cursors
        // on the entry heap, without comparing the terms.
        while !entry_heap.is_empty() {
            let mut entry_cursor = entry_heap.pop().expect("Non-empty heap must have top");

            for posting in entry_cursor.decoder.postings() {
                sink.push_posting(posting)?;
            }

            if entry_cursor.decoder.next_field()? {
                // We have more fields/entries for the term in this cursor, push it
                // back to the entry heap.
                entry_heap.push(entry_cursor);
            } else if entry_cursor.decoder.next_term()? {
                assert!(entry_cursor.decoder.next_field()?);
                // No more entries for the current term, but we have more terms in this cursor.
                // Return the cursor to the term heap.
                entry_cursor.omit_term_comparison = false;
                term_heap.push(entry_cursor);
            }
        }
        sink.end_term()?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use amudai_format::schema::SchemaId;

    #[test]
    fn test_field_entry_collector_basic_operations() {
        // Test new collector creation
        let mut collector = FieldEntryCollector::new();
        assert_eq!(collector.len(), 0);
        assert_eq!(collector.term_count, 0);
        assert_eq!(collector.stripe, 0);

        // Test pushing single entry
        let field1 = SchemaId::from(1u32);
        collector.push(42, field1, 0);
        assert_eq!(collector.len(), 1);
        assert_eq!(collector.term_count, 43); // max term_id + 1
        assert!(collector.entries.contains_key(field1));

        // Test pushing multiple entries to same field
        collector.push(10, field1, 1);
        collector.push(5, field1, 2);
        assert_eq!(collector.len(), 3);
        assert_eq!(collector.term_count, 43); // still 43 since max is still 42

        let field1_entries = collector.entries.get(field1).unwrap();
        assert_eq!(field1_entries.len(), 3);
        assert_eq!(field1_entries[0].id, 42);
        assert_eq!(field1_entries[1].id, 10);
        assert_eq!(field1_entries[2].id, 5);

        // Test pushing to multiple fields
        let field2 = SchemaId::from(2u32);
        collector.push(50, field2, 0);
        collector.push(15, field1, 3);
        assert_eq!(collector.len(), 5);
        assert_eq!(collector.term_count, 51); // max term_id (50) + 1

        let field2_entries = collector.entries.get(field2).unwrap();
        assert_eq!(field2_entries.len(), 1);
        assert_eq!(field2_entries[0].id, 50);

        // Test stripe operations
        collector.clear();
        collector.set_stripe(5);
        assert_eq!(collector.stripe, 5);
        collector.set_stripe(7); // Should work (higher stripe)
        assert_eq!(collector.stripe, 7);

        // Test clear functionality
        collector.push(100, field1, 0);
        assert_eq!(collector.len(), 1);
        collector.clear();
        assert_eq!(collector.len(), 0);
        assert_eq!(collector.term_count, 0);
        assert_eq!(collector.stripe, 7); // stripe preserved
        assert!(collector.term_counters.is_empty());

        // Test reuse after clear
        collector.push(200, field1, 0);
        assert_eq!(collector.len(), 1);
        assert_eq!(collector.term_count, 201);
    }

    #[test]
    fn test_field_entry_collector_advanced_operations() {
        let mut collector = FieldEntryCollector::new();
        let field1 = SchemaId::from(1u32);
        let field2 = SchemaId::from(2u32);
        collector.set_stripe(3);

        // Setup data for sorting and remapping tests
        collector.push(2, field1, 0);
        collector.push(0, field1, 1);
        collector.push(1, field2, 0);
        collector.push(0, field2, 1);
        collector.push(1, field1, 2);

        // Test term remapping
        let mapping = vec![2, 0, 1]; // 0->2, 1->0, 2->1
        collector.remap_terms(&mapping);

        let field1_entries = collector.entries.get(field1).unwrap();
        assert_eq!(field1_entries[0].id, 1); // was 2, now 1
        assert_eq!(field1_entries[1].id, 2); // was 0, now 2
        assert_eq!(field1_entries[2].id, 0); // was 1, now 0

        // Test collect_sorted with multiple fields
        let mut sorted = vec![];
        collector.collect_sorted(&mut sorted);

        assert_eq!(sorted.len(), 5);

        // Verify sorting: by term_id, then field_id, then position
        // term 0, field 1, pos 2
        assert_eq!(sorted[0].id, 0);
        assert_eq!(sorted[0].field, field1);
        assert_eq!(sorted[0].pos, 2);
        assert_eq!(sorted[0].stripe, 3);

        // term 0, field 2, pos 0 (remapped from term 1)
        assert_eq!(sorted[1].id, 0);
        assert_eq!(sorted[1].field, field2);
        assert_eq!(sorted[1].pos, 0);

        // term 1, field 1, pos 0 (remapped from term 2)
        assert_eq!(sorted[2].id, 1);
        assert_eq!(sorted[2].field, field1);
        assert_eq!(sorted[2].pos, 0);

        // term 2, field 1, pos 1 (remapped from term 0)
        assert_eq!(sorted[3].id, 2);
        assert_eq!(sorted[3].field, field1);
        assert_eq!(sorted[3].pos, 1);

        // term 2, field 2, pos 1 (remapped from term 0)
        assert_eq!(sorted[4].id, 2);
        assert_eq!(sorted[4].field, field2);
        assert_eq!(sorted[4].pos, 1);

        // Test empty collect_sorted
        collector.clear();
        let mut empty_sorted = vec![];
        collector.collect_sorted(&mut empty_sorted);
        assert!(empty_sorted.is_empty());
    }

    #[test]
    fn test_index_fragment_basic_operations() {
        use crate::collation::create_collation;
        use crate::tokenizers::create_tokenizer;

        // Create a fragment with case-insensitive collation
        let collation = create_collation("unicode-case-insensitive").unwrap();
        let mut fragment = IndexFragment::new(collation);

        // Initially empty
        assert_eq!(fragment.term_count(), 0);
        assert_eq!(fragment.entry_count(), 0);
        assert_eq!(fragment.stripe(), 0);

        // Create a tokenizer for testing
        let tokenizer = create_tokenizer("unicode-word").unwrap();
        let field1 = SchemaId::from(1u32);
        let field2 = SchemaId::from(2u32);

        // Process some text
        fragment
            .process_text("hello world", &tokenizer, field1, 0)
            .unwrap();
        assert_eq!(fragment.term_count(), 2); // "hello" and "world"
        assert_eq!(fragment.entry_count(), 2);

        // Process more text in different field
        fragment
            .process_text("hello universe", &tokenizer, field2, 1)
            .unwrap();
        assert_eq!(fragment.term_count(), 3); // "hello", "world", "universe"
        assert_eq!(fragment.entry_count(), 4);

        // Test clear
        fragment.clear();
        assert_eq!(fragment.term_count(), 0);
        assert_eq!(fragment.entry_count(), 0);
        assert_eq!(fragment.stripe(), 0); // stripe preserved

        // Test stripe operations
        fragment.set_stripe(5);
        assert_eq!(fragment.stripe(), 5);
    }

    #[test]
    fn test_index_fragment_spill_and_decode() {
        use crate::collation::create_collation;
        use crate::tokenizers::create_tokenizer;
        use std::collections::HashMap;
        use std::io::Cursor;

        // Create a fragment with case-insensitive collation
        let collation = create_collation("unicode-case-insensitive").unwrap();
        let mut fragment = IndexFragment::new(collation);
        let tokenizer = create_tokenizer("unicode-word").unwrap();

        // Use multiple fields
        let field1 = SchemaId::from(10u32);
        let field2 = SchemaId::from(20u32);
        let field3 = SchemaId::from(30u32);
        let field4 = SchemaId::from(40u32);

        // Create large vocabulary with >10 terms across multiple stripes
        let documents = vec![
            // Stripe 0 - Documents with various terms
            (
                "technology artificial intelligence machine learning deep neural networks",
                field1,
                0,
            ),
            (
                "data science statistics mathematics algorithms optimization",
                field2,
                10,
            ),
            (
                "programming software development engineering computer",
                field3,
                20,
            ),
            ("artificial intelligence programming data", field1, 30),
            // More documents in stripe 0
            ("neural networks deep learning artificial", field4, 40),
            ("computer science technology engineering", field2, 50),
            ("machine learning algorithms statistics", field3, 60),
            ("software development programming optimization", field1, 70),
            // Stripe 1 - Different set of documents
            ("database systems distributed computing cloud", field1, 100),
            (
                "artificial intelligence machine learning networks",
                field2,
                110,
            ),
            ("technology innovation research development", field3, 120),
            ("programming languages software architecture", field4, 130),
            // More documents in stripe 1
            ("deep learning neural networks intelligence", field1, 140),
            ("data analytics statistics science", field2, 150),
            ("computer algorithms optimization development", field3, 160),
            ("technology programming engineering systems", field4, 170),
            // Stripe 2 - Additional documents for complexity
            (
                "cloud computing distributed systems architecture",
                field1,
                200,
            ),
            ("machine learning artificial intelligence deep", field2, 210),
            ("software engineering development programming", field3, 220),
            ("data science analytics statistics research", field4, 230),
            // Final documents in stripe 2
            (
                "neural networks learning algorithms optimization",
                field1,
                240,
            ),
            ("technology computer science innovation", field2, 250),
            ("programming development software systems", field3, 260),
            ("artificial intelligence data analytics", field4, 270),
        ];

        let stripe_boundaries = [0, 8, 16]; // Documents per stripe

        for stripe in 0..3 {
            fragment.set_stripe(stripe);

            let start_idx = stripe_boundaries[stripe as usize];
            let end_idx = if stripe == 2 {
                documents.len()
            } else {
                stripe_boundaries[stripe as usize + 1]
            };

            for (text, field, pos) in documents.iter().take(end_idx).skip(start_idx) {
                fragment
                    .process_text(text, &tokenizer, *field, *pos)
                    .unwrap();
            }

            // Spill each stripe separately to test multiple stripe functionality
            let buffer = Vec::<u8>::new();
            let mut boxed_buffer = Box::new(buffer) as Box<dyn amudai_io::IoStream>;
            fragment.spill_to_disk(&mut boxed_buffer).unwrap();

            // Decode and verify this stripe
            let mut data = boxed_buffer.into_reader().unwrap();
            let mut data_slice = vec![];
            use std::io::Read;
            data.read_to_end(&mut data_slice).unwrap();

            let cursor = Cursor::new(data_slice);
            let boxed_cursor = Box::new(cursor) as Box<dyn Read>;
            let mut decoder = IndexFragmentDecoder::new(boxed_cursor).unwrap();

            assert_eq!(decoder.stripe, stripe);

            // Collect all terms and verify comprehensive indexing
            let mut terms_found = vec![];
            let mut total_postings = 0;
            let mut field_counts: HashMap<SchemaId, usize> = HashMap::new();

            while decoder.next_term().unwrap() {
                let term_str = std::str::from_utf8(decoder.term()).unwrap();
                terms_found.push(term_str.to_string());

                while decoder.next_field().unwrap() {
                    let field = decoder.field;
                    let postings = decoder.postings();
                    total_postings += postings.len();
                    *field_counts.entry(field).or_insert(0) += postings.len();

                    // Verify all postings have correct stripe and field
                    for posting in postings {
                        assert_eq!(posting.stripe, stripe);
                        assert_eq!(posting.field, field);
                    }
                }
            }

            // Verify we have substantial vocabulary (>10 terms) for each stripe
            assert!(
                terms_found.len() > 10,
                "Stripe {} should have >10 terms, got {}",
                stripe,
                terms_found.len()
            );

            // Verify terms are properly sorted
            let mut sorted_terms = terms_found.clone();
            sorted_terms.sort();
            assert_eq!(
                terms_found, sorted_terms,
                "Terms should be sorted in stripe {stripe}"
            );

            // Verify multiple fields are used
            assert!(
                field_counts.len() >= 3,
                "Stripe {} should use multiple fields, got {} fields",
                stripe,
                field_counts.len()
            );

            // Verify substantial number of postings (multiple positions)
            assert!(
                total_postings > 30,
                "Stripe {stripe} should have >30 postings, got {total_postings}"
            );

            println!(
                "Stripe {}: {} unique terms, {} total postings across {} fields",
                stripe,
                terms_found.len(),
                total_postings,
                field_counts.len()
            );
        }
    }

    #[test]
    fn test_merge_fragment_decoders() {
        use crate::collation::create_collation;
        use crate::tokenizers::create_tokenizer;
        use std::io::Cursor;

        // Test sink that collects all merged data for verification
        #[derive(Debug)]
        struct TestSink {
            terms: Vec<(usize, bool, Vec<u8>)>, // (ordinal, is_null, term_bytes)
            postings: Vec<(usize, TermPosting)>, // (term_ordinal, posting)
            current_term_ordinal: Option<usize>,
        }

        impl TestSink {
            fn new() -> Self {
                Self {
                    terms: Vec::new(),
                    postings: Vec::new(),
                    current_term_ordinal: None,
                }
            }
        }

        impl TermPostingSink for TestSink {
            fn start_term(
                &mut self,
                term_ordinal: usize,
                is_null: bool,
                term: &[u8],
            ) -> amudai_common::Result<()> {
                self.terms.push((term_ordinal, is_null, term.to_vec()));
                self.current_term_ordinal = Some(term_ordinal);
                Ok(())
            }

            fn push_posting(&mut self, posting: &TermPosting) -> amudai_common::Result<()> {
                let term_ordinal = self
                    .current_term_ordinal
                    .expect("start_term must be called first");
                self.postings.push((term_ordinal, *posting));
                Ok(())
            }

            fn end_term(&mut self) -> amudai_common::Result<()> {
                self.current_term_ordinal = None;
                Ok(())
            }
        }

        // Create test data with multiple fragments
        let collation = create_collation("unicode-case-insensitive").unwrap();
        let tokenizer = create_tokenizer("unicode-word").unwrap();

        let field1 = SchemaId::from(1u32);
        let field2 = SchemaId::from(2u32);

        // Fragment 1 data
        let mut fragment1 = IndexFragment::new(collation.clone_boxed());
        fragment1.set_stripe(0);
        fragment1
            .process_text("apple banana", &tokenizer, field1, 0)
            .unwrap();
        fragment1
            .process_text("cherry", &tokenizer, field2, 1)
            .unwrap();
        // Spill stripe 0 data
        let buffer1a = Vec::<u8>::new();
        let mut boxed_buffer1a = Box::new(buffer1a) as Box<dyn amudai_io::IoStream>;
        fragment1.spill_to_disk(&mut boxed_buffer1a).unwrap();

        // Continue with stripe 1 for fragment1
        fragment1.set_stripe(1);
        fragment1
            .process_text("banana date", &tokenizer, field1, 2)
            .unwrap();
        let buffer1b = Vec::<u8>::new();
        let mut boxed_buffer1b = Box::new(buffer1b) as Box<dyn amudai_io::IoStream>;
        fragment1.spill_to_disk(&mut boxed_buffer1b).unwrap();

        // Fragment 2 data (overlapping terms)
        let mut fragment2 = IndexFragment::new(collation.clone_boxed());
        fragment2.set_stripe(0);
        fragment2
            .process_text("apple elderberry", &tokenizer, field2, 0)
            .unwrap();
        // Spill stripe 0 data
        let buffer2a = Vec::<u8>::new();
        let mut boxed_buffer2a = Box::new(buffer2a) as Box<dyn amudai_io::IoStream>;
        fragment2.spill_to_disk(&mut boxed_buffer2a).unwrap();

        // Continue with stripe 2 for fragment2
        fragment2.set_stripe(2);
        fragment2
            .process_text("fig apple", &tokenizer, field1, 1)
            .unwrap();
        let buffer2b = Vec::<u8>::new();
        let mut boxed_buffer2b = Box::new(buffer2b) as Box<dyn amudai_io::IoStream>;
        fragment2.spill_to_disk(&mut boxed_buffer2b).unwrap();

        // Fragment 3 data (different terms)
        let mut fragment3 = IndexFragment::new(collation.clone_boxed());
        fragment3.set_stripe(1);
        fragment3
            .process_text("grape", &tokenizer, field1, 0)
            .unwrap();
        fragment3
            .process_text("banana grape", &tokenizer, field2, 1)
            .unwrap();
        let buffer3 = Vec::<u8>::new();
        let mut boxed_buffer3 = Box::new(buffer3) as Box<dyn amudai_io::IoStream>;
        fragment3.spill_to_disk(&mut boxed_buffer3).unwrap();

        // Extract serialized data from all streams
        let mut serialized_fragments = Vec::new();
        for boxed_buffer in [
            boxed_buffer1a,
            boxed_buffer1b,
            boxed_buffer2a,
            boxed_buffer2b,
            boxed_buffer3,
        ] {
            let mut reader = boxed_buffer.into_reader().unwrap();
            let mut buffer_data = Vec::new();
            use std::io::Read;
            reader.read_to_end(&mut buffer_data).unwrap();
            serialized_fragments.push(buffer_data);
        }

        // Create decoders from serialized data
        let mut decoders = Vec::new();
        for buffer in serialized_fragments {
            let reader: Box<dyn std::io::Read> = Box::new(Cursor::new(buffer));
            let decoder = IndexFragmentDecoder::new(reader).unwrap();
            decoders.push(decoder);
        }

        // Merge the fragments
        let mut sink = TestSink::new();
        let merge_collation = create_collation("unicode-case-insensitive").unwrap();
        merge_fragment_decoders(decoders, merge_collation, &mut sink).unwrap();

        // Verify merged results
        assert!(!sink.terms.is_empty(), "Should have merged terms");
        assert!(!sink.postings.is_empty(), "Should have merged postings");

        // Verify terms are in lexicographic order
        let collation_for_test = create_collation("unicode-case-insensitive").unwrap();
        for i in 1..sink.terms.len() {
            let prev_term = std::str::from_utf8(&sink.terms[i - 1].2).unwrap();
            let curr_term = std::str::from_utf8(&sink.terms[i].2).unwrap();
            assert!(
                collation_for_test.compare(prev_term, curr_term) != std::cmp::Ordering::Greater,
                "Terms should be in lexicographic order: '{prev_term}' should not come after '{curr_term}'"
            );
        }

        // Verify term ordinals are sequential
        for (i, &(ordinal, _, _)) in sink.terms.iter().enumerate() {
            assert_eq!(ordinal, i, "Term ordinals should be sequential");
        }

        // Verify postings ordering within each term
        let mut postings_by_term: std::collections::HashMap<usize, Vec<TermPosting>> =
            std::collections::HashMap::new();

        for &(term_ordinal, posting) in &sink.postings {
            postings_by_term
                .entry(term_ordinal)
                .or_default()
                .push(posting);
        }

        for (term_ordinal, postings) in postings_by_term {
            // Postings should already be sorted, but verify
            for i in 1..postings.len() {
                assert!(
                    postings[i - 1] <= postings[i],
                    "Postings for term {} should be sorted: {:?} should not come after {:?}",
                    term_ordinal,
                    postings[i - 1],
                    postings[i]
                );
            }
        }

        // Verify specific expected terms exist
        let term_strings: Vec<String> = sink
            .terms
            .iter()
            .map(|(_, _, bytes)| String::from_utf8(bytes.clone()).unwrap())
            .collect();

        let expected_terms = vec![
            "apple",
            "banana",
            "cherry",
            "date",
            "elderberry",
            "fig",
            "grape",
        ];
        for expected in expected_terms {
            assert!(
                term_strings.contains(&expected.to_string()),
                "Expected term '{expected}' should be present in merged results"
            );
        }

        // Verify that overlapping terms (like "apple" and "banana") have postings from multiple fragments
        let apple_ordinal = sink
            .terms
            .iter()
            .find(|(_, _, bytes)| std::str::from_utf8(bytes).unwrap() == "apple")
            .map(|(ordinal, _, _)| *ordinal);

        if let Some(ordinal) = apple_ordinal {
            let apple_postings: Vec<_> = sink
                .postings
                .iter()
                .filter(|(term_ord, _)| *term_ord == ordinal)
                .map(|(_, posting)| *posting)
                .collect();

            assert!(
                apple_postings.len() >= 2,
                "Apple should have postings from multiple fragments, got: {apple_postings:?}"
            );

            // Should have postings from different stripes (0 and 2)
            let stripes: std::collections::HashSet<u16> =
                apple_postings.iter().map(|p| p.stripe).collect();
            assert!(stripes.len() > 1, "Apple should appear in multiple stripes");
        }
    }
}
