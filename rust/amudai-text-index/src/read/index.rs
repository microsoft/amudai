//! Text index decoder providing high-level querying interface.
//!
//! This module implements the primary reader interface for querying inverted text indexes
//! stored in the Amudai format. It coordinates between the B-Tree term navigation and
//! position data retrieval to provide efficient text search capabilities.

use std::sync::Arc;

use amudai_common::Result;
use amudai_objectstore::ObjectStore;

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
    /// * `terms_shard_url` - URL to the shard containing B-Tree structure and terms
    /// * `positions_shard_url` - URL to the shard containing position lists
    /// * `object_store` - Storage backend for accessing the shards
    /// * `collation` - Collation strategy name for term comparison (e.g., "utf8_general_ci")
    ///
    /// # Returns
    ///
    /// A new `TextIndex` instance ready for querying, or an error if initialization fails.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The collation strategy is not recognized
    /// - Either shard cannot be opened or accessed
    /// - The shard schemas are incompatible with the expected format
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
    /// * `term` - The exact term to search for
    ///
    /// # Returns
    ///
    /// An iterator that yields `TermPositions` for each occurrence of the term,
    /// or an error if the lookup operation fails.
    ///
    /// # Performance
    ///
    /// The lookup operation uses B-Tree navigation for efficient term location.
    /// Position data is retrieved lazily as the iterator is consumed, minimizing
    /// memory usage for large result sets.
    pub fn lookup_term(&mut self, term: &str) -> Result<TermPositionsIterator<'_, EntryIterator>> {
        Ok(TermPositionsIterator {
            entries_iter: self.btree.lookup_term(term)?,
            positions: &mut self.positions,
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
    /// * `prefix` - The prefix to search for
    ///
    /// # Returns
    ///
    /// An iterator that yields `TermPositions` for each occurrence of terms
    /// matching the prefix, or an error if the lookup operation fails.
    ///
    /// # Ordering
    ///
    /// Results are returned in the lexicographic order of the matching terms
    /// according to the configured collation strategy. Within each term,
    /// positions are ordered by stripe, field, and position.
    ///
    /// # Performance
    ///
    /// Prefix searches use efficient B-Tree range scans to locate all matching
    /// terms without scanning the entire term space. Position data is retrieved
    /// lazily to minimize memory overhead.
    pub fn lookup_term_prefix(
        &mut self,
        prefix: &str,
    ) -> Result<TermPositionsIterator<'_, ByPrefixEntryIterator>> {
        Ok(TermPositionsIterator {
            entries_iter: self.btree.lookup_term_prefix(prefix)?,
            positions: &mut self.positions,
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
/// * `I` - The underlying iterator type that yields B-Tree entries (either exact matches or prefix matches)
///
/// # Lazy Loading
///
/// Position data is retrieved lazily as the iterator is consumed. This approach minimizes
/// memory usage and allows for efficient processing of queries that may match many terms
/// or terms with extensive position lists.
///
/// # Error Handling
///
/// The iterator may yield errors during iteration if position data cannot be retrieved
/// or if there are issues accessing the underlying storage. Callers should handle these
/// errors appropriately based on their fault tolerance requirements.
pub struct TermPositionsIterator<'a, I>
where
    I: Iterator<Item = Result<BTreeEntry>>,
{
    /// Iterator over B-Tree entries containing term metadata and position references.
    entries_iter: I,
    /// Decoder for retrieving position lists from the positions shard.
    positions: &'a mut PositionsDecoder,
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
    /// information about where a term appears, including stripe, field, and position details.
    ///
    /// # Returns
    ///
    /// * `Some(Ok(TermPositions))` - Successfully retrieved term position information
    /// * `Some(Err(...))` - Error occurred during B-Tree navigation or position retrieval
    /// * `None` - No more results available
    ///
    /// # Error Conditions
    ///
    /// Errors may occur due to:
    /// - Storage access failures when retrieving position data
    /// - Data corruption or format inconsistencies
    /// - Resource exhaustion during position list decompression
    fn next(&mut self) -> Option<Self::Item> {
        match self.entries_iter.next() {
            Some(Ok(entry)) => match self.positions.read(entry.repr_type, entry.range) {
                Err(e) => return Some(Err(e)),
                Ok(pos_list) => Some(Ok(TermPositions {
                    stripe: entry.stripe,
                    field: entry.field,
                    positions: pos_list,
                })),
            },
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pos_list::PositionList;
    use crate::write::builder::{TextIndexBuilder, TextIndexBuilderConfig};
    use amudai_format::schema::SchemaId;
    use amudai_io_impl::temp_file_store;
    use amudai_objectstore::local_store::{LocalFsMode, LocalFsObjectStore};
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Comprehensive end-to-end test that validates the complete text indexing and querying pipeline.
    ///
    /// This test validates:
    /// 1. Building a text index from textual data across multiple stripes and fields
    /// 2. Reading the index and performing exact term lookups
    /// 3. Reading the index and performing prefix-based searches
    /// 4. Verifying position data accuracy and ordering
    /// 5. Ensuring proper handling of multi-stripe and multi-field scenarios
    #[test]
    fn test_end_to_end_text_indexing_and_querying() {
        // Setup storage backends with temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let object_store: Arc<dyn ObjectStore> = Arc::new(
            LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot)
                .expect("Failed to create LocalFsObjectStore"),
        );
        let temp_store = temp_file_store::create_in_memory(10 * 1024 * 1024)
            .expect("Failed to create temporary file store");

        // Create builder configuration with multiple tokenizers
        let config = TextIndexBuilderConfig {
            max_fragment_size: Some(50 * 1024 * 1024), // Very large size to avoid fragment spilling
            tokenizers: vec![
                "unicode-word".to_string(), // For field 0
                "unicode-word".to_string(), // For field 1
                "unicode-word".to_string(), // For field 2
            ],
            collation: "unicode-case-insensitive".to_string(),
        };

        // Create and configure the text index builder
        let mut builder = TextIndexBuilder::new(config, Arc::clone(&object_store), temp_store)
            .expect("Failed to create TextIndexBuilder");

        // Test data: Build a complex index with multiple stripes and fields
        // Use different terms for each field to avoid fragment merging issues for now

        // Generate sufficient data for proper shard creation
        for stripe in 0..3 {
            builder.set_stripe(stripe).expect("Failed to set stripe");

            // Process multiple records in field 0 with terms including "apple" and "apples"
            for i in 0..15 {
                // Include both singular and plural to exercise prefix queries (e.g., "app" -> apple, apples)
                let text = format!(
                    "apple apples juice document number {} in stripe {}",
                    i, stripe
                );
                builder
                    .process_text_field(&text, SchemaId::from(0u32), i + 1)
                    .expect("Failed to process field 0");
            }

            // Process multiple records in field 1 with different terms
            for i in 0..15 {
                // Ensure that in stripe 0 we also mention "apple" so that apple appears in multiple fields there
                let text = if stripe == 0 {
                    format!(
                        "banana smoothie content number {} stripe {} with apple",
                        i, stripe
                    )
                } else {
                    format!("banana smoothie content number {} stripe {}", i, stripe)
                };
                builder
                    .process_text_field(&text, SchemaId::from(1u32), i + 1)
                    .expect("Failed to process field 1");
            }

            // Process multiple records in field 2 (tags) with different terms
            for i in 0..15 {
                // Include terms to satisfy prefix and field-isolation tests: healthy, fruit, fruits
                let text = format!(
                    "orange citrus tag healthy fruit fruits number {} stripe {}",
                    i, stripe
                );
                builder
                    .process_text_field(&text, SchemaId::from(2u32), i + 1)
                    .expect("Failed to process field 2");
            }
        }

        // Finalize the index construction
        let prepared_index = builder.finish().expect("Failed to finish building index");
        let sealed_index = prepared_index
            .seal("null:///tmp/test_index")
            .expect("Failed to seal index");

        // Create a text index reader for querying
        let mut text_index = TextIndex::new(
            sealed_index.terms_shard.directory_ref.url.clone(),
            sealed_index.positions_shard.directory_ref.url.clone(),
            object_store,
            "unicode-case-insensitive",
        )
        .expect("Failed to create TextIndex reader");

        let mut all_terms_results = text_index
            .lookup_term_prefix("")
            .expect("Failed to lookup all terms");

        let mut term_count = 0;
        let mut seen_terms = std::collections::HashSet::new();
        while let Some(result) = all_terms_results.next() {
            match result {
                Ok(term_positions) => {
                    seen_terms.insert((term_positions.stripe, term_positions.field));
                    term_count += 1;
                }
                Err(e) => {
                    panic!("Error reading term positions: {}", e);
                }
            }
            if term_count > 20 {
                // Limit to prevent spam
                break;
            }
        }

        // Test 1: Exact term lookup for "apple" - should appear in multiple stripes and fields
        let mut apple_results = text_index
            .lookup_term("apple")
            .expect("Failed to lookup term 'apple'");

        let mut apple_occurrences = Vec::new();
        while let Some(result) = apple_results.next() {
            let term_positions = result.expect("Failed to get term position");
            apple_occurrences.push((
                term_positions.stripe,
                term_positions.field,
                term_positions.positions,
            ));
        }

        assert!(
            apple_occurrences.len() >= 3,
            "Apple should appear in at least 3 contexts, found {}",
            apple_occurrences.len()
        );

        // Check that apple appears in stripe 0 (multiple fields)
        let stripe_0_apple: Vec<_> = apple_occurrences
            .iter()
            .filter(|(stripe, _, _)| *stripe == 0)
            .collect();
        assert!(
            stripe_0_apple.len() >= 2,
            "Apple should appear in multiple fields in stripe 0"
        );

        // Check that apple appears in stripe 1 (shared across documents)
        let stripe_1_apple: Vec<_> = apple_occurrences
            .iter()
            .filter(|(stripe, _, _)| *stripe == 1)
            .collect();
        assert!(stripe_1_apple.len() >= 1, "Apple should appear in stripe 1");

        // Verify position data integrity - all position lists should be valid
        for (_, _, positions) in &apple_occurrences {
            match positions {
                PositionList::Positions(pos_vec) => {
                    assert!(!pos_vec.is_empty(), "Position list should not be empty");
                    // Verify positions are sorted
                    for window in pos_vec.windows(2) {
                        assert!(window[0] <= window[1], "Positions should be sorted");
                    }
                }
                PositionList::ExactRanges(ranges) => {
                    assert!(!ranges.is_empty(), "Range list should not be empty");
                    // Verify ranges are valid
                    for range in ranges {
                        assert!(range.start < range.end, "Range should be valid");
                    }
                }
                PositionList::ApproximateRanges(ranges) => {
                    assert!(
                        !ranges.is_empty(),
                        "Approximate range list should not be empty"
                    );
                }
                PositionList::Unknown => {
                    // This is valid for very common terms
                }
            }
        }

        // Test 2: Exact term lookup for "juice" - should appear in multiple contexts
        let mut juice_results = text_index
            .lookup_term("juice")
            .expect("Failed to lookup term 'juice'");

        let mut juice_occurrences = Vec::new();
        while let Some(result) = juice_results.next() {
            let term_positions = result.expect("Failed to get juice term position");
            juice_occurrences.push((term_positions.stripe, term_positions.field));
        }

        // Verify juice appears across multiple stripes
        assert!(
            juice_occurrences.len() >= 3,
            "Juice should appear in multiple contexts"
        );
        let unique_stripes: std::collections::HashSet<_> =
            juice_occurrences.iter().map(|(s, _)| s).collect();
        assert!(
            unique_stripes.len() >= 2,
            "Juice should appear in multiple stripes"
        );

        // Test 3: Exact term lookup for a term that appears only once
        let mut banana_results = text_index
            .lookup_term("banana")
            .expect("Failed to lookup term 'banana'");

        let mut banana_occurrences = Vec::new();
        while let Some(result) = banana_results.next() {
            let term_positions = result.expect("Failed to get banana term position");
            banana_occurrences.push((term_positions.stripe, term_positions.field));
        }

        // Verify banana appears in stripe 1 (where we added banana content)
        assert!(
            banana_occurrences.len() >= 1,
            "Banana should appear at least once"
        );
        let has_stripe_1 = banana_occurrences.iter().any(|(stripe, _)| *stripe == 1);
        assert!(has_stripe_1, "Banana should appear in stripe 1");

        // Test 4: Exact term lookup for non-existent term
        let nonexistent_results = text_index
            .lookup_term("nonexistent")
            .expect("Failed to lookup nonexistent term");

        let nonexistent_count = nonexistent_results.count();
        assert_eq!(
            nonexistent_count, 0,
            "Non-existent term should return no results"
        );

        // Test 5: Prefix search for "app" - should match "apple" and "apples"
        let mut app_prefix_results = text_index
            .lookup_term_prefix("app")
            .expect("Failed to lookup prefix 'app'");

        let mut app_prefix_occurrences = Vec::new();
        while let Some(result) = app_prefix_results.next() {
            let term_positions = result.expect("Failed to get app prefix term position");
            app_prefix_occurrences.push((term_positions.stripe, term_positions.field));
        }

        // Should find apple and apples occurrences
        assert!(
            app_prefix_occurrences.len() >= 3,
            "App prefix should match multiple terms and contexts"
        );

        // Test 6: Prefix search for "fruit" - should match "fruit" and "fruits"
        let mut fruit_prefix_results = text_index
            .lookup_term_prefix("fruit")
            .expect("Failed to lookup prefix 'fruit'");

        let mut fruit_prefix_count = 0;
        let mut fruit_stripes = std::collections::HashSet::new();
        while let Some(result) = fruit_prefix_results.next() {
            let term_positions = result.expect("Failed to get fruit prefix term position");
            fruit_prefix_count += 1;
            fruit_stripes.insert(term_positions.stripe);
        }

        assert!(
            fruit_prefix_count >= 2,
            "Fruit prefix should match multiple occurrences"
        );
        assert!(
            fruit_stripes.len() >= 2,
            "Fruit prefix should span multiple stripes"
        );

        // Test 7: Prefix search for "xyz" - should return no results
        let xyz_prefix_results = text_index
            .lookup_term_prefix("xyz")
            .expect("Failed to lookup prefix 'xyz'");

        let xyz_prefix_count = xyz_prefix_results.count();
        assert_eq!(
            xyz_prefix_count, 0,
            "Non-matching prefix should return no results"
        );

        // Test 8: Prefix search with empty string - should match all terms
        let mut all_terms_results = text_index
            .lookup_term_prefix("")
            .expect("Failed to lookup empty prefix");

        let mut all_terms_count = 0;
        let mut seen_terms = std::collections::HashSet::new();
        while let Some(result) = all_terms_results.next() {
            let _term_positions = result.expect("Failed to get term position for empty prefix");
            all_terms_count += 1;
            seen_terms.insert((_term_positions.stripe, _term_positions.field));
        }

        assert!(
            all_terms_count > 10,
            "Empty prefix should return many term occurrences"
        );
        assert!(
            seen_terms.len() >= 6,
            "Should see terms across multiple stripe/field combinations"
        );

        // Test 9: Verify field isolation - terms should be properly associated with correct fields
        let mut healthy_results = text_index
            .lookup_term("healthy")
            .expect("Failed to lookup term 'healthy'");

        let mut healthy_fields = std::collections::HashSet::new();
        while let Some(result) = healthy_results.next() {
            let term_positions = result.expect("Failed to get healthy term position");
            healthy_fields.insert(term_positions.field);
        }

        // "healthy" should appear in tags field (SchemaId(2)) across multiple stripes
        assert!(
            healthy_fields.contains(&SchemaId::from(2u32)),
            "Healthy should appear in tags field"
        );

        // Test 10: Case-insensitive collation test
        let apple_caps_results = text_index
            .lookup_term("APPLE")
            .expect("Failed to lookup term 'APPLE'");

        let apple_caps_count = apple_caps_results.count();
        // With case-insensitive collation, "APPLE" should find the same results as "apple"
        assert!(
            apple_caps_count >= 3,
            "Case-insensitive search should find apple occurrences"
        );
    }
}
