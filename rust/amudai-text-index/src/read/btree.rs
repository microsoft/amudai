//! B-Tree decoder for reading text index term postings.

use arrow_array::{
    Array, LargeBinaryArray, LargeListArray, StructArray, UInt8Array, UInt16Array, UInt64Array,
};
use std::{
    ops::Range,
    sync::{Arc, OnceLock},
};

use amudai_arrow::builder::ArrowReaderBuilder;
use amudai_common::{Result, error::Error};
use amudai_format::schema::SchemaId;
use amudai_objectstore::{ObjectStore, url::ObjectUrl};
use amudai_ranges::SharedRangeList;
use amudai_shard::read::shard::{Shard, ShardOptions};

use crate::{collation::Collation, pos_list::PositionsReprType};

/// A B-Tree index reader for term lookup operations in text indices.
///
/// `BTreeDecoder` provides read access to a persistent B-Tree index structure,
/// enabling efficient term searches with logarithmic time complexity. The decoder
/// loads B-Tree pages on demand from an object store and uses configurable
/// collation for term comparison operations.
///
/// ## Architecture
///
/// The decoder reads from a single shard containing the B-Tree structure with
/// interior and leaf pages. Leaf pages contain terms and references to position
/// data, while interior pages provide navigation through the tree hierarchy.
///
/// ## Performance Characteristics
///
/// - **Term Lookup**: O(log n) complexity using binary search
/// - **Prefix Search**: O(log n + k) where k is the number of matching terms
/// - **Memory Usage**: Lazy loading with page-level caching
/// - **I/O Efficiency**: On-demand page loading from object store
///
/// ## Supported Operations
///
/// - **Exact Term Lookup**: Find all entries matching a specific term
/// - **Prefix Search**: Find all terms starting with a given prefix
/// - **Collation Support**: Configurable term comparison and ordering
///
/// ## Thread Safety
///
/// `BTreeDecoder` is thread-safe for concurrent, read-only use. All public
/// methods take `&self` and the only internal mutable state is the lazily
/// initialized `Shard`, managed by a `OnceLock` with fallible initialization.
/// Multiple threads can safely call lookup methods concurrently, including the
/// first access that triggers shard loading.
///
/// The returned iterators (`EntryIterator`, `ByPrefixEntryIterator`) are
/// stateful and not designed to be shared *between* threads after creation;
/// however they can each be used on a single thread while other threads create
/// and use their own iterators simultaneously. Clone or create a new iterator
/// per thread if needed.
pub struct BTreeDecoder {
    /// URL of the shard containing B-Tree pages with terms and tree structure.
    shard_url: String,
    /// Object store for reading shard data.
    object_store: Arc<dyn ObjectStore>,
    /// The shard containing B-Tree pages with terms and tree structure.
    shard: OnceLock<Shard>,
    /// The collation used for term comparison during lookups.
    collation: Box<dyn Collation>,
}

impl BTreeDecoder {
    /// Creates a new B-Tree decoder instance.
    ///
    /// The decoder is initialized with the specified shard URL, object store, and collation
    /// strategy. The shard is opened lazily on first access to minimize initialization overhead.
    ///
    /// # Arguments
    ///
    /// * `shard_url` - URL of the shard containing the B-Tree structure
    /// * `object_store` - Object store implementation for reading shard data
    /// * `collation` - Collation strategy for term comparison operations
    ///
    /// # Returns
    ///
    /// A new `BTreeDecoder` instance ready for term lookup operations.
    ///
    /// # Errors
    ///
    /// This method performs minimal validation. Errors related to invalid URLs
    /// or inaccessible shards will be reported during actual lookup operations.
    pub fn new(
        shard_url: String,
        object_store: Arc<dyn ObjectStore>,
        collation: Box<dyn Collation>,
    ) -> Result<Self> {
        Ok(Self {
            shard_url,
            object_store,
            shard: OnceLock::new(),
            collation,
        })
    }

    /// Gets or lazily creates the terms shard instance.
    ///
    /// Opens the terms shard on first access using the configured object store, implementing
    /// a lazy initialization pattern that minimizes resource usage during decoder creation.
    /// Subsequent calls return the cached shard instance without additional I/O operations.
    ///
    /// The terms shard contains the complete B-Tree structure including:
    /// - Interior pages with navigation pointers and separator terms
    /// - Leaf pages with actual term entries and position references
    /// - Metadata about the B-Tree organization and page layout
    ///
    /// ## Lazy Loading Benefits
    ///
    /// - Defers expensive I/O operations until actually needed
    /// - Allows decoder creation even when shards are temporarily unavailable
    /// - Reduces memory footprint for unused decoder instances
    /// - Enables efficient batching of multiple decoder operations
    ///
    /// ## Thread Safety
    ///
    /// Uses `OnceLock` to ensure thread-safe initialization even in concurrent scenarios.
    /// The decoder supports concurrent access from multiple threads safely.
    ///
    /// # Returns
    ///
    /// A reference to the opened terms shard, which remains valid for the lifetime
    /// of the decoder instance.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The shard URL is malformed or uses an unsupported scheme
    /// - The object store cannot access the shard location
    /// - The shard file is corrupted, incomplete, or in an incompatible format
    /// - Network or storage I/O errors occur during shard opening
    fn shard(&self) -> Result<&Shard> {
        if let Some(s) = self.shard.get() {
            return Ok(s);
        }

        let url = ObjectUrl::parse(&self.shard_url)
            .map_err(|e| Error::invalid_format(format!("Invalid terms shard URL: {e}")))?;

        let options = ShardOptions::new(Arc::clone(&self.object_store));

        let shard = options
            .open(url)
            .map_err(|e| Error::invalid_format(format!("Failed to open terms shard: {e}")))?;

        // If another thread won the race, we ignore our shard (they should be equivalent).
        let _ = self.shard.set(shard);

        Ok(self.shard.get().unwrap())
    }

    /// Returns the total number of pages available in the terms shard.
    ///
    /// ## Validation
    ///
    /// An empty shard (with zero pages) is considered an invalid state for a B-Tree index,
    /// as even the minimal B-Tree structure requires at least a root page. This method
    /// will return an error if such a condition is detected.
    ///
    /// # Returns
    ///
    /// The total number of pages in the terms shard as a 64-bit unsigned integer.
    /// This value is always > 0 for valid B-Tree indices.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The terms shard cannot be accessed or opened
    /// - The shard directory is corrupted or unreadable
    /// - The shard is empty (contains zero pages), indicating an invalid B-Tree state
    fn pages_count(&self) -> Result<u64> {
        let total_pages = self.shard()?.directory().total_record_count;
        if total_pages == 0 {
            return Err(Error::invalid_format("Empty terms shard"));
        }
        Ok(total_pages)
    }

    /// Performs an exact term lookup and returns an iterator over matching entries.
    ///
    /// Searches the B-Tree for entries that exactly match the specified term using a binary search
    /// algorithm with O(log n) performance characteristics. The search starts from the root page
    /// and traverses down through interior pages until reaching the appropriate leaf pages that
    /// contain matching terms.
    ///
    /// The lookup process respects the configured collation strategy, ensuring that term matching
    /// follows the same ordering semantics used when the index was created. This is crucial for
    /// correct results when dealing with case-insensitive searches, locale-specific ordering,
    /// or other specialized text processing requirements.
    ///
    /// ## Behavior
    ///
    /// - Uses binary search at each B-Tree level for optimal performance
    /// - Handles multi-page term spans automatically when terms appear across page boundaries
    /// - Returns an iterator that lazily loads additional pages as needed
    /// - Respects collation semantics for term comparison and matching
    ///
    /// ## Performance
    ///
    /// - **Time Complexity**: O(log n) where n is the number of terms in the index
    /// - **Space Complexity**: O(1) with lazy page loading
    /// - **I/O Pattern**: Minimal object store requests through page-level caching
    ///
    /// # Arguments
    ///
    /// * `term` - The exact term to search for in the B-Tree index. The term must match
    ///   exactly according to the configured collation strategy.
    ///
    /// # Returns
    ///
    /// An iterator that yields `Result<BTreeEntry>` for all entries matching the exact term.
    /// Each `BTreeEntry` contains the stripe, field, representation type, and position range
    /// information for the term. The iterator automatically handles multi-page spans if the
    /// term appears across multiple leaf pages, and will be empty if no matching terms are found.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The B-Tree structure is corrupted or inconsistent
    /// - Pages cannot be loaded from the object store
    /// - The terms shard is inaccessible or malformed
    /// - Navigation encounters unexpected data formats
    pub fn lookup_term(&self, term: &str) -> Result<EntryIterator<'_>> {
        let root_position = self.root_page_position()?;
        self.lookup_term_recursive(term, root_position)
    }

    /// Performs a prefix-based term lookup and returns an iterator over matching entries.
    ///
    /// Searches the B-Tree for all entries whose terms start with the specified prefix string.
    /// Uses binary search to efficiently locate the first matching entry with O(log n) complexity,
    /// then creates an iterator that scans forward through subsequent pages until no more terms
    /// match the prefix pattern.
    ///
    /// This method is particularly useful for implementing autocomplete functionality, wildcard
    /// searches, and other scenarios where you need to find all terms that share a common prefix.
    /// The prefix matching respects the configured collation strategy to ensure consistent
    /// behavior with the term ordering used during index creation.
    ///
    /// ## Algorithm
    ///
    /// 1. Uses binary search to find the first term >= the prefix
    /// 2. Creates an iterator starting from that position
    /// 3. The iterator continues yielding terms while they start with the prefix
    /// 4. Automatically handles page boundaries and multi-page result sets
    ///
    /// ## Performance
    ///
    /// - **Time Complexity**: O(log n + k) where n is total terms and k is matching results
    /// - **Space Complexity**: O(1) with lazy page loading and streaming results
    /// - **I/O Pattern**: Efficient sequential access after initial binary search
    ///
    /// ## Prefix Matching Semantics
    ///
    /// The prefix matching is performed using the configured collation strategy. For example:
    /// - Case-sensitive collation: "Pre" matches "Prefix" but not "prefix"
    /// - Case-insensitive collation: "Pre" matches both "Prefix" and "prefix"
    /// - Locale-specific collation: Handles unicode normalization and cultural sorting
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix string that terms must start with to be included in results.
    ///   An empty string will match all terms in the index.
    ///
    /// # Returns
    ///
    /// An iterator that yields `Result<BTreeEntry>` for all entries whose terms start with
    /// the specified prefix. Each `BTreeEntry` contains the stripe, field, representation type,
    /// and position range information for the term. The iterator automatically handles
    /// multi-page spans and will be empty if no terms match the prefix.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The B-Tree structure is corrupted or inconsistent
    /// - Pages cannot be loaded from the object store
    /// - The terms shard is inaccessible or malformed
    /// - Navigation encounters unexpected data formats
    pub fn lookup_term_prefix(&self, prefix: &str) -> Result<ByPrefixEntryIterator<'_>> {
        let root_position = self.root_page_position()?;
        self.lookup_term_prefix_recursive(prefix, root_position)
    }

    /// Finds the leftmost index of entries that exactly match the target term.
    ///
    /// When multiple entries have the same term (which can happen in leaf pages),
    /// this function locates the first occurrence. This is essential for ensuring
    /// complete iteration over all matching entries during term lookups.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of entries being searched
    /// * `F` - The type of the getter function that extracts terms from entries
    ///
    /// # Arguments
    ///
    /// * `entries` - Slice of entries to search within
    /// * `term` - The exact term to find the leftmost occurrence of
    /// * `collation` - Collation strategy for term comparison
    /// * `get_term` - Function that extracts the term string from an entry
    ///
    /// # Returns
    ///
    /// Some(index) of the leftmost matching entry, or None if no match is found
    fn find_leftmost_equal<T, F>(
        entries: &[T],
        term: &str,
        collation: &dyn Collation,
        get_term: F,
    ) -> Option<usize>
    where
        F: Fn(&T) -> &str,
    {
        match entries.binary_search_by(|e| collation.compare(get_term(e), term)) {
            Ok(mut idx) => {
                while idx > 0
                    && collation.compare(get_term(&entries[idx - 1]), term)
                        == std::cmp::Ordering::Equal
                {
                    idx -= 1;
                }
                Some(idx)
            }
            Err(_) => None,
        }
    }

    /// Finds the leftmost index of entries that are greater than or equal to the target term.
    ///
    /// This function is crucial for prefix searches and range queries, as it locates
    /// the starting point for iteration. If all entries are less than the target,
    /// returns the length of the entries slice (one past the end).
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of entries being searched
    /// * `F` - The type of the getter function that extracts terms from entries
    ///
    /// # Arguments
    ///
    /// * `entries` - Slice of entries to search within
    /// * `term` - The target term for the lower bound search
    /// * `collation` - Collation strategy for term comparison
    /// * `get_term` - Function that extracts the term string from an entry
    ///
    /// # Returns
    ///
    /// Index of the first entry >= target term, or entries.len() if all are < target
    fn find_leftmost_ge<T, F>(
        entries: &[T],
        term: &str,
        collation: &dyn Collation,
        get_term: F,
    ) -> usize
    where
        F: Fn(&T) -> &str,
    {
        match entries.binary_search_by(|e| collation.compare(get_term(e), term)) {
            Ok(mut idx) => {
                while idx > 0
                    && collation.compare(get_term(&entries[idx - 1]), term)
                        != std::cmp::Ordering::Less
                {
                    idx -= 1;
                }
                idx
            }
            Err(i) => i,
        }
    }

    /// Recursively searches for an exact term match starting from a specified page position.
    ///
    /// Implements the core B-Tree traversal algorithm for exact term lookups. Starts from
    /// the given page position (typically the root) and navigates down through interior
    /// pages until reaching a leaf page. Uses binary search at each level for efficient
    /// navigation with O(log n) complexity.
    ///
    /// # Arguments
    ///
    /// * `term` - The exact term to search for
    /// * `page_position` - Starting page position for the search (typically root page)
    ///
    /// # Returns
    ///
    /// An iterator positioned at the first matching entry in the appropriate leaf page.
    /// If no match is found, the iterator will be marked as done.
    ///
    /// # Errors
    ///
    /// Returns an error if pages cannot be loaded, the B-Tree structure is inconsistent,
    /// or navigation encounters corrupted data.
    fn lookup_term_recursive(
        &self,
        term: &str,
        mut page_position: u64,
    ) -> Result<EntryIterator<'_>> {
        loop {
            match BTreePage::load(self, page_position)? {
                BTreePage::InteriorPage(entries) => {
                    // Choose the leftmost child whose separator term is >= target.
                    let mut idx =
                        Self::find_leftmost_ge(&entries, term, &*self.collation, |e| &e.term);
                    if idx >= entries.len() {
                        // Descend into the rightmost child when target is greater than all separators.
                        idx = entries.len() - 1;
                    }
                    page_position = entries[idx].child_position;
                }
                BTreePage::LeafPage(start_offset, entries) => {
                    return if let Some(start_index) =
                        Self::find_leftmost_equal(&entries, term, &*self.collation, |e| &e.term)
                    {
                        EntryIterator::new(
                            self,
                            term.to_owned(),
                            page_position,
                            start_offset,
                            entries,
                            start_index,
                        )
                    } else {
                        Ok(EntryIterator::empty(self))
                    };
                }
            }
        }
    }

    /// Recursively searches for terms matching a prefix starting from a specified page position.
    ///
    /// Implements B-Tree traversal for prefix-based searches. Navigates down through interior
    /// pages to find the leaf page containing the first term that could match the prefix,
    /// then creates an iterator that will scan forward through subsequent pages as needed.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix string that terms must start with
    /// * `page_position` - Starting page position for the search (typically root page)
    ///
    /// # Returns
    ///
    /// An iterator positioned at the first entry whose term is >= the prefix. The iterator
    /// will continue yielding entries as long as their terms start with the prefix.
    ///
    /// # Errors
    ///
    /// Returns an error if pages cannot be loaded, the B-Tree structure is inconsistent,
    /// or navigation encounters corrupted data.
    fn lookup_term_prefix_recursive(
        &self,
        prefix: &str,
        mut page_position: u64,
    ) -> Result<ByPrefixEntryIterator<'_>> {
        loop {
            match BTreePage::load(self, page_position)? {
                BTreePage::InteriorPage(entries) => {
                    let mut idx =
                        Self::find_leftmost_ge(&entries, prefix, &*self.collation, |e| &e.term);
                    if idx >= entries.len() {
                        idx = entries.len() - 1;
                    }
                    page_position = entries[idx].child_position;
                }
                BTreePage::LeafPage(start_offset, entries) => {
                    let start_index =
                        Self::find_leftmost_ge(&entries, prefix, &*self.collation, |e| &e.term);
                    return ByPrefixEntryIterator::new(
                        self,
                        prefix.to_owned(),
                        page_position,
                        start_offset,
                        entries,
                        start_index,
                    );
                }
            }
        }
    }

    /// Determines the position of the root page within the terms shard.
    ///
    /// In the current B-Tree implementation, pages are written in bottom-up order
    /// with the root page being written last. Therefore, the root page is always
    /// located at the final position in the terms shard.
    ///
    /// # Returns
    ///
    /// The position/index of the root page within the terms shard
    ///
    /// # Errors
    ///
    /// Returns an error if the terms shard is empty or cannot be accessed.
    fn root_page_position(&self) -> Result<u64> {
        // Root page is always the last page written
        Ok(self.pages_count()? - 1)
    }
}

/// Lazily enumerates the entries of the B-tree matching a given exact term.
///
/// This iterator provides efficient traversal over all entries that exactly match
/// a specified term. It handles multi-page scenarios where a single term's postings
/// span across multiple leaf pages, automatically advancing to subsequent pages
/// as needed while maintaining proper ordering and completeness.
///
/// The iterator is stateful and tracks its current position within the B-Tree,
/// caching page data and computing position offsets incrementally for optimal
/// performance during large result set iteration.
pub struct EntryIterator<'a> {
    /// Reference to the decoder for accessing shard data and performing page loads
    decoder: &'a BTreeDecoder,
    /// The exact term being searched for, stored for comparison during iteration
    term: String,
    /// Current leaf page position within the terms shard being processed
    curr_page_pos: u64,
    /// Cached entries from the current leaf page to avoid repeated page loads
    curr_entries: Vec<BTreeLeafEntry>,
    /// Index of the next entry within curr_entries to examine during iteration
    curr_index: usize,
    /// Cached starting offset for position data reads, computed once per page/entry
    curr_position_offset: u64,
    /// Flag indicating whether iteration has completed (no more matching entries)
    done: bool,
    /// Current stripe index within the current leaf entry
    within_stripe_idx: usize,
    /// Current field index within the current stripe of the current leaf entry
    within_field_idx: usize,
}

impl<'a> EntryIterator<'a> {
    /// Creates a new iterator for exact term matching positioned at the specified starting point.
    ///
    /// Initializes the iterator state with the current page data and determines the starting
    /// position for iteration. If a start_index is provided, iteration begins at that position;
    /// otherwise, the iterator is marked as done (indicating no matches were found).
    ///
    /// # Arguments
    ///
    /// * `decoder` - Mutable reference to the BTreeDecoder for data access
    /// * `term` - The exact term being searched for
    /// * `page_pos` - Position of the leaf page containing potential matches
    /// * `entries` - Pre-loaded entries from the leaf page
    /// * `start_index` - Optional starting index within entries, None if no matches found
    ///
    /// # Returns
    ///
    /// A new BTreeEntriesIterator ready for iteration over matching entries
    fn new(
        decoder: &'a BTreeDecoder,
        term: String,
        page_pos: u64,
        page_start_offset: u64,
        entries: Vec<BTreeLeafEntry>,
        start_index: usize,
    ) -> Result<Self> {
        let curr_position_offset = if start_index == 0 {
            page_start_offset
        } else {
            let prev = entries.get(start_index - 1).ok_or_else(|| {
                Error::invalid_format(
                    "Corrupted B-Tree leaf: missing prior entry for offset computation",
                )
            })?;
            prev.last_position_offset().ok_or_else(|| {
                Error::invalid_format(
                    "Corrupted B-Tree leaf entry: missing term positions or fields",
                )
            })?
        };
        Ok(Self {
            decoder,
            term,
            curr_page_pos: page_pos,
            curr_entries: entries,
            curr_index: start_index,
            curr_position_offset,
            done: false,
            within_stripe_idx: 0,
            within_field_idx: 0,
        })
    }

    pub fn empty(decoder: &'a BTreeDecoder) -> Self {
        Self {
            decoder,
            term: String::new(),
            curr_page_pos: 0,
            curr_entries: Vec::new(),
            curr_index: 0,
            curr_position_offset: 0,
            done: true,
            within_stripe_idx: 0,
            within_field_idx: 0,
        }
    }

    /// Advances the iterator to the next leaf page that contains the target term.
    ///
    /// Scans forward through the B-Tree pages starting from the current position,
    /// skipping over interior pages and examining leaf pages for continued matches.
    /// If a leaf page is found where the first entry matches the target term,
    /// the iterator state is updated to that page.
    ///
    /// # Returns
    ///
    /// Ok(true) if successfully advanced to a leaf page with matching terms at index 0,
    /// Ok(false) if no more matching pages are found or the end is reached
    ///
    /// # Errors
    ///
    /// Returns an error if page loading fails or the B-Tree structure is corrupted
    fn move_to_next_leaf(&mut self) -> Result<bool> {
        // Returns Ok(true) if advanced to a leaf containing the term (at index 0), else Ok(false)
        let total_pages = self.decoder.pages_count()?;
        let mut pos = self.curr_page_pos + 1;
        while pos < total_pages {
            match BTreePage::load(self.decoder, pos)? {
                BTreePage::InteriorPage(_) => {
                    // Skip interior pages
                    pos += 1;
                    continue;
                }
                BTreePage::LeafPage(start_offset, entries) => {
                    // If the next leaf has more of the same term, it must start at index 0
                    if let Some(first) = entries.first() {
                        if self.decoder.collation.compare(&first.term, &self.term)
                            == std::cmp::Ordering::Equal
                        {
                            self.curr_page_pos = pos;
                            self.curr_entries = entries;
                            self.curr_index = 0;
                            // First entry on a new page starts at the page-level offset
                            self.curr_position_offset = start_offset;
                            return Ok(true);
                        }
                    }
                    // Either empty leaf (unexpected) or we've passed the target term
                    return Ok(false);
                }
            }
        }
        Ok(false)
    }
}

impl<'a> Iterator for EntryIterator<'a> {
    type Item = Result<BTreeEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            if self.curr_index < self.curr_entries.len() {
                let entry = &self.curr_entries[self.curr_index];
                // Stop if we've moved past the searched term
                if self.decoder.collation.compare(&entry.term, &self.term)
                    != std::cmp::Ordering::Equal
                {
                    self.done = true;
                    return None;
                }

                // Advance to next available field within the current entry
                while self.within_stripe_idx < entry.term_positions.len() {
                    let tp = &entry.term_positions[self.within_stripe_idx];
                    if self.within_field_idx < tp.fields.len() {
                        let field_data = &tp.fields[self.within_field_idx];
                        let start = self.curr_position_offset;
                        let end = field_data.pos_list_end_offset;
                        self.curr_position_offset = end;
                        self.within_field_idx += 1;

                        return Some(Ok(BTreeEntry {
                            stripe: tp.stripe_id,
                            field: field_data.field_schema_id,
                            repr_type: field_data.repr_type,
                            range: start..end,
                        }));
                    }

                    // Move to next stripe within the entry
                    self.within_stripe_idx += 1;
                    self.within_field_idx = 0;
                }

                // Finished this entry; prepare to move to the next entry on the page
                self.curr_index += 1;
                self.within_stripe_idx = 0;
                self.within_field_idx = 0;

                // If there is a next entry on the page and it still matches the term, we keep
                // the curr_position_offset as-is (it was advanced to the last field's end),
                // which is exactly the start offset for the next entry's first field.
                continue;
            }

            // Move to next leaf page that still contains the term
            match self.move_to_next_leaf() {
                Ok(true) => {
                    // Reset intra-entry iterators for the new page
                    self.within_stripe_idx = 0;
                    self.within_field_idx = 0;
                    continue;
                }
                Ok(false) => {
                    self.done = true;
                    return None;
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

/// Lazily enumerates all leaf entries whose terms start with a given prefix.
///
/// This iterator performs a lower-bound seek to the first entry >= prefix and then
/// scans forward page-by-page, yielding postings for each matching entry until it
/// reaches a term that does not start with the prefix. Unlike the exact term iterator,
/// this iterator continues across multiple terms as long as they share the prefix.
///
/// The iterator efficiently handles large result sets that span multiple pages,
/// automatically advancing through the B-Tree structure while maintaining proper
/// ordering and ensuring all matching entries are processed.
pub struct ByPrefixEntryIterator<'a> {
    /// Reference to the decoder for accessing shard data and performing page operations
    decoder: &'a BTreeDecoder,
    /// The prefix string that terms must start with to be included in results
    prefix: String,
    /// Current leaf page position within the terms shard being processed
    curr_page_pos: u64,
    /// Cached entries from the current leaf page to minimize page loading overhead
    curr_entries: Vec<BTreeLeafEntry>,
    /// Index of the next entry within curr_entries to examine during iteration
    curr_index: usize,
    /// Cached starting offset for position data reads, computed once per page/entry
    curr_position_offset: u64,
    /// Flag indicating whether iteration has completed (no more matching entries)
    done: bool,
    /// Current stripe index within the current leaf entry
    within_stripe_idx: usize,
    /// Current field index within the current stripe of the current leaf entry
    within_field_idx: usize,
}

impl<'a> ByPrefixEntryIterator<'a> {
    /// Creates a new iterator for prefix-based term matching positioned at the specified starting point.
    ///
    /// Initializes the iterator state with the current page data and determines the starting
    /// position for iteration. The start_index indicates the first entry that could potentially
    /// match the prefix, and iteration will continue until terms no longer match the prefix.
    ///
    /// # Arguments
    ///
    /// * `decoder` - Mutable reference to the BTreeDecoder for data access
    /// * `prefix` - The prefix string that terms must start with
    /// * `page_pos` - Position of the leaf page containing potential matches
    /// * `entries` - Pre-loaded entries from the leaf page
    /// * `start_index` - Starting index within entries for iteration
    ///
    /// # Returns
    ///
    /// A new BTreeByPrefixEntriesIterator ready for iteration over prefix-matching entries
    fn new(
        decoder: &'a BTreeDecoder,
        prefix: String,
        page_pos: u64,
        page_start_offset: u64,
        entries: Vec<BTreeLeafEntry>,
        start_index: usize,
    ) -> Result<Self> {
        let curr_position_offset = if start_index == 0 {
            page_start_offset
        } else {
            let prev = entries.get(start_index - 1).ok_or_else(|| {
                Error::invalid_format(
                    "Corrupted B-Tree leaf: missing prior entry for offset computation",
                )
            })?;
            prev.last_position_offset().ok_or_else(|| {
                Error::invalid_format(
                    "Corrupted B-Tree leaf entry: missing term positions or fields",
                )
            })?
        };
        Ok(Self {
            decoder,
            prefix,
            curr_page_pos: page_pos,
            curr_entries: entries,
            curr_index: start_index,
            curr_position_offset,
            done: false,
            within_stripe_idx: 0,
            within_field_idx: 0,
        })
    }

    /// Advances the iterator to the next leaf page for continued prefix matching.
    ///
    /// Scans forward through the B-Tree pages starting from the current position,
    /// skipping over interior pages and examining leaf pages. Unlike the exact term
    /// iterator, this method doesn't check for specific term matches since prefix
    /// matching will be validated in the main iteration logic.
    ///
    /// # Returns
    ///
    /// Ok(true) if successfully advanced to a non-empty leaf page,
    /// Ok(false) if no more pages are available or the end is reached
    ///
    /// # Errors
    ///
    /// Returns an error if page loading fails or the B-Tree structure is corrupted
    fn move_to_next_leaf(&mut self) -> Result<bool> {
        let total_pages = self.decoder.pages_count()?;
        let mut pos = self.curr_page_pos + 1;
        while pos < total_pages {
            match BTreePage::load(self.decoder, pos)? {
                BTreePage::InteriorPage(_) => {
                    pos += 1;
                    continue;
                }
                BTreePage::LeafPage(start_offset, entries) => {
                    if entries.is_empty() {
                        pos += 1;
                        continue;
                    }
                    // Start from the first entry of the next leaf. If it no longer matches the
                    // prefix, the iterator's next() will terminate.
                    self.curr_page_pos = pos;
                    self.curr_entries = entries;
                    self.curr_index = 0;
                    // First entry on a new page starts at the page-level offset
                    self.curr_position_offset = start_offset;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

impl<'a> Iterator for ByPrefixEntryIterator<'a> {
    type Item = Result<BTreeEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            if self.curr_index < self.curr_entries.len() {
                let entry = &self.curr_entries[self.curr_index];

                if self
                    .decoder
                    .collation
                    .starts_with(&entry.term, &self.prefix)
                {
                    // Produce a single BTreeEntry at a time by walking fields within stripes
                    while self.within_stripe_idx < entry.term_positions.len() {
                        let tp = &entry.term_positions[self.within_stripe_idx];
                        if self.within_field_idx < tp.fields.len() {
                            let field_data = &tp.fields[self.within_field_idx];
                            let start = self.curr_position_offset;
                            let end = field_data.pos_list_end_offset;
                            self.curr_position_offset = end;
                            self.within_field_idx += 1;

                            return Some(Ok(BTreeEntry {
                                stripe: tp.stripe_id,
                                field: field_data.field_schema_id,
                                repr_type: field_data.repr_type,
                                range: start..end,
                            }));
                        }

                        self.within_stripe_idx += 1;
                        self.within_field_idx = 0;
                    }

                    // Finished this entry; move to next
                    self.curr_index += 1;
                    self.within_stripe_idx = 0;
                    self.within_field_idx = 0;
                    continue;
                }

                // Stop if we've moved past the prefix.
                self.done = true;
                return None;
            }

            match self.move_to_next_leaf() {
                Ok(true) => {
                    self.within_stripe_idx = 0;
                    self.within_field_idx = 0;
                    continue;
                }
                Ok(false) => {
                    self.done = true;
                    return None;
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

/// A single term occurrence entry from the B-Tree index.
///
/// `BTreeEntry` represents one occurrence of a term in the text index, providing
/// the necessary metadata to locate and read the actual position data for that term.
/// Each entry describes where a term appears within the data structure hierarchy
/// and how to interpret the associated position information.
///
/// ## Structure Hierarchy
///
/// The entry references data organized in this hierarchy:
/// - **Stripe**: A logical data partition containing multiple fields
/// - **Field**: A specific field within a document schema (e.g., "title", "content")
/// - **Positions**: The actual locations where the term appears within that field
///
/// ## Position Data Access
///
/// The `range` field provides byte offsets into the positions shard where the
/// term's position data is stored. The `repr_type` indicates how this position
/// data should be interpreted when read from the shard.
///
/// ## Usage Context
///
/// Entries are typically produced by iterators over B-Tree search results:
/// - [`ByTermEntryIterator`] for exact term matches
/// - [`ByPrefixEntryIterator`] for prefix-based searches
///
/// Each entry allows consumers to:
/// 1. Identify the data context (stripe and field)
/// 2. Read position data from the positions shard using the range
/// 3. Interpret position data according to the representation type
pub struct BTreeEntry {
    /// The data stripe identifier where the term appears.
    ///
    /// Stripes represent logical partitions of the indexed data, typically
    /// corresponding to batches of documents processed together. Each stripe
    /// maintains its own position numbering scheme.
    pub stripe: u16,

    /// The field identifier within the document schema where the term appears.
    ///
    /// Fields correspond to specific attributes or columns in the document
    /// structure (e.g., title, content, metadata fields). The schema ID
    /// provides a stable reference to the field definition.
    pub field: SchemaId,

    /// The representation type for the position data stored in the positions shard.
    ///
    /// Determines how to interpret the binary data when reading from the range:
    /// - [`PositionsReprType::Positions`]: Individual position values
    /// - [`PositionsReprType::ExactRanges`]: Precise position ranges
    /// - [`PositionsReprType::ApproximateRanges`]: Approximate position ranges
    ///
    /// Different representations offer trade-offs between precision and storage efficiency.
    pub repr_type: PositionsReprType,

    /// The byte range in the positions shard containing this term's position data.
    ///
    /// Provides the start and end offsets for reading position information from
    /// the positions shard. The exact format of the data within this range is
    /// determined by the `repr_type` field.
    ///
    /// This range is typically obtained during B-Tree traversal and represents
    /// a contiguous slice of position data specific to this term occurrence.
    pub range: Range<u64>,
}

/// Represents a single page within the B-Tree hierarchical structure.
///
/// Pages are the fundamental storage units of the B-Tree and can be either interior
/// pages (containing navigation data and child pointers) or leaf pages (containing
/// actual term data and position references).
///
/// Interior pages contain entries that guide navigation to child pages.
/// Leaf pages contain the actual indexed terms and their associated position data.
#[derive(Debug, Clone)]
enum BTreePage {
    /// A leaf page (level 0) containing actual term data and position references.
    ///
    /// Leaf pages represent the bottom level of the B-Tree hierarchy and store
    /// the actual indexed terms along with their position data references.
    /// The u64 is the page-level starting offset in the positions shard.
    LeafPage(u64, Vec<BTreeLeafEntry>),
    /// An interior page (level > 0) containing navigation data and child pointers.
    ///
    /// Interior pages guide navigation through the B-Tree hierarchy, with level 1 being
    /// the parents of leaf pages, level 2 being grandparents, and so on.
    InteriorPage(Vec<BTreeInteriorEntry>),
}

impl BTreePage {
    /// Loads and parses a B-Tree page from the terms shard at the specified position.
    ///
    /// Reads a single page from the terms shard using ArrowReaderBuilder and parses
    /// the Arrow record batch into a `BTreePage` structure. The page contains level
    /// information and a list of entries with terms, child positions, and term position data.
    ///
    /// # Arguments
    ///
    /// * `decoder` - The BTreeDecoder instance used to access the terms shard
    /// * `position` - The position of the page within the terms shard
    ///
    /// # Returns
    ///
    /// A `BTreePage` enum representing either a leaf or interior page
    ///
    /// # Errors
    ///
    /// Returns an error if the page cannot be read, parsed, or if the expected
    /// structure is not found in the Arrow record batch.
    pub fn load(decoder: &BTreeDecoder, position: u64) -> Result<Self> {
        let total_pages = decoder.pages_count()?;

        // Validate page position
        if position >= total_pages {
            return Err(Error::invalid_format("Page position out of bounds"));
        }

        // Create ArrowReaderBuilder for the terms shard
        let arrow_reader_builder = ArrowReaderBuilder::try_new(&decoder.shard_url)
            .map_err(|e| {
                Error::invalid_format(format!("Failed to create ArrowReaderBuilder: {e}"))
            })?
            .with_object_store(Arc::clone(&decoder.object_store))
            .with_batch_size(1); // Read one page at a time

        // Set position range to read only the specific page
        let page_range = SharedRangeList::from_elem(position..(position + 1));
        let reader = arrow_reader_builder
            .with_position_ranges(page_range)
            .build()
            .map_err(|e| Error::invalid_format(format!("Failed to build reader: {e}")))?;

        // Read the record batch containing our page
        let mut page_batch = None;
        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| Error::invalid_format(format!("Failed to read batch: {e}")))?;
            if batch.num_rows() > 0 {
                page_batch = Some(batch);
                break;
            }
        }

        let batch =
            page_batch.ok_or_else(|| Error::invalid_format("No data found for page position"))?;

        // Extract level field (UInt8Array)
        let level_column = batch
            .column_by_name("level")
            .ok_or_else(|| Error::invalid_format("Missing level column"))?;
        let level_array = level_column
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| Error::invalid_format("Level column is not UInt8Array"))?;
        let level = level_array.value(0);

        // Extract entries field (LargeListArray of Struct)
        let entries_column = batch
            .column_by_name("entries")
            .ok_or_else(|| Error::invalid_format("Missing entries column"))?;

        let entries_list_array = entries_column
            .as_any()
            .downcast_ref::<LargeListArray>()
            .ok_or_else(|| Error::invalid_format("Entries column is not LargeListArray"))?;

        // Get the struct array containing the entry data
        let entries_value = entries_list_array.value(0); // first (and only) page's entries
        let entries_struct_array = entries_value
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| Error::invalid_format("Entries list value is not StructArray"))?;

        // Extract term field (LargeBinaryArray)
        let term_column = entries_struct_array
            .column_by_name("term")
            .ok_or_else(|| Error::invalid_format("Missing term column in entries"))?;
        let term_array = term_column
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| Error::invalid_format("Term column is not LargeBinaryArray"))?;

        // Extract child_position field (UInt64Array, nullable)
        let child_position_array = entries_struct_array
            .column_by_name("child_position")
            .ok_or_else(|| Error::invalid_format("Missing child_position column in entries"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| Error::invalid_format("Child position column is not UInt64Array"))?;

        // Extract term_positions field (LargeListArray of Struct)
        let term_positions_column = entries_struct_array
            .column_by_name("term_positions")
            .ok_or_else(|| Error::invalid_format("Missing term_positions column in entries"))?;
        let term_positions_list_array = term_positions_column
            .as_any()
            .downcast_ref::<LargeListArray>()
            .ok_or_else(|| Error::invalid_format("Term positions column is not LargeListArray"))?;

        // Extract page-level start offset (UInt64Array, nullable)
        let start_offset_column = batch
            .column_by_name("pos_list_start_offset")
            .ok_or_else(|| Error::invalid_format("Missing pos_list_start_offset column"))?;
        let start_offset_array = start_offset_column
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| Error::invalid_format("pos_list_start_offset is not UInt64Array"))?;

        // Parse entries from the Arrow arrays based on page level
        if level == 0 {
            // Leaf page
            if start_offset_array.is_null(0) {
                return Err(Error::invalid_format(
                    "Leaf page missing pos_list_start_offset",
                ));
            }
            let start_offset = start_offset_array.value(0);
            let entries = Self::load_leaf_page_entries(term_array, term_positions_list_array)?;
            Ok(BTreePage::LeafPage(start_offset, entries))
        } else {
            // Interior page
            let entries = Self::load_interior_page_entries(term_array, child_position_array)?;
            Ok(BTreePage::InteriorPage(entries))
        }
    }

    /// Parses B-Tree leaf entries from Arrow array structures into native objects.
    ///
    /// Converts the nested Arrow array structure representing leaf page entries into
    /// `BTreeLeafEntry` objects. This involves extracting terms and complex nested
    /// term position data from the Arrow columnar format.
    ///
    /// # Arguments
    ///
    /// * `term_array` - Array containing term byte data for each entry
    /// * `term_positions_list_array` - Nested array structure containing term position data
    ///
    /// # Returns
    ///
    /// A vector of parsed `BTreeLeafEntry` objects ready for use in page operations
    ///
    /// # Errors
    ///
    /// Returns an error if the Arrow array structure is malformed, expected
    /// columns are missing, or data types don't match the expected schema.
    fn load_leaf_page_entries(
        term_array: &LargeBinaryArray,
        term_positions_list_array: &LargeListArray,
    ) -> Result<Vec<BTreeLeafEntry>> {
        use arrow_array::{Array, StructArray, UInt8Array, UInt32Array};

        let num_entries = term_array.len();
        if num_entries == 0 {
            return Err(Error::invalid_format("Leaf page entries cannot be empty"));
        }
        let mut entries = Vec::with_capacity(num_entries);

        for entry_idx in 0..num_entries {
            // Extract term bytes and convert to String
            // SAFETY: Terms are guaranteed to be valid UTF-8 as they are produced by tokenizers
            let term_bytes = term_array.value(entry_idx).to_vec();
            let term = unsafe { String::from_utf8_unchecked(term_bytes) };

            // Extract term positions
            let mut term_positions = Vec::new();

            // Every leaf entry should have term positions since the encoder enforces
            // that every term must have at least one posting
            if term_positions_list_array.is_null(entry_idx) {
                return Err(Error::invalid_format(
                    "Term positions should never be null for leaf entries. \
                     This indicates data corruption or an incompatible encoder version.",
                ));
            }

            let term_positions_value = term_positions_list_array.value(entry_idx);
            let term_positions_struct_array = term_positions_value
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| Error::invalid_format("Term positions is not StructArray"))?;

            // Extract stripe_id array
            let stripe_id_array = term_positions_struct_array
                .column_by_name("stripe_id")
                .ok_or_else(|| Error::invalid_format("Missing stripe_id in term_positions"))?
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| Error::invalid_format("Stripe ID is not UInt16Array"))?;

            // Extract fields list array
            let fields_list_array = term_positions_struct_array
                .column_by_name("fields")
                .ok_or_else(|| Error::invalid_format("Missing fields in term_positions"))?
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| Error::invalid_format("Fields is not LargeListArray"))?;

            let num_stripes = stripe_id_array.len();
            for stripe_idx in 0..num_stripes {
                let stripe_id = stripe_id_array.value(stripe_idx);
                let mut fields = Vec::new();

                if !fields_list_array.is_null(stripe_idx) {
                    let fields_value = fields_list_array.value(stripe_idx);
                    let fields_struct_array = fields_value
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| {
                        Error::invalid_format("Fields list value is not StructArray")
                    })?;

                    // Extract field arrays
                    let field_schema_id_array = fields_struct_array
                        .column_by_name("field_schema_id")
                        .ok_or_else(|| Error::invalid_format("Missing field_schema_id in fields"))?
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .ok_or_else(|| {
                            Error::invalid_format("Field schema ID is not UInt32Array")
                        })?;

                    let repr_type_array = fields_struct_array
                        .column_by_name("repr_type")
                        .ok_or_else(|| Error::invalid_format("Missing repr_type in fields"))?
                        .as_any()
                        .downcast_ref::<UInt8Array>()
                        .ok_or_else(|| Error::invalid_format("Repr type is not UInt8Array"))?;

                    let pos_list_end_offset_array = fields_struct_array
                        .column_by_name("pos_list_end_offset")
                        .ok_or_else(|| {
                            Error::invalid_format("Missing pos_list_end_offset in fields")
                        })?
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| {
                            Error::invalid_format("Pos list end offset is not UInt64Array")
                        })?;

                    let num_fields = field_schema_id_array.len();
                    for field_idx in 0..num_fields {
                        fields.push(FieldPositionData {
                            field_schema_id: SchemaId::from(field_schema_id_array.value(field_idx)),
                            repr_type: repr_type_array.value(field_idx).try_into().map_err(
                                |_| Error::invalid_format("Repr type value out of bounds"),
                            )?,
                            pos_list_end_offset: pos_list_end_offset_array.value(field_idx),
                        });
                    }
                }

                term_positions.push(TermPositionData { stripe_id, fields });
            }

            entries.push(BTreeLeafEntry {
                term,
                term_positions,
            });
        }

        Ok(entries)
    }

    /// Parses B-Tree interior entries from Arrow array structures into native objects.
    ///
    /// Converts the Arrow array structure representing interior page entries into
    /// `BTreeInteriorEntry` objects. This involves extracting terms and child positions
    /// from the Arrow columnar format.
    ///
    /// # Arguments
    ///
    /// * `term_array` - Array containing term byte data for each entry
    /// * `child_position_array` - Array containing child page positions
    ///
    /// # Returns
    ///
    /// A vector of parsed `BTreeInteriorEntry` objects ready for use in page operations
    ///
    /// # Errors
    ///
    /// Returns an error if the Arrow array structure is malformed, expected
    /// columns are missing, or data types don't match the expected schema.
    fn load_interior_page_entries(
        term_array: &LargeBinaryArray,
        child_position_array: &UInt64Array,
    ) -> Result<Vec<BTreeInteriorEntry>> {
        let num_entries = term_array.len();
        if num_entries == 0 {
            return Err(Error::invalid_format(
                "Interior page entries cannot be empty",
            ));
        }
        let mut entries = Vec::with_capacity(num_entries);

        for entry_idx in 0..num_entries {
            // Extract term bytes and convert to String
            // SAFETY: Terms are guaranteed to be valid UTF-8 as they are produced by tokenizers
            let term_bytes = term_array.value(entry_idx).to_vec();
            let term = unsafe { String::from_utf8_unchecked(term_bytes) };

            // Extract child position (should not be null for interior entries)
            if child_position_array.is_null(entry_idx) {
                return Err(Error::invalid_format(
                    "Interior entry missing child position",
                ));
            }
            let child_position = child_position_array.value(entry_idx);

            entries.push(BTreeInteriorEntry {
                term,
                child_position,
            });
        }

        Ok(entries)
    }
}

/// Represents a single entry within a B-Tree leaf page, containing term data and position information.
///
/// Leaf entries contain the actual indexed terms along with references to position lists
/// stored in the separate positions shard. These entries form the terminal nodes of the
/// B-Tree and contain the data that query results are constructed from.
#[derive(Debug, Clone)]
struct BTreeLeafEntry {
    /// The term stored in this entry as a UTF-8 string.
    ///
    /// Terms are guaranteed to be valid UTF-8 strings as they are produced by
    /// tokenizers that ensure proper encoding. This allows for efficient string
    /// operations and collation comparisons without additional validation.
    pub term: String,
    /// Term position data for this leaf entry, organized by stripe and field.
    ///
    /// This field contains the position information structured to support
    /// efficient reconstruction of term postings during query processing.
    pub term_positions: Vec<TermPositionData>,
}

impl BTreeLeafEntry {
    pub fn last_position_offset(&self) -> Option<u64> {
        self.term_positions
            .last()
            .and_then(|tp| tp.fields.last())
            .map(|f| f.pos_list_end_offset)
    }
}

/// Represents a single entry within a B-Tree interior page, containing term data and child navigation information.
///
/// Interior entries provide navigation guidance with child page pointers. These entries
/// do not contain position data themselves but guide the traversal to the appropriate
/// child pages where the actual term data can be found.
#[derive(Debug, Clone)]
struct BTreeInteriorEntry {
    /// The term stored in this entry as a UTF-8 string.
    ///
    /// Terms are guaranteed to be valid UTF-8 strings as they are produced by
    /// tokenizers that ensure proper encoding. This allows for efficient string
    /// operations and collation comparisons without additional validation.
    pub term: String,
    /// Position of the child page for this interior page entry.
    ///
    /// This field provides the location of the child page to follow during
    /// tree navigation based on term comparison results.
    pub child_position: u64,
}

/// Aggregates term position data for a specific stripe within the text index.
///
/// This structure organizes position information by stripe identifier, which
/// represents a logical grouping or partition of the indexed data. Each stripe
/// can contain multiple fields, and this structure maintains the field-specific
/// position data for efficient access during term posting reconstruction.
///
/// Stripes provide a way to partition large datasets and enable parallel
/// processing during both indexing and querying operations.
#[derive(Debug, Clone)]
struct TermPositionData {
    /// The unique identifier for the stripe containing this term's position data.
    ///
    /// Stripes represent logical partitions of the indexed data and enable
    /// parallel processing. This identifier allows the system to correctly
    /// associate position data with the appropriate data partition.
    pub stripe_id: u16,
    /// Collection of field-specific position data within this stripe.
    ///
    /// Each field represents a different indexed column or attribute within
    /// the stripe. The position data is organized by field to support
    /// field-specific queries and efficient posting reconstruction.
    pub fields: Vec<FieldPositionData>,
}

/// Contains position data and metadata for a specific field within a stripe.
///
/// This structure provides the necessary information to locate and read position
/// lists from the positions shard. It includes field identification, representation
/// type information, and offset data for efficient random access to position arrays.
///
/// The offset-based approach allows for variable-length position lists while
/// maintaining efficient access patterns for reading specific ranges of positions.
#[derive(Debug, Clone)]
struct FieldPositionData {
    /// The schema field identifier for this position data.
    ///
    /// This identifier corresponds to a specific field definition in the schema,
    /// allowing the system to correctly associate position data with the
    /// appropriate field during query processing and result reconstruction.
    pub field_schema_id: SchemaId,
    /// The representation type indicating how positions are encoded and stored.
    ///
    /// Different representation types may use different encoding strategies
    /// for position data. For example, type 0 typically indicates a standard
    /// position list encoding optimized for sequential access patterns.
    pub repr_type: PositionsReprType,
    /// The end offset in the positions array for this field's position data.
    ///
    /// This offset, combined with the previous field's end offset (or 0 for
    /// the first field), defines the range of positions belonging to this
    /// field within the positions shard array.
    pub pos_list_end_offset: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        collation::{UnicodeCaseInsensitiveCollation, UnicodeCasePreservingCollation},
        pos_list::PositionsReprType,
        write::btree::BTreeEncoder,
    };
    use amudai_format::schema::SchemaId;
    use amudai_io_impl::temp_file_store;
    use amudai_objectstore::{
        local_store::{LocalFsMode, LocalFsObjectStore},
        url::RelativePath,
    };
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_btree_decoder_exact_term_lookup() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let object_store = Arc::new(
            LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot)
                .expect("Failed to create LocalFsObjectStore"),
        );

        // Create the B-Tree
        let temp_store = temp_file_store::create_in_memory(10 * 1024 * 1024)
            .expect("Failed to create temporary file store");
        let mut encoder = BTreeEncoder::new(object_store.clone(), temp_store.clone())
            .expect("Failed to create BTreeEncoder");

        let field1 = SchemaId::from(1u32);
        let field2 = SchemaId::from(2u32);
        let mut next_off: u64 = 100; // Start with offset 100 for easy validation

        let terms = ["alpha", "beta", "gamma", "delta", "epsilon"];

        for (idx, term) in terms.iter().enumerate() {
            for stripe in 0..=1u16 {
                let range_start = next_off;
                next_off += 10; // Fixed offset ranges for predictable testing
                let range = range_start..next_off;

                encoder
                    .push(
                        term.as_bytes(),
                        stripe,
                        field1,
                        range,
                        PositionsReprType::Positions,
                    )
                    .unwrap_or_else(|_| panic!("Failed to push term: {term}"));

                // Add second field for some terms
                if idx % 2 == 0 {
                    let range_start = next_off;
                    next_off += 5;
                    let range = range_start..next_off;

                    encoder
                        .push(
                            term.as_bytes(),
                            stripe,
                            field2,
                            range,
                            PositionsReprType::ExactRanges,
                        )
                        .unwrap_or_else(|_| panic!("Failed to push term: {term}"));
                }
            }
        }

        let prepared = encoder.finish().expect("Failed to finish BTreeEncoder");
        let shard_url = object_store
            .container_url()
            .resolve_relative(RelativePath::new("simple_btree.amudai.shard").unwrap())
            .unwrap();
        let _sealed_terms = prepared
            .seal(shard_url.as_str())
            .expect("Failed to seal terms shard");

        // Now test the decoder
        let collation = Box::new(UnicodeCasePreservingCollation);
        let decoder = BTreeDecoder::new(shard_url.to_string(), object_store, collation)
            .expect("Failed to create BTreeDecoder");

        // Test exact term lookup for "alpha"
        let results = decoder
            .lookup_term("alpha")
            .expect("Failed to lookup 'alpha'");
        let mut entries = Vec::new();
        for entry_result in results {
            let entry = entry_result.expect("Failed to get entry");
            entries.push(entry);
        }

        // Should have 4 entries: 2 stripes × 2 fields (field1 always, field2 for even indices)
        assert_eq!(entries.len(), 4, "Expected 4 entries for 'alpha'");

        // Validate stripe and field distributions
        let stripe_0_field_1 = entries
            .iter()
            .find(|e| e.stripe == 0 && e.field == SchemaId::from(1u32));
        let stripe_1_field_1 = entries
            .iter()
            .find(|e| e.stripe == 1 && e.field == SchemaId::from(1u32));
        let stripe_0_field_2 = entries
            .iter()
            .find(|e| e.stripe == 0 && e.field == SchemaId::from(2u32));
        let stripe_1_field_2 = entries
            .iter()
            .find(|e| e.stripe == 1 && e.field == SchemaId::from(2u32));

        assert!(
            stripe_0_field_1.is_some(),
            "Missing stripe 0, field 1 for 'alpha'"
        );
        assert!(
            stripe_1_field_1.is_some(),
            "Missing stripe 1, field 1 for 'alpha'"
        );
        assert!(
            stripe_0_field_2.is_some(),
            "Missing stripe 0, field 2 for 'alpha'"
        );
        assert!(
            stripe_1_field_2.is_some(),
            "Missing stripe 1, field 2 for 'alpha'"
        );

        // Validate that ranges contain expected offsets
        // Based on our push pattern: alpha is index 0, starts at offset 100
        let expected_field1_stripe0_range = 100..110;
        let expected_field2_stripe0_range = 110..115;
        let expected_field1_stripe1_range = 115..125;
        let expected_field2_stripe1_range = 125..130;

        assert_eq!(
            stripe_0_field_1.unwrap().range,
            expected_field1_stripe0_range
        );
        assert_eq!(
            stripe_0_field_2.unwrap().range,
            expected_field2_stripe0_range
        );
        assert_eq!(
            stripe_1_field_1.unwrap().range,
            expected_field1_stripe1_range
        );
        assert_eq!(
            stripe_1_field_2.unwrap().range,
            expected_field2_stripe1_range
        );

        // Validate position data fields are correct
        assert_eq!(stripe_0_field_1.unwrap().field, SchemaId::from(1u32));
        assert_eq!(stripe_0_field_1.unwrap().stripe, 0);
        assert_eq!(
            stripe_0_field_1.unwrap().repr_type,
            PositionsReprType::Positions
        );

        assert_eq!(stripe_0_field_2.unwrap().field, SchemaId::from(2u32));
        assert_eq!(stripe_0_field_2.unwrap().stripe, 0);
        assert_eq!(
            stripe_0_field_2.unwrap().repr_type,
            PositionsReprType::ExactRanges
        );

        assert_eq!(stripe_1_field_1.unwrap().field, SchemaId::from(1u32));
        assert_eq!(stripe_1_field_1.unwrap().stripe, 1);
        assert_eq!(
            stripe_1_field_1.unwrap().repr_type,
            PositionsReprType::Positions
        );

        assert_eq!(stripe_1_field_2.unwrap().field, SchemaId::from(2u32));
        assert_eq!(stripe_1_field_2.unwrap().stripe, 1);
        assert_eq!(
            stripe_1_field_2.unwrap().repr_type,
            PositionsReprType::ExactRanges
        );
    }

    #[test]
    fn test_btree_decoder_exact_term_lookup_nonexistent() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let object_store = Arc::new(
            LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot)
                .expect("Failed to create LocalFsObjectStore"),
        );

        // Create the B-Tree
        let temp_store = temp_file_store::create_in_memory(10 * 1024 * 1024)
            .expect("Failed to create temporary file store");
        let mut encoder = BTreeEncoder::new(object_store.clone(), temp_store.clone())
            .expect("Failed to create BTreeEncoder");

        let field1 = SchemaId::from(1u32);
        let field2 = SchemaId::from(2u32);
        let mut next_off: u64 = 100;

        let terms = ["alpha", "beta", "gamma", "delta", "epsilon"];

        for (idx, term) in terms.iter().enumerate() {
            for stripe in 0..=1u16 {
                let range_start = next_off;
                next_off += 10;
                let range = range_start..next_off;

                encoder
                    .push(
                        term.as_bytes(),
                        stripe,
                        field1,
                        range,
                        PositionsReprType::Positions,
                    )
                    .unwrap_or_else(|_| panic!("Failed to push term: {term}"));

                if idx % 2 == 0 {
                    let range_start = next_off;
                    next_off += 5;
                    let range = range_start..next_off;

                    encoder
                        .push(
                            term.as_bytes(),
                            stripe,
                            field2,
                            range,
                            PositionsReprType::ExactRanges,
                        )
                        .unwrap_or_else(|_| panic!("Failed to push term: {term}"));
                }
            }
        }

        let prepared = encoder.finish().expect("Failed to finish BTreeEncoder");
        let shard_url = object_store
            .container_url()
            .resolve_relative(RelativePath::new("simple_btree.amudai.shard").unwrap())
            .unwrap();
        let _sealed_terms = prepared
            .seal(shard_url.as_str())
            .expect("Failed to seal terms shard");

        let collation = Box::new(UnicodeCasePreservingCollation);
        let decoder = BTreeDecoder::new(shard_url.to_string(), object_store, collation)
            .expect("Failed to create BTreeDecoder");

        // Test lookup for non-existent term
        let results = decoder
            .lookup_term("zzzz")
            .expect("Failed to lookup 'zzzz'");
        let entries: Vec<_> = results.collect();
        assert!(
            entries.is_empty(),
            "Expected no entries for non-existent term 'zzzz'"
        );

        // Test lookup for term that would come before all existing terms
        let results = decoder.lookup_term("aaa").expect("Failed to lookup 'aaa'");
        let entries: Vec<_> = results.collect();
        assert!(
            entries.is_empty(),
            "Expected no entries for non-existent term 'aaa'"
        );
    }

    #[test]
    fn test_btree_decoder_prefix_lookup() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let object_store = Arc::new(
            LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot)
                .expect("Failed to create LocalFsObjectStore"),
        );

        // Create the B-Tree
        let temp_store = temp_file_store::create_in_memory(10 * 1024 * 1024)
            .expect("Failed to create temporary file store");
        let mut encoder = BTreeEncoder::new(object_store.clone(), temp_store.clone())
            .expect("Failed to create BTreeEncoder");

        let field1 = SchemaId::from(1u32);
        let mut next_off: u64 = 100;

        let terms = ["alpha", "beta", "gamma", "delta", "epsilon"];

        for term in terms.iter() {
            let range_start = next_off;
            next_off += 10;
            let range = range_start..next_off;

            encoder
                .push(
                    term.as_bytes(),
                    0,
                    field1,
                    range,
                    PositionsReprType::Positions,
                )
                .unwrap_or_else(|_| panic!("Failed to push term: {term}"));
        }

        let prepared = encoder.finish().expect("Failed to finish BTreeEncoder");
        let shard_url = object_store
            .container_url()
            .resolve_relative(RelativePath::new("prefix_test_btree.amudai.shard").unwrap())
            .unwrap();
        let _sealed_terms = prepared
            .seal(shard_url.as_str())
            .expect("Failed to seal terms shard");

        let collation = Box::new(UnicodeCasePreservingCollation);
        let decoder = BTreeDecoder::new(shard_url.to_string(), object_store, collation)
            .expect("Failed to create BTreeDecoder");

        // Test prefix lookup for "a" - should match "alpha"
        let results = decoder
            .lookup_term_prefix("a")
            .expect("Failed to lookup prefix 'a'");
        let entries: Vec<_> = results.collect();
        let valid_entries: Vec<_> = entries.into_iter().filter_map(|r| r.ok()).collect();
        assert_eq!(valid_entries.len(), 1, "Expected 1 entry for prefix 'a'");

        // Test prefix lookup for "be" - should match "beta"
        let results = decoder
            .lookup_term_prefix("be")
            .expect("Failed to lookup prefix 'be'");
        let entries: Vec<_> = results.collect();
        let valid_entries: Vec<_> = entries.into_iter().filter_map(|r| r.ok()).collect();
        assert_eq!(valid_entries.len(), 1, "Expected 1 entry for prefix 'be'");

        // Test prefix lookup for "al" - should match "alpha"
        let results = decoder
            .lookup_term_prefix("al")
            .expect("Failed to lookup prefix 'al'");
        let entries: Vec<_> = results.collect();
        let valid_entries: Vec<_> = entries.into_iter().filter_map(|r| r.ok()).collect();
        assert_eq!(valid_entries.len(), 1, "Expected 1 entry for prefix 'al'");

        // Test prefix lookup for non-matching prefix
        let results = decoder
            .lookup_term_prefix("xyz")
            .expect("Failed to lookup prefix 'xyz'");
        let entries: Vec<_> = results.collect();
        assert!(
            entries.is_empty(),
            "Expected no entries for non-matching prefix 'xyz'"
        );
    }

    #[test]
    fn test_btree_decoder_unicode_terms() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let object_store = Arc::new(
            LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot)
                .expect("Failed to create LocalFsObjectStore"),
        );

        // Create the B-Tree with Unicode terms
        let temp_store = temp_file_store::create_in_memory(10 * 1024 * 1024)
            .expect("Failed to create temporary file store");
        let mut encoder = BTreeEncoder::new(object_store.clone(), temp_store.clone())
            .expect("Failed to create BTreeEncoder");

        let field1 = SchemaId::from(1u32);
        let mut next_off: u64 = 100;

        let unicode_terms = ["café", "naïve", "résumé", "русский", "中文"];

        for term in unicode_terms.iter() {
            let range_start = next_off;
            next_off += 10;
            let range = range_start..next_off;

            encoder
                .push(
                    term.as_bytes(),
                    0,
                    field1,
                    range,
                    PositionsReprType::Positions,
                )
                .unwrap_or_else(|_| panic!("Failed to push term: {term}"));
        }

        let prepared = encoder.finish().expect("Failed to finish BTreeEncoder");
        let shard_url = object_store
            .container_url()
            .resolve_relative(RelativePath::new("unicode_test_btree.amudai.shard").unwrap())
            .unwrap();
        let _sealed_terms = prepared
            .seal(shard_url.as_str())
            .expect("Failed to seal terms shard");

        let collation = Box::new(UnicodeCasePreservingCollation);
        let decoder = BTreeDecoder::new(shard_url.to_string(), object_store, collation)
            .expect("Failed to create BTreeDecoder");

        // Test exact lookup for Unicode terms
        for term in unicode_terms {
            let results = decoder
                .lookup_term(term)
                .unwrap_or_else(|_| panic!("Failed to lookup '{term}'"));
            let entries: Vec<_> = results.collect();
            let valid_entries: Vec<_> = entries.into_iter().filter_map(|r| r.ok()).collect();
            assert_eq!(
                valid_entries.len(),
                1,
                "Expected 1 entry for Unicode term '{term}'"
            );

            // Validate the range is correct
            let entry = &valid_entries[0];
            assert!(
                entry.range.start < entry.range.end,
                "Entry range should be valid for term '{term}'"
            );

            // Validate position data fields
            assert_eq!(
                entry.field,
                SchemaId::from(1u32),
                "Field should be 1 for term '{term}'"
            );
            assert_eq!(entry.stripe, 0, "Stripe should be 0 for term '{term}'");
            assert_eq!(
                entry.repr_type,
                PositionsReprType::Positions,
                "Repr type should be Positions for term '{term}'"
            );
        }
    }

    #[test]
    fn test_btree_decoder_case_sensitivity() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let object_store = Arc::new(
            LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot)
                .expect("Failed to create LocalFsObjectStore"),
        );

        // Create the B-Tree with mixed case terms
        let temp_store = temp_file_store::create_in_memory(10 * 1024 * 1024)
            .expect("Failed to create temporary file store");
        let mut encoder = BTreeEncoder::new(object_store.clone(), temp_store.clone())
            .expect("Failed to create BTreeEncoder");

        let field1 = SchemaId::from(1u32);
        let mut next_off: u64 = 100;

        let mixed_case_terms = ["Apple", "BANANA", "apple", "banana"];

        for term in mixed_case_terms.iter() {
            let range_start = next_off;
            next_off += 10;
            let range = range_start..next_off;

            encoder
                .push(
                    term.as_bytes(),
                    0,
                    field1,
                    range,
                    PositionsReprType::Positions,
                )
                .unwrap_or_else(|_| panic!("Failed to push term: {term}"));
        }

        let prepared = encoder.finish().expect("Failed to finish BTreeEncoder");
        let shard_url = object_store
            .container_url()
            .resolve_relative(RelativePath::new("case_test_btree.amudai.shard").unwrap())
            .unwrap();
        let _sealed_terms = prepared
            .seal(shard_url.as_str())
            .expect("Failed to seal terms shard");

        // Test case-sensitive collation
        let collation = Box::new(UnicodeCasePreservingCollation);
        let decoder = BTreeDecoder::new(shard_url.to_string(), object_store.clone(), collation)
            .expect("Failed to create BTreeDecoder");

        // Test case-sensitive exact lookups - different cases should be treated as different terms
        let results = decoder
            .lookup_term("apple")
            .expect("Failed to lookup 'apple'");
        let entries: Vec<_> = results.collect();
        let valid_entries: Vec<_> = entries.into_iter().filter_map(|r| r.ok()).collect();
        assert!(
            !valid_entries.is_empty(),
            "Expected entries for 'apple' (case-sensitive)"
        );

        // Validate position data for the first entry
        let first_entry = &valid_entries[0];
        assert_eq!(first_entry.field, SchemaId::from(1u32));
        assert_eq!(first_entry.stripe, 0);
        assert_eq!(first_entry.repr_type, PositionsReprType::Positions);
        assert!(first_entry.range.start < first_entry.range.end);

        let results = decoder
            .lookup_term("Apple")
            .expect("Failed to lookup 'Apple'");
        let entries: Vec<_> = results.collect();
        let valid_entries: Vec<_> = entries.into_iter().filter_map(|r| r.ok()).collect();
        assert!(
            !valid_entries.is_empty(),
            "Expected entries for 'Apple' (case-sensitive)"
        );

        // Test case-insensitive collation
        let collation = Box::new(UnicodeCaseInsensitiveCollation);
        let decoder = BTreeDecoder::new(shard_url.to_string(), object_store, collation)
            .expect("Failed to create BTreeDecoder");

        // With case-insensitive collation, should find same terms regardless of case
        let results = decoder
            .lookup_term("apple")
            .expect("Failed to lookup 'apple'");
        let entries: Vec<_> = results.collect();
        let valid_entries: Vec<_> = entries.into_iter().filter_map(|r| r.ok()).collect();
        assert!(
            !valid_entries.is_empty(),
            "Expected entries for 'apple' (case-insensitive)"
        );

        let results = decoder
            .lookup_term("APPLE")
            .expect("Failed to lookup 'APPLE'");
        let entries: Vec<_> = results.collect();
        let valid_entries: Vec<_> = entries.into_iter().filter_map(|r| r.ok()).collect();
        assert!(
            !valid_entries.is_empty(),
            "Expected entries for 'APPLE' (case-insensitive)"
        );
    }

    #[test]
    fn test_btree_decoder_big_multilevel_btree() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let object_store = Arc::new(
            LocalFsObjectStore::new(temp_dir.path(), LocalFsMode::VirtualRoot)
                .expect("Failed to create LocalFsObjectStore"),
        );

        // Create a large B-Tree with thousands of terms to force multiple levels
        let temp_store = temp_file_store::create_in_memory(100 * 1024 * 1024)
            .expect("Failed to create temporary file store");
        let mut encoder = BTreeEncoder::new(object_store.clone(), temp_store.clone())
            .expect("Failed to create BTreeEncoder");

        let field1 = SchemaId::from(1u32);
        let field2 = SchemaId::from(2u32);
        let field3 = SchemaId::from(3u32);
        let field4 = SchemaId::from(4u32);
        let mut next_off: u64 = 1000;

        // Generate 25,000 unique terms to force multiple B-Tree levels
        // With 10 stripes and 4 fields, we'll get 40 entries per term = 1M total entries

        // Store expected ranges for validation
        #[allow(clippy::type_complexity)]
        let mut expected_ranges: std::collections::HashMap<
            String,
            Vec<(u16, SchemaId, Range<u64>, PositionsReprType)>,
        > = std::collections::HashMap::new();

        let mut total_entries = 0;

        for i in 0..25000 {
            let term = format!("term_{i:06}"); // term_000000, term_000001, etc.
            let mut term_ranges = Vec::new();

            // Add 10 stripes for each term to get more entries
            for stripe in 0..10u16 {
                // Always add all 4 fields for each stripe
                for field in [field1, field2, field3, field4] {
                    let range_start = next_off;
                    next_off += 20; // Larger ranges to be distinctive
                    let range = range_start..next_off;
                    let repr_type = match (stripe as u32 + field.as_u32()) % 3 {
                        0 => PositionsReprType::Positions,
                        1 => PositionsReprType::ExactRanges,
                        2 => PositionsReprType::ApproximateRanges,
                        _ => unreachable!(),
                    };

                    encoder
                        .push(term.as_bytes(), stripe, field, range.clone(), repr_type)
                        .unwrap_or_else(|_| panic!("Failed to push term: {term}"));

                    term_ranges.push((stripe, field, range, repr_type));
                    total_entries += 1;
                }
            }

            expected_ranges.insert(term.clone(), term_ranges);
        }

        // Verify we have approximately 1M entries
        assert!(
            total_entries >= 1_000_000,
            "Expected at least 1M entries, got {total_entries}"
        );

        let prepared = encoder.finish().expect("Failed to finish BTreeEncoder");
        let shard_url = object_store
            .container_url()
            .resolve_relative(RelativePath::new("big_multilevel_btree.amudai.shard").unwrap())
            .unwrap();
        let _sealed_terms = prepared
            .seal(shard_url.as_str())
            .expect("Failed to seal terms shard");

        // Create decoder and verify we have multiple levels
        let collation = Box::new(UnicodeCasePreservingCollation);
        let decoder = BTreeDecoder::new(shard_url.to_string(), object_store, collation)
            .expect("Failed to create BTreeDecoder");

        let total_pages = decoder.pages_count().expect("Failed to get pages count");

        // With 5000 terms, we should definitely have multiple B-Tree levels
        assert!(
            total_pages > 10,
            "Expected many pages for large B-Tree, got {total_pages}"
        );

        // Test exact lookups for selected terms and validate all position data
        let test_terms = [
            "term_000000",
            "term_000123",
            "term_001500",
            "term_012999",
            "term_024999",
        ];

        for term in test_terms {
            let results = decoder
                .lookup_term(term)
                .unwrap_or_else(|_| panic!("Failed to lookup '{term}'"));
            let entries: Vec<_> = results.collect();
            let valid_entries: Vec<_> = entries.into_iter().filter_map(|r| r.ok()).collect();

            let expected = expected_ranges
                .get(term)
                .unwrap_or_else(|| panic!("Missing expected ranges for term '{term}'"));

            // Should have exactly the expected number of entries
            assert_eq!(
                valid_entries.len(),
                expected.len(),
                "Term '{}' should have {} entries, got {}",
                term,
                expected.len(),
                valid_entries.len()
            );

            // Validate each entry matches the expected data
            for (stripe, field, expected_range, expected_repr_type) in expected {
                let matching_entry = valid_entries
                    .iter()
                    .find(|e| e.stripe == *stripe && e.field == *field)
                    .unwrap_or_else(|| {
                        panic!("Missing entry for term '{term}', stripe {stripe}, field {field:?}")
                    });

                // Validate range matches exactly
                assert_eq!(
                    matching_entry.range, *expected_range,
                    "Range mismatch for term '{}', stripe {}, field {:?}. Expected {:?}, got {:?}",
                    term, stripe, field, expected_range, matching_entry.range
                );

                // Validate representation type matches
                assert_eq!(
                    matching_entry.repr_type, *expected_repr_type,
                    "Repr type mismatch for term '{}', stripe {}, field {:?}. Expected {:?}, got {:?}",
                    term, stripe, field, expected_repr_type, matching_entry.repr_type
                );

                // Validate stripe is in expected range
                assert!(
                    (matching_entry.stripe as u32) < 10,
                    "Stripe should be 0-9 for term '{}', got {}",
                    term,
                    matching_entry.stripe
                );
            }
        }

        // Test prefix lookup on large dataset
        let results = decoder
            .lookup_term_prefix("term_0001")
            .expect("Failed to lookup prefix 'term_0001'");
        let entries: Vec<_> = results.collect();
        let valid_entries: Vec<_> = entries.into_iter().filter_map(|r| r.ok()).collect();

        // Should match term_000100 through term_000199 (100 terms)
        // Each term has 10 stripes × 4 fields = 40 entries per term
        // So 100 terms × 40 entries = 4000 entries
        assert!(
            valid_entries.len() >= 3900,
            "Expected at least 3900 entries for prefix 'term_0001', got {}",
            valid_entries.len()
        );
        assert!(
            valid_entries.len() <= 4100,
            "Expected at most 4100 entries for prefix 'term_0001', got {}",
            valid_entries.len()
        );

        // Validate all entries have proper ranges (start < end)
        for entry in &valid_entries {
            assert!(
                entry.range.start < entry.range.end,
                "Invalid range {:?} for entry",
                entry.range
            );
            assert!(
                entry.range.start >= 1000,
                "Range start {} should be >= 1000",
                entry.range.start
            );
        }
    }
}
