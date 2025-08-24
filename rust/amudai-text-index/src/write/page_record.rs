use arrow_array::{
    RecordBatch,
    builder::{
        BinaryBuilder, ListBuilder, StructBuilder as ArrowStructBuilder, UInt8Builder,
        UInt16Builder, UInt32Builder, UInt64Builder,
    },
};
use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema, SchemaRef};
use std::sync::Arc;

/// Internal structure for constructing B-Tree pages represented in Arrow format.
///
/// This struct manages the Arrow builders needed to construct the complex nested
/// structure of B-Tree pages, including entries with terms, child positions, and
/// term position lists organized by stripe and field.
///
/// The builder creates pages with a standardized Arrow schema that supports both
/// interior and leaf pages of the B-Tree. The schema uses nullable fields to
/// differentiate between page types - interior pages have null `term_positions`
/// while leaf pages have null `child_position` fields.
///
/// A page-level `pos_list_start_offset` (UInt64, nullable) is also present.
/// For leaf pages it must be set to the starting offset in the positions shard
/// where this page’s first entry begins; for interior pages it must be null.
struct BTreePageRecordBuilder {
    /// Arrow schema definition for the page structure.
    schema: SchemaRef,
    /// Main struct builder for constructing page records.
    builder: ArrowStructBuilder,
}

impl BTreePageRecordBuilder {
    /// Creates a new ArrowPageBuilder instance with the appropriate Arrow schema and builders.
    ///
    /// Initializes the complex nested schema structure for B-Tree pages including:
    /// - Page level information (UInt8)
    /// - Entry lists containing terms and child positions
    /// - Term position lists organized by stripe and field hierarchies
    /// - Page-level `pos_list_start_offset` for leaf pages (nullable for interior)
    /// - Field position metadata with schema IDs, representation types, and offset information
    ///
    /// The schema is designed to be flexible enough to represent both interior and leaf
    /// pages through the use of nullable fields.
    fn new() -> Self {
        let field_positions_fields = Fields::from(vec![
            Field::new("field_schema_id", DataType::UInt32, false),
            Field::new("repr_type", DataType::UInt8, false),
            Field::new("pos_list_end_offset", DataType::UInt64, false),
        ]);

        let stripe_field_positions_fields = Fields::from(vec![
            Field::new("stripe_id", DataType::UInt16, false),
            Field::new(
                "fields",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(field_positions_fields.clone()),
                    true,
                ))),
                true,
            ),
        ]);

        let entries_fields = Fields::from(vec![
            Field::new("term", DataType::Binary, false),
            Field::new("child_position", DataType::UInt64, true),
            Field::new(
                "term_positions",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(stripe_field_positions_fields.clone()),
                    true,
                ))),
                true,
            ),
        ]);

        let page_fields = Fields::from(vec![
            Field::new("level", DataType::UInt8, false),
            Field::new(
                "entries",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(entries_fields.clone()),
                    true,
                ))),
                true,
            ),
            Field::new("pos_list_start_offset", DataType::UInt64, true),
        ]);
        let page_schema = Arc::new(ArrowSchema::new(page_fields.clone()));

        let fields_struct_builder = ArrowStructBuilder::new(
            field_positions_fields,
            vec![
                Box::new(UInt32Builder::new()),
                Box::new(UInt8Builder::new()),
                Box::new(UInt64Builder::new()),
            ],
        );

        let term_positions_struct_builder = ArrowStructBuilder::new(
            stripe_field_positions_fields,
            vec![
                Box::new(UInt16Builder::new()),
                Box::new(ListBuilder::new(fields_struct_builder)),
            ],
        );

        let entries_struct_builder = ArrowStructBuilder::new(
            entries_fields,
            vec![
                Box::new(BinaryBuilder::new()),
                Box::new(UInt64Builder::new()),
                Box::new(ListBuilder::new(term_positions_struct_builder)),
            ],
        );

        let builder = ArrowStructBuilder::new(
            page_fields,
            vec![
                Box::new(UInt8Builder::new()),
                Box::new(ListBuilder::new(entries_struct_builder)),
                Box::new(UInt64Builder::new()),
            ],
        );

        Self {
            schema: page_schema,
            builder,
        }
    }

    /// Starts building a new page record by appending to the main page builder.
    ///
    /// This method initializes a new page entry in the builder and must be called
    /// before adding any page-level data such as level or entries.
    fn start_page(&mut self) {
        self.builder.append(true);
    }

    /// Returns a mutable reference to the level field builder for setting page levels.
    ///
    /// The level indicates the depth of the page in the B-Tree hierarchy, where
    /// leaf pages have level 0 and interior pages have level > 0.
    fn level_builder(&mut self) -> &mut UInt8Builder {
        self.builder.field_builder(0).unwrap()
    }

    /// Returns a mutable reference to the entries list builder for adding entry lists to pages.
    ///
    /// Each page contains a list of entries, where each entry represents either a
    /// term (for leaf pages) or a separator key (for interior pages) along with
    /// associated metadata.
    fn entries_list_builder(&mut self) -> &mut ListBuilder<ArrowStructBuilder> {
        self.builder.field_builder(1).unwrap()
    }

    /// Returns a mutable reference to the entries struct builder for building individual entries.
    ///
    /// This builder constructs the struct that contains the term, child position,
    /// and term positions for each entry in the page.
    fn entries_struct_builder(&mut self) -> &mut ArrowStructBuilder {
        self.entries_list_builder().values()
    }

    /// Returns a mutable reference to the term builder for setting term byte values.
    ///
    /// Terms are stored as binary data and represent either the actual indexed terms
    /// (for leaf pages) or separator keys (for interior pages) used for navigation.
    fn term_builder(&mut self) -> &mut BinaryBuilder {
        self.entries_struct_builder().field_builder(0).unwrap()
    }

    /// Returns the last term added to the page, if any.
    fn last_term(&mut self) -> Option<&[u8]> {
        // Access the term builder through the existing helper methods
        let term_builder = self.term_builder();
        let offsets = term_builder.offsets_slice();
        let values = term_builder.values_slice();

        // Need at least 2 offsets to have 1 term (start and end positions)
        if offsets.len() < 2 {
            None
        } else {
            let last_start = offsets[offsets.len() - 2] as usize;
            let last_end = offsets[offsets.len() - 1] as usize;
            Some(&values[last_start..last_end])
        }
    }

    /// Returns a mutable reference to the child position builder for setting child page positions.
    ///
    /// Child positions point to lower-level pages in the B-Tree hierarchy. This field
    /// is only used by interior pages and should be null for leaf pages.
    fn child_pos_builder(&mut self) -> &mut UInt64Builder {
        self.entries_struct_builder().field_builder(1).unwrap()
    }

    /// Returns a mutable reference to the term positions list builder for adding position lists.
    ///
    /// Term positions contain the hierarchical structure of stripe and field information
    /// that describes where terms appear in the indexed data. This field is only used
    /// by leaf pages and should be null for interior pages.
    fn term_positions_list_builder(&mut self) -> &mut ListBuilder<ArrowStructBuilder> {
        self.entries_struct_builder().field_builder(2).unwrap()
    }

    /// Returns a mutable reference to the term positions struct builder for building position entries.
    ///
    /// This builder constructs individual stripe entries within the term positions list,
    /// containing stripe IDs and their associated field lists.
    fn term_positions_struct_builder(&mut self) -> &mut ArrowStructBuilder {
        self.term_positions_list_builder().values()
    }

    /// Returns a mutable reference to the stripe ID builder for setting stripe identifiers.
    ///
    /// Stripe IDs identify logical partitions of the indexed data and are used to
    /// organize term positions within the hierarchical structure.
    fn stripe_id_builder(&mut self) -> &mut UInt16Builder {
        self.term_positions_struct_builder()
            .field_builder(0)
            .unwrap()
    }

    /// Returns a mutable reference to the fields list builder for adding field lists.
    ///
    /// Each stripe contains a list of fields that describe the specific data fields
    /// where the term appears, along with their metadata.
    fn fields_list_builder(&mut self) -> &mut ListBuilder<ArrowStructBuilder> {
        self.term_positions_struct_builder()
            .field_builder(1)
            .unwrap()
    }

    /// Returns a mutable reference to the fields struct builder for building field entries.
    ///
    /// This builder constructs individual field entries containing the field schema ID,
    /// representation type, and position list end offset.
    fn fields_struct_builder(&mut self) -> &mut ArrowStructBuilder {
        self.fields_list_builder().values()
    }

    /// Returns a mutable reference to the field schema ID builder for setting field identifiers.
    ///
    /// Field schema IDs uniquely identify the data fields within the schema where
    /// the indexed term appears.
    fn field_schema_id_builder(&mut self) -> &mut UInt32Builder {
        self.fields_struct_builder().field_builder(0).unwrap()
    }

    /// Returns a mutable reference to the representation type builder for setting field data types.
    ///
    /// The representation type indicates how the data is encoded or stored for the
    /// specific field, affecting how position information should be interpreted.
    fn repr_type_builder(&mut self) -> &mut UInt8Builder {
        self.fields_struct_builder().field_builder(1).unwrap()
    }

    /// Returns a mutable reference to the position list end offset builder.
    ///
    /// The position list end offset indicates the ending position in the serialized
    /// position data for this field, enabling efficient access to term position information.
    fn pos_list_end_offset_builder(&mut self) -> &mut UInt64Builder {
        self.fields_struct_builder().field_builder(2).unwrap()
    }

    /// Returns a mutable reference to the page-level positions start offset builder.
    ///
    /// This column must contain a value for leaf pages (the page’s starting offset
    /// in the positions shard) and a null for interior pages.
    fn page_pos_list_start_offset_builder(&mut self) -> &mut UInt64Builder {
        self.builder.field_builder(2).unwrap()
    }

    /// Finishes building the current page and returns a RecordBatch.
    ///
    /// This method completes the construction of the current page by finishing
    /// all the Arrow builders and creating a RecordBatch with the defined schema.
    /// The resulting RecordBatch contains a single row representing the complete
    /// page structure.
    ///
    /// Note: This method consumes the builder, so it cannot be reused after calling finish().
    pub fn finish(mut self) -> RecordBatch {
        let struct_array = self.builder.finish();
        let (_, columns, _) = struct_array.into_parts();
        RecordBatch::try_new(self.schema.clone(), columns).unwrap()
    }
}

/// Builder for constructing interior pages of the B-Tree in Arrow format.
///
/// Interior pages contain separator keys and child page references used for navigation
/// through the B-Tree hierarchy. They do not contain actual term position data.
///
/// ## Arrow Schema Structure
///
/// ```text
/// Interior Page Schema:
/// ┌─────────────────────────────────────────────────────────┐
/// │ Root Struct                                             │
/// ├─────────────────────────────────────────────────────────┤
/// │ ├─ level: UInt8 (required)                              │
/// │ ├─ pos_list_start_offset: UInt64 (NULL)                 │
/// │ └─ entries: List<Struct> (nullable)                     │
/// │    └─ Entry Struct:                                     │
/// │       ├─ term: Binary (required)                        │
/// │       ├─ child_position: UInt64 (required for interior) │
/// │       └─ term_positions: List<Struct> (NULL)            │
/// └─────────────────────────────────────────────────────────┘
/// ```
///
/// ## Field Usage for Interior Pages
/// - `level`: Page level in B-Tree (> 0 for interior pages)
/// - `pos_list_start_offset`: Must be appended as null on interior pages
/// - `entries.term`: Separator keys for navigation
/// - `entries.child_position`: References to child pages
/// - `entries.term_positions`: Always NULL for interior pages
pub struct InteriorPageRecordBuilder {
    inner: BTreePageRecordBuilder,
    estimated_size: usize,
    count: usize,
}

impl InteriorPageRecordBuilder {
    /// Constant representing the overhead size of each page entry in bytes.
    const PAGE_ENTRY_OVERHEAD: usize = 24;

    /// Creates a new ArrowInteriorPageBuilder instance for the specified B-Tree level.
    ///
    /// Initializes the builder with the given level and prepares it for adding
    /// separator key entries. The level must be greater than 0 for interior pages.
    ///
    /// # Parameters
    /// - `level`: The level of this page in the B-Tree hierarchy (must be > 0)
    pub fn new(level: u8) -> Self {
        assert!(level > 0, "Interior page level must be greater than 0");
        let mut inner = BTreePageRecordBuilder::new();
        inner.start_page();
        inner.level_builder().append_value(level);
        // Interior pages have no positions start offset
        inner.page_pos_list_start_offset_builder().append_null();
        Self {
            inner,
            estimated_size: 0,
            count: 0,
        }
    }

    /// Adds a separator key entry to the interior page.
    ///
    /// Each entry consists of a term (separator key) and a child position pointing
    /// to a lower-level page in the B-Tree. The term_positions field is automatically
    /// set to NULL as it's not used in interior pages.
    ///
    /// # Parameters
    /// - `term`: The separator key as binary data
    /// - `child_position`: Position/offset of the child page this entry points to
    pub fn push(&mut self, term: &[u8], child_position: u64) {
        self.inner.term_builder().append_value(term);
        self.inner.child_pos_builder().append_value(child_position);
        self.inner.term_positions_list_builder().append_null(); // No term positions in interior pages
        self.inner.entries_struct_builder().append(true);
        self.estimated_size += term.len() + Self::PAGE_ENTRY_OVERHEAD;
        self.count += 1;
    }

    /// Returns the last term added to the interior page, if any.
    ///
    /// # Returns
    ///
    /// This method retrieves the last term that was added to the interior page.
    pub fn last_term(&mut self) -> Option<&[u8]> {
        self.inner.last_term()
    }

    /// Returns the estimated size of the interior page in bytes.
    pub fn estimated_size(&self) -> usize {
        self.estimated_size
    }

    /// Returns whether the interior page has no entries.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Finalizes the interior page construction and returns the completed RecordBatch.
    ///
    /// This method properly closes all builder structures and creates a RecordBatch
    /// representing the complete interior page. For empty pages, an empty entries
    /// list is created to maintain schema consistency.
    ///
    /// The builder is consumed by this operation and cannot be reused.
    ///
    /// # Returns
    /// A RecordBatch containing the interior page data in the standard Arrow format
    pub fn finish(mut self) -> RecordBatch {
        assert!(
            self.last_term().is_some(),
            "Interior page must have at least one entry",
        );
        self.inner.entries_list_builder().append(true);
        self.inner.finish()
    }
}

/// Builder for constructing leaf pages of the B-Tree in Arrow format.
///
/// Leaf pages contain the actual indexed terms along with their position information
/// organized by stripe and field hierarchies. They do not contain child page references.
///
/// ## Arrow Schema Structure
///
/// ```text
/// Leaf Page Schema:
/// ┌─────────────────────────────────────────────────────────┐
/// │ Root Struct                                             │
/// ├─────────────────────────────────────────────────────────┤
/// │ ├─ level: UInt8 (required, always 0)                    │
/// │ ├─ entries: List<Struct> (nullable)                     │
/// │ └─ pos_list_start_offset: UInt64 (required)             │
/// │    └─ Entry Struct:                                     │
/// │       ├─ term: Binary (required)                        │
/// │       ├─ child_position: UInt64 (NULL)                  │
/// │       └─ term_positions: List<Struct> (nullable)        │
/// │          └─ Stripe Struct:                              │
/// │             ├─ stripe_id: UInt16 (required)             │
/// │             └─ fields: List<Struct> (nullable)          │
/// │                └─ Field Struct:                         │
/// │                   ├─ field_schema_id: UInt32 (required) │
/// │                   ├─ repr_type: UInt8 (required)        │
/// │                   └─ pos_list_end_offset: UInt64 (req)  │
/// └─────────────────────────────────────────────────────────┘
/// ```
///
/// ## Field Usage for Leaf Pages
/// - `level`: Always 0 for leaf pages
/// - `entries.term`: Actual indexed terms
/// - `entries.child_position`: Always NULL for leaf pages
/// - `entries.term_positions`: Hierarchical position data by stripe and field
/// - `term_positions.stripe_id`: Logical data partition identifier
/// - `term_positions.fields`: List of fields where term appears in this stripe
/// - `fields.field_schema_id`: Unique field identifier in the schema
/// - `fields.repr_type`: Data representation/encoding type for the field
/// - `fields.pos_list_end_offset`: End position in serialized position data
///
/// ## Builder Behavior
/// The builder automatically handles deduplication and grouping:
/// - Multiple pushes with the same term are grouped into a single term entry
/// - Multiple pushes with the same term and stripe are grouped into a single stripe
/// - Each push represents a unique (term, stripe_id, field_schema_id) combination
pub struct LeafPageRecordBuilder {
    inner: BTreePageRecordBuilder,
    current_stripe_id: Option<u16>,
    estimated_size: usize,
    count: usize,
    page_start_set: bool,
}

impl LeafPageRecordBuilder {
    /// Constant representing the overhead size of each field entry in bytes.
    const PAGE_ENTRY_OVERHEAD: usize = 16;

    /// Creates a new ArrowLeafPageBuilder instance for constructing leaf pages.
    ///
    /// Initializes the builder with level 0 (indicating a leaf page) and prepares
    /// it for adding term entries with their position information.
    ///
    /// # Returns
    /// A new builder ready to accept term entries via the `push` method
    pub fn new() -> Self {
        let mut inner = BTreePageRecordBuilder::new();
        inner.start_page();
        inner.level_builder().append_value(0);
        Self {
            inner,
            current_stripe_id: None,
            estimated_size: 0,
            count: 0,
            page_start_set: false,
        }
    }

    /// Sets the page-level positions start offset for this leaf page (only once).
    ///
    /// This value seeds the starting offset for the first entry on the page and
    /// enables readers to avoid cross-page lookups when reconstructing postings.
    pub fn set_page_positions_start_offset(&mut self, offset: u64) {
        if !self.page_start_set {
            self.inner
                .page_pos_list_start_offset_builder()
                .append_value(offset);
            self.page_start_set = true;
        }
    }

    /// Adds a term entry with position information to the leaf page.
    ///
    /// This method handles the hierarchical structure of term positions,
    /// automatically grouping entries by term and stripe. Each call to push
    /// creates a new field entry within the current stripe.
    ///
    /// # Parameters
    /// - `term`: The indexed term as binary data
    /// - `stripe_id`: Logical partition identifier for the data
    /// - `field_schema_id`: Unique identifier for the field in the schema
    /// - `repr_type`: Data representation/encoding type for this field
    /// - `pos_list_end_offset`: End position in the serialized position data
    ///
    /// # Behavior
    /// - Terms are grouped together across multiple calls
    /// - Within each term, stripes are grouped together
    /// - Each call represents a unique (term, stripe_id, field_schema_id) combination
    pub fn push(
        &mut self,
        term: &[u8],
        stripe_id: u16,
        field_schema_id: u32,
        repr_type: u8,
        pos_list_end_offset: u64,
    ) {
        // Check if this is a new term
        let last_term = self.inner.last_term();
        if last_term != Some(term) {
            // If we had a previous term, finalize it
            if last_term.is_some() {
                // Close previous stripe entry
                self.inner.fields_list_builder().append(true);
                self.inner.term_positions_struct_builder().append(true);
                // Close previous term entry
                self.inner.term_positions_list_builder().append(true);
                self.inner.entries_struct_builder().append(true);
            }

            // Start new term
            self.inner.term_builder().append_value(term);
            self.inner.child_pos_builder().append_null(); // Leaf pages don't have child positions
            self.current_stripe_id = None;
            self.estimated_size += term.len();
        }

        // Check if this is a new stripe (but only if we haven't already handled it in term transition)
        if self.current_stripe_id != Some(stripe_id) {
            // If we had a previous stripe, finalize it
            if self.current_stripe_id.is_some() {
                // Close previous stripe entry
                self.inner.fields_list_builder().append(true);
                self.inner.term_positions_struct_builder().append(true);
            }

            // Start new stripe
            self.inner.stripe_id_builder().append_value(stripe_id);
            self.current_stripe_id = Some(stripe_id);
        }

        // Always add a new field entry (no deduplication at field level)
        self.inner
            .field_schema_id_builder()
            .append_value(field_schema_id);
        self.inner.repr_type_builder().append_value(repr_type);
        self.inner
            .pos_list_end_offset_builder()
            .append_value(pos_list_end_offset);
        self.inner.fields_struct_builder().append(true);

        // Add overhead for field entries
        self.estimated_size += Self::PAGE_ENTRY_OVERHEAD;
        self.count += 1;
    }

    /// Returns the last term added to the leaf page, if any.
    ///
    /// # Returns
    ///
    /// This method retrieves the last term that was added to the leaf page.
    pub fn last_term(&mut self) -> Option<&[u8]> {
        self.inner.last_term()
    }

    /// Returns the estimated size of the leaf page in bytes.
    pub fn estimated_size(&self) -> usize {
        self.estimated_size
    }

    /// Returns whether the leaf page has no entries.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Finalizes the leaf page construction and returns the completed RecordBatch.
    ///
    /// This method properly closes all hierarchical builder structures (fields,
    /// stripes, terms, and entries) and creates a RecordBatch representing the
    /// complete leaf page. The method handles the complex nested structure by
    /// closing builders in the correct order.
    ///
    /// For empty pages, an empty entries list is created to maintain schema consistency.
    /// The builder is consumed by this operation and cannot be reused.
    ///
    /// # Returns
    /// A RecordBatch containing the leaf page data in the standard Arrow format
    pub fn finish(mut self) -> RecordBatch {
        assert!(
            self.current_stripe_id.is_some(),
            "Leaf page must have at least one entry"
        );
        assert!(
            self.page_start_set,
            "Leaf page must have pos_list_start_offset set"
        );

        // Close the last stripe entry
        self.inner.fields_list_builder().append(true);
        self.inner.term_positions_struct_builder().append(true);

        // Close the last term entry
        self.inner.term_positions_list_builder().append(true);
        self.inner.entries_struct_builder().append(true);

        // Close the entries list
        self.inner.entries_list_builder().append(true);

        self.inner.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;

    #[test]
    fn test_btree_page_record_builder_schema_consistency() {
        // Test that both builders produce the same schema
        let mut interior_builder = InteriorPageRecordBuilder::new(1);
        interior_builder.push(b"test", 123);
        let interior_batch = interior_builder.finish();

        let mut leaf_builder = LeafPageRecordBuilder::new();
        leaf_builder.set_page_positions_start_offset(0);
        leaf_builder.push(b"test", 1, 10, 1, 100);
        let leaf_batch = leaf_builder.finish();

        assert_eq!(interior_batch.schema(), leaf_batch.schema());

        // Verify that both have correct structure
        assert_eq!(interior_batch.num_columns(), 3);
        assert_eq!(leaf_batch.num_columns(), 3);
        assert_eq!(interior_batch.num_rows(), 1);
        assert_eq!(leaf_batch.num_rows(), 1);

        // Verify schema fields
        let schema = interior_batch.schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "level");
        assert_eq!(schema.field(1).name(), "entries");
        assert_eq!(schema.field(2).name(), "pos_list_start_offset");
    }

    #[test]
    fn test_interior_page_record_builder() {
        let mut builder = InteriorPageRecordBuilder::new(1);

        // Add some entries
        builder.push(b"apple", 100);
        assert_eq!(builder.last_term(), Some(b"apple" as &[u8]));
        builder.push(b"banana", 200);
        assert_eq!(builder.last_term(), Some(b"banana" as &[u8]));
        builder.push(b"cherry", 300);
        assert_eq!(builder.last_term(), Some(b"cherry" as &[u8]));

        let batch = builder.finish();

        // Verify schema structure
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.num_rows(), 1);

        // Verify level
        let level_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::UInt8Array>()
            .unwrap();
        assert_eq!(level_array.value(0), 1);

        // Verify entries structure
        let entries_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        assert_eq!(entries_array.len(), 1);
        assert!(!entries_array.is_null(0));

        // pos_list_start_offset must be null for interior page
        let start_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        assert!(start_array.is_null(0));

        // Get the entries struct array
        let entries_value = entries_array.value(0);
        let entries_struct = entries_value
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .unwrap();
        assert_eq!(entries_struct.len(), 3); // Should have 3 entries

        // Verify terms
        let terms_array = entries_struct
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();
        assert_eq!(terms_array.value(0), b"apple");
        assert_eq!(terms_array.value(1), b"banana");
        assert_eq!(terms_array.value(2), b"cherry");

        // Verify child positions
        let positions_array = entries_struct
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        assert_eq!(positions_array.value(0), 100);
        assert_eq!(positions_array.value(1), 200);
        assert_eq!(positions_array.value(2), 300);

        // Verify term positions are null for interior pages
        let term_positions_array = entries_struct
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        assert!(term_positions_array.is_null(0));
        assert!(term_positions_array.is_null(1));
        assert!(term_positions_array.is_null(2));
    }

    #[test]
    fn test_leaf_page_record_builder() {
        let mut builder = LeafPageRecordBuilder::new();
        builder.set_page_positions_start_offset(0);

        for term_idx in 0..100 {
            let term = format!("term{term_idx:03}").into_bytes(); // term000, term000, ..., term009, term099
            for stripe_idx in 0..5 {
                for field_idx in 0..7 {
                    builder.push(
                        &term,
                        stripe_idx,
                        field_idx,
                        fastrand::u8(0..3),
                        fastrand::u64(0..1000),
                    );
                }
            }
        }

        let batch = builder.finish();
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.num_rows(), 1);

        // Verify level
        let level_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::UInt8Array>()
            .unwrap();
        assert_eq!(level_array.value(0), 0);

        // Verify entries structure
        let entries_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        assert!(!entries_array.is_null(0));

        // pos_list_start_offset must be set for leaf page
        let start_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        assert!(!start_array.is_null(0));

        let entries_value = entries_array.value(0);
        let entries_struct = entries_value
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .unwrap();

        // With sorted terms, we should have 100 unique terms (term000..term099)
        assert_eq!(entries_struct.len(), 100);

        // Verify some sample terms
        let terms_array = entries_struct
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();
        assert_eq!(terms_array.len(), 100);
        assert_eq!(terms_array.value(0), b"term000");
        assert_eq!(terms_array.value(1), b"term001");
        assert_eq!(terms_array.value(9), b"term009");
        assert_eq!(terms_array.value(99), b"term099");

        // Verify all child positions are null for leaf pages
        let positions_array = entries_struct
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        assert_eq!(positions_array.len(), 100);
        for i in 0..100 {
            assert!(positions_array.is_null(i));
        }

        // Verify all terms has the expected stripe structure
        for i in 0..100 {
            let term_positions_array = entries_struct
                .column(2)
                .as_any()
                .downcast_ref::<arrow_array::ListArray>()
                .unwrap();
            let term_positions_value = term_positions_array.value(i);
            let term_positions_struct = term_positions_value
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .unwrap();

            // There should be 5 unique stripes (0, 1, 2, 3, 4) due to proper deduplication
            assert_eq!(term_positions_struct.len(), 5);

            // Verify stripe IDs
            let stripe_ids_array = term_positions_struct
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::UInt16Array>()
                .unwrap();
            let fields_list_array = term_positions_struct
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::ListArray>()
                .unwrap();
            assert_eq!(stripe_ids_array.len(), 5);
            for stripe_idx in 0..5 {
                assert_eq!(stripe_ids_array.value(stripe_idx), stripe_idx as u16);

                // Verify term positions (expected to have 7 unique fields per stripe)
                let stripe_fields_value = fields_list_array.value(stripe_idx);
                let stripe_fields_struct = stripe_fields_value
                    .as_any()
                    .downcast_ref::<arrow_array::StructArray>()
                    .unwrap();
                assert_eq!(stripe_fields_struct.len(), 7);
                // Verify field schema IDs
                let field_schema_ids_array = stripe_fields_struct
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::UInt32Array>()
                    .unwrap();
                assert_eq!(field_schema_ids_array.len(), 7);
                for field_idx in 0..7 {
                    assert_eq!(field_schema_ids_array.value(field_idx), field_idx as u32);
                }
            }
        }
    }
}
