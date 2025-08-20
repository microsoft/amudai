//! B-Tree encoder for the text index term postings.

use std::{ops::Range, sync::Arc};

use amudai_common::Result;
use amudai_encodings::block_encoder::BlockEncodingProfile;
use amudai_format::{
    schema::{BasicType as AmudaiBasicType, SchemaId},
    schema_builder::{DataTypeBuilder, SchemaBuilder},
};
use amudai_io::TemporaryFileStore;
use amudai_objectstore::ObjectStore;
use amudai_shard::write::{
    shard_builder::{PreparedShard, ShardBuilder, ShardBuilderParams, ShardFileOrganization},
    stripe_builder::StripeBuilder,
};

use crate::{
    pos_list::PositionsReprType,
    write::page_record::{InteriorPageRecordBuilder, LeafPageRecordBuilder},
};

/// B-Tree encoder for building text index term navigation structure.
///
/// This encoder takes term postings and organizes them into a B-Tree index structure
/// suitable for efficient term lookups. It manages the tree structure (terms and
/// navigation) and references to position data that is stored separately in a
/// positions shard by the higher-level TextIndexEncoder.
///
/// The encoder focuses solely on the terms shard containing:
/// - **B-Tree pages**: Navigation structure with terms and child page pointers
/// - **Position references**: Ranges and representation types pointing to position data
/// - **Metadata**: Page-level information for efficient query processing
///
/// # Input Requirements
///
/// The encoder assumes that term postings are provided in sorted order:
/// 1. First by term (lexicographically)
/// 2. Then by stripe ID (numerically)
/// 3. Then by field schema ID (numerically)
///
/// This ordering is critical for the B-Tree structure to function correctly during queries.
///
/// # Page Management
///
/// The encoder automatically manages page sizes and splits:
/// - Leaf pages are limited to 8KB and contain actual terms with position references
/// - Interior pages are limited to 4KB and contain separator keys with child page pointers
/// - Pages are written to storage when they exceed size limits
/// - The tree is built bottom-up with automatic parent page creation
pub struct BTreeEncoder {
    /// Builder for the shard containing B-Tree structure and terms.
    shard: ShardBuilder,
    /// Stripe builder for writing B-Tree pages.
    stripe: StripeBuilder,
    /// Current leaf page builder.
    leaf: LeafPageBuilder,
}

impl BTreeEncoder {
    /// Creates a new B-Tree encoder with the specified storage backends.
    ///
    /// Initializes the terms shard builder with its schema and creates the necessary
    /// stripe builder for writing B-Tree page data.
    ///
    /// # Arguments
    ///
    /// * `object_store` - Storage backend for persisting the terms shard
    /// * `temp_store` - Temporary storage for intermediate data during construction
    ///
    /// # Returns
    ///
    /// A new BTreeEncoder instance ready for receiving term postings
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        temp_store: Arc<dyn TemporaryFileStore>,
    ) -> Result<Self> {
        let terms_shard_params = ShardBuilderParams {
            schema: Self::create_shard_schema(),
            object_store: Arc::clone(&object_store),
            temp_store: Arc::clone(&temp_store),
            encoding_profile: BlockEncodingProfile::MinimalCompression,
            file_organization: ShardFileOrganization::SingleFile,
        };
        let terms_shard_builder = ShardBuilder::new(terms_shard_params)?;
        let terms_stripe_builder = terms_shard_builder.build_stripe()?;

        Ok(Self {
            shard: terms_shard_builder,
            stripe: terms_stripe_builder,
            leaf: LeafPageBuilder::new(),
        })
    }

    /// Creates the Amudai schema for the terms shard containing B-Tree structure.
    ///
    /// This function defines the complex nested schema used for storing B-Tree pages,
    /// including page levels, entry lists with terms and child positions, and
    /// term position data organized by stripe and field. In addition, a page-level
    /// `pos_list_start_offset` (UInt64, nullable) is included to mark the starting
    /// offset in the positions shard for each leaf page. This value is null for
    /// interior pages and set for leaf pages, allowing readers to compute starting
    /// offsets without inspecting previous pages.
    ///
    /// # Returns
    ///
    /// A SchemaMessage defining the structure of B-Tree pages
    fn create_shard_schema() -> amudai_format::schema::SchemaMessage {
        // Field position struct fields
        let field_schema_id =
            DataTypeBuilder::new("field_schema_id", AmudaiBasicType::Int32, false, 0, vec![]);
        let repr_type = DataTypeBuilder::new("repr_type", AmudaiBasicType::Int8, false, 0, vec![]);
        let pos_list_end_offset = DataTypeBuilder::new(
            "pos_list_end_offset",
            AmudaiBasicType::Int64,
            false,
            0,
            vec![],
        );

        // Field position struct
        let field_position_struct = DataTypeBuilder::new(
            "item",
            AmudaiBasicType::Struct,
            false,
            0,
            vec![field_schema_id, repr_type, pos_list_end_offset],
        );

        // Fields list (List of field position structs)
        let fields_list = DataTypeBuilder::new(
            "fields",
            AmudaiBasicType::List,
            false,
            0,
            vec![field_position_struct],
        );

        // Term position struct fields
        let stripe_id = DataTypeBuilder::new("stripe_id", AmudaiBasicType::Int16, false, 0, vec![]);

        // Term position struct
        let term_position_struct = DataTypeBuilder::new(
            "item",
            AmudaiBasicType::Struct,
            false,
            0,
            vec![stripe_id, fields_list],
        );

        // Term positions list (List of term position structs)
        let term_positions_list = DataTypeBuilder::new(
            "term_positions",
            AmudaiBasicType::List,
            false,
            0,
            vec![term_position_struct],
        );

        // Entry struct fields
        let term = DataTypeBuilder::new("term", AmudaiBasicType::Binary, false, 0, vec![]);
        let child_position =
            DataTypeBuilder::new("child_position", AmudaiBasicType::Int64, false, 0, vec![]);

        // Entry struct
        let entry_struct = DataTypeBuilder::new(
            "item",
            AmudaiBasicType::Struct,
            false,
            0,
            vec![term, child_position, term_positions_list],
        );

        // Entries list (List of entry structs)
        let entries_list = DataTypeBuilder::new(
            "entries",
            AmudaiBasicType::List,
            false,
            0,
            vec![entry_struct],
        );

        // Page level field
        let level = DataTypeBuilder::new("level", AmudaiBasicType::Int8, false, 0, vec![]);

        // Page-level positions start offset (UInt64; nullable on interior pages)
        let pos_list_start_offset = DataTypeBuilder::new(
            "pos_list_start_offset",
            AmudaiBasicType::Int64,
            false, // unsigned -> Arrow UInt64
            0,
            vec![],
        );

        // Root schema
        let schema = SchemaBuilder::new(vec![
            level.into(),
            entries_list.into(),
            pos_list_start_offset.into(),
        ]);
        schema.finish_and_seal()
    }

    /// Adds a term posting to the B-Tree index.
    ///
    /// This method processes a single term occurrence by adding the term metadata
    /// and position reference information to the current leaf page. Position data
    /// itself is managed separately by the higher-level TextIndexEncoder.
    /// If the leaf page becomes too large, it will be automatically flushed to storage
    /// and a new page will be started.
    ///
    /// # Arguments
    ///
    /// * `term` - The term as binary data (typically UTF-8 encoded text)
    /// * `stripe_id` - The logical partition identifier where this term appears
    /// * `field_schema_id` - The schema identifier for the field containing this term
    /// * `positions_range` - Range reference to position data in the positions shard
    /// * `repr_type` - How the position data is represented (positions, exact ranges, etc.)
    ///
    /// # Input Ordering Requirements
    ///
    /// Terms must be provided in sorted order: first by term (lexicographically),
    /// then by stripe ID, then by field schema ID. This ordering is critical for
    /// the B-Tree structure to function correctly.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the operation fails.
    pub fn push(
        &mut self,
        term: &[u8],
        stripe_id: u16,
        field_schema_id: SchemaId,
        positions_range: Range<u64>,
        repr_type: PositionsReprType,
    ) -> Result<()> {
        self.leaf.push(
            term,
            stripe_id,
            field_schema_id,
            positions_range,
            repr_type,
            &mut self.stripe,
        )
    }

    /// Completes the B-Tree construction and returns the prepared terms shard.
    ///
    /// This method finalizes the B-Tree by:
    /// 1. Finishing the current leaf page, which automatically flushes any remaining content
    ///    and recursively completes all parent interior pages up to the root
    /// 2. Finalizing the terms stripe and adding it to the terms shard
    /// 3. Completing the terms shard builder to create the prepared terms shard
    ///
    /// The leaf page completion process automatically handles the construction of the
    /// complete B-Tree hierarchy, ensuring all interior pages are properly written
    /// and a valid tree structure is created.
    ///
    /// # Returns
    ///
    /// A `PreparedShard` containing the completed terms shard with B-Tree structure.
    pub fn finish(mut self) -> Result<PreparedShard> {
        self.leaf.finish(&mut self.stripe)?;

        let stripe = self.stripe.finish()?;
        self.shard.add_stripe(stripe)?;
        self.shard.finish()
    }
}

/// Builder for constructing interior (non-leaf) pages of the B-Tree.
///
/// Interior pages contain separator keys and pointers to child pages, enabling
/// efficient navigation through the B-Tree structure. Each interior page has a level
/// greater than 0, with higher levels being closer to the root.
///
/// When an interior page becomes full (exceeds 4KB), it automatically splits by
/// flushing its content to storage and creating a parent page to hold the reference.
/// This process continues recursively until a single root page is created.
///
/// # Page Structure
///
/// Interior pages contain entries with:
/// - Separator keys (terms) used for navigation during lookups
/// - Child page positions pointing to lower-level pages
/// - No position data (term_positions field is always null)
struct InteriorPageBuilder {
    inner: InteriorPageRecordBuilder,
    parent: Option<Box<InteriorPageBuilder>>,
    level: u8,
}

impl InteriorPageBuilder {
    /// Maximum size limit for interior pages in bytes (4KB).
    ///
    /// When an interior page exceeds this size, it will be flushed to storage
    /// and a new page will be started.
    const MAX_INTERIOR_PAGE_SIZE: usize = 4 * 1024;

    /// Creates a new interior page builder for the specified B-Tree level.
    ///
    /// # Arguments
    ///
    /// * `level` - The level of this interior page in the B-Tree hierarchy.
    ///   Must be greater than 0, with higher values being closer to the root.
    ///
    /// # Returns
    ///
    /// A new `InteriorPageBuilder` ready to accept separator key entries.
    pub fn new(level: u8) -> Self {
        let inner = InteriorPageRecordBuilder::new(level);
        Self {
            inner,
            parent: None,
            level,
        }
    }

    /// Adds a separator key entry to this interior page.
    ///
    /// Each entry consists of a separator key and a pointer to a child page.
    /// The separator key is used during tree navigation to determine which
    /// child page to follow for a given search term.
    ///
    /// # Arguments
    ///
    /// * `term` - The separator key as binary data
    /// * `child_position` - The position of the child page this entry points to
    /// * `terms_stripe` - The stripe builder for writing pages when needed
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the operation fails.
    ///
    /// # Automatic Page Splitting
    ///
    /// If adding this entry would cause the page to exceed the maximum size,
    /// the current page is automatically flushed to storage before adding the entry.
    pub fn push(
        &mut self,
        term: &[u8],
        child_position: u64,
        terms_stripe: &mut StripeBuilder,
    ) -> Result<()> {
        self.inner.push(term, child_position);

        if self.inner.estimated_size() > Self::MAX_INTERIOR_PAGE_SIZE {
            self.flush_page(terms_stripe, false)?;
        }
        Ok(())
    }

    /// Flushes the current interior page to storage and optionally propagates to parent.
    ///
    /// This method writes the current page content to the terms stripe and creates
    /// a parent page reference if appropriate. The behavior differs based on whether
    /// this is part of normal page splitting or final tree sealing.
    ///
    /// # Arguments
    ///
    /// * `terms_stripe` - The stripe builder for writing the page data
    /// * `sealing` - Whether this flush is part of final tree construction
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the operation fails.
    fn flush_page(&mut self, terms_stripe: &mut StripeBuilder, sealing: bool) -> Result<()> {
        // Write the current interior page to the terms stripe.
        let mut inner =
            std::mem::replace(&mut self.inner, InteriorPageRecordBuilder::new(self.level));
        let last_term = inner
            .last_term()
            .expect("The inner page record builder must not be empty")
            .to_owned();
        let page_position = terms_stripe.next_record_position();
        terms_stripe.push_batch(&inner.finish())?;

        // When sealing, we propagate the entry to the parent only if
        // the parent already exists (otherwise, we'd end up creating
        // the parent encoders infinitely).
        if !sealing || self.parent.is_some() {
            self.establish_parent();

            self.parent
                .as_mut()
                .unwrap()
                .push(&last_term, page_position, terms_stripe)?;
        }

        Ok(())
    }

    /// Creates a parent page for this interior page if one doesn't exist.
    ///
    /// This method is called when the current page needs to add an entry to its parent.
    /// If no parent exists, a new one is created at the next higher level.
    fn establish_parent(&mut self) {
        if self.parent.is_none() {
            self.parent = Some(Box::new(InteriorPageBuilder::new(self.level + 1)));
        }
    }

    /// Finalizes this interior page and all its parent pages.
    ///
    /// This method completes the construction of the interior page by flushing
    /// any remaining content to storage and recursively finishing parent pages.
    /// This process continues up the tree until the root is reached.
    ///
    /// # Arguments
    ///
    /// * `terms_stripe` - The stripe builder for writing page data
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the operation fails.
    pub fn finish(mut self, terms_stripe: &mut StripeBuilder) -> Result<()> {
        if !self.inner.is_empty() {
            self.flush_page(terms_stripe, true)?;
        }

        if let Some(parent) = self.parent.take() {
            parent.finish(terms_stripe)?;
        }
        Ok(())
    }
}

/// Builder for constructing leaf pages of the B-Tree.
///
/// Leaf pages are the bottom level of the B-Tree and contain the actual indexed terms
/// along with their position information organized by stripe and field. Unlike interior
/// pages, leaf pages do not contain child page pointers.
///
/// When a leaf page becomes full (exceeds 8KB), it automatically splits by flushing
/// its content to storage and creating a reference in its parent interior page.
///
/// # Page Structure
///
/// Leaf pages contain entries with:
/// - Terms as binary data (the actual indexed content)
/// - Position references organized hierarchically by stripe and field
/// - Metadata including representation types and position offsets
/// - No child page pointers (child_position field is always null)
///
/// # Hierarchical Organization
///
/// Position data within each term is organized as:
/// - Term → List of Stripes → List of Fields → Position metadata
/// - Each field contains schema ID, representation type, and position offset
/// - This structure enables efficient queries across different data partitions
struct LeafPageBuilder {
    parent: InteriorPageBuilder,
    inner: LeafPageRecordBuilder,
}

impl LeafPageBuilder {
    /// Maximum size limit for leaf pages in bytes (8KB).
    ///
    /// When a leaf page exceeds this size, it will be flushed to storage
    /// and a new page will be started. Leaf pages are larger than interior
    /// pages because they contain more complex position data.
    const MAX_LEAF_PAGE_SIZE: usize = 8 * 1024;

    /// Creates a new leaf page builder.
    ///
    /// Initializes a new leaf page builder with its parent interior page builder
    /// at level 1. The leaf page itself is at level 0 in the B-Tree hierarchy.
    ///
    /// # Returns
    ///
    /// A new `LeafPageBuilder` ready to accept term entries.
    pub fn new() -> Self {
        let parent = InteriorPageBuilder::new(1);
        let record_builder = LeafPageRecordBuilder::new();
        Self {
            parent,
            inner: record_builder,
        }
    }

    /// Adds a term entry to this leaf page.
    ///
    /// This method processes a term occurrence by first storing its position data
    /// in the positions shard, then adding the term metadata to the current leaf page.
    /// The position data is stored separately and referenced by offset.
    ///
    /// # Arguments
    ///
    /// * `term` - The term as binary data
    /// * `stripe_id` - The logical partition identifier for this term occurrence
    /// * `field_schema_id` - The schema identifier for the field containing this term
    /// * `positions` - The position list indicating where the term appears
    /// * `terms_stripe` - The stripe builder for writing pages when needed
    /// * `positions_builder` - The positions shard builder for storing position data
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the operation fails.
    ///
    /// # Automatic Page Splitting
    ///
    /// If adding this entry would cause the page to exceed the maximum size,
    /// the current page is automatically flushed to storage before adding the entry.
    ///
    /// # Input Ordering Requirements
    ///
    /// Terms must be provided in sorted order: first by term, then by stripe ID,
    /// then by field schema ID. This ensures proper B-Tree structure and efficient queries.
    pub fn push(
        &mut self,
        term: &[u8],
        stripe_id: u16,
        field_schema_id: SchemaId,
        positions_range: Range<u64>,
        repr_type: PositionsReprType,
        terms_stripe: &mut StripeBuilder,
    ) -> Result<()> {
        // If this is the first entry on a fresh page, capture the page-level start offset
        self.inner
            .set_page_positions_start_offset(positions_range.start);

        self.inner.push(
            term,
            stripe_id,
            field_schema_id.as_u32(),
            repr_type as u8,
            positions_range.end,
        );

        if self.inner.estimated_size() > Self::MAX_LEAF_PAGE_SIZE {
            self.flush_page(terms_stripe)?;
        }
        Ok(())
    }

    /// Flushes the current leaf page to storage and creates a parent reference.
    ///
    /// This method writes the current page content to the terms stripe and adds
    /// a reference to this page in the parent interior page. The last term in
    /// the page becomes the separator key for navigation.
    ///
    /// # Arguments
    ///
    /// * `terms_stripe` - The stripe builder for writing the page data
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the operation fails.
    fn flush_page(&mut self, terms_stripe: &mut StripeBuilder) -> Result<()> {
        // Write the current leaf page to the terms stripe.
        let mut inner = std::mem::replace(&mut self.inner, LeafPageRecordBuilder::new());
        let last_term = inner
            .last_term()
            .expect("The inner page record builder must not be empty")
            .to_owned();
        let page_position = terms_stripe.next_record_position();
        terms_stripe.push_batch(&inner.finish())?;

        // Add a reference to this leaf page in the parent page.
        self.parent.push(&last_term, page_position, terms_stripe)?;

        Ok(())
    }

    /// Finalizes this leaf page and all parent pages.
    ///
    /// This method completes the construction of the leaf page by flushing
    /// any remaining content to storage and recursively finishing all parent
    /// interior pages up to the root.
    ///
    /// # Arguments
    ///
    /// * `terms_stripe` - The stripe builder for writing page data
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the operation fails.
    pub fn finish(mut self, terms_stripe: &mut StripeBuilder) -> Result<()> {
        if !self.inner.is_empty() {
            self.flush_page(terms_stripe)?;
        }
        self.parent.finish(terms_stripe)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pos_list::PositionsReprType;
    use amudai_format::schema::SchemaId;
    use amudai_io_impl::temp_file_store;
    use amudai_objectstore::null_store::NullObjectStore;

    #[test]
    fn test_btree_encoder_builds_and_seals() {
        let object_store = Arc::new(NullObjectStore);
        let temp_store = temp_file_store::create_in_memory(10 * 1024 * 1024)
            .expect("Failed to create temporary file store");
        let mut encoder = BTreeEncoder::new(object_store.clone(), temp_store.clone())
            .expect("Failed to create BTreeEncoder");

        // Act: push a few ordered term postings spanning multiple stripes/fields
        let field1 = SchemaId::from(1u32);
        let field2 = SchemaId::from(2u32);

        // We'll simulate contiguous positions storage by advancing a running offset.
        let mut next_off: u64 = 0;

        encoder
            .push(
                b"alpha",
                0,
                field1,
                {
                    let start = next_off;
                    next_off += 2;
                    start..next_off
                },
                PositionsReprType::Positions,
            )
            .expect("push alpha-0-f1 failed");
        encoder
            .push(
                b"alpha",
                0,
                field2,
                {
                    let start = next_off;
                    next_off += 2;
                    start..next_off
                },
                PositionsReprType::ExactRanges,
            )
            .expect("push alpha-0-f2 failed");
        encoder
            .push(
                b"alpha",
                1,
                field1,
                {
                    let start = next_off;
                    next_off += 2;
                    start..next_off
                },
                PositionsReprType::ApproximateRanges,
            )
            .expect("push alpha-1-f1 failed");
        encoder
            .push(
                b"beta",
                0,
                field1,
                {
                    let start = next_off;
                    next_off += 1;
                    start..next_off
                },
                PositionsReprType::Positions,
            )
            .expect("push beta-0-f1 failed");

        let prepared = encoder.finish().expect("Failed to finish BTreeEncoder");

        // Assert: shards can be sealed (writes go to NullObjectStore)
        let _sealed_terms = prepared
            .seal("test://container/terms.amudai.shard")
            .expect("Failed to seal terms shard");
    }
}
