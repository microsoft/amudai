//! High-level text index encoder coordinating B-Tree and position storage.
//!
//! This module provides the primary interface for building inverted text indexes
//! in the Amudai format. It coordinates between B-Tree construction and position
//! data storage to create a complete, queryable text index.

use std::sync::Arc;

use amudai_common::Result;
use amudai_format::schema::SchemaId;
use amudai_io::TemporaryFileStore;
use amudai_objectstore::ObjectStore;

use crate::write::{
    TermPosting, TermPostingSink, btree::BTreeEncoder, positions::PositionsEncoder,
    poslist_builder::PositionListBuilder,
};
use amudai_shard::write::shard_builder::{PreparedShard, SealedShard};

/// High-level encoder for building complete inverted text indexes.
///
/// The `TextIndexEncoder` provides the primary interface for constructing text indexes
/// by coordinating between B-Tree term storage and position data management. It
/// implements the two-shard architecture that separates navigation structure from
/// detailed position information.
///
/// # Architecture
///
/// The encoder manages two separate components:
/// - **B-Tree Encoder**: Builds the term navigation structure
/// - **Positions Encoder**: Stores position lists with efficient compression
///
/// This separation enables optimal storage characteristics: compact navigation
/// structure for fast term lookups, and separately compressed position data
/// that's only accessed when needed.
///
/// # Usage Pattern
///
/// Terms must be provided in sorted order: first by term (lexicographically),
/// then by stripe ID, then by field schema ID. This ordering is essential for
/// the B-Tree structure to function correctly during query operations.
pub struct TextIndexEncoder {
    /// A B-Tree encoder used for writing the term dictionary and postings data.
    btree: BTreeEncoder,
    /// A positions shard builder used for writing position lists data.
    positions: PositionsEncoder,
    /// Builder for aggregating positions for the current term/stripe/field combination.
    position_builder: PositionListBuilder,
    /// Current term being processed. The vector is reused to avoid allocations.
    current_term: Vec<u8>,
    /// Current posting being processed.
    current_posting: TermPosting,
}

impl TextIndexEncoder {
    /// Creates a new index encoder with the specified storage backends.
    ///
    /// This method initializes both the B-Tree encoder and positions encoder
    /// with their respective storage configurations. The encoders will use
    /// the provided storage backends for persisting the final index data.
    ///
    /// # Arguments
    ///
    /// * `object_store` - Storage backend for persisting the final shards
    /// * `temp_store` - Temporary storage for intermediate data during construction
    ///
    /// # Returns
    ///
    /// A new TextIndexEncoder instance ready for receiving term postings, or an
    /// error if initialization of either encoder fails.
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        temp_store: Arc<dyn TemporaryFileStore>,
    ) -> Result<Self> {
        let btree = BTreeEncoder::new(Arc::clone(&object_store), Arc::clone(&temp_store))?;
        let positions = PositionsEncoder::new(object_store, temp_store)?;
        Ok(Self {
            btree,
            positions,
            position_builder: PositionListBuilder::new(),
            current_term: vec![],
            current_posting: TermPosting {
                stripe: u16::MAX,
                field: SchemaId::invalid(),
                pos: u32::MAX,
            },
        })
    }

    /// Flushes the current position list being built and pushes it to the index.
    ///
    /// This method is called when switching to a new stripe or field to ensure
    /// that accumulated positions for the current term/stripe/field combination
    /// are properly stored in the index.
    fn flush_positions(&mut self) -> Result<()> {
        if !self.position_builder.is_empty() {
            let position_list = std::mem::take(&mut self.position_builder).build();

            let positions_start = self.positions.current_offset();
            let (repr_type, positions_end) = self.positions.push(position_list)?;
            let positions_range = positions_start..positions_end;

            self.btree.push(
                self.current_term.as_slice(),
                self.current_posting.stripe,
                self.current_posting.field,
                positions_range,
                repr_type,
            )?;
        }
        Ok(())
    }

    /// Finalizes index construction and returns the prepared shards.
    ///
    /// This method completes the index building process by finalizing both the
    /// B-Tree structure and the positions data storage. The resulting prepared
    /// shards can be sealed and stored for later querying.
    ///
    /// # Returns
    ///
    /// A `PreparedIndex` containing both prepared shards (terms and positions),
    /// or an error if the finalization process fails.
    ///
    /// # Resource Management
    ///
    /// This method consumes the encoder, ensuring that all buffered data is
    /// properly flushed and finalized. After calling this method, the encoder
    /// cannot be used for further operations.
    pub fn finish(mut self) -> Result<PreparedTextIndex> {
        // Flush any remaining positions before finalizing
        self.flush_positions()?;

        let terms_shard = self.btree.finish()?;
        let positions_shard = self.positions.finish()?;
        Ok(PreparedTextIndex {
            terms_shard,
            positions_shard,
        })
    }
}

impl TermPostingSink for TextIndexEncoder {
    /// Begins processing a new term in the inverted index.
    fn start_term(&mut self, _term_ordinal: usize, _is_null: bool, term: &[u8]) -> Result<()> {
        // Flush any existing positions for the previous term
        self.flush_positions()?;

        self.current_term.clear();
        self.current_term.extend_from_slice(term);

        Ok(())
    }

    /// Adds a posting to the current term being processed.
    fn push_posting(&mut self, posting: &TermPosting) -> Result<()> {
        if self.current_posting.stripe != posting.stripe
            || self.current_posting.field != posting.field
        {
            // Flush positions if we are switching stripes or fields
            self.flush_positions()?;
        } else {
            // Avoid duplicate positions for the same stripe/field
            if self.current_posting.pos == posting.pos {
                return Ok(());
            }
        }
        self.current_posting = *posting;

        // Add position to the builder
        self.position_builder.insert(posting.pos as u64);

        Ok(())
    }

    /// Finalizes processing for the current term.
    fn end_term(&mut self) -> Result<()> {
        // Flush the final positions for this term
        self.flush_positions()?;

        Ok(())
    }
}

/// A completed inverted text index ready for storage and querying.
///
/// `PreparedIndex` represents the final result of the index building process,
/// containing both shards that make up the complete inverted text index.
/// The shards can be sealed and stored for later use in query operations.
///
/// # Architecture
///
/// The prepared index maintains the two-shard architecture:
/// - **Terms Shard**: Contains B-Tree navigation structure and term metadata
/// - **Positions Shard**: Contains detailed position lists for memory efficiency
///
/// # Usage
///
/// After obtaining a `PreparedIndex`, the shards should be sealed with
/// appropriate URLs and stored in the object store for later querying
/// via the `TextIndex` decoder.
pub struct PreparedTextIndex {
    /// The shard containing B-Tree structure with terms and navigation information.
    pub terms_shard: PreparedShard,
    /// The shard containing position lists referenced by terms.
    pub positions_shard: PreparedShard,
}

impl PreparedTextIndex {
    /// Seals the index, finalizing its storage.
    ///
    /// # Errors
    /// Returns an error if any partition fails to seal or storage issues occur
    pub fn seal(self, index_url: &str) -> Result<SealedTextIndex> {
        let terms_shard = self
            .terms_shard
            .seal(format!("{index_url}/terms.shard").as_str())?;
        let positions_shard = self
            .positions_shard
            .seal(format!("{index_url}/positions.shard").as_str())?;
        Ok(SealedTextIndex {
            terms_shard,
            positions_shard,
        })
    }
}

/// A sealed index that has been written to persistent storage and is ready for querying.
pub struct SealedTextIndex {
    /// The shard containing B-Tree structure with terms and navigation information.
    pub terms_shard: SealedShard,
    /// The shard containing position lists referenced by terms.
    pub positions_shard: SealedShard,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::TermPosting;
    use amudai_io_impl::temp_file_store;
    use amudai_objectstore::null_store::NullObjectStore;

    #[test]
    fn test_index_encoder_multiple_terms_stripes_fields() {
        // Setup storage backends
        let object_store = Arc::new(NullObjectStore);
        let temp_store = temp_file_store::create_in_memory(10 * 1024 * 1024)
            .expect("Failed to create temporary file store");

        let mut encoder =
            TextIndexEncoder::new(object_store, temp_store).expect("Failed to create IndexEncoder");

        // Test data with multiple terms, stripes, and fields
        let test_data = [
            // Term "apple" - multiple stripes and fields
            (
                "apple",
                vec![
                    TermPosting {
                        stripe: 0,
                        field: SchemaId::from(1u32),
                        pos: 10,
                    },
                    TermPosting {
                        stripe: 0,
                        field: SchemaId::from(1u32),
                        pos: 25,
                    },
                    TermPosting {
                        stripe: 0,
                        field: SchemaId::from(2u32),
                        pos: 5,
                    },
                    TermPosting {
                        stripe: 1,
                        field: SchemaId::from(1u32),
                        pos: 15,
                    },
                    TermPosting {
                        stripe: 1,
                        field: SchemaId::from(3u32),
                        pos: 8,
                    },
                ],
            ),
            // Term "banana" - different stripe/field combinations
            (
                "banana",
                vec![
                    TermPosting {
                        stripe: 0,
                        field: SchemaId::from(2u32),
                        pos: 12,
                    },
                    TermPosting {
                        stripe: 0,
                        field: SchemaId::from(3u32),
                        pos: 3,
                    },
                    TermPosting {
                        stripe: 2,
                        field: SchemaId::from(1u32),
                        pos: 20,
                    },
                    TermPosting {
                        stripe: 2,
                        field: SchemaId::from(2u32),
                        pos: 7,
                    },
                ],
            ),
            // Term "cherry" - single stripe but multiple fields
            (
                "cherry",
                vec![
                    TermPosting {
                        stripe: 1,
                        field: SchemaId::from(1u32),
                        pos: 2,
                    },
                    TermPosting {
                        stripe: 1,
                        field: SchemaId::from(1u32),
                        pos: 18,
                    },
                    TermPosting {
                        stripe: 1,
                        field: SchemaId::from(2u32),
                        pos: 30,
                    },
                    TermPosting {
                        stripe: 1,
                        field: SchemaId::from(3u32),
                        pos: 1,
                    },
                    TermPosting {
                        stripe: 1,
                        field: SchemaId::from(3u32),
                        pos: 22,
                    },
                ],
            ),
            // Term "date" - sparse across different stripes
            (
                "date",
                vec![
                    TermPosting {
                        stripe: 0,
                        field: SchemaId::from(1u32),
                        pos: 35,
                    },
                    TermPosting {
                        stripe: 3,
                        field: SchemaId::from(2u32),
                        pos: 14,
                    },
                    TermPosting {
                        stripe: 5,
                        field: SchemaId::from(1u32),
                        pos: 9,
                    },
                ],
            ),
        ];

        // Process all terms and their postings
        for (term_ordinal, (term, postings)) in test_data.iter().enumerate() {
            encoder
                .start_term(term_ordinal, false, term.as_bytes())
                .expect("Failed to start term");

            for posting in postings {
                encoder
                    .push_posting(posting)
                    .expect("Failed to push posting");
            }

            encoder.end_term().expect("Failed to end term");
        }

        // Finish encoding and get prepared index
        let prepared_index = encoder.finish().expect("Failed to finish encoding");

        // Verify that both shards are created and can be accessed
        // This test verifies that the TextIndexEncoder properly coordinates between
        // the B-Tree encoder and positions encoder to create separate shards
        let _terms_shard = prepared_index.terms_shard;
        let _positions_shard = prepared_index.positions_shard;
    }
}
