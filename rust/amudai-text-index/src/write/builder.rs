//! # Text Index Builder
//!
//! This module provides the core functionality for building searchable text indexes from textual data.
//! It implements a memory-efficient, scalable approach to text indexing that can handle large datasets
//! by using a fragment-based architecture with automatic spilling to disk when memory limits are exceeded.
//!
//! ## Architecture
//!
//! The text index builder operates on several key concepts:
//!
//! - **Fragments**: In-memory data structures that accumulate tokenized terms and their positions.
//!   When fragments grow too large, they are automatically spilled to temporary disk storage.
//!
//! - **Stripes**: Logical partitions of data that maintain ordering and organization within the index.
//!   Data is processed stripe by stripe, with automatic fragment boundaries at stripe transitions.
//!
//! - **Tokenizers**: Configurable text processing components that break raw text into searchable terms.
//!   Different fields can use different tokenization strategies based on their content type.
//!
//! - **Collation**: Text comparison and sorting strategies that determine how terms are ordered
//!   within the index, supporting different linguistic and cultural requirements.
//!
//! ## Usage Flow
//!
//! 1. Create a `TextIndexBuilderConfig` with appropriate settings for tokenizers, collation, and memory limits
//! 2. Initialize a `TextIndexBuilder` with the configuration and storage backends
//! 3. Process text data by calling `set_stripe()` and `process_text_field()` for each text field
//! 4. Call `finish()` to complete the index construction and obtain a `PreparedTextIndex`
//!
//! The builder automatically manages memory usage and disk spilling, making it suitable for
//! processing datasets that exceed available memory.

use std::{io::Read, sync::Arc};

use amudai_common::{Result, error::Error};
use amudai_format::schema::SchemaId;
use amudai_io::TemporaryFileStore;
use amudai_objectstore::ObjectStore;

use crate::{
    collation,
    tokenizers::{self, TokenizerType},
    write::{
        encoder::{PreparedTextIndex, TextIndexEncoder},
        fragments::{self, IndexFragment, IndexFragmentDecoder},
    },
};

/// Configuration parameters for building a text index.
///
/// This struct contains all the necessary settings to control how a text index
/// is constructed, including memory management, tokenization, and collation settings.
pub struct TextIndexBuilderConfig {
    /// Maximum size of the in-memory fragment. When this size is exceeded,
    /// the fragment is spilled to disk and the new fragment is created.
    pub max_fragment_size: Option<usize>,

    /// Field tokenizers to use when breaking text values into terms.
    pub tokenizers: Vec<String>,

    /// The name of the collation to use for terms comparison.
    pub collation: String,
}

/// The main entry point for constructing text indexes from textual data.
///
/// The `TextIndexBuilder` is the primary interface for building efficient searchable indexes
/// from text content. It processes text fields and builds an optimized index by tokenizing
/// text content, managing memory through fragment-based storage, and handling large datasets
/// by spilling intermediate results to disk when memory limits are exceeded.
///
/// This builder provides a high-level API that abstracts the complexity of text indexing,
/// including memory management, tokenization strategies, and efficient storage organization.
/// The builder operates on a stripe-based architecture where data is organized into stripes,
/// and each stripe can contain multiple text fields that are tokenized and indexed.
pub struct TextIndexBuilder {
    /// Storage backend for persisting the final constructed text index.
    /// Used during the finalization phase to store the merged and optimized index data.
    object_store: Arc<dyn ObjectStore>,

    /// Temporary storage facility for intermediate fragment data during index construction.
    /// When in-memory fragments exceed size limits, they are spilled to this temporary store
    /// until the final merge phase.
    temp_store: Arc<dyn TemporaryFileStore>,

    /// Collation strategy used for comparing and ordering text terms within the index.
    /// Determines the sorting behavior and equality semantics for indexed terms, supporting
    /// different linguistic and cultural text comparison rules.
    collation: Box<dyn collation::Collation>,

    /// Maximum allowed size in bytes for in-memory fragments before they are spilled to disk.
    /// Controls memory usage during index construction by triggering fragment persistence
    /// when this threshold is exceeded.
    max_fragment_size: usize,

    /// Collection of tokenizers, one for each field schema ID, used to break text content
    /// into searchable terms. The tokenizer at index N corresponds to schema field ID N,
    /// allowing different fields to use different tokenization strategies.
    field_tokenizers: Vec<TokenizerType>,

    /// The currently active in-memory fragment that accumulates tokenized terms and their
    /// positions. When this fragment grows too large or when switching stripes, it gets
    /// spilled to temporary storage.
    current_fragment: IndexFragment,

    /// Collection of readers for fragments that have been spilled to disk storage.
    /// During finalization, these readers are used to access and merge all fragment data
    /// into the final consolidated index.
    fragments: Vec<Box<dyn Read>>,
}

impl TextIndexBuilder {
    const DEFAULT_MAX_FRAGMENT_SIZE: usize = 64 * 1024 * 1024; // 64 MiB

    /// Creates a new `TextIndexBuilder` with the specified configuration.
    ///
    /// This constructor initializes all components needed for text indexing, including
    /// setting up tokenizers for each field, creating the collation strategy for term
    /// comparison, and establishing the fragment management system.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration parameters controlling the indexing behavior
    /// * `object_store` - Storage backend for persisting the final index
    /// * `temp_store` - Temporary storage for intermediate fragments during construction
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the initialized builder or an error if configuration
    /// validation fails, such as invalid tokenizer names or unsupported collation.
    pub fn new(
        config: TextIndexBuilderConfig,
        object_store: Arc<dyn ObjectStore>,
        temp_store: Arc<dyn TemporaryFileStore>,
    ) -> Result<Self> {
        let max_fragment_size = config
            .max_fragment_size
            .unwrap_or(Self::DEFAULT_MAX_FRAGMENT_SIZE);

        let collation = collation::create_collation(&config.collation)?;

        let field_tokenizers = config
            .tokenizers
            .iter()
            .map(|name| tokenizers::create_tokenizer(name))
            .collect::<Result<Vec<_>>>()?;

        let current_fragment = IndexFragment::new(collation.clone_boxed());

        Ok(Self {
            object_store,
            temp_store,
            collation,
            max_fragment_size,
            field_tokenizers,
            current_fragment,
            fragments: vec![],
        })
    }

    /// Sets the current stripe index for subsequent text processing operations.
    ///
    /// Stripes represent logical partitions of data within the index. When transitioning
    /// to a new stripe, any existing in-memory fragment data is automatically spilled to
    /// disk to maintain stripe boundaries and ensure proper data organization.
    ///
    /// # Arguments
    ///
    /// * `stripe_idx` - The index of the stripe to set as current. Must be greater than
    ///   the current stripe index to maintain ordering.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the stripe transition fails during
    /// fragment spilling operations.
    ///
    /// # Panics
    ///
    /// Panics in debug builds if the new stripe index is not greater than the current one,
    /// as this would violate the expected stripe ordering.
    pub fn set_stripe(&mut self, stripe_idx: u16) -> Result<()> {
        if stripe_idx != self.current_fragment.stripe() {
            assert!(stripe_idx > self.current_fragment.stripe());
            if self.current_fragment.entry_count() != 0 {
                self.spill_current_fragment()?;
            }
            self.current_fragment.set_stripe(stripe_idx);
        }
        Ok(())
    }

    /// Spills the current in-memory fragment to temporary disk storage.
    ///
    /// This method is called automatically when the current fragment exceeds the maximum
    /// size threshold or when transitioning between stripes. The fragment data is serialized
    /// and written to a temporary stream, which is then converted to a reader for later
    /// processing during the final merge phase.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the fragment is successfully spilled to disk, or an error if
    /// temporary storage allocation or fragment serialization fails.
    fn spill_current_fragment(&mut self) -> Result<()> {
        let mut temp_stream = self
            .temp_store
            .allocate_stream(Some(self.current_fragment.estimate_size()))?;
        self.current_fragment.spill_to_disk(&mut temp_stream)?;
        self.fragments.push(temp_stream.into_reader()?);
        Ok(())
    }

    /// Processes a text field by tokenizing its content and adding it to the current index fragment.
    ///
    /// This method takes raw text content, applies the appropriate tokenizer based on the field's
    /// schema ID, and stores the resulting tokens in the current fragment along with their position
    /// information. If processing this text would cause the fragment to exceed the maximum size
    /// threshold, the current fragment is automatically spilled to disk.
    ///
    /// # Arguments
    ///
    /// * `text` - The raw text content to be tokenized and indexed
    /// * `field` - The schema ID identifying which field this text belongs to, used to select
    ///   the appropriate tokenizer
    /// * `position` - The position or record number where this text field appears in the dataset
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the text is successfully processed and added to the index, or an error
    /// if no tokenizer is configured for the specified field or if fragment processing fails.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - No tokenizer is configured for the provided field schema ID
    /// - Text tokenization fails
    /// - Fragment size estimation or spilling operations fail
    pub fn process_text_field(&mut self, text: &str, field: SchemaId, position: u32) -> Result<()> {
        let tokenizer = self.field_tokenizers.get(field.as_usize()).ok_or_else(|| {
            Error::invalid_operation(format!(
                "No tokenizer configured for field schema ID {}",
                field
            ))
        })?;

        self.current_fragment
            .process_text(text, tokenizer, field, position)?;

        if self.current_fragment.estimate_size() > self.max_fragment_size {
            self.spill_current_fragment()?;
        }
        Ok(())
    }

    /// Completes the index construction process and returns a prepared text index.
    ///
    /// This method finalizes the indexing process by spilling any remaining in-memory fragment
    /// data to disk, then merging all fragment decoders using the configured collation strategy
    /// to produce a unified, optimized text index. The resulting index is ready for querying
    /// and can be persisted to the object store.
    ///
    /// This method consumes the builder, as the index construction process is complete and
    /// the builder cannot be reused.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a `PreparedTextIndex` ready for sealing, or an error
    /// if the final fragment spilling, decoder creation, or merge operations fail.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - Final fragment spilling to disk fails
    /// - Fragment decoder creation fails for any spilled fragment
    /// - Text index encoder initialization fails
    /// - Fragment merging process encounters errors
    /// - Final index preparation fails
    pub fn finish(mut self) -> Result<PreparedTextIndex> {
        if self.current_fragment.entry_count() != 0 {
            self.spill_current_fragment()?;
        }

        let readers = std::mem::take(&mut self.fragments);
        let mut decoders = Vec::with_capacity(readers.len());
        for reader in readers {
            decoders.push(IndexFragmentDecoder::new(reader)?);
        }

        let mut encoder = TextIndexEncoder::new(self.object_store, self.temp_store)?;
        fragments::merge_fragment_decoders(decoders, self.collation, &mut encoder)?;

        encoder.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use amudai_format::schema::SchemaId;
    use amudai_io_impl::temp_file_store;
    use amudai_objectstore::null_store::NullObjectStore;

    #[test]
    fn test_text_index_builder_basic_functionality() {
        // Create test stores
        let object_store = Arc::new(NullObjectStore);
        let temp_store = temp_file_store::create_in_memory(10 * 1024 * 1024)
            .expect("Failed to create temporary file store");

        // Create a basic configuration
        let config = TextIndexBuilderConfig {
            max_fragment_size: Some(1024), // Small size to test fragment spilling
            tokenizers: vec!["trivial".to_string(), "unicode-word".to_string()],
            collation: "unicode-case-insensitive".to_string(),
        };

        // Create the builder
        let mut builder = TextIndexBuilder::new(config, object_store, temp_store)
            .expect("Failed to create TextIndexBuilder");

        // Test setting stripe and processing text fields
        builder.set_stripe(0).expect("Failed to set stripe 0");

        // Process some text for different fields
        builder
            .process_text_field("hello world", SchemaId::from(0u32), 1)
            .expect("Failed to process text for field 0");

        builder
            .process_text_field("rust programming", SchemaId::from(1u32), 1)
            .expect("Failed to process text for field 1");

        // Switch to another stripe
        builder.set_stripe(1).expect("Failed to set stripe 1");

        builder
            .process_text_field("another text", SchemaId::from(0u32), 2)
            .expect("Failed to process text for field 0 in stripe 1");

        // Process enough text to trigger fragment spilling
        for i in 0..500 {
            builder
                .process_text_field(
                    &format!("sample text number {} with some content", i),
                    SchemaId::from(0u32),
                    i + 10,
                )
                .expect("Failed to process bulk text");
        }

        // Finish the builder and create the index
        let prepared_index = builder.finish().expect("Failed to finish building index");
        let _sealed_index = prepared_index
            .seal("null:///tmp/test_text_index")
            .expect("Failed to seal text index");
    }
}
