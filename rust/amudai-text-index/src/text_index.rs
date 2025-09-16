//! Text index type implementation for the Amudai indexing system.
//!
//! This module provides the `TextIndexType` implementation of the `IndexType` trait,
//! enabling text indexes to be integrated into the Amudai index ecosystem. The text
//! index type supports full-text search capabilities with inverted term indexing.

use ahash::AHashSet;
use std::sync::{Arc, Weak};

use amudai_common::{Result, error::Error};
use amudai_format::{
    defs::{common::any_value, shard::IndexDescriptor},
    schema::BasicType,
};
use amudai_index_core::{
    IndexBuilder, IndexMaintainer, IndexReader, IndexType,
    builder::Parameters,
    metadata::{self, Capabilities, Granularity, PositionFidelity, QueryKind, ScopeKind},
    shard_markers::DynShard,
};
use amudai_shard::read::shard::Shard;

use crate::read::reader::TextIndexReader;
use crate::write::builder::{TextIndexBuilder, TextIndexBuilderConfig};

/// Text index type implementation for full-text search capabilities.
///
/// The `TextIndexType` provides inverted text indexing with support for:
/// - Exact term lookups
/// - Prefix-based term searches  
/// - Multiple field indexing
/// - Configurable collation strategies
/// - Two-shard architecture (terms + positions)
///
/// This index type is optimized for text search workloads and provides
/// efficient term-based filtering for large text collections.
pub struct TextIndexType {
    /// Weak self-reference to enable Arc creation with cyclic references
    this: Weak<TextIndexType>,
    /// Capabilities supported by this index type
    capabilities: Capabilities,
}

impl TextIndexType {
    /// The canonical name for the text index type.
    pub const NAME: &'static str = "inverted-text-index-v1";

    /// Creates a new text index type instance.
    ///
    /// Initializes the index type with its supported capabilities, including
    /// scope patterns, granularity levels, position fidelity, and query types.
    pub fn new() -> Arc<Self> {
        Arc::new_cyclic(|this| Self {
            this: this.clone(),
            capabilities: Self::build_capabilities(),
        })
    }

    /// Returns an Arc to this index type.
    ///
    /// This method upgrades the weak self-reference to a strong reference,
    /// enabling the index type to provide itself to builders and other components
    /// that need a reference back to the type definition.
    ///
    /// # Returns
    ///
    /// An Arc-wrapped reference to this index type
    ///
    /// # Panics
    ///
    /// Panics if the weak reference cannot be upgraded (should never happen in practice)
    pub fn shared_from_self(&self) -> Arc<dyn IndexType> {
        self.this.upgrade().expect("upgrade")
    }

    /// Creates the capabilities supported by the text index type.
    ///
    /// Text indexes support:
    /// - Multi-field indexing at stripe and shard levels
    /// - Element-level position granularity for precise results
    /// - Exact position fidelity for reliable search results
    /// - String data type indexing
    /// - Term and prefix query operations
    fn build_capabilities() -> Capabilities {
        let mut scope = AHashSet::new();
        scope.insert(ScopeKind::MultiStripeField);
        scope.insert(ScopeKind::MultiShardField);

        let mut granularity = AHashSet::new();
        granularity.insert(Granularity::Element);

        let mut fidelity = AHashSet::new();
        fidelity.insert(PositionFidelity::Exact);

        let mut data_types = AHashSet::new();
        data_types.insert(BasicType::String);

        let mut query_kind = AHashSet::new();
        query_kind.insert(QueryKind::Term);
        query_kind.insert(QueryKind::TermPrefix);
        query_kind.insert(QueryKind::TermSet);
        query_kind.insert(QueryKind::TermPrefixSet);
        query_kind.insert(QueryKind::Custom); // For opaque text queries

        Capabilities {
            scope,
            granularity,
            fidelity,
            data_types,
            query_kind,
            value_size: 0..usize::MAX, // Support any string length
        }
    }
}

impl IndexType for TextIndexType {
    /// Returns the canonical name for this index type.
    fn name(&self) -> &str {
        Self::NAME
    }

    /// Returns the capabilities supported by this index type.
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    /// Creates a new index builder for constructing text indexes.
    ///
    /// # Arguments
    ///
    /// * `parameters` - Configuration including object stores, scope, and properties
    ///
    /// # Returns
    ///
    /// A boxed IndexBuilder ready to process data frames and build text indexes
    ///
    /// # Errors
    ///
    /// Returns an error if the builder cannot be created due to missing parameters
    /// or incompatible configuration.
    fn create_builder(&self, parameters: Parameters) -> Result<Box<dyn IndexBuilder>> {
        let field_count = match &parameters.scope {
            metadata::Scope::StripeField(f) | metadata::Scope::ShardField(f) => f.schema_ids.len(),
            metadata::Scope::MultiStripeField(v) | metadata::Scope::MultiShardField(v) => {
                v.iter().map(|f| f.schema_ids.len()).sum()
            }
        };

        let collation = parameters
            .properties
            .get("collation")
            .and_then(|av| av.as_str().map(|s| s.to_string()))
            .unwrap_or_else(|| "unicode-case-insensitive".to_string());

        let mut tokenizers = vec!["unicode-word".to_string(); field_count];
        if let Some(any_val) = parameters.properties.get("tokenizers") {
            if let Some(any_value::Kind::ListValue(list_val)) = &any_val.kind {
                for (idx, elem) in list_val.elements.iter().enumerate() {
                    if idx >= tokenizers.len() {
                        break;
                    }
                    if let Some(any_value::Kind::StringValue(s)) = &elem.kind {
                        tokenizers[idx] = s.clone();
                    }
                }
            }
        }

        let config = TextIndexBuilderConfig {
            max_fragment_size: None,
            tokenizers,
            collation,
        };
        let builder = TextIndexBuilder::new(config, self.shared_from_self(), parameters)?;
        Ok(Box::new(builder))
    }

    /// Creates an index reader for querying previously built text indexes.
    ///
    /// Opens a reader that can perform text-based searches against the stored
    /// index data. The reader requires access to both the terms shard (B-Tree)
    /// and positions shard (position lists).
    ///
    /// # Arguments
    ///
    /// * `descriptor` - Metadata describing the index structure and shard locations
    /// * `shard` - Optional shard context providing object store access
    ///
    /// # Returns
    ///
    /// An IndexReader capable of performing text search operations
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Required shard URLs cannot be extracted from the descriptor
    /// - Object store access cannot be established
    /// - Index shards cannot be opened or are corrupted
    fn open_reader(
        &self,
        descriptor: &IndexDescriptor,
        shard: Option<Box<dyn DynShard>>,
    ) -> Result<Arc<dyn IndexReader>> {
        // Try to reuse the object store and reference resolver from the provided shard when available,
        // otherwise we expect readers to initialize their own from context.
        let (object_store, reference_resolver) = if let Some(shard) = shard {
            let shard = Shard::try_from(shard).map_err(|_| {
                Error::invalid_operation("text index reader: unexpected shard type")
            })?;
            (
                Arc::clone(shard.object_store()),
                Some(Arc::clone(shard.reference_resolver())),
            )
        } else {
            // Without a shard we can't infer the object store; return a clear error.
            return Err(Error::invalid_operation(
                "text index reader requires shard to supply object store",
            ));
        };

        let reader = TextIndexReader::open(
            self.shared_from_self(),
            descriptor,
            object_store,
            reference_resolver,
        )?;
        Ok(Arc::new(reader))
    }

    /// Creates an index maintainer for text index maintenance operations.
    ///
    /// Text indexes may benefit from periodic maintenance such as:
    /// - Merging position lists for better compression
    /// - Rebalancing B-Tree structures
    /// - Cleaning up unused terms
    ///
    /// For now, returns a no-op maintainer since text indexes are primarily
    /// read-only after construction.
    fn create_maintainer(&self) -> Result<Box<dyn IndexMaintainer>> {
        struct NoopMaintainer;
        impl IndexMaintainer for NoopMaintainer {}
        Ok(Box::new(NoopMaintainer))
    }
}
