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

use super::hashmap_index::{HashmapIndex, HashmapIndexOptions, LookupResult};
use xxhash_rust::xxh3::xxh3_64;

/// Reader for field-hashing indexes that enables efficient equality lookups on field-value pairs.
///
/// This reader provides query capabilities for hash-based indexes built by `FieldHashingIndexBuilder`.
/// It supports exact-match queries on field paths and their corresponding values, using the same
/// hashing strategy (xxHash3) employed during index construction.
///
/// The reader loads and manages partitioned hashmap data from object storage, enabling fast
/// position lookups based on field-value hash keys. Query results are returned as position
/// sets that can be used to filter data during query processing.
///
/// # Key Synchronization
///
/// The key construction logic in this reader must remain synchronized with the indexing logic
/// in `FieldHashingIndexBuilder` to ensure that query keys match the indexed keys exactly. Both use
/// the same format: `field.path:value` hashed with xxHash3.
pub struct FieldHashingIndexReader {
    /// Reference to the index type that defines the capabilities and behavior of this reader
    index_type: Arc<dyn IndexType>,
    /// Metadata describing the scope, granularity, and query capabilities of this index
    meta: IndexMetadata,
    /// Underlying hashmap index that manages partitioned storage and performs lookups
    index: HashmapIndex,
}

impl FieldHashingIndexReader {
    /// Creates a new FieldHashingIndexReader from an index descriptor and object storage.
    ///
    /// Opens a reader that can perform hash-based lookups against a previously constructed
    /// field-hashing index. The reader loads partition metadata from the descriptor and
    /// prepares to access the underlying hashmap data on demand.
    ///
    /// # Arguments
    ///
    /// * `index_type` - The index type definition that created the original index
    /// * `descriptor` - Metadata describing the index structure, location, and properties
    /// * `object_store` - Storage backend for accessing index partition data
    /// * `reference_resolver` - Optional resolver for handling object references
    ///
    /// # Returns
    ///
    /// A configured reader ready to process field-hashing queries
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The descriptor's index type doesn't match the provided index type
    /// - The underlying hashmap index cannot be opened from the descriptor
    /// - Object storage access fails during initialization
    pub fn open(
        index_type: Arc<dyn IndexType>,
        descriptor: &IndexDescriptor,
        object_store: Arc<dyn ObjectStore>,
        reference_resolver: Option<Arc<dyn ReferenceResolver>>,
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
        let mut options = HashmapIndexOptions::new(object_store);
        if let Some(rr) = reference_resolver {
            options = options.reference_resolver(rr);
        }
        let index = options.open_from_descriptor(descriptor)?;
        let meta = Self::build_metadata_from_descriptor(descriptor);
        Ok(Self {
            index_type,
            meta,
            index,
        })
    }

    /// Constructs index metadata from the provided descriptor.
    ///
    /// Builds metadata that describes the operational characteristics of this index,
    /// including its scope (single or multi-field), supported query types, and
    /// position fidelity guarantees. This metadata is used by query planners
    /// to determine index compatibility and optimization opportunities.
    ///
    /// # Arguments
    ///
    /// * `descriptor` - Index descriptor containing field information and properties
    ///
    /// # Returns
    ///
    /// IndexMetadata configured for hash-based field queries with exact position fidelity
    fn build_metadata_from_descriptor(descriptor: &IndexDescriptor) -> IndexMetadata {
        use ahash::AHashSet;
        let scope = if descriptor.indexed_fields.len() == 1 {
            Scope::ShardField(descriptor.indexed_fields[0].clone())
        } else {
            Scope::MultiShardField(descriptor.indexed_fields.clone())
        };
        let mut query_kind = AHashSet::new();
        query_kind.insert(QueryKind::Hash);
        IndexMetadata {
            scope,
            granularity: Granularity::Record,
            fidelity: PositionFidelity::Exact,
            query_kind,
            bag: descriptor.properties.clone(),
        }
    }

    /// Constructs a hash key for field-value lookups using the same format as indexing.
    ///
    /// This method must produce identical hash values to those generated during index
    /// construction in `FieldHashingIndexBuilder`. The key format is `field.path:value`
    /// where the field path components are joined with dots and separated from the
    /// value by a colon. The entire string is then hashed using xxHash3.
    ///
    /// # Arguments
    ///
    /// * `path` - Field path components (e.g., ["person", "name"])
    /// * `value` - Binary representation of the field value to look up
    ///
    /// # Returns
    ///
    /// 64-bit hash value that can be used for lookups in the underlying hashmap
    ///
    /// # Important
    ///
    /// This key generation logic must remain synchronized with the logic in
    /// `FieldHashingIndexBuilder::process_frame()` to ensure query-time lookups match
    /// the keys that were actually indexed.
    fn build_query_hash(path: &[Arc<str>], value: &Arc<[u8]>) -> u64 {
        let mut buf = path.join(".").into_bytes();
        buf.push(b':');
        buf.extend_from_slice(value);
        xxh3_64(buf.as_slice())
    }
}

impl IndexReader for FieldHashingIndexReader {
    /// Returns a reference to the index type that defines this reader's capabilities.
    ///
    /// The index type provides information about supported data types, query patterns,
    /// and operational scopes. This reference is used by query planners and other
    /// components that need to understand the index's characteristics.
    fn index_type(&self) -> &Arc<dyn IndexType> {
        &self.index_type
    }

    /// Returns metadata describing the operational characteristics of this index.
    ///
    /// The metadata includes scope information (which fields are indexed), query
    /// capabilities (hash-based lookups), granularity (record-level), and position
    /// fidelity (exact positions). This information guides query planning and
    /// optimization decisions.
    fn metadata(&self) -> &IndexMetadata {
        &self.meta
    }

    /// Executes a field-hashing query and returns matching positions.
    ///
    /// Performs hash-based lookup for exact field-value matches. The query must be
    /// of type `FieldHashing` containing the field path, value, and optional position
    /// span to search within. The field path and value are hashed using the same
    /// algorithm used during indexing to ensure consistent lookups.
    ///
    /// # Arguments
    ///
    /// * `query` - Index query specifying the field path, value, and search span
    ///
    /// # Returns
    ///
    /// A `TernaryPositionSet` containing:
    /// - Exact positions if matches are found
    /// - Empty set (all unknown) if no matches exist
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The query is not a `FieldHashing` query type
    /// - The underlying hashmap lookup fails
    /// - Invalid query parameters are provided
    fn query(&self, query: &IndexQuery) -> Result<TernaryPositionSet> {
        let IndexQuery::FieldHashing(query) = query else {
            return Err(Error::invalid_arg(
                "query",
                "FieldHashingIndexReader only supports FieldHashing queries with field path, value, and optional span",
            ));
        };

        let hash = Self::build_query_hash(&query.path, &query.value);

        match self.index.lookup(hash, query.span)? {
            LookupResult::Found { positions } => Ok(TernaryPositionSet::Definite(positions)),
            LookupResult::NotFound => Ok(TernaryPositionSet::make_unknown(query.span)),
        }
    }
}
