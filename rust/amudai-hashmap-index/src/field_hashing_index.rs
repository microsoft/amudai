use ahash::AHashSet;
use std::sync::{Arc, Weak};

use amudai_common::Result;
use amudai_common::error::Error;
use amudai_format::defs::shard::IndexDescriptor;
use amudai_format::schema::BasicType;
use amudai_index_core::IndexMaintainer;
use amudai_index_core::builder::Parameters;
use amudai_index_core::metadata::{
    Capabilities, Granularity, PositionFidelity, QueryKind, ScopeKind,
};
use amudai_index_core::reader::IndexReader;
use amudai_index_core::shard_markers::DynShard;
use amudai_index_core::{IndexBuilder, IndexType};
use amudai_shard::read::shard::Shard;

use crate::read::field_hashing_index_reader::FieldHashingIndexReader;
use crate::write::field_hashing_index_builder::FieldHashingIndexBuilder;

/// Index type descriptor that defines capabilities and behavior for field hashing indexes.
///
/// Declares support for hash-based queries on structured field data with exact position
/// fidelity. The capabilities enumeration drives query planner decisions and validation
/// of indexing operations, ensuring compatibility between query requirements and index
/// capabilities.
///
/// This type focuses on exact-match lookups for field-value combinations rather than
/// range or fuzzy matching, optimizing for high-throughput equality filters.
pub struct FieldHashingIndexType {
    /// Weak self-reference to enable Arc creation with cyclic references
    this: Weak<FieldHashingIndexType>,
    /// Defines the data types, query kinds, and operational scopes supported by this index
    capabilities: Capabilities,
}

impl FieldHashingIndexType {
    /// The canonical name identifier for this index type
    pub const NAME: &'static str = "field-hashing-index";

    /// Creates a new FieldHashingIndexType instance with appropriate capabilities.
    ///
    /// Uses Arc::new_cyclic to handle the self-referential weak pointer needed
    /// for the IndexType trait implementation. The weak reference enables
    /// the index type to create builders that reference back to the type.
    ///
    /// # Returns
    ///
    /// Arc-wrapped FieldHashingIndexType instance ready for use
    pub fn new() -> Arc<FieldHashingIndexType> {
        Arc::new_cyclic(|this| FieldHashingIndexType {
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
    fn shared_from_self(&self) -> Arc<FieldHashingIndexType> {
        self.this.upgrade().expect("upgrade")
    }

    /// Constructs the capability matrix defining what this index type supports.
    ///
    /// Enumerates supported data types (primarily primitive types suitable for hashing),
    /// query patterns (hash-based equality), and operational scopes (single/multi-field).
    /// The capability declaration drives compatibility checks during index construction
    /// and query planning phases.
    ///
    /// Excludes floating-point types due to precision issues with hash-based equality,
    /// and complex nested types that require specialized handling beyond simple hashing.
    ///
    /// # Returns
    ///
    /// A Capabilities structure defining all supported operations and data types
    fn build_capabilities() -> amudai_index_core::metadata::Capabilities {
        let mut scope = AHashSet::new();
        scope.insert(ScopeKind::ShardField);
        scope.insert(ScopeKind::MultiShardField);

        let mut granularity = AHashSet::new();
        granularity.insert(Granularity::Record);

        let mut fidelity = AHashSet::new();
        fidelity.insert(PositionFidelity::Exact);

        let mut data_types = AHashSet::new();
        data_types.insert(BasicType::String);
        data_types.insert(BasicType::Boolean);
        data_types.insert(BasicType::Int8);
        data_types.insert(BasicType::Int16);
        data_types.insert(BasicType::Int32);
        data_types.insert(BasicType::Int64);
        data_types.insert(BasicType::Guid);
        data_types.insert(BasicType::Binary);
        data_types.insert(BasicType::FixedSizeBinary);
        data_types.insert(BasicType::DateTime);

        let mut query_kind = AHashSet::new();
        query_kind.insert(QueryKind::Hash);

        Capabilities {
            scope,
            granularity,
            fidelity,
            data_types,
            query_kind,
            value_size: 0..usize::MAX,
        }
    }
}

impl IndexType for FieldHashingIndexType {
    /// Returns the canonical name for this index type.
    ///
    /// This name is used for registration, serialization, and identification
    /// of the index type across the system.
    fn name(&self) -> &str {
        Self::NAME
    }

    /// Returns the capabilities supported by this index type.
    ///
    /// The capabilities define what data types, query patterns, and operational
    /// scopes this index can handle. This information is used during query
    /// planning and index construction validation.
    fn capabilities(&self) -> &amudai_index_core::metadata::Capabilities {
        &self.capabilities
    }

    /// Creates a new index builder instance for constructing indexes of this type.
    ///
    /// The builder will be configured with the provided parameters and will be
    /// able to process data frames to build hash-based field indexes.
    ///
    /// # Arguments
    ///
    /// * `parameters` - Configuration including object stores, scope, and properties
    ///
    /// # Returns
    ///
    /// A boxed IndexBuilder ready to process data frames
    ///
    /// # Errors
    ///
    /// Returns an error if required parameters (like object store) are missing
    fn create_builder(&self, parameters: Parameters) -> Result<Box<dyn IndexBuilder>> {
        Ok(Box::new(FieldHashingIndexBuilder::new(
            parameters,
            self.shared_from_self(),
        )?))
    }

    /// Creates an index reader for querying previously built indexes.
    ///
    /// Opens a reader that can perform hash-based lookups against field-value
    /// combinations using the stored index partitions. The reader requires
    /// access to object storage to load index data on demand.
    ///
    /// # Arguments
    ///
    /// * `descriptor` - Metadata describing the index structure and location
    /// * `shard` - Optional shard context providing object store access
    ///
    /// # Returns
    ///
    /// An IndexReader capable of performing field hash lookups
    ///
    /// # Errors
    ///
    /// Returns an error if the shard cannot provide the required object store
    fn open_reader(
        &self,
        descriptor: &IndexDescriptor,
        shard: Option<Box<dyn DynShard>>,
    ) -> Result<Arc<dyn IndexReader>> {
        // Try to reuse the object store and reference resolver from the provided shard when available,
        // otherwise we expect readers to initialize their own from context.
        let (object_store, reference_resolver) = if let Some(shard) = shard {
            let shard = Shard::try_from(shard).map_err(|_| {
                Error::invalid_operation("fields reader: unexpected shard type".to_string())
            })?;
            (
                Arc::clone(shard.object_store()),
                Some(Arc::clone(shard.reference_resolver())),
            )
        } else {
            // Without a shard we can't infer the object store; return a clear error.
            return Err(Error::invalid_operation(
                "fields reader requires shard to supply object store".to_string(),
            ));
        };

        let reader = FieldHashingIndexReader::open(
            self.shared_from_self(),
            descriptor,
            object_store,
            reference_resolver,
        )?;
        Ok(Arc::new(reader))
    }

    /// Creates an index maintainer for this index type.
    ///
    /// Currently returns a no-op maintainer since field hashing indexes
    /// do not require ongoing maintenance operations like compaction
    /// or reorganization. Future versions may add maintenance capabilities.
    ///
    /// # Returns
    ///
    /// A maintainer that performs no operations
    fn create_maintainer(&self) -> Result<Box<dyn IndexMaintainer>> {
        struct NoopMaintainer;
        impl IndexMaintainer for NoopMaintainer {}
        Ok(Box::new(NoopMaintainer))
    }
}
