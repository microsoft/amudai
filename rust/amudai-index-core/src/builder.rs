//! Index builder infrastructure for creating indexes over Amudai data.
//!
//! This module provides the core traits and structures for building various
//! types of indexes that can be used to accelerate queries on Amudai shards.
//! Index builders process data frames and produce index artifacts that can be
//! stored alongside the main data.

use std::sync::Arc;

use ahash::AHashSet;
use amudai_common::Result;
use amudai_format::{defs::common::DataRef, property_bag::PropertyBagBuilder};
use amudai_io::{ReadAt, TemporaryFileStore};
use amudai_objectstore::{ObjectStore, url::ObjectUrl};
use amudai_sequence::frame::{Frame, FrameLocation};

use crate::{IndexType, metadata, shard_markers::DynPreparedShard};

/// Trait for building indexes over Amudai data.
///
/// Implementers of this trait define how to construct a specific type of index
/// by processing data frames and producing index artifacts.
pub trait IndexBuilder {
    /// Returns the type of index being built.
    ///
    /// This identifies the index implementation that was used to create this
    /// `IndexBuilder`.
    fn index_type(&self) -> &Arc<dyn IndexType>;

    /// Processes a single data frame and updates the index state.
    ///
    /// This method is called for each frame in the data being indexed.
    /// The builder should extract relevant information from the frame
    /// and update its internal state accordingly.
    ///
    /// # Arguments
    /// * `frame` - The data frame to process
    /// * `location` - The location information for the frame within the larger dataset
    ///
    /// # Errors
    /// Returns an error if the frame cannot be processed or if the index
    /// state cannot be updated.
    fn process_frame(&mut self, frame: &Frame, location: &FrameLocation) -> Result<()>;

    /// Finalizes the index construction and produces the index artifacts.
    ///
    /// This method is called after all frames have been processed. It should
    /// finalize any internal state and produce the final index artifacts that
    /// can be stored and used for query acceleration.
    ///
    /// The method consumes `self` to ensure the builder cannot be used after sealing.
    ///
    /// # Errors
    /// Returns an error if the index cannot be finalized or if artifacts
    /// cannot be created.
    fn seal(self: Box<Self>) -> Result<PreparedIndex>;
}

/// Index builder parameters that control index construction.
///
/// These parameters are provided to index builders to configure how the index
/// is built, where artifacts are stored, and what capabilities the index should have.
pub struct Parameters {
    /// Temp store to use for storing the transient index artifacts.
    ///
    /// Index builders can use this to create temporary files during construction
    /// that will be cleaned up automatically.
    pub temp_store: Arc<dyn TemporaryFileStore>,

    /// Optional object store, if the index builder decides to put the final artifacts
    /// directly into the object store (not recommended).
    ///
    /// When provided, builders can write artifacts directly to object storage
    /// rather than producing in-memory buffers.
    pub object_store: Option<Arc<dyn ObjectStore>>,

    /// Full URL of the container (virtual folder) in the object store where all index
    /// artifacts should be placed.
    ///
    /// This is typically used in conjunction with `object_store` to specify
    /// the exact location for index artifacts.
    pub container: Option<ObjectUrl>,

    /// Specifies which fields are indexed and at what level (stripe or shard).
    ///
    /// The scope determines whether the index covers all fields or specific ones,
    /// and whether indexing happens at the stripe or shard level.
    pub scope: metadata::Scope,

    /// The granularity level used by this index.
    ///
    /// Indicates whether positions are tracked at the Element, List, or Record level.
    /// This affects the precision of query results and the size of the index.
    pub granularity: metadata::Granularity,

    /// The fidelity of positions returned by this index.
    ///
    /// Specifies whether search results are Exact, Approximate, or Mixed.
    /// Approximate results may include false positives that need to be filtered
    /// during query execution.
    pub fidelity: metadata::PositionFidelity,

    /// Set of supported query types.
    ///
    /// Specifies which [`QueryKind`] operations (e.g., Term, TermPrefix, NumericRange)
    /// the index will be able to handle. Builders should only construct index
    /// structures necessary for the specified query types.
    pub query_kind: AHashSet<metadata::QueryKind>,

    /// Additional properties of the index.
    ///
    /// Custom key-value pairs that can be used to pass index-specific
    /// configuration or store metadata about the index.
    pub properties: PropertyBagBuilder,
}

/// The result of building an index, containing all generated artifacts.
///
/// This structure is returned by [`IndexBuilder::seal`] and contains all the
/// information needed to store and use the index.
pub struct PreparedIndex {
    /// The type identifier of the index.
    ///
    /// This should match the value returned by [`IndexType::name`] for the
    /// index type that created this prepared index.
    pub index_type: String,

    /// The collection of artifacts produced by the index builder.
    ///
    /// Each artifact represents a distinct piece of the index that needs
    /// to be stored. For example, a text index might produce separate artifacts
    /// for the terms B-tree and position data.
    pub artifacts: Vec<PreparedArtifact>,

    /// The scope of fields covered by this index.
    ///
    /// This should match or be a subset of the scope provided in the
    /// builder parameters.
    pub scope: metadata::Scope,

    /// Additional properties of the prepared index.
    ///
    /// Can include metadata about the index construction, statistics,
    /// or index-specific configuration.
    pub properties: PropertyBagBuilder,

    /// Optional total size of all artifacts in bytes.
    ///
    /// When provided, this gives the total storage footprint of the index.
    pub size: Option<u64>,
}

/// A single artifact produced by an index builder.
///
/// Artifacts are the individual components of an index that need to be stored.
/// Each artifact has a name, optional properties, and the actual data.
pub struct PreparedArtifact {
    /// The name of the artifact.
    ///
    /// This name is used to identify the artifact within the index and should
    /// be unique within the context of a single index. Common names might include
    /// "dictionary", "postings", "positions", etc.
    pub name: String,

    /// Additional properties specific to this artifact.
    ///
    /// Can include metadata about the artifact such as its format version,
    /// compression settings, or statistics.
    pub properties: PropertyBagBuilder,

    /// The actual data of the artifact.
    ///
    /// The data can be provided in several forms depending on how the
    /// index builder chooses to produce it.
    pub data: ArtifactData,
}

/// The different forms of data that can be contained in an index artifact.
///
/// This enum provides flexibility in how index builders produce their artifacts,
/// allowing for in-memory buffers, prepared shards, or custom data references.
pub enum ArtifactData {
    /// An in-memory buffer containing the artifact data.
    ///
    /// This is the most common form, where the index builder produces
    /// the artifact data as a byte buffer that implements [`ReadAt`].
    Buffer(Arc<dyn ReadAt>),

    /// A prepared shard that can be written directly to storage.
    ///
    /// This allows index builders to produce artifacts in the same format
    /// as regular Amudai shards, enabling them to be read using the standard
    /// shard reading infrastructure.
    PreparedShard(Box<dyn DynPreparedShard>),

    /// A custom data reference.
    ///
    /// This variant allows for index-specific data formats that don't fit
    /// into the standard buffer or shard models. The [`DataRef`] can point
    /// to external data or contain inline data.
    Custom(DataRef),
}
