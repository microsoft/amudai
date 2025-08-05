//! Frame implementation for Amudai nested-columnar record batches.
//!
//! A Frame encapsulates a batch of records in columnar format, where each field is represented
//! as a [`Sequence`].
//!
//! ## Schema Projection
//!
//! Frames can optionally include a [`SchemaProjection`] that defines the expected structure
//! and types of the fields. When present, the schema enables:
//! - Type validation during Frame construction
//! - Field name resolution
//! - Metadata propagation (e.g., linking the sequence to the original type node in the full
//!   shard schema)
//!
//! ## Nested Data Support
//!
//! Through the [`Sequence`] abstraction, Frames support complex nested data structures including:
//! - Structs (nested records)
//! - Lists (variable-length arrays)
//! - Maps (key-value pairs)
//! - Fixed-size lists
//!
//! # Performance Considerations
//!
//! - Frames are designed for batch processing, typically containing 1K-64K records
//! - Field sequences are heap-allocated to support dynamic dispatch
//! - Schema validation occurs only during construction, not during data access
//! - Clone operations perform deep copies of all sequences

use std::{ops::Range, sync::Arc};

use amudai_common::{Result, verify_arg};
use amudai_format::projection::SchemaProjection;

use crate::sequence::Sequence;

/// A nested-columnar record batch containing multiple sequences.
///
/// `Frame` is the fundamental data structure for representing batches of records in Amudai's
/// columnar storage format. It consists of a collection of [`Sequence`]s, where each sequence
/// represents a single field across all records in the batch.
///
/// # Structure
///
/// A Frame contains:
/// - **fields**: A vector of boxed [`Sequence`] trait objects, one per field
/// - **len**: The number of logical records (all sequences must have this length)
/// - **schema**: Optional [`SchemaProjection`] for type validation and metadata
///
/// # Guarantees
///
/// The Frame maintains several invariants:
/// 1. All field sequences have exactly the same length
/// 2. If a schema is present, the number of fields matches the schema field count
/// 3. If a schema is present, each field's type matches the corresponding schema type
///
/// # Schema Validation
///
/// When a schema is provided, the Frame validates that the field sequences match the expected
/// types and structure:
///
/// # Memory Layout
///
/// Frames use dynamic dispatch through trait objects, which means:
/// - Each field sequence is heap-allocated
/// - Virtual function calls are used for sequence operations
/// - This allows for heterogeneous field types and nested structures
///
/// # Relationship to Arrow
///
/// While Frames serve a similar purpose to Apache Arrow's RecordBatch, they differ in key ways:
/// - Frames are designed for internal use within Amudai rather than ecosystem-wide
///   interoperability
/// - They use Amudai's [`Sequence`] abstraction instead of Arrow arrays
/// - They support Amudai-specific data types, encodings and optimizations
/// - They integrate with Amudai's schema projection system
pub struct Frame {
    /// Optional schema projection defining the structure and types of fields.
    pub schema: Option<Arc<SchemaProjection>>,
    /// Top-level field sequences. Each sequence must have the same length.
    pub fields: Vec<Box<dyn Sequence>>,
    /// Number of logical records in this frame.
    pub len: usize,
}

impl Frame {
    /// Creates a new frame with schema validation, panicking on failure.
    ///
    /// # Panics
    /// Panics if validation fails (mismatched lengths or schema incompatibility).
    pub fn new(
        schema: Option<Arc<SchemaProjection>>,
        fields: Vec<Box<dyn Sequence>>,
        len: usize,
    ) -> Frame {
        Frame::try_new(schema, fields, len).expect("Frame::try_new")
    }

    /// Creates a new frame with schema validation, returning a Result.
    ///
    /// Validates that:
    /// - All field sequences have the specified length
    /// - Field count matches schema field count (if schema provided)
    /// - Each field's type matches the corresponding schema field type
    pub fn try_new(
        schema: Option<Arc<SchemaProjection>>,
        fields: Vec<Box<dyn Sequence>>,
        len: usize,
    ) -> Result<Frame> {
        verify_arg!(fields, fields.iter().all(|field| field.len() == len));
        if let Some(schema) = schema.as_ref() {
            verify_arg!(schema, schema.fields().len() == fields.len());
            verify_arg!(
                schema,
                schema
                    .fields()
                    .iter()
                    .zip(fields.iter())
                    .all(|(field, seq)| field.type_descriptor() == seq.basic_type())
            );
        }
        Ok(Frame {
            schema,
            fields,
            len,
        })
    }

    /// Returns the number of logical records in this frame.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the frame contains no records.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// Clone implementation for Frame.
///
/// Creates a deep copy by cloning the schema and all field sequences.
impl Clone for Frame {
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            fields: self
                .fields
                .iter()
                .map(|field| field.clone_boxed())
                .collect(),
            len: self.len,
        }
    }
}

/// Describes the position and bounds of a [`Frame`] within a data set.
///
/// A `FrameLocation` captures metadata about where a frame resides in its containing
/// shard and stripe. It includes:
/// - `stripe_ordinal`: the index of the stripe containing the frame
/// - `shard_range`: the range of record positions within the shard
/// - `stripe_range`: the range of record positions within the stripe
///
/// Ranges may be empty when the position information is unknown or not applicable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameLocation {
    /// The ordinal index of the containing stripe within the shard.
    pub stripe_ordinal: usize,

    /// The shard-relative range of record positions covered by this frame.
    ///
    /// This range describes the positions of records within the entire shard.
    /// It will be empty if the information is unknown or not applicable.
    pub shard_range: Range<u64>,

    /// The stripe-relative range of record positions covered by this frame.
    ///
    /// This range describes the positions of records within the stripe.
    /// It will be empty if the information is unknown or not applicable.
    pub stripe_range: Range<u64>,
}
