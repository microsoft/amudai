//! Building Apache Arrow record batches with strongly-typed field builders.
//!
//! This module provides [`RecordBatchBuilder`], a high-level interface for constructing
//! Apache Arrow [`RecordBatch`](arrow_array::RecordBatch)es in a record-by-record fashion.
//! It builds upon the struct field builder infrastructure to enable efficient construction
//! of tabular data with multiple heterogeneous columns.
//!
//! # Core Concepts
//!
//! ## Record-by-Record Building
//!
//! Unlike array builders that focus on building individual arrays, [`RecordBatchBuilder`]
//! is designed for building complete records (rows) at a time. Each record consists of
//! values across multiple fields (columns), and the builder ensures all fields maintain
//! consistent lengths.
//!
//! ## Field Management
//!
//! The builder leverages the [`StructFieldsBuilder`] trait to manage individual field
//! builders. This provides:
//! - Type-safe access to field builders
//! - Automatic position synchronization across all fields
//! - Consistent schema generation
//!
//! # Usage Pattern
//!
//! The typical workflow for building record batches:
//!
//! 1. **Initialize builder**: Create a [`RecordBatchBuilder`] with appropriate field types
//! 2. **Populate fields**: Set values for the current record across multiple fields
//! 3. **Finish record**: Call [`finish_record()`](RecordBatchBuilder::finish_record) to
//!    complete the current record
//! 4. **Repeat**: Continue adding more records
//! 5. **Build batch**: Call [`build()`](RecordBatchBuilder::build) to create the final
//!    [`RecordBatch`](arrow_array::RecordBatch)
//!
//! # Integration with Struct Builders
//!
//! [`RecordBatchBuilder`] is designed to work seamlessly with the [`struct_builder!`](arrow_builders_macros::struct_builder)
//! macro. The macro generates the necessary [`StructFieldsBuilder`] implementation that
//! provides field accessor methods and manages the underlying field builders.

use std::sync::Arc;

use crate::StructFieldsBuilder;

/// A builder for creating Apache Arrow record batches with strongly-typed fields.
///
/// `RecordBatchBuilder` provides a high-level interface for constructing
/// [`RecordBatch`](arrow_array::RecordBatch)es in a record-by-record fashion. It wraps
/// a [`StructFieldsBuilder`] to manage individual field builders and ensures consistent
/// record construction across all fields.
///
/// The builder maintains the current record position and provides methods to advance
/// to the next record, ensuring all fields are properly synchronized. When building
/// the final record batch, it automatically generates the appropriate Arrow schema
/// based on the field types and names.
///
/// # Type Parameters
///
/// * `S` - A type implementing [`StructFieldsBuilder`] that manages the individual field builders
pub struct RecordBatchBuilder<S: StructFieldsBuilder> {
    fields: S,
}

impl<S: StructFieldsBuilder> RecordBatchBuilder<S> {
    /// Finishes the current record and advances to the next record position.
    ///
    /// This method should be called after all desired fields for the current record
    /// have been populated. It ensures that:
    /// - Any fields not explicitly set for this record will be filled with null values
    /// - All field builders are advanced to the next position consistently
    /// - The record is considered complete and ready for the next record
    pub fn finish_record(&mut self) {
        self.fields.move_to_pos(self.fields.next_pos() + 1);
    }

    /// Returns the number of complete records that have been added to the builder.
    ///
    /// This count reflects the number of times [`finish_record()`](Self::finish_record)
    /// has been called, representing the number of rows that will be in the final
    /// record batch.
    ///
    /// # Returns
    ///
    /// The number of complete records as a `usize`
    pub fn len(&self) -> usize {
        self.fields.next_pos() as usize
    }

    /// Returns `true` if no complete records have been added to the builder.
    ///
    /// This is equivalent to checking if [`len()`](Self::len) returns 0.
    ///
    /// # Returns
    ///
    /// `true` if the builder contains no complete records, `false` otherwise
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Consumes the builder and creates the final Apache Arrow record batch.
    ///
    /// This method performs the complete construction of a [`RecordBatch`](arrow_array::RecordBatch)
    /// by:
    /// 1. Extracting field names from the struct fields builder
    /// 2. Building all field arrays by consuming the individual field builders
    /// 3. Inferring the Arrow schema from the field names and array data types
    /// 4. Constructing the final record batch with the schema and arrays
    ///
    /// All fields in the resulting record batch are marked as nullable (`true`) to
    /// accommodate the null values that may have been automatically inserted for
    /// incomplete records.
    ///
    /// # Returns
    ///
    /// A [`RecordBatch`](arrow_array::RecordBatch) containing all the records that were added
    ///
    /// # Panics
    ///
    /// This method will panic if the internal Arrow record batch construction fails,
    /// which should not happen under normal circumstances with valid builders.
    pub fn build(self) -> arrow_array::RecordBatch {
        let field_names = (0..self.fields.field_count())
            .map(|i| self.fields.field_name(i).to_string())
            .collect::<Vec<_>>();

        let arrays = self.fields.build_fields();

        let fields = field_names
            .into_iter()
            .enumerate()
            .map(|(i, name)| arrow_schema::Field::new(name, arrays[i].data_type().clone(), true))
            .collect::<Vec<_>>();
        let fields = arrow_schema::Fields::from(fields);
        let schema = arrow_schema::Schema::new(fields);
        arrow_array::RecordBatch::try_new(Arc::new(schema), arrays).expect("try_new")
    }
}

/// Provides immutable access to the underlying struct fields builder.
///
/// This implementation allows the `RecordBatchBuilder` to be used as if it were
/// the fields builder directly, enabling direct access to field builder methods
/// without explicitly accessing the `fields` member.
impl<S: StructFieldsBuilder> std::ops::Deref for RecordBatchBuilder<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.fields
    }
}

/// Provides mutable access to the underlying struct fields builder.
///
/// This implementation allows the `RecordBatchBuilder` to be used as if it were
/// the fields builder directly, enabling direct access to mutable field builder
/// methods without explicitly accessing the `fields` member.
impl<S: StructFieldsBuilder> std::ops::DerefMut for RecordBatchBuilder<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.fields
    }
}

/// Creates a new `RecordBatchBuilder` with default field builders.
///
/// The default implementation creates a new instance with:
/// - A default-initialized struct fields builder (type `S`)
/// - All field builders in their initial state (position 0, empty buffers)
impl<S: StructFieldsBuilder> Default for RecordBatchBuilder<S> {
    fn default() -> Self {
        Self {
            fields: Default::default(),
        }
    }
}
