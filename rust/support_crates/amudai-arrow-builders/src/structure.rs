//! Building Apache Arrow struct arrays with strongly-typed field builders.
//!
//! This module provides the infrastructure for building Apache Arrow [`StructArray`](arrow_array::StructArray)s
//! through a strongly-typed, position-aware interface. It enables the construction of complex
//! nested structures with multiple heterogeneous fields while maintaining type safety and
//! automatic null handling.
//!
//! # Core Concepts
//!
//! ## StructFieldsBuilder Trait
//!
//! The [`StructFieldsBuilder`] trait defines the interface for managing field builders within
//! a struct array. Implementors are responsible for:
//! - Tracking the current position across all fields
//! - Providing access to individual field builders
//! - Synchronizing field builders to maintain consistent array lengths
//! - Building the final field arrays
//!
//! ## StructBuilder
//!
//! [`StructBuilder<FB>`] is a generic wrapper that combines:
//! - A fields builder (`FB`) implementing [`StructFieldsBuilder`]
//! - A null buffer for tracking which struct elements are null
//!
//! # Building Process
//!
//! The typical workflow for building struct arrays:
//!
//! 1. **Initialize builders**: Create a struct builder with appropriate field builders
//! 2. **Set field values**: Use field accessor methods to set values for the current struct element
//! 3. **Finalize elements**: Call [`finish_struct()`](StructBuilder::finish_struct) or
//!    [`finish_null_struct()`](StructBuilder::finish_null_struct)
//! 4. **Repeat**: Continue for additional struct elements
//! 5. **Build array**: Call [`build()`](crate::ArrayBuilder::build) to create the final
//!    [`StructArray`](arrow_array::StructArray)
//!
//! # Usage with Macros
//!
//! While this module can be used directly, the most convenient way to work with struct builders
//! is through the [`struct_builder!`](amudai_arrow_builders_macros::struct_builder) macro, which
//! automatically generates the necessary [`StructFieldsBuilder`] implementation.

use std::sync::Arc;

use arrow_array::Array;

use crate::ArrayBuilder;

/// A trait for building the fields of a struct array.
///
/// This trait provides the interface for managing field builders within a struct array builder.
/// Implementors are responsible for tracking the current position, managing individual field builders,
/// and constructing the final field arrays.
///
/// The trait requires that implementors are `Default`, `Send`, `Sync`, and have a `'static` lifetime
/// to ensure they can be safely used in multi-threaded contexts and stored in various data structures.
///
/// This trait should rarely be implemented manually. The most convenient way to work with struct builders
/// is using the [`struct_builder!`](amudai_arrow_builders_macros::struct_builder) macro.
pub trait StructFieldsBuilder: Default + Send + Sync + 'static {
    /// Returns the current logical position in the struct array being built.
    ///
    /// This position represents the index of the next struct element to be added.
    /// All field builders should be synchronized to this position before adding values.
    fn next_pos(&self) -> u64;

    /// Moves the builder to the specified logical position.
    ///
    /// This method updates the internal position tracking and ensures that all field builders
    /// are prepared to accept values at the given position. This is typically called when
    /// the struct builder needs to advance to a new position.
    ///
    /// # Arguments
    ///
    /// * `pos` - The target position to move to
    fn move_to_pos(&mut self, pos: u64);

    /// Returns the number of fields in the struct.
    ///
    /// This should return a constant value that represents the total number of fields
    /// that will be present in the final struct array.
    fn field_count(&self) -> usize;

    /// Returns the name of the field at the specified index.
    ///
    /// # Arguments
    ///
    /// * `index` - The zero-based index of the field
    ///
    /// # Returns
    ///
    /// The name of the field as a string slice
    ///
    /// # Panics
    ///
    /// May panic if the index is out of bounds (>= `field_count()`)
    fn field_name(&self, index: usize) -> &str;

    /// Consumes the builder and returns the built field arrays.
    ///
    /// This method finalizes all field builders and returns a vector of arrays
    /// that will be used to construct the final struct array. The order of arrays
    /// in the returned vector should correspond to the field indices used in
    /// `field_name()`.
    ///
    /// # Returns
    ///
    /// A vector of arrays, one for each field in the struct
    fn build_fields(self) -> Vec<Arc<dyn Array>>;
}

/// A builder for creating Apache Arrow struct arrays.
///
/// `StructBuilder` provides a generic way to build struct arrays by managing
/// both the field builders (through the `StructFieldsBuilder` trait) and the
/// null buffer for the struct elements themselves.
///
/// The builder maintains two key pieces of state:
/// - A fields builder (`FB`) that manages individual field builders
/// - A null buffer that tracks which struct elements are null vs non-null
///
/// # Type Parameters
///
/// * `FB` - A type implementing `StructFieldsBuilder` that manages the individual field builders
pub struct StructBuilder<FB: StructFieldsBuilder> {
    fields: FB,
    nulls: arrow_buffer::NullBufferBuilder,
}

impl<FB: StructFieldsBuilder> StructBuilder<FB> {
    /// Finishes the current struct element as a non-null value.
    ///
    /// This method should be called after all fields for the current struct element
    /// have been populated. It ensures that any missing field values are filled with
    /// nulls, marks the struct element as non-null, and advances the position for
    /// the next struct element.
    pub fn finish_struct(&mut self) {
        self.fill_missing();
        self.nulls.append_non_null();
        self.fields.move_to_pos(self.next_pos() + 1);
    }

    /// Finishes the current struct element as a null value.
    ///
    /// This method adds a null struct element to the array. Even though the struct
    /// itself is null, any missing field values are still filled to maintain
    /// consistent array lengths across all fields.
    pub fn finish_null_struct(&mut self) {
        self.fill_missing();
        self.nulls.append_null();
        self.fields.move_to_pos(self.next_pos() + 1);
    }

    /// Returns the current position in the null buffer.
    #[inline]
    fn inner_pos(&self) -> u64 {
        self.nulls.len() as u64
    }

    /// Fills any gap between the null buffer position and the target position with nulls.
    ///
    /// This ensures that the null buffer is synchronized with the field builders' positions.
    /// If the null buffer is behind the target position, null values are appended to catch up.
    ///
    /// # Panics
    ///
    /// Panics if the null buffer position is ahead of the target position, which would
    /// indicate an inconsistent internal state.
    #[inline]
    fn fill_missing(&mut self) {
        if self.inner_pos() < self.next_pos() {
            let null_count = self.next_pos() - self.inner_pos();
            self.nulls.append_n_nulls(null_count as usize);
        }
        assert_eq!(self.inner_pos(), self.next_pos());
    }
}

/// Provides immutable access to the underlying fields builder.
///
/// This allows the `StructBuilder` to be used as if it were the fields builder directly,
/// enabling direct access to field builder methods.
impl<FB: StructFieldsBuilder> std::ops::Deref for StructBuilder<FB> {
    type Target = FB;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.fields
    }
}

/// Provides mutable access to the underlying fields builder.
///
/// This allows the `StructBuilder` to be used as if it were the fields builder directly,
/// enabling direct access to mutable field builder methods.
impl<FB: StructFieldsBuilder> std::ops::DerefMut for StructBuilder<FB> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.fields
    }
}

/// Creates a new `StructBuilder` with default field builders and null buffer.
impl<FB: StructFieldsBuilder> Default for StructBuilder<FB> {
    fn default() -> Self {
        StructBuilder {
            fields: FB::default(),
            nulls: arrow_buffer::NullBufferBuilder::new(1024),
        }
    }
}

/// Implementation of `ArrayBuilder` for `StructBuilder`.
///
/// This implementation delegates position management to the underlying fields builder
/// and provides the logic to construct the final Apache Arrow `StructArray`.
impl<FB: StructFieldsBuilder> ArrayBuilder for StructBuilder<FB> {
    /// Returns the next logical position from the fields builder.
    #[inline]
    fn next_pos(&self) -> u64 {
        self.fields.next_pos()
    }

    /// Moves the fields builder to the specified position.
    #[inline]
    fn move_to_pos(&mut self, pos: u64) {
        self.fields.move_to_pos(pos);
    }

    /// Builds and returns the final struct array.
    ///
    /// This method:
    /// 1. Ensures any missing null values are filled
    /// 2. Collects field names from the fields builder
    /// 3. Builds all field arrays
    /// 4. Finalizes the null buffer
    /// 5. Creates the appropriate Arrow schema fields
    /// 6. Constructs and returns the final `StructArray`
    ///
    /// # Returns
    ///
    /// An `Arc<dyn Array>` containing the built struct array
    fn build(mut self) -> Arc<dyn Array> {
        self.fill_missing();

        let field_names = (0..self.fields.field_count())
            .map(|i| self.fields.field_name(i).to_string())
            .collect::<Vec<_>>();

        let arrays = self.fields.build_fields();
        let nulls = self.nulls.finish();

        let fields = field_names
            .into_iter()
            .enumerate()
            .map(|(i, name)| arrow_schema::Field::new(name, arrays[i].data_type().clone(), true))
            .collect::<Vec<_>>();
        let fields = arrow_schema::Fields::from(fields);

        Arc::new(arrow_array::StructArray::new(fields, arrays, nulls))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::Array;

    use crate::{
        ArrayBuilder,
        binary::{BinaryBuilder, FixedSizeBinaryBuilder},
        list::ListBuilder,
        primitive::Int64Builder,
        string::StringBuilder,
        structure::{StructBuilder, StructFieldsBuilder},
    };
    /// Tests the basic functionality of `StructBuilder` with multiple field types.
    ///
    /// This test creates a struct builder with fields for name (string), value (int64),
    /// blob (binary), and measures (list of fixed-size binary). It demonstrates:
    /// - Adding complete struct elements with multiple fields
    /// - Adding struct elements with only some fields populated (nulls for missing fields)
    /// - Working with nested list fields
    /// - Building the final array
    #[test]
    fn test_struct_builder() {
        let mut builder = FooBuilder::default();

        builder.name_field().set("aaa");
        builder.value_field().set(10);
        builder.finish_struct();

        builder.name_field().set("bbb");

        {
            let measures = builder.measures_field();
            measures.item().set(b"111");
            measures.item().set(b"222");
            measures.finish_list();
        }
        builder.finish_struct();

        builder.name_field().set("ccc");
        builder.value_field().set(20);
        builder.blob_field().set(b"1234abc");
        builder.finish_struct();

        let arr = builder.build();
        dbg!(arr);
    }

    /// A concrete implementation of `StructFieldsBuilder` for demonstration and testing.
    ///
    /// This struct contains four different field types:
    /// - `name`: A string field
    /// - `value`: A 64-bit integer field  
    /// - `blob`: A variable-length binary field
    /// - `measures`: A list field containing fixed-size (3-byte) binary values
    /// - `next_pos`: Tracks the current position in the struct array
    ///
    /// This example shows how to implement the `StructFieldsBuilder` trait for a
    /// struct with multiple heterogeneous field types including nested collections.
    pub struct FooFields {
        name: StringBuilder,
        value: Int64Builder,
        blob: BinaryBuilder,
        measures: ListBuilder<FixedSizeBinaryBuilder<3>>,
        next_pos: u64,
    }
    impl FooFields {
        /// Returns a mutable reference to the name field builder.
        ///
        /// This method synchronizes the name field builder to the current position
        /// before returning the reference, ensuring that subsequent operations
        /// will write to the correct position in the array.
        ///
        /// # Returns
        ///
        /// A mutable reference to the `StringBuilder` for the name field
        pub fn name_field(&mut self) -> &mut StringBuilder {
            self.name.move_to_pos(self.next_pos);
            &mut self.name
        }

        /// Returns a mutable reference to the value field builder.
        ///
        /// This method synchronizes the value field builder to the current position
        /// before returning the reference, ensuring that subsequent operations
        /// will write to the correct position in the array.
        ///
        /// # Returns
        ///
        /// A mutable reference to the `Int64Builder` for the value field
        pub fn value_field(&mut self) -> &mut Int64Builder {
            self.value.move_to_pos(self.next_pos);
            &mut self.value
        }

        /// Returns a mutable reference to the blob field builder.
        ///
        /// This method synchronizes the blob field builder to the current position
        /// before returning the reference, ensuring that subsequent operations
        /// will write to the correct position in the array.
        ///
        /// # Returns
        ///
        /// A mutable reference to the `BinaryBuilder` for the blob field
        pub fn blob_field(&mut self) -> &mut BinaryBuilder {
            self.blob.move_to_pos(self.next_pos);
            &mut self.blob
        }

        /// Returns a mutable reference to the measures field builder.
        ///
        /// This method synchronizes the measures field builder to the current position
        /// before returning the reference, ensuring that subsequent operations
        /// will write to the correct position in the array.
        ///
        /// The measures field is a list of fixed-size binary values, where each binary
        /// value is exactly 3 bytes long.
        ///
        /// # Returns
        ///
        /// A mutable reference to the `ListBuilder<FixedSizeBinaryBuilder<3>>` for the measures field
        pub fn measures_field(&mut self) -> &mut ListBuilder<FixedSizeBinaryBuilder<3>> {
            self.measures.move_to_pos(self.next_pos);
            &mut self.measures
        }
    }

    /// Default implementation for `FooFields`.
    ///
    /// Creates a new `FooFields` instance with all field builders initialized to their
    /// default values and the position set to 0.
    impl Default for FooFields {
        fn default() -> Self {
            FooFields {
                name: Default::default(),
                value: Default::default(),
                blob: Default::default(),
                measures: Default::default(),
                next_pos: 0,
            }
        }
    }

    /// Implementation of `StructFieldsBuilder` for `FooFields`.
    ///
    /// This implementation provides the necessary methods to manage the position tracking
    /// and field array building for the test struct with four different field types.
    impl StructFieldsBuilder for FooFields {
        #[inline]
        fn next_pos(&self) -> u64 {
            self.next_pos
        }

        #[inline]
        fn move_to_pos(&mut self, pos: u64) {
            self.next_pos = pos;
        }

        #[inline]
        fn field_count(&self) -> usize {
            4
        }

        fn field_name(&self, index: usize) -> &str {
            const NAMES: &'static [&'static str] = &["name", "value", "blob", "measures"];
            NAMES[index]
        }

        fn build_fields(mut self) -> Vec<Arc<dyn Array>> {
            self.name.move_to_pos(self.next_pos);
            self.value.move_to_pos(self.next_pos);
            self.blob.move_to_pos(self.next_pos);
            self.measures.move_to_pos(self.next_pos);
            vec![
                self.name.build(),
                self.value.build(),
                self.blob.build(),
                self.measures.build(),
            ]
        }
    }

    /// Type alias for a `StructBuilder` using `FooFields` as the fields builder.
    ///
    /// This provides a convenient way to refer to the specific struct builder type
    /// used in tests and examples. It demonstrates how to create a type alias for
    /// a specific instantiation of the generic `StructBuilder`.
    pub type FooBuilder = StructBuilder<FooFields>;
}
