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

use ahash::AHashMap;
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
    /// Returns the schema fields that define the structure of the struct array.
    ///
    /// This method provides the Apache Arrow schema definition for all fields that will be
    /// present in the final struct array. The schema includes field names, data types, and
    /// nullability information for each field in the struct.
    ///
    /// The returned [`arrow_schema::Fields`] contains the complete field definitions in the
    /// same order that field arrays will be returned by [`build_fields()`](Self::build_fields).
    /// This schema information is used when constructing the final [`StructArray`](arrow_array::StructArray)
    /// to ensure proper type information and metadata.
    ///
    /// # Returns
    ///
    /// An [`arrow_schema::Fields`] collection containing the field definitions for all
    /// fields in the struct, including their names, data types, and nullability constraints.
    fn schema(&self) -> arrow_schema::Fields;

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
    fn build_fields(&mut self) -> Vec<Arc<dyn Array>>;
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
    fn data_type(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Struct(self.fields.schema())
    }

    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }

    fn as_any_mut(&mut self) -> &mut (dyn std::any::Any + 'static) {
        self
    }

    fn try_push_raw_value(&mut self, value: Option<&[u8]>) -> Result<(), arrow_schema::ArrowError> {
        if value.is_some() {
            self.finish_struct();
        } else {
            self.finish_null_struct();
        }
        Ok(())
    }

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
    fn build(&mut self) -> Arc<dyn Array> {
        self.fill_missing();

        let nulls = self.nulls.finish();
        if self.field_count() == 0 {
            let len = self.next_pos() as usize;
            Arc::new(arrow_array::StructArray::new_empty_fields(len, nulls))
        } else {
            let fields = self.fields.schema();
            let arrays = self.fields.build_fields();
            Arc::new(arrow_array::StructArray::new(fields, arrays, nulls))
        }
    }
}

/// A dynamic struct fields builder that allows registering fields at runtime.
///
/// `FluidStructFields` provides a flexible implementation of [`StructFieldsBuilder`] that
/// allows fields to be registered dynamically during execution, rather than being fixed
/// at compile time. This is useful when the structure of the data isn't known until runtime
/// or when building generic data processing pipelines.
///
/// Unlike statically-defined struct builders (like those generated by macros),
/// `FluidStructFields` maintains a registry of field names mapped to their builders,
/// allowing new fields to be added on-demand as they are encountered in the data.
///
/// # Fields
///
/// The struct maintains three key pieces of state:
/// - A vector of field builders paired with their names
/// - A hash map for fast field name-to-index lookups
/// - The current logical position in the struct array being built
///
/// # Example Usage
///
/// ```rust
/// use amudai_arrow_builders::structure::{FluidStructFields, StructBuilder};
/// use amudai_arrow_builders::{ArrayBuilder, ArrayBuilderExt};
/// use arrow_schema::DataType;
///
/// let mut builder = StructBuilder::<FluidStructFields>::default();
///
/// // Register fields dynamically
/// builder.register_field("name", &DataType::Utf8);
/// builder.register_field("age", &DataType::Int32);
///
/// // Access and populate fields
/// builder.field("name").push_raw_value(Some(b"Alice"));
/// builder.field("age").push_raw_value(Some(&42i32.to_le_bytes()));
/// builder.finish_struct();
///
/// let array = builder.build();
/// ```
pub struct FluidStructFields {
    /// Vector of field builders paired with their names.
    ///
    /// Each element contains a boxed array builder for the field's data type
    /// and the field's name as a string. The index in this vector serves as
    /// the field's identifier for efficient access.
    fields: Vec<(Box<dyn ArrayBuilder>, String)>,

    /// Hash map providing fast field name-to-index lookups.
    ///
    /// Maps field names to their corresponding indices in the `fields` vector,
    /// enabling O(1) field access by name rather than requiring linear searches.
    field_map: AHashMap<String, usize>,

    /// The current logical position in the struct array being built.
    ///
    /// This tracks the index of the next struct element to be added and is used
    /// to synchronize all field builders to the same position before accessing them.
    next_pos: u64,
}

impl FluidStructFields {
    /// Registers a new field with the specified name and data type.
    ///
    /// This method creates an appropriate array builder for the given data type and
    /// registers it with the specified field name. If a field with the same name
    /// already exists, the registration is ignored to prevent duplicate fields.
    ///
    /// The data type is used to create the most appropriate builder type via
    /// [`crate::create_builder`], which handles the mapping from Arrow data types
    /// to their corresponding builder implementations.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the field to register
    /// * `data_type` - The Arrow data type for the field
    pub fn register_field(&mut self, name: &str, data_type: &arrow_schema::DataType) {
        self.register_field_builder(name, crate::create_builder(data_type));
    }

    /// Registers a new field with a pre-created array builder.
    ///
    /// This method allows for more fine-grained control over field registration by
    /// accepting a pre-configured array builder instead of just a data type. This is
    /// useful when you need specific builder configurations or when working with
    /// custom builder implementations.
    ///
    /// If a field with the same name already exists, the registration is ignored
    /// to prevent duplicate fields and maintain consistency.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the field to register
    /// * `builder` - A boxed array builder for the field
    pub fn register_field_builder(&mut self, name: &str, builder: Box<dyn ArrayBuilder>) {
        if self.field_map.contains_key(name) {
            return;
        }
        let index = self.fields.len();
        self.field_map.insert(name.to_string(), index);
        self.fields.push((builder, name.to_string()));
    }

    /// Checks whether a field with the specified name has been registered.
    ///
    /// This method performs a fast lookup to determine if a field with the given name
    /// exists in the current set of registered fields. It's useful for conditional
    /// field access or validation before attempting to access a field.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the field to check for
    ///
    /// # Returns
    ///
    /// `true` if a field with the specified name exists, `false` otherwise
    pub fn contains_field(&self, name: &str) -> bool {
        self.field_map.contains_key(name)
    }

    /// Returns a mutable reference to the array builder for the specified field.
    ///
    /// This method provides access to the array builder associated with the given
    /// field name. Before returning the builder, it synchronizes the builder to the
    /// current position to ensure that subsequent operations write to the correct
    /// array index.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the field to access (can be any type that converts to `&str`)
    ///
    /// # Returns
    ///
    /// A mutable reference to the field's array builder
    ///
    /// # Panics
    ///
    /// Panics if the field name is not found. Fields must be registered before
    /// they can be accessed.
    pub fn field(&mut self, name: impl AsRef<str>) -> &mut dyn ArrayBuilder {
        let index = *self.field_map.get(name.as_ref()).expect("field");
        self.field_at(index)
    }

    /// Returns a mutable reference to the array builder for the specified field, if it exists.
    ///
    /// This method is similar to [`field`](Self::field) but returns `None` instead of panicking
    /// when the field name is not found. This allows for safe field access when you're not certain
    /// whether a field has been registered.
    ///
    /// Before returning the builder, it synchronizes the builder to the current position to ensure
    /// that subsequent operations write to the correct array index.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the field to access (can be any type that converts to `&str`)
    ///
    /// # Returns
    ///
    /// `Some(&mut dyn ArrayBuilder)` if the field exists, `None` otherwise
    pub fn try_field(&mut self, name: impl AsRef<str>) -> Option<&mut dyn ArrayBuilder> {
        self.field_map
            .get(name.as_ref())
            .cloned()
            .map(|i| self.field_at(i))
    }

    /// Returns a mutable reference to the array builder at the specified index.
    ///
    /// This method provides direct access to a field builder by its numeric index
    /// rather than by name. This can be more efficient when the index is already
    /// known, as it avoids the hash map lookup.
    ///
    /// Before returning the builder, it synchronizes the builder to the current
    /// position to ensure that subsequent operations write to the correct array index.
    ///
    /// # Arguments
    ///
    /// * `index` - The zero-based index of the field to access
    ///
    /// # Returns
    ///
    /// A mutable reference to the field's array builder
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds (>= the number of registered fields).
    pub fn field_at(&mut self, index: usize) -> &mut dyn ArrayBuilder {
        let field = &mut self.fields[index];
        field.0.move_to_pos(self.next_pos);
        field.0.as_mut()
    }
}

/// Implementation of [`StructFieldsBuilder`] for [`FluidStructFields`].
///
/// This implementation provides the standard struct builder interface for the dynamic
/// field registration system. It handles position tracking, field counting, field naming,
/// and the final array building process.
impl StructFieldsBuilder for FluidStructFields {
    fn schema(&self) -> arrow_schema::Fields {
        let fields = self
            .fields
            .iter()
            .map(|(builder, name)| {
                arrow_schema::Field::new(name.clone(), builder.data_type(), true)
            })
            .collect::<Vec<_>>();
        arrow_schema::Fields::from(fields)
    }

    /// Returns the current logical position in the struct array being built.
    ///
    /// This position represents the index of the next struct element that will be added.
    /// All field builders should be synchronized to this position before adding values.
    #[inline]
    fn next_pos(&self) -> u64 {
        self.next_pos
    }

    /// Moves the builder to the specified logical position.
    ///
    /// This updates the internal position tracking. Individual field builders are
    /// synchronized to this position when they are accessed through [`field`](Self::field)
    /// or [`field_at`](Self::field_at) methods.
    ///
    /// # Arguments
    ///
    /// * `pos` - The target position to move to
    #[inline]
    fn move_to_pos(&mut self, pos: u64) {
        self.next_pos = pos;
    }

    /// Returns the number of currently registered fields.
    ///
    /// Unlike static struct builders, this count can change as new fields are
    /// registered during the building process.
    ///
    /// # Returns
    ///
    /// The current number of registered fields
    #[inline]
    fn field_count(&self) -> usize {
        self.fields.len()
    }

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
    /// Panics if the index is out of bounds (>= [`field_count()`](Self::field_count))
    fn field_name(&self, index: usize) -> &str {
        self.fields[index].1.as_str()
    }

    /// Consumes the builder and returns the built field arrays.
    ///
    /// This method finalizes all registered field builders and returns a vector of arrays
    /// that will be used to construct the final struct array. Before building, all field
    /// builders are synchronized to the current position to ensure consistent array lengths.
    ///
    /// After building, the position is reset to 0 to allow the builder to be potentially
    /// reused (though this is not a common pattern).
    ///
    /// # Returns
    ///
    /// A vector of arrays, one for each registered field, in the order they were registered
    fn build_fields(&mut self) -> Vec<Arc<dyn Array>> {
        self.fields
            .iter_mut()
            .for_each(|(field, _)| field.move_to_pos(self.next_pos));

        self.next_pos = 0;

        self.fields
            .iter_mut()
            .map(|field| field.0.build())
            .collect()
    }
}

/// Default implementation for [`FluidStructFields`].
///
/// Creates a new instance with:
/// - An empty fields vector
/// - An empty field name-to-index map  
/// - Position initialized to 0
///
/// This provides a clean starting state for dynamic field registration.
impl Default for FluidStructFields {
    fn default() -> Self {
        Self {
            fields: Default::default(),
            field_map: Default::default(),
            next_pos: 0,
        }
    }
}

/// Type alias for a [`StructBuilder`] using [`FluidStructFields`].
///
/// This provides a convenient way to work with dynamic struct builders without
/// having to specify the generic type parameter. It combines the struct building
/// infrastructure with the dynamic field registration capabilities of [`FluidStructFields`].
pub type FluidStructBuilder = StructBuilder<FluidStructFields>;

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

    #[test]
    fn test_struct_builder() {
        let mut builder = FooBuilder::default();

        builder.name_field().push("aaa");
        builder.value_field().push(10);
        builder.finish_struct();

        builder.name_field().push("bbb");

        {
            let measures = builder.measures_field();
            measures.item().push(b"111");
            measures.item().push(b"222");
            measures.finish_list();
        }
        builder.finish_struct();

        builder.name_field().push("ccc");
        builder.value_field().push(20);
        builder.blob_field().push(b"1234abc");
        builder.finish_struct();

        let arr = builder.build();
        dbg!(arr);
    }

    #[derive(Default)]
    pub struct FooFields {
        name: StringBuilder,
        value: Int64Builder,
        blob: BinaryBuilder,
        measures: ListBuilder<FixedSizeBinaryBuilder<3>>,
        next_pos: u64,
    }
    impl FooFields {
        pub fn name_field(&mut self) -> &mut StringBuilder {
            self.name.move_to_pos(self.next_pos);
            &mut self.name
        }

        pub fn value_field(&mut self) -> &mut Int64Builder {
            self.value.move_to_pos(self.next_pos);
            &mut self.value
        }

        pub fn blob_field(&mut self) -> &mut BinaryBuilder {
            self.blob.move_to_pos(self.next_pos);
            &mut self.blob
        }

        pub fn measures_field(&mut self) -> &mut ListBuilder<FixedSizeBinaryBuilder<3>> {
            self.measures.move_to_pos(self.next_pos);
            &mut self.measures
        }
    }

    impl StructFieldsBuilder for FooFields {
        fn schema(&self) -> arrow_schema::Fields {
            arrow_schema::Fields::from(vec![
                arrow_schema::Field::new("name", self.name.data_type(), true),
                arrow_schema::Field::new("value", self.value.data_type(), true),
                arrow_schema::Field::new("blob", self.blob.data_type(), true),
                arrow_schema::Field::new("measures", self.measures.data_type(), true),
            ])
        }

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
            const NAMES: &[&str] = &["name", "value", "blob", "measures"];
            NAMES[index]
        }

        fn build_fields(&mut self) -> Vec<Arc<dyn Array>> {
            self.name.move_to_pos(self.next_pos);
            self.value.move_to_pos(self.next_pos);
            self.blob.move_to_pos(self.next_pos);
            self.measures.move_to_pos(self.next_pos);
            self.next_pos = 0;
            vec![
                self.name.build(),
                self.value.build(),
                self.blob.build(),
                self.measures.build(),
            ]
        }
    }

    pub type FooBuilder = StructBuilder<FooFields>;
}
