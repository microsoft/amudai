//! # Arrow Builders
//!
//! A convenient set of data structures for record-by-record building of Apache Arrow record batches
//! and value-by-value building of arrays, including nested structs, lists, and maps.
//!
//! This crate provides strongly-typed builders that follow a bottom-up building approach: start
//! from leaf values, populate lists and structs, then finalize records. Values may be skipped
//! and will be automatically filled with nulls.
//!
//! ## Key Features
//!
//! - **Strongly typed builders**: Each builder is parameterized by specific Arrow types
//! - **Nested structure support**: Build complex nested structures including lists of lists, maps of lists, etc.
//! - **Automatic null handling**: Skip values and they'll be filled with nulls automatically
//! - **Position-based building**: Builders track positions to allow sparse value setting
//! - **Macro-generated struct builders**: Use the `struct_builder!` macro for convenient struct definitions
//!
//! ## Core Trait
//!
//! All builders implement the [`ArrayBuilder`] trait which provides:
//! - `next_pos()`: Get the current position
//! - `move_to_pos(pos)`: Move to a specific position
//! - `build()`: Consume the builder and produce an Arrow array
//!
//! ## Building Approach
//!
//! The workflow follows a bottom-up approach:
//!
//! 1. **Start with leaf values**: Set primitive values (strings, numbers, binary data)
//! 2. **Build collections**: Populate lists and maps using the leaf values
//! 3. **Construct structs**: Combine fields into structured records
//! 4. **Finalize records**: Call `next_record()` or equivalent to complete a record
//!
//! ## Basic Usage
//!
//! ### Primitive Builders
//!
//! ```rust
//! use amudai_arrow_builders::{StringBuilder, Int64Builder, ArrayBuilder};
//!
//! // Build a string array
//! let mut string_builder = StringBuilder::default();
//! string_builder.set("hello");
//! string_builder.set("world");
//! string_builder.set_null(); // Explicitly set null
//! let string_array = string_builder.build();
//!
//! // Build an integer array
//! let mut int_builder = Int64Builder::default();
//! int_builder.set(42);
//! int_builder.set(-10);
//! int_builder.set(100);
//! let int_array = int_builder.build();
//! ```
//!
//! ### List Builders
//!
//! ```rust
//! use amudai_arrow_builders::{ListBuilder, Int64Builder, ArrayBuilder};
//!
//! let mut list_builder = ListBuilder::<Int64Builder>::default();
//!
//! // First list: [1, 2, 3]
//! list_builder.item().set(1);
//! list_builder.item().set(2);
//! list_builder.item().set(3);
//! list_builder.finish_list();
//!
//! // Second list: [] (empty)
//! list_builder.finish_list();
//!
//! // Third list: null
//! list_builder.finish_null_list();
//!
//! let list_array = list_builder.build();
//! ```
//!
//! ### Map Builders
//!
//! ```rust
//! use amudai_arrow_builders::{MapBuilder, StringBuilder, Int64Builder, ArrayBuilder};
//!
//! let mut map_builder = MapBuilder::<StringBuilder, Int64Builder>::default();
//!
//! // Map: {"key1": 10, "key2": 20}
//! map_builder.key().set("key1");
//! map_builder.value().set(10);
//! map_builder.key().set("key2");
//! map_builder.value().set(20);
//! map_builder.finish_map();
//!
//! // Null map
//! map_builder.finish_null_map();
//!
//! let map_array = map_builder.build();
//! ```
//!
//! ## Struct Builders with Macros
//!
//! The most convenient way to work with struct builders is using the
//! [`struct_builder!`](amudai_arrow_builders_macros::struct_builder) macro:
//!
//! ```rust
//! use amudai_arrow_builders_macros::struct_builder;
//! use amudai_arrow_builders::ArrayBuilder;
//!
//! // Define a struct builder
//! struct_builder!(
//!     struct Person {
//!         id: i64,
//!         name: String,
//!         scores: List<Float64>,
//!         metadata: Map<String, String>,
//!     }
//! );
//!
//! // Use the generated builder
//! let mut builder = PersonBuilder::default();
//!
//! // Build first person
//! builder.id_field().set(1);
//! builder.name_field().set("Alice");
//! {
//!     let scores = builder.scores_field();
//!     scores.item().set(95.5);
//!     scores.item().set(87.2);
//!     scores.finish_list();
//! }
//! {
//!     let metadata = builder.metadata_field();
//!     metadata.key().set("department");
//!     metadata.value().set("engineering");
//!     metadata.finish_map();
//! }
//! builder.finish_struct();
//!
//! // Build second person (with some fields null)
//! builder.id_field().set(2);
//! builder.name_field().set("Bob");
//! // scores and metadata will be null if not set
//! builder.finish_struct();
//!
//! let struct_array = builder.build();
//! ```
//!
//! ## Advanced Usage
//!
//! ### Deeply Nested Structures
//!
//! ```rust
//! use amudai_arrow_builders_macros::struct_builder;
//!
//! struct_builder!(
//!     struct ComplexData {
//!         nested_lists: List<List<String>>,
//!         map_of_lists: Map<String, List<i64>>,
//!     }
//! );
//!
//! let mut builder = ComplexDataBuilder::default();
//!
//! // Build nested list: [["a", "b"], ["c"]]
//! {
//!     let nested = builder.nested_lists_field();
//!     
//!     let inner1 = nested.item();
//!     inner1.item().set("a");
//!     inner1.item().set("b");
//!     inner1.finish_list();
//!     
//!     let inner2 = nested.item();
//!     inner2.item().set("c");
//!     inner2.finish_list();
//!     
//!     nested.finish_list();
//! }
//!
//! // Build map of lists: {"numbers": [1, 2, 3]}
//! {
//!     let map_field = builder.map_of_lists_field();
//!     map_field.key().set("numbers");
//!     let list_value = map_field.value();
//!     list_value.item().set(1);
//!     list_value.item().set(2);
//!     list_value.item().set(3);
//!     list_value.finish_list();
//!     map_field.finish_map();
//! }
//!
//! builder.finish_struct();
//! ```
//!
//! ### Record Batch Building
//!
//! For building record batches (tables), use [`RecordBatchBuilder`]:
//!
//! ```rust
//! use amudai_arrow_builders_macros::struct_builder;
//! use amudai_arrow_builders::record_batch::RecordBatchBuilder;
//!
//! struct_builder!(
//!     struct Person {
//!         id: i64,
//!         name: String,
//!         scores: List<Float64>,
//!         metadata: Map<String, String>,
//!     }
//! );
//!
//! let mut batch_builder = RecordBatchBuilder::<PersonFields>::default();
//!
//! // Add multiple records
//! batch_builder.id_field().set(1);
//! batch_builder.name_field().set("Alice");
//! batch_builder.finish_record();
//!
//! batch_builder.id_field().set(2);
//! batch_builder.name_field().set("Bob");
//! batch_builder.finish_record();
//!
//! let record_batch = batch_builder.build();
//! ```
//!
//! ## Null Handling
//!
//! Builders automatically handle nulls in several ways:
//!
//! - **Explicit nulls**: Call `set_null()` methods
//! - **Implicit nulls**: Skip setting values - they'll be filled as nulls
//! - **Null collections**: Use `finish_null_list()`, `finish_null_map()`, etc.
//! - **Sparse setting**: Set values at any position, gaps are filled with nulls
//!
//! ## Type Mapping
//!
//! The following types are supported in struct builder macros:
//!
//! | Macro Type | Builder Type | Arrow Type |
//! |------------|--------------|------------|
//! | `String` | `StringBuilder` | `LargeString` |
//! | `Binary` | `BinaryBuilder` | `LargeBinary` |
//! | `FixedSizeBinary<N>` | `FixedSizeBinaryBuilder<N>` | `FixedSizeBinary` |
//! | `i8` | `Int8Builder` | `Int8` |
//! | `i16` | `Int16Builder` | `Int16` |
//! | `i32` | `Int32Builder` | `Int32` |
//! | `i64` | `Int64Builder` | `Int64` |
//! | `u8` | `UInt8Builder` | `UInt8` |
//! | `u16` | `UInt16Builder` | `UInt16` |
//! | `u32` | `UInt32Builder` | `UInt32` |
//! | `u64` | `UInt64Builder` | `UInt64` |
//! | `f32`/`Float32` | `Float32Builder` | `Float32` |
//! | `f64`/`Float64` | `Float64Builder` | `Float64` |
//! | `Timestamp` | `TimestampBuilder` | `Timestamp(Nanosecond)` |
//! | `List<T>` | `ListBuilder<T>` | `LargeList` |
//! | `Map<K, V>` | `MapBuilder<K, V>` | `Map` |

use std::sync::Arc;

use arrow_array::Array;

pub mod binary;
pub mod list;
pub mod map;
pub mod primitive;
pub mod record_batch;
pub mod string;
pub mod structure;
#[cfg(test)]
mod tests;

pub use binary::{BinaryBuilder, FixedSizeBinaryBuilder};
pub use list::ListBuilder;
pub use map::MapBuilder;
pub use primitive::{
    Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder,
    TimestampBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
};
pub use string::StringBuilder;
pub use structure::{StructBuilder, StructFieldsBuilder};

/// Core trait for building Apache Arrow arrays.
///
/// The `ArrayBuilder` trait provides a consistent interface for all array builders in this crate.
/// It enables position-based building where values can be set at specific positions, with
/// automatic null filling for gaps.
///
/// ## Position-Based Building
///
/// All builders maintain a logical position that tracks where the next value should be placed.
/// This allows for sparse value setting where you can skip positions and they will be
/// automatically filled with nulls.
///
/// ## Core Methods
///
/// - [`next_pos()`]: Returns the current logical position (where the next value will be placed)
/// - [`move_to_pos()`]: Moves the builder to a specific position
/// - [`build()`]: Consumes the builder and produces the final Arrow array
///
/// ## Implementation Requirements
///
/// Implementors must ensure that:
/// - The builder starts at position 0 by default
/// - Calling `move_to_pos(pos)` sets the next insertion point to `pos`
/// - Values set after the current position automatically fill gaps with nulls
/// - The `build()` method finalizes any remaining gaps and produces a valid Arrow array
pub trait ArrayBuilder: Default + Send + Sync + 'static {
    /// Returns the current logical position where the next value will be placed.
    ///
    /// This position represents the index of the next element to be added to the array.
    /// When a builder is first created, this returns 0. After setting a value or
    /// explicitly moving to a position, this reflects the next available slot.
    fn next_pos(&self) -> u64;

    /// Moves the builder to a specific logical position.
    ///
    /// This sets where the next value will be placed in the array. If the new position
    /// is beyond the current position, any gaps will be automatically filled with nulls
    /// when values are set or when the array is built.
    ///
    /// # Parameters
    ///
    /// * `pos` - The target position (0-based index) for the next value, must be greater
    ///   or equal to the current position.
    fn move_to_pos(&mut self, pos: u64);

    /// Consumes the builder and produces the final Apache Arrow array.
    ///
    /// This method finalizes the building process by:
    /// 1. Filling any remaining gaps between the last set value and the current position with nulls
    /// 2. Converting the internal buffer(s) into an Apache Arrow array
    /// 3. Returning the array wrapped in an `Arc<dyn Array>`
    ///
    /// After calling this method, the builder is consumed and cannot be used further.
    ///
    /// # Returns
    ///
    /// An `Arc<dyn Array>` containing the built Arrow array. The concrete type depends
    /// on the builder implementation (e.g., `LargeStringArray` for `StringBuilder`).
    fn build(self) -> Arc<dyn Array>;
}
