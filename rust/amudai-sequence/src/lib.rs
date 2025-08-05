//! # amudai-sequence
//!
//! Sequence abstractions for columnar data retrieval in the Amudai format.
//! This crate provides both abstract interfaces and concrete implementations
//! for efficiently accessing, manipulating, and streaming columnar data with
//! full support for nulls, variable-length values, and runtime type safety.
//!
//! ## Core Traits & Types
//!
//! - [`crate::sequence::Sequence`]:
//!   A type‐erased sequence interface exposing:
//!   - `basic_type()` – the underlying `BasicType` descriptor
//!   - `len()` – number of elements
//!   - `.into_value_sequence()` – convert to a `ValueSequence`
//!
//! - [`crate::value_sequence::ValueSequence`]:
//!   Primary owned sequence implementation holding:
//!   - **Values** in an aligned byte buffer
//!   - **Offsets** for variable‐length elements
//!   - **Presence** for null tracking (trivial/all‐nulls/bitset)
//!   - **Type descriptor** metadata
//!
//! ## Container Sequences
//!
//! - [`crate::list_sequence::ListSequence`][]: nested lists
//! - [`crate::struct_sequence::StructSequence`][]: struct‐of‐arrays
//! - [`crate::map_sequence::MapSequence`][]: key/value maps
//! - [`crate::fixed_list_sequence::FixedListSequence`][]: fixed‐length lists
//!
//! ## Supporting Utilities
//!
//! - [`crate::values::Values`]: alignment‐guaranteed raw storage
//! - [`crate::offsets::Offsets`]: offset arrays
//! - [`crate::presence::Presence`]: nulls representation
//! - [`crate::sequence_reader`]: sequences reader trait
//! - [`crate::json_printer`]: JSON serialization helpers
//!
//! ## Supported Types
//!
//! All types listed in [`amudai_format::defs::schema::BasicType`]:
//! - Primitives: `Int8`, `Int16`, `Int32`, `Int64`, `Float32`, `Float64`
//! - Variable-length: `String`, `Binary`
//! - Fixed-size: `FixedSizeBinary`, `Guid`
//! - Temporal: `DateTime`
//! - Logical: `Boolean`
//! - Containers: `Struct`, `List`, `Map`, `FixedSizeList`, `Union`
//!
//! For more details, see the individual module docs below.

pub mod fixed_list_sequence;
pub mod frame;
pub mod json_printer;
pub mod list_sequence;
pub mod map_sequence;
pub mod offsets;
pub mod presence;
pub mod sequence;
pub mod sequence_reader;
pub mod struct_sequence;
pub mod value_sequence;
pub mod values;
