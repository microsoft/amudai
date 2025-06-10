//! Sequence abstractions for columnar data retrieval in the Amudai format.
//!
//! This crate provides the foundational components for working with sequences of
//! typed values in Amudai's columnar storage format. It offers both abstract interfaces
//! and concrete implementations for efficiently accessing, and manipulating columnar
//! data with support for null values, variable-length data, and type safety.
//!
//! # Core Concepts
//!
//! ## Sequences
//!
//! A sequence represents an owned slice of data containing values of a single storage
//! type.
//!
//! ## Type Safety
//!
//! The crate provides type-safe abstractions through the [`crate::sequence::Sequence`] trait,
//! which allows working with sequences in a type-erased manner while maintaining runtime type
//! information through [`amudai_format::defs::schema_ext::BasicTypeDescriptor`].
//!
//! ## Memory Efficiency
//!
//! The implementation is designed for memory efficiency and performance:
//! - Values are stored in contiguous, aligned byte buffers
//! - Null value tracking uses optimized representations (trivial, all-nulls, or bitset)
//! - Variable-length data uses offset arrays for efficient access
//!
//! # Main Components
//!
//! ## [`crate::sequence::Sequence`] Trait
//!
//! The core abstraction representing a sequence of values. It provides:
//! - Type introspection through [`basic_type()`](crate::sequence::Sequence::basic_type)
//! - Length information via [`len()`](crate::sequence::Sequence::len)
//! - Conversion to the concrete [`crate::value_sequence::ValueSequence`] representation
//!
//! ## [`crate::value_sequence::ValueSequence`]
//!
//! The primary concrete implementation of [`crate::sequence::Sequence`]. It stores:
//! - **Values**: Raw data in an aligned byte buffer
//! - **Offsets**: For variable-length data like strings and binary
//! - **Presence**: Null/non-null information
//! - **Type descriptor**: Metadata about the stored data type
//!
//! ## Supporting Types
//!
//! - [`crate::values::Values`]: Type-safe storage for raw data with alignment guarantees
//! - [`crate::offsets::Offsets`]: Management of offset arrays for variable-length data
//! - [`crate::presence::Presence`]: Efficient tracking of null values with multiple storage strategies
//!
//! # Supported Data Types
//!
//! The crate supports various data types through [`amudai_format::defs::schema::BasicType`]:
//!
//! - **Primitive types**: `Int8`, `Int16`, `Int32`, `Int64`, `Float32`, `Float64`
//! - **Variable-length**: `String`, `Binary`
//! - **Fixed-size**: `FixedSizeBinary`, `Guid`
//! - **Temporal**: `DateTime`
//! - **Logical**: `Boolean`
//! - **Containers**: `Struct`, `List`, `Map` and `Union`.

pub mod offsets;
pub mod presence;
pub mod sequence;
pub mod value_sequence;
pub mod values;
