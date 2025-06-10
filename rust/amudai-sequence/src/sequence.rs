//! This module defines the core abstraction for sequences of values used in the
//! Amudai data system.
//!
//! It provides the [`Sequence`] trait, representing an abstract sequence of values,
//! and re-exports [`ValueSequence`] for concrete implementations. Sequences serve as
//! the primary interface for working with columnar data in Amudai shards, supporting
//! dynamic type inspection and efficient access to element counts, types, and values.

use std::{any::Any, borrow::Cow};

use amudai_format::schema::BasicTypeDescriptor;

/// A basic sequence of values.
///
/// This is a re-export of [`ValueSequence`] from the `value_sequence` module,
/// provided here for convenience. `ValueSequence` is a concrete implementation
/// of the [`Sequence`] trait, supporting storage and access to values
/// in a type-erased manner.
pub use super::value_sequence::ValueSequence;

/// Trait representing an abstract sequence of values.
///
/// Types implementing this trait provide access to a list of values of a single
/// logical type.
pub trait Sequence: Send + Sync + 'static {
    /// Returns a reference to this sequence as a type-erased `Any` trait object.
    ///
    /// Enables dynamic downcasting to concrete sequence types.
    fn as_any(&self) -> &(dyn Any + Send + Sync + 'static);

    /// Returns the basic type descriptor for the values in this sequence.
    fn basic_type(&self) -> BasicTypeDescriptor;

    /// Returns the number of elements (value slots) in the sequence.
    ///
    /// This is the total number of elements in the sequence, regardless of
    /// whether the values are null or not.
    fn len(&self) -> usize;

    /// Returns `true` if the sequence has no elements.
    fn is_empty(&self) -> bool;

    /// Converts ("lowers") the representation of this sequence into a [`ValueSequence`],
    /// which is the most basic representation.
    ///
    /// Returns a borrowed `self` if this is already a `ValueSequence`.
    ///
    /// **Note:** This may incur additional memory allocation, copying, and in general,
    /// CPU-intensive decoding.
    fn to_value_sequence(&self) -> Cow<ValueSequence>;
}
