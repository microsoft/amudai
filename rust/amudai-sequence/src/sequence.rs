//! This module defines the core abstraction for sequences of values used in the
//! Amudai data system.
//!
//! It provides the [`Sequence`] trait, representing an abstract sequence of values,
//! and re-exports [`ValueSequence`] for concrete implementations. Sequences serve as
//! the primary interface for working with columnar data in Amudai shards, supporting
//! dynamic type inspection and efficient access to element counts, types, and values.

use std::{
    any::{Any, TypeId},
    borrow::Cow,
};

use amudai_format::{projection::TypeProjectionRef, schema::BasicTypeDescriptor};

use crate::{
    fixed_list_sequence::FixedListSequence, list_sequence::ListSequence, map_sequence::MapSequence,
    struct_sequence::StructSequence,
};

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

    /// Consumes this sequence and converts it into a type-erased `Box<dyn Any>`
    /// trait object.
    ///
    /// Enables dynamic downcasting to concrete sequence types.
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync + 'static>;

    /// Returns the `TypeId` of this sequence's concrete Rust type.
    ///
    /// This can be used for runtime type inspection of the sequence implementation
    /// itself and drive type-based downcasting logic.
    ///
    /// Under the hood it’s equivalent to `std::any::TypeId::of::<Self>()`.
    fn type_id(&self) -> TypeId;

    /// Creates a deep copy of this sequence and returns it as a boxed trait object.
    ///
    /// This method allows cloning sequences when working with trait objects,
    /// since the standard `Clone` trait cannot be used in object-safe contexts.
    /// The returned sequence will contain the same data as the original.
    fn clone_boxed(&self) -> Box<dyn Sequence>;

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
    fn to_value_sequence(&self) -> Cow<'_, ValueSequence>;
}

/// Extension trait for `Sequence` trait objects to provide type-specific
/// downcasting, basic type inspection, and convenient access to nested
/// sequence data.
///
/// This trait is sealed and implemented only for `dyn Sequence` and
/// `Box<dyn Sequence>`. It offers methods to:
/// - Retrieve the basic type descriptor of the sequence.
/// - Attempt downcasts to concrete sequence implementations (`ValueSequence`,
///   `StructSequence`, `ListSequence`, `FixedListSequence`, `MapSequence`).
/// - Unwrap those downcasts with panicking helpers (`as_value`, `as_struct`,
///   etc.) when the type is known.
/// - Access the nested `TypeProjectionRef` for composite types (lists,
///   fixed-size lists, structs, maps).
pub trait AsSequence: private::Sealed {
    /// Returns the basic type descriptor for the underlying sequence.
    fn basic_type(&self) -> BasicTypeDescriptor;

    /// Attempts to downcast this sequence to a `ValueSequence`, returning `None`
    /// if the underlying type is different.
    fn as_value_opt(&self) -> Option<&ValueSequence>;

    /// Panicking downcast to `ValueSequence`. This will panic if the underlying
    /// sequence is not a `ValueSequence`.
    fn as_value(&self) -> &ValueSequence {
        self.as_value_opt().expect("value sequence")
    }

    /// Attempts to downcast this sequence to a `StructSequence`, returning `None`
    /// if the underlying type is different.
    fn as_struct_opt(&self) -> Option<&StructSequence>;

    /// Panicking downcast to `StructSequence`. This will panic if the underlying
    /// sequence is not a `StructSequence`.
    fn as_struct(&self) -> &StructSequence {
        self.as_struct_opt().expect("struct sequence")
    }

    /// Attempts to downcast this sequence to a `ListSequence`, returning `None`
    /// if the underlying type is different.
    fn as_list_opt(&self) -> Option<&ListSequence>;

    /// Panicking downcast to `ListSequence`. This will panic if the underlying
    /// sequence is not a `ListSequence`.
    fn as_list(&self) -> &ListSequence {
        self.as_list_opt().expect("list sequence")
    }

    /// Attempts to downcast this sequence to a `FixedListSequence`, returning `None`
    /// if the underlying type is different.
    fn as_fixed_list_opt(&self) -> Option<&FixedListSequence>;

    /// Panicking downcast to `FixedListSequence`. This will panic if the underlying
    /// sequence is not a `FixedListSequence`.
    fn as_fixed_list(&self) -> &FixedListSequence {
        self.as_fixed_list_opt().expect("fixed_list sequence")
    }

    /// Attempts to downcast this sequence to a `MapSequence`, returning `None`
    /// if the underlying type is different.
    fn as_map_opt(&self) -> Option<&MapSequence>;

    /// Panicking downcast to `MapSequence`. This will panic if the underlying
    /// sequence is not a `MapSequence`.
    fn as_map(&self) -> &MapSequence {
        self.as_map_opt().expect("map sequence")
    }

    /// Returns the nested `TypeProjectionRef` for composite sequence types
    /// (`List`, `FixedSizeList`, `Struct`, `Map`), or `None` for primitive
    /// types. Panics on `Union` until it is supported.
    fn data_type(&self) -> Option<&TypeProjectionRef> {
        use amudai_format::defs::schema::BasicType;
        match self.basic_type().basic_type {
            BasicType::List => self.as_list().data_type.as_ref(),
            BasicType::FixedSizeList => self.as_fixed_list().data_type.as_ref(),
            BasicType::Struct => self.as_struct().data_type.as_ref(),
            BasicType::Map => self.as_map().data_type.as_ref(),
            BasicType::Union => todo!(),
            _ => None,
        }
    }
}

impl private::Sealed for dyn Sequence {}

impl AsSequence for dyn Sequence {
    fn basic_type(&self) -> BasicTypeDescriptor {
        Sequence::basic_type(self)
    }

    fn as_value_opt(&self) -> Option<&ValueSequence> {
        self.as_any().downcast_ref()
    }

    fn as_struct_opt(&self) -> Option<&StructSequence> {
        self.as_any().downcast_ref()
    }

    fn as_list_opt(&self) -> Option<&ListSequence> {
        self.as_any().downcast_ref()
    }

    fn as_fixed_list_opt(&self) -> Option<&FixedListSequence> {
        self.as_any().downcast_ref()
    }

    fn as_map_opt(&self) -> Option<&MapSequence> {
        self.as_any().downcast_ref()
    }
}

impl private::Sealed for Box<dyn Sequence> {}

impl AsSequence for Box<dyn Sequence> {
    fn basic_type(&self) -> BasicTypeDescriptor {
        self.as_ref().basic_type()
    }

    fn as_value_opt(&self) -> Option<&ValueSequence> {
        self.as_ref().as_value_opt()
    }

    fn as_struct_opt(&self) -> Option<&StructSequence> {
        self.as_ref().as_struct_opt()
    }

    fn as_list_opt(&self) -> Option<&ListSequence> {
        self.as_ref().as_list_opt()
    }

    fn as_fixed_list_opt(&self) -> Option<&FixedListSequence> {
        self.as_ref().as_fixed_list_opt()
    }

    fn as_map_opt(&self) -> Option<&MapSequence> {
        self.as_ref().as_map_opt()
    }
}

mod private {
    pub trait Sealed {}
}
