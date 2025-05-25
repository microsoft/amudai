//! Conversion utilities for transforming Amudai `Sequence` objects into Arrow arrays.
//!
//! This module provides the [`IntoArrowArray`] trait and its implementation for [`Sequence`],
//! enabling conversion of Amudai's sequence abstraction into Arrow's array types.
//! It supports various primitive, string, and binary types, bridging the gap between
//! Amudai's internal data representation and Arrow's columnar format.

use amudai_common::{error::Error, Result};
use amudai_sequence::sequence::ValueSequence;
use arrow_array::{
    types::{
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    },
    ArrayRef, ArrowPrimitiveType, LargeBinaryArray, LargeStringArray, PrimitiveArray,
};
use arrow_buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use std::sync::Arc;

use crate::buffers::aligned_vec_to_buf;
use amudai_format::schema::BasicType;

/// Trait for converting a type into an Arrow [`ArrayRef`].
///
/// Implementors of this trait can be transformed into Arrow arrays,
/// facilitating interoperability with Arrow-based data processing.
pub trait IntoArrowArray {
    /// Converts `self` into an Arrow [`ArrayRef`].
    ///
    /// # Errors
    ///
    /// Returns an error if the conversion is not supported or fails.
    fn into_arrow_array(self) -> Result<ArrayRef>;
}

impl IntoArrowArray for ValueSequence {
    /// Converts an Amudai [`Sequence`] into an Arrow [`ArrayRef`].
    ///
    /// Supports primitive, string, and binary types as described by the sequence's type descriptor.
    /// Returns an error for unsupported or unimplemented types.
    fn into_arrow_array(self) -> Result<ArrayRef> {
        let type_desc = self.type_desc;
        let len = self.len();
        let values = aligned_vec_to_buf(self.values.into_inner());
        let offsets = self.offsets.map(|offsets| {
            assert_eq!(offsets.item_count(), len);
            let buf = aligned_vec_to_buf(offsets.into_inner().into_inner());
            let buf = ScalarBuffer::<i64>::new(buf, 0, len + 1);
            OffsetBuffer::new(buf)
        });
        match type_desc.basic_type {
            BasicType::Unit => todo!(),
            BasicType::Boolean => todo!(),
            BasicType::Int8 => {
                assert!(offsets.is_none());
                if type_desc.signed {
                    make_primitive_array::<Int8Type>(values, len)
                } else {
                    make_primitive_array::<UInt8Type>(values, len)
                }
            }
            BasicType::Int16 => {
                assert!(offsets.is_none());
                if type_desc.signed {
                    make_primitive_array::<Int16Type>(values, len)
                } else {
                    make_primitive_array::<UInt16Type>(values, len)
                }
            }
            BasicType::Int32 => {
                assert!(offsets.is_none());
                if type_desc.signed {
                    make_primitive_array::<Int32Type>(values, len)
                } else {
                    make_primitive_array::<UInt32Type>(values, len)
                }
            }
            BasicType::Int64 => {
                assert!(offsets.is_none());
                if type_desc.signed {
                    make_primitive_array::<Int64Type>(values, len)
                } else {
                    make_primitive_array::<UInt64Type>(values, len)
                }
            }
            BasicType::Float32 => {
                assert!(offsets.is_none());
                make_primitive_array::<Float32Type>(values, len)
            }
            BasicType::Float64 => {
                assert!(offsets.is_none());
                make_primitive_array::<Float64Type>(values, len)
            }
            BasicType::Binary => {
                let offsets = offsets.expect("offsets");
                LargeBinaryArray::try_new(offsets, values, None)
                    .map(|arr| Arc::new(arr) as ArrayRef)
                    .map_err(|e| Error::arrow("new binary array", e))
            }
            BasicType::FixedSizeBinary => todo!(),
            BasicType::String => {
                let offsets = offsets.expect("offsets");
                LargeStringArray::try_new(offsets, values, None)
                    .map(|arr| Arc::new(arr) as ArrayRef)
                    .map_err(|e| Error::arrow("new string array", e))
            }
            BasicType::Guid => todo!(),
            BasicType::DateTime => todo!(),
            BasicType::List => todo!(),
            BasicType::FixedSizeList => todo!(),
            BasicType::Struct => todo!(),
            BasicType::Map => todo!(),
            BasicType::Union => todo!(),
        }
    }
}

/// Helper function to construct an Arrow primitive array from a buffer.
///
/// # Parameters
/// - `values_buf`: Buffer containing the primitive values.
/// - `len`: Number of elements in the array.
///
/// # Returns
/// An [`ArrayRef`] wrapping the constructed Arrow primitive array.
///
/// # Errors
/// Returns an error if the array construction fails.
fn make_primitive_array<T: ArrowPrimitiveType>(values_buf: Buffer, len: usize) -> Result<ArrayRef> {
    let values_buf = ScalarBuffer::new(values_buf, 0, len);
    PrimitiveArray::<T>::try_new(values_buf, None)
        .map(|arr| Arc::new(arr) as ArrayRef)
        .map_err(|e| Error::arrow("make_primitive_array", e))
}
