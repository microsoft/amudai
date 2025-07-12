//! Conversion utilities for transforming Amudai `Sequence` objects into Arrow arrays.
//!
//! This module provides the [`IntoArrowArray`] trait and its implementation for
//! [`ValueSequence`], enabling conversion of Amudai's sequence abstraction into
//! Arrow's array types.
//! It supports various primitive, string, and binary types, bridging the gap between
//! Amudai's internal data representation and Arrow's columnar format.

use amudai_common::{Result, error::Error};
use amudai_sequence::{offsets::Offsets, presence::Presence, sequence::ValueSequence};
use arrow_array::{
    ArrayRef, ArrowPrimitiveType, FixedSizeBinaryArray, LargeBinaryArray, LargeStringArray,
    PrimitiveArray,
    types::{
        Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    },
};
use arrow_buffer::{Buffer, MutableBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::DataType as ArrowDataType;
use std::sync::Arc;

use crate::{buffers::aligned_vec_to_buf, datetime_conversions::datetime_sequence_to_array};
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
    fn into_arrow_array(self, arrow_type: &ArrowDataType) -> Result<ArrayRef>;
}

impl IntoArrowArray for ValueSequence {
    /// Converts an Amudai [`ValueSequence`] into an Arrow [`ArrayRef`].
    ///
    /// Supports primitive, string, and binary types as described by the sequence's type descriptor.
    /// Returns an error for unsupported or unimplemented types.
    fn into_arrow_array(self, arrow_type: &ArrowDataType) -> Result<ArrayRef> {
        if self.type_desc.basic_type == BasicType::DateTime {
            return datetime_sequence_to_array(self, arrow_type);
        }

        let type_desc = self.type_desc;
        let len = self.len();
        let values = aligned_vec_to_buf(self.values.into_inner());
        let nulls = self.presence.into_arrow_null_buffer()?;
        let offsets = self.offsets.map(|offsets| {
            assert_eq!(offsets.item_count(), len);
            offsets.into_arrow_offsets_64()
        });
        match type_desc.basic_type {
            BasicType::Unit | BasicType::Boolean => Err(Error::invalid_operation(format!(
                "into_arrow_array for ValueSequence({:?})",
                type_desc.basic_type
            ))),
            BasicType::Int8 => {
                assert!(offsets.is_none());
                if type_desc.signed {
                    make_primitive_array::<Int8Type>(values, len, nulls)
                } else {
                    make_primitive_array::<UInt8Type>(values, len, nulls)
                }
            }
            BasicType::Int16 => {
                assert!(offsets.is_none());
                if type_desc.signed {
                    make_primitive_array::<Int16Type>(values, len, nulls)
                } else {
                    make_primitive_array::<UInt16Type>(values, len, nulls)
                }
            }
            BasicType::Int32 => {
                assert!(offsets.is_none());
                if type_desc.signed {
                    make_primitive_array::<Int32Type>(values, len, nulls)
                } else {
                    make_primitive_array::<UInt32Type>(values, len, nulls)
                }
            }
            BasicType::Int64 => {
                assert!(offsets.is_none());
                if type_desc.signed {
                    make_primitive_array::<Int64Type>(values, len, nulls)
                } else {
                    make_primitive_array::<UInt64Type>(values, len, nulls)
                }
            }
            BasicType::Float32 => {
                assert!(offsets.is_none());
                make_primitive_array::<Float32Type>(values, len, nulls)
            }
            BasicType::Float64 => {
                assert!(offsets.is_none());
                make_primitive_array::<Float64Type>(values, len, nulls)
            }
            BasicType::Binary => {
                let offsets = offsets.expect("offsets");
                LargeBinaryArray::try_new(offsets, values, nulls)
                    .map(|arr| Arc::new(arr) as ArrayRef)
                    .map_err(|e| Error::arrow("new binary array", e))
            }
            BasicType::FixedSizeBinary | BasicType::Guid => {
                FixedSizeBinaryArray::try_new(type_desc.fixed_size as i32, values, nulls)
                    .map(|arr| Arc::new(arr) as ArrayRef)
                    .map_err(|e| Error::arrow("new fixed-size binary array", e))
            }
            BasicType::String => {
                let offsets = offsets.expect("offsets");
                LargeStringArray::try_new(offsets, values, nulls)
                    .map(|arr| Arc::new(arr) as ArrayRef)
                    .map_err(|e| Error::arrow("new string array", e))
            }
            BasicType::DateTime => todo!(),
            BasicType::List => todo!(),
            BasicType::FixedSizeList => todo!(),
            BasicType::Struct => todo!(),
            BasicType::Map => todo!(),
            BasicType::Union => todo!(),
        }
    }
}

/// Trait for converting a type into an Arrow [`NullBuffer`].
///
/// This trait enables conversion from Amudai's internal nullability representation
/// to Arrow's [`NullBuffer`] format, which uses a packed bitmask to encode validity.
///
/// The conversion returns `Option<NullBuffer>` where:
/// - `None` indicates all values are valid (no nulls present) - this is an optimization
///   since Arrow arrays can omit the null buffer entirely when all values are valid
/// - `Some(NullBuffer)` contains the actual null mask when nulls are present
///
/// # Arrow Format
///
/// Arrow uses a packed bitmask where each bit represents the validity of a value:
/// - `true` bit indicates the value is valid (not null)
/// - `false` bit indicates the value is null
///
/// This differs from some internal representations that may use bytes or other
/// encoding schemes for nullability information.
pub trait IntoArrowNullBuffer {
    /// Converts `self` into an Arrow [`NullBuffer`].
    ///
    /// # Returns
    ///
    /// - `Ok(None)` if all values are valid (no null buffer needed)
    /// - `Ok(Some(NullBuffer))` if nulls are present
    /// - `Err(...)` if the conversion fails
    ///
    /// # Errors
    ///
    /// Returns an error if the conversion process fails, such as when the
    /// input data is malformed or incompatible with Arrow's format requirements.
    fn into_arrow_null_buffer(self) -> Result<Option<NullBuffer>>;
}

impl IntoArrowNullBuffer for Presence {
    /// Converts Amudai's [`Presence`] representation into an Arrow [`NullBuffer`].
    ///
    /// This method handles the conversion between Amudai's three-variant nullability
    /// representation and Arrow's packed bitmask format.
    ///
    /// # Conversion Logic
    ///
    /// - **`Presence::Trivial(_)`**: All values are valid, returns `None` to indicate
    ///   no null buffer is needed. This is an important optimization as Arrow arrays
    ///   can omit the null buffer entirely when all values are valid.
    ///
    /// - **`Presence::Nulls(len)`**: All values are null, returns a `NullBuffer` with
    ///   all bits set to `false` (indicating all nulls).
    ///
    /// - **`Presence::Bytes(bytes)`**: Mixed validity using a byte array where each
    ///   byte represents one value's validity (1=present, 0=null). Converts to Arrow's
    ///   bit-packed format where each bit represents validity (true=valid, false=null).
    ///
    /// # Format Differences
    ///
    /// Amudai's `Presence::Bytes` uses one byte per value (8 bits of storage per value),
    /// while Arrow's `NullBuffer` uses one bit per value (1 bit of storage per value).
    /// The conversion maps:
    /// - Amudai byte `!= 0` → Arrow bit `true` (valid)
    /// - Amudai byte `== 0` → Arrow bit `false` (null)
    ///
    /// # Performance Notes
    ///
    /// The `Bytes` variant conversion currently uses an iterator approach which is
    /// straightforward but not optimal. There's a TODO to implement bit-packing
    /// optimization that would directly convert the byte array to a packed bit array,
    /// which would be significantly faster for large arrays.
    fn into_arrow_null_buffer(self) -> Result<Option<NullBuffer>> {
        match self {
            Presence::Trivial(_) => Ok(None),
            Presence::Nulls(len) => Ok(Some(NullBuffer::new_null(len))),
            Presence::Bytes(bytes) => {
                // TODO: optimize (bit-pack from presence 0/1 bytes)
                Ok(Some(NullBuffer::from_iter(bytes.iter().map(|&b| b != 0))))
            }
        }
    }
}

/// Trait for converting a type into Arrow [`OffsetBuffer`] formats.
///
/// This trait provides conversion methods for transforming Amudai's internal offset
/// representation into Arrow's offset buffer formats, which are used for variable-length
/// data types like strings, binary arrays, and lists.
pub trait IntoArrowOffsets {
    /// Converts `self` into a 64-bit Arrow [`OffsetBuffer`].
    ///
    /// This is the preferred conversion method as it can represent any offset value
    /// that fits in Amudai's internal u64 representation without overflow concerns.
    /// Arrow's "large" array types (e.g., `LargeStringArray`, `LargeBinaryArray`)
    /// use this format.
    ///
    /// # Returns
    ///
    /// An [`OffsetBuffer<i64>`] containing the converted offset values.
    ///
    /// # Performance
    ///
    /// This conversion is generally zero-copy or very efficient, as it primarily
    /// involves buffer reinterpretation rather than data transformation.
    fn into_arrow_offsets_64(self) -> OffsetBuffer<i64>;

    /// Converts `self` into a 32-bit Arrow [`OffsetBuffer`].
    ///
    /// This method provides conversion to the standard Arrow offset format used by
    /// regular array types (`StringArray`, `BinaryArray`, etc.). It includes overflow
    /// checking to ensure all offset values fit within the i32 range.
    ///
    /// # Returns
    ///
    /// An [`OffsetBuffer<i32>`] containing the converted offset values.
    ///
    /// # Errors
    ///
    /// Returns an error if any offset value exceeds `i32::MAX`, as this would cause
    /// data truncation and incorrect array interpretation.
    fn into_arrow_offsets_32(self) -> Result<OffsetBuffer<i32>>;
}

impl IntoArrowOffsets for Offsets {
    /// Converts Amudai [`Offsets`] into a 64-bit Arrow [`OffsetBuffer`].
    ///
    /// This implementation provides an efficient conversion from Amudai's internal
    /// offset representation to Arrow's 64-bit offset format. The conversion is
    /// nearly zero-copy, reusing the existing buffer data where possible.
    fn into_arrow_offsets_64(self) -> OffsetBuffer<i64> {
        let len = self.item_count() + 1;
        let buffer = aligned_vec_to_buf(self.into_inner().into_inner());
        OffsetBuffer::new(ScalarBuffer::new(buffer, 0, len))
    }

    /// Converts Amudai [`Offsets`] into a 32-bit Arrow [`OffsetBuffer`].
    ///
    /// This implementation converts from Amudai's internal 64-bit offset representation
    /// to Arrow's 32-bit offset format, with overflow checking to ensure data integrity.
    fn into_arrow_offsets_32(self) -> Result<OffsetBuffer<i32>> {
        if self.as_slice().iter().any(|&off| off > i32::MAX as u64) {
            return Err(Error::invalid_arg("offsets", "offsets do not fit into i32"));
        }

        let mut buf = MutableBuffer::with_capacity((self.item_count() + 1) * 4);
        buf.extend(self.as_slice().iter().map(|&off| off as i32));
        Ok(OffsetBuffer::new(ScalarBuffer::new(
            buf.into(),
            0,
            self.item_count() + 1,
        )))
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
fn make_primitive_array<T: ArrowPrimitiveType>(
    values_buf: Buffer,
    len: usize,
    nulls: Option<NullBuffer>,
) -> Result<ArrayRef> {
    let values_buf = ScalarBuffer::new(values_buf, 0, len);
    PrimitiveArray::<T>::try_new(values_buf, nulls)
        .map(|arr| Arc::new(arr) as ArrayRef)
        .map_err(|e| Error::arrow("make_primitive_array", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use amudai_sequence::{offsets::Offsets, values::Values};

    fn create_offsets_from_slice(offsets: &[u64]) -> Offsets {
        let mut values = Values::new();
        for &offset in offsets {
            values.push(offset);
        }
        Offsets::from_values(values)
    }

    fn verify_offset_buffer_i64(buffer: &OffsetBuffer<i64>, expected: &[i64]) {
        assert_eq!(buffer.len(), expected.len());
        for (i, &expected_val) in expected.iter().enumerate() {
            assert_eq!(buffer[i], expected_val);
        }
    }

    fn verify_offset_buffer_i32(buffer: &OffsetBuffer<i32>, expected: &[i32]) {
        assert_eq!(buffer.len(), expected.len());
        for (i, &expected_val) in expected.iter().enumerate() {
            assert_eq!(buffer[i], expected_val);
        }
    }

    #[test]
    fn test_into_arrow_offsets_64_empty() {
        let offsets = Offsets::new();
        let arrow_buffer = offsets.into_arrow_offsets_64();
        verify_offset_buffer_i64(&arrow_buffer, &[0]);
    }

    #[test]
    fn test_into_arrow_offsets_32_empty() {
        let offsets = Offsets::new();
        let arrow_buffer = offsets.into_arrow_offsets_32().unwrap();
        verify_offset_buffer_i32(&arrow_buffer, &[0]);
    }

    #[test]
    fn test_into_arrow_offsets_64_single_item() {
        let offsets = create_offsets_from_slice(&[0, 5]);
        let arrow_buffer = offsets.into_arrow_offsets_64();
        verify_offset_buffer_i64(&arrow_buffer, &[0, 5]);
    }

    #[test]
    fn test_into_arrow_offsets_32_single_item() {
        let offsets = create_offsets_from_slice(&[0, 5]);
        let arrow_buffer = offsets.into_arrow_offsets_32().unwrap();
        verify_offset_buffer_i32(&arrow_buffer, &[0, 5]);
    }

    #[test]
    fn test_into_arrow_offsets_64_multiple_items() {
        let offsets = create_offsets_from_slice(&[0, 3, 7, 12, 20]);
        let arrow_buffer = offsets.into_arrow_offsets_64();
        verify_offset_buffer_i64(&arrow_buffer, &[0, 3, 7, 12, 20]);
    }

    #[test]
    fn test_into_arrow_offsets_32_multiple_items() {
        let offsets = create_offsets_from_slice(&[0, 3, 7, 12, 20]);
        let arrow_buffer = offsets.into_arrow_offsets_32().unwrap();
        verify_offset_buffer_i32(&arrow_buffer, &[0, 3, 7, 12, 20]);
    }

    #[test]
    fn test_into_arrow_offsets_64_large_values() {
        let large_offsets = &[0, 1_000_000, 2_000_000_000, 5_000_000_000];
        let offsets = create_offsets_from_slice(large_offsets);
        let arrow_buffer = offsets.into_arrow_offsets_64();
        verify_offset_buffer_i64(&arrow_buffer, &[0, 1_000_000, 2_000_000_000, 5_000_000_000]);
    }

    #[test]
    fn test_into_arrow_offsets_64_duplicates_pattern() {
        let offsets = create_offsets_from_slice(&[0, 5, 5, 10, 10, 15]);
        let arrow_buffer = offsets.into_arrow_offsets_64();
        verify_offset_buffer_i64(&arrow_buffer, &[0, 5, 5, 10, 10, 15]);
    }

    #[test]
    fn test_into_arrow_offsets_32_small_values() {
        let offsets = create_offsets_from_slice(&[0, 100, 1000, 10000]);
        let arrow_buffer = offsets.into_arrow_offsets_32().unwrap();
        verify_offset_buffer_i32(&arrow_buffer, &[0, 100, 1000, 10000]);
    }

    #[test]
    fn test_into_arrow_offsets_32_boundary_i32_max() {
        let i32_max = i32::MAX as u64;
        let offsets = create_offsets_from_slice(&[0, i32_max - 1, i32_max]);
        let arrow_buffer = offsets.into_arrow_offsets_32().unwrap();
        verify_offset_buffer_i32(&arrow_buffer, &[0, i32::MAX - 1, i32::MAX]);
    }

    #[test]
    fn test_into_arrow_offsets_32_overflow_detection() {
        let i32_max_plus_1 = (i32::MAX as u64) + 1;
        let offsets = create_offsets_from_slice(&[0, i32_max_plus_1]);

        let result = offsets.into_arrow_offsets_32();
        assert!(result.is_err());
    }

    #[test]
    fn test_into_arrow_offsets_32_overflow_multiple_values() {
        let large_value = 5_000_000_000u64; // > i32::MAX
        let offsets = create_offsets_from_slice(&[0, 1000, large_value, large_value + 1000]);

        let result = offsets.into_arrow_offsets_32();
        assert!(result.is_err());
    }

    #[test]
    fn test_into_arrow_offsets_all_zeros() {
        let offsets = create_offsets_from_slice(&[0, 0, 0, 0]);

        let arrow_buffer_64 = offsets.clone().into_arrow_offsets_64();
        verify_offset_buffer_i64(&arrow_buffer_64, &[0, 0, 0, 0]);

        let arrow_buffer_32 = offsets.into_arrow_offsets_32().unwrap();
        verify_offset_buffer_i32(&arrow_buffer_32, &[0, 0, 0, 0]);
    }

    #[test]
    fn test_into_arrow_offsets_round_trip_compatibility() {
        let original_data = &[0, 5, 12, 20, 35, 50];
        let offsets = create_offsets_from_slice(original_data);

        let arrow_buffer_64 = offsets.clone().into_arrow_offsets_64();
        let arrow_buffer_32 = offsets.into_arrow_offsets_32().unwrap();

        assert_eq!(arrow_buffer_64.first(), Some(&0));
        assert_eq!(arrow_buffer_64.last(), Some(&50));

        assert_eq!(arrow_buffer_32.first(), Some(&0));
        assert_eq!(arrow_buffer_32.last(), Some(&50));
    }
}
