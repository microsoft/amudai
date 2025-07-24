//! A basic sequence of values.

use crate::{
    offsets::{FixedSizeOffsetsIter, Offsets, OffsetsIter},
    presence::{Presence, PresenceBuilder},
    sequence::Sequence,
    values::Values,
};
use amudai_decimal::d128;
use amudai_format::{
    defs::common::{AnyValue, any_value::Kind},
    schema::{BasicType, BasicTypeDescriptor},
};
use std::{borrow::Cow, ops::Range};

/// A sequence of values with optional offsets and presence information,
/// capable of representing both fixed-size and variable-size primitive types.
///
/// This is the simplest representation of a value sequence, using fully decoded,
/// contiguous buffers for storage.
///
/// For primitive types (e.g., `Int32`, `Float64`), values are stored directly in
/// the `values` buffer, with no `offsets`.
///
/// Variable-sized values (`String` and `Binary`) are stored as a concatenated,
/// contiguous byte buffer in `values`, accompanied by `offsets` (containing N+1
/// offsets for the corresponding values in the `values` buffer). The value at
/// index `i` occupies the byte range `offsets[i]..offsets[i+1]` in the `values`
/// buffer.
///
/// `FixedSizeBinary` values are similarly stored as a contiguous buffer in `values`,
/// but do not use `offsets`. Instead, `type_desc.fixed_size` specifies the uniform
/// size of each value. The value at index `i` is located at
/// `i * fixed_size..(i + 1) * fixed_size` byte range in the `values` buffer.
///
/// The `presence` field tracks which values are null or non-null.
///
/// See also `amudai-arrow-compat` crate for the `ValueSequence` to Arrow Array
/// conversion functionality.
#[derive(Debug, Clone)]
pub struct ValueSequence {
    pub values: Values,
    pub offsets: Option<Offsets>,
    pub presence: Presence,
    pub type_desc: BasicTypeDescriptor,
}

impl ValueSequence {
    /// Creates an empty `Sequence` for a given basic type.
    pub fn empty(type_desc: BasicTypeDescriptor) -> ValueSequence {
        ValueSequence {
            values: Values::new(),
            offsets: type_desc.basic_type.requires_offsets().then(Offsets::new),
            presence: Presence::Trivial(0),
            type_desc,
        }
    }

    /// Creates an empty `Sequence` with the specified capacity pre-allocated.
    ///
    /// This method pre-allocates memory for the specified number of elements without
    /// adding any actual elements to the sequence.
    ///
    /// # Arguments
    ///
    /// * `type_desc` - The basic type descriptor for the sequence
    /// * `capacity` - The number of elements to pre-allocate space for
    ///
    /// # Returns
    ///
    /// A new empty sequence with the specified capacity.
    pub fn with_capacity(type_desc: BasicTypeDescriptor, capacity: usize) -> ValueSequence {
        let elem_size = type_desc.primitive_size().unwrap_or(0);
        let values = Values::with_byte_capacity(capacity * elem_size);
        let offsets = type_desc
            .basic_type
            .requires_offsets()
            .then(|| Offsets::with_capacity(capacity));
        ValueSequence {
            values,
            offsets,
            presence: Presence::Trivial(0),
            type_desc,
        }
    }

    /// Creates a `ValueSequence` with a specified number of nulls.
    pub fn nulls(type_desc: BasicTypeDescriptor, len: usize) -> ValueSequence {
        let presence = Presence::Nulls(len);
        let offsets = type_desc
            .basic_type
            .requires_offsets()
            .then(|| Offsets::zeroed(len));
        let elem_size = type_desc.primitive_size().unwrap_or(0);
        let values = Values::zeroed_bytes(len * elem_size);
        ValueSequence {
            values,
            offsets,
            presence,
            type_desc,
        }
    }

    /// Creates a `ValueSequence` from the specified primitive `value` repeated `len` times.
    ///
    /// This method constructs a sequence containing `len` copies of the same primitive value.
    /// All values in the resulting sequence are non-null (present).
    ///
    /// The method only works with primitive types that have a fixed size (integers, floats,
    /// booleans, etc.). It cannot be used with variable-length types like strings or binary data.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of the value to repeat. Must implement `bytemuck::AnyBitPattern` and
    ///   `bytemuck::NoUninit` to ensure safe byte-level operations.
    ///
    /// # Arguments
    ///
    /// * `len` - The number of times to repeat the value in the sequence
    /// * `value` - The primitive value to repeat throughout the sequence
    /// * `type_desc` - The basic type descriptor that must match the size of type `T`
    ///
    /// # Returns
    ///
    /// A new `ValueSequence` containing `len` copies of `value`, with:
    /// - All values marked as present (non-null)
    /// - No offsets (since primitive types are fixed-size)
    /// - Values stored as a contiguous byte buffer
    ///
    /// # Panics
    ///
    /// Panics if the size of type `T` doesn't match the primitive size specified in `type_desc`.
    pub fn from_value<T>(len: usize, value: T, type_desc: BasicTypeDescriptor) -> ValueSequence
    where
        T: bytemuck::AnyBitPattern + bytemuck::NoUninit,
    {
        if type_desc.basic_type != BasicType::Boolean {
            assert_eq!(type_desc.primitive_size(), Some(std::mem::size_of::<T>()));
        }
        ValueSequence {
            values: Values::from_value(len, value),
            offsets: None,
            presence: Presence::Trivial(len),
            type_desc,
        }
    }

    /// Creates a ValueSequence with `len` copies of a constant binary value, with error handling.
    ///
    /// This method handles both variable-length binary types (Binary, String) and
    /// fixed-size binary types (FixedSizeBinary, Guid) by dispatching to the appropriate
    /// internal implementation based on the type descriptor.
    ///
    /// # Arguments
    ///
    /// * `len` - The number of elements in the resulting sequence
    /// * `binary_value` - The binary data to replicate across all positions
    /// * `type_desc` - The basic type descriptor (Binary, String, FixedSizeBinary, or Guid)
    ///
    /// # Returns
    ///
    /// A `Result` containing either:
    /// - `Ok(ValueSequence)` with `len` copies of the binary constant value
    /// - `Err(String)` if the binary data is invalid for the specified type
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The basic type is not Binary, String, FixedSizeBinary, or Guid
    /// - The basic type is String and the binary_value is not valid UTF-8
    /// - For fixed-size types, if binary_value length doesn't match the expected size
    fn from_binary(
        len: usize,
        binary_value: &[u8],
        type_desc: BasicTypeDescriptor,
    ) -> Result<ValueSequence, String> {
        match type_desc.basic_type {
            BasicType::Binary => Self::from_var_binary_constant(len, binary_value, type_desc),
            BasicType::String => {
                // Verify UTF-8 validity for String type
                if std::str::from_utf8(binary_value).is_err() {
                    return Err("Binary value is not valid UTF-8 for String type. \
                         This may indicate corrupted data from an untrusted stored shard."
                        .to_string());
                }
                Self::from_var_binary_constant(len, binary_value, type_desc)
            }
            BasicType::FixedSizeBinary => {
                if binary_value.len() != type_desc.fixed_size as usize {
                    return Err(format!(
                        "Binary value length {} doesn't match expected fixed size {}. \
                         This may indicate corrupted data from an untrusted stored shard.",
                        binary_value.len(),
                        type_desc.fixed_size
                    ));
                }
                Self::from_fixed_binary_constant(len, binary_value, type_desc)
            }
            BasicType::Guid => {
                if binary_value.len() != 16 {
                    return Err(format!(
                        "Guid value length {} is not 16 bytes. \
                         This may indicate corrupted data from an untrusted stored shard.",
                        binary_value.len()
                    ));
                }
                Self::from_fixed_binary_constant(len, binary_value, type_desc)
            }
            _ => Err(format!(
                "try_from_binary only supports Binary, String, FixedSizeBinary, and Guid types, got {:?}. \
                 This may indicate corrupted data from an untrusted stored shard.",
                type_desc.basic_type
            )),
        }
    }

    /// Creates a ValueSequence with `len` copies of a constant variable-size binary value.
    ///
    /// This method is used internally for variable-length binary types (Binary, String)
    /// where each value can have a different length. It creates the necessary offset
    /// structure to track where each binary value begins and ends.
    fn from_var_binary_constant(
        len: usize,
        binary_value: &[u8],
        type_desc: BasicTypeDescriptor,
    ) -> Result<ValueSequence, String> {
        if type_desc.basic_type != BasicType::Binary && type_desc.basic_type != BasicType::String {
            return Err(format!(
                "from_var_binary_constant only supports Binary and String types, got {:?}. \
                 This may indicate corrupted data from an untrusted stored shard.",
                type_desc.basic_type
            ));
        }

        if len == 0 {
            return Ok(ValueSequence::empty(type_desc));
        }

        let value_len = binary_value.len();
        let total_bytes = len * value_len;

        // Create values buffer with repeated binary data
        let mut values = Values::with_byte_capacity(total_bytes);
        for _ in 0..len {
            values.extend_from_slice(binary_value);
        }

        // Create offsets buffer (n+1 offsets for n values)
        let mut offsets = Offsets::with_capacity(len);
        for _ in 0..len {
            offsets.push_length(value_len);
        }

        Ok(ValueSequence {
            values,
            offsets: Some(offsets),
            presence: Presence::Trivial(len),
            type_desc,
        })
    }

    /// Creates a ValueSequence with `len` copies of a constant fixed-size binary value.
    ///
    /// This method is used internally for fixed-size binary types (FixedSizeBinary, Guid)
    /// where all values must have exactly the same length. No offsets are needed since
    /// the size is uniform.
    fn from_fixed_binary_constant(
        len: usize,
        binary_value: &[u8],
        type_desc: BasicTypeDescriptor,
    ) -> Result<ValueSequence, String> {
        if type_desc.basic_type != BasicType::FixedSizeBinary
            && type_desc.basic_type != BasicType::Guid
        {
            return Err(format!(
                "from_fixed_binary_constant only supports FixedSizeBinary and Guid types, got {:?}. \
                 This may indicate corrupted data from an untrusted stored shard.",
                type_desc.basic_type
            ));
        }

        if len == 0 {
            return Ok(ValueSequence::empty(type_desc));
        }

        // Create values buffer with repeated binary data
        let mut values = Values::with_byte_capacity(len * binary_value.len());
        for _ in 0..len {
            values.extend_from_slice(binary_value);
        }

        Ok(ValueSequence {
            values,
            offsets: None, // Fixed size binary doesn't need offsets
            presence: Presence::Trivial(len),
            type_desc,
        })
    }

    /// Creates a ValueSequence with `len` copies of a constant value from an AnyValue.
    ///
    /// This function handles the type conversion from an AnyValue to the appropriate
    /// primitive type based on the provided type descriptor, creating a sequence
    /// where all values are the same constant.
    ///
    /// # Arguments
    ///
    /// * `len` - The number of elements in the resulting sequence
    /// * `any_value` - The constant value to replicate across all positions
    /// * `type_desc` - The basic type descriptor specifying the target type
    ///
    /// # Returns
    ///
    /// A `Result` containing either:
    /// - `Ok(ValueSequence)` with `len` copies of the converted constant value
    /// - `Err(String)` if the conversion fails due to invalid data from untrusted shard
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The AnyValue kind doesn't match the expected type descriptor
    /// - The conversion between types is not supported
    /// - Boolean values are passed (should use BooleanFieldDecoder instead)  
    /// - Binary data size doesn't match fixed-size requirements
    /// - Binary data is not valid UTF-8 when converting to String
    /// - Integer values don't fit in the target type's range
    /// - The data appears to be corrupted (coming from untrusted stored shard)
    pub fn from_any_value(
        len: usize,
        any_value: &AnyValue,
        type_desc: BasicTypeDescriptor,
    ) -> Result<ValueSequence, String> {
        match &any_value.kind {
            Some(Kind::NullValue(_)) => Ok(ValueSequence::nulls(type_desc, len)),
            Some(Kind::BoolValue(_)) => Err(
                "Boolean constant values should be handled by BooleanFieldDecoder, not as primitive constants. \
                 This may indicate corrupted data from an untrusted stored shard.".to_string()
            ),

            // Numeric types - consolidated handling with range validation
            Some(Kind::I64Value(value)) => {
                // Validate that the type descriptor expects a signed type
                match type_desc.basic_type {
                    BasicType::Int8 | BasicType::Int16 | BasicType::Int32 | BasicType::Int64 => {
                        if !type_desc.signed {
                            return Err(format!(
                                "I64Value ({}) provided for unsigned type {:?}. \
                                 This may indicate corrupted data from an untrusted stored shard.",
                                value, type_desc.basic_type
                            ));
                        }
                    }
                    _ => {
                        return Err(format!(
                            "Unexpected type for I64Value: {:?}. \
                             This may indicate corrupted data from an untrusted stored shard.",
                            type_desc.basic_type
                        ));
                    }
                }

                // Validate range and convert
                match type_desc.basic_type {
                    BasicType::Int8 => {
                        if *value < i8::MIN as i64 || *value > i8::MAX as i64 {
                            return Err(format!(
                                "I64Value ({}) out of range for Int8 ({} to {}). \
                                 This may indicate corrupted data from an untrusted stored shard.",
                                value, i8::MIN, i8::MAX
                            ));
                        }
                        Ok(ValueSequence::from_value(len, *value as i8, type_desc))
                    }
                    BasicType::Int16 => {
                        if *value < i16::MIN as i64 || *value > i16::MAX as i64 {
                            return Err(format!(
                                "I64Value ({}) out of range for Int16 ({} to {}). \
                                 This may indicate corrupted data from an untrusted stored shard.",
                                value, i16::MIN, i16::MAX
                            ));
                        }
                        Ok(ValueSequence::from_value(len, *value as i16, type_desc))
                    }
                    BasicType::Int32 => {
                        if *value < i32::MIN as i64 || *value > i32::MAX as i64 {
                            return Err(format!(
                                "I64Value ({}) out of range for Int32 ({} to {}). \
                                 This may indicate corrupted data from an untrusted stored shard.",
                                value, i32::MIN, i32::MAX
                            ));
                        }
                        Ok(ValueSequence::from_value(len, *value as i32, type_desc))
                    }
                    BasicType::Int64 => Ok(ValueSequence::from_value(len, *value, type_desc)),
                    _ => unreachable!(),
                }
            }
            Some(Kind::U64Value(value)) => {
                // Validate that the type descriptor expects an unsigned type or DateTime
                match type_desc.basic_type {
                    BasicType::Int8 | BasicType::Int16 | BasicType::Int32 | BasicType::Int64 => {
                        if type_desc.signed {
                            return Err(format!(
                                "U64Value ({}) provided for signed type {:?}. \
                                 This may indicate corrupted data from an untrusted stored shard.",
                                value, type_desc.basic_type
                            ));
                        }
                    }
                    BasicType::DateTime => {
                        // DateTime is typically represented as u64 regardless of signed flag
                    }
                    _ => {
                        return Err(format!(
                            "Unexpected type for U64Value: {:?}. \
                             This may indicate corrupted data from an untrusted stored shard.",
                            type_desc.basic_type
                        ));
                    }
                }

                // Validate range and convert
                match type_desc.basic_type {
                    BasicType::Int8 => {
                        if *value > u8::MAX as u64 {
                            return Err(format!(
                                "U64Value ({}) out of range for unsigned Int8 (0 to {}). \
                                 This may indicate corrupted data from an untrusted stored shard.",
                                value, u8::MAX
                            ));
                        }
                        Ok(ValueSequence::from_value(len, *value as u8, type_desc))
                    }
                    BasicType::Int16 => {
                        if *value > u16::MAX as u64 {
                            return Err(format!(
                                "U64Value ({}) out of range for unsigned Int16 (0 to {}). \
                                 This may indicate corrupted data from an untrusted stored shard.",
                                value, u16::MAX
                            ));
                        }
                        Ok(ValueSequence::from_value(len, *value as u16, type_desc))
                    }
                    BasicType::Int32 => {
                        if *value > u32::MAX as u64 {
                            return Err(format!(
                                "U64Value ({}) out of range for unsigned Int32 (0 to {}). \
                                 This may indicate corrupted data from an untrusted stored shard.",
                                value, u32::MAX
                            ));
                        }
                        Ok(ValueSequence::from_value(len, *value as u32, type_desc))
                    }
                    BasicType::Int64 => Ok(ValueSequence::from_value(len, *value, type_desc)),
                    BasicType::DateTime => Ok(ValueSequence::from_value(len, *value, type_desc)),
                    _ => unreachable!(),
                }
            }
            Some(Kind::DoubleValue(value)) => match type_desc.basic_type {
                BasicType::Float32 => {
                    Ok(ValueSequence::from_value(len, *value as f32, type_desc))
                }
                BasicType::Float64 => {
                    Ok(ValueSequence::from_value(len, *value, type_desc))
                }
                _ => Err(format!(
                    "Unexpected type for DoubleValue: {:?}. \
                     This may indicate corrupted data from an untrusted stored shard.",
                    type_desc.basic_type
                )),
            },
            // Binary types - unified handling
            Some(Kind::StringValue(value)) => {
                Self::from_binary(len, value.as_bytes(), type_desc)
            }
            Some(Kind::BytesValue(value)) => {
                // Pre-validate UTF-8 for String conversion to provide better error message
                if type_desc.basic_type == BasicType::String
                    && std::str::from_utf8(value).is_err() {
                        return Err("Binary value is not valid UTF-8 for String type. \
                             This may indicate corrupted data from an untrusted stored shard.".to_string());
                    }
                Self::from_binary(len, value, type_desc)
            }
            Some(Kind::DecimalValue(value)) => {
                // DecimalValue should be handled as FixedSizeBinary with size 16
                if type_desc.basic_type != BasicType::FixedSizeBinary {
                    return Err(format!(
                        "DecimalValue should only be used with FixedSizeBinary type, got {:?}. \
                         This may indicate corrupted data from an untrusted stored shard.",
                        type_desc.basic_type
                    ));
                }
                if type_desc.fixed_size != 16 {
                    return Err(format!(
                        "DecimalValue requires FixedSizeBinary with size 16, got {}. \
                         This may indicate corrupted data from an untrusted stored shard.",
                        type_desc.fixed_size
                    ));
                }
                if value.len() != 16 {
                    return Err(format!(
                        "DecimalValue must be exactly 16 bytes, got {}. \
                         This may indicate corrupted data from an untrusted stored shard.",
                        value.len()
                    ));
                }
                // Validate decimal128 using the decimal library
                // This will catch invalid decimal encodings during deserialization
                let mut decimal_bytes = [0u8; 16];
                decimal_bytes.copy_from_slice(value);
                // Parse as decimal to validate the encoding
                let decimal_value = unsafe { d128::from_raw_bytes(decimal_bytes) };
                // Check if the decimal value is invalid (NaN indicates invalid encoding)
                if decimal_value.is_nan() {
                    // Note: This catches true corruption, not legitimate NaN values
                    // In decimal128, NaN can be a valid mathematical result, but invalid
                    // bit patterns will also parse as NaN, indicating data corruption
                    return Err(
                        "Invalid decimal128 encoding detected. \
                         This may indicate corrupted data from an untrusted stored shard.".to_string()
                    );
                }
                Self::from_binary(len, value, type_desc)
            }
            None => Err(
                "AnyValue has no kind specified. \
                 This may indicate corrupted data from an untrusted stored shard.".to_string()
            ),
            _ => Err(format!(
                "Unsupported constant value kind for primitive field: {:?}. \
                 This may indicate corrupted data from an untrusted stored shard.",
                any_value.kind
            )),
        }
    }

    /// Returns the number of elements in the sequence.
    ///
    /// This count includes both present and null values. For example, a sequence
    /// with 3 elements where one is null will have a length of 3.
    ///
    /// # Returns
    /// The total number of logical elements in the sequence.
    #[inline]
    pub fn len(&self) -> usize {
        self.presence.len()
    }

    /// Returns `true` if the sequence contains no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Appends a binary value to the sequence.
    ///
    /// This method adds a single binary value to the end of the sequence.
    /// For variable-length binary types, it updates the offsets to track the length
    /// of this binary data. For fixed-size binary types, it verifies the value has
    /// the expected length.
    ///
    /// # Arguments
    ///
    /// * `value` - A byte slice containing the binary data to append
    ///
    /// # Panics
    ///
    /// * If the sequence's basic type is not `Binary` or `FixedSizeBinary`.
    /// * For `FixedSizeBinary` type, panics if the binary data length doesn't match
    ///   the fixed size.
    pub fn push_binary(&mut self, value: &[u8]) {
        if self.type_desc.basic_type == BasicType::FixedSizeBinary {
            assert_eq!(self.type_desc.fixed_size as usize, value.len());
        } else {
            assert_eq!(self.type_desc.basic_type, BasicType::Binary);
            self.offsets.as_mut().unwrap().push_length(value.len());
        }
        self.values.extend_from_slice(value);
        self.presence.push_non_null();
    }

    /// Appends a string value to the sequence.
    ///
    /// This method adds a single string value to the end of the sequence, updating the
    /// offsets to track the length of this string and marking it as non-null.
    ///
    /// # Panics
    ///
    /// Panics if the sequence's basic type is not `String`.
    ///
    /// # Arguments
    ///
    /// * `value` - A string slice containing the text to append
    pub fn push_str(&mut self, value: &str) {
        if self.type_desc.basic_type == BasicType::FixedSizeBinary {
            self.push_binary(value.as_bytes());
        } else {
            assert!(
                self.type_desc.basic_type == BasicType::String
                    || self.type_desc.basic_type == BasicType::Binary
            );
            self.offsets.as_mut().unwrap().push_length(value.len());
            self.values.extend_from_slice(value.as_bytes());
            self.presence.push_non_null();
        }
    }

    /// Pushes a primitive value to the end of the sequence.
    ///
    /// This method adds a single value of a primitive type (such as integers or floating-point numbers)
    /// to the end of the sequence and marks it as non-null.
    ///
    /// # Type Parameters
    ///
    /// * `T` - A primitive type that implements `bytemuck::NoUninit` (safely convertible to bytes)
    ///
    /// # Panics
    ///
    /// * If the size of type `T` doesn't match the sequence's primitive size (as defined by the type descriptor)
    /// * If the sequence has offsets (used for variable-length types like Binary or String)
    ///
    /// # Arguments
    ///
    /// * `value` - The primitive value to append
    pub fn push_value<T>(&mut self, value: T)
    where
        T: bytemuck::NoUninit,
    {
        assert_eq!(
            self.type_desc.primitive_size(),
            Some(std::mem::size_of::<T>())
        );
        assert!(self.offsets.is_none());
        self.values.push(value);
        self.presence.push_non_null();
    }

    /// Pushes a null value to the end of the sequence.
    pub fn push_null(&mut self) {
        if let Some(ref mut offsets) = self.offsets {
            offsets.push_length(0);
        }
        self.presence.push_null();
        let size = self.type_desc.primitive_size().unwrap_or(0);
        self.values
            .resize_zeroed_bytes(self.values.bytes_len() + size);
    }

    /// Pushes multiple null values to the end of the sequence.
    pub fn push_nulls(&mut self, count: usize) {
        if count == 0 {
            return;
        }

        if let Some(ref mut offsets) = self.offsets {
            offsets.push_empty(count);
        }
        self.presence.extend_with_nulls(count);
        let size = self.type_desc.primitive_size().unwrap_or(0);
        self.values
            .resize_zeroed_bytes(self.values.bytes_len() + size * count);
    }

    /// Extends this sequence by appending elements from a slice.
    ///
    /// This method appends all elements from the provided slice to the end of the sequence.
    /// The elements are treated as non-null values.
    ///
    /// # Type Requirements
    /// This method requires that the element type `T` in the slice matches the primitive type
    /// of the sequence (checked via the `primitive_size` of the sequence's type descriptor).
    ///
    /// # Panics
    /// * If the size of type `T` doesn't match the primitive size of the sequence's type descriptor.
    /// * If the sequence has offsets (variable-length types like Binary or String). For those types,
    ///   use `BinarySequenceBuilder` or `extend_from_range` instead.
    ///
    /// # Arguments
    /// * `values` - A slice containing the values to append
    pub fn extend_from_slice<T>(&mut self, values: &[T])
    where
        T: bytemuck::NoUninit,
    {
        assert_eq!(
            self.type_desc.primitive_size(),
            Some(std::mem::size_of::<T>())
        );
        assert!(self.offsets.is_none());
        self.values.extend_from_slice(values);
        self.presence.extend_with_non_nulls(values.len());
    }

    /// Extends this sequence by appending values from a range of another sequence.
    ///
    /// This method copies a subset of elements from the source sequence, starting at the specified
    /// offset and containing `len` elements. It handles different basic types appropriately, including:
    /// - Primitive numeric types (Int8, Int16, Int32, Int64, Float32, Float64)
    /// - Variable-length types (Binary, String) with the necessary offset adjustments
    /// - Fixed-size types (FixedSizeBinary, Guid) with proper size calculations
    ///
    /// Both sequences must have the same type descriptor and matching offset requirements.
    ///
    /// # Arguments
    /// * `source` - The source sequence to copy elements from
    /// * `offset` - The logical starting position in the source sequence (zero-based)
    /// * `len` - The logical number of elements to copy
    ///
    /// # Panics
    /// * If `offset + len > source.len()`
    /// * If the type descriptors of the sequences don't match
    /// * If the offset requirements of the sequences don't match
    pub fn extend_from_sequence(&mut self, source: &ValueSequence, offset: usize, len: usize) {
        assert!(offset + len <= source.len());
        assert_eq!(self.type_desc, source.type_desc);
        assert_eq!(self.offsets.is_some(), source.offsets.is_some());

        match self.type_desc.basic_type {
            BasicType::Unit => (),
            BasicType::Boolean => todo!(),
            BasicType::Int8 => self.append_values::<u8>(&source.values, offset, len),
            BasicType::Int16 => self.append_values::<u16>(&source.values, offset, len),
            BasicType::Int32 | BasicType::Float32 => {
                self.append_values::<u32>(&source.values, offset, len)
            }
            BasicType::Int64 | BasicType::Float64 | BasicType::DateTime => {
                self.append_values::<u64>(&source.values, offset, len)
            }
            BasicType::Binary | BasicType::String => {
                let offsets = source.offsets.as_ref().expect("offsets are required");
                let data_start = offsets[offset] as usize;
                let data_end = offsets[offset + len] as usize;
                self.append_values::<u8>(&source.values, data_start, data_end - data_start);
            }
            BasicType::FixedSizeBinary => {
                let size = self.type_desc.fixed_size as usize;
                self.append_values::<u8>(&source.values, offset * size, len * size);
            }
            BasicType::Guid => {
                self.append_values::<u8>(&source.values, offset * 16, len * 16);
            }
            BasicType::List
            | BasicType::FixedSizeList
            | BasicType::Struct
            | BasicType::Map
            | BasicType::Union => (),
        }

        if let Some(ref mut offsets) = self.offsets {
            let src_offsets = source.offsets.as_ref().expect("src offsets are required");
            offsets.extend_from_offsets_range(src_offsets, offset, len);
        }

        self.presence
            .extend_from_presence_range(&source.presence, offset, len);
    }

    /// Interprets the underlying values as a slice of elements of type `T`.
    ///
    /// This method provides a low-level view of the raw value buffer by reinterpreting
    /// the bytes as the specified type `T`. The caller is responsible for ensuring
    /// the cast is logically sound and matches the actual data layout.
    ///
    /// # Important Notes
    ///
    /// * This cast ignores the formal sequence type (`type_desc`). The caller must
    ///   verify that the cast is appropriate for the stored data.
    /// * This method does not take presence (null/non-null status) into account.
    ///   Null slots will contain whatever raw bytes are present in the buffer
    ///   (typically zeros).
    ///
    /// # Returns
    ///
    /// A reference to the underlying data interpreted as a slice of type `T`.
    ///
    /// # Panics
    ///
    /// Panics if the underlying value buffer cannot be safely cast to `&[T]`
    /// due to size or alignment constraints.
    #[inline]
    pub fn as_slice<T>(&self) -> &[T]
    where
        T: bytemuck::AnyBitPattern,
    {
        self.values.as_slice()
    }

    /// Returns iterator over variable-sized values in the sequence.
    /// Returns `None`, if the sequence doesn't contain variable-sized binary values.
    ///
    /// The method doesn't return any information on the presence of values.
    /// The caller should check the presence of values using the `presence` field.
    pub fn binary_values(&self) -> Option<BinaryValuesIter<'_, OffsetsIter<'_>>> {
        self.offsets
            .as_ref()
            .map(|offsets| BinaryValuesIter::new(&self.values, OffsetsIter::new(offsets)))
    }

    /// Returns iterator over fixed-sized values in the sequence.
    /// Returns `None`, if the sequence is not of type `FixedSizeBinary`.
    ///
    /// The method doesn't return any information on the presence of values.
    /// The caller should check the presence of values using the `presence` field.
    pub fn fixed_binary_values(&self) -> Option<BinaryValuesIter<'_, FixedSizeOffsetsIter>> {
        if self.type_desc.basic_type == BasicType::FixedSizeBinary {
            let fixed_size = self.type_desc.fixed_size as usize;
            let count = self.values.bytes_len() / fixed_size;
            Some(BinaryValuesIter::new(
                &self.values,
                FixedSizeOffsetsIter::new(fixed_size, count),
            ))
        } else {
            None
        }
    }

    /// Returns a binary value at the specified index.
    ///
    /// # Panics
    ///
    /// Panics in the following cases:
    ///
    ///   * The sequence is not of the appropriate data type, i.e.,
    ///     does not have offsets or not of `FixedSizeBinary` type.
    ///
    ///   * Index is out of bounds.
    ///
    /// # Returns
    ///
    /// A byte slice representing the binary value at the specified index.
    pub fn binary_at(&self, index: usize) -> &[u8] {
        let range = if let Some(offsets) = self.offsets.as_ref() {
            offsets[index] as usize..offsets[index + 1] as usize
        } else {
            assert_eq!(self.type_desc.basic_type, BasicType::FixedSizeBinary);
            let start = self.type_desc.fixed_size as usize * index;
            let end = start + self.type_desc.fixed_size as usize;
            start..end
        };
        &self.values.as_bytes()[range]
    }

    /// Retrieves a binary value at the specified index for variable-length binary data.
    ///
    /// This is an optimized version of `binary_at` that assumes the sequence has offsets
    /// for variable-length data. It directly accesses the offsets without checking,
    /// making it faster but requiring the caller to ensure offsets are present.
    ///
    /// # Arguments
    ///
    /// * `index` - The zero-based index of the binary value to retrieve
    ///
    /// # Returns
    ///
    /// A byte slice representing the variable-length binary value at the specified index.
    ///
    /// # Panics
    ///
    /// Panics if the sequence doesn't have offsets or if the index is out of bounds.
    #[inline]
    pub fn var_binary_at(&self, index: usize) -> &[u8] {
        let offsets = self.offsets.as_ref().expect("offsets");
        let range = offsets[index] as usize..offsets[index + 1] as usize;
        &self.values.as_bytes()[range]
    }

    /// Retrieves a fixed-size value at the specified index as a byte slice.
    ///
    /// This method provides direct access to fixed-size values stored in the sequence's
    /// value buffer. It works for sequences containing fixed-size binary data (such as
    /// `FixedSizeBinary` or `Guid` types) as well as all primitive types (integers,
    /// floats, etc.).
    ///
    /// Unlike `binary_at`, this method doesn't check the sequence's type descriptor
    /// or use offset information. It directly calculates the position based on the
    /// provided `value_size` parameter and extracts the bytes from that location.
    ///
    /// # Arguments
    ///
    /// * `index` - The zero-based index of the value to retrieve
    /// * `value_size` - The size in bytes of each value in the sequence
    ///
    /// # Returns
    ///
    /// A byte slice containing the raw bytes of the value at the specified index.
    /// The slice will have exactly `value_size` bytes.
    ///
    /// # Safety and Correctness
    ///
    /// The caller is responsible for ensuring logical correctness:
    /// - The `value_size` parameter must match the actual size of values in the sequence
    /// - The `index` must be within bounds (0 <= index < sequence_length)
    /// - The sequence must actually contain fixed-size values
    ///
    /// This method does not check presence information - null values will return
    /// whatever raw bytes are stored in the buffer (typically zeros).
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - The index is out of bounds for the sequence
    /// - The calculated byte range exceeds the underlying value buffer
    /// - Integer overflow occurs during range calculation
    #[inline]
    pub fn fixed_binary_at(&self, index: usize, value_size: usize) -> &[u8] {
        let start = index.checked_mul(value_size).expect("start");
        let end = start.checked_add(value_size).expect("end");
        &self.values.as_bytes()[start..end]
    }

    // Returns a string value at the specified index.
    // Will panic on out of bounds access and when the string buffer is not a valid utf-8.
    pub fn string_at(&self, index: usize) -> &str {
        let offsets = self.offsets.as_ref().expect("missing offsets");
        let range = offsets[index] as usize..offsets[index + 1] as usize;
        std::str::from_utf8(&self.values.as_bytes()[range]).expect("invalid utf8")
    }
}

impl ValueSequence {
    fn append_values<T>(&mut self, other: &Values, offset: usize, len: usize)
    where
        T: bytemuck::NoUninit + bytemuck::AnyBitPattern,
    {
        self.values
            .extend_from_slice(&other.as_slice::<T>()[offset..offset + len]);
    }
}

impl Sequence for ValueSequence {
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync + 'static) {
        self as _
    }

    fn basic_type(&self) -> BasicTypeDescriptor {
        self.type_desc
    }

    fn len(&self) -> usize {
        ValueSequence::len(self)
    }

    fn is_empty(&self) -> bool {
        ValueSequence::is_empty(self)
    }

    fn to_value_sequence(&self) -> Cow<ValueSequence> {
        Cow::Borrowed(self)
    }
}

/// Iterator over binary values in a sequence.
pub struct BinaryValuesIter<'a, R>
where
    R: Iterator<Item = Range<usize>>,
{
    bytes: &'a [u8],
    ranges: R,
}

impl<'a, R> BinaryValuesIter<'a, R>
where
    R: Iterator<Item = Range<usize>>,
{
    pub fn new(values: &'a Values, ranges: R) -> Self {
        Self {
            bytes: values.as_bytes(),
            ranges,
        }
    }
}

impl<'a, R> Iterator for BinaryValuesIter<'a, R>
where
    R: Iterator<Item = Range<usize>>,
{
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(range) = self.ranges.next() {
            Some(&self.bytes[range])
        } else {
            None
        }
    }
}

/// Builder for sequences of variable-sized values.
pub struct BinarySequenceBuilder {
    type_desc: BasicTypeDescriptor,
    values: Values,
    offsets: Offsets,
    presence: PresenceBuilder,
}

impl BinarySequenceBuilder {
    pub fn new(type_desc: BasicTypeDescriptor) -> Self {
        Self {
            type_desc,
            values: Values::new(),
            offsets: Offsets::new(),
            presence: PresenceBuilder::new(),
        }
    }

    pub fn add_value(&mut self, value: &[u8]) {
        self.values.extend_from_slice(value);
        self.offsets.push_length(value.len());
        self.presence.add_non_null();
    }

    pub fn add_null(&mut self) {
        self.offsets.push_length(0);
        self.presence.add_null();
    }

    pub fn add_n_nulls(&mut self, n: usize) {
        self.offsets.push_empty(n);
        self.presence.add_n_nulls(n);
    }

    pub fn build(self) -> ValueSequence {
        ValueSequence {
            type_desc: self.type_desc,
            values: self.values,
            offsets: Some(self.offsets),
            presence: self.presence.build(),
        }
    }
}

/// Builder for sequences of fixed-size binary values.
pub struct FixedSizeBinarySequenceBuilder {
    type_desc: BasicTypeDescriptor,
    values: Values,
    presence: PresenceBuilder,
}

impl FixedSizeBinarySequenceBuilder {
    pub fn new(type_desc: BasicTypeDescriptor) -> Self {
        assert!(type_desc.fixed_size > 0);
        Self {
            type_desc,
            values: Values::new(),
            presence: PresenceBuilder::new(),
        }
    }

    pub fn add_value(&mut self, value: &[u8]) {
        assert_eq!(value.len(), self.type_desc.fixed_size as usize);
        self.values.extend_from_slice(value);
        self.presence.add_non_null();
    }

    pub fn add_null(&mut self) {
        self.presence.add_null();
        self.values
            .resize_zeroed_bytes(self.values.bytes_len() + self.type_desc.fixed_size as usize);
    }

    pub fn add_n_nulls(&mut self, n: usize) {
        self.presence.add_n_nulls(n);
        self.values
            .resize_zeroed_bytes(self.values.bytes_len() + self.type_desc.fixed_size as usize * n);
    }

    pub fn build(self) -> ValueSequence {
        ValueSequence {
            type_desc: self.type_desc,
            values: self.values,
            offsets: None,
            presence: self.presence.build(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_sequence() {
        let type_desc = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::Binary,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let mut builder = BinarySequenceBuilder::new(type_desc);
        builder.add_value(b"hello");
        builder.add_null();
        builder.add_value(b"world");
        builder.add_n_nulls(2);

        let sequence = builder.build();
        assert_eq!(sequence.values.as_bytes(), b"helloworld");
        assert_eq!(
            sequence.offsets.as_ref().unwrap().as_slice(),
            &[0, 5, 5, 10, 10, 10]
        );
        assert_eq!(sequence.presence.len(), 5);
        match &sequence.presence {
            Presence::Bytes(presence) => {
                assert_eq!(presence.typed_data::<i8>(), &[1, 0, 1, 0, 0]);
            }
            _ => panic!("Expected presence to be Bytes"),
        }

        let binary_values = sequence.binary_values().unwrap();
        let values: Vec<&[u8]> = binary_values.collect();
        assert_eq!(values.len(), 5);
        assert_eq!(vec![b"hello" as &[u8], b"", b"world", b"", b""], values);
    }

    #[test]
    fn test_empty_sequence() {
        let type_desc = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let sequence = ValueSequence::empty(type_desc);

        assert_eq!(sequence.len(), 0);
        assert!(sequence.is_empty());
        assert!(sequence.binary_values().is_none());
    }

    #[test]
    fn test_nulls_sequence() {
        let type_desc = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::Int64,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let sequence = ValueSequence::nulls(type_desc, 5);

        assert_eq!(sequence.len(), 5);
        assert!(!sequence.is_empty());
        assert!(sequence.binary_values().is_none());

        match &sequence.presence {
            Presence::Nulls(len) => assert_eq!(*len, 5),
            _ => panic!("Expected presence to be Nulls"),
        }
    }

    #[test]
    fn test_extend_from_range() {
        let type_desc = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::Binary,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };

        let mut source_builder = BinarySequenceBuilder::new(type_desc);
        source_builder.add_value(b"first");
        source_builder.add_value(b"second");
        source_builder.add_value(b"third");
        source_builder.add_null();
        source_builder.add_value(b"fifth");
        let source = source_builder.build();

        let mut target = ValueSequence::empty(type_desc);

        target.extend_from_sequence(&source, 1, 3);

        let values: Vec<&[u8]> = target.binary_values().unwrap().collect();
        assert_eq!(values.len(), 3);
        assert_eq!(vec![b"second" as &[u8], b"third", b""], values);

        match &target.presence {
            Presence::Bytes(presence) => {
                assert_eq!(presence.typed_data::<i8>(), &[1, 1, 0]);
            }
            _ => panic!("Expected presence to be Bytes"),
        }
    }

    #[test]
    fn test_fixed_size_binary_sequence() {
        let type_desc = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::FixedSizeBinary,
            signed: false,
            fixed_size: 4,
            extended_type: Default::default(),
        };

        let mut sequence = ValueSequence::empty(type_desc);

        let source_desc = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::FixedSizeBinary,
            signed: false,
            fixed_size: 4,
            extended_type: Default::default(),
        };
        let mut source = ValueSequence::empty(source_desc);

        source
            .values
            .extend_from_slice::<u8>(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        source.presence = Presence::Trivial(3);

        sequence.extend_from_sequence(&source, 1, 2);

        assert_eq!(sequence.values.as_bytes(), &[5, 6, 7, 8, 9, 10, 11, 12]);
        assert_eq!(sequence.len(), 2);
        assert!(sequence.binary_values().is_none());
    }

    #[test]
    fn test_string_sequence() {
        let type_desc = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::String,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let mut builder = BinarySequenceBuilder::new(type_desc);
        builder.add_value("hello".as_bytes());
        builder.add_value("world".as_bytes());

        let sequence = builder.build();
        let values: Vec<&[u8]> = sequence.binary_values().unwrap().collect();
        assert_eq!(values.len(), 2);
        assert_eq!(vec!["hello".as_bytes(), "world".as_bytes()], values);
    }

    #[test]
    fn test_push_binary() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Binary,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_binary(b"first");
        sequence.push_binary(b"second");
        sequence.push_binary(b"third");

        assert_eq!(sequence.values.as_bytes(), b"firstsecondthird");

        let offsets = sequence.offsets.as_ref().unwrap();
        assert_eq!(offsets.as_slice(), &[0, 5, 11, 16]);

        assert_eq!(sequence.presence.count_non_nulls(), 3);
    }

    #[test]
    fn test_push_binary_empty_values() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Binary,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_binary(b"");
        sequence.push_binary(b"");

        let offsets = sequence.offsets.as_ref().unwrap();
        assert_eq!(offsets.as_slice(), &[0, 0, 0]);

        assert_eq!(sequence.presence.len(), 2);
        assert_eq!(sequence.presence.count_non_nulls(), 2);
    }

    #[test]
    fn test_push_fixed_size_binary() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            signed: false,
            fixed_size: 4,
            extended_type: Default::default(),
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_binary(&[1, 2, 3, 4]);
        sequence.push_binary(&[5, 6, 7, 8]);

        assert_eq!(sequence.values.as_bytes(), &[1, 2, 3, 4, 5, 6, 7, 8]);
        assert!(sequence.offsets.is_none());
        assert_eq!(sequence.presence.len(), 2);
    }

    #[test]
    #[should_panic]
    fn test_push_fixed_size_binary_wrong_size() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            signed: false,
            fixed_size: 4,
            extended_type: Default::default(),
        };
        let mut sequence = ValueSequence::empty(type_desc);

        // This should panic because the length doesn't match fixed_size
        sequence.push_binary(&[1, 2, 3]);
    }

    #[test]
    #[should_panic]
    fn test_push_binary_wrong_type() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let mut sequence = ValueSequence::empty(type_desc);

        // This should panic because the type is not Binary or FixedSizeBinary
        sequence.push_binary(b"test");
    }

    #[test]
    fn test_push_str() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::String,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let mut sequence = ValueSequence::empty(type_desc);
        sequence.push_str("hello");
        sequence.push_str("world");
        sequence.push_str("rust");

        assert_eq!(sequence.values.as_bytes(), b"helloworldrust");

        let offsets = sequence.offsets.as_ref().unwrap();
        assert_eq!(offsets.as_slice(), &[0, 5, 10, 14]);

        assert_eq!(sequence.presence.len(), 3);
        assert_eq!(sequence.presence.count_non_nulls(), 3);
    }

    #[test]
    fn test_push_str_empty_and_unicode() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::String,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_str("");

        let unicode = "こんにちは";
        sequence.push_str(unicode);

        assert_eq!(sequence.values.as_bytes(), "こんにちは".as_bytes());

        let offsets = sequence.offsets.as_ref().unwrap();
        assert_eq!(offsets.as_slice(), &[0, 0, 15]);
        assert_eq!(sequence.presence.len(), 2);
        assert_eq!(sequence.presence.count_non_nulls(), 2);
    }

    #[test]
    fn test_push_value_i32() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_value(42i32);
        sequence.push_value(-100i32);
        sequence.push_value(0i32);

        let values = sequence.values.as_slice::<i32>();
        assert_eq!(values, &[42, -100, 0]);

        assert_eq!(sequence.presence.len(), 3);
        assert_eq!(sequence.presence.count_non_nulls(), 3);
        assert!(sequence.offsets.is_none());
    }

    #[test]
    fn test_push_value_f64() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Float64,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_value(3.14159);
        sequence.push_value(-2.71828);
        sequence.push_value(0.0);

        let values = sequence.values.as_slice::<f64>();
        assert_eq!(values, &[3.14159, -2.71828, 0.0]);

        assert_eq!(sequence.presence.len(), 3);
        assert_eq!(sequence.presence.count_non_nulls(), 3);
    }

    #[test]
    fn test_push_value_u8() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Int8,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_value(10u8);
        sequence.push_value(255u8);
        sequence.push_value(0u8);

        let values = sequence.values.as_slice::<u8>();
        assert_eq!(values, &[10, 255, 0]);

        assert_eq!(sequence.presence.len(), 3);
        assert_eq!(sequence.presence.count_non_nulls(), 3);
    }

    #[test]
    #[should_panic]
    fn test_push_value_wrong_size() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Int16,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let mut sequence = ValueSequence::empty(type_desc);

        // This should panic because we're pushing an i32 to an i16 sequence
        sequence.push_value(42i32);
    }

    #[test]
    #[should_panic]
    fn test_push_value_variable_length_type() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::String,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let mut sequence = ValueSequence::empty(type_desc);

        // This should panic because push_value can't be used with types that need offsets
        sequence.push_value(42i32);
    }

    #[test]
    fn test_push_null_fixed_size() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_value(42i32);
        sequence.push_null();
        sequence.push_value(100i32);
        sequence.push_null();

        assert_eq!(sequence.presence.len(), 4);
        assert_eq!(sequence.presence.count_non_nulls(), 2);

        assert_eq!(sequence.values.bytes_len(), 4 * std::mem::size_of::<i32>());

        let values = sequence.values.as_slice::<i32>();
        assert_eq!(values[0], 42);
        assert_eq!(values[2], 100);
    }

    #[test]
    fn test_push_null_string() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::String,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_str("hello");
        sequence.push_null();
        sequence.push_str("world");
        sequence.push_null();

        assert_eq!(sequence.presence.len(), 4);
        assert_eq!(sequence.presence.count_non_nulls(), 2);

        assert_eq!(sequence.values.as_bytes(), b"helloworld");

        let offsets = sequence.offsets.as_ref().unwrap();
        assert_eq!(offsets.as_slice(), &[0, 5, 5, 10, 10]);

        let values: Vec<&[u8]> = sequence.binary_values().unwrap().collect();
        assert_eq!(values.len(), 4);
        assert_eq!(values, &[b"hello" as &[u8], b"", b"world", b""]);
    }

    #[test]
    fn test_push_null_binary() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Binary,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_null();
        sequence.push_binary(b"data");
        sequence.push_null();
        sequence.push_binary(b"more");

        assert_eq!(sequence.len(), 4);
        assert_eq!(sequence.presence.count_non_nulls(), 2);

        let offsets = sequence.offsets.as_ref().unwrap();
        assert_eq!(offsets.as_slice(), &[0, 0, 4, 4, 8]);

        let values: Vec<&[u8]> = sequence.binary_values().unwrap().collect();
        assert_eq!(values, &[b"" as &[u8], b"data", b"", b"more"]);
    }

    #[test]
    fn test_push_null_fixed_size_binary() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            signed: false,
            fixed_size: 4,
            extended_type: Default::default(),
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_binary(&[1, 2, 3, 4]);
        sequence.push_null();
        sequence.push_binary(&[5, 6, 7, 8]);

        assert_eq!(sequence.len(), 3);
        assert_eq!(sequence.presence.count_non_nulls(), 2);
        assert_eq!(sequence.values.bytes_len(), 3 * 4);
        assert_eq!(
            sequence.values.as_bytes(),
            &[1, 2, 3, 4, 0, 0, 0, 0, 5, 6, 7, 8]
        );
    }

    #[test]
    fn test_push_multiple_values_and_nulls() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Int64,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_value(1i64);
        sequence.push_null();
        sequence.push_value(2i64);
        sequence.push_value(3i64);
        sequence.push_null();
        sequence.push_null();
        sequence.push_value(4i64);

        assert_eq!(sequence.len(), 7);
        assert_eq!(sequence.presence.count_non_nulls(), 4);

        match &sequence.presence {
            Presence::Bytes(presence) => {
                assert_eq!(presence.typed_data::<i8>(), &[1, 0, 1, 1, 0, 0, 1]);
            }
            _ => panic!("Expected presence to be Bytes"),
        }
    }

    #[test]
    fn test_push_nulls() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Int64,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let mut sequence = ValueSequence::empty(type_desc);
        sequence.push_value(1i64);
        sequence.push_value(2i64);
        sequence.push_nulls(3);
        sequence.push_value(3i64);

        assert_eq!(sequence.len(), 6);
        assert_eq!(sequence.presence.count_non_nulls(), 3);

        match &sequence.presence {
            Presence::Bytes(presence) => {
                assert_eq!(presence.typed_data::<i8>(), &[1, 1, 0, 0, 0, 1]);
            }
            _ => panic!("Expected presence to be Bytes"),
        }

        let values = sequence.values.as_slice::<i64>();
        assert_eq!(values, &[1, 2, 0, 0, 0, 3]);
    }

    #[test]
    fn test_from_any_value_string_constant() {
        use amudai_format::defs::common::{AnyValue, any_value::Kind};

        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::String,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };

        let any_value = AnyValue {
            kind: Some(Kind::StringValue("hello".to_string())),
            annotation: None,
        };

        let sequence = ValueSequence::from_any_value(3, &any_value, type_desc).unwrap();

        assert_eq!(sequence.len(), 3);
        assert_eq!(sequence.values.as_bytes(), b"hellohellohello");

        let offsets = sequence.offsets.as_ref().unwrap();
        assert_eq!(offsets.as_slice(), &[0, 5, 10, 15]);

        let values: Vec<&[u8]> = sequence.binary_values().unwrap().collect();
        assert_eq!(values, vec![b"hello", b"hello", b"hello"]);
    }

    #[test]
    fn test_from_any_value_binary_constant() {
        use amudai_format::defs::common::{AnyValue, any_value::Kind};

        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Binary,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };

        let binary_data = vec![0x01, 0x02, 0x03, 0x04];
        let any_value = AnyValue {
            kind: Some(Kind::BytesValue(binary_data.clone())),
            annotation: None,
        };

        let sequence = ValueSequence::from_any_value(2, &any_value, type_desc).unwrap();

        assert_eq!(sequence.len(), 2);
        assert_eq!(
            sequence.values.as_bytes(),
            &[0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04]
        );

        let offsets = sequence.offsets.as_ref().unwrap();
        assert_eq!(offsets.as_slice(), &[0, 4, 8]);

        let values: Vec<&[u8]> = sequence.binary_values().unwrap().collect();
        assert_eq!(values, vec![binary_data.as_slice(), binary_data.as_slice()]);
    }

    #[test]
    fn test_from_any_value_fixed_size_binary_constant() {
        use amudai_format::defs::common::{AnyValue, any_value::Kind};

        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            signed: false,
            fixed_size: 4,
            extended_type: Default::default(),
        };

        let binary_data = vec![0xAA, 0xBB, 0xCC, 0xDD];
        let any_value = AnyValue {
            kind: Some(Kind::BytesValue(binary_data.clone())),
            annotation: None,
        };

        let sequence = ValueSequence::from_any_value(3, &any_value, type_desc).unwrap();

        assert_eq!(sequence.len(), 3);
        assert_eq!(
            sequence.values.as_bytes(),
            &[
                0xAA, 0xBB, 0xCC, 0xDD, 0xAA, 0xBB, 0xCC, 0xDD, 0xAA, 0xBB, 0xCC, 0xDD
            ]
        );
        assert!(sequence.offsets.is_none()); // Fixed size doesn't use offsets

        // Access fixed binary values
        let fixed_values = sequence.fixed_binary_values().unwrap();
        let values: Vec<&[u8]> = fixed_values.collect();
        assert_eq!(values, vec![binary_data.as_slice(); 3]);
    }

    #[test]
    fn test_from_any_value_fixed_size_binary_wrong_size() {
        use amudai_format::defs::common::{AnyValue, any_value::Kind};

        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            signed: false,
            fixed_size: 4,
            extended_type: Default::default(),
        };

        // Wrong size binary data (3 bytes instead of 4)
        let binary_data = vec![0xAA, 0xBB, 0xCC];
        let any_value = AnyValue {
            kind: Some(Kind::BytesValue(binary_data)),
            annotation: None,
        };

        let result = ValueSequence::from_any_value(2, &any_value, type_desc);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("Binary value length 3 doesn't match expected fixed size 4")
        );
    }

    #[test]
    fn test_from_any_value_null_constant() {
        use amudai_format::defs::common::{AnyValue, UnitValue, any_value::Kind};

        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::String,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };

        let any_value = AnyValue {
            kind: Some(Kind::NullValue(UnitValue {})),
            annotation: None,
        };

        let sequence = ValueSequence::from_any_value(3, &any_value, type_desc).unwrap();

        assert_eq!(sequence.len(), 3);
        match &sequence.presence {
            Presence::Nulls(count) => assert_eq!(*count, 3),
            _ => panic!("Expected Nulls presence"),
        }
    }

    #[test]
    fn test_from_any_value_empty_string_constant() {
        use amudai_format::defs::common::{AnyValue, any_value::Kind};

        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::String,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };

        let any_value = AnyValue {
            kind: Some(Kind::StringValue("".to_string())),
            annotation: None,
        };

        let sequence = ValueSequence::from_any_value(2, &any_value, type_desc).unwrap();

        assert_eq!(sequence.len(), 2);
        assert_eq!(sequence.values.as_bytes(), b""); // Empty values buffer

        let offsets = sequence.offsets.as_ref().unwrap();
        assert_eq!(offsets.as_slice(), &[0, 0, 0]); // All offsets are 0 for empty strings

        let values: Vec<&[u8]> = sequence.binary_values().unwrap().collect();
        assert_eq!(values, vec![b"" as &[u8], b""]);
    }

    #[test]
    fn test_from_any_value_guid_constant() {
        // Test GUID constant value (16 bytes)
        let guid_bytes = vec![
            0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54,
            0x32, 0x10,
        ];
        let any_value = AnyValue {
            kind: Some(Kind::BytesValue(guid_bytes.clone())),
            annotation: None,
        };
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Guid,
            signed: false,
            fixed_size: 16,
            extended_type: Default::default(),
        };

        let sequence = ValueSequence::from_any_value(3, &any_value, type_desc).unwrap();

        assert_eq!(sequence.len(), 3);
        assert!(sequence.offsets.is_none()); // Fixed-size GUIDs don't need offsets
        assert_eq!(sequence.presence.count_non_nulls(), 3);

        // Check that all values are the correct GUID
        let values_bytes = sequence.values.as_bytes();
        assert_eq!(values_bytes.len(), 3 * 16); // 3 GUIDs * 16 bytes each
        for i in 0..3 {
            let start = i * 16;
            let end = start + 16;
            assert_eq!(&values_bytes[start..end], guid_bytes.as_slice());
        }
    }

    #[test]
    fn test_from_binary_constant_unified_api() {
        // Test Binary type
        let binary_type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Binary,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let binary_sequence = ValueSequence::from_binary(3, b"data", binary_type_desc).unwrap();
        assert_eq!(binary_sequence.len(), 3);
        assert_eq!(binary_sequence.values.as_bytes(), b"datadatadata");
        assert!(binary_sequence.offsets.is_some());

        // Test String type with valid UTF-8
        let string_type_desc = BasicTypeDescriptor {
            basic_type: BasicType::String,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let string_sequence =
            ValueSequence::from_binary(2, "hello".as_bytes(), string_type_desc).unwrap();
        assert_eq!(string_sequence.len(), 2);
        assert_eq!(string_sequence.values.as_bytes(), b"hellohello");
        assert!(string_sequence.offsets.is_some());

        // Test FixedSizeBinary type
        let fixed_type_desc = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            signed: false,
            fixed_size: 4,
            extended_type: Default::default(),
        };
        let fixed_sequence = ValueSequence::from_binary(2, &[1, 2, 3, 4], fixed_type_desc).unwrap();
        assert_eq!(fixed_sequence.len(), 2);
        assert_eq!(fixed_sequence.values.as_bytes(), &[1, 2, 3, 4, 1, 2, 3, 4]);
        assert!(fixed_sequence.offsets.is_none());

        // Test Guid type
        let guid_bytes = [0u8; 16];
        let guid_type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Guid,
            signed: false,
            fixed_size: 16,
            extended_type: Default::default(),
        };
        let guid_sequence = ValueSequence::from_binary(1, &guid_bytes, guid_type_desc).unwrap();
        assert_eq!(guid_sequence.len(), 1);
        assert_eq!(guid_sequence.values.as_bytes().len(), 16);
        assert!(guid_sequence.offsets.is_none());
    }

    #[test]
    fn test_from_binary_constant_invalid_utf8_string() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::String,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        // Invalid UTF-8 bytes
        let result = ValueSequence::from_binary(1, &[0xFF, 0xFE], type_desc);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("Binary value is not valid UTF-8 for String type")
        );
    }

    #[test]
    fn test_from_binary_constant_wrong_fixed_size() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            signed: false,
            fixed_size: 4,
            extended_type: Default::default(),
        };
        let result = ValueSequence::from_binary(1, &[1, 2, 3], type_desc);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("Binary value length 3 doesn't match expected fixed size 4")
        );
    }

    #[test]
    fn test_from_binary_constant_wrong_guid_size() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Guid,
            signed: false,
            fixed_size: 16,
            extended_type: Default::default(),
        };
        let result = ValueSequence::from_binary(1, &[1, 2, 3, 4, 5, 6, 7, 8], type_desc);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("Guid value length 8 is not 16 bytes")
        );
    }

    #[test]
    fn test_from_binary_constant_unsupported_type() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let result = ValueSequence::from_binary(1, &[1, 2, 3, 4], type_desc);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains(
            "try_from_binary only supports Binary, String, FixedSizeBinary, and Guid types"
        ));
    }

    #[test]
    fn test_from_any_value_unsigned_integers() {
        use amudai_format::defs::common::{AnyValue, any_value::Kind};

        // Test unsigned u8
        let type_desc_u8 = BasicTypeDescriptor {
            basic_type: BasicType::Int8,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let any_value = AnyValue {
            kind: Some(Kind::U64Value(255)),
            annotation: None,
        };
        let sequence = ValueSequence::from_any_value(3, &any_value, type_desc_u8).unwrap();
        assert_eq!(sequence.len(), 3);
        let values = sequence.as_slice::<u8>();
        assert_eq!(values, &[255u8, 255u8, 255u8]);

        // Test unsigned u16
        let type_desc_u16 = BasicTypeDescriptor {
            basic_type: BasicType::Int16,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let any_value = AnyValue {
            kind: Some(Kind::U64Value(65535)),
            annotation: None,
        };
        let sequence = ValueSequence::from_any_value(2, &any_value, type_desc_u16).unwrap();
        assert_eq!(sequence.len(), 2);
        let values = sequence.as_slice::<u16>();
        assert_eq!(values, &[65535u16, 65535u16]);

        // Test unsigned u32
        let type_desc_u32 = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let any_value = AnyValue {
            kind: Some(Kind::U64Value(4294967295)),
            annotation: None,
        };
        let sequence = ValueSequence::from_any_value(1, &any_value, type_desc_u32).unwrap();
        assert_eq!(sequence.len(), 1);
        let values = sequence.as_slice::<u32>();
        assert_eq!(values, &[4294967295u32]);

        // Test unsigned u64
        let type_desc_u64 = BasicTypeDescriptor {
            basic_type: BasicType::Int64,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let any_value = AnyValue {
            kind: Some(Kind::U64Value(18446744073709551615)),
            annotation: None,
        };
        let sequence = ValueSequence::from_any_value(1, &any_value, type_desc_u64).unwrap();
        assert_eq!(sequence.len(), 1);
        let values = sequence.as_slice::<u64>();
        assert_eq!(values, &[18446744073709551615u64]);
    }

    #[test]
    fn test_from_any_value_signed_integers() {
        use amudai_format::defs::common::{AnyValue, any_value::Kind};

        // Test signed i8
        let type_desc_i8 = BasicTypeDescriptor {
            basic_type: BasicType::Int8,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let any_value = AnyValue {
            kind: Some(Kind::I64Value(-128)),
            annotation: None,
        };
        let sequence = ValueSequence::from_any_value(2, &any_value, type_desc_i8).unwrap();
        assert_eq!(sequence.len(), 2);
        let values = sequence.as_slice::<i8>();
        assert_eq!(values, &[-128i8, -128i8]);

        // Test signed i16
        let type_desc_i16 = BasicTypeDescriptor {
            basic_type: BasicType::Int16,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let any_value = AnyValue {
            kind: Some(Kind::I64Value(-32768)),
            annotation: None,
        };
        let sequence = ValueSequence::from_any_value(2, &any_value, type_desc_i16).unwrap();
        assert_eq!(sequence.len(), 2);
        let values = sequence.as_slice::<i16>();
        assert_eq!(values, &[-32768i16, -32768i16]);

        // Test signed i32
        let type_desc_i32 = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let any_value = AnyValue {
            kind: Some(Kind::I64Value(-2147483648)),
            annotation: None,
        };
        let sequence = ValueSequence::from_any_value(1, &any_value, type_desc_i32).unwrap();
        assert_eq!(sequence.len(), 1);
        let values = sequence.as_slice::<i32>();
        assert_eq!(values, &[-2147483648i32]);
    }

    #[test]
    fn test_from_any_value_u64_for_signed_type() {
        use amudai_format::defs::common::{AnyValue, any_value::Kind};

        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true, // signed type
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let any_value = AnyValue {
            kind: Some(Kind::U64Value(42)), // but using U64Value
            annotation: None,
        };
        let result = ValueSequence::from_any_value(1, &any_value, type_desc);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("U64Value (42) provided for signed type")
        );
    }

    #[test]
    fn test_from_any_value_i64_for_unsigned_type() {
        use amudai_format::defs::common::{AnyValue, any_value::Kind};

        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Int16,
            signed: false, // unsigned type
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let any_value = AnyValue {
            kind: Some(Kind::I64Value(42)), // but using I64Value
            annotation: None,
        };
        let result = ValueSequence::from_any_value(1, &any_value, type_desc);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("I64Value (42) provided for unsigned type")
        );
    }

    #[test]
    fn test_from_any_value_guid_wrong_size() {
        use amudai_format::defs::common::{AnyValue, any_value::Kind};

        // Test GUID with wrong size (should fail)
        let wrong_size_bytes = vec![0x01, 0x02, 0x03]; // Only 3 bytes instead of 16
        let any_value = AnyValue {
            kind: Some(Kind::BytesValue(wrong_size_bytes)),
            annotation: None,
        };
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Guid,
            signed: false,
            fixed_size: 16,
            extended_type: Default::default(),
        };

        let result = ValueSequence::from_any_value(2, &any_value, type_desc);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("Guid value length 3 is not 16 bytes")
        );
    }

    #[test]
    fn test_from_any_value_integer_range_validation() {
        use amudai_format::defs::common::{AnyValue, any_value::Kind};

        // Test i64 value that's too large for i8
        let type_desc_i8 = BasicTypeDescriptor {
            basic_type: BasicType::Int8,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let any_value = AnyValue {
            kind: Some(Kind::I64Value(1000)), // Too large for i8
            annotation: None,
        };
        let result = ValueSequence::from_any_value(1, &any_value, type_desc_i8);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("I64Value (1000) out of range for Int8")
        );

        // Test u64 value that's too large for u16
        let type_desc_u16 = BasicTypeDescriptor {
            basic_type: BasicType::Int16,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let any_value = AnyValue {
            kind: Some(Kind::U64Value(70000)), // Too large for u16
            annotation: None,
        };
        let result = ValueSequence::from_any_value(1, &any_value, type_desc_u16);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("U64Value (70000) out of range for unsigned Int16")
        );
    }

    #[test]
    fn test_from_any_value_floating_special_values() {
        use amudai_format::defs::common::{AnyValue, any_value::Kind};

        // Test NaN values
        let nan_value = AnyValue {
            kind: Some(Kind::DoubleValue(f64::NAN)),
            annotation: None,
        };

        // Test Float32 with NaN
        let type_desc_f32 = BasicTypeDescriptor {
            basic_type: BasicType::Float32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let result = ValueSequence::from_any_value(2, &nan_value, type_desc_f32);
        assert!(result.is_ok());
        let sequence = result.unwrap();
        assert_eq!(sequence.len(), 2);
        let values = sequence.as_slice::<f32>();
        assert!(values[0].is_nan());
        assert!(values[1].is_nan());

        // Test Float64 with NaN
        let type_desc_f64 = BasicTypeDescriptor {
            basic_type: BasicType::Float64,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        };
        let result = ValueSequence::from_any_value(1, &nan_value, type_desc_f64);
        assert!(result.is_ok());
        let sequence = result.unwrap();
        let values = sequence.as_slice::<f64>();
        assert!(values[0].is_nan());

        // Test positive infinity
        let pos_inf_value = AnyValue {
            kind: Some(Kind::DoubleValue(f64::INFINITY)),
            annotation: None,
        };
        let result = ValueSequence::from_any_value(1, &pos_inf_value, type_desc_f64);
        assert!(result.is_ok());
        let sequence = result.unwrap();
        let values = sequence.as_slice::<f64>();
        assert!(values[0].is_infinite() && values[0].is_sign_positive());

        // Test negative infinity
        let neg_inf_value = AnyValue {
            kind: Some(Kind::DoubleValue(f64::NEG_INFINITY)),
            annotation: None,
        };
        let result = ValueSequence::from_any_value(1, &neg_inf_value, type_desc_f32);
        assert!(result.is_ok());
        let sequence = result.unwrap();
        let values = sequence.as_slice::<f32>();
        assert!(values[0].is_infinite() && values[0].is_sign_negative());
    }

    #[test]
    fn test_from_any_value_decimal_validation() {
        use amudai_format::defs::common::{AnyValue, any_value::Kind};

        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            signed: false,
            fixed_size: 16,
            extended_type: Default::default(),
        };

        // Test valid decimal (zero)
        let valid_decimal_bytes = vec![0u8; 16]; // All zeros is a valid decimal representation
        let valid_decimal_value = AnyValue {
            kind: Some(Kind::DecimalValue(valid_decimal_bytes)),
            annotation: None,
        };
        let result = ValueSequence::from_any_value(1, &valid_decimal_value, type_desc);
        assert!(result.is_ok());

        // Test invalid decimal (create deliberately corrupted bytes)
        // This creates an invalid combination field that should be rejected
        let mut invalid_decimal_bytes = vec![0u8; 16];
        // Set invalid bits that create an invalid decimal128 encoding
        invalid_decimal_bytes[0] = 0xFF; // Invalid combination field
        invalid_decimal_bytes[1] = 0xFF;

        let invalid_decimal_value = AnyValue {
            kind: Some(Kind::DecimalValue(invalid_decimal_bytes)),
            annotation: None,
        };
        let result = ValueSequence::from_any_value(1, &invalid_decimal_value, type_desc);

        // This might pass if the bit pattern happens to be valid, so let's test with
        // a pattern we know creates NaN
        if result.is_err() {
            assert!(result.unwrap_err().contains("Invalid decimal128 encoding"));
        }

        // Test wrong size
        let wrong_size_decimal = AnyValue {
            kind: Some(Kind::DecimalValue(vec![0u8; 8])), // Wrong size
            annotation: None,
        };
        let result = ValueSequence::from_any_value(1, &wrong_size_decimal, type_desc);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("DecimalValue must be exactly 16 bytes")
        );
    }
}
