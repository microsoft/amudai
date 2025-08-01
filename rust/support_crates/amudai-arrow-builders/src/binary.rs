//! Binary array builders for Apache Arrow.
//!
//! This module provides builders for constructing Apache Arrow binary arrays using a
//! record-by-record building approach. Binary arrays store arbitrary byte sequences,
//! making them suitable for storing serialized data, images, documents, or any other
//! binary content.
//!
//! # Available Types
//!
//! - [`BinaryBuilder`] - Variable-length binary arrays using `LargeBinaryArray`
//! - [`FixedSizeBinaryBuilder`] - Fixed-length binary arrays where all values have the same byte length
//!
//! # Building Strategy
//!
//! Both builders follow a position-based approach:
//! 1. Push binary values using `push()` with byte slices or arrays
//! 2. Push explicit nulls using `push_null()` when needed
//! 3. Skip positions to automatically insert nulls for sparse arrays
//! 4. Build the final array with `build()`
//!
//! # Memory Considerations
//!
//! - `BinaryBuilder` uses `LargeBinaryArray` which supports arrays up to 2^63 bytes total
//! - `FixedSizeBinaryBuilder` requires all non-null values to have exactly the specified byte length
//! - Both builders automatically handle null filling for sparse data

use std::sync::Arc;

use arrow_array::Array;

use crate::ArrayBuilder;

/// A builder for Apache Arrow variable-length binary arrays.
///
/// `BinaryBuilder` provides a type-safe interface for constructing `LargeBinaryArray`s
/// where each element can contain an arbitrary number of bytes. It handles automatic
/// null filling for sparse arrays and maintains position-based building semantics.
///
/// # Memory Layout
///
/// Uses Apache Arrow's `LargeBinaryArray` format which consists of:
/// - **Values buffer**: All binary data concatenated together
/// - **Offsets buffer**: 64-bit integers indicating where each binary value starts and ends
/// - **Null buffer**: Tracks which elements are null vs non-null
///
/// # Core Operations
///
/// - [`push()`](Self::push) - Sets a binary value at the current position and advances
/// - [`push_null()`](Self::push_null) - Sets an explicit null at the current position and advances
pub struct BinaryBuilder {
    /// The current logical position where the next binary value will be placed.
    next_pos: u64,
    /// In-progress list of offsets for each binary item in `data`.
    /// For `n` items, there are `n+1` offsets in this list.
    offsets: arrow_buffer::OffsetBufferBuilder<i64>,
    /// Consecutive data buffers.
    data: arrow_buffer::MutableBuffer,
    /// Presence (nulls) bitmap.
    nulls: arrow_buffer::NullBufferBuilder,
}

impl BinaryBuilder {
    /// Sets a binary value at the current position and advances to the next position.
    ///
    /// This method accepts any type that can be converted to a byte slice (`AsRef<[u8]>`),
    /// including `&[u8]`, `Vec<u8>`, `&str`, and `String`. The binary data is copied
    /// into the builder's internal buffer.
    ///
    /// # Parameters
    ///
    /// * `value` - The binary data to store. Can be any type implementing `AsRef<[u8]>`.
    pub fn push(&mut self, value: impl AsRef<[u8]>) {
        self.fill_missing();
        let value = value.as_ref();
        self.offsets.push_length(value.len());
        self.data.extend_from_slice(value);
        self.nulls.append_non_null();
        self.next_pos += 1;
    }

    /// Concatenates multiple binary fragments into a single value at the current position
    /// and advances.
    ///
    /// This method accepts an iterable of fragments that can each be converted to byte slices
    /// (`AsRef<[u8]>`). All fragments are concatenated together in order to form a single
    /// binary value that is stored at the current position. This is useful for efficiently
    /// building binary values from multiple parts without intermediate allocations.
    ///
    /// # Parameters
    ///
    /// * `fragments` - An iterable of binary fragments to concatenate. Each fragment can be
    ///   any type implementing `AsRef<[u8]>` (e.g., `&[u8]`, `Vec<u8>`, `&str`, `String`).
    ///   The iterator must be cloneable to allow for efficient length calculation.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// # use amudai_arrow_builders::binary::BinaryBuilder;
    /// let mut builder = BinaryBuilder::default();
    ///
    /// // Concatenate string fragments
    /// builder.push_concat(&[b"Hello".as_ref(), b" ", b"World"]);
    ///
    /// // Concatenate mixed types
    /// builder.push_concat(&[b"prefix".as_ref(), "middle".as_bytes(), b"suffix"]);
    /// ```
    pub fn push_concat(&mut self, fragments: impl IntoIterator<Item = impl AsRef<[u8]>> + Clone) {
        self.fill_missing();

        let len = fragments
            .clone()
            .into_iter()
            .map(|s| s.as_ref().len())
            .sum();
        self.data.reserve(len);
        self.offsets.push_length(len);
        for slice in fragments {
            self.data.extend_from_slice(slice.as_ref());
        }
        self.nulls.append_non_null();

        self.next_pos += 1;
    }

    /// Sets an explicit null value at the current position and advances to the next position.
    ///
    /// This creates a null entry in the binary array, which is different from an empty
    /// binary value (zero-length byte array). Null values are tracked in the array's
    /// null buffer and can be checked with `is_null()` methods on the resulting array.
    pub fn push_null(&mut self) {
        self.fill_missing();
        self.offsets.push_length(0);
        self.nulls.append_null();
        self.next_pos += 1;
    }

    /// Returns the current length of the underlying Arrow builder.
    #[inline]
    fn inner_pos(&self) -> u64 {
        self.offsets.len() as u64 - 1
    }

    /// Fills any gaps between the inner position and the target position with nulls.
    ///
    /// This method ensures that the inner builder's length matches the current
    /// logical position by appending null values for any missing entries.
    /// This enables sparse building where positions can be skipped.
    ///
    /// # Panics
    ///
    /// Panics if the inner position doesn't match the target position after filling.
    #[inline]
    fn fill_missing(&mut self) {
        if self.inner_pos() < self.next_pos {
            let null_count = (self.next_pos - self.inner_pos()) as usize;
            for _ in 0..null_count {
                self.offsets.push_length(0);
            }
            self.nulls.append_n_nulls(null_count);
        }
        assert_eq!(self.inner_pos(), self.next_pos);
    }
}

/// Creates a new, empty `BinaryBuilder` with default settings.
///
/// The builder starts at position 0 with empty underlying buffers.
impl Default for BinaryBuilder {
    fn default() -> Self {
        BinaryBuilder {
            next_pos: 0,
            offsets: arrow_buffer::OffsetBufferBuilder::new(128),
            data: Default::default(),
            nulls: arrow_buffer::NullBufferBuilder::new(128),
        }
    }
}

/// Implementation of [`ArrayBuilder`] for [`BinaryBuilder`].
///
/// This implementation enables `BinaryBuilder` to be used anywhere an `ArrayBuilder`
/// is expected, including as an element type for nested structures like lists of
/// binary data or maps with binary values.
impl ArrayBuilder for BinaryBuilder {
    fn data_type(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::LargeBinary
    }

    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }

    fn as_any_mut(&mut self) -> &mut (dyn std::any::Any + 'static) {
        self
    }

    fn try_push_raw_value(&mut self, value: Option<&[u8]>) -> Result<(), arrow_schema::ArrowError> {
        if let Some(value) = value {
            self.push(value);
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Returns the current logical position in the binary array being built.
    ///
    /// This represents the index where the next binary value will be placed.
    #[inline]
    fn next_pos(&self) -> u64 {
        self.next_pos
    }

    /// Moves the builder to the specified logical position.
    ///
    /// This updates the internal position counter but doesn't immediately affect
    /// the underlying array. Any gaps will be filled with nulls when the next
    /// value is set or when the array is built.
    ///
    /// # Parameters
    ///
    /// * `pos` - The target position to move to
    #[inline]
    fn move_to_pos(&mut self, pos: u64) {
        self.next_pos = pos;
    }

    /// Consumes the builder and produces the final Apache Arrow binary array.
    ///
    /// This method fills any remaining gaps with nulls and then finalizes the
    /// underlying `LargeBinaryBuilder` to create a `LargeBinaryArray`.
    ///
    /// # Returns
    ///
    /// An `Arc<dyn Array>` containing the built `LargeBinaryArray`.
    fn build(&mut self) -> Arc<dyn Array> {
        self.fill_missing();
        self.next_pos = 0;

        let nulls = self.nulls.finish();
        let offsets = std::mem::replace(
            &mut self.offsets,
            arrow_buffer::OffsetBufferBuilder::new(128),
        )
        .finish();
        let data = std::mem::take(&mut self.data).into();
        let array = arrow_array::GenericBinaryArray::new(offsets, data, nulls);
        Arc::new(array)
    }
}

/// A builder for Apache Arrow fixed-size binary arrays.
///
/// `FixedSizeBinaryBuilder` provides a type-safe interface for constructing
/// `FixedSizeBinaryArray`s where all non-null elements must have exactly the same
/// byte length. The byte length is specified as a const generic parameter `S`.
///
/// # Type Parameter
///
/// * `S` - The fixed size in bytes that all binary values must have.
///
/// # Memory Layout
///
/// Uses Apache Arrow's `FixedSizeBinaryArray` format which consists of:
/// - **Values buffer**: All binary data stored contiguously (size × count bytes)
/// - **Null buffer**: Tracks which elements are null vs non-null
///
/// # Core Operations
///
/// - [`set()`](Self::set) - Sets a binary value with exact length validation
/// - [`set_null()`](Self::set_null) - Sets an explicit null value
///
/// # Length Validation
///
/// All binary values passed to [`set()`](Self::set) must have exactly `S` bytes.
/// Values with incorrect length will cause a panic during append operations.
pub struct FixedSizeBinaryBuilder<const S: usize> {
    /// The current logical position where the next binary value will be placed.
    next_pos: u64,
    /// The underlying Apache Arrow fixed-size binary builder.
    inner: arrow_array::builder::FixedSizeBinaryBuilder,
    value_size: usize,
}

impl FixedSizeBinaryBuilder<0> {
    pub fn new(size: usize) -> Self {
        FixedSizeBinaryBuilder {
            next_pos: 0,
            inner: arrow_array::builder::FixedSizeBinaryBuilder::new(size as i32),
            value_size: size,
        }
    }
}

impl<const S: usize> FixedSizeBinaryBuilder<S> {
    /// Sets a fixed-size binary value at the current position and advances to the next position.
    ///
    /// This method accepts any type that can be converted to a byte slice (`AsRef<[u8]>`).
    /// The provided value must have exactly `S` bytes, or the operation will panic.
    ///
    /// # Parameters
    ///
    /// * `value` - The binary data to store. Must have exactly `S` bytes.
    ///
    /// # Panics
    ///
    /// Panics if the provided value doesn't have exactly `S` bytes.
    pub fn push(&mut self, value: impl AsRef<[u8]>) {
        self.fill_missing();
        self.inner.append_value(value).expect("append");
        self.next_pos += 1;
    }

    /// Sets an explicit null value at the current position and advances to the next position.
    ///
    /// This creates a null entry in the fixed-size binary array. Null values don't
    /// consume space in the values buffer and are tracked separately in the null buffer.
    pub fn push_null(&mut self) {
        self.fill_missing();
        self.inner.append_null();
        self.next_pos += 1;
    }

    /// Returns the current length of the underlying Arrow builder.
    #[inline]
    fn inner_pos(&self) -> u64 {
        arrow_array::builder::ArrayBuilder::len(&self.inner) as u64
    }

    /// Fills any gaps between the inner position and the target position with nulls.
    ///
    /// This method ensures that the inner builder's length matches the current
    /// logical position by appending null values for any missing entries.
    /// This enables sparse building where positions can be skipped.
    ///
    /// # Panics
    ///
    /// Panics if the inner position doesn't match the target position after filling.
    #[inline]
    fn fill_missing(&mut self) {
        if self.inner_pos() < self.next_pos {
            let null_count = self.next_pos - self.inner_pos();
            for _ in 0..null_count {
                self.inner.append_null();
            }
        }
        assert_eq!(self.inner_pos(), self.next_pos);
    }
}

/// Creates a new, empty `FixedSizeBinaryBuilder` with the specified byte size.
///
/// The builder starts at position 0 with an underlying `FixedSizeBinaryBuilder`
/// configured for the const generic byte size `S`.
impl<const S: usize> Default for FixedSizeBinaryBuilder<S> {
    fn default() -> Self {
        FixedSizeBinaryBuilder {
            next_pos: 0,
            inner: arrow_array::builder::FixedSizeBinaryBuilder::new(S as i32),
            value_size: S,
        }
    }
}

/// Implementation of [`ArrayBuilder`] for [`FixedSizeBinaryBuilder`].
///
/// This implementation enables `FixedSizeBinaryBuilder` to be used anywhere an
/// `ArrayBuilder` is expected, including as an element type for nested structures
/// like lists of fixed-size binary data.
impl<const S: usize> ArrayBuilder for FixedSizeBinaryBuilder<S> {
    fn data_type(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::FixedSizeBinary(self.value_size as i32)
    }

    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }

    fn as_any_mut(&mut self) -> &mut (dyn std::any::Any + 'static) {
        self
    }

    fn try_push_raw_value(&mut self, value: Option<&[u8]>) -> Result<(), arrow_schema::ArrowError> {
        if let Some(value) = value {
            if value.len() != self.value_size {
                return Err(arrow_schema::ArrowError::InvalidArgumentError(format!(
                    "Expected value size {}, got {}",
                    self.value_size,
                    value.len()
                )));
            }
            self.push(value);
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Returns the current logical position in the fixed-size binary array being built.
    ///
    /// This represents the index where the next binary value will be placed.
    #[inline]
    fn next_pos(&self) -> u64 {
        self.next_pos
    }

    /// Moves the builder to the specified logical position.
    ///
    /// This updates the internal position counter but doesn't immediately affect
    /// the underlying array. Any gaps will be filled with nulls when the next
    /// value is set or when the array is built.
    ///
    /// # Parameters
    ///
    /// * `pos` - The target position to move to
    #[inline]
    fn move_to_pos(&mut self, pos: u64) {
        self.next_pos = pos;
    }

    /// Consumes the builder and produces the final Apache Arrow fixed-size binary array.
    ///
    /// This method fills any remaining gaps with nulls and then finalizes the
    /// underlying `FixedSizeBinaryBuilder` to create a `FixedSizeBinaryArray`
    /// with the specified byte size.
    ///
    /// # Returns
    ///
    /// An `Arc<dyn Array>` containing the built `FixedSizeBinaryArray`.
    fn build(&mut self) -> Arc<dyn Array> {
        self.fill_missing();
        self.next_pos = 0;
        Arc::new(self.inner.finish())
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::cast::AsArray;

    use crate::{ArrayBuilder, binary::BinaryBuilder};

    #[test]
    fn test_binary_builder() {
        let mut builder = BinaryBuilder::default();
        builder.push(b"abc");
        builder.move_to_pos(10);
        builder.push_concat(&[b"def", b"1234".as_ref()]);
        builder.push(b"def");
        let arr = builder.build();
        let arr = arr.as_binary::<i64>();
        assert_eq!(arr.value(3), b"");
        assert_eq!(arr.value(10), b"def1234");
    }
}
