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
//! 1. Set binary values using [`set()`] with byte slices or arrays
//! 2. Set explicit nulls using [`set_null()`] when needed
//! 3. Skip positions to automatically insert nulls for sparse arrays
//! 4. Build the final array with [`build()`]
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
/// - [`set()`](Self::set) - Sets a binary value at the current position and advances
/// - [`set_null()`](Self::set_null) - Sets an explicit null at the current position and advances
pub struct BinaryBuilder {
    /// The current logical position where the next binary value will be placed.
    next_pos: u64,
    /// The underlying Apache Arrow large binary builder that handles data storage.
    inner: arrow_array::builder::LargeBinaryBuilder,
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
    pub fn set(&mut self, value: impl AsRef<[u8]>) {
        self.fill_missing();
        self.inner.append_value(value);
        self.next_pos += 1;
    }

    /// Sets an explicit null value at the current position and advances to the next position.
    ///
    /// This creates a null entry in the binary array, which is different from an empty
    /// binary value (zero-length byte array). Null values are tracked in the array's
    /// null buffer and can be checked with `is_null()` methods on the resulting array.
    pub fn set_null(&mut self) {
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

/// Creates a new, empty `BinaryBuilder` with default settings.
///
/// The builder starts at position 0 with an empty underlying `LargeBinaryBuilder`.
impl Default for BinaryBuilder {
    fn default() -> Self {
        BinaryBuilder {
            next_pos: 0,
            inner: arrow_array::builder::LargeBinaryBuilder::new(),
        }
    }
}

/// Implementation of [`ArrayBuilder`] for [`BinaryBuilder`].
///
/// This implementation enables `BinaryBuilder` to be used anywhere an `ArrayBuilder`
/// is expected, including as an element type for nested structures like lists of
/// binary data or maps with binary values.
impl ArrayBuilder for BinaryBuilder {
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
    fn build(mut self) -> Arc<dyn Array> {
        self.fill_missing();
        Arc::new(self.inner.finish())
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
    pub fn set(&mut self, value: impl AsRef<[u8]>) {
        self.fill_missing();
        self.inner.append_value(value).expect("append");
        self.next_pos += 1;
    }

    /// Sets an explicit null value at the current position and advances to the next position.
    ///
    /// This creates a null entry in the fixed-size binary array. Null values don't
    /// consume space in the values buffer and are tracked separately in the null buffer.
    pub fn set_null(&mut self) {
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
        }
    }
}

/// Implementation of [`ArrayBuilder`] for [`FixedSizeBinaryBuilder`].
///
/// This implementation enables `FixedSizeBinaryBuilder` to be used anywhere an
/// `ArrayBuilder` is expected, including as an element type for nested structures
/// like lists of fixed-size binary data.
impl<const S: usize> ArrayBuilder for FixedSizeBinaryBuilder<S> {
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
    fn build(mut self) -> Arc<dyn Array> {
        self.fill_missing();
        Arc::new(self.inner.finish())
    }
}
