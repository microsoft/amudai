//! Primitive array builders for fundamental numeric and timestamp types.
//!
//! This module provides builders for Apache Arrow primitive data types including integers,
//! floating-point numbers, and timestamps. All primitive builders implement automatic null
//! handling and position-based building.
//!
//! # Available Types
//!
//! ## Unsigned Integers
//! - [`UInt8Builder`] - 8-bit unsigned integers (0 to 255)
//! - [`UInt16Builder`] - 16-bit unsigned integers (0 to 65,535)
//! - [`UInt32Builder`] - 32-bit unsigned integers (0 to 4,294,967,295)
//! - [`UInt64Builder`] - 64-bit unsigned integers (0 to 18,446,744,073,709,551,615)
//!
//! ## Signed Integers
//! - [`Int8Builder`] - 8-bit signed integers (-128 to 127)
//! - [`Int16Builder`] - 16-bit signed integers (-32,768 to 32,767)
//! - [`Int32Builder`] - 32-bit signed integers (-2,147,483,648 to 2,147,483,647)
//! - [`Int64Builder`] - 64-bit signed integers (-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)
//!
//! ## Floating Point
//! - [`Float32Builder`] - 32-bit IEEE 754 floating-point numbers
//! - [`Float64Builder`] - 64-bit IEEE 754 floating-point numbers
//!
//! ## Temporal Types
//! - [`TimestampBuilder`] - 64-bit timestamps with nanosecond precision

use std::sync::Arc;

use arrow_array::{
    Array, ArrowPrimitiveType,
    types::{
        DurationNanosecondType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
        Int64Type, TimestampNanosecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};

use crate::ArrayBuilder;

/// A builder for Apache Arrow primitive arrays.
///
/// `PrimitiveBuilder` provides a type-safe interface for building arrays of primitive values
/// such as integers, floating-point numbers, and timestamps. It handles automatic null filling
/// for sparse arrays and maintains position-based building semantics.
///
/// # Type Parameter
///
/// * `T` - An Apache Arrow primitive type implementing [`ArrowPrimitiveType`]. This determines
///   the specific primitive type being built (e.g., `Int64Type`, `Float64Type`).
///
/// # Core Operations
///
/// The builder supports two primary operations:
/// - [`set()`](Self::set) - Sets a value at the current position and advances
/// - [`set_null()`](Self::set_null) - Sets an explicit null at the current position and advances
///
/// # Position Management
///
/// The builder tracks a logical position (`next_pos`) that represents where the next value
/// will be placed. When values are set beyond the current position, any gaps are automatically
/// filled with nulls to maintain array coherence.
///
/// # Memory Management
///
/// Internally wraps an Arrow `PrimitiveBuilder` from the `arrow_array` crate.
pub struct PrimitiveBuilder<T: ArrowPrimitiveType> {
    /// The current logical position where the next value will be placed.
    next_pos: u64,
    /// The underlying Apache Arrow primitive builder that handles the actual data storage.
    inner: arrow_array::builder::PrimitiveBuilder<T>,
}

impl<T: ArrowPrimitiveType> PrimitiveBuilder<T> {
    /// Sets a value at the current position and advances to the next position.
    ///
    /// This method places the provided value at the current logical position
    /// and increments the position counter. If there are any gaps between the
    /// last written position and the current position, they are automatically
    /// filled with nulls.
    ///
    /// # Parameters
    ///
    /// * `value` - The primitive value to set. The type must match the native type
    ///   for the Arrow primitive type `T` (e.g., `i64` for `Int64Type`, `f64` for `Float64Type`).
    ///
    /// # Automatic Gap Filling
    ///
    /// If the builder's position was moved beyond the current array length,
    /// gaps are filled with nulls.
    pub fn push(&mut self, value: <T as ArrowPrimitiveType>::Native) {
        self.fill_missing();
        self.inner.append_value(value);
        self.next_pos += 1;
    }

    /// Sets an explicit null at the current position and advances to the next position.
    ///
    /// This method places a null value at the current logical position and increments
    /// the position counter. Like [`set()`](Self::set), any gaps are automatically
    /// filled with nulls before placing the explicit null.
    ///
    /// # Note
    ///
    /// This method explicitly sets a null value, which is different from simply
    /// skipping a position. Both approaches result in null values, but explicit
    /// nulls via this method are more intentional and clear in code.
    pub fn push_null(&mut self) {
        self.fill_missing();
        self.inner.append_null();
        self.next_pos += 1;
    }

    /// Returns the current length of the internal array builder.
    ///
    /// This represents the actual number of elements that have been written to
    /// the underlying Arrow builder, which may be less than `next_pos` if the
    /// position has been moved ahead without writing values.
    #[inline]
    fn inner_pos(&self) -> u64 {
        use arrow_array::builder::ArrayBuilder as _;
        self.inner.len() as u64
    }

    /// Fills any gaps between the internal position and the logical position with nulls.
    ///
    /// This method ensures that the internal Arrow builder has the same number of
    /// elements as the logical position by appending nulls for any missing positions.
    /// It's called automatically before any `set` or `set_null` operation to maintain
    /// consistency.
    ///
    /// # Panics
    ///
    /// Panics if the internal state becomes inconsistent (internal position doesn't
    /// match the logical position after filling).
    #[inline]
    fn fill_missing(&mut self) {
        if self.inner_pos() < self.next_pos {
            let null_count = self.next_pos - self.inner_pos();
            self.inner.append_nulls(null_count as usize);
        }
        assert_eq!(self.inner_pos(), self.next_pos);
    }
}

impl<T: ArrowPrimitiveType> Default for PrimitiveBuilder<T> {
    /// Creates a new `PrimitiveBuilder` with default settings.
    ///
    /// The builder starts at position 0 and is ready to accept values.
    /// The internal Arrow builder is initialized with default capacity.
    fn default() -> Self {
        PrimitiveBuilder {
            next_pos: 0,
            inner: arrow_array::builder::PrimitiveBuilder::new(),
        }
    }
}

impl<T: ArrowPrimitiveType> ArrayBuilder for PrimitiveBuilder<T> {
    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }

    fn as_any_mut(&mut self) -> &mut (dyn std::any::Any + 'static) {
        self
    }

    fn try_push_raw_value(&mut self, value: Option<&[u8]>) -> Result<(), arrow_schema::ArrowError> {
        use arrow_buffer::ArrowNativeType;
        if let Some(value) = value {
            if value.len() != T::Native::get_byte_width() {
                return Err(arrow_schema::ArrowError::InvalidArgumentError(
                    "raw value len".into(),
                ));
            }
            let mut v = T::Native::default();
            unsafe {
                std::ptr::copy_nonoverlapping(
                    value.as_ptr(),
                    &mut v as *mut T::Native as *mut u8,
                    value.len(),
                );
            }
            self.push(v);
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Returns the current logical position where the next value will be placed.
    ///
    /// This position represents the index of the next element to be added to the array.
    /// It starts at 0 and increments each time a value is set via [`set()`](Self::set)
    /// or [`set_null()`](Self::set_null).
    #[inline]
    fn next_pos(&self) -> u64 {
        self.next_pos
    }

    /// Moves the builder to a specific logical position.
    ///
    /// This sets where the next value will be placed in the array. If the new position
    /// is beyond the current position, any intermediate positions will be filled with
    /// nulls when the next value is set or when the array is built.
    ///
    /// # Parameters
    ///
    /// * `pos` - The target position (0-based index) for the next value.
    #[inline]
    fn move_to_pos(&mut self, pos: u64) {
        self.next_pos = pos;
    }

    /// Consumes the builder and produces the final Apache Arrow primitive array.
    ///
    /// This method finalizes the building process by filling any remaining gaps
    /// with nulls and converting the internal buffer into an Apache Arrow array
    /// of the appropriate primitive type.
    ///
    /// # Returns
    ///
    /// An `Arc<dyn Array>` containing the built primitive array. The concrete type
    /// will be the appropriate Arrow primitive array (e.g., `Int64Array`, `Float64Array`).
    fn build(&mut self) -> Arc<dyn Array> {
        self.fill_missing();
        self.next_pos = 0;
        Arc::new(self.inner.finish())
    }
}

// Type aliases for convenience and ergonomics

/// Type alias for building arrays of 8-bit unsigned integers.
pub type UInt8Builder = PrimitiveBuilder<UInt8Type>;

/// Type alias for building arrays of 16-bit unsigned integers.
pub type UInt16Builder = PrimitiveBuilder<UInt16Type>;

/// Type alias for building arrays of 32-bit unsigned integers.
pub type UInt32Builder = PrimitiveBuilder<UInt32Type>;

/// Type alias for building arrays of 64-bit unsigned integers.
pub type UInt64Builder = PrimitiveBuilder<UInt64Type>;

/// Type alias for building arrays of 8-bit signed integers.
pub type Int8Builder = PrimitiveBuilder<Int8Type>;

/// Type alias for building arrays of 16-bit signed integers.
pub type Int16Builder = PrimitiveBuilder<Int16Type>;

/// Type alias for building arrays of 32-bit signed integers.
pub type Int32Builder = PrimitiveBuilder<Int32Type>;

/// Type alias for building arrays of 64-bit signed integers.
pub type Int64Builder = PrimitiveBuilder<Int64Type>;

/// Type alias for building arrays of 32-bit IEEE 754 floating-point numbers.
pub type Float32Builder = PrimitiveBuilder<Float32Type>;

/// Type alias for building arrays of 64-bit IEEE 754 floating-point numbers.
pub type Float64Builder = PrimitiveBuilder<Float64Type>;

/// Type alias for building arrays of timestamps with nanosecond precision.
///
/// Timestamps are stored as 64-bit signed integers representing nanoseconds since
/// the Unix epoch (1970-01-01 00:00:00 UTC).
///
/// # Note
///
/// The timestamp values are raw nanosecond values. For working with higher-level
/// date/time types, consider converting from your date/time library to nanosecond
/// timestamps before using this builder.
pub type TimestampBuilder = PrimitiveBuilder<TimestampNanosecondType>;

/// Type alias for building arrays of durations with nanosecond precision.
pub type DurationBuilder = PrimitiveBuilder<DurationNanosecondType>;
