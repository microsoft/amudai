//! String array builder for Apache Arrow integration.
//!
//! This module provides [`StringBuilder`]for constructing Apache Arrow [`LargeStringArray`]s.
//! The builder implements the custom [`ArrayBuilder`] trait, allowing for sparse value setting
//! with automatic null handling.
//!
//! [`LargeStringArray`]: arrow_array::LargeStringArray

use std::sync::Arc;

use arrow_array::Array;

use crate::ArrayBuilder;

/// A builder for constructing Apache Arrow [`LargeStringArray`]s.
///
/// `StringBuilder` allows for efficient construction of string arrays with support for
/// sparse value setting, automatic null handling, and position-based building. It wraps
/// Arrow's [`LargeStringBuilder`] and extends it with the custom [`ArrayBuilder`] interface.
///
/// # Building Process
///
/// 1. **Set values**: Use [`set()`] to add string values at the current position
/// 2. **Set nulls**: Use [`set_null()`] to explicitly add null values  
/// 3. **Skip positions**: Use [`move_to_pos()`] to jump to specific positions (gaps become nulls)
/// 4. **Build array**: Use [`build()`] to consume the builder and produce the final array
///
/// # Memory Layout
///
/// The builder uses Arrow's large string format which stores:
/// - **Offsets**: 64-bit offsets into the value buffer (allowing arrays > 2GB)
/// - **Values**: UTF-8 encoded string data in a contiguous buffer
/// - **Validity**: Optional null bitmap indicating which positions contain valid data
///
/// [`set()`]: StringBuilder::set
/// [`set_null()`]: StringBuilder::set_null
/// [`move_to_pos()`]: ArrayBuilder::move_to_pos
/// [`build()`]: ArrayBuilder::build
/// [`LargeStringArray`]: arrow_array::LargeStringArray
/// [`LargeStringBuilder`]: arrow_array::builder::LargeStringBuilder
pub struct StringBuilder {
    /// The current logical position where the next value will be placed.
    ///
    /// This tracks the index of the next element to be added to the array.
    /// When values are set or positions are moved, this is automatically updated.
    next_pos: u64,

    /// The underlying Arrow string builder that handles the actual string storage.
    ///
    /// This wraps Arrow's `LargeStringBuilder` which manages the offsets buffer,
    /// values buffer, and null bitmap for efficient string array construction.
    inner: arrow_array::builder::LargeStringBuilder,
}

impl StringBuilder {
    /// Sets a string value at the current position and advances to the next position.
    ///
    /// This method adds the provided string to the array at the current logical position.
    /// Any gaps between the last set position and the current position are automatically
    /// filled with null values. After setting the value, the position is advanced by 1.
    ///
    /// # Parameters
    ///
    /// * `value` - A string value that implements [`AsRef<str>`]. This includes `&str`,
    ///   `String`, and other string-like types. The value must be valid UTF-8.
    ///
    /// # Performance
    ///
    /// The string data is copied into Arrow's internal buffers.
    pub fn set(&mut self, value: impl AsRef<str>) {
        self.fill_missing();
        self.inner.append_value(value);
        self.next_pos += 1;
    }

    /// Sets a null value at the current position and advances to the next position.
    ///
    /// This method explicitly adds a null value to the array at the current logical position.
    /// Any gaps between the last set position and the current position are automatically
    /// filled with null values. After setting the null, the position is advanced by 1.
    pub fn set_null(&mut self) {
        self.fill_missing();
        self.inner.append_null();
        self.next_pos += 1;
    }

    /// Returns the current number of elements in the underlying Arrow builder.
    ///
    /// This represents the actual number of values (including nulls) that have been
    /// added to the internal Arrow builder. This may be less than `next_pos` if there
    /// are unfilled gaps that will be filled with nulls when values are set or the
    /// array is built.
    ///
    /// # Returns
    ///
    /// The current length of the underlying Arrow builder as a `u64`.
    ///
    /// # Usage
    ///
    /// This method is primarily used internally to determine when gaps need to be filled
    /// with null values. It's marked `inline` for performance in the gap-filling logic.
    #[inline]
    fn inner_pos(&self) -> u64 {
        arrow_array::builder::ArrayBuilder::len(&self.inner) as u64
    }

    /// Fills any gaps between the current inner position and the logical position with nulls.
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

/// Creates a new `StringBuilder` with default settings.
///
/// The default implementation creates an empty builder starting at position 0
/// with a new underlying Arrow `LargeStringBuilder`. The builder is ready to
/// accept string values immediately.
impl Default for StringBuilder {
    fn default() -> Self {
        StringBuilder {
            next_pos: 0,
            inner: arrow_array::builder::LargeStringBuilder::new(),
        }
    }
}

/// Implementation of the [`ArrayBuilder`] trait for [`StringBuilder`].
///
/// This implementation provides the standard position-based building interface
/// used by all builders in this crate. It enables sparse value setting and
/// automatic gap filling with null values.
impl ArrayBuilder for StringBuilder {
    /// Returns the current logical position where the next value will be placed.
    ///
    /// # Returns
    ///
    /// The current position as a `u64`. This starts at 0 and increments each time
    /// a value is set or when explicitly moved via [`move_to_pos()`].
    ///
    /// [`move_to_pos()`]: ArrayBuilder::move_to_pos
    fn next_pos(&self) -> u64 {
        self.next_pos
    }

    /// Moves the builder to a specific logical position.
    ///
    /// This sets where the next value will be placed in the array. If the new position
    /// is beyond the current position, any gaps will be filled with nulls when the next
    /// value is set or when the array is built.
    ///
    /// # Parameters
    ///
    /// * `pos` - The target position (0-based index) for the next value
    fn move_to_pos(&mut self, pos: u64) {
        self.next_pos = pos;
    }

    /// Consumes the builder and produces the final Apache Arrow [`LargeStringArray`].
    ///
    /// This method finalizes the building process by:
    /// 1. Filling any remaining gaps with null values
    /// 2. Converting the internal buffers into a `LargeStringArray`
    /// 3. Wrapping the result in an `Arc<dyn Array>`
    ///
    /// # Returns
    ///
    /// An `Arc<dyn Array>` containing the built `LargeStringArray`. The array contains
    /// all the string values and nulls that were set during the building process.
    ///
    /// [`LargeStringArray`]: arrow_array::LargeStringArray
    fn build(mut self) -> Arc<dyn Array> {
        self.fill_missing();
        Arc::new(self.inner.finish())
    }
}
