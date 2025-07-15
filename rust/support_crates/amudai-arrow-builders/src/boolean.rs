use std::sync::Arc;

use arrow_array::Array;

use crate::ArrayBuilder;

/// A builder for Apache Arrow [`BooleanArray`]s with automatic null handling.
///
/// `BooleanBuilder` provides a type-safe interface for constructing boolean arrays
/// where each element can be either `true`, `false`, or `null`. It handles automatic
/// null filling for sparse arrays and maintains position-based building semantics.
///
/// # Core Operations
///
/// - [`push()`](Self::push) - Sets a boolean value at the current position and advances
/// - [`push_null()`](Self::push_null) - Sets an explicit null at the current position and advances
///
/// Uses Apache Arrow's `BooleanArray` format which consists of:
/// - **Values buffer**: Packed bits where each bit represents a boolean value (1 = true, 0 = false)
/// - **Null buffer**: Tracks which elements are null vs non-null
///
/// [`BooleanArray`]: arrow_array::BooleanArray
pub struct BooleanBuilder {
    /// The current logical position where the next value will be placed.
    next_pos: u64,
    /// The underlying Apache Arrow builder that handles the actual data storage.
    inner: arrow_array::builder::BooleanBuilder,
}

impl BooleanBuilder {
    /// Sets a value at the current position and advances to the next position.
    ///
    /// This method places the provided value at the current logical position
    /// and increments the position counter. If there are any gaps between the
    /// last written position and the current position, they are automatically
    /// filled with nulls.
    ///
    /// # Parameters
    ///
    /// * `value` - The value to push.
    ///
    /// # Automatic Gap Filling
    ///
    /// If the builder's position was moved beyond the current array length,
    /// gaps are filled with nulls.
    pub fn push(&mut self, value: bool) {
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

impl Default for BooleanBuilder {
    /// Creates a new `BooleanBuilder` with default settings.
    ///
    /// The builder starts at position 0 and is ready to accept values.
    /// The internal Arrow builder is initialized with default capacity.
    fn default() -> Self {
        BooleanBuilder {
            next_pos: 0,
            inner: arrow_array::builder::BooleanBuilder::new(),
        }
    }
}

impl ArrayBuilder for BooleanBuilder {
    fn data_type(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Boolean
    }

    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }

    fn as_any_mut(&mut self) -> &mut (dyn std::any::Any + 'static) {
        self
    }

    fn try_push_raw_value(&mut self, value: Option<&[u8]>) -> Result<(), arrow_schema::ArrowError> {
        if let Some(value) = value {
            if value.len() != 1 {
                return Err(arrow_schema::ArrowError::InvalidArgumentError(
                    "raw Boolean value len".into(),
                ));
            }
            self.push(value[0] != 0);
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
