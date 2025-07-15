//! List array builders for Apache Arrow.
//!
//! This module provides [`ListBuilder`] for constructing Apache Arrow `LargeListArray`s
//! using a record-by-record building approach. Lists can contain any type that implements
//! [`ArrayBuilder`], enabling construction of nested structures like lists of strings,
//! lists of integers, or even lists of lists.
//!
//! # Building Strategy
//!
//! The list builder follows a bottom-up approach:
//! 1. Add items to the current list using [`item()`](ListBuilder::item)
//! 2. Finalize the list with [`finish_list()`](ListBuilder::finish_list) or
//!    [`finish_null_list()`](ListBuilder::finish_null_list)
//! 3. Repeat for additional lists
//! 4. Build the final array with [`build()`](ListBuilder::build)
//!
//! # Memory Layout
//!
//! The builder maintains:
//! - A values array containing all items across all lists
//! - An offsets array indicating where each list starts and ends in the values array
//! - A null buffer tracking which lists are null

use std::sync::Arc;

use arrow_array::Array;

use crate::ArrayBuilder;

/// A builder for Apache Arrow [`LargeListArray`]s with automatic null handling.
///
/// `ListBuilder` provides a type-safe interface for constructing list arrays where each
/// list can contain a variable number of items of type `T`. The builder manages the
/// memory layout of Arrow list arrays, including value storage, offset tracking,
/// and null handling.
///
/// # Type Parameter
///
/// * `T` - The builder type for list items, which must implement [`ArrayBuilder`]. This can
///   be any primitive builder (like [`crate::StringBuilder`], [`crate::Int64Builder`]) or
///   another nested builder (like `ListBuilder<Int64Builder>` for lists of lists).
///
/// # Architecture
///
/// List arrays in Apache Arrow consist of:
/// - **Values array**: All items from all lists stored contiguously
/// - **Offsets array**: Indices indicating where each list starts and ends in the values array
/// - **Null buffer**: Tracks which lists are null vs empty
///
/// # Building Process
///
/// 1. **Add items**: Use [`item()`](Self::item) to get a mutable reference to the values builder
/// 2. **Finish lists**: Call [`finish_list()`](Self::finish_list) to complete a non-null list
///    or [`finish_null_list()`](Self::finish_null_list) for null lists
/// 3. **Build array**: Call [`build()`](Self::build) to consume the builder and create the final array
///
/// # Performance Considerations
///
/// - The values builder (`T`) grows dynamically as items are added
/// - Offsets are stored as 64-bit integers, supporting very large arrays
pub struct ListBuilder<T: ArrayBuilder> {
    /// The current logical position where the next list will be placed.
    /// This allows for sparse building where lists can be added out of order.
    next_pos: u64,

    /// The builder for all values across all lists. Items from all lists
    /// are stored contiguously in this single values array.
    values: T,

    /// Offsets into the values array indicating where each list starts and ends.
    /// The i-th list contains values from offsets[i] to offsets[i+1] (exclusive).
    /// Always starts with [0] and grows as lists are finished.
    offsets: Vec<i64>,

    /// Tracks which lists are null. Distinguishes between null lists and empty lists.
    nulls: arrow_buffer::NullBufferBuilder,
}

impl<T: ArrayBuilder> ListBuilder<T> {
    /// Finishes the current list and marks it as non-null.
    ///
    /// This method completes the current list by recording the current values position
    /// as the end offset for this list. The list is marked as non-null in the null buffer,
    /// and the builder advances to the next position.
    ///
    /// # Returns
    ///
    /// The number of items in the completed list.
    pub fn finish_list(&mut self) -> usize {
        self.fill_missing();

        let next_offset = self.values.next_pos() as i64;
        let list_len = next_offset - *self.offsets.last().unwrap();

        self.offsets.push(next_offset);
        self.nulls.append_non_null();
        self.next_pos += 1;

        list_len as usize
    }

    /// Finishes the current list and marks it as null.
    ///
    /// This method completes the current list by recording the current values position
    /// as the end offset, but marks the list as null in the null buffer.
    pub fn finish_null_list(&mut self) {
        self.fill_missing();
        let next_offset = self.values.next_pos() as i64;
        self.offsets.push(next_offset);
        self.nulls.append_null();
        self.next_pos += 1;
    }

    /// Returns a mutable reference to the values builder for adding items to the current list.
    ///
    /// This method provides access to the underlying values builder, allowing you to add
    /// items to the list currently being constructed. Items are added sequentially and
    /// will be included in the next list that is finished.
    pub fn item(&mut self) -> &mut T {
        &mut self.values
    }

    /// Returns the current inner position (number of completed lists).
    ///
    /// This is calculated as the length of the offsets array minus 1, since the
    /// offsets array always starts with [0] and adds one offset per completed list.
    #[inline]
    fn inner_pos(&self) -> u64 {
        self.offsets.len() as u64 - 1
    }

    /// Fills any missing positions with null lists to maintain position consistency.
    ///
    /// This method is called internally to ensure that the builder maintains the invariant
    /// that `inner_pos() == next_pos`. If the builder's logical position has advanced beyond
    /// the number of completed lists (due to sparse building or position jumps), this method
    /// fills the gap with null lists.
    ///
    /// For each missing position:
    /// - Adds a null entry to the null buffer
    /// - Adds an offset entry (using the current offset value, creating empty lists)
    #[inline]
    fn fill_missing(&mut self) {
        if self.inner_pos() < self.next_pos {
            let null_count = self.next_pos - self.inner_pos();
            self.nulls.append_n_nulls(null_count as usize);
            let off = *self.offsets.last().unwrap();
            self.offsets
                .resize(self.offsets.len() + null_count as usize, off);
        }
        assert_eq!(self.inner_pos(), self.next_pos);
    }

    fn get_item_field(&self) -> Arc<arrow_schema::Field> {
        Arc::new(arrow_schema::Field::new(
            "item",
            self.values.data_type(),
            true,
        ))
    }
}

/// Creates a new, empty `ListBuilder` with default settings.
///
/// The builder starts at position 0 with:
/// - An empty values builder of type `T`
/// - Offsets initialized to `[0]`
/// - A null buffer with initial capacity
impl<T: ArrayBuilder + Default> Default for ListBuilder<T> {
    fn default() -> Self {
        ListBuilder {
            next_pos: 0,
            values: T::default(),
            offsets: vec![0],
            nulls: arrow_buffer::NullBufferBuilder::new(1024),
        }
    }
}

/// Implementation of [`ArrayBuilder`] for [`ListBuilder`].
///
/// This implementation enables `ListBuilder` to be used anywhere an `ArrayBuilder` is expected,
/// including as the element type for nested structures like lists of lists or maps with list values.
impl<T: ArrayBuilder> ArrayBuilder for ListBuilder<T> {
    fn data_type(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::LargeList(self.get_item_field())
    }

    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }

    fn as_any_mut(&mut self) -> &mut (dyn std::any::Any + 'static) {
        self
    }

    fn try_push_raw_value(&mut self, value: Option<&[u8]>) -> Result<(), arrow_schema::ArrowError> {
        if value.is_some() {
            self.finish_list();
        } else {
            self.finish_null_list();
        }
        Ok(())
    }

    /// Returns the current logical position where the next list will be placed.
    #[inline]
    fn next_pos(&self) -> u64 {
        self.next_pos
    }

    /// Moves the builder to a specific logical position.
    fn move_to_pos(&mut self, pos: u64) {
        self.next_pos = pos;
    }

    /// Consumes the builder and produces a `LargeListArray`.
    ///
    /// This method finalizes the building process by:
    /// 1. Filling any missing positions with null lists
    /// 2. Building the values array from the internal values builder
    /// 3. Creating the appropriate Arrow field metadata
    /// 4. Constructing the final `LargeListArray` with proper offsets and null handling
    ///
    /// # Returns
    ///
    /// An `Arc<dyn Array>` containing the built `LargeListArray`. Cast to the specific
    /// type using Arrow's casting utilities if needed.
    fn build(&mut self) -> Arc<dyn Array> {
        let field = self.get_item_field();

        self.fill_missing();

        let values = self.values.build();
        let offsets = arrow_buffer::OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(
            std::mem::take(&mut self.offsets),
        ));
        let nulls = self.nulls.finish();

        self.next_pos = 0;
        self.offsets.push(0);

        Arc::new(arrow_array::LargeListArray::new(
            field, offsets, values, nulls,
        ))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, cast::AsArray};

    use crate::{ArrayBuilder, list::ListBuilder, string::StringBuilder};

    #[test]
    fn test_list_builder() {
        let mut builder = ListBuilder::<StringBuilder>::default();

        builder.item().push("1");
        builder.item().push("2");
        builder.item().push("3");
        builder.finish_list();

        builder.finish_null_list();

        builder.item().push("4");
        builder.item().push("5");
        builder.finish_list();

        let list = builder.build();
        let list = list.as_list::<i64>();
        assert_eq!(list.len(), 3);
    }
}
