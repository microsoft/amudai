//! Map array builder for constructing Apache Arrow Map arrays.
//!
//! This module provides [`MapBuilder`], a specialized builder for creating Apache Arrow Map arrays.
//! Map arrays represent collections of key-value pairs, similar to dictionaries or hash maps,
//! where each map entry can contain zero or more key-value pairs.
//!
//! # Map Array Structure
//!
//! An Apache Arrow Map array consists of:
//! - A list of maps, where each map contains key-value pairs
//! - Keys and values are stored as separate arrays
//! - Each map can contain a different number of entries
//! - Maps can be null (representing an absent map)
//! - Keys within a map should be unique (though this is not enforced by the builder)
//!
//! # Usage
//!
//! The basic workflow for building map arrays:
//!
//! 1. **Add key-value pairs**: Use [`key()`] and [`value()`] to add entries to the current map
//! 2. **Finish maps**: Call [`finish_map()`] to complete a map with the added entries
//! 3. **Add null maps**: Call [`finish_null_map()`] to add a null map entry
//! 4. **Build the array**: Call [`build()`] to produce the final [`MapArray`]
//!
//! # Type Parameters
//!
//! - `K`: The key builder type, must implement [`ArrayBuilder`]
//! - `V`: The value builder type, must implement [`ArrayBuilder`]
//!
//! [`MapArray`]: arrow_array::MapArray

use std::sync::Arc;

use arrow_array::Array;

use crate::ArrayBuilder;

/// Builder for constructing Apache Arrow Map arrays.
///
/// `MapBuilder` creates Map arrays where each element is a collection of key-value pairs.
/// Each map can contain zero or more entries, and maps can be null. The keys and values
/// are stored in separate typed arrays for efficient memory layout and type safety.
///
/// # Type Parameters
///
/// * `K` - The builder type for map keys, must implement [`ArrayBuilder`]
/// * `V` - The builder type for map values, must implement [`ArrayBuilder`]
///
/// # Map Structure
///
/// The builder internally maintains:
/// - Separate builders for keys and values
/// - Offset array tracking map boundaries
/// - Null bitmap for null maps
/// - Position tracking for sparse building
///
/// # Building Process
///
/// 1. Add key-value pairs using [`key()`]/[`value()`] or [`entry()`]
/// 2. Complete maps with [`finish_map()`] or add null maps with [`finish_null_map()`]
/// 3. Build the final array with [`build()`]
///
/// [`key()`]: MapBuilder::key
/// [`value()`]: MapBuilder::value
/// [`entry()`]: MapBuilder::entry
/// [`finish_map()`]: MapBuilder::finish_map
/// [`finish_null_map()`]: MapBuilder::finish_null_map
/// [`build()`]: ArrayBuilder::build
pub struct MapBuilder<K: ArrayBuilder, V: ArrayBuilder> {
    /// Current logical position in the map array being built.
    ///
    /// This tracks how many maps (including null maps) have been added to the array.
    /// It's used to ensure proper alignment when building sparse arrays and for
    /// filling gaps with null entries when needed.
    next_pos: u64,

    /// Builder for the keys of all map entries.
    ///
    /// All keys from all maps are stored contiguously in this single builder.
    /// The `offsets` array is used to determine which keys belong to which map.
    keys: K,

    /// Builder for the values of all map entries.
    ///
    /// All values from all maps are stored contiguously in this single builder.
    /// The `offsets` array is used to determine which values belong to which map.
    /// The number of values must always equal the number of keys.
    values: V,

    /// Offset array marking the boundaries of each map.
    ///
    /// Each map spans from `offsets[i]` to `offsets[i+1]` in the key/value arrays.
    /// This allows efficient random access to individual maps without scanning
    /// the entire key/value arrays. The first offset is always 0, and subsequent
    /// offsets mark the end of each map.
    offsets: Vec<i32>,

    /// Null buffer builder for tracking which maps are null.
    ///
    /// This efficiently tracks which map positions contain null values using
    /// a compact bitmap representation. Null maps don't consume any entries
    /// in the key/value arrays but still occupy a position in the map array.
    nulls: arrow_buffer::NullBufferBuilder,
}

impl<K: ArrayBuilder, V: ArrayBuilder> MapBuilder<K, V> {
    /// Finishes the current map and returns the number of key-value pairs in it.
    ///
    /// This method completes the current map being built by:
    /// 1. Filling any gaps in the map array with null entries
    /// 2. Recording the current position as a map boundary
    /// 3. Marking this map as non-null in the null bitmap
    /// 4. Advancing the logical position
    ///
    /// # Returns
    ///
    /// The number of key-value pairs that were added to this map.
    ///
    /// # Panics
    ///
    /// Panics if the number of keys doesn't equal the number of values, or if
    /// the key/value count exceeds `i32::MAX`.
    pub fn finish_map(&mut self) -> usize {
        self.fill_missing();

        assert!(self.keys.next_pos() < i32::MAX as u64);
        assert_eq!(self.keys.next_pos(), self.values.next_pos());

        let next_offset = self.keys.next_pos() as i32;
        let map_len = next_offset - *self.offsets.last().unwrap();
        self.offsets.push(next_offset);
        self.nulls.append_non_null();
        self.next_pos += 1;

        map_len as usize
    }

    /// Adds a null map entry to the array.
    ///
    /// This method adds a null map (representing an absent/missing map) to the array.
    /// No key-value pairs are consumed, but the logical position is advanced.
    ///
    /// Null maps are useful for representing optional or missing data in the map array.
    pub fn finish_null_map(&mut self) {
        self.fill_missing();

        assert!(self.keys.next_pos() < i32::MAX as u64);
        assert_eq!(self.keys.next_pos(), self.values.next_pos());

        let next_offset = self.keys.next_pos() as i32;
        self.offsets.push(next_offset);
        self.nulls.append_null();
        self.next_pos += 1;
    }

    /// Returns a mutable reference to the key builder.
    ///
    /// Use this to add keys to the current map being built. Keys should be added
    /// in the same order as their corresponding values (added via [`value()`]).
    pub fn key(&mut self) -> &mut K {
        &mut self.keys
    }

    /// Returns a mutable reference to the value builder.
    ///
    /// Use this to add values to the current map being built. Values should be added
    /// in the same order as their corresponding keys (added via [`key()`]).
    pub fn value(&mut self) -> &mut V {
        &mut self.values
    }

    /// Returns mutable references to both the key and value builders.
    ///
    /// This is a convenience method that returns a tuple of mutable references
    /// to both builders, allowing you to add a key-value pair in a single operation.
    /// This can be more ergonomic than calling [`key()`] and [`value()`] separately.
    ///
    /// # Returns
    ///
    /// A tuple `(&mut K, &mut V)` containing mutable references to the key and value builders.
    pub fn entry(&mut self) -> (&mut K, &mut V) {
        (&mut self.keys, &mut self.values)
    }

    /// Returns the current internal position in the offsets array.
    #[inline]
    fn inner_pos(&self) -> u64 {
        self.offsets.len() as u64 - 1
    }

    /// Fills any gaps between the internal position and the next logical position with null maps.
    ///
    /// This method ensures that the internal state is consistent by adding null map entries
    /// for any positions that were skipped (e.g., when building sparse arrays). It synchronizes
    /// the offsets array and null buffer with the logical position.
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
}

impl<K: ArrayBuilder + Default, V: ArrayBuilder + Default> Default for MapBuilder<K, V> {
    /// Creates a new `MapBuilder` with default configuration.
    ///
    /// This initializes:
    /// - Position counters to 0
    /// - Default key and value builders
    /// - An offset array starting with 0
    /// - A null buffer builder with initial capacity
    fn default() -> Self {
        MapBuilder {
            next_pos: 0,
            keys: K::default(),
            values: V::default(),
            offsets: vec![0],
            nulls: arrow_buffer::NullBufferBuilder::new(1024),
        }
    }
}

impl<K: ArrayBuilder, V: ArrayBuilder> ArrayBuilder for MapBuilder<K, V> {
    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }

    fn as_any_mut(&mut self) -> &mut (dyn std::any::Any + 'static) {
        self
    }

    fn try_push_raw_value(&mut self, value: Option<&[u8]>) -> Result<(), arrow_schema::ArrowError> {
        if value.is_some() {
            self.finish_map();
        } else {
            self.finish_null_map();
        }
        Ok(())
    }

    /// Returns the current logical position in the map array.
    ///
    /// This represents how many maps should exist at this position,
    /// which may include null maps that haven't been physically added yet.
    #[inline]
    fn next_pos(&self) -> u64 {
        self.next_pos
    }

    /// Moves the logical position to the specified position.
    ///
    /// This is used for sparse building where maps may not be added sequentially.
    /// Any gaps between the current position and the new position will be filled
    /// with null maps when the next map operation occurs.
    ///
    /// # Parameters
    ///
    /// * `pos` - The new logical position to move to
    fn move_to_pos(&mut self, pos: u64) {
        self.next_pos = pos;
    }

    /// Builds the final Apache Arrow Map array.
    ///
    /// This method consumes the builder and creates a [`MapArray`] containing all
    /// the maps that were added. It performs the following steps:
    ///
    /// 1. Fills any remaining gaps with null maps
    /// 2. Builds the key and value arrays
    /// 3. Creates the appropriate Arrow schema for the map structure
    /// 4. Constructs the final `MapArray` with proper offsets and null bitmap
    ///
    /// # Returns
    ///
    /// An `Arc<dyn Array>` containing the built `MapArray`.
    ///
    /// # Panics
    ///
    /// Panics if the number of keys doesn't equal the number of values.
    ///
    /// [`MapArray`]: arrow_array::MapArray
    fn build(&mut self) -> Arc<dyn Array> {
        self.fill_missing();

        let keys = self.keys.build();
        let values = self.values.build();

        assert_eq!(keys.len(), values.len());

        let key_field = Arc::new(arrow_schema::Field::new(
            "key",
            keys.data_type().clone(),
            false,
        ));
        let value_field = Arc::new(arrow_schema::Field::new(
            "value",
            values.data_type().clone(),
            true,
        ));

        let struct_fields = arrow_schema::Fields::from(vec![key_field, value_field]);
        let struct_array = arrow_array::StructArray::new(struct_fields, vec![keys, values], None);

        let entries_field = Arc::new(arrow_schema::Field::new(
            "entries",
            struct_array.data_type().clone(),
            false,
        ));

        let offsets = arrow_buffer::OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(
            std::mem::take(&mut self.offsets),
        ));

        let nulls = self.nulls.finish();

        self.next_pos = 0;
        self.offsets.push(0);

        Arc::new(arrow_array::MapArray::new(
            entries_field,
            offsets,
            struct_array,
            nulls,
            false, // keys_sorted - assume not sorted by default
        ))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, cast::AsArray};

    use crate::{ArrayBuilder, map::MapBuilder, primitive::Int32Builder, string::StringBuilder};

    #[test]
    fn test_map_builder() {
        let mut builder = MapBuilder::<StringBuilder, Int32Builder>::default();

        // First map: {"a": 1, "b": 2}
        builder.key().push("a");
        builder.value().push(1);
        builder.key().push("b");
        builder.value().push(2);
        builder.finish_map();

        // Null map
        builder.finish_null_map();

        // Third map: {"c": 3}
        builder.key().push("c");
        builder.value().push(3);
        builder.finish_map();

        let map_array = builder.build();
        let map_array = map_array.as_map();
        assert_eq!(map_array.len(), 3);
        assert!(map_array.is_null(1)); // Second map is null
        assert!(!map_array.is_null(0)); // First map is not null
        assert!(!map_array.is_null(2)); // Third map is not null
    }

    #[test]
    fn test_map_builder_with_add_entry() {
        let mut builder = MapBuilder::<StringBuilder, Int32Builder>::default();

        // First map: {"x": 10, "y": 20}
        {
            let (key, value) = builder.entry();
            key.push("x");
            value.push(10);
        }
        {
            let (key, value) = builder.entry();
            key.push("y");
            value.push(20);
        }
        builder.finish_map();

        // Second map: {"z": 30}
        {
            let (key, value) = builder.entry();
            key.push("z");
            value.push(30);
        }
        builder.finish_map();

        let map_array = builder.build();
        let map_array = map_array.as_map();
        assert_eq!(map_array.len(), 2);
    }
}
