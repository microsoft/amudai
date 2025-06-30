//! Array Reader Module
//!
//! Provides low-level readers for converting Amudai fields into Apache Arrow arrays.
//! Contains abstractions for reading primitive arrays from Amudai shards and exposing them as Arrow-compatible data structures.

use std::{ops::Range, sync::Arc};

use amudai_arrow_compat::{
    amudai_to_arrow_error::ToArrowResult,
    sequence_to_array::{IntoArrowArray, IntoArrowNullBuffer, IntoArrowOffsets},
};
use amudai_format::schema::BasicType;
use amudai_sequence::offsets::Offsets;
use amudai_shard::read::{
    field::Field,
    field_decoder::{FieldDecoder, FieldReader, boolean::BooleanFieldReader},
};
use arrow::{
    array::{ArrayRef, LargeListArray, MapArray, StructArray},
    datatypes::{DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields},
    error::ArrowError,
};
use itertools::Itertools;

use crate::shard_reader::{create_array_reader, resolve_field};

/// An enum representing different array readers for Amudai fields.
///
/// This enum provides a unified interface for reading different types of Amudai fields
/// and converting them to Apache Arrow arrays. Each variant handles a specific category
/// of data types with their own reading and conversion logic.
pub enum ArrayReader {
    /// Reader for basic types (primitive, string and binary).
    ///
    /// Handles simple data types that can be directly converted to Arrow arrays
    /// without complex nested structure processing.
    Simple(SimpleArrayReader),

    /// Reader for boolean types with optimized Arrow BooleanBuffer handling.
    ///
    /// This specialized variant provides better boolean data processing by working
    /// directly with Arrow's BooleanBuffer format, avoiding the overhead of expanded
    /// byte buffer conversions used by SimpleArrayReader.
    Boolean(BooleanArrayReader),

    /// Reader for struct types with named fields.
    ///
    /// Handles complex structured data with multiple named fields, converting
    /// them to Arrow StructArray instances.
    Struct(StructArrayReader),

    /// Reader for list/array types with variable-length elements.
    ///
    /// Handles nested list structures, converting them to Arrow LargeListArray
    /// instances with proper offset and null handling.
    List(ListArrayReader),

    /// Reader for map types.
    Map(MapArrayReader),
}

impl ArrayReader {
    /// Constructs a new `ArrayReader` for the given Arrow type and Amudai field.
    ///
    /// # Arguments
    /// * `arrow_type` - The Arrow data type to produce.
    /// * `stripe_field` - The Amudai field to read from.
    /// * `pos_ranges_hint` - Iterator over position ranges to read.
    ///
    /// # Errors
    /// Returns an error if the field cannot be decoded or the reader cannot be created.
    pub fn new(
        arrow_type: ArrowDataType,
        stripe_field: Field,
        pos_ranges_hint: impl Iterator<Item = Range<u64>> + Clone,
    ) -> Result<ArrayReader, ArrowError> {
        match arrow_type {
            ArrowDataType::Struct(fields) => {
                ArrayReader::new_struct(fields, stripe_field, pos_ranges_hint)
            }
            ArrowDataType::LargeList(field) => {
                ArrayReader::new_list(field, stripe_field, pos_ranges_hint)
            }
            ArrowDataType::Map(entry_field, _) => {
                ArrayReader::new_map(entry_field, stripe_field, pos_ranges_hint)
            }
            _ => ArrayReader::new_basic(arrow_type, stripe_field, pos_ranges_hint),
        }
    }

    /// Reads an Arrow array for the specified position range.
    ///
    /// This method delegates to the appropriate reader implementation based on the
    /// array reader variant.
    ///
    /// # Arguments
    /// * `pos_range` - The range of logical positions to read from the underlying field.
    ///
    /// # Returns
    /// Returns an `ArrayRef` containing the Arrow array data for the specified range.
    pub fn read(&mut self, pos_range: Range<u64>) -> Result<ArrayRef, ArrowError> {
        match self {
            ArrayReader::Simple(reader) => reader.read(pos_range),
            ArrayReader::Boolean(reader) => reader.read(pos_range),
            ArrayReader::Struct(reader) => reader.read(pos_range),
            ArrayReader::List(reader) => reader.read(pos_range),
            ArrayReader::Map(reader) => reader.read(pos_range),
        }
    }
}

impl ArrayReader {
    /// Creates a new struct array reader for converting Amudai struct fields to Arrow
    /// StructArray.
    ///
    /// This method validates that the Amudai field is indeed a struct type, then creates
    /// individual array readers for each child field based on the provided Arrow struct
    /// field definitions. It resolves field mappings between Arrow and Amudai schemas
    /// and sets up the necessary infrastructure for reading struct data.
    ///
    /// # Arguments
    /// * `arrow_struct_fields` - Arrow field definitions for all struct fields that should
    ///   be present in the resulting StructArray
    /// * `amudai_struct_field` - The Amudai field containing the struct data to be read
    /// * `pos_ranges_hint` - Iterator over position ranges that provides hints for
    ///   optimizing child field access patterns
    ///
    /// # Returns
    /// Returns a new `ArrayReader::Struct` variant containing a configured `StructArrayReader`.
    fn new_struct(
        arrow_struct_fields: ArrowFields,
        amudai_struct_field: Field,
        pos_ranges_hint: impl Iterator<Item = Range<u64>> + Clone,
    ) -> Result<ArrayReader, ArrowError> {
        let basic_type = amudai_struct_field.data_type().describe().to_arrow_res()?;
        if basic_type.basic_type != BasicType::Struct {
            return Err(ArrowError::SchemaError(
                "Field cannot be converted to a struct".into(),
            ));
        }

        let stripe = amudai_struct_field.get_stripe();
        let amudai_child_fields = amudai_struct_field
            .data_type()
            .field_list()
            .to_arrow_res()?;

        let mut arrow_field_readers = Vec::with_capacity(arrow_struct_fields.len());
        for arrow_struct_field in arrow_struct_fields.iter() {
            let amudai_type = resolve_field(&stripe, &amudai_child_fields, arrow_struct_field)?;
            let arrow_field_reader = create_array_reader(
                &stripe,
                Some(&amudai_struct_field),
                amudai_type,
                arrow_struct_field.data_type().clone(),
                pos_ranges_hint.clone(),
            )?;
            arrow_field_readers.push(arrow_field_reader);
        }

        let struct_reader = amudai_struct_field
            .create_decoder()
            .to_arrow_res()?
            .create_reader(pos_ranges_hint.clone())
            .to_arrow_res()?;

        Ok(ArrayReader::Struct(StructArrayReader::new(
            arrow_struct_fields,
            struct_reader,
            arrow_field_readers,
        )))
    }

    /// Creates a new list array reader for converting Amudai list fields to Arrow
    /// LargeListArray.
    ///
    /// This method validates that the Amudai field is indeed a list type, then sets up
    /// the infrastructure for reading both list metadata (offsets and nulls) and child
    /// element data. It creates optimized child position range hints to minimize I/O
    /// when reading nested list data.
    ///
    /// # Arguments
    /// * `arrow_item_field` - Arrow field definition for the list's child elements
    /// * `amudai_list_field` - The Amudai field containing the list data to be read
    /// * `pos_ranges_hint` - Iterator over position ranges that provides hints for
    ///   optimizing list and child element access patterns
    ///
    /// # Returns
    /// Returns a new `ArrayReader::List` variant containing a configured `ListArrayReader`.
    fn new_list(
        arrow_item_field: Arc<ArrowField>,
        amudai_list_field: Field,
        pos_ranges_hint: impl Iterator<Item = Range<u64>> + Clone,
    ) -> Result<ArrayReader, ArrowError> {
        let basic_type = amudai_list_field.data_type().describe().to_arrow_res()?;
        if basic_type.basic_type != BasicType::List {
            return Err(ArrowError::SchemaError(
                "Field cannot be converted to a list".into(),
            ));
        }

        let list_reader = amudai_list_field
            .create_decoder()
            .to_arrow_res()?
            .create_reader(pos_ranges_hint.clone())
            .to_arrow_res()?;

        let stripe = amudai_list_field.get_stripe();
        let amudai_item_type = amudai_list_field.data_type().child_at(0).to_arrow_res()?;

        let amudai_item_field = stripe.open_field(amudai_item_type.clone()).to_arrow_res()?;
        let list_count = amudai_list_field.position_count();
        let item_count = amudai_item_field.position_count();
        let item_ranges = derive_list_item_pos_ranges_hint(pos_ranges_hint, list_count, item_count);

        let child_reader = create_array_reader(
            &stripe,
            Some(&amudai_list_field),
            Some(amudai_item_type),
            arrow_item_field.data_type().clone(),
            item_ranges.iter().cloned(),
        )?;

        Ok(ArrayReader::List(ListArrayReader::new(
            arrow_item_field,
            list_reader,
            Box::new(child_reader),
        )))
    }

    /// Creates a new map array reader for converting Amudai map fields to Arrow
    /// MapArray.
    ///
    /// # Arguments
    /// * `arrow_entry_field` - Arrow field definition for the map's child entry struct
    /// * `amudai_map_field` - The Amudai field containing the map data to be read
    /// * `pos_ranges_hint` - Iterator over position ranges that provides hints for
    ///   optimizing list and child element access patterns
    ///
    /// # Returns
    /// Returns a new `ArrayReader::Map` variant containing a configured `MapArrayReader`.
    fn new_map(
        arrow_entry_field: Arc<ArrowField>,
        amudai_map_field: Field,
        pos_ranges_hint: impl Iterator<Item = Range<u64>> + Clone,
    ) -> Result<ArrayReader, ArrowError> {
        let basic_type = amudai_map_field.data_type().describe().to_arrow_res()?;
        if basic_type.basic_type != BasicType::Map {
            return Err(ArrowError::SchemaError(
                "Field cannot be converted to a map".into(),
            ));
        }

        let ArrowDataType::Struct(struct_fields) = arrow_entry_field.data_type() else {
            return Err(ArrowError::SchemaError(
                "Arrow map entry field is not a struct".into(),
            ));
        };
        if struct_fields.len() != 2 {
            return Err(ArrowError::SchemaError(format!(
                "Arrow map entry struct has unexpected field count {}",
                struct_fields.len()
            )));
        }

        let arrow_key_field = &struct_fields[0];
        let arrow_value_field = &struct_fields[1];

        let map_reader = amudai_map_field
            .create_decoder()
            .to_arrow_res()?
            .create_reader(pos_ranges_hint.clone())
            .to_arrow_res()?;

        let stripe = amudai_map_field.get_stripe();
        let amudai_key_type = amudai_map_field.data_type().child_at(0).to_arrow_res()?;
        let amudai_value_type = amudai_map_field.data_type().child_at(1).to_arrow_res()?;

        let amudai_key_field = stripe.open_field(amudai_key_type.clone()).to_arrow_res()?;
        let map_count = amudai_map_field.position_count();
        let entry_count = amudai_key_field.position_count();
        let entry_ranges =
            derive_list_item_pos_ranges_hint(pos_ranges_hint, map_count, entry_count);

        let key_reader = create_array_reader(
            &stripe,
            Some(&amudai_map_field),
            Some(amudai_key_type),
            arrow_key_field.data_type().clone(),
            entry_ranges.iter().cloned(),
        )?;

        let value_reader = create_array_reader(
            &stripe,
            Some(&amudai_map_field),
            Some(amudai_value_type),
            arrow_value_field.data_type().clone(),
            entry_ranges.iter().cloned(),
        )?;

        Ok(ArrayReader::Map(MapArrayReader::new(
            arrow_entry_field,
            map_reader,
            Box::new(key_reader),
            Box::new(value_reader),
        )))
    }

    /// Creates a new basic array reader for converting simple Amudai fields to Arrow arrays.
    ///
    /// This method handles primitive data types (integers, floats, strings, binary data)
    /// that can be directly converted to Arrow arrays without complex nested structure
    /// processing. It sets up a `SimpleArrayReader` with the appropriate field decoder.
    ///
    /// # Arguments
    /// * `arrow_type` - The Arrow data type to produce (currently unused, but reserved
    ///   for future type conversion logic)
    /// * `stripe_field` - The Amudai field containing the basic data to be read
    /// * `pos_ranges_hint` - Iterator over position ranges that provides hints for
    ///   optimizing field access patterns
    ///
    /// # Returns
    /// Returns a new `ArrayReader::Simple` variant containing a configured `SimpleArrayReader`.
    fn new_basic(
        arrow_type: ArrowDataType,
        stripe_field: Field,
        pos_ranges_hint: impl Iterator<Item = Range<u64>> + Clone,
    ) -> Result<ArrayReader, ArrowError> {
        if matches!(arrow_type, ArrowDataType::Boolean) {
            return Self::new_boolean(arrow_type, stripe_field, pos_ranges_hint);
        }

        // TODO: check whether we need on-the-fly data conversion, i.e. the requested
        // formal Arrow type is different from the actual stripe field data type, but
        // the types are conceptually compatible and conversion is possible (e.g.Guid
        // to FixedSizeBinary(16), or different timestamp representations).
        let reader = stripe_field
            .create_decoder()
            .to_arrow_res()?
            .create_reader(pos_ranges_hint)
            .to_arrow_res()?;
        Ok(ArrayReader::Simple(SimpleArrayReader::new(
            reader, arrow_type,
        )))
    }

    /// Creates a new boolean array reader for converting Amudai boolean fields to Arrow
    /// BooleanArray.
    ///
    /// This method validates that the Amudai field is indeed a boolean type, then creates
    /// a specialized boolean field reader that can efficiently handle boolean data with
    /// proper null handling and optimized boolean storage.
    ///
    /// # Arguments
    /// * `arrow_type` - The Arrow data type (must be Boolean)
    /// * `stripe_field` - The Amudai field containing the boolean data to be read
    /// * `pos_ranges_hint` - Iterator over position ranges that provides hints for
    ///   optimizing boolean field access patterns
    ///
    /// # Returns
    /// Returns a new `ArrayReader::Boolean` variant containing a configured `BooleanArrayReader`.
    fn new_boolean(
        arrow_type: ArrowDataType,
        stripe_field: Field,
        pos_ranges_hint: impl Iterator<Item = Range<u64>> + Clone,
    ) -> Result<ArrayReader, ArrowError> {
        let basic_type = stripe_field.data_type().describe().to_arrow_res()?;
        if basic_type.basic_type != BasicType::Boolean {
            return Err(ArrowError::NotYetImplemented(format!(
                "{:?} field to Arrow {} conversion",
                basic_type.basic_type, arrow_type
            )));
        }

        let decoder = match stripe_field.create_decoder().to_arrow_res()? {
            FieldDecoder::Boolean(decoder) => decoder,
            _ => panic!("unexpected field decoder variant"),
        };
        let reader = decoder
            .create_boolean_reader(pos_ranges_hint)
            .to_arrow_res()?;
        Ok(ArrayReader::Boolean(BooleanArrayReader::new(reader)))
    }
}

/// Reader for basic Amudai fields that do not require any data conversions,
/// producing Arrow arrays.
pub struct SimpleArrayReader {
    reader: Box<dyn FieldReader>,
    arrow_type: ArrowDataType,
}

impl SimpleArrayReader {
    /// Constructs a new `SimpleArrayReader`.
    ///
    /// # Arguments
    /// * `reader` - The field reader implementation.
    pub fn new(reader: Box<dyn FieldReader>, arrow_type: ArrowDataType) -> SimpleArrayReader {
        SimpleArrayReader { reader, arrow_type }
    }

    /// Reads an Arrow array for the specified position range.
    ///
    /// # Arguments
    /// * `pos_range` - The range of logical positions to read.
    pub fn read(&mut self, pos_range: Range<u64>) -> Result<ArrayRef, ArrowError> {
        let seq = self.reader.read(pos_range).to_arrow_res()?;
        let arr = seq.into_arrow_array(&self.arrow_type).to_arrow_res()?;
        Ok(arr)
    }
}

/// Reader for Amudai boolean fields, converting them to Arrow BooleanArray
/// instances.
///
/// This reader provides optimized handling of boolean data from Amudai shards,
/// efficiently converting boolean values with proper null handling and bit-packed
/// storage as expected by Apache Arrow's BooleanArray format.
///
/// The reader wraps a specialized `BooleanFieldReader` to leverage Amudai's
/// boolean field decoding capabilities.
pub struct BooleanArrayReader(BooleanFieldReader);

impl BooleanArrayReader {
    /// Constructs a new `BooleanArrayReader`.
    ///
    /// # Arguments
    /// * `reader` - The specialized boolean field reader that handles the low-level
    ///   boolean data decoding from Amudai shard format.
    pub fn new(reader: BooleanFieldReader) -> BooleanArrayReader {
        BooleanArrayReader(reader)
    }

    /// Reads an Arrow BooleanArray for the specified position range.
    ///
    /// # Arguments
    /// * `pos_range` - The range of logical positions to read from the boolean field.
    ///
    /// # Returns
    /// Returns an `ArrayRef` containing the Arrow BooleanArray data for the specified range.
    pub fn read(&mut self, pos_range: Range<u64>) -> Result<ArrayRef, ArrowError> {
        let arr = self.0.read_array(pos_range).to_arrow_res()?;
        Ok(Arc::new(arr))
    }
}

/// Reader for Amudai struct fields, converting them to Arrow StructArray instances.
///
/// This reader handles complex structured data with multiple named fields. It coordinates
/// reading the struct's null/presence information along with all child field data,
/// then combines them into a cohesive Arrow StructArray.
///
/// The reader maintains separate array readers for each child field.
pub struct StructArrayReader {
    /// Arrow field definitions for all struct fields
    fields: ArrowFields,
    /// Reader for the struct's metadata (null/presence information)
    struct_reader: Box<dyn FieldReader>,
    /// Individual array readers for each child field
    field_readers: Vec<ArrayReader>,
}

impl StructArrayReader {
    /// Constructs a new `StructArrayReader`.
    ///
    /// # Arguments
    /// * `fields` - Arrow field definitions for all struct fields
    /// * `struct_reader` - Reader for the struct's metadata (null/presence information)
    /// * `field_readers` - Individual array readers for each child field
    pub fn new(
        fields: ArrowFields,
        struct_reader: Box<dyn FieldReader>,
        field_readers: Vec<ArrayReader>,
    ) -> StructArrayReader {
        StructArrayReader {
            fields,
            struct_reader,
            field_readers,
        }
    }

    /// Reads an Arrow StructArray for the specified position range.
    ///
    /// This method coordinates reading both the struct's null/presence information
    /// and all child field data, then combines them into a single StructArray.
    ///
    /// # Arguments
    /// * `pos_range` - The range of struct positions to read
    ///
    /// # Errors
    /// Returns an error if reading any field fails or if struct construction fails
    pub fn read(&mut self, pos_range: Range<u64>) -> Result<ArrayRef, ArrowError> {
        let values = self.struct_reader.read(pos_range.clone()).to_arrow_res()?;
        assert!(values.values.is_empty());
        assert!(values.offsets.is_none());

        let mut arrays = Vec::with_capacity(self.field_readers.len());
        for field_reader in &mut self.field_readers {
            let array = field_reader.read(pos_range.clone())?;
            arrays.push(array);
        }

        let nulls = values.presence.into_arrow_null_buffer().to_arrow_res()?;

        Ok(Arc::new(StructArray::try_new(
            self.fields.clone(),
            arrays,
            nulls,
        )?))
    }
}

/// Reader for Amudai list fields, converting them to Arrow LargeListArray instances.
///
/// This reader handles variable-length list values by coordinating the reading
/// of list metadata (offsets and null information) with the reading of child element
/// data. It properly handles the conversion from Amudai's absolute offset representation
/// to Arrow's relative offset representation.
///
/// The reader uses position range hints to optimize child data access patterns,
/// avoiding unnecessary I/O when only specific list ranges are being read.
pub struct ListArrayReader {
    /// Arrow field definition for child elements
    child_field: Arc<ArrowField>,
    /// Reader for the list structure (offsets + nulls)
    list_reader: Box<dyn FieldReader>,
    /// Array reader for child elements
    child_reader: Box<ArrayReader>,
}

impl ListArrayReader {
    /// Constructs a new `ListArrayReader`.
    ///
    /// # Arguments
    /// * `child_field` - The Arrow field definition for child elements.
    /// * `list_reader` - The field reader for the list structure (offsets + nulls).
    /// * `child_reader` - The array reader for child elements.
    pub fn new(
        child_field: Arc<ArrowField>,
        list_reader: Box<dyn FieldReader>,
        child_reader: Box<ArrayReader>,
    ) -> ListArrayReader {
        ListArrayReader {
            child_field,
            list_reader,
            child_reader,
        }
    }

    /// Reads an Arrow LargeListArray for the specified position range.
    ///
    /// This method coordinates reading list metadata (offsets and nulls) and child data,
    /// properly converting Amudai's absolute offset representation to Arrow's relative
    /// offset representation. It handles edge cases like empty lists and validates
    /// offset consistency.
    ///
    /// # Arguments
    /// * `pos_range` - The range of list positions to read
    ///
    /// # Errors
    /// Returns an error if:
    /// - Position range end overflows when incremented
    /// - Reading list or child data fails
    /// - Child offset ranges are invalid
    /// - Arrow array construction fails
    pub fn read(&mut self, pos_range: Range<u64>) -> Result<ArrayRef, ArrowError> {
        let start = pos_range.start;
        let end = pos_range.end.checked_add(1).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!("invalid pos range end: {}", pos_range.end))
        })?;

        let list_values = self.list_reader.read(start..end).to_arrow_res()?;

        let mut offsets = Offsets::from_values(list_values.values);
        let child_start = offsets.first();
        let child_end = offsets.last();
        if child_start > child_end {
            return Err(ArrowError::InvalidArgumentError("child list range".into()));
        }

        let child_array = if child_start == child_end {
            arrow::array::new_empty_array(self.child_field.data_type())
        } else {
            self.child_reader.read(child_start..child_end)?
        };

        // Convert absolute (stripe-based) child offsets into zero-based
        // child array offsets.
        offsets.normalize();

        let offset_buffer = offsets.into_arrow_offsets_64();

        let nulls = list_values
            .presence
            .into_arrow_null_buffer()
            .to_arrow_res()?;

        let list_array =
            LargeListArray::try_new(self.child_field.clone(), offset_buffer, child_array, nulls)?;
        Ok(Arc::new(list_array))
    }
}

/// Reader for Amudai map fields, converting them to Arrow MapArray instances.
///
/// This reader handles variable-length map entries by coordinating the reading
/// of map metadata (offsets and null information) with the reading of child entry
/// data. It properly handles the conversion from Amudai's absolute offset representation
/// to Arrow's relative offset representation.
///
/// The reader uses position range hints to optimize child data access patterns,
/// avoiding unnecessary I/O when only specific map ranges are being read.
pub struct MapArrayReader {
    entry_field: Arc<ArrowField>,
    kv_fields: ArrowFields,
    map_reader: Box<dyn FieldReader>,
    key_reader: Box<ArrayReader>,
    value_reader: Box<ArrayReader>,
}

impl MapArrayReader {
    pub fn new(
        entry_field: Arc<ArrowField>,
        map_reader: Box<dyn FieldReader>,
        key_reader: Box<ArrayReader>,
        value_reader: Box<ArrayReader>,
    ) -> MapArrayReader {
        let ArrowDataType::Struct(fields) = entry_field.data_type() else {
            panic!("unexpected entry type");
        };
        assert_eq!(fields.len(), 2);
        MapArrayReader {
            kv_fields: fields.clone(),
            entry_field,
            map_reader,
            key_reader,
            value_reader,
        }
    }

    pub fn read(&mut self, pos_range: Range<u64>) -> Result<ArrayRef, ArrowError> {
        let start = pos_range.start;
        let end = pos_range.end.checked_add(1).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!("invalid pos range end: {}", pos_range.end))
        })?;

        let map_values = self.map_reader.read(start..end).to_arrow_res()?;

        let mut offsets = Offsets::from_values(map_values.values);
        let child_start = offsets.first();
        let child_end = offsets.last();
        if child_start > child_end {
            return Err(ArrowError::InvalidArgumentError("child list range".into()));
        }

        let key_array = if child_start == child_end {
            // arrow::array::new_empty_array(self.entry_field.data_type())
            todo!()
        } else {
            self.key_reader.read(child_start..child_end)?
        };

        let value_array = if child_start == child_end {
            // arrow::array::new_empty_array(self.entry_field.data_type())
            todo!()
        } else {
            self.value_reader.read(child_start..child_end)?
        };

        // Convert absolute (stripe-based) child offsets into zero-based
        // child array offsets.
        offsets.normalize();

        let offset_buffer = offsets.into_arrow_offsets_32().to_arrow_res()?;

        let nulls = map_values
            .presence
            .into_arrow_null_buffer()
            .to_arrow_res()?;

        let entries =
            StructArray::try_new(self.kv_fields.clone(), vec![key_array, value_array], None)?;

        let map_array = MapArray::try_new(
            self.entry_field.clone(),
            offset_buffer,
            entries,
            nulls,
            false,
        )?;
        Ok(Arc::new(map_array))
    }
}

/// Derives estimated position ranges for child list items based on parent
/// list position ranges.
///
/// This function provides a heuristic optimization for reading nested list
/// data by estimating which child item positions are likely to be accessed
/// based on the parent list positions being read. It uses statistical estimation
/// based on average list lengths to avoid prefetching the entire child dataset
/// when only specific parent list ranges are needed.
///
/// # Purpose
///
/// When reading list arrays, we often need to read specific ranges of parent
/// lists and their corresponding child items. Rather than prefetching all child
/// items, this function estimates child position ranges using the average list
/// size, providing a balance between accuracy and performance.
///
/// # Algorithm
///
/// The estimation works as follows:
/// 1. Calculates the average list size as `item_count / list_count`
/// 2. Creates conservative bounds:
///    - `min_size` = 75% of average (for range start estimation)
///    - `max_size` = 125% of average (for range end estimation)
/// 3. For each parent list range `[start, end)`:
///    - Estimates child start as `start * min_size`
///    - Estimates child end as `end * max_size`
/// 4. Clips estimated ranges to `[0, item_count)`
/// 5. Filters out empty ranges
/// 6. Coalesces overlapping ranges to produce non-overlapping, ascending ranges
///
/// # Parameters
///
/// * `list_pos_ranges` - Iterator over position ranges in the parent list
///   to be read.
///   Each range represents a contiguous segment of parent list positions.
/// * `list_count` - Total number of lists in the parent collection. Used to
///   calculate average list size. Must be > 0 for meaningful results.
/// * `item_count` - Total number of items across all child lists. Used to
///   calculate average list size and as an upper bound for clipping.
///
/// # Returns
///
/// Returns an iterator over estimated child item position ranges. The returned
/// ranges are:
/// - Non-overlapping and in ascending order
/// - Clipped to valid bounds `[0, item_count)`
/// - Coalesced to minimize the number of separate read operations
///
/// # Limitations
///
/// - Assumes relatively uniform list size distribution
/// - May over-estimate for datasets with highly variable list sizes
/// - Does not account for actual data layout or clustering
/// - Provides hints only; actual reads may require additional data
///
/// # Usage Context
///
/// This function is primarily used within `ListArrayReader` to optimize child data
/// access patterns when constructing Apache Arrow `LargeListArray` instances from
/// Amudai shard data. The estimated ranges are passed as hints to child array readers
/// to improve I/O efficiency.
fn derive_list_item_pos_ranges_hint(
    list_pos_ranges: impl Iterator<Item = Range<u64>> + Clone,
    list_count: u64,
    item_count: u64,
) -> Vec<Range<u64>> {
    let list_size = item_count as f64 / list_count.max(1) as f64;
    let min_size = (list_size * 0.75) as u64;
    let max_size = ((list_size * 1.25) as u64).max(min_size + 1);
    list_pos_ranges
        .map(move |r| {
            r.start.saturating_mul(min_size).min(item_count)
                ..r.end.saturating_mul(max_size).min(item_count)
        })
        .filter(|r| r.start < r.end)
        .coalesce(|a, b| {
            if a.end >= b.start {
                Ok(a.start.min(b.start)..a.end.max(b.end))
            } else {
                Err((a, b))
            }
        })
        .collect()
}
