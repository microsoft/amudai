//! Array Reader Module
//!
//! Provides low-level readers for converting Amudai fields into Apache Arrow arrays.
//! Contains abstractions for reading primitive arrays from Amudai shards and exposing them as Arrow-compatible data structures.

use std::ops::Range;

use amudai_arrow_compat::{
    amudai_to_arrow_error::ToArrowResult, sequence_to_array::IntoArrowArray,
};
use amudai_format::schema::BasicTypeDescriptor;
use amudai_shard::read::{field::Field, field_decoder::FieldReader};
use arrow::{array::ArrayRef, datatypes::DataType as ArrowDataType, error::ArrowError};

/// An enum representing different array readers for Amudai fields.
/// Currently supports primitive array reading.
pub enum ArrayReader {
    /// Reader for primitive types.
    Primitive(PrimitiveArrayReader),
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
        let basic_type = stripe_field.data_type().describe().to_arrow_res()?;

        let reader = stripe_field
            .create_decoder()
            .to_arrow_res()?
            .create_reader(pos_ranges_hint)
            .to_arrow_res()?;

        Ok(ArrayReader::Primitive(PrimitiveArrayReader::new(
            arrow_type,
            stripe_field,
            basic_type,
            reader,
        )))
    }

    /// Reads an Arrow array for the specified position range.
    ///
    /// # Arguments
    /// * `pos_range` - The range of positions to read.
    ///
    /// # Errors
    /// Returns an error if reading fails.
    pub fn read(&mut self, pos_range: Range<u64>) -> Result<ArrayRef, ArrowError> {
        match self {
            ArrayReader::Primitive(reader) => reader.read(pos_range),
        }
    }
}

/// Reader for primitive Amudai fields, producing Arrow arrays.
/// Holds the Arrow type, Amudai field, type descriptor, and a field reader.
pub struct PrimitiveArrayReader {
    _arrow_type: ArrowDataType,
    _field: Field,
    _basic_type: BasicTypeDescriptor,
    reader: Box<dyn FieldReader>,
}

impl PrimitiveArrayReader {
    /// Constructs a new `PrimitiveArrayReader`.
    ///
    /// # Arguments
    /// * `arrow_type` - The Arrow data type to produce.
    /// * `field` - The Amudai field to read from.
    /// * `basic_type` - The Amudai type descriptor.
    /// * `reader` - The field reader implementation.
    pub fn new(
        arrow_type: ArrowDataType,
        field: Field,
        basic_type: BasicTypeDescriptor,
        reader: Box<dyn FieldReader>,
    ) -> PrimitiveArrayReader {
        PrimitiveArrayReader {
            _arrow_type: arrow_type,
            _field: field,
            _basic_type: basic_type,
            reader,
        }
    }

    /// Reads a primitive Arrow array for the specified position range.
    ///
    /// # Arguments
    /// * `pos_range` - The range of logical positions to read.
    ///
    /// # Errors
    /// Returns an error if reading or conversion fails.
    pub fn read(&mut self, pos_range: Range<u64>) -> Result<ArrayRef, ArrowError> {
        let seq = self.reader.read(pos_range).to_arrow_res()?;
        let arr = seq.into_arrow_array().to_arrow_res()?;
        Ok(arr)
    }
}
