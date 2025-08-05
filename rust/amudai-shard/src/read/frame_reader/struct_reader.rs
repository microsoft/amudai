//! Defines `StructSequenceReader`, which reads struct data by combining presence
//! information from a bit-packed field and per-field sequence readers, yielding
//! `StructSequence` objects.

use std::ops::Range;

use amudai_common::{Result, verify_arg};
use amudai_format::projection::TypeProjectionRef;
use amudai_sequence::{
    sequence::Sequence, sequence_reader::SequenceReader, struct_sequence::StructSequence,
};

use crate::read::field_decoder::FieldReader;

/// Sequence reader for struct types.
///
/// `StructSequenceReader` wraps a presence `FieldReader` together with a set of
/// `SequenceReader`s, one for each struct field, to produce a `StructSequence`
/// over a given position range.
pub struct StructSequenceReader {
    /// Reference to the projection defining the struct's layout and types.
    projection: TypeProjectionRef,
    /// Field reader responsible for reading the presence bitmap.
    struct_reader: Box<dyn FieldReader>,
    /// Sequence readers for each individual field within the struct.
    field_readers: Vec<Box<dyn SequenceReader>>,
}

impl StructSequenceReader {
    /// Creates a new `StructSequenceReader`.
    ///
    /// # Arguments
    ///
    /// * `projection` – Type projection reference describing field order and types.
    /// * `struct_reader` – Boxed `FieldReader` to parse the presence information.
    /// * `field_readers` – Vector of boxed `SequenceReader`s, one per struct field.
    pub fn new(
        projection: TypeProjectionRef,
        struct_reader: Box<dyn FieldReader>,
        field_readers: Vec<Box<dyn SequenceReader>>,
    ) -> StructSequenceReader {
        StructSequenceReader {
            projection,
            struct_reader,
            field_readers,
        }
    }
}

impl SequenceReader for StructSequenceReader {
    /// Reads a `StructSequence` over the specified position range.
    ///
    /// Performs argument validation, returns an empty sequence if the range is empty,
    /// otherwise reads the presence bitmap followed by each field's sequence, and
    /// assembles them into a `StructSequence`.
    ///
    /// # Arguments
    ///
    /// * `pos_range` – A `Range<u64>` of stripe positions with `start` (inclusive) and
    ///   `end` (exclusive).
    ///
    /// # Returns
    ///
    /// * `Ok(Box<dyn Sequence>)` containing the resulting `StructSequence`.
    /// * `Err` if the range is invalid or any field read fails.
    fn read_sequence(&mut self, pos_range: Range<u64>) -> Result<Box<dyn Sequence>> {
        verify_arg!(pos_range, pos_range.end >= pos_range.start);
        if pos_range.end == pos_range.start {
            return Ok(Box::new(StructSequence::empty()));
        }

        let presence = self.struct_reader.read_range(pos_range.clone())?.presence;
        let fields = self
            .field_readers
            .iter_mut()
            .map(|field_reader| field_reader.read_sequence(pos_range.clone()))
            .collect::<Result<Vec<_>>>()?;
        StructSequence::try_new(Some(self.projection.clone()), fields, presence)
            .map(|seq| Box::new(seq) as _)
    }
}
