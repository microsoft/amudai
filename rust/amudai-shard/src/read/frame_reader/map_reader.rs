//! Provides `MapSequenceReader`, which implements [`SequenceReader`] to decode
//! map sequences from raw field data. It uses an underlying [`FieldReader`] to read
//! presence bits and offsets, and optional child readers for keys and values.

use std::ops::Range;

use amudai_common::{Result, verify_arg};
use amudai_format::projection::TypeProjectionRef;
use amudai_sequence::{
    map_sequence::MapSequence, offsets::Offsets, sequence::Sequence,
    sequence_reader::SequenceReader,
};

use crate::read::field_decoder::FieldReader;

/// `MapSequenceReader` reads map-encoded sequence data by first decoding
/// presence bits and offsets, then delegating to optional key/value readers
/// to produce a [`MapSequence`].
pub struct MapSequenceReader {
    projection: TypeProjectionRef,
    map_reader: Box<dyn FieldReader>,
    key_reader: Option<Box<dyn SequenceReader>>,
    value_reader: Option<Box<dyn SequenceReader>>,
}

impl MapSequenceReader {
    /// Creates a new `MapSequenceReader`.
    ///
    /// # Arguments
    ///
    /// * `projection` – Reference to the type projection guiding map entry decoding.
    /// * `map_reader` – Base `FieldReader` used to read raw map representation (presence + offsets).
    /// * `key_reader` – Optional [`SequenceReader`] to decode map keys.
    /// * `value_reader` – Optional [`SequenceReader`] to decode map values.
    pub fn new(
        projection: TypeProjectionRef,
        map_reader: Box<dyn FieldReader>,
        key_reader: Option<Box<dyn SequenceReader>>,
        value_reader: Option<Box<dyn SequenceReader>>,
    ) -> MapSequenceReader {
        MapSequenceReader {
            projection,
            map_reader,
            key_reader,
            value_reader,
        }
    }
}

impl SequenceReader for MapSequenceReader {
    /// Reads a map sequence over the specified stripe position range and returns
    /// a boxed [`Sequence`] (namely a [`MapSequence`]).
    ///
    /// This method:
    /// 1. Validates that `pos_range.end >= pos_range.start`.
    /// 2. Reads the raw map field (presence bits + offsets) via the `map_reader`.
    /// 3. Constructs and normalizes [`Offsets`] to zero-based.
    /// 4. Recursively reads child sequences for keys and values, if readers are provided.
    /// 5. Builds and returns a `MapSequence` with projection, child sequences, offsets,
    ///    and presence.
    ///
    /// # Arguments
    ///
    /// * `pos_range` - range of logical Map field positions to read.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The range is invalid.
    /// - Reading the raw map field fails.
    /// - Reading any child sequence fails.
    fn read_sequence(&mut self, pos_range: Range<u64>) -> Result<Box<dyn Sequence>> {
        verify_arg!(pos_range, pos_range.end >= pos_range.start);
        let len = (pos_range.end - pos_range.start) as usize;
        if len == 0 {
            return Ok(Box::new(MapSequence::empty()));
        }

        let map_seq = self
            .map_reader
            .read_range(pos_range.start..pos_range.end + 1)?;
        let mut presence = map_seq.presence;
        presence.truncate(len);
        let mut offsets = Offsets::from_values(map_seq.values);

        let child_range = offsets.range();
        let key = self
            .key_reader
            .as_mut()
            .map(|key_reader| key_reader.read_sequence(child_range.clone()))
            .transpose()?;
        let value = self
            .value_reader
            .as_mut()
            .map(|value_reader| value_reader.read_sequence(child_range))
            .transpose()?;

        // Shift down to make all offsets 0-based.
        offsets.normalize();

        MapSequence::try_new(Some(self.projection.clone()), key, value, offsets, presence)
            .map(|seq| Box::new(seq) as _)
    }
}
