//! Provides a `SequenceReader` implementation for reading sequences of typed values
//! from shard frame data. It uses a [`FieldReader`](crate::read::field_decoder::FieldReader)
//! to decode raw bytes into values according to a given type projection (`TypeProjectionRef`).

use std::ops::Range;

use amudai_common::{Result, verify_arg};
use amudai_format::projection::TypeProjectionRef;
use amudai_sequence::{
    sequence::{Sequence, ValueSequence},
    sequence_reader::SequenceReader,
};

use crate::read::field_decoder::FieldReader;

/// A `SequenceReader` that produces a `ValueSequence` by delegating to a boxed [`FieldReader`].
pub struct ValueSequenceReader {
    projection: TypeProjectionRef,
    field_reader: Box<dyn FieldReader>,
}

impl ValueSequenceReader {
    /// Constructs a new `ValueSequenceReader`.
    ///
    /// # Parameters
    /// - `projection`: A reference to the type projection that defines the schema
    ///   element of this reader.
    /// - `field_reader`: A boxed implementation of `FieldReader` responsible
    ///   for actually decoding the raw field data.
    pub fn new(
        projection: TypeProjectionRef,
        field_reader: Box<dyn FieldReader>,
    ) -> ValueSequenceReader {
        ValueSequenceReader {
            projection,
            field_reader,
        }
    }
}

impl SequenceReader for ValueSequenceReader {
    /// Reads a sequence of values from the underlying stripe field within the
    /// given position range.
    ///
    /// # Parameters
    /// - `pos_range`: A `Range<u64>` specifying the start (inclusive) and end
    ///   (exclusive) positions within the stripe.
    ///
    /// # Returns
    /// A `Result` containing a boxed `Sequence` of typed values, or an error
    /// if validation or decoding fails.
    fn read_sequence(&mut self, pos_range: Range<u64>) -> Result<Box<dyn Sequence>> {
        verify_arg!(pos_range, pos_range.end >= pos_range.start);

        if pos_range.end == pos_range.start {
            return Ok(Box::new(ValueSequence::empty(
                self.projection.type_descriptor(),
            )));
        }

        self.field_reader
            .read_range(pos_range)
            .map(|seq| Box::new(seq) as _)
    }
}
