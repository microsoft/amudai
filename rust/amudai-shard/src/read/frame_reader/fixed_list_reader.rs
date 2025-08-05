//! Sequence reader for fixed-size list sequences.
//!
//! This module defines the `FixedListSequenceReader`, which implements the
//! `SequenceReader` trait to read sequences of lists where each list has a fixed
//! number of items. It uses a field reader to obtain presence information
//! (e.g. null bitmap) and an optional nested sequence reader for the list items.

use std::ops::Range;

use amudai_common::{Result, verify_arg};
use amudai_format::projection::TypeProjectionRef;
use amudai_sequence::{
    fixed_list_sequence::FixedListSequence, sequence::Sequence, sequence_reader::SequenceReader,
};

use crate::read::field_decoder::FieldReader;

/// Reader for sequences of fixed-size lists.
///
/// A `FixedListSequenceReader` reads contiguous lists of a constant size.
/// It uses:
/// - `list_reader` to read presence (null) information for each list element,
/// - optionally `item_reader` to read nested item sequences for each list.
///
/// Each read operation returns a `FixedListSequence` encapsulating the results.
pub struct FixedListSequenceReader {
    /// Type projection for interpreting the sequence values.
    projection: TypeProjectionRef,
    /// Number of items in each fixed-size list.
    list_size: usize,
    /// Reader for presence/validity of each list element.
    list_reader: Box<dyn FieldReader>,
    /// Optional reader for nested item sequences within each list.
    item_reader: Option<Box<dyn SequenceReader>>,
}

impl FixedListSequenceReader {
    /// Constructs a new `FixedListSequenceReader`.
    ///
    /// # Arguments
    ///
    /// * `projection` - Projection of the sequence value type.
    /// * `list_size` - Fixed size (number of elements) of each list. Must be non-zero.
    /// * `list_reader` - Field reader to obtain presence (validity) bitmap.
    /// * `item_reader` - Optional sequence reader for list items.
    ///
    /// # Panics
    ///
    /// Panics if `list_size` is zero.
    pub fn new(
        projection: TypeProjectionRef,
        list_size: usize,
        list_reader: Box<dyn FieldReader>,
        item_reader: Option<Box<dyn SequenceReader>>,
    ) -> FixedListSequenceReader {
        assert_ne!(list_size, 0);
        FixedListSequenceReader {
            projection,
            list_size,
            list_reader,
            item_reader,
        }
    }
}

impl SequenceReader for FixedListSequenceReader {
    /// Reads a sequence of fixed-size lists in the given position range.
    ///
    /// Each list in the returned sequence has exactly `list_size` elements.
    /// The presence bitmap for the lists is read via `list_reader`. If
    /// `item_reader` is provided, nested item sequences are read for each list
    /// using the computed `child_range`.
    ///
    /// # Arguments
    ///
    /// * `pos_range` - range of logical positions in the list column.
    ///
    /// # Returns
    ///
    /// A boxed `Sequence` of fixed-size lists. If `pos_range` is empty (`start == end`),
    /// returns an empty `FixedListSequence`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `pos_range` is invalid (`end < start`),
    /// - underlying field or sequence readers fail.
    fn read_sequence(&mut self, pos_range: Range<u64>) -> Result<Box<dyn Sequence>> {
        verify_arg!(pos_range, pos_range.end >= pos_range.start);

        if pos_range.end == pos_range.start {
            return Ok(Box::new(FixedListSequence::empty(self.list_size)));
        }

        let child_range = pos_range
            .start
            .checked_mul(self.list_size as u64)
            .expect("start overflow")
            ..pos_range
                .end
                .checked_mul(self.list_size as u64)
                .expect("end overflow");

        let presence = self.list_reader.read_range(pos_range.clone())?.presence;

        let item = self
            .item_reader
            .as_mut()
            .map(|reader| reader.read_sequence(child_range))
            .transpose()?;

        FixedListSequence::try_new(
            Some(self.projection.clone()),
            self.list_size,
            item,
            presence,
        )
        .map(|seq| Box::new(seq) as _)
    }
}
