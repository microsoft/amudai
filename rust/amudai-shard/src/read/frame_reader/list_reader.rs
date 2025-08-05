//! Reader implementation for list-typed sequences.
//!
//! The `ListSequenceReader` uses a low-level `FieldReader` to decode the raw list field
//! (which contains presence bitmap and offsets) and an optional child `SequenceReader`
//! to read the items in the list.  It assembles these into a [`ListSequence`] that can be
//! consumed by downstream logic.

use std::ops::Range;

use amudai_common::{Result, verify_arg};
use amudai_format::projection::TypeProjectionRef;
use amudai_sequence::{
    list_sequence::ListSequence, offsets::Offsets, sequence::Sequence,
    sequence_reader::SequenceReader,
};

use crate::read::field_decoder::FieldReader;

/// A `SequenceReader` for list-valued fields.
///
/// This reader will:
/// 1. Decode the presence bitmap and raw offsets for the list elements using
///    the provided `FieldReader`.
/// 2. Optionally invoke a child `SequenceReader` to read the individual item
///    sequences.
/// 3. Normalize offsets to be zero-based and produce a [`ListSequence`]
///    containing both the presence vector and the child item sequence.
pub struct ListSequenceReader {
    projection: TypeProjectionRef,
    /// Underlying field decoder for the list container (presence + offsets).
    list_reader: Box<dyn FieldReader>,
    /// Optional reader for the element type within each list.
    item_reader: Option<Box<dyn SequenceReader>>,
}

impl ListSequenceReader {
    /// Create a new `ListSequenceReader`.
    ///
    /// # Parameters
    ///
    /// - `projection`: type projection information for the list field,
    ///   propagated into the returned sequence.
    /// - `list_reader`: a `FieldReader` that can decode the raw list field
    ///   (presence bits and offsets).
    /// - `item_reader`: an optional `SequenceReader` for the element type;
    ///   if `None`, only the list structure is decoded without reading elements.
    pub fn new(
        projection: TypeProjectionRef,
        list_reader: Box<dyn FieldReader>,
        item_reader: Option<Box<dyn SequenceReader>>,
    ) -> ListSequenceReader {
        ListSequenceReader {
            projection,
            list_reader,
            item_reader,
        }
    }
}

impl SequenceReader for ListSequenceReader {
    /// Read a slice of list-valued entries from the underlying storage.
    ///
    /// The `pos_range` is a range of logical positions in the list column.
    /// It must satisfy `pos_range.end >= pos_range.start`.  The reader will:
    ///
    /// 1. Return an empty sequence if `pos_range` is empty.
    /// 2. Use `list_reader.read_range` to decode presence bits and offsets
    ///    over `[start..=end]` (note the extra offset to close the last list).
    /// 3. Truncate the presence vector to the requested length.
    /// 4. Build `Offsets` from the raw offset values and normalize to zero-based.
    /// 5. If an `item_reader` is provided, read the concatenated item range
    ///    and attach it as the child sequence.
    /// 6. Assemble and return a [`ListSequence`] wrapping projection,
    ///    optional child sequence, offsets, and presence.
    ///
    /// # Errors
    ///
    /// Returns an error if argument validation fails or if decoding any of
    /// the underlying sequences fails.
    fn read_sequence(&mut self, pos_range: Range<u64>) -> Result<Box<dyn Sequence>> {
        verify_arg!(pos_range, pos_range.end >= pos_range.start);
        let len = (pos_range.end - pos_range.start) as usize;
        if len == 0 {
            return Ok(Box::new(ListSequence::empty()));
        }

        // Decode the raw list field (presence + offsets) over one extra position
        // to get the end offset of the last list.
        let list_seq = self
            .list_reader
            .read_range(pos_range.start..pos_range.end + 1)?;
        // Trim the presence bits to exactly the requested number of entries.
        let mut presence = list_seq.presence;
        presence.truncate(len);

        // Build Offsets from the raw offset values.
        let mut offsets = Offsets::from_values(list_seq.values);

        // Determine the child item range (concatenated across all lists).
        let child_range = offsets.range();
        // Optionally read the child item sequence.
        let item = self
            .item_reader
            .as_mut()
            .map(|item_reader| item_reader.read_sequence(child_range))
            .transpose()?;

        // Normalize offsets so that the first list element is at index 0.
        offsets.normalize();

        ListSequence::try_new(Some(self.projection.clone()), item, offsets, presence)
            .map(|seq| Box::new(seq) as _)
    }
}
