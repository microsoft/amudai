//! A trait for reading sequences of values from various sources.

use std::ops::Range;

use amudai_common::Result;

use crate::sequence::Sequence;

/// A trait for reading sequences of values from various sources.
///
/// `SequenceReader` provides an abstraction over different ways of reading
/// sequences of values, such as from field decoders or in-memory buffers.
/// Implementations of this trait handle the details of how to retrieve and
/// construct sequences from their underlying data sources.
pub trait SequenceReader {
    /// Reads a sequence of values from the specified logical position range.
    ///
    /// This method retrieves values starting at position `pos_range.start` up to
    /// but not including `pos_range.end`. The positions are logical indices that
    /// may map to different physical locations depending on the implementation.
    ///
    /// # Arguments
    ///
    /// * `pos_range` - A range specifying the start (inclusive) and end (exclusive)
    ///   positions of values to read. The range uses logical positions, where each
    ///   position represents a value slot in the sequence.
    ///
    /// # Returns
    ///
    /// Returns a boxed `Sequence` containing the requested values on success, or
    /// an error if the read operation fails.
    ///
    /// # Errors
    ///
    /// This method may return an error if:
    /// - The requested range is invalid (e.g., out of bounds)
    /// - The underlying data source is unavailable or corrupted
    /// - Any I/O operation fails during reading
    ///
    /// # Implementation Notes
    ///
    /// Implementations should ensure that:
    /// - Empty ranges (where `start >= end`) return an empty sequence
    /// - The returned sequence contains exactly `end - start` values
    /// - Reading is efficient for both small and large ranges
    fn read_sequence(&mut self, pos_range: Range<u64>) -> Result<Box<dyn Sequence>>;
}
