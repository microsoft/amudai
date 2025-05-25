//! Field decoder traits and implementations.

use std::ops::Range;

use amudai_common::Result;
use amudai_sequence::sequence::ValueSequence;
use bytes::BytesFieldDecoder;
use list::ListFieldDecoder;
use primitive::PrimitiveFieldDecoder;

pub mod bytes;
pub mod list;
pub mod primitive;

/// Decoder for a stripe field.
///
/// This enum wraps different field decoder implementations based on the underlying
/// field type. It serves as the entry point for reading field data from a stripe,
/// allowing clients to create readers for accessing ranges of field values.
pub enum FieldDecoder {
    /// Decoder for primitive-typed fields (integers, floats, etc.)
    Primitive(PrimitiveFieldDecoder),
    /// Decoder for bytes-like fields (strings, binary, GUID, etc.)
    Bytes(BytesFieldDecoder),
    /// List offsets decoder
    List(ListFieldDecoder),
}

impl FieldDecoder {
    /// Creates a reader for efficiently accessing ranges of values in this field.
    ///
    /// # Arguments
    ///
    /// * `pos_ranges_hint` - An iterator of logical position ranges that are likely
    ///   to be accessed. These hints are used to optimize prefetching strategies
    ///   for better performance when reading from storage.
    ///
    /// # Returns
    ///
    /// A boxed field reader for accessing the field values.
    pub fn create_reader(
        &self,
        pos_ranges_hint: impl Iterator<Item = Range<u64>> + Clone,
    ) -> Result<Box<dyn FieldReader>> {
        match self {
            FieldDecoder::Primitive(decoder) => decoder.create_reader(pos_ranges_hint),
            FieldDecoder::Bytes(decoder) => decoder.create_reader(pos_ranges_hint),
            FieldDecoder::List(decoder) => decoder.create_reader(pos_ranges_hint),
        }
    }
}

/// A reader for the stripe field data that efficiently accesses and decodes
/// ranges of logical values.
///
/// Field readers provide sequential or random access to values within a field,
/// regardless of how they're physically stored or encoded. They handle the details
/// of locating blocks, decoding values, and assembling the results into a sequence.
///
/// Field readers may maintain internal caches and prefetch data to improve
/// performance for sequential reads or reads within previously hinted ranges.
pub trait FieldReader: Send + Sync + 'static {
    /// Reads values from the specified logical position range.
    ///
    /// # Arguments
    ///
    /// * `pos_range` - The range of logical positions to read.
    ///
    /// # Returns
    ///
    /// A sequence containing the decoded values for the requested range.
    fn read(&mut self, pos_range: Range<u64>) -> Result<ValueSequence>;
}
