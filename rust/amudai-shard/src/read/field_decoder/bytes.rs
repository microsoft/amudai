//! Decoders for bytes-like fields.

use std::ops::Range;

use amudai_blockstream::read::{
    block_stream::{BlockReaderPrefetch, DecodedBlock},
    bytes_buffer::{BytesBufferDecoder, BytesBufferReader},
};
use amudai_common::{Result, error::Error};
use amudai_format::{
    defs::shard,
    schema::{BasicType, BasicTypeDescriptor},
};
use amudai_sequence::sequence::ValueSequence;

use crate::{read::field_context::FieldContext, write::field_encoder::EncodedField};

use super::FieldReader;

/// A decoder for binary and string field types.
///
/// This decoder handles reading of binary data (including `Binary`, `String`,
/// `FixedSizeBinary`, and `Guid` types) from encoded buffers stored in the shard
/// format.
#[derive(Clone)]
pub struct BytesFieldDecoder {
    /// The underlying bytes buffer decoder that provides access
    /// to the encoded binary data
    bytes_buffer: BytesBufferDecoder,
    // TODO: add optional presence buffer decoder
    // (when nulls are not embedded in the value buffer)
    // TODO: add optional offsets buffer decoder
    // (when offsets are not embedded in the value buffer)
}

impl BytesFieldDecoder {
    /// Creates a new `BytesFieldDecoder` from an existing `BytesBufferDecoder`.
    ///
    /// # Arguments
    ///
    /// * `bytes_buffer` - The underlying bytes buffer decoder
    pub fn new(bytes_buffer: BytesBufferDecoder) -> BytesFieldDecoder {
        BytesFieldDecoder { bytes_buffer }
    }

    /// Constructs a `BytesFieldDecoder` from a field context.
    ///
    /// This method extracts the required information from the field context,
    /// locates the appropriate encoding buffers, and initializes the decoder.
    ///
    /// # Arguments
    ///
    /// * `field` - The field context containing metadata about the field
    ///
    /// # Returns
    ///
    /// A result containing the decoder or an error
    ///
    /// # Errors
    ///
    /// Returns an error if the field has an invalid or unsupported encoding format
    pub(crate) fn from_field(field: &FieldContext) -> Result<BytesFieldDecoder> {
        let basic_type = field.data_type().describe()?;

        let data_buffer = field.get_encoded_buffer(shard::BufferKind::Data)?;
        let reader = field.open_data_ref(
            data_buffer
                .buffer
                .as_ref()
                .ok_or_else(|| Error::invalid_format("Missing buffer reference"))?,
        )?;

        let bytes_buffer =
            BytesBufferDecoder::from_encoded_buffer(reader.into_inner(), data_buffer, basic_type)?;

        Ok(BytesFieldDecoder::new(bytes_buffer))
    }

    /// Creates a `BytesFieldDecoder` from an encoded field.
    ///
    /// This method creates a decoder from a transient `EncodedField` that contains
    /// prepared encoded buffers ready for reading. Unlike `from_field`, this method
    /// works with encoded data that hasn't been written to permanent storage yet.
    ///
    /// # Arguments
    ///
    /// * `field` - The encoded field containing prepared buffers with primitive data
    /// * `basic_type` - The basic type descriptor describing the primitive data type
    pub(crate) fn from_encoded_field(
        field: &EncodedField,
        basic_type: BasicTypeDescriptor,
    ) -> Result<BytesFieldDecoder> {
        let prepared_buffer = field.get_encoded_buffer(shard::BufferKind::Data)?;
        let bytes_buffer = BytesBufferDecoder::from_prepared_buffer(prepared_buffer, basic_type)?;
        Ok(BytesFieldDecoder::new(bytes_buffer))
    }

    /// Returns the basic type descriptor for the values in this field.
    pub fn basic_type(&self) -> BasicTypeDescriptor {
        self.bytes_buffer.basic_type()
    }

    /// Creates a reader for efficiently accessing ranges of values in this field.
    ///
    /// The reader can fetch specific ranges of binary values from the field.
    /// The pos_ranges_hint parameter allows the system to optimize data access
    /// by prefetching required blocks.
    ///
    /// # Arguments
    ///
    /// * `pos_ranges_hint` - An iterator of logical position ranges that are likely
    ///   to be accessed, used for prefetching optimization
    ///
    /// # Returns
    ///
    /// A boxed field reader for accessing the values
    pub fn create_reader_with_ranges(
        &self,
        pos_ranges_hint: impl Iterator<Item = Range<u64>> + Clone,
    ) -> Result<Box<dyn FieldReader>> {
        let buffer_reader = self
            .bytes_buffer
            .create_reader_with_ranges(pos_ranges_hint, BlockReaderPrefetch::Enabled)?;
        Ok(Box::new(BytesFieldReader::new(buffer_reader)))
    }

    /// Creates a reader for efficiently accessing specific positions in this field.
    ///
    /// # Arguments
    ///
    /// * `positions_hint` - An iterator of non-descending logical positions that are likely
    ///   to be accessed. These hints are used to optimize prefetching strategies
    ///   for better performance when reading from storage. The positions must be
    ///   in non-descending order but don't need to be unique or contiguous.
    ///
    /// # Returns
    ///
    /// A boxed field reader for accessing the primitive values at the specified positions.
    pub fn create_reader_with_positions(
        &self,
        positions_hint: impl Iterator<Item = u64> + Clone,
    ) -> Result<Box<dyn FieldReader>> {
        let buffer_reader = self
            .bytes_buffer
            .create_reader_with_positions(positions_hint, BlockReaderPrefetch::Enabled)?;
        Ok(Box::new(BytesFieldReader::new(buffer_reader)))
    }
}

/// A cursor for iterator-style access to binary data in a stripe field.
///
/// `BytesFieldCursor` provides optimized access to binary values through a forward-moving
/// access pattern with sparse position requests. This approach is both more convenient
/// and more performant than using `FieldReader` to read entire position ranges when
/// the access pattern involves reading individual values at specific positions.
///
/// The cursor maintains an internal cache of the current data block and only fetches
/// new blocks when the requested position falls outside the cached block's range,
/// minimizing I/O overhead for sequential or nearby access patterns.
///
/// This cursor is best created through [`FieldDecoder::create_bytes_cursor`](super::FieldDecoder::create_bytes_cursor),
/// providing a hint of the positions that are likely to be accessed for optimal prefetching.
///
/// # Supported Types
///
/// `BytesFieldCursor` supports the following field types:
/// - `Binary` - Variable-length binary data
/// - `FixedSizeBinary` - Fixed-length binary data
/// - All fixed-size primitive types (integers, floating-point numbers, GUIDs, etc.)
pub struct BytesFieldCursor {
    block: DecodedBlock,
    reader: Box<dyn FieldReader>,
    fixed_size: usize,
    basic_type: BasicTypeDescriptor,
}

impl BytesFieldCursor {
    /// Creates a new bytes field cursor with the specified reader and type descriptor.
    ///
    /// # Arguments
    ///
    /// * `reader` - A boxed field reader that provides access to the underlying data blocks
    /// * `basic_type` - The basic type descriptor defining the structure and properties of the
    ///   field. `BytesFieldCursor` supports `Binary`, `FixedSizeBinary`, and all fixed-size
    ///   primitive types (integers, floating-point numbers, GUIDs, etc.)
    ///
    /// # Returns
    ///
    /// A new `BytesFieldCursor` ready to read binary data from the field.
    pub fn new(reader: Box<dyn FieldReader>, basic_type: BasicTypeDescriptor) -> BytesFieldCursor {
        let fixed_size = if basic_type.basic_type == BasicType::Boolean {
            1
        } else {
            basic_type.primitive_size().unwrap_or_default()
        };
        BytesFieldCursor {
            block: DecodedBlock::empty(),
            reader,
            fixed_size,
            basic_type,
        }
    }

    /// Returns the basic type descriptor for this field.
    ///
    /// # Returns
    ///
    /// A copy of the `BasicTypeDescriptor` that defines the structure of this field.
    #[inline]
    pub fn basic_type(&self) -> BasicTypeDescriptor {
        self.basic_type
    }

    /// Fetches the binary value at the specified logical position within the stripe field.
    ///
    /// This method returns the raw binary data at the given position, ignoring null status.
    /// If the value slot at this position is null, the returned data will be a placeholder:
    /// - For variable-sized binary fields: an empty buffer (`&[]`)
    /// - For fixed-size fields: a zero-filled buffer of the appropriate size
    ///
    /// Use [`fetch_nullable`] if you need to distinguish between actual data and null values.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position of the value to fetch within the stripe field
    ///
    /// # Returns
    ///
    /// A `Result` containing a byte slice reference to the binary data at the specified
    /// position, or an error if I/O fails or the position is invalid.
    ///
    /// # Performance
    ///
    /// This method is optimized for sequential or nearby access patterns. If the requested
    /// position is within the currently cached block, the access is very fast. Moving to
    /// a new block triggers I/O and is handled as a cold path.
    ///
    /// [`fetch_nullable`]: Self::fetch_nullable
    #[inline]
    pub fn fetch(&mut self, position: u64) -> Result<&[u8]> {
        self.establish_block(position)?;

        let index = self.position_index(position);
        if self.fixed_size == 0 {
            Ok(self.block.values.var_binary_at(index))
        } else {
            Ok(self.block.values.fixed_binary_at(index, self.fixed_size))
        }
    }

    /// Fetches the binary value at the specified position, returning `None` if the
    /// value slot at this position is null.
    ///
    /// This method handles nullable fields by checking the presence information
    /// before retrieving the binary data. Returns `None` for null values and
    /// `Some(data)` for valid values.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position of the value to fetch
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Option<&[u8]>` where `None` indicates a null value
    /// and `Some` contains the binary data, or an error if I/O fails.
    #[inline]
    pub fn fetch_nullable(&mut self, position: u64) -> Result<Option<&[u8]>> {
        self.establish_block(position)?;

        let index = self.position_index(position);
        let is_valid = self.block.values.presence.is_valid(index);
        let res = is_valid.then(|| {
            if self.fixed_size == 0 {
                self.block.values.var_binary_at(index)
            } else {
                self.block.values.fixed_binary_at(index, self.fixed_size)
            }
        });
        Ok(res)
    }

    /// Checks if the value at the specified position is null.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position to check for null
    ///
    /// # Returns
    ///
    /// A `Result` containing `true` if the value is null, `false` otherwise,
    /// or an error if I/O fails.
    #[inline]
    pub fn is_null(&mut self, position: u64) -> Result<bool> {
        self.establish_block(position)?;
        let index = self.position_index(position);
        Ok(self.block.values.presence.is_null(index))
    }

    /// Ensures the current block contains the specified position.
    ///
    /// If the current cached block doesn't contain the position, fetches
    /// the appropriate block from the reader.
    #[inline]
    fn establish_block(&mut self, position: u64) -> Result<()> {
        if !self.block.descriptor.contains(position) {
            self.fetch_block(position)
        } else {
            Ok(())
        }
    }

    /// Converts a logical position to a block-relative index.
    #[inline]
    fn position_index(&self, position: u64) -> usize {
        self.block.descriptor.position_index(position)
    }

    /// Fetches a new block containing the specified position.
    ///
    /// This method is marked as cold since it should be called infrequently
    /// when the cursor needs to move to a different block.
    #[cold]
    fn fetch_block(&mut self, position: u64) -> Result<()> {
        self.block = self.reader.read_containing_block(position)?;
        Ok(())
    }
}

pub struct StringFieldCursor {
    inner: BytesFieldCursor,
}

impl StringFieldCursor {
    /// Creates a new string field cursor with the specified reader and type descriptor.
    ///
    /// # Arguments
    ///
    /// * `reader` - A boxed field reader that provides access to the underlying data blocks
    /// * `basic_type` - The basic type descriptor defining the structure and properties of the
    ///   field. Must have `BasicType::String`.
    ///
    /// # Returns
    ///
    /// A new `BytesFieldCursor` ready to read binary data from the field.
    pub fn new(reader: Box<dyn FieldReader>, basic_type: BasicTypeDescriptor) -> StringFieldCursor {
        assert_eq!(basic_type.basic_type, BasicType::String);
        StringFieldCursor {
            inner: BytesFieldCursor::new(reader, basic_type),
        }
    }

    /// Returns the basic type descriptor for this field.
    ///
    /// # Returns
    ///
    /// A copy of the `BasicTypeDescriptor` that defines the structure of this field.
    #[inline]
    pub fn basic_type(&self) -> BasicTypeDescriptor {
        self.inner.basic_type
    }

    /// Fetches the string value at the specified logical position within the stripe field.
    ///
    /// This method returns the raw string data at the given position, ignoring null status.
    /// If the value slot at this position is null, the returned data will be an empty string
    /// placeholder.
    ///
    /// Use [`fetch_nullable`] if you need to distinguish between actual data and null values.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position of the value to fetch within the stripe field
    ///
    /// # Returns
    ///
    /// A `Result` containing a string slice at the specified position, or an error if I/O
    /// fails, the position is invalid or the value is not a valid Utf-8.
    ///
    /// [`fetch_nullable`]: Self::fetch_nullable
    #[inline]
    pub fn fetch(&mut self, position: u64) -> Result<&str> {
        let bytes = self.inner.fetch(position)?;
        // TODO: optimize Utf-8 validation.
        //   In cases the decoded field is strictly ASCII (indicated by the string stats),
        //   use the fast ASCII validation followed by unchecked converion.
        Self::bytes_to_str(bytes)
    }

    /// Fetches the string value at the specified position, returning `None` if the
    /// value slot at this position is null.
    ///
    /// This method handles nullable fields by checking the presence information
    /// before retrieving the binary data. Returns `None` for null values and
    /// `Some(data)` for valid values.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position of the value to fetch
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Option<&str>` where `None` indicates a null value
    /// and `Some` contains the string data, or an error if I/O fails.
    #[inline]
    pub fn fetch_nullable(&mut self, position: u64) -> Result<Option<&str>> {
        let bytes = self.inner.fetch_nullable(position)?;
        // TODO: optimize Utf-8 validation.
        //   In cases the decoded field is strictly ASCII (indicated by the string stats),
        //   use the fast ASCII validation followed by unchecked converion.
        bytes.map(|bytes| Self::bytes_to_str(bytes)).transpose()
    }

    /// Checks if the value at the specified position is null.
    ///
    /// # Arguments
    ///
    /// * `position` - The logical position to check for null
    ///
    /// # Returns
    ///
    /// A `Result` containing `true` if the value is null, `false` otherwise,
    /// or an error if I/O fails.
    #[inline]
    pub fn is_null(&mut self, position: u64) -> Result<bool> {
        self.inner.is_null(position)
    }

    fn bytes_to_str(bytes: &[u8]) -> Result<&str> {
        std::str::from_utf8(bytes).map_err(|e| Error::invalid_format(e.to_string()))
    }
}

/// A reader for accessing binary data from a field.
///
/// This reader provides access to ranges of binary values from an encoded buffer.
struct BytesFieldReader {
    buffer_reader: BytesBufferReader,
}

impl BytesFieldReader {
    /// Creates a new `BytesFieldReader` from a `BytesBufferReader`.
    ///
    /// # Arguments
    ///
    /// * `buffer_reader` - The underlying buffer reader for binary data
    fn new(buffer_reader: BytesBufferReader) -> BytesFieldReader {
        BytesFieldReader { buffer_reader }
    }
}

impl FieldReader for BytesFieldReader {
    fn read_range(&mut self, pos_range: Range<u64>) -> Result<ValueSequence> {
        self.buffer_reader.read_range(pos_range)
    }

    fn read_containing_block(&mut self, position: u64) -> Result<DecodedBlock> {
        self.buffer_reader.read_containing_block(position)
    }
}

#[cfg(test)]
mod tests {
    use amudai_objectstore::url::ObjectUrl;

    use crate::{
        read::shard::ShardOptions,
        tests::{data_generator::create_bytes_flat_test_schema, shard_store::ShardStore},
    };

    #[test]
    fn test_bytes_field_reader() {
        let shard_store = ShardStore::new();

        let shard_ref =
            shard_store.ingest_shard_with_schema(&create_bytes_flat_test_schema(), 20000);

        let shard = ShardOptions::new(shard_store.object_store.clone())
            .open(ObjectUrl::parse(&shard_ref.url).unwrap())
            .unwrap();
        assert_eq!(shard.directory().total_record_count, 20000);
        let schema = shard.fetch_schema().unwrap();
        let (_, schema_field) = schema.find_field("str_words").unwrap().unwrap();
        let stripe = shard.open_stripe(0).unwrap();
        let field = stripe
            .open_field(schema_field.data_type().unwrap())
            .unwrap();
        let decoder = field.create_decoder().unwrap();

        fastrand::seed(987321546);
        let ranges = amudai_arrow_processing::array_sequence::sample_subranges(
            shard.directory().total_record_count as usize,
            1,
            2000,
            shard.directory().total_record_count as usize / 2,
        )
        .into_iter()
        .map(|r| r.start as u64..r.end as u64)
        .collect::<Vec<_>>();

        let mut reader = decoder
            .create_reader_with_ranges(ranges.iter().cloned())
            .unwrap();

        let mut null_count = 0usize;
        for range in ranges {
            let seq = reader.read_range(range.clone()).unwrap();
            assert_eq!(seq.len(), (range.end - range.start) as usize);
            null_count += seq.presence.count_nulls();
        }
        assert!(null_count > 500);

        let mut reader = decoder
            .create_reader_with_ranges(std::iter::empty())
            .unwrap();
        let seq = reader.read_range(1000..1020).unwrap();
        let mut len = 0;
        for i in 0..20 {
            len += seq.string_at(i).len();
        }
        assert!(len > 200);
    }
}
