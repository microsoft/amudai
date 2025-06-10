//! Decoders for bytes-like fields.

use std::ops::Range;

use amudai_blockstream::read::{
    block_stream::BlockReaderPrefetch,
    bytes_buffer::{BytesBufferDecoder, BytesBufferReader},
};
use amudai_common::{Result, error::Error};
use amudai_format::schema::BasicTypeDescriptor;
use amudai_sequence::sequence::ValueSequence;

use crate::read::field_context::FieldContext;

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

        let encoded_buffers = field.get_encoded_buffers()?;
        if encoded_buffers.len() > 1 {
            return Err(Error::not_implemented("More than one encoding buffer"));
        }

        let encoded_buffer = encoded_buffers
            .first()
            .ok_or_else(|| Error::invalid_format("No encoded buffer found for bytes field"))?;

        let reader = field.open_data_ref(
            encoded_buffer
                .buffer
                .as_ref()
                .ok_or_else(|| Error::invalid_format("Missing buffer reference"))?,
        )?;

        let bytes_buffer = BytesBufferDecoder::from_encoded_buffer(
            reader.into_inner(),
            encoded_buffer,
            basic_type,
        )?;

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
    pub fn create_reader(
        &self,
        pos_ranges_hint: impl Iterator<Item = Range<u64>> + Clone,
    ) -> Result<Box<dyn FieldReader>> {
        let buffer_reader = self
            .bytes_buffer
            .create_reader(pos_ranges_hint, BlockReaderPrefetch::Enabled)?;
        Ok(Box::new(BytesFieldReader::new(buffer_reader)))
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
    fn read(&mut self, pos_range: Range<u64>) -> Result<ValueSequence> {
        self.buffer_reader.read(pos_range)
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
        let ranges = arrow_processing::array_sequence::sample_subranges(
            shard.directory().total_record_count as usize,
            1,
            2000,
            shard.directory().total_record_count as usize / 2,
        )
        .into_iter()
        .map(|r| r.start as u64..r.end as u64)
        .collect::<Vec<_>>();

        let mut reader = decoder.create_reader(ranges.iter().cloned()).unwrap();

        let mut null_count = 0usize;
        for range in ranges {
            let seq = reader.read(range.clone()).unwrap();
            assert_eq!(seq.len(), (range.end - range.start) as usize);
            null_count += seq.presence.count_nulls();
        }
        assert!(null_count > 500);

        let mut reader = decoder.create_reader(std::iter::empty()).unwrap();
        let seq = reader.read(1000..1020).unwrap();
        let mut len = 0;
        for i in 0..20 {
            len += seq.string_at(i).len();
        }
        assert!(len > 200);
    }
}
