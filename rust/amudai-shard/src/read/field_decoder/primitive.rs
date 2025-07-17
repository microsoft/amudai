//! Decoders for basic primitive type fields.

use std::ops::Range;

use amudai_blockstream::read::{
    block_stream::BlockReaderPrefetch,
    primitive_buffer::{PrimitiveBufferDecoder, PrimitiveBufferReader},
};
use amudai_common::{Result, error::Error};
use amudai_format::{defs::shard, schema::BasicTypeDescriptor};
use amudai_sequence::sequence::ValueSequence;

use crate::{read::field_context::FieldContext, write::field_encoder::EncodedField};

use super::FieldReader;

/// Decodes primitive typed fields from encoded buffer data in a stripe.
///
/// Handles numeric types like integers, floating point numbers, and other primitive
/// data that has fixed-size representations. This decoder accesses encoded primitive values
/// from block-based storage and provides methods to create readers for efficient access
/// to ranges of logical values.
#[derive(Clone)]
pub struct PrimitiveFieldDecoder {
    /// Decoder for the buffer containing the encoded primitive values
    value_buffer: PrimitiveBufferDecoder,
    // TODO: add optional presence buffer decoder
    // (when nulls are not embedded in the value buffer)
}

impl PrimitiveFieldDecoder {
    /// Creates a new primitive field decoder from a primitive buffer decoder.
    ///
    /// # Arguments
    ///
    /// * `value_buffer` - Decoder for the buffer containing the encoded primitive values
    pub fn new(value_buffer: PrimitiveBufferDecoder) -> PrimitiveFieldDecoder {
        PrimitiveFieldDecoder { value_buffer }
    }

    /// Creates a primitive field decoder from a field context.
    ///
    /// Extracts the necessary encoding information from the field's descriptor,
    /// loads the appropriate buffer reference, and initializes a decoder for that buffer.
    ///
    /// # Arguments
    ///
    /// * `field` - Field context containing metadata and references to encoded data
    ///
    /// # Returns
    ///
    /// A result containing the initialized decoder or an error if the field
    /// cannot be decoded as a primitive field
    pub(crate) fn from_field(field: &FieldContext) -> Result<PrimitiveFieldDecoder> {
        let basic_type = field.data_type().describe()?;

        let data_buffer = field.get_encoded_buffer(shard::BufferKind::Data)?;

        let reader = field.open_data_ref(
            data_buffer
                .buffer
                .as_ref()
                .ok_or_else(|| Error::invalid_format("Missing buffer reference"))?,
        )?;

        let value_buffer = PrimitiveBufferDecoder::from_encoded_buffer(
            reader.into_inner(),
            data_buffer,
            basic_type,
        )?;

        Ok(PrimitiveFieldDecoder::new(value_buffer))
    }

    /// Creates a primitive field decoder from an encoded field.
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
    ) -> Result<PrimitiveFieldDecoder> {
        let prepared_buffer = field.get_encoded_buffer(shard::BufferKind::Data)?;
        let value_buffer =
            PrimitiveBufferDecoder::from_prepared_buffer(prepared_buffer, basic_type)?;
        Ok(PrimitiveFieldDecoder::new(value_buffer))
    }

    /// Returns the basic type descriptor for the values in this field.
    pub fn basic_type(&self) -> BasicTypeDescriptor {
        self.value_buffer.basic_type()
    }

    /// Creates a reader for efficiently accessing ranges of values in this field.
    ///
    /// # Arguments
    ///
    /// * `pos_ranges_hint` - An iterator of logical position ranges that are likely
    ///   to be accessed, used for prefetching optimization
    ///
    /// # Returns
    ///
    /// A boxed field reader for accessing the primitive values
    pub fn create_reader(
        &self,
        pos_ranges_hint: impl Iterator<Item = Range<u64>> + Clone,
    ) -> Result<Box<dyn FieldReader>> {
        let buffer_reader = self
            .value_buffer
            .create_reader(pos_ranges_hint, BlockReaderPrefetch::Enabled)?;
        Ok(Box::new(PrimitiveFieldReader::new(buffer_reader)))
    }
}

struct PrimitiveFieldReader {
    buffer_reader: PrimitiveBufferReader,
}

impl PrimitiveFieldReader {
    fn new(buffer_reader: PrimitiveBufferReader) -> PrimitiveFieldReader {
        PrimitiveFieldReader { buffer_reader }
    }
}

impl FieldReader for PrimitiveFieldReader {
    fn read(&mut self, pos_range: Range<u64>) -> Result<ValueSequence> {
        self.buffer_reader.read(pos_range)
    }
}

#[cfg(test)]
mod tests {
    use amudai_objectstore::url::ObjectUrl;

    use crate::{
        read::shard::ShardOptions,
        tests::{data_generator::create_primitive_flat_test_schema, shard_store::ShardStore},
    };

    #[test]
    fn test_primitive_field_reader() {
        let shard_store = ShardStore::new();

        let shard_ref =
            shard_store.ingest_shard_with_schema(&create_primitive_flat_test_schema(), 20000);

        let shard = ShardOptions::new(shard_store.object_store.clone())
            .open(ObjectUrl::parse(&shard_ref.url).unwrap())
            .unwrap();
        assert_eq!(shard.directory().total_record_count, 20000);
        let schema = shard.fetch_schema().unwrap();
        let (_, schema_field) = schema.find_field("num_i32").unwrap().unwrap();
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

        let mut reader = decoder.create_reader(ranges.iter().cloned()).unwrap();

        let mut null_count = 0usize;
        for range in ranges {
            let seq = reader.read(range.clone()).unwrap();
            assert_eq!(seq.len(), (range.end - range.start) as usize);
            null_count += seq.presence.count_nulls();
        }
        assert!(null_count > 500);
    }
}
