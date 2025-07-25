use crate::{
    binary_block_encoder::BinaryBlockEncoderMetadata,
    block_decoder::BlockDecoder,
    block_encoder::BlockEncodingParameters,
    encodings::{self, ALIGNMENT_BYTES, EncodingContext},
};
use amudai_format::schema::BasicTypeDescriptor;
use amudai_sequence::sequence::ValueSequence;
use std::sync::Arc;

/// Block decoder specialized for binary (variable-length) data types.
///
/// The `BinaryBlockDecoder` is the counterpart to `BinaryBlockEncoder`
/// and handles decoding of encoded binary data blocks back into their
/// original form. It supports all the same data types and encoding schemes
/// as the encoder.
///
/// # Supported Decodings
///
/// - Plain encoding (direct memory copy)
/// - Run-length decoding for repeated patterns
/// - Dictionary decoding with code expansion
/// - FSST string decompression
/// - LZ4 general-purpose decompression
/// - Zstandard high-ratio decompression
/// - Complex cascaded decodings
///
/// # Usage
///
/// The decoder works with encoded blocks that include metadata headers
/// describing the encoding scheme used, allowing automatic selection
/// of the appropriate decoding strategy.
pub struct BinaryBlockDecoder {
    /// Encoding parameters used during block creation (for future use).
    _parameters: BlockEncodingParameters,
    /// Type descriptor for the binary data being decoded.
    basic_type: BasicTypeDescriptor,
    /// Shared encoding context containing decoders and configuration.
    context: Arc<EncodingContext>,
}

impl BinaryBlockDecoder {
    /// Creates a new binary block decoder.
    ///
    /// # Arguments
    ///
    /// * `parameters` - Block encoding parameters that were used during encoding
    /// * `basic_type` - Type descriptor for the binary data being decoded
    /// * `context` - Shared encoding context with available decoders
    ///
    /// # Returns
    ///
    /// A new `BinaryBlockDecoder` instance ready to decode blocks of the
    /// specified binary type.
    pub fn new(
        parameters: BlockEncodingParameters,
        basic_type: BasicTypeDescriptor,
        context: Arc<EncodingContext>,
    ) -> Self {
        Self {
            _parameters: parameters,
            basic_type,
            context,
        }
    }
}

impl BlockDecoder for BinaryBlockDecoder {
    fn decode(&self, encoded: &[u8], value_count: usize) -> amudai_common::Result<ValueSequence> {
        let metadata = BinaryBlockEncoderMetadata::read_from(encoded)?;

        let aligned_presence_size = metadata.presence_size.next_multiple_of(ALIGNMENT_BYTES);
        let values_end_offset =
            BinaryBlockEncoderMetadata::size() + aligned_presence_size + metadata.values_size;
        if encoded.len() < values_end_offset {
            return Err(amudai_common::error::Error::invalid_format(
                "Encoded block is too short",
            ));
        }

        let encoded = &encoded[BinaryBlockEncoderMetadata::size()..];
        let encoded_presence = &encoded[..metadata.presence_size];
        let presence = encodings::presence::decode_presence(encoded_presence, value_count)?;

        let encoded_data =
            &encoded[aligned_presence_size..aligned_presence_size + metadata.values_size];
        self.context.binary_encoders.decode(
            encoded_data,
            presence,
            self.basic_type,
            None,
            &self.context,
        )
    }

    fn inspect(&self, encoded: &[u8]) -> amudai_common::Result<encodings::EncodingPlan> {
        let metadata = BinaryBlockEncoderMetadata::read_from(encoded)?;
        let values_pos = BinaryBlockEncoderMetadata::size()
            + metadata.presence_size.next_multiple_of(ALIGNMENT_BYTES);
        let encoded_data = &encoded[values_pos..values_pos + metadata.values_size];
        self.context
            .binary_encoders
            .inspect(encoded_data, &self.context)
    }
}

#[cfg(test)]
mod tests {
    use super::BinaryBlockDecoder;
    use crate::{
        binary_block_encoder::BinaryBlockEncoder,
        block_decoder::BlockDecoder,
        block_encoder::{
            BlockChecksum, BlockEncoder, BlockEncodingParameters, BlockEncodingPolicy,
            BlockEncodingProfile, EncodedBlock, PresenceEncoding,
        },
        encodings::EncodingContext,
    };
    use amudai_bytes::buffer::AlignedByteVec;
    use amudai_format::schema::BasicTypeDescriptor;
    use amudai_sequence::value_sequence::BinarySequenceBuilder;
    use std::sync::Arc;

    #[test]
    fn test_round_trip() {
        let policy = BlockEncodingPolicy {
            profile: BlockEncodingProfile::Balanced,
            parameters: BlockEncodingParameters {
                presence: PresenceEncoding::Enabled,
                checksum: BlockChecksum::Enabled,
            },
            size_constraints: None,
        };
        let basic_type = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::Binary,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        };

        let context = Arc::new(EncodingContext::default());
        let mut encoder = BinaryBlockEncoder::new(policy.clone(), basic_type, Arc::clone(&context));
        let sample_len = encoder.sample_size().unwrap().value_count.start;
        let sample = arrow_array::array::BinaryArray::from_iter_values(
            (0..sample_len).map(|_| fastrand::i64(0..10000).to_string().as_bytes().to_vec()),
        );
        let block_constraints = encoder.analyze_sample(&sample).unwrap();
        let blocks_count = 16;
        let mut encoded = AlignedByteVec::new();
        let values_len = block_constraints.value_count.start;
        let mut block_offsets = vec![0];
        let mut original_data = vec![];
        for _ in 0..blocks_count {
            let nulls = (0..values_len).map(|_| fastrand::f64() >= 0.1);
            let values =
                (0..values_len).map(|_| fastrand::i64(0..10000).to_string().as_bytes().to_vec());
            let mut builder = arrow_array::builder::BinaryBuilder::new();
            let mut orig_builder = BinarySequenceBuilder::new(basic_type);
            for (value, null) in values.zip(nulls) {
                if null {
                    builder.append_null();
                    orig_builder.add_null();
                } else {
                    orig_builder.add_value(&value);
                    builder.append_value(value);
                }
            }
            let values = builder.finish();
            let encoded_block = encoder.encode(&values).unwrap();
            encoded.extend_from_slice(encoded_block.as_ref());
            block_offsets.push(encoded.len());
            original_data.push(orig_builder.build());
        }

        let decoder = BinaryBlockDecoder::new(policy.parameters, basic_type, Arc::clone(&context));
        for block in 0..blocks_count {
            let encoded_block =
                EncodedBlock::Borrowed(&encoded[block_offsets[block]..block_offsets[block + 1]]);
            let decoded = decoder.decode(encoded_block.as_ref(), values_len).unwrap();
            assert_eq!(&original_data[block].presence, &decoded.presence);
            assert_eq!(
                original_data[block].values.as_slice::<u8>(),
                decoded.values.as_slice::<u8>()
            );
            assert_eq!(
                original_data[block].offsets.as_ref().unwrap().as_slice(),
                decoded.offsets.as_ref().unwrap().as_slice()
            );
        }
    }
}
