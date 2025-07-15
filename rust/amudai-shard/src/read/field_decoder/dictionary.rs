//! Decoder for dictionary-encoded fields.

use prost::Message;
use std::ops::Range;

use amudai_blockstream::read::{
    block_stream::BlockReaderPrefetch,
    primitive_buffer::{PrimitiveBufferDecoder, PrimitiveBufferReader},
};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::{Result, error::Error};
use amudai_format::{
    defs::{
        common::DataRef,
        shard::{
            BufferKind, DictionaryFixedSizeValuesSection, DictionaryVarSizeValuesSection,
            ValueDictionaryHeader,
        },
    },
    schema::{BasicType, BasicTypeDescriptor},
};
use amudai_sequence::{
    offsets::Offsets,
    presence::{Presence, PresenceBuilder},
    sequence::ValueSequence,
    values::Values,
};

use super::FieldReader;
use crate::read::{artifact_reader::ArtifactReader, field_context::FieldContext};

/// Decoder for dictionary-encoded fields.
///
/// Dictionary encoding compresses data by mapping duplicate values to compact integer codes.
/// This decoder reconstructs original values by mapping codes back to their corresponding
/// dictionary entries.
///
/// ## Input Format
///
/// Expects two encoded buffers:
///  - **Codes buffer**: U32 integer sequence referencing dictionary entries.
///  - **Dictionary buffer**: The dictionary buffer follows the ValueDictionaryHeader protobuf message design.
#[derive(Clone)]
pub struct DictionaryFieldDecoder {
    /// Decoder for codes buffer containing integer codes referencing dictionary entries
    codes_buffer: PrimitiveBufferDecoder,
    /// Reader for the `VALUE_DICTIONARY` buffer containing dictionary entries.
    dictionary_reader: ArtifactReader,
    /// Reference to the dictionary entries buffer.
    dictionary_ref: DataRef,
    /// Basic type descriptor of the encoded field
    basic_type: BasicTypeDescriptor,
}

impl DictionaryFieldDecoder {
    /// Creates a new dictionary decoder from existing buffer decoders.
    ///
    /// # Arguments
    /// * `codes_buffer` - Decoder for the codes buffer
    /// * `dictionary_reader` - Reader for the dictionary entries buffer
    /// * `dictionary_ref` - Reference to the dictionary entries buffer
    /// * `basic_type` - Basic type descriptor of the encoded field
    pub fn new(
        codes_buffer: PrimitiveBufferDecoder,
        dictionary_reader: ArtifactReader,
        dictionary_ref: DataRef,
        basic_type: BasicTypeDescriptor,
    ) -> DictionaryFieldDecoder {
        DictionaryFieldDecoder {
            codes_buffer,
            dictionary_reader,
            dictionary_ref,
            basic_type,
        }
    }

    /// Constructs a dictionary decoder from field context.
    ///
    /// Extracts required information from field context, locates appropriate
    /// encoding buffers, and initializes the decoder.
    ///
    /// # Arguments
    /// * `field` - Field context containing metadata
    ///
    /// # Returns
    /// Configured decoder or error for invalid/unsupported encoding format
    pub(crate) fn from_field(field: &FieldContext) -> Result<DictionaryFieldDecoder> {
        let basic_type = field.data_type().describe()?;

        let encoded_buffers = field.get_encoded_buffers()?;
        if encoded_buffers.len() != 2 {
            return Err(Error::invalid_format(
                "Dictionary encoding requires exactly two buffers: codes and dictionary entries",
            ));
        }

        // First buffer is the codes buffer (u32 values)
        let codes_encoded_buffer = &encoded_buffers[0];
        let codes_reader = field.open_data_ref(
            codes_encoded_buffer
                .buffer
                .as_ref()
                .ok_or_else(|| Error::invalid_format("Missing codes buffer reference"))?,
        )?;

        let codes_buffer = PrimitiveBufferDecoder::from_encoded_buffer(
            codes_reader.into_inner(),
            codes_encoded_buffer,
            BasicTypeDescriptor {
                basic_type: BasicType::Int32,
                fixed_size: 0,
                signed: false,
                extended_type: Default::default(),
            },
        )?;

        // Second buffer is the dictionary entries buffer
        let dictionary_encoded_buffer = &encoded_buffers[1];
        if dictionary_encoded_buffer.kind() != BufferKind::ValueDictionary {
            return Err(Error::invalid_format(
                "Second buffer must be a dictionary buffer",
            ));
        }
        let dictionary_ref = dictionary_encoded_buffer
            .buffer
            .as_ref()
            .ok_or_else(|| Error::invalid_format("Missing dictionary buffer reference"))?;
        let dictionary_reader = field.open_data_ref(dictionary_ref)?;

        Ok(DictionaryFieldDecoder::new(
            codes_buffer,
            dictionary_reader,
            dictionary_ref.clone(),
            basic_type,
        ))
    }

    /// Returns the original basic type descriptor before dictionary encoding.
    pub fn basic_type(&self) -> BasicTypeDescriptor {
        self.basic_type
    }

    /// Creates a reader for accessing value ranges in this field.
    ///
    /// The reader fetches specific ranges of dictionary-decoded values.
    /// Position range hints enable optimization through prefetching.
    ///
    /// # Arguments
    /// * `pos_ranges_hint` - Iterator of logical position ranges for prefetching optimization
    ///
    /// # Returns
    /// Boxed field reader for accessing dictionary-decoded values
    pub fn create_reader(
        &self,
        pos_ranges_hint: impl Iterator<Item = Range<u64>> + Clone,
    ) -> Result<Box<dyn FieldReader>> {
        let codes_reader = self
            .codes_buffer
            .create_reader(pos_ranges_hint.clone(), BlockReaderPrefetch::Enabled)?;

        Ok(Box::new(DictionaryFieldReader {
            codes_reader,
            dictionary_reader: self.dictionary_reader.clone(),
            dictionary_ref: self.dictionary_ref.clone(),
            basic_type: self.basic_type,
            dictionary_buffers: None,
            dictionary_offsets: None,
            sorted_ids: None,
            null_code: None,
            fixed_value_size: None,
        }))
    }
}

/// Reader for dictionary-encoded field data.
///
/// Decodes dictionary-compressed data by reading codes and mapping them
/// to corresponding values in the dictionary.
pub struct DictionaryFieldReader {
    /// Reader for codes buffer
    codes_reader: PrimitiveBufferReader,
    /// Reader for dictionary entries buffer
    dictionary_reader: ArtifactReader,
    /// Reference to the dictionary entries buffer
    dictionary_ref: DataRef,
    /// Basic type descriptor of the encoded field
    basic_type: BasicTypeDescriptor,
    /// Cached dictionary buffers as single byte sequence
    dictionary_buffers: Option<Vec<u8>>,
    /// Offsets for variable-length dictionary entries (indexed by code)
    dictionary_offsets: Option<Vec<u64>>,
    /// Sorted IDs section for dictionary entries (if available)
    sorted_ids: Option<Vec<u32>>,
    /// Null value code if null entry exists in dictionary
    null_code: Option<u32>,
    /// For fixed-sized values, their size in bytes as stored in dictionary header.
    fixed_value_size: Option<u32>,
}

impl DictionaryFieldReader {
    /// Loads and caches the complete dictionary for efficient lookup.
    ///
    /// Reads all dictionary entries once and caches them for efficient
    /// value reconstruction during decoding.
    fn establish_dictionary(&mut self) -> Result<()> {
        if self.dictionary_buffers.is_some() {
            return Ok(());
        }

        // Read the entire dictionary buffer.
        let dictionary_buf = self.dictionary_reader.read_at(&self.dictionary_ref)?;

        // Read and validate the dictionary header.
        let header_buf = amudai_format::checksum::validate_message(&dictionary_buf)?;
        let header = ValueDictionaryHeader::decode(header_buf).map_err(|e| {
            Error::invalid_format(format!("Failed to decode dictionary header: {e}"))
        })?;

        // Skip the header bytes as all the other section ranges are relative
        // to the end of the header section.
        let dictionary_buf = &dictionary_buf[header_buf.len()
            + amudai_format::defs::MESSAGE_LEN_SIZE
            + amudai_format::defs::CHECKSUM_SIZE..];

        // Load dictionary values based on their type.
        let values_range = header.values_section_range.ok_or_else(|| {
            Error::invalid_format("Dictionary header does not contain values section reference")
        })?;
        let values_buf = amudai_format::checksum::validate_message(
            &dictionary_buf[values_range.start as usize..values_range.end as usize],
        )?;
        if let Some(fixed_value_size) = header.fixed_value_size {
            assert_eq!(self.basic_type.fixed_size, fixed_value_size);
            let values = DictionaryFixedSizeValuesSection::decode(values_buf).map_err(|e| {
                Error::invalid_format(format!("Failed to decode fixed-size values section: {e}"))
            })?;
            self.dictionary_buffers = Some(values.values);
        } else {
            let mut values = DictionaryVarSizeValuesSection::decode(values_buf).map_err(|e| {
                Error::invalid_format(format!(
                    "Failed to decode variable-size values section: {e}"
                ))
            })?;
            if let Some(values) = values.values.take() {
                self.dictionary_offsets = Some(values.offsets);
                self.dictionary_buffers = Some(values.data);
            }
        }

        // Load sorted IDs if available.
        if let Some(sorted_ids_range) = header.sorted_ids_section_range {
            let sorted_ids_buf = amudai_format::checksum::validate_message(
                &dictionary_buf[sorted_ids_range.start as usize..sorted_ids_range.end as usize],
            )?;
            let sorted_ids =
                amudai_format::defs::shard::DictionarySortedIdsSection::decode(sorted_ids_buf)
                    .map_err(|e| {
                        Error::invalid_format(format!("Failed to decode sorted IDs section: {e}"))
                    })?;
            self.sorted_ids = Some(sorted_ids.sorted_ids);
        }

        // Load the rest of dictionary metadata.
        self.null_code = header.null_id;
        self.fixed_value_size = header.fixed_value_size;

        Ok(())
    }

    /// Reconstructs original values by mapping codes to dictionary entries.
    ///
    /// # Arguments
    /// * `codes_sequence` - Value sequence containing the dictionary codes
    ///
    /// # Returns
    /// Value sequence with reconstructed original values
    fn reconstruct_values(&mut self, codes_sequence: ValueSequence) -> Result<ValueSequence> {
        self.establish_dictionary()?;

        let dictionary = self.dictionary_buffers.as_ref().unwrap();
        let codes_slice = codes_sequence.values.as_slice::<u32>();

        if let Some(fixed_value_size) = self.fixed_value_size {
            self.reconstruct_fixed_sized_values(codes_slice, dictionary, fixed_value_size as usize)
        } else {
            let offsets = self.dictionary_offsets.as_ref().unwrap();
            self.reconstruct_variable_sized_values(codes_slice, dictionary, offsets)
        }
    }

    /// Reconstructs fixed-size values from codes using dictionary buffers.
    ///
    /// Each code maps to a fixed-size value in the dictionary buffer.
    /// Values are stored consecutively, so code `n` maps to bytes
    /// `[n * value_size .. (n+1) * value_size]` in the dictionary buffer.
    fn reconstruct_fixed_sized_values(
        &self,
        codes_slice: &[u32],
        dictionary_buffers: &[u8],
        value_size: usize,
    ) -> Result<ValueSequence> {
        let mut values = AlignedByteVec::new();

        let presence = if let Some(null_code) = self.null_code {
            let mut presence = PresenceBuilder::new();
            let empty_value = vec![0u8; value_size];
            for &code in codes_slice {
                if code == null_code {
                    values.extend_from_slice(&empty_value);
                    presence.add_null();
                } else {
                    let start_index = (code as usize) * value_size;
                    let value = dictionary_buffers
                        .get(start_index..start_index + value_size)
                        .ok_or_else(|| {
                            Error::invalid_format(format!("Out of bounds dictionary code: {code}"))
                        })?;
                    values.extend_from_slice(value);
                    presence.add_non_null();
                }
            }
            presence.build()
        } else {
            for &code in codes_slice {
                let start_index = (code as usize) * value_size;
                let value = dictionary_buffers
                    .get(start_index..start_index + value_size)
                    .ok_or_else(|| {
                        Error::invalid_format(format!("Out of bounds dictionary code: {code}"))
                    })?;
                values.extend_from_slice(value);
            }
            Presence::Trivial(codes_slice.len())
        };

        Ok(ValueSequence {
            values: Values::from_vec(values),
            offsets: None,
            presence,
            type_desc: self.basic_type,
        })
    }

    /// Reconstructs variable-size values from codes using dictionary buffers.
    ///
    /// Each code maps to a variable-size value in the dictionary buffer.
    /// The `dictionary_offsets` array contains start positions for each value,
    /// so code `n` maps to bytes `[offsets[n] .. offsets[n+1]]` in the dictionary buffer.
    fn reconstruct_variable_sized_values(
        &self,
        codes_slice: &[u32],
        dictionary_buffers: &[u8],
        dictionary_offsets: &[u64],
    ) -> Result<ValueSequence> {
        let mut values = AlignedByteVec::new();
        let mut offsets = Offsets::with_capacity(codes_slice.len());

        let presence = if let Some(null_code) = self.null_code {
            let mut presence = PresenceBuilder::new();
            for &code in codes_slice {
                if code == null_code {
                    presence.add_null();
                    offsets.push_empty(1);
                } else {
                    if code as usize >= dictionary_offsets.len() - 1 {
                        return Err(Error::invalid_format(format!(
                            "Out of bounds dictionary code: {code}"
                        )));
                    }
                    let value_start = dictionary_offsets[code as usize] as usize;
                    let value_end = dictionary_offsets[code as usize + 1] as usize;
                    let value = &dictionary_buffers[value_start..value_end];
                    values.extend_from_slice(value);
                    offsets.push_length(value.len());
                    presence.add_non_null();
                }
            }
            presence.build()
        } else {
            for &code in codes_slice {
                if code as usize >= dictionary_offsets.len() - 1 {
                    return Err(Error::invalid_format(format!(
                        "Out of bounds dictionary code: {code}"
                    )));
                }
                let value_start = dictionary_offsets[code as usize] as usize;
                let value_end = dictionary_offsets[code as usize + 1] as usize;
                let value = &dictionary_buffers[value_start..value_end];
                values.extend_from_slice(value);
                offsets.push_length(value.len());
            }
            Presence::Trivial(codes_slice.len())
        };

        Ok(ValueSequence {
            values: Values::from_vec(values),
            offsets: Some(offsets),
            presence,
            type_desc: self.basic_type,
        })
    }
}

impl FieldReader for DictionaryFieldReader {
    /// Reads dictionary-encoded values from the specified position range.
    ///
    /// Reads codes for the requested range, then reconstructs original values
    /// by mapping them through the dictionary.
    ///
    /// # Arguments
    /// * `pos_range` - Logical position range to read (inclusive start, exclusive end)
    ///
    /// # Returns
    /// Value sequence containing reconstructed original values
    fn read(&mut self, pos_range: Range<u64>) -> Result<ValueSequence> {
        // First read the codes for the requested range
        let codes_sequence = self.codes_reader.read(pos_range)?;

        // Then reconstruct the original values using the dictionary
        self.reconstruct_values(codes_sequence)
    }
}
