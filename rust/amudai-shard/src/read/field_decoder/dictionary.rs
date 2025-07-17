//! Decoder for dictionary-encoded fields.

use amudai_io::{PrecachedReadAt, ReadAt, SlicedFile};
use prost::Message;
use std::{
    ops::Range,
    sync::{Arc, OnceLock},
};

use amudai_blockstream::{
    read::{
        block_stream::BlockReaderPrefetch,
        primitive_buffer::{PrimitiveBufferDecoder, PrimitiveBufferReader},
    },
    write::PreparedEncodedBuffer,
};
use amudai_bytes::{Bytes, buffer::AlignedByteVec};
use amudai_common::{Result, error::Error, verify_arg, verify_data};
use amudai_format::{
    defs::shard::{
        BufferKind, DictionaryFixedSizeValuesSection, DictionarySortedIdsSection,
        DictionaryVarSizeValuesSection, EncodedBuffer, ValueDictionaryHeader,
    },
    schema::{BasicType, BasicTypeDescriptor},
};
use amudai_sequence::{
    offsets::Offsets, presence::Presence, sequence::ValueSequence, values::Values,
};

use super::FieldReader;
use crate::{read::field_context::FieldContext, write::field_encoder::EncodedField};

/// Decoder for 'VALUE_DICTIONARY' buffer containing value mappings and metadata.
///
/// The `DictionaryDecoder` provides access to dictionary-encoded data by managing
/// the decoding of dictionary headers, value sections, and optional sorted ID sections.
/// It uses lazy loading to defer expensive operations until actually needed.
///
/// # Dictionary Structure
///
/// A dictionary buffer contains:
/// - **Header**: Metadata about the dictionary (type info, section offsets, null handling)
/// - **Values Section**: The actual dictionary values (fixed or variable size)
/// - **Sorted IDs Section** (optional): Pre-sorted dictionary IDs for efficient lookups
pub struct DictionaryDecoder {
    /// Reader spanning the entire encoded dictionary buffer.
    ///
    /// This reader provides access to the raw dictionary data, including the header
    /// and all sections. It's sliced to exactly match the dictionary buffer boundaries.
    reader: SlicedFile<Arc<dyn ReadAt>>,

    /// Lazily-loaded dictionary header with cached buffer prefix.
    ///
    /// The header contains metadata about the dictionary structure, including:
    /// - Value type information and fixed size (if applicable)
    /// - Section offsets for values and sorted IDs
    /// - Null value handling (null_id)
    ///
    /// Uses `OnceLock` to ensure thread-safe single initialization.
    header: OnceLock<Header>,

    /// Lazily-loaded values section containing the actual dictionary entries.
    ///
    /// The values section stores the unique values that codes map to. It can be
    /// either fixed-size (for primitive types) or variable-size (for strings/bytes).
    ///
    /// Uses `OnceLock` to ensure thread-safe single initialization.
    values: OnceLock<DictionaryValues>,

    /// Lazily-loaded optional sorted IDs section for efficient lookups.
    ///
    /// When present, this section contains dictionary IDs sorted by their corresponding
    /// values, enabling binary search operations for value-to-code mapping.
    ///
    /// Uses `OnceLock` to ensure thread-safe single initialization.
    sorted_ids: OnceLock<Option<DictionarySortedIdsSection>>,
}

impl DictionaryDecoder {
    /// Creates a new `DictionaryDecoder` from an encoded buffer.
    ///
    /// This method initializes a decoder for a dictionary buffer that follows the
    /// ValueDictionary format. The buffer must contain a valid dictionary header
    /// followed by the values section and optionally a sorted IDs section.
    ///
    /// # Arguments
    ///
    /// * `reader` - Reader providing access to the underlying data storage
    /// * `encoded_buffer` - Buffer descriptor containing the dictionary data range and metadata
    ///
    /// # Returns
    ///
    /// A new `DictionaryDecoder` instance ready for decoding operations.
    pub fn from_encoded_buffer(
        reader: Arc<dyn ReadAt>,
        encoded_buffer: &EncodedBuffer,
    ) -> Result<DictionaryDecoder> {
        verify_arg!(
            encoded_buffer,
            encoded_buffer.kind == BufferKind::ValueDictionary as i32
        );
        let range: Range<u64> = encoded_buffer
            .buffer
            .as_ref()
            .ok_or_else(|| Error::invalid_format("missing buffer ref in EncodedBuffer"))?
            .into();

        Ok(DictionaryDecoder {
            reader: SlicedFile::new(reader, range),
            header: OnceLock::new(),
            values: OnceLock::new(),
            sorted_ids: OnceLock::new(),
        })
    }

    /// Creates a new `DictionaryDecoder` from a prepared encoded buffer.
    ///
    /// This is a convenience method for creating a decoder from a `PreparedEncodedBuffer`,
    /// which is typically used when working with transient dictionary data that hasn't
    /// been written to permanent storage yet.
    ///
    /// # Arguments
    ///
    /// * `prepared_buffer` - Prepared buffer containing dictionary data and descriptor
    ///
    /// # Returns
    ///
    /// A new `DictionaryDecoder` instance.
    pub fn from_prepared_buffer(
        prepared_buffer: &PreparedEncodedBuffer,
    ) -> Result<DictionaryDecoder> {
        Self::from_encoded_buffer(prepared_buffer.data.clone(), &prepared_buffer.descriptor)
    }

    /// Verifies that the dictionary's value type matches the expected type descriptor.
    ///
    /// This method ensures type safety by validating that the dictionary was encoded
    /// with the same type information as expected by the decoder. It checks both the
    /// basic type and fixed size (if applicable).
    ///
    /// # Arguments
    ///
    /// * `type_desc` - Expected type descriptor to validate against
    ///
    /// # Returns
    ///
    /// `Ok(())` if the types match, error otherwise.
    pub fn verify_basic_type(&self, type_desc: BasicTypeDescriptor) -> Result<()> {
        let header = self.header()?;
        verify_data!(
            header.value_type,
            header.value_type == type_desc.basic_type as u32
        );
        if let Some(fixed_size) = header.fixed_value_size {
            verify_data!(header.fixed_value_size, type_desc.fixed_size == fixed_size);
        }
        Ok(())
    }

    /// Returns a reference to the dictionary header.
    ///
    /// The header contains metadata about the dictionary structure, including:
    /// - Value type information
    /// - Fixed value size (for fixed-size types)
    /// - Section offsets and ranges
    /// - Null value ID (if nulls are supported)
    ///
    /// The header is loaded lazily on first access and cached for subsequent calls.
    ///
    /// # Returns
    ///
    /// Reference to the `ValueDictionaryHeader`.
    ///
    /// # Errors
    ///
    /// Returns an error if the header cannot be loaded or decoded.
    pub fn header(&self) -> Result<&ValueDictionaryHeader> {
        Ok(&self.establish_header()?.inner)
    }

    /// Returns a reference to the dictionary values section.
    ///
    /// The values section contains the actual dictionary entries that codes map to.
    /// It can be either fixed-size (for primitive types) or variable-size (for strings).
    ///
    /// The values are loaded lazily on first access and cached for subsequent calls.
    ///
    /// # Returns
    ///
    /// Reference to the `DictionaryValues` enum containing the values section.
    ///
    /// # Errors
    ///
    /// Returns an error if the values section cannot be loaded or decoded.
    pub fn values(&self) -> Result<&DictionaryValues> {
        self.establish_values()
    }

    /// Returns a reference to the optional sorted IDs section.
    ///
    /// The sorted IDs section contains dictionary IDs sorted by their corresponding
    /// values. This enables binary search operations for value-to-code mapping.
    /// Not all dictionaries include this section.
    ///
    /// The sorted IDs are loaded lazily on first access and cached for subsequent calls.
    ///
    /// # Returns
    ///
    /// Optional reference to the `DictionarySortedIdsSection`.
    /// Returns `None` if the dictionary doesn't include sorted IDs.
    ///
    /// # Errors
    ///
    /// Returns an error if the sorted IDs section exists but cannot be loaded or decoded.
    pub fn sorted_ids(&self) -> Result<Option<&DictionarySortedIdsSection>> {
        self.establish_sorted_ids()
    }

    /// Decodes a sequence of dictionary codes into a complete `ValueSequence`.
    ///
    /// This is the primary decoding method that reconstructs original values from
    /// dictionary codes. It handles both values and presence information (nulls).
    ///
    /// # Arguments
    ///
    /// * `codes` - Array of dictionary codes to decode
    /// * `type_desc` - Type descriptor for the expected output values
    ///
    /// # Returns
    ///
    /// A `ValueSequence` containing:
    /// - Reconstructed values
    /// - Offsets (for variable-size values)
    /// - Presence information (null/non-null indicators)
    /// - Type descriptor
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Type verification fails
    /// - Values or presence information cannot be decoded
    /// - Dictionary codes are invalid
    pub fn decode_sequence(
        &self,
        codes: &[u32],
        type_desc: BasicTypeDescriptor,
    ) -> Result<ValueSequence> {
        self.verify_basic_type(type_desc)?;
        let (values, offsets) = self.decode_values(codes)?;
        let presence = self.decode_presence(codes)?;
        Ok(ValueSequence {
            values,
            offsets,
            presence,
            type_desc,
        })
    }

    /// Decodes dictionary codes into values and optional offsets.
    ///
    /// This method reconstructs the actual data values from dictionary codes.
    /// For fixed-size values, no offsets are returned. For variable-size values,
    /// offsets indicate the start/end positions of each value in the values buffer.
    ///
    /// # Arguments
    ///
    /// * `codes` - Array of dictionary codes to decode
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - `Values`: The reconstructed value data
    /// - `Option<Offsets>`: Offset information for variable-size values, or `None` for fixed-size
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The values section cannot be accessed
    /// - Dictionary codes are out of bounds
    /// - Value decoding fails
    pub fn decode_values(&self, codes: &[u32]) -> Result<(Values, Option<Offsets>)> {
        self.values()?.decode(codes)
    }

    /// Decodes dictionary codes into presence information.
    ///
    /// This method determines which values are null based on the dictionary's
    /// null ID configuration. If the dictionary supports nulls, it creates
    /// appropriate presence indicators.
    ///
    /// # Arguments
    ///
    /// * `codes` - Array of dictionary codes to analyze for null values
    pub fn decode_presence(&self, codes: &[u32]) -> Result<Presence> {
        let header = self.header()?;
        if let Some(null_id) = header.null_id {
            if codes.iter().all(|&code| code == null_id) {
                Ok(Presence::Nulls(codes.len()))
            } else {
                let mut bytes = AlignedByteVec::zeroed(codes.len());
                codes
                    .iter()
                    .zip(bytes.iter_mut())
                    .for_each(|(&code, byte)| *byte = (code != null_id) as u8);
                Ok(Presence::Bytes(bytes))
            }
        } else {
            Ok(Presence::Trivial(codes.len()))
        }
    }
}

impl DictionaryDecoder {
    /// Establishes and returns a reference to the values section, loading it if necessary.
    fn establish_values(&self) -> Result<&DictionaryValues> {
        if let Some(values) = self.values.get() {
            Ok(values)
        } else {
            let header = self.establish_header()?;
            let values = header.load_values()?;
            values.verify(header.inner.null_id)?;
            let _ = self.values.set(values);
            Ok(self.values.get().expect("values"))
        }
    }

    /// Establishes and returns a reference to the sorted IDs section, loading it if necessary.
    fn establish_sorted_ids(&self) -> Result<Option<&DictionarySortedIdsSection>> {
        if let Some(sorted_ids) = self.sorted_ids.get() {
            Ok(sorted_ids.as_ref())
        } else {
            let header = self.establish_header()?;
            let sorted_ids = header.load_sorted_ids()?;
            let _ = self.sorted_ids.set(sorted_ids);
            Ok(self.sorted_ids.get().expect("sorted_ids").as_ref())
        }
    }

    /// Establishes and returns a reference to the header, loading it if necessary.
    fn establish_header(&self) -> Result<&Header> {
        if let Some(header) = self.header.get() {
            Ok(header)
        } else {
            let header = Header::load(self.reader.clone())?;
            let _ = self.header.set(header);
            Ok(self.header.get().expect("header"))
        }
    }
}

/// Dictionary values supporting both fixed-size and variable-size data types.
///
/// The `DictionaryValues` enum encapsulates the actual dictionary entries that dictionary
/// codes map to. It provides a unified interface for decoding values regardless of whether
/// they are stored in fixed-size or variable-size format.
///
/// # Value Storage Formats
///
/// ## Fixed-Size Values (`FixedSize`)
///
/// Values are stored as a contiguous byte buffer where each value occupies exactly
/// `value_size` bytes. For a value with dictionary ID `i`, its bytes are located at
/// `buffer[i * value_size..(i + 1) * value_size]` in little-endian format.
///
/// ## Variable-Size Values (`VarSize`)
///
/// Used for types with variable byte lengths:
/// - `String` - UTF-8 encoded text
/// - `Binary` - Arbitrary byte sequences
///
/// Values are stored as a `BytesList` where each entry can have different lengths.
/// The ordinal position of each value in the list corresponds to its dictionary ID.
pub enum DictionaryValues {
    /// Fixed-size values storage for primitive and fixed-length types.
    ///
    /// Contains:
    /// - `DictionaryFixedSizeValuesSection`: Protobuf section with packed value buffer
    /// - `usize`: Size in bytes of each individual value
    ///
    /// The value size must be consistent with the declared type and is validated
    /// during dictionary creation. All values in the buffer have identical byte length.
    FixedSize(DictionaryFixedSizeValuesSection, usize),

    /// Variable-size values storage for strings and binary data.
    ///
    /// Contains:
    /// - `DictionaryVarSizeValuesSection`: Protobuf section with `BytesList` of values
    VarSize(DictionaryVarSizeValuesSection),
}

impl DictionaryValues {
    /// Decodes dictionary codes into values and optional offsets.
    ///
    /// This method is the primary interface for converting dictionary codes back into
    /// their original values. It handles both fixed-size and variable-size data,
    /// returning appropriate offset information for variable-size types.
    ///
    /// # Arguments
    ///
    /// * `codes` - Array of dictionary codes to decode. Each code must be a valid
    ///   index into the dictionary (< dictionary size).
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - `Values`: Reconstructed value data as a contiguous byte buffer
    /// - `Option<Offsets>`:
    ///   - `None` for fixed-size values (no offsets needed)
    ///   - `Some(offsets)` for variable-size values indicating value boundaries
    ///     within the returned `Values` buffer
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Any dictionary code is out of bounds (>= dictionary size)
    /// - The underlying dictionary section is corrupted
    pub fn decode(&self, codes: &[u32]) -> Result<(Values, Option<Offsets>)> {
        match self {
            DictionaryValues::FixedSize(section, value_size) => {
                let values = Self::decode_fixed(section, *value_size, codes)?;
                Ok((values, None))
            }
            DictionaryValues::VarSize(section) => {
                let (values, offsets) = Self::decode_variable(section, codes)?;
                Ok((values, Some(offsets)))
            }
        }
    }

    /// Verifies the integrity and consistency of the dictionary values section.
    ///
    /// This method performs validation checks specific to the dictionary format
    /// to ensure data integrity and catch potential corruption early.
    ///
    /// # Arguments
    ///
    /// * `null_id` - Optional dictionary ID reserved for null values
    ///
    /// # Planned Validations
    ///
    /// For **Fixed-size values**:
    /// - Value size is non-zero
    /// - Buffer length is exactly divisible by value size
    /// - All dictionary IDs reference valid buffer positions
    ///
    /// For **Variable-size values**:
    /// - All offsets are consecutive and within the data buffer bounds
    /// - Null value (if present) has zero length
    ///
    /// # Returns
    ///
    /// `Ok(())` if validation passes, error otherwise.
    ///
    /// # Note
    ///
    /// Currently returns `Ok(())` as a placeholder. Full validation logic
    /// is TBD and will be implemented in the future.
    pub fn verify(&self, _null_id: Option<u32>) -> Result<()> {
        Ok(())
    }

    /// Decodes fixed-size dictionary values into a contiguous value buffer.
    ///
    /// This method handles the decoding of primitive types and other fixed-size data
    /// by directly copying value bytes from the dictionary buffer at calculated offsets.
    ///
    /// # Arguments
    ///
    /// * `section` - Fixed-size values section containing the packed value buffer
    /// * `value_size` - Size in bytes of each individual value
    /// * `codes` - Array of dictionary codes to decode
    ///
    /// # Returns
    ///
    /// A `Values` buffer containing the reconstructed values in sequence.
    /// Values are stored contiguously with each value occupying exactly `value_size` bytes.
    fn decode_fixed(
        section: &DictionaryFixedSizeValuesSection,
        value_size: usize,
        codes: &[u32],
    ) -> Result<Values> {
        let mut values =
            Values::with_byte_capacity(codes.len().checked_mul(value_size).expect("mul"));
        for &code in codes {
            let code = code as usize;
            let start = code * value_size;
            let end = start + value_size;
            verify_data!(code, end <= section.values.len());
            values.extend_from_slice(&section.values[start..end]);
        }
        Ok(values)
    }

    /// Decodes variable-size dictionary values into values buffer and offset array.
    ///
    /// This method handles the decoding of strings and binary data where each dictionary
    /// entry can have different lengths. It reconstructs both the value data and the
    /// offset information needed to delineate individual values.
    ///
    /// # Arguments
    ///
    /// * `section` - Variable-size values section containing the `BytesList`
    /// * `codes` - Array of dictionary codes to decode
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - `Values`: Contiguous buffer with all reconstructed values concatenated
    /// - `Offsets`: Array indicating the start/end positions of each value in the buffer
    ///
    /// # Algorithm
    ///
    /// 1. **Offset calculation**: For each code, determine the length of the corresponding
    ///    value from the `BytesList` and build cumulative offsets
    /// 2. **Value reconstruction**: Concatenate all referenced values into a single buffer
    ///    maintaining the order specified by the input codes
    fn decode_variable(
        section: &DictionaryVarSizeValuesSection,
        codes: &[u32],
    ) -> Result<(Values, Offsets)> {
        let bytes_list = section.values.as_ref().expect("values");

        let mut offsets = Offsets::with_capacity(codes.len());
        for &code in codes {
            verify_data!(code, (code as usize) < bytes_list.len());
            offsets.push_length(bytes_list.length_at(code as usize));
        }

        let mut values = Values::with_byte_capacity(offsets.last() as usize);
        for &code in codes {
            values.extend_from_byte_slice(bytes_list.value_at(code as usize));
        }
        Ok((values, offsets))
    }
}

/// Dictionary buffer header containing metadata and section access.
///
/// The `Header` struct provides access to dictionary metadata and enables efficient
/// reading of dictionary sections. It wraps the parsed protobuf header with enhanced
/// I/O capabilities through precached buffering and section offset tracking.
///
/// # Dictionary Buffer Structure
///
/// A dictionary buffer follows this layout:
/// ```text
/// ┌─────────────────────┬─────────────────┬─────────────────────┐
/// │ Header Section      │ Values Section  │ Sorted IDs Section  │
/// │ (with envelope)     │ (with envelope) │ (with envelope)     │
/// └─────────────────────┴─────────────────┴─────────────────────┘
/// ```
///
/// Each section is wrapped with a message envelope containing:
/// - 4-byte length prefix
/// - Protobuf-encoded section data
/// - 4-byte checksum suffix
///
/// # Performance Optimizations
///
/// - **Precached Reading**: The header uses a `PrecachedReadAt` wrapper that caches
///   a small prefix of the dictionary buffer (typically 4KB) containing the header
///   and potentially the beginning of subsequent sections.
/// - **Lazy Section Loading**: Individual sections (values, sorted IDs) are loaded
///   only when first accessed.
/// - **Offset Tracking**: Maintains the exact offset where data sections begin,
///   accounting for the header envelope size.
struct Header {
    /// Parsed protobuf header containing dictionary metadata.
    inner: ValueDictionaryHeader,

    /// Dictionary buffer reader with precached sections buffer for efficient access.
    ///
    /// This reader wraps the original dictionary buffer with a small cached prefix
    /// (typically 4KB) that likely contains the header and potentially the beginning
    /// of the values section. This reduces I/O operations for small dictionaries
    /// and frequently accessed dictionary metadata.
    reader: PrecachedReadAt<SlicedFile<Arc<dyn ReadAt>>>,

    /// Byte offset to the first data section in the dictionary buffer.
    ///
    /// All section ranges in the header are relative to this offset.
    section_offset: u64,
}

impl Header {
    /// Loads and parses a dictionary header from a reader.
    fn load(reader: SlicedFile<Arc<dyn ReadAt>>) -> Result<Header> {
        let prefix_size = std::cmp::min(
            reader.slice_size(),
            reader.storage_profile().clamp_io_size(4 * 1024) as u64,
        );
        let reader = PrecachedReadAt::from_prefix(reader, prefix_size)?;

        let prefix_buf = reader.read_at(0..prefix_size)?;
        let header_buf = amudai_format::checksum::validate_message(&prefix_buf)?;
        let header = ValueDictionaryHeader::decode(header_buf).map_err(|e| {
            Error::invalid_format(format!("Failed to decode dictionary header: {e}"))
        })?;
        // Valid dictionary must have at least the values section
        verify_data!(header, header.values_section_range.is_some());

        let section_offset = (header_buf.len()
            + amudai_format::defs::MESSAGE_LEN_SIZE
            + amudai_format::defs::CHECKSUM_SIZE) as u64;

        Ok(Header {
            inner: header,
            reader,
            section_offset,
        })
    }

    /// Loads and decodes the dictionary values section.
    fn load_values(&self) -> Result<DictionaryValues> {
        let values_buf = self.read_values_buffer()?;
        let values_buf = amudai_format::checksum::validate_message(&values_buf)?;

        if let Some(value_size) = self.inner.fixed_value_size {
            verify_data!(value_size, value_size != 0);
            let values = DictionaryFixedSizeValuesSection::decode(values_buf).map_err(|e| {
                Error::invalid_format(format!("Failed to decode fixed-size values section: {e}"))
            })?;
            verify_data!(values, values.values.len() % value_size as usize == 0);
            Ok(DictionaryValues::FixedSize(values, value_size as usize))
        } else {
            let values = DictionaryVarSizeValuesSection::decode(values_buf).map_err(|e| {
                Error::invalid_format(format!(
                    "Failed to decode variable-size values section: {e}"
                ))
            })?;
            verify_data!(values, values.values.is_some());
            Ok(DictionaryValues::VarSize(values))
        }
    }

    /// Loads and decodes the optional sorted IDs section.
    fn load_sorted_ids(&self) -> Result<Option<DictionarySortedIdsSection>> {
        if let Some(ids_buf) = self.read_sorted_ids_buffer()? {
            let ids_buf = amudai_format::checksum::validate_message(&ids_buf)?;
            let sorted_ids =
                amudai_format::defs::shard::DictionarySortedIdsSection::decode(ids_buf).map_err(
                    |e| Error::invalid_format(format!("Failed to decode sorted IDs section: {e}")),
                )?;
            Ok(Some(sorted_ids))
        } else {
            Ok(None)
        }
    }

    /// Reads the raw values section buffer from the dictionary.
    ///
    /// This method extracts the values section data using the range specified
    /// in the header, adjusting for the section offset. The returned buffer
    /// includes the complete message envelope (length + data + checksum).
    fn read_values_buffer(&self) -> Result<Bytes> {
        let range = self.inner.values_section_range.expect("values range");
        let bytes = self
            .reader
            .read_at(range.start + self.section_offset..range.end + self.section_offset)?;
        Ok(bytes)
    }

    /// Reads the raw sorted IDs section buffer if it exists.
    ///
    /// This method checks if the dictionary includes a sorted IDs section and,
    /// if present, extracts its data using the range specified in the header.
    /// The returned buffer includes the complete message envelope.
    fn read_sorted_ids_buffer(&self) -> Result<Option<Bytes>> {
        let Some(range) = self.inner.sorted_ids_section_range else {
            return Ok(None);
        };
        let bytes = self
            .reader
            .read_at(range.start + self.section_offset..range.end + self.section_offset)?;
        Ok(Some(bytes))
    }
}

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
    dictionary: Arc<DictionaryDecoder>,
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
        dictionary: Arc<DictionaryDecoder>,
        basic_type: BasicTypeDescriptor,
    ) -> DictionaryFieldDecoder {
        DictionaryFieldDecoder {
            codes_buffer,
            dictionary,
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

        // First buffer is the codes buffer (u32 values)
        let codes_encoded_buffer = field.get_encoded_buffer(BufferKind::Data)?;
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
        let dictionary_encoded_buffer = field.get_encoded_buffer(BufferKind::ValueDictionary)?;
        let dictionary_ref = dictionary_encoded_buffer
            .buffer
            .as_ref()
            .ok_or_else(|| Error::invalid_format("Missing dictionary buffer reference"))?;
        let dictionary_reader = field.open_data_ref(dictionary_ref)?;

        let dictionary = Arc::new(DictionaryDecoder::from_encoded_buffer(
            dictionary_reader.into_inner(),
            dictionary_encoded_buffer,
        )?);

        Ok(DictionaryFieldDecoder::new(
            codes_buffer,
            dictionary,
            basic_type,
        ))
    }

    /// Creates a `DictionaryFieldDecoder` from an encoded field.
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
    ) -> Result<DictionaryFieldDecoder> {
        let codes_buffer = field.get_encoded_buffer(BufferKind::Data)?;
        let dictionary_buffer = field.get_encoded_buffer(BufferKind::ValueDictionary)?;

        let codes_buffer = PrimitiveBufferDecoder::from_prepared_buffer(
            codes_buffer,
            BasicTypeDescriptor {
                basic_type: BasicType::Int32,
                fixed_size: 0,
                signed: false,
                extended_type: Default::default(),
            },
        )?;

        let dictionary = Arc::new(DictionaryDecoder::from_prepared_buffer(dictionary_buffer)?);

        Ok(DictionaryFieldDecoder::new(
            codes_buffer,
            dictionary,
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
            dictionary: self.dictionary.clone(),
            basic_type: self.basic_type,
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
    dictionary: Arc<DictionaryDecoder>,
    /// Basic type descriptor of the encoded field
    basic_type: BasicTypeDescriptor,
}

impl DictionaryFieldReader {
    /// Decodes code sequence into plain `ValueSequence` by mapping codes to dictionary entries.
    ///
    /// # Arguments
    /// * `codes_sequence` - Value sequence containing the dictionary codes
    ///
    /// # Returns
    /// Value sequence with reconstructed original values
    fn decode_sequence(&mut self, codes_sequence: ValueSequence) -> Result<ValueSequence> {
        let codes = codes_sequence.values.as_slice::<u32>();
        self.dictionary.decode_sequence(codes, self.basic_type)
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
        self.decode_sequence(codes_sequence)
    }
}
