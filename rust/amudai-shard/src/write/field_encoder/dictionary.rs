use ahash::AHashMap;
use amudai_io::{AlignWrite, TemporaryFileStore};
use arrow_array::Array;
use prost::Message;
use std::{collections::HashMap, hash::Hash, sync::Arc};

use amudai_arrow_processing::for_each::for_each_as_binary;
use amudai_blockstream::{
    read::block_stream::BlockReaderPrefetch,
    write::{
        PreparedEncodedBuffer, primitive_buffer::PrimitiveBufferEncoder,
        staging_buffer::PrimitiveStagingBuffer,
    },
};
use amudai_common::Result;
use amudai_encodings::block_encoder::{
    BlockChecksum, BlockEncodingParameters, BlockEncodingPolicy, BlockEncodingProfile,
    PresenceEncoding,
};
use amudai_format::{
    defs::{
        common::{BytesList, DataRef},
        schema_ext::BasicTypeDescriptor,
        shard::{
            BufferKind, DictionaryFixedSizeValuesSection, DictionarySortedIdsSection,
            DictionaryVarSizeValuesSection, EncodedBuffer, ValueDictionaryHeader,
        },
    },
    schema::BasicType,
};

use super::{EncodedField, FieldEncoderOps};
use crate::write::field_encoder::{
    DictionaryEncoding, EncodedFieldStatistics, FieldEncoder, FieldEncoderParams,
    bytes::BytesStatsCollector,
};
use amudai_arrow_builders::ArrayBuilderExt;

/// Dictionary encoder that compresses data by mapping duplicate values to compact integer codes.
///
/// This implementation uses a progressive dictionary building approach optimized for large datasets:
///
/// ## Architecture
///
/// The dictionary is split into two parts:
/// - **Frozen dictionary**: Contains stable value-to-ID mappings that won't change
/// - **Active dictionary**: Contains new mappings added since the last flush
///
/// ## Processing Flow
///
/// 1. **Value Processing**: Incoming values are mapped through both dictionaries:
///    - First check frozen dictionary (most common case in steady state)
///    - Then check active dictionary
///    - Create new entry in active dictionary if value is unseen
///    - Append dictionary ID to staging buffer
///
/// 2. **Periodic Flushing**: When staging buffer reaches ~64k values:
///    - Sort active dictionary entries by frequency (most frequent first)
///    - Reassign IDs based on frequency to optimize compression
///    - Remap affected IDs in staging buffer
///    - Encode staging buffer directly using PrimitiveBufferEncoder
///    - Merge active entries into frozen dictionary
///
/// 3. **Final Encoding**: During `finish()`:
///    - Perform final flush to ensure all entries are frozen
///    - Finalize the codes encoder to get the encoded codes buffer
///    - Generate dictionary values buffer
///
/// ## Output Format
///
/// Produces two encoded buffers:
///
///  - **Codes buffer**: Encoded sequence of dictionary IDs that replace original values.
///  - **Dictionary buffer**: The dictionary buffer encoded as `ValueDictionaryHeader` protobuf message.
///
pub struct DictionaryFieldEncoder<T>
where
    T: Clone + Eq + Hash + Default,
{
    /// Field encoder parameters
    params: FieldEncoderParams,
    /// The accumulated size of all unique strings seen so far.
    dict_size: usize,
    /// The accumulated size of all strings seen so far.
    plain_size: usize,
    /// Counter of the populated logical positions (i.e. the number of
    /// pushed buffers and nulls)
    positions: usize,
    /// Frozen (stable) dictionary entries whose IDs will not change
    frozen_entries: AHashMap<T, DictionaryEntry>,
    /// Active dictionary entries added since the last flush
    active_entries: AHashMap<T, DictionaryEntry>,
    /// Entry for a null value (if it was observed in the stream)
    null_entry: Option<DictionaryEntry>,
    /// Buffer that holds intermediate encoding for the recent batch
    /// of pushed items until it is flushed
    staging_buf: Vec<u32>,
    /// Next available (unassigned) dictionary ID
    next_id: u32,
    /// The first dictionary ID that is not yet frozen
    unfrozen_id: u32,
    /// Scratch buffer to store new (unfrozen) entries with their counts
    /// in preparation for remapping during flush
    freq_buf: Vec<(u32, usize)>,
    /// Scratch buffer to store unfrozen-to-frozen entry ID mapping during flush
    remap_buf: Vec<u32>,
    /// Encoder for values codes (dictionary IDs assigned to encoded values).
    codes_encoder: Option<PrimitiveBufferEncoder>,
}

impl DictionaryFieldEncoder<Vec<u8>> {
    /// Creates a new dictionary encoder for the specified field parameters.
    ///
    /// # Arguments
    /// * `params` - Field encoding parameters including type information
    ///
    /// # Returns
    /// A configured `DictionaryFieldEncoder` instance
    fn new(params: &FieldEncoderParams) -> Result<Self> {
        assert!(matches!(
            params.basic_type.basic_type,
            BasicType::Binary | BasicType::FixedSizeBinary | BasicType::String | BasicType::Guid
        ));
        if params.basic_type.basic_type == BasicType::Guid {
            assert_eq!(params.basic_type.fixed_size, 16);
        }

        let codes_encoder = PrimitiveBufferEncoder::new(
            BlockEncodingPolicy {
                parameters: BlockEncodingParameters {
                    presence: PresenceEncoding::Disabled,
                    checksum: BlockChecksum::Enabled,
                },
                profile: BlockEncodingProfile::MinimalCompression,
                size_constraints: None,
            },
            BasicTypeDescriptor {
                basic_type: BasicType::Int32,
                fixed_size: 0,
                signed: false,
                extended_type: Default::default(),
            },
            params.temp_store.clone(),
        )?;

        Ok(DictionaryFieldEncoder {
            params: params.clone(),
            dict_size: 0,
            plain_size: 0,
            positions: 0,
            frozen_entries: AHashMap::default(),
            active_entries: AHashMap::default(),
            null_entry: None,
            staging_buf: Vec::new(),
            next_id: 0,
            unfrozen_id: 0,
            freq_buf: Vec::new(),
            remap_buf: Vec::new(),
            codes_encoder: Some(codes_encoder),
        })
    }

    /// Adds a value to the dictionary or increments its occurrence count.
    ///
    /// # Arguments
    /// * `value` - Byte slice representing the value
    ///
    /// # Returns
    /// Dictionary code assigned to the value
    #[inline]
    fn add_value(&mut self, value: &[u8]) -> u32 {
        self.positions += 1;
        self.plain_size += value.len();

        // Try the frozen dictionary first. In the steady state, this would be
        // the most common path.
        if let Some(entry) = self.frozen_entries.get_mut(value) {
            entry.count += 1;
            return entry.id;
        }

        // Try the active dictionary
        if let Some(entry) = self.active_entries.get_mut(value) {
            entry.count += 1;
            return entry.id;
        }

        // A new item, not yet in the dictionary.
        let id = self.next_id;
        assert!(id < u32::MAX);
        self.next_id += 1;
        let entry = DictionaryEntry { id, count: 1 };
        self.active_entries.insert(value.to_vec(), entry);

        self.dict_size += value.len();

        id
    }

    /// Adds null values to the encoder.
    ///
    /// Creates a new null entry or increments the occurrence count of an existing one.
    ///
    /// # Arguments
    /// * `count` - Number of null values to add
    ///
    /// # Returns
    /// Dictionary code assigned to the null entry
    #[inline]
    fn add_nulls(&mut self, count: usize) -> u32 {
        self.positions += count;

        if let Some(entry) = self.null_entry.as_mut() {
            // If we already have the null entry, just increment its count
            entry.count += count;
            entry.id
        } else {
            // Create a new null entry with the current id.
            let null_id = self.next_id;
            self.null_entry = Some(DictionaryEntry { id: null_id, count });
            self.next_id += 1;
            null_id
        }
    }

    /// Flushes the staging buffer to the codes encoder.
    fn flush(&mut self) -> Result<()> {
        if self.staging_buf.is_empty() {
            return Ok(());
        }

        self.prepare_flush();
        assert_eq!(self.unfrozen_id, self.next_id);

        let mut array = arrow_array::builder::UInt32Builder::with_capacity(self.staging_buf.len());
        array.append_slice(&self.staging_buf);
        let mut staging = PrimitiveStagingBuffer::new(arrow_schema::DataType::UInt32);
        staging.append(Arc::new(array.finish()));
        self.staging_buf.clear();

        let codes_encoder = self
            .codes_encoder
            .as_mut()
            .expect("codes encoder must be present");

        // Get preferred block size by analyzing a sample of staging values.
        let sample_size = codes_encoder
            .sample_size()
            .map(|constraints| constraints.value_count.start)
            .unwrap_or(1024);
        let sample = staging.sample(sample_size)?;
        let block_size = codes_encoder
            .analyze_sample(&sample)?
            .value_count
            .end
            .max(1);

        // Encode the staging buffer using the preferred block size.
        while !staging.is_empty() {
            let values = staging.dequeue(block_size)?;
            codes_encoder.encode_block(&values)?;
        }

        Ok(())
    }

    fn should_flush(&self) -> bool {
        self.staging_buf.len() >= PrimitiveStagingBuffer::DEFAULT_LEN_THRESHOLD
    }

    /// Prepares for the flush of the staging buffer: re-assigns the new entry
    /// id's based on their frequency (lower id's to the most frequent entries),
    /// adjusts the relevant id's both in the staging buffer and in the active_entries
    /// hashmap and propagates them to the "frozen" `frozen_entries` hashmap.
    fn prepare_flush(&mut self) {
        if self.unfrozen_id == self.next_id {
            assert!(self.active_entries.is_empty());
            return;
        }

        // Collect all of the new (id, count) tuples into the freq_buf and sort
        // the new entries by frequency.
        self.collect_new_entries();

        // Prepare the remapping buffer, that takes us from the currently assigned
        // entry ID to the final, stable one.
        // The buffer indexes are logically shifted by unfrozen_id.
        self.prepare_remapping_buf();

        // Remap the new dictionary IDs in the staging buffer
        self.remap_staging_buf();

        // Remap and transfer the new entries to the stable dictionary
        self.remap_and_transfer_entries();

        assert!(self.active_entries.is_empty());
        self.unfrozen_id = self.next_id;
        self.freq_buf.clear();
        self.remap_buf.clear();
    }

    // Collects all of the new (id, count) tuples into the freq_buf and sorts
    // the new entries by frequency.
    fn collect_new_entries(&mut self) {
        for entry in self.active_entries.values() {
            assert!(entry.id >= self.unfrozen_id);
            self.freq_buf.push((entry.id, entry.count));
        }

        if let Some(ref entry) = self.null_entry {
            if entry.id >= self.unfrozen_id {
                self.freq_buf.push((entry.id, entry.count));
            }
        }

        // Sort by frequency (descending)
        self.freq_buf.sort_unstable_by_key(|e| usize::MAX - e.1);
    }

    // Prepares the remapping buffer, that takes us from the currently assigned
    // entry id to the stable one. The buffer indexes are logically shifted by
    // unfrozen_id.
    fn prepare_remapping_buf(&mut self) {
        self.remap_buf.clear();
        self.remap_buf.resize(self.freq_buf.len(), u32::MAX);
        let start_id = self.unfrozen_id;
        for (i, entry) in self.freq_buf.drain(..).enumerate() {
            assert!(
                entry.0 >= start_id,
                "Frozen ID was unexpected among the new entries"
            );
            self.remap_buf[(entry.0 - start_id) as usize] = i as u32 + start_id;
        }
        assert!(
            self.remap_buf.iter().all(|&i| i != u32::MAX),
            "all remap_buf slots are initialized"
        );
    }

    // Remaps the new (unfrozen) id's in the staging buffer
    fn remap_staging_buf(&mut self) {
        let start_id = self.unfrozen_id;
        for id in self.staging_buf.iter_mut() {
            if *id >= start_id {
                *id = self.remap_buf[(*id - start_id) as usize];
            }
        }
    }

    // Remaps and transfer the new entries to the stable dictionary
    fn remap_and_transfer_entries(&mut self) {
        let start_id = self.unfrozen_id;

        for (item, mut entry) in self.active_entries.drain() {
            assert!(entry.id >= start_id);
            entry.id = self.remap_buf[(entry.id - start_id) as usize];
            let res = self.frozen_entries.insert(item, entry);
            assert!(res.is_none());
        }

        if let Some(ref mut entry) = self.null_entry {
            if entry.id >= self.unfrozen_id {
                entry.id = self.remap_buf[(entry.id - start_id) as usize];
            }
        }
    }

    /// Extracts dictionary entries in ID order for final encoding.
    ///
    /// Returns entries ordered by their assigned IDs (which have already been
    /// optimized during periodic flushes based on frequency).
    /// Returned entries include the null entry if it was observed.
    fn prepare_and_take_dictionary(&mut self) -> PreparedDictionary {
        // First ensure all entries are frozen
        self.prepare_flush();

        let mut entries = self.frozen_entries.drain().collect::<Vec<_>>();

        // Make existing null entry part of the dictionary.
        if let Some(null_entry) = self.null_entry.as_ref() {
            entries.push((
                vec![0u8; self.params.basic_type.fixed_size as usize],
                null_entry.clone(),
            ));
        }
        assert_eq!(self.next_id, entries.len() as u32);

        // Sort by ID to maintain the order that codes expect
        entries.sort_unstable_by_key(|(_, entry)| entry.id);

        PreparedDictionary {
            basic_type: self.params.basic_type,
            entries,
            null_entry: self.null_entry.clone(),
        }
    }

    /// Evaluates dictionary encoding efficiency based on compression ratio.
    ///
    /// # Arguments
    ///
    /// * `force` - If true, forces the evaluation even if the number of processed values
    ///   is less than the minimum threshold.
    fn is_efficient(&self, force: bool) -> bool {
        // Minimum values to observe before evaluating efficiency.
        const EFFICIENCY_EVAL_MIN_VALUES: usize = 10 * 1024;

        // If the encoded size for the first N values (N < EFFICIENCY_EVAL_MIN_VALUES) is more
        // greater than this value, then we consider the dictionary encoding inefficient.
        const TOO_FAST_GROWTH_THRESHOLD: usize = 10 * 1024 * 1024; // 10MB

        /// The estimated efficient compressio ratio of the dictionary encoding.
        const ESTIMATED_COMPRESSION_RATIO: usize = 10;

        // Estimation of the dictionary encoded size, which is the sum of:
        // 1. The dictionary values (stored in values section).
        // 2. An estimation of the value ID's encoding (codes buffer).
        // 3. Sorted IDs section (4 bytes per unique value for lexicographic ordering).
        // 4. Offsets for variable-size values (4 bytes per unique value).
        let unique_count = self.active_entries.len() + self.frozen_entries.len();

        // Assume BitPacking is used for encodign the codes buffer (dictionary IDs).
        let bits_per_id = 64 - unique_count.leading_zeros() as usize;
        let offsets_size = if self.params.basic_type.fixed_size == 0 {
            unique_count * 8 // Offsets for variable-size values (8 bytes per unique value)
        } else {
            0 // No offsets for fixed-size values
        };
        let dict_encoded_size = self.dict_size // Dictionary values
            + offsets_size // Offsets for variable-size dictionary values
            + bits_per_id * self.positions / 8  // Bit-packed codes buffer
            + unique_count * 4; // Sorted IDs section (4 bytes per unique value)

        if !force && self.positions < EFFICIENCY_EVAL_MIN_VALUES {
            // If the dictionary size grows too fast, we consider it inefficient.
            return dict_encoded_size < TOO_FAST_GROWTH_THRESHOLD;
        }

        let plain_encoding_size =
            self.plain_size / ESTIMATED_COMPRESSION_RATIO + 8 * self.positions;
        dict_encoded_size < plain_encoding_size
    }

    /// Re-encodes entries processed by this encoder using the specified encoder.
    ///
    /// This method reads the encoded codes from the codes encoder, builds an inverted dictionary
    /// to map codes back to their original values, and re-encodes the entries into the provided encoder.
    ///
    /// # Arguments
    /// * `encoder` - The encoder to which the re-encoded entries will be pushed
    ///
    fn reencode_entries(mut self, encoder: &mut dyn FieldEncoderOps) -> Result<()> {
        // Use consume() to get all encoded codes
        let codes_encoder = self
            .codes_encoder
            .take()
            .expect("codes encoder must be present");
        let codes_decoder = codes_encoder.consume()?;

        // Create a reader to read the codes back.
        let total_count = codes_decoder.block_stream().block_map().value_count()?;
        let mut reader = codes_decoder.create_reader(
            std::iter::once(0..total_count),
            BlockReaderPrefetch::Disabled,
        )?;

        // Build inverted dictionary for looking up values by code
        let mut inverted_dict = HashMap::new();
        for (value, entry) in self.frozen_entries.iter() {
            inverted_dict.insert(entry.id, value.clone());
        }
        for (value, entry) in self.active_entries.iter() {
            inverted_dict.insert(entry.id, value.clone());
        }

        // Process codes in batches to avoid excessive memory usage
        const BATCH_SIZE: u64 = 1024; // Process 1k values at a time

        let data_type = match self.params.basic_type.basic_type {
            BasicType::String => arrow_schema::DataType::Utf8,
            BasicType::FixedSizeBinary | BasicType::Guid => {
                arrow_schema::DataType::FixedSizeBinary(self.params.basic_type.fixed_size as i32)
            }
            BasicType::Binary => arrow_schema::DataType::Binary,
            _ => unreachable!(
                "Unsupported basic type for dictionary encoding: {:?}",
                self.params.basic_type.basic_type
            ),
        };

        let mut offset = 0u64;
        while offset < total_count {
            let batch_end = std::cmp::min(offset + BATCH_SIZE, total_count);
            let codes_sequence = reader.read(offset..batch_end)?;
            let codes = codes_sequence.values.as_slice::<u32>();
            let mut array_builder = amudai_arrow_builders::create_builder(&data_type);
            for &code in codes {
                // If the code is not found, it means it's a null value.
                let value = inverted_dict.get(&code).map(|v| v.as_slice());
                array_builder.push_raw_value(value);
            }
            // Append the built array to the encoder
            encoder.push_array(array_builder.build())?;
            offset = batch_end;
        }

        // Re-encode the values from the staging buffer as well.
        let mut array_builder = amudai_arrow_builders::create_builder(&data_type);
        for &code in self.staging_buf.iter() {
            // If the code is not found, it means it's a null value.
            let value = inverted_dict.get(&code).map(|v| v.as_slice());
            array_builder.push_raw_value(value);
        }
        // Append the built array to the encoder
        encoder.push_array(array_builder.build())?;
        Ok(())
    }
}

impl FieldEncoderOps for DictionaryFieldEncoder<Vec<u8>> {
    fn push_array(&mut self, array: std::sync::Arc<dyn Array>) -> Result<()> {
        if array.is_empty() {
            return Ok(());
        }

        for_each_as_binary(array.as_ref(), |_, value| -> Result<()> {
            if let Some(value) = value {
                let id = self.add_value(value);
                self.staging_buf.push(id);
            } else {
                let id = self.add_nulls(1);
                self.staging_buf.push(id);
            }
            Ok(())
        })?;

        // Check if we should flush
        if self.should_flush() {
            self.flush()?;
        }

        Ok(())
    }

    fn push_nulls(&mut self, count: usize) -> Result<()> {
        if count == 0 {
            return Ok(());
        }

        let id = self.add_nulls(count);
        for _ in 0..count {
            self.staging_buf.push(id);
        }

        // Check if we should flush
        if self.should_flush() {
            self.flush()?;
        }

        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<EncodedField> {
        if !self.staging_buf.is_empty() {
            self.flush()?;
        }

        let mut buffers = Vec::new();

        // Create the final values codes buffer.
        let codes_buffer = self
            .codes_encoder
            .take()
            .expect("codes encoder must be present")
            .finish()?;
        buffers.push(codes_buffer);

        // Collect statistics and create the final dictionary buffer.
        let dictionary = self.prepare_and_take_dictionary();

        let statistics = collect_dictionary_statistics(
            self.params.basic_type.basic_type,
            self.params.encoding_profile,
            &dictionary.entries,
            &dictionary.null_entry,
        )?;

        let dictionary_size = dictionary.entries.len() as u64;

        buffers.push(dictionary.encode_buffer(self.params.temp_store.as_ref())?);

        Ok(EncodedField {
            buffers,
            statistics,
            dictionary_size: Some(dictionary_size),
        })
    }
}

/// Collects binary or string statistics based on the dictionary entries,
/// without the need to process each value individually.
fn collect_dictionary_statistics(
    basic_type: BasicType,
    encoding_profile: BlockEncodingProfile,
    entries: &[(Vec<u8>, DictionaryEntry)],
    null_entry: &Option<DictionaryEntry>,
) -> Result<Option<EncodedFieldStatistics>> {
    let mut stats_collector = BytesStatsCollector::new(basic_type, encoding_profile);
    if let Some(null_entry) = null_entry {
        stats_collector.process_nulls(null_entry.count as usize)?;
    }
    for (value, entry) in entries {
        if let Some(null_entry) = null_entry {
            if null_entry.id == entry.id {
                // Skip the null entry.
                continue;
            }
        }
        stats_collector.process_value(value, entry.count as usize)?;
    }
    stats_collector.finish()
}

/// Adaptive encoder that dynamically switches between dictionary and native encoding.
///
/// Begins with dictionary encoding and automatically transitions to native type encoding
/// when dictionary compression becomes inefficient based on data characteristics.
pub struct AdaptiveDictionaryFieldEncoder {
    /// Field encoding parameters
    params: FieldEncoderParams,
    /// Active dictionary encoder (None after switching to native encoding)
    dictionary_encoder: Option<DictionaryFieldEncoder<Vec<u8>>>,
    /// Native encoder used after dictionary encoding becomes inefficient
    native_encoder: Option<Box<dyn FieldEncoderOps>>,
}

impl AdaptiveDictionaryFieldEncoder {
    /// Switches to native encoding when dictionary encoding becomes inefficient.
    ///
    /// Evaluates efficiency and transitions by transferring accumulated data
    /// from dictionary encoder to native encoder.
    ///
    /// # Arguments
    /// * `array` - Array for populating newly created native encoder
    /// * `is_sealing` - Indicates if this is the final encoding operation
    fn switch_to_native_encoder_if_needed(&mut self, is_sealing: bool) -> Result<()> {
        if self.native_encoder.is_some() {
            // If we already have a bytes encoder, do nothing
            return Ok(());
        }

        if self
            .dictionary_encoder
            .as_ref()
            .expect("Dictionary encoder must be present")
            .is_efficient(is_sealing)
        {
            // Either not enough data to determine efficiency or the dictionary encoding
            // is still efficient. In this case, continue using the dictionary encoder.
            return Ok(());
        }

        let dictionary_encoder = self
            .dictionary_encoder
            .take()
            .expect("Dictionary encoder must be present");

        // Push collected data from the dictionary encoder to the native encoder.
        let mut params = self.params.clone();
        params.dictionary_encoding = DictionaryEncoding::Disabled;
        let mut native_encoder = FieldEncoder::create_encoder(&params)?;
        dictionary_encoder.reencode_entries(native_encoder.as_mut())?;

        self.native_encoder = Some(native_encoder);

        Ok(())
    }
}

impl AdaptiveDictionaryFieldEncoder {
    /// Creates a new adaptive dictionary encoder.
    ///
    /// # Arguments
    /// * `params` - Field encoding parameters
    ///
    /// # Returns
    /// Boxed field encoder trait object
    pub(crate) fn create(params: &FieldEncoderParams) -> Result<Box<dyn FieldEncoderOps>> {
        let dictionary_encoder = DictionaryFieldEncoder::new(params)?;
        Ok(Box::new(AdaptiveDictionaryFieldEncoder {
            params: params.clone(),
            dictionary_encoder: Some(dictionary_encoder),
            native_encoder: None,
        }))
    }

    /// Returns reference to the currently active encoder.
    ///
    /// Provides unified access to either dictionary or native encoder
    /// based on current encoding strategy.
    fn current_encoder(&mut self) -> &mut dyn FieldEncoderOps {
        if let Some(native_encoder) = self.native_encoder.as_mut() {
            native_encoder.as_mut()
        } else if let Some(dictionary_encoder) = self.dictionary_encoder.as_mut() {
            dictionary_encoder
        } else {
            panic!("Either dictionary encoder or native encoder must be present");
        }
    }
}

impl FieldEncoderOps for AdaptiveDictionaryFieldEncoder {
    fn push_array(&mut self, array: Arc<dyn Array>) -> Result<()> {
        self.current_encoder().push_array(array.clone())?;
        self.switch_to_native_encoder_if_needed(false)?;
        Ok(())
    }

    fn push_nulls(&mut self, count: usize) -> Result<()> {
        self.current_encoder().push_nulls(count)?;
        self.switch_to_native_encoder_if_needed(false)?;
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<EncodedField> {
        self.switch_to_native_encoder_if_needed(true)?;

        if let Some(dictionary_encoder) = self.dictionary_encoder.take() {
            Box::new(dictionary_encoder).finish()
        } else if let Some(native_encoder) = self.native_encoder.take() {
            native_encoder.finish()
        } else {
            panic!("Either dictionary encoder or native encoder must be present");
        }
    }
}

/// Dictionary entry tracking unique value code assignment and occurrence frequency.
///
/// Maps unique values to dictionary codes and maintains occurrence statistics
/// for compression optimization.
#[derive(Debug, Clone)]
pub struct DictionaryEntry {
    /// Dictionary code assigned to this entry
    pub id: u32,
    /// Number of occurrences of this value in the dataset
    pub count: usize,
}

/// Prepared dictionary ready to be encoded as 'ValueDictionary' buffer.
pub struct PreparedDictionary {
    /// Formal value type
    pub basic_type: BasicTypeDescriptor,
    /// Entries, sorted by assigned dictionary ID.
    /// Includes a null value entry if present.
    pub entries: Vec<(Vec<u8>, DictionaryEntry)>,
    /// Null value entry.
    pub null_entry: Option<DictionaryEntry>,
}

impl PreparedDictionary {
    /// Constructs an empty `PreparedDictionary`.
    ///
    /// Useful mostly in test scenarios.
    pub fn new(basic_type: BasicTypeDescriptor) -> PreparedDictionary {
        PreparedDictionary {
            basic_type,
            entries: Vec::new(),
            null_entry: None,
        }
    }

    /// Manually adds an entry to the dictionary.
    ///
    /// Useful mostly in test scenarios.
    pub fn push(&mut self, value: &[u8], count: usize) {
        let id = self.entries.len() as u32;
        self.entries
            .push((value.to_vec(), DictionaryEntry { id, count }));
    }

    /// Manually adds a null entry to the dictionary.
    ///
    /// Useful mostly in test scenarios.
    pub fn push_null(&mut self, count: usize) {
        self.push(&vec![0u8; self.basic_type.fixed_size as usize], count);
        self.null_entry = Some(self.entries.last().unwrap().1.clone());
    }

    /// Creates the dictionary entries buffer containing unique values.
    ///
    /// This method encodes the dictionary entries into a structured buffer
    /// that includes:
    /// - Header section with metadata and ranges (`ValueDictionaryHeader`)
    /// - Values section with unique values (either `DictionaryFixedSizeValuesSection` or `DictionaryVarSizeValuesSection`)
    /// - Sorted IDs section with dictionary IDs in lexicographic order (`DictionarySortedIdsSection`)
    ///
    /// Each written section is accompanied by its length and checksum.
    /// The resulted buffer is aligned to 8 bytes.
    ///
    /// # Arguments
    /// * `entries` - Dictionary entries with unique values and metadata
    ///
    /// # Returns
    /// Prepared encoded buffer containing the structured dictionary
    pub fn encode_buffer(
        &self,
        temp_store: &dyn TemporaryFileStore,
    ) -> Result<PreparedEncodedBuffer> {
        let values_section = self.create_values_section();
        let values_section_range = amudai_format::defs::common::UInt64Range {
            start: 0,
            end: values_section.len() as u64,
        };

        let sorted_ids_section = self.create_sorted_ids_section();
        let sorted_ids_section_range = amudai_format::defs::common::UInt64Range {
            start: values_section_range.end,
            end: values_section_range.end + sorted_ids_section.len() as u64,
        };

        let header_section =
            self.create_header_section(values_section_range, sorted_ids_section_range);

        // Write all the sections into the temporary buffer.
        let mut buffer = temp_store.allocate_stream(Some(
            header_section.len() + values_section.len() + sorted_ids_section.len(),
        ))?;
        buffer.write_all(&header_section)?;
        buffer.write_all(&values_section)?;
        buffer.write_all(&sorted_ids_section)?;

        // Align to 8 bytes
        buffer.align(8)?;

        // Create the PreparedEncodedBuffer
        let data_size = buffer.current_size();
        let data = buffer.into_read_at()?;
        let data_ref = DataRef::new("", 0..data_size);
        let prepared_buffer = PreparedEncodedBuffer {
            data,
            data_size,
            block_map_size: 0,
            descriptor: EncodedBuffer {
                kind: BufferKind::ValueDictionary as i32,
                buffer: Some(data_ref),
                block_map: None,
                block_count: None,
                block_checksums: false,
                embedded_presence: false,
                embedded_offsets: false,
                buffer_id: None,
                packed_group_index: None,
            },
        };

        Ok(prepared_buffer)
    }

    /// Creates the section containing the unique values of the dictionary.
    ///
    /// # Returns
    ///
    /// Encoded `DictionaryFixedSizeValuesSection` or `DictionaryVarSizeValuesSection` protobuf message
    /// accompanied with its length and checksum.
    fn create_values_section(&self) -> Vec<u8> {
        let payload = if self.basic_type.fixed_size > 0 {
            // Fixed-size values section
            let mut values_data = vec![];
            for (value, _) in self.entries.iter() {
                assert_eq!(value.len(), self.basic_type.fixed_size as usize);
                values_data.extend_from_slice(value);
            }
            DictionaryFixedSizeValuesSection {
                values: values_data,
            }
            .encode_to_vec()
        } else {
            // Variable-size values section
            let mut offsets = vec![0u64];
            let mut data = vec![];
            for (value, _) in self.entries.iter() {
                data.extend_from_slice(value);
                offsets.push(data.len() as u64);
            }
            let bytes_list = BytesList { offsets, data };
            DictionaryVarSizeValuesSection {
                values: Some(bytes_list),
            }
            .encode_to_vec()
        };
        amudai_format::checksum::create_message_vec(&payload)
    }

    fn create_sorted_ids_section(&self) -> Vec<u8> {
        assert!(
            self.entries
                .iter()
                .enumerate()
                .all(|(i, (_, entry))| entry.id as usize == i)
        );

        // Filter out null id from the sorted ids section, if there is one.
        // Sorted ids are used for searching specific (non-null) values, so we do not
        // want to accidentally stumble upon a null representation which might be the
        // same as e.g. some valid empty string.
        let null_id = self.null_entry.as_ref().map(|e| e.id).unwrap_or(u32::MAX);
        let mut sorted_ids = (0..self.entries.len())
            .map(|i| i as u32)
            .filter(|&id| id != null_id)
            .collect::<Vec<_>>();

        sorted_ids.sort_unstable_by(|&a_id, &b_id| {
            self.entries[a_id as usize]
                .0
                .cmp(&self.entries[b_id as usize].0)
        });

        let payload = DictionarySortedIdsSection { sorted_ids }.encode_to_vec();
        amudai_format::checksum::create_message_vec(&payload)
    }

    /// Creates the top-level header message.
    ///
    /// # Returns
    ///
    /// Encoded `ValueDictionaryHeader` protobuf message accompanied with its length and checksum.
    fn create_header_section(
        &self,
        values_section_range: amudai_format::defs::common::UInt64Range,
        sorted_ids_section_range: amudai_format::defs::common::UInt64Range,
    ) -> Vec<u8> {
        let fixed_value_size = if self.basic_type.fixed_size > 0 {
            Some(self.basic_type.fixed_size)
        } else {
            None
        };
        let payload = ValueDictionaryHeader {
            value_type: self.basic_type.basic_type as u32,
            value_count: self.entries.len() as u32,
            null_id: self.null_entry.as_ref().map(|e| e.id),
            fixed_value_size,
            values_section_range: Some(values_section_range),
            sorted_ids_section_range: Some(sorted_ids_section_range),
        }
        .encode_to_vec();

        let buffer = amudai_format::checksum::create_message_vec(&payload);
        buffer
    }
}
