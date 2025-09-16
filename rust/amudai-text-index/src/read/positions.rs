//! Position data decoder for retrieving term occurrence information.
//!
//! This module provides functionality for reading position lists from the positions
//! shard, converting the stored integer sequences back into structured position
//! information based on their representation type.

use std::{
    ops::Range,
    sync::{Arc, OnceLock},
};

use amudai_common::{Result, error::Error};
use amudai_objectstore::{ObjectStore, url::ObjectUrl};
use amudai_shard::read::{
    field_decoder::{FieldDecoder, FieldReader},
    shard::{Shard, ShardOptions},
};

use crate::pos_list::{PositionList, PositionsReprType};

/// Decoder for retrieving position lists from the positions shard.
///
/// The `PositionsDecoder` handles the lazy initialization and reading of position
/// data stored as sequential integer arrays in the positions shard. It supports
/// different position list representations (exact positions, exact ranges, and
/// approximate ranges) and efficiently converts the stored data back into
/// structured position information.
///
/// # Position Data Storage
///
/// Position data is stored in a specialized shard containing a single "position"
/// field with u64 values. The interpretation of these values depends on the
/// `PositionsReprType`:
/// - **Positions**: Each value represents an individual character position
/// - **ExactRanges**: Values are stored in pairs (start, end) for precise ranges
/// - **ApproximateRanges**: Values are stored in pairs (start, end) for approximate ranges
///
/// # Lazy Initialization
///
/// The decoder uses lazy initialization patterns to defer expensive operations
/// (shard opening, schema resolution, reader creation) until they are actually
/// needed. This enables efficient construction and resource management.
///
/// # Thread Safety
///
/// The decoder uses `OnceLock` for thread-safe lazy initialization of shared
/// resources. Multiple threads can safely call methods concurrently, with the
/// lazy initialization being handled atomically.
pub struct PositionsDecoder {
    /// URL of the positions shard containing position data.
    shard_url: String,
    /// Object store for accessing the positions shard.
    object_store: Arc<dyn ObjectStore>,
    /// Lazily initialized shard handle.
    shard: OnceLock<Shard>,
    /// Lazily initialized field decoder for position data.
    reader: OnceLock<FieldDecoder>,
}

impl PositionsDecoder {
    /// Creates a new positions decoder for the specified shard.
    ///
    /// This method performs minimal initialization, storing the shard URL and object store
    /// reference while deferring expensive operations like shard opening and reader creation
    /// until they are actually needed via lazy initialization.
    ///
    /// # Arguments
    ///
    /// * `shard_url` - URL of the positions shard to read from. Must be a valid URL
    ///   that can be parsed and accessed via the provided object store.
    /// * `object_store` - Object store for accessing the shard data. Must support
    ///   the protocol specified in the shard URL.
    ///
    /// # Returns
    ///
    /// A new `PositionsDecoder` ready for lazy initialization. The decoder will
    /// not attempt to open the shard or validate the URL until the first call to `read`.
    ///
    /// # Errors
    ///
    /// This method currently always succeeds, but returns a `Result` for future
    /// extensibility and consistency with other constructor patterns in the codebase.
    pub fn new(shard_url: String, object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        Ok(Self {
            shard_url,
            object_store,
            shard: OnceLock::new(),
            reader: OnceLock::new(),
        })
    }

    /// Gets or initializes the shard handle for reading position data.
    ///
    /// This method implements lazy initialization of the shard connection,
    /// opening and preparing the shard only when position data is first accessed.
    /// The shard URL is parsed into an `ObjectUrl` and opened using the configured
    /// object store with default shard options.
    ///
    /// # Returns
    ///
    /// A reference to the opened shard, or an error if the shard cannot be
    /// opened or is incompatible with the expected schema.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The shard URL cannot be parsed as a valid `ObjectUrl`
    /// - The shard cannot be opened via the object store
    /// - The shard format is incompatible with position data expectations
    ///
    /// # Thread Safety
    ///
    /// This method is thread-safe. If multiple threads call this method concurrently,
    /// the first successful initialization will be used and subsequent calls will
    /// return the same shard reference.
    fn shard(&self) -> Result<&Shard> {
        if let Some(shard) = self.shard.get() {
            return Ok(shard);
        }

        let url = ObjectUrl::parse(&self.shard_url)
            .map_err(|e| Error::invalid_format(format!("Invalid positions shard URL: {e}")))?;

        let options = ShardOptions::new(Arc::clone(&self.object_store));

        let shard = options
            .open(url)
            .map_err(|e| Error::invalid_format(format!("Failed to open positions shard: {e}")))?;

        // If another thread won the race, we ignore our shard (they should be equivalent).
        let _ = self.shard.set(shard);

        Ok(self.shard.get().unwrap())
    }

    /// Gets or initializes the field decoder for reading position data.
    ///
    /// This method implements lazy initialization of the field decoder, opening
    /// the positions stripe, fetching the schema, and creating a decoder for the
    /// "position" field only when position data is first accessed.
    ///
    /// # Returns
    ///
    /// A cloned `FieldDecoder` configured for reading u64 position values,
    /// or an error if the decoder cannot be created or the shard schema is invalid.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The shard cannot be opened
    /// - The schema cannot be fetched
    /// - The "position" field is missing from the schema
    /// - The field decoder cannot be created
    fn field_decoder(&self) -> Result<FieldDecoder> {
        if let Some(decoder) = self.reader.get() {
            return Ok(decoder.clone());
        }

        let positions_stripe = self.shard()?.open_stripe(0)?;
        let schema = positions_stripe.fetch_schema()?;
        let (_, position_field) = schema
            .find_field("position")?
            .ok_or_else(|| Error::invalid_format("Missing position field in positions shard"))?;

        let field_decoder = positions_stripe
            .open_field(position_field.data_type()?)?
            .create_decoder()?;

        // If another thread won the race, we ignore our decoder (they should be equivalent).
        let _ = self.reader.set(field_decoder);

        Ok(self.reader.get().unwrap().clone())
    }

    /// Creates a field reader configured for the entire position data range.
    ///
    /// This method creates a new field reader instance that can read from the
    /// complete range of position records in the shard (from 0 to total record count).
    /// The reader is initialized with the field decoder and configured to access
    /// all available position data.
    ///
    /// # Returns
    ///
    /// A boxed `FieldReader` ready to read position data from any range within
    /// the shard, or an error if the reader cannot be created.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The field decoder cannot be obtained
    /// - The shard directory cannot be accessed
    /// - The field reader cannot be created
    fn create_reader(&self) -> Result<Box<dyn FieldReader>> {
        let field_decoder = self.field_decoder()?;
        let total_positions = self.shard()?.directory().total_record_count;
        field_decoder.create_reader(0..total_positions)
    }

    /// Reads and decodes a range of position data based on the representation type.
    ///
    /// This method retrieves position data from the specified range and interprets
    /// it according to the representation type, reconstructing the appropriate
    /// `PositionList` variant. The raw u64 values are read from the shard and
    /// then transformed based on how they were originally encoded.
    ///
    /// # Arguments
    ///
    /// * `repr_type` - How the position data is encoded in the range
    /// * `range` - The range of position values to read from the shard (in record indices)
    ///
    /// # Returns
    ///
    /// A `PositionList` containing the decoded position information, or an error
    /// if the data cannot be read or decoded.
    ///
    /// # Position Representations
    ///
    /// - **Positions**: Each stored value is an individual character position.
    ///   The stored u64 values are directly used as position values.
    /// - **ExactRanges**: Values are stored in pairs (start, end) representing precise ranges.
    ///   The stored u64 values are grouped into chunks of 2 and converted to `Range<u64>`.
    /// - **ApproximateRanges**: Values are stored in pairs (start, end) representing approximate ranges.
    ///   The stored u64 values are grouped into chunks of 2 and converted to `Range<u64>`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The field reader cannot be created
    /// - The specified range cannot be read from the shard
    /// - The raw sequence data cannot be interpreted as u64 values
    pub fn read(&self, repr_type: PositionsReprType, range: Range<u64>) -> Result<PositionList> {
        let mut reader = self.create_reader()?;
        let seq = reader.read_range(range)?;
        let positions = seq.values.as_slice::<u64>();
        let result = match repr_type {
            PositionsReprType::Positions => PositionList::Positions(positions.to_vec()),
            PositionsReprType::ExactRanges => {
                PositionList::ExactRanges(positions.chunks_exact(2).map(|p| p[0]..p[1]).collect())
            }
            PositionsReprType::ApproximateRanges => PositionList::ApproximateRanges(
                positions.chunks_exact(2).map(|p| p[0]..p[1]).collect(),
            ),
        };
        Ok(result)
    }
}
