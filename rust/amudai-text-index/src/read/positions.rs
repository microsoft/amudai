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
    field_decoder::FieldReader,
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
/// # Lazy Initialization
///
/// The decoder uses lazy initialization patterns to defer expensive operations
/// (shard opening, schema resolution, reader creation) until they are actually
/// needed. This enables efficient construction and resource management.
///
/// # Thread Safety
///
/// The decoder uses `OnceLock` for thread-safe lazy initialization of shared
/// resources. However, the reader itself is not thread-safe and should not
/// be accessed concurrently.
pub struct PositionsDecoder {
    /// URL of the positions shard containing position data.
    shard_url: String,
    /// Object store for accessing the positions shard.
    object_store: Arc<dyn ObjectStore>,
    /// Lazily initialized shard handle.
    shard: OnceLock<Shard>,
    /// Lazily initialized field reader for position data.
    reader: OnceLock<Box<dyn FieldReader>>,
}

impl PositionsDecoder {
    /// Creates a new positions decoder for the specified shard.
    ///
    /// This method performs minimal initialization, deferring expensive operations
    /// like shard opening and reader creation until they are actually needed.
    ///
    /// # Arguments
    ///
    /// * `shard_url` - URL of the positions shard to read from
    /// * `object_store` - Object store for accessing the shard data
    ///
    /// # Returns
    ///
    /// A new `PositionsDecoder` ready for lazy initialization, or an error
    /// if the basic setup fails.
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
    ///
    /// # Returns
    ///
    /// A reference to the opened shard, or an error if the shard cannot be
    /// opened or is incompatible with the expected schema.
    fn shard(&self) -> Result<&Shard> {
        if let Some(shard) = self.shard.get() {
            return Ok(shard);
        }

        let options = ShardOptions::new(Arc::clone(&self.object_store));
        let url = ObjectUrl::parse(&self.shard_url)
            .map_err(|e| Error::invalid_format(format!("Invalid positions shard URL: {e}")))?;
        let shard = options
            .open(url)
            .map_err(|e| Error::invalid_format(format!("Failed to open positions shard: {e}")))?;

        if self.shard.set(shard).is_err() {
            panic!("Positions shard already set");
        }
        Ok(self.shard.get().unwrap())
    }

    /// Creates a field reader for position data within the positions shard.
    ///
    /// This method initializes a reader that can efficiently access the position
    /// integer array stored in the positions shard. The reader is configured to
    /// handle the full range of position data in the shard.
    ///
    /// # Returns
    ///
    /// A field reader configured for the position data, or an error if the
    /// shard schema is incompatible or the reader cannot be created.
    fn create_reader(&self) -> Result<Box<dyn FieldReader>> {
        let positions_stripe = self.shard()?.open_stripe(0)?;
        let schema = positions_stripe.fetch_schema()?;
        let (_, position_field) = schema
            .find_field("position")?
            .ok_or_else(|| Error::invalid_format("Missing position field in positions shard"))?;

        let field_decoder = positions_stripe
            .open_field(position_field.data_type()?)?
            .create_decoder()?;
        // Use total element count from the positions shard, not terms pages count
        let total_positions = self.shard()?.directory().total_record_count;
        field_decoder.create_reader(0..total_positions)
    }

    /// Reads and decodes a range of position data based on the representation type.
    ///
    /// This method retrieves position data from the specified range and interprets
    /// it according to the representation type, reconstructing the appropriate
    /// `PositionList` variant.
    ///
    /// # Arguments
    ///
    /// * `repr_type` - How the position data is encoded in the range
    /// * `range` - The range of position values to read from the shard
    ///
    /// # Returns
    ///
    /// A `PositionList` containing the decoded position information, or an error
    /// if the data cannot be read or decoded.
    ///
    /// # Position Representations
    ///
    /// - **Positions**: Each stored value is an individual position
    /// - **ExactRanges**: Values are stored in pairs (start, end) representing precise ranges
    /// - **ApproximateRanges**: Values are stored in pairs (start, end) representing approximate ranges
    ///
    /// # Performance
    ///
    /// The method performs lazy initialization of the field reader on first access,
    /// then efficiently reads only the requested range of position data.
    pub fn read(
        &mut self,
        repr_type: PositionsReprType,
        range: Range<u64>,
    ) -> Result<PositionList> {
        // Establish positions reader if not already done.
        if self.reader.get().is_none() {
            let reader = self.create_reader()?;
            if self.reader.set(reader).is_err() {
                panic!("Positions reader already set");
            }
        }
        let reader = self.reader.get_mut().unwrap();
        let seq = reader.read_range(range)?;
        let positions = seq.values.as_slice::<u64>();
        match repr_type {
            PositionsReprType::Positions => Ok(PositionList::Positions(
                positions.iter().map(|&p| p).collect(),
            )),
            PositionsReprType::ExactRanges => Ok(PositionList::ExactRanges(
                positions.chunks_exact(2).map(|p| p[0]..p[1]).collect(),
            )),
            PositionsReprType::ApproximateRanges => Ok(PositionList::ApproximateRanges(
                positions.chunks_exact(2).map(|p| p[0]..p[1]).collect(),
            )),
        }
    }
}
