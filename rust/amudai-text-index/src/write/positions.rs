//! Builder for constructing the positions shard containing term position data.
//! The positions shard stores the actual position lists that are referenced by
//! terms in the B-Tree pages. This separate storage design enables efficient
//! compression and encoding of position data while keeping the B-Tree structure
//! compact and optimized for navigation.
use arrow_array::{
    RecordBatch,
    builder::{ArrayBuilder, UInt64Builder},
};
use std::sync::Arc;

use amudai_common::{Result, error::Error};
use amudai_format::{
    schema::BasicType as AmudaiBasicType,
    schema_builder::{DataTypeBuilder, SchemaBuilder},
};
use amudai_io::TemporaryFileStore;
use amudai_objectstore::ObjectStore;
use amudai_shard::write::{
    shard_builder::{PreparedShard, ShardBuilder, ShardBuilderParams, ShardFileOrganization},
    stripe_builder::StripeBuilder,
};

use crate::pos_list::PositionList;
use crate::pos_list::PositionsReprType;
use amudai_encodings::block_encoder::BlockEncodingProfile;

/// Builder for constructing the positions shard containing term position data.
///
/// The positions shard stores the actual position lists that are referenced by
/// terms in the B-Tree pages. This separate storage design enables efficient
/// compression and encoding of position data while keeping the B-Tree structure
/// compact and optimized for navigation.
///
/// Position data is stored as a sequential stream of 64-bit integers, with different
/// representation types (exact positions, ranges, etc.) encoded inline. The B-Tree
/// pages contain offsets into this stream, allowing for efficient random access
/// to position data during queries.
///
/// # Buffering Strategy
///
/// The builder uses an internal buffer to accumulate position values before writing
/// them to the underlying shard. This reduces the number of write operations and
/// improves overall performance, especially for small position lists.
pub struct PositionsEncoder {
    /// Builder for the shard containing position data.
    shard: ShardBuilder,
    /// Stripe builder for writing position data.
    stripe: StripeBuilder,
    /// Buffer for accumulating position values.
    buffer: UInt64Builder,
    /// Current offset of the last written position.
    /// This includes positions that not yet flushed to the shard.
    offset: u64,
}

impl PositionsEncoder {
    /// Default buffer size for accumulating position values before flushing.
    ///
    /// This buffer size provides a balance between memory usage and write efficiency.
    /// When the buffer exceeds this size, it will be automatically flushed to the shard.
    const DEFAULT_BUFFER_SIZE: usize = 128;

    /// Creates a new positions encoder.
    ///
    /// Initializes the shard builder with the positions schema and creates the necessary
    /// stripe builder for writing position data. The schema defines a simple structure
    /// with a single column of 64-bit integer positions.
    ///
    /// # Arguments
    ///
    /// * `object_store` - Storage backend for persisting the positions shard
    /// * `temp_store` - Temporary storage for intermediate data during construction
    ///
    /// # Returns
    ///
    /// A new `PositionsEncoder` ready to accept position data, or an error
    /// if initialization fails.
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        temp_store: Arc<dyn TemporaryFileStore>,
    ) -> Result<Self> {
        let shard_params = ShardBuilderParams {
            schema: Self::create_shard_schema(),
            object_store,
            temp_store,
            encoding_profile: BlockEncodingProfile::MinimalCompression,
            file_organization: ShardFileOrganization::SingleFile,
        };
        let shard_builder = ShardBuilder::new(shard_params)?;
        let stripe_builder = shard_builder.build_stripe()?;
        let buffer = UInt64Builder::new();
        Ok(Self {
            shard: shard_builder,
            stripe: stripe_builder,
            buffer,
            offset: 0,
        })
    }

    /// Returns the current offset of the last written position.
    /// This includes positions that have been buffered but not yet flushed.
    ///
    /// # Returns
    ///
    /// The current position offset as a 64-bit unsigned integer.
    pub fn current_offset(&self) -> u64 {
        self.offset
    }

    /// Creates the Amudai schema for the positions shard containing position data.
    ///
    /// This function defines a simple schema for storing position values that are
    /// referenced by the B-Tree term entries. The schema contains a single column
    /// of 64-bit integer positions.
    ///
    /// # Returns
    ///
    /// A SchemaMessage defining the structure of position data
    fn create_shard_schema() -> amudai_format::schema::SchemaMessage {
        let schema = SchemaBuilder::new(vec![
            DataTypeBuilder::new("position", AmudaiBasicType::Int64, false, 0, vec![]).into(),
        ]);
        schema.finish_and_seal()
    }

    /// Appends a position list to the positions shard and returns metadata.
    ///
    /// This method processes different types of position lists and stores them
    /// sequentially in the positions shard. The representation type and position
    /// data are handled according to the specific position list variant.
    ///
    /// # Position List Types
    ///
    /// - **Positions**: Stored as individual position values
    /// - **ExactRanges**: Stored as start/end pairs (2 values per range)
    /// - **ApproximateRanges**: Stored as start/end pairs (2 values per range)
    /// - **Unknown**: Not supported and will cause a panic
    ///
    /// # Arguments
    ///
    /// * `positions` - The position list to append to the positions shard
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - The representation type indicating how the data is encoded
    /// - The new cumulative offset after appending this position list
    ///
    /// # Automatic Buffer Management
    ///
    /// If the internal buffer becomes too large, it will be automatically
    /// flushed to the underlying shard before adding the new positions.
    pub fn push(&mut self, positions: PositionList) -> Result<(PositionsReprType, u64)> {
        if self.buffer.len() > Self::DEFAULT_BUFFER_SIZE {
            self.flush_buffer()?;
        }

        let (repr_type, count) = match positions {
            PositionList::Positions(positions) => {
                self.buffer.append_slice(&positions);
                (PositionsReprType::Positions, positions.len())
            }
            PositionList::ExactRanges(ranges) => {
                for range in &ranges {
                    self.buffer.append_value(range.start);
                    self.buffer.append_value(range.end);
                }
                (PositionsReprType::ExactRanges, ranges.len() * 2)
            }
            PositionList::ApproximateRanges(ranges) => {
                for range in &ranges {
                    self.buffer.append_value(range.start);
                    self.buffer.append_value(range.end);
                }
                (PositionsReprType::ApproximateRanges, ranges.len() * 2)
            }
            PositionList::Unknown => unreachable!("Unexpected position list type"),
        };

        self.offset += count as u64;

        Ok((repr_type, self.offset))
    }

    /// Flushes the internal position buffer to the underlying shard.
    ///
    /// This method creates a RecordBatch from the accumulated position values
    /// and writes it to the stripe builder. After flushing, the buffer is
    /// reset and ready to accept new position data.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the flush operation fails.
    fn flush_buffer(&mut self) -> Result<()> {
        // Create a RecordBatch from the buffer
        let positions_batch = RecordBatch::try_from_iter(vec![(
            "position",
            Arc::new(self.buffer.finish()) as Arc<dyn arrow_array::Array>,
        )])
        .map_err(|e| Error::arrow("Failed to create positions batch", e))?;

        // Write the batch to the stripe
        self.stripe.push_batch(&positions_batch)?;

        // Reset the buffer
        self.buffer = UInt64Builder::new();
        Ok(())
    }

    /// Finalizes the positions shard and returns the prepared shard.
    ///
    /// This method completes the construction of the positions shard by flushing
    /// any remaining buffer content to storage and finalizing the underlying
    /// shard builder. The resulting prepared shard can be used for reading
    /// position data during queries.
    ///
    /// # Returns
    ///
    /// A `PreparedShard` containing the finalized positions data, or an error
    /// if the finalization process fails.
    pub fn finish(mut self) -> Result<PreparedShard> {
        // Flush any remaining buffer content before finalizing the shard
        if !self.buffer.is_empty() {
            self.flush_buffer()?;
        }

        // Finalize the stripe and add it to the shard
        let stripe = self.stripe.finish()?;
        self.shard.add_stripe(stripe)?;
        self.shard.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use amudai_io_impl::temp_file_store;
    use amudai_objectstore::null_store::NullObjectStore;
    use std::ops::Range;

    #[test]
    fn test_positions_encoder() {
        let object_store = Arc::new(NullObjectStore);
        let temp_store = temp_file_store::create_in_memory(10 * 1024 * 1024)
            .expect("Failed to create temporary file store");
        let mut builder = PositionsEncoder::new(object_store, temp_store)
            .expect("Failed to create positions encoder");

        let mut positions_written = 0;
        // Mix different position types to test that offsets accumulate correctly
        let positions = PositionList::new_exact_positions(vec![1, 2, 3]);
        let (repr_type, offset) = builder
            .push(positions)
            .expect("Failed to append exact positions");
        positions_written += 3;
        assert_eq!(repr_type as u8, PositionsReprType::Positions as u8);
        assert_eq!(offset, positions_written);

        let ranges = vec![Range { start: 10, end: 20 }, Range { start: 30, end: 40 }];
        let positions = PositionList::new_exact_ranges(ranges);
        let (repr_type, offset) = builder
            .push(positions)
            .expect("Failed to append exact ranges");
        positions_written += 4; // 2 ranges * 2 values each
        assert_eq!(repr_type as u8, PositionsReprType::ExactRanges as u8);
        assert_eq!(offset, positions_written);

        let approx_ranges = vec![Range {
            start: 100,
            end: 200,
        }];
        let positions = PositionList::new_approximate_ranges(approx_ranges);
        let (repr_type, offset) = builder
            .push(positions)
            .expect("Failed to append approximate ranges");
        positions_written += 2; // 1 range * 2 values
        assert_eq!(repr_type as u8, PositionsReprType::ApproximateRanges as u8);
        assert_eq!(offset, positions_written);

        // Test writing a large chunk of positions to trigger buffer flush
        for i in 0..1000 {
            let (_, offset) = builder
                .push(PositionList::Positions((0..i).collect()))
                .expect("Failed to append positions");
            positions_written += i;
            assert_eq!(offset, positions_written);
        }
        let final_offset = builder.offset;
        assert_eq!(final_offset, positions_written);

        let _shard = builder
            .finish()
            .expect("Failed to finish positions encoder");
    }
}
