use std::sync::Arc;

use amudai_common::Result;
use amudai_encodings::block_encoder::{
    BlockChecksum, BlockEncodingParameters, BlockEncodingPolicy, BlockEncodingProfile,
    PresenceEncoding,
};
use amudai_format::schema::{BasicType, BasicTypeDescriptor};
use amudai_io::temp_file_store::TemporaryFileStore;
use arrow_buffer::BooleanBuffer;

use super::{
    PreparedEncodedBuffer, primitive_buffer::PrimitiveBufferEncoder,
    staging_buffer::BitStagingBuffer,
};

/// An encoder for efficiently buffering and encoding streams of boolean values as bits.
///
/// `BitBufferEncoder` optimizes for both constant run of a single boolean value and
/// for general streams of booleans by automatically switching between a constant run
/// representation and a block-based encoder. This allows for efficient memory usage
/// and encoding performance in streaming scenarios where values are appended
/// incrementally. The encoder supports appending single values, buffers, or repeated
/// values, and finalizes to an encoded buffer suitable for storage or transmission.
///
/// Internally, the encoder starts in a constant run mode and switches to block encoding
/// when mixed values are detected.
pub struct BitBufferEncoder {
    /// Internal representation: either a constant run or a block encoder.
    repr: Repr,
    /// Shared reference to a temporary file store for intermediate storage.
    temp_store: Arc<dyn TemporaryFileStore>,
    /// Block encoding profile to use when switching to the block encoder.
    profile: BlockEncodingProfile,
}

impl BitBufferEncoder {
    /// Creates a new `BitBufferEncoder` with the given temporary file store.
    ///
    /// # Parameters
    /// - `temp_store`: Shared reference to a temporary file store for intermediate storage.
    ///
    /// # Returns
    /// A new `BitBufferEncoder` instance.
    pub fn new(
        temp_store: Arc<dyn TemporaryFileStore>,
        profile: BlockEncodingProfile,
    ) -> BitBufferEncoder {
        BitBufferEncoder {
            repr: Repr::Constant(true, 0),
            temp_store,
            profile,
        }
    }

    /// Creates a block-encoded buffer with the provided constant value in one go.
    pub fn encode_blocks(
        count: usize,
        value: bool,
        temp_store: Arc<dyn TemporaryFileStore>,
    ) -> Result<PreparedEncodedBuffer> {
        let mut encoder = BitBufferBlocksEncoder::new(temp_store)?;
        encoder.append_repeated(count, value)?;
        encoder.finish()
    }

    /// Appends a single boolean value to the encoder.
    ///
    /// Optimizes for runs of the same value; automatically switches to block encoding
    /// if mixed values are detected.
    ///
    /// # Parameters
    /// - `value`: The boolean value to append.
    ///
    /// # Returns
    /// An empty result on success, or an error if encoding fails.
    pub fn append_value(&mut self, value: bool) -> Result<()> {
        match &mut self.repr {
            Repr::Constant(const_value, count) => {
                if *count == 0 || *const_value == value {
                    *count += 1;
                    *const_value = value;
                    return Ok(());
                }
            }
            Repr::Blocks(encoder) => return encoder.append_value(value),
        }
        self.switch_to_blocks()?.append_value(value)
    }

    /// Appends a buffer of boolean values to the encoder.
    ///
    /// Optimizes for all-true or all-false buffers; automatically switches to block
    /// encoding if mixed values are detected.
    ///
    /// # Parameters
    /// - `buffer`: Reference to a buffer of boolean values to append.
    ///
    /// # Returns
    /// An empty result on success, or an error if encoding fails.
    pub fn append(&mut self, buffer: &BooleanBuffer) -> Result<()> {
        match &mut self.repr {
            Repr::Constant(const_value, count) => {
                let set_bits = buffer.count_set_bits();
                let const_buffer = (set_bits == 0) || (set_bits == buffer.len());
                if *count == 0 && const_buffer
                    || (set_bits == 0 && !*const_value)
                    || (set_bits == buffer.len() && *const_value)
                {
                    *count += buffer.len();
                    *const_value = set_bits != 0;
                    return Ok(());
                }
            }
            Repr::Blocks(encoder) => return encoder.append(buffer),
        }
        self.switch_to_blocks()?.append(buffer)
    }

    /// Appends a repeated boolean value to the encoder.
    ///
    /// Efficiently encodes runs of the same value; automatically switches to block
    /// encoding if mixed values are detected.
    ///
    /// # Parameters
    /// - `count`: Number of times to append the value.
    /// - `value`: The boolean value to repeat.
    ///
    /// # Returns
    /// An empty result on success, or an error if encoding fails.
    pub fn append_repeated(&mut self, count: usize, value: bool) -> Result<()> {
        match &mut self.repr {
            Repr::Constant(const_value, prev_count) => {
                if *prev_count == 0 || *const_value == value {
                    *prev_count += count;
                    *const_value = value;
                    return Ok(());
                }
            }
            Repr::Blocks(encoder) => return encoder.append_repeated(count, value),
        }
        self.switch_to_blocks()?.append_repeated(count, value)
    }

    /// Finalizes the encoder, returning the encoded buffer.
    ///
    /// After calling this method, the encoder cannot be used further.
    ///
    /// # Returns
    /// An `EncodedBitBuffer` containing either a constant run or a block-encoded buffer,
    /// or an error if finalization fails.
    pub fn finish(self) -> Result<EncodedBitBuffer> {
        match self.repr {
            Repr::Constant(value, count) => Ok(EncodedBitBuffer::Constant(value, count)),
            Repr::Blocks(encoder) => Ok(EncodedBitBuffer::Blocks(encoder.finish()?)),
        }
    }
}

/// Represents the result of encoding a stream of boolean values as bits.
///
/// `EncodedBitBuffer` is produced by [`BitBufferEncoder::finish`] and encapsulates
/// the most efficient representation for the encoded boolean data. It optimizes
/// for both constant runs (all values are the same) and general streams (mixed values)
/// by selecting between a compact constant run or a block-encoded buffer.
///
/// # Variants
///
/// - [`EncodedBitBuffer::Constant`]: Used when all values in the stream are identical
///   (all true or all false).
/// - [`EncodedBitBuffer::Blocks`]: Used for general or mixed-value streams, storing
///   the encoded data in blocks.
///
/// See also: [`BitBufferEncoder`], [`PreparedEncodedBuffer`].
pub enum EncodedBitBuffer {
    /// A constant run of a single boolean value.
    ///
    /// This variant is used when the entire stream consists of repeated
    /// occurrences of the same boolean value (`true` or `false`).
    ///
    /// - `bool`: The constant value.
    /// - `usize`: The number of times the value is repeated.
    Constant(bool, usize),

    /// A block-encoded buffer of boolean values.
    ///
    /// This variant is used when the stream contains a mix of `true` and `false`.
    ///
    /// - [`PreparedEncodedBuffer`]: The encoded buffer containing the bit-packed
    ///   data.
    Blocks(PreparedEncodedBuffer),
}

impl BitBufferEncoder {
    #[cold]
    fn switch_to_blocks(&mut self) -> Result<&mut BitBufferBlocksEncoder> {
        let repr = std::mem::replace(&mut self.repr, Repr::Constant(true, 0));
        match repr {
            Repr::Constant(value, count) => {
                let mut encoder =
                    BitBufferBlocksEncoder::with_profile(self.profile, self.temp_store.clone())?;
                encoder.append_repeated(count, value)?;
                self.repr = Repr::Blocks(Box::new(encoder));
            }
            Repr::Blocks(encoder) => {
                self.repr = Repr::Blocks(encoder);
            }
        }

        match &mut self.repr {
            Repr::Blocks(encoder) => Ok(encoder),
            _ => panic!("Unexpected repr"),
        }
    }
}

enum Repr {
    Constant(bool, usize),
    Blocks(Box<BitBufferBlocksEncoder>),
}

/// An encoder for efficiently buffering and block-encoding streams of boolean
/// values as bits.
///
/// This struct manages a staging buffer for incoming boolean values and encodes
/// them into blocks using a primitive buffer encoder. It is optimized for
/// streaming scenarios where values are appended incrementally and periodically
/// flushed into encoded blocks. The encoder supports configurable encoding
/// policies.
pub struct BitBufferBlocksEncoder {
    /// Staging buffer that accumulates incoming boolean values before
    /// block encoding.
    staging: BitStagingBuffer,
    /// Underlying encoder responsible for encoding staged data into blocks.
    encoder: PrimitiveBufferEncoder,
    /// Total count of values pushed to the encoder.
    value_count: u64,
    /// Count of `true` values pushed to the encoder.
    true_count: u64,
}

impl BitBufferBlocksEncoder {
    /// Creates a new `BitBufferBlockedEncoder` with a default balanced encoding policy.
    ///
    /// # Parameters
    /// - `temp_store`: Shared reference to a temporary file store for intermediate storage.
    ///
    /// # Returns
    /// A new `BitBufferBlockedEncoder` instance on success, or an error if initialization
    /// fails.
    pub fn new(temp_store: Arc<dyn TemporaryFileStore>) -> Result<BitBufferBlocksEncoder> {
        BitBufferBlocksEncoder::with_profile(BlockEncodingProfile::Balanced, temp_store)
    }

    /// Creates a new `BitBufferBlockedEncoder` with the specified encoding profile.
    pub fn with_profile(
        encoding_profile: BlockEncodingProfile,
        temp_store: Arc<dyn TemporaryFileStore>,
    ) -> Result<BitBufferBlocksEncoder> {
        BitBufferBlocksEncoder::with_policy(
            BlockEncodingPolicy {
                parameters: BlockEncodingParameters {
                    checksum: BlockChecksum::Enabled,
                    presence: PresenceEncoding::Disabled,
                },
                profile: encoding_profile,
                size_constraints: None,
            },
            temp_store,
        )
    }

    /// Creates a new `BitBufferBlockedEncoder` with a custom encoding policy.
    ///
    /// # Parameters
    /// - `policy`: The block encoding policy to use for block encoding.
    /// - `temp_store`: Shared reference to a temporary file store for
    ///   intermediate storage.
    ///
    /// # Returns
    /// A new `BitBufferBlockedEncoder` instance on success, or an error
    /// if initialization fails.
    pub fn with_policy(
        policy: BlockEncodingPolicy,
        temp_store: Arc<dyn TemporaryFileStore>,
    ) -> Result<BitBufferBlocksEncoder> {
        let encoder = PrimitiveBufferEncoder::new(
            policy,
            BasicTypeDescriptor {
                basic_type: BasicType::Int8,
                fixed_size: 0,
                signed: false,
                extended_type: Default::default(),
            },
            temp_store,
        )?;
        Ok(BitBufferBlocksEncoder {
            staging: BitStagingBuffer::new(),
            encoder,
            value_count: 0,
            true_count: 0,
        })
    }

    /// Returns the total number of boolean values that have been processed by this encoder.
    ///
    /// This includes all values that have been appended via [`append_value`], [`append`],
    /// or [`append_repeated`] methods, regardless of their boolean value.
    ///
    /// [`append_value`]: Self::append_value
    /// [`append`]: Self::append
    /// [`append_repeated`]: Self::append_repeated
    pub fn value_count(&self) -> u64 {
        self.value_count
    }

    /// Returns the total number of `true` values that have been processed by this encoder.
    ///
    /// This count includes all `true` values that have been appended via [`append_value`],
    /// [`append`], or [`append_repeated`] methods.
    ///
    /// [`append_value`]: Self::append_value
    /// [`append`]: Self::append
    /// [`append_repeated`]: Self::append_repeated
    pub fn true_count(&self) -> u64 {
        self.true_count
    }

    /// Returns the total number of `false` values that have been processed by this encoder.
    ///
    /// This count is calculated as the difference between the total value count and the
    /// true value count. It includes all `false` values that have been appended via
    /// [`append_value`], [`append`], or [`append_repeated`] methods.
    ///
    /// [`append_value`]: Self::append_value
    /// [`append`]: Self::append
    /// [`append_repeated`]: Self::append_repeated
    pub fn false_count(&self) -> u64 {
        self.value_count - self.true_count
    }

    /// Appends a single boolean value to the staging buffer.
    ///
    /// If the staging buffer exceeds its flush threshold, a flush is triggered.
    ///
    /// # Parameters
    /// - `value`: The boolean value to append.
    ///
    /// # Returns
    /// An empty result on success, or an error if flushing fails.
    pub fn append_value(&mut self, value: bool) -> Result<()> {
        self.value_count += 1;
        self.true_count += value as u64;

        self.staging.append_value(value);
        if self.may_flush() {
            self.flush(false)?;
        }
        Ok(())
    }

    /// Appends a buffer of boolean values to the staging buffer.
    ///
    /// If the staging buffer exceeds its flush threshold, a flush is triggered.
    ///
    /// # Parameters
    /// - `buffer`: Reference to a buffer of boolean values to append.
    ///
    /// # Returns
    /// An empty result on success, or an error if flushing fails.
    pub fn append(&mut self, buffer: &BooleanBuffer) -> Result<()> {
        self.value_count += buffer.len() as u64;
        self.true_count += buffer.count_set_bits() as u64;

        self.staging.append(buffer);
        if self.may_flush() {
            self.flush(false)?;
        }
        Ok(())
    }

    /// Appends a repeated boolean value to the staging buffer.
    ///
    /// This is useful for efficiently encoding runs of the same value.
    /// If the staging buffer exceeds its flush threshold, a flush is triggered.
    ///
    /// # Parameters
    /// - `count`: Number of times to append the value.
    /// - `value`: The boolean value to repeat.
    ///
    /// # Returns
    /// An empty result on success, or an error if flushing fails.
    pub fn append_repeated(&mut self, mut count: usize, value: bool) -> Result<()> {
        self.value_count += count as u64;
        self.true_count += (value as u64) * (count as u64);

        // Avoid unreasonable resizing of the staging buffer, proceed in chunks of up to 1M bits.
        while count != 0 {
            let chunk_size = std::cmp::min(count, 1024 * 1024);
            self.staging.append_repeated(chunk_size, value);
            count -= chunk_size;
            if self.may_flush() {
                self.flush(false)?;
            }
        }
        Ok(())
    }

    /// Finalizes the encoder, flushing any remaining staged data and returning
    /// the encoded buffer.
    ///
    /// After calling this method, the encoder cannot be used further.
    ///
    /// # Returns
    /// The prepared encoded buffer on success, or an error if flushing or finalization
    /// fails.
    pub fn finish(mut self) -> Result<PreparedEncodedBuffer> {
        self.flush(true)?;
        assert_eq!(
            self.staging.logical_offset(),
            self.staging.logical_position()
        );

        self.encoder.finish()
    }
}

impl BitBufferBlocksEncoder {
    /// Determines whether the staging buffer has reached the threshold for flushing.
    ///
    /// # Returns
    /// `true` if the buffer should be flushed; otherwise, `false`.
    #[inline]
    fn may_flush(&self) -> bool {
        self.staging.len() >= BitStagingBuffer::DEFAULT_LEN_THRESHOLD
    }

    /// Flushes staged data into encoded blocks if the threshold is reached or if forced.
    ///
    /// When `force` is true, all remaining data is flushed, including any partial blocks.
    /// When `force` is false, only full byte chunks are flushed if the threshold is met.
    ///
    /// # Parameters
    /// - `force`: Whether to force flushing of all remaining data.
    ///
    /// # Returns
    /// An empty result on success, or an error if encoding fails.
    #[cold]
    fn flush(&mut self, force: bool) -> Result<()> {
        if self.staging.is_empty() {
            return Ok(());
        }

        let sample = self.staging.sample(2048);
        let block_size = if !sample.is_empty() {
            self.encoder.analyze_sample(&sample)?.value_count.start
        } else {
            8 * 1024
        };

        for chunk in self.staging.dequeue_full_byte_chunks(block_size) {
            self.encoder.encode_block(&chunk)?;
        }

        if force {
            let tail = self.staging.dequeue_remaining();
            if !tail.is_empty() {
                self.encoder.encode_block(&tail)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // BitBufferEncoder tests
    use super::*;
    use amudai_io::temp_file_store::null_temp_store::NullTempFileStore;
    use arrow_buffer::BooleanBuffer;
    use std::sync::Arc;

    fn create_bit_buffer_encoder() -> BitBufferEncoder {
        let temp_store =
            amudai_io_impl::temp_file_store::create_in_memory(8 * 1024 * 1024).unwrap();
        BitBufferEncoder::new(temp_store, Default::default())
    }

    #[test]
    fn bitbufferencoder_append_single_true() {
        let mut enc = create_bit_buffer_encoder();
        enc.append_value(true).unwrap();
        match enc.finish().unwrap() {
            EncodedBitBuffer::Constant(v, n) => {
                assert!(v);
                assert_eq!(n, 1);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn bitbufferencoder_append_single_false() {
        let mut enc = create_bit_buffer_encoder();
        enc.append_value(false).unwrap();
        match enc.finish().unwrap() {
            EncodedBitBuffer::Constant(v, n) => {
                assert!(!v);
                assert_eq!(n, 1);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn bitbufferencoder_append_multiple_same() {
        let mut enc = create_bit_buffer_encoder();
        for _ in 0..10 {
            enc.append_value(true).unwrap();
        }
        match enc.finish().unwrap() {
            EncodedBitBuffer::Constant(v, n) => {
                assert!(v);
                assert_eq!(n, 10);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn bitbufferencoder_append_multiple_switch_to_blocks() {
        let mut enc = create_bit_buffer_encoder();
        enc.append_value(true).unwrap();
        enc.append_value(false).unwrap();
        match enc.finish().unwrap() {
            EncodedBitBuffer::Blocks(buf) => {
                assert!(buf.data_size > 0);
                verify_buf(&buf, 2);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn bitbufferencoder_append_buffer_all_true() {
        let mut enc = create_bit_buffer_encoder();
        let buf = BooleanBuffer::from_iter(std::iter::repeat(true).take(32));
        enc.append(&buf).unwrap();
        match enc.finish().unwrap() {
            EncodedBitBuffer::Constant(v, n) => {
                assert!(v);
                assert_eq!(n, 32);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn bitbufferencoder_append_buffer_all_false() {
        let mut enc = create_bit_buffer_encoder();
        let buf = BooleanBuffer::from_iter(std::iter::repeat(false).take(32));
        enc.append(&buf).unwrap();
        match enc.finish().unwrap() {
            EncodedBitBuffer::Constant(v, n) => {
                assert!(!v);
                assert_eq!(n, 32);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn bitbufferencoder_append_buffer_mixed() {
        let mut enc = create_bit_buffer_encoder();
        let buf = BooleanBuffer::from_iter((0..16).map(|i| i % 2 == 0));
        enc.append(&buf).unwrap();
        match enc.finish().unwrap() {
            EncodedBitBuffer::Blocks(buf) => {
                verify_buf(&buf, 16);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn bitbufferencoder_append_repeated_true() {
        let mut enc = create_bit_buffer_encoder();
        enc.append_repeated(100, true).unwrap();
        match enc.finish().unwrap() {
            EncodedBitBuffer::Constant(v, n) => {
                assert!(v);
                assert_eq!(n, 100);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn bitbufferencoder_append_repeated_false() {
        let mut enc = create_bit_buffer_encoder();
        enc.append_repeated(100, false).unwrap();
        match enc.finish().unwrap() {
            EncodedBitBuffer::Constant(v, n) => {
                assert!(!v);
                assert_eq!(n, 100);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn bitbufferencoder_append_repeated_switch_to_blocks() {
        let mut enc = create_bit_buffer_encoder();
        enc.append_repeated(10, true).unwrap();
        enc.append_repeated(5, false).unwrap();
        match enc.finish().unwrap() {
            EncodedBitBuffer::Blocks(buf) => {
                verify_buf(&buf, 15);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn bitbufferencoder_empty() {
        let enc = create_bit_buffer_encoder();
        match enc.finish().unwrap() {
            EncodedBitBuffer::Constant(_, n) => {
                assert_eq!(n, 0);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn bitbufferencoder_large_constant() {
        let mut enc = create_bit_buffer_encoder();
        enc.append_repeated(10000, true).unwrap();
        match enc.finish().unwrap() {
            EncodedBitBuffer::Constant(v, n) => {
                assert!(v);
                assert_eq!(n, 10000);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn bitbufferencoder_large_blocks() {
        let mut enc = create_bit_buffer_encoder();
        for i in 0..10000 {
            enc.append_value(i % 2 == 0).unwrap();
        }
        match enc.finish().unwrap() {
            EncodedBitBuffer::Blocks(buf) => {
                verify_buf(&buf, 10000);
            }
            _ => panic!(),
        }
    }

    // BitBufferBlocksEncoder tests
    use crate::read::{
        block_stream::BlockReaderPrefetch, primitive_buffer::PrimitiveBufferDecoder,
    };

    fn bit_buffer_blocks_encoder_default() -> BitBufferBlocksEncoder {
        let temp_store =
            amudai_io_impl::temp_file_store::create_in_memory(8 * 1024 * 1024).unwrap();
        BitBufferBlocksEncoder::new(temp_store).unwrap()
    }

    fn bit_buffer_blocks_encoder_custom() -> BitBufferBlocksEncoder {
        let policy = BlockEncodingPolicy {
            parameters: BlockEncodingParameters {
                checksum: BlockChecksum::Disabled,
                presence: PresenceEncoding::Disabled,
            },
            profile: BlockEncodingProfile::HighCompression,
            size_constraints: None,
        };
        BitBufferBlocksEncoder::with_policy(policy, Arc::new(NullTempFileStore)).unwrap()
    }

    fn verify_buf(buf: &PreparedEncodedBuffer, bit_count: u64) {
        let decoder = PrimitiveBufferDecoder::from_prepared_buffer(
            &buf,
            BasicTypeDescriptor {
                basic_type: BasicType::Int8,
                fixed_size: 0,
                signed: false,
                extended_type: Default::default(),
            },
        )
        .unwrap();
        let byte_count = decoder.block_stream().block_map().value_count().unwrap();
        assert_eq!((bit_count + 7) / 8, byte_count);
        let mut reader = decoder
            .create_reader(std::iter::empty(), BlockReaderPrefetch::Disabled)
            .unwrap();
        let mut pos = 0;
        while pos < byte_count {
            let read_len =
                std::cmp::min(fastrand::u64(1..=(byte_count / 16) + 1), byte_count - pos).max(1);
            reader.read(pos..pos + read_len).unwrap();
            pos += read_len;
        }
    }

    #[test]
    fn test_new_encoder() {
        let _ = bit_buffer_blocks_encoder_default();
    }

    #[test]
    fn test_new_encoder_custom_policy() {
        let _ = bit_buffer_blocks_encoder_custom();
    }

    #[test]
    fn test_append_single_true() {
        let mut enc = bit_buffer_blocks_encoder_default();
        enc.append_value(true).unwrap();
        let buf = enc.finish().unwrap();
        verify_buf(&buf, 1);
    }

    #[test]
    fn test_append_single_false() {
        let mut enc = bit_buffer_blocks_encoder_default();
        enc.append_value(false).unwrap();
        let buf = enc.finish().unwrap();
        verify_buf(&buf, 1);
    }

    #[test]
    fn test_append_alternating() {
        let mut enc = bit_buffer_blocks_encoder_default();
        for i in 0..100 {
            enc.append_value(i % 2 == 0).unwrap();
        }
        let buf = enc.finish().unwrap();
        verify_buf(&buf, 100);
    }

    #[test]
    fn test_append_buffer_all_true() {
        let mut enc = bit_buffer_blocks_encoder_default();
        let buffer = BooleanBuffer::from_iter(std::iter::repeat(true).take(128));
        enc.append(&buffer).unwrap();
        let buf = enc.finish().unwrap();
        verify_buf(&buf, 128);
    }

    #[test]
    fn test_append_buffer_all_false() {
        let mut enc = bit_buffer_blocks_encoder_default();
        let buffer = BooleanBuffer::from_iter(std::iter::repeat(false).take(128));
        enc.append(&buffer).unwrap();
        let buf = enc.finish().unwrap();
        verify_buf(&buf, 128);
    }

    #[test]
    fn test_append_buffer_mixed() {
        let mut enc = bit_buffer_blocks_encoder_default();
        let buffer = BooleanBuffer::from_iter((0..128).map(|i| i % 3 == 0));
        enc.append(&buffer).unwrap();
        let buf = enc.finish().unwrap();
        verify_buf(&buf, 128);
    }

    #[test]
    fn test_append_repeated_true() {
        let mut enc = bit_buffer_blocks_encoder_default();
        enc.append_repeated(256, true).unwrap();
        let buf = enc.finish().unwrap();
        verify_buf(&buf, 256);
    }

    #[test]
    fn test_append_repeated_false() {
        let mut enc = bit_buffer_blocks_encoder_default();
        enc.append_repeated(256, false).unwrap();
        let buf = enc.finish().unwrap();
        verify_buf(&buf, 256);
    }

    #[test]
    fn test_append_repeated_zero() {
        let mut enc = bit_buffer_blocks_encoder_default();
        enc.append_repeated(0, true).unwrap();
        let buf = enc.finish().unwrap();
        assert_eq!(buf.data_size, 0);
    }

    #[test]
    fn test_flush_threshold() {
        let mut enc = bit_buffer_blocks_encoder_default();
        let threshold = super::BitStagingBuffer::DEFAULT_LEN_THRESHOLD;
        for _ in 0..threshold - 1 {
            enc.append_value(true).unwrap();
        }
        enc.append_value(true).unwrap();
        enc.append_value(false).unwrap();
        assert!(enc.staging.logical_offset() > 32 * 1024);
        let buf = enc.finish().unwrap();
        verify_buf(&buf, 65536 + 1);
    }

    #[test]
    fn test_finish_empty() {
        let enc = bit_buffer_blocks_encoder_default();
        let buf = enc.finish().unwrap();
        assert_eq!(buf.data_size, 0);
    }

    #[test]
    fn test_finish_after_partial_block() {
        let mut enc = bit_buffer_blocks_encoder_default();
        for _ in 0..15 {
            enc.append_value(true).unwrap();
        }
        let buf = enc.finish().unwrap();
        verify_buf(&buf, 15);
    }

    #[test]
    fn test_large_input() {
        let mut enc = bit_buffer_blocks_encoder_default();
        for _ in 0..1000000 {
            enc.append_value(true).unwrap();
        }
        let buf = enc.finish().unwrap();
        dbg!(buf.data_size);
        verify_buf(&buf, 1000000);
    }
}
