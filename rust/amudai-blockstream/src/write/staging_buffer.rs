//! FIFO-like staging buffers for encoding the value blocks.

use std::sync::Arc;

use amudai_common::{error::Error, Result};
use arrow_array::{Array, UInt8Array};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, ScalarBuffer};
use arrow_processing::{
    array_sequence::ArraySequence, data_size::binary_data_size, for_each::for_each_as_binary,
};

/// A buffer for staging `binary`-like value arrays (`Binary`, `Utf8`,
/// `FixedSizeBinary`), optimized for efficient appending, sampling
/// and slice consumption.
pub struct BytesStagingBuffer {
    /// A "queue" of accumulated arrays.
    arrays: ArraySequence,
    /// Current logical offset of the first value in this staging buffer.
    /// It is advanced when a slice of values is dequeued from the buffer.
    logical_offset: usize,
    /// Total size of the currently accumulated binary data in bytes
    data_size: usize,
    /// A common "canonical" Arrow data type that all values are normalized to upon
    /// dequeuing or sampling.
    normalized_type: arrow_schema::DataType,
}

impl BytesStagingBuffer {
    /// The default length threshold (logical value slot count) for the accumulated buffer.
    pub const DEFAULT_LEN_THRESHOLD: usize = 64 * 1024;
    /// The default data size threshold for the accumulated buffer.
    pub const DEFAULT_DATA_SIZE_THRESHOLD: usize = 4 * 1024 * 1024;
    /// The minimum value count of a sampled run in a multi-run sample.
    pub const MIN_SAMPLED_RUN: usize = 64;
    /// The maximum value count of a sampled run in a multi-run sample.
    pub const MAX_SAMPLED_RUN: usize = 64 * 3;

    /// Creates a new empty `BytesStagingBuffer`.
    pub fn new(normalized_type: arrow_schema::DataType) -> BytesStagingBuffer {
        BytesStagingBuffer {
            arrays: ArraySequence::new(),
            logical_offset: 0,
            data_size: 0,
            normalized_type,
        }
    }

    /// Returns the total number of logical values (value slots) in the buffer.
    pub fn len(&self) -> usize {
        self.arrays.len()
    }

    /// Returns `true` if there are no elements in the accumulated buffer.
    pub fn is_empty(&self) -> bool {
        self.arrays.is_empty()
    }

    /// Returns the current logical offset of the first value in this staging buffer.
    pub fn logical_offset(&self) -> usize {
        self.logical_offset
    }

    /// Returns the total size of the underlying binary data in bytes.
    pub fn data_size(&self) -> usize {
        self.data_size
    }

    /// Appends a new Arrow array to the end of the buffer.
    ///
    /// The data size of the buffer is updated to reflect the added array.
    ///
    /// # Arguments
    ///
    /// * `array`: Arrow array to append. Should be one of the `Binary`
    ///   or `Utf8` variations.
    pub fn append(&mut self, array: Arc<dyn Array>) {
        let data_size = binary_data_size(&array);
        self.arrays.append(array);
        self.data_size = self
            .data_size
            .checked_add(data_size)
            .expect("add data_size");
    }

    /// Samples a subset of the data in the buffer, up to a maximum number of elements and data size.
    ///
    /// The sample is built from multiple non-overlapping sequential runs of elements,
    /// collected up to a total desired count or max data size.
    ///
    /// # Arguments
    ///
    /// * `count`: The desired number of elements to sample.
    /// * `max_data_size`: The maximum data size in bytes to sample.
    ///
    /// # Returns
    ///
    /// A new Arrow array containing the sampled data.
    pub fn sample(&self, count: usize, max_data_size: usize) -> Result<Arc<dyn Array>> {
        if self.is_empty() {
            return Ok(arrow_array::new_empty_array(&self.normalized_type));
        }
        assert_ne!(self.len(), 0);
        let avg_buf_size = (self.data_size() / self.len()).max(1);
        let max_count = (max_data_size / avg_buf_size).max(1);
        let count = count.min(max_count);
        self.arrays
            .sample_ranges(count, Self::MIN_SAMPLED_RUN, Self::MAX_SAMPLED_RUN)
            .flatten(&self.normalized_type)
            .map_err(|e| Error::arrow("flatten bytes array sequence sample", e))
    }

    /// Dequeues a specified number of elements (e.g. strings) from the buffer.
    ///
    /// The logical offset and data size of the buffer are updated to reflect the removed elements.
    ///
    /// # Arguments
    ///
    /// * `count`: The number of elements to dequeue.
    ///
    /// # Returns
    ///
    /// A new Arrow array containing the dequeued data.
    pub fn dequeue(&mut self, count: usize) -> Result<Arc<dyn Array>> {
        let arrays = self.arrays.dequeue(count);
        let data_size: usize = arrays.arrays().map(|a| binary_data_size(a)).sum();
        self.logical_offset += arrays.len();
        self.data_size -= self.data_size.min(data_size);
        arrays
            .flatten(&self.normalized_type)
            .map_err(|e| Error::arrow("flatten bytes array sequence", e))
    }

    /// Dequeues elements (e.g. strings) from the buffer, up to a maximum number of elements
    /// and data size.
    ///
    /// # Arguments
    ///
    /// * `max_count`: The maximum number of elements to dequeue.
    /// * `max_data_size`: The maximum data size in bytes to dequeue.
    ///
    /// # Returns
    ///
    /// A new Arrow array containing the dequeued data.
    pub fn dequeue_at_most(
        &mut self,
        max_count: usize,
        max_data_size: usize,
    ) -> Result<Arc<dyn Array>> {
        let count = self.count_elements_up_to_size(max_count, max_data_size);
        self.dequeue(count)
    }

    /// Counts the number of elements that can be dequeued without exceeding the maximum count or data size.
    fn count_elements_up_to_size(&self, max_count: usize, max_data_size: usize) -> usize {
        let mut count = 0;
        let mut size = 0;
        for array in self.arrays.arrays() {
            let res = for_each_as_binary(array, |_, buf| {
                let buf_len = buf.map_or(0, <[u8]>::len);
                if count + 1 > max_count || size + buf_len > max_data_size {
                    Err::<(), ()>(())
                } else {
                    count += 1;
                    size += buf_len;
                    Ok::<(), ()>(())
                }
            });
            if res.is_err() {
                break;
            }
        }
        count.max(1)
    }
}

/// A buffer for staging primitive value arrays, optimized for efficient
/// appending, sampling and slice consumption.
pub struct PrimitiveStagingBuffer {
    /// A "queue" of accumulated arrays.
    arrays: ArraySequence,
    /// Current logical offset of the first value in this staging buffer.
    /// It is advanced when a slice of values is dequeued from the buffer.
    logical_offset: usize,
    /// A common "canonical" Arrow data type that all values are normalized to upon
    /// dequeuing or sampling.
    normalized_type: arrow_schema::DataType,
}

impl PrimitiveStagingBuffer {
    /// The default total length threshold for the accumulated sequence of arrays.
    pub const DEFAULT_LEN_THRESHOLD: usize = 64 * 1024;
    /// The minimum value count of a sampled run in a multi-run sample.
    pub const MIN_SAMPLED_RUN: usize = 64;
    /// The maximum value count of a sampled run in a multi-run sample.
    pub const MAX_SAMPLED_RUN: usize = 64 * 3;

    /// Creates a new empty `PrimitiveStagingBuffer`.
    ///
    /// # Arguments
    ///
    /// * `normalized_type`: The "canonical" common Arrow data type of the primitive
    ///   values,used for upcasting the values during `sample()` and `dequeue()`.
    pub fn new(normalized_type: arrow_schema::DataType) -> PrimitiveStagingBuffer {
        PrimitiveStagingBuffer {
            arrays: ArraySequence::new(),
            logical_offset: 0,
            normalized_type,
        }
    }

    /// Returns the total number of logical values (value slots) in the buffer.
    pub fn len(&self) -> usize {
        self.arrays.len()
    }

    /// Returns the current logical offset of the first value in this staging buffer.
    pub fn logical_offset(&self) -> usize {
        self.logical_offset
    }

    /// Returns `true` if there are no accumulated elements.
    pub fn is_empty(&self) -> bool {
        self.arrays.is_empty()
    }

    /// Returns the total size of the underlying data in bytes.
    pub fn data_size(&self) -> usize {
        self.len()
            * self
                .normalized_type
                .primitive_width()
                .expect("primitive width")
    }

    /// Appends a new Arrow array to the end of the buffer.
    ///
    /// # Arguments
    ///
    /// * `array`: Arrow array to append.
    pub fn append(&mut self, array: Arc<dyn Array>) {
        self.arrays.append(array);
    }

    /// Samples a subset of the data in the buffer, up to a maximum `count` of elements.
    ///
    /// The sample is built from multiple non-overlapping sequential runs of elements,
    /// collected up to a total desired count.
    ///
    /// # Arguments
    ///
    /// * `count`: The desired number of elements to sample.
    ///
    /// # Returns
    ///
    /// A new Arrow array containing the sampled data.
    pub fn sample(&self, count: usize) -> Result<Arc<dyn Array>> {
        self.arrays
            .sample_ranges(count, Self::MIN_SAMPLED_RUN, Self::MAX_SAMPLED_RUN)
            .flatten(&self.normalized_type)
            .map_err(|e| Error::arrow("flatten primitive array sequence sample", e))
    }

    /// Dequeues a specified number of elements from the beginning of the buffer.
    ///
    /// This method removes the specified number of elements from the buffer and returns
    /// them as a new Arrow array. The logical offset of the buffer is updated accordingly.
    ///
    /// # Arguments
    ///
    /// * `count`: The number of elements to dequeue.
    ///
    /// # Returns
    ///
    /// A new Arrow array containing the dequeued elements.
    pub fn dequeue(&mut self, count: usize) -> Result<Arc<dyn Array>> {
        let arrays = self.arrays.dequeue(count);
        self.logical_offset += arrays.len();
        arrays
            .flatten(&self.normalized_type)
            .map_err(|e| Error::arrow("flatten primitive array sequence", e))
    }
}

/// A FIFO-like buffer for staging boolean values (bits), optimized for efficient appending,
/// sampling, and chunked consumption.
///
/// `BitStagingBuffer` is primarily used for encoding boolean columns or 'presence' data buffers
/// in the storage format.
/// It accumulates bits using an internal [`BooleanBufferBuilder`], and supports:
/// - Appending single or multiple bits, or entire [`BooleanBuffer`]s.
/// - Random sampling of contiguous byte-aligned chunks.
/// - Dequeuing the buffer in fixed-size byte chunks or draining all remaining bits
///   as Arrow [`UInt8Array`]s.
///
/// The buffer maintains logical position and offset counters to track the total number of bits
/// appended and consumed, respectively.
pub struct BitStagingBuffer {
    /// Internal builder used to accumulate boolean values (bits) before
    /// chunking or sampling.
    builder: BooleanBufferBuilder,

    /// Logical position: total number of bits ever appended to the buffer.
    ///
    /// This counter is incremented with every append operation and is not
    /// decremented when bits are dequeued.
    logical_position: u64,

    /// Logical offset: total number of bits dequeued (consumed) from the buffer.
    ///
    /// This counter is incremented when bits are dequeued in chunks or drained,
    /// and represents the number of bits no longer present in the buffer.
    logical_offset: u64,
}

impl BitStagingBuffer {
    /// The default total length threshold (in bits) for the buffer.
    ///
    /// This value determines the initial capacity for the internal builder,
    /// chosen for performance and memory efficiency.
    pub const DEFAULT_LEN_THRESHOLD: usize = 64 * 1024;

    /// Creates a new empty [`BitStagingBuffer`] with default capacity.
    ///
    /// # Returns
    ///
    /// A new, empty `BitStagingBuffer` with internal capacity set to [`Self::DEFAULT_LEN_THRESHOLD`].
    pub fn new() -> BitStagingBuffer {
        BitStagingBuffer {
            builder: BooleanBufferBuilder::new(Self::DEFAULT_LEN_THRESHOLD),
            logical_position: 0,
            logical_offset: 0,
        }
    }

    /// Returns the number of bits currently stored in the buffer (not yet dequeued).
    ///
    /// # Returns
    ///
    /// The number of bits currently present in the buffer.
    pub fn len(&self) -> usize {
        self.builder.len()
    }

    /// Returns the total number of bits ever appended to the buffer.
    ///
    /// This value is incremented with every append operation and is not
    /// decremented when bits are dequeued.
    pub fn logical_position(&self) -> u64 {
        self.logical_position
    }

    /// Returns the total number of bits that have been dequeued (consumed)
    /// from the buffer.
    ///
    /// This value is incremented when bits are dequeued in chunks or drained,
    /// and represents the number of bits no longer present in the buffer.
    pub fn logical_offset(&self) -> u64 {
        self.logical_offset
    }

    /// Returns `true` if the buffer contains no bits.
    ///
    /// # Returns
    ///
    /// `true` if the buffer is empty, `false` otherwise.
    pub fn is_empty(&self) -> bool {
        self.builder.is_empty()
    }

    /// Appends a single boolean value (bit) to the buffer.
    ///
    /// # Arguments
    ///
    /// * `value` - The boolean value to append as a bit.
    ///
    /// Increments the logical position by 1.
    #[inline]
    pub fn append_value(&mut self, value: bool) {
        self.builder.append(value);
        self.logical_position += 1;
    }

    /// Appends all bits from the provided [`BooleanBuffer`] to the buffer.
    ///
    /// # Arguments
    ///
    /// * `buffer` - The [`BooleanBuffer`] whose bits will be appended.
    ///
    /// Increments the logical position by the number of bits appended.
    pub fn append(&mut self, buffer: &BooleanBuffer) {
        self.builder.append_buffer(buffer);
        self.logical_position += buffer.len() as u64;
    }

    /// Appends `count` repeated bits with the specified value to the buffer.
    ///
    /// # Arguments
    ///
    /// * `count` - The number of bits to append.
    /// * `value` - The boolean value to repeat.
    ///
    /// Increments the logical position by `count`.
    #[inline]
    pub fn append_repeated(&mut self, count: usize, value: bool) {
        self.builder.append_n(count, value);
        self.logical_position += count as u64;
    }

    /// Randomly samples a contiguous chunk of bits from the buffer, up to the specified bit count,
    /// and returns it as an Arrow [`UInt8Array`].
    ///
    /// The sample is a random slice of bytes (not bits), and the number of sampled bytes is
    /// `min(bit_count / 8, self.len() / 8)`. If there are not enough bits to form a full byte,
    /// returns an empty array.
    ///
    /// # Arguments
    ///
    /// * `bit_count` - The maximum number of bits to sample (rounded down to the nearest byte).
    ///
    /// # Returns
    ///
    /// An [`UInt8Array`] containing the sampled bytes.
    pub fn sample(&self, bit_count: usize) -> UInt8Array {
        let byte_count = std::cmp::min(bit_count / 8, self.len() / 8);
        if byte_count == 0 {
            return UInt8Array::from(Vec::<u8>::new());
        }
        let start = fastrand::usize(0..=self.len() / 8 - byte_count);
        let end = start + byte_count;
        UInt8Array::from(self.builder.as_slice()[start..end].to_vec())
    }

    /// Dequeues the buffer in fixed-size byte chunks, returning each chunk as an Arrow [`UInt8Array`].
    ///
    /// The buffer is consumed in-place. Any remaining bits that do not form a full chunk are
    /// preserved in the buffer for future appends or dequeues.
    ///
    /// # Arguments
    ///
    /// * `chunk_size` - The number of bytes per chunk. Must be non-zero.
    ///
    /// # Returns
    ///
    /// A vector of [`UInt8Array`]s, each of length `chunk_size`.
    ///
    /// # Panics
    ///
    /// Panics if `chunk_size` is zero.
    pub fn dequeue_full_byte_chunks(&mut self, chunk_size: usize) -> Vec<UInt8Array> {
        assert_ne!(chunk_size, 0, "chunk_size must be non-zero");
        if self.len() < chunk_size * 8 {
            return Vec::new();
        }

        let mut remaining_bits = self.builder.len();
        let buf = self.builder.finish().into_inner();
        let buf_len = buf.len();
        assert_eq!(buf_len, (remaining_bits + 7) / 8);
        self.builder.reserve(Self::DEFAULT_LEN_THRESHOLD);

        let mut array = UInt8Array::new(ScalarBuffer::new(buf, 0, buf_len), None);
        let mut chunks = Vec::with_capacity(buf_len / chunk_size);

        while remaining_bits >= chunk_size * 8 {
            let chunk = array.slice(0, chunk_size);
            chunks.push(chunk);
            array = array.slice(chunk_size, array.len() - chunk_size);
            remaining_bits -= chunk_size * 8;
            self.logical_offset += (chunk_size * 8) as u64;
        }

        // Preserve any remaining bits that do not form a full chunk.
        if remaining_bits != 0 {
            let buf = array.into_parts().1.into_inner();
            let bool_buf = BooleanBuffer::new(buf, 0, remaining_bits);
            self.builder.append_buffer(&bool_buf);
        }

        chunks
    }

    /// Dequeues and returns all remaining bits in the buffer as a single Arrow [`UInt8Array`].
    ///
    /// This method should generally be called only at the end of the encoding flow.
    /// The buffer is emptied by this operation.
    ///
    /// # Returns
    ///
    /// An [`UInt8Array`] containing all remaining bits as bytes.
    pub fn dequeue_remaining(&mut self) -> UInt8Array {
        self.logical_offset += self.builder.len() as u64;
        let buf = self.builder.finish().into_inner();
        let buf_len = buf.len();
        UInt8Array::new(ScalarBuffer::new(buf, 0, buf_len), None)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{builder::StringBuilder, cast::AsArray, BinaryArray, StringArray};
    use arrow_schema::DataType;

    use super::*;

    #[test]
    fn test_bit_staging_buffer_basics() {
        let mut buf = BitStagingBuffer::new();
        for _ in 0..40 {
            buf.append_value(true);
            buf.append_value(false);
        }
        buf.append_value(true);
        assert_eq!(buf.logical_position(), 81);
        assert_eq!(buf.logical_offset(), 0);

        let chunks = buf.dequeue_full_byte_chunks(5);
        assert_eq!(buf.logical_offset(), 80);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].values(), &[85, 85, 85, 85, 85]);
        let arr = buf.dequeue_remaining();
        assert_eq!(buf.logical_offset(), 81);
        assert_eq!(arr.values(), &[1]);
    }

    #[test]
    fn test_bit_staging_buffer_empty() {
        let buf = BitStagingBuffer::new();
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.logical_position(), 0);
        assert_eq!(buf.logical_offset(), 0);
        assert!(buf.is_empty());
        let sample = buf.sample(8);
        assert_eq!(sample.len(), 0);
    }

    #[test]
    fn test_bit_staging_buffer_append_value_and_repeated() {
        let mut buf = BitStagingBuffer::new();
        buf.append_value(true);
        buf.append_value(false);
        buf.append_repeated(6, true);
        buf.append_repeated(5, false);
        assert_eq!(buf.len(), 13);
        assert_eq!(buf.logical_position(), 13);
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_bit_staging_buffer_append_boolean_buffer() {
        let mut buf = BitStagingBuffer::new();
        let mut builder = BooleanBufferBuilder::new(8);
        for i in 0..8 {
            builder.append(i % 2 == 0);
        }
        let bool_buf = builder.finish();
        buf.append(&bool_buf);
        assert_eq!(buf.len(), 8);
        assert_eq!(buf.logical_position(), 8);
        let mut builder2 = BooleanBufferBuilder::new(4);
        for _ in 0..4 {
            builder2.append(true);
        }
        let bool_buf2 = builder2.finish();
        buf.append(&bool_buf2);
        assert_eq!(buf.len(), 12);
        assert_eq!(buf.logical_position(), 12);
    }

    #[test]
    fn test_bit_staging_buffer_sample_various_sizes() {
        let mut buf = BitStagingBuffer::new();
        for _ in 0..16 {
            buf.append_value(true);
        }
        let sample = buf.sample(8);
        assert_eq!(sample.len(), 1);
        assert_eq!(sample.value(0), 255);
        let sample2 = buf.sample(16);
        assert_eq!(sample2.len(), 2);
        for i in 0..sample2.len() {
            assert_eq!(sample2.value(i), 255);
        }
        let sample3 = buf.sample(7);
        assert_eq!(sample3.len(), 0);
    }

    #[test]
    fn test_bit_staging_buffer_sample_more_than_available() {
        let mut buf = BitStagingBuffer::new();
        for _ in 0..10 {
            buf.append_value(true);
        }
        let sample = buf.sample(32);
        assert_eq!(sample.len(), 1);
        assert_eq!(sample.value(0), 255);
    }

    #[test]
    fn test_bit_staging_buffer_dequeue_full_byte_chunks_basic() {
        let mut buf = BitStagingBuffer::new();
        for _ in 0..16 {
            buf.append_value(true);
        }
        let chunks = buf.dequeue_full_byte_chunks(1);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].value(0), 255);
        assert_eq!(chunks[1].value(0), 255);
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.logical_offset(), 16);
    }

    #[test]
    fn test_bit_staging_buffer_dequeue_full_byte_chunks_partial() {
        let mut buf = BitStagingBuffer::new();
        for _ in 0..10 {
            buf.append_value(true);
        }
        let chunks = buf.dequeue_full_byte_chunks(1);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].value(0), 255);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.logical_offset(), 8);
    }

    #[test]
    fn test_bit_staging_buffer_dequeue_full_byte_chunks_not_enough() {
        let mut buf = BitStagingBuffer::new();
        for _ in 0..7 {
            buf.append_value(true);
        }
        let chunks = buf.dequeue_full_byte_chunks(1);
        assert_eq!(chunks.len(), 0);
        assert_eq!(buf.len(), 7);
        assert_eq!(buf.logical_offset(), 0);
    }

    #[test]
    #[should_panic]
    fn test_bit_staging_buffer_dequeue_full_byte_chunks_zero_chunk() {
        let mut buf = BitStagingBuffer::new();
        buf.append_value(true);
        buf.dequeue_full_byte_chunks(0);
    }

    #[test]
    fn test_bit_staging_buffer_dequeue_remaining() {
        let mut buf = BitStagingBuffer::new();
        for _ in 0..10 {
            buf.append_value(true);
        }
        let arr = buf.dequeue_remaining();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr.value(0), 255);
        assert_eq!(arr.value(1), 3);
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.logical_offset(), 10);
        let arr2 = buf.dequeue_remaining();
        assert_eq!(arr2.len(), 0);
        assert_eq!(buf.logical_offset(), 10);
    }

    #[test]
    fn test_bit_staging_buffer_append_and_dequeue_interleaved() {
        let mut buf = BitStagingBuffer::new();
        buf.append_repeated(8, true);
        let chunks = buf.dequeue_full_byte_chunks(1);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].value(0), 255);
        buf.append_repeated(4, false);
        buf.append_repeated(4, true);
        let arr = buf.dequeue_remaining();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr.value(0), 240);
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.logical_offset(), 16);
    }

    #[test]
    fn test_bit_staging_buffer_state_tracking() {
        let mut buf = BitStagingBuffer::new();
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.logical_position(), 0);
        assert_eq!(buf.logical_offset(), 0);
        assert!(buf.is_empty());
        buf.append_value(true);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.logical_position(), 1);
        assert_eq!(buf.logical_offset(), 0);
        assert!(!buf.is_empty());
        buf.dequeue_remaining();
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.logical_offset(), 1);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_bit_staging_buffer_append_zero_bits() {
        let mut buf = BitStagingBuffer::new();
        buf.append_repeated(0, true);
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.logical_position(), 0);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_bit_staging_buffer_random_patterns() {
        let mut buf = BitStagingBuffer::new();
        let mut pattern = Vec::new();
        for _ in 0..32 {
            let v = fastrand::bool();
            buf.append_value(v);
            pattern.push(v);
        }
        let arr = buf.dequeue_remaining();
        let mut bits = Vec::new();
        for byte in arr.values() {
            for i in 0..8 {
                bits.push((byte >> i) & 1 == 1);
            }
        }
        for (i, &v) in pattern.iter().enumerate() {
            assert_eq!(bits[i], v);
        }
    }

    #[test]
    fn test_append_binary() {
        let mut buffer = BytesStagingBuffer::new(DataType::Binary);
        let array1 = Arc::new(BinaryArray::from(vec![
            Some(b"hello".as_ref()),
            Some(b"world"),
        ])) as Arc<dyn Array>;
        let array2 = Arc::new(BinaryArray::from(vec![Some(b"foo".as_ref()), Some(b"bar")]))
            as Arc<dyn Array>;

        buffer.append(array1.clone());
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.data_size(), 10);
        assert!(!buffer.is_empty());

        buffer.append(array2.clone());
        assert_eq!(buffer.len(), 4);
        assert_eq!(buffer.data_size(), 16);
    }

    #[test]
    fn test_dequeue_basic() {
        let mut buffer = BytesStagingBuffer::new(DataType::Utf8);
        let array = Arc::new(StringArray::from(vec![
            Some("a"),
            Some("bb"),
            Some("ccc"),
            Some("dddd"),
        ])) as Arc<dyn Array>;
        buffer.append(array);

        let dequeued = buffer.dequeue(2).unwrap();
        let dequeued_str = dequeued.as_string::<i32>();
        assert_eq!(dequeued_str.len(), 2);
        assert_eq!(dequeued_str.value(0), "a");
        assert_eq!(dequeued_str.value(1), "bb");
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.data_size(), 7);
        assert_eq!(buffer.logical_offset(), 2);
    }

    #[test]
    fn test_dequeue_all() {
        let mut buffer = BytesStagingBuffer::new(DataType::Utf8);
        let array =
            Arc::new(StringArray::from(vec![Some("a"), Some("bb"), Some("ccc")])) as Arc<dyn Array>;
        buffer.append(array);

        let dequeued = buffer.dequeue(3).unwrap();
        let dequeued_str = dequeued.as_string::<i32>();
        assert_eq!(dequeued_str.len(), 3);
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.data_size(), 0);
        assert_eq!(buffer.logical_offset(), 3);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_dequeue_more_than_available() {
        let mut buffer = BytesStagingBuffer::new(DataType::LargeUtf8);
        let array = Arc::new(StringArray::from(vec![Some("a"), Some("bb")])) as Arc<dyn Array>;
        buffer.append(array);

        let dequeued = buffer.dequeue(5).unwrap();
        let dequeued_str = dequeued.as_string::<i64>();
        assert_eq!(dequeued_str.len(), 2);
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.data_size(), 0);
        assert_eq!(buffer.logical_offset(), 2);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_dequeue_empty() {
        let mut buffer = BytesStagingBuffer::new(DataType::Utf8);
        let result = buffer.dequeue(10).unwrap();
        assert_eq!(result.len(), 0);
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.data_size(), 0);
        assert_eq!(buffer.logical_offset(), 0);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_dequeue_multiple_arrays() {
        let mut buffer = BytesStagingBuffer::new(DataType::LargeUtf8);
        let array1 = Arc::new(StringArray::from(vec![Some("a"), Some("bb")])) as Arc<dyn Array>;
        let array2 = Arc::new(StringArray::from(vec![Some("ccc"), Some("dddd")])) as Arc<dyn Array>;
        buffer.append(array1);
        buffer.append(array2);

        let dequeued = buffer.dequeue(3).unwrap();
        let dequeued_str = dequeued.as_string::<i64>();
        assert_eq!(dequeued_str.len(), 3);
        assert_eq!(dequeued_str.value(0), "a");
        assert_eq!(dequeued_str.value(1), "bb");
        assert_eq!(dequeued_str.value(2), "ccc");
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.data_size(), 4);
        assert_eq!(buffer.logical_offset(), 3);
    }

    #[test]
    fn test_dequeue_at_most_basic() {
        let mut buffer = BytesStagingBuffer::new(DataType::LargeUtf8);
        let array = Arc::new(StringArray::from(vec![
            Some("a"),
            Some("bb"),
            Some("ccc"),
            Some("dddd"),
        ])) as Arc<dyn Array>;
        buffer.append(array);

        let dequeued = buffer.dequeue_at_most(2, 100).unwrap();
        let dequeued_str = dequeued.as_string::<i64>();
        assert_eq!(dequeued_str.len(), 2);
        assert_eq!(dequeued_str.value(0), "a");
        assert_eq!(dequeued_str.value(1), "bb");
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.data_size(), 7);
        assert_eq!(buffer.logical_offset(), 2);
    }

    #[test]
    fn test_dequeue_at_most_max_data_size() {
        let mut buffer = BytesStagingBuffer::new(DataType::LargeUtf8);
        let array = Arc::new(StringArray::from(vec![
            Some("a"),
            Some("bb"),
            Some("ccc"),
            Some("dddd"),
        ])) as Arc<dyn Array>;
        buffer.append(array);

        let dequeued = buffer.dequeue_at_most(10, 5).unwrap();
        let dequeued_str = dequeued.as_string::<i64>();
        assert!(dequeued_str.len() <= 4);
        let dequeued_data_size: usize = dequeued_str.iter().map(|x| x.map_or(0, str::len)).sum();
        assert!(dequeued_data_size <= 5);
        assert_eq!(buffer.len(), 4 - dequeued_str.len());
    }

    #[test]
    fn test_dequeue_at_most_count_limit() {
        let mut buffer = BytesStagingBuffer::new(DataType::LargeUtf8);
        let array = Arc::new(StringArray::from(vec![
            Some("a"),
            Some("bb"),
            Some("ccc"),
            Some("dddd"),
        ])) as Arc<dyn Array>;
        buffer.append(array);

        let dequeued = buffer.dequeue_at_most(2, 100).unwrap();
        let dequeued_str = dequeued.as_string::<i64>();
        assert_eq!(dequeued_str.len(), 2);
        assert_eq!(buffer.len(), 2);
    }

    #[test]
    fn test_dequeue_at_most_empty() {
        let mut buffer = BytesStagingBuffer::new(DataType::Utf8);
        let result = buffer.dequeue_at_most(10, 100).unwrap();
        assert_eq!(result.len(), 0);
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.data_size(), 0);
        assert_eq!(buffer.logical_offset(), 0);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_count_elements_up_to_size() {
        let mut buffer = BytesStagingBuffer::new(DataType::Utf8);
        let array = Arc::new(StringArray::from(vec![
            Some("a"),
            Some("bb"),
            Some("ccc"),
            Some("dddd"),
        ])) as Arc<dyn Array>;
        buffer.append(array);

        let count = buffer.count_elements_up_to_size(2, 100);
        assert_eq!(count, 2);

        let count = buffer.count_elements_up_to_size(10, 5);
        assert_eq!(count, 2);

        let count = buffer.count_elements_up_to_size(10, 100);
        assert_eq!(count, 4);
    }

    #[test]
    fn test_count_elements_up_to_size_empty() {
        let buffer = BytesStagingBuffer::new(DataType::Utf8);
        let count = buffer.count_elements_up_to_size(10, 100);
        assert_eq!(count, 1);
    }

    #[test]
    fn test_count_elements_up_to_size_multiple_arrays() {
        let mut buffer = BytesStagingBuffer::new(DataType::Utf8);
        let array1 = Arc::new(StringArray::from(vec![Some("a"), Some("bb")])) as Arc<dyn Array>;
        let array2 = Arc::new(StringArray::from(vec![Some("ccc"), Some("dddd")])) as Arc<dyn Array>;
        buffer.append(array1);
        buffer.append(array2);

        let count = buffer.count_elements_up_to_size(3, 100);
        assert_eq!(count, 3);

        let count = buffer.count_elements_up_to_size(10, 5);
        assert_eq!(count, 2);
    }

    #[test]
    fn test_dequeue_normalization() {
        let mut buffer = BytesStagingBuffer::new(DataType::Utf8);
        let array = Arc::new(StringArray::from(vec![
            Some("a"),
            Some("bb"),
            Some("ccc"),
            Some("dddd"),
        ])) as Arc<dyn Array>;
        buffer.append(array);

        // Create a bad array sequence that will cause an error when flattening
        let binary_array =
            Arc::new(BinaryArray::from(vec![Some(b"hello".as_ref())])) as Arc<dyn Array>;
        buffer.append(binary_array);

        let result = buffer.dequeue(5).unwrap();
        let result = result.as_string::<i32>();
        assert_eq!(result.value(1), "bb");
        assert!(!result.is_null(4));
        assert_eq!(result.value(4), "hello");
    }

    #[test]
    fn test_dequeue_normalization_with_nulls() {
        let mut buffer = BytesStagingBuffer::new(DataType::Utf8);
        let array = Arc::new(StringArray::from(vec![
            Some("a"),
            Some("bb"),
            Some("ccc"),
            Some("dddd"),
        ])) as Arc<dyn Array>;
        buffer.append(array);

        // Create a bad array sequence that will cause an error when flattening
        let binary_array = Arc::new(BinaryArray::from(vec![
            Some([200u8, 210u8, 220u8, 230u8, 240u8, 250u8].as_ref()),
            Some(b"hello".as_ref()),
        ])) as Arc<dyn Array>;
        buffer.append(binary_array);

        let result = buffer.dequeue(10).unwrap();
        let result = result.as_string::<i32>();
        assert_eq!(result.len(), 6);
        assert_eq!(result.value(1), "bb");
        assert!(result.is_null(4));
        assert_eq!(result.value(4), "");
    }

    #[test]
    fn test_bytes_buffer_sample() {
        let mut buffer = BytesStagingBuffer::new(DataType::LargeUtf8);
        for arr_idx in 0..80 {
            let mut builder = StringBuilder::new();
            for i in 0..(1000 + arr_idx * 10) {
                if (i + 5) % 10 == 0 {
                    builder.append_null();
                } else {
                    let value = format!("value_{arr_idx}_{i}");
                    builder.append_value(&value);
                }
            }
            let arr = builder.finish();
            buffer.append(Arc::new(arr));
        }

        let sample = buffer.sample(2000, 10000).unwrap();
        assert!(sample.len() < 2000);
        assert!(sample.len() > 500);
        let sample = sample.as_string::<i64>();
        let null_count = sample.iter().filter(std::option::Option::is_none).count();
        assert!(null_count > sample.len() / 20);
        assert!(null_count < sample.len() / 5);
    }
}
