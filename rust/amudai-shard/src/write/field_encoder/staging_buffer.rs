//! FIFO-like staging buffers for encoding the value blocks.

use std::sync::Arc;

use amudai_common::{error::Error, Result};
use arrow_array::Array;
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub fn logical_offset(&self) -> usize {
        self.logical_offset
    }

    /// Returns `true` if there are no accumulated elements.
    pub fn is_empty(&self) -> bool {
        self.arrays.is_empty()
    }

    /// Returns the total size of the underlying data in bytes.
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub fn dequeue(&mut self, count: usize) -> Result<Arc<dyn Array>> {
        let arrays = self.arrays.dequeue(count);
        self.logical_offset += arrays.len();
        arrays
            .flatten(&self.normalized_type)
            .map_err(|e| Error::arrow("flatten primitive array sequence", e))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{builder::StringBuilder, cast::AsArray, BinaryArray, StringArray};
    use arrow_schema::DataType;

    use super::*;

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
