//! List of arrays (chunks) that can to be processed as a continuous sequence.

use std::{collections::VecDeque, ops::Range, sync::Arc};

use arrow_array::Array;
use arrow_cast::CastOptions;
use arrow_schema::{ArrowError, DataType};

/// A sequence of Arrow arrays, allowing for efficient appending and slicing.
///
/// This struct maintains a queue of `Arc<dyn Array>` and tracks the total length
/// of all arrays. It provides methods for appending new arrays, dequeuing a
/// specified number of elements, and creating slices of the sequence.
///
/// The `ArraySequence` is designed to handle cases where data is received in
/// chunks (arrays) and needs to be processed as a continuous sequence.
pub struct ArraySequence {
    arrays: VecDeque<Arc<dyn Array>>,
    len: usize,
}

impl Default for ArraySequence {
    /// Creates a new empty `ArraySequence`.
    fn default() -> Self {
        ArraySequence::new()
    }
}

impl ArraySequence {
    /// Creates a new empty `ArraySequence`.
    pub fn new() -> Self {
        ArraySequence {
            arrays: VecDeque::new(),
            len: 0,
        }
    }

    /// Appends a new Arrow array to the end of the sequence.
    ///
    /// The length of the sequence is updated to reflect the added array.
    ///
    /// # Arguments
    ///
    /// * `array`: An `Arc` containing the Arrow array to append.
    pub fn append(&mut self, array: Arc<dyn Array>) {
        self.len += array.len();
        self.arrays.push_back(array);
    }

    /// Dequeues a specified number of logical values (value slots) from the
    /// beginning of the sequence.
    ///
    /// Returns a new `ArraySequence` containing the dequeued elements. If the
    /// requested length exceeds the total length of the sequence, only the
    /// available elements are dequeued.
    ///
    /// # Arguments
    ///
    /// * `len`: The number of logical values (value slots) to dequeue.
    ///
    /// # Returns
    ///
    /// A `Result` containing the new `ArraySequence` with the dequeued elements,
    /// or an `ArrowError` if an error occurs.
    pub fn dequeue(&mut self, len: usize) -> ArraySequence {
        let len = len.min(self.len);
        let mut dequeued_arrays = VecDeque::new();
        let mut remaining_len = len;

        while remaining_len > 0 {
            let array = self
                .arrays
                .pop_front()
                .expect("array queue should not be empty (length verified)");
            let array_len = array.len();
            if array_len <= remaining_len {
                dequeued_arrays.push_back(array);
                remaining_len -= array_len;
            } else {
                let sliced_array = array.slice(0, remaining_len);
                let remaining_array = array.slice(remaining_len, array_len - remaining_len);
                dequeued_arrays.push_back(Arc::new(sliced_array));
                self.arrays.push_front(Arc::new(remaining_array));
                remaining_len = 0;
            }
        }

        self.len -= len;
        ArraySequence {
            arrays: dequeued_arrays,
            len,
        }
    }

    /// Creates a slice of the sequence, starting at a given offset and with a specified length.
    ///
    /// Returns a new `ArraySequence` containing the sliced elements.
    ///
    /// # Arguments
    ///
    /// * `offset`: The starting offset of the slice.
    /// * `len`: The length of the slice.
    ///
    /// # Returns
    ///
    /// A `Result` containing the new `ArraySequence` with the sliced elements,
    /// or an `ArrowError` if the slice is out of bounds.
    ///
    /// # Errors
    ///
    /// Returns an `ArrowError::InvalidArgumentError` if the requested slice is out of bounds.
    pub fn get_slice(&self, offset: usize, len: usize) -> Result<ArraySequence, ArrowError> {
        if offset + len > self.len {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Slice out of bounds: offset + len = {}, total length = {}",
                offset + len,
                self.len
            )));
        }

        let mut sliced_arrays = VecDeque::new();
        let mut remaining_offset = offset;
        let mut remaining_len = len;

        for array in &self.arrays {
            let array_len = array.len();
            if remaining_offset >= array_len {
                remaining_offset -= array_len;
                continue;
            }

            let slice_start = if remaining_offset > 0 {
                let start = remaining_offset;
                remaining_offset = 0;
                start
            } else {
                0
            };

            let slice_len = std::cmp::min(remaining_len, array_len - slice_start);
            let sliced_array = array.slice(slice_start, slice_len);
            sliced_arrays.push_back(sliced_array);
            remaining_len -= slice_len;

            if remaining_len == 0 {
                break;
            }
        }

        Ok(ArraySequence {
            arrays: sliced_arrays,
            len,
        })
    }

    /// Takes multiple non-overlapping slices from the sequence, returning a new `ArraySequence`.
    ///
    /// This function allows extracting multiple, non-overlapping ranges of elements from the
    /// `ArraySequence`. The provided ranges must be sorted in ascending order and must not
    /// overlap. If these conditions are not met, the function will panic.
    ///
    /// The function iterates through the underlying arrays and extracts the specified ranges,
    /// creating a new `ArraySequence` containing the sliced elements.
    ///
    /// # Arguments
    ///
    /// * ranges: A slice of `Range<usize>` representing the ranges to extract. Each range
    ///   specifies a start and end index within the overall sequence.
    ///
    /// # Panics
    ///
    /// This function will panic if:
    ///
    /// * The provided ranges are not sorted in ascending order.
    /// * The provided ranges overlap.
    /// * The start or end of a range is outside the bounds of the sequence.
    ///
    /// # Returns
    ///
    /// A new `ArraySequence` containing the elements from the specified ranges.
    pub fn take_ranges(&self, ranges: &[Range<usize>]) -> ArraySequence {
        if ranges.len() > 1 {
            for i in 0..ranges.len() - 1 {
                assert!(
                    ranges[i].end <= ranges[i + 1].start,
                    "Ranges must be non-overlapping and sorted ({:?} vs {:?})",
                    ranges[i],
                    ranges[i + 1],
                );
            }
        }

        let mut slices = VecDeque::new();
        let mut range_idx = 0;
        let mut current_offset = 0;
        let mut total_len = 0;

        for array in &self.arrays {
            let array_len = array.len();

            while range_idx < ranges.len() && ranges[range_idx].start < current_offset + array_len {
                let range = &ranges[range_idx];
                let slice_start = range.start.saturating_sub(current_offset);
                let slice_end = std::cmp::min(range.end, current_offset + array_len);
                let slice_len = slice_end - (current_offset + slice_start);

                assert!(
                    slice_start <= array_len,
                    "slice_start must be within array bounds"
                );
                assert!(
                    slice_len <= array_len - slice_start,
                    "slice_len must be within array bounds"
                );

                if slice_len > 0 {
                    let slice = array.slice(slice_start, slice_len);
                    slices.push_back(slice);
                    total_len += slice_len;
                }

                if range.end <= current_offset + array_len {
                    range_idx += 1;
                } else {
                    break;
                }
            }
            current_offset += array_len;
        }

        ArraySequence {
            arrays: slices,
            len: total_len,
        }
    }

    /// Samples a set of non-overlapping subranges from the sequence, returning a new `ArraySequence`.
    ///
    /// This function samples a number of subranges from the sequence, with each subrange
    /// having a random size between `min_range` and `max_range`. The total length of the sampled
    /// subranges is controlled by `sample_size`.
    ///
    /// # Arguments
    ///
    /// * `sample_size`: The total length of the sampled subranges.
    /// * `min_range`: The minimum size of each subrange.
    /// * `max_range`: The maximum size of each subrange.
    ///
    /// # Returns
    ///
    /// A new `ArraySequence` containing the elements from the sampled subranges.
    /// Returns an empty `ArraySequence` if the sequence is empty.
    pub fn sample_ranges(
        &self,
        sample_size: usize,
        min_range: usize,
        max_range: usize,
    ) -> ArraySequence {
        if self.is_empty() {
            return Default::default();
        }

        let ranges = sample_subranges(self.len, min_range, max_range, sample_size);
        self.take_ranges(&ranges)
    }

    /// Returns the total length of the sequence.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Checks if the sequence is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns an iterator over the arrays in the sequence.
    ///
    /// The iterator yields references to the `Arc<dyn Array>` elements in the
    /// order they were appended to the sequence.
    ///
    /// # Returns
    ///
    /// An iterator over the arrays in the sequence.
    pub fn arrays(&self) -> std::collections::vec_deque::Iter<'_, Arc<dyn Array>> {
        self.arrays.iter()
    }

    /// Returns an iterator over the arrays in the sequence along with their ranges.
    ///
    /// The iterator yields tuples of `(&Arc<dyn Array>, Range<usize>)`, where the
    /// `Range<usize>` represents the start and end indices of the array within
    /// the overall sequence.
    ///
    /// # Returns
    ///
    /// An iterator over the arrays and their corresponding ranges.
    pub fn arrays_with_ranges(&self) -> impl Iterator<Item = (&Arc<dyn Array>, Range<usize>)> {
        self.arrays.iter().scan(0usize, |pos, array| {
            let start = *pos;
            let end = start + array.len();
            *pos = end;
            Some((array, start..end))
        })
    }

    /// Flattens the sequence into a single Arrow array of the specified type.
    ///
    /// This method concatenates all arrays in the sequence into a single array.
    /// If the sequence contains multiple arrays, they are cast to the specified
    /// `to_type` before concatenation. If the sequence contains only one array
    /// and its type matches `to_type`, it is returned directly without casting.
    ///
    /// # Arguments
    ///
    /// * `to_type`: The desired `DataType` of the flattened array.
    ///
    /// # Returns
    ///
    /// A `Result` containing the flattened `Arc<dyn Array>`, or an `ArrowError`
    /// if an error occurs during casting or concatenation.
    pub fn flatten(&self, to_type: &DataType) -> Result<Arc<dyn Array>, ArrowError> {
        if self.is_empty() {
            return Ok(arrow_array::new_empty_array(to_type));
        }
        if self.arrays.len() == 1 && self.arrays[0].data_type() == to_type {
            return Ok(self.arrays[0].clone());
        }
        let arrays = self
            .arrays
            .iter()
            .map(|array| {
                arrow_cast::cast_with_options(
                    array,
                    to_type,
                    &CastOptions {
                        safe: true,
                        format_options: Default::default(),
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        if arrays.len() == 1 {
            return Ok(arrays[0].clone());
        }
        arrow_select::concat::concat(&arrays.iter().map(Arc::as_ref).collect::<Vec<_>>())
    }
}

/// Generates a vector of non-overlapping subranges within a given length, with random sizes and gaps.
///
/// This function creates a series of subranges within a larger range of length `len`. The subranges
/// are generated with random sizes, constrained by `min_size` and `max_size`, except potentially the
/// last one, and are separated by random gaps. The total length of the subranges is controlled by
/// `sample_len`.
///
/// # Parameters
///
/// - `len`: The total length of the range within which subranges will be generated.
/// - `min_size`: The minimum allowable size for each subrange.
/// - `max_size`: The maximum allowable size for each subrange.
/// - `sample_len`: The total length of all subranges combined. This value must be less than or equal
///   to `len`.
///
/// # Returns
///
/// A `Vec<Range<usize>>` representing the generated subranges. Each range is defined by a start and
/// end index within the original length `len`. The subranges are non-overlapping, their combined
/// length is equal to `sample_len`, and they are ordered in ascending order based on their start indices.
pub fn sample_subranges(
    len: usize,
    min_size: usize,
    max_size: usize,
    sample_len: usize,
) -> Vec<Range<usize>> {
    if len == 0 {
        return Vec::new();
    }
    if sample_len >= len {
        return vec![Range { start: 0, end: len }];
    }

    let sizes = random_split(sample_len, min_size, max_size);
    let total_gap = len - sample_len;
    let avg_gap = total_gap / (sizes.len() + 1);
    let min_gap = (avg_gap / 2).max(1);
    let max_gap = (avg_gap * 3 / 2).max(min_gap);
    let gaps = random_split(total_gap, min_gap, max_gap);
    let mut ranges = Vec::with_capacity(sizes.len());
    let mut start = 0;
    for (i, size) in sizes.into_iter().enumerate() {
        if i < gaps.len() {
            start += gaps[i];
        }
        let end = start + size;
        ranges.push(start..end);
        start = end;
    }
    ranges
}

/// Splits a specified length into random-sized chunks within defined bounds.
///
/// The `random_split` function divides a total length into a vector of randomly sized chunks.
/// Each chunk size, except potentially the last one, adheres to specified minimum and maximum
/// size constraints. The function guarantees that the sum of all chunk sizes equals the original
/// length.
///
/// # Parameters
///
/// - `len`: The total length to be divided. If `len` is zero, the function returns an empty vector.
/// - `min_size`: The minimum allowable size for each chunk. It must be less than or equal to `max_size`.
/// - `max_size`: The maximum allowable size for each chunk. It must be greater than or equal to `min_size`.
///
/// # Returns
///
/// A `Vec<usize>` representing the sizes of the chunks. Each size falls within the specified
/// `min_size..=max_size` range, with the possible exception of the last chunk. The sum of the
/// elements in the vector will equal `len`. If `len` is zero, an empty vector is returned.
/// If `min_size` is greater than or equal to `len`, the function returns a single-element vector
/// containing `len`.
///
/// # Panics
///
/// This function will panic if `min_size` is greater than `max_size`.
pub fn random_split(len: usize, min_size: usize, max_size: usize) -> Vec<usize> {
    if len == 0 {
        return Vec::new();
    }
    assert!(
        min_size <= max_size,
        "min_size must be less than or equal to max_size"
    );
    if min_size >= len {
        return vec![len];
    }

    let mut remaining_len = len;
    let mut result = Vec::new();
    while remaining_len > 0 {
        let size = if remaining_len < max_size {
            remaining_len
        } else {
            fastrand::usize(min_size..=max_size)
        };
        result.push(size);
        remaining_len -= size;
    }
    assert_eq!(result.iter().sum::<usize>(), len);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn test_flatten_empty() {
        let seq = ArraySequence::new();
        let result = seq.flatten(&DataType::Int32).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_flatten_single_array_same_type() {
        let mut seq = ArraySequence::new();
        let array = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        seq.append(array.clone());
        let result = seq.flatten(&DataType::Int32).unwrap();
        assert_eq!(result.len(), 3);
        let result_array = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result_array.value(0), 1);
        assert_eq!(result_array.value(1), 2);
        assert_eq!(result_array.value(2), 3);
        // Check that the same array is returned
        assert!(Arc::ptr_eq(&array, &result));
    }

    #[test]
    fn test_flatten_single_array_different_type() {
        let mut seq = ArraySequence::new();
        let array = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        seq.append(array);
        let result = seq.flatten(&DataType::Utf8).unwrap();
        assert_eq!(result.len(), 3);
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "1");
        assert_eq!(result_array.value(1), "2");
        assert_eq!(result_array.value(2), "3");
    }

    #[test]
    fn test_flatten_multiple_arrays_same_type() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let array2 = Arc::new(Int32Array::from(vec![4, 5])) as Arc<dyn Array>;
        seq.append(array1);
        seq.append(array2);
        let result = seq.flatten(&DataType::Int32).unwrap();
        assert_eq!(result.len(), 5);
        let result_array = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result_array.value(0), 1);
        assert_eq!(result_array.value(1), 2);
        assert_eq!(result_array.value(2), 3);
        assert_eq!(result_array.value(3), 4);
        assert_eq!(result_array.value(4), 5);
    }

    #[test]
    fn test_flatten_multiple_arrays_different_type() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let array2 = Arc::new(Int32Array::from(vec![4, 5])) as Arc<dyn Array>;
        seq.append(array1);
        seq.append(array2);
        let result = seq.flatten(&DataType::Utf8).unwrap();
        assert_eq!(result.len(), 5);
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "1");
        assert_eq!(result_array.value(1), "2");
        assert_eq!(result_array.value(2), "3");
        assert_eq!(result_array.value(3), "4");
        assert_eq!(result_array.value(4), "5");
    }

    #[test]
    fn test_flatten_multiple_arrays_mixed_types() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let array2 =
            Arc::new(StringArray::from(vec![Some("4"), None, Some("6")])) as Arc<dyn Array>;
        seq.append(array1);
        seq.append(array2);
        let result = seq.flatten(&DataType::Utf8).unwrap();
        assert_eq!(result.len(), 6);
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "1");
        assert_eq!(result_array.value(1), "2");
        assert_eq!(result_array.value(2), "3");
        assert_eq!(result_array.value(3), "4");
        assert_eq!(result_array.value(4), "");
        assert_eq!(result_array.value(5), "6");
        let valid_idxs = result_array
            .nulls()
            .unwrap()
            .valid_indices()
            .collect::<Vec<usize>>();
        assert_eq!(valid_idxs, &[0, 1, 2, 3, 5]);
    }

    #[test]
    fn test_flatten_cast_error() {
        let mut seq = ArraySequence::new();
        let array = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        seq.append(array);
        let result = seq.flatten(&DataType::FixedSizeBinary(10));
        assert!(result.is_err());
    }

    #[test]
    fn test_flatten_after_dequeue() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let array2 = Arc::new(Int32Array::from(vec![4, 5, 6])) as Arc<dyn Array>;
        seq.append(array1);
        seq.append(array2);

        let dequeued = seq.dequeue(2);
        let result = seq.flatten(&DataType::Int32).unwrap();
        assert_eq!(result.len(), 4);
        let result_array = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result_array.value(0), 3);
        assert_eq!(result_array.value(1), 4);
        assert_eq!(result_array.value(2), 5);
        assert_eq!(result_array.value(3), 6);

        let dequeued_result = dequeued.flatten(&DataType::Int32).unwrap();
        assert_eq!(dequeued_result.len(), 2);
        let dequeued_result_array = dequeued_result
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(dequeued_result_array.value(0), 1);
        assert_eq!(dequeued_result_array.value(1), 2);
    }

    #[test]
    fn test_new() {
        let seq = ArraySequence::new();
        assert_eq!(seq.len(), 0);
        assert!(seq.arrays.is_empty());
    }

    #[test]
    fn test_default() {
        let seq = ArraySequence::default();
        assert_eq!(seq.len(), 0);
        assert!(seq.arrays.is_empty());
    }

    #[test]
    fn test_append() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let array2 = Arc::new(Int32Array::from(vec![4, 5])) as Arc<dyn Array>;

        seq.append(array1.clone());
        assert_eq!(seq.len(), 3);
        assert_eq!(seq.arrays.len(), 1);

        seq.append(array2.clone());
        assert_eq!(seq.len(), 5);
        assert_eq!(seq.arrays.len(), 2);

        let mut iter = seq.arrays();
        assert_eq!(iter.next().unwrap().len(), 3);
        assert_eq!(iter.next().unwrap().len(), 2);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_dequeue_less_than_total() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let array2 = Arc::new(Int32Array::from(vec![4, 5, 6, 7])) as Arc<dyn Array>;
        seq.append(array1);
        seq.append(array2);

        let dequeued = seq.dequeue(4);
        assert_eq!(dequeued.len(), 4);
        assert_eq!(seq.len(), 3);

        let mut iter = dequeued.arrays();
        assert_eq!(iter.next().unwrap().len(), 3);
        assert_eq!(iter.next().unwrap().len(), 1);
        assert!(iter.next().is_none());

        let mut iter = seq.arrays();
        assert_eq!(iter.next().unwrap().len(), 3);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_dequeue_equal_to_total() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let array2 = Arc::new(Int32Array::from(vec![4, 5])) as Arc<dyn Array>;
        seq.append(array1);
        seq.append(array2);

        let dequeued = seq.dequeue(5);
        assert_eq!(dequeued.len(), 5);
        assert_eq!(seq.len(), 0);
        assert!(seq.arrays.is_empty());

        let mut iter = dequeued.arrays();
        assert_eq!(iter.next().unwrap().len(), 3);
        assert_eq!(iter.next().unwrap().len(), 2);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_dequeue_more_than_total() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        seq.append(array1);

        let dequeued = seq.dequeue(10);
        assert_eq!(dequeued.len(), 3);
        assert_eq!(seq.len(), 0);
        assert!(seq.arrays.is_empty());

        let mut iter = dequeued.arrays();
        assert_eq!(iter.next().unwrap().len(), 3);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_dequeue_empty() {
        let mut seq = ArraySequence::new();
        let dequeued = seq.dequeue(5);
        assert_eq!(dequeued.len(), 0);
        assert_eq!(seq.len(), 0);
        assert!(dequeued.arrays.is_empty());
    }

    #[test]
    fn test_dequeue_with_slice() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as Arc<dyn Array>;
        seq.append(array1);

        let dequeued = seq.dequeue(3);
        assert_eq!(dequeued.len(), 3);
        assert_eq!(seq.len(), 2);

        let mut iter = dequeued.arrays();
        assert_eq!(iter.next().unwrap().len(), 3);
        assert!(iter.next().is_none());

        let mut iter = seq.arrays();
        assert_eq!(iter.next().unwrap().len(), 2);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_get_slice_within_bounds() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let array2 = Arc::new(Int32Array::from(vec![4, 5, 6, 7])) as Arc<dyn Array>;
        seq.append(array1);
        seq.append(array2);

        let slice = seq.get_slice(1, 4).unwrap();
        assert_eq!(slice.len(), 4);

        let mut iter = slice.arrays();
        assert_eq!(iter.next().unwrap().len(), 2);
        assert_eq!(iter.next().unwrap().len(), 2);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_get_slice_at_start() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let array2 = Arc::new(Int32Array::from(vec![4, 5, 6, 7])) as Arc<dyn Array>;
        seq.append(array1);
        seq.append(array2);

        let slice = seq.get_slice(0, 3).unwrap();
        assert_eq!(slice.len(), 3);

        let mut iter = slice.arrays();
        assert_eq!(iter.next().unwrap().len(), 3);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_get_slice_at_end() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let array2 = Arc::new(Int32Array::from(vec![4, 5, 6, 7])) as Arc<dyn Array>;
        seq.append(array1);
        seq.append(array2);

        let slice = seq.get_slice(3, 4).unwrap();
        assert_eq!(slice.len(), 4);

        let mut iter = slice.arrays();
        assert_eq!(iter.next().unwrap().len(), 4);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_get_slice_across_arrays() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let array2 = Arc::new(Int32Array::from(vec![4, 5, 6, 7])) as Arc<dyn Array>;
        seq.append(array1);
        seq.append(array2);

        let slice = seq.get_slice(2, 3).unwrap();
        assert_eq!(slice.len(), 3);

        let mut iter = slice.arrays();
        assert_eq!(iter.next().unwrap().len(), 1);
        assert_eq!(iter.next().unwrap().len(), 2);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_get_slice_out_of_bounds() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        seq.append(array1);

        let result = seq.get_slice(2, 3);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_slice_empty() {
        let seq = ArraySequence::new();
        let result = seq.get_slice(0, 0).unwrap();
        assert_eq!(result.len(), 0);
        assert!(result.arrays.is_empty());
    }

    #[test]
    fn test_len() {
        let mut seq = ArraySequence::new();
        assert_eq!(seq.len(), 0);

        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        seq.append(array1);
        assert_eq!(seq.len(), 3);

        let array2 = Arc::new(Int32Array::from(vec![4, 5])) as Arc<dyn Array>;
        seq.append(array2);
        assert_eq!(seq.len(), 5);
    }

    #[test]
    fn test_is_empty() {
        let mut seq = ArraySequence::new();
        assert!(seq.is_empty());

        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        seq.append(array1);
        assert!(!seq.is_empty());
    }

    #[test]
    fn test_arrays_iter() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let array2 = Arc::new(StringArray::from(vec!["a", "b"])) as Arc<dyn Array>;
        seq.append(array1.clone());
        seq.append(array2.clone());

        let mut iter = seq.arrays();
        assert_eq!(iter.next().unwrap().as_ref(), array1.as_ref());
        assert_eq!(iter.next().unwrap().as_ref(), array2.as_ref());
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_take_ranges() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let array2 = Arc::new(Int32Array::from(vec![4, 5, 6, 7]));
        let array3 = Arc::new(Int32Array::from(vec![8, 9]));
        seq.append(array1);
        seq.append(array2);
        seq.append(array3);

        let ranges = vec![0..2, 4..6, 7..9];
        let sliced_seq = seq.take_ranges(&ranges);

        assert_eq!(sliced_seq.len(), 6);
        let expected_values: Vec<i32> = vec![1, 2, 5, 6, 8, 9];
        let mut actual_values: Vec<i32> = Vec::new();
        for array in sliced_seq.arrays() {
            let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..int_array.len() {
                actual_values.push(int_array.value(i));
            }
        }
        assert_eq!(actual_values, expected_values);
    }

    #[test]
    fn test_take_ranges_large_span() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let array2 = Arc::new(Int32Array::from(vec![4, 5, 6, 7]));
        let array3 = Arc::new(Int32Array::from(vec![8, 9, 10, 11, 12]));
        let array4 = Arc::new(Int32Array::from(vec![13, 14, 15, 16, 17, 18, 19]));
        seq.append(array1);
        seq.append(array2);
        seq.append(array3);
        seq.append(array4);

        let ranges = vec![1..14, 15..16];
        let sliced_seq = seq.take_ranges(&ranges);

        assert_eq!(sliced_seq.len(), 14);
        let expected_values: Vec<i32> = vec![2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16];
        let mut actual_values: Vec<i32> = Vec::new();
        for array in sliced_seq.arrays() {
            let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..int_array.len() {
                actual_values.push(int_array.value(i));
            }
        }
        assert_eq!(actual_values, expected_values);
    }

    #[test]
    fn test_take_ranges_string() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let array2 = Arc::new(StringArray::from(vec!["d", "e", "f", "g"]));
        let array3 = Arc::new(StringArray::from(vec!["h", "i"]));
        seq.append(array1);
        seq.append(array2);
        seq.append(array3);

        let ranges = vec![0..2, 4..6, 7..9];
        let sliced_seq = seq.take_ranges(&ranges);

        assert_eq!(sliced_seq.len(), 6);
        let expected_values: Vec<&str> = vec!["a", "b", "e", "f", "h", "i"];
        let mut actual_values: Vec<&str> = Vec::new();
        for array in sliced_seq.arrays() {
            let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..string_array.len() {
                actual_values.push(string_array.value(i));
            }
        }
        assert_eq!(actual_values, expected_values);
    }

    #[test]
    fn test_take_ranges_empty() {
        let seq = ArraySequence::new();
        let ranges: Vec<Range<usize>> = vec![];
        let sliced_seq = seq.take_ranges(&ranges);
        assert_eq!(sliced_seq.len(), 0);
        assert!(sliced_seq.is_empty());
    }

    #[test]
    fn test_take_ranges_empty_ranges() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3]));
        seq.append(array1);
        let ranges: Vec<Range<usize>> = vec![];
        let sliced_seq = seq.take_ranges(&ranges);
        assert_eq!(sliced_seq.len(), 0);
        assert!(sliced_seq.is_empty());
    }

    #[test]
    fn test_take_ranges_single_range() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let array2 = Arc::new(Int32Array::from(vec![4, 5, 6, 7]));
        seq.append(array1);
        seq.append(array2);

        let ranges = vec![Range { start: 1, end: 5 }];
        let sliced_seq = seq.take_ranges(&ranges);

        assert_eq!(sliced_seq.len(), 4);
        let expected_values: Vec<i32> = vec![2, 3, 4, 5];
        let mut actual_values: Vec<i32> = Vec::new();
        for array in sliced_seq.arrays() {
            let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..int_array.len() {
                actual_values.push(int_array.value(i));
            }
        }
        assert_eq!(actual_values, expected_values);
    }

    #[test]
    fn test_take_ranges_range_at_end() {
        let mut seq = ArraySequence::new();
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let array2 = Arc::new(Int32Array::from(vec![4, 5, 6, 7]));
        seq.append(array1);
        seq.append(array2);

        let ranges = vec![Range { start: 4, end: 7 }];
        let sliced_seq = seq.take_ranges(&ranges);

        assert_eq!(sliced_seq.len(), 3);
        let expected_values: Vec<i32> = vec![5, 6, 7];
        let mut actual_values: Vec<i32> = Vec::new();
        for array in sliced_seq.arrays() {
            let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..int_array.len() {
                actual_values.push(int_array.value(i));
            }
        }
        assert_eq!(actual_values, expected_values);
    }

    #[test]
    fn test_random_split_empty() {
        let result = random_split(0, 1, 10);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_random_split_single_chunk() {
        let result = random_split(5, 10, 20);
        assert_eq!(result, vec![5]);
    }

    #[test]
    fn test_random_split_basic() {
        let len = 100;
        let min_size = 10;
        let max_size = 20;
        let result = random_split(len, min_size, max_size);
        let sum: usize = result.iter().sum();
        assert_eq!(sum, len);
        dbg!(&result);
        for &chunk in result.iter().take(result.len() - 1) {
            assert!(chunk >= min_size);
            assert!(chunk <= max_size);
        }
    }

    #[test]
    fn test_random_split_min_eq_max() {
        let len = 100;
        let min_size = 10;
        let max_size = 10;
        let result = random_split(len, min_size, max_size);
        let sum: usize = result.iter().sum();
        assert_eq!(sum, len);
        for &chunk in &result {
            assert_eq!(chunk, min_size);
        }
    }

    #[test]
    fn test_random_split_min_greater_than_len() {
        let result = random_split(5, 10, 20);
        assert_eq!(result, vec![5]);
    }

    #[test]
    #[should_panic]
    fn test_random_split_min_greater_than_max() {
        random_split(10, 20, 10);
    }

    #[test]
    fn test_random_split_small_len() {
        let len = 5;
        let min_size = 1;
        let max_size = 3;
        let result = random_split(len, min_size, max_size);
        let sum: usize = result.iter().sum();
        assert_eq!(sum, len);
        for &chunk in &result {
            assert!(chunk >= min_size);
            assert!(chunk <= max_size);
        }
    }

    #[test]
    fn test_sample_empty_range() {
        let ranges = sample_subranges(0, 1, 5, 3);
        assert_eq!(ranges.len(), 0);
    }

    #[test]
    fn test_sample_len_greater_than_len() {
        let ranges = sample_subranges(5, 1, 3, 7);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], 0..5);
    }

    #[test]
    fn test_sample_len_equal_to_len() {
        let ranges = sample_subranges(5, 1, 3, 5);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], 0..5);
    }

    #[test]
    fn test_sample_single_subrange() {
        let ranges = sample_subranges(10, 2, 4, 3);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].end - ranges[0].start, 3);
    }

    #[test]
    fn test_multiple_subranges() {
        let ranges = sample_subranges(20, 2, 5, 10);
        assert!(!ranges.is_empty());
        dbg!(&ranges);
        let total_len: usize = ranges.iter().map(|r| r.end - r.start).sum();
        assert_eq!(total_len, 10);
        for range in &ranges {
            assert!(range.end <= 20);
        }
        for i in 0..ranges.len() - 1 {
            assert!(ranges[i].end <= ranges[i + 1].start);
        }
    }

    #[test]
    fn test_min_size_equals_max_size() {
        let ranges = sample_subranges(20, 3, 3, 9);
        assert!(!ranges.is_empty());
        let total_len: usize = ranges.iter().map(|r| r.end - r.start).sum();
        assert_eq!(total_len, 9);
        for range in &ranges {
            assert_eq!(range.end - range.start, 3);
            assert!(range.end <= 20);
        }
        for i in 0..ranges.len() - 1 {
            assert!(ranges[i].end <= ranges[i + 1].start);
        }
    }

    #[test]
    fn test_min_size_close_to_max_size() {
        let ranges = sample_subranges(20, 4, 5, 10);
        assert!(!ranges.is_empty());
        let total_len: usize = ranges.iter().map(|r| r.end - r.start).sum();
        assert_eq!(total_len, 10);
        for range in ranges.iter().take(ranges.len() - 1) {
            assert!(range.end - range.start >= 4);
            assert!(range.end - range.start <= 5);
            assert!(range.end <= 20);
        }
        for i in 0..ranges.len() - 1 {
            assert!(ranges[i].end <= ranges[i + 1].start);
        }
    }

    #[test]
    fn test_large_len_and_sample_len() {
        let ranges = sample_subranges(1000, 10, 50, 500);
        assert!(!ranges.is_empty());
        let total_len: usize = ranges.iter().map(|r| r.end - r.start).sum();
        assert_eq!(total_len, 500);
        for range in &ranges {
            assert!(range.end <= 1000);
        }
        for i in 0..ranges.len() - 1 {
            assert!(ranges[i].end <= ranges[i + 1].start);
        }
    }

    #[test]
    fn test_sample_len_is_small() {
        let ranges = sample_subranges(100, 10, 20, 15);
        assert!(!ranges.is_empty());
        let total_len: usize = ranges.iter().map(|r| r.end - r.start).sum();
        assert_eq!(total_len, 15);
        for range in &ranges {
            assert!(range.end <= 100);
        }
        for i in 0..ranges.len() - 1 {
            assert!(ranges[i].end <= ranges[i + 1].start);
        }
    }

    #[test]
    fn test_no_gaps_possible() {
        let ranges = sample_subranges(10, 1, 1, 9);
        assert!(!ranges.is_empty());
        let total_len: usize = ranges.iter().map(|r| r.end - r.start).sum();
        assert_eq!(total_len, 9);
        for range in &ranges {
            assert!(range.end <= 10);
        }
        for i in 0..ranges.len() - 1 {
            assert!(ranges[i].end <= ranges[i + 1].start);
        }
    }

    #[test]
    fn test_large_min_size() {
        let ranges = sample_subranges(100, 30, 40, 70);
        assert!(!ranges.is_empty());
        let total_len: usize = ranges.iter().map(|r| r.end - r.start).sum();
        assert_eq!(total_len, 70);
        for range in &ranges {
            assert!(range.end <= 100);
        }
        for i in 0..ranges.len() - 1 {
            assert!(ranges[i].end <= ranges[i + 1].start);
        }
    }

    #[test]
    fn test_max_size_equals_len() {
        let ranges = sample_subranges(10, 1, 10, 5);
        assert!(!ranges.is_empty());
        let total_len: usize = ranges.iter().map(|r| r.end - r.start).sum();
        assert_eq!(total_len, 5);
        for range in &ranges {
            assert!(range.end <= 10);
        }
        for i in 0..ranges.len() - 1 {
            assert!(ranges[i].end <= ranges[i + 1].start);
        }
    }

    #[test]
    fn test_min_size_equals_sample_len() {
        let ranges = sample_subranges(10, 5, 10, 5);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].end - ranges[0].start, 5);
    }

    #[test]
    fn test_max_size_equals_sample_len() {
        let ranges = sample_subranges(10, 1, 5, 5);
        assert!(!ranges.is_empty());
        let total_len: usize = ranges.iter().map(|r| r.end - r.start).sum();
        assert_eq!(total_len, 5);
        for range in &ranges {
            assert!(range.end <= 10);
        }
        for i in 0..ranges.len() - 1 {
            assert!(ranges[i].end <= ranges[i + 1].start);
        }
    }

    #[test]
    fn test_min_size_greater_than_sample_len() {
        let ranges = sample_subranges(10, 6, 10, 5);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].end - ranges[0].start, 5);
    }

    #[test]
    fn test_sample_len_is_zero() {
        let ranges = sample_subranges(10, 1, 5, 0);
        assert_eq!(ranges.len(), 0);
    }
}
