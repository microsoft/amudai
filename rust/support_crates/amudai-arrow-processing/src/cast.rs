//! Array casting and conversion utilities.

use std::sync::Arc;

use arrow_array::{Array, cast::AsArray};
use arrow_cast::CastOptions;
use arrow_schema::{ArrowError, DataType};

/// Converts a binary-like Arrow array to a `LargeBinary` array.
///
/// This function provides a convenient way to standardize various binary-like arrays
/// (Binary, FixedSizeBinary, Utf8, LargeUtf8) to a common LargeBinary format.
///
/// # Arguments
///
/// * `array` - The source array to convert
///
/// # Returns
///
/// A new `LargeBinary` array containing the same data
///
/// # Errors
///
/// Returns an `ArrowError::CastError` if the input array's data type is not compatible
/// with binary conversion (must be one of: Binary, LargeBinary, FixedSizeBinary, Utf8,
/// or LargeUtf8).
pub fn binary_like_to_large_binary(array: &dyn Array) -> Result<Arc<dyn Array>, ArrowError> {
    let data_type = array.data_type();
    match data_type {
        DataType::LargeBinary => return Ok(Arc::new(array.as_binary::<i64>().clone())),
        DataType::Binary | DataType::FixedSizeBinary(_) | DataType::Utf8 | DataType::LargeUtf8 => {}
        _ => {
            return Err(ArrowError::CastError(format!(
                "binary_like_to_large_binary: unexpected data type {data_type}"
            )));
        }
    }
    arrow_cast::cast_with_options(
        array,
        &DataType::LargeBinary,
        &CastOptions {
            safe: true,
            format_options: Default::default(),
        },
    )
}

/// Optimizes an Arrow array by removing redundant null buffers.
///
/// This function analyzes the input array and removes the null buffer when it's
/// unnecessary - specifically when a null buffer is present but all values are
/// actually non-null. This optimization is important for downstream processing
/// (such as block encoding) that can be more efficient without redundant null
/// metadata.
///
/// In Apache Arrow, arrays can have a null buffer even when no values are actually
/// null. This can happen when arrays are created from iterators with `Option` types
/// or when arrays are sliced/filtered. While functionally equivalent, arrays with
/// unnecessary null buffers consume more memory and may be processed less efficiently.
///
/// # Arguments
///
/// * `array` - The Arrow array to normalize.
///
/// # Returns
///
/// Returns a `Result` containing the normalized array:
/// - **No change**: If the array has no null buffer, returns the original array unchanged
/// - **No change**: If the array has actual null values, returns the original array unchanged  
/// - **Optimized**: If the array has a null buffer but no actual null values, returns a new
///   array with identical data but without the null buffer
///
/// # Performance
///
/// - **Zero-copy optimization**: When no changes are needed, the original array is returned
/// - **Minimal allocation**: When optimization is needed, only the array metadata is rebuilt
pub fn normalize_null_buffer(array: Arc<dyn Array>) -> Result<Arc<dyn Array>, ArrowError> {
    // If the array already has no null buffer, return it as-is
    if array.nulls().is_none() {
        return Ok(array);
    }

    // If the array has nulls, check if all values are actually non-null
    if array.null_count() > 0 {
        // Array has actual null values, keep the null buffer
        return Ok(array);
    }

    // Array has a null buffer but no actual nulls - recreate without null buffer
    let data = array.to_data();
    let new_data = data
        .into_builder()
        .nulls(None) // Remove the null buffer
        .build()?;
    Ok(arrow_array::make_array(new_data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        BinaryArray, FixedSizeBinaryArray, LargeBinaryArray, LargeStringArray, StringArray,
    };

    #[test]
    fn test_binary_to_large_binary() {
        // Create a BinaryArray with some values and nulls
        let values = vec![Some(b"hello".as_ref()), None, Some(b"world".as_ref())];
        let array = BinaryArray::from(values);

        // Convert to LargeBinary
        let result = binary_like_to_large_binary(&array).unwrap();

        // Check that it was converted to a LargeBinaryArray
        let large_array = result.as_any().downcast_ref::<LargeBinaryArray>().unwrap();

        // Verify the contents were preserved
        assert_eq!(large_array.len(), 3);
        assert_eq!(large_array.value(0), b"hello");
        assert!(large_array.is_null(1));
        assert_eq!(large_array.value(2), b"world");
    }

    #[test]
    fn test_fixed_size_binary_to_large_binary() {
        // Create a FixedSizeBinaryArray with some values and nulls
        let values = vec![
            Some(b"12345".as_ref()),
            Some(b"67890".as_ref()),
            None,
            Some(b"abcde".as_ref()),
        ];
        let array = FixedSizeBinaryArray::from(values);

        // Convert to LargeBinary
        let result = binary_like_to_large_binary(&array).unwrap();

        // Check that it was converted to a LargeBinaryArray
        let large_array = result.as_any().downcast_ref::<LargeBinaryArray>().unwrap();

        // Verify the contents were preserved
        assert_eq!(large_array.len(), 4);
        assert_eq!(large_array.value(0), b"12345");
        assert_eq!(large_array.value(1), b"67890");
        assert!(large_array.is_null(2));
        assert_eq!(large_array.value(3), b"abcde");
    }

    #[test]
    fn test_utf8_to_large_binary() {
        // Create a StringArray with some values and nulls
        let values = vec![Some("hello"), None, Some("world")];
        let array = StringArray::from(values);

        // Convert to LargeBinary
        let result = binary_like_to_large_binary(&array).unwrap();

        // Check that it was converted to a LargeBinaryArray
        let large_array = result.as_any().downcast_ref::<LargeBinaryArray>().unwrap();

        // Verify the contents were preserved
        assert_eq!(large_array.len(), 3);
        assert_eq!(large_array.value(0), b"hello");
        assert!(large_array.is_null(1));
        assert_eq!(large_array.value(2), b"world");
    }

    #[test]
    fn test_large_utf8_to_large_binary() {
        // Create a LargeStringArray with some values and nulls
        let values = vec![
            Some("hello"),
            None,
            Some("world"),
            Some("with special chars: 😊"),
        ];
        let array = LargeStringArray::from(values);

        // Convert to LargeBinary
        let result = binary_like_to_large_binary(&array).unwrap();

        // Check that it was converted to a LargeBinaryArray
        let large_array = result.as_any().downcast_ref::<LargeBinaryArray>().unwrap();

        // Verify the contents were preserved
        assert_eq!(large_array.len(), 4);
        assert_eq!(large_array.value(0), b"hello");
        assert!(large_array.is_null(1));
        assert_eq!(large_array.value(2), b"world");
        assert_eq!(large_array.value(3), "with special chars: 😊".as_bytes());
    }

    #[test]
    fn test_large_binary_identity() {
        // Create a LargeBinaryArray
        let values = vec![
            Some(b"hello".as_ref()),
            None,
            Some(b"world".as_ref()),
            Some(b"test".as_ref()),
        ];
        let array = LargeBinaryArray::from(values);

        // Convert to LargeBinary (should be essentially a no-op)
        let result = binary_like_to_large_binary(&array).unwrap();

        // Check that it's still a LargeBinaryArray
        let large_array = result.as_any().downcast_ref::<LargeBinaryArray>().unwrap();

        // Verify the contents were preserved
        assert_eq!(large_array.len(), 4);
        assert_eq!(large_array.value(0), b"hello");
        assert!(large_array.is_null(1));
        assert_eq!(large_array.value(2), b"world");
        assert_eq!(large_array.value(3), b"test");
    }

    #[test]
    fn test_empty_arrays() {
        // Test with empty arrays of each supported type
        let binary_array = BinaryArray::from_vec(Vec::<&[u8]>::new());
        let large_binary = binary_like_to_large_binary(&binary_array).unwrap();
        assert_eq!(large_binary.len(), 0);

        let string_array = StringArray::from(Vec::<&str>::new());
        let large_binary = binary_like_to_large_binary(&string_array).unwrap();
        assert_eq!(large_binary.len(), 0);

        let large_string_array = LargeStringArray::from(Vec::<&str>::new());
        let large_binary = binary_like_to_large_binary(&large_string_array).unwrap();
        assert_eq!(large_binary.len(), 0);
    }

    #[test]
    fn test_large_binary_with_large_values() {
        // Create a larger value to test with
        let large_value = vec![b'a'; 1000000]; // 1MB of 'a's

        let values = vec![Some(b"small".as_ref()), Some(large_value.as_slice()), None];
        let array = BinaryArray::from(values);

        let result = binary_like_to_large_binary(&array).unwrap();
        let large_array = result.as_any().downcast_ref::<LargeBinaryArray>().unwrap();

        assert_eq!(large_array.len(), 3);
        assert_eq!(large_array.value(0), b"small");
        assert_eq!(large_array.value(1).len(), 1000000);
        assert!(large_array.value(1).iter().all(|&b| b == b'a'));
        assert!(large_array.is_null(2));
    }

    #[test]
    fn test_unsupported_type() {
        use arrow_array::Int32Array;

        // Try to convert an Int32Array, which should fail
        let array = Int32Array::from(vec![1, 2, 3]);
        let result = binary_like_to_large_binary(&array);

        assert!(result.is_err());
        if let Err(ArrowError::CastError(msg)) = result {
            assert!(msg.contains("unexpected data type Int32"));
        } else {
            panic!("Expected CastError, but got a different error or success");
        }
    }

    #[test]
    fn test_all_nulls() {
        // Test with arrays that are all nulls
        let values: Vec<Option<&[u8]>> = vec![None, None, None];

        // BinaryArray with all nulls
        let array = BinaryArray::from(values.clone());
        let result = binary_like_to_large_binary(&array).unwrap();
        let large_array = result.as_any().downcast_ref::<LargeBinaryArray>().unwrap();

        assert_eq!(large_array.len(), 3);
        assert!(large_array.is_null(0));
        assert!(large_array.is_null(1));
        assert!(large_array.is_null(2));

        // FixedSizeBinaryArray with all nulls
        let array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            values.iter().cloned(),
            5, // size of 5 bytes
        )
        .unwrap();
        let result = binary_like_to_large_binary(&array).unwrap();
        let large_array = result.as_any().downcast_ref::<LargeBinaryArray>().unwrap();

        assert_eq!(large_array.len(), 3);
        assert!(large_array.is_null(0));
        assert!(large_array.is_null(1));
        assert!(large_array.is_null(2));
    }
    #[test]
    fn test_zero_length_values() {
        // Test with empty strings/binary values
        let values = vec![
            Some(b"".as_ref()),
            Some(b"non-empty".as_ref()),
            Some(b"".as_ref()),
        ];
        let array = BinaryArray::from(values);

        let result = binary_like_to_large_binary(&array).unwrap();
        let large_array = result.as_any().downcast_ref::<LargeBinaryArray>().unwrap();

        assert_eq!(large_array.len(), 3);
        assert_eq!(large_array.value(0), b"");
        assert_eq!(large_array.value(1), b"non-empty");
        assert_eq!(large_array.value(2), b"");
    }
    #[test]
    fn test_optimize_null_buffer_no_nulls() {
        use arrow_array::Int32Array;

        // Create an array with no null buffer
        let array_no_nulls = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let result = normalize_null_buffer(array_no_nulls.clone()).unwrap();

        // Should return the same array since it already has no null buffer
        assert!(Arc::ptr_eq(&array_no_nulls, &result));
        assert!(result.nulls().is_none());
        assert_eq!(result.null_count(), 0);

        // Create an array with all valid values but with a null buffer
        let values = vec![Some(1), Some(2), Some(3)];
        let array_with_null_buffer = Arc::new(Int32Array::from(values)) as Arc<dyn Array>;

        // This array should have a null buffer even though no values are null
        // (depending on Arrow's implementation)
        let result = normalize_null_buffer(array_with_null_buffer).unwrap();

        // Result should have no null buffer and same values
        assert_eq!(result.len(), 3);
        assert_eq!(result.null_count(), 0);
        let int_result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_result.value(0), 1);
        assert_eq!(int_result.value(1), 2);
        assert_eq!(int_result.value(2), 3);
    }

    #[test]
    fn test_optimize_null_buffer_with_nulls() {
        use arrow_array::Int32Array;

        // Create an array that actually has null values
        let values = vec![Some(1), None, Some(3)];
        let array_with_nulls = Arc::new(Int32Array::from(values)) as Arc<dyn Array>;
        let result = normalize_null_buffer(array_with_nulls.clone()).unwrap();

        // Should return the same array since it actually has nulls
        assert!(Arc::ptr_eq(&array_with_nulls, &result));
        assert!(result.nulls().is_some());
        assert_eq!(result.null_count(), 1);
        assert!(result.is_null(1));
    }

    #[test]
    fn test_optimize_null_buffer_string_array() {
        use arrow_array::StringArray;

        // Test with string array that has no nulls but might have a null buffer
        let values = vec![Some("hello"), Some("world"), Some("test")];
        let array = Arc::new(StringArray::from(values)) as Arc<dyn Array>;
        let result = normalize_null_buffer(array).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.null_count(), 0);
        let string_result = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_result.value(0), "hello");
        assert_eq!(string_result.value(1), "world");
        assert_eq!(string_result.value(2), "test");
    }
}
