//! Array casting and conversion utilities.

use std::sync::Arc;

use arrow_array::{cast::AsArray, Array};
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
            )))
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
}
