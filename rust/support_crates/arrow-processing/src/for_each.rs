//! Inspection of values within Arrow arrays in a generic way.

use arrow_array::{Array, cast::AsArray};
use arrow_schema::DataType;

/// Invokes a function `f` on each binary item of the `array`.
///
/// This function iterates through the elements of the provided array and applies the function `f` to each binary item.
/// The iteration stops immediately if `f` returns an error.
///
/// The function `f` receives two arguments:
///
/// *   `usize`: The index of the item in the array.
/// *   `Option<&[u8]>`: An optional byte slice representing the item's data. This will be `None` if the item is null.
///
/// # Supported Array Types
///
/// This function supports the following array types:
///
/// *   `StringArray`
/// *   `LargeStringArray`
/// *   `StringViewArray`
/// *   `BinaryArray`
/// *   `LargeBinaryArray`
/// *   `BinaryViewArray`
/// *   `FixedSizeBinaryArray`
///
/// # Errors
///
/// Returns an error if the provided function `f` returns an error.
pub fn for_each_as_binary<E>(
    array: &dyn Array,
    mut f: impl FnMut(usize, Option<&[u8]>) -> Result<(), E>,
) -> Result<(), E> {
    assert!(
        is_binary_compatible_type(array.data_type()),
        "for_each_as_binary: incompatible array type {:?}",
        array.data_type()
    );

    match array.data_type() {
        DataType::Utf8 => {
            for (i, value) in array.as_string::<i32>().iter().enumerate() {
                f(i, value.map(str::as_bytes))?;
            }
        }
        DataType::LargeUtf8 => {
            for (i, value) in array.as_string::<i64>().iter().enumerate() {
                f(i, value.map(str::as_bytes))?;
            }
        }
        DataType::Utf8View => {
            for (i, value) in array.as_string_view().iter().enumerate() {
                f(i, value.map(str::as_bytes))?;
            }
        }
        DataType::Binary => {
            for (i, value) in array.as_binary::<i32>().iter().enumerate() {
                f(i, value)?;
            }
        }
        DataType::LargeBinary => {
            for (i, value) in array.as_binary::<i64>().iter().enumerate() {
                f(i, value)?;
            }
        }
        DataType::BinaryView => {
            for (i, value) in array.as_binary_view().iter().enumerate() {
                f(i, value)?;
            }
        }
        DataType::FixedSizeBinary(_) => {
            for (i, value) in array.as_fixed_size_binary().iter().enumerate() {
                f(i, value)?;
            }
        }
        _ => panic!("Unsupported array type: {:?}", array.data_type()),
    }
    Ok(())
}

fn is_binary_compatible_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        BinaryArray, BinaryViewArray, DictionaryArray, FixedSizeBinaryArray, Int32Array,
        LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
    };
    use arrow_buffer::{Buffer, NullBuffer};
    use std::sync::Arc;

    #[test]
    fn test_string_array() {
        let array = StringArray::from(vec![Some("hello"), None, Some("world")]);
        let mut result = Vec::new();
        for_each_as_binary(&array, |index, value| {
            result.push((index, value.map(<[u8]>::to_vec)));
            Ok::<(), ()>(())
        })
        .unwrap();
        assert_eq!(
            result,
            vec![
                (0, Some(b"hello".to_vec())),
                (1, None),
                (2, Some(b"world".to_vec())),
            ]
        );
    }

    #[test]
    fn test_large_string_array() {
        let array = LargeStringArray::from(vec![Some("hello"), None, Some("world")]);
        let mut result = Vec::new();
        for_each_as_binary(&array, |index, value| {
            result.push((index, value.map(<[u8]>::to_vec)));
            Ok::<(), ()>(())
        })
        .unwrap();
        assert_eq!(
            result,
            vec![
                (0, Some(b"hello".to_vec())),
                (1, None),
                (2, Some(b"world".to_vec())),
            ]
        );
    }

    #[test]
    fn test_string_view_array() {
        let array = StringViewArray::from(vec![Some("hello"), None, Some("world")]);
        let mut result = Vec::new();
        for_each_as_binary(&array, |index, value| {
            result.push((index, value.map(<[u8]>::to_vec)));
            Ok::<(), ()>(())
        })
        .unwrap();
        assert_eq!(
            result,
            vec![
                (0, Some(b"hello".to_vec())),
                (1, None),
                (2, Some(b"world".to_vec())),
            ]
        );
    }

    #[test]
    fn test_binary_array() {
        let array = BinaryArray::from(vec![Some(b"hello".as_ref()), None, Some(b"world".as_ref())]);
        let mut result = Vec::new();
        for_each_as_binary(&array, |index, value| {
            result.push((index, value.map(<[u8]>::to_vec)));
            Ok::<(), ()>(())
        })
        .unwrap();
        assert_eq!(
            result,
            vec![
                (0, Some(b"hello".to_vec())),
                (1, None),
                (2, Some(b"world".to_vec())),
            ]
        );
    }

    #[test]
    fn test_large_binary_array() {
        let array =
            LargeBinaryArray::from(vec![Some(b"hello".as_ref()), None, Some(b"world".as_ref())]);
        let mut result = Vec::new();
        for_each_as_binary(&array, |index, value| {
            result.push((index, value.map(<[u8]>::to_vec)));
            Ok::<(), ()>(())
        })
        .unwrap();
        assert_eq!(
            result,
            vec![
                (0, Some(b"hello".to_vec())),
                (1, None),
                (2, Some(b"world".to_vec())),
            ]
        );
    }

    #[test]
    fn test_binary_view_array() {
        let array =
            BinaryViewArray::from(vec![Some(b"hello".as_ref()), None, Some(b"world".as_ref())]);
        let mut result = Vec::new();
        for_each_as_binary(&array, |index, value| {
            result.push((index, value.map(<[u8]>::to_vec)));
            Ok::<(), ()>(())
        })
        .unwrap();
        assert_eq!(
            result,
            vec![
                (0, Some(b"hello".to_vec())),
                (1, None),
                (2, Some(b"world".to_vec())),
            ]
        );
    }

    #[test]
    fn test_fixed_size_binary_array() {
        let array = FixedSizeBinaryArray::try_new(
            5,
            Buffer::from(vec![
                104u8, 101, 108, 108, 111, // "hello"
                0, 0, 0, 0, 0, // null
                119, 111, 114, 108, 100, // "world"
            ]),
            Some(NullBuffer::from(vec![true, false, true])),
        )
        .unwrap();
        let mut result = Vec::new();
        for_each_as_binary(&array, |index, value| {
            result.push((index, value.map(<[u8]>::to_vec)));
            Ok::<(), ()>(())
        })
        .unwrap();
        assert_eq!(
            result,
            vec![
                (0, Some(b"hello".to_vec())),
                (1, None),
                (2, Some(b"world".to_vec())),
            ]
        );
    }

    #[test]
    #[should_panic(expected = "for_each_as_binary: incompatible array type Int32")]
    fn test_unsupported_array_type() {
        let array = Int32Array::from(vec![1, 2, 3]);
        for_each_as_binary(&array, |_, _| Ok::<(), ()>(())).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "for_each_as_binary: incompatible array type Dictionary(Int32, Int32)"
    )]
    fn test_unsupported_dictionary_value_type() {
        let keys = Int32Array::from(vec![Some(0), Some(1)]);
        let values = Int32Array::from(vec![1, 2]);
        let array = DictionaryArray::try_new(keys, Arc::new(values)).unwrap();
        for_each_as_binary(&array, |_, _| Ok::<(), ()>(())).unwrap();
    }

    #[test]
    fn test_error_propagation() {
        let array = StringArray::from(vec![Some("hello"), Some("world")]);
        let result = for_each_as_binary(&array, |index, _| {
            if index == 1 {
                Err("Test Error")
            } else {
                Ok(())
            }
        });
        assert_eq!(result, Err("Test Error"));
    }
}
