//! Compute the logical data size of an Arrow array.

use arrow_array::{cast::AsArray, Array};

/// Calculates the total size in bytes of the underlying data buffer for a given binary-like array.
///
/// This function supports various binary-like array types, including:
///
/// *   `Binary`: Variable-length byte arrays with 32-bit offsets.
/// *   `FixedSizeBinary`: Fixed-length byte arrays.
/// *   `LargeBinary`: Variable-length byte arrays with 64-bit offsets.
/// *   `BinaryView`: Variable-length byte arrays represented by a view structure.
/// *   `Utf8`: Variable-length UTF-8 encoded strings with 32-bit offsets.
/// *   `LargeUtf8`: Variable-length UTF-8 encoded strings with 64-bit offsets.
/// *   `Utf8View`: Variable-length UTF-8 encoded strings represented by a view structure.
///
/// # Arguments
///
/// *   `array`: A reference to an `Array` trait object. The underlying array must be one
///     of the supported binary-like types.
///
/// # Returns
///
/// The total size in bytes of the underlying data buffer.
///
/// # Panics
///
/// This function will panic if the provided array is not one of the supported binary-like types.
pub fn binary_data_size(array: &dyn Array) -> usize {
    match array.data_type() {
        arrow_schema::DataType::Binary => {
            data_size_from_offsets(array.as_binary::<i32>().value_offsets())
        }
        arrow_schema::DataType::FixedSizeBinary(size) => array.len().wrapping_mul(*size as usize),
        arrow_schema::DataType::LargeBinary => {
            data_size_from_offsets(array.as_binary::<i64>().value_offsets())
        }
        arrow_schema::DataType::BinaryView => array
            .as_binary_view()
            .views()
            .iter()
            .map(|v| (*v as i32) as usize)
            .sum(),
        arrow_schema::DataType::Utf8 => {
            data_size_from_offsets(array.as_string::<i32>().value_offsets())
        }
        arrow_schema::DataType::LargeUtf8 => {
            data_size_from_offsets(array.as_string::<i64>().value_offsets())
        }
        arrow_schema::DataType::Utf8View => array
            .as_string_view()
            .views()
            .iter()
            .map(|v| (*v as i32) as usize)
            .sum(),
        _ => panic!(
            "binary_data_size: unsupported array data type {:?}",
            array.data_type()
        ),
    }
}

fn data_size_from_offsets<T: arrow_array::OffsetSizeTrait>(offsets: &[T]) -> usize {
    assert!(!offsets.is_empty());
    offsets
        .last()
        .unwrap()
        .as_usize()
        .checked_sub(offsets[0].as_usize())
        .expect("offset diff")
}

#[cfg(test)]
mod tests {
    use arrow_array::{builder::StringViewBuilder, BinaryArray, FixedSizeBinaryArray, StringArray};
    use arrow_schema::DataType;

    use super::*;

    #[test]
    fn test_binary_array() {
        let data: Vec<&[u8]> = vec![b"hello", b"world", b"rust"];
        let array = BinaryArray::from_vec(data);
        assert_eq!(binary_data_size(&array), 14);
    }

    #[test]
    fn test_binary_array_empty() {
        let array = BinaryArray::from_vec(Vec::<&[u8]>::new());
        assert_eq!(binary_data_size(&array), 0);
    }

    #[test]
    fn test_binary_array_nulls() {
        let data: Vec<Option<&[u8]>> = vec![Some(b"hello"), None, Some(b"rust")];
        let array = BinaryArray::from_opt_vec(data);
        assert_eq!(binary_data_size(&array), 9);
    }

    #[test]
    fn test_string_array() {
        let array = StringArray::from(vec!["hello", "world", "rust"]);
        assert_eq!(binary_data_size(&array), 14);
        let array = StringArray::from(vec!["hello", "", "", "rust"]);
        assert_eq!(binary_data_size(&array), 9);
    }

    #[test]
    fn test_string_view_array() {
        let mut builder = StringViewBuilder::new();

        let block = builder.append_block(b"hellorust".into());
        builder.try_append_view(block, 0, 5).unwrap();
        builder.try_append_view(block, 5, 4).unwrap();
        builder.try_append_view(block, 0, 9).unwrap();
        builder.try_append_view(block, 5, 0).unwrap();
        builder.try_append_view(block, 5, 4).unwrap();
        builder.try_append_view(block, 0, 5).unwrap();
        let array = builder.finish();
        let array2 = arrow_cast::cast(&array, &DataType::LargeUtf8).unwrap();
        assert_eq!(binary_data_size(&array), 27);
        assert_eq!(binary_data_size(&array2), 27);
    }

    #[test]
    fn test_fixed_size_binary_array() {
        let array = FixedSizeBinaryArray::from(vec![b"abcd", b"efgh", b"ijkl"]);
        assert_eq!(binary_data_size(&array), 12);
    }
}
