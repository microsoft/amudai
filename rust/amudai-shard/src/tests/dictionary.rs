use std::cmp::Ordering;

use amudai_format::{
    defs::shard::BufferKind,
    schema::{BasicType, BasicTypeDescriptor},
};

use crate::{
    read::field_decoder::dictionary::DictionaryDecoder, write::field_encoder::PreparedDictionary,
};

#[test]
fn test_dictionary_buffer_encoding_str() {
    let temp_store = amudai_io_impl::temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap();

    let mut dictionary = PreparedDictionary::new(BasicTypeDescriptor {
        basic_type: BasicType::String,
        ..Default::default()
    });

    dictionary.push(b"aaa", 10);
    dictionary.push(b"abcdefghijk", 20);
    dictionary.push_null(25);
    dictionary.push(b"1234567", 30);

    let buffer = dictionary.encode_buffer(temp_store.as_ref()).unwrap();
    assert_eq!(buffer.descriptor.kind, BufferKind::ValueDictionary as i32);
    assert!(buffer.data_size > 20);
    assert!(buffer.data_size < 1000);
}

#[test]
fn test_dictionary_buffer_encoding_fixed() {
    let temp_store = amudai_io_impl::temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap();

    let mut dictionary = PreparedDictionary::new(BasicTypeDescriptor {
        basic_type: BasicType::FixedSizeBinary,
        fixed_size: 5,
        ..Default::default()
    });

    dictionary.push(b"aaaaa", 10);
    dictionary.push(b"bbbbb", 20);
    dictionary.push_null(25);
    dictionary.push(b"11111", 30);

    let buffer = dictionary.encode_buffer(temp_store.as_ref()).unwrap();
    assert_eq!(buffer.descriptor.kind, BufferKind::ValueDictionary as i32);
    assert!(buffer.data_size > 20);
    assert!(buffer.data_size < 1000);
}

#[test]
fn test_dictionary_buffer_decoding_str() {
    let temp_store = amudai_io_impl::temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap();

    let mut dictionary = PreparedDictionary::new(BasicTypeDescriptor {
        basic_type: BasicType::String,
        ..Default::default()
    });

    for i in 0..1000 {
        if i != 400 {
            dictionary.push(format!("value_{i}").as_bytes(), i + 10000);
        } else {
            dictionary.push_null(i + 10000);
        }
    }

    let buffer = dictionary.encode_buffer(temp_store.as_ref()).unwrap();

    let decoder = DictionaryDecoder::from_prepared_buffer(&buffer).unwrap();

    // Header validations
    let header = decoder.header().unwrap();
    assert_eq!(header.value_count, 1000);
    assert_eq!(header.value_type, BasicType::String);
    assert_eq!(header.fixed_value_size, 0);

    let values = decoder.values().unwrap();
    for id in 0u32..header.value_count {
        values.get(id);
    }

    let sorted_ids = decoder.sorted_ids().unwrap().unwrap();
    let mut prev: &[u8] = &[];
    for &id in sorted_ids.as_slice() {
        let next = values.get(id);
        assert_ne!(prev.cmp(next), Ordering::Greater);
        prev = next;
    }
}

#[test]
fn test_dictionary_buffer_decoding_fixed() {
    let temp_store = amudai_io_impl::temp_file_store::create_in_memory(64 * 1024 * 1024).unwrap();

    let mut dictionary = PreparedDictionary::new(BasicTypeDescriptor {
        basic_type: BasicType::FixedSizeBinary,
        fixed_size: 10,
        ..Default::default()
    });

    for i in 0..1000 {
        if i != 400 {
            let mut value: Vec<u8> = format!("value_{i}").into();
            value.resize(10, 0); // Ensure fixed size
            dictionary.push(&value, i + 10000);
        } else {
            dictionary.push_null(i + 10000);
        }
    }

    let buffer = dictionary.encode_buffer(temp_store.as_ref()).unwrap();

    let decoder = DictionaryDecoder::from_prepared_buffer(&buffer).unwrap();

    // Header validations
    let header = decoder.header().unwrap();
    assert_eq!(header.value_count, 1000);
    assert_eq!(header.value_type, BasicType::FixedSizeBinary);
    assert_eq!(header.fixed_value_size, 10);

    let values = decoder.values().unwrap();
    for id in 0u32..header.value_count {
        values.get(id);
    }

    let sorted_ids = decoder.sorted_ids().unwrap();
    if let Some(sorted_ids) = sorted_ids {
        let mut prev: &[u8] = &[];
        for &id in sorted_ids.as_slice() {
            let next = values.get(id);
            assert_ne!(prev.cmp(next), Ordering::Greater);
            prev = next;
        }
    }
}
