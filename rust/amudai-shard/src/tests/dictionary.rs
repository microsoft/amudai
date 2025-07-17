use std::cmp::Ordering;

use amudai_format::{
    defs::shard::BufferKind,
    schema::{BasicType, BasicTypeDescriptor},
};

use crate::{
    read::field_decoder::dictionary::DictionaryDecoder, write::field_encoder::PreparedDictionary,
};

#[test]
fn test_dictionary_buffer_encoding() {
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
    let values = decoder.values().unwrap();
    for id in 0u32..decoder.header().unwrap().value_count {
        values.get(id);
    }

    let sorted_ids = decoder.sorted_ids().unwrap().unwrap();
    let mut prev: &[u8] = &[];
    for &id in &sorted_ids.sorted_ids {
        let next = values.get(id);
        assert_ne!(prev.cmp(next), Ordering::Greater);
        prev = next;
    }
}
