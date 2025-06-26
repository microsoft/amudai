use std::io::{Read, Write};
use std::ops::Range;
use std::sync::Arc;

use amudai_io::temp_file_store::TemporaryFileStore;

fn create_stores() -> Vec<Arc<dyn TemporaryFileStore>> {
    create_stores_with_capacity(10 * 1024 * 1024)
}

fn create_stores_with_capacity(capacity: u64) -> Vec<Arc<dyn TemporaryFileStore>> {
    vec![
        super::create_in_memory(capacity).unwrap(),
        super::create_file_based_cached(capacity, None).unwrap(),
        super::create_file_based_uncached(capacity, None).unwrap(),
    ]
}

fn test_allocate_writable_write_read_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut writable = store
        .allocate_writable(None)
        .expect("Failed to allocate writable");

    let data = b"Hello, TemporaryFileStore!";
    writable.write_all(data).expect("Failed to write data");

    let mut reader = writable.into_reader().expect("Failed to convert to reader");

    let mut read_data = Vec::with_capacity(data.len());
    reader
        .read_to_end(&mut read_data)
        .expect("Failed to read data");

    assert_eq!(read_data, data);
}
#[test]
fn test_allocate_writable_write_read() {
    for store in create_stores() {
        test_allocate_writable_write_read_impl(&store);
    }
}

fn test_allocate_writable_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut writable = store
        .allocate_writable(None)
        .expect("Failed to allocate writable");
    assert_eq!(writable.current_size(), 0);

    let data = b"Hello, world!";
    writable.write_all(data).expect("Failed to write data");
    assert_eq!(writable.current_size(), data.len() as u64);
}

#[test]
fn test_allocate_writable() {
    for store in create_stores_with_capacity(1024) {
        test_allocate_writable_impl(&store);
    }
}

fn test_allocate_buffer_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut buffer = store
        .allocate_buffer(None)
        .expect("Failed to allocate buffer");
    assert_eq!(buffer.current_size(), 0);

    let data = b"Hello, buffer!";
    buffer.write_all(data).expect("Failed to write data");
    assert_eq!(buffer.current_size(), data.len() as u64);

    let read_data = buffer
        .read_at(0..data.len() as u64)
        .expect("Failed to read data");
    assert_eq!(read_data.as_ref(), data);
}

#[test]
fn test_allocate_buffer() {
    for store in create_stores_with_capacity(1024) {
        test_allocate_buffer_impl(&store);
    }
}

fn test_truncate_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut writable = store
        .allocate_writable(None)
        .expect("Failed to allocate writable");
    let data = b"Hello, world!";
    writable.write_all(data).expect("Failed to write data");

    writable.truncate(5).expect("Failed to truncate");
    assert_eq!(writable.current_size(), 5);

    let mut reader = writable.into_reader().expect("Failed to convert to reader");
    let mut read_data = vec![0; 5];
    reader
        .read_exact(&mut read_data)
        .expect("Failed to read data");
    assert_eq!(&read_data, b"Hello");
}

#[test]
fn test_truncate() {
    for store in create_stores_with_capacity(1024) {
        test_truncate_impl(&store);
    }
}

fn test_allocate_buffer_append_write_read_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut buffer = store
        .allocate_buffer(None)
        .expect("Failed to allocate buffer");

    // Append initial data
    let initial_data = b"Initial data.";
    buffer
        .write_all(initial_data)
        .expect("Failed to append initial data");

    // Write at a specific position
    let overwrite_data = b"Overwrite!";
    buffer
        .write_at(8, overwrite_data)
        .expect("Failed to write at position");

    let range = 0..buffer.current_size();

    // Convert to reader and verify
    let reader = buffer
        .into_read_at()
        .expect("Failed to convert to ReadAtBlocking");

    // Read the entire range
    let bytes = reader.read_at(range).expect("Failed to read at range");

    let expected = b"Initial Overwrite!";
    assert_eq!(&bytes[..], &expected[..]);
}

#[test]
fn test_allocate_buffer_append_write_read() {
    for store in create_stores() {
        test_allocate_buffer_append_write_read_impl(&store);
    }
}

fn test_truncate_writable_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut writable = store
        .allocate_writable(None)
        .expect("Failed to allocate writable");

    let data = b"Data before truncation.";
    writable.write_all(data).expect("Failed to write data");

    writable.truncate(10).expect("Failed to truncate");

    assert_eq!(writable.current_size(), 10);

    let mut reader = writable.into_reader().expect("Failed to convert to reader");

    let mut read_data = Vec::new();
    reader
        .read_to_end(&mut read_data)
        .expect("Failed to read data");

    assert_eq!(&read_data[..], &data[..10]);
}

#[test]
fn test_truncate_writable() {
    for store in create_stores() {
        test_truncate_writable_impl(&store);
    }
}

/// Test reading beyond the end of the writable stream to ensure short reads are handled.
fn test_read_beyond_end_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut writable = store
        .allocate_writable(None)
        .expect("Failed to allocate writable");

    let data = b"Short data.";
    writable.write_all(data).expect("Failed to write data");

    let reader = writable
        .into_read_at()
        .expect("Failed to convert to ReadAtBlocking");

    let range = 0..data.len() as u64 + 10; // Extend beyond the data length
    let bytes = reader.read_at(range).expect("Failed to read at range");

    assert_eq!(&bytes[..], &data[..]);
    assert_eq!(bytes.len() as u64, data.len() as u64); // Short read
}

#[test]
fn test_read_beyond_end() {
    for store in create_stores() {
        test_read_beyond_end_impl(&store);
    }
}

/// Test concurrent allocations and operations on the `TemporaryFileStore`.
fn test_concurrent_access_impl(store: &Arc<dyn TemporaryFileStore>) {
    use std::thread;

    let num_threads = 10;
    let data_per_thread = b"Concurrent data.";

    let mut handles = Vec::new();

    for _ in 0..num_threads {
        let store_clone = Arc::clone(store);
        let data = data_per_thread.to_vec();
        handles.push(thread::spawn(move || {
            let mut writable = store_clone
                .allocate_writable(None)
                .expect("Failed to allocate writable");
            writable.write_all(&data).expect("Failed to write data");
            let mut reader = writable.into_reader().expect("Failed to convert to reader");
            let mut read_data = Vec::new();
            reader
                .read_to_end(&mut read_data)
                .expect("Failed to read data");
            assert_eq!(read_data, data);
        }));
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }
}

#[test]
fn test_concurrent_access() {
    for store in create_stores() {
        test_concurrent_access_impl(&store);
    }
}

/// Test handling of empty writable streams.
fn test_empty_writable_impl(store: &Arc<dyn TemporaryFileStore>) {
    let writable = store
        .allocate_writable(None)
        .expect("Failed to allocate writable");

    assert_eq!(writable.current_size(), 0);

    let reader = writable
        .into_read_at()
        .expect("Failed to convert to reader");

    let bytes = reader
        .read_at(0..10)
        .expect("Failed to read from empty writer");
    assert!(bytes.is_empty());
}

#[test]
fn test_empty_writable() {
    for store in create_stores() {
        test_empty_writable_impl(&store);
    }
}

/// Test writing at the beginning, middle, and end positions.
fn test_write_at_positions_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut buffer = store
        .allocate_buffer(None)
        .expect("Failed to allocate buffer");

    // Initial write
    let initial_data = b"HelloWorld";
    buffer
        .write_all(initial_data)
        .expect("Failed to write initial data");

    // Write at beginning
    let beginning_data = b"Start-";
    buffer
        .write_at(0, beginning_data)
        .expect("Failed to write at beginning");

    // Write in the middle
    let middle_data = b"-Middle-";
    buffer
        .write_at(5, middle_data)
        .expect("Failed to write in the middle");

    // Write at the end
    let end_data = b"End";
    buffer
        .write_at(buffer.current_size(), end_data)
        .expect("Failed to write at end");

    let range = 0..buffer.current_size();

    // Convert to reader and verify
    let reader = buffer
        .into_read_at()
        .expect("Failed to convert to ReadAtBlocking");

    let bytes = reader.read_at(range).expect("Failed to read at range");

    let expected = b"Start-Middle-End";
    assert_eq!(&bytes[..], &expected[..]);
}

#[test]
fn test_write_at_positions() {
    for store in create_stores() {
        test_write_at_positions_impl(&store);
    }
}

/// Test allocating multiple buffers and ensuring isolation between them.
fn test_multiple_buffers_isolation_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut buffer1 = store
        .allocate_buffer(None)
        .expect("Failed to allocate buffer1");
    let mut buffer2 = store
        .allocate_buffer(None)
        .expect("Failed to allocate buffer2");

    buffer1
        .write_all(b"Buffer1 Data")
        .expect("Failed to write to buffer1");
    buffer2
        .write_all(b"Buffer2 Data")
        .expect("Failed to write to buffer2");

    let buffer1_size = buffer1.current_size();
    let buffer2_size = buffer1.current_size();

    let reader1 = buffer1
        .into_read_at()
        .expect("Failed to convert buffer1 to reader");
    let reader2 = buffer2
        .into_read_at()
        .expect("Failed to convert buffer2 to reader");

    let data1 = reader1
        .read_at(0..buffer1_size)
        .expect("Failed to read from buffer1");
    let data2 = reader2
        .read_at(0..buffer2_size)
        .expect("Failed to read from buffer2");

    assert_eq!(&data1[..], b"Buffer1 Data");
    assert_eq!(&data2[..], b"Buffer2 Data");
}

#[test]
fn test_multiple_buffers_isolation() {
    for store in create_stores() {
        test_multiple_buffers_isolation_impl(&store);
    }
}

/// Test truncating a buffer and ensuring subsequent reads reflect the truncation.
fn test_truncate_buffer_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut buffer = store
        .allocate_buffer(None)
        .expect("Failed to allocate buffer");

    buffer
        .write_all(b"Data to be truncated")
        .expect("Failed to write data");
    buffer.truncate(9).expect("Failed to truncate buffer");

    assert_eq!(buffer.current_size(), 9);

    let reader = buffer
        .into_read_at()
        .expect("Failed to convert to ReadAtBlocking");
    let bytes = reader.read_at(0..9).expect("Failed to read from buffer");

    assert_eq!(&bytes[..], b"Data to b");
}

#[test]
fn test_truncate_buffer() {
    for store in create_stores() {
        test_truncate_buffer_impl(&store);
    }
}

/// Test writing beyond the current size to ensure the buffer expands correctly.
fn test_write_beyond_current_size_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut buffer = store
        .allocate_buffer(None)
        .expect("Failed to allocate buffer");

    buffer
        .write_all(b"Initial")
        .expect("Failed to write initial data");
    buffer
        .write_at(10, b"Extended")
        .expect("Failed to write beyond current size");

    assert_eq!(buffer.current_size(), 18);

    let reader = buffer
        .into_read_at()
        .expect("Failed to convert to ReadAtBlocking");

    let bytes = reader.read_at(0..18).expect("Failed to read at range");

    let mut expected = vec![];
    expected.extend_from_slice(b"Initial");
    expected.extend_from_slice(&[0u8; 3]); // Padding
    expected.extend_from_slice(b"Extended");

    assert_eq!(&bytes[..], &expected[..]);
}

#[test]
fn test_write_beyond_current_size() {
    for store in create_stores() {
        test_write_beyond_current_size_impl(&store);
    }
}

/// Test multiple readers accessing the same buffer concurrently.
fn test_multiple_readers_concurrent_access_impl(store: &Arc<dyn TemporaryFileStore>) {
    use std::thread;

    let mut buffer = store
        .allocate_buffer(None)
        .expect("Failed to allocate buffer");
    buffer
        .write_all(b"Concurrent Readers Data")
        .expect("Failed to write data");
    let reader = buffer
        .into_read_at()
        .expect("Failed to convert to ReadAtBlocking");

    let reader_arc = Arc::new(reader);
    let num_threads = 5;
    let mut handles = Vec::new();

    for _ in 0..num_threads {
        let reader_clone = Arc::clone(&reader_arc);
        handles.push(thread::spawn(move || {
            let bytes = reader_clone.read_at(0..24).expect("Failed to read data");
            assert_eq!(&bytes[..], b"Concurrent Readers Data");
        }));
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }
}

#[test]
fn test_multiple_readers_concurrent_access() {
    for store in create_stores() {
        test_multiple_readers_concurrent_access_impl(&store);
    }
}

/// Test reading and writing with invalid ranges to ensure proper error handling.
fn test_invalid_range_operations_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut writable = store
        .allocate_writable(None)
        .expect("Failed to allocate writable");
    writable
        .write_all(b"Valid data")
        .expect("Failed to write data");
    let reader = writable
        .into_read_at()
        .expect("Failed to convert to ReadAtBlocking");

    // Invalid range where start > end
    let invalid_range = Range { start: 10, end: 5 };
    let result = reader.read_at(invalid_range);
    assert!(result.is_err());

    // Negative positions are not possible with u64, so no need to test

    // Extremely large range
    let large_range = 0..u64::MAX;
    let bytes = reader
        .read_at(large_range)
        .expect("Failed to read with large range");
    assert_eq!(&bytes[..], b"Valid data");
}

#[test]
fn test_invalid_range_operations() {
    for store in create_stores() {
        test_invalid_range_operations_impl(&store);
    }
}

/// Test that writing zero bytes does not alter the buffer.
fn test_write_zero_bytes_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut buffer = store
        .allocate_buffer(None)
        .expect("Failed to allocate buffer");
    buffer.write_all(b"Data").expect("Failed to write data");
    buffer.write_all(&[]).expect("Failed to write zero bytes");

    assert_eq!(buffer.current_size(), 4);

    let reader = buffer
        .into_read_at()
        .expect("Failed to convert to ReadAtBlocking");
    let bytes = reader.read_at(0..4).expect("Failed to read data");

    assert_eq!(&bytes[..], b"Data");
}

#[test]
fn test_write_zero_bytes() {
    for store in create_stores() {
        test_write_zero_bytes_impl(&store);
    }
}

/// Test reading from a buffer after multiple write and truncate operations.
fn test_read_after_multiple_writes_truncates_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut buffer = store
        .allocate_buffer(None)
        .expect("Failed to allocate buffer");

    buffer
        .write_all(b"First part. ")
        .expect("Failed to write first part");
    buffer
        .write_all(b"Second part. ")
        .expect("Failed to write second part");
    buffer
        .truncate(12)
        .expect("Failed to truncate to 'First part.'");

    buffer
        .write_all(b"Modified. ")
        .expect("Failed to write modified data");
    buffer
        .write_all(b"Third part.")
        .expect("Failed to write third part");

    let size = buffer.current_size();

    let reader = buffer
        .into_read_at()
        .expect("Failed to convert to ReadAtBlocking");
    let bytes = reader.read_at(0..size).expect("Failed to read data");

    let expected = b"First part. Modified. Third part.";
    assert_eq!(&bytes[..], &expected[..]);
}

#[test]
fn test_read_after_multiple_writes_truncates() {
    for store in create_stores() {
        test_read_after_multiple_writes_truncates_impl(&store);
    }
}

/// Test writing to a buffer and reading specific ranges to verify `read_at` functionality.
fn test_read_at_specific_ranges_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut buffer = store
        .allocate_buffer(None)
        .expect("Failed to allocate buffer");
    let data = b"The quick brown fox jumps over the lazy dog.";
    buffer.write_all(data).expect("Failed to write data");

    let reader = buffer
        .into_read_at()
        .expect("Failed to convert to ReadAtBlocking");

    // Read the word "quick"
    let quick = reader.read_at(4..9).expect("Failed to read 'quick'");
    assert_eq!(&quick[..], b"quick");

    // Read the word "fox"
    let fox = reader.read_at(16..19).expect("Failed to read 'fox'");
    assert_eq!(&fox[..], b"fox");

    // Read the last word "dog."
    let dog = reader.read_at(40..44).expect("Failed to read 'dog.'");
    assert_eq!(&dog[..], b"dog.");

    // Read a range that includes padding (if any)
    let partial = reader
        .read_at(0..10)
        .expect("Failed to read first 10 bytes");
    assert_eq!(&partial[..], b"The quick ");
}

#[test]
fn test_read_at_specific_ranges() {
    for store in create_stores() {
        test_read_at_specific_ranges_impl(&store);
    }
}

/// Test writing non-sequential data using `write_at` and verifying the entire buffer.
fn test_write_at_non_sequential_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut buffer = store
        .allocate_buffer(None)
        .expect("Failed to allocate buffer");

    // Write "Hello"
    buffer
        .write_at(0, b"Hello")
        .expect("Failed to write 'Hello'");

    // Write "World" starting at position 10
    buffer
        .write_at(10, b"World")
        .expect("Failed to write 'World'");

    // The buffer should have "Hello" followed by 5 padding bytes and then "World"
    let expected = b"Hello\x00\x00\x00\x00\x00World";

    let reader = buffer
        .into_read_at()
        .expect("Failed to convert to ReadAtBlocking");
    let bytes = reader.read_at(0..15).expect("Failed to read data");

    assert_eq!(&bytes[..], &expected[..]);
}

#[test]
fn test_write_at_non_sequential() {
    for store in create_stores() {
        test_write_at_non_sequential_impl(&store);
    }
}

/// Test that multiple truncates work as expected.
fn test_multiple_truncates_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut buffer = store
        .allocate_buffer(None)
        .expect("Failed to allocate buffer");

    buffer
        .write_all(b"Data for truncation testing")
        .expect("Failed to write data");
    buffer.truncate(10).expect("Failed to truncate to 10 bytes");
    assert_eq!(buffer.current_size(), 10);

    buffer.truncate(5).expect("Failed to truncate to 5 bytes");
    assert_eq!(buffer.current_size(), 5);

    buffer.write_at(8, b"aaa").expect("Failed to extend");
    buffer.truncate(8).expect("Failed to truncate to 8 bytes");
    assert_eq!(buffer.current_size(), 8);

    let reader = buffer
        .into_read_at()
        .expect("Failed to convert to ReadAtBlocking");
    let bytes = reader.read_at(0..8).expect("Failed to read data");

    // Expect first 5 bytes of "Data " followed by 3 padding bytes
    let expected = b"Data \x00\x00\x00";
    assert_eq!(&bytes[..], &expected[..]);
}

#[test]
fn test_multiple_truncates() {
    for store in create_stores() {
        test_multiple_truncates_impl(&store);
    }
}

/// Test writing and reading multiple small chunks to simulate fragmented writes.
fn test_fragmented_writes_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut buffer = store
        .allocate_buffer(None)
        .expect("Failed to allocate buffer");

    let chunks: Vec<&[u8]> = vec![b"Frag", b"mented", b" ", b"Write", b"s."];

    for chunk in &chunks {
        buffer.write_all(chunk).expect("Failed to write chunk");
    }

    let size = buffer.current_size();

    let reader = buffer
        .into_read_at()
        .expect("Failed to convert to ReadAtBlocking");
    let bytes = reader.read_at(0..size).expect("Failed to read data");

    let expected = b"Fragmented Writes.";
    assert_eq!(&bytes[..], expected);
}

#[test]
fn test_fragmented_writes() {
    for store in create_stores() {
        test_fragmented_writes_impl(&store);
    }
}

/// Test that writing and reading with overlapping `write_at` operations maintains data integrity.
fn test_overlapping_write_at_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut buffer = store
        .allocate_buffer(None)
        .expect("Failed to allocate buffer");

    buffer
        .write_all(b"ABCDEFGHIJ")
        .expect("Failed to write initial data");

    // Overwrite "CDE" with "123"
    buffer
        .write_at(2, b"123")
        .expect("Failed to overwrite 'CDE'");

    // Overwrite "FGH" with "4567"
    buffer
        .write_at(5, b"4567")
        .expect("Failed to overwrite 'FGH'");

    let size = buffer.current_size();
    let reader = buffer
        .into_read_at()
        .expect("Failed to convert to ReadAtBlocking");
    let bytes = reader.read_at(0..size).expect("Failed to read data");

    let expected = b"AB1234567J";
    assert_eq!(&bytes[..], expected);
}

#[test]
fn test_overlapping_write_at() {
    for store in create_stores() {
        test_overlapping_write_at_impl(&store);
    }
}

/// Test writing and reading zero-length ranges to ensure no data is returned but no error occurs.
fn test_zero_length_read_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut writable = store
        .allocate_writable(None)
        .expect("Failed to allocate writable");
    writable
        .write_all(b"Non-empty data")
        .expect("Failed to write data");

    let reader = writable
        .into_read_at()
        .expect("Failed to convert to reader");

    let bytes = reader
        .read_at(5..5)
        .expect("Failed to read zero-length range");
    assert!(bytes.is_empty());
}

#[test]
fn test_zero_length_read() {
    for store in create_stores() {
        test_zero_length_read_impl(&store);
    }
}

/// Test converting a writable stream to a `std::io::Read` and verifying data integrity.
fn test_into_std_read_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut writable = store
        .allocate_writable(None)
        .expect("Failed to allocate writable");
    let data = b"Standard Read data.";
    writable.write_all(data).expect("Failed to write data");

    let mut std_reader = writable
        .into_reader()
        .expect("Failed to convert to std::io::Read");
    let mut read_data = Vec::new();
    std_reader
        .read_to_end(&mut read_data)
        .expect("Failed to read data");

    assert_eq!(&read_data[..], data);
}

#[test]
fn test_into_std_read() {
    for store in create_stores() {
        test_into_std_read_impl(&store);
    }
}

/// Test writing and reading with non-aligned positions to ensure flexibility.
fn test_non_aligned_positions_impl(store: &Arc<dyn TemporaryFileStore>) {
    let mut buffer = store
        .allocate_buffer(None)
        .expect("Failed to allocate buffer");

    // Write "Rustaceans" starting at position 3
    buffer
        .write_at(3, b"Rustaceans")
        .expect("Failed to write 'Rustaceans'");

    let reader = buffer
        .into_read_at()
        .expect("Failed to convert to ReadAtBlocking");
    let bytes = reader.read_at(0..13).expect("Failed to read data");

    let expected = b"\x00\x00\x00Rustaceans";
    assert_eq!(&bytes[..], expected);
}

#[test]
fn test_non_aligned_positions() {
    for store in create_stores() {
        test_non_aligned_positions_impl(&store);
    }
}
