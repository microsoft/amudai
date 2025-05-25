use std::io::Write;

use amudai_common::verify_arg;

use crate::defs::{CHECKSUM_SIZE, MESSAGE_LEN_SIZE};

/// Validates a message by checking its size and checksum.
///
/// The function expects the message to be at least 8 bytes long. It extracts
/// the size of the payload from the first 4 bytes, verifies that the message
/// contains enough bytes for the payload and the 4-byte checksum, and then
/// validates the checksum of the payload.
///
/// # Arguments
///
/// * `message` - A byte slice representing the message to be validated. The message
///   is expected to contain a 4-byte size prefix, followed by the payload, and a 4-byte
///   checksum at the end.
///
/// # Returns
///
/// Returns a `Result` containing a byte slice of the payload if the message is
/// valid, or an error if the validation fails.
///
/// # Errors
///
/// Returns an error if the message is too short, if the size is invalid, or if
/// the checksum does not match.
pub fn validate_message(message: &[u8]) -> amudai_common::Result<&[u8]> {
    verify_arg!(message, message.len() >= 8);
    let size = u32::from_le_bytes(message[0..4].try_into().expect("size bytes")) as usize;
    verify_arg!(size, size + MESSAGE_LEN_SIZE <= message.len());
    let message = &message[MESSAGE_LEN_SIZE..];
    let payload = &message[..size];
    let checksum = u32::from_le_bytes(
        message[size..size + CHECKSUM_SIZE]
            .try_into()
            .expect("checksum bytes"),
    );
    validate_buffer(payload, checksum, Some("message"))?;
    Ok(payload)
}

/// Validates a buffer by comparing its computed checksum with the provided checksum.
///
/// # Arguments
///
/// * `buf` - A byte slice representing the buffer to be validated.
/// * `checksum` - The expected checksum.
/// * `name` - An optional name of the element being validated, used for error reporting.
///
/// # Returns
///
/// Returns a `Result` indicating success if the checksum matches, or an error
/// if it does not.
///
/// # Errors
///
/// Returns an error if the computed checksum does not match the provided checksum.
pub fn validate_buffer(buf: &[u8], checksum: u32, name: Option<&str>) -> amudai_common::Result<()> {
    use amudai_common::error::ErrorKind;

    let actual = compute(buf);
    if actual == checksum {
        Ok(())
    } else {
        Err(ErrorKind::ChecksumMismatch {
            element: name.unwrap_or_default().to_string(),
        }
        .into())
    }
}

/// Computes a checksum for a given buffer using the xxHash algorithm.
pub fn compute(buf: &[u8]) -> u32 {
    let h = xxhash_rust::xxh3::xxh3_64(buf);
    (h as u32) ^ ((h >> 32) as u32)
}

/// Constructs a valid message from a given payload and returns it as a `Vec<u8>`.
///
/// # Arguments
///
/// * `payload` - A slice of bytes representing the payload of the message.
///
/// # Returns
///
/// A `Vec<u8>` containing the constructed message, which includes:
/// - The size of the payload as a 4-byte little-endian integer.
/// - The payload itself.
/// - A 4-byte checksum of the payload.
pub fn create_message_vec(payload: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(MESSAGE_LEN_SIZE + payload.len() + CHECKSUM_SIZE);
    write_message(payload, &mut message).expect("write_message");
    message
}

/// Constructs a valid message from a given payload and writes it into a generic `Write`.
///
/// # Arguments
///
/// * `payload` - A slice of bytes representing the payload to be included in the message.
/// * `writer` - A mutable reference to an object implementing the `Write` trait, where the
///   message will be written.
///
/// # Returns
///
/// A `Result` indicating success or failure. On success, the message is written to the
/// provided writer.
pub fn write_message<W: Write>(payload: &[u8], writer: &mut W) -> std::io::Result<()> {
    let size = payload.len() as u32;
    let checksum = compute(payload);

    writer.write_all(&size.to_le_bytes())?;
    writer.write_all(payload)?;
    writer.write_all(&checksum.to_le_bytes())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use amudai_common::error::ErrorKind;

    #[test]
    fn test_validate_message_valid() {
        let payload = b"testdata";
        let message = create_message_vec(payload);

        let result = validate_message(&message);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), payload);
    }

    #[test]
    fn test_validate_message_invalid_length() {
        let message = b"short";

        let result = validate_message(message);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_message_invalid_size() {
        let payload = b"testdata";
        let checksum = compute(payload);
        let mut message = Vec::new();
        message.extend_from_slice(&(payload.len() as u32 + 10).to_le_bytes()); // Invalid size
        message.extend_from_slice(payload);
        message.extend_from_slice(&checksum.to_le_bytes());

        let result = validate_message(&message);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_message_invalid_checksum() {
        let payload = b"testdata";
        let checksum = compute(payload) ^ 1; // Incorrect checksum
        let mut message = Vec::new();
        message.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        message.extend_from_slice(payload);
        message.extend_from_slice(&checksum.to_le_bytes());

        let result = validate_message(&message);
        assert!(result.is_err());

        if let Err(e) = result {
            assert!(matches!(
                e.kind(),
                ErrorKind::ChecksumMismatch { element: _ }
            ));
        }
    }

    #[test]
    fn test_validate_buffer_valid() {
        let buf = b"testdata";
        let checksum = compute(buf);

        let result = validate_buffer(buf, checksum, Some("buffer"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_buffer_invalid_checksum() {
        let buf = b"testdata";
        let checksum = compute(buf) ^ 0x1000; // Incorrect checksum

        let result = validate_buffer(buf, checksum, Some("buffer"));
        assert!(result.is_err());

        if let Err(e) = result {
            assert!(matches!(
                e.kind(),
                ErrorKind::ChecksumMismatch { element: _ }
            ));
        }
    }

    #[test]
    fn test_compute() {
        let buf = b"testdata";
        let expected_checksum = compute(buf);

        let checksum = compute(buf);
        assert_eq!(checksum, expected_checksum);
    }
}
