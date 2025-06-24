//! Shared filesystem utilities for amudai-io-impl.
//!
//! This module provides common functionality used across the filesystem
//! implementation components, including temporary file name generation
//! and other filesystem-related utilities.

use std::ffi::OsString;

/// Generates a temporary file name with a random alphanumeric suffix.
///
/// Creates a temporary file name that starts with ".tmp" followed by
/// a randomly generated alphanumeric string of the specified length.
///
/// # Arguments
///
/// * `len` - The length of the random alphanumeric suffix to append
///           after the ".tmp" prefix
///
/// # Returns
///
/// An `OsString` containing the generated temporary file name in the
/// format ".tmp{random_string}" where `random_string` is `len` characters long.
pub fn generate_temp_file_name(len: usize) -> OsString {
    let capacity = len + 4;
    let mut buf = OsString::with_capacity(capacity);
    buf.push(".tmp");
    let mut rng = fastrand::Rng::new();
    let mut char_buf = [0u8; 4];
    for c in std::iter::repeat_with(|| rng.alphanumeric()).take(len) {
        buf.push(c.encode_utf8(&mut char_buf));
    }
    buf
}
