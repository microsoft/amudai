use simdutf8;
use std::borrow::Cow;

/// Error type for UTF-8 validation
#[derive(Debug)]
pub struct Utf8Error;

/// The function returns a `&str` reference if the bytes slice `input` only
/// contains valid UTF-8 characters. Otherwise, the method returns `Err(Utf8Error)`.
#[inline]
pub fn from_utf8_fast(input: &[u8]) -> Result<&str, Utf8Error> {
    simdutf8::basic::from_utf8(input).map_err(|_| Utf8Error)
}

/// The function validates that bytes slice `input` only contains valid UTF-8
/// characters.
#[inline]
pub fn is_utf8_fast(input: &[u8]) -> bool {
    simdutf8::basic::from_utf8(input).is_ok()
}

/// The function interprets bytes slice `input` as `&str` reference in case the
/// slice only contains valid UTF-8 characters. Otherwise, replaces invalid UTF-8
/// characters with `char::REPLACEMENT_CHAR` (`\u{FFFD}`) and returns updated
/// string. Buffer `scratch` if provided, is used in case of erroneous UTF-8
/// content for storing updated string content. If `scratch` is not provided,
/// a new string will be allocated on heap to host the "fixed" string content.
///
/// The size of the `scratch` buffer must be at least triple the size of the
/// `input` slice (one invalid UTF-8 character of one byte size is replaced
/// with '\u{FFFD}').
///
/// ## Returns
///
/// The function returns a tuple containing:
///
///   - Reference to a valid UTF-8 string.
///   - Whether `input` contained invalid UTF-8 characters.
///
pub fn from_utf8_lossy_fast<'a>(
    mut input: &'a [u8],
    mut scratch: Option<&'a mut [u8]>,
) -> (Cow<'a, str>, bool) {
    const REPLACEMENT_CHAR: &[u8] = b"\xEF\xBF\xBD";
    const REPLACEMENT_CHAR_LEN: usize = REPLACEMENT_CHAR.len();

    match simdutf8::compat::from_utf8(input) {
        Ok(valid) => (Cow::Borrowed(valid), false),
        Err(error) => {
            let mut owned_scratch = Vec::new();
            let mut pos = 0usize;
            let mut ptr = if let Some(ref mut scratch) = scratch {
                assert!(
                    scratch.len() >= input.len() * 3,
                    "Scratch buffer is too small to accommodate replacement characters."
                );
                scratch.as_mut_ptr()
            } else {
                owned_scratch.resize(input.len() * 3, 0); // TODO: resize without re-initializing
                owned_scratch.as_mut_ptr()
            };

            let valid_len = error.valid_up_to();
            let (valid, after_valid) = input.split_at(valid_len);

            unsafe {
                std::ptr::copy_nonoverlapping(valid.as_ptr(), ptr, valid_len);
                pos += valid_len;
                ptr = ptr.add(valid_len);
                std::ptr::copy_nonoverlapping(REPLACEMENT_CHAR.as_ptr(), ptr, REPLACEMENT_CHAR_LEN);
                pos += REPLACEMENT_CHAR_LEN;
                ptr = ptr.add(REPLACEMENT_CHAR_LEN);
            }

            if let Some(invalid_sequence_length) = error.error_len() {
                input = &after_valid[invalid_sequence_length..];
                loop {
                    match simdutf8::compat::from_utf8(input) {
                        Ok(_) => {
                            let valid_len = input.len();
                            unsafe {
                                std::ptr::copy_nonoverlapping(input.as_ptr(), ptr, valid_len)
                            };
                            pos += valid_len;
                            break;
                        }
                        Err(error) => {
                            let valid_len = error.valid_up_to();
                            let (valid, after_valid) = input.split_at(valid_len);

                            unsafe {
                                std::ptr::copy_nonoverlapping(valid.as_ptr(), ptr, valid_len);
                                pos += valid_len;
                                ptr = ptr.add(valid_len);
                                std::ptr::copy_nonoverlapping(
                                    REPLACEMENT_CHAR.as_ptr(),
                                    ptr,
                                    REPLACEMENT_CHAR_LEN,
                                );
                                pos += REPLACEMENT_CHAR_LEN;
                                ptr = ptr.add(REPLACEMENT_CHAR_LEN);
                            }

                            if let Some(invalid_sequence_length) = error.error_len() {
                                input = &after_valid[invalid_sequence_length..]
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
            let result = match scratch {
                Some(s) => Cow::Borrowed(unsafe { std::str::from_utf8_unchecked(&s[0..pos]) }),
                None => {
                    unsafe { owned_scratch.set_len(pos) };
                    Cow::Owned(unsafe { String::from_utf8_unchecked(owned_scratch) })
                }
            };
            (result, true)
        }
    }
}

/// Converts a UTF-8 buffer to valid UTF-8, replacing invalid sequences with replacement characters.
///
/// ## Safety
///
/// This function is unsafe because it dereferences raw pointers. The caller must ensure:
/// - `src` is valid for reads of `src_len` bytes
/// - `buf` is valid for reads and writes of `buf_len` bytes
/// - `dst` and `dst_len` are valid for writes
/// - `buf_len` is at least 3 times `src_len` to accommodate replacement characters
/// - All pointers are properly aligned and non-null
pub unsafe fn convert_to_utf8_lossy(
    src: *const u8,
    src_len: usize,
    buf: *mut u8,
    buf_len: usize,
    dst: *mut *const u8,
    dst_len: *mut u64,
) {
    unsafe {
        let input = std::slice::from_raw_parts(src, src_len);
        let scratch = std::slice::from_raw_parts_mut(buf, buf_len);
        let (res, _) = from_utf8_lossy_fast(input, Some(scratch));
        *dst = res.as_ptr();
        *dst_len = res.len() as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_utf8_lossy_fast() {
        let mut scratch = [0u8; 1024];

        assert_eq!(
            (Cow::Borrowed(""), false),
            from_utf8_lossy_fast(b"", Some(&mut scratch))
        );

        let input = "אבגדהוזחטיכלמנןסעפףצץקרשת абвгдежзиклмнопрстуфхцчшщъыьэюя";
        let output = from_utf8_lossy_fast(input.as_bytes(), Some(&mut scratch));
        assert_eq!((Cow::Borrowed(input), false), output);

        let input = b"Hello \xF0\x90\x80World\xF0\x90\x80\xF0\x90\x80!";
        let output = from_utf8_lossy_fast(input, None);
        assert_eq!((Cow::Owned(String::from("Hello �World��!")), true), output);

        let input = b"\xa0\xa0\xa0\xa0\xa0\xa0\xa0";
        let output = from_utf8_lossy_fast(input, Some(&mut scratch));
        assert_eq!((Cow::Borrowed("�������"), true), output);
    }
}
