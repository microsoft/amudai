/// Converts UTF-8 characters stored in a buffer referenced by `src` to lower case,
/// and stores the result in a buffer referenced by `dst`.
///
/// ## Assumptions
///
///  * Target buffer size must be at least source buffer size * 3.
///  * The function assumes that `src` contains valid UTF-8 string.
///
/// ## Returns
///
/// The function returns the number of bytes written to `dst`.
///
/// ## Safety
///
/// This function is unsafe because it dereferences raw pointers. The caller must ensure:
/// - `src` is valid for reads of `src_len` bytes
/// - `dst` is valid for writes of `dst_len` bytes  
/// - `dst_len` is at least `src_len * 3`
/// - `src` contains valid UTF-8 data
pub unsafe fn utf8_tolower(src: *const u8, src_len: u64, dst: *mut u8, dst_len: u64) -> u64 {
    debug_assert!(dst_len >= src_len * 3);
    let src = unsafe { std::slice::from_raw_parts(src, src_len as usize) };
    let dst = unsafe { std::slice::from_raw_parts_mut(dst, dst_len as usize) };
    let mut idx: usize = 0;
    let utf8_str = unsafe { std::str::from_utf8_unchecked(src) };
    utf8_str.chars().for_each(|ch| {
        let lc = ch.to_lowercase_ignore_special();
        idx += lc.encode_utf8(&mut dst[idx..]).len();
    });
    idx as u64
}

/// Converts UTF-8 characters stored in a buffer referenced by `src` to upper case, and stores
/// the result in a buffer referenced by `dst`.
///
/// ## Assumptions
///
///  * Target buffer size must be at least source buffer size * 3.
///  * The function assumes that `src` contains valid UTF-8 string.
///
/// ## Returns
///
/// The function returns the number of bytes written to `dst`.
///
/// ## Safety
///
/// This function is unsafe because it dereferences raw pointers. The caller must ensure:
/// - `src` is valid for reads of `src_len` bytes
/// - `dst` is valid for writes of `dst_len` bytes  
/// - `dst_len` is at least `src_len * 3`
/// - `src` contains valid UTF-8 data
pub unsafe fn utf8_toupper(src: *const u8, src_len: u64, dst: *mut u8, dst_len: u64) -> u64 {
    debug_assert!(dst_len >= src_len * 3);
    let src = unsafe { std::slice::from_raw_parts(src, src_len as usize) };
    let dst = unsafe { std::slice::from_raw_parts_mut(dst, dst_len as usize) };
    let mut idx: usize = 0;
    let utf8_str = unsafe { std::str::from_utf8_unchecked(src) };
    utf8_str.chars().for_each(|ch| {
        let uc = ch.to_uppercase_ignore_special();
        idx += uc.encode_utf8(&mut dst[idx..]).len();
    });
    idx as u64
}

include!(concat!(env!("OUT_DIR"), "/case_mapping.rs"));

/// Implementation of `char::to_lowercase()` and `char::to_uppercase`, which support conversion
/// of 'ß' (lower Eszett) into 'ẞ' (upper Eszett, which officially exists since 2017) and visa
/// versa.
///
/// In addition, the new iterator only returns a single character of the opposite case,
/// which means that for the following special casing characters of Unicode, the
/// implementation should return the same character in the opposite case:
///
/// <pre>
/// İ ŉ ǰ ΐ ΰ և ẖ ẗ ẘ ẙ ẚ ὐ ὒ ὔ ὖ ᾀ ᾁ ᾂ ᾃ ᾄ ᾅ ᾆ ᾇ ᾈ ᾉ ᾊ ᾋ ᾌ ᾍ ᾎ ᾏ ᾐ ᾑ ᾒ ᾓ ᾔ ᾕ ᾖ ᾗ
/// ᾘ ᾙ ᾚ ᾛ ᾜ ᾝ ᾞ ᾟ ᾠ ᾡ ᾢ ᾣ ᾤ ᾥ ᾦ ᾧ ᾨ ᾩ ᾪ ᾫ ᾬ ᾭ ᾮ ᾯ ᾲ ᾳ ᾴ ᾶ ᾷ ᾼ ῂ ῃ ῄ ῆ ῇ ῌ ῒ ΐ ῖ
/// ῗ ῢ ΰ ῤ ῦ ῧ ῲ ῳ ῴ ῶ ῷ ῼ ﬀ ﬁ ﬂ ﬃ ﬄ ﬅ ﬆ ﬓ ﬔ ﬕ ﬖ ﬗ
/// </pre>
pub trait CharCaseMapper {
    fn to_lowercase_ignore_special(&self) -> Self;
    fn to_uppercase_ignore_special(&self) -> Self;
}

impl CharCaseMapper for char {
    fn to_lowercase_ignore_special(&self) -> Self {
        let idx = u32::from(*self) as usize;
        if idx < case_mapping::TOLOWER_MAP_LEN {
            unsafe {
                let lc = *case_mapping::TOLOWER_MAP.get_unchecked(idx);
                if lc != 0 {
                    return char::from_u32_unchecked(lc);
                }
            }
        }
        *self
    }

    fn to_uppercase_ignore_special(&self) -> Self {
        let idx = u32::from(*self) as usize;
        if idx < case_mapping::TOUPPER_MAP_LEN {
            unsafe {
                let lc = *case_mapping::TOUPPER_MAP.get_unchecked(idx);
                if lc != 0 {
                    return char::from_u32_unchecked(lc);
                }
            }
        }
        *self
    }
}

/// Custom implementation of `str::to_lowercase` and `str::to_uppercase`, which
/// is consistent with behavior of applying `char::to_lowercase_ignore_special`
/// or `char::to_uppercase_ignore_special` over iterator of chars.
pub trait StringCaseMapper {
    fn to_lowercase_ignore_special(&self) -> String;
    fn to_uppercase_ignore_special(&self) -> String;
}

impl StringCaseMapper for str {
    fn to_lowercase_ignore_special(&self) -> String {
        let mut s = String::with_capacity(self.len());
        for ch in self.chars() {
            s.push(ch.to_lowercase_ignore_special())
        }
        s
    }

    fn to_uppercase_ignore_special(&self) -> String {
        let mut s = String::with_capacity(self.len());
        for ch in self.chars() {
            s.push(ch.to_uppercase_ignore_special())
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use crate::case_conversions::{CharCaseMapper, StringCaseMapper};

    #[test]
    pub fn test_to_uppercase_ignore_special() {
        assert_eq!('ẞ', 'ß'.to_uppercase_ignore_special());
        assert_eq!("STRAẞE", "Straße".to_uppercase_ignore_special().as_str());
        assert_eq!("אבגדה", "אבגדה".to_uppercase_ignore_special().as_str());
        assert_eq!("", "".to_uppercase_ignore_special().as_str());
        assert_eq!(
            "🐥 AND 🐓 ARE FUNNY",
            "🐥 and 🐓 are funny".to_uppercase_ignore_special().as_str()
        );
    }

    #[test]
    pub fn to_lowercase_ignore_special() {
        assert_eq!('ß', 'ẞ'.to_lowercase_ignore_special());
        assert_eq!("straße", "STRAẞE".to_lowercase_ignore_special().as_str());
        assert_eq!("אבגדה", "אבגדה".to_lowercase_ignore_special().as_str());
        assert_eq!("", "".to_lowercase_ignore_special().as_str());
        assert_eq!(
            "🐥 and 🐓 are funny",
            "🐥 And 🐓 are funny".to_lowercase_ignore_special().as_str()
        );
    }
}
