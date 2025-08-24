use crate::case_conversions::CharCaseMapper;
use itertools::{EitherOrBoth, Itertools};

/// The function converts UTF-8 string `s1` to lowercase, and states whether its lowercase
/// variant equals to UTF-8 string `s2`.
#[inline]
pub fn str_utf8_lowercase_equal(s1: &str, s2: &str) -> bool {
    s1.chars()
        .map(|c| c.to_lowercase_ignore_special())
        .zip_longest(s2.chars())
        .all(|eb| match eb {
            EitherOrBoth::Both(c1, c2) => c1 == c2,
            _ => false,
        })
}

/// The function converts UTF-8 string `s1` to lowercase, and states whether its lowercase
/// variant is a prefix of UTF-8 string `s2`.
#[inline]
pub fn str_utf8_lowercase_is_prefix(s1: &str, s2: &str) -> bool {
    for eob in s1
        .chars()
        .map(|c| c.to_lowercase_ignore_special())
        .zip_longest(s2.chars())
    {
        match eob {
            EitherOrBoth::Both(c1, c2) => {
                if c1 != c2 {
                    return false;
                }
            }
            EitherOrBoth::Left(_) => return true, // s2 is exhausted
            EitherOrBoth::Right(_) => return false, // s2 is longer than s1
        }
    }
    true
}

/// The function converts UTF-8 string `s1` to lowercase, and states whether its lowercase
/// variant is a siffox of UTF-8 string `s2`.
#[inline]
pub fn str_utf8_lowercase_is_suffix(s1: &str, s2: &str) -> bool {
    for eob in s1
        .chars()
        .map(|c| c.to_lowercase_ignore_special())
        .rev()
        .zip_longest(s2.chars().rev())
    {
        match eob {
            EitherOrBoth::Both(c1, c2) => {
                if c1 != c2 {
                    return false;
                }
            }
            EitherOrBoth::Left(_) => return true, // s2 is exhausted
            EitherOrBoth::Right(_) => return false, // s2 is longer than s1
        }
    }
    true
}
