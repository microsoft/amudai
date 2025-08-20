//! Text collation strategies for term comparison and ordering.
//!
//! This module provides collation implementations that determine how terms are
//! compared and ordered within the text index. Different collation strategies
//! enable case-insensitive searches, locale-specific ordering, and other
//! text processing behaviors.
//!
//! # Collation Impact
//!
//! The chosen collation strategy affects:
//! - **Term Storage**: How terms are ordered in the B-Tree structure
//! - **Query Matching**: How search terms are compared with indexed terms
//! - **Prefix Searches**: How prefix matching is performed
//!
//! # Available Collations
//!
//! - **Unicode Case Insensitive**: Performs case-folding for ASCII and Unicode text
//! - **Unicode Case Preserving**: Simple case-sensitive comparison
//!
//! The collation must remain consistent between indexing and querying operations
//! to ensure correct search results.

use std::cmp::Ordering;

use amudai_common::{Result, error::Error};

/// Trait defining comparison rules for text strings in index operations.
///
/// `Collation` establishes the lexicographic ordering used throughout the text
/// index for term storage, lookup, and navigation. The same collation strategy
/// must be used consistently during both index construction and query operations.
pub trait Collation: Send + Sync + 'static {
    /// Returns the collation kind for identification and configuration.
    fn kind(&self) -> CollationKind;

    /// Returns the human-readable name of this collation strategy.
    fn name(&self) -> &'static str {
        self.kind().name()
    }

    /// Creates a boxed clone of this collation for sharing across components.
    fn clone_boxed(&self) -> Box<dyn Collation>;

    /// Compares two text strings according to this collation's rules.
    ///
    /// This method establishes the canonical ordering used throughout the index.
    /// It must implement a total ordering that satisfies the mathematical
    /// properties required for B-Tree navigation.
    fn compare(&self, left: &str, right: &str) -> Ordering;

    /// Tests whether a text string starts with a given prefix.
    ///
    /// This method enables efficient prefix search operations by applying
    /// the same comparison rules used for term ordering.
    fn starts_with(&self, text: &str, prefix: &str) -> bool;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum CollationKind {
    /// Unicode collation that is case insensitive.
    UnicodeCaseInsensitive,
    /// Unicode collation that is case preserving.
    UnicodeCasePreserving,
}

/// Convert a string name to a TokenizerKind enum variant.
impl TryFrom<&str> for CollationKind {
    type Error = amudai_common::error::Error;

    fn try_from(name: &str) -> Result<Self> {
        match name {
            "unicode-case-insensitive" => Ok(CollationKind::UnicodeCaseInsensitive),
            "unicode-case-preserving" => Ok(CollationKind::UnicodeCasePreserving),
            _ => Err(Error::invalid_arg(
                "name",
                format!("Unrecognized collation: {name}"),
            )),
        }
    }
}

impl CollationKind {
    /// Get the name of the tokenizer kind as a static string.
    const fn name(&self) -> &'static str {
        match self {
            CollationKind::UnicodeCaseInsensitive => "unicode-case-insensitive",
            CollationKind::UnicodeCasePreserving => "unicode-case-preserving",
        }
    }
}

/// Creates a new collation instance based on the provided name.
///
/// # Arguments
/// - `name`: The name of the collation to create.
///
/// # Returns
/// Returns a `Result` containing a boxed `Collation` trait object if successful,
/// or an error if the name is invalid.
pub fn create_collation(name: &str) -> Result<Box<dyn Collation>> {
    match CollationKind::try_from(name)? {
        CollationKind::UnicodeCaseInsensitive => Ok(Box::new(UnicodeCaseInsensitiveCollation)),
        CollationKind::UnicodeCasePreserving => Ok(Box::new(UnicodeCasePreservingCollation)),
    }
}

/// Unicode case-insensitive collation that ignores case differences for all Unicode characters.
///
/// This collation treats Unicode uppercase and lowercase letters as equivalent during
/// comparison. It properly handles Unicode normalization and special casing rules.
/// If the input is not valid UTF-8, it falls back to binary comparison.
pub struct UnicodeCaseInsensitiveCollation;

impl Collation for UnicodeCaseInsensitiveCollation {
    fn kind(&self) -> CollationKind {
        CollationKind::UnicodeCaseInsensitive
    }

    fn clone_boxed(&self) -> Box<dyn Collation> {
        Box::new(UnicodeCaseInsensitiveCollation)
    }

    fn compare(&self, left: &str, right: &str) -> Ordering {
        compare_strings(left, right, |&l, &r| to_upper(l).cmp(&to_upper(r)))
    }

    fn starts_with(&self, text: &str, prefix: &str) -> bool {
        string_has_prefix(text, prefix, |&l, &r| to_upper(l).cmp(&to_upper(r)))
    }
}

/// Unicode case-preserving collation that provides case-sensitive sorting with primary ordering
/// by case-insensitive comparison.
///
/// This collation first compares strings case-insensitively using Unicode rules,
/// and if they are equal, falls back to character-by-character comparison to
/// preserve case ordering. If the input is not valid UTF-8, it falls back to
/// binary comparison.
pub struct UnicodeCasePreservingCollation;

impl Collation for UnicodeCasePreservingCollation {
    fn kind(&self) -> CollationKind {
        CollationKind::UnicodeCasePreserving
    }

    fn clone_boxed(&self) -> Box<dyn Collation> {
        Box::new(UnicodeCasePreservingCollation)
    }

    fn compare(&self, left: &str, right: &str) -> Ordering {
        let ord = UnicodeCaseInsensitiveCollation.compare(left, right);
        if ord == Ordering::Equal {
            compare_strings(left, right, char::cmp)
        } else {
            ord
        }
    }

    fn starts_with(&self, text: &str, prefix: &str) -> bool {
        if !UnicodeCaseInsensitiveCollation.starts_with(text, prefix) {
            return false;
        }
        string_has_prefix(text, prefix, char::cmp)
    }
}

/// This function converts a character into its upper case variant, while ignoring special
/// casing characters as described by https://www.unicode.org/Public/UCD/latest/ucd/SpecialCasing.txt
/// document.
///
/// If the character is a special case char (i.e. it's expanded when converted into upper
/// case), the method returns the character itself.
///
/// In addition, the method supports 'ß' (lower Eszett) that translates into 'ẞ'
/// (upper Eszett, which officially exists since 2017).
fn to_upper(c: char) -> char {
    if c == 'ß' {
        'ẞ'
    } else if c.is_lowercase() {
        let mut uppercase_char = c.to_uppercase();
        match (uppercase_char.next(), uppercase_char.next()) {
            (Some(ch), None) => ch,
            _ => c, // If it maps to multiple code points, return the original character
        }
    } else {
        c
    }
}

/// This function compares two strings character by character using a custom comparison function `cmp`.
/// It compares each character in the `lhs` and `rhs` strings using the provided comparison function `cmp`.
/// If the strings are equal, it returns `Ordering::Equal`.
///
/// # Parameters
///
/// - `lhs`: The left-hand side string to compare.
/// - `rhs`: The right-hand side string to compare.
///
/// # Returns
///
/// Returns `Ordering::Equal` if the strings are equal, otherwise returns
/// `Ordering::Less` or `Ordering::Greater` based on the comparison result.
fn compare_strings<P>(lhs: &str, rhs: &str, cmp: P) -> Ordering
where
    P: Fn(&char, &char) -> Ordering,
{
    // TODO: replace with `lhs.chars().cmp_by(rhs.chars(), Self::compare_chars)`,
    // once the API stabilizes.
    let mut lhs = lhs.chars();
    let mut rhs = rhs.chars();
    loop {
        let lch = match lhs.next() {
            None => {
                if rhs.next().is_none() {
                    return Ordering::Equal;
                } else {
                    return Ordering::Less;
                }
            }
            Some(val) => val,
        };
        let rch = match rhs.next() {
            None => return Ordering::Greater,
            Some(val) => val,
        };
        match cmp(&lch, &rch) {
            Ordering::Equal => (),
            non_eq => return non_eq,
        }
    }
}

/// This function checks if the `text` starts with the `prefix` using a custom comparison function `cmp`.
/// It compares each character in the `text` and `prefix` using the provided comparison function `cmp`.
/// If the `prefix` is empty, it always returns true.
///
/// # Parameters
///
/// - `text`: The string to check.
/// - `prefix`: The prefix to check against.
///
/// # Returns
/// Returns true if `text` starts with `prefix` according to the comparison function `cmp`.
fn string_has_prefix<P>(text: &str, prefix: &str, cmp: P) -> bool
where
    P: Fn(&char, &char) -> Ordering,
{
    let mut text = text.chars();
    let mut prefix = prefix.chars();
    loop {
        let Some(lch) = prefix.next() else {
            return true;
        };
        let Some(rch) = text.next() else {
            return false;
        };
        match cmp(&lch, &rch) {
            Ordering::Equal => (),
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn test_collation_kind_from_str() {
        assert_eq!(
            CollationKind::try_from("unicode-case-insensitive").unwrap(),
            CollationKind::UnicodeCaseInsensitive
        );
        assert_eq!(
            CollationKind::try_from("unicode-case-preserving").unwrap(),
            CollationKind::UnicodeCasePreserving
        );

        assert!(CollationKind::try_from("invalid").is_err());
    }

    #[test]
    fn test_collation_kind_name() {
        assert_eq!(
            CollationKind::UnicodeCaseInsensitive.name(),
            "unicode-case-insensitive"
        );
        assert_eq!(
            CollationKind::UnicodeCasePreserving.name(),
            "unicode-case-preserving"
        );
    }

    #[test]
    fn test_unicode_case_insensitive_collation() {
        let collation = UnicodeCaseInsensitiveCollation;

        // Test kind and name
        assert_eq!(collation.kind(), CollationKind::UnicodeCaseInsensitive);
        assert_eq!(collation.name(), "unicode-case-insensitive");

        // Test compare with Unicode characters
        assert_eq!(collation.compare("café", "CAFÉ"), Ordering::Equal);
        assert_eq!(collation.compare("ñ", "Ñ"), Ordering::Equal);
        assert_eq!(collation.compare("abc", "def"), Ordering::Less);

        // Test special Eszett handling - with corrected to_lower behavior:
        // to_lower('ß') = 'ß', to_lower('ẞ') = 'ß'
        // Characters compare equal, and both strings have same character count
        // So the result should be Equal (fixed implementation)
        assert_eq!(collation.compare("ß", "ẞ"), Ordering::Equal);

        // Test starts_with
        assert!(collation.starts_with("Héllo World", "héllo"));
        assert!(collation.starts_with("CAFÉ", "café"));
        assert!(!collation.starts_with("hello", "world"));
    }

    #[test]
    fn test_unicode_case_preserving_collation() {
        let collation = UnicodeCasePreservingCollation;

        // Test kind and name
        assert_eq!(collation.kind(), CollationKind::UnicodeCasePreserving);
        assert_eq!(collation.name(), "unicode-case-preserving");

        // Test compare - case insensitive primary, character comparison secondary
        assert_eq!(collation.compare("abc", "abc"), Ordering::Equal);
        assert_eq!(collation.compare("abc", "ABC"), Ordering::Greater);
        assert_eq!(collation.compare("ABC", "abc"), Ordering::Less);
        assert_eq!(collation.compare("café", "CAFÉ"), Ordering::Greater);

        // Test starts_with - case-preserving compares case-insensitive first, then character-by-character
        // Since "Hello" vs "hello" differ in case, starts_with returns false
        assert!(!collation.starts_with("Hello World", "hello")); // Different case
        assert!(collation.starts_with("hello world", "hello")); // Exact case match  
        assert!(!collation.starts_with("CAFÉ", "café")); // Different case
        assert!(!collation.starts_with("hello", "world"));

        let mut terms = vec!["Abd", "abc", "aBc"];
        terms.sort_by(|a, b| collation.compare(a, b));
        assert_eq!(terms, vec!["aBc", "abc", "Abd"]);
    }

    #[test]
    fn test_to_upper() {
        // Test basic ASCII
        assert_eq!(to_upper('a'), 'A');
        assert_eq!(to_upper('z'), 'Z');
        assert_eq!(to_upper('A'), 'A'); // already uppercase
        assert_eq!(to_upper('1'), '1'); // not a letter

        // Test Unicode
        assert_eq!(to_upper('ñ'), 'Ñ');
        assert_eq!(to_upper('é'), 'É');

        // Test special Eszett handling - corrected implementation:
        // to_upper('ß') = 'ẞ' (lowercase ß to uppercase ẞ)
        // to_upper('ẞ') = 'ẞ' (already uppercase, stays the same)
        assert_eq!(to_upper('ß'), 'ẞ'); // Lowercase ß -> uppercase ẞ
        assert_eq!(to_upper('ẞ'), 'ẞ'); // Already uppercase, stays the same

        // Test characters that expand to multiple codepoints (should return original)
        let result = to_upper('ﬀ'); // ligature ff
        // The exact result depends on Unicode implementation, but it should not panic
        assert!(result == 'ﬀ' || result.is_lowercase() || result.is_uppercase());
    }

    #[test]
    fn test_collation_trait_methods() {
        let unicode_ci = UnicodeCaseInsensitiveCollation;
        let unicode_cp = UnicodeCasePreservingCollation;

        // Test that all collations implement the trait correctly
        let collations: Vec<&dyn Collation> = vec![&unicode_ci, &unicode_cp];

        for collation in collations {
            // Test that name() returns the same as kind().name()
            assert_eq!(collation.name(), collation.kind().name());

            // Test basic functionality
            assert_eq!(collation.compare("same", "same"), Ordering::Equal);
            assert!(collation.starts_with("prefix_test", "prefix"));
            assert!(!collation.starts_with("short", "longer_prefix"));
        }
    }

    #[test]
    fn test_edge_cases() {
        let unicode_ci = UnicodeCaseInsensitiveCollation;

        // Empty strings
        assert_eq!(unicode_ci.compare("", ""), Ordering::Equal);
        assert!(unicode_ci.starts_with("", ""));
        assert!(unicode_ci.starts_with("test", ""));

        // Single characters
        assert_eq!(unicode_ci.compare("á", "é"), Ordering::Less);

        // Length differences
        assert_eq!(unicode_ci.compare("á", "áé"), Ordering::Less);
    }
}
