//! Trivial Tokenizer - returns the input unchanged.

use std::iter;

use super::{DEFAULT_MAX_TERM_LENGTH, DEFAULT_MIN_TERM_LENGTH, Tokenizer, truncate_str};
use crate::tokenizers::TokenizerKind;

/// Trivial Tokenizer - returns the input unchanged or truncated.
///
/// This tokenizer doesn't extract any terms; it simply returns the raw value
/// of the field. If the input exceeds the maximum length, it is truncated at
/// UTF-8 character boundaries. If the input is shorter than the minimum length
/// or empty, no token is returned. This makes it highly efficient for field
/// equality and prefix searches, though it sacrifices flexibility. It's
/// particularly well-suited for fields similar to GUIDs.
pub struct TrivialTokenizer {
    max_term_length: usize,
    min_term_length: usize,
}

impl TrivialTokenizer {
    /// Create a new TrivialTokenizer with custom max and min term lengths.
    #[allow(dead_code)]
    pub fn with_lengths(max_term_length: usize, min_term_length: usize) -> Self {
        Self {
            max_term_length,
            min_term_length,
        }
    }

    /// Create a new TrivialTokenizer with default settings.
    pub fn new() -> Self {
        Self {
            max_term_length: DEFAULT_MAX_TERM_LENGTH,
            min_term_length: DEFAULT_MIN_TERM_LENGTH,
        }
    }
}

/// Provides default construction for TrivialTokenizer using standard settings.
impl Default for TrivialTokenizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Tokenizer for TrivialTokenizer {
    type TokenIter<'a> = iter::Take<iter::Once<&'a str>>;

    fn tokenize<'a>(&'a self, input: &'a str) -> Self::TokenIter<'a> {
        if input.is_empty() || input.len() < self.min_term_length {
            iter::once("").take(0)
        } else {
            iter::once(truncate_str(input, self.max_term_length)).take(1)
        }
    }

    fn kind(&self) -> TokenizerKind {
        TokenizerKind::Trivial
    }

    fn max_term_length(&self) -> usize {
        self.max_term_length
    }

    fn min_term_length(&self) -> usize {
        self.min_term_length
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trivial_tokenizer() {
        let tokenizer = TrivialTokenizer::new();

        // Returns input unchanged
        let terms: Vec<&str> = tokenizer.tokenize("guid-12345-abcdef").collect();
        assert_eq!(terms, vec!["guid-12345-abcdef"]);

        // Empty string
        let terms: Vec<&str> = tokenizer.tokenize("").collect();
        assert_eq!(terms, Vec::<&str>::new());

        // Complex string returned as-is
        let input = "This is a complex string with spaces and punctuation!";
        let terms: Vec<&str> = tokenizer.tokenize(input).collect();
        assert_eq!(terms, vec![input]);

        // Test name
        assert_eq!(tokenizer.name(), "trivial");
    }

    #[test]
    fn test_min_term_length_filtering() {
        let tokenizer = TrivialTokenizer::new();

        // Use the trait methods to test the defaults
        assert_eq!(tokenizer.min_term_length(), 1);
        assert_eq!(tokenizer.max_term_length(), 128);

        // Test actual filtering - empty string should be excluded
        let terms: Vec<&str> = tokenizer.tokenize("").collect();
        assert_eq!(terms, Vec::<&str>::new()); // empty string should be excluded

        // Test normal input
        let terms: Vec<&str> = tokenizer.tokenize("test").collect();
        assert_eq!(terms, vec!["test"]);
    }

    #[test]
    fn test_trivial_truncation() {
        // Test truncation of long input
        // Create a tokenizer with small max length for testing
        let small_tokenizer = TrivialTokenizer::with_lengths(5, 1);

        // Long input should be truncated
        let terms: Vec<&str> = small_tokenizer.tokenize("this-is-very-long").collect();
        assert_eq!(terms, vec!["this-"]); // truncated to 5 bytes

        // Short input should pass through
        let terms: Vec<&str> = small_tokenizer.tokenize("test").collect();
        assert_eq!(terms, vec!["test"]);

        // Test Unicode truncation at character boundaries
        let terms: Vec<&str> = small_tokenizer.tokenize("café-test").collect();
        assert_eq!(terms, vec!["café"]); // "café-test" is longer than 5 bytes, truncated to "café" (5 bytes)
    }

    #[test]
    fn test_configurable_min_max_lengths() {
        // Test tokenizer with custom min and max term lengths
        let tokenizer = TrivialTokenizer::with_lengths(10, 3);

        // Test that it uses the custom lengths
        assert_eq!(tokenizer.max_term_length(), 10);
        assert_eq!(tokenizer.min_term_length(), 3);

        // Input shorter than min_term_length should be excluded
        let terms: Vec<&str> = tokenizer.tokenize("ab").collect();
        assert_eq!(terms, Vec::<&str>::new());

        // Input equal to min_term_length should be included
        let terms: Vec<&str> = tokenizer.tokenize("abc").collect();
        assert_eq!(terms, vec!["abc"]);

        // Input longer than max_term_length should be truncated
        let terms: Vec<&str> = tokenizer.tokenize("this-is-very-long-text").collect();
        assert_eq!(terms, vec!["this-is-ve"]); // truncated to 10 bytes

        // Test that default constructor works
        let default_tokenizer = TrivialTokenizer::new();
        assert_eq!(default_tokenizer.max_term_length(), 128);
        assert_eq!(default_tokenizer.min_term_length(), 1);
    }

    #[test]
    fn test_trivial_tokenizer_with_unicode() {
        let tokenizer = TrivialTokenizer::new();

        // Trivial tokenizer should return the entire input unchanged
        let chinese_input = "这是中文测试";
        let chinese_terms: Vec<&str> = tokenizer.tokenize(chinese_input).collect();
        assert_eq!(chinese_terms, vec![chinese_input]);

        let russian_input = "Это русский тест";
        let russian_terms: Vec<&str> = tokenizer.tokenize(russian_input).collect();
        assert_eq!(russian_terms, vec![russian_input]);

        let german_input = "Das ist ein deutscher Test mit Umlauten: äöü";
        let german_terms: Vec<&str> = tokenizer.tokenize(german_input).collect();
        assert_eq!(german_terms, vec![german_input]);

        // Test mixed multilingual input
        let mixed_input = "Hello 你好 Привет Hallo";
        let mixed_terms: Vec<&str> = tokenizer.tokenize(mixed_input).collect();
        assert_eq!(mixed_terms, vec![mixed_input]);
    }
}
