//! Tokenizers for extracting terms from text values.
//!
//! Tokenizers are used in two key scenarios:
//!
//! 1. **Index Creation**: When building an inverted text index, tokenizers extract
//!    searchable terms from document text values. Each unique term becomes an entry
//!    in the index, pointing to the documents that contain it.
//!
//! 2. **Query Processing**: When searching the index, tokenizers process query strings
//!    to extract the same terms that were used during indexing. This ensures consistent
//!    term extraction between indexing and querying phases.
//!
//! The tokenizers return iterators of string slices to avoid memory allocations during
//! the tokenization process.

pub mod trivial;
pub mod unicode_log;
pub mod unicode_word;

use amudai_common::{Result, error::Error};
pub use trivial::TrivialTokenizer;
pub use unicode_log::UnicodeLogTokenizer;
pub use unicode_word::UnicodeWordTokenizer;

/// Default maximum length of a single term in bytes before truncation
pub const DEFAULT_MAX_TERM_LENGTH: usize = 128;

/// Default minimum length of a single term in bytes
pub const DEFAULT_MIN_TERM_LENGTH: usize = 1;

/// A tokenizer extracts terms (tokens) from raw string values for indexing.
///
/// This trait returns an iterator of string slices to avoid memory allocations.
/// Terms longer than the maximum length are truncated at UTF-8 character boundaries.
/// Terms shorter than the minimum length are excluded entirely.
pub trait Tokenizer: Send + Sync {
    /// The iterator type returned by tokenize.
    type TokenIter<'a>: Iterator<Item = &'a str>
    where
        Self: 'a;

    /// Extract terms from the input string as an iterator of string slices.
    /// Returns an iterator that yields references to substrings of the input.
    fn tokenize<'a>(&'a self, input: &'a str) -> Self::TokenIter<'a>;

    /// Get the kind of the tokenizer.
    fn kind(&self) -> TokenizerKind;

    /// Get the name of the tokenizer kind as a static string.
    #[allow(dead_code)]
    fn name(&self) -> &'static str {
        self.kind().name()
    }

    /// Maximum length of a single term in bytes before exclusion.
    /// Terms longer than this will be excluded from the results entirely.
    fn max_term_length(&self) -> usize;

    /// Minimum length of a single term in bytes before exclusion.
    /// Terms shorter than this will be excluded from the results entirely.
    fn min_term_length(&self) -> usize;
}

/// Creates a tokenizer instance based on the provided name string.
///
/// This factory function provides a convenient way to create tokenizer instances
/// by name, which is useful for configuration-driven tokenizer selection.
///
/// # Arguments
/// * `name` - The name of the tokenizer to create (case-sensitive)
///
/// # Returns
/// * `Ok(TokenizerType)` - The requested tokenizer wrapped in a `TokenizerType` enum
/// * `Err(Error)` - If the tokenizer name is not recognized
///
/// # Errors
/// Returns an [`Error::invalid_arg`] if the provided tokenizer name is not recognized.
///
pub fn create_tokenizer(name: &str) -> Result<TokenizerType> {
    match name.try_into()? {
        TokenizerKind::Trivial => Ok(TokenizerType::Trivial(TrivialTokenizer::new())),
        TokenizerKind::UnicodeWord => Ok(TokenizerType::UnicodeWord(UnicodeWordTokenizer::new())),
        TokenizerKind::UnicodeLog => Ok(TokenizerType::UnicodeLog(UnicodeLogTokenizer::new())),
    }
}

/// Truncate a string slice to the maximum allowed length at a codepoint boundary.
/// Returns a subslice of the input that respects UTF-8 boundaries.
///
/// If the input is shorter than or equal to max_term_length, it is returned unchanged.
/// If the input is longer, it is truncated at the last valid UTF-8 character boundary
/// that fits within the limit. This ensures the result is always valid UTF-8.
pub(crate) fn truncate_str(input: &str, max_term_length: usize) -> &str {
    if input.len() <= max_term_length {
        return input;
    }

    // Find the last valid codepoint boundary within the limit
    let mut boundary = max_term_length;
    while boundary > 0 && !input.is_char_boundary(boundary) {
        boundary -= 1;
    }

    &input[..boundary]
}

/// Enum representing the different tokenizer kinds available.
/// This enum is used to identify the type of tokenizer being used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenizerKind {
    /// A tokenizer that does not perform any processing, simply returns the input as a single term.
    Trivial,
    /// A tokenizer that splits input into terms based on Unicode word boundaries.
    UnicodeWord,
    /// A tokenizer that splits input into terms based on Unicode word boundaries. In addition, it
    /// recognizes IPv4 addresses and treats them as single terms.
    UnicodeLog,
}

/// Convert a string name to a TokenizerKind enum variant.
impl TryFrom<&str> for TokenizerKind {
    type Error = amudai_common::error::Error;

    fn try_from(name: &str) -> Result<Self> {
        match name {
            "trivial" => Ok(TokenizerKind::Trivial),
            "unicode-word" => Ok(TokenizerKind::UnicodeWord),
            "unicode-log" => Ok(TokenizerKind::UnicodeLog),
            _ => Err(Error::invalid_arg(
                "name",
                format!("Unrecognized tokenizer: {name}"),
            )),
        }
    }
}

impl TokenizerKind {
    /// Get the name of the tokenizer kind as a static string.
    const fn name(&self) -> &'static str {
        match self {
            TokenizerKind::Trivial => "trivial",
            TokenizerKind::UnicodeWord => "unicode-word",
            TokenizerKind::UnicodeLog => "unicode-log",
        }
    }
}

/// Enum that holds all available tokenizer types.
/// This allows for dynamic dispatch while maintaining the iterator-based API.
pub enum TokenizerType {
    Trivial(TrivialTokenizer),
    UnicodeWord(UnicodeWordTokenizer),
    UnicodeLog(UnicodeLogTokenizer),
}

impl Tokenizer for TokenizerType {
    type TokenIter<'a> = Box<dyn Iterator<Item = &'a str> + 'a>;

    fn tokenize<'a>(&'a self, input: &'a str) -> Self::TokenIter<'a> {
        match self {
            TokenizerType::Trivial(tokenizer) => Box::new(tokenizer.tokenize(input)),
            TokenizerType::UnicodeWord(tokenizer) => Box::new(tokenizer.tokenize(input)),
            TokenizerType::UnicodeLog(tokenizer) => Box::new(tokenizer.tokenize(input)),
        }
    }

    fn kind(&self) -> TokenizerKind {
        match self {
            TokenizerType::Trivial(tokenizer) => tokenizer.kind(),
            TokenizerType::UnicodeWord(tokenizer) => tokenizer.kind(),
            TokenizerType::UnicodeLog(tokenizer) => tokenizer.kind(),
        }
    }

    fn max_term_length(&self) -> usize {
        match self {
            TokenizerType::Trivial(tokenizer) => tokenizer.max_term_length(),
            TokenizerType::UnicodeWord(tokenizer) => tokenizer.max_term_length(),
            TokenizerType::UnicodeLog(tokenizer) => tokenizer.max_term_length(),
        }
    }

    fn min_term_length(&self) -> usize {
        match self {
            TokenizerType::Trivial(tokenizer) => tokenizer.min_term_length(),
            TokenizerType::UnicodeWord(tokenizer) => tokenizer.min_term_length(),
            TokenizerType::UnicodeLog(tokenizer) => tokenizer.min_term_length(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_tokenizer() {
        assert!(create_tokenizer("unicode-word").is_ok());
        assert!(create_tokenizer("unicode-log").is_ok());
        assert!(create_tokenizer("trivial").is_ok());
        assert!(create_tokenizer("unknown").is_err());

        // Test that created tokenizers have correct names
        assert_eq!(
            create_tokenizer("unicode-word").unwrap().name(),
            "unicode-word"
        );
        assert_eq!(
            create_tokenizer("unicode-log").unwrap().name(),
            "unicode-log"
        );
        assert_eq!(create_tokenizer("trivial").unwrap().name(), "trivial");
    }

    #[test]
    fn test_term_truncation() {
        let long_term = "a".repeat(150);
        let truncated = truncate_str(&long_term, DEFAULT_MAX_TERM_LENGTH);
        assert_eq!(truncated.len(), DEFAULT_MAX_TERM_LENGTH);

        // Test with Unicode characters
        let unicode_term = "café".repeat(50); // Should be > 128 bytes
        let truncated = truncate_str(&unicode_term, DEFAULT_MAX_TERM_LENGTH);
        assert!(truncated.len() <= DEFAULT_MAX_TERM_LENGTH);
        assert!(truncated.is_char_boundary(truncated.len()));
    }

    #[test]
    fn test_unicode_truncation_at_character_boundaries() {
        // Test that truncation respects UTF-8 character boundaries for various scripts

        // Chinese characters (3 bytes each in UTF-8)
        let chinese_text = "你好世界测试文本内容";
        let truncated_chinese = truncate_str(chinese_text, 10);
        assert!(truncated_chinese.len() <= 10);
        assert!(chinese_text.is_char_boundary(truncated_chinese.len()));

        // Russian characters (2 bytes each for most Cyrillic)
        let russian_text = "Привет мир тест";
        let truncated_russian = truncate_str(russian_text, 10);
        assert!(truncated_russian.len() <= 10);
        assert!(russian_text.is_char_boundary(truncated_russian.len()));

        // German with umlauts (2 bytes each for ä, ö, ü, ß)
        let german_text = "Schöne Grüße";
        let truncated_german = truncate_str(german_text, 10);
        assert!(truncated_german.len() <= 10);
        assert!(german_text.is_char_boundary(truncated_german.len()));

        // Mixed scripts
        let mixed_text = "Hello你好Привет";
        let truncated_mixed = truncate_str(mixed_text, 12);
        assert!(truncated_mixed.len() <= 12);
        assert!(mixed_text.is_char_boundary(truncated_mixed.len()));
    }
}
