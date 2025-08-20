//! Unicode Word Tokenizer - extracts alphanumeric words using Unicode properties.

use std::str::CharIndices;

use super::{DEFAULT_MAX_TERM_LENGTH, DEFAULT_MIN_TERM_LENGTH, Tokenizer, truncate_str};
use crate::tokenizers::TokenizerKind;

/// Word Tokenizer - extracts alphanumeric words from text.
///
/// This tokenizer extracts "words" which are the longest continuous sequences
/// of alphanumeric characters. Terms longer than the maximum length are
/// truncated at UTF-8 character boundaries. Terms shorter than the minimum
/// length are excluded entirely.
pub struct UnicodeWordTokenizer {
    max_term_length: usize,
    min_term_length: usize,
}

impl UnicodeWordTokenizer {
    /// Create a new UnicodeWordTokenizer with custom max and min term lengths.
    pub fn with_lengths(max_term_length: usize, min_term_length: usize) -> Self {
        Self {
            max_term_length,
            min_term_length,
        }
    }

    /// Create a new UnicodeWordTokenizer with default settings.
    pub fn new() -> Self {
        Self {
            max_term_length: DEFAULT_MAX_TERM_LENGTH,
            min_term_length: DEFAULT_MIN_TERM_LENGTH,
        }
    }
}

/// Provides default construction for UnicodeWordTokenizer using standard settings.
impl Default for UnicodeWordTokenizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator that yields word tokens from a string input.
pub struct WordTokenIterator<'a> {
    input: &'a str,
    char_indices: CharIndices<'a>,
    max_term_length: usize,
    min_term_length: usize,
}

impl<'a> WordTokenIterator<'a> {
    /// Creates a new WordTokenIterator for the given input string.
    ///
    /// # Arguments
    /// * `input` - The input string to tokenize into words
    /// * `max_term_length` - Maximum length for words before truncation
    /// * `min_term_length` - Minimum length for words to be included
    ///
    /// # Returns
    /// A new WordTokenIterator that will extract alphanumeric word sequences
    pub fn new(input: &'a str, max_term_length: usize, min_term_length: usize) -> Self {
        Self {
            input,
            char_indices: input.char_indices(),
            max_term_length,
            min_term_length,
        }
    }
}

impl<'a> Iterator for WordTokenIterator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let mut word_start = None;
        let mut word_end = None;

        // Skip to the start of the next alphanumeric sequence
        for (pos, ch) in self.char_indices.by_ref() {
            if ch.is_alphanumeric() {
                if word_start.is_none() {
                    word_start = Some(pos);
                }
            } else if word_start.is_some() {
                word_end = Some(pos);
                break;
            }
        }

        // Handle case where word extends to end of string
        if word_start.is_some() && word_end.is_none() {
            word_end = Some(self.input.len());
        }

        if let (Some(start), Some(end)) = (word_start, word_end) {
            let word = &self.input[start..end];
            // Only return words that meet minimum length requirement
            if word.len() >= self.min_term_length {
                // Truncate if it exceeds max length, otherwise return as-is
                Some(truncate_str(word, self.max_term_length))
            } else {
                // Skip this word and try the next one (too short)
                self.next()
            }
        } else {
            None
        }
    }
}

impl Tokenizer for UnicodeWordTokenizer {
    type TokenIter<'a> = WordTokenIterator<'a>;

    fn tokenize<'a>(&'a self, input: &'a str) -> Self::TokenIter<'a> {
        WordTokenIterator::new(input, self.max_term_length, self.min_term_length)
    }

    fn kind(&self) -> TokenizerKind {
        TokenizerKind::UnicodeWord
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
    fn test_unicode_word_tokenizer() {
        let tokenizer = UnicodeWordTokenizer::new();

        // Basic word extraction
        let terms: Vec<&str> = tokenizer.tokenize("Typically 3-4 levels deep,").collect();
        assert_eq!(terms, vec!["Typically", "3", "4", "levels", "deep"]);

        // Empty string
        let terms: Vec<&str> = tokenizer.tokenize("").collect();
        assert_eq!(terms, Vec::<&str>::new());

        // Single word
        let terms: Vec<&str> = tokenizer.tokenize("word").collect();
        assert_eq!(terms, vec!["word"]);

        // Only delimiters
        let terms: Vec<&str> = tokenizer.tokenize("!@#$%^&*()").collect();
        assert_eq!(terms, Vec::<&str>::new());

        // Unicode characters
        let terms: Vec<&str> = tokenizer.tokenize("café naïve résumé").collect();
        assert_eq!(terms, vec!["café", "naïve", "résumé"]);

        // Test name
        assert_eq!(tokenizer.name(), "unicode-word");
    }

    #[test]
    fn test_word_length_filtering() {
        // Create a tokenizer with a small max length to test truncation
        let tokenizer = UnicodeWordTokenizer::with_lengths(3, 1); // Max length of 3 bytes, min length of 1

        // Words longer than 3 bytes should be truncated, not omitted
        let terms: Vec<&str> = tokenizer.tokenize("cat dog elephant mouse").collect();
        assert_eq!(terms, vec!["cat", "dog", "ele", "mou"]); // "elephant" -> "ele", "mouse" -> "mou"

        // Test edge case at exactly max length
        let terms: Vec<&str> = tokenizer.tokenize("abc def ghij").collect();
        assert_eq!(terms, vec!["abc", "def", "ghi"]); // "ghij" -> "ghi"

        // Test Unicode truncation at character boundaries
        let terms: Vec<&str> = tokenizer.tokenize("café").collect();
        assert_eq!(terms, vec!["caf"]); // "café" (5 bytes total) truncated to "caf" (3 bytes)
    }
    #[test]
    fn test_min_term_length_filtering() {
        // Create a tokenizer with a higher min length to test filtering
        let tokenizer = UnicodeWordTokenizer::with_lengths(128, 3); // Max length of 128, min length of 3

        // Words shorter than 3 bytes should be excluded
        let terms: Vec<&str> = tokenizer.tokenize("a bb cat dog elephant").collect();
        assert_eq!(terms, vec!["cat", "dog", "elephant"]); // "a" and "bb" are too short

        // Test edge case at exactly min length
        let terms: Vec<&str> = tokenizer.tokenize("ab abc defg").collect();
        assert_eq!(terms, vec!["abc", "defg"]); // "ab" is 2 bytes, too short
    }

    #[test]
    fn test_configurable_word_tokenizer() {
        // Test tokenizer with custom min and max term lengths
        let tokenizer = UnicodeWordTokenizer::with_lengths(6, 2);

        // Test that it uses the custom lengths
        assert_eq!(tokenizer.max_term_length(), 6);
        assert_eq!(tokenizer.min_term_length(), 2);

        // Test filtering and truncation
        let terms: Vec<&str> = tokenizer.tokenize("a bb cat verylongword").collect();
        assert_eq!(terms, vec!["bb", "cat", "verylo"]); // "a" too short, "verylongword" truncated

        // Test that default constructor works
        let default_tokenizer = UnicodeWordTokenizer::new();
        assert_eq!(default_tokenizer.max_term_length(), 128);
        assert_eq!(default_tokenizer.min_term_length(), 1);
    }

    #[test]
    fn test_chinese_text_tokenization() {
        let tokenizer = UnicodeWordTokenizer::new();

        // Test Chinese text mixed with ASCII
        let input = "你好世界 hello 2024年 world 北京大学";
        let terms: Vec<&str> = tokenizer.tokenize(input).collect();

        // Should extract exactly 5 terms
        assert_eq!(terms.len(), 5);

        // Chinese characters should be treated as alphanumeric
        assert!(terms.contains(&"你好世界"));
        assert!(terms.contains(&"hello"));
        assert!(terms.contains(&"2024年"));
        assert!(terms.contains(&"world"));
        assert!(terms.contains(&"北京大学"));

        // Test with punctuation
        let input_punct = "你好，世界！这是测试。";
        let terms_punct: Vec<&str> = tokenizer.tokenize(input_punct).collect();

        // Should extract exactly 3 terms
        assert_eq!(terms_punct.len(), 3);
        assert!(terms_punct.contains(&"你好"));
        assert!(terms_punct.contains(&"世界"));
        assert!(terms_punct.contains(&"这是测试"));
    }

    #[test]
    fn test_hebrew_text_tokenization() {
        let tokenizer = UnicodeWordTokenizer::new();

        // Test Hebrew text (right-to-left script)
        let input = "שלום עולם! זה בדיקה 2024 שנה בתל אביב.";
        let terms: Vec<&str> = tokenizer.tokenize(input).collect();

        // Should extract exactly 8 terms
        assert_eq!(terms.len(), 8);

        // Hebrew words should be extracted correctly
        assert!(terms.contains(&"שלום"));
        assert!(terms.contains(&"עולם"));
        assert!(terms.contains(&"זה"));
        assert!(terms.contains(&"בדיקה"));
        assert!(terms.contains(&"2024"));
        assert!(terms.contains(&"שנה"));
        assert!(terms.contains(&"בתל"));
        assert!(terms.contains(&"אביב"));

        // Test mixed Hebrew and Latin
        let mixed_input = "Hello שלום world עולם test בדיקה";
        let mixed_terms: Vec<&str> = tokenizer.tokenize(mixed_input).collect();

        // Should extract exactly 6 terms
        assert_eq!(mixed_terms.len(), 6);
        assert!(mixed_terms.contains(&"Hello"));
        assert!(mixed_terms.contains(&"שלום"));
        assert!(mixed_terms.contains(&"world"));
        assert!(mixed_terms.contains(&"עולם"));
        assert!(mixed_terms.contains(&"test"));
        assert!(mixed_terms.contains(&"בדיקה"));
    }

    #[test]
    fn test_german_text_tokenization() {
        let tokenizer = UnicodeWordTokenizer::new();

        // Test German text with umlauts and special characters
        let input = "Hallo Welt! Schöne Grüße aus München. Tschüß!";
        let terms: Vec<&str> = tokenizer.tokenize(input).collect();

        // Should extract exactly 7 terms
        assert_eq!(terms.len(), 7);

        // German words with umlauts should be extracted correctly
        assert!(terms.contains(&"Hallo"));
        assert!(terms.contains(&"Welt"));
        assert!(terms.contains(&"Schöne"));
        assert!(terms.contains(&"Grüße"));
        assert!(terms.contains(&"aus"));
        assert!(terms.contains(&"München"));
        assert!(terms.contains(&"Tschüß"));

        // Test compound words (common in German)
        let compound_input = "Donaudampfschifffahrtskapitän Straßenbahn Fußballmannschaft";
        let compound_terms: Vec<&str> = tokenizer.tokenize(compound_input).collect();

        // Should extract exactly 3 terms
        assert_eq!(compound_terms.len(), 3);
        assert!(compound_terms.contains(&"Donaudampfschifffahrtskapitän"));
        assert!(compound_terms.contains(&"Straßenbahn"));
        assert!(compound_terms.contains(&"Fußballmannschaft"));
    }

    #[test]
    fn test_multilingual_mixed_text() {
        let tokenizer = UnicodeWordTokenizer::new();

        // Test text mixing Chinese, Hebrew, German, and English
        let input = "Hello 你好 שלום Hallo world 世界 עולם Welt 2024年";
        let terms: Vec<&str> = tokenizer.tokenize(input).collect();

        // Should extract exactly 9 terms
        assert_eq!(terms.len(), 9);

        // English
        assert!(terms.contains(&"Hello"));
        assert!(terms.contains(&"world"));

        // Chinese
        assert!(terms.contains(&"你好"));
        assert!(terms.contains(&"世界"));
        assert!(terms.contains(&"2024年"));

        // Hebrew
        assert!(terms.contains(&"שלום"));
        assert!(terms.contains(&"עולם"));

        // German
        assert!(terms.contains(&"Hallo"));
        assert!(terms.contains(&"Welt"));
    }

    #[test]
    fn test_emoji_and_special_unicode() {
        let tokenizer = UnicodeWordTokenizer::new();

        // Test with emojis and special Unicode characters
        let input = "Hello 👋 world 🌍 test 测试 😀 2024";
        let terms: Vec<&str> = tokenizer.tokenize(input).collect();

        // Should extract exactly 5 terms (emojis are not alphanumeric)
        assert_eq!(terms.len(), 5);

        // Alphanumeric words should be extracted
        assert!(terms.contains(&"Hello"));
        assert!(terms.contains(&"world"));
        assert!(terms.contains(&"test"));
        assert!(terms.contains(&"测试"));
        assert!(terms.contains(&"2024"));

        // Emojis are not alphanumeric, so they should not be in separate terms
        // but they should not break the tokenization
        assert!(!terms.contains(&"👋"));
        assert!(!terms.contains(&"🌍"));
        assert!(!terms.contains(&"😀"));
    }

    #[test]
    fn test_configurable_tokenizer_with_unicode() {
        // Test that configurable tokenizers work correctly with Unicode text
        let tokenizer = UnicodeWordTokenizer::with_lengths(20, 3);

        // Chinese text where some words might be filtered by min length
        let input = "你好 我 世界 测试文本";
        let terms: Vec<&str> = tokenizer.tokenize(input).collect();

        // Should extract exactly 4 terms (all meet minimum length requirement)
        assert_eq!(terms.len(), 4);

        // "你好" (6 bytes), "世界" (6 bytes), "测试文本" (12 bytes) should be included
        assert!(terms.contains(&"你好"));
        assert!(terms.contains(&"世界"));
        assert!(terms.contains(&"测试文本"));

        // "我" (3 bytes) should be included (exactly at min length)
        assert!(terms.contains(&"我"));

        // Test German with configurable tokenizer
        let german_input = "Ich bin ein Test";
        let german_terms: Vec<&str> = tokenizer.tokenize(german_input).collect();

        // Should extract exactly 4 terms
        assert_eq!(german_terms.len(), 4);

        // "Ich" (3 bytes) should be included (exactly at min length)
        assert!(german_terms.contains(&"Ich"));
        assert!(german_terms.contains(&"bin"));
        assert!(german_terms.contains(&"ein"));
        assert!(german_terms.contains(&"Test"));
    }
}
