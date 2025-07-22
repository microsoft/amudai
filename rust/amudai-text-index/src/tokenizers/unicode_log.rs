//! Unicode Log Tokenizer - extends word tokenizer with IPv4 address recognition.

use super::{DEFAULT_MAX_TERM_LENGTH, DEFAULT_MIN_TERM_LENGTH, Tokenizer, truncate_str};

/// Log Tokenizer - extends word tokenizer with IPv4 address recognition.
///
/// This tokenizer extends the functionality of the unicode-word tokenizer
/// by also recognizing IPv4 addresses that are properly separated by
/// non-alphanumeric characters. It extracts both IPv4 addresses AND
/// individual words/numbers from the entire input, which may result in
/// overlapping tokens (e.g., "192.168.1.1" produces both the full IP
/// and the individual numbers "192", "168", "1", "1").
///
/// Terms longer than the maximum length are truncated at UTF-8 character
/// boundaries. Terms shorter than the minimum length are excluded entirely.
pub struct UnicodeLogTokenizer {
    max_term_length: usize,
    min_term_length: usize,
}

/// Iterator that yields both IPv4 addresses and word tokens from a string input.
pub struct LogTokenIterator<'a> {
    input: &'a str,
    word_iterator: super::unicode_word::WordTokenIterator<'a>,
    current_phase: LogTokenPhase,
    max_term_length: usize,
    min_term_length: usize,
    ipv4_search_pos: usize,
}

/// Represents the current phase of tokenization in the log tokenizer.
///
/// The log tokenizer operates in two phases:
/// 1. First, it extracts all IPv4 addresses from the input
/// 2. Then, it extracts individual words and numbers using the word tokenizer
#[derive(Debug, PartialEq)]
enum LogTokenPhase {
    /// Currently searching for and extracting IPv4 addresses
    IPv4,
    /// Currently extracting individual words and numbers
    Words,
}

impl<'a> LogTokenIterator<'a> {
    /// Creates a new LogTokenIterator for the given input string.
    ///
    /// # Arguments
    /// * `input` - The input string to tokenize
    /// * `max_term_length` - Maximum length for terms before truncation
    /// * `min_term_length` - Minimum length for terms to be included
    ///
    /// # Returns
    /// A new LogTokenIterator that will first extract IPv4 addresses, then words
    fn new(input: &'a str, max_term_length: usize, min_term_length: usize) -> Self {
        let word_iterator =
            super::unicode_word::WordTokenIterator::new(input, max_term_length, min_term_length);

        Self {
            input,
            word_iterator,
            current_phase: LogTokenPhase::IPv4,
            max_term_length,
            min_term_length,
            ipv4_search_pos: 0,
        }
    }

    /// Finds the next valid IPv4 address in the input string.
    ///
    /// Searches for IPv4 addresses that are properly bounded by non-alphanumeric
    /// characters and have valid octet values (0-255). Skips IPv4 addresses that
    /// would be truncated due to maximum term length constraints.
    ///
    /// # Returns
    /// * `Some(&str)` - The next valid IPv4 address found in the input
    /// * `None` - No more valid IPv4 addresses found
    fn find_next_ipv4(&mut self) -> Option<&'a str> {
        let bytes = self.input.as_bytes();

        while self.ipv4_search_pos < bytes.len() {
            // Look for start of potential IPv4 (digit after word boundary)
            if bytes[self.ipv4_search_pos].is_ascii_digit() {
                // Check if we're at a word boundary (start of string or after non-alphanumeric)
                let at_boundary = self.ipv4_search_pos == 0
                    || !bytes[self.ipv4_search_pos - 1].is_ascii_alphanumeric();

                if at_boundary {
                    let (end_pos, is_valid) =
                        parse_ipv4_at_position(self.input, self.ipv4_search_pos);

                    if is_valid {
                        // Check if there's a word boundary after the IPv4
                        let at_end_boundary =
                            end_pos >= bytes.len() || !bytes[end_pos].is_ascii_alphanumeric();

                        if at_end_boundary {
                            let ipv4 = &self.input[self.ipv4_search_pos..end_pos];

                            // Check if the IP would be truncated due to max_term_length
                            // If it would be truncated, don't return it as a valid IP
                            if ipv4.len() > self.max_term_length {
                                // Skip this IP since it would be truncated and become invalid
                                self.ipv4_search_pos = end_pos;
                                continue;
                            }

                            self.ipv4_search_pos = end_pos;
                            return Some(ipv4);
                        }
                    }
                }
            }

            self.ipv4_search_pos += 1;
        }

        None
    }
}

impl<'a> Iterator for LogTokenIterator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current_phase {
            LogTokenPhase::IPv4 => {
                if let Some(ip) = self.find_next_ipv4() {
                    if ip.len() >= self.min_term_length {
                        Some(truncate_str(ip, self.max_term_length))
                    } else {
                        // Skip this IP and try the next one (too short)
                        self.next()
                    }
                } else {
                    // Switch to word phase
                    self.current_phase = LogTokenPhase::Words;
                    self.next()
                }
            }
            LogTokenPhase::Words => self.word_iterator.next(),
        }
    }
}

impl UnicodeLogTokenizer {
    /// Create a new UnicodeLogTokenizer with custom max and min term lengths.
    pub fn with_lengths(max_term_length: usize, min_term_length: usize) -> Self {
        Self {
            max_term_length,
            min_term_length,
        }
    }

    /// Create a new UnicodeLogTokenizer with default settings.
    pub fn new() -> Self {
        Self {
            max_term_length: DEFAULT_MAX_TERM_LENGTH,
            min_term_length: DEFAULT_MIN_TERM_LENGTH,
        }
    }
}

/// Provides default construction for UnicodeLogTokenizer using standard settings.
impl Default for UnicodeLogTokenizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Tokenizer for UnicodeLogTokenizer {
    type TokenIter<'a> = LogTokenIterator<'a>;

    fn tokenize<'a>(&'a self, input: &'a str) -> Self::TokenIter<'a> {
        LogTokenIterator::new(input, self.max_term_length, self.min_term_length)
    }

    fn name(&self) -> &'static str {
        "unicode-log"
    }

    fn max_term_length(&self) -> usize {
        self.max_term_length
    }

    fn min_term_length(&self) -> usize {
        self.min_term_length
    }
}

/// Parses an IPv4 address from text starting at the given position.
///
/// This function attempts to parse a complete IPv4 address (four octets separated
/// by dots) starting from the specified position. Each octet must be a valid
/// decimal number between 0-255.
///
/// # Arguments
/// * `text` - The input text containing the potential IPv4 address
/// * `start` - The starting position in the text to begin parsing
///
/// # Returns
/// A tuple containing:
/// * `usize` - The end position after the IPv4 address (or start position if invalid)
/// * `bool` - Whether a valid IPv4 address was found
///
fn parse_ipv4_at_position(text: &str, start: usize) -> (usize, bool) {
    let bytes = text.as_bytes();
    let mut pos = start;
    let mut octet_count = 0;

    while pos < bytes.len() && octet_count < 4 {
        // Parse one octet (0-255)
        let mut octet_value = 0u32;
        let mut digit_count = 0;

        // Parse digits
        while pos < bytes.len() && bytes[pos].is_ascii_digit() {
            let digit = (bytes[pos] - b'0') as u32;
            octet_value = octet_value * 10 + digit;
            digit_count += 1;
            pos += 1;

            // Too many digits or value too large
            if digit_count > 3 || octet_value > 255 {
                return (start, false);
            }
        }

        // Must have at least one digit
        if digit_count == 0 {
            return (start, false);
        }

        octet_count += 1;

        // Check for dot separator (except after last octet)
        if octet_count < 4 {
            if pos >= bytes.len() || bytes[pos] != b'.' {
                return (start, false);
            }
            pos += 1; // Skip the dot
        }
    }

    // Must have exactly 4 octets
    if octet_count == 4 {
        (pos, true)
    } else {
        (start, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unicode_log_tokenizer() {
        let tokenizer = UnicodeLogTokenizer::new();

        // IPv4 addresses extraction
        let terms: Vec<&str> = tokenizer
            .tokenize("10.0.0.1|192.168.1.1,,8.8.8.8 1.1.1.1")
            .collect();
        assert!(terms.contains(&"10.0.0.1"));
        assert!(terms.contains(&"192.168.1.1"));
        assert!(terms.contains(&"8.8.8.8"));
        assert!(terms.contains(&"1.1.1.1"));

        // Regular words should also be extracted
        let terms: Vec<&str> = tokenizer
            .tokenize("Error connecting to 192.168.1.1 server")
            .collect();
        assert!(terms.contains(&"192.168.1.1"));
        assert!(terms.contains(&"Error"));
        assert!(terms.contains(&"connecting"));
        assert!(terms.contains(&"to"));
        assert!(terms.contains(&"server"));
    }

    #[test]
    fn test_ipv4_regex_edge_cases() {
        let tokenizer = UnicodeLogTokenizer::new();

        // Invalid IPv4 addresses should not be extracted as IP addresses
        let terms: Vec<&str> = tokenizer.tokenize("999.999.999.999 256.1.1.1").collect();
        // These should be extracted as words, not as IP addresses
        assert!(terms.contains(&"999"));
        assert!(terms.contains(&"256"));

        // Valid IPv4 at boundaries
        let terms: Vec<&str> = tokenizer.tokenize("start192.168.1.1end").collect();
        // This should not match as IPv4 because it's not properly bounded
        assert!(terms.contains(&"start192"));
        assert!(terms.contains(&"168"));
        assert!(terms.contains(&"1"));
        assert!(terms.contains(&"1end"));

        // Properly bounded IPv4
        let terms: Vec<&str> = tokenizer.tokenize("start 192.168.1.1 end").collect();
        assert!(terms.contains(&"192.168.1.1"));
        assert!(terms.contains(&"start"));
        assert!(terms.contains(&"end"));
    }

    #[test]
    fn test_unicode_and_ascii_with_ips() {
        let tokenizer = UnicodeLogTokenizer::new();

        // Mix of Unicode, ASCII and IP addresses
        let input = "Error: café server at 192.168.1.1 naïve connection to 10.0.0.1 résumé";
        let terms: Vec<&str> = tokenizer.tokenize(input).collect();

        // Should extract exactly 18 terms
        assert_eq!(terms.len(), 18);

        // Should extract IP addresses
        assert!(terms.contains(&"192.168.1.1"));
        assert!(terms.contains(&"10.0.0.1"));

        // Should extract ASCII words
        assert!(terms.contains(&"Error"));
        assert!(terms.contains(&"server"));
        assert!(terms.contains(&"at"));
        assert!(terms.contains(&"connection"));
        assert!(terms.contains(&"to"));

        // Should extract Unicode words
        assert!(terms.contains(&"café"));
        assert!(terms.contains(&"naïve"));
        assert!(terms.contains(&"résumé"));
    }

    #[test]
    fn test_incomplete_ip_addresses() {
        let tokenizer = UnicodeLogTokenizer::new();

        // Incomplete IP addresses should be treated as separate words
        let terms: Vec<&str> = tokenizer
            .tokenize("192.168.1 incomplete 10.20 also 1.2.3 missing")
            .collect();

        // These should NOT be extracted as IP addresses
        assert!(!terms.contains(&"192.168.1"));
        assert!(!terms.contains(&"10.20"));
        assert!(!terms.contains(&"1.2.3"));

        // They should be extracted as individual words/numbers
        assert!(terms.contains(&"192"));
        assert!(terms.contains(&"168"));
        assert!(terms.contains(&"1"));
        assert!(terms.contains(&"incomplete"));
        assert!(terms.contains(&"10"));
        assert!(terms.contains(&"20"));
        assert!(terms.contains(&"also"));
        assert!(terms.contains(&"2"));
        assert!(terms.contains(&"3"));
        assert!(terms.contains(&"missing"));
    }

    #[test]
    fn test_malformed_ip_addresses() {
        let tokenizer = UnicodeLogTokenizer::new();

        // Malformed IP addresses with out-of-range values
        let input = "Invalid IPs: 300.168.1.1 192.300.1.1 192.168.300.1 192.168.1.300";
        let terms: Vec<&str> = tokenizer.tokenize(input).collect();

        // None of these should be extracted as IP addresses
        assert!(!terms.contains(&"300.168.1.1"));
        assert!(!terms.contains(&"192.300.1.1"));
        assert!(!terms.contains(&"192.168.300.1"));
        assert!(!terms.contains(&"192.168.1.300"));

        // They should be broken down into individual components
        assert!(terms.contains(&"Invalid"));
        assert!(terms.contains(&"IPs"));
        assert!(terms.contains(&"300"));
        assert!(terms.contains(&"192"));
        assert!(terms.contains(&"168"));
        assert!(terms.contains(&"1"));
    }

    #[test]
    fn test_mixed_punctuation_and_special_chars() {
        let tokenizer = UnicodeLogTokenizer::new();

        // Test with various punctuation and special characters
        let input = "Connection from 127.0.0.1:8080 to server@192.168.1.1, timeout=30s café-server";
        let terms: Vec<&str> = tokenizer.tokenize(input).collect();

        // Should extract valid IP addresses
        assert!(terms.contains(&"127.0.0.1"));
        assert!(terms.contains(&"192.168.1.1"));

        // Should extract words around punctuation
        assert!(terms.contains(&"Connection"));
        assert!(terms.contains(&"from"));
        assert!(terms.contains(&"8080"));
        assert!(terms.contains(&"to"));
        assert!(terms.contains(&"server"));
        assert!(terms.contains(&"timeout"));
        assert!(terms.contains(&"30s"));
        assert!(terms.contains(&"café"));
        assert!(terms.contains(&"server"));
    }

    #[test]
    fn test_edge_case_valid_ips() {
        let tokenizer = UnicodeLogTokenizer::new();

        // Test edge cases of valid IP addresses
        let input = "IPs: 0.0.0.0 255.255.255.255 127.0.0.1 192.168.0.1";
        let terms: Vec<&str> = tokenizer.tokenize(input).collect();

        // All should be extracted as valid IP addresses
        assert!(terms.contains(&"0.0.0.0"));
        assert!(terms.contains(&"255.255.255.255"));
        assert!(terms.contains(&"127.0.0.1"));
        assert!(terms.contains(&"192.168.0.1"));

        // Should also extract the word "IPs"
        assert!(terms.contains(&"IPs"));
    }

    #[test]
    fn test_unicode_with_numbers_and_ips() {
        let tokenizer = UnicodeLogTokenizer::new();

        // Complex Unicode text with numbers and IP addresses
        let input =
            "日本語 server-2024 at 172.16.0.1, café3000 connects to 10.10.10.10 über connection";
        let terms: Vec<&str> = tokenizer.tokenize(input).collect();

        // Should extract exactly 19 terms
        assert_eq!(terms.len(), 19);

        // Should extract IP addresses
        assert!(terms.contains(&"172.16.0.1"));
        assert!(terms.contains(&"10.10.10.10"));

        // Should extract Unicode words
        assert!(terms.contains(&"日本語"));
        assert!(terms.contains(&"café3000"));
        assert!(terms.contains(&"über"));

        // Should extract ASCII words and numbers
        assert!(terms.contains(&"server"));
        assert!(terms.contains(&"2024"));
        assert!(terms.contains(&"at"));
        assert!(terms.contains(&"connects"));
        assert!(terms.contains(&"to"));
        assert!(terms.contains(&"connection"));
    }

    #[test]
    fn test_empty_and_whitespace_only() {
        let tokenizer = UnicodeLogTokenizer::new();

        // Empty string
        let terms: Vec<&str> = tokenizer.tokenize("").collect();
        assert!(terms.is_empty());

        // Whitespace only
        let terms: Vec<&str> = tokenizer.tokenize("   \t\n\r   ").collect();
        assert!(terms.is_empty());

        // Only punctuation and special characters
        let terms: Vec<&str> = tokenizer
            .tokenize("!@#$%^&*()_+-={}[]|\\:;\"'<>?,./")
            .collect();
        assert!(terms.is_empty());
    }

    #[test]
    fn test_min_term_length_filtering() {
        // Create a tokenizer with a higher min length to test filtering
        let tokenizer = UnicodeLogTokenizer::with_lengths(128, 4); // Max length of 128, min length of 4

        // IPv4 addresses should be kept even if individual segments are short
        // Words shorter than 4 bytes should be excluded
        let terms: Vec<&str> = tokenizer
            .tokenize("192.168.1.1 cat dog elephant error")
            .collect();
        assert_eq!(terms, vec!["192.168.1.1", "elephant", "error"]); // "cat" and "dog" are too short

        // Test with incomplete IP addresses
        let terms: Vec<&str> = tokenizer
            .tokenize("192.168.1 incomplete test port")
            .collect();
        assert_eq!(terms, vec!["incomplete", "test", "port"]); // "192.168.1" doesn't match IPv4 regex
    }

    #[test]
    fn test_truncation_behavior() {
        // Create a tokenizer with a small max length to test truncation
        let tokenizer = UnicodeLogTokenizer::with_lengths(8, 1); // Max length of 8 bytes

        // Log tokenizer extracts BOTH IP addresses AND individual words/numbers
        // So "1.2.3.4 elephant" will produce: IP "1.2.3.4" + words "1", "2", "3", "4", "elephant"
        let terms: Vec<&str> = tokenizer.tokenize("1.2.3.4 elephant").collect();
        assert_eq!(terms, vec!["1.2.3.4", "1", "2", "3", "4", "elephant"]); // IP + individual numbers + word

        // Test with a very long word
        let terms: Vec<&str> = tokenizer
            .tokenize("supercalifragilisticexpialidocious")
            .collect();
        assert_eq!(terms, vec!["supercal"]); // truncated to 8 bytes
    }

    #[test]
    fn test_configurable_log_tokenizer() {
        // Test tokenizer with custom min and max term lengths
        let tokenizer = UnicodeLogTokenizer::with_lengths(10, 2); // Max length of 10 bytes

        // Test that it uses the custom lengths
        assert_eq!(tokenizer.max_term_length(), 10);
        assert_eq!(tokenizer.min_term_length(), 2);

        // Test filtering and truncation
        let terms: Vec<&str> = tokenizer
            .tokenize("a bb cat 192.168.1.1 verylongword")
            .collect();

        // "a" should be filtered out (too short)
        assert!(!terms.contains(&"a"));

        // These should be included
        assert!(terms.contains(&"bb"));
        assert!(terms.contains(&"cat"));

        // The IP "192.168.1.1" (11 chars) should NOT be included as an IP because it would be truncated
        assert!(!terms.contains(&"192.168.1.1"));

        // But its components should be included as individual words (those that meet min_term_length >= 2)
        assert!(terms.contains(&"192"));
        assert!(terms.contains(&"168"));
        // Note: "1" is only 1 character, so it gets filtered out by min_term_length = 2
        assert!(!terms.contains(&"1"));

        // "verylongword" (12 chars) should be truncated to 10 chars
        assert!(terms.contains(&"verylongwo")); // truncated to 10 bytes

        // Test that default constructor works
        let default_tokenizer = UnicodeLogTokenizer::new();
        assert_eq!(default_tokenizer.max_term_length(), 128);
        assert_eq!(default_tokenizer.min_term_length(), 1);

        // Test with a longer max length where IP should be preserved
        let long_tokenizer = UnicodeLogTokenizer::with_lengths(20, 2);
        let terms_long: Vec<&str> = long_tokenizer.tokenize("192.168.1.1 test").collect();
        assert!(terms_long.contains(&"192.168.1.1")); // Should be included now
    }

    #[test]
    fn test_unicode_log_tokenizer_with_international_text() {
        let tokenizer = UnicodeLogTokenizer::new();

        // Test log-style text with international content and IP addresses
        let input = "错误：连接到服务器 192.168.1.1 失败。Ошибка: подключение к 10.0.0.1 неудачно. Fehler: Verbindung zu 172.16.0.1 fehlgeschlagen.";
        let terms: Vec<&str> = tokenizer.tokenize(input).collect();

        // Should extract exactly 26 terms
        assert_eq!(terms.len(), 26);

        // IP addresses should be extracted
        assert!(terms.contains(&"192.168.1.1"));
        assert!(terms.contains(&"10.0.0.1"));
        assert!(terms.contains(&"172.16.0.1"));

        // Chinese words
        assert!(terms.contains(&"错误"));
        assert!(terms.contains(&"连接到服务器"));
        assert!(terms.contains(&"失败"));

        // Russian words
        assert!(terms.contains(&"Ошибка"));
        assert!(terms.contains(&"подключение"));
        assert!(terms.contains(&"неудачно"));

        // German words
        assert!(terms.contains(&"Fehler"));
        assert!(terms.contains(&"Verbindung"));
        assert!(terms.contains(&"fehlgeschlagen"));
    }

    #[test]
    fn test_complex_network_configurations() {
        let tokenizer = UnicodeLogTokenizer::new();

        // Complex network configuration with various formats
        let input =
            "Route: 172.16.0.0/16 via 192.168.1.1 gateway, DNS 8.8.8.8:53 and backup 1.1.1.1:853";
        let terms: Vec<&str> = tokenizer.tokenize(input).collect();

        // Should extract exactly 29 terms
        assert_eq!(terms.len(), 29);

        // Valid standalone IPv4 addresses should be extracted
        assert!(terms.contains(&"192.168.1.1"));
        assert!(terms.contains(&"172.16.0.0"));
        assert!(terms.contains(&"8.8.8.8"));
        assert!(terms.contains(&"1.1.1.1"));

        // Network masks and ports as separate tokens
        assert!(terms.contains(&"16"));
        assert!(terms.contains(&"53"));
        assert!(terms.contains(&"853"));

        // Regular words
        assert!(terms.contains(&"Route"));
        assert!(terms.contains(&"via"));
        assert!(terms.contains(&"gateway"));
        assert!(terms.contains(&"DNS"));
        assert!(terms.contains(&"and"));
        assert!(terms.contains(&"backup"));
    }
}
