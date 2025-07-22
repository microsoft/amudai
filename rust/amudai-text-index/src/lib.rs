//! Inverted text index implementation.
//!
//! This crate provides text tokenization functionality for building inverted text indices.
//! It offers various tokenization strategies for extracting searchable terms from text data.
//!
//! # Overview
//!
//! The crate is primarily focused on tokenizers that break text into searchable terms.
//! These tokenizers are used in two key scenarios:
//!
//! 1. **Index Creation**: When building an inverted text index, tokenizers extract
//!    searchable terms from document text values.
//! 2. **Query Processing**: When searching the index, tokenizers process query strings
//!    to extract terms for matching.
//!
//! # Available Tokenizers
//!
//! - **Trivial Tokenizer** (`"trivial"`): Returns the input unchanged, useful for exact matches
//! - **Unicode Word Tokenizer** (`"unicode-word"`): Extracts alphanumeric words from text
//! - **Unicode Log Tokenizer** (`"unicode-log"`): Extracts both IPv4 addresses and words from log data
//!
//! # Quick Start
//!
//! ```rust
//! use amudai_text_index::{create_tokenizer, Tokenizer};
//!
//! // Create a word tokenizer
//! let tokenizer = create_tokenizer("unicode-word").unwrap();
//!
//! // Tokenize some text
//! let terms: Vec<&str> = tokenizer.tokenize("Hello, world! This is a test.").collect();
//! // Results in: ["Hello", "world", "This", "is", "a", "test"]
//! ```

mod tokenizers;

pub use tokenizers::{Tokenizer, create_tokenizer};
