//! Storage backend abstraction for dense bitsets.
//!
//! This module defines [`BitStore`], a minimal trait used to allocate
//! fixed-length arrays of 64-bit words that back bitsets and similar data
//! structures. The `count` parameter used by the trait methods is the number
//! of `u64` words (not bits).
//!
//! Implementations exist for heap-backed `Box<[u64]>` and a page-aligned,
//! memory-mapped buffer via
//! [`amudai_page_alloc::mmap_buffer::MmapBuffer`].

use amudai_page_alloc::mmap_buffer::MmapBuffer;

/// A storage backend for fixed-length arrays of 64-bit words.
///
/// Types implementing `BitStore` can allocate `count` words of `u64`
/// initialized either to zero or to a provided bit pattern. The `count` is a
/// word count (number of `u64`s), not a bit count.
pub trait BitStore {
    /// Allocate `count` words of `u64`, initialized to zero.
    fn new_zeroed(count: usize) -> Self;

    /// Allocate `count` words of `u64`, initializing every word to `pattern`.
    fn new_with_pattern(count: usize, pattern: u64) -> Self;
}

impl BitStore for Box<[u64]> {
    fn new_zeroed(count: usize) -> Self {
        vec![0u64; count].into_boxed_slice()
    }

    fn new_with_pattern(count: usize, pattern: u64) -> Self {
        vec![pattern; count].into_boxed_slice()
    }
}

impl BitStore for MmapBuffer {
    /// Allocates a memory region large enough for `count` `u64` words,
    /// zero-initialized. Uses a large-page allocation with a regular mmap
    /// fallback.
    fn new_zeroed(count: usize) -> Self {
        MmapBuffer::allocate_with_fallback(count * std::mem::size_of::<u64>()).expect("allocate")
    }

    /// Allocates a memory region large enough for `count` `u64` words and
    /// fills every word with `pattern`.
    fn new_with_pattern(count: usize, pattern: u64) -> Self {
        let mut this = Self::new_zeroed(count);
        this.as_mut_slice::<u64>().fill(pattern);
        this
    }
}
