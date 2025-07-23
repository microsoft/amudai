//! Iterator adapters for working with `Range<u64>` sequences.
//!
//! This module provides utilities for manipulating iterators of `Range<u64>`,
//! including chunking ranges into smaller pieces and shifting ranges up or down
//! by a fixed amount. These adapters are useful for efficiently processing
//! large or fragmented ranges, such as when splitting I/O requests or
//! normalizing logical regions.
//!
//! # Provided Adapters
//!
//! - [`ChunkedRanges`]: Splits each input range into subranges of at most a given size.
//! - [`ShiftDownRanges`]: Shifts all range bounds down by a fixed amount (with underflow checking).
//! - [`ShiftUpRanges`]: Shifts all range bounds up by a fixed amount (with overflow checking).
//!
//! The [`RangeIteratorsExt`] trait is implemented for all iterators over `Range<u64>`,
//! providing convenient methods to construct these adapters.

use std::ops::Range;

pub mod chunk;
pub mod shift;

/// Extension trait for more idiomatic usage of the range iterator adapters.
///
/// This trait provides convenient methods to adapt iterators of `Range<u64>`
/// into chunked or shifted forms.
pub trait RangeIteratorsExt: Iterator<Item = Range<u64>> + Sized {
    /// Adapts an iterator of `Range<u64>` to yield ranges chunked to a maximum size.
    ///
    /// Each output range will have a length at most `chunk_size`.
    ///
    /// # Panics
    ///
    /// Panics if `chunk_size` is 0.
    fn chunk_ranges(self, chunk_size: u64) -> chunk::ChunkedRanges<Self> {
        chunk::ChunkedRanges::new(self, chunk_size)
    }

    /// Adapts an iterator of `Range<u64>` to yield ranges shifted down by `amount`.
    ///
    /// Each output range will have its `start` and `end` decreased by `amount`.
    ///
    /// # Panics
    ///
    /// Panics if subtracting `amount` would underflow either bound.
    fn shift_down(self, amount: u64) -> shift::ShiftDownRanges<Self> {
        shift::ShiftDownRanges::new(self, amount)
    }

    /// Adapts an iterator of `Range<u64>` to yield ranges shifted up by `amount`.
    ///
    /// Each output range will have its `start` and `end` increased by `amount`.
    ///
    /// # Panics
    ///
    /// Panics if adding `amount` would overflow either bound.
    fn shift_up(self, amount: u64) -> shift::ShiftUpRanges<Self> {
        shift::ShiftUpRanges::new(self, amount)
    }
}

impl<I: Iterator<Item = Range<u64>>> RangeIteratorsExt for I {}
