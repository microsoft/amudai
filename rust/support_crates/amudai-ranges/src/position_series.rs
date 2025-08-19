//! Position series abstraction for handling sequences of numeric positions or ranges.
//!
//! This module provides a generic abstraction over sequences that can contain either
//! individual positions or ranges of positions. This is particularly useful when working
//! with data structures that need to accept both single indices and index ranges.
//!
//! The main trait [`PositionSeries`] enables writing generic functions that can work with:
//! - Individual positions (e.g., `[1, 3, 5, 7]`)
//! - Ranges of positions (e.g., `[0..10, 20..30, 40..50]`)
//! - Any iterator that produces either positions or ranges
//!
//! This abstraction eliminates the need for duplicate implementations when handling
//! both position types.

use crate::into_iter_adapters::IntoIteratorExt;
use std::ops::Range;

/// A trait for abstracting over sequences of numeric positions or position ranges.
///
/// `PositionSeries` represents an iterable sequence that can contain either:
/// - Individual positions (e.g., `[1, 3, 5, 7]`)
/// - Ranges of positions (e.g., `[0..10, 20..30, 40..50]`)
///
/// This abstraction is useful when you want to write functions that can work with
/// both types of position representations without duplicating logic.
///
/// # Type Parameters
///
/// * `T` - The primitive numeric type for positions (typically `usize`, `u64`, or `u32`)
///
/// # Design Notes
///
/// The trait uses associated constants and compile-time dispatch to ensure zero-cost
/// abstraction. The `RANGES` constant allows checking at compile time whether the
/// series contains ranges or individual positions.
pub trait PositionSeries<T> {
    /// A compile-time constant indicating whether this series contains ranges.
    ///
    /// - `true` if the series contains `Range<T>` items
    /// - `false` if the series contains individual `T` positions
    const RANGES: bool;

    /// Returns whether this series contains ranges or individual positions.
    ///
    /// This is a runtime accessor for the compile-time `RANGES` constant.
    /// Useful when you need to branch logic based on the series type.
    ///
    /// # Returns
    ///
    /// - `true` if the series contains ranges
    /// - `false` if the series contains individual positions
    fn is_ranges(&self) -> bool {
        Self::RANGES
    }

    /// Converts this series into an iterator of ranges.
    ///
    /// If the series already contains ranges, they are returned as-is.
    /// If the series contains individual positions, each position `p` is
    /// converted to a single-element range `p..p+1`.
    ///
    /// # Returns
    ///
    /// An iterator that yields `Range<T>` items. The iterator is `Clone`
    /// to support multiple passes over the data.
    fn into_ranges(self) -> impl IntoIterator<Item = Range<T>> + Clone;

    /// Converts this series into an iterator of individual positions.
    ///
    /// If the series contains individual positions, they are returned as-is.
    /// If the series contains ranges, each range is expanded into its
    /// constituent positions.
    ///
    /// # Returns
    ///
    /// An iterator that yields individual `T` positions. The iterator is `Clone`
    /// to support multiple passes over the data.
    ///
    /// # Note
    ///
    /// When expanding ranges, be aware that large ranges will produce many positions,
    /// which may impact performance and memory usage.
    fn into_positions(self) -> impl IntoIterator<Item = T> + Clone;
}

/// Represents an item in a position series iterator.
///
/// This trait is the core of the type dispatch mechanism. It allows the system
/// to determine at compile time whether an iterator contains individual positions
/// or ranges, and dispatch to the appropriate conversion method.
///
/// # Type Parameters
///
/// * `Num` - The underlying numeric type for positions
///
/// # Implementation Note
///
/// This trait is implemented for numeric types (positions) and `Range<T>` types (ranges),
/// as well as their reference variants.
///
/// The trait uses static dispatch through associated constants and types to ensure
/// zero-cost abstraction. The default implementations panic if called on the wrong
/// variant, but in practice, the correct method is always selected at compile time.
pub trait PositionItem {
    /// The numeric type used for positions in this item.
    type Num;

    /// A compile-time constant indicating whether this item type represents a range.
    ///
    /// - `true` for `Range<T>` types
    /// - `false` for individual position types
    const IS_RANGE: bool;

    /// Converts an iterator of these items into an iterator of ranges.
    ///
    /// # Parameters
    ///
    /// * `into_it` - An iterator that yields items of this type
    ///
    /// # Returns
    ///
    /// An iterator that yields `Range<Self::Num>` items
    ///
    /// # Panics
    ///
    /// The default implementation panics if called on a non-range item type.
    /// However, this should never occur in practice due to compile-time dispatch.
    fn into_ranges(
        _into_it: impl IntoIterator<Item = Self> + Clone,
    ) -> impl IntoIterator<Item = Range<Self::Num>> + Clone {
        if !Self::IS_RANGE {
            unimplemented!("into_ranges");
        }
        std::iter::empty()
    }

    /// Converts an iterator of these items into an iterator of individual positions.
    ///
    /// # Parameters
    ///
    /// * `into_it` - An iterator that yields items of this type
    ///
    /// # Returns
    ///
    /// An iterator that yields individual `Self::Num` positions
    ///
    /// # Panics
    ///
    /// The default implementation panics if called on a range item type.
    /// However, this should never occur in practice due to compile-time dispatch.
    fn into_positions(
        _into_it: impl IntoIterator<Item = Self> + Clone,
    ) -> impl IntoIterator<Item = Self::Num> + Clone {
        if Self::IS_RANGE {
            unimplemented!("into_positions");
        }
        std::iter::empty()
    }
}

impl<T, I> PositionSeries<T> for I
where
    I: IntoIterator + Clone,
    I::Item: PositionItem<Num = T>,
{
    const RANGES: bool = I::Item::IS_RANGE;

    fn into_ranges(self) -> impl IntoIterator<Item = Range<T>> + Clone {
        I::Item::into_ranges(self)
    }

    fn into_positions(self) -> impl IntoIterator<Item = T> + Clone {
        I::Item::into_positions(self)
    }
}

impl PositionItem for u64 {
    type Num = u64;

    const IS_RANGE: bool = false;

    fn into_ranges(
        into_it: impl IntoIterator<Item = Self> + Clone,
    ) -> impl IntoIterator<Item = Range<u64>> + Clone {
        into_it.mapped(|it| it.map(|pos| pos..pos + 1))
    }

    fn into_positions(
        into_it: impl IntoIterator<Item = Self> + Clone,
    ) -> impl IntoIterator<Item = u64> + Clone {
        into_it
    }
}

impl PositionItem for &u64 {
    type Num = u64;

    const IS_RANGE: bool = false;

    fn into_ranges(
        into_it: impl IntoIterator<Item = Self> + Clone,
    ) -> impl IntoIterator<Item = Range<u64>> + Clone {
        into_it.mapped(|it| it.map(|&pos| pos..pos + 1))
    }

    fn into_positions(
        into_it: impl IntoIterator<Item = Self> + Clone,
    ) -> impl IntoIterator<Item = u64> + Clone {
        into_it.copied()
    }
}

impl PositionItem for Range<u64> {
    type Num = u64;

    const IS_RANGE: bool = true;

    fn into_ranges(
        into_it: impl IntoIterator<Item = Self> + Clone,
    ) -> impl IntoIterator<Item = Range<u64>> + Clone {
        into_it
    }

    fn into_positions(
        into_it: impl IntoIterator<Item = Self> + Clone,
    ) -> impl IntoIterator<Item = u64> + Clone {
        into_it.mapped(|it| it.flatten())
    }
}

impl PositionItem for &Range<u64> {
    type Num = u64;

    const IS_RANGE: bool = true;

    fn into_ranges(
        into_it: impl IntoIterator<Item = Self> + Clone,
    ) -> impl IntoIterator<Item = Range<u64>> + Clone {
        into_it.cloned()
    }

    fn into_positions(
        into_it: impl IntoIterator<Item = Self> + Clone,
    ) -> impl IntoIterator<Item = u64> + Clone {
        into_it.cloned().mapped(|it| it.flatten())
    }
}

#[cfg(test)]
mod tests {
    use crate::{RangeIteratorsExt, position_series::PositionSeries};

    #[test]
    fn test_accept_series() {
        assert_eq!(compute([2, 3, 8, 10]), 9);
        assert_eq!(compute_slow([2, 3, 8, 10]), 9);

        assert_eq!(compute([2, 3, 8, 10]), 9);
        assert_eq!(compute_slow([2, 3, 8, 10]), 9);

        assert_eq!(compute([10..20, 30..40]), 30);
        assert_eq!(compute_slow([10..20, 30..40]), 30);

        assert_eq!(compute(&[10..20, 30..40]), 30);
        assert_eq!(compute_slow(&[10..20, 30..40]), 30);

        assert_eq!(compute(2..101), 99);
        assert_eq!(compute_slow(2..101), 99);

        assert_eq!(compute((0..100).step_by(20)), 81);
        assert_eq!(compute_slow((0..100).step_by(20)), 81);

        assert_eq!(compute((10..20).map(|i| i * 10..i * 100)), 1800);
        assert_eq!(compute_slow((10..20).map(|i| i * 10..i * 100)), 1800);

        let v = [2u64, 3, 4, 5, 7, 9];
        assert_eq!(compute(v.iter()), 8);
        assert_eq!(compute_slow(v.iter()), 8);

        assert_eq!(compute(v.iter().copied()), 8);
        assert_eq!(compute_slow(v.iter().copied()), 8);

        assert_eq!(compute(&v[1..]), 7);
        assert_eq!(compute_slow(&v[1..]), 7);

        assert_eq!(compute(std::iter::empty::<u64>()), 0);
        assert_eq!(compute_slow(std::iter::empty::<u64>()), 0);

        let v = vec![2u64..10, 20..30, 40..45, 50..60, 70..71, 90..91];
        assert_eq!(compute(v.iter()), 89);
        assert_eq!(compute_slow(v.iter()), 89);

        assert_eq!(compute(v.iter().skip(1)), 71);
        assert_eq!(compute_slow(v.iter().skip(1)), 71);

        assert_eq!(compute(v.iter().cloned().shift_up(100)), 89);
        assert_eq!(compute_slow(v.iter().cloned().shift_up(100)), 89);

        assert_eq!(compute(&v), 89);
        assert_eq!(compute_slow(&v), 89);
    }

    fn compute(positions: impl PositionSeries<u64>) -> u64 {
        if positions.is_ranges() {
            let mut ranges = positions.into_ranges().into_iter();
            let start = ranges.next().unwrap_or(0..0);
            let end = ranges.last().unwrap_or(start.clone());
            end.end - start.start
        } else {
            let mut positions = positions.into_positions().into_iter();
            let start = match positions.next() {
                Some(pos) => pos,
                None => return 0,
            };
            let end = positions.last().unwrap_or(start);
            end + 1 - start
        }
    }

    fn compute_slow(positions: impl PositionSeries<u64>) -> u64 {
        if positions.is_ranges() {
            let mut positions = positions.into_positions().into_iter();
            let start = match positions.next() {
                Some(pos) => pos,
                None => return 0,
            };
            let end = positions.last().unwrap_or(start);
            end + 1 - start
        } else {
            let mut ranges = positions.into_ranges().into_iter();
            let start = ranges.next().unwrap_or(0..0);
            let end = ranges.last().unwrap_or(start.clone());
            end.end - start.start
        }
    }
}
