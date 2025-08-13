//! Efficient manipulation and storage of numeric ranges.
//!
//! This crate provides utilities for working with sequences of `Range<T>` values,
//! particularly focused on `u64` ranges. It offers:
//!
//! - **Range transformations**: Iterator adapters for chunking, shifting, and other operations
//! - **Shared storage**: Immutable, cheaply-cloneable range lists with efficient slicing
//! - **Generic abstractions**: Work with both individual positions and ranges uniformly
//!
//! # Key Types
//!
//! - [`SharedRangeList`] - An immutable list of non-overlapping ranges with cheap cloning
//! - [`RangeIteratorsExt`] - Extension trait providing range transformation methods
//! - [`PositionSeries`] - Abstraction over sequences of positions or ranges

pub mod into_iter_adapters;
pub mod position_series;
pub mod put_back;
pub mod range_list_slice;
pub mod set_ops;
pub mod shared_range_list;
pub mod slice_ext;
pub mod transform;

pub use into_iter_adapters::IntoIteratorExt;
pub use position_series::{PositionItem, PositionSeries};
pub use range_list_slice::RangeListSlice;
pub use shared_range_list::SharedRangeList;
pub use transform::RangeIteratorsExt;
