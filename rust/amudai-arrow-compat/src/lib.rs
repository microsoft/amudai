//! `amudai-arrow-compat` provides compatibility layers between Amudai formats and Apache Arrow.
//!
//! This crate includes:
//! - Conversion utilities for schemas:
//!   - Arrow Schema to Amudai Schema (`arrow_to_amudai_schema`)
//!   - Amudai Schema to Arrow Schema (`amudai_to_arrow_schema`)
//! - Support for handling Amudai `AnyValue` with Arrow.
//! - Buffer utilities for Arrow interop.
//! - Zero-copy conversion of the Amudai sequences to basic Arrow arrays.
//! - Amudai/Arrow error conversion

pub mod amudai_to_arrow_error;
pub mod amudai_to_arrow_schema;
pub mod any_value_support;
pub mod arrow_fields;
pub mod arrow_to_amudai_schema;
pub mod buffers;
pub mod datetime_conversions;
pub mod sequence_to_array;
