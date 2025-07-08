//! Shard creation (low-level API).

pub mod artifact_writer;
pub mod decimal_index;
pub mod field_builder;
pub mod field_descriptor;
pub mod field_encoder;
pub mod format_elements_ext;
pub mod numeric_index;
pub mod shard_builder;
pub mod stripe_builder;
#[cfg(test)]
mod tests;
