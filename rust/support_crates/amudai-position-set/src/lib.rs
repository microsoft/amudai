pub mod bit_array;
pub mod bit_store;
pub mod position_set;
pub mod position_set_builder;
pub mod segment;
#[cfg(test)]
mod tests;

pub use position_set::PositionSet;

pub struct TernaryPositionSet;
