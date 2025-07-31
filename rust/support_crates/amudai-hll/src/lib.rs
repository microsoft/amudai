extern crate byteorder;
#[cfg(test)]
extern crate rand;

mod bias;
pub mod config;
pub mod hll;
pub mod hll_sketch;

#[cfg(test)]
mod tests;

// Re-export main types
pub use config::HllSketchConfig;
pub use hll_sketch::HllSketch;
