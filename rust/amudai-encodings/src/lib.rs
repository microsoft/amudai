//! # Amudai Encodings
//!
//! This crate implements various encodings for data serialization and deserialization within
//! the Amudai database system. It provides efficient and reliable methods to encode and decode
//! data in multiple formats, ensuring compatibility and performance across different data types
//! including primitives, binary data, and complex structures.
//!
//! The crate supports a wide range of encoding schemes optimized for different use cases:
//! - **Plain encoding**: Direct "memcpy"-style operations without compression
//! - **Lightweight compression**: Minimal CPU overhead with basic compression
//! - **Balanced compression**: Efficient encoding cascades for general use
//! - **High compression**: Prioritizes compression ratio over speed
//! - **Minimal size**: Maximum compression regardless of performance impact
//!
//! ## Core Components
//!
//! - **Block Encoders/Decoders**: Handle encoding and decoding of data blocks
//! - **Primitive Encoders/Decoders**: Specialized for numeric and fixed-size data types
//! - **Binary Encoders/Decoders**: Handle variable-length binary data and strings
//! - **Encoding Strategies**: Various compression and optimization algorithms
//!
//! ## SIMD Optimizations
//!
//! Most encoding methods are implemented to allow LLVM to generate SIMD code for
//! optimal performance. Use `cargo-show-asm` to verify SIMD optimizations:
//!
//! ```bash
//! cargo install cargo-show-asm
//! RUSTFLAGS='-C target-cpu=native' cargo asm --release -p amudai-encodings --rust --lib
//! ```

/// Handles decoding of binary (variable-length) data blocks.
pub mod binary_block_decoder;

/// Handles encoding of binary (variable-length) data blocks.
pub mod binary_block_encoder;

/// Core trait and types for block decoders.
pub mod block_decoder;

/// Core traits and types for block encoders, including encoding policies and parameters.
pub mod block_encoder;

/// Internal encoding implementations and strategies.
mod encodings;

/// Handles decoding of primitive (fixed-size) data blocks.
pub mod primitive_block_decoder;

/// Handles encoding of primitive (fixed-size) data blocks.
pub mod primitive_block_encoder;

/// Internal buffer pool for memory management.
mod buffers_pool;

/// Internal data sampling utilities.
mod sampler;
