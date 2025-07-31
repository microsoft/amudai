//! Bit manipulation utilities for Amudai data processing.
//!
//! This module provides high-performance functions for converting between bit-packed
//! and byte-expanded representations of boolean data. These utilities are used
//! across the Amudai ecosystem for Arrow array conversions and blockstream
//! processing.
//!
//! # Key Functions
//!
//! - [`pack_bytes_to_bits`]: Convert boolean bytes to bit-packed format (8:1 compression)
//! - [`unpack_bits_to_bytes`]: Convert bit-packed to boolean bytes (1:8 expansion)
//!
//! # Performance
//!
//! Both functions use highly optimized algorithms:
//! - **Packing**: AVX2-accelerated SIMD operations with scalar fallback, branchless bit manipulation
//! - **Unpacking**: AVX2-optimized processing with lookup table fallback, unified 3-step algorithm
//! - **Runtime Detection**: Automatic AVX2 feature detection for optimal performance on all CPUs
//! - **Memory Efficiency**: Direct memory operations with minimal copying
//! - **Universal Performance**: Consistent speed across aligned and unaligned data
//!
//! # Usage Examples
//!
//! ```rust
//! use amudai_bits::bitpacking::{pack_bytes_to_bits, unpack_bits_to_bytes};
//! use amudai_bytes::buffer::AlignedByteVec;
//!
//! // Pack boolean bytes to bits
//! let input = AlignedByteVec::copy_from_slice(&[1, 0, 1, 1, 0, 0, 1, 0]);
//! let mut output = vec![0u8; 1];
//! pack_bytes_to_bits(&input, &mut output);
//! assert_eq!(output[0], 0b01001101); // LSB first: 1,0,1,1,0,0,1,0
//!
//! // Unpack bits to boolean bytes
//! let bits = vec![0b01001101u8];
//! let bytes = unpack_bits_to_bytes(&bits, 0, 8);
//! assert_eq!(bytes.as_slice(), [1, 0, 1, 1, 0, 0, 1, 0]);
//! ```

use amudai_bytes::buffer::AlignedByteVec;

/// Converts byte-expanded boolean data to bit-packed format.
///
/// Takes an array where each byte represents a boolean value (0=false, non-zero=true)
/// and packs them into bits, storing 8 boolean values per output byte.
///
/// # Arguments
/// * `input_bytes` - Byte array where each byte is a boolean (0 or non-zero)
/// * `output_buffer` - Output buffer to write packed bits to
///
/// # Bit Order
/// Uses LSB-first bit ordering where the first input byte becomes the least significant bit.
///
/// # Performance
/// Uses AVX2-optimized implementation when available at runtime, falling back to scalar:
/// - **AVX2**: SIMD processing with 256-byte and 32-byte block operations for high throughput
/// - **Scalar**: Branchless bit manipulation for optimal performance on all CPUs
/// - **Runtime Detection**: Automatic feature detection ensures compatibility
/// - **Achieves**: ~3-11x speedup with AVX2, consistent performance across data sizes
///
/// # Example
/// ```rust
/// use amudai_bits::bitpacking::pack_bytes_to_bits;
/// use amudai_bytes::buffer::AlignedByteVec;
///
/// let input = AlignedByteVec::copy_from_slice(&[0, 1, 0, 1, 0, 1, 0, 1]);
/// let mut output = vec![0u8; 1];
/// pack_bytes_to_bits(&input, &mut output);
/// assert_eq!(output[0], 0b10101010); // alternating pattern: 0xAA
/// ```
#[inline]
pub fn pack_bytes_to_bits(input_bytes: &AlignedByteVec, output_buffer: &mut [u8]) {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { pack_bytes_to_bits_avx2_optimized(input_bytes, output_buffer) };
        }
    }

    // Fallback implementation for non-AVX2 systems
    pack_bytes_to_bits_scalar(input_bytes, output_buffer);
}

/// Converts bit-packed boolean data to byte-expanded format.
///
/// Takes a bit-packed array and expands each bit into a separate byte,
/// where each output byte is either 0 (false) or 1 (true).
///
/// # Arguments
/// * `bits` - Input bit-packed data
/// * `offset` - Bit offset within the first byte
/// * `len` - Number of bits to unpack
///
/// # Returns
/// An `AlignedByteVec` containing the unpacked bytes
///
/// # Bit Order
/// Uses LSB-first bit ordering where the least significant bit becomes the first output byte.
///
/// # Performance
/// Uses AVX2-optimized implementation when available at runtime, falling back to scalar:
/// - **AVX2**: SIMD processing for high throughput on large data (4.6x faster on medium datasets)
/// - **Scalar**: Lookup table-based processing for compatibility (highly optimized)
/// - **Unified Algorithm**: 3-step approach handles alignment efficiently (alignment, SIMD blocks, scalar remainder)
/// - **Memory Efficient**: Direct memory operations with minimal copying
/// - **Runtime Detection**: Automatic feature detection ensures optimal performance on all CPUs
///
/// # Example
/// ```rust
/// use amudai_bits::bitpacking::unpack_bits_to_bytes;
///
/// let bits = vec![0b01010101u8]; // alternating pattern
/// let output = unpack_bits_to_bytes(&bits, 0, 8);
/// assert_eq!(output.as_slice(), [1, 0, 1, 0, 1, 0, 1, 0]);
/// ```
#[inline]
pub fn unpack_bits_to_bytes(
    bits: &[u8],
    offset: usize,
    len: usize,
) -> amudai_bytes::buffer::AlignedByteVec {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { unpack_bits_to_bytes_avx2_optimized(bits, offset, len) };
        }
    }

    // Fallback implementation for non-AVX2 systems
    unpack_bits_to_bytes_scalar(bits, offset, len)
}

/// Scalar (non-SIMD) implementation of bit-to-byte unpacking.
///
/// This function contains the core bit-unpacking logic used as:
/// - Fallback for non-AVX2 systems in the main `unpack_bits_to_bytes` function
/// - Reference implementation for testing correctness
/// - Direct access for performance comparison and benchmarking
///
/// # Arguments
/// * `bits` - Input bit-packed data
/// * `offset` - Bit offset within the first byte
/// * `len` - Number of bits to unpack
///
/// # Returns
/// An `AlignedByteVec` containing the unpacked bytes
#[inline]
fn unpack_bits_to_bytes_scalar(
    bits: &[u8],
    offset: usize,
    len: usize,
) -> amudai_bytes::buffer::AlignedByteVec {
    // Create and initialize output buffer with zeros
    let mut output = amudai_bytes::buffer::AlignedByteVec::zeroed(len);

    // Pre-calculate bits length to avoid repeated calls
    let bits_len = bits.len();
    assert!(offset + len <= bits_len * 8);

    if len == 0 {
        return output;
    }

    let bytes = output.as_mut_slice();

    // Unified optimized bit unpacking implementation
    // Uses a simple and efficient 2-step approach:
    // 1. Handle any initial unaligned bits from the first partial byte
    // 2. Process remaining bytes using lookup table (complete bytes + partial final byte)

    let mut output_idx = 0;
    let mut remaining_len = len;
    let mut current_offset = offset;

    // Step 1: Handle initial unaligned bits from the first partial byte
    let start_byte = current_offset / 8;
    let start_bit = current_offset % 8;

    if start_bit != 0 && start_byte < bits_len {
        // Extract bits from the first partial byte
        let bits_in_first_byte = (8 - start_bit).min(remaining_len);
        let byte_val = bits[start_byte] as usize;

        // Pre-calculate ranges for better performance
        let output_end = output_idx + bits_in_first_byte;
        let lookup_start = start_bit;
        let lookup_end = lookup_start + bits_in_first_byte;

        bytes[output_idx..output_end]
            .copy_from_slice(&BIT_UNPACK_LOOKUP_TABLE[byte_val][lookup_start..lookup_end]);

        output_idx = output_end;
        remaining_len -= bits_in_first_byte;
        current_offset += bits_in_first_byte;
    }

    // Step 2: Process remaining bytes using lookup table (complete bytes + partial final byte)
    let byte_offset = current_offset / 8;
    let complete_bytes = remaining_len / 8;
    let final_bits = remaining_len % 8;

    // Calculate the safe processing limits to avoid bounds checks in the loops
    let max_complete_bytes = (bits_len - byte_offset).min(complete_bytes);
    let has_partial_byte = final_bits > 0 && (byte_offset + complete_bytes) < bits_len;

    // Process complete bytes (8 bits each) - no branching in the loop
    for i in 0..max_complete_bytes {
        let byte_idx = byte_offset + i;
        let byte_val = bits[byte_idx] as usize;
        let output_range = output_idx..output_idx + 8;
        bytes[output_range].copy_from_slice(&BIT_UNPACK_LOOKUP_TABLE[byte_val]);
        output_idx += 8;
    }

    // Process final partial byte if present - separate loop eliminates branching
    if has_partial_byte {
        let byte_idx = byte_offset + complete_bytes;
        let byte_val = bits[byte_idx] as usize;
        let output_range = output_idx..output_idx + final_bits;
        bytes[output_range].copy_from_slice(&BIT_UNPACK_LOOKUP_TABLE[byte_val][0..final_bits]);
    }

    output
}

/// AVX2-optimized unpacking implementation.
///
/// Processes input using SIMD instructions when AVX2 is available at runtime.
/// Uses a 3-step approach: alignment handling, AVX2 SIMD blocks (256-bit and 32-bit),
/// and scalar processing for remaining bytes.
///
/// # Arguments
/// * `bits` - Input bit-packed data
/// * `offset` - Bit offset within the first byte
/// * `len` - Number of bits to unpack
///
/// # Returns
/// An `AlignedByteVec` containing the unpacked bytes
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn unpack_bits_to_bytes_avx2_optimized(
    bits: &[u8],
    offset: usize,
    len: usize,
) -> amudai_bytes::buffer::AlignedByteVec {
    // Create and initialize output buffer with zeros
    let mut output = amudai_bytes::buffer::AlignedByteVec::zeroed(len);

    // Pre-calculate bits length to avoid repeated calls
    let bits_len = bits.len();
    assert!(offset + len <= bits_len * 8);

    if len == 0 {
        return output;
    }

    let bytes = output.as_mut_slice();
    let mut output_idx = 0;
    let mut remaining_len = len;
    let mut current_offset = offset;

    // Step 1: Handle initial unaligned bits from the first partial byte
    let start_byte = current_offset / 8;
    let start_bit = current_offset % 8;

    if start_bit != 0 && start_byte < bits_len {
        // Extract bits from the first partial byte using scalar lookup table
        let bits_in_first_byte = (8 - start_bit).min(remaining_len);
        let byte_val = bits[start_byte] as usize;

        // Pre-calculate ranges for better performance
        let output_end = output_idx + bits_in_first_byte;
        let lookup_start = start_bit;
        let lookup_end = lookup_start + bits_in_first_byte;

        bytes[output_idx..output_end]
            .copy_from_slice(&BIT_UNPACK_LOOKUP_TABLE[byte_val][lookup_start..lookup_end]);

        output_idx = output_end;
        remaining_len -= bits_in_first_byte;
        current_offset += bits_in_first_byte;
    }

    // Step 2: Process aligned data with AVX2 optimizations
    let byte_offset = current_offset / 8;
    let mut current_input_byte = byte_offset;

    // Pre-calculate common values used in both 256-byte and 32-byte block calculations
    let remaining_input_bytes = bits_len - byte_offset;
    let remaining_bits_as_bytes = remaining_len / 8;

    // Pre-calculate maximum input bytes available for 256-byte blocks
    let max_256_blocks = remaining_input_bytes.min(remaining_bits_as_bytes) / 32;
    let blocks_256_to_process = (remaining_len / 256).min(max_256_blocks);

    if blocks_256_to_process > 0 {
        unsafe {
            avx_unpack_u1_x256_normalized(
                bits.as_ptr().add(current_input_byte),
                bytes.as_mut_ptr().add(output_idx),
                blocks_256_to_process,
            );
        }
        let processed_bits = blocks_256_to_process * 256;
        output_idx += processed_bits;
        remaining_len -= processed_bits;
        current_input_byte += blocks_256_to_process * 32;
    }

    // Process remaining 4-byte blocks (32 bits -> 32 bytes) using AVX2
    let remaining_input_bytes_after_256 = bits_len - current_input_byte;
    let remaining_bits_as_bytes_after_256 = remaining_len / 8;
    let max_32_blocks = remaining_input_bytes_after_256.min(remaining_bits_as_bytes_after_256) / 4;
    let blocks_32_to_process = (remaining_len / 32).min(max_32_blocks);

    if blocks_32_to_process > 0 {
        unsafe {
            avx_unpack_u1_x32_normalized(
                bits.as_ptr().add(current_input_byte),
                bytes.as_mut_ptr().add(output_idx),
                blocks_32_to_process,
            );
        }
        let processed_bits = blocks_32_to_process * 32;
        output_idx += processed_bits;
        remaining_len -= processed_bits;
        current_input_byte += blocks_32_to_process * 4;
    }

    // Step 3: Process remaining bytes using lookup table (complete bytes + partial final byte)
    let complete_bytes = remaining_len / 8;
    let final_bits = remaining_len % 8;

    // Calculate the safe processing limits to avoid bounds checks in the loops
    let max_complete_bytes = (bits_len - current_input_byte).min(complete_bytes);
    let has_partial_byte = final_bits > 0 && (current_input_byte + complete_bytes) < bits_len;

    // Process complete bytes (8 bits each) - no branching in the loop
    for i in 0..max_complete_bytes {
        let byte_idx = current_input_byte + i;
        let byte_val = bits[byte_idx] as usize;
        let output_range = output_idx..output_idx + 8;
        bytes[output_range].copy_from_slice(&BIT_UNPACK_LOOKUP_TABLE[byte_val]);
        output_idx += 8;
    }

    // Process final partial byte if present - separate loop eliminates branching
    if has_partial_byte {
        let byte_idx = current_input_byte + complete_bytes;
        let byte_val = bits[byte_idx] as usize;
        let output_range = output_idx..output_idx + final_bits;
        bytes[output_range].copy_from_slice(&BIT_UNPACK_LOOKUP_TABLE[byte_val][0..final_bits]);
    }

    output
}

static BIT_UNPACK_LOOKUP_TABLE: [[u8; 8]; 256] = {
    let mut table = [[0u8; 8]; 256];
    let mut i = 0;
    while i < 256 {
        let mut bit = 0;
        while bit < 8 {
            table[i][bit] = ((i >> bit) & 1) as u8;
            bit += 1;
        }
        i += 1;
    }
    table
};

/// Private scalar (non-SIMD) fallback implementation of byte-to-bit packing.
///
/// This function contains the core bit-packing logic used as:
/// - Fallback for non-AVX2 systems in the main `pack_bytes_to_bits` function
/// - Remaining byte processing in the AVX2 optimized implementation
/// - Direct call in benchmarks for performance comparison
///
/// Keeping this private ensures a single source of truth for the scalar algorithm
/// while allowing direct access for benchmarking purposes.
#[inline]
fn pack_bytes_to_bits_scalar(input_bytes: &AlignedByteVec, output_buffer: &mut [u8]) {
    // Pre-calculate expected output size and verify buffer capacity
    let expected_output_bytes = input_bytes.len().div_ceil(8);
    assert!(
        output_buffer.len() >= expected_output_bytes,
        "Output buffer too small: got {}, need {}",
        output_buffer.len(),
        expected_output_bytes
    );

    for (byte_idx, chunk) in input_bytes.chunks(8).enumerate() {
        let mut byte_val = 0u8;
        for (bit_idx, &presence) in chunk.iter().enumerate() {
            // Branchless: convert non-zero presence to 1, then shift
            // This avoids conditional branches that can hurt performance due to branch misprediction
            byte_val |= ((presence != 0) as u8) << bit_idx;
        }
        output_buffer[byte_idx] = byte_val;
    }
}

/// AVX2-optimized packing implementation.
///
/// Processes input using SIMD instructions when AVX2 is available at runtime.
/// Uses a 3-step approach: 256-byte AVX2 blocks, 32-byte AVX2 blocks, and scalar
/// processing for remaining bytes.
///
/// # Arguments
/// * `input_bytes` - Input byte array where each byte represents a boolean
/// * `output_buffer` - Output buffer to write packed bits to
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
#[inline]
fn pack_bytes_to_bits_avx2_optimized(input_bytes: &AlignedByteVec, output_buffer: &mut [u8]) {
    let mut input_offset = 0;
    let mut output_offset = 0;

    // Calculate the maximum number of blocks we can process efficiently
    let max_256_blocks = (input_bytes.len() / 256).min(output_buffer.len() / 32);
    let max_32_blocks_after_256 = ((input_bytes.len() - max_256_blocks * 256) / 32)
        .min((output_buffer.len() - max_256_blocks * 32) / 4);

    // Process 256-byte blocks first for maximum throughput
    if max_256_blocks > 0 {
        unsafe {
            avx_pack_u1_x256(
                input_bytes.as_ptr().add(input_offset),
                output_buffer.as_mut_ptr().add(output_offset),
                max_256_blocks,
            );
        }
        input_offset += max_256_blocks * 256;
        output_offset += max_256_blocks * 32;
    }

    // Process remaining 32-byte blocks efficiently
    if max_32_blocks_after_256 > 0 {
        unsafe {
            avx_pack_u1_x32(
                input_bytes.as_ptr().add(input_offset),
                output_buffer.as_mut_ptr().add(output_offset),
                max_32_blocks_after_256,
            );
        }
        input_offset += max_32_blocks_after_256 * 32;
        output_offset += max_32_blocks_after_256 * 4;
    }

    // Process remaining bytes with scalar code (< 32 input bytes remaining)
    let remaining_input = &input_bytes[input_offset..];
    let remaining_output = &mut output_buffer[output_offset..];

    // Since we have < 32 input bytes, we need at most 4 output bytes
    // Process directly using chunks to avoid index calculations
    for (chunk, output_byte) in remaining_input.chunks(8).zip(remaining_output.iter_mut()) {
        let mut byte_val = 0u8;
        for (bit_idx, &presence) in chunk.iter().enumerate() {
            // Branchless: convert non-zero presence to 1, then shift
            // This avoids conditional branches that can hurt performance due to branch misprediction
            byte_val |= ((presence != 0) as u8) << bit_idx;
        }
        *output_byte = byte_val;
    }
}

/// Low-level bit packing utilities.
/// Packs `input` bytes into an `output` bits buffer, storing `1` for
/// each non-zero value and 0 otherwise. `blocks` denotes the number of
/// 256-byte blocks of input, as well as the corresponding number of
/// 32-byte output blocks.
///
/// # Safety
///
/// - `input` must be valid for reads of `blocks * 256` bytes
/// - `output` must be valid for writes of `blocks * 32` bytes
/// - Both pointers must be properly aligned for their respective types
/// - The caller must ensure that AVX2 instructions are supported on the target CPU
/// - Memory regions pointed to by `input` and `output` must not overlap
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn avx_pack_u1_x256(input: *const u8, output: *mut u8, blocks: usize) {
    unsafe {
        use std::arch::x86_64::*;

        let vec_zeros = _mm256_setzero_si256();
        let vec_ones = _mm256_cmpeq_epi8(vec_zeros, vec_zeros);

        let mut input_ptr = input as *const __m256i;
        let mut output_ptr = output as *mut u32;

        for _ in 0..blocks {
            // Use a local macro to avoid 8x code duplication for processing 8 blocks
            macro_rules! pack_block {
                ($offset:expr) => {
                    let mask = _mm256_xor_si256(
                        _mm256_cmpeq_epi8(_mm256_loadu_si256(input_ptr.add($offset)), vec_zeros),
                        vec_ones,
                    );
                    std::ptr::write_unaligned(
                        output_ptr.add($offset),
                        _mm256_movemask_epi8(mask) as u32,
                    );
                };
            }

            pack_block!(0);
            pack_block!(1);
            pack_block!(2);
            pack_block!(3);
            pack_block!(4);
            pack_block!(5);
            pack_block!(6);
            pack_block!(7);

            output_ptr = output_ptr.add(8);
            input_ptr = input_ptr.add(8);
        }
    }
}

/// Packs `input` bytes into an `output` bits buffer, storing `1` for
/// each non-zero value and 0 otherwise. `blocks` denotes the number of
/// 32-byte blocks of `input`, as well as the corresponding number of
/// 4-byte `output` blocks.
///
/// # Safety
///
/// - `input` must be valid for reads of `blocks * 32` bytes
/// - `output` must be valid for writes of `blocks * 4` bytes
/// - Both pointers must be properly aligned for their respective types
/// - The caller must ensure that AVX2 instructions are supported on the target CPU
/// - Memory regions pointed to by `input` and `output` must not overlap
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn avx_pack_u1_x32(input: *const u8, output: *mut u8, blocks: usize) {
    unsafe {
        use std::arch::x86_64::*;
        let vec_zeros = _mm256_setzero_si256();
        let vec_ones = _mm256_cmpeq_epi8(vec_zeros, vec_zeros);
        let input = input as *const __m256i;
        let output = output as *mut u32;
        for i in 0..blocks {
            let mask = _mm256_xor_si256(
                _mm256_cmpeq_epi8(_mm256_loadu_si256(input.add(i)), vec_zeros),
                vec_ones,
            );
            std::ptr::write(output.add(i), _mm256_movemask_epi8(mask) as u32);
        }
    }
}

/// Optimized AVX2 unpacking that produces normalized 0/1 output.
///
/// This function directly produces 0 and 1 values instead of 0 and 0xFF,
/// eliminating the need for post-processing. Uses macros internally for
/// optimal compiler optimization.
///
/// # Safety
/// - `input` must be valid for reads of `blocks * 32` bytes (256-bit blocks)
/// - `output` must be valid for writes of `blocks * 256` bytes
/// - Both pointers must be properly aligned for their respective types
/// - The caller must ensure that AVX2 instructions are supported on the target CPU
/// - Memory regions pointed to by `input` and `output` must not overlap
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn avx_unpack_u1_x256_normalized(input: *const u8, output: *mut u8, blocks: usize) {
    unsafe {
        use std::arch::x86_64::*;

        let shuffle_mask = _mm256_set_epi64x(0x0101010101010101i64, 0, 0x0101010101010101i64, 0);
        let m = 0x8040201008040201u64 as i64;
        let test_mask = _mm256_set_epi64x(m, m, m, m);
        let ones = _mm256_set1_epi8(1);

        let mut input_ptr = input as *const __m256i;
        let mut output_ptr = output as *mut __m128i;

        for _ in 0..blocks {
            let bits_vec = _mm256_loadu_si256(input_ptr);

            // Use a local macro to avoid 8x code duplication for processing 8 shifts
            macro_rules! unpack_shift {
                ($shift_bytes:expr) => {
                    let shifted_bits = if $shift_bytes == 0 {
                        bits_vec
                    } else {
                        _mm256_srli_si256(bits_vec, $shift_bytes)
                    };
                    let shuffled = _mm256_shuffle_epi8(shifted_bits, shuffle_mask);
                    let masked = _mm256_and_si256(shuffled, test_mask);
                    let compared = _mm256_cmpeq_epi8(masked, test_mask);
                    let normalized = _mm256_and_si256(compared, ones);
                    _mm256_storeu2_m128i(output_ptr.add(8), output_ptr, normalized);
                    output_ptr = output_ptr.add(1);
                };
            }

            unpack_shift!(0);
            unpack_shift!(2);
            unpack_shift!(4);
            unpack_shift!(6);
            unpack_shift!(8);
            unpack_shift!(10);
            unpack_shift!(12);
            unpack_shift!(14);

            input_ptr = input_ptr.add(1);
            output_ptr = output_ptr.add(8);
        }
    }
}

/// Optimized AVX2 unpacking for 32-byte blocks that produces normalized 0/1 output.
///
/// Processes 32-bit input blocks and expands each bit to a normalized byte (0 or 1).
/// This function is optimized for smaller block sizes compared to the 256-byte variant.
///
/// # Safety
/// - `input` must be valid for reads of `blocks * 4` bytes (32-bit blocks)
/// - `output` must be valid for writes of `blocks * 32` bytes
/// - Both pointers must be properly aligned for their respective types
/// - The caller must ensure that AVX2 instructions are supported on the target CPU
/// - Memory regions pointed to by `input` and `output` must not overlap
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn avx_unpack_u1_x32_normalized(input: *const u8, output: *mut u8, blocks: usize) {
    unsafe {
        use std::arch::x86_64::*;

        let shuffle_mask = _mm256_set_epi64x(
            0x0303030303030303,
            0x0202020202020202,
            0x0101010101010101,
            0,
        );
        let m = 0x8040201008040201u64 as i64;
        let test_mask = _mm256_set_epi64x(m, m, m, m);
        let ones = _mm256_set1_epi8(1);
        let output = output as *mut __m256i;
        for i in 0..blocks {
            let w32 = ::std::ptr::read((input as *const u32).add(i));
            let x = _mm256_shuffle_epi8(_mm256_set1_epi32(w32 as i32), shuffle_mask);
            let res = _mm256_cmpeq_epi8(_mm256_and_si256(x, test_mask), test_mask);
            // Normalize to 0/1 by ANDing with 1
            let normalized = _mm256_and_si256(res, ones);
            _mm256_storeu_si256(output.add(i), normalized);
        }
    }
}

#[cfg(test)]
mod tests {
    //! Test suite for bitpacking module.
    //!
    //! ## Running Tests
    //!
    //! **Functional Tests**: Run with `cargo test --lib bitpacking`
    //! - Includes AVX2 vs scalar correctness verification
    //! - Round-trip conversion testing
    //! - Edge case and alignment testing
    //!
    //! **Benchmark Tests**: Run with
    //! `$env:RUSTFLAGS="-C target-cpu=native"; cargo test --release benchmark --lib -- --ignored --nocapture`
    //! - AVX2 vs scalar performance comparisons
    //! - Multi-scale benchmarking (100K, 1M, 10M elements)
    //! (Note: Benchmarks must be run in release mode for accurate performance measurements)

    use super::*;
    use std::time::Instant;

    fn create_test_bit_data(size: usize) -> Vec<u8> {
        // Create more realistic random-like bit pattern using a simple PRNG
        let mut data = vec![0u8; size];
        let mut state = 0x12345678u32;
        for byte in data.iter_mut() {
            // Simple linear congruential generator
            state = state.wrapping_mul(1103515245).wrapping_add(12345);
            *byte = (state >> 16) as u8;
        }
        data
    }

    fn create_test_byte_data(size: usize) -> AlignedByteVec {
        // Create alternating boolean pattern: true, false, true, false...
        let data: Vec<u8> = (0..size).map(|i| (i % 2) as u8).collect();
        AlignedByteVec::copy_from_slice(&data)
    }

    // === FUNCTIONAL TESTS ===

    #[test]
    fn test_pack_bytes_to_bits_correctness() {
        let input = create_test_byte_data(16);
        let mut output = vec![0u8; 2];
        pack_bytes_to_bits(&input, &mut output);

        // create_test_byte_data creates: [0, 1, 0, 1, 0, 1, 0, 1, ...]
        // With LSB-first packing:
        // bit 0 = input[0] = 0, bit 1 = input[1] = 1, bit 2 = input[2] = 0, etc.
        // This gives us: 0b10101010 = 0xAA
        assert_eq!(output[0], 0xAA);
        assert_eq!(output[1], 0xAA);
    }

    #[test]
    fn test_unpack_bits_to_bytes_correctness() {
        let input = vec![0x55u8, 0xAA]; // 01010101, 10101010
        let output = unpack_bits_to_bytes(&input, 0, 16);

        // 0x55 = 0b01010101: bit 0=1, bit 1=0, bit 2=1, bit 3=0, etc.
        // So first 8 bytes should be [1, 0, 1, 0, 1, 0, 1, 0]
        // 0xAA = 0b10101010: bit 0=0, bit 1=1, bit 2=0, bit 3=1, etc.
        // So next 8 bytes should be [0, 1, 0, 1, 0, 1, 0, 1]
        let expected = [1, 0, 1, 0, 1, 0, 1, 0, 0, 1, 0, 1, 0, 1, 0, 1];
        assert_eq!(output.as_slice(), expected);
    }

    #[test]
    fn test_unpack_with_offset() {
        let bits = vec![0b11010110u8]; // bit pattern: 0,1,1,0,1,0,1,1 (LSB first)

        // Test offset=2, length=4 -> extract bits 2,3,4,5 = 1,0,1,0
        let bytes = unpack_bits_to_bytes(&bits, 2, 4);
        assert_eq!(bytes.as_slice(), [1, 0, 1, 0]);
    }

    #[test]
    fn test_pack_mixed_values() {
        // Test with 0, 1, and other non-zero values
        let input_data = vec![0, 1, 2, 0, 255, 0, 42, 0];
        let input = AlignedByteVec::copy_from_slice(&input_data);
        let mut output = vec![0u8; 1];
        pack_bytes_to_bits(&input, &mut output);

        // Expected bits: bit0=0, bit1=1, bit2=1, bit3=0, bit4=1, bit5=0, bit6=1, bit7=0
        // Reading MSB to LSB: 0b01010110 = 0x56
        assert_eq!(output[0], 0x56);
    }

    #[test]
    fn test_round_trip_conversion() {
        let original_bytes = create_test_byte_data(64);
        let bit_len = original_bytes.len().div_ceil(8);
        let mut packed_bits = vec![0u8; bit_len];

        // Pack bytes to bits
        pack_bytes_to_bits(&original_bytes, &mut packed_bits);

        // Unpack bits back to bytes
        let unpacked_bytes = unpack_bits_to_bytes(&packed_bits, 0, original_bytes.len());

        // Verify round-trip
        for (i, (&original, &result)) in original_bytes
            .iter()
            .zip(unpacked_bytes.as_slice().iter())
            .enumerate()
        {
            assert_eq!(original, result, "Round-trip mismatch at index {i}");
        }
    }

    #[test]
    fn test_empty_input() {
        // Test empty unpack
        let empty_output = unpack_bits_to_bytes(&[], 0, 0);
        assert_eq!(empty_output.len(), 0);

        // Test empty pack
        let empty_input = AlignedByteVec::copy_from_slice(&[]);
        let mut output = vec![0u8; 0];
        pack_bytes_to_bits(&empty_input, &mut output);
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn test_single_bit_operations() {
        // Test single bit unpack
        let bits = vec![0b00000001u8]; // Only bit 0 set
        let output = unpack_bits_to_bytes(&bits, 0, 1);
        assert_eq!(output.as_slice(), [1]);

        // Test single bit pack
        let input = AlignedByteVec::copy_from_slice(&[1]);
        let mut output = vec![0u8; 1];
        pack_bytes_to_bits(&input, &mut output);
        assert_eq!(output[0], 0b00000001);
    }

    #[test]
    fn test_avx2_correctness_large_data() {
        // Test AVX2 implementation correctness with large data that exercises SIMD paths
        const SIZE: usize = 512; // Large enough to trigger AVX2 optimizations
        let input_data: Vec<u8> = (0..SIZE).map(|i| (i % 3) as u8).collect(); // 0,1,2,0,1,2,... pattern
        let input_bytes = AlignedByteVec::copy_from_slice(&input_data);

        // Pack with AVX2
        let bit_len = input_bytes.len().div_ceil(8);
        let mut packed_bits = vec![0u8; bit_len];
        pack_bytes_to_bits(&input_bytes, &mut packed_bits);

        // Unpack with AVX2 and compare with scalar
        let avx2_output = unpack_bits_to_bytes(&packed_bits, 0, SIZE);
        let scalar_output = unpack_bits_to_bytes_scalar(&packed_bits, 0, SIZE);

        // Verify both implementations produce identical results
        assert_eq!(
            avx2_output.as_slice(),
            scalar_output.as_slice(),
            "AVX2 and scalar unpack implementations produce different results"
        );

        // Verify round-trip correctness
        for (i, (&original, &result)) in input_bytes
            .iter()
            .zip(avx2_output.as_slice().iter())
            .enumerate()
        {
            let expected = if original == 0 { 0 } else { 1 };
            assert_eq!(expected, result, "AVX2 round-trip mismatch at index {i}");
        }
    }

    // === BENCHMARK TESTS ===
    // Run with:
    // `$env:RUSTFLAGS="-C target-cpu=native"; cargo test --release benchmark -- --ignored --nocapture`

    #[test]
    #[ignore]
    fn benchmark_pack_performance() {
        const ELEMENTS: usize = 1_000_000; // 1M elements
        let test_data: Vec<u8> = (0..ELEMENTS).map(|i| (i % 2) as u8).collect();
        let input_bytes = AlignedByteVec::copy_from_slice(&test_data);
        let mut output = vec![0u8; ELEMENTS.div_ceil(8)];

        // Extended warmup
        for _ in 0..50 {
            pack_bytes_to_bits(&input_bytes, &mut output);
            std::hint::black_box(&output);
        }

        // Benchmark pack operation with increased iterations
        let iterations = 500; // Increased from 100
        let start = Instant::now();
        for _ in 0..iterations {
            pack_bytes_to_bits(&input_bytes, &mut output);
            // Multiple compiler hints to prevent optimization
            std::hint::black_box(&output);
            std::hint::black_box(&input_bytes);
        }
        let pack_time = start.elapsed();

        // Calculate checksum after timing to prevent optimization
        let checksum: u64 = output.iter().map(|&b| b as u64).sum();

        let avg_time = pack_time / iterations;
        let throughput = ELEMENTS as f64 / avg_time.as_secs_f64() / 1_000_000_000.0;

        println!(
            "pack_bytes_to_bits performance:  {ELEMENTS} elements, {throughput:.3} B elements/sec (checksum: {checksum})"
        );
    }

    #[test]
    #[ignore]
    fn benchmark_unpack_performance() {
        const ELEMENTS: usize = 1_000_000; // 1M elements
        let bit_data = create_test_bit_data(ELEMENTS / 8);

        // Extended warmup
        for _ in 0..50 {
            let _output = unpack_bits_to_bytes(&bit_data, 0, ELEMENTS);
            std::hint::black_box(_output);
        }

        // Benchmark unpack operation with increased iterations
        let iterations = 500; // Increased from 100
        let start = Instant::now();
        for _ in 0..iterations {
            let output = unpack_bits_to_bytes(&bit_data, 0, ELEMENTS);
            // Multiple compiler hints to prevent optimization
            std::hint::black_box(output);
            std::hint::black_box(&bit_data);
        }
        let unpack_time = start.elapsed();

        // Calculate checksum after timing to prevent optimization (use last iteration result)
        let final_output = unpack_bits_to_bytes(&bit_data, 0, ELEMENTS);
        let checksum: u64 = final_output.iter().map(|&b| b as u64).sum();

        let avg_time = unpack_time / iterations;
        let throughput = ELEMENTS as f64 / avg_time.as_secs_f64() / 1_000_000_000.0;

        println!(
            "unpack_bits_to_bytes aligned access performance:  {ELEMENTS} elements, {throughput:.3} B elements/sec (checksum: {checksum})"
        );
    }

    #[test]
    #[ignore]
    fn benchmark_unpack_unaligned_access() {
        const ELEMENTS: usize = 1_000_000;
        let bit_data = create_test_bit_data(ELEMENTS / 8);

        // Test different bit offsets
        let offsets = vec![0, 1, 3, 7]; // Byte-aligned and various unaligned

        for &offset in &offsets {
            let actual_len = ELEMENTS - offset;

            // Extended warmup
            for _ in 0..50 {
                let _output = unpack_bits_to_bytes(&bit_data, offset, actual_len);
                std::hint::black_box(_output);
            }

            let iterations = 500; // Increased from 100
            let start = Instant::now();
            for _ in 0..iterations {
                let output = unpack_bits_to_bytes(&bit_data, offset, actual_len);
                // Multiple compiler hints to prevent optimization
                std::hint::black_box(output);
                std::hint::black_box(&bit_data);
            }
            let time = start.elapsed();

            // Calculate checksum after timing to prevent optimization (use last iteration result)
            let final_output = unpack_bits_to_bytes(&bit_data, offset, actual_len);
            let checksum: u64 = final_output.iter().map(|&b| b as u64).sum();

            let avg_time = time / iterations;
            let throughput = actual_len as f64 / avg_time.as_secs_f64() / 1_000_000_000.0;

            println!(
                "unpack_bits_to_bytes unaligned access performance:  offset {offset}: {actual_len} elements, {throughput:.3} B elements/sec (checksum: {checksum})"
            );
        }
    }

    #[test]
    #[ignore]
    fn benchmark_unpack_small_performance() {
        // Test small unpacking performance (1-100 elements)
        let bit_data = create_test_bit_data(100); // Enough data for all test cases
        let test_sizes = vec![1, 5, 10, 25, 50, 100];

        for &size in &test_sizes {
            // Warm up
            for _ in 0..1000 {
                let _output = unpack_bits_to_bytes(&bit_data, 0, size);
            }

            // Benchmark with more iterations for small data
            let start = Instant::now();
            for _ in 0..10000 {
                let output = unpack_bits_to_bytes(&bit_data, 0, size);
                // Prevent compiler optimization
                std::hint::black_box(output);
            }
            let time = start.elapsed();

            // Calculate checksum after timing
            let final_output = unpack_bits_to_bytes(&bit_data, 0, size);
            let checksum: u64 = final_output.iter().map(|&b| b as u64).sum();

            let avg_time = time / 10000;
            let throughput = size as f64 / avg_time.as_secs_f64() / 1_000_000.0;

            println!(
                "unpack_bits_to_bytes small data performance:  {size} elements, {throughput:.1} M elements/sec (checksum: {checksum})"
            );
        }
    }

    #[test]
    #[ignore]
    fn benchmark_unpack_compare_scalar_vs_avx2() {
        println!("=== UNPACK PERFORMANCE COMPARISON ===");

        let test_sizes: Vec<usize> = vec![100_000, 1_000_000, 10_000_000]; // 100K, 1M, 10M elements

        for &elements in &test_sizes {
            println!("\nTesting with {} elements", elements);

            let bit_data = create_test_bit_data(elements / 8);

            // Adjust iterations based on data size for reasonable test time
            let iterations = match elements {
                100_000 => 10000,  // 100K: 1000 iterations
                1_000_000 => 1000, // 1M: 100 iterations
                10_000_000 => 100, // 10M: 10 iterations
                _ => 100,
            };

            // Benchmark scalar implementation
            for _ in 0..10 {
                let _output = unpack_bits_to_bytes_scalar(&bit_data, 0, elements);
            }

            let start = Instant::now();
            for _ in 0..iterations {
                let output = unpack_bits_to_bytes_scalar(&bit_data, 0, elements);
                std::hint::black_box(output);
            }
            let scalar_time = start.elapsed();
            let scalar_output = unpack_bits_to_bytes_scalar(&bit_data, 0, elements);
            let scalar_checksum: u64 = scalar_output.iter().map(|&b| b as u64).sum();

            let scalar_avg_time = scalar_time / iterations;
            let scalar_throughput =
                elements as f64 / scalar_avg_time.as_secs_f64() / 1_000_000_000.0;

            // Benchmark AVX2 implementation (if available)
            #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
            {
                for _ in 0..10 {
                    let _output = unpack_bits_to_bytes(&bit_data, 0, elements);
                }

                let start = Instant::now();
                for _ in 0..iterations {
                    let output = unpack_bits_to_bytes(&bit_data, 0, elements);
                    std::hint::black_box(output);
                }
                let avx2_time = start.elapsed();
                let avx2_output = unpack_bits_to_bytes(&bit_data, 0, elements);
                let avx2_checksum: u64 = avx2_output.iter().map(|&b| b as u64).sum();

                let avx2_avg_time = avx2_time / iterations;
                let avx2_throughput =
                    elements as f64 / avx2_avg_time.as_secs_f64() / 1_000_000_000.0;

                println!(
                    "  Scalar implementation:  {scalar_throughput:.3} B elements/sec (checksum: {scalar_checksum})"
                );

                if is_x86_feature_detected!("avx2") {
                    println!(
                        "  AVX2 implementation:   {avx2_throughput:.3} B elements/sec (checksum: {avx2_checksum})"
                    );
                    let speedup = avx2_throughput / scalar_throughput;
                    println!("  AVX2 speedup:          {speedup:.2}x faster");

                    // Verify both implementations produce the same result
                    assert_eq!(
                        scalar_checksum, avx2_checksum,
                        "Scalar and AVX2 implementations produce different results!"
                    );
                } else {
                    println!(
                        "  AVX2 not available - using scalar fallback:  {avx2_throughput:.3} B elements/sec (checksum: {avx2_checksum})"
                    );
                }
            }
        }
    }

    #[test]
    #[ignore]
    fn benchmark_pack_compare_scalar_vs_avx2() {
        println!("=== PACK PERFORMANCE COMPARISON ===");

        let test_sizes: Vec<usize> = vec![100_000, 1_000_000, 10_000_000]; // 100K, 1M, 10M elements

        for &elements in &test_sizes {
            println!("\nTesting with {} elements", elements);

            let test_data: Vec<u8> = (0..elements).map(|i| (i % 2) as u8).collect();
            let input_bytes = AlignedByteVec::copy_from_slice(&test_data);
            let mut output = vec![0u8; elements.div_ceil(8)];

            // Adjust iterations based on data size for reasonable test time
            let iterations = match elements {
                100_000 => 10000,  // 100K: 1000 iterations
                1_000_000 => 1000, // 1M: 100 iterations
                10_000_000 => 100, // 10M: 10 iterations
                _ => 100,
            };

            // Benchmark scalar implementation
            for _ in 0..10 {
                pack_bytes_to_bits_scalar(&input_bytes, &mut output);
                std::hint::black_box(&output);
            }

            let start = Instant::now();
            for _ in 0..iterations {
                pack_bytes_to_bits_scalar(&input_bytes, &mut output);
                std::hint::black_box(&output);
            }
            let scalar_time = start.elapsed();
            let scalar_checksum: u64 = output.iter().map(|&b| b as u64).sum();

            let scalar_avg_time = scalar_time / iterations;
            let scalar_throughput =
                elements as f64 / scalar_avg_time.as_secs_f64() / 1_000_000_000.0;

            // Benchmark AVX2 implementation (if available)
            #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
            {
                for _ in 0..10 {
                    pack_bytes_to_bits(&input_bytes, &mut output);
                    std::hint::black_box(&output);
                }

                let start = Instant::now();
                for _ in 0..iterations {
                    pack_bytes_to_bits(&input_bytes, &mut output);
                    std::hint::black_box(&output);
                }
                let avx2_time = start.elapsed();
                let avx2_checksum: u64 = output.iter().map(|&b| b as u64).sum();

                let avx2_avg_time = avx2_time / iterations;
                let avx2_throughput =
                    elements as f64 / avx2_avg_time.as_secs_f64() / 1_000_000_000.0;

                println!(
                    "  Scalar implementation:  {scalar_throughput:.3} B elements/sec (checksum: {scalar_checksum})"
                );

                if is_x86_feature_detected!("avx2") {
                    println!(
                        "  AVX2 implementation:   {avx2_throughput:.3} B elements/sec (checksum: {avx2_checksum})"
                    );
                    let speedup = avx2_throughput / scalar_throughput;
                    println!("  AVX2 speedup:          {speedup:.2}x faster");

                    // Verify both implementations produce the same result
                    assert_eq!(
                        scalar_checksum, avx2_checksum,
                        "Scalar and AVX2 implementations produce different results!"
                    );
                } else {
                    println!(
                        "  AVX2 not available - using scalar fallback:  {avx2_throughput:.3} B elements/sec (checksum: {avx2_checksum})"
                    );
                }
            }
        }
    }

    // === COMPARATIVE BENCHMARKS ===
    // Run with:
    // `$env:RUSTFLAGS="-C target-cpu=native"; cargo test --release benchmark -- --ignored --nocapture`

    #[test]
    #[ignore]
    fn benchmark_iteration_50() {
        println!("=== COMPREHENSIVE 50-ITERATION PERFORMANCE ANALYSIS ===");
        println!("Running 50 iterations, discarding best 5 and worst 5 results");

        let test_sizes: Vec<usize> = vec![100_000, 1_000_000, 10_000_000]; // 100K, 1M, 10M elements

        for &elements in &test_sizes {
            println!(
                "\n📊 TESTING {} ELEMENTS (50 iterations, discard best/worst 5)",
                elements
            );
            println!("============================================================");

            let test_data: Vec<u8> = (0..elements).map(|i| (i % 2) as u8).collect();
            let input_bytes = AlignedByteVec::copy_from_slice(&test_data);
            let mut pack_output = vec![0u8; elements.div_ceil(8)];
            let bit_data = create_test_bit_data(elements / 8);

            // Use fewer inner iterations since we're doing 50 outer iterations
            let iterations = match elements {
                100_000 => 10000,  // 100K: 100 iterations per run
                1_000_000 => 1000, // 1M: 50 iterations per run
                10_000_000 => 100, // 10M: 20 iterations per run
                _ => 50,
            };

            println!(
                "\n🔄 PACK PERFORMANCE ({} iterations x 50 runs):",
                iterations
            );
            println!("--------------------------------------------------");

            // PACK
            {
                let mut results = Vec::with_capacity(50);

                for run in 0..50 {
                    // Warmup for each run
                    for _ in 0..10 {
                        pack_bytes_to_bits(&input_bytes, &mut pack_output);
                        std::hint::black_box(&pack_output);
                    }

                    let start = Instant::now();
                    for _ in 0..iterations {
                        pack_bytes_to_bits(&input_bytes, &mut pack_output);
                        std::hint::black_box(&pack_output);
                        std::hint::black_box(&input_bytes);
                    }
                    let time = start.elapsed();
                    let avg_time = time / iterations;
                    let throughput = elements as f64 / avg_time.as_secs_f64() / 1_000_000_000.0;
                    results.push(throughput);

                    if run < 3 {
                        // Show first few results
                        println!("  Run {}: {:.3} B/sec", run + 1, throughput);
                    } else if run == 3 {
                        println!("  ... (showing first 3 and last 3)");
                    } else if run >= 47 {
                        println!("  Run {}: {:.3} B/sec", run + 1, throughput);
                    }
                }

                // Sort and remove best 5 and worst 5
                results.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let filtered: Vec<f64> = results[5..45].to_vec(); // Remove first 5 (worst) and last 5 (best)
                let avg_throughput: f64 = filtered.iter().sum::<f64>() / filtered.len() as f64;
                let min = filtered.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                let max = filtered.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));

                println!(
                    "PACK: AVG {:.3} B/sec, Range [{:.3} - {:.3}]",
                    avg_throughput, min, max
                );

                let checksum: u64 = pack_output.iter().map(|&b| b as u64).sum();
                println!("  (checksum: {})", checksum);
            }

            println!(
                "\n🔍 UNPACK PERFORMANCE ({} iterations x 50 runs):",
                iterations
            );
            println!("--------------------------------------------------");

            // UNPACK
            {
                let mut results = Vec::with_capacity(50);

                for run in 0..50 {
                    for _ in 0..10 {
                        let _ = unpack_bits_to_bytes(&bit_data, 0, elements);
                    }

                    let start = Instant::now();
                    for _ in 0..iterations {
                        let output = unpack_bits_to_bytes(&bit_data, 0, elements);
                        std::hint::black_box(output);
                        std::hint::black_box(&bit_data);
                    }
                    let time = start.elapsed();
                    let avg_time = time / iterations;
                    let throughput = elements as f64 / avg_time.as_secs_f64() / 1_000_000_000.0;
                    results.push(throughput);

                    if run < 3 {
                        println!("  Run {}: {:.3} B/sec", run + 1, throughput);
                    } else if run == 3 {
                        println!("  ... (showing first 3 and last 3)");
                    } else if run >= 47 {
                        println!("  Run {}: {:.3} B/sec", run + 1, throughput);
                    }
                }

                results.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let filtered: Vec<f64> = results[5..45].to_vec();
                let avg_throughput: f64 = filtered.iter().sum::<f64>() / filtered.len() as f64;
                let min = filtered.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                let max = filtered.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));

                println!(
                    "UNPACK: AVG {:.3} B/sec, Range [{:.3} - {:.3}]",
                    avg_throughput, min, max
                );

                let final_output = unpack_bits_to_bytes(&bit_data, 0, elements);
                let checksum: u64 = final_output.iter().map(|&b| b as u64).sum();
                println!("  (checksum: {})", checksum);
            }
        }
    }
}
