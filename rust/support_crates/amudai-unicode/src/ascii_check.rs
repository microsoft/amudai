//! Fast ASCII string validation using AVX2 instructions when available.

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use std::arch::x86_64::{_mm256_loadu_si256, _mm256_movemask_epi8, _mm256_or_si256};

/// The function returns true if all bytes in `src` are
/// 7-bit values (0x00..0x7F). Otherwise, it returns false.
/// Uses AVX2 instructions when available on x86/x86_64 platforms.
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn is_ascii_string_avx2(src: &[u8]) -> bool {
    let len = src.len();
    let src = src.as_ptr();
    let mut i = 0;

    if len == 0 {
        return true;
    }

    // Process in batches of 4 chunks (128 bytes) for better parallelism
    while i + 128 <= len {
        // Unroll 4 chunks for better parallelism and instruction pipelining
        unsafe {
            let chunk1 = _mm256_loadu_si256(src.add(i) as *const _);
            let chunk2 = _mm256_loadu_si256(src.add(i + 32) as *const _);
            let chunk3 = _mm256_loadu_si256(src.add(i + 64) as *const _);
            let chunk4 = _mm256_loadu_si256(src.add(i + 96) as *const _);

            // Tree-based combining for better ILP (2 levels instead of linear chain)
            // This allows the CPU to execute OR operations in parallel
            let combined_12 = _mm256_or_si256(chunk1, chunk2);
            let combined_34 = _mm256_or_si256(chunk3, chunk4);
            let batch_error = _mm256_or_si256(combined_12, combined_34);

            // Single branch check per 128 bytes
            let error_mask = _mm256_movemask_epi8(batch_error);
            if error_mask != 0 {
                return false;
            }
            i += 128;
        }
    }

    // Process remaining 64-byte chunks (2 x 32-byte AVX2 operations)
    while i + 64 <= len {
        unsafe {
            let chunk1 = _mm256_loadu_si256(src.add(i) as *const _);
            let chunk2 = _mm256_loadu_si256(src.add(i + 32) as *const _);

            let combined = _mm256_or_si256(chunk1, chunk2);
            let error_mask = _mm256_movemask_epi8(combined);
            if error_mask != 0 {
                return false;
            }
        }
        i += 64;
    }

    // Process remaining 32-byte chunks
    while i + 32 <= len {
        unsafe {
            let current_bytes = _mm256_loadu_si256(src.add(i) as *const _);
            let error_mask = _mm256_movemask_epi8(current_bytes);
            if error_mask != 0 {
                return false;
            }
        }
        i += 32;
    }

    // Process remaining bytes (tail) - optimized for small tails
    while i + 8 <= len {
        unsafe {
            let eight_bytes = (src.add(i) as *const u64).read_unaligned();
            if eight_bytes & 0x8080808080808080 != 0 {
                return false;
            }
        }
        i += 8;
    }

    // Process final 1-7 bytes
    while i < len {
        unsafe {
            if *src.add(i) & 0x80 != 0 {
                return false;
            }
        }
        i += 1;
    }

    true
}

/// Fallback implementation using Rust's standard library.
/// The function returns true if all bytes in `src` are
/// 7-bit values (0x00..0x7F). Otherwise, it returns false.
#[inline]
fn is_ascii_string_fallback(src: &[u8]) -> bool {
    src.is_ascii()
}

/// The function returns true if all bytes in `src` are
/// 7-bit values (0x00..0x7F). Otherwise, it returns false.
/// Uses the fastest available implementation for the current platform.
#[inline]
fn is_ascii_string(src: &[u8]) -> bool {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            unsafe { is_ascii_string_avx2(src) }
        } else {
            is_ascii_string_fallback(src)
        }
    }

    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    {
        is_ascii_string_fallback(src)
    }
}

pub trait IsAsciiFast {
    /// Uses AVX2 instructions to determine whether `self`
    /// only contains 7-bit values (0x00..0x7F).
    fn is_ascii_fast(&self) -> bool;
}

impl IsAsciiFast for [u8] {
    fn is_ascii_fast(&self) -> bool {
        is_ascii_string(self)
    }
}

impl IsAsciiFast for &[u8] {
    fn is_ascii_fast(&self) -> bool {
        is_ascii_string(self)
    }
}

impl IsAsciiFast for &str {
    fn is_ascii_fast(&self) -> bool {
        is_ascii_string(self.as_bytes())
    }
}

impl IsAsciiFast for String {
    fn is_ascii_fast(&self) -> bool {
        is_ascii_string(self.as_bytes())
    }
}

impl IsAsciiFast for Vec<u8> {
    fn is_ascii_fast(&self) -> bool {
        is_ascii_string(self.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use crate::ascii_check::{IsAsciiFast, is_ascii_string_fallback};
    use rand::{Rng, SeedableRng};

    #[test]
    fn test_ascii_check() {
        assert!(b"".is_ascii_fast());
        assert!(!"ascii string with 😜 in it".is_ascii_fast());
        assert!("pure ascii string".is_ascii_fast());
        let mut rng = rand::rngs::SmallRng::from_rng(&mut rand::rng());
        let v = (0..25000)
            .map(|_| rng.random_range(0x00..0x7F) as u8)
            .collect::<Vec<_>>();
        assert!(v.as_slice().is_ascii_fast());
    }

    #[test]
    fn test_ascii_check_fallback() {
        // Test the fallback implementation directly
        assert!(is_ascii_string_fallback(b""));
        assert!(is_ascii_string_fallback(b"hello world"));
        assert!(is_ascii_string_fallback(b"ASCII string with numbers 123"));
        assert!(!is_ascii_string_fallback("non-ascii 😜".as_bytes()));
        assert!(!is_ascii_string_fallback(&[0x80])); // First non-ASCII byte
        assert!(!is_ascii_string_fallback(&[0xFF])); // Last byte

        // Test edge cases
        let ascii_bytes: Vec<u8> = (0x00..0x80).collect();
        assert!(is_ascii_string_fallback(&ascii_bytes));

        let non_ascii_bytes: Vec<u8> = (0x80..=0xFF).collect();
        assert!(!is_ascii_string_fallback(&non_ascii_bytes));
    }

    #[test]
    fn test_ascii_check_comprehensive() {
        // Test empty string
        assert!(b"".is_ascii_fast());
        assert!("".is_ascii_fast());
        assert!(Vec::<u8>::new().is_ascii_fast());

        // Test single characters
        for i in 0x00..0x80 {
            let single_ascii = vec![i];
            assert!(
                single_ascii.is_ascii_fast(),
                "ASCII byte {i:#x} should pass"
            );
        }

        for i in 0x80..=0xFF {
            let single_non_ascii = vec![i];
            assert!(
                !single_non_ascii.is_ascii_fast(),
                "Non-ASCII byte {i:#x} should fail"
            );
        }

        // Test boundary cases around batch sizes
        let test_sizes = [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 95, 96, 97, 127, 128,
            129, 255, 256, 257, 511, 512, 513, 1023, 1024, 1025,
        ];

        for &size in &test_sizes {
            // Pure ASCII strings of various sizes
            let ascii_data = vec![b'A'; size];
            assert!(
                ascii_data.is_ascii_fast(),
                "Pure ASCII of size {size} should pass"
            );

            // Mixed data with non-ASCII at start
            if size > 0 {
                let mut mixed_start = vec![b'A'; size];
                mixed_start[0] = 0x80;
                assert!(
                    !mixed_start.is_ascii_fast(),
                    "Mixed data (non-ASCII at start) of size {size} should fail"
                );
            }

            // Mixed data with non-ASCII at end
            if size > 0 {
                let mut mixed_end = vec![b'A'; size];
                mixed_end[size - 1] = 0xFF;
                assert!(
                    !mixed_end.is_ascii_fast(),
                    "Mixed data (non-ASCII at end) of size {size} should fail"
                );
            }

            // Mixed data with non-ASCII in middle
            if size > 2 {
                let mut mixed_middle = vec![b'A'; size];
                mixed_middle[size / 2] = 0x90;
                assert!(
                    !mixed_middle.is_ascii_fast(),
                    "Mixed data (non-ASCII in middle) of size {size} should fail"
                );
            }
        }

        // Test critical AVX2 boundaries
        let critical_sizes = [128, 160, 192, 224, 256]; // 128-byte boundaries + partial chunks
        for &size in &critical_sizes {
            // Pure ASCII
            let ascii_data = vec![b'X'; size];
            assert!(
                ascii_data.is_ascii_fast(),
                "Pure ASCII of critical size {size} should pass"
            );

            // Non-ASCII at various positions within the first 128-byte batch
            for pos in [0, 31, 32, 63, 64, 95, 96, 127] {
                if pos < size {
                    let mut mixed = vec![b'Y'; size];
                    mixed[pos] = 0x81;
                    assert!(
                        !mixed.is_ascii_fast(),
                        "Non-ASCII at position {pos} in size {size} should fail"
                    );
                }
            }

            // Non-ASCII in tail processing (after 128-byte batches)
            if size > 128 {
                for tail_pos in [128, 129, 135, 136, 143, 144] {
                    if tail_pos < size {
                        let mut mixed = vec![b'Z'; size];
                        mixed[tail_pos] = 0x82;
                        assert!(
                            !mixed.is_ascii_fast(),
                            "Non-ASCII at tail position {tail_pos} in size {size} should fail"
                        );
                    }
                }
            }
        }

        // Test 8-byte alignment edge cases in tail processing
        for remainder in 0..16 {
            let size = 128 + remainder; // Just past the 128-byte batch boundary
            let ascii_data = vec![b'M'; size];
            assert!(
                ascii_data.is_ascii_fast(),
                "ASCII data with {remainder} remainder bytes should pass"
            );

            if remainder > 0 {
                let mut mixed = vec![b'N'; size];
                mixed[size - 1] = 0x83;
                assert!(
                    !mixed.is_ascii_fast(),
                    "Mixed data with {remainder} remainder bytes should fail"
                );
            }
        }

        // Test very large strings to ensure no overflow issues
        let large_sizes = [65536, 131072]; // Large powers of 2
        for &size in &large_sizes {
            let large_ascii = vec![b'L'; size];
            assert!(
                large_ascii.is_ascii_fast(),
                "Large ASCII string of size {size} should pass"
            );

            // Test non-ASCII at the very end of large strings
            let mut large_mixed = vec![b'L'; size];
            large_mixed[size - 1] = 0x84;
            assert!(
                !large_mixed.is_ascii_fast(),
                "Large mixed string of size {size} should fail"
            );
        }
    }

    #[test]
    fn test_ascii_check_string_types() {
        let test_data = "Hello, World! 123";
        let test_bytes = test_data.as_bytes();

        // Test all supported types with same data
        assert!(test_data.is_ascii_fast()); // &str
        assert!(test_data.to_string().is_ascii_fast()); // String
        assert!(test_bytes.is_ascii_fast()); // &[u8]
        assert!(test_bytes.to_vec().is_ascii_fast()); // Vec<u8>

        let non_ascii_data = "Hello, 世界! 🌍";
        let non_ascii_bytes = non_ascii_data.as_bytes();

        // Test all supported types with non-ASCII data
        assert!(!non_ascii_data.is_ascii_fast()); // &str
        assert!(!non_ascii_data.to_string().is_ascii_fast()); // String
        assert!(!non_ascii_bytes.is_ascii_fast()); // &[u8]
        assert!(!non_ascii_bytes.to_vec().is_ascii_fast()); // Vec<u8>
    }

    #[test]
    fn test_ascii_check_boundary_values() {
        // Test boundary ASCII values
        let boundary_ascii = vec![0x00, 0x01, 0x7E, 0x7F]; // First and last ASCII values
        assert!(boundary_ascii.is_ascii_fast());

        // Test boundary non-ASCII values
        let boundary_non_ascii = vec![0x80, 0x81, 0xFE, 0xFF]; // First and last non-ASCII values
        assert!(!boundary_non_ascii.is_ascii_fast());

        // Test mixed boundary values
        let mixed_boundary = vec![0x7F, 0x80]; // Last ASCII + first non-ASCII
        assert!(!mixed_boundary.is_ascii_fast());
    }
}
