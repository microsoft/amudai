/// Aligns a number up to the next multiple of the specified alignment.
///
/// This function rounds up the input number to the nearest multiple of the alignment
/// that is greater than or equal to the input. If the input is already aligned,
/// it returns the input unchanged.
///
/// # Arguments
///
/// * `n` - The number to align up
/// * `alignment` - The alignment boundary (must be a power of 2 and non-zero)
///
/// # Returns
///
/// The smallest multiple of `alignment` that is greater than or equal to `n`.
///
/// # Examples
///
/// ```
/// use amudai_bytes::align::align_up_u64;
///
/// assert_eq!(align_up_u64(0, 8), 0);
/// assert_eq!(align_up_u64(1, 8), 8);
/// assert_eq!(align_up_u64(7, 8), 8);
/// assert_eq!(align_up_u64(8, 8), 8);
/// assert_eq!(align_up_u64(9, 8), 16);
/// assert_eq!(align_up_u64(15, 8), 16);
/// ```
///
/// # Panics
///
/// This function will panic in debug builds if:
/// - `alignment` is 0
/// - `alignment` is not a power of 2
#[inline]
pub fn align_up_u64(n: u64, alignment: u64) -> u64 {
    debug_assert_ne!(alignment, 0);
    debug_assert!(alignment.is_power_of_two());
    (n + alignment - 1) & !(alignment - 1)
}

/// Aligns a number down to the previous multiple of the specified alignment.
///
/// This function rounds down the input number to the nearest multiple of the alignment
/// that is less than or equal to the input. If the input is already aligned,
/// it returns the input unchanged.
///
/// # Arguments
///
/// * `n` - The number to align down
/// * `alignment` - The alignment boundary (must be a power of 2 and non-zero)
///
/// # Returns
///
/// The largest multiple of `alignment` that is less than or equal to `n`.
///
/// # Examples
///
/// ```
/// use amudai_bytes::align::align_down_u64;
///
/// assert_eq!(align_down_u64(0, 8), 0);
/// assert_eq!(align_down_u64(1, 8), 0);
/// assert_eq!(align_down_u64(7, 8), 0);
/// assert_eq!(align_down_u64(8, 8), 8);
/// assert_eq!(align_down_u64(9, 8), 8);
/// assert_eq!(align_down_u64(15, 8), 8);
/// assert_eq!(align_down_u64(16, 8), 16);
/// ```
///
/// # Panics
///
/// This function will panic in debug builds if:
/// - `alignment` is 0
/// - `alignment` is not a power of 2
#[inline]
pub fn align_down_u64(n: u64, alignment: u64) -> u64 {
    debug_assert_ne!(alignment, 0);
    debug_assert!(alignment.is_power_of_two());
    n & !(alignment - 1)
}

/// Checks if a number is aligned to the specified alignment boundary.
///
/// This function determines whether the input number is evenly divisible by the
/// alignment value, meaning it lies exactly on an alignment boundary.
///
/// # Arguments
///
/// * `n` - The number to check for alignment
/// * `alignment` - The alignment boundary to check against (must be a power of 2 and non-zero)
///
/// # Returns
///
/// `true` if `n` is a multiple of `alignment`, `false` otherwise.
///
/// # Examples
///
/// ```
/// use amudai_bytes::align::is_aligned_u64;
///
/// assert_eq!(is_aligned_u64(0, 8), true);
/// assert_eq!(is_aligned_u64(1, 8), false);
/// assert_eq!(is_aligned_u64(7, 8), false);
/// assert_eq!(is_aligned_u64(8, 8), true);
/// assert_eq!(is_aligned_u64(16, 8), true);
/// assert_eq!(is_aligned_u64(17, 8), false);
/// ```
///
/// # Panics
///
/// This function will panic in debug builds if:
/// - `alignment` is 0
/// - `alignment` is not a power of 2
#[inline]
pub fn is_aligned_u64(n: u64, alignment: u64) -> bool {
    debug_assert_ne!(alignment, 0);
    debug_assert!(alignment.is_power_of_two());
    (n & (alignment - 1)) == 0
}
