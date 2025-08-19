// ! This FastLanes implementation is based on https://github.com/spiraldb/fastlanes repository.
// ! It's modified to allow using with release Rust builds, without hurting the ability to generate
// ! vectorized code.

use arrayref::{array_mut_ref, array_ref};
use const_for::const_for;
use num_traits::PrimInt;
use paste::paste;

pub const BLOCK_SIZE: usize = 1024;

const FL_ORDER: [usize; 8] = [0, 4, 2, 6, 1, 5, 3, 7];

#[macro_export]
macro_rules! iterate {
    ($T:ty, $lane: expr, | $_1:tt $idx:ident | $($body:tt)*) => {
        macro_rules! __kernel__ {( $_1 $idx:ident ) => ( $($body)* )}
        {
            use $crate::seq_t;
            use paste::paste;

            #[inline(always)]
            fn index(row: usize, lane: usize) -> usize {
                let o = row / 8;
                let s = row % 8;
                (FL_ORDER[o] * 16) + (s * 128) + lane
            }

            paste!(seq_t!(row in $T {
                let idx = index(row, $lane);
                __kernel__!(idx);
            }));
        }
    }
}

#[macro_export]
macro_rules! pack {
    ($T:ty, $W:expr, $packed:expr, $lane:expr, | $_1:tt $idx:ident | $($body:tt)*) => {
        macro_rules! __kernel__ {( $_1 $idx:ident ) => ( $($body)* )}
        {
            use $crate::seq_t;
            use paste::paste;

            // The number of bits of T.
            const T: usize = <$T>::T;

            #[inline(always)]
            fn index(row: usize, lane: usize) -> usize {
                let o = row / 8;
                let s = row % 8;
                (FL_ORDER[o] * 16) + (s * 128) + lane
            }

            if $W == 0 {
                // Nothing to do if W is 0, since the packed array is zero bytes.
            } else if $W == T {
                // Special case for W=T, we can just copy the input value directly to the packed value.
                paste!(seq_t!(row in $T {
                    let idx = index(row, $lane);
                    $packed[<$T>::LANES * row + $lane] = __kernel__!(idx);
                }));
            } else {
                // A mask of W bits.
                let mask: $T = (1 << $W) - 1;

                // First we loop over each lane in the virtual `BLOCK_SIZE` bit word.
                let mut tmp: $T = 0;

                // Loop over each of the rows of the lane.
                // Inlining this loop means all branches are known at compile time and
                // the code is auto-vectorized for SIMD execution.
                paste!(seq_t!(row in $T {
                    let idx = index(row, $lane);
                    let src = __kernel__!(idx);
                    let src = src & mask;

                    // Shift the src bits into their position in the tmp output variable.
                    if row == 0 {
                        tmp = src;
                    } else {
                        tmp |= src << (row * $W) % T;
                    }

                    // If the next packed position is after our current one, then we have filled
                    // the current output and we can write the packed value.
                    let curr_word: usize = (row * $W) / T;
                    let next_word: usize = ((row + 1) * $W) / T;

                    #[allow(unused_assignments)]
                    if next_word > curr_word {
                        $packed[<$T>::LANES * curr_word + $lane] = tmp;
                        let remaining_bits: usize = ((row + 1) * $W) % T;
                        // Keep the remaining bits for the next packed value.
                        tmp = src >> $W - remaining_bits;
                    }
                }));
            }
        }
    };
}

#[macro_export]
macro_rules! unpack {
    ($T:ty, $W:expr, $packed:expr, $lane:expr, | $_1:tt $idx:ident, $_2:tt $elem:ident | $($body:tt)*) => {
        macro_rules! __kernel__ {( $_1 $idx:ident, $_2 $elem:ident ) => ( $($body)* )}
        {
            use $crate::seq_t;
            use paste::paste;

            // The number of bits of T.
            const T: usize = <$T>::T;

            #[inline(always)]
            fn index(row: usize, lane: usize) -> usize {
                let o = row / 8;
                let s = row % 8;
                (FL_ORDER[o] * 16) + (s * 128) + lane
            }

            if $W == 0 {
                // Special case for W=0, we just need to zero the output.
                // We'll still respect the iteration order in case the kernel has side effects.
                paste!(seq_t!(row in $T {
                    let idx = index(row, $lane);
                    let zero: $T = 0;
                    __kernel__!(idx, zero);
                }));
            } else if $W == T {
                // Special case for W=T, we can just copy the packed value directly to the output.
                paste!(seq_t!(row in $T {
                    let idx = index(row, $lane);
                    let src = $packed[<$T>::LANES * row + $lane];
                    __kernel__!(idx, src);
                }));
            } else {
                #[inline]
                fn mask(width: usize) -> $T {
                    if width == T { <$T>::MAX } else { (1 << (width % T)) - 1 }
                }

                let mut src: $T = $packed[$lane];
                let mut tmp: $T;

                paste!(seq_t!(row in $T {
                    // Figure out the packed positions
                    let curr_word: usize = (row * $W) / T;
                    let next_word = ((row + 1) * $W) / T;

                    let shift = (row * $W) % T;

                    if next_word > curr_word {
                        // Consume some bits from the curr packed input, the remainder are in the next
                        // packed input value
                        let remaining_bits = ((row + 1) * $W) % T;
                        let current_bits = $W - remaining_bits;
                        tmp = (src >> shift) & mask(current_bits);

                        if next_word < $W {
                            // Load the next packed value
                            src = $packed[<$T>::LANES * next_word + $lane];
                            // Consume the remaining bits from the next input value.
                            tmp |= (src & mask(remaining_bits)) << current_bits;
                        }
                    } else {
                        // Otherwise, just grab W bits from the src value
                        tmp = (src >> shift) & mask($W);
                    }

                    // Write out the unpacked value
                    let idx = index(row, $lane);
                    __kernel__!(idx, tmp);
                }));
            }
        }
    };
}

pub trait FastLanes: Sized + PrimInt {
    const T: usize = size_of::<Self>() * 8;
    const LANES: usize = BLOCK_SIZE / Self::T;
}

impl FastLanes for i8 {}
impl FastLanes for i16 {}
impl FastLanes for i32 {}
impl FastLanes for i64 {}
impl FastLanes for i128 {}
impl FastLanes for u8 {}
impl FastLanes for u16 {}
impl FastLanes for u32 {}
impl FastLanes for u64 {}
impl FastLanes for u128 {}

// Macro for repeating a code block bit_size_of::<T> times.
#[macro_export]
macro_rules! seq_t {
    ($ident:ident in u8 $body:tt) => {seq_macro::seq!($ident in 0..8 $body)};
    ($ident:ident in u16 $body:tt) => {seq_macro::seq!($ident in 0..16 $body)};
    ($ident:ident in u32 $body:tt) => {seq_macro::seq!($ident in 0..32 $body)};
    ($ident:ident in u64 $body:tt) => {seq_macro::seq!($ident in 0..64 $body)};
}

pub struct BitPackWidth<const W: usize>;

pub trait SupportedBitPackWidth<T> {}

impl<const W: usize, T> SupportedBitPackWidth<T> for BitPackWidth<W> {}

/// `BitPack` into a compile-time known bit-width.
#[allow(dead_code)]
pub trait BitPacking: FastLanes {
    /// Packs `BLOCK_SIZE` elements into W bits each.
    /// The output buffer must have the capacity for storing `BLOCK_SIZE * W / T` elements.
    /// The method returns the number of elements written to the output buffer,
    /// which is always `BLOCK_SIZE * W / T`.
    fn pack_gen<const W: usize>(input: &[Self; BLOCK_SIZE], output: &mut [Self]) -> usize
    where
        BitPackWidth<W>: SupportedBitPackWidth<Self>;

    /// Packs `BLOCK_SIZE` elements into `W` bits each, where `W` is runtime-known instead of
    /// compile-time known.
    ///
    /// The method returns the number of elements written to the output buffer,
    /// which is always `BLOCK_SIZE * W / T`.
    ///
    /// # Safety
    /// The input slice must be of exactly length `BLOCK_SIZE`. The output slice must be of length
    /// `BLOCK_SIZE * W / T`, where `T` is the bit-width of Self and `W` is the packed width.
    /// These lengths are checked only with `debug_assert` (i.e., not checked on release builds).
    fn pack(width: usize, input: &[Self], output: &mut [Self]) -> usize;

    /// Unpacks `BLOCK_SIZE` elements from `W` bits each.
    fn unpack_gen<const W: usize>(input: &[Self], output: &mut [Self; BLOCK_SIZE])
    where
        BitPackWidth<W>: SupportedBitPackWidth<Self>;

    /// Unpacks `BLOCK_SIZE` elements from `W` bits each, where `W` is runtime-known instead of
    /// compile-time known.
    ///
    /// # Safety
    /// The input slice must be of length `BLOCK_SIZE * W / T`, where `T` is the bit-width of Self and `W`
    /// is the packed width. The output slice must be of exactly length `BLOCK_SIZE`.
    /// These lengths are checked only with `debug_assert` (i.e., not checked on release builds).
    fn unpack(width: usize, input: &[Self], output: &mut [Self]);

    /// Unpacks a single element at the provided index from a packed array of `BLOCK_SIZE` `W` bit elements.
    fn unpack_single_gen<const W: usize>(packed: &[Self], index: usize) -> Self
    where
        BitPackWidth<W>: SupportedBitPackWidth<Self>;

    /// Unpacks a single element at the provided index from a packed array of `BLOCK_SIZE` `W` bit elements,
    /// where `W` is runtime-known instead of compile-time known.
    ///
    /// # Safety
    /// The input slice must be of length `BLOCK_SIZE * W / T`, where `T` is the bit-width of Self and `W`
    /// is the packed width. The output slice must be of exactly length `BLOCK_SIZE`.
    /// These lengths are checked only with `debug_assert` (i.e., not checked on release builds).
    fn unpack_single(width: usize, input: &[Self], index: usize) -> Self;
}

macro_rules! impl_packing {
    ($T:ty) => {
        paste! {
            impl BitPacking for $T {
                fn pack_gen<const W: usize>(
                    input: &[Self; BLOCK_SIZE],
                    output: &mut [Self],
                ) -> usize where BitPackWidth<W>: SupportedBitPackWidth<Self> {
                    for lane in 0..Self::LANES {
                        pack!($T, W, output, lane, |$idx| {
                            input[$idx]
                        });
                    }
                    BLOCK_SIZE * W / Self::T
                }

                fn pack(width: usize, input: &[Self], output: &mut [Self]) -> usize {
                    let packed_len = 128 * width / size_of::<Self>();
                    debug_assert!(output.len() >= packed_len, "Output buffer must be at least of size BLOCK_SIZE * W / T");
                    debug_assert_eq!(input.len(), BLOCK_SIZE, "Input buffer must be of size BLOCK_SIZE");
                    debug_assert!(width <= Self::T, "Width must be less than or equal to {}", Self::T);

                    seq_t!(W in $T {
                        match width {
                            #(W => Self::pack_gen::<W>(
                                array_ref![input, 0, BLOCK_SIZE],
                                array_mut_ref![output, 0, BLOCK_SIZE * W / <$T>::T],
                            ),)*
                            // seq_t has exclusive upper bound
                            Self::T => Self::pack_gen::<{ Self::T }>(
                                array_ref![input, 0, BLOCK_SIZE],
                                array_mut_ref![output, 0, BLOCK_SIZE],
                            ),
                            _ => unreachable!("Unsupported width: {}", width)
                        }
                    })
                }

                fn unpack_gen<const W: usize>(
                    input: &[Self],
                    output: &mut [Self; BLOCK_SIZE],
                ) where BitPackWidth<W>: SupportedBitPackWidth<Self> {
                    for lane in 0..Self::LANES {
                        unpack!($T, W, input, lane, |$idx, $elem| {
                            output[$idx] = $elem
                        });
                    }
                }

                fn unpack(width: usize, input: &[Self], output: &mut [Self]) {
                    let packed_len = 128 * width / size_of::<Self>();
                    debug_assert!(input.len() >= packed_len, "Input buffer must be at least of size BLOCK_SIZE * W / T");
                    debug_assert!(output.len() >= BLOCK_SIZE, "Output buffer must be at least of size BLOCK_SIZE");
                    debug_assert!(width <= Self::T, "Width must be less than or equal to {}", Self::T);

                    seq_t!(W in $T {
                        match width {
                            #(W => Self::unpack_gen::<W>(
                                array_ref![input, 0, BLOCK_SIZE * W / <$T>::T],
                                array_mut_ref![output, 0, BLOCK_SIZE],
                            ),)*
                            // seq_t has exclusive upper bound
                            Self::T => Self::unpack_gen::<{ Self::T }>(
                                array_ref![input, 0, BLOCK_SIZE],
                                array_mut_ref![output, 0, BLOCK_SIZE],
                            ),
                            _ => unreachable!("Unsupported width: {}", width)
                        }
                    })
                }

                /// Unpacks a single element at the provided index from a packed array of BLOCK_SIZE `W` bit elements.
                fn unpack_single_gen<const W: usize>(packed: &[Self], index: usize) -> Self
                where
                    BitPackWidth<W>: SupportedBitPackWidth<Self>,
                {
                    if W == 0 {
                        // Special case for W=0, we just need to zero the output.
                        return 0 as $T;
                    }

                    // We can think of the input array as effectively a row-major, left-to-right
                    // 2-D array of with `Self::LANES` columns and `Self::T` rows.
                    //
                    // Meanwhile, we can think of the packed array as either:
                    //      1. `Self::T` rows of W-bit elements, with `Self::LANES` columns
                    //      2. `W` rows of `Self::T`-bit words, with `Self::LANES` columns
                    //
                    // Bitpacking involves a transposition of the input array ordering, such that
                    // decompression can be fused efficiently with encodings like delta and RLE.
                    //
                    // First step, we need to get the lane and row for interpretation #1 above.
                    assert!(index < BLOCK_SIZE, "Index must be less than BLOCK_SIZE, got {}", index);
                    let (lane, row): (usize, usize) = {
                        const LANES: [u8; BLOCK_SIZE] = lanes_by_index::<$T>();
                        const ROWS: [u8; BLOCK_SIZE] = rows_by_index::<$T>();
                        (LANES[index] as usize, ROWS[index] as usize)
                    };

                    if W == <$T>::T {
                        // Special case for W==T, we can just read the value directly
                        return packed[<$T>::LANES * row + lane] as Self;
                    }

                    let mask: $T = (1 << (W % <$T>::T)) - 1;
                    let start_bit = row * W;
                    let start_word = start_bit / <$T>::T;
                    let lo_shift = start_bit % <$T>::T;
                    let remaining_bits = <$T>::T - lo_shift;

                    let lo = packed[<$T>::LANES * start_word + lane] >> lo_shift;
                    return if remaining_bits >= W {
                        // in this case we will mask out all bits of hi word
                        (lo & mask) as Self
                    } else {
                        // guaranteed that lo_shift > 0 and thus remaining_bits < T
                        let hi = packed[<$T>::LANES * (start_word + 1) + lane] << remaining_bits;
                        ((lo | hi) & mask) as Self
                    };
                }

                fn unpack_single(width: usize, packed: &[Self], index: usize) -> Self {
                    const T: usize = <$T>::T;

                    let packed_len = 128 * width / size_of::<Self>();
                    debug_assert!(packed.len() >= packed_len, "Input buffer must be at least of size {}", packed_len);
                    debug_assert!(width <= Self::T, "Width must be less than or equal to {}", Self::T);

                    seq_t!(W in $T {
                        match width {
                            #(W => {
                                return <$T>::unpack_single_gen::<W>(array_ref![packed, 0, BLOCK_SIZE * W / T], index);
                            },)*
                            // seq_t has exclusive upper bound
                            T => {
                                return <$T>::unpack_single_gen::<T>(array_ref![packed, 0, BLOCK_SIZE], index);
                            },
                            _ => unreachable!("Unsupported width: {}", width)
                        }
                    })
                }
            }
        }
    };
}

macro_rules! impl_packing_unimpl {
    ($T:ty) => {
        paste! {
            impl BitPacking for $T {
                fn pack_gen<const W: usize>(
                    _: &[Self; BLOCK_SIZE],
                    _: &mut [Self],
                ) -> usize where BitPackWidth<W>: SupportedBitPackWidth<Self> {
                    unimplemented!()
                }

                fn pack(_: usize, _: &[Self], _: &mut [Self]) -> usize {
                    unimplemented!()
                }

                fn unpack_gen<const W: usize>(
                    _: &[Self],
                    _: &mut [Self; BLOCK_SIZE],
                ) where BitPackWidth<W>: SupportedBitPackWidth<Self> {
                    unimplemented!()
                }

                fn unpack(_: usize, _: &[Self], _: &mut [Self]) {
                    unimplemented!()
                }

                fn unpack_single_gen<const W: usize>(_: &[Self], _: usize) -> Self
                where
                    BitPackWidth<W>: SupportedBitPackWidth<Self>,
                {
                    unimplemented!()
                }

                fn unpack_single(_: usize, _: &[Self], _: usize) -> Self {
                    unimplemented!()
                }
            }
        }
    };
}

// helper function executed at compile-time to speed up unpack_single at runtime
const fn lanes_by_index<T: FastLanes>() -> [u8; BLOCK_SIZE] {
    let mut lanes = [0u8; BLOCK_SIZE];
    const_for!(i in 0..BLOCK_SIZE => {
        lanes[i] = (i % T::LANES) as u8;
    });
    lanes
}

// helper function executed at compile-time to speed up unpack_single at runtime
const fn rows_by_index<T: FastLanes>() -> [u8; BLOCK_SIZE] {
    let mut rows = [0u8; BLOCK_SIZE];
    const_for!(i in 0..BLOCK_SIZE => {
        // This is the inverse of the `index` function from the pack/unpack macros:
        //     fn index(row: usize, lane: usize) -> usize {
        //         let o = row / 8;
        //         let s = row % 8;
        //         (FL_ORDER[o] * 16) + (s * 128) + lane
        //     }
        let lane = i % T::LANES;
        let s = i / 128; // because `(FL_ORDER[o] * 16) + lane` is always < 128
        let fl_order = (i - s * 128 - lane) / 16; // value of FL_ORDER[o]
        let o = FL_ORDER[fl_order]; // because this transposition is invertible!
        rows[i] = (o * 8 + s) as u8;
    });
    rows
}

impl_packing!(u8);
impl_packing!(u16);
impl_packing!(u32);
impl_packing!(u64);
impl_packing_unimpl!(u128);

/// Frame-of-Reference encoding that is fused with BitPacking into a single SIMD sequence of
/// operations.
pub trait FusedFrameOfReference: BitPacking {
    fn ffor_pack_gen<const W: usize>(input: &[Self], reference: Self, output: &mut [Self]) -> usize
    where
        BitPackWidth<W>: SupportedBitPackWidth<Self>;

    fn ffor_pack(width: usize, input: &[Self], reference: Self, output: &mut [Self]) -> usize;

    fn ffor_unpack_gen<const W: usize>(input: &[Self], reference: Self, output: &mut [Self])
    where
        BitPackWidth<W>: SupportedBitPackWidth<Self>;

    fn ffor_unpack(width: usize, input: &[Self], reference: Self, output: &mut [Self]);
}

macro_rules! impl_ffor {
    ($T:ty) => {
        paste! {
            impl FusedFrameOfReference for $T {
                fn ffor_pack_gen<const W: usize>(
                    input: &[Self],
                    reference: Self,
                    output: &mut [Self],
                ) -> usize
                where
                    BitPackWidth<W>: SupportedBitPackWidth<Self>,
                {
                    for lane in 0..Self::LANES {
                        pack!($T, W, output, lane, |$idx| {
                            input[$idx].wrapping_sub(reference)
                        });
                    }
                    BLOCK_SIZE * W / Self::T
                }

                fn ffor_pack(width: usize, input: &[Self], reference: Self, output: &mut [Self]) -> usize {
                    let packed_len = 128 * width / size_of::<Self>();
                    debug_assert!(output.len() >= packed_len, "Output buffer must be at least of size BLOCK_SIZE * W / T");
                    debug_assert_eq!(input.len(), BLOCK_SIZE, "Input buffer must be of size BLOCK_SIZE");
                    debug_assert!(width <= Self::T, "Width must be less than or equal to {}", Self::T);

                    seq_t!(W in $T {
                        match width {
                            #(W => Self::ffor_pack_gen::<W>(
                                array_ref![input, 0, BLOCK_SIZE],
                                reference,
                                array_mut_ref![output, 0, BLOCK_SIZE * W / <$T>::T],
                            ),)*
                            // seq_t has exclusive upper bound
                            Self::T => Self::ffor_pack_gen::<{ Self::T }>(
                                array_ref![input, 0, BLOCK_SIZE],
                                reference,
                                array_mut_ref![output, 0, BLOCK_SIZE],
                            ),
                            _ => unreachable!("Unsupported width: {}", width)
                        }
                    })
                }

                fn ffor_unpack_gen<const W: usize>(
                    input: &[Self],
                    reference: Self,
                    output: &mut [Self],
                ) where
                    BitPackWidth<W>: SupportedBitPackWidth<Self>,
                {
                    for lane in 0..Self::LANES {
                        unpack!($T, W, input, lane, |$idx, $elem| {
                            output[$idx] = $elem.wrapping_add(reference)
                        });
                    }
                }

                fn ffor_unpack(width: usize, input: &[Self], reference: Self, output: &mut [Self]) {
                    let packed_len = 128 * width / size_of::<Self>();
                    debug_assert!(input.len() >= packed_len, "Input buffer must be at least of size BLOCK_SIZE * W / T");
                    debug_assert!(output.len() >= BLOCK_SIZE, "Output buffer must be at least of size BLOCK_SIZE");
                    debug_assert!(width <= Self::T, "Width must be less than or equal to {}", Self::T);

                    seq_t!(W in $T {
                        match width {
                            #(W => Self::ffor_unpack_gen::<W>(
                                array_ref![input, 0, BLOCK_SIZE * W / <$T>::T],
                                reference,
                                array_mut_ref![output, 0, BLOCK_SIZE],
                            ),)*
                            // seq_t has exclusive upper bound
                            Self::T => Self::ffor_unpack_gen::<{ Self::T }>(
                                array_ref![input, 0, BLOCK_SIZE],
                                reference,
                                array_mut_ref![output, 0, BLOCK_SIZE],
                            ),
                            _ => unreachable!("Unsupported width: {}", width)
                        }
                    })
                }
            }
        }
    };
}

macro_rules! impl_ffor_unimpl {
    ($T:ty) => {
        paste! {
            impl FusedFrameOfReference for $T {
                fn ffor_pack_gen<const W: usize>(_: &[Self], _: Self, _: &mut [Self]) -> usize
                where
                    BitPackWidth<W>: SupportedBitPackWidth<Self>,
                {
                    unimplemented!()
                }

                fn ffor_pack(_: usize, _: &[Self], _: Self, _: &mut [Self]) -> usize {
                    unimplemented!()
                }

                fn ffor_unpack_gen<const W: usize>(
                    _: &[Self],
                    _: Self,
                    _: &mut [Self],
                ) where
                    BitPackWidth<W>: SupportedBitPackWidth<Self>,
                {
                    unimplemented!()
                }

                fn ffor_unpack(_: usize, _: &[Self], _: Self, _: &mut [Self]) {
                    unimplemented!()
                }
            }
        }
    };
}

impl_ffor!(u8);
impl_ffor!(u16);
impl_ffor!(u32);
impl_ffor!(u64);
impl_ffor_unimpl!(u128);

#[cfg(test)]
#[allow(clippy::needless_range_loop)]
mod test {
    use core::array;
    use core::fmt::Debug;
    use seq_macro::seq;

    use super::*;

    #[test]
    fn test_pack() {
        let input = array::from_fn(|i| i as u32);
        let mut packed = [0; BLOCK_SIZE];
        let packed_size = BitPacking::pack(10, &input, &mut packed);
        assert_eq!(packed_size, 320);
        let mut output = [0; BLOCK_SIZE];
        BitPacking::unpack(10, &packed, &mut output);
        assert_eq!(input, output);
    }

    #[test]
    fn test_unpack_single() {
        let values = array::from_fn(|i| i as u32);
        let mut packed = [0; 512];
        BitPacking::pack_gen::<16>(&values, &mut packed);

        for i in 0..BLOCK_SIZE {
            assert_eq!(BitPacking::unpack_single_gen::<16>(&packed, i), values[i]);
            assert_eq!(BitPacking::unpack_single(16, &packed, i), values[i]);
        }
    }

    fn try_round_trip<T: BitPacking + Debug, const W: usize>()
    where
        BitPackWidth<W>: SupportedBitPackWidth<T>,
    {
        let mut values: [T; BLOCK_SIZE] = [T::zero(); BLOCK_SIZE];
        for i in 0..BLOCK_SIZE {
            values[i] = T::from(i % (1 << (W % T::T))).unwrap();
        }

        let mut packed = [T::zero(); BLOCK_SIZE];
        let packed_size = BitPacking::pack_gen::<W>(&values, &mut packed);
        assert_eq!(packed_size, BLOCK_SIZE * W / T::T);

        let mut unpacked = [T::zero(); BLOCK_SIZE];
        BitPacking::unpack_gen::<W>(&packed, &mut unpacked);

        assert_eq!(&unpacked, &values);

        for i in 0..BLOCK_SIZE {
            assert_eq!(BitPacking::unpack_single_gen::<W>(&packed, i), values[i]);
            assert_eq!(BitPacking::unpack_single(W, &packed, i), values[i]);
        }
    }

    macro_rules! impl_try_round_trip {
        ($T:ty, $W:expr) => {
            paste! {
                #[test]
                fn [<test_round_trip_ $T _ $W>]() {
                    try_round_trip::<$T, $W>();
                }
            }
        };
    }

    seq!(W in 0..=8 { impl_try_round_trip!(u8, W); });
    seq!(W in 0..=16 { impl_try_round_trip!(u16, W); });
    seq!(W in 0..=32 { impl_try_round_trip!(u32, W); });
    seq!(W in 0..=64 { impl_try_round_trip!(u64, W); });

    #[test]
    fn test_ffor() {
        let values = (1000001u32..1001025).collect::<Vec<_>>();
        let min = *values.iter().min().unwrap();
        let max = *values.iter().max().unwrap();
        let width = std::mem::size_of::<u32>() * 8 - (max - min).leading_zeros() as usize;

        let mut packed = vec![0; values.len()];
        let packed_size = FusedFrameOfReference::ffor_pack(width, &values, min, &mut packed);
        packed.truncate(packed_size);

        let mut unpacked = vec![0; values.len()];
        FusedFrameOfReference::ffor_unpack(width, &packed, min, &mut unpacked);

        for (&a, &b) in values.iter().zip(unpacked.iter()) {
            assert_eq!(a, b);
        }
    }
}
