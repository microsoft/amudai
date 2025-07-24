use super::Class;
use super::Rounding;
use super::Status;

use crate::context::*;
use libc::c_char;
use num_traits::Zero;
#[cfg(feature = "ord_subset")]
#[cfg(feature = "serde")]
use std::borrow::Borrow;
use std::cell::RefCell;
use std::default::Default;
use std::ffi::{CStr, CString};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::{Product, Sum};
use std::mem::MaybeUninit;
use std::num::FpCategory;
use std::ops::{
    Add, AddAssign, BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor, BitXorAssign, Div, DivAssign,
    Mul, MulAssign, Neg, Not, Rem, RemAssign, Shl, ShlAssign, Shr, ShrAssign, Sub, SubAssign,
};
use std::str::FromStr;
use std::str::from_utf8_unchecked;

thread_local!(static CTX: RefCell<Context> = RefCell::new(d128::default_context()));

#[repr(C)]
#[derive(Clone, Copy)]
#[allow(non_camel_case_types)]
/// A 128-bit decimal floating point type.
pub struct d128 {
    bytes: [u8; 16],
}

/// Error type for string parsing failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError;

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid string format")
    }
}

impl std::error::Error for ParseError {}

#[repr(C)]
#[derive(Clone, Copy)]
#[allow(non_camel_case_types)]
struct decNumber {
    digits: i32,
    exponent: i32,
    bits: u8,
    // DECPUN = 3 because this is the fastest for conversion between decNumber and decQuad
    // DECNUMDIGITS = 34 because we use decQuad only
    // 12 = ((DECNUMDIGITS+DECDPUN-1)/DECDPUN)
    lsu: [u16; 12],
}

impl Default for d128 {
    fn default() -> Self {
        d128::zero()
    }
}

#[cfg(feature = "ord_subset")]
impl ord_subset::OrdSubset for d128 {
    fn is_outside_order(&self) -> bool {
        self.is_nan()
    }
}

#[cfg(feature = "ord_subset")]
impl From<d128> for ord_subset::OrdVar<d128> {
    fn from(val: d128) -> Self {
        ord_subset::OrdVar::new(val)
    }
}

impl Hash for d128 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let reduced_self = self.reduce();
        reduced_self.bytes.hash(state);
    }
}

#[cfg(feature = "serde")]
impl serde::ser::Serialize for d128 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::de::Deserialize<'de> for d128 {
    fn deserialize<D>(deserializer: D) -> Result<d128, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_str(d128Visitor)
    }
}

#[cfg(feature = "serde")]
#[allow(non_camel_case_types)]
struct d128Visitor;

#[cfg(feature = "serde")]
impl<'de> serde::de::Visitor<'de> for d128Visitor {
    type Value = d128;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a d128 value")
    }

    fn visit_str<E>(self, s: &str) -> Result<d128, E>
    where
        E: serde::de::Error,
    {
        use serde::de::Unexpected;
        d128::from_str(s).map_err(|_| E::invalid_value(Unexpected::Str(s), &self))
    }
}

/// Converts an i32 to d128. The result is exact and no error is possible.
impl From<i32> for d128 {
    fn from(val: i32) -> d128 {
        unsafe {
            let mut res: MaybeUninit<d128> = MaybeUninit::uninit();
            *decQuadFromInt32(res.as_mut_ptr(), val)
        }
    }
}

/// Converts an u32 to d128. The result is exact and no error is possible.
impl From<u32> for d128 {
    fn from(val: u32) -> d128 {
        unsafe {
            let mut res: MaybeUninit<d128> = MaybeUninit::uninit();
            *decQuadFromUInt32(res.as_mut_ptr(), val)
        }
    }
}

/// Converts an u64 to d128. The result is exact and no error is possible.
impl From<u64> for d128 {
    fn from(mut val: u64) -> d128 {
        let mut bcd = [0u8; 34];
        let mut i = 33;
        while val > 0 {
            bcd[i] = (val % 10) as u8;
            val /= 10;
            i -= 1;
        }
        unsafe {
            let mut res: MaybeUninit<d128> = MaybeUninit::uninit();
            *decQuadFromBCD(res.as_mut_ptr(), 0, bcd.as_ptr(), 0)
        }
    }
}

/// Converts an i64 to d128. The result is exact and no error is possible.
impl From<i64> for d128 {
    fn from(val: i64) -> d128 {
        if val < 0 {
            -d128::from(!(val as u64) + 1)
        } else {
            d128::from(val as u64)
        }
    }
}

impl From<f32> for d128 {
    fn from(val: f32) -> Self {
        if val.is_finite() {
            let mut buf = dtoa::Buffer::new();
            let s = buf.format_finite(val);
            return d128::from_str(s).expect("str to d128");
        }
        if val.is_nan() {
            d128::nan()
        } else if val.is_sign_negative() {
            d128::neg_infinity()
        } else {
            d128::infinity()
        }
    }
}

impl From<f64> for d128 {
    fn from(val: f64) -> Self {
        if val.is_finite() {
            let mut buf = dtoa::Buffer::new();
            let s = buf.format_finite(val);
            return d128::from_str(s).expect("str to d128");
        }
        if val.is_nan() {
            d128::nan()
        } else if val.is_sign_negative() {
            d128::neg_infinity()
        } else {
            d128::infinity()
        }
    }
}

impl AsRef<d128> for d128 {
    fn as_ref(&self) -> &d128 {
        self
    }
}

/// Converts a string to d128.
///
/// The input string is trimmed of leading and trailing whitespace before parsing. Empty strings
/// (including whitespace-only strings) return a `ParseError`. The string "qNaN" is normalized to
/// "NaN" since all NaN values are quiet by default in this implementation.
///
/// The length of the coefficient and the size of the exponent are checked by this routine, so
/// rounding will be applied if necessary, and this may set status flags (`UNDERFLOW`, `OVERFLOW`).
/// There is no limit to the coefficient length for finite inputs; NaN payloads must be integers
/// with no more than 33 digits. Exponents may have up to nine significant digits.
///
/// The syntax of the string is fully checked; if it is not valid, a `ParseError` is returned.
/// This includes strings containing null bytes, invalid decimal syntax, or conversion errors.
impl FromStr for d128 {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, ParseError> {
        // Trim whitespace before parsing
        let trimmed = s.trim();

        // Check for empty string after trimming
        if trimmed.is_empty() {
            return Err(ParseError);
        }

        // Normalize qNaN variations to NaN (since all NaN are quiet by default)
        let normalized = if trimmed.eq_ignore_ascii_case("qnan") {
            "NaN"
        } else {
            trimmed
        };

        let cstr = match CString::new(normalized) {
            Err(..) => return Err(ParseError), // Contains null bytes
            Ok(cstr) => cstr,
        };

        d128::with_context(|ctx| {
            // Clear status before parsing
            ctx.status = 0;

            let out: d128 = unsafe {
                let mut res: MaybeUninit<d128> = MaybeUninit::uninit();
                decQuadFromString(res.as_mut_ptr(), cstr.as_ptr(), ctx);
                res.assume_init()
            };

            // Check if parsing resulted in an error
            let status = Status::from_bits_truncate(ctx.status);
            if status.contains(Status::CONVERSION_SYNTAX)
                || (out.is_nan()
                    && !normalized.eq_ignore_ascii_case("nan")
                    && !normalized.eq_ignore_ascii_case("snan"))
            {
                Err(ParseError)
            } else {
                Ok(out)
            }
        })
    }
}

/// Converts this d128 to an i32. It uses Rounding::Down.
impl From<d128> for i32 {
    fn from(val: d128) -> Self {
        d128::with_context(|ctx| unsafe { decQuadToInt32(&val, ctx, Rounding::Down) })
    }
}

/// Converts this d128 to an u32. It uses Rounding::Down.
impl From<d128> for u32 {
    fn from(val: d128) -> Self {
        d128::with_context(|ctx| unsafe { decQuadToUInt32(&val, ctx, Rounding::Down) })
    }
}

/// Converts this d128 to an i64.
impl From<d128> for i64 {
    fn from(val: d128) -> Self {
        if val > d128::from(i64::MAX) || val < d128::from(i64::MIN) {
            return 0;
        }

        if val > d128::from(0i32) {
            if val <= d128::from(i32::MAX) {
                i32::from(val) as i64
            } else {
                let modulo = d128::from(u32::MAX) + d128::from(1i32);
                let ms = u32::from(val / modulo) as i64;
                let ls = u32::from(val.rem(&modulo)) as i64;

                (ms << 32) | ls
            }
        } else if val >= d128::from(i32::MIN) {
            i32::from(val) as i64
        } else {
            let modulo = d128::from(u32::MAX) + d128::from(1i32);
            let ms = i32::from(val / modulo) as i64;
            let ls = i32::from(val.rem(&modulo)) as i64;

            (ms << 32) + ls
        }
    }
}

impl From<d128> for f64 {
    fn from(val: d128) -> Self {
        if val.is_nan() {
            f64::NAN
        } else if val.is_infinite() {
            if val.is_positive() {
                f64::INFINITY
            } else {
                f64::NEG_INFINITY
            }
        } else {
            let mut buf = [0u8; 43];
            unsafe {
                let len = val.write_to_buffer(&mut buf);
                from_utf8_unchecked(&buf[..len])
                    .parse::<f64>()
                    .expect("parse f64")
            }
        }
    }
}

impl From<&d128> for f64 {
    fn from(val: &d128) -> Self {
        (*val).into()
    }
}

impl From<d128> for f32 {
    fn from(val: d128) -> Self {
        if val.is_nan() {
            f32::NAN
        } else if val.is_infinite() {
            if val.is_positive() {
                f32::INFINITY
            } else {
                f32::NEG_INFINITY
            }
        } else {
            let mut buf = [0u8; 43];
            unsafe {
                let len = val.write_to_buffer(&mut buf);
                from_utf8_unchecked(&buf[..len])
                    .parse::<f32>()
                    .expect("parse f32")
            }
        }
    }
}

impl From<&d128> for f32 {
    fn from(val: &d128) -> Self {
        (*val).into()
    }
}

/// Formats a d128. Finite numbers will be converted to a string with exponential notation if the
/// exponent is positive or if the magnitude of x is less than 1 and would require more than five
/// zeros between the decimal point and the first significant digit. Note that strings which are
/// not simply numbers (one of Infinity, –Infinity, NaN, or sNaN) are possible. A NaN string may
/// have a leading – sign and/or following payload digits. No digits follow the NaN string if the
/// payload is 0.
impl fmt::Display for d128 {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let mut buf = [0u8; 43];
        unsafe {
            let len = self.write_to_buffer(&mut buf);
            fmt.write_str(from_utf8_unchecked(&buf[..len]))
        }
    }
}

/// Same as `fmt::Display`.
impl fmt::Debug for d128 {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, fmt)
    }
}

/// Formats a d128 with engineering notation. This is the same as fmt::Display except that if
/// exponential notation is used the exponent will be a multiple of 3.
impl fmt::LowerExp for d128 {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let mut buf = [0; 43];
        unsafe {
            decQuadToEngString(self, buf.as_mut().as_mut_ptr());
            let cstr = CStr::from_ptr(buf.as_ptr());
            fmt.pad(from_utf8_unchecked(cstr.to_bytes()))
        }
    }
}

/// Formats a d128 to hexadecimal binary representation.
impl fmt::LowerHex for d128 {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        for b in self.bytes.iter().rev() {
            write!(fmt, "{b:02x}")?;
        }
        Ok(())
    }
}

impl PartialEq<d128> for d128 {
    fn eq(&self, other: &d128) -> bool {
        self.compare(other).is_zero()
    }
}

impl PartialOrd<d128> for d128 {
    fn partial_cmp(&self, other: &d128) -> Option<::std::cmp::Ordering> {
        use std::cmp::Ordering;
        match self.compare(other) {
            v if v.is_nan() => None,
            v if v.is_zero() => Some(Ordering::Equal),
            v if v.is_positive() => Some(Ordering::Greater),
            v if v.is_negative() => Some(Ordering::Less),
            _ => unreachable!(),
        }
    }
}

macro_rules! ffi_unary_op {
    ($(#[$attr:meta])* impl $op:ident, $method:ident, $ffi:ident for $t:ident) => {
        $(#[$attr])*
        impl $op for $t {
            type Output = $t;

            fn $method(mut self) -> $t {
                $t::with_context(|ctx| {
                    unsafe { *$ffi(&mut self, &self, ctx)}
                })
            }
        }

        impl<'a> $op for &'a $t {
            type Output = $t;

            fn $method(self) -> $t {
                $t::with_context(|ctx| {
                    unsafe {
                        let mut res: MaybeUninit<$t> = MaybeUninit::uninit();
                        *$ffi(res.as_mut_ptr(), self, ctx)
                    }
                })
            }
        }
    }
}

macro_rules! ffi_binary_op {
    ($(#[$attr:meta])* impl $op:ident, $method:ident, $ffi:ident for $t:ident) => {
        $(#[$attr])*
        impl $op<$t> for $t {
            type Output = $t;

            fn $method(mut self, other: $t) -> $t {
                $t::with_context(|ctx| {
                    unsafe { *$ffi(&mut self, &self, &other, ctx)}
                })
            }
        }

        impl<'a> $op<$t> for &'a $t {
            type Output = $t;

            fn $method(self, mut other: $t) -> $t {
                $t::with_context(|ctx| {
                    unsafe { *$ffi(&mut other, self, &other, ctx) }
                })
            }
        }

        impl<'a> $op<&'a$t> for $t {
            type Output = $t;

            fn $method(mut self, other: &'a $t) -> $t {
                $t::with_context(|ctx| {
                    unsafe { *$ffi(&mut self, &self, other, ctx) }
                })
            }
        }

        impl<'a, 'b> $op<&'a $t> for &'b $t {
            type Output = $t;

            fn $method(self, other: &'a $t) -> $t {
                $t::with_context(|ctx| {
                    unsafe {
                        let mut res: MaybeUninit<$t> = MaybeUninit::uninit();
                        *$ffi(res.as_mut_ptr(), self, other, ctx)
                    }
                })
            }
        }
    }
}

macro_rules! ffi_unary_assign_op {
    ($(#[$attr:meta])* impl $op:ident, $method:ident, $ffi:ident for $t:ident) => {
        $(#[$attr])*
        impl $op<$t> for $t {
            fn $method(&mut self, other: $t) {
                $t::with_context(|ctx| {
                    unsafe { $ffi(self, self, &other, ctx); }
                })
            }
        }
    }
}

ffi_binary_op!(impl Add, add, decQuadAdd for d128);
ffi_binary_op!(impl Sub, sub, decQuadSubtract for d128);
ffi_binary_op!(impl Mul, mul, decQuadMultiply for d128);
ffi_binary_op!(impl Div, div, decQuadDivide for d128);
ffi_binary_op!(
/// The operands must be zero or positive, an integer (finite with zero exponent) and comprise
/// only zeros and/or ones; if not, INVALID_OPERATION is set.
    impl BitAnd, bitand, decQuadAnd for d128);
ffi_binary_op!(
/// The operands must be zero or positive, an integer (finite with zero exponent) and comprise
/// only zeros and/or ones; if not, INVALID_OPERATION is set.
    impl BitOr, bitor, decQuadOr for d128);
ffi_binary_op!(
/// The operands must be zero or positive, an integer (finite with zero exponent) and comprise
/// only zeros and/or ones; if not, INVALID_OPERATION is set.
    impl BitXor, bitxor, decQuadXor for d128);
ffi_binary_op!(impl Rem, rem, decQuadRemainder for d128);

ffi_unary_assign_op!(impl AddAssign, add_assign, decQuadAdd for d128);
ffi_unary_assign_op!(impl SubAssign, sub_assign, decQuadSubtract for d128);
ffi_unary_assign_op!(impl MulAssign, mul_assign, decQuadMultiply for d128);
ffi_unary_assign_op!(impl DivAssign, div_assign, decQuadDivide for d128);
ffi_unary_assign_op!(impl BitAndAssign, bitand_assign, decQuadAnd for d128);
ffi_unary_assign_op!(impl BitOrAssign, bitor_assign, decQuadOr for d128);
ffi_unary_assign_op!(impl BitXorAssign, bitxor_assign, decQuadXor for d128);
ffi_unary_assign_op!(impl RemAssign, rem_assign, decQuadRemainder for d128);

ffi_unary_op!(impl Neg, neg, decQuadMinus for d128);
ffi_unary_op!(
/// The operand must be zero or positive, an integer (finite with zero exponent) and comprise
/// only zeros and/or ones; if not, INVALID_OPERATION is set.
    impl Not, not, decQuadInvert for d128);

/// The result is `self` with the digits of the coefficient shifted to the left without adjusting
/// the exponent or the sign of `self`. Any digits ‘shifted in’ from the right will be 0. `amount`
/// is the count of positions to shift and must be a in the range –34 through +34. NaNs are
/// propagated as usual. If `self` is infinite the result is Infinity of the same sign. No status
/// is set unless `amount` is invalid or `self` is an sNaN.
impl Shl<usize> for d128 {
    type Output = d128;

    fn shl(mut self, amount: usize) -> d128 {
        let shift = d128::from(amount as u32);
        d128::with_context(|ctx| unsafe { *decQuadShift(&mut self, &self, &shift, ctx) })
    }
}

impl Shl<usize> for &d128 {
    type Output = d128;

    fn shl(self, amount: usize) -> d128 {
        let shift = d128::from(amount as u32);
        d128::with_context(|ctx| unsafe {
            let mut res: MaybeUninit<d128> = MaybeUninit::uninit();
            *decQuadShift(res.as_mut_ptr(), self, &shift, ctx)
        })
    }
}

impl ShlAssign<usize> for d128 {
    fn shl_assign(&mut self, amount: usize) {
        let shift = d128::from(amount as u32);
        d128::with_context(|ctx| unsafe {
            decQuadShift(self, self, &shift, ctx);
        })
    }
}

/// The result is `self` with the digits of the coefficient shifted to the right without adjusting
/// the exponent or the sign of `self`. Any digits ‘shifted in’ from the left will be 0. `amount`
/// is the count of positions to shift and must be a in the range –34 through +34. NaNs are
/// propagated as usual. If `self` is infinite the result is Infinity of the same sign. No status
/// is set unless `amount` is invalid or `self` is an sNaN.
impl Shr<usize> for d128 {
    type Output = d128;

    fn shr(mut self, amount: usize) -> d128 {
        let shift = -d128::from(amount as u32);
        d128::with_context(|ctx| unsafe { *decQuadShift(&mut self, &self, &shift, ctx) })
    }
}

impl Shr<usize> for &d128 {
    type Output = d128;

    fn shr(self, amount: usize) -> d128 {
        let shift = -d128::from(amount as u32);
        d128::with_context(|ctx| unsafe {
            let mut res: MaybeUninit<d128> = MaybeUninit::uninit();
            *decQuadShift(res.as_mut_ptr(), self, &shift, ctx)
        })
    }
}

impl ShrAssign<usize> for d128 {
    fn shr_assign(&mut self, amount: usize) {
        let shift = -d128::from(amount as u32);
        d128::with_context(|ctx| unsafe {
            decQuadShift(self, self, &shift, ctx);
        })
    }
}

impl<T> Sum<T> for d128
where
    T: Borrow<d128>,
{
    fn sum<I: IntoIterator<Item = T>>(iter: I) -> d128 {
        iter.into_iter()
            .fold(d128::zero(), |acc, val| acc + val.borrow())
    }
}

impl<T> Product<T> for d128
where
    T: Borrow<d128>,
{
    fn product<I: IntoIterator<Item = T>>(iter: I) -> d128 {
        iter.into_iter()
            .fold(d128::from(1.0), |acc, val| acc * val.borrow())
    }
}

impl Zero for d128 {
    fn zero() -> Self {
        d128::zero()
    }

    fn is_zero(&self) -> bool {
        self.is_zero()
    }
}

impl d128 {
    fn default_context() -> Context {
        unsafe {
            let mut res: MaybeUninit<Context> = MaybeUninit::uninit();
            let out = decContextDefault(res.as_mut_ptr(), 128);
            *out
        }
    }

    fn with_context<F, R>(f: F) -> R
    where
        F: FnOnce(&mut Context) -> R,
    {
        CTX.with(|ctx| f(&mut ctx.borrow_mut()))
    }

    /// Creates a d128 from raw bytes. Endianess is host dependent.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the provided bytes represent a valid decimal128
    /// value. Invalid byte patterns may result in undefined behavior when the
    /// resulting d128 is used in operations.
    pub const unsafe fn from_raw_bytes(bytes: [u8; 16]) -> d128 {
        d128 { bytes }
    }

    /// Returns raw bytes for this d128. Endianess is host dependent.
    pub fn to_raw_bytes(&self) -> [u8; 16] {
        self.bytes
    }

    /// Returns the thread local status.
    pub fn get_status() -> Status {
        d128::with_context(|ctx| Status::from_bits_truncate(ctx.status))
    }

    /// Sets the thread local status.
    pub fn set_status(status: Status) {
        d128::with_context(|ctx| ctx.status = status.bits());
    }

    /// Reads the hex binary representation from a string. This is the reverse of formatting with
    /// {:x}. Returns an error if the string is not a valid 32-character hexadecimal representation.
    pub fn from_hex(s: &str) -> Result<d128, ParseError> {
        if s.len() != 32 {
            return Err(ParseError);
        }
        unsafe {
            let mut res: d128 = MaybeUninit::zeroed().assume_init();
            for (i, octet) in s.as_bytes().chunks(2).rev().enumerate() {
                res.bytes[i] = match u8::from_str_radix(from_utf8_unchecked(octet), 16) {
                    Ok(val) => val,
                    Err(..) => return Err(ParseError),
                };
            }
            Ok(res)
        }
    }

    // Utilities and conversions, extractors, etc.

    /// Returns the d128 representing +0.
    pub fn zero() -> d128 {
        unsafe {
            let mut res: MaybeUninit<d128> = MaybeUninit::uninit();
            *decQuadZero(res.as_mut_ptr())
        }
    }

    /// Returns the d128 representing +Infinity.
    pub fn infinity() -> d128 {
        d128!(Infinity)
    }

    /// Returns the d128 representing -Infinity.
    pub fn neg_infinity() -> d128 {
        d128!(-Infinity)
    }

    /// Returns the d128 representing NaN.
    pub fn nan() -> d128 {
        d128!(NaN)
    }

    // Computational.

    /// Returns the absolute value of `self`.
    ///
    /// This function returns the magnitude of the decimal number, removing any negative sign.
    /// For special values:
    /// - If `self` is NaN, the result is NaN (possibly with a different sign)
    /// - If `self` is infinite, the result is positive infinity
    /// - If `self` is zero, the result is positive zero
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_decimal::d128;
    /// # use std::str::FromStr;
    /// assert_eq!(d128::from_str("-42.5").unwrap().abs(), d128::from_str("42.5").unwrap());
    /// assert_eq!(d128::from_str("42.5").unwrap().abs(), d128::from_str("42.5").unwrap());
    /// assert_eq!(d128::from_str("-0").unwrap().abs(), d128::from_str("0").unwrap());
    /// ```
    pub fn abs(mut self) -> d128 {
        d128::with_context(|ctx| unsafe { *decQuadAbs(&mut self, &self, ctx) })
    }

    /// Calculates the fused multiply-add `self` × `a` + `b` and returns the result. The multiply
    /// is carried out first and is exact, so this operation has only the one, final, rounding.
    pub fn mul_add<O: AsRef<d128>>(mut self, a: O, b: O) -> d128 {
        d128::with_context(|ctx| unsafe {
            *decQuadFMA(&mut self, &self, a.as_ref(), b.as_ref(), ctx)
        })
    }

    /// Returns the adjusted exponent of `self`, according to IEEE 754 rules. That is, the exponent
    /// returned is calculated as if the decimal point followed the first significant digit (so,
    /// for example, if `self` were 123 then the result would be 2). If `self` is infinite, the
    /// result is +Infinity. If `self` is a zero, the result is –Infinity, and the
    /// `DIVISION_BY_ZERO` flag is set. If `self` is less than zero, the absolute value of `self`
    /// is used. If `self` is 1, the result is 0. NaNs are handled (propagated) as for arithmetic
    /// operations.
    pub fn logb(mut self) -> d128 {
        d128::with_context(|ctx| unsafe { *decQuadLogB(&mut self, &self, ctx) })
    }

    /// If both `self` and `other` are numeric (not NaNs) this returns the larger of the two
    /// (compared using total ordering, to give a well-defined result). If either (but not both of)
    /// is a quiet NaN then the other argument is the result; otherwise NaNs are handled as for
    /// arithmetic operations.
    pub fn max<O: AsRef<d128>>(mut self, other: O) -> d128 {
        d128::with_context(|ctx| unsafe { *decQuadMax(&mut self, &self, other.as_ref(), ctx) })
    }

    /// If both `self` and `other`  are numeric (not NaNs) this returns the smaller of the two
    /// (compared using total ordering, to give a well-defined result). If either (but not both of)
    /// is a quiet NaN then the other argument is the result; otherwise NaNs are handled as for
    /// arithmetic operations.
    pub fn min<O: AsRef<d128>>(mut self, other: O) -> d128 {
        d128::with_context(|ctx| unsafe { *decQuadMin(&mut self, &self, other.as_ref(), ctx) })
    }

    /// Returns the ‘next’ d128 to `self` in the direction of +Infinity according to IEEE 754 rules
    /// for nextUp. The only status possible is `INVALID_OPERATION` (from an sNaN).
    pub fn next(mut self) -> d128 {
        d128::with_context(|ctx| unsafe { *decQuadNextPlus(&mut self, &self, ctx) })
    }

    /// Returns the ‘next’ d128 to `self` in the direction of –Infinity according to IEEE 754 rules
    /// for nextDown. The only status possible is `INVALID_OPERATION` (from an sNaN).
    pub fn previous(mut self) -> d128 {
        d128::with_context(|ctx| unsafe { *decQuadNextMinus(&mut self, &self, ctx) })
    }

    /// The number is set to the result of raising `self` to the power of `exp`. Results will be
    /// exact when `exp` has an integral value and the result does not need to be rounded, and also
    /// will be exact in certain special cases, such as when `self` is a zero (see the arithmetic
    /// specification for details). Inexact results will always be full precision, and will almost
    /// always be correctly rounded, but may be up to 1 _ulp_ (unit in last place) in error in rare
    /// cases. This is a mathematical function; the 10<sup>6</sup> restrictions on precision and
    /// range apply as described above, except that the normal range of values is allowed if `exp`
    /// has an integral value in the range –1999999997 through +999999999.
    pub fn pow<O: AsRef<d128>>(mut self, exp: O) -> d128 {
        d128::with_context(|ctx| unsafe {
            let mut num_self: MaybeUninit<decNumber> = MaybeUninit::uninit();
            let mut num_exp: MaybeUninit<decNumber> = MaybeUninit::uninit();
            decimal128ToNumber(&self, num_self.as_mut_ptr());
            decimal128ToNumber(exp.as_ref(), num_exp.as_mut_ptr());
            let mut num_self = num_self.assume_init();
            let num_exp = num_exp.assume_init();
            decNumberPower(&mut num_self, &num_self, &num_exp, ctx);
            *decimal128FromNumber(&mut self, &num_self, ctx)
        })
    }

    /// Returns the square root of `self`.
    ///
    /// The result is the positive square root of `self`. If `self` is negative,
    /// the result will be NaN. If `self` is zero, the result is zero with the same sign.
    /// If `self` is infinite and positive, the result is positive infinity.
    ///
    /// Finite results will always be full precision and may be inexact. Inexact results
    /// will almost always be correctly rounded, but may be up to 1 ulp (unit in last place)
    /// in error in rare cases.
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_decimal::d128;
    /// # use std::str::FromStr;
    /// assert_eq!(d128::from_str("4").unwrap().sqrt(), d128::from_str("2").unwrap());
    /// assert_eq!(d128::from_str("9").unwrap().sqrt(), d128::from_str("3").unwrap());
    /// assert!(d128::from_str("-1").unwrap().sqrt().is_nan());
    /// ```
    pub fn sqrt(mut self) -> d128 {
        d128::with_context(|ctx| unsafe {
            let mut num_self: MaybeUninit<decNumber> = MaybeUninit::uninit();
            decimal128ToNumber(&self, num_self.as_mut_ptr());
            let mut num_self = num_self.assume_init();
            decNumberSquareRoot(&mut num_self, &num_self, ctx);
            *decimal128FromNumber(&mut self, &num_self, ctx)
        })
    }

    /// The number is set to _e_ raised to the power of `self`. Finite results will always be full
    /// precision and inexact, except when `self` is a zero or –Infinity (giving 1 or 0
    /// respectively). Inexact results will almost always be correctly rounded, but may be up to 1
    /// ulp (unit in last place) in error in rare cases. This is a mathematical function; the
    /// 10<sup>6</sup> restrictions on precision and range apply as described above.
    pub fn exp(mut self) -> d128 {
        d128::with_context(|ctx| unsafe {
            let mut num_self: MaybeUninit<decNumber> = MaybeUninit::uninit();
            decimal128ToNumber(&self, num_self.as_mut_ptr());
            let mut num_self = num_self.assume_init();
            decNumberExp(&mut num_self, &num_self, ctx);
            *decimal128FromNumber(&mut self, &num_self, ctx)
        })
    }

    /// Rounds `self` to the nearest integer using half-up rounding.
    ///
    /// This function rounds to an integer with zero decimal places using half-up rounding.
    /// Half-up rounding rounds away from zero when the value is exactly halfway between
    /// two integers. Special values (NaN, infinity) are handled appropriately by the
    /// underlying decimal library.
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_decimal::d128;
    /// # use std::str::FromStr;
    /// assert_eq!(d128::from_str("2.4").unwrap().round(), d128::from_str("2").unwrap());
    /// assert_eq!(d128::from_str("2.5").unwrap().round(), d128::from_str("3").unwrap());
    /// assert_eq!(d128::from_str("-2.5").unwrap().round(), d128::from_str("-3").unwrap());
    /// ```
    pub fn round(mut self) -> d128 {
        d128::with_context(|ctx| unsafe {
            *decQuadToIntegralValue(&mut self, &self, ctx, Rounding::HalfUp)
        })
    }

    /// Returns the largest integer less than or equal to `self` (floor function).
    ///
    /// This function rounds `self` down to the nearest integer using floor rounding.
    /// If `self` is infinite or NaN, the appropriate special value is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_decimal::d128;
    /// # use std::str::FromStr;
    /// assert_eq!(d128::from_str("2.9").unwrap().floor(), d128::from_str("2").unwrap());
    /// assert_eq!(d128::from_str("-2.1").unwrap().floor(), d128::from_str("-3").unwrap());
    /// ```
    pub fn floor(mut self) -> d128 {
        d128::with_context(|ctx| unsafe {
            *decQuadToIntegralValue(&mut self, &self, ctx, Rounding::Floor)
        })
    }

    /// The number is set to the natural logarithm (logarithm in base e) of `self`. `self` must be
    /// positive or a zero. Finite results will always be full precision and inexact, except when
    /// `self` is equal to 1, which gives an exact result of 0. Inexact results will almost always
    /// be correctly rounded, but may be up to 1 ulp (unit in last place) in error in rare cases.
    /// This is a mathematical function; the 10<sup>6</sup> restrictions on precision and range
    /// apply as described above.
    pub fn ln(mut self) -> d128 {
        d128::with_context(|ctx| unsafe {
            let mut num_self: MaybeUninit<decNumber> = MaybeUninit::uninit();
            decimal128ToNumber(&self, num_self.as_mut_ptr());
            let mut num_self = num_self.assume_init();
            decNumberLn(&mut num_self, &num_self, ctx);
            *decimal128FromNumber(&mut self, &num_self, ctx)
        })
    }

    /// The number is set to the logarithm in base ten of `self`. `self` must be positive or a
    /// zero. Finite results will always be full precision and inexact, except when `self` is equal
    /// to an integral power of ten, in which case the result is the exact integer. Inexact results
    /// will almost always be correctly rounded, but may be up to 1 ulp (unit in last place) in
    /// error in rare cases. This is a mathematical function; the 10<sup>6</sup> restrictions on
    /// precision and range apply as described above.
    pub fn log10(mut self) -> d128 {
        d128::with_context(|ctx| unsafe {
            let mut num_self: MaybeUninit<decNumber> = MaybeUninit::uninit();
            decimal128ToNumber(&self, num_self.as_mut_ptr());
            let mut num_self = num_self.assume_init();
            decNumberLog10(&mut num_self, &num_self, ctx);
            *decimal128FromNumber(&mut self, &num_self, ctx)
        })
    }

    /// Returns the ‘next’ d128 to `self` in the direction of `other` according to proposed IEEE
    /// 754  rules for nextAfter.  If `self` == `other` the result is `self`. If either operand is
    /// a NaN the result is as for arithmetic operations. Otherwise (the operands are numeric and
    /// different) the result of adding (or subtracting) an infinitesimal positive amount to `self`
    /// and rounding towards +Infinity (or –Infinity) is returned, depending on whether `other` is
    /// larger  (or smaller) than `self`. The addition will set flags, except that if the result is
    /// normal  (finite, non-zero, and not subnormal) no flags are set.
    pub fn towards<O: AsRef<d128>>(mut self, other: O) -> d128 {
        d128::with_context(|ctx| unsafe {
            *decQuadNextToward(&mut self, &self, other.as_ref(), ctx)
        })
    }

    /// Returns `self` set to have the same quantum as `other`, if possible (that is, numerically
    /// the same value but rounded or padded if necessary to have the same exponent as `other`, for
    /// example to round a monetary quantity to cents).
    pub fn quantize<O: AsRef<d128>>(mut self, other: O) -> d128 {
        d128::with_context(|ctx| unsafe { *decQuadQuantize(&mut self, &self, other.as_ref(), ctx) })
    }

    /// Returns `self` set to have the same quantum as `other`, if possible (that is, numerically
    /// the same value but rounded or padded if necessary to have the same exponent as `other`, for
    /// example to round a monetary quantity to cents), while using the given rounding mode.
    pub fn quantize_with_rounding<O: AsRef<d128>>(mut self, other: O, rounding: Rounding) -> d128 {
        let mut context = d128::default_context();
        context.rounding = rounding;
        unsafe { *decQuadQuantize(&mut self, &self, other.as_ref(), &mut context) }
    }

    /// Returns a copy of `self` with its coefficient reduced to its shortest possible form without
    /// changing the value of the result. This removes all possible trailing zeros from the
    /// coefficient (some may remain when the number is very close to the most positive or most
    /// negative number). Infinities and NaNs are unchanged and no status is set unless `self` is
    /// an sNaN. If `self` is a zero the result exponent is 0.
    pub fn reduce(mut self) -> d128 {
        d128::with_context(|ctx| unsafe { *decQuadReduce(&mut self, &self, ctx) })
    }

    /// The result is a copy of `self` with the digits of the coefficient rotated to the left (if
    /// `amount` is positive) or to the right (if `amount` is negative) without adjusting the
    /// exponent or the sign of `self`. `amount` is the count of positions to rotate and must be a
    /// finite integer (with exponent=0) in the range -34 through +34. NaNs are propagated as
    /// usual. If `self` is infinite the result is Infinity of the same sign. No status is set
    /// unless `amount` is invalid or an operand is an sNaN.
    pub fn rotate<O: AsRef<d128>>(mut self, amount: O) -> d128 {
        d128::with_context(|ctx| unsafe { *decQuadRotate(&mut self, &self, amount.as_ref(), ctx) })
    }

    /// This calculates `self` × 10<sup>`other`</sup> and returns the result. `other` must be an
    /// integer (finite with exponent=0) in the range ±2 × (34 + 6144), typically resulting from
    /// `logb`. Underflow and overflow might occur. NaNs propagate as usual.
    pub fn scaleb<O: AsRef<d128>>(mut self, other: O) -> d128 {
        d128::with_context(|ctx| unsafe { *decQuadScaleB(&mut self, &self, other.as_ref(), ctx) })
    }

    // Comparisons.

    /// Compares `self` and `other` numerically and returns the result. The result may be –1, 0, 1,
    /// or NaN (unordered); –1 indicates that `self` is less than `other`, 0 indicates that they
    /// are numerically equal, and 1 indicates that `self` is greater than `other`. NaN is returned
    /// only if `self` or `other` is a NaN.
    pub fn compare<O: AsRef<d128>>(&self, other: O) -> d128 {
        d128::with_context(|ctx| unsafe {
            let mut res: MaybeUninit<d128> = MaybeUninit::uninit();
            *decQuadCompare(res.as_mut_ptr(), self, other.as_ref(), ctx)
        })
    }

    /// Compares `self` and `other` using the IEEE 754 total ordering (which takes into account the
    /// exponent) and returns the result. No status is set (a signaling NaN is ordered between
    /// Infinity and NaN). The result will be –1, 0, or 1.
    pub fn compare_total<O: AsRef<d128>>(&self, other: O) -> d128 {
        d128::with_context(|ctx| unsafe {
            let mut res: MaybeUninit<d128> = MaybeUninit::uninit();
            *decQuadCompareTotal(res.as_mut_ptr(), self, other.as_ref(), ctx)
        })
    }

    // Copies.

    /// Returns `self` ensuring that the encoding is canonical.
    pub fn canonical(mut self) -> d128 {
        unsafe { *decQuadCanonical(&mut self, &self) }
    }

    // Non-computational.

    /// Returns the class of `self`.
    pub fn class(&self) -> Class {
        unsafe { decQuadClass(self) }
    }

    /// Same as `class()` but returns `std::num::FpCategory`.
    pub fn classify(&self) -> FpCategory {
        use super::Class::*;
        use std::num::FpCategory::*;

        match self.class() {
            Qnan | Snan => Nan,
            PosInf | NegInf => Infinite,
            PosZero | NegZero => Zero,
            PosNormal | NegNormal => Normal,
            PosSubnormal | NegSubnormal => Subnormal,
        }
    }

    /// Returns the number of significant digits in `self`. If `self` is a zero or is infinite, 1
    /// is returned. If `self` is a NaN then the number of digits in the payload is returned.
    pub fn digits(&self) -> u32 {
        unsafe { decQuadDigits(self) }
    }

    /// Returns `true` if the encoding of `self` is canonical, or `false` otherwise.
    pub fn is_canonical(&self) -> bool {
        unsafe { decQuadIsCanonical(self) != 0 }
    }

    /// Returns `true` if `self` is neither infinite nor a NaN, or `false` otherwise.
    pub fn is_finite(&self) -> bool {
        unsafe { decQuadIsFinite(self) != 0 }
    }

    /// Returns `true` if `self` is finite and its exponent is zero, or `false` otherwise.
    pub fn is_integer(&self) -> bool {
        unsafe { decQuadIsInteger(self) != 0 }
    }

    /// Returns `true` if `self`  is a valid argument for logical operations (that is, `self` is
    /// zero or positive, an integer (finite with a zero exponent) and comprises only zeros and/or
    /// ones), or `false` otherwise.
    pub fn is_logical(&self) -> bool {
        unsafe { decQuadIsLogical(self) != 0 }
    }

    /// Returns `true` if the encoding of `self` is an infinity, or `false` otherwise.
    pub fn is_infinite(&self) -> bool {
        unsafe { decQuadIsInfinite(self) != 0 }
    }

    /// Returns `true` if `self` is a NaN (quiet or signaling), or `false` otherwise.
    pub fn is_nan(&self) -> bool {
        unsafe { decQuadIsNaN(self) != 0 }
    }

    /// Returns `true` if `self` is less than zero and not a NaN, or `false` otherwise.
    pub fn is_negative(&self) -> bool {
        unsafe { decQuadIsNegative(self) != 0 }
    }

    /// Returns `true` if `self` is a normal number (that is, is finite, non-zero, and not
    /// subnormal), or `false` otherwise.
    pub fn is_normal(&self) -> bool {
        unsafe { decQuadIsNormal(self) != 0 }
    }

    /// Returns `true` if `self` is greater than zero and not a NaN, or `false` otherwise.
    pub fn is_positive(&self) -> bool {
        unsafe { decQuadIsPositive(self) != 0 }
    }

    /// Returns `true` if `self` is a signaling NaN, or `false` otherwise.
    pub fn is_signaling(&self) -> bool {
        unsafe { decQuadIsSignaling(self) != 0 }
    }

    /// Returns `true` if `self` has a minus sign, or `false` otherwise. Note that zeros and NaNs
    /// may have a minus sign.
    pub fn is_signed(&self) -> bool {
        unsafe { decQuadIsSigned(self) != 0 }
    }

    /// Returns `true` if `self` is subnormal (that is, finite, non-zero, and with magnitude less
    /// than 10<sup>-6143</sup>), or `false` otherwise.
    pub fn is_subnormal(&self) -> bool {
        unsafe { decQuadIsSubnormal(self) != 0 }
    }

    /// Returns `true` if `self` is zero, or `false` otherwise.
    pub fn is_zero(&self) -> bool {
        unsafe { decQuadIsZero(self) != 0 }
    }

    /// Writes the decimal string to the byte buffer.
    ///
    /// Returns the length of the written string.
    ///
    /// # Safety
    /// This method does **not** ensure that the buffer is large enough for
    /// the decimal string.
    ///
    /// It is the responsibility of the caller that the size of the buffer is at least 43.
    pub unsafe fn write_to_buffer(&self, buf: &mut [u8]) -> usize {
        d128::with_context(|_ctx| unsafe {
            let mut num_self: MaybeUninit<decNumber> = MaybeUninit::uninit();
            decimal128ToNumber(self, num_self.as_mut_ptr());
            let mut num_self = num_self.assume_init();
            decNumberTrim(&mut num_self); // Reduces trailing zeroes (fractions)
            decNumberToString(&num_self, buf.as_mut().as_mut_ptr() as _);
        });
        buf.iter()
            .position(|&byte| byte == 0u8)
            .unwrap_or(buf.len())
    }

    /// Converts this d128 to a pair of u64 values representing the raw bytes.
    ///
    /// The decimal128 value is split into two 64-bit unsigned integers in little-endian
    /// byte order. The first u64 contains the lower 8 bytes and the second u64 contains
    /// the upper 8 bytes of the 16-byte decimal128 representation.
    ///
    /// # Returns
    ///
    /// A tuple `(lower_u64, upper_u64)` where:
    /// - `lower_u64` contains bytes 0-7 of the decimal128
    /// - `upper_u64` contains bytes 8-15 of the decimal128
    ///
    /// # Examples
    ///
    /// ```
    /// # use amudai_decimal::d128;
    /// let d = d128::from(42);
    /// let (lower, upper) = d.to_u64_pair();
    /// // The exact values depend on the internal decimal128 representation
    /// ```
    pub fn to_u64_pair(&self) -> (u64, u64) {
        let raw_bytes = self.to_raw_bytes();

        let v0 = u64::from_le_bytes(raw_bytes[..8].try_into().expect("to_u64_pair"));
        let v1 = u64::from_le_bytes(raw_bytes[8..].try_into().expect("to_u64_pair"));

        (v0, v1)
    }
}

unsafe extern "C" {
    // Context.
    fn decContextDefault(ctx: *mut Context, kind: u32) -> *mut Context;
    // Utilities and conversions, extractors, etc.
    fn decQuadFromBCD(res: *mut d128, exp: i32, bcd: *const u8, sign: i32) -> *mut d128;
    fn decQuadFromInt32(res: *mut d128, src: i32) -> *mut d128;
    fn decQuadFromString(res: *mut d128, s: *const c_char, ctx: *mut Context) -> *mut d128;
    fn decQuadFromUInt32(res: *mut d128, src: u32) -> *mut d128;
    //fn decQuadToString(src: *const d128, s: *mut c_char) -> *mut c_char;
    fn decQuadToInt32(src: *const d128, ctx: *mut Context, round: Rounding) -> i32;
    fn decQuadToUInt32(src: *const d128, ctx: *mut Context, round: Rounding) -> u32;
    fn decQuadToEngString(res: *const d128, s: *mut c_char) -> *mut c_char;
    fn decQuadZero(res: *mut d128) -> *mut d128;
    // Computational.
    fn decQuadAbs(res: *mut d128, src: *const d128, ctx: *mut Context) -> *mut d128;
    fn decQuadAdd(res: *mut d128, a: *const d128, b: *const d128, ctx: *mut Context) -> *mut d128;
    fn decQuadAnd(res: *mut d128, a: *const d128, b: *const d128, ctx: *mut Context) -> *mut d128;
    fn decQuadDivide(
        res: *mut d128,
        a: *const d128,
        b: *const d128,
        ctx: *mut Context,
    ) -> *mut d128;
    fn decQuadFMA(
        res: *mut d128,
        a: *const d128,
        b: *const d128,
        c: *const d128,
        ctx: *mut Context,
    ) -> *mut d128;
    fn decQuadInvert(res: *mut d128, src: *const d128, ctx: *mut Context) -> *mut d128;
    fn decQuadLogB(res: *mut d128, src: *const d128, ctx: *mut Context) -> *mut d128;
    fn decQuadMax(res: *mut d128, a: *const d128, b: *const d128, ctx: *mut Context) -> *mut d128;
    fn decQuadMin(res: *mut d128, a: *const d128, b: *const d128, ctx: *mut Context) -> *mut d128;
    fn decQuadMinus(res: *mut d128, src: *const d128, ctx: *mut Context) -> *mut d128;
    fn decQuadMultiply(
        res: *mut d128,
        a: *const d128,
        b: *const d128,
        ctx: *mut Context,
    ) -> *mut d128;
    fn decQuadNextMinus(res: *mut d128, src: *const d128, ctx: *mut Context) -> *mut d128;
    fn decQuadNextPlus(res: *mut d128, src: *const d128, ctx: *mut Context) -> *mut d128;
    fn decQuadNextToward(
        res: *mut d128,
        src: *const d128,
        other: *const d128,
        ctx: *mut Context,
    ) -> *mut d128;
    fn decQuadOr(res: *mut d128, a: *const d128, b: *const d128, ctx: *mut Context) -> *mut d128;
    fn decQuadQuantize(
        res: *mut d128,
        a: *const d128,
        b: *const d128,
        ctx: *mut Context,
    ) -> *mut d128;
    fn decQuadReduce(res: *mut d128, src: *const d128, ctx: *mut Context) -> *mut d128;
    fn decQuadRemainder(
        res: *mut d128,
        a: *const d128,
        b: *const d128,
        ctx: *mut Context,
    ) -> *mut d128;
    fn decQuadRotate(
        res: *mut d128,
        a: *const d128,
        b: *const d128,
        ctx: *mut Context,
    ) -> *mut d128;
    fn decQuadScaleB(
        res: *mut d128,
        a: *const d128,
        b: *const d128,
        ctx: *mut Context,
    ) -> *mut d128;
    fn decQuadShift(res: *mut d128, a: *const d128, b: *const d128, ctx: *mut Context)
    -> *mut d128;
    fn decQuadSubtract(
        res: *mut d128,
        a: *const d128,
        b: *const d128,
        ctx: *mut Context,
    ) -> *mut d128;
    fn decQuadXor(res: *mut d128, a: *const d128, b: *const d128, ctx: *mut Context) -> *mut d128;
    // Comparisons.
    fn decQuadCompare(
        res: *mut d128,
        a: *const d128,
        b: *const d128,
        ctx: *mut Context,
    ) -> *mut d128;
    fn decQuadCompareTotal(
        res: *mut d128,
        a: *const d128,
        b: *const d128,
        ctx: *mut Context,
    ) -> *mut d128;
    // Copies.
    fn decQuadCanonical(res: *mut d128, src: *const d128) -> *mut d128;
    fn decQuadToIntegralValue(
        res: *mut d128,
        src: *const d128,
        ctx: *mut Context,
        round: Rounding,
    ) -> *mut d128;
    // Non-computational.
    fn decQuadClass(src: *const d128) -> Class;
    fn decQuadDigits(src: *const d128) -> u32;
    fn decQuadIsCanonical(src: *const d128) -> u32;
    fn decQuadIsFinite(src: *const d128) -> u32;
    fn decQuadIsInteger(src: *const d128) -> u32;
    fn decQuadIsLogical(src: *const d128) -> u32;
    fn decQuadIsInfinite(src: *const d128) -> u32;
    fn decQuadIsNaN(src: *const d128) -> u32;
    fn decQuadIsNegative(src: *const d128) -> u32;
    fn decQuadIsNormal(src: *const d128) -> u32;
    fn decQuadIsPositive(src: *const d128) -> u32;
    fn decQuadIsSignaling(src: *const d128) -> u32;
    fn decQuadIsSigned(src: *const d128) -> u32;
    fn decQuadIsSubnormal(src: *const d128) -> u32;
    fn decQuadIsZero(src: *const d128) -> u32;
    // decNumber stuff.
    fn decimal128FromNumber(res: *mut d128, src: *const decNumber, ctx: *mut Context) -> *mut d128;
    fn decimal128ToNumber(src: *const d128, res: *mut decNumber) -> *mut decNumber;
    fn decNumberPower(
        res: *mut decNumber,
        lhs: *const decNumber,
        rhs: *const decNumber,
        ctx: *mut Context,
    ) -> *mut decNumber;
    fn decNumberSquareRoot(
        res: *mut decNumber,
        lhs: *const decNumber,
        ctx: *mut Context,
    ) -> *mut decNumber;
    fn decNumberExp(
        res: *mut decNumber,
        lhs: *const decNumber,
        ctx: *mut Context,
    ) -> *mut decNumber;
    fn decNumberLn(res: *mut decNumber, rhs: *const decNumber, ctx: *mut Context)
    -> *mut decNumber;
    fn decNumberLog10(
        res: *mut decNumber,
        rhs: *const decNumber,
        ctx: *mut Context,
    ) -> *mut decNumber;
    fn decNumberTrim(res: *mut decNumber) -> *mut decNumber;
    fn decNumberToString(src: *const decNumber, s: *mut c_char) -> *mut c_char;
}
