#[macro_use]
extern crate bitflags;
extern crate dtoa;
extern crate libc;
extern crate num_traits;
#[cfg(feature = "ord_subset")]
extern crate ord_subset;
#[cfg(feature = "serde")]
extern crate serde;
#[cfg(feature = "serde")]
#[cfg(test)]
extern crate serde_json;

#[macro_export]
/// A macro to construct d128 literals.
///
/// # Examples:
/// ```
/// # #[macro_use]
/// # extern crate amudai_decimal;
/// # fn main() {
/// assert!(d128!(NaN).is_nan());
/// assert!(d128!(0).is_zero());
/// assert!(d128!(-0.1).is_negative());
/// # }
/// ```
macro_rules! d128 {
    ($lit:expr) => {{
        use std::str::FromStr;
        $crate::d128::from_str(stringify!($lit)).expect("Invalid decimal float literal")
    }};
}

mod context;
mod dec128;

#[cfg(test)]
mod test;

pub use dec128::d128;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Rounding {
    Ceiling = 0,
    Up,
    HalfUp,
    /// Round to nearest; if equidistant, round so that the final digit is even.
    /// This is the only rounding mode supported.
    HalfEven,
    HalfDown,
    Down,
    Floor,
    ZeroOrFiveUp,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
#[allow(dead_code)]
pub enum Class {
    Snan = 0,
    Qnan,
    NegInf,
    NegNormal,
    NegSubnormal,
    NegZero,
    PosZero,
    PosSubnormal,
    PosNormal,
    PosInf,
}

bitflags! {
    /// The status of the floating point context. Instead of faulting after every operation errors
    /// are added to this status. User code can check and/or clear the status fully or partially at
    /// specific points to check the validity of the computation. Each thread gets its own status.
    /// The status is initially empty.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[macro_use]
    /// # extern crate amudai_decimal;
    /// # use amudai_decimal::d128;
    /// # use amudai_decimal::Status;
    /// # fn main() {
    /// // Clear any existing status
    /// d128::set_status(Status::empty());
    /// assert_eq!(d128::get_status(), Status::empty());
    ///
    /// // Create numbers directly to avoid macro parsing affecting status
    /// let one = d128::from(1);
    /// let zero = d128::from(0);
    ///
    /// let _ = one / zero;
    /// assert!(d128::get_status().contains(amudai_decimal::Status::DIVISION_BY_ZERO));
    ///
    /// let _ = zero / zero;
    /// assert!(d128::get_status().contains(amudai_decimal::Status::DIVISION_UNDEFINED));
    /// // The previous status flag was not cleared!
    /// assert!(d128::get_status().contains(amudai_decimal::Status::DIVISION_BY_ZERO));
    /// # }
    #[derive(Debug, PartialEq)]
    pub struct Status: u32 {
        /// Conversion syntax error.
        const CONVERSION_SYNTAX    = 0x00000001;
        /// Division by zero.
        const DIVISION_BY_ZERO     = 0x00000002;
        /// Division impossible.
        const DIVISION_IMPOSSIBLE  = 0x00000004;
        /// Division undefined.
        const DIVISION_UNDEFINED   = 0x00000008;
        // const INSUFFICIENT_STORAGE = 0x00000010;
        /// The result is inexact.
        const INEXACT              = 0x00000020;
        // const INVALID_CONTEXT      = 0x00000040;
        /// An invalid operation was requested.
        const INVALID_OPERATION    = 0x00000080;
        // const LOST_DIGITS          = 0x00000100;
        /// Overflow.
        const OVERFLOW             = 0x00000200;
        // const CLAMPED              = 0x00000400;
        // const ROUNDED              = 0x00000800;
        // const SUBNORMAL            = 0x00001000;
        /// Underflow.
        const UNDERFLOW            = 0x00002000;
    }
}
