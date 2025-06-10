use super::fastlanes;
use amudai_bytes::buffer::AlignedByteVec;
use num_traits::{
    AsPrimitive, FromBytes, One, PrimInt, Signed, ToBytes, Unsigned, WrappingAdd, WrappingSub, Zero,
};
use ordered_float::{FloatCore, OrderedFloat, PrimitiveFloat};
use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    ops::{Add, Mul, Sub},
};

/// Numeric value type that can be used for encoding using
/// one of existing numeric encodings.
pub trait NumericValue:
    Ord
    + Copy
    + Zero
    + One
    + Hash
    + Add<Output = Self>
    + Sub<Output = Self>
    + Mul<Output = Self>
    + ToBytes
    + FromBytes
    + bytemuck::Zeroable
    + bytemuck::Pod
    + Send
    + Sync
    + Default
    + 'static
{
    const SIZE: usize;
    const BITS_COUNT: usize = Self::SIZE * 8;

    fn read_from<S: AsRef<[u8]>>(src: S) -> Self;
}

macro_rules! impl_numeric_value {
    ($T:ty) => {
        impl NumericValue for $T {
            const SIZE: usize = std::mem::size_of::<$T>();

            fn read_from<S: AsRef<[u8]>>(src: S) -> $T {
                let mut b = [0u8; Self::SIZE];
                b.copy_from_slice(&src.as_ref()[..Self::SIZE]);
                <$T as FromBytes>::from_le_bytes(&b)
            }
        }
    };
}

impl_numeric_value!(FloatValue<f32>);
impl_numeric_value!(FloatValue<f64>);
impl_numeric_value!(i8);
impl_numeric_value!(i16);
impl_numeric_value!(i32);
impl_numeric_value!(i64);
impl_numeric_value!(i128);
impl_numeric_value!(u8);
impl_numeric_value!(u16);
impl_numeric_value!(u32);
impl_numeric_value!(u64);
impl_numeric_value!(u128);

pub trait IntegerValue:
    NumericValue + PrimInt + WrappingAdd + WrappingSub + AsPrimitive<Self::UnsignedType>
{
    type UnsignedType: UIntegerValue + AsPrimitive<Self>;
}

macro_rules! impl_integer_value {
    ($T:ty, $U:ty) => {
        impl IntegerValue for $T {
            type UnsignedType = $U;
        }
    };
}

impl_integer_value!(i8, u8);
impl_integer_value!(i16, u16);
impl_integer_value!(i32, u32);
impl_integer_value!(i64, u64);
impl_integer_value!(i128, u128);
impl_integer_value!(u8, u8);
impl_integer_value!(u16, u16);
impl_integer_value!(u32, u32);
impl_integer_value!(u64, u64);
impl_integer_value!(u128, u128);

pub trait SIntegerValue: IntegerValue + Signed + AsPrimitive<Self::UnsignedType> {}

impl SIntegerValue for i8 {}
impl SIntegerValue for i16 {}
impl SIntegerValue for i32 {}
impl SIntegerValue for i64 {}
impl SIntegerValue for i128 {}

pub trait UIntegerValue:
    IntegerValue + Unsigned + fastlanes::BitPacking + fastlanes::FusedFrameOfReference
{
}

impl UIntegerValue for u8 {}
impl UIntegerValue for u16 {}
impl UIntegerValue for u32 {}
impl UIntegerValue for u64 {}
impl UIntegerValue for u128 {}

pub trait DecimalValue: NumericValue {}

impl DecimalValue for FloatValue<f32> {}
impl DecimalValue for FloatValue<f64> {}

impl<T> Default for FloatValue<T>
where
    T: FloatCore,
{
    fn default() -> Self {
        FloatValue(OrderedFloat(T::zero()))
    }
}

/// A wrapper around basic floating point types that implements
/// all required traits needed for encoding a sequence of such values.
#[derive(Debug, Clone, Copy)]
pub struct FloatValue<T>(pub OrderedFloat<T>)
where
    T: FloatCore;

impl<T> FloatValue<T>
where
    T: FloatCore,
{
    pub const fn new(value: T) -> Self {
        FloatValue(OrderedFloat(value))
    }
}

impl<T> From<T> for FloatValue<T>
where
    T: FloatCore,
{
    fn from(value: T) -> Self {
        Self(OrderedFloat(value))
    }
}

impl ToBytes for FloatValue<f32> {
    type Bytes = [u8; 4];

    fn to_be_bytes(&self) -> Self::Bytes {
        self.0.to_be_bytes()
    }

    fn to_le_bytes(&self) -> Self::Bytes {
        self.0.to_le_bytes()
    }
}

impl ToBytes for FloatValue<f64> {
    type Bytes = [u8; 8];

    fn to_be_bytes(&self) -> Self::Bytes {
        self.0.to_be_bytes()
    }

    fn to_le_bytes(&self) -> Self::Bytes {
        self.0.to_le_bytes()
    }
}

impl FromBytes for FloatValue<f32> {
    type Bytes = [u8; 4];

    fn from_be_bytes(bytes: &Self::Bytes) -> Self {
        Self(OrderedFloat(f32::from_be_bytes(*bytes)))
    }

    fn from_le_bytes(bytes: &Self::Bytes) -> Self {
        Self(OrderedFloat(f32::from_le_bytes(*bytes)))
    }
}

impl FromBytes for FloatValue<f64> {
    type Bytes = [u8; 8];

    fn from_be_bytes(bytes: &Self::Bytes) -> Self {
        Self(OrderedFloat(f64::from_be_bytes(*bytes)))
    }

    fn from_le_bytes(bytes: &Self::Bytes) -> Self {
        Self(OrderedFloat(f64::from_le_bytes(*bytes)))
    }
}

impl<T> Sub for FloatValue<T>
where
    T: FloatCore,
{
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0 - rhs.0)
    }
}

impl<T> Add for FloatValue<T>
where
    T: FloatCore,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl<T> Mul for FloatValue<T>
where
    T: FloatCore,
{
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        Self(self.0 * rhs.0)
    }
}

impl<T> Zero for FloatValue<T>
where
    T: FloatCore,
{
    fn zero() -> Self {
        FloatValue(OrderedFloat(T::zero()))
    }

    fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

impl<T> One for FloatValue<T>
where
    T: FloatCore,
{
    fn one() -> Self {
        FloatValue(OrderedFloat(T::one()))
    }
}

impl<T> Hash for FloatValue<T>
where
    T: FloatCore + PrimitiveFloat,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl<T> PartialEq for FloatValue<T>
where
    T: FloatCore,
{
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl<T: FloatCore> Eq for FloatValue<T> {}

impl<T> PartialOrd for FloatValue<T>
where
    T: FloatCore,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for FloatValue<T>
where
    T: FloatCore,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

unsafe impl bytemuck::Zeroable for FloatValue<f32> {}

unsafe impl bytemuck::Zeroable for FloatValue<f64> {}

unsafe impl bytemuck::Pod for FloatValue<f32> {}

unsafe impl bytemuck::Pod for FloatValue<f64> {}

pub trait ValueWriter {
    /// Appends a value to the end of the buffer.
    fn write_value<T>(&mut self, value: T)
    where
        T: ToBytes;

    /// Writes a value at the specified position in the buffer.
    fn write_value_at<T>(&mut self, pos: usize, value: T)
    where
        T: ToBytes;

    /// Appends multiple values to the end of the buffer.
    fn write_values<T>(&mut self, values: &[T])
    where
        T: ToBytes;
}

impl ValueWriter for AlignedByteVec {
    #[inline]
    fn write_value<T>(&mut self, value: T)
    where
        T: ToBytes,
    {
        self.extend_from_slice(value.to_le_bytes().as_ref());
    }

    #[inline]
    fn write_value_at<T>(&mut self, pos: usize, value: T)
    where
        T: ToBytes,
    {
        self[pos..pos + std::mem::size_of::<T>()].copy_from_slice(value.to_le_bytes().as_ref());
    }

    #[inline]
    fn write_values<T>(&mut self, values: &[T])
    where
        T: ToBytes,
    {
        let bytes_count = std::mem::size_of_val(values);
        let values = unsafe { std::slice::from_raw_parts(values.as_ptr() as *mut u8, bytes_count) };
        self.extend_from_slice(values);
    }
}

pub trait ValueReader {
    fn read_value<T>(&self, pos: usize) -> T
    where
        T: NumericValue;
}

impl<S> ValueReader for S
where
    S: AsRef<[u8]>,
{
    fn read_value<T>(&self, pos: usize) -> T
    where
        T: NumericValue,
    {
        T::read_from(&self.as_ref()[pos..])
    }
}

#[allow(dead_code)]
pub trait AsByteSlice {
    fn as_byte_slice(&self) -> &[u8];
}

pub trait AsMutByteSlice {
    fn as_mut_byte_slice(&mut self) -> &mut [u8];
}

impl<T> AsByteSlice for &[T] {
    fn as_byte_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self.as_ptr() as *const u8, std::mem::size_of_val(*self))
        }
    }
}

impl<T> AsMutByteSlice for &mut [T] {
    fn as_mut_byte_slice(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.as_mut_ptr() as *mut u8,
                std::mem::size_of_val(*self),
            )
        }
    }
}
