//! Adapters for converting iterators over references into iterators over owned values.
//!
//! This module provides extension methods for `IntoIterator` types that allow
//! cloning or copying elements from iterators over references, similar to the
//! standard library's `Iterator::cloned()` and `Iterator::copied()` methods,
//! but working at the `IntoIterator` level.
//!
//! # Examples
//!
//! ```
//! use amudai_ranges::IntoIteratorExt;
//!
//! let vec = vec![1, 2, 3];
//! let slice = &vec[..];
//!
//! // Using copied() on a slice (which implements IntoIterator)
//! let sum: i32 = slice.copied().into_iter().sum();
//! assert_eq!(sum, 6);
//!
//! // Using cloned() on a vector of strings
//! let strings = vec![String::from("hello"), String::from("world")];
//! let refs = &strings;
//! let cloned: Vec<String> = refs.cloned().into_iter().collect();
//! assert_eq!(cloned, strings);
//! ```

/// An adapter that clones elements from an iterator over references.
///
/// This struct is created by the [`IntoIteratorExt::cloned`] method.
/// It implements `IntoIterator` to produce an iterator that clones each
/// referenced element.
#[derive(Clone)]
pub struct ClonedIntoIter<I> {
    inner: I,
}

impl<'a, I, T: 'a + Clone> IntoIterator for ClonedIntoIter<I>
where
    I: IntoIterator<Item = &'a T>,
{
    type Item = T;
    type IntoIter = std::iter::Cloned<I::IntoIter>;

    /// Converts this adapter into an iterator that clones each element.
    fn into_iter(self) -> Self::IntoIter {
        <I::IntoIter as Iterator>::cloned(self.inner.into_iter())
    }
}

/// An adapter that copies elements from an iterator over references.
///
/// This struct is created by the [`IntoIteratorExt::copied`] method.
/// It implements `IntoIterator` to produce an iterator that copies each
/// referenced element.
#[derive(Clone)]
pub struct CopiedIntoIter<I> {
    inner: I,
}

impl<'a, I, T: 'a + Copy> IntoIterator for CopiedIntoIter<I>
where
    I: IntoIterator<Item = &'a T>,
{
    type Item = T;
    type IntoIter = std::iter::Copied<I::IntoIter>;

    /// Converts this adapter into an iterator that copies each element.
    fn into_iter(self) -> Self::IntoIter {
        <I::IntoIter as Iterator>::copied(self.inner.into_iter())
    }
}

/// An adapter that transforms an iterator using a custom mapping function.
///
/// This struct is created by the [`IntoIteratorExt::mapped`] method.
/// It implements `IntoIterator` to produce an iterator that is the result
/// of applying a transformation function to the source iterator.
///
/// Unlike `Iterator::map` which transforms individual elements, this adapter
/// transforms the entire iterator, allowing for more complex transformations
/// such as chaining multiple iterator methods or applying custom iterator
/// adapters.
///
/// # Type Parameters
///
/// * `I` - The source type that implements `IntoIterator`
/// * `F` - The transformation function that takes `I::IntoIter` and returns `R`
/// * `R` - The resulting iterator type returned by the transformation function
pub struct MappedIntoIter<I, F, R> {
    inner: I,
    f: F,
    _p: std::marker::PhantomData<R>,
}

impl<I, F, R> Clone for MappedIntoIter<I, F, R>
where
    I: Clone,
    F: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            f: self.f.clone(),
            _p: Default::default(),
        }
    }
}

impl<I, F, R> IntoIterator for MappedIntoIter<I, F, R>
where
    I: IntoIterator,
    F: Fn(I::IntoIter) -> R,
    R: Iterator,
{
    type Item = R::Item;

    type IntoIter = R;

    fn into_iter(self) -> Self::IntoIter {
        (self.f)(self.inner.into_iter())
    }
}

/// Extension trait for `IntoIterator` types that provides adapters for
/// cloning or copying elements from iterators over references.
///
/// This trait is automatically implemented for all types that implement
/// `IntoIterator`, allowing you to use `cloned()` and `copied()` methods
/// before calling `into_iter()`.
///
/// # Examples
///
/// ```
/// use amudai_ranges::IntoIteratorExt;
///
/// // Works with slices
/// let numbers = vec![1, 2, 3];
/// let doubled: Vec<i32> = numbers[..]
///     .copied()
///     .into_iter()
///     .map(|x| x * 2)
///     .collect();
/// assert_eq!(doubled, vec![2, 4, 6]);
///
/// // Works with references to collections
/// let strings = vec![String::from("hello"), String::from("world")];
/// let uppercased: Vec<String> = (&strings)
///     .cloned()
///     .into_iter()
///     .map(|s| s.to_uppercase())
///     .collect();
/// assert_eq!(uppercased, vec!["HELLO", "WORLD"]);
/// ```
pub trait IntoIteratorExt {
    /// Creates an adapter that will clone each element when iterated.
    ///
    /// This is useful when you have an `IntoIterator` that yields references
    /// but you need owned values.
    fn cloned<'a, T>(self) -> ClonedIntoIter<Self>
    where
        T: 'a + Clone,
        Self: Sized + IntoIterator<Item = &'a T>,
    {
        ClonedIntoIter { inner: self }
    }

    /// Creates an adapter that will copy each element when iterated.
    ///
    /// This is more efficient than `cloned()` for types that implement `Copy`,
    /// as it avoids the overhead of cloning.
    fn copied<'a, T>(self) -> CopiedIntoIter<Self>
    where
        T: 'a + Copy,
        Self: Sized + IntoIterator<Item = &'a T>,
    {
        CopiedIntoIter { inner: self }
    }

    /// Creates an adapter that transforms the iterator itself using a custom function.
    ///
    /// Unlike `Iterator::map` which transforms individual elements, `mapped` transforms
    /// the entire iterator after it's created from `into_iter()`. This allows for complex
    /// iterator transformations that go beyond simple element mapping, such as chaining
    /// multiple iterator methods, applying custom iterator adapters, or conditionally
    /// modifying the iteration behavior.
    ///
    /// # Type Parameters
    ///
    /// * `F` - A function that takes the iterator produced by `self.into_iter()` and
    ///   returns a new iterator of type `R`
    /// * `R` - The type of iterator returned by the transformation function
    fn mapped<F, R>(self, f: F) -> MappedIntoIter<Self, F, R>
    where
        Self: Sized + IntoIterator,
        F: Fn(<Self as IntoIterator>::IntoIter) -> R,
        R: Iterator,
    {
        MappedIntoIter {
            inner: self,
            f,
            _p: Default::default(),
        }
    }
}

impl<I> IntoIteratorExt for I where I: IntoIterator {}

#[cfg(test)]
mod tests {
    use crate::IntoIteratorExt;

    #[test]
    fn test_map() {
        let v = vec![1usize, 2, 3];
        let v1 = (&v).mapped(|it| it.map(|&x| x * 2));
        let res = v1.into_iter().sum::<usize>();
        assert_eq!(res, 12);
    }
}
