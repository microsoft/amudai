//! Flexible property storage and retrieval for shard metadata.
//!
//! This module provides utilities for working with collections of name-value property pairs,
//! which are commonly used throughout the Amudai format for storing metadata and
//! configuration information.
//!
//! # Overview
//!
//! The module offers two main types:
//!
//! - [`PropertyBagBuilder`]: A mutable builder for constructing property collections
//! - [`PropertyBag`]: An immutable view for reading property values with type-safe accessors
//!
//! Properties are stored as name-value pairs where:
//! - Names are strings
//! - Values are stored as [`AnyValue`] variants, supporting multiple data types

use ahash::AHashMap;

use crate::defs::common::{AnyValue, NameValuePair};

/// A builder for constructing collections of name-value property pairs.
///
/// `PropertyBagBuilder` provides a convenient way to build collections of properties
/// with type safety and efficient storage. It accepts various value types that can
/// be converted to `AnyValue` and produces a vector of `NameValuePair` entries
/// suitable for serialization.
#[derive(Debug, Clone, Default)]
pub struct PropertyBagBuilder(AHashMap<String, AnyValue>);

impl PropertyBagBuilder {
    /// Creates a new empty `PropertyBagBuilder`.
    ///
    /// # Returns
    ///
    /// A new builder instance ready to accept property assignments.
    pub fn new() -> PropertyBagBuilder {
        PropertyBagBuilder(Default::default())
    }

    /// Sets a property with the given name and value.
    ///
    /// If a property with the same name already exists, it will be replaced.
    /// The value can be any type that implements `Into<AnyValue>`.
    ///
    /// # Arguments
    ///
    /// * `name` - The property name (must implement `Into<String>`)
    /// * `value` - The property value (must implement `Into<AnyValue>`)
    pub fn set(&mut self, name: impl Into<String>, value: impl Into<AnyValue>) {
        self.0.insert(name.into(), value.into());
    }

    /// Retrieves a property value by name.
    ///
    /// Returns a reference to the `AnyValue` if the property exists,
    /// or `None` if no property with the given name is found.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to retrieve
    ///
    /// # Returns
    ///
    /// `Some(&AnyValue)` if the property exists, `None` otherwise.
    pub fn get(&self, name: impl AsRef<str>) -> Option<&AnyValue> {
        self.0.get(name.as_ref())
    }

    /// Consumes the builder and returns the constructed property collection.
    ///
    /// This method converts all stored properties into a vector of `NameValuePair`
    /// entries suitable for inclusion in the shard metadata messages.
    ///
    /// # Returns
    ///
    /// A vector of `NameValuePair` entries containing all properties.
    pub fn finish(self) -> Vec<NameValuePair> {
        self.0
            .into_iter()
            .map(|(name, value)| NameValuePair {
                name,
                value: Some(value),
            })
            .collect()
    }
}

/// A read-only view over a collection of name-value property pairs.
///
/// `PropertyBag` provides convenient methods to access typed values from a slice
/// of `NameValuePair` entries. It supports retrieval of common data types including
/// strings, integers, floating-point numbers, booleans, and byte arrays.
///
/// The bag is backed by a reference to an existing slice, making it lightweight
/// and suitable for passing around without copying the underlying data.
pub struct PropertyBag<'a>(&'a [NameValuePair]);

impl<'a> PropertyBag<'a> {
    /// Creates a new `PropertyBag` from a slice of name-value pairs.
    ///
    /// # Arguments
    ///
    /// * `pairs` - A slice of `NameValuePair` entries to wrap
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use amudai_shard::read::properties::PropertyBag;
    /// use amudai_format::defs::common::NameValuePair;
    ///
    /// let pairs = vec![];
    /// let bag = PropertyBag::new(&pairs);
    /// assert!(bag.is_empty());
    /// ```
    pub fn new(pairs: &'a [NameValuePair]) -> PropertyBag<'a> {
        PropertyBag(pairs)
    }

    /// Returns `true` if the property bag contains no entries.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Checks if a property with the given name exists in the bag.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to check for
    ///
    /// # Returns
    ///
    /// `true` if a property with the specified name exists, `false` otherwise
    pub fn contains(&self, name: impl AsRef<str>) -> bool {
        self.find_entry(name).is_some()
    }

    /// Retrieves a string value for the specified property name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to retrieve
    ///
    /// # Returns
    ///
    /// `Some(&str)` if the property exists and contains a string value,
    /// `None` if the property doesn't exist or contains a different type
    pub fn get_str(&self, name: impl AsRef<str>) -> Option<&str> {
        self.find_entry(name)
            .and_then(|entry| entry.value.as_ref())
            .and_then(|value| value.as_str())
    }

    /// Retrieves an `i64` value for the specified property name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to retrieve
    ///
    /// # Returns
    ///
    /// `Some(i64)` if the property exists and contains a compatible integer value,
    /// `None` if the property doesn't exist or contains an incompatible type
    pub fn get_i64(&self, name: impl AsRef<str>) -> Option<i64> {
        self.find_entry(name)
            .and_then(|entry| entry.value.as_ref())
            .and_then(|value| value.as_i64())
    }

    /// Retrieves a `u64` value for the specified property name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to retrieve
    ///
    /// # Returns
    ///
    /// `Some(u64)` if the property exists and contains a compatible unsigned integer value,
    /// `None` if the property doesn't exist or contains an incompatible type
    pub fn get_u64(&self, name: impl AsRef<str>) -> Option<u64> {
        self.find_entry(name)
            .and_then(|entry| entry.value.as_ref())
            .and_then(|value| value.as_u64())
    }

    /// Retrieves an `f64` value for the specified property name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to retrieve
    ///
    /// # Returns
    ///
    /// `Some(f64)` if the property exists and contains a floating-point value,
    /// `None` if the property doesn't exist or contains a different type
    pub fn get_f64(&self, name: impl AsRef<str>) -> Option<f64> {
        self.find_entry(name)
            .and_then(|entry| entry.value.as_ref())
            .and_then(|value| value.as_f64())
    }

    /// Retrieves a boolean value for the specified property name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to retrieve
    ///
    /// # Returns
    ///
    /// `Some(bool)` if the property exists and contains a boolean value,
    /// `None` if the property doesn't exist or contains a different type
    pub fn get_bool(&self, name: impl AsRef<str>) -> Option<bool> {
        self.find_entry(name)
            .and_then(|entry| entry.value.as_ref())
            .and_then(|value| value.as_bool())
    }

    /// Retrieves a byte array value for the specified property name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to retrieve
    ///
    /// # Returns
    ///
    /// `Some(&[u8])` if the property exists and contains binary data or string data,
    /// `None` if the property doesn't exist or contains an incompatible type
    pub fn get_bytes(&self, name: impl AsRef<str>) -> Option<&[u8]> {
        self.find_entry(name)
            .and_then(|entry| entry.value.as_ref())
            .and_then(|value| value.as_bytes())
    }

    /// Retrieves the raw `AnyValue` for the specified property name.
    ///
    /// This method returns the underlying `AnyValue` without type conversion,
    /// allowing access to the original value and its annotation if present.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to retrieve
    ///
    /// # Returns
    ///
    /// `Some(&AnyValue)` if the property exists, `None` if it doesn't exist
    pub fn get_any_value(&self, name: impl AsRef<str>) -> Option<&AnyValue> {
        self.find_entry(name).and_then(|entry| entry.value.as_ref())
    }

    /// Finds the raw name-value pair entry for the specified property name.
    ///
    /// This method provides access to the underlying `NameValuePair` structure,
    /// which includes both the property name and its value.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to find
    ///
    /// # Returns
    ///
    /// `Some(&NameValuePair)` if a property with the given name exists,
    /// `None` if no such property is found
    pub fn find_entry(&self, name: impl AsRef<str>) -> Option<&NameValuePair> {
        let name = name.as_ref();
        self.pairs().find(|entry| entry.name == name)
    }

    /// Returns an iterator over all name-value pairs in the property bag.
    ///
    /// # Returns
    ///
    /// An iterator that yields references to `NameValuePair` entries
    pub fn pairs(&self) -> impl Iterator<Item = &NameValuePair> {
        self.0.iter()
    }
}
