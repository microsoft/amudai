use amudai_format::defs::common;

/// A read-only view over a collection of name-value property pairs.
///
/// `PropertyBag` provides convenient methods to access typed values from a slice
/// of `NameValuePair` entries. It supports retrieval of common data types including
/// strings, integers, floating-point numbers, booleans, and byte arrays.
///
/// The bag is backed by a reference to an existing slice, making it lightweight
/// and suitable for passing around without copying the underlying data.
pub struct PropertyBag<'a>(&'a [common::NameValuePair]);

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
    pub fn new(pairs: &'a [common::NameValuePair]) -> PropertyBag<'a> {
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
    pub fn get_any_value(&self, name: impl AsRef<str>) -> Option<&common::AnyValue> {
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
    pub fn find_entry(&self, name: impl AsRef<str>) -> Option<&common::NameValuePair> {
        let name = name.as_ref();
        self.pairs().find(|entry| entry.name == name)
    }

    /// Returns an iterator over all name-value pairs in the property bag.
    ///
    /// # Returns
    ///
    /// An iterator that yields references to `NameValuePair` entries
    pub fn pairs(&self) -> impl Iterator<Item = &common::NameValuePair> {
        self.0.iter()
    }
}
