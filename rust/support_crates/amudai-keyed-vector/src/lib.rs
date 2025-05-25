use std::ops::Range;

/// A vector-like data structure that allows accessing elements by a key.
///
/// `KeyedVector` stores values in a `Vec` and maintains an index that maps keys to the
/// corresponding index in the `Vec`. This allows for efficient lookup of values by key.
///
/// The key type `K` must implement `From<usize>`, `Into<usize>`, and `Copy`.
///
/// The `KeyedVector` is not meant to be a general-purpose map, but rather a specialized
/// data structure for cases where keys are derived from the values themselves and are
/// relatively dense.
///
/// # Examples
///
/// ```
/// use amudai_keyed_vector::KeyedVector;
/// use amudai_keyed_vector::KeyFromValue;
///
/// #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
/// struct TestKey(u32);
///
/// impl From<usize> for TestKey {
///     fn from(value: usize) -> Self {
///         TestKey(value as u32)
///     }
/// }
///
/// impl From<TestKey> for usize {
///     fn from(value: TestKey) -> Self {
///         value.0 as usize
///     }
/// }
///
/// #[derive(Debug)]
/// struct TestValue(TestKey, u64);
///
/// impl KeyFromValue<TestKey> for TestValue {
///     fn key(&self) -> TestKey {
///         self.0
///     }
/// }
///
/// let mut v = KeyedVector::<TestKey, TestValue>::new();
/// v.push(TestValue(1.into(), 100));
/// v.push(TestValue(3.into(), 300));
///
/// assert_eq!(v.get(1.into()).unwrap().1, 100);
/// assert_eq!(v.get(3.into()).unwrap().1, 300);
/// assert!(v.get(2.into()).is_none());
/// ```
#[derive(Clone, Default)]
pub struct KeyedVector<K, V> {
    /// The underlying vector storing the values.
    values: Vec<V>,
    /// An index mapping keys to indices in the `values` vector.
    index: Vec<usize>,
    /// Phantom data to hold the key type.
    _k: std::marker::PhantomData<K>,
}

impl<K, V> KeyedVector<K, V> {
    /// Creates a new empty `KeyedVector`.
    pub fn new() -> KeyedVector<K, V> {
        KeyedVector {
            values: Vec::new(),
            index: Vec::new(),
            _k: Default::default(),
        }
    }

    /// Creates a new empty `KeyedVector` with the specified capacity.
    pub fn with_capacity(capacity: usize) -> KeyedVector<K, V> {
        KeyedVector {
            values: Vec::with_capacity(capacity),
            index: Vec::with_capacity(capacity),
            _k: Default::default(),
        }
    }

    /// Returns the number of elements in the `KeyedVector`.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns `true` if the `KeyedVector` contains no elements.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns a slice of the values in the `KeyedVector`.
    pub fn values(&self) -> &[V] {
        &self.values
    }

    /// Returns a mutable slice of the values in the `KeyedVector`.
    pub fn values_mut(&mut self) -> &mut [V] {
        &mut self.values
    }

    /// Consumes the `KeyedVector` and returns the underlying `Vec` of values.
    pub fn into_values(self) -> Vec<V> {
        self.values
    }
}

impl<K, V> KeyedVector<K, V>
where
    K: From<usize> + Into<usize> + Copy,
{
    /// Checks if the `KeyedVector` contains a value associated with the given key.
    pub fn contains_key(&self, key: K) -> bool {
        self.index_of_key(key).is_some()
    }

    /// Returns a reference to the value associated with the given key,
    /// or `None` if the key is not present.
    pub fn get(&self, key: K) -> Option<&V> {
        self.index_of_key(key).map(|i| &self.values[i])
    }

    /// Returns a mutable reference to the value associated with the given key,
    /// or `None` if the key is not present.
    pub fn get_mut(&mut self, key: K) -> Option<&mut V> {
        self.index_of_key(key).map(|i| &mut self.values[i])
    }

    /// Inserts a new value with the given key into the `KeyedVector`.
    ///
    /// # Panics
    ///
    /// Panics if the key already exists in the `KeyedVector`.
    pub fn push_entry(&mut self, key: K, value: V) {
        assert!(!self.contains_key(key));
        let key = key.into();
        let idx = self.values.len();
        self.values.push(value);
        if key >= self.index.len() {
            self.index.resize(key + 1, usize::MAX);
        }
        self.index[key] = idx;
    }

    pub fn key_range(&self) -> Range<K> {
        K::from(0)..K::from(self.index.len())
    }

    /// Returns the index of the value associated with the given key,
    /// or `None` if the key is not present.
    fn index_of_key(&self, key: K) -> Option<usize> {
        self.index
            .get(key.into())
            .copied()
            .filter(|&i| i != usize::MAX)
    }
}

impl<K, V> KeyedVector<K, V>
where
    K: From<usize> + Into<usize> + Copy,
    V: KeyFromValue<K>,
{
    /// Inserts a new value into the `KeyedVector`, using the value's key.
    pub fn push(&mut self, value: V) {
        self.push_entry(value.key(), value);
    }

    /// Removes the last element from the `KeyedVector` and returns it,
    /// or `None` if it is empty.
    pub fn pop(&mut self) -> Option<V> {
        let v = self.values.pop()?;
        let key = v.key();
        self.index[key.into()] = usize::MAX;
        Some(v)
    }

    /// Removes the value associated with the given key from the `KeyedVector`
    /// and returns it, or `None` if the key is not present.
    ///
    /// This method swaps the removed element with the last element in the vector,
    /// which does not preserve the order of elements.
    pub fn swap_remove(&mut self, key: K) -> Option<V> {
        let idx = self.index_of_key(key)?;
        let v = self.values.swap_remove(idx);
        self.index[key.into()] = usize::MAX;
        if idx < self.values.len() {
            let key = self.values[idx].key();
            self.index[key.into()] = idx;
        }
        Some(v)
    }

    /// Returns an iterator over the key-value pairs in the `KeyedVector`.
    pub fn iter(&self) -> impl Iterator<Item = (K, &V)> {
        self.values.iter().map(|v| (v.key(), v))
    }

    /// Verifies the internal consistency of the `KeyedVector`.
    ///
    /// # Panics
    ///
    /// Panics if the internal state of the `KeyedVector` is inconsistent.
    pub fn verify(&self) {
        assert_eq!(
            self.index.iter().filter(|&&i| i != usize::MAX).count(),
            self.len()
        );
        for v in &self.values {
            assert!(self.contains_key(v.key()));
        }
    }

    fn from_values(values: Vec<V>) -> KeyedVector<K, V> {
        let index_len = values
            .iter()
            .map(|v| v.key().into())
            .max()
            .map_or(0, |i| i + 1);
        let mut index = vec![usize::MAX; index_len];
        values.iter().enumerate().for_each(|(idx, v)| {
            assert_eq!(
                index[v.key().into()],
                usize::MAX,
                "attempt to build KeyedVector with duplicate keys"
            );
            index[v.key().into()] = idx;
        });
        KeyedVector {
            values,
            index,
            _k: Default::default(),
        }
    }
}

impl<K, V> std::ops::Index<usize> for KeyedVector<K, V> {
    type Output = V;

    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

impl<K, V> std::ops::IndexMut<usize> for KeyedVector<K, V> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.values[index]
    }
}

impl<K, V> std::fmt::Debug for KeyedVector<K, V>
where
    V: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyedVector")
            .field("values", &self.values)
            .finish_non_exhaustive()
    }
}

impl<K, V> From<Vec<V>> for KeyedVector<K, V>
where
    K: From<usize> + Into<usize> + Copy,
    V: KeyFromValue<K>,
{
    fn from(values: Vec<V>) -> Self {
        Self::from_values(values)
    }
}

/// A trait for types that can provide a key.
pub trait KeyFromValue<K> {
    /// Returns the key associated with the value.
    fn key(&self) -> K;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
    struct TestKey(u32);

    impl From<usize> for TestKey {
        fn from(value: usize) -> Self {
            TestKey(value as u32)
        }
    }

    impl From<TestKey> for usize {
        fn from(value: TestKey) -> Self {
            value.0 as usize
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestValue(TestKey, u64);

    impl KeyFromValue<TestKey> for TestValue {
        fn key(&self) -> TestKey {
            self.0
        }
    }

    #[test]
    fn test_push_pop() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        for k in 0..10 {
            v.push(TestValue((k * 5).into(), k as u64 * 1000));
        }

        assert_eq!(v.len(), 10);
        assert!(v.contains_key(10.into()));
        assert!(!v.contains_key(11.into()));
        assert_eq!(v.get(20.into()).unwrap().1, 4000);
        dbg!(v.values());

        assert_eq!(v.pop().unwrap().1, 9000);
        assert!(!v.contains_key(45.into()));
        assert!(v.contains_key(10.into()));
        v.verify();
    }

    #[test]
    fn test_new() {
        let v = KeyedVector::<TestKey, TestValue>::new();
        assert!(v.is_empty());
        assert_eq!(v.len(), 0);
    }

    #[test]
    fn test_with_capacity() {
        let v = KeyedVector::<TestKey, TestValue>::with_capacity(10);
        assert!(v.is_empty());
        assert_eq!(v.len(), 0);
    }

    #[test]
    fn test_len_and_is_empty() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        assert!(v.is_empty());
        assert_eq!(v.len(), 0);

        v.push(TestValue(1.into(), 100));
        assert!(!v.is_empty());
        assert_eq!(v.len(), 1);

        v.push(TestValue(2.into(), 200));
        assert!(!v.is_empty());
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn test_values() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        v.push(TestValue(1.into(), 100));
        v.push(TestValue(3.into(), 300));

        let values = v.values();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].1, 100);
        assert_eq!(values[1].1, 300);
    }

    #[test]
    fn test_values_mut() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        v.push(TestValue(1.into(), 100));
        v.push(TestValue(3.into(), 300));

        let values = v.values_mut();
        values[0].1 = 101;
        values[1].1 = 301;

        assert_eq!(v.get(1.into()).unwrap().1, 101);
        assert_eq!(v.get(3.into()).unwrap().1, 301);
    }

    #[test]
    fn test_into_values() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        v.push(TestValue(1.into(), 100));
        v.push(TestValue(3.into(), 300));

        let values = v.into_values();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].1, 100);
        assert_eq!(values[1].1, 300);
    }

    #[test]
    fn test_contains_key() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        v.push(TestValue(1.into(), 100));
        v.push(TestValue(3.into(), 300));

        assert!(v.contains_key(1.into()));
        assert!(v.contains_key(3.into()));
        assert!(!v.contains_key(2.into()));
        assert!(!v.contains_key(4.into()));
    }

    #[test]
    fn test_get() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        v.push(TestValue(1.into(), 100));
        v.push(TestValue(3.into(), 300));

        assert_eq!(v.get(1.into()).unwrap().1, 100);
        assert_eq!(v.get(3.into()).unwrap().1, 300);
        assert!(v.get(2.into()).is_none());
    }

    #[test]
    fn test_get_mut() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        v.push(TestValue(1.into(), 100));
        v.push(TestValue(3.into(), 300));

        v.get_mut(1.into()).unwrap().1 = 101;
        assert_eq!(v.get(1.into()).unwrap().1, 101);

        let value = v.get_mut(3.into()).unwrap();
        value.1 = 301;
        assert_eq!(v.get(3.into()).unwrap().1, 301);

        assert!(v.get_mut(2.into()).is_none());
    }

    #[test]
    fn test_push_entry() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        v.push_entry(1.into(), TestValue(1.into(), 100));
        v.push_entry(3.into(), TestValue(3.into(), 300));

        assert_eq!(v.get(1.into()).unwrap().1, 100);
        assert_eq!(v.get(3.into()).unwrap().1, 300);
        assert!(v.get(2.into()).is_none());
    }

    #[test]
    #[should_panic]
    fn test_push_entry_duplicate_key() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        v.push_entry(1.into(), TestValue(1.into(), 100));
        v.push_entry(1.into(), TestValue(1.into(), 200));
    }

    #[test]
    fn test_push() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        v.push(TestValue(1.into(), 100));
        v.push(TestValue(3.into(), 300));

        assert_eq!(v.get(1.into()).unwrap().1, 100);
        assert_eq!(v.get(3.into()).unwrap().1, 300);
        assert!(v.get(2.into()).is_none());
    }

    #[test]
    fn test_pop_empty() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        assert!(v.pop().is_none());
    }

    #[test]
    fn test_pop() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        v.push(TestValue(1.into(), 100));
        v.push(TestValue(3.into(), 300));

        let popped = v.pop().unwrap();
        assert_eq!(popped.1, 300);
        assert_eq!(v.len(), 1);
        assert!(v.contains_key(1.into()));
        assert!(!v.contains_key(3.into()));
    }

    #[test]
    fn test_swap_remove_empty() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        assert!(v.swap_remove(1.into()).is_none());
    }

    #[test]
    fn test_swap_remove_not_found() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        v.push(TestValue(1.into(), 100));
        assert!(v.swap_remove(2.into()).is_none());
        assert_eq!(v.len(), 1);
        assert!(v.contains_key(1.into()));
    }

    #[test]
    fn test_swap_remove() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        v.push(TestValue(1.into(), 100));
        v.push(TestValue(3.into(), 300));
        v.push(TestValue(5.into(), 500));

        let removed = v.swap_remove(3.into()).unwrap();
        assert_eq!(removed.1, 300);
        assert_eq!(v.len(), 2);
        assert!(v.contains_key(1.into()));
        assert!(v.contains_key(5.into()));
        assert!(!v.contains_key(3.into()));

        let removed = v.swap_remove(1.into()).unwrap();
        assert_eq!(removed.1, 100);
        assert_eq!(v.len(), 1);
        assert!(!v.contains_key(1.into()));
        assert!(v.contains_key(5.into()));

        let removed = v.swap_remove(5.into()).unwrap();
        assert_eq!(removed.1, 500);
        assert_eq!(v.len(), 0);
        assert!(!v.contains_key(5.into()));
    }

    #[test]
    fn test_iter() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        v.push(TestValue(1.into(), 100));
        v.push(TestValue(3.into(), 300));
        v.push(TestValue(5.into(), 500));

        let mut iter = v.iter();
        assert_eq!(iter.next().unwrap(), (1.into(), &TestValue(1.into(), 100)));
        assert_eq!(iter.next().unwrap(), (3.into(), &TestValue(3.into(), 300)));
        assert_eq!(iter.next().unwrap(), (5.into(), &TestValue(5.into(), 500)));
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_index() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        v.push(TestValue(1.into(), 100));
        v.push(TestValue(3.into(), 300));

        assert_eq!(v[0].1, 100);
        assert_eq!(v[1].1, 300);
    }

    #[test]
    fn test_index_mut() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        v.push(TestValue(1.into(), 100));
        v.push(TestValue(3.into(), 300));

        v[0].1 = 101;
        v[1].1 = 301;

        assert_eq!(v[0].1, 101);
        assert_eq!(v[1].1, 301);
    }

    #[test]
    fn test_debug() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        v.push(TestValue(1.into(), 100));
        v.push(TestValue(3.into(), 300));

        let debug_str = format!("{v:?}");
        assert!(debug_str.contains("KeyedVector"));
        assert!(debug_str.contains("values:"));
        assert!(debug_str.contains("TestValue(TestKey(1), 100)"));
        assert!(debug_str.contains("TestValue(TestKey(3), 300)"));
    }

    #[test]
    fn test_verify_empty() {
        let v = KeyedVector::<TestKey, TestValue>::new();
        v.verify();
    }

    #[test]
    fn test_verify_consistent() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        v.push(TestValue(1.into(), 100));
        v.push(TestValue(3.into(), 300));
        v.verify();
    }

    #[test]
    #[should_panic]
    fn test_verify_inconsistent_index() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        v.push(TestValue(1.into(), 100));
        v.push(TestValue(3.into(), 300));
        v.index[1] = usize::MAX;
        v.verify();
    }

    #[test]
    #[should_panic]
    fn test_verify_inconsistent_key() {
        let mut v = KeyedVector::<TestKey, TestValue>::new();
        v.push(TestValue(1.into(), 100));
        v.push(TestValue(3.into(), 300));
        v.values[0] = TestValue(2.into(), 100);
        v.verify();
    }

    #[test]
    fn test_from_vec() {
        let values = vec![
            TestValue(0.into(), 10),
            TestValue(2.into(), 20),
            TestValue(5.into(), 50),
        ];
        let keyed_vector: KeyedVector<TestKey, TestValue> = values.clone().into();

        assert_eq!(keyed_vector.len(), 3);
        assert_eq!(
            keyed_vector.get(0.into()).unwrap(),
            &TestValue(0.into(), 10)
        );
        assert_eq!(
            keyed_vector.get(2.into()).unwrap(),
            &TestValue(2.into(), 20)
        );
        assert_eq!(
            keyed_vector.get(5.into()).unwrap(),
            &TestValue(5.into(), 50)
        );
        assert!(keyed_vector.get(1.into()).is_none());
        assert!(keyed_vector.get(3.into()).is_none());
        assert!(keyed_vector.get(4.into()).is_none());
        keyed_vector.verify();
    }

    #[test]
    #[should_panic(expected = "attempt to build KeyedVector with duplicate keys")]
    fn test_from_vec_duplicate_keys() {
        let values = vec![
            TestValue(0.into(), 10),
            TestValue(2.into(), 20),
            TestValue(5.into(), 50),
            TestValue(2.into(), 30),
        ];
        let keyed_vector: KeyedVector<TestKey, TestValue> = values.clone().into();
        assert!(!keyed_vector.is_empty());
    }

    #[test]
    fn test_from_vec_empty() {
        let values: Vec<TestValue> = vec![];
        let keyed_vector: KeyedVector<TestKey, TestValue> = values.into();
        assert_eq!(keyed_vector.len(), 0);
        assert!(keyed_vector.is_empty());
        keyed_vector.verify();
    }

    #[test]
    fn test_from_vec_with_gaps() {
        let values = vec![
            TestValue(1.into(), 10),
            TestValue(5.into(), 50),
            TestValue(10.into(), 100),
        ];
        let keyed_vector: KeyedVector<TestKey, TestValue> = values.clone().into();

        assert_eq!(keyed_vector.len(), 3);
        assert_eq!(
            keyed_vector.get(1.into()).unwrap(),
            &TestValue(1.into(), 10)
        );
        assert_eq!(
            keyed_vector.get(5.into()).unwrap(),
            &TestValue(5.into(), 50)
        );
        assert_eq!(
            keyed_vector.get(10.into()).unwrap(),
            &TestValue(10.into(), 100)
        );
        assert!(keyed_vector.get(0.into()).is_none());
        assert!(keyed_vector.get(2.into()).is_none());
        assert!(keyed_vector.get(4.into()).is_none());
        assert!(keyed_vector.get(6.into()).is_none());
        assert!(keyed_vector.get(9.into()).is_none());
        keyed_vector.verify();
    }

    #[test]
    fn test_from_vec_and_push() {
        let values = vec![TestValue(1.into(), 10), TestValue(5.into(), 50)];
        let mut keyed_vector: KeyedVector<TestKey, TestValue> = values.into();
        keyed_vector.push(TestValue(3.into(), 30));

        assert_eq!(keyed_vector.len(), 3);
        assert_eq!(
            keyed_vector.get(1.into()).unwrap(),
            &TestValue(1.into(), 10)
        );
        assert_eq!(
            keyed_vector.get(3.into()).unwrap(),
            &TestValue(3.into(), 30)
        );
        assert_eq!(
            keyed_vector.get(5.into()).unwrap(),
            &TestValue(5.into(), 50)
        );
        keyed_vector.verify();
    }

    #[test]
    fn test_from_vec_and_swap_remove() {
        let values = vec![
            TestValue(1.into(), 10),
            TestValue(5.into(), 50),
            TestValue(10.into(), 100),
        ];
        let mut keyed_vector: KeyedVector<TestKey, TestValue> = values.into();
        let removed = keyed_vector.swap_remove(10.into()).unwrap();
        assert_eq!(removed, TestValue(10.into(), 100));
        assert_eq!(keyed_vector.len(), 2);

        let removed = keyed_vector.swap_remove(1.into()).unwrap();

        assert_eq!(removed, TestValue(1.into(), 10));
        assert_eq!(keyed_vector.len(), 1);
        assert_eq!(
            keyed_vector.get(5.into()).unwrap(),
            &TestValue(5.into(), 50)
        );
        assert!(keyed_vector.get(10.into()).is_none());
        keyed_vector.verify();
    }
}
