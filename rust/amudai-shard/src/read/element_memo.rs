use std::{borrow::Borrow, hash::Hash, sync::RwLock};

use ahash::AHashMap;

/// A simple memoization struct using a `RwLock` protected `AHashMap`.
pub struct Memo<K, V>(RwLock<AHashMap<K, V>>);

impl<K, V> Memo<K, V> {
    /// Creates a new, empty `Memo`.
    pub fn new() -> Memo<K, V> {
        Memo(Default::default())
    }

    /// Inserts a key-value pair into the memo.
    ///
    /// If the memo already contained a value for this key, the old value is overwritten.
    pub fn put(&self, key: K, value: V)
    where
        K: Hash + Eq,
    {
        self.0.write().expect("write lock").insert(key, value);
    }
}

impl<K, V: Clone> Memo<K, V> {
    /// Retrieves a value from the memo, cloning it if present.
    ///
    /// Returns `Some(V)` if the key is found, otherwise `None`.  The returned value
    /// is a clone of the value stored in the memo.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q> + Hash + Eq,
        Q: ?Sized + Hash + Eq,
    {
        self.0.read().expect("read lock").get(key).cloned()
    }
}
