use std::cmp::Ordering;

use amudai_collections::identity_hash::IdentityHashMap128;
use amudai_format::schema::{BasicType, BasicTypeDescriptor};
use amudai_sequence::{sequence::ValueSequence, values::Values};

/// A bidirectional mapping between terms (arbitrary byte sequences) and unique integer identifiers.
///
/// `TermDictionary` maintains a two-way mapping of terms to integers in the range [0, N),
/// where N is the current number of distinct terms in the dictionary. Each term is assigned
/// a unique ID when first encountered, and subsequent lookups return the same ID.
///
/// The dictionary uses xxHash for efficient term lookup and stores terms in insertion order.
/// It supports a special NULL term that can be mapped separately from regular byte sequences.
///
/// # Key Operations
/// - `map_term`: Insert a new term or retrieve the ID of an existing term
/// - `term_by_id`: Retrieve a term by its assigned ID
/// - `sorted_order`: Get term IDs sorted by a custom comparison function
///
/// # Limitations
/// - Term removal is not supported
/// - Maximum of 2^32 - 1 distinct terms
/// - Hash collisions are not handled (assumes xxHash produces unique hashes for distinct terms)
pub struct TermDictionary {
    /// Hash-to-ID mapping for fast term lookup. Uses xxHash for term hashing.
    dict: IdentityHashMap128<u128, u32>,
    /// Sequential storage of term values, indexed by term ID.
    terms: ValueSequence,
    /// ID of the special NULL term, if one has been mapped.
    null_id: Option<u32>,
}

impl Default for TermDictionary {
    fn default() -> Self {
        Self::new()
    }
}

impl TermDictionary {
    /// Creates a new, empty term dictionary.
    ///
    /// The dictionary starts with no terms and no NULL term mapped.
    /// Internal storage is initialized with minimal capacity.
    pub fn new() -> Self {
        TermDictionary {
            dict: IdentityHashMap128::default(),
            terms: ValueSequence::empty(BasicTypeDescriptor {
                basic_type: BasicType::Binary,
                ..Default::default()
            }),
            null_id: None,
        }
    }

    /// Returns the number of distinct terms currently stored in the dictionary.
    ///
    /// This count includes the NULL term if one has been mapped via [`map_null`].
    ///
    /// [`map_null`]: TermDictionary::map_null
    pub fn len(&self) -> usize {
        self.terms.len()
    }

    /// Returns the total number of bytes used to store all terms in the internal buffer.
    ///
    /// This represents the cumulative size of all term data, not including metadata
    /// or hash table overhead. Empty terms contribute 0 bytes to this total.
    pub fn buf_size(&self) -> usize {
        self.terms.values.bytes_len()
    }

    /// Maps a term to its unique identifier, creating a new mapping if the term is not present.
    ///
    /// This is the primary method for interacting with the dictionary. If the term already
    /// exists, its existing ID is returned. If the term is new, it's assigned the next
    /// available ID (equal to the current dictionary size) and stored.
    ///
    /// # Arguments
    /// * `term` - A byte slice representing the term to map
    ///
    /// # Returns
    /// The unique ID assigned to this term (0-based, sequential)
    pub fn map_term(&mut self, term: &[u8]) -> u32 {
        let hash = xxhash_rust::xxh3::xxh3_64(term) as u128;
        if let Some(&id) = self.dict.get(&hash) {
            id
        } else {
            let id = self.terms.len() as u32;
            self.dict.insert(hash, id);
            self.terms.push_binary(term);
            id
        }
    }

    /// Checks whether a specific term exists in the dictionary.
    ///
    /// This method performs a hash lookup to determine if the term has been previously
    /// mapped, without modifying the dictionary or returning the term's ID.
    ///
    /// # Arguments
    /// * `term` - The byte slice to search for
    ///
    /// # Returns
    /// `true` if the term exists in the dictionary, `false` otherwise
    #[allow(unused)]
    pub fn contains_term(&self, term: &[u8]) -> bool {
        let hash = xxhash_rust::xxh3::xxh3_64(term) as u128;
        self.dict.contains_key(&hash)
    }

    /// Checks whether a specific term ID is valid for this dictionary.
    ///
    /// This verifies that the given ID is within the valid range [0, len()).
    /// It does not verify that the ID corresponds to a non-NULL term.
    ///
    /// # Arguments
    /// * `id` - The term ID to validate
    ///
    /// # Returns
    /// `true` if the ID is valid, `false` otherwise
    #[allow(unused)]
    pub fn contains_id(&self, id: u32) -> bool {
        (id as usize) < self.terms.len()
    }

    /// Checks whether the dictionary contains a NULL term.
    ///
    /// The NULL term is a special entry that can be mapped separately from
    /// regular byte sequences using [`map_null`].
    ///
    /// # Returns
    /// `true` if a NULL term has been mapped, `false` otherwise
    ///
    /// [`map_null`]: TermDictionary::map_null
    #[allow(unused)]
    pub fn contains_null(&self) -> bool {
        self.null_id.is_some()
    }

    /// Retrieves the term associated with the given ID.
    ///
    /// This method provides the reverse mapping from ID to term. The returned
    /// byte slice is borrowed from the internal storage and remains valid as
    /// long as the dictionary exists and is not cleared.
    ///
    /// # Arguments
    /// * `id` - The term ID to look up
    ///
    /// # Returns
    /// A byte slice containing the term data
    ///
    /// # Panics
    /// Panics if the term ID is out of bounds (>= dictionary length).
    /// Use [`contains_id`] to check validity before calling this method.
    ///
    /// [`contains_id`]: TermDictionary::contains_id
    pub fn term_by_id(&self, id: u32) -> &[u8] {
        assert!(
            id < self.terms.len() as u32,
            "term id {id} is out of bounds"
        );
        self.terms.var_binary_at(id as usize)
    }

    /// Maps the special NULL term and returns its ID.
    ///
    /// The NULL term is distinct from any byte sequence and can only be created
    /// through this method. If a NULL term has already been mapped, this returns
    /// the existing ID rather than creating a duplicate.
    ///
    /// # Returns
    /// The unique ID assigned to the NULL term
    #[allow(unused)]
    pub fn map_null(&mut self) -> u32 {
        if let Some(id) = self.null_id {
            id
        } else {
            let id = self.terms.len() as u32;
            self.terms.push_null();
            self.null_id = Some(id);
            id
        }
    }

    /// Returns the ID of the NULL term, if one has been mapped.
    /// # Returns
    ///
    /// `Some(id)` if a NULL term exists, `None` otherwise
    #[allow(unused)]
    pub fn null_id(&self) -> Option<u32> {
        self.null_id
    }

    /// Checks whether the given ID corresponds to the NULL term.
    ///
    /// # Arguments
    /// * `id` - The term ID to check
    ///
    /// # Returns
    /// `true` if this ID represents the NULL term, `false` otherwise
    #[allow(unused)]
    pub fn is_null(&self, id: u32) -> bool {
        self.null_id == Some(id)
    }

    /// Returns a reference to the internal value storage containing all terms.
    ///
    /// The terms are stored in the order they were first mapped (by ID).
    /// This provides direct access to the underlying storage for advanced use cases.
    #[allow(unused)]
    pub fn terms(&self) -> &Values {
        &self.terms.values
    }

    /// Constructs a list of term IDs sorted according to the provided comparison function.
    ///
    /// This method creates a vector of indices representing the sorted order of terms
    /// without modifying the original dictionary. The comparison function is applied
    /// to the actual term byte data.
    ///
    /// Given terms `["cc", "dd", "bb", "aa"]` with IDs `[0, 1, 2, 3]`,
    /// lexicographic sorting would return `[3, 2, 0, 1]`, meaning:
    /// - Position 0: ID 3 ("aa")  
    /// - Position 1: ID 2 ("bb")
    /// - Position 2: ID 0 ("cc")
    /// - Position 3: ID 1 ("dd")
    ///
    /// # Arguments  
    /// * `compare` - A comparison function that takes two byte slices and returns an `Ordering`
    ///
    /// # Returns
    /// A vector of term IDs in the order specified by the comparison function
    pub fn sorted_order<F>(&self, mut compare: F) -> Vec<usize>
    where
        F: FnMut(&[u8], &[u8]) -> Ordering,
    {
        let mut indices: Vec<usize> = (0..self.terms.len()).collect();
        indices.sort_by(|&a, &b| {
            let term_a = self.terms.var_binary_at(a);
            let term_b = self.terms.var_binary_at(b);
            compare(term_a, term_b)
        });
        indices
    }

    /// Clears all terms from the dictionary while retaining allocated capacity.
    ///
    /// After calling this method, the dictionary will be empty but internal
    /// buffers may retain their capacity for efficient reuse. Both regular
    /// terms and the NULL term (if present) are removed.
    pub fn clear(&mut self) {
        self.dict.clear();
        self.terms.clear();
        self.null_id = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_dictionary() {
        let dict = TermDictionary::new();
        assert_eq!(dict.len(), 0);
        assert_eq!(dict.buf_size(), 0);
        assert!(!dict.contains_null());
        assert_eq!(dict.null_id(), None);
    }

    #[test]
    fn test_map_term() {
        let mut dict = TermDictionary::new();

        let id1 = dict.map_term(b"hello");
        let id2 = dict.map_term(b"world");
        let id3 = dict.map_term(b"hello"); // Should return same id as first

        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id3, 0); // Same as first "hello"
        assert_eq!(dict.len(), 2);
    }

    #[test]
    fn test_contains_term() {
        let mut dict = TermDictionary::new();

        assert!(!dict.contains_term(b"hello"));
        dict.map_term(b"hello");
        assert!(dict.contains_term(b"hello"));
        assert!(!dict.contains_term(b"world"));
    }

    #[test]
    fn test_contains_id() {
        let mut dict = TermDictionary::new();

        assert!(!dict.contains_id(0));
        dict.map_term(b"hello");
        assert!(dict.contains_id(0));
        assert!(!dict.contains_id(1));
    }

    #[test]
    fn test_term_by_id() {
        let mut dict = TermDictionary::new();
        let id1 = dict.map_term(b"hello");
        let id2 = dict.map_term(b"world");
        assert_eq!(dict.term_by_id(id1), b"hello");
        assert_eq!(dict.term_by_id(id2), b"world");

        let mut dict = TermDictionary::new();
        assert_eq!(dict.map_term(b"aaa"), 0);
        assert_eq!(dict.map_term(b"bbb"), 1);
        assert_eq!(dict.map_term(b"aaa"), 0);
        assert_eq!(dict.map_term(b""), 2);
        assert_eq!(dict.map_term(b"ccc"), 3);
        assert_eq!(dict.map_term(b""), 2);
        assert_eq!(dict.len(), 4);
        assert_eq!(dict.term_by_id(0), b"aaa");
        assert_eq!(dict.term_by_id(1), b"bbb");
        assert_eq!(dict.term_by_id(2), b"");
        assert_eq!(dict.term_by_id(3), b"ccc");
        assert_eq!(dict.terms().as_bytes(), b"aaabbbccc");
    }

    #[test]
    #[should_panic(expected = "term id 0 is out of bounds")]
    fn test_term_by_id_panic() {
        let dict = TermDictionary::new();
        dict.term_by_id(0);
    }

    #[test]
    fn test_map_null() {
        let mut dict = TermDictionary::new();

        assert!(!dict.contains_null());
        let null_id1 = dict.map_null();
        assert!(dict.contains_null());
        assert_eq!(dict.null_id(), Some(null_id1));

        let null_id2 = dict.map_null(); // Should return same id
        assert_eq!(null_id1, null_id2);
        assert_eq!(dict.len(), 1); // Only one null entry
    }

    #[test]
    fn test_is_null() {
        let mut dict = TermDictionary::new();

        let term_id = dict.map_term(b"hello");
        let null_id = dict.map_null();

        assert!(!dict.is_null(term_id));
        assert!(dict.is_null(null_id));
    }

    #[test]
    fn test_sorted_order() {
        let mut dict = TermDictionary::new();

        dict.map_term(b"cc");
        dict.map_term(b"dd");
        dict.map_term(b"bb");
        dict.map_term(b"aa");

        let sorted = dict.sorted_order(|a, b| a.cmp(b));
        assert_eq!(sorted, vec![3, 2, 0, 1]); // aa, bb, cc, dd
    }

    #[test]
    fn test_clear() {
        let mut dict = TermDictionary::new();

        dict.map_term(b"hello");
        dict.map_term(b"world");
        dict.map_null();

        assert_eq!(dict.len(), 3);
        assert!(dict.contains_null());

        dict.clear();

        assert_eq!(dict.len(), 0);
        assert_eq!(dict.buf_size(), 0);
        assert!(!dict.contains_null());
        assert_eq!(dict.null_id(), None);
    }
}
