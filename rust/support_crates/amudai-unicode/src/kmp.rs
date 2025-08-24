use crate::case_conversions::CharCaseMapper;

/// Implementation of Knuth-Morris-Pratt algorithm for sub-text search.
/// https://en.wikipedia.org/wiki/Knuth%E2%80%93Morris%E2%80%93Pratt_algorithm
pub struct KMPSearch {
    pattern: Vec<char>,
    pattern_str: String,
    case_sensitive: bool,
    lps: Vec<usize>,
}

impl KMPSearch {
    pub fn new(case_sensitive: bool) -> Self {
        KMPSearch {
            pattern: Vec::default(),
            pattern_str: String::default(),
            case_sensitive,
            lps: Vec::new(),
        }
    }

    pub fn new_with_pattern(pattern: &str, case_sensitive: bool) -> Self {
        let mut kmp = Self::new(case_sensitive);
        kmp.set_pattern(pattern);
        kmp
    }

    pub fn set_pattern(&mut self, pattern: &str) {
        self.pattern.clear();
        if self.case_sensitive {
            self.pattern.extend(pattern.chars());
        } else {
            self.pattern
                .extend(pattern.chars().map(|ch| ch.to_lowercase_ignore_special()));
        }

        self.pattern_str.clear();
        self.pattern_str.extend(self.pattern.iter());

        self.build_lps_table();
    }

    pub fn get_pattern(&self) -> &str {
        self.pattern_str.as_str()
    }

    pub fn get_pattern_len(&self) -> usize {
        self.pattern.len()
    }

    /// Pre-builds the longest proper suffix table based on the pattern.
    fn build_lps_table(&mut self) {
        if self.pattern.is_empty() {
            self.lps.clear();
            return;
        }

        unsafe {
            let pattern_len = self.pattern.len();
            let mut lps = std::mem::take(&mut self.lps);
            lps.resize(pattern_len, 0); // TODO: resize without re-initializing
            *lps.get_unchecked_mut(0) = 0;

            let mut i = 1usize;
            let mut j = 0usize;
            let pattern = self.pattern.as_slice();
            while i < pattern_len {
                if pattern.get_unchecked(i) == pattern.get_unchecked(j) {
                    *lps.get_unchecked_mut(i) = j + 1;
                    i += 1;
                    j += 1;
                } else if j != 0 {
                    j = *lps.get_unchecked(j - 1);
                } else {
                    *lps.get_unchecked_mut(i) = 0;
                    i += 1;
                }
            }
            let _ = std::mem::replace(&mut self.lps, lps);
        }
    }

    /// Searches for the first pattern apperance in `text`. If pattern is found,
    /// pattern's start/end byte offset is returned.
    pub fn search(&self, text: &str) -> Option<(usize, usize)> {
        let mut result: Option<(usize, usize)> = None;
        self.search_all(text, |start_pos, end_pos| {
            result.replace((start_pos, end_pos));
            true
        });
        result
    }

    /// Searches for all pattern apperances in `text`. The passed collector is
    /// called on every entrance found with pattern's start and end byte offset.
    /// The search continues until the collector signals to stop by returning true,
    /// or until the end of the text.
    pub fn search_all<F>(&self, text: &str, mut collector: F)
    where
        F: FnMut(usize, usize) -> bool,
    {
        if self.pattern.is_empty() {
            collector(0, 0);
            return;
        }
        if self.case_sensitive {
            let chars = text.char_indices().map(|(pos, ch)| (pos, ch, ch));
            self.search_all_in_chars(chars, text, collector)
        } else {
            // Create new iterator that returns lowercase'd variant of the text.
            let lc_chars = text.char_indices().map(|(byte_pos, ch)| {
                let lc_char = ch.to_lowercase_ignore_special();
                (byte_pos, ch, lc_char)
            });
            self.search_all_in_chars(lc_chars, text, collector)
        }
    }

    /// Search for pattern matches in a sequence of characters.
    /// Chars iterator returns tuple containing:
    ///   - Original byte offset in unchanged text.
    ///   - Original character of unchanged text.
    ///   - Case mapped character, if case-insensitive search is invoked.
    ///
    /// The collector is a function that accepts two arguments:
    ///   - Start byte position of the found pattern.
    ///   - End byte position of the found pattern.
    fn search_all_in_chars<C, F>(&self, mut chars: C, orig_text: &str, mut collector: F)
    where
        C: Iterator<Item = (usize, char, char)>,
        F: FnMut(usize, usize) -> bool,
    {
        let mut next = chars.next();
        let mut i = 0usize;
        while let Some((byte_pos, orig_ch, mapped_ch)) = next {
            unsafe {
                if *self.pattern.get_unchecked(i) != mapped_ch {
                    if i == 0 {
                        next = chars.next();
                    } else {
                        i = *self.lps.get_unchecked(i - 1);
                    }
                } else {
                    i += 1;
                    let pattern_len = self.pattern.len();
                    if i == pattern_len {
                        // If the text is "STRAẞE", and the pattern is "raß" ('ß' is lowercase):
                        //   - byte_pos points to "STRA*ẞE"
                        //   - orig_char is 'ẞ' (size=3)
                        //   - mapped_ch is 'ß' (size=2)

                        // Size of matched text without the last character (in bytes).
                        let matched_text_till_pos_size = &orig_text[0..byte_pos]
                            .chars()
                            .rev()
                            .take(pattern_len - 1)
                            .map(|ch| ch.len_utf8())
                            .sum::<usize>();
                        let start_pos = byte_pos - matched_text_till_pos_size;
                        let end_pos = byte_pos + orig_ch.len_utf8();
                        if collector(start_pos, end_pos) {
                            return;
                        }
                        i = *self.lps.get_unchecked(i - 1);
                    }
                    next = chars.next();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kmp_search() {
        let mut kmp = KMPSearch::new_with_pattern("abc", true);
        assert_eq!(Some((4, 7)), kmp.search("abababcabab"));
        assert_eq!(None, kmp.search("ababd"));
        assert_eq!(None, kmp.search("abCdef"));
        kmp.set_pattern("שלום");
        assert_eq!(Some((0, 8)), kmp.search("שלום לכם"));
        assert_eq!(Some((9, 17)), kmp.search("הסכם שלום"));

        let mut kmp = KMPSearch::new_with_pattern("Abc", false);
        assert_eq!(Some((4, 7)), kmp.search("abababcabab"));
        assert_eq!(None, kmp.search("abAbd"));
        kmp.set_pattern("Trüben");
        assert_eq!(
            Some((25, 32)),
            kmp.search("Die früh sich einst dem trüben Blick gezeigt.")
        );

        let mut kmp = KMPSearch::new(true);
        assert_eq!(Some((0, 0)), kmp.search("any text"));
        kmp.set_pattern("👌");
        assert_eq!(Some((18, 22)), kmp.search("все будет 👌"));
    }

    #[test]
    fn test_kmp_search_all() {
        for cs in [false, true] {
            let mut results = Vec::new();
            let mut kmp = KMPSearch::new(cs);
            kmp.search_all("any text", |start, end| {
                results.push((start, end));
                false
            });
            assert_eq!(vec![(0, 0)], results);

            kmp.set_pattern("abc");
            let text = "abcabc.abc.abc";
            results.clear();
            kmp.search_all(text, |start, end| {
                results.push((start, end));
                false
            });
            assert_eq!(vec![(0, 3), (3, 6), (7, 10), (11, 14)], results);

            let mut kmp = KMPSearch::new(cs);
            kmp.set_pattern("🐓");
            let text = "🐓  🐥  🐥🐓🐥🐥  🐥🐓";
            results.clear();
            kmp.search_all(text, |start, end| {
                results.push((start, end));
                false
            });
            assert_eq!(vec![(0, 4), (16, 20), (34, 38)], results);
        }

        for pattern in ["ß", "ẞ"] {
            let mut results = Vec::new();
            let mut kmp = KMPSearch::new(false);
            kmp.set_pattern(pattern);
            let text = "ẞamm-ßamm";
            results.clear();
            kmp.search_all(text, |start, end| {
                results.push((start, end));
                false
            });
            assert_eq!(vec![(0, 3), (7, 9)], results);
        }
    }
}
