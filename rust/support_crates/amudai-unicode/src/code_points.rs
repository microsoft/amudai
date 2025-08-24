use std::cmp;

#[inline]
fn utf8_char_width_relaxed(first_byte: u8) -> usize {
    if first_byte < 128 {
        1
    } else if (first_byte & 0xe0) == 0xc0 {
        2
    } else if (first_byte & 0xf0) == 0xe0 {
        3
    } else if (first_byte & 0xf8) == 0xf0 {
        4
    } else {
        1
    }
}

/// NOTE: `ptr` must point after at least one UTF-8 character - the method
/// doesn't validate that.
#[inline]
fn utf8_prev_char_width_relaxed(ptr: *const u8) -> usize {
    unsafe {
        let b = *ptr.sub(1);
        if b < 128 {
            return 1;
        }

        let b = *ptr.sub(2);
        if (b as i8) >= -64 {
            return 2;
        }

        let b = *ptr.sub(3);
        if (b as i8) >= -64 {
            return 3;
        }

        4
    }
}

#[derive(Clone)]
pub struct UnverifiedPosIter {
    pos: usize,
    end: usize,
    ptr: *const u8,
}

impl Iterator for UnverifiedPosIter {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        let pos = self.pos;
        if pos >= self.end {
            return None;
        }

        let first_byte = unsafe { *self.ptr.add(pos) };
        self.pos = pos + utf8_char_width_relaxed(first_byte);
        Some(pos)
    }
}

impl DoubleEndedIterator for UnverifiedPosIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        let mut end = self.end;
        if end <= self.pos {
            return None;
        }

        unsafe {
            let ptr = self.ptr.add(end);
            end -= utf8_prev_char_width_relaxed(ptr);
        }
        self.end = end;
        Some(end)
    }
}

#[derive(Clone)]
pub struct UnverifiedRangeIter<'a> {
    curr: usize,
    next: usize,
    s: &'a [u8],
}

impl<'a> UnverifiedRangeIter<'a> {
    #[inline]
    pub fn current_slice(&self) -> &[u8] {
        // Our `curr` and `next` are "curated": they're always within `0..s.len()`
        // (inclusive) range.
        unsafe { self.s.get_unchecked(self.curr..self.next) }
    }

    pub fn current_range(&self) -> (usize, usize) {
        (self.curr, self.next)
    }

    pub fn current_pos(&self) -> usize {
        self.curr
    }
}

impl<'a> Iterator for UnverifiedRangeIter<'a> {
    type Item = (usize, usize);

    #[inline]
    fn next(&mut self) -> Option<(usize, usize)> {
        // `self.next` starts from 0 and goes up to `self.s.len()`, as guaranteed
        // by the `min()` computation below.
        let curr = self.next;
        debug_assert!(curr <= self.s.len());

        if curr == self.s.len() {
            self.curr = curr;
            return None;
        }

        // We've just verified that curr is below `self.s.len()`.
        let first_byte = unsafe { *self.s.get_unchecked(curr) };
        let w = utf8_char_width_relaxed(first_byte);
        let next = cmp::min(curr + w, self.s.len());
        self.curr = curr;
        self.next = next;
        Some((curr, next))
    }
}

pub trait UnverifiedPosUtils {
    fn unverified_positions(&self) -> UnverifiedPosIter;

    fn unverified_ranges(&self) -> UnverifiedRangeIter;

    /// Counts number of characters in the UTF-8 string.
    fn unverified_len(&self) -> usize;

    /// Returns longest prefix of unverified UTF-8 string `s` with at most `max_size` bytes in it.
    fn unverified_truncate(&self, max_size: usize) -> &Self;
}

impl UnverifiedPosUtils for [u8] {
    fn unverified_positions(&self) -> UnverifiedPosIter {
        UnverifiedPosIter {
            pos: 0,
            end: self.len(),
            ptr: self.as_ptr(),
        }
    }

    fn unverified_ranges(&self) -> UnverifiedRangeIter {
        UnverifiedRangeIter {
            curr: 0,
            next: 0,
            s: self,
        }
    }

    fn unverified_len(&self) -> usize {
        self.unverified_positions().count()
    }

    #[inline]
    fn unverified_truncate(&self, max_size: usize) -> &Self {
        let last_pos = self
            .unverified_positions()
            .take_while(|pos| *pos <= max_size)
            .last()
            .unwrap_or(0);
        &self[0..last_pos]
    }
}

impl UnverifiedPosUtils for str {
    fn unverified_positions(&self) -> UnverifiedPosIter {
        self.as_bytes().unverified_positions()
    }

    fn unverified_ranges(&self) -> UnverifiedRangeIter {
        self.as_bytes().unverified_ranges()
    }

    fn unverified_len(&self) -> usize {
        self.as_bytes().unverified_len()
    }

    fn unverified_truncate(&self, max_size: usize) -> &Self {
        let b = self.as_bytes().unverified_truncate(max_size);
        unsafe { std::str::from_utf8_unchecked(b) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ranges_iter() {
        assert_eq!(
            b"ab".unverified_ranges().collect::<Vec<_>>(),
            &[(0, 1), (1, 2)]
        );
        assert_eq!(
            b"a\xd7\x90".unverified_ranges().collect::<Vec<_>>(),
            &[(0, 1), (1, 3)]
        );
        assert_eq!(
            b"a\xd7".unverified_ranges().collect::<Vec<_>>(),
            &[(0, 1), (1, 2)]
        );

        let s = b"\xD0\xB5\xD0\xBD\xD0\xBE\xD1\x82";
        let mut it = s.unverified_ranges().skip(1).take(2);
        let a = it.next().unwrap_or((0, 0)).0;
        let b = it.last().unwrap_or((0, 0)).1;
        assert_eq!(a..b, 2..6);

        let mut it = s.unverified_ranges().take(5);
        let a = it.next().map(|r| r.0).unwrap_or(0);
        let b = it.last().map(|r| r.1).unwrap_or(0);
        assert_eq!(a..b, 0..8);
    }

    #[test]
    fn test_pos_iter() {
        assert_eq!(b"a".unverified_positions().collect::<Vec<_>>(), &[0]);
        assert_eq!(b"a".unverified_positions().rev().collect::<Vec<_>>(), &[0]);
        assert_eq!(b"".unverified_positions().collect::<Vec<_>>(), &[0usize; 0]);
        assert_eq!(
            b"".unverified_positions().rev().collect::<Vec<_>>(),
            &[0usize; 0]
        );
        assert_eq!(b"ab".unverified_positions().collect::<Vec<_>>(), &[0, 1]);
        assert_eq!(
            b"ab".unverified_positions().rev().collect::<Vec<_>>(),
            &[1, 0]
        );
        assert_eq!(
            b"a\xd7\x90".unverified_positions().collect::<Vec<_>>(),
            &[0, 1]
        );
        assert_eq!(
            b"a\xd7\x90"
                .unverified_positions()
                .rev()
                .collect::<Vec<_>>(),
            &[1, 0]
        );
        assert_eq!(
            b"a\xd7\x90b".unverified_positions().collect::<Vec<_>>(),
            &[0, 1, 3]
        );
        assert_eq!(
            b"a\xd7\x90b"
                .unverified_positions()
                .rev()
                .collect::<Vec<_>>(),
            &[3, 1, 0]
        );
        // Invalid UTF-8
        assert_eq!(b"a\xd7".unverified_positions().collect::<Vec<_>>(), &[0, 1]);
        assert_eq!(
            b"a\xd7".unverified_positions().rev().collect::<Vec<_>>(),
            &[0]
        );
    }

    #[test]
    fn test_truncate() {
        assert_eq!(b"".unverified_truncate(10), b"");
        assert_eq!(b"abc".unverified_truncate(0), b"");
        assert_eq!(b"a\xe4\xb9\x97\xe7\x90\x86".unverified_truncate(1), b"a");
        assert_eq!(b"a\xe4\xb9\x97\xe7\x90\x86".unverified_truncate(3), b"a");
        assert_eq!(
            b"a\xe4\xb9\x97\xe7\x90\x86".unverified_truncate(5),
            b"a\xe4\xb9\x97"
        );
    }

    #[test]
    fn test_unverified_len() {
        assert_eq!(4, "אבגד".unverified_len());
        assert_eq!(6, "Straẞe".unverified_len());
        assert_eq!(6, "Straße".unverified_len());
    }
}
