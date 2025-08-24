use unicode_segmentation::GraphemeCursor;

/// Calculates bytes offsets of previous/next grapheme clusters around byte offset
/// position `pos` in string `s`.
///
/// Of no previous/next grapheme cluster exists, the function returns 0 or the
/// string length respectively.
///
/// Assumption: `pos` must be a proper Unicode boundary position.
pub fn utf8_grapheme_prev_and_next_pos(s: &str, pos: usize) -> (usize, usize) {
    let len = s.len();
    let mut cursor = GraphemeCursor::new(pos, len, true);
    let prev = if pos == 0 {
        0
    } else {
        cursor
            .prev_boundary(s, 0)
            .ok()
            .flatten()
            .expect("prev boundary")
    };
    let next = if pos >= len {
        len
    } else {
        cursor.set_cursor(pos);
        cursor
            .next_boundary(s, 0)
            .ok()
            .flatten()
            .expect("next boundary")
    };
    (prev, next)
}

/// Calculates bytes offset of the next grapheme cluster after around byte offset
/// position `pos` in string `s`. If no next grapheme cluster exists, the function
/// returns the string length.
///
/// Assumption: `pos` must be a proper Unicode boundary position.
pub fn utf8_grapheme_next_pos(s: &str, pos: usize) -> usize {
    let len = s.len();
    let mut cursor = GraphemeCursor::new(pos, len, true);
    if pos >= len {
        len
    } else {
        cursor
            .next_boundary(s, 0)
            .ok()
            .flatten()
            .expect("next boundary")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utf8_grapheme_prev_and_next_pos() {
        assert_eq!((0, 0), utf8_grapheme_prev_and_next_pos("", 0));
        assert_eq!((0, 1), utf8_grapheme_prev_and_next_pos("abcde", 0));
        assert_eq!((4, 5), utf8_grapheme_prev_and_next_pos("abcde", 5));
        assert_eq!((4, 8), utf8_grapheme_prev_and_next_pos("אבגדה", 6));
        assert_eq!(
            (12, 13 + "Ç".len()),
            utf8_grapheme_prev_and_next_pos("Greek letter Çedilla", 13)
        );
    }

    #[test]
    fn test_utf8_grapheme_next_pos() {
        assert_eq!(0, utf8_grapheme_next_pos("", 0));
        assert_eq!(1, utf8_grapheme_next_pos("abcde", 0));
        assert_eq!(5, utf8_grapheme_next_pos("abcde", 5));
        assert_eq!(8, utf8_grapheme_next_pos("אבגדה", 6));
        assert_eq!(
            13 + "Ç".len(),
            utf8_grapheme_next_pos("Greek letter Çedilla", 13)
        );
    }
}
