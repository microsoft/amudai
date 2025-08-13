use itertools::Itertools;

use crate::bit_array::{BigBitArray, BitArray, BitArrayBase};

#[test]
fn test_bit_array_iter() {
    // Test with empty array
    let empty = BitArray::empty(10);
    let positions: Vec<usize> = empty.iter().collect();
    assert_eq!(positions, Vec::<usize>::new());

    // Test with single bit set
    let mut single = BitArray::empty(10);
    single.set(5);
    let positions: Vec<usize> = single.iter().collect();
    assert_eq!(positions, vec![5]);

    // Test with multiple bits in single word
    let mut multi = BitArray::empty(20);
    multi.set(0);
    multi.set(3);
    multi.set(7);
    multi.set(11);
    multi.set(19);
    let positions: Vec<usize> = multi.iter().collect();
    assert_eq!(positions, vec![0, 3, 7, 11, 19]);

    // Test across word boundaries
    let mut cross_word = BitArray::empty(150);
    cross_word.set(0); // First word
    cross_word.set(63); // Last bit of first word
    cross_word.set(64); // First bit of second word
    cross_word.set(65); // Second bit of second word
    cross_word.set(127); // Last bit of second word
    cross_word.set(128); // First bit of third word
    cross_word.set(149); // Last bit
    let positions: Vec<usize> = cross_word.iter().collect();
    assert_eq!(positions, vec![0, 63, 64, 65, 127, 128, 149]);

    // Test with all bits set
    let mut all_set = BitArray::empty(8);
    all_set.set_all();
    let positions: Vec<usize> = all_set.iter().collect();
    assert_eq!(positions, vec![0, 1, 2, 3, 4, 5, 6, 7]);

    // Test with consecutive bits
    let mut consecutive = BitArray::empty(10);
    consecutive.set(3);
    consecutive.set(4);
    consecutive.set(5);
    consecutive.set(6);
    let positions: Vec<usize> = consecutive.iter().collect();
    assert_eq!(positions, vec![3, 4, 5, 6]);

    // Test iterator multiple passes
    let mut reuse = BitArray::empty(10);
    reuse.set(2);
    reuse.set(5);
    reuse.set(8);

    let first_pass: Vec<usize> = reuse.iter().collect();
    let second_pass: Vec<usize> = reuse.iter().collect();
    assert_eq!(first_pass, vec![2, 5, 8]);
    assert_eq!(second_pass, vec![2, 5, 8]);

    // Test with pattern at word boundaries
    let mut boundary = BitArray::empty(130);
    boundary.set(62);
    boundary.set(63);
    boundary.set(64);
    boundary.set(65);
    let positions: Vec<usize> = boundary.iter().collect();
    assert_eq!(positions, vec![62, 63, 64, 65]);

    // Test iterator can be used in for loop
    let mut for_loop_test = BitArray::empty(10);
    for_loop_test.set(1);
    for_loop_test.set(4);
    for_loop_test.set(7);

    let mut collected = Vec::new();
    for pos in for_loop_test.iter() {
        collected.push(pos);
    }
    assert_eq!(collected, vec![1, 4, 7]);

    // Test with exactly 64 bits (one full word)
    let mut full_word = BitArray::empty(64);
    full_word.set(0);
    full_word.set(31);
    full_word.set(32);
    full_word.set(63);
    let positions: Vec<usize> = full_word.iter().collect();
    assert_eq!(positions, vec![0, 31, 32, 63]);

    // Test that iterator respects bit array length
    let words = [0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF];
    let partial = BitArray::from_lsb_words(&words, 70); // Only 70 bits, not 128
    let positions: Vec<usize> = partial.iter().collect();
    assert_eq!(positions.len(), 70);
    assert_eq!(*positions.last().unwrap(), 69);
}

#[test]
fn test_bit_operations() {
    // Test with a small bit array first
    let mut bit_array = BitArray::empty(10);

    // Test initial state - all bits should be 0
    for i in 0..10 {
        assert!(
            !bit_array.contains(i),
            "Bit {} should be unset initially",
            i
        );
    }

    // Test set() method
    bit_array.set(3);
    bit_array.set(7);
    bit_array.set(9);

    assert!(bit_array.contains(3), "Bit 3 should be set");
    assert!(bit_array.contains(7), "Bit 7 should be set");
    assert!(bit_array.contains(9), "Bit 9 should be set");

    // Verify other bits remain unset
    assert!(!bit_array.contains(0), "Bit 0 should remain unset");
    assert!(!bit_array.contains(1), "Bit 1 should remain unset");
    assert!(!bit_array.contains(2), "Bit 2 should remain unset");
    assert!(!bit_array.contains(4), "Bit 4 should remain unset");
    assert!(!bit_array.contains(5), "Bit 5 should remain unset");
    assert!(!bit_array.contains(6), "Bit 6 should remain unset");
    assert!(!bit_array.contains(8), "Bit 8 should remain unset");

    // Test reset() method
    bit_array.reset(7);
    assert!(!bit_array.contains(7), "Bit 7 should be reset");
    assert!(bit_array.contains(3), "Bit 3 should still be set");
    assert!(bit_array.contains(9), "Bit 9 should still be set");

    // Test set_value() method
    bit_array.set_value(1, true);
    bit_array.set_value(5, true);
    bit_array.set_value(3, false); // Reset bit 3

    assert!(
        bit_array.contains(1),
        "Bit 1 should be set via set_value(true)"
    );
    assert!(
        bit_array.contains(5),
        "Bit 5 should be set via set_value(true)"
    );
    assert!(
        !bit_array.contains(3),
        "Bit 3 should be reset via set_value(false)"
    );
    assert!(bit_array.contains(9), "Bit 9 should still be set");

    // Test set_all() method
    bit_array.set_all();
    for i in 0..10 {
        assert!(
            bit_array.contains(i),
            "Bit {} should be set after set_all()",
            i
        );
    }

    // Test clear() method
    bit_array.clear();
    for i in 0..10 {
        assert!(
            !bit_array.contains(i),
            "Bit {} should be unset after clear()",
            i
        );
    }
}

#[test]
fn test_bit_operations_across_word_boundaries() {
    // Test with a larger bit array that spans multiple u64 words
    let mut bit_array = BitArray::empty(150);

    // Test operations on bits in different u64 words
    let test_indices = [0, 31, 32, 63, 64, 65, 127, 128, 149];

    // Set specific bits
    for &index in &test_indices {
        bit_array.set(index);
    }

    // Verify they're set
    for &index in &test_indices {
        assert!(bit_array.contains(index), "Bit {} should be set", index);
    }

    // Test reset on some bits
    bit_array.reset(32);
    bit_array.reset(128);

    assert!(!bit_array.contains(32), "Bit 32 should be reset");
    assert!(!bit_array.contains(128), "Bit 128 should be reset");

    // Verify other bits are still set
    for &index in &[0, 31, 63, 64, 65, 127, 149] {
        assert!(
            bit_array.contains(index),
            "Bit {} should still be set",
            index
        );
    }

    // Test set_value with mixed operations
    bit_array.set_value(32, true); // Set back
    bit_array.set_value(128, true); // Set back
    bit_array.set_value(0, false); // Reset
    bit_array.set_value(149, false); // Reset

    assert!(bit_array.contains(32), "Bit 32 should be set via set_value");
    assert!(
        bit_array.contains(128),
        "Bit 128 should be set via set_value"
    );
    assert!(
        !bit_array.contains(0),
        "Bit 0 should be reset via set_value"
    );
    assert!(
        !bit_array.contains(149),
        "Bit 149 should be reset via set_value"
    );

    // Test set_all and verify it works across all words
    bit_array.set_all();
    for i in 0..150 {
        assert!(
            bit_array.contains(i),
            "Bit {} should be set after set_all()",
            i
        );
    }

    // Test clear and verify it works across all words
    bit_array.clear();
    for i in 0..150 {
        assert!(
            !bit_array.contains(i),
            "Bit {} should be unset after clear()",
            i
        );
    }
}

#[test]
fn test_edge_cases() {
    // Test with single bit
    let mut single_bit = BitArray::empty(1);
    assert!(!single_bit.contains(0));

    single_bit.set(0);
    assert!(single_bit.contains(0));

    single_bit.reset(0);
    assert!(!single_bit.contains(0));

    single_bit.set_value(0, true);
    assert!(single_bit.contains(0));

    single_bit.set_value(0, false);
    assert!(!single_bit.contains(0));

    single_bit.set_all();
    assert!(single_bit.contains(0));

    single_bit.clear();
    assert!(!single_bit.contains(0));

    // Test with exactly 64 bits (one full word)
    let mut full_word = BitArray::empty(64);

    // Set first and last bits
    full_word.set(0);
    full_word.set(63);

    assert!(full_word.contains(0));
    assert!(full_word.contains(63));
    assert!(!full_word.contains(32));

    full_word.set_all();
    for i in 0..64 {
        assert!(full_word.contains(i));
    }

    full_word.clear();
    for i in 0..64 {
        assert!(!full_word.contains(i));
    }
}

#[test]
fn test_mask_tail() {
    let mut bits = vec![];
    BitArray::mask_tail(&mut bits, 0);

    let mut bits = vec![0xFFFFFFFFFFFFFFFF];
    BitArray::mask_tail(&mut bits, 64);
    assert_eq!(bits, vec![0xFFFFFFFFFFFFFFFF]); // Should remain unchanged

    let mut bits = vec![0xFFFFFFFFFFFFFFFF];
    BitArray::mask_tail(&mut bits, 63);
    assert_eq!(bits, vec![0x7FFFFFFFFFFFFFFF]); // Top bit masked out

    let mut bits = vec![0xFFFFFFFFFFFFFFFF];
    BitArray::mask_tail(&mut bits, 8);
    assert_eq!(bits, vec![0xFF]); // Only bottom 8 bits remain

    let mut bits = vec![0xFFFFFFFFFFFFFFFF];
    BitArray::mask_tail(&mut bits, 1);
    assert_eq!(bits, vec![0x1]); // Only bottom bit remains

    let mut bits = vec![0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF];
    BitArray::mask_tail(&mut bits, 72); // 64 + 8 bits
    assert_eq!(bits, vec![0xFFFFFFFFFFFFFFFF, 0xFF]);

    let mut bits = vec![0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF];
    BitArray::mask_tail(&mut bits, 65); // 64 + 1 bits
    assert_eq!(bits, vec![0xFFFFFFFFFFFFFFFF, 0x1]);

    let mut bits = vec![0x123456789ABCDEF0];
    BitArray::mask_tail(&mut bits, 32); // Keep only bottom 32 bits
    assert_eq!(bits, vec![0x9ABCDEF0]);

    let mut bits = vec![0xDEADBEEFCAFEBABE];
    BitArray::mask_tail(&mut bits, 32);
    assert_eq!(bits, vec![0xCAFEBABE]);
}

#[test]
fn test_from_lsb_words() {
    // Test with single word, full length
    let words = [0b1010101010101010101010101010101010101010101010101010101010101010u64];
    let bit_array = BitArray::from_lsb_words(&words, 64);

    assert_eq!(bit_array.len(), 64);
    // Check that alternating bits are set (LSB pattern)
    for i in 0..64 {
        let expected = i % 2 == 1;
        assert_eq!(
            bit_array.contains(i),
            expected,
            "Bit {} should be {}",
            i,
            expected
        );
    }

    // Test with single word, partial length
    let words = [0xFFFFFFFFFFFFFFFFu64];
    let bit_array = BitArray::from_lsb_words(&words, 10);

    assert_eq!(bit_array.len(), 10);
    // First 10 bits should be set
    for i in 0..10 {
        assert!(bit_array.contains(i), "Bit {} should be set", i);
    }
    // Storage should have excess bits masked
    assert_eq!(bit_array.storage()[0], 0x3FF); // Only bottom 10 bits set

    // Test with multiple words
    let words = [0x123456789ABCDEFFu64, 0xFEDCBA0987654321u64];
    let bit_array = BitArray::from_lsb_words(&words, 128);

    assert_eq!(bit_array.len(), 128);
    assert_eq!(bit_array.storage().len(), 2);
    assert_eq!(bit_array.storage()[0], 0x123456789ABCDEFFu64);
    assert_eq!(bit_array.storage()[1], 0xFEDCBA0987654321u64);

    // Test with multiple words, partial length in second word
    let words = [
        0xFFFFFFFFFFFFFFFFu64,
        0xFFFFFFFFFFFFFFFFu64,
        0xFFFFFFFFFFFFFFFFu64,
    ];
    let bit_array = BitArray::from_lsb_words(&words, 80); // 64 + 16 bits

    assert_eq!(bit_array.len(), 80);
    assert_eq!(bit_array.storage().len(), 2); // Should only use 2 words
    assert_eq!(bit_array.storage()[0], 0xFFFFFFFFFFFFFFFFu64);
    assert_eq!(bit_array.storage()[1], 0xFFFFu64); // Only bottom 16 bits set

    // Test with specific bit patterns
    let words = [0x1u64]; // Only LSB set
    let bit_array = BitArray::from_lsb_words(&words, 64);

    assert!(bit_array.contains(0), "Bit 0 should be set");
    for i in 1..64 {
        assert!(!bit_array.contains(i), "Bit {} should not be set", i);
    }

    // Test with zero length (edge case)
    let words = [0xFFFFFFFFFFFFFFFFu64];
    let bit_array = BitArray::from_lsb_words(&words, 0);

    assert_eq!(bit_array.len(), 0);
    assert!(bit_array.is_empty());

    // Test with exact word boundary
    let words = [0xAAAAAAAAAAAAAAAAu64, 0x5555555555555555u64];
    let bit_array = BitArray::from_lsb_words(&words, 64);

    assert_eq!(bit_array.len(), 64);
    assert_eq!(bit_array.storage().len(), 1);
    assert_eq!(bit_array.storage()[0], 0xAAAAAAAAAAAAAAAAu64);

    // Verify the pattern (alternating bits starting with 0)
    for i in 0..64 {
        let expected = i % 2 == 1;
        assert_eq!(
            bit_array.contains(i),
            expected,
            "Bit {} should be {}",
            i,
            expected
        );
    }
}

#[test]
fn test_bitwise_and() {
    let mut arr1 = BitArray::empty(8);
    let mut arr2 = BitArray::empty(8);

    // Set bits: arr1 = 10101010, arr2 = 11001100
    arr1.set(1);
    arr1.set(3);
    arr1.set(5);
    arr1.set(7);
    arr2.set(2);
    arr2.set(3);
    arr2.set(6);
    arr2.set(7);

    let result = &arr1 & &arr2;

    // Expected result: 10000010 (only bits 3 and 7 are set in both)
    assert!(result.contains(3));
    assert!(result.contains(7));
    assert!(!result.contains(1));
    assert!(!result.contains(5));
    assert!(!result.contains(2));
    assert!(!result.contains(6));
}

#[test]
fn test_bitwise_or() {
    let mut arr1 = BitArray::empty(8);
    let mut arr2 = BitArray::empty(8);

    // Set bits: arr1 = 10100000, arr2 = 00001100
    arr1.set(5);
    arr1.set(7);
    arr2.set(2);
    arr2.set(3);

    let result = &arr1 | &arr2;

    // Expected result: 10101100
    assert!(result.contains(2));
    assert!(result.contains(3));
    assert!(result.contains(5));
    assert!(result.contains(7));
    assert!(!result.contains(0));
    assert!(!result.contains(1));
    assert!(!result.contains(4));
    assert!(!result.contains(6));
}

#[test]
fn test_bitwise_xor() {
    let mut arr1 = BitArray::empty(6);
    let mut arr2 = BitArray::empty(6);

    // Set bits: arr1 = 101010, arr2 = 110011
    arr1.set(1);
    arr1.set(3);
    arr1.set(5);
    arr2.set(0);
    arr2.set(1);
    arr2.set(4);
    arr2.set(5);

    let result = &arr1 ^ &arr2;

    // Expected result: 011001 (bits different between arr1 and arr2)
    assert!(result.contains(0)); // only in arr2
    assert!(!result.contains(1)); // in both
    assert!(result.contains(3)); // only in arr1
    assert!(result.contains(4)); // only in arr2
    assert!(!result.contains(5)); // in both
}

#[test]
fn test_bitwise_not() {
    let mut arr = BitArray::empty(5);
    arr.set(1);
    arr.set(4);

    let result = !&arr;

    // Original: 10010, Result: 01101
    assert!(result.contains(0));
    assert!(!result.contains(1));
    assert!(result.contains(2));
    assert!(result.contains(3));
    assert!(!result.contains(4));
}

#[test]
fn test_bitwise_assign_operations() {
    let mut arr1 = BitArray::empty(4);
    let mut arr2 = BitArray::empty(4);

    arr1.set(0);
    arr1.set(2); // 0101
    arr2.set(1);
    arr2.set(2); // 0110

    // Test AND assignment
    let mut test_arr = arr1.clone();
    test_arr &= &arr2;
    assert!(!test_arr.contains(0));
    assert!(!test_arr.contains(1));
    assert!(test_arr.contains(2)); // Only bit 2 is in both
    assert!(!test_arr.contains(3));

    // Test OR assignment
    let mut test_arr = arr1.clone();
    test_arr |= &arr2;
    assert!(test_arr.contains(0));
    assert!(test_arr.contains(1));
    assert!(test_arr.contains(2));
    assert!(!test_arr.contains(3));

    // Test XOR assignment
    let mut test_arr = arr1.clone();
    test_arr ^= &arr2;
    assert!(test_arr.contains(0));
    assert!(test_arr.contains(1));
    assert!(!test_arr.contains(2)); // Bit 2 cancelled out
    assert!(!test_arr.contains(3));
}

#[test]
fn test_select() {
    // Test with empty bit array
    let empty_array = BitArray::empty(10);
    assert_eq!(
        empty_array.select(0),
        None,
        "Empty array should return None"
    );

    // Test with single set bit
    let mut single_bit = BitArray::empty(10);
    single_bit.set(5);
    assert_eq!(
        single_bit.select(0),
        Some(5),
        "First set bit should be at position 5"
    );
    assert_eq!(single_bit.select(1), None, "Second set bit doesn't exist");

    // Test with multiple set bits
    let mut multi_bits = BitArray::empty(20);
    multi_bits.set(2);
    multi_bits.set(7);
    multi_bits.set(10);
    multi_bits.set(15);
    multi_bits.set(19);

    assert_eq!(
        multi_bits.select(0),
        Some(2),
        "0th set bit should be at position 2"
    );
    assert_eq!(
        multi_bits.select(1),
        Some(7),
        "1st set bit should be at position 7"
    );
    assert_eq!(
        multi_bits.select(2),
        Some(10),
        "2nd set bit should be at position 10"
    );
    assert_eq!(
        multi_bits.select(3),
        Some(15),
        "3rd set bit should be at position 15"
    );
    assert_eq!(
        multi_bits.select(4),
        Some(19),
        "4th set bit should be at position 19"
    );
    assert_eq!(multi_bits.select(5), None, "5th set bit doesn't exist");

    // Test across word boundaries (multiple u64 words)
    let mut large_array = BitArray::empty(200);
    // Set bits at positions: 1, 63, 64, 65, 127, 128, 150
    large_array.set(1);
    large_array.set(63); // Last bit of first word
    large_array.set(64); // First bit of second word
    large_array.set(65); // Second bit of second word
    large_array.set(127); // Last bit of second word
    large_array.set(128); // First bit of third word
    large_array.set(150);

    assert_eq!(large_array.select(0), Some(1));
    assert_eq!(large_array.select(1), Some(63));
    assert_eq!(large_array.select(2), Some(64));
    assert_eq!(large_array.select(3), Some(65));
    assert_eq!(large_array.select(4), Some(127));
    assert_eq!(large_array.select(5), Some(128));
    assert_eq!(large_array.select(6), Some(150));
    assert_eq!(large_array.select(7), None);

    // Test with consecutive set bits
    let mut consecutive = BitArray::empty(10);
    consecutive.set(3);
    consecutive.set(4);
    consecutive.set(5);
    consecutive.set(6);

    assert_eq!(consecutive.select(0), Some(3));
    assert_eq!(consecutive.select(1), Some(4));
    assert_eq!(consecutive.select(2), Some(5));
    assert_eq!(consecutive.select(3), Some(6));
    assert_eq!(consecutive.select(4), None);

    // Test with all bits set
    let mut all_set = BitArray::empty(8);
    all_set.set_all();

    for i in 0..8 {
        assert_eq!(
            all_set.select(i),
            Some(i),
            "select({}) should return Some({})",
            i,
            i
        );
    }
    assert_eq!(
        all_set.select(8),
        None,
        "select(8) should return None for 8-bit array"
    );

    // Test edge case: first bit set
    let mut first_bit = BitArray::empty(64);
    first_bit.set(0);
    assert_eq!(
        first_bit.select(0),
        Some(0),
        "First bit should be selectable"
    );

    // Test edge case: last bit set in a word boundary
    let mut last_bit = BitArray::empty(64);
    last_bit.set(63);
    assert_eq!(
        last_bit.select(0),
        Some(63),
        "Last bit in word should be selectable"
    );

    // Test with pattern spanning multiple words
    let mut pattern = BitArray::empty(130);
    // Set every 10th bit: 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120
    for i in (10..130).step_by(10) {
        pattern.set(i);
    }

    assert_eq!(pattern.select(0), Some(10));
    assert_eq!(pattern.select(1), Some(20));
    assert_eq!(pattern.select(5), Some(60));
    assert_eq!(pattern.select(11), Some(120));
    assert_eq!(pattern.select(12), None);

    // Test with single word, various patterns
    let mut single_word = BitArray::empty(64);
    // Set bits: 0, 1, 31, 32, 62, 63
    single_word.set(0);
    single_word.set(1);
    single_word.set(31);
    single_word.set(32);
    single_word.set(62);
    single_word.set(63);

    assert_eq!(single_word.select(0), Some(0));
    assert_eq!(single_word.select(1), Some(1));
    assert_eq!(single_word.select(2), Some(31));
    assert_eq!(single_word.select(3), Some(32));
    assert_eq!(single_word.select(4), Some(62));
    assert_eq!(single_word.select(5), Some(63));
    assert_eq!(single_word.select(6), None);
}

#[test]
fn test_rank() {
    // Test with empty bit array
    let empty_array = BitArray::empty(10);
    for i in 0..10 {
        assert_eq!(
            empty_array.rank(i),
            0,
            "Rank of position {} in empty array should be 0",
            i
        );
    }

    // Test with single set bit
    let mut single_bit = BitArray::empty(10);
    single_bit.set(5);

    // Before the set bit, rank should be 0
    for i in 0..5 {
        assert_eq!(
            single_bit.rank(i),
            0,
            "Rank of position {} should be 0 (before set bit)",
            i
        );
    }
    // At and after the set bit, rank should be 1
    for i in 5..10 {
        assert_eq!(
            single_bit.rank(i),
            1,
            "Rank of position {} should be 1 (at or after set bit)",
            i
        );
    }

    // Test with multiple set bits in single word
    let mut multi_bits = BitArray::empty(20);
    multi_bits.set(2); // positions with set bits: 2, 7, 10, 15
    multi_bits.set(7);
    multi_bits.set(10);
    multi_bits.set(15);

    assert_eq!(multi_bits.rank(0), 0, "Rank at position 0");
    assert_eq!(multi_bits.rank(1), 0, "Rank at position 1");
    assert_eq!(multi_bits.rank(2), 1, "Rank at position 2 (first set bit)");
    assert_eq!(multi_bits.rank(3), 1, "Rank at position 3");
    assert_eq!(multi_bits.rank(6), 1, "Rank at position 6");
    assert_eq!(multi_bits.rank(7), 2, "Rank at position 7 (second set bit)");
    assert_eq!(multi_bits.rank(8), 2, "Rank at position 8");
    assert_eq!(multi_bits.rank(9), 2, "Rank at position 9");
    assert_eq!(
        multi_bits.rank(10),
        3,
        "Rank at position 10 (third set bit)"
    );
    assert_eq!(multi_bits.rank(14), 3, "Rank at position 14");
    assert_eq!(
        multi_bits.rank(15),
        4,
        "Rank at position 15 (fourth set bit)"
    );
    assert_eq!(
        multi_bits.rank(19),
        4,
        "Rank at position 19 (last position)"
    );

    // Test across word boundaries (multiple u64 words)
    let mut large_array = BitArray::empty(200);
    // Set bits at positions: 1, 63, 64, 65, 127, 128, 150
    large_array.set(1); // 1st set bit
    large_array.set(63); // 2nd set bit (last bit of first word)
    large_array.set(64); // 3rd set bit (first bit of second word)
    large_array.set(65); // 4th set bit
    large_array.set(127); // 5th set bit (last bit of second word)
    large_array.set(128); // 6th set bit (first bit of third word)
    large_array.set(150); // 7th set bit

    assert_eq!(large_array.rank(0), 0, "Rank at position 0");
    assert_eq!(large_array.rank(1), 1, "Rank at position 1");
    assert_eq!(large_array.rank(2), 1, "Rank at position 2");
    assert_eq!(large_array.rank(62), 1, "Rank at position 62");
    assert_eq!(
        large_array.rank(63),
        2,
        "Rank at position 63 (word boundary)"
    );
    assert_eq!(large_array.rank(64), 3, "Rank at position 64 (next word)");
    assert_eq!(large_array.rank(65), 4, "Rank at position 65");
    assert_eq!(large_array.rank(100), 4, "Rank at position 100");
    assert_eq!(
        large_array.rank(127),
        5,
        "Rank at position 127 (word boundary)"
    );
    assert_eq!(large_array.rank(128), 6, "Rank at position 128 (next word)");
    assert_eq!(large_array.rank(149), 6, "Rank at position 149");
    assert_eq!(large_array.rank(150), 7, "Rank at position 150");
    assert_eq!(large_array.rank(199), 7, "Rank at last position");

    // Test with consecutive set bits
    let mut consecutive = BitArray::empty(15);
    consecutive.set(5);
    consecutive.set(6);
    consecutive.set(7);
    consecutive.set(8);
    consecutive.set(9);

    assert_eq!(consecutive.rank(4), 0, "Rank before consecutive bits");
    assert_eq!(consecutive.rank(5), 1, "Rank at start of consecutive bits");
    assert_eq!(consecutive.rank(6), 2, "Rank in consecutive bits");
    assert_eq!(consecutive.rank(7), 3, "Rank in consecutive bits");
    assert_eq!(consecutive.rank(8), 4, "Rank in consecutive bits");
    assert_eq!(consecutive.rank(9), 5, "Rank at end of consecutive bits");
    assert_eq!(consecutive.rank(10), 5, "Rank after consecutive bits");
    assert_eq!(consecutive.rank(14), 5, "Rank at last position");

    // Test with all bits set
    let mut all_set = BitArray::empty(10);
    all_set.set_all();

    for i in 0..10 {
        assert_eq!(
            all_set.rank(i),
            i + 1,
            "Rank at position {} should be {} when all bits are set",
            i,
            i + 1
        );
    }

    // Test edge cases: first and last positions
    let mut edge_case = BitArray::empty(64);
    edge_case.set(0); // First bit
    edge_case.set(63); // Last bit

    assert_eq!(
        edge_case.rank(0),
        1,
        "Rank at first position when first bit is set"
    );
    assert_eq!(edge_case.rank(32), 1, "Rank at middle position");
    assert_eq!(edge_case.rank(62), 1, "Rank just before last bit");
    assert_eq!(
        edge_case.rank(63),
        2,
        "Rank at last position when last bit is set"
    );

    // Test with pattern spanning exactly 64 bits (one word)
    let mut one_word = BitArray::empty(64);
    // Set every 8th bit: 0, 8, 16, 24, 32, 40, 48, 56
    for i in (0..64).step_by(8) {
        one_word.set(i);
    }

    assert_eq!(one_word.rank(0), 1, "Rank at position 0");
    assert_eq!(one_word.rank(7), 1, "Rank at position 7");
    assert_eq!(one_word.rank(8), 2, "Rank at position 8");
    assert_eq!(one_word.rank(15), 2, "Rank at position 15");
    assert_eq!(one_word.rank(16), 3, "Rank at position 16");
    assert_eq!(one_word.rank(31), 4, "Rank at position 31");
    assert_eq!(one_word.rank(32), 5, "Rank at position 32");
    assert_eq!(one_word.rank(56), 8, "Rank at position 56 (last set bit)");
    assert_eq!(one_word.rank(63), 8, "Rank at last position");

    // Test with alternating pattern
    let mut alternating = BitArray::empty(20);
    // Set odd positions: 1, 3, 5, 7, 9, 11, 13, 15, 17, 19
    for i in (1..20).step_by(2) {
        alternating.set(i);
    }

    assert_eq!(alternating.rank(0), 0, "Rank at even position 0");
    assert_eq!(alternating.rank(1), 1, "Rank at odd position 1");
    assert_eq!(alternating.rank(2), 1, "Rank at even position 2");
    assert_eq!(alternating.rank(3), 2, "Rank at odd position 3");
    assert_eq!(alternating.rank(10), 5, "Rank at even position 10");
    assert_eq!(alternating.rank(11), 6, "Rank at odd position 11");
    assert_eq!(alternating.rank(18), 9, "Rank at even position 18");
    assert_eq!(alternating.rank(19), 10, "Rank at last odd position");
}

#[test]
fn test_set_range() {
    // Test setting range within a single word
    let mut bit_array = BitArray::empty(20);
    bit_array.set_range(3..8);

    // Verify bits 3-7 are set, others are not
    for i in 0..20 {
        if i >= 3 && i < 8 {
            assert!(bit_array.contains(i), "Bit {} should be set", i);
        } else {
            assert!(!bit_array.contains(i), "Bit {} should not be set", i);
        }
    }

    // Test setting additional range that overlaps
    bit_array.set_range(5..12);

    // Verify bits 3-11 are now set
    for i in 0..20 {
        if i >= 3 && i < 12 {
            assert!(
                bit_array.contains(i),
                "Bit {} should be set after overlap",
                i
            );
        } else {
            assert!(
                !bit_array.contains(i),
                "Bit {} should not be set after overlap",
                i
            );
        }
    }

    // Test setting range across word boundaries
    let mut large_array = BitArray::empty(150);
    large_array.set_range(62..67); // Spans from word 0 to word 1

    assert!(large_array.contains(62), "Bit 62 should be set");
    assert!(large_array.contains(63), "Bit 63 should be set");
    assert!(large_array.contains(64), "Bit 64 should be set");
    assert!(large_array.contains(65), "Bit 65 should be set");
    assert!(large_array.contains(66), "Bit 66 should be set");
    assert!(!large_array.contains(61), "Bit 61 should not be set");
    assert!(!large_array.contains(67), "Bit 67 should not be set");

    // Test setting range across multiple words
    large_array.clear();
    large_array.set_range(60..130); // Spans across 3 words

    for i in 60..130 {
        assert!(
            large_array.contains(i),
            "Bit {} should be set in multi-word range",
            i
        );
    }
    assert!(!large_array.contains(59), "Bit 59 should not be set");
    assert!(!large_array.contains(130), "Bit 130 should not be set");

    // Test setting range at word boundary (exactly at bit 64)
    let mut boundary_array = BitArray::empty(200);
    boundary_array.set_range(64..128); // Exactly one full word

    for i in 64..128 {
        assert!(
            boundary_array.contains(i),
            "Bit {} should be set at word boundary",
            i
        );
    }
    assert!(!boundary_array.contains(63), "Bit 63 should not be set");
    assert!(!boundary_array.contains(128), "Bit 128 should not be set");

    // Test setting single bit range
    let mut single_bit = BitArray::empty(10);
    single_bit.set_range(5..6);

    assert!(single_bit.contains(5), "Single bit 5 should be set");
    for i in 0..10 {
        if i != 5 {
            assert!(!single_bit.contains(i), "Bit {} should not be set", i);
        }
    }

    // Test setting range to end of array
    let mut end_range = BitArray::empty(10);
    end_range.set_range(7..10);

    assert!(end_range.contains(7), "Bit 7 should be set");
    assert!(end_range.contains(8), "Bit 8 should be set");
    assert!(end_range.contains(9), "Bit 9 should be set");
    for i in 0..7 {
        assert!(!end_range.contains(i), "Bit {} should not be set", i);
    }

    // Test setting range from beginning of array
    let mut start_range = BitArray::empty(10);
    start_range.set_range(0..4);

    for i in 0..4 {
        assert!(
            start_range.contains(i),
            "Bit {} should be set from start",
            i
        );
    }
    for i in 4..10 {
        assert!(!start_range.contains(i), "Bit {} should not be set", i);
    }

    // Test empty range (should do nothing)
    let mut empty_range = BitArray::empty(10);
    empty_range.set(3);
    empty_range.set_range(5..5); // Empty range

    assert!(empty_range.contains(3), "Existing bit should remain set");
    assert!(
        !empty_range.contains(5),
        "Empty range should not set any bits"
    );

    // Test reverse range (start > end, should do nothing)
    empty_range.set_range(8..6); // Reverse range
    assert!(
        !empty_range.contains(6),
        "Reverse range should not set bits"
    );
    assert!(
        !empty_range.contains(7),
        "Reverse range should not set bits"
    );
    assert!(
        !empty_range.contains(8),
        "Reverse range should not set bits"
    );

    // Test setting entire array
    let mut full_array = BitArray::empty(10);
    full_array.set_range(0..10);

    for i in 0..10 {
        assert!(full_array.contains(i), "All bits should be set");
    }

    // Test setting range on array with existing bits
    let mut existing_bits = BitArray::empty(10);
    existing_bits.set(1);
    existing_bits.set(8);
    existing_bits.set_range(3..7);

    assert!(existing_bits.contains(1), "Existing bit 1 should remain");
    assert!(existing_bits.contains(3), "Range bit 3 should be set");
    assert!(existing_bits.contains(4), "Range bit 4 should be set");
    assert!(existing_bits.contains(5), "Range bit 5 should be set");
    assert!(existing_bits.contains(6), "Range bit 6 should be set");
    assert!(existing_bits.contains(8), "Existing bit 8 should remain");
    assert!(!existing_bits.contains(0), "Bit 0 should not be set");
    assert!(!existing_bits.contains(2), "Bit 2 should not be set");
    assert!(!existing_bits.contains(7), "Bit 7 should not be set");
    assert!(!existing_bits.contains(9), "Bit 9 should not be set");
}

#[test]
fn test_reset_range() {
    // Test resetting range within a single word
    let mut bit_array = BitArray::full(20); // Start with all bits set
    bit_array.reset_range(3..8);

    // Verify bits 3-7 are reset, others remain set
    for i in 0..20 {
        if i >= 3 && i < 8 {
            assert!(!bit_array.contains(i), "Bit {} should be reset", i);
        } else {
            assert!(bit_array.contains(i), "Bit {} should remain set", i);
        }
    }

    // Test resetting additional range that overlaps
    bit_array.reset_range(5..12);

    // Verify bits 3-11 are now reset
    for i in 0..20 {
        if i >= 3 && i < 12 {
            assert!(
                !bit_array.contains(i),
                "Bit {} should be reset after overlap",
                i
            );
        } else {
            assert!(
                bit_array.contains(i),
                "Bit {} should remain set after overlap",
                i
            );
        }
    }

    // Test resetting range across word boundaries
    let mut large_array = BitArray::full(150);
    large_array.reset_range(62..67); // Spans from word 0 to word 1

    assert!(!large_array.contains(62), "Bit 62 should be reset");
    assert!(!large_array.contains(63), "Bit 63 should be reset");
    assert!(!large_array.contains(64), "Bit 64 should be reset");
    assert!(!large_array.contains(65), "Bit 65 should be reset");
    assert!(!large_array.contains(66), "Bit 66 should be reset");
    assert!(large_array.contains(61), "Bit 61 should remain set");
    assert!(large_array.contains(67), "Bit 67 should remain set");

    // Test resetting range across multiple words
    large_array.set_all();
    large_array.reset_range(60..130); // Spans across 3 words

    for i in 60..130 {
        assert!(
            !large_array.contains(i),
            "Bit {} should be reset in multi-word range",
            i
        );
    }
    assert!(large_array.contains(59), "Bit 59 should remain set");
    assert!(large_array.contains(130), "Bit 130 should remain set");

    // Test resetting range at word boundary (exactly at bit 64)
    let mut boundary_array = BitArray::full(200);
    boundary_array.reset_range(64..128); // Exactly one full word

    for i in 64..128 {
        assert!(
            !boundary_array.contains(i),
            "Bit {} should be reset at word boundary",
            i
        );
    }
    assert!(boundary_array.contains(63), "Bit 63 should remain set");
    assert!(boundary_array.contains(128), "Bit 128 should remain set");

    // Test resetting single bit range
    let mut single_bit = BitArray::full(10);
    single_bit.reset_range(5..6);

    assert!(!single_bit.contains(5), "Single bit 5 should be reset");
    for i in 0..10 {
        if i != 5 {
            assert!(single_bit.contains(i), "Bit {} should remain set", i);
        }
    }

    // Test resetting range to end of array
    let mut end_range = BitArray::full(10);
    end_range.reset_range(7..10);

    assert!(!end_range.contains(7), "Bit 7 should be reset");
    assert!(!end_range.contains(8), "Bit 8 should be reset");
    assert!(!end_range.contains(9), "Bit 9 should be reset");
    for i in 0..7 {
        assert!(end_range.contains(i), "Bit {} should remain set", i);
    }

    // Test resetting range from beginning of array
    let mut start_range = BitArray::full(10);
    start_range.reset_range(0..4);

    for i in 0..4 {
        assert!(
            !start_range.contains(i),
            "Bit {} should be reset from start",
            i
        );
    }
    for i in 4..10 {
        assert!(start_range.contains(i), "Bit {} should remain set", i);
    }

    // Test empty range (should do nothing)
    let mut empty_range = BitArray::full(10);
    empty_range.reset(3);
    empty_range.reset_range(5..5); // Empty range

    assert!(
        !empty_range.contains(3),
        "Manually reset bit should remain reset"
    );
    assert!(
        empty_range.contains(5),
        "Empty range should not reset any bits"
    );

    // Test reverse range (start > end, should do nothing)
    empty_range.reset_range(8..6); // Reverse range
    assert!(
        empty_range.contains(6),
        "Reverse range should not reset bits"
    );
    assert!(
        empty_range.contains(7),
        "Reverse range should not reset bits"
    );
    assert!(
        empty_range.contains(8),
        "Reverse range should not reset bits"
    );

    // Test resetting entire array
    let mut full_array = BitArray::full(10);
    full_array.reset_range(0..10);

    for i in 0..10 {
        assert!(!full_array.contains(i), "All bits should be reset");
    }

    // Test resetting range on array with mixed bits
    let mut mixed_bits = BitArray::empty(10);
    mixed_bits.set(1);
    mixed_bits.set(3);
    mixed_bits.set(4);
    mixed_bits.set(5);
    mixed_bits.set(6);
    mixed_bits.set(8);
    mixed_bits.reset_range(3..7);

    assert!(mixed_bits.contains(1), "Unaffected bit 1 should remain set");
    assert!(!mixed_bits.contains(3), "Range bit 3 should be reset");
    assert!(!mixed_bits.contains(4), "Range bit 4 should be reset");
    assert!(!mixed_bits.contains(5), "Range bit 5 should be reset");
    assert!(!mixed_bits.contains(6), "Range bit 6 should be reset");
    assert!(mixed_bits.contains(8), "Unaffected bit 8 should remain set");
    assert!(!mixed_bits.contains(0), "Bit 0 should remain unset");
    assert!(!mixed_bits.contains(2), "Bit 2 should remain unset");
    assert!(!mixed_bits.contains(7), "Bit 7 should remain unset");
    assert!(!mixed_bits.contains(9), "Bit 9 should remain unset");
}

#[test]
fn test_set_reset_range_edge_cases() {
    // Test with exactly 64-bit array (single word)
    let mut single_word = BitArray::empty(64);
    single_word.set_range(0..64);

    for i in 0..64 {
        assert!(
            single_word.contains(i),
            "Bit {} should be set in full word",
            i
        );
    }

    single_word.reset_range(20..44);
    for i in 0..20 {
        assert!(single_word.contains(i), "Bit {} should remain set", i);
    }
    for i in 20..44 {
        assert!(!single_word.contains(i), "Bit {} should be reset", i);
    }
    for i in 44..64 {
        assert!(single_word.contains(i), "Bit {} should remain set", i);
    }

    // Test with exactly 128-bit array (two words)
    let mut two_words = BitArray::empty(128);
    two_words.set_range(32..96); // Spans across word boundary

    for i in 32..96 {
        assert!(
            two_words.contains(i),
            "Bit {} should be set across words",
            i
        );
    }
    for i in 0..32 {
        assert!(!two_words.contains(i), "Bit {} should not be set", i);
    }
    for i in 96..128 {
        assert!(!two_words.contains(i), "Bit {} should not be set", i);
    }

    // Test setting and resetting the same range
    let mut toggle_range = BitArray::empty(20);
    toggle_range.set_range(5..15);

    for i in 5..15 {
        assert!(
            toggle_range.contains(i),
            "Bit {} should be set initially",
            i
        );
    }

    toggle_range.reset_range(5..15);

    for i in 5..15 {
        assert!(!toggle_range.contains(i), "Bit {} should be reset", i);
    }

    // Test partial overlap between set and reset ranges
    let mut overlap_test = BitArray::empty(20);
    overlap_test.set_range(3..12);
    overlap_test.reset_range(8..15);

    for i in 3..8 {
        assert!(overlap_test.contains(i), "Bit {} should remain set", i);
    }
    for i in 8..12 {
        assert!(
            !overlap_test.contains(i),
            "Bit {} should be reset by overlap",
            i
        );
    }
    for i in 12..15 {
        assert!(
            !overlap_test.contains(i),
            "Bit {} should be reset (was not set)",
            i
        );
    }

    // Test with non-64-aligned boundaries
    let mut unaligned = BitArray::empty(100);
    unaligned.set_range(13..87); // Start and end not aligned to word boundaries

    for i in 13..87 {
        assert!(
            unaligned.contains(i),
            "Bit {} should be set in unaligned range",
            i
        );
    }
    assert!(!unaligned.contains(12), "Bit 12 should not be set");
    assert!(!unaligned.contains(87), "Bit 87 should not be set");

    // Test with very small array
    let mut tiny = BitArray::empty(3);
    tiny.set_range(1..3);

    assert!(!tiny.contains(0), "Bit 0 should not be set");
    assert!(tiny.contains(1), "Bit 1 should be set");
    assert!(tiny.contains(2), "Bit 2 should be set");

    tiny.reset_range(1..2);
    assert!(!tiny.contains(1), "Bit 1 should be reset");
    assert!(tiny.contains(2), "Bit 2 should remain set");
}

#[test]
#[should_panic(expected = "Range end 15 out of bounds (len: 10)")]
fn test_set_range_out_of_bounds() {
    let mut bit_array = BitArray::empty(10);
    bit_array.set_range(5..15); // End is beyond array length
}

#[test]
#[should_panic(expected = "Range end 150 out of bounds (len: 100)")]
fn test_reset_range_out_of_bounds() {
    let mut bit_array = BitArray::full(100);
    bit_array.reset_range(90..150); // End is beyond array length
}

#[test]
fn test_set_reset_range_count_verification() {
    // Verify that count_ones is correct after range operations
    let mut bit_array = BitArray::empty(100);

    // Initially empty
    assert_eq!(bit_array.count_ones(), 0);

    // Set a range
    bit_array.set_range(10..20);
    assert_eq!(bit_array.count_ones(), 10);

    // Set another non-overlapping range
    bit_array.set_range(30..35);
    assert_eq!(bit_array.count_ones(), 15);

    // Set overlapping range
    bit_array.set_range(18..32);
    assert_eq!(bit_array.count_ones(), 25); // 10-35 are now set

    // Reset partial range
    bit_array.reset_range(15..25);
    assert_eq!(bit_array.count_ones(), 15); // 10-14 and 25-35 are set

    // Reset everything
    bit_array.reset_range(0..100);
    assert_eq!(bit_array.count_ones(), 0);
}

// ...existing code...

#[test]
fn test_from_positions() {
    // Test with empty positions
    let empty_positions = std::iter::empty();
    let bit_array = BitArray::from_positions(empty_positions, 10);
    assert_eq!(bit_array.len(), 10);
    assert_eq!(bit_array.count_ones(), 0);
    for i in 0..10 {
        assert!(!bit_array.contains(i), "Bit {} should not be set", i);
    }

    // Test with single position
    let single_position = std::iter::once(5);
    let bit_array = BitArray::from_positions(single_position, 10);
    assert_eq!(bit_array.len(), 10);
    assert_eq!(bit_array.count_ones(), 1);
    assert!(bit_array.contains(5), "Bit 5 should be set");
    for i in 0..10 {
        if i != 5 {
            assert!(!bit_array.contains(i), "Bit {} should not be set", i);
        }
    }

    // Test with multiple positions within single word
    let positions = vec![0, 3, 7, 11, 19];
    let bit_array = BitArray::from_positions(positions.into_iter(), 20);
    assert_eq!(bit_array.len(), 20);
    assert_eq!(bit_array.count_ones(), 5);

    for &pos in &[0, 3, 7, 11, 19] {
        assert!(bit_array.contains(pos), "Bit {} should be set", pos);
    }

    for i in 0..20 {
        if ![0, 3, 7, 11, 19].contains(&i) {
            assert!(!bit_array.contains(i), "Bit {} should not be set", i);
        }
    }

    // Test with positions across word boundaries
    let positions = vec![0, 63, 64, 65, 127, 128, 149];
    let bit_array = BitArray::from_positions(positions.into_iter(), 150);
    assert_eq!(bit_array.len(), 150);
    assert_eq!(bit_array.count_ones(), 7);

    for &pos in &[0, 63, 64, 65, 127, 128, 149] {
        assert!(bit_array.contains(pos), "Bit {} should be set", pos);
    }

    // Test with consecutive positions
    let positions = (5..10).collect::<Vec<_>>();
    let bit_array = BitArray::from_positions(positions.into_iter(), 15);
    assert_eq!(bit_array.len(), 15);
    assert_eq!(bit_array.count_ones(), 5);

    for i in 5..10 {
        assert!(bit_array.contains(i), "Bit {} should be set", i);
    }
    for i in 0..5 {
        assert!(!bit_array.contains(i), "Bit {} should not be set", i);
    }
    for i in 10..15 {
        assert!(!bit_array.contains(i), "Bit {} should not be set", i);
    }

    // Test with duplicate positions (should handle gracefully)
    let positions = vec![2, 5, 2, 8, 5, 2];
    let bit_array = BitArray::from_positions(positions.into_iter(), 10);
    assert_eq!(bit_array.len(), 10);
    assert_eq!(bit_array.count_ones(), 3); // Only unique positions count
    assert!(bit_array.contains(2));
    assert!(bit_array.contains(5));
    assert!(bit_array.contains(8));

    // Test with positions at word boundaries
    let positions = vec![63, 64, 127, 128];
    let bit_array = BitArray::from_positions(positions.into_iter(), 200);
    assert_eq!(bit_array.len(), 200);
    assert_eq!(bit_array.count_ones(), 4);
    assert!(bit_array.contains(63)); // Last bit of first word
    assert!(bit_array.contains(64)); // First bit of second word
    assert!(bit_array.contains(127)); // Last bit of second word
    assert!(bit_array.contains(128)); // First bit of third word

    // Test with all positions in range
    let positions = (0..8).collect::<Vec<_>>();
    let bit_array = BitArray::from_positions(positions.into_iter(), 8);
    assert_eq!(bit_array.len(), 8);
    assert_eq!(bit_array.count_ones(), 8);
    for i in 0..8 {
        assert!(bit_array.contains(i), "Bit {} should be set", i);
    }

    // Test with positions spanning multiple words
    let positions = vec![10, 50, 100, 150, 200];
    let bit_array = BitArray::from_positions(positions.into_iter(), 250);
    assert_eq!(bit_array.len(), 250);
    assert_eq!(bit_array.count_ones(), 5);
    for &pos in &[10, 50, 100, 150, 200] {
        assert!(bit_array.contains(pos), "Bit {} should be set", pos);
    }

    // Test with first and last positions
    let positions = vec![0, 99];
    let bit_array = BitArray::from_positions(positions.into_iter(), 100);
    assert_eq!(bit_array.len(), 100);
    assert_eq!(bit_array.count_ones(), 2);
    assert!(bit_array.contains(0));
    assert!(bit_array.contains(99));
    for i in 1..99 {
        assert!(!bit_array.contains(i), "Bit {} should not be set", i);
    }

    // Test with zero length array
    let empty_positions = std::iter::empty();
    let bit_array = BitArray::from_positions(empty_positions, 0);
    assert_eq!(bit_array.len(), 0);
    assert!(bit_array.is_empty());
    assert_eq!(bit_array.count_ones(), 0);
}

#[test]
fn test_from_ranges() {
    // Test with empty ranges
    let empty_ranges = std::iter::empty();
    let bit_array = BitArray::from_ranges(empty_ranges, 10);
    assert_eq!(bit_array.len(), 10);
    assert_eq!(bit_array.count_ones(), 0);
    for i in 0..10 {
        assert!(!bit_array.contains(i), "Bit {} should not be set", i);
    }

    // Test with single range
    let single_range = std::iter::once(3..7);
    let bit_array = BitArray::from_ranges(single_range, 10);
    assert_eq!(bit_array.len(), 10);
    assert_eq!(bit_array.count_ones(), 4);

    for i in 3..7 {
        assert!(bit_array.contains(i), "Bit {} should be set", i);
    }
    for i in 0..3 {
        assert!(!bit_array.contains(i), "Bit {} should not be set", i);
    }
    for i in 7..10 {
        assert!(!bit_array.contains(i), "Bit {} should not be set", i);
    }

    // Test with multiple non-overlapping ranges
    let ranges = vec![1..3, 5..8, 12..15];
    let bit_array = BitArray::from_ranges(ranges.into_iter(), 20);
    assert_eq!(bit_array.len(), 20);
    assert_eq!(bit_array.count_ones(), 8); // (3-1) + (8-5) + (15-12) = 2 + 3 + 3 = 8

    // Check set bits: 1, 2, 5, 6, 7, 12, 13, 14
    for &pos in &[1, 2, 5, 6, 7, 12, 13, 14] {
        assert!(bit_array.contains(pos), "Bit {} should be set", pos);
    }

    // Check unset bits
    for i in [0, 3, 4, 8, 9, 10, 11, 15, 16, 17, 18, 19] {
        assert!(!bit_array.contains(i), "Bit {} should not be set", i);
    }

    // Test with overlapping ranges
    let ranges = vec![2..6, 4..9, 7..12];
    let bit_array = BitArray::from_ranges(ranges.into_iter(), 15);
    assert_eq!(bit_array.len(), 15);
    assert_eq!(bit_array.count_ones(), 10); // Union of ranges: 2..12 = 10 bits

    for i in 2..12 {
        assert!(bit_array.contains(i), "Bit {} should be set", i);
    }
    for i in [0, 1, 12, 13, 14] {
        assert!(!bit_array.contains(i), "Bit {} should not be set", i);
    }

    // Test with ranges across word boundaries
    let ranges = vec![60..68, 125..130];
    let bit_array = BitArray::from_ranges(ranges.into_iter(), 150);
    assert_eq!(bit_array.len(), 150);
    assert_eq!(bit_array.count_ones(), 13); // (68-60) + (130-125) = 8 + 5 = 13

    // Check first range: 60..68
    for i in 60..68 {
        assert!(bit_array.contains(i), "Bit {} should be set", i);
    }

    // Check second range: 125..130
    for i in 125..130 {
        assert!(bit_array.contains(i), "Bit {} should be set", i);
    }

    // Check some unset bits
    for i in [0, 59, 68, 69, 124, 130, 149] {
        assert!(!bit_array.contains(i), "Bit {} should not be set", i);
    }

    // Test with empty ranges (start == end)
    let ranges = vec![3..3, 5..5, 7..10];
    let bit_array = BitArray::from_ranges(ranges.into_iter(), 15);
    assert_eq!(bit_array.len(), 15);
    assert_eq!(bit_array.count_ones(), 3); // Only 7..10 contributes

    for i in 7..10 {
        assert!(bit_array.contains(i), "Bit {} should be set", i);
    }
    for i in [0, 1, 2, 3, 4, 5, 6, 10, 11, 12, 13, 14] {
        assert!(!bit_array.contains(i), "Bit {} should not be set", i);
    }

    // Test with single bit ranges
    let ranges = vec![2..3, 5..6, 8..9];
    let bit_array = BitArray::from_ranges(ranges.into_iter(), 10);
    assert_eq!(bit_array.len(), 10);
    assert_eq!(bit_array.count_ones(), 3);
    assert!(bit_array.contains(2));
    assert!(bit_array.contains(5));
    assert!(bit_array.contains(8));

    // Test with range covering entire array
    let ranges = vec![0..10];
    let bit_array = BitArray::from_ranges(ranges.into_iter(), 10);
    assert_eq!(bit_array.len(), 10);
    assert_eq!(bit_array.count_ones(), 10);
    for i in 0..10 {
        assert!(bit_array.contains(i), "Bit {} should be set", i);
    }

    // Test with ranges at exact word boundaries
    let ranges = vec![0..64, 64..128, 128..192];
    let bit_array = BitArray::from_ranges(ranges.into_iter(), 200);
    assert_eq!(bit_array.len(), 200);
    assert_eq!(bit_array.count_ones(), 192); // 64 + 64 + 64 = 192

    for i in 0..192 {
        assert!(bit_array.contains(i), "Bit {} should be set", i);
    }
    for i in 192..200 {
        assert!(!bit_array.contains(i), "Bit {} should not be set", i);
    }

    // Test with overlapping ranges that merge
    let ranges = vec![10..20, 15..25, 22..30];
    let bit_array = BitArray::from_ranges(ranges.into_iter(), 40);
    assert_eq!(bit_array.len(), 40);
    assert_eq!(bit_array.count_ones(), 20); // Merged range: 10..30 = 20 bits

    for i in 10..30 {
        assert!(bit_array.contains(i), "Bit {} should be set", i);
    }
    for i in 0..10 {
        assert!(!bit_array.contains(i), "Bit {} should not be set", i);
    }
    for i in 30..40 {
        assert!(!bit_array.contains(i), "Bit {} should not be set", i);
    }

    // Test with zero length array
    let empty_ranges = std::iter::empty();
    let bit_array = BitArray::from_ranges(empty_ranges, 0);
    assert_eq!(bit_array.len(), 0);
    assert!(bit_array.is_empty());
    assert_eq!(bit_array.count_ones(), 0);

    // Test with range spanning single word exactly
    let ranges = vec![0..64];
    let bit_array = BitArray::from_ranges(ranges.into_iter(), 64);
    assert_eq!(bit_array.len(), 64);
    assert_eq!(bit_array.count_ones(), 64);
    for i in 0..64 {
        assert!(bit_array.contains(i), "Bit {} should be set", i);
    }

    // Test with ranges that touch but don't overlap
    let ranges = vec![5..10, 10..15, 15..20];
    let bit_array = BitArray::from_ranges(ranges.into_iter(), 25);
    assert_eq!(bit_array.len(), 25);
    assert_eq!(bit_array.count_ones(), 15); // 5 + 5 + 5 = 15

    for i in 5..20 {
        assert!(bit_array.contains(i), "Bit {} should be set", i);
    }
    for i in 0..5 {
        assert!(!bit_array.contains(i), "Bit {} should not be set", i);
    }
    for i in 20..25 {
        assert!(!bit_array.contains(i), "Bit {} should not be set", i);
    }
}

#[test]
#[should_panic(expected = "out of bounds")]
fn test_from_ranges_panic_on_out_of_bounds() {
    let ranges = vec![0..5, 8..12]; // Range 8..12 is out of bounds for length 10
    BitArray::from_ranges(ranges.into_iter(), 10);
}

#[test]
fn test_iter_within() {
    // Test with empty array
    let empty = BitArray::empty(10);
    let positions: Vec<usize> = empty.iter_within(0..10).collect();
    assert!(positions.is_empty());

    // Test with empty range
    let mut single = BitArray::empty(10);
    single.set(5);
    let positions: Vec<usize> = single.iter_within(3..3).collect();
    assert_eq!(
        positions,
        Vec::<usize>::new(),
        "Empty range should return no bits"
    );

    // Test with reverse range (start > end)
    let positions: Vec<usize> = single.iter_within(7..3).collect();
    assert_eq!(
        positions,
        Vec::<usize>::new(),
        "Reverse range should return no bits"
    );

    // Test with single bit in range
    let positions: Vec<usize> = single.iter_within(3..8).collect();
    assert_eq!(positions, vec![5], "Should find single bit in range");

    // Test with single bit outside range
    let positions: Vec<usize> = single.iter_within(0..3).collect();
    assert_eq!(
        positions,
        Vec::<usize>::new(),
        "Should not find bit outside range"
    );

    let positions: Vec<usize> = single.iter_within(6..10).collect();
    assert_eq!(
        positions,
        Vec::<usize>::new(),
        "Should not find bit outside range"
    );

    // Test with multiple bits, some in range, some out
    let mut multi = BitArray::empty(20);
    multi.set(2); // in range
    multi.set(5); // in range
    multi.set(8); // in range
    multi.set(12); // out of range
    multi.set(18); // out of range
    let positions: Vec<usize> = multi.iter_within(1..10).collect();
    assert_eq!(
        positions,
        vec![2, 5, 8],
        "Should only find bits within range"
    );

    // Test range that starts at bit 0
    let positions: Vec<usize> = multi.iter_within(0..10).collect();
    assert_eq!(
        positions,
        vec![2, 5, 8],
        "Should handle range starting at 0"
    );

    // Test range that ends at last bit
    let positions: Vec<usize> = multi.iter_within(10..20).collect();
    assert_eq!(
        positions,
        vec![12, 18],
        "Should handle range ending at last bit"
    );

    // Test exact range boundaries
    let positions: Vec<usize> = multi.iter_within(2..9).collect();
    assert_eq!(
        positions,
        vec![2, 5, 8],
        "Should include start boundary, exclude end boundary"
    );

    let positions: Vec<usize> = multi.iter_within(3..8).collect();
    assert_eq!(
        positions,
        vec![5],
        "Should exclude boundaries when bits are on them"
    );

    // Test across word boundaries (multiple u64 words)
    let mut cross_word = BitArray::empty(200);
    cross_word.set(30); // First word, in range
    cross_word.set(60); // First word, in range
    cross_word.set(63); // Last bit of first word, in range
    cross_word.set(64); // First bit of second word, in range
    cross_word.set(65); // Second bit of second word, in range
    cross_word.set(100); // Second word, in range
    cross_word.set(127); // Last bit of second word, in range
    cross_word.set(128); // First bit of third word, out of range
    cross_word.set(150); // Third word, out of range

    let positions: Vec<usize> = cross_word.iter_within(25..130).collect();
    assert_eq!(
        positions,
        vec![30, 60, 63, 64, 65, 100, 127, 128],
        "Should handle word boundaries correctly"
    );

    // Test range that starts and ends in middle of words
    let positions: Vec<usize> = cross_word.iter_within(62..66).collect();
    assert_eq!(
        positions,
        vec![63, 64, 65],
        "Should handle mid-word start/end"
    );

    // Test range exactly at word boundary (bit 64)
    let positions: Vec<usize> = cross_word.iter_within(64..128).collect();
    assert_eq!(
        positions,
        vec![64, 65, 100, 127],
        "Should handle exact word boundary"
    );

    // Test range that spans exactly one word boundary
    let positions: Vec<usize> = cross_word.iter_within(63..65).collect();
    assert_eq!(
        positions,
        vec![63, 64],
        "Should handle single word boundary span"
    );

    // Test with consecutive bits across word boundary
    let mut consecutive = BitArray::empty(130);
    for i in 61..68 {
        consecutive.set(i);
    }
    let positions: Vec<usize> = consecutive.iter_within(62..66).collect();
    assert_eq!(
        positions,
        vec![62, 63, 64, 65],
        "Should handle consecutive bits across word boundary"
    );

    // Test with all bits set in a range
    let mut all_set = BitArray::empty(100);
    all_set.set_all();
    let positions: Vec<usize> = all_set.iter_within(10..20).collect();
    assert_eq!(
        positions,
        (10..20).collect::<Vec<_>>(),
        "Should return all positions in range when all bits set"
    );

    // Test with pattern at exact word boundaries
    let mut boundary = BitArray::empty(192); // 3 full words
    boundary.set(0); // First bit of first word
    boundary.set(63); // Last bit of first word
    boundary.set(64); // First bit of second word
    boundary.set(127); // Last bit of second word
    boundary.set(128); // First bit of third word
    boundary.set(191); // Last bit of third word

    // Test range that covers exactly first word
    let positions: Vec<usize> = boundary.iter_within(0..64).collect();
    assert_eq!(
        positions,
        vec![0, 63],
        "Should handle exact first word range"
    );

    // Test range that covers exactly second word
    let positions: Vec<usize> = boundary.iter_within(64..128).collect();
    assert_eq!(
        positions,
        vec![64, 127],
        "Should handle exact middle word range"
    );

    // Test range that covers exactly third word
    let positions: Vec<usize> = boundary.iter_within(128..192).collect();
    assert_eq!(
        positions,
        vec![128, 191],
        "Should handle exact last word range"
    );

    // Test range that covers multiple complete words
    let positions: Vec<usize> = boundary.iter_within(0..128).collect();
    assert_eq!(
        positions,
        vec![0, 63, 64, 127],
        "Should handle multiple complete words"
    );

    // Test single-bit ranges
    let positions: Vec<usize> = boundary.iter_within(63..64).collect();
    assert_eq!(positions, vec![63], "Should handle single-bit range");

    let positions: Vec<usize> = boundary.iter_within(64..65).collect();
    assert_eq!(
        positions,
        vec![64],
        "Should handle single-bit range at word boundary"
    );

    // Test range with no set bits
    let sparse = BitArray::empty(100);
    let positions: Vec<usize> = sparse.iter_within(20..80).collect();
    assert_eq!(
        positions,
        Vec::<usize>::new(),
        "Should return empty for range with no set bits"
    );

    // Test iterator can be used multiple times
    let positions1: Vec<usize> = multi.iter_within(0..10).collect();
    let positions2: Vec<usize> = multi.iter_within(0..10).collect();
    assert_eq!(positions1, positions2, "Iterator should be reusable");

    // Test iterator in for loop
    let mut collected = Vec::new();
    for pos in multi.iter_within(0..10) {
        collected.push(pos);
    }
    assert_eq!(collected, vec![2, 5, 8], "Should work in for loop");

    // Test with alternating pattern
    let mut alternating = BitArray::empty(100);
    for i in (0..100).step_by(2) {
        alternating.set(i); // Set even positions: 0, 2, 4, 6, ...
    }
    let positions: Vec<usize> = alternating.iter_within(5..15).collect();
    assert_eq!(
        positions,
        vec![6, 8, 10, 12, 14],
        "Should handle alternating pattern"
    );

    // Test edge case: range at very end of bit array
    let mut end_array = BitArray::empty(10);
    end_array.set(8);
    end_array.set(9);
    let positions: Vec<usize> = end_array.iter_within(7..10).collect();
    assert_eq!(positions, vec![8, 9], "Should handle range at end of array");

    // Test edge case: range that covers entire array
    let positions: Vec<usize> = multi.iter_within(0..20).collect();
    assert_eq!(
        positions,
        vec![2, 5, 8, 12, 18],
        "Should handle range covering entire array"
    );

    // Test with large sparse array
    let mut large_sparse = BitArray::empty(1000);
    large_sparse.set(100);
    large_sparse.set(500);
    large_sparse.set(900);
    let positions: Vec<usize> = large_sparse.iter_within(50..600).collect();
    assert_eq!(
        positions,
        vec![100, 500],
        "Should efficiently handle large sparse arrays"
    );

    // Test performance comparison case - should be equivalent to filtered iter
    let mut comparison = BitArray::empty(64);
    comparison.set(10);
    comparison.set(20);
    comparison.set(30);
    comparison.set(40);
    comparison.set(50);

    let iter_within_result: Vec<usize> = comparison.iter_within(15..45).collect();
    let filtered_iter_result: Vec<usize> = comparison
        .iter()
        .filter(|&pos| pos >= 15 && pos < 45)
        .collect();
    assert_eq!(
        iter_within_result, filtered_iter_result,
        "iter_within should produce same result as filtered iter"
    );
}

#[test]
#[should_panic(expected = "assertion failed")]
fn test_iter_within_out_of_bounds() {
    let bit_array = BitArray::empty(10);
    let _: Vec<usize> = bit_array.iter_within(0..15).collect(); // end > len should panic
}

#[test]
fn test_iter_within_edge_cases() {
    let zero_len = BitArray::empty(0);
    let positions: Vec<usize> = zero_len.iter_within(0..0).collect();
    assert_eq!(
        positions,
        Vec::<usize>::new(),
        "Zero-length array should work"
    );

    // Test with single-bit array
    let mut single_bit = BitArray::empty(1);
    single_bit.set(0);
    let positions: Vec<usize> = single_bit.iter_within(0..1).collect();
    assert_eq!(positions, vec![0], "Single-bit array should work");

    let positions: Vec<usize> = single_bit.iter_within(0..0).collect();
    assert_eq!(
        positions,
        Vec::<usize>::new(),
        "Empty range on single-bit array"
    );

    // Test with exactly 64-bit array (one full word)
    let mut full_word = BitArray::empty(64);
    full_word.set(0);
    full_word.set(31);
    full_word.set(32);
    full_word.set(63);

    let positions: Vec<usize> = full_word.iter_within(30..35).collect();
    assert_eq!(
        positions,
        vec![31, 32],
        "Should work with exact word-size array"
    );

    let positions: Vec<usize> = full_word.iter_within(0..64).collect();
    assert_eq!(
        positions,
        vec![0, 31, 32, 63],
        "Should work with full word range"
    );

    // Test range at word boundary with no bits set in that word
    let mut boundary_empty = BitArray::empty(128);
    boundary_empty.set(10); // First word
    boundary_empty.set(100); // Second word

    let positions: Vec<usize> = boundary_empty.iter_within(64..80).collect();
    assert_eq!(
        positions,
        Vec::<usize>::new(),
        "Should handle empty word in range"
    );

    let positions: Vec<usize> = boundary_empty.iter_within(50..70).collect();
    assert_eq!(
        positions,
        Vec::<usize>::new(),
        "Should handle range spanning empty word boundary"
    );
}

#[test]
fn test_from_lsb_bytes() {
    let arr = BitArray::from_lsb_bytes(&[], 0);
    assert!(arr.is_empty());

    let arr = BitArray::from_lsb_bytes(&[], 100);
    assert_eq!(arr.len(), 100);
    assert_eq!(arr.count_ones(), 0);

    let arr = BitArray::from_lsb_bytes(&[1, 1, 1, 1, 1, 1], 20);
    assert_eq!(arr.len(), 20);
    assert_eq!(arr.count_ones(), 3);
    let positions = arr.iter().collect::<Vec<_>>();
    assert_eq!(positions, &[0, 8, 16]);

    let arr = BitArray::from_lsb_bytes(&[1, 2, 5], 50);
    assert_eq!(arr.len(), 50);
    let positions = arr.iter().collect::<Vec<_>>();
    assert_eq!(positions, &[0, 9, 16, 18]);
}

#[test]
fn test_count_runs_basic() {
    // empty length
    let empty0 = BitArray::empty(0);
    assert_eq!(empty0.count_runs(), 0);

    // all zeros
    let empty = BitArray::empty(10);
    assert_eq!(empty.count_runs(), 0);

    // single bit at various positions
    let mut single0 = BitArray::empty(10);
    single0.set(0);
    assert_eq!(single0.count_runs(), 1);

    let mut single_mid = BitArray::empty(10);
    single_mid.set(5);
    assert_eq!(single_mid.count_runs(), 1);

    let mut single_last = BitArray::empty(64);
    single_last.set(63);
    assert_eq!(single_last.count_runs(), 1);

    // multiple runs in a single word: [1,2], [5], [7,8,9] => 3 runs
    let mut multi = BitArray::empty(16);
    multi.set(1);
    multi.set(2);
    multi.set(5);
    multi.set(7);
    multi.set(8);
    multi.set(9);
    assert_eq!(multi.count_runs(), 3);

    // all ones is one run (non-empty)
    let mut all_set = BitArray::empty(75);
    all_set.set_all();
    assert_eq!(all_set.count_runs(), 1);
}

#[test]
fn test_count_runs_word_boundaries() {
    // One continuous run across word boundary: 62..=65
    let mut cross = BitArray::empty(130);
    cross.set_range(62..66);
    assert_eq!(cross.count_runs(), 1);

    // Add another isolated bit => +1 run
    cross.set(100);
    assert_eq!(cross.count_runs(), 2);

    // Start exactly at a word boundary (bit 64) with no previous bit set => new run
    let mut start_at_64 = BitArray::empty(130);
    start_at_64.set(64);
    assert_eq!(start_at_64.count_runs(), 1);

    // Bits 63 and 64 both set form a single continuous run, not two
    let mut join_across = BitArray::empty(130);
    join_across.set(63);
    join_across.set(64);
    assert_eq!(join_across.count_runs(), 1);

    // Ends at word boundary then resumes after a gap => two runs
    let mut gap_after_boundary = BitArray::empty(130);
    gap_after_boundary.set_range(60..64); // ends at 63
    gap_after_boundary.set_range(66..70); // starts at 66
    assert_eq!(gap_after_boundary.count_runs(), 2);
}

fn count_runs_slow(b: &BitArray) -> usize {
    b.iter()
        .coalesce(|p, n| if p + 1 == n { Ok(n) } else { Err((p, n)) })
        .count()
}

#[test]
fn test_count_runs_patterns_and_tail() {
    // Alternating bits: every 1 is isolated => runs == number of ones
    let words = [0xAAAAAAAAAAAAAAAAu64]; // ...1010 pattern (ones at odd positions)
    let alt = BitArray::from_lsb_words(&words, 64);
    assert_eq!(alt.count_runs(), count_runs_slow(&alt));

    // Partial tail should be masked and not create extra runs
    let full_partial = BitArray::full(70);
    assert_eq!(full_partial.count_runs(), count_runs_slow(&full_partial));

    // Pattern over multiple words
    let words2 = [0x0000_FFFF_0000_FFFFu64, 0xF0F0_F0F0_F0F0_F0F0u64];
    let multi = BitArray::from_lsb_words(&words2, 128);
    // Rough expectation: the first word has two 16-bit runs repeated twice => 4 runs,
    // the second word has isolated 4-bit runs repeating => 8 runs
    assert_eq!(multi.count_runs(), count_runs_slow(&multi));
}

// Slow reference for ranges within a half-open interval [start, end)
// Scans bit-by-bit using contains() and builds contiguous 1-runs.
fn ranges_within_slow_scan(b: &BitArray, r: std::ops::Range<usize>) -> Vec<std::ops::Range<usize>> {
    assert!(r.end <= b.len(), "out of bounds in slow reference");
    if r.start >= r.end {
        return Vec::new();
    }
    let mut res = Vec::new();
    let mut i = r.start;
    while i < r.end {
        if b.contains(i) {
            let mut j = i + 1;
            while j < r.end && b.contains(j) {
                j += 1;
            }
            res.push(i..j);
            i = j;
        } else {
            i += 1;
        }
    }
    res
}

#[test]
fn test_ranges_iter_within_empty_inputs() {
    // Zero-length array, empty range
    let b = BitArray::empty(0);
    let got: Vec<_> = b.ranges_iter_within(0..0).collect();
    assert!(got.is_empty());

    // Non-zero-length array with all zeros
    let b = BitArray::empty(10);
    assert!(b.ranges_iter_within(0..0).next().is_none());
    assert!(b.ranges_iter_within(3..3).next().is_none());
    assert!(
        b.ranges_iter_within(7..3).next().is_none(),
        "start >= end => empty"
    );
    assert!(b.ranges_iter_within(0..10).next().is_none());
}

#[test]
fn test_ranges_iter_within_single_bit_cases() {
    let mut b = BitArray::empty(10);
    b.set(5);

    // Fully enclosing
    assert_eq!(b.ranges_iter_within(0..10).collect::<Vec<_>>(), vec![5..6]);

    // Exact range around the bit
    assert_eq!(b.ranges_iter_within(5..6).collect::<Vec<_>>(), vec![5..6]);

    // Clipped from left/right
    assert_eq!(b.ranges_iter_within(3..8).collect::<Vec<_>>(), vec![5..6]);
    assert_eq!(b.ranges_iter_within(5..7).collect::<Vec<_>>(), vec![5..6]);

    // Outside the bit
    assert!(b.ranges_iter_within(0..5).next().is_none());
    assert!(b.ranges_iter_within(6..10).next().is_none());
    assert!(b.ranges_iter_within(6..6).next().is_none());
}

#[test]
fn test_ranges_iter_within_multiple_runs_and_clipping() {
    let mut b = BitArray::empty(20);
    b.set_range(1..3); // [1,2]
    b.set_range(5..8); // [5,6,7]
    b.set_range(12..15); // [12,13,14]

    // Full
    assert_eq!(
        b.ranges_iter_within(0..20).collect::<Vec<_>>(),
        vec![1..3, 5..8, 12..15]
    );

    // Clips left and right
    assert_eq!(
        b.ranges_iter_within(2..6).collect::<Vec<_>>(),
        vec![2..3, 5..6]
    );
    assert_eq!(b.ranges_iter_within(6..12).collect::<Vec<_>>(), vec![6..8]);
    assert_eq!(
        b.ranges_iter_within(7..14).collect::<Vec<_>>(),
        vec![7..8, 12..14]
    );
    assert!(b.ranges_iter_within(3..5).next().is_none());
    assert_eq!(b.ranges_iter_within(3..6).collect::<Vec<_>>(), vec![5..6]);
    assert!(b.ranges_iter_within(8..12).next().is_none());
}

#[test]
fn test_ranges_iter_within_word_boundaries_and_crossing() {
    // Run crosses 64-bit word boundary: 60..70
    let mut b = BitArray::empty(130);
    b.set_range(60..70);
    b.set_range(125..130); // tail run

    // Sub-ranges inside the first run
    assert_eq!(
        b.ranges_iter_within(62..66).collect::<Vec<_>>(),
        vec![62..66]
    );
    assert_eq!(
        b.ranges_iter_within(63..65).collect::<Vec<_>>(),
        vec![63..65]
    );
    assert_eq!(
        b.ranges_iter_within(59..61).collect::<Vec<_>>(),
        vec![60..61]
    );
    assert_eq!(
        b.ranges_iter_within(68..72).collect::<Vec<_>>(),
        vec![68..70]
    );

    // A range that spans two runs and clips at the end boundary (128)
    assert_eq!(
        b.ranges_iter_within(64..128).collect::<Vec<_>>(),
        vec![64..70, 125..128]
    );

    // Exactly at the boundary bit(s)
    let mut c = BitArray::empty(130);
    c.set(63);
    c.set(64);
    assert_eq!(
        c.ranges_iter_within(63..65).collect::<Vec<_>>(),
        vec![63..65]
    );
    assert_eq!(
        c.ranges_iter_within(64..65).collect::<Vec<_>>(),
        vec![64..65]
    );
    assert_eq!(
        c.ranges_iter_within(63..64).collect::<Vec<_>>(),
        vec![63..64]
    );

    // end == 64 clips the run to [63..64)
    assert_eq!(
        c.ranges_iter_within(0..64).collect::<Vec<_>>(),
        vec![63..64]
    );
}

#[test]
fn test_ranges_iter_within_long_run_spanning_full_words() {
    // Long contiguous run from 10..190 spanning multiple 64-bit words
    let mut b = BitArray::empty(200);
    b.set_range(10..190);

    assert_eq!(
        b.ranges_iter_within(0..200).collect::<Vec<_>>(),
        vec![10..190]
    );
    assert_eq!(
        b.ranges_iter_within(50..150).collect::<Vec<_>>(),
        vec![50..150]
    );
    assert_eq!(
        b.ranges_iter_within(150..200).collect::<Vec<_>>(),
        vec![150..190]
    );
    assert!(b.ranges_iter_within(10..10).next().is_none());

    // Create a partial-first-word -> full-words -> partial-next-word structure
    // to exercise the internal "full word" and "partial leading ones" branches.
    let mut c = BitArray::empty(300);
    c.set_range(5..5 + 64 + 64 + 9); // 5..142 => partial word (5..64), full (64..128), partial (128..142)
    assert_eq!(
        c.ranges_iter_within(0..300).collect::<Vec<_>>(),
        vec![5..142]
    );
    // Clip within the second full word and the final partial
    assert_eq!(
        c.ranges_iter_within(100..140).collect::<Vec<_>>(),
        vec![100..140]
    );
    // Clip starting mid-first word and ending mid-second full word
    assert_eq!(
        c.ranges_iter_within(10..130).collect::<Vec<_>>(),
        vec![10..130]
    );
}

#[test]
fn test_ranges_iter_within_tail_masking_and_exact_bounds() {
    // Non-64-aligned length where all bits are set
    let mut b = BitArray::empty(70);
    b.set_all();

    assert_eq!(b.ranges_iter_within(0..70).collect::<Vec<_>>(), vec![0..70]);
    assert_eq!(b.ranges_iter_within(0..64).collect::<Vec<_>>(), vec![0..64]);
    assert_eq!(
        b.ranges_iter_within(64..70).collect::<Vec<_>>(),
        vec![64..70]
    );
    assert_eq!(
        b.ranges_iter_within(65..70).collect::<Vec<_>>(),
        vec![65..70]
    );

    // Ensure a run starting before the range is clipped to the start
    let mut c = BitArray::empty(100);
    c.set_range(0..20);
    assert_eq!(c.ranges_iter_within(5..10).collect::<Vec<_>>(), vec![5..10]);
}

#[test]
fn test_ranges_iter_within_matches_slow_reference_on_various_patterns() {
    // Alternating single-bit runs
    let mut alt = BitArray::empty(100);
    for i in (0..100).step_by(2) {
        alt.set(i);
    }
    let got = alt.ranges_iter_within(5..15).collect::<Vec<_>>();
    let exp = ranges_within_slow_scan(&alt, 5..15);
    assert_eq!(got, exp);

    // Sparse points across words
    let mut sparse = BitArray::empty(200);
    for &i in &[30, 60, 63, 64, 65, 100, 127, 128, 150] {
        sparse.set(i);
    }
    for &(s, e) in &[
        (0, 200),
        (25, 130),
        (62, 66),
        (64, 128),
        (63, 65),
        (63, 64),
        (128, 151),
    ] {
        let got = sparse.ranges_iter_within(s..e).collect::<Vec<_>>();
        let exp = ranges_within_slow_scan(&sparse, s..e);
        assert_eq!(got, exp, "mismatch on range {}..{}", s, e);
    }

    // A few random-ish non-overlapping runs
    let mut runs = BitArray::empty(256);
    for r in [1..3, 5..12, 60..70, 90..120, 180..183, 200..210, 240..256] {
        runs.set_range(r);
    }
    for &(s, e) in &[
        (0, 256),
        (2, 6),
        (6, 12),
        (59, 61),
        (64, 128),
        (85, 130),
        (100, 205),
        (239, 256),
        (250, 256),
        (0, 0),
        (127, 127),
    ] {
        let got = runs.ranges_iter_within(s..e).collect::<Vec<_>>();
        let exp = ranges_within_slow_scan(&runs, s..e);
        assert_eq!(got, exp, "mismatch on range {}..{}", s, e);
    }
}

#[test]
fn test_ranges_iter_within_random_patterns() {
    fastrand::seed(6412384656);

    const LEN: usize = 1000;
    for i in 0..800 {
        let mut arr = BitArray::empty(LEN);
        for _ in 0..i {
            arr.set(fastrand::usize(..LEN));
        }

        for _ in 0..100 {
            let start = fastrand::usize(..LEN - 1);
            let end = fastrand::usize(start..LEN);
            let expected = ranges_within_slow_scan(&arr, start..end);
            let actual = arr.ranges_iter_within(start..end).collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
    }
}

#[test]
fn test_ranges_iter_within_equals_ranges_iter_for_full_bounds() {
    // For [0..len), ranges_iter_within should be identical to ranges_iter
    let mut b = BitArray::empty(150);
    b.set_range(0..10);
    b.set_range(20..30);
    b.set_range(63..66);
    b.set_range(64..80); // join across boundary
    b.set(149);

    let full = b.ranges_iter().collect::<Vec<_>>();
    let within = b.ranges_iter_within(0..b.len()).collect::<Vec<_>>();
    assert_eq!(full, within);
}

#[test]
#[should_panic(expected = "assertion failed")]
fn test_ranges_iter_within_out_of_bounds_panics() {
    let b = BitArray::empty(10);
    // end > len
    let _ = b.ranges_iter_within(0..15).collect::<Vec<_>>();
}

#[test]
#[should_panic(expected = "assertion failed")]
fn test_ranges_iter_within_zero_len_array_out_of_bounds_panics() {
    let b = BitArray::empty(0);
    let _ = b.ranges_iter_within(0..1).collect::<Vec<_>>();
}

#[test]
fn test_for_each_set_bit() {
    // Empty array: no calls
    let empty = BitArray::empty(10);
    let mut calls: Vec<(usize, u64)> = vec![];
    empty.for_each_set_bit(|i, p| calls.push((i, p)));
    assert!(calls.is_empty());

    // Single bit
    let mut single = BitArray::empty(10);
    single.set(5);
    let mut calls: Vec<(usize, u64)> = vec![];
    single.for_each_set_bit(|i, p| calls.push((i, p)));
    assert_eq!(calls, vec![(0, 5u64)]);

    // Multiple bits in one word
    let mut multi = BitArray::empty(20);
    for &i in &[0, 3, 7, 11, 19] {
        multi.set(i);
    }
    let mut calls: Vec<(usize, u64)> = vec![];
    multi.for_each_set_bit(|i, p| calls.push((i, p)));
    let expected: Vec<(usize, u64)> = vec![0, 3, 7, 11, 19]
        .into_iter()
        .enumerate()
        .map(|(i, p)| (i, p as u64))
        .collect();
    assert_eq!(calls, expected);

    // Across word boundaries
    let mut cross_word = BitArray::empty(150);
    for &i in &[0, 63, 64, 65, 127, 128, 149] {
        cross_word.set(i);
    }
    let mut calls: Vec<(usize, u64)> = vec![];
    cross_word.for_each_set_bit(|i, p| calls.push((i, p)));
    let expected: Vec<(usize, u64)> = vec![0, 63, 64, 65, 127, 128, 149]
        .into_iter()
        .enumerate()
        .map(|(i, p)| (i, p as u64))
        .collect();
    assert_eq!(calls, expected);

    // All bits set
    let mut all_set = BitArray::empty(8);
    all_set.set_all();
    let mut calls: Vec<(usize, u64)> = vec![];
    all_set.for_each_set_bit(|i, p| calls.push((i, p)));
    let expected: Vec<(usize, u64)> = (0..8).map(|p| (p, p as u64)).collect();
    assert_eq!(calls, expected);

    // Respects bit array length (partial last word)
    let words = [0xFFFFFFFFFFFFFFFFu64, 0xFFFFFFFFFFFFFFFFu64];
    let partial = BitArray::from_lsb_words(&words, 70);
    let mut calls: Vec<(usize, u64)> = vec![];
    partial.for_each_set_bit(|i, p| calls.push((i, p)));
    assert_eq!(calls.len(), 70);
    assert_eq!(calls.first(), Some(&(0, 0)));
    assert_eq!(calls.last(), Some(&(69, 69)));

    // Exactly 64 bits (one full word)
    let mut full_word = BitArray::empty(64);
    for &i in &[0, 31, 32, 63] {
        full_word.set(i);
    }
    let mut calls: Vec<(usize, u64)> = vec![];
    full_word.for_each_set_bit(|i, p| calls.push((i, p)));
    let expected: Vec<(usize, u64)> = vec![0, 31, 32, 63]
        .into_iter()
        .enumerate()
        .map(|(i, p)| (i, p as u64))
        .collect();
    assert_eq!(calls, expected);
}

#[test]
fn test_big_bit_array() {
    const LEN: usize = 16 * 1024 * 1024 + 100;
    let mut bits = BigBitArray::empty(16 * 1024 * 1024 + 100);
    assert_eq!(bits.count_ones(), 0);
    assert_eq!(bits.len(), LEN);

    for pos in (0..LEN).step_by(7) {
        bits.set(pos);
    }

    assert!(bits.contains(0));
    assert!(bits.contains(7));
    assert_eq!(bits.count_ones(), LEN / 7 + (LEN % 7 != 0) as usize);
    bits.clear();
    assert_eq!(bits.count_ones(), 0);
}

#[test]
fn test_bit_array_on_slice() {
    let bits = BitArrayBase::wrap_lsb_words([1u64, 2u64, 3u64].as_ref(), Some(188));
    assert_eq!(bits.count_ones(), 4);
    let positions = bits.iter().collect::<Vec<_>>();
    assert_eq!(&positions, &[0, 65, 128, 129]);
}
