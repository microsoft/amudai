//! Zero-copy buffer-level Arrow interop.

use amudai_bytes::buffer::AlignedByteVec;

/// Converts an `AlignedByteVec` into an `arrow_buffer::Buffer` without copying or allocating.
///
/// This function reuses the underlying memory of the `AlignedByteVec` to create an
/// `arrow_buffer::Buffer`. If the `AlignedByteVec` is empty, a new empty `arrow_buffer::Buffer`
/// is returned.
///
/// # Arguments
///
/// * `aligned_vec` - The `AlignedByteVec` to convert.  The `AlignedByteVec` is consumed.
///
/// # Returns
///
/// An `arrow_buffer::Buffer` containing the data from the `AlignedByteVec`.
pub fn aligned_vec_to_buf(aligned_vec: AlignedByteVec) -> arrow_buffer::Buffer {
    if aligned_vec.is_empty() {
        return arrow_buffer::MutableBuffer::new(0).into();
    }
    let (vec, offset) = aligned_vec.into_vec();
    let mut buf = arrow_buffer::Buffer::from_vec(vec);
    buf.advance(offset);
    buf
}

#[cfg(test)]
mod tests {
    use amudai_bytes::buffer::AlignedByteVec;

    use super::aligned_vec_to_buf;

    #[test]
    fn test_aligned_vec_to_buf() {
        let mut v = AlignedByteVec::new();
        v.resize(64, 255);
        let buf = aligned_vec_to_buf(v);
        let values = buf.typed_data::<u64>();
        assert_eq!(values.len(), 8);
        assert_eq!(values[0], u64::MAX);
        assert_eq!(values[7], u64::MAX);

        let buf = aligned_vec_to_buf(AlignedByteVec::new());
        assert!(buf.is_empty());
    }
}
