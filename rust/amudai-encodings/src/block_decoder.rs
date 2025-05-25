use amudai_sequence::sequence::ValueSequence;

pub trait BlockDecoder: Send + Sync + 'static {
    /// Decodes a single block of encoded binary data into a sequence of values.
    ///
    /// # Arguments
    ///
    /// - `encoded` - A byte slice containing the encoded block, including the header
    ///   and the payload.
    /// - `value_count` - The number of values that are encoded in the block.
    ///
    /// # Returns
    ///
    /// A sequence of values, potentially accompanied by a validity (presence) bitmap.
    fn decode(&self, encoded: &[u8], value_count: usize) -> amudai_common::Result<ValueSequence>;
}
